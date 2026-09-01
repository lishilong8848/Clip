# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import datetime as dt
import hashlib
import math
import os
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any


MORNING_MEETING_NAMESPACE = "daily_morning_meeting"
MORNING_MEETING_SCOPES = ("A", "B", "C", "D", "E", "H", "110")
MORNING_MEETING_ROW_BY_SCOPE = {
    "A": 4,
    "B": 5,
    "C": 6,
    "D": 7,
    "E": 8,
    "H": 9,
    "110": 10,
}
MORNING_MEETING_TEMPLATE_SHA256 = (
    "1ea64b64f96b0ed48a4b1797f5011f5509e0a9056b7dd0a923aae1b78de7f431"
)

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_TEMPLATE_B64 = Path(__file__).with_name("templates") / "EA118-H楼晨会.xlsx.b64"
_SHEET_PATH = "xl/worksheets/sheet1.xml"
_WORKBOOK_PATH = "xl/workbook.xml"
_BASE_ROW_HEIGHTS = {4: 78.0, 5: 72.0, 6: 78.0, 7: 78.0, 8: 119.0, 9: 78.0, 10: 78.0}


class MorningMeetingError(RuntimeError):
    pass


def morning_meeting_template_bytes() -> bytes:
    try:
        content = base64.b64decode(_TEMPLATE_B64.read_text(encoding="ascii"))
    except Exception as exc:
        raise MorningMeetingError("晨会表格模板无法读取。") from exc
    if hashlib.sha256(content).hexdigest() != MORNING_MEETING_TEMPLATE_SHA256:
        raise MorningMeetingError("晨会表格模板校验失败。")
    try:
        from io import BytesIO

        with zipfile.ZipFile(BytesIO(content)) as archive:
            if archive.testzip() is not None:
                raise MorningMeetingError("晨会表格模板已损坏。")
            workbook = ET.fromstring(archive.read(_WORKBOOK_PATH))
            sheet_names = [
                str(item.attrib.get("name") or "")
                for item in workbook.findall(f".//{{{_MAIN_NS}}}sheet")
            ]
            if sheet_names != ["H楼晨会"] or _SHEET_PATH not in archive.namelist():
                raise MorningMeetingError("晨会表格模板结构不匹配。")
    except MorningMeetingError:
        raise
    except Exception as exc:
        raise MorningMeetingError("晨会表格模板已损坏。") from exc
    return content


def morning_meeting_file_name(day: dt.date) -> str:
    return f"EA118-H楼{day.month}月{day.day}日晨会.xlsx"


def _cell(root: ET.Element, reference: str) -> ET.Element:
    node = root.find(f".//{{{_MAIN_NS}}}c[@r='{reference}']")
    if node is None:
        raise MorningMeetingError(f"晨会模板缺少单元格 {reference}。")
    return node


def _clear_cell(cell: ET.Element) -> None:
    for child in list(cell):
        if child.tag in {
            f"{{{_MAIN_NS}}}f",
            f"{{{_MAIN_NS}}}v",
            f"{{{_MAIN_NS}}}is",
        }:
            cell.remove(child)


def _set_text(root: ET.Element, reference: str, value: Any) -> None:
    cell = _cell(root, reference)
    _clear_cell(cell)
    cell.attrib["t"] = "inlineStr"
    inline = ET.SubElement(cell, f"{{{_MAIN_NS}}}is")
    text_node = ET.SubElement(inline, f"{{{_MAIN_NS}}}t")
    text_node.attrib["{http://www.w3.org/XML/1998/namespace}space"] = "preserve"
    text_node.text = str(value or "")


def _set_number(root: ET.Element, reference: str, value: Any) -> None:
    cell = _cell(root, reference)
    _clear_cell(cell)
    if value in (None, ""):
        cell.attrib["t"] = "inlineStr"
        ET.SubElement(ET.SubElement(cell, f"{{{_MAIN_NS}}}is"), f"{{{_MAIN_NS}}}t").text = ""
        return
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise MorningMeetingError(f"{reference} 温度格式无效。") from exc
    if not math.isfinite(number):
        raise MorningMeetingError(f"{reference} 温度格式无效。")
    cell.attrib.pop("t", None)
    ET.SubElement(cell, f"{{{_MAIN_NS}}}v").text = f"{number:g}"


def _display_width(value: str) -> int:
    return sum(2 if ord(char) > 127 else 1 for char in str(value or ""))


def _row_height(lines: list[str], base_height: float) -> float:
    visual_lines = sum(max(1, math.ceil(_display_width(line) / 120)) for line in lines)
    return max(base_height, 18.0 + visual_lines * 18.0)


def _patch_sheet(sheet_bytes: bytes, model: dict[str, Any]) -> bytes:
    root = ET.fromstring(sheet_bytes)
    try:
        day = dt.date.fromisoformat(str(model.get("date") or ""))
    except ValueError as exc:
        raise MorningMeetingError("晨会日期格式无效。") from exc
    excel_serial = (day - dt.date(1899, 12, 30)).days
    _set_number(root, "B2", excel_serial)
    _set_text(root, "D2", model.get("weather_condition"))
    _set_number(root, "F2", model.get("dry_bulb_temperature"))
    _set_number(root, "H2", model.get("wet_bulb_temperature"))
    rows = model.get("rows") if isinstance(model.get("rows"), list) else []
    rows_by_scope = {
        str(item.get("scope") or ""): item
        for item in rows
        if isinstance(item, dict)
    }
    for scope, row_number in MORNING_MEETING_ROW_BY_SCOPE.items():
        row = rows_by_scope.get(scope) or {}
        lines = [str(item or "").strip() for item in row.get("lines") or [] if str(item or "").strip()]
        if not lines:
            lines = ["值班巡检"]
        _set_text(
            root,
            f"B{row_number}",
            "\n".join(f"{index}、{line}" for index, line in enumerate(lines, 1)),
        )
        row_node = root.find(f".//{{{_MAIN_NS}}}row[@r='{row_number}']")
        if row_node is None:
            raise MorningMeetingError(f"晨会模板缺少第 {row_number} 行。")
        row_node.attrib["ht"] = f"{_row_height(lines, _BASE_ROW_HEIGHTS[row_number]):g}"
        row_node.attrib["customHeight"] = "1"
    page_setup = root.find(f"{{{_MAIN_NS}}}pageSetup")
    if page_setup is None:
        raise MorningMeetingError("晨会模板缺少打印设置。")
    page_setup.attrib.update(
        {
            "paperSize": "9",
            "orientation": "landscape",
            "fitToWidth": "1",
            "fitToHeight": "1",
        }
    )
    page_setup.attrib.pop("scale", None)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def build_morning_meeting_workbook(
    output_path: Path | str,
    model: dict[str, Any],
) -> Path:
    destination = Path(output_path).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    template = morning_meeting_template_bytes()
    from io import BytesIO

    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=destination.parent,
            prefix=f".{destination.stem}-",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            with zipfile.ZipFile(BytesIO(template)) as source, zipfile.ZipFile(
                temporary,
                "w",
                zipfile.ZIP_DEFLATED,
            ) as target:
                for info in source.infolist():
                    content = source.read(info.filename)
                    if info.filename == _SHEET_PATH:
                        content = _patch_sheet(content, model)
                    target.writestr(info, content)
        with zipfile.ZipFile(temporary_path) as generated:
            if generated.testzip() is not None:
                raise MorningMeetingError("生成的晨会表格校验失败。")
        os.replace(temporary_path, destination)
        temporary_path = None
        return destination
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import copy
import datetime as dt
import hashlib
import io
import os
import posixpath
import re
import shutil
import threading
import time
import uuid
import warnings
import zipfile
import xml.etree.ElementTree as ET
from contextlib import suppress
from pathlib import Path
from typing import Any, BinaryIO, Callable


DRILL_DEFINITION_NAMESPACE = "drill_definition"
DRILL_EXECUTION_NAMESPACE = "drill_execution"
DRILL_SCOPES = ("A", "B", "C", "D", "E")
DRILL_MAX_XLSX_BYTES = 64 * 1024 * 1024
DRILL_MAX_XLSX_ENTRIES = 8192
DRILL_MAX_XLSX_EXPANDED_BYTES = 512 * 1024 * 1024
DRILL_MAX_PARTICIPANTS = 10
DRILL_MAX_SOURCE_ROWS = 500
DRILL_MAX_SOURCE_COLUMNS = 80
DRILL_MAX_SOURCE_CELLS = DRILL_MAX_SOURCE_ROWS * DRILL_MAX_SOURCE_COLUMNS
DRILL_MAX_SIGNATURE_BYTES = 2 * 1024 * 1024
DRILL_MAX_SIGNATURE_DIMENSION = 4096
DRILL_MAX_SIGNATURE_PIXELS = 16_000_000
DRILL_MAX_PREVIEW_IMAGE_BYTES = 1024 * 1024
DRILL_MAX_PREVIEW_IMAGES_BYTES = 2 * 1024 * 1024
DRILL_MAX_PREVIEW_IMAGES = 20

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_DOC_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_XDR_NS = "http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing"
_A_NS = "http://schemas.openxmlformats.org/drawingml/2006/main"
_PIC_NS = "http://schemas.openxmlformats.org/drawingml/2006/picture"
_CT_NS = "http://schemas.openxmlformats.org/package/2006/content-types"
_EMU_PER_PIXEL = 9525

ET.register_namespace("", _MAIN_NS)
ET.register_namespace("r", _DOC_REL_NS)
ET.register_namespace("xdr", _XDR_NS)
ET.register_namespace("a", _A_NS)


class DrillError(RuntimeError):
    status_code = 400


class DrillNotFoundError(DrillError):
    status_code = 404


class DrillConflictError(DrillError):
    status_code = 409


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _safe_file_name(value: Any, fallback: str = "演练记录.xlsx") -> str:
    name = Path(str(value or "").strip()).name
    name = re.sub(r"[\x00-\x1f<>:\"/\\|?*]+", "_", name).strip(" .")
    return (name or fallback)[:180]


def _normalize_scope(value: Any) -> str:
    scope = str(value or "").strip().upper()
    if scope not in DRILL_SCOPES:
        raise DrillError("演练管理仅支持 A、B、C、D、E 楼。")
    return scope


def _normalize_month(year: Any, month: Any) -> tuple[int, int]:
    try:
        normalized_year = int(year)
        normalized_month = int(month)
    except (TypeError, ValueError) as exc:
        raise DrillError("年份和月份格式无效。") from exc
    if normalized_year not in range(2000, 2101) or normalized_month not in range(1, 13):
        raise DrillError("年份或月份超出允许范围。")
    return normalized_year, normalized_month


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_xlsx_archive(path: Path) -> None:
    if not path.is_file() or path.stat().st_size <= 0:
        raise DrillError("上传文件为空。")
    if path.stat().st_size > DRILL_MAX_XLSX_BYTES:
        raise DrillError("演练文件不能超过 64MB。")
    if not zipfile.is_zipfile(path):
        raise DrillError("上传文件不是有效的 .xlsx 工作簿。")
    blocked_prefixes = (
        "xl/externallinks/",
        "xl/embeddings/",
        "xl/activex/",
        "customui/",
    )
    try:
        with zipfile.ZipFile(path) as archive:
            entries = archive.infolist()
            if len(entries) > DRILL_MAX_XLSX_ENTRIES:
                raise DrillError("上传工作簿内部文件过多，请精简后重试。")
            expanded = 0
            names = set()
            normalized_names = set()
            for entry in entries:
                name = str(entry.filename or "").replace("\\", "/")
                lowered = name.lower()
                parts = [part for part in name.split("/") if part]
                if not name or name.startswith("/") or ".." in parts:
                    raise DrillError("上传工作簿包含不安全的内部路径。")
                normalized_name = name.casefold()
                if normalized_name in normalized_names:
                    raise DrillError("上传工作簿包含重复的内部路径。")
                normalized_names.add(normalized_name)
                if int(entry.flag_bits or 0) & 0x1:
                    raise DrillError("不支持加密的 Excel 工作簿。")
                if lowered.startswith(blocked_prefixes) or lowered.endswith("vbaproject.bin"):
                    raise DrillError("上传工作簿包含不支持的外部对象或宏。")
                if lowered.endswith((".xml", ".rels")) and int(entry.file_size or 0) > 32 * 1024 * 1024:
                    raise DrillError("上传工作簿中的 XML 结构过大。")
                if (
                    int(entry.file_size or 0) > 8 * 1024 * 1024
                    and int(entry.compress_size or 0) > 0
                    and int(entry.file_size or 0) / int(entry.compress_size or 1) > 200
                ):
                    raise DrillError("上传工作簿包含异常压缩内容。")
                expanded += max(0, int(entry.file_size or 0))
                if expanded > DRILL_MAX_XLSX_EXPANDED_BYTES:
                    raise DrillError("上传工作簿解压后体积过大，请精简后重试。")
                names.add(name)
            required = {"[Content_Types].xml", "xl/workbook.xml", "xl/_rels/workbook.xml.rels"}
            if not required.issubset(names):
                raise DrillError("上传工作簿缺少必要的 Excel 结构。")
            for relationship_path in (
                name for name in names if name.lower().endswith(".rels")
            ):
                relationship_root = ET.fromstring(archive.read(relationship_path))
                if any(
                    str(item.attrib.get("TargetMode") or "").strip().lower()
                    == "external"
                    for item in relationship_root.findall(
                        f"{{{_PKG_REL_NS}}}Relationship"
                    )
                ):
                    raise DrillError("上传工作簿包含外部链接关系。")
    except DrillError:
        raise
    except (OSError, zipfile.BadZipFile, zipfile.LargeZipFile, ET.ParseError) as exc:
        raise DrillError("上传文件损坏或不是有效的 Excel 工作簿。") from exc


def _column_number(reference: str) -> int:
    letters = re.match(r"[A-Z]+", str(reference or "").upper())
    if not letters:
        return 0
    result = 0
    for character in letters.group(0):
        result = result * 26 + ord(character) - 64
    return result


def _column_name(number: int) -> str:
    result = ""
    value = int(number)
    while value > 0:
        value, remainder = divmod(value - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _cell_parts(reference: str) -> tuple[int, int]:
    match = re.fullmatch(r"\$?([A-Za-z]{1,3})\$?(\d+)", str(reference or "").strip())
    if not match:
        raise DrillError(f"无效的单元格地址：{reference or '空'}")
    return int(match.group(2)), _column_number(match.group(1))


def _cell_ref(row: int, col: int) -> str:
    return f"{_column_name(col)}{row}"


def _range_bounds(reference: str) -> tuple[int, int, int, int]:
    text = str(reference or "").replace("$", "").strip()
    left, _, right = text.partition(":")
    row1, col1 = _cell_parts(left)
    row2, col2 = _cell_parts(right or left)
    return min(row1, row2), min(col1, col2), max(row1, row2), max(col1, col2)


def _validated_range(reference: str) -> str:
    row1, col1, row2, col2 = _range_bounds(reference)
    if (
        row1 < 1
        or col1 < 1
        or row2 > DRILL_MAX_SOURCE_ROWS
        or col2 > DRILL_MAX_SOURCE_COLUMNS
    ):
        raise DrillError(
            f"单元格映射最多支持 {DRILL_MAX_SOURCE_ROWS} 行、"
            f"{DRILL_MAX_SOURCE_COLUMNS} 列。"
        )
    return str(reference or "").replace("$", "").upper()


def _normalize_label(value: Any) -> str:
    return re.sub(r"[\s：:（）()【】\[\]]+", "", str(value or "")).strip()


def parse_duration_minutes(value: Any) -> int:
    text = re.sub(r"\s+", "", str(value or "")).replace("小时", "时").replace("分钟", "分")
    if not text:
        raise DrillError("演练步骤缺少预估时间。")
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        minutes = float(text)
    else:
        match = re.fullmatch(r"(?:(\d+(?:\.\d+)?)时)?(?:(\d+(?:\.\d+)?)分)?", text)
        if not match or not any(match.groups()):
            raise DrillError(f"无法识别预估时间：{text}")
        minutes = float(match.group(1) or 0) * 60 + float(match.group(2) or 0)
    rounded = int(round(minutes))
    if rounded <= 0 or rounded > 24 * 60:
        raise DrillError(f"预估时间必须在 1–1440 分钟之间：{text}")
    return rounded


def build_time_chain(drill_date: str, first_start_time: str, durations: list[int]) -> list[dict[str, str]]:
    try:
        current = dt.datetime.strptime(
            f"{str(drill_date or '').strip()} {str(first_start_time or '').strip()}",
            "%Y-%m-%d %H:%M",
        )
    except ValueError as exc:
        raise DrillError("演练日期或开始时间格式无效。") from exc
    result: list[dict[str, str]] = []
    for duration in durations:
        if int(duration or 0) <= 0:
            raise DrillError("演练步骤预估时间必须大于 0。")
        end = current + dt.timedelta(minutes=int(duration))
        result.append(
            {
                "start_at": current.isoformat(timespec="minutes"),
                "end_at": end.isoformat(timespec="minutes"),
                "start_time": current.strftime("%H:%M"),
                "end_time": end.strftime("%H:%M"),
            }
        )
        current = end
    return result


def _resolve_zip_target(base_part: str, target: str) -> str:
    target = str(target or "").replace("\\", "/")
    if target.startswith("/"):
        return target.lstrip("/")
    return posixpath.normpath(posixpath.join(posixpath.dirname(base_part), target))


def _relationship_map(root: ET.Element) -> dict[str, str]:
    return {
        str(item.attrib.get("Id") or ""): str(item.attrib.get("Target") or "")
        for item in root.findall(f"{{{_PKG_REL_NS}}}Relationship")
    }


def _shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    return ["".join(item.itertext()) for item in root.findall(f"{{{_MAIN_NS}}}si")]


def _theme_colors(archive: zipfile.ZipFile) -> dict[int, str]:
    if "xl/theme/theme1.xml" not in archive.namelist():
        return {}
    drawing_ns = "http://schemas.openxmlformats.org/drawingml/2006/main"
    root = ET.fromstring(archive.read("xl/theme/theme1.xml"))
    scheme = root.find(f".//{{{drawing_ns}}}clrScheme")
    if scheme is None:
        return {}
    by_name: dict[str, str] = {}
    for item in list(scheme):
        color = next(iter(list(item)), None)
        if color is None:
            continue
        value = str(color.attrib.get("val") or color.attrib.get("lastClr") or "")
        if re.fullmatch(r"[0-9A-Fa-f]{6}", value):
            by_name[item.tag.rsplit("}", 1)[-1]] = f"#{value.upper()}"
    order = (
        "lt1",
        "dk1",
        "lt2",
        "dk2",
        "accent1",
        "accent2",
        "accent3",
        "accent4",
        "accent5",
        "accent6",
        "hlink",
        "folHlink",
    )
    return {index: by_name[name] for index, name in enumerate(order) if name in by_name}


def _style_color(node: ET.Element | None, theme_colors: dict[int, str]) -> str:
    if node is None:
        return ""
    rgb = str(node.attrib.get("rgb") or "")
    if re.fullmatch(r"[0-9A-Fa-f]{8}", rgb):
        return f"#{rgb[-6:].upper()}"
    if re.fullmatch(r"[0-9A-Fa-f]{6}", rgb):
        return f"#{rgb.upper()}"
    try:
        return theme_colors.get(int(node.attrib.get("theme") or -1), "")
    except (TypeError, ValueError):
        return ""


def _cell_style_catalog(archive: zipfile.ZipFile) -> list[dict[str, Any]]:
    if "xl/styles.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/styles.xml"))
    theme_colors = _theme_colors(archive)
    fonts = []
    fonts_root = root.find(f"{{{_MAIN_NS}}}fonts")
    for font in list(fonts_root) if fonts_root is not None else []:
        size = font.find(f"{{{_MAIN_NS}}}sz")
        name_node = font.find(f"{{{_MAIN_NS}}}name")
        try:
            font_size = float(size.attrib.get("val") or 0) if size is not None else 0
        except (TypeError, ValueError):
            font_size = 0
        fonts.append(
            {
                "font_size": font_size,
                "bold": font.find(f"{{{_MAIN_NS}}}b") is not None,
                "italic": font.find(f"{{{_MAIN_NS}}}i") is not None,
                "underline": font.find(f"{{{_MAIN_NS}}}u") is not None,
                "font_name": str(
                    (name_node.attrib.get("val") if name_node is not None else "")
                    or ""
                ),
                "font_color": _style_color(
                    font.find(f"{{{_MAIN_NS}}}color"), theme_colors
                ),
            }
        )
    fills = []
    fills_root = root.find(f"{{{_MAIN_NS}}}fills")
    for fill in list(fills_root) if fills_root is not None else []:
        pattern = fill.find(f"{{{_MAIN_NS}}}patternFill")
        fills.append(
            _style_color(
                pattern.find(f"{{{_MAIN_NS}}}fgColor")
                if pattern is not None
                else None,
                theme_colors,
            )
        )
    borders: list[dict[str, Any]] = []
    borders_root = root.find(f"{{{_MAIN_NS}}}borders")
    for border in list(borders_root) if borders_root is not None else []:
        edges: dict[str, Any] = {}
        for edge_name in ("left", "right", "top", "bottom"):
            edge = border.find(f"{{{_MAIN_NS}}}{edge_name}")
            edge_style = str((edge.attrib.get("style") if edge is not None else "") or "")
            if edge_style:
                edges[edge_name] = {
                    "style": edge_style,
                    "color": _style_color(
                        edge.find(f"{{{_MAIN_NS}}}color") if edge is not None else None,
                        theme_colors,
                    )
                    or "#000000",
                }
        borders.append(edges)
    styles: list[dict[str, Any]] = []
    cell_xfs = root.find(f"{{{_MAIN_NS}}}cellXfs")
    for xf in list(cell_xfs) if cell_xfs is not None else []:
        try:
            font = fonts[int(xf.attrib.get("fontId") or 0)]
        except (IndexError, TypeError, ValueError):
            font = {}
        try:
            fill = fills[int(xf.attrib.get("fillId") or 0)]
        except (IndexError, TypeError, ValueError):
            fill = ""
        try:
            border = borders[int(xf.attrib.get("borderId") or 0)]
        except (IndexError, TypeError, ValueError):
            border = {}
        alignment = xf.find(f"{{{_MAIN_NS}}}alignment")
        style = {
            **{key: value for key, value in font.items() if value},
            **({"fill": fill} if fill else {}),
            "borders": copy.deepcopy(border),
        }
        if alignment is not None:
            horizontal = str(alignment.attrib.get("horizontal") or "")
            vertical = str(alignment.attrib.get("vertical") or "")
            if horizontal:
                style["align"] = horizontal
            if vertical:
                style["vertical_align"] = vertical
            if str(alignment.attrib.get("wrapText") or "") in {"1", "true"}:
                style["wrap_text"] = True
        styles.append(style)
    return styles


def _cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        inline = cell.find(f"{{{_MAIN_NS}}}is")
        return "" if inline is None else "".join(inline.itertext())
    value = cell.find(f"{{{_MAIN_NS}}}v")
    raw = "" if value is None or value.text is None else value.text
    if cell_type == "s":
        try:
            return shared_strings[int(raw)]
        except (IndexError, TypeError, ValueError):
            return raw
    return raw


def _sheet_relationship_path(sheet_path: str) -> str:
    return posixpath.join(posixpath.dirname(sheet_path), "_rels", posixpath.basename(sheet_path) + ".rels")


def _drawing_relationship_path(drawing_path: str) -> str:
    return posixpath.join(posixpath.dirname(drawing_path), "_rels", posixpath.basename(drawing_path) + ".rels")


def _parse_workbook(
    path: Path, *, include_assets_for: set[str] | None = None
) -> dict[str, Any]:
    asset_sheets = {str(item) for item in (include_assets_for or set())}
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        strings = _shared_strings(archive)
        style_catalog = _cell_style_catalog(archive)
        workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
        workbook_rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        workbook_relations = _relationship_map(workbook_rels)
        sheets: list[dict[str, Any]] = []
        for sheet_node in workbook_root.findall(f".//{{{_MAIN_NS}}}sheet"):
            name = str(sheet_node.attrib.get("name") or "Sheet")
            rel_id = str(sheet_node.attrib.get(f"{{{_DOC_REL_NS}}}id") or "")
            target = workbook_relations.get(rel_id, "")
            sheet_path = _resolve_zip_target("xl/workbook.xml", target)
            if sheet_path not in names:
                continue
            root = ET.fromstring(archive.read(sheet_path))
            cells: dict[str, str] = {}
            cell_styles: dict[str, dict[str, Any]] = {}
            max_row = max_col = 0
            cell_nodes = root.findall(f".//{{{_MAIN_NS}}}c")
            if len(cell_nodes) > DRILL_MAX_SOURCE_CELLS:
                raise DrillError(
                    f"工作表“{name}”单元格过多，最多支持 "
                    f"{DRILL_MAX_SOURCE_CELLS} 个。"
                )
            for cell in cell_nodes:
                reference = str(cell.attrib.get("r") or "")
                try:
                    row, col = _cell_parts(reference)
                except DrillError:
                    continue
                if row > DRILL_MAX_SOURCE_ROWS or col > DRILL_MAX_SOURCE_COLUMNS:
                    raise DrillError(
                        f"工作表“{name}”超出支持范围，最多支持 "
                        f"{DRILL_MAX_SOURCE_ROWS} 行、{DRILL_MAX_SOURCE_COLUMNS} 列。"
                    )
                normalized_reference = reference.replace("$", "").upper()
                cells[normalized_reference] = _cell_value(cell, strings)
                try:
                    style = style_catalog[int(cell.attrib.get("s") or 0)]
                except (IndexError, TypeError, ValueError):
                    style = {}
                if style:
                    cell_styles[normalized_reference] = copy.deepcopy(style)
                max_row, max_col = max(max_row, row), max(max_col, col)
            merges = [
                str(item.attrib.get("ref") or "").replace("$", "")
                for item in root.findall(f".//{{{_MAIN_NS}}}mergeCell")
                if str(item.attrib.get("ref") or "").strip()
            ]
            for reference in merges:
                row1, col1, row2, col2 = _range_bounds(reference)
                if row2 > DRILL_MAX_SOURCE_ROWS or col2 > DRILL_MAX_SOURCE_COLUMNS:
                    raise DrillError(
                        f"工作表“{name}”的合并单元格超出支持范围。"
                    )
                max_row, max_col = max(max_row, row2), max(max_col, col2)
            default_row_height = 15.0
            sheet_format = root.find(f"{{{_MAIN_NS}}}sheetFormatPr")
            if sheet_format is not None:
                try:
                    default_row_height = float(sheet_format.attrib.get("defaultRowHeight") or 15.0)
                except ValueError:
                    pass
            row_heights: dict[int, float] = {}
            for row_node in root.findall(f".//{{{_MAIN_NS}}}row"):
                if row_node.attrib.get("ht"):
                    try:
                        row_heights[int(row_node.attrib.get("r") or 0)] = float(row_node.attrib["ht"])
                    except (TypeError, ValueError):
                        pass
            default_col_width = 8.43
            column_widths: dict[int, float] = {}
            for col_node in root.findall(f".//{{{_MAIN_NS}}}col"):
                try:
                    left = int(col_node.attrib.get("min") or 0)
                    right = int(col_node.attrib.get("max") or left)
                    width = float(col_node.attrib.get("width") or default_col_width)
                except (TypeError, ValueError):
                    continue
                for col in range(max(1, left), min(right, 200) + 1):
                    column_widths[col] = width
            connectors: list[dict[str, int]] = []
            preview_images: list[dict[str, Any]] = []
            preview_images_truncated = False
            drawing_path = ""
            drawing = root.find(f"{{{_MAIN_NS}}}drawing")
            sheet_rels_path = _sheet_relationship_path(sheet_path)
            if drawing is not None and sheet_rels_path in names:
                rels_root = ET.fromstring(archive.read(sheet_rels_path))
                target = _relationship_map(rels_root).get(
                    str(drawing.attrib.get(f"{{{_DOC_REL_NS}}}id") or ""), ""
                )
                drawing_path = _resolve_zip_target(sheet_path, target)
                if drawing_path in names:
                    drawing_root = ET.fromstring(archive.read(drawing_path))
                    for anchor in list(drawing_root):
                        if anchor.find(f"{{{_XDR_NS}}}cxnSp") is None:
                            continue
                        start = anchor.find(f"{{{_XDR_NS}}}from")
                        end = anchor.find(f"{{{_XDR_NS}}}to")
                        if start is None or end is None:
                            continue
                        try:
                            connectors.append(
                                {
                                    "from_col": int(start.findtext(f"{{{_XDR_NS}}}col") or 0),
                                    "from_row": int(start.findtext(f"{{{_XDR_NS}}}row") or 0),
                                    "to_col": int(end.findtext(f"{{{_XDR_NS}}}col") or 0),
                                    "to_row": int(end.findtext(f"{{{_XDR_NS}}}row") or 0),
                                }
                            )
                        except ValueError:
                            continue
                    if name in asset_sheets:
                        drawing_rels_path = _drawing_relationship_path(
                            drawing_path
                        )
                        drawing_rels_root = (
                            ET.fromstring(archive.read(drawing_rels_path))
                            if drawing_rels_path in names
                            else ET.Element(f"{{{_PKG_REL_NS}}}Relationships")
                        )
                        preview_images, preview_images_truncated = (
                            _drawing_preview_images(
                                archive,
                                drawing_path,
                                drawing_root,
                                drawing_rels_root,
                                {
                                    "default_row_height": default_row_height,
                                    "row_heights": row_heights,
                                    "default_col_width": default_col_width,
                                    "column_widths": column_widths,
                                },
                            )
                        )
            sheets.append(
                {
                    "name": name,
                    "path": sheet_path,
                    "drawing_path": drawing_path,
                    "cells": cells,
                    "cell_styles": cell_styles,
                    "merges": merges,
                    "max_row": max_row,
                    "max_col": max_col,
                    "default_row_height": default_row_height,
                    "row_heights": row_heights,
                    "default_col_width": default_col_width,
                    "column_widths": column_widths,
                    "connectors": connectors,
                    "preview_images": preview_images,
                    "preview_images_truncated": preview_images_truncated,
                }
            )
    return {"sheets": sheets}


def _merged_range_for(sheet: dict[str, Any], row: int, col: int) -> str:
    for reference in sheet.get("merges") or []:
        row1, col1, row2, col2 = _range_bounds(reference)
        if row1 <= row <= row2 and col1 <= col <= col2:
            return reference
    return _cell_ref(row, col)


def _find_label(sheet: dict[str, Any], labels: tuple[str, ...]) -> tuple[int, int] | None:
    expected = {_normalize_label(item) for item in labels}
    for reference, value in (sheet.get("cells") or {}).items():
        normalized = _normalize_label(value)
        if normalized not in expected:
            continue
        return _cell_parts(reference)
    return None


def _target_right(sheet: dict[str, Any], label: tuple[int, int] | None) -> str:
    if not label:
        return ""
    row, col = label
    label_range = _merged_range_for(sheet, row, col)
    _row1, _col1, _row2, last_col = _range_bounds(label_range)
    return _merged_range_for(sheet, row, last_col + 1)


def _connector_slot_count(sheet: dict[str, Any], row: int, executor_col: int) -> int:
    anchor_row = row - 1
    executor_zero = executor_col - 1
    separators = 0
    for item in sheet.get("connectors") or []:
        if int(item.get("from_row") or -1) != anchor_row or int(item.get("to_row") or -1) != anchor_row:
            continue
        left = min(int(item.get("from_col") or 0), int(item.get("to_col") or 0))
        right = max(int(item.get("from_col") or 0), int(item.get("to_col") or 0))
        if left <= executor_zero <= right:
            separators += 1
    return max(1, separators + 1)


def _detect_record_configuration(sheet: dict[str, Any]) -> dict[str, Any]:
    labels = {
        "machine_room": ("机房名称",),
        "drill_date": ("演练时间", "演练日期"),
        "drill_name": ("演练名称",),
        "area": ("涉及区域", "演练区域"),
        "commander": ("总指挥人", "指挥人"),
        "participants": ("参演人员",),
        "predicted_total": ("预估总用时",),
        "actual_total": ("演练总用时",),
        "participant_signatures": ("演练参演人签字", "参演人签字"),
        "recorder_signature": ("演练记录人签字", "记录人签字"),
    }
    mapping = {key: _target_right(sheet, _find_label(sheet, aliases)) for key, aliases in labels.items()}
    headers = {
        "location_col": _find_label(sheet, ("演练位置",)),
        "content_col": _find_label(sheet, ("演练步骤", "操作步骤")),
        "duration_col": _find_label(sheet, ("预估时间",)),
        "start_col": _find_label(sheet, ("开始时间",)),
        "end_col": _find_label(sheet, ("结束时间",)),
        "executor_col": _find_label(sheet, ("步骤执行人", "执行人")),
        "result_col": _find_label(sheet, ("确认结果",)),
    }
    found = [item for item in headers.values() if item]
    if len(found) != len(headers) or len({item[0] for item in found}) != 1:
        raise DrillError(f"工作表“{sheet.get('name')}”未识别到完整的演练步骤表头。")
    header_row = found[0][0]
    total_label = _find_label(sheet, ("预估总用时", "演练总用时"))
    end_row = (total_label[0] - 1) if total_label else int(sheet.get("max_row") or header_row)
    steps: list[dict[str, Any]] = []
    for row in range(header_row + 1, end_row + 1):
        location_col = int(headers["location_col"][1])
        content_col = int(headers["content_col"][1])
        duration_col = int(headers["duration_col"][1])
        location = str((sheet.get("cells") or {}).get(_cell_ref(row, location_col)) or "").strip()
        content = str((sheet.get("cells") or {}).get(_cell_ref(row, content_col)) or "").strip()
        duration_text = str((sheet.get("cells") or {}).get(_cell_ref(row, duration_col)) or "").strip()
        if not any((location, content, duration_text)):
            continue
        duration_minutes = parse_duration_minutes(duration_text)
        executor_col = int(headers["executor_col"][1])
        steps.append(
            {
                "row": row,
                "location": location,
                "content": content,
                "duration_text": duration_text,
                "duration_minutes": duration_minutes,
                "signature_slots": _connector_slot_count(sheet, row, executor_col),
            }
        )
    if not steps:
        raise DrillError(f"工作表“{sheet.get('name')}”没有可用的演练步骤。")
    mapping["steps"] = {
        "header_row": header_row,
        "start_row": min(item["row"] for item in steps),
        "end_row": max(item["row"] for item in steps),
        **{key: _column_name(int(value[1])) for key, value in headers.items()},
    }
    required = ("machine_room", "drill_date", "drill_name", "participants", "commander", "participant_signatures", "recorder_signature")
    missing = [key for key in required if not mapping.get(key)]
    if missing:
        raise DrillError(f"工作表“{sheet.get('name')}”缺少必要标签：{', '.join(missing)}")
    predicted_target = _cell_target(mapping["predicted_total"]) if mapping.get("predicted_total") else ""
    return {
        "mapping": mapping,
        "steps": steps,
        "predicted_total_text": str((sheet.get("cells") or {}).get(predicted_target) or ""),
    }


def _number(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _detect_assessment_configuration(sheet: dict[str, Any]) -> dict[str, Any]:
    labels = {
        "drill_name": ("演练名称",),
        "drill_date": ("演练日期", "演练时间"),
        "participants": ("参演人员",),
        "start_time": ("开始时间",),
        "end_time": ("结束时间",),
        "total_score": ("总分",),
    }
    mapping = {key: _target_right(sheet, _find_label(sheet, aliases)) for key, aliases in labels.items()}
    score_header = _find_label(sheet, ("分值",))
    result_header = _find_label(sheet, ("得分",))
    if not score_header or not result_header:
        raise DrillError(f"工作表“{sheet.get('name')}”缺少分值或得分列。")
    total_label = _find_label(sheet, ("总分",))
    last_row = (total_label[0] - 1) if total_label else int(sheet.get("max_row") or score_header[0])
    score_rows: list[dict[str, Any]] = []
    for row in range(score_header[0] + 1, last_row + 1):
        value_cell = _cell_ref(row, score_header[1])
        score = _number((sheet.get("cells") or {}).get(value_cell))
        if score is None:
            continue
        score_rows.append(
            {
                "row": row,
                "value_cell": value_cell,
                "score_cell": _cell_ref(row, result_header[1]),
                "score": score,
            }
        )
    total = sum(float(item["score"]) for item in score_rows)
    if not score_rows or abs(total - 100.0) > 0.001:
        raise DrillError(f"评估表分值合计必须为 100，当前为 {total:g}。")
    missing = [key for key, value in mapping.items() if not value]
    if missing:
        raise DrillError(f"工作表“{sheet.get('name')}”缺少必要标签：{', '.join(missing)}")
    mapping["score_rows"] = score_rows
    return {"mapping": mapping}


def detect_drill_configuration(workbook: dict[str, Any]) -> dict[str, Any]:
    sheets = list(workbook.get("sheets") or [])
    if len(sheets) < 2:
        raise DrillError("演练文件至少需要演练记录表和评估表两个工作表。")
    record_candidates = [
        sheet
        for sheet in sheets
        if any("演练记录表" in _normalize_label(value) for value in (sheet.get("cells") or {}).values())
    ]
    assessment_candidates = [
        sheet
        for sheet in sheets
        if any("演练评估表" in _normalize_label(value) for value in (sheet.get("cells") or {}).values())
    ]
    if not record_candidates or not assessment_candidates:
        raise DrillError("无法识别演练记录表和评估表，请检查模板标题。")

    def preferred(items: list[dict[str, Any]], marker: str) -> dict[str, Any]:
        return sorted(
            items,
            key=lambda item: (
                0 if marker in str(item.get("name") or "") else 1,
                1 if "非本月" in str(item.get("name") or "") else 0,
                [str(sheet.get("name") or "") for sheet in sheets].index(
                    str(item.get("name") or "")
                ),
            ),
        )[0]

    record = preferred(record_candidates, "本月")
    assessment = preferred(assessment_candidates, "评估")
    if record["name"] == assessment["name"]:
        raise DrillError("演练记录表和评估表不能选择同一个工作表。")
    record_config = _detect_record_configuration(record)
    assessment_config = _detect_assessment_configuration(assessment)
    return {
        "record_sheet": record["name"],
        "assessment_sheet": assessment["name"],
        "mapping": {
            **record_config["mapping"],
            "assessment": assessment_config["mapping"],
        },
        "steps": record_config["steps"],
        "predicted_total_text": record_config.get("predicted_total_text") or "",
    }


def _find_sheet(workbook: dict[str, Any], name: str) -> dict[str, Any]:
    matches = [sheet for sheet in workbook.get("sheets") or [] if str(sheet.get("name") or "") == str(name or "")]
    if len(matches) != 1:
        raise DrillError(f"工作簿中不存在唯一的工作表“{name or '未选择'}”。")
    return matches[0]


def _validate_configuration(workbook: dict[str, Any], configuration: dict[str, Any]) -> dict[str, Any]:
    config = copy.deepcopy(configuration or {})
    record_sheet = str(config.get("record_sheet") or "").strip()
    assessment_sheet = str(config.get("assessment_sheet") or "").strip()
    if not record_sheet or not assessment_sheet or record_sheet == assessment_sheet:
        raise DrillError("演练记录表和评估表必须选择两个不同的工作表。")
    record = _find_sheet(workbook, record_sheet)
    assessment = _find_sheet(workbook, assessment_sheet)
    try:
        detected_record = _detect_record_configuration(record)
    except DrillError:
        detected_record = {"mapping": {}, "steps": []}
    try:
        detected_assessment = _detect_assessment_configuration(assessment)
    except DrillError:
        detected_assessment = {"mapping": {}}
    supplied_mapping = config.get("mapping") if isinstance(config.get("mapping"), dict) else {}
    mapping = copy.deepcopy(detected_record["mapping"])
    for key in (
        "machine_room",
        "drill_date",
        "drill_name",
        "area",
        "commander",
        "participants",
        "predicted_total",
        "actual_total",
        "participant_signatures",
        "recorder_signature",
    ):
        value = supplied_mapping.get(key)
        if isinstance(value, str) and value.strip():
            mapping[key] = _validated_range(value)
    step_mapping = copy.deepcopy(mapping.get("steps") or {})
    supplied_step_mapping = supplied_mapping.get("steps") if isinstance(supplied_mapping.get("steps"), dict) else {}
    for key in (
        "header_row",
        "start_row",
        "end_row",
        "location_col",
        "content_col",
        "duration_col",
        "start_col",
        "end_col",
        "executor_col",
        "result_col",
    ):
        value = supplied_step_mapping.get(key)
        if value not in (None, ""):
            step_mapping[key] = value
    for key in ("header_row", "start_row", "end_row"):
        try:
            step_mapping[key] = int(step_mapping.get(key) or 0)
        except (TypeError, ValueError) as exc:
            raise DrillError("步骤表头或起止行配置无效。") from exc
    if (
        step_mapping["header_row"] < 1
        or step_mapping["start_row"] <= step_mapping["header_row"]
        or step_mapping["end_row"] < step_mapping["start_row"]
        or step_mapping["end_row"] > DRILL_MAX_SOURCE_ROWS
    ):
        raise DrillError("步骤表头和起止行顺序无效。")
    for key in (
        "location_col",
        "content_col",
        "duration_col",
        "start_col",
        "end_col",
        "executor_col",
        "result_col",
    ):
        column = str(step_mapping.get(key) or "").strip().upper()
        if not re.fullmatch(r"[A-Z]{1,3}", column) or _column_number(column) > 80:
            raise DrillError(f"步骤列配置无效：{key}")
        step_mapping[key] = column
    mapping["steps"] = step_mapping
    required_record = (
        "machine_room",
        "drill_date",
        "drill_name",
        "commander",
        "participants",
        "predicted_total",
        "actual_total",
        "participant_signatures",
        "recorder_signature",
    )
    missing_record = [key for key in required_record if not mapping.get(key)]
    if missing_record:
        raise DrillError("演练记录表映射不完整：" + "、".join(missing_record))

    assessment_mapping = copy.deepcopy(detected_assessment["mapping"])
    supplied_assessment = supplied_mapping.get("assessment") if isinstance(supplied_mapping.get("assessment"), dict) else {}
    for key, value in supplied_assessment.items():
        if key == "score_rows":
            continue
        if isinstance(value, str) and value.strip():
            assessment_mapping[key] = _validated_range(value)
    supplied_score_rows = supplied_assessment.get("score_rows")
    if isinstance(supplied_score_rows, list) and supplied_score_rows:
        score_rows = []
        for raw in supplied_score_rows:
            if not isinstance(raw, dict):
                continue
            value_cell = str(raw.get("value_cell") or "").replace("$", "").upper()
            score_cell = str(raw.get("score_cell") or "").replace("$", "").upper()
            _validated_range(value_cell)
            _validated_range(score_cell)
            score = _number(raw.get("score"))
            if score is None:
                score = _number((assessment.get("cells") or {}).get(value_cell))
            if score is None:
                raise DrillError(f"评估表 {value_cell} 缺少分值。")
            score_rows.append(
                {
                    "row": _cell_parts(value_cell)[0],
                    "value_cell": value_cell,
                    "score_cell": score_cell,
                    "score": score,
                }
            )
        assessment_mapping["score_rows"] = score_rows
    required_assessment = (
        "drill_name",
        "drill_date",
        "participants",
        "start_time",
        "end_time",
        "total_score",
        "score_rows",
    )
    missing_assessment = [key for key in required_assessment if not assessment_mapping.get(key)]
    if missing_assessment:
        raise DrillError("演练评估表映射不完整：" + "、".join(missing_assessment))
    assessment_total = sum(float(item.get("score") or 0) for item in assessment_mapping["score_rows"])
    if abs(assessment_total - 100.0) > 0.001:
        raise DrillError(f"评估表分值合计必须为 100，当前为 {assessment_total:g}。")

    supplied_steps = config.get("steps") if isinstance(config.get("steps"), list) else []
    supplied_slots: dict[int, int] = {}
    for raw in supplied_steps:
        if not isinstance(raw, dict):
            continue
        try:
            row = int(raw.get("row") or 0)
            slots = int(raw.get("signature_slots") or 0)
        except (TypeError, ValueError) as exc:
            raise DrillError("演练步骤行号或签名人数无效。") from exc
        if row in supplied_slots:
            raise DrillError(f"演练步骤第 {row} 行重复。")
        if row <= 0 or row > DRILL_MAX_SOURCE_ROWS:
            raise DrillError("演练步骤行号无效。")
        if slots not in range(1, DRILL_MAX_PARTICIPANTS + 1):
            raise DrillError(f"第 {row} 行步骤签名人数必须在 1–10 人之间。")
        supplied_slots[row] = slots

    detected_by_row = {int(item["row"]): item for item in detected_record["steps"]}
    normalized_steps: list[dict[str, Any]] = []
    for row in range(step_mapping["start_row"], step_mapping["end_row"] + 1):
        detected = detected_by_row.get(row) or {}
        cells = record.get("cells") or {}
        location = str(cells.get(f"{step_mapping['location_col']}{row}") or "").strip()
        content = str(cells.get(f"{step_mapping['content_col']}{row}") or "").strip()
        duration_text = str(cells.get(f"{step_mapping['duration_col']}{row}") or "").strip()
        if not any((location, content, duration_text)):
            continue
        slots = supplied_slots.get(
            row,
            int(detected.get("signature_slots") or 0)
            or _connector_slot_count(
                record, row, _column_number(step_mapping["executor_col"])
            ),
        )
        if slots not in range(1, DRILL_MAX_PARTICIPANTS + 1):
            raise DrillError(f"第 {row} 行步骤签名人数必须在 1–10 人之间。")
        if not content:
            raise DrillError(f"演练记录表第 {row} 行缺少步骤内容。")
        normalized_steps.append(
            {
                "row": row,
                "location": location,
                "content": content,
                "duration_text": duration_text,
                "duration_minutes": parse_duration_minutes(duration_text),
                "signature_slots": slots,
            }
        )
    if not normalized_steps:
        raise DrillError("演练记录表至少需要一个演练步骤。")
    predicted_cell = _cell_target(mapping["predicted_total"])
    predicted_text = str((record.get("cells") or {}).get(predicted_cell) or "").strip()
    if predicted_text:
        predicted_minutes = parse_duration_minutes(predicted_text)
        configured_minutes = sum(int(item["duration_minutes"]) for item in normalized_steps)
        if predicted_minutes != configured_minutes:
            raise DrillError(
                f"步骤预估时间合计为 {configured_minutes} 分钟，与预估总用时 {predicted_minutes} 分钟不一致。"
            )
    return {
        "record_sheet": record_sheet,
        "assessment_sheet": assessment_sheet,
        "mapping": {**mapping, "assessment": assessment_mapping},
        "steps": normalized_steps,
        "predicted_total_text": predicted_text,
    }


def _copy_limited(source: bytes | bytearray | BinaryIO, destination: Path) -> tuple[int, str]:
    stream: BinaryIO = io.BytesIO(bytes(source)) if isinstance(source, (bytes, bytearray)) else source
    if not hasattr(stream, "read"):
        raise DrillError("上传文件内容无效。")
    digest = hashlib.sha256()
    size = 0
    with destination.open("wb") as output:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            if not isinstance(chunk, (bytes, bytearray)):
                raise DrillError("上传文件必须是二进制内容。")
            size += len(chunk)
            if size > DRILL_MAX_XLSX_BYTES:
                raise DrillError("演练文件不能超过 64MB。")
            digest.update(chunk)
            output.write(chunk)
    if size <= 0:
        raise DrillError("上传文件为空。")
    return size, digest.hexdigest()


def _execution_key(drill_id: str, scope: str) -> str:
    return f"{drill_id}:{scope}"


def _person(value: Any) -> dict[str, str]:
    source = value if isinstance(value, dict) else {}
    return {
        key: str(source.get(key) or "").strip()
        for key in ("record_id", "name", "employee_no", "building", "open_id")
    }


def _cell_target(reference: str) -> str:
    row, col, _row2, _col2 = _range_bounds(reference)
    return _cell_ref(row, col)


def _derived_values(
    definition: dict[str, Any], execution: dict[str, Any], *, allow_incomplete: bool = False
) -> dict[str, Any]:
    configuration = definition["configuration"]
    mapping = configuration["mapping"]
    steps = configuration["steps"]
    commander = _person(execution.get("commander"))
    participants = [_person(item) for item in execution.get("participants") or []]
    timeline: list[dict[str, str]] = []
    drill_date_text = str(execution.get("drill_date") or "")
    try:
        drill_date = dt.date.fromisoformat(drill_date_text)
    except ValueError:
        drill_date = None
    try:
        timeline = build_time_chain(
            drill_date_text,
            str(execution.get("first_start_time") or ""),
            [int(item.get("duration_minutes") or 0) for item in steps],
        )
    except DrillError:
        if not allow_incomplete:
            raise
    record_values: dict[str, str | int | float] = {
        _cell_target(mapping["machine_room"]): f"南通机房{execution['scope']}楼",
        _cell_target(mapping["drill_name"]): str(definition.get("name") or ""),
        _cell_target(mapping["commander"]): "",
        _cell_target(mapping["participants"]): "",
    }
    if drill_date:
        record_values[_cell_target(mapping["drill_date"])] = (
            f"{drill_date.year}年{drill_date.month}月{drill_date.day}日"
        )
    step_mapping = mapping["steps"]
    for item, times in zip(steps, timeline):
        row = int(item["row"])
        record_values[f"{step_mapping['start_col']}{row}"] = times["start_time"]
        record_values[f"{step_mapping['end_col']}{row}"] = times["end_time"]
        record_values[f"{step_mapping['result_col']}{row}"] = "符合【 √ 】 不符【   】"
    total_minutes = sum(int(item["duration_minutes"]) for item in steps)
    total_text = str(configuration.get("predicted_total_text") or "").strip()
    if not total_text:
        total_text = f"{total_minutes // 60}时{total_minutes % 60:02d}分"
    if mapping.get("actual_total"):
        record_values[_cell_target(mapping["actual_total"])] = total_text
    assessment = mapping["assessment"]
    assessment_values: dict[str, str | int | float] = {
        _cell_target(assessment["drill_name"]): str(definition.get("name") or ""),
        _cell_target(assessment["participants"]): "、".join(item["name"] for item in participants),
        _cell_target(assessment["total_score"]): 100,
    }
    if drill_date:
        assessment_values[_cell_target(assessment["drill_date"])] = record_values[
            _cell_target(mapping["drill_date"])
        ]
    if timeline:
        assessment_values[_cell_target(assessment["start_time"])] = timeline[0]["start_time"]
        assessment_values[_cell_target(assessment["end_time"])] = timeline[-1]["end_time"]
    for score_row in assessment.get("score_rows") or []:
        assessment_values[str(score_row["score_cell"])] = score_row["score"]
    signature_cells = [
        {
            "range": mapping["commander"],
            "layout": "grid",
            "signers": [commander],
        },
        {
            "range": mapping["participants"],
            "layout": "grid",
            "signers": participants,
        },
        {
            "range": mapping["participant_signatures"],
            "layout": "grid",
            "signers": participants,
        },
        {
            "range": mapping["recorder_signature"],
            "layout": "grid",
            "signers": [commander],
        },
    ]
    step_signers = execution.get("step_signers") if isinstance(execution.get("step_signers"), dict) else {}
    participant_by_id = {item["record_id"]: item for item in participants}
    for step in steps:
        row = int(step["row"])
        signers = [participant_by_id.get(str(record_id or ""), {}) for record_id in step_signers.get(str(row), [])]
        signature_cells.append(
            {
                "range": f"{step_mapping['executor_col']}{row}",
                "layout": "vertical",
                "signers": [item for item in signers if item],
            }
        )
    return {
        "record_values": record_values,
        "assessment_values": assessment_values,
        "signature_cells": signature_cells,
        "timeline": timeline,
    }


def _validate_execution(definition: dict[str, Any], execution: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        dt.date.fromisoformat(str(execution.get("drill_date") or ""))
    except ValueError:
        errors.append("请选择有效的演练日期。")
    if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", str(execution.get("first_start_time") or "")):
        errors.append("请选择有效的第一步开始时间。")
    commander = _person(execution.get("commander"))
    if not commander["record_id"] or not commander["name"]:
        errors.append("请选择一名指挥人。")
    participants = [_person(item) for item in execution.get("participants") or []]
    ids = [item["record_id"] for item in participants if item["record_id"]]
    if not commander["record_id"] or not ids or ids[0] != commander["record_id"]:
        errors.append("指挥人必须排在参演人员第一位。")
    if len(ids) != len(set(ids)) or len(ids) > DRILL_MAX_PARTICIPANTS:
        errors.append("参演人员不能重复且最多选择 10 人。")
    selected = set(ids)
    step_signers = execution.get("step_signers") if isinstance(execution.get("step_signers"), dict) else {}
    for step in definition.get("configuration", {}).get("steps") or []:
        row = int(step.get("row") or 0)
        assigned = [str(item or "") for item in step_signers.get(str(row), []) if str(item or "")]
        if len(assigned) != int(step.get("signature_slots") or 0):
            errors.append(f"第 {row} 行步骤需要选择 {int(step.get('signature_slots') or 0)} 名执行人。")
            continue
        if len(assigned) != len(set(assigned)) or any(item not in selected for item in assigned):
            errors.append(f"第 {row} 行步骤执行人必须来自参演人员且不能重复。")
        if "ECC" in str(step.get("location") or "").upper() and assigned and assigned[0] != commander["record_id"]:
            errors.append(f"第 {row} 行包含 ECC，指挥人必须位于第一个签名位。")
    return errors


def _row_col_pixels(sheet: dict[str, Any], reference: str) -> tuple[int, int, int, int, float, float]:
    row1, col1, row2, col2 = _range_bounds(reference)
    widths = sheet.get("column_widths") or {}
    heights = sheet.get("row_heights") or {}
    width = sum(max(12.0, float(widths.get(col, sheet.get("default_col_width") or 8.43)) * 7.0 + 5.0) for col in range(col1, col2 + 1))
    height = sum(max(12.0, float(heights.get(row, sheet.get("default_row_height") or 15.0)) * 96.0 / 72.0) for row in range(row1, row2 + 1))
    return row1, col1, row2, col2, width, height


def _marker_pixels(sheet: dict[str, Any], marker: ET.Element) -> tuple[int, int, float, float]:
    col_zero = int(marker.findtext(f"{{{_XDR_NS}}}col") or 0)
    row_zero = int(marker.findtext(f"{{{_XDR_NS}}}row") or 0)
    col_offset = int(marker.findtext(f"{{{_XDR_NS}}}colOff") or 0) / _EMU_PER_PIXEL
    row_offset = int(marker.findtext(f"{{{_XDR_NS}}}rowOff") or 0) / _EMU_PER_PIXEL
    widths = sheet.get("column_widths") or {}
    heights = sheet.get("row_heights") or {}
    x = sum(
        max(
            12.0,
            float(widths.get(col, sheet.get("default_col_width") or 8.43)) * 7.0
            + 5.0,
        )
        for col in range(1, col_zero + 1)
    ) + col_offset
    y = sum(
        max(
            12.0,
            float(heights.get(row, sheet.get("default_row_height") or 15.0))
            * 96.0
            / 72.0,
        )
        for row in range(1, row_zero + 1)
    ) + row_offset
    return row_zero, col_zero, x, y


def _drawing_preview_images(
    archive: zipfile.ZipFile,
    drawing_path: str,
    drawing_root: ET.Element,
    relationship_root: ET.Element,
    sheet: dict[str, Any],
) -> tuple[list[dict[str, Any]], bool]:
    relationships = {
        str(item.attrib.get("Id") or ""): item
        for item in relationship_root.findall(f"{{{_PKG_REL_NS}}}Relationship")
    }
    images: list[dict[str, Any]] = []
    total_size = 0
    truncated = False
    for anchor in list(drawing_root):
        picture = anchor.find(f"{{{_XDR_NS}}}pic")
        start = anchor.find(f"{{{_XDR_NS}}}from")
        if picture is None or start is None:
            continue
        blip = picture.find(f".//{{{_A_NS}}}blip")
        rel_id = (
            str(blip.attrib.get(f"{{{_DOC_REL_NS}}}embed") or "")
            if blip is not None
            else ""
        )
        relationship = relationships.get(rel_id)
        if relationship is None or not str(
            relationship.attrib.get("Type") or ""
        ).endswith("/image"):
            continue
        target = _resolve_zip_target(
            drawing_path, str(relationship.attrib.get("Target") or "")
        )
        if target not in archive.namelist():
            continue
        properties = picture.find(f".//{{{_XDR_NS}}}cNvPr")
        picture_name = (
            str(properties.attrib.get("name") or "")
            if properties is not None
            else ""
        )
        if picture_name.startswith("演练签名-") or Path(target).name.startswith(
            "drill_signature_"
        ):
            continue
        content = archive.read(target)
        if (
            len(images) >= DRILL_MAX_PREVIEW_IMAGES
            or len(content) > DRILL_MAX_PREVIEW_IMAGE_BYTES
            or total_size + len(content) > DRILL_MAX_PREVIEW_IMAGES_BYTES
        ):
            truncated = True
            continue
        if content.startswith(b"\x89PNG\r\n\x1a\n"):
            mime_type = "image/png"
        elif content.startswith(b"\xff\xd8\xff"):
            mime_type = "image/jpeg"
        elif content.startswith((b"GIF87a", b"GIF89a")):
            mime_type = "image/gif"
        else:
            truncated = True
            continue
        start_row, start_col, start_x, start_y = _marker_pixels(sheet, start)
        end = anchor.find(f"{{{_XDR_NS}}}to")
        extent = anchor.find(f"{{{_XDR_NS}}}ext")
        if end is not None:
            end_row, end_col, end_x, end_y = _marker_pixels(sheet, end)
            width = max(1.0, end_x - start_x)
            height = max(1.0, end_y - start_y)
        elif extent is not None:
            end_row, end_col = start_row, start_col
            width = max(1.0, int(extent.attrib.get("cx") or 0) / _EMU_PER_PIXEL)
            height = max(1.0, int(extent.attrib.get("cy") or 0) / _EMU_PER_PIXEL)
        else:
            truncated = True
            continue
        images.append(
            {
                "name": picture_name or Path(target).name,
                "range": (
                    f"{_cell_ref(start_row + 1, start_col + 1)}:"
                    f"{_cell_ref(end_row + 1, end_col + 1)}"
                ),
                "row": start_row,
                "col": start_col,
                "x_offset_px": round(
                    int(start.findtext(f"{{{_XDR_NS}}}colOff") or 0)
                    / _EMU_PER_PIXEL,
                    2,
                ),
                "y_offset_px": round(
                    int(start.findtext(f"{{{_XDR_NS}}}rowOff") or 0)
                    / _EMU_PER_PIXEL,
                    2,
                ),
                "width_px": round(width),
                "height_px": round(height),
                "data_url": "data:"
                + mime_type
                + ";base64,"
                + base64.b64encode(content).decode("ascii"),
            }
        )
        total_size += len(content)
    return images, truncated


def _anchor_from_offset(
    sheet: dict[str, Any], row: int, col: int, x_px: float, y_px: float
) -> tuple[int, int, float, float]:
    widths = sheet.get("column_widths") or {}
    heights = sheet.get("row_heights") or {}
    while x_px > 0:
        width = max(
            12.0,
            float(widths.get(col, sheet.get("default_col_width") or 8.43)) * 7.0
            + 5.0,
        )
        if x_px < width:
            break
        x_px -= width
        col += 1
    while y_px > 0:
        height = max(
            12.0,
            float(heights.get(row, sheet.get("default_row_height") or 15.0))
            * 96.0
            / 72.0,
        )
        if y_px < height:
            break
        y_px -= height
        row += 1
    return row, col, x_px, y_px


def _image_dimensions(content: bytes) -> tuple[int, int]:
    try:
        from PIL import Image

        with Image.open(io.BytesIO(content)) as image:
            image.verify()
            width, height = image.size
    except Exception as exc:
        raise DrillError("签名图片无法读取，请重新签名。") from exc
    if width <= 0 or height <= 0:
        raise DrillError("签名图片尺寸无效。")
    return width, height


def _horizontal_signature_layout(
    image_sizes: list[tuple[int, int]],
    area_width: float,
    area_height: float,
) -> list[tuple[float, float, float, float]]:
    if not image_sizes:
        return []
    padding = min(4.0, area_width * 0.02, area_height * 0.08)
    gap = min(
        3.0,
        max(0.0, (area_width - padding * 2) / (len(image_sizes) * 10)),
    )
    max_height = max(1.0, area_height - padding * 2)
    base_sizes = []
    for source_width, source_height in image_sizes:
        scale = min(max_height / source_height, 120.0 / source_width)
        base_sizes.append((source_width * scale, source_height * scale))
    available_width = max(
        1.0,
        area_width - padding * 2 - gap * (len(base_sizes) - 1),
    )
    shrink = min(1.0, available_width / sum(width for width, _height in base_sizes))
    x = padding
    placements = []
    for width, height in base_sizes:
        width *= shrink
        height *= shrink
        placements.append((x, (area_height - height) / 2, width, height))
        x += width + gap
    return placements


def normalize_drill_signature_png(content: bytes) -> bytes:
    content = bytes(content or b"")
    if not content or len(content) > DRILL_MAX_SIGNATURE_BYTES:
        raise DrillError("签名图片为空或超过 2MB。")
    try:
        from PIL import Image, ImageOps

        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(content)) as source:
                width, height = source.size
                if (
                    width <= 0
                    or height <= 0
                    or width > DRILL_MAX_SIGNATURE_DIMENSION
                    or height > DRILL_MAX_SIGNATURE_DIMENSION
                    or width * height > DRILL_MAX_SIGNATURE_PIXELS
                ):
                    raise DrillError("签名图片尺寸过大。")
                image = ImageOps.exif_transpose(source).convert("RGBA")
                image.load()
                output = io.BytesIO()
                image.save(output, format="PNG", optimize=True)
                return output.getvalue()
    except DrillError:
        raise
    except Exception as exc:
        raise DrillError("签名图片无法读取，请重新签名。") from exc


def _next_rid(root: ET.Element) -> str:
    numbers = [
        int(match.group(1))
        for item in list(root)
        if (match := re.fullmatch(r"rId(\d+)", str(item.attrib.get("Id") or "")))
    ]
    return f"rId{max(numbers or [0]) + 1}"


def _ensure_drawing_parts(
    replacements: dict[str, bytes],
    additions: dict[str, bytes],
    archive: zipfile.ZipFile,
    sheet: dict[str, Any],
    sheet_root: ET.Element,
) -> tuple[str, ET.Element, str, ET.Element]:
    names = set(archive.namelist()) | set(additions)
    sheet_path = str(sheet["path"])
    sheet_rels_path = _sheet_relationship_path(sheet_path)
    if sheet_rels_path in replacements:
        sheet_rels_root = ET.fromstring(replacements[sheet_rels_path])
    elif sheet_rels_path in names:
        sheet_rels_root = ET.fromstring(archive.read(sheet_rels_path))
    else:
        sheet_rels_root = ET.Element(f"{{{_PKG_REL_NS}}}Relationships")
    drawing_node = sheet_root.find(f"{{{_MAIN_NS}}}drawing")
    drawing_path = str(sheet.get("drawing_path") or "")
    if not drawing_path:
        if drawing_node is not None:
            raise DrillError("演练记录表的绘图关系已损坏。")
        indexes = [
            int(match.group(1))
            for name in names
            if (match := re.fullmatch(r"xl/drawings/drawing(\d+)\.xml", name))
        ]
        drawing_path = f"xl/drawings/drawing{max(indexes or [0]) + 1}.xml"
        rel_id = _next_rid(sheet_rels_root)
        ET.SubElement(
            sheet_rels_root,
            f"{{{_PKG_REL_NS}}}Relationship",
            {
                "Id": rel_id,
                "Type": f"{_DOC_REL_NS}/drawing",
                "Target": posixpath.relpath(drawing_path, posixpath.dirname(sheet_path)),
            },
        )
        drawing_node = ET.Element(f"{{{_MAIN_NS}}}drawing", {f"{{{_DOC_REL_NS}}}id": rel_id})
        following_tags = {
            f"{{{_MAIN_NS}}}{name}"
            for name in (
                "legacyDrawing",
                "legacyDrawingHF",
                "picture",
                "oleObjects",
                "controls",
                "webPublishItems",
                "tableParts",
                "extLst",
            )
        }
        following = next(
            (child for child in list(sheet_root) if child.tag in following_tags),
            None,
        )
        if following is None:
            sheet_root.append(drawing_node)
        else:
            sheet_root.insert(list(sheet_root).index(following), drawing_node)
        drawing_root = ET.Element(f"{{{_XDR_NS}}}wsDr")
    else:
        drawing_root = ET.fromstring(
            replacements.get(drawing_path) or archive.read(drawing_path)
        )
    drawing_rels_path = _drawing_relationship_path(drawing_path)
    if drawing_rels_path in replacements:
        drawing_rels_root = ET.fromstring(replacements[drawing_rels_path])
    elif drawing_rels_path in names:
        drawing_rels_root = ET.fromstring(archive.read(drawing_rels_path))
    else:
        drawing_rels_root = ET.Element(f"{{{_PKG_REL_NS}}}Relationships")
    replacements[sheet_rels_path] = ET.tostring(sheet_rels_root, encoding="utf-8", xml_declaration=True)
    return drawing_path, drawing_root, drawing_rels_path, drawing_rels_root


def _set_inline_text(sheet_root: ET.Element, reference: str, value: Any) -> None:
    row_number, col_number = _cell_parts(reference)
    sheet_data = sheet_root.find(f"{{{_MAIN_NS}}}sheetData")
    if sheet_data is None:
        sheet_data = ET.SubElement(sheet_root, f"{{{_MAIN_NS}}}sheetData")
    row = next((item for item in sheet_data.findall(f"{{{_MAIN_NS}}}row") if int(item.attrib.get("r") or 0) == row_number), None)
    if row is None:
        row = ET.Element(f"{{{_MAIN_NS}}}row", {"r": str(row_number)})
        before = next((item for item in list(sheet_data) if int(item.attrib.get("r") or 0) > row_number), None)
        sheet_data.insert(list(sheet_data).index(before), row) if before is not None else sheet_data.append(row)
    cell = next((item for item in row.findall(f"{{{_MAIN_NS}}}c") if str(item.attrib.get("r") or "").upper() == reference.upper()), None)
    if cell is None:
        cell = ET.Element(f"{{{_MAIN_NS}}}c", {"r": reference.upper()})
        before = next((item for item in list(row) if _column_number(str(item.attrib.get("r") or "")) > col_number), None)
        row.insert(list(row).index(before), cell) if before is not None else row.append(cell)
    for child in list(cell):
        if child.tag in {f"{{{_MAIN_NS}}}f", f"{{{_MAIN_NS}}}v", f"{{{_MAIN_NS}}}is"}:
            cell.remove(child)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        cell.attrib.pop("t", None)
        ET.SubElement(cell, f"{{{_MAIN_NS}}}v").text = f"{value:g}"
        return
    cell.attrib["t"] = "inlineStr"
    inline = ET.SubElement(cell, f"{{{_MAIN_NS}}}is")
    text_node = ET.SubElement(inline, f"{{{_MAIN_NS}}}t")
    text = str(value if value is not None else "")
    if text[:1].isspace() or text[-1:].isspace():
        text_node.attrib["{http://www.w3.org/XML/1998/namespace}space"] = "preserve"
    text_node.text = text


def _append_signature_anchor(
    drawing_root: ET.Element,
    *,
    rel_id: str,
    shape_id: int,
    name: str,
    row: int,
    col: int,
    col_offset_px: float,
    row_offset_px: float,
    width_px: float,
    height_px: float,
) -> None:
    anchor = ET.SubElement(drawing_root, f"{{{_XDR_NS}}}oneCellAnchor")
    marker = ET.SubElement(anchor, f"{{{_XDR_NS}}}from")
    for tag, value in (
        ("col", col - 1),
        ("colOff", int(col_offset_px * _EMU_PER_PIXEL)),
        ("row", row - 1),
        ("rowOff", int(row_offset_px * _EMU_PER_PIXEL)),
    ):
        ET.SubElement(marker, f"{{{_XDR_NS}}}{tag}").text = str(value)
    ET.SubElement(
        anchor,
        f"{{{_XDR_NS}}}ext",
        {"cx": str(int(width_px * _EMU_PER_PIXEL)), "cy": str(int(height_px * _EMU_PER_PIXEL))},
    )
    picture = ET.SubElement(anchor, f"{{{_XDR_NS}}}pic")
    nv = ET.SubElement(picture, f"{{{_XDR_NS}}}nvPicPr")
    ET.SubElement(nv, f"{{{_XDR_NS}}}cNvPr", {"id": str(shape_id), "name": name})
    ET.SubElement(nv, f"{{{_XDR_NS}}}cNvPicPr")
    fill = ET.SubElement(picture, f"{{{_XDR_NS}}}blipFill")
    ET.SubElement(fill, f"{{{_A_NS}}}blip", {f"{{{_DOC_REL_NS}}}embed": rel_id})
    stretch = ET.SubElement(fill, f"{{{_A_NS}}}stretch")
    ET.SubElement(stretch, f"{{{_A_NS}}}fillRect")
    props = ET.SubElement(picture, f"{{{_XDR_NS}}}spPr")
    transform = ET.SubElement(props, f"{{{_A_NS}}}xfrm")
    ET.SubElement(transform, f"{{{_A_NS}}}off", {"x": "0", "y": "0"})
    ET.SubElement(transform, f"{{{_A_NS}}}ext", {"cx": str(int(width_px * _EMU_PER_PIXEL)), "cy": str(int(height_px * _EMU_PER_PIXEL))})
    geometry = ET.SubElement(props, f"{{{_A_NS}}}prstGeom", {"prst": "rect"})
    ET.SubElement(geometry, f"{{{_A_NS}}}avLst")
    ET.SubElement(anchor, f"{{{_XDR_NS}}}clientData")


def _patch_workbook(
    source_path: Path,
    output_path: Path,
    definition: dict[str, Any],
    execution: dict[str, Any],
    derived: dict[str, Any],
    signatures: dict[str, bytes],
) -> None:
    workbook = _parse_workbook(source_path)
    configuration = definition["configuration"]
    record_sheet = _find_sheet(workbook, configuration["record_sheet"])
    assessment_sheet = _find_sheet(workbook, configuration["assessment_sheet"])
    replacements: dict[str, bytes] = {}
    additions: dict[str, bytes] = {}
    with zipfile.ZipFile(source_path) as archive:
        archive_names = set(archive.namelist())
        sheet_roots: dict[str, ET.Element] = {
            record_sheet["path"]: ET.fromstring(archive.read(record_sheet["path"])),
            assessment_sheet["path"]: ET.fromstring(archive.read(assessment_sheet["path"])),
        }
        for reference, value in (derived.get("record_values") or {}).items():
            _set_inline_text(sheet_roots[record_sheet["path"]], reference, value)
        for reference, value in (derived.get("assessment_values") or {}).items():
            _set_inline_text(sheet_roots[assessment_sheet["path"]], reference, value)
        drawing_path, drawing_root, drawing_rels_path, drawing_rels_root = _ensure_drawing_parts(
            replacements, additions, archive, record_sheet, sheet_roots[record_sheet["path"]]
        )
        existing_ids = [
            int(item.attrib.get("id") or 0)
            for item in drawing_root.findall(f".//{{{_XDR_NS}}}cNvPr")
            if str(item.attrib.get("id") or "").isdigit()
        ]
        shape_id = max(existing_ids or [0]) + 1
        image_relations: dict[str, str] = {}
        for placement in derived.get("signature_cells") or []:
            signers = [item for item in placement.get("signers") or [] if str(item.get("record_id") or "")]
            if not signers:
                continue
            row1, col1, _row2, _col2, area_width, area_height = _row_col_pixels(record_sheet, str(placement["range"]))
            count = len(signers)
            prepared = []
            for signer in signers:
                record_id = str(signer.get("record_id") or "")
                content = signatures.get(record_id)
                if not content:
                    raise DrillError(f"{signer.get('name') or record_id}缺少可用签名。")
                prepared.append((signer, content, _image_dimensions(content)))
            if placement.get("layout") == "vertical":
                slot_height = area_height / count
                positions = []
                for index, (_signer, _content, (source_width, source_height)) in enumerate(prepared):
                    padding = min(5.0, area_width * 0.08, slot_height * 0.08)
                    scale = min(
                        max(1.0, area_width - padding * 2) / source_width,
                        max(1.0, slot_height - padding * 2) / source_height,
                    )
                    width, height = source_width * scale, source_height * scale
                    positions.append(
                        (
                            (area_width - width) / 2,
                            index * slot_height + (slot_height - height) / 2,
                            width,
                            height,
                        )
                    )
            else:
                positions = _horizontal_signature_layout(
                    [size for _signer, _content, size in prepared],
                    area_width,
                    area_height,
                )
            for (signer, content, _source_size), (x, y, width, height) in zip(
                prepared, positions
            ):
                digest = hashlib.sha256(content).hexdigest()
                media_name = f"xl/media/drill_signature_{digest}.png"
                suffix = 0
                while media_name in archive_names or media_name in additions:
                    existing = (
                        additions.get(media_name)
                        if media_name in additions
                        else archive.read(media_name)
                    )
                    if existing == content:
                        break
                    suffix += 1
                    media_name = (
                        f"xl/media/drill_signature_{digest}_{suffix}.png"
                    )
                if media_name not in archive_names:
                    additions.setdefault(media_name, content)
                rel_id = image_relations.get(digest)
                if not rel_id:
                    rel_id = _next_rid(drawing_rels_root)
                    ET.SubElement(
                        drawing_rels_root,
                        f"{{{_PKG_REL_NS}}}Relationship",
                        {
                            "Id": rel_id,
                            "Type": f"{_DOC_REL_NS}/image",
                            "Target": posixpath.relpath(media_name, posixpath.dirname(drawing_path)),
                        },
                    )
                    image_relations[digest] = rel_id
                anchor_row, anchor_col, x, y = _anchor_from_offset(
                    record_sheet, row1, col1, x, y
                )
                _append_signature_anchor(
                    drawing_root,
                    rel_id=rel_id,
                    shape_id=shape_id,
                    name=f"演练签名-{shape_id}",
                    row=anchor_row,
                    col=anchor_col,
                    col_offset_px=x,
                    row_offset_px=y,
                    width_px=width,
                    height_px=height,
                )
                shape_id += 1
        for sheet_path, root in sheet_roots.items():
            replacements[sheet_path] = ET.tostring(root, encoding="utf-8", xml_declaration=True)
        replacements[drawing_path] = ET.tostring(drawing_root, encoding="utf-8", xml_declaration=True)
        replacements[drawing_rels_path] = ET.tostring(drawing_rels_root, encoding="utf-8", xml_declaration=True)
        content_types = ET.fromstring(archive.read("[Content_Types].xml"))
        if not any(
            item.attrib.get("Extension", "").lower() == "png"
            for item in content_types.findall(f"{{{_CT_NS}}}Default")
        ):
            ET.SubElement(content_types, f"{{{_CT_NS}}}Default", {"Extension": "png", "ContentType": "image/png"})
        if not any(
            item.attrib.get("PartName") == f"/{drawing_path}"
            for item in content_types.findall(f"{{{_CT_NS}}}Override")
        ):
            ET.SubElement(
                content_types,
                f"{{{_CT_NS}}}Override",
                {"PartName": f"/{drawing_path}", "ContentType": "application/vnd.openxmlformats-officedocument.drawing+xml"},
            )
        replacements["[Content_Types].xml"] = ET.tostring(content_types, encoding="utf-8", xml_declaration=True)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with zipfile.ZipFile(temporary, "w", allowZip64=True) as output:
                for info in archive.infolist():
                    name = str(info.filename)
                    output.writestr(info, replacements.pop(name, archive.read(name)))
                for name, content in {**replacements, **additions}.items():
                    output.writestr(name, content, compress_type=zipfile.ZIP_DEFLATED)
            _validate_xlsx_archive(temporary)
            os.replace(temporary, output_path)
        finally:
            if temporary.exists():
                temporary.unlink()


def _generated_value_matches(actual: Any, expected: Any) -> bool:
    if isinstance(expected, (int, float)) and not isinstance(expected, bool):
        try:
            return abs(float(actual) - float(expected)) <= 0.000001
        except (TypeError, ValueError):
            return False
    return str(actual if actual is not None else "") == str(
        expected if expected is not None else ""
    )


def _verify_generated_workbook(
    output_path: Path,
    definition: dict[str, Any],
    derived: dict[str, Any],
    signatures: dict[str, bytes],
) -> None:
    workbook = _parse_workbook(output_path)
    configuration = definition["configuration"]
    for sheet_name, values in (
        (configuration["record_sheet"], derived.get("record_values") or {}),
        (
            configuration["assessment_sheet"],
            derived.get("assessment_values") or {},
        ),
    ):
        sheet = _find_sheet(workbook, str(sheet_name or ""))
        for reference, expected in values.items():
            actual = (sheet.get("cells") or {}).get(str(reference).upper())
            if not _generated_value_matches(actual, expected):
                raise DrillError(
                    f"生成文件回读校验失败：{sheet_name}!{reference}。"
                )

    record_sheet = _find_sheet(workbook, configuration["record_sheet"])
    drawing_path = str(record_sheet.get("drawing_path") or "")
    expected_anchor_count = sum(
        len(item.get("signers") or [])
        for item in derived.get("signature_cells") or []
    )
    expected_hashes = {
        hashlib.sha256(signatures[record_id]).hexdigest()
        for record_id in {
            str(person.get("record_id") or "")
            for item in derived.get("signature_cells") or []
            for person in item.get("signers") or []
            if str(person.get("record_id") or "")
        }
    }
    with zipfile.ZipFile(output_path) as archive:
        names = set(archive.namelist())
        drawing_rels_path = _drawing_relationship_path(drawing_path)
        if not drawing_path or drawing_path not in names or drawing_rels_path not in names:
            raise DrillError("生成文件缺少签名绘图关系。")
        drawing_root = ET.fromstring(archive.read(drawing_path))
        relationship_root = ET.fromstring(archive.read(drawing_rels_path))
        relationships = {
            str(item.attrib.get("Id") or ""): item
            for item in relationship_root.findall(
                f"{{{_PKG_REL_NS}}}Relationship"
            )
        }
        generated_anchors = []
        for anchor in list(drawing_root):
            properties = anchor.find(f".//{{{_XDR_NS}}}cNvPr")
            if properties is not None and str(
                properties.attrib.get("name") or ""
            ).startswith("演练签名-"):
                generated_anchors.append(anchor)
        if len(generated_anchors) != expected_anchor_count:
            raise DrillError("生成文件中的签名图片数量不正确。")
        used_relationships = set()
        actual_hashes = set()
        for anchor in generated_anchors:
            blip = anchor.find(f".//{{{_A_NS}}}blip")
            rel_id = (
                str(blip.attrib.get(f"{{{_DOC_REL_NS}}}embed") or "")
                if blip is not None
                else ""
            )
            relationship = relationships.get(rel_id)
            if (
                not rel_id
                or relationship is None
                or not str(relationship.attrib.get("Type") or "").endswith(
                    "/image"
                )
            ):
                raise DrillError("生成文件中的签名图片关系无效。")
            target = _resolve_zip_target(
                drawing_path, str(relationship.attrib.get("Target") or "")
            )
            if target not in names:
                raise DrillError("生成文件中的签名图片文件缺失。")
            content = archive.read(target)
            if not content.startswith(b"\x89PNG\r\n\x1a\n"):
                raise DrillError("生成文件中的签名图片不是有效 PNG。")
            used_relationships.add(rel_id)
            actual_hashes.add(hashlib.sha256(content).hexdigest())
        if len(used_relationships) != len(expected_hashes) or actual_hashes != expected_hashes:
            raise DrillError("生成文件中的签名图片或关系数量不正确。")


def _sheet_preview_model(sheet: dict[str, Any], *, sheet_type: str, derived: dict[str, Any], generated_current: bool) -> dict[str, Any]:
    values = dict(sheet.get("cells") or {})
    values.update(derived.get(f"{sheet_type}_values") or {})
    significant = [
        _cell_parts(reference)
        for reference, value in values.items()
        if str(value or "").strip()
    ]
    merge_bounds = [_range_bounds(reference) for reference in sheet.get("merges") or []]
    row_count = min(
        max([row for row, _col in significant] + [item[2] for item in merge_bounds] + [1]),
        500,
    )
    col_count = min(
        max([col for _row, col in significant] + [item[3] for item in merge_bounds] + [1]),
        80,
    )
    rows = [
        [str(values.get(_cell_ref(row, col)) or "") for col in range(1, col_count + 1)]
        for row in range(1, row_count + 1)
    ]
    merges = []
    for reference in sheet.get("merges") or []:
        row1, col1, row2, col2 = _range_bounds(reference)
        merges.append({"row": row1 - 1, "col": col1 - 1, "rowspan": row2 - row1 + 1, "colspan": col2 - col1 + 1})
    signature_cells = [
        {
            "range": item["range"],
            "layout": item.get("layout"),
            "width_px": round(_row_col_pixels(sheet, str(item["range"]))[4]),
            "height_px": round(_row_col_pixels(sheet, str(item["range"]))[5]),
            "signers": [
                {key: str(person.get(key) or "") for key in ("record_id", "name")}
                for person in item.get("signers") or []
            ],
        }
        for item in derived.get("signature_cells") or []
    ] if sheet_type == "record" else []
    cell_styles = {
        reference: copy.deepcopy(style)
        for reference, style in (sheet.get("cell_styles") or {}).items()
        if _cell_parts(reference)[0] <= row_count
        and _cell_parts(reference)[1] <= col_count
    }
    images = [
        copy.deepcopy(item)
        for item in sheet.get("preview_images") or []
        if int(item.get("row") or 0) < row_count
        and int(item.get("col") or 0) < col_count
    ]
    _row1, _col1, _row2, _col2, sheet_width, sheet_height = _row_col_pixels(
        sheet,
        f"A1:{_cell_ref(row_count, col_count)}",
    )
    return {
        "sheet_type": sheet_type,
        "sheet_name": sheet.get("name"),
        "orientation": "portrait" if sheet_type == "record" else "landscape",
        "row_count": row_count,
        "column_count": col_count,
        "rows": rows,
        "merges": merges,
        "default_row_height_px": round(float(sheet.get("default_row_height") or 15.0) * 96 / 72),
        "row_heights": {str(row - 1): round(float(height) * 96 / 72) for row, height in (sheet.get("row_heights") or {}).items()},
        "column_widths": {str(col - 1): round(float(width) * 7 + 5) for col, width in (sheet.get("column_widths") or {}).items()},
        "sheet_width_px": round(sheet_width),
        "sheet_height_px": round(sheet_height),
        "cell_styles": cell_styles,
        "images": images,
        "images_truncated": bool(sheet.get("preview_images_truncated")),
        "signature_cells": signature_cells,
        "generated_current": bool(generated_current),
    }


class DrillManagementService:
    # ponytail: one lock keeps template/execution generation atomic; split per drill if generation throughput grows.
    _lock = threading.RLock()

    def __init__(self, state_store: Any, *, data_root: Path | str | None = None) -> None:
        self.state_store = state_store
        if data_root is None:
            from upload_event_module.utils import get_data_file_path

            data_root = get_data_file_path("drill_management")
        self.data_root = Path(data_root).resolve()

    def _definition_directory(self, definition: dict[str, Any]) -> Path:
        year, month = _normalize_month(definition.get("year"), definition.get("month"))
        drill_id = str(definition.get("drill_id") or "")
        if not re.fullmatch(r"[a-f0-9]{32}", drill_id):
            raise DrillError("演练 ID 无效。")
        path = (self.data_root / f"{year:04d}-{month:02d}" / drill_id).resolve()
        if path == self.data_root or not path.is_relative_to(self.data_root):
            raise DrillError("演练文件路径无效。")
        return path

    def _source_path(self, definition: dict[str, Any]) -> Path:
        path = Path(str((definition.get("source") or {}).get("path") or "")).resolve()
        directory = self._definition_directory(definition)
        if not path.is_file() or not path.is_relative_to(directory):
            raise DrillNotFoundError("演练源文件不存在。")
        return path

    def list_definitions(self, month: str = "", *, include_archived: bool = False) -> list[dict[str, Any]]:
        normalized_month = str(month or "").strip()
        execution_ids = {
            str((document.get("payload") or {}).get("drill_id") or "")
            for document in self.state_store.list_documents(DRILL_EXECUTION_NAMESPACE)
            if isinstance(document.get("payload"), dict)
        }
        items = []
        for document in self.state_store.list_documents(DRILL_DEFINITION_NAMESPACE):
            item = document.get("payload") if isinstance(document, dict) else None
            if not isinstance(item, dict):
                continue
            if not include_archived and str(item.get("status") or "") == "archived":
                continue
            marker = f"{int(item.get('year') or 0):04d}-{int(item.get('month') or 0):02d}"
            if normalized_month and marker != normalized_month:
                continue
            result = copy.deepcopy(item)
            result["has_executions"] = str(result.get("drill_id") or "") in execution_ids
            result["configuration_locked"] = result["has_executions"]
            items.append(result)
        return sorted(items, key=lambda item: (int(item.get("year") or 0), int(item.get("month") or 0), str(item.get("created_at") or "")), reverse=True)

    def get_definition(self, drill_id: str) -> dict[str, Any]:
        item = self.state_store.get_document(DRILL_DEFINITION_NAMESPACE, str(drill_id or "").strip())
        if not isinstance(item, dict):
            raise DrillNotFoundError("演练不存在。")
        result = copy.deepcopy(item)
        result["has_executions"] = self._has_execution(str(result.get("drill_id") or ""))
        result["configuration_locked"] = result["has_executions"]
        return result

    def create_definition(
        self,
        *,
        name: str,
        year: Any,
        month: Any,
        file_name: str,
        source: bytes | bytearray | BinaryIO,
        actor: str = "",
    ) -> dict[str, Any]:
        title = str(name or "").strip()
        normalized_year, normalized_month = _normalize_month(year, month)
        if not title or len(title) > 200:
            raise DrillError("演练名称不能为空且不能超过 200 个字符。")
        safe_name = _safe_file_name(file_name, "演练模板.xlsx")
        if Path(safe_name).suffix.lower() != ".xlsx":
            raise DrillError("演练模板只支持 .xlsx 文件。")
        drill_id = uuid.uuid4().hex
        definition_stub = {"drill_id": drill_id, "year": normalized_year, "month": normalized_month}
        directory = self._definition_directory(definition_stub)
        directory.mkdir(parents=True, exist_ok=False)
        temporary = directory / f".{uuid.uuid4().hex}.upload.xlsx"
        source_path = directory / "source.xlsx"
        try:
            size, digest = _copy_limited(source, temporary)
            _validate_xlsx_archive(temporary)
            workbook = _parse_workbook(temporary)
            detection_error = ""
            try:
                configuration = detect_drill_configuration(workbook)
            except DrillError as exc:
                configuration = {
                    "record_sheet": "",
                    "assessment_sheet": "",
                    "mapping": {},
                    "steps": [],
                }
                detection_error = str(exc)
            os.replace(temporary, source_path)
            now = _now_text()
            definition = {
                "drill_id": drill_id,
                "name": title,
                "year": normalized_year,
                "month": normalized_month,
                "status": "draft",
                "version": 1,
                "source": {"path": str(source_path), "name": safe_name, "size": size, "sha256": digest},
                "sheets": [{"name": str(item.get("name") or "")} for item in workbook.get("sheets") or []],
                "configuration": configuration,
                "detection_error": detection_error,
                "created_at": now,
                "created_by": str(actor or ""),
                "updated_at": now,
                "updated_by": str(actor or ""),
            }
            self.state_store.put_document(DRILL_DEFINITION_NAMESPACE, drill_id, definition)
            return copy.deepcopy(definition)
        except Exception:
            with suppress(OSError):
                shutil.rmtree(directory)
            raise

    def save_configuration(self, drill_id: str, configuration: dict[str, Any], *, expected_version: int, actor: str = "") -> dict[str, Any]:
        with self._lock:
            definition = self.get_definition(drill_id)
            if int(expected_version or 0) != int(definition.get("version") or 0):
                raise DrillConflictError("演练配置已被其他用户修改，请刷新后重试。")
            if self._has_execution(drill_id):
                raise DrillConflictError("该演练已有楼栋执行数据，不能再修改模板配置。")
            workbook = _parse_workbook(self._source_path(definition))
            definition["configuration"] = _validate_configuration(workbook, configuration)
            definition["version"] = int(definition.get("version") or 0) + 1
            definition["status"] = "draft"
            definition["updated_at"] = _now_text()
            definition["updated_by"] = str(actor or "")
            self.state_store.put_document(DRILL_DEFINITION_NAMESPACE, drill_id, definition)
            return copy.deepcopy(definition)

    def publish(self, drill_id: str, *, expected_version: int, actor: str = "") -> dict[str, Any]:
        with self._lock:
            definition = self.get_definition(drill_id)
            if int(expected_version or 0) != int(definition.get("version") or 0):
                raise DrillConflictError("演练配置已被修改，请刷新后重试。")
            workbook = _parse_workbook(self._source_path(definition))
            definition["configuration"] = _validate_configuration(workbook, definition.get("configuration") or {})
            definition["status"] = "published"
            definition["version"] = int(definition.get("version") or 0) + 1
            definition["updated_at"] = _now_text()
            definition["updated_by"] = str(actor or "")
            self.state_store.put_document(DRILL_DEFINITION_NAMESPACE, drill_id, definition)
            return copy.deepcopy(definition)

    def _has_execution(self, drill_id: str) -> bool:
        return bool(self.state_store.list_documents(DRILL_EXECUTION_NAMESPACE, key_prefix=f"{drill_id}:"))

    def delete_definition(self, drill_id: str, *, expected_version: int) -> dict[str, Any]:
        with self._lock:
            definition = self.get_definition(drill_id)
            if int(expected_version or 0) != int(definition.get("version") or 0):
                raise DrillConflictError("演练已被修改，请刷新后重试。")
            if self._has_execution(drill_id):
                definition["status"] = "archived"
                definition["version"] = int(definition.get("version") or 0) + 1
                definition["updated_at"] = _now_text()
                self.state_store.put_document(DRILL_DEFINITION_NAMESPACE, drill_id, definition)
                return {"deleted": False, "archived": True, "drill_id": drill_id}
            directory = self._definition_directory(definition)
            self.state_store.delete_document(DRILL_DEFINITION_NAMESPACE, drill_id)
            if directory.is_dir() and directory.is_relative_to(self.data_root):
                shutil.rmtree(directory)
            return {"deleted": True, "archived": False, "drill_id": drill_id}

    def get_execution(self, drill_id: str, scope: str, *, create: bool = False) -> dict[str, Any]:
        definition = self.get_definition(drill_id)
        normalized_scope = _normalize_scope(scope)
        key = _execution_key(drill_id, normalized_scope)
        execution = self.state_store.get_document(DRILL_EXECUTION_NAMESPACE, key)
        if not isinstance(execution, dict):
            if not create:
                raise DrillNotFoundError("该楼栋尚未填写演练。")
            now = _now_text()
            execution = {
                "key": key,
                "drill_id": drill_id,
                "scope": normalized_scope,
                "version": 0,
                "execution_version": 0,
                "generated_version": 0,
                "status": "draft",
                "drill_date": "",
                "first_start_time": "",
                "commander": {},
                "participants": [],
                "step_signers": {},
                "validation_errors": [],
                "last_error": "",
                "generated": {},
                "sync": {},
                "created_at": now,
                "updated_at": now,
            }
        execution["definition_version"] = int(definition.get("version") or 0)
        return copy.deepcopy(execution)

    def save_execution(self, drill_id: str, scope: str, payload: dict[str, Any], *, expected_version: int, actor: str = "") -> dict[str, Any]:
        with self._lock:
            definition = self.get_definition(drill_id)
            if str(definition.get("status") or "") != "published":
                raise DrillConflictError("演练模板尚未发布。")
            normalized_scope = _normalize_scope(scope)
            current = self.get_execution(drill_id, normalized_scope, create=True)
            if int(expected_version or 0) != int(current.get("version") or 0):
                raise DrillConflictError("演练内容已被其他用户修改，请刷新后重试。")
            if str(current.get("status") or "") in {
                "queued",
                "generating",
                "syncing",
            }:
                raise DrillConflictError(
                    "演练文件正在生成或同步，请完成后再修改。"
                )
            commander = _person((payload or {}).get("commander"))
            incoming = [_person(item) for item in ((payload or {}).get("participants") or []) if isinstance(item, dict)]
            participants: list[dict[str, str]] = []
            seen = set()
            for item in ([commander] if commander["record_id"] else []) + incoming:
                if not item["record_id"] or item["record_id"] in seen:
                    continue
                participants.append(item)
                seen.add(item["record_id"])
            if len(participants) > DRILL_MAX_PARTICIPANTS:
                raise DrillError("参演人员最多选择 10 人。")
            next_execution = {
                **current,
                "drill_date": str((payload or {}).get("drill_date") or "").strip(),
                "first_start_time": str((payload or {}).get("first_start_time") or "").strip(),
                "commander": commander,
                "participants": participants,
                "step_signers": {
                    str(key): [str(value or "") for value in values]
                    for key, values in (((payload or {}).get("step_signers") or {}).items())
                    if isinstance(values, list)
                },
                "version": int(current.get("version") or 0) + 1,
                "execution_version": int(current.get("execution_version") or 0) + 1,
                "updated_at": _now_text(),
                "updated_by": str(actor or ""),
                "last_error": "",
            }
            next_execution.pop("generation_queue", None)
            errors = _validate_execution(definition, next_execution)
            next_execution["validation_errors"] = errors
            next_execution["status"] = "ready" if not errors else "draft"
            self.state_store.put_document(DRILL_EXECUTION_NAMESPACE, next_execution["key"], next_execution)
            return copy.deepcopy(next_execution)

    def queue_generation(
        self,
        drill_id: str,
        scope: str,
        *,
        expected_version: int,
    ) -> dict[str, Any]:
        with self._lock:
            definition = self.get_definition(drill_id)
            if str(definition.get("status") or "") != "published":
                raise DrillConflictError("演练模板已归档或尚未发布，不能生成。")
            execution = self.get_execution(drill_id, scope, create=False)
            queued = (
                execution.get("generation_queue")
                if isinstance(execution.get("generation_queue"), dict)
                else {}
            )
            if (
                str(execution.get("status") or "") == "queued"
                and int(queued.get("request_version", -1))
                == int(expected_version or 0)
            ):
                return copy.deepcopy(execution)
            if str(execution.get("status") or "") in {
                "queued",
                "generating",
                "sync_pending",
                "syncing",
            }:
                raise DrillConflictError("演练文件正在生成或同步，请勿重复提交。")
            if int(expected_version or 0) != int(execution.get("version") or 0):
                raise DrillConflictError("演练记录已被其他人修改，请刷新后重新生成。")
            now = _now_text()
            execution["generation_queue"] = {
                "request_version": int(expected_version or 0),
                "execution_version": int(execution.get("execution_version") or 0),
                "queued_at": now,
            }
            execution["status"] = "queued"
            execution["last_error"] = ""
            execution["version"] = int(execution.get("version") or 0) + 1
            execution["updated_at"] = now
            self.state_store.put_document(
                DRILL_EXECUTION_NAMESPACE, execution["key"], execution
            )
            return copy.deepcopy(execution)

    def _models(
        self,
        drill_id: str,
        scope: str,
        *,
        require_complete: bool = False,
        prefer_generated: bool = True,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        definition = self.get_definition(drill_id)
        execution = self.get_execution(drill_id, scope, create=not require_complete)
        errors = _validate_execution(definition, execution)
        if require_complete and errors:
            raise DrillConflictError(errors[0])
        configuration = definition["configuration"]
        current = int(execution.get("generated_version") or 0) == int(
            execution.get("execution_version") or 0
        ) > 0
        workbook_path = self._source_path(definition)
        if current and prefer_generated:
            workbook_path, _file_name = self.generated_file(drill_id, scope)
        workbook = _parse_workbook(
            workbook_path,
            include_assets_for={
                str(configuration.get("record_sheet") or ""),
                str(configuration.get("assessment_sheet") or ""),
            },
        )
        derived = _derived_values(definition, execution, allow_incomplete=not require_complete)
        record = _sheet_preview_model(_find_sheet(workbook, definition["configuration"]["record_sheet"]), sheet_type="record", derived=derived, generated_current=current)
        assessment = _sheet_preview_model(_find_sheet(workbook, definition["configuration"]["assessment_sheet"]), sheet_type="assessment", derived=derived, generated_current=current)
        return definition, execution, derived, {"record": record, "assessment": assessment}

    def preview_model(self, drill_id: str, scope: str, sheet_type: str) -> dict[str, Any]:
        _definition, execution, _derived, models = self._models(drill_id, scope)
        normalized = str(sheet_type or "").strip().lower()
        if normalized not in models:
            raise DrillError("预览类型必须是 record 或 assessment。")
        return {**copy.deepcopy(models[normalized]), "execution_version": int(execution.get("execution_version") or 0), "generated_version": int(execution.get("generated_version") or 0)}

    def print_model(self, drill_id: str, scope: str, sheet_type: str) -> dict[str, Any]:
        _definition, execution, _derived, models = self._models(
            drill_id, scope, require_complete=True
        )
        if (
            int(execution.get("generated_version") or 0)
            != int(execution.get("execution_version") or 0)
            or int(execution.get("generated_version") or 0) <= 0
        ):
            raise DrillConflictError("演练内容已修改，请重新生成后打印。")
        normalized = str(sheet_type or "").strip().lower()
        if normalized not in models:
            raise DrillError("打印类型必须是 record 或 assessment。")
        return {
            **copy.deepcopy(models[normalized]),
            "execution_version": int(execution.get("execution_version") or 0),
            "generated_version": int(execution.get("generated_version") or 0),
        }

    def generate(
        self,
        drill_id: str,
        scope: str,
        *,
        signatures: dict[str, bytes] | None = None,
        signature_resolver: Callable[[str], bytes] | None = None,
        expected_execution_version: int | None = None,
    ) -> dict[str, Any]:
        normalized_scope = _normalize_scope(scope)
        with self._lock:
            definition, execution, derived, models = self._models(
                drill_id,
                normalized_scope,
                require_complete=True,
                prefer_generated=False,
            )
            if str(definition.get("status") or "") != "published":
                raise DrillConflictError("演练模板已归档或尚未发布，不能生成。")
            if (
                expected_execution_version is not None
                and int(execution.get("execution_version") or 0)
                != int(expected_execution_version)
            ):
                raise DrillConflictError(
                    "演练内容在排队后已修改，已取消旧版本生成任务。"
                )
            execution["status"] = "generating"
            execution["last_error"] = ""
            execution["version"] = int(execution.get("version") or 0) + 1
            execution["updated_at"] = _now_text()
            self.state_store.put_document(DRILL_EXECUTION_NAMESPACE, execution["key"], execution)
            try:
                needed = {
                    str(person.get("record_id") or "")
                    for item in derived["signature_cells"]
                    for person in item.get("signers") or []
                    if str(person.get("record_id") or "")
                }
                resolved = dict(signatures or {})
                resolver_errors: dict[str, str] = {}
                for record_id in sorted(needed):
                    if resolved.get(record_id) or not signature_resolver:
                        continue
                    try:
                        resolved[record_id] = bytes(signature_resolver(record_id) or b"")
                    except Exception as exc:
                        resolver_errors[record_id] = str(exc)
                        resolved[record_id] = b""
                missing = [record_id for record_id in sorted(needed) if not resolved.get(record_id)]
                if missing:
                    names = {
                        str(person.get("record_id") or ""): str(person.get("name") or "")
                        for item in derived["signature_cells"]
                        for person in item.get("signers") or []
                    }
                    detail = "、".join(names.get(item) or item for item in missing)
                    read_error = next((resolver_errors.get(item) for item in missing if resolver_errors.get(item)), "")
                    raise DrillConflictError(
                        f"以下人员缺少可用签名：{detail}"
                        + (f"（{read_error}）" if read_error else "")
                    )
                resolved = {
                    record_id: normalize_drill_signature_png(
                        bytes(resolved[record_id])
                    )
                    for record_id in needed
                }
                directory = self._definition_directory(definition) / normalized_scope
                safe_title = _safe_file_name(str(definition.get("name") or "演练"), "演练")
                drill_date = dt.date.fromisoformat(str(execution.get("drill_date") or ""))
                file_name = _safe_file_name(
                    f"{normalized_scope}楼-{drill_date.year}年{drill_date.month:02d}月"
                    f"{drill_date.day:02d}日-{safe_title}-演练记录.xlsx"
                )
                output_path = directory / "current.xlsx"
                _patch_workbook(self._source_path(definition), output_path, definition, execution, derived, resolved)
                _verify_generated_workbook(
                    output_path, definition, derived, resolved
                )
                metadata = {"path": str(output_path), "name": file_name, "size": output_path.stat().st_size, "sha256": _file_sha256(output_path)}
                execution.update(
                    {
                        "status": "sync_pending",
                        "generated_version": int(execution.get("execution_version") or 0),
                        "generated": metadata,
                        "last_error": "",
                        "version": int(execution.get("version") or 0) + 1,
                        "updated_at": _now_text(),
                    }
                )
                execution.pop("generation_queue", None)
                self.state_store.put_document(DRILL_EXECUTION_NAMESPACE, execution["key"], execution)
                for model in models.values():
                    model["generated_current"] = True
                return {"execution": copy.deepcopy(execution), "generated": metadata, "print_models": models}
            except Exception as exc:
                execution.update({"status": "error", "last_error": str(exc), "version": int(execution.get("version") or 0) + 1, "updated_at": _now_text()})
                execution.pop("generation_queue", None)
                self.state_store.put_document(DRILL_EXECUTION_NAMESPACE, execution["key"], execution)
                raise

    def generated_file(self, drill_id: str, scope: str) -> tuple[Path, str]:
        definition = self.get_definition(drill_id)
        execution = self.get_execution(drill_id, scope)
        if int(execution.get("generated_version") or 0) != int(execution.get("execution_version") or 0) or int(execution.get("generated_version") or 0) <= 0:
            raise DrillConflictError("演练内容已修改，请重新生成后下载。")
        generated = execution.get("generated") if isinstance(execution.get("generated"), dict) else {}
        path = Path(str(generated.get("path") or "")).resolve()
        directory = (self._definition_directory(definition) / _normalize_scope(scope)).resolve()
        if not path.is_file() or not path.is_relative_to(directory):
            raise DrillNotFoundError("演练生成文件不存在。")
        return path, str(generated.get("name") or path.name)

    def begin_sync(
        self,
        drill_id: str,
        scope: str,
        *,
        generated_version: int,
    ) -> dict[str, Any]:
        with self._lock:
            execution = self.get_execution(drill_id, scope)
            expected = int(generated_version or 0)
            if (
                expected <= 0
                or expected != int(execution.get("generated_version") or 0)
                or expected != int(execution.get("execution_version") or 0)
            ):
                raise DrillConflictError(
                    "演练内容已修改，旧版本文件不再同步，请重新生成。"
                )
            sync = dict(execution.get("sync") or {})
            if str(execution.get("status") or "") == "syncing":
                if int(sync.get("expected_execution_version") or 0) == expected:
                    return copy.deepcopy(execution)
                raise DrillConflictError("演练正在同步其他版本，请刷新后重试。")
            now = _now_text()
            sync.update(
                {
                    "expected_execution_version": expected,
                    "started_at": now,
                    "last_error": "",
                }
            )
            execution.update(
                {
                    "sync": sync,
                    "status": "syncing",
                    "last_error": "",
                    "version": int(execution.get("version") or 0) + 1,
                    "updated_at": now,
                }
            )
            self.state_store.put_document(
                DRILL_EXECUTION_NAMESPACE, execution["key"], execution
            )
            return copy.deepcopy(execution)

    def mark_sync_result(self, drill_id: str, scope: str, *, generated_version: int, record_id: str = "", file_token: str = "", error: str = "", retryable: bool = True) -> dict[str, Any]:
        with self._lock:
            execution = self.get_execution(drill_id, scope)
            if (
                int(generated_version or 0)
                != int(execution.get("generated_version") or 0)
                or int(generated_version or 0)
                != int(execution.get("execution_version") or 0)
            ):
                raise DrillConflictError("演练已生成新版本，忽略旧版本同步结果。")
            now = _now_text()
            sync = dict(execution.get("sync") or {})
            sync.update({"record_id": str(record_id or sync.get("record_id") or ""), "file_token": str(file_token or sync.get("file_token") or ""), "last_error": str(error or ""), "updated_at": now})
            if record_id:
                execution["remote_record_id"] = str(record_id)
            execution.update({"sync": sync, "status": ("sync_pending" if retryable else "error") if error else "synced", "last_error": str(error or ""), "version": int(execution.get("version") or 0) + 1, "updated_at": now})
            self.state_store.put_document(DRILL_EXECUTION_NAMESPACE, execution["key"], execution)
            return copy.deepcopy(execution)

    def recover_pending(self) -> list[dict[str, Any]]:
        return [
            copy.deepcopy(document.get("payload") or {})
            for document in self.state_store.list_documents(DRILL_EXECUTION_NAMESPACE)
            if isinstance(document.get("payload"), dict)
            and str((document.get("payload") or {}).get("status") or "") in {"queued", "generating", "sync_pending", "syncing"}
        ]


__all__ = [
    "DRILL_DEFINITION_NAMESPACE",
    "DRILL_EXECUTION_NAMESPACE",
    "DRILL_MAX_XLSX_BYTES",
    "DRILL_SCOPES",
    "DrillConflictError",
    "DrillError",
    "DrillManagementService",
    "DrillNotFoundError",
    "build_time_chain",
    "detect_drill_configuration",
    "normalize_drill_signature_png",
    "parse_duration_minutes",
]

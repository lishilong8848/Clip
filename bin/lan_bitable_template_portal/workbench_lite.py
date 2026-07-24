# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime as dt
import html
import json
import re
from contextlib import suppress
from typing import Any
from urllib.parse import urlencode


WORK_TYPE_LABELS: dict[str, str] = {
    "maintenance": "维保",
    "change": "变更",
    "repair": "检修",
    "power": "上/下电",
    "polling": "轮巡",
    "adjust": "调整",
}

ALL_WORK_TYPE = "all"
WORK_TYPE_FILTER_LABELS: dict[str, str] = {ALL_WORK_TYPE: "全部", **WORK_TYPE_LABELS}

NOTICE_TYPE_BY_WORK_TYPE: dict[str, str] = {
    "maintenance": "维保通告",
    "change": "变更通告",
    "repair": "设备检修",
    "power": "上电通告",
    "polling": "设备轮巡",
    "adjust": "设备调整",
}

WORK_TYPE_BY_NOTICE_TYPE: dict[str, str] = {
    "维保通告": "maintenance",
    "维护通告": "maintenance",
    "变更通告": "change",
    "设备检修": "repair",
    "检修通告": "repair",
    "上下电通告": "power",
    "上电通告": "power",
    "下电通告": "power",
    "设备轮巡": "polling",
    "轮巡通告": "polling",
    "设备调整": "adjust",
    "调整通告": "adjust",
    "事件通告": "event",
}

ITEM_WORK_TYPE_LABELS: dict[str, str] = {**WORK_TYPE_LABELS, "event": "事件"}

POWER_NOTICE_TYPES = ("上电通告", "下电通告")
SPECIALTY_OPTIONS = ("电气", "暖通", "消防", "弱电")
MAINTENANCE_CYCLE_OPTIONS = ("/", "每月", "每季", "每年", "半年", "每两年", "每三年", "每五年", "冬季保温每日", "非计划性")
SITE_PHOTO_REQUIRED_WORK_TYPES = {"maintenance", "change", "repair"}
BINDABLE_TARGET_WORK_TYPES = {"maintenance", "change", "repair", "power", "polling", "adjust"}
BUILDING_SCOPE_CODES = ("110", "A", "B", "C", "D", "E", "H")
PENDING_PAGE_SIZE = 24
ONGOING_PAGE_SIZE = 18
REQUIRED_UPLOAD_FIELDS_BY_WORK_TYPE: dict[str, set[str]] = {
    "maintenance": {"title", "start_time", "end_time", "location", "content", "reason", "impact", "progress", "specialty", "maintenance_cycle"},
    "change": {"title", "level", "start_time", "end_time", "location", "content", "reason", "impact", "progress", "specialty"},
    "repair": {
        "title",
        "location",
        "level",
        "specialty",
        "end_time",
        "start_time",
        "repair_device",
        "repair_fault",
        "fault_type",
        "repair_mode",
        "impact",
        "discovery",
        "symptom",
        "reason",
        "solution",
        "progress",
    },
    "power": {"title", "start_time", "end_time", "cabinet", "quantity", "progress", "specialty"},
    "polling": {"title", "start_time", "end_time", "device", "content", "impact", "progress", "specialty"},
    "adjust": {"title", "start_time", "end_time", "location", "content", "reason", "impact", "progress", "specialty"},
}


def _e(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


_DRAFT_DOM_KEYS = {
    "action",
    "operation_id",
    "active_item_id",
    "source_record_id",
    "target_record_id",
    "record_id",
    "repair_management_record_id",
    "manual_id",
    "scope",
    "work_type",
    "notice_type",
    "title",
    "building",
    "buildings",
    "specialty",
    "maintenance_cycle",
    "level",
    "start_time",
    "end_time",
    "status",
    "site_photo_count",
    "mop_status",
    "name",
    "location",
    "content",
    "reason",
    "impact",
    "progress",
    "repair_device",
    "repair_fault",
    "fault_type",
    "repair_mode",
    "discovery",
    "symptom",
    "solution",
    "spare_parts",
    "fault_time",
    "expected_time",
    "device",
    "cabinet",
    "quantity",
}


def _safe_draft_snapshot(draft: dict[str, Any] | None) -> dict[str, str]:
    if not isinstance(draft, dict):
        return {}
    snapshot: dict[str, str] = {}
    for key in _DRAFT_DOM_KEYS:
        value = draft.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, (list, tuple)):
            text = "、".join(str(item or "") for item in value if str(item or "").strip())
        elif isinstance(value, dict):
            continue
        else:
            text = str(value)
        text = text.strip()
        if text:
            snapshot[key] = text[:800]
    return snapshot


def _safe_draft_json_attr(draft: dict[str, Any] | None) -> str:
    return _json_dumps(_safe_draft_snapshot(draft))


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _page_number(value: Any) -> int:
    return max(1, _to_int(value, 1))


def _page_count(total: int, page_size: int) -> int:
    total = max(0, int(total or 0))
    page_size = max(1, int(page_size or 1))
    return max(1, (total + page_size - 1) // page_size)


def _page_slice(items: list[dict[str, Any]], page: int, page_size: int) -> tuple[list[dict[str, Any]], int, int]:
    total_pages = _page_count(len(items), page_size)
    page = min(max(1, int(page or 1)), total_pages)
    start = (page - 1) * page_size
    return items[start : start + page_size], page, total_pages


def _pagination_controls(
    *,
    label: str,
    page_param: str,
    page: int,
    total_pages: int,
    total: int,
    page_size: int,
    base_params: dict[str, Any],
) -> str:
    if total <= page_size or total_pages <= 1:
        return ""
    page = min(max(1, page), total_pages)
    start = max(1, page - 2)
    end = min(total_pages, page + 2)

    def page_link(target_page: int, text: str, class_name: str = "") -> str:
        params = dict(base_params)
        params[page_param] = target_page
        disabled = target_page == page
        if disabled:
            return f"<span class=\"page-btn current {class_name}\" aria-current=\"page\">{_e(text)}</span>"
        return f"<a class=\"page-btn {class_name}\" href=\"{_e(_query_url('/workbench-lite', **params))}\">{_e(text)}</a>"

    parts = [
        f"<nav class=\"list-pagination\" aria-label=\"{_e(label)}分页\">",
        f"<span class=\"page-summary\">{_e(label)} {total} 条 · 每页 {page_size} 条</span>",
        "<span class=\"page-links\">",
        page_link(max(1, page - 1), "上一页", "prev") if page > 1 else "<span class=\"page-btn disabled\">上一页</span>",
    ]
    if start > 1:
        parts.append(page_link(1, "1"))
        if start > 2:
            parts.append("<span class=\"page-ellipsis\">...</span>")
    for item_page in range(start, end + 1):
        parts.append(page_link(item_page, str(item_page)))
    if end < total_pages:
        if end < total_pages - 1:
            parts.append("<span class=\"page-ellipsis\">...</span>")
        parts.append(page_link(total_pages, str(total_pages)))
    parts.append(
        page_link(min(total_pages, page + 1), "下一页", "next")
        if page < total_pages
        else "<span class=\"page-btn disabled\">下一页</span>"
    )
    parts.extend(["</span>", "</nav>"])
    return "".join(parts)


def _field(record: dict[str, Any], *names: str) -> str:
    fields = record.get("display_fields") if isinstance(record.get("display_fields"), dict) else {}
    for name in names:
        value = fields.get(name)
        if isinstance(value, list):
            value = "、".join(str(item or "").strip() for item in value if str(item or "").strip())
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _relation_record_ids(record: dict[str, Any], *names: str) -> list[str]:
    raw_fields = (
        record.get("raw_fields")
        if isinstance(record.get("raw_fields"), dict)
        else {}
    )
    display_fields = (
        record.get("display_fields")
        if isinstance(record.get("display_fields"), dict)
        else {}
    )
    result: list[str] = []

    def collect(value: Any) -> None:
        if isinstance(value, dict):
            nested = value.get("record_ids")
            if not isinstance(nested, list):
                nested = value.get("link_record_ids")
            if isinstance(nested, list):
                for item in nested:
                    collect(item)
                return
            collect(
                value.get("record_id")
                or value.get("id")
                or value.get("text")
                or ""
            )
            return
        if isinstance(value, list):
            for item in value:
                collect(item)
            return
        text = str(value or "").strip()
        if not text:
            return
        if text.startswith(("[", "{")):
            try:
                collect(json.loads(text))
                return
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
        for record_id in re.findall(r"\brec[A-Za-z0-9_-]+", text):
            if record_id not in result:
                result.append(record_id)

    for name in names:
        collect(raw_fields.get(name))
        collect(display_fields.get(name))
    return result


def _first(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _work_type(value: Any) -> str:
    text = str(value or "maintenance").strip()
    return text if text in WORK_TYPE_LABELS else "maintenance"


def _view_work_type(value: Any) -> str:
    text = str(value or "maintenance").strip()
    if text == ALL_WORK_TYPE:
        return ALL_WORK_TYPE
    return _work_type(text)


def _item_work_type(item: dict[str, Any] | None) -> str:
    item = item if isinstance(item, dict) else {}
    notice_type = str(item.get("notice_type") or "").strip()
    mapped = WORK_TYPE_BY_NOTICE_TYPE.get(notice_type)
    if mapped:
        return mapped
    raw_work = str(
        item.get("work_type") or item.get("lan_work_type") or ""
    ).strip()
    if raw_work in ITEM_WORK_TYPE_LABELS:
        return raw_work
    text = "\n".join(
        str(item.get(key) or "").strip()
        for key in ("text", "content", "title", "name")
        if str(item.get(key) or "").strip()
    )
    if re.search(r"上电通告|下电通告|上下电通告", text):
        return "power"
    if re.search(r"设备轮巡|轮巡通告", text):
        return "polling"
    if re.search(r"设备调整|调整通告", text):
        return "adjust"
    if re.search(r"设备检修|检修通告", text):
        return "repair"
    if re.search(r"变更通告", text):
        return "change"
    if re.search(r"事件通告", text):
        return "event"
    return "maintenance"


def _record_key(record: dict[str, Any]) -> str:
    return f"{_item_work_type(record)}:{record.get('record_id') or record.get('source_record_id') or ''}"


def _source_id(record: dict[str, Any] | None) -> str:
    if not isinstance(record, dict):
        return ""
    return str(record.get("source_record_id") or record.get("record_id") or "").strip()


def _explicit_source_id(record: dict[str, Any] | None) -> str:
    if not isinstance(record, dict):
        return ""
    return str(record.get("source_record_id") or "").strip()


def _record_building_code(record: dict[str, Any], title: str = "") -> str:
    values: list[str] = []
    raw_codes = record.get("building_codes")
    if isinstance(raw_codes, list):
        values.extend(str(item or "") for item in raw_codes)
    values.extend(
        [
            str(record.get("building_code") or ""),
            str(record.get("scope") or ""),
            _field(record, "楼栋", "变更楼栋", "所属数据中心/楼栋-使用"),
            str(record.get("building") or ""),
            str(title or ""),
        ]
    )
    text = " ".join(value.strip() for value in values if value and value.strip()).upper()
    if "110" in text:
        return "110"
    match = re.search(r"[ABCDEH]", text)
    return match.group(0) if match else ""


def _maintenance_prefixed_title(record: dict[str, Any], raw_title: str) -> str:
    title = str(raw_title or "").strip()
    if not title:
        return ""
    code = _record_building_code(record, title)
    if code == "110":
        prefix = "EA118-110KV阿里中天变"
        if title.startswith(prefix):
            return title
        title = re.sub(
            r"^EA118\s*(?:机房)?\s*[-－]?\s*110\s*(?:站|KV|千伏)?\s*",
            "",
            title,
            flags=re.I,
        ).strip()
        title = re.sub(r"^110\s*(?:站|KV|千伏)?\s*", "", title, flags=re.I).strip()
        return f"{prefix}{title}".strip()
    if code in BUILDING_SCOPE_CODES:
        prefix = f"EA118机房{code}楼"
        if title.startswith(prefix):
            return title
        title = re.sub(
            r"^EA118\s*(?:机房)?\s*[ABCDEH]\s*[楼栋]?\s*",
            "",
            title,
            flags=re.I,
        ).strip()
        title = re.sub(r"^[ABCDEH]\s*[楼栋]\s*", "", title, flags=re.I).strip()
        return f"{prefix}{title}".strip()
    return title


def _repair_title_candidate(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("text", "name", "value", "title"):
            text = _repair_title_candidate(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, (list, tuple, set)):
        parts = [_repair_title_candidate(item) for item in value]
        return "、".join(part for part in parts if part)
    text = str(value or "").strip()
    if not text or text in {"-", "--", "—", "暂无", "未填写", "None", "null"}:
        return ""
    if re.fullmatch(r"opt[A-Za-z0-9]{6,}", text):
        return ""
    if re.fullmatch(
        r"EA118[_-]?C01机房(?:110站|[ABCDEH]楼)?检修(?:通告)?",
        text,
        flags=re.I,
    ):
        return ""
    return text


def _record_title(record: dict[str, Any]) -> str:
    work_type = _item_work_type(record)
    text = str(record.get("text") or "").strip()
    text_title = ""
    if text:
        match = re.search(r"【(?:名称|标题)】\s*([^\n\r；;]+)", text)
        if match:
            text_title = match.group(1).strip()
    if work_type == "change":
        return _first(
            record.get("title"),
            _field(record, "变更简述", "名称", "标题"),
            text_title,
            record.get("record_id"),
        )
    if work_type == "repair":
        fields = (
            record.get("display_fields")
            if isinstance(record.get("display_fields"), dict)
            else {}
        )
        for value in (
            record.get("title"),
            fields.get("检修通告名称"),
            fields.get("名称（标题）"),
            fields.get("检修概述"),
            text_title,
            fields.get("事件描述"),
            fields.get("故障维修原因"),
            fields.get("故障发生现象描述"),
            fields.get("维修名称"),
            fields.get("标题"),
        ):
            candidate = _repair_title_candidate(value)
            if candidate:
                return candidate
        return str(record.get("record_id") or "").strip()
    title = _first(
        record.get("title"),
        _field(record, "维护总项", "名称", "标题", "手动标题"),
        text_title,
        record.get("record_id"),
    )
    return _maintenance_prefixed_title(record, title)


def _record_display_title(record: dict[str, Any]) -> str:
    """Return a business label without exposing internal identity values."""
    title = str(_record_title(record) or "").strip()
    technical_ids = {
        str(record.get(key) or "").strip()
        for key in (
            "active_item_id",
            "source_record_id",
            "target_record_id",
            "record_id",
        )
        if str(record.get(key) or "").strip()
    }
    title_exposes_identity = any(
        title == technical_id or title.endswith(technical_id)
        for technical_id in technical_ids
    )
    if title and not title_exposes_identity:
        return title
    work_type = _item_work_type(record)
    raw_notice_type = str(record.get("notice_type") or "").strip()
    notice_label = (
        raw_notice_type
        if WORK_TYPE_BY_NOTICE_TYPE.get(raw_notice_type) == work_type
        else NOTICE_TYPE_BY_WORK_TYPE.get(work_type, "通告")
    )
    return f"未命名{notice_label}"


def _record_building(record: dict[str, Any]) -> str:
    return _first(
        record.get("building"),
        _field(record, "楼栋", "变更楼栋", "所属数据中心/楼栋-使用"),
    )


def _record_specialty(record: dict[str, Any]) -> str:
    return _first(record.get("specialty"), _field(record, "专业类别", "专业", "所属专业"))


def _record_progress(record: dict[str, Any]) -> str:
    work_summary = (
        record.get("work_summary")
        if isinstance(record.get("work_summary"), dict)
        else {}
    )
    source_terminal_status = _first(
        record.get("source_progress"),
        record.get("source_status"),
        record.get("status"),
    )
    if _record_disabled_reason(source_terminal_status):
        return source_terminal_status
    return _first(
        work_summary.get("status"),
        record.get("source_progress"),
        record.get("source_status"),
        record.get("status"),
        _field(record, "维护实施状态", "变更进度", "维修开始时间"),
        "未开始",
    )


def _meta_chip(value: Any, *, tone: str = "") -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    tone_class = f" {tone}" if tone else ""
    return f"<small class=\"meta-chip{tone_class}\">{_e(text)}</small>"


def _row_meta(*values: Any, progress: Any = "", extra_chips: list[str] | None = None) -> str:
    chips = []
    progress_text = str(progress or "").strip()
    if progress_text:
        tone = "ready" if any(flag in progress_text for flag in ("未开始", "延期未开始", "进行中")) else "muted"
        chips.append(_meta_chip(progress_text, tone=tone))
    chips.extend(extra_chips or [])
    for value in values:
        chips.append(_meta_chip(value))
    chips = [item for item in chips if item]
    return f"<span class=\"row-meta\">{''.join(chips)}</span>"


def _progress_tone(progress: Any) -> str:
    text = str(progress or "").strip()
    if any(flag in text for flag in ("失败", "异常", "不可")):
        return "danger"
    if "未结束" not in text and any(flag in text for flag in ("已结束", "正常结束", "延期结束", "结束", "闭环", "已完成")):
        return "done"
    if "进行中" in text:
        return "working"
    if any(flag in text for flag in ("延期未开始", "未开始")):
        return "ready"
    return "muted"


def _progress_badge(progress: Any) -> str:
    text = str(progress or "").strip() or "未开始"
    return f"<span class=\"row-status {_e(_progress_tone(text))}\">{_e(text)}</span>"


def _ongoing_display_status(record: dict[str, Any]) -> str:
    status_text = " ".join(
        str(record.get(key) or "").strip()
        for key in ("status", "phase", "error", "last_error", "failure_reason")
        if str(record.get(key) or "").strip()
    )
    if any(flag in status_text for flag in ("失败", "错误", "异常")):
        return "发送失败"
    if any(flag in status_text for flag in ("待处理", "需处理", "重试")):
        return "需处理"
    return "进行中"


def _record_disabled_reason(
    progress: Any,
    *,
    linked_ongoing: bool = False,
) -> str:
    text = str(progress or "").strip()
    if "未结束" not in text and any(flag in text for flag in ("已结束", "正常结束", "延期结束", "结束", "闭环", "已完成")):
        return "该事项已结束，只保留查看状态，不可再次发起。"
    if linked_ongoing and ("进行中" in text or "未结束" in text):
        return "该事项已在“未结束通告”中，请从通告处理列表继续办理。"
    return ""


def _record_sort_key(record: dict[str, Any]) -> tuple[int, str, str]:
    progress = _record_progress(record)
    title = _record_title(record)
    if "延期未开始" in progress:
        priority = 0
    elif "未开始" in progress:
        priority = 1
    elif "进行中" in progress or "未结束" in progress:
        priority = 2
    elif any(flag in progress for flag in ("失败", "异常", "待处理")):
        priority = 3
    elif _record_disabled_reason(progress):
        priority = 9
    else:
        priority = 5
    return priority, title, str(record.get("record_id") or record.get("source_record_id") or "")


def _record_cycle(record: dict[str, Any]) -> str:
    return _first(
        record.get("maintenance_cycle"),
        _field(record, "维保周期", "维护周期"),
    )


def _maintenance_cycle_chip(record: dict[str, Any], work_type: str) -> str:
    if (_work_type(work_type) if work_type else _item_work_type(record)) != "maintenance":
        return ""
    cycle = _record_cycle(record)
    if not cycle:
        return _meta_chip("周期未填", tone="warn")
    if cycle == "/":
        return _meta_chip("周期 /", tone="ready")
    return _meta_chip(f"周期 {cycle}", tone="ready")


def _truthy_display(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "已上传", "上传成功", "success", "done", "ok"}


def _attachment_count(value: Any) -> int:
    if isinstance(value, list):
        return sum(1 for item in value if item)
    if isinstance(value, tuple):
        return sum(1 for item in value if item)
    if isinstance(value, dict):
        if isinstance(value.get("files"), list):
            return _attachment_count(value.get("files"))
        if isinstance(value.get("attachments"), list):
            return _attachment_count(value.get("attachments"))
        return 1 if any(str(value.get(key) or "").strip() for key in ("file_token", "token", "upload_id", "name", "url")) else 0
    return 1 if str(value or "").strip() else 0


def _site_photo_count(record: dict[str, Any]) -> int:
    for key in ("site_photo_count", "site_photos_count", "extra_image_count"):
        count = _to_int(record.get(key), -1)
        if count >= 0:
            return count
    fields = record.get("display_fields") if isinstance(record.get("display_fields"), dict) else {}
    count = 0
    for key in ("extra_images", "site_photos", "process_site_images"):
        count += _attachment_count(record.get(key))
    for key in ("过程现场图片", "现场图片"):
        count += _attachment_count(fields.get(key))
    return count


def _site_photo_chip(record: dict[str, Any], work_type: str) -> str:
    if (_work_type(work_type) if work_type else _item_work_type(record)) not in SITE_PHOTO_REQUIRED_WORK_TYPES:
        return ""
    count = _site_photo_count(record)
    if count > 0:
        return _meta_chip(f"现场图{count}张", tone="success")
    return _meta_chip("现场图未传", tone="warn")


def _site_photo_uploader(work_type: str, existing_count: int) -> str:
    work = _work_type(work_type)
    if work not in SITE_PHOTO_REQUIRED_WORK_TYPES:
        return ""
    count = max(0, _to_int(existing_count, 0))
    status_text = (
        f"已累计 {count} 张，结束时满足现场照片要求。"
        if count > 0
        else "开始、更新、结束任意一次上传 1 张现场照片后，即可满足结束要求。"
    )
    return f"""
        <section class="site-photo-panel" data-site-photo-panel data-existing-count="{_e(count)}">
          <input type="hidden" name="site_photos_json" value="[]">
          <header class="site-photo-head">
            <div>
              <strong>现场照片</strong>
              <span>仅维保、变更、检修需要；用于多维表“过程现场图片”。</span>
            </div>
            <b id="lite-site-photo-badge">{_e('已满足' if count > 0 else '待添加')}</b>
          </header>
          <div class="site-photo-upload">
            <label class="site-photo-drop" for="lite-site-photo-input" tabindex="0">
              <input id="lite-site-photo-input" type="file" accept="image/*" multiple>
              <span>点击 / 拖入 / Ctrl+V 粘贴现场照片</span>
              <small>支持截图粘贴、多张图片，单张不超过 8MB</small>
            </label>
            <div class="site-photo-side">
              <span id="lite-site-photo-status">{_e(status_text)}</span>
              <div id="lite-site-photo-list" class="site-photo-list" aria-live="polite"></div>
            </div>
          </div>
        </section>
    """


def _mop_status_info(record: dict[str, Any], work_type: str) -> tuple[str, str]:
    if (_work_type(work_type) if work_type else _item_work_type(record)) != "maintenance":
        return "", ""
    fields = record.get("display_fields") if isinstance(record.get("display_fields"), dict) else {}
    engineer_confirmed = _truthy_display(record.get("mop_engineer_confirmed")) or _truthy_display(fields.get("工程师确认"))
    supervisor_confirmed = _truthy_display(record.get("mop_supervisor_confirmed")) or _truthy_display(fields.get("主管确认"))
    uploaded_keys = (
        "mop_uploaded",
        "mop_uploaded_at",
        "mop_upload_status",
        "mop_attachment_count",
        "mop_file_token",
        "maintenance_sheet_uploaded",
    )
    if any(key in record for key in uploaded_keys) or fields.get("维护保养单"):
        uploaded = (
            _truthy_display(record.get("mop_uploaded"))
            or bool(str(record.get("mop_uploaded_at") or "").strip())
            or "成功" in str(record.get("mop_upload_status") or "")
            or _to_int(record.get("mop_attachment_count"), 0) > 0
            or bool(str(record.get("mop_file_token") or "").strip())
            or bool(fields.get("维护保养单"))
        )
        if uploaded and engineer_confirmed and supervisor_confirmed:
            return "MOP源表已确认", "success"
        if uploaded:
            return "MOP已上传", "success"
        return "MOP未上传", "warn"
    if _truthy_display(record.get("mop_filled")) or str(record.get("mop_filled_at") or "").strip():
        return "MOP已填写未上传", "warn"
    if _first(
        record.get("mop_title"),
        record.get("mop_attachment_name"),
        record.get("mop_record_id"),
        record.get("mop_binding"),
    ):
        return "MOP已绑定未填写", "ready"
    return "MOP未绑定", "muted"


def _mop_status_text(record: dict[str, Any], work_type: str) -> str:
    text, _tone = _mop_status_info(record, work_type)
    return text


def _mop_status_chip(record: dict[str, Any], work_type: str) -> str:
    text, tone = _mop_status_info(record, work_type)
    return _meta_chip(text, tone=tone)


HISTORY_MEMORY_DRAFT_FIELDS = (
    "location",
    "content",
    "reason",
    "impact",
    "progress",
    "maintenance_cycle",
    "specialty",
    "level",
    "repair_device",
    "repair_fault",
    "fault_type",
    "repair_mode",
    "discovery",
    "symptom",
    "solution",
)


def _record_history_memory(record: dict[str, Any]) -> dict[str, Any]:
    memory = record.get("memory")
    return memory if isinstance(memory, dict) else {}


def _history_memory_has_meaningful_fields(memory: dict[str, Any]) -> bool:
    return any(
        str(memory.get(key) or "").strip()
        for key in HISTORY_MEMORY_DRAFT_FIELDS
    )


def _memory_status_chip(record: dict[str, Any]) -> str:
    memory = _record_history_memory(record)
    if (
        _truthy_display(record.get("memory_applied"))
        or _truthy_display(record.get("notice_memory_applied"))
        or _history_memory_has_meaningful_fields(memory)
    ):
        return _meta_chip("已用历史记忆", tone="success")
    if str(record.get("memory_status") or "").strip() in {"missing", "none", "未匹配"}:
        return _meta_chip("无历史记忆", tone="muted")
    return ""


def _source_mode_chip(record: dict[str, Any], *, ongoing: bool = False) -> str:
    if _explicit_source_id(record):
        return _meta_chip("源表事项", tone="source")
    if ongoing or record.get("active_item_id") or record.get("manual"):
        return _meta_chip("纯手填/复制", tone="manual")
    return ""


def _repair_event_link_chip(record: dict[str, Any], work_type: str) -> str:
    if work_type != "repair":
        return ""
    event_ids = _relation_record_ids(record, "关联事件单")
    source_event_id = str(
        record.get("source_event_id")
        or record.get("related_event_record_id")
        or ""
    ).strip()
    event_label = _field(record, "关联事件单")
    if event_ids or source_event_id or event_label:
        return _meta_chip("事件已关联", tone="success")
    return ""


def _record_extra_chips(record: dict[str, Any], work_type: str, *, ongoing: bool = False) -> list[str]:
    chips = [
        _maintenance_cycle_chip(record, work_type),
        _site_photo_chip(record, work_type),
        _mop_status_chip(record, work_type),
        _repair_event_link_chip(record, work_type),
        _memory_status_chip(record),
        _source_mode_chip(record, ongoing=ongoing),
    ]
    return [chip for chip in chips if chip]


def _action_for_record(record: dict[str, Any]) -> str:
    if (
        _item_work_type(record) == "change"
        and str(record.get("source_work_type") or record.get("converted_from_work_type") or "").strip()
        == "maintenance"
    ):
        return "start"
    progress = _record_progress(record)
    return (
        "start"
        if not progress
        or any(flag in progress for flag in ("未开始", "进行中", "未结束"))
        else "update"
    )


def _query_url(path: str, **params: Any) -> str:
    clean = {
        key: str(value)
        for key, value in params.items()
        if value is not None and str(value) != ""
    }
    return f"{path}?{urlencode(clean)}" if clean else path


def _manual_url(scope: str, work_type: str, month: str = "") -> str:
    return _query_url(
        "/workbench-lite",
        scope=scope,
        work_type=work_type,
        month=month,
        manual="1",
    )


def _undo_created_time(value: Any) -> str:
    try:
        timestamp = float(value or 0)
    except (TypeError, ValueError):
        return ""
    if timestamp <= 0:
        return ""
    try:
        return dt.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")
    except (OverflowError, OSError, ValueError):
        return ""


def _datetime_local(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("T", " ")
    try:
        parsed = dt.datetime.fromisoformat(text[:16])
        return parsed.strftime("%Y-%m-%dT%H:%M")
    except Exception:
        return text[:16].replace(" ", "T")


def _datetime_local_on_date(value: Any, target_date: dt.date) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = (
        text.replace("T", " ")
        .replace("：", ":")
        .replace("．", ".")
        .replace("。", ".")
    )
    hour = minute = None
    with suppress(Exception):
        parsed = dt.datetime.fromisoformat(normalized[:19])
        hour, minute = parsed.hour, parsed.minute
    if hour is None or minute is None:
        match = re.search(
            r"(?:[日号T\s]|^)(\d{1,2})\s*(?:[:.]|时)\s*(\d{1,2})",
            normalized,
        )
        if match:
            hour, minute = int(match.group(1)), int(match.group(2))
    if hour is None or minute is None or not (0 <= hour <= 23 and 0 <= minute <= 59):
        return _datetime_local(value)
    return f"{target_date.isoformat()}T{hour:02d}:{minute:02d}"


def _normalize_notice_label(label: str) -> str:
    return str(label or "").replace(" ", "").replace("\t", "").replace("：", "").replace(":", "")


def _parse_notice_sections(text: str) -> dict[str, str]:
    sections: dict[str, str] = {}
    pattern = re.compile(r"【([^】]+)】([\s\S]*?)(?=(?:\n\s*)*【[^】]+】|$)")
    for match in pattern.finditer(str(text or "")):
        sections[_normalize_notice_label(match.group(1))] = str(match.group(2) or "").strip()
    return sections


def _section_value(sections: dict[str, str], *names: str) -> str:
    for name in names:
        value = sections.get(_normalize_notice_label(name), "")
        if str(value or "").strip():
            return str(value).strip()
    return ""


def _to_datetime_local(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    value = value.replace("T", " ")
    patterns = [
        r"(\d{4})[年/-](\d{1,2})[月/-](\d{1,2})日?\D+(\d{1,2})[：:点时.](\d{1,2})?",
        r"(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{1,2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, value)
        if not match:
            continue
        return (
            f"{match.group(1)}-{match.group(2).zfill(2)}-{match.group(3).zfill(2)}"
            f"T{match.group(4).zfill(2)}:{(match.group(5) or '00').zfill(2)}"
        )
    return ""


def _split_notice_time_range(text: str) -> tuple[str, str]:
    value = str(text or "").strip()
    if not value:
        return "", ""
    matches = re.findall(r"\d{4}[年/-]\d{1,2}[月/-]\d{1,2}日?\s*\d{1,2}[：:点时.]\d{1,2}", value)
    if len(matches) >= 2:
        return _to_datetime_local(matches[0]), _to_datetime_local(matches[1])
    same_day = re.search(
        r"(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}日?)\s*(\d{1,2}[：:点时.]\d{1,2})\s*(?:-|至|~|～|—|--)\s*(\d{1,2}[：:点时.]\d{1,2})",
        value,
    )
    if same_day:
        prefix = same_day.group(1)
        return _to_datetime_local(f"{prefix} {same_day.group(2)}"), _to_datetime_local(f"{prefix} {same_day.group(3)}")
    parts = [item.strip() for item in re.split(r"\s*(?:至|~|～|—|--)\s*", value, maxsplit=1) if item.strip()]
    if len(parts) >= 2:
        start = _to_datetime_local(parts[0])
        end = _to_datetime_local(parts[1])
        if not end and start:
            date_prefix = re.search(r"(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}日?)", parts[0])
            clock = re.search(r"(\d{1,2}[：:点时.]\d{1,2})", parts[1])
            if date_prefix and clock:
                end = _to_datetime_local(f"{date_prefix.group(1)} {clock.group(1)}")
        return start, end
    return _to_datetime_local(value), ""


def _pasted_work_type(
    text: str,
    sections: dict[str, str],
    *,
    fallback_work_type: str = "",
) -> str:
    notice_type = _pasted_notice_type(text)
    mapped_work_type = WORK_TYPE_BY_NOTICE_TYPE.get(notice_type, "")
    if mapped_work_type in WORK_TYPE_LABELS:
        return mapped_work_type

    title_text = "\n".join(
        item for item in [
            _section_value(sections, "名称", "标题", "通告名称", "维修名称"),
            _section_value(sections, "内容"),
        ] if item
    )
    raw = str(text or "")
    head_match = re.match(r"^【[^】]+】", raw)
    head_text = f"{head_match.group(0) if head_match else ''}\n{title_text}"
    raw_head = "\n".join(raw.splitlines()[:5])
    for source in (head_text, raw_head):
        if re.search(r"设备检修|检修通告", source):
            return "repair"
        if re.search(r"上电通告|上下电通告|下电通告", source):
            return "power"
        if re.search(r"设备轮巡|轮巡通告", source):
            return "polling"
        if re.search(r"设备调整|调整通告", source):
            return "adjust"
        if "变更通告" in source:
            return "change"
    normalized_fallback = str(fallback_work_type or "").strip()
    if normalized_fallback in WORK_TYPE_LABELS:
        return normalized_fallback
    return "maintenance"


def _pasted_action(text: str) -> str:
    match = re.search(r"状态\s*[：:]\s*(开始|更新|结束)", str(text or ""))
    status = match.group(1) if match else "开始"
    if status == "更新":
        return "update"
    if status == "结束":
        return "end"
    return "start"


def parse_pasted_notice_to_draft(
    text: str,
    *,
    fallback_work_type: str = "",
) -> tuple[str, str, dict[str, str]]:
    sections = _parse_notice_sections(text)
    work = _pasted_work_type(
        text,
        sections,
        fallback_work_type=fallback_work_type,
    )
    action = _pasted_action(text)
    notice_type = _pasted_notice_type(text)
    time_value = _section_value(sections, "时间")
    start_time, end_time = _split_notice_time_range(time_value)
    if work == "repair":
        start_time = _to_datetime_local(_section_value(sections, "期望完成时间"))
        end_time = _to_datetime_local(_section_value(sections, "发现故障时间"))
    draft = {
        "work_type": work,
        "title": _section_value(sections, "名称", "标题", "通告名称", "维修名称"),
        "level": _section_value(sections, "等级", "紧急程度"),
        "start_time": start_time,
        "end_time": end_time,
        "location": _section_value(sections, "位置", "地点"),
        "content": _section_value(sections, "内容"),
        "reason": _section_value(sections, "原因", "故障原因"),
        "impact": _section_value(sections, "影响", "影响范围"),
        "progress": _section_value(sections, "进度", "完成情况"),
        "specialty": _section_value(sections, "专业"),
        "repair_device": _section_value(sections, "维修设备"),
        "repair_fault": _section_value(sections, "维修故障"),
        "fault_type": _section_value(sections, "故障类型"),
        "repair_mode": _section_value(sections, "维修方式"),
        "discovery": _section_value(sections, "故障发现方式"),
        "symptom": _section_value(sections, "故障现象"),
        "solution": _section_value(sections, "解决方案"),
        "spare_parts": _section_value(sections, "备件更换情况"),
        "device": _section_value(sections, "设备"),
        "cabinet": _section_value(sections, "柜号"),
        "quantity": _section_value(sections, "数量"),
    }
    if notice_type:
        draft["notice_type"] = notice_type
    return work, action, {key: value for key, value in draft.items() if str(value or "").strip()}


def _pasted_notice_type(text: str) -> str:
    match = re.search(r"【([^】]+)】", str(text or ""))
    if not match:
        return ""
    notice_type = match.group(1).strip()
    return notice_type if notice_type in WORK_TYPE_BY_NOTICE_TYPE else ""


def _draft_from_record(record: dict[str, Any], *, manual: bool = False, work_type: str = "") -> dict[str, str]:
    work = _work_type(work_type) if work_type else _item_work_type(record)
    title = _record_title(record)
    if manual:
        title = ""
    draft = {
        "title": title,
        "notice_type": str(record.get("notice_type") or "").strip(),
        "building": _record_building(record),
        "specialty": _record_specialty(record),
        "maintenance_cycle": _first(record.get("maintenance_cycle"), _field(record, "维护周期")),
        "level": _first(record.get("level"), _field(record, "变更等级（阿里）", "紧急程度"), "I3" if work == "change" else ""),
        "start_time": _datetime_local(_first(record.get("start_time"), _field(record, "计划开始时间", "计划开始", "变更开始日期（阿里）"))),
        "end_time": _datetime_local(_first(record.get("end_time"), _field(record, "计划结束时间", "计划结束", "变更结束日期（阿里）"))),
        "location": _first(record.get("location"), _field(record, "位置", "地点")),
        "content": _first(record.get("content"), _field(record, "内容", "标题/补充内容", "标题补充内容")),
        "reason": _first(record.get("reason"), _field(record, "原因", "故障原因", "故障维修原因")),
        "impact": _first(record.get("impact"), _field(record, "影响", "影响范围")),
        "progress": _first(record.get("progress"), _field(record, "进度", "维修进展描述", "当前维修进度"), "准备工作已完成，人员已就位，是否可以操作？"),
        "repair_device": _first(record.get("repair_device"), _field(record, "维修设备")),
        "repair_fault": _first(record.get("repair_fault"), _field(record, "维修故障", "故障维修原因")),
        "fault_type": _first(record.get("fault_type"), _field(record, "故障类型"), "设备故障" if work == "repair" else ""),
        "repair_mode": _first(record.get("repair_mode"), _field(record, "维修方式", "维修方", "供应商名称")),
        "discovery": _first(record.get("discovery"), _field(record, "故障发现方式", "对应来源")),
        "symptom": _first(record.get("symptom"), _field(record, "故障现象", "故障发生现象描述")),
        "solution": _first(record.get("solution"), _field(record, "解决方案", "维修方案", "后续整改措施")),
        "spare_parts": _first(record.get("spare_parts"), _field(record, "备件更换情况", "备件使用情况")),
        "device": _first(record.get("device"), _field(record, "设备")),
        "cabinet": _first(record.get("cabinet"), _field(record, "柜号")),
        "quantity": _first(record.get("quantity"), _field(record, "数量")),
        "repair_management_record_id": str(
            record.get("repair_management_record_id") or ""
        ).strip(),
    }
    memory = _record_history_memory(record)
    memory_applied = _history_memory_has_meaningful_fields(memory)
    if memory_applied:
        for key in HISTORY_MEMORY_DRAFT_FIELDS:
            value = str(memory.get(key) or "").strip()
            if value:
                draft[key] = value
        if not str(record.get("active_item_id") or "").strip():
            today = dt.date.today()
            draft["start_time"] = _datetime_local_on_date(
                draft.get("start_time"),
                today,
            )
            draft["end_time"] = _datetime_local_on_date(
                draft.get("end_time"),
                today,
            )
    source_work_type = str(record.get("source_work_type") or "").strip()
    converted_from = str(record.get("converted_from_work_type") or "").strip()
    converted_to = str(record.get("converted_to_work_type") or "").strip()
    if work == "change" and (source_work_type == "maintenance" or converted_from == "maintenance"):
        draft.update(
            {
                "source_work_type": "maintenance",
                "converted_from_work_type": "maintenance",
                "converted_to_work_type": "change",
                "sync_maintenance_target": "1",
                "paired_maintenance_original_title": title,
            }
        )
    elif source_work_type:
        draft["source_work_type"] = source_work_type
    if converted_to:
        draft["converted_to_work_type"] = converted_to
    return draft


def _input(
    name: str,
    label: str,
    value: Any = "",
    *,
    textarea: bool = False,
    input_type: str = "text",
    datalist: str = "",
    required: bool = False,
) -> str:
    label_class = " class=\"required\"" if required else ""
    required_attr = " required aria-required=\"true\"" if required else ""
    if textarea:
        return (
            f"<label{label_class}><span>{_e(label)}</span>"
            f"<textarea name=\"{_e(name)}\" rows=\"1\"{required_attr}>{_e(value)}</textarea></label>"
        )
    list_attr = f" list=\"{_e(datalist)}\"" if datalist else ""
    return (
        f"<label{label_class}><span>{_e(label)}</span>"
        f"<input name=\"{_e(name)}\" type=\"{_e(input_type)}\" value=\"{_e(value)}\"{list_attr}{required_attr}></label>"
    )


def _select(
    name: str,
    label: str,
    value: Any,
    options: tuple[str, ...],
    *,
    required: bool = False,
) -> str:
    selected_value = str(value or "").strip()
    if selected_value not in options:
        selected_value = options[0] if options else ""
    label_class = " class=\"required\"" if required else ""
    required_attr = " required aria-required=\"true\"" if required else ""
    option_html = "".join(
        f"<option value=\"{_e(option)}\"{' selected' if option == selected_value else ''}>{_e(option)}</option>"
        for option in options
    )
    return (
        f"<label{label_class}><span>{_e(label)}</span>"
        f"<select name=\"{_e(name)}\"{required_attr}>{option_html}</select></label>"
    )


def _is_required_upload_field(work_type: str, name: str) -> bool:
    return name in REQUIRED_UPLOAD_FIELDS_BY_WORK_TYPE.get(_work_type(work_type), set())


def _field_group(title: str, description: str, fields: list[str]) -> str:
    if not fields:
        return ""
    description_html = (
        f"<span>{_e(description)}</span>" if str(description or "").strip() else ""
    )
    return (
        "<section class=\"form-section\">"
        f"<header><strong>{_e(title)}</strong>{description_html}</header>"
        f"<div class=\"form-grid\">{''.join(fields)}</div>"
        "</section>"
    )


def _form_fields(work_type: str, draft: dict[str, str]) -> str:
    def field(name: str, label: str, value: Any = "", **kwargs: Any) -> str:
        return _input(name, label, value, required=_is_required_upload_field(work_type, name), **kwargs)

    primary_fields: list[str] = [
        field("title", "名称" if work_type not in {"repair", "polling"} else "标题", draft.get("title")),
        field("start_time", "开始时间" if work_type != "repair" else "期望完成时间", draft.get("start_time"), input_type="datetime-local"),
        field("end_time", "结束时间" if work_type != "repair" else "发现故障时间", draft.get("end_time"), input_type="datetime-local"),
        _select(
            "specialty",
            "专业",
            draft.get("specialty"),
            SPECIALTY_OPTIONS,
            required=_is_required_upload_field(work_type, "specialty"),
        ),
    ]
    if work_type == "power":
        primary_fields.insert(0, _select("notice_type", "通告类型", draft.get("notice_type"), POWER_NOTICE_TYPES))
    if work_type == "maintenance":
        primary_fields.append(field("maintenance_cycle", "维保周期", draft.get("maintenance_cycle"), datalist="maintenance-cycle-options"))
    if work_type in {"change", "repair"}:
        primary_fields.append(field("level", "等级" if work_type == "change" else "紧急程度", draft.get("level")))
    notice_fields: list[str] = []
    if work_type in {"maintenance", "change", "adjust", "repair"}:
        notice_fields.extend([
            field("location", "位置" if work_type != "repair" else "地点", draft.get("location")),
            field("content", "内容", draft.get("content"), textarea=True),
            field("reason", "原因" if work_type != "repair" else "故障原因", draft.get("reason"), textarea=True),
            field("impact", "影响" if work_type != "repair" else "影响范围", draft.get("impact"), textarea=True),
        ])
    if work_type == "repair":
        notice_fields.extend([
            field("repair_device", "维修设备", draft.get("repair_device")),
            field("repair_fault", "维修故障", draft.get("repair_fault")),
            field("fault_type", "故障类型", draft.get("fault_type")),
            field("repair_mode", "维修方式", draft.get("repair_mode")),
            field("discovery", "故障发现方式", draft.get("discovery")),
            field("symptom", "故障现象", draft.get("symptom")),
            field("solution", "解决方案", draft.get("solution"), textarea=True),
            field("spare_parts", "备件更换情况", draft.get("spare_parts"), textarea=True),
        ])
    if work_type == "power":
        notice_fields.extend([
            field("cabinet", "柜号", draft.get("cabinet")),
            field("quantity", "数量", draft.get("quantity")),
        ])
    if work_type == "polling":
        notice_fields.extend([
            field("device", "设备", draft.get("device")),
            field("content", "内容", draft.get("content"), textarea=True),
            field("impact", "影响", draft.get("impact"), textarea=True),
        ])
    notice_fields.append(field("progress", "进度" if work_type != "repair" else "完成情况", draft.get("progress"), textarea=True))
    return "\n".join([
        _field_group("基础必填", "", primary_fields),
        _field_group("通告内容字段", "", notice_fields),
    ])


def _record_rows(
    records: list[dict[str, Any]],
    *,
    ongoing_items: list[dict[str, Any]] | None,
    scope: str,
    work_type: str,
    month: str = "",
    search: str,
    specialty: str,
    selected_id: str,
    pending_page: int = 1,
    ongoing_page: int = 1,
) -> str:
    if not records:
        return "<div class=\"empty\">没有计划通告</div>"
    rows: list[str] = []
    for record in sorted(records, key=_record_sort_key):
        linked_ongoing = _linked_ongoing_for_source(record, ongoing_items)
        row_source = linked_ongoing or record
        record_id = str(record.get("record_id") or record.get("source_record_id") or "")
        row_work_type = _item_work_type(row_source)
        url = _query_url(
            "/workbench-lite",
            scope=scope,
            work_type=work_type,
            month=month,
            search=search,
            specialty=specialty,
            record_id=record_id,
            pending_page=pending_page,
            ongoing_page=ongoing_page,
        )
        active = " active" if record_id == selected_id else ""
        title = _record_title(row_source)
        progress = _record_progress(row_source)
        draft = _draft_from_record(row_source, work_type=row_work_type)
        source_record_id = _source_id(record)
        action = "update" if linked_ongoing else _action_for_record(record)
        disabled_reason = _record_disabled_reason(
            progress,
            linked_ongoing=bool(linked_ongoing),
        )
        site_photo_count = _site_photo_count(row_source)
        mop_status = _mop_status_text(row_source, row_work_type)
        extra_chips = _record_extra_chips(
            row_source,
            row_work_type,
            ongoing=bool(linked_ongoing),
        )
        source_event_chip = _repair_event_link_chip(record, row_work_type)
        if source_event_chip and source_event_chip not in extra_chips:
            extra_chips.append(source_event_chip)
        linked_active_item_id = str((linked_ongoing or {}).get("active_item_id") or "")
        source_event_ids = _relation_record_ids(record, "关联事件单")
        source_event_id = str(
            record.get("source_event_id")
            or (source_event_ids[0] if source_event_ids else "")
        ).strip()
        source_event_title = _first(
            record.get("event_title"),
            _field(record, "事件描述", "故障维修原因", "故障发生现象描述"),
        )
        linked_target_record_id = str(
            (linked_ongoing or {}).get("target_record_id")
            or (linked_ongoing or {}).get("record_id")
            or ""
        )
        disabled_class = " is-disabled" if disabled_reason else ""
        aria_disabled = "true" if disabled_reason else "false"
        rows.append(
        f"<a class=\"notice-row{active}{disabled_class}\" href=\"{_e(url)}\" title=\"{_e(disabled_reason or title)}\""
        f" aria-current=\"{'true' if active else 'false'}\""
        f" aria-disabled=\"{aria_disabled}\""
        f" data-row-kind=\"source\""
        f" data-work-type=\"{_e(row_work_type)}\""
        f" data-record-id=\"{_e(record_id)}\""
        f" data-source-record-id=\"{_e(source_record_id)}\""
        f" data-source-event-id=\"{_e(source_event_id)}\""
        f" data-source-event-title=\"{_e(source_event_title)}\""
        f" data-linked-ongoing=\"{'1' if linked_ongoing else '0'}\""
        f" data-active-item-id=\"{_e(linked_active_item_id)}\""
        f" data-target-record-id=\"{_e(linked_target_record_id)}\""
        f" data-site-photo-count=\"{_e(site_photo_count)}\""
        f" data-mop-status=\"{_e(mop_status)}\""
        f" data-action=\"{_e(action)}\""
        f" data-disabled-reason=\"{_e(disabled_reason)}\""
            f" data-title=\"{_e(title)}\""
        f" data-draft=\"{_e(_safe_draft_json_attr(draft))}\">"
            f"<span class=\"row-main\"><strong>{_e(title)}</strong>{_progress_badge(progress)}</span>"
            f"{_row_meta(_record_building(row_source), _record_specialty(row_source), progress=progress, extra_chips=extra_chips)}"
            "</a>"
        )
    return "\n".join(rows)


def _ongoing_rows(
    items: list[dict[str, Any]],
    *,
    scope: str,
    work_type: str,
    month: str = "",
    selected_id: str,
    pending_page: int = 1,
    ongoing_page: int = 1,
) -> str:
    filter_work_type = "" if work_type == ALL_WORK_TYPE else work_type
    filtered = [item for item in items if not filter_work_type or _item_work_type(item) == filter_work_type]
    if not filtered:
        return "<div class=\"empty\">当前没有未结束通告</div>"
    rows: list[str] = []
    for item in filtered:
        active_id = str(item.get("active_item_id") or item.get("target_record_id") or item.get("record_id") or "")
        row_work_type = _item_work_type(item)
        direct_navigation = row_work_type == "event"
        if direct_navigation:
            event_scope = _record_building_code(item) or scope
            url = _query_url(
                "/", scope=event_scope, mode="events", detail="1"
            )
        else:
            url = _query_url(
                "/workbench-lite",
                scope=scope,
                work_type=work_type,
                month=month,
                active_item_id=active_id,
                pending_page=pending_page,
                ongoing_page=ongoing_page,
            )
        active = " active" if active_id == selected_id else ""
        title = _record_display_title(item)
        draft = _draft_from_record(item, work_type=row_work_type)
        target_record_id = str(item.get("target_record_id") or item.get("record_id") or "")
        source_record_id = _explicit_source_id(item)
        status = _ongoing_display_status(item)
        site_photo_count = _site_photo_count(item)
        mop_status = _mop_status_text(item, row_work_type)
        needs_site_class = " needs-site-photo" if row_work_type in SITE_PHOTO_REQUIRED_WORK_TYPES and site_photo_count <= 0 else ""
        needs_mop_class = " needs-mop" if row_work_type == "maintenance" and ("未" in mop_status or not mop_status) else ""
        rows.append(
        f"<a class=\"ongoing-row{active}{needs_site_class}{needs_mop_class}\" href=\"{_e(url)}\" title=\"{_e(title)}\""
        f" aria-current=\"{'true' if active else 'false'}\""
        f" data-row-kind=\"ongoing\""
        f" data-direct-navigation=\"{'1' if direct_navigation else ''}\""
        f" data-work-type=\"{_e(row_work_type)}\""
        f" data-active-item-id=\"{_e(str(item.get('active_item_id') or ''))}\""
        f" data-record-id=\"{_e(str(item.get('record_id') or ''))}\""
        f" data-target-record-id=\"{_e(target_record_id)}\""
        f" data-source-record-id=\"{_e(source_record_id)}\""
        f" data-site-photo-count=\"{_e(site_photo_count)}\""
        f" data-mop-status=\"{_e(mop_status)}\""
        f" data-action=\"update\""
            f" data-title=\"{_e(title)}\""
        f" data-draft=\"{_e(_safe_draft_json_attr(draft))}\">"
            f"<span class=\"row-main\"><strong>{_e(title)}</strong>{_progress_badge(status)}</span>"
            f"{_row_meta(_record_building(item), _record_specialty(item), extra_chips=_record_extra_chips(item, row_work_type, ongoing=True))}"
            "</a>"
        )
    return "\n".join(rows)


def _attention_rows(
    items: list[dict[str, Any]],
    *,
    work_type: str,
    scope: str,
    month: str = "",
    pending_page: int | str = 1,
    ongoing_page: int | str = 1,
) -> str:
    rows: list[str] = []
    filter_work_type = "" if work_type == ALL_WORK_TYPE else work_type
    for item in items or []:
        if filter_work_type and _item_work_type(item) != filter_work_type:
            continue
        status_text = " ".join(
            str(value or "")
            for value in [
                item.get("status"),
                item.get("phase"),
                item.get("error"),
                item.get("last_error"),
                item.get("failure_reason"),
            ]
        )
        if not any(flag in status_text for flag in ("失败", "错误", "异常", "重试", "缺失", "不存在", "待处理")):
            continue
        title = _record_title(item)
        reason = str(item.get("error") or item.get("last_error") or item.get("failure_reason") or item.get("status") or "需要处理").strip()
        active_id = str(item.get("active_item_id") or item.get("target_record_id") or item.get("record_id") or "").strip()
        detail_url = _query_url(
            "/workbench-lite",
            scope=scope,
            work_type=work_type,
            month=month,
            active_item_id=active_id,
            pending_page=pending_page,
            ongoing_page=ongoing_page,
        )
        action_label = "重新绑定目标记录" if any(
            flag in reason for flag in ("目标多维记录不存在", "RecordIdNotFound", "record_id不存在", "缺少目标")
        ) else "打开处理"
        rows.append(
            "<article class=\"attention-row\">"
            f"<strong>{_e(title or '待处理通告')}</strong>"
            f"<span>{_e(reason[:120])}</span>"
            "<div class=\"attention-actions\">"
            f"<a class=\"btn ghost\" href=\"{_e(detail_url)}\">{_e(action_label)}</a>"
            "</div>"
            "</article>"
        )
    return "\n".join(rows) or "<div class=\"empty compact\">当前没有需要处理的问题</div>"


def _selected_source(
    records: list[dict[str, Any]],
    record_id: str,
    *,
    fallback_to_first: bool = True,
) -> dict[str, Any] | None:
    if not records:
        return None
    if record_id:
        for record in records:
            if str(record.get("record_id") or record.get("source_record_id") or "") == record_id:
                return record
    return records[0] if fallback_to_first else None


def _selected_ongoing(items: list[dict[str, Any]], active_item_id: str) -> dict[str, Any] | None:
    if not active_item_id:
        return None
    for item in items or []:
        candidates = {
            str(item.get("active_item_id") or ""),
            str(item.get("target_record_id") or ""),
            str(item.get("record_id") or ""),
        }
        if active_item_id in candidates:
            return item
    return None


def _linked_ongoing_for_source(
    record: dict[str, Any] | None,
    ongoing_items: list[dict[str, Any]] | None,
) -> dict[str, Any] | None:
    if not isinstance(record, dict):
        return None
    source_record_id = _source_id(record)
    work_type = _item_work_type(record)
    if not source_record_id:
        return None

    embedded = record.get("linked_ongoing")
    if isinstance(embedded, dict):
        candidates = [embedded]
    else:
        candidates = [
            item
            for item in ongoing_items or []
            if isinstance(item, dict)
            and _item_work_type(item) == work_type
            and _explicit_source_id(item) == source_record_id
        ]
    if len(candidates) != 1:
        return None

    candidate = candidates[0]
    if _item_work_type(candidate) != work_type:
        return None
    candidate_source_id = _explicit_source_id(candidate) or source_record_id
    if candidate_source_id != source_record_id:
        return None
    active_item_id = str(candidate.get("active_item_id") or "").strip()
    target_record_id = str(candidate.get("target_record_id") or "").strip()
    candidate_record_id = str(candidate.get("record_id") or "").strip()
    if not target_record_id and candidate_record_id != source_record_id:
        target_record_id = candidate_record_id
    if not (active_item_id or target_record_id):
        return None

    linked = dict(record)
    linked.update(candidate)
    linked["work_type"] = work_type
    linked["source_record_id"] = source_record_id
    if active_item_id:
        linked["active_item_id"] = active_item_id
    if target_record_id:
        linked["target_record_id"] = target_record_id
        linked["record_id"] = target_record_id
    linked["source_progress"] = str(candidate.get("status") or "进行中").strip()
    linked["source_status"] = linked["source_progress"]
    return linked


def _source_link_options(
    records: list[dict[str, Any]],
    ongoing_items: list[dict[str, Any]],
    *,
    work_type: str,
) -> list[dict[str, str]]:
    used_source_ids = {
        str(item.get("source_record_id") or "").strip()
        for item in ongoing_items or []
        if _item_work_type(item) == work_type and str(item.get("source_record_id") or "").strip()
    }
    options: list[dict[str, str]] = []
    for record in records or []:
        if _item_work_type(record) != work_type:
            continue
        source_id = _source_id(record)
        if not source_id or source_id in used_source_ids:
            continue
        progress = _record_progress(record)
        if _record_disabled_reason(progress) or not any(
            flag in progress
            for flag in ("未开始", "进行中", "未结束")
        ):
            continue
        options.append(
            {
                "source_record_id": source_id,
                "title": _record_title(record),
                "building": _record_building(record),
                "specialty": _record_specialty(record),
                "progress": progress,
            }
        )
    return options


def _manual_source_binding_panel() -> str:
    return """
        <section class="manual-source-binding" data-manual-source-binding>
          <input type="hidden" name="manual_binding_required" value="1">
          <input type="hidden" name="manual_binding_choice" value="">
          <input type="hidden" name="source_record_id" value="">
          <div class="manual-source-binding-head">
            <strong>计划通告关联</strong>
            <span id="lite-manual-binding-status">请选择绑定方式</span>
          </div>
          <div class="manual-source-binding-actions">
            <button class="btn ghost" type="button" data-manual-binding-mode="bind">绑定计划通告</button>
            <button class="btn ghost" type="button" data-manual-binding-mode="unbound">不绑定</button>
          </div>
        </section>
    """


def _source_link_select(
    *,
    ongoing_item: dict[str, Any] | None,
    current_source_id: str,
    options: list[dict[str, str]],
) -> str:
    _ = ongoing_item
    if current_source_id:
        return (
            "<label class=\"source-link-field readonly\"><span>源表</span>"
            "<div class=\"source-link-title\">已关联</div>"
            f"<input type=\"hidden\" name=\"source_record_id\" value=\"{_e(current_source_id)}\">"
            "</label>"
        )
    if not options:
        return (
            "<label class=\"source-link-field readonly\"><span>源表</span>"
            "<div class=\"source-link-title\">未关联</div>"
            "</label>"
        )
    option_html = [
        "<option value=\"\">不关联源表记录</option>"
    ]
    for item in options:
        label = " · ".join(
            part for part in [
                item.get("progress") or "",
                item.get("building") or "",
                item.get("specialty") or "",
                item.get("title") or "",
            ] if part
        )
        option_html.append(
            f"<option value=\"{_e(item.get('source_record_id'))}\">{_e(label)}</option>"
        )
    return (
        "<label class=\"source-link-field\"><span>源表</span>"
        "<div class=\"source-link-title\">可关联</div>"
        f"<select name=\"source_record_id\">{''.join(option_html)}</select>"
        "</label>"
    )


def _target_link_panel(work_type: str, target_record_id: str) -> str:
    work = _work_type(work_type)
    if work not in BINDABLE_TARGET_WORK_TYPES:
        return ""
    linked = bool(str(target_record_id or "").strip())
    status_text = "已绑定" if linked else "未绑定"
    return f"""
        <section class="target-link-panel">
          <div>
            <strong>目标多维</strong>
            <span id="lite-target-link-status">{_e(status_text)}</span>
          </div>
          <button class="btn ghost" id="lite-target-search" type="button">{_e('更换' if linked else '查找')}</button>
        </section>
    """


def _repair_event_link_panel(
    source: dict[str, Any],
    *,
    visible: bool,
    repair_management_record_id: str = "",
) -> str:
    event_record_ids = _relation_record_ids(source, "关联事件单")
    event_record_id = str(
        source.get("source_event_id")
        or (event_record_ids[0] if event_record_ids else "")
    ).strip()
    event_title = _first(
        source.get("event_title"),
        _field(source, "事件描述", "故障维修原因", "故障发生现象描述"),
    )
    status = event_title or ("已关联事件" if event_record_id else "未选择")
    repair_management_record_id = str(
        repair_management_record_id
        or source.get("repair_management_record_id")
        or ""
    ).strip()
    hidden_attr = "" if visible else " hidden"
    return f"""
        <section class="repair-event-link-panel" data-repair-event-link{hidden_attr}>
          <input type="hidden" name="related_event_record_id" value="{_e(event_record_id)}">
          <input type="hidden" name="repair_management_record_id" value="{_e(repair_management_record_id)}">
          <div>
            <strong>对应事件</strong>
            <span id="lite-repair-event-link-status">{_e(status)}</span>
          </div>
          <button class="btn ghost" id="lite-repair-event-open" type="button">对应事件关联</button>
        </section>
    """


def _mop_next_action_panel(
    *,
    scope: str,
    work_type: str,
    source_record_id: str,
    mop_status: str,
) -> str:
    if _work_type(work_type) != "maintenance":
        return ""
    params: dict[str, Any] = {"scope": scope}
    if source_record_id:
        params["source_record_id"] = source_record_id
    mop_url = _query_url("/engineer/mop", **params)
    status = mop_status or "MOP未绑定"
    if "已上传" in status or "已确认" in status:
        title = "维护单"
        detail = ""
        action = "查看MOP"
        tone = "ok"
    elif "未绑定" in status:
        title = "维护单"
        detail = ""
        action = "绑定MOP"
        tone = "warn"
    else:
        title = "维护单"
        detail = ""
        action = "处理MOP"
        tone = "ready"
    detail_html = f"<span>{_e(detail)}</span>" if detail else ""
    return (
        f"<section class=\"mop-action-panel {tone}\">"
        "<div>"
        f"<strong>{_e(title)}</strong>"
        f"{detail_html}"
        f"<em>{_e(status)}</em>"
        "</div>"
        f"<a class=\"btn ghost\" href=\"{_e(mop_url)}\">{_e(action)}</a>"
        "</section>"
    )


def _completion_hint(work_type: str) -> str:
    if work_type in {"maintenance", "change", "repair"}:
        return "整条通告任意一次上传过现场图片后即可结束，后端按累计现场图片校验。"
    return "当前类型结束不强制上传现场图片。"


def _detail_mode_note(
    *,
    ongoing_item: dict[str, Any] | None,
    manual: bool,
    parsed_draft: dict[str, str] | None,
    source_record_id: str,
    target_record_id: str,
) -> str:
    if ongoing_item:
        if source_record_id and target_record_id:
            return ""
        if target_record_id:
            return ""
        return "需绑定目标"
    if manual or parsed_draft:
        return "纯手填"
    if source_record_id:
        return ""
    return "待选择"


def _detail_form(
    *,
    record: dict[str, Any] | None,
    ongoing_item: dict[str, Any] | None,
    scope: str,
    work_type: str,
    manual: bool,
    source_month: str = "",
    parsed_draft: dict[str, str] | None = None,
    parsed_action: str = "",
    source_link_options: list[dict[str, str]] | None = None,
    is_admin: bool = False,
    prefill_draft: dict[str, str] | None = None,
    prefill_source_record_id: str = "",
    prefill_target_record_id: str = "",
    prefill_action: str = "",
    prefill_context_id: str = "",
) -> str:
    source = ongoing_item or record or {}
    work = _work_type(work_type) if work_type else _item_work_type(source)
    explicit_prefill_source_id = str(prefill_source_record_id or "").strip()
    effective_manual = bool(
        manual
        or parsed_draft
        or (
            not ongoing_item
            and not record
            and not explicit_prefill_source_id
        )
    )
    draft = _draft_from_record(source, manual=effective_manual, work_type=work)
    if parsed_draft:
        draft.update(parsed_draft)
    if prefill_draft:
        draft.update(prefill_draft)
        draft["start_time"] = _datetime_local(draft.get("start_time"))
        draft["end_time"] = _datetime_local(draft.get("end_time"))
    action = "start" if effective_manual or not ongoing_item else "update"
    if record and not ongoing_item:
        action = _action_for_record(record)
    if parsed_action in {"start", "update", "end"}:
        action = parsed_action
    if prefill_draft and prefill_action in {"start", "update", "end"}:
        action = prefill_action
    record_id = (
        explicit_prefill_source_id
        if prefill_draft
        else (
            ""
            if effective_manual
            else str(
                (record or {}).get("record_id")
                or (record or {}).get("source_record_id")
                or ""
            )
        )
    )
    source_record_id = (
        explicit_prefill_source_id
        if prefill_draft
        else ("" if effective_manual else str(source.get("source_record_id") or record_id))
    )
    target_record_id = (
        str(prefill_target_record_id or "").strip()
        if prefill_draft
        else (
            str(source.get("target_record_id") or source.get("record_id") or "")
            if ongoing_item
            else ""
        )
    )
    active_item_id = str(source.get("active_item_id") or "") if ongoing_item else ""
    site_photo_count = _site_photo_count(source)
    mop_status = _mop_status_text(source, work)
    require_manual_binding = bool(
        effective_manual and not parsed_draft and not prefill_draft
    )
    source_link_html = (
        _manual_source_binding_panel()
        if require_manual_binding
        else _source_link_select(
            ongoing_item=ongoing_item,
            current_source_id=source_record_id,
            options=source_link_options or [],
        )
    )
    hidden_source_input = "" if source_link_html else f'<input type="hidden" name="source_record_id" value="{_e(source_record_id)}">'
    title = (
        "维修单生成检修通告"
        if prefill_draft
        else (
            "解析粘贴通告"
            if parsed_draft
            else (
                "纯手填通告"
                if effective_manual
                else (_record_title(source) or "选择左侧事项")
            )
        )
    )
    disabled = "" if source or effective_manual or prefill_draft else " disabled"
    detail_mode = "ongoing" if ongoing_item else ("manual" if effective_manual else "source")
    mode_note = _detail_mode_note(
        ongoing_item=ongoing_item,
        manual=effective_manual,
        parsed_draft=parsed_draft,
        source_record_id=source_record_id,
        target_record_id=target_record_id,
    )
    mode_note_html = (
        f"<small class=\"detail-mode-note\">{_e(mode_note)}</small>"
        if mode_note
        else "<small class=\"detail-mode-note\" hidden></small>"
    )
    mop_action_panel = _mop_next_action_panel(
        scope=scope,
        work_type=work,
        source_record_id=source_record_id,
        mop_status=mop_status,
    )
    repair_event_link_panel = (
        _repair_event_link_panel(
            source,
            visible=bool(record) and not ongoing_item and not effective_manual,
            repair_management_record_id=str(
                source.get("repair_management_record_id")
                or prefill_context_id
                or (
                    source_record_id
                    if source_record_id and not effective_manual
                    else ""
                )
            ).strip(),
        )
        if work == "repair"
        else ""
    )
    if ongoing_item:
        admin_remove_button = (
            "<button class=\"btn danger-ghost\" type=\"button\" data-ongoing-delete-mode=\"local\">移除显示</button>"
            if is_admin
            else ""
        )
        action_buttons = (
            "<button class=\"btn primary\" type=\"submit\" name=\"submit_action\" value=\"update\">发送更新</button>"
            "<button class=\"btn danger\" type=\"submit\" name=\"submit_action\" value=\"end\">发送结束</button>"
            "<button class=\"btn danger-ghost\" type=\"button\" data-ongoing-delete-mode=\"remote\">删除通告</button>"
            f"{admin_remove_button}"
        )
    else:
        convert_change_button = (
            "<button class=\"btn ghost\" type=\"button\" data-convert-to-change=\"1\">转为变更通告</button>"
            if work == "maintenance" and record_id and not effective_manual
            else ""
        )
        revert_maintenance_button = (
            "<button class=\"btn ghost\" type=\"button\" data-revert-to-maintenance=\"1\">转回维保</button>"
            if work == "change"
            and record_id
            and not effective_manual
            and str(draft.get("converted_from_work_type") or draft.get("source_work_type") or "") == "maintenance"
            else ""
        )
        action_buttons = (
            f"<button class=\"btn primary\" type=\"submit\" name=\"submit_action\" value=\"{_e(action)}\"{disabled}>发送{_e('开始' if action == 'start' else '更新')}</button>"
            f"{convert_change_button}"
            f"{revert_maintenance_button}"
        )
    datalists = (
        f"<datalist id=\"specialty-options\">{''.join(f'<option value=\"{_e(item)}\"></option>' for item in SPECIALTY_OPTIONS)}</datalist>"
        f"<datalist id=\"maintenance-cycle-options\">{''.join(f'<option value=\"{_e(item)}\"></option>' for item in MAINTENANCE_CYCLE_OPTIONS)}</datalist>"
    )
    notice_type_value = str(draft.get("notice_type") or NOTICE_TYPE_BY_WORK_TYPE.get(work, "维保通告")).strip()
    if work == "power" and notice_type_value not in POWER_NOTICE_TYPES:
        notice_type_value = NOTICE_TYPE_BY_WORK_TYPE.get(work, "上电通告")
    notice_type_input = (
        ""
        if work == "power"
        else f"<input type=\"hidden\" name=\"notice_type\" value=\"{_e(notice_type_value)}\">"
    )
    converted_hidden_inputs = "".join(
        f"<input type=\"hidden\" name=\"{_e(key)}\" value=\"{_e(draft.get(key) or '')}\">"
        for key in [
            "source_work_type",
            "converted_from_work_type",
            "converted_to_work_type",
            "sync_maintenance_target",
            "paired_maintenance_target_record_id",
            "paired_maintenance_original_title",
            "paired_maintenance_actual_start_time",
        ]
    )
    manual_id = ""
    if effective_manual:
        context_id = str(prefill_context_id or "").strip()
        manual_id = (
            f"manual:repair-management:{context_id}"
            if context_id
            else f"manual:lite:{scope}:{work}"
        )
    return f"""
      <form id="lite-notice-form" class="detail-form" data-action="{_e(action)}" data-detail-mode="{_e(detail_mode)}" data-work-type="{_e(work)}">
        {datalists}
        <input type="hidden" name="scope" value="{_e(scope)}">
        <input type="hidden" name="source_month" value="{_e(source_month)}">
        <input type="hidden" name="work_type" value="{_e(work)}">
        {notice_type_input}
        {converted_hidden_inputs}
        <input type="hidden" name="manual" value="{_e('1' if effective_manual else '')}">
        <input type="hidden" name="manual_id" value="{_e(manual_id)}">
        <input type="hidden" name="record_id" value="{_e(record_id)}">
        {hidden_source_input}
        <input type="hidden" name="target_record_id" value="{_e(target_record_id)}">
        <input type="hidden" name="active_item_id" value="{_e(active_item_id)}">
        <input type="hidden" name="site_photo_count" value="{_e(site_photo_count)}">
        <input type="hidden" name="mop_status" value="{_e(mop_status)}">
        <header class="detail-head">
          <span>{_e(WORK_TYPE_LABELS.get(work, '通告'))}</span>
          <strong>{_e(title)}</strong>
          {mode_note_html}
        </header>
        {mop_action_panel}
        {source_link_html}
        {repair_event_link_panel}
        {_target_link_panel(work, target_record_id)}
        {_form_fields(work, draft)}
        {_site_photo_uploader(work, site_photo_count)}
        <section class="notice-preview" aria-live="polite">
          <div class="preview-head">
            <span>发送预览</span>
            <b id="lite-completion-hint">{_e(_completion_hint(work))}</b>
          </div>
          <pre id="lite-notice-preview"></pre>
        </section>
        <div class="form-actions">
          <label class="actual-action-time"><span>实际发送时间</span><input type="datetime-local" name="actual_action_time" data-auto-actual-time="1"></label>
          <span id="lite-job-status" class="job-status" aria-live="polite">等待操作</span>
          <span id="lite-action-reason" class="action-reason">请先确认标题和进度。</span>
          {action_buttons}
        </div>
      </form>
    """


def render_workbench_lite(
    *,
    payload: dict[str, Any],
    session: dict[str, Any],
    scope: str,
    work_type: str,
    month: str = "",
    search: str = "",
    specialty: str = "",
    record_id: str = "",
    active_item_id: str = "",
    pending_page: int | str = 1,
    ongoing_page: int | str = 1,
    manual: bool = False,
    scope_options: list[dict[str, Any]] | None = None,
    parsed_draft: dict[str, str] | None = None,
    parsed_action: str = "",
    paste_text: str = "",
    notice_undos: list[dict[str, Any]] | None = None,
    prefill_draft: dict[str, str] | None = None,
    prefill_source_record_id: str = "",
    prefill_target_record_id: str = "",
    prefill_action: str = "",
    prefill_context_id: str = "",
) -> str:
    records = payload.get("records") if isinstance(payload.get("records"), list) else []
    ongoing = payload.get("ongoing") if isinstance(payload.get("ongoing"), list) else []
    payload_filters = (
        payload.get("filters")
        if isinstance(payload.get("filters"), dict)
        else {}
    )
    selected_month = str(
        month
        or payload_filters.get("default_month")
        or f"{dt.datetime.now().month}月"
    ).strip()
    month_options = [
        str(item or "").strip()
        for item in (payload_filters.get("months") or [])
        if str(item or "").strip()
    ]
    if not month_options:
        current_start = dt.datetime(dt.datetime.now().year, dt.datetime.now().month, 1)
        previous_day = current_start - dt.timedelta(days=1)
        month_options = [
            f"{current_start.month}月",
            f"{previous_day.month}月",
        ]
    if selected_month and selected_month not in month_options:
        month_options.insert(0, selected_month)
    daily = payload.get("daily_summary") if isinstance(payload.get("daily_summary"), dict) else {}
    stats = daily.get("stats") if isinstance(daily.get("stats"), dict) else {}
    view_work = _view_work_type(work_type)
    work_filter = "" if view_work == ALL_WORK_TYPE else view_work
    payload_record_counts = payload.get("record_type_counts") if isinstance(payload.get("record_type_counts"), dict) else {}
    payload_ongoing_counts = payload.get("ongoing_type_counts") if isinstance(payload.get("ongoing_type_counts"), dict) else {}
    fallback_record_counts = {key: 0 for key in WORK_TYPE_LABELS}
    fallback_ongoing_counts = {key: 0 for key in WORK_TYPE_LABELS}
    for record in records:
        record_work = _item_work_type(record)
        fallback_record_counts[record_work] = fallback_record_counts.get(record_work, 0) + 1
    for item in ongoing:
        item_work = _item_work_type(item)
        fallback_ongoing_counts[item_work] = fallback_ongoing_counts.get(item_work, 0) + 1
    record_counts = {
        key: _to_int(payload_record_counts.get(key), fallback_record_counts.get(key, 0))
        for key in WORK_TYPE_LABELS
    }
    ongoing_counts = {
        key: _to_int(payload_ongoing_counts.get(key), fallback_ongoing_counts.get(key, 0))
        for key in WORK_TYPE_LABELS
    }
    record_counts[ALL_WORK_TYPE] = sum(record_counts.get(key, 0) for key in WORK_TYPE_LABELS)
    ongoing_counts[ALL_WORK_TYPE] = (
        sum(ongoing_counts.get(key, 0) for key in WORK_TYPE_LABELS)
        + _to_int(
            payload_ongoing_counts.get("event"),
            fallback_ongoing_counts.get("event", 0),
        )
    )
    records_pagination = (
        payload.get("records_pagination")
        if isinstance(payload.get("records_pagination"), dict)
        else {}
    )
    ongoing_pagination = (
        payload.get("ongoing_pagination")
        if isinstance(payload.get("ongoing_pagination"), dict)
        else {}
    )
    sorted_records = sorted(records, key=_record_sort_key)
    filtered_ongoing = [
        item for item in ongoing if not work_filter or _item_work_type(item) == work_filter
    ]
    if records_pagination:
        visible_records = sorted_records
        pending_page_num = _page_number(records_pagination.get("page"))
        pending_pages = max(1, _to_int(records_pagination.get("total_pages"), 1))
        current_pending_count = _to_int(records_pagination.get("total"), len(sorted_records))
        pending_page_size = max(1, _to_int(records_pagination.get("page_size"), PENDING_PAGE_SIZE))
    else:
        pending_page_num = _page_number(pending_page)
        visible_records, pending_page_num, pending_pages = _page_slice(
            sorted_records,
            pending_page_num,
            PENDING_PAGE_SIZE,
        )
        current_pending_count = len(sorted_records)
        pending_page_size = PENDING_PAGE_SIZE
    if ongoing_pagination:
        visible_ongoing = filtered_ongoing
        ongoing_page_num = _page_number(ongoing_pagination.get("page"))
        ongoing_pages = max(1, _to_int(ongoing_pagination.get("total_pages"), 1))
        current_ongoing_count = _to_int(ongoing_pagination.get("total"), len(filtered_ongoing))
        ongoing_page_size = max(1, _to_int(ongoing_pagination.get("page_size"), ONGOING_PAGE_SIZE))
    else:
        ongoing_page_num = _page_number(ongoing_page)
        visible_ongoing, ongoing_page_num, ongoing_pages = _page_slice(
            filtered_ongoing,
            ongoing_page_num,
            ONGOING_PAGE_SIZE,
        )
        current_ongoing_count = len(filtered_ongoing)
        ongoing_page_size = ONGOING_PAGE_SIZE
    effective_record_id = str(prefill_source_record_id or record_id or "").strip()
    detail_requested = bool(
        effective_record_id
        or active_item_id
        or manual
        or parsed_draft
        or prefill_draft
        or prefill_source_record_id
        or prefill_context_id
    )
    pager_base = {
        "scope": scope,
        "work_type": view_work,
        "month": selected_month,
        "search": search,
        "specialty": specialty,
        "record_id": effective_record_id,
        "active_item_id": active_item_id,
        "manual": "1" if manual else "",
        "repair_management_record_id": prefill_context_id,
    }
    pending_pager = _pagination_controls(
        label="计划通告列表",
        page_param="pending_page",
        page=pending_page_num,
        total_pages=pending_pages,
        total=current_pending_count,
        page_size=pending_page_size,
        base_params={**pager_base, "ongoing_page": ongoing_page_num},
    )
    ongoing_pager = _pagination_controls(
        label="未结束通告",
        page_param="ongoing_page",
        page=ongoing_page_num,
        total_pages=ongoing_pages,
        total=current_ongoing_count,
        page_size=ongoing_page_size,
        base_params={**pager_base, "pending_page": pending_page_num},
    )
    selected_ongoing = _selected_ongoing(ongoing, active_item_id)
    selected_record = (
        None
        if selected_ongoing or manual or parsed_draft
        else _selected_source(
            records,
            effective_record_id,
            fallback_to_first=not bool(prefill_source_record_id or prefill_draft),
        )
    )
    if selected_record and not selected_ongoing:
        selected_ongoing = _linked_ongoing_for_source(selected_record, ongoing)
    selected_record_id = (
        str((selected_record or {}).get("record_id") or "")
        if detail_requested
        else ""
    )
    detail_source = selected_ongoing or selected_record or {}
    detail_work = _item_work_type(detail_source) if detail_source else (work_filter or "maintenance")
    if parsed_draft:
        parsed_work = str(parsed_draft.get("work_type") or "").strip()
        parsed_notice_type = str(parsed_draft.get("notice_type") or "").strip()
        detail_work = _work_type(
            parsed_work
            if parsed_work in WORK_TYPE_LABELS
            else (WORK_TYPE_BY_NOTICE_TYPE.get(parsed_notice_type) or detail_work)
        )
    elif prefill_draft:
        detail_work = _work_type(prefill_draft.get("work_type") or work_filter or "repair")
    elif manual and work_filter:
        detail_work = work_filter
    source_options = _source_link_options(records, ongoing, work_type=detail_work)
    detail_drawer_open = detail_requested
    drawer_title = str(
        (parsed_draft or {}).get("title")
        or (prefill_draft or {}).get("title")
        or (_record_title(detail_source) if detail_source else "")
        or f"{WORK_TYPE_LABELS.get(detail_work, '通告')}通告"
    ).strip()
    user = session.get("user") if isinstance(session.get("user"), dict) else {}
    is_admin_session = bool(session.get("is_admin")) or str(session.get("role") or "").strip().lower() == "admin"
    scope_options = scope_options or []
    scope_select = "".join(
        f"<option value=\"{_e(option.get('value'))}\"{' selected' if str(option.get('value')) == scope else ''}>{_e(option.get('label') or option.get('value'))}</option>"
        for option in scope_options
    )
    type_tabs = "".join(
        f"<a class=\"type-tab{' active' if key == view_work else ''}\" href=\"{_e(_query_url('/workbench-lite', scope=scope, work_type=key, month=selected_month))}\" title=\"待发起 {record_counts.get(key, 0)}，进行中 {ongoing_counts.get(key, 0)}\""
        f" aria-current=\"{'page' if key == view_work else 'false'}\">"
        f"<span>{_e(label)}</span><span class=\"type-counts\" aria-label=\"待发起 {record_counts.get(key, 0)}，进行中 {ongoing_counts.get(key, 0)}\"><b class=\"type-count pending\">{_e(record_counts.get(key, 0))}</b><i>/</i><b class=\"type-count ongoing\">{_e(ongoing_counts.get(key, 0))}</b></span>"
        "</a>"
        for key, label in WORK_TYPE_FILTER_LABELS.items()
    )
    manual_url = _query_url(
        "/workbench-lite",
        scope=scope,
        work_type=(work_filter or "maintenance"),
        month=selected_month,
        manual="1",
    )
    undo_items = notice_undos or []
    source_loaded_at = str(payload.get("last_loaded_at") or "").strip()
    source_loaded_text = source_loaded_at or "暂无成功同步时间"
    undo_html = "".join(
        f"<button class=\"undo-row\" type=\"button\" data-undo-id=\"{_e(item.get('undo_id'))}\""
        f" data-undo-title=\"{_e(item.get('title') or item.get('undo_label') or '可回退通告')}\""
        f" data-undo-action=\"{_e(item.get('undo_label') or item.get('undo_action_type') or '回退')}\""
        f" data-undo-time=\"{_e(_undo_created_time(item.get('undo_created_at')))}\">"
        f"<strong>{_e(item.get('title') or item.get('undo_label') or '可回退通告')}</strong>"
        f"<span>{_e(item.get('undo_label') or item.get('undo_action_type') or '回退')}</span>"
        "</button>"
        for item in undo_items[:12]
    ) or "<div class=\"empty compact\">近三天暂无可回退通告</div>"
    attention_html = _attention_rows(
        ongoing,
        work_type=view_work,
        scope=scope,
        month=selected_month,
        pending_page=pending_page_num,
        ongoing_page=ongoing_page_num,
    )
    attention_count = 0 if "当前没有需要处理的问题" in attention_html else attention_html.count("attention-row")
    undo_count = len(undo_items[:12])
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>南通基地-运维灯塔工作台</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ margin:0; min-height:100vh; font-family:"Microsoft YaHei", Arial, sans-serif; color:#08204a; background:linear-gradient(180deg,#eaf3ff 0,#f6f9ff 42%,#eef5ff 100%); }}
    .visually-hidden {{ position:absolute !important; width:1px; height:1px; padding:0; margin:-1px; overflow:hidden; clip:rect(0,0,0,0); white-space:nowrap; border:0; }}
    .topbar {{ min-height:96px; padding:18px 34px 28px; display:flex; justify-content:space-between; align-items:center; color:#fff; background:linear-gradient(120deg,#0c57d8,#07348d); background-image:linear-gradient(rgba(255,255,255,.07) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.07) 1px,transparent 1px),linear-gradient(120deg,#0c57d8,#07348d); background-size:64px 64px,64px 64px,auto; border-bottom:1px solid rgba(255,255,255,.16); box-shadow:0 18px 42px rgba(7,52,141,.22); }}
    .brand {{ display:flex; gap:24px; align-items:center; }}
    .logo {{ font-weight:900; line-height:1.05; border-right:1px solid rgba(255,255,255,.35); padding-right:24px; }}
    .brand h1 {{ margin:0; font-size:27px; letter-spacing:0; line-height:1.16; }}
    .brand p {{ margin:5px 0 0; color:#dbeafe; line-height:1.45; font-size:13px; font-weight:700; }}
    .top-actions {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; justify-content:flex-end; }}
    .top-actions a,.top-actions button,.scope-switch {{ min-height:44px; border:1px solid rgba(255,255,255,.34); border-radius:18px; padding:0 16px; color:#fff; background:rgba(255,255,255,.14); text-decoration:none; font-weight:900; box-shadow:inset 0 1px 0 rgba(255,255,255,.16); transition:background .16s ease, border-color .16s ease, box-shadow .16s ease; cursor:pointer; }}
    .top-actions a,.top-actions button,.scope-switch {{ display:inline-flex; align-items:center; justify-content:center; gap:8px; }}
    .top-actions a:hover,.top-actions button:hover,.scope-switch:hover {{ background:rgba(255,255,255,.22); border-color:rgba(255,255,255,.5); box-shadow:0 10px 24px rgba(0,0,0,.12), inset 0 1px 0 rgba(255,255,255,.18); }}
    .top-actions a:focus-visible,.top-actions button:focus-visible,.scope-switch:focus-within,.scope-select:focus-visible,.btn:focus-visible,.type-tab:focus-visible,.notice-row:focus-visible,.ongoing-row:focus-visible,input:focus-visible,select:focus-visible,textarea:focus-visible {{ outline:3px solid rgba(75,153,255,.38); outline-offset:2px; }}
    .scope-switch {{ min-width:188px; gap:10px; padding:6px 8px 6px 10px; border-color:rgba(255,255,255,.78); background:linear-gradient(135deg,#ffffff,#eff6ff); color:#0c3f99; box-shadow:0 12px 26px rgba(4,46,145,.16), inset 0 1px 0 rgba(255,255,255,.72); }}
    .scope-icon {{ flex:0 0 auto; width:32px; height:32px; border-radius:13px; display:grid; place-items:center; color:#fff; background:linear-gradient(180deg,#1f63ff,#00aeda); font-size:13px; font-weight:950; box-shadow:0 8px 18px rgba(31,99,255,.24); }}
    .scope-switch span {{ margin:0; color:#5873a3; font-size:11px; font-weight:950; line-height:1; white-space:nowrap; }}
    .scope-select {{ min-width:88px; min-height:34px; border:1px solid #cfe0ff; border-radius:14px; padding:0 30px 0 12px; color:#073f9d; background:#f7fbff; cursor:pointer; font-weight:950; box-shadow:inset 0 0 0 1px rgba(255,255,255,.7); }}
    .top-actions .top-link {{ min-width:92px; }}
    .top-actions .exit {{ color:#c03943; background:#fff; border-color:#fff; min-width:76px; }}
    .shell {{ max-width:1840px; margin:-18px auto 0; padding:0 22px 28px; }}
    .status {{ position:relative; max-width:1180px; min-height:38px; display:flex; align-items:center; gap:10px; margin:0 auto 12px; border:1px solid #d6e4f7; border-radius:999px; padding:8px 16px 8px 44px; background:linear-gradient(135deg,rgba(255,255,255,.96),rgba(244,249,255,.94)); color:#506783; box-shadow:0 10px 22px rgba(15,73,153,.08); font-size:12px; font-weight:850; }}
    .status::before {{ content:""; position:absolute; left:12px; top:50%; width:21px; height:21px; margin-top:-10px; border-radius:9px; background:linear-gradient(180deg,#1f63ff,#00aeda); box-shadow:0 6px 14px rgba(31,99,255,.18); }}
    .status::after {{ content:""; position:absolute; left:20px; top:50%; width:6px; height:10px; margin-top:-7px; border:solid #fff; border-width:0 2px 2px 0; transform:rotate(45deg); }}
    body.has-dirty-lite-form .status {{ border-color:#ffd599; color:#8a4b00; background:linear-gradient(135deg,#fff8eb,#fff); }}
    body.has-dirty-lite-form .status::before {{ background:linear-gradient(180deg,#f59e0b,#f97316); }}
    .summary {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:10px; margin-bottom:10px; }}
    .summary article {{ position:relative; overflow:hidden; min-height:58px; border:1px solid #d8e5f7; border-radius:18px; padding:9px 16px 9px 42px; background:rgba(255,255,255,.94); box-shadow:0 10px 22px rgba(15,73,153,.08); }}
    .summary article::before {{ content:""; position:absolute; left:16px; top:18px; width:15px; height:15px; border-radius:7px; background:linear-gradient(180deg,#1f63ff,#00aeda); box-shadow:0 6px 14px rgba(31,99,255,.2); }}
    .summary article:nth-child(2)::before {{ background:linear-gradient(180deg,#16b6d6,#10b981); }}
    .summary article:nth-child(3)::before {{ background:linear-gradient(180deg,#12b886,#26c281); }}
    .summary article:nth-child(4)::before {{ background:linear-gradient(180deg,#315fbd,#5b8cff); }}
    .summary strong {{ display:block; font-size:12px; color:#53677f; }}
    .summary b {{ display:block; margin-top:1px; font-size:23px; color:#0a4fc4; line-height:1.05; }}
    .workbench-guide {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:8px; margin-bottom:10px; }}
    .guide-step {{ position:relative; min-height:42px; border:1px solid #d8e5f7; border-radius:16px; padding:8px 10px 8px 42px; background:rgba(255,255,255,.92); box-shadow:0 8px 18px rgba(15,73,153,.05); }}
    .guide-step::before {{ content:attr(data-step); position:absolute; left:10px; top:9px; width:24px; height:24px; border-radius:10px; display:grid; place-items:center; color:#fff; background:linear-gradient(180deg,#1f63ff,#00aeda); font-size:12px; font-weight:900; box-shadow:0 6px 14px rgba(31,99,255,.14); }}
    .guide-step strong {{ display:block; color:#0c244d; font-size:13px; line-height:1.25; }}
    .guide-step span {{ display:block; margin-top:1px; color:#64748b; font-size:11px; line-height:1.25; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    .toolbar {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; border:1px solid #d8e5f7; border-radius:18px; padding:8px; background:rgba(255,255,255,.95); box-shadow:0 10px 24px rgba(15,73,153,.08); position:relative; }}
    .type-tabs {{ display:flex; gap:6px; flex-wrap:wrap; }}
    .type-tab {{ min-height:36px; padding:6px 8px 6px 11px; border-radius:14px; color:#12345f; text-decoration:none; font-size:13px; font-weight:900; background:#f4f8ff; display:inline-flex; align-items:center; gap:7px; border:1px solid transparent; transition:background .16s ease, border-color .16s ease, box-shadow .16s ease; }}
    .type-tab:hover {{ border-color:#b9d7ff; background:#eaf3ff; }}
    .type-tab.active,.type-tab.is-loading {{ color:#fff; background:linear-gradient(180deg,#1e63ff,#00aeda); }}
    .type-counts {{ min-height:21px; border-radius:999px; padding:2px 5px; display:inline-flex; align-items:center; justify-content:center; gap:3px; color:#0a57d8; background:#fff; box-shadow:inset 0 0 0 1px rgba(31,99,255,.12); }}
    .type-counts i {{ color:#8aa3c7; font-style:normal; font-size:10px; }}
    .type-count {{ min-width:18px; height:17px; border-radius:999px; padding:0 4px; display:inline-flex; align-items:center; justify-content:center; font-size:10px; line-height:1; }}
    .type-count.pending {{ color:#0a57d8; background:#eef6ff; }}
    .type-count.ongoing {{ color:#087f5b; background:#e8fff5; }}
    .type-tab.active .type-counts,.type-tab.is-loading .type-counts {{ color:#0a57d8; background:#fff; }}
    .toolbar form {{ margin-left:auto; min-width:min(100%, 360px); flex:1 1 420px; display:flex; flex-wrap:wrap; gap:8px; align-items:center; justify-content:flex-end; }}
    input,select,textarea {{ width:100%; min-height:34px; border:1px solid #d5e2f2; border-radius:10px; padding:7px 10px; font:inherit; font-size:13px; line-height:1.35; background:#fff; }}
    select {{ appearance:none; -webkit-appearance:none; padding-right:28px; color:#073f9d; font-weight:900; background:linear-gradient(45deg,transparent 50%,#0a57d8 50%) calc(100% - 15px) 50%/6px 6px no-repeat,linear-gradient(135deg,#0a57d8 50%,transparent 50%) calc(100% - 10px) 50%/6px 6px no-repeat,linear-gradient(180deg,#ffffff,#f4f8ff); box-shadow:inset 0 0 0 1px rgba(255,255,255,.66); cursor:pointer; }}
    select:hover {{ border-color:#9cc7ff; background:linear-gradient(45deg,transparent 50%,#075bd8 50%) calc(100% - 15px) 50%/6px 6px no-repeat,linear-gradient(135deg,#075bd8 50%,transparent 50%) calc(100% - 10px) 50%/6px 6px no-repeat,linear-gradient(180deg,#ffffff,#eaf3ff); }}
    #lite-notice-form textarea {{ height:34px; min-height:34px; overflow:auto; resize:vertical; }}
    .toolbar input {{ min-width:220px; flex:1 1 240px; }}
    .toolbar select {{ min-width:90px; max-width:112px; flex:0 0 104px; border-radius:999px; padding-left:12px; font-size:12px; text-align:left; }}
    .btn {{ min-height:38px; border:0; border-radius:12px; padding:8px 14px; font-size:13px; font-weight:900; cursor:pointer; text-decoration:none; display:inline-flex; align-items:center; justify-content:center; transition:background .16s ease, border-color .16s ease, box-shadow .16s ease; }}
    .btn:hover {{ box-shadow:0 10px 22px rgba(21,99,255,.14); }}
    .btn.primary {{ color:#fff; background:linear-gradient(180deg,#1f63ff,#0097d7); box-shadow:0 10px 18px rgba(21,99,255,.2); }}
    .btn.ghost {{ color:#0a4fc4; background:#eef6ff; }}
    .btn.danger {{ color:#fff; background:#e04d5f; }}
    .btn.danger-ghost {{ color:#b42318; border:1px solid #ffc7bf; background:#fff1f0; }}
    .btn.danger-ghost:hover {{ border-color:#f58b88; background:#ffe9e7; }}
    .btn.is-busy,.btn[aria-busy="true"] {{ position:relative; color:transparent !important; pointer-events:none; }}
    .btn.is-busy::after,.btn[aria-busy="true"]::after {{ content:""; width:16px; height:16px; border-radius:999px; border:2px solid rgba(255,255,255,.55); border-top-color:#fff; animation:liteSpin .8s linear infinite; position:absolute; inset:auto; }}
    .btn.ghost.is-busy::after,.btn.ghost[aria-busy="true"]::after {{ border-color:rgba(10,79,196,.25); border-top-color:#0a4fc4; }}
    .btn.danger-ghost.is-busy::after,.btn.danger-ghost[aria-busy="true"]::after {{ border-color:rgba(180,35,24,.22); border-top-color:#b42318; }}
    button:disabled,.btn[disabled] {{ cursor:not-allowed; opacity:.58; box-shadow:none; transform:none; }}
    .manual-picker {{ position:relative; display:inline-flex; }}
    .manual-menu {{ position:absolute; z-index:20; top:50px; left:0; width:310px; display:none; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; padding:10px; border:1px solid #c9def8; border-radius:18px; background:#fff; box-shadow:0 18px 42px rgba(15,73,153,.18); }}
    .manual-picker.open .manual-menu {{ display:grid; }}
    .manual-menu a {{ border:1px solid #dce8f8; border-radius:14px; padding:10px 12px; color:#12345f; background:#f7fbff; text-decoration:none; font-weight:900; text-align:center; }}
    .manual-menu a:hover {{ border-color:#1f63ff; background:#eaf3ff; }}
    .refresh-picker {{ position:relative; display:inline-flex; }}
    .refresh-menu {{ position:absolute; z-index:20; top:50px; left:0; width:230px; display:none; gap:6px; padding:8px; border:1px solid #c9def8; border-radius:18px; background:#fff; box-shadow:0 18px 42px rgba(15,73,153,.18); }}
    .refresh-picker.open .refresh-menu {{ display:grid; }}
    .refresh-menu-head {{ display:flex; align-items:center; justify-content:space-between; gap:8px; border:1px solid #dce8f8; border-radius:14px; padding:8px 10px; background:linear-gradient(135deg,#f8fbff,#eef6ff); color:#12345f; }}
    .refresh-menu-head strong {{ font-size:12px; font-weight:950; white-space:nowrap; }}
    .refresh-menu-head span {{ min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:#64748b; font-size:11px; font-weight:800; line-height:1.2; }}
    .refresh-menu button {{ width:100%; min-height:36px; border:1px solid #dce8f8; border-radius:13px; padding:7px 10px; color:#12345f; background:#f7fbff; font-weight:900; text-align:left; cursor:pointer; }}
    .refresh-menu button:hover {{ border-color:#1f63ff; background:#eaf3ff; }}
    .refresh-menu button[aria-busy="true"] {{ position:relative; color:transparent; pointer-events:none; }}
    .refresh-menu button[aria-busy="true"]::after {{ content:""; width:16px; height:16px; border-radius:999px; border:2px solid rgba(10,79,196,.25); border-top-color:#0a4fc4; animation:liteSpin .8s linear infinite; position:absolute; left:18px; top:50%; margin-top:-8px; }}
    .refresh-menu small {{ display:none; }}
    .lite-tools {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-top:16px; }}
    .paste-box,.undo-box {{ border:1px solid #d8e5f7; border-radius:22px; padding:14px; background:rgba(255,255,255,.94); box-shadow:0 12px 30px rgba(15,73,153,.07); }}
    .paste-box h2,.undo-box h2 {{ margin:0 0 10px; font-size:16px; }}
    .paste-box form {{ display:grid; gap:10px; }}
    .paste-box textarea {{ min-height:92px; resize:vertical; }}
    .undo-list {{ display:grid; gap:8px; max-height:180px; overflow:auto; }}
    .undo-row {{ text-align:left; border:1px solid #dce8f8; border-radius:14px; background:#fff; padding:10px; cursor:pointer; }}
    .undo-row strong {{ display:block; color:#0c244d; }}
    .undo-row span {{ color:#0a57d8; font-weight:900; font-size:12px; }}
    .empty.compact {{ padding:10px; }}
    .workspace {{ display:grid; grid-template-columns:minmax(0,1fr); gap:12px; margin-top:10px; align-items:start; }}
    .workspace.is-switching {{ position:relative; min-height:420px; }}
    .workspace.is-switching > * {{ opacity:0; pointer-events:none; }}
    .workspace.is-switching::before {{ content:""; position:absolute; inset:0; z-index:3; border:1px solid #d8e5f7; border-radius:20px; background:linear-gradient(135deg,rgba(255,255,255,.96),rgba(238,246,255,.94)); box-shadow:0 14px 32px rgba(15,73,153,.08); }}
    .workspace.is-switching::after {{ content:attr(data-loading-text); position:absolute; left:50%; top:50%; z-index:4; transform:translate(-50%,-50%); min-width:180px; min-height:42px; border-radius:999px; padding:11px 22px; display:flex; align-items:center; justify-content:center; color:#0a57d8; background:#fff; border:1px solid #cfe0ff; box-shadow:0 12px 28px rgba(31,99,255,.14); font-size:14px; font-weight:950; }}
    .task-inbox {{ position:relative; display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; overflow:visible; align-self:start; }}
    .task-inbox.panel {{ padding:10px; }}
    .inbox-head {{ grid-column:1 / -1; display:grid; grid-template-columns:minmax(0,1fr) minmax(240px,360px); gap:12px; align-items:center; }}
    .inbox-title {{ margin:0; display:flex; align-items:center; justify-content:space-between; gap:10px; font-size:16px; }}
    .inbox-summary {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:7px; }}
    .inbox-summary span {{ min-width:0; display:flex; align-items:center; justify-content:space-between; gap:6px; border:1px solid #dce8f8; border-radius:13px; padding:7px 9px; color:#53677f; background:#f8fbff; font-size:11px; font-weight:950; }}
    .inbox-summary b {{ color:#0a57d8; font-size:14px; }}
    .inbox-section {{ border:1px solid #d8e5f7; border-radius:16px; padding:8px; background:linear-gradient(180deg,#fff,#f8fbff); box-shadow:0 6px 16px rgba(15,73,153,.04); }}
    .inbox-section h3 {{ margin:0 0 8px; display:flex; align-items:center; justify-content:space-between; gap:10px; color:#0c244d; font-size:14px; line-height:1.2; }}
    .inbox-section h3 b {{ min-width:28px; height:22px; border-radius:999px; display:inline-flex; align-items:center; justify-content:center; color:#0a57d8; background:#eaf3ff; font-size:12px; font-weight:950; }}
    .inbox-section .list {{ max-height:54vh; }}
    .inbox-section.ongoing-inbox-panel h3 b {{ color:#087443; background:#e8fff3; }}
    body.notice-drawer-open {{ overflow:hidden; }}
    .notice-detail-overlay {{ position:fixed; z-index:180; inset:0; display:none; justify-content:flex-end; background:rgba(8,25,52,.5); backdrop-filter:blur(3px); }}
    .notice-detail-overlay.open {{ display:flex; }}
    .panel.notice-detail-drawer {{ width:min(1180px,calc(100vw - 72px)); height:100%; min-width:0; overflow:hidden; display:grid; grid-template-rows:auto minmax(0,1fr); border-radius:16px 0 0 16px; border-right:0; padding:0; background:#fff; box-shadow:-18px 0 56px rgba(8,37,82,.22); isolation:isolate; }}
    .panel.notice-detail-drawer::before {{ display:none; }}
    .notice-detail-drawer.loading::after {{ top:82px; right:22px; }}
    .notice-drawer-head {{ position:relative; z-index:2; min-height:72px; display:flex; align-items:center; justify-content:space-between; gap:16px; padding:12px 18px; border-bottom:1px solid #d8e5f7; background:linear-gradient(135deg,#f8fbff,#eef6ff); }}
    .notice-drawer-title {{ min-width:0; display:grid; gap:3px; }}
    .notice-drawer-title span {{ width:max-content; border-radius:999px; padding:4px 9px; color:#0a57d8; background:#eaf3ff; font-size:11px; font-weight:950; }}
    .notice-drawer-title h2 {{ min-width:0; margin:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:#0c244d; font-size:18px; line-height:1.25; }}
    .notice-drawer-close {{ flex:0 0 auto; width:38px; height:38px; display:grid; place-items:center; border:1px solid #cfe0f5; border-radius:13px; color:#0a57d8; background:#fff; cursor:pointer; font-size:25px; line-height:1; box-shadow:0 8px 18px rgba(15,73,153,.08); }}
    .notice-drawer-close:hover {{ border-color:#1f63ff; background:#eef6ff; }}
    .notice-drawer-close:focus-visible {{ outline:3px solid rgba(31,99,255,.22); outline-offset:2px; }}
    .notice-drawer-body {{ position:relative; z-index:1; min-height:0; overflow:auto; display:flex; flex-direction:column; align-items:stretch; gap:9px; padding:12px 16px 20px; }}
    .notice-drawer-body > .paste-drawer,.notice-drawer-body > .detail-form {{ flex:0 0 auto; }}
    .detail-panel {{ min-width:0; }}
    .panel {{ position:relative; border:1px solid #d8e5f7; border-radius:18px; padding:10px; background:rgba(255,255,255,.95); box-shadow:0 10px 24px rgba(15,73,153,.08); }}
    .panel::before {{ content:""; position:absolute; left:14px; right:14px; top:0; height:3px; border-radius:0 0 999px 999px; background:linear-gradient(90deg,#1f63ff,#00aeda); opacity:.72; }}
    .panel.loading {{ position:relative; opacity:.7; pointer-events:none; }}
    .panel.loading::after {{ content:"正在加载"; position:absolute; right:14px; top:14px; border-radius:999px; padding:5px 10px; color:#075bd8; background:#eaf3ff; font-size:12px; font-weight:900; }}
    .panel h2 {{ margin:0 0 9px; font-size:16px; }}
    .panel-title {{ display:flex; align-items:center; justify-content:space-between; gap:10px; }}
    .panel-title .panel-count {{ min-width:30px; height:24px; border-radius:999px; display:inline-flex; align-items:center; justify-content:center; color:#0a57d8; background:#eaf3ff; font-size:12px; font-weight:900; }}
    .list {{ display:grid; gap:6px; max-height:70vh; overflow:auto; padding-right:3px; }}
    .list-pagination {{ display:flex; align-items:center; justify-content:space-between; gap:8px; margin-top:8px; border:1px solid #dce8f8; border-radius:14px; padding:7px 8px; background:linear-gradient(135deg,#fbfdff,#f1f7ff); }}
    .page-summary {{ color:#64748b; font-size:11px; font-weight:900; white-space:nowrap; }}
    .page-links {{ display:flex; align-items:center; justify-content:flex-end; gap:4px; flex-wrap:wrap; }}
    .page-btn {{ min-width:28px; min-height:26px; border:1px solid #cfe0f5; border-radius:10px; padding:4px 8px; display:inline-flex; align-items:center; justify-content:center; color:#0a57d8; background:#fff; text-decoration:none; font-size:11px; font-weight:950; line-height:1; }}
    .page-btn:hover {{ border-color:#1f63ff; background:#eaf3ff; }}
    .page-btn.current {{ color:#fff; border-color:#1f63ff; background:linear-gradient(180deg,#1f63ff,#00aeda); box-shadow:0 6px 14px rgba(31,99,255,.16); }}
    .page-btn.disabled {{ color:#94a3b8; background:#f3f7fd; cursor:not-allowed; }}
    .page-ellipsis {{ min-width:16px; color:#94a3b8; text-align:center; font-size:11px; font-weight:950; }}
    .rail-panel {{ position:relative; border:1px solid #d8e5f7; border-radius:18px; padding:12px; background:rgba(255,255,255,.95); box-shadow:0 10px 24px rgba(15,73,153,.07); overflow:hidden; }}
    .rail-panel::before {{ content:""; position:absolute; left:14px; right:14px; top:0; height:3px; border-radius:0 0 999px 999px; background:linear-gradient(90deg,#1f63ff,#00aeda); opacity:.62; }}
    .rail-panel h2 {{ margin:0 0 9px; font-size:16px; }}
    .rail-panel.undo::before {{ background:linear-gradient(90deg,#f59e0b,#22c55e); }}
    .rail-panel.attention::before {{ background:linear-gradient(90deg,#ef4444,#f59e0b); }}
    .rail-panel .list {{ max-height:42vh; }}
    .rail-fold {{ border:1px solid #d8e5f7; border-radius:16px; background:rgba(255,255,255,.95); box-shadow:0 8px 18px rgba(15,73,153,.06); overflow:hidden; }}
    .rail-fold summary {{ min-height:38px; display:flex; align-items:center; justify-content:space-between; gap:10px; padding:8px 12px; cursor:pointer; color:#0c244d; font-size:14px; font-weight:950; list-style:none; }}
    .rail-fold summary::-webkit-details-marker {{ display:none; }}
    .rail-fold summary::after {{ content:"展开"; border-radius:999px; padding:3px 8px; color:#0a57d8; background:#eaf3ff; font-size:11px; font-weight:900; }}
    .rail-fold[open] summary::after {{ content:"收起"; }}
    .rail-fold .rail-panel {{ border:0; border-radius:0; box-shadow:none; padding:0 12px 12px; background:transparent; }}
    .rail-fold .rail-panel::before,.rail-fold .rail-panel h2 {{ display:none; }}
    .undo-panel-list {{ display:grid; gap:6px; max-height:180px; overflow:auto; padding-right:3px; }}
    .attention-list {{ display:grid; gap:6px; max-height:160px; overflow:auto; padding-right:3px; }}
    .attention-row {{ border:1px solid #ffd3b0; border-radius:14px; padding:10px; background:#fff8ed; }}
    .attention-row strong {{ display:block; color:#7c2d12; font-size:13px; line-height:1.35; }}
    .attention-row span {{ display:block; margin-top:4px; color:#9a3412; font-size:12px; line-height:1.45; overflow-wrap:anywhere; }}
    .attention-actions {{ display:flex; justify-content:flex-end; gap:8px; margin-top:8px; }}
    .attention-actions .btn {{ min-height:30px; padding:5px 10px; border-radius:10px; font-size:12px; }}
    .notice-row,.ongoing-row {{ position:relative; min-width:0; display:grid; gap:5px; border:1px solid #dce8f8; border-radius:13px; padding:8px 10px 8px 13px; color:#0c244d; text-decoration:none; background:#fff; transition:border-color .12s ease, box-shadow .12s ease, transform .12s ease, background .12s ease; overflow:hidden; }}
    .notice-row::before,.ongoing-row::before {{ content:""; position:absolute; left:0; top:10px; bottom:10px; width:4px; border-radius:999px; background:#cfe0f5; }}
    .notice-row.active::before,.ongoing-row.active::before {{ background:#1f63ff; }}
    .notice-row:hover,.ongoing-row:hover {{ border-color:#9cc7ff; box-shadow:0 8px 18px rgba(31,99,255,.08); transform:translateY(-1px); }}
    .notice-row.active,.ongoing-row.active {{ border-color:#1f63ff; box-shadow:0 0 0 3px rgba(31,99,255,.12), 0 10px 22px rgba(31,99,255,.09); background:linear-gradient(180deg,#fff,#f4f9ff); }}
    .notice-row.needs-site-photo:not(.active),.ongoing-row.needs-site-photo:not(.active) {{ border-color:#ffd899; background:linear-gradient(180deg,#fff,#fffaf0); }}
    .notice-row.needs-site-photo::before,.ongoing-row.needs-site-photo::before {{ background:#f59e0b; }}
    .notice-row.needs-mop:not(.active),.ongoing-row.needs-mop:not(.active) {{ box-shadow:inset 0 -2px 0 rgba(245,158,11,.18); }}
    .notice-row.is-disabled {{ cursor:not-allowed; background:#f7f9fc; opacity:.76; }}
    .notice-row.is-disabled:hover {{ transform:none; box-shadow:none; border-color:#dce8f8; }}
    .notice-row.is-loading,.ongoing-row.is-loading {{ border-color:#1f63ff; background:#eef6ff; box-shadow:0 0 0 3px rgba(31,99,255,.14); }}
    .notice-row.is-loading::after,.ongoing-row.is-loading::after {{ content:"正在打开"; width:max-content; justify-self:end; border-radius:999px; padding:3px 8px; color:#0a57d8; background:#fff; font-size:11px; font-weight:900; }}
    .ongoing-row.optimistic {{ border-color:#6aa8ff; background:linear-gradient(180deg,#f7fbff,#eef6ff); }}
    .ongoing-row.optimistic::after {{ content:"同步中"; width:max-content; justify-self:end; border-radius:999px; padding:3px 8px; color:#0a57d8; background:#fff; font-size:11px; font-weight:900; }}
    .ongoing-row.failed {{ border-color:#f5a3aa; background:linear-gradient(180deg,#fffafa,#fff1f0); }}
    .ongoing-row.failed::before {{ background:#e04d5f; }}
    .notice-row strong,.ongoing-row strong {{ min-width:0; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; overflow-wrap:anywhere; font-size:12px; line-height:1.32; }}
    .row-main {{ min-width:0; display:grid; grid-template-columns:minmax(0,1fr) auto; gap:8px; align-items:start; }}
    .row-status {{ white-space:nowrap; border-radius:999px; padding:3px 7px; font-size:10px; font-weight:900; }}
    .row-status.ready {{ color:#075fb8; background:#e7f2ff; }}
    .row-status.working {{ color:#8a4b00; background:#fff4d6; }}
    .row-status.done {{ color:#475569; background:#eef2f7; }}
    .row-status.danger {{ color:#b42318; background:#fff1f0; }}
    .row-status.muted {{ color:#64748b; background:#f1f5f9; }}
    .row-meta {{ display:flex; flex-wrap:wrap; gap:4px; align-items:center; }}
    .meta-chip {{ max-width:100%; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; border-radius:999px; padding:2px 6px; color:#53677f; background:#f1f6fd; font-size:10px; font-weight:900; }}
    .meta-chip.ready {{ color:#075fb8; background:#e7f2ff; }}
    .meta-chip.muted {{ color:#64748b; background:#eef2f7; }}
    .meta-chip.warn {{ color:#8a4b00; background:#fff4d6; }}
    .meta-chip.success {{ color:#087443; background:#e8fff3; }}
    .meta-chip.source {{ color:#0a57d8; background:#eaf3ff; }}
    .meta-chip.manual {{ color:#7c3aed; background:#f3e8ff; }}
    .detail-head {{ display:grid; grid-template-columns:auto minmax(0,1fr) auto; align-items:center; gap:6px; margin-bottom:5px; border:1px solid #e3edf9; border-radius:13px; padding:6px 8px; background:linear-gradient(135deg,#f8fbff,#eef6ff); }}
    .detail-head span {{ width:max-content; border-radius:999px; padding:3px 7px; color:#0a57d8; background:#eaf3ff; font-size:11px; font-weight:900; }}
    .detail-head strong {{ min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-size:14px; line-height:1.22; }}
    .detail-head em {{ color:#64748b; font-style:normal; }}
    .detail-mode-note {{ width:max-content; max-width:100%; border-radius:999px; padding:3px 7px; color:#0a57d8; background:#fff; box-shadow:inset 0 0 0 1px rgba(31,99,255,.12); font-size:10px; font-weight:900; line-height:1.25; }}
    .detail-form {{ display:grid; gap:6px; }}
    .current-notice-empty {{ min-height:180px; }}
    .site-photo-panel {{ border:1px solid #cfe0f5; border-radius:16px; padding:10px; background:linear-gradient(135deg,#f8fbff,#eef6ff); display:grid; gap:9px; }}
    .site-photo-head {{ display:flex; justify-content:space-between; gap:10px; align-items:flex-start; }}
    .site-photo-head strong {{ display:block; color:#0c244d; font-size:14px; line-height:1.25; }}
    .site-photo-head span {{ display:block; margin-top:2px; color:#64748b; font-size:11px; line-height:1.35; }}
    .site-photo-head b {{ white-space:nowrap; border-radius:999px; padding:4px 9px; color:#0a57d8; background:#fff; box-shadow:inset 0 0 0 1px rgba(31,99,255,.14); font-size:11px; }}
    .site-photo-upload {{ display:grid; grid-template-columns:minmax(150px,190px) minmax(0,1fr); gap:10px; align-items:stretch; }}
    .site-photo-drop {{ min-height:62px; border:1px dashed #93c5fd; border-radius:14px; display:grid; place-items:center; gap:2px; padding:10px; color:#0a57d8; background:#fff; cursor:pointer; font-weight:950; text-align:center; transition:border-color .16s ease, background .16s ease, box-shadow .16s ease; }}
    .site-photo-drop:hover,.site-photo-drop.dragover {{ border-color:#1f63ff; background:#eef6ff; box-shadow:0 10px 22px rgba(31,99,255,.1); }}
    .site-photo-drop input {{ display:none; }}
    .site-photo-drop small {{ color:#64748b; font-size:10px; font-weight:800; }}
    .site-photo-side {{ min-width:0; display:grid; gap:7px; align-content:start; }}
    #lite-site-photo-status {{ color:#53677f; font-size:12px; font-weight:900; line-height:1.45; }}
    .site-photo-list {{ display:flex; flex-wrap:wrap; gap:6px; min-height:24px; }}
    .site-photo-item {{ max-width:180px; min-width:0; display:inline-flex; align-items:center; gap:6px; border:1px solid #d8e5f7; border-radius:999px; padding:4px 8px; color:#0c244d; background:#fff; font-size:11px; font-weight:900; }}
    .site-photo-item span {{ min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
    .site-photo-remove {{ border:0; border-radius:999px; width:18px; height:18px; display:grid; place-items:center; color:#64748b; background:#eef2f7; cursor:pointer; font-size:12px; line-height:1; }}
    .site-photo-remove:hover {{ color:#b42318; background:#fff1f0; }}
    .site-photo-panel.uploading .site-photo-drop {{ pointer-events:none; opacity:.68; }}
    .paste-drawer {{ position:relative; z-index:1; border:1px solid #cfe0f5; border-radius:18px; background:linear-gradient(135deg,#f8fbff,#eef6ff); overflow:hidden; }}
    .paste-drawer-toggle {{ width:100%; min-height:38px; display:flex; align-items:center; justify-content:space-between; gap:12px; border:0; padding:8px 11px; cursor:pointer; color:#0a57d8; background:transparent; font:900 13px/1.4 "Microsoft YaHei",Arial,sans-serif; text-align:left; }}
    .paste-drawer-toggle::after {{ content:"展开"; flex:0 0 auto; border-radius:999px; padding:4px 9px; color:#0a57d8; background:#fff; font-size:12px; box-shadow:inset 0 0 0 1px rgba(31,99,255,.14); }}
    .paste-drawer.open .paste-drawer-toggle::after {{ content:"收起"; }}
    .paste-drawer-content[hidden] {{ display:none; }}
    .paste-drawer form {{ display:grid; gap:10px; padding:0 13px 13px; }}
    .paste-drawer textarea {{ min-height:96px; resize:vertical; }}
    .form-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; }}
    .form-section {{ border:1px solid #d8e5f7; border-radius:14px; padding:9px; background:#fff; }}
    .form-section + .form-section {{ margin-top:2px; }}
    .form-section header {{ display:flex; justify-content:space-between; gap:10px; align-items:center; margin-bottom:8px; }}
    .form-section header strong {{ color:#0c244d; font-size:13px; }}
    .form-section header span {{ margin:0; color:#64748b; font-size:11px; line-height:1.25; text-align:right; }}
    label span {{ display:block; margin:0 0 4px; color:#51677f; font-size:11px; font-weight:900; }}
    label.required > span::after {{ content:"必填"; display:inline-flex; margin-left:6px; border-radius:999px; padding:1px 6px; color:#b42318; background:#fff1f0; font-size:10px; font-weight:950; vertical-align:middle; }}
    input:required:invalid,textarea:required:invalid {{ border-color:#ffc7bf; background:#fffafa; }}
    .form-grid > label {{ min-width:0; }}
    .source-link-field {{ display:flex; flex-wrap:wrap; align-items:center; gap:6px; width:max-content; max-width:100%; margin:0 0 5px; border:1px solid #a8cdfa; border-radius:999px; padding:4px 6px; background:linear-gradient(135deg,#f5fbff,#eef6ff); }}
    .source-link-field span {{ margin:0; border-radius:999px; padding:3px 7px; color:#0a57d8; background:#fff; font-size:10px; font-weight:950; }}
    .source-link-title {{ color:#0c244d; font-size:11px; font-weight:950; line-height:1.2; }}
    .source-link-field select {{ flex:1 1 220px; min-width:160px; min-height:28px; border-radius:999px; padding:3px 9px; font-size:12px; }}
    .source-link-field.readonly {{ padding-right:9px; }}
    .manual-source-binding {{ display:grid; gap:8px; border:1px solid #bfdbfe; border-radius:14px; padding:9px 10px; background:linear-gradient(135deg,#f8fbff,#eef6ff); }}
    .manual-source-binding-head {{ display:flex; align-items:center; justify-content:space-between; gap:10px; }}
    .manual-source-binding-head strong {{ color:#0c244d; font-size:12px; }}
    .manual-source-binding-head span {{ color:#b15d00; font-size:11px; font-weight:900; }}
    .manual-source-binding-head span.ready {{ color:#087443; }}
    .manual-source-binding-actions {{ display:flex; flex-wrap:wrap; gap:7px; }}
    .manual-source-binding-actions .btn {{ min-height:30px; padding:5px 10px; border-radius:999px; }}
    .manual-source-binding-actions .btn.active {{ color:#fff; border-color:#1f63ff; background:#1f63ff; box-shadow:0 8px 18px rgba(31,99,255,.18); }}
    .mop-action-panel {{ display:flex; flex-wrap:wrap; align-items:center; justify-content:space-between; gap:6px; width:max-content; max-width:100%; margin:0 0 5px; border:1px solid #bfdbfe; border-radius:999px; padding:4px 6px 4px 9px; background:linear-gradient(135deg,#f5fbff,#ffffff); }}
    .mop-action-panel.warn {{ border-color:#fbd38d; background:linear-gradient(135deg,#fff8ed,#ffffff); }}
    .mop-action-panel.ok {{ border-color:#bbf7d0; background:linear-gradient(135deg,#f0fdf4,#ffffff); }}
    .mop-action-panel > div {{ min-width:0; display:flex; align-items:center; gap:6px; }}
    .mop-action-panel strong {{ color:#0c244d; font-size:11px; line-height:1.2; white-space:nowrap; }}
    .mop-action-panel span,.mop-action-panel em {{ color:#64748b; font-size:11px; font-style:normal; line-height:1.2; }}
    .mop-action-panel em {{ color:#0a57d8; font-weight:900; }}
    .mop-action-panel .btn {{ flex:0 0 auto; min-height:28px; padding:5px 9px; border-radius:999px; }}
    .target-link-panel {{ display:flex; flex-wrap:wrap; align-items:center; justify-content:space-between; gap:6px; width:max-content; max-width:100%; border:1px solid #a8cdfa; border-radius:999px; padding:4px 6px 4px 9px; background:linear-gradient(135deg,#f5fbff,#ffffff); }}
    .target-link-panel > div {{ min-width:0; display:flex; align-items:center; gap:6px; }}
    .target-link-panel strong {{ display:block; color:#0c244d; font-size:11px; line-height:1.2; white-space:nowrap; }}
    .target-link-panel span {{ display:block; margin:0; color:#64748b; font-size:11px; font-weight:900; line-height:1.2; }}
    .target-link-panel .btn {{ flex:0 0 auto; min-height:28px; padding:5px 9px; border-radius:999px; }}
    .repair-event-link-panel {{ display:flex; flex-wrap:wrap; align-items:center; justify-content:space-between; gap:6px; width:max-content; max-width:100%; border:1px solid #c4b5fd; border-radius:999px; padding:4px 6px 4px 9px; background:linear-gradient(135deg,#f8f7ff,#ffffff); }}
    .repair-event-link-panel > div {{ min-width:0; display:flex; align-items:center; gap:6px; }}
    .repair-event-link-panel strong {{ color:#4338ca; font-size:11px; line-height:1.2; white-space:nowrap; }}
    .repair-event-link-panel span {{ max-width:360px; overflow:hidden; color:#64748b; font-size:11px; font-weight:900; line-height:1.2; text-overflow:ellipsis; white-space:nowrap; }}
    .repair-event-link-panel .btn {{ flex:0 0 auto; min-height:28px; padding:5px 9px; border-radius:999px; }}
    .notice-preview {{ border:1px solid #d8e5f7; border-radius:14px; background:#fbfdff; overflow:hidden; }}
    .preview-head {{ display:flex; align-items:center; justify-content:space-between; gap:10px; padding:10px 12px; border-bottom:1px solid #e5edf8; background:linear-gradient(180deg,#f8fbff,#eef6ff); }}
    .preview-head span {{ color:#0a57d8; font-size:13px; font-weight:900; }}
    .preview-head b {{ color:#7c5a00; border-radius:999px; padding:5px 9px; background:#fff8db; font-size:12px; line-height:1.25; text-align:right; }}
    .notice-preview pre {{ margin:0; max-height:88px; overflow:auto; white-space:pre-wrap; overflow-wrap:anywhere; padding:9px 10px; color:#0c244d; background:#fff; font:12px/1.5 "Microsoft YaHei", Arial, sans-serif; }}
    .notice-preview:focus-within pre,.notice-preview:hover pre {{ max-height:160px; }}
    .form-actions {{ position:sticky; bottom:8px; z-index:8; display:flex; justify-content:flex-end; gap:7px; align-items:center; margin-top:0; border:1px solid #d8e5f7; border-radius:15px; padding:7px; background:linear-gradient(135deg,rgba(255,255,255,.98),rgba(247,251,255,.96)); box-shadow:0 12px 26px rgba(15,73,153,.12); backdrop-filter:blur(8px); }}
    .actual-action-time {{ flex:0 1 190px; min-width:168px; display:grid; gap:2px; border:1px solid #d8e5f7; border-radius:12px; padding:4px 7px; background:#fff; }}
    .actual-action-time span {{ margin:0; color:#0a57d8; font-size:10px; font-weight:950; line-height:1.1; }}
    .actual-action-time input {{ min-height:26px; border:0; border-radius:8px; padding:0; color:#0c244d; background:transparent; font-size:12px; font-weight:900; box-shadow:none; }}
    .actual-action-time input:focus {{ outline:2px solid rgba(31,99,255,.16); outline-offset:2px; }}
    .job-status {{ color:#64748b; font-size:12px; font-weight:800; }}
    .form-actions .job-status {{ margin-right:auto; }}
    .action-reason {{ max-width:190px; border-radius:999px; padding:4px 7px; color:#64748b; background:#f1f6fd; font-size:11px; font-weight:900; line-height:1.25; }}
    .action-reason.warn {{ color:#8a4b00; background:#fff4d6; }}
    .action-reason.ready {{ color:#087443; background:#e8fff3; }}
    .action-reason.blocked {{ color:#b42318; background:#fff1f0; }}
    .form-actions .btn.primary,.form-actions .btn.danger,.form-actions .btn.danger-ghost {{ min-width:104px; box-shadow:0 12px 24px rgba(21,99,255,.14); }}
    .end-check-backdrop {{ position:fixed; inset:0; z-index:240; display:grid; place-items:center; padding:24px; background:rgba(8,32,74,.36); backdrop-filter:blur(8px); }}
    .end-check-backdrop[hidden] {{ display:none; }}
    .end-check-dialog {{ width:min(560px,100%); border:1px solid #d8e5f7; border-radius:24px; background:#fff; box-shadow:0 28px 70px rgba(8,32,74,.24); overflow:hidden; }}
    .end-check-head {{ padding:15px 18px; background:linear-gradient(135deg,#f8fbff,#eef6ff); border-bottom:1px solid #e5edf8; }}
    .end-check-head span {{ display:inline-flex; width:max-content; border-radius:999px; padding:5px 10px; color:#0a57d8; background:#eaf3ff; font-weight:900; font-size:12px; }}
    .end-check-head strong {{ display:block; margin-top:7px; color:#0c244d; font-size:18px; line-height:1.25; }}
    .end-check-head p {{ display:none; }}
    .end-check-list {{ display:grid; gap:7px; margin:0; padding:14px 18px; list-style:none; }}
    .end-check-list li {{ display:grid; grid-template-columns:20px minmax(0,1fr); gap:9px; align-items:start; border:1px solid #dce8f8; border-radius:15px; padding:9px 10px; background:#fbfdff; }}
    .end-check-list li::before {{ content:""; width:14px; height:14px; margin-top:2px; border-radius:999px; background:#94a3b8; box-shadow:0 0 0 4px #f1f5f9; }}
    .end-check-list li.ok::before {{ background:#12b886; box-shadow:0 0 0 4px #e8fff3; }}
    .end-check-list li.warn::before {{ background:#f59e0b; box-shadow:0 0 0 4px #fff4d6; }}
    .end-check-list li.blocked::before {{ background:#e04d5f; box-shadow:0 0 0 4px #fff1f0; }}
    .end-check-list b {{ display:block; color:#0c244d; font-size:14px; }}
    .end-check-list small {{ display:block; margin-top:3px; color:#64748b; line-height:1.45; }}
    .end-check-actions {{ display:flex; justify-content:flex-end; gap:10px; padding:12px 18px 15px; border-top:1px solid #e5edf8; background:#fbfdff; }}
    .target-candidate-dialog {{ width:min(760px,100%); max-height:min(760px,88vh); display:grid; grid-template-rows:auto minmax(0,1fr) auto; }}
    .repair-event-candidate-dialog {{ width:min(980px,100%); }}
    .target-candidate-list {{ display:grid; gap:9px; overflow:auto; padding:16px 20px; }}
    .target-candidate-row {{ border:1px solid #dce8f8; border-radius:16px; padding:12px; background:#fff; text-align:left; cursor:pointer; display:grid; gap:7px; color:#0c244d; }}
    .target-candidate-row:hover,.target-candidate-row.active {{ border-color:#1f63ff; box-shadow:0 0 0 3px rgba(31,99,255,.12); background:#f7fbff; }}
    .target-candidate-row strong {{ font-size:14px; line-height:1.35; }}
    .target-candidate-row span {{ color:#64748b; font-size:12px; line-height:1.45; }}
    .target-candidate-row small {{ width:max-content; max-width:100%; border-radius:999px; padding:3px 8px; color:#0a57d8; background:#eaf3ff; font-size:11px; font-weight:900; }}
    .target-candidate-empty {{ border:1px dashed #cbdaf0; border-radius:16px; padding:18px; color:#64748b; text-align:center; background:#f8fbff; }}
    .manual-source-tools {{ display:flex; gap:8px; padding:12px 20px 0; }}
    .manual-source-tools input {{ flex:1 1 auto; min-width:0; }}
    .undo-confirm-copy {{ display:grid; gap:8px; padding:16px 18px; }}
    .undo-confirm-copy strong {{ color:#0c244d; font-size:15px; line-height:1.45; overflow-wrap:anywhere; }}
    .undo-confirm-copy span {{ color:#64748b; font-size:12px; line-height:1.5; }}
    body.has-dirty-lite-form .job-status {{ color:#b15d00; }}
    .empty {{ border:1px dashed #cbdaf0; border-radius:16px; padding:18px; color:#64748b; text-align:center; background:#f8fbff; }}
    @keyframes liteSpin {{ to {{ transform:rotate(360deg); }} }}
    @media (prefers-reduced-motion: reduce) {{ *, *::before, *::after {{ transition:none !important; animation:none !important; scroll-behavior:auto !important; }} }}
    @media (max-width: 1180px) {{ .workspace,.summary,.lite-tools,.workbench-guide {{ grid-template-columns:1fr; }} .task-inbox {{ position:relative; top:auto; max-height:none; grid-template-columns:1fr; overflow:visible; }} .inbox-head {{ grid-column:auto; grid-template-columns:1fr; }} .inbox-section .list {{ max-height:42vh; }} .toolbar {{ flex-wrap:wrap; }} .notice-detail-drawer {{ width:min(1000px,calc(100vw - 28px)); }} }}
    @media (max-width: 900px) {{
      .topbar {{ min-height:auto; padding:18px 20px; flex-direction:column; align-items:stretch; gap:16px; }}
      .brand {{ gap:14px; align-items:flex-start; }}
      .brand h1 {{ font-size:24px; line-height:1.2; }}
      .logo {{ padding-right:14px; }}
      .top-actions {{ justify-content:flex-start; }}
      .scope-switch {{ flex:1 1 180px; justify-content:space-between; }}
      .top-actions .top-link,.top-actions .exit {{ flex:1 1 120px; }}
      .shell {{ padding:14px 14px 28px; }}
      .status {{ border-radius:18px; }}
      .summary {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
      .toolbar form {{ width:100%; margin-left:0; flex-wrap:wrap; }}
      .toolbar input,.toolbar select {{ min-width:0; flex:1 1 180px; }}
      .manual-picker,.refresh-picker {{ flex:1 1 150px; }}
      .manual-picker > .btn,.refresh-picker > .btn {{ width:100%; }}
      .manual-menu,.refresh-menu {{ left:0; right:auto; max-width:calc(100vw - 32px); }}
      .form-grid {{ grid-template-columns:1fr; }}
      .site-photo-upload {{ grid-template-columns:1fr; }}
      .detail-head {{ grid-template-columns:1fr; }}
      label:has(textarea), label:nth-last-child(1) {{ grid-column:auto; }}
      .form-actions {{ flex-wrap:wrap; }}
      .form-actions .btn {{ flex:1 1 140px; }}
      .notice-detail-drawer {{ width:100%; border-radius:0; }}
      .notice-drawer-head {{ min-height:64px; padding:10px 14px; }}
      .notice-drawer-title h2 {{ font-size:16px; }}
      .notice-drawer-body {{ padding:10px 12px 18px; }}
    }}
  </style>
</head>
<body>
  <header class="topbar">
    <div class="brand">
      <div class="logo brand-logo">世纪互联<br>VNET</div>
      <div><h1>南通基地-运维灯塔工作台</h1><p id="lite-workbench-subtitle">{_e(WORK_TYPE_FILTER_LABELS.get(view_work, '通告'))} · 工作台</p></div>
    </div>
    <nav class="top-actions">
      <label class="scope-switch"><b class="scope-icon" aria-hidden="true">楼</b><span>当前楼栋</span><select class="scope-select" id="lite-scope-select" aria-label="切换楼栋">{scope_select}</select></label>
      <a class="top-link" href="/" aria-label="返回">返回</a>
      <a class="top-link" href="/engineer/mop?scope={_e(scope)}" aria-label="打开维护单管理">维护单</a>
      <a class="exit" href="/api/auth/logout" aria-label="退出登录">退出</a>
    </nav>
  </header>
  <main class="shell">
    <section class="summary">
      <article><strong>已发起</strong><b>{_e(stats.get('started') or 0)}</b></article>
      <article><strong>有更新</strong><b>{_e(stats.get('updated') or 0)}</b></article>
      <article><strong>已结束</strong><b>{_e(stats.get('ended') or 0)}</b></article>
      <article><strong>进行中</strong><b>{_e(len(ongoing))}</b></article>
    </section>
    <section class="toolbar">
      <div class="type-tabs">{type_tabs}</div>
      <div class="manual-picker" id="manual-picker">
        <button class="btn ghost" id="manual-open" type="button" aria-expanded="false" aria-controls="manual-menu">纯手填</button>
        <div class="manual-menu" id="manual-menu">
          {''.join(f'<a href="{_e(_manual_url(scope, key, selected_month))}">{_e(label)}</a>' for key, label in WORK_TYPE_LABELS.items())}
        </div>
      </div>
      <div class="refresh-picker" id="refresh-picker">
        <button class="btn ghost" id="refresh-open" type="button" aria-expanded="false" aria-controls="refresh-menu">刷新数据</button>
        <div class="refresh-menu" id="refresh-menu">
          <div class="refresh-menu-head"><strong>刷新数据</strong><span>{_e(source_loaded_text)}</span></div>
          <button id="lite-refresh-page" type="button">刷新本页</button>
          <button id="lite-refresh-repair" type="button">刷新检修</button>
          <button id="lite-refresh-change" type="button">刷新变更</button>
        </div>
      </div>
      <form method="get" action="/workbench-lite">
        <input type="hidden" name="scope" value="{_e(scope)}">
        <input type="hidden" name="work_type" value="{_e(view_work)}">
        <label class="visually-hidden" for="lite-month-select">筛选月份</label>
        <select id="lite-month-select" name="month" aria-label="筛选月份">
          {''.join(f'<option value="{_e(item)}"{" selected" if item == selected_month else ""}>{_e(item)}</option>' for item in month_options)}
        </select>
        <label class="visually-hidden" for="lite-search-input">搜索标题、楼栋、专业</label>
        <input id="lite-search-input" name="search" value="{_e(search)}" placeholder="搜索标题、楼栋、专业">
        <label class="visually-hidden" for="lite-specialty-select">筛选专业</label>
        <select id="lite-specialty-select" name="specialty" aria-label="筛选专业">
          <option value="">全部专业</option>
          {''.join(f'<option value="{_e(item)}"{" selected" if item == specialty else ""}>{_e(item)}</option>' for item in SPECIALTY_OPTIONS)}
        </select>
        <button class="btn primary" type="submit">筛选</button>
      </form>
    </section>
    <section class="workbench-guide" aria-label="通告办理步骤" hidden></section>
    <section class="workspace">
      <aside class="task-inbox panel" aria-label="通告处理">
        <div class="inbox-head">
          <h2 class="inbox-title"><span>通告处理</span></h2>
          <div class="inbox-summary" aria-label="任务数量">
            <span>计划通告 <b>{_e(current_pending_count)}</b></span>
            <span>未结束 <b data-inbox-ongoing-count>{_e(current_ongoing_count)}</b></span>
          </div>
        </div>
        <section class="inbox-section pending-inbox-panel">
          <h3><span>计划通告列表</span><b>{_e(current_pending_count)}</b></h3>
          <div class="list">{_record_rows(visible_records, ongoing_items=ongoing, scope=scope, work_type=view_work, month=selected_month, search=search, specialty=specialty, selected_id=selected_record_id, pending_page=pending_page_num, ongoing_page=ongoing_page_num)}</div>
          {pending_pager}
        </section>
        <section class="inbox-section ongoing-inbox-panel" data-ongoing-panel>
          <h3><span>未结束通告</span><b class="panel-count">{_e(current_ongoing_count)}</b></h3>
          <div class="list">{_ongoing_rows(visible_ongoing, scope=scope, work_type=view_work, month=selected_month, selected_id=active_item_id, pending_page=pending_page_num, ongoing_page=ongoing_page_num)}</div>
          {ongoing_pager}
        </section>
        <details class="rail-fold attention"{' open' if attention_count else ''}><summary>待处理问题 <b class="panel-count">{_e(attention_count)}</b></summary><section class="rail-panel attention"><h2>待处理问题</h2><div class="attention-list">{attention_html}</div></section></details>
        <details class="rail-fold undo"><summary>近三天可回退 <b class="panel-count">{_e(undo_count)}</b></summary><section class="rail-panel undo"><h2>近三天可回退</h2><div class="undo-panel-list">{undo_html}</div></section></details>
      </aside>
      <div class="notice-detail-overlay{' open' if detail_drawer_open else ''}" id="lite-notice-detail-overlay" data-open-on-load="{'1' if detail_drawer_open else '0'}" aria-hidden="{'false' if detail_drawer_open else 'true'}">
        <section class="panel detail-panel notice-detail-drawer" id="detail-panel" role="dialog" aria-modal="true" aria-labelledby="lite-notice-drawer-title" tabindex="-1">
          <header class="notice-drawer-head">
            <div class="notice-drawer-title">
              <span>当前通告</span>
              <h2 id="lite-notice-drawer-title">{_e(drawer_title)}</h2>
            </div>
            <button class="notice-drawer-close" id="lite-notice-drawer-close" type="button" aria-label="关闭当前通告">×</button>
          </header>
          <div class="notice-drawer-body">
            <section class="paste-drawer{' open' if parsed_draft else ''}">
              <button class="paste-drawer-toggle" id="lite-paste-toggle" type="button" aria-expanded="{'true' if parsed_draft else 'false'}" aria-controls="lite-paste-content">解析粘贴通告</button>
              <div class="paste-drawer-content" id="lite-paste-content"{'' if parsed_draft else ' hidden'}>
                <form method="post" action="/workbench-lite/parse">
                  <input type="hidden" name="scope" value="{_e(scope)}">
                  <input type="hidden" name="work_type" value="{_e(view_work)}">
                  <input type="hidden" name="month" value="{_e(selected_month)}">
                  <textarea name="paste_text" placeholder="粘贴完整通告文本">{_e(paste_text)}</textarea>
                  <button class="btn primary" type="submit">解析到当前通告</button>
                </form>
              </div>
            </section>
            {_detail_form(record=selected_record, ongoing_item=selected_ongoing, scope=scope, work_type=detail_work, manual=manual or bool(parsed_draft), source_month=selected_month, parsed_draft=parsed_draft, parsed_action=parsed_action, source_link_options=source_options, is_admin=is_admin_session, prefill_draft=prefill_draft, prefill_source_record_id=prefill_source_record_id, prefill_target_record_id=prefill_target_record_id, prefill_action=prefill_action, prefill_context_id=prefill_context_id)}
          </div>
        </section>
      </div>
    </section>
  </main>
  <div class="end-check-backdrop" id="lite-end-check" hidden>
    <section class="end-check-dialog" role="dialog" aria-modal="true" aria-labelledby="lite-end-check-title">
      <header class="end-check-head">
        <span>结束前检查</span>
        <strong id="lite-end-check-title">确认发送结束通告</strong>
      </header>
      <ul class="end-check-list" id="lite-end-check-list"></ul>
      <footer class="end-check-actions">
        <button class="btn ghost" type="button" id="lite-end-check-cancel">返回修改</button>
        <button class="btn danger" type="button" id="lite-end-check-confirm">确认发送结束</button>
      </footer>
    </section>
  </div>
  <div class="end-check-backdrop" id="lite-target-candidates" hidden>
    <section class="end-check-dialog target-candidate-dialog" role="dialog" aria-modal="true" aria-labelledby="lite-target-candidates-title">
      <header class="end-check-head">
        <span>目标多维关系</span>
        <strong id="lite-target-candidates-title">选择对应目标记录</strong>
      </header>
      <div class="target-candidate-list" id="lite-target-candidate-list"></div>
      <footer class="end-check-actions">
        <button class="btn ghost" type="button" id="lite-target-candidates-cancel">取消</button>
        <button class="btn primary" type="button" id="lite-target-candidates-confirm" disabled>确认绑定</button>
      </footer>
    </section>
  </div>
  <div class="end-check-backdrop" id="lite-manual-source-candidates" hidden>
    <section class="end-check-dialog target-candidate-dialog" role="dialog" aria-modal="true" aria-labelledby="lite-manual-source-title">
      <header class="end-check-head">
        <span>计划通告关联</span>
        <strong id="lite-manual-source-title">选择要绑定的计划通告</strong>
      </header>
      <div class="manual-source-tools">
        <input id="lite-manual-source-search" type="search" placeholder="搜索名称、楼栋或专业" autocomplete="off">
      </div>
      <div class="target-candidate-list" id="lite-manual-source-list"></div>
      <footer class="end-check-actions">
        <button class="btn ghost" type="button" id="lite-manual-source-cancel">取消</button>
        <button class="btn primary" type="button" id="lite-manual-source-confirm" disabled>确认绑定</button>
      </footer>
    </section>
  </div>
  <div class="end-check-backdrop" id="lite-repair-event-candidates" hidden>
    <section class="end-check-dialog target-candidate-dialog repair-event-candidate-dialog" role="dialog" aria-modal="true" aria-labelledby="lite-repair-event-title">
      <header class="end-check-head">
        <span>事件转检修</span>
        <strong id="lite-repair-event-title">选择对应事件</strong>
      </header>
      <div class="manual-source-tools">
        <input id="lite-repair-event-search" type="search" placeholder="搜索事件、维修项目、楼栋或专业" autocomplete="off">
      </div>
      <div class="target-candidate-list" id="lite-repair-event-list"></div>
      <footer class="end-check-actions">
        <button class="btn ghost" type="button" id="lite-repair-event-cancel">取消</button>
        <button class="btn primary" type="button" id="lite-repair-event-confirm" disabled>保存关联并填入</button>
      </footer>
    </section>
  </div>
  <div class="end-check-backdrop" id="lite-undo-confirm" hidden>
    <section class="end-check-dialog" role="dialog" aria-modal="true" aria-labelledby="lite-undo-confirm-title">
      <header class="end-check-head">
        <span>回退确认</span>
        <strong id="lite-undo-confirm-title">确认回退这条通告</strong>
      </header>
      <div class="undo-confirm-copy">
        <strong id="lite-undo-confirm-name"></strong>
        <span id="lite-undo-confirm-meta"></span>
      </div>
      <footer class="end-check-actions">
        <button class="btn ghost" type="button" id="lite-undo-confirm-cancel">取消</button>
        <button class="btn danger" type="button" id="lite-undo-confirm-apply">确认回退</button>
      </footer>
    </section>
  </div>
  <div class="end-check-backdrop" id="lite-discard-confirm" hidden>
    <section class="end-check-dialog lite-discard-dialog" role="dialog" aria-modal="true" aria-labelledby="lite-discard-title">
      <header class="end-check-head">
        <span>未发送修改</span>
        <strong id="lite-discard-title">切换会丢失当前修改</strong>
      </header>
      <footer class="end-check-actions">
        <button class="btn ghost" type="button" id="lite-discard-cancel">继续编辑</button>
        <button class="btn primary" type="button" id="lite-discard-confirm-button">放弃并切换</button>
      </footer>
    </section>
  </div>
  <script>
    const initialScope = {_json_dumps(scope)};
    const liteIsAdmin = {_json_dumps(bool(is_admin_session))};
    let liteFormDirty = false;
    function currentUrlScope() {{
      return new URLSearchParams(location.search).get('scope') || initialScope;
    }}
    function currentViewWorkType() {{
      return new URLSearchParams(location.search).get('work_type') || '{_e(view_work)}';
    }}
    function getCurrentScope() {{
      return document.getElementById('lite-scope-select')?.value || currentUrlScope();
    }}
    function statusBox() {{
      return document.getElementById('lite-job-status');
    }}
    function setLiteStatus(text) {{
      const box = statusBox();
      if (box) box.textContent = text;
      const pageStatus = document.getElementById('lite-page-status');
      if (pageStatus) pageStatus.textContent = text;
    }}
    function friendlyLiteMessage(message) {{
      const raw = String(message || '').trim();
      if (!raw) return '操作失败';
      if (raw.includes('目标多维记录不存在') || raw.includes('RecordIdNotFound') || (raw.includes('record_id') && raw.includes('不存在'))) {{
        return '没有找到对应的目标多维记录。请先点击“查找目标记录”重新绑定，再更新或结束。';
      }}
      if (raw.includes('字段') && raw.includes('不存在')) {{
        return '多维表字段配置不一致，请联系管理员检查字段。';
      }}
      if (raw.includes('请求过于频繁')) {{
        return '操作太快了，请稍等几秒后再试。';
      }}
      return raw;
    }}
    function showLiteError(message) {{
      const text = friendlyLiteMessage(message);
      setLiteStatus(text);
      const box = statusBox();
      if (box) {{
        box.classList.add('failed');
        window.setTimeout(() => box.classList.remove('failed'), 3200);
      }}
    }}
    function liteAuthLoginUrl() {{
      const next = location.pathname + location.search + location.hash;
      return '/api/auth/login?next=' + encodeURIComponent(next || '/');
    }}
    function isLiteAuthRequired(response, data, text) {{
      const message = String(text || (data && data.error) || '');
      if (response && response.ok && !data) return false;
      return Boolean(
        (response && response.status === 401) ||
        (data && data.auth_required) ||
        message.includes('请先使用飞书扫码登录') ||
        message.includes('登录状态已失效') ||
        message.includes('登录状态已过期')
      );
    }}
    function redirectLiteAuthRequired() {{
      setLiteFormDirty(false);
      setLiteStatus('登录已过期，正在跳转飞书扫码登录...');
      location.href = liteAuthLoginUrl();
    }}
    function handleLiteAuthRequired(response, data, text) {{
      if (!isLiteAuthRequired(response, data, text)) return false;
      redirectLiteAuthRequired();
      return true;
    }}
    const liteAuthHeartbeatMs = 60 * 1000;
    let liteAuthHeartbeatTimer = null;
    let liteAuthHeartbeatInFlight = false;
    let liteAuthRedirecting = false;
    function redirectLiteAuthRequiredOnce() {{
      if (liteAuthRedirecting) return;
      liteAuthRedirecting = true;
      redirectLiteAuthRequired();
    }}
    function scheduleLiteAuthHeartbeat(delayMs = liteAuthHeartbeatMs) {{
      if (liteAuthHeartbeatTimer !== null) window.clearTimeout(liteAuthHeartbeatTimer);
      liteAuthHeartbeatTimer = window.setTimeout(() => {{
        liteAuthHeartbeatTimer = null;
        checkLiteAuthStatus();
      }}, delayMs);
    }}
    async function checkLiteAuthStatus() {{
      if (liteAuthHeartbeatInFlight || liteAuthRedirecting) return;
      liteAuthHeartbeatInFlight = true;
      try {{
        const next = location.pathname + location.search + location.hash;
        const response = await fetch('/api/auth/status?next=' + encodeURIComponent(next || '/'), {{
          credentials: 'same-origin',
          cache: 'no-store',
        }});
        const payload = await response.json().catch(() => ({{}}));
        if (isLiteAuthRequired(response, payload)) {{
          redirectLiteAuthRequiredOnce();
          return;
        }}
        const status = payload && typeof payload.data === 'object' ? payload.data : payload;
        if (response.ok && status && status.logged_in === false) {{
          redirectLiteAuthRequiredOnce();
          return;
        }}
      }} catch {{
        // 网络短暂中断不等于登录过期，保留当前页面并在下一轮重试。
      }} finally {{
        liteAuthHeartbeatInFlight = false;
        if (!liteAuthRedirecting) scheduleLiteAuthHeartbeat();
      }}
    }}
    document.addEventListener('visibilitychange', () => {{
      if (!document.hidden) checkLiteAuthStatus();
    }});
    window.addEventListener('focus', checkLiteAuthStatus);
    scheduleLiteAuthHeartbeat();
    function setLiteFormDirty(enabled) {{
      liteFormDirty = Boolean(enabled);
      document.body.classList.toggle('has-dirty-lite-form', liteFormDirty);
      if (liteFormDirty) setLiteStatus('有未发送修改');
    }}
    let pendingDiscardResolver = null;
    let pendingDiscardPromise = null;
    let discardReturnFocus = null;
    function confirmDiscardLiteChanges() {{
      if (!liteFormDirty) return Promise.resolve(true);
      if (pendingDiscardPromise) return pendingDiscardPromise;
      const modal = document.getElementById('lite-discard-confirm');
      const confirmButton = document.getElementById('lite-discard-confirm-button');
      if (!modal || !confirmButton) return Promise.resolve(false);
      discardReturnFocus = document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
      modal.hidden = false;
      confirmButton.focus();
      pendingDiscardPromise = new Promise(resolve => {{
        pendingDiscardResolver = resolve;
      }});
      return pendingDiscardPromise;
    }}
    function resolveDiscardConfirm(approved) {{
      const modal = document.getElementById('lite-discard-confirm');
      if (modal) modal.hidden = true;
      const resolver = pendingDiscardResolver;
      const returnFocus = discardReturnFocus;
      pendingDiscardResolver = null;
      pendingDiscardPromise = null;
      discardReturnFocus = null;
      if (resolver) resolver(Boolean(approved));
      if (!approved && returnFocus?.isConnected) {{
        requestAnimationFrame(() => returnFocus.focus());
      }}
    }}
    function setPanelLoading(enabled) {{
      const panel = document.getElementById('detail-panel');
      if (panel) panel.classList.toggle('loading', Boolean(enabled));
    }}
    function noticeDrawerOverlay() {{
      return document.getElementById('lite-notice-detail-overlay');
    }}
    let lastNoticeDrawerTrigger = null;
    function updateNoticeDrawerTitle(title) {{
      const heading = document.getElementById('lite-notice-drawer-title');
      if (heading && String(title || '').trim()) heading.textContent = String(title).trim();
    }}
    function syncNoticeDrawerState() {{
      const overlay = noticeDrawerOverlay();
      if (!overlay) {{
        document.body.classList.remove('notice-drawer-open');
        return;
      }}
      const shouldOpen = overlay.dataset.openOnLoad === '1' || overlay.classList.contains('open');
      overlay.classList.toggle('open', shouldOpen);
      overlay.setAttribute('aria-hidden', shouldOpen ? 'false' : 'true');
      document.body.classList.toggle('notice-drawer-open', shouldOpen);
    }}
    function openNoticeDrawer(title, trigger) {{
      const overlay = noticeDrawerOverlay();
      if (!overlay) return;
      const wasOpen = overlay.classList.contains('open');
      if (trigger instanceof HTMLElement) lastNoticeDrawerTrigger = trigger;
      overlay.dataset.openOnLoad = '1';
      delete overlay.dataset.detailNeedsReload;
      overlay.classList.add('open');
      overlay.setAttribute('aria-hidden', 'false');
      document.body.classList.add('notice-drawer-open');
      updateNoticeDrawerTitle(title);
      if (!wasOpen) {{
        requestAnimationFrame(() => {{
          const body = overlay.querySelector('.notice-drawer-body');
          if (body) body.scrollTop = 0;
          document.getElementById('lite-notice-drawer-close')?.focus();
        }});
      }}
    }}
    function closeNoticeDrawer({{ resetUrl = true }} = {{}}) {{
      const overlay = noticeDrawerOverlay();
      if (!overlay) return;
      overlay.dataset.openOnLoad = '0';
      overlay.classList.remove('open');
      overlay.setAttribute('aria-hidden', 'true');
      document.body.classList.remove('notice-drawer-open');
      document.querySelectorAll('.notice-row.active,.ongoing-row.active').forEach(row => {{
        row.classList.remove('active');
        row.setAttribute('aria-current', 'false');
      }});
      if (!resetUrl) return;
      const url = new URL(location.href);
      for (const key of ['active_item_id', 'record_id', 'manual', 'repair_management_record_id']) {{
        url.searchParams.delete(key);
      }}
      history.replaceState({{ lite: true }}, '', url.pathname + url.search + url.hash);
      const returnFocus = lastNoticeDrawerTrigger;
      lastNoticeDrawerTrigger = null;
      if (returnFocus?.isConnected) {{
        requestAnimationFrame(() => returnFocus.focus());
      }}
    }}
    async function requestCloseNoticeDrawer() {{
      const overlay = noticeDrawerOverlay();
      if (!overlay || !overlay.classList.contains('open')) return true;
      const hadUnsavedChanges = liteFormDirty;
      if (!(await confirmDiscardLiteChanges())) return false;
      if (hadUnsavedChanges) overlay.dataset.detailNeedsReload = '1';
      setLiteFormDirty(false);
      closeNoticeDrawer();
      return true;
    }}
    function setWorkspaceSwitching(enabled, label) {{
      const workspace = document.querySelector('.workspace');
      if (!workspace) return;
      workspace.classList.toggle('is-switching', Boolean(enabled));
      workspace.setAttribute('aria-busy', enabled ? 'true' : 'false');
      if (enabled) workspace.dataset.loadingText = label || '正在加载通告';
      else delete workspace.dataset.loadingText;
    }}
    function setButtonBusy(button, enabled) {{
      if (!button) return;
      button.disabled = Boolean(enabled);
      button.classList.toggle('is-busy', Boolean(enabled));
      if (enabled) button.setAttribute('aria-busy', 'true');
      else button.removeAttribute('aria-busy');
    }}
    function setFormSubmitBusy(form, enabled) {{
      if (!form) return;
      form.classList.toggle('is-submitting', Boolean(enabled));
      form.querySelectorAll('button[name="submit_action"],button[data-ongoing-delete-mode]').forEach(button => setButtonBusy(button, enabled));
    }}
    function setPickerOpen(pickerId, openerId, open) {{
      document.getElementById(pickerId)?.classList.toggle('open', Boolean(open));
      document.getElementById(openerId)?.setAttribute('aria-expanded', open ? 'true' : 'false');
    }}
    function closePickers() {{
      setPickerOpen('manual-picker', 'manual-open', false);
      setPickerOpen('refresh-picker', 'refresh-open', false);
    }}
    let pendingEndSubmitter = null;
    function isMissingTargetRecordId(value) {{
      const text = String(value || '').trim();
      return !text || text.startsWith('local') || text.startsWith('manual:') || text.startsWith('active:');
    }}
    function endCheckItem(title, detail, tone) {{
      const item = document.createElement('li');
      item.className = tone || 'warn';
      const body = document.createElement('div');
      const heading = document.createElement('b');
      heading.textContent = title;
      const desc = document.createElement('small');
      desc.textContent = detail;
      body.append(heading, desc);
      item.append(body);
      return item;
    }}
    function openEndCheck(form, submitter) {{
      const modal = document.getElementById('lite-end-check');
      const list = document.getElementById('lite-end-check-list');
      const confirmButton = document.getElementById('lite-end-check-confirm');
      if (!modal || !list || !confirmButton) return false;
      const workType = previewValue(form, 'work_type') || form.dataset.workType || 'maintenance';
      const targetRecordId = previewValue(form, 'target_record_id');
      const sitePhotoCount = Number(previewValue(form, 'site_photo_count') || 0);
      const mopStatus = previewValue(form, 'mop_status');
      const checks = [];
      if (['maintenance', 'change', 'repair'].includes(workType)) {{
        checks.push(sitePhotoCount > 0
          ? ['现场图片', `已累计上传${{sitePhotoCount}}张，满足结束条件。`, 'ok']
          : ['现场图片', '还没有现场照片。请先在当前通告里上传至少 1 张现场照片，再发送结束。', 'blocked']);
      }} else {{
        checks.push(['现场图片', '无要求', 'ok']);
      }}
      checks.push(isMissingTargetRecordId(targetRecordId)
        ? ['目标多维', '缺少目标多维记录 ID，不能直接结束，请先绑定或重新选择目标记录。', 'blocked']
        : ['目标多维', '已绑定目标多维记录，结束会更新同一条记录。', 'ok']);
      if (workType === 'maintenance') {{
        checks.push(mopStatus
          ? ['MOP状态', mopStatus, mopStatus.includes('未') ? 'warn' : 'ok']
          : ['MOP状态', '未读取到MOP状态，不阻塞结束通告。', 'warn']);
      }} else {{
        checks.push(['MOP状态', '当前通告类型不要求MOP。', 'ok']);
      }}
      checks.push(['飞书消息', '确认后发送结束通告；如个人消息失败，可复制文本。', 'ok']);
      list.replaceChildren(...checks.map(([title, detail, tone]) => endCheckItem(title, detail, tone)));
      confirmButton.disabled = checks.some(([, , tone]) => tone === 'blocked');
      pendingEndSubmitter = submitter || null;
      modal.hidden = false;
      confirmButton.focus();
      return true;
    }}
    function closeEndCheck() {{
      const modal = document.getElementById('lite-end-check');
      if (modal) modal.hidden = true;
      pendingEndSubmitter = null;
    }}
    function replaceFromDocument(nextDoc, selector) {{
      const current = document.querySelector(selector);
      const next = nextDoc.querySelector(selector);
      if (current && next) current.replaceWith(next);
    }}
    function captureWorkspaceScroll() {{
      return Array.from(document.querySelectorAll('.workspace .list,.undo-panel-list')).map(node => node.scrollTop || 0);
    }}
    function restoreWorkspaceScroll(positions) {{
      if (!Array.isArray(positions)) return;
      requestAnimationFrame(() => {{
        document.querySelectorAll('.workspace .list,.undo-panel-list').forEach((node, index) => {{
          if (typeof positions[index] === 'number') node.scrollTop = positions[index];
        }});
      }});
    }}
    function applyLiteDocument(nextDoc, url, push, selectors) {{
      const replaceSelectors = selectors || ['#lite-workbench-subtitle', '.status', '.summary', '.toolbar', '.workbench-guide', '.workspace'];
      for (const selector of replaceSelectors) {{
        replaceFromDocument(nextDoc, selector);
      }}
      if (push && url) history.pushState({{ lite: true }}, '', url);
      requestAnimationFrame(hydrateLitePreview);
    }}
    function applyLiteHtml(html, url, push, selectors) {{
      const nextDoc = new DOMParser().parseFromString(html, 'text/html');
      applyLiteDocument(nextDoc, url, push, selectors);
    }}
    function canonicalLiteUrlFromDocument(nextDoc, fallbackUrl) {{
      const form = nextDoc.querySelector('#lite-notice-form');
      const nextScope = form?.querySelector('input[name="scope"]')?.value || getCurrentScope();
      const nextWorkType = form?.querySelector('input[name="work_type"]')?.value || new URLSearchParams(location.search).get('work_type') || 'maintenance';
      const nextMonth = form?.querySelector('input[name="source_month"]')?.value || new URLSearchParams(location.search).get('month') || '';
      const isManual = form?.querySelector('input[name="manual"]')?.value === '1';
      const url = new URL('/workbench-lite', location.origin);
      url.searchParams.set('scope', nextScope);
      url.searchParams.set('work_type', nextWorkType);
      if (nextMonth) url.searchParams.set('month', nextMonth);
      if (isManual) url.searchParams.set('manual', '1');
      return url.pathname + url.search;
    }}
    const liteHtmlCache = window.__clipflowLiteHtmlCache || (window.__clipflowLiteHtmlCache = new Map());
    const liteHtmlCacheTtlMs = 25000;
    let liteNavigateController = null;
    function liteCacheKey(url) {{
      try {{
        const parsed = new URL(url, location.origin);
        return parsed.pathname + parsed.search;
      }} catch {{
        return String(url || '');
      }}
    }}
    function getLiteHtmlCache(url) {{
      const item = liteHtmlCache.get(liteCacheKey(url));
      if (!item || Date.now() - Number(item.at || 0) > liteHtmlCacheTtlMs) return '';
      return String(item.html || '');
    }}
    function setLiteHtmlCache(url, html) {{
      if (!html) return;
      liteHtmlCache.set(liteCacheKey(url), {{ at: Date.now(), html: String(html) }});
      if (liteHtmlCache.size > 12) {{
        const keys = Array.from(liteHtmlCache.keys());
        for (const key of keys.slice(0, liteHtmlCache.size - 12)) liteHtmlCache.delete(key);
      }}
    }}
    function clearLiteHtmlCache() {{
      liteHtmlCache.clear();
    }}
    function parseJsonAttr(node, attrName) {{
      try {{ return JSON.parse(node.getAttribute(attrName) || '{{}}'); }}
      catch {{ return {{}}; }}
    }}
    const liteDraftCache = window.__clipflowLiteDraftCache || (window.__clipflowLiteDraftCache = new Map());
    const liteDraftDomKeys = [
      'action', 'operation_id', 'active_item_id', 'source_record_id', 'target_record_id', 'record_id',
      'repair_management_record_id',
      'manual_id', 'scope', 'source_month', 'work_type', 'notice_type', 'title', 'building', 'buildings', 'specialty',
      'maintenance_cycle', 'level', 'start_time', 'end_time', 'status', 'site_photo_count', 'mop_status',
      'source_work_type', 'converted_from_work_type', 'converted_to_work_type', 'sync_maintenance_target',
      'paired_maintenance_target_record_id', 'paired_maintenance_original_title', 'paired_maintenance_actual_start_time',
      'actual_action_time',
      'name', 'location', 'content', 'reason', 'impact', 'progress',
      'repair_device', 'repair_fault', 'fault_type', 'repair_mode', 'discovery',
      'symptom', 'solution', 'spare_parts', 'fault_time', 'expected_time',
      'device', 'cabinet', 'quantity'
    ];
    function draftCacheKey(draft) {{
      return String(
        draft.operation_id || draft.active_item_id || draft.target_record_id ||
        draft.record_id || draft.source_record_id || draft.manual_id || ''
      ).trim();
    }}
    function safeDraftSnapshot(draft) {{
      const snapshot = {{}};
      for (const key of liteDraftDomKeys) {{
        const value = draft ? draft[key] : null;
        if (value == null || value === '') continue;
        if (Array.isArray(value)) {{
          snapshot[key] = value.map(item => String(item || '')).join('、').slice(0, 300);
        }} else if (typeof value !== 'object') {{
          snapshot[key] = String(value).slice(0, 800);
        }}
      }}
      return snapshot;
    }}
    function rememberLiteDraft(draft) {{
      const key = draftCacheKey(draft || {{}});
      if (key) liteDraftCache.set(key, Object.assign({{}}, draft || {{}}));
      return key;
    }}
    function setSafeDraftAttr(row, draft) {{
      const key = rememberLiteDraft(draft || {{}});
      if (key) row.setAttribute('data-draft-key', key);
      row.setAttribute('data-draft', JSON.stringify(safeDraftSnapshot(draft || {{}})));
    }}
    function draftFromRow(row) {{
      const key = row ? String(row.getAttribute('data-draft-key') || '').trim() : '';
      if (key && liteDraftCache.has(key)) return Object.assign({{}}, liteDraftCache.get(key) || {{}});
      return parseJsonAttr(row, 'data-draft');
    }}
    function compactOptimisticDraft(draft) {{
      const compact = Object.assign({{}}, draft || {{}});
      delete compact.site_photos;
      delete compact.extra_images;
      delete compact.process_site_images;
      delete compact.attachments;
      delete compact.site_photos_json;
      delete compact.process_images_json;
      return compact;
    }}
    const commandPatchKeys = new Set([
      'manual', 'manual_id', 'scope', 'source_month', 'action', 'work_type', 'notice_type', 'operation_id',
      'manual_binding_choice', 'manual_binding_required',
      'active_item_id', 'source_record_id', 'target_record_id', 'record_id',
      'repair_management_record_id',
      'source_work_type', 'converted_from_work_type', 'converted_to_work_type', 'sync_maintenance_target',
      'paired_maintenance_target_record_id', 'paired_maintenance_original_title',
      'paired_maintenance_actual_start_time',
      'building', 'buildings', 'building_code', 'building_codes', 'title', 'name',
      'specialty', 'maintenance_cycle', 'level', 'start_time', 'end_time',
      'actual_action_time', 'response_time',
      'location', 'content', 'reason', 'impact', 'progress',
      'repair_device', 'repair_fault', 'fault_type', 'repair_mode', 'discovery',
      'symptom', 'solution', 'spare_parts', 'fault_time', 'expected_time',
      'cabinet', 'quantity', 'device',
      'status', 'site_photo_count', 'site_photos', 'extra_images',
      'mop_status', 'zhihang_record_id', 'lan_zhihang_record_id', 'zhihang_involved'
    ]);
    const noticeFormValueKeys = [
      'notice_type', 'title', 'specialty', 'maintenance_cycle', 'level',
      'start_time', 'end_time', 'location', 'content', 'reason', 'impact', 'progress',
      'repair_device', 'repair_fault', 'fault_type', 'repair_mode', 'discovery',
      'symptom', 'solution', 'spare_parts', 'cabinet', 'quantity', 'device',
      'actual_action_time'
    ];
    function captureNoticeFormValues(form) {{
      const values = {{}};
      if (!form) return values;
      for (const name of noticeFormValueKeys) {{
        const field = form.elements.namedItem(name);
        if (!field || typeof field.value === 'undefined') continue;
        values[name] = String(field.value == null ? '' : field.value);
      }}
      return values;
    }}
    function compactCommandPatch(patch) {{
      const compact = {{}};
      const source = patch || {{}};
      for (const key of commandPatchKeys) {{
        if (!Object.prototype.hasOwnProperty.call(source, key)) continue;
        const value = source[key];
        if (value == null || value === '') continue;
        if (Array.isArray(value)) {{
          compact[key] = value.map(item => {{
            if (item && typeof item === 'object') {{
              const safe = {{}};
              for (const field of ['upload_id', 'file_token', 'token', 'file_name', 'url', 'mime_type', 'content_type', 'size']) {{
                if (item[field] != null && item[field] !== '') safe[field] = String(item[field]).slice(0, 500);
              }}
              return safe;
            }}
            return String(item || '').slice(0, 500);
          }});
        }} else if (typeof value === 'object') {{
          continue;
        }} else {{
          compact[key] = String(value).slice(0, 2000);
        }}
      }}
      return compact;
    }}
    function ongoingDeletePayload(form) {{
      const fd = new FormData(form);
      const patch = Object.fromEntries(fd.entries());
      const targetRecordId = String(patch.target_record_id || '').trim();
      const sourceRecordId = String(patch.source_record_id || '').trim();
      const rawRecordId = String(patch.record_id || '').trim();
      const recordId = targetRecordId || rawRecordId;
      return {{
        scope: patch.scope || getCurrentScope(),
        work_type: patch.work_type || '',
        notice_type: patch.notice_type || '',
        active_item_id: patch.active_item_id || '',
        source_record_id: sourceRecordId,
        target_record_id: targetRecordId,
        record_id: recordId,
      }};
    }}
    function workbenchBaseUrl(scope, workType) {{
      const url = new URL('/workbench-lite', location.origin);
      url.searchParams.set('scope', scope || getCurrentScope());
      url.searchParams.set('work_type', workType || currentViewWorkType() || 'maintenance');
      const month = new URLSearchParams(location.search).get('month') || '';
      if (month) url.searchParams.set('month', month);
      return url.pathname + url.search;
    }}
    async function deleteOngoingFromForm(button) {{
      const form = button.closest('#lite-notice-form') || document.getElementById('lite-notice-form');
      if (!form) return;
      const mode = String(button.getAttribute('data-ongoing-delete-mode') || 'remote');
      const isLocalRemove = mode === 'local';
      const confirmText = isLocalRemove ? '确认移除' : '确认删除';
      const originalText = button.getAttribute('data-original-text') || button.textContent || (isLocalRemove ? '移除显示' : '删除通告');
      button.setAttribute('data-original-text', originalText);
      if (button.getAttribute('data-confirmed') !== '1') {{
        button.setAttribute('data-confirmed', '1');
        button.textContent = confirmText;
        setLiteStatus(isLocalRemove ? '再次点击确认仅移除前端和 Qt 显示，不删除多维记录。' : '再次点击确认删除通告，并同步删除对应多维记录。');
        window.setTimeout(() => {{
          if (button && button.getAttribute('data-confirmed') === '1') {{
            button.removeAttribute('data-confirmed');
            button.textContent = originalText;
          }}
        }}, 4200);
        return;
      }}
      const payload = ongoingDeletePayload(form);
      const endpoint = isLocalRemove ? '/api/ongoing-items/remove-local' : '/api/ongoing-items/delete';
      clearLiteHtmlCache();
      setLiteFormDirty(false);
      setFormSubmitBusy(form, true);
      setLiteStatus(isLocalRemove ? '正在移除显示...' : '正在删除通告和对应多维记录...');
      try {{
        const response = await fetch(endpoint, {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          credentials: 'same-origin',
          body: JSON.stringify(payload),
        }});
        const data = await response.json().catch(() => ({{}}));
        if (handleLiteAuthRequired(response, data)) return;
        if (!response.ok || data.ok === false) {{
          throw new Error(data.error || (data.data && data.data.error) || (isLocalRemove ? '移除失败' : '删除失败'));
        }}
        const row = findOngoingRowByDraft(payload);
        if (row) removeOngoingRow(row);
        const result = data.data || {{}};
        const message = isLocalRemove
          ? '已移除显示，Qt 和前端将同步消失，多维未删除。'
          : (result.remote_deleted ? '已删除通告，并同步删除对应多维记录。' : '已删除通告，本地显示已同步。');
        setLiteStatus(message);
        await navigateLite(workbenchBaseUrl(payload.scope, currentViewWorkType() || payload.work_type), {{
          push: true,
          label: message,
          selectors: ['.status', '.summary', '.workbench-guide', '.workspace'],
        }});
      }} catch (error) {{
        const message = error && error.message ? error.message : (isLocalRemove ? '移除失败' : '删除失败');
        showLiteError(message);
      }} finally {{
        button.removeAttribute('data-confirmed');
        button.textContent = originalText;
        setFormSubmitBusy(form, false);
      }}
    }}
    function setFormValue(form, name, value) {{
      const field = form.querySelector(`[name="${{CSS.escape(name)}}"]`);
      if (!field) return;
      field.value = value == null ? '' : String(value);
    }}
    const TARGET_FORM_PROTECTED_FIELDS = new Set([
      'scope', 'source_month', 'work_type', 'manual', 'manual_id',
      'record_id', 'source_record_id', 'target_record_id', 'active_item_id',
      'site_photo_count', 'mop_status', 'actual_action_time',
    ]);
    function applyTargetRecordFormFields(form, values) {{
      if (!form || !values || typeof values !== 'object') return 0;
      let changed = 0;
      for (const [name, rawValue] of Object.entries(values)) {{
        if (TARGET_FORM_PROTECTED_FIELDS.has(name)) continue;
        const field = form.querySelector(`[name="${{CSS.escape(name)}}"]`);
        if (!field) continue;
        const value = rawValue == null ? '' : String(rawValue);
        if (field instanceof HTMLSelectElement && value) {{
          const optionExists = Array.from(field.options).some(option => option.value === value);
          if (!optionExists) field.append(new Option(value, value));
        }}
        if (field.value === value) continue;
        field.value = value;
        changed += 1;
      }}
      const title = String(values.title || '').trim();
      const heading = form.querySelector('.detail-head strong');
      if (heading && title) heading.textContent = title;
      updateNoticePreview(form);
      updateActionAvailability(form);
      return changed;
    }}
    function setSourceLinkDisplay(form, sourceId, titleText) {{
      if (!form) return;
      const sourceField = form.querySelector('.source-link-field');
      if (!sourceField) return;
      const normalized = String(sourceId || '').trim();
      const title = sourceField.querySelector('.source-link-title');
      const select = sourceField.querySelector('select');
      if (title) {{
        title.textContent = normalized
          ? (titleText || '已关联')
          : '未关联';
      }}
      if (!select) return;
      if (select.disabled) {{
        select.replaceChildren(new Option(normalized || '暂无源表记录', normalized));
      }} else {{
        select.value = normalized;
      }}
    }}
    function applyIdentityBindingToInbox(form, result, fallbackTargetRecordId) {{
      if (!form) return;
      const sourceRecordId = String(
        result?.source_record_id || previewValue(form, 'source_record_id') || ''
      ).trim();
      const targetRecordId = String(
        result?.target_record_id || fallbackTargetRecordId || ''
      ).trim();
      const activeItemId = String(
        result?.active_item_id || previewValue(form, 'active_item_id') || ''
      ).trim();
      if (sourceRecordId) {{
        const sourceRow = Array.from(document.querySelectorAll('.notice-row')).find(row =>
          [row.getAttribute('data-source-record-id'), row.getAttribute('data-record-id')]
            .map(value => String(value || '').trim())
            .includes(sourceRecordId)
        );
        if (sourceRow) {{
          sourceRow.classList.remove('is-disabled');
          sourceRow.setAttribute('aria-disabled', 'false');
          sourceRow.setAttribute('data-disabled-reason', '');
          sourceRow.setAttribute('data-linked-ongoing', '1');
          sourceRow.setAttribute('data-action', 'update');
          sourceRow.setAttribute('data-active-item-id', activeItemId);
          sourceRow.setAttribute('data-target-record-id', targetRecordId);
          setOngoingRowStatus(sourceRow, '进行中', 'working');
        }}
      }}
      const ongoingRow = Array.from(document.querySelectorAll('.ongoing-row')).find(row => {{
        const identities = [
          row.getAttribute('data-active-item-id'),
          row.getAttribute('data-target-record-id'),
          row.getAttribute('data-record-id'),
        ].map(value => String(value || '').trim()).filter(Boolean);
        return (activeItemId && identities.includes(activeItemId))
          || (targetRecordId && identities.includes(targetRecordId));
      }});
      if (ongoingRow) {{
        ongoingRow.setAttribute('data-source-record-id', sourceRecordId);
        if (targetRecordId) {{
          ongoingRow.setAttribute('data-target-record-id', targetRecordId);
          ongoingRow.setAttribute('data-record-id', targetRecordId);
        }}
      }}
    }}
    async function refreshTaskInboxAfterIdentityBinding() {{
      clearLiteHtmlCache();
      const scrollPositions = captureWorkspaceScroll();
      const response = await fetch(location.pathname + location.search, {{
        credentials: 'same-origin',
        cache: 'no-store',
      }});
      const html = await response.text();
      if (handleLiteAuthRequired(response, null, html)) return;
      if (!response.ok) throw new Error('刷新通告处理状态失败');
      const nextDoc = new DOMParser().parseFromString(html, 'text/html');
      replaceFromDocument(nextDoc, '.task-inbox');
      restoreWorkspaceScroll(scrollPositions);
    }}
    let liteSitePhotos = [];
    let liteSitePhotoSignature = '';
    function sitePhotoSignature(form) {{
      if (!form) return '';
      return [
        previewValue(form, 'scope'),
        previewValue(form, 'work_type'),
        previewValue(form, 'active_item_id'),
        previewValue(form, 'target_record_id'),
        previewValue(form, 'source_record_id'),
        previewValue(form, 'record_id'),
        previewValue(form, 'manual_id'),
      ].join('|');
    }}
    function parseSitePhotosFromForm(form) {{
      const hidden = form?.querySelector('[name="site_photos_json"]');
      if (!hidden) return [];
      try {{
        const parsed = JSON.parse(hidden.value || '[]');
        return Array.isArray(parsed) ? parsed.filter(item => item && typeof item === 'object' && item.upload_id) : [];
      }} catch {{
        return [];
      }}
    }}
    function resetSitePhotoState(form, existingCount) {{
      if (!form) return;
      const panel = form.querySelector('[data-site-photo-panel]');
      const hiddenJson = form.querySelector('[name="site_photos_json"]');
      if (!panel || !hiddenJson) {{
        liteSitePhotos = [];
        liteSitePhotoSignature = '';
        return;
      }}
      const countField = form.querySelector('[name="site_photo_count"]');
      const count = Math.max(0, Number(existingCount ?? countField?.value ?? panel.dataset.existingCount ?? 0) || 0);
      panel.dataset.existingCount = String(count);
      liteSitePhotos = parseSitePhotosFromForm(form);
      liteSitePhotoSignature = sitePhotoSignature(form);
      updateSitePhotoUi(form);
    }}
    function ensureSitePhotoState(form) {{
      if (!form) return;
      if (sitePhotoSignature(form) !== liteSitePhotoSignature) resetSitePhotoState(form);
    }}
    function updateSitePhotoUi(form) {{
      if (!form) return;
      const panel = form.querySelector('[data-site-photo-panel]');
      const hiddenJson = form.querySelector('[name="site_photos_json"]');
      const countField = form.querySelector('[name="site_photo_count"]');
      if (!panel || !hiddenJson || !countField) return;
      const existing = Math.max(0, Number(panel.dataset.existingCount || countField.value || 0) || 0);
      const uploaded = liteSitePhotos.length;
      const total = existing + uploaded;
      hiddenJson.value = JSON.stringify(liteSitePhotos);
      countField.value = String(total);
      const badge = document.getElementById('lite-site-photo-badge');
      if (badge) badge.textContent = total > 0 ? `已累计 ${{total}} 张` : '待添加';
      const status = document.getElementById('lite-site-photo-status');
      if (status) {{
        status.textContent = total > 0
          ? `已累计 ${{total}} 张现场照片，可用于结束校验。`
          : '开始、更新、结束任意一次上传 1 张现场照片后，即可满足结束要求。';
      }}
      const list = document.getElementById('lite-site-photo-list');
      if (list) {{
        const items = liteSitePhotos.map((item, index) => {{
          const chip = document.createElement('span');
          chip.className = 'site-photo-item';
          const label = document.createElement('span');
          label.textContent = item.file_name || `现场照片${{index + 1}}`;
          const remove = document.createElement('button');
          remove.className = 'site-photo-remove';
          remove.type = 'button';
          remove.setAttribute('data-site-photo-remove', String(index));
          remove.setAttribute('aria-label', '移除现场照片');
          remove.textContent = '×';
          chip.append(label, remove);
          return chip;
        }});
        list.replaceChildren(...items);
      }}
      updateActionAvailability(form);
    }}
    function sitePhotoPayload(form) {{
      ensureSitePhotoState(form);
      return liteSitePhotos.map(item => ({{
        upload_id: String(item.upload_id || ''),
        file_name: String(item.file_name || 'site_photo.png'),
        mime_type: String(item.mime_type || item.content_type || 'image/png'),
        size: Number(item.size || 0),
      }})).filter(item => item.upload_id);
    }}
    async function uploadSitePhotoFile(file, form) {{
      if (!file || !form) return null;
      if (!String(file.type || '').startsWith('image/')) throw new Error('只能上传图片作为现场照片。');
      if (file.size > 8 * 1024 * 1024) throw new Error('现场照片不能超过 8MB。');
      const url = '/api/notice-attachments?file_name=' + encodeURIComponent(file.name || 'site_photo.png');
      const response = await fetch(url, {{
        method: 'POST',
        headers: {{ 'Content-Type': file.type || 'image/png' }},
        body: file,
        credentials: 'same-origin',
      }});
      const data = await response.json().catch(() => ({{}}));
      if (handleLiteAuthRequired(response, data)) return null;
      if (!response.ok || data.ok === false) throw new Error(data.error || data.message || '现场照片上传失败');
      const item = data.data || data;
      if (!item.upload_id) throw new Error('现场照片上传返回缺少 upload_id');
      return {{
        upload_id: item.upload_id,
        file_name: item.file_name || file.name || 'site_photo.png',
        mime_type: item.mime_type || file.type || 'image/png',
        size: item.size || file.size || 0,
      }};
    }}
    async function handleSitePhotoFiles(files, form) {{
      const panel = form?.querySelector('[data-site-photo-panel]');
      if (!panel) return;
      ensureSitePhotoState(form);
      const fileItems = Array.from(files || []).filter(Boolean);
      if (!fileItems.length) return;
      const status = document.getElementById('lite-site-photo-status');
      panel.classList.add('uploading');
      if (status) status.textContent = '现场照片上传中...';
      const uploaded = [];
      try {{
        for (const file of fileItems) {{
          const item = await uploadSitePhotoFile(file, form);
          if (item) uploaded.push(item);
        }}
        liteSitePhotos = liteSitePhotos.concat(uploaded);
        setLiteFormDirty(true);
        updateSitePhotoUi(form);
        setLiteStatus(`已添加现场照片 ${{uploaded.length}} 张`);
      }} catch (error) {{
        if (status) status.textContent = error && error.message ? error.message : '现场照片上传失败';
        showLiteError(error && error.message ? error.message : '现场照片上传失败');
      }} finally {{
        panel.classList.remove('uploading');
        const input = form.querySelector('#lite-site-photo-input');
        if (input) input.value = '';
      }}
    }}
    function isTextEditingTarget(target) {{
      if (!(target instanceof Element)) return false;
      if (target.closest('[data-site-photo-panel]')) return false;
      return Boolean(target.closest('input, textarea, select, [contenteditable="true"]'));
    }}
    function clipboardImageFiles(event) {{
      const data = event.clipboardData;
      if (!data) return [];
      const files = [];
      Array.from(data.files || []).forEach(file => {{
        if (file && String(file.type || '').startsWith('image/')) files.push(file);
      }});
      Array.from(data.items || []).forEach((item, index) => {{
        if (!item || item.kind !== 'file' || !String(item.type || '').startsWith('image/')) return;
        const file = item.getAsFile && item.getAsFile();
        if (!file) return;
        const named = file.name ? file : new File(
          [file],
          `粘贴现场照片${{Date.now()}}_${{index + 1}}.${{String(file.type || 'image/png').split('/')[1] || 'png'}}`,
          {{ type: file.type || 'image/png' }}
        );
        if (!files.some(existing => existing === named || (existing.size === named.size && existing.type === named.type && existing.name === named.name))) {{
          files.push(named);
        }}
      }});
      return files;
    }}
    function sitePhotoPanelFromTarget(target) {{
      const element = target instanceof Element ? target : null;
      return element?.closest('[data-site-photo-panel]') || document.querySelector('#lite-notice-form [data-site-photo-panel]');
    }}
    function sitePhotoDropFromTarget(target) {{
      const panel = sitePhotoPanelFromTarget(target);
      return panel?.querySelector('.site-photo-drop') || null;
    }}
    async function handleSitePhotoPaste(event) {{
      const target = event.target instanceof Element ? event.target : null;
      if (isTextEditingTarget(target)) return;
      const panel = sitePhotoPanelFromTarget(target);
      if (!panel) return;
      const files = clipboardImageFiles(event);
      if (!files.length) {{
        if (target?.closest('[data-site-photo-panel]')) {{
          const status = document.getElementById('lite-site-photo-status');
          if (status) status.textContent = '剪贴板里没有图片。请复制截图/图片文件后再粘贴。';
          setLiteStatus('剪贴板里没有可上传的现场照片');
        }}
        return;
      }}
      event.preventDefault();
      const form = panel.closest('#lite-notice-form') || document.getElementById('lite-notice-form');
      const drop = panel.querySelector('.site-photo-drop');
      if (drop) drop.classList.add('dragover');
      try {{
        await handleSitePhotoFiles(files, form);
      }} finally {{
        if (drop) drop.classList.remove('dragover');
      }}
    }}
    function setDetailModeNote(text) {{
      const note = document.querySelector('.detail-mode-note');
      if (!note) return;
      note.textContent = text || '';
      note.hidden = !text;
    }}
    const previewTemplates = {{
      maintenance: {{
        heading: '维保通告',
        fields: [
          ['名称', 'title'], ['时间', '__range'], ['位置', 'location'],
          ['内容', 'content'], ['原因', 'reason'], ['影响', 'impact'], ['进度', 'progress']
        ]
      }},
      change: {{
        heading: '变更通告',
        fields: [
          ['名称', 'title'], ['等级', 'level'], ['时间', '__range'], ['位置', 'location'],
          ['内容', 'content'], ['原因', 'reason'], ['影响', 'impact'], ['进度', 'progress']
        ]
      }},
      repair: {{
        heading: '设备检修',
        fields: [
          ['标题', 'title'], ['地点', 'location'], ['紧急程度', 'level'], ['专业', 'specialty'],
          ['发现故障时间', 'end_time'], ['期望完成时间', 'start_time'],
          ['维修设备', 'repair_device'], ['维修故障', 'repair_fault'], ['故障类型', 'fault_type'],
          ['维修方式', 'repair_mode'], ['影响范围', 'impact'], ['故障发现方式', 'discovery'],
          ['故障现象', 'symptom'], ['故障原因', 'reason'], ['解决方案', 'solution'],
          ['备件更换情况', 'spare_parts'], ['完成情况', 'progress']
        ]
      }},
      power: {{
        heading: '上电通告',
        fields: [['名称', 'title'], ['时间', '__range'], ['柜号', 'cabinet'], ['数量', 'quantity'], ['进度', 'progress']]
      }},
      polling: {{
        heading: '设备轮巡',
        fields: [['标题', 'title'], ['时间', '__range'], ['设备', 'device'], ['内容', 'content'], ['影响', 'impact'], ['进度', 'progress']]
      }},
      adjust: {{
        heading: '设备调整',
        fields: [['名称', 'title'], ['时间', '__range'], ['位置', 'location'], ['内容', 'content'], ['原因', 'reason'], ['影响', 'impact'], ['进度', 'progress']]
      }}
    }};
    function previewValue(form, name) {{
      const field = form.querySelector(`[name="${{CSS.escape(name)}}"]`);
      return field ? String(field.value || '').trim() : '';
    }}
    function previewDate(value) {{
      return String(value || '').trim().replace('T', ' ');
    }}
    const previewDateFieldKeys = new Set(['start_time', 'end_time', 'fault_time', 'expected_time', 'response_time']);
    function isPreviewDateField(key) {{
      return previewDateFieldKeys.has(String(key || ''));
    }}
    function previewActionLabel(action) {{
      return action === 'end' ? '结束' : (action === 'update' ? '更新' : '开始');
    }}
    const requiredUploadFields = {{
      maintenance: [
        ['title', '名称'], ['start_time', '开始时间'], ['end_time', '结束时间'],
        ['location', '位置'], ['content', '内容'], ['reason', '原因'], ['impact', '影响'],
        ['progress', '进度'], ['specialty', '专业'], ['maintenance_cycle', '维保周期']
      ],
      change: [
        ['title', '名称'], ['level', '等级'], ['start_time', '开始时间'], ['end_time', '结束时间'],
        ['location', '位置'], ['content', '内容'], ['reason', '原因'], ['impact', '影响'],
        ['progress', '进度'], ['specialty', '专业']
      ],
      repair: [
        ['title', '标题'], ['location', '地点'], ['level', '紧急程度'], ['specialty', '专业'],
        ['end_time', '发现故障时间'], ['start_time', '期望完成时间'],
        ['repair_device', '维修设备'], ['repair_fault', '维修故障'], ['fault_type', '故障类型'],
        ['repair_mode', '维修方式'], ['impact', '影响范围'], ['discovery', '故障发现方式'],
        ['symptom', '故障现象'], ['reason', '故障原因'], ['solution', '解决方案'], ['progress', '完成情况']
      ],
      power: [
        ['title', '名称'], ['start_time', '开始时间'], ['end_time', '结束时间'],
        ['cabinet', '柜号'], ['quantity', '数量'], ['progress', '进度'], ['specialty', '专业']
      ],
      polling: [
        ['title', '标题'], ['start_time', '开始时间'], ['end_time', '结束时间'],
        ['device', '设备'], ['content', '内容'], ['impact', '影响'], ['progress', '进度'], ['specialty', '专业']
      ],
      adjust: [
        ['title', '名称'], ['start_time', '开始时间'], ['end_time', '结束时间'],
        ['location', '位置'], ['content', '内容'], ['reason', '原因'], ['impact', '影响'],
        ['progress', '进度'], ['specialty', '专业']
      ]
    }};
    function requiredFieldsFor(workType) {{
      return requiredUploadFields[workType] || requiredUploadFields.maintenance;
    }}
    function missingRequiredFields(form) {{
      const workType = form?.querySelector('[name="work_type"]')?.value || form?.dataset.workType || 'maintenance';
      return requiredFieldsFor(workType)
        .filter(([name]) => !previewValue(form, name))
        .map(([, label]) => label);
    }}
    function manualBindingIssue(form) {{
      if (!form || previewValue(form, 'manual') !== '1' || previewValue(form, 'manual_binding_required') !== '1') return '';
      const choice = previewValue(form, 'manual_binding_choice');
      if (!choice) return '请选择绑定计划通告或不绑定';
      if (choice === 'bind' && !previewValue(form, 'source_record_id')) return '请选择要绑定的计划通告';
      if (!['bind', 'unbound'].includes(choice)) return '计划通告绑定方式无效';
      return '';
    }}
    function parseLiteDateTime(value) {{
      const raw = String(value || '').trim();
      if (!raw) return null;
      const normalized = raw.replace('T', ' ');
      const match = normalized.match(/^(\\d{{4}})-(\\d{{1,2}})-(\\d{{1,2}})\\s+(\\d{{1,2}}):(\\d{{1,2}})/);
      if (!match) return null;
      const [, year, month, day, hour, minute] = match;
      const parsed = new Date(
        Number(year),
        Number(month) - 1,
        Number(day),
        Number(hour),
        Number(minute),
        0,
        0
      );
      return Number.isNaN(parsed.getTime()) ? null : parsed;
    }}
    function noticeDurationIssue(form) {{
      const workType = form?.querySelector('[name="work_type"]')?.value || form?.dataset.workType || 'maintenance';
      const startName = workType === 'repair' ? 'end_time' : 'start_time';
      const endName = workType === 'repair' ? 'start_time' : 'end_time';
      const startLabel = workType === 'repair' ? '发现故障时间' : '开始时间';
      const endLabel = workType === 'repair' ? '期望完成时间' : '结束时间';
      const start = parseLiteDateTime(previewValue(form, startName));
      const end = parseLiteDateTime(previewValue(form, endName));
      if (!start || !end) return '';
      if (end.getTime() - start.getTime() < 60 * 60 * 1000) {{
        return `${{startLabel}}和${{endLabel}}之间不能少于1小时。`;
      }}
      return '';
    }}
    function buildPreviewLine(form, label, key) {{
      let value = '';
      if (key === '__range') {{
        const start = previewDate(previewValue(form, 'start_time'));
        const end = previewDate(previewValue(form, 'end_time'));
        value = start && end ? `${{start}}~${{end}}` : (start || end);
      }} else {{
        const rawValue = previewValue(form, key);
        value = isPreviewDateField(key) ? previewDate(rawValue) : rawValue;
      }}
      return `【${{label}}】${{value}}`;
    }}
    function updateNoticePreview(form) {{
      const targetForm = form || document.getElementById('lite-notice-form');
      const preview = document.getElementById('lite-notice-preview');
      if (!targetForm || !preview) return;
      const workType = targetForm.querySelector('[name="work_type"]')?.value || targetForm.dataset.workType || 'maintenance';
      const template = previewTemplates[workType] || previewTemplates.maintenance;
      const noticeType = previewValue(targetForm, 'notice_type');
      const heading = workType === 'power' && ['上电通告', '下电通告'].includes(noticeType)
        ? noticeType
        : template.heading;
      const action = targetForm.dataset.action || 'start';
      const lines = [`【${{heading}}】状态：${{previewActionLabel(action)}}`];
      for (const [label, key] of template.fields) {{
        lines.push(buildPreviewLine(targetForm, label, key));
      }}
      preview.textContent = lines.join('\\n');
      updateActionAvailability(targetForm);
    }}
    function setActionReason(text, tone) {{
      const reason = document.getElementById('lite-action-reason');
      if (!reason) return;
      reason.textContent = text;
      reason.className = 'action-reason ' + (tone || '');
    }}
    function updateActionAvailability(form) {{
      const targetForm = form || document.getElementById('lite-notice-form');
      if (!targetForm) return;
      const title = previewValue(targetForm, 'title');
      const action = targetForm.dataset.action || 'start';
      const workType = targetForm.querySelector('[name="work_type"]')?.value || targetForm.dataset.workType || 'maintenance';
      const sitePhotoCount = Number(previewValue(targetForm, 'site_photo_count') || 0);
      const buttons = targetForm.querySelectorAll('button[name="submit_action"]');
      const bindingIssue = manualBindingIssue(targetForm);
      if (bindingIssue) {{
        buttons.forEach(button => button.disabled = true);
        setActionReason(bindingIssue, 'blocked');
        return;
      }}
      const missing = missingRequiredFields(targetForm);
      if (missing.length) {{
        buttons.forEach(button => button.disabled = true);
        setActionReason('缺少' + missing.join('、') + '，暂不能发送', 'blocked');
        return;
      }}
      const durationIssue = noticeDurationIssue(targetForm);
      if (durationIssue) {{
        buttons.forEach(button => button.disabled = true);
        setActionReason(durationIssue, 'blocked');
        return;
      }}
      if (targetForm.dataset.targetEnded === '1') {{
        buttons.forEach(button => button.disabled = true);
        setActionReason('已关联已结束目标记录，不需要再次发送', 'blocked');
        return;
      }}
      buttons.forEach(button => button.disabled = false);
      if (action === 'end' && ['maintenance', 'change', 'repair'].includes(workType)) {{
        if (sitePhotoCount > 0) {{
          setActionReason(`已累计现场图片${{sitePhotoCount}}张，可结束`, 'ready');
        }} else {{
          setActionReason('尚未看到现场图片，结束时后端按累计记录校验', 'warn');
        }}
        return;
      }}
      setActionReason('字段可发送', 'ready');
    }}
    let liteTargetCandidates = [];
    let liteSelectedTargetIndex = -1;
    function closeTargetCandidates() {{
      const modal = document.getElementById('lite-target-candidates');
      if (modal) modal.hidden = true;
      liteTargetCandidates = [];
      liteSelectedTargetIndex = -1;
    }}
    let liteManualSourceCandidates = [];
    let liteSelectedManualSourceIndex = -1;
    let liteManualSourceSearchTimer = 0;
    function manualSourceModal() {{
      return document.getElementById('lite-manual-source-candidates');
    }}
    function closeManualSourceCandidates() {{
      const modal = manualSourceModal();
      if (modal) modal.hidden = true;
      liteManualSourceCandidates = [];
      liteSelectedManualSourceIndex = -1;
    }}
    function setManualBindingChoice(form, choice, candidate) {{
      if (!form) return;
      const normalized = ['bind', 'unbound'].includes(choice) ? choice : '';
      setFormValue(form, 'manual_binding_choice', normalized);
      const sourceId = normalized === 'bind' ? String(candidate?.source_record_id || '').trim() : '';
      setFormValue(form, 'source_record_id', sourceId);
      setFormValue(form, 'record_id', sourceId);
      const workType = previewValue(form, 'work_type') || form.dataset.workType || '';
      setFormValue(
        form,
        'repair_management_record_id',
        normalized === 'bind' && workType === 'repair' ? sourceId : ''
      );
      const panel = form.querySelector('[data-manual-source-binding]');
      panel?.querySelectorAll('[data-manual-binding-mode]').forEach(button => {{
        button.classList.toggle('active', button.getAttribute('data-manual-binding-mode') === normalized);
      }});
      const status = document.getElementById('lite-manual-binding-status');
      if (status) {{
        status.textContent = normalized === 'bind'
          ? (candidate?.title || '已绑定计划通告')
          : (normalized === 'unbound' ? '不绑定计划通告' : '请选择绑定方式');
        status.classList.toggle('ready', Boolean(normalized));
      }}
      setLiteFormDirty(true);
      updateNoticePreview(form);
    }}
    function renderManualSourceCandidates(items) {{
      const list = document.getElementById('lite-manual-source-list');
      const confirmButton = document.getElementById('lite-manual-source-confirm');
      if (!list || !confirmButton) return;
      liteManualSourceCandidates = Array.isArray(items) ? items : [];
      liteSelectedManualSourceIndex = -1;
      confirmButton.disabled = true;
      if (!liteManualSourceCandidates.length) {{
        const empty = document.createElement('div');
        empty.className = 'target-candidate-empty';
        empty.textContent = '没有可绑定的计划通告。未结束和已结束通告不会出现在这里。';
        list.replaceChildren(empty);
        return;
      }}
      const rows = liteManualSourceCandidates.map((candidate, index) => {{
        const row = document.createElement('button');
        row.className = 'target-candidate-row';
        row.type = 'button';
        row.setAttribute('data-manual-source-index', String(index));
        const title = document.createElement('strong');
        title.textContent = candidate.title || '未命名计划通告';
        const meta = document.createElement('span');
        meta.textContent = [
          candidate.progress || '未开始',
          candidate.building || '',
          candidate.specialty || '',
        ].filter(Boolean).join(' · ');
        row.append(title, meta);
        return row;
      }});
      list.replaceChildren(...rows);
    }}
    async function loadManualSourceCandidates(search) {{
      const form = document.getElementById('lite-notice-form');
      const list = document.getElementById('lite-manual-source-list');
      if (!form || !list) return;
      const loading = document.createElement('div');
      loading.className = 'target-candidate-empty';
      loading.textContent = '正在读取计划通告...';
      list.replaceChildren(loading);
      const url = new URL('/api/workbench/source-options', location.origin);
      url.searchParams.set('scope', previewValue(form, 'scope') || getCurrentScope());
      url.searchParams.set('work_type', previewValue(form, 'work_type') || form.dataset.workType || 'maintenance');
      const sourceMonth = previewValue(form, 'source_month') || new URLSearchParams(location.search).get('month') || '';
      if (sourceMonth) url.searchParams.set('month', sourceMonth);
      if (String(search || '').trim()) url.searchParams.set('q', String(search || '').trim());
      const response = await fetch(url.pathname + url.search, {{ credentials: 'same-origin', cache: 'no-store' }});
      const data = await response.json().catch(() => ({{}}));
      if (handleLiteAuthRequired(response, data)) return;
      if (!response.ok || data.ok === false) throw new Error(data.error || '读取计划通告失败');
      renderManualSourceCandidates((data.data && data.data.items) || data.items || []);
    }}
    async function openManualSourceCandidates() {{
      const modal = manualSourceModal();
      const search = document.getElementById('lite-manual-source-search');
      if (!modal) return;
      modal.hidden = false;
      if (search) search.value = '';
      try {{
        await loadManualSourceCandidates('');
        search?.focus();
      }} catch (error) {{
        closeManualSourceCandidates();
        showLiteError(error && error.message ? error.message : '读取计划通告失败');
      }}
    }}
    function selectManualSourceCandidate(index) {{
      liteSelectedManualSourceIndex = Number.isInteger(index) ? index : -1;
      document.querySelectorAll('[data-manual-source-index]').forEach(row => {{
        row.classList.toggle('active', Number(row.getAttribute('data-manual-source-index')) === liteSelectedManualSourceIndex);
      }});
      const confirmButton = document.getElementById('lite-manual-source-confirm');
      if (confirmButton) confirmButton.disabled = liteSelectedManualSourceIndex < 0;
    }}
    const liteRepairNoticeDraftFields = [
      'title', 'location', 'level', 'specialty', 'start_time', 'end_time',
      'content', 'repair_device', 'repair_fault', 'fault_type', 'repair_mode',
      'impact', 'discovery', 'symptom', 'reason', 'solution', 'spare_parts',
      'progress'
    ];
    function applyRepairNoticeDraft(form, draft) {{
      if (!form || !draft || typeof draft !== 'object') return 0;
      let filledCount = 0;
      for (const key of liteRepairNoticeDraftFields) {{
        const value = normalizeRepairEventPrefillValue(key, draft[key]);
        const field = form.querySelector(`[name="${{CSS.escape(key)}}"]`);
        if (!field || String(field.value || '') === value) continue;
        field.value = value;
        filledCount += 1;
      }}
      return filledCount;
    }}
    async function confirmManualSourceCandidate() {{
      const candidate = liteManualSourceCandidates[liteSelectedManualSourceIndex];
      const form = document.getElementById('lite-notice-form');
      if (!candidate || !form) return;
      const workType = previewValue(form, 'work_type') || form.dataset.workType || '';
      if (workType !== 'repair') {{
        setManualBindingChoice(form, 'bind', candidate);
        closeManualSourceCandidates();
        setLiteStatus('已绑定计划通告');
        return;
      }}
      const sourceRecordId = String(candidate.source_record_id || '').trim();
      if (!sourceRecordId) {{
        showLiteError('所选维修单缺少源记录 ID。');
        return;
      }}
      const manualIdAtStart = previewValue(form, 'manual_id');
      const confirmButton = document.getElementById('lite-manual-source-confirm');
      setButtonBusy(confirmButton, true);
      try {{
        const url = new URL('/api/workbench/repair-event-prefill', location.origin);
        url.searchParams.set(
          'scope',
          previewValue(form, 'scope') || getCurrentScope()
        );
        url.searchParams.set('repair_management_record_id', sourceRecordId);
        const response = await fetch(url.pathname + url.search, {{
          credentials: 'same-origin',
          cache: 'no-store',
        }});
        const data = await response.json().catch(() => ({{}}));
        if (handleLiteAuthRequired(response, data)) return;
        if (!response.ok || data.ok === false) {{
          throw new Error(data.error || '读取维修单字段失败');
        }}
        const activeForm = document.getElementById('lite-notice-form');
        if (
          activeForm !== form
          || previewValue(form, 'manual_id') !== manualIdAtStart
        ) {{
          closeManualSourceCandidates();
          setLiteStatus('当前通告已切换，未改写当前表单');
          return;
        }}
        const result = data.data || data;
        if (String(result.target_record_id || '').trim()) {{
          throw new Error('所选维修单已关联检修通告，请从未结束通告中处理');
        }}
        const draft = result.draft && typeof result.draft === 'object'
          ? result.draft
          : {{}};
        setManualBindingChoice(form, 'bind', candidate);
        const filledCount = applyRepairNoticeDraft(form, draft);
        setFormValue(
          form,
          'repair_management_record_id',
          result.repair_management_record_id || sourceRecordId
        );
        setLiteFormDirty(true);
        updateNoticePreview(form);
        closeManualSourceCandidates();
        setLiteStatus(
          filledCount
            ? `已绑定维修单并填入 ${{filledCount}} 项字段`
            : '已绑定维修单'
        );
      }} catch (error) {{
        showLiteError(error && error.message ? error.message : '绑定维修单失败');
      }} finally {{
        setButtonBusy(confirmButton, false);
      }}
    }}
    let liteRepairEventCandidates = [];
    let liteSelectedRepairEventIndex = -1;
    let liteRepairEventSearchTimer = 0;
    let liteRepairEventRequestController = null;
    let liteRepairEventRequestSequence = 0;
    let liteRepairEventPrefillController = null;
    let liteRepairEventPrefillSequence = 0;
    function repairEventModal() {{
      return document.getElementById('lite-repair-event-candidates');
    }}
    function closeRepairEventCandidates() {{
      window.clearTimeout(liteRepairEventSearchTimer);
      liteRepairEventSearchTimer = 0;
      liteRepairEventRequestSequence += 1;
      if (liteRepairEventRequestController) liteRepairEventRequestController.abort();
      liteRepairEventRequestController = null;
      liteRepairEventPrefillSequence += 1;
      if (liteRepairEventPrefillController) liteRepairEventPrefillController.abort();
      liteRepairEventPrefillController = null;
      const modal = repairEventModal();
      if (modal) modal.hidden = true;
      liteRepairEventCandidates = [];
      liteSelectedRepairEventIndex = -1;
    }}
    function resetRepairEventSelection(form, visible) {{
      const panel = form?.querySelector('[data-repair-event-link]');
      if (!panel) return;
      panel.hidden = !visible;
      setFormValue(form, 'related_event_record_id', '');
      setFormValue(form, 'repair_management_record_id', '');
      const status = panel.querySelector('#lite-repair-event-link-status');
      if (status) status.textContent = '未选择';
    }}
    function renderRepairEventCandidates(items) {{
      const list = document.getElementById('lite-repair-event-list');
      const confirmButton = document.getElementById('lite-repair-event-confirm');
      if (!list || !confirmButton) return;
      liteRepairEventCandidates = Array.isArray(items) ? items : [];
      liteSelectedRepairEventIndex = -1;
      confirmButton.disabled = true;
      if (!liteRepairEventCandidates.length) {{
        const empty = document.createElement('div');
        empty.className = 'target-candidate-empty';
        empty.textContent = '没有检修进展不足 100% 的事件转检修记录。';
        list.replaceChildren(empty);
        return;
      }}
      const rows = liteRepairEventCandidates.map((candidate, index) => {{
        const row = document.createElement('button');
        row.className = 'target-candidate-row';
        row.type = 'button';
        row.setAttribute('data-repair-event-index', String(index));
        const title = document.createElement('strong');
        title.textContent = candidate.title || candidate.project_title || '未命名事件';
        const project = document.createElement('span');
        project.textContent = [
          candidate.project_title && candidate.project_title !== candidate.title
            ? '维修项目 ' + candidate.project_title
            : '',
          candidate.building || '',
          candidate.specialty || '',
          candidate.source || '',
          candidate.occurrence_time ? '事件时间 ' + candidate.occurrence_time : '',
        ].filter(Boolean).join(' · ');
        const progress = document.createElement('small');
        progress.textContent = '检修进展 ' + (candidate.progress_label || `${{Number(candidate.progress_percent || 0)}}%`);
        row.append(title, project, progress);
        return row;
      }});
      list.replaceChildren(...rows);
    }}
    async function loadRepairEventCandidates(search) {{
      const form = document.getElementById('lite-notice-form');
      const list = document.getElementById('lite-repair-event-list');
      if (!form || !list) return false;
      const requestSequence = ++liteRepairEventRequestSequence;
      if (liteRepairEventRequestController) liteRepairEventRequestController.abort();
      const controller = new AbortController();
      liteRepairEventRequestController = controller;
      const loading = document.createElement('div');
      loading.className = 'target-candidate-empty';
      loading.textContent = '正在读取事件转检修记录...';
      list.replaceChildren(loading);
      const url = new URL('/api/workbench/repair-event-candidates', location.origin);
      url.searchParams.set('scope', previewValue(form, 'scope') || getCurrentScope());
      url.searchParams.set('limit', '100');
      if (String(search || '').trim()) url.searchParams.set('q', String(search || '').trim());
      try {{
        const response = await fetch(url.pathname + url.search, {{
          credentials: 'same-origin',
          cache: 'no-store',
          signal: controller.signal,
        }});
        const data = await response.json().catch(() => ({{}}));
        if (controller.signal.aborted || requestSequence !== liteRepairEventRequestSequence) {{
          return false;
        }}
        if (handleLiteAuthRequired(response, data)) return false;
        if (!response.ok || data.ok === false) {{
          throw new Error(data.error || '读取事件转检修记录失败');
        }}
        const result = data.data || data;
        renderRepairEventCandidates(result.records || []);
        const warnings = Array.isArray(result.warnings) ? result.warnings.filter(Boolean) : [];
        setLiteStatus(
          warnings.length
            ? warnings.join('；')
            : `找到 ${{Number(result.total || (result.records || []).length)}} 条可选记录`
        );
        return true;
      }} catch (error) {{
        if (controller.signal.aborted || requestSequence !== liteRepairEventRequestSequence) {{
          return false;
        }}
        throw error;
      }} finally {{
        if (liteRepairEventRequestController === controller) {{
          liteRepairEventRequestController = null;
        }}
      }}
    }}
    async function openRepairEventCandidates(opener) {{
      const form = document.getElementById('lite-notice-form');
      const panel = form?.querySelector('[data-repair-event-link]');
      if (!form || !panel || panel.hidden || form.dataset.detailMode !== 'source') return;
      const modal = repairEventModal();
      const search = document.getElementById('lite-repair-event-search');
      if (!modal) return;
      modal.hidden = false;
      if (search) search.value = '';
      setButtonBusy(opener, true);
      try {{
        const loaded = await loadRepairEventCandidates('');
        if (loaded) search?.focus();
      }} catch (error) {{
        closeRepairEventCandidates();
        showLiteError(error && error.message ? error.message : '读取事件转检修记录失败');
      }} finally {{
        setButtonBusy(opener, false);
      }}
    }}
    function selectRepairEventCandidate(index) {{
      liteSelectedRepairEventIndex = Number.isInteger(index) ? index : -1;
      document.querySelectorAll('[data-repair-event-index]').forEach(row => {{
        row.classList.toggle(
          'active',
          Number(row.getAttribute('data-repair-event-index')) === liteSelectedRepairEventIndex
        );
      }});
      const confirmButton = document.getElementById('lite-repair-event-confirm');
      if (confirmButton) confirmButton.disabled = liteSelectedRepairEventIndex < 0;
    }}
    function normalizeRepairEventPrefillValue(name, value) {{
      const normalized = String(value == null ? '' : value).trim();
      if (!normalized || !['start_time', 'end_time'].includes(name)) return normalized;
      const dateTime = normalized.replace(' ', 'T');
      return /^\\d{{4}}-\\d{{2}}-\\d{{2}}T\\d{{2}}:\\d{{2}}/.test(dateTime)
        ? dateTime.slice(0, 16)
        : normalized;
    }}
    async function confirmRepairEventCandidate() {{
      const candidate = liteRepairEventCandidates[liteSelectedRepairEventIndex];
      const form = document.getElementById('lite-notice-form');
      const repairManagementRecordId = String(candidate?.repair_management_record_id || '').trim();
      if (!candidate || !form || !repairManagementRecordId) return;
      const sourceRecordIdAtStart = previewValue(form, 'source_record_id');
      const recordIdAtStart = previewValue(form, 'record_id');
      const detailModeAtStart = String(form.dataset.detailMode || '');
      const wasDirtyAtStart = liteFormDirty;
      if (!sourceRecordIdAtStart || detailModeAtStart !== 'source') {{
        showLiteError('请先选择一条待发起检修事项。');
        return;
      }}
      const requestSequence = ++liteRepairEventPrefillSequence;
      if (liteRepairEventPrefillController) liteRepairEventPrefillController.abort();
      const controller = new AbortController();
      liteRepairEventPrefillController = controller;
      const confirmButton = document.getElementById('lite-repair-event-confirm');
      setButtonBusy(confirmButton, true);
      try {{
        const response = await fetch('/api/workbench/repair-event-bind', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          credentials: 'same-origin',
          cache: 'no-store',
          signal: controller.signal,
          body: JSON.stringify({{
            scope: previewValue(form, 'scope') || getCurrentScope(),
            source_record_id: sourceRecordIdAtStart,
            event_record_id: String(candidate.event_record_id || '').trim(),
            candidate_project_record_id: repairManagementRecordId,
          }}),
        }});
        const data = await response.json().catch(() => ({{}}));
        if (controller.signal.aborted || requestSequence !== liteRepairEventPrefillSequence) {{
          return;
        }}
        if (handleLiteAuthRequired(response, data)) return;
        if (!response.ok || data.ok === false) {{
          throw new Error(data.error || '保存事件关联失败');
        }}
        const activeForm = document.getElementById('lite-notice-form');
        if (
          activeForm !== form
          || previewValue(form, 'source_record_id') !== sourceRecordIdAtStart
          || previewValue(form, 'record_id') !== recordIdAtStart
          || String(form.dataset.detailMode || '') !== detailModeAtStart
        ) {{
          closeRepairEventCandidates();
          setLiteStatus('事件关联已保存；当前通告已切换，未改写当前表单');
          return;
        }}
        const result = data.data || data;
        const draft = result.draft && typeof result.draft === 'object' ? result.draft : {{}};
        const draftComplete = result.draft_complete === true;
        const filledCount = draftComplete
          ? applyRepairNoticeDraft(form, draft)
          : 0;
        setFormValue(form, 'related_event_record_id', candidate.event_record_id || '');
        setFormValue(form, 'repair_management_record_id', sourceRecordIdAtStart);
        const status = document.getElementById('lite-repair-event-link-status');
        if (status) status.textContent = candidate.title || candidate.project_title || '已选择事件';
        const sourceRow = Array.from(document.querySelectorAll('.notice-row')).find(row =>
          [row.getAttribute('data-source-record-id'), row.getAttribute('data-record-id')]
            .map(value => String(value || '').trim())
            .includes(sourceRecordIdAtStart)
        );
        if (sourceRow) {{
          if (draftComplete) {{
            setSafeDraftAttr(sourceRow, Object.assign({{}}, draftFromRow(sourceRow), draft));
          }}
          sourceRow.setAttribute('data-source-event-id', String(candidate.event_record_id || '').trim());
          sourceRow.setAttribute(
            'data-source-event-title',
            candidate.title || candidate.project_title || '已关联事件'
          );
        }}
        setLiteFormDirty(wasDirtyAtStart);
        updateNoticePreview(form);
        closeRepairEventCandidates();
        const warnings = Array.isArray(result.warnings) ? result.warnings.filter(Boolean) : [];
        setLiteStatus(
          !draftComplete
            ? (warnings.join('；') || '事件关联已保存，页面字段暂未刷新')
            : (
              warnings.length
                ? `事件关联已保存，已更新 ${{filledCount}} 个字段；${{warnings.join('；')}}`
                : `事件关联已保存，已更新 ${{filledCount}} 个字段`
            )
        );
        refreshTaskInboxAfterIdentityBinding().catch(() => {{
          const overlay = noticeDrawerOverlay();
          if (overlay) overlay.dataset.detailNeedsReload = '1';
        }});
      }} catch (error) {{
        if (controller.signal.aborted || requestSequence !== liteRepairEventPrefillSequence) {{
          return;
        }}
        showLiteError(error && error.message ? error.message : '保存事件关联失败');
      }} finally {{
        if (liteRepairEventPrefillController === controller) {{
          liteRepairEventPrefillController = null;
        }}
        setButtonBusy(confirmButton, false);
      }}
    }}
    let pendingUndoButton = null;
    function closeUndoConfirm() {{
      const modal = document.getElementById('lite-undo-confirm');
      if (modal) modal.hidden = true;
      pendingUndoButton = null;
    }}
    function openUndoConfirm(button) {{
      const modal = document.getElementById('lite-undo-confirm');
      if (!modal || !button) return;
      pendingUndoButton = button;
      const title = button.getAttribute('data-undo-title') || '可回退通告';
      const action = button.getAttribute('data-undo-action') || '回退上一步';
      const createdAt = button.getAttribute('data-undo-time') || '';
      const nameNode = document.getElementById('lite-undo-confirm-name');
      const metaNode = document.getElementById('lite-undo-confirm-meta');
      if (nameNode) nameNode.textContent = title;
      if (metaNode) metaNode.textContent = [action, createdAt ? '操作时间 ' + createdAt : ''].filter(Boolean).join(' · ');
      modal.hidden = false;
      document.getElementById('lite-undo-confirm-apply')?.focus();
    }}
    async function pollUndoJob(jobId) {{
      const startedAt = Date.now();
      let delay = 320;
      while (Date.now() - startedAt < 90000) {{
        await sleep(delay);
        const response = await fetch(`/api/jobs/${{encodeURIComponent(jobId)}}`, {{
          credentials: 'same-origin',
          cache: 'no-store',
        }});
        const data = await response.json().catch(() => ({{}}));
        if (handleLiteAuthRequired(response, data)) return false;
        if (!response.ok || data.ok === false) throw new Error(data.error || '读取回退状态失败');
        const job = data.data || {{}};
        const phase = String(job.phase || '');
        if (phase === 'success') return true;
        if (phase === 'failed') throw new Error(job.error || job.upload_message || '回退失败');
        setLiteStatus('回退中：' + jobPhaseText(phase));
        delay = Math.min(1200, Math.round(delay * 1.35));
      }}
      throw new Error('回退仍在处理中，请稍后刷新本页');
    }}
    async function applyPendingUndo() {{
      const undoButton = pendingUndoButton;
      const undoId = undoButton?.getAttribute('data-undo-id') || '';
      if (!undoButton || !undoId) return;
      const applyButton = document.getElementById('lite-undo-confirm-apply');
      setButtonBusy(applyButton, true);
      undoButton.disabled = true;
      clearLiteHtmlCache();
      try {{
        const response = await fetch('/api/notice-undo/' + encodeURIComponent(undoId) + '/apply', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          credentials: 'same-origin',
          body: JSON.stringify({{ scope: getCurrentScope() }}),
        }});
        const data = await response.json().catch(() => ({{}}));
        if (handleLiteAuthRequired(response, data)) return;
        if (!response.ok || data.ok === false) throw new Error(data.error || '回退失败');
        const jobId = (data.data && data.data.job_id) || data.job_id || '';
        closeUndoConfirm();
        setLiteStatus('回退已受理，正在恢复通告...');
        await pollUndoJob(jobId);
        clearLiteHtmlCache();
        await refreshCurrentLite('回退成功，正在更新列表...', ['.status', '.summary', '.workspace']);
        setLiteStatus('回退成功，通告已恢复');
      }} catch (error) {{
        showLiteError(error && error.message ? error.message : '回退失败');
        undoButton.disabled = false;
      }} finally {{
        setButtonBusy(applyButton, false);
      }}
    }}
    function targetCandidateRecordId(candidate) {{
      return String(candidate?.target_record_id || candidate?.record_id || '').trim();
    }}
    function isEndedCandidate(candidate) {{
      return String(candidate?.status || '').includes('结束');
    }}
    function targetLookupPayload(form) {{
      return {{
        scope: previewValue(form, 'scope') || getCurrentScope(),
        work_type: previewValue(form, 'work_type') || form?.dataset.workType || 'maintenance',
        title: previewValue(form, 'title'),
        start_time: previewValue(form, 'start_time'),
        end_time: previewValue(form, 'end_time'),
        action: form?.dataset.action || 'update',
        content: previewValue(form, 'content'),
        reason: previewValue(form, 'reason'),
        impact: previewValue(form, 'impact'),
        progress: previewValue(form, 'progress'),
        text: document.getElementById('lite-notice-preview')?.textContent || '',
      }};
    }}
    function renderTargetCandidates(candidates) {{
      const list = document.getElementById('lite-target-candidate-list');
      const confirmButton = document.getElementById('lite-target-candidates-confirm');
      if (!list || !confirmButton) return;
      liteTargetCandidates = Array.isArray(candidates) ? candidates : [];
      liteSelectedTargetIndex = -1;
      confirmButton.disabled = true;
      if (!liteTargetCandidates.length) {{
        const empty = document.createElement('div');
        empty.className = 'target-candidate-empty';
        empty.textContent = '未找到可关联的目标多维记录。可以先发送开始通告，由后端创建新的目标记录。';
        list.replaceChildren(empty);
        return;
      }}
      const rows = liteTargetCandidates.map((candidate, index) => {{
        const row = document.createElement('button');
        row.className = 'target-candidate-row';
        row.type = 'button';
        row.setAttribute('data-target-candidate-index', String(index));
        const title = document.createElement('strong');
        title.textContent = candidate.title || '未命名目标记录';
        const meta = document.createElement('span');
        meta.textContent = [
          candidate.building || '',
          candidate.status ? '状态 ' + candidate.status : '',
          candidate.start_time || candidate.end_time ? `时间 ${{candidate.start_time || '-'}} ~ ${{candidate.end_time || '-'}}` : '',
        ].filter(Boolean).join(' · ');
        const reason = document.createElement('small');
        reason.textContent = candidate.match_reason || (candidate.date_matched ? '时间匹配' : '相似记录');
        row.append(title, meta, reason);
        return row;
      }});
      list.replaceChildren(...rows);
    }}
    async function openTargetCandidates(form, opener) {{
      if (!form) return;
      const payload = targetLookupPayload(form);
      if (!payload.title) {{
        showLiteError('请先填写标题或名称，再查找目标记录。');
        return;
      }}
      setButtonBusy(opener, true);
      setLiteStatus('正在查找目标多维记录...');
      try {{
        const response = await fetch('/api/notice-target-candidates', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          credentials: 'same-origin',
          body: JSON.stringify(payload),
        }});
        const data = await response.json().catch(() => ({{}}));
        if (handleLiteAuthRequired(response, data)) return;
        if (!response.ok || data.ok === false) throw new Error(data.error || '查询目标记录失败');
        const result = data.data || data;
        renderTargetCandidates(result.candidates || []);
        const modal = document.getElementById('lite-target-candidates');
        if (modal) modal.hidden = false;
        setLiteStatus(`找到 ${{(result.candidates || []).length}} 条候选目标记录`);
      }} catch (error) {{
        showLiteError(error && error.message ? error.message : '查询目标记录失败');
        setLiteStatus(error && error.message ? error.message : '查询目标记录失败');
      }} finally {{
        setButtonBusy(opener, false);
      }}
    }}
    function selectTargetCandidate(index) {{
      liteSelectedTargetIndex = index;
      document.querySelectorAll('.target-candidate-row').forEach((node, nodeIndex) => {{
        node.classList.toggle('active', nodeIndex === index);
      }});
      const confirmButton = document.getElementById('lite-target-candidates-confirm');
      if (confirmButton) confirmButton.disabled = index < 0;
    }}
    async function confirmTargetCandidateBinding() {{
      const candidate = liteTargetCandidates[liteSelectedTargetIndex];
      const form = document.getElementById('lite-notice-form');
      const targetRecordId = targetCandidateRecordId(candidate);
      if (!form || !targetRecordId) return;
      const confirmButton = document.getElementById('lite-target-candidates-confirm');
      setButtonBusy(confirmButton, true);
      try {{
        const payload = {{
          scope: previewValue(form, 'scope') || getCurrentScope(),
          work_type: previewValue(form, 'work_type') || form.dataset.workType || 'maintenance',
          notice_type: previewValue(form, 'notice_type'),
          active_item_id: previewValue(form, 'active_item_id'),
          source_record_id: previewValue(form, 'source_record_id'),
          target_record_id: targetRecordId,
          record_id: targetRecordId,
          title: previewValue(form, 'title') || candidate.title || '',
          reason: previewValue(form, 'reason'),
          start_time: previewValue(form, 'start_time'),
          end_time: previewValue(form, 'end_time'),
          status: candidate.status || '',
        }};
        const response = await fetch('/api/notice-identity/bind', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          credentials: 'same-origin',
          body: JSON.stringify(payload),
        }});
        const data = await response.json().catch(() => ({{}}));
        if (handleLiteAuthRequired(response, data)) return;
        if (!response.ok || data.ok === false) throw new Error(data.error || '绑定目标记录失败');
        const result = data.data || data;
        const targetFormFields = result.target_form_fields
          || result.validation?.target_form_fields
          || candidate.form_fields
          || {{}};
        setFormValue(form, 'target_record_id', targetRecordId);
        const appliedFieldCount = applyTargetRecordFormFields(form, targetFormFields);
        applyIdentityBindingToInbox(form, result, targetRecordId);
        form.dataset.targetEnded = isEndedCandidate(candidate) ? '1' : '';
        const status = document.getElementById('lite-target-link-status');
        if (status) status.textContent = isEndedCandidate(candidate)
          ? '已闭环'
          : '已绑定';
        if (isEndedCandidate(candidate)) {{
          updateNoticePreview(form);
        }} else {{
          setOngoingSubmitButtons(form);
        }}
        setLiteFormDirty(appliedFieldCount > 0);
        closeTargetCandidates();
        setLiteStatus(appliedFieldCount > 0 ? '已绑定目标记录并填入字段' : '目标多维关系已保存');
        refreshTaskInboxAfterIdentityBinding().catch(() => {{
          const overlay = noticeDrawerOverlay();
          if (overlay) overlay.dataset.detailNeedsReload = '1';
        }});
      }} catch (error) {{
        showLiteError(error && error.message ? error.message : '绑定目标记录失败');
      }} finally {{
        setButtonBusy(confirmButton, false);
      }}
    }}
    function hydrateLitePreview() {{
      const form = document.getElementById('lite-notice-form');
      resetSitePhotoState(form);
      resetActualActionTime(form);
      updateNoticePreview(form);
      syncNoticeDrawerState();
    }}
    function setSubmitButtons(form, action) {{
      const actions = form.querySelector('.form-actions');
      if (!actions) return;
      actions.querySelectorAll('button[name="submit_action"],button[data-ongoing-delete-mode],button[data-convert-to-change],button[data-revert-to-maintenance]').forEach(node => node.remove());
      const button = document.createElement('button');
      button.className = 'btn primary';
      button.type = 'submit';
      button.name = 'submit_action';
      button.value = action === 'update' ? 'update' : 'start';
      button.textContent = action === 'update' ? '发送更新' : '发送开始';
      actions.appendChild(button);
      if (
        (previewValue(form, 'work_type') || form.dataset.workType) === 'maintenance' &&
        form.dataset.detailMode === 'source' &&
        previewValue(form, 'source_record_id')
      ) {{
        const convertButton = document.createElement('button');
        convertButton.className = 'btn ghost';
        convertButton.type = 'button';
        convertButton.setAttribute('data-convert-to-change', '1');
        convertButton.textContent = '转为变更通告';
        actions.appendChild(convertButton);
      }}
      if (
        (previewValue(form, 'work_type') || form.dataset.workType) === 'change' &&
        form.dataset.detailMode === 'source' &&
        previewValue(form, 'source_record_id') &&
        (previewValue(form, 'source_work_type') === 'maintenance' || previewValue(form, 'converted_from_work_type') === 'maintenance')
      ) {{
        const revertButton = document.createElement('button');
        revertButton.className = 'btn ghost';
        revertButton.type = 'button';
        revertButton.setAttribute('data-revert-to-maintenance', '1');
        revertButton.textContent = '转回维保';
        actions.appendChild(revertButton);
      }}
      resetActualActionTime(form);
      updateNoticePreview(form);
    }}
    function setOngoingSubmitButtons(form) {{
      const actions = form.querySelector('.form-actions');
      if (!actions) return;
      actions.querySelectorAll('button[name="submit_action"],button[data-ongoing-delete-mode],button[data-convert-to-change],button[data-revert-to-maintenance]').forEach(node => node.remove());
      const updateButton = document.createElement('button');
      updateButton.className = 'btn primary';
      updateButton.type = 'submit';
      updateButton.name = 'submit_action';
      updateButton.value = 'update';
      updateButton.textContent = '发送更新';
      const endButton = document.createElement('button');
      endButton.className = 'btn danger';
      endButton.type = 'submit';
      endButton.name = 'submit_action';
      endButton.value = 'end';
      endButton.textContent = '发送结束';
      const deleteButton = document.createElement('button');
      deleteButton.className = 'btn danger-ghost';
      deleteButton.type = 'button';
      deleteButton.setAttribute('data-ongoing-delete-mode', 'remote');
      deleteButton.textContent = '删除通告';
      actions.appendChild(updateButton);
      actions.appendChild(endButton);
      actions.appendChild(deleteButton);
      if (liteIsAdmin) {{
        const localRemoveButton = document.createElement('button');
        localRemoveButton.className = 'btn danger-ghost';
        localRemoveButton.type = 'button';
        localRemoveButton.setAttribute('data-ongoing-delete-mode', 'local');
        localRemoveButton.textContent = '移除显示';
        actions.appendChild(localRemoveButton);
      }}
      resetActualActionTime(form);
      updateNoticePreview(form);
    }}
    function applySourceRowToDetail(link) {{
      const form = document.getElementById('lite-notice-form');
      if (!form || !link || !link.matches('.notice-row')) return false;
      const workType = link.getAttribute('data-work-type') || '';
      if (workType && form.dataset.workType && workType !== form.dataset.workType) return false;
      const draft = draftFromRow(link);
      const linkedOngoing = link.getAttribute('data-linked-ongoing') === '1';
      const action = linkedOngoing ? 'update' : (link.getAttribute('data-action') || 'start');
      const title = link.getAttribute('data-title') || draft.title || '选择左侧事项';
      form.dataset.action = action;
      form.dataset.detailMode = linkedOngoing ? 'ongoing' : 'source';
      form.dataset.targetEnded = '';
      setFormValue(form, 'manual', '');
      setFormValue(form, 'manual_id', '');
      resetRepairEventSelection(form, !linkedOngoing && workType === 'repair');
      for (const [key, value] of Object.entries(draft)) {{
        setFormValue(form, key, value);
      }}
      const sourceRecordId = link.getAttribute('data-source-record-id') || link.getAttribute('data-record-id') || '';
      const sourceEventId = link.getAttribute('data-source-event-id') || '';
      const sourceEventTitle = link.getAttribute('data-source-event-title') || '';
      const targetRecordId = linkedOngoing ? (link.getAttribute('data-target-record-id') || '') : '';
      setFormValue(form, 'record_id', targetRecordId || sourceRecordId);
      setFormValue(form, 'source_record_id', sourceRecordId);
      setFormValue(form, 'related_event_record_id', sourceEventId);
      setFormValue(form, 'repair_management_record_id', sourceRecordId);
      const repairEventStatus = form.querySelector('#lite-repair-event-link-status');
      if (repairEventStatus && !linkedOngoing && workType === 'repair') {{
        repairEventStatus.textContent = sourceEventTitle || (sourceEventId ? '已关联事件' : '未选择');
      }}
      setSourceLinkDisplay(form, sourceRecordId, '已关联');
      setFormValue(form, 'target_record_id', targetRecordId);
      setFormValue(form, 'active_item_id', linkedOngoing ? (link.getAttribute('data-active-item-id') || '') : '');
      setFormValue(form, 'site_photo_count', link.getAttribute('data-site-photo-count') || '0');
      setFormValue(form, 'mop_status', link.getAttribute('data-mop-status') || '');
      form.querySelector('.detail-head strong')?.replaceChildren(document.createTextNode(title));
      setDetailModeNote('');
      resetSitePhotoState(form, Number(link.getAttribute('data-site-photo-count') || 0));
      if (linkedOngoing) setOngoingSubmitButtons(form);
      else setSubmitButtons(form, action);
      updateNoticePreview(form);
      setLiteStatus(linkedOngoing
        ? '该事项已在进行中，可发送更新、结束或删除'
        : '已选择计划通告，可继续编辑后发送');
      openNoticeDrawer(title, link);
      return true;
    }}
    function applyOngoingRowToDetail(link) {{
      const form = document.getElementById('lite-notice-form');
      if (!form || !link || !link.matches('.ongoing-row')) return false;
      const workType = link.getAttribute('data-work-type') || '';
      if (workType && form.dataset.workType && workType !== form.dataset.workType) return false;
      const draft = draftFromRow(link);
      const title = link.getAttribute('data-title') || draft.title || '未结束通告';
      form.dataset.action = 'update';
      form.dataset.detailMode = 'ongoing';
      form.dataset.targetEnded = '';
      setFormValue(form, 'manual', '');
      setFormValue(form, 'manual_id', '');
      resetRepairEventSelection(form, false);
      setFormValue(form, 'record_id', link.getAttribute('data-target-record-id') || link.getAttribute('data-record-id') || '');
      setFormValue(form, 'source_record_id', link.getAttribute('data-source-record-id') || '');
      setSourceLinkDisplay(form, link.getAttribute('data-source-record-id') || '', '已关联');
      setFormValue(form, 'target_record_id', link.getAttribute('data-target-record-id') || link.getAttribute('data-record-id') || '');
      setFormValue(form, 'active_item_id', link.getAttribute('data-active-item-id') || '');
      setFormValue(form, 'site_photo_count', link.getAttribute('data-site-photo-count') || '0');
      setFormValue(form, 'mop_status', link.getAttribute('data-mop-status') || '');
      for (const [key, value] of Object.entries(draft)) {{
        setFormValue(form, key, value);
      }}
      form.querySelector('.detail-head strong')?.replaceChildren(document.createTextNode(title));
      const hint = form.querySelector('.detail-head em');
      if (hint) hint.textContent = '';
      const sourceId = link.getAttribute('data-source-record-id') || '';
      const targetId = link.getAttribute('data-target-record-id') || link.getAttribute('data-record-id') || '';
      setDetailModeNote(sourceId && targetId
        ? ''
        : (targetId ? '' : '需绑定目标')
      );
      resetSitePhotoState(form, Number(link.getAttribute('data-site-photo-count') || 0));
      setOngoingSubmitButtons(form);
      updateNoticePreview(form);
      setLiteStatus('已选择未结束通告，可发送更新或结束');
      openNoticeDrawer(title, link);
      return true;
    }}
    async function navigateLite(url, options = {{}}) {{
      const useWorkspaceSwitch = Boolean(options.workspaceSwitch);
      const useCache = Boolean(options.useCache);
      setPanelLoading(true);
      if (useWorkspaceSwitch) setWorkspaceSwitching(true, options.loadingText || options.label || '正在加载通告');
      setLiteStatus(options.label || '正在切换...');
      const scrollPositions = options.preserveWorkspaceScroll ? captureWorkspaceScroll() : null;
      let pushedWithCache = false;
      try {{
        if (useCache) {{
          const cachedHtml = getLiteHtmlCache(url);
          if (cachedHtml) {{
            applyLiteHtml(cachedHtml, url, options.push !== false, options.selectors);
            pushedWithCache = options.push !== false;
            if (useWorkspaceSwitch) setWorkspaceSwitching(false);
            setLiteStatus('已切换，正在校准最新数据...');
          }}
        }}
        if (liteNavigateController) liteNavigateController.abort();
        liteNavigateController = new AbortController();
        const response = await fetch(url, {{
          credentials: 'same-origin',
          signal: liteNavigateController.signal,
        }});
        const html = await response.text();
        if (handleLiteAuthRequired(response, null, html)) return;
        if (!response.ok) throw new Error(html || '页面加载失败');
        if (useCache) setLiteHtmlCache(url, html);
        applyLiteHtml(html, url, !pushedWithCache && options.push !== false, options.selectors);
        if (options.preserveWorkspaceScroll) restoreWorkspaceScroll(scrollPositions);
      }} catch (error) {{
        if (error && error.name === 'AbortError') return;
        throw error;
      }} finally {{
        setPanelLoading(false);
        if (useWorkspaceSwitch) setWorkspaceSwitching(false);
      }}
    }}
    async function refreshCurrentLite(label, selectors) {{
      await navigateLite(location.href, {{ push: false, label: label || '正在刷新...', selectors, preserveWorkspaceScroll: true }});
    }}
    async function liteFetchThenRefresh(url, label) {{
      clearLiteHtmlCache();
      setLiteStatus(label + '中...');
      const response = await fetch(url, {{ credentials: 'same-origin' }});
      const data = await response.json().catch(() => ({{}}));
      if (handleLiteAuthRequired(response, data)) return;
      if (!response.ok || data.ok === false) throw new Error(data.error || label + '失败');
      await refreshCurrentLite(label + '完成，正在更新页面...');
    }}
    async function applyCurrentMaintenanceWorkTypeOverride(button, targetWorkType) {{
      const form = button?.closest('#lite-notice-form') || document.getElementById('lite-notice-form');
      if (!form) return;
      const recordId = previewValue(form, 'source_record_id') || previewValue(form, 'record_id');
      if (!recordId) {{
        showLiteError('当前事项缺少源记录，无法转换');
        return;
      }}
      const targetType = targetWorkType === 'maintenance' ? 'maintenance' : 'change';
      const targetLabel = targetType === 'maintenance' ? '维保' : '变更通告';
      setLiteFormDirty(false);
      clearLiteHtmlCache();
      setButtonBusy(button, true);
      setLiteStatus('正在转为' + targetLabel + '...');
      try {{
        const response = await fetch('/api/notice-work-type-override', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          credentials: 'same-origin',
          body: JSON.stringify({{
            scope: getCurrentScope(),
            record_id: recordId,
            source_work_type: 'maintenance',
            target_work_type: targetType
          }})
        }});
        const data = await response.json().catch(() => ({{}}));
        if (handleLiteAuthRequired(response, data)) return;
        if (!response.ok || data.ok === false) {{
          throw new Error(data.error || (data.data && data.data.error) || '转换失败');
        }}
        const url = workbenchBaseUrl(getCurrentScope(), targetType);
        setLiteStatus('已转为' + targetLabel + '，正在打开列表...');
        await navigateLite(url, {{
          push: true,
          label: '已转为' + targetLabel,
          selectors: ['#lite-workbench-subtitle', '.status', '.summary', '.toolbar', '.workbench-guide', '.workspace']
        }});
      }} catch (error) {{
        showLiteError(error && error.message ? error.message : '转换失败');
      }} finally {{
        setButtonBusy(button, false);
      }}
    }}
    document.addEventListener('click', async (event) => {{
      const target = event.target instanceof Element ? event.target : null;
      if (!target) return;
      const noticeDrawerClose = target.closest('#lite-notice-drawer-close');
      if (noticeDrawerClose) {{
        event.preventDefault();
        await requestCloseNoticeDrawer();
        return;
      }}
      const noticeDrawerBackdrop = target.closest('#lite-notice-detail-overlay');
      if (noticeDrawerBackdrop && target === noticeDrawerBackdrop) {{
        event.preventDefault();
        await requestCloseNoticeDrawer();
        return;
      }}
      const pasteDrawerToggle = target.closest('#lite-paste-toggle');
      if (pasteDrawerToggle) {{
        event.preventDefault();
        const pasteDrawer = pasteDrawerToggle.closest('.paste-drawer');
        const content = document.getElementById('lite-paste-content');
        const shouldOpen = !pasteDrawer?.classList.contains('open');
        pasteDrawer?.classList.toggle('open', shouldOpen);
        pasteDrawerToggle.setAttribute('aria-expanded', shouldOpen ? 'true' : 'false');
        if (content) content.hidden = !shouldOpen;
        return;
      }}
      const convertButton = target.closest('button[data-convert-to-change]');
      if (convertButton) {{
        event.preventDefault();
        await applyCurrentMaintenanceWorkTypeOverride(convertButton, 'change');
        return;
      }}
      const revertButton = target.closest('button[data-revert-to-maintenance]');
      if (revertButton) {{
        event.preventDefault();
        await applyCurrentMaintenanceWorkTypeOverride(revertButton, 'maintenance');
        return;
      }}
      const photoRemove = target.closest('[data-site-photo-remove]');
      if (photoRemove) {{
        event.preventDefault();
        const form = photoRemove.closest('#lite-notice-form') || document.getElementById('lite-notice-form');
        ensureSitePhotoState(form);
        const index = Number(photoRemove.getAttribute('data-site-photo-remove'));
        if (Number.isInteger(index) && index >= 0) {{
          liteSitePhotos.splice(index, 1);
          setLiteFormDirty(true);
          updateSitePhotoUi(form);
        }}
        return;
      }}
      const targetSearch = target.closest('#lite-target-search');
      if (targetSearch) {{
        event.preventDefault();
        await openTargetCandidates(document.getElementById('lite-notice-form'), targetSearch);
        return;
      }}
      const targetCandidateRow = target.closest('[data-target-candidate-index]');
      if (targetCandidateRow) {{
        event.preventDefault();
        selectTargetCandidate(Number(targetCandidateRow.getAttribute('data-target-candidate-index')));
        return;
      }}
      const targetCancel = target.closest('#lite-target-candidates-cancel');
      if (targetCancel) {{
        event.preventDefault();
        closeTargetCandidates();
        return;
      }}
      const targetConfirm = target.closest('#lite-target-candidates-confirm');
      if (targetConfirm) {{
        event.preventDefault();
        await confirmTargetCandidateBinding();
        return;
      }}
      const repairEventOpen = target.closest('#lite-repair-event-open');
      if (repairEventOpen) {{
        event.preventDefault();
        await openRepairEventCandidates(repairEventOpen);
        return;
      }}
      const repairEventRow = target.closest('[data-repair-event-index]');
      if (repairEventRow) {{
        event.preventDefault();
        selectRepairEventCandidate(Number(repairEventRow.getAttribute('data-repair-event-index')));
        return;
      }}
      const repairEventCancel = target.closest('#lite-repair-event-cancel');
      if (repairEventCancel) {{
        event.preventDefault();
        closeRepairEventCandidates();
        return;
      }}
      const repairEventConfirm = target.closest('#lite-repair-event-confirm');
      if (repairEventConfirm) {{
        event.preventDefault();
        await confirmRepairEventCandidate();
        return;
      }}
      const discardCancel = target.closest('#lite-discard-cancel');
      if (discardCancel) {{
        event.preventDefault();
        resolveDiscardConfirm(false);
        return;
      }}
      const discardConfirm = target.closest('#lite-discard-confirm-button');
      if (discardConfirm) {{
        event.preventDefault();
        resolveDiscardConfirm(true);
        return;
      }}
      const manualBindingButton = target.closest('[data-manual-binding-mode]');
      if (manualBindingButton) {{
        event.preventDefault();
        const form = manualBindingButton.closest('#lite-notice-form') || document.getElementById('lite-notice-form');
        const mode = manualBindingButton.getAttribute('data-manual-binding-mode') || '';
        if (mode === 'bind') await openManualSourceCandidates();
        else if (mode === 'unbound') {{
          setManualBindingChoice(form, 'unbound', null);
          setLiteStatus('已选择不绑定计划通告');
        }}
        return;
      }}
      const manualSourceRow = target.closest('[data-manual-source-index]');
      if (manualSourceRow) {{
        event.preventDefault();
        selectManualSourceCandidate(Number(manualSourceRow.getAttribute('data-manual-source-index')));
        return;
      }}
      const manualSourceCancel = target.closest('#lite-manual-source-cancel');
      if (manualSourceCancel) {{
        event.preventDefault();
        closeManualSourceCandidates();
        return;
      }}
      const manualSourceConfirm = target.closest('#lite-manual-source-confirm');
      if (manualSourceConfirm) {{
        event.preventDefault();
        await confirmManualSourceCandidate();
        return;
      }}
      const undoCancel = target.closest('#lite-undo-confirm-cancel');
      if (undoCancel) {{
        event.preventDefault();
        closeUndoConfirm();
        return;
      }}
      const undoApply = target.closest('#lite-undo-confirm-apply');
      if (undoApply) {{
        event.preventDefault();
        await applyPendingUndo();
        return;
      }}
      const manualOpen = target.closest('#manual-open');
      if (manualOpen) {{
        event.preventDefault();
        const willOpen = !document.getElementById('manual-picker')?.classList.contains('open');
        setPickerOpen('manual-picker', 'manual-open', willOpen);
        setPickerOpen('refresh-picker', 'refresh-open', false);
        return;
      }}
      const refreshOpen = target.closest('#refresh-open');
      if (refreshOpen) {{
        event.preventDefault();
        const willOpen = !document.getElementById('refresh-picker')?.classList.contains('open');
        setPickerOpen('refresh-picker', 'refresh-open', willOpen);
        setPickerOpen('manual-picker', 'manual-open', false);
        return;
      }}
      const navLink = target.closest('.notice-row,.ongoing-row,.type-tab,.manual-menu a,.page-btn[href]');
      if (navLink) {{
        event.preventDefault();
        if (navLink.getAttribute('data-direct-navigation') === '1') {{
          if (!(await confirmDiscardLiteChanges())) return;
          setLiteFormDirty(false);
          window.location.assign(navLink.href);
          return;
        }}
        if (navLink.matches('.ongoing-row.optimistic')) {{
          setLiteStatus('该通告正在同步，完成后可继续处理');
          return;
        }}
        if (navLink.matches('.notice-row.is-disabled')) {{
          setLiteStatus(navLink.getAttribute('data-disabled-reason') || '该事项不可操作');
          return;
        }}
        if (!(await confirmDiscardLiteChanges())) return;
        setLiteFormDirty(false);
        closePickers();
        navLink.classList.add('is-loading');
        const isRow = navLink.matches('.notice-row,.ongoing-row');
        const isTypeTab = navLink.matches('.type-tab');
        if (isTypeTab) {{
          document.querySelectorAll('.type-tab').forEach(node => {{
            node.classList.toggle('active', node === navLink);
            node.setAttribute('aria-current', node === navLink ? 'page' : 'false');
          }});
        }}
        const detailNeedsReload = noticeDrawerOverlay()?.dataset.detailNeedsReload === '1';
        const appliedLocally = isRow && navLink.matches('.notice-row') && !detailNeedsReload && applySourceRowToDetail(navLink);
        const appliedOngoingLocally = isRow && navLink.matches('.ongoing-row') && !detailNeedsReload && applyOngoingRowToDetail(navLink);
        if (appliedLocally || appliedOngoingLocally) {{
          const selectingOngoing = navLink.matches('.ongoing-row');
          document.querySelectorAll('.notice-row').forEach(node => {{
            node.classList.toggle('active', !selectingOngoing && node === navLink);
            node.setAttribute('aria-current', !selectingOngoing && node === navLink ? 'true' : 'false');
          }});
          document.querySelectorAll('.ongoing-row').forEach(node => {{
            node.classList.toggle('active', selectingOngoing && node === navLink);
            node.setAttribute('aria-current', selectingOngoing && node === navLink ? 'true' : 'false');
          }});
          navLink.classList.remove('is-loading');
          history.replaceState({{ lite: true }}, '', navLink.href);
          return;
        }}
        const selectors = isRow ? ['.status', '#detail-panel'] : (isTypeTab ? ['#lite-workbench-subtitle', '.status', '.summary', '.toolbar', '.workbench-guide', '.workspace'] : undefined);
        const rowGroupSelector = navLink.matches('.notice-row') ? '.notice-row' : '.ongoing-row';
        try {{
          await navigateLite(navLink.href, {{
            label: isTypeTab ? '正在切换分类...' : '正在打开通告...',
            loadingText: isTypeTab ? '正在加载通告' : '正在打开通告',
            selectors,
            preserveWorkspaceScroll: isRow,
            workspaceSwitch: isTypeTab,
            useCache: isTypeTab
          }});
          if (isRow) {{
            document.querySelectorAll(rowGroupSelector).forEach(node => {{
              node.classList.toggle('active', node === navLink);
              node.setAttribute('aria-current', node === navLink ? 'true' : 'false');
            }});
            navLink.classList.remove('is-loading');
            openNoticeDrawer(navLink.getAttribute('data-title') || '', navLink);
          }}
        }}
        catch (error) {{
          navLink.classList.remove('is-loading');
          showLiteError(error && error.message ? error.message : '打开失败');
        }}
        if (isTypeTab) navLink.classList.remove('is-loading');
        return;
      }}
      const pageRefreshButton = target.closest('#lite-refresh-page');
      if (pageRefreshButton) {{
        if (!(await confirmDiscardLiteChanges())) return;
        setLiteFormDirty(false);
        setButtonBusy(pageRefreshButton, true);
        setPickerOpen('refresh-picker', 'refresh-open', false);
        try {{ await refreshCurrentLite('正在刷新本页...', ['.status', '.summary', '.workbench-guide', '.workspace']); }}
        catch (error) {{ showLiteError(error && error.message ? error.message : '刷新本页失败'); }}
        finally {{ setButtonBusy(pageRefreshButton, false); }}
        return;
      }}
      const repairButton = target.closest('#lite-refresh-repair');
      if (repairButton) {{
        if (!(await confirmDiscardLiteChanges())) return;
        setLiteFormDirty(false);
        setButtonBusy(repairButton, true);
        setPickerOpen('refresh-picker', 'refresh-open', false);
        try {{ await liteFetchThenRefresh('/api/repair-refresh?scope=' + encodeURIComponent(getCurrentScope()), '刷新检修'); }}
        catch (error) {{ showLiteError(error && error.message ? error.message : '刷新检修失败'); }}
        finally {{ setButtonBusy(repairButton, false); }}
        return;
      }}
      const changeButton = target.closest('#lite-refresh-change');
      if (changeButton) {{
        if (!(await confirmDiscardLiteChanges())) return;
        setLiteFormDirty(false);
        setButtonBusy(changeButton, true);
        setPickerOpen('refresh-picker', 'refresh-open', false);
        try {{ await liteFetchThenRefresh('/api/change-refresh?scope=' + encodeURIComponent(getCurrentScope()), '刷新变更'); }}
        catch (error) {{ showLiteError(error && error.message ? error.message : '刷新变更失败'); }}
        finally {{ setButtonBusy(changeButton, false); }}
        return;
      }}
      const undoButton = target.closest('.undo-row[data-undo-id]');
      if (undoButton) {{
        event.preventDefault();
        openUndoConfirm(undoButton);
        return;
      }}
      const ongoingDeleteButton = target.closest('button[data-ongoing-delete-mode]');
      if (ongoingDeleteButton) {{
        event.preventDefault();
        await deleteOngoingFromForm(ongoingDeleteButton);
        return;
      }}
      const endCancel = target.closest('#lite-end-check-cancel');
      if (endCancel) {{
        event.preventDefault();
        closeEndCheck();
        return;
      }}
      const endConfirm = target.closest('#lite-end-check-confirm');
      if (endConfirm) {{
        event.preventDefault();
        const form = document.getElementById('lite-notice-form');
        if (!form) return;
        form.dataset.endCheckApproved = '1';
        const submitter = pendingEndSubmitter;
        form.dataset.pendingSubmitAction = (submitter && submitter.value) ? submitter.value : 'end';
        closeEndCheck();
        if (submitter && typeof form.requestSubmit === 'function') form.requestSubmit(submitter);
        else form.requestSubmit();
        return;
      }}
      if (!target.closest('#manual-picker')) {{
        setPickerOpen('manual-picker', 'manual-open', false);
      }}
      if (!target.closest('#refresh-picker')) {{
        setPickerOpen('refresh-picker', 'refresh-open', false);
      }}
    }});
    function padLiteNumber(value) {{
      return String(value).padStart(2, '0');
    }}
    function currentLiteDatetimeLocal() {{
      const now = new Date();
      return [
        now.getFullYear(),
        padLiteNumber(now.getMonth() + 1),
        padLiteNumber(now.getDate()),
      ].join('-') + 'T' + [
        padLiteNumber(now.getHours()),
        padLiteNumber(now.getMinutes()),
      ].join(':');
    }}
    function resetActualActionTime(form) {{
      const field = form?.querySelector('[name="actual_action_time"]');
      if (!field) return;
      field.dataset.autoActualTime = '1';
      field.value = currentLiteDatetimeLocal();
    }}
    function ensureActualActionTime(form) {{
      const field = form?.querySelector('[name="actual_action_time"]');
      if (!field) return '';
      if (field.dataset.autoActualTime !== '0' || !field.value) {{
        field.value = currentLiteDatetimeLocal();
        field.dataset.autoActualTime = '1';
      }}
      return field.value;
    }}
    document.addEventListener('change', async (event) => {{
      if (event.target && event.target.id === 'lite-month-select') {{
        if (!(await confirmDiscardLiteChanges())) {{
          event.target.value = new URLSearchParams(location.search).get('month') || '{_e(selected_month)}';
          return;
        }}
        setLiteFormDirty(false);
        const filterForm = event.target.closest('form');
        const params = new URLSearchParams();
        if (filterForm) {{
          new FormData(filterForm).forEach((value, key) => {{
            if (String(value || '').trim()) params.set(key, String(value));
          }});
        }}
        params.set('month', event.target.value);
        params.delete('record_id');
        params.delete('active_item_id');
        params.delete('manual');
        params.delete('pending_page');
        const url = '/workbench-lite?' + params.toString();
        try {{
          await navigateLite(url, {{
            label: '正在切换月份...',
            selectors: ['.status', '.summary', '.toolbar', '.workbench-guide', '.workspace'],
          }});
        }} catch (error) {{
          location.href = url;
        }}
        return;
      }}
      if (event.target && event.target.id === 'lite-scope-select') {{
        if (!(await confirmDiscardLiteChanges())) {{
          event.target.value = currentUrlScope();
          return;
        }}
        setLiteFormDirty(false);
        const params = new URLSearchParams(location.search);
        params.set('scope', event.target.value);
        params.set('work_type', params.get('work_type') || '{_e(view_work)}');
        params.delete('record_id');
        params.delete('active_item_id');
        params.delete('manual');
        const url = '/workbench-lite?' + params.toString();
        try {{ await navigateLite(url, {{ label: '正在切换楼栋...', selectors: ['.topbar', '.status', '.summary', '.toolbar', '.workbench-guide', '.workspace'] }}); }}
        catch (error) {{ location.href = url; }}
        return;
      }}
      if (event.target && event.target.id === 'lite-site-photo-input') {{
        event.preventDefault();
        const form = event.target.closest('#lite-notice-form') || document.getElementById('lite-notice-form');
        await handleSitePhotoFiles(event.target.files, form);
        return;
      }}
      if (event.target && event.target.name === 'source_record_id' && event.target.closest('#lite-notice-form')) {{
        const form = event.target.closest('#lite-notice-form');
        const sourceId = event.target.value || '';
        setFormValue(form, 'record_id', sourceId);
        setFormValue(form, 'target_record_id', '');
        setSourceLinkDisplay(form, sourceId, sourceId ? '已选择' : '未关联');
        form.dataset.targetEnded = '';
        const status = document.getElementById('lite-target-link-status');
        if (status) status.textContent = '需确认';
      }}
      if (event.target && event.target.name === 'actual_action_time' && event.target.closest('#lite-notice-form')) {{
        event.target.dataset.autoActualTime = event.target.value ? '0' : '1';
      }}
      if (event.target && event.target.closest('#lite-notice-form')) {{
        setLiteFormDirty(true);
        updateNoticePreview(event.target.closest('#lite-notice-form'));
      }}
    }});
    document.addEventListener('dragover', (event) => {{
      const target = event.target instanceof Element ? event.target : null;
      const drop = sitePhotoDropFromTarget(target);
      if (!drop) return;
      event.preventDefault();
      drop.classList.add('dragover');
    }});
    document.addEventListener('dragleave', (event) => {{
      const target = event.target instanceof Element ? event.target : null;
      const drop = sitePhotoDropFromTarget(target);
      if (drop) drop.classList.remove('dragover');
    }});
    document.addEventListener('drop', async (event) => {{
      const target = event.target instanceof Element ? event.target : null;
      const drop = sitePhotoDropFromTarget(target);
      if (!drop) return;
      event.preventDefault();
      drop.classList.remove('dragover');
      const form = drop.closest('#lite-notice-form') || sitePhotoPanelFromTarget(target)?.closest('#lite-notice-form') || document.getElementById('lite-notice-form');
      await handleSitePhotoFiles(event.dataTransfer?.files, form);
    }});
    document.addEventListener('paste', (event) => {{
      handleSitePhotoPaste(event).catch(error => {{
        const message = error && error.message ? error.message : '粘贴现场照片失败';
        const status = document.getElementById('lite-site-photo-status');
        if (status) status.textContent = message;
        setLiteStatus(message);
      }});
    }});
    document.addEventListener('keydown', (event) => {{
      if (event.key === 'Escape') {{
        const hadOpenDialog = Array.from(document.querySelectorAll('.end-check-backdrop')).some(node => !node.hidden);
        closePickers();
        closeEndCheck();
        closeTargetCandidates();
        closeManualSourceCandidates();
        closeRepairEventCandidates();
        closeUndoConfirm();
        if (!hadOpenDialog) requestCloseNoticeDrawer().catch(() => null);
      }}
    }});
    window.addEventListener('popstate', () => {{
      navigateLite(location.href, {{ push: false, label: '正在恢复页面...' }}).catch(() => {{
        setLiteStatus('页面状态恢复失败，请点击刷新本页');
      }});
    }});
    function formPayload(form, submitter, actionOverride) {{
      const actualActionTime = ensureActualActionTime(form);
      const formValues = captureNoticeFormValues(form);
      const fd = new FormData(form);
      const action = actionOverride || (submitter && submitter.value ? submitter.value : (form.dataset.action || 'start'));
      const patch = Object.assign(Object.fromEntries(fd.entries()), formValues);
      const photos = sitePhotoPayload(form);
      const sourceRecordId = String(patch.source_record_id || '').trim();
      const repairManagementRecordId = String(
        patch.repair_management_record_id
        || (
          patch.work_type === 'repair' && sourceRecordId.startsWith('rec')
            ? sourceRecordId
            : ''
        )
      ).trim();
      const rawRecordId = String(patch.record_id || '').trim();
      const explicitTargetRecordId = String(patch.target_record_id || '').trim();
      let targetRecordId = explicitTargetRecordId;
      if (
        action !== 'start' &&
        !targetRecordId &&
        !isMissingTargetRecordId(rawRecordId) &&
        rawRecordId !== sourceRecordId
      ) {{
        targetRecordId = rawRecordId;
      }}
      const submitRecordId = action === 'start'
        ? (sourceRecordId || patch.manual_id || rawRecordId || '')
        : (targetRecordId || rawRecordId || '');
      const operationIdentity = action === 'start'
        ? (sourceRecordId || patch.manual_id || rawRecordId || patch.active_item_id || '')
        : (targetRecordId || patch.active_item_id || rawRecordId || '');
      patch.action = action;
      patch.manual = ['1', 'true', 'yes', 'on'].includes(String(patch.manual || '').trim().toLowerCase());
      patch.actual_action_time = actualActionTime || patch.actual_action_time || '';
      patch.response_time = patch.actual_action_time || patch.response_time || '';
      patch.source_record_id = sourceRecordId;
      patch.repair_management_record_id = repairManagementRecordId;
      patch.target_record_id = targetRecordId;
      patch.record_id = submitRecordId;
      patch.operation_id = `${{patch.scope}}:${{patch.work_type}}:${{operationIdentity}}:${{action}}:${{Date.now()}}`;
      if (patch.work_type === 'repair') {{
        patch.fault_time = patch.end_time || patch.fault_time || '';
        patch.expected_time = patch.start_time || patch.expected_time || '';
      }}
      delete patch.site_photos_json;
      if (photos.length) {{
        patch.site_photos = photos;
        patch.extra_images = photos;
      }}
      const commandPatch = compactCommandPatch(patch);
      return {{
        command_format: 'notice_command',
        action,
        scope: patch.scope,
        work_type: patch.work_type,
        notice_type: patch.notice_type,
        active_item_id: patch.active_item_id || '',
        source_record_id: sourceRecordId,
        target_record_id: targetRecordId,
        repair_management_record_id: repairManagementRecordId,
        record_id: submitRecordId,
        actual_action_time: patch.actual_action_time || '',
        operation_id: patch.operation_id,
        patch: commandPatch
      }};
    }}
    function rowIdentityCandidates(draft) {{
      return [
        draft.active_item_id,
        draft.target_record_id,
        draft.record_id,
        draft.source_record_id,
        draft.manual_id,
        draft.operation_id,
      ].map(value => String(value || '').trim()).filter(Boolean);
    }}
    function currentNoticeMatchesDraft(form, draft) {{
      if (!form || !draft) return false;
      const draftIds = new Set(rowIdentityCandidates(draft));
      if (!draftIds.size) return false;
      const formIds = [
        'active_item_id',
        'target_record_id',
        'record_id',
        'source_record_id',
        'manual_id',
      ].map(name => String(previewValue(form, name) || '').trim()).filter(Boolean);
      return formIds.some(value => draftIds.has(value));
    }}
    function clearCompletedCurrentNotice(draft) {{
      const form = document.getElementById('lite-notice-form');
      if (!currentNoticeMatchesDraft(form, draft)) return false;
      setLiteFormDirty(false);
      liteSitePhotos = [];
      liteSitePhotoSignature = '';
      document.querySelectorAll('.notice-row.active,.ongoing-row.active').forEach(row => {{
        row.classList.remove('active');
        row.setAttribute('aria-current', 'false');
      }});
      const empty = document.createElement('div');
      empty.id = 'lite-current-notice-empty';
      empty.className = 'current-notice-empty';
      empty.setAttribute('aria-label', '当前通告已结束');
      form.replaceWith(empty);
      const url = new URL(location.href);
      for (const key of ['active_item_id', 'record_id', 'manual', 'repair_management_record_id']) {{
        url.searchParams.delete(key);
      }}
      history.replaceState({{ lite: true }}, '', url.pathname + url.search + url.hash);
      closeEndCheck();
      closeNoticeDrawer({{ resetUrl: false }});
      return true;
    }}
    function findOngoingRowByDraft(draft) {{
      const candidates = rowIdentityCandidates(draft);
      if (!candidates.length) return null;
      return Array.from(document.querySelectorAll('.ongoing-row')).find(row => {{
        const rowValues = [
          row.getAttribute('data-active-item-id'),
          row.getAttribute('data-target-record-id'),
          row.getAttribute('data-record-id'),
          row.getAttribute('data-source-record-id'),
        ].map(value => String(value || '').trim()).filter(Boolean);
        return candidates.some(value => rowValues.includes(value));
      }}) || null;
    }}
    function ongoingListElements() {{
      const panel = document.querySelector('[data-ongoing-panel]');
      return {{
        panel,
        list: panel ? panel.querySelector('.list') : null,
        count: panel ? panel.querySelector('.panel-count') : null,
      }};
    }}
    function setPanelCountValue(countNode, value) {{
      if (!countNode) return;
      countNode.textContent = String(Math.max(0, Number(value) || 0));
    }}
    function adjustOngoingCount(delta) {{
      const {{ count }} = ongoingListElements();
      const current = Number(count?.textContent || 0);
      setPanelCountValue(count, current + delta);
      const inboxOngoingCount = document.querySelector('[data-inbox-ongoing-count]');
      setPanelCountValue(inboxOngoingCount, Number(inboxOngoingCount?.textContent || 0) + delta);
      document.querySelectorAll('.summary article').forEach(card => {{
        const label = card.querySelector('strong')?.textContent?.trim();
        if (label !== '进行中') return;
        const value = card.querySelector('b');
        setPanelCountValue(value, Number(value?.textContent || 0) + delta);
      }});
    }}
    function setMetaChip(parent, text, tone) {{
      if (!text) return;
      const chip = document.createElement('small');
      chip.className = 'meta-chip' + (tone ? ' ' + tone : '');
      chip.textContent = text;
      parent.appendChild(chip);
    }}
    function setOngoingRowStatus(row, text, tone) {{
      const status = row?.querySelector('.row-status');
      if (!status) return;
      status.className = 'row-status ' + (tone || 'working');
      status.textContent = text || '进行中';
    }}
    function ongoingDisplayTitle(draft) {{
      const rawTitle = String(draft?.title || '').trim();
      const technicalIds = [
        draft?.active_item_id,
        draft?.source_record_id,
        draft?.target_record_id,
        draft?.record_id,
      ].map(value => String(value || '').trim()).filter(Boolean);
      const exposesIdentity = technicalIds.some(value => rawTitle === value || rawTitle.endsWith(value));
      if (rawTitle && !exposesIdentity) return rawTitle;
      const labels = {{
        maintenance: '维保通告',
        change: '变更通告',
        repair: '设备检修',
        power: String(draft?.notice_type || '') === '下电通告' ? '下电通告' : '上电通告',
        polling: '设备轮巡',
        adjust: '设备调整',
      }};
      return '未命名' + (labels[String(draft?.work_type || '')] || '通告');
    }}
    function renderOngoingRow(row, draft, action) {{
      const title = ongoingDisplayTitle(draft);
      const isEnding = action === 'end';
      const status = isEnding ? '结束发送中' : '发送中';
      row.className = 'ongoing-row active optimistic';
      row.href = '#';
      row.title = title;
      row.setAttribute('aria-current', 'true');
      row.setAttribute('data-row-kind', 'ongoing');
      row.setAttribute('data-work-type', draft.work_type || '');
      row.setAttribute('data-active-item-id', draft.active_item_id || draft.operation_id || '');
      const optimisticTargetId = draft.target_record_id || (action === 'start' ? '' : draft.record_id || '');
      row.setAttribute('data-record-id', optimisticTargetId);
      row.setAttribute('data-target-record-id', optimisticTargetId);
      row.setAttribute('data-source-record-id', draft.source_record_id || '');
      row.setAttribute('data-site-photo-count', draft.site_photo_count || '0');
      row.setAttribute('data-mop-status', draft.mop_status || '');
      row.setAttribute('data-action', 'update');
      row.setAttribute('data-title', title);
      row.setAttribute('data-operation-id', draft.operation_id || '');
      setSafeDraftAttr(row, draft);
      const rowMain = document.createElement('span');
      rowMain.className = 'row-main';
      const strong = document.createElement('strong');
      strong.textContent = title;
      const badge = document.createElement('span');
      badge.className = 'row-status working';
      badge.textContent = status;
      rowMain.append(strong, badge);
      const meta = document.createElement('span');
      meta.className = 'row-meta';
      setMetaChip(meta, draft.building || draft.buildings || getCurrentScope());
      setMetaChip(meta, draft.specialty || '');
      if (draft.maintenance_cycle) setMetaChip(meta, '周期 ' + draft.maintenance_cycle, 'ready');
      const sitePhotoCount = Number(draft.site_photo_count || 0);
      if (['maintenance', 'change', 'repair'].includes(draft.work_type || '')) {{
        setMetaChip(meta, sitePhotoCount > 0 ? `现场图${{sitePhotoCount}}张` : '现场图未传', sitePhotoCount > 0 ? 'success' : 'warn');
      }}
      row.replaceChildren(rowMain, meta);
      return row;
    }}
    function removeSourceRowForDraft(draft) {{
      const sourceId = String(draft.source_record_id || draft.record_id || '').trim();
      if (!sourceId) return;
      const row = Array.from(document.querySelectorAll('.notice-row')).find(node =>
        [node.getAttribute('data-source-record-id'), node.getAttribute('data-record-id')]
          .map(value => String(value || '').trim())
          .includes(sourceId)
      );
      if (!row) return;
      row.classList.add('is-disabled');
      row.setAttribute('aria-disabled', 'true');
      row.setAttribute('data-disabled-reason', '已提交');
      row.querySelector('.row-status')?.replaceChildren(document.createTextNode('处理中'));
    }}
    function markSourceRowCompleted(draft) {{
      const sourceId = String(draft?.source_record_id || '').trim();
      if (!sourceId) return;
      const row = Array.from(document.querySelectorAll('.notice-row')).find(node =>
        [node.getAttribute('data-source-record-id'), node.getAttribute('data-record-id')]
          .map(value => String(value || '').trim())
          .includes(sourceId)
      );
      if (!row) return;
      row.classList.add('is-disabled');
      row.setAttribute('aria-disabled', 'true');
      row.setAttribute('data-disabled-reason', '该事项已结束，只保留查看状态，不可再次发起。');
      row.setAttribute('title', '该事项已结束，只保留查看状态，不可再次发起。');
      const status = row.querySelector('.row-status');
      if (status) {{
        status.className = 'row-status done';
        status.textContent = '已结束';
      }}
    }}
    function applyOptimisticSubmission(form, action, payload) {{
      const draft = compactOptimisticDraft(Object.assign({{}}, payload.patch || {{}}));
      draft.action = action;
      draft.operation_id = payload.operation_id || draft.operation_id || '';
      draft.active_item_id = payload.active_item_id || draft.active_item_id || '';
      draft.source_record_id = payload.source_record_id || draft.source_record_id || '';
      draft.target_record_id = payload.target_record_id || draft.target_record_id || '';
      draft.record_id = payload.record_id || draft.record_id || '';
      const row = findOngoingRowByDraft(draft);
      if (row) {{
        row.classList.add('optimistic');
        setOngoingRowStatus(row, action === 'end' ? '结束发送中' : '发送中', 'working');
      }} else if (action === 'start') {{
        const {{ list }} = ongoingListElements();
        if (list) {{
          const newRow = renderOngoingRow(document.createElement('a'), draft, action);
          const onlyEmpty = list.children.length === 1 && list.firstElementChild?.classList.contains('empty');
          if (onlyEmpty) list.replaceChildren(newRow);
          else list.prepend(newRow);
          adjustOngoingCount(1);
        }}
      }}
      if (action === 'start') {{
        removeSourceRowForDraft(draft);
        setLiteStatus('开始已提交，发送中');
      }} else if (action === 'end') {{
        setLiteStatus('结束已提交，发送中');
      }} else {{
        setLiteStatus('更新已提交，发送中');
      }}
      return true;
    }}
    function removeOptimisticSubmission(payload) {{
      const operationId = String(payload?.operation_id || '').trim();
      if (!operationId) return;
      document.querySelectorAll(`.ongoing-row.optimistic[data-operation-id="${{CSS.escape(operationId)}}"]`).forEach(row => {{
        const list = row.parentElement;
        row.remove();
        adjustOngoingCount(-1);
        if (list && !list.querySelector('.ongoing-row')) {{
          const empty = document.createElement('div');
          empty.className = 'empty';
          empty.textContent = '当前没有未结束通告';
          list.replaceChildren(empty);
        }}
      }});
    }}
    function markSubmissionResult(payload, ok, message) {{
      const draft = compactOptimisticDraft(Object.assign({{}}, payload?.patch || {{}}));
      draft.action = payload?.action || draft.action || '';
      draft.operation_id = payload?.operation_id || draft.operation_id || '';
      draft.active_item_id = payload?.active_item_id || draft.active_item_id || '';
      draft.source_record_id = payload?.source_record_id || draft.source_record_id || '';
      draft.target_record_id = payload?.target_record_id || draft.target_record_id || '';
      draft.record_id = payload?.record_id || draft.record_id || '';
      const row = findOngoingRowByDraft(draft);
      if (ok && draft.action === 'end') {{
        markSourceRowCompleted(draft);
        removeOngoingRow(row);
        clearCompletedCurrentNotice(draft);
        return;
      }}
      if (!row) return;
      row.classList.remove('optimistic');
      row.classList.toggle('failed', !ok);
      setOngoingRowStatus(row, ok ? '进行中' : '发送失败', ok ? 'working' : 'danger');
    }}
    function removeOngoingRow(row) {{
      if (!row) return;
      const list = row.parentElement;
      row.remove();
      adjustOngoingCount(-1);
      if (list && !list.querySelector('.ongoing-row')) {{
        const empty = document.createElement('div');
        empty.className = 'empty';
        empty.textContent = '当前没有未结束通告';
        list.replaceChildren(empty);
      }}
    }}
    function promoteSubmittedStartToOngoing(draft) {{
      if (!draft || draft.action !== 'start') return;
      const form = document.getElementById('lite-notice-form');
      if (!form) return;
      const currentSourceId = String(previewValue(form, 'source_record_id') || '').trim();
      const draftSourceId = String(draft.source_record_id || '').trim();
      if (currentSourceId && draftSourceId && currentSourceId !== draftSourceId) return;
      const targetRecordId = String(draft.target_record_id || draft.record_id || '').trim();
      if (isMissingTargetRecordId(targetRecordId)) return;
      form.dataset.action = 'update';
      form.dataset.detailMode = 'ongoing';
      form.dataset.targetEnded = '';
      setFormValue(form, 'record_id', targetRecordId);
      setFormValue(form, 'target_record_id', targetRecordId);
      setFormValue(form, 'active_item_id', draft.active_item_id || targetRecordId);
      if (draftSourceId) {{
        setFormValue(form, 'source_record_id', draftSourceId);
        setSourceLinkDisplay(form, draftSourceId, '已关联');
      }}
      setOngoingSubmitButtons(form);
      setLiteFormDirty(false);
    }}
    function applyJobPatch(jobPatch, payload, ok, message) {{
      const patch = jobPatch && typeof jobPatch === 'object' ? jobPatch : null;
      if (!patch || patch.kind !== 'notice_action_result') {{
        if (ok && payload?.action === 'start') {{
          applyOptimisticSubmission(document.getElementById('lite-notice-form'), 'start', payload);
        }}
        markSubmissionResult(payload, ok, message);
        return;
      }}
      const draft = compactOptimisticDraft(Object.assign({{}}, payload?.patch || {{}}, patch));
      draft.action = patch.action || payload?.action || draft.action || '';
      draft.operation_id = payload?.operation_id || draft.operation_id || '';
      draft.active_item_id = patch.active_item_id || payload?.active_item_id || draft.active_item_id || '';
      draft.source_record_id = patch.source_record_id || payload?.source_record_id || draft.source_record_id || '';
      draft.target_record_id = patch.target_record_id || payload?.target_record_id || draft.target_record_id || '';
      draft.record_id = patch.record_id || draft.target_record_id || payload?.record_id || draft.record_id || '';
      if (ok && draft.action === 'end') {{
        markSourceRowCompleted(draft);
        removeOngoingRow(findOngoingRowByDraft(draft));
        clearCompletedCurrentNotice(draft);
        return;
      }}
      let row = findOngoingRowByDraft(draft);
      if (!ok) {{
        if (!row) {{
          markSubmissionResult(payload, false, patch.target_selection_message || message || '发送失败');
          return;
        }}
        row.classList.remove('optimistic');
        row.classList.add('failed');
        setOngoingRowStatus(row, '发送失败', 'danger');
        return;
      }}
      if (!row && draft.action === 'start') {{
        const {{ list }} = ongoingListElements();
        if (list) {{
          row = renderOngoingRow(document.createElement('a'), draft, 'update');
          const onlyEmpty = list.children.length === 1 && list.firstElementChild?.classList.contains('empty');
          if (onlyEmpty) list.replaceChildren(row);
          else list.prepend(row);
          adjustOngoingCount(1);
        }}
      }}
      if (!row) return;
      row.classList.remove('optimistic', 'failed');
      row.setAttribute('data-active-item-id', draft.active_item_id || '');
      row.setAttribute('data-record-id', draft.record_id || draft.target_record_id || '');
      row.setAttribute('data-target-record-id', draft.target_record_id || draft.record_id || '');
      row.setAttribute('data-source-record-id', draft.source_record_id || '');
      row.setAttribute('data-site-photo-count', draft.site_photo_count || row.getAttribute('data-site-photo-count') || '0');
      row.setAttribute('data-operation-id', draft.operation_id || row.getAttribute('data-operation-id') || '');
      if (draft.title) {{
        const displayTitle = ongoingDisplayTitle(draft);
        row.setAttribute('data-title', displayTitle);
        row.title = displayTitle;
        const strong = row.querySelector('.row-main strong');
        if (strong) strong.textContent = displayTitle;
      }}
      setOngoingRowStatus(row, '进行中', 'working');
      setSafeDraftAttr(row, Object.assign({{}}, draftFromRow(row), draft));
      if (draft.action === 'start') {{
        promoteSubmittedStartToOngoing(draft);
        removeSourceRowForDraft(draft);
      }}
    }}
    function sleep(ms) {{
      return new Promise(resolve => setTimeout(resolve, ms));
    }}
    function nextBrowserTurn() {{
      return new Promise(resolve => requestAnimationFrame(() => setTimeout(resolve, 0)));
    }}
    function jobPhaseText(phase) {{
      const map = {{
        accepted: '已受理',
        queued: '排队中',
        sending_message: '正在发送个人消息',
        message_sent: '个人消息已发送',
        qt_queued: '等待展示',
        upload_waiting: '等待上传多维',
        uploading: '正在上传多维',
        undo_queued: '回退已排队',
        undoing_remote: '正在恢复多维记录',
        undoing_local: '正在恢复本地状态',
        success: '发送成功',
        failed: '发送失败'
      }};
      return map[String(phase || '')] || String(phase || '发送中');
    }}
    function successfulNoticeActionText(action) {{
      if (action === 'end') return '结束成功，已从进行中移除';
      if (action === 'update') return '更新成功，通告仍在进行中';
      return '发送成功，通告已进入进行中';
    }}
    async function pollSubmittedJob(jobId, label, payload) {{
      if (!jobId) return false;
      const startedAt = Date.now();
      let delay = 800;
      while (Date.now() - startedAt < 90000) {{
        await sleep(delay);
        try {{
          const response = await fetch(`/api/jobs/${{encodeURIComponent(jobId)}}`, {{ credentials: 'same-origin' }});
          const data = await response.json().catch(() => ({{}}));
          if (handleLiteAuthRequired(response, data)) return true;
          if (!response.ok || data.ok === false) throw new Error(data.error || '读取任务状态失败');
          const job = data.data || {{}};
          const phase = String(job.phase || '');
          if (phase === 'success') {{
            applyJobPatch(job.frontend_patch, payload, true, job.upload_message || '已完成');
            setLiteStatus(successfulNoticeActionText(payload?.action));
            return true;
          }}
          if (phase === 'failed') {{
            const message = job.error || job.upload_message || job.message_error || '发送失败';
            applyJobPatch(job.frontend_patch, payload, false, message);
            showLiteError(message);
            setLiteStatus('发送失败：' + message);
            setLiteStatus('发送失败：' + friendlyLiteMessage(message));
            return true;
          }}
          setLiteStatus('发送中：' + jobPhaseText(phase));
        }} catch (error) {{
          setLiteStatus('正在读取任务状态...');
        }}
        delay = Math.min(2600, Math.round(delay * 1.25));
      }}
      setLiteStatus('仍在处理，请稍后刷新本页查看结果');
      return false;
    }}
    function schedulePostSubmitRefresh(label, jobId, payload) {{
      if (jobId) {{
        pollSubmittedJob(jobId, label || '任务已完成，正在更新列表...', payload).catch(() => {{
          setLiteStatus('仍在处理，请稍后刷新本页');
        }});
        return;
      }}
      setTimeout(() => {{
        if (liteFormDirty) {{
          setLiteStatus('有新状态，当前正在编辑，暂不刷新列表');
          return;
        }}
        refreshCurrentLite('后台状态校准中...', ['.status', '.summary']).catch(() => null);
      }}, 8000);
    }}
    document.addEventListener('submit', async (event) => {{
      const pasteForm = event.target.closest('.paste-drawer form,.paste-box form');
      if (pasteForm) {{
        event.preventDefault();
        if (!(await confirmDiscardLiteChanges())) return;
        setLiteFormDirty(false);
        const submitter = event.submitter;
        if (submitter) submitter.disabled = true;
        try {{
          const response = await fetch(pasteForm.action, {{
            method: 'POST',
            body: new FormData(pasteForm),
            credentials: 'same-origin',
          }});
          const html = await response.text();
          if (handleLiteAuthRequired(response, null, html)) return;
          if (!response.ok) throw new Error(html || '解析失败');
          const nextDoc = new DOMParser().parseFromString(html, 'text/html');
          applyLiteDocument(nextDoc, canonicalLiteUrlFromDocument(nextDoc, location.href), true);
        }} catch (error) {{
          showLiteError(error && error.message ? error.message : '解析失败');
          if (submitter) submitter.disabled = false;
        }}
        return;
      }}
      const filterForm = event.target.closest('.toolbar form');
      if (filterForm) {{
        event.preventDefault();
        if (!(await confirmDiscardLiteChanges())) return;
        setLiteFormDirty(false);
        const url = filterForm.action + '?' + new URLSearchParams(new FormData(filterForm)).toString();
        try {{ await navigateLite(url, {{ label: '正在筛选...' }}); }}
        catch (error) {{ location.href = url; }}
        return;
      }}
      const form = event.target.closest('#lite-notice-form');
      if (!form) return;
      event.preventDefault();
      const submitter = event.submitter;
      const submitAction = form.dataset.pendingSubmitAction || (submitter && submitter.value ? submitter.value : (form.dataset.action || 'start'));
      delete form.dataset.pendingSubmitAction;
      if (submitAction === 'end' && form.dataset.endCheckApproved !== '1') {{
        openEndCheck(form, submitter);
        return;
      }}
      delete form.dataset.endCheckApproved;
      const bindingIssue = manualBindingIssue(form);
      if (bindingIssue) {{
        showLiteError(bindingIssue);
        setLiteStatus('提交失败：' + bindingIssue);
        updateActionAvailability(form);
        return;
      }}
      const missing = missingRequiredFields(form);
      if (missing.length) {{
        const message = '缺少' + missing.join('、') + '，暂不能发送';
        showLiteError(message);
        setLiteStatus('提交失败：' + message);
        updateActionAvailability(form);
        return;
      }}
      const durationIssue = noticeDurationIssue(form);
      if (durationIssue) {{
        showLiteError(durationIssue);
        setLiteStatus('提交失败：' + durationIssue);
        updateActionAvailability(form);
        return;
      }}
      let payload = null;
      try {{
        // Capture the exact visible values before yielding; a background patch may
        // replace the drawer while the browser processes the next frame.
        payload = formPayload(form, submitter, submitAction);
        setLiteFormDirty(false);
        clearLiteHtmlCache();
        setFormSubmitBusy(form, true);
        setLiteStatus('已提交，发送中');
        setLiteStatus('正在提交');
        await nextBrowserTurn();
        const response = await fetch('/api/workbench-actions', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify(payload),
        }});
        const data = await response.json().catch(() => ({{}}));
        if (handleLiteAuthRequired(response, data)) return;
        if (!response.ok || data.ok === false) throw new Error(data.error || '提交失败');
        const jobId = (data && data.job_id) || (data && data.data && data.data.job_id) || '';
        setLiteStatus(`后端已受理，正在排队发送和上传。任务号 ${{jobId}}`);
        schedulePostSubmitRefresh('任务已受理，正在更新列表...', jobId, payload);
      }} catch (error) {{
        setLiteFormDirty(true);
        if (payload) removeOptimisticSubmission(payload);
        const message = error && error.message ? error.message : '提交失败';
        showLiteError(message);
        setLiteStatus('提交失败：' + friendlyLiteMessage(message));
      }} finally {{
        setFormSubmitBusy(form, false);
      }}
    }});
    document.addEventListener('input', (event) => {{
      if (event.target && event.target.id === 'lite-manual-source-search') {{
        window.clearTimeout(liteManualSourceSearchTimer);
        liteManualSourceSearchTimer = window.setTimeout(() => {{
          loadManualSourceCandidates(event.target.value).catch(error => {{
            showLiteError(error && error.message ? error.message : '搜索计划通告失败');
          }});
        }}, 260);
        return;
      }}
      if (event.target && event.target.id === 'lite-repair-event-search') {{
        window.clearTimeout(liteRepairEventSearchTimer);
        liteRepairEventRequestSequence += 1;
        if (liteRepairEventRequestController) liteRepairEventRequestController.abort();
        liteRepairEventRequestController = null;
        liteRepairEventSearchTimer = window.setTimeout(() => {{
          loadRepairEventCandidates(event.target.value).catch(error => {{
            showLiteError(error && error.message ? error.message : '搜索事件转检修记录失败');
          }});
        }}, 260);
        return;
      }}
      if (event.target && event.target.closest('#lite-notice-form')) {{
        if (event.target.name === 'actual_action_time') {{
          event.target.dataset.autoActualTime = event.target.value ? '0' : '1';
        }}
        setLiteFormDirty(true);
        updateNoticePreview(event.target.closest('#lite-notice-form'));
      }}
    }});
    hydrateLitePreview();
  </script>
</body>
</html>"""

import re

from ..config import (
    ALI_LEVEL_HIGH,
    ALI_LEVEL_LOW,
    ALI_LEVEL_MEDIUM,
    ALI_LEVEL_ULTRA_LOW,
    EVENT_LEVEL_OPTIONS,
    EVENT_LEVEL_UPGRADE_I3_TO_I1,
    EVENT_LEVEL_UPGRADE_I3_TO_I2,
    LEVEL_E0,
    LEVEL_I1,
    LEVEL_I2,
    LEVEL_I3,
    OPTION_SLASH,
)
from ..core.parser import extract_event_info


_TITLE_PATTERNS = (
    re.compile(r"【标题】(.*?)(?=【|$)", re.DOTALL),
    re.compile(r"【名称】(.*?)(?=【|$)", re.DOTALL),
)
_SECTION_PATTERN_TEMPLATE = r"【{label}】(.*?)(?=【|$)"
_LEVEL_LOCK_NOTICE_TYPES = {"设备变更", "变更通告", "事件通告"}


def _clean_text(value) -> str:
    return str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def _recover_notice_text_from_log_preview(text: str) -> str:
    clean_text = _clean_text(text)
    if not clean_text:
        return ""
    if extract_event_info(clean_text):
        return clean_text
    for line in clean_text.splitlines():
        candidate_line = str(line or "").strip()
        if "text=【" not in candidate_line:
            continue
        candidate = candidate_line.split("text=", 1)[-1].strip()
        if extract_event_info(candidate):
            return candidate
    return clean_text


def _extract_section(text: str, label: str) -> str:
    clean_text = _clean_text(text)
    if not clean_text:
        return ""
    pattern = re.compile(
        _SECTION_PATTERN_TEMPLATE.format(label=re.escape(label)),
        re.DOTALL,
    )
    match = pattern.search(clean_text)
    if not match:
        return ""
    return str(match.group(1) or "").strip()


def _fallback_title_from_text(text: str) -> str:
    clean_text = _clean_text(text)
    for pattern in _TITLE_PATTERNS:
        match = pattern.search(clean_text)
        if match:
            title = re.sub(r"\s+", " ", str(match.group(1) or "")).strip()
            if title:
                return title
    first_line = next((line.strip() for line in clean_text.splitlines() if line.strip()), "")
    return first_line


def _build_preview_text(text: str, limit: int = 40) -> str:
    preview = re.sub(r"\s+", " ", _clean_text(text))
    if len(preview) <= limit:
        return preview
    return preview[:limit] + "..."


def notice_supports_level_lock(notice_type: str) -> bool:
    return str(notice_type or "").strip() in _LEVEL_LOCK_NOTICE_TYPES


def detect_level_from_notice_text(notice_type: str, text: str) -> str:
    notice_type = str(notice_type or "").strip()
    raw_text = _clean_text(text)
    if not raw_text or not notice_supports_level_lock(notice_type):
        return ""

    if notice_type in ("设备变更", "变更通告"):
        raw = _extract_section(raw_text, "等级")
        if ALI_LEVEL_ULTRA_LOW in raw:
            return LEVEL_I3
        if ALI_LEVEL_LOW in raw:
            return LEVEL_I3
        if ALI_LEVEL_MEDIUM in raw:
            return LEVEL_I2
        if ALI_LEVEL_HIGH in raw:
            return LEVEL_I1
        return ""

    title = _extract_section(raw_text, "标题") or raw_text
    title_upper = title.upper()
    if re.search(rf"{LEVEL_I3}\s*[→>\\-]\s*{LEVEL_I2}", title_upper):
        return EVENT_LEVEL_UPGRADE_I3_TO_I2
    if re.search(rf"{LEVEL_I3}\s*[→>\\-]\s*{LEVEL_I1}", title_upper):
        return EVENT_LEVEL_UPGRADE_I3_TO_I1
    for option in EVENT_LEVEL_OPTIONS:
        if option == OPTION_SLASH:
            continue
        if option in title_upper:
            return option
    return ""


def normalize_active_item_data(data_dict: dict | None) -> dict:
    if not isinstance(data_dict, dict):
        return {}

    normalized = dict(data_dict)
    normalized.pop("need_upload_first", None)
    raw_text = _recover_notice_text_from_log_preview(normalized.get("text"))
    normalized["text"] = raw_text
    normalized.pop("title", None)

    if not raw_text:
        return normalized

    info = extract_event_info(raw_text) or {}
    cleaned_text = _clean_text(info.get("content") or raw_text)
    normalized["text"] = cleaned_text

    notice_type = str(info.get("notice_type") or "").strip()
    if notice_type:
        normalized["notice_type"] = notice_type

    level_locked = bool(normalized.get("level_locked"))
    if level_locked:
        normalized["level_locked"] = True
        locked_level = str(normalized.get("level") or "").strip()
        if locked_level:
            normalized["level"] = locked_level
        else:
            normalized.pop("level", None)
    else:
        normalized.pop("level_locked", None)
        level = info.get("level")
        if level:
            normalized["level"] = level

    source = str(info.get("source") or "").strip()
    if source:
        normalized["source"] = source
    else:
        normalized.pop("source", None)

    time_str = str(info.get("time_str") or "").strip()
    if time_str:
        normalized["time_str"] = time_str
    else:
        normalized.pop("time_str", None)

    binding_state = str(normalized.get("record_binding_state") or "").strip().lower()
    if normalized.get("_is_placeholder_record"):
        normalized["record_binding_state"] = "placeholder"
        normalized.pop("record_binding_error", None)
    elif binding_state == "conflicted":
        normalized["record_binding_state"] = "conflicted"
        binding_error = str(normalized.get("record_binding_error") or "").strip()
        normalized["record_binding_error"] = binding_error or "Record ID 冲突"
    elif str(normalized.get("record_id") or "").strip():
        normalized["record_binding_state"] = "bound"
        normalized.pop("record_binding_error", None)
    else:
        normalized["record_binding_state"] = "placeholder"
        normalized.pop("record_binding_error", None)

    return normalized


def _build_record_binding_subtitle(normalized: dict) -> str:
    binding_error = str(normalized.get("record_binding_error") or "").strip()
    if not binding_error:
        return "Record ID 冲突"
    if "多维记录不存在" in binding_error or "已失效" in binding_error:
        return "多维中记录不存在"
    return "Record ID 冲突"


def build_notice_display_snapshot(data_dict: dict | None) -> dict:
    normalized = normalize_active_item_data(data_dict)
    text = normalized.get("text", "")
    info = extract_event_info(text) or {}

    title = str(info.get("title") or "").strip()
    if not title:
        title = _fallback_title_from_text(text)
    title = re.sub(r"\s+", " ", title).strip()

    notice_type = str(info.get("notice_type") or normalized.get("notice_type") or "").strip()
    source = str(info.get("source") or normalized.get("source") or "").strip()
    time_str = str(info.get("time_str") or normalized.get("time_str") or "").strip()

    if notice_type == "事件通告":
        subtitle_parts = [part for part in (source, time_str) if part]
        subtitle = " | ".join(subtitle_parts) if subtitle_parts else _build_preview_text(text, 40)
    else:
        subtitle = _build_preview_text(text, 40)

    if normalized.get("record_binding_state") == "conflicted":
        conflict_subtitle = _build_record_binding_subtitle(normalized)
        subtitle = f"{subtitle} | {conflict_subtitle}" if subtitle else conflict_subtitle

    if str(normalized.get("routing_state") or "").strip().lower() == "conflicted":
        subtitle = f"{subtitle} | 条目路由冲突" if subtitle else "条目路由冲突"

    if normalized.get("lan_created_from_portal"):
        subtitle = f"{subtitle} | 网页工作台" if subtitle else "网页工作台"

    return {
        "title": title,
        "subtitle": subtitle,
        "text": text,
        "notice_type": notice_type,
        "source": source,
        "time_str": time_str,
    }


"""Cross-building notice-history recommendations (read-only, local only)."""
from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from .planned_notices import STABLE, TYPES, _error

# Source buildings that may contribute reusable history; the target building
# itself is excluded in the loop.
_CROSS_BUILDING_CODES = ("A", "B", "C", "D", "E", "H")

# Fields always considered site-specific so the user confirms them on site.
_SITE_CONFIRM_FIELDS = ("location", "execution_party")

# Markers / patterns for content that cannot move across buildings.  If any hit
# the whole field is dropped: device/room numbers, personnel / signatures,
# internal IDs, times, on-site status, mixed building references.
_UNPORTABLE_MARKERS = (
    "柜号", "机柜", "机房编号", "设备编号", "设备id", "目标id",
    "target_record_id", "target_id", "recordid", "record_id",
    "openid", "open_id", "签名", "签字", "负责人", "操作人", "经办人", "指派",
    "联系电话", "手机号", "手机号码", "现场", "进度", "已完成", "进行中", "未开始",
)
_UNPORTABLE_PATTERNS = (
    r"\d{4}-\d{1,2}-\d{1,2}",
    r"\d{1,2}:\d{2}",
    r"\d+\s*号\s*(?:机组|主机|机柜|柜|设备|机房)",
    r"(?<![A-Za-z0-9])EA\d+",
    r"(?<![A-Za-z0-9])[A-Z]{1,3}(?:-[\dA-Z]+)+(?![A-Za-z0-9])",
    r"(?<![A-Za-z0-9])[A-Z]{2}\d{4,}(?![A-Za-z0-9])",
    r"(?:ou_|on_|rec)[A-Za-z0-9]{8,}",
    r"\d{2,4}\s*(?:机房|房间|冷机|主机|机组)",
    r"\d+\s*#\s*[^\s,，。;；:：]+",
    r"\bopenid\b|\bopen_id\b|\brecord_id\b|\brecordid\b|\bsignature\b",
)
_MEMORY_NAMESPACE = "notice_memory"
_READONLY_ERROR_TYPES = (OSError, ValueError, sqlite3.Error)


def _canonical_memory_name(service, item: dict[str, Any], work_type: str) -> str:
    value = str(
        item.get("memory_name")
        or item.get("maintenance_total")
        or item.get("converted_title")
        or ""
    ).strip()
    if not value:
        return ""
    return _comparison_title(service, value, work_type)


def _comparison_title(service, value, work_type):
    # Strip only the site/building prefix, never a device identifier in the title.
    value = re.sub(r'^\s*EA118[\s_-]*(?:C01[\s_-]*)?(?:机房|数据中心|基地)?[\s_-]*', '', str(value), flags=re.I)
    value = re.sub(r'^(?:南通基地|南通)?\s*[ABCDEH](?:楼|栋)[\s_-]*', '', value, flags=re.I)
    return service._canonical_history_notice_title(value, work_type)


def _to_success_order(item: dict[str, Any]) -> float:
    try:
        return float(item.get("success_order") or 0)
    except (TypeError, ValueError):
        return 0.0


def _is_valid_success(item: dict[str, Any]) -> bool:
    return isinstance(item, dict) and _to_success_order(item) > 0


def _read_building_memory(service, building: str) -> dict[str, Any]:
    """Read building memory without writing a state_store migration (read-only).

    Prefers the already-persisted document; falls back to a read-only JSON file
    parse.  Never calls ``put_document``.
    """
    key = service._building_memory_key(building)
    try:
        stored = service._state_store.get_document(_MEMORY_NAMESPACE, key)
        if isinstance(stored, dict):
            payload = dict(stored)
            if not isinstance(payload.get("items"), dict):
                payload["items"] = {}
            payload["building"] = str(payload.get("building") or building or "")
            return payload
    except _READONLY_ERROR_TYPES:
        return {"building": building, "items": {}, "_read_error": "本地记忆读取失败"}
    try:
        path = service._building_memory_path(building)
        with path.open("r", encoding="utf-8") as fp:
            payload = json.load(fp)
        if not isinstance(payload, dict):
            return {"building": building, "items": {}}
        items = payload.get("items")
        if not isinstance(items, dict):
            payload["items"] = {}
        return payload
    except _READONLY_ERROR_TYPES:
        return {"building": building, "items": {}}


def _contains_unportable(text: str) -> bool:
    text = str(text or "").strip()
    if not text:
        return False
    lower = text.lower()
    for marker in _UNPORTABLE_MARKERS:
        if marker in lower:
            return True
    for pattern in _UNPORTABLE_PATTERNS:
        if re.search(pattern, text):
            return True
    # Reject mixed-building fields instead of rewriting every label to the target.
    labels = re.findall(r"(?:110站|[ABCDEH]楼)", text)
    if len(set(labels)) > 1:
        return True
    return False


def _replace_buildings(text: str, target_label: str) -> str:
    target_label = str(target_label or "").strip()
    result = str(text or "")
    for code in ("110", "A", "B", "C", "D", "E", "H"):
        label = "110站" if code == "110" else f"{code}楼"
        if label == target_label:
            continue
        result = result.replace(label, target_label)
    return result


def _record_memory_name(service, record: dict[str, Any], work_type: str) -> str:
    fields = record.get("display_fields") or {}
    if work_type == "change":
        return str(service._change_title(record) or "").strip()
    if work_type == "repair":
        return str(service._repair_title(record) or "").strip()
    if work_type in ("power", "polling", "adjust"):
        return str(
            record.get("title")
            or fields.get("名称")
            or fields.get("标题")
            or fields.get("维护总项")
            or ""
        ).strip()
    return str(fields.get("维护总项") or "").strip()


def _record_memory_cycle(service, record: dict[str, Any], work_type: str) -> str:
    fields = record.get("display_fields") or {}
    return str(fields.get("维护周期") or fields.get("维保周期") or record.get("maintenance_cycle") or "").strip()


def _matches_item(
    service,
    item: dict[str, Any],
    *,
    work_type: str,
    target_canonical: str,
    target_cycle: str,
) -> bool:
    if str(item.get("work_type") or "").strip() != work_type:
        return False
    source_canonical = _canonical_memory_name(service, item, work_type)
    if not source_canonical or source_canonical != target_canonical:
        return False
    source_cycle = str(item.get("maintenance_cycle") or "").strip()
    if service._normalize_memory_cycle(source_cycle) != service._normalize_memory_cycle(target_cycle):
        return False
    return True


def _suggest_fields(
    service,
    item: dict[str, Any],
    *,
    work_type: str,
    target_label: str,
    target_filled: set[str],
) -> tuple[dict[str, str], list[str]]:
    suggested: dict[str, str] = {}
    confirmation: list[str] = []
    for field in STABLE.get(work_type, set()):
        if field in target_filled:
            continue
        if field == "maintenance_cycle":
            continue
        value = str(item.get(field) or "").strip()
        if not value:
            continue
        if _contains_unportable(value):
            continue
        rewritten = _replace_buildings(value, target_label)
        if not rewritten.strip():
            continue
        suggested[field] = rewritten
        if field in _SITE_CONFIRM_FIELDS:
            confirmation.append(field)
    return suggested, confirmation


def _target_filled(service, record, draft, work_type: str, target_label: str) -> set[str]:
    if draft is not None:
        return {
            key
            for key, value in draft.items()
            if value not in (None, "", [])
        }
    from .workbench_lite import _draft_from_record

    filled = {
        key
        for key, value in _draft_from_record(record, work_type=work_type).items()
        if value not in (None, "", [])
    }
    payload = _read_building_memory(service, target_label)
    target_name = _record_memory_name(service, record, work_type)
    target_key = service._memory_item_key(
        work_type, target_name, _record_memory_cycle(service, record, work_type)
    )
    item = (payload.get("items") or {}).get(target_key) or {}
    for key in STABLE.get(work_type, set()):
        if str(item.get(key) or "").strip():
            filled.add(key)
    return filled


def build_recommendations(
    service,
    *,
    record: dict[str, Any],
    draft: dict[str, Any] | None = None,
    scope: str,
    month: str,
    work_type: str,
    target_label: str | None = None,
    limit: int = 3,
) -> dict[str, Any]:
    """Pure recommendation core.  Uses an existing record/draft, never prefill_record."""
    if target_label is None:
        target_label = service._building_label_from_codes([service._normalize_scope(scope)])
    target_name = _record_memory_name(service, record, work_type)
    target_cycle = _record_memory_cycle(service, record, work_type)
    target_canonical = _comparison_title(service, target_name, work_type)
    if not target_canonical:
        return {"total": 0, "recommendations": []}

    target_filled = _target_filled(service, record, draft, work_type, target_label)
    limit = max(1, min(3, int(limit or 3)))
    candidates: list[dict[str, Any]] = []
    notice = ""
    for code in _CROSS_BUILDING_CODES:
        source_label = service._building_label_from_code(code)
        if source_label == target_label:
            continue
        payload = _read_building_memory(service, source_label)
        notice = notice or str(payload.get("_read_error") or "")
        for item in (payload.get("items") or {}).values():
            if not _is_valid_success(item):
                continue
            if not _matches_item(
                service,
                item,
                work_type=work_type,
                target_canonical=target_canonical,
                target_cycle=target_cycle,
            ):
                continue
            suggested, confirmation = _suggest_fields(
                service,
                item,
                work_type=work_type,
                target_label=target_label,
                target_filled=target_filled,
            )
            if not suggested:
                continue
            source_name = str(
                item.get("memory_name") or item.get("maintenance_total") or ""
            ).strip()
            candidates.append(
                {
                    "source_building": source_label,
                    "source_title": source_name,
                    "work_type": work_type,
                    "source_updated_at": str(item.get("updated_at") or ""),
                    "needs_site_confirmation": confirmation,
                    "suggested_values": suggested,
                    "_success_order": _to_success_order(item),
                }
            )

    candidates.sort(key=lambda row: row["_success_order"], reverse=True)
    results = [
        {
            "source_building": row["source_building"],
            "source_title": row["source_title"],
            "work_type": row["work_type"],
            "source_updated_at": row["source_updated_at"],
            "needs_site_confirmation": row["needs_site_confirmation"],
            "suggested_values": row["suggested_values"],
        }
        for row in candidates[:limit]
    ]
    result = {"total": len(results), "recommendations": results}
    if notice:
        result["notice"] = "本地记忆读取异常，本次部分楼栋未参与跨楼建议。"
    return result


def cross_building_recommendations(
    service,
    *,
    scope: str,
    month: str,
    work_type: str,
    source_record_id: str,
    limit: int = 3,
) -> dict[str, Any]:
    """Standalone cross-building recommendation entry (read-only, local only)."""
    if work_type not in TYPES:
        _error("此入口仅支持非事件计划通告。")
    records = service._workbench_records(scope=scope, month=month)
    record = next(
        (
            row
            for row in records
            if row.get("record_id") == source_record_id
            and service._record_work_type(row) == work_type
        ),
        None,
    )
    if record is None:
        _error("计划不在本次楼栋和月份内，请重新匹配。")
    payload = build_recommendations(
        service,
        record=record,
        draft=None,
        scope=scope,
        month=month,
        work_type=work_type,
        limit=limit,
    )
    return {
        "total": payload["total"],
        "recommendations": payload["recommendations"],
        **( {"notice": payload["notice"]} if payload.get("notice") else {}),
    }

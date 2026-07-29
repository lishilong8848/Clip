# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime
import re
from typing import Any


_MONTH_TOKEN_PATTERN = re.compile(
    r"(?<!\d)(?P<year>20\d{2})\s*(?:年|[-/.])\s*"
    r"(?P<month>0?[1-9]|1[0-2])(?=\s*(?:月|[-/.T\s]|$))"
)
_MONTH_ONLY_PATTERN = re.compile(r"(?<!\d)(?P<month>0?[1-9]|1[0-2])\s*月")
_NOTICE_DATE_LABEL_PATTERN = re.compile(
    r"【(?:时间|开始时间|结束时间|计划开始时间|计划结束时间|实际开始时间|"
    r"实际结束时间|发现故障时间|期望完成时间|故障发生时间|事件发生时间|"
    r"事件恢复时间|事件结束时间|响应时间|进展更新时间)】\s*([^【\r\n]+)"
)
_NOTICE_DATE_KEYS = {
    "time",
    "time_str",
    "start_time",
    "end_time",
    "plan_start_time",
    "plan_end_time",
    "planned_start_time",
    "planned_end_time",
    "actual_start_time",
    "actual_end_time",
    "fault_time",
    "expected_time",
    "discovery_time",
    "event_time",
    "event_occurrence_time",
    "response_time",
    "recovery_time",
    "close_time",
    "event_end_time",
    "started_at",
    "ended_at",
    "last_action_at",
    "last_remote_write_at",
    "actual_send_time",
    "时间",
    "开始时间",
    "结束时间",
    "计划开始时间",
    "计划结束时间",
    "实际开始时间",
    "实际结束时间",
    "发现故障时间",
    "期望完成时间",
    "故障发生时间",
    "事件发生时间",
    "事件恢复时间",
    "事件结束时间",
    "响应时间",
    "进展更新时间",
}
_NOTICE_ACTIVITY_DATE_KEYS = {
    "created_at",
    "updated_at",
    "last_updated_at",
}
_NOTICE_DATE_CONTAINER_KEYS = {
    "actions",
    "history",
    "notice_actions",
    "fields",
    "form",
    "payload",
    "prepared",
}
_NOTICE_RANGE_KEY_PAIRS = (
    ("start_time", "end_time"),
    ("plan_start_time", "plan_end_time"),
    ("planned_start_time", "planned_end_time"),
    ("actual_start_time", "actual_end_time"),
    ("fault_time", "expected_time"),
    ("开始时间", "结束时间"),
    ("计划开始时间", "计划结束时间"),
    ("实际开始时间", "实际结束时间"),
    ("发现故障时间", "期望完成时间"),
)


def text(value: Any) -> str:
    return str(value or "").strip()


def current_local_month_key(now: date | datetime | None = None) -> str:
    current = now or datetime.now()
    return f"{current.year:04d}-{current.month:02d}"


def _month_ordinal(month_key: str) -> int | None:
    match = re.fullmatch(r"(20\d{2})-(0[1-9]|1[0-2])", text(month_key))
    if not match:
        return None
    return int(match.group(1)) * 12 + int(match.group(2))


def _month_keys_from_value(value: Any) -> list[str]:
    if isinstance(value, datetime):
        return [f"{value.year:04d}-{value.month:02d}"]
    if isinstance(value, date):
        return [f"{value.year:04d}-{value.month:02d}"]
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        timestamp = float(value)
        absolute = abs(timestamp)
        if 1_000_000_000_000 <= absolute < 100_000_000_000_000:
            timestamp /= 1000.0
        if 1_000_000_000 <= abs(timestamp) < 100_000_000_000:
            try:
                parsed = datetime.fromtimestamp(timestamp)
                return [f"{parsed.year:04d}-{parsed.month:02d}"]
            except (OSError, OverflowError, ValueError):
                return []
        return []
    if isinstance(value, (list, tuple, set)):
        months: list[str] = []
        for item in value:
            months.extend(_month_keys_from_value(item))
        return months
    if isinstance(value, dict):
        months: list[str] = []
        for item in value.values():
            months.extend(_month_keys_from_value(item))
        return months
    raw = text(value)
    if not raw:
        return []
    return [
        f"{int(match.group('year')):04d}-{int(match.group('month')):02d}"
        for match in _MONTH_TOKEN_PATTERN.finditer(raw)
    ]


def _append_month_range(
    intervals: list[tuple[int, int]],
    start_value: Any,
    end_value: Any,
) -> None:
    start_months = _month_keys_from_value(start_value)
    end_months = _month_keys_from_value(end_value)
    if not start_months or not end_months:
        return
    start_ordinal = _month_ordinal(start_months[0])
    end_ordinal = _month_ordinal(end_months[-1])
    if start_ordinal is None or end_ordinal is None:
        return
    intervals.append(
        (min(start_ordinal, end_ordinal), max(start_ordinal, end_ordinal))
    )


def _collect_notice_months(
    mapping: dict[str, Any],
    *,
    months: set[str],
    intervals: list[tuple[int, int]],
    depth: int = 0,
    activity_container: bool = False,
) -> None:
    if depth > 3:
        return
    for start_key, end_key in _NOTICE_RANGE_KEY_PAIRS:
        if mapping.get(start_key) not in (None, "") and mapping.get(end_key) not in (
            None,
            "",
        ):
            _append_month_range(
                intervals,
                mapping.get(start_key),
                mapping.get(end_key),
            )
    for key, value in mapping.items():
        key_text = str(key or "").strip()
        if key_text in _NOTICE_DATE_KEYS or (
            activity_container and key_text in _NOTICE_ACTIVITY_DATE_KEYS
        ):
            value_months = _month_keys_from_value(value)
            months.update(value_months)
            if key_text in {"time", "time_str", "时间"} and len(value_months) >= 2:
                start_ordinal = _month_ordinal(value_months[0])
                end_ordinal = _month_ordinal(value_months[-1])
                if start_ordinal is not None and end_ordinal is not None:
                    intervals.append(
                        (
                            min(start_ordinal, end_ordinal),
                            max(start_ordinal, end_ordinal),
                        )
                    )
        if key_text not in _NOTICE_DATE_CONTAINER_KEYS:
            continue
        if isinstance(value, dict):
            _collect_notice_months(
                value,
                months=months,
                intervals=intervals,
                depth=depth + 1,
                activity_container=key_text
                in {"actions", "history", "notice_actions"},
            )
        elif isinstance(value, (list, tuple)):
            for item in value:
                if isinstance(item, dict):
                    _collect_notice_months(
                        item,
                        months=months,
                        intervals=intervals,
                        depth=depth + 1,
                        activity_container=key_text
                        in {"actions", "history", "notice_actions"},
                    )


def notice_payload_matches_month(
    payload: dict[str, Any] | None,
    *,
    month_key: str = "",
    now: date | datetime | None = None,
) -> bool:
    """Return whether an active notice belongs to the requested display month.

    Unknown/undated payloads stay visible so a malformed active item is not
    silently lost. A cross-month time range or a current-month action keeps the
    notice visible even when its original start time is older.
    """

    if not isinstance(payload, dict):
        return True
    target_month = text(month_key) or current_local_month_key(now)
    target_ordinal = _month_ordinal(target_month)
    if target_ordinal is None:
        target_month = current_local_month_key(now)
        target_ordinal = _month_ordinal(target_month)

    months: set[str] = set()
    intervals: list[tuple[int, int]] = []
    _collect_notice_months(payload, months=months, intervals=intervals)

    notice_text = text(payload.get("text"))
    for match in _NOTICE_DATE_LABEL_PATTERN.finditer(notice_text):
        value_months = _month_keys_from_value(match.group(1))
        months.update(value_months)
        if len(value_months) >= 2:
            start_ordinal = _month_ordinal(value_months[0])
            end_ordinal = _month_ordinal(value_months[-1])
            if start_ordinal is not None and end_ordinal is not None:
                intervals.append(
                    (
                        min(start_ordinal, end_ordinal),
                        max(start_ordinal, end_ordinal),
                    )
                )

    if target_month in months:
        return True
    if target_ordinal is not None and any(
        start <= target_ordinal <= end for start, end in intervals
    ):
        return True
    if months:
        return False

    plan_month = text(payload.get("plan_month") or payload.get("计划维护月份"))
    month_only_match = _MONTH_ONLY_PATTERN.search(plan_month)
    if month_only_match:
        return int(month_only_match.group("month")) == int(target_month.split("-")[1])
    return True


def is_local_record_id(record_id: str) -> bool:
    record_id = text(record_id)
    if not record_id:
        return True
    if re.fullmatch(r"[0-9a-fA-F]{32}", record_id):
        return True
    if "|" in record_id:
        return True
    if re.search(r"[\u4e00-\u9fff]", record_id):
        return True
    return (
        record_id.startswith("local_")
        or record_id.startswith("localid")
        or record_id.startswith("placeholder-")
        or record_id.startswith("manual:")
        or record_id.startswith("draft:")
    )


def normalize_notice_identity_payload(
    payload: dict[str, Any] | None,
    *,
    action: str = "",
) -> dict[str, Any]:
    """Normalize notice IDs into explicit source/target identities.

    Canonical rule:
    - source_record_id identifies the source table row.
    - target_record_id identifies the target bitable row used for update/end/delete.
    - record_id is not a canonical identity. It may remain in payloads for UI
      labels, but business code must not infer source/target identity from it.
    """

    if not isinstance(payload, dict):
        return {}
    normalized = dict(payload)
    source_record_id = text(normalized.get("source_record_id"))
    target_record_id = text(normalized.get("target_record_id"))
    if is_local_record_id(source_record_id):
        source_record_id = ""
    if is_local_record_id(target_record_id):
        target_record_id = ""
    if source_record_id:
        normalized["source_record_id"] = source_record_id
    else:
        normalized.pop("source_record_id", None)
    if target_record_id:
        normalized["target_record_id"] = target_record_id
    else:
        normalized.pop("target_record_id", None)
    return normalized


def canonical_target_record_id(payload: dict[str, Any] | None) -> str:
    return text(normalize_notice_identity_payload(payload).get("target_record_id"))


def canonical_source_record_id(payload: dict[str, Any] | None) -> str:
    return text(normalize_notice_identity_payload(payload).get("source_record_id"))

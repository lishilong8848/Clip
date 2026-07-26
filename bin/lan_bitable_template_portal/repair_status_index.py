# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
from typing import Any, Callable


REPAIR_PROJECT_STATUS_INDEX_VERSION = 2
REPAIR_COMPLETION_FIELD_NAMES = (
    "维修结束时间（2026）",
    "维修结束时间",
    "实际结束时间",
)


def build_repair_status_source_signature(
    source_state: list[dict[str, Any]],
) -> str:
    """Version the derived index independently from its source snapshots."""

    encoded = json.dumps(
        {
            "index_version": REPAIR_PROJECT_STATUS_INDEX_VERSION,
            "sources": source_state,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def repair_completed_at_seconds(
    record: dict[str, Any],
    *,
    is_completed: bool,
    parse_datetime_ms: Callable[[Any], int | None],
) -> float:
    """Return the explicit repair completion timestamp, never an edit-time fallback."""

    if not is_completed:
        return 0.0
    containers = (
        record.get("raw_fields"),
        record.get("display_fields"),
    )
    for field_name in REPAIR_COMPLETION_FIELD_NAMES:
        for container in containers:
            if not isinstance(container, dict):
                continue
            parsed_ms = parse_datetime_ms(container.get(field_name))
            if parsed_ms is not None and parsed_ms > 0:
                return float(parsed_ms) / 1000.0
    return 0.0

# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from typing import Any


def text(value: Any) -> str:
    return str(value or "").strip()


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

# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any


def text(value: Any) -> str:
    return str(value or "").strip()


def is_local_record_id(record_id: str) -> bool:
    record_id = text(record_id)
    return (
        not record_id
        or record_id.startswith("local_")
        or record_id.startswith("manual:")
        or record_id.startswith("draft:")
    )


def normalize_notice_identity_payload(
    payload: dict[str, Any] | None,
    *,
    action: str = "",
) -> dict[str, Any]:
    """Normalize legacy notice IDs into explicit source/target identities.

    Canonical rule:
    - source_record_id identifies the source table row.
    - target_record_id identifies the target bitable row used for update/end/delete.
    - record_id is accepted as a legacy alias only when it is clearly not a
      source/local/manual id.
    """

    if not isinstance(payload, dict):
        return {}
    normalized = dict(payload)
    action = text(action or normalized.get("action") or normalized.get("action_type")).lower()
    source_record_id = text(
        normalized.get("source_record_id") or normalized.get("lan_source_record_id")
    )
    target_record_id = text(
        normalized.get("target_record_id")
        or normalized.get("feishu_record_id")
        or normalized.get("raw_record_id")
    )
    record_id = text(normalized.get("record_id"))
    manual_id = text(normalized.get("manual_id") or normalized.get("manual_key"))
    record_id_kind = text(normalized.get("_record_id_kind")).lower()
    has_source_marker = bool(
        text(normalized.get("source_app_token"))
        or text(normalized.get("source_table_id"))
        or bool(normalized.get("source_only"))
        or bool(normalized.get("_source_only_delete"))
        or record_id_kind == "source"
    )
    record_id_is_target = (
        record_id_kind == "target"
        or bool(normalized.get("record_id_is_target"))
        or bool(normalized.get("_record_id_is_target"))
    )
    can_infer_record_id_as_target = action not in {"start", "upload", "create"}
    if (
        not target_record_id
        and record_id
        and not is_local_record_id(record_id)
        and can_infer_record_id_as_target
    ):
        if record_id_is_target:
            target_record_id = record_id
        elif record_id != source_record_id and record_id != manual_id and not has_source_marker:
            target_record_id = record_id
    if source_record_id:
        normalized["source_record_id"] = source_record_id
    if target_record_id:
        normalized["target_record_id"] = target_record_id
    return normalized


def canonical_target_record_id(payload: dict[str, Any] | None) -> str:
    return text(normalize_notice_identity_payload(payload).get("target_record_id"))


def canonical_source_record_id(payload: dict[str, Any] | None) -> str:
    return text(normalize_notice_identity_payload(payload).get("source_record_id"))

# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import hashlib
import logging
import uuid
from collections.abc import Callable
from typing import Any

from .state_store import LanPortalStateStore


logger = logging.getLogger(__name__)


AUDIT_METADATA_KEYS = (
    "work_type",
    "notice_type",
    "operation_type",
    "phase",
    "status",
    "role",
    "file_name",
    "paired_upload_status",
)


def _bounded_text(value: Any, limit: int) -> str:
    text = str(value or "").strip()
    return text if len(text) <= limit else f"{text[:limit]}..."


def business_audit_id(domain: str, action: str, operation_id: str = "") -> str:
    normalized_operation_id = str(operation_id or "").strip()
    if not normalized_operation_id:
        return uuid.uuid4().hex
    raw = f"{str(domain or '').strip()}:{str(action or '').strip()}:{normalized_operation_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def safe_audit_metadata(value: dict[str, Any] | None) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    result: dict[str, Any] = {}
    for key in AUDIT_METADATA_KEYS:
        item = source.get(key)
        if item in (None, "", [], {}):
            continue
        if isinstance(item, bool):
            result[key] = item
        elif isinstance(item, (int, float)):
            result[key] = item
        elif isinstance(item, str):
            result[key] = _bounded_text(item, 500)
    return result


def _result_warning(result: dict[str, Any]) -> str:
    warnings = result.get("warnings")
    if isinstance(warnings, list):
        return _bounded_text(
            "；".join(
                dict.fromkeys(
                    str(item or "").strip()
                    for item in warnings
                    if str(item or "").strip()
                )
            ),
            4000,
        )
    return _bounded_text(
        result.get("warning")
        or result.get("message_warning")
        or result.get("notification_warning")
        or "",
        4000,
    )


def begin_business_audit(
    store: LanPortalStateStore,
    *,
    domain: str,
    action: str,
    operation_id: str = "",
    scope: str = "",
    actor_open_id: str = "",
    actor_name: str = "",
    active_item_id: str = "",
    source_record_id: str = "",
    target_record_id: str = "",
    summary_record_id: str = "",
    related_record_ids: list[str] | tuple[str, ...] | None = None,
    metadata: dict[str, Any] | None = None,
) -> str:
    audit_id = business_audit_id(domain, action, operation_id)
    try:
        record_audit = getattr(store, "record_business_operation_audit")
        record_audit(
            audit_id=audit_id,
            operation_id=operation_id,
            domain=domain,
            action=action,
            status="started",
            scope=scope,
            actor_open_id=actor_open_id,
            actor_name=actor_name,
            active_item_id=active_item_id,
            source_record_id=source_record_id,
            target_record_id=target_record_id,
            summary_record_id=summary_record_id,
            related_record_ids=related_record_ids,
            metadata=safe_audit_metadata(metadata),
        )
    except Exception as exc:
        logger.warning(
            "业务操作审计启动记录失败: domain=%s action=%s audit_id=%s error=%s",
            domain,
            action,
            audit_id,
            exc,
        )
    return audit_id


def finish_business_audit(
    store: LanPortalStateStore,
    audit_id: str,
    *,
    success: bool,
    result: dict[str, Any] | None = None,
    error: str = "",
    error_stage: str = "",
    remote_written: bool | None = None,
    message_sent: bool | None = None,
) -> dict[str, Any]:
    normalized_result = result if isinstance(result, dict) else {}
    resolved_target_id = str(
        normalized_result.get("target_record_id")
        or normalized_result.get("real_record_id")
        or normalized_result.get("record_id")
        or ""
    ).strip()
    resolved_summary_id = str(
        normalized_result.get("summary_record_id") or ""
    ).strip()
    resolved_source_id = str(
        normalized_result.get("source_record_id") or ""
    ).strip()
    resolved_active_id = str(
        normalized_result.get("active_item_id") or ""
    ).strip()
    if remote_written is None and "remote_written" in normalized_result:
        remote_written = bool(normalized_result.get("remote_written"))
    if message_sent is None:
        if "message_sent" in normalized_result:
            message_sent = bool(normalized_result.get("message_sent"))
        elif "robot_sent" in normalized_result:
            message_sent = bool(normalized_result.get("robot_sent"))
    try:
        record_audit = getattr(store, "record_business_operation_audit")
        return record_audit(
            audit_id=audit_id,
            status="success" if success else "failed",
            active_item_id=resolved_active_id or None,
            source_record_id=resolved_source_id or None,
            target_record_id=resolved_target_id or None,
            summary_record_id=resolved_summary_id or None,
            remote_written=remote_written,
            message_sent=message_sent,
            warning=_result_warning(normalized_result) or None,
            error_stage=_bounded_text(error_stage, 120) or None,
            error=_bounded_text(error, 4000) or None,
            metadata=safe_audit_metadata(normalized_result),
        )
    except Exception as exc:
        logger.warning(
            "业务操作审计完成记录失败: audit_id=%s status=%s error=%s",
            audit_id,
            "success" if success else "failed",
            exc,
        )
        return {}


async def audited_thread_call(
    store: LanPortalStateStore,
    function: Callable[..., Any],
    *args: Any,
    audit_domain: str,
    audit_action: str,
    audit_operation_id: str = "",
    audit_scope: str = "",
    audit_actor_open_id: str = "",
    audit_actor_name: str = "",
    audit_active_item_id: str = "",
    audit_source_record_id: str = "",
    audit_target_record_id: str = "",
    audit_summary_record_id: str = "",
    audit_related_record_ids: list[str] | tuple[str, ...] | None = None,
    audit_metadata: dict[str, Any] | None = None,
    audit_remote_written_on_success: bool | None = None,
    **kwargs: Any,
) -> Any:
    audit_id = begin_business_audit(
        store,
        domain=audit_domain,
        action=audit_action,
        operation_id=audit_operation_id,
        scope=audit_scope,
        actor_open_id=audit_actor_open_id,
        actor_name=audit_actor_name,
        active_item_id=audit_active_item_id,
        source_record_id=audit_source_record_id,
        target_record_id=audit_target_record_id,
        summary_record_id=audit_summary_record_id,
        related_record_ids=audit_related_record_ids,
        metadata=audit_metadata,
    )
    try:
        result = await asyncio.to_thread(function, *args, **kwargs)
    except Exception as exc:
        finish_business_audit(
            store,
            audit_id,
            success=False,
            error=str(exc),
            error_stage="execute",
        )
        raise
    finish_business_audit(
        store,
        audit_id,
        success=True,
        result=result if isinstance(result, dict) else {},
        remote_written=audit_remote_written_on_success,
    )
    return result

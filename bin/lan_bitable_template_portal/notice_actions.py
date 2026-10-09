"""Shared native notice submission for web and assistant notice drafts."""
import asyncio
from fastapi.responses import JSONResponse
from .identity_utils import normalize_notice_identity_payload
from .operation_audit import begin_business_audit, finish_business_audit


async def submit_notice_action(controller, runtime, session, payload, *, card_submission=False):
    payload = normalize_notice_identity_payload(payload)
    payload.pop("_notice_card_submission", None)
    scope = controller._authorized_scope_or_error(
        session, payload.get("scope") or "ALL"
    )
    user = session.get("user") if isinstance(session.get("user"), dict) else {}
    payload["scope"] = scope
    payload["_auth_open_id"] = str(user.get("open_id") or "")
    payload["_auth_user_name"] = str(
        user.get("name") or user.get("en_name") or ""
    )
    payload["_web_action_request"] = True
    if str(payload.get("command_format") or "") == "notice_command":
        standalone_start = (
            payload.get("action") == "start"
            and payload.get("manual_binding_choice") == "unbound"
            and not payload.get("active_item_id")
            and not payload.get("target_record_id")
        )
        ongoing = [] if standalone_start else await asyncio.to_thread(controller._get_ongoing, scope)
        payload = await asyncio.to_thread(
            runtime.service.expand_workbench_action_command,
            payload,
            scope=scope,
            ongoing_items=ongoing,
        )
        payload["scope"] = scope
        payload["_auth_open_id"] = str(user.get("open_id") or "")
        payload["_auth_user_name"] = str(
            user.get("name") or user.get("en_name") or ""
        )
        payload["_web_action_request"] = True
    payload.pop("_notice_card_submission", None)
    if card_submission:
        payload["_notice_card_submission"] = True
    job_id, should_start = await asyncio.to_thread(runtime.service.create_action_job, payload)
    job = await asyncio.to_thread(runtime.service.get_job, job_id) or {}
    audit_id = str(job.get("business_audit_id") or "").strip()
    if should_start or not audit_id:
        audit_id = await asyncio.to_thread(
            begin_business_audit,
            runtime.state_store,
            domain="notice",
            action=str(payload.get("action") or "submit"),
            operation_id=job_id,
            scope=scope,
            actor_open_id=str(user.get("open_id") or ""),
            actor_name=str(user.get("name") or user.get("en_name") or ""),
            active_item_id=str(payload.get("active_item_id") or ""),
            source_record_id=str(payload.get("source_record_id") or ""),
            target_record_id=str(payload.get("target_record_id") or ""),
            metadata={
                "work_type": payload.get("work_type"),
                "notice_type": payload.get("notice_type"),
                "phase": "accepted",
            },
        )
        mark_job = getattr(runtime.service, "mark_job", None)
        if callable(mark_job):
            await asyncio.to_thread(
                mark_job,
                job_id,
                business_audit_id=audit_id,
                _persist=True,
            )
        if (
            not should_start
            and str(job.get("phase") or "") == "success"
        ):
            await asyncio.to_thread(
                finish_business_audit,
                runtime.state_store,
                audit_id,
                success=True,
                result={
                    "active_item_id": str(
                        job.get("active_item_id") or ""
                    ),
                    "target_record_id": str(
                        job.get("target_record_id")
                        or job.get("record_id")
                        or ""
                    ),
                    "message_sent": bool(job.get("message_sent")),
                    "message_warning": str(
                        job.get("message_warning") or ""
                    ),
                    "work_type": payload.get("work_type"),
                    "notice_type": payload.get("notice_type"),
                    "phase": "success",
                },
                remote_written=bool(
                    job.get("target_record_id")
                    or job.get("record_id")
                ),
                message_sent=bool(job.get("message_sent")),
            )
    if should_start:
        runtime.clear_payload_cache()
        controller._clear_read_cache()
        runtime.enqueue_initial_message_or_upload_job(job_id)
    job = await asyncio.to_thread(runtime.service.get_job, job_id) or job
    return JSONResponse(
        {
            "ok": True,
            "data": {
                "job_id": job_id,
                "accepted_at": job.get("accepted_at") or 0,
                "initial_phase": job.get("phase") or "accepted",
                "supersedes_job_ids": job.get("supersedes_job_ids") or [],
            },
        },
        status_code=202,
    )

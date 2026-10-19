"""Shared native notice submission for web and assistant notice drafts."""
import asyncio
import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar
from . import account_impersonation as impersonation
from fastapi.responses import JSONResponse
from .identity_utils import normalize_notice_identity_payload
from .operation_audit import begin_business_audit, finish_business_audit

CURRENT_NOTICE_JOB = ContextVar('impersonated_notice_job', default=None)

class NoticeImpersonationPending(Exception):
    """Local binding/audit persistence must recover before remote execution."""


class NoticeJobPermissionDenied(Exception):
    """Do not turn a revoked execution grant into a retryable remote failure."""


def trusted_notice_request(payload):
    """Drop client audit identities, including those nested in command patches."""
    def clean(value):
        if isinstance(value, dict):
            return {key: clean(item) for key, item in value.items()
                    if not str(key).lower().startswith(('_admin_', '_impersonation'))}
        if isinstance(value, list):
            return [clean(item) for item in value]
        return value
    result = clean(payload)
    grant = impersonation.CURRENT_IMPERSONATION.get()
    if grant:
        from .portal_service import PortalError
        from .portal_auth import TOPIC_EXECUTION
        if TOPIC_EXECUTION.get() is not None or result.get('_topic_delegation_id'):
            raise PortalError('管理员代登录不可与话题委派混用。')
        if not all(grant.get(key) for key in ('id', 'operator_open_id', 'target_open_id')):
            raise PortalError('管理员代登录身份未验证。')
        if result.get('_auth_open_id') not in (None, '', grant['target_open_id']):
            raise PortalError('通告办理账号与代登录目标不一致。')
        result['_auth_open_id'] = grant['target_open_id']
        result['_admin_impersonation_id'] = grant['id']
    return result


def check_notice_job_owner(previous, request):
    if previous.get('_admin_impersonation_id') or request.get('_admin_impersonation_id'):
        if any(previous.get(key) != request.get(key)
               for key in ('_admin_impersonation_id', '_auth_open_id')):
            from .portal_service import PortalError
            raise PortalError('原通告任务属于另一办理身份，不能复用或重新归属。')


def check_notice_job_impersonation(runtime, job_id):
    job = runtime.service.get_job(job_id) or {}
    request = job.get('request') or {}
    if not request.get('_admin_impersonation_id'):
        return None
    from .portal_auth import TOPIC_EXECUTION
    if request.get('_topic_delegation_id') or TOPIC_EXECUTION.get() is not None:
        raise NoticeJobPermissionDenied('原通告代登录授权无法验证。')
    try:
        if job.get('impersonation_binding_pending'):
            _recover_notice_binding(runtime, job_id, job)
        grant = impersonation.resume_job(runtime.auth_manager, runtime.state_store, request, job_id)
    except (OSError, sqlite3.Error) as exc:
        raise NoticeImpersonationPending('通告授权资料暂不可读，保留原任务等待恢复。') from exc
    except Exception as exc:
        raise NoticeJobPermissionDenied('原通告代登录授权已失效。') from exc
    if not grant or grant.get('id') != request['_admin_impersonation_id']:
        raise NoticeJobPermissionDenied('原通告代登录绑定不一致。')
    return grant


def _recover_notice_binding(runtime, job_id, job):
    """Repair only the server-stamped, persisted acceptance before any remote work."""
    from .portal_service import STATE_NS_ACTION_JOB
    from .module_access import require_module
    request = job.get('request') or {}
    store, auth = runtime.state_store, runtime.auth_manager
    with store._lock:
        if store.get_document(impersonation.NS + '_jobs', job_id):
            return  # resume_job, not this repair, verifies an existing binding.
        persisted = store.get_document(STATE_NS_ACTION_JOB, job_id) or {}
        grant = store.get_document(impersonation.NS, request.get('_admin_impersonation_id')) or {}
        accepted = float(persisted.get('accepted_at') or 0)
        if (persisted.get('job_id') != job_id or not persisted.get('impersonation_binding_pending') or persisted.get('request') != request
                or accepted <= 0
                or persisted.get('remote_intent') or persisted.get('remote_written') or persisted.get('message_sent')
                or persisted.get('upload_started_at') or persisted.get('qt_result')
                or grant.get('id') != request.get('_admin_impersonation_id')
                or grant.get('target_open_id') != request.get('_auth_open_id')
                or not grant.get('parent_hash') or not grant.get('operator_open_id')
                or not (float(grant.get('created_at') or 0) <= accepted <= float(grant.get('expires_at') or 0))
                or grant.get('ended_at') and accepted > float(grant['ended_at'])
                or not impersonation.can_impersonate(auth, grant.get('operator_open_id'), grant.get('target_open_id'))):
            raise NoticeJobPermissionDenied('缺少原任务的可靠代登录接受证据，未重新绑定。')
        session = {'user': {'open_id': grant['target_open_id']}}
        require_module(auth, session, request.get('work_type') or 'maintenance')
        if not auth.scope_allowed(session, request.get('scope') or 'ALL'):
            raise NoticeJobPermissionDenied('原通告当前楼栋权限已变化，未修复绑定。')
        impersonation.bind_job(store, grant, job_id)


def _ensure_impersonation_audit(runtime, job_id, grant):
    from .operation_audit import business_audit_id
    job = runtime.service.get_job(job_id) or {}
    request = job.get('request') or {}
    action = str(request.get('action') or 'submit')
    audit_id = job.get('business_audit_id') or business_audit_id('notice', action, job_id)
    rows = runtime.state_store.list_business_operation_audits(domain='notice', operation_id=job_id)
    audit = next((row for row in rows if row.get('audit_id') == audit_id), None)
    identity = {key: grant.get(key, '') for key in
                ('id', 'operator_open_id', 'operator_name', 'target_open_id', 'target_name')}
    if audit:
        if (audit.get('actor_open_id') != grant['operator_open_id']
                or any((audit.get('metadata', {}).get('impersonation') or {}).get(key) != grant.get(key)
                       for key in ('operator_open_id', 'target_open_id'))):
            from .portal_service import PortalError
            raise PortalError('原通告审计身份不一致，已阻止执行。')
    else:
        audit = runtime.state_store.record_business_operation_audit(
            audit_id=audit_id, domain='notice', action=action, operation_id=job_id,
            status='started', scope=str(request.get('scope') or ''),
            actor_open_id=grant['operator_open_id'], actor_name=grant.get('operator_name', ''),
            active_item_id=str(request.get('active_item_id') or ''),
            source_record_id=str(request.get('source_record_id') or ''),
            target_record_id=str(request.get('target_record_id') or ''),
            metadata={'work_type': request.get('work_type'), 'notice_type': request.get('notice_type'),
                      'phase': 'accepted', 'impersonation': identity})
        if not audit:
            raise OSError('通告审计尚未保存，等待原任务恢复。')
    runtime.service.mark_job(job_id, business_audit_id=audit_id,
                             impersonation_binding_pending=False, _persist=True)
    return audit_id


def bind_notice_job(runtime, job_id):
    grant = impersonation.CURRENT_IMPERSONATION.get()
    request = (runtime.service.get_job(job_id) or {}).get('request') or {}
    if grant:
        check_notice_job_owner(request, {'_admin_impersonation_id': grant['id'],
                                        '_auth_open_id': grant['target_open_id']})
        impersonation.bind_job(runtime.state_store, grant, job_id)
        _ensure_impersonation_audit(runtime, job_id, grant)
        finish_notice_job_audit(runtime, job_id)
    elif request.get('_admin_impersonation_id'):
        from .portal_service import PortalError
        raise PortalError('不能以本人身份接管管理员代登录任务。')


def finish_notice_job_audit(runtime, job_id):
    job = runtime.service.get_job(job_id) or {}
    if not (job.get('request') or {}).get('_admin_impersonation_id') or job.get('phase') not in {'success', 'failed', 'cancelled'}:
        return
    if not job.get('business_audit_id'):
        raise NoticeImpersonationPending('通告结果已保存，原审计标识待恢复。')
    rows = runtime.state_store.list_business_operation_audits(domain='notice', operation_id=job_id)
    audit = next((row for row in rows if row.get('audit_id') == job['business_audit_id']), None)
    expected_status = 'success' if job['phase'] == 'success' else 'failed'
    if audit and audit.get('status') == expected_status:
        if job.get('impersonation_audit_pending'):
            runtime.service.mark_job(job_id, impersonation_audit_pending=False, _persist=True)
        return
    runtime.service.mark_job(job_id, impersonation_audit_pending=True, _persist=True)
    result = finish_business_audit(runtime.state_store, job.get('business_audit_id') or '',
        success=job['phase'] == 'success', result={
            'phase': job['phase'], 'record_id': job.get('target_record_id') or job.get('record_id') or '',
            'message_warning': job.get('message_warning') or ''},
        error=job.get('error') or '', error_stage=job.get('error_category') or '',
        remote_written=bool(job.get('remote_written') or (job.get('qt_result') or {}).get('ok')),
        message_sent=bool(job.get('message_sent')))
    if not result:
        raise NoticeImpersonationPending('通告结果已保存，审计待恢复，不重复执行。')
    runtime.service.mark_job(job_id, impersonation_audit_pending=False, _persist=True)


@contextmanager
def notice_job_impersonation(runtime, job_id, *, required=False):
    job = runtime.service.get_job(job_id) or {}
    if not job and required:
        raise NoticeJobPermissionDenied('原通告任务不存在，无法核验执行身份。')
    grant = check_notice_job_impersonation(runtime, job_id)
    token = impersonation.CURRENT_IMPERSONATION.set(grant)
    job_token = CURRENT_NOTICE_JOB.set(job_id if grant else None)
    try:
        if grant:
            try:
                _ensure_impersonation_audit(runtime, job_id, grant)
            except (OSError, sqlite3.Error) as exc:
                raise NoticeImpersonationPending('通告审计尚未保存，等待原任务恢复。') from exc
        yield grant
    finally:
        CURRENT_NOTICE_JOB.reset(job_token)
        impersonation.CURRENT_IMPERSONATION.reset(token)


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
    from .module_access import require_module
    require_module(runtime.auth_manager, session, str(payload.get("work_type") or "maintenance"))
    from .portal_auth import topic_execution
    delegated = topic_execution(runtime.auth_manager, session)
    payload.pop('_topic_delegation_id', None)
    if delegated is not None:
        payload['_topic_delegation_id'] = delegated.grant['id']
    job_id, should_start = await asyncio.to_thread(runtime.service.create_action_job, payload)
    binding_pending = False
    try:
        await asyncio.to_thread(bind_notice_job, runtime, job_id)
    except (OSError, sqlite3.Error, NoticeImpersonationPending):
        accepted = await asyncio.to_thread(runtime.service.get_job, job_id) or {}
        if not (accepted.get('request') or {}).get('_admin_impersonation_id'):
            raise
        binding_pending = True
    job = await asyncio.to_thread(runtime.service.get_job, job_id) or {}
    audit_id = str(job.get("business_audit_id") or "").strip()
    if (should_start or not audit_id) and not (job.get('request') or {}).get('_admin_impersonation_id'):
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
    if should_start or binding_pending:
        runtime.clear_payload_cache()
        controller._clear_read_cache()
        # The accepted job+queue already committed together; let the worker repair its own binding.
        token = impersonation.CURRENT_IMPERSONATION.set(None) if binding_pending else None
        try:
            runtime.enqueue_initial_message_or_upload_job(job_id)
        finally:
            if token is not None:
                impersonation.CURRENT_IMPERSONATION.reset(token)
    job = await asyncio.to_thread(runtime.service.get_job, job_id) or job
    from .event_priority import public_job
    job = public_job(job, runtime.auth_manager, session)
    return JSONResponse(
        {
            "ok": True,
            "data": {
                "job_id": job_id,
                "accepted_at": job.get("accepted_at") or 0,
                "initial_phase": job.get("phase") or "accepted",
                "event_wait": job.get("event_wait") or {},
                "authorization_pending": bool(job.get('impersonation_binding_pending')),
                "supersedes_job_ids": job.get("supersedes_job_ids") or [],
            },
        },
        status_code=202,
    )

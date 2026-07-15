# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import mimetypes
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from collections import deque
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import parse_qs, urlencode

import httpx
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse, Response, StreamingResponse

from clipflow_backend.preflight import (
    build_backend_preflight_report as _build_backend_preflight_report,
)
from clipflow_backend.runtime_helpers import (
    env_float as _env_float,
    external_guard_status as _external_guard_status,
    mock_external_enabled as _mock_external_enabled,
    send_text_to_open_ids_guarded as _send_text_to_open_ids_guarded,
    wait_until_listening as _wait_until_listening,
)
from clipflow_backend.api_models import (
    APIModel,
    AuthPermissionsSaveRequest,
    ChangeTargetConfirmRequest,
    ChangeTargetLookupRequest,
    EngineerMopBindRequest,
    EngineerMopFillRequest,
    EngineerMopResetRequest,
    EngineerMopSettingsSaveRequest,
    EngineerMopUploadSignedRequest,
    EventTransferRepairRequest,
    GenerateTemplatesRequest,
    HandoverLinksAuthRequest,
    HandoverLinksSaveRequest,
    HandoverPasswordResetConfirmRequest,
    JobMarkStuckFailedRequest,
    MockPressureRequest,
    NoticeMemoryHistorySaveRequest,
    NoticeMemoryHistoryScanRequest,
    NoticeMemoryImportRequest,
    NoticeIdentityBindRequest,
    NoticeTargetLookupRequest,
    NoticeUndoApplyRequest,
    NoticeWorkTypeOverrideRequest,
    OngoingDeleteRequest,
    PermissionRequestBulkReviewRequest,
    PermissionRequestConfirm,
    PermissionRequestCreate,
    PermissionRequestReviewRequest,
    RepairManagementPrefillRequest,
    RepairManagementRecordRequest,
    RepairFollowupRecordRequest,
    QtClipboardAckRequest,
    QtClipboardEventRequest,
    QtActiveItemsDeltaRequest,
    QtCommandRequest,
    QtDialogResultRequest,
    QtDialogSessionRequest,
    QtEventAckRequest,
    QtJobProgressRequest,
    QtJobResultRequest,
    QtLocalHeartbeatRequest,
    QtOngoingSnapshotRequest,
    SendGeneratedRequest,
    ExternalSignatureSaveRequest,
    SignatureSendLinkRequest,
    SignatureSaveRequest,
    SignatureUsageConfirmationSendRequest,
    TemporarySignatureCreateRequest,
    TemporarySignatureSaveRequest,
    TemporarySignatureSendLinkRequest,
    WorkbenchActionRequest,
    parse_api_model,
)
from lan_bitable_template_portal.server import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    MAX_JSON_BODY_BYTES,
    PortalRuntime,
    find_available_port,
    portal_asset_file,
    portal_frontend_dist_enabled,
    portal_frontend_dist_ready,
    portal_index_file,
    portal_static_roots,
)
from lan_bitable_template_portal.workbench_lite import (
    ONGOING_PAGE_SIZE,
    PENDING_PAGE_SIZE,
    parse_pasted_notice_to_draft,
    render_workbench_lite,
)
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME
from lan_bitable_template_portal.identity_utils import (
    canonical_source_record_id,
    canonical_target_record_id,
    is_local_record_id,
    normalize_notice_identity_payload,
)
from lan_bitable_template_portal.portal_service import (
    CHANGE_SOURCE_APP_TOKEN,
    CHANGE_SOURCE_TABLE_ID,
    DEFAULT_APP_TOKEN,
    DEFAULT_TABLE_ID,
    NOTICE_TYPE_CHANGE,
    NOTICE_TYPE_MAINTENANCE,
    NOTICE_TYPE_REPAIR,
    NOTICE_TYPE_BY_WORK_TYPE,
    PortalError,
    REPAIR_SOURCE_APP_TOKEN,
    REPAIR_SOURCE_TABLE_ID,
    REPAIR_SYNC_TABLE_ID,
    SCOPE_OPTIONS,
    WORK_TYPE_BY_NOTICE_TYPE,
    ZHIHANG_CHANGE_APP_TOKEN,
    ZHIHANG_CHANGE_TABLE_ID,
    engineer_mop_fill_kwargs_from_payload,
    engineer_mop_upload_signed_kwargs_from_payload,
)
from lan_bitable_template_portal.portal_service import MaintenancePortalService
from lan_bitable_template_portal.portal_auth import PortalAuthManager
from lan_bitable_template_portal.state_store import LanPortalStateStore
from upload_event_module.config import config
from upload_event_module.core.parser import extract_event_info
from upload_event_module.services.feishu_service import check_token_status

INLINE_IMAGE_B64_FIELDS = {"bytes_b64", "screenshot_bytes_b64"}
from upload_event_module.services.service_registry import query_record_by_id
from upload_event_module.logger import log_error, log_info, log_warning

try:
    from apscheduler.schedulers.background import BackgroundScheduler
except Exception:
    BackgroundScheduler = None


MAX_SITE_PHOTO_BYTES = 8 * 1024 * 1024
MAX_NOTICE_ATTACHMENT_PENDING_BYTES = int(
    _env_float(
        "CLIPFLOW_NOTICE_ATTACHMENT_PENDING_MAX_BYTES",
        300 * 1024 * 1024,
        minimum=16 * 1024 * 1024,
        maximum=2 * 1024 * 1024 * 1024,
    )
)


def _queue_stats() -> dict:
    qt_outbox_counts: dict[str, int] = {}
    runtime_queue_details: dict[str, dict] = {}
    try:
        qt_outbox_counts = PortalRuntime.state_store.count_outbox_events("qt_action")
    except Exception:
        qt_outbox_counts = {}
    try:
        runtime_queue_details = PortalRuntime.state_store.runtime_queue_details()
    except Exception:
        runtime_queue_details = {}
    with PortalRuntime.message_queue_lock:
        message_queue_size = 0
        message_workers = sum(
            1 for worker in PortalRuntime.message_worker_threads if worker.is_alive()
        )
    with PortalRuntime.action_queue_lock:
        qt_queue_size = 0
        qt_worker_alive = bool(
            PortalRuntime.action_worker_thread
            and PortalRuntime.action_worker_thread.is_alive()
        )
    message_details = runtime_queue_details.get("message")
    if isinstance(message_details, dict):
        message_queue_size = int(message_details.get("queued_due") or 0) + int(
            message_details.get("queued_future") or 0
        )
    qt_details = runtime_queue_details.get("qt_action")
    if isinstance(qt_details, dict):
        qt_queue_size = int(qt_details.get("queued_due") or 0) + int(
            qt_details.get("queued_future") or 0
        )
    with PortalRuntime.upload_wait_lock:
        upload_wait_size = 0
        upload_wait_alive = bool(
            PortalRuntime.upload_wait_thread
            and PortalRuntime.upload_wait_thread.is_alive()
        )
    upload_wait_details = runtime_queue_details.get("upload_wait")
    if isinstance(upload_wait_details, dict):
        upload_wait_size = int(upload_wait_details.get("queued_due") or 0) + int(
            upload_wait_details.get("queued_future") or 0
        ) + int(upload_wait_details.get("processing_active") or 0)
    with PortalRuntime.source_refresh_lock:
        source_refresh_inflight = bool(PortalRuntime.source_refresh_inflight)
    with PortalRuntime.repair_refresh_lock:
        repair_refresh_inflight = bool(PortalRuntime.repair_refresh_inflight)
    with PortalRuntime.change_refresh_lock:
        change_refresh_inflight = bool(PortalRuntime.change_refresh_inflight)
    return {
        "message_queue_size": message_queue_size,
        "message_worker_count": int(PortalRuntime.message_worker_count or 0),
        "message_workers_alive": message_workers,
        "qt_queue_size": qt_queue_size,
        "qt_worker_alive": qt_worker_alive,
        "qt_outbox_pending": int(qt_outbox_counts.get("pending") or 0),
        "qt_outbox_leased": int(qt_outbox_counts.get("leased") or 0),
        "qt_outbox_failed": int(qt_outbox_counts.get("failed") or 0),
        "upload_wait_size": upload_wait_size,
        "upload_wait_alive": upload_wait_alive,
        "source_refresh_alive": bool(
            PortalRuntime.source_refresh_thread
            and PortalRuntime.source_refresh_thread.is_alive()
        ),
        "source_refresh_inflight": source_refresh_inflight,
        "repair_refresh_inflight": repair_refresh_inflight,
        "change_refresh_inflight": change_refresh_inflight,
        "payload_cache_entries": len(PortalRuntime.payload_cache),
        "runtime_limits": PortalRuntime.runtime_limits(),
        "runtime_pressure": PortalRuntime.runtime_pressure(),
        "sqlite_write_worker": PortalRuntime.state_store.get_write_worker_stats(),
        "runtime_queue_counts": PortalRuntime.state_store.runtime_queue_counts(),
        "runtime_queue_details": PortalRuntime.state_store.runtime_queue_details(),
    }


class FastAPIPortalController:
    """FastAPI/Uvicorn front controller for the production portal."""

    def __init__(
        self,
        *,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        app_token: str = DEFAULT_APP_TOKEN,
        table_id: str = DEFAULT_TABLE_ID,
    ) -> None:
        self.host = str(host or DEFAULT_HOST).strip() or DEFAULT_HOST
        self.preferred_port = int(port or DEFAULT_PORT)
        self.app_token = str(app_token or DEFAULT_APP_TOKEN).strip()
        self.table_id = str(table_id or DEFAULT_TABLE_ID).strip()
        self.bound_port: int | None = None
        self._app: FastAPI | None = None
        self._server = None
        self._thread: threading.Thread | None = None
        self._scheduler = None
        self._shutdown_event = threading.Event()
        self._state_store = LanPortalStateStore()
        self.notice_callback = None
        self.ongoing_callback = None
        self.ongoing_delete_callback = None
        self.maintenance_action_callback = None
        self._read_cache_lock = threading.RLock()
        self._read_cache: dict[tuple, tuple[float, float, object]] = {}
        self._read_cache_max_entries = 128
        self._static_cache_lock = threading.RLock()
        self._static_cache: dict[str, dict] = {}
        self._static_cache_max_entries = int(
            _env_float("CLIPFLOW_STATIC_CACHE_MAX_ENTRIES", 64, minimum=8, maximum=512)
        )
        self._static_cache_max_bytes = int(
            _env_float(
                "CLIPFLOW_STATIC_CACHE_MAX_BYTES",
                16 * 1024 * 1024,
                minimum=1024 * 1024,
                maximum=256 * 1024 * 1024,
            )
        )
        self._static_cache_max_entry_bytes = int(
            _env_float(
                "CLIPFLOW_STATIC_CACHE_MAX_ENTRY_BYTES",
                4 * 1024 * 1024,
                minimum=256 * 1024,
                maximum=64 * 1024 * 1024,
            )
        )
        self._rate_limit_lock = threading.RLock()
        self._rate_limit_buckets: dict[tuple, dict] = {}
        self._perf_lock = threading.RLock()
        self._perf: dict[str, dict] = {}
        self._perf_max_entries = int(
            _env_float("CLIPFLOW_PERF_MAX_ENDPOINTS", 200, minimum=20, maximum=1000)
        )
        self._job_batch_lock = threading.RLock()
        self._job_batch_stats = {
            "requests": 0,
            "requested_jobs": 0,
            "returned_jobs": 0,
            "missing_jobs": 0,
            "denied_jobs": 0,
            "max_request_size": 0,
            "updated_at": 0.0,
        }
        self._sse_lock = threading.RLock()
        self._sse_connections: dict[tuple, int] = {}
        self._sse_connection_seq = 0
        self._background_executor = ThreadPoolExecutor(
            max_workers=int(
                _env_float("CLIPFLOW_BACKEND_BG_WORKERS", 6, minimum=2, maximum=24)
            ),
            thread_name_prefix="ClipFlowBackendBg",
        )

    def _submit_background(self, name: str, fn, *args) -> bool:
        if self._shutdown_event.is_set():
            return False

        def _run() -> None:
            try:
                fn(*args)
            except Exception as exc:
                log_warning(f"后端后台任务异常: {name}: {exc}")

        try:
            self._background_executor.submit(_run)
            return True
        except RuntimeError as exc:
            log_warning(f"后端后台任务提交失败: {name}: {exc}")
            return False

    @staticmethod
    def _permission_scope_text(permission_request: dict) -> str:
        labels = permission_request.get("requested_scope_labels")
        if isinstance(labels, list) and labels:
            return "、".join(str(item) for item in labels if item)
        scopes = permission_request.get("requested_scopes")
        if isinstance(scopes, list) and scopes:
            return "、".join(str(item) for item in scopes if item)
        return "未选择"

    def _notify_permission_review_result(
        self,
        permission_request: dict,
        *,
        approved: bool,
        actor_name: str,
        reason: str = "",
    ) -> dict:
        open_id = str(permission_request.get("open_id") or "").strip()
        if not open_id:
            return {"ok": False, "message": "申请人 openid 为空，无法通知。"}
        status_text = "已通过" if approved else "未通过"
        reason_text = str(reason or permission_request.get("reject_reason") or "").strip()
        text = (
            "南通基地-运维灯塔工作台权限申请处理结果。\n"
            f"处理结果：{status_text}\n"
            f"申请范围：{self._permission_scope_text(permission_request)}\n"
            f"处理人：{actor_name or '管理员'}\n"
        )
        if reason_text and not approved:
            text += f"拒绝原因：{reason_text}\n"
        text += "请重新进入门户或刷新页面查看最新权限。"
        ok, message, _ = _send_text_to_open_ids_guarded(text, [open_id])
        return {"ok": bool(ok), "message": str(message or ""), "recipient": open_id}

    def _submit_notice_undo_job(self, job_id: str, *, name: str = "") -> bool:
        job_id = str(job_id or "").strip()
        if not job_id:
            return False
        task_name = name or f"NoticeUndo-{job_id[:8]}"
        submitted = self._submit_background(task_name, self._run_notice_undo_job, job_id)
        if not submitted:
            with suppress(Exception):
                PortalRuntime.service.mark_job(
                    job_id,
                    phase="failed",
                    error="后端正在关闭，回退任务未启动。",
                    upload_message="后端正在关闭，回退任务未启动。",
                )
        return submitted

    def _build_app(self) -> FastAPI:
        app = FastAPI(title="ClipFlow LAN Backend")

        @app.middleware("http")
        async def pressure_guard(request: Request, call_next):
            group = self._endpoint_group(request)
            started = time.perf_counter()
            rate_limited = False
            if not self._rate_limit_exempt(request, group):
                allowed, retry_after = self._consume_rate_limit(request, group)
                if not allowed:
                    rate_limited = True
                    stale = self._stale_response_for_rate_limit(request)
                    if stale is not None:
                        self._record_endpoint_perf(
                            request,
                            group=group,
                            elapsed_ms=(time.perf_counter() - started) * 1000.0,
                            status_code=200,
                            rate_limited=True,
                            cache_hit=True,
                        )
                        return stale
                    self._record_endpoint_perf(
                        request,
                        group=group,
                        elapsed_ms=(time.perf_counter() - started) * 1000.0,
                        status_code=429,
                        rate_limited=True,
                    )
                    return JSONResponse(
                        {
                            "ok": False,
                            "error": "请求过于频繁，请稍后再试。",
                            "retry_after_seconds": retry_after,
                        },
                        status_code=429,
                        headers={"Retry-After": str(max(1, int(retry_after)))},
                    )
            try:
                response = await call_next(request)
                return response
            finally:
                self._record_endpoint_perf(
                    request,
                    group=group,
                    elapsed_ms=(time.perf_counter() - started) * 1000.0,
                    status_code=int(getattr(locals().get("response", None), "status_code", 500) or 500),
                    cache_hit=bool(getattr(request.state, "cache_hit", False)),
                    cache_miss=bool(getattr(request.state, "cache_miss", False)),
                    rate_limited=rate_limited,
                )

        @app.get("/")
        async def root(request: Request):
            oauth_response = self._root_oauth_callback_response(request)
            if oauth_response is not None:
                return oauth_response
            if (
                str(request.query_params.get("scope") or "").strip()
                and str(request.query_params.get("mode") or "").strip() != "events"
            ):
                params = {
                    key: value
                    for key, value in request.query_params.multi_items()
                    if key != "frontend"
                }
                return Response(
                    status_code=302,
                    headers={"Location": f"/workbench-lite?{urlencode(params)}"},
                )
            return self._static_file_response(request, portal_index_file(), html=True)

        @app.get("/admin/history-memory")
        @app.get("/admin/history-memory/")
        async def admin_history_memory_page(request: Request):
            return self._static_file_response(request, portal_index_file(), html=True)

        @app.get("/workbench-lite")
        @app.get("/workbench-lite/")
        async def workbench_lite_page(request: Request):
            session = self._current_session(request)
            if session is None:
                next_path = str(request.url.path or "/workbench-lite")
                if str(request.url.query or ""):
                    next_path += "?" + str(request.url.query)
                login_url = f"/api/auth/login?{urlencode({'next': next_path})}"
                return Response(status_code=302, headers={"Location": login_url})
            try:
                self._ensure_source_snapshot_background()
                requested_scope = (
                    request.query_params.get("scope")
                    or PortalRuntime.auth_manager.default_scope(session)
                    or "ALL"
                )
                scope = self._authorized_scope_or_error(session, requested_scope)
                work_type = str(request.query_params.get("work_type") or "maintenance").strip()
                if work_type != "all" and work_type not in NOTICE_TYPE_BY_WORK_TYPE:
                    work_type = "maintenance"
                repair_management_record_id = str(
                    request.query_params.get("repair_management_record_id") or ""
                ).strip()
                repair_notice_prefill: dict[str, Any] = {}
                if repair_management_record_id:
                    work_type = "repair"
                    repair_notice_prefill = await asyncio.to_thread(
                        PortalRuntime.service.repair_management_notice_prefill,
                        repair_management_record_id,
                        scope=scope,
                    )
                search = str(request.query_params.get("search") or "")
                specialty = str(request.query_params.get("specialty") or "")
                record_id = str(request.query_params.get("record_id") or "")
                active_item_id = str(request.query_params.get("active_item_id") or "")
                pending_page = str(request.query_params.get("pending_page") or "1")
                ongoing_page = str(request.query_params.get("ongoing_page") or "1")
                manual = str(request.query_params.get("manual") or "").strip().lower() in {
                    "1",
                    "true",
                    "yes",
                    "on",
                }
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                sections = ("records", "ongoing", "stats", "zhihang")
                open_id = str((session.get("user") or {}).get("open_id") or "")
                payload_task = asyncio.to_thread(
                    self._cached_service_payload,
                    (
                        "workbench",
                        open_id,
                        scope,
                        "",
                        specialty,
                        search,
                        work_type,
                        sections,
                        pending_page,
                        str(PENDING_PAGE_SIZE),
                        ongoing_page,
                        str(ONGOING_PAGE_SIZE),
                        PortalRuntime._ongoing_items_marker(ongoing),
                    ),
                    lambda: PortalRuntime.service.query_records(
                        scope=scope,
                        specialty=specialty,
                        search=search,
                        ongoing_items=ongoing,
                        work_type=work_type,
                        sections=sections,
                        records_page=pending_page,
                        records_page_size=PENDING_PAGE_SIZE,
                        ongoing_page=ongoing_page,
                        ongoing_page_size=ONGOING_PAGE_SIZE,
                    ),
                )
                undo_task = asyncio.to_thread(
                    PortalRuntime.service.list_available_notice_undos,
                    scope=scope,
                    since_seconds=3 * 24 * 60 * 60,
                )
                payload, notice_undos = await asyncio.gather(payload_task, undo_task)
                scope_options = PortalRuntime.auth_manager.filter_scope_options(
                    SCOPE_OPTIONS,
                    session,
                )
                html_body = render_workbench_lite(
                    payload=payload if isinstance(payload, dict) else {},
                    session=session,
                    scope=scope,
                    work_type=work_type,
                    search=search,
                    specialty=specialty,
                    record_id=record_id,
                    active_item_id=active_item_id,
                    pending_page=pending_page,
                    ongoing_page=ongoing_page,
                    manual=manual,
                    scope_options=scope_options,
                    notice_undos=notice_undos if isinstance(notice_undos, list) else [],
                    prefill_draft=(
                        repair_notice_prefill.get("draft")
                        if isinstance(repair_notice_prefill.get("draft"), dict)
                        else None
                    ),
                    prefill_source_record_id=str(
                        repair_notice_prefill.get("source_record_id") or ""
                    ),
                    prefill_target_record_id=str(
                        repair_notice_prefill.get("target_record_id") or ""
                    ),
                    prefill_action=str(
                        repair_notice_prefill.get("action") or "start"
                    ),
                    prefill_context_id=repair_management_record_id,
                )
                return Response(
                    content=html_body.encode("utf-8"),
                    media_type="text/html; charset=utf-8",
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/workbench-lite/parse")
        async def workbench_lite_parse_page(
            request: Request,
            scope: str = Form("ALL"),
            work_type: str = Form("maintenance"),
            paste_text: str = Form(""),
        ):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                self._ensure_source_snapshot_background()
                checked_scope = self._authorized_scope_or_error(session, scope or "ALL")
                parsed_work_type, parsed_action, parsed_draft = parse_pasted_notice_to_draft(
                    paste_text
                )
                if parsed_work_type not in NOTICE_TYPE_BY_WORK_TYPE:
                    parsed_work_type = str(work_type or "maintenance")
                if parsed_work_type not in NOTICE_TYPE_BY_WORK_TYPE:
                    parsed_work_type = "maintenance"
                ongoing = self._get_ongoing(checked_scope)
                self._reconcile_orphan_started_items(checked_scope, ongoing)
                payload = await asyncio.to_thread(
                    PortalRuntime.service.query_records,
                    scope=checked_scope,
                    ongoing_items=ongoing,
                    work_type=parsed_work_type,
                    sections=("records", "ongoing", "stats", "zhihang"),
                    records_page=1,
                    records_page_size=PENDING_PAGE_SIZE,
                    ongoing_page=1,
                    ongoing_page_size=ONGOING_PAGE_SIZE,
                )
                scope_options = PortalRuntime.auth_manager.filter_scope_options(
                    SCOPE_OPTIONS,
                    session,
                )
                notice_undos = await asyncio.to_thread(
                    PortalRuntime.service.list_available_notice_undos,
                    scope=checked_scope,
                    since_seconds=3 * 24 * 60 * 60,
                )
                html_body = render_workbench_lite(
                    payload=payload if isinstance(payload, dict) else {},
                    session=session,
                    scope=checked_scope,
                    work_type=parsed_work_type,
                    manual=True,
                    scope_options=scope_options,
                    parsed_draft=parsed_draft,
                    parsed_action=parsed_action,
                    paste_text=paste_text,
                    notice_undos=notice_undos if isinstance(notice_undos, list) else [],
                )
                return Response(
                    content=html_body.encode("utf-8"),
                    media_type="text/html; charset=utf-8",
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.get("/engineer/mop")
        @app.get("/engineer/mop/")
        async def engineer_mop_page(request: Request):
            return self._static_file_response(request, portal_index_file(), html=True)

        @app.get("/repair-management")
        @app.get("/repair-management/")
        async def repair_management_page(request: Request):
            return self._static_file_response(request, portal_index_file(), html=True)

        @app.get("/repair-status")
        @app.get("/repair-status/")
        async def repair_status_page(request: Request):
            return self._static_file_response(request, portal_index_file(), html=True)

        @app.get("/signature")
        @app.get("/signature/")
        async def signature_page(request: Request):
            return self._static_file_response(request, portal_index_file(), html=True)

        @app.get("/assets/{asset_path:path}")
        async def assets(asset_path: str, request: Request):
            relative = Path(*str(asset_path or "").split("/"))
            return self._static_file_response(request, portal_asset_file(relative))

        @app.get("/api/auth/login")
        async def auth_login(request: Request):
            next_path = str(request.query_params.get("next") or "/")
            redirect_uri = f"{self._request_base_url(request)}/api/auth/feishu/callback"
            try:
                login_url = PortalRuntime.auth_manager.start_login(
                    redirect_uri=redirect_uri,
                    next_path=next_path,
                )
            except PortalError as exc:
                return self._html_message(500, "飞书登录未启用", str(exc))
            return Response(
                status_code=302,
                headers={"Location": login_url},
            )

        @app.get("/api/auth/feishu/callback")
        async def auth_callback(request: Request):
            code = str(request.query_params.get("code") or "")
            state = str(request.query_params.get("state") or "")
            redirect_uri = f"{self._request_base_url(request)}/api/auth/feishu/callback"
            try:
                session_id, next_path = PortalRuntime.auth_manager.complete_login(
                    code=code,
                    state=state,
                    redirect_uri=redirect_uri,
                )
            except PortalError as exc:
                return self._html_message(400, "飞书登录失败", str(exc))
            self._clear_read_cache(("auth_status",))
            return Response(
                status_code=302,
                headers={
                    "Location": next_path,
                    "Set-Cookie": PortalRuntime.auth_manager.cookie_header(session_id),
                },
            )

        @app.get("/api/auth/logout")
        async def auth_logout_get(request: Request):
            PortalRuntime.auth_manager.clear_session(
                str(request.cookies.get(AUTH_COOKIE_NAME) or "")
            )
            self._clear_read_cache(("auth_status",))
            return Response(
                status_code=302,
                headers={
                    "Location": "/",
                    "Set-Cookie": PortalRuntime.auth_manager.clear_cookie_header(),
                },
            )

        @app.post("/api/auth/logout")
        async def auth_logout_post(request: Request):
            PortalRuntime.auth_manager.clear_session(
                str(request.cookies.get(AUTH_COOKIE_NAME) or "")
            )
            self._clear_read_cache(("auth_status",))
            return JSONResponse(
                {"ok": True, "data": {}},
                headers={"Set-Cookie": PortalRuntime.auth_manager.clear_cookie_header()},
            )

        @app.get("/api/health")
        async def health(request: Request) -> dict:
            cached = self._read_cache_get(("health",), ttl=1.5, stale_ttl=5.0)
            if cached is not None:
                request.state.cache_hit = True
                return cached
            request.state.cache_miss = True
            payload = {
                "ok": True,
                "data": {
                    "service": "clipflow_backend",
                    "backend": "fastapi",
                    "mock_external": _mock_external_enabled(),
                    "external_guard": _external_guard_status(),
                    "url": self.get_url(),
                    "frontend": {
                        "vue_ready": portal_frontend_dist_ready(),
                        "vue_enabled": portal_frontend_dist_enabled(),
                    },
                    "time": time.time(),
                    "runtime": PortalRuntime.state_store.get_backend_runtime("backend") or {},
                },
            }
            self._read_cache_put(("health",), payload, ttl=1.5, stale_ttl=5.0)
            return payload

        @app.get("/api/backend/stats")
        async def stats(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            if not PortalRuntime.auth_manager.is_admin(session):
                return JSONResponse(
                    {"ok": False, "error": "只有管理员可以查看后端状态。"},
                    status_code=403,
                )
            service = PortalRuntime.service
            last_loaded_at = ""
            last_loaded_ts = 0.0
            warnings: list[str] = []
            capacity_warnings: list[str] = []
            recent_failed_jobs: list[dict] = []
            job_count = 0
            job_phase_counts: dict[str, int] = {}
            failed_retryable_count = 0
            failed_non_retryable_count = 0
            qt_bridge = PortalRuntime.state_store.get_backend_runtime("qt_bridge") or {}
            qt_bridge_payload = dict(qt_bridge) if isinstance(qt_bridge, dict) else {}
            qt_bridge_heartbeat_at = float(qt_bridge_payload.get("heartbeat_at") or 0)
            qt_bridge_age_seconds = (
                max(0.0, time.time() - qt_bridge_heartbeat_at)
                if qt_bridge_heartbeat_at
                else 0.0
            )
            job_cleanup = PortalRuntime.state_store.get_backend_runtime("job_cleanup") or {}
            preflight = PortalRuntime.state_store.get_backend_runtime("preflight") or {}
            token_status = PortalRuntime.state_store.get_backend_runtime("token_status") or {}
            sqlite_maintenance = (
                PortalRuntime.state_store.get_backend_runtime("sqlite_maintenance")
                or {}
            )
            sqlite_stats: dict = {}
            source_snapshot_stats: dict = {}
            event_snapshot_stats: dict = {}
            repair_snapshot_stats: dict = {}
            source_type_stats: dict = {}
            schema_status: dict = {}
            runtime_health: dict = {}
            upload_attachment_stats: dict = {}
            signature_crypto_stats: dict = {}
            slow_jobs: list[dict] = []
            with suppress(Exception):
                sqlite_stats = PortalRuntime.state_store.get_database_stats()
            with suppress(Exception):
                source_snapshot_stats = PortalRuntime.state_store.source_snapshot_stats()
            with suppress(Exception):
                event_snapshot_stats = PortalRuntime.state_store.event_month_snapshot_stats()
            with suppress(Exception):
                repair_snapshot_stats = PortalRuntime.state_store.repair_snapshot_stats()
            with suppress(Exception):
                source_type_stats = PortalRuntime.state_store.source_snapshot_work_type_stats()
            with suppress(Exception):
                schema_status = PortalRuntime.state_store.schema_health()
            with suppress(Exception):
                runtime_health = PortalRuntime.state_store.runtime_health_report()
            with suppress(Exception):
                upload_attachment_stats = PortalRuntime.state_store.notice_upload_attachment_stats()
                upload_attachment_stats["max_pending_bytes"] = MAX_NOTICE_ATTACHMENT_PENDING_BYTES
            with suppress(Exception):
                signature_crypto_stats = service.signature_crypto_status()
            with suppress(Exception):
                last_loaded_at = str(getattr(service, "_last_loaded_at", "") or "")
                last_loaded_ts = float(getattr(service, "_last_loaded_ts", 0.0) or 0.0)
                warnings = list(getattr(service, "_load_warnings", []) or [])[-10:]
                with service._jobs_lock:
                    all_jobs = [
                        dict(job)
                        for job in service._jobs.values()
                        if isinstance(job, dict)
                    ]
                    failed_jobs = [
                        dict(job)
                        for job in all_jobs
                        if str(job.get("phase") or "") == "failed"
                    ]
                job_count = len(all_jobs)
                for job in all_jobs:
                    phase = str(job.get("phase") or "unknown").strip() or "unknown"
                    job_phase_counts[phase] = job_phase_counts.get(phase, 0) + 1
                failed_jobs.sort(
                    key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""),
                    reverse=True,
                )
                failed_retryable_count = sum(
                    1 for item in failed_jobs if bool(item.get("error_retryable"))
                )
                failed_non_retryable_count = max(
                    0, len(failed_jobs) - failed_retryable_count
                )
                recent_failed_jobs = [
                    {
                        "job_id": str(item.get("job_id") or ""),
                        "updated_at": str(item.get("updated_at") or ""),
                        "work_type": str((item.get("request") or {}).get("work_type") or ""),
                        "action": str((item.get("request") or {}).get("action") or ""),
                        "error": str(item.get("error") or item.get("upload_message") or ""),
                        "error_category": str(item.get("error_category") or ""),
                        "error_retryable": bool(item.get("error_retryable")),
                        "can_retry": bool(item.get("error_retryable")),
                        "can_clear": True,
                    }
                    for item in failed_jobs[:10]
                ]
                slow_jobs = self._slow_jobs_snapshot(all_jobs)
            table_counts = (
                sqlite_stats.get("table_counts")
                if isinstance(sqlite_stats, dict)
                else {}
            ) or {}
            try:
                wal_bytes = int((sqlite_stats or {}).get("wal_bytes") or 0)
                if wal_bytes > 32 * 1024 * 1024:
                    capacity_warnings.append("SQLite WAL 较大，后台维护会在空闲时整理。")
            except Exception:
                pass
            for table_name, threshold, label in (
                ("event_outbox", 1000, "Qt 事件队列历史较多"),
                ("runtime_task_queue", 500, "运行任务队列历史较多"),
                ("append_events", 5000, "剪贴板/事件日志较多"),
            ):
                try:
                    if int(table_counts.get(table_name) or 0) > threshold:
                        capacity_warnings.append(f"{label}，后台清理会逐步回收。")
                except Exception:
                    continue
            return {
                "ok": True,
                "data": {
                    **_queue_stats(),
                    "mock_external": _mock_external_enabled(),
                    "external_guard": _external_guard_status(),
                    "frontend": {
                        "vue_ready": portal_frontend_dist_ready(),
                        "vue_enabled": portal_frontend_dist_enabled(),
                    },
                    "last_loaded_at": last_loaded_at,
                    "last_loaded_ts": last_loaded_ts,
                    "warnings": warnings,
                    "capacity_warnings": capacity_warnings,
                    "job_count": job_count,
                    "job_phase_counts": job_phase_counts,
                    "failed_retryable_count": failed_retryable_count,
                    "failed_non_retryable_count": failed_non_retryable_count,
                    "qt_bridge": {
                        **qt_bridge_payload,
                        "connected": bool(
                            qt_bridge_heartbeat_at and qt_bridge_age_seconds <= 20.0
                        ),
                        "age_seconds": round(qt_bridge_age_seconds, 1),
                    },
                    "job_cleanup": job_cleanup if isinstance(job_cleanup, dict) else {},
                    "preflight": preflight if isinstance(preflight, dict) else {},
                    "token_status": token_status if isinstance(token_status, dict) else {},
                    "sqlite_maintenance": sqlite_maintenance
                    if isinstance(sqlite_maintenance, dict)
                    else {},
                    "sqlite": sqlite_stats if isinstance(sqlite_stats, dict) else {},
                    "schema": schema_status if isinstance(schema_status, dict) else {},
                    "runtime_health": runtime_health if isinstance(runtime_health, dict) else {},
                    "source_snapshot": source_snapshot_stats if isinstance(source_snapshot_stats, dict) else {},
                    "event_snapshot": event_snapshot_stats if isinstance(event_snapshot_stats, dict) else {},
                    "repair_snapshot": repair_snapshot_stats if isinstance(repair_snapshot_stats, dict) else {},
                    "source_type_summary": source_type_stats if isinstance(source_type_stats, dict) else {},
                    "upload_attachments": upload_attachment_stats if isinstance(upload_attachment_stats, dict) else {},
                    "signature_crypto": signature_crypto_stats if isinstance(signature_crypto_stats, dict) else {},
                    "job_batch": self._job_batch_snapshot(),
                    "read_cache": self._read_cache_stats(),
                    "static_cache": self._static_cache_stats(),
                    "singleflight": {
                        "payload_cache_inflight": len(PortalRuntime.payload_cache_inflight),
                        "payload_cache_entries": len(PortalRuntime.payload_cache),
                    },
                    "sse_connections": self._sse_stats(),
                    "recent_failed_jobs": recent_failed_jobs,
                    "slow_jobs": slow_jobs,
                    "time": time.time(),
                },
            }

        @app.get("/api/backend/queues")
        async def backend_queues(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            service = PortalRuntime.service
            job_phase_counts: dict[str, int] = {}
            with suppress(Exception):
                with service._jobs_lock:
                    for job in service._jobs.values():
                        if not isinstance(job, dict):
                            continue
                        phase = str(job.get("phase") or "unknown").strip() or "unknown"
                        job_phase_counts[phase] = job_phase_counts.get(phase, 0) + 1
            return {
                "ok": True,
                "data": {
                    **_queue_stats(),
                    "job_phase_counts": job_phase_counts,
                    "schema": PortalRuntime.state_store.schema_health(),
                    "source_snapshot": PortalRuntime.state_store.source_snapshot_stats(),
                    "event_snapshot": PortalRuntime.state_store.event_month_snapshot_stats(),
                    "repair_snapshot": PortalRuntime.state_store.repair_snapshot_stats(),
                    "qt_active_items": PortalRuntime.state_store.qt_active_items_stats(),
                    "time": time.time(),
                },
            }

        @app.get("/api/backend/perf")
        async def backend_perf(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            qt_bridge = PortalRuntime.state_store.get_backend_runtime("qt_bridge") or {}
            qt_bridge_payload = dict(qt_bridge) if isinstance(qt_bridge, dict) else {}
            return {
                "ok": True,
                "data": {
                    "queues": _queue_stats(),
                    "sqlite": PortalRuntime.state_store.get_database_stats(),
                    "qt_active_items": PortalRuntime.state_store.qt_active_items_stats(),
                    "qt_bridge": qt_bridge_payload,
                    "endpoint_metrics": self._perf_snapshot(),
                    "job_batch": self._job_batch_snapshot(),
                    "read_cache": self._read_cache_stats(),
                    "static_cache": self._static_cache_stats(),
                    "sse_connections": self._sse_stats(),
                    "singleflight": {
                        "payload_cache_inflight": len(PortalRuntime.payload_cache_inflight),
                        "payload_cache_entries": len(PortalRuntime.payload_cache),
                    },
                    "slow_thresholds": {
                        "qt_ui_mutation_ms": 120,
                        "qt_event_ms": 500,
                        "sqlite_write_ms": 100,
                        "remote_http_ms": 3000,
                    },
                    "time": time.time(),
                },
            }

        @app.get("/api/backend/consistency")
        async def backend_consistency(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            scope = str(request.query_params.get("scope") or "ALL").strip() or "ALL"
            try:
                data = await asyncio.to_thread(
                    self._backend_consistency_snapshot,
                    scope,
                )
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.get("/api/backend/notice-diagnostic")
        async def backend_notice_diagnostic(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            query = str(request.query_params.get("query") or "").strip()
            scope = str(request.query_params.get("scope") or "ALL").strip() or "ALL"
            try:
                data = await asyncio.to_thread(
                    self._backend_notice_diagnostic,
                    query,
                    scope,
                )
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.post("/api/backend/notice-projection-repair")
        async def backend_notice_projection_repair(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            scope = str(request.query_params.get("scope") or "ALL").strip() or "ALL"
            try:
                data = await asyncio.to_thread(
                    self._backend_repair_notice_projection,
                    scope,
                )
                PortalRuntime.state_store.put_backend_runtime(
                    "notice_projection_repair",
                    data,
                )
                self._clear_read_cache()
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.post("/api/backend/preflight")
        async def backend_preflight(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                report = await asyncio.to_thread(
                    _build_backend_preflight_report, PortalRuntime.service
                )
                PortalRuntime.state_store.put_backend_runtime("preflight", report)
                return {"ok": True, "data": report}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.post("/api/backend/sqlite/checkpoint")
        async def sqlite_checkpoint(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                data = await asyncio.to_thread(
                    PortalRuntime.state_store.checkpoint_database, truncate=True
                )
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.post("/api/backend/jobs/cleanup")
        async def backend_jobs_cleanup(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                result = await asyncio.to_thread(self._collect_backend_cleanup_payload)
                PortalRuntime.state_store.put_backend_runtime("job_cleanup", result)
                return {"ok": True, "data": result}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.post("/api/backend/mock-pressure")
        async def backend_mock_pressure(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            payload = (
                await self._read_model_request(request, MockPressureRequest)
            ).to_payload()
            count = max(1, min(int(payload.get("count") or 10), 60))
            concurrency = max(1, min(int(payload.get("concurrency") or 5), 10))
            raw_scopes = payload.get("scopes")
            scopes: list[str] = []
            if isinstance(raw_scopes, list):
                scopes = [str(item or "").strip().upper() for item in raw_scopes if str(item or "").strip()]
            elif isinstance(raw_scopes, str):
                scopes = [item.strip().upper() for item in raw_scopes.split(",") if item.strip()]
            per_scope = max(0, min(int(payload.get("per_scope") or 0), 10))
            scenario = str(payload.get("scenario") or "accepted").strip()
            if scenario not in {"accepted", "failed-network", "failed-remote-missing", "mixed"}:
                scenario = "accepted"
            include_site_photos = bool(payload.get("include_site_photos"))
            site_photo_count = max(1, min(int(payload.get("site_photo_count") or 1), 3))
            site_photo_kb = max(1, min(int(payload.get("site_photo_kb") or 32), 512))
            max_submit_average_ms = max(
                1.0,
                min(float(payload.get("max_submit_average_ms") or 300.0), 10000.0),
            )
            max_total_seconds = max(
                1.0,
                min(float(payload.get("max_total_seconds") or 20.0), 600.0),
            )
            max_failed = max(0, min(int(payload.get("max_failed") or 0), 60))
            tool_path = Path(__file__).resolve().parents[1] / "tools" / "mock_lan_portal_pressure.py"
            args = [
                sys.executable,
                os.fspath(tool_path),
                "--count",
                str(count),
                "--concurrency",
                str(concurrency),
                "--scenario",
                scenario,
            ]
            if include_site_photos:
                args.extend(
                    [
                        "--with-site-photos",
                        "--site-photo-count",
                        str(site_photo_count),
                        "--site-photo-kb",
                        str(site_photo_kb),
                    ]
                )
            if scopes and per_scope > 0:
                args.extend(["--scopes", ",".join(scopes), "--per-scope", str(per_scope)])
            run_kwargs = {}
            if os.name == "nt":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = 0  # SW_HIDE
                run_kwargs["startupinfo"] = startupinfo
                run_kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            try:
                with tempfile.TemporaryDirectory(prefix="clipflow_mock_pressure_") as tmp_dir:
                    stdout_path = Path(tmp_dir) / "stdout.txt"
                    stderr_path = Path(tmp_dir) / "stderr.txt"

                    def _run_mock_pressure_subprocess() -> subprocess.CompletedProcess:
                        with stdout_path.open("w", encoding="utf-8", errors="ignore") as stdout_file, stderr_path.open(
                            "w",
                            encoding="utf-8",
                            errors="ignore",
                        ) as stderr_file:
                            completed_process = subprocess.run(
                                args,
                                cwd=os.fspath(Path(__file__).resolve().parents[1]),
                                stdout=stdout_file,
                                stderr=stderr_file,
                                text=True,
                                timeout=60,
                                check=False,
                                **run_kwargs,
                            )
                        completed_process.stdout = stdout_path.read_text(encoding="utf-8", errors="ignore")
                        completed_process.stderr = stderr_path.read_text(encoding="utf-8", errors="ignore")
                        return completed_process

                    completed = await asyncio.to_thread(_run_mock_pressure_subprocess)
            except subprocess.TimeoutExpired:
                return JSONResponse(
                    {"ok": False, "error": "mock 压测超过 60 秒，已中止。"},
                    status_code=504,
                )
            if completed.returncode != 0:
                return JSONResponse(
                    {
                        "ok": False,
                        "error": completed.stderr.strip() or completed.stdout.strip() or "mock 压测失败。",
                    },
                    status_code=500,
                )
            try:
                data = json.loads(completed.stdout or "{}")
            except Exception:
                data = {"raw_output": completed.stdout}
            if isinstance(data, dict) and isinstance(data.get("runs"), list):
                runs = [item for item in data.get("runs") or [] if isinstance(item, dict)]
                if runs:
                    summary = dict(data)
                    flattened = dict(runs[0])
                    flattened["summary"] = summary
                    flattened["ok"] = bool(summary.get("ok", flattened.get("ok", True)))
                    data = flattened
            if isinstance(data, dict):
                data["assessment"] = self._assess_mock_pressure_result(
                    data,
                    max_submit_average_ms=max_submit_average_ms,
                    max_total_seconds=max_total_seconds,
                    max_failed=max_failed,
                )
            return {"ok": True, "data": data}

        @app.get("/api/auth/status")
        async def auth_status(request: Request):
            session = self._current_session(request)
            next_path = str(
                request.query_params.get("next") or self._request_target(request)
            )
            session_id = str(request.cookies.get(AUTH_COOKIE_NAME) or "")
            cache_key = ("auth_status", session_id, next_path)
            cached = self._read_cache_get(cache_key, ttl=2.0, stale_ttl=10.0)
            if cached is not None:
                request.state.cache_hit = True
                return cached
            request.state.cache_miss = True
            payload = {
                "ok": True,
                "data": PortalRuntime.auth_manager.public_status(
                    session,
                    next_path=next_path,
                    redirect_uri=f"{self._request_base_url(request)}/api/auth/feishu/callback",
                ),
            }
            self._read_cache_put(cache_key, payload, ttl=2.0, stale_ttl=10.0)
            return self._json_response(request, session, payload)

        @app.get("/api/jobs/recent")
        async def recent_jobs(
            request: Request,
            phase: str = "",
            limit: int = 50,
            retryable: str = "",
        ):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            if not PortalRuntime.auth_manager.is_admin(session):
                return JSONResponse(
                    {"ok": False, "error": "只有管理员可以查看最近任务。"},
                    status_code=403,
                )
            jobs = []
            service = PortalRuntime.service
            lock = getattr(service, "_jobs_lock", threading.RLock())
            phase_filter = str(phase or "").strip()
            retryable_filter = str(retryable or "").strip().lower()
            limit = max(1, min(int(limit or 50), 200))
            with lock:
                for job in getattr(service, "_jobs", {}).values():
                    if isinstance(job, dict):
                        item = dict(job)
                        if phase_filter and str(item.get("phase") or "") != phase_filter:
                            continue
                        if retryable_filter in {"1", "true", "yes"} and not bool(
                            item.get("error_retryable")
                        ):
                            continue
                        if retryable_filter in {"0", "false", "no"} and bool(
                            item.get("error_retryable")
                        ):
                            continue
                        request_payload = item.get("request") if isinstance(item.get("request"), dict) else {}
                        prepared = item.get("prepared") if isinstance(item.get("prepared"), dict) else {}
                        jobs.append(
                            {
                                "job_id": str(item.get("job_id") or ""),
                                "phase": str(item.get("phase") or ""),
                                "created_at": str(item.get("created_at") or ""),
                                "updated_at": str(item.get("updated_at") or ""),
                                "work_type": str(
                                    prepared.get("work_type")
                                    or request_payload.get("work_type")
                                    or ""
                                ),
                                "action": str(
                                    prepared.get("action")
                                    or request_payload.get("action")
                                    or ""
                                ),
                                "record_id": str(item.get("record_id") or ""),
                                "active_item_id": str(item.get("active_item_id") or ""),
                                "message_sent": bool(item.get("message_sent")),
                                "error": str(item.get("error") or item.get("upload_message") or ""),
                                "error_category": str(item.get("error_category") or ""),
                                "error_retryable": bool(item.get("error_retryable")),
                                "qt_queue_position": int(item.get("qt_queue_position") or 0),
                                "upload_queue_position": int(item.get("upload_queue_position") or 0),
                                "can_retry": str(item.get("phase") or "") == "failed"
                                and bool(item.get("error_retryable")),
                                "can_clear": str(item.get("phase") or "") == "failed",
                                "can_mark_stuck_failed": str(item.get("phase") or "")
                                not in {"success", "failed", ""},
                            }
                        )
            jobs.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
            return {"ok": True, "data": {"items": jobs[:limit]}}

        @app.post("/api/jobs/{job_id}/clear")
        async def clear_job(job_id: str, request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            normalized_job_id = str(job_id or "").strip()
            job = PortalRuntime.service.get_job(normalized_job_id)
            if not job:
                return JSONResponse(
                    {"ok": False, "error": "任务不存在或已清理。"},
                    status_code=404,
                )
            if str(job.get("phase") or "") != "failed":
                return JSONResponse(
                    {"ok": False, "error": "只能清理失败任务。"},
                    status_code=400,
                )
            PortalRuntime.service.delete_action_job(normalized_job_id)
            return {"ok": True, "data": {"job_id": normalized_job_id, "cleared": True}}

        @app.post("/api/jobs/{job_id}/retry")
        async def retry_job(job_id: str, request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            normalized_job_id = str(job_id or "").strip()
            try:
                job = PortalRuntime.service.retry_action_job(normalized_job_id)
                PortalRuntime.clear_payload_cache()
                self._clear_read_cache()
                PortalRuntime.enqueue_initial_message_or_upload_job(normalized_job_id)
                return {
                    "ok": True,
                    "data": {
                        "job_id": normalized_job_id,
                        "phase": job.get("phase") or "accepted",
                        "retry_count": int(job.get("retry_count") or 0),
                    },
                }
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/jobs/{job_id}/mark-stuck-failed")
        async def mark_stuck_job_failed(job_id: str, request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            payload = await self._read_model_request(request, JobMarkStuckFailedRequest)
            normalized_job_id = str(job_id or "").strip()
            job = PortalRuntime.service.get_job(normalized_job_id)
            if not job:
                return JSONResponse(
                    {"ok": False, "error": "任务不存在或已清理。"},
                    status_code=404,
                )
            phase = str(job.get("phase") or "").strip()
            if phase in {"success", "failed"}:
                return JSONResponse(
                    {"ok": False, "error": "只能标记未完成任务。"},
                    status_code=400,
                )
            reason = str(payload.reason or "").strip() or "管理员手动标记卡住任务，请核对后重试。"
            PortalRuntime.service.mark_job(
                normalized_job_id,
                phase="failed",
                error=reason,
                error_category="admin_stuck_reset",
                error_retryable=True,
            )
            for queue_name in ("message", "qt_action", "upload_wait"):
                try:
                    PortalRuntime.state_store.mark_runtime_queue_item(
                        queue_name,
                        normalized_job_id,
                        "failed",
                        error=reason,
                    )
                except Exception:
                    pass
            PortalRuntime.clear_payload_cache()
            self._clear_read_cache()
            return {
                "ok": True,
                "data": {
                    "job_id": normalized_job_id,
                    "phase": "failed",
                    "error": reason,
                    "error_category": "admin_stuck_reset",
                    "error_retryable": True,
                },
            }

        @app.get("/api/jobs/stream")
        async def jobs_stream(request: Request):
            return StreamingResponse(
                self._job_stream(request),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-store"},
            )

        @app.get("/api/jobs/batch")
        async def get_jobs_batch(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            raw_ids = str(request.query_params.get("ids") or "")
            job_ids: list[str] = []
            seen: set[str] = set()
            for raw_id in raw_ids.split(","):
                job_id = str(raw_id or "").strip()
                if not job_id or job_id in seen:
                    continue
                seen.add(job_id)
                job_ids.append(job_id)
                if len(job_ids) >= 100:
                    break
            items: list[dict] = []
            missing: list[str] = []
            denied: list[str] = []
            for job_id in job_ids:
                job = PortalRuntime.service.get_job(job_id)
                if not job:
                    missing.append(job_id)
                    continue
                if not self._job_visible_to_session(job, session):
                    denied.append(job_id)
                    continue
                payload = dict(job)
                payload["job_id"] = str(payload.get("job_id") or job_id)
                items.append(payload)
            self._record_job_batch_stats(
                requested=len(job_ids),
                returned=len(items),
                missing=len(missing),
                denied=len(denied),
            )
            return {
                "ok": True,
                "data": {
                    "items": items,
                    "missing": missing,
                    "denied": denied,
                    "count": len(items),
                },
            }

        @app.get("/api/jobs/{job_id}")
        async def get_job(job_id: str, request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            job = PortalRuntime.service.get_job(str(job_id or "").strip())
            if not job:
                return JSONResponse(
                    {"ok": False, "error": "任务状态已丢失，请核对多维后重试。"},
                    status_code=404,
                )
            if not self._job_visible_to_session(job, session):
                return JSONResponse(
                    {"ok": False, "error": "无权查看该任务。"},
                    status_code=403,
                )
            job = dict(job)
            job["job_id"] = str(job.get("job_id") or job_id)
            return {"ok": True, "data": job}

        @app.get("/api/bootstrap")
        async def bootstrap(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                self._ensure_source_snapshot_background()
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                open_id = str((session.get("user") or {}).get("open_id") or "")
                data = await asyncio.to_thread(
                    self._cached_service_payload,
                    (
                        "bootstrap",
                        open_id,
                        scope,
                        PortalRuntime._ongoing_items_marker(ongoing),
                    ),
                    lambda: PortalRuntime.service.get_bootstrap(
                        scope=scope, ongoing_items=ongoing
                    ),
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/scope-overview")
        async def scope_overview(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                self._ensure_source_snapshot_background()
                ongoing = self._get_ongoing("ALL")
                self._reconcile_orphan_started_items("ALL", ongoing)
                allowed_options = PortalRuntime.auth_manager.filter_scope_options(
                    SCOPE_OPTIONS, session
                )
                allowed_scopes = [
                    str(option.get("value") or "")
                    for option in allowed_options
                    if str(option.get("value") or "").strip()
                ]
                open_id = str((session.get("user") or {}).get("open_id") or "")
                data = await asyncio.to_thread(
                    self._cached_service_payload,
                    (
                        "scope-overview",
                        open_id,
                        tuple(sorted(allowed_scopes)),
                        PortalRuntime._ongoing_items_marker(ongoing),
                    ),
                    lambda: PortalRuntime.service.get_scope_overview(
                        ongoing_items=ongoing,
                        scopes=allowed_scopes,
                        include_prepared=False,
                    ),
                )
                data = PortalRuntime.auth_manager.filter_scope_overview(data, session)
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.get("/api/records")
        @app.get("/api/workbench")
        async def workbench(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                self._ensure_source_snapshot_background()
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                month = str(request.query_params.get("month") or "")
                specialty = str(request.query_params.get("specialty") or "")
                search = str(request.query_params.get("search") or "")
                work_type = str(request.query_params.get("work_type") or "").strip()
                records_page = str(
                    request.query_params.get("records_page")
                    or request.query_params.get("pending_page")
                    or request.query_params.get("page")
                    or "1"
                )
                records_page_size = str(
                    request.query_params.get("records_page_size")
                    or request.query_params.get("pending_page_size")
                    or request.query_params.get("page_size")
                    or "0"
                )
                ongoing_page = str(request.query_params.get("ongoing_page") or "1")
                ongoing_page_size = str(request.query_params.get("ongoing_page_size") or "0")
                raw_sections = str(request.query_params.get("sections") or "").strip()
                sections = tuple(
                    item.strip().lower()
                    for item in raw_sections.split(",")
                    if item.strip()
                )
                prefetch_only = str(request.query_params.get("prefetch") or "").strip().lower() in {
                    "1",
                    "true",
                    "yes",
                    "on",
                }
                open_id = str((session.get("user") or {}).get("open_id") or "")
                payload_key = "records" if request.url.path == "/api/records" else "workbench"
                if payload_key == "records":
                    work_type = ""
                    sections = ()
                payload = await asyncio.to_thread(
                    self._cached_service_payload,
                    (
                        payload_key,
                        open_id,
                        scope,
                        month,
                        specialty,
                        search,
                        work_type,
                        sections,
                        records_page,
                        records_page_size,
                        ongoing_page,
                        ongoing_page_size,
                        PortalRuntime._ongoing_items_marker(ongoing),
                    ),
                    lambda: PortalRuntime.service.query_records(
                        month=month,
                        specialty=specialty,
                        search=search,
                        scope=scope,
                        ongoing_items=ongoing,
                        work_type=work_type,
                        sections=sections,
                        records_page=records_page,
                        records_page_size=records_page_size,
                        ongoing_page=ongoing_page,
                        ongoing_page_size=ongoing_page_size,
                    ),
                )
                if payload_key == "workbench" and prefetch_only:
                    return self._json_ok(request, session, {"prefetched": True})
                if (
                    payload_key == "workbench"
                    and work_type in NOTICE_TYPE_BY_WORK_TYPE
                    and isinstance(payload, dict)
                ):
                    filtered_payload = dict(payload)
                    filtered_payload["records"] = [
                        item
                        for item in (payload.get("records") or [])
                        if str((item or {}).get("work_type") or "maintenance") == work_type
                    ]
                    type_count = 0
                    with suppress(Exception):
                        type_count = int(float((payload.get("record_type_counts") or {}).get(work_type, 0)))
                    if not filtered_payload["records"] and type_count > 0:
                        broad_payload = await asyncio.to_thread(
                            PortalRuntime.service.query_records,
                            month=month,
                            specialty=specialty,
                            search=search,
                            scope=scope,
                            ongoing_items=ongoing,
                            work_type="",
                            sections=sections,
                            records_page=1,
                            records_page_size=0,
                            ongoing_page=1,
                            ongoing_page_size=0,
                        )
                        if isinstance(broad_payload, dict):
                            filtered_payload["records"] = [
                                item
                                for item in (broad_payload.get("records") or [])
                                if str((item or {}).get("work_type") or "maintenance") == work_type
                            ]
                            filtered_payload["record_type_counts"] = (
                                broad_payload.get("record_type_counts")
                                or filtered_payload.get("record_type_counts")
                                or {}
                            )
                    payload = filtered_payload
                return self._json_ok(request, session, payload)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/workbench/closed")
        async def workbench_closed(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                ongoing = self._get_ongoing(scope)
                payload = await asyncio.to_thread(
                    PortalRuntime.service.get_workbench_closed_items,
                    scope=scope,
                    work_type=str(request.query_params.get("work_type") or ""),
                    ongoing_items=ongoing,
                )
                return self._json_ok(request, session, payload)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/history-summary")
        async def history_summary(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                payload = await asyncio.to_thread(
                    PortalRuntime.service.get_history_summary,
                    scope=scope,
                    month=str(request.query_params.get("month") or ""),
                    work_type=str(request.query_params.get("work_type") or "all"),
                )
                return self._json_ok(request, session, payload)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/notice-work-type-override")
        async def notice_work_type_override(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(
                        request, NoticeWorkTypeOverrideRequest
                    )
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                result = await asyncio.to_thread(
                    PortalRuntime.service.set_notice_work_type_override,
                    record_id=str(payload.get("record_id") or ""),
                    source_work_type=str(
                        payload.get("source_work_type") or "maintenance"
                    ),
                    target_work_type=str(payload.get("target_work_type") or "change"),
                    scope=scope,
                    updated_by=str(user.get("open_id") or ""),
                )
                PortalRuntime.clear_payload_cache()
                self._clear_read_cache()
                return self._json_ok(request, session, result)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/engineer/mop/bootstrap")
        async def engineer_mop_bootstrap(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                self._ensure_source_snapshot_background()
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                ongoing = self._get_ongoing(scope)
                data = await asyncio.to_thread(
                    PortalRuntime.service.engineer_mop_bootstrap,
                    scope=scope,
                    ongoing_items=ongoing,
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/engineer/mop/bind")
        async def engineer_mop_bind(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        EngineerMopBindRequest,
                        max_bytes=512 * 1024,
                    )
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                payload["scope"] = scope
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                data = await asyncio.to_thread(
                    PortalRuntime.service.bind_engineer_mop_notice,
                    payload=payload,
                    updated_by=str(user.get("open_id") or ""),
                )
                PortalRuntime.clear_payload_cache()
                self._clear_read_cache()
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/engineer/mop/upload-local")
        async def engineer_mop_upload_local(
            request: Request,
            file: UploadFile = File(...),
            scope: str = Form("ALL"),
            source_record_id: str = Form(""),
            notice_key: str = Form(""),
            notice_title: str = Form(""),
        ):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                normalized_scope = self._authorized_scope_or_error(session, scope or "ALL")
                content = await file.read(21 * 1024 * 1024)
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                data = await asyncio.to_thread(
                    PortalRuntime.service.upload_engineer_mop_local_file,
                    scope=normalized_scope,
                    source_record_id=source_record_id,
                    notice_key=notice_key,
                    notice_title=notice_title,
                    file_name=str(file.filename or ""),
                    content=content,
                    created_by_openid=str(user.get("open_id") or ""),
                )
                PortalRuntime.clear_payload_cache()
                self._clear_read_cache()
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)
            finally:
                with suppress(Exception):
                    await file.close()

        @app.get("/api/engineer/mop/preview")
        async def engineer_mop_preview(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.preview_engineer_mop_attachment,
                    scope=str(request.query_params.get("scope") or "ALL"),
                    mop_record_id=str(request.query_params.get("mop_record_id") or ""),
                    file_token=str(request.query_params.get("file_token") or ""),
                    file_name=str(request.query_params.get("file_name") or ""),
                    upload_id=str(request.query_params.get("upload_id") or ""),
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/engineer/mop/fill")
        async def engineer_mop_fill(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        EngineerMopFillRequest,
                        max_bytes=4 * 1024 * 1024,
                    )
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.fill_engineer_mop_file,
                    **engineer_mop_fill_kwargs_from_payload(payload, scope=scope),
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/engineer/mop/upload-signed")
        async def engineer_mop_upload_signed(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        EngineerMopUploadSignedRequest,
                        max_bytes=4 * 1024 * 1024,
                    )
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                data = await asyncio.to_thread(
                    PortalRuntime.service.upload_signed_engineer_mop_file,
                    **engineer_mop_upload_signed_kwargs_from_payload(
                        payload,
                        scope=scope,
                        operator_open_id=str(user.get("open_id") or ""),
                        operator_name=str(user.get("name") or ""),
                    ),
                )
                PortalRuntime.clear_payload_cache()
                self._clear_read_cache()
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/engineer/mop/reset")
        async def engineer_mop_reset(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        EngineerMopResetRequest,
                        max_bytes=256 * 1024,
                    )
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.reset_engineer_mop_file,
                    scope=scope,
                    filled_file_path=str(payload.get("filled_file_path") or ""),
                    mop_record_id=str(payload.get("mop_record_id") or ""),
                    file_token=str(payload.get("file_token") or ""),
                    file_name=str(payload.get("file_name") or ""),
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/signatures/people")
        async def signatures_people(request: Request):
            try:
                session = self._current_session(request)
                record_id = str(request.query_params.get("record_id") or "")
                link_token = str(request.query_params.get("token") or "")
                if session is None:
                    if not PortalRuntime.service.validate_signature_link_token(
                        record_id=record_id,
                        token=link_token,
                    ):
                        return JSONResponse(
                            {"ok": False, "error": "签名链接无效或已过期。"},
                            status_code=403,
                        )
                data = await asyncio.to_thread(
                    PortalRuntime.service.signature_people,
                    scope=str(request.query_params.get("scope") or ""),
                    query=str(request.query_params.get("q") or ""),
                    record_id=record_id,
                    link_token=link_token if session is None else "",
                    notice_key=str(request.query_params.get("notice_key") or ""),
                    operator_open_id=str((session.get("user") or {}).get("open_id") or "") if isinstance(session, dict) and isinstance(session.get("user"), dict) else "",
                    limit=int(str(request.query_params.get("limit") or "80") or 80),
                    refresh=str(request.query_params.get("refresh") or "").lower() in {"1", "true", "yes"},
                )
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.get("/api/signatures/image")
        async def signatures_image(request: Request):
            try:
                session = self._current_session(request)
                record_id = str(request.query_params.get("record_id") or "")
                if session is None and not PortalRuntime.service.validate_signature_link_token(
                    record_id=record_id,
                    token=str(request.query_params.get("token") or ""),
                ):
                    return JSONResponse(
                        {"ok": False, "error": "签名链接无效或已过期。"},
                        status_code=404,
                    )
                content, content_type = await asyncio.to_thread(
                    PortalRuntime.service.signature_image_bytes,
                    record_id=record_id,
                )
                return Response(
                    content=content,
                    media_type=content_type,
                    headers={
                        "Cache-Control": "private, max-age=300",
                    },
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=404)

        @app.get("/api/signatures/usage-confirm")
        async def signatures_usage_confirm(request: Request):
            try:
                data = await asyncio.to_thread(
                    PortalRuntime.service.signature_usage_confirmation,
                    token=str(request.query_params.get("token") or ""),
                )
                return self._signature_usage_confirmation_page(
                    token=str(request.query_params.get("token") or ""),
                    data=data,
                )
            except Exception as exc:
                return self._html_message(400, "签名确认失败", str(exc))

        @app.post("/api/signatures/usage-confirm")
        async def signatures_usage_confirm_decide(request: Request):
            try:
                body = (await request.body()).decode("utf-8", errors="ignore")
                form = parse_qs(body)
                token = (form.get("token") or [""])[0]
                decision = (form.get("decision") or form.get("action") or [""])[0]
                data = await asyncio.to_thread(
                    PortalRuntime.service.decide_signature_usage,
                    token=token,
                    decision=decision,
                )
                return self._signature_usage_confirmation_page(
                    token=token,
                    data=data,
                    result_message="已确认允许使用签名。"
                    if str(data.get("status") or "") == "confirmed"
                    else "已拒绝本次使用签名。",
                )
            except Exception as exc:
                return self._html_message(400, "签名确认失败", str(exc))

        @app.get("/api/signatures/temporary/session")
        async def temporary_signature_session(request: Request):
            try:
                data = await asyncio.to_thread(
                    PortalRuntime.service.temporary_signature_session,
                    temp_id=str(request.query_params.get("temporary_id") or ""),
                    token=str(request.query_params.get("token") or ""),
                )
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/signatures/temporary/image")
        async def temporary_signature_image(request: Request):
            try:
                session = self._current_session(request)
                temp_id = str(request.query_params.get("temporary_id") or "")
                record_id = str(request.query_params.get("record_id") or "")
                token = str(request.query_params.get("token") or "")
                if record_id:
                    if session is None:
                        return self._auth_required_response()
                    requested_scope = str(request.query_params.get("scope") or "").strip()
                    if requested_scope:
                        self._authorized_scope_or_error(session, requested_scope)
                    content, content_type = await asyncio.to_thread(
                        PortalRuntime.service.external_signature_image_bytes,
                        record_id=record_id,
                    )
                elif session is None:
                    await asyncio.to_thread(
                        PortalRuntime.service.temporary_signature_session,
                        temp_id=temp_id,
                        token=token,
                    )
                    content, content_type = await asyncio.to_thread(
                        PortalRuntime.service.temporary_signature_image_bytes,
                        temp_id=temp_id,
                    )
                else:
                    temp_session = await asyncio.to_thread(
                        PortalRuntime.state_store.get_mop_temporary_signature_session,
                        temp_id=temp_id,
                    )
                    if not temp_session:
                        return JSONResponse(
                            {"ok": False, "error": "临时签名记录不存在。"},
                            status_code=404,
                        )
                    self._authorized_scope_or_error(
                        session,
                        str(temp_session.get("scope") or "ALL"),
                    )
                    content, content_type = await asyncio.to_thread(
                        PortalRuntime.service.temporary_signature_image_bytes,
                        temp_id=temp_id,
                    )
                return Response(
                    content=content,
                    media_type=content_type,
                    headers={"Cache-Control": "private, max-age=300"},
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=404)

        @app.post("/api/signatures/save")
        async def signatures_save(request: Request):
            try:
                session = self._current_session(request)
                payload = (
                    await self._read_model_request(
                        request,
                        SignatureSaveRequest,
                        max_bytes=4 * 1024 * 1024,
                    )
                ).to_payload()
                record_id = str(payload.get("record_id") or "")
                link_token = str(payload.get("token") or "")
                if session is None:
                    if not PortalRuntime.service.validate_signature_link_token(
                        record_id=record_id,
                        token=link_token,
                    ):
                        return JSONResponse(
                            {"ok": False, "error": "签名链接无效或已过期。"},
                            status_code=403,
                        )
                data = await asyncio.to_thread(
                    PortalRuntime.service.save_signature_for_person,
                    record_id=record_id,
                    signature_png=str(payload.get("signature_png") or ""),
                    signer_name=str(payload.get("signer_name") or ""),
                    link_token=link_token if session is None else "",
                    operator_open_id=str((session or {}).get("open_id") or ""),
                    operator_name=str((session or {}).get("name") or ""),
                )
                if session is None:
                    await asyncio.to_thread(
                        PortalRuntime.service.mark_signature_link_token_used,
                        record_id=record_id,
                        token=link_token,
                    )
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/signatures/external/save")
        async def external_signatures_save(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        ExternalSignatureSaveRequest,
                        max_bytes=4 * 1024 * 1024,
                    )
                ).to_payload()
                data = await asyncio.to_thread(
                    PortalRuntime.service.save_external_signature_for_person,
                    record_id=str(payload.get("record_id") or ""),
                    signature_png=str(payload.get("signature_png") or ""),
                    signer_name=str(payload.get("signer_name") or ""),
                )
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/signatures/temporary/create")
        async def temporary_signature_create(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        TemporarySignatureCreateRequest,
                        max_bytes=64 * 1024,
                    )
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session,
                    str(payload.get("scope") or "ALL"),
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                data = await asyncio.to_thread(
                    PortalRuntime.service.create_temporary_signature_session,
                    scope=scope,
                    notice_key=str(payload.get("notice_key") or ""),
                    role=str(payload.get("role") or "implementer"),
                    notice_title=str(payload.get("notice_title") or ""),
                    specialty=str(payload.get("specialty") or ""),
                    display_name=str(payload.get("display_name") or ""),
                    created_by=str(user.get("open_id") or ""),
                )
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/signatures/temporary/save")
        async def temporary_signature_save(request: Request):
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        TemporarySignatureSaveRequest,
                        max_bytes=4 * 1024 * 1024,
                    )
                ).to_payload()
                temp_id = str(payload.get("temporary_id") or "")
                token = str(payload.get("token") or "")
                if not token:
                    session = self._current_session(request)
                    if session is None:
                        return self._auth_required_response()
                    temp_session = PortalRuntime.state_store.get_mop_temporary_signature_session(
                        temp_id=temp_id,
                    )
                    if not temp_session:
                        raise PortalError("临时签名记录不存在。")
                    self._authorized_scope_or_error(
                        session,
                        str(temp_session.get("scope") or "ALL"),
                    )
                data = await asyncio.to_thread(
                    PortalRuntime.service.save_temporary_signature,
                    temp_id=temp_id,
                    token=token,
                    signature_png=str(payload.get("signature_png") or ""),
                )
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/signatures/send-link")
        async def signatures_send_link(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        SignatureSendLinkRequest,
                        max_bytes=64 * 1024,
                    )
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session,
                    str(payload.get("scope") or "ALL"),
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.build_signature_link_message,
                    record_id=str(payload.get("record_id") or ""),
                    signer_name=str(payload.get("signer_name") or ""),
                    scope=scope,
                    request_base_url=str(payload.get("request_base_url") or "")
                    or self._request_base_url(request),
                    created_by=str((session.get("user") or {}).get("open_id") if isinstance(session.get("user"), dict) else ""),
                )
                ok, message, results = await asyncio.to_thread(
                    _send_text_to_open_ids_guarded,
                    str(data.get("text") or ""),
                    [str(data.get("open_id") or "")],
                )
                if not ok:
                    return JSONResponse(
                        {
                            "ok": False,
                            "error": message or "签名链接发送失败。",
                            "data": {
                                "person": data.get("person") or {},
                                "link_url": data.get("link_url") or "",
                                "results": results,
                            },
                        },
                        status_code=400,
                    )
                return {
                    "ok": True,
                    "data": {
                        "person": data.get("person") or {},
                        "link_url": data.get("link_url") or "",
                        "message": message,
                        "results": results,
                    },
                }
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/signatures/usage-confirmations/send")
        async def signatures_usage_confirmations_send(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        SignatureUsageConfirmationSendRequest,
                        max_bytes=256 * 1024,
                    )
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session,
                    str(payload.get("scope") or "ALL"),
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                data = await asyncio.to_thread(
                    PortalRuntime.service.build_signature_usage_confirmation_messages,
                    scope=scope,
                    notice_key=str(payload.get("notice_key") or ""),
                    notice_title=str(payload.get("notice_title") or ""),
                    signatures=[
                        item for item in (payload.get("signatures") or [])
                        if isinstance(item, dict)
                    ],
                    mop_attachment_name=str(payload.get("mop_attachment_name") or ""),
                    request_base_url=str(payload.get("request_base_url") or "")
                    or self._request_base_url(request),
                    operator_open_id=str(user.get("open_id") or ""),
                    operator_name=str(user.get("name") or user.get("en_name") or ""),
                )
                messages = list(data.get("messages") or [])
                results: list[dict[str, Any]] = []
                for item in messages:
                    open_id = str(item.get("open_id") or "")
                    ok, message, send_results = await asyncio.to_thread(
                        _send_text_to_open_ids_guarded,
                        str(item.get("text") or ""),
                        [open_id],
                    )
                    send_result = (send_results or [{}])[0] if send_results else {}
                    results.append(
                        {
                            "record_id": item.get("record_id") or "",
                            "open_id": open_id,
                            "ok": bool(ok and send_result.get("ok", ok)),
                            "message": str(send_result.get("message") or message or ""),
                        }
                    )
                failed = [item for item in results if not item.get("ok")]
                return {
                    "ok": True,
                    "data": {
                        "sent_count": len(results) - len(failed),
                        "failed_count": len(failed),
                        "results": results,
                        "skipped": data.get("skipped") or [],
                    },
                }
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.get("/api/signatures/temporary/list")
        async def temporary_signature_list(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session,
                    str(request.query_params.get("scope") or "ALL"),
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                data = await asyncio.to_thread(
                    PortalRuntime.service.list_temporary_signatures,
                    scope=scope,
                    notice_key=str(request.query_params.get("notice_key") or ""),
                    created_by=str(user.get("open_id") or ""),
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/signatures/temporary/people")
        async def temporary_signature_people(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session,
                    str(request.query_params.get("scope") or "ALL"),
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.temporary_signature_people,
                    scope=scope,
                    query=str(request.query_params.get("q") or ""),
                    limit=int(str(request.query_params.get("limit") or "80") or 80),
                    refresh=str(request.query_params.get("refresh") or "").lower() in {"1", "true", "yes"},
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/signatures/temporary/send-link")
        async def temporary_signature_send_link(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        TemporarySignatureSendLinkRequest,
                        max_bytes=64 * 1024,
                    )
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session,
                    str(payload.get("scope") or "ALL"),
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                temporary_id = str(payload.get("temporary_id") or "").strip()
                if temporary_id:
                    temp_session = PortalRuntime.state_store.get_mop_temporary_signature_session(
                        temp_id=temporary_id,
                    )
                    if not temp_session:
                        raise PortalError("临时签名记录不存在。")
                    self._authorized_scope_or_error(
                        session,
                        str(temp_session.get("scope") or scope or "ALL"),
                    )
                    data = await asyncio.to_thread(
                        PortalRuntime.service.build_existing_temporary_signature_link_message,
                        temp_id=temporary_id,
                        request_base_url=str(payload.get("request_base_url") or "")
                        or self._request_base_url(request),
                    )
                else:
                    data = await asyncio.to_thread(
                        PortalRuntime.service.build_temporary_signature_link_message,
                        scope=scope,
                        notice_key=str(payload.get("notice_key") or ""),
                        role=str(payload.get("role") or "implementer"),
                        recipient_open_ids=list(payload.get("recipient_open_ids") or []),
                        notice_title=str(payload.get("notice_title") or ""),
                        specialty=str(payload.get("specialty") or ""),
                        display_name=str(payload.get("display_name") or ""),
                        request_base_url=str(payload.get("request_base_url") or "")
                        or self._request_base_url(request),
                        created_by=str(user.get("open_id") or ""),
                    )
                open_ids = [
                    str(item or "").strip()
                    for item in (data.get("open_ids") or [])
                    if str(item or "").strip()
                ]
                ok, message, results = await asyncio.to_thread(
                    _send_text_to_open_ids_guarded,
                    str(data.get("text") or ""),
                    open_ids,
                )
                if not ok:
                    with suppress(Exception):
                        PortalRuntime.state_store.update_mop_temporary_signature_session(
                            temp_id=str(data.get("temp_id") or ""),
                            status="failed",
                            payload_patch={"send_error": message or "签名链接发送失败。"},
                        )
                    return JSONResponse(
                        {
                            "ok": False,
                            "error": message or "其他人员签名链接发送失败。",
                            "data": {
                                "signature": data.get("signature") or {},
                                "link_url": data.get("link_url") or "",
                                "results": results,
                            },
                        },
                        status_code=400,
                    )
                return {
                    "ok": True,
                    "data": {
                        "signature": data.get("signature") or {},
                        "link_url": data.get("link_url") or "",
                        "message": message,
                        "results": results,
                    },
                }
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.get("/api/admin/mop-settings")
        async def admin_mop_settings(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                settings = PortalRuntime.service._engineer_mop_settings()
                return {
                    "ok": True,
                    "data": {
                        "mop_app_token": settings.get("app_token", ""),
                        "mop_table_id": settings.get("table_id", ""),
                        "mop_view_id": settings.get("view_id", ""),
                        "mop_title_field": settings.get("title_field", "文件名"),
                        "mop_attachment_field": settings.get("attachment_field", "文件"),
                    },
                }
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.post("/api/admin/mop-settings")
        async def save_admin_mop_settings(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        EngineerMopSettingsSaveRequest,
                        max_bytes=64 * 1024,
                    )
                ).to_payload()
                values = {
                    "mop_app_token": str(payload.get("mop_app_token") or "").strip(),
                    "mop_table_id": str(payload.get("mop_table_id") or "").strip(),
                    "mop_view_id": str(payload.get("mop_view_id") or "").strip(),
                    "mop_title_field": str(payload.get("mop_title_field") or "文件名").strip() or "文件名",
                    "mop_attachment_field": str(payload.get("mop_attachment_field") or "文件").strip() or "文件",
                }
                PortalRuntime.state_store.put_settings(values)
                PortalRuntime.service.clear_engineer_mop_cache()
                PortalRuntime.clear_payload_cache()
                self._clear_read_cache()
                return {"ok": True, "data": values}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.post("/api/notice-memory/import")
        async def notice_memory_import(request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        NoticeMemoryImportRequest,
                        max_bytes=2 * 1024 * 1024,
                    )
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                result = await asyncio.to_thread(
                    PortalRuntime.service.import_historical_notice_memory,
                    text=str(payload.get("text") or ""),
                    scope=scope,
                    allowed_scopes=PortalRuntime.auth_manager.session_scopes(session),
                    is_admin=PortalRuntime.auth_manager.is_admin(session),
                    imported_by=str(user.get("open_id") or ""),
                )
                PortalRuntime.clear_payload_cache()
                self._clear_read_cache()
                return self._json_ok(request, session, result)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/admin/notice-memory/history-scan")
        async def admin_notice_memory_history_scan(request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        NoticeMemoryHistoryScanRequest,
                        max_bytes=256 * 1024,
                    )
                ).to_payload()
                result = await asyncio.to_thread(
                    PortalRuntime.service.scan_historical_notice_memory_candidates,
                    work_types=payload.get("work_types"),
                    months=int(payload.get("months") or 3),
                )
                return self._json_ok(request, session, result)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/admin/notice-memory/history-save")
        async def admin_notice_memory_history_save(request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        NoticeMemoryHistorySaveRequest,
                        max_bytes=4 * 1024 * 1024,
                    )
                ).to_payload()
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                result = await asyncio.to_thread(
                    PortalRuntime.service.save_historical_notice_memory_matches,
                    matches=payload.get("matches") or [],
                    imported_by=str(user.get("open_id") or ""),
                )
                PortalRuntime.clear_payload_cache()
                self._clear_read_cache()
                return self._json_ok(request, session, result)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/refresh")
        async def refresh_sources(request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                refresh_result = PortalRuntime.request_source_refresh(force=True)
                self._clear_read_cache()
                refreshed = bool(refresh_result.get("refreshed", False))
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing, force=True)
                data = await asyncio.to_thread(
                    PortalRuntime.service.get_bootstrap,
                    scope=scope,
                    ongoing_items=ongoing,
                )
                data["source_refresh_triggered"] = refreshed
                data.update(refresh_result)
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/repair-refresh")
        async def repair_refresh(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                refresh_result = await asyncio.to_thread(
                    PortalRuntime._refresh_repair_source_singleflight
                )
                self._clear_read_cache()
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                data = await asyncio.to_thread(
                    PortalRuntime.service.get_bootstrap,
                    scope=scope,
                    ongoing_items=ongoing,
                )
                data.update(refresh_result)
                data["repair_source_refreshed"] = True
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/change-refresh")
        async def change_refresh(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                refresh_result = await asyncio.to_thread(
                    PortalRuntime.request_change_source_refresh
                )
                self._clear_read_cache()
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                data = await asyncio.to_thread(
                    PortalRuntime.service.get_bootstrap,
                    scope=scope,
                    ongoing_items=ongoing,
                )
                data.update(refresh_result)
                data["change_source_refreshed"] = True
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/events/monthly")
        async def events_monthly(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                month = str(request.query_params.get("month") or "").strip()
                data = await asyncio.to_thread(
                    PortalRuntime.service.get_event_monthly_snapshot,
                    scope=scope,
                    month=month,
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/events/overview")
        async def events_overview(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                month = str(request.query_params.get("month") or "").strip()
                data = await asyncio.to_thread(
                    PortalRuntime.service.get_event_monthly_overview,
                    month=month,
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.post("/api/events/refresh")
        async def events_refresh(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                month = str(request.query_params.get("month") or "").strip()
                refresh_result = await asyncio.to_thread(
                    PortalRuntime.request_event_month_refresh,
                    month,
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.get_event_monthly_snapshot,
                    scope=scope,
                    month=month,
                )
                data.update(refresh_result)
                data["event_source_refreshed"] = True
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/events/transfer-repair")
        async def events_transfer_repair(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, EventTransferRepairRequest)
                ).to_payload()
                self._authorized_scope_or_error(session, payload.get("scope") or "ALL")
                data = await asyncio.to_thread(
                    PortalRuntime.service.mark_event_transferred_to_repair,
                    record_id=str(payload.get("record_id") or ""),
                    month=str(payload.get("month") or ""),
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.get("/api/repair-management/event-candidates")
        async def repair_management_event_candidates(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                month = str(request.query_params.get("month") or "").strip()
                query = str(request.query_params.get("q") or "").strip()
                try:
                    limit = int(request.query_params.get("limit") or 50)
                except ValueError:
                    limit = 50
                data = await asyncio.to_thread(
                    PortalRuntime.service.list_repair_management_event_candidates,
                    scope=scope,
                    month=month,
                    query=query,
                    limit=limit,
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/repair-management/event-prefill")
        async def repair_management_event_prefill(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.repair_management_event_prefill,
                    scope=scope,
                    record_id=str(request.query_params.get("record_id") or ""),
                    month=str(request.query_params.get("month") or ""),
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.get("/api/repair-management/repair-candidates")
        async def repair_management_repair_candidates(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                try:
                    limit = int(request.query_params.get("limit") or 80)
                except ValueError:
                    limit = 80
                data = await asyncio.to_thread(
                    PortalRuntime.service.list_repair_management_repair_candidates,
                    scope=scope,
                    event_record_id=str(request.query_params.get("event_record_id") or ""),
                    month=str(request.query_params.get("month") or ""),
                    query=str(request.query_params.get("q") or ""),
                    limit=limit,
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.get("/api/repair-management/cmdb-candidates")
        async def repair_management_cmdb_candidates(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                try:
                    limit = int(request.query_params.get("limit") or 160)
                except ValueError:
                    limit = 160
                data = await asyncio.to_thread(
                    PortalRuntime.service.list_repair_management_cmdb_candidates,
                    scope=scope,
                    query=str(request.query_params.get("q") or ""),
                    limit=limit,
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.get("/api/repair-management/people")
        async def repair_management_people(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                try:
                    limit = int(request.query_params.get("limit") or 80)
                except ValueError:
                    limit = 80
                data = await asyncio.to_thread(
                    PortalRuntime.service.list_repair_followup_people,
                    scope=scope,
                    query=str(request.query_params.get("q") or ""),
                    limit=limit,
                    refresh=str(
                        request.query_params.get("refresh") or ""
                    ).lower()
                    in {"1", "true", "yes"},
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.get("/api/repair-management/followups")
        async def repair_management_followups(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                try:
                    limit = int(request.query_params.get("limit") or 100)
                except ValueError:
                    limit = 100
                try:
                    offset = int(request.query_params.get("offset") or 0)
                except ValueError:
                    offset = 0
                data = await asyncio.to_thread(
                    PortalRuntime.service.get_repair_followup_records,
                    summary_record_id=str(
                        request.query_params.get("summary_record_id") or ""
                    ),
                    scope=scope,
                    query=str(request.query_params.get("q") or ""),
                    limit=limit,
                    offset=offset,
                    force_refresh=str(
                        request.query_params.get("refresh") or ""
                    ).lower()
                    in {"1", "true", "yes"},
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/repair-management/followups")
        async def repair_management_followup_create(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, RepairFollowupRecordRequest)
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.create_repair_followup_record,
                    summary_record_id=str(payload.get("summary_record_id") or ""),
                    fields=payload.get("fields") or {},
                    cmdb_record_ids=payload.get("cmdb_record_ids") or [],
                    operation_id=str(payload.get("operation_id") or ""),
                    scope=scope,
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.put("/api/repair-management/followups/{record_id}")
        async def repair_management_followup_update(record_id: str, request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, RepairFollowupRecordRequest)
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.update_repair_followup_record,
                    record_id,
                    summary_record_id=str(payload.get("summary_record_id") or ""),
                    fields=payload.get("fields") or {},
                    cmdb_record_ids=payload.get("cmdb_record_ids") or [],
                    scope=scope,
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.delete("/api/repair-management/followups/{record_id}")
        async def repair_management_followup_delete(record_id: str, request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.delete_repair_followup_record,
                    record_id,
                    summary_record_id=str(
                        request.query_params.get("summary_record_id") or ""
                    ),
                    scope=scope,
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/repair-management/prefill")
        async def repair_management_prefill(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, RepairManagementPrefillRequest)
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.repair_management_combined_prefill,
                    scope=scope,
                    event_record_id=str(payload.get("source_event_id") or ""),
                    repair_record_ids=payload.get("source_repair_ids") or [],
                    month=str(payload.get("source_month") or ""),
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.get("/api/repair-management/status")
        async def repair_management_status(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                try:
                    limit = int(request.query_params.get("limit") or 100)
                except ValueError:
                    limit = 100
                try:
                    offset = int(request.query_params.get("offset") or 0)
                except ValueError:
                    offset = 0
                data = await asyncio.to_thread(
                    PortalRuntime.service.get_repair_management_status,
                    scope=scope,
                    query=str(request.query_params.get("q") or ""),
                    state=str(request.query_params.get("state") or "all"),
                    period=str(request.query_params.get("period") or "all"),
                    limit=limit,
                    offset=offset,
                    force_refresh=str(
                        request.query_params.get("refresh") or ""
                    ).lower()
                    in {"1", "true", "yes"},
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/repair-management/records")
        async def repair_management_records(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                query = str(request.query_params.get("q") or "").strip()
                try:
                    limit = int(request.query_params.get("limit") or 200)
                except ValueError:
                    limit = 200
                try:
                    offset = int(request.query_params.get("offset") or 0)
                except ValueError:
                    offset = 0
                data = await asyncio.to_thread(
                    PortalRuntime.service.get_repair_management_records,
                    scope=request.query_params.get("scope") or "ALL",
                    query=query,
                    limit=limit,
                    offset=offset,
                    focus_record_id=str(
                        request.query_params.get("focus_record_id") or ""
                    ),
                    force_refresh=str(
                        request.query_params.get("refresh") or ""
                    ).lower()
                    in {"1", "true", "yes"},
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/repair-management/records/{record_id}")
        async def repair_management_record_detail(record_id: str, request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.get_repair_management_record,
                    record_id,
                    scope=scope,
                    force_refresh=str(
                        request.query_params.get("refresh") or ""
                    ).lower()
                    in {"1", "true", "yes"},
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=404)

        @app.post("/api/repair-management/records")
        async def repair_management_record_create(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, RepairManagementRecordRequest)
                ).to_payload()
                self._authorized_scope_or_error(session, payload.get("scope") or "ALL")
                data = await asyncio.to_thread(
                    PortalRuntime.service.create_repair_management_record,
                    payload.get("fields") if isinstance(payload.get("fields"), dict) else {},
                    operation_id=str(payload.get("operation_id") or ""),
                    source_event_id=str(payload.get("source_event_id") or ""),
                    source_repair_ids=payload.get("source_repair_ids") or [],
                    source_month=str(payload.get("source_month") or ""),
                    scope=str(payload.get("scope") or "ALL"),
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.put("/api/repair-management/records/{record_id}")
        async def repair_management_record_update(record_id: str, request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, RepairManagementRecordRequest)
                ).to_payload()
                self._authorized_scope_or_error(session, payload.get("scope") or "ALL")
                data = await asyncio.to_thread(
                    PortalRuntime.service.update_repair_management_record,
                    record_id,
                    payload.get("fields") if isinstance(payload.get("fields"), dict) else {},
                    source_event_id=str(payload.get("source_event_id") or ""),
                    source_repair_ids=payload.get("source_repair_ids") or [],
                    replace_source_relations=bool(
                        payload.get("replace_source_relations")
                    ),
                    source_month=str(payload.get("source_month") or ""),
                    scope=str(payload.get("scope") or "ALL"),
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.delete("/api/repair-management/records/{record_id}")
        async def repair_management_record_delete(record_id: str, request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                data = await asyncio.to_thread(
                    PortalRuntime.service.delete_repair_management_record,
                    record_id,
                    scope=request.query_params.get("scope") or "ALL",
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.get("/api/handover-links")
        async def handover_links(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                data = PortalRuntime.auth_manager.filter_handover_links(
                    PortalRuntime.service.get_handover_links(), session
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.get("/api/auth/permissions")
        async def auth_permissions(request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                data = PortalRuntime.auth_manager.get_permissions_payload()
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.get("/api/auth/permission-requests/current")
        async def current_permission_request(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            user = session.get("user") if isinstance(session.get("user"), dict) else {}
            permission_request = PortalRuntime.auth_manager.get_current_permission_request(
                str(user.get("open_id") or "")
            )
            return self._json_ok(request, session, {"request": permission_request or None})

        @app.get("/api/auth/permission-requests/admin")
        async def admin_permission_requests(
            request: Request,
            status: str = "pending",
            limit: int = 100,
        ):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                items = PortalRuntime.auth_manager.list_permission_requests_for_admin(
                    status=status,
                    limit=limit,
                )
                return self._json_ok(request, session, {"items": items})
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.post("/api/auth/permission-requests/bulk-approve")
        async def bulk_approve_permission_requests(request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(
                        request, PermissionRequestBulkReviewRequest
                    )
                ).to_payload()
                actor = session.get("user") if isinstance(session.get("user"), dict) else {}
                actor_open_id = str(actor.get("open_id") or "")
                actor_name = str(actor.get("name") or actor.get("en_name") or "管理员")
                request_ids = [
                    str(item or "").strip()
                    for item in (payload.get("request_ids") or [])
                    if str(item or "").strip()
                ]
                scopes_by_request_id = payload.get("scopes_by_request_id") or {}
                items: list[dict] = []
                failed: list[dict] = []
                for request_id in dict.fromkeys(request_ids):
                    try:
                        result = PortalRuntime.auth_manager.approve_permission_request(
                            request_id,
                            scopes=scopes_by_request_id.get(request_id) or [],
                            updated_by=actor_open_id,
                        )
                        notice = self._notify_permission_review_result(
                            result.get("request") or {},
                            approved=True,
                            actor_name=actor_name,
                        )
                        item = dict(result.get("request") or {})
                        item["notification"] = notice
                        items.append(item)
                    except Exception as exc:
                        failed.append({"request_id": request_id, "error": str(exc)})
                self._clear_read_cache(("auth_status",))
                return self._json_ok(
                    request,
                    session,
                    {"items": items, "failed": failed},
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/auth/permission-requests/bulk-reject")
        async def bulk_reject_permission_requests(request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(
                        request, PermissionRequestBulkReviewRequest
                    )
                ).to_payload()
                actor = session.get("user") if isinstance(session.get("user"), dict) else {}
                actor_open_id = str(actor.get("open_id") or "")
                actor_name = str(actor.get("name") or actor.get("en_name") or "管理员")
                request_ids = [
                    str(item or "").strip()
                    for item in (payload.get("request_ids") or [])
                    if str(item or "").strip()
                ]
                items: list[dict] = []
                failed: list[dict] = []
                reason = str(payload.get("reason") or "").strip()
                for request_id in dict.fromkeys(request_ids):
                    try:
                        item = PortalRuntime.auth_manager.reject_permission_request(
                            request_id,
                            reason=reason,
                            updated_by=actor_open_id,
                        )
                        notice = self._notify_permission_review_result(
                            item,
                            approved=False,
                            actor_name=actor_name,
                            reason=reason,
                        )
                        item = dict(item)
                        item["notification"] = notice
                        items.append(item)
                    except Exception as exc:
                        failed.append({"request_id": request_id, "error": str(exc)})
                self._clear_read_cache(("auth_status",))
                return self._json_ok(
                    request,
                    session,
                    {"items": items, "failed": failed},
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/auth/permission-requests/{request_id}/approve")
        async def approve_permission_request(request_id: str, request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(request, PermissionRequestReviewRequest)
                ).to_payload()
                actor = session.get("user") if isinstance(session.get("user"), dict) else {}
                actor_name = str(actor.get("name") or actor.get("en_name") or "管理员")
                result = PortalRuntime.auth_manager.approve_permission_request(
                    request_id,
                    scopes=payload.get("scopes") or [],
                    updated_by=str(actor.get("open_id") or ""),
                )
                notice = self._notify_permission_review_result(
                    result.get("request") or {},
                    approved=True,
                    actor_name=actor_name,
                )
                self._clear_read_cache(("auth_status",))
                return self._json_ok(
                    request,
                    session,
                    {
                        "request": result.get("request") or {},
                        "permissions": result.get("permissions") or {},
                        "notification": notice,
                    },
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/auth/permission-requests/{request_id}/reject")
        async def reject_permission_request(request_id: str, request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(request, PermissionRequestReviewRequest)
                ).to_payload()
                actor = session.get("user") if isinstance(session.get("user"), dict) else {}
                actor_name = str(actor.get("name") or actor.get("en_name") or "管理员")
                item = PortalRuntime.auth_manager.reject_permission_request(
                    request_id,
                    reason=str(payload.get("reason") or ""),
                    updated_by=str(actor.get("open_id") or ""),
                )
                notice = self._notify_permission_review_result(
                    item,
                    approved=False,
                    actor_name=actor_name,
                    reason=str(payload.get("reason") or ""),
                )
                self._clear_read_cache(("auth_status",))
                return self._json_ok(
                    request,
                    session,
                    {"request": item, "notification": notice},
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/maintenance-actions")
        @app.post("/api/workbench-actions")
        async def workbench_actions(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(
                        request,
                        WorkbenchActionRequest,
                        reject_large_inline_images=True,
                    )
                ).to_payload()
                payload = normalize_notice_identity_payload(payload)
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                payload["scope"] = scope
                payload["_auth_open_id"] = str(user.get("open_id") or "")
                payload["_auth_user_name"] = str(
                    user.get("name") or user.get("en_name") or ""
                )
                if str(payload.get("command_format") or "") == "notice_command":
                    payload = await asyncio.to_thread(
                        PortalRuntime.service.expand_workbench_action_command,
                        payload,
                        scope=scope,
                        ongoing_items=self._get_ongoing(scope),
                    )
                    payload["scope"] = scope
                    payload["_auth_open_id"] = str(user.get("open_id") or "")
                    payload["_auth_user_name"] = str(
                        user.get("name") or user.get("en_name") or ""
                    )
                job_id, should_start = PortalRuntime.service.create_action_job(payload)
                if should_start:
                    PortalRuntime.clear_payload_cache()
                    self._clear_read_cache()
                    PortalRuntime.enqueue_initial_message_or_upload_job(job_id)
                job = PortalRuntime.service.get_job(job_id) or {}
                return JSONResponse(
                    {
                        "ok": True,
                        "data": {
                            "job_id": job_id,
                            "accepted_at": job.get("accepted_at") or 0,
                            "initial_phase": job.get("phase") or "accepted",
                        },
                    },
                    status_code=202,
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/notice-attachments")
        async def notice_attachments(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                raw_length = request.headers.get("content-length", "0") or "0"
                try:
                    length = int(raw_length)
                except ValueError as exc:
                    raise PortalError("现场照片大小无效。") from exc
                if length > MAX_SITE_PHOTO_BYTES:
                    raise PortalError("现场照片不能超过 8MB。")
                content_type = str(request.headers.get("content-type") or "").split(";", 1)[0].strip()
                if not content_type.startswith("image/"):
                    raise PortalError("只能上传图片作为现场照片。")
                body = await request.body()
                if not body:
                    raise PortalError("现场照片内容为空。")
                if len(body) > MAX_SITE_PHOTO_BYTES:
                    raise PortalError("现场照片不能超过 8MB。")
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                file_name = str(
                    request.query_params.get("file_name")
                    or f"site_photo_{uuid.uuid4().hex[:8]}.png"
                ).strip()
                if len(file_name) > 120:
                    file_name = file_name[:120]
                await asyncio.to_thread(
                    PortalRuntime.state_store.cleanup_notice_upload_attachments
                )
                try:
                    attachment = await asyncio.to_thread(
                        PortalRuntime.state_store.put_notice_upload_attachment,
                        open_id=str(user.get("open_id") or ""),
                        file_name=file_name,
                        mime_type=content_type,
                        content=body,
                        ttl_seconds=24 * 60 * 60,
                        max_pending_bytes=MAX_NOTICE_ATTACHMENT_PENDING_BYTES,
                    )
                except ValueError as exc:
                    raise PortalError(str(exc)) from exc
                return self._json_ok(
                    request,
                    session,
                    {
                        "upload_id": attachment.get("upload_id"),
                        "file_name": attachment.get("file_name"),
                        "mime_type": attachment.get("mime_type"),
                        "size": attachment.get("size"),
                        "expires_at": attachment.get("expires_at"),
                    },
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/qt/local/notice-attachments")
        async def qt_local_notice_attachments(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            try:
                raw_length = request.headers.get("content-length", "0") or "0"
                try:
                    length = int(raw_length)
                except ValueError as exc:
                    raise PortalError("图片大小无效。") from exc
                if length > MAX_SITE_PHOTO_BYTES:
                    raise PortalError("图片不能超过 8MB。")
                content_type = str(request.headers.get("content-type") or "").split(";", 1)[0].strip()
                if not content_type.startswith("image/"):
                    raise PortalError("只能上传图片。")
                body = await request.body()
                if not body:
                    raise PortalError("图片内容为空。")
                if len(body) > MAX_SITE_PHOTO_BYTES:
                    raise PortalError("图片不能超过 8MB。")
                file_name = str(
                    request.query_params.get("file_name")
                    or f"qt_notice_image_{uuid.uuid4().hex[:8]}.png"
                ).strip()
                if len(file_name) > 120:
                    file_name = file_name[:120]
                await asyncio.to_thread(
                    PortalRuntime.state_store.cleanup_notice_upload_attachments
                )
                try:
                    attachment = await asyncio.to_thread(
                        PortalRuntime.state_store.put_notice_upload_attachment,
                        open_id="qt-local",
                        file_name=file_name,
                        mime_type=content_type,
                        content=body,
                        ttl_seconds=24 * 60 * 60,
                        max_pending_bytes=MAX_NOTICE_ATTACHMENT_PENDING_BYTES,
                    )
                except ValueError as exc:
                    raise PortalError(str(exc)) from exc
                return {
                    "ok": True,
                    "data": {
                        "upload_id": attachment.get("upload_id"),
                        "file_name": attachment.get("file_name"),
                        "mime_type": attachment.get("mime_type"),
                        "size": attachment.get("size"),
                        "expires_at": attachment.get("expires_at"),
                    },
                }
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/ongoing-items/delete")
        async def ongoing_items_delete(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, OngoingDeleteRequest)
                ).to_payload()
                payload = normalize_notice_identity_payload(payload)
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                payload["scope"] = scope
                payload["_auth_open_id"] = str(user.get("open_id") or "")
                payload["_auth_user_name"] = str(
                    user.get("name") or user.get("en_name") or ""
                )
                PortalRuntime.service.validate_ongoing_delete_item(payload, scope=scope)
                delete_result = await asyncio.to_thread(
                    PortalRuntime.execute_local_delete_active_item,
                    {"data_dict": payload},
                )
                if not bool((delete_result or {}).get("ok")):
                    return JSONResponse(
                        {
                            "ok": False,
                            "error": str((delete_result or {}).get("message") or "多维记录删除失败。"),
                        },
                        status_code=500,
                    )
                data = PortalRuntime.service.hide_ongoing_item(
                    payload,
                    scope=scope,
                    deleted_by=payload["_auth_open_id"],
                )
                data.update(
                    PortalRuntime.service.discard_deleted_ongoing_state(
                        payload, scope=scope
                    )
                )
                PortalRuntime.clear_payload_cache()
                self._clear_read_cache()
                event_id = PortalRuntime.state_store.enqueue_outbox_event(
                    "qt_action",
                    {
                        "kind": "active_delete",
                        "payload": {
                            "active_item_id": str(payload.get("active_item_id") or ""),
                            "record_id": str(
                                (delete_result or {}).get("record_id")
                                or payload.get("target_record_id")
                                or ""
                            ),
                            "source_record_id": str(payload.get("source_record_id") or ""),
                            "work_type": str(payload.get("work_type") or ""),
                            "notice_type": str(payload.get("notice_type") or ""),
                        },
                    },
                )
                data["qt_deleted"] = bool(delete_result.get("active_item_id") or delete_result.get("record_id"))
                data["qt_event_id"] = event_id
                data["remote_deleted"] = bool(delete_result.get("remote_deleted"))
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/ongoing-items/remove-local")
        async def ongoing_items_remove_local(request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(request, OngoingDeleteRequest)
                ).to_payload()
                payload = normalize_notice_identity_payload(payload)
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                payload["scope"] = scope
                payload["_auth_open_id"] = str(user.get("open_id") or "")
                payload["_auth_user_name"] = str(
                    user.get("name") or user.get("en_name") or ""
                )
                PortalRuntime.service.validate_ongoing_delete_item(payload, scope=scope)
                remove_result = await asyncio.to_thread(
                    PortalRuntime.execute_local_remove_active_item,
                    {"data_dict": payload},
                )
                if not bool((remove_result or {}).get("ok")):
                    return JSONResponse(
                        {
                            "ok": False,
                            "error": str((remove_result or {}).get("message") or "本地移除失败。"),
                        },
                        status_code=409,
                    )
                data = PortalRuntime.service.hide_ongoing_item(
                    payload,
                    scope=scope,
                    deleted_by=payload["_auth_open_id"],
                )
                data.update(
                    PortalRuntime.service.discard_deleted_ongoing_state(
                        payload, scope=scope
                    )
                )
                PortalRuntime.clear_payload_cache()
                self._clear_read_cache()
                event_id = PortalRuntime.state_store.enqueue_outbox_event(
                    "qt_action",
                    {
                        "kind": "active_delete",
                        "payload": {
                            "active_item_id": str(payload.get("active_item_id") or ""),
                            "record_id": str(
                                (remove_result or {}).get("record_id")
                                or payload.get("target_record_id")
                                or ""
                            ),
                            "source_record_id": str(payload.get("source_record_id") or ""),
                            "work_type": str(payload.get("work_type") or ""),
                            "notice_type": str(payload.get("notice_type") or ""),
                        },
                    },
                )
                data["qt_deleted"] = bool(remove_result.get("active_item_id") or remove_result.get("record_id"))
                data["qt_event_id"] = event_id
                data["remote_deleted"] = False
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/notice-undo/available")
        async def notice_undo_available(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                items = await asyncio.to_thread(
                    PortalRuntime.service.list_available_notice_undos,
                    scope=scope,
                    action_type=str(request.query_params.get("action_type") or ""),
                    since_seconds=float(request.query_params.get("since_seconds") or 0),
                )
                return self._json_ok(request, session, {"items": items})
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/notice-undo/{undo_id}/apply")
        async def notice_undo_apply(undo_id: str, request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, NoticeUndoApplyRequest)
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                job_id = PortalRuntime.service.create_notice_undo_job(
                    str(undo_id or ""),
                    scope=scope,
                    auth_open_id=str(user.get("open_id") or ""),
                    auth_user_name=str(user.get("name") or user.get("en_name") or ""),
                )
                self._submit_notice_undo_job(job_id, name=f"NoticeUndo-{job_id[:8]}")
                job = PortalRuntime.service.get_job(job_id) or {}
                return JSONResponse(
                    {
                        "ok": True,
                        "data": {
                            "job_id": job_id,
                            "accepted_at": job.get("accepted_at") or 0,
                            "initial_phase": job.get("phase") or "undo_queued",
                        },
                    },
                    status_code=202,
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/change-target-candidates")
        async def change_target_candidates(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, ChangeTargetLookupRequest)
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                result = await asyncio.to_thread(
                    PortalRuntime.service.lookup_change_target_candidates,
                    scope=scope,
                    title=payload.get("title") or "",
                    start_time=payload.get("start_time") or "",
                    end_time=payload.get("end_time") or "",
                    action=payload.get("action") or "update",
                    content=payload.get("content") or "",
                    reason=payload.get("reason") or "",
                    impact=payload.get("impact") or "",
                    progress=payload.get("progress") or "",
                    text=payload.get("text") or "",
                )
                return self._json_ok(request, session, result)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/notice-target-candidates")
        async def notice_target_candidates(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, NoticeTargetLookupRequest)
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                result = await asyncio.to_thread(
                    PortalRuntime.service.lookup_notice_target_candidates,
                    work_type=payload.get("work_type") or "maintenance",
                    scope=scope,
                    title=payload.get("title") or "",
                    start_time=payload.get("start_time") or "",
                    end_time=payload.get("end_time") or "",
                    action=payload.get("action") or "update",
                    content=payload.get("content") or "",
                    reason=payload.get("reason") or "",
                    impact=payload.get("impact") or "",
                    progress=payload.get("progress") or "",
                    text=payload.get("text") or "",
                )
                return self._json_ok(request, session, result)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/notice-identity/bind")
        async def notice_identity_bind(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, NoticeIdentityBindRequest)
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                work_type = str(payload.get("work_type") or "maintenance").strip()
                notice_type = str(
                    payload.get("notice_type")
                    or NOTICE_TYPE_BY_WORK_TYPE.get(work_type)
                    or ""
                ).strip()
                source_record_id = str(payload.get("source_record_id") or "").strip()
                target_record_id = str(
                    payload.get("target_record_id") or payload.get("record_id") or ""
                ).strip()
                active_item_id = str(payload.get("active_item_id") or "").strip()
                if target_record_id and is_local_record_id(target_record_id):
                    target_record_id = ""
                if not (source_record_id or target_record_id or active_item_id):
                    raise PortalError("缺少可绑定的源表记录、目标记录或本地进行中条目。")
                validation = await asyncio.to_thread(
                    PortalRuntime.service.validate_notice_identity_binding,
                    scope=scope,
                    work_type=work_type,
                    notice_type=notice_type,
                    source_record_id=source_record_id,
                    target_record_id=target_record_id,
                    active_item_id=active_item_id,
                )
                scope = str(validation.get("scope") or scope)
                work_type = str(validation.get("work_type") or work_type)
                notice_type = str(validation.get("notice_type") or notice_type)
                source_record_id = str(validation.get("source_record_id") or "")
                target_record_id = str(validation.get("target_record_id") or "")
                active_item_id = str(validation.get("active_item_id") or active_item_id)

                identity_payload = dict(payload)
                identity_payload.update(
                    {
                        "scope": scope,
                        "work_type": work_type,
                        "notice_type": notice_type,
                        "source_record_id": source_record_id,
                        "target_record_id": target_record_id,
                        "record_id": target_record_id or source_record_id,
                        "active_item_id": active_item_id,
                    }
                )

                def persist_binding() -> dict[str, Any]:
                    state_store = PortalRuntime.state_store
                    identity = state_store.upsert_notice_identity(
                        identity_payload,
                        origin="manual_notice_binding",
                    )
                    active_updated = False
                    qt_event_id = 0
                    if active_item_id:
                        try:
                            for row in state_store.list_qt_active_items(include_deleted=False):
                                if str(row.get("active_item_id") or "") != active_item_id:
                                    continue
                                row_payload = row.get("payload")
                                merged = dict(row_payload if isinstance(row_payload, dict) else {})
                                merged.update(
                                    {
                                        "active_item_id": active_item_id,
                                        "work_type": work_type or merged.get("work_type") or "",
                                        "notice_type": notice_type or merged.get("notice_type") or "",
                                    }
                                )
                                if source_record_id:
                                    merged["source_record_id"] = source_record_id
                                if target_record_id:
                                    merged["target_record_id"] = target_record_id
                                    merged["record_id"] = target_record_id
                                state_store.upsert_qt_active_item(
                                    merged,
                                    section=str(row.get("section") or ""),
                                    sort_order=int(row.get("sort_order") or 0),
                                    origin=str(row.get("origin") or "manual_notice_binding"),
                                )
                                qt_event_id = state_store.enqueue_outbox_event(
                                    "qt_action",
                                    {
                                        "kind": "active_upsert",
                                        "payload": {
                                            "item": {
                                                "active_item_id": active_item_id,
                                                "record_id": target_record_id
                                                or str(row.get("record_id") or ""),
                                                "notice_type": notice_type
                                                or str(row.get("notice_type") or ""),
                                                "section": str(row.get("section") or ""),
                                                "sort_order": int(row.get("sort_order") or 0),
                                                "origin": str(
                                                    row.get("origin")
                                                    or "manual_notice_binding"
                                                ),
                                                "payload": merged,
                                            },
                                            "source": "manual_notice_binding",
                                        },
                                    },
                                )
                                active_updated = True
                                break
                        except Exception as exc:
                            log_warning(f"同步 Qt active 绑定关系失败: {exc}")
                    return {
                        "identity": identity or {},
                        "active_updated": active_updated,
                        "qt_event_id": qt_event_id,
                    }

                result = await asyncio.to_thread(persist_binding)
                return self._json_ok(
                    request,
                    session,
                    {
                        "scope": scope,
                        "work_type": work_type,
                        "notice_type": notice_type,
                        "source_record_id": source_record_id,
                        "target_record_id": target_record_id,
                        "active_item_id": active_item_id,
                        "validation": validation,
                        **result,
                    },
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/change-target-candidates/confirm")
        async def change_target_candidates_confirm(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, ChangeTargetConfirmRequest)
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                result = await asyncio.to_thread(
                    PortalRuntime.confirm_change_target_candidate,
                    scope=scope,
                    title=payload.get("title") or "",
                    start_time=payload.get("start_time") or "",
                    end_time=payload.get("end_time") or "",
                    action=payload.get("action") or "update",
                    record_id=payload.get("record_id") or "",
                )
                return self._json_ok(request, session, result)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/handover-links")
        async def save_handover_links(request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(request, HandoverLinksSaveRequest)
                ).to_payload()
                data = PortalRuntime.service.save_handover_links(
                    payload.get("links") or {},
                    password=str(payload.get("password") or ""),
                )
                self._clear_read_cache()
                data = PortalRuntime.auth_manager.filter_handover_links(data, session)
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/handover-links-auth")
        async def handover_links_auth(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(request, HandoverLinksAuthRequest)
                ).to_payload()
                ok = PortalRuntime.service.verify_handover_settings_password(
                    str(payload.get("password") or "")
                )
                if not ok:
                    return JSONResponse(
                        {"ok": False, "error": "设置密码错误。"},
                        status_code=403,
                    )
                return {"ok": True, "data": {"authorized": True}}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/handover-password-reset/request")
        async def handover_password_reset_request(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                data = PortalRuntime.service.request_handover_password_reset()
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/handover-password-reset/confirm")
        async def handover_password_reset_confirm(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(
                        request, HandoverPasswordResetConfirmRequest
                    )
                ).to_payload()
                data = PortalRuntime.service.reset_handover_password_with_code(
                    reset_id=str(payload.get("reset_id") or ""),
                    code=str(payload.get("code") or ""),
                    new_password=str(payload.get("new_password") or ""),
                )
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/auth/permission-requests")
        async def create_permission_request(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, PermissionRequestCreate)
                ).to_payload()
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                data = PortalRuntime.auth_manager.create_permission_request(
                    open_id=str(user.get("open_id") or ""),
                    name=str(user.get("name") or user.get("en_name") or "飞书用户"),
                    scopes=payload.get("scopes") or [],
                    reason=str(payload.get("reason") or ""),
                )
                recipients = PortalRuntime.auth_manager.admin_open_ids()
                labels = "、".join(data.get("requested_scope_labels") or [])
                reason = str(data.get("reason") or "").strip() or "未填写"
                text = (
                    "南通基地-运维灯塔工作台有新的权限申请待审批。\n"
                    f"申请人：{data.get('name') or '飞书用户'}\n"
                    f"openid：{data.get('open_id') or ''}\n"
                    f"申请范围：{labels}\n"
                    f"申请原因：{reason}\n"
                    f"申请编号：{data.get('request_id') or ''}\n"
                    "请管理员进入门户 管理/诊断 -> 权限 -> 权限申请 进行审批。"
                )
                self._submit_background(
                    "LANPortalPermissionRequestNotify",
                    _send_text_to_open_ids_guarded,
                    text,
                    recipients,
                )
                self._clear_read_cache(("auth_status",))
                data["notification"] = {
                    "ok": True,
                    "recipients_count": len(recipients),
                    "message": "已提交，等待管理员审批",
                }
                return self._json_ok(request, session, {"request": data})
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/auth/permission-requests/confirm")
        async def confirm_permission_request(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, PermissionRequestConfirm)
                ).to_payload()
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                permissions = PortalRuntime.auth_manager.confirm_permission_request(
                    open_id=str(user.get("open_id") or ""),
                    request_id=str(payload.get("request_id") or ""),
                    code=str(payload.get("code") or ""),
                    updated_by=str(user.get("open_id") or "permission_request"),
                )
                self._clear_read_cache(("auth_status",))
                refreshed_session = self._current_session(request) or session
                return self._json_ok(
                    request,
                    refreshed_session,
                    {"approved": True, "permissions": permissions},
                )
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/auth/permissions")
        async def save_permissions(request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = (
                    await self._read_model_request(request, AuthPermissionsSaveRequest)
                ).to_payload()
                actor = session.get("user") if isinstance(session.get("user"), dict) else {}
                data, changed_open_ids = PortalRuntime.auth_manager.save_permissions_payload(
                    payload.get("users") or [],
                    updated_by=str(actor.get("open_id") or ""),
                )
                self._clear_read_cache(("auth_status",))
                if changed_open_ids:
                    actor_name = str(actor.get("name") or actor.get("en_name") or "管理员")
                    text = (
                        "南通基地-运维灯塔工作台权限已更新。\n"
                        f"操作人：{actor_name}\n"
                        "请重新进入门户或刷新页面查看最新楼栋权限。"
                    )
                    self._submit_background(
                        "LANPortalPermissionNotify",
                        _send_text_to_open_ids_guarded,
                        text,
                        changed_open_ids,
                    )
                    notify_message = "已加入飞书通知队列"
                else:
                    notify_message = "无权限变更通知"
                data["changed_open_ids"] = changed_open_ids
                data["notification"] = {
                    "ok": True,
                    "message": notify_message,
                    "recipients_count": len(changed_open_ids),
                }
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/generate")
        async def generate_templates(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, GenerateTemplatesRequest)
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session,
                    payload.get("scope")
                    or PortalRuntime.auth_manager.default_scope(session)
                    or "ALL",
                )
                drafts = payload.get("drafts") or []
                await asyncio.to_thread(
                    PortalRuntime.service.assert_generated_drafts_allowed,
                    drafts,
                    scope=scope,
                )
                generated = await asyncio.to_thread(
                    PortalRuntime.service.generate_templates,
                    drafts,
                )
                return {"ok": True, "data": {"items": generated}}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/send-generated")
        async def send_generated(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, SendGeneratedRequest)
                ).to_payload()
                scope = self._authorized_scope_or_error(
                    session,
                    payload.get("scope")
                    or PortalRuntime.auth_manager.default_scope(session)
                    or "ALL",
                )
                items = payload.get("items") or []
                await asyncio.to_thread(
                    PortalRuntime.service.assert_generated_items_allowed,
                    items,
                    scope=scope,
                )
                notice_callback = PortalRuntime.notice_callback or self._enqueue_notice_outbox
                results = await asyncio.to_thread(
                    PortalRuntime.service.send_generated_templates,
                    items,
                    notice_callback=notice_callback,
                )
                return {"ok": True, "data": {"items": results}}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/qt-active-items/stream")
        async def qt_active_items_stream(request: Request):
            return StreamingResponse(
                self._qt_active_items_stream(request),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-store"},
            )

        @app.get("/api/qt/shell/bootstrap")
        async def qt_shell_bootstrap(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            active_items = []
            for row in PortalRuntime.state_store.list_qt_active_items():
                payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
                info = extract_event_info(str(payload.get("text") or "")) or {}
                payload_status = str(payload.get("status") or "").strip()
                if payload_status == "结束" or str(info.get("status") or "").strip() == "结束":
                    PortalRuntime.state_store.delete_qt_active_item(
                        active_item_id=str(row.get("active_item_id") or ""),
                        record_id=str(row.get("record_id") or ""),
                    )
                    continue
                active_items.append(row)
            clipboard_candidates = PortalRuntime.state_store.list_clipboard_candidates(
                status="pending",
                limit=100,
            )
            dialog_sessions = PortalRuntime.state_store.list_dialog_sessions(
                status="pending",
                limit=50,
            )
            return {
                "ok": True,
                "data": {
                    "active_items": active_items,
                    "history_items": [],
                    "clipboard_candidates": clipboard_candidates,
                    "dialog_sessions": dialog_sessions,
                    "runtime_limits": PortalRuntime.runtime_limits(),
                    "runtime_pressure": PortalRuntime.runtime_pressure(),
                    "qt_active_items": PortalRuntime.state_store.qt_active_items_stats(),
                    "qt_bridge": PortalRuntime.state_store.get_backend_runtime("qt_bridge") or {},
                },
            }

        @app.post("/api/qt/commands")
        async def qt_commands(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = (
                await self._read_model_request(
                    request,
                    QtCommandRequest,
                    reject_large_inline_images=True,
                )
            ).to_payload()
            command = str(payload.get("command") or "").strip().lower()
            try:
                if command in {"notice_upload", "notice_update", "notice_end", "notice_archive"}:
                    action_map = {
                        "notice_upload": "upload",
                        "notice_update": "update",
                        "notice_end": "end",
                        "notice_archive": "upload_replace",
                    }
                    action_name_map = {
                        "notice_upload": "上传",
                        "notice_update": "更新",
                        "notice_end": "结束",
                        "notice_archive": "归档",
                    }
                    command_payload = dict(payload.get("payload") or {})
                    command_payload["action_type"] = action_map[command]
                    command_payload = normalize_notice_identity_payload(command_payload)
                    try:
                        data = await asyncio.to_thread(
                            PortalRuntime.execute_local_notice_upload,
                            command_payload,
                        )
                    except Exception as exc:
                        data_dict = (
                            command_payload.get("data_dict")
                            if isinstance(command_payload.get("data_dict"), dict)
                            else {}
                        )
                        fail_record_id = str(
                            data_dict.get("target_record_id")
                            or data_dict.get("record_id")
                            or command_payload.get("target_record_id")
                            or command_payload.get("record_id")
                            or ""
                        ).strip()
                        return {
                            "ok": True,
                            "data": {
                                "ok": False,
                                "name": action_name_map.get(command, "上传"),
                                "message": str(exc),
                                "record_id": fail_record_id,
                                "real_record_id": "",
                            },
                        }
                    PortalRuntime.clear_payload_cache()
                    self._clear_read_cache()
                    return {"ok": True, "data": data}
                if command == "delete_active_item":
                    command_payload = dict(payload.get("payload") or {})
                    command_payload = normalize_notice_identity_payload(command_payload)
                    data = await asyncio.to_thread(
                        PortalRuntime.execute_local_delete_active_item,
                        command_payload,
                    )
                    if not bool((data or {}).get("ok")):
                        return {"ok": True, "data": data}
                    delete_payload = command_payload
                    if isinstance(command_payload.get("data_dict"), dict):
                        delete_payload = command_payload.get("data_dict") or {}
                    elif isinstance(command_payload.get("data"), dict):
                        delete_payload = command_payload.get("data") or {}
                    scope = str(delete_payload.get("scope") or "ALL").strip() or "ALL"
                    try:
                        data.update(
                            PortalRuntime.service.hide_ongoing_item(
                                delete_payload,
                                scope=scope,
                                deleted_by=str(delete_payload.get("_auth_open_id") or "qt"),
                            )
                        )
                        data.update(
                            PortalRuntime.service.discard_deleted_ongoing_state(
                                delete_payload,
                                scope=scope,
                            )
                        )
                    except Exception as cleanup_exc:
                        data["cleanup_warning"] = str(cleanup_exc)
                    PortalRuntime.clear_payload_cache()
                    self._clear_read_cache()
                    PortalRuntime.state_store.enqueue_outbox_event(
                        "qt_action",
                        {
                            "kind": "active_delete",
                            "payload": {
                                "active_item_id": str(
                                    data.get("active_item_id")
                                    or delete_payload.get("active_item_id")
                                    or ""
                                ),
                                "record_id": str(
                                    data.get("record_id")
                                    or delete_payload.get("target_record_id")
                                    or ""
                                ),
                                "source_record_id": str(
                                    delete_payload.get("source_record_id") or ""
                                ),
                                "work_type": str(delete_payload.get("work_type") or ""),
                                "notice_type": str(delete_payload.get("notice_type") or ""),
                            },
                        },
                    )
                    return {"ok": True, "data": data}
                if command == "apply_notice_undo":
                    command_payload = dict(payload.get("payload") or {})
                    undo_id = str(command_payload.get("undo_id") or "").strip()
                    if not undo_id:
                        return JSONResponse(
                            {"ok": False, "error": "缺少 undo_id"},
                            status_code=400,
                        )
                    job_id = await asyncio.to_thread(
                        PortalRuntime.service.create_notice_undo_job,
                        undo_id,
                        scope=str(command_payload.get("scope") or "ALL"),
                        auth_open_id=str(command_payload.get("auth_open_id") or "qt"),
                        auth_user_name=str(command_payload.get("auth_user_name") or "Qt"),
                    )
                    self._submit_notice_undo_job(
                        job_id,
                        name=f"ClipFlowUndoJob-{job_id[:8]}",
                    )
                    job = PortalRuntime.service.get_job(job_id) or {}
                    return {
                        "ok": True,
                        "data": {
                            "ok": True,
                            "job_id": job_id,
                            "initial_phase": job.get("phase") or "undo_queued",
                        },
                    }
                if command == "list_notice_undos":
                    command_payload = dict(payload.get("payload") or {})
                    items = await asyncio.to_thread(
                        PortalRuntime.service.list_available_notice_undos,
                        scope=str(command_payload.get("scope") or "ALL").strip() or "ALL",
                        action_type=str(command_payload.get("action_type") or ""),
                        since_seconds=float(command_payload.get("since_seconds") or 0),
                    )
                    return {"ok": True, "data": {"ok": True, "items": items}}
                if command == "set_today_in_progress":
                    command_payload = dict(payload.get("payload") or {})
                    data = await asyncio.to_thread(
                        PortalRuntime.execute_local_today_progress_update,
                        command_payload,
                    )
                    PortalRuntime.clear_payload_cache()
                    self._clear_read_cache()
                    return {"ok": True, "data": data}
                if command == "validate_record_id":
                    command_payload = dict(payload.get("payload") or {})
                    record_id = str(command_payload.get("record_id") or "").strip()
                    notice_type = str(command_payload.get("notice_type") or "").strip()
                    if not record_id:
                        return {
                            "ok": True,
                            "data": {"ok": False, "message": "缺少 record_id。"},
                        }
                    if not notice_type:
                        return {
                            "ok": True,
                            "data": {"ok": False, "message": "缺少通告类型。"},
                        }
                    ok, result = await asyncio.to_thread(
                        query_record_by_id,
                        record_id,
                        notice_type,
                    )
                    return {
                        "ok": True,
                        "data": {
                            "ok": bool(ok),
                            "message": "" if ok else str(result or "未找到对应记录。"),
                        },
                    }
                if command == "query_record_by_id":
                    command_payload = dict(payload.get("payload") or {})
                    record_id = str(command_payload.get("record_id") or "").strip()
                    notice_type = str(command_payload.get("notice_type") or "").strip()
                    if not record_id:
                        return {
                            "ok": True,
                            "data": {"ok": False, "message": "缺少 record_id。"},
                        }
                    if not notice_type:
                        return {
                            "ok": True,
                            "data": {"ok": False, "message": "缺少通告类型。"},
                        }
                    ok, result = await asyncio.to_thread(
                        query_record_by_id,
                        record_id,
                        notice_type,
                    )
                    return {
                        "ok": True,
                        "data": {
                            "ok": bool(ok),
                            "record": result if ok and isinstance(result, dict) else {},
                            "message": "" if ok else str(result or "查询记录失败。"),
                        },
                    }
                return JSONResponse(
                    {"ok": False, "error": f"不支持的 Qt command: {command or '-'}"},
                    status_code=400,
                )
            except Exception as exc:
                return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

        @app.post("/api/qt/dialog-result")
        async def qt_dialog_result(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = (
                await self._read_model_request(request, QtDialogResultRequest)
            ).to_payload()
            session_id = str(payload.get("session_id") or "").strip()
            if not session_id:
                return JSONResponse({"ok": False, "error": "缺少 session_id"}, status_code=400)
            updated = PortalRuntime.state_store.mark_dialog_session(
                session_id,
                str(payload.get("status") or "completed"),
                payload={"result": payload.get("result")},
            )
            if not updated:
                return JSONResponse({"ok": False, "error": "dialog session 不存在"}, status_code=404)
            return {"ok": True, "data": {"session_id": session_id}}

        @app.post("/api/qt/dialog-sessions")
        async def qt_dialog_sessions(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = (
                await self._read_model_request(request, QtDialogSessionRequest)
            ).to_payload()
            session_id = str(payload.get("session_id") or "").strip() or uuid.uuid4().hex
            dialog_type = str(payload.get("type") or payload.get("dialog_type") or "dialog").strip()
            stored_payload = {
                "session_id": session_id,
                "type": dialog_type,
                "action_type": str(payload.get("action_type") or "").strip(),
                "record_id": str(payload.get("record_id") or "").strip(),
                "active_item_id": str(payload.get("active_item_id") or "").strip(),
                "created_by": "qt",
                "created_at": time.time(),
                "payload": payload.get("payload") if isinstance(payload.get("payload"), dict) else {},
            }
            PortalRuntime.state_store.upsert_dialog_session(
                session_id,
                session_type=dialog_type,
                payload=stored_payload,
                status="pending",
            )
            return {"ok": True, "data": {"session_id": session_id, "payload": stored_payload}}

        @app.post("/api/qt/clipboard-candidates/{candidate_id}/ack")
        async def qt_clipboard_candidate_ack(candidate_id: str, request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = (
                await self._read_model_request(request, QtClipboardAckRequest)
            ).to_payload()
            ok = bool(payload.get("ok", True))
            status = "done" if ok else str(payload.get("status") or "failed")
            updated = PortalRuntime.state_store.mark_clipboard_candidate(
                candidate_id,
                status,
                payload={"ack": payload},
            )
            if not updated:
                return JSONResponse({"ok": False, "error": "clipboard candidate 不存在"}, status_code=404)
            return {"ok": True, "data": {"candidate_id": candidate_id, "status": status}}

        @app.get("/api/qt/events")
        async def qt_events(request: Request, limit: int = 1):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            items = PortalRuntime.state_store.lease_outbox_events(
                "qt_action",
                limit=max(1, min(int(limit or 1), 5)),
                lease_seconds=30,
            )
            return {"ok": True, "data": {"items": items}}

        @app.get("/api/qt/events/stream")
        async def qt_events_stream(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            return StreamingResponse(
                self._qt_event_stream(request),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-store"},
            )

        @app.post("/api/qt/events/{event_id}/ack")
        async def qt_event_ack(event_id: int, request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = (
                await self._read_model_request(request, QtEventAckRequest)
            ).to_payload()
            ok = bool((payload or {}).get("ok", True))
            error = str((payload or {}).get("error") or "")
            event = PortalRuntime.state_store.mark_outbox_event(
                event_id,
                "done" if ok else "pending",
                error=error,
                max_attempts=3,
            )
            event_status = str((event or {}).get("status") or ("done" if ok else "pending"))
            event_payload = (event or {}).get("payload") if isinstance(event, dict) else {}
            event_payload = event_payload if isinstance(event_payload, dict) else {}
            event_kind = str(event_payload.get("kind") or "").strip()
            job_id = str(event_payload.get("job_id") or "").strip()
            if job_id:
                if event_status == "failed":
                    PortalRuntime.service.mark_job(
                        job_id,
                        phase="failed",
                        error=error or "Qt 事件连续执行失败。",
                        qt_event_attempts=int((event or {}).get("attempts") or 0),
                    )
                elif not ok:
                    PortalRuntime.service.mark_job(
                        job_id,
                        phase="qt_queued",
                        qt_phase="outbox_retry",
                        qt_event_attempts=int((event or {}).get("attempts") or 0),
                        qt_event_error=error,
                    )
            if event_kind == "ongoing_delete" and ok and event_status == "done":
                delete_payload = event_payload.get("payload")
                delete_payload = delete_payload if isinstance(delete_payload, dict) else {}
                try:
                    scope = str(delete_payload.get("scope") or "ALL")
                    data = PortalRuntime.service.hide_ongoing_item(
                        delete_payload,
                        scope=scope,
                        deleted_by=str(delete_payload.get("_auth_open_id") or ""),
                    )
                    data.update(
                        PortalRuntime.service.discard_deleted_ongoing_state(
                            delete_payload, scope=scope
                        )
                    )
                    PortalRuntime.clear_payload_cache()
                except Exception as exc:
                    PortalRuntime.state_store.mark_outbox_event(
                        event_id,
                        "pending",
                        error=f"Qt 已处理删除，但本地状态清理失败: {exc}",
                        max_attempts=3,
                    )
            if event_kind == "clipboard_candidate":
                candidate_payload = event_payload.get("payload")
                candidate_payload = candidate_payload if isinstance(candidate_payload, dict) else {}
                candidate_id = str(candidate_payload.get("candidate_id") or "").strip()
                if candidate_id:
                    try:
                        PortalRuntime.state_store.mark_clipboard_candidate(
                            candidate_id,
                            "done" if ok and event_status == "done" else "pending",
                            payload={
                                "event_id": event_id,
                                "ok": ok,
                                "status": event_status,
                                "error": error,
                            },
                        )
                    except Exception:
                        pass
            if event_kind == "dialog_request":
                dialog_payload = event_payload.get("payload")
                dialog_payload = dialog_payload if isinstance(dialog_payload, dict) else {}
                session_id = str(dialog_payload.get("session_id") or "").strip()
                if session_id and ok and event_status == "done":
                    try:
                        PortalRuntime.state_store.mark_dialog_session(
                            session_id,
                            "delivered",
                            payload={"event_id": event_id},
                        )
                    except Exception:
                        pass
            return {
                "ok": True,
                "data": {
                    "event_id": event_id,
                    "status": event_status,
                    "attempts": int((event or {}).get("attempts") or 0),
                },
            }

        @app.post("/api/qt/ongoing-snapshot")
        async def qt_ongoing_snapshot(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = (
                await self._read_model_request(request, QtOngoingSnapshotRequest)
            ).to_payload()
            items = payload.get("items") if isinstance(payload, dict) else []
            result = PortalRuntime.state_store.replace_ongoing_items(
                items if isinstance(items, list) else []
            )
            PortalRuntime.clear_payload_cache()
            return {"ok": True, "data": result}

        @app.post("/api/qt/active-items/delta")
        async def qt_active_items_delta(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = (
                await self._read_model_request(request, QtActiveItemsDeltaRequest)
            ).to_payload()
            upserts = payload.get("upserts") or payload.get("items") or []
            deletes = payload.get("deletes") or payload.get("deleted") or []
            if not isinstance(upserts, list):
                upserts = []
            if not isinstance(deletes, list):
                deletes = []
            upserted = 0
            deleted = 0
            for entry in upserts:
                if not isinstance(entry, dict):
                    continue
                data = entry.get("data") if isinstance(entry.get("data"), dict) else entry
                section = str(entry.get("section") or "").strip()
                origin = str(entry.get("origin") or "qt").strip() or "qt"
                try:
                    sort_order = int(entry.get("sort_order") or 0)
                except Exception:
                    sort_order = 0
                if PortalRuntime.state_store.upsert_qt_active_item(
                    data,
                    section=section,
                    sort_order=sort_order,
                    origin=origin,
                ):
                    upserted += 1
            for entry in deletes:
                if isinstance(entry, dict):
                    active_item_id = str(entry.get("active_item_id") or "").strip()
                    record_id = str(entry.get("record_id") or "").strip()
                else:
                    active_item_id = str(entry or "").strip()
                    record_id = ""
                if PortalRuntime.state_store.delete_qt_active_item(
                    active_item_id=active_item_id,
                    record_id=record_id,
                ):
                    deleted += 1
            if upserted or deleted:
                PortalRuntime.clear_payload_cache()
                self._clear_read_cache()
            return {
                "ok": True,
                "data": {
                    "upserted": upserted,
                    "deleted": deleted,
                    "qt_active_items": PortalRuntime.state_store.qt_active_items_stats(),
                },
            }

        @app.post("/api/qt/bridge-heartbeat")
        async def qt_bridge_heartbeat(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = (
                await self._read_model_request(request, QtLocalHeartbeatRequest)
            ).to_payload()
            payload["heartbeat_at"] = time.time()
            PortalRuntime.state_store.put_backend_runtime("qt_bridge", payload)
            return {"ok": True, "data": {"heartbeat_at": payload["heartbeat_at"]}}

        @app.post("/api/qt/local/heartbeat")
        async def qt_local_heartbeat(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = (
                await self._read_model_request(request, QtLocalHeartbeatRequest)
            ).to_payload()
            payload["heartbeat_at"] = time.time()
            PortalRuntime.state_store.put_backend_runtime("qt_local_shell", payload)
            return {"ok": True, "data": {"heartbeat_at": payload["heartbeat_at"]}}

        @app.post("/api/qt/local/clipboard-event")
        async def qt_local_clipboard_event(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = (
                await self._read_model_request(request, QtClipboardEventRequest)
            ).to_payload()
            content = str(payload.get("content") or "").strip()
            if not content:
                return JSONResponse({"ok": False, "error": "缺少 content"}, status_code=400)
            event_payload = {
                "content": content,
                "ts": payload.get("ts") or int(time.time() * 1000),
                "source": str(payload.get("source") or "clipboard"),
                "target_record_id": str(payload.get("target_record_id") or "").strip(),
            }
            event_id = PortalRuntime.state_store.append_event(
                "clipboard",
                event_payload,
            )
            entry = self._clipboard_entry_from_content(
                content,
                ts=int(event_payload["ts"] or time.time() * 1000),
                source=event_payload["source"],
            )
            if not entry:
                return {
                    "ok": True,
                    "data": {
                        "event_id": event_id,
                        "ignored": True,
                        "reason": "不是支持的通告文本。",
                    },
                }
            candidate_id = str(entry.get("entry_id") or "").strip()
            entry["target_record_id"] = event_payload["target_record_id"]
            PortalRuntime.state_store.upsert_clipboard_candidate(
                candidate_id,
                content=content,
                payload={**event_payload, "entry": entry},
                status="projecting",
                source_event_id=event_id,
            )
            try:
                projection = self._project_clipboard_entry_to_active(entry)
            except Exception as exc:
                PortalRuntime.state_store.mark_clipboard_candidate(
                    candidate_id,
                    "pending",
                    payload={"error": str(exc), "entry": entry},
                )
                raise
            PortalRuntime.state_store.mark_clipboard_candidate(
                candidate_id,
                "done" if projection.get("ok") else "pending",
                payload={"projection": projection, "entry": entry},
            )
            if projection.get("ok") and not projection.get("ignored"):
                PortalRuntime.clear_payload_cache()
                self._clear_read_cache()
            return {
                "ok": True,
                "data": {
                    "event_id": event_id,
                    "candidate_id": candidate_id,
                    "projection": projection,
                },
            }

        @app.get("/api/qt/jobs/{job_id}")
        async def qt_get_job(job_id: str, request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            job = PortalRuntime.service.get_job(job_id)
            if not job:
                return JSONResponse({"ok": False, "error": "任务不存在"}, status_code=404)
            return {"ok": True, "data": job}

        @app.post("/api/qt/jobs/{job_id}/progress")
        async def qt_job_progress(job_id: str, request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            patch = (
                await self._read_model_request(request, QtJobProgressRequest)
            ).to_payload()
            patch = {
                key: value
                for key, value in patch.items()
                if value is not None and value != ""
            }
            self.mark_job_progress(job_id, **patch)
            return {"ok": True, "data": {"job_id": job_id}}

        @app.post("/api/qt/jobs/{job_id}/result")
        async def qt_job_result(job_id: str, request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = (
                await self._read_model_request(request, QtJobResultRequest)
            ).to_payload()
            result_record_id = str(
                payload.get("record_id") or payload.get("target_record_id") or ""
            )
            self.mark_job_upload_result(
                job_id,
                success=bool(payload.get("success")),
                message=str(payload.get("message") or ""),
                record_id=result_record_id,
                active_item_id=str(payload.get("active_item_id") or ""),
            )
            return {"ok": True, "data": {"job_id": job_id}}

        @app.post("/api/backend/shutdown")
        async def backend_shutdown(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            threading.Thread(target=self.stop, name="ClipFlowBackendShutdown", daemon=True).start()
            return {"ok": True, "data": {"stopping": True}}

        @app.api_route(
            "/{path:path}",
            methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
        )
        async def proxy(path: str, request: Request) -> Response:
            return await self._proxy_request(path, request)

        return app

    def _initialize_portal_handler_state(self) -> None:
        PortalRuntime.service = MaintenancePortalService(
            app_token=self.app_token,
            table_id=self.table_id,
            enable_repair_snapshots=True,
        )
        try:
            PortalRuntime.service.ensure_snapshot_loaded()
        except Exception:
            pass
        try:
            PortalRuntime.service.start_repair_snapshot_warmup_async()
        except Exception:
            pass
        try:
            PortalRuntime.service._maybe_start_daily_attachment_cache_refresh()
        except Exception:
            pass
        try:
            PortalRuntime.service.start_signature_crypto_migration_async(delay_seconds=30.0)
        except Exception:
            pass
        PortalRuntime.auth_manager = PortalAuthManager()
        PortalRuntime.state_store = LanPortalStateStore()
        PortalRuntime.apply_runtime_settings()
        self._state_store = PortalRuntime.state_store
        try:
            PortalRuntime.state_store.reset_runtime_queue_incomplete()
        except Exception as exc:
            log_warning(f"运行队列状态重置失败: {exc}")
        PortalRuntime.notice_callback = self.notice_callback
        PortalRuntime.ongoing_callback = self.ongoing_callback
        PortalRuntime.ongoing_delete_callback = self.ongoing_delete_callback
        PortalRuntime.maintenance_action_callback = self.maintenance_action_callback
        with PortalRuntime.source_refresh_lock:
            PortalRuntime.source_refresh_inflight = False
            PortalRuntime.source_refresh_last_result = {}
            PortalRuntime.source_refresh_last_finished = 0.0
        with PortalRuntime.repair_refresh_lock:
            PortalRuntime.repair_refresh_inflight = False
            PortalRuntime.repair_refresh_event = threading.Event()
            PortalRuntime.repair_refresh_last_result = {}
            PortalRuntime.repair_refresh_last_error = ""
            PortalRuntime.repair_refresh_last_finished = 0.0
        with PortalRuntime.action_queue_lock:
            PortalRuntime.action_worker_stop = False
            PortalRuntime.action_queue_event.clear()
        with PortalRuntime.upload_wait_lock:
            PortalRuntime.upload_wait_stop = False
            PortalRuntime.upload_wait_event.clear()
        with PortalRuntime.message_queue_lock:
            PortalRuntime.message_worker_stop = False
            PortalRuntime.message_queue_event.clear()

    @staticmethod
    def _qt_bridge_callback_ready(callback_key: str) -> bool:
        callback_key = str(callback_key or "").strip()
        if not callback_key:
            return False
        try:
            payload = PortalRuntime.state_store.get_backend_runtime("qt_bridge") or {}
            payload = payload if isinstance(payload, dict) else {}
            heartbeat_at = float(payload.get("heartbeat_at") or 0.0)
            if not heartbeat_at or time.time() - heartbeat_at > 20.0:
                return False
            return bool(payload.get(callback_key)) and bool(
                payload.get("event_thread_alive", True)
            )
        except Exception:
            return False

    @staticmethod
    def _scoped_ongoing_signature(scope: str, items: list[dict]) -> tuple[str, int]:
        normalized_scope = PortalRuntime.service._normalize_scope(scope)
        compact: list[dict[str, str]] = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            try:
                if not PortalRuntime.service._scope_matches_item(normalized_scope, item):
                    continue
            except Exception:
                continue
            compact.append(
                {
                    "active_item_id": str(item.get("active_item_id") or ""),
                    "record_id": str(item.get("record_id") or ""),
                    "source_record_id": str(item.get("source_record_id") or ""),
                    "work_type": str(item.get("work_type") or ""),
                    "notice_type": str(item.get("notice_type") or ""),
                    "title": str(item.get("title") or ""),
                    "upload_state": str(item.get("upload_state") or ""),
                    "updated_at": str(
                        item.get("updated_at")
                        or item.get("last_updated_at")
                        or item.get("time")
                        or ""
                    ),
                }
            )
        compact.sort(
            key=lambda row: (
                row.get("active_item_id") or "",
                row.get("record_id") or "",
                row.get("source_record_id") or "",
            )
        )
        raw = json.dumps(compact, ensure_ascii=False, sort_keys=True)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest(), len(compact)

    @staticmethod
    def _scoped_qt_active_signature(scope: str) -> tuple[str, int]:
        try:
            active_rows = PortalRuntime.state_store.list_qt_active_items()
        except Exception:
            active_rows = []
        payloads: list[dict] = []
        for row in active_rows:
            if not isinstance(row, dict):
                continue
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            if not isinstance(payload, dict):
                continue
            item = dict(payload)
            item.setdefault("active_item_id", str(row.get("active_item_id") or ""))
            item.setdefault("record_id", str(row.get("record_id") or ""))
            item.setdefault("target_record_id", str(row.get("record_id") or ""))
            payloads.append(item)
        return FastAPIPortalController._scoped_ongoing_signature(scope, payloads)

    @staticmethod
    def _is_local_client(request: Request) -> bool:
        host = str(request.client.host if request.client else "").strip()
        return host in {"127.0.0.1", "::1", "localhost", "testclient"}

    def _local_only_response(self, request: Request) -> JSONResponse | None:
        if self._is_local_client(request):
            return None
        return JSONResponse(
            {"ok": False, "error": "该接口仅允许本机 Qt 客户端访问。"},
            status_code=403,
        )

    @staticmethod
    def _auth_required_response() -> JSONResponse:
        return JSONResponse(
            {
                "ok": False,
                "error": "请先使用飞书扫码登录。",
                "auth_required": True,
            },
            status_code=401,
        )

    def _current_session(self, request: Request) -> dict | None:
        return PortalRuntime.auth_manager.get_session(
            str(request.cookies.get(AUTH_COOKIE_NAME) or "")
        )

    @staticmethod
    def _request_session_id(request: Request) -> str:
        return str(request.cookies.get(AUTH_COOKIE_NAME) or "").strip()

    def _endpoint_group(self, request: Request) -> str:
        path = str(request.url.path or "/")
        method = str(request.method or "GET").upper()
        if path.endswith("/stream"):
            return "stream"
        if method not in {"GET", "HEAD"}:
            return "mutation"
        if path == "/" or path.startswith("/assets/"):
            return "static"
        if path in {"/api/health", "/api/auth/status"}:
            return "light_read"
        if path in {
            "/api/workbench",
            "/api/records",
            "/api/bootstrap",
            "/api/scope-overview",
            "/api/history-summary",
            "/api/handover-links",
        }:
            return "heavy_read"
        if path.startswith("/api/backend/") or path.startswith("/api/jobs/recent"):
            return "admin_read"
        if path.startswith("/api/"):
            return "read"
        return "static"

    @staticmethod
    def _rate_limit_exempt(request: Request, group: str) -> bool:
        path = str(request.url.path or "")
        if group == "stream":
            return True
        if path.startswith("/api/qt/"):
            return True
        return False

    @staticmethod
    def _rate_limit_params(group: str) -> tuple[float, float]:
        if group == "mutation":
            return 20.0, 120.0
        if group == "heavy_read":
            return 3.0, 18.0
        if group == "light_read":
            return 120.0, 240.0
        if group == "admin_read":
            return 2.0, 12.0
        if group == "static":
            return 200.0, 300.0
        return 60.0, 120.0

    def _consume_rate_limit(self, request: Request, group: str) -> tuple[bool, float]:
        host = str(request.client.host if request.client else "unknown").strip() or "unknown"
        session_id = self._request_session_id(request)
        key = (host, session_id, group)
        rate_per_second, burst = self._rate_limit_params(group)
        now = time.monotonic()
        with self._rate_limit_lock:
            bucket = self._rate_limit_buckets.get(key)
            if not bucket:
                bucket = {"tokens": burst, "updated": now}
                self._rate_limit_buckets[key] = bucket
            elapsed = max(0.0, now - float(bucket.get("updated") or now))
            tokens = min(burst, float(bucket.get("tokens") or 0.0) + elapsed * rate_per_second)
            bucket["updated"] = now
            if tokens >= 1.0:
                bucket["tokens"] = tokens - 1.0
                if len(self._rate_limit_buckets) > 2000:
                    stale_keys = [
                        item_key
                        for item_key, item in self._rate_limit_buckets.items()
                        if now - float(item.get("updated") or 0.0) > 120.0
                    ]
                    for item_key in stale_keys[:500]:
                        self._rate_limit_buckets.pop(item_key, None)
                return True, 0.0
            bucket["tokens"] = tokens
            retry_after = (1.0 - tokens) / max(0.1, rate_per_second)
            return False, retry_after

    def _read_cache_get(self, key: tuple, *, ttl: float, stale_ttl: float = 0.0):
        now = time.monotonic()
        with self._read_cache_lock:
            cached = self._read_cache.get(tuple(key))
            if not cached:
                return None
            expires_at, stale_until, payload = cached
            if expires_at > now:
                return payload
            if stale_ttl and stale_until > now:
                return payload
            return None

    def _read_cache_get_stale(self, key: tuple):
        now = time.monotonic()
        with self._read_cache_lock:
            cached = self._read_cache.get(tuple(key))
            if cached and cached[1] > now:
                return cached[2]
        return None

    def _read_cache_put(self, key: tuple, payload, *, ttl: float = 2.0, stale_ttl: float = 10.0) -> None:
        now = time.monotonic()
        with self._read_cache_lock:
            self._read_cache[tuple(key)] = (now + float(ttl), now + float(stale_ttl), payload)
            if len(self._read_cache) <= self._read_cache_max_entries:
                return
            ordered = sorted(self._read_cache.items(), key=lambda item: item[1][1])
            for old_key, _value in ordered[: max(1, len(ordered) - self._read_cache_max_entries)]:
                self._read_cache.pop(old_key, None)

    def _clear_read_cache(self, prefix: tuple | None = None) -> None:
        with self._read_cache_lock:
            if not prefix:
                self._read_cache.clear()
                return
            prefix = tuple(prefix)
            for key in list(self._read_cache.keys()):
                if tuple(key[: len(prefix)]) == prefix:
                    self._read_cache.pop(key, None)

    def _read_cache_stats(self) -> dict:
        now = time.monotonic()
        with self._read_cache_lock:
            fresh = 0
            stale = 0
            for expires_at, stale_until, _payload in self._read_cache.values():
                if expires_at > now:
                    fresh += 1
                elif stale_until > now:
                    stale += 1
            return {
                "entries": len(self._read_cache),
                "fresh": fresh,
                "stale": stale,
                "max_entries": self._read_cache_max_entries,
            }

    def _stale_response_for_rate_limit(self, request: Request) -> JSONResponse | None:
        path = str(request.url.path or "")
        if str(request.method or "GET").upper() not in {"GET", "HEAD"}:
            return None
        if path == "/api/health":
            payload = self._read_cache_get_stale(("health",))
        elif path == "/api/auth/status":
            next_path = str(request.query_params.get("next") or self._request_target(request))
            payload = self._read_cache_get_stale(
                ("auth_status", self._request_session_id(request), next_path)
            )
        else:
            payload = None
        if payload is None:
            return None
        return JSONResponse(
            payload,
            headers={
                "X-ClipFlow-Stale-Cache": "1",
                "Cache-Control": "no-store",
            },
        )

    def _record_endpoint_perf(
        self,
        request: Request,
        *,
        group: str,
        elapsed_ms: float,
        status_code: int,
        cache_hit: bool = False,
        cache_miss: bool = False,
        rate_limited: bool = False,
    ) -> None:
        path = str(request.url.path or "/")
        method = str(request.method or "GET").upper()
        key = f"{method} {path}"
        with self._perf_lock:
            item = self._perf.get(key)
            if not item:
                if len(self._perf) >= int(getattr(self, "_perf_max_entries", 200) or 200):
                    oldest_key = min(
                        self._perf.items(),
                        key=lambda pair: float((pair[1] or {}).get("updated_at") or 0.0),
                    )[0]
                    self._perf.pop(oldest_key, None)
                item = {
                    "method": method,
                    "path": path,
                    "group": group,
                    "count": 0,
                    "total_ms": 0.0,
                    "samples": deque(maxlen=200),
                    "status_counts": {},
                    "cache_hit": 0,
                    "cache_miss": 0,
                    "rate_limited": 0,
                    "updated_at": 0.0,
                }
                self._perf[key] = item
            item["updated_at"] = time.monotonic()
            item["count"] += 1
            item["total_ms"] += float(elapsed_ms)
            item["samples"].append(float(elapsed_ms))
            status_counts = item["status_counts"]
            status_key = str(int(status_code or 0))
            status_counts[status_key] = int(status_counts.get(status_key) or 0) + 1
            if cache_hit:
                item["cache_hit"] += 1
            if cache_miss:
                item["cache_miss"] += 1
            if rate_limited:
                item["rate_limited"] += 1

    def _perf_snapshot(self) -> dict:
        with self._perf_lock:
            result: dict[str, dict] = {}
            for key, item in self._perf.items():
                samples = sorted(float(value) for value in item.get("samples") or [])
                p95 = 0.0
                if samples:
                    p95 = samples[min(len(samples) - 1, int(len(samples) * 0.95))]
                count = int(item.get("count") or 0)
                result[key] = {
                    "group": item.get("group") or "",
                    "count": count,
                    "avg_ms": round(float(item.get("total_ms") or 0.0) / max(1, count), 2),
                    "p95_ms": round(p95, 2),
                    "cache_hit": int(item.get("cache_hit") or 0),
                    "cache_miss": int(item.get("cache_miss") or 0),
                    "rate_limited": int(item.get("rate_limited") or 0),
                    "status_counts": dict(item.get("status_counts") or {}),
                }
            return result

    def _record_job_batch_stats(
        self,
        *,
        requested: int,
        returned: int,
        missing: int,
        denied: int,
    ) -> None:
        with self._job_batch_lock:
            stats = self._job_batch_stats
            stats["requests"] = int(stats.get("requests") or 0) + 1
            stats["requested_jobs"] = int(stats.get("requested_jobs") or 0) + max(
                0, int(requested or 0)
            )
            stats["returned_jobs"] = int(stats.get("returned_jobs") or 0) + max(
                0, int(returned or 0)
            )
            stats["missing_jobs"] = int(stats.get("missing_jobs") or 0) + max(
                0, int(missing or 0)
            )
            stats["denied_jobs"] = int(stats.get("denied_jobs") or 0) + max(
                0, int(denied or 0)
            )
            stats["max_request_size"] = max(
                int(stats.get("max_request_size") or 0),
                max(0, int(requested or 0)),
            )
            stats["updated_at"] = time.time()

    def _job_batch_snapshot(self) -> dict:
        with self._job_batch_lock:
            stats = dict(self._job_batch_stats)
        requests = max(1, int(stats.get("requests") or 0))
        requested_jobs = int(stats.get("requested_jobs") or 0)
        stats["avg_request_size"] = round(requested_jobs / requests, 2)
        return stats

    @staticmethod
    def _assess_mock_pressure_result(
        data: dict,
        *,
        max_submit_average_ms: float,
        max_total_seconds: float,
        max_failed: int,
    ) -> dict:
        failures: list[str] = []
        count = int(data.get("count") or 0)
        accepted = int(data.get("accepted") or 0)
        failed = int(data.get("failed") or max(0, count - accepted))
        submit_average_ms = float(data.get("submit_average_ms") or 0.0)
        elapsed_seconds = float(data.get("elapsed_seconds") or 0.0)
        if failed > max_failed:
            failures.append(f"失败请求 {failed} 条，超过阈值 {max_failed} 条。")
        if count and accepted + failed < count:
            failures.append(
                f"压测结果不完整：预期 {count} 条，仅统计到 {accepted + failed} 条。"
            )
        if submit_average_ms > max_submit_average_ms:
            failures.append(
                f"平均提交耗时 {submit_average_ms:.1f} ms，超过阈值 {max_submit_average_ms:.1f} ms。"
            )
        if elapsed_seconds > max_total_seconds:
            failures.append(
                f"总耗时 {elapsed_seconds:.1f} 秒，超过阈值 {max_total_seconds:.1f} 秒。"
            )
        site_photos = data.get("site_photos") if isinstance(data.get("site_photos"), dict) else {}
        expected_uploads = int((site_photos or {}).get("expected_uploads") or 0)
        expected_bytes = int((site_photos or {}).get("expected_bytes") or 0)
        stats = data.get("stats") if isinstance(data.get("stats"), dict) else {}
        upload_attachments = (
            stats.get("upload_attachments")
            if isinstance(stats.get("upload_attachments"), dict)
            else {}
        )
        if expected_uploads:
            pending = int((upload_attachments or {}).get("pending") or 0)
            total_bytes = int((upload_attachments or {}).get("total_bytes") or 0)
            if pending < expected_uploads:
                failures.append(
                    f"附件暂存 {pending} 个，少于预期 {expected_uploads} 个。"
                )
            if expected_bytes and total_bytes < expected_bytes:
                failures.append(
                    f"附件暂存大小 {total_bytes} 字节，少于预期 {expected_bytes} 字节。"
                )
        ok = not failures
        return {
            "ok": ok,
            "summary": "达标" if ok else "未达标",
            "failures": failures,
            "thresholds": {
                "max_failed": max_failed,
                "max_submit_average_ms": max_submit_average_ms,
                "max_total_seconds": max_total_seconds,
            },
            "observed": {
                "count": count,
                "accepted": accepted,
                "failed": failed,
                "submit_average_ms": submit_average_ms,
                "elapsed_seconds": elapsed_seconds,
                "expected_uploads": expected_uploads,
            },
        }

    @staticmethod
    def _slow_jobs_snapshot(jobs: list[dict], *, limit: int = 10) -> list[dict]:
        now = time.time()
        terminal_phases = {"success", "failed"}
        rows: list[tuple[float, dict]] = []
        for job in jobs:
            if not isinstance(job, dict):
                continue
            request_payload = job.get("request") if isinstance(job.get("request"), dict) else {}
            accepted_at = float(job.get("accepted_at") or 0.0)
            if accepted_at <= 0:
                continue
            phase = str(job.get("phase") or "").strip() or "unknown"
            finished_at = float(job.get("upload_finished_at") or 0.0)
            if phase not in terminal_phases:
                finished_at = now
            elif finished_at <= 0:
                finished_at = accepted_at
            elapsed_seconds = max(0.0, finished_at - accepted_at)
            rows.append(
                (
                    elapsed_seconds,
                    {
                        "job_id": str(job.get("job_id") or ""),
                        "phase": phase,
                        "elapsed_seconds": round(elapsed_seconds, 1),
                        "work_type": str(request_payload.get("work_type") or ""),
                        "action": str(request_payload.get("action") or ""),
                        "title": str(request_payload.get("title") or "")[:80],
                        "accepted_at": accepted_at,
                        "message_seconds": FastAPIPortalController._duration_between(
                            job.get("message_started_at"), job.get("message_finished_at")
                        ),
                        "upload_wait_seconds": FastAPIPortalController._duration_between(
                            job.get("upload_queued_at"), job.get("upload_started_at")
                        ),
                        "upload_seconds": FastAPIPortalController._duration_between(
                            job.get("upload_started_at"), job.get("upload_finished_at")
                        ),
                        "error_category": str(job.get("error_category") or ""),
                    },
                )
            )
        rows.sort(key=lambda pair: pair[0], reverse=True)
        return [payload for _, payload in rows[: max(1, min(int(limit or 10), 50))]]

    @staticmethod
    def _duration_between(start: Any, end: Any) -> float:
        start_ts = float(start or 0.0)
        end_ts = float(end or 0.0)
        if start_ts <= 0 or end_ts <= 0 or end_ts < start_ts:
            return 0.0
        return round(end_ts - start_ts, 1)

    def _register_sse(self, request: Request, stream_name: str, scope: str = "") -> tuple[tuple, int]:
        host = str(request.client.host if request.client else "unknown").strip() or "unknown"
        session_id = self._request_session_id(request)
        key = (stream_name, host, session_id, str(scope or ""))
        with self._sse_lock:
            self._sse_connection_seq += 1
            connection_id = self._sse_connection_seq
            self._sse_connections[key] = connection_id
            return key, connection_id

    def _sse_active(self, key: tuple, connection_id: int) -> bool:
        with self._sse_lock:
            return self._sse_connections.get(key) == connection_id

    def _unregister_sse(self, key: tuple, connection_id: int) -> None:
        with self._sse_lock:
            if self._sse_connections.get(key) == connection_id:
                self._sse_connections.pop(key, None)

    def _sse_stats(self) -> dict:
        with self._sse_lock:
            by_stream: dict[str, int] = {}
            for key in self._sse_connections:
                stream = str(key[0] if key else "unknown")
                by_stream[stream] = by_stream.get(stream, 0) + 1
            return {"connections": len(self._sse_connections), "by_stream": by_stream}

    def _static_file_response(self, request: Request, path: Path, *, html: bool = False) -> Response:
        resolved = Path(path).resolve()
        allowed = False
        for static_root in portal_static_roots():
            try:
                resolved.relative_to(static_root)
                allowed = True
                break
            except ValueError:
                continue
        if not allowed:
            return JSONResponse({"ok": False, "error": "Not Found"}, status_code=404)
        if not resolved.is_file():
            return JSONResponse({"ok": False, "error": "Not Found"}, status_code=404)
        stat = resolved.stat()
        cache_key = str(resolved)
        now = time.monotonic()
        with self._static_cache_lock:
            cached = self._static_cache.get(cache_key)
            if (
                cached
                and cached.get("mtime_ns") == stat.st_mtime_ns
                and cached.get("size") == stat.st_size
            ):
                cached["last_access"] = now
                entry = cached
            else:
                content = resolved.read_bytes()
                etag = hashlib.sha1(
                    f"{resolved}:{stat.st_mtime_ns}:{stat.st_size}".encode("utf-8")
                ).hexdigest()
                entry = {
                    "mtime_ns": stat.st_mtime_ns,
                    "size": stat.st_size,
                    "content": content,
                    "etag": f'"{etag}"',
                    "content_type": "text/html; charset=utf-8"
                    if html
                    else (mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"),
                    "last_access": now,
                }
                if len(content) <= int(getattr(self, "_static_cache_max_entry_bytes", 0) or 0):
                    self._static_cache[cache_key] = entry
                    self._prune_static_cache_locked()
        headers = {
            "Content-Type": str(entry.get("content_type") or "application/octet-stream"),
            "ETag": str(entry.get("etag") or ""),
            "Cache-Control": "no-store" if html else "public, max-age=86400",
        }
        if str(request.headers.get("if-none-match") or "").strip() == headers["ETag"]:
            return Response(status_code=304, headers=headers)
        return Response(content=entry.get("content") or b"", status_code=200, headers=headers)

    def _prune_static_cache_locked(self) -> None:
        max_entries = max(1, int(getattr(self, "_static_cache_max_entries", 64) or 64))
        max_bytes = max(
            1024,
            int(getattr(self, "_static_cache_max_bytes", 16 * 1024 * 1024) or 0),
        )
        def total_bytes() -> int:
            return sum(
                int((entry or {}).get("size") or len((entry or {}).get("content") or b""))
                for entry in self._static_cache.values()
            )

        while len(self._static_cache) > max_entries or total_bytes() > max_bytes:
            if not self._static_cache:
                return
            oldest_key = min(
                self._static_cache.items(),
                key=lambda pair: float((pair[1] or {}).get("last_access") or 0.0),
            )[0]
            self._static_cache.pop(oldest_key, None)

    def _static_cache_stats(self) -> dict:
        with self._static_cache_lock:
            bytes_used = sum(
                int((entry or {}).get("size") or len((entry or {}).get("content") or b""))
                for entry in self._static_cache.values()
            )
            return {
                "entries": len(self._static_cache),
                "bytes": bytes_used,
                "max_entries": int(getattr(self, "_static_cache_max_entries", 64) or 64),
                "max_bytes": int(
                    getattr(self, "_static_cache_max_bytes", 16 * 1024 * 1024) or 0
                ),
            }

    @staticmethod
    def _html_message(status: int, title: str, message: str) -> Response:
        import html

        body = (
            "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\">"
            f"<title>{html.escape(title)}</title></head><body>"
            f"<h2>{html.escape(title)}</h2><p>{html.escape(message)}</p>"
            "<p><a href=\"/\">返回工作台</a></p></body></html>"
        ).encode("utf-8")
        return Response(
            content=body,
            status_code=status,
            headers={"Content-Type": "text/html; charset=utf-8"},
        )

    @staticmethod
    def _signature_usage_confirmation_page(
        *,
        token: str,
        data: dict[str, Any],
        result_message: str = "",
    ) -> Response:
        import html

        payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
        status = str(data.get("status") or "pending")
        signer_name = str(data.get("signer_name") or "签名人员")
        role_text = str(payload.get("role_text") or "")
        if not role_text:
            role_text = "维护审核人" if str(data.get("role") or "") == "auditor" else "维护实施人"
        notice_title = str(payload.get("notice_title") or "未命名维保通告")
        mop_attachment_name = str(payload.get("mop_attachment_name") or "当前选中附件")
        requested_by = str(data.get("requested_by_name") or "未知操作人")
        requested_by_openid = str(data.get("requested_by_openid") or "")
        status_label = {
            "pending": "等待确认",
            "confirmed": "已确认",
            "rejected": "已拒绝",
        }.get(status, status or "等待确认")
        status_class = {
            "pending": "pending",
            "confirmed": "confirmed",
            "rejected": "rejected",
        }.get(status, "pending")
        action_html = ""
        if status == "pending":
            escaped_token = html.escape(str(token or ""), quote=True)
            action_html = f"""
            <form method="post" action="/api/signatures/usage-confirm" class="actions">
              <input type="hidden" name="token" value="{escaped_token}">
              <button class="btn approve" type="submit" name="decision" value="confirmed">确认使用</button>
              <button class="btn reject" type="submit" name="decision" value="rejected">拒绝使用</button>
            </form>
            """
        result_html = (
            f"<div class=\"result {status_class}\">{html.escape(result_message)}</div>"
            if result_message
            else ""
        )
        operator_text = requested_by
        if requested_by_openid:
            operator_text = f"{requested_by}（{requested_by_openid}）"
        body = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MOP签名使用确认</title>
  <style>
    :root {{
      color-scheme: light;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei", sans-serif;
      background: linear-gradient(180deg, #eaf3ff 0%, #f7fbff 48%, #ffffff 100%);
      color: #0f172a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 20px;
    }}
    .card {{
      width: min(560px, 100%);
      border: 1px solid #d8e6f8;
      border-radius: 24px;
      background: rgba(255, 255, 255, 0.96);
      box-shadow: 0 22px 60px rgba(17, 72, 166, 0.15);
      padding: 26px;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      background: #eaf2ff;
      color: #1d4ed8;
      padding: 7px 12px;
      font-size: 14px;
      font-weight: 800;
    }}
    h1 {{
      margin: 18px 0 12px;
      font-size: clamp(24px, 7vw, 34px);
      line-height: 1.18;
      letter-spacing: 0;
    }}
    .status {{
      display: inline-flex;
      margin-bottom: 18px;
      border-radius: 999px;
      padding: 8px 12px;
      font-weight: 900;
      font-size: 15px;
    }}
    .status.pending {{ background: #fff7ed; color: #c2410c; }}
    .status.confirmed {{ background: #ecfdf5; color: #047857; }}
    .status.rejected {{ background: #fef2f2; color: #b91c1c; }}
    dl {{
      display: grid;
      gap: 12px;
      margin: 0;
      padding: 0;
    }}
    .row {{
      border: 1px solid #e5edf8;
      border-radius: 16px;
      padding: 12px 14px;
      background: #fbfdff;
    }}
    dt {{
      margin: 0 0 4px;
      color: #64748b;
      font-size: 13px;
      font-weight: 800;
    }}
    dd {{
      margin: 0;
      color: #0f172a;
      font-size: 17px;
      font-weight: 850;
      line-height: 1.45;
      word-break: break-word;
    }}
    .actions {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      margin-top: 22px;
    }}
    .btn {{
      min-height: 54px;
      border: 0;
      border-radius: 16px;
      font-size: 18px;
      font-weight: 950;
      cursor: pointer;
      touch-action: manipulation;
    }}
    .btn.approve {{
      background: linear-gradient(135deg, #0f6bff, #0ea5e9);
      color: #fff;
      box-shadow: 0 12px 26px rgba(15, 107, 255, 0.24);
    }}
    .btn.reject {{
      background: #fff1f2;
      color: #be123c;
      border: 1px solid #fecdd3;
    }}
    .result {{
      margin: 0 0 16px;
      border-radius: 16px;
      padding: 12px 14px;
      font-weight: 900;
    }}
    .result.confirmed {{ background: #ecfdf5; color: #047857; }}
    .result.rejected {{ background: #fef2f2; color: #b91c1c; }}
    @media (max-width: 480px) {{
      body {{ padding: 12px; place-items: stretch; }}
      .card {{ padding: 20px; border-radius: 20px; }}
      .actions {{ grid-template-columns: 1fr; }}
      .btn {{ min-height: 58px; }}
    }}
  </style>
</head>
<body>
  <main class="card">
    <span class="badge">MOP签名使用确认</span>
    <h1>是否允许本次使用你的已保存签名？</h1>
    {result_html}
    <div class="status {html.escape(status_class, quote=True)}">{html.escape(status_label)}</div>
    <dl>
      <div class="row"><dt>签名人员</dt><dd>{html.escape(signer_name)}</dd></div>
      <div class="row"><dt>签名角色</dt><dd>{html.escape(role_text)}</dd></div>
      <div class="row"><dt>维护通告</dt><dd>{html.escape(notice_title)}</dd></div>
      <div class="row"><dt>MOP附件</dt><dd>{html.escape(mop_attachment_name)}</dd></div>
      <div class="row"><dt>操作人</dt><dd>{html.escape(operator_text)}</dd></div>
    </dl>
    {action_html}
  </main>
</body>
</html>"""
        return Response(
            content=body.encode("utf-8"),
            status_code=200,
            headers={"Content-Type": "text/html; charset=utf-8"},
        )

    def _root_oauth_callback_response(self, request: Request) -> Response | None:
        code = str(request.query_params.get("code") or "")
        state = str(request.query_params.get("state") or "")
        if not code or not state:
            return None
        sanitized_params: list[tuple[str, str]] = [
            (key, value)
            for key, value in request.query_params.multi_items()
            if key not in {"code", "state"}
        ]
        sanitized = "/"
        if sanitized_params:
            sanitized = f"/?{urlencode(sanitized_params, doseq=True)}"
        if self._current_session(request) is not None:
            return Response(status_code=302, headers={"Location": sanitized})
        redirect_uri = f"{self._request_base_url(request)}/api/auth/feishu/callback"
        try:
            session_id, next_path = PortalRuntime.auth_manager.complete_login(
                code=code,
                state=state,
                redirect_uri=redirect_uri,
            )
        except PortalError as exc:
            return self._html_message(400, "飞书登录失败", str(exc))
        self._clear_read_cache(("auth_status",))
        return Response(
            status_code=302,
            headers={
                "Location": next_path or sanitized,
                "Set-Cookie": PortalRuntime.auth_manager.cookie_header(session_id),
            },
        )

    def _require_admin_response(self, request: Request) -> tuple[JSONResponse | None, dict]:
        session = self._current_session(request)
        if session is None:
            return self._auth_required_response(), {}
        if not PortalRuntime.auth_manager.is_admin(session):
            return (
                JSONResponse(
                    {"ok": False, "error": "只有管理员可以执行该操作。"},
                    status_code=403,
                ),
                session,
            )
        return None, session

    def _request_base_url(self, request: Request) -> str:
        host = PortalRuntime._safe_host_value(
            str(request.headers.get("host") or ""),
            self.get_url().replace("http://", "", 1),
        )
        proto = PortalRuntime._safe_proto_value(
            str(request.headers.get("x-forwarded-proto") or request.url.scheme or "http")
        )
        return f"{proto}://{host}"

    @staticmethod
    def _request_target(request: Request) -> str:
        path = request.url.path or "/"
        if not path.startswith("/"):
            path = "/"
        query = request.url.query
        return f"{path}?{query}" if query else path

    def _with_auth_context(self, payload: dict, session: dict, request: Request) -> dict:
        if not isinstance(payload, dict):
            return payload
        result = dict(payload)
        result["auth"] = PortalRuntime.auth_manager.public_status(
            session,
            next_path=self._request_target(request),
            redirect_uri=f"{self._request_base_url(request)}/api/auth/feishu/callback",
        )
        scope_options = result.get("scope_options")
        if isinstance(scope_options, list):
            result["scope_options"] = PortalRuntime.auth_manager.filter_scope_options(
                scope_options, session
            )
        return result

    def _auth_refresh_headers(self, request: Request, session: dict | None) -> dict[str, str]:
        if not session:
            return {}
        session_id = self._request_session_id(request)
        if not session_id:
            return {}
        return {"Set-Cookie": PortalRuntime.auth_manager.cookie_header(session_id)}

    def _json_response(
        self,
        request: Request,
        session: dict | None,
        payload: dict,
        *,
        status_code: int = 200,
    ) -> JSONResponse:
        return JSONResponse(
            payload,
            status_code=status_code,
            headers=self._auth_refresh_headers(request, session),
        )

    def _json_ok(self, request: Request, session: dict, payload: dict):
        data = PortalRuntime._with_runtime_warnings(payload if isinstance(payload, dict) else {})
        return self._json_response(
            request,
            session,
            {
                "ok": True,
                "data": self._with_auth_context(data, session, request),
            },
        )

    @staticmethod
    async def _read_json_request(
        request: Request, *, max_bytes: int | None = None
    ) -> dict:
        raw_length = request.headers.get("content-length", "0") or "0"
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValueError("请求体长度无效。") from exc
        if length < 0:
            raise ValueError("请求体长度无效。")
        limit = int(max_bytes or MAX_JSON_BODY_BYTES)
        if length > limit:
            raise ValueError("请求体过大，请减少粘贴内容后重试。")
        payload = await request.json()
        if not isinstance(payload, dict):
            raise ValueError("请求体必须是 JSON 对象。")
        return payload

    @classmethod
    async def _read_model_request(
        cls,
        request: Request,
        model_cls: type[APIModel],
        *,
        max_bytes: int | None = None,
        reject_large_inline_images: bool = False,
    ) -> APIModel:
        payload = await cls._read_json_request(request, max_bytes=max_bytes)
        if reject_large_inline_images:
            cls._reject_large_inline_images(payload)
        return parse_api_model(model_cls, payload)

    @staticmethod
    def _reject_large_inline_images(payload: Any) -> None:
        inline_paths: list[str] = []

        def _walk(value: Any, path: str) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    key_text = str(key or "")
                    child_path = f"{path}.{key_text}" if path else key_text
                    if key_text in INLINE_IMAGE_B64_FIELDS and isinstance(child, str):
                        if child.strip():
                            inline_paths.append(child_path)
                            continue
                    _walk(child, child_path)
                return
            if isinstance(value, list):
                for index, child in enumerate(value):
                    _walk(child, f"{path}[{index}]")

        _walk(payload, "")
        if inline_paths:
            raise ValueError(
                "图片不能以内联 base64 提交，请先上传图片生成 upload_id 后再提交。"
                f" 位置: {', '.join(inline_paths[:3])}"
            )

    @staticmethod
    def _portal_error_response(exc: Exception, *, default_status: int) -> JSONResponse:
        status = default_status if isinstance(exc, PortalError) else 500
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=status)

    @staticmethod
    def _authorized_scope_or_error(session: dict, scope: str) -> str:
        normalized = PortalRuntime.auth_manager.normalize_scope(scope)
        if not PortalRuntime.auth_manager.scope_allowed(session, normalized):
            raise PortalError(
                f"当前飞书账号无权访问 {PortalRuntime.auth_manager.scope_label(normalized)}。"
            )
        return normalized

    @staticmethod
    def _cached_service_payload(key_parts: tuple, builder) -> dict:
        class _CacheAdapter:
            @property
            def service(self):
                return PortalRuntime.service

        return PortalRuntime._cached_service_payload(_CacheAdapter(), key_parts, builder)

    @staticmethod
    def _diagnostic_work_type(item: dict) -> str:
        work_type = str(
            item.get("work_type") or item.get("lan_work_type") or ""
        ).strip()
        if work_type:
            return work_type
        notice_type = str(item.get("notice_type") or "").strip()
        return WORK_TYPE_BY_NOTICE_TYPE.get(notice_type) or "maintenance"

    @staticmethod
    def _diagnostic_text_key(value: object) -> str:
        return re.sub(
            r"[\s,，;；:：。.【】（）()《》<>\"'“”‘’\-－_/\\~～至]+",
            "",
            str(value or ""),
        ).strip().lower()

    @classmethod
    def _diagnostic_summary_from_payload(
        cls,
        payload: dict,
        *,
        row: dict | None = None,
        source: str = "",
    ) -> dict:
        row = row if isinstance(row, dict) else {}
        payload = normalize_notice_identity_payload(dict(payload or {}))
        notice_type = str(payload.get("notice_type") or row.get("notice_type") or "").strip()
        work_type = cls._diagnostic_work_type({**payload, "notice_type": notice_type})
        active_item_id = str(
            payload.get("active_item_id")
            or row.get("active_item_id")
            or ""
        ).strip()
        target_record_id = canonical_target_record_id(payload)
        row_record_id = str(row.get("record_id") or "").strip()
        if not target_record_id and row_record_id and not is_local_record_id(row_record_id):
            target_record_id = row_record_id
        source_record_id = canonical_source_record_id(payload)
        title = str(
            payload.get("title")
            or payload.get("name")
            or payload.get("content")
            or payload.get("text")
            or ""
        ).strip()
        if len(title) > 80:
            title = title[:80] + "..."
        building_codes = payload.get("building_codes")
        if not isinstance(building_codes, list):
            building_codes = []
        return {
            "source": source,
            "work_type": work_type,
            "notice_type": notice_type,
            "active_item_id": active_item_id,
            "source_record_id": source_record_id,
            "target_record_id": target_record_id,
            "record_id": target_record_id,
            "title": title,
            "origin": str(payload.get("origin") or row.get("origin") or "").strip(),
            "building_codes": [
                str(item or "").strip()
                for item in building_codes
                if str(item or "").strip()
            ],
            "status": str(payload.get("status") or "").strip(),
            "updated_at": float(row.get("updated_at") or payload.get("updated_at") or 0),
        }

    @classmethod
    def _diagnostic_identity_keys(cls, item: dict) -> set[str]:
        work_type = str(item.get("work_type") or "maintenance").strip() or "maintenance"
        keys: set[str] = set()
        for kind in ("active_item_id", "source_record_id", "target_record_id"):
            value = str(item.get(kind) or "").strip()
            if value and not is_local_record_id(value):
                label = {
                    "active_item_id": "active",
                    "source_record_id": "source",
                    "target_record_id": "target",
                }[kind]
                keys.add(f"{work_type}:{label}:{value}")
        active_item_id = str(item.get("active_item_id") or "").strip()
        if active_item_id and not keys:
            keys.add(f"{work_type}:active:{active_item_id}")
        if not keys:
            title_key = cls._diagnostic_text_key(item.get("title"))
            if title_key:
                keys.add(f"{work_type}:title:{title_key}")
        return keys

    def _backend_consistency_snapshot(self, scope: str = "ALL") -> dict:
        normalized_scope = PortalRuntime.auth_manager.normalize_scope(scope or "ALL")
        qt_items: list[dict] = []
        web_items: list[dict] = []
        errors: list[str] = []
        try:
            rows = PortalRuntime.state_store.list_qt_active_items(include_deleted=False)
            for row in rows:
                payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
                summary = self._diagnostic_summary_from_payload(
                    payload,
                    row=row,
                    source="qt_active_items",
                )
                scope_payload = dict(payload)
                scope_payload.setdefault("building_codes", summary.get("building_codes") or [])
                if PortalRuntime.service._scope_matches_item(normalized_scope, scope_payload):
                    qt_items.append(summary)
        except Exception as exc:
            errors.append(f"Qt active 读取失败: {exc}")
        try:
            ongoing = self._get_ongoing(normalized_scope)
            for item in ongoing:
                if not isinstance(item, dict):
                    continue
                summary = self._diagnostic_summary_from_payload(
                    item,
                    row={},
                    source="web_ongoing",
                )
                web_items.append(summary)
        except Exception as exc:
            errors.append(f"网页进行中投影读取失败: {exc}")

        web_keys: set[str] = set()
        for item in web_items:
            web_keys.update(self._diagnostic_identity_keys(item))
        qt_keys: set[str] = set()
        for item in qt_items:
            qt_keys.update(self._diagnostic_identity_keys(item))

        qt_only = [
            item for item in qt_items
            if not (self._diagnostic_identity_keys(item) & web_keys)
        ]
        web_only = [
            item for item in web_items
            if not (self._diagnostic_identity_keys(item) & qt_keys)
        ]
        missing_target = [
            item for item in qt_items + web_items
            if str(item.get("work_type") or "") != "event"
            and not str(item.get("target_record_id") or "").strip()
        ]
        target_groups: dict[str, list[dict]] = {}
        for item in qt_items:
            target = str(item.get("target_record_id") or "").strip()
            if target:
                target_groups.setdefault(target, []).append(item)
        duplicate_targets = [
            {
                "target_record_id": target,
                "count": len(items),
                "items": items[:5],
            }
            for target, items in sorted(target_groups.items())
            if len(items) > 1
        ]
        issue_count = (
            len(qt_only)
            + len(web_only)
            + len(missing_target)
            + len(duplicate_targets)
            + len(errors)
        )
        return {
            "scope": normalized_scope,
            "ok": issue_count == 0,
            "checked_at": time.time(),
            "counts": {
                "qt_active": len(qt_items),
                "web_ongoing": len(web_items),
                "qt_only": len(qt_only),
                "web_only": len(web_only),
                "missing_target": len(missing_target),
                "duplicate_targets": len(duplicate_targets),
                "errors": len(errors),
            },
            "qt_only": qt_only[:20],
            "web_only": web_only[:20],
            "missing_target": missing_target[:20],
            "duplicate_targets": duplicate_targets[:20],
            "errors": errors,
        }

    def _backend_notice_diagnostic(self, query: str, scope: str = "ALL") -> dict:
        normalized_scope = PortalRuntime.auth_manager.normalize_scope(scope or "ALL")
        query = str(query or "").strip()
        query_lower = query.lower()
        query_key = self._diagnostic_text_key(query)
        errors: list[str] = []
        results: list[dict] = []
        seen: set[str] = set()

        def _matches(item: dict) -> bool:
            if not query:
                return False
            values = [
                item.get("active_item_id"),
                item.get("source_record_id"),
                item.get("target_record_id"),
                item.get("record_id"),
                item.get("identity_id"),
                item.get("title"),
                item.get("notice_type"),
                item.get("work_type"),
                item.get("status"),
                item.get("origin"),
            ]
            haystack = " ".join(str(value or "") for value in values).lower()
            if query_lower and query_lower in haystack:
                return True
            return bool(query_key and query_key in self._diagnostic_text_key(haystack))

        def _scope_matches(summary: dict, raw_payload: dict | None = None) -> bool:
            if normalized_scope in {"ALL", "*"}:
                return True
            payload = dict(raw_payload or {})
            payload.setdefault("building_codes", summary.get("building_codes") or [])
            return bool(PortalRuntime.service._scope_matches_item(normalized_scope, payload))

        def _add(summary: dict, *, source: str, raw_payload: dict | None = None) -> None:
            if not _scope_matches(summary, raw_payload):
                return
            if not _matches(summary):
                return
            key = "|".join([
                source,
                str(summary.get("identity_id") or ""),
                str(summary.get("active_item_id") or ""),
                str(summary.get("source_record_id") or ""),
                str(summary.get("target_record_id") or ""),
                str(summary.get("title") or ""),
            ])
            if key in seen:
                return
            seen.add(key)
            target_record_id = str(summary.get("target_record_id") or "").strip()
            source_record_id = str(summary.get("source_record_id") or "").strip()
            results.append({
                **summary,
                "diagnostic_source": source,
                "binding_status": "已绑定目标多维" if target_record_id else "缺目标多维",
                "source_status": "有源表记录" if source_record_id else "无源表记录/纯手填",
                "remote_check": "未查远端，避免诊断触发飞书接口",
            })

        try:
            rows = PortalRuntime.state_store.list_qt_active_items(include_deleted=True)
            for row in rows:
                payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
                summary = self._diagnostic_summary_from_payload(
                    payload,
                    row=row,
                    source="qt_active_items",
                )
                if row.get("deleted_at"):
                    summary["deleted_at"] = row.get("deleted_at")
                    summary["status"] = summary.get("status") or "deleted"
                _add(summary, source="Qt 活动项", raw_payload=payload)
        except Exception as exc:
            errors.append(f"Qt active 读取失败: {exc}")

        try:
            for item in self._get_ongoing(normalized_scope):
                if not isinstance(item, dict):
                    continue
                summary = self._diagnostic_summary_from_payload(
                    item,
                    row={},
                    source="web_ongoing",
                )
                _add(summary, source="网页进行中", raw_payload=item)
        except Exception as exc:
            errors.append(f"网页进行中读取失败: {exc}")

        try:
            for identity in PortalRuntime.state_store.list_notice_identities(
                include_deleted=True,
                limit=2000,
            ):
                if not isinstance(identity, dict):
                    continue
                payload = dict(identity.get("payload") or {})
                merged = {**payload, **{k: v for k, v in identity.items() if k != "payload"}}
                summary = self._diagnostic_summary_from_payload(
                    merged,
                    row={},
                    source="notice_identity_map",
                )
                summary.update({
                    "identity_id": identity.get("identity_id") or "",
                    "deleted_at": identity.get("deleted_at"),
                    "source_app_token": identity.get("source_app_token") or "",
                    "source_table_id": identity.get("source_table_id") or "",
                    "target_app_token": identity.get("target_app_token") or "",
                    "target_table_id": identity.get("target_table_id") or "",
                })
                _add(summary, source="身份映射", raw_payload=merged)
        except Exception as exc:
            errors.append(f"身份映射读取失败: {exc}")

        results.sort(
            key=lambda item: float(item.get("updated_at") or item.get("deleted_at") or 0),
            reverse=True,
        )
        return {
            "scope": normalized_scope,
            "query": query,
            "checked_at": time.time(),
            "query_required": not bool(query),
            "count": len(results),
            "items": results[:50],
            "errors": errors,
            "tips": [
                "更新/结束/删除必须有 target_record_id。",
                "纯手填或剪贴板通告可以没有 source_record_id。",
                "该诊断只查本地 SQLite，不主动访问飞书。",
            ],
        }

    def _backend_repair_notice_projection(self, scope: str = "ALL") -> dict:
        normalized_scope = PortalRuntime.auth_manager.normalize_scope(scope or "ALL")
        repaired_identities = 0
        repaired_qt_items = 0
        skipped = 0
        errors: list[str] = []

        def _is_ended(payload: dict) -> bool:
            status = str(payload.get("status") or "").strip()
            text = str(payload.get("text") or payload.get("content") or "").strip()
            return status == "结束" or "状态：结束" in text or "状态:结束" in text

        def _scope_matches(payload: dict) -> bool:
            if normalized_scope in {"ALL", "*"}:
                return True
            try:
                return bool(PortalRuntime.service._scope_matches_item(normalized_scope, payload))
            except Exception:
                return False

        def _identity_ready(payload: dict) -> bool:
            return bool(
                str(payload.get("active_item_id") or "").strip()
                or canonical_source_record_id(payload)
                or canonical_target_record_id(payload)
            )

        def _normalized_payload(payload: dict, *, row: dict | None = None) -> dict:
            row = row if isinstance(row, dict) else {}
            merged = dict(payload or {})
            merged.setdefault("active_item_id", str(row.get("active_item_id") or ""))
            merged.setdefault("record_id", str(row.get("record_id") or ""))
            merged.setdefault("target_record_id", str(row.get("record_id") or ""))
            merged.setdefault("notice_type", row.get("notice_type") or "")
            merged.setdefault("origin", row.get("origin") or "")
            return normalize_notice_identity_payload(merged)

        def _upsert_identity(payload: dict, origin: str) -> None:
            nonlocal repaired_identities, skipped
            if not _identity_ready(payload):
                skipped += 1
                return
            identity = PortalRuntime.state_store.upsert_notice_identity(
                payload,
                origin=origin,
            )
            if identity:
                repaired_identities += 1

        try:
            for row in PortalRuntime.state_store.list_qt_active_items(include_deleted=False):
                payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
                normalized = _normalized_payload(payload, row=row)
                if _is_ended(normalized) or not _scope_matches(normalized):
                    skipped += 1
                    continue
                _upsert_identity(normalized, "notice_projection_repair_qt")
        except Exception as exc:
            errors.append(f"Qt active 修复失败: {exc}")

        try:
            for item in self._get_ongoing(normalized_scope):
                if not isinstance(item, dict):
                    skipped += 1
                    continue
                normalized = normalize_notice_identity_payload(dict(item))
                if _is_ended(normalized) or not _scope_matches(normalized):
                    skipped += 1
                    continue
                _upsert_identity(normalized, "notice_projection_repair_web")
                if PortalRuntime.state_store.upsert_qt_active_item(
                    normalized,
                    section="event" if str(normalized.get("notice_type") or "") == "事件通告" else "other",
                    origin=str(normalized.get("origin") or "notice_projection_repair_web"),
                ):
                    repaired_qt_items += 1
        except Exception as exc:
            errors.append(f"网页进行中投影修复失败: {exc}")

        snapshot = self._backend_consistency_snapshot(normalized_scope)
        return {
            "scope": normalized_scope,
            "repaired_identities": repaired_identities,
            "repaired_qt_items": repaired_qt_items,
            "skipped": skipped,
            "errors": errors,
            "checked_at": time.time(),
            "consistency": snapshot,
            "note": "仅修复本地 SQLite identity/Qt 投影，不访问飞书、不写多维。",
        }

    @staticmethod
    def _get_ongoing(scope: str) -> list[dict]:
        active_rows: list[dict] = []
        active_qt_identity_keys: set[str] = set()
        deleted_qt_identity_keys: set[str] = set()

        def _identity_keys(item: dict) -> set[str]:
            try:
                return set(PortalRuntime.service._ongoing_merge_identity_keys(item))
            except Exception:
                normalized = normalize_notice_identity_payload(item)
                work_type = str(normalized.get("work_type") or "maintenance").strip()
                keys: set[str] = set()
                target_record_id = str(normalized.get("target_record_id") or "").strip()
                source_record_id = str(normalized.get("source_record_id") or "").strip()
                active_item_id = str(normalized.get("active_item_id") or "").strip()
                if target_record_id:
                    keys.add(f"{work_type}:target:{target_record_id}")
                if source_record_id:
                    keys.add(f"{work_type}:source:{source_record_id}")
                if active_item_id:
                    keys.add(f"{work_type}:active:{active_item_id}")
                title = str(normalized.get("title") or normalized.get("content") or "").strip()
                if title:
                    keys.add(f"{work_type}:title:{title}")
                return {key for key in keys if key}

        def _row_payload(active_item: dict) -> dict:
            payload = (
                active_item.get("payload")
                if isinstance(active_item, dict)
                and isinstance(active_item.get("payload"), dict)
                else {}
            )
            item = normalize_notice_identity_payload(dict(payload or {}))
            item.setdefault("active_item_id", str(active_item.get("active_item_id") or ""))
            item.setdefault("target_record_id", str(active_item.get("record_id") or ""))
            item.setdefault("record_id", str(active_item.get("record_id") or ""))
            item.setdefault("notice_type", active_item.get("notice_type"))
            item.setdefault("origin", active_item.get("origin"))
            return item

        def _item_deleted_in_qt_store(item: dict) -> bool:
            return bool(_identity_keys(item) & deleted_qt_identity_keys)

        def _item_is_ended(item: dict) -> bool:
            status = str(item.get("status") or "").strip()
            if status == "结束":
                return True
            text = str(item.get("text") or item.get("content") or "").strip()
            if not text:
                return False
            info = extract_event_info(text) or {}
            return str(info.get("status") or "").strip() == "结束"

        def _append(item: dict) -> None:
            if not isinstance(item, dict):
                return
            if _item_is_ended(item):
                return
            if _item_deleted_in_qt_store(item):
                return
            if not PortalRuntime.service._scope_matches_item(scope, item):
                return
            qt_projected_items.append(normalize_notice_identity_payload(dict(item)))

        try:
            active_rows = PortalRuntime.state_store.list_qt_active_items(
                include_deleted=True
            )
            for active_item in active_rows:
                if not isinstance(active_item, dict):
                    continue
                if active_item.get("deleted_at") is not None:
                    continue
                active_qt_identity_keys.update(_identity_keys(_row_payload(active_item)))
            for active_item in active_rows:
                if not isinstance(active_item, dict):
                    continue
                if active_item.get("deleted_at") is None:
                    continue
                deleted_qt_identity_keys.update(_identity_keys(_row_payload(active_item)))
            deleted_qt_identity_keys.difference_update(active_qt_identity_keys)
        except Exception as exc:
            warning = f"SQLite Qt 活动删除状态读取失败: {exc}"
            if not PortalRuntime.last_ongoing_error:
                PortalRuntime.last_ongoing_error = warning
            log_warning(warning)

        qt_projected_items: list[dict] = []
        try:
            for active_item in active_rows or PortalRuntime.state_store.list_qt_active_items():
                if active_item.get("deleted_at") is not None:
                    continue
                payload = _row_payload(active_item)
                if not payload:
                    continue
                text = str(payload.get("text") or "").strip()
                info = extract_event_info(text) or {}
                if str(info.get("status") or "").strip() == "结束":
                    continue
                item = FastAPIPortalController._merge_projected_notice_fields(
                    dict(payload),
                    FastAPIPortalController._projected_notice_fields_from_text(text),
                    overwrite=False,
                )
                item.setdefault("active_item_id", active_item.get("active_item_id"))
                item.setdefault("target_record_id", active_item.get("record_id"))
                if not str(item.get("record_id") or "").strip():
                    item["record_id"] = str(active_item.get("record_id") or "")
                item.setdefault("notice_type", active_item.get("notice_type"))
                item.setdefault("origin", active_item.get("origin"))
                if not item.get("title") and info.get("title"):
                    item["title"] = info.get("title")
                if not item.get("work_type"):
                    notice_type = str(item.get("notice_type") or "").strip()
                    mapped_work_type = WORK_TYPE_BY_NOTICE_TYPE.get(notice_type)
                    if mapped_work_type:
                        item["work_type"] = mapped_work_type
                try:
                    identity = PortalRuntime.state_store.resolve_notice_identity(
                        work_type=str(item.get("work_type") or ""),
                        active_item_id=str(item.get("active_item_id") or ""),
                        source_record_id=str(item.get("source_record_id") or ""),
                        target_record_id=str(item.get("target_record_id") or ""),
                    )
                except Exception:
                    identity = None
                if isinstance(identity, dict):
                    target_record_id = str(identity.get("target_record_id") or "").strip()
                    source_record_id = str(identity.get("source_record_id") or "").strip()
                    if target_record_id:
                        item["target_record_id"] = target_record_id
                        if not str(item.get("record_id") or "").strip():
                            item["record_id"] = target_record_id
                    if source_record_id and not str(item.get("source_record_id") or "").strip():
                        item["source_record_id"] = source_record_id
                    item["binding_status"] = "bound" if target_record_id else "needs_binding"
                elif str(item.get("target_record_id") or "").strip():
                    item["binding_status"] = "bound"
                else:
                    item["binding_status"] = "needs_binding"
                if not isinstance(item.get("building_codes"), list):
                    building_text = " ".join(
                        str(value or "")
                        for value in (
                            item.get("building"),
                            item.get("location"),
                            item.get("title"),
                            text,
                        )
                    )
                    item["building_codes"] = (
                        PortalRuntime.service._building_codes_from_value(building_text)
                    )
                _append(item)
            if active_rows:
                PortalRuntime.last_ongoing_error = ""
                return qt_projected_items
        except Exception as exc:
            warning = f"SQLite Qt 活动条目合并失败: {exc}"
            if not PortalRuntime.last_ongoing_error:
                PortalRuntime.last_ongoing_error = warning
            log_warning(warning)
        try:
            snapshot = PortalRuntime.state_store.get_ongoing_snapshot()
            if snapshot.get("exists"):
                PortalRuntime.last_ongoing_error = ""
                return [
                    normalize_notice_identity_payload(dict(item))
                    for item in snapshot.get("items", [])
                    if isinstance(item, dict)
                    and not _item_is_ended(item)
                    and not _item_deleted_in_qt_store(item)
                    and PortalRuntime.service._scope_matches_item(scope, item)
                ]
        except Exception as exc:
            PortalRuntime.last_ongoing_error = f"SQLite 进行中状态读取失败: {exc}"
            log_warning(PortalRuntime.last_ongoing_error)
        return []

    @staticmethod
    def _reconcile_orphan_started_items(
        scope: str, ongoing: list[dict] | None, *, force: bool = False
    ) -> None:
        scope = PortalRuntime.auth_manager.normalize_scope(scope)
        now = time.monotonic()
        with PortalRuntime.orphan_reconcile_lock:
            if scope in PortalRuntime.orphan_reconcile_pending:
                return
            last = float(PortalRuntime.orphan_reconcile_last.get(scope) or 0)
            if not force and now - last < PortalRuntime.orphan_reconcile_interval_s:
                return
            PortalRuntime.orphan_reconcile_pending.add(scope)
            PortalRuntime.orphan_reconcile_last[scope] = now
        ongoing_copy = [
            dict(item) for item in (ongoing or []) if isinstance(item, dict)
        ]

        def _worker() -> None:
            try:
                result = PortalRuntime.service.reconcile_orphan_started_items(
                    scope=scope, ongoing_items=ongoing_copy
                )
                if int((result or {}).get("removed") or 0) > 0:
                    PortalRuntime.clear_payload_cache()
            except Exception as exc:
                warning = f"本地已开始状态清理失败: {exc}"
                if warning not in PortalRuntime.service._load_warnings:
                    PortalRuntime.service._load_warnings.append(warning)
            finally:
                with PortalRuntime.orphan_reconcile_lock:
                    PortalRuntime.orphan_reconcile_pending.discard(scope)

        try:
            submitted = self._submit_background(
                f"LANOrphanReconcile-{scope}",
                _worker,
            )
            if not submitted:
                raise RuntimeError("后端后台任务队列不可用")
        except Exception as exc:
            with PortalRuntime.orphan_reconcile_lock:
                PortalRuntime.orphan_reconcile_pending.discard(scope)
            warning = f"本地已开始状态清理启动失败: {exc}"
            if warning not in PortalRuntime.service._load_warnings:
                PortalRuntime.service._load_warnings.append(warning)

    @staticmethod
    def _job_visible_to_session(job: dict, session: dict) -> bool:
        if PortalRuntime.auth_manager.is_admin(session):
            return True
        open_id = str((session.get("user") or {}).get("open_id") or "")
        request_payload = job.get("request") if isinstance(job.get("request"), dict) else {}
        return bool(open_id and str(request_payload.get("_auth_open_id") or "") == open_id)

    def _write_runtime_heartbeat(self) -> None:
        try:
            if not PortalRuntime.state_store.put_backend_runtime_async(
                "backend",
                {
                    "backend": "fastapi",
                    "url": self.get_url(),
                    "stats": _queue_stats(),
                    "heartbeat_at": time.time(),
                },
            ):
                PortalRuntime.state_store.put_backend_runtime(
                    "backend",
                    {
                        "backend": "fastapi",
                        "url": self.get_url(),
                        "stats": _queue_stats(),
                        "heartbeat_at": time.time(),
                    },
                )
        except Exception as exc:
            log_warning(f"后端心跳写入失败: {exc}")

    @staticmethod
    def _enqueue_notice_outbox(payload: dict) -> dict:
        event_id = PortalRuntime.state_store.enqueue_outbox_event(
            "qt_action",
            {
                "kind": "notice",
                "payload": payload if isinstance(payload, dict) else {},
            },
        )
        return {"ok": True, "event_id": event_id}

    @staticmethod
    def _clipboard_entry_id(entry: dict) -> str:
        key = (
            f"{entry.get('unique_key', '')}|{entry.get('status', '')}|"
            f"{entry.get('notice_type', '')}|{entry.get('title', '')}|"
            f"{entry.get('reason', '')}"
        )
        return hashlib.md5(key.encode("utf-8", errors="ignore")).hexdigest()

    @classmethod
    def _clipboard_entry_from_content(
        cls, content: str, *, ts: int | None = None, source: str = "clipboard"
    ) -> dict | None:
        info = extract_event_info(content) or {}
        if not info:
            return None
        notice_type = str(info.get("notice_type") or "").strip()
        event_source = str(info.get("source") or "").strip()
        entry = {
            "content": info.get("content") or content,
            "status": str(info.get("status") or "").strip(),
            "title": str(info.get("title") or "").strip(),
            "notice_type": notice_type,
            "level": info.get("level"),
            "source": event_source if notice_type == "事件通告" else str(info.get("source") or "").strip(),
            "origin": str(source or "clipboard").strip() or "clipboard",
            "time_str": str(info.get("time_str") or "").strip(),
            "reason": str(info.get("reason") or "").strip(),
            "unique_key": str(info.get("unique_key") or "").strip(),
            "ts": int(ts or time.time() * 1000),
        }
        entry["entry_id"] = cls._clipboard_entry_id(entry)
        return entry

    @classmethod
    def _find_qt_active_item_for_clipboard_entry(cls, entry: dict) -> dict | None:
        notice_type = str(entry.get("notice_type") or "").strip()
        unique_key = str(entry.get("unique_key") or "").strip()
        title = str(entry.get("title") or "").strip()
        reason = str(entry.get("reason") or "").strip()
        target_record_id = str(entry.get("target_record_id") or "").strip()
        if not notice_type or not (unique_key or title):
            return None
        normalized_title = cls._normalize_clipboard_event_title(title)
        event_title_matches = []
        for item in PortalRuntime.state_store.list_qt_active_items():
            payload = item.get("payload") if isinstance(item, dict) else {}
            payload = payload if isinstance(payload, dict) else {}
            if str(payload.get("notice_type") or item.get("notice_type") or "").strip() != notice_type:
                continue
            if target_record_id and target_record_id in {
                str(payload.get("target_record_id") or "").strip(),
                str(payload.get("record_id") or "").strip(),
                str(item.get("record_id") or "").strip(),
            }:
                return item
            info = extract_event_info(str(payload.get("text") or "")) or {}
            item_key = str(info.get("unique_key") or "").strip()
            item_event_identity_key = str(payload.get("event_identity_key") or "").strip()
            incoming_event_identity_key = ""
            if notice_type == "事件通告":
                try:
                    incoming_event_identity_key = PortalRuntime._event_notice_identity_key(
                        {"notice_type": notice_type, "text": str(entry.get("content") or "")}
                    )
                except Exception:
                    incoming_event_identity_key = ""
            item_title = str(
                payload.get("match_title")
                or info.get("title")
                or payload.get("title")
                or ""
            ).strip()
            item_reason = str(info.get("reason") or payload.get("reason") or "").strip()
            if unique_key and item_key and item_key == unique_key:
                return item
            if (
                notice_type == "事件通告"
                and incoming_event_identity_key
                and item_event_identity_key
                and incoming_event_identity_key == item_event_identity_key
            ):
                return item
            if (
                normalized_title
                and item_title
                and cls._normalize_clipboard_event_title(item_title) == normalized_title
            ):
                if notice_type == "事件通告":
                    event_title_matches.append(item)
                    continue
                if notice_type == "维保通告" and reason and item_reason and reason != item_reason:
                    continue
                if notice_type == "维保通告" and reason and not item_reason:
                    continue
                return item
        if notice_type == "事件通告" and event_title_matches:
            # Event notices often share the same alarm title across different
            # occurrence times.  Title-only matching can bind a new local event
            # to an old target_record_id, and deletion can then remove the
            # wrong remote bitable row.  Only exact unique_key or explicit
            # target_record_id matches are allowed for events.
            return None
        return None

    @staticmethod
    def _normalize_clipboard_event_title(value: Any) -> str:
        text = unicodedata.normalize("NFKC", str(value or ""))
        text = re.sub(r"\s+", " ", text).strip()
        return text.strip(" ;；")

    @staticmethod
    def _clean_projected_notice_field(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip(" ;；")

    @staticmethod
    def _projected_action_from_status(status: Any) -> tuple[str, str]:
        text = str(status or "").strip()
        if "结束" in text:
            return "end", "结束"
        if "更新" in text:
            return "update", "更新"
        if "新增" in text:
            return "start", "新增"
        if "开始" in text:
            return "start", "开始"
        return "", text

    @classmethod
    def _normalize_projected_notice_type(
        cls, notice_type: str, work_type: str
    ) -> str:
        notice_type = str(notice_type or "").strip()
        work_type = str(work_type or "").strip()
        if notice_type == NOTICE_TYPE_CHANGE:
            return NOTICE_TYPE_CHANGE
        if notice_type:
            return notice_type
        return str(NOTICE_TYPE_BY_WORK_TYPE.get(work_type) or "").strip()

    @classmethod
    def _split_projected_notice_time_range(cls, value: Any) -> tuple[str, str]:
        text = str(value or "").strip()
        if not text:
            return "", ""
        normalized = (
            text.replace("－", "-")
            .replace("—", "-")
            .replace("～", "~")
            .replace("：", ":")
        )

        def _format(parsed: dt.datetime | None, fallback: str) -> str:
            if parsed:
                return parsed.strftime("%Y-%m-%d %H:%M")
            return cls._clean_projected_notice_field(fallback)

        for separator in ("~", "至", "到"):
            if separator not in normalized:
                continue
            left, right = normalized.split(separator, 1)
            left = left.strip()
            right = right.strip()
            left_dt = PortalRuntime.service._parse_notice_datetime(left)
            right_dt = PortalRuntime.service._parse_notice_datetime(right)
            if left_dt and not right_dt:
                time_only = re.search(r"(\d{1,2})\s*[:点时.]\s*(\d{1,2})?", right)
                if time_only:
                    right_dt = left_dt.replace(
                        hour=int(time_only.group(1)),
                        minute=int(time_only.group(2) or 0),
                    )
            return _format(left_dt, left), _format(right_dt, right)

        hyphen = re.match(
            r"(.+?\d{1,2}\s*[:点时.]\s*\d{0,2})\s*-\s*(\d{1,2}\s*[:点时.]\s*\d{0,2}.*)$",
            normalized,
        )
        if hyphen:
            left = hyphen.group(1).strip()
            right = hyphen.group(2).strip()
            left_dt = PortalRuntime.service._parse_notice_datetime(left)
            right_dt = PortalRuntime.service._parse_notice_datetime(right)
            if left_dt and not right_dt:
                time_only = re.search(r"(\d{1,2})\s*[:点时.]\s*(\d{1,2})?", right)
                if time_only:
                    right_dt = left_dt.replace(
                        hour=int(time_only.group(1)),
                        minute=int(time_only.group(2) or 0),
                    )
            return _format(left_dt, left), _format(right_dt, right)
        parsed = PortalRuntime.service._parse_notice_datetime(normalized)
        return _format(parsed, normalized), ""

    @classmethod
    def _projected_notice_fields_from_text(
        cls, text: str, *, entry: dict | None = None
    ) -> dict[str, Any]:
        text = str(text or "").strip()
        if not text:
            return {}
        entry = entry if isinstance(entry, dict) else {}
        sections = PortalRuntime.service._parse_notice_sections(text)
        work_type = PortalRuntime.service._notice_work_type_from_text(text, sections)

        def value(names: list[str], fallback: Any = "") -> str:
            return cls._clean_projected_notice_field(
                PortalRuntime.service._notice_section_value(sections, names, str(fallback or ""))
            )

        notice_type = cls._normalize_projected_notice_type(
            str(entry.get("notice_type") or ""), work_type
        )
        title = value(
            ["名称", "标题", "通告名称", "维修名称", "事件描述"],
            entry.get("title"),
        )
        time_str = value(
            ["时间", "事件发生时间", "发生时间", "计划时间", "维护时间"],
            entry.get("time_str"),
        )
        start_time, end_time = cls._split_projected_notice_time_range(time_str)
        projected: dict[str, Any] = {
            "notice_type": notice_type,
            "work_type": work_type,
            "lan_work_type": work_type,
            "title": title,
            "time_str": time_str,
            "level": value(["等级", "变更等级", "紧急程度"], entry.get("level")),
            "specialty": value(["专业", "专业类别", "所属专业"]),
            "location": value(["地点", "位置"]),
            "content": value(["内容"]),
            "reason": value(["原因", "故障原因", "故障维修原因"]),
            "impact": value(["影响", "影响范围"]),
            "progress": value(["进度", "完成情况"]),
            "start_time": start_time,
            "end_time": end_time,
            "maintenance_cycle": value(["维保周期", "维护周期"]),
            "repair_device": value(["维修设备"]),
            "repair_fault": value(["维修故障"]),
            "fault_type": value(["故障类型"]),
            "repair_mode": value(["维修方式"]),
            "discovery": value(["故障发现方式"]),
            "symptom": value(["故障现象"]),
            "solution": value(["解决方案"]),
            "spare_parts": value(["备件更换情况", "备件使用情况"]),
            "device": value(["设备"]),
            "cabinet": value(["柜号"]),
            "quantity": value(["数量"]),
        }
        building_codes = PortalRuntime.service._building_codes_from_notice_text(
            value(["楼栋", "变更楼栋", "所属楼栋"]),
            projected.get("location"),
            title,
            text,
        )
        if building_codes:
            projected["building_codes"] = building_codes
        if work_type == "repair":
            fault_time = value(["发现故障时间", "故障发生时间", "发生故障时间"])
            expected_time = value(["期望完成时间", "计划完成时间"])
            projected["fault_time"] = (
                cls._split_projected_notice_time_range(fault_time)[0] or fault_time
            )
            projected["expected_time"] = (
                cls._split_projected_notice_time_range(expected_time)[0] or expected_time
            )
            if projected.get("expected_time"):
                projected["start_time"] = projected["expected_time"]
            if projected.get("fault_time"):
                projected["end_time"] = projected["fault_time"]
        return {
            key: value
            for key, value in projected.items()
            if value not in (None, "", [], {})
        }

    @classmethod
    def _merge_projected_notice_fields(
        cls,
        payload: dict[str, Any],
        projected: dict[str, Any],
        *,
        overwrite: bool,
    ) -> dict[str, Any]:
        merged = dict(payload or {})
        for key, value in (projected or {}).items():
            if value in (None, "", [], {}):
                continue
            if key == "building_codes" and isinstance(value, list):
                current_codes = merged.get(key)
                if (
                    overwrite
                    or not isinstance(current_codes, list)
                    or not current_codes
                    or len(current_codes) > len(value)
                ):
                    merged[key] = list(value)
                continue
            if overwrite or merged.get(key) in (None, "", [], {}):
                merged[key] = value
        return merged

    @staticmethod
    def _reset_projected_upload_state(data: dict) -> None:
        data["_has_unuploaded_changes"] = True
        data["_pending_upload_hash"] = None
        data["_upload_in_progress"] = False
        data.pop("_last_upload_error", None)
        data.pop("_upload_started_monotonic", None)

    @classmethod
    def _project_clipboard_entry_to_active(cls, entry: dict) -> dict:
        status = str(entry.get("status") or "").strip()
        content = str(entry.get("content") or "").strip()
        notice_type = str(entry.get("notice_type") or "").strip()
        projected_action, projected_status = cls._projected_action_from_status(status)
        entry_target_record_id = str(entry.get("target_record_id") or "").strip()
        if entry_target_record_id and notice_type:
            incoming_notice_type = cls._normalize_projected_notice_type(
                notice_type,
                str(entry.get("work_type") or entry.get("lan_work_type") or "").strip(),
            )
            for item in PortalRuntime.state_store.list_qt_active_items():
                payload = item.get("payload") if isinstance(item, dict) else {}
                payload = payload if isinstance(payload, dict) else {}
                item_target_record_id = str(
                    payload.get("target_record_id")
                    or payload.get("record_id")
                    or item.get("record_id")
                    or ""
                ).strip()
                if item_target_record_id != entry_target_record_id:
                    continue
                item_notice_type = cls._normalize_projected_notice_type(
                    str(payload.get("notice_type") or item.get("notice_type") or "").strip(),
                    str(payload.get("work_type") or payload.get("lan_work_type") or "").strip(),
                )
                if item_notice_type and incoming_notice_type and item_notice_type != incoming_notice_type:
                    return {
                        "ok": True,
                        "ignored": True,
                        "reason": "目标记录已绑定其他通告类型，已忽略本次剪贴板投影。",
                    }
        existing = cls._find_qt_active_item_for_clipboard_entry(entry)
        active_item_id = ""
        if existing and isinstance(existing.get("payload"), dict):
            data = dict(existing.get("payload") or {})
            active_item_id = str(data.get("active_item_id") or existing.get("active_item_id") or "").strip()
            existing_record_id = str(existing.get("record_id") or "").strip()
            existing_target_id = str(
                data.get("target_record_id")
                or existing_record_id
                or data.get("record_id")
                or ""
            ).strip()
            if existing_target_id and not is_local_record_id(existing_target_id):
                data["target_record_id"] = existing_target_id
                data["record_id"] = existing_target_id
                data["_is_placeholder_record"] = False
            elif existing_record_id and not str(data.get("record_id") or "").strip():
                data["record_id"] = existing_record_id
            existing_notice_type = cls._normalize_projected_notice_type(
                str(data.get("notice_type") or existing.get("notice_type") or "").strip(),
                str(data.get("work_type") or data.get("lan_work_type") or "").strip(),
            )
            incoming_notice_type = cls._normalize_projected_notice_type(
                notice_type,
                str(entry.get("work_type") or entry.get("lan_work_type") or "").strip(),
            )
            if existing_notice_type and incoming_notice_type and existing_notice_type != incoming_notice_type:
                return {
                    "ok": True,
                    "ignored": True,
                    "reason": "目标记录已绑定其他通告类型，已忽略本次剪贴板投影。",
                }
        else:
            data = {}
        if status not in {"", "开始", "新增"} and not data and not entry_target_record_id:
            if notice_type == "事件通告":
                projected_for_lookup = cls._projected_notice_fields_from_text(
                    content,
                    entry=entry,
                )
                lookup_payload = {
                    **projected_for_lookup,
                    "notice_type": notice_type,
                    "text": content,
                    "source": entry.get("source"),
                    "event_source": entry.get("source"),
                    "level": entry.get("level"),
                    "time_str": entry.get("time_str"),
                }
                entry_target_record_id = PortalRuntime._event_target_from_identity_map(
                    lookup_payload
                )
                if entry_target_record_id:
                    data = {
                        **lookup_payload,
                        "target_record_id": entry_target_record_id,
                        "record_id": entry_target_record_id,
                        "_is_placeholder_record": False,
                        "binding_status": "bound",
                    }
            if not data and not entry_target_record_id:
                return {"ok": True, "ignored": True, "reason": "未找到可更新的活动条目。"}
        if not active_item_id:
            active_item_id = str(entry.get("entry_id") or "").strip() or uuid.uuid4().hex
        record_id = str(
            entry_target_record_id
            or data.get("target_record_id")
            or data.get("record_id")
            or ""
        ).strip()
        if not record_id:
            record_id = f"local_{active_item_id[:24]}"
        if entry_target_record_id:
            is_placeholder = False
        elif "_is_placeholder_record" in data:
            is_placeholder = bool(data.get("_is_placeholder_record"))
        else:
            is_placeholder = not bool(str(data.get("record_id") or "").strip())
        projected_fields = cls._projected_notice_fields_from_text(content, entry=entry)
        data = cls._merge_projected_notice_fields(
            data,
            projected_fields,
            overwrite=True,
        )
        saved_event_source = str(
            data.get("event_source") or data.get("source") or ""
        ).strip()
        incoming_event_source = str(entry.get("source") or "").strip()
        building_codes = data.get("building_codes")
        if not isinstance(building_codes, list):
            building_codes = PortalRuntime.service._building_codes_from_notice_text(
                data.get("building"),
                data.get("location"),
                entry.get("title"),
                data.get("title"),
                content,
            )
        work_type = data.get("work_type") or data.get("lan_work_type") or ""
        if not work_type:
            if notice_type == "维保通告":
                work_type = "maintenance"
            elif notice_type == NOTICE_TYPE_CHANGE:
                work_type = "change"
            elif notice_type == "设备检修":
                work_type = "repair"
        notice_type = cls._normalize_projected_notice_type(notice_type, work_type)
        projected_target_record_id = str(
            entry_target_record_id or data.get("target_record_id") or ""
        ).strip()
        if not projected_target_record_id and record_id and not is_local_record_id(record_id):
            projected_target_record_id = record_id
        data.update(
            {
                "active_item_id": active_item_id,
                "record_id": record_id,
                "target_record_id": projected_target_record_id,
                "_is_placeholder_record": is_placeholder,
                "text": content,
                "title": data.get("title") or entry.get("title") or "",
                "notice_type": notice_type,
                "action": projected_action or data.get("action") or "",
                "status": projected_status or data.get("status") or "",
                "level": data.get("level") or entry.get("level"),
                "source": (
                    saved_event_source or incoming_event_source
                    if notice_type == "事件通告"
                    else incoming_event_source or data.get("source", "")
                ),
                "time_str": data.get("time_str") or entry.get("time_str") or "",
                "reason": data.get("reason") or entry.get("reason") or "",
                "origin": data.get("origin") or "clipboard",
                "building_codes": building_codes,
                "work_type": work_type,
                "lan_work_type": work_type,
            }
        )
        if notice_type == "事件通告":
            event_source = str(data.get("source") or "").strip()
            if event_source:
                data["event_source"] = event_source
            else:
                data.pop("event_source", None)
            data.update(PortalRuntime._event_identity_payload_patch(data))
            recovered_target_record_id = PortalRuntime._event_target_from_identity_map(data)
            current_target_record_id = str(data.get("target_record_id") or "").strip()
            if (
                recovered_target_record_id
                and (
                    not current_target_record_id
                    or is_local_record_id(current_target_record_id)
                    or bool(data.get("_is_placeholder_record"))
                )
            ):
                data["target_record_id"] = recovered_target_record_id
                data["record_id"] = recovered_target_record_id
                data["_is_placeholder_record"] = False
                data["binding_status"] = "bound"
                record_id = recovered_target_record_id
                projected_target_record_id = recovered_target_record_id
        cls._reset_projected_upload_state(data)
        section = "event" if notice_type == "事件通告" else "other"
        projected_item = {
            "active_item_id": active_item_id,
            "record_id": record_id,
            "notice_type": notice_type,
            "section": section,
            "sort_order": 0,
            "origin": "clipboard",
            "payload": data,
        }
        PortalRuntime.state_store.upsert_qt_active_item(
            data,
            section=section,
            sort_order=0,
            origin="clipboard",
        )
        event_id = PortalRuntime.state_store.enqueue_outbox_event(
            "qt_action",
            {
                "kind": "active_upsert",
                "payload": {
                    "item": projected_item,
                    "source": "clipboard",
                },
            },
        )
        return {
            "ok": True,
            "active_item_id": active_item_id,
            "record_id": record_id,
            "qt_event_id": event_id,
            "item": projected_item,
        }

    def _ensure_source_snapshot_background(self) -> None:
        """Kick off one background source refresh when the local SQLite snapshot is empty."""
        try:
            result = PortalRuntime.ensure_source_snapshot_refresh_started()
        except Exception as exc:
            log_warning(f"源表快照缺失，后台刷新触发失败: {exc}")
            return
        if result.get("source_refresh_started"):
            self._clear_read_cache()

    def _run_scheduled_source_refresh(self) -> None:
        if _mock_external_enabled():
            return
        has_snapshot = PortalRuntime.source_snapshot_ready("ALL")
        result = PortalRuntime.refresh_sources_once(
            force=not has_snapshot,
            min_interval_seconds=30 * 60,
            defer_if_busy=has_snapshot,
        )
        if result.get("source_refresh_deferred"):
            if not PortalRuntime.state_store.put_backend_runtime_async(
                "source_refresh_deferred",
                {
                    "deferred_at": time.time(),
                    "reason": result.get("source_refresh_defer_reason") or "",
                    "runtime_pressure": result.get("runtime_pressure") or {},
                },
            ):
                PortalRuntime.state_store.put_backend_runtime(
                    "source_refresh_deferred",
                    {
                        "deferred_at": time.time(),
                        "reason": result.get("source_refresh_defer_reason") or "",
                        "runtime_pressure": result.get("runtime_pressure") or {},
                    },
            )
            return
        if result.get("refreshed"):
            self._clear_read_cache()
        for warning in result.get("warnings") or []:
            log_warning(str(warning))

    def _collect_backend_cleanup_payload(self) -> dict:
        cleanup = PortalRuntime.service.cleanup_action_jobs()
        queue_removed = PortalRuntime.state_store.cleanup_runtime_queue_items(
            retention_seconds=3 * 24 * 3600,
            max_delete=500,
        )
        outbox_removed = PortalRuntime.state_store.cleanup_outbox_events(
            done_retention_seconds=24 * 3600,
            failed_retention_seconds=7 * 24 * 3600,
            max_delete=1000,
        )
        append_events_removed = PortalRuntime.state_store.cleanup_append_events(
            retention_seconds=3 * 24 * 3600,
            keep_latest=5000,
            max_delete=2000,
        )
        undo_removed = PortalRuntime.state_store.cleanup_notice_undo_actions(
            retain_days=7,
        )
        attachment_removed = (
            PortalRuntime.state_store.cleanup_notice_upload_attachments()
        )
        clipboard_removed = PortalRuntime.state_store.cleanup_clipboard_candidates()
        dialog_removed = PortalRuntime.state_store.cleanup_dialog_sessions()
        mop_temp_signature_removed = (
            PortalRuntime.state_store.cleanup_mop_temporary_signature_sessions()
        )
        return {
            **cleanup,
            "runtime_queue_removed": queue_removed,
            "outbox_removed": outbox_removed,
            "append_events_removed": append_events_removed,
            "undo_removed": undo_removed,
            "attachment_removed": attachment_removed,
            "clipboard_removed": clipboard_removed,
            "dialog_removed": dialog_removed,
            "mop_temp_signature_removed": mop_temp_signature_removed,
            "cleaned_at": time.time(),
        }

    def _run_scheduled_job_cleanup(self) -> None:
        try:
            payload = self._collect_backend_cleanup_payload()
            if not PortalRuntime.state_store.put_backend_runtime_async(
                "job_cleanup",
                payload,
            ):
                PortalRuntime.state_store.put_backend_runtime("job_cleanup", payload)
        except Exception as exc:
            log_warning(f"后台任务状态清理失败: {exc}")

    def _run_scheduled_token_refresh(self) -> None:
        if _mock_external_enabled():
            return
        token = check_token_status()
        payload = {
            "checked_at": time.time(),
            "has_token": bool(token),
            "token_expire_time": int(getattr(config, "token_expire_time", 0) or 0),
        }
        if not PortalRuntime.state_store.put_backend_runtime_async(
            "token_status",
            payload,
        ):
            PortalRuntime.state_store.put_backend_runtime("token_status", payload)

    def _run_scheduled_sqlite_maintenance(self) -> None:
        try:
            pressure = PortalRuntime.runtime_pressure()
            if pressure.get("busy"):
                payload = {
                    "skipped_at": time.time(),
                    "reason": "runtime_busy",
                    "runtime_pressure": pressure,
                }
                if not PortalRuntime.state_store.put_backend_runtime_async(
                    "sqlite_maintenance",
                    payload,
                ):
                    PortalRuntime.state_store.put_backend_runtime(
                        "sqlite_maintenance",
                        payload,
                    )
                return
            stats = PortalRuntime.state_store.get_database_stats()
            wal_threshold = int(
                _env_float(
                    "CLIPFLOW_SQLITE_WAL_CHECKPOINT_BYTES",
                    32 * 1024 * 1024,
                    minimum=1024 * 1024,
                    maximum=1024 * 1024 * 1024,
                )
            )
            freelist_threshold = int(
                _env_float(
                    "CLIPFLOW_SQLITE_CHECKPOINT_FREELIST_PAGES",
                    2048,
                    minimum=64,
                    maximum=100000,
                )
            )
            wal_bytes = int(stats.get("wal_bytes") or 0)
            freelist_count = int(stats.get("freelist_count") or 0)
            if wal_bytes < wal_threshold and freelist_count < freelist_threshold:
                payload = {
                    "checked_at": time.time(),
                    "checkpointed": False,
                    "wal_bytes": wal_bytes,
                    "freelist_count": freelist_count,
                }
                if not PortalRuntime.state_store.put_backend_runtime_async(
                    "sqlite_maintenance",
                    payload,
                ):
                    PortalRuntime.state_store.put_backend_runtime(
                        "sqlite_maintenance",
                        payload,
                    )
                return
            result = PortalRuntime.state_store.checkpoint_database(truncate=True)
            payload = {
                "checked_at": time.time(),
                "checkpointed": True,
                "wal_bytes": wal_bytes,
                "freelist_count": freelist_count,
                "result": result,
            }
            if not PortalRuntime.state_store.put_backend_runtime_async(
                "sqlite_maintenance",
                payload,
            ):
                PortalRuntime.state_store.put_backend_runtime(
                    "sqlite_maintenance",
                    payload,
                )
        except Exception as exc:
            log_warning(f"SQLite 后台维护失败: {exc}")

    def _run_notice_undo_job(self, job_id: str) -> None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return
        job = PortalRuntime.service.get_job(job_id) or {}
        request = job.get("request") if isinstance(job.get("request"), dict) else {}
        undo_id = str(request.get("undo_id") or "").strip()
        requested_by = str(request.get("_auth_open_id") or "").strip()
        try:
            PortalRuntime.service.mark_job(
                job_id,
                phase="undoing_remote",
                upload_message="正在回退多维记录",
            )
            result = PortalRuntime.execute_notice_undo(
                undo_id,
                job_id=job_id,
                requested_by=requested_by,
            )
            PortalRuntime.service.mark_job(
                job_id,
                phase="success",
                record_id=str((result or {}).get("record_id") or ""),
                upload_message=str((result or {}).get("message") or "回退成功"),
                error="",
                error_retryable=False,
            )
            PortalRuntime.clear_payload_cache()
            self._clear_read_cache()
        except Exception as exc:
            if undo_id:
                PortalRuntime.state_store.mark_notice_undo_action(
                    undo_id,
                    "available",
                    error=str(exc),
                )
            PortalRuntime.service.mark_job(
                job_id,
                phase="failed",
                error=str(exc),
                upload_message=str(exc),
            )

    def _start_scheduler(self) -> None:
        if self._scheduler is not None:
            return
        if BackgroundScheduler is None:
            log_warning("APScheduler 不可用，后端定时任务降级为旧线程。")
            return
        scheduler = BackgroundScheduler(
            timezone="Asia/Shanghai",
            daemon=True,
        )
        scheduler.add_job(
            self._write_runtime_heartbeat,
            "interval",
            seconds=30,
            id="backend_heartbeat",
            replace_existing=True,
            max_instances=1,
        )
        scheduler.add_job(
            self._run_scheduled_job_cleanup,
            "interval",
            hours=1,
            id="job_cleanup",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        scheduler.add_job(
            self._run_scheduled_sqlite_maintenance,
            "interval",
            minutes=30,
            id="sqlite_maintenance",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        scheduler.add_job(
            self._run_scheduled_token_refresh,
            "interval",
            minutes=10,
            id="feishu_token_refresh",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        scheduler.add_job(
            self._run_scheduled_job_cleanup,
            "date",
            run_date=dt.datetime.now() + dt.timedelta(seconds=30),
            id="job_cleanup_startup",
            replace_existing=True,
            max_instances=1,
        )
        if not _mock_external_enabled():
            scheduler.add_job(
                self._run_scheduled_token_refresh,
                "date",
                run_date=dt.datetime.now() + dt.timedelta(seconds=5),
                id="feishu_token_refresh_startup",
                replace_existing=True,
                max_instances=1,
            )
        scheduler.start()
        self._scheduler = scheduler
        self._write_runtime_heartbeat()

    def _stop_scheduler(self) -> None:
        scheduler = self._scheduler
        self._scheduler = None
        if scheduler:
            with suppress(Exception):
                scheduler.shutdown(wait=False)

    async def _job_stream(self, request: Request) -> AsyncIterator[bytes]:
        job_id = str(request.query_params.get("job_id") or "").strip()
        session = self._current_session(request)
        if session is None:
            payload = json.dumps(
                {"ok": False, "error": "请先使用飞书扫码登录。", "auth_required": True},
                ensure_ascii=False,
            )
            yield f"event: error\ndata: {payload}\n\n".encode("utf-8")
            return
        if not job_id and not PortalRuntime.auth_manager.is_admin(session):
            payload = json.dumps(
                {"ok": False, "error": "只有管理员可以查看任务队列状态。"},
                ensure_ascii=False,
            )
            yield f"event: error\ndata: {payload}\n\n".encode("utf-8")
            return
        last_payload = ""
        interval_s = _env_float(
            "CLIPFLOW_JOB_SSE_INTERVAL_SECONDS",
            0.75 if job_id else 2.0,
            minimum=0.5,
            maximum=10.0,
        )
        sse_key, sse_id = self._register_sse(request, "jobs", job_id)
        try:
            while self._sse_active(sse_key, sse_id) and not await request.is_disconnected():
                payload: dict
                if job_id:
                    job = PortalRuntime.service.get_job(job_id) or {}
                    if not job:
                        payload = {
                            "job_id": job_id,
                            "job": {
                                "job_id": job_id,
                                "phase": "failed",
                                "error": "任务状态已丢失，请核对多维后重试。",
                            },
                        }
                    elif not self._job_visible_to_session(job, session):
                        payload = {
                            "job_id": job_id,
                            "job": {"job_id": job_id, "phase": "failed", "error": "无权查看该任务。"},
                        }
                        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
                        yield f"event: job\ndata: {raw}\n\n".encode("utf-8")
                        return
                    else:
                        job["job_id"] = str(job.get("job_id") or job_id)
                        payload = {"job_id": job_id, "job": job}
                    payload_job = (
                        (payload.get("job") or {})
                        if isinstance(payload, dict)
                        else {}
                    )
                    terminal = str(payload_job.get("phase") or "") in {
                        "success",
                        "failed",
                    }
                else:
                    payload = {"stats": _queue_stats(), "time": time.time()}
                    terminal = False
                raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
                if raw != last_payload:
                    event_id = hashlib.sha1(raw.encode("utf-8")).hexdigest()
                    yield f"id: {event_id}\nevent: job\ndata: {raw}\n\n".encode("utf-8")
                    last_payload = raw
                if job_id and terminal:
                    return
                await asyncio.sleep(interval_s)
        finally:
            self._unregister_sse(sse_key, sse_id)

    async def _heartbeat_stream(
        self, request: Request, *, event_name: str
    ) -> AsyncIterator[bytes]:
        while not await request.is_disconnected():
            payload = json.dumps(
                {"time": time.time(), "stats": _queue_stats()},
                ensure_ascii=False,
                sort_keys=True,
            )
            yield f"event: {event_name}\ndata: {payload}\n\n".encode("utf-8")
            await asyncio.sleep(5.0)

    async def _qt_event_stream(self, request: Request) -> AsyncIterator[bytes]:
        last_heartbeat = 0.0
        heartbeat_seconds = _env_float(
            "CLIPFLOW_QT_EVENT_SSE_HEARTBEAT_SECONDS",
            10.0,
            minimum=3.0,
            maximum=60.0,
        )
        idle_sleep_seconds = _env_float(
            "CLIPFLOW_QT_EVENT_SSE_IDLE_SECONDS",
            1.0,
            minimum=0.2,
            maximum=5.0,
        )
        sse_key, sse_id = self._register_sse(request, "qt_events")
        try:
            while self._sse_active(sse_key, sse_id) and not await request.is_disconnected():
                items = await asyncio.to_thread(
                    PortalRuntime.state_store.lease_outbox_events,
                    "qt_action",
                    limit=1,
                    lease_seconds=30,
                )
                if items:
                    for item in items:
                        raw = json.dumps(item, ensure_ascii=False, sort_keys=True)
                        event_id = int(item.get("id") or 0)
                        yield (
                            f"id: {event_id}\nevent: qt_event\ndata: {raw}\n\n"
                        ).encode("utf-8")
                    await asyncio.sleep(0.05)
                    continue
                now = time.monotonic()
                if now - last_heartbeat >= heartbeat_seconds:
                    last_heartbeat = now
                    payload = json.dumps(
                        {"time": time.time(), "stats": _queue_stats()},
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    yield f"event: heartbeat\ndata: {payload}\n\n".encode("utf-8")
                await asyncio.sleep(idle_sleep_seconds)
        finally:
            self._unregister_sse(sse_key, sse_id)

    async def _qt_active_items_stream(self, request: Request) -> AsyncIterator[bytes]:
        session = self._current_session(request)
        if session is None:
            payload = json.dumps(
                {"ok": False, "error": "请先使用飞书扫码登录。", "auth_required": True},
                ensure_ascii=False,
            )
            yield f"event: error\ndata: {payload}\n\n".encode("utf-8")
            return
        try:
            scope = self._authorized_scope_or_error(
                session, request.query_params.get("scope") or "ALL"
            )
        except Exception as exc:
            payload = json.dumps(
                {"ok": False, "error": str(exc), "auth_required": False},
                ensure_ascii=False,
            )
            yield f"event: error\ndata: {payload}\n\n".encode("utf-8")
            return
        last_payload = ""
        active_interval = _env_float(
            "CLIPFLOW_QT_ACTIVE_SSE_ACTIVE_SECONDS",
            3.0,
            minimum=1.0,
            maximum=30.0,
        )
        idle_interval = _env_float(
            "CLIPFLOW_QT_ACTIVE_SSE_IDLE_SECONDS",
            10.0,
            minimum=3.0,
            maximum=60.0,
        )
        sse_key, sse_id = self._register_sse(request, "qt_active_items", scope)
        last_version_signature = ""
        try:
            while self._sse_active(sse_key, sse_id) and not await request.is_disconnected():
                ongoing_meta = PortalRuntime.state_store.get_ongoing_snapshot_meta()
                source_active = PortalRuntime.state_store.active_source_snapshot_meta()
                qt_active_meta = PortalRuntime.state_store.qt_active_items_meta()
                source_signature = ":".join(
                    [
                        str(source_active.get("snapshot_id") or ""),
                        str(source_active.get("updated_at") or ""),
                    ]
                )
                version_signature = hashlib.sha1(
                    "|".join(
                        [
                            str(scope),
                            str(ongoing_meta.get("snapshot_id") or ""),
                            str(ongoing_meta.get("updated_at") or ""),
                            str(ongoing_meta.get("count") or 0),
                            str(ongoing_meta.get("hash") or ""),
                            source_signature,
                            str(qt_active_meta.get("active") or 0),
                            str(qt_active_meta.get("deleted") or 0),
                            str(qt_active_meta.get("updated_at") or 0),
                        ]
                    ).encode("utf-8")
                ).hexdigest()
                if last_payload and version_signature == last_version_signature:
                    await asyncio.sleep(idle_interval)
                    continue
                snapshot = PortalRuntime.state_store.get_ongoing_snapshot()
                scoped_signature, scoped_count = self._scoped_ongoing_signature(
                    scope, list(snapshot.get("items") or [])
                )
                qt_scoped_signature, qt_scoped_count = self._scoped_qt_active_signature(
                    scope
                )
                qt_active_items = dict(PortalRuntime.state_store.qt_active_items_stats())
                qt_active_items.pop("checked_at", None)
                display_signature = hashlib.sha1(
                    f"{scoped_signature}|{qt_scoped_signature}|{source_signature}".encode("utf-8")
                ).hexdigest()
                payload = json.dumps(
                    {
                        "scope": scope,
                        "snapshot_id": snapshot.get("snapshot_id", ""),
                        "count": snapshot.get("count", 0),
                        "scope_count": scoped_count,
                        "qt_scope_count": qt_scoped_count,
                        "scope_signature": scoped_signature,
                        "qt_scope_signature": qt_scoped_signature,
                        "display_signature": display_signature,
                        "source_snapshot_id": source_active.get("snapshot_id", ""),
                        "source_snapshot_updated_at": source_active.get("updated_at", 0),
                        "updated_at": snapshot.get("updated_at", 0),
                        "qt_active_items": qt_active_items,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
                if payload != last_payload:
                    event_id = hashlib.sha1(payload.encode("utf-8")).hexdigest()
                    yield (
                        f"id: {event_id}\nevent: qt_active_items\ndata: {payload}\n\n"
                    ).encode("utf-8")
                    last_payload = payload
                    last_version_signature = version_signature
                    await asyncio.sleep(active_interval)
                else:
                    last_version_signature = version_signature
                    await asyncio.sleep(idle_interval)
        finally:
            self._unregister_sse(sse_key, sse_id)

    async def _proxy_request(self, path: str, request: Request) -> Response:
        return Response(
            json.dumps({"ok": False, "error": "Not Found"}, ensure_ascii=False),
            status_code=404,
            media_type="application/json",
        )

    def start(self) -> str:
        if self._server and self._thread and self._thread.is_alive():
            return self.get_url()
        try:
            import uvicorn
        except Exception as exc:
            raise RuntimeError(f"Uvicorn 不可用: {exc}") from exc

        self._initialize_portal_handler_state()
        PortalRuntime.ensure_message_workers()
        PortalRuntime.ensure_action_worker()
        PortalRuntime.ensure_upload_wait_worker()
        PortalRuntime.ensure_source_refresh_worker()
        for job_id in PortalRuntime.service.recoverable_action_job_ids():
            PortalRuntime.enqueue_initial_message_or_upload_job(job_id)

        bound_port = find_available_port(self.host, self.preferred_port)
        self.bound_port = bound_port
        self._app = self._build_app()
        config = uvicorn.Config(
            self._app,
            host=self.host,
            port=bound_port,
            log_level="warning",
            access_log=False,
            log_config=None,
        )
        self._server = uvicorn.Server(config)

        def _run() -> None:
            try:
                self._server.run()
            except Exception as exc:
                log_error(f"FastAPI门户服务异常: {exc}")

        self._thread = threading.Thread(
            target=_run,
            name="ClipFlowFastAPIBackend",
            daemon=True,
        )
        self._thread.start()
        if not _wait_until_listening(self.host, bound_port):
            log_warning(f"FastAPI门户端口监听确认超时: {self.get_url()}")
        self._start_scheduler()
        log_info(f"FastAPI门户已启动: public={self.get_url()}")
        return self.get_url()

    def stop(self) -> None:
        self._stop_scheduler()
        try:
            PortalRuntime.stop_source_refresh_worker()
        except Exception:
            pass
        server = self._server
        if server:
            server.should_exit = True
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=3)
        if thread and thread.is_alive() and server:
            server.force_exit = True
            thread.join(timeout=1)
        self._thread = None
        self._server = None
        PortalRuntime.stop_message_workers()
        PortalRuntime.stop_action_worker()
        PortalRuntime.stop_upload_wait_worker()
        PortalRuntime.notice_callback = None
        PortalRuntime.ongoing_callback = None
        PortalRuntime.ongoing_delete_callback = None
        PortalRuntime.maintenance_action_callback = None
        with suppress(Exception):
            self._background_executor.shutdown(wait=False, cancel_futures=True)
        try:
            PortalRuntime.state_store.shutdown_write_worker(timeout=2.0)
        except Exception as exc:
            log_warning(f"SQLite写入队列停止失败: {exc}")
        self.bound_port = None
        self._shutdown_event.set()

    def wait(self) -> None:
        self._shutdown_event.wait()

    def get_url(self) -> str:
        port = self.bound_port or self.preferred_port
        display_host = self.host if self.host != "0.0.0.0" else "127.0.0.1"
        return f"http://{display_host}:{port}"

    def set_notice_callback(self, callback) -> None:
        self.notice_callback = callback
        PortalRuntime.notice_callback = callback

    def set_ongoing_callback(self, callback) -> None:
        self.ongoing_callback = callback
        PortalRuntime.ongoing_callback = callback

    def set_ongoing_delete_callback(self, callback) -> None:
        self.ongoing_delete_callback = callback
        PortalRuntime.ongoing_delete_callback = callback

    def set_maintenance_action_callback(self, callback) -> None:
        self.maintenance_action_callback = callback
        PortalRuntime.maintenance_action_callback = callback

    def _sync_qt_active_item_upload_result(
        self,
        *,
        job_id: str,
        record_id: str,
        active_item_id: str = "",
        job_snapshot: dict[str, Any] | None = None,
    ) -> None:
        target_record_id = str(record_id or "").strip()
        if not target_record_id or is_local_record_id(target_record_id):
            return
        job = job_snapshot if isinstance(job_snapshot, dict) else {}
        prepared = job.get("prepared") if isinstance(job.get("prepared"), dict) else {}
        active_item_id = str(
            active_item_id
            or prepared.get("active_item_id")
            or job.get("active_item_id")
            or ""
        ).strip()
        placeholder_ids = {
            str(value or "").strip()
            for value in (
                prepared.get("record_id"),
                prepared.get("target_record_id"),
                job.get("record_id"),
                job.get("target_record_id"),
            )
            if str(value or "").strip()
        }
        matched_item: dict[str, Any] | None = None
        for item in PortalRuntime.state_store.list_qt_active_items():
            payload = item.get("payload") if isinstance(item, dict) else {}
            payload = payload if isinstance(payload, dict) else {}
            item_active_id = str(
                payload.get("active_item_id") or item.get("active_item_id") or ""
            ).strip()
            item_record_ids = {
                str(value or "").strip()
                for value in (
                    item.get("record_id"),
                    payload.get("record_id"),
                    payload.get("target_record_id"),
                )
                if str(value or "").strip()
            }
            if active_item_id and item_active_id == active_item_id:
                matched_item = item
                break
            if target_record_id in item_record_ids:
                matched_item = item
                break
            if placeholder_ids and item_record_ids.intersection(placeholder_ids):
                matched_item = item
                break
        if not matched_item:
            return
        payload = matched_item.get("payload") if isinstance(matched_item, dict) else {}
        payload = dict(payload) if isinstance(payload, dict) else {}
        if active_item_id:
            payload["active_item_id"] = active_item_id
        else:
            active_item_id = str(
                payload.get("active_item_id") or matched_item.get("active_item_id") or ""
            ).strip()
        old_record_id = str(
            payload.get("record_id") or matched_item.get("record_id") or ""
        ).strip()
        if old_record_id and old_record_id != target_record_id:
            aliases = payload.get("record_id_aliases")
            if not isinstance(aliases, list):
                aliases = []
            if old_record_id not in aliases:
                aliases.append(old_record_id)
            payload["record_id_aliases"] = aliases[-8:]
        payload["record_id"] = target_record_id
        payload["target_record_id"] = target_record_id
        payload["_is_placeholder_record"] = False
        payload["binding_status"] = "bound"
        notice_type = str(
            payload.get("notice_type") or matched_item.get("notice_type") or ""
        ).strip()
        if notice_type == "事件通告":
            source = str(payload.get("event_source") or payload.get("source") or "").strip()
            if source:
                payload["event_source"] = source
                payload["source"] = source
            try:
                event_identity_key = PortalRuntime._event_notice_identity_key(payload)
            except Exception:
                event_identity_key = ""
            if event_identity_key:
                payload["event_identity_key"] = event_identity_key
                try:
                    payload["event_match_fields"] = PortalRuntime._event_match_fields(payload)
                except Exception:
                    pass
        section = str(matched_item.get("section") or "").strip()
        sort_order = int(matched_item.get("sort_order") or 0)
        origin = str(matched_item.get("origin") or payload.get("origin") or "").strip()
        PortalRuntime.state_store.upsert_qt_active_item(
            payload,
            section=section,
            sort_order=sort_order,
            origin=origin,
        )
        projected_item = {
            "active_item_id": active_item_id,
            "record_id": target_record_id,
            "notice_type": notice_type,
            "section": section,
            "sort_order": sort_order,
            "origin": origin,
            "payload": payload,
        }
        PortalRuntime.state_store.enqueue_outbox_event(
            "qt_action",
            {
                "kind": "active_upsert",
                "payload": {
                    "item": projected_item,
                    "source": "qt_upload_result",
                    "job_id": str(job_id or "").strip(),
                },
            },
        )

    def mark_job_upload_result(self, job_id: str, **kwargs) -> None:
        job_snapshot = PortalRuntime.service.get_job(job_id) or {}
        PortalRuntime.service.mark_action_upload_result(job_id, **kwargs)
        if bool(kwargs.get("success")):
            self._sync_qt_active_item_upload_result(
                job_id=job_id,
                record_id=str(kwargs.get("record_id") or ""),
                active_item_id=str(kwargs.get("active_item_id") or ""),
                job_snapshot=job_snapshot,
            )

    def mark_job_progress(self, job_id: str, **patch) -> None:
        if not job_id:
            return
        PortalRuntime.service.mark_job(job_id, **patch)
        phase = str((patch or {}).get("phase") or "").strip()
        if phase == "uploading":
            PortalRuntime.track_upload_wait_job(job_id)
        elif phase in {"success", "failed"}:
            try:
                PortalRuntime.state_store.mark_runtime_queue_item(
                    "upload_wait",
                    job_id,
                    "done" if phase == "success" else "failed",
                    error=str((patch or {}).get("error") or ""),
                )
            except Exception:
                pass

    def get_job(self, job_id: str) -> dict | None:
        return PortalRuntime.service.get_job(job_id)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ClipFlow FastAPI 后端门户")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--mock-external",
        action="store_true",
        help="禁用真实飞书/多维写入和后台刷新，仅用于本地自动化测试。",
    )
    parser.add_argument(
        "--allow-real-external",
        action="store_true",
        help="允许直接命令行启动的后端执行真实飞书/多维写入。",
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    if args.mock_external:
        os.environ["CLIPFLOW_BACKEND_MOCK_EXTERNAL"] = "1"
    if args.allow_real_external:
        os.environ["CLIPFLOW_REQUIRE_REAL_EXTERNAL_CONFIRM"] = "1"
        os.environ["CLIPFLOW_REAL_EXTERNAL_CONFIRMED"] = "1"
    controller = FastAPIPortalController(host=args.host, port=args.port)
    url = controller.start()
    print(f"ClipFlow FastAPI 后端已启动: {url}")
    try:
        controller.wait()
    except KeyboardInterrupt:
        controller.stop()


if __name__ == "__main__":
    main()

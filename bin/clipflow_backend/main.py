# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import mimetypes
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from contextlib import suppress
from collections import deque
from pathlib import Path
from typing import AsyncIterator
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from clipflow_backend.api_models import (
    APIModel,
    AuthPermissionsSaveRequest,
    ChangeTargetConfirmRequest,
    ChangeTargetLookupRequest,
    GenerateTemplatesRequest,
    HandoverLinksAuthRequest,
    HandoverLinksSaveRequest,
    HandoverPasswordResetConfirmRequest,
    MockPressureRequest,
    NoticeMemoryHistorySaveRequest,
    NoticeMemoryHistoryScanRequest,
    NoticeMemoryImportRequest,
    NoticeTargetLookupRequest,
    NoticeUndoApplyRequest,
    OngoingDeleteRequest,
    PermissionRequestConfirm,
    PermissionRequestCreate,
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
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME
from lan_bitable_template_portal.identity_utils import normalize_notice_identity_payload
from lan_bitable_template_portal.portal_service import (
    CHANGE_SOURCE_APP_TOKEN,
    CHANGE_SOURCE_TABLE_ID,
    DEFAULT_APP_TOKEN,
    DEFAULT_TABLE_ID,
    NOTICE_TYPE_CHANGE,
    NOTICE_TYPE_MAINTENANCE,
    NOTICE_TYPE_REPAIR,
    PortalError,
    REPAIR_SOURCE_APP_TOKEN,
    REPAIR_SOURCE_TABLE_ID,
    REPAIR_SYNC_TABLE_ID,
    SCOPE_OPTIONS,
    ZHIHANG_CHANGE_APP_TOKEN,
    ZHIHANG_CHANGE_TABLE_ID,
    external_real_write_guard,
)
from lan_bitable_template_portal.portal_service import MaintenancePortalService
from lan_bitable_template_portal.portal_auth import PortalAuthManager
from lan_bitable_template_portal.state_store import LanPortalStateStore
from upload_event_module.config import config, get_field_config
from upload_event_module.core.parser import extract_event_info
from upload_event_module.services.feishu_service import check_token_status
from upload_event_module.services.service_registry import query_record_by_id
from upload_event_module.logger import log_error, log_info, log_warning
from upload_event_module.services.robot_webhook import send_text_to_open_ids

try:
    from apscheduler.schedulers.background import BackgroundScheduler
except Exception:
    BackgroundScheduler = None


def _wait_until_listening(host: str, port: int, *, timeout_s: float = 3.0) -> bool:
    probe_host = str(host or "127.0.0.1").strip()
    if probe_host in {"0.0.0.0", "::"}:
        probe_host = "127.0.0.1"
    deadline = time.monotonic() + max(0.1, float(timeout_s or 0))
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((probe_host, int(port)), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def _mock_external_enabled() -> bool:
    return os.environ.get("CLIPFLOW_BACKEND_MOCK_EXTERNAL") == "1"


def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    try:
        value = float(str(os.environ.get(name, "") or "").strip() or default)
    except Exception:
        value = float(default)
    return max(float(minimum), min(float(maximum), value))


def _external_guard_status() -> dict:
    return external_real_write_guard()


def _send_text_to_open_ids_guarded(text: str, recipients: list[str]) -> tuple[bool, str, list[dict]]:
    clean_recipients = [
        str(open_id or "").strip()
        for open_id in (recipients or [])
        if str(open_id or "").strip()
    ]
    guard = _external_guard_status()
    if guard.get("mock_external"):
        return True, "mock external send skipped", [
            {"open_id": open_id, "ok": True, "message": "mock external send skipped"}
            for open_id in clean_recipients
        ]
    if not guard.get("real_write_allowed"):
        return False, str(guard.get("reason") or "真实外部写入未确认。"), []
    return send_text_to_open_ids(text, clean_recipients)


def _queue_stats() -> dict:
    qt_outbox_counts: dict[str, int] = {}
    try:
        qt_outbox_counts = PortalRuntime.state_store.count_outbox_events("qt_action")
    except Exception:
        qt_outbox_counts = {}
    with PortalRuntime.message_queue_lock:
        message_queue_size = len(PortalRuntime.message_queue)
        message_workers = sum(
            1 for worker in PortalRuntime.message_worker_threads if worker.is_alive()
        )
    with PortalRuntime.action_queue_lock:
        qt_queue_size = len(PortalRuntime.action_queue)
        qt_worker_alive = bool(
            PortalRuntime.action_worker_thread
            and PortalRuntime.action_worker_thread.is_alive()
        )
    with PortalRuntime.upload_wait_lock:
        upload_wait_size = len(PortalRuntime.upload_wait_jobs)
        upload_wait_alive = bool(
            PortalRuntime.upload_wait_thread
            and PortalRuntime.upload_wait_thread.is_alive()
        )
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


def _field_names(metas: list) -> set[str]:
    names: set[str] = set()
    for meta in metas or []:
        field_name = str(getattr(meta, "field_name", "") or "").strip()
        if field_name:
            names.add(field_name)
    return names


def _missing_fields(
    names: set[str],
    fields: list[str] | None,
    aliases: dict[str, list[str]] | None = None,
) -> list[str]:
    aliases = aliases or {}
    missing: list[str] = []
    for field_name in fields or []:
        candidates = [str(field_name or "").strip()]
        candidates.extend(str(item or "").strip() for item in aliases.get(field_name, []))
        candidates = [item for item in candidates if item]
        if not candidates:
            continue
        if not any(candidate in names for candidate in candidates):
            missing.append(candidates[0])
    return missing


def _check_field_set(
    service: MaintenancePortalService,
    *,
    label: str,
    app_token: str,
    table_id: str,
    required: list[str],
    optional: list[str] | None = None,
    required_aliases: dict[str, list[str]] | None = None,
    optional_aliases: dict[str, list[str]] | None = None,
) -> dict:
    app_token = str(app_token or "").strip()
    table_id = str(table_id or "").strip()
    if not app_token or not table_id:
        return {
            "label": label,
            "status": "warning",
            "message": "app_token 或 table_id 未配置，已跳过字段检查。",
            "missing_required": list(required or []),
            "missing_optional": [],
            "field_count": 0,
        }
    if not hasattr(service, "_load_table_fields"):
        return {
            "label": label,
            "status": "warning",
            "message": "当前服务对象不支持字段预检。",
            "missing_required": [],
            "missing_optional": [],
            "field_count": 0,
        }
    try:
        metas, _ = service._load_table_fields(app_token=app_token, table_id=table_id)
        names = _field_names(metas)
        missing_required = _missing_fields(names, required, required_aliases)
        missing_optional = _missing_fields(names, optional, optional_aliases)
        status = "fail" if missing_required else "warning" if missing_optional else "ok"
        message = (
            "缺少必需字段。"
            if missing_required
            else "缺少可选字段，部分回填可能为空。"
            if missing_optional
            else "字段检查通过。"
        )
        return {
            "label": label,
            "status": status,
            "message": message,
            "missing_required": missing_required,
            "missing_optional": missing_optional,
            "field_count": len(names),
        }
    except Exception as exc:
        return {
            "label": label,
            "status": "fail",
            "message": str(exc),
            "missing_required": [],
            "missing_optional": [],
            "field_count": 0,
        }


def _build_backend_preflight_report(service: MaintenancePortalService) -> dict:
    started = time.perf_counter()
    checks: list[dict] = []
    config_checks = [
        ("飞书 App ID", bool(str(config.app_id or "").strip())),
        ("飞书 App Secret", bool(str(config.app_secret or "").strip())),
        ("目标 app_token", bool(str(config.app_token or "").strip())),
        ("维保目标表", bool(str(config.table_id_weibao or "").strip())),
        ("变更目标表", bool(str(config.table_id_biangeng or "").strip())),
        ("检修目标表", bool(str(config.table_id_overhaul or "").strip())),
    ]
    for label, ok in config_checks:
        checks.append(
            {
                "label": label,
                "status": "ok" if ok else "warning",
                "message": "已配置。" if ok else "未配置，相关真实链路会失败。",
            }
        )
    checks.extend(
        [
            _check_field_set(
                service,
                label="维保源表字段",
                app_token=getattr(service, "app_token", "") or DEFAULT_APP_TOKEN,
                table_id=getattr(service, "table_id", "") or DEFAULT_TABLE_ID,
                required=[
                    "楼栋",
                    "维护总项",
                    "维护实施状态",
                    "计划维护月份",
                    "专业类别",
                    "维护周期",
                ],
                optional=["维护编号", "维护项目"],
            ),
            _check_field_set(
                service,
                label="变更源表字段",
                app_token=CHANGE_SOURCE_APP_TOKEN,
                table_id=CHANGE_SOURCE_TABLE_ID,
                required=[
                    "变更简述",
                    "变更进度",
                    "变更楼栋",
                    "专业",
                    "变更等级（阿里）",
                ],
                optional=[
                    "变更开始日期（阿里）",
                    "变更结束日期（阿里）",
                ],
            ),
            _check_field_set(
                service,
                label="检修源表字段",
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_SOURCE_TABLE_ID,
                required=["检修通告名称", "所属数据中心/楼栋-使用"],
                optional=[
                    "维修名称",
                    "所属专业",
                    "专业（推送消息用）",
                    "故障发生时间",
                    "维修开始时间",
                    "期望完成时间",
                    "事件描述",
                    "对应事件等级",
                    "对应来源",
                    "设备检修关联",
                ],
            ),
            _check_field_set(
                service,
                label="智航变更源表字段",
                app_token=ZHIHANG_CHANGE_APP_TOKEN,
                table_id=ZHIHANG_CHANGE_TABLE_ID,
                required=["进展"],
                optional=["标题"],
                required_aliases={"进展": ["进度"]},
                optional_aliases={
                    "标题": [
                        "名称",
                        "变更名称",
                        "变更标题",
                        "变更简述",
                        "工作内容",
                    ]
                },
            ),
            _check_field_set(
                service,
                label="检修同步表字段",
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_SYNC_TABLE_ID,
                required=[],
                optional=["名称（标题）", "名称"],
            ),
            _check_field_set(
                service,
                label="维保目标表字段",
                app_token=config.app_token,
                table_id=config.table_id_weibao,
                required=list(get_field_config(NOTICE_TYPE_MAINTENANCE).values()),
            ),
            _check_field_set(
                service,
                label="变更目标表字段",
                app_token=config.app_token,
                table_id=config.table_id_biangeng,
                required=list(get_field_config(NOTICE_TYPE_CHANGE).values()),
            ),
            _check_field_set(
                service,
                label="检修目标表字段",
                app_token=config.app_token,
                table_id=config.table_id_overhaul,
                required=list(get_field_config(NOTICE_TYPE_REPAIR).values()),
            ),
        ]
    )
    failed = sum(1 for item in checks if item.get("status") == "fail")
    warnings = sum(1 for item in checks if item.get("status") == "warning")
    report = {
        "checked_at": time.time(),
        "duration_ms": round((time.perf_counter() - started) * 1000.0, 1),
        "status": "fail" if failed else "warning" if warnings else "ok",
        "failed": failed,
        "warnings": warnings,
        "checks": checks,
        "external_guard": _external_guard_status(),
    }
    return report


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
        self._sse_lock = threading.RLock()
        self._sse_connections: dict[tuple, int] = {}
        self._sse_connection_seq = 0

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
            return self._static_file_response(request, portal_index_file(), html=True)

        @app.get("/admin/history-memory")
        @app.get("/admin/history-memory/")
        async def admin_history_memory_page(request: Request):
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
            sqlite_stats: dict = {}
            source_snapshot_stats: dict = {}
            schema_status: dict = {}
            runtime_health: dict = {}
            with suppress(Exception):
                sqlite_stats = PortalRuntime.state_store.get_database_stats()
            with suppress(Exception):
                source_snapshot_stats = PortalRuntime.state_store.source_snapshot_stats()
            with suppress(Exception):
                schema_status = PortalRuntime.state_store.schema_health()
            with suppress(Exception):
                runtime_health = PortalRuntime.state_store.runtime_health_report()
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
                    "sqlite": sqlite_stats if isinstance(sqlite_stats, dict) else {},
                    "schema": schema_status if isinstance(schema_status, dict) else {},
                    "runtime_health": runtime_health if isinstance(runtime_health, dict) else {},
                    "source_snapshot": source_snapshot_stats if isinstance(source_snapshot_stats, dict) else {},
                    "read_cache": self._read_cache_stats(),
                    "static_cache": self._static_cache_stats(),
                    "singleflight": {
                        "payload_cache_inflight": len(PortalRuntime.payload_cache_inflight),
                        "payload_cache_entries": len(PortalRuntime.payload_cache),
                    },
                    "sse_connections": self._sse_stats(),
                    "recent_failed_jobs": recent_failed_jobs,
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
                result = await asyncio.to_thread(PortalRuntime.service.cleanup_action_jobs)
                result["cleaned_at"] = time.time()
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
            count = max(1, min(int(payload.get("count") or 10), 50))
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
                completed = await asyncio.to_thread(
                    subprocess.run,
                    args,
                    cwd=os.fspath(Path(__file__).resolve().parents[1]),
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                    **run_kwargs,
                )
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

        @app.get("/api/jobs/stream")
        async def jobs_stream(request: Request):
            return StreamingResponse(
                self._job_stream(request),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-store"},
            )

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
                open_id = str((session.get("user") or {}).get("open_id") or "")
                payload_key = "records" if request.url.path == "/api/records" else "workbench"
                payload = await asyncio.to_thread(
                    self._cached_service_payload,
                    (
                        payload_key,
                        open_id,
                        scope,
                        month,
                        specialty,
                        PortalRuntime._ongoing_items_marker(ongoing),
                    ),
                    lambda: PortalRuntime.service.query_records(
                        month=month,
                        specialty=specialty,
                        scope=scope,
                        ongoing_items=ongoing,
                    ),
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
                refresh_result = PortalRuntime.request_repair_source_refresh()
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

        @app.post("/api/maintenance-actions")
        @app.post("/api/workbench-actions")
        async def workbench_actions(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = (
                    await self._read_model_request(request, WorkbenchActionRequest)
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
                threading.Thread(
                    target=self._run_notice_undo_job,
                    args=(job_id,),
                    name=f"NoticeUndo-{job_id[:8]}",
                    daemon=True,
                ).start()
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
                code = str(data.pop("code") or "")
                recipients = PortalRuntime.auth_manager.admin_open_ids()
                labels = "、".join(data.get("requested_scope_labels") or [])
                reason = str(data.get("reason") or "").strip() or "未填写"
                text = (
                    "南通基地-运维灯塔工作台权限申请。\n"
                    f"申请人：{data.get('name') or '飞书用户'}\n"
                    f"openid：{data.get('open_id') or ''}\n"
                    f"申请范围：{labels}\n"
                    f"申请原因：{reason}\n"
                    f"申请编号：{data.get('request_id') or ''}\n"
                    f"验证码：{code}\n"
                    f"有效期至：{data.get('expires_at') or ''}\n"
                    "请管理员确认申请人身份和申请范围后，再将验证码告知申请人。"
                )
                ok, message, _ = _send_text_to_open_ids_guarded(text, recipients)
                if not ok:
                    PortalRuntime.auth_manager.mark_permission_request_notify_failed(
                        str(data.get("request_id") or "")
                    )
                    raise PortalError(f"通知管理员失败: {message}")
                activated = PortalRuntime.auth_manager.activate_permission_request(
                    str(data.get("request_id") or "")
                )
                data.update(activated)
                PortalRuntime.auth_manager.supersede_other_permission_requests(
                    open_id=str(data.get("open_id") or ""),
                    keep_request_id=str(data.get("request_id") or ""),
                )
                self._clear_read_cache(("auth_status",))
                data["notification"] = {
                    "ok": True,
                    "recipients_count": len(recipients),
                    "message": "已发送给管理员",
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
                    threading.Thread(
                        target=_send_text_to_open_ids_guarded,
                        args=(text, changed_open_ids),
                        name="LANPortalPermissionNotify",
                        daemon=True,
                    ).start()
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
                await self._read_model_request(request, QtCommandRequest)
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
                    command_payload = dict(payload.get("payload") or {})
                    command_payload["action_type"] = action_map[command]
                    command_payload = normalize_notice_identity_payload(command_payload)
                    data = await asyncio.to_thread(
                        PortalRuntime.execute_local_notice_upload,
                        command_payload,
                    )
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
                    threading.Thread(
                        target=self._run_notice_undo_job,
                        args=(job_id,),
                        name=f"ClipFlowUndoJob-{job_id[:8]}",
                        daemon=True,
                    ).start()
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
            self.mark_job_upload_result(
                job_id,
                success=bool(payload.get("success")),
                message=str(payload.get("message") or ""),
                record_id=str(payload.get("record_id") or ""),
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
        )
        try:
            PortalRuntime.service.ensure_snapshot_loaded()
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
            PortalRuntime.action_queue.clear()
            PortalRuntime.action_worker_stop = False
            PortalRuntime.action_queue_event.clear()
        with PortalRuntime.upload_wait_lock:
            PortalRuntime.upload_wait_jobs.clear()
            PortalRuntime.upload_wait_stop = False
            PortalRuntime.upload_wait_event.clear()
        with PortalRuntime.message_queue_lock:
            PortalRuntime.message_queue.clear()
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
    ) -> APIModel:
        payload = await cls._read_json_request(request, max_bytes=max_bytes)
        return parse_api_model(model_cls, payload)

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
    def _get_ongoing(scope: str) -> list[dict]:
        merged: list[dict] = []
        seen: set[str] = set()
        active_rows: list[dict] = []
        deleted_active_item_ids: set[str] = set()
        deleted_record_ids: set[str] = set()

        def _key(item: dict) -> str:
            return "|".join(
                [
                    str(item.get("active_item_id") or "").strip(),
                    str(item.get("target_record_id") or "").strip(),
                    str(item.get("source_record_id") or "").strip(),
                    str(item.get("title") or "").strip(),
                ]
            )

        def _item_deleted_in_qt_store(item: dict) -> bool:
            active_item_id = str(item.get("active_item_id") or "").strip()
            record_id = str(
                item.get("target_record_id")
                or ""
            ).strip()
            return bool(
                (active_item_id and active_item_id in deleted_active_item_ids)
                or (record_id and record_id in deleted_record_ids)
            )

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
            key = _key(item)
            if key in seen:
                return
            seen.add(key)
            merged.append(dict(item))

        try:
            active_rows = PortalRuntime.state_store.list_qt_active_items(
                include_deleted=True
            )
            for active_item in active_rows:
                if not isinstance(active_item, dict):
                    continue
                if active_item.get("deleted_at") is None:
                    continue
                active_item_id = str(active_item.get("active_item_id") or "").strip()
                record_id = str(active_item.get("record_id") or "").strip()
                if active_item_id:
                    deleted_active_item_ids.add(active_item_id)
                if record_id:
                    deleted_record_ids.add(record_id)
        except Exception as exc:
            warning = f"SQLite Qt 活动删除状态读取失败: {exc}"
            if not PortalRuntime.last_ongoing_error:
                PortalRuntime.last_ongoing_error = warning
            log_warning(warning)

        try:
            snapshot = PortalRuntime.state_store.get_ongoing_snapshot()
            if snapshot.get("exists"):
                PortalRuntime.last_ongoing_error = ""
                for item in snapshot.get("items", []):
                    _append(item)
        except Exception as exc:
            PortalRuntime.last_ongoing_error = f"SQLite 进行中状态读取失败: {exc}"
            log_warning(PortalRuntime.last_ongoing_error)
        try:
            for active_item in active_rows or PortalRuntime.state_store.list_qt_active_items():
                if active_item.get("deleted_at") is not None:
                    continue
                payload = (
                    active_item.get("payload")
                    if isinstance(active_item, dict)
                    and isinstance(active_item.get("payload"), dict)
                    else {}
                )
                if not payload:
                    continue
                text = str(payload.get("text") or "").strip()
                info = extract_event_info(text) or {}
                if str(info.get("status") or "").strip() == "结束":
                    continue
                item = dict(payload)
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
                    if notice_type == "维保通告":
                        item["work_type"] = "maintenance"
                    elif notice_type in {"设备变更", "变更通告"}:
                        item["work_type"] = "change"
                    elif notice_type == "设备检修":
                        item["work_type"] = "repair"
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
                        item["feishu_record_id"] = target_record_id
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
        except Exception as exc:
            warning = f"SQLite Qt 活动条目合并失败: {exc}"
            if not PortalRuntime.last_ongoing_error:
                PortalRuntime.last_ongoing_error = warning
            log_warning(warning)
        return merged

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
            threading.Thread(
                target=_worker,
                name=f"LANOrphanReconcile-{scope}",
                daemon=True,
            ).start()
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
        entry = {
            "content": info.get("content") or content,
            "status": str(info.get("status") or "").strip(),
            "title": str(info.get("title") or "").strip(),
            "notice_type": str(info.get("notice_type") or "").strip(),
            "level": info.get("level"),
            "source": str(info.get("source") or source or "").strip(),
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
        for item in PortalRuntime.state_store.list_qt_active_items():
            payload = item.get("payload") if isinstance(item, dict) else {}
            payload = payload if isinstance(payload, dict) else {}
            if str(payload.get("notice_type") or item.get("notice_type") or "").strip() != notice_type:
                continue
            if target_record_id and str(payload.get("record_id") or item.get("record_id") or "").strip() == target_record_id:
                return item
            info = extract_event_info(str(payload.get("text") or "")) or {}
            item_key = str(info.get("unique_key") or "").strip()
            item_title = str(info.get("title") or payload.get("title") or "").strip()
            item_reason = str(info.get("reason") or payload.get("reason") or "").strip()
            if unique_key and item_key and item_key == unique_key:
                return item
            if title and item_title and item_title == title and notice_type != "事件通告":
                if notice_type == "维保通告" and reason and item_reason and reason != item_reason:
                    continue
                if notice_type == "维保通告" and reason and not item_reason:
                    continue
                return item
        return None

    @classmethod
    def _project_clipboard_entry_to_active(cls, entry: dict) -> dict:
        status = str(entry.get("status") or "").strip()
        content = str(entry.get("content") or "").strip()
        notice_type = str(entry.get("notice_type") or "").strip()
        existing = cls._find_qt_active_item_for_clipboard_entry(entry)
        active_item_id = ""
        if existing and isinstance(existing.get("payload"), dict):
            data = dict(existing.get("payload") or {})
            active_item_id = str(data.get("active_item_id") or existing.get("active_item_id") or "").strip()
        else:
            data = {}
        if status not in {"", "开始", "新增"} and not data:
            return {"ok": True, "ignored": True, "reason": "未找到可更新的活动条目。"}
        if not active_item_id:
            active_item_id = str(entry.get("entry_id") or "").strip() or uuid.uuid4().hex
        record_id = str(data.get("record_id") or "").strip()
        if not record_id:
            record_id = f"local_{active_item_id[:24]}"
        if "_is_placeholder_record" in data:
            is_placeholder = bool(data.get("_is_placeholder_record"))
        else:
            is_placeholder = not bool(str(data.get("record_id") or "").strip())
        building_codes = data.get("building_codes")
        if not isinstance(building_codes, list):
            building_codes = PortalRuntime.service._building_codes_from_value(
                " ".join(
                    str(value or "")
                    for value in (
                        data.get("building"),
                        data.get("location"),
                        entry.get("title"),
                        content,
                    )
                )
            )
        work_type = data.get("work_type") or data.get("lan_work_type") or ""
        if not work_type:
            if notice_type == "维保通告":
                work_type = "maintenance"
            elif notice_type in {"设备变更", "变更通告"}:
                work_type = "change"
            elif notice_type == "设备检修":
                work_type = "repair"
        data.update(
            {
                "active_item_id": active_item_id,
                "record_id": record_id,
                "_is_placeholder_record": is_placeholder,
                "text": content,
                "title": entry.get("title") or data.get("title", ""),
                "notice_type": notice_type,
                "level": entry.get("level") or data.get("level"),
                "source": entry.get("source") or data.get("source", ""),
                "time_str": entry.get("time_str") or data.get("time_str", ""),
                "reason": entry.get("reason") or data.get("reason", ""),
                "_has_unuploaded_changes": True,
                "_pending_upload_hash": None,
                "_upload_in_progress": False,
                "origin": data.get("origin") or "clipboard",
                "building_codes": building_codes,
                "work_type": work_type,
                "lan_work_type": work_type,
            }
        )
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

    def _run_scheduled_job_cleanup(self) -> None:
        try:
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
            payload = {
                **cleanup,
                "runtime_queue_removed": queue_removed,
                "outbox_removed": outbox_removed,
                "append_events_removed": append_events_removed,
                "undo_removed": undo_removed,
                "cleaned_at": time.time(),
            }
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
        try:
            while self._sse_active(sse_key, sse_id) and not await request.is_disconnected():
                snapshot = PortalRuntime.state_store.get_ongoing_snapshot()
                scoped_signature, scoped_count = self._scoped_ongoing_signature(
                    scope, list(snapshot.get("items") or [])
                )
                qt_active_items = dict(PortalRuntime.state_store.qt_active_items_stats())
                qt_active_items.pop("checked_at", None)
                source_snapshot_stats = PortalRuntime.state_store.source_snapshot_stats()
                source_active = (
                    source_snapshot_stats.get("active")
                    if isinstance(source_snapshot_stats, dict)
                    else {}
                )
                source_active = source_active if isinstance(source_active, dict) else {}
                source_signature = ":".join(
                    [
                        str(source_active.get("snapshot_id") or ""),
                        str(source_active.get("updated_at") or ""),
                    ]
                )
                display_signature = hashlib.sha1(
                    f"{scoped_signature}|{source_signature}".encode("utf-8")
                ).hexdigest()
                payload = json.dumps(
                    {
                        "scope": scope,
                        "snapshot_id": snapshot.get("snapshot_id", ""),
                        "count": snapshot.get("count", 0),
                        "scope_count": scoped_count,
                        "scope_signature": scoped_signature,
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
                    await asyncio.sleep(active_interval)
                else:
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

    def mark_job_upload_result(self, job_id: str, **kwargs) -> None:
        PortalRuntime.service.mark_action_upload_result(job_id, **kwargs)

    def mark_job_progress(self, job_id: str, **patch) -> None:
        if not job_id:
            return
        PortalRuntime.service.mark_job(job_id, **patch)
        phase = str((patch or {}).get("phase") or "").strip()
        if phase == "uploading":
            PortalRuntime.track_upload_wait_job(job_id)
        elif phase in {"success", "failed"}:
            with PortalRuntime.upload_wait_lock:
                PortalRuntime.upload_wait_jobs.pop(job_id, None)

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

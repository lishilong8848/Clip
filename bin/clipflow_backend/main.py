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
from contextlib import suppress
from pathlib import Path
from typing import AsyncIterator
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from lan_bitable_template_portal.server import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    MAX_JSON_BODY_BYTES,
    PortalHandler,
    PortalServerController,
    find_available_port,
    portal_asset_file,
    portal_index_file,
    portal_static_roots,
)
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME
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
from upload_event_module.services.feishu_service import check_token_status
from upload_event_module.logger import log_error, log_info, log_warning
from upload_event_module.services.robot_webhook import send_text_to_open_ids

try:
    from apscheduler.schedulers.background import BackgroundScheduler
except Exception:
    BackgroundScheduler = None


HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-encoding",
    "content-length",
}


def _find_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _response_headers(headers: httpx.Headers) -> dict[str, str]:
    result: dict[str, str] = {}
    for name, value in headers.items():
        lower = name.lower()
        if lower in HOP_BY_HOP_HEADERS:
            continue
        result[name] = value
    return result


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


def _legacy_adapter_enabled() -> bool:
    return os.environ.get("CLIPFLOW_FASTAPI_LEGACY_ADAPTER") == "1"


def _queue_stats() -> dict:
    qt_outbox_counts: dict[str, int] = {}
    try:
        qt_outbox_counts = PortalHandler.state_store.count_outbox_events("qt_action")
    except Exception:
        qt_outbox_counts = {}
    with PortalHandler.message_queue_lock:
        message_queue_size = len(PortalHandler.message_queue)
        message_workers = sum(
            1 for worker in PortalHandler.message_worker_threads if worker.is_alive()
        )
    with PortalHandler.action_queue_lock:
        qt_queue_size = len(PortalHandler.action_queue)
        qt_worker_alive = bool(
            PortalHandler.action_worker_thread
            and PortalHandler.action_worker_thread.is_alive()
        )
    with PortalHandler.upload_wait_lock:
        upload_wait_size = len(PortalHandler.upload_wait_jobs)
        upload_wait_alive = bool(
            PortalHandler.upload_wait_thread
            and PortalHandler.upload_wait_thread.is_alive()
        )
    with PortalHandler.source_refresh_lock:
        source_refresh_inflight = bool(PortalHandler.source_refresh_inflight)
    with PortalHandler.repair_refresh_lock:
        repair_refresh_inflight = bool(PortalHandler.repair_refresh_inflight)
    return {
        "message_queue_size": message_queue_size,
        "message_worker_count": int(PortalHandler.message_worker_count or 0),
        "message_workers_alive": message_workers,
        "qt_queue_size": qt_queue_size,
        "qt_worker_alive": qt_worker_alive,
        "qt_outbox_pending": int(qt_outbox_counts.get("pending") or 0),
        "qt_outbox_leased": int(qt_outbox_counts.get("leased") or 0),
        "qt_outbox_failed": int(qt_outbox_counts.get("failed") or 0),
        "upload_wait_size": upload_wait_size,
        "upload_wait_alive": upload_wait_alive,
        "source_refresh_alive": bool(
            PortalHandler.source_refresh_thread
            and PortalHandler.source_refresh_thread.is_alive()
        ),
        "source_refresh_inflight": source_refresh_inflight,
        "repair_refresh_inflight": repair_refresh_inflight,
        "payload_cache_entries": len(PortalHandler.payload_cache),
        "runtime_limits": PortalHandler.runtime_limits(),
        "runtime_pressure": PortalHandler.runtime_pressure(),
        "sqlite_write_worker": PortalHandler.state_store.get_write_worker_stats(),
        "runtime_queue_counts": PortalHandler.state_store.runtime_queue_counts(),
        "runtime_queue_details": PortalHandler.state_store.runtime_queue_details(),
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
        "legacy_adapter_enabled": _legacy_adapter_enabled(),
    }
    return report


class FastAPIPortalController:
    """FastAPI/Uvicorn front controller.

    Native FastAPI routes are the default runtime path. The legacy
    BaseHTTPRequestHandler portal can still be enabled with
    CLIPFLOW_FASTAPI_LEGACY_ADAPTER=1 as a temporary fallback.
    """

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
        self._legacy_controller: PortalServerController | None = None
        self._legacy_url = ""
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

    def _build_app(self) -> FastAPI:
        app = FastAPI(title="ClipFlow LAN Backend")

        @app.get("/")
        async def root(request: Request):
            oauth_response = self._root_oauth_callback_response(request)
            if oauth_response is not None:
                return oauth_response
            return self._static_file_response(portal_index_file(), html=True)

        @app.get("/assets/{asset_path:path}")
        async def assets(asset_path: str):
            relative = Path(*str(asset_path or "").split("/"))
            return self._static_file_response(portal_asset_file(relative))

        @app.get("/api/auth/login")
        async def auth_login(request: Request):
            next_path = str(request.query_params.get("next") or "/")
            redirect_uri = f"{self._request_base_url(request)}/api/auth/feishu/callback"
            try:
                login_url = PortalHandler.auth_manager.start_login(
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
                session_id, next_path = PortalHandler.auth_manager.complete_login(
                    code=code,
                    state=state,
                    redirect_uri=redirect_uri,
                )
            except PortalError as exc:
                return self._html_message(400, "飞书登录失败", str(exc))
            return Response(
                status_code=302,
                headers={
                    "Location": next_path,
                    "Set-Cookie": PortalHandler.auth_manager.cookie_header(session_id),
                },
            )

        @app.get("/api/auth/logout")
        async def auth_logout_get(request: Request):
            PortalHandler.auth_manager.clear_session(
                str(request.cookies.get(AUTH_COOKIE_NAME) or "")
            )
            return Response(
                status_code=302,
                headers={
                    "Location": "/",
                    "Set-Cookie": PortalHandler.auth_manager.clear_cookie_header(),
                },
            )

        @app.post("/api/auth/logout")
        async def auth_logout_post(request: Request):
            PortalHandler.auth_manager.clear_session(
                str(request.cookies.get(AUTH_COOKIE_NAME) or "")
            )
            return JSONResponse(
                {"ok": True, "data": {}},
                headers={"Set-Cookie": PortalHandler.auth_manager.clear_cookie_header()},
            )

        @app.get("/api/health")
        async def health() -> dict:
            legacy_alive = bool(
                self._legacy_controller
                and self._legacy_controller.thread
                and self._legacy_controller.thread.is_alive()
            )
            return {
                "ok": True,
                "data": {
                    "service": "clipflow_backend",
                    "backend": "fastapi",
                    "legacy_adapter": legacy_alive,
                    "mock_external": _mock_external_enabled(),
                    "external_guard": _external_guard_status(),
                    "url": self.get_url(),
                    "internal_url": self._legacy_url,
                    "time": time.time(),
                    "runtime": PortalHandler.state_store.get_backend_runtime("backend") or {},
                },
            }

        @app.get("/api/backend/stats")
        async def stats(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            if not PortalHandler.auth_manager.is_admin(session):
                return JSONResponse(
                    {"ok": False, "error": "只有管理员可以查看后端状态。"},
                    status_code=403,
                )
            service = PortalHandler.service
            last_loaded_at = ""
            last_loaded_ts = 0.0
            warnings: list[str] = []
            recent_failed_jobs: list[dict] = []
            job_count = 0
            job_phase_counts: dict[str, int] = {}
            failed_retryable_count = 0
            failed_non_retryable_count = 0
            qt_bridge = PortalHandler.state_store.get_backend_runtime("qt_bridge") or {}
            qt_bridge_payload = dict(qt_bridge) if isinstance(qt_bridge, dict) else {}
            qt_bridge_heartbeat_at = float(qt_bridge_payload.get("heartbeat_at") or 0)
            qt_bridge_age_seconds = (
                max(0.0, time.time() - qt_bridge_heartbeat_at)
                if qt_bridge_heartbeat_at
                else 0.0
            )
            job_cleanup = PortalHandler.state_store.get_backend_runtime("job_cleanup") or {}
            preflight = PortalHandler.state_store.get_backend_runtime("preflight") or {}
            token_status = PortalHandler.state_store.get_backend_runtime("token_status") or {}
            sqlite_stats: dict = {}
            with suppress(Exception):
                sqlite_stats = PortalHandler.state_store.get_database_stats()
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
            return {
                "ok": True,
                "data": {
                    **_queue_stats(),
                    "mock_external": _mock_external_enabled(),
                    "external_guard": _external_guard_status(),
                    "legacy_adapter_enabled": _legacy_adapter_enabled(),
                    "legacy_fallback_env": os.environ.get("CLIPFLOW_LEGACY_PORTAL", ""),
                    "last_loaded_at": last_loaded_at,
                    "last_loaded_ts": last_loaded_ts,
                    "warnings": warnings,
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
                    "recent_failed_jobs": recent_failed_jobs,
                    "time": time.time(),
                },
            }

        @app.get("/api/backend/queues")
        async def backend_queues(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            service = PortalHandler.service
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
                    "qt_active_items": PortalHandler.state_store.qt_active_items_stats(),
                    "time": time.time(),
                },
            }

        @app.get("/api/backend/perf")
        async def backend_perf(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            qt_bridge = PortalHandler.state_store.get_backend_runtime("qt_bridge") or {}
            qt_bridge_payload = dict(qt_bridge) if isinstance(qt_bridge, dict) else {}
            return {
                "ok": True,
                "data": {
                    "queues": _queue_stats(),
                    "sqlite": PortalHandler.state_store.get_database_stats(),
                    "qt_active_items": PortalHandler.state_store.qt_active_items_stats(),
                    "qt_bridge": qt_bridge_payload,
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
                    _build_backend_preflight_report, PortalHandler.service
                )
                PortalHandler.state_store.put_backend_runtime("preflight", report)
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
                    PortalHandler.state_store.checkpoint_database, truncate=True
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
                result = await asyncio.to_thread(PortalHandler.service.cleanup_action_jobs)
                result["cleaned_at"] = time.time()
                PortalHandler.state_store.put_backend_runtime("job_cleanup", result)
                return {"ok": True, "data": result}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.post("/api/backend/mock-pressure")
        async def backend_mock_pressure(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            payload = await self._read_json_request(request)
            count = max(1, min(int(payload.get("count") or 10), 50))
            concurrency = max(1, min(int(payload.get("concurrency") or 5), 10))
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
            return {
                "ok": True,
                "data": PortalHandler.auth_manager.public_status(
                    session,
                    next_path=next_path,
                    redirect_uri=f"{self._request_base_url(request)}/api/auth/feishu/callback",
                ),
            }

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
            if not PortalHandler.auth_manager.is_admin(session):
                return JSONResponse(
                    {"ok": False, "error": "只有管理员可以查看最近任务。"},
                    status_code=403,
                )
            jobs = []
            service = PortalHandler.service
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
            job = PortalHandler.service.get_job(normalized_job_id)
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
            PortalHandler.service.delete_action_job(normalized_job_id)
            return {"ok": True, "data": {"job_id": normalized_job_id, "cleared": True}}

        @app.post("/api/jobs/{job_id}/retry")
        async def retry_job(job_id: str, request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            normalized_job_id = str(job_id or "").strip()
            try:
                job = PortalHandler.service.retry_action_job(normalized_job_id)
                PortalHandler.clear_payload_cache()
                PortalHandler.enqueue_initial_message_or_upload_job(normalized_job_id)
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
            job = PortalHandler.service.get_job(str(job_id or "").strip())
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
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                open_id = str((session.get("user") or {}).get("open_id") or "")
                data = self._cached_service_payload(
                    (
                        "bootstrap",
                        open_id,
                        scope,
                        PortalHandler._ongoing_items_marker(ongoing),
                    ),
                    lambda: PortalHandler.service.get_bootstrap(
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
                ongoing = self._get_ongoing("ALL")
                self._reconcile_orphan_started_items("ALL", ongoing)
                allowed_options = PortalHandler.auth_manager.filter_scope_options(
                    SCOPE_OPTIONS, session
                )
                allowed_scopes = [
                    str(option.get("value") or "")
                    for option in allowed_options
                    if str(option.get("value") or "").strip()
                ]
                open_id = str((session.get("user") or {}).get("open_id") or "")
                data = self._cached_service_payload(
                    (
                        "scope-overview",
                        open_id,
                        tuple(sorted(allowed_scopes)),
                        PortalHandler._ongoing_items_marker(ongoing),
                    ),
                    lambda: PortalHandler.service.get_scope_overview(
                        ongoing_items=ongoing,
                        scopes=allowed_scopes,
                        include_prepared=False,
                    ),
                )
                data = PortalHandler.auth_manager.filter_scope_overview(data, session)
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
                scope = self._authorized_scope_or_error(
                    session, request.query_params.get("scope") or "ALL"
                )
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                month = str(request.query_params.get("month") or "")
                specialty = str(request.query_params.get("specialty") or "")
                open_id = str((session.get("user") or {}).get("open_id") or "")
                payload_key = "records" if request.url.path == "/api/records" else "workbench"
                payload = self._cached_service_payload(
                    (
                        payload_key,
                        open_id,
                        scope,
                        month,
                        specialty,
                        PortalHandler._ongoing_items_marker(ongoing),
                    ),
                    lambda: PortalHandler.service.query_records(
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
                payload = PortalHandler.service.get_history_summary(
                    scope=scope,
                    month=str(request.query_params.get("month") or ""),
                    work_type=str(request.query_params.get("work_type") or "all"),
                )
                return self._json_ok(request, session, payload)
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
                refresh_result = PortalHandler.request_source_refresh(force=True)
                refreshed = bool(refresh_result.get("refreshed", False))
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing, force=True)
                data = PortalHandler.service.get_bootstrap(
                    scope=scope, ongoing_items=ongoing
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
                refresh_result = PortalHandler.request_repair_source_refresh()
                ongoing = self._get_ongoing(scope)
                self._reconcile_orphan_started_items(scope, ongoing)
                data = PortalHandler.service.get_bootstrap(
                    scope=scope, ongoing_items=ongoing
                )
                data.update(refresh_result)
                data["repair_source_refreshed"] = True
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.get("/api/handover-links")
        async def handover_links(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                data = PortalHandler.auth_manager.filter_handover_links(
                    PortalHandler.service.get_handover_links(), session
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=500)

        @app.get("/api/auth/permission-requests/current")
        async def current_permission_request(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            user = session.get("user") if isinstance(session.get("user"), dict) else {}
            permission_request = PortalHandler.auth_manager.get_current_permission_request(
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
                payload = await self._read_json_request(request)
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                payload["scope"] = scope
                payload["_auth_open_id"] = str(user.get("open_id") or "")
                payload["_auth_user_name"] = str(
                    user.get("name") or user.get("en_name") or ""
                )
                job_id, should_start = PortalHandler.service.create_action_job(payload)
                if should_start:
                    PortalHandler.clear_payload_cache()
                    PortalHandler.enqueue_initial_message_or_upload_job(job_id)
                job = PortalHandler.service.get_job(job_id) or {}
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
                payload = await self._read_json_request(request)
                scope = self._authorized_scope_or_error(
                    session, payload.get("scope") or "ALL"
                )
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                payload["scope"] = scope
                payload["_auth_open_id"] = str(user.get("open_id") or "")
                payload["_auth_user_name"] = str(
                    user.get("name") or user.get("en_name") or ""
                )
                PortalHandler.service.validate_ongoing_delete_item(payload, scope=scope)
                callback = PortalHandler.ongoing_delete_callback
                if callback is None:
                    event_id = PortalHandler.state_store.enqueue_outbox_event(
                        "qt_action",
                        {
                            "kind": "ongoing_delete",
                            "payload": payload,
                        },
                    )
                    data = PortalHandler.service.hide_ongoing_item(
                        payload,
                        scope=scope,
                        deleted_by=payload["_auth_open_id"],
                    )
                    PortalHandler.clear_payload_cache()
                    data["qt_delete_queued"] = True
                    data["qt_event_id"] = event_id
                    return JSONResponse(
                        {"ok": True, "data": PortalHandler._with_runtime_warnings(data)},
                        status_code=202,
                    )
                try:
                    accepted = callback(payload)
                except Exception as exc:
                    return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)
                if isinstance(accepted, dict):
                    qt_deleted = bool(accepted.get("ok"))
                    error = str(accepted.get("error") or "").strip()
                else:
                    qt_deleted = bool(accepted)
                    error = ""
                if not qt_deleted:
                    return JSONResponse(
                        {"ok": False, "error": error or "主窗口删除失败。"},
                        status_code=500,
                    )
                data = PortalHandler.service.hide_ongoing_item(
                    payload,
                    scope=scope,
                    deleted_by=payload["_auth_open_id"],
                )
                data.update(
                    PortalHandler.service.discard_deleted_ongoing_state(
                        payload, scope=scope
                    )
                )
                PortalHandler.clear_payload_cache()
                data["qt_deleted"] = True
                data["remote_deleted"] = bool(
                    accepted.get("remote_deleted")
                    if isinstance(accepted, dict)
                    else False
                )
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/handover-links")
        async def save_handover_links(request: Request):
            admin_response, session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = await self._read_json_request(request)
                data = PortalHandler.service.save_handover_links(
                    payload.get("links") or {},
                    password=str(payload.get("password") or ""),
                )
                data = PortalHandler.auth_manager.filter_handover_links(data, session)
                return self._json_ok(request, session, data)
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/handover-links-auth")
        async def handover_links_auth(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = await self._read_json_request(request)
                ok = PortalHandler.service.verify_handover_settings_password(
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
                data = PortalHandler.service.request_handover_password_reset()
                return {"ok": True, "data": data}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=400)

        @app.post("/api/handover-password-reset/confirm")
        async def handover_password_reset_confirm(request: Request):
            admin_response, _session = self._require_admin_response(request)
            if admin_response is not None:
                return admin_response
            try:
                payload = await self._read_json_request(request)
                data = PortalHandler.service.reset_handover_password_with_code(
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
                payload = await self._read_json_request(request)
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                data = PortalHandler.auth_manager.create_permission_request(
                    open_id=str(user.get("open_id") or ""),
                    name=str(user.get("name") or user.get("en_name") or "飞书用户"),
                    scopes=payload.get("scopes") or [],
                    reason=str(payload.get("reason") or ""),
                )
                code = str(data.pop("code") or "")
                recipients = PortalHandler.auth_manager.admin_open_ids()
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
                    PortalHandler.auth_manager.mark_permission_request_notify_failed(
                        str(data.get("request_id") or "")
                    )
                    raise PortalError(f"通知管理员失败: {message}")
                activated = PortalHandler.auth_manager.activate_permission_request(
                    str(data.get("request_id") or "")
                )
                data.update(activated)
                PortalHandler.auth_manager.supersede_other_permission_requests(
                    open_id=str(data.get("open_id") or ""),
                    keep_request_id=str(data.get("request_id") or ""),
                )
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
                payload = await self._read_json_request(request)
                user = session.get("user") if isinstance(session.get("user"), dict) else {}
                permissions = PortalHandler.auth_manager.confirm_permission_request(
                    open_id=str(user.get("open_id") or ""),
                    request_id=str(payload.get("request_id") or ""),
                    code=str(payload.get("code") or ""),
                    updated_by=str(user.get("open_id") or "permission_request"),
                )
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
                payload = await self._read_json_request(request)
                actor = session.get("user") if isinstance(session.get("user"), dict) else {}
                data, changed_open_ids = PortalHandler.auth_manager.save_permissions_payload(
                    payload.get("users") or [],
                    updated_by=str(actor.get("open_id") or ""),
                )
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
                payload = await self._read_json_request(request)
                scope = self._authorized_scope_or_error(
                    session,
                    payload.get("scope")
                    or PortalHandler.auth_manager.default_scope(session)
                    or "ALL",
                )
                drafts = payload.get("drafts") or []
                PortalHandler.service.assert_generated_drafts_allowed(
                    drafts, scope=scope
                )
                generated = PortalHandler.service.generate_templates(drafts)
                return {"ok": True, "data": {"items": generated}}
            except Exception as exc:
                return self._portal_error_response(exc, default_status=403)

        @app.post("/api/send-generated")
        async def send_generated(request: Request):
            session = self._current_session(request)
            if session is None:
                return self._auth_required_response()
            try:
                payload = await self._read_json_request(request)
                scope = self._authorized_scope_or_error(
                    session,
                    payload.get("scope")
                    or PortalHandler.auth_manager.default_scope(session)
                    or "ALL",
                )
                items = payload.get("items") or []
                PortalHandler.service.assert_generated_items_allowed(
                    items, scope=scope
                )
                notice_callback = PortalHandler.notice_callback or self._enqueue_notice_outbox
                results = PortalHandler.service.send_generated_templates(
                    items, notice_callback=notice_callback
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

        @app.get("/api/qt/events")
        async def qt_events(request: Request, limit: int = 1):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            items = PortalHandler.state_store.lease_outbox_events(
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
            payload = await request.json()
            ok = bool((payload or {}).get("ok", True))
            error = str((payload or {}).get("error") or "")
            event = PortalHandler.state_store.mark_outbox_event(
                event_id,
                "done" if ok else "pending",
                error=error,
                max_attempts=3,
            )
            event_status = str((event or {}).get("status") or ("done" if ok else "pending"))
            event_payload = (event or {}).get("payload") if isinstance(event, dict) else {}
            event_payload = event_payload if isinstance(event_payload, dict) else {}
            job_id = str(event_payload.get("job_id") or "").strip()
            if job_id:
                if event_status == "failed":
                    PortalHandler.service.mark_job(
                        job_id,
                        phase="failed",
                        error=error or "Qt 事件连续执行失败。",
                        qt_event_attempts=int((event or {}).get("attempts") or 0),
                    )
                elif not ok:
                    PortalHandler.service.mark_job(
                        job_id,
                        phase="qt_queued",
                        qt_phase="outbox_retry",
                        qt_event_attempts=int((event or {}).get("attempts") or 0),
                        qt_event_error=error,
                    )
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
            payload = await request.json()
            items = payload.get("items") if isinstance(payload, dict) else []
            result = PortalHandler.state_store.replace_ongoing_items(
                items if isinstance(items, list) else []
            )
            PortalHandler.clear_payload_cache()
            return {"ok": True, "data": result}

        @app.post("/api/qt/active-items/delta")
        async def qt_active_items_delta(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = await request.json()
            payload = payload if isinstance(payload, dict) else {}
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
                if PortalHandler.state_store.upsert_qt_active_item(
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
                if PortalHandler.state_store.delete_qt_active_item(
                    active_item_id=active_item_id,
                    record_id=record_id,
                ):
                    deleted += 1
            if upserted or deleted:
                PortalHandler.clear_payload_cache()
            return {
                "ok": True,
                "data": {
                    "upserted": upserted,
                    "deleted": deleted,
                    "qt_active_items": PortalHandler.state_store.qt_active_items_stats(),
                },
            }

        @app.post("/api/qt/bridge-heartbeat")
        async def qt_bridge_heartbeat(request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = await request.json()
            payload = payload if isinstance(payload, dict) else {}
            payload["heartbeat_at"] = time.time()
            PortalHandler.state_store.put_backend_runtime("qt_bridge", payload)
            return {"ok": True, "data": {"heartbeat_at": payload["heartbeat_at"]}}

        @app.get("/api/qt/jobs/{job_id}")
        async def qt_get_job(job_id: str, request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            job = PortalHandler.service.get_job(job_id)
            if not job:
                return JSONResponse({"ok": False, "error": "任务不存在"}, status_code=404)
            return {"ok": True, "data": job}

        @app.post("/api/qt/jobs/{job_id}/progress")
        async def qt_job_progress(job_id: str, request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            patch = await request.json()
            if not isinstance(patch, dict):
                patch = {}
            self.mark_job_progress(job_id, **patch)
            return {"ok": True, "data": {"job_id": job_id}}

        @app.post("/api/qt/jobs/{job_id}/result")
        async def qt_job_result(job_id: str, request: Request):
            deny = self._local_only_response(request)
            if deny is not None:
                return deny
            payload = await request.json()
            if not isinstance(payload, dict):
                payload = {}
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
        PortalHandler.service = MaintenancePortalService(
            app_token=self.app_token,
            table_id=self.table_id,
        )
        try:
            PortalHandler.service.ensure_snapshot_loaded()
        except Exception:
            pass
        PortalHandler.auth_manager = PortalAuthManager()
        PortalHandler.state_store = LanPortalStateStore()
        PortalHandler.apply_runtime_settings()
        self._state_store = PortalHandler.state_store
        try:
            PortalHandler.state_store.reset_runtime_queue_incomplete()
        except Exception as exc:
            log_warning(f"运行队列状态重置失败: {exc}")
        PortalHandler.notice_callback = self.notice_callback
        PortalHandler.ongoing_callback = self.ongoing_callback
        PortalHandler.ongoing_delete_callback = self.ongoing_delete_callback
        PortalHandler.maintenance_action_callback = self.maintenance_action_callback
        with PortalHandler.source_refresh_lock:
            PortalHandler.source_refresh_inflight = False
            PortalHandler.source_refresh_last_result = {}
            PortalHandler.source_refresh_last_finished = 0.0
        with PortalHandler.repair_refresh_lock:
            PortalHandler.repair_refresh_inflight = False
            PortalHandler.repair_refresh_event = threading.Event()
            PortalHandler.repair_refresh_last_result = {}
            PortalHandler.repair_refresh_last_error = ""
            PortalHandler.repair_refresh_last_finished = 0.0
        with PortalHandler.action_queue_lock:
            PortalHandler.action_queue.clear()
            PortalHandler.action_worker_stop = False
            PortalHandler.action_queue_event.clear()
        with PortalHandler.upload_wait_lock:
            PortalHandler.upload_wait_jobs.clear()
            PortalHandler.upload_wait_stop = False
            PortalHandler.upload_wait_event.clear()
        with PortalHandler.message_queue_lock:
            PortalHandler.message_queue.clear()
            PortalHandler.message_worker_stop = False
            PortalHandler.message_queue_event.clear()

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
        return PortalHandler.auth_manager.get_session(
            str(request.cookies.get(AUTH_COOKIE_NAME) or "")
        )

    def _static_file_response(self, path: Path, *, html: bool = False) -> Response:
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
        content_type = "text/html; charset=utf-8" if html else (
            mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
        )
        headers = {"Content-Type": content_type}
        if not html:
            headers["Cache-Control"] = "public, max-age=86400"
        return Response(content=resolved.read_bytes(), status_code=200, headers=headers)

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
            session_id, next_path = PortalHandler.auth_manager.complete_login(
                code=code,
                state=state,
                redirect_uri=redirect_uri,
            )
        except PortalError as exc:
            return self._html_message(400, "飞书登录失败", str(exc))
        return Response(
            status_code=302,
            headers={
                "Location": next_path or sanitized,
                "Set-Cookie": PortalHandler.auth_manager.cookie_header(session_id),
            },
        )

    def _require_admin_response(self, request: Request) -> tuple[JSONResponse | None, dict]:
        session = self._current_session(request)
        if session is None:
            return self._auth_required_response(), {}
        if not PortalHandler.auth_manager.is_admin(session):
            return (
                JSONResponse(
                    {"ok": False, "error": "只有管理员可以执行该操作。"},
                    status_code=403,
                ),
                session,
            )
        return None, session

    def _request_base_url(self, request: Request) -> str:
        host = PortalHandler._safe_host_value(
            str(request.headers.get("host") or ""),
            self.get_url().replace("http://", "", 1),
        )
        proto = PortalHandler._safe_proto_value(
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
        result["auth"] = PortalHandler.auth_manager.public_status(
            session,
            next_path=self._request_target(request),
            redirect_uri=f"{self._request_base_url(request)}/api/auth/feishu/callback",
        )
        scope_options = result.get("scope_options")
        if isinstance(scope_options, list):
            result["scope_options"] = PortalHandler.auth_manager.filter_scope_options(
                scope_options, session
            )
        return result

    def _json_ok(self, request: Request, session: dict, payload: dict):
        data = PortalHandler._with_runtime_warnings(payload if isinstance(payload, dict) else {})
        return {
            "ok": True,
            "data": self._with_auth_context(data, session, request),
        }

    @staticmethod
    async def _read_json_request(request: Request) -> dict:
        raw_length = request.headers.get("content-length", "0") or "0"
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValueError("请求体长度无效。") from exc
        if length < 0:
            raise ValueError("请求体长度无效。")
        if length > MAX_JSON_BODY_BYTES:
            raise ValueError("请求体过大，请减少粘贴内容后重试。")
        payload = await request.json()
        if not isinstance(payload, dict):
            raise ValueError("请求体必须是 JSON 对象。")
        return payload

    @staticmethod
    def _portal_error_response(exc: Exception, *, default_status: int) -> JSONResponse:
        status = default_status if isinstance(exc, PortalError) else 500
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=status)

    @staticmethod
    def _authorized_scope_or_error(session: dict, scope: str) -> str:
        normalized = PortalHandler.auth_manager.normalize_scope(scope)
        if not PortalHandler.auth_manager.scope_allowed(session, normalized):
            raise PortalError(
                f"当前飞书账号无权访问 {PortalHandler.auth_manager.scope_label(normalized)}。"
            )
        return normalized

    @staticmethod
    def _cached_service_payload(key_parts: tuple, builder) -> dict:
        class _CacheAdapter:
            @property
            def service(self):
                return PortalHandler.service

        return PortalHandler._cached_service_payload(_CacheAdapter(), key_parts, builder)

    @staticmethod
    def _get_ongoing(scope: str) -> list[dict]:
        try:
            snapshot = PortalHandler.state_store.get_ongoing_snapshot()
            if snapshot.get("exists"):
                PortalHandler.last_ongoing_error = ""
                return [
                    dict(item)
                    for item in snapshot.get("items", [])
                    if isinstance(item, dict)
                    and PortalHandler.service._scope_matches_item(scope, item)
                ]
        except Exception as exc:
            PortalHandler.last_ongoing_error = f"SQLite 进行中状态读取失败: {exc}"
            log_warning(PortalHandler.last_ongoing_error)
        return []

    @staticmethod
    def _reconcile_orphan_started_items(
        scope: str, ongoing: list[dict] | None, *, force: bool = False
    ) -> None:
        scope = PortalHandler.auth_manager.normalize_scope(scope)
        now = time.monotonic()
        with PortalHandler.orphan_reconcile_lock:
            if scope in PortalHandler.orphan_reconcile_pending:
                return
            last = float(PortalHandler.orphan_reconcile_last.get(scope) or 0)
            if not force and now - last < PortalHandler.orphan_reconcile_interval_s:
                return
            PortalHandler.orphan_reconcile_pending.add(scope)
            PortalHandler.orphan_reconcile_last[scope] = now
        ongoing_copy = [
            dict(item) for item in (ongoing or []) if isinstance(item, dict)
        ]

        def _worker() -> None:
            try:
                result = PortalHandler.service.reconcile_orphan_started_items(
                    scope=scope, ongoing_items=ongoing_copy
                )
                if int((result or {}).get("removed") or 0) > 0:
                    PortalHandler.clear_payload_cache()
            except Exception as exc:
                warning = f"本地已开始状态清理失败: {exc}"
                if warning not in PortalHandler.service._load_warnings:
                    PortalHandler.service._load_warnings.append(warning)
            finally:
                with PortalHandler.orphan_reconcile_lock:
                    PortalHandler.orphan_reconcile_pending.discard(scope)

        try:
            threading.Thread(
                target=_worker,
                name=f"LANOrphanReconcile-{scope}",
                daemon=True,
            ).start()
        except Exception as exc:
            with PortalHandler.orphan_reconcile_lock:
                PortalHandler.orphan_reconcile_pending.discard(scope)
            warning = f"本地已开始状态清理启动失败: {exc}"
            if warning not in PortalHandler.service._load_warnings:
                PortalHandler.service._load_warnings.append(warning)

    @staticmethod
    def _job_visible_to_session(job: dict, session: dict) -> bool:
        if PortalHandler.auth_manager.is_admin(session):
            return True
        open_id = str((session.get("user") or {}).get("open_id") or "")
        request_payload = job.get("request") if isinstance(job.get("request"), dict) else {}
        return bool(open_id and str(request_payload.get("_auth_open_id") or "") == open_id)

    def _write_runtime_heartbeat(self) -> None:
        try:
            if not PortalHandler.state_store.put_backend_runtime_async(
                "backend",
                {
                    "backend": "fastapi",
                    "url": self.get_url(),
                    "internal_url": self._legacy_url,
                    "stats": _queue_stats(),
                    "heartbeat_at": time.time(),
                },
            ):
                PortalHandler.state_store.put_backend_runtime(
                    "backend",
                    {
                        "backend": "fastapi",
                        "url": self.get_url(),
                        "internal_url": self._legacy_url,
                        "stats": _queue_stats(),
                        "heartbeat_at": time.time(),
                    },
                )
        except Exception as exc:
            log_warning(f"后端心跳写入失败: {exc}")

    @staticmethod
    def _enqueue_notice_outbox(payload: dict) -> dict:
        event_id = PortalHandler.state_store.enqueue_outbox_event(
            "qt_action",
            {
                "kind": "notice",
                "payload": payload if isinstance(payload, dict) else {},
            },
        )
        return {"ok": True, "event_id": event_id}

    def _run_scheduled_source_refresh(self) -> None:
        if _mock_external_enabled():
            return
        result = PortalHandler.refresh_sources_once(
            force=False,
            min_interval_seconds=30 * 60,
            defer_if_busy=True,
        )
        if result.get("source_refresh_deferred"):
            if not PortalHandler.state_store.put_backend_runtime_async(
                "source_refresh_deferred",
                {
                    "deferred_at": time.time(),
                    "reason": result.get("source_refresh_defer_reason") or "",
                    "runtime_pressure": result.get("runtime_pressure") or {},
                },
            ):
                PortalHandler.state_store.put_backend_runtime(
                    "source_refresh_deferred",
                    {
                        "deferred_at": time.time(),
                        "reason": result.get("source_refresh_defer_reason") or "",
                        "runtime_pressure": result.get("runtime_pressure") or {},
                    },
                )
            return
        for warning in result.get("warnings") or []:
            log_warning(str(warning))

    def _run_scheduled_job_cleanup(self) -> None:
        try:
            cleanup = PortalHandler.service.cleanup_action_jobs()
            queue_removed = PortalHandler.state_store.cleanup_runtime_queue_items(
                retention_seconds=3 * 24 * 3600,
                max_delete=500,
            )
            payload = {
                **cleanup,
                "runtime_queue_removed": queue_removed,
                "cleaned_at": time.time(),
            }
            if not PortalHandler.state_store.put_backend_runtime_async(
                "job_cleanup",
                payload,
            ):
                PortalHandler.state_store.put_backend_runtime("job_cleanup", payload)
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
        if not PortalHandler.state_store.put_backend_runtime_async(
            "token_status",
            payload,
        ):
            PortalHandler.state_store.put_backend_runtime("token_status", payload)

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
            self._run_scheduled_source_refresh,
            "interval",
            minutes=30,
            id="source_refresh",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
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
            scheduler.add_job(
                self._run_scheduled_source_refresh,
                "date",
                run_date=dt.datetime.now() + dt.timedelta(seconds=60),
                id="source_refresh_startup",
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
        if not job_id and not PortalHandler.auth_manager.is_admin(session):
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
        while not await request.is_disconnected():
            payload: dict
            if job_id:
                job = PortalHandler.service.get_job(job_id) or {}
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
            else:
                payload = {"stats": _queue_stats(), "time": time.time()}
            raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            if raw != last_payload:
                event_id = hashlib.sha1(raw.encode("utf-8")).hexdigest()
                yield f"id: {event_id}\nevent: job\ndata: {raw}\n\n".encode("utf-8")
                last_payload = raw
            await asyncio.sleep(interval_s)

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
        while not await request.is_disconnected():
            items = await asyncio.to_thread(
                PortalHandler.state_store.lease_outbox_events,
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
            if now - last_heartbeat >= 3.0:
                last_heartbeat = now
                payload = json.dumps(
                    {"time": time.time(), "stats": _queue_stats()},
                    ensure_ascii=False,
                    sort_keys=True,
                )
                yield f"event: heartbeat\ndata: {payload}\n\n".encode("utf-8")
            await asyncio.sleep(0.5)

    async def _qt_active_items_stream(self, request: Request) -> AsyncIterator[bytes]:
        session = self._current_session(request)
        if session is None:
            payload = json.dumps(
                {"ok": False, "error": "请先使用飞书扫码登录。", "auth_required": True},
                ensure_ascii=False,
            )
            yield f"event: error\ndata: {payload}\n\n".encode("utf-8")
            return
        last_payload = ""
        while not await request.is_disconnected():
            snapshot = PortalHandler.state_store.get_ongoing_snapshot()
            qt_active_items = PortalHandler.state_store.qt_active_items_stats()
            payload = json.dumps(
                {
                    "snapshot_id": snapshot.get("snapshot_id", ""),
                    "count": snapshot.get("count", 0),
                    "updated_at": snapshot.get("updated_at", 0),
                    "qt_active_items": qt_active_items,
                    "time": time.time(),
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
            await asyncio.sleep(3.0)

    async def _proxy_request(self, path: str, request: Request) -> Response:
        if not self._legacy_url:
            return Response(
                json.dumps({"ok": False, "error": "Not Found"}, ensure_ascii=False),
                status_code=404,
                media_type="application/json",
            )
        url = f"{self._legacy_url}/{path}"
        if request.url.query:
            url = f"{url}?{request.url.query}"
        headers = {
            key: value
            for key, value in request.headers.items()
            if key.lower() not in HOP_BY_HOP_HEADERS and key.lower() != "host"
        }
        if request.headers.get("host"):
            headers["host"] = str(request.headers.get("host") or "")
        headers["x-forwarded-proto"] = request.url.scheme or "http"
        body = await request.body()
        timeout = httpx.Timeout(connect=3.0, read=120.0, write=30.0, pool=3.0)
        try:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
                proxied = await client.request(
                    request.method,
                    url,
                    content=body,
                    headers=headers,
                )
        except Exception as exc:
            log_error(f"FastAPI门户代理失败: {exc}")
            return Response(
                json.dumps(
                    {"ok": False, "error": f"内部门户代理失败: {exc}"},
                    ensure_ascii=False,
                ),
                status_code=502,
                media_type="application/json",
            )
        return Response(
            content=proxied.content,
            status_code=proxied.status_code,
            headers=_response_headers(proxied.headers),
            media_type=proxied.headers.get("content-type"),
        )

    def start(self) -> str:
        if self._server and self._thread and self._thread.is_alive():
            return self.get_url()
        try:
            import uvicorn
        except Exception as exc:
            raise RuntimeError(f"Uvicorn 不可用: {exc}") from exc

        if _legacy_adapter_enabled():
            internal_port = _find_loopback_port()
            legacy = PortalServerController(
                host="127.0.0.1",
                port=internal_port,
                app_token=self.app_token,
                table_id=self.table_id,
                start_source_refresh_worker=False,
            )
            legacy.notice_callback = self.notice_callback
            legacy.ongoing_callback = self.ongoing_callback
            legacy.ongoing_delete_callback = self.ongoing_delete_callback
            legacy.maintenance_action_callback = self.maintenance_action_callback
            self._legacy_url = legacy.start()
            self._legacy_controller = legacy
            self._state_store = PortalHandler.state_store
        else:
            self._initialize_portal_handler_state()
            self._legacy_url = ""
            self._legacy_controller = None
            PortalHandler.ensure_message_workers()
            PortalHandler.ensure_action_worker()
            for job_id in PortalHandler.service.recoverable_action_job_ids():
                PortalHandler.enqueue_initial_message_or_upload_job(job_id)

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
        log_info(f"FastAPI门户已启动: public={self.get_url()} internal={self._legacy_url}")
        return self.get_url()

    def stop(self) -> None:
        self._stop_scheduler()
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
        legacy = self._legacy_controller
        self._legacy_controller = None
        if legacy:
            legacy.stop()
        else:
            PortalHandler.stop_message_workers()
            PortalHandler.stop_action_worker()
            PortalHandler.stop_upload_wait_worker()
            PortalHandler.notice_callback = None
            PortalHandler.ongoing_callback = None
            PortalHandler.ongoing_delete_callback = None
            PortalHandler.maintenance_action_callback = None
        try:
            PortalHandler.state_store.shutdown_write_worker(timeout=2.0)
        except Exception as exc:
            log_warning(f"SQLite写入队列停止失败: {exc}")
        self.bound_port = None
        self._legacy_url = ""
        self._shutdown_event.set()

    def wait(self) -> None:
        self._shutdown_event.wait()

    def get_url(self) -> str:
        port = self.bound_port or self.preferred_port
        display_host = self.host if self.host != "0.0.0.0" else "127.0.0.1"
        return f"http://{display_host}:{port}"

    def set_notice_callback(self, callback) -> None:
        self.notice_callback = callback
        PortalHandler.notice_callback = callback
        if self._legacy_controller:
            self._legacy_controller.set_notice_callback(callback)

    def set_ongoing_callback(self, callback) -> None:
        self.ongoing_callback = callback
        PortalHandler.ongoing_callback = callback
        if self._legacy_controller:
            self._legacy_controller.set_ongoing_callback(callback)

    def set_ongoing_delete_callback(self, callback) -> None:
        self.ongoing_delete_callback = callback
        PortalHandler.ongoing_delete_callback = callback
        if self._legacy_controller:
            self._legacy_controller.set_ongoing_delete_callback(callback)

    def set_maintenance_action_callback(self, callback) -> None:
        self.maintenance_action_callback = callback
        PortalHandler.maintenance_action_callback = callback
        if self._legacy_controller:
            self._legacy_controller.set_maintenance_action_callback(callback)

    def mark_job_upload_result(self, job_id: str, **kwargs) -> None:
        if self._legacy_controller:
            self._legacy_controller.mark_job_upload_result(job_id, **kwargs)
            return
        PortalHandler.service.mark_action_upload_result(job_id, **kwargs)

    def mark_job_progress(self, job_id: str, **patch) -> None:
        if self._legacy_controller and hasattr(self._legacy_controller, "mark_job_progress"):
            self._legacy_controller.mark_job_progress(job_id, **patch)
            return
        if not job_id:
            return
        PortalHandler.service.mark_job(job_id, **patch)
        phase = str((patch or {}).get("phase") or "").strip()
        if phase == "uploading":
            PortalHandler.track_upload_wait_job(job_id)
        elif phase in {"success", "failed"}:
            with PortalHandler.upload_wait_lock:
                PortalHandler.upload_wait_jobs.pop(job_id, None)

    def get_job(self, job_id: str) -> dict | None:
        if self._legacy_controller:
            return self._legacy_controller.get_job(job_id)
        return PortalHandler.service.get_job(job_id)


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

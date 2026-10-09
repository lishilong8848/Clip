"""Assistant-native notice panel routes (no Feishu transport).

The panel is installed lazily and returned from a ``get_manager`` closure so the
module never spawns workers or listeners at import time.  Every request handler
authenticates with ``controller._current_session``, forbids guest/temporary/no real
``user.open_id`` sessions, validates same-origin on POST, and authorizes each scope.
"""
from __future__ import annotations

import asyncio
import json
import threading
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse

from .notice_panel import NoticePanel, PanelVersionConflict, SCOPES, edition_metadata
from .portal_service import PortalError

MAX_BODY_BYTES = 256 * 1024


def install_notice_panel_routes(app, controller, runtime):
    manager = None
    lock = threading.Lock()

    def get_manager():
        nonlocal manager
        with lock:
            if manager is None:
                manager = NoticePanel(controller, runtime)
                controller._notice_panel = manager
        return manager

    manager_factory = get_manager
    controller._get_notice_panel = manager_factory

    def fail(exc, default_status=400):
        return controller._portal_error_response(exc, default_status=default_status)

    def current_session(request):
        session = controller._current_session(request)
        if not session:
            raise PortalError("请先登录。")
        return session

    def same_origin(request):
        origin = urlsplit(request.headers.get("origin") or request.headers.get("referer") or "")
        if origin.scheme != request.url.scheme or origin.netloc != request.url.netloc:
            raise PortalError("请求来源无效。")

    async def read_body(request):
        data = bytearray()
        async for chunk in request.stream():
            data.extend(chunk)
            if len(data) > MAX_BODY_BYTES:
                raise PortalError("请求内容过大。")
        try:
            value = json.loads(data)
        except Exception:
            raise PortalError("请求格式无效。")
        if not isinstance(value, dict):
            raise PortalError("请求格式无效。")
        return value

    @app.get("/api/assistant/notice-panel")
    async def notice_panel_meta(request: Request):
        try:
            session = current_session(request)
            mang = await asyncio.to_thread(get_manager)
            await asyncio.to_thread(mang._require_user, session)
            auth = runtime.auth_manager
            allowed = [scope for scope in SCOPES if auth.scope_allowed(session, scope)]
            scopes = [{"value": scope, "label": auth.scope_label(scope)} for scope in allowed]
            edition, next_at = edition_metadata()
            return JSONResponse({"ok": True, "data": {"edition": edition, "scopes": scopes, "next_at": next_at}}, headers={"Cache-Control": "private, no-store"})
        except Exception as exc:
            return fail(exc)

    @app.post("/api/assistant/notice-panel/open")
    async def notice_panel_open(request: Request):
        try:
            session = current_session(request)
            same_origin(request)
            data = await read_body(request)
            unknown = set(data) - {"scope", "slot"}
            if unknown:
                raise PortalError("请求含未知字段。")
            scope = data.get("scope")
            if not isinstance(scope, str) or not str(scope).strip():
                raise PortalError("请选择楼栋。")
            slot = data.get("slot")
            if slot is not None and not isinstance(slot, str):
                raise PortalError("时段时间无效。")
            mang = await asyncio.to_thread(get_manager)
            run = await asyncio.to_thread(mang.open_run, session, str(scope).strip(), slot)
            return JSONResponse({"ok": True, "data": {"run": run}}, status_code=202)
        except Exception as exc:
            return fail(exc)

    @app.get("/api/assistant/notice-panel/{identity}")
    async def notice_panel_status(identity: str, request: Request):
        try:
            session = current_session(request)
            mang = await asyncio.to_thread(get_manager)
            run = await asyncio.to_thread(mang.get_run, session, identity)
            return JSONResponse({"ok": True, "data": {"run": run}}, headers={"Cache-Control": "private, no-store"})
        except Exception as exc:
            return fail(exc)

    @app.get("/api/assistant/notice-panel/{identity}/sop-options")
    async def notice_panel_sop_options(identity: str, request: Request):
        try:
            session = current_session(request)
            item_key = request.query_params.get("item_key", "")
            q = request.query_params.get("q", "")
            mang = await asyncio.to_thread(get_manager)
            data = await asyncio.to_thread(mang.sop_options, session, identity, item_key, q)
            return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "private, no-store"})
        except Exception as exc:
            return fail(exc)

    @app.post("/api/assistant/notice-panel/{identity}/action")
    async def notice_panel_action(identity: str, request: Request):
        try:
            session = current_session(request)
            same_origin(request)
            data = await read_body(request)
            unknown = set(data) - {"action", "revision", "changes"}
            if unknown:
                raise PortalError("请求含未知字段。")
            action = data.get("action")
            valid_actions = ("save", "preview", "confirm", "edit", "refresh", "retry")
            if action not in valid_actions:
                raise PortalError("不支持的操作。")
            if "revision" not in data:
                raise PortalError("缺少版本号。")
            if "changes" in data and not isinstance(data.get("changes"), list):
                raise PortalError("变更字段格式无效。")
            mang = await asyncio.to_thread(get_manager)
            try:
                run = await asyncio.to_thread(
                    mang.run_action, session, identity,
                    {"action": action, "revision": data.get("revision"),
                     "changes": data.get("changes", [])})
            except PanelVersionConflict as conflict:
                current_run = await asyncio.to_thread(mang.public_run, identity)
                return JSONResponse(
                    {"ok": False, "error": str(conflict), "data": {"run": current_run}},
                    status_code=409)
            return {"ok": True, "data": {"run": run}}
        except Exception as exc:
            return fail(exc)

    return manager_factory

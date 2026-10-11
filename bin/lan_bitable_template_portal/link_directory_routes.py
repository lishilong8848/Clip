"""Authenticated directory CRUD, confined to the designated directory table."""
import asyncio
import json
import logging
import threading
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse

from .link_directory import LinkDirectory, LinkRemote
from .portal_service import PortalError


def install_link_directory_routes(app, controller, runtime):
    service = None
    lock = threading.Lock()

    def manager():
        nonlocal service
        with lock:
            if service is None:
                service = LinkDirectory(runtime.state_store, LinkRemote(runtime.service))
        return service

    async def endpoint(request: Request):
        try:
            session = await asyncio.to_thread(controller._current_session, request)
            if not session:
                return controller._auth_required_response()
            user = session.get("user") or {}
            role = session.get("role") or user.get("role")
            if role in {"guest", "temporary"} or not user.get("open_id"):
                return JSONResponse({"ok": False, "error": "请使用正式账号登录后查看多维表导航。"}, status_code=403)
            admin = bool(runtime.auth_manager.is_admin(session))
            writing = request.method != "GET"
            refreshing = request.url.path.endswith("/refresh")
            if writing and not refreshing and not admin:
                return JSONResponse({"ok": False, "error": "仅管理员可维护多维表导航。"}, status_code=403)
            payload = {}
            if writing:
                source = urlsplit(request.headers.get("origin") or request.headers.get("referer") or "")
                expected = urlsplit(controller._request_base_url(request))
                if (source.scheme, source.netloc) != (expected.scheme, expected.netloc) or request.headers.get("sec-fetch-site") == "cross-site":
                    return JSONResponse({"ok": False, "error": "不允许跨来源提交。"}, status_code=403)
                content = bytearray()
                async for part in request.stream():
                    content.extend(part)
                    if len(content) > 16 * 1024:
                        raise PortalError("填写内容过长。")
                payload = json.loads(content or b"{}")
                if not isinstance(payload, dict):
                    raise PortalError("提交格式无效。")
            current = await asyncio.to_thread(manager)
            if request.method == "POST" and request.url.path.endswith("/reorder"):
                if not isinstance(payload, dict) or set(payload) != {"record_id", "target_id", "placement"}:
                    raise PortalError("排序请求字段无效。")
                data = await asyncio.to_thread(current.reorder,
                                               payload["record_id"], payload["target_id"], payload["placement"])
                data["can_edit"] = admin
            elif request.method == "GET" or refreshing:
                data = await asyncio.to_thread(current.read, refreshing)
                data["can_edit"] = admin
            elif request.method == "DELETE":
                data = await asyncio.to_thread(current.delete, request.path_params["record_id"])
            else:
                data = await asyncio.to_thread(current.save, payload, request.path_params.get("record_id"))
            return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "private, no-store"})
        except (PortalError, ValueError) as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=getattr(exc, "status_code", 400))
        except Exception:
            logging.exception("Link directory operation failed")
            return JSONResponse({"ok": False, "error": "多维表导航同步未完成，填写已保留，请刷新核对后重试。"}, status_code=502)

    app.add_api_route("/api/link-directory", endpoint, methods=["GET", "POST"])
    app.add_api_route("/api/link-directory/refresh", endpoint, methods=["POST"])
    # Register the reorder route before the dynamic {record_id} route so the path
    # cannot be captured as a record id.
    app.add_api_route("/api/link-directory/reorder", endpoint, methods=["POST"])
    app.add_api_route("/api/link-directory/{record_id}", endpoint, methods=["PUT", "DELETE"])

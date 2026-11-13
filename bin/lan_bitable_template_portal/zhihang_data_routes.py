"""Administrator endpoints for the Zhihang refresh queue."""
import asyncio
from urllib.parse import urlencode, urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

from . import plan_convergence_auth as auth
from .module_access import allowed_modules
from .zhihang_data_cards import notify
from .zhihang_data_sync import SyncError, ZhihangSync, describe_error


def install_zhihang_data(app, controller, runtime, browser_login):
    from .portal_auth import topic_execution
    from .account_impersonation import COOKIE

    auth.bind_store(runtime.state_store)
    service = ZhihangSync(runtime.state_store, notify=lambda *args: notify(*args, store=runtime.state_store))
    app.add_event_handler("shutdown", service.close)

    def can_access(request, session):
        return bool(session and not session.get("is_guest") and not session.get("impersonation")
                    and not getattr(request.state, "admin_impersonation_session", None)
                    and not request.cookies.get(COOKIE)
                    and topic_execution(runtime.auth_manager, session) is None
                    and runtime.auth_manager.is_admin(session)
                    and "accounts" in allowed_modules(runtime.auth_manager, session))

    async def endpoint(request: Request):
        session = await asyncio.to_thread(controller._current_session, request)
        if not session:
            return controller._auth_required_response()
        if not await asyncio.to_thread(can_access, request, session):
            return JSONResponse({"ok": False, "error": "仅管理员可使用智航数据拉取。"}, status_code=403)
        try:
            body = {}
            method = request.method
            path = request.scope["route"].path
            if method != "GET":
                origin = request.headers.get("origin") or request.headers.get("referer")
                expected = urlsplit(controller._request_base_url(request))
                actual = urlsplit(origin) if origin else expected
                if request.headers.get("sec-fetch-site") == "cross-site" or (actual.scheme, actual.netloc) != (expected.scheme, expected.netloc):
                    return JSONResponse({"ok": False, "error": "不允许跨来源提交。"}, status_code=403)
                if path.endswith(("/login/cancel", "/settings", "/sync")):
                    body = await controller._read_json_request(request, max_bytes=16 * 1024)
                if not isinstance(body, dict):
                    raise SyncError("提交内容须为对象。")
            params = request.path_params
            if path.endswith("/login"):
                login = await asyncio.to_thread(browser_login)
                result = await asyncio.to_thread(login.status if method == "GET" else login.start)
            elif path.endswith("/login/cancel"):
                login = await asyncio.to_thread(browser_login)
                result = await asyncio.to_thread(login.cancel, body.get("job_id"))
            elif path.endswith("/test"):
                result = await asyncio.to_thread(auth.test_connection)
            elif path.endswith("/{kind}/settings"):
                result = await asyncio.to_thread(service.configure, params["kind"], body)
            elif path.endswith("/{kind}/preview"):
                result = await asyncio.to_thread(service.enqueue, params["kind"], "preview")
            elif path.endswith("/{kind}/sync"):
                preview_id = body.get("preview_id")
                if not isinstance(preview_id, str):
                    raise SyncError("请先完成本类数据的拉取预览。")
                result = await asyncio.to_thread(service.enqueue, params["kind"], "sync", preview_id)
            elif "id" in params:
                page = max(1, int(request.query_params.get("page", 1)))
                result = await asyncio.to_thread(service.detail, params["id"], page)
            else:
                result = await asyncio.to_thread(service.overview)
            return controller._json_response(request, session, {"ok": True, "data": result})
        except FileNotFoundError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=404)
        except Exception as exc:
            return JSONResponse({"ok": False, "error": describe_error(exc)},
                                status_code=400 if isinstance(exc, ValueError) else 502)

    for suffix, methods in {
        "": ["GET"], "/login": ["GET", "POST"], "/login/cancel": ["POST"], "/test": ["POST"],
        "/{kind}/preview": ["POST"], "/{kind}/sync": ["POST"], "/{kind}/settings": ["PUT"], "/runs/{id}": ["GET"],
    }.items():
        app.add_api_route("/api/zhihang-data" + suffix, endpoint, methods=methods)

    @app.get("/zhihang-data", include_in_schema=False)
    async def page(request: Request):
        session = await asyncio.to_thread(controller._current_session, request)
        if not session:
            return RedirectResponse("/api/auth/login?" + urlencode({"next": "/zhihang-data"}), status_code=302)
        if not await asyncio.to_thread(can_access, request, session):
            return JSONResponse({"ok": False, "error": "仅管理员可使用智航数据拉取。"}, status_code=403)
        from .server import portal_index_file
        return await asyncio.to_thread(controller._static_file_response, request, portal_index_file(), html=True)

    return service

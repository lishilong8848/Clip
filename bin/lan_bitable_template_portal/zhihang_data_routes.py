"""Administrator endpoints for Zhihang data refresh."""
import asyncio
from urllib.parse import urlencode, urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

from . import plan_convergence_auth as auth
from .module_access import allowed_modules
from .plan_convergence_browser_login import BrowserLogin
from .zhihang_data_cards import notify
from .zhihang_data_digest import ZhihangDigests
from .zhihang_data_sync import SyncBusy, SyncError, ZhihangSync, describe_error
from .zhihang_h_drill import HDutyDrillSync, KIND as H_DRILL_KIND

MANUAL_RESULT_WAIT = 15


def install_zhihang_data(app, controller, runtime, browser_login):
    from .portal_auth import topic_execution
    from .account_impersonation import COOKIE

    auth.bind_store(runtime.state_store)
    service = ZhihangSync(runtime.state_store, notify=lambda *args: notify(*args, store=runtime.state_store))
    service.digests = ZhihangDigests(service)
    service.h_duty_drills = h_service = HDutyDrillSync(runtime.state_store)
    h_login = BrowserLogin(runtime.state_store, save_token=h_service.save_login, profile_dir="zhihang_h_drill_auth")
    app.add_event_handler("shutdown", service.close)
    app.add_event_handler("shutdown", h_service.close)
    app.add_event_handler("shutdown", h_login.close)

    async def run_now(kind, mode, preview_id=""):
        started, future = await asyncio.to_thread(service.start_now, kind, mode, preview_id)
        if future is None:
            return started
        pending = asyncio.wrap_future(future)
        done, _ = await asyncio.wait({pending}, timeout=MANUAL_RESULT_WAIT)
        # Long batches already have a durable execution ID and remain visible after disconnect.
        return pending.result() if done else started

    def can_access(request, session, module="zhihang_data"):
        return bool(session and not session.get("is_guest") and not session.get("impersonation")
                    and not getattr(request.state, "admin_impersonation_session", None)
                    and not request.cookies.get(COOKIE)
                    and topic_execution(runtime.auth_manager, session) is None
                    and runtime.auth_manager.is_admin(session)
                    and module in allowed_modules(runtime.auth_manager, session))

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
                result = await run_now(params["kind"], "preview")
            elif path.endswith("/{kind}/sync"):
                preview_id = body.get("preview_id")
                if not isinstance(preview_id, str):
                    raise SyncError("请先完成本类数据的拉取预览。")
                result = await run_now(params["kind"], "sync", preview_id)
            elif "id" in params:
                page = max(1, int(request.query_params.get("page", 1)))
                result = await asyncio.to_thread(service.detail, params["id"], page)
            else:
                history_page = request.query_params.get("history_page")
                result = await asyncio.to_thread(
                    service.overview, history_page=int(history_page) if history_page is not None else None,
                )
            return controller._json_response(request, session, {"ok": True, "data": result})
        except FileNotFoundError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=404)
        except SyncBusy as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=409)
        except Exception as exc:
            return JSONResponse({"ok": False, "error": describe_error(exc)},
                                status_code=400 if isinstance(exc, ValueError) else 502)

    for suffix, methods in {
        "": ["GET"], "/login": ["GET", "POST"], "/login/cancel": ["POST"], "/test": ["POST"],
        "/{kind}/preview": ["POST"], "/{kind}/sync": ["POST"], "/{kind}/settings": ["PUT"], "/runs/{id}": ["GET"],
    }.items():
        app.add_api_route("/api/zhihang-data" + suffix, endpoint, methods=methods)

    async def h_drill_endpoint(request: Request):
        session = await asyncio.to_thread(controller._current_session, request)
        if not session:
            return controller._auth_required_response()
        if not await asyncio.to_thread(can_access, request, session, "drills"):
            return JSONResponse({"ok": False, "error": "当前账号无权管理 H 楼值班演练。"}, status_code=403)
        try:
            path = request.scope["route"].path
            if request.method != "GET":
                origin = request.headers.get("origin") or request.headers.get("referer")
                expected = urlsplit(controller._request_base_url(request))
                actual = urlsplit(origin) if origin else expected
                if (request.headers.get("sec-fetch-site") == "cross-site"
                        or (actual.scheme, actual.netloc) != (expected.scheme, expected.netloc)):
                    return JSONResponse({"ok": False, "error": "不允许跨来源提交。"}, status_code=403)
            if path.endswith("/records"):
                result = await asyncio.to_thread(h_service.records)
                from .module_access import professional_record_visible, session_engineer_professions

                professions = await asyncio.to_thread(session_engineer_professions, runtime.auth_manager, session)
                if professions is not None:
                    result = {**result, 'rows': [row for row in result['rows'] if professional_record_visible(professions, row)]}
            elif path.endswith("/login"):
                result = await asyncio.to_thread(h_login.start)
            elif path.endswith("/settings"):
                body = await controller._read_json_request(request, max_bytes=4096)
                if not isinstance(body, dict):
                    raise SyncError("设置内容须为对象。")
                result = await asyncio.to_thread(h_service.configure_h, body)
            elif path.endswith("/preview"):
                result = await asyncio.to_thread(h_service.enqueue, H_DRILL_KIND, "preview")
            elif path.endswith("/sync"):
                body = await controller._read_json_request(request, max_bytes=4096)
                if not isinstance(body, dict) or not isinstance(body.get("preview_id"), str):
                    raise SyncError("请先完成演练数据预览。")
                result = await asyncio.to_thread(h_service.enqueue, H_DRILL_KIND, "sync", body["preview_id"])
            elif "id" in request.path_params:
                page = max(1, int(request.query_params.get("page", 1)))
                result = await asyncio.to_thread(h_service.detail, request.path_params["id"], page)
            else:
                result = await asyncio.to_thread(h_service.status)
                result["login_job"] = h_login.status()
            return controller._json_response(request, session, {"ok": True, "data": result})
        except FileNotFoundError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=404)
        except Exception as exc:
            return JSONResponse({"ok": False, "error": describe_error(exc)},
                                status_code=400 if isinstance(exc, ValueError) else 502)

    for suffix, methods in {
        "": ["GET"], "/records": ["GET"], "/settings": ["PUT"], "/preview": ["POST"], "/login": ["POST"],
        "/sync": ["POST"], "/runs/{id}": ["GET"],
    }.items():
        app.add_api_route("/api/drills/zhihang-h" + suffix, h_drill_endpoint, methods=methods)

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

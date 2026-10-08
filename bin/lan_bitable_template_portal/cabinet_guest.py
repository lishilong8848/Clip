"""Cabinet-only visitor access. All unlisted business routes remain denied."""
import re

GUEST_SCOPES = ("A", "B", "C", "D", "E")
GUEST_SESSION_PREFIX = "guest_"


def is_cabinet_guest(session):
    return isinstance(session, dict) and session.get("role") == "guest"


def cabinet_guest_api_allowed(method, path):
    if method == "POST":
        return path == "/api/cabinet-power/exports"
    if method != "GET":
        return False
    return bool(re.fullmatch(
        r"/api/cabinet-power/(?:buildings|overview|rooms|racks|operations|export-history"
        r"|rooms/[^/]+/layout|jobs/[^/]+|exports/[^/]+/download"
        r"|operations/[^/]+/(?:evidence|documents)/[^/]+)", path))


def install_cabinet_guest_access(app, controller, runtime):
    from fastapi import Request
    from fastapi.responses import JSONResponse, RedirectResponse
    from .portal_auth import AUTH_COOKIE_NAME

    @app.post("/api/auth/guest")
    async def guest_login(request: Request):
        # Do not replace a signed-in employee's session with a visitor identity.
        if controller._current_session(request) is not None:
            return RedirectResponse("/cabinet-power", status_code=303)
        session_id = runtime.auth_manager.create_guest_session()
        return RedirectResponse("/cabinet-power", status_code=303, headers={
            "Set-Cookie": runtime.auth_manager.cookie_header(session_id),
            "Cache-Control": "no-store",
        })

    @app.middleware("http")
    async def guest_access(request: Request, call_next):
        if not str(request.cookies.get(AUTH_COOKIE_NAME) or "").startswith(GUEST_SESSION_PREFIX):
            return await call_next(request)
        session = controller._current_session(request)
        if is_cabinet_guest(session):
            method, path = request.method, request.url.path
            auth_route = (
                method == "GET" and path in {
                    "/api/auth/status", "/api/auth/login", "/api/auth/feishu/callback", "/api/auth/logout",
                } or method == "POST" and path in {"/api/auth/guest", "/api/auth/logout"}
            )
            static_route = method == "GET" and (
                path.startswith("/assets/") or path in {"/favicon.ico", "/clipflow-favicon.svg", "/cabinet-power", "/cabinet-power/"}
            )
            health_probe = method == "GET" and path == "/api/health" and request.query_params.get("probe") == "1"
            if not (auth_route or static_route or health_probe or cabinet_guest_api_allowed(method, path)):
                if path.startswith("/api/") or method != "GET":
                    return JSONResponse({"ok": False, "error": "访客仅可查看机柜数据和导出单楼文件。"}, status_code=403)
                return RedirectResponse("/cabinet-power", status_code=303)
        return await call_next(request)

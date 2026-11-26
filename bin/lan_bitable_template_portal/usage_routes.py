"""Browser telemetry uses a bounded local write; the request never waits for Feishu."""
import asyncio
import logging
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse

from .usage_metrics import record_usage_events


def install_usage_routes(app, controller, runtime):
    @app.post("/api/usage/events")
    async def receive(request: Request):
        try:
            origin = urlsplit(request.headers.get("origin") or request.headers.get("referer") or "")
            expected = urlsplit(controller._request_base_url(request))
            if ((origin.scheme, origin.netloc) != (expected.scheme, expected.netloc)
                    or request.headers.get("sec-fetch-site") == "cross-site"):
                return JSONResponse({"ok": False, "error": "不允许跨来源提交。"}, status_code=403)
            if request.headers.get("content-type","").split(";",1)[0].strip().lower() != "application/json":
                return JSONResponse({"ok": False, "error": "请使用 JSON。"}, status_code=415)
            body = await controller._read_json_request(request, max_bytes=16 * 1024)
            if not isinstance(body, dict) or body.keys() != {"events"}:
                raise ValueError("使用统计请求字段无效。")
            session = await asyncio.to_thread(controller._current_session, request)
            result = await asyncio.to_thread(record_usage_events, runtime.state_store.db_path.parent,
                                             body["events"], web_only=True, public_only=not session,
                                             guest=bool(session and session.get("role") == "guest"))
            return {"ok": True, "data": result}
        except PermissionError:
            return JSONResponse({"ok": False, "error": "请登录后上报该页面。"}, status_code=401)
        except (ValueError, UnicodeError):
            return JSONResponse({"ok": False, "error": "使用统计请求格式无效。"}, status_code=400)
        except OSError as exc:
            logging.warning("使用统计暂存失败 (%s)", type(exc).__name__)
        except Exception:
            logging.exception("使用统计暂存失败")
        return JSONResponse({"ok": False, "error": "使用统计暂未保存，可稍后重试。"}, status_code=503)

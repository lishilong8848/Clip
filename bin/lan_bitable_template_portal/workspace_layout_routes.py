"""Small account-owned display preferences; never a business-data write path."""
import asyncio
import json
import logging
import re
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse

from .portal_service import PortalError


PAGES = frozenset("workspace notices sops mop repair cabinet events daily links signatures accounts handover "
                  "plan-review plan-audit plan-rules guard-building guard-admin learning-practice learning-profile".split())
WIDGET_FIELDS = frozenset("id kind title module scope display period metric presentation links taskKinds x y w h builtin minW minH".split())


def validate_layout(page, value):
    if not isinstance(value, dict):
        raise ValueError("布局格式无效。")
    if page == "workspace":
        layout, field = value, "widgets"
        if set(value) != {"version", "widgets"}:
            raise ValueError("工作台布局字段无效。")
    else:
        if set(value) != {"layout", "presentations"} or not isinstance(value["presentations"], dict):
            raise ValueError("页面布局字段无效。")
        layout, field = value["layout"], "items"
        if not isinstance(layout, dict) or set(layout) != {"version", "items"}:
            raise ValueError("页面布局格式无效。")
    if type(layout.get("version")) is not int or layout["version"] != 1 or not isinstance(layout.get(field), list) or len(layout[field]) > 16:
        raise ValueError("布局版本或组件数量无效。")
    ids = set()
    for item in layout[field]:
        if not isinstance(item, dict) or set(item) - WIDGET_FIELDS:
            raise ValueError("布局组件字段无效。")
        identity = item.get("id")
        if not isinstance(identity, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", identity) or identity in ids:
            raise ValueError("布局组件标识无效。")
        ids.add(identity)
        for key, minimum, maximum in (("x", 0, 11), ("y", 0, 1000), ("w", 1, 12), ("h", 1, 30)):
            if type(item.get(key)) is not int or not minimum <= item[key] <= maximum:
                raise ValueError("布局组件尺寸无效。")
        if item["x"] + item["w"] > 12:
            raise ValueError("布局组件超出页面宽度。")
        for key, entry in item.items():
            if key in {"x", "y", "w", "h", "id"}:
                continue
            if key in {"minW", "minH"}:
                valid = type(entry) is int and 1 <= entry <= (12 if key == "minW" else 30)
            elif key == "builtin":
                valid = type(entry) is bool
            elif key == "links":
                valid = isinstance(entry, list) and len(entry) <= 64 and all(isinstance(link, str) and re.fullmatch(r"[a-z_]{1,48}", link) for link in entry)
            elif key == "taskKinds":
                valid = (page == "workspace" and item.get("kind") == "tasks" and isinstance(entry, list)
                         and len(entry) <= 7 and all(isinstance(kind, str) and kind in {
                             "notices", "repair_management", "critical_guard", "risk_inspection", "water", "learning", "signatures"
                         } for kind in entry) and len(set(entry)) == len(entry))
            else:
                valid = isinstance(entry, str) and len(entry) <= 80
            if not valid:
                raise ValueError("布局组件内容无效。")
    if page != "workspace" and any(key not in ids or not isinstance(entry, str) or entry not in {"default", "compact", "comfortable"} for key, entry in value["presentations"].items()):
        raise ValueError("布局显示方式无效。")
    return value


def install_workspace_layout_routes(app, controller, runtime):
    async def endpoint(request: Request):
        headers = {"Cache-Control": "private, no-store"}
        try:
            session = await asyncio.to_thread(controller._current_session, request)
            if not session:
                return controller._auth_required_response()
            user = session.get("user") or {}
            owner = str(user.get("open_id") or "").strip()
            if not owner or session.get("role") in {"guest", "temporary"}:
                return JSONResponse({"ok": False, "error": "请使用正式账号保存个人布局。"}, status_code=403, headers=headers)
            page, device = request.path_params["page"], request.path_params["device"]
            if page not in PAGES or device not in {"desktop", "mobile"} or request.query_params:
                raise ValueError("布局页面或设备类型无效。")
            if request.method == "GET":
                result = await asyncio.to_thread(runtime.state_store.get_workspace_layout, owner, device, page)
            else:
                source = urlsplit(request.headers.get("origin") or request.headers.get("referer") or "")
                expected = urlsplit(controller._request_base_url(request))
                if (source.scheme, source.netloc) != (expected.scheme, expected.netloc) or request.headers.get("sec-fetch-site") == "cross-site":
                    return JSONResponse({"ok": False, "error": "不允许跨来源提交。"}, status_code=403, headers=headers)
                content = bytearray()
                async for part in request.stream():
                    content.extend(part)
                    if len(content) > 32 * 1024:
                        return JSONResponse({"ok": False, "error": "布局内容过大。"}, status_code=413, headers=headers)
                payload = json.loads(content)
                if not isinstance(payload, dict) or set(payload) != {"revision", "value"} or type(payload["revision"]) is not int or not 0 <= payload["revision"] <= 2**53 - 1:
                    raise ValueError("布局保存请求无效。")
                value = validate_layout(page, payload["value"])
                result = await asyncio.to_thread(runtime.state_store.save_workspace_layout, owner, device, page, value, payload["revision"])
            return JSONResponse({"ok": True, "data": result}, headers=headers)
        except (PortalError, ValueError) as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=getattr(exc, "status_code", 400), headers=headers)
        except Exception:
            logging.exception("Account layout storage failed")
            return JSONResponse({"ok": False, "error": "账号布局暂未同步，当前布局已保留，请稍后重试。"}, status_code=503, headers=headers)

    app.add_api_route("/api/workspace-layouts/{device}/{page}", endpoint, methods=["GET", "PUT"])

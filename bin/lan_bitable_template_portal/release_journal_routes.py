"""Read-only release journal endpoints using the existing administrator policy."""
import asyncio

from fastapi import Request
from fastapi.responses import JSONResponse

from .release_journal import ReleaseJournalError, ReleaseJournalStore


def install_release_journal_routes(app, controller, runtime, *, store=None):
    headers = {"Cache-Control": "private, no-store", "Vary": "Cookie"}

    def get_store():
        if store is not None:
            return store
        from upload_event_module.utils import get_data_file_path
        return ReleaseJournalStore(get_data_file_path(""))

    async def endpoint(request: Request):
        session = await asyncio.to_thread(controller._current_session, request)
        if not session:
            return JSONResponse({"ok": False, "error": "请先登录。"}, status_code=401, headers=headers)
        if session.get("is_guest") or session.get("role") in {"guest", "temporary"} or not runtime.auth_manager.is_admin(session):
            return JSONResponse({"ok": False, "error": "仅管理员可查看更新记录。"}, status_code=403, headers=headers)
        try:
            journal = get_store()
            identity = request.path_params.get("identity")
            if identity is None:
                try:
                    page = int(request.query_params.get("page", "1"))
                except ValueError:
                    raise ReleaseJournalError("更新记录分页参数无效。") from None
                value = await asyncio.to_thread(journal.list, q=request.query_params.get("q", ""),
                                                module=request.query_params.get("module", ""), page=page)
            else:
                value = await asyncio.to_thread(journal.detail, identity)
                if value is None:
                    return JSONResponse({"ok": False, "error": "更新记录不存在。"}, status_code=404, headers=headers)
            return JSONResponse({"ok": True, "data": value}, headers=headers)
        except ReleaseJournalError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=400, headers=headers)
        except Exception:
            return JSONResponse({"ok": False, "error": "更新记录暂不可读，原历史已保留。"}, status_code=503, headers=headers)

    app.add_api_route("/api/release-journal", endpoint, methods=["GET"])
    app.add_api_route("/api/release-journal/{identity}", endpoint, methods=["GET"])

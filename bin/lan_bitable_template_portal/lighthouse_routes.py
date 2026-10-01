"""Assistant APIs: authenticated read-only knowledge and private model settings."""
import asyncio
import json
import threading
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from starlette.formparsers import MultiPartException, MultiPartParser

from .lighthouse_ai import AssistantError, LighthouseAssistant
from .lighthouse_sources import LocalAssistantSources, SCOPES
from .portal_service import BUILDING_OPEN_ID_MAP, LI_SHILONG_OPEN_ID, MA_JINYU_OPEN_ID

MODEL_SETTINGS_USERS = frozenset((LI_SHILONG_OPEN_ID, MA_JINYU_OPEN_ID, BUILDING_OPEN_ID_MAP["H"]))


def install_lighthouse_routes(app, controller, runtime):
    service = None
    agent = None
    streams = None
    lock = threading.Lock()

    def get_service():
        nonlocal service
        with lock:
            if service is None:
                service = LighthouseAssistant(runtime.state_store, LocalAssistantSources(runtime.state_store))
            return service

    def get_agent():
        nonlocal agent
        current = get_service()
        with lock:
            if agent is None:
                from .lighthouse_api import PortalAPICatalog
                from .lighthouse_agent import PortalAgent
                from .lighthouse_files import LighthouseFiles
                agent = PortalAgent(current, PortalAPICatalog(app), LighthouseFiles(runtime.state_store))
            return agent

    async def ready_agent():
        return await asyncio.to_thread(get_agent)

    def get_streams():
        nonlocal streams
        current_agent = get_agent()
        with lock:
            if streams is None:
                # Imported in ready_streams' worker, never on the FastAPI loop or Qt thread.
                from pydantic_ai.ui.vercel_ai import response_types
                from .lighthouse_stream import LighthouseStream
                from .lighthouse_pending import cached_items
                streams = LighthouseStream(current_agent, cached_reader=lambda kind, scopes, allowed:
                    cached_items(kind, scopes, runtime, allowed=allowed))
            return streams

    async def ready_streams():
        return await asyncio.to_thread(get_streams)

    async def actor_for(request):
        session = await asyncio.to_thread(controller._current_session, request)
        if not session:
            raise AssistantError("请重新登录后继续。", 401)
        user = session.get("user") or {}
        identity = str(session.get("open_id") or user.get("open_id") or "").strip()
        if not identity or session.get("is_guest") or str(session.get("role") or user.get("role")).lower() == "guest":
            raise AssistantError("请使用正式账号登录后使用灯塔助手。", 403)
        allowed = set(runtime.auth_manager.session_scopes(session))
        if "ALL" in allowed:
            allowed = set(SCOPES)
        if "CAMPUS" in allowed:
            allowed.update("ABCDE")
        actor = {"id": identity, "is_admin": runtime.auth_manager.is_admin(session),
                 "scopes": sorted(allowed & SCOPES), "can_manage_settings": identity in MODEL_SETTINGS_USERS}
        actor["learning_scopes"] = sorted(set("ABCDEH") & set(actor["scopes"])) if actor["is_admin"] else [
            code for code in "ABCDEH" if BUILDING_OPEN_ID_MAP.get(code) == identity and code in actor["scopes"]]
        if not actor["scopes"]:
            raise AssistantError("当前账号尚无业务访问权限。", 403)
        return actor

    async def shutdown():
        if streams is not None:
            await streams.close()
        if agent is not None:
            for task in tuple(agent.tasks):
                task.cancel()
            if agent.tasks:
                await asyncio.gather(*tuple(agent.tasks), return_exceptions=True)
        if service is not None:
            await asyncio.to_thread(service.model.close)
    app.add_event_handler("shutdown", shutdown)

    async def endpoint(request: Request):
        try:
            actor = await actor_for(request)
            action = request.url.path.removeprefix("/api/assistant/")
            if action == "settings" and not actor["can_manage_settings"]:
                raise AssistantError("仅李世龙、马进宇和H楼值班账号可以查看及修改助手设置。", 403)
            payload = {}
            if request.method != "GET":
                source = request.headers.get("origin") or request.headers.get("referer")
                expected = urlsplit(controller._request_base_url(request))
                actual = urlsplit(source) if source else None
                if not source or request.headers.get("sec-fetch-site", "").lower() == "cross-site" or (actual and (actual.scheme, actual.netloc) != (expected.scheme, expected.netloc)):
                    raise AssistantError("不允许跨来源提交。", 403)
                if action == "files":
                    current_agent = await ready_agent()
                    from .lighthouse_files import MAX_BATCH_BYTES, MAX_FILE_BYTES
                    total = 0
                    async def bounded_stream():
                        nonlocal total
                        async for chunk in request.stream():
                            total += len(chunk)
                            if total > MAX_BATCH_BYTES + 65536:
                                # The native parser closes spooled files on this exception.
                                raise MultiPartException("附件合计不得超过100MiB。")
                            yield chunk
                    try:
                        form = await MultiPartParser(request.headers, bounded_stream(), max_files=10, max_fields=0).parse()
                    except MultiPartException:
                        raise AssistantError("上传格式无效：每次1至10个文件，合计最多100MiB。", 413) from None
                    try:
                        uploads = form.getlist("files")
                        if set(form) != {"files"} or not 1 <= len(uploads) <= 10 or any(not hasattr(upload, "read") for upload in uploads):
                            raise AssistantError("上传文件格式无效。")
                        if any(not upload.size or upload.size > MAX_FILE_BYTES for upload in uploads) or sum(upload.size for upload in uploads) > MAX_BATCH_BYTES:
                            raise AssistantError("单文件须为1字节至20MiB，合计最多100MiB。", 413)
                        results = []
                        for upload in uploads:
                            content = await upload.read(MAX_FILE_BYTES + 1)
                            results.append(await asyncio.to_thread(current_agent.files.upload, actor, upload.filename, content))
                    finally:
                        await form.close()
                    return JSONResponse({"ok": True, "data": {"files": results}}, headers={"Cache-Control": "no-store"})
                body = bytearray()
                async for chunk in request.stream():
                    if len(body) + len(chunk) > (128000 if action in {"agent", "messages"} or action.startswith("plans/") else 16000):
                        raise AssistantError("提交内容过大。", 413)
                    body.extend(chunk)
                try:
                    payload = json.loads(body or b"{}")
                except (ValueError, UnicodeError, RecursionError):
                    raise AssistantError("请求格式无效。") from None
                if not isinstance(payload, dict):
                    raise AssistantError("请求内容须为对象。")
            if action == "messages":
                current_streams = await ready_streams()
                data = await current_streams.submit(actor, payload, request, lambda: actor_for(request))
                return JSONResponse({"ok": True, "data": data}, status_code=202, headers={"Cache-Control": "no-store"})
            if action == "history":
                current_streams = await ready_streams()
                try:
                    before = float(request.query_params.get("before", "0"))
                except ValueError:
                    raise AssistantError("历史分页位置无效。") from None
                data = await current_streams.history(actor, before)
                return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
            if action.startswith("runs/"):
                current_streams = await ready_streams()
                run_id = action.split("/")[1]
                if action.endswith("/cancel"):
                    data = await current_streams.stop(actor, run_id)
                    return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
                await asyncio.to_thread(current_streams.get_run, actor, run_id)
                return StreamingResponse(current_streams.stream(actor, run_id, lambda: actor_for(request)),
                    media_type="text/event-stream", headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no", "x-vercel-ai-ui-message-stream": "v1"})
            if action.startswith("files/"):
                current_agent = await ready_agent()
                item = await asyncio.to_thread(current_agent.files.get, actor, action.split("/")[1])
                return FileResponse(item["path"], media_type=item["mime"], filename=item["name"], content_disposition_type="inline" if item["mime"].startswith("image/") else "attachment", headers={"Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff"})
            if action == "capabilities":
                current_agent = await ready_agent()
                data = current_agent.catalog.discover(keyword=str(request.query_params.get("keyword", "")), group=str(request.query_params.get("group", "")), page=request.query_params.get("page", 1), page_size=request.query_params.get("page_size", 30))
                return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
            if action == "pending":
                from .lighthouse_pending import cached_items, collect_pending
                current_agent = await ready_agent()
                data = await collect_pending(actor, str(request.query_params.get("q") or ""),
                    lambda operation: current_agent._invoke(actor, operation, request),
                    lambda kind, scopes: cached_items(kind, scopes, runtime, allowed=actor["scopes"]))
                return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
            if action in {"agent", "chat"}:
                # One previous UI generation still expects a single JSON reply.
                # It uses the same worker, permissions and operation ID as SSE.
                current_streams = await ready_streams()
                await current_streams.submit(actor, payload, request, lambda: actor_for(request))
                task = current_streams.workers.get(actor["id"])
                if task:
                    await asyncio.shield(task)
                data = await current_streams.conversation(actor)
                return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
            if action.startswith("plans/"):
                identity = action.split("/")[1]
                current_agent = await ready_agent()
                if request.method == "PATCH":
                    data = await asyncio.to_thread(current_agent.amend, actor, identity, payload)
                elif action.endswith("/confirm"):
                    data = await current_agent.confirm(actor, identity, payload, request)
                elif action.endswith("/cancel"):
                    data = await asyncio.to_thread(current_agent.cancel, actor, identity)
                elif action.endswith("/options"):
                    data = await current_agent.field_options(actor, identity, str(request.query_params.get("field", "")), request)
                else:
                    data = await current_agent.refresh(actor, current_agent.get_plan(actor, identity), request)
                return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
            current = get_service()
            if action == "conversation":
                if request.method == "GET":
                    current_streams = await ready_streams()
                    data = await current_streams.conversation(actor)
                    if agent is not None:
                        refreshed = False
                        for turn in data.get("turns", []):
                            if (turn.get("plan") or {}).get("status") in {"running", "submitted"}:
                                await agent.refresh(actor, agent.get_plan(actor, turn["plan"]["id"]), request)
                                refreshed = True
                        if refreshed:
                            data = await current_streams.conversation(actor)
                    return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
                if request.method == "DELETE":
                    current_turns = (await asyncio.to_thread(current.conversation, actor)).get("turns", [])
                    if (agent is not None and actor["id"] in agent.executing) or any((turn.get("plan") or {}).get("status") in {"running", "submitted"} for turn in current_turns):
                        raise AssistantError("业务操作仍在处理，请完成后再清空会话。", 409)
                if request.method == "PATCH":
                    fn, args = current.select_model, (actor, payload)
                else:
                    fn, args = (current.clear, (actor,)) if request.method == "DELETE" else (current.conversation, (actor,))
            else:
                fn, args = (current.model.configure, (payload,)) if request.method == "PUT" else (current.model.settings, ())
            data = await asyncio.to_thread(fn, *args)
            if action == "conversation":
                data = await (await ready_streams()).conversation(actor)
            # Conversation and configuration must not enter shared/browser caches.
            return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
        except AssistantError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=exc.status, headers={"Cache-Control": "no-store"})
        except Exception:
            # Do not log request bodies, vendor response bodies or decrypted credentials.
            return JSONResponse({"ok": False, "error": "助手暂时不可用，请稍后重试。"}, status_code=503, headers={"Cache-Control": "no-store"})

    for path, methods in (("conversation", ["GET", "DELETE", "PATCH"]), ("chat", ["POST"]), ("settings", ["GET", "PUT"])):
        for method in methods:
            app.add_api_route("/api/assistant/" + path, endpoint, methods=[method], name="lighthouse_" + path.replace("/", "_") + "_" + method.lower())
    for path, methods in (("messages", ["POST"]), ("history", ["GET"]), ("runs/{run_id}/stream", ["GET"]), ("runs/{run_id}/cancel", ["POST"]), ("agent", ["POST"]), ("pending", ["GET"]), ("capabilities", ["GET"]), ("files", ["POST"]), ("files/{file_id}", ["GET"]), ("plans/{plan_id}", ["GET", "PATCH"]), ("plans/{plan_id}/confirm", ["POST"]), ("plans/{plan_id}/cancel", ["POST"]), ("plans/{plan_id}/options", ["GET"])):
        for method in methods:
            app.add_api_route("/api/assistant/" + path, endpoint, methods=[method], name="lighthouse_" + path.replace("/", "_") + "_" + method.lower())

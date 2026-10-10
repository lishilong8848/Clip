"""Assistant API implementation, served only by the independent local service."""
import asyncio
import json
import logging
import os
import threading

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from starlette.formparsers import MultiPartException, MultiPartParser

from .lighthouse_ai import AssistantError, LighthouseAssistant
from .lighthouse_startup_log import emit as startup_log
from ..protocol import MAX_CONCURRENT_ACCOUNTS


def install_assistant_routes(app, host):
    service = None
    agent = None
    streams = None
    lock = threading.Lock()
    preparation = None
    closing = False
    pending_warmups = {}
    tagging_pool = None
    knowledge = None
    knowledge_lock = threading.Lock()

    def get_knowledge():
        nonlocal knowledge
        with knowledge_lock:
            if knowledge is None:
                from .lighthouse_knowledge import KnowledgeBase
                knowledge = KnowledgeBase(host.state / 'knowledge_base')
            return knowledge

    async def recommend_notice_tags(payload):
        nonlocal tagging_pool
        from concurrent.futures import ThreadPoolExecutor
        from upload_event_module.services.process_lifetime import lower_current_thread_priority
        from .lighthouse_alert_tagging import recommend
        if host.store is None or closing:
            raise AssistantError('标签服务尚未就绪。', 503)
        owner, notices = payload.get('owner'), payload.get('notices')
        if not isinstance(owner, str) or len(owner) > 128 or not isinstance(notices, list) or not 1 <= len(notices) <= 6:
            raise AssistantError('标签请求无效。')
        if tagging_pool is None:
            tagging_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix='NoticeTags', initializer=lower_current_thread_priority)
        def work():
            current = get_service()
            model = current.model.for_actor(owner) if owner else current.model
            return recommend(model, notices)
        return await asyncio.get_running_loop().run_in_executor(tagging_pool, work)

    def get_service():
        nonlocal service
        with lock:
            if service is None:
                service = LighthouseAssistant(host.store, host.portal_bridge.search)
            return service

    def get_agent():
        nonlocal agent
        current = get_service()
        with lock:
            if agent is None:
                from .lighthouse_agent import PortalAgent
                from .lighthouse_files import LighthouseFiles
                agent = PortalAgent(current, host.catalog, LighthouseFiles(host.store, root=host.state / 'files'))
                agent.get_knowledge = get_knowledge
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
                cached_reader = host.portal_bridge.cached
                engine = None
                if os.environ.get("LIGHTHOUSE_AGENT_ENGINE", "openclaw").strip().lower() != "legacy":
                    from .lighthouse_openclaw import LighthouseOpenClaw
                    engine = LighthouseOpenClaw(current_agent, cached_reader=cached_reader,
                        bridge_url=lambda: "http://127.0.0.1:" + str(host.port) + "/api/assistant/openclaw-tools",
                        manager=host.manager)
                streams = LighthouseStream(current_agent, cached_reader=cached_reader, engine=engine)
            return streams

    async def ready_streams():
        task = asyncio.create_task(asyncio.to_thread(get_streams))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # Cancellation cannot stop a Python import already running in a worker.
            await asyncio.gather(task, return_exceptions=True)
            raise

    async def prepare_runtime():
        try:
            current = await ready_streams()
            if getattr(current.engine, 'public_sources', None) and hasattr(host, 'spawn'):
                host.spawn(asyncio.to_thread(current.engine.public_sources.prepare))
            if getattr(current.engine, 'manages_runtime', False) is True:
                for actor in tuple(pending_warmups.values()):
                    warm_current(current, actor)
                pending_warmups.clear()
                await current.engine.prepare()
                from .lighthouse_skills import catalog
                startup_log('skills_ready', skills=len(await asyncio.to_thread(catalog)))
                if not getattr(current.engine, 'warming', {}) and not getattr(getattr(current.engine, 'manager', None), 'accounts', {}):
                    startup_log('awaiting_login')
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            startup_log('startup_failed', error=type(exc).__name__)
            logging.getLogger(__name__).warning('Assistant startup preparation failed: type=%s', type(exc).__name__)

    async def startup():
        nonlocal preparation
        # Do not delay the portal listener or execute work on Qt's main thread.
        if os.environ.get("LIGHTHOUSE_AGENT_ENGINE", "openclaw").strip().lower() != "legacy":
            startup_log('preparing')
            preparation = asyncio.create_task(prepare_runtime())
        else:
            startup_log('legacy_engine')

    def warm_current(current, actor):
        if not closing and getattr(current.engine, 'manages_runtime', False) is True:
            current.engine.queue_warmup(actor)

    def warm_authenticated(actor):
        if streams is not None:
            warm_current(streams, actor)
        elif not closing and preparation is not None and not preparation.done() and (
                actor['id'] in pending_warmups or len(pending_warmups) < (MAX_CONCURRENT_ACCOUNTS or 2)):
            pending_warmups[actor['id']] = actor

    async def actor_for(request):
        return await host.authorize(request)

    async def shutdown():
        nonlocal closing
        closing = True
        if tagging_pool is not None:
            tagging_pool.shutdown(wait=False, cancel_futures=True)
        pending_warmups.clear()
        if preparation:
            preparation.cancel()
            await asyncio.gather(preparation, return_exceptions=True)
        if streams is not None:
            await streams.close()
        if agent is not None:
            for task in tuple(agent.tasks):
                task.cancel()
            if agent.tasks:
                await asyncio.gather(*tuple(agent.tasks), return_exceptions=True)
        if service is not None:
            await asyncio.to_thread(service.model.close)
        if knowledge is not None:
            await asyncio.to_thread(knowledge.close)
    # Host starts preparation only after migration and portal registration.
    from .lighthouse_knowledge_routes import install_knowledge_routes
    install_knowledge_routes(app, actor_for, get_knowledge)

    async def openclaw_tools(request: Request):
        try:
            if not request.client or request.client.host != "127.0.0.1" or request.headers.get("origin"):
                raise AssistantError("不允许访问内部工具通道。", 403)
            authorization = request.headers.get("authorization", "")
            token = authorization.removeprefix("Bearer ") if authorization.startswith("Bearer ") else ""
            bridge = getattr(getattr(streams, "engine", None), "bridge", None)
            if not bridge or not token or len(token) > 100:
                raise AssistantError("内部工具认证未通过。", 403)
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 256 * 1024:
                    raise AssistantError("工具参数过大。", 413)
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise AssistantError("工具参数无效。")
            result = await bridge.call_shared(token, payload, host.manager)
            return JSONResponse(result, headers={"Cache-Control": "no-store"})
        except AssistantError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=exc.status)
        except Exception:
            return JSONResponse({"ok": False, "error": "工具调用未完成。"}, status_code=503)

    app.add_api_route("/api/assistant/openclaw-tools", openclaw_tools, methods=["POST"], name="lighthouse_internal_tools")

    async def endpoint(request: Request):
        try:
            actor = await actor_for(request)
            action = request.url.path.removeprefix("/api/assistant/")
            payload = {}
            if request.method != "GET":
                # Browser CSRF is checked by the portal; host.authorize requires
                # the private service key and a live portal-issued context.
                if action == 'skills/install':
                    from .lighthouse_shared_skills import SharedSkills, MAX_ARCHIVE_BYTES
                    total = 0
                    async def skill_stream():
                        nonlocal total
                        async for chunk in request.stream():
                            total += len(chunk)
                            if total > MAX_ARCHIVE_BYTES + 65536:
                                raise MultiPartException('技能文件不得超过10MiB。')
                            yield chunk
                    try:
                        form = await MultiPartParser(request.headers, skill_stream(), max_files=1, max_fields=0).parse()
                    except MultiPartException:
                        raise AssistantError('请上传一个不超过10MiB的SKILL.md或技能ZIP包。', 413) from None
                    try:
                        uploads = form.getlist('file')
                        if set(form) != {'file'} or len(uploads) != 1 or not hasattr(uploads[0], 'read'):
                            raise AssistantError('请上传一个技能文件。')
                        upload = uploads[0]
                        content = await upload.read(MAX_ARCHIVE_BYTES + 1)
                        data = await asyncio.to_thread(SharedSkills(host.store).install, actor, upload.filename, content)
                    finally:
                        await form.close()
                    return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'no-store'})
                if action == "files":
                    current_agent = await ready_agent()
                    from .lighthouse_files import upload_limit
                    purpose = str(request.query_params.get("purpose") or "").strip() or None
                    limits = upload_limit(actor, purpose)
                    max_bytes = limits["max_bytes"]
                    batch_bytes = limits["batch_bytes"]
                    max_items = limits["max_items"]
                    is_drill = purpose == "drill_template"
                    total = 0
                    async def bounded_stream():
                        nonlocal total
                        async for chunk in request.stream():
                            total += len(chunk)
                            if total > batch_bytes + 65536:
                                # The native parser closes spooled files on this exception.
                                raise MultiPartException("附件合计不得超过100MiB。")
                            yield chunk
                    try:
                        form = await MultiPartParser(request.headers, bounded_stream(), max_files=max_items, max_fields=0).parse()
                    except MultiPartException:
                        if is_drill:
                            raise AssistantError("上传格式无效：演练模板每次仅1个文件，合计最多100MiB。", 413) from None
                        raise AssistantError(f"上传格式无效：每次1至{max_items}个文件，合计最多100MiB。", 413) from None
                    try:
                        uploads = form.getlist("files")
                        if set(form) != {"files"} or not 1 <= len(uploads) <= max_items or any(not hasattr(upload, "read") for upload in uploads):
                            if is_drill:
                                raise AssistantError("演练模板每次仅可上传1个文件。")
                            raise AssistantError("上传文件格式无效。")
                        if any(not upload.size or upload.size > max_bytes for upload in uploads) or sum(upload.size for upload in uploads) > batch_bytes:
                            if is_drill:
                                raise AssistantError("演练模板单文件须为1字节至64MiB，合计最多100MiB。", 413)
                            raise AssistantError(f"单文件须为1字节至{max_bytes // (1024 * 1024)}MiB，合计最多100MiB。", 413)
                        results = []
                        for upload in uploads:
                            content = await upload.read(max_bytes + 1)
                            results.append(await asyncio.to_thread(
                                current_agent.files.upload, actor, upload.filename, content,
                                extract=limits["extract"], purpose=purpose))
                    finally:
                        await form.close()
                    return JSONResponse({"ok": True, "data": {"files": results}}, headers={"Cache-Control": "no-store"})
                body = bytearray()
                body_limit = 128000 if action in {"agent", "messages"} or action.startswith("plans/") else 16000
                if action.startswith("plans/") and (request.method == "PATCH" or action.endswith("/cabinet-text-preview")):
                    current_agent = await ready_agent()
                    plan = await asyncio.to_thread(current_agent.get_plan, actor, action.split("/")[1])
                    if any(field.get("native_cabinet_text_fill") or field.get("native_cabinet_text_create") or field.get("native_cabinet_edit") for field in plan.get("fields", [])):
                        body_limit = 4 * 1024 * 1024
                async for chunk in request.stream():
                    if len(body) + len(chunk) > body_limit:
                        raise AssistantError("提交内容过大。", 413)
                    body.extend(chunk)
                try:
                    payload = json.loads(body or b"{}")
                except (ValueError, UnicodeError, RecursionError):
                    raise AssistantError("请求格式无效。") from None
                if not isinstance(payload, dict):
                    raise AssistantError("请求内容须为对象。")
            if action == "appearance":
                from .lighthouse_appearance import read_appearance, save_appearance
                args = (host.store, actor["id"])
                data = await asyncio.to_thread(read_appearance, *args) if request.method == "GET" else await asyncio.to_thread(save_appearance, *args, payload)
                warm_authenticated(actor)
                return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
            if action == 'commands':
                from .lighthouse_commands import commands
                current_agent = await ready_agent()
                data = await asyncio.to_thread(commands, host.store, actor, current_agent.catalog,
                    kind=request.query_params.get('kind', 'skills'), keyword=request.query_params.get('keyword', ''),
                    group=request.query_params.get('group', ''), page=request.query_params.get('page', 1))
                return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'no-store'})
            if action == 'skills':
                from .lighthouse_commands import skills
                data = await asyncio.to_thread(skills, host.store, actor)
                return JSONResponse({'ok': True, 'data': {'items': data}}, headers={'Cache-Control': 'no-store'})
            if action.startswith('skills/'):
                from .lighthouse_commands import read_skill
                from .lighthouse_shared_skills import SharedSkills
                name = action.split('/')[1]
                if request.method == 'DELETE':
                    data = await asyncio.to_thread(SharedSkills(host.store).remove, actor, name)
                else:
                    try:
                        offset = int(request.query_params.get('offset', 0))
                    except ValueError:
                        raise AssistantError('分页位置无效。') from None
                    data = await asyncio.to_thread(read_skill, host.store, actor, name, request.query_params.get('reference', ''), offset)
                return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'no-store'})
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
                from .lighthouse_pending import collect_pending
                current_agent = await ready_agent()
                data = await collect_pending(actor, str(request.query_params.get("q") or ""),
                    lambda operation: current_agent._invoke(actor, operation, request),
                    lambda kind, scopes: host.portal_bridge.cached(kind, scopes, actor["scopes"]))
                return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
            if action == "question-bank":
                from .lighthouse_ai import safe_data
                data = await host.portal_bridge.acall('question_bank', {'query': dict(request.query_params)})
                return JSONResponse({"ok": True, "data": safe_data(data)}, headers={"Cache-Control": "no-store"})
            if action == "work-orders":
                from .lighthouse_ai import safe_data
                data = await host.portal_bridge.acall('work_orders', {'query': dict(request.query_params)})
                return JSONResponse({"ok": True, "data": safe_data(data, list_limit=1000)}, headers={"Cache-Control": "no-store"})
            if action == "question-material":
                from .lighthouse_ai import safe_data
                from .lighthouse_sources import question_material_text
                source = await host.portal_bridge.acall('question_material', {'query': dict(request.query_params)})
                data = await asyncio.to_thread(question_material_text, host.store, source)
                return JSONResponse({"ok": True, "data": safe_data(data)}, headers={"Cache-Control": "no-store"})
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
                    existing = await asyncio.to_thread(current_agent.get_plan, actor, identity)
                    if existing.get('_planned'):
                        from .lighthouse_planned import amend_selection
                        data = await amend_selection(current_agent, actor, identity, payload, request)
                    else:
                        data = await asyncio.to_thread(current_agent.amend, actor, identity, payload)
                elif action.endswith("/confirm"):
                    data = await current_agent.confirm(actor, identity, payload, request)
                elif action.endswith("/retry"):
                    data = await current_agent.retry_notice(actor, identity, payload, request)
                elif action.endswith("/cancel"):
                    data = await asyncio.to_thread(current_agent.cancel, actor, identity)
                elif action.endswith("/options"):
                    data = await current_agent.field_options(actor, identity, str(request.query_params.get("field", "")), request)
                elif action.endswith("/cabinet-text-preview"):
                    data = await current_agent.preview_cabinet_text(actor, identity, payload, request)
                elif action.endswith("/repair-prefill"):
                    data = await current_agent.preview_repair(actor, identity, payload, request)
                elif action.endswith("/notice-prefill"):
                    data = await current_agent.preview_notice(actor, identity, payload, request)
                else:
                    plan = await asyncio.to_thread(current_agent.get_plan, actor, identity)
                    data = await current_agent.refresh(actor, plan, request)
                return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
            current = await asyncio.to_thread(get_service)
            if action == "conversation":
                if request.method == "GET":
                    current_streams = await ready_streams()
                    warm_current(current_streams, actor)
                    data = await current_streams.conversation(actor)
                    if agent is not None:
                        refreshed = False
                        for turn in data.get("turns", []):
                            if (turn.get("plan") or {}).get("status") in {"running", "submitted"}:
                                plan = await asyncio.to_thread(agent.get_plan, actor, turn["plan"]["id"])
                                await agent.refresh(actor, plan, request)
                                refreshed = True
                        if refreshed:
                            data = await current_streams.conversation(actor)
                    return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
                if request.method == "DELETE":
                    current_turns = (await asyncio.to_thread(current.conversation, actor)).get("turns", [])
                    if (agent is not None and any(owner == actor["id"] for owner, _plan_id in tuple(agent.executing))) or any((turn.get("plan") or {}).get("status") in {"running", "submitted"} for turn in current_turns):
                        raise AssistantError("业务操作仍在处理，请完成后再清空会话。", 409)
                if request.method == "PATCH":
                    fn, args = current.select_model, (actor, payload)
                else:
                    fn, args = (current.clear, (actor,)) if request.method == "DELETE" else (current.conversation, (actor,))
            else:
                fn, args = current.model_settings, (actor, payload if request.method == "PUT" else None)
            data = await asyncio.to_thread(fn, *args)
            if action == "conversation":
                current_streams = await ready_streams()
                warm_current(current_streams, actor)
                data = await current_streams.conversation(actor)
            elif action == "settings" and request.method == "PUT":
                warm_current(await ready_streams(), actor)
            # Conversation and configuration must not enter shared/browser caches.
            return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
        except AssistantError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=exc.status, headers={"Cache-Control": "no-store"})
        except Exception:
            # Do not log request bodies, vendor response bodies or decrypted credentials.
            return JSONResponse({"ok": False, "error": "助手暂时不可用，请稍后重试。"}, status_code=503, headers={"Cache-Control": "no-store"})

    for path, methods in (("appearance", ["GET", "PUT"]), ("conversation", ["GET", "DELETE", "PATCH"]), ("chat", ["POST"]), ("settings", ["GET", "PUT"]), ("question-bank", ["GET"]), ("question-material", ["GET"]), ("work-orders", ["GET"]), ('commands', ['GET']), ('skills', ['GET']), ('skills/install', ['POST']), ('skills/{skill_name}', ['GET', 'DELETE'])):
        for method in methods:
            app.add_api_route("/api/assistant/" + path, endpoint, methods=[method], name="lighthouse_" + path.replace("/", "_") + "_" + method.lower())
    for path, methods in (("messages", ["POST"]), ("history", ["GET"]), ("runs/{run_id}/stream", ["GET"]), ("runs/{run_id}/cancel", ["POST"]), ("agent", ["POST"]), ("pending", ["GET"]), ("capabilities", ["GET"]), ("files", ["POST"]), ("files/{file_id}", ["GET"]), ("plans/{plan_id}", ["GET", "PATCH"]), ("plans/{plan_id}/confirm", ["POST"]), ("plans/{plan_id}/cancel", ["POST"]), ("plans/{plan_id}/options", ["GET"]), ("plans/{plan_id}/cabinet-text-preview", ["POST"]), ("plans/{plan_id}/repair-prefill", ["POST"]), ("plans/{plan_id}/notice-prefill", ["POST"])):
        for method in methods:
            app.add_api_route("/api/assistant/" + path, endpoint, methods=[method], name="lighthouse_" + path.replace("/", "_") + "_" + method.lower())
    app.add_api_route("/api/assistant/plans/{plan_id}/retry", endpoint, methods=["POST"], name="lighthouse_plan_retry")
    async def disconnect():
        pending = list(agent.tasks) if agent is not None else []
        if streams is not None:
            pending += [task for task in streams.workers.values() if not task.done()]
        if preparation is not None and not preparation.done():
            pending.append(preparation)
        if streams is not None and getattr(streams.engine, 'warming', None):
            pending.extend(streams.engine.warming.values())
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if streams is not None and getattr(streams.engine, 'bridge', None):
            streams.engine.bridge.active.clear()
    return {'startup': startup, 'shutdown': shutdown, 'disconnect': disconnect, 'get_streams': get_streams,
            'recommend_notice_tags': recommend_notice_tags}

"""Native Feishu delivery APIs; every sender is derived from the portal session."""
import asyncio
import json
import threading
from urllib.parse import urlsplit
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.formparsers import FormParser, MultiPartParser, MultiPartException
from .message_delivery import DeliveryError, MessageDelivery, MessageDeliveryRequest, MAX_FILE, MAX_TOTAL


def install_message_delivery_routes(app, controller, runtime):
    delivery = None
    lock = threading.Lock()

    def get_delivery():
        nonlocal delivery
        with lock:
            if delivery is None:
                delivery = MessageDelivery(runtime.service)
            return delivery

    def owner_for(request):
        session = controller._current_session(request)
        if not session or session.get("is_guest") or str(session.get('role') or (session.get('user') or {}).get('role')).lower() == 'guest':
            raise DeliveryError("请使用正式账号登录后发送消息。", 401)
        owner = str(session.get("open_id") or (session.get("user") or {}).get("open_id") or "")
        if not owner or not runtime.auth_manager.session_scopes(session):
            raise DeliveryError("当前登录人没有业务访问权限。", 403)
        if request.method != 'GET':
            origin = request.headers.get('origin') or request.headers.get('referer') or ''
            if not origin or urlsplit(origin).netloc != request.url.netloc or urlsplit(origin).scheme != request.url.scheme:
                raise DeliveryError('请求来源无效。', 403)
        return session, owner

    def fail(exc):
        return JSONResponse({"ok": False, "error": str(exc) if isinstance(exc, DeliveryError) else "发送参数或人员目录暂不可用。"}, status_code=getattr(exc, "status", 400))

    @app.get("/api/message-delivery/recipients")
    async def recipients(request: Request):
        try:
            session, owner = owner_for(request)
            delivery = await asyncio.to_thread(get_delivery)
            result = await asyncio.to_thread(delivery.recipients, owner, request.query_params.get("q", "")[:120])
            return controller._json_ok(request, session, result)
        except Exception as exc:
            return fail(exc)

    @app.post("/api/message-delivery/send")
    async def send(request: Request):
        try:
            session, owner = owner_for(request)
            delivery = await asyncio.to_thread(get_delivery)
            files = []
            if "multipart/form-data" in request.headers.get("content-type", "") or "application/x-www-form-urlencoded" in request.headers.get("content-type", ""):
                total = 0
                multipart = 'multipart/form-data' in request.headers.get('content-type', '')
                limit = MAX_TOTAL + 262144 if multipart else 1048576
                async def bounded_stream():
                    nonlocal total
                    async for chunk in request.stream():
                        total += len(chunk)
                        if total > limit:
                            raise MultiPartException('发送内容超过大小限制。')
                        yield chunk
                if multipart:
                    form = await MultiPartParser(request.headers, bounded_stream(), max_files=10, max_fields=5).parse()
                else:
                    form = await FormParser(request.headers, bounded_stream()).parse()
                try:
                    payload = {"operation_id": form.get("operation_id"), "text": form.get("text", ""),
                               "recipient_ids": json.loads(form.get("recipient_ids", "[]")),
                               "retry_attempt": form.get("retry_attempt", 0)}
                    for upload in form.getlist("files"):
                        if not getattr(upload, "filename", None):
                            raise DeliveryError("附件格式无效。")
                        files.append((upload.filename, await upload.read(MAX_FILE + 1)))
                finally:
                    await form.close()
            else:
                payload = (await controller._read_model_request(request, MessageDeliveryRequest)).model_dump()
            result = await asyncio.to_thread(delivery.prepare, owner, payload, files)
            if result["status"] != "completed":
                if not controller._submit_background("FeishuDelivery", delivery.run, owner, result["delivery_id"]):
                    raise DeliveryError("发送服务暂忙，请继续原任务。", 503)
            response = controller._json_ok(request, session, result)
            response.status_code = 202
            return response
        except Exception as exc:
            return fail(exc)

    @app.get("/api/message-delivery/{delivery_id}")
    async def status(delivery_id: str, request: Request):
        try:
            session, owner = owner_for(request)
            delivery = await asyncio.to_thread(get_delivery)
            return controller._json_ok(request, session, await asyncio.to_thread(delivery.get, owner, delivery_id))
        except Exception as exc:
            return fail(exc)

    @app.post("/api/message-delivery/{delivery_id}/retry")
    async def retry(delivery_id: str, request: Request):
        try:
            session, owner = owner_for(request)
            delivery = await asyncio.to_thread(get_delivery)
            result = await asyncio.to_thread(delivery.get, owner, delivery_id)
            if result["status"] != "completed" and not controller._submit_background("FeishuDelivery", delivery.run, owner, delivery_id):
                raise DeliveryError("发送服务暂忙，请稍后继续原任务。", 503)
            return controller._json_ok(request, session, {**result, "status": "queued" if result["status"] != "completed" else "completed"})
        except Exception as exc:
            return fail(exc)

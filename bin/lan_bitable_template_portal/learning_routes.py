"""Authenticated learning routes; importing this module performs no service I/O."""
import asyncio
import json
import logging
import threading
from contextlib import ExitStack
from urllib.parse import quote, urlencode, urlsplit
from uuid import NAMESPACE_URL, uuid5

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse, Response
try:
    from python_multipart import FormParser
    from python_multipart.exceptions import MultipartParseError
    from python_multipart.multipart import Field, File, parse_options_header
except ModuleNotFoundError as exc:
    if exc.name != "python_multipart":
        raise
    from multipart import FormParser
    from multipart.exceptions import MultipartParseError
    from multipart.multipart import Field, File, parse_options_header


SCOPES = frozenset("ABCDEH")
MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_FILE_BYTES = 20 * 1024 * 1024
MAX_TOTAL_BYTES = 100 * 1024 * 1024
MAX_FILES = 10

ROUTES = {
    "bootstrap": {"GET": "bootstrap"},
    "papers": {"GET": "papers.list"},
    "people": {"GET": "people"},
    "papers/claim": {"POST": "paper.claim"},
    "papers/{id}": {"GET": "paper.get", "DELETE": "paper.delete"},
    "papers/{id}/answer": {"POST": "paper.answer"},
    "papers/{id}/reveal": {"POST": "paper.reveal"},
    "papers/{id}/notes": {"POST": "paper.notes"},
    "history": {"GET": "history"},
    "review": {"GET": "review"},
    "profile": {"GET": "profile"},
    "issues": {"GET": "issues.list", "POST": "issue.create"},
    "issues/{id}": {"PATCH": "issue.update"},
    "questions": {"GET": "questions.list", "POST": "question.save"},
    "questions/{id}": {"GET": "question.get", "PUT": "question.save"},
    "questions/{id}/status": {"POST": "question.status"},
    "questions/{id}/copy": {"POST": "question.copy"},
    "attachments": {"POST": "attachments"},
    "attachments/{id}": {"GET": "attachment", "DELETE": "attachment.delete"},
    "settings": {"GET": "settings.get", "PUT": "settings.save"},
    "refresh": {"POST": "refresh"},
    "publish": {"POST": "publish"},
    "export": {"GET": "export"},
    "import": {"POST": "import"},
}


def install_learning_routes(app, controller, runtime):
    from .learning import LearningError, LearningService
    from .portal_service import BUILDING_OPEN_ID_MAP

    service = None
    service_lock = threading.Lock()

    def send_message(scope, message, identity):
        from clipflow_backend.runtime_helpers import send_text_to_open_ids_guarded

        if scope not in SCOPES or not BUILDING_OPEN_ID_MAP.get(scope):
            raise LearningError("通知接收楼栋无效。", 400)
        return send_text_to_open_ids_guarded(
            message, [BUILDING_OPEN_ID_MAP[scope]], message_uuid=str(uuid5(NAMESPACE_URL, identity)),
        )

    def get_service():
        nonlocal service
        with service_lock:
            if service is None:
                service = LearningService(send_message=send_message,
                                          get_people=lambda: runtime.service.signature_management.directory(refresh=True),
                                          get_portal_url=lambda: runtime.service._critical_guard_public_base_url())
                controller._learning = service
                runtime.learning_service = service
        return service

    async def startup():
        await asyncio.to_thread(lambda: get_service().start())

    async def shutdown():
        if service is not None:
            await asyncio.to_thread(service.stop)

    app.add_event_handler("startup", startup)
    app.add_event_handler("shutdown", shutdown)

    def identity(request):
        session = controller._current_session(request)
        if not session:
            return None, None
        user = session.get("user") or {}
        open_id = str(session.get("open_id") or user.get("open_id") or "").strip()
        if not open_id:
            raise LearningError("登录身份不完整，请重新登录。", 401)
        role = str(session.get("role") or user.get("role") or "").strip().lower()
        if session.get("is_guest") or role == "guest":
            raise LearningError("游客无法使用画像学练，请先登录。", 403)
        admin = bool(runtime.auth_manager.is_admin(session))
        scope = next((s for s in SCOPES if open_id == BUILDING_OPEN_ID_MAP.get(s)), "")
        if not admin and not scope:
            raise LearningError("仅 A、B、C、D、E、H 楼值班账号和管理员可使用画像学练。", 403)
        return session, {
            "id": open_id,
            "name": str(user.get("name") or session.get("name") or ""),
            "is_admin": admin,
            "scope": scope,
            "can_answer": True,
        }

    def check_scope(values, actor):
        if "scope" not in values:
            return
        scope = values["scope"]
        if scope == "":
            values["scope"] = "" if actor["is_admin"] else actor["scope"]
            return
        if not isinstance(scope, str) or scope not in SCOPES:
            raise LearningError("楼栋须为 A、B、C、D、E 或 H。", 400)

    def check_origin(request):
        # The portal uses HttpOnly/SameSite=Lax cookies, without CSRF tokens.
        # Also reject cross-origin writes, including same-site sibling origins.
        if request.headers.get("sec-fetch-site", "").lower() == "cross-site":
            raise LearningError("不允许跨来源提交请求，请从门户页面操作。", 403)
        source = request.headers.get("origin") or request.headers.get("referer")
        if source:
            def origin(value):
                parsed = urlsplit(value)
                if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                    raise ValueError("请求来源无效。")
                return parsed.scheme, parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)

            try:
                matches = origin(source) == origin(controller._request_base_url(request))
            except ValueError:
                matches = False
            if not matches:
                raise LearningError("不允许跨来源提交请求，请从门户页面操作。", 403)

    async def read_json(request):
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > MAX_JSON_BYTES:
                raise LearningError("提交的 JSON 内容不能超过 2 MiB。", 413)
            body.extend(chunk)
        try:
            payload = await asyncio.to_thread(json.loads, body or b"{}")
        except (ValueError, UnicodeError, RecursionError):
            raise LearningError("提交的 JSON 格式无效。", 400) from None
        if not isinstance(payload, dict):
            raise LearningError("提交的 JSON 内容必须是对象。", 400)
        return payload

    async def read_files(request):
        files, fields = [], {}
        total = count = field_count = 0
        finished = False
        cleanup = ExitStack()

        class LimitedFile(File):
            def __init__(self, *args, **kwargs):
                nonlocal count
                count += 1
                if count > MAX_FILES:
                    raise LearningError("每次最多上传 10 个文件。", 413)
                super().__init__(*args, **kwargs)
                cleanup.callback(self.close)

            def on_data(self, data):
                nonlocal total
                total += len(data)
                if self.size + len(data) > MAX_FILE_BYTES or total > MAX_TOTAL_BYTES:
                    raise LearningError("单个文件不能超过 20 MiB，文件合计不能超过 100 MiB。", 413)
                return super().on_data(data)

        class LimitedField(Field):
            def __init__(self, *args, **kwargs):
                nonlocal field_count
                field_count += 1
                if field_count > 20:
                    raise LearningError("上传表单字段过多，最多允许 20 个。", 413)
                super().__init__(*args, **kwargs)
                self.bytes_read = 0

            def on_data(self, data):
                self.bytes_read += len(data)
                if self.bytes_read > MAX_JSON_BYTES:
                    raise LearningError("单个表单字段不能超过 2 MiB。", 413)
                return super().on_data(data)

        def on_file(upload):
            if not upload.file_name:
                raise LearningError("上传文件必须包含文件名。", 400)
            upload.file_object.seek(0)
            files.append((upload.file_name.decode("utf-8", "replace"), upload.file_object.read()))

        def on_field(field):
            name = (field.field_name or b"").decode("utf-8", "replace")
            value = (field.value or b"").decode("utf-8", "replace")
            if name in fields and fields[name] != value:
                raise LearningError("重复提交的表单字段内容不一致。", 400)
            fields[name] = value

        def on_end():
            nonlocal finished
            finished = True

        try:
            content_type, options = parse_options_header(request.headers.get("content-type", ""))
            if content_type != b"multipart/form-data" or not options.get(b"boundary"):
                raise LearningError("请使用文件上传表单提交附件。", 400)
            parser = FormParser(
                "multipart/form-data", on_field, on_file, on_end,
                boundary=options[b"boundary"], FileClass=LimitedFile, FieldClass=LimitedField,
            )
            received = 0
            async for chunk in request.stream():
                received += len(chunk)
                if received > MAX_TOTAL_BYTES + MAX_JSON_BYTES:
                    raise LearningError("上传请求过大，请减少附件或表单内容。", 413)
                await asyncio.to_thread(parser.write, chunk)
            await asyncio.to_thread(parser.finalize)
            if not finished or not files:
                raise LearningError("上传内容不完整或未包含文件。", 400)
            return files, fields
        except LearningError:
            raise
        except (MultipartParseError, ValueError):
            raise LearningError("文件上传表单格式无效。", 400) from None
        finally:
            await asyncio.to_thread(cleanup.close)

    def error_response(exc):
        if isinstance(exc, LearningError):
            status = getattr(exc, "status", 400)
            if not isinstance(status, int) or not 400 <= status <= 599:
                status = 400
            return JSONResponse({"ok": False, "error": str(getattr(exc, "message", str(exc)))}, status_code=status)
        logging.getLogger(__name__).exception("Learning request failed")
        return JSONResponse({"ok": False, "error": "画像学练服务暂时不可用，请稍后重试。"}, status_code=500)

    async def endpoint(request: Request):
        try:
            session, actor = await asyncio.to_thread(identity, request)
            if session is None:
                return controller._auth_required_response()
            path = request.scope["route"].path.removeprefix("/api/learning/")
            action = ROUTES[path][request.method]
            query = dict(request.query_params)
            for scope in request.query_params.getlist("scope"):
                check_scope({"scope": scope}, actor)
            check_scope(query, actor)
            if request.method not in {"GET", "HEAD"}:
                await asyncio.to_thread(check_origin, request)
            payload = {}
            if action == "attachments":
                files, fields = await read_files(request)
                check_scope(fields, actor)
                query = {**fields, **query}
            elif request.method in {"POST", "PUT", "PATCH", "DELETE"}:
                payload = await read_json(request)
                check_scope(payload, actor)
            if "id" in request.path_params:
                payload["id"] = query["id"] = request.path_params["id"]
            current = await asyncio.to_thread(get_service)
            if action == "bootstrap":
                scope = query.get("scope", "" if actor["is_admin"] else actor["scope"])
                data = await asyncio.to_thread(current.bootstrap, scope, actor)
            elif action == "attachments":
                data = await asyncio.to_thread(current.add_attachments, files, query, actor)
            elif action == "attachment":
                file_path, name, mime = await asyncio.to_thread(current.attachment, query["id"], actor)
                disposition = "inline" if mime in {"image/png", "image/jpeg", "image/webp", "application/pdf"} else "attachment"
                return FileResponse(file_path, filename=name, media_type=mime, content_disposition_type=disposition, headers={
                    "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff",
                    "Content-Security-Policy": "sandbox; default-src 'none'; frame-ancestors 'self'",
                })
            elif action == "export":
                content, name, mime = await asyncio.to_thread(current.export, query.get("kind", ""), query, actor)
                return Response(content, media_type=mime, headers={
                    "Content-Disposition": "attachment; filename*=UTF-8''" + quote(name, safe=""),
                    "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff",
                })
            else:
                data = await asyncio.to_thread(current.dispatch, action, payload, actor, query)
            return await asyncio.to_thread(controller._json_ok, request, session, data)
        except Exception as exc:
            return error_response(exc)

    async def page(request: Request):
        try:
            session, actor = await asyncio.to_thread(identity, request)
            if session is None:
                target = request.url.path + ("?" + request.url.query if request.url.query else "")
                return Response(status_code=302, headers={"Location": "/api/auth/login?" + urlencode({"next": target})})
            for scope in request.query_params.getlist("scope"):
                check_scope({"scope": scope}, actor)

            def static_page():
                from .server import portal_index_file

                return controller._static_file_response(request, portal_index_file(), html=True)

            return await asyncio.to_thread(static_page)
        except Exception as exc:
            return error_response(exc)

    for path, methods in ROUTES.items():
        app.add_api_route("/api/learning/" + path, endpoint, methods=list(methods), name="learning_" + path.replace("/", "_"))
    for path in ("/learning", "/learning/"):
        app.add_api_route(path, page, methods=["GET"], include_in_schema=False)

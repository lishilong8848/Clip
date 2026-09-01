from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import os
import re
import time
import uuid
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .store import RelayError, RelayStore


COOKIE_NAME = "wo_session"
LOGGER = logging.getLogger("public_polling_relay")
RELAY_PROTOCOL_VERSION = 1
SINGLE_AUTHORITY_KEY = "EA118"
MAX_JSON_BYTES = 1 * 1024 * 1024
MAX_PHOTO_BYTES = 8 * 1024 * 1024
MAX_PHOTO_PIXELS = 40_000_000
MAX_PHOTO_DIMENSION = 12_000
ID_RE = re.compile(r"^[A-Za-z0-9_-]{12,128}$")
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
COMMAND_TYPES = {
    "activate",
    "release",
    "confirm",
    "rollback",
    "retry_attachment",
}
GROUP_STATES = {"active", "upload_pending", "completed", "cancelled", "stopped"}


@dataclass(frozen=True)
class RelaySettings:
    db_path: Path
    upload_root: Path
    public_base_url: str = ""
    secure_cookie: bool = True
    session_ttl_seconds: int = 12 * 60 * 60
    upload_reservation_ttl_seconds: int = 15 * 60
    upload_ttl_seconds: int = 30 * 60
    command_ttl_seconds: int = 60
    authority_lease_seconds: int = 45
    command_lease_seconds: int = 10 * 60

    @classmethod
    def from_env(cls) -> "RelaySettings":
        base = Path(__file__).resolve().parent / "runtime"
        secure_cookie = str(
            os.environ.get("PUBLIC_POLLING_RELAY_SECURE_COOKIE") or "1"
        ).lower() not in {"0", "false", "no"}
        public_base_url = str(
            os.environ.get("PUBLIC_POLLING_RELAY_PUBLIC_URL") or ""
        ).strip().rstrip("/")
        parsed_public_url = urlsplit(public_base_url)
        if (
            not public_base_url
            or parsed_public_url.scheme not in ({"https"} if secure_cookie else {"https", "http"})
            or not parsed_public_url.netloc
            or parsed_public_url.username
            or parsed_public_url.password
            or parsed_public_url.query
            or parsed_public_url.fragment
            or parsed_public_url.path not in {"", "/"}
        ):
            raise RuntimeError("必须配置无账号、无路径参数的公网 HTTPS 根地址。")
        return cls(
            db_path=Path(os.environ.get("PUBLIC_POLLING_RELAY_DB") or base / "relay.sqlite3"),
            upload_root=Path(os.environ.get("PUBLIC_POLLING_RELAY_UPLOAD_ROOT") or base / "uploads"),
            public_base_url=public_base_url,
            secure_cookie=secure_cookie,
        )


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LinkExchangeRequest(StrictModel):
    link_id: str = Field(min_length=12, max_length=128)
    secret: str = Field(min_length=24, max_length=512)
    device_nonce: str = Field(default="", max_length=128)


class CommandRequest(StrictModel):
    type: str = Field(min_length=1, max_length=32)
    expected_version: int = Field(ge=0)
    run_index: int | None = Field(default=None, ge=1, le=6)
    step_key: str | None = Field(default=None, max_length=128)
    upload_id: str | None = Field(default=None, max_length=128)


class UploadRequest(StrictModel):
    step_key: str = Field(min_length=1, max_length=128)
    expected_version: int = Field(ge=0)
    file_name: str = Field(min_length=1, max_length=255)
    content_type: str = Field(min_length=1, max_length=100)
    size: int = Field(gt=0, le=MAX_PHOTO_BYTES)
    sha256: str = Field(min_length=64, max_length=64)


class LeaseRequest(StrictModel):
    instance_id: str = Field(min_length=8, max_length=128)
    previous_epoch: int | None = Field(default=None, ge=0)


class RoleLink(StrictModel):
    link_id: str = Field(min_length=12, max_length=128)
    secret_sha256: str = Field(min_length=64, max_length=64)
    generation: int = Field(default=1, ge=1)
    assigned_name: str = Field(default="", max_length=100)


class GroupRegisterRequest(StrictModel):
    protocol_version: int = Field(ge=RELAY_PROTOCOL_VERSION, le=RELAY_PROTOCOL_VERSION)
    registration_version: int = Field(ge=1)
    state: str = Field(default="active", max_length=32)
    authority_version: int = Field(ge=0)
    projection_revision: int = Field(ge=0)
    projection: dict[str, Any] = Field(default_factory=dict)
    links: dict[str, RoleLink]


class ProjectionRequest(StrictModel):
    state: str = Field(max_length=32)
    authority_version: int = Field(ge=0)
    projection_revision: int = Field(ge=0)
    projection: dict[str, Any]


class CommandAckRequest(StrictModel):
    outcome: str = Field(min_length=1, max_length=32)
    authority_version: int = Field(ge=0)
    projection_revision: int = Field(ge=0)
    result: dict[str, Any] = Field(default_factory=dict)
    error_code: str = Field(default="", max_length=100)
    error: str = Field(default="", max_length=1000)
    projection: dict[str, Any] | None = None


class CancelRequest(StrictModel):
    reason: str = Field(default="", max_length=1000)


def _clean_text(value: Any, limit: int) -> str:
    return str(value or "")[:limit]


def _sanitize_photo(value: Any) -> dict[str, Any]:
    photo = value if isinstance(value, dict) else {}
    photo_id = _clean_text(
        photo.get("public_photo_id") or photo.get("relay_upload_id") or photo.get("photo_id"),
        128,
    )
    result = {
        "photo_id": photo_id,
        "name": _clean_text(photo.get("name") or photo.get("file_name"), 255),
        "content_type": _clean_text(photo.get("content_type") or photo.get("mime_type"), 100),
        "size": max(0, int(photo.get("size") or 0)),
        "sha256": _clean_text(photo.get("sha256"), 64),
        "status": _clean_text(photo.get("status") or "authority_attached", 32),
    }
    if photo_id and ID_RE.fullmatch(photo_id):
        result["preview_url"] = f"/api/v1/work-orders/photos/{photo_id}"
    return result


def sanitize_projection(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    result: dict[str, Any] = {}
    text_limits = {"title": 500, "sop_name": 300, "last_error": 1000, "authority_server_time": 80}
    for key, limit in text_limits.items():
        if key in source:
            result[key] = _clean_text(source.get(key), limit)
    for key in ("current_run_index", "current_index", "total_steps"):
        if key in source:
            result[key] = max(0, int(source.get(key) or 0))
    for key in ("can_release_selection", "can_rollback_previous"):
        if key in source:
            result[key] = bool(source.get(key))
    state = _clean_text(source.get("state") or "active", 32)
    result["state"] = state if state in GROUP_STATES else "active"
    work_orders: list[dict[str, Any]] = []
    for item in list(source.get("work_orders") or [])[:6]:
        if not isinstance(item, dict):
            continue
        work_orders.append(
            {
                "run_index": max(1, int(item.get("run_index") or 1)),
                "from_unit": _clean_text(item.get("from_unit"), 20),
                "to_unit": _clean_text(item.get("to_unit"), 20),
                "label": _clean_text(item.get("label"), 80),
                "step_count": max(0, int(item.get("step_count") or 0)),
                "completed_steps": max(0, int(item.get("completed_steps") or 0)),
                "state": _clean_text(item.get("state"), 32),
                "selectable": bool(item.get("selectable")),
            }
        )
    result["work_orders"] = work_orders
    steps: list[dict[str, Any]] = []
    for item in list(source.get("steps") or [])[:3]:
        if not isinstance(item, dict):
            continue
        step = {
            "step_key": _clean_text(item.get("step_key"), 128),
            "global_index": max(0, int(item.get("global_index") or 0)),
            "run_index": max(1, int(item.get("run_index") or 1)),
            "run_count": max(1, int(item.get("run_count") or 1)),
            "run_label": _clean_text(item.get("run_label"), 80),
            "step_index": max(1, int(item.get("step_index") or 1)),
            "step_count": max(1, int(item.get("step_count") or 1)),
            "content": _clean_text(item.get("content"), 5000),
            "position": _clean_text(item.get("position"), 20),
            "operator_required": bool(item.get("operator_required")),
            "reviewer_required": bool(item.get("reviewer_required")),
            "operator_confirmed": bool(item.get("operator_confirmed")),
            "reviewer_confirmed": bool(item.get("reviewer_confirmed")),
            "time_limit_seconds": max(0, int(item.get("time_limit_seconds") or 0)),
            "timer_started": bool(item.get("timer_started")),
            "confirm_available_at": max(0.0, float(item.get("confirm_available_at") or 0)),
            "remaining_seconds": max(0, int(item.get("remaining_seconds") or 0)),
            "photos": [_sanitize_photo(photo) for photo in list(item.get("photos") or [])[:5]],
        }
        steps.append(step)
    result["steps"] = steps
    completion = source.get("completion") if isinstance(source.get("completion"), dict) else {}
    result["completion"] = {
        "steps_completed": bool(completion.get("steps_completed")),
        "artifact_state": _clean_text(completion.get("artifact_state") or "not_started", 40),
        "remote_verified": bool(completion.get("remote_verified")),
        "error": _clean_text(completion.get("error"), 1000),
    }
    return result


def sanitize_projection_bundle(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    role_keyed = any(role in source for role in ("operator", "reviewer"))
    if not role_keyed:
        return sanitize_projection(source)
    if not all(isinstance(source.get(role), dict) for role in ("operator", "reviewer")):
        raise RelayError(422, "role_projections_required", "必须同时提供操作人和审核人投影。")
    result = {
        "operator": sanitize_projection(source["operator"]),
        "reviewer": sanitize_projection(source["reviewer"]),
    }
    if result["operator"]["state"] != result["reviewer"]["state"]:
        raise RelayError(422, "role_projection_state_conflict", "两种角色投影的工单状态不一致。")
    return result


def _set_projection_state(projection: dict[str, Any], state: str) -> None:
    if all(isinstance(projection.get(role), dict) for role in ("operator", "reviewer")):
        projection["operator"]["state"] = state
        projection["reviewer"]["state"] = state
    else:
        projection["state"] = state


def _validated(model: type[BaseModel], value: Any) -> BaseModel:
    try:
        return model.model_validate(value)
    except ValidationError as exc:
        first = exc.errors()[0] if exc.errors() else {}
        location = ".".join(str(item) for item in first.get("loc") or [])
        message = str(first.get("msg") or "请求参数无效。")
        raise RelayError(422, "validation_error", f"{location}: {message}".strip(": ")) from exc


async def _json_body(request: Request, *, limit: int = MAX_JSON_BYTES) -> dict[str, Any]:
    try:
        declared = int(request.headers.get("content-length") or 0)
    except ValueError as exc:
        raise RelayError(400, "invalid_content_length", "Content-Length 无效。") from exc
    if declared > limit:
        raise RelayError(413, "request_too_large", "请求内容过大。")
    body = await request.body()
    if len(body) > limit:
        raise RelayError(413, "request_too_large", "请求内容过大。")
    try:
        value = json.loads(body or b"{}")
    except (UnicodeDecodeError, ValueError) as exc:
        raise RelayError(400, "invalid_json", "请求 JSON 无效。") from exc
    if not isinstance(value, dict):
        raise RelayError(422, "invalid_json_object", "请求正文必须是 JSON 对象。")
    return value


def _detect_image_type(path: Path) -> str:
    with path.open("rb") as stream:
        header = stream.read(16)
    if header.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if header.startswith(b"RIFF") and header[8:12] == b"WEBP":
        return "image/webp"
    raise RelayError(415, "unsupported_image", "仅支持 JPEG、PNG 或 WebP 图片。")


def _verify_image_dimensions(path: Path) -> None:
    try:
        from PIL import Image

        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(path) as image:
                width, height = image.size
                if (
                    width <= 0
                    or height <= 0
                    or width > MAX_PHOTO_DIMENSION
                    or height > MAX_PHOTO_DIMENSION
                    or width * height > MAX_PHOTO_PIXELS
                ):
                    raise RelayError(413, "photo_dimensions_limit", "操作照片像素或尺寸超过限制。")
                image.verify()
    except RelayError:
        raise
    except Exception as exc:
        raise RelayError(415, "invalid_image", "操作照片无法解码。") from exc


def _api_ok(data: Any, *, status_code: int = 200) -> JSONResponse:
    return JSONResponse({"ok": True, "data": data}, status_code=status_code)


def create_app(settings: RelaySettings) -> FastAPI:
    store = RelayStore(settings.db_path, settings.upload_root)
    store.cleanup()

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI):
        async def cleanup_loop() -> None:
            while True:
                await asyncio.sleep(300)
                await asyncio.to_thread(store.cleanup)

        task = asyncio.create_task(cleanup_loop())
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    app = FastAPI(
        title="ClipFlow Public Polling Relay",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.store = store

    @app.exception_handler(RelayError)
    async def relay_error_handler(_request: Request, exc: RelayError) -> JSONResponse:
        if exc.status_code >= 500 or exc.code == "authority_lease_busy":
            LOGGER.warning("relay request failed: code=%s message=%s", exc.code, exc.message)
        headers = {"Retry-After": "5"} if exc.status_code == 503 else None
        return JSONResponse(
            {"ok": False, "error": exc.message, "error_code": exc.code},
            status_code=exc.status_code,
            headers=headers,
        )

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Permissions-Policy", "camera=(self)")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; img-src 'self' blob: data:; style-src 'self' 'unsafe-inline'; "
            "script-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'",
        )
        if settings.secure_cookie:
            response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        session_token = str(request.cookies.get(COOKIE_NAME) or "")
        if (
            session_token
            and response.status_code < 400
            and request.url.path != "/api/v1/link-sessions/logout"
        ):
            response.set_cookie(
                COOKIE_NAME,
                session_token,
                max_age=settings.session_ttl_seconds,
                secure=settings.secure_cookie,
                httponly=True,
                samesite="strict",
                path="/",
            )
        return response

    async def session_for(request: Request, *, csrf: bool = False) -> dict[str, Any]:
        token = str(request.cookies.get(COOKIE_NAME) or "")
        if not token:
            raise RelayError(401, "session_required", "请从操作人或审核人链接进入工单。")
        session = store.authenticate_session(
            token,
            ttl_seconds=settings.session_ttl_seconds,
        )
        if csrf:
            store.verify_csrf(session, str(request.headers.get("x-csrf-token") or ""))
        return session

    async def internal_for(request: Request, *, fence: bool) -> tuple[str, bytes, dict[str, Any]]:
        body = await request.body()
        if len(body) > MAX_JSON_BYTES:
            raise RelayError(413, "request_too_large", "内部请求内容过大。")
        lease: dict[str, Any] = {}
        if fence:
            lease = store.verify_fencing_token(
                SINGLE_AUTHORITY_KEY,
                str(request.headers.get("x-relay-fencing-token") or ""),
            )
        return SINGLE_AUTHORITY_KEY, body, lease

    @app.get("/api/v1/health")
    async def health() -> JSONResponse:
        readiness = await asyncio.to_thread(store.readiness)
        payload = {
            "ok": True,
            "service": "public_polling_relay",
            "protocol_version": RELAY_PROTOCOL_VERSION,
            "time": time.time(),
            **readiness,
        }
        return JSONResponse(payload, status_code=200 if readiness["ready"] else 503)

    @app.get("/polling-work-order")
    async def polling_page() -> HTMLResponse:
        from .frontend import render_polling_work_order_page

        return HTMLResponse(render_polling_work_order_page(), headers={"Cache-Control": "no-store"})

    @app.get("/polling-work-order/steps")
    async def polling_steps_page() -> HTMLResponse:
        from .frontend import render_polling_work_order_steps_page

        return HTMLResponse(render_polling_work_order_steps_page(), headers={"Cache-Control": "no-store"})

    @app.post("/api/v1/link-sessions/exchange")
    async def exchange_link(request: Request) -> JSONResponse:
        payload = _validated(LinkExchangeRequest, await _json_body(request))
        assert isinstance(payload, LinkExchangeRequest)
        if not ID_RE.fullmatch(payload.link_id):
            raise RelayError(422, "invalid_link_id", "角色链接标识无效。")
        exchanged = store.exchange_link(
            link_id=payload.link_id,
            secret=payload.secret,
            session_ttl_seconds=settings.session_ttl_seconds,
        )
        session = store.authenticate_session(
            exchanged["session_token"],
            ttl_seconds=settings.session_ttl_seconds,
        )
        response = _api_ok(
            {
                "csrf_token": exchanged["csrf_token"],
                "expires_at": exchanged["expires_at"],
                "snapshot": store.public_snapshot(session),
            }
        )
        response.set_cookie(
            COOKIE_NAME,
            exchanged["session_token"],
            max_age=settings.session_ttl_seconds,
            secure=settings.secure_cookie,
            httponly=True,
            samesite="strict",
            path="/",
        )
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.post("/api/v1/link-sessions/logout")
    async def logout(request: Request) -> JSONResponse:
        session = await session_for(request, csrf=True)
        store.revoke_session(str(session["session_hash"]))
        response = _api_ok({"logged_out": True})
        response.delete_cookie(COOKIE_NAME, path="/", secure=settings.secure_cookie, samesite="strict")
        return response

    @app.get("/api/v1/work-orders/session")
    async def public_session(request: Request) -> Response:
        session = await session_for(request)
        try:
            after_revision = int(request.query_params.get("after_revision") or -1)
            wait_seconds = min(30, max(0, int(request.query_params.get("wait_seconds") or 0)))
        except ValueError as exc:
            raise RelayError(422, "invalid_poll_parameters", "轮询参数无效。") from exc
        deadline = time.monotonic() + wait_seconds
        while True:
            snapshot = store.public_snapshot(session)
            if int(snapshot.get("projection_revision") or 0) != after_revision:
                response = _api_ok(snapshot)
                response.headers["Cache-Control"] = "no-store"
                response.headers["ETag"] = f'"{snapshot.get("projection_revision", 0)}"'
                return response
            if time.monotonic() >= deadline:
                return Response(status_code=204, headers={"Cache-Control": "no-store"})
            await asyncio.sleep(0.5)
            session = store.authenticate_session(
                str(request.cookies.get(COOKIE_NAME) or ""), touch=False
            )

    @app.post("/api/v1/work-orders/commands")
    async def public_command(request: Request) -> JSONResponse:
        session = await session_for(request, csrf=True)
        payload = _validated(CommandRequest, await _json_body(request))
        assert isinstance(payload, CommandRequest)
        if payload.type not in COMMAND_TYPES:
            raise RelayError(422, "invalid_command", "工单命令类型无效。")
        command_payload: dict[str, Any] = {}
        if payload.type in {"activate", "release"}:
            if payload.run_index is None:
                raise RelayError(422, "run_index_required", "请选择工单。")
            command_payload["run_index"] = payload.run_index
        elif payload.type in {"confirm", "rollback"}:
            if not str(payload.step_key or "").strip():
                raise RelayError(422, "step_key_required", "当前步骤无效。")
            command_payload["step_key"] = str(payload.step_key)
        idempotency_key = str(request.headers.get("idempotency-key") or "").strip()
        if not ID_RE.fullmatch(idempotency_key):
            raise RelayError(422, "invalid_idempotency_key", "必须提供有效的 Idempotency-Key。")
        command, _created = store.enqueue_command(
            session=session,
            idempotency_key=idempotency_key,
            kind=payload.type,
            payload=command_payload,
            expected_version=payload.expected_version,
            ttl_seconds=settings.command_ttl_seconds,
        )
        status_code = 202 if command["status"] in {"pending", "leased"} else 200
        return _api_ok(command, status_code=status_code)

    @app.get("/api/v1/work-orders/commands/{command_id}")
    async def public_command_status(command_id: str, request: Request) -> JSONResponse:
        session = await session_for(request)
        if not ID_RE.fullmatch(command_id):
            raise RelayError(404, "command_not_found", "命令不存在。")
        return _api_ok(store.command_for_session(session, command_id))

    @app.post("/api/v1/work-orders/uploads")
    async def initialize_upload(request: Request) -> JSONResponse:
        session = await session_for(request, csrf=True)
        payload = _validated(UploadRequest, await _json_body(request))
        assert isinstance(payload, UploadRequest)
        digest = payload.sha256.lower()
        if not SHA256_RE.fullmatch(digest):
            raise RelayError(422, "invalid_sha256", "照片 SHA-256 无效。")
        content_type = payload.content_type.lower().split(";", 1)[0].strip()
        if content_type == "image/jpg":
            content_type = "image/jpeg"
        if content_type not in {"image/jpeg", "image/png", "image/webp"}:
            raise RelayError(415, "unsupported_image", "仅支持 JPEG、PNG 或 WebP 图片。")
        return _api_ok(
            store.create_upload(
                session=session,
                step_key=payload.step_key,
                expected_version=payload.expected_version,
                file_name=Path(payload.file_name).name,
                content_type=content_type,
                size=payload.size,
                digest=digest,
                ttl_seconds=settings.upload_reservation_ttl_seconds,
            ),
            status_code=201,
        )

    @app.put("/api/v1/work-orders/uploads/{upload_id}/content")
    async def upload_content(upload_id: str, request: Request) -> JSONResponse:
        session = await session_for(request, csrf=True)
        store.require_public_write_online(session)
        if not ID_RE.fullmatch(upload_id):
            raise RelayError(404, "upload_not_found", "照片暂存记录不存在。")
        upload = store.upload_for_session(session, upload_id)
        if str(upload["status"]) in {"uploaded", "relay_ready", "authority_attached"}:
            return _api_ok(store._upload_public(upload))
        content_type = str(request.headers.get("content-type") or "").lower().split(";", 1)[0].strip()
        if content_type == "image/jpg":
            content_type = "image/jpeg"
        if content_type != str(upload["content_type"]):
            raise RelayError(415, "content_type_mismatch", "照片类型与初始化请求不一致。")
        header_digest = str(request.headers.get("x-content-sha256") or "").strip().lower()
        if not hmac.compare_digest(header_digest, str(upload["declared_sha256"])):
            raise RelayError(409, "upload_checksum_mismatch", "照片校验值与初始化请求不一致。")
        directory = (settings.upload_root.resolve() / upload_id[:2]).resolve()
        if not directory.is_relative_to(settings.upload_root.resolve()):
            raise RelayError(400, "invalid_upload_path", "照片暂存路径无效。")
        directory.mkdir(parents=True, exist_ok=True)
        target = (directory / f"{upload_id}.blob").resolve()
        temporary = (directory / f"{upload_id}.{uuid.uuid4().hex}.part").resolve()
        digest = hashlib.sha256()
        size = 0
        persisted = False
        store.begin_upload_content(upload_id, str(session["session_hash"]))
        try:
            with temporary.open("xb") as output:
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > MAX_PHOTO_BYTES or size > int(upload["declared_size"]):
                        raise RelayError(413, "photo_too_large", "单张照片不能超过 8MB。")
                    output.write(chunk)
                    digest.update(chunk)
            actual_digest = digest.hexdigest()
            if size != int(upload["declared_size"]) or not hmac.compare_digest(
                actual_digest, str(upload["declared_sha256"])
            ):
                raise RelayError(409, "upload_checksum_mismatch", "照片大小或 SHA-256 不一致。")
            detected_type = _detect_image_type(temporary)
            if detected_type != content_type:
                raise RelayError(415, "image_signature_mismatch", "照片实际格式与声明格式不一致。")
            _verify_image_dimensions(temporary)
            os.replace(temporary, target)
            updated = store.mark_upload_content(
                upload_id=upload_id,
                session_hash=str(session["session_hash"]),
                path=target,
                actual_size=size,
                actual_sha256=actual_digest,
            )
            persisted = True
            return _api_ok(store._upload_public(updated))
        except Exception:
            store.reset_upload_content(upload_id, str(session["session_hash"]))
            if not persisted and target.is_file():
                try:
                    target.unlink()
                except OSError:
                    pass
            raise
        finally:
            if temporary.is_file():
                try:
                    temporary.unlink()
                except OSError:
                    pass

    @app.post("/api/v1/work-orders/uploads/{upload_id}/complete")
    async def complete_upload(upload_id: str, request: Request) -> JSONResponse:
        session = await session_for(request, csrf=True)
        if not ID_RE.fullmatch(upload_id):
            raise RelayError(404, "upload_not_found", "照片暂存记录不存在。")
        upload = store.complete_upload(
            session,
            upload_id,
            ttl_seconds=settings.upload_ttl_seconds,
        )
        stored_upload = store.upload_for_session(session, upload_id)
        command, _created = store.enqueue_command(
            session=session,
            idempotency_key=f"attach_{upload_id}",
            kind="attach_photo",
            payload={
                "upload_id": upload_id,
                "step_key": str(stored_upload["step_key"]),
            },
            expected_version=int(stored_upload["expected_version"]),
            ttl_seconds=settings.command_ttl_seconds,
        )
        return _api_ok(
            {**upload, "command_id": command["command_id"], "command_status": command["status"]},
            status_code=202 if command["status"] in {"pending", "leased"} else 200,
        )

    @app.get("/api/v1/work-orders/photos/{photo_id}")
    async def public_photo(photo_id: str, request: Request) -> FileResponse:
        session = await session_for(request)
        if not ID_RE.fullmatch(photo_id):
            raise RelayError(404, "photo_not_found", "照片不存在。")
        photo = store.photo_for_session(session, photo_id)
        path = Path(str(photo["path"])).resolve()
        return FileResponse(
            path,
            media_type=str(photo["content_type"]),
            filename=Path(str(photo["file_name"])).name,
            content_disposition_type="inline",
            headers={"Cache-Control": "private, no-store", "X-Content-SHA256": str(photo["actual_sha256"])},
        )

    @app.post("/api/v1/internal/authority/lease")
    async def authority_lease(request: Request) -> JSONResponse:
        authority_id, _body, _lease = await internal_for(request, fence=False)
        payload = _validated(LeaseRequest, await _json_body(request))
        assert isinstance(payload, LeaseRequest)
        lease = store.issue_authority_lease(
            authority_id,
            payload.instance_id,
            ttl_seconds=settings.authority_lease_seconds,
        )
        lease.pop("authority_id", None)
        return _api_ok(lease)

    @app.put("/api/v1/internal/groups/{public_group_id}")
    async def register_group(public_group_id: str, request: Request) -> JSONResponse:
        authority_id, _body, _lease = await internal_for(request, fence=True)
        if not ID_RE.fullmatch(public_group_id):
            raise RelayError(422, "invalid_group_id", "公网工单标识无效。")
        payload = _validated(GroupRegisterRequest, await _json_body(request))
        assert isinstance(payload, GroupRegisterRequest)
        if payload.state not in GROUP_STATES:
            raise RelayError(422, "invalid_group_state", "工单状态无效。")
        if set(payload.links) != {"operator", "reviewer"}:
            raise RelayError(422, "role_links_required", "必须同时注册操作人和审核人链接。")
        links: list[dict[str, Any]] = []
        seen_link_ids: set[str] = set()
        for role in ("operator", "reviewer"):
            link = payload.links[role]
            if not ID_RE.fullmatch(link.link_id) or not SHA256_RE.fullmatch(link.secret_sha256.lower()):
                raise RelayError(422, "invalid_role_link", "角色链接参数无效。")
            if link.link_id in seen_link_ids:
                raise RelayError(422, "duplicate_role_link", "两条角色链接必须相互隔离。")
            seen_link_ids.add(link.link_id)
            links.append(
                {
                    "role": role,
                    "link_id": link.link_id,
                    "secret_sha256": link.secret_sha256.lower(),
                    "generation": link.generation,
                    "assigned_name": link.assigned_name,
                }
            )
        projection = sanitize_projection_bundle(payload.projection)
        _set_projection_state(projection, payload.state)
        registration_payload = {
            "protocol_version": payload.protocol_version,
            "registration_version": payload.registration_version,
            "state": payload.state,
            "authority_version": payload.authority_version,
            "projection_revision": payload.projection_revision,
            "projection": projection,
            "links": links,
        }
        registered = store.register_group(
            authority_id=authority_id,
            public_group_id=public_group_id,
            registration_version=payload.registration_version,
            registration_payload=registration_payload,
            state=payload.state,
            authority_version=payload.authority_version,
            projection_revision=payload.projection_revision,
            projection=projection,
            links=links,
        )
        public_base_url = settings.public_base_url or str(request.base_url).rstrip("/")
        registered["protocol_version"] = RELAY_PROTOCOL_VERSION
        registered["entry_url"] = f"{public_base_url}/polling-work-order"
        registered["entry_path"] = "/polling-work-order"
        LOGGER.info("work order registered: group=%s", public_group_id)
        return _api_ok(registered)

    @app.get("/api/v1/internal/commands/lease")
    async def lease_commands(request: Request) -> JSONResponse:
        authority_id, _body, lease = await internal_for(request, fence=True)
        try:
            limit = min(100, max(1, int(request.query_params.get("limit") or 20)))
            wait_seconds = min(25, max(0, int(request.query_params.get("wait_seconds") or 0)))
        except ValueError as exc:
            raise RelayError(422, "invalid_lease_parameters", "命令租取参数无效。") from exc
        deadline = time.monotonic() + wait_seconds
        while True:
            lease = store.verify_fencing_token(
                authority_id, str(request.headers.get("x-relay-fencing-token") or "")
            )
            commands = store.lease_commands(
                authority_id=authority_id,
                instance_id=str(lease["instance_id"]),
                authority_epoch=int(lease["epoch"]),
                limit=limit,
                lease_seconds=settings.command_lease_seconds,
            )
            if commands or time.monotonic() >= deadline:
                return _api_ok(
                    {
                        "commands": commands,
                        "authority_epoch": int(lease["epoch"]),
                        "lease_expires_at": float(lease["expires_at"]),
                    }
                )
            await asyncio.sleep(0.5)

    @app.get("/api/v1/internal/uploads/{upload_id}/content")
    async def internal_upload(upload_id: str, request: Request) -> FileResponse:
        authority_id, _body, _lease = await internal_for(request, fence=True)
        if not ID_RE.fullmatch(upload_id):
            raise RelayError(404, "upload_not_found", "照片暂存内容不存在。")
        upload = store.internal_upload(authority_id, upload_id)
        return FileResponse(
            Path(str(upload["path"])).resolve(),
            media_type=str(upload["content_type"]),
            filename=Path(str(upload["file_name"])).name,
            headers={
                "Cache-Control": "no-store",
                "X-Content-SHA256": str(upload["actual_sha256"]),
                "X-Public-Group-Id": str(upload["public_group_id"]),
                "X-Step-Key": str(upload["step_key"]),
            },
        )

    @app.post("/api/v1/internal/commands/{command_id}/ack")
    async def acknowledge_command(command_id: str, request: Request) -> JSONResponse:
        authority_id, _body, lease = await internal_for(request, fence=True)
        if not ID_RE.fullmatch(command_id):
            raise RelayError(404, "command_not_found", "命令不存在。")
        payload = _validated(CommandAckRequest, await _json_body(request))
        assert isinstance(payload, CommandAckRequest)
        if payload.outcome in {"succeeded", "success"} and payload.projection is None:
            raise RelayError(422, "projection_required", "成功 ACK 必须携带最新角色投影。")
        projection = (
            sanitize_projection_bundle(payload.projection)
            if payload.projection is not None
            else None
        )
        acknowledgement = {
            "outcome": payload.outcome,
            "authority_version": payload.authority_version,
            "projection_revision": payload.projection_revision,
            "result": payload.result,
            "error_code": payload.error_code,
            "error": payload.error,
        }
        return _api_ok(
            store.acknowledge_command(
                authority_id=authority_id,
                command_id=command_id,
                authority_epoch=int(lease["epoch"]),
                instance_id=str(lease["instance_id"]),
                acknowledgement=acknowledgement,
                projection=projection,
            )
        )

    @app.put("/api/v1/internal/groups/{public_group_id}/projection")
    async def update_projection(public_group_id: str, request: Request) -> JSONResponse:
        authority_id, _body, _lease = await internal_for(request, fence=True)
        if not ID_RE.fullmatch(public_group_id):
            raise RelayError(404, "group_not_found", "工单不存在。")
        payload = _validated(ProjectionRequest, await _json_body(request))
        assert isinstance(payload, ProjectionRequest)
        if payload.state not in GROUP_STATES:
            raise RelayError(422, "invalid_group_state", "工单状态无效。")
        projection = sanitize_projection_bundle(payload.projection)
        _set_projection_state(projection, payload.state)
        return _api_ok(
            store.update_projection(
                authority_id=authority_id,
                public_group_id=public_group_id,
                authority_version=payload.authority_version,
                projection_revision=payload.projection_revision,
                projection=projection,
                state=payload.state,
            )
        )

    @app.post("/api/v1/internal/groups/{public_group_id}/cancel")
    async def cancel_group(public_group_id: str, request: Request) -> JSONResponse:
        authority_id, _body, _lease = await internal_for(request, fence=True)
        if not ID_RE.fullmatch(public_group_id):
            raise RelayError(404, "group_not_found", "工单不存在。")
        payload = _validated(CancelRequest, await _json_body(request))
        assert isinstance(payload, CancelRequest)
        result = store.cancel_group(authority_id, public_group_id, reason=payload.reason)
        LOGGER.info(
            "work order cancelled: group=%s purged=%s",
            public_group_id,
            bool(result.get("purged")),
        )
        return _api_ok(result)

    return app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="ClipFlow 公网轮巡工单中继")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址；生产建议由 HTTPS 反向代理接入")
    parser.add_argument("--port", type=int, default=18767)
    args = parser.parse_args(argv)
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    LOGGER.info("starting relay on %s:%s", args.host, args.port)
    uvicorn.run(create_app(RelaySettings.from_env()), host=args.host, port=args.port, access_log=False)


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import nullcontext, suppress
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol


POLLING_RELAY_RUNTIME_NAMESPACE = "polling_work_order_relay_runtime"
POLLING_RELAY_GROUP_NAMESPACE = "polling_work_order_relay_group"
POLLING_RELAY_COMMAND_NAMESPACE = "polling_work_order_relay_command"
POLLING_RELAY_RUNTIME_KEY = "connector"
POLLING_RELAY_MAX_JSON_BYTES = 2 * 1024 * 1024
POLLING_RELAY_MAX_PHOTO_BYTES = 8 * 1024 * 1024
POLLING_RELAY_REMOTE_VERIFY_SECONDS = 5 * 60
POLLING_RELAY_PROTOCOL_VERSION = 1
POLLING_PUBLIC_SERVICE_NAME = "public_polling_work_order"
POLLING_PUBLIC_ARTIFACT_MAX_BYTES = 20 * 1024 * 1024


class PollingRelayError(RuntimeError):
    pass


class PollingRelayConfigurationError(PollingRelayError):
    pass


class PollingRelayProtocolError(PollingRelayError):
    pass


class PollingRelayLeaseError(PollingRelayError):
    pass


class PollingRelayHttpError(PollingRelayError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = int(status or 0)


def _flag(value: Any, default: bool = False) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on", "是"}


def _bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        result = int(str(value or "").strip() or default)
    except (TypeError, ValueError):
        result = default
    return max(minimum, min(maximum, result))


@dataclass(frozen=True)
class PollingRelayConfig:
    enabled: bool
    base_url: str = ""
    connector_id: str = ""
    lease_seconds: int = 90
    renew_before_seconds: int = 30
    long_poll_seconds: int = 10
    request_timeout_seconds: int = 40
    max_commands: int = 20
    allow_insecure_http: bool = False

    @classmethod
    def from_env(
        cls, environ: Mapping[str, str] | None = None
    ) -> "PollingRelayConfig":
        env = os.environ if environ is None else environ
        base_url = str(env.get("CLIPFLOW_POLLING_RELAY_URL") or "").strip().rstrip("/")
        connector_id = str(
            env.get("CLIPFLOW_POLLING_RELAY_CONNECTOR_ID") or ""
        ).strip()
        explicit = str(env.get("CLIPFLOW_POLLING_RELAY_ENABLED") or "").strip()
        enabled = _flag(explicit, bool(base_url))
        allow_insecure = _flag(
            env.get("CLIPFLOW_POLLING_RELAY_ALLOW_INSECURE_HTTP"), False
        )
        if enabled:
            parsed = urllib.parse.urlsplit(base_url)
            if (
                parsed.scheme not in ({"https", "http"} if allow_insecure else {"https"})
                or not parsed.netloc
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or parsed.path not in {"", "/"}
            ):
                raise PollingRelayConfigurationError(
                    "公网中继地址必须是无账号、无路径参数的 HTTPS 根地址。"
                )
            if connector_id and not re.fullmatch(
                r"[A-Za-z0-9_-]{8,160}", connector_id
            ):
                raise PollingRelayConfigurationError("公网中继 connector_id 格式无效。")
        long_poll_seconds = _bounded_int(
            env.get("CLIPFLOW_POLLING_RELAY_LONG_POLL_SECONDS"), 10, 1, 15
        )
        request_timeout_seconds = max(
            long_poll_seconds + 5,
            _bounded_int(
                env.get("CLIPFLOW_POLLING_RELAY_TIMEOUT_SECONDS"), 40, 5, 90
            ),
        )
        return cls(
            enabled=enabled,
            base_url=base_url,
            connector_id=connector_id,
            lease_seconds=_bounded_int(
                env.get("CLIPFLOW_POLLING_RELAY_LEASE_SECONDS"), 90, 30, 600
            ),
            renew_before_seconds=_bounded_int(
                env.get("CLIPFLOW_POLLING_RELAY_RENEW_BEFORE_SECONDS"),
                30,
                5,
                300,
            ),
            long_poll_seconds=long_poll_seconds,
            request_timeout_seconds=request_timeout_seconds,
            max_commands=_bounded_int(
                env.get("CLIPFLOW_POLLING_RELAY_MAX_COMMANDS"), 20, 1, 100
            ),
            allow_insecure_http=allow_insecure,
        )


@dataclass(frozen=True)
class RelayResponse:
    status: int
    headers: dict[str, str]
    body: bytes


class PollingRelayTransport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes,
        timeout: float,
        max_bytes: int,
    ) -> RelayResponse: ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class UrllibPollingRelayTransport:
    def __init__(self) -> None:
        self._opener = urllib.request.build_opener(_NoRedirect())

    @staticmethod
    def _read_limited(stream: Any, maximum: int) -> bytes:
        content = stream.read(maximum + 1)
        if len(content) > maximum:
            raise PollingRelayProtocolError("公网中继响应超过允许大小。")
        return content

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes,
        timeout: float,
        max_bytes: int,
    ) -> RelayResponse:
        request = urllib.request.Request(
            url,
            data=body if method.upper() not in {"GET", "HEAD"} else None,
            headers=dict(headers),
            method=method.upper(),
        )
        try:
            with self._opener.open(request, timeout=timeout) as response:
                return RelayResponse(
                    int(response.status or 0),
                    {str(key).lower(): str(value) for key, value in response.headers.items()},
                    self._read_limited(response, max_bytes),
                )
        except urllib.error.HTTPError as exc:
            return RelayResponse(
                int(exc.code or 0),
                {str(key).lower(): str(value) for key, value in exc.headers.items()},
                self._read_limited(exc, max_bytes),
            )
        except (OSError, urllib.error.URLError) as exc:
            raise PollingRelayError(f"公网中继连接失败：{exc}") from exc


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _json_object(content: bytes) -> dict[str, Any]:
    if not content:
        return {}
    try:
        value = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PollingRelayProtocolError("公网中继返回了无效 JSON。") from exc
    if not isinstance(value, dict):
        raise PollingRelayProtocolError("公网中继 JSON 顶层必须是对象。")
    return value


def probe_polling_relay_health(
    base_url: str,
    *,
    timeout: float = 2.0,
    transport: PollingRelayTransport | None = None,
) -> dict[str, Any]:
    """Return a small, non-throwing compatibility probe for settings and starts."""
    started = time.monotonic()
    result: dict[str, Any] = {
        "ready": False,
        "service": "",
        "protocol_version": 0,
        "elapsed_ms": 0.0,
        "error": "",
    }
    try:
        root = str(base_url or "").strip().rstrip("/")
        parsed = urllib.parse.urlsplit(root)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise PollingRelayConfigurationError("公网工单地址格式无效。")
        response = (transport or UrllibPollingRelayTransport()).request(
            "GET",
            f"{root}/api/v1/health",
            headers={"Accept": "application/json"},
            body=b"",
            timeout=max(0.2, float(timeout)),
            max_bytes=64 * 1024,
        )
        if not 200 <= int(response.status) < 300:
            raise PollingRelayHttpError(
                int(response.status), f"公网工单健康检查返回 HTTP {response.status}。"
            )
        payload = _json_object(response.body)
        result.update(
            {
                "service": str(payload.get("service") or ""),
                "protocol_version": int(payload.get("protocol_version") or 0),
            }
        )
        if result["service"] != POLLING_PUBLIC_SERVICE_NAME:
            raise PollingRelayProtocolError("公网地址返回的不是工单服务。")
        if result["protocol_version"] != POLLING_RELAY_PROTOCOL_VERSION:
            raise PollingRelayProtocolError("公网工单协议版本不兼容。")
        if payload.get("ready") is not True:
            raise PollingRelayProtocolError("公网工单服务尚未就绪。")
        result["ready"] = True
    except Exception as exc:
        result["error"] = str(exc or "公网工单连接失败。")
    finally:
        result["elapsed_ms"] = round((time.monotonic() - started) * 1000.0, 1)
    return result


def _body_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _epoch_seconds(value: Any, fallback: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return float(fallback)
    try:
        return float(text)
    except ValueError:
        try:
            return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return float(fallback)


class PollingWorkOrderRelayConnector:
    """Single-authority bridge between local work orders and a public relay."""

    def __init__(
        self,
        state_store: Any,
        work_orders: Any,
        *,
        config: PollingRelayConfig | None = None,
        transport: PollingRelayTransport | None = None,
        finalize_callback: Callable[[str], dict[str, Any]] | None = None,
        relay_metadata_writer: Callable[[str, dict[str, Any]], None] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.state_store = state_store
        self.work_orders = work_orders
        self.config = config or PollingRelayConfig.from_env()
        self.transport = transport or UrllibPollingRelayTransport()
        self.finalize_callback = finalize_callback
        self.relay_metadata_writer = relay_metadata_writer
        self.clock = clock
        self._lease_lock = threading.RLock()
        self._command_locks_guard = threading.RLock()
        self._command_locks: dict[str, threading.RLock] = {}
        self._runtime = self._load_runtime()

    @property
    def enabled(self) -> bool:
        return bool(self.config.enabled)

    def _load_runtime(self) -> dict[str, Any]:
        stored = self.state_store.get_document(
            POLLING_RELAY_RUNTIME_NAMESPACE, POLLING_RELAY_RUNTIME_KEY
        ) or {}
        installation_id = self.config.connector_id or str(
            stored.get("installation_id") or stored.get("connector_id") or ""
        ).strip()
        if installation_id and not re.fullmatch(
            r"[A-Za-z0-9_-]{8,160}", installation_id
        ):
            installation_id = ""
        if not installation_id:
            installation_id = secrets.token_urlsafe(24)
        connector_id = f"{installation_id[:100]}_{secrets.token_urlsafe(12)}"
        runtime = {
            **stored,
            "installation_id": installation_id,
            "connector_id": connector_id,
            "fencing_token": "",
            "lease_expires_at": 0,
            "authority_epoch": 0,
        }
        runtime.pop("site_id", None)
        if self.enabled:
            self.state_store.put_document(
                POLLING_RELAY_RUNTIME_NAMESPACE,
                POLLING_RELAY_RUNTIME_KEY,
                runtime,
            )
        return runtime

    @property
    def connector_id(self) -> str:
        return str(self._runtime.get("connector_id") or "")

    def _path_url(self, path: str, query: Mapping[str, Any] | None = None) -> tuple[str, str]:
        normalized = "/" + str(path or "").lstrip("/")
        encoded_query = urllib.parse.urlencode(
            sorted((str(key), str(value)) for key, value in (query or {}).items())
        )
        signed_path = normalized + (f"?{encoded_query}" if encoded_query else "")
        return self.config.base_url + signed_path, signed_path

    def _request_headers(
        self,
        *,
        idempotency_key: str = "",
        fencing_token: str = "",
    ) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key[:160]
        if fencing_token:
            headers["X-Relay-Fencing-Token"] = fencing_token
        return headers

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        query: Mapping[str, Any] | None = None,
        idempotency_key: str = "",
        fencing_token: str = "",
        max_bytes: int = POLLING_RELAY_MAX_JSON_BYTES,
    ) -> RelayResponse:
        if not self.enabled:
            raise PollingRelayConfigurationError("公网中继未启用。")
        body = _json_bytes(payload or {}) if method.upper() not in {"GET", "HEAD"} else b""
        url, _signed_path = self._path_url(path, query)
        response = self.transport.request(
            method,
            url,
            headers=self._request_headers(
                idempotency_key=idempotency_key,
                fencing_token=fencing_token,
            ),
            body=body,
            timeout=float(self.config.request_timeout_seconds),
            max_bytes=max_bytes,
        )
        if not 200 <= int(response.status) < 300:
            error = ""
            try:
                error_value = _json_object(response.body).get("error")
                error = (
                    str(error_value.get("message") or error_value.get("code") or "")
                    if isinstance(error_value, dict)
                    else str(error_value or "")
                )
            except PollingRelayProtocolError:
                error = response.body.decode("utf-8", errors="replace")[:300]
            raise PollingRelayHttpError(
                int(response.status), error or f"公网中继返回 HTTP {response.status}。"
            )
        return response

    def _json_request(self, *args, **kwargs) -> dict[str, Any]:
        response = _json_object(self._request(*args, **kwargs).body)
        data = response.get("data")
        return copy.deepcopy(data) if response.get("ok") is True and isinstance(data, dict) else response

    def _save_runtime(self, **changes: Any) -> dict[str, Any]:
        self._runtime = {**self._runtime, **changes, "updated_at": self.clock()}
        self.state_store.put_document(
            POLLING_RELAY_RUNTIME_NAMESPACE,
            POLLING_RELAY_RUNTIME_KEY,
            self._runtime,
        )
        return copy.deepcopy(self._runtime)

    def ensure_lease(self, *, force: bool = False) -> dict[str, Any]:
        with self._lease_lock:
            expires_at = float(self._runtime.get("lease_expires_at") or 0)
            authority_epoch = int(self._runtime.get("authority_epoch") or 0)
            fencing_token = str(self._runtime.get("fencing_token") or "")
            if (
                not force
                and authority_epoch > 0
                and fencing_token
                and expires_at - self.clock() > self.config.renew_before_seconds
            ):
                return copy.deepcopy(self._runtime)
            attempt_epoch = int(
                self._runtime.get("lease_attempt_epoch") or 0
            )
            attempt_id = str(self._runtime.get("lease_attempt_id") or "")
            if not attempt_id or attempt_epoch != authority_epoch:
                attempt_id = secrets.token_urlsafe(18)
                self._save_runtime(
                    lease_attempt_id=attempt_id,
                    lease_attempt_epoch=authority_epoch,
                )
            try:
                response = self._json_request(
                    "POST",
                    "/api/v1/internal/authority/lease",
                    payload={
                        "instance_id": self.connector_id,
                        "previous_epoch": authority_epoch,
                    },
                    idempotency_key=f"lease:{self.connector_id}:{attempt_id}",
                )
                if response.get("authoritative") is False:
                    raise PollingRelayLeaseError("当前连接器没有公网中继权威租约。")
                next_epoch = int(response.get("authority_epoch") or 0)
                next_fencing_token = str(
                    response.get("fencing_token") or response.get("lease_id") or ""
                ).strip()
                if next_epoch <= 0:
                    raise PollingRelayProtocolError("公网中继返回的 authority_epoch 无效。")
                if not re.fullmatch(r"[A-Za-z0-9_-]{24,200}", next_fencing_token):
                    raise PollingRelayProtocolError("公网中继返回的 fencing_token 无效。")
                next_expires = _epoch_seconds(
                    response.get("lease_expires_at") or response.get("expires_at"),
                    self.clock() + self.config.lease_seconds,
                )
                if next_expires <= self.clock():
                    raise PollingRelayProtocolError("公网中继返回了已过期租约。")
                return self._save_runtime(
                    fencing_token=next_fencing_token,
                    lease_expires_at=next_expires,
                    authority_epoch=next_epoch,
                    lease_attempt_id="",
                    lease_attempt_epoch=0,
                    lease_error="",
                )
            except Exception as exc:
                self._save_runtime(lease_error=str(exc), lease_error_at=self.clock())
                raise

    def _authority_credentials(self) -> tuple[int, str]:
        runtime = self.ensure_lease()
        authority_epoch = int(runtime.get("authority_epoch") or 0)
        fencing_token = str(runtime.get("fencing_token") or "")
        if authority_epoch <= 0 or not fencing_token:
            raise PollingRelayLeaseError("缺少公网中继权威租约。")
        return authority_epoch, fencing_token

    @staticmethod
    def _safe_person(value: Any) -> dict[str, str]:
        source = value if isinstance(value, dict) else {}
        return {"name": str(source.get("name") or "").strip()}

    @staticmethod
    def _safe_photo(value: Any) -> dict[str, Any]:
        source = value if isinstance(value, dict) else {}
        return {
            key: source.get(key)
            for key in (
                "name",
                "mime_type",
                "size",
                "sha256",
                "uploaded_role",
                "uploaded_at",
            )
            if source.get(key) not in (None, "")
        }

    def _role_projection(self, target_record_id: str, role: str) -> dict[str, Any]:
        session = self.work_orders.session(
            self.work_orders.role_token(target_record_id, role)
        )
        work_orders = [
            {
                key: item.get(key)
                for key in (
                    "run_index",
                    "from_unit",
                    "to_unit",
                    "label",
                    "step_count",
                    "completed_steps",
                    "state",
                    "selectable",
                )
            }
            for item in session.get("work_orders") or []
            if isinstance(item, dict)
        ]
        steps = []
        for item in session.get("steps") or []:
            if not isinstance(item, dict):
                continue
            steps.append(
                {
                    key: copy.deepcopy(item.get(key))
                    for key in (
                        "step_key",
                        "global_index",
                        "run_index",
                        "run_count",
                        "run_label",
                        "step_index",
                        "step_count",
                        "content",
                        "operator_required",
                        "reviewer_required",
                        "time_limit_seconds",
                        "position",
                        "operator_confirmed",
                        "reviewer_confirmed",
                        "timer_started",
                        "confirm_available_at",
                        "remaining_seconds",
                    )
                }
            )
            steps[-1]["photos"] = [
                self._safe_photo(photo)
                for photo in item.get("photos") or []
                if isinstance(photo, dict)
            ]
        return {
            "role": role,
            "role_label": str(session.get("role_label") or ""),
            "assigned_person": self._safe_person(session.get("assigned_person")),
            "state": str(session.get("state") or ""),
            "version": int(session.get("version") or 0),
            "title": str(session.get("title") or ""),
            "sop_name": str(session.get("sop_name") or ""),
            "current_index": int(session.get("current_index") or 0),
            "total_steps": int(session.get("total_steps") or 0),
            "current_run_index": int(session.get("current_run_index") or 0),
            "can_release_selection": bool(session.get("can_release_selection")),
            "can_rollback_previous": bool(session.get("can_rollback_previous")),
            "work_orders": work_orders,
            "steps": steps,
            "last_error": str(session.get("last_error") or ""),
        }

    def dual_projection(self, target_record_id: str) -> dict[str, Any]:
        manager_lock = getattr(self.work_orders, "_lock", None)
        context = manager_lock if manager_lock is not None else nullcontext()
        with context:
            return {
                "operator": self._role_projection(target_record_id, "operator"),
                "reviewer": self._role_projection(target_record_id, "reviewer"),
            }

    def _group_document(self, target_record_id: str) -> dict[str, Any]:
        return self.state_store.get_document(
            POLLING_RELAY_GROUP_NAMESPACE, str(target_record_id or "").strip()
        ) or {}

    def _put_group_document(
        self, target_record_id: str, document: dict[str, Any]
    ) -> dict[str, Any]:
        normalized = {**document, "target_record_id": target_record_id, "updated_at": self.clock()}
        self.state_store.put_document(
            POLLING_RELAY_GROUP_NAMESPACE, target_record_id, normalized
        )
        self._write_group_relay_metadata(target_record_id, normalized)
        return copy.deepcopy(normalized)

    def _write_group_relay_metadata(
        self, target_record_id: str, document: dict[str, Any]
    ) -> None:
        relay = {
            "mode": "public_relay",
            "relay_url": self.config.base_url,
            "public_group_id": str(document.get("public_group_id") or ""),
            "registration_state": str(document.get("registration_state") or ""),
            "operator_link": str(document.get("operator_link") or ""),
            "reviewer_link": str(document.get("reviewer_link") or ""),
            "link_ids": copy.deepcopy(document.get("link_ids") or {}),
            "last_error": str(document.get("last_error") or ""),
            "projection_revision": int(
                document.get("projection_revision")
                or document.get("relay_revision")
                or 0
            ),
            "updated_at": self.clock(),
        }
        if self.relay_metadata_writer:
            self.relay_metadata_writer(target_record_id, relay)
            return
        manager_lock = getattr(self.work_orders, "_lock", None)
        context = manager_lock if manager_lock is not None else nullcontext()
        with context:
            group = self.state_store.get_document(
                "polling_work_order", target_record_id
            )
            if not isinstance(group, dict):
                return
            group["relay"] = relay
            group["updated_at"] = group.get("updated_at") or self.clock()
            self.state_store.put_document(
                "polling_work_order", target_record_id, group
            )

    @staticmethod
    def _retry_delay(attempts: int) -> float:
        return float(min(300, max(2, 2 ** min(max(1, attempts), 8))))

    @staticmethod
    def _public_link(value: Any, *, allow_http: bool) -> str:
        text = str(value or "").strip()
        parsed = urllib.parse.urlsplit(text)
        if (
            any(ord(character) < 32 or ord(character) == 127 for character in text)
            or parsed.scheme not in ({"https", "http"} if allow_http else {"https"})
            or not parsed.netloc
            or parsed.username
            or parsed.password
        ):
            raise PollingRelayProtocolError("公网中继返回了无效能力链接。")
        return text

    @staticmethod
    def _valid_link_credentials(value: Any) -> bool:
        if not isinstance(value, dict):
            return False
        return all(
            isinstance(value.get(role), dict)
            and re.fullmatch(
                r"[A-Za-z0-9_-]{16,160}", str(value[role].get("link_id") or "")
            )
            and len(str(value[role].get("secret") or "")) >= 32
            for role in ("operator", "reviewer")
        )

    def _link_credentials(self, document: dict[str, Any]) -> dict[str, dict[str, Any]]:
        existing = document.get("link_credentials")
        if self._valid_link_credentials(existing):
            return copy.deepcopy(existing)
        return {
            role: {
                "link_id": f"lnk_{secrets.token_urlsafe(18)}",
                "secret": secrets.token_urlsafe(32),
                "generation": 1,
            }
            for role in ("operator", "reviewer")
        }

    def _capability_link(self, entry_url: Any, credential: dict[str, Any]) -> str:
        entry = str(entry_url or "").strip() or (
            f"{self.config.base_url}/polling-work-order"
        )
        parsed = urllib.parse.urlsplit(entry)
        entry = urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, parsed.query, "")
        )
        self._public_link(entry, allow_http=self.config.allow_insecure_http)
        link_id = urllib.parse.quote(str(credential.get("link_id") or ""), safe="_-.")
        secret = urllib.parse.quote(str(credential.get("secret") or ""), safe="_-.")
        return self._public_link(
            f"{entry}#link_id={link_id}&secret={secret}",
            allow_http=self.config.allow_insecure_http,
        )

    def _authority_request_kwargs(self) -> dict[str, Any]:
        return {"fencing_token": self._authority_credentials()[1]}

    def register_group(self, target_record_id: str, *, force: bool = False) -> dict[str, Any]:
        group = self.work_orders.get_group(target_record_id)
        if str(group.get("state") or "") in {"cancelled", "stopped"}:
            return self.cancel_registration(
                target_record_id,
                reason=str(group.get("cancel_reason") or "local_cancelled"),
                force=force,
            )
        document = self._group_document(target_record_id)
        if (
            not force
            and str(document.get("registration_state") or "") == "registered"
        ):
            same_version = int(document.get("registered_version") or 0) == int(
                group.get("version") or 0
            )
            if same_version:
                if float(document.get("last_remote_verified_at") or 0) > (
                    self.clock() - POLLING_RELAY_REMOTE_VERIFY_SECONDS
                ):
                    return document
            else:
                if float(document.get("next_retry_at") or 0) > self.clock():
                    return document
                try:
                    self.push_projection(target_record_id)
                    return self._group_document(target_record_id)
                except Exception as exc:
                    if isinstance(exc, PollingRelayHttpError) and exc.status == 404:
                        self._put_group_document(
                            target_record_id,
                            {
                                **document,
                                "registration_state": "registration_pending",
                                "next_retry_at": 0,
                                "last_error": "公网工单不存在，正在重新注册。",
                            },
                        )
                        return self.register_group(target_record_id, force=True)
                    attempts = int(document.get("projection_attempts") or 0) + 1
                    return self._put_group_document(
                        target_record_id,
                        {
                            **document,
                            "projection_attempts": attempts,
                            "next_retry_at": self.clock() + self._retry_delay(attempts),
                            "last_error": str(exc),
                        },
                    )
            if float(document.get("next_retry_at") or 0) > self.clock():
                return document
        if not force and float(document.get("next_retry_at") or 0) > self.clock():
            return document
        public_id = str(document.get("public_group_id") or "")
        if not public_id:
            public_id = f"g_{secrets.token_urlsafe(30)}"
            document = self._put_group_document(
                target_record_id, {**document, "public_group_id": public_id}
            )
        credentials = self._link_credentials(document)
        payload = document.get("registration_payload")
        if isinstance(payload, dict) and int(payload.get("protocol_version") or 0) != (
            POLLING_RELAY_PROTOCOL_VERSION
        ):
            payload = {
                **payload,
                "protocol_version": POLLING_RELAY_PROTOCOL_VERSION,
                "registration_version": max(
                    2, int(payload.get("registration_version") or 1) + 1
                ),
            }
            document = self._put_group_document(
                target_record_id,
                {
                    **document,
                    "registration_version": int(payload["registration_version"]),
                    "registration_payload": copy.deepcopy(payload),
                },
            )
        if not isinstance(payload, dict):
            projection = self.dual_projection(target_record_id)
            registration_version = max(1, int(document.get("registration_version") or 1))
            projection_revision = max(1, int(document.get("projection_revision") or 1))
            payload = {
                "protocol_version": POLLING_RELAY_PROTOCOL_VERSION,
                "registration_version": registration_version,
                "state": str(group.get("state") or ""),
                "authority_version": int(group.get("version") or 0),
                "projection_revision": projection_revision,
                "projection": projection,
                "links": {
                    role: {
                        "link_id": str(credentials[role]["link_id"]),
                        "secret_sha256": hashlib.sha256(
                            str(credentials[role]["secret"]).encode("utf-8")
                        ).hexdigest(),
                        "generation": int(credentials[role].get("generation") or 1),
                        "assigned_name": projection[role]["assigned_person"]["name"],
                    }
                    for role in ("operator", "reviewer")
                },
            }
            document = self._put_group_document(
                target_record_id,
                {
                    **document,
                    "public_group_id": public_id,
                    "registration_state": "registration_pending",
                    "registration_version": registration_version,
                    "projection_revision": projection_revision,
                    "link_credentials": credentials,
                    "link_ids": {
                        role: str(credentials[role]["link_id"])
                        for role in ("operator", "reviewer")
                    },
                    "registration_payload": copy.deepcopy(payload),
                },
            )
        attempts = int(document.get("registration_attempts") or 0) + 1
        try:
            response = self._json_request(
                "PUT",
                f"/api/v1/internal/groups/{urllib.parse.quote(public_id, safe='')}",
                payload=payload,
                idempotency_key=f"register:{public_id}:{int(payload.get('registration_version') or 1)}",
                **self._authority_request_kwargs(),
            )
            if int(response.get("protocol_version") or 0) != (
                POLLING_RELAY_PROTOCOL_VERSION
            ):
                raise PollingRelayProtocolError(
                    "公网工单中继协议版本不兼容。"
                )
            response_links = response.get("links") if isinstance(response.get("links"), dict) else {}
            operator_link = str(response.get("operator_link") or response_links.get("operator") or "")
            reviewer_link = str(response.get("reviewer_link") or response_links.get("reviewer") or "")
            entry_url = response.get("entry_url") or (
                f"{self.config.base_url}/polling-work-order"
            )
            if not operator_link:
                operator_link = self._capability_link(entry_url, credentials["operator"])
            if not reviewer_link:
                reviewer_link = self._capability_link(entry_url, credentials["reviewer"])
            operator_link = self._public_link(
                operator_link, allow_http=self.config.allow_insecure_http
            )
            reviewer_link = self._public_link(
                reviewer_link, allow_http=self.config.allow_insecure_http
            )
            return self._put_group_document(
                target_record_id,
                {
                    **document,
                    "public_group_id": public_id,
                    "registration_state": "registered",
                    "registered_version": int(
                        response.get("authority_version")
                        or payload.get("authority_version")
                        or 0
                    ),
                    "registration_attempts": attempts,
                    "next_retry_at": 0,
                    "last_remote_verified_at": self.clock(),
                    "last_error": "",
                    "operator_link": operator_link,
                    "reviewer_link": reviewer_link,
                    "link_ids": {
                        role: str(credentials[role]["link_id"])
                        for role in ("operator", "reviewer")
                    },
                    "link_credentials": credentials,
                    "entry_url": str(entry_url),
                    "projection_revision": int(
                        response.get("projection_revision")
                        or payload.get("projection_revision")
                        or 1
                    ),
                },
            )
        except Exception as exc:
            current = self._group_document(target_record_id)
            return self._put_group_document(
                target_record_id,
                {
                    **current,
                    "public_group_id": public_id,
                    "registration_state": "registration_pending",
                    "registration_attempts": attempts,
                    "next_retry_at": self.clock() + self._retry_delay(attempts),
                    "last_error": str(exc),
                },
            )

    def public_links(self, target_record_id: str) -> dict[str, str]:
        document = self._group_document(target_record_id)
        return {
            "operator_link": str(document.get("operator_link") or ""),
            "reviewer_link": str(document.get("reviewer_link") or ""),
        }

    def _stage_projection(self, target_record_id: str) -> dict[str, Any]:
        document = self._group_document(target_record_id)
        public_id = str(document.get("public_group_id") or "")
        if (
            not public_id
            or str(document.get("registration_state") or "") != "registered"
            or not str(document.get("operator_link") or "")
            or not str(document.get("reviewer_link") or "")
        ):
            document = self.register_group(target_record_id, force=True)
            public_id = str(document.get("public_group_id") or "")
        if (
            str(document.get("registration_state") or "") != "registered"
            or not public_id
        ):
            raise PollingRelayError(
                str(document.get("last_error") or "公网工单注册尚未完成。")
            )
        group = self.work_orders.get_group(target_record_id)
        payload = document.get("pending_projection")
        pending_revision = (
            int(payload.get("projection_revision") or 0)
            if isinstance(payload, dict)
            else 0
        )
        if (
            not isinstance(payload, dict)
            or not str(payload.get("state") or "")
            or not isinstance(payload.get("projection"), dict)
            or pending_revision <= 0
            or int(payload.get("authority_version") or 0)
            != int(group.get("version") or 0)
        ):
            projection = self.dual_projection(target_record_id)
            payload = {
                "state": str(group.get("state") or ""),
                "authority_version": int(group.get("version") or 0),
                "projection_revision": max(
                    int(document.get("projection_revision") or 0),
                    pending_revision,
                )
                + 1,
                "projection": projection,
            }
            document = self._put_group_document(
                target_record_id,
                {**document, "pending_projection": copy.deepcopy(payload)},
            )
        return copy.deepcopy(payload)

    def push_projection(self, target_record_id: str) -> dict[str, Any]:
        payload = self._stage_projection(target_record_id)
        document = self._group_document(target_record_id)
        public_id = str(document.get("public_group_id") or "")
        projection = copy.deepcopy(payload.get("projection") or {})
        response = self._json_request(
            "PUT",
            f"/api/v1/internal/groups/{urllib.parse.quote(public_id, safe='')}/projection",
            payload=payload,
            idempotency_key=f"projection:{public_id}:{int(payload.get('projection_revision') or 0)}:{_body_sha256(_json_bytes(projection))[:16]}",
            **self._authority_request_kwargs(),
        )
        current = self._group_document(target_record_id)
        self._put_group_document(
            target_record_id,
            {
                **current,
                "registration_state": "registered",
                "registered_version": int(payload.get("authority_version") or 0),
                "projected_version": int(payload.get("authority_version") or 0),
                "projection_attempts": 0,
                "next_retry_at": 0,
                "projection_revision": int(
                    response.get("projection_revision")
                    or payload.get("projection_revision")
                    or 0
                ),
                "pending_projection": {},
                "last_remote_verified_at": self.clock(),
                "last_error": "",
            },
        )
        return projection

    def cancel_registration(
        self, target_record_id: str, *, reason: str, force: bool = False
    ) -> dict[str, Any]:
        document = self._group_document(target_record_id)
        public_id = str(document.get("public_group_id") or "")
        if not public_id:
            return self._put_group_document(
                target_record_id,
                {
                    **document,
                    "registration_state": "cancelled",
                    "operator_link": "",
                    "reviewer_link": "",
                    "last_error": "",
                },
            )
        if str(document.get("registration_state") or "") == "cancelled" and not force:
            return document
        if not force and float(document.get("next_retry_at") or 0) > self.clock():
            return document
        attempts = int(document.get("cancel_attempts") or 0) + 1
        try:
            self._json_request(
                "POST",
                f"/api/v1/internal/groups/{urllib.parse.quote(public_id, safe='')}/cancel",
                payload={"reason": str(reason or "local_cancelled")[:160]},
                idempotency_key=f"cancel:{public_id}",
                **self._authority_request_kwargs(),
            )
            return self._put_group_document(
                target_record_id,
                {
                    **document,
                    "registration_state": "cancelled",
                    "cancel_attempts": attempts,
                    "next_retry_at": 0,
                    "last_error": "",
                    "operator_link": "",
                    "reviewer_link": "",
                },
            )
        except Exception as exc:
            if isinstance(exc, PollingRelayHttpError) and exc.status == 404:
                return self._put_group_document(
                    target_record_id,
                    {
                        **document,
                        "registration_state": "cancelled",
                        "cancel_attempts": attempts,
                        "next_retry_at": 0,
                        "last_error": "",
                        "operator_link": "",
                        "reviewer_link": "",
                    },
                )
            return self._put_group_document(
                target_record_id,
                {
                    **document,
                    "registration_state": "cancel_pending",
                    "cancel_attempts": attempts,
                    "next_retry_at": self.clock() + self._retry_delay(attempts),
                    "last_error": str(exc),
                },
            )

    def reconcile_groups(self) -> dict[str, int]:
        open_groups = {
            str(group.get("target_record_id") or ""): group
            for group in self.work_orders.open_groups()
            if str(group.get("target_record_id") or "")
            and isinstance(group.get("relay"), dict)
            and str(group["relay"].get("mode") or "") == "public_relay"
        }
        registered = pending = cancelled = 0
        for target_record_id in open_groups:
            result = self.register_group(target_record_id)
            if str(result.get("registration_state") or "") == "registered":
                registered += 1
            else:
                pending += 1
        for item in self.state_store.list_documents(POLLING_RELAY_GROUP_NAMESPACE):
            document = item.get("payload") if isinstance(item, dict) else None
            if not isinstance(document, dict):
                continue
            target_record_id = str(document.get("target_record_id") or "")
            if not target_record_id or target_record_id in open_groups:
                continue
            try:
                group = self.work_orders.get_group(target_record_id)
                reason = str(group.get("cancel_reason") or "local_terminal")
                terminal = str(group.get("state") or "") in {"cancelled", "stopped"}
            except Exception:
                reason, terminal = "local_missing", True
            if terminal:
                result = self.cancel_registration(target_record_id, reason=reason)
                if str(result.get("registration_state") or "") == "cancelled":
                    cancelled += 1
                else:
                    pending += 1
        return {"registered": registered, "pending": pending, "cancelled": cancelled}

    def _target_for_public_group(self, public_group_id: str) -> str:
        for item in self.state_store.list_documents(POLLING_RELAY_GROUP_NAMESPACE):
            payload = item.get("payload") if isinstance(item, dict) else None
            if isinstance(payload, dict) and hmac.compare_digest(
                str(payload.get("public_group_id") or ""), public_group_id
            ):
                return str(payload.get("target_record_id") or "")
        raise PollingRelayProtocolError("命令引用了未知公网工单。")

    @staticmethod
    def _command_id(command: dict[str, Any]) -> str:
        command_id = str(command.get("command_id") or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{12,160}", command_id):
            raise PollingRelayProtocolError("公网中继 command_id 无效。")
        return command_id

    @staticmethod
    def _command_type(command: dict[str, Any]) -> str:
        value = str(command.get("type") or command.get("kind") or "").strip().lower()
        return {
            "attach_photo": "photo",
            "retry_attachment": "retry_upload",
        }.get(value, value)

    @staticmethod
    def _expected_version(command: dict[str, Any]) -> int:
        payload = command.get("payload") if isinstance(command.get("payload"), dict) else {}
        return int(command.get("expected_version") or payload.get("expected_version") or 0)

    def _command_fingerprint(self, command: dict[str, Any]) -> str:
        payload = command.get("payload") if isinstance(command.get("payload"), dict) else {}
        return _body_sha256(
            _json_bytes(
                {
                    "public_group_id": str(command.get("public_group_id") or ""),
                    "role": str(command.get("role") or ""),
                    "kind": self._command_type(command),
                    "expected_version": self._expected_version(command),
                    "payload": payload,
                    "upload": self._photo_upload(command),
                }
            )
        )

    @staticmethod
    def _photo_upload(command: dict[str, Any]) -> dict[str, Any]:
        payload = command.get("payload") if isinstance(command.get("payload"), dict) else {}
        upload = payload.get("upload") if isinstance(payload.get("upload"), dict) else None
        if upload is None and isinstance(command.get("upload"), dict):
            upload = command.get("upload")
        return copy.deepcopy(upload or {})

    def _download_photo(
        self,
        upload: dict[str, Any],
        *,
        public_group_id: str,
        step_key: str,
    ) -> tuple[bytes, str, str]:
        upload_id = str(upload.get("upload_id") or "").strip()
        expected_sha = str(upload.get("sha256") or "").strip().lower()
        expected_mime = str(
            upload.get("mime_type") or upload.get("content_type") or ""
        ).strip().lower()
        expected_size = int(upload.get("size") or 0)
        if (
            not re.fullmatch(r"[A-Za-z0-9_-]{8,160}", upload_id)
            or not re.fullmatch(r"[0-9a-f]{64}", expected_sha)
            or not expected_mime.startswith("image/")
            or expected_size <= 0
            or expected_size > POLLING_RELAY_MAX_PHOTO_BYTES
        ):
            raise PollingRelayProtocolError("公网照片元数据无效。")
        response = self._request(
            "GET",
            f"/api/v1/internal/uploads/{urllib.parse.quote(upload_id, safe='')}/content",
            max_bytes=POLLING_RELAY_MAX_PHOTO_BYTES,
            **self._authority_request_kwargs(),
        )
        headers = {
            str(key).lower(): str(value) for key, value in response.headers.items()
        }
        content_type = str(headers.get("content-type") or "").split(";", 1)[0].lower()
        content_length = str(headers.get("content-length") or "").strip()
        if (
            len(response.body) != expected_size
            or _body_sha256(response.body) != expected_sha
            or (content_length and int(content_length) != expected_size)
            or not content_type.startswith("image/")
            or content_type != expected_mime
            or (
                headers.get("x-content-sha256")
                and not hmac.compare_digest(headers["x-content-sha256"], expected_sha)
            )
            or (
                headers.get("x-public-group-id")
                and headers["x-public-group-id"] != public_group_id
            )
            or (
                headers.get("x-step-key")
                and headers["x-step-key"] != step_key
            )
        ):
            raise PollingRelayProtocolError("公网照片内容、大小或摘要校验失败。")
        return response.body, content_type, str(
            upload.get("file_name") or upload.get("filename") or "step_photo.png"
        )

    def _execute_command(self, command: dict[str, Any], target_record_id: str) -> dict[str, Any]:
        command_type = self._command_type(command)
        role = str(command.get("role") or "").strip().lower()
        if role not in {"operator", "reviewer"}:
            raise PollingRelayProtocolError("公网命令角色无效。")
        payload = command.get("payload") if isinstance(command.get("payload"), dict) else {}
        token = self.work_orders.role_token(target_record_id, role)
        expected_version = self._expected_version(command)
        if command_type == "activate":
            self.work_orders.activate(
                token,
                run_index=int(payload.get("run_index") or 0),
                expected_version=expected_version,
            )
        elif command_type == "release":
            self.work_orders.release_selection(
                token,
                run_index=int(payload.get("run_index") or 0),
                expected_version=expected_version,
            )
        elif command_type == "confirm":
            self.work_orders.confirm(
                token,
                step_key=str(payload.get("step_key") or ""),
                expected_version=expected_version,
                actual_open_id="",
                actual_name="公网能力链接",
            )
        elif command_type == "rollback":
            self.work_orders.rollback_previous(
                token,
                step_key=str(payload.get("step_key") or ""),
                expected_version=expected_version,
                actual_open_id="",
                actual_name="公网能力链接",
            )
        elif command_type == "photo":
            step_key = str(payload.get("step_key") or "")
            content, mime_type, file_name = self._download_photo(
                self._photo_upload(command),
                public_group_id=str(command.get("public_group_id") or ""),
                step_key=step_key,
            )
            self.work_orders.add_step_photo(
                token,
                step_key=step_key,
                expected_version=expected_version,
                file_name=file_name,
                mime_type=mime_type,
                content=content,
            )
        elif command_type == "retry_upload":
            if not self.finalize_callback:
                raise PollingRelayProtocolError("内网未配置工单附件完成回调。")
            result = self.finalize_callback(target_record_id)
            if isinstance(result, dict) and not result.get("ok"):
                raise PollingRelayError(
                    str(result.get("error") or "工单附件上传仍未完成。")
                )
        elif command_type != "refresh":
            raise PollingRelayProtocolError(f"不支持的公网命令：{command_type or '-'}")
        return self.dual_projection(target_record_id)

    def _command_document(self, command_id: str) -> dict[str, Any]:
        return self.state_store.get_document(
            POLLING_RELAY_COMMAND_NAMESPACE, command_id
        ) or {}

    def _put_command_document(
        self, command_id: str, document: dict[str, Any]
    ) -> dict[str, Any]:
        normalized = {**document, "command_id": command_id, "updated_at": self.clock()}
        self.state_store.put_document(
            POLLING_RELAY_COMMAND_NAMESPACE, command_id, normalized
        )
        return copy.deepcopy(normalized)

    def _build_ack_payload(self, document: dict[str, Any]) -> dict[str, Any]:
        target_record_id = str(document.get("target_record_id") or "")
        group = self.work_orders.get_group(target_record_id)
        relay_document = self._group_document(target_record_id)
        pending = relay_document.get("pending_projection")
        if not isinstance(pending, dict) or not isinstance(
            pending.get("projection"), dict
        ):
            pending = self._stage_projection(target_record_id)
            relay_document = self._group_document(target_record_id)
        result = document.get("result") if isinstance(document.get("result"), dict) else {}
        projection = (
            copy.deepcopy(pending.get("projection") or {})
            if isinstance(pending, dict)
            else copy.deepcopy(result.get("projection") or {})
        )
        if not projection:
            projection = self.dual_projection(target_record_id)
        projection_revision = int(
            (pending.get("projection_revision") if isinstance(pending, dict) else 0)
            or result.get("projection_revision")
            or relay_document.get("projection_revision")
            or 1
        )
        succeeded = str(document.get("status") or "") == "completed"
        outcome = str(document.get("outcome") or "")
        if not outcome:
            outcome = "succeeded" if succeeded else "rejected"
        return {
            "outcome": outcome,
            "authority_version": int(group.get("version") or 0),
            "projection_revision": projection_revision,
            "result": {
                "recovered_after_restart": bool(result.get("recovered_after_restart")),
                "projection_error": str(result.get("projection_error") or ""),
            },
            "error_code": str(document.get("error_code") or ""),
            "error": str(document.get("error") or ""),
            "projection": projection,
        }

    def _ack_document(self, document: dict[str, Any]) -> dict[str, Any]:
        command_id = str(document.get("command_id") or "")
        if not command_id:
            return document
        attempts = int(document.get("ack_attempts") or 0) + 1
        ack_payload = document.get("ack_payload")
        if not isinstance(ack_payload, dict):
            ack_payload = self._build_ack_payload(document)
            document = self._put_command_document(
                command_id, {**document, "ack_payload": copy.deepcopy(ack_payload)}
            )
        try:
            self._json_request(
                "POST",
                f"/api/v1/internal/commands/{urllib.parse.quote(command_id, safe='')}/ack",
                payload=ack_payload,
                idempotency_key=f"ack:{command_id}",
                **self._authority_request_kwargs(),
            )
            target_record_id = str(document.get("target_record_id") or "")
            relay_document = self._group_document(target_record_id)
            acknowledged_revision = int(ack_payload.get("projection_revision") or 0)
            if acknowledged_revision >= int(
                relay_document.get("projection_revision") or 0
            ):
                pending = relay_document.get("pending_projection")
                clear_pending = bool(
                    isinstance(pending, dict)
                    and int(pending.get("projection_revision") or 0)
                    <= acknowledged_revision
                )
                self._put_group_document(
                    target_record_id,
                    {
                        **relay_document,
                        "projection_revision": acknowledged_revision,
                        "registered_version": int(
                            ack_payload.get("authority_version") or 0
                        ),
                        "projected_version": int(
                            ack_payload.get("authority_version") or 0
                        ),
                        "pending_projection": {} if clear_pending else pending or {},
                        "last_remote_verified_at": self.clock(),
                        "last_error": "",
                    },
                )
            return self._put_command_document(
                command_id,
                {
                    **document,
                    "ack_status": "sent",
                    "ack_attempts": attempts,
                    "ack_next_retry_at": 0,
                    "ack_error": "",
                },
            )
        except Exception as exc:
            target_record_id = str(document.get("target_record_id") or "")
            relay_document = self._group_document(target_record_id)
            try:
                group_state = str(
                    self.work_orders.get_group(target_record_id).get("state") or ""
                )
            except Exception:
                group_state = ""
            if (
                isinstance(exc, PollingRelayHttpError)
                and exc.status in {404, 409, 410}
                and (
                    group_state in {"cancelled", "stopped"}
                    or str(relay_document.get("registration_state") or "")
                    == "cancelled"
                )
            ):
                return self._put_command_document(
                    command_id,
                    {
                        **document,
                        "ack_status": "discarded",
                        "ack_attempts": attempts,
                        "ack_next_retry_at": 0,
                        "ack_error": str(exc),
                    },
                )
            return self._put_command_document(
                command_id,
                {
                    **document,
                    "ack_status": "pending",
                    "ack_attempts": attempts,
                    "ack_next_retry_at": self.clock() + self._retry_delay(attempts),
                    "ack_error": str(exc),
                },
            )

    def _command_effect_applied(
        self, command: dict[str, Any], target_record_id: str
    ) -> bool:
        """Reconcile the narrow crash window after the local action was committed."""
        command_type = self._command_type(command)
        if command_type == "refresh":
            return True
        payload = command.get("payload") if isinstance(command.get("payload"), dict) else {}
        group = self.work_orders.get_group(target_record_id)
        if int(group.get("version") or 0) <= self._expected_version(command):
            return False
        steps = [item for item in group.get("steps") or [] if isinstance(item, dict)]
        step_key = str(payload.get("step_key") or "")
        step = next(
            (item for item in steps if str(item.get("step_key") or "") == step_key),
            None,
        )
        role = str(command.get("role") or "").strip().lower()
        if command_type == "activate":
            run_index = int(payload.get("run_index") or 0)
            return int(group.get("selected_run_index") or 0) == run_index and any(
                int(item.get("run_index") or 0) == run_index
                and float(item.get("activated_at_ts") or 0) > 0
                for item in steps
            )
        if command_type == "release":
            return int(group.get("selected_run_index") or 0) == 0
        if command_type == "confirm":
            return bool(step and step.get(f"{role}_confirmation"))
        if command_type == "rollback":
            rollback = group.get("last_rollback")
            return bool(
                isinstance(rollback, dict)
                and str(rollback.get("from_step_key") or "") == step_key
            )
        if command_type == "photo":
            upload = self._photo_upload(command)
            expected_sha = str(upload.get("sha256") or "").strip().lower()
            return bool(
                step
                and expected_sha
                and any(
                    str(photo.get("sha256") or "").strip().lower() == expected_sha
                    for photo in step.get("photos") or []
                    if isinstance(photo, dict)
                )
            )
        if command_type == "retry_upload":
            return str(group.get("state") or "") == "completed"
        return False

    def process_command(self, command: dict[str, Any]) -> dict[str, Any]:
        public_group_id = str(command.get("public_group_id") or "").strip()
        with self._command_locks_guard:
            lock = self._command_locks.setdefault(public_group_id, threading.RLock())
        with lock:
            return self._process_command_locked(command)

    def _process_command_locked(self, command: dict[str, Any]) -> dict[str, Any]:
        command_id = self._command_id(command)
        digest = self._command_fingerprint(command)
        existing = self._command_document(command_id)
        if existing and str(existing.get("payload_sha256") or "") != digest:
            raise PollingRelayProtocolError("相同 command_id 的命令内容不一致。")
        if str(existing.get("status") or "") in {"completed", "failed"}:
            return self._ack_document(existing)
        public_group_id = str(command.get("public_group_id") or "").strip()
        target_record_id = self._target_for_public_group(public_group_id)
        if str(existing.get("status") or "") == "executing" and self._command_effect_applied(
            command, target_record_id
        ):
            projection = self.dual_projection(target_record_id)
            projection_error = ""
            try:
                staged = self._stage_projection(target_record_id)
                projection = copy.deepcopy(staged.get("projection") or projection)
            except Exception as exc:
                projection_error = str(exc)
            relay_document = self._group_document(target_record_id)
            recovered = self._put_command_document(
                command_id,
                {
                    **existing,
                    "status": "completed",
                    "result": {
                        "projection": projection,
                        "projection_error": projection_error,
                        "projection_revision": int(
                            relay_document.get("projection_revision")
                            or (relay_document.get("pending_projection") or {}).get(
                                "projection_revision"
                            )
                            or 1
                        ),
                        "recovered_after_restart": True,
                    },
                    "error": "",
                    "completed_at": self.clock(),
                },
            )
            return self._ack_document(recovered)
        document = self._put_command_document(
            command_id,
            {
                **existing,
                "payload_sha256": digest,
                "public_group_id": public_group_id,
                "target_record_id": target_record_id,
                "type": self._command_type(command),
                "role": str(command.get("role") or ""),
                "status": "executing",
                "ack_status": "pending",
                "created_at": existing.get("created_at") or self.clock(),
            },
        )
        try:
            projection = self._execute_command(command, target_record_id)
            projection_error = ""
            try:
                staged = self._stage_projection(target_record_id)
                projection = copy.deepcopy(staged.get("projection") or projection)
            except Exception as exc:
                projection_error = str(exc)
            relay_document = self._group_document(target_record_id)
            document.update(
                {
                    "status": "completed",
                    "result": {
                        "projection": projection,
                        "projection_error": projection_error,
                        "projection_revision": int(
                            relay_document.get("projection_revision")
                            or (relay_document.get("pending_projection") or {}).get(
                                "projection_revision"
                            )
                            or 1
                        ),
                    },
                    "error": "",
                    "completed_at": self.clock(),
                }
            )
        except Exception as exc:
            projection = self.dual_projection(target_record_id)
            projection_error = ""
            try:
                staged = self._stage_projection(target_record_id)
                projection = copy.deepcopy(staged.get("projection") or projection)
            except Exception as projection_exc:
                projection_error = str(projection_exc)
            relay_document = self._group_document(target_record_id)
            error_code = type(exc).__name__
            if error_code == "PortalConflictError":
                outcome = "conflict"
            elif self._command_type(command) == "retry_upload" or (
                isinstance(exc, PollingRelayHttpError) and exc.status >= 500
            ):
                outcome = "retryable_error"
            else:
                outcome = "rejected"
            document.update(
                {
                    "status": "failed",
                    "outcome": outcome,
                    "result": {
                        "projection": projection,
                        "projection_error": projection_error,
                        "projection_revision": int(
                            relay_document.get("projection_revision")
                            or (relay_document.get("pending_projection") or {}).get(
                                "projection_revision"
                            )
                            or 1
                        ),
                    },
                    "error_code": error_code,
                    "error": str(exc),
                    "completed_at": self.clock(),
                }
            )
        return self._ack_document(self._put_command_document(command_id, document))

    def retry_pending_acks(self) -> int:
        sent = 0
        for item in self.state_store.list_documents(POLLING_RELAY_COMMAND_NAMESPACE):
            document = item.get("payload") if isinstance(item, dict) else None
            if (
                not isinstance(document, dict)
                or str(document.get("ack_status") or "") != "pending"
                or float(document.get("ack_next_retry_at") or 0) > self.clock()
            ):
                continue
            if str(self._ack_document(document).get("ack_status") or "") == "sent":
                sent += 1
        return sent

    def poll_commands_once(self) -> dict[str, int]:
        self.ensure_lease()
        response = self._json_request(
            "GET",
            "/api/v1/internal/commands/lease",
            query={
                "wait_seconds": self.config.long_poll_seconds,
                "limit": self.config.max_commands,
            },
            **self._authority_request_kwargs(),
        )
        commands = response.get("commands") if isinstance(response.get("commands"), list) else []
        valid_commands = [
            command
            for command in commands[: self.config.max_commands]
            if isinstance(command, dict)
        ]
        failed = len(commands[: self.config.max_commands]) - len(valid_commands)
        completed = 0
        if valid_commands:
            with ThreadPoolExecutor(
                max_workers=min(4, len(valid_commands)),
                thread_name_prefix="PollingRelayCommand",
            ) as executor:
                pending = {
                    executor.submit(self.process_command, command)
                    for command in valid_commands
                }
                while pending:
                    done, pending = wait(pending, timeout=10.0)
                    if pending:
                        try:
                            self.ensure_lease(force=True)
                        except Exception:
                            pass
                    for future in done:
                        try:
                            result = future.result()
                        except Exception:
                            failed += 1
                        else:
                            if str(result.get("status") or "") == "completed":
                                completed += 1
                            else:
                                failed += 1
        return {"received": len(commands), "completed": completed, "failed": failed}

    def run_once(self) -> dict[str, Any]:
        if not self.enabled:
            return {"enabled": False}
        self.ensure_lease(force=True)
        acked = self.retry_pending_acks()
        groups = self.reconcile_groups()
        commands = self.poll_commands_once()
        return {"enabled": True, "acked": acked, "groups": groups, "commands": commands}


class PollingWorkOrderPublicClient:
    """Small client for a public server that owns the work-order execution."""

    def __init__(
        self,
        state_store: Any,
        work_orders: Any,
        *,
        base_url: str,
        allow_insecure_http: bool = False,
        transport: PollingRelayTransport | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        root = str(base_url or "").strip().rstrip("/")
        parsed = urllib.parse.urlsplit(root)
        allowed_schemes = {"https", "http"} if allow_insecure_http else {"https"}
        if (
            parsed.scheme not in allowed_schemes
            or not parsed.netloc
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise PollingRelayConfigurationError("公网工单地址格式无效。")
        self.state_store = state_store
        self.work_orders = work_orders
        self.base_url = root
        self.allow_insecure_http = bool(allow_insecure_http)
        self.transport = transport or UrllibPollingRelayTransport()
        self.clock = clock

    @property
    def enabled(self) -> bool:
        return True

    @staticmethod
    def _retry_delay(attempts: int) -> float:
        return float(min(300, max(2, 2 ** min(max(1, attempts), 8))))

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        token: str = "",
        idempotency_key: str = "",
        max_bytes: int = POLLING_RELAY_MAX_JSON_BYTES,
        timeout: float = 10.0,
    ) -> RelayResponse:
        headers = {"Accept": "application/json"}
        body = b""
        if method.upper() not in {"GET", "HEAD"}:
            body = _json_bytes(payload or {})
            headers["Content-Type"] = "application/json"
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key[:160]
        response = self.transport.request(
            method,
            self.base_url + "/" + str(path or "").lstrip("/"),
            headers=headers,
            body=body,
            timeout=max(0.2, float(timeout)),
            max_bytes=max_bytes,
        )
        if not 200 <= int(response.status) < 300:
            message = ""
            with suppress(Exception):
                error = _json_object(response.body).get("error")
                message = (
                    str(error.get("message") or error.get("code") or "")
                    if isinstance(error, dict)
                    else str(error or "")
                )
            raise PollingRelayHttpError(
                int(response.status),
                message or f"公网工单返回 HTTP {response.status}。",
            )
        return response

    def _json_request(self, *args, **kwargs) -> dict[str, Any]:
        payload = _json_object(self._request(*args, **kwargs).body)
        data = payload.get("data")
        return copy.deepcopy(data) if payload.get("ok") is True and isinstance(data, dict) else payload

    def _public_link(self, value: Any) -> str:
        text = str(value or "").strip()
        parsed = urllib.parse.urlsplit(text)
        allowed_schemes = {"https", "http"} if self.allow_insecure_http else {"https"}
        if (
            parsed.scheme not in allowed_schemes
            or not parsed.netloc
            or parsed.username
            or parsed.password
            or any(ord(character) < 32 or ord(character) == 127 for character in text)
        ):
            raise PollingRelayProtocolError("公网工单返回了无效链接。")
        return text

    @staticmethod
    def _create_payload(
        group: dict,
        client_order_id: str,
        management_token_sha256: str,
        links: dict[str, dict[str, str]],
    ) -> dict[str, Any]:
        return {
            "protocol_version": POLLING_RELAY_PROTOCOL_VERSION,
            "client_order_id": client_order_id,
            "management_token_sha256": management_token_sha256,
            "links": {
                role: {
                    "link_id": str(item.get("link_id") or ""),
                    "secret_sha256": hashlib.sha256(
                        str(item.get("secret") or "").encode("utf-8")
                    ).hexdigest(),
                    "assigned_name": str((group.get(role) or {}).get("name") or ""),
                }
                for role, item in links.items()
            },
            "work_type": str(group.get("work_type") or "polling"),
            "title": str(group.get("title") or ""),
            "scope": str(group.get("scope") or ""),
            "sop_name": str(group.get("sop_name") or ""),
            "operator": {"name": str((group.get("operator") or {}).get("name") or "")},
            "reviewer": {"name": str((group.get("reviewer") or {}).get("name") or "")},
            "runs": [
                {
                    key: copy.deepcopy(run.get(key))
                    for key in ("run_index", "from_unit", "to_unit", "other_unit", "label")
                    if run.get(key) not in (None, "")
                }
                for run in group.get("runs") or []
                if isinstance(run, dict)
            ],
            "steps": [
                {
                    key: copy.deepcopy(step.get(key))
                    for key in (
                        "step_key",
                        "global_index",
                        "run_index",
                        "run_count",
                        "run_label",
                        "step_index",
                        "step_count",
                        "content",
                        "operator_required",
                        "reviewer_required",
                        "time_limit_seconds",
                    )
                }
                for step in group.get("steps") or []
                if isinstance(step, dict)
            ],
        }

    def register_group(self, target_record_id: str, *, force: bool = False) -> dict[str, Any]:
        group = self.work_orders.get_group(target_record_id)
        relay = dict(group.get("relay") or {})
        if str(relay.get("mode") or "") != "public_service":
            return group
        if (
            not force
            and str(relay.get("registration_state") or "") == "registered"
            and relay.get("public_group_id")
            and relay.get("management_token")
        ):
            return group
        if not force and float(relay.get("next_retry_at") or 0) > self.clock():
            return group
        client_order_id = str(relay.get("client_order_id") or "").strip()
        if not client_order_id:
            client_order_id = f"wo_{secrets.token_urlsafe(24)}"
        management_token = str(relay.get("management_token") or "").strip()
        if not management_token:
            management_token = secrets.token_urlsafe(32)
        link_credentials = relay.get("link_credentials")
        if not isinstance(link_credentials, dict) or any(
            not isinstance(link_credentials.get(role), dict)
            or not link_credentials[role].get("link_id")
            or not link_credentials[role].get("secret")
            for role in ("operator", "reviewer")
        ):
            link_credentials = {
                role: {
                    "link_id": f"lnk_{secrets.token_urlsafe(18)}",
                    "secret": secrets.token_urlsafe(32),
                }
                for role in ("operator", "reviewer")
            }
        if (
            client_order_id != str(relay.get("client_order_id") or "")
            or management_token != str(relay.get("management_token") or "")
            or link_credentials != relay.get("link_credentials")
        ):
            group = self.work_orders.update_relay(
                target_record_id,
                {
                    "client_order_id": client_order_id,
                    "management_token": management_token,
                    "link_credentials": copy.deepcopy(link_credentials),
                },
            )
            relay = dict(group.get("relay") or {})
        attempts = int(relay.get("registration_attempts") or 0) + 1
        try:
            response = self._json_request(
                "POST",
                "/api/v1/work-orders",
                payload=self._create_payload(
                    group,
                    client_order_id,
                    hashlib.sha256(management_token.encode("utf-8")).hexdigest(),
                    link_credentials,
                ),
                idempotency_key=f"create:{client_order_id}",
            )
            public_group_id = str(response.get("public_group_id") or "").strip()
            operator_link = self._public_link(
                f"{self.base_url}/polling-work-order#link="
                f"{link_credentials['operator']['link_id']}.{link_credentials['operator']['secret']}"
            )
            reviewer_link = self._public_link(
                f"{self.base_url}/polling-work-order#link="
                f"{link_credentials['reviewer']['link_id']}.{link_credentials['reviewer']['secret']}"
            )
            if not re.fullmatch(r"[A-Za-z0-9_-]{8,200}", public_group_id):
                raise PollingRelayProtocolError("公网工单ID无效。")
            if not (24 <= len(management_token) <= 512):
                raise PollingRelayProtocolError("公网工单管理令牌无效。")
            return self.work_orders.update_relay(
                target_record_id,
                {
                    "relay_url": self.base_url,
                    "public_group_id": public_group_id,
                    "management_token": management_token,
                    "operator_link": operator_link,
                    "reviewer_link": reviewer_link,
                    "registration_state": "registered",
                    "registration_attempts": attempts,
                    "next_retry_at": 0,
                    "last_error": "",
                },
            )
        except Exception as exc:
            return self.work_orders.update_relay(
                target_record_id,
                {
                    "registration_state": "registration_pending",
                    "registration_attempts": attempts,
                    "next_retry_at": self.clock() + self._retry_delay(attempts),
                    "last_error": str(exc),
                },
            )

    def sync_group(self, target_record_id: str, *, force: bool = False) -> dict[str, Any]:
        group = self.work_orders.get_group(target_record_id)
        if str((group.get("relay") or {}).get("registration_state") or "") != "registered":
            group = self.register_group(target_record_id, force=force)
        relay = dict(group.get("relay") or {})
        if str(relay.get("registration_state") or "") != "registered":
            return group
        if not force and float(relay.get("next_retry_at") or 0) > self.clock():
            return group
        public_id = urllib.parse.quote(str(relay.get("public_group_id") or ""), safe="")
        token = str(relay.get("management_token") or "")
        attempts = int(relay.get("sync_attempts") or 0) + 1
        try:
            response = self._json_request(
                "GET", f"/api/v1/work-orders/{public_id}", token=token
            )
            remote_state = str(response.get("state") or "").strip().lower()
            if remote_state not in {"active", "artifact_ready", "completed", "cancelled"}:
                raise PollingRelayProtocolError("公网工单返回了未知状态。")
            artifact = response.get("artifact") if isinstance(response.get("artifact"), dict) else {}
            artifact_sha256 = str(artifact.get("sha256") or "").lower()
            if remote_state in {"artifact_ready", "completed"} and not re.fullmatch(
                r"[0-9a-f]{64}", artifact_sha256
            ):
                raise PollingRelayProtocolError("公网工单Excel缺少有效校验值。")
            changes = {
                "remote_state": remote_state,
                "remote_version": int(response.get("version") or 0),
                "artifact_name": str(artifact.get("name") or "轮巡操作流程.xlsx"),
                "artifact_sha256": artifact_sha256,
                "sync_attempts": attempts,
                "next_retry_at": 0,
                "last_error": "",
            }
            group = self.work_orders.update_relay(target_record_id, changes)
            if remote_state in {"artifact_ready", "completed"}:
                return self.work_orders.mark_public_artifact_ready(target_record_id)
            if remote_state == "cancelled":
                self.work_orders.cancel_group(target_record_id, reason="public_cancelled")
                return self.work_orders.get_group(target_record_id)
            return group
        except Exception as exc:
            if isinstance(exc, PollingRelayHttpError) and exc.status == 404:
                return self.work_orders.cancel_and_get(
                    target_record_id, reason="public_not_found"
                )
            return self.work_orders.update_relay(
                target_record_id,
                {
                    "sync_attempts": attempts,
                    "next_retry_at": self.clock() + self._retry_delay(attempts),
                    "last_error": str(exc),
                },
            )

    def download_artifact(self, target_record_id: str) -> dict[str, Any]:
        group = self.sync_group(target_record_id, force=True)
        relay = dict(group.get("relay") or {})
        if str(relay.get("remote_state") or "") not in {"artifact_ready", "completed"}:
            raise PollingRelayError("公网工单附件尚未生成。")
        public_id = urllib.parse.quote(str(relay.get("public_group_id") or ""), safe="")
        response = self._request(
            "GET",
            f"/api/v1/work-orders/{public_id}/artifact",
            token=str(relay.get("management_token") or ""),
            max_bytes=POLLING_PUBLIC_ARTIFACT_MAX_BYTES,
            timeout=45.0,
        )
        return self.work_orders.save_public_artifact(
            target_record_id,
            content=response.body,
            name=str(relay.get("artifact_name") or "轮巡操作流程.xlsx"),
            expected_sha256=str(relay.get("artifact_sha256") or ""),
        )

    def cancel_group(self, target_record_id: str, *, reason: str) -> dict[str, Any]:
        group = self.work_orders.get_group(target_record_id)
        relay = dict(group.get("relay") or {})
        public_group_id = str(relay.get("public_group_id") or "")
        token = str(relay.get("management_token") or "")
        if not public_group_id or not token:
            return self.work_orders.update_relay(
                target_record_id, {"registration_state": "cancelled", "last_error": ""}
            )
        attempts = int(relay.get("cancel_attempts") or 0) + 1
        if float(relay.get("next_retry_at") or 0) > self.clock():
            return group
        try:
            self._json_request(
                "POST",
                f"/api/v1/work-orders/{urllib.parse.quote(public_group_id, safe='')}/cancel",
                token=token,
                payload={"reason": str(reason or "local_cancelled")[:160]},
                idempotency_key=f"cancel:{public_group_id}",
            )
        except PollingRelayHttpError as exc:
            if exc.status != 404:
                return self.work_orders.update_relay(
                    target_record_id,
                    {
                        "registration_state": "cancel_pending",
                        "cancel_attempts": attempts,
                        "next_retry_at": self.clock() + self._retry_delay(attempts),
                        "last_error": str(exc),
                    },
                )
        except Exception as exc:
            return self.work_orders.update_relay(
                target_record_id,
                {
                    "registration_state": "cancel_pending",
                    "cancel_attempts": attempts,
                    "next_retry_at": self.clock() + self._retry_delay(attempts),
                    "last_error": str(exc),
                },
            )
        return self.work_orders.update_relay(
            target_record_id,
            {
                "registration_state": "cancelled",
                "operator_link": "",
                "reviewer_link": "",
                "cancel_attempts": attempts,
                "next_retry_at": 0,
                "last_error": "",
            },
        )

    def run_once(self) -> dict[str, Any]:
        synced = pending = cancelled = failed = 0
        for item in self.state_store.list_documents("polling_work_order"):
            group = item.get("payload") if isinstance(item, dict) else None
            if not isinstance(group, dict) or str((group.get("relay") or {}).get("mode") or "") != "public_service":
                continue
            target_record_id = str(group.get("target_record_id") or "")
            try:
                if str(group.get("state") or "") in {"cancelled", "stopped"}:
                    result = self.cancel_group(
                        target_record_id,
                        reason=str(group.get("cancel_reason") or "local_terminal"),
                    )
                    cancelled += int(str((result.get("relay") or {}).get("registration_state") or "") == "cancelled")
                    continue
                if str(group.get("state") or "") == "completed":
                    continue
                result = self.sync_group(target_record_id)
                if str((result.get("relay") or {}).get("registration_state") or "") == "registered":
                    synced += 1
                else:
                    pending += 1
            except Exception:
                failed += 1
        return {"enabled": True, "synced": synced, "pending": pending, "cancelled": cancelled, "failed": failed}


__all__ = [
    "POLLING_RELAY_COMMAND_NAMESPACE",
    "POLLING_RELAY_GROUP_NAMESPACE",
    "POLLING_RELAY_RUNTIME_NAMESPACE",
    "PollingRelayConfig",
    "PollingRelayConfigurationError",
    "PollingRelayError",
    "PollingRelayHttpError",
    "PollingRelayLeaseError",
    "PollingRelayProtocolError",
    "PollingWorkOrderRelayConnector",
    "PollingWorkOrderPublicClient",
    "RelayResponse",
    "UrllibPollingRelayTransport",
    "probe_polling_relay_health",
]

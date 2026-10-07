"""Bounded OpenClaw Gateway transport for trusted loopback backend clients.

Implements the official Gateway wire protocol v4 (docs.openclaw.ai/gateway/
protocol/handshake) as an asynchronous client restricted to the literal
``ws://127.0.0.1:PORT`` origin.

Design constraints (accepted upgrade):

* Async client connects ONLY to literal ``ws://127.0.0.1:port``.
* Waits a valid ``connect.challenge`` event: non-empty ``nonce`` and a
  non-negative integer ``ts``.
* Authenticates as the trusted local backend identity: client id
  ``gateway-client``, client mode ``backend``, role ``operator``, using the
  shared token.  ``device`` is intentionally omitted per the official loopback
  exception for same-process backend clients.
* Announces capability ``tool-events`` and negotiates min/max protocol ``4``.
* Readiness (usable connection) is only declared after a valid ``hello-ok``
  response that negotiates protocol ``4``; the validated payload is retained on
  the read-only :attr:`GatewayClient.hello` property.
* Concurrent RPC responses are correlated by unique request ids.
* The event queue is bounded by an aggregate byte budget; oversized / invalid
  frames reject the connection.
* Pending RPC requests are woken on disconnect.
* Per-RPC timeout and caller cancellation are propagated and cleaned up.
* Mutating RPCs are NEVER automatically resent.
* Accepted RPC responses and run terminal lifecycle events are handled
  separately: RPC results are returned by :meth:`GatewayClient.request`, while
  lifecycle/event pushes surface through :meth:`GatewayClient.next_event`.
* Exceptions never expose the token, config, or arbitrary server message/params;
  they surface a safe error code and an optional allowlisted reason /
  ``retryAfterMs`` only.
"""

from __future__ import annotations

import asyncio
import collections
import json
import re
from typing import Any, Awaitable, Callable, Deque, Dict, Optional, Set, Tuple

try:  # pragma: no cover - depends on environment
    import websockets
    from websockets.exceptions import ConnectionClosed
except Exception as exc:  # pragma: no cover
    websockets = None
    ConnectionClosed = None

__all__ = ["GatewayClient", "GatewayError", "CONNECT_CHANNEL", "REQ_ID_PREFIX"]

# ---------------------------------------------------------------------------
# Protocol constants (v4)
# ---------------------------------------------------------------------------

DEFAULT_HANDSHAKE_TIMEOUT = 15.0  # official preauth/connect-challenge budget
DEFAULT_REQUEST_TIMEOUT = 30.0  # reference client default per RPC

MAX_PREAUTH_PAYLOAD_BYTES = 64 * 1024  # 64 KiB pre-handshake cap
DEFAULT_MAX_PAYLOAD_BYTES = 25 * 1024 * 1024  # 25 MiB post-handshake default
DEFAULT_MAX_BUFFERED_BYTES = 8 * 1024 * 1024  # 8 MiB client aggregate budget

# Local ceilings: a server hello policy can never push us past these.
LOCAL_MAX_PAYLOAD_BYTES = DEFAULT_MAX_PAYLOAD_BYTES
LOCAL_MAX_BUFFERED_BYTES = DEFAULT_MAX_BUFFERED_BYTES
# Client-side aggregate byte budget for buffered events.
DEFAULT_EVENT_QUEUE_BYTES = 8 * 1024 * 1024

CLIENT_ID = "gateway-client"
CLIENT_MODE = "backend"
CLIENT_PLATFORM = "python"
CLIENT_VERSION = "lighthouse-gateway/1.0"
CLIENT_USER_AGENT = "openclaw-lighthouse-backend/1.0"
ROLE = "operator"
DEFAULT_SCOPES = ("operator.read", "operator.write")
CAPS_TOOL_EVENTS = ("tool-events",)
MIN_PROTOCOL = 4
MAX_PROTOCOL = 4

CONNECT_CHANNEL = "__lighthouse_connect__"
REQ_ID_PREFIX = "lhg-"

# Error codes we are allowed to surface to callers.  Anything else coming off
# the wire is normalized to INTERNAL_ERROR (the raw server message is dropped).
_SAFE_CODES = frozenset(
    {
        "DISCONNECTED",
        "CLOSED",
        "TIMEOUT",
        "CANCELLED",
        "NOT_CONNECTED",
        "INVALID_URL",
        "BAD_HANDSHAKE",
        "UNSUPPORTED_PROTOCOL",
        "HANDSHAKE_TIMEOUT",
        "INVALID_FRAME",
        "OVERSIZED_FRAME",
        "EVENT_QUEUE_OVERFLOW",
        "REQUEST_REJECTED",
        "AUTH_REQUIRED",
        "INVALID_PARAMS",
        "METHOD_NOT_FOUND",
        "RATE_LIMITED",
        "UNAVAILABLE",
        "FORBIDDEN",
        "NOT_FOUND",
        "CONFLICT",
        "INTERNAL_ERROR",
        "INVALID_REQUEST",
        "SESSION_NOT_FOUND",
    }
)

# Reasons from the wire that are safe to propagate (they are protocol-defined
# machine-readable tokens, never message bodies).
_ALLOWED_REASON_TOKENS = frozenset(
    {
        "startup-sidecars",
        "pairing-pending",
        "rate-limit",
        "missing-scope",
    }
)

# Patterns used to validate the loopback URL strictly.
_LOOPBACK_SCHEME = "ws"
_LOOPBACK_HOST = "127.0.0.1"
_LOOPBACK_URL_RE = re.compile(r"^ws://127\.0\.0\.1:[0-9]+$")


class GatewayError(Exception):
    """A sanitized gateway error.

    The public surface is strictly limited to:

    * ``code`` - a safe, machine-readable error code (always present).
    * ``reason`` - an optional allowlisted reason token.
    * ``retry_after_ms`` - an optional non-negative integer (ms).

    No token, configuration, raw server message, or request params ever
    appear in this exception.
    """

    __slots__ = ("code", "reason", "retry_after_ms")

    def __init__(
        self,
        code: str,
        *,
        reason: Optional[str] = None,
        retry_after_ms: Optional[int] = None,
    ) -> None:
        safe_code = code if code in _SAFE_CODES else "INTERNAL_ERROR"
        if reason is not None and reason not in _ALLOWED_REASON_TOKENS:
            reason = None
        if retry_after_ms is not None:
            try:
                retry_after_ms = int(retry_after_ms)
            except (TypeError, ValueError):
                retry_after_ms = None
            if retry_after_ms is not None and retry_after_ms < 0:
                retry_after_ms = None
        super().__init__(safe_code)
        self.code = safe_code
        self.reason = reason
        self.retry_after_ms = retry_after_ms

    def __str__(self) -> str:  # pragma: no cover - trivial
        parts = [self.code]
        if self.reason is not None:
            parts.append(self.reason)
        if self.retry_after_ms is not None:
            parts.append(f"retryAfterMs={self.retry_after_ms}")
        return " ".join(parts)


def _sanitize_server_error(error: Any) -> GatewayError:
    """Build a sanitized :class:`GatewayError` from a wire ``error`` object."""
    if isinstance(error, dict):
        code = error.get("code")
        if isinstance(code, str):
            code = code.upper().replace(" ", "_")
        else:
            code = None
        reason = error.get("reason") or error.get("details_reason")
        if not isinstance(reason, str):
            details = error.get("details")
            if isinstance(details, dict):
                reason = details.get("reason")
            elif isinstance(details, list) and details and isinstance(details[0], str):
                # Some older shapes carry details.reason as a single-element list.
                reason = details[0]
        if isinstance(reason, list) and reason and isinstance(reason[0], str):
            reason = reason[0]
        retry = error.get("retryAfterMs")
        if not isinstance(retry, (int, float)):
            retry = None
        return GatewayError(
            str(code) if code else "REQUEST_REJECTED",
            reason=str(reason) if reason else None,
            retry_after_ms=int(retry) if retry is not None else None,
        )
    return GatewayError("REQUEST_REJECTED")


# ---------------------------------------------------------------------------
# URL validation
# ---------------------------------------------------------------------------


def _validate_url(url: str) -> None:
    if not isinstance(url, str) or not url:
        raise GatewayError("INVALID_URL", reason=None)
    if not url.startswith(f"{_LOOPBACK_SCHEME}://{_LOOPBACK_HOST}:"):
        raise GatewayError("INVALID_URL", reason=None)
    if not _LOOPBACK_URL_RE.match(url):
        raise GatewayError("INVALID_URL", reason=None)
    rest = url[len(f"{_LOOPBACK_SCHEME}://{_LOOPBACK_HOST}:"):]
    port_part = rest.split("/", 1)[0]
    try:
        port = int(port_part)
    except ValueError:
        raise GatewayError("INVALID_URL", reason=None) from None
    if not (1 <= port <= 65535):
        raise GatewayError("INVALID_URL", reason=None)


# ---------------------------------------------------------------------------
# WebSocket factory helper
# ---------------------------------------------------------------------------


def _default_connect() -> Callable[..., Awaitable[Any]]:
    """Return a factory building a real websockets 15 client connection."""

    if websockets is None:  # pragma: no cover - import guard
        raise RuntimeError("websockets library is not available")

    def _factory(url: str) -> Awaitable[Any]:
        return websockets.connect(
            url,
            compression=None,
            proxy=None,  # literal loopback: never route through a proxy
            max_size=DEFAULT_MAX_PAYLOAD_BYTES + 4096,
            open_timeout=DEFAULT_HANDSHAKE_TIMEOUT,
            ping_interval=None,  # caller-owned heartbeat; do not add noise
        )

    return _factory


# ---------------------------------------------------------------------------
# GatewayClient
# ---------------------------------------------------------------------------


class GatewayClient:
    """Bounded async OpenClaw Gateway v4 transport (loopback only).

    Usage::

        async with GatewayClient(url="ws://127.0.0.1:18789", token="...") as gw:
            payload = await gw.request("tools.catalog")
            event = await gw.next_event(timeout=5.0)

    ``request`` returns the payload of an accepted response frame and does NOT
    follow run/terminal lifecycle.  Lifecycle/event pushes are consumed via
    ``next_event`` as complete ``{type, event, payload, seq?}`` envelopes.

    For isolated tests pass ``connector`` - an async callable ``async def
    connector(url) -> websocket_like`` returning an object with ``send``,
    ``recv``, ``close`` and (optionally) ``closed`` / ``close_code``.
    """

    __slots__ = (
        "_url",
        "_token",
        "_connector",
        "_ws",
        "_handshaked",
        "_closed",
        "_disconnected",
        "_disconnect_error",
        "_reader",
        "_pending",
        "_events",
        "_selected_run",
        "_event_waiters",
        "_event_bytes",
        "_event_byte_budget",
        "_max_payload",
        "_max_buffered",
        "_connect_id",
        "_req_counter",
        "_hello",
    )

    def __init__(
        self,
        url: str,
        token: str,
        connector: Optional[Callable[..., Awaitable[Any]]] = None,
    ) -> None:
        if not isinstance(token, str) or not token:
            raise GatewayError("INVALID_URL", reason=None)
        _validate_url(url)
        self._url = url
        self._token = token
        self._connector = connector
        self._ws: Any = None
        self._handshaked = False
        self._closed = False
        self._disconnected = False
        self._disconnect_error: Optional[GatewayError] = None
        self._reader: Optional[asyncio.Task] = None
        self._pending: Dict[str, asyncio.Future] = {}
        self._events: Deque[Tuple[Dict[str, Any], int]] = collections.deque()
        self._selected_run = None
        self._event_waiters: Set[asyncio.Future] = set()
        self._event_bytes = 0
        self._event_byte_budget = DEFAULT_EVENT_QUEUE_BYTES
        self._max_payload = DEFAULT_MAX_PAYLOAD_BYTES
        self._max_buffered = DEFAULT_MAX_BUFFERED_BYTES
        self._connect_id = f"{REQ_ID_PREFIX}connect"
        self._req_counter = 0
        self._hello: Optional[Dict[str, Any]] = None

    # -- context manager ----------------------------------------------------

    async def __aenter__(self) -> "GatewayClient":
        try:
            if self._connector is not None:
                self._ws = await self._connector(self._url)
            else:
                self._ws = await _default_connect()(self._url)
            await self._handshake()
            # Handshake succeeded: the connection is ready and a reader loop now
            # owns post-handshake frames.
            self._reader = asyncio.create_task(self._reader_loop())
            return self
        except BaseException:
            # Never leak the socket on handshake errors or caller cancellation.
            await self.close()
            raise

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    # -- public API ---------------------------------------------------------

    @property
    def connected(self) -> bool:
        return not self._closed and self._handshaked and not self._disconnected

    @property
    def protocol(self) -> int:
        """Negotiated protocol, only meaningful after readiness."""
        return MAX_PROTOCOL if self._handshaked else 0

    @property
    def hello(self) -> Optional[Dict[str, Any]]:
        """Validated ``hello-ok`` payload, only populated after readiness."""
        return self._hello

    def discard_events(self) -> None:
        """Drop idle/previous-run frames before a new owned run starts."""
        if self._event_waiters:
            raise RuntimeError("Cannot discard events while a run is reading")
        self._events.clear()
        self._event_bytes = 0

    def select_run(self, run_id=None):
        """Filter shared-gateway broadcasts before they enter this account's queue."""
        self.discard_events()
        self._selected_run = run_id or ''

    async def request(
        self,
        method: str,
        params: Any = None,
        timeout: Optional[float] = DEFAULT_REQUEST_TIMEOUT,
    ) -> Any:
        """Issue an RPC and await the matching response payload.

        * Concurrent requests are correlated by unique ids.
        * On timeout/cancellation the pending entry is removed; the request is
          NEVER automatically retried (especially mutating RPCs).
        * On disconnect all pending requests are woken with a safe
          ``DISCONNECTED`` error.
        """
        if not self.connected:
            raise GatewayError("DISCONNECTED")
        if not isinstance(method, str) or not method:
            raise GatewayError("REQUEST_REJECTED", reason=None)

        req_id = self._next_id()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[req_id] = fut
        frame: Dict[str, Any] = {"type": "req", "id": req_id, "method": method}
        if params is not None:
            frame["params"] = params

        try:
            try:
                await self._ws.send(json.dumps(frame, ensure_ascii=False))
            except asyncio.CancelledError:
                fut.cancel()
                raise
            except Exception:  # noqa: BLE001
                # Send failure means the socket is unusable; surface a safe
                # DISCONNECTED and never retry the RPC.
                fut.cancel()
                raise GatewayError("DISCONNECTED") from None
            if timeout is not None:
                return await asyncio.wait_for(fut, timeout)
            return await fut
        except asyncio.TimeoutError:
            fut.cancel()
            raise
        except asyncio.CancelledError:
            fut.cancel()
            raise
        except GatewayError:
            fut.cancel()
            raise
        finally:
            self._pending.pop(req_id, None)

    async def next_event(self, timeout: Optional[float] = None) -> Any:
        """Return the next complete validated event envelope.

        Envelope shape is ``{"type": "event", "event": ..., "payload": ...,
        "seq": ...}`` (``seq`` only when present on the wire).
        """
        if self._disconnected and not self._events:
            raise self._disconnect_error or GatewayError("DISCONNECTED")
        if self._events:
            envelope, size = self._events.popleft()
            self._event_bytes -= size
            return envelope

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._event_waiters.add(fut)
        try:
            if self._disconnected:
                raise self._disconnect_error or GatewayError("DISCONNECTED")
            if timeout is not None:
                return await asyncio.wait_for(fut, timeout)
            return await fut
        finally:
            self._event_waiters.discard(fut)

    async def close(self) -> None:
        """Close the connection, waking all pending callers with CLOSED."""
        if self._closed:
            return
        self._closed = True
        self._disconnected = True
        self._disconnect_error = GatewayError("CLOSED")
        if self._reader is not None and not self._reader.done():
            self._reader.cancel()
            try:
                await self._reader
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._reader = None
        self._wake_callers(self._disconnect_error)
        ws = self._ws
        if ws is not None:
            try:
                method = getattr(ws, "close", None)
                if method is not None:
                    await method()
            except (Exception, asyncio.CancelledError):  # noqa: BLE001
                pass
        self._ws = None

    # -- internals ----------------------------------------------------------

    def _next_id(self) -> str:
        self._req_counter += 1
        return f"{REQ_ID_PREFIX}{self._req_counter}"

    def _fail(self, error: GatewayError) -> None:
        """Transition the connection to a failed/closed state with a safe code."""
        if self._disconnected:
            return
        self._disconnected = True
        self._disconnect_error = error
        self._wake_callers(error)
        ws = self._ws
        if ws is not None:
            try:
                method = getattr(ws, "close", None)
                if method is not None:
                    asyncio.get_running_loop().create_task(
                        _safe_close(ws, method)
                    )
            except Exception:  # noqa: BLE001
                pass

    def _wake_callers(self, error: GatewayError) -> None:
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(error)
        self._pending.clear()
        if self._event_waiters:
            for wfut in list(self._event_waiters):
                if not wfut.done():
                    wfut.set_exception(error)
            self._event_waiters.clear()

    async def _handshake(self) -> None:
        """Perform the connect.challenge -> connect req -> hello-ok dance."""
        try:
            async with asyncio.timeout(DEFAULT_HANDSHAKE_TIMEOUT):
                # 1. Connect challenge event.
                frame = await self._recv_frame(pre_auth=True)
                self._validate_challenge(frame)

                # 2. Send the trusted backend connect request (device omitted per
                #    the official trusted-same-process loopback exception).
                connect_params = {
                    "minProtocol": MIN_PROTOCOL,
                    "maxProtocol": MAX_PROTOCOL,
                    "client": {
                        "id": CLIENT_ID,
                        "version": CLIENT_VERSION,
                        "platform": CLIENT_PLATFORM,
                        "mode": CLIENT_MODE,
                    },
                    "role": ROLE,
                    "scopes": list(DEFAULT_SCOPES),
                    "caps": list(CAPS_TOOL_EVENTS),
                    "commands": [],
                    "permissions": {},
                    "auth": {"token": self._token},
                    "locale": "en-US",
                    "userAgent": CLIENT_USER_AGENT,
                }
                request_frame = {
                    "type": "req",
                    "id": self._connect_id,
                    "method": "connect",
                    "params": connect_params,
                }
                await self._ws.send(json.dumps(request_frame, ensure_ascii=False))

                # 3. Wait for the hello-ok response (strictly protocol 4).
                hello = await self._recv_hello_ok(self._connect_id)
                self._apply_hello_policy(hello)
                self._hello = dict(hello)  # validated, read-only snapshot
                self._handshaked = True
        except asyncio.TimeoutError:
            raise GatewayError("HANDSHAKE_TIMEOUT") from None
        except GatewayError:
            raise
        except Exception:  # noqa: BLE001
            raise GatewayError("HANDSHAKE_TIMEOUT") from None

    async def _recv_frame(self, pre_auth: bool) -> Dict[str, Any]:
        try:
            raw = await self._ws.recv()
        except ConnectionClosed:
            raise GatewayError("DISCONNECTED") from None
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            raise GatewayError("DISCONNECTED") from None

        if not isinstance(raw, str):
            if isinstance(raw, (bytes, bytearray)):
                try:
                    raw = bytes(raw).decode("utf-8")
                except UnicodeDecodeError:
                    raise GatewayError("INVALID_FRAME") from None
            else:
                raise GatewayError("INVALID_FRAME")

        size = len(raw.encode("utf-8", errors="replace"))
        cap = (
            MAX_PREAUTH_PAYLOAD_BYTES
            if pre_auth
            else self._max_payload + 4096
        )
        if size > cap:
            raise GatewayError("OVERSIZED_FRAME", retry_after_ms=None)

        try:
            obj = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            raise GatewayError("INVALID_FRAME") from None

        if not isinstance(obj, dict):
            raise GatewayError("INVALID_FRAME")
        return obj

    def _validate_challenge(self, frame: Dict[str, Any]) -> None:
        if frame.get("type") != "event" or frame.get("event") != "connect.challenge":
            raise GatewayError("BAD_HANDSHAKE")
        payload = frame.get("payload")
        if not isinstance(payload, dict):
            raise GatewayError("BAD_HANDSHAKE")
        nonce = payload.get("nonce")
        if not isinstance(nonce, str) or not nonce:
            raise GatewayError("BAD_HANDSHAKE")
        ts = payload.get("ts")
        if not isinstance(ts, int) or isinstance(ts, bool) or ts < 0:
            raise GatewayError("BAD_HANDSHAKE")

    async def _recv_hello_ok(self, req_id: str) -> Dict[str, Any]:
        while True:
            frame = await self._recv_frame(pre_auth=True)
            ftype = frame.get("type")
            fid = frame.get("id")
            if ftype == "res" and fid == req_id:
                ok = frame.get("ok")
                if not isinstance(ok, bool):
                    raise GatewayError("INVALID_FRAME")
                if ok is True:
                    payload = frame.get("payload")
                    self._validate_hello_payload(payload)
                    return payload
                # ok False: sanitize error.
                raise _sanitize_server_error(frame.get("error"))
            elif ftype == "event":
                # A well-behaved v4 gateway only pushes connect.challenge and
                # then the hello-ok res.  Anything else before hello-ok is a
                # protocol violation from our client's perspective.
                raise GatewayError("BAD_HANDSHAKE")
            else:
                raise GatewayError("BAD_HANDSHAKE")

    def _validate_hello_payload(self, payload: Any) -> None:
        """Validate one hello-ok payload against the v4 HelloOk structure."""
        if not isinstance(payload, dict) or payload.get("type") != "hello-ok":
            raise GatewayError("BAD_HANDSHAKE")
        if payload.get("protocol") != MAX_PROTOCOL:
            raise GatewayError("UNSUPPORTED_PROTOCOL")

        server = payload.get("server")
        if not isinstance(server, dict):
            raise GatewayError("BAD_HANDSHAKE")
        if not isinstance(server.get("version"), str) or not server.get("version"):
            raise GatewayError("BAD_HANDSHAKE")
        if not isinstance(server.get("connId"), str) or not server.get("connId"):
            raise GatewayError("BAD_HANDSHAKE")

        features = payload.get("features")
        if not isinstance(features, dict):
            raise GatewayError("BAD_HANDSHAKE")
        methods = features.get("methods")
        events = features.get("events")
        if not isinstance(methods, list) or not all(
            isinstance(m, str) for m in methods
        ):
            raise GatewayError("BAD_HANDSHAKE")
        if not isinstance(events, list) or not all(
            isinstance(e, str) for e in events
        ):
            raise GatewayError("BAD_HANDSHAKE")

        if not isinstance(payload.get("snapshot"), dict):
            raise GatewayError("BAD_HANDSHAKE")

        auth = payload.get("auth")
        if not isinstance(auth, dict):
            raise GatewayError("BAD_HANDSHAKE")
        if not isinstance(auth.get("role"), str) or not auth.get("role"):
            raise GatewayError("BAD_HANDSHAKE")
        scopes = auth.get("scopes")
        if not isinstance(scopes, list) or not all(
            isinstance(s, str) for s in scopes
        ):
            raise GatewayError("BAD_HANDSHAKE")

        policy = payload.get("policy")
        if not isinstance(policy, dict):
            raise GatewayError("BAD_HANDSHAKE")
        for key in ("maxPayload", "maxBufferedBytes", "tickIntervalMs"):
            value = policy.get(key)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise GatewayError("BAD_HANDSHAKE")

    def _apply_hello_policy(self, hello: Dict[str, Any]) -> None:
        policy = hello.get("policy")
        if isinstance(policy, dict):
            mp = policy.get("maxPayload")
            if isinstance(mp, int) and not isinstance(mp, bool) and mp > 0:
                self._max_payload = min(int(mp), LOCAL_MAX_PAYLOAD_BYTES)
            mb = policy.get("maxBufferedBytes")
            if isinstance(mb, int) and not isinstance(mb, bool) and mb > 0:
                self._max_buffered = min(int(mb), LOCAL_MAX_BUFFERED_BYTES)

    async def _reader_loop(self) -> None:
        try:
            while not self._closed:
                frame = await self._recv_frame(pre_auth=False)
                self._dispatch(frame)
        except asyncio.CancelledError:
            raise
        except GatewayError as exc:
            self._fail(exc)
        except ConnectionClosed:
            self._fail(GatewayError("DISCONNECTED"))
        except Exception:  # noqa: BLE001
            self._fail(GatewayError("DISCONNECTED"))
        finally:
            if not self._closed:
                self._fail(GatewayError("DISCONNECTED"))

    def _dispatch(self, frame: Dict[str, Any]) -> None:
        ftype = frame.get("type")
        if ftype == "res":
            self._handle_response(frame)
            return
        if ftype == "event":
            self._handle_event(frame)
            return
        raise GatewayError("INVALID_FRAME")

    def _handle_response(self, frame: Dict[str, Any]) -> None:
        fid = frame.get("id")
        if not isinstance(fid, str) or fid not in self._pending:
            # Unknown response id: drop it (no replay/resend semantics).
            return
        ok = frame.get("ok")
        if not isinstance(ok, bool):
            # Invalid response frame: fail the connection; the pending caller
            # is woken by _fail -> _wake_callers.
            raise GatewayError("INVALID_FRAME")
        fut = self._pending.pop(fid)
        if fut.done():
            return
        if ok is True:
            fut.set_result(frame.get("payload"))
        else:
            fut.set_exception(_sanitize_server_error(frame.get("error")))

    def _handle_event(self, frame: Dict[str, Any]) -> None:
        name = frame.get("event")
        if not isinstance(name, str) or not name:
            raise GatewayError("INVALID_FRAME")
        if "payload" not in frame:
            raise GatewayError("INVALID_FRAME")
        if name in {'agent', 'chat'} and self._selected_run is not None:
            payload = frame.get('payload')
            if not isinstance(payload, dict) or payload.get('runId') != self._selected_run:
                return
        seq = frame.get("seq")
        if seq is not None and (
            not isinstance(seq, int) or isinstance(seq, bool) or seq < 0
        ):
            raise GatewayError("INVALID_FRAME")
        envelope: Dict[str, Any] = {
            "type": "event",
            "event": name,
            "payload": frame.get("payload"),
        }
        if seq is not None:
            envelope["seq"] = seq

        if self._event_waiters:
            fut = self._event_waiters.pop()
            if fut.done():
                # Cancelled/timed-out waiter: deliver to the bounded buffer so
                # the event is not lost (and never exceeds the byte budget).
                self._enqueue(envelope)
            else:
                fut.set_result(envelope)
            return
        self._enqueue(envelope)

    def _enqueue(self, envelope: Dict[str, Any]) -> None:
        size = len(json.dumps(envelope, ensure_ascii=False).encode("utf-8", errors="replace"))
        if self._event_bytes + size > self._event_byte_budget:
            # Bounded event queue: overflow is a connection fault, never an
            # unbounded buffer.
            raise GatewayError("EVENT_QUEUE_OVERFLOW")
        self._events.append((envelope, size))
        self._event_bytes += size


async def _safe_close(ws: Any, method: Callable[[], Awaitable[None]]) -> None:
    """Close a websocket without leaking exceptions into the caller."""
    try:
        await method()
    except (Exception, asyncio.CancelledError):  # noqa: BLE001
        pass

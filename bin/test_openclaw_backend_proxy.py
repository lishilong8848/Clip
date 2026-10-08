"""Bounded, TEST-ONLY integration tests for the assistant portal proxy + service.

Scope:
    * Portal authentication proxy (`lan_bitable_template_portal.lighthouse_routes`)
    * Portal business authority (`lan_bitable_template_portal.lighthouse_bridge`)
    * Independent service storage (`openclaw_service.server.Host` +
      `openclaw_service.store.AssistantStore`) and business bridge
      (`openclaw_service.bridge`)

Everything runs in memory/ASGI.  No real host, no Node, no Task Scheduler,
no cloud calls, and no live credentials are used.  A patched
`ResidentRuntime` performs register/setup against a fake in-process service,
and patched httpx transports route loopback traffic between the fake portal
and the fake service.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import hashlib
import hmac
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse
from types import SimpleNamespace
from unittest.mock import patch

import openclaw_service.client as openclaw_client
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_appearance import DEFAULT_APPEARANCE
from openclaw_service.bridge import OPERATION, PortalBridge
from openclaw_service.protocol import (
    PROTOCOL,
    PROJECT,
    atomic_json,
    control_key,
    identity,
    process_stamp,
)
from openclaw_service.server import Host, build_app
from openclaw_service.assistant.lighthouse_ai import ENDPOINT, MODEL, protect_key

from lan_bitable_template_portal.lighthouse_bridge import PortalAuthority
from lan_bitable_template_portal.lighthouse_routes import install_lighthouse_routes

from test_openclaw_service import FakeManager

# Capture the real httpx client classes before any test patches them so the
# patched factories can forward to the genuine implementation (avoids recursion).
_REAL_ASYNC = httpx.AsyncClient
_REAL_SYNC = httpx.Client
# Capture the genuine ResidentRuntime so one test can exercise the real class
# (health + register) while the global world patch keeps the portal handshake
# on the fake runtime.
_REAL_RUNTIME = openclaw_client.ResidentRuntime

ALICE = "actor-alice"
BOB = "actor-bob"
ADMIN = "actor-admin"
GUEST = "actor-guest"

_SERVICE_KEY = "synthetic-service-key-" + "x" * 40


# ---------------------------------------------------------------------------
# ASGI transport helpers
# ---------------------------------------------------------------------------
class RoutingTransport(httpx.AsyncBaseTransport):
    """Route each request to the right in-process ASGI app by port."""

    def __init__(self, routes, on_request=None):
        self._routes = {int(port): httpx.ASGITransport(app=app) for port, app in routes.items()}
        # Optional test hook (fires on the async loop) used to capture the
        # x-clipflow-context header the portal proxy sends to the service.
        self.on_request = on_request

    async def handle_async_request(self, request):
        if self.on_request is not None:
            self.on_request(request)
        transport = self._routes.get(request.url.port)
        if transport is None:
            raise httpx.ConnectError("no routing for port %d" % request.url.port, request=request)
        return await transport.handle_async_request(request)


class RoutingSyncTransport(httpx.BaseTransport):
    """Real sync-to-loop adapter for worker threads (e.g. PortalBridge.call).

    httpx.ASGITransport has no synchronous ``handle_request``, so this adapter
    builds a fresh async request with the buffered body and runs the async ASGI
    round-trip on the captured test event loop via
    `asyncio.run_coroutine_threadsafe`.  It refuses to run on the same loop it
    is bound to (which would deadlock and indicates an async context is being
    misused as sync).
    """

    def __init__(self, async_transport):
        self._async = async_transport
        self._loop = None

    def bind_loop(self, loop):
        self._loop = loop

    def handle_request(self, request):
        loop = self._loop
        if loop is None:
            raise RuntimeError("RoutingSyncTransport is not bound to an event loop")
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None  # called from a worker thread
        if running is loop:
            raise RuntimeError("RoutingSyncTransport must only be used from worker threads, not the bound loop")

        async def roundtrip():
            async_request = httpx.Request(
                request.method, str(request.url), headers=request.headers, content=request.read()
            )
            async_response = await self._async.handle_async_request(async_request)
            try:
                await async_response.aread()
            except httpx.ResponseNotRead:
                pass
            return async_response

        future = asyncio.run_coroutine_threadsafe(roundtrip(), loop)
        async_response = future.result(timeout=30)
        content = async_response.content if async_response.content is not None else b""
        return httpx.Response(
            async_response.status_code,
            headers=async_response.headers,
            content=content,
            request=request,
            extensions=async_response.extensions or {},
        )


def _async_factory(routed):
    """Return a real-compatible subclass that injects the RoutingTransport.

    A plain function would break third-party imports that alias
    `httpx.AsyncClient` (e.g. pydantic_ai performs
    `legacy_httpx.AsyncClient | httpx2.AsyncClient` at import time and requires
    the attribute to remain a type).  Returning a subclass keeps it a class
    while still injecting our transport when the caller passed none.
    """

    class RoutingAsyncClient(_REAL_ASYNC):
        def __init__(self, *args, **kwargs):
            if "transport" not in kwargs:
                kwargs["transport"] = routed
            super().__init__(*args, **kwargs)

    return RoutingAsyncClient


def _sync_factory(routed, loop=None):
    # The sync adapter must bridge a worker thread onto the async test loop.
    sync_adapter = RoutingSyncTransport(routed)
    if loop is not None:
        sync_adapter.bind_loop(loop)

    class RoutingSyncClient(_REAL_SYNC):
        def __init__(self, *args, **kwargs):
            if "transport" not in kwargs:
                kwargs["transport"] = sync_adapter
            super().__init__(*args, **kwargs)

    return RoutingSyncClient


# ---------------------------------------------------------------------------
# Fake portal controller / runtime and a fake service client
# ---------------------------------------------------------------------------
class FakeController:
    def __init__(self, world):
        self._world = world
        self.bound_port = world.portal_port
        self.preferred_port = world.portal_port

    def _current_session(self, request):
        sid = request.cookies.get("sid")
        if not sid:
            return None
        return copy.deepcopy(world_sessions(self._world).get(sid))

    def _request_base_url(self, request):
        return "http://127.0.0.1:%d/" % (self.bound_port or self.preferred_port)


def world_sessions(world):
    return world.sessions


class FakeResidentRuntime:
    """Replacement for openclaw_service.client.ResidentRuntime.

    Never spawns a process or negotiates over a real socket.  If a Host is
    supplied, ``prepare()`` performs register + setup against that in-process
    Host (which still does the real migration and portal-catalog handshake).
    """

    resident = True

    def __init__(self, state_root, *, host=None, callback_url=None, legacy_db=None,
                 instance=None, lease=None, key=None):
        self.host = host
        self.root = Path(state_root).resolve()
        self.callback_url = callback_url
        self.legacy_db = legacy_db
        self.portal_id = uuid.uuid4().hex
        self.instance = instance or (host.instance if host else None)
        self.lease = lease
        self.key = key or (host.key if host else None)
        self.accounts = {}
        self.closing = False
        self.disconnected = False
        self.lock = asyncio.Lock()
        self.http_lock = asyncio.Lock()
        self.http = None
        self.monitor = None
        self.descriptor = {"port": host.port if host else None, "instance": self.instance}

    http_client = _REAL_RUNTIME.http_client

    async def prepare(self, *, progress=None):
        if self.host is not None:
            async with self.lock:
                if self.lease is None:
                    registered = await self.host.register({
                        "portal_id": self.portal_id,
                        "pid": os.getpid(),
                        "process_stamp": process_stamp(os.getpid()),
                        "callback_url": self.callback_url() if callable(self.callback_url) else self.callback_url,
                        "digest": self.host.digest,
                        "assistant_backend": True,
                        "legacy_db": str(self.legacy_db) if self.legacy_db else None,
                    })
                    self.lease = registered["lease"]
                    self.instance = registered["instance"] or self.host.instance
                    self.descriptor = {"port": self.host.port, "instance": self.host.instance}
                    await self.host.setup_backend({"instance": self.instance, "lease": self.lease})
        return {"instance": self.instance}

    async def close(self):
        self.closing = True
        if self.http is not None:
            await self.http.aclose()


def runtime_factory_for(world):
    def factory(state_root, *, callback_url=None, project=None, launch=None,
                legacy_db=None, assistant_backend=False):
        return FakeResidentRuntime(state_root, host=world.host, callback_url=callback_url,
                                   legacy_db=legacy_db,
                                   instance=world.host.instance if world.host else None,
                                   key=world.host.key if world.host else None)

    return factory


def real_runtime_factory_for(world):
    """Factory replaying the real ResidentRuntime lifecycle (health -> register ->
    setup) against the injected ASGI stack. ``launch`` always raises so any attempt
    to spawn a real process fails the test loudly."""
    def deny_launch(*args, **kwargs):
        raise AssertionError("ResidentRuntime.launch must never be called in isolated tests")

    def factory(state_root, *, callback_url=None, project=None, launch=None,
                legacy_db=None, assistant_backend=False):
        runtime = _REAL_RUNTIME(
            state_root,
            callback_url=callback_url,
            project=project or PROJECT,
            launch=deny_launch,
            legacy_db=legacy_db,
            assistant_backend=assistant_backend,
        )
        world.real_runtime = runtime
        return runtime

    return factory


# ---------------------------------------------------------------------------
# Test world construction
# ---------------------------------------------------------------------------
def build_legacy_db(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE json_documents ("
            "namespace TEXT NOT NULL, key TEXT NOT NULL, payload_json TEXT NOT NULL,"
            "updated_at REAL NOT NULL, PRIMARY KEY(namespace, key))"
        )
        appearance = dict(DEFAULT_APPEARANCE)
        appearance["color"] = "encre"
        conn.execute(
            "INSERT INTO json_documents(namespace, key, payload_json, updated_at) VALUES (?,?,?,?)",
            ("lighthouse_appearance", ALICE, json.dumps(appearance, ensure_ascii=False), time.time()),
        )
        conn.commit()
    finally:
        conn.close()


class World:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.portal_port = 48601
        self.service_port = 48602
        # Production layout: the portal runtime stores the legacy window store under
        # <data>/lan_portal_state.sqlite3 and the assistant service keeps its own
        # state (including assistant.sqlite3) under <data>/lighthouse_openclaw.
        # The portal proxy derives the assistant root as the sibling directory of
        # the legacy store, so these two must stay consistent for a faithful test.
        self.legacy_db = self.directory / "data" / "lan_portal_state.sqlite3"
        self.service_state = self.directory / "data" / "lighthouse_openclaw"
        self.runtime_root = self.service_state
        self.probe_count = {"probe": 0, "commit": 0}

        build_legacy_db(self.legacy_db)

        self.sessions = {
            "alice": {"open_id": ALICE, "scopes": ["A", "B"], "is_admin": False},
            "bob": {"open_id": BOB, "scopes": ["D"], "is_admin": False},
            "admin": {"open_id": ADMIN, "scopes": ["ALL"], "is_admin": True},
            "guest": {"open_id": GUEST, "scopes": [], "is_guest": True},
            "reduced": {"open_id": ALICE, "scopes": ["A"], "is_admin": False},
        }

        self.controller = FakeController(self)
        self.runtime = SimpleNamespace(
            state_store=SimpleNamespace(db_path=str(self.legacy_db)),
            auth_manager=SimpleNamespace(
                session_scopes=lambda session: list(session.get("scopes", [])),
                is_admin=lambda session: bool(session.get("is_admin", False)),
            ),
            learning_service=None,
        )

        self.manager = FakeManager()
        # Proxy fixture uses a real DPAPI-encrypted control key stored under the
        # temp service state (never the synthetic constant).
        self.host = Host(state=str(self.service_state), key=control_key(self.service_state), manager=self.manager)
        self.host.port = self.service_port
        self._clients = []
        self.host_key = self.host.key
        self.real_runtime = None

        # Fake portal application.
        self.portal_app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
        install_lighthouse_routes(self.portal_app, self.controller, self.runtime)
        self._add_business_routes()
        self.portal_app.add_api_route("/api/health", self._health, methods=["GET"], name="fake_health")

        # Fake service application.
        self.service_app = build_app(self.host)

        routes = {self.service_port: self.service_app, self.portal_port: self.portal_app}

        def on_request(request):
            headers = dict(request.headers)
            context = request.url.path
            if request.url.path == "/api/assistant/appearance" or request.url.path.startswith("/api/assistant/stream"):
                self.last_service_headers = headers.copy()
            if headers.get("x-clipflow-context"):
                self.last_context_header = headers.get("x-clipflow-context")

        # A pluggable "failure" state: route -> raises ConnectionError (used to
        # simulate service interruption through the browser proxy).
        self.failure_ports = set()

        async def routed_handle(request, _transport, port):
            if port in self.failure_ports:
                raise httpx.ConnectError("simulated service connection failure", request=request)
            return await _transport.handle_async_request(request)

        orig_routes = dict(routes)

        class _RoutingTransport(RoutingTransport):
            def __init__(self, routed, owner):
                super().__init__(routed, on_request=owner._on_request_hook)
                self._owner = owner

            async def handle_async_request(self, request):
                if self._owner.failure_ports and request.url.port in self._owner.failure_ports:
                    raise httpx.ConnectError("simulated service connection failure", request=request)
                return await super().handle_async_request(request)

        self._routed_async = _RoutingTransport(orig_routes, self)
        self._routed_sync = RoutingSyncTransport(self._routed_async)
        self.async_factory = _async_factory(self._routed_async)
        self.sync_factory = _sync_factory(self._routed_sync)
        self.runtime_factory = runtime_factory_for(self)

    def _on_request_hook(self, request):
        headers = dict(request.headers)
        if headers.get("x-clipflow-context"):
            self.last_context_header = headers.get("x-clipflow-context")

    # -- synthetic business routes scanned into the portal catalogue ----------
    def _add_business_routes(self):
        async def biz_probe(request: Request):
            self.probe_count["probe"] += 1
            return JSONResponse({"probe": True, "count": self.probe_count["probe"]})

        async def biz_commit(request: Request):
            self.probe_count["commit"] += 1
            return JSONResponse({"committed": True, "count": self.probe_count["commit"]})

        self.portal_app.add_api_route("/api/business/probe", biz_probe, methods=["GET"],
                                      name="fake_business_probe")
        self.portal_app.add_api_route("/api/business/commit", biz_commit, methods=["POST"],
                                      name="fake_business_commit")

    async def _health(self):
        return JSONResponse({"ok": True})

    @contextlib.asynccontextmanager
    async def activate(self, *, real_runtime=False):
        """Context manager installing all runtime patches for this world.

        When ``real_runtime`` is true the genuine ``ResidentRuntime`` lifecycle is
        replayed (launch always raises); otherwise a fake resident runtime avoids
        any process spawning for the routine proxy fixtures.
        """
        stack = contextlib.ExitStack()
        loop = asyncio.get_running_loop()
        factory = real_runtime_factory_for(self) if real_runtime else self.runtime_factory
        stack.enter_context(patch.object(openclaw_client, "ResidentRuntime", factory))
        stack.enter_context(patch("httpx.AsyncClient", self.async_factory))
        stack.enter_context(patch("httpx.Client", _sync_factory(self._routed_async, loop=loop)))
        # Restore any pre-existing engine override rather than blindly removing it.
        env_value = os.environ.get("LIGHTHOUSE_AGENT_ENGINE")
        os.environ["LIGHTHOUSE_AGENT_ENGINE"] = "legacy"
        stack.callback(lambda: _restore_env(env_value))
        try:
            yield self
        finally:
            stack.close()
            if getattr(self, "real_runtime", None) is not None:
                try:
                    await self.real_runtime.close()
                except Exception:  # noqa: BLE001
                    pass
                self.real_runtime = None
            await self.close_clients()

    async def close_clients(self):
        for client in self._clients:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001
                pass
        self._clients.clear()

    # -- clients --------------------------------------------------------------
    def portal_client(self):
        client = _REAL_ASYNC(transport=httpx.ASGITransport(app=self.portal_app),
                             base_url="http://127.0.0.1:%d" % self.portal_port)
        self._clients.append(client)
        return client

    def service_client(self, *, with_key=True):
        headers = {"Authorization": "Bearer " + self.host.key} if with_key else {}
        client = _REAL_ASYNC(transport=httpx.ASGITransport(app=self.service_app),
                             base_url="http://127.0.0.1:%d" % self.service_port,
                             headers=headers)
        self._clients.append(client)
        return client

    def routed_sync_client(self):
        """A real sync httpx.Client routed through the in-process apps.

        Used from worker threads (e.g. `asyncio.to_thread`) to exercise the real
        synchronous `PortalBridge.call` path.
        """
        client = _REAL_SYNC(transport=self._routed_sync, base_url="http://127.0.0.1:%d" % self.service_port)
        # closing a sync client is a no-op for ownership purposes; the transport
        # is shared and lives for the duration of the world.
        return client

    async def warm(self, client):
        """Trigger the first portal request so register + setup completes and assert it worked."""
        response = await client.get("/api/assistant/appearance", cookies={"sid": "alice"})
        _assert_warm(response)


def _restore_env(value):
    if value is None:
        os.environ.pop("LIGHTHOUSE_AGENT_ENGINE", None)
    else:
        os.environ["LIGHTHOUSE_AGENT_ENGINE"] = value


def _assert_warm(response):
    if response.status_code != 200:
        raise AssertionError("warm-up failed: %s %s" % (response.status_code, response.text))
    body = response.json()
    if not body.get("ok"):
        raise AssertionError("warm-up response did not report ok=True: %r" % (body,))
    content_type = response.headers.get("content-type", "")
    if "application/json" not in content_type:
        raise AssertionError("warm-up response was not JSON: %r" % content_type)


class BusinessWorld:
    """Self-contained portal authority + assistant host used to exercise the real
    service->portal business bridge (``PortalBridge.acall``/``RemoteCatalog``).

    Unlike :class:`World` (which tests the browser proxy route-to-route flow),
    this world wires a directly-constructed :class:`PortalAuthority` to a test
    portal app so the tests can create their own plan-granted contexts, while
    still driving the genuine ``Host``/``PortalBridge``/``RemoteCatalog`` stack
    over the injected ASGI routing transport.
    """

    def __init__(self, directory):
        self.directory = Path(directory)
        self.portal_port = 48701
        self.service_port = 48702
        self.legacy_db = self.directory / "data" / "lan_portal_state.sqlite3"
        self.service_state = self.directory / "data" / "lighthouse_openclaw"
        build_legacy_db(self.legacy_db)
        self.sessions = {
            "alice": {"open_id": ALICE, "scopes": ["A", "B"], "is_admin": False},
            "bob": {"open_id": BOB, "scopes": ["D"], "is_admin": False},
        }
        self.probe_count = {"probe": 0, "commit": 0}
        self.uploads_received = []
        self.drop_callback_responses = False
        self._clients = []

        self.controller = _StringController(self)
        self.runtime = SimpleNamespace(
            state_store=SimpleNamespace(db_path=str(self.legacy_db)),
            auth_manager=SimpleNamespace(
                session_scopes=lambda session: list(session.get("scopes", [])),
                is_admin=lambda session: bool(session.get("is_admin", False)),
            ),
            learning_service=None,
        )

        self.manager = FakeManager()
        self.host = Host(state=str(self.service_state), key=control_key(self.service_state), manager=self.manager)
        self.host.port = self.service_port
        self.host_key = self.host.key
        self.client_root = self.service_state

        self.portal_app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
        self._add_business_routes()
        self._add_service_bridge()

        self.client = FakeResidentRuntime(
            self.client_root,
            callback_url=lambda: "http://127.0.0.1:%d/api/assistant/service-bridge" % self.portal_port,
            legacy_db=self.legacy_db,
            instance=self.host.instance,
            key=self.host.key,
        )
        # PortalAuthority binds the runtime's instance/lease for auth; set them
        # to the host lease once registration happens in activate().
        self.authority = PortalAuthority(self.portal_app, self.controller, self.runtime, self.client)

        routes = {self.portal_port: self.portal_app}
        self._routed_async = _BusinessRoutingTransport(dict(routes), self)
        self.async_factory = _async_factory(self._routed_async)
        self.sync_factory = _sync_factory(RoutingSyncTransport(self._routed_async))

    def _add_business_routes(self):
        async def biz_probe(request: Request):
            self.probe_count["probe"] += 1
            return JSONResponse({"probe": True, "count": self.probe_count["probe"]})

        async def biz_commit(request: Request):
            self.probe_count["commit"] += 1
            return JSONResponse({"committed": True, "count": self.probe_count["commit"]})

        async def biz_proof(attachment: UploadFile = File(...), scope: str = Form(...)):
            content = await attachment.read()
            self.uploads_received.append({
                "name": attachment.filename,
                "content": content,
                "scope": scope,
            })
            return JSONResponse({"ok": True, "data": {"uploads": len(self.uploads_received)}})

        self.portal_app.add_api_route("/api/business/probe", biz_probe, methods=["GET"], name="biz_probe")
        self.portal_app.add_api_route("/api/business/commit", biz_commit, methods=["POST"], name="biz_commit")
        self.portal_app.add_api_route("/api/business/proof", biz_proof, methods=["POST"], name="biz_proof")

    def _add_service_bridge(self):
        async def service_bridge(request: Request):
            authorization = request.headers.get("authorization", "")
            token = authorization[7:] if authorization.startswith("Bearer ") else ""
            if (
                not request.client or request.client.host != "127.0.0.1" or request.headers.get("origin")
                or not self.client.key or not hmac.compare_digest(token, self.client.key)
            ):
                raise AssistantError("内部业务通道认证未通过。", 403)
            message = await request.json()
            data = await self.authority.dispatch(message)
            return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})

        self.portal_app.add_api_route(
            "/api/assistant/service-bridge", service_bridge, methods=["POST"], name="business_world_service_bridge"
        )

    @contextlib.asynccontextmanager
    async def activate(self):
        stack = contextlib.ExitStack()
        loop = asyncio.get_running_loop()
        stack.enter_context(patch("httpx.AsyncClient", self.async_factory))
        stack.enter_context(patch("httpx.Client", _sync_factory(self._routed_async, loop=loop)))
        env_value = os.environ.get("LIGHTHOUSE_AGENT_ENGINE")
        os.environ["LIGHTHOUSE_AGENT_ENGINE"] = "legacy"
        stack.callback(lambda: _restore_env(env_value))
        try:
            # Register the host against this test portal and complete the real
            # setup handshake (store migration + catalogue) over the bridge.
            registered = await self.host.register({
                "portal_id": self.client.portal_id,
                "pid": os.getpid(),
                "process_stamp": process_stamp(os.getpid()),
                "callback_url": "http://127.0.0.1:%d/api/assistant/service-bridge" % self.portal_port,
                "digest": self.host.digest,
                "assistant_backend": True,
                "legacy_db": str(self.legacy_db),
            })
            self.client.lease = registered["lease"]
            self.client.instance = registered["instance"] or self.host.instance
            self.client.descriptor = {"port": self.host.port, "instance": self.host.instance}
            await self.host.setup_backend({"instance": self.client.instance, "lease": self.client.lease})
            yield self
        finally:
            stack.close()
            for client in self._clients:
                try:
                    await client.aclose()
                except Exception:  # noqa: BLE001
                    pass
            self._clients.clear()
            await self.host.close()

    def make_context(self, *, sid="alice", plan_id=None):
        request = build_request(sid, base_port=self.portal_port)
        actor = {
            "id": self.sessions[sid]["open_id"],
            "is_admin": False,
            "scopes": list(self.sessions[sid]["scopes"]),
            "can_manage_settings": True,
            "learning_scopes": [],
        }
        return self.authority.context(request, actor, plan_id=plan_id)

    def upload_item(self, identity, *, owner=ALICE, scopes=("A", "B"), content=b"hello attachment body"):
        root = self.client_root / "files"
        root.mkdir(parents=True, exist_ok=True)
        path = root / (identity + ".txt")
        path.write_bytes(content)
        sha256 = hashlib.sha256(content).hexdigest()
        return {
            "id": identity,
            "owner": owner,
            "name": "proof.txt",
            "mime": "text/plain",
            "path": str(path),
            "sha256": sha256,
            "size": len(content),
            "source_scopes": list(scopes),
        }


class _StringController:
    """Minimal controller used by BusinessWorld: session lookup for a real request."""

    def __init__(self, world):
        self._world = world
        self.bound_port = world.portal_port
        self.preferred_port = world.portal_port

    def _current_session(self, request):
        sid = request.cookies.get("sid")
        if not sid:
            return None
        return copy.deepcopy(self._world.sessions.get(sid))

    def _request_base_url(self, request):
        return "http://127.0.0.1:%d/" % self.bound_port


class _BusinessRoutingTransport(RoutingTransport):
    """Routing transport that can drop the portal callback response after the
    portal-side authority has already committed its mutation."""

    def __init__(self, routes, owner):
        super().__init__(routes)
        self._owner = owner

    async def handle_async_request(self, request):
        # Run the portal app (mutation happens), buffer, then optionally drop the
        # response to simulate a real callback loss after a successful commit.
        transport = self._routes.get(request.url.port)
        if transport is None:
            raise httpx.ConnectError("no routing for port %d" % request.url.port, request=request)
        response = await transport.handle_async_request(request)
        if self._owner.drop_callback_responses and request.url.path.endswith("/api/assistant/service-bridge"):
            await response.aread()
            raise httpx.ConnectError("simulated portal callback response loss after mutation", request=request)
        return response


def build_request(sid, *, method="POST", path="/api/assistant/appearance",
                  origin=None, query=b"", base_port=48601):
    headers = []
    if sid:
        headers.append((b"cookie", ("sid=" + sid).encode()))
    if origin:
        headers.append((b"origin", origin.encode()))
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": method,
        "path": path,
        "raw_path": path.encode(),
        "query_string": query,
        "headers": headers,
        "scheme": "http",
        "server": ("127.0.0.1", base_port),
        "client": ("127.0.0.1", 54321),
        "root_path": "",
        "app": None,
        "state": {},
    }
    return Request(scope)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestAssistantBackendProxy(unittest.IsolatedAsyncioTestCase):
    async def test_notice_retry_is_forwarded_and_still_owner_protected(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            async with world.activate():
                client = world.portal_client()
                await world.warm(client)
                conversation = await client.get('/api/assistant/conversation', cookies={'sid': 'alice'})
                plan_id = uuid.uuid4().hex
                world.host.store.put_document('lighthouse_agent_plans', plan_id, {
                    'id': plan_id, 'owner': ALICE, 'conversation_id': conversation.json()['data']['conversation_id'],
                    'turn_id': 'old-notice', 'scopes': ['A', 'B'], 'status': 'superseded', 'version': 1,
                    'title': 'Retired notice', 'fields': [], 'error': '', 'results': [],
                    'operations': [{'api_id': 'POST /api/workbench-actions'}],
                })
                for sid, expected in (('alice', 200), ('bob', 404), ('reduced', 403)):
                    response = await client.post('/api/assistant/plans/' + plan_id + '/retry',
                        cookies={'sid': sid}, headers={'origin': 'http://127.0.0.1:' + str(world.portal_port)}, json={'version': 1})
                    self.assertEqual(response.status_code, expected, response.text)
                    if expected == 200:
                        self.assertEqual(response.json()['data']['status'], 'superseded')
                self.assertEqual(world.probe_count['commit'], 0)

    async def asyncSetUp(self):
        self._engine_env = os.environ.get("LIGHTHOUSE_AGENT_ENGINE")
        os.environ["LIGHTHOUSE_AGENT_ENGINE"] = "legacy"

    async def asyncTearDown(self):
        host = getattr(self, "_host_for_cleanup", None)
        if host is not None:
            try:
                await host.close()
            except Exception:  # noqa: BLE001
                pass
        _restore_env(getattr(self, "_engine_env", None))

    # -- 1. appearance stored only in service assistant.sqlite3 ----------------
    async def test_appearance_stored_only_in_service_sqlite(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            async with world.activate():
                client = world.portal_client()
                await world.warm(client)

                put = await client.put("/api/assistant/appearance",
                                       json={"color": "rouge"},
                                       cookies={"sid": "alice"},
                                       headers={"Origin": "http://127.0.0.1:%d" % world.portal_port})
                self.assertEqual(put.status_code, 200, put.text)
                data = put.json()["data"]
                self.assertEqual(data["color"], "rouge")

                got = await client.get("/api/assistant/appearance", cookies={"sid": "alice"})
                self.assertEqual(got.json()["data"]["color"], "rouge")

                # Service store owns the value; legacy DB stays untouched.
                self.assertEqual(world.host.store.get_document(
                    "lighthouse_appearance", ALICE)["color"], "rouge")
                self.assertTrue((world.host.state / "assistant.sqlite3").is_file())

                legacy = sqlite3.connect(str(world.legacy_db))
                try:
                    row = legacy.execute(
                        "SELECT payload_json FROM json_documents WHERE namespace=? AND key=?",
                        ("lighthouse_appearance", ALICE)).fetchone()
                finally:
                    legacy.close()
                self.assertIsNotNone(row)
                self.assertEqual(json.loads(row[0])["color"], "encre")
                self.assertNotIn("rouge", row[0])

    # -- 2. model settings per account, no plaintext credentials --------------
    async def test_model_settings_per_account_no_plaintext(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            async with world.activate():
                client = world.portal_client()
                await world.warm(client)

                secret = "PLAIN-SECRET-TOPSECRET-12345"
                body = {
                    "action": "upsert",
                    "profile": {
                        "id": "model-x",
                        "name": "X",
                        "endpoint": "https://example.com/v1/chat/completions",
                        "model": "gpt-x",
                        "api_key": secret,
                    },
                }
                put = await client.put(
                    "/api/assistant/settings", json=body, cookies={"sid": "alice"},
                    headers={"Origin": "http://127.0.0.1:%d" % world.portal_port,
                             "content-type": "application/json"})
                self.assertEqual(put.status_code, 200, put.text)
                self.assertNotIn(secret, put.text)
                self.assertNotIn("key_cipher", put.json()["data"].get("models", [{}])[0] if put.json()["data"] else {})
                models = put.json()["data"]["models"]
                configured = [m for m in models if m["id"] == "model-x"]
                self.assertTrue(configured and configured[0]["configured"] is True)

                # GET never leaks the plaintext or ciphertext.
                got = await client.get("/api/assistant/settings", cookies={"sid": "alice"})
                self.assertEqual(got.status_code, 200)
                self.assertNotIn(secret, got.text)

                # Per-account isolation: bob still has an unconfigured default.
                bob_got = await client.get("/api/assistant/settings", cookies={"sid": "bob"})
                self.assertEqual(bob_got.status_code, 200)
                active = bob_got.json()["data"]
                self.assertTrue(active["configured"] is False)

                # Ciphertext, not plaintext, is what is persisted.
                saved = world.host.store.get_document("lighthouse_ai", "model:" + ALICE)
                self.assertIsNotNone(saved)
                model = next(m for m in saved["models"] if m["id"] == "model-x")
                self.assertTrue(model.get("key_cipher"))
                self.assertNotIn(secret, json.dumps(model))

    # -- 3. forged query actor id cannot switch account ------------------------
    async def test_shared_models_require_admin_and_formal_unassigned_user_is_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            world.sessions['unassigned'] = {'open_id': 'formal-unassigned', 'scopes': [], 'is_admin': False}
            async with world.activate():
                client = world.portal_client()
                await world.warm(client)
                headers = {'Origin': 'http://127.0.0.1:%d' % world.portal_port}
                body = {'scope': 'shared', 'action': 'upsert', 'profile': {'id': 'team-model', 'name': 'Shared fixture',
                    'endpoint': 'https://example.com/v1/chat/completions', 'model': 'fixture', 'api_key': 'fixture-secret-only'}}
                denied = await client.put('/api/assistant/settings', json=body, cookies={'sid': 'alice'}, headers=headers)
                self.assertEqual(denied.status_code, 403, denied.text)
                saved = await client.put('/api/assistant/settings', json=body, cookies={'sid': 'admin'}, headers=headers)
                self.assertEqual(saved.status_code, 200, saved.text)
                self.assertNotIn('fixture-secret-only', saved.text)
                for sid in ('alice', 'bob', 'unassigned'):
                    response = await client.get('/api/assistant/settings', cookies={'sid': sid})
                    self.assertEqual(response.status_code, 200, response.text)
                    data = response.json()['data']
                    self.assertFalse(data['can_manage_shared'])
                    self.assertTrue(any(m['id'] == 'shared_team-model' and m['configured'] for m in data['models']))
                    conversation = await client.get('/api/assistant/conversation', cookies={'sid': sid})
                    self.assertEqual(conversation.status_code, 200, conversation.text)
                    self.assertTrue(conversation.json()['data']['configured'])
                removed = await client.put('/api/assistant/settings', json={'scope': 'shared', 'action': 'delete',
                    'id': 'shared_team-model'}, cookies={'sid': 'admin'}, headers=headers)
                self.assertEqual(removed.status_code, 200, removed.text)
                response = await client.get('/api/assistant/settings', cookies={'sid': 'bob'})
                self.assertNotIn('shared_team-model', response.text)
                guest = await client.get('/api/assistant/settings', cookies={'sid': 'guest'})
                self.assertEqual(guest.status_code, 403)

    async def test_forged_query_actor_cannot_switch_account(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            async with world.activate():
                client = world.portal_client()
                await world.warm(client)

                await client.put(
                    "/api/assistant/appearance", json={"color": "rouge"},
                    cookies={"sid": "alice"},
                    headers={"Origin": "http://127.0.0.1:%d" % world.portal_port})

                forged = await client.get(
                    "/api/assistant/appearance",
                    params={"actor": BOB},
                    cookies={"sid": "alice"})
                self.assertEqual(forged.status_code, 200)
                self.assertEqual(forged.json()["data"]["color"], "rouge")
                self.assertEqual(world.host.store.get_document(
                    "lighthouse_appearance", ALICE)["color"], "rouge")

    # -- 4. same-origin / guest / unauth / cross-origin rules ------------------
    async def test_browser_guest_unauth_and_cross_origin_denied(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            async with world.activate():
                client = world.portal_client()
                await world.warm(client)

                unauth = await client.get("/api/assistant/appearance")
                self.assertIn(unauth.status_code, (401, 403))

                guest = await client.get("/api/assistant/appearance", cookies={"sid": "guest"})
                self.assertEqual(guest.status_code, 403)

                cross = await client.put(
                    "/api/assistant/appearance", json={"color": "vert"},
                    cookies={"sid": "alice"},
                    headers={"Origin": "http://evil.example"})
                self.assertEqual(cross.status_code, 403)

                ok = await client.put(
                    "/api/assistant/appearance", json={"color": "vert"},
                    cookies={"sid": "alice"},
                    headers={"Origin": "http://127.0.0.1:%d" % world.portal_port})
                self.assertEqual(ok.status_code, 200, ok.text)

    # -- 5. direct service without bearer + instance + lease + context ---------
    async def test_direct_service_requires_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            async with world.activate():
                client = world.service_client(with_key=False)
                resp = await client.get("/api/assistant/appearance")
                self.assertIn(resp.status_code, (401, 403, 409))

                authed = world.service_client(with_key=True)
                resp2 = await authed.get("/api/assistant/appearance")
                self.assertIn(resp2.status_code, (401, 403, 409))

    # -- 6. service interruption through browser proxy: assistant down, health ok --
    async def test_service_interruption_affects_assistant_but_not_health(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            async with world.activate():
                client = world.portal_client()
                await world.warm(client)

                # Interrupt the assistant backend at the exact routing layer so the
                # browser-facing portal proxy is the path that observes the loss.
                world.failure_ports.add(world.service_port)

                assistant = await client.get("/api/assistant/appearance", cookies={"sid": "alice"})
                self.assertEqual(assistant.status_code, 503)

                # The portal's own dummy health endpoint remains usable.
                health = await client.get("/api/health")
                self.assertEqual(health.status_code, 200)

    # -- 7. logout / scope revoke invalidates old context ----------------------
    async def test_logout_and_scope_revoke_invalidate_old_context(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            runtime = FakeResidentRuntime(
                Path(directory), instance="i" * 32, lease="lease-test-1", key=_SERVICE_KEY)
            authority = PortalAuthority(world.portal_app, world.controller, world.runtime, runtime)

            request = build_request("alice", method="POST", path="/api/assistant/appearance",
                                    origin="http://127.0.0.1:%d" % world.portal_port)
            with self.assertRaises(AssistantError):
                current = await authority.authorize("missing-context-id")

            ctx_id = authority.context(request, {
                "id": ALICE, "is_admin": False, "scopes": ["A", "B"],
                "learning_scopes": [], "can_manage_settings": True,
            }, plan_id="plan-abc")
            _, ok = await authority.authorize(ctx_id)
            self.assertEqual(ok["id"], ALICE)

            # Logout: session cookie no longer resolves.
            world.sessions.pop("alice", None)
            with self.assertRaises(AssistantError):
                await authority.authorize(ctx_id)

            # Re-login with a reduced-scope session invalidates the old grant.
            world.sessions["alice"] = {"open_id": ALICE, "scopes": ["A"], "is_admin": False}
            ctx_id2 = authority.context(build_request("alice", origin="http://127.0.0.1:%d" % world.portal_port), {
                "id": ALICE, "is_admin": False, "scopes": ["A", "B"],
                "learning_scopes": [], "can_manage_settings": True,
            })
            with self.assertRaises(AssistantError):
                await authority.authorize(ctx_id2)

    # -- 8. synthetic business read/write permissions + scopes -----------------
    async def test_synthetic_business_permissions_scopes_and_plan_grant(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            runtime = FakeResidentRuntime(
                Path(directory), instance="i" * 32, lease="lease-test-2", key=_SERVICE_KEY)
            authority = PortalAuthority(world.portal_app, world.controller, world.runtime, runtime)

            request = build_request("alice", origin="http://127.0.0.1:%d" % world.portal_port)
            actor = {"id": ALICE, "is_admin": False, "scopes": ["A", "B"],
                     "learning_scopes": [], "can_manage_settings": True}

            # Read-only business RPC is allowed without any plan grant.
            ctx_read = authority.context(request, copy.deepcopy(actor))
            result = await authority.dispatch({
                "instance": runtime.instance, "lease": runtime.lease,
                "action": "invoke", "context_id": ctx_read,
                "payload": {"operation": {"api_id": "GET /api/business/probe"}},
            })
            self.assertIsInstance(result, dict)
            self.assertTrue(result.get("ok"))

            # Write RPC without a plan confirmation grant is refused.
            ctx_no_plan = authority.context(request, copy.deepcopy(actor))
            with self.assertRaises(AssistantError) as caught:
                await authority.dispatch({
                    "instance": runtime.instance, "lease": runtime.lease,
                    "action": "invoke", "context_id": ctx_no_plan,
                    "payload": {"operation": {"api_id": "POST /api/business/commit"},
                                "operation_id": "plan:plan-denied:001"},
                })
            self.assertEqual(caught.exception.status, 403)

            # Scope narrowing rejects out-of-scope codes and accepts subsets.
            with self.assertRaises(AssistantError):
                authority.narrowed(actor, ["X"])
            narrowed = authority.narrowed(actor, ["A"])
            self.assertEqual(narrowed["scopes"], ["A"])

    # -- 9. lost callback response after commit: never auto-resend, idempotent ----
    async def test_duplicate_confirmed_write_attempted_only_once(self):
        with tempfile.TemporaryDirectory() as directory:
            bw = BusinessWorld(directory)
            async with bw.activate():
                plan_id = "plan-dup-0001"
                ctx_id = bw.make_context(sid="alice", plan_id=plan_id)
                context = {"id": ctx_id, "lease": bw.client.lease}
                op = {"api_id": "POST /api/business/commit", "params": {}}
                payload = {
                    "operation": op,
                    "operation_id": "plan:plan-dup-0001:001",
                    "uploads": [],
                }

                # 1) First confirmed commit runs and succeeds, but the portal
                #    callback response is dropped by the routing transport AFTER
                #    the authority's mutation has completed. The service therefore
                #    receives a real transport failure instead of the response.
                bw.drop_callback_responses = True
                with self.assertRaises(AssistantError):
                    await bw.host.portal_bridge.acall("invoke", payload, context=context)
                self.assertEqual(bw.probe_count["commit"], 1)

                # No automatic retry: the same operation was never resubmitted.
                await asyncio.sleep(0.05)
                self.assertEqual(bw.probe_count["commit"], 1)

                # 2) Transport repaired: explicit same-operation re-query returns the
                #    original deduplicated result without a second mutation.
                bw.drop_callback_responses = False
                replay = await bw.host.portal_bridge.acall("invoke", payload, context=context)
                self.assertEqual(bw.probe_count["commit"], 1)
                self.assertTrue(replay.get("ok"))
                self.assertEqual(replay["data"], {"committed": True, "count": 1})
                again = await bw.host.portal_bridge.acall("invoke", payload, context=context)
                self.assertEqual(again, replay)
                self.assertEqual(bw.probe_count["commit"], 1)

                # 3) A distinct confirmation may still commit once.
                changed = copy.deepcopy(payload)
                changed["operation_id"] = "plan:plan-dup-0001:002"
                await bw.host.portal_bridge.acall("invoke", changed, context=context)
                self.assertEqual(bw.probe_count["commit"], 2)

    async def test_submitted_plan_refresh_resumes_only_remaining_approved_steps(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            job_reads = []
            @world.portal_app.get('/api/jobs/{job_id}')
            async def job_status(job_id: str):
                job_reads.append(job_id)
                return {'phase': 'success'}
            async with world.activate():
                client = world.portal_client()
                await world.warm(client)
                conversation = await client.get('/api/assistant/conversation', cookies={'sid': 'alice'})
                conversation_id = conversation.json()['data']['conversation_id']
                plan_id = uuid.uuid4().hex
                plan = {'id': plan_id, 'owner': ALICE, 'conversation_id': conversation_id,
                    'turn_id': 'original-approved-submit', 'scopes': ['A', 'B'], 'status': 'submitted',
                    'version': 1, 'title': 'Original approved plan', 'risk': 'normal', 'fields': [], 'error': '',
                    'operations': [{'api_id': 'POST /api/business/commit'}, {'api_id': 'POST /api/business/commit'}],
                    'results': [{'ok': True, 'api_id': 'POST /api/business/commit',
                        '_raw': {'job_id': 'original-submitted-job'}, 'data': {'job_id': 'original-submitted-job'},
                        '_task': {'operation': {'api_id': 'GET /api/jobs/{job_id}',
                            'path_params': {'job_id': 'original-submitted-job'}}, 'kind': 'job'}}]}
                world.host.store.put_document('lighthouse_agent_plans', plan_id, plan)
                response = await client.get('/api/assistant/plans/' + plan_id, cookies={'sid': 'alice'})
                self.assertEqual(response.status_code, 200, response.text)
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    stored = world.host.store.get_document('lighthouse_agent_plans', plan_id)
                    if stored['status'] in {'completed', 'failed'}:
                        break
                    await asyncio.sleep(.01)
                self.assertEqual(stored['status'], 'completed', stored.get('error'))
                self.assertEqual(world.probe_count['commit'], 1, 'Original first step was replayed')
                self.assertEqual(job_reads, ['original-submitted-job'])
                self.assertEqual(stored['results'][0]['_raw']['job_id'], 'original-submitted-job')
                await client.get('/api/assistant/plans/' + plan_id, cookies={'sid': 'alice'})
                self.assertEqual(world.probe_count['commit'], 1)

    async def test_plan_status_read_does_not_confirm_or_execute_unapproved_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            async with world.activate():
                client = world.portal_client()
                await world.warm(client)
                conversation = await client.get('/api/assistant/conversation', cookies={'sid': 'alice'})
                plan_id = uuid.uuid4().hex
                plan = {'id': plan_id, 'owner': ALICE, 'conversation_id': conversation.json()['data']['conversation_id'],
                    'turn_id': 'unapproved-submit', 'scopes': ['A', 'B'], 'status': 'awaiting_confirmation',
                    'version': 1, 'title': 'Unapproved plan', 'risk': 'normal', 'fields': [], 'error': '',
                    'operations': [{'api_id': 'POST /api/business/commit'}], 'results': []}
                world.host.store.put_document('lighthouse_agent_plans', plan_id, plan)
                response = await client.get('/api/assistant/plans/' + plan_id, cookies={'sid': 'alice'})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()['data']['status'], 'awaiting_confirmation')
                for sid, status in (('reduced', 403), ('bob', 404)):
                    response = await client.get('/api/assistant/plans/' + plan_id, cookies={'sid': sid})
                    self.assertEqual(response.status_code, status)
                self.assertEqual(world.probe_count['commit'], 0)

    # -- confirmed multipart upload: RemoteCatalog -> PortalAuthority.files -----
    async def test_confirmed_multipart_upload_remote_catalog_to_authority_files(self):
        with tempfile.TemporaryDirectory() as directory:
            bw = BusinessWorld(directory)
            async with bw.activate():
                plan_id = "plan-upload-0001"
                ctx_id = bw.make_context(sid="alice", plan_id=plan_id)
                request = build_request("alice", base_port=bw.portal_port)
                context = {"id": ctx_id, "lease": bw.client.lease}
                request.state.portal_context = context

                identity = uuid.uuid4().hex
                content = b"hello attachment body"
                item = bw.upload_item(identity, content=content)
                provider = {identity: item}

                # Mid-file SHA matches bytes (auto-generated by helper), then we
                # exercise the real RemoteCatalog.invoke which forwards uploads to
                # PortalAuthority.files and finally posts a native multipart body.
                self.assertEqual(item["sha256"], hashlib.sha256(content).hexdigest())

                OPERATION.set("plan:plan-upload-0001:001")
                operation = {
                    "api_id": "POST /api/business/proof",
                    "params": {},
                    "body": {"scope": "A"},
                    "files": {"attachment": [identity]},
                }
                try:
                    result = await bw.host.catalog.invoke(
                        operation, request, file_provider=lambda _id: provider[_id])
                finally:
                    OPERATION.set(None)

                self.assertTrue(result.get("ok"))
                self.assertEqual(bw.uploads_received[0]["name"], "proof.txt")
                self.assertEqual(bw.uploads_received[0]["content"], content)
                self.assertEqual(bw.uploads_received[0]["scope"], "A")

    # -- 10. attachment proof / upload / preview authorization ------------------
    async def test_attachment_authorization_no_arbitrary_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            root = Path(directory)
            files_dir = root / "files"
            files_dir.mkdir(parents=True, exist_ok=True)
            identity = "a" * 32

            runtime = FakeResidentRuntime(
                root, instance="i" * 32, lease="lease-test-4", key=_SERVICE_KEY)
            authority = PortalAuthority(world.portal_app, world.controller, world.runtime, runtime)
            actor = {"id": ALICE, "scopes": ["A", "B"]}

            # Valid owned attachment inside the managed root passes.
            target = files_dir / (identity + ".txt")
            target.write_bytes(b"x" * 10)
            operation_ok = {"api_id": "GET /api/business/probe", "files": {"attachment": [identity]}}
            uploads_ok = [{
                "id": identity, "owner": ALICE, "name": "proof.txt", "mime": "text/plain",
                "path": str(target), "size": 10, "source_scopes": ["A"],
            }]
            resolved = authority.files(actor, operation_ok, uploads_ok)
            self.assertIn(identity, resolved)

            # Arbitrary path outside the managed files root is rejected.
            other_dir = root / "other"
            other_dir.mkdir(parents=True, exist_ok=True)
            outside = other_dir / ("outside-" + identity + ".txt")
            outside.write_bytes(b"y" * 10)
            uploads_outside = [dict(uploads_ok[0], path=str(outside))]
            with self.assertRaises(AssistantError) as caught:
                authority.files(actor, operation_ok, uploads_outside)
            self.assertEqual(caught.exception.status, 404)

            # Owner mismatch is rejected.
            uploads_other_owner = [dict(uploads_ok[0], owner=BOB)]
            with self.assertRaises(AssistantError):
                authority.files(actor, operation_ok, uploads_other_owner)

            # Incomplete wanted set is rejected.
            missing_op = {"api_id": "GET /api/business/probe",
                          "files": {"attachment": [identity, "b" * 32]}}
            with self.assertRaises(AssistantError):
                authority.files(actor, missing_op, uploads_ok)

    # -- 11. unregister retains service storage and idle fake gateway ----------
    async def test_unregister_retains_service_storage_and_idle_gateway(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            async with world.activate():
                portal = world.portal_client()
                # A first portal request readies the shared client/host.
                await world.warm(portal)

                acquired = await world.host.acquire({
                    "instance": world.host.instance,
                    "lease": world.host.lease["id"],
                    "actor": {"id": ALICE, "scopes": ["A"]},
                    "profile": {"id": "fixture", "model": "fixture-model", "name": "Fixture",
                                "endpoint": "https://example.com/v1/chat/completions",
                                "key_cipher": "not-a-real-key"},
                    "definitions": [{"name": "lighthouse_probe_read"}],
                    "request_id": "req-" + "1" * 20,
                    "warm_only": True,
                })
                self.assertIn(acquired["key"], world.manager.accounts)
                self.assertTrue((world.host.state / "assistant.sqlite3").is_file())

                # Portal unregister (invalidate) must not discard storage or the idle gateway.
                await world.host.invalidate()
                self.assertIsNotNone(world.host.store)
                self.assertTrue((world.host.state / "assistant.sqlite3").is_file())
                self.assertIn(acquired["key"], world.manager.accounts)

    # -- 12. real PortalBridge.call('validate')/parse_notice on a worker thread --
    async def test_portal_bridge_sync_call_validate_and_parse_notice(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            async with world.activate():
                await world.warm(world.portal_client())

                ctx_id = world.last_context_header
                self.assertTrue(ctx_id, "proxy did not forward a context header")
                context = {"id": ctx_id, "lease": world.host.lease["id"]}

                def run_sync_calls():
                    # PortalBridge.call uses the synchronous httpx.Client path;
                    # the RoutingSyncTransport bridges the worker thread onto the
                    # test loop.  It must never be called from the loop itself.
                    validated = world.host.portal_bridge.call(
                        "validate", {"operation": {"api_id": "GET /api/business/probe"}}, context=context)
                    notice = world.host.portal_bridge.call(
                        "parse_notice", {"text": "测试通知", "fallback_work_type": ""}, context=context)
                    return validated, notice

                validated, notice = await asyncio.to_thread(run_sync_calls)
                self.assertEqual(validated["operation"]["api_id"], "GET /api/business/probe")
                self.assertEqual(validated["missing"], [])
                self.assertIsInstance(notice, dict)

                # The sync adapter must refuse running on the async loop itself.
                with self.assertRaises(RuntimeError):
                    world.host.portal_bridge.call(
                        "validate", {"operation": {"api_id": "GET /api/business/probe"}}, context=context)

    # -- 13. real ResidentRuntime.prepare (health + register) over injected ASGI --
    async def test_real_resident_runtime_prepare_over_asgi(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            # The genuine ResidentRuntime performs the full health -> register ->
            # setup handshake against the injected ASGI stack. `launch` is replaced
            # with a guard so a real process spawn fails the test immediately.
            async with world.activate(real_runtime=True):
                # A descriptor must already exist so `prepare` skips recovery/launch.
                record = {
                    "identity": identity(PROJECT, world.service_state),
                    "port": world.service_port,
                    "pid": os.getpid(),
                    "instance": world.host.instance,
                    "process_stamp": process_stamp(os.getpid()),
                }
                atomic_json(world.service_state / "service.json", record)

                client = world.portal_client()
                # The first browser request triggers ready() which constructs the
                # real ResidentRuntime (assistant_backend=True) through the portal
                # route and runs prepare(): health, register, setup, catalogue.
                await world.warm(client)

                self.assertIsNotNone(world.real_runtime)
                self.assertIsInstance(
                    world.real_runtime, _REAL_RUNTIME,
                    "activate(real_runtime=True) must instantiate the genuine class",
                )
                # The real prepare()->setup_backend handshake leaves the backend lease
                # belonging to the real runtime's register.
                self.assertEqual(world.host.portal_bridge.host, world.host)
                self.assertIsNotNone(world.host.lease)
                self.assertEqual(world.host.lease["callback_url"],
                                 "http://127.0.0.1:%d/api/assistant/service-bridge" % world.portal_port)
                # backend store migrated from the legacy window DB for this runtime.
                self.assertEqual(world.host.backend_lease, world.real_runtime.lease)
                self.assertTrue((world.service_state / "assistant.sqlite3").is_file())

                # The descriptor / health / register / setup all went through the
                # injected ASGI applications (no real localhost socket opened).
                appearance = await client.get("/api/assistant/appearance", cookies={"sid": "alice"})
                _assert_warm(appearance)
                self.assertEqual(appearance.status_code, 200)

                # A second genuine runtime (assistant_backend=False) can also share
                # the lease and read the same store after the warm ready() handshake.
                second = _REAL_RUNTIME(
                    world.service_state,
                    callback_url=lambda: "http://127.0.0.1:%d/api/assistant/service-bridge" % world.portal_port,
                    project=PROJECT,
                    launch=lambda *args, **kwargs: (_ for _ in ()).throw(
                        AssertionError("ResidentRuntime.launch must not run when a descriptor exists")),
                    legacy_db=None,
                    assistant_backend=False,
                )
                try:
                    health = await second.prepare()
                    self.assertEqual(health["instance"], world.host.instance)
                    self.assertIsNotNone(second.lease)
                finally:
                    await second.close()

    # -- 14. messages -> stream -> stop with a fake independent engine ----------
    async def test_messages_stream_stop_with_fake_engine(self):
        from openclaw_service.assistant import lighthouse_stream as _stream_module

        class FakeEngine:
            max_parallel = 1
            manages_context = False
            manages_runtime = False

            def __init__(self, *args, **kwargs):
                self.gate = None  # each instance may set a barrier
                self.seen = []
                self.started = asyncio.Event()

            async def answer(self, public_actor, turn, eligible, request, emit, authorize, context):
                self.seen.append((list(public_actor['scopes']), turn.get('prompt', turn['question'])))
                self.started.set()
                # Block forever so the test can exercise an in-flight stream + cancel.
                if self.gate is not None:
                    await self.gate.wait()
                return {"answer": "(fake)"}

            async def close(self):
                pass

        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            async with world.activate():
                gate = asyncio.Event()
                engine = FakeEngine()
                engine.gate = gate
                with patch.object(_stream_module, "LighthouseModel", lambda *a, **k: engine):
                    client = world.portal_client()
                    await world.warm(client)

                    # The submit path reads the configured model from the service
                    # store even when the engine itself is faked; seed a profile with
                    # a DPAPI-encrypted (non-plaintext) key so profile() passes.
                    world.host.store.put_document("lighthouse_ai", "model:actor-alice", {
                        "enabled": True,
                        "active_model_id": "test-model",
                        "models": [{
                            "id": "test-model", "name": "Fixture", "endpoint": ENDPOINT,
                            "model": MODEL, "key_cipher": protect_key("fake-model-key"),
                        }],
                    })

                    conv = await client.get("/api/assistant/conversation", cookies={"sid": "alice"})
                    self.assertEqual(conv.status_code, 200, conv.text)
                    conversation_id = conv.json()["data"]["conversation_id"]

                    op_id = "op-" + "x" * 20
                    post = await client.post(
                        "/api/assistant/messages",
                        cookies={"sid": "alice"},
                        headers={"Origin": "http://127.0.0.1:%d" % world.portal_port,
                                 "content-type": "application/json"},
                        json={
                            "operation_id": op_id,
                            "question": "全部楼栋，请查询并输出结果",
                            "conversation_id": conversation_id,
                        },
                    )
                    self.assertEqual(post.status_code, 202, post.text)
                    run_id = post.json()["data"]["run_id"]
                    self.assertTrue(run_id)

                    await asyncio.wait_for(engine.started.wait(), 3)
                    engine.started.clear()
                    supplement = await client.post(
                        '/api/assistant/messages', cookies={'sid': 'alice'},
                        headers={'Origin': 'http://127.0.0.1:%d' % world.portal_port},
                        json={'operation_id': 'supplement-' + 'y' * 20, 'question': '只看A楼',
                              'conversation_id': conversation_id})
                    self.assertEqual(supplement.status_code, 202, supplement.text)
                    original_run = run_id
                    run_id = supplement.json()['data']['run_id']
                    self.assertNotEqual(original_run, run_id)
                    await asyncio.wait_for(engine.started.wait(), 3)
                    self.assertEqual(engine.seen[-1][0], ['A'])
                    self.assertIn('只看A楼', engine.seen[-1][1])
                    self.assertIn('全部楼栋', engine.seen[-1][1])
                    prior = world.host.store.get_document('lighthouse_runs', original_run)
                    self.assertEqual(prior['status'], 'stopped')
                    saved = world.host.store.get_document('lighthouse_ai', 'conversation:' + ALICE)
                    self.assertEqual(saved['id'], conversation_id)
                    self.assertEqual(len(saved['turns']), 2)

                    # Cancel the in-flight run while the fake engine is paused.
                    stop = await client.post(
                        "/api/assistant/runs/%s/cancel" % run_id,
                        cookies={"sid": "alice"},
                        headers={"Origin": "http://127.0.0.1:%d" % world.portal_port})
                    self.assertEqual(stop.status_code, 200, stop.text)
                    self.assertEqual(stop.json()["data"]["status"], "stopped")

                    # Let the cancelled worker unwind, then release the engine gate.
                    await asyncio.sleep(0)
                    gate.set()
                    await asyncio.sleep(0)

                    # Stream the stopped run: buffered events + Done/Abort chunk return
                    # promptly because the run is no longer ACTIVE.
                    async with client.stream(
                        "GET", "/api/assistant/runs/%s/stream" % run_id, cookies={"sid": "alice"}
                    ) as stream2:
                        payload_bytes = await stream2.aread()
                        self.assertIn(b"data:", payload_bytes)

                    # Run is persisted in the service store as stopped.
                    run = world.host.store.get_document("lighthouse_runs", run_id)
                    self.assertIsNotNone(run)
                    self.assertEqual(run["status"], "stopped")

    # -- 15. multipart upload + authorized preview through the portal proxy -----
    async def test_multipart_upload_and_authorized_preview_through_proxy(self):
        with tempfile.TemporaryDirectory() as directory:
            world = World(directory)
            self._host_for_cleanup = world.host
            async with world.activate():
                client = world.portal_client()
                await world.warm(client)

                payload = (
                    "--boundary\r\n"
                    'Content-Disposition: form-data; name="files"; filename="proof.txt"\r\n'
                    "Content-Type: text/plain\r\n\r\n"
                    "hello attachment body\r\n"
                    "--boundary--\r\n"
                )
                upload = await client.post(
                    "/api/assistant/files",
                    cookies={"sid": "alice"},
                    headers={
                        "Origin": "http://127.0.0.1:%d" % world.portal_port,
                        "content-type": "multipart/form-data; boundary=boundary",
                    },
                    content=payload.encode(),
                )
                self.assertEqual(upload.status_code, 200, upload.text)
                files = upload.json()["data"]["files"]
                self.assertEqual(len(files), 1)
                file_id = files[0]["id"]
                stored = world.host.store.get_document("lighthouse_files", file_id)
                self.assertIsNotNone(stored)
                stored_path = Path(stored["path"])
                self.assertTrue(stored_path.is_file())
                self.assertEqual(stored_path.read_bytes(), b"hello attachment body")

                # Authorized owner can preview the stored attachment via the proxy.
                preview = await client.get(
                    "/api/assistant/files/%s" % file_id, cookies={"sid": "alice"})
                self.assertEqual(preview.status_code, 200, preview.text)
                self.assertIn("attachment", preview.headers.get("content-disposition", "attachment"))

                # A different actor cannot preview another user's attachment.
                denied = await client.get(
                    "/api/assistant/files/%s" % file_id, cookies={"sid": "bob"})
                self.assertIn(denied.status_code, (403, 404))


if __name__ == "__main__":
    import unittest

    unittest.main()

"""Isolated lifecycle tests for ``lan_bitable_template_portal.lighthouse_routes``.

These verify that the portal's startup/shutdown must not be serialised behind the
resident assistant's ``ResidentRuntime.prepare``, and that a failing or still-blocked
assistant preparation never blocks ordinary portal routes or leaks gateway/service
termination:

* When ``ResidentRuntime.prepare`` is blocked on an ``asyncio.Event``, the app startup
  handler returns immediately and the ordinary ``/api/health`` route answers on the same
  event loop (assistant preparation is fully decoupled from portal startup).
* When ``ResidentRuntime.prepare`` fails, ordinary health/business routes keep working
  while assistant endpoints return a safe error that is contained inside the assistant.
* Shutdown cancels the in-flight preparation and closes the resident connection, but
  never invokes the real stop surface of ``ResidentRuntime`` (``_stop`` or a
  ``_request`` with the ``shutdown``/``stop-account`` actions). Real unregister
  semantics of ``close()`` are covered by the dedicated client tests; this module only
  proves the portal closes the connection.

Everything is synthetic: a temporary ``LanPortalStateStore``, a real ASGI ``FastAPI`` app,
and a controlled stand-in for ``openclaw_service.client.ResidentRuntime``. No real Node,
no real service, no task scheduler, no Feishu, no environment mutation.
"""
from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

import httpx
from fastapi import FastAPI

from lan_bitable_template_portal.lighthouse_routes import install_lighthouse_routes
from lan_bitable_template_portal.state_store import LanPortalStateStore
from openclaw_service.protocol import ServiceError

# Real actions that would tell the resident host to shut down or stop an account.
SHUTDOWN_ACTIONS = {"shutdown", "stop-account"}


class ControlledResidentRuntime:
    """Controllable stand-in for ``openclaw_service.client.ResidentRuntime``.

    Mirrors the real interface used by ``lighthouse_routes``: construction takes
    ``(state_root, *, callback_url, project, launch, legacy_db, assistant_backend)`` and
    ``prepare()``/``close()`` are async. It never starts Node.

    The stand-in also *guards the real stop surfaces* of ``ResidentRuntime``: calling
    ``_stop(item)`` or ``_request('shutdown'/'stop-account', ...)`` fails the test because
    the portal must only close its own connection and must never stop the resident host.
    ``close()`` merely records that the portal shut the connection; its unregister
    semantics are asserted by the dedicated client tests, not here.
    """

    def __init__(self, state_root, *, callback_url=None, project=None, launch=None,
                 legacy_db=None, assistant_backend=False):
        self.state_root = Path(state_root)
        self.root = self.state_root
        self.project = Path(project) if project else self.state_root
        self.callback_url = callback_url
        self.launch = launch
        self.legacy_db = legacy_db
        self.assistant_backend = assistant_backend
        self.descriptor = None
        self.instance = "fixture-instance"
        self.lease = "fixture-lease"
        self.key = "fixture-key-0123456789abcdef0123456789abcdef"
        self.disconnected = False
        self.closing = False
        self.accounts = {}
        self.prepare_called = 0
        self.close_called = 0

        # Counters for the real stop surface; must always stay 0 in this module.
        self.stop_called = 0
        self.stop_actions = []

        # Test injection points (None = disabled).
        self.prepare_gate = None        # asyncio.Event; when set prepare blocks on it
        self.prepare_failure = None     # exception raised on every prepare() call
        self.prepare_started = None     # asyncio.Event set before prepare() body
        self.prepare_cancelled = None   # asyncio.Event set when a blocked prepare is cancelled
        self.prepare_task = None        # asyncio.Task currently running prepare()

    async def prepare(self, *, progress=None):
        self.prepare_called += 1
        self.prepare_task = asyncio.current_task()
        if self.prepare_started is not None:
            self.prepare_started.set()
        if self.prepare_failure is not None:
            raise self.prepare_failure
        if self.prepare_gate is not None:
            try:
                await self.prepare_gate.wait()
            except asyncio.CancelledError:
                if self.prepare_cancelled is not None:
                    self.prepare_cancelled.set()
                raise
        return {"protocol": 4, "instance": self.instance, "digest": "fixture"}

    async def close(self):
        """Portal-side close: drops the portal's resident connection.

        The real unregister semantics of ``ResidentRuntime.close`` are verified by the
        dedicated client tests; this module only proves the portal calls ``close``.
        """
        self.close_called += 1
        self.disconnected = True
        self.lease = None

    # --- Guards for the real stop surface of ResidentRuntime ----------------------
    # ``_stop(item)`` stops an account and ``_request('shutdown'/'stop-account')``
    # tells the resident host to shut itself down. These must never fire when the
    # portal shuts down: all three tests below assert the counters stay 0.

    async def _stop(self, item):
        self.stop_called += 1
        raise AssertionError(
            "portal shutdown must never call ResidentRuntime._stop (account stop surface)"
        )

    async def _request(self, action, payload=None, *, timeout=5, registered=True):
        if action in SHUTDOWN_ACTIONS:
            self.stop_called += 1
            self.stop_actions.append(action)
            raise AssertionError(
                f"portal shutdown must never call ResidentRuntime._request({action!r})"
            )
        raise AssertionError(f"unexpected ResidentRuntime._request({action!r}) in portal test")


def build_app(tmp):
    """Temporary state_store, minimal portal controller runtime, real ASGI app."""
    store = LanPortalStateStore(db_path=Path(tmp) / "state.sqlite3")
    session = {"open_id": "user-a", "role": "admin", "user": {}, "is_guest": False}

    controller = SimpleNamespace(
        bound_port=17301,
        preferred_port=17301,
        _current_session=lambda request: session,
        _request_base_url=lambda request: str(request.base_url).rstrip("/"),
    )
    runtime = SimpleNamespace(
        state_store=store,
        auth_manager=SimpleNamespace(
            session_scopes=lambda s: ["A", "B", "C", "D", "E", "H"],
            is_admin=lambda s: True,
        ),
    )

    app = FastAPI()

    @app.get("/api/health")
    async def health():
        return {"ok": True, "service": "lighthouse-routes-isolated-test"}

    @app.get("/api/business/probe")
    async def business_probe():
        return {"ok": True, "endpoint": "business"}

    install_lighthouse_routes(app, controller, runtime)
    return store, app


def patch_runtime(fake):
    return patch(
        "openclaw_service.client.ResidentRuntime",
        new=lambda *args, **kwargs: fake,
    )


def background_tasks(exclude=None):
    current = asyncio.current_task()
    return {t for t in asyncio.all_tasks() if t is not current and t is not exclude}


class LighthouseRoutesIsolatedLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_startup_returns_immediately_while_prepare_blocked_and_health_usable(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, app = build_app(tmp)
            fake = ControlledResidentRuntime(Path(tmp) / "lighthouse_openclaw")
            fake.prepare_gate = asyncio.Event()
            fake.prepare_started = asyncio.Event()
            fake.prepare_cancelled = asyncio.Event()

            with patch_runtime(fake):
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as client:
                    async with asyncio.timeout(10):
                        async with app.router.lifespan_context(app):
                            # startup() schedules connect() and returns straight away;
                            # wait until the assistant's prepare is actually invoked.
                            await asyncio.wait_for(fake.prepare_started.wait(), 5)
                            self.assertIsNotNone(fake.prepare_task)
                            self.assertFalse(
                                fake.prepare_task.done(),
                                "assistant preparation must still be pending",
                            )
                            # Ordinary route answers while prepare is blocked.
                            health = await asyncio.wait_for(client.get("/api/health"), 5)
                            self.assertEqual(health.status_code, 200, health.text)
                            self.assertTrue(health.json()["ok"])
                            self.assertFalse(fake.prepare_task.done())

            # Shutdown cancelled the blocked prep and closed the portal connection.
            self.assertTrue(fake.prepare_cancelled.is_set(),
                            "shutdown must cancel the blocked prepare")
            self.assertTrue(fake.prepare_task.done(),
                            "preparation task must finish during shutdown")
            self.assertEqual(fake.close_called, 1,
                             "shutdown must close the resident connection")
            self.assertEqual(fake.stop_called, 0,
                             "shutdown must never hit the resident stop surface")

    async def test_prepare_failure_keeps_health_business_while_assistant_returns_safe_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, app = build_app(tmp)
            fake = ControlledResidentRuntime(Path(tmp) / "lighthouse_openclaw")
            fake.prepare_failure = ServiceError("fixture prepare failure", 503)
            fake.prepare_started = asyncio.Event()

            with patch_runtime(fake):
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as client:
                    async with asyncio.timeout(10):
                        async with app.router.lifespan_context(app):
                            # Let the startup's background attempt hit the failure exactly
                            # once so the cached client is present before we probe routes.
                            await asyncio.wait_for(fake.prepare_started.wait(), 5)

                            health = await asyncio.wait_for(client.get("/api/health"), 5)
                            self.assertEqual(health.status_code, 200, health.text)
                            self.assertTrue(health.json()["ok"])

                            business = await asyncio.wait_for(
                                client.get("/api/business/probe"), 5)
                            self.assertEqual(business.status_code, 200, business.text)
                            self.assertTrue(business.json()["ok"])

                            # Assistant endpoint fails only inside the assistant with a
                            # safe 5xx barrier; it never escapes to an unhandled error.
                            assistant = await asyncio.wait_for(
                                client.get("/api/assistant/appearance"), 5)
                            self.assertEqual(assistant.status_code, 503, assistant.text)
                            self.assertFalse(assistant.json()["ok"])
                            self.assertIn("fixture prepare failure", assistant.json()["error"])

            # The failed preparation never blocked shutdown and the connection was closed.
            self.assertEqual(fake.prepare_called, 2,
                             "startup + assistant endpoint each attempted prepare")
            self.assertEqual(fake.close_called, 1)
            self.assertEqual(fake.stop_called, 0,
                             "shutdown must never hit the resident stop surface")

    async def test_shutdown_cancels_prepare_and_closes_without_stop_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, app = build_app(tmp)
            fake = ControlledResidentRuntime(Path(tmp) / "lighthouse_openclaw")
            fake.prepare_gate = asyncio.Event()
            fake.prepare_started = asyncio.Event()
            fake.prepare_cancelled = asyncio.Event()

            with patch_runtime(fake):
                async with asyncio.timeout(10):
                    async with app.router.lifespan_context(app):
                        await asyncio.wait_for(fake.prepare_started.wait(), 5)
                        self.assertFalse(fake.prepare_task.done())

                    # Exiting the lifespan ran shutdown: nothing left running.
                    self.assertTrue(fake.prepare_task.done())
                    self.assertTrue(fake.prepare_cancelled.is_set())
                    self.assertEqual(fake.close_called, 1,
                                     "shutdown must close the portal's resident connection")
                    self.assertEqual(fake.stop_called, 0,
                                     "shutdown must never hit the resident stop surface")
                    self.assertEqual(fake.stop_actions, [])

                    leaks = [t for t in background_tasks()
                             if t is not fake.prepare_task]
                    self.assertEqual(leaks, [], "shutdown must leave no leaked background tasks")


if __name__ == "__main__":
    unittest.main()
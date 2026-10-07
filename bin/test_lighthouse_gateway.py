"""Isolated tests for the bounded OpenClaw Gateway v4 transport.

These tests are fully synthetic: no live gateway, model, or cloud calls.  A
fake in-memory websocket is injected through the ``connector`` argument of
:class:`~lighthouse_gateway.GatewayClient`.  The wire protocol frames are
generated locally to exercise handshake (including hello validation), RPC
correlation, event streaming (full validated envelopes), byte-bounded queues,
disconnect/timeout/cancellation cleanup, replay/resend guards, strict loopback
URL constraints, proxy bypass, and res.ok / event-name frame validation.
"""

from __future__ import annotations

import asyncio
import copy
import json
import sys
import unittest
from pathlib import Path
from typing import Any, List

from websockets.exceptions import ConnectionClosed

MODULE_BIN = Path(__file__).resolve().parent
sys.path.insert(0, str(MODULE_BIN))
from lan_bitable_template_portal import lighthouse_gateway as gw

URL = "ws://127.0.0.1:18789"
TOKEN = "shared-token"

CHALLENGE = {
    "type": "event",
    "event": "connect.challenge",
    "payload": {"nonce": "abc-123", "ts": 1737264000000},
}


def hello_ok(*, protocol: int = 4, max_payload: int = 26214400) -> dict:
    return {
        "type": "res",
        "id": "lhg-connect",
        "ok": True,
        "payload": {
            "type": "hello-ok",
            "protocol": protocol,
            "server": {"version": "2026.9.8", "connId": "c1"},
            "features": {"methods": ["tools.catalog"], "events": ["tool-events"]},
            "snapshot": {},
            "auth": {"role": "operator", "scopes": ["operator.read", "operator.write"]},
            "policy": {
                "maxPayload": max_payload,
                "maxBufferedBytes": 8388608,
                "tickIntervalMs": 15000,
            },
        },
    }


def res_frame(
    req_id: str, payload: Any = None, *, ok: bool = True, error: Any = None
) -> dict:
    if ok:
        return {"type": "res", "id": req_id, "ok": True, "payload": payload}
    return {"type": "res", "id": req_id, "ok": False, "error": error}


def event_frame(event: str, payload: Any) -> dict:
    return {"type": "event", "event": event, "payload": payload}


class _CloseSentinel:
    pass


_CLOSE = _CloseSentinel()


class FakeWebSocket:
    """In-memory websocket used for synthetic end-to-end transport tests.

    frames may be pushed as dicts (JSON-encoded) or raw strings.  ``close()``
    injects a close sentinel which makes ``recv()`` raise
    :class:`~websockets.exceptions.ConnectionClosed`, simulating a disconnect.
    """

    def __init__(self, initial: List[Any] = None) -> None:
        self.sent: List[dict] = []
        self.closed = False
        self._in: asyncio.Queue = asyncio.Queue()
        for frame in initial or []:
            self._put(frame)

    def _put(self, frame: Any) -> None:
        if frame is _CLOSE:
            self._in.put_nowait(_CLOSE)
        elif isinstance(frame, str):
            self._in.put_nowait(frame)
        else:
            self._in.put_nowait(json.dumps(frame))

    async def push(self, frame: Any) -> None:
        self._put(frame)

    async def send(self, text: str) -> None:
        self.sent.append(json.loads(text))

    async def recv(self) -> str:
        item = await self._in.get()
        if item is _CLOSE:
            raise ConnectionClosed(None, None)
        if isinstance(item, bytes):
            return item.decode("utf-8")
        return item

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._in.put_nowait(_CLOSE)

    @property
    def requests(self) -> List[dict]:
        # Exclude the connect handshake request; this is the RPC request list.
        return [
            f
            for f in self.sent
            if f.get("type") == "req" and f.get("method") != "connect"
        ]

    @property
    def connect_request(self) -> dict:
        for f in self.sent:
            if f.get("type") == "req" and f.get("method") == "connect":
                return f
        raise AssertionError("no connect request was sent")


def connector_factory(ws: FakeWebSocket):
    async def _connector(url: str) -> FakeWebSocket:
        assert url == URL, f"connector must receive literal loopback URL, got {url!r}"
        return ws

    return _connector


class GatewayTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_handshake_success_and_connect_frame_shape(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            self.assertTrue(client.connected)
            self.assertEqual(client.protocol, 4)
            # Validated hello-ok is retained on a read-only property.
            self.assertEqual(client.hello["type"], "hello-ok")
            self.assertEqual(client.hello["protocol"], 4)
            with self.assertRaises(AttributeError):
                client.hello = {}  # read-only

        connect_req = ws.connect_request
        self.assertEqual(connect_req["type"], "req")
        self.assertEqual(connect_req["method"], "connect")
        params = connect_req["params"]
        self.assertEqual(params["minProtocol"], 4)
        self.assertEqual(params["maxProtocol"], 4)
        self.assertEqual(params["client"]["id"], "gateway-client")
        self.assertEqual(params["client"]["mode"], "backend")
        self.assertEqual(params["role"], "operator")
        self.assertIn("tool-events", params.get("caps", []))
        self.assertEqual(params["auth"]["token"], TOKEN)
        # device MUST be omitted per the trusted loopback backend exception.
        self.assertNotIn("device", params)

    async def test_invalid_challenge_rejected(self):
        cases = [
            {"type": "event", "event": "connect.challenge", "payload": {"ts": 1}},
            {"type": "event", "event": "connect.challenge", "payload": {}},
            {"type": "event", "event": "connect.challenge",
             "payload": {"nonce": "x", "ts": -5}},
            {"type": "event", "event": "connect.challenge",
             "payload": {"nonce": "x", "ts": 1.5}},
            {"type": "event", "event": "connect.challenge",
             "payload": {"nonce": "x", "ts": True}},
            {"type": "event", "event": "other", "payload": {"nonce": "x", "ts": 1}},
            {"type": "res", "id": "lhg-connect", "ok": True, "payload": {}},
        ]
        for bad in cases:
            with self.subTest(bad=bad):
                ws = FakeWebSocket(initial=[bad])
                with self.assertRaises(gw.GatewayError) as ctx:
                    async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)):
                        pass  # pragma: no cover
                self.assertEqual(ctx.exception.code, "BAD_HANDSHAKE")

    async def test_unsupported_version_rejected(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok(protocol=3)])
        with self.assertRaises(gw.GatewayError) as ctx:
            async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)):
                pass  # pragma: no cover
        self.assertEqual(ctx.exception.code, "UNSUPPORTED_PROTOCOL")

    async def test_missing_hello_structures_rejected(self):
        base = hello_ok()["payload"]
        bad_payloads = []
        for key in ("server", "features", "snapshot", "auth", "policy"):
            p = copy.deepcopy(base)
            p.pop(key)
            bad_payloads.append(p)
        p = copy.deepcopy(base)
        p["server"] = {"version": "x"}  # missing connId
        bad_payloads.append(p)
        p = copy.deepcopy(base)
        p["auth"]["scopes"] = "operator.read"  # must be a list
        bad_payloads.append(p)
        p = copy.deepcopy(base)
        p["policy"]["maxPayload"] = 0  # must be positive
        bad_payloads.append(p)
        p = copy.deepcopy(base)
        p["features"]["events"] = [1]  # must be strings
        bad_payloads.append(p)

        for payload in bad_payloads:
            with self.subTest(payload=payload):
                ws = FakeWebSocket(initial=[
                    CHALLENGE,
                    {"type": "res", "id": "lhg-connect", "ok": True, "payload": payload},
                ])
                with self.assertRaises(gw.GatewayError) as ctx:
                    async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)):
                        pass  # pragma: no cover
                self.assertEqual(ctx.exception.code, "BAD_HANDSHAKE")

    async def test_policy_caps_bounded_by_local_maximums(self):
        hello = hello_ok()
        hello["payload"]["policy"]["maxPayload"] = 10 * 1024 * 1024 * 1024
        hello["payload"]["policy"]["maxBufferedBytes"] = 20 * 1024 * 1024 * 1024
        ws = FakeWebSocket(initial=[CHALLENGE, hello])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            self.assertEqual(client._max_payload, gw.LOCAL_MAX_PAYLOAD_BYTES)
            self.assertEqual(client._max_buffered, gw.LOCAL_MAX_BUFFERED_BYTES)

    async def test_response_correlation_out_of_order(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            task_a = asyncio.create_task(client.request("m.a", {"x": 1}))
            task_b = asyncio.create_task(client.request("m.b", {"y": 2}))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            reqs = ws.requests
            self.assertEqual(len(reqs), 2)
            id_a = reqs[0]["id"]
            id_b = reqs[1]["id"]
            # Deliver B first, then A (out of order).  Each must resolve with
            # the payload matching its own request id.
            await ws.push(res_frame(id_b, {"method": "m.b", "params": {"y": 2}}))
            await ws.push(res_frame(id_a, {"method": "m.a", "params": {"x": 1}}))
            a, b = await asyncio.gather(task_a, task_b)
            self.assertEqual(a, {"method": "m.a", "params": {"x": 1}})
            self.assertEqual(b, {"method": "m.b", "params": {"y": 2}})

    async def test_events_streamed_with_full_envelope(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            await ws.push(event_frame("agent", {"phase": "started"}))
            await ws.push({"type": "event", "event": "tool.tool-event",
                           "payload": {"name": "shell"}, "seq": 7})
            first = await client.next_event(timeout=1)
            second = await client.next_event(timeout=1)
            self.assertEqual(first, {"type": "event", "event": "agent",
                                     "payload": {"phase": "started"}})
            self.assertEqual(second, {"type": "event", "event": "tool.tool-event",
                                      "payload": {"name": "shell"}, "seq": 7})

    async def test_accepted_response_separate_from_lifecycle_events(self):
        """An 'accepted' res payload is the RPC result; run lifecycle events
        remain events and are NOT conflated with the response."""
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            task = asyncio.create_task(client.request("tools.invoke", {"op": "run"}))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            req_id = ws.requests[0]["id"]
            # Accepted response first.
            await ws.push(res_frame(req_id, {"status": "accepted", "runId": "r1"}))
            # Then the terminal lifecycle event stream.
            await ws.push(event_frame("agent", {"runId": "r1", "phase": "terminated"}))
            accepted = await task
            lifecycle = await client.next_event(timeout=1)
            self.assertEqual(accepted, {"status": "accepted", "runId": "r1"})
            self.assertEqual(lifecycle, {"type": "event", "event": "agent",
                                         "payload": {"runId": "r1", "phase": "terminated"}})

    async def test_event_queue_byte_budget_overflow_fails_connection(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        client = gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws))
        # Shrink the byte budget to exactly two small envelopes so the third
        # overflows the aggregate byte bound.
        small = event_frame("tick", {"n": 0})
        client._event_byte_budget = len(
            json.dumps(small, ensure_ascii=False).encode("utf-8")
        ) * 2
        async with client:
            await ws.push(event_frame("tick", {"n": 0}))
            await ws.push(event_frame("tick", {"n": 1}))
            await ws.push(event_frame("tick", {"n": 2}))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            self.assertFalse(client.connected)
            # Buffered events are still drained first; afterwards the overflow
            # fault is surfaced.
            self.assertEqual(await client.next_event(timeout=1),
                             {"type": "event", "event": "tick", "payload": {"n": 0}})
            self.assertEqual(await client.next_event(timeout=1),
                             {"type": "event", "event": "tick", "payload": {"n": 1}})
            with self.assertRaises(gw.GatewayError) as ctx:
                await client.next_event(timeout=1)
            self.assertEqual(ctx.exception.code, "EVENT_QUEUE_OVERFLOW")

    async def test_oversized_frame_rejected_with_safe_code(self):
        # hello advertises a tiny maxPayload so the synthetic oversized frame
        # stays small.
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok(max_payload=64)])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            big = event_frame("tick", {"blob": "x" * 5000})
            await ws.push(big)
            with self.assertRaises(gw.GatewayError) as ctx:
                await client.next_event(timeout=1)
            self.assertEqual(ctx.exception.code, "OVERSIZED_FRAME")

    async def test_invalid_json_frame_rejected(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            await ws.push("this is not json {{{")
            with self.assertRaises(gw.GatewayError) as ctx:
                await client.next_event(timeout=1)
            self.assertEqual(ctx.exception.code, "INVALID_FRAME")

    async def test_nonstring_event_name_is_invalid_frame(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            await ws.push({"type": "event", "event": 123, "payload": {}})
            with self.assertRaises(gw.GatewayError) as ctx:
                await client.next_event(timeout=1)
            self.assertEqual(ctx.exception.code, "INVALID_FRAME")

    async def test_negative_seq_is_invalid_frame(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            await ws.push({"type": "event", "event": "tick", "payload": {}, "seq": -1})
            with self.assertRaises(gw.GatewayError) as ctx:
                await client.next_event(timeout=1)
            self.assertEqual(ctx.exception.code, "INVALID_FRAME")

    async def test_nonboolean_res_ok_is_invalid_frame(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            task = asyncio.create_task(client.request("m.x"))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            req_id = ws.requests[0]["id"]
            await ws.push({"type": "res", "id": req_id, "ok": "yes", "payload": {}})
            with self.assertRaises(gw.GatewayError) as ctx:
                await task
            self.assertEqual(ctx.exception.code, "INVALID_FRAME")

    async def test_timeout_cleans_pending_and_does_not_resend(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            with self.assertRaises(asyncio.TimeoutError):
                await client.request("m.slow", timeout=0.05)
            self.assertEqual(len(client._pending), 0)
            self.assertEqual(len(ws.requests), 1)  # exactly one send, no resend

    async def test_cancellation_cleans_pending(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            task = asyncio.create_task(client.request("m.cancel", timeout=10))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(len(client._pending), 0)
            self.assertEqual(len(ws.requests), 1)  # no resend

    async def test_disconnect_wakes_pending_request(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            task = asyncio.create_task(client.request("m.long", timeout=10))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            await ws.close()  # server-side disconnect
            with self.assertRaises(gw.GatewayError) as ctx:
                await task
            self.assertEqual(ctx.exception.code, "DISCONNECTED")
            self.assertEqual(len(client._pending), 0)

    async def test_no_replay_of_late_response_after_timeout(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            with self.assertRaises(asyncio.TimeoutError):
                await client.request("m.mutating", timeout=0.05)
            req_id = ws.requests[0]["id"]
            self.assertEqual(len(ws.requests), 1)
            # Server eventually sends the response for the abandoned request.
            # It must be dropped, never replayed / never resend.
            await ws.push(res_frame(req_id, {"ok": "late"}))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            self.assertEqual(len(ws.requests), 1)  # no resend
            self.assertEqual(len(client._pending), 0)

    async def test_stale_response_id_dropped(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            # Unknown/never-sent id arrives: must be ignored silently.
            await ws.push(res_frame("lhg-9999", {"junk": True}))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            self.assertEqual(len(client._pending), 0)
            # Connection stays healthy.
            self.assertTrue(client.connected)

    async def test_server_error_sanitized_and_reason_allowlisted(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            task = asyncio.create_task(client.request("tools.catalog"))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            req_id = ws.requests[0]["id"]
            await ws.push(res_frame(
                req_id,
                ok=False,
                error={
                    "code": "UNAVAILABLE",
                    "message": "secret internal detail",
                    "details": {"reason": "startup-sidecars"},
                    "retryAfterMs": 500,
                },
            ))
            with self.assertRaises(gw.GatewayError) as ctx:
                await task
            err = ctx.exception
            self.assertEqual(err.code, "UNAVAILABLE")
            self.assertEqual(err.reason, "startup-sidecars")
            self.assertEqual(err.retry_after_ms, 500)
            self.assertNotIn("secret internal detail", str(err))

    async def test_server_error_reason_blocked_when_not_allowlisted(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)) as client:
            task = asyncio.create_task(client.request("tools.catalog"))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            req_id = ws.requests[0]["id"]
            await ws.push(res_frame(
                req_id,
                ok=False,
                error={
                    "code": "INTERNAL_ERROR",
                    "message": "db password=topsecret",
                    "details": {"reason": "db offline"},
                },
            ))
            with self.assertRaises(gw.GatewayError) as ctx:
                await task
            err = ctx.exception
            self.assertEqual(err.code, "INTERNAL_ERROR")
            self.assertIsNone(err.reason)
            self.assertNotIn("db password=topsecret", str(err))
            self.assertNotIn("db offline", str(err))

    async def test_local_url_constraints(self):
        for bad in (
            "http://127.0.0.1:18789",
            "wss://127.0.0.1:18789",
            "ws://localhost:18789",
            "ws://0.0.0.0:18789",
            "ws://10.0.0.1:9",
            "ws://127.0.0.1",
            "ws://127.0.0.1:0",
            "ws://127.0.0.1:70000",
            "ws://127.0.0.1:80/extra",
            "ws://[::1]:80",
            "",
            None,
        ):
            with self.subTest(url=bad):
                with self.assertRaises(gw.GatewayError) as ctx:
                    gw.GatewayClient(bad, TOKEN, connector=None)
                self.assertEqual(ctx.exception.code, "INVALID_URL")

        # Literal loopback ws://127.0.0.1:PORT passes at construction.
        gw.GatewayClient("ws://127.0.0.1:18789", TOKEN, connector=None)
        gw.GatewayClient("ws://127.0.0.1:1", TOKEN, connector=None)
        gw.GatewayClient("ws://127.0.0.1:65535", TOKEN, connector=None)

    async def test_close_is_idempotent_and_wakes_pending(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        client = gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws))
        await client.__aenter__()
        task = asyncio.create_task(client.request("m.pending", timeout=10))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await client.close()
        await client.close()  # second close is a no-op
        with self.assertRaises(gw.GatewayError) as ctx:
            await task
        self.assertEqual(ctx.exception.code, "CLOSED")
        self.assertTrue(client._closed)

    async def test_handshake_success_closes_socket_on_exit(self):
        ws = FakeWebSocket(initial=[CHALLENGE, hello_ok()])
        async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)):
            self.assertFalse(ws.closed)
        self.assertTrue(ws.closed)

    async def test_handshake_failure_closes_socket(self):
        bad = {"type": "event", "event": "connect.challenge", "payload": {"ts": 1}}
        ws = FakeWebSocket(initial=[bad])
        with self.assertRaises(gw.GatewayError) as ctx:
            async with gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws)):
                pass  # pragma: no cover
        self.assertEqual(ctx.exception.code, "BAD_HANDSHAKE")
        self.assertTrue(ws.closed)  # no leaked websocket

    async def test_cancellation_during_handshake_closes_socket(self):
        ws = FakeWebSocket(initial=[CHALLENGE])  # no hello yet: recv blocks
        client = gw.GatewayClient(URL, TOKEN, connector=connector_factory(ws))
        task = asyncio.create_task(client.__aenter__())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(ws.closed)  # no leaked websocket

    async def test_default_connect_bypasses_proxy_for_loopback(self):
        captured = {}

        async def fake_connect(*args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs
            raise RuntimeError("stop before connecting")

        original = gw.websockets.connect
        gw.websockets.connect = fake_connect
        try:
            factory = gw._default_connect()
            with self.assertRaises(RuntimeError):
                await factory("ws://127.0.0.1:18789")
        finally:
            gw.websockets.connect = original

        self.assertEqual(captured["args"], ("ws://127.0.0.1:18789",))
        self.assertIsNone(captured["kwargs"].get("proxy"))
        self.assertIsNone(captured["kwargs"].get("compression"))
        self.assertIsNone(captured["kwargs"].get("ping_interval"))


class SharedEventIsolationTests(unittest.TestCase):
    def test_other_accounts_broadcasts_are_dropped_before_buffering(self):
        client = gw.GatewayClient('ws://127.0.0.1:1234', 'fixture-token')
        client.select_run('own-run')
        for _ in range(100):
            client._handle_event({'event': 'agent', 'payload': {'runId': 'other-run', 'text': 'x' * 100000}})
        self.assertEqual(client._event_bytes, 0)
        self.assertFalse(client._events)
        client._handle_event({'event': 'agent', 'payload': {'runId': 'own-run', 'text': 'owned'}})
        self.assertEqual(len(client._events), 1)
        client.select_run()
        client._handle_event({'event': 'chat', 'payload': {'runId': 'own-run', 'text': 'old'}})
        self.assertFalse(client._events)


if __name__ == "__main__":
    unittest.main()

# -*- coding: utf-8 -*-
import os
import queue
import threading
import time
import json
from contextlib import asynccontextmanager
from typing import Optional, Set

from ..hot_reload.state_store import get_user_data_dir
from ..config import config

try:
    from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import FileResponse, HTMLResponse
    from pydantic import BaseModel

    FASTAPI_AVAILABLE = True
    FASTAPI_IMPORT_ERROR = None
except Exception as exc:
    FastAPI = None  # type: ignore[assignment]
    HTTPException = Exception  # type: ignore[assignment]
    WebSocket = None  # type: ignore[assignment]
    WebSocketDisconnect = Exception  # type: ignore[assignment]
    CORSMiddleware = None  # type: ignore[assignment]
    FileResponse = None  # type: ignore[assignment]
    HTMLResponse = None  # type: ignore[assignment]

    class BaseModel:  # type: ignore[no-redef]
        pass

    FASTAPI_AVAILABLE = False
    FASTAPI_IMPORT_ERROR = exc

from ..logger import log_info, log_warning, log_error
from ..utils import get_resource_path


DEDUP_WINDOW_SECONDS = 300
FALLBACK_INDEX_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>ClipFlow Event Relay</title>
  <style>
    body { font-family: -apple-system, Segoe UI, sans-serif; margin: 24px; color: #111; }
    .card { max-width: 680px; padding: 16px 18px; border: 1px solid #ddd; border-radius: 10px; }
    code { background: #f5f5f5; padding: 2px 6px; border-radius: 6px; }
  </style>
</head>
<body>
  <div class="card">
    <h2>ClipFlow Event Relay</h2>
    <p>网页静态文件未找到，当前使用内置兜底页面。</p>
    <p>事件接口：<code>POST /event</code></p>
    <p>健康检查：<code>GET /</code></p>
  </div>
</body>
</html>
"""


class EventPayload(BaseModel):
    louhao: str = ""
    content: str
    ts: int = 0


class DingTalkProxyPayload(BaseModel):
    url: str
    data: dict


class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.recent_events: dict[str, float] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        log_info(
            f"[EventRelay] WebSocket connected. clients={len(self.active_connections)}"
        )

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        log_info(
            f"[EventRelay] WebSocket disconnected. clients={len(self.active_connections)}"
        )

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return
        import json

        data = json.dumps(message, ensure_ascii=False)
        disconnected = set()
        for connection in self.active_connections:
            try:
                await connection.send_text(data)
            except Exception as exc:
                log_warning(f"[EventRelay] WS send failed: {exc}")
                disconnected.add(connection)
        for conn in disconnected:
            self.active_connections.discard(conn)

    def is_duplicate(self, louhao: str, ts: int) -> bool:
        dedup_key = f"{louhao}:{ts}"
        now = time.time()
        expired = [
            k for k, t in self.recent_events.items() if now - t > DEDUP_WINDOW_SECONDS
        ]
        for k in expired:
            self.recent_events.pop(k, None)
        if dedup_key in self.recent_events:
            return True
        self.recent_events[dedup_key] = now
        return False


def get_event_relay_log_path():
    return get_user_data_dir() / "event_relay_events.jsonl"


class EventRelayServer:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
        event_queue: Optional[queue.Queue] = None,
        log_path=None,
        allowed_origins: Optional[list[str]] = None,
        enable_proxy_dingtalk: bool = False,
    ):
        self.host = host
        self.port = port
        self.event_queue = event_queue or queue.Queue()
        self._event_log_path = log_path or get_event_relay_log_path()
        self.allowed_origins = (
            allowed_origins
            if isinstance(allowed_origins, list) and allowed_origins
            else list(getattr(config, "relay_allowed_origins", []))
        )
        self.enable_proxy_dingtalk = bool(enable_proxy_dingtalk)
        self._thread: Optional[threading.Thread] = None
        self._server = None
        self._manager = ConnectionManager()
        self._app = self._build_app()

    def _append_event_log(self, event: dict) -> None:
        try:
            path = self._event_log_path
            path.parent.mkdir(parents=True, exist_ok=True)
            line = json.dumps(event, ensure_ascii=False)
            with path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception as exc:
            log_warning(f"[EventRelay] write log failed: {exc}")

    def _build_app(self):
        if not FASTAPI_AVAILABLE:
            return None

        @asynccontextmanager
        async def lifespan(app):
            log_info("[EventRelay] service started")
            yield
            log_info("[EventRelay] service stopped")

        app = FastAPI(title="ClipFlow Event Relay", lifespan=lifespan)
        app.add_middleware(
            CORSMiddleware,
            allow_origins=self.allowed_origins or ["http://127.0.0.1:62345"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        def _index_path() -> str:
            path = get_resource_path(os.path.join("upload_event_module", "web", "index.html"))
            return path

        @app.get("/")
        async def root():
            index_path = _index_path()
            if os.path.exists(index_path):
                return FileResponse(index_path)
            return HTMLResponse(FALLBACK_INDEX_HTML)

        @app.get("/index.html")
        async def index_page():
            index_path = _index_path()
            if os.path.exists(index_path):
                return FileResponse(index_path)
            return HTMLResponse(FALLBACK_INDEX_HTML)

        @app.get("/web/index.html")
        async def web_index_page():
            index_path = _index_path()
            if os.path.exists(index_path):
                return FileResponse(index_path)
            return HTMLResponse(FALLBACK_INDEX_HTML)

        @app.post("/event")
        async def receive_event(payload: EventPayload):
            if not payload.content or not payload.content.strip():
                return {"status": "ignored", "reason": "empty content"}
            ts = payload.ts or int(time.time() * 1000)
            if ts and self._manager.is_duplicate(payload.louhao, ts):
                return {"status": "duplicate", "clients": len(self._manager.active_connections)}

            event = {
                "louhao": payload.louhao,
                "content": payload.content,
                "ts": ts,
            }
            self._enqueue_event(event)
            self._append_event_log(event)

            await self._manager.broadcast({"type": "event", "data": event})
            return {"status": "ok", "clients": len(self._manager.active_connections)}

        @app.post("/proxy_dingtalk")
        async def proxy_dingtalk(payload: DingTalkProxyPayload):
            if not self.enable_proxy_dingtalk:
                raise HTTPException(status_code=403, detail="proxy_dingtalk disabled")
            import requests

            try:
                resp = requests.post(
                    payload.url,
                    json=payload.data,
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )
                try:
                    return resp.json()
                except Exception:
                    return {"status": resp.status_code, "text": resp.text}
            except Exception as exc:
                return {"errcode": -1, "errmsg": str(exc)}

        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await self._manager.connect(websocket)
            try:
                while True:
                    data = await websocket.receive_text()
                    if data == "ping":
                        await websocket.send_text("pong")
            except WebSocketDisconnect:
                self._manager.disconnect(websocket)
            except Exception as exc:
                log_warning(f"[EventRelay] WebSocket error: {exc}")
                self._manager.disconnect(websocket)

        return app

    def _enqueue_event(self, event: dict):
        try:
            self.event_queue.put_nowait(event)
        except Exception as exc:
            log_warning(f"[EventRelay] enqueue failed: {exc}")

    def start(self) -> bool:
        if not FASTAPI_AVAILABLE:
            log_error(f"[EventRelay] FastAPI not available: {FASTAPI_IMPORT_ERROR}")
            return False
        if self._app is None:
            log_error("[EventRelay] App initialization failed")
            return False
        try:
            import uvicorn
        except Exception as exc:
            log_error(f"[EventRelay] uvicorn not available: {exc}")
            return False

        if self._thread and self._thread.is_alive():
            return True

        config = uvicorn.Config(
            self._app,
            host=self.host,
            port=self.port,
            log_level="warning",
            access_log=False,
            log_config=None,
        )
        self._server = uvicorn.Server(config)

        def _run():
            try:
                self._server.run()
            except Exception as exc:
                log_error(f"[EventRelay] server error: {exc}")

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        return True

    def stop(self):
        server = self._server
        if server:
            server.should_exit = True
        thread = self._thread
        if thread:
            thread.join(timeout=2)
        if thread and thread.is_alive() and server:
            server.force_exit = True

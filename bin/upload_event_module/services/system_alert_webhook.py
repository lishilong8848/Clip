# -*- coding: utf-8 -*-
import hashlib
import json
import logging
import queue
import socket
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from pathlib import Path
from typing import Any

from ..config import config
from ..utils import BASE_DIR, get_data_file_path


_LOGGER = logging.getLogger("SystemAlertWebhook")
_ALERT_LOG_FILE = Path(get_data_file_path("system_alerts.jsonl"))
_DEDUP_SECONDS = 120
_RATE_WINDOW_SECONDS = 600
_RATE_LIMIT = 30
_SEND_TIMEOUT_SECONDS = 8
_CIRCUIT_FAIL_THRESHOLD = 3
_CIRCUIT_OPEN_SECONDS = 600

_lock = threading.RLock()
_recent_events: dict[str, float] = {}
_rate_window: deque[float] = deque()
_send_queue: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=200)
_worker_started = False
_fail_streak = 0
_circuit_open_until = 0.0


def _now() -> float:
    return time.time()


def _safe_text(value: Any, max_len: int = 1000) -> str:
    text = str(value or "").strip()
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def _get_display_version() -> str:
    try:
        meta_path = Path(BASE_DIR) / "build_meta.json"
        if not meta_path.exists():
            return "unknown"
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        display = str(payload.get("display_version") or "").strip()
        if display:
            return display
        build_id = str(payload.get("build_id") or "").strip()
        return build_id or "unknown"
    except Exception:
        return "unknown"


def _append_local_record(record: dict[str, Any]) -> None:
    try:
        _ALERT_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with _ALERT_LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _prune_state_unlocked(now_ts: float) -> None:
    stale = [k for k, ts in _recent_events.items() if (now_ts - ts) > _DEDUP_SECONDS]
    for key in stale:
        _recent_events.pop(key, None)
    while _rate_window and (now_ts - _rate_window[0]) > _RATE_WINDOW_SECONDS:
        _rate_window.popleft()


def _build_dedup_key(event_code: str, dedup_key: str, detail: str) -> str:
    digest = hashlib.sha1(
        f"{event_code}|{dedup_key}|{detail}".encode("utf-8", errors="ignore")
    ).hexdigest()
    return f"{event_code}:{digest}"


def _build_alert_text(payload: dict[str, Any]) -> str:
    lines = [
        "【事件通告】ClipFlow系统告警",
        f"事件编码: {payload.get('event_code', '')}",
        f"级别: {payload.get('severity', 'error')}",
        f"时间: {payload.get('time_text', '')}",
        f"主机: {payload.get('host', '')}",
        f"版本: {payload.get('version', '')}",
        f"标题: {payload.get('title', '')}",
        f"详情: {payload.get('detail', '')}",
    ]
    trace_path = _safe_text(payload.get("trace_path", ""), 240)
    if trace_path:
        lines.append(f"Trace: {trace_path}")
    extra = payload.get("extra")
    if isinstance(extra, dict) and extra:
        try:
            extra_text = json.dumps(extra, ensure_ascii=False)
        except Exception:
            extra_text = str(extra)
        lines.append(f"扩展: {_safe_text(extra_text, 500)}")
    return "\n".join(lines)


def _build_http_payload(format_name: str, text: str) -> dict[str, Any]:
    if format_name == "dingtalk":
        return {"msgtype": "text", "text": {"content": text}}
    return {"msg_type": "text", "content": {"text": text}}


def _send_http(webhook: str, format_name: str, text: str) -> tuple[bool, str]:
    body = _build_http_payload(format_name, text)
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        webhook,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT_SECONDS) as resp:
            resp_text = resp.read().decode("utf-8", errors="ignore")
            ok = 200 <= int(resp.status) < 300
            return ok, f"HTTP {resp.status} {_safe_text(resp_text, 180)}"
    except urllib.error.HTTPError as exc:
        try:
            body_text = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            body_text = ""
        return False, f"HTTP {exc.code} {_safe_text(body_text, 180)}"
    except Exception as exc:
        return False, _safe_text(str(exc), 180)


def _ensure_worker() -> None:
    global _worker_started
    with _lock:
        if _worker_started:
            return
        thread = threading.Thread(target=_worker_loop, daemon=True)
        thread.start()
        _worker_started = True


def _worker_loop() -> None:
    global _fail_streak, _circuit_open_until
    while True:
        item = _send_queue.get()
        try:
            if not isinstance(item, dict):
                continue
            now_ts = _now()
            with _lock:
                if now_ts < _circuit_open_until:
                    _append_local_record(
                        {
                            "ts": int(now_ts * 1000),
                            "status": "circuit_open",
                            "event_code": item.get("event_code", ""),
                            "reason": "circuit breaker active",
                        }
                    )
                    continue

            ok, result = _send_http(
                item.get("webhook", ""),
                item.get("webhook_format", "feishu"),
                _build_alert_text(item),
            )
            now_ms = int(_now() * 1000)
            with _lock:
                if ok:
                    _fail_streak = 0
                else:
                    _fail_streak += 1
                    if _fail_streak >= _CIRCUIT_FAIL_THRESHOLD:
                        _circuit_open_until = _now() + _CIRCUIT_OPEN_SECONDS
            _append_local_record(
                {
                    "ts": now_ms,
                    "status": "sent" if ok else "fail",
                    "event_code": item.get("event_code", ""),
                    "severity": item.get("severity", ""),
                    "result": result,
                    "dedup_key": item.get("dedup_hash", ""),
                }
            )
            if not ok:
                _LOGGER.warning("System alert send failed: %s", result)
        except Exception as exc:
            _append_local_record(
                {
                    "ts": int(_now() * 1000),
                    "status": "worker_error",
                    "reason": _safe_text(str(exc), 180),
                }
            )
        finally:
            _send_queue.task_done()


def send_system_alert(
    event_code: str,
    title: str,
    detail: str,
    severity: str = "error",
    dedup_key: str = "",
    extra: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    webhook = _safe_text(getattr(config, "relay_webhook", ""), 500)
    webhook_format = _safe_text(getattr(config, "relay_webhook_format", "feishu"), 20)
    if not webhook:
        return False, "webhook not configured"

    now_ts = _now()
    detail_text = _safe_text(detail, 1200)
    dedup_hash = _build_dedup_key(event_code, dedup_key, detail_text)

    with _lock:
        _prune_state_unlocked(now_ts)
        if now_ts < _circuit_open_until:
            _append_local_record(
                {
                    "ts": int(now_ts * 1000),
                    "status": "circuit_open",
                    "event_code": event_code,
                    "dedup_key": dedup_hash,
                }
            )
            return False, "circuit open"

        if dedup_hash in _recent_events:
            _append_local_record(
                {
                    "ts": int(now_ts * 1000),
                    "status": "suppressed",
                    "event_code": event_code,
                    "reason": "dedup",
                    "dedup_key": dedup_hash,
                }
            )
            return False, "suppressed dedup"

        if len(_rate_window) >= _RATE_LIMIT:
            _append_local_record(
                {
                    "ts": int(now_ts * 1000),
                    "status": "suppressed",
                    "event_code": event_code,
                    "reason": "rate_limited",
                    "dedup_key": dedup_hash,
                }
            )
            return False, "suppressed rate limit"

        _recent_events[dedup_hash] = now_ts
        _rate_window.append(now_ts)

    payload = {
        "event_code": _safe_text(event_code, 80),
        "severity": _safe_text(severity, 20) or "error",
        "title": _safe_text(title, 160),
        "detail": detail_text,
        "host": socket.gethostname(),
        "version": _get_display_version(),
        "time_text": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "trace_path": _safe_text((extra or {}).get("trace_path", ""), 260)
        if isinstance(extra, dict)
        else "",
        "extra": extra if isinstance(extra, dict) else None,
        "webhook": webhook,
        "webhook_format": webhook_format if webhook_format in ("feishu", "dingtalk") else "feishu",
        "dedup_hash": dedup_hash,
    }

    _ensure_worker()
    try:
        _send_queue.put_nowait(payload)
        return True, "queued"
    except queue.Full:
        _append_local_record(
            {
                "ts": int(_now() * 1000),
                "status": "suppressed",
                "event_code": event_code,
                "reason": "queue_full",
                "dedup_key": dedup_hash,
            }
        )
        return False, "queue full"

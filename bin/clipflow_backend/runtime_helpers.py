# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import socket
import time

from lan_bitable_template_portal.portal_service import external_real_write_guard
from upload_event_module.services.robot_webhook import send_text_to_open_ids


def wait_until_listening(host: str, port: int, *, timeout_s: float = 3.0) -> bool:
    probe_host = str(host or "127.0.0.1").strip()
    if probe_host in {"0.0.0.0", "::"}:
        probe_host = "127.0.0.1"
    deadline = time.monotonic() + max(0.1, float(timeout_s or 0))
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((probe_host, int(port)), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def mock_external_enabled() -> bool:
    return os.environ.get("CLIPFLOW_BACKEND_MOCK_EXTERNAL") == "1"


def env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    try:
        value = float(str(os.environ.get(name, "") or "").strip() or default)
    except Exception:
        value = float(default)
    return max(float(minimum), min(float(maximum), value))


def external_guard_status() -> dict:
    return external_real_write_guard()


def send_text_to_open_ids_guarded(text: str, recipients: list[str]) -> tuple[bool, str, list[dict]]:
    clean_recipients = [
        str(open_id or "").strip()
        for open_id in (recipients or [])
        if str(open_id or "").strip()
    ]
    guard = external_guard_status()
    if guard.get("mock_external"):
        return True, "mock external send skipped", [
            {"open_id": open_id, "ok": True, "message": "mock external send skipped"}
            for open_id in clean_recipients
        ]
    if not guard.get("real_write_allowed"):
        return False, str(guard.get("reason") or "真实外部写入未确认。"), []
    return send_text_to_open_ids(text, clean_recipients)

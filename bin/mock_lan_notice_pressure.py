# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import concurrent.futures
import json
import tempfile
import time
from pathlib import Path

from lan_bitable_template_portal.state_store import LanPortalStateStore


def _sleep_until(target: float) -> None:
    delay = float(target or 0) - time.perf_counter()
    if delay > 0:
        time.sleep(delay)


def run_mock_pressure(
    *,
    total: int = 10,
    message_workers: int = 5,
    qt_interval: float = 0.01,
    upload_interval: float = 0.05,
) -> dict:
    total = max(1, int(total or 1))
    message_workers = max(1, int(message_workers or 1))
    accepted_at = time.perf_counter()
    message_started: list[float] = []
    message_finished: list[float] = []
    qt_displayed: list[float] = []
    uploaded: list[float] = []

    with tempfile.TemporaryDirectory() as tmp:
        store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
        for index in range(total):
            job_id = f"job-{index + 1:03d}"
            store.upsert_runtime_queue_item("message", job_id)
            store.upsert_runtime_queue_item("qt_action", job_id)
            store.upsert_runtime_queue_item("bitable_upload", job_id)

        def send_message(job_id: str) -> str:
            store.mark_runtime_queue_item("message", job_id, "processing")
            message_started.append(time.perf_counter())
            time.sleep(0.02)
            message_finished.append(time.perf_counter())
            store.mark_runtime_queue_item("message", job_id, "done")
            return job_id

        message_jobs = [f"job-{index + 1:03d}" for index in range(total)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=message_workers) as pool:
            list(pool.map(send_message, message_jobs))

        next_qt_at = time.perf_counter()
        for job_id in message_jobs:
            _sleep_until(next_qt_at)
            store.mark_runtime_queue_item("qt_action", job_id, "processing")
            qt_displayed.append(time.perf_counter())
            store.mark_runtime_queue_item("qt_action", job_id, "done")
            next_qt_at += max(0.0, float(qt_interval or 0))

        next_upload_at = time.perf_counter()
        for job_id in message_jobs:
            _sleep_until(next_upload_at)
            store.mark_runtime_queue_item("bitable_upload", job_id, "processing")
            uploaded.append(time.perf_counter())
            store.mark_runtime_queue_item("bitable_upload", job_id, "done")
            next_upload_at += max(0.0, float(upload_interval or 0))

        counts = store.runtime_queue_counts()

    first_window = sorted(message_started)[: min(message_workers, len(message_started))]
    first_message_span_ms = (
        (max(first_window) - accepted_at) * 1000.0 if first_window else 0.0
    )
    qt_min_gap_ms = min(
        ((b - a) * 1000.0 for a, b in zip(qt_displayed, qt_displayed[1:])),
        default=0.0,
    )
    upload_min_gap_ms = min(
        ((b - a) * 1000.0 for a, b in zip(uploaded, uploaded[1:])),
        default=0.0,
    )
    ok = (
        len(message_started) == total
        and len(uploaded) == total
        and first_message_span_ms < 1000.0
        and all(status_counts.get("done") == total for status_counts in counts.values())
    )
    return {
        "ok": ok,
        "total": total,
        "message_workers": message_workers,
        "first_message_span_ms": round(first_message_span_ms, 1),
        "qt_min_gap_ms": round(qt_min_gap_ms, 1),
        "upload_min_gap_ms": round(upload_min_gap_ms, 1),
        "counts": counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Mock LAN notice queue pressure test.")
    parser.add_argument("--total", type=int, default=10)
    parser.add_argument("--message-workers", type=int, default=5)
    parser.add_argument("--qt-interval", type=float, default=0.01)
    parser.add_argument("--upload-interval", type=float, default=0.05)
    args = parser.parse_args()
    result = run_mock_pressure(
        total=args.total,
        message_workers=args.message_workers,
        qt_interval=args.qt_interval,
        upload_interval=args.upload_interval,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

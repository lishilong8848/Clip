# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parents[1]
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from fastapi.testclient import TestClient  # noqa: E402

from clipflow_backend.main import FastAPIPortalController  # noqa: E402
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME, PortalAuthManager  # noqa: E402
from lan_bitable_template_portal.portal_service import MaintenancePortalService  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class MockPortalService:
    _last_loaded_at = ""
    _last_loaded_ts = 0.0
    _load_warnings: list[str] = []

    def __init__(self, scenario: str = "accepted") -> None:
        self._jobs: dict[str, dict] = {}
        self._jobs_lock = threading.RLock()
        self._scenario = str(scenario or "accepted").strip().lower()
        self._created = 0

    def _failure_for_index(self, index: int) -> str:
        scenario = self._scenario
        if scenario == "failed-network":
            return "网络连接失败"
        if scenario == "failed-remote-missing":
            return "飞书接口失败: code=1254002, msg=Fail"
        if scenario == "mixed":
            if index % 3 == 1:
                return ""
            if index % 3 == 2:
                return "网络连接失败"
            return "飞书接口失败: code=1254002, msg=Fail"
        return ""

    def create_action_job(self, payload: dict) -> tuple[str, bool]:
        now = time.time()
        job_id = uuid.uuid4().hex
        with self._jobs_lock:
            self._created += 1
            failure = self._failure_for_index(self._created)
            classified = MaintenancePortalService.classify_job_error(failure)
            self._jobs[job_id] = {
                "job_id": job_id,
                "phase": "failed" if failure else "accepted",
                "accepted_at": now,
                "created_at": time.strftime("%Y-%m-%d %H:%M"),
                "updated_at": time.strftime("%Y-%m-%d %H:%M"),
                "request": dict(payload or {}),
                "error": failure,
                "error_category": classified["error_category"],
                "error_retryable": classified["error_retryable"],
            }
        return job_id, False

    def get_job(self, job_id: str) -> dict | None:
        with self._jobs_lock:
            job = self._jobs.get(str(job_id or ""))
            return dict(job) if job else None

    def mark_job(self, job_id: str, **patch) -> None:
        with self._jobs_lock:
            job = self._jobs.get(str(job_id or ""))
            if job:
                job.update(patch)
                job["updated_at"] = time.strftime("%Y-%m-%d %H:%M")

    def delete_action_job(self, job_id: str) -> bool:
        with self._jobs_lock:
            return self._jobs.pop(str(job_id or ""), None) is not None


def run_pressure(
    count: int,
    concurrency: int,
    *,
    scopes: list[str] | None = None,
    per_scope: int = 0,
    query_jobs: bool = True,
    scenario: str = "accepted",
) -> dict:
    controller = FastAPIPortalController(host="127.0.0.1", port=18766)
    original_service = PortalRuntime.service
    original_auth = PortalRuntime.auth_manager
    original_store = PortalRuntime.state_store
    temp_dir = tempfile.TemporaryDirectory()
    session_id = "mock-pressure-session"
    try:
        PortalRuntime.service = MockPortalService(scenario=scenario)
        PortalRuntime.state_store = LanPortalStateStore(Path(temp_dir.name) / "state.sqlite3")
        PortalRuntime.auth_manager = PortalAuthManager()
        PortalRuntime.auth_manager._state_store = PortalRuntime.state_store
        PortalRuntime.auth_manager.upsert_permission_user(
            open_id="ou_mock_admin",
            name="mock-admin",
            role="admin",
            scopes=["ALL"],
            enabled=True,
            updated_by="mock-pressure",
        )
        with PortalRuntime.auth_manager._lock:
            PortalRuntime.auth_manager._sessions[session_id] = {
                "session_id": session_id,
                "user": {"name": "mock-admin", "open_id": "ou_mock_admin"},
                "role": "admin",
                "allowed_scopes": ["ALL"],
                "expires_at": time.time() + 3600,
            }
        client = TestClient(controller._build_app())
        headers = {"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"}

        scope_plan: list[tuple[str, int]] = []
        normalized_scopes = [
            str(scope or "").strip().upper()
            for scope in (scopes or [])
            if str(scope or "").strip()
        ]
        if normalized_scopes and per_scope > 0:
            for scope in normalized_scopes:
                for scope_index in range(1, per_scope + 1):
                    scope_plan.append((scope, scope_index))
        else:
            for index in range(1, count + 1):
                scope_plan.append(("ALL", index))
        count = len(scope_plan)

        def submit(index: int, scope: str, scope_index: int) -> tuple[int, str, int, int, str]:
            response = client.post(
                "/api/workbench-actions",
                headers=headers,
                json={
                    "scope": scope,
                    "work_type": "maintenance",
                    "action": "start",
                    "record_id": f"mock-record-{scope}-{scope_index}",
                    "operation_id": f"mock-op-{scope}-{scope_index}",
                    "title": f"{scope}楼测试测试测试{scope_index}",
                    "location": "测试测试测试",
                    "content": "测试测试测试",
                    "reason": "测试测试测试",
                    "impact": "测试测试测试",
                    "progress": "测试测试测试",
                    "maintenance_cycle": "/",
                },
            )
            job_id = ""
            try:
                job_id = str((response.json().get("data") or {}).get("job_id") or "")
            except Exception:
                pass
            return index, scope, scope_index, response.status_code, job_id

        def query_job(job_id: str) -> tuple[str, int, str]:
            response = client.get(f"/api/jobs/{job_id}", headers=headers)
            phase = ""
            try:
                phase = str((response.json().get("data") or {}).get("phase") or "")
            except Exception:
                pass
            return job_id, response.status_code, phase

        started = time.perf_counter()
        results = []
        submit_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            futures = [
                pool.submit(submit, index, scope, scope_index)
                for index, (scope, scope_index) in enumerate(scope_plan, start=1)
            ]
            for future in as_completed(futures):
                results.append(future.result())
        submit_elapsed = time.perf_counter() - submit_started
        ok_count = sum(1 for _, _, _, status, job_id in results if status == 202 and job_id)
        per_scope_counts: dict[str, dict[str, int]] = {}
        for _, scope, _, status, job_id in results:
            bucket = per_scope_counts.setdefault(scope, {"accepted": 0, "failed": 0})
            if status == 202 and job_id:
                bucket["accepted"] += 1
            else:
                bucket["failed"] += 1
        job_ids = [job_id for _, _, _, status, job_id in results if status == 202 and job_id]
        job_query_results: list[tuple[str, int, str]] = []
        job_query_elapsed = 0.0
        if query_jobs and job_ids:
            query_started = time.perf_counter()
            with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
                futures = [pool.submit(query_job, job_id) for job_id in job_ids]
                for future in as_completed(futures):
                    job_query_results.append(future.result())
            job_query_elapsed = time.perf_counter() - query_started
        stats_started = time.perf_counter()
        stats_response = client.get("/api/backend/stats", headers=headers)
        stats_elapsed = time.perf_counter() - stats_started
        stats_data = {}
        try:
            stats_data = stats_response.json().get("data") or {}
        except Exception:
            stats_data = {}
        elapsed = time.perf_counter() - started
        job_query_ok = sum(
            1 for _, status, phase in job_query_results if status == 200 and phase
        )
        phase_counts: dict[str, int] = {}
        for _, _, phase in job_query_results:
            if not phase:
                phase = "unknown"
            phase_counts[phase] = phase_counts.get(phase, 0) + 1
        return {
            "count": count,
            "concurrency": concurrency,
            "scopes": normalized_scopes or ["ALL"],
            "per_scope": per_scope if normalized_scopes and per_scope > 0 else 0,
            "per_scope_counts": per_scope_counts,
            "scenario": scenario,
            "accepted": ok_count,
            "failed": count - ok_count,
            "elapsed_seconds": round(elapsed, 3),
            "submit_seconds": round(submit_elapsed, 3),
            "submit_average_ms": round((submit_elapsed / max(1, count)) * 1000, 2),
            "job_query_enabled": bool(query_jobs),
            "job_query_ok": job_query_ok,
            "job_query_seconds": round(job_query_elapsed, 3),
            "job_phase_counts": phase_counts,
            "stats_status": stats_response.status_code,
            "stats_seconds": round(stats_elapsed, 3),
            "stats": {
                "message_queue_size": stats_data.get("message_queue_size"),
                "message_worker_count": stats_data.get("message_worker_count"),
                "job_count": stats_data.get("job_count"),
                "job_phase_counts": stats_data.get("job_phase_counts"),
                "qt_queue_size": stats_data.get("qt_queue_size"),
                "qt_outbox_pending": stats_data.get("qt_outbox_pending"),
                "upload_wait_size": stats_data.get("upload_wait_size"),
                "source_refresh_inflight": stats_data.get("source_refresh_inflight"),
                "repair_refresh_inflight": stats_data.get("repair_refresh_inflight"),
            },
        }
    finally:
        PortalRuntime.service = original_service
        PortalRuntime.auth_manager = original_auth
        PortalRuntime.state_store = original_store
        temp_dir.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description="Mock LAN portal submit pressure test.")
    parser.add_argument("--count", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument(
        "--scopes",
        default="",
        help="逗号分隔楼栋，例如 A,B,C,D,E,H；配合 --per-scope 模拟多楼并发。",
    )
    parser.add_argument(
        "--per-scope",
        type=int,
        default=0,
        help="每个楼栋提交多少条；大于 0 且指定 --scopes 时覆盖 --count。",
    )
    parser.add_argument("--repeat", type=int, default=1, help="连续压测轮数。")
    parser.add_argument("--pause", type=float, default=0.2, help="每轮之间暂停秒数。")
    parser.add_argument(
        "--max-submit-average-ms",
        type=float,
        default=0.0,
        help="提交平均耗时阈值，超过则脚本返回失败；0 表示不检查。",
    )
    parser.add_argument(
        "--max-total-seconds",
        type=float,
        default=0.0,
        help="单轮总耗时阈值，超过则脚本返回失败；0 表示不检查。",
    )
    parser.add_argument(
        "--no-job-query",
        action="store_true",
        help="只测提交通告，不再并发查询每个 job 状态。",
    )
    parser.add_argument(
        "--scenario",
        choices=["accepted", "failed-network", "failed-remote-missing", "mixed"],
        default="accepted",
        help="模拟外部链路结果，不触发真实飞书或多维。",
    )
    args = parser.parse_args()
    repeat = max(1, int(args.repeat or 1))
    runs = []
    ok = True
    for index in range(repeat):
        result = run_pressure(
            max(1, args.count),
            max(1, args.concurrency),
            scopes=[item.strip() for item in str(args.scopes or "").split(",") if item.strip()],
            per_scope=max(0, int(args.per_scope or 0)),
            query_jobs=not bool(args.no_job_query),
            scenario=args.scenario,
        )
        result["run"] = index + 1
        threshold_failures = []
        if (
            float(args.max_submit_average_ms or 0) > 0
            and float(result.get("submit_average_ms") or 0) > float(args.max_submit_average_ms)
        ):
            threshold_failures.append("submit_average_ms")
        if (
            float(args.max_total_seconds or 0) > 0
            and float(result.get("elapsed_seconds") or 0) > float(args.max_total_seconds)
        ):
            threshold_failures.append("elapsed_seconds")
        if int(result.get("accepted") or 0) != int(result.get("count") or 0):
            threshold_failures.append("accepted_count")
        result["threshold_failures"] = threshold_failures
        ok = ok and not threshold_failures
        runs.append(result)
        if index + 1 < repeat:
            time.sleep(max(0.0, float(args.pause or 0)))
    summary = {
        "ok": ok,
        "repeat": repeat,
        "count": int(runs[0].get("count") or max(1, args.count)) if runs else max(1, args.count),
        "concurrency": max(1, args.concurrency),
        "scopes": runs[0].get("scopes") if runs else [],
        "per_scope": int(runs[0].get("per_scope") or 0) if runs else 0,
        "scenario": args.scenario,
        "runs": runs,
        "max_submit_average_ms": max(
            (float(item.get("submit_average_ms") or 0) for item in runs),
            default=0.0,
        ),
        "max_elapsed_seconds": max(
            (float(item.get("elapsed_seconds") or 0) for item in runs),
            default=0.0,
        ),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

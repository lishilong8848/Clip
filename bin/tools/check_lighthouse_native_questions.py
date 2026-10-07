"""Opt-in acceptance probe: LighthouseOpenClaw DEFAULT engine + real configured model + synthetic business APIs.

Ownership: this file is a probe tool only; it never touches production code or business data. All business
endpoints are synthetic read-only FastAPI fixtures backed by a temporary state DB. Nothing is written to real
data. Synthetic endpoints mirror the native data structures used by the DEFAULT engine's OpenClaw tooling
(cabinet overview/racks, learning papers items+stats, daily-task checklist, plan-convergence rulesets list,
work-order items+steps+progress). The fixture authorization is strict: scope E only; any other explicit scope
returns HTTP 403 rather than silently accepting another building.

Routing notes:
  * deterministic fast paths (own-model, events, unfinished repairs) run without any model/runtime call
  * provider-loop scenarios run the real configured model, which must perform discover -> query against the
    running fixture server. They are real acceptance checks and require the installed runtime.
  * exception output is sanitized: only the exception type (plus status code) is printed, never raw upstream
    or provider text, so credentials or provider echoes cannot leak.

Opt-in only: exit code 0 = all selected scenarios satisfied; 1 = at least one failed; 2 = no configured model
profile. Use --case to run a substring filter and --deterministic for the offline (no real model) scenarios.
"""
import argparse
import asyncio
import datetime as dt
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_ai import AssistantError, CustomModel, LighthouseAssistant, safe_text, unprotect_key
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.lighthouse_openclaw import LighthouseOpenClaw
from lan_bitable_template_portal.lighthouse_queries import TZ
from lan_bitable_template_portal.lighthouse_runtime import free_port, install_model_route
from check_lighthouse_general import configured_profile
from test_lighthouse_stream import Store


class TracedLighthouseOpenClaw(LighthouseOpenClaw):
    """Subclass that keeps inherited deterministic fast paths but counts provider-loop entries."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.provider_calls = 0

    def make_agent(self, profile, *, actor, turn, emit, **kwargs):
        # Only reached when inference actually enters the OpenClaw tool/provider loop;
        # deterministic business fast paths return before this hook.
        self.provider_calls += 1
        return super().make_agent(profile, actor=actor, turn=turn, emit=emit, **kwargs)


def _guard(scope, allowed):
    """Resolve an actor-scope query param: E (or omitted/ALL) is allowed; anything else is 403.

    This ensures the synthetic fixture never silently accepts a scope the probe actor does not hold,
    mirroring production authorization (unknown scopes are rejected).
    """
    normalized = (scope or "ALL").strip().upper()
    if normalized in ("", "ALL"):
        return next(iter(sorted(allowed)))
    if normalized not in allowed:
        raise HTTPException(status_code=403, detail="无权访问该楼栋")
    return normalized


def _cabinet_overview(scope):
    counts = {"formal": 8, "test": 0, "off": 3, "unknown": 1}
    racks = [
        {"room": "101", "rack": "A%02d" % i, "state": "formal", "rack_type": "电力机柜",
         "room_label": "E楼机房101", "rack_label": "A%02d" % i}
        for i in range(1, 9)]
    racks += [
        {"room": "101", "rack": "C01", "state": "off", "rack_type": "电力机柜",
         "room_label": "E楼机房101", "rack_label": "C01"},
        {"room": "101", "rack": "C02", "state": "off", "rack_type": "电力机柜",
         "room_label": "E楼机房101", "rack_label": "C02"},
        {"room": "101", "rack": "C03", "state": "off", "rack_type": "电力机柜",
         "room_label": "E楼机房101", "rack_label": "C03"},
        {"room": "101", "rack": "X01", "state": "unknown", "rack_type": "电力机柜",
         "room_label": "E楼机房101", "rack_label": "X01"}]
    return {
        "scope": scope, "configured": True, "activated": True, "history_ready": True,
        "source": "synthetic", "counts": counts,
        "rooms": [{"id": "101", "name": "E-101机房", "counts": counts,
                   "types": {"电力机柜": 12}, "carrier": False, "unlocated": 0,
                   "unlocated_counts": {"unknown": 0, "off": 0}}],
        "racks": racks, "issues": [], "version": "synthetic", "updated_at": 0,
        "daily": [], "record_count": 0, "inventory_only": 0, "sheet_formats": [],
        "table_url": "/synthetic/cabinet", "error": "",
        "export_state": {"scope": scope, "pending": 0}}


def build_fixture(calls):
    """Register synthetic read-only business endpoints that mirror native data shapes."""
    allowed = {"E"}
    fixture_today = dt.datetime.now(TZ).date()
    month = fixture_today.strftime("%Y-%m")
    app = FastAPI()

    @app.get("/api/workbench")
    async def workbench(scope: str = "ALL", work_type: str = "", sections: str = "records,ongoing",
                        records_page: int = 1, records_page_size: int = 200,
                        ongoing_page: int = 1, ongoing_page_size: int = 200):
        scope = _guard(scope, allowed)
        calls.append("workbench")
        pending = [{"record_id": "recSyntheticPlan%d" % i, "scope": scope,
                    "title": "E楼待发计划%d" % i, "source_progress": "未开始",
                    "work_type": "change" if i < 2 else "repair" if i == 2 else "maintenance"}
                   for i in range(250)]
        ongoing = [{"target_record_id": "recSyntheticNotice%d" % i,
                    "active_item_id": "activeSyntheticNotice%d" % i,
                    "source_record_id": "recSyntheticStarted%d" % i,
                    "scope": scope, "building_codes": [scope], "status": "开始",
                    "title": "E楼进行中通告%d" % i,
                    "work_type": "repair" if i < 5 else "maintenance"}
                   for i in range(7)]
        started = [{"record_id": row["source_record_id"], "scope": scope,
                    "title": row["title"], "work_type": row["work_type"],
                    "source_progress": "进行中", "linked_ongoing": True} for row in ongoing]
        records = pending + started
        if work_type:
            records = [row for row in records if row["work_type"] == work_type]
            ongoing = [row for row in ongoing if row["work_type"] == work_type]

        def paged(rows, page, size):
            offset = (page - 1) * size
            return rows[offset:offset + size], {"total": len(rows), "page": page,
                "page_size": size, "has_more": offset + size < len(rows)}

        record_page, record_meta = paged(records, records_page, records_page_size)
        ongoing_page_rows, ongoing_meta = paged(ongoing, ongoing_page, ongoing_page_size)
        data = {"source_snapshot_ready": True, "payload_version": "synthetic-1",
                "record_type_counts": {"maintenance": 247, "change": 2, "repair": 1}, "warnings": []}
        if "records" in sections.split(","):
            data.update(records=record_page, records_pagination=record_meta)
        if "ongoing" in sections.split(","):
            data.update(ongoing=ongoing_page_rows, ongoing_pagination=ongoing_meta)
        return {"ok": True, "data": data}

    @app.get("/api/cabinet-power/batches")
    async def cabinet_batches(scope: str = "ALL", status: str = "todo", page: int = 1, page_size: int = 100):
        scope = _guard(scope, allowed)
        calls.append("batches")
        return {"ok": True, "data": {"total": 1, "page": 1, "items": [{
            "batch_id": "batchSyntheticPending", "scope": scope, "is_todo": True, "pending_rows": 2}]}}

    @app.get("/api/critical-guard/tasks")
    async def guard_tasks(scope: str = "ALL"):
        _guard(scope, allowed)
        calls.append("guard")
        return {"ok": True, "data": {"tasks": []}}

    @app.get("/api/events/monthly")
    async def event_monthly(scope: str = "ALL", month: str = month):
        scope = _guard(scope, allowed)
        calls.append("event-monthly")
        records = []
        for index in range(5):
            processing = index < 2
            records.append({
                "record_id": "recSyntheticEvent%d" % index,
                "title": "E楼隔离测试事件%d" % index,
                "scope": scope,
                "occurrence_time": "%s 09:%02d" % (fixture_today.isoformat(), index),
                "end_time": "" if processing else "%s 12:00" % fixture_today.isoformat(),
                "status": "处理中" if processing else "已结束",
            })
        return {"ok": True, "data": {
            "scope": scope, "month": month, "date_field": "occurrence_time",
            "snapshot_exists": True, "last_refreshed_at": 0,
            "records": records}}

    @app.get("/api/repair-management/records")
    async def repair_records(scope: str = "E", state: str = "active", period: str = "all",
                             limit: int = 200, summary_only: str = "1", offset: int = 0,
                             page: int = 1, page_size: int = 20, search: str = "", work_type: str = ""):
        scope = _guard(scope, allowed)
        calls.append("repair-records")
        rows = [{"record_id": "recSyntheticRepair%d" % i,
                 "title": "E楼未完成维修项目%d" % i,
                 "scope": scope,
                 "workflow": "维修中",
                 "is_completed": False,
                 "followup_state_verified": True}
                for i in range(1, 4)]
        return {"ok": True, "data": {
            "scope": scope, "total": 3, "records": rows,
            "has_more": False, "offset": offset}}

    @app.get("/api/repair-management/records/{record_id}")
    async def repair(record_id: str, scope: str = "ALL"):
        scope = _guard(scope, allowed)
        if record_id != "recSyntheticRepair":
            return {"ok": False, "error": "Unknown synthetic record"}
        calls.append("repair")
        return {"ok": True, "data": {"record": {
            "record_id": record_id, "scope": scope, "title": "E楼隔离测试维修",
            "followup_count": 3, "followup_state_verified": True,
            "progress_percent": 58, "workflow": "维修中"}}}

    @app.get("/api/capacity/water/records")
    async def water(scope: str = "ALL", start_date: str = "", end_date: str = "",
                    limit: int = 100, offset: int = 0, month: str = ""):
        scope = _guard(scope, allowed)
        calls.append("water")
        return {"ok": True, "data": {"scope": scope, "total": 1, "records": [{
            "record_id": "recSyntheticWater", "scope": scope, "title": "E楼隔离测试水表",
            "statistic_date": fixture_today.isoformat(), "computed_usage": 112, "unit": "t"}]}}

    @app.get("/api/cabinet-power/overview")
    async def cabinet_overview(scope: str = "ALL", summary: str = ""):
        scope = _guard(scope, allowed)
        calls.append("cabinet")
        overview = _cabinet_overview(scope)
        if summary == "1":
            overview = {k: v for k, v in overview.items() if k != "racks"}
        return {"ok": True, "data": overview}

    @app.get("/api/cabinet-power/racks")
    async def cabinet_racks(scope: str = "ALL"):
        scope = _guard(scope, allowed)
        calls.append("cabinet")
        return {"ok": True, "data": {"items": _cabinet_overview(scope)["racks"],
                                     "version": "synthetic"}}

    @app.get("/api/learning/papers")
    async def learning_papers(scope: str = "ALL", today: str = "", date: str = "", from_: str = "",
                              to: str = "", page: int = 1, page_size: int = 20, search: str = "",
                              bank: str = "", kind: str = "", period: str = ""):
        scope = _guard(scope, allowed)
        calls.append("learning")
        return {"ok": True, "data": {
            "scope": scope, "total": 1, "page": 1, "page_size": 20,
            "today": fixture_today.isoformat(),
            "items": [{
                "id": "paperSyntheticProgress", "date": date or fixture_today.isoformat(), "scope": scope,
                "shortage": {}, "created_at": "", "version": 0,
                "status": "pending", "completed_at": "", "sync_pending": False,
                "questions": [],
                "stats": {"total": 10, "answered": 3, "shortage": 0}}]}}

    @app.get("/api/assistant/question-bank")
    async def question_bank(scope: str = "ALL", search: str = "", bank: str = "",
                            page: int = 1, page_size: int = 20):
        scope = _guard(scope, allowed)
        calls.append("question-bank")
        terms = search.casefold()
        items = [{"id": "questionSyntheticUPS", "version": 1, "bank": "written",
                  "stem": "UPS不间断电源的主要作用是什么？", "type": "single",
                  "options": [], "topic": "UPS 不间断电源 备用电源",
                  "answer_text": "市电中断时由储能继续供电，保障负载供电连续性。",
                  "analysis": "不代表当前任何楼栋设备的实时运行状态。"}]
        if terms and not any(word in terms for word in ("ups", "不间断", "电源", "供电")):
            items = []
        return {"ok": True, "data": {"scope": scope, "items": items, "total": len(items),
            "page": page, "page_size": page_size, "source": "题库本地缓存",
            "updated_at": fixture_today.isoformat(), "visibility": "管理员完整题库"}}

    @app.get("/api/daily-tasks")
    async def daily_tasks(scope: str = "ALL", date: str = ""):
        scope = _guard(scope, allowed)
        calls.append("daily")
        tasks = [
            {"task_id": "notice_1", "title": "E楼晨会准备", "status_tone": "ongoing",
             "time": "08:30", "building": "E楼", "sort_time": 830,
             "category": "notice", "category_label": "通告"},
            {"task_id": "event_1", "title": "E楼机房巡检", "status_tone": "ongoing",
             "time": "09:00", "building": "E楼", "sort_time": 900,
             "category": "event", "category_label": "事件"},
        ]
        return {"ok": True, "data": {
            "scope": scope, "date": date or fixture_today.isoformat(),
            "generated_at": fixture_today.isoformat() + " 08:00:00",
            "stats": {"total": 2, "ongoing": 2, "completed": 0, "attention": 0},
            "categories": [
                {"key": "notice", "label": "通告", "count": 1, "tasks": [tasks[0]]},
                {"key": "event", "label": "事件", "count": 1, "tasks": [tasks[1]]}],
            "tasks": tasks, "warnings": []}}

    @app.get("/api/drills")
    async def drill_list(scope: str = "ALL", month: str = "", year: int = 0,
                         page: int = 1, page_size: int = 20):
        scope = _guard(scope, allowed)
        calls.append("drill")
        return {"ok": True, "data": {"total": 1, "page": 1, "page_size": 20, "items": [{
            "drill_id": "drillSyntheticPublished", "name": "E楼隔离测试演练",
            "assigned_scopes": [scope], "status": "published", "execution": {"status": "in_progress"},
            "year": fixture_today.year, "month": fixture_today.month, "created_at": ""}]}}

    @app.get("/api/plan-convergence/rulesets")
    async def rulesets(scope: str = "ALL", refresh: str = "", page: int = 1, page_size: int = 20):
        scope = _guard(scope, allowed)
        calls.append("convergence")
        return {"ok": True, "data": [
            {"id": 1, "name": "E楼隔离收敛规则1", "remark": "", "created_at": "",
             "updated_at": "", "item_count": 3},
            {"id": 2, "name": "E楼隔离收敛规则2", "remark": "", "created_at": "",
             "updated_at": "", "item_count": 1}]}

    @app.get("/api/assistant/work-orders")
    async def work_orders(scope: str = "ALL", state: str = "all", search: str = "",
                          work_type: str = "", group_id: str = "", page: int = 1, page_size: int = 20):
        scope = _guard(scope, allowed)
        calls.append("work-orders")
        total_steps = 48
        steps = []
        for i in range(total_steps):
            steps.append({
                "number": i + 1, "run_index": 0, "run_label": "默认轮", "step_index": i,
                "source_step_index": i, "repeat_round": 0,
                "content": "E楼隔离水质循环第%d步" % (i + 1),
                "operator_required": True, "reviewer_required": i % 4 == 3,
                "operator_confirmed": i < 45, "reviewer_confirmed": i < 45,
                "confirmations": {
                    "operator": {"assigned_name": "隔离操作员", "actual_name": "隔离操作员",
                                 "confirmed_at": "" if i >= 45 else "08:00"},
                    "reviewer": {"assigned_name": "隔离审核员", "actual_name": "隔离审核员",
                                 "confirmed_at": "" if (i % 4 != 3 or i >= 45) else "08:05"}},
                "completed": i < 45, "activated": True, "position": 0,
                "time_limit_seconds": 0, "timer_started": False, "confirm_available_at": "",
                "remaining_seconds": None, "photo_required": False, "photo_count": 0,
                "delay_status": "normal"})
        return {"ok": True, "data": {"items": [{
            "group_id": "recSyntheticOrder", "target_record_id": "recSyntheticOrder",
            "title": "E楼隔离水质循环工单", "sop_name": "隔离水质循环", "scope": scope,
            "work_type": "polling", "state": "active", "created_at": "", "updated_at": "",
            "step_count": total_steps, "completed_steps": 45, "current_step_number": 46,
            "current_run_index": 0, "delay_status": "normal",
            "uploaded_file_count": 0, "upload_completed": False,
            "operator": {"name": "隔离操作员", "employee_no": "OP001", "staff_no": "S001"},
            "reviewer": {"name": "隔离审核员", "employee_no": "RV001", "staff_no": "S002"},
            "runs": [{"run_index": 0, "from_unit": "1F", "to_unit": "2F",
                      "step_count": total_steps, "completed_steps": 45}],
            "steps": steps}],
            "total": 1, "page": 1, "page_size": 20, "source": "本地工单状态",
            "scopes": [scope], "execution_available": False}}

    return app, fixture_today


def _evidence(sources):
    """Sources are relevant only when non-empty and each entry carries a title or data payload."""
    return bool(sources) and all(isinstance(x, dict) and bool(x.get("title") or x.get("data")) for x in sources)


def _pending_counts(sources, expected):
    groups = [group for source in sources for group in source.get("data", {}).get("groups", [])]
    actual = {group["key"]: group["count"] for group in groups if group.get("available")}
    return actual == expected


def _exc_label(exc):
    """Sanitized exception label: type plus optional status code, never raw upstream/provider text."""
    status = getattr(exc, "status", "")
    label = ("%s(status=%s)" % (type(exc).__name__, status)) if status else type(exc).__name__
    if isinstance(exc, AssistantError):
        label += ": " + safe_text(str(exc), limit=300)
    return label


async def check(selected):
    root = Path(__file__).resolve().parents[2]
    try:
        profile = configured_profile(root)
    except (OSError, ValueError, sqlite3.Error, RuntimeError):
        print("OpenClaw probe: no configured model profile")
        return 2
    saved = {"enabled": True, "models": [profile], "active_model_id": profile["id"]}

    calls = []

    with tempfile.TemporaryDirectory(prefix="openclaw-probe-") as directory:
        store = Store(Path(directory) / "fixture.sqlite3")
        store.put_document("lighthouse_ai", "model", saved)
        model = CustomModel(store, unprotect=unprotect_key)
        assistant = LighthouseAssistant(store, lambda *_: ([], []), model=model)

        app, today = build_fixture(calls)
        portal = PortalAgent(assistant, PortalAPICatalog(app), LighthouseFiles(store))
        port = free_port()

        def cached_reader(kind, scopes, allowed_scopes):
            if set(scopes) != {"E"} or not set(scopes) <= set(allowed_scopes):
                raise AssistantError("无权读取测试快照。", 403)
            calls.append("cached-" + kind)
            count = {"events": 2, "orders": 1, "mops": 1}[kind]
            return [{"id": "synthetic-%s-%d" % (kind, i), "scopes": ["E"],
                "title": "E楼隔离%s%d" % (kind, i), "status": "待完成", "url": "/synthetic"}
                for i in range(count)], []

        engine = TracedLighthouseOpenClaw(
            portal,
            cached_reader=cached_reader,
            bridge_url=lambda: "http://127.0.0.1:%d/api/assistant/openclaw-tools" % port,
            state_root=Path(directory) / "runtime",
            runtime_root=Path(os.environ.get("LIGHTHOUSE_PROBE_RUNTIME", root / "build_output/lighthouse_openclaw_verified")))
        install_model_route(app, engine.manager)

        @app.post("/api/assistant/openclaw-tools")
        async def bridge(request: Request):
            try:
                payload = await request.json()
                result = await engine.bridge.call_shared(
                    request.headers.get("authorization", "").removeprefix("Bearer "), payload, engine.manager)
                if payload.get('tool') == 'lighthouse_read_skill' and result.get('kind') == 'workflow_guide':
                    calls.append('skill-guide' if result.get('reference', '').endswith('/SKILL.md') else 'skill-reference')
                return result
            except Exception as exc:  # noqa: BLE001
                print("Bridge failure:", _exc_label(exc))
                return JSONResponse({"ok": False}, status_code=500)

        server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
        server_task = asyncio.create_task(server.serve())
        try:
            for _ in range(100):
                if server.started:
                    break
                await asyncio.sleep(0.05)

            actor = {"id": "synthetic-probe", "scopes": ["E"], "is_admin": True, "learning_scopes": ["E"]}
            request = Request({
                "type": "http", "scheme": "http", "server": ("127.0.0.1", port),
                "client": ("127.0.0.1", 1), "path": "/api/assistant/messages", "root_path": "",
                "query_string": b"", "headers": [(b"origin", ("http://127.0.0.1:%d" % port).encode()),
                                                 (b"cookie", b"fixture=synthetic")]})
            async def authorize():
                return actor.copy()
            async def emit(kind, value):
                if kind == "status":
                    print("Probe progress: " + str(value.get("label", "")), flush=True)

            scenarios = [
                ("ordinary-nonbusiness",
                 "你好，请用一句话介绍你自己，不要查询业务。",
                 (), None, True, lambda a, s, e: bool(a.strip()) and not calls and not s),
                ("own-model",
                 "你用的什么模型？",
                 (profile["name"], profile["model"]), None, False,
                 lambda a, s, e: bool(a.strip())),
                ("equipment-knowledge",
                 "UPS是什么？用两句话解释并给出处。",
                 ("不间断", "供电"), "question-bank", True,
                 lambda a, s, e: _evidence(s) and set(calls) == {"question-bank"}),
                ("events-today-5-processing-2",
                 "今天E楼发生的事件通告有几条？其中处理中的有几条？",
                 ("5", "2"), "event-monthly", False,
                 lambda a, s, e: True),
                ("unfinished-repairs-3",
                 "E楼目前未完成的维修项目有几条？",
                 ("3",), "repair-records", False,
                 lambda a, s, e: True),
                ("ongoing-notices-7",
                 "今天有多少条进行中的通告？",
                 ("未结束通告", "7"), "workbench", False,
                 lambda a, s, e: _pending_counts(s, {"notices": 7}) and set(calls) == {"workbench"}),
                ("pending-change-plans-2",
                 "变更通告现在有多少条待发起？",
                 ("未发计划通告", "2"), "workbench", False,
                 lambda a, s, e: _pending_counts(s, {"plans": 2}) and set(calls) == {"workbench"}),
                ("repair-overview-separated-counts",
                 "查询当前未发检修计划、未结束检修通告和未完成维修项目的数量。",
                 ("待开始检修计划", "未结束检修通告", "未完成维修项目"), "workbench", False,
                 lambda a, s, e: _pending_counts(s, {"planned_repairs": 1, "repair_notices": 5, "repairs": 3})),
                ("all-current-unfinished-work",
                 "今天未结束的工作有哪些？",
                 ("未发计划通告", "250", "今日学练", "待答 7 题"), "workbench", False,
                 lambda a, s, e: _pending_counts(s, {"notices": 7, "plans": 250, "events": 2,
                     "repairs": 3, "batches": 1, "orders": 1, "mops": 1, "drills": 1,
                     "guard": 0, "learning": 1}) and "重保检查表" not in a),
                ("specific-repair-followup-3-progress-58",
                 "查询E楼维修项目record_id=recSyntheticRepair的跟进条数和当前维修进度。只查询，不修改。",
                 ("3", "58"), "repair", True,
                 lambda a, s, e: _evidence(s)),
                ("water-usage-112",
                 "查询E楼%s的水耗记录，用水量是多少？单位是什么？只查询。" % today.isoformat(),
                 ("112",), "water", True,
                 lambda a, s, e: _evidence(s) and ("吨" in a or "t" in a)),
                ("cabinet-state-totals",
                 "查询E楼机柜当前通电/断电状态的数量统计，只查询。",
                 ("8", "3"), "cabinet", True,
                 lambda a, s, e: _evidence(s)),
                ("learning-progress",
                 "查询我今天做的学练题单进度，做了几题、还剩几题，只查询。",
                 ("3", "7"), "learning", True,
                 lambda a, s, e: _evidence(s)),
                ("daily-tasks",
                 "查询今天的日常工作有哪些，只查询。",
                 ("晨会", "机房巡检"), "daily", True,
                 lambda a, s, e: _evidence(s)),
                ("published-drills",
                 "查询E楼本月已发布的演练，返回演练名称和参演楼栋，只查询。",
                 ("E楼隔离测试演练", "E楼"), "drill", True,
                 lambda a, s, e: _evidence(s)),
                ("convergence-rulesets",
                 "查询当前计划收敛规则集有哪些，返回规则集名称，只查询。",
                 ("E楼隔离收敛规则",), "convergence", True,
                 lambda a, s, e: _evidence(s)),
                ("work-order-query",
                 "查询E楼SOP工单recSyntheticOrder当前做到第几步了，操作人和审核人是谁，只查询。",
                 ("46", "隔离操作员", "隔离审核员"), "work-orders", True,
                 lambda a, s, e: _evidence(s)),
            ]
            skill_registry = root / 'bin/openclaw_service/assistant/openclaw/skills/workbuddy-registry.json'
            if skill_registry.is_file():
                skill = next(r for r in json.loads(skill_registry.read_text(encoding='utf-8')) if r['source'] == 'pdf')
                scenarios.append((
                    'installed-skill-and-reference',
                    '先实际调用 read_skill(name="' + skill['name'] + '", reference="", offset=0) 阅读技能，'
                    + '再调用 read_skill(name="' + skill['name'] + '", reference="' + skill['references'][0] + '", offset=0) 阅读登记资料'
                    '，随后用中文一句话说明 PDF 填写或核验原则。仅阅读指南，不查询系统记录，不执行任何操作。',
                    ('PDF',), 'skill-reference', True,
                    lambda a, s, e: 'skill-guide' in calls and set(calls) <= {'skill-guide','skill-reference'}))

            failures = []
            total = 0
            for index, (name, question, expected_terms, endpoint_token, provider_required, validator) in enumerate(scenarios):
                if not selected(name, provider_required):
                    continue
                total += 1
                engine.provider_calls = 0
                calls.clear()
                started = time.monotonic()
                try:
                    async with asyncio.timeout(240):
                        result = await engine.answer(actor, {"question": question,
                            "operation_id": "synthetic_native_probe_%s" % name,
                            "_profile": profile, "file_ids": []}, [], request, emit, authorize, {})
                    answer = result.get("answer", "") or ""
                    sources = result.get("sources") or []
                    used_provider = engine.provider_calls > 0
                    ok = True
                    reason = []
                    if provider_required and not used_provider:
                        ok = False
                        reason.append("expected OpenClaw provider loop, got deterministic path")
                    if not provider_required and used_provider:
                        ok = False
                        reason.append("expected deterministic fast path, got provider loop")
                    if endpoint_token is not None and endpoint_token not in calls:
                        ok = False
                        reason.append("endpoint %s not called (calls=%s)" % (endpoint_token, calls))
                    extra = validator(answer, sources, used_provider)
                    if extra is not True:
                        ok = False
                        reason.append("validator failed: " + str(extra))
                    missing = [t for t in expected_terms if t not in answer]
                    if missing:
                        ok = False
                        reason.append("missing expected terms: " + ",".join(missing))
                    if "unexpected-write" in calls:
                        ok = False
                        reason.append("unexpected business write attempted")
                    if ok:
                        print("[NativeProbe] %02d %s: OK (provider=%s, calls=%s, seconds=%.3f)" %
                              (index + 1, name, used_provider, calls, time.monotonic() - started))
                    else:
                        failures.append((name, reason, answer, calls))
                        print("[NativeProbe] %02d %s: FAIL (provider=%s, calls=%s)" % (index + 1, name, used_provider, calls))
                        print("  reason: " + "; ".join(reason))
                        print("  answer: " + answer[:400].replace("\n", " "))
                except Exception as exc:  # noqa: BLE001
                    label = _exc_label(exc)
                    failures.append((name, [label], "", list(calls)))
                    print("[NativeProbe] %02d %s: ERROR %s (no raw provider text echoed)" % (index + 1, name, label))

            if failures:
                print("\nNative probe failed %d/%d selected scenario(s)." % (len(failures), total))
                return 1
            print("\nNative probe: all %d selected scenarios satisfied." % total)
            return 0
        finally:
            await engine.close()
            server.should_exit = True
            await asyncio.wait_for(server_task, 5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Live acceptance probe for the DEFAULT engine's native questions (opt-in).")
    parser.add_argument("--case", action="append", default=[],
                        help="run only scenarios whose name contains any of these (comma-separated) substrings")
    parser.add_argument("--deterministic", action="store_true",
                        help="run only deterministic fast-path scenarios (no real model calls)")
    args = parser.parse_args()
    subs = [part.strip() for raw in args.case for part in raw.split(",") if part.strip()]

    def selected(name, provider_required):
        if args.deterministic and provider_required:
            return False
        if subs and not any(sub in name for sub in subs):
            return False
        return True

    raise SystemExit(asyncio.run(check(selected)))

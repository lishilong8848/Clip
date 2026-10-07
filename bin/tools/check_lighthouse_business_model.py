"""Opt-in model probe using synthetic business APIs; never writes real business data."""
import asyncio
import argparse
import copy
import datetime as dt
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")


async def main(*, notice_only=False, event_transfer_only=False, notice_binding_only=False, creation_only=False, water_only=False, query_pages_only=False, question_bank_only=False, work_orders_only=False, question_material_only=False):
    from fastapi import FastAPI, Request
    from lan_bitable_template_portal.lighthouse_ai import CustomModel, LighthouseAssistant, unprotect_key
    from lan_bitable_template_portal.lighthouse_agent import PortalAgent
    from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
    from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
    from lan_bitable_template_portal.lighthouse_model import LighthouseModel
    from clipflow_backend.api_models import DrillExecutionRequest, DrillConfigurationRequest, SignatureUsageConfirmationSendRequest, WorkbenchActionRequest, EventTransferRepairRequest, NoticeIdentityBindRequest
    from test_lighthouse_stream import Store
    from test_drill_management import _fixture_xlsx
    from lan_bitable_template_portal.drill_management import DrillManagementService
    path = Path(__file__).resolve().parents[1] / "data" / "lan_portal_state.sqlite3"
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as conn:
        row = conn.execute("SELECT payload_json FROM json_documents WHERE namespace=? AND key=?", ("lighthouse_ai", "model")).fetchone()
    if not row:
        print("Business model probe: no configured profile")
        return 2
    saved = json.loads(row[0])
    profile = CustomModel._default(saved)
    calls = []
    app = FastAPI()

    @app.get("/api/repair-management/records")
    async def repair_records(scope: str = "E", page: int = 1, page_size: int = 100):
        calls.append("repair-records")
        assert scope == "E"
        rows = [{"record_id": f"rec{i + 1:03d}", "title": f"隔离维修项目{i + 1:03d}", "scope": scope, "status": "进行中"} for i in range(73)]
        for index, name in enumerate(("隔离冷却水参数核对", "隔离供电切换确认", "隔离报警回路检查"), 60):
            rows[index]["title"] = name
        return {"ok": True, "data": {"scope": scope, "page": page, "page_size": page_size, "total": 73, "records": rows}}

    @app.get("/api/workbench")
    async def workbench(scope: str = "E", work_type: str = "maintenance", sections: str = "ongoing", search: str = ""):
        calls.append("notice")
        return {"ok": True, "data": {"scope": scope, "ongoing": [{
            "record_id": "recSyntheticNotice", "target_record_id": "recSyntheticNotice", "active_item_id": "activeSyntheticNotice",
            "scope": "E", "building_codes": ["E"], "work_type": "maintenance", "notice_type": "维保通告", "status": "开始",
            "title": "EA118机房E楼隔离测试空调维护", "start_time": "2026-10-03 09:00", "end_time": "2026-10-03 18:00",
            "specialty": "暖通", "maintenance_cycle": "每月", "execution_party": "自维", "location": "E楼机房",
            "content": "空调维护检查", "reason": "月度维护", "impact": "无业务影响", "progress": "正在检查",
        }], "ongoing_pagination": {"page": 1, "page_size": 200, "total": 1}}}

    @app.post("/api/workbench-actions")
    async def notice_action(body: WorkbenchActionRequest):
        calls.append("unexpected-write")
        raise AssertionError("Notice form must not send before human confirmation")

    @app.post("/api/notice-identity/bind")
    async def bind_notice(body: NoticeIdentityBindRequest):
        calls.append("unexpected-write")
        raise AssertionError("Binding form must not save before human confirmation")

    @app.get("/api/events/monthly")
    async def event_monthly(scope: str = "E", month: str = "2026-10", date_field: str = "occurrence_time"):
        calls.append("event-monthly")
        return {"ok": True, "data": {"scope": scope, "month": month, "date_field": date_field, "snapshot_exists": True,
            "records": [{"record_id": "recSyntheticEvent", "title": "E楼隔离测试空调报警", "scope": "E", "occurrence_time": "2026-10-01 09:00", "status": "处理中", "transfer_to_overhaul": False}]}}

    @app.post("/api/events/transfer-repair")
    async def event_transfer(body: EventTransferRepairRequest):
        calls.append("unexpected-write")
        raise AssertionError("Event form must not write before human confirmation")

    @app.get("/api/critical-guard/tasks/{task_id}")
    async def guard_task(task_id: str, scope: str):
        calls.append("guard-usage")
        return {"ok": True, "data": {"task_id": "guardSyntheticUsage", "task_name": "隔离重保签名测试", "responses": [
            {"response_id": "responseSyntheticUsage", "scope": "E", "sheet_type": "设备安全", "version": 1, "cells": {}}]}}

    @app.post("/api/signatures/usage-confirmations/send")
    async def signature_usage(body: SignatureUsageConfirmationSendRequest):
        calls.append("unexpected-write")
        raise AssertionError("Approval request form must not send messages")

    @app.get("/api/repair-management/records/{record_id}")
    async def repair(record_id: str, scope: str = "ALL"):
        if record_id != "recSyntheticRepair" or scope != "E":
            return {"ok": False, "error": "Unknown synthetic record/scope"}
        calls.append("repair")
        return {"ok": True, "data": {"record": {"record_id": record_id, "scope": scope, "title": "隔离测试维修", "followup_count": 3,
            "followup_state_verified": True, "progress_percent": 58, "workflow": "维修中"}}}

    @app.get("/api/capacity/water/records")
    async def water(scope: str = "ALL", start_date: str = "", end_date: str = "", limit: int = 100, offset: int = 0):
        if scope != "E":
            return {"ok": False, "error": "Unknown synthetic scope"}
        calls.append("water")
        return {"ok": True, "data": {"scope": scope, "total": 1, "records": [{"record_id": "recSyntheticWater", "scope": scope,
            "title": "隔离测试水表", "statistic_date": "2026-10-01", "computed_usage": 112, "unit": "t"}]}}

    @app.get("/api/drills/{drill_id}/execution")
    async def execution(drill_id: str, scope: str):
        if drill_id != drill_definition["drill_id"] or scope != "E":
            return {"ok": False, "error": "Unknown synthetic drill/scope"}
        calls.append("execution")
        return {"ok": True, "data": {"drill": copy.deepcopy(drill_definition), "execution": copy.deepcopy(drill_execution)}}

    @app.get("/api/drills/bootstrap")
    async def drill_bootstrap(scope: str):
        return {"ok": True, "data": {"default_scope": "E", "scopes": [{"scope": "E"}], "people": [
            {"source": "staff", "record_id": "recSyntheticSigner", "name": "测试评估员", "has_signature": True}]}}

    @app.get("/api/drills")
    async def drill_list(scope: str = ""):
        return {"ok": True, "data": {"items": [copy.deepcopy(drill_definition if scope else drill_template)]}}

    @app.put("/api/drills/{drill_id}/configuration")
    async def save_configuration(drill_id: str, body: DrillConfigurationRequest):
        calls.append("unexpected-write")
        raise AssertionError("A configuration form must not save without confirmation")

    @app.get("/api/cabinet-power/batches/{batch_id}")
    async def cabinet_batch(batch_id: str):
        if batch_id != "batchSyntheticProof":
            return {"ok": False, "error": "Unknown synthetic batch"}
        calls.append("cabinet-proof")
        return {"ok": True, "data": {"batch_id": batch_id, "version": 7, "scopes": ["E"], "source": "image", "owner_id": "synthetic-probe", "rows": [
            {"row_id": "row-synthetic", "scope": "E", "room": "202", "rack": "A11", "editable": True, "status": "ready"}],
            "images": [{"image_id": "proof-synthetic", "name": "隔离测试截图.png", "status": "done", "suggestions": []}]}}

    @app.post("/api/cabinet-power/batches/{batch_id}/images/{image_id}/apply")
    async def cabinet_proof_apply(batch_id: str, image_id: str, request: Request):
        calls.append("unexpected-write")
        raise AssertionError("Proof form preparation must not link screenshots")

    @app.post("/api/cabinet-power/batches/{batch_id}/images/{image_id}/correct")
    async def cabinet_proof_correct(batch_id: str, image_id: str, request: Request):
        calls.append("unexpected-write")
        raise AssertionError("Correction form preparation must not add cabinet rows")

    @app.post("/api/cabinet-power/batches")
    async def cabinet_create(request: Request):
        calls.append("unexpected-write")
        raise AssertionError("Text form preparation must not create a batch")

    @app.put("/api/drills/{drill_id}/execution")
    async def save_execution(drill_id: str, body: DrillExecutionRequest, scope: str):
        calls.append("unexpected-write")
        raise AssertionError("A model proposal must never write without confirmation")

    actor = {"id": "synthetic-probe", "scopes": ["E"], "is_admin": False}
    request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80), "client": ("127.0.0.1", 1),
        "path": "/api/assistant/messages", "root_path": "", "query_string": b"", "headers": [(b"origin", b"http://testserver"), (b"cookie", b"fixture=synthetic")]})
    async def authorize(): return actor
    async def emit(kind, payload):
        if kind == "status":
            print("Synthetic progress: " + str(payload.get("label", "")))
    with tempfile.TemporaryDirectory(prefix="lighthouse-model-probe-") as directory:
        store = Store(Path(directory) / "state.sqlite3")
        drills = DrillManagementService(store, data_root=Path(directory) / "drills")
        drill_definition = drills.create_definition(name="隔离测试演练", year=2026, month=9,
            file_name="fixture.xlsx", source=_fixture_xlsx(with_evaluator=True))
        drill_definition = drills.publish(drill_definition["drill_id"], expected_version=drill_definition["version"])
        drill_execution = drills.get_execution(drill_definition["drill_id"], "E", create=True)
        drill_execution.update(version=12, execution_version=2, status="synced", drill_date="2026-09-29", first_start_time="09:00",
            evaluator={"source": "staff", "record_id": "recSyntheticSigner", "name": "测试评估员"},
            signature_time="2026-09-29T10:00", evaluation_time="2026-09-29T11:00")
        draft = drills.create_definition(name="隔离模板配置测试", year=2026, month=10, assigned_scopes=["E"],
            file_name="configuration.xlsx", source=_fixture_xlsx(with_evaluator=True))
        drill_template = drills.get_definition(draft["drill_id"])
        store.put_document("lighthouse_ai", "model", saved)
        model = CustomModel(store, unprotect=unprotect_key)
        assistant = LighthouseAssistant(store, lambda *_: ([], []), model=model)
        if work_orders_only:
            import time
            from lan_bitable_template_portal.lighthouse_sources import work_order_records
            steps = [{"run_index": 1, "run_label": "第1轮", "step_index": index + 1,
                      "content": f"隔离步骤{index + 1}：核对运行参数", "operator_required": True,
                      "reviewer_required": True, "operator_confirmation": {"confirmed_at": "2026-10-03 09:00"} if index < 45 else {},
                      "reviewer_confirmation": {"confirmed_at": "2026-10-03 09:01"} if index < 45 else {}, "photos": []}
                     for index in range(75)]
            steps[60]["content"] = "隔离第61步：核对末端压差稳定"
            steps[20].update(delay_reminder_minutes=5, delay_reminder={"due_at_ts": time.time() + 120})
            store.put_document("polling_work_order", "recSyntheticOrder", {
                "group_id": "recSyntheticOrder", "target_record_id": "recSyntheticOrder", "scope": "E", "work_type": "maintenance",
                "title": "E楼隔离水质循环工单", "sop_name": "隔离水质循环", "state": "active", "current_index": 45,
                "selected_run_index": 1, "steps": steps, "operator": {"name": "隔离操作员", "open_id": "not-public"},
                "reviewer": {"name": "隔离审核员", "open_id": "not-public"}, "token_hashes": {"operator": "not-public"}})
            @app.get("/api/assistant/work-orders")
            async def read_orders(scope: str = "ALL", state: str = "all", search: str = "", work_type: str = "", group_id: str = "", page: int = 1, page_size: int = 20):
                calls.append("work-orders")
                return {"ok": True, "data": work_order_records(store, actor, {"scope": scope, "state": state, "search": search,
                    "work_type": work_type, "group_id": group_id, "page": page, "page_size": page_size})}
        if question_bank_only or question_material_only:
            from lan_bitable_template_portal.learning import LearningService
            from lan_bitable_template_portal.lighthouse_sources import question_bank
            from test_learning import FakeCloud, FakeSender, LearningTests
            bank_service = LearningService(Path(directory) / "questions", FakeCloud(enabled=False), FakeSender())
            stems = ("UPS的主要功能是什么？", "柴油发电机启动失败应检查哪些项目？", "CRAH机组过滤器堵塞对压差的影响是什么？")
            answers = ("隔离题库结论甲：输入供电异常时维持负载连续供电。", "隔离题库结论乙：先核对启动蓄电池和启动回路。", "隔离题库结论丙：脏堵会使压差增大，应检查过滤器。")
            with bank_service.transaction() as conn:
                for index, stem in enumerate(stems):
                    item = LearningTests.question(None, index, bank="professional", id=f"professional:target{index}", stem=stem, answer_text=answers[index])
                    bank_service._put("question", item["id"], item, conn, False)
                for index in range(90):
                    item = LearningTests.question(None, index, bank="professional", stem=f"隔离样例无关题{index}：文档封面编号如何填写？", answer_text="按文档编号填写。")
                    bank_service._put("question", item["id"], item, conn, False)
                bank_service._put("local", "refresh", {"at": "2026-10-03T08:00:00+08:00"}, conn, False)
                if question_material_only:
                    import io
                    from PIL import Image, ImageDraw, ImageFont
                    picture = Image.new("RGB", (950, 180), "white")
                    ImageDraw.Draw(picture).multiline_text((20, 15), "SYNTHETIC UPS DATA\nRATED POWER: 18 kW\nREFERENCE CODE: ALPHA-17",
                        fill="black", font=ImageFont.truetype("arial.ttf", 32), spacing=8)
                    buffer = io.BytesIO()
                    picture.save(buffer, format="PNG")
                    file = bank_service._save_file("synthetic-ups.png", buffer.getvalue())
                    attachment = {"id": "synthetic-ups-material", "name": file.name, "kind": "answer",
                                  "question_id": "professional:target0", "local_file": file.name}
                    item = bank_service._get("question", "professional:target0", conn)
                    item.update(answer_text="", attachments=[attachment])
                    bank_service._put("question", item["id"], item, conn, False)
                    bank_service._put("attachment", attachment["id"], attachment, conn, False)
            @app.get("/api/assistant/question-bank")
            async def read_bank(scope: str = "ALL", search: str = "", bank: str = "", page: int = 1, page_size: int = 20):
                calls.append("question-bank")
                print("Synthetic bank query: " + json.dumps({"search": search, "bank": bank, "page": page, "page_size": page_size}, ensure_ascii=False))
                return {"ok": True, "data": question_bank(bank_service, actor, {"scope": scope, "search": search, "bank": bank, "page": page, "page_size": page_size})}
            if question_material_only:
                from lan_bitable_template_portal.lighthouse_sources import question_material
                @app.get("/api/assistant/question-material")
                async def read_material(scope: str = "ALL", material_id: str = "", offset: int = 0, length: int = 4000):
                    calls.append("question-material")
                    data = await asyncio.to_thread(question_material, bank_service, store, actor,
                        {"scope": scope, "material_id": material_id, "offset": offset, "length": length})
                    return {"ok": True, "data": data}
        portal = PortalAgent(assistant, PortalAPICatalog(app), LighthouseFiles(store))
        original_discover = portal.catalog.discover
        def trace_discover(*args, **kwargs):
            result = original_discover(*args, **kwargs)
            print("Synthetic discovery: " + json.dumps({"keyword": kwargs.get("keyword"), "group": kwargs.get("group"), "total": result["total"], "chars": len(json.dumps(result))}, ensure_ascii=False))
            return result
        portal.catalog.discover = trace_discover
        original_prepare = portal.prepare
        def trace_prepare(*args, **kwargs):
            from lan_bitable_template_portal.lighthouse_ai import AssistantError, safe_text
            try:
                return original_prepare(*args, **kwargs)
            except AssistantError as exc:
                print("Synthetic prepare rejected: " + safe_text(str(exc))[:300])
                decision = args[1]
                print("Synthetic operation targets: " + json.dumps([{key: op.get(key) for key in ("api_id", "path_params", "params")} for op in decision.get("operations", [])]))
                query_data = args[5] if len(args) > 5 else kwargs.get("queries", {})
                print("Synthetic query shapes: " + json.dumps([{ "ref": key, "keys": list(data) if isinstance(data, dict) else [],
                    "execution_scope": (data.get("execution") or {}).get("scope") if isinstance(data, dict) else None,
                    "execution_drill": (data.get("execution") or {}).get("drill_id") if isinstance(data, dict) else None }
                    for key, data in (query_data or {}).items()]))
                raise
        portal.prepare = trace_prepare
        engine = LighthouseModel(portal)
        if question_material_only:
            actor = {**actor, "is_admin": True, "learning_scopes": ["E"]}
            try:
                async with asyncio.timeout(120):
                    result = await engine.answer(actor, {"question": "请读取题库中UPS那道题的答案图片资料，告诉我图片里额定功率和核对编号是什么。只读取，不发布或答题。",
                        "operation_id": "synthetic_question_material", "_profile": profile}, [], request, emit, authorize, {})
                if "question-material" not in calls:
                    print("Synthetic material answer: " + str(result.get("answer", ""))[:1200])
                assert "question-material" in calls and set(calls) <= {"question-bank", "question-material"}, calls
                assert "18" in result.get("answer", "") and "ALPHA-17" in result.get("answer", ""), result.get("answer")
                assert result.get("sources") and not result.get("plan") and not bank_service.cloud.calls
                print("Question-material model probe: native OCR + original permissions + grounded attachment answer + no business writes OK")
                return 0
            finally:
                model.close()
        if work_orders_only:
            try:
                async with asyncio.timeout(120):
                    result = await engine.answer(actor, {"question": "查询E楼水质循环SOP工单做到第几步了，操作人和审核人是谁？再告诉我第61步的具体内容。只查询。",
                        "operation_id": "synthetic_work_order_query", "_profile": profile}, [], request, emit, authorize, {})
                text = result.get("answer", "")
                assert calls and set(calls) == {"work-orders"}, calls
                assert all(word in text for word in ("46", "隔离操作员", "隔离审核员", "末端压差稳定")), text
                assert "not-public" not in json.dumps(result) and not result.get("plan")
                assert any(isinstance(source.get("data"), dict) and source["data"].get("snapshot_only") for source in result.get("sources", [])), "Later steps were not read from snapshot"
                print("Work-order query model probe: current step + role names + later step text + no execution or credentials OK")
                return 0
            finally:
                model.close()
        if question_bank_only:
            actor = {**actor, "is_admin": True, "learning_scopes": ["E"]}
            questions = ("题库里有没有讲停电了负载还能继续供电的设备，主要用途是什么？",
                         "柴油机备用发电启动不了通常先查哪一块？",
                         "题库里空调两端压力差偏大应优先排查什么？")
            try:
                for index, question in enumerate(questions):
                    calls.clear()
                    async with asyncio.timeout(120):
                        result = await engine.answer(actor, {"question": question,
                            "operation_id": f"synthetic_question_bank_{index}", "_profile": profile}, [], request, emit, authorize, {})
                    assert calls and set(calls) == {"question-bank"}, calls
                    assert result.get("sources") and f"professional:target{index}" in json.dumps(result["sources"]), "Relevant source not retrieved beyond initial page"
                    expected_terms = (("UPS", "供电"), ("蓄电池", "启动回路"), ("过滤器", "压差"))[index]
                    assert all(term in result.get("answer", "") for term in expected_terms), result.get("answer")
                    assert not result.get("plan") and not bank_service.cloud.calls
                    print(f"Question-bank semantic probe {index + 1}: paraphrase -> authorized source + grounded answer + no writes OK")
                return 0
            finally:
                model.close()
        if query_pages_only:
            actor = {**actor, "is_admin": True}
            try:
                async with asyncio.timeout(120):
                    result = await engine.answer(actor, {"question": "读取E楼本月维修项目列表，仅给出列表的第61到第63项项目名称。只查询。",
                        "operation_id": "synthetic_query_pages", "_profile": profile}, [], request, emit, authorize, {})
                answer = result.get("answer", "")
                pages = [source["data"] for source in result.get("sources", []) if isinstance(source.get("data"), dict) and source["data"].get("snapshot_only")]
                assert calls == ["repair-records"], calls
                assert pages and any(page.get("offset", 0) >= 40 for page in pages), "Model did not read beyond the first sample"
                assert all(name in answer for name in ("隔离冷却水参数核对", "隔离供电切换确认", "隔离报警回路检查")), answer
                print("Business model probe: rows61-63 read from authorized snapshot + one native read + no writes OK")
                return 0
            finally:
                model.close()
        if creation_only:
            from test_lighthouse_creation_workflows import CreationWorkflowTests, ACTOR, MORNING, DRILL
            fixture = CreationWorkflowTests()
            fixture.setUp()
            actor = copy.deepcopy(ACTOR)
            @fixture.agent.catalog.app.get("/api/daily-tasks/morning-meeting/preview")
            async def preview(date: str = "", temperature_only: str = ""):
                return {"ok": True, "data": {"date": fixture.today, "weather_condition": "晴", "dry_bulb_temperature": 21, "wet_bulb_temperature": 18}}
            portal = PortalAgent(assistant, PortalAPICatalog(fixture.agent.catalog.app), LighthouseFiles(store))
            engine = LighthouseModel(portal)
            try:
                for api, flag, question in (
                    (MORNING, "native_morning_meeting", "生成今天的晨会表格，先打开天气和温度填写表单给我核对，现在不要生成文件。"),
                    (DRILL, "native_drill_create", "我要上传一份新的演练模板，请打开月份、参演楼栋、模板文件选择表单，我会在表单选择，先不要发布。"),
                ):
                    async with asyncio.timeout(120):
                        result = await engine.answer(actor, {"question": question, "operation_id": "synthetic_creation_" + flag, "_profile": profile}, [], request, emit, authorize, {})
                    plan = result.get("plan") or {}
                    assert plan.get("status") == "needs_input", {"answer": result.get("answer"), "status": plan.get("status")}
                    assert any(field.get(flag) for field in plan.get("fields", [])), flag
                    assert any(op["api_id"] == api for op in plan["operations"])
                    assert not fixture.calls, "Preparing a form must never write"
                    print("Business model probe: " + flag + " opens native form without business writes OK")
                return 0
            finally:
                fixture.fixture.tmp.cleanup()
                model.close()
        if water_only:
            from test_lighthouse_water_workflows import WaterWorkflowTests, ACTOR, CREATE, UPDATE
            fixture = WaterWorkflowTests()
            fixture.setUp()
            actor = copy.deepcopy(ACTOR)
            @fixture.agent.catalog.app.get("/api/capacity/water/bootstrap")
            async def water_bootstrap(scope: str):
                assert scope == "E"
                return {"ok": True, "data": copy.deepcopy(fixture.bootstrap)}
            @fixture.agent.catalog.app.get("/api/capacity/water/records/{record_id}")
            async def water_detail(record_id: str, scope: str):
                assert scope == "E" and record_id == "recWaterE"
                return {"ok": True, "data": copy.deepcopy(fixture.detail)}
            portal = PortalAgent(assistant, PortalAPICatalog(fixture.agent.catalog.app), LighthouseFiles(store))
            engine = LighthouseModel(portal)
            try:
                for api, question in (
                    (CREATE, "我要录入E楼水耗，请读取本楼基础选项，打开水表、日期、读数和照片填写表单，我会选择并填写，先不要保存。"),
                    (UPDATE, "修改E楼水耗记录recWaterE，请读取基础选项和原记录，打开填写表单，保留原照片让我选择，不实际保存。"),
                ):
                    async with asyncio.timeout(120):
                        result = await engine.answer(actor, {"question": question, "operation_id": "synthetic_water_" + api[:5], "_profile": profile}, [], request, emit, authorize, {})
                    plan = result.get("plan") or {}
                    assert plan.get("status") == "needs_input", {"answer": result.get("answer"), "status": plan.get("status")}
                    assert any(field.get("native_water_record") for field in plan.get("fields", []))
                    assert any(field.get("native_water_photos") for field in plan.get("fields", []))
                    assert any(op["api_id"] == api for op in plan["operations"])
                    assert not fixture.saves and not fixture.uploads
                    print("Business model probe: " + api.split()[0] + " water form and photo controls without business writes OK")
                return 0
            finally:
                fixture.fixture.tmp.cleanup()
                model.close()
        if notice_binding_only:
            async with asyncio.timeout(120):
                result = await engine.answer(actor, {"question": "读取E楼未结束维保通告，给EA118机房E楼隔离测试空调维护补绑已有计划通告。请打开源表事项选择表单，只准备保存关系，不发送更新。",
                    "operation_id": "synthetic_notice_binding", "_profile": profile}, [], request, emit, authorize, {})
            plan = result.get("plan") or {}
            field = next(field for field in plan.get("fields", []) if field.get("native_notice_identity") and field.get("options_source"))
            assert plan["status"] == "needs_input" and "notice" in calls and "unexpected-write" not in calls
            assert field["path"] == "source_record_id" and field["type"] == "select"
            assert plan["operations"][0]["api_id"] == "POST /api/notice-identity/bind"
            assert plan["operations"][0]["body"]["target_record_id"] == "recSyntheticNotice"
            assert any(item["path"] == "source_month" and item["type"] == "select" for item in plan["fields"])
            print("Business model probe: native source binding selector + original target + no notice send/write OK")
            return 0
        if event_transfer_only:
            async with asyncio.timeout(120):
                result = await engine.answer(actor, {"question": "帮我把2026年10月E楼隔离测试空调报警这条事件标记为转检修，请打开选择表单，只准备，不实际提交。",
                    "operation_id": "synthetic_event_transfer", "_profile": profile}, [], request, emit, authorize, {})
            plan = result.get("plan") or {}
            field = next(field for field in plan.get("fields", []) if field.get("native_event_transfer"))
            assert plan["status"] == "needs_input" and field["type"] == "select" and "unexpected-write" not in calls
            assert any(item["type"] == "month" for item in plan["fields"])
            assert plan["operations"][0]["api_id"] == "POST /api/events/transfer-repair"
            print("Business model probe: event transfer opens native scope/month/event selection without writes OK")
            return 0
        if notice_only:
            try:
                async with asyncio.timeout(120):
                    result = await engine.answer(actor, {"question": "读取E楼未结束的维保通告，打开EA118机房E楼隔离测试空调维护的编辑表单，只把进度改为现场检查已完成60%，其他内容保留，不实际发送。",
                        "operation_id": "synthetic_notice_edit", "_profile": profile}, [], request, emit, authorize, {})
                plan = result.get("plan") or {}
                field = next(field for field in plan.get("fields", []) if field.get("native_notice"))
                children = {child["path"]: child for child in field["children"]}
                assert plan["status"] == "needs_input" and "notice" in calls and "unexpected-write" not in calls
                assert children["start_time"]["type"] == "datetime-local" and children["specialty"]["type"] == "select"
                assert field["value"]["title"] == "EA118机房E楼隔离测试空调维护" and field["value"]["progress"] == "现场检查已完成60%"
                assert plan["operations"][0]["body"]["target_record_id"] == "recSyntheticNotice"
                print("Business model probe: native notice form + same target + requested progress + no write OK")
                async with asyncio.timeout(120):
                    result = await engine.answer(actor, {"question": "准备一条新的E楼维保开始通告，名称为EA118机房E楼隔离测试水泵维护。SOP、操作人和现场审核人由我在表单选择，现在只打开填写表单，不实际发送。",
                        "operation_id": "synthetic_notice_sop", "_profile": profile}, [], request, emit, authorize, {})
                plan = result.get("plan") or {}
                field = next(field for field in plan.get("fields", []) if field.get("native_notice_sop"))
                assert plan["status"] == "needs_input" and field["scopes"] == ["E"]
                assert field["value"]["exempt"] is False and not field["value"]["operator_record_id"]
                assert any(field.get("native_notice") for field in plan["fields"]) and "unexpected-write" not in calls
                print("Business model probe: notice start opens SOP/person selection without exemption or writes OK")
                return 0
            except Exception as exc:
                print("Business model notice probe failed: " + type(exc).__name__)
                if "result" in locals():
                    print("Synthetic notice result: " + json.dumps({"answer": result.get("answer"), "plan": result.get("plan")}, ensure_ascii=False)[:5000])
                return 1
        for question, expected, endpoint in (
            ("查询E楼维修项目record_id=recSyntheticRepair的跟进条数和当前维修进度。只查询，不修改。", ("3", "58"), "repair"),
            ("查询E楼2026-10-01的水耗记录，告诉我用水量及单位，只查询。", ("112",), "water"),
        ):
            try:
                async with asyncio.timeout(120):
                    result = await engine.answer(actor, {"question": question, "operation_id": "synthetic_probe_message", "_profile": profile}, [], request, emit, authorize, {})
                answer = result.get("answer", "")
                if endpoint not in calls or not result.get("sources") or not all(text in answer for text in expected):
                    print("Business model probe: contract mismatch for " + endpoint)
                    return 1
                print("Business model probe: " + endpoint + " native read + evidence + answer OK")
            except Exception as exc:
                print("Business model probe failed: " + type(exc).__name__ + "; http_status=" + str(getattr(exc, "status_code", "unavailable")))
                return 1
        try:
            async with asyncio.timeout(120):
                result = await engine.answer(actor, {"question": f"读取E楼演练{drill_definition['drill_id']}的执行信息，保留原评估人及其他内容，只把演练审核人签名时间改成2026-09-29T10:30、演练评估人评估时间改成2026-09-29T11:45。准备保存修改的待确认操作，不实际提交。",
                    "operation_id": "synthetic_signing_proposal", "_profile": profile}, [], request, emit, authorize, {})
            public = result.get("plan") or {}
            plan = store.get_document("lighthouse_agent_plans", public.get("id")) or {}
            assert calls.count("execution") and "unexpected-write" not in calls and plan.get("operations")
            from lan_bitable_template_portal.lighthouse_agent import _result_refs
            body = _result_refs(plan["operations"][0]["body"], [], plan.get("_references"), plan.get("_queries"))
            assert dt.datetime.fromisoformat(body["signature_time"]) == dt.datetime(2026, 9, 29, 10, 30)
            assert dt.datetime.fromisoformat(body["evaluation_time"]) == dt.datetime(2026, 9, 29, 11, 45)
            assert body["expected_version"] == 12 and body["evaluator"]["record_id"] == "recSyntheticSigner"
            print("Business model probe: signer/time proposal + preserved identity + no write OK")
            async with asyncio.timeout(120):
                result = await engine.answer(actor, {"question": f"重新填写E楼演练{drill_definition['drill_id']}的全部内容。",
                    "operation_id": "synthetic_refill_proposal", "_profile": profile}, [], request, emit, authorize, {})
            public = result.get("plan") or {}
            control = next(field for field in public.get("fields", []) if field.get("native_drill"))
            fields = {field["path"]: field for field in control["children"]}
            assert fields["drill_date"]["type"] == "date" and fields["first_start_time"]["type"] == "time"
            assert fields["commander"]["person_picker"] and fields["evaluator"]["person_picker"] and "step_signers" in fields
            assert "unexpected-write" not in calls and "请提供" not in result["answer"]
            print("Business model probe: refill opens native date/person/step form + no write OK")
            async with asyncio.timeout(120):
                result = await engine.answer(actor, {"question": "把E楼机柜批次batchSyntheticProof的确认截图关联到这个批次的机柜，请打开选择表单。",
                    "operation_id": "synthetic_proof_proposal", "_profile": profile}, [], request, emit, authorize, {})
            field = next(field for field in (result.get("plan") or {}).get("fields", []) if field.get("native_cabinet_proof"))
            assert field["images"][0]["name"] == "隔离测试截图.png" and field["rows"][0]["rack"] == "A11"
            assert "cabinet-proof" in calls and "unexpected-write" not in calls
            print("Business model probe: screenshot association opens authorized image/rack form + no write OK")
            async with asyncio.timeout(120):
                result = await engine.answer(actor, {"question": "我要新建E楼粘贴文本机柜待办，先打开粘贴识别表单。",
                    "operation_id": "synthetic_text_create", "_profile": profile}, [], request, emit, authorize, {})
            field = next(field for field in (result.get("plan") or {}).get("fields", []) if field.get("native_cabinet_text_create"))
            assert field["scopes"] == ["E"] and "unexpected-write" not in calls
            print("Business model probe: text batch creation opens paste preview form + no write OK")
            async with asyncio.timeout(120):
                result = await engine.answer(actor, {"question": "补全E楼图片批次batchSyntheticProof漏识别的机柜，请打开截图和目录选择表单。",
                    "operation_id": "synthetic_proof_correct", "_profile": profile}, [], request, emit, authorize, {})
            field = next(field for field in (result.get("plan") or {}).get("fields", []) if field.get("native_cabinet_correct"))
            assert field["options_source"] == "cabinet_racks" and field["scopes"] == ["E"] and "unexpected-write" not in calls
            print("Business model probe: missing cabinet correction opens scoped directory form + no write OK")
            actor = {**actor, "is_admin": True}
            async with asyncio.timeout(120):
                result = await engine.answer(actor, {"question": "请读取2026年10月的演练隔离模板配置测试，打开模板配置表单，让我修改每步签名人数。只准备，不实际保存。",
                    "operation_id": "synthetic_drill_configuration", "_profile": profile}, [], request, emit, authorize, {})
            field = next(field for field in (result.get("plan") or {}).get("fields", []) if field.get("native_drill_configuration"))
            assert field["value"]["record_sheet"] == drill_template["configuration"]["record_sheet"] and "unexpected-write" not in calls
            assert any(child["path"] == "signers" for child in field["children"])
            print("Business model probe: admin draft template opens native configuration form + no write OK")
            async with asyncio.timeout(120):
                result = await engine.answer(actor, {"question": "帮我导入E楼近3个月维保历史通告记忆，先打开选择事项和历史候选的核对表单，不实际保存。",
                    "operation_id": "synthetic_history_memory", "_profile": profile}, [], request, emit, authorize, {})
            assert not result.get("plan") and "/?admin=status" in result["answer"] and "unexpected-write" not in calls
            print("Business model probe: excluded settings only link to native page + no write OK")
            async with asyncio.timeout(120):
                result = await engine.answer(actor, {"question": "请读取E楼重保任务guardSyntheticUsage，帮我给检查人发送签名使用确认，先打开人员选择表单，不实际发送。",
                    "operation_id": "synthetic_signature_usage", "_profile": profile}, [], request, emit, authorize, {})
            field = next(field for field in (result.get("plan") or {}).get("fields", []) if field.get("native_usage_confirmation"))
            assert field["options_source"] == "usage_people" and "guard-usage" in calls and "unexpected-write" not in calls
            print("Business model probe: signature usage opens native recipient selection + no write OK")
        except Exception as exc:
            from lan_bitable_template_portal.lighthouse_ai import safe_text
            print("Business model proposal failed: " + type(exc).__name__ + ("; missing_field=" + str(exc.args[0]) if isinstance(exc, KeyError) else ""))
            print("Synthetic probe error: " + safe_text(str(exc))[:350])
            if "result" in locals():
                from lan_bitable_template_portal.lighthouse_ai import safe_text
                failed_plan = result.get("plan") or {}
                print("Business model proposal metadata: status=" + str(failed_plan.get("status")) + "; apis=" + str([op.get("api_id") for op in failed_plan.get("operations", [])])
                      + "; controls=" + str([[key for key in field if key.startswith("native_")] for field in failed_plan.get("fields", [])]))
                print("Synthetic proposal answer: " + safe_text(str(result.get("answer", "")))[:800])
                if "body" in locals():
                    print("Synthetic proposal dates: " + str({key: body.get(key) for key in ("signature_time", "evaluation_time", "expected_version")}))
            return 1
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--notice-only", action="store_true")
    parser.add_argument("--event-transfer-only", action="store_true")
    parser.add_argument("--notice-binding-only", action="store_true")
    parser.add_argument("--creation-only", action="store_true")
    parser.add_argument("--water-only", action="store_true")
    parser.add_argument("--query-pages-only", action="store_true")
    parser.add_argument("--question-bank-only", action="store_true")
    parser.add_argument("--work-orders-only", action="store_true")
    parser.add_argument("--question-material-only", action="store_true")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(notice_only=args.notice_only, event_transfer_only=args.event_transfer_only, notice_binding_only=args.notice_binding_only, creation_only=args.creation_only, water_only=args.water_only, query_pages_only=args.query_pages_only, question_bank_only=args.question_bank_only, work_orders_only=args.work_orders_only, question_material_only=args.question_material_only)))

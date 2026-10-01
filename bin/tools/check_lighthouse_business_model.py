"""Opt-in model probe using synthetic business APIs; never writes real business data."""
import asyncio
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")


async def main():
    from fastapi import FastAPI, Request
    from lan_bitable_template_portal.lighthouse_ai import CustomModel, LighthouseAssistant, unprotect_key
    from lan_bitable_template_portal.lighthouse_agent import PortalAgent
    from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
    from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
    from lan_bitable_template_portal.lighthouse_model import LighthouseModel
    from clipflow_backend.api_models import DrillExecutionRequest
    from test_lighthouse_stream import Store
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
        calls.append("execution")
        return {"ok": True, "data": {"drill": {"drill_id": drill_id, "scope": scope}, "execution": {
            "version": 2, "evaluator": {"source": "staff", "record_id": "recSyntheticSigner", "name": "测试评估员"},
            "signature_time": "10:00", "evaluation_time": "11:00"}}}

    @app.put("/api/drills/{drill_id}/execution")
    async def save_execution(drill_id: str, body: DrillExecutionRequest, scope: str):
        calls.append("unexpected-write")
        raise AssertionError("A model proposal must never write without confirmation")

    actor = {"id": "synthetic-probe", "scopes": ["E"], "is_admin": False}
    request = Request({"type": "http", "scheme": "http", "server": ("testserver", 80), "client": ("127.0.0.1", 1),
        "path": "/api/assistant/messages", "root_path": "", "query_string": b"", "headers": [(b"origin", b"http://testserver"), (b"cookie", b"fixture=synthetic")]})
    async def authorize(): return actor
    async def emit(*args): pass
    with tempfile.TemporaryDirectory(prefix="lighthouse-model-probe-") as directory:
        store = Store(Path(directory) / "state.sqlite3")
        store.put_document("lighthouse_ai", "model", saved)
        model = CustomModel(store, unprotect=unprotect_key)
        assistant = LighthouseAssistant(store, lambda *_: ([], []), model=model)
        portal = PortalAgent(assistant, PortalAPICatalog(app), LighthouseFiles(store))
        engine = LighthouseModel(portal)
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
                result = await engine.answer(actor, {"question": "读取E楼演练drillSynthetic的执行信息，保留原评估人及其他内容，只把演练审核人签名时间改成10:30、演练评估人评估时间改成11:45。准备保存修改的待确认操作，不实际提交。",
                    "operation_id": "synthetic_signing_proposal", "_profile": profile}, [], request, emit, authorize, {})
            public = result.get("plan") or {}
            plan = store.get_document("lighthouse_agent_plans", public.get("id")) or {}
            assert calls.count("execution") and "unexpected-write" not in calls and plan.get("operations")
            from lan_bitable_template_portal.lighthouse_agent import _result_refs
            body = _result_refs(plan["operations"][0]["body"], [], plan.get("_references"), plan.get("_queries"))
            assert body["signature_time"] == "10:30" and body["evaluation_time"] == "11:45"
            assert body["expected_version"] == 2 and body["evaluator"]["record_id"] == "recSyntheticSigner"
            print("Business model probe: signer/time proposal + preserved identity + no write OK")
        except Exception as exc:
            print("Business model proposal failed: " + type(exc).__name__ + ("; missing_field=" + str(exc.args[0]) if isinstance(exc, KeyError) else ""))
            if "plan" in locals():
                print("Business model proposal metadata: status=" + str(plan.get("status")) + "; bodies=" + str([sorted((op.get("body") or {}).keys()) for op in plan.get("operations", [])]))
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

"""Isolated loopback UI preview. Synthetic model, accounts and business data only."""
import asyncio
import copy
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from openclaw_service.assistant import routes as lighthouse_routes, lighthouse_stream
from lighthouse_test_backend import install_test_backend
from lan_bitable_template_portal.lighthouse_ai import CustomModel, LighthouseAssistant, NAMESPACE
from lan_bitable_template_portal.portal_service import LI_SHILONG_OPEN_ID
from test_lighthouse_stream import Store

native = os.environ.get("LIGHTHOUSE_NATIVE_PREVIEW") == "1"
port = int(os.environ.get("LIGHTHOUSE_PREVIEW_PORT", "19003"))
temp = tempfile.TemporaryDirectory(prefix="lighthouse_stream_preview_")
store = Store(Path(temp.name) / "preview.sqlite3")
model = CustomModel(store, protect=lambda value: value, unprotect=lambda value: value)
model.configure({"api_key": "synthetic-not-a-real-credential"})
if native:
    os.environ.pop("LIGHTHOUSE_AGENT_ENGINE", None)
    store.put_document(NAMESPACE, "model", {"enabled": True, "active_model_id": "fixture", "models": [
        {"id": "fixture", "name": "隔离原生模型", "endpoint": f"http://127.0.0.1:{port}/v1/chat/completions",
         "model": "fixture", "key_cipher": "synthetic-not-a-real-credential"}]})
assistant = LighthouseAssistant(store, lambda *_: ([], []), model=model)
lighthouse_routes.LighthouseAssistant = lambda *_: assistant


class PreviewEngine:
    def __init__(self, *args, **kwargs):
        pass
    async def answer(self, actor, turn, history, request, emit, authorize, context):
        await emit("status", {"label": "正在查询隔离测试资料"})
        text = "**隔离预览**\n\n查询范围：" + "、".join(s + "楼" for s in actor["scopes"]) + "\n\n| 项目 | 数量 |\n| --- | ---: |\n| 待发检修 | 2 |\n| 未结束检修 | 1 |\n| 未完成维修单 | 3 |\n\n"
        if "长回答" in turn["question"]:
            text += "本行仅用于测试长对话和滚动。\n" * 80
        for line in text.splitlines(keepends=True):
            await asyncio.sleep(.15 if "快速" in turn["question"] else .5)
            await emit("text", {"delta": line})
        return {"answer": text, "sources": []}
    async def summarize(self, *args):
        return "隔离测试历史摘要。"


if not native:
    os.environ["LIGHTHOUSE_AGENT_ENGINE"] = "legacy"
    lighthouse_stream.LighthouseModel = PreviewEngine
app = FastAPI()
def session(request):
    code = request.cookies.get("fixture_user", "D")
    admin = code == "ALL"
    return {"user": {"open_id": LI_SHILONG_OPEN_ID if admin else "fixture-" + code, "name": "隔离测试" + code, "role": "admin" if admin else "building"},
            "role": "admin" if admin else "building", "allowed_scopes": [code]}
controller = SimpleNamespace(_current_session=session, _auth_required_response=lambda: JSONResponse({"ok": False}, status_code=401),
                             _request_base_url=lambda request: str(request.base_url).rstrip("/"),
                             bound_port=port, preferred_port=port)
runtime = SimpleNamespace(state_store=store, auth_manager=SimpleNamespace(is_admin=lambda item: item["role"] == "admin", session_scopes=lambda item: item["allowed_scopes"]))
model_calls, business_calls = [], []
if native:
    @app.get("/api/repair-management/records")
    async def native_records(request: Request, scope: str):
        allowed = session(request)["allowed_scopes"]
        if allowed != ["ALL"] and scope not in allowed:
            return JSONResponse({"ok": False, "error": "无权读取此楼栋"}, status_code=403)
        assert scope in {"D", "E"}, scope
        business_calls.append(scope)
        count = 2 if scope == "D" else 3
        return {"ok": True, "data": {"scope": scope, "total": count, "records": [
            {"record_id": f"fixture-{scope}-{index}", "scope": scope, "title": f"隔离维修{index}"}
            for index in range(count)]}}

    @app.post("/v1/chat/completions")
    async def native_model(request: Request):
        assert request.headers.get("authorization") == "Bearer synthetic-not-a-real-credential"
        payload = await request.json()
        messages = payload["messages"]
        model_calls.append({"model": payload["model"], "tools": len(payload.get("tools", []))})
        system = "\n".join(str(item.get("content") or "") for item in messages if item["role"] == "system")
        scope_match = re.search(r"本轮楼栋：([^\n]+)", system)
        scope = "E" if scope_match and scope_match[1].strip() == "E" else "D"
        last_user = max(index for index, item in enumerate(messages) if item["role"] == "user")
        found = any(item["role"] == "tool" for item in messages[last_user + 1:])
        if not found and "慢速" in str(messages[last_user].get("content")):
            await asyncio.sleep(10)
        count = 2 if scope == "D" else 3
        answer = f"{scope}楼维修项目共 **{count}条**。[1]\n\n| 楼栋 | 数量 |\n| --- | ---: |\n| {scope}楼 | {count} |"
        call = {"id": "fixture-call-" + str(len(model_calls)), "type": "function", "function": {
            "name": "lighthouse_query", "arguments": json.dumps({"api_id": "GET /api/repair-management/records", "params": {"scope": scope}})}}
        async def events():
            delta = {"role": "assistant", "tool_calls": [{"index": 0, **call}]} if not found else {"role": "assistant", "content": answer}
            chunk = {"id": "fixture-completion", "object": "chat.completion.chunk", "created": int(time.time()),
                     "model": "fixture", "choices": [{"index": 0, "delta": delta, "finish_reason": None}]}
            yield "data: " + json.dumps(chunk) + "\n\n"
            chunk["choices"] = [{"index": 0, "delta": {}, "finish_reason": "stop" if found else "tool_calls"}]
            chunk["usage"] = {"prompt_tokens": 200, "completion_tokens": 40, "total_tokens": 240}
            yield "data: " + json.dumps(chunk) + "\n\n"
            yield "data: [DONE]\n\n"
        assert payload.get("stream"), "native fixture must exercise streaming"
        return StreamingResponse(events(), media_type="text/event-stream")

    @app.get("/fixture/native-state")
    async def native_state():
        return {"model_calls": model_calls, "business_calls": business_calls}

preview_host = install_test_backend(app, controller, runtime)
if native:
    from openclaw_service.assistant.lighthouse_runtime import OpenClawRuntime
    preview_host.manager = OpenClawRuntime(Path(temp.name) / 'accounts')
dist = Path(__file__).resolve().parents[1] / "lan_bitable_template_portal" / "frontend" / "dist"
app.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")
@app.get("/api/health")
async def health():
    return {"ok": True, "service": "clipflow_backend", "instance_id": "isolated-lighthouse-openclaw" if native else "isolated-lighthouse-stream"}
@app.get("/api/{rest:path}")
async def fixtures(rest: str, request: Request):
    current = session(request)
    if rest == "auth/status":
        options = [{"value": code, "label": code + "楼"} for code in ("ABCDEH" if current["role"] == "admin" else current["allowed_scopes"])]
        data = {"logged_in": True, "user": current["user"], "scope_options": options}
    elif rest == "scope-overview":
        await asyncio.sleep(.8)
        data = {"source_snapshot_ready": True, "scopes": {code: {"pending": 5, "ongoing": 7, "event_total": 5, "event_processing": 2,
            "maintenance_pending": 5, "maintenance_ongoing": 7} for code in current['allowed_scopes']}}
    elif rest in {'workbench/lite-fragment', 'workbench/lite-detail'}:
        from lan_bitable_template_portal.workbench_lite import extract_workbench_lite_fragments
        params = dict(request.query_params); params.pop('_assistant_frame', None)
        data = {'canonical_url': '/workbench-lite?' + urlencode(params),
            'fragments': extract_workbench_lite_fragments(native_workbench_html(request))}
    else:
        data = {}
    return {"ok": True, "data": data}

def native_workbench_html(request):
    from lan_bitable_template_portal.workbench_lite import render_workbench_lite
    scope = request.query_params.get('scope') or 'D'
    work = request.query_params.get('work_type') or 'maintenance'
    return render_workbench_lite(payload={'source_snapshot_ready': True, 'records': [{'record_id': 'fixture-plan',
        'work_type': work, 'notice_type': '维保通告', 'title': scope + '楼隔离维保', 'scope': scope,
        'display_fields': {'名称': scope + '楼隔离维保', '楼栋': scope + '楼', '专业': '电气', '流程': '未开始'}}],
        'ongoing': [], 'stats': {'pending': 1, 'ongoing': 0}}, session=session(request), scope=scope, work_type=work,
        month=request.query_params.get('month') or '10月', manual=request.query_params.get('manual') == '1',
        record_id=request.query_params.get('record_id') or '', scope_options=[{'value': scope, 'label': scope + '楼'}])

@app.get('/workbench-lite')
async def framed_workbench(request: Request):
    if request.query_params.get('_assistant_frame') == '1':
        await asyncio.sleep(.4)
        return HTMLResponse(native_workbench_html(request))
    return FileResponse(dist / 'index.html')
@app.get('/legacy-widget-fixture')
async def legacy_widget(request: Request):
    from lan_bitable_template_portal.lighthouse_widget import inject_lighthouse_widget
    body = '''<!doctype html><html><head><title>Isolated legacy widget</title></head><body>
      <div style="background:#fff3df;padding:17px;color:#31445f" id="legacy-business">Legacy business content</div>
      <button id="drawer-open" onclick="document.getElementById('legacy-drawer').classList.add('open')">Open drawer</button>
      <div class="notice-detail-overlay" id="legacy-drawer" style="display:none;position:fixed;inset:0;pointer-events:none">
        <section role="dialog" style="position:absolute;right:0;top:0;width:680px;height:100%;background:#fff;border:1px solid #abc;pointer-events:auto"><button onclick="this.closest('.notice-detail-overlay').classList.remove('open')">Close drawer</button></section>
      </div><style>.notice-detail-overlay.open{display:block!important}</style></body></html>'''
    return HTMLResponse(inject_lighthouse_widget(body, session(request), dist / 'index.html'))

@app.get("/{rest:path}")
async def page(rest: str):
    return FileResponse(dist / "index.html")
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning", log_config=None)

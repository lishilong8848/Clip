"""Isolated loopback UI preview. Synthetic model, accounts and business data only."""
import asyncio
import copy
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from lan_bitable_template_portal import lighthouse_routes, lighthouse_stream
from lan_bitable_template_portal.lighthouse_ai import CustomModel, LighthouseAssistant
from lan_bitable_template_portal.portal_service import LI_SHILONG_OPEN_ID
from test_lighthouse_stream import Store

temp = tempfile.TemporaryDirectory(prefix="lighthouse_stream_preview_")
store = Store(Path(temp.name) / "preview.sqlite3")
model = CustomModel(store, protect=lambda value: value, unprotect=lambda value: value)
model.configure({"api_key": "synthetic-not-a-real-credential"})
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


lighthouse_stream.LighthouseModel = PreviewEngine
app = FastAPI()
def session(request):
    code = request.cookies.get("fixture_user", "D")
    admin = code == "ALL"
    return {"user": {"open_id": LI_SHILONG_OPEN_ID if admin else "fixture-" + code, "name": "隔离测试" + code, "role": "admin" if admin else "building"},
            "role": "admin" if admin else "building", "allowed_scopes": [code]}
controller = SimpleNamespace(_current_session=session, _auth_required_response=lambda: JSONResponse({"ok": False}, status_code=401),
                             _request_base_url=lambda request: str(request.base_url).rstrip("/"))
runtime = SimpleNamespace(state_store=store, auth_manager=SimpleNamespace(is_admin=lambda item: item["role"] == "admin", session_scopes=lambda item: item["allowed_scopes"]))
lighthouse_routes.install_lighthouse_routes(app, controller, runtime)
dist = Path(__file__).resolve().parents[1] / "lan_bitable_template_portal" / "frontend" / "dist"
app.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")
@app.get("/api/health")
async def health():
    return {"ok": True, "service": "clipflow_backend", "instance_id": "isolated-lighthouse-stream"}
@app.get("/api/{rest:path}")
async def fixtures(rest: str, request: Request):
    current = session(request)
    if rest == "auth/status":
        options = [{"value": code, "label": code + "楼"} for code in ("ABCDEH" if current["role"] == "admin" else current["allowed_scopes"])]
        data = {"logged_in": True, "user": current["user"], "scope_options": options}
    elif rest == "scope-overview":
        data = {"scopes": {}}
    else:
        data = {}
    return {"ok": True, "data": data}
@app.get("/{rest:path}")
async def page(rest: str):
    return FileResponse(dist / "index.html")
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=19003, log_level="warning", log_config=None)

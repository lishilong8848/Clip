"""Loopback-only UI test server. All writes target in-memory FakeFeishu."""
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from fastapi import FastAPI,Request
from fastapi.responses import FileResponse,HTMLResponse,JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/'bin'))
from bin.test_cabinet_power import fixtures,MemoryStore,FakeFeishu
from bin.lan_bitable_template_portal.cabinet_power_routes import install_cabinet_power_routes

class Controller:
    def _current_session(self,request): return {"open_id":"fixture-user"}
    def _auth_required_response(self): return JSONResponse({},status_code=401)
    def _json_ok(self,request,session,data): return JSONResponse({"ok":True,"data":data})
    def _portal_error_response(self,exc,default_status): return JSONResponse({"error":str(exc)},status_code=default_status)
    async def _read_json_request(self,request,max_bytes): return await request.json()

app=FastAPI(); controller=Controller(); temporary=tempfile.TemporaryDirectory()
models,records,directory,configs=fixtures(with_power_baseline=True)
runtime=SimpleNamespace(state_store=MemoryStore(),auth_manager=SimpleNamespace(is_admin=lambda s:True,scope_allowed=lambda s,scope:True))
install_cabinet_power_routes(app,controller,runtime)
controller._cabinet_power.root=Path(temporary.name)
from bin.lan_bitable_template_portal.cabinet_power_store import CabinetStore
controller._cabinet_power.local=CabinetStore(Path(temporary.name)/'buildings')
class SlowFakeFeishu(FakeFeishu):
    def update(self,record_id,fields):
        if fields.get('机柜功率（W）')==12347: time.sleep(6)
        return super().update(record_id,fields)

controller._cabinet_power.remote=SlowFakeFeishu(records)
controller._cabinet_power._directory=FakeFeishu(directory)
controller._cabinet_power.do_refresh('',{}, {})

@app.get('/api/auth/status')
def auth(): return {"ok":True,"data":{"logged_in":True,"user":{"name":"本地测试","role":"admin","is_admin":True},"scope_options":[{"value":s,"label":s+"楼"} for s in "ABCDE"]}}

@app.get('/api/health')
def health(): return {"ok":True,"service":"clipflow_backend","instance_id":"cabinet-ui-fixture"}

@app.get('/workbench-lite')
def workbench(request:Request):
    from bin.lan_bitable_template_portal.workbench_lite import render_workbench_lite
    return HTMLResponse(render_workbench_lite(payload={'records':[],'ongoing':[],'stats':{}},session={'role':'admin'},scope=request.query_params.get('scope','D'),work_type=request.query_params.get('work_type','maintenance')))

@app.get('/api/{path:path}')
def empty(path): return {"ok":True,"data":{}}

dist=ROOT/'bin/lan_bitable_template_portal/frontend/dist'
app.mount('/assets',StaticFiles(directory=dist/'assets'),name='assets')
@app.get('/{path:path}')
def index(path): return FileResponse(dist/'index.html')

if __name__=='__main__': uvicorn.run(app,host='127.0.0.1',port=int(sys.argv[1]),log_level='error',log_config=None)

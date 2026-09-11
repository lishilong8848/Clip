"""Authenticated cabinet APIs. Migration is a one-time CLI, not a user workflow."""
import asyncio
from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse
from .cabinet_power import CabinetPowerService
from .cabinet_power_excel import CabinetError, TOTALS
from pathlib import Path

def install_cabinet_power_routes(app,controller,runtime):
    service=CabinetPowerService(runtime.state_store)
    controller._cabinet_power=service

    async def endpoint(request: Request):
        session=controller._current_session(request)
        if session is None: return controller._auth_required_response()
        try:
            path=request.url.path.removeprefix("/api/cabinet-power").strip("/")
            admin=runtime.auth_manager.is_admin(session)
            owner=str(session.get("open_id") or session.get("user",{}).get("open_id") or "")
            if not owner: raise CabinetError("登录身份不完整",401)
            allowed=[s for s in TOTALS if admin or runtime.auth_manager.scope_allowed(session,s)]
            if path=="buildings":
                def buildings():
                    status={item["scope"]:item for item in service.bootstrap(owner).get("buildings",[])}
                    items=[]
                    for scope_code in allowed:
                        current=status.get(scope_code,{"scope":scope_code,"status":"idle","error":""})
                        if current["status"]=="succeeded":
                            try:
                                item={k:v for k,v in service.overview(scope_code).items() if k in ("scope","counts","updated_at","record_count","inventory_only","source")}
                                item["bootstrap_status"]="succeeded"; item["bootstrap_error"]=""
                                items.append(item); continue
                            except Exception as exc:
                                current={**current,"status":"failed","error":str(exc)}
                        items.append({"scope":scope_code,"counts":{"total":0,"formal":0,"test":0,"off":0,"unknown":0},"updated_at":"","record_count":0,"inventory_only":0,"source":"local","bootstrap_status":current["status"],"bootstrap_error":current.get("error","")})
                    return {"buildings":items}
                return controller._json_ok(request,session,await asyncio.to_thread(buildings))
            query=dict(request.query_params)
            payload=await controller._read_json_request(request,max_bytes=512*1024) if request.method in ("POST","PATCH") else {}
            scope=str(payload.get("scope") or query.get("scope") or "")
            if path=="bootstrap":
                if scope and scope not in allowed: raise CabinetError("无权访问该楼栋",403)
                data=await asyncio.to_thread(service.bootstrap,owner,request.method=="POST",bool(payload.get("retry_failed")))
                return controller._json_ok(request,session,data)
            resource=None
            if path.startswith("jobs/"): resource=await asyncio.to_thread(service.job_status,path.split("/")[1])
            elif path.startswith("exports/"): resource=await asyncio.to_thread(service.read,"export:"+path.split("/")[1])
            if path.startswith(("jobs/","exports/")):
                if not resource: raise CabinetError("对象不存在",404)
                scope=resource["scope"]
            if scope not in allowed: raise CabinetError("无权访问该楼栋",403)
            if path.startswith("exports/") and path.endswith("/download"):
                if resource.get("deleted") or not Path(resource["path"]).is_file(): raise CabinetError("导出文件已清理或不可用",410)
                return FileResponse(resource["path"],filename=resource["filename"],media_type="application/vnd.ms-excel.sheet.macroEnabled.12",headers={"Cache-Control":"no-store"})
            if path.startswith("exports/") and path.endswith("/cleanup"):
                data=await asyncio.to_thread(service.cleanup_export,scope,path.split("/")[1])
            elif path=="writes":
                data=await asyncio.to_thread(service.pending_status,scope,owner,admin)
            elif path.startswith("writes/"):
                oid=path.split("/")[1]
                if path.endswith("/resume"): data=await asyncio.to_thread(service.resume_write,scope,oid,owner,admin,defer=query.get('defer')=='1')
                elif path.endswith("/reconcile"): data=await asyncio.to_thread(service.reconcile_write,scope,oid,owner,admin)
                else: data=await asyncio.to_thread(service.write_status,scope,oid,owner,admin,details=query.get('details')=='1')
            elif path=="export-history": data=await asyncio.to_thread(service.export_history,scope)
            elif resource is not None: data=resource
            elif path in ("overview","rooms"): data=await asyncio.to_thread(service.overview,scope)
            elif path.startswith("rooms/") and path.endswith("/layout"): data=await asyncio.to_thread(service.layout,scope,path.split("/")[1])
            elif path=="operations" and request.method=="GET": data=await asyncio.to_thread(service.operations,scope,query)
            elif (path=="operations" and request.method=="POST") or (path.startswith("operations/") and request.method=="PATCH"):
                rid=path.split("/")[1] if "/" in path else ""
                data=await asyncio.to_thread(service.save_operation,scope,payload,owner,rid,can_move_scope=bool(admin and payload.get("confirm_scope_move") is True),defer=query.get('defer')=='1')
            else:
                kind={"refresh":"refresh","exports":"export"}.get(path)
                if not kind: raise CabinetError("接口不存在",404)
                data=await asyncio.to_thread(service.job,scope,kind,owner,payload)
                data={k:v for k,v in data.items() if k!="payload"}
            return controller._json_ok(request,session,data)
        except CabinetError as exc: return JSONResponse({"ok":False,"error":str(exc)},status_code=exc.status_code)
        except Exception as exc: return controller._portal_error_response(exc,default_status=400)

    for path,methods in {
        "buildings":["GET"],"overview":["GET"],"rooms":["GET"],"rooms/{room_id}/layout":["GET"],
        "operations":["GET","POST"],"operations/{record_id}":["PATCH"],"refresh":["POST"],
        "exports":["POST"],"jobs/{job_id}":["GET"],"exports/{export_id}/download":["GET"],
        "writes":["GET"],"writes/{operation_id}":["GET"],"writes/{operation_id}/resume":["POST"],
        "writes/{operation_id}/reconcile":["POST"],
        "export-history":["GET"],"exports/{export_id}/cleanup":["POST"],
        "bootstrap":["GET","POST"],
    }.items():
        app.add_api_route("/api/cabinet-power/"+path,endpoint,methods=methods,name="cabinet_"+path.replace("/","_"))
    app.add_event_handler("shutdown",lambda:service.pool.shutdown(wait=False,cancel_futures=True))

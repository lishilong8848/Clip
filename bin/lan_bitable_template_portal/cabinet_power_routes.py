"""Authenticated cabinet APIs. Migration is a one-time CLI, not a user workflow."""
import asyncio
from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse
from .cabinet_power import CabinetPowerService
from .cabinet_power_batches import MAX_FILE_BYTES, MAX_TOTAL_BYTES
from .cabinet_power_excel import CabinetError, TOTALS
from pathlib import Path

def install_cabinet_power_routes(app,controller,runtime):
    service=CabinetPowerService(runtime.state_store)
    controller._cabinet_power=service
    runtime.cabinet_power_service=service
    ensure_notice_worker=getattr(runtime,"ensure_cabinet_notice_worker",None)
    if callable(ensure_notice_worker): app.add_event_handler("startup",ensure_notice_worker)

    async def endpoint(request: Request):
        session=controller._current_session(request)
        if session is None: return controller._auth_required_response()
        try:
            path=request.url.path.removeprefix("/api/cabinet-power").strip("/")
            admin=runtime.auth_manager.is_admin(session)
            owner=str(session.get("open_id") or session.get("user",{}).get("open_id") or "")
            if not owner: raise CabinetError("登录身份不完整",401)
            allowed=[s for s in TOTALS if admin or runtime.auth_manager.scope_allowed(session,s)]
            query=dict(request.query_params)
            if path.startswith("batches"):
                if path=="batches/retry-handoffs" and request.method=="POST":
                    if not admin: raise CabinetError("仅管理员可重试通告联动",403)
                    primary=runtime.state_store.requeue_failed_outbox_events(
                        runtime.cabinet_notice_queue_channel,max_attempts=runtime.cabinet_notice_max_attempts+1)
                    fallback=service.batches.store.requeue_failed_notice_handoffs()
                    runtime.ensure_cabinet_notice_worker(); runtime.cabinet_notice_queue_event.set()
                    return controller._json_ok(request,session,{"requeued":primary+fallback})
                if path=="batches/recognize" and request.method=="POST":
                    try:
                        form=await request.form(max_files=10,max_fields=20,max_part_size=10*1024*1024)
                    except TypeError:
                        form=await request.form(max_files=10,max_fields=20)
                    uploads=form.getlist("files")
                    files=[]; total=0
                    for upload in uploads:
                        if not getattr(upload,"filename","") or not hasattr(upload,"read"): continue
                        try: content=await upload.read(10*1024*1024+1)
                        finally: await upload.close()
                        total+=len(content)
                        if len(content)>10*1024*1024: raise CabinetError(f"{upload.filename} 超过10MiB",413)
                        if total>30*1024*1024: raise CabinetError("单批PDF总大小不能超过30MiB",413)
                        files.append((upload.filename,content))
                    data=await asyncio.to_thread(service.batches.recognize,files,owner)
                    data=service.batches.visible(data,owner,allowed,admin)
                    response=controller._json_ok(request,session,data); response.status_code=202
                    return response
                payload=await controller._read_json_request(request,max_bytes=4*1024*1024) if request.method in ("POST","PATCH") and not (path.endswith("/images") and request.method=="POST") else {}
                if path=="batches/text-preview" and request.method=="POST":
                    data=await asyncio.to_thread(service.batches.text_preview,payload.get("sources"),allowed)
                    return controller._json_ok(request,session,data)
                if path=="batches":
                    if request.method=="POST":
                        if payload.get("source")=="image":
                            if payload.get("rows"): raise CabinetError("图片识别批次不能预置机柜记录",400)
                            data=await asyncio.to_thread(service.batches.create_image_batch,owner,allowed,payload.get("scope"))
                        elif payload.get("source")=="text":
                            data=await asyncio.to_thread(service.batches.create_text,payload,owner,allowed)
                        else:
                            rows=payload.get("rows",[])
                            requested={str(row.get("scope") or "").upper().replace("楼","") for row in rows if isinstance(row,dict)}
                            if not admin and not requested<=set(allowed): raise CabinetError("批量内容包含无权操作的楼栋",403)
                            data=await asyncio.to_thread(service.batches.create_manual,rows,owner)
                        data=service.batches.visible(data,owner,allowed,admin)
                    else:
                        data=await asyncio.to_thread(service.batches.list,owner,allowed,admin,str(query.get("scope") or ""),str(query.get("status") or ""),str(query.get("from") or ""),str(query.get("to") or ""),query.get("page",1),query.get("page_size",20))
                        if admin:
                            primary=runtime.state_store.list_outbox_events(
                                runtime.cabinet_notice_queue_channel,status="failed",limit=100)
                            fallback=service.batches.store.failed_notice_handoffs()
                            data["handoff_errors"]=[
                                {"source":"主队列","event_id":item["id"],"attempts":item["attempts"],
                                 "error":item["last_error"],"updated_at":item["updated_at"],**{
                                     key:(item.get("payload") or {}).get(key,"") for key in
                                     ("target_record_id","notice_type","event_action")}}
                                for item in primary
                            ]+[
                                {"source":"兜底队列","event_id":item["key"],"attempts":item["attempts"],
                                 "error":item["error"],"updated_at":item["updated_at"],**{
                                     key:(item.get("payload") or {}).get(key,"") for key in
                                     ("target_record_id","notice_type","event_action")}}
                                for item in fallback
                            ]
                    return controller._json_ok(request,session,data)
                parts=path.split("/")
                if len(parts)<2: raise CabinetError("接口不存在",404)
                batch_id=parts[1]
                if len(parts)==3 and parts[2]=="status" and request.method=="GET":
                    data=await asyncio.to_thread(service.batches.status,batch_id,owner,allowed,admin)
                    return controller._json_ok(request,session,data)
                if len(parts)==3 and parts[2]=="images" and request.method=="POST":
                    try:
                        form=await request.form(max_files=10,max_fields=20,max_part_size=MAX_FILE_BYTES)
                    except TypeError:
                        form=await request.form(max_files=10,max_fields=20)
                    uploads=form.getlist("files")
                    files=[]; total=0
                    for upload in uploads:
                        if not getattr(upload,"filename","") or not hasattr(upload,"read"): continue
                        try: content=await upload.read(MAX_FILE_BYTES+1)
                        finally: await upload.close()
                        total+=len(content)
                        if len(content)>MAX_FILE_BYTES or total>MAX_TOTAL_BYTES: raise CabinetError("截图超出单张10MiB或每次30MiB限制",413)
                        files.append((upload.filename,content))
                    data=await asyncio.to_thread(service.batches.add_images,batch_id,files,owner,allowed,admin)
                    response=controller._json_ok(request,session,service.batches.visible(data,owner,allowed,admin))
                    response.status_code=202
                    return response
                if len(parts)==4 and parts[2]=="images" and request.method=="GET":
                    thumbnail=query.get("thumbnail")=="1"
                    file_path,image=await asyncio.to_thread(service.batches.image_path,batch_id,parts[3],owner,allowed,admin,thumbnail)
                    media_type="image/png" if thumbnail else {".jpg":"image/jpeg",".png":"image/png",".webp":"image/webp"}.get(image["extension"],"application/octet-stream")
                    return FileResponse(file_path,media_type=media_type,headers={"Cache-Control":"private, max-age=86400", "X-Content-Type-Options":"nosniff"})
                if len(parts)==4 and parts[2]=="images" and request.method=="DELETE":
                    data=await asyncio.to_thread(service.batches.delete_image,batch_id,parts[3],query.get("version"),owner,allowed,admin)
                    return controller._json_ok(request,session,service.batches.visible(data,owner,allowed,admin))
                if len(parts)==5 and parts[2]=="images" and parts[4]=="apply" and request.method=="POST":
                    data=await asyncio.to_thread(service.batches.apply_image,batch_id,parts[3],payload,owner,allowed,admin)
                    return controller._json_ok(request,session,service.batches.visible(data,owner,allowed,admin))
                if len(parts)==5 and parts[2]=="images" and parts[4] in ("retry","correct") and request.method=="POST":
                    action=service.batches.retry_image if parts[4]=="retry" else service.batches.correct_image
                    data=await asyncio.to_thread(action,batch_id,parts[3],payload,owner,allowed,admin)
                    return controller._json_ok(request,session,service.batches.visible(data,owner,allowed,admin))
                if len(parts)==5 and parts[2]=="images" and parts[4]=="restore" and request.method=="POST":
                    data=await asyncio.to_thread(service.batches.restore_image,batch_id,parts[3],payload.get("version"),owner,allowed,admin)
                    return controller._json_ok(request,session,service.batches.visible(data,owner,allowed,admin))
                if len(parts)==4 and parts[2]=="files" and request.method=="GET":
                    file_path,filename=await asyncio.to_thread(service.batches.file_path,batch_id,parts[3],owner,admin)
                    return FileResponse(file_path,filename=filename,media_type="application/pdf",headers={"Cache-Control":"no-store"})
                if len(parts)==5 and parts[2]=="files" and parts[4]=="cleanup":
                    data=await asyncio.to_thread(service.batches.cleanup_file,batch_id,parts[3],owner,admin)
                    return controller._json_ok(request,session,service.batches.visible(data,owner,allowed,admin))
                if len(parts)==2:
                    if request.method=="DELETE":
                        data=await asyncio.to_thread(service.batches.delete_empty,batch_id,owner,admin,query.get("version"))
                    elif request.method=="PATCH":
                        data=await asyncio.to_thread(service.batches.update,batch_id,payload,owner,allowed,admin)
                    else: data=await asyncio.to_thread(service.batches.get,batch_id)
                elif len(parts)==3 and parts[2]=="clear-overlaps":
                    data=await asyncio.to_thread(service.batches.clear_overlaps,batch_id,payload.get("version"),owner,allowed,admin)
                elif len(parts)==3 and parts[2]=="confirm":
                    data=await asyncio.to_thread(service.batches.confirm,batch_id,payload,owner,allowed,admin)
                elif len(parts)==3 and parts[2]=="rollback":
                    data=await asyncio.to_thread(service.batches.rollback,batch_id,payload,owner,allowed,admin)
                elif len(parts)==3 and parts[2]=="restore-rows":
                    data=await asyncio.to_thread(service.batches.restore_rows,batch_id,payload,owner,allowed,admin)
                elif len(parts)==3 and parts[2]=="cancel":
                    data=await asyncio.to_thread(service.batches.cancel,batch_id,owner,admin,payload.get("version"))
                else: raise CabinetError("接口不存在",404)
                return controller._json_ok(request,session,service.batches.visible(data,owner,allowed,admin))
            if path=="buildings":
                def buildings():
                    status={item["scope"]:item for item in service.bootstrap(owner).get("buildings",[])}
                    items=[]
                    for scope_code in allowed:
                        current=status.get(scope_code,{"scope":scope_code,"status":"idle","error":""})
                        if current["status"]=="succeeded":
                            try:
                                item={k:v for k,v in service.overview(scope_code,False).items() if k in ("scope","counts","updated_at","record_count","inventory_only","source")}
                                item["bootstrap_status"]="succeeded"; item["bootstrap_error"]=""
                                items.append(item); continue
                            except Exception as exc:
                                current={**current,"status":"failed","error":str(exc)}
                        items.append({"scope":scope_code,"counts":{"total":0,"formal":0,"test":0,"off":0,"unknown":0},"updated_at":"","record_count":0,"inventory_only":0,"source":"local","bootstrap_status":current["status"],"bootstrap_error":current.get("error","")})
                    return {"buildings":items}
                return controller._json_ok(request,session,await asyncio.to_thread(buildings))
            if path=="storage":
                if not admin: raise CabinetError("仅管理员可管理机柜本地缓存",403)
                data=await asyncio.to_thread(
                    service.batches.cleanup_evidence_cache if request.method=="POST" else service.batches.storage_status
                )
                return controller._json_ok(request,session,data)
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
            if path.startswith("exports/") and path.endswith("/upload"):
                data=await asyncio.to_thread(service.upload_export,scope,path.split("/")[1],owner)
            elif path.startswith("exports/") and path.endswith("/cleanup"):
                data=await asyncio.to_thread(service.cleanup_export,scope,path.split("/")[1])
            elif path=="writes":
                data=await asyncio.to_thread(service.pending_status,scope,owner,admin)
            elif path.startswith("writes/"):
                oid=path.split("/")[1]
                if path.endswith("/resume"): data=await asyncio.to_thread(service.resume_write,scope,oid,owner,admin,defer=query.get('defer')=='1')
                elif path.endswith("/reconcile"): data=await asyncio.to_thread(service.reconcile_write,scope,oid,owner,admin)
                else: data=await asyncio.to_thread(service.write_status,scope,oid,owner,admin,details=query.get('details')=='1')
            elif path=="export-history": data=await asyncio.to_thread(service.export_history,scope,query.get("page",1),query.get("page_size",20))
            elif resource is not None: data=resource
            elif path in ("overview","rooms"): data=await asyncio.to_thread(service.overview,scope,query.get("summary")!="1")
            elif path=="racks": data=await asyncio.to_thread(service.racks,scope)
            elif path=="rack-power" and request.method=="PATCH":
                data=await asyncio.to_thread(service.save_rack_power,scope,payload,owner,defer=query.get('defer')=='1')
            elif path.startswith("rooms/") and path.endswith("/layout"): data=await asyncio.to_thread(service.layout,scope,path.split("/")[1])
            elif path=="operations" and request.method=="GET": data=await asyncio.to_thread(service.operations,scope,query)
            elif path.startswith("operations/") and path.count("/")==3 and "/evidence/" in path and request.method=="GET":
                _,record_id,_,image_id=path.split("/")
                file_path,media_type=await asyncio.to_thread(service.evidence_path,scope,record_id,image_id,query.get("thumbnail")=="1")
                return FileResponse(file_path,media_type=media_type,headers={"Cache-Control":"private, max-age=86400","X-Content-Type-Options":"nosniff"})
            elif path.startswith("operations/") and path.count("/")==3 and "/documents/" in path and request.method=="GET":
                _,record_id,_,file_id=path.split("/")
                file_path,filename=await asyncio.to_thread(service.document_path,scope,record_id,file_id,allowed,admin)
                return FileResponse(file_path,filename=filename,media_type="application/pdf",
                    headers={"Cache-Control":"private, no-store","X-Content-Type-Options":"nosniff"})
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
        "batches/retry-handoffs":["POST"],"batches/recognize":["POST"],"batches":["GET","POST"],"batches/{batch_id}":["GET","PATCH","DELETE"],
        "batches/{batch_id}/status":["GET"],
        "batches/{batch_id}/clear-overlaps":["POST"],"batches/{batch_id}/confirm":["POST"],"batches/{batch_id}/rollback":["POST"],
        "batches/{batch_id}/cancel":["POST"],"batches/{batch_id}/restore-rows":["POST"],"batches/{batch_id}/files/{file_id}":["GET"],
        "batches/{batch_id}/images":["POST"],"batches/{batch_id}/images/{image_id}":["GET","DELETE"],
        "batches/{batch_id}/images/{image_id}/apply":["POST"],
        "batches/{batch_id}/images/{image_id}/retry":["POST"],
        "batches/{batch_id}/images/{image_id}/correct":["POST"],
        "batches/text-preview":["POST"],
        "batches/{batch_id}/images/{image_id}/restore":["POST"],
        "batches/{batch_id}/files/{file_id}/cleanup":["POST"],
        "buildings":["GET"],"overview":["GET"],"rooms":["GET"],"racks":["GET"],"rooms/{room_id}/layout":["GET"],
        "operations":["GET","POST"],"operations/{record_id}":["PATCH"],
        "rack-power":["PATCH"],
        "operations/{record_id}/evidence/{image_id}":["GET"],"operations/{record_id}/documents/{file_id}":["GET"],"refresh":["POST"],
        "exports":["POST"],"jobs/{job_id}":["GET"],"exports/{export_id}/download":["GET"],
        "writes":["GET"],"writes/{operation_id}":["GET"],"writes/{operation_id}/resume":["POST"],
        "writes/{operation_id}/reconcile":["POST"],
        "export-history":["GET"],"exports/{export_id}/upload":["POST"],"exports/{export_id}/cleanup":["POST"],
        "bootstrap":["GET","POST"],
        "storage":["GET","POST"],
    }.items():
        app.add_api_route("/api/cabinet-power/"+path,endpoint,methods=methods,name="cabinet_"+path.replace("/","_"))
    def shutdown():
        stop_notice_worker=getattr(runtime,"stop_cabinet_notice_worker",None)
        if callable(stop_notice_worker): stop_notice_worker()
        service.pool.shutdown(wait=False,cancel_futures=True)
    app.add_event_handler("shutdown",shutdown)

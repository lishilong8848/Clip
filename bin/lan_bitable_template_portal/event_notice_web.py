"""Web event cards reuse the existing durable notice job pipeline."""
import asyncio
import re
from urllib.parse import urlsplit
from fastapi import Request
from fastapi.responses import JSONResponse
from .portal_service import PortalError
from .module_access import require_module
from upload_event_module.config import EVENT_LEVEL_OPTIONS, EVENT_SOURCE_OPTIONS, SPECIALTY_OPTIONS


def prepare_event(service,payload,job_id):
    action=str(payload.get('action') or '')
    status={'start':'新增','update':'更新','end':'结束'}.get(action)
    if not status:raise PortalError('请选择开始、更新或结束。')
    values={key:str(payload.get(key) or '').strip() for key in ('title','content','impact','progress','event_source','level','specialty')}
    values['content']=values['content'] or values['title']
    when=service._format_input_datetime(payload.get('time_str') or payload.get('start_time'))
    if not all(values[key] for key in ('title','content','impact','progress','event_source','level','specialty')) or not when:
        raise PortalError('事件必填字段不完整，请填写标题、发生时间、来源、等级、专业、概述、影响和进展。')
    if values['level'] not in EVENT_LEVEL_OPTIONS or values['event_source'] not in EVENT_SOURCE_OPTIONS:
        raise PortalError('事件等级或来源字段无效，请重新选择。')
    scope=service._normalize_scope(payload.get('scope'))
    building,codes=service._resolve_notice_submit_building(scope=scope,request_payload=payload,
        building=payload.get('building'),title=values['title'],location='',content=values['content'],
        work_type_label='事件通告',allow_scope_fallback=True)
    target=service._target_record_id_from_request_payload(payload) if action!='start' else ''
    if action!='start' and not target:raise PortalError('事件目标记录缺失，请刷新后核对。')
    code,recipients,error=service._recipients_for_building_codes(codes,fallback_building=building)
    if error:raise PortalError(error)
    prepared={**values,'job_id':job_id,'operation_id':payload.get('operation_id') or job_id,
        'work_type':'event','notice_type':'事件通告','action':action,'status':status,'manual':True,
        'scope':scope,'scope_label':service._scope_label(scope),'building':building,'building_code':code,
        'building_codes':codes,'buildings':[service._building_label_from_code(c) for c in codes],
        'target_building':service._building_label_from_codes(codes),'record_id':payload.get('record_id') or payload.get('manual_id'),
        'source_record_id':'','target_record_id':target,'active_item_id':payload.get('active_item_id') or '',
        'time_str':when,'start_time':when,'end_time':'','response_time':service._action_response_time(payload),
        'recipients':recipients,'skip_personal_message':False,'extra_images':[],
        'recover_selected':False,'transfer_to_overhaul':payload.get('transfer_to_overhaul')}
    return prepared


def install_event_notice_web(app,controller,runtime):
    from .event_notice_delivery import EventNoticeDelivery,NS,OPS,SCOPES,dispatch
    manager=EventNoticeDelivery(runtime)

    def legacy_listing(session,scope,refresh=False):
        service=runtime.service
        require_module(runtime.auth_manager,session,'event')
        scope=controller._authorized_scope_or_error(session,scope)
        from .account_policy import require_notice_delete
        try:require_notice_delete(runtime.auth_manager,session,'event');can_delete=True
        except Exception:can_delete=False
        rows={}
        ongoing=controller._get_ongoing(scope)
        for item in ongoing:
            if service._item_work_type(item)!='event':continue
            target=str(item.get('target_record_id') or item.get('record_id') or '')
            if not target.startswith('rec'):continue
            rows[target]=dict(item)
        from upload_event_module.config import get_field_config
        field_config=get_field_config('事件通告')
        for record in service._target_records_for_notice_type('事件通告','event',force_refresh=refresh):
            fields=record.get('display_fields') or {}
            codes=service._target_record_building_codes(fields,field_config)
            if not codes or not service._scope_matches_buildings(scope,codes) or not all(runtime.auth_manager.scope_allowed(session,c) for c in codes):continue
            target=record['record_id'];current=rows.get(target,{})
            operations=runtime.state_store.list_notice_remote_operations_for_target(target)
            latest=next((op for op in reversed(operations) if op.get('status')=='completed' and (op.get('result') or {}).get('robot_notice_type')=='事件通告'),{})
            original=(latest.get('result') or {}).get('robot_payload') or {}
            if original.get('text'):
                from upload_event_module.services.handlers.event_notice import EventNoticeHandler
                parsed={key:EventNoticeHandler()._extract_section(original['text'],label) for key,label in (('title','标题'),('content','概述'),('impact','影响'),('progress','进展'))}
                current={**parsed,**current,'text':original['text']}
            form=service._target_record_form_fields(work_type='event',notice_type='事件通告',target_record=record)
            # Qt projection retains the full original text, including progress.
            merged={**form,**current,'record_id':target,'target_record_id':target,'work_type':'event',
                'notice_type':'事件通告','building_codes':codes,'building':service._building_label_from_codes(codes),
                'transfer_to_overhaul':service._event_checkbox_checked(fields.get(field_config.get('transfer_to_overhaul'))),
                'recover_time':service._target_form_datetime_value(fields.get(field_config.get('recover_time'))),
                'progress_time_base':service._clean_source_text(fields.get(field_config.get('progress_update'))),
                'ended':service._event_status(fields)=='已结束'}
            merged['time_str']=merged.get('time_str') or form.get('start_time','')
            merged['content']=merged.get('content') or merged.get('alarm_desc') or merged.get('title','')
            rows[target]=merged
        # Recheck even cached Qt rows; this endpoint never broadens building access.
        items=[]
        for item in rows.values():
            codes=service._clean_building_codes(item.get('building_codes') or item.get('buildings') or item.get('building'))
            if not codes or not service._scope_matches_buildings(scope,codes) or not all(runtime.auth_manager.scope_allowed(session,c) for c in codes):continue
            items.append({key:item.get(key) for key in ('record_id','target_record_id','active_item_id','title','text','content','impact','progress','time_str','start_time','end_time','event_source','source','level','specialty','building','building_codes','ended','transfer_to_overhaul','recover_time','progress_time_base')})
        return {'items':items,'levels':list(EVENT_LEVEL_OPTIONS),'sources':list(EVENT_SOURCE_OPTIONS),
            'can_delete':can_delete,'user_id':str((session.get('user') or {}).get('open_id') or ''),
            'scopes':[c for c in ('110','A','B','C','D','E','H') if runtime.auth_manager.scope_allowed(session,c) and (scope in ('ALL','CAMPUS') or scope==c)]}
    def options(session,scope):
        require_module(runtime.auth_manager,session,'event')
        scope=controller._authorized_scope_or_error(session,scope)
        allowed=[c for c in SCOPES if runtime.auth_manager.scope_allowed(session,c) and (scope in ('ALL','CAMPUS') or scope==c)]
        if not allowed:raise PortalError('当前楼栋范围不可用。')
        from .account_policy import require_notice_delete
        try:require_notice_delete(runtime.auth_manager,session,'event');can_delete=True
        except PortalError:can_delete=False
        return {'user_id':str((session.get('user') or {}).get('open_id') or ''),'scopes':allowed,'levels':list(EVENT_LEVEL_OPTIONS),'specialties':list(SPECIALTY_OPTIONS),
                'sources':list(EVENT_SOURCE_OPTIONS),'can_delete':can_delete,
                'defaults':manager.preference(str((session.get('user') or {}).get('open_id') or '')),
                'robots':[{key:r[key] for key in ('id','name','remark','scopes','types')} for r in manager.ding.config()['robots'] if r['enabled'] and (not r['types'] or '事件通告' in r['types']) and (not r['scopes'] or set(allowed)&set(r['scopes']))]}

    def authorized(session,row,*,write=False):
        actor=str((session.get('user') or {}).get('open_id') or '')
        if row.get('created_by') and not row.get('operations') and not row.get('target_record_id') and row['created_by']!=actor:return False
        scopes=row.get('access_scopes') or []
        return bool(scopes) and all(runtime.auth_manager.scope_allowed(session,code) for code in scopes)

    def listing(session,scope,refresh=False,local=False):
        result=options(session,scope);warning=''
        try:
            old={'items':[]} if local else legacy_listing(session,scope,refresh)
            known={item['payload'].get('target_record_id'):item['payload'] for item in runtime.state_store.list_documents(NS) if item['payload'].get('target_record_id')}
            for item in old['items']:
                target=item['record_id']
                if target in known:continue
                identity='remote_'+target
                with manager.lock('event:'+identity):
                    if manager.get(identity):continue
                    row=manager.new(identity,'',item['building_codes'])
                    row.update(state='ended' if item.get('ended') else 'active',target_record_id=target,progress_time_base=item.get('progress_time_base') or '',
                        text=item.get('text') or runtime.service.build_event_notice_text(status='结束' if item.get('ended') else '更新',title=item.get('title',''),source=item.get('event_source') or item.get('source',''),time_str=item.get('time_str',''),summary=item.get('content',''),impact=item.get('impact',''),progress=item.get('progress','')),
                        metadata={'summary':item.get('content') or item.get('title',''),'building':item['building_codes'][0] if len(item['building_codes'])==1 else item['building'],'specialty':item.get('specialty') or '',
                            'level':item.get('level') or '','event_source':item.get('event_source') or item.get('source') or '',
                            'occurrence_time':item.get('time_str') or item.get('start_time') or '',
                            'transfer_to_overhaul':bool(item.get('transfer_to_overhaul')),'recover_time':item.get('recover_time') or ''},sync_state='synced')
                    row['synced_fields']=manager.metadata_fields({**row,'target_record_id':''})
                    runtime.state_store.put_document(NS,identity,row)
        except Exception:warning='远端历史事件暂未完整返回，已保留本地记录；发送新消息不受影响。'
        rows=[item['payload'] for item in runtime.state_store.list_documents(NS)]
        result['items']=[manager.public(row) for row in rows if row.get('state')!='deleted' and authorized(session,row) and set(row['access_scopes'])&set(result['scopes']) and (row.get('operations') or row.get('target_record_id'))]
        result['warning']=warning
        return result

    async def endpoint(request:Request):
        session=await asyncio.to_thread(controller._current_session,request)
        if not session:return controller._auth_required_response()
        try:require_module(runtime.auth_manager,session,'event')
        except PortalError:
            return JSONResponse({'ok':False,'error':'当前账号没有事件通告权限。'},status_code=403)
        try:
            reader=options if request.url.path.endswith('/options') else listing
            args=[session,request.query_params.get('scope','ALL')]
            if reader==listing:args.extend([request.query_params.get('refresh')=='1',request.query_params.get('local')=='1'])
            data=await asyncio.to_thread(reader,*args)
            return JSONResponse({'ok':True,'data':data},headers={'Cache-Control':'no-store'})
        except Exception as exc:
            return JSONResponse({'ok':False,'error':str(exc) if isinstance(exc,PortalError) else '事件资料暂未完整返回，请重试。'},status_code=503)
    app.add_api_route('/api/workbench-events',endpoint,methods=['GET'])
    app.add_api_route('/api/workbench-events/options',endpoint,methods=['GET'])

    async def mutation(request:Request):
        session=await asyncio.to_thread(controller._current_session,request)
        if not session:return controller._auth_required_response()
        try:
            require_module(runtime.auth_manager,session,'event')
            if session.get('is_guest') or session.get('temporary'):raise PortalError('请使用正式账号操作。')
            if session.get('impersonation') or session.get('source')=='admin_impersonation':raise PortalError('请使用本人账号发送或补充网页事件资料。')
            origin=urlsplit(request.headers.get('origin') or request.headers.get('referer') or '')
            base=urlsplit(controller._request_base_url(request))
            if not origin.netloc or (origin.scheme,origin.netloc)!=(base.scheme,base.netloc) or request.headers.get('sec-fetch-site')=='cross-site':raise PortalError('请求来源无效。')
            body=await controller._read_json_request(request,max_bytes=128*1024)
            if not isinstance(body,dict):raise PortalError('请求格式无效。')
            actor=str((session.get('user') or {}).get('open_id') or '')
            if not actor:raise PortalError('登录身份无效。')
            context=await asyncio.to_thread(options,session,body.get('scope') or 'ALL')
            action=request.path_params['action']
            if action=='defaults':
                ids=body.get('robot_ids',[])
                if not isinstance(ids,list) or not set(ids)<={r['id'] for r in context['robots']}:raise PortalError('默认机器人不在当前可选范围内。')
                data=await asyncio.to_thread(manager.save_preference,actor,ids,body.get('revision'))
            else:
                identity=str(body.get('event_id') or '')
                if not re.fullmatch(r'(?:web_[a-f0-9]{32}|remote_rec[A-Za-z0-9_-]+)',identity):raise PortalError('事件标识无效。')
                row=await asyncio.to_thread(manager.get,identity)
                if row and not authorized(session,row,write=True):raise PortalError('无权处理该事件。')
                if not row and not identity.startswith('web_'):raise PortalError('请刷新事件后再操作。')
                if action=='send':
                    data=await asyncio.to_thread(manager.accept,identity,str(body.get('operation_id') or ''),actor,context['scopes'],body.get('action'),body.get('text'))
                    if body.get('metadata_patch'):
                        try:data=await asyncio.to_thread(manager.save_metadata,identity,actor,context['scopes'],body['metadata_patch'],body.get('metadata_base') or {})
                        except PortalError as exc:data['metadata_save_warning']=str(exc)
                elif action=='metadata':
                    data=await asyncio.to_thread(manager.save_metadata,identity,actor,context['scopes'],body.get('patch') or {},body.get('base') or {})
                elif action=='retry':
                    data=await asyncio.to_thread(manager.retry,identity,str(body.get('operation_id') or ''),body.get('confirm_uncertain') is True)
                elif action=='delete':
                    from .account_policy import require_notice_delete
                    require_notice_delete(runtime.auth_manager,session,'event')
                    data=await asyncio.to_thread(manager.delete,identity,actor)
                else:raise PortalError('操作不存在。')
                dispatch(runtime)
            return JSONResponse({'ok':True,'data':data},headers={'Cache-Control':'no-store'})
        except PortalError as exc:return JSONResponse({'ok':False,'error':str(exc)},status_code=409)
        except Exception:return JSONResponse({'ok':False,'error':'结果尚未完整返回，原消息与填写保留，请重试原操作。'},status_code=503)
    app.add_api_route('/api/workbench-events/{action}',mutation,methods=['POST'])

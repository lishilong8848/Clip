import asyncio
import hashlib
import re
from urllib.parse import urlsplit
from fastapi import Request
from fastapi.responses import JSONResponse
from .dingtalk_notices import DingTalkNotices,DingTalkError


def install_dingtalk_notice_routes(app,controller,runtime):
    async def options(request:Request):
        session=await asyncio.to_thread(controller._current_session,request)
        if not session:return controller._auth_required_response()
        from .module_access import require_module
        work=request.query_params.get('work_type','maintenance')
        if work not in {'maintenance','change','event','repair','polling','adjust','power'}:
            return JSONResponse({'ok':False,'error':'通告类型无效。'},status_code=400)
        require_module(runtime.auth_manager,session,work)
        robots=(await asyncio.to_thread(DingTalkNotices(runtime.state_store).config))['robots']
        selected=[]
        target=request.query_params.get('target_record_id','')
        if re.fullmatch(r'rec[A-Za-z0-9_-]+',target):
            operations=await asyncio.to_thread(runtime.state_store.list_notice_remote_operations_for_target,target)
            for op in reversed(operations):
                result=op.get('result') or {}; payload=result.get('robot_payload') or {}; request_data=op.get('request') or {}
                if op.get('status')!='completed' or request_data.get('work_type')!=work:continue
                scopes=runtime.service._building_codes_from_value(payload.get('buildings') or request_data.get('building_codes') or request_data.get('building'))
                if not scopes or not all(runtime.auth_manager.scope_allowed(session,c) for c in scopes):continue
                ids=request_data.get('dingtalk_robot_ids')
                if isinstance(ids,list):selected=[value for value in ids if isinstance(value,str)];break
        return JSONResponse({'ok':True,'data':{'selected_robot_ids':selected,'robots':[{key:r[key] for key in ('id','name','remark','scopes','types')} for r in robots if r['enabled']]}},headers={'Cache-Control':'no-store'})
    app.add_api_route('/api/workbench/dingtalk-options',options,methods=['GET'])
    async def job_delivery(request:Request):
        session=await asyncio.to_thread(controller._current_session,request)
        if not session:return controller._auth_required_response()
        job=await asyncio.to_thread(runtime.service.get_job,request.path_params['job_id'])
        if not job:return JSONResponse({'ok':False,'error':'原任务不存在。'},status_code=404)
        if not controller._job_visible_to_session(job,session):
            return JSONResponse({'ok':False,'error':'无权查看该任务。'},status_code=403)
        service=DingTalkNotices(runtime.state_store)
        operation_id=job.get('remote_operation_id') or 'notice_action:'+request.path_params['job_id']
        operation=await asyncio.to_thread(runtime.state_store.get_notice_remote_operation,operation_id) or {}
        try:
            if request.method=='POST':
                actual=urlsplit(request.headers.get('origin') or request.headers.get('referer') or '')
                expected=urlsplit(controller._request_base_url(request))
                if (actual.scheme,actual.netloc)!=(expected.scheme,expected.netloc) or not actual.netloc:raise DingTalkError('请求来源无效。',403)
                actor=(session.get('user') or {}).get('open_id')
                if actor!=(job.get('request') or {}).get('_auth_open_id') and not runtime.auth_manager.is_admin(session):raise DingTalkError('仅原发起人或管理员可以重试消息。',403)
                if not runtime.auth_manager.scope_allowed(session,(job.get('request') or {}).get('scope') or 'ALL'):raise DingTalkError('当前账号已无权处理原任务楼栋。',403)
                if operation.get('status')!='completed':raise DingTalkError('原通告尚未完成，请等待原任务。',409)
                body=await controller._read_json_request(request,max_bytes=2048)
                if not isinstance(body,dict):raise DingTalkError('请求格式无效。')
                for target in (operation.get('result') or {}).get('dingtalk_targets') or []:
                    key=hashlib.sha256((operation_id+':'+target['id']).encode()).hexdigest()
                    row=await asyncio.to_thread(runtime.state_store.get_document,'dingtalk_notices','delivery:'+key)
                    if row and row.get('state') in ('failed','uncertain','posting'):
                        await asyncio.to_thread(service.retry,key,body.get('confirm_uncertain') is True)
            result=operation.get('result') or {};rows=[]
            for target in result.get('dingtalk_targets') or []:
                key=hashlib.sha256((operation_id+':'+target['id']).encode()).hexdigest()
                row=await asyncio.to_thread(runtime.state_store.get_document,'dingtalk_notices','delivery:'+key) or {'state':'pending'}
                rows.append({'name':target['name'],'state':row.get('state'),'reason':row.get('reason','') or result.get('dingtalk_selection_error','') or target.get('mention_warning','')})
            state=('skipped' if result.get('dingtalk_delivery_state')=='superseded' else 'sent' if result.get('dingtalk_delivery_state')=='done' else 'failed' if result.get('dingtalk_delivery_state')=='failed' else 'pending') if rows else ('sent' if operation.get('status')=='completed' else 'pending')
            if request.method=='POST':state='pending'
            return JSONResponse({'ok':True,'data':{'state':state,'robots':rows,'time_warning':'实际开始时间已被其他操作修改，未覆盖，请核对记录。' if result.get('dingtalk_start_time_state')=='conflict' else ''}},headers={'Cache-Control':'no-store'})
        except DingTalkError as exc:return JSONResponse({'ok':False,'error':str(exc)},status_code=exc.status)
    app.add_api_route('/api/jobs/{job_id}/dingtalk',job_delivery,methods=['GET','POST'])
    async def endpoint(request:Request):
        try:
            session=await asyncio.to_thread(controller._current_session,request)
            if not session:raise DingTalkError('请先登录。',401)
            if session.get('is_guest') or not runtime.auth_manager.is_admin(session) or session.get('impersonation') or session.get('source')=='admin_impersonation':
                raise DingTalkError('仅管理员本人可以管理钉钉通告推送。',403)
            service=DingTalkNotices(runtime.state_store)
            if request.method=='GET':
                data=await asyncio.to_thread(service.public)
                data['deliveries']=await asyncio.to_thread(service.recent)
            else:
                actual=urlsplit(request.headers.get('origin') or request.headers.get('referer') or '')
                expected=urlsplit(controller._request_base_url(request))
                if not actual.netloc or (actual.scheme,actual.netloc)!=(expected.scheme,expected.netloc) or request.headers.get('sec-fetch-site')=='cross-site':raise DingTalkError('请求来源无效。',403)
                body=await controller._read_json_request(request,max_bytes=65536)
                if not isinstance(body,dict):raise DingTalkError('请求格式无效。')
                action=request.path_params.get('action','')
                if not action:data=await asyncio.to_thread(service.save,body)
                elif action=='test':
                    operation=str(body.get('operation_id') or '')
                    if not re.fullmatch('[a-f0-9]{32}',operation):raise DingTalkError('测试操作标识无效。')
                    robot=next((r for r in service.config()['robots'] if r['id']==body.get('robot_id') and r['enabled']),None)
                    if not robot:raise DingTalkError('请先保存并启用机器人。')
                    key=hashlib.sha256(('test:'+operation+':'+robot['id']).encode()).hexdigest()
                    data=await asyncio.to_thread(service.deliver,key,{k:robot[k] for k in ('id','revision','name')},
                        '【灯塔通告推送测试】\n这是一条管理员主动发送的连接测试消息，不是正式业务通告。',operation_id='test:'+operation)
                elif action=='retry':
                    data=await asyncio.to_thread(service.retry,str(body.get('id') or ''),body.get('confirm_uncertain') is True)
                else:raise DingTalkError('操作不存在。',404)
            return JSONResponse({'ok':True,'data':data},headers={'Cache-Control':'no-store'})
        except DingTalkError as error:
            return JSONResponse({'ok':False,'error':str(error)},status_code=error.status,headers={'Cache-Control':'no-store'})
        except Exception:
            return JSONResponse({'ok':False,'error':'钉钉配置或发送结果暂未确认，请刷新核对。'},status_code=503,headers={'Cache-Control':'no-store'})
    app.add_api_route('/api/dingtalk-notices',endpoint,methods=['GET','POST'])
    app.add_api_route('/api/dingtalk-notices/{action}',endpoint,methods=['POST'])

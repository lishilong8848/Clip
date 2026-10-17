"""Authenticated handover APIs sharing one store with Feishu confirmations."""
import asyncio
import logging
import os
import threading
import uuid
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse

from .work_handover import Handovers, HandoverError, MODULE, SCOPES, authorized, text, timestamp
from .work_handover_sources import HandoverSources, SOURCES, responsibility_modules

MAX_FILE_BYTES = 20 * 1024 * 1024
FILE_EXTS = {'.pdf','.doc','.docx','.xls','.xlsx','.xlsm','.md','.txt','.csv','.jpg','.jpeg','.png','.webp'}


def install_work_handover_routes(app, controller, runtime):
    store = None
    lock = threading.Lock()

    def actor_for_open_id(open_id, name='', *, check_access=True):
        auth = runtime.auth_manager
        profile = auth.account_profile(open_id)
        person = auth.directory_person(open_id)
        if person.get('inactive') or person and not person.get('selectable'):
            raise HandoverError('当前人员信息不可用，请管理员核对。',403)
        actor = {**profile, 'id':person.get('id') or 'account:'+open_id,
                 'open_id':open_id,'name':person.get('name') or name or '当前人员',
                 'position':person.get('position', '')}
        if check_access:
            Handovers.check_actor(actor)
        return actor

    def directory(*, force_refresh=False):
        return runtime.service.get_permission_directory_people(force_refresh=force_refresh).get('items',[])

    def person_for(record_id):
        row = next((r for r in directory() if r.get('record_id')==record_id and r.get('selectable') and not r.get('departed')),None)
        if not row or not row.get('open_id'):
            raise HandoverError('接收人已不在可选在岗人员目录中，请重新选择。',409)
        actor = actor_for_open_id(row['open_id'],row['name'])
        if actor['id'] != row['record_id']:
            raise HandoverError('接收人身份尚未同步，请重新读取人员名单后再试。', 409)
        return actor

    def manager():
        nonlocal store
        with lock:
            if store is None:
                store = Handovers(runtime.state_store.db_path,directory,person_for)
        return store

    runtime.work_handovers = manager
    runtime.work_handover_actor = actor_for_open_id

    def actor_for(request):
        session=controller._current_session(request)
        if not session:
            raise HandoverError('请先登录后使用人员工作交接。',401)
        from .cabinet_guest import is_cabinet_guest
        if is_cabinet_guest(session):
            raise HandoverError('访客不能使用人员工作交接。',403)
        user=runtime.auth_manager.public_user(session)
        return actor_for_open_id(str(user.get('open_id') or ''),str(user.get('name') or ''))

    def same_origin(request):
        source=request.headers.get('origin') or request.headers.get('referer')
        def origin(value):
            parsed=urlsplit(value)
            return parsed.scheme, parsed.hostname, parsed.port or (443 if parsed.scheme=='https' else 80)
        if request.headers.get('sec-fetch-site')=='cross-site' or source and origin(source)!=origin(controller._request_base_url(request)):
            raise HandoverError('不允许跨来源提交。',403)

    async def endpoint(request: Request):
        try:
            actor=await asyncio.to_thread(actor_for,request)
            svc=await asyncio.to_thread(manager)
            catalog=HandoverSources(runtime,controller,svc)
            path=request.url.path.removeprefix('/api/work-handovers').strip('/')
            body={}
            if request.method!='GET':
                same_origin(request)
                body=await controller._read_json_request(request,max_bytes=3*1024*1024)
                if not isinstance(body,dict):
                    raise HandoverError('提交内容无效。')
            identity=request.path_params.get('identity','')
            if path=='people':
                rows=await asyncio.to_thread(directory,force_refresh=request.query_params.get('refresh')=='1')
                items=[]
                unresolved = 0
                query = request.query_params.get('query', '').strip().casefold()
                for row in rows:
                    if not row.get('selectable') or row.get('departed') or not row.get('open_id'):
                        continue
                    try:
                        target=await asyncio.to_thread(actor_for_open_id,row['open_id'],row['name'],check_access=False)
                    except HandoverError:
                        continue
                    if target['id'] != row.get('record_id'):
                        unresolved += 1
                        continue
                    if target['id']==actor['id'] or not any(authorized(actor,s) and authorized(target,s) for s in SCOPES):
                        continue
                    if query and query not in ' '.join(str(row.get(k) or '') for k in ('name', 'employee_no', 'position')).casefold():
                        continue
                    eligible = MODULE in target.get('modules', [])
                    items.append({**{k:row.get(k,'') for k in ('record_id','name','employee_no','position')},
                                  'scopes':[s for s in SCOPES if authorized(actor,s) and authorized(target,s)],
                                  'selectable':eligible,
                                  'unavailable_reason':'' if eligible else '尚无人员工作交接权限，请联系管理员授权'})
                data={'items':items,'self':{k:actor.get(k, '') for k in ('id','name','category','level','home_scope')},
                      'scopes':[s for s in SCOPES if authorized(actor,s)],
                      'modules':[{'value':k,'label':v} for k,v in SOURCES.items() if k in actor['modules']],
                      'default_modules':responsibility_modules(actor),
                      'warnings':['部分人员身份尚未同步，请刷新人员名单后重试。'] if unresolved else []}
            elif path=='sources':
                query=request.query_params
                reader = catalog.defaults if query.get('mode') == 'responsibilities' else catalog.load
                args = () if query.get('mode') == 'responsibilities' else (query.get('module',''),)
                data=await asyncio.to_thread(reader,actor,*args,query.get('scope',''),query.get('period',''))
            elif not path:
                if request.method=='GET':
                    data=await asyncio.to_thread(svc.list,actor,request.query_params.get('view','received'),request.query_params.get('page',1))
                else:
                    if set(body)!={'id'}:
                        raise HandoverError('创建参数无效。')
                    data=await asyncio.to_thread(svc.create,actor,body['id'])
            elif path.endswith('/refresh'):
                current=await asyncio.to_thread(svc.get,actor,identity)
                warnings=[]
                groups={(i['module'],i['scope'],i['period']) for i in current['items'] if i.get('source_id') and not i.get('restricted')}
                for module,scope,period in sorted(groups):
                    try:
                        result=await asyncio.to_thread(catalog.load,actor,module,scope,period)
                        warnings.extend(result['warnings'])
                        present={i['id'] for i in result['items']}
                        missing=[{**i,'id':i['source_id'],'business_status':'原列表未找到，请核验','source_checked_at':timestamp()} for i in current['items']
                                 if (i.get('module'),i.get('scope'),i.get('period'))==(module,scope,period) and i.get('source_id') not in present]
                        await asyncio.to_thread(svc.cache_sources,missing)
                    except Exception:
                        warnings.append(SOURCES[module]+'状态暂未刷新，保留上次读取结果。')
                data=await asyncio.to_thread(svc.get,actor,identity)
                data['refresh_warnings']=list(dict.fromkeys(warnings))
            elif path.endswith('/actions'):
                if set(body)-{'version','action','reason','item_id'} or type(body.get('version')) is not int:
                    raise HandoverError('操作参数无效。')
                data=await asyncio.to_thread(svc.action,actor,identity,body['version'],body.get('action'),reason=body.get('reason',''),item_id=body.get('item_id',''))
            elif request.method=='PUT':
                if set(body)!={'version','data'} or type(body['version']) is not int or not isinstance(body['data'],dict):
                    raise HandoverError('保存参数无效。')
                data=await asyncio.to_thread(svc.save,actor,identity,body['version'],body['data'])
            else:
                data=await asyncio.to_thread(svc.get,actor,identity)
            return JSONResponse({'ok':True,'data':data},headers={'Cache-Control':'no-store'})
        except HandoverError as exc:
            return JSONResponse({'ok':False,'error':str(exc)},status_code=exc.status)
        except Exception:
            logging.exception('Work handover request failed')
            return JSONResponse({'ok':False,'error':'交接操作暂未完成，请重新读取核对；已保存内容保留。'},status_code=503)

    for path,methods in (('', ['GET','POST']),('/people',['GET']),('/sources',['GET']),
                         ('/{identity}',['GET','PUT']),('/{identity}/actions',['POST']),('/{identity}/refresh',['POST'])):
        app.add_api_route('/api/work-handovers'+path,endpoint,methods=methods)

    def file_owner(svc,actor,identity):
        data=svc.get(actor,identity)
        if not data['is_owner'] or data['status'] not in {'draft','returned','withdrawn'}:
            raise HandoverError('附件只能在草稿中由交接人修改。',409)
        return data

    @app.post('/api/work-handovers/{identity}/files')
    async def upload(request: Request,identity: str):
        stage=None
        final=None
        try:
            actor=await asyncio.to_thread(actor_for,request)
            same_origin(request)
            svc=await asyncio.to_thread(manager)
            data=await asyncio.to_thread(file_owner,svc,actor,identity)
            name=Path(text(request.query_params.get('name',''),240,True).replace('\\','/')).name
            if Path(name).suffix.lower() not in FILE_EXTS:
                raise HandoverError('仅支持照片、PDF、Word、Excel及文本资料。')
            folder=svc.db_path.parent/'work_handover_files'
            folder.mkdir(parents=True,exist_ok=True)
            fid=uuid.uuid4().hex
            stage=folder/(fid+'.part')
            final=folder/fid
            count=0
            with stage.open('wb') as fp:
                async for chunk in request.stream():
                    count+=len(chunk)
                    if count>MAX_FILE_BYTES:
                        raise HandoverError('单个交接附件不能超过20MB。',413)
                    await asyncio.to_thread(fp.write,chunk)
            if not count:
                raise HandoverError('不能上传空文件。')
            actor=await asyncio.to_thread(actor_for,request)
            def commit():
                with svc.connect(True) as db:
                    current=svc._read(db,identity)
                    if actor['id']!=current['owner']['id'] or current['status'] not in {'draft','returned','withdrawn'} or current['version']!=data['version']:
                        raise HandoverError('交接单已更新，请重新读取后上传。',409)
                    total=db.execute('SELECT COUNT(*),COALESCE(SUM(size),0) FROM work_handover_files WHERE handover_id=?',(identity,)).fetchone()
                    if total[0]>=50 or total[1]+count>100*1024*1024:
                        raise HandoverError('每份交接最多50个附件，合计100MB。',413)
                    os.replace(stage,final)
                    db.execute('INSERT INTO work_handover_files(id,handover_id,name,size,time) VALUES(?,?,?,?,?)',(fid,identity,name,count,timestamp()))
                    current['version']+=1
                    svc._save(db,current,actor,'添加交接附件',{'name':name})
                    return svc._public(db,current,actor)
            commit_task=asyncio.create_task(asyncio.to_thread(commit))
            try:
                result=await asyncio.shield(commit_task)
            except asyncio.CancelledError:
                # A disconnected browser must not delete bytes after their metadata commits.
                try:
                    await commit_task
                    final=None
                finally:
                    raise
            final=None
            return JSONResponse({'ok':True,'data':result})
        except HandoverError as exc:
            return JSONResponse({'ok':False,'error':str(exc)},status_code=exc.status)
        except Exception:
            logging.exception('Work handover attachment upload failed')
            return JSONResponse({'ok':False,'error':'附件未确认上传完成，请重新读取交接单核对后重试。'},status_code=503)
        finally:
            for path in (stage,final):
                if path:
                    try:
                        path.unlink(missing_ok=True)
                    except OSError:
                        logging.warning('Handover temporary attachment cleanup deferred')

    @app.get('/api/work-handovers/files/{file_id}')
    async def download(request: Request,file_id: str):
        try:
            actor=await asyncio.to_thread(actor_for,request)
            svc=await asyncio.to_thread(manager)
            with svc.connect() as db:
                row=db.execute('SELECT * FROM work_handover_files WHERE id=?',(file_id,)).fetchone()
            if not row:
                raise HandoverError('附件不存在。',404)
            data=await asyncio.to_thread(svc.get,actor,row['handover_id'])
            if any(i.get('restricted') for i in data['items']):
                raise HandoverError('当前业务权限不足，不能读取交接附件。',403)
            if not any(file['id']==file_id for file in data['files']):
                raise HandoverError('该附件不属于当前可访问的交接版本。',403)
            path=svc.db_path.parent/'work_handover_files'/row['id']
            if not path.is_file():
                raise HandoverError('附件文件不可用，请联系交接人补充。',404)
            return FileResponse(path,filename=row['name'],media_type='application/octet-stream',headers={'Cache-Control':'no-store','X-Content-Type-Options':'nosniff'})
        except HandoverError as exc:
            return JSONResponse({'ok':False,'error':str(exc)},status_code=exc.status)

    @app.delete('/api/work-handovers/files/{file_id}')
    async def remove_file(request: Request,file_id: str):
        try:
            actor=await asyncio.to_thread(actor_for,request)
            same_origin(request)
            body=await controller._read_json_request(request,max_bytes=4096)
            if not isinstance(body,dict) or set(body)!={'version'} or type(body['version']) is not int:
                raise HandoverError('附件操作参数无效。')
            svc=await asyncio.to_thread(manager)
            def remove():
                with svc.connect(True) as db:
                    row=db.execute('SELECT * FROM work_handover_files WHERE id=?',(file_id,)).fetchone()
                    if not row:
                        raise HandoverError('附件不存在。',404)
                    data=svc._read(db,row['handover_id'])
                    svc._access(data,actor)
                    if data['owner']['id']!=actor['id'] or data['status'] not in {'draft','returned','withdrawn'}:
                        raise HandoverError('只能从本人交接草稿移除附件。',403)
                    if data['version']!=body['version']:
                        raise HandoverError('交接单已更新，请重新读取后操作。',409)
                    # Keep bytes referenced by previously sent versions.
                    db.execute('UPDATE work_handover_files SET visible=0 WHERE id=?',(file_id,))
                    data['version']+=1
                    svc._save(db,data,actor,'移除交接附件',{'name':row['name']})
                    return svc._public(db,data,actor)
            return JSONResponse({'ok':True,'data':await asyncio.to_thread(remove)},headers={'Cache-Control':'no-store'})
        except HandoverError as exc:
            return JSONResponse({'ok':False,'error':str(exc)},status_code=exc.status)

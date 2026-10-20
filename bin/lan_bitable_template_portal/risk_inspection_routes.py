"""Risk inspection HTTP entry points; no new login or Feishu connection."""
import asyncio
import logging
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlsplit
import uuid

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse

from .risk_inspection import RiskError, RiskInspections, SCOPES, can_scope, final_manager, manager, text
from .risk_inspection_cloud import RiskCloud

MAX_FILE = 20 * 1024 * 1024
EXTENSIONS = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.xlsm', '.md', '.txt', '.csv', '.jpg', '.jpeg', '.png', '.webp'}


def risk_task_access(runtime, session, rid=''):
    from .cabinet_guest import is_cabinet_guest
    if not session or is_cabinet_guest(session):
        return False
    actor_for = getattr(runtime, 'risk_inspection_actor', None)
    service = getattr(runtime, 'risk_inspections', None)
    if not callable(actor_for) or not callable(service):
        return False
    try:
        user = runtime.auth_manager.public_user(session)
        actor = actor_for(str(user.get('open_id') or ''), str(user.get('name') or ''))
        return service().has_task_access(actor, rid)
    except RiskError:
        return False
    except Exception:
        logging.exception('Risk inspection task access could not be verified')
        return False


def install_risk_inspection_routes(app, controller, runtime):
    service = cloud = None
    lock, stop = threading.Lock(), threading.Event()
    worker = None

    def actor_for(open_id, name=''):
        if not open_id:
            raise RiskError('请先登录。', 401)
        auth = runtime.auth_manager
        person = auth.directory_person(open_id)
        if person.get('inactive') or person and not person.get('selectable'):
            raise RiskError('当前人员已离职或身份不可用。', 403)
        disabled = getattr(auth, '_open_id_explicitly_disabled', None)
        if callable(disabled) and disabled(open_id):
            raise RiskError('当前账号已停用，不能处理排查任务。', 403)
        profile = auth.account_profile(open_id)
        actor = {**profile, 'id': person.get('id') or 'account:' + open_id,
                 'open_id': open_id, 'name': person.get('name') or name or '当前人员'}
        RiskInspections.check(actor)
        return actor

    def directory():
        return runtime.service.get_permission_directory_people().get('items', [])

    def person_for(identity):
        if identity.startswith('account:'):
            return actor_for(identity.removeprefix('account:'))
        row = next((r for r in directory() if r.get('record_id') == identity and r.get('selectable') and not r.get('departed')), None)
        if not row or not row.get('open_id'):
            raise RiskError('人员已不在可用目录中，请管理员核对。', 409)
        actor = actor_for(row['open_id'], row['name'])
        if actor['id'] != identity:
            raise RiskError('人员身份关系已变化，已停止操作以避免串人。', 409)
        return actor

    def recipient(actor):
        from .permission_review_cards import PermissionCards
        return PermissionCards(SimpleNamespace(runtime=runtime)).union_id(actor['open_id'])

    def get_services():
        nonlocal service, cloud
        with lock:
            if service is None:
                service = RiskInspections(runtime.state_store.db_path, person_for)
                cloud = RiskCloud(service, service.db_path.parent / 'feishu_assistant.json', recipient,
                                  runtime.service._critical_guard_public_base_url)
        return service, cloud

    runtime.risk_inspections = lambda: get_services()[0]
    runtime.risk_inspection_actor = actor_for

    def read_actor(request):
        session = controller._current_session(request)
        if not session:
            raise RiskError('请先登录后使用风险排查。', 401)
        from .cabinet_guest import is_cabinet_guest
        if is_cabinet_guest(session):
            raise RiskError('访客不能使用风险排查。', 403)
        user = runtime.auth_manager.public_user(session)
        return actor_for(str(user.get('open_id') or ''), str(user.get('name') or ''))

    def same_origin(request):
        source = request.headers.get('origin') or request.headers.get('referer')
        def origin(value):
            p = urlsplit(value)
            return p.scheme, p.hostname, p.port or (443 if p.scheme == 'https' else 80)
        if request.headers.get('sec-fetch-site') == 'cross-site' or source and origin(source) != origin(controller._request_base_url(request)):
            raise RiskError('不允许跨来源提交。', 403)

    def response(data=None, error=None):
        return JSONResponse({'ok': error is None, **({'data': data} if error is None else {'error': str(error)})},
                            status_code=error.status if error else 200, headers={'Cache-Control': 'no-store'})

    async def endpoint(request: Request):
        try:
            actor = await asyncio.to_thread(read_actor, request)
            svc, remote = await asyncio.to_thread(get_services)
            path = request.url.path.removeprefix('/api/risk-inspections').strip('/')
            body = {}
            if path == 'base-templates/import-file' and request.method == 'POST':
                same_origin(request)
                if not manager(actor):
                    raise RiskError('仅管理员或楼栋主管可导入基础模板。', 403)
                from .risk_inspection_imports import preview_file
                content, size = [], 0
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > MAX_FILE:
                        raise RiskError('导入表格不能超过20MB。', 413)
                    content.append(chunk)
                data = await asyncio.to_thread(preview_file, request.query_params.get('name', ''), b''.join(content),
                                               sheet=request.query_params.get('sheet'), title_field=request.query_params.get('title_field'))
                return response(data)
            if request.method != 'GET':
                same_origin(request)
                body = await controller._read_json_request(request, max_bytes=2 * 1024 * 1024)
                if not isinstance(body, dict):
                    raise RiskError('请求内容无效。')
            rid = request.path_params.get('rid', '')
            if path == 'base-templates/import-bitable':
                if not manager(actor):
                    raise RiskError('仅管理员或楼栋主管可导入基础模板。', 403)
                if set(body) - {'url', 'title_field'} or not isinstance(body.get('url'), str):
                    raise RiskError('请填写需要导入的多维表链接。')
                from .risk_inspection_imports import preview_bitable
                data = await asyncio.to_thread(preview_bitable, remote, body['url'], title_field=body.get('title_field'), actor=actor)
            elif path == 'base-templates':
                if not manager(actor):
                    raise RiskError('仅管理员或楼栋主管可使用基础模板。', 403)
                if request.method == 'POST':
                    data = await asyncio.to_thread(svc.history.add_base, actor, body)
                else:
                    if await asyncio.to_thread(svc.history.needs_base, actor):
                        source = await asyncio.to_thread(remote.read_template)
                        await asyncio.to_thread(svc.history.seed_base, actor, source)
                    data = await asyncio.to_thread(svc.history.list, actor, request.query_params.get('page', 1), request.query_params.get('search', ''), 'base')
            elif path.startswith('base-templates/'):
                tid = request.path_params['tid']
                await asyncio.to_thread(svc.history.get, actor, tid, kind='base')
                if path.endswith('/retry'):
                    if body:
                        raise RiskError('重试参数无效。')
                    data = await asyncio.to_thread(svc.history.retry, actor, tid)
                elif request.method == 'PUT':
                    data = await asyncio.to_thread(svc.history.update, actor, tid, body)
                else:
                    data = await asyncio.to_thread(svc.history.get, actor, tid, kind='base')
            elif path == 'templates':
                data = await asyncio.to_thread(svc.history.list, actor, request.query_params.get('page', 1), request.query_params.get('search', ''))
            elif path.startswith('templates/'):
                tid = request.path_params['tid']
                if request.method == 'PUT':
                    await asyncio.to_thread(svc.history.get, actor, tid, kind='history')
                    data = await asyncio.to_thread(svc.history.update, actor, tid, body)
                else:
                    data = await asyncio.to_thread(svc.history.get, actor, tid, kind='history')
            elif path == 'people':
                if not manager(actor):
                    raise RiskError('仅发布人可查看可分配人员。', 403)
                def people():
                    result = []
                    for row in directory():
                        if not row.get('selectable') or row.get('departed') or not row.get('open_id'):
                            continue
                        try:
                            person = person_for(row['record_id'])
                        except RiskError:
                            continue
                        scopes = [s for s in SCOPES if can_scope(actor, s)]
                        result.append({'id': person['id'], 'name': person['name'], 'employee_no': row.get('employee_no', ''),
                                       'scopes': scopes, 'home_scope': person.get('home_scope', ''), 'can_review': True,
                                       'final_scopes': [s for s in scopes if can_scope(person, s)],
                                       'category': person.get('category', ''), 'can_receive_final': final_manager(person)})
                    if not any(p['id'] == actor['id'] for p in result):
                        result.insert(0, {'id': actor['id'], 'name': actor['name'], 'employee_no': '',
                                         'scopes': [s for s in SCOPES if can_scope(actor, s)],
                                         'home_scope': actor.get('home_scope', ''), 'can_review': True,
                                         'final_scopes': [s for s in SCOPES if can_scope(actor, s)],
                                         'category': actor.get('category', ''), 'can_receive_final': final_manager(actor)})
                    return {'items': result, 'self_id': actor['id'], 'scopes': [s for s in SCOPES if can_scope(actor, s)]}
                data = await asyncio.to_thread(people)
            elif path == 'template':
                if not manager(actor):
                    raise RiskError('仅发布人可读取发布模板。', 403)
                data = await asyncio.to_thread(remote.edit_field, actor, body) if request.method == 'POST' else await asyncio.to_thread(remote.read_template)
            elif not path:
                if request.method == 'GET':
                    data = await asyncio.to_thread(svc.list, actor, request.query_params.get('page', 1), request.query_params.get('scope', ''), request.query_params.get('view', 'all'))
                else:
                    if not manager(actor):
                        raise RiskError('无权发布风险排查。', 403)
                    with svc.connect() as db:
                        exists = isinstance(body.get('id'), str) and db.execute('SELECT 1 FROM risk_releases WHERE id=?', (body['id'],)).fetchone()
                    template = None if exists or body.get('history_template_id') or body.get('base_template_id') else await asyncio.to_thread(remote.read_template)
                    data = await asyncio.to_thread(svc.publish, actor, body, template)
            elif path.endswith('/retry'):
                if set(body) - {'kind'} or body.get('kind') not in (None, 'template_sync'):
                    raise RiskError('重试范围无效。')
                data = await asyncio.to_thread(svc.retry, actor, rid, template_only=body.get('kind') == 'template_sync')
            elif path.endswith('/reviewer'):
                data = await asyncio.to_thread(svc.change_reviewer, actor, rid, body)
            elif path.endswith('/claim'):
                data = await asyncio.to_thread(svc.claim, actor, rid, body)
            elif path.endswith('/actions'):
                data = await asyncio.to_thread(svc.action, actor, rid, request.path_params['pid'], body)
            elif request.method == 'DELETE':
                data = await asyncio.to_thread(svc.delete, actor, rid, body)
            else:
                data = await asyncio.to_thread(svc.get, actor, rid)
            return response(data)
        except RiskError as exc:
            return response(error=exc)
        except Exception:
            logging.exception('Risk inspection request failed')
            return response(error=RiskError('排查操作暂未确认完成；已保存内容保留，请重新读取核对。', 503))

    for path, methods in (('', ['GET', 'POST']), ('/people', ['GET']), ('/template', ['GET', 'POST']), ('/templates', ['GET']), ('/templates/{tid}', ['GET', 'PUT']),
                          ('/base-templates/import-file', ['POST']), ('/base-templates/import-bitable', ['POST']),
                          ('/base-templates', ['GET', 'POST']), ('/base-templates/{tid}', ['GET', 'PUT']), ('/base-templates/{tid}/retry', ['POST']),
                          ('/{rid}', ['GET', 'DELETE']), ('/{rid}/retry', ['POST']), ('/{rid}/reviewer', ['POST']), ('/{rid}/claim', ['POST']), ('/{rid}/people/{pid}/actions', ['POST'])):
        app.add_api_route('/api/risk-inspections' + path, endpoint, methods=methods)

    def editable(svc, db, actor, rid, item_id, field_id):
        release = RiskInspections._release(db, rid)
        person = RiskInspections._person(db, rid, actor['id'])
        item = next((i for i in person['items'] if i['id'] == item_id), None)
        field = next((f for f in release['fields'] if f['key'] == field_id), None)
        if release['status'] != 'active' or person['revoked'] or not item or not svc._visible(actor, release, person, item=item) or item['status'] not in {'pending', 'returned'}:
            raise RiskError('当前事项不能上传附件。', 409)
        if not field or field['type'] != 17 or field['role'] != 'input':
            raise RiskError('附件字段无效。')

    @app.post('/api/risk-inspections/{rid}/files')
    async def upload(request: Request, rid: str):
        stage = final = None
        try:
            actor = await asyncio.to_thread(read_actor, request)
            same_origin(request)
            svc, _ = await asyncio.to_thread(get_services)
            item_id, field_id = request.query_params.get('item_id', ''), request.query_params.get('field_id', '')
            with svc.connect() as db:
                editable(svc, db, actor, rid, item_id, field_id)
            name = Path(text(request.query_params.get('name', ''), 240, True).replace('\\', '/')).name
            if Path(name).suffix.lower() not in EXTENSIONS:
                raise RiskError('请选择照片、PDF、Word、Excel或文本文件。')
            folder = svc.db_path.parent / 'risk_inspection_files'
            folder.mkdir(parents=True, exist_ok=True)
            fid = uuid.uuid4().hex
            stage, final = folder / (fid + '.part'), folder / fid
            size = 0
            with stage.open('wb') as stream:
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > MAX_FILE:
                        raise RiskError('单个排查附件不能超过20MB。', 413)
                    await asyncio.to_thread(stream.write, chunk)
            if not size:
                raise RiskError('不能上传空文件。')
            actor = await asyncio.to_thread(read_actor, request)
            def commit():
                with svc.connect(True) as db:
                    editable(svc, db, actor, rid, item_id, field_id)
                    total = db.execute('SELECT COUNT(*),COALESCE(SUM(size),0) FROM risk_files WHERE release_id=? AND person_id=?', (rid, actor['id'])).fetchone()
                    if total[0] >= 100 or total[1] + size > 200 * 1024 * 1024:
                        raise RiskError('每人每次排查最多100个附件，合计200MB。', 413)
                    os.replace(stage, final)
                    db.execute('INSERT INTO risk_files VALUES(?,?,?,?,?,?,?,?)', (fid, rid, actor['id'], item_id, field_id, name, size, time.time()))
            task = asyncio.create_task(asyncio.to_thread(commit))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                final = None
                raise
            final = None
            return response({'id': fid, 'name': name, 'size': size, 'field_id': field_id})
        except RiskError as exc:
            return response(error=exc)
        except Exception:
            logging.exception('Risk inspection upload failed')
            return response(error=RiskError('附件上传未确认完成，请重新读取后核对。', 503))
        finally:
            for path in (stage, final):
                if path:
                    path.unlink(missing_ok=True)

    @app.get('/api/risk-inspections/files/{fid}')
    async def download(request: Request, fid: str):
        try:
            actor = await asyncio.to_thread(read_actor, request)
            svc, _ = await asyncio.to_thread(get_services)
            with svc.connect() as db:
                row = db.execute('SELECT * FROM risk_files WHERE id=?', (fid,)).fetchone()
                if not row:
                    raise RiskError('附件不存在。', 404)
                release, person = svc._release(db, row['release_id']), svc._person(db, row['release_id'], row['person_id'])
                item = next((i for i in person['items'] if i['id'] == row['item_id']), None)
                if not item or not svc._visible(actor, release, person, item=item):
                    raise RiskError('无权读取该人员附件。', 403)
            return FileResponse(svc.db_path.parent / 'risk_inspection_files' / row['id'], filename=row['name'],
                                media_type='application/octet-stream', headers={'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff'})
        except RiskError as exc:
            return response(error=exc)

    def run():
        while not stop.is_set():
            job = None
            try:
                if not getattr(runtime, 'state_store', None) or not getattr(runtime, 'service', None):
                    stop.wait(5)
                    continue
                svc, remote = get_services()
                job = svc.claim_job()
                if job:
                    remote.process(job)
                    svc.finish_job(job)
            except Exception as exc:
                logging.exception('Risk inspection background operation deferred')
                if job:
                    try:
                        svc.finish_job(job, str(exc) if isinstance(exc, RiskError) else '云端操作未确认完成，数据保留等待重试。')
                    except Exception:
                        logging.exception('Risk inspection receipt unavailable; persisted lease will recover the job')
            stop.wait(.25 if job else 5)

    def start_worker():
        nonlocal worker
        if worker is None:
            worker = threading.Thread(target=run, name='risk-inspection-sync', daemon=True)
            worker.start()

    async def stop_worker():
        stop.set()
        if worker:
            await asyncio.to_thread(worker.join, 40)
        if cloud and (not worker or not worker.is_alive()):
            cloud.close()

    app.add_event_handler('startup', start_worker)
    app.add_event_handler('shutdown', stop_worker)

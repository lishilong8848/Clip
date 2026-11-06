"""Cold-source status and durable, per-recipient notification processing."""
import asyncio
import hashlib
import json
import logging
import math
import re
import time
import uuid
from contextlib import closing
from datetime import datetime, timezone, timedelta

from fastapi import Request
from fastapi.responses import JSONResponse

from .eop_scenario_editor import plain
from .module_access import allowed_modules
from .notice_recipients import notification_directory
from .risk_inspection import RiskError
from .risk_inspection_cloud import RiskCloud

BASE = 'Ku17b5rmgaULYJsioBpcHl0Xnqd'
TABLE = 'tblm3MOOxKCW3ZPd'
VIEW = 'vewllyzxWv'
NS = 'cold_source_monitor'
TZ = timezone(timedelta(hours=8))
LOG = logging.getLogger(__name__)


def snapshot(cloud):
    path = cloud.table_path(BASE, TABLE) + '/records/search'
    records, ids, cursors, cursor, total = [], set(), set(), '', None
    for _ in range(100):
        params = {'page_size': 100, **({'page_token': cursor} if cursor else {})}
        data = cloud.request('POST', path, params=params, json={'view_id': VIEW})
        count=data.get('total')
        if (not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool
                or type(count) is not int or total is not None and total != count):
            raise RiskError('冷源数据读取不完整，请稍后重试。', 503)
        total=count
        for row in data['items']:
            rid, f = row.get('record_id'), row.get('fields')
            if not isinstance(rid, str) or rid in ids or not isinstance(f, dict):
                raise RiskError('冷源记录不完整，请稍后重试。', 503)
            ids.add(rid)
            scope = plain(f.get('楼栋')).strip().removesuffix('楼')
            temperature = f.get('天气湿球温度')
            if isinstance(temperature, bool) or not isinstance(temperature, (int, float)) or not math.isfinite(temperature):
                temperature = None
            timestamp = f.get('日期')
            date = datetime.fromtimestamp(timestamp / 1000, TZ).strftime('%Y-%m-%d %H:%M') if isinstance(timestamp, (int, float)) and 0 < timestamp < 253402272000000 else ''
            records.append({'id': rid, 'scope': scope, 'date': date, 'temperature': temperature,
                            'mode': plain(f.get('冷源运行模式')).strip(),
                            'hint': plain(f.get('是否需要提示')).strip()})
        if not data['has_more'] and len(records)==total:
            return records
        if not data['has_more']:
            break
        cursor = data.get('page_token')
        if not isinstance(cursor, str) or not cursor or cursor in cursors:
            break
        cursors.add(cursor)
    raise RiskError('冷源记录分页未完成，请稍后重试。', 503)


def actionable(row):
    # The Feishu formula returns the warning itself, or an empty string.
    hint=row['hint'].strip()
    return (row['scope'] in {'110','A','B','C','D','E','H'} and row['temperature'] is not None
            and bool(row['mode']) and bool(hint) and hint not in {'否','不需要','无需提示'})


def card(row):
    label = '110站' if row['scope'] == '110' else row['scope'] + '楼'
    return {'config': {'wide_screen_mode': True},
            'header': {'template': 'blue', 'title': {'tag': 'plain_text', 'content': '湿球温度及模式切换提示'}},
            'elements': [{'tag': 'div', 'text': {'tag': 'plain_text', 'content':
                f"【当前涉及楼栋】：{label}\n【当前湿球温度】：{row['temperature']}℃\n【模式切换提示】：{row['hint']}"}}]}


def recipients(auth, scope):
    from .portal_service import BUILDING_OPEN_ID_MAP
    from .account_policy import categorize_position
    from upload_event_module.services.robot_webhook import current_app_credentials
    duty = BUILDING_OPEN_ID_MAP.get(scope)
    app_id = current_app_credentials()[0]
    if not duty or not app_id:
        raise ValueError('楼栋值班账号或飞书应用尚未配置')
    found = {duty: {'id': duty, 'name': scope + '楼值班账号'}}
    missing=False
    try:
        with notification_directory(auth) as (directory_app, people):
            if directory_app != app_id:
                raise ValueError('人员目录与当前飞书应用不一致')
            for person in people:
                if person.get('inactive') or person.get('selectable') is not True:
                    continue
                ids = person.get('login_ids') or []
                if len(ids) != 1:
                    if scope in (person.get('scopes') or []) and (categorize_position(person.get('position')) == 'supervisor' or '楼长' in str(person.get('position') or '')):
                        missing=True
                    continue
                identity = auth.current_feishu_open_id(ids[0], app_id)
                if not identity:
                    if scope in (person.get('scopes') or []) and categorize_position(person.get('position')) == 'supervisor': missing=True
                    continue
                profile = auth.account_profile(identity)
                if (profile.get('enabled') and profile.get('home_scope') == scope and
                    (profile.get('category') == 'supervisor' or profile.get('level') == 'L3'
                     or '楼长' in str(person.get('position') or ''))):
                    found[identity] = {'id': identity, 'name': str(person.get('name') or '楼长')}
    except Exception:
        # A directory outage must not block the fixed duty account.
        found={duty:found[duty]}; missing=True
    return list(found.values()), missing or len(found) < 2


def _load(conn, key):
    row = conn.execute('SELECT payload_json FROM json_documents WHERE namespace=? AND key=?',(NS,key)).fetchone()
    return json.loads(row['payload_json']) if row else None


def _save(conn, key, value):
    conn.execute('INSERT OR REPLACE INTO json_documents(namespace,key,payload_json,updated_at) VALUES(?,?,?,?)',
                 (NS,key,json.dumps(value,ensure_ascii=False),time.time()))


def observe(store, rows):
    # First successful read establishes the baseline. No old alerts are replayed.
    with store._lock, closing(store._connect()) as conn:
        store._ensure_schema_locked(conn); conn.execute('BEGIN IMMEDIATE')
        try:
            meta = _load(conn,'meta') or {}
            initial = not meta.get('ready')
            for row in rows:
                digest = hashlib.sha256(json.dumps(row,ensure_ascii=False,sort_keys=True).encode()).hexdigest()
                old = _load(conn,'seen:'+row['id'])
                if not initial and (not old or old.get('digest') != digest) and actionable(row):
                    key = 'job:' + uuid.uuid4().hex
                    _save(conn,key,{'id':key,'row':row,'created_at':time.time(),'status':'pending', 'targets':{}})
                _save(conn,'seen:'+row['id'],{'digest':digest})
            _save(conn,'meta',{'ready':True,'updated_at':time.time()})
            conn.commit()
        except BaseException:
            conn.rollback(); raise


def claim(store, key):
    with store._lock, closing(store._connect()) as conn:
        store._ensure_schema_locked(conn); conn.execute('BEGIN IMMEDIATE')
        job = _load(conn,key)
        if not job or job.get('status') in ('sent','superseded') or job.get('lease_until',0) > time.time():
            conn.commit(); return None
        job['lease'] = uuid.uuid4().hex; job['lease_until'] = time.time()+180
        _save(conn,key,job); conn.commit(); return job


def save_claim(store, job, *, release=False):
    with store._lock, closing(store._connect()) as conn:
        store._ensure_schema_locked(conn); conn.execute('BEGIN IMMEDIATE')
        current = _load(conn,job['id'])
        if not current or current.get('lease') != job.get('lease'):
            conn.rollback(); raise RuntimeError('冷源提醒任务已由其他进程接管')
        job['lease_until'] = 0 if release else time.time()+180
        _save(conn,job['id'],job); conn.commit()


def process(store, cloud, auth, send, *, stopping=lambda: False):
    from .daily_inspection_reminders import saved_receipt, delivery_route
    from .feishu_delivery import current_recipient_open_id
    from types import SimpleNamespace
    from upload_event_module.services.robot_webhook import current_app_credentials
    rows = snapshot(cloud)
    observe(store, rows)
    current_rows={row['id']:row for row in rows}
    for item in store.list_documents(NS,key_prefix='job:'):
        if stopping(): return
        job = claim(store,item['key'])
        if not job: continue
        try:
            if current_rows.get(job['row']['id']) != job['row'] or not actionable(job['row']):
                job.update(status='superseded',error='来源数据已变化，本次未完成的提醒不再发送')
                continue
            current, missing = recipients(auth,job['row']['scope'])
            targets=job.setdefault('targets',{})
            for person in current:
                targets.setdefault(person['id'],{**person,'status':'pending'})
            current_ids={person['id'] for person in current}
            save_claim(store,job)
            for identity,target in job['targets'].items():
                if target.get('status') in ('sent','uncertain'): continue
                if identity not in current_ids:
                    target.update(status='uncertain',error='接收人身份或岗位已变化，等待核对'); save_claim(store,job); continue
                mid = target.setdefault('uuid',str(uuid.uuid5(uuid.NAMESPACE_URL,job['id']+':'+identity)))
                payload={'kind':'card','card':card(job['row'])}
                previous=saved_receipt(store,identity,mid,payload)
                if previous:
                    target.update(status='sent',receipt=previous); save_claim(store,job); continue
                route=delivery_route(store,identity,mid,payload)
                since=target.get('attempted_at')
                if since and time.time()-since >= 3000 and route.get('status') != 'not_sent':
                    target['status']='uncertain'; save_claim(store,job); continue
                try:
                    current_recipient_open_id(SimpleNamespace(state_store=store,auth_manager=auth),identity,current_app_credentials()[0])
                    target.setdefault('attempted_at',time.time()); save_claim(store,job)
                    ok,message,receipts=send(payload['card'],[identity],message_uuid=mid)
                    receipt=next((x for x in receipts if x.get('open_id')==identity),{})
                except Exception as error:
                    target.update(status='pending',error=str(error)[:160]); save_claim(store,job); continue
                if (ok and receipt.get('ok') is True and receipt.get('message_uuid') == mid
                        and receipt.get('sender_app_id') == current_app_credentials()[0]
                        and re.fullmatch(r'om_[A-Za-z0-9]+',str(receipt.get('message_id') or ''))
                        and re.fullmatch(r'oc_[A-Za-z0-9]+',str(receipt.get('chat_id') or ''))):
                    target.update(status='sent',receipt=receipt, delivered_at=time.time())
                else:
                    target.update(status='pending',error=str(message or '发送尚未确认')[:160])
                    if receipt.get('failure_kind') in {'not_sent','bot_unavailable','blocked','agent_required'}:
                        target.pop('attempted_at',None)
                save_claim(store,job)
            states=[x.get('status') for x in job['targets'].values()]
            job['status']='sent' if states and not missing and all(x=='sent' for x in states) else 'uncertain' if 'uncertain' in states else 'pending'
            job['error']='本楼楼长身份尚未核对，等待重试' if missing else ''
        except Exception as error:
            job['error']=str(error)[:160]; LOG.warning('冷源提醒暂未完成: %s',type(error).__name__)
        finally:
            save_claim(store,job,release=True)


def install_cold_source_monitor(app, controller, runtime):
    cloud = RiskCloud(None,runtime.state_store.db_path.parent/'feishu_assistant.json',None,None)
    async def endpoint(request: Request):
        try:
            session=await asyncio.to_thread(controller._current_session,request)
            if not session: return controller._auth_required_response()
            granted=await asyncio.to_thread(allowed_modules,runtime.auth_manager,session)
            if 'daily' not in granted: raise RiskError('需要每日任务权限。',403)
            scopes=await asyncio.to_thread(runtime.auth_manager.session_scopes,session)
            allowed=lambda scope: 'ALL' in scopes or scope in scopes
            rows=await asyncio.to_thread(snapshot,cloud)
            history=await asyncio.to_thread(runtime.state_store.list_documents,NS,key_prefix='job:')
            jobs=[d['payload'] for d in history if isinstance(d.get('payload'),dict) and allowed((d['payload'].get('row') or {}).get('scope'))]
            jobs.sort(key=lambda x:x.get('created_at',0),reverse=True)
            try:
                page=max(1,int(request.query_params.get('page','1')))
            except ValueError:
                raise RiskError('记录页码无效。',400) from None
            page=min(page,max(1,(len(jobs)+9)//10))
            return JSONResponse({'ok':True,'data':{'items':[r for r in rows if allowed(r['scope'])],
                                                  'history':jobs[(page-1)*10:page*10],
                                                  'history_total':len(jobs),'history_page':page}},headers={'Cache-Control':'private, no-store'})
        except RiskError as error:
            return JSONResponse({'ok':False,'error':str(error)},status_code=error.status)
        except Exception:
            LOG.exception('冷源运行状态读取失败')
            return JSONResponse({'ok':False,'error':'冷源数据暂时无法读取，请稍后重试。'},status_code=503)
    app.add_api_route('/api/cold-source-monitor',endpoint,methods=['GET'])

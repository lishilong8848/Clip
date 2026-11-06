"""Event text delivery and optional Bitable metadata are separate durable jobs."""
import copy
import hashlib
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from .portal_service import PortalError,external_real_write_guard
from .dingtalk_notices import DingTalkNotices,DingTalkError
from upload_event_module.config import EVENT_NOTICE_FIELDS,EVENT_LEVEL_OPTIONS,EVENT_SOURCE_OPTIONS,SPECIALTY_OPTIONS,config

NS='web_event_notices'
OPS='web_event_deliveries'
PREFS='web_event_defaults'
CHANNEL='web_event_messages'
FIELD_NAMES={'summary':'告警描述','building':'楼栋','specialty':'专业','level':'事件等级','event_source':'事件发现来源','occurrence_time':'事件发生时间'}
EDITABLE=set(FIELD_NAMES)|{'transfer_to_overhaul','recover_time'}
SCOPES=('110','A','B','C','D','E','H')


def missing(metadata):
    return [label for key,label in FIELD_NAMES.items() if not metadata.get(key)]


def feishu_destination(text):
    from upload_event_module.services.handlers.event_notice import EventNoticeHandler
    from upload_event_module.core.parser import extract_event_info
    # Reuse Qt's routing only. Never map pasted text to editable Bitable fields.
    handler=EventNoticeHandler()
    info=extract_event_info(text) or {}
    heading=handler._extract_section(text,'标题') or handler._extract_section(text,'名称') or str(info.get('title') or '') or (text.splitlines() or [''])[0]
    explicit=handler._extract_section(text,'事件等级') or handler._extract_section(text,'等级')
    level=handler._normalize_event_level(explicit or str(info.get('level') or ''),heading)
    if handler._route_event_group_level(level)!='I3':return None
    return {'id':'i3','name':str(config.group_name_event_i3 or '')}


class EventNoticeDelivery:
    def __init__(self,runtime):
        self.runtime=runtime;self.store=runtime.state_store
        self.ding=DingTalkNotices(self.store)

    @contextmanager
    def lock(self,key,ttl=120):
        owner=uuid.uuid4().hex;name='web-event:'+key
        deadline=time.monotonic()+3
        while True:
            ok,_=self.store.acquire_notice_operation_lock(name,owner=owner,action='event_web',ttl_seconds=ttl)
            if ok:break
            if time.monotonic()>=deadline:raise PortalError('该事件正在保存，请稍后重试原操作。')
            time.sleep(.02)
        try:yield
        finally:self.store.release_notice_operation_lock(name,owner)

    def get(self,event_id):return self.store.get_document(NS,event_id)

    def preference(self,open_id):return self.store.get_document(PREFS,open_id) or {'robot_ids':[],'revision':0}

    def save_preference(self,open_id,ids,expected_revision):
        if not isinstance(ids,list) or len(ids)>20 or any(not isinstance(x,str) for x in ids):raise PortalError('默认机器人选择无效。')
        enabled={r['id'] for r in self.ding.config()['robots'] if r['enabled']}
        if not set(ids)<=enabled:raise PortalError('所选机器人已删除或停用，请刷新。')
        with self.lock('defaults:'+open_id):
            previous=self.preference(open_id)
            if previous['revision']!=expected_revision:raise PortalError('默认设置已在其他页面修改，请刷新后核对。')
            result={'robot_ids':list(dict.fromkeys(ids)),'revision':previous['revision']+1}
            self.store.put_document(PREFS,open_id,result)
            return result

    @staticmethod
    def new(event_id,actor,scopes):
        return {'event_id':event_id,'created_by':actor,'access_scopes':list(scopes),'created_at':time.time(),
                'state':'draft','metadata':{},'metadata_revision':0,'sync_revision':0,'sync_state':'waiting',
                'operations':[],'sequence':0,'lifecycle_sequence':0,'target_record_id':'','text':''}

    def save_metadata(self,event_id,actor,scopes,patch,base):
        if not isinstance(patch,dict) or not isinstance(base,dict):raise PortalError('事件资料格式无效。')
        if set(patch)-EDITABLE:raise PortalError('包含不可编辑的事件资料字段。')
        normalized={};invalid={}
        for key,value in patch.items():
            if key=='transfer_to_overhaul':
                if type(value) is not bool:invalid[key]='请选择是或否';continue
            else:
                value=str(value or '').strip()
                if len(value)>5000:invalid[key]='内容过长';continue
                if key=='level' and value and value not in EVENT_LEVEL_OPTIONS:invalid[key]='等级无效';continue
                if key=='specialty' and value and value not in SPECIALTY_OPTIONS:invalid[key]='专业无效';continue
                if key=='event_source' and value and value not in EVENT_SOURCE_OPTIONS:invalid[key]='来源无效';continue
                if key=='building' and value and value not in scopes:invalid[key]='无权填写该楼栋';continue
                if key in ('occurrence_time','recover_time') and value:
                    value=self.runtime.service._format_input_datetime(value)
                    if not value or not self.runtime.service._repair_management_datetime_ms(value):invalid[key]='时间无效';continue
            normalized[key]=value
        with self.lock('event:'+event_id):
            row=self.get(event_id) or self.new(event_id,actor,scopes)
            if set(row['access_scopes'])-set(scopes) or row.get('created_by') not in ('',actor) and not row.get('operations') and not row.get('target_record_id'):
                raise PortalError('无权修改该事件资料。')
            if row.get('state')=='deleted':raise PortalError('事件已删除，未覆盖资料。')
            metadata=dict(row['metadata']);conflicts={};changed=False;dirty=set(row.get('dirty_keys') or [])
            for key,value in normalized.items():
                current=metadata.get(key,'' if key!='transfer_to_overhaul' else False)
                if current!=base.get(key,'' if key!='transfer_to_overhaul' else False) and current!=value:
                    conflicts[key]='该字段已被其他页面修改，请核对';continue
                if current!=value:metadata[key]=value;changed=True;dirty.add(key)
            if changed:
                row.update(metadata=metadata,dirty_keys=sorted(dirty),metadata_revision=row['metadata_revision']+1,sync_state='pending',sync_next_at=0)
                if metadata.get('building') in scopes:row['access_scopes']=[metadata['building']]
            row.update(updated_at=time.time(),metadata_issues={**invalid,**conflicts})
            self.store.put_document(NS,event_id,row)
            return self.public(row)

    def accept(self,event_id,operation_id,actor,scopes,action,text,*,mention_buildings=None):
        if action not in ('start','update','end'):raise PortalError('请选择新增、更新或结束。')
        if not isinstance(text,str) or not text.strip() or len(text.encode('utf-8'))>15000:raise PortalError('请粘贴事件通告原文，单条不超过15000字节。')
        if not re.fullmatch(r'[a-f0-9]{32}',operation_id):raise PortalError('发送操作标识无效。')
        intent={'event_id':event_id,'actor':actor,'action':action,'text':text}
        with self.lock('event:'+event_id):
            previous=self.store.get_document(OPS,operation_id)
            if previous:
                if previous['intent']!=intent:raise PortalError('原任务内容不一致，请勿复用操作编号。')
                return self.public(self.get(event_id))
            row=self.get(event_id) or self.new(event_id,actor,scopes)
            if set(row['access_scopes'])-set(scopes) or row.get('created_by') not in ('',actor) and not row.get('operations') and not row.get('target_record_id'):
                raise PortalError('无权发送该事件。')
            if row['state']=='deleted':raise PortalError('事件已删除。')
            if action=='start' and row['operations']:raise PortalError('本事件已有新增任务，请重试原任务或发送更新。')
            if action!='start' and not (row['operations'] or row.get('target_record_id')):raise PortalError('请先发送本事件的新增通告。')
            targets=[]
            selected=self.preference(actor)['robot_ids']
            if not selected:raise PortalError('请至少选择一个默认钉钉机器人后发送。')
            robots={r['id']:r for r in self.ding.config()['robots']}
            for identity in selected:
                try:target=self.ding.snapshot('事件通告',{'buildings':list(scopes),'mention_buildings':mention_buildings or [],'dingtalk_robot_ids':[identity]})[0]
                except DingTalkError as exc:
                    target={'id':identity,'name':robots.get(identity,{}).get('name','已失效机器人'),'invalid':str(exc)}
                targets.append({'channel':'dingtalk',**target})
            feishu=feishu_destination(text)
            if feishu is not None:targets.append({'channel':'feishu',**feishu})
            if not targets:raise PortalError('非I3事件不发飞书群，请先在右上角设置默认钉钉机器人。')
            row['sequence']+=1
            op={'operation_id':operation_id,'intent':intent,'access_scopes':list(scopes),'sequence':row['sequence'],'targets':targets,'dingtalk_selection':list(selected),'receipts':{},'state':'pending','created_at':time.time()}
            row['operations'].append(operation_id);row.update(text=text,updated_at=time.time(),sync_next_at=0)
            if action=='start':row['start_requested_at']=op['created_at']
            self.store.enqueue_outbox_event(CHANNEL,{'operation_id':operation_id,'event_id':event_id,'idempotency_key':operation_id},
                documents={(NS,event_id):row,(OPS,operation_id):op})
        dispatch(self.runtime)
        return self.public(row)

    def public(self,row):
        if not row:return {}
        value={key:copy.deepcopy(row.get(key)) for key in ('event_id','created_by','state','created_at','updated_at','text','metadata','metadata_revision','sync_revision','sync_state','sync_error','target_record_id','access_scopes','metadata_issues')}
        value['missing']=missing(row.get('metadata') or {})
        op=self.store.get_document(OPS,row['operations'][-1]) if row.get('operations') else None
        value['delivery']={'operation_id':op['operation_id'],'state':op['state'],'action':op['intent']['action'],
            'targets':[{'channel':target['channel'],'name':target.get('name') or '飞书I3群',**(op['receipts'].get(target['channel']+':'+target['id']) or {'state':'pending'})} for target in op['targets']]} if op else None
        return value

    def _receipt(self,op,target,receipt):
        identity=op['operation_id'];event_id=op['intent']['event_id'];key=target['channel']+':'+target['id']
        with self.lock('event:'+event_id):
            current=self.store.get_document(OPS,identity);row=self.get(event_id)
            current['receipts'][key]=receipt
            documents={(OPS,identity):current}
            if receipt.get('state')=='sent':
                before=self.system_fields(row)
                stamp=receipt.get('sent_at') or time.time()
                row['first_delivery_at']=min(row.get('first_delivery_at') or stamp,stamp)
                if op['intent']['action']=='start':row['first_sent_at']=min(row.get('first_sent_at') or stamp,stamp)
                if op['intent']['action']=='update':
                    times=dict(row.get('update_times') or {});times[identity]=min(times.get(identity) or stamp,stamp);row['update_times']=times
                if target['channel']=='dingtalk' and op['intent']['action']=='start':row['dingtalk_start_at']=min(row.get('dingtalk_start_at') or stamp,stamp)
                if op['sequence']>=row.get('lifecycle_sequence',0):
                    same=op['sequence']==row.get('lifecycle_sequence',0)
                    row['lifecycle_sequence']=op['sequence'];row['state']='ended' if op['intent']['action']=='end' else 'active'
                    if op['intent']['action']=='end':row['ended_at']=min(row.get('ended_at') or stamp,stamp) if same else stamp
                changed=[name for name,value in self.system_fields(row).items() if before.get(name)!=value]
                if changed:
                    row.update(auto_dirty=list(dict.fromkeys([*(row.get('auto_dirty') or []),*changed])),metadata_revision=row['metadata_revision']+1,sync_state='pending',sync_next_at=0)
                row['updated_at']=time.time()
                documents[(NS,event_id)]=row
            from .event_priority import STATE_LOCK
            with STATE_LOCK:
                if receipt.get('state')=='sent':documents.update(self._priority_documents(row,op['created_at']))
                self.store.put_documents_atomic(documents)

    def _priority(self,row):
        from .event_priority import STATE_LOCK
        with STATE_LOCK:self.store.put_documents_atomic(self._priority_documents(row))

    def _priority_documents(self,row,operation_created_at=None):
        from . import event_priority
        metadata=row.get('metadata') or {};webkey='web-message:'+row['event_id'];key=row.get('target_record_id') or webkey
        summary={'building_codes':row['access_scopes'],'building':metadata.get('building') or '相关楼栋',
            'title':metadata.get('summary') or (row.get('text','').splitlines() or ['事件通告'])[0][:200],
            'occurrence_time':metadata.get('occurrence_time') or '待补充','source':metadata.get('event_source') or '待补充',
            'level':metadata.get('level') or '待补充','progress':row.get('text','')[:1000]}
        document={'state':'ended' if row['state'] in ('ended','deleted') else 'active','target_record_id':row.get('target_record_id',''),
            'web_event_id':row['event_id'],'first_delivered_at':row.get('first_sent_at') or row.get('created_at'),
            'summary':summary,'updated_at':time.time()}
        previous=self.store.get_document(event_priority.NAMESPACE,key) or {}
        if key!=webkey and previous and previous.get('web_event_id')!=row['event_id'] and (operation_created_at is None or previous.get('updated_at',0)>operation_created_at):
            document=previous
        documents={(event_priority.NAMESPACE,key):document}
        if key!=webkey:documents[(event_priority.NAMESPACE,webkey)]={**document,'state':'redirect'}
        return documents

    def _send_target(self,op,target):
        if (self.get(op['intent']['event_id']) or {}).get('state')=='deleted':return
        key=target['channel']+':'+target['id'];previous=op['receipts'].get(key) or {}
        if previous.get('state') in ('sent','failed','uncertain'):return
        if target.get('invalid'):
            self._receipt(op,target,{'state':'failed','reason':target['invalid']});return
        if target['channel']=='dingtalk':
            digest=hashlib.sha256(('web-event:'+op['operation_id']+':'+target['id']).encode()).hexdigest()
            result=self.ding.deliver(digest,target,op['intent']['text'],operation_id='web-event:'+op['operation_id'])
            self._receipt(op,target,{'state':result['state'],'reason':result.get('reason',''),'sent_at':result.get('first_sent_at'), 'delivery_id':digest})
            return
        from upload_event_module.services import robot_webhook as bot
        if not target.get('name'):
            self._receipt(op,target,{'state':'failed','reason':'Qt尚未配置飞书I3事件群。'});return
        chat_id=previous.get('chat_id')
        if previous.get('state')=='posting' and chat_id:
            from .feishu_delivery import PublicFeishuSender
            identity=str(uuid.uuid5(uuid.NAMESPACE_URL,'web-event:'+op['operation_id']+':feishu:'+chat_id+':'+str(target.get('attempt',0))))
            saved=self.store.get_document('feishu_public_delivery',PublicFeishuSender.route_key(chat_id,'chat_id',identity)) or {}
            if saved.get('status')=='sent':
                self._receipt(op,target,{'state':'sent','chat_id':chat_id,'sent_at':(saved.get('receipt') or {}).get('accepted_at') or time.time()});return
            if saved.get('status')!='not_sent':
                self._receipt(op,target,{'state':'uncertain','reason':'上次群消息结果未确认，请先核对群消息。'});return
        if not chat_id:
            chat_id,error=bot._find_chat_id_by_name(target['name'])
            if error:self._receipt(op,target,{'state':'pending','reason':'飞书I3群暂未就绪，后台将重试。'});return
        self._receipt(op,target,{'state':'posting','chat_id':chat_id})
        identity=str(uuid.uuid5(uuid.NAMESPACE_URL,'web-event:'+op['operation_id']+':feishu:'+chat_id+':'+str(target.get('attempt',0))))
        ok,_,results=bot._deliver_many('text',{'text':op['intent']['text']},[chat_id],'chat_id',identity)
        attempt=results[0] if results else {};state='sent' if ok else 'failed' if attempt.get('failure_kind') in ('not_sent','blocked','bot_unavailable') else 'uncertain'
        self._receipt(op,target,{'state':state,'chat_id':chat_id,'sent_at':time.time() if ok else None,
            'reason':'' if ok else '飞书群消息未送达，请检查应用和群权限后重试原消息。','message_uuid':identity})

    def claim_messages(self,limit=5):
        for task in self.store.list_outbox_events(CHANNEL,status='deferred',limit=100):
            if time.time()-task['updated_at']>5:self.store.mark_outbox_event(task['id'],'pending',count_attempt=False)
        return self.store.lease_outbox_events(CHANNEL,limit=limit,lease_seconds=300)

    def process_messages(self):
        for task in self.claim_messages():self.process_message_task(task)

    def process_message_task(self,task):
        try:
            with self.lock('lane:'+task['payload']['event_id'],300):
                op=self.store.get_document(OPS,task['payload']['operation_id'])
                row=self.get(op['intent']['event_id']) or {}
                if row.get('state')=='deleted':
                    self.store.mark_outbox_event(task['id'],'done',count_attempt=False);return
                if op['sequence']<row.get('sequence',0):
                    with self.lock('event:'+op['intent']['event_id']):
                        current=self.store.get_document(OPS,op['operation_id']);current['state']='superseded';self.store.put_document(OPS,op['operation_id'],current)
                    self.store.mark_outbox_event(task['id'],'done',count_attempt=False);return
                if not external_real_write_guard().get('real_write_allowed'):
                    self.store.mark_outbox_event(task['id'],'deferred',error='当前运行环境禁止真实消息发送',count_attempt=False);return
                actual=[target for target in op['targets'] if target['channel']=='dingtalk']
                selected=op.get('dingtalk_selection')
                if selected is not None and not self.ding.selection_matches({'request':{'dingtalk_robot_ids':selected},'result':{'dingtalk_targets':actual}}):
                    for target in actual:
                        self._receipt(op,target,{'state':'failed','reason':'钉钉目标与本次默认选择不一致，已停止发送，请核对设置。'})
                    targets=[target for target in op['targets'] if target['channel']!='dingtalk']
                else:
                    targets=op['targets']
                with ThreadPoolExecutor(max_workers=max(1,min(21,len(op['targets']))),thread_name_prefix='event-groups') as pool:
                    futures=[pool.submit(self._send_target,op,target) for target in targets]
                    for future in futures:
                        try:future.result()
                        except Exception:pass
                with self.lock('event:'+op['intent']['event_id']):
                    current=self.store.get_document(OPS,op['operation_id'])
                    states=[current['receipts'].get(t['channel']+':'+t['id'],{}).get('state','pending') for t in op['targets']]
                    current['state']='pending' if any(s in ('pending','posting') for s in states) else 'failed' if any(s!='sent' for s in states) else 'sent'
                    self.store.put_document(OPS,op['operation_id'],current)
                self.store.mark_outbox_event(task['id'],'deferred' if current['state']=='pending' else 'done',count_attempt=False)
        except Exception:self.store.mark_outbox_event(task['id'],'deferred',error='原消息保留，后台将继续处理',count_attempt=False)

    def retry(self,event_id,operation_id,confirm_uncertain=False):
        with self.lock('event:'+event_id):
            row=self.get(event_id);op=self.store.get_document(OPS,operation_id)
            if not row or not op or op['intent']['event_id']!=event_id:raise PortalError('原发送任务不存在。')
            if row['state']=='deleted':raise PortalError('事件已删除，未重发。')
            if op['sequence']<row.get('sequence',0):raise PortalError('已有后续事件消息，不重发旧通告。')
            actual=[target for target in op['targets'] if target['channel']=='dingtalk']
            if op.get('dingtalk_selection') is not None and not self.ding.selection_matches({'request':{'dingtalk_robot_ids':op['dingtalk_selection']},'result':{'dingtalk_targets':actual}}):
                raise PortalError('原任务的钉钉目标与默认选择不一致，请核对设置。')
            if op['state']=='pending':return self.public(row)
            uncertain=any(r.get('state') in ('posting','uncertain') for r in op['receipts'].values())
            if uncertain and not confirm_uncertain:raise PortalError('部分群消息结果未确认，请先核对群中是否已收到，再确认重发。')
            documents={}
            for target in op['targets']:
                key=target['channel']+':'+target['id'];receipt=op['receipts'].get(key) or {}
                if receipt.get('state')=='sent':continue
                if target['channel']=='dingtalk':
                    try:current=self.ding.snapshot('事件通告',{'buildings':op.get('access_scopes') or row['access_scopes'],'dingtalk_robot_ids':[target['id']]})[0]
                    except DingTalkError as exc:target['invalid']=str(exc);continue
                    # Keep the mention audience accepted with the original task.
                    current['mention_cipher']=target.get('mention_cipher','')
                    current['mention_warning']=target.get('mention_warning','')
                    target.update(current);target.pop('invalid',None)
                    delivery_id=receipt.get('delivery_id') or hashlib.sha256(('web-event:'+operation_id+':'+target['id']).encode()).hexdigest()
                    delivery=self.store.get_document('dingtalk_notices','delivery:'+delivery_id)
                    if delivery and delivery['state']!='sent':
                        delivery.update(state='pending',target=current,next_at=0,reason='用户核对后重试原消息')
                        documents[('dingtalk_notices','delivery:'+delivery_id)]=delivery
                else:
                    if receipt.get('state') in ('posting','uncertain'):target['attempt']=target.get('attempt',0)+1
                    if not target.get('name'):target['name']=str(config.group_name_event_i3 or '')
                op['receipts'][key]={'state':'pending',**({'chat_id':receipt['chat_id']} if receipt.get('chat_id') else {})}
            op['state']='pending';documents[(OPS,operation_id)]=op
            event=self.store.enqueue_outbox_event(CHANNEL,{'operation_id':operation_id,'event_id':event_id,'idempotency_key':operation_id},documents=documents)
            self.store.mark_outbox_event(event,'pending',count_attempt=False)
        dispatch(self.runtime)
        return self.public(row)

    def delete(self,event_id,actor):
        with self.lock('sync:'+event_id,300),self.lock('event:'+event_id,300):
            row=self.get(event_id)
            if not row:raise PortalError('事件不存在。')
            for identity in row.get('operations',[]):
                if (self.store.get_document(OPS,identity) or {}).get('state')=='pending':raise PortalError('群消息仍在处理，请等待发送结果后删除。')
            if row.get('target_record_id'):
                target=row['target_record_id']
                result=self.runtime.execute_local_delete_active_item({'data_dict':{'work_type':'event','notice_type':'事件通告',
                    'scope':row['access_scopes'][0] if len(row['access_scopes'])==1 else 'ALL',
                    '_auth_open_id':actor,'record_id':target,'target_record_id':target,'active_item_id':target}})
                if not result.get('ok'):raise PortalError('后台尚未确认删除成功，事件仍保留。')
            elif row.get('create_intent'):raise PortalError('多维新增结果仍待核验，暂不删除，避免遗留无法关联的记录。')
            row['state']='deleted';row['updated_at']=time.time()
            from .event_priority import STATE_LOCK
            with STATE_LOCK:self.store.put_documents_atomic({(NS,event_id):row,**self._priority_documents(row)})
        return {'deleted':True}

    def metadata_fields(self,row):
        meta=row['metadata'];result={}
        mapping={'summary':'alarm_desc','building':'building','specialty':'specialty','level':'level','event_source':'source','occurrence_time':'occurrence_time','recover_time':'recover_time','transfer_to_overhaul':'transfer_to_overhaul'}
        for key,value in meta.items():
            if key not in mapping:continue
            if row.get('target_record_id') and key not in (row.get('dirty_keys') or []):continue
            if key in ('occurrence_time','recover_time'):value=self.runtime.service._repair_management_datetime_ms(value) if value else None
            if key=='building' and value:value=self.runtime.service._building_label_from_code(value)
            result[EVENT_NOTICE_FIELDS[mapping[key]]]=value if value!='' else None
        result.update({key:value for key,value in self.system_fields(row).items() if not row.get('target_record_id') or key in row.get('auto_dirty',[])})
        return result

    def system_fields(self,row):
        result={}
        if row.get('first_sent_at'):result[EVENT_NOTICE_FIELDS['response_time']]=int((row.get('dingtalk_start_at') or row.get('start_requested_at') or row['first_sent_at'])*1000)
        if row.get('ended_at'):result[EVENT_NOTICE_FIELDS['end_time']]=int(row['ended_at']*1000)
        if row.get('update_times'):
            from datetime import datetime,timezone,timedelta
            lines=(row.get('progress_time_base') or '').splitlines()+[datetime.fromtimestamp(stamp,timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S') for stamp in sorted(row['update_times'].values())]
            result[EVENT_NOTICE_FIELDS['progress_update']]='\n'.join(dict.fromkeys(value for value in lines if value))
        return result

    def process_metadata(self):
        if not external_real_write_guard().get('real_write_allowed'):return
        from upload_event_module.services.service_registry import create_bitable_record_fields,update_bitable_record_fields,query_record_by_id
        attempted=0
        for item in sorted(self.store.list_documents(NS),key=lambda item:item['payload'].get('last_sync_attempt',0)):
            row=item['payload']
            if row.get('state')=='deleted' or row.get('sync_state') not in ('pending','retry') or row.get('sync_next_at',0)>time.time():continue
            if not row.get('operations') and not row.get('target_record_id'):continue
            if not row.get('target_record_id') and not row.get('first_delivery_at'):continue
            if attempted>=8:break
            attempted+=1
            try:
                with self.lock('sync:'+row['event_id'],300):
                    with self.lock('event:'+row['event_id']):
                        row=self.get(row['event_id'])
                        if row.get('state')=='deleted' or row.get('sync_state') not in ('pending','retry'):continue
                        row['last_sync_attempt']=time.time();self.store.put_document(NS,row['event_id'],row)
                    snapshot=copy.deepcopy(row);fields=self.metadata_fields(snapshot)
                    target=row.get('target_record_id')
                    if not fields and not target:continue
                    if not target:
                        # Freeze the first create body and client token across restarts.
                        with self.lock('event:'+row['event_id']):
                            current=self.get(row['event_id'])
                            if not current.get('create_intent'):
                                current['create_intent']={'fields':fields,'client_token':str(uuid.uuid4()),'created_at':time.time()}
                                self.store.put_document(NS,row['event_id'],current)
                            intent=current['create_intent']
                        ok,target=create_bitable_record_fields('事件通告',intent['fields'],client_token=intent['client_token'],audit_action='事件资料自动新增')
                        if not ok:raise PortalError('多维表新增尚未完成，消息发送不受影响。')
                        with self.lock('event:'+row['event_id']):
                            current=self.get(row['event_id']);current['target_record_id']=target
                            from .event_priority import STATE_LOCK
                            with STATE_LOCK:self.store.put_documents_atomic({(NS,row['event_id']):current,**(self._priority_documents(current) if current.get('lifecycle_sequence') else {})})
                    ok,remote=query_record_by_id(target,'事件通告')
                    if not ok or not isinstance(remote,dict):raise PortalError('多维表记录暂未读回，资料保留等待同步。')
                    def same(actual,expected):
                        if expected is None:return actual in (None,'',[],{})
                        if isinstance(expected,str):
                            from upload_event_module.services.handlers.base import notice_field_text
                            return notice_field_text(actual)==expected
                        return actual==expected
                    baseline=snapshot.get('synced_fields') or (snapshot.get('create_intent') or {}).get('fields') or {}
                    progress_key=EVENT_NOTICE_FIELDS['progress_update']
                    if progress_key in fields:
                        from upload_event_module.services.handlers.base import notice_field_text
                        fields[progress_key]='\n'.join(dict.fromkeys(notice_field_text((remote.get('fields') or {}).get(progress_key)).splitlines()+fields[progress_key].splitlines()))
                    # Only explicitly edited manual fields are patched. Preserve later
                    # corrections to automatic timestamps made by another workflow.
                    preserved={key:(remote.get('fields') or {}).get(key) for key in self.system_fields(snapshot)
                        if key!=progress_key and key in fields and key in baseline and not same((remote.get('fields') or {}).get(key),baseline[key])
                        and not same((remote.get('fields') or {}).get(key),fields[key])}
                    fields={key:value for key,value in fields.items() if key not in preserved}
                    changed={key:value for key,value in fields.items() if not same((remote.get('fields') or {}).get(key),value)}
                    if changed:
                        ok,_=update_bitable_record_fields(target,'事件通告',changed)
                        if not ok:raise PortalError('资料同步尚未完成，后台将重试。')
                        ok,remote=query_record_by_id(target,'事件通告')
                    if not ok or any(not same((remote.get('fields') or {}).get(k),v) for k,v in fields.items()):raise PortalError('资料写入待回读核验。')
                    with self.lock('event:'+row['event_id']):
                        current=self.get(row['event_id'])
                        dirty=[key for key in current.get('dirty_keys',[]) if key not in snapshot.get('dirty_keys',[]) or current['metadata'].get(key)!=snapshot['metadata'].get(key)]
                        automatic=[key for key in current.get('auto_dirty',[]) if key not in snapshot.get('auto_dirty',[]) or self.system_fields(current).get(key)!=self.system_fields(snapshot).get(key)]
                        current.update(sync_revision=snapshot['metadata_revision'],dirty_keys=dirty,auto_dirty=automatic,synced_fields={**(current.get('synced_fields') or {}),**fields,**preserved},sync_error='',sync_state='synced' if current['metadata_revision']==snapshot['metadata_revision'] else 'pending')
                        self.store.put_document(NS,row['event_id'],current)
                    self.runtime.service.clear_target_record_cache();self.runtime.clear_payload_cache()
            except Exception as exc:
                with self.lock('event:'+row['event_id']):
                    current=self.get(row['event_id'])
                    newer=current['metadata_revision']!=row['metadata_revision']
                    current.update(sync_state='pending' if newer else 'retry',sync_next_at=0 if newer else time.time()+60,sync_error='' if newer else str(exc) if isinstance(exc,PortalError) else '资料同步暂未完成，后台将重试；群消息不受影响。')
                    self.store.put_document(NS,row['event_id'],current)


_worker_lock=threading.Lock()
_workers={}
_message_executor=ThreadPoolExecutor(max_workers=4,thread_name_prefix='web-event-operation')
_message_jobs=[]
def dispatch(runtime):
    service=EventNoticeDelivery(runtime)
    with _worker_lock:
        _message_jobs[:]=[job for job in _message_jobs if not job.done()]
        if len(_message_jobs)<4:
            for task in service.claim_messages(4-len(_message_jobs)):_message_jobs.append(_message_executor.submit(service.process_message_task,task))
        for name,work in (('metadata',service.process_metadata),):
            previous=_workers.get(name)
            if previous is not None and previous.is_alive():continue
            thread=threading.Thread(target=work,name='web-event-'+name,daemon=True);_workers[name]=thread;thread.start()

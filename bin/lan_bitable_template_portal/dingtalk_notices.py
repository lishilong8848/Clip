"""Configurable notice robots, isolated from Feishu and the business write transaction."""
import base64
import hashlib
import hmac
import json
import logging
import re
import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from urllib.parse import parse_qs,urlsplit
from contextlib import contextmanager

NS='dingtalk_notices'
CHANNEL='notice_dingtalk'
TYPES=['维保通告','变更通告','事件通告','设备检修','设备轮巡','设备调整','上下电通告']
SCOPES=['110','A','B','C','D','E','H']
_dispatch_lock=threading.Lock()
_dispatch_thread=None


def dispatch(runtime):
    """Keep slow DingTalk receipts independent of the existing Feishu worker."""
    global _dispatch_thread
    with _dispatch_lock:
        if _dispatch_thread is not None and _dispatch_thread.is_alive():return
        service=DingTalkNotices(runtime.state_store)
        def run():
            try:service.process(runtime)
            except Exception:logging.getLogger(__name__).warning('钉钉投递暂未完成，原任务保留。')
        _dispatch_thread=threading.Thread(target=run,name='notice-dingtalk-dispatch',daemon=True)
        _dispatch_thread.start()


class DingTalkError(Exception):
    def __init__(self,message,status=400):super().__init__(message);self.status=status


def credentials(webhook,secret):
    try:
        url=urlsplit(webhook.strip());query=parse_qs(url.query)
        if url.scheme!='https' or url.hostname!='oapi.dingtalk.com' or url.port not in (None,443) or url.path!='/robot/send' or url.username or url.password or url.fragment or set(query)!={'access_token'}:
            raise ValueError()
        token=query['access_token'][0]
        if len(query['access_token'])!=1 or not re.fullmatch(r'[A-Za-z0-9]{16,256}',token):raise ValueError()
        if not re.fullmatch(r'SEC[A-Za-z0-9]{16,256}',secret.strip()):raise ValueError()
        return token,secret.strip()
    except (ValueError,TypeError,AttributeError):
        raise DingTalkError('请输入钉钉自定义机器人的完整 Webhook 和 SEC 开头的加签密钥。') from None


class DingTalkTransport:
    def send(self,token,secret,text,mention=None):
        from .portal_service import external_real_write_guard
        if not external_real_write_guard()['real_write_allowed']:
            return {'state':'failed','reason':'当前运行环境禁止真实外部发送。'}
        timestamp=str(int(time.time()*1000))
        signature=base64.b64encode(hmac.new(secret.encode(),(timestamp+'\n'+secret).encode(),hashlib.sha256).digest()).decode()
        # Do not log HTTP exceptions: their URL contains the webhook credential.
        import httpx
        try:
            body={'msgtype':'text','text':{'content':text}}
            if mention and mention.get('mode')=='all':
                body['text']['content']+='\n@所有人'
                body['at']={'isAtAll':True}
            elif mention and mention.get('mode')=='people':
                users=mention.get('user_ids') or [];mobiles=mention.get('mobiles') or []
                body['text']['content']+='\n'+' '.join('@'+item for item in [*users,*mobiles])
                body['at']={'atUserIds':users,'atMobiles':mobiles,'isAtAll':False}
            response=_http().request('POST','https://oapi.dingtalk.com/robot/send',
                params={'access_token':token,'timestamp':timestamp,'sign':signature},
                json=body,timeout=httpx.Timeout(15,connect=5),follow_redirects=False)
            if response.status_code==429:
                return {'state':'pending','reason':'钉钉限制发送频率，稍后自动重试。','next_at':time.time()+600}
            data=response.json()
            if response.status_code==200 and type(data.get('errcode')) is int and data['errcode']==0:
                return {'state':'sent','reason':''}
            code=data.get('errcode')
            if type(code) is int and code!=0:
                return {'state':'failed','reason':'钉钉拒绝消息，请核对机器人安全设置、可用状态及频率（code='+str(code)[:30]+'）。'}
            return {'state':'uncertain','reason':'钉钉未返回可核验的发送回执，请先查看群消息。'}
        except (httpx.ConnectTimeout,httpx.ConnectError):
            return {'state':'pending','reason':'未能连接钉钉，稍后自动重试。','next_at':time.time()+60}
        except Exception:
            return {'state':'uncertain','reason':'发送结果未确认，已暂停重发；请先查看群消息，避免重复。'}


_client=None
_client_lock=threading.Lock()
class _SecretFilter(logging.Filter):
    def filter(self,record):
        message=record.getMessage()
        if 'access_token=' in message:
            record.msg=re.sub(r'(access_token|sign)=[^&\s\'\"<>]+',r'\1=[redacted]',message)
            record.args=()
        return True

def _http():
    global _client
    with _client_lock:
        if _client is None:
            from upload_event_module.services.http_client import FeishuHttpClient
            for name in ('httpx','httpcore.http11','httpcore.http2'):
                logger=logging.getLogger(name)
                if not any(isinstance(f,_SecretFilter) for f in logger.filters):logger.addFilter(_SecretFilter())
            _client=FeishuHttpClient(retries=0)
    return _client


def message_parts(text,limit=15000):
    parts=[];part='';size=0
    for char in text:
        width=len(char.encode('utf8'))
        if size+width>limit:parts.append(part);part='';size=0
        part+=char;size+=width
    if part:parts.append(part)
    return [f'【通告 {i+1}/{len(parts)}】\n'+part for i,part in enumerate(parts)] if len(parts)>1 else parts


class DingTalkNotices:
    def __init__(self,store,transport=None):self.store=store;self.transport=transport or DingTalkTransport()

    @contextmanager
    def lock(self,key,ttl_seconds=90):
        owner=uuid.uuid4().hex;key='dingtalk:'+key
        acquired,_=self.store.acquire_notice_operation_lock(key,owner=owner,action='dingtalk',ttl_seconds=ttl_seconds)
        if not acquired:raise DingTalkError('该操作正在处理，请稍后再试。',409)
        try:yield
        finally:self.store.release_notice_operation_lock(key,owner)

    def config(self):return self.store.get_document(NS,'config') or {'revision':0,'robots':[]}

    @staticmethod
    def _mention(cipher):
        if not cipher:return {'mode':'none','user_ids':[],'mobiles':[]}
        from openclaw_service.assistant.lighthouse_ai import unprotect_key
        try:
            value=json.loads(unprotect_key(cipher))
            if not isinstance(value,dict) or value.get('mode') not in ('none','people','all'):raise ValueError()
            return value
        except Exception:raise DingTalkError('本机无法读取机器人艾特设置，请重新保存配置。',409) from None

    @staticmethod
    def _validate_mention(value):
        mode=value.get('mention_mode','none')
        users=value.get('mention_user_ids',[]);mobiles=value.get('mention_mobiles',[])
        buildings=value.get('mention_building_mobiles',{})
        if mode not in ('none','people','all') or not isinstance(users,list) or not isinstance(mobiles,list) or not isinstance(buildings,dict):
            raise DingTalkError('艾特设置格式无效。')
        if any(scope not in SCOPES or not isinstance(numbers,list) for scope,numbers in buildings.items()):
            raise DingTalkError('楼栋艾特设置格式无效。')
        building_numbers=[number for numbers in buildings.values() for number in numbers]
        if len(users)+len(mobiles)+len(building_numbers)>50:raise DingTalkError('每个机器人最多设置50个艾特对象。')
        if any(not isinstance(x,str) or not x.isprintable() or not re.fullmatch(r'[^\s,，;；@]{1,96}',x) for x in users):
            raise DingTalkError('钉钉用户ID格式无效，请使用实际用户ID并以逗号或换行分隔。')
        if any(not isinstance(x,str) or not re.fullmatch(r'\+?[0-9]{6,20}',x) for x in [*mobiles,*building_numbers]):
            raise DingTalkError('手机号格式无效，请以逗号或换行分隔。')
        users=list(dict.fromkeys(users));mobiles=list(dict.fromkeys(mobiles))
        buildings={scope:list(dict.fromkeys(numbers)) for scope,numbers in buildings.items() if numbers}
        if sum(len(x.encode('utf-8'))+2 for x in [*users,*mobiles,*building_numbers])>4000:raise DingTalkError('艾特对象信息过长，请减少成员数量。')
        if mode=='people' and not users and not mobiles and not buildings:raise DingTalkError('请选择指定成员的用户ID或手机号。')
        result={'mode':mode,'user_ids':users,'mobiles':mobiles}
        if buildings:result['building_mobiles']=buildings
        return result

    def public(self):
        data=self.config()
        robots=[]
        for r in data['robots']:
            mention=self._mention(r.get('mention_cipher'))
            robots.append({k:v for k,v in r.items() if k not in ('token_cipher','secret_cipher','token_hash','mention_cipher')}|
                {'configured':True,'mention_mode':mention['mode'],'mention_user_ids':mention.get('user_ids',[]),'mention_mobiles':mention.get('mobiles',[]),'mention_building_mobiles':mention.get('building_mobiles',{})})
        return {'revision':data['revision'],'robots':robots,
                'types':TYPES,'scopes':SCOPES}

    def save(self,payload):
        from openclaw_service.assistant.lighthouse_ai import protect_key
        if not isinstance(payload,dict) or not isinstance(payload.get('robots'),list) or len(payload['robots'])>20:
            raise DingTalkError('最多配置20个钉钉机器人。')
        with self.lock('config'):
            current=self.config()
            if payload.get('revision')!=current['revision']:raise DingTalkError('配置已变化，请刷新后重新核对。',409)
            old={r['id']:r for r in current['robots']};robots=[];tokens=set();ids=set()
            for value in payload['robots']:
                if not isinstance(value,dict):raise DingTalkError('机器人配置格式无效。')
                identity=value.get('id') or uuid.uuid4().hex
                if not re.fullmatch('[a-f0-9]{32}',identity) or identity in ids or value.get('id') and identity not in old:raise DingTalkError('机器人标识重复或无效。')
                ids.add(identity);previous=old.get(identity,{})
                name=str(value.get('name') or '').strip();remark=str(value.get('remark') or '').strip()
                if not name or len(name)>80 or len(remark)>500:raise DingTalkError('请填写80字以内的名称和500字以内的备注。')
                scopes=value.get('scopes',[]);types=value.get('types',[])
                if not isinstance(scopes,list) or not isinstance(types,list) or any(x not in SCOPES for x in scopes) or any(x not in TYPES for x in types):raise DingTalkError('楼栋或通告类型无效。')
                if type(value.get('enabled')) is not bool:raise DingTalkError('请选择是否启用机器人。')
                robot={k:previous[k] for k in ('token_cipher','secret_cipher','token_hash','webhook_hint') if k in previous}
                webhook=str(value.get('webhook') or '').strip();secret=str(value.get('secret') or '').strip()
                if webhook or secret or not previous:
                    from openclaw_service.assistant.lighthouse_ai import unprotect_key
                    if not webhook and previous:webhook='https://oapi.dingtalk.com/robot/send?access_token='+unprotect_key(previous['token_cipher'])
                    if not secret and previous:secret=unprotect_key(previous['secret_cipher'])
                    token,secret=credentials(webhook,secret)
                    robot.update(token_cipher=protect_key(token),secret_cipher=protect_key(secret),token_hash=hashlib.sha256(token.encode()).hexdigest(),webhook_hint='已保存 · 尾号 '+token[-4:])
                if robot['token_hash'] in tokens:raise DingTalkError('同一机器人不能重复添加。')
                tokens.add(robot['token_hash'])
                robot.update(id=identity,name=name,remark=remark,enabled=value['enabled'],scopes=list(dict.fromkeys(scopes)),types=list(dict.fromkeys(types)))
                previous_mention=self._mention(previous.get('mention_cipher'))
                if not any(key in value for key in ('mention_mode','mention_user_ids','mention_mobiles','mention_building_mobiles')):
                    mention=previous_mention
                else:mention=self._validate_mention({'mention_mode':previous_mention.get('mode','none'),
                    'mention_user_ids':previous_mention.get('user_ids',[]),'mention_mobiles':previous_mention.get('mobiles',[]),
                    'mention_building_mobiles':previous_mention.get('building_mobiles',{}),**value})
                if mention==previous_mention and previous.get('mention_cipher'):robot['mention_cipher']=previous['mention_cipher']
                elif mention['mode']!='none' or mention['user_ids'] or mention['mobiles'] or mention.get('building_mobiles'):
                    robot['mention_cipher']=protect_key(json.dumps(mention,ensure_ascii=False,separators=(',',':')))
                # Remarks/names do not invalidate pending deliveries; credential or route changes do.
                routing=('token_cipher','secret_cipher','enabled','scopes','types')
                changed=any(robot.get(k)!=previous.get(k) for k in routing)
                robot['revision']=previous.get('revision',0)+int(changed or not previous)
                robots.append(robot)
            self.store.put_document(NS,'config',{'revision':current['revision']+1,'robots':robots})
        return self.public()

    def snapshot(self,notice_type,payload):
        if notice_type in ('上电通告','下电通告'):notice_type='上下电通告'
        selected=payload.get('dingtalk_robot_ids') or []
        if not isinstance(selected,list) or len(selected)>20 or any(not isinstance(x,str) or not x for x in selected) or len(selected)!=len(set(selected)):
            raise DingTalkError('钉钉机器人选择无效。')
        if not selected:return []
        buildings={code for item in payload.get('buildings') or [] for code in re.findall(r'110|[ABCDEH]',str(item).upper())}
        mention_input=payload.get('mention_buildings',payload.get('buildings') or [])
        if not isinstance(mention_input,list):raise DingTalkError('通告楼栋格式无效。')
        from .portal_service import MaintenancePortalService
        mention_buildings=set(MaintenancePortalService._building_codes_from_value(mention_input))
        targets=[]
        for robot in self.config()['robots']:
            if not robot['enabled'] or robot['id'] not in selected or (robot['types'] and notice_type not in robot['types']) or (robot['scopes'] and not buildings.intersection(robot['scopes'])):continue
            cipher=robot.get('mention_cipher','');warning=''
            mention=self._mention(cipher)
            mapping=mention.get('building_mobiles') or {}
            if mention['mode']=='people' and mapping:
                phones=list(dict.fromkeys([*(mention.get('mobiles') or []),*(number for scope in SCOPES if scope in mention_buildings for number in mapping.get(scope,[]))]))
                resolved={'mode':'people' if phones or mention.get('user_ids') else 'none','user_ids':mention.get('user_ids') or [],'mobiles':phones}
                from openclaw_service.assistant.lighthouse_ai import protect_key
                cipher=protect_key(json.dumps(resolved,ensure_ascii=False,separators=(',',':')))
                missing=[scope for scope in SCOPES if scope in mention_buildings and scope not in mapping]
                if not mention_buildings:warning='未识别通告楼栋，未附加楼栋专属艾特。'
                elif missing:warning='部分楼栋未配置专属艾特手机号：'+'、'.join(missing)+'。'
            targets.append({'id':robot['id'],'revision':robot['revision'],'name':robot['name'],'mention_cipher':cipher,'mention_warning':warning})
        if set(selected)!={r['id'] for r in targets}:raise DingTalkError('所选钉钉机器人已停用、删除或不适用于当前楼栋和通告类型，请重新选择。')
        return targets

    def _robot(self,target):
        robot=next((r for r in self.config()['robots'] if r['id']==target['id']),None)
        if not robot or not robot['enabled'] or robot['revision']!=target['revision']:
            raise DingTalkError('机器人已删除、停用或配置已变化，请核对后按当前配置重试。',409)
        return robot

    def _save_delivery(self,key,data):
        data['updated_at']=time.time();self.store.put_document(NS,'delivery:'+key,data)

    @staticmethod
    def selection_matches(operation):
        """The durable sending targets must equal the original explicit selection."""
        requested=(operation.get('request') or {}).get('dingtalk_robot_ids')
        targets=(operation.get('result') or {}).get('dingtalk_targets')
        if not isinstance(requested,list) or not isinstance(targets,list):return False
        if any(not isinstance(value,str) or not value for value in requested):return False
        if any(not isinstance(value,dict) or not isinstance(value.get('id'),str) or not value['id'] for value in targets):return False
        actual=[value['id'] for value in targets]
        return len(requested)==len(set(requested)) and len(actual)==len(set(actual)) and set(actual)==set(requested)

    def deliver(self,key,target,text,*,retry=False,operation_id=''):
        from openclaw_service.assistant.lighthouse_ai import unprotect_key
        with self.lock('delivery:'+key):
            data=self.store.get_document(NS,'delivery:'+key)
            if not data:
                try:mention=self._mention(target.get('mention_cipher'))
                except DingTalkError as error:
                    data={'id':key,'robot_id':target['id'],'robot_name':target['name'],'target':target,'mention_cipher':target.get('mention_cipher',''),
                        'text':text,'parts':message_parts(text),'sent_parts':0,'state':'failed','reason':str(error),'created_at':time.time(),'operation_id':operation_id}
                    self._save_delivery(key,data);return data
                suffix='@所有人' if mention['mode']=='all' else ' '.join('@'+item for item in [*mention.get('user_ids',[]),*mention.get('mobiles',[])]) if mention['mode']=='people' else ''
                limit=15000-len(suffix.encode('utf-8'))-64 if suffix else 15000
                data={'id':key,'robot_id':target['id'],'robot_name':target['name'],'target':target,
                    'mention_cipher':target.get('mention_cipher',''),'text':text,'parts':message_parts(text,limit),
                    'sent_parts':0,'state':'pending','created_at':time.time(),'operation_id':operation_id}
            if data['text']!=text:raise DingTalkError('原任务正文发生变化，已停止发送。',409)
            if data['state']=='sent':return data
            if data['state']=='posting':
                data.update(state='uncertain',reason='上次发送后未保存完整回执，请先核对群消息。');self._save_delivery(key,data)
            if data['state'] in ('failed','uncertain') and not retry:return data
            if data.get('next_at',0)>time.time() and not retry:return data
            try:robot=self._robot(target)
            except DingTalkError as error:
                data.update(state='failed',reason=str(error));self._save_delivery(key,data);return data
            with self.lock('robot:'+target['id']):
                rate=self.store.get_document(NS,'rate:'+target['id']) or {}
                if rate.get('next_at',0)>time.time():
                    data.update(state='failed' if data.get('operation_id','').startswith('test:') else 'pending',next_at=rate['next_at'],reason='等待发送间隔，请稍后继续');self._save_delivery(key,data);return data
                if not data['parts']:
                    data.update(state='failed',reason='通告正文为空');self._save_delivery(key,data);return data
                try:token=unprotect_key(robot['token_cipher']);secret=unprotect_key(robot['secret_cipher'])
                except Exception:
                    data.update(state='failed',reason='本机无法读取机器人密钥，请重新保存配置。');self._save_delivery(key,data);return data
                try:mention=self._mention(data.get('mention_cipher'))
                except DingTalkError as error:
                    data.update(state='failed',reason=str(error));self._save_delivery(key,data);return data
                data.update(state='posting',target=target,next_at=0,updated_at=time.time())
                self.store.put_documents(NS,{'delivery:'+key:data,'rate:'+target['id']:{'next_at':time.time()+4}})
                part=data['parts'][data['sent_parts']]
                if mention['mode']!='none' and data['sent_parts']==len(data['parts'])-1:
                    result=self.transport.send(token,secret,part,mention=mention)
                else:result=self.transport.send(token,secret,part)
                if result['state']=='sent':
                    data.setdefault('first_sent_at',time.time())
                    data['sent_parts']+=1
                    complete=data['sent_parts']==len(data['parts'])
                    data.update(state='sent' if complete else 'pending',reason=target.get('mention_warning','') if complete else '',next_at=time.time()+4)
                else:
                    data.update(result)
                    if data['state']=='pending' and data.get('operation_id','').startswith('test:'):data['state']='failed'
                self._save_delivery(key,data);return data

    def process(self,runtime):
        # Reuse the portal's existing five-second notice scheduler.
        for task in self.store.list_outbox_events(CHANNEL,status='deferred',limit=500):
            if time.time()-task['updated_at']>=5:self.store.mark_outbox_event(task['id'],'pending',count_attempt=False)
        for task in self.store.lease_outbox_events(CHANNEL,limit=3,lease_seconds=900):
            operation_id=task['payload'].get('operation_id','')
            try:
                with self.lock('operation:'+operation_id,ttl_seconds=900):
                    if operation_id.startswith(('qt_notice:','qt_upload:','qt_update:','qt_end:','qt_upload_replace:')):
                        self.store.mark_outbox_event(task['id'],'done',error='Qt通告不发送钉钉',count_attempt=False);continue
                    operation=self.store.get_notice_remote_operation(operation_id) or {};result=operation.get('result') or {}
                    if operation.get('status')!='completed':raise DingTalkError('通告尚未完成，钉钉推送等待确认。',409)
                    if not self.selection_matches(operation):
                        reason='实际钉钉目标与本次勾选不一致，已停止发送，请核对原任务。'
                        self.store.mark_notice_remote_operation(operation_id,status='completed',result={'dingtalk_delivery_state':'failed','dingtalk_selection_error':reason})
                        if operation_id.startswith('notice_action:') and hasattr(runtime.service,'set_notice_followup'):
                            runtime.service.set_notice_followup(operation_id.removeprefix('notice_action:'),'dingtalk','failed',reason)
                        self.store.mark_outbox_event(task['id'],'failed',error=reason,count_attempt=False);continue
                    kind=result.get('robot_notice_type','');payload=result.get('robot_payload') or {}
                    if kind!='事件通告':
                        if operation_id.startswith('notice_action:'):
                            job=runtime.service.get_job(operation_id.removeprefix('notice_action:')) or {}
                            if job.get('superseded_by_job_id'):
                                self.store.mark_notice_remote_operation(operation_id,status='completed',result={'dingtalk_delivery_state':'superseded'})
                                self.store.mark_outbox_event(task['id'],'done',count_attempt=False);continue
                        from .event_priority import blockers
                        if blockers(self.store):
                            self.store.mark_outbox_event(task['id'],'deferred',error='等待事件结束',count_attempt=False);continue
                    else:
                        reason,error=runtime._event_robot_retry_guard(operation,check_remote=False)
                        if error:raise DingTalkError('事件状态尚未核验，等待确认。',409)
                        if reason:
                            self.store.mark_notice_remote_operation(operation_id,status='completed',result={'dingtalk_delivery_state':'superseded'})
                            self.store.mark_outbox_event(task['id'],'done',error='事件已被后续操作替代',count_attempt=False);continue
                    if result.get('robot_delivery_state')=='superseded':
                        self.store.mark_notice_remote_operation(operation_id,status='completed',result={'dingtalk_delivery_state':'superseded'})
                        self.store.mark_outbox_event(task['id'],'done',count_attempt=False);continue
                    targets=list({target['id']:target for target in result.get('dingtalk_targets') or []}.values())
                    if targets:
                        # Each robot owns its durable receipt and rate limit. Slow/failed
                        # robots must not hold back the other selected destinations.
                        with ThreadPoolExecutor(max_workers=min(20,len(targets)),thread_name_prefix='notice-dingtalk') as pool:
                            futures=[pool.submit(copy_context().run,self._deliver_target,runtime,operation,target) for target in targets]
                            outcomes=[future.result() for future in futures]
                    else:outcomes=[]
                    first_sent=min((x['first_sent_at'] for x in outcomes if x.get('first_sent_at')),default=0)
                    time_state='unchanged'
                    if first_sent and operation.get('target_record_id'):
                        from .dingtalk_notice_time import sync_start_time
                        first_sent=result.get('dingtalk_first_sent_at') or first_sent
                        self.store.mark_notice_remote_operation(operation_id,status='completed',result={'dingtalk_first_sent_at':first_sent})
                        time_state=sync_start_time(runtime,operation,first_sent)
                        self.store.mark_notice_remote_operation(operation_id,status='completed',result={'dingtalk_start_time_state':time_state})
                    state='deferred' if time_state=='pending' or any(x['state'] in ('pending','posting') for x in outcomes) else 'failed' if time_state=='conflict' or any(x['state']!='sent' for x in outcomes) else 'done'
                    self.store.mark_notice_remote_operation(operation_id,status='completed',result={'dingtalk_delivery_state':state})
                    if operation_id.startswith('notice_action:') and hasattr(runtime.service,'set_notice_followup'):
                        runtime.service.set_notice_followup(operation_id.removeprefix('notice_action:'),'dingtalk',state,
                            '' if state=='done' else '钉钉推送或开始时间回填尚未完成，请查看钉钉通告推送设置。')
                    self.store.mark_outbox_event(task['id'],state,error='' if state=='done' else '钉钉推送详情见系统设置',count_attempt=False)
            except Exception:
                # Never put raw transport exceptions or credentials into the outbox.
                self.store.mark_outbox_event(task['id'],'deferred',error='钉钉任务暂未完成，请在系统设置核对',count_attempt=False)

    def _deliver_target(self,runtime,operation,target):
        operation_id=operation['operation_id']
        key=hashlib.sha256((operation_id+':'+target['id']).encode()).hexdigest()
        try:
            previous=self.store.get_document(NS,'delivery:'+key)
            if previous and previous['state'] not in ('pending','posting'):return previous
            # Authentication context must be entered in the worker thread, not
            # merely in the scheduler that created the thread pool.
            with runtime._notice_robot_impersonation(operation):
                result=operation.get('result') or {}
                if result.get('robot_notice_type')!='事件通告':
                    from .event_priority import blockers
                    if blockers(self.store):return previous or {'state':'pending'}
                return self.deliver(key,previous.get('target',target) if previous else target,
                    str((result.get('robot_payload') or {}).get('text') or ''),operation_id=operation_id)
        except Exception:
            # A worker failure cannot abort its peers. A persisted posting state
            # is reconciled as uncertain by deliver(), never blindly resent.
            return {'state':'pending'}

    def recent(self):
        rows=[x['payload'] for x in self.store.list_documents(NS,key_prefix='delivery:')]
        return [{k:x.get(k) for k in ('id','robot_name','state','reason','sent_parts','created_at','updated_at')}|
                {'parts':len(x['parts']),'summary':' '.join(x['text'].splitlines()[:3])[:180]} for x in sorted(rows,key=lambda x:x.get('updated_at',0),reverse=True)[:50]]

    def retry(self,key,confirm_uncertain=False):
        data=self.store.get_document(NS,'delivery:'+key)
        if not data:raise DingTalkError('投递任务不存在。',404)
        if data.get('operation_id','').startswith(('qt_notice:','qt_upload:','qt_update:','qt_end:','qt_upload_replace:')):
            raise DingTalkError('Qt通告不发送钉钉，请在网页办理事件通告。',409)
        if data.get('operation_id','').startswith('web-event:'):
            raise DingTalkError('请返回事件通告卡片核对并重试原消息；不会重发已经送达的群。',409)
        if data['state'] in ('posting','uncertain') and not confirm_uncertain:raise DingTalkError('发送结果未确认，请先核对钉钉群，再确认是否重发。',409)
        robot=next((r for r in self.config()['robots'] if r['id']==data['robot_id'] and r['enabled']),None)
        if not robot:raise DingTalkError('请先启用对应机器人。')
        if data.get('operation_id','').startswith('test:'):
            return self.deliver(key,{k:robot[k] for k in ('id','revision','name')},data['text'],retry=True)
        if not data.get('operation_id'):raise DingTalkError('原通告操作编号缺失，未重新发送。',409)
        operation=self.store.get_notice_remote_operation(data['operation_id']) or {}
        if not self.selection_matches(operation):raise DingTalkError('原任务的钉钉目标与本次勾选不一致，请核对原任务。',409)
        result=operation.get('result') or {}
        self.snapshot(result.get('robot_notice_type',''),{**(result.get('robot_payload') or {}),'dingtalk_robot_ids':[data['robot_id']]})
        with self.lock('delivery:'+key):
            current=self.store.get_document(NS,'delivery:'+key)
            if current['state']=='sent':return current
            target={k:robot[k] for k in ('id','revision','name')}
            # Keep the original audience and any scope warning across a manual retry.
            target['mention_cipher']=current.get('mention_cipher','')
            target['mention_warning']=(current.get('target') or {}).get('mention_warning','')
            current.update(state='pending',reason='等待按当前配置重试',target=target)
            event_id=self.store.enqueue_outbox_event(CHANNEL,{'operation_id':data['operation_id'],'idempotency_key':'notice_dingtalk:'+data['operation_id']},documents={(NS,'delivery:'+key):current})
            self.store.mark_outbox_event(event_id,'pending',count_attempt=False)
        return current

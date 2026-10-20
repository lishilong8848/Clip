"""Durable handover invitations over the existing Feishu assistant connection."""
import asyncio
import hashlib
import json
import logging
import re
import time

from openclaw_service.assistant.lighthouse_ai import AssistantError
from upload_event_module.services.robot_webhook import active_bot_matches, current_app_credentials
from .work_handover import HandoverError, authorized

STATUS = {'draft':'草稿','pending':'待接收','accepted':'已接收','returned':'退回补充','withdrawn':'已撤回'}


class HandoverDeliveryScopeError(HandoverError):
    def __init__(self,message):
        super().__init__(message,409)


def parse_event(payload,app_id):
    try:
        header,event=payload['header'],payload['event']
        action=event['action']
        value=action['value']
        operator,context=event['operator'],event['context']
        fields=action.get('form_value') or {}
        if header.get('app_id')!=app_id or header.get('event_type')!='card.action.trigger' or value.get('kind')!='work_handover':
            return None
        if value.get('action') not in {'accept','return'} or type(value.get('version')) is not int or not isinstance(fields,dict) or set(fields)-{'reason'}:
            return None
        if not all(re.fullmatch(pattern,str(v or '')) for v,pattern in (
            (value.get('id'),r'[a-f0-9]{32}'),(value.get('token'),r'[A-Za-z0-9_-]{40,64}'),
            (operator.get('open_id'),r'ou_[A-Za-z0-9]+'),(context.get('open_chat_id'),r'oc_[A-Za-z0-9]+'),
            (context.get('open_message_id'),r'om_[A-Za-z0-9]+'))):
            return None
        reason=fields.get('reason','')
        if not isinstance(reason,str) or len(reason)>1000 or not isinstance(header.get('event_id'),str) or not 0<len(header['event_id'])<=128:
            return None
        return {'kind':'work_handover','message_id':'wh_'+hashlib.sha256((header['event_id']+operator['open_id']).encode()).hexdigest()[:32],
                'reply_to_message_id':context['open_message_id'],'chat_id':context['open_chat_id'],'chat_type':'p2p',
                'sender_id':operator['open_id'],'union_id':operator.get('union_id',''),'question':'[人员工作交接]',
                'handover':{'id':value['id'],'version':value['version'],'action':value['action'],'token':value['token'],'reason':reason}}
    except (KeyError,TypeError,AttributeError):
        return None


def card(data,base):
    plain=lambda value:{'tag':'plain_text','content':str(value)}
    rows=[i for i in data['items'] if not i.get('removed')]
    count=lambda kinds:sum(i['kind'] in kinds for i in rows)
    elements=[{'tag':'div','text':plain(data['title'])},
              {'tag':'div','text':plain(data['owner']['name']+' → '+data['recipient_name'])},
              {'tag':'div','text':plain(f"进行中 {count({'ongoing'})} 项 · 待处理 {count({'todo'})} 项 · 历史完成 {count({'history'})} 项 · 资料 {count({'reference'})} 项")},
              {'tag':'div','text':plain('状态：'+STATUS.get(data['status'],data['status']))}]
    for row in sorted(rows,key=lambda i:i['kind'] in {'history','reference'})[:5]:
        label={'110':'110站','CAMPUS':'园区'}.get(row['scope'],row['scope']+'楼')
        elements.append({'tag':'div','text':plain(label+' · '+row['title'])})
    elements.append({'tag':'button','text':plain('查看完整交接单'),'url':base.rstrip('/')+'/work-handover?id='+data['id'],'type':'default'})
    if data['status']=='pending':
        buttons=[]
        for action,label,style in [('accept','确认接收','primary'),('return','退回补充','default')]:
            buttons.append({'tag':'column','width':'weighted','weight':1,'elements':[{
                'tag':'button','name':action,'text':plain(label),'type':style,'form_action_type':'submit',
                'value':{'kind':'work_handover','id':data['id'],'version':data['sent_version'],'token':data['card_token'],'action':action}}]})
        elements.append({'tag':'form','name':'handover','elements':[
            {'tag':'input','name':'reason','label':plain('接收意见 / 需补充内容'),'placeholder':plain('退回时请填写需补充内容'),'max_length':1000},
            {'tag':'column_set','flex_mode':'flow','columns':buttons}]})
    elif data.get('decision_by'):
        elements.append({'tag':'div','text':plain(data['decision_by']+' · '+data.get('decision_at','')+' '+data.get('decision_reason',''))})
    return {'schema':'2.0','header':{'title':plain('灯塔 · 人员工作交接'),'template':'green' if data['status']=='accepted' else 'blue'},'body':{'elements':elements}}


class HandoverCards:
    def __init__(self,bot):
        self.bot,self.task=bot,None

    def tick(self):
        if self.task is None or self.task.done():
            self.task=asyncio.create_task(self.drain())

    async def close(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task,return_exceptions=True)

    async def handle(self,message):
        user=await self.bot._resolve_user(message)
        value=message['handover']
        try:
            actor=await asyncio.to_thread(self.bot.runtime.work_handover_actor,user['open_id'],user.get('name',''))
            svc=await asyncio.to_thread(self.bot.runtime.work_handovers)
            result=await asyncio.to_thread(svc.action,actor,value['id'],value['version'],value['action'],reason=value['reason'],token=value['token'])
        except HandoverError as exc:
            raise AssistantError(str(exc),exc.status) from exc
        self.tick()
        return {'text':'已确认接收，事项已进入你的接续工作清单。' if result['status']=='accepted' else '已退回补充，交接尚未生效。'}

    async def drain(self):
        try:
            svc=await asyncio.to_thread(self.bot.runtime.work_handovers)
            def due():
                with svc.connect() as db:
                    return [dict(r) for r in db.execute("SELECT * FROM work_handover_outbox WHERE state IN ('queued','failed') AND next_at<=? ORDER BY rowid LIMIT 3",(time.time(),))]
            for row in await asyncio.to_thread(due):
                try:
                    mid=await self.deliver(svc,row)
                    def completed():
                        with svc.connect(True) as db:
                            db.execute("UPDATE work_handover_outbox SET state=?,message_id=?,error='' WHERE id=? AND state IN ('queued','failed')",('sent' if mid else 'cancelled',mid or '',row['id']))
                    await asyncio.to_thread(completed)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logging.warning('Handover notification pending: %s',row['id'])
                    def failed():
                        attempts=row['attempts']+1
                        held=isinstance(exc,HandoverDeliveryScopeError)
                        with svc.connect(True) as db:
                            db.execute("UPDATE work_handover_outbox SET attempts=?,state=?,next_at=?,error=? WHERE id=? AND state IN ('queued','failed')",
                                (attempts,'failed' if held or attempts>=6 else 'queued',time.time()+(1800 if held or attempts>=6 else min(300,10*2**attempts)),str(exc) if held else '飞书通知暂未送达，可在网页查看交接单。',row['id']))
                    await asyncio.to_thread(failed)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception('Handover notification recovery deferred')

    @staticmethod
    def _outbox_row(db,identity):
        row=dict(db.execute('SELECT * FROM work_handover_outbox WHERE id=?',(identity,)).fetchone())
        data=json.loads(row['payload'])
        if row['state']!='sent' and '_delivery' not in data:
            # Zero attempts can also mean a legacy crash or a manual retry reset.
            data['_delivery']={'sender_app_id':'','status':'uncertain'}
            row['payload']=json.dumps(data,ensure_ascii=False)
            db.execute('UPDATE work_handover_outbox SET payload=? WHERE id=?',(row['payload'],identity))
        return row,data

    @staticmethod
    def _check_scope(delivery,app_id):
        if not isinstance(delivery,dict) or delivery.get('status') not in {'not_sent','posting','uncertain'}:
            raise HandoverDeliveryScopeError('交接通知投递记录不完整，请先核验原消息；未重新发送。')
        if delivery['status']=='not_sent':
            return
        if not delivery.get('sender_app_id') or delivery['sender_app_id']!=app_id:
            raise HandoverDeliveryScopeError('原交接通知的应用或发送结果未确认，请先核验原消息；未换应用重发。')
        if time.time()-delivery.get('attempted_at',0)>3000:
            raise HandoverDeliveryScopeError('原交接通知已超出去重安全时限，请先核验原消息；未重新发送。')

    def _claim(self,svc,identity,app_id,target,fingerprint):
        with svc.connect(True) as db:
            row,data=self._outbox_row(db,identity)
            if row['state'] not in {'queued','failed'}:
                raise HandoverDeliveryScopeError('交接通知已结束，未重新发送。')
            delivery=data['_delivery']
            self._check_scope(delivery,app_id)
            unknown=delivery['status']!='not_sent'
            if unknown and (delivery.get('target')!=target or delivery.get('fingerprint')!=fingerprint):
                raise HandoverDeliveryScopeError('原交接通知接收人或内容已变化，未重新发送。')
            data['_delivery']={'sender_app_id':app_id,'status':'posting','target':target,'fingerprint':fingerprint,
                'attempted_at':delivery.get('attempted_at') if unknown else time.time(),'prior_unconfirmed':unknown}
            db.execute('UPDATE work_handover_outbox SET payload=? WHERE id=?',(json.dumps(data,ensure_ascii=False),identity))

    def _finish(self,svc,identity,app_id,*,receipt=None):
        with svc.connect(True) as db:
            row,data=self._outbox_row(db,identity)
            if row['state']=='sent':
                return
            delivery=data['_delivery']
            if delivery.get('sender_app_id')!=app_id or delivery.get('status')!='posting':
                raise HandoverDeliveryScopeError('交接通知应用归属已变化，请先核验原消息。')
            if receipt is not None:
                delivery.update(status='sent',**receipt)
                db.execute("UPDATE work_handover_outbox SET state='sent',message_id=?,error='',payload=? WHERE id=?",
                    (receipt['message_id'],json.dumps(data,ensure_ascii=False),identity))
            else:
                delivery['status']='uncertain' if delivery.get('prior_unconfirmed') else 'not_sent'
                db.execute('UPDATE work_handover_outbox SET payload=? WHERE id=?',(json.dumps(data,ensure_ascii=False),identity))

    async def deliver(self,svc,row):
        from .portal_service import external_real_write_guard
        def read():
            with svc.connect(True) as db:
                stored,data=self._outbox_row(db,row['id'])
                current=svc._read(db,stored['handover_id'])
                previous=db.execute("SELECT message_id,payload,state FROM work_handover_outbox WHERE handover_id=? AND kind='invite' AND version=?",(data['id'],data.get('sent_version',0))).fetchone()
                return stored,data,current,dict(previous) if previous else {}
        row,data,current,previous=await asyncio.to_thread(read)
        if row['state']=='sent':
            return row['message_id']
        credentials=await asyncio.to_thread(current_app_credentials)
        app_id=credentials[0]
        self._check_scope(data.get('_delivery'),app_id)
        messenger=getattr(self.bot,'messenger',None)
        if (not active_bot_matches(self.bot,credentials,messenger)
                or not external_real_write_guard()['real_write_allowed'] or not self.bot.messenger):
            raise HandoverError('飞书智能体与当前Qt应用尚未就绪。',503)
        if row['kind']=='invite' and (current['status']!='pending' or current.get('sent_version')!=row['version']):
            return None
        if row['kind']=='result' and current.get('sent_version')!=data.get('sent_version'):
            return None
        target=await asyncio.to_thread(svc.actor_by_person,row['target']) if row['target']!=data['owner']['id'] else await asyncio.to_thread(self.bot.runtime.work_handover_actor,data['owner']['open_id'],data['owner']['name'])
        if any(not authorized(target,i['scope'],i.get('module','')) for i in data['items']):
            raise HandoverError('接收人权限已变化，通知未发送。',403)
        base=await asyncio.to_thread(self.bot.runtime.service._critical_guard_public_base_url)
        content=card(data,base)
        if row['kind']=='refresh':
            if not previous or not previous['message_id']:
                return None
            owner=(json.loads(previous['payload']).get('_delivery') or {}).get('sender_app_id')
            if previous['state']!='sent' or owner!=app_id:
                raise HandoverDeliveryScopeError('原交接卡片不属于当前应用，未跨应用更新原消息。')
            receive_type,receive_id='message_id',previous['message_id']
        else:
            receive_type,receive_id=await asyncio.to_thread(self.recipient_id,target)
            pattern={'open_id':r'ou_[A-Za-z0-9]+','union_id':r'on_[A-Za-z0-9]+'}.get(receive_type)
            if not pattern or not re.fullmatch(pattern,str(receive_id or '')):
                raise HandoverError('交接通知接收人身份无效，尚未发送。',403)
        client=await messenger.client()
        headers=await messenger.headers()
        if (await asyncio.to_thread(current_app_credentials)!=credentials or not active_bot_matches(self.bot,credentials,messenger)
                or not external_real_write_guard()['real_write_allowed']):
            raise HandoverError('Qt应用配置或真实写入权限已变化，尚未发送。',503)
        fingerprint=hashlib.sha256(json.dumps(content,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        await asyncio.to_thread(self._claim,svc,row['id'],app_id,[receive_type,receive_id],fingerprint)
        if (await asyncio.to_thread(current_app_credentials)!=credentials or not active_bot_matches(self.bot,credentials,messenger)
                or not external_real_write_guard()['real_write_allowed']):
            await asyncio.to_thread(self._finish,svc,row['id'],app_id)
            raise HandoverError('Qt应用配置或真实写入权限已变化，尚未发送。',503)
        if row['kind']=='refresh':
            response=await client.patch('https://open.feishu.cn/open-apis/im/v1/messages/'+receive_id,headers=headers,json={'content':json.dumps(content,ensure_ascii=False)})
        else:
            response=await client.post('https://open.feishu.cn/open-apis/im/v1/messages',params={'receive_id_type':receive_type},headers=headers,
                json={'receive_id':receive_id,'msg_type':'interactive','content':json.dumps(content,ensure_ascii=False),'uuid':hashlib.sha256(row['id'].encode()).hexdigest()[:32]})
        result=response.json()
        if (await asyncio.to_thread(current_app_credentials)!=credentials or not active_bot_matches(self.bot,credentials,messenger) or response.status_code==408
                or not 200<=response.status_code<500 or 300<=response.status_code<400
                or not isinstance(result,dict) or type(result.get('code')) is not int
                or response.status_code>=400 and result['code']==0):
            raise HandoverError('交接通知响应不完整或应用已变化，发送结果未确认。',503)
        if result['code']!=0:
            await asyncio.to_thread(self._finish,svc,row['id'],app_id)
            raise HandoverError('飞书明确拒绝交接通知。',503)
        mid=receive_id if row['kind']=='refresh' else (result.get('data') or {}).get('message_id')
        if not isinstance(mid,str) or not re.fullmatch(r'om_[A-Za-z0-9]+',mid):
            raise HandoverError('交接通知缺少有效消息回执，发送结果未确认。',503)
        await asyncio.to_thread(self._finish,svc,row['id'],app_id,receipt={'message_id':mid,'accepted_at':time.time()})
        return mid

    def recipient_id(self,target):
        from .feishu_delivery import current_recipient_open_id, FeishuDeliveryError
        try:
            return 'open_id',current_recipient_open_id(self.bot.runtime,target['open_id'],self.bot.config.get('app_id',''),record_id=target['id'])
        except FeishuDeliveryError as exc:
            raise HandoverError('交接通知接收人或内容已变化：'+str(exc),403) from exc

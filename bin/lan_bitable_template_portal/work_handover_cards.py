"""Durable handover invitations over the existing Feishu assistant connection."""
import asyncio
import hashlib
import json
import logging
import re
import time

from openclaw_service.assistant.lighthouse_ai import AssistantError
from .work_handover import HandoverError, authorized

STATUS = {'draft':'草稿','pending':'待接收','accepted':'已接收','returned':'退回补充','withdrawn':'已撤回'}


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
                            db.execute("UPDATE work_handover_outbox SET state=?,message_id=?,error='' WHERE id=?",('sent' if mid else 'cancelled',mid or '',row['id']))
                    await asyncio.to_thread(completed)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logging.warning('Handover notification pending: %s',row['id'])
                    def failed():
                        attempts=row['attempts']+1
                        with svc.connect(True) as db:
                            db.execute('UPDATE work_handover_outbox SET attempts=?,state=?,next_at=?,error=? WHERE id=?',
                                (attempts,'failed' if attempts>=6 else 'queued',time.time()+(1800 if attempts>=6 else min(300,10*2**attempts)),'飞书通知暂未送达，可在网页查看交接单。',row['id']))
                    await asyncio.to_thread(failed)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception('Handover notification recovery deferred')

    async def deliver(self,svc,row):
        from .portal_service import external_real_write_guard
        if not external_real_write_guard()['real_write_allowed'] or not self.bot.messenger:
            raise HandoverError('飞书智能体暂未就绪。',503)
        def read():
            with svc.connect() as db:
                current=svc._read(db,row['handover_id'])
                data=json.loads(row['payload'])
                previous=db.execute("SELECT message_id FROM work_handover_outbox WHERE handover_id=? AND kind='invite' AND version=?",(data['id'],data.get('sent_version',0))).fetchone()
                return data,current,previous[0] if previous else ''
        data,current,previous=await asyncio.to_thread(read)
        if row['kind']=='invite' and (current['status']!='pending' or current.get('sent_version')!=row['version']):
            return None
        if row['kind']=='result' and current.get('sent_version')!=data.get('sent_version'):
            return None
        target=await asyncio.to_thread(svc.actor_by_person,row['target']) if row['target']!=data['owner']['id'] else await asyncio.to_thread(self.bot.runtime.work_handover_actor,data['owner']['open_id'],data['owner']['name'])
        if any(not authorized(target,i['scope'],i.get('module','')) for i in data['items']):
            raise HandoverError('接收人权限已变化，通知未发送。',403)
        base=await asyncio.to_thread(self.bot.runtime.service._critical_guard_public_base_url)
        content=card(data,base)
        messenger=self.bot.messenger
        client=await messenger.client()
        headers=await messenger.headers()
        if row['kind']=='refresh':
            if not previous:
                return None
            response=await client.patch('https://open.feishu.cn/open-apis/im/v1/messages/'+previous,headers=headers,json={'content':json.dumps(content,ensure_ascii=False)})
            if response.json().get('code')!=0:
                raise HandoverError('卡片状态更新未完成。',503)
            return previous
        receive_type,receive_id=await asyncio.to_thread(self.recipient_id,target)
        response=await client.post('https://open.feishu.cn/open-apis/im/v1/messages',params={'receive_id_type':receive_type},headers=headers,
            json={'receive_id':receive_id,'msg_type':'interactive','content':json.dumps(content,ensure_ascii=False),'uuid':hashlib.sha256(row['id'].encode()).hexdigest()[:32]})
        result=response.json()
        mid=(result.get('data') or {}).get('message_id')
        if result.get('code')!=0 or not mid:
            raise HandoverError('交接卡片未送达。',503)
        return mid

    def recipient_id(self,target):
        prefix=str(self.bot.config.get('app_id') or '')+':'
        bindings=self.bot.runtime.state_store.list_documents('feishu_assistant_sender',key_prefix=prefix)
        identities=set()
        for row in bindings:
            person=row.get('payload') or {}
            if person.get('matched_by') not in {'record_id','union_id','admin'}:
                continue
            identity=str(row.get('key') or '').removeprefix(prefix)
            if person.get('open_id')==target['open_id'] and person.get('record_id')==target['id'] and re.fullmatch(r'ou_[A-Za-z0-9]+',identity):
                identities.add(identity)
        if len(identities)==1:
            return 'open_id',identities.pop()
        return 'union_id',self.bot.permission_cards.union_id(target['open_id'])

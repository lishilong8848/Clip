"""One portal-owned scheduler; use the existing assistant and Feishu reply pipelines."""
import asyncio
import copy
import logging
import secrets
import time
import uuid

import httpx
from fastapi import Request

from openclaw_service.assistant.lighthouse_ai import AssistantError
from .assistant_schedules import ScheduledTasks, local_time
from .portal_auth import AUTH_COOKIE_NAME


class AssistantScheduler:
    def __init__(self, controller, runtime, ready):
        self.controller, self.runtime, self.ready = controller, runtime, ready
        self.tasks = ScheduledTasks(runtime.state_store)
        self.worker = uuid.uuid4().hex
        self.wake = asyncio.Event()
        self.loop = None
        self.running = set()
        self.closing = False

    async def db(self, function, *args, **kwargs):
        task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # Cancellation cannot stop SQLite running in a thread. Drain it before
            # shutdown releases the database directory or a replacement claim.
            await asyncio.gather(task, return_exceptions=True)
            raise

    async def manage(self, context, actor, payload):
        result = await self.db(self.tasks.dispatch, context, actor, payload)
        self.wake.set()
        return result

    def validate(self, schedule):
        task = self.runtime.state_store.get_document('assistant_schedules', 'task:' + schedule['id'])
        if not task or task['status'] != 'active' or task['revision'] != schedule['revision']:
            raise AssistantError('定时任务已暂停、修改或取消。', 409)
        if task['spec'].get('until') and local_time(task['spec']['until']).timestamp() < time.time():
            raise AssistantError('定时任务已到截止时间，未继续执行。', 409)
        if schedule.get('run_id'):
            job = self.runtime.state_store.get_document('assistant_schedules', 'run:' + schedule['run_id'])
            if (not job or job.get('claim_token') != schedule.get('claim_token')
                    or job['status'] != 'running' or job.get('lease_until', 0) <= time.time()
                    or task.get('active_run') != job['id']):
                raise AssistantError('定时任务领取已失效，未继续执行。', 409)

    def start(self):
        if self.loop is None:
            self.loop = asyncio.create_task(self.run())

    async def close(self):
        self.closing = True
        if self.loop:
            self.loop.cancel()
        for job in tuple(self.running):
            job.cancel()
        await asyncio.gather(*([self.loop] if self.loop else []), *tuple(self.running), return_exceptions=True)

    async def run(self):
        while not self.closing:
            try:
                await self.db(self.tasks.recover_protocol_failures)
                while len(self.running) < 4:
                    claimed = await self.db(self.tasks.claim, self.worker)
                    if not claimed:
                        break
                    task = asyncio.create_task(self.execute(*claimed))
                    self.running.add(task)
                    task.add_done_callback(self.running.discard)
                self.wake.clear()
                try:
                    await asyncio.wait_for(self.wake.wait(), 15)
                except asyncio.TimeoutError:
                    pass
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger(__name__).exception('Assistant schedule scan failed; durable jobs retained')
                await asyncio.sleep(15)

    async def request_context(self, task, job):
        from .lighthouse_bridge import actor_for
        auth = self.runtime.auth_manager
        profile = await self.db(auth.account_profile, task['owner'])
        if not profile.get('enabled') or await self.db(auth._open_id_explicitly_disabled, task['owner']):
            raise AssistantError('定时任务账号已停用或身份待核对。', 403)
        origin = task['origin']
        message = None
        if origin['route']:
            bot = self.runtime.feishu_assistant_channel
            message = {**copy.deepcopy(origin['route']), 'question': task['spec']['instruction'],
                       'message_id': 'schedule_' + job['id'], 'reply_to_message_id': origin['route']['message_id'],
                       '_scheduled_task': True}
            request, actor = await bot._request_context(message)
        else:
            token = secrets.token_urlsafe(32)
            session = {'user': {'open_id': task['owner'], 'name': origin.get('name', '')},
                       'role': await self.db(auth.role_for_open_id, task['owner']),
                       'allowed_scopes': await self.db(auth.scopes_for_open_id, task['owner']),
                       'created_at_ts': time.time(), 'expires_at': time.time() + 1800, 'source': 'assistant_schedule'}
            await self.db(auth._state_store.put_auth_session, auth._secret_hash(token), session)
            port = self.controller.bound_port or self.controller.preferred_port
            headers = [(b'host', f'127.0.0.1:{port}'.encode()), (b'cookie', f'{AUTH_COOKIE_NAME}={token}'.encode())]
            if origin.get('client_id'):
                headers.append((b'x-lighthouse-client', origin['client_id'].encode()))
            request = Request({'type': 'http', 'method': 'POST', 'path': '/api/assistant/chat',
                'scheme': 'http', 'query_string': b'', 'server': ('127.0.0.1', port), 'client': ('127.0.0.1', 0),
                'headers': headers, 'state': {}})
            actor = await actor_for(self.controller, self.runtime, request)
        if actor['id'] != task['owner'] or (actor.get('_feishu_topic') or {}).get('id', '') != origin['topic_id']:
            raise AssistantError('定时任务原账号或话题已变化，未转发至其他会话。', 403)
        schedule = {'id': task['id'], 'revision': task['revision'], 'mode': task['spec']['kind'],
                    'run_id': job['id'], 'claim_token': job['claim_token']}
        await self.db(self.validate, schedule)
        request.state.assistant_schedule = schedule
        request.state.assistant_schedule_id = task['id']
        actor.update(_scheduled_task=task['id'], _scheduled_mode=task['spec']['kind'],
                     _scheduled_run_id=job['id'], _execution_lane='schedule:' + task['id'])
        return request, actor, message

    async def call(self, request, actor, path, body=None, params=None):
        client, authority = await self.ready()
        context = authority.context(request, actor)
        headers = {'Authorization': 'Bearer ' + client.key, 'x-clipflow-instance': client.instance,
                   'x-clipflow-lease': client.lease, 'x-clipflow-context': context}
        completed = False
        try:
            http = await client.http_client()
            response = await http.request('POST' if body is not None else 'GET',
                f"http://127.0.0.1:{client.descriptor['port']}/api/assistant/" + path,
                headers=headers, json=body, params=params, timeout=180)
            try:
                result = response.json()
            except ValueError:
                raise AssistantError('定时任务响应未确认，将继续读取原任务。', 503) from None
            if not isinstance(result, dict):
                raise AssistantError('定时任务响应未确认，将继续读取原任务。', 503)
            if not result.get('ok'):
                raise AssistantError(result.get('error') or '定时任务处理未完成。', response.status_code)
            data = result.get('data') or {}
            completed = path != 'chat' or not any(t.get('status') in {'pending', 'running', 'queued'} for t in data.get('turns', []))
            return data
        finally:
            # A lost chat response may still own a live model run; retain its expiring context.
            if path != 'chat' or completed:
                authority.contexts.pop(context, None)

    async def execute(self, task, job):
        claim = job['claim_token']
        parent = asyncio.current_task()
        async def heartbeat():
            try:
                while True:
                    await asyncio.sleep(30)
                    await self.db(self.tasks.checkpoint, job, claim)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Stop this attempt before another worker can replace an expired claim.
                parent.cancel()
        pulse = asyncio.create_task(heartbeat())
        try:
            request, actor, message = await self.request_context(task, job)
            operation = 'schedule_' + job['id']
            conversation = await self.call(request, actor, 'conversation', params={'operation_id': operation})
            if conversation.get('conversation_id') != task['origin']['conversation_id']:
                raise AssistantError('原会话已清空或更换，定时任务已暂停，未写入其他会话。', 409)
            turn = next((row for row in conversation.get('turns', []) if row.get('operation_id') == operation), None)
            if not job.get('reply'):
                # A disconnected POST can still own a shielded upstream run. Keep its
                # claim/context alive and read the same operation instead of revoking it.
                deadline = time.monotonic() + 1200
                read_again = False
                while not turn or turn.get('status') not in {'completed', 'failed', 'stopped', 'cancelled', 'superseded'}:
                    if time.monotonic() >= deadline:
                        raise AssistantError('原定时回答仍未结束，已暂停后续执行，请核对原会话。', 409)
                    await self.db(self.tasks.checkpoint, job, claim)
                    try:
                        if read_again:
                            conversation = await self.call(request, actor, 'conversation', params={'operation_id': operation})
                            if conversation.get('conversation_id') != task['origin']['conversation_id']:
                                raise AssistantError('原会话已清空，定时任务已暂停。', 409)
                            turn = next((row for row in conversation.get('turns', []) if row.get('operation_id') == operation), None)
                        if not turn:
                            if not job.get('question'):
                                heading = f"【定时任务：{task['spec']['title']}】\n"
                                if job.get('phase') != 'submitted' and time.time() - job['due_at'] > 60:
                                    from datetime import datetime
                                    from .assistant_schedules import TZ
                                    heading += '补发提醒，原定时间：' + datetime.fromtimestamp(job['due_at'], TZ).strftime('%Y-%m-%d %H:%M:%S') + '（北京时间）。\n'
                                await self.db(self.tasks.checkpoint, job, claim,
                                    phase='submitted', question=heading + task['spec']['instruction'])
                            conversation = await self.call(request, actor, 'chat', {
                                'conversation_id': task['origin']['conversation_id'], 'operation_id': operation, 'attempt_id': operation,
                                'question': job['question']})
                            if conversation.get('conversation_id') != task['origin']['conversation_id']:
                                raise AssistantError('原会话已变化，定时任务已暂停。', 409)
                            turn = next((row for row in conversation.get('turns', []) if row.get('operation_id') == operation), None)
                        if turn and turn.get('status') in {'completed', 'failed', 'stopped', 'cancelled', 'superseded'}:
                            break
                    except AssistantError as exc:
                        if exc.status in {400, 401, 403, 404, 409, 410, 422}:
                            raise
                    except (OSError, asyncio.TimeoutError, httpx.TransportError):
                        pass
                    read_again = True
                    await asyncio.sleep(5)
                reply = None
                if message:
                    bot = self.runtime.feishu_assistant_channel
                    reply = await bot._turn_reply(message, request, actor, conversation, turn, task['spec']['instruction'])
                await self.db(self.tasks.checkpoint, job, claim,
                    phase='ready', reply=reply, answer_status=turn.get('status'), answer_error=turn.get('error', ''), completed_at=time.time())
            if message:
                reply = job['reply']
                if job.get('delivery_started') and not job.get('delivered_at') and time.time() - job['delivery_started'] >= 3600:
                    raise AssistantError('原飞书投递结果未确认，已暂停补发以避免重复消息，请核对原话题。', 409)
                bot = self.runtime.feishu_assistant_channel
                await self.db(self.validate, request.state.assistant_schedule)
                await bot._validate_delivery(message, reply, schedule=request.state.assistant_schedule)
                if reply.get('files'):
                    # Cached file keys still require fresh access checks on each retry.
                    await bot._prepare_reply_files(message, reply)
                await self.db(self.tasks.checkpoint, job, claim,
                    phase='delivering', delivery_started=job.get('delivery_started') or time.time())
                async def checkpoint():
                    await self.db(self.tasks.checkpoint, job, claim, reply=copy.deepcopy(reply))
                async def authorize():
                    await self.db(self.tasks.checkpoint, job, claim)
                    await self.db(self.validate, request.state.assistant_schedule)
                    await self.runtime.feishu_assistant_channel._validate_delivery(message, reply, schedule=request.state.assistant_schedule)
                await self.runtime.feishu_assistant_channel.messenger.send(message, reply, checkpoint=checkpoint, authorize=authorize)
                await self.db(self.tasks.checkpoint, job, claim, phase='delivered', delivered_at=job.get('delivered_at') or time.time())
                inbox = self.runtime.feishu_assistant_channel.inbox
                if inbox is not None:
                    await self.db(inbox.set, message['message_id'], 'done', reply)
                    await self.db(inbox.remember_public_reply, message, reply)
            await self.db(self.tasks.finish, job, claim)
            logging.getLogger(__name__).info('Assistant schedule completed id=%s run=%s channel=%s',
                task['id'], job['id'], 'feishu' if message else 'web')
        except asyncio.CancelledError:
            await self.db(self.tasks.finish, job, claim, error='执行已中断，将在领取恢复后继续原定时任务。')
            raise
        except Exception as exc:
            permanent = isinstance(exc, AssistantError) and exc.status in {400, 401, 403, 404, 409, 410, 422}
            error = str(exc) if isinstance(exc, AssistantError) else '定时任务连接暂未完成，保留原任务重试。'
            await self.db(self.tasks.finish, job, claim, error=error, permanent=permanent)
            from openclaw_service.diagnostics import capture
            capture('assistant', 'service_failed', run_id=job['id'], category=getattr(exc, 'category', '') or 'schedule_execution', error=exc)
            logging.getLogger(__name__).warning('Assistant schedule failed id=%s type=%s permanent=%s', task['id'], type(exc).__name__, permanent)
        finally:
            pulse.cancel()
            await asyncio.gather(pulse, return_exceptions=True)
            self.wake.set()

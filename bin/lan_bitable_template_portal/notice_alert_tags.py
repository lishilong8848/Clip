"""Durable, best-effort notice recommendations, outside the notice upload queue."""
import asyncio
import hashlib
import json
import logging
import threading
import time
import uuid
from collections import defaultdict

from fastapi import Request
from fastapi.responses import JSONResponse
from openclaw_service.assistant.lighthouse_alert_tagging import clean_tags, fallback_text, tag_text
from openclaw_service.assistant.lighthouse_sources import codes

NS = CHANNEL = 'notice_alert_tags'
NOTICE_TYPES = {'maintenance': '维保通告', 'change': '变更通告', 'repair': '设备检修',
                'polling': '设备轮巡', 'adjust': '设备调整', 'event': '事件通告'}
LOG = logging.getLogger(__name__)


def identity(*values):
    return hashlib.sha256(json.dumps(values, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


class NoticeAlertTags:
    def __init__(self, service, recommend, send, *, clock=time.time, store=None):
        self.service, self.store = service, store if store is not None else service._state_store
        self.recommend, self.send, self.clock = recommend, send, clock
        self.lock = threading.RLock()
        self.capacity = asyncio.Semaphore(2)

    def enqueue(self, notice, *, operation_id, target_record_id, request=None):
        action = {'upload': 'start'}.get(notice.get('action'), notice.get('action'))
        if action not in {'start', 'update'} or not target_record_id or not operation_id:
            return
        request = request or {}
        text = str(notice.get('text') or notice.get('content') or '').strip()
        kind = str(notice.get('notice_type') or '')
        if not text or not kind or notice.get('projection_superseded_by_terminal'):
            return
        scopes = sorted(codes(notice.get('building_codes') or notice.get('building') or notice.get('scope') or request.get('scope')))
        _, recipients, recipient_warning = self.service._recipients_for_building_codes(scopes, fallback_building=notice.get('building', ''))
        owner = str(request.get('_auth_open_id') or notice.get('_auth_open_id') or request.get('operator_open_id') or '')
        job_id = identity(kind, target_record_id, operation_id)
        with self.lock:
            if self.store.get_document(NS, 'job:' + job_id):
                return
            job = {'id': job_id, 'owner': owner, 'group': owner or 'qt:' + identity(recipients), 'text': text,
                   'title': str(notice.get('title') or notice.get('name') or kind), 'notice_type': kind,
                   'target_record_id': target_record_id, 'operation_id': operation_id, 'action': action,
                   'recipients': recipients, 'recipient_warning': recipient_warning,
                   'scopes': scopes, 'created_at': self.clock(), 'status': 'pending'}
            self.store.enqueue_outbox_event(CHANNEL, {'idempotency_key': job_id, 'job_id': job_id}, documents={
                (NS, 'job:' + job_id): job,
                (NS, 'notice:' + identity(kind, target_record_id)): {'job_id': job_id},
            })

    def latest(self, record_id, work_type, allowed):
        kinds = ['上电通告', '下电通告'] if work_type == 'power' else [NOTICE_TYPES.get(work_type, '')]
        items = []
        for kind in kinds:
            ref = self.store.get_document(NS, 'notice:' + identity(kind, record_id)) or {}
            job = self.store.get_document(NS, 'job:' + ref.get('job_id', '')) if ref else None
            # Native workbench lists a shared notice under each involved building.
            if job and job['scopes'] and set(job['scopes']) & codes(allowed):
                items.append(job)
        if not items:
            return None
        job = max(items, key=lambda row: row['created_at'])
        display = {}
        if job['status'] == 'failed':
            # Repair only the viewed notice. Keep the original notification
            # receipt/result immutable, including any in-flight delivery retry.
            key = 'display:' + job['id']
            with self.lock:
                display = self.store.get_document(NS, key) or {
                    'status': 'failed', 'retry_after': float(job.get('finished_at') or job['created_at']) + 60}
                if display['status'] == 'failed' and self.clock() >= display.get('retry_after', 0):
                    attempt = int(display.get('attempts', 0)) + 1
                    display = {**display, 'status': 'pending', 'attempts': attempt}
                    self.store.enqueue_outbox_event(CHANNEL, {
                        'idempotency_key': f"display:{job['id']}:{attempt}", 'job_id': job['id'], 'display_only': True},
                        documents={(NS, key): display})
        result = {key: job.get(key) for key in ('id', 'title', 'status', 'action', 'scopes', 'notice_type', 'target_record_id',
                                              'created_at', 'finished_at', 'tags', 'error', 'message_warning')}
        if job['status'] == 'failed':
            result.update({key: display[key] for key in ('status', 'tags', 'error', 'finished_at', 'retry_after') if key in display})
        # Old display overrides may still contain full rules or an outdated error.
        result['error'] = fallback_text(job['notice_type']) if result['status'] == 'failed' else ''
        result['tags'] = clean_tags(result.get('tags'))
        return result

    async def refresh_display(self, row):
        job_id = row['payload']['job_id']
        key = 'display:' + job_id
        async with self.capacity:
            job = await asyncio.to_thread(self.store.get_document, NS, 'job:' + job_id)
            ref = await asyncio.to_thread(self.store.get_document, NS, 'notice:' + identity(job['notice_type'], job['target_record_id'])) if job else None
            if job and ref and ref.get('job_id') == job_id:
                display = await asyncio.to_thread(self.store.get_document, NS, key) or {}
                if display.get('status') == 'ready':
                    await asyncio.to_thread(self.store.mark_outbox_event, row['id'], 'done')
                    return
                try:
                    result = await asyncio.wait_for(self.recommend(job['owner'], [
                        {name: job[name] for name in ('id', 'title', 'text', 'action')}]), 85)
                    if not isinstance(result, dict) or set(result) != {job_id} or not result[job_id]:
                        raise ValueError('Incomplete recommendation')
                    display.update(status='ready', tags=result[job_id], error='', finished_at=self.clock())
                except asyncio.CancelledError:
                    raise
                except Exception:
                    attempt = int(display.get('attempts', 1))
                    delay = (60, 300, 1800, 3600)[min(max(attempt - 1, 0), 3)]
                    display.update(status='failed', error=fallback_text(job['notice_type']), retry_after=self.clock() + delay)
                await asyncio.to_thread(self.store.put_document, NS, key, display)
            await asyncio.to_thread(self.store.mark_outbox_event, row['id'], 'done')

    def batch(self, jobs):
        batch_id = identity(jobs[0]['group'], sorted(job['id'] for job in jobs))
        batch = self.store.get_document(NS, 'batch:' + batch_id)
        if batch:
            return batch
        batch = {'id': batch_id, 'job_ids': [job['id'] for job in jobs], 'owner': jobs[0]['owner'], 'notifications': {}}
        self.store.put_documents(NS, {'batch:' + batch_id: batch, **{
            'job:' + job['id']: {**job, 'batch_id': batch_id} for job in jobs}})
        return batch

    async def process_batch(self, batch):
        async with self.capacity:
            jobs = await asyncio.to_thread(lambda: [self.store.get_document(NS, 'job:' + key) for key in batch['job_ids']])
            if not batch.get('generated'):
                result = None
                for attempt in range(2):
                    try:
                        result = await asyncio.wait_for(self.recommend(batch['owner'], [
                            {key: job[key] for key in ('id', 'title', 'text', 'action')} for job in jobs]), 85)
                        if not isinstance(result, dict) or set(result) != set(batch['job_ids']):
                            raise ValueError('Incomplete recommendation')
                        break
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        result = None
                        if not attempt:
                            await asyncio.sleep(3)
                for job in jobs:
                    job.update(status='ready' if result else 'failed', tags=result.get(job['id'], []) if result else [],
                               error='' if result else fallback_text(job['notice_type']),
                               finished_at=self.clock())
                batch['generated'] = True
                await asyncio.to_thread(self.store.put_documents, NS, {
                    'batch:' + batch['id']: batch, **{'job:' + job['id']: job for job in jobs}})
            return await asyncio.to_thread(self.notify, batch, jobs)

    def notify(self, batch, jobs):
        warnings = [job['recipient_warning'] for job in jobs if job.get('recipient_warning')]
        recipients = defaultdict(list)
        for job in jobs:
            for recipient in dict.fromkeys(job['recipients']):
                recipients[recipient].append(job)
        for recipient, selected in recipients.items():
            receipt = batch['notifications'].setdefault(recipient, {})
            if receipt.get('sent'):
                continue
            if receipt.get('attempted_at') and self.clock() - receipt['attempted_at'] > 3500:
                warnings.append('个人消息上次结果未确认且已超出去重时限，未重复发送。')
                continue
            chunks = ['通告推荐标签（仅供现场核对，不代表已给告警打标）']
            for job in selected:
                chunks.append(job['title'] + ' · ' + ('开始' if job['action'] == 'start' else '更新') + '\n' + (
                    tag_text(job['tags']) if job['status'] == 'ready' else fallback_text(job['notice_type'])))
            receipt.setdefault('attempted_at', self.clock())
            self.store.put_document(NS, 'batch:' + batch['id'], batch)
            try:
                ok, _, _ = self.send('\n\n'.join(chunks), [recipient],
                    message_uuid=str(uuid.uuid5(uuid.NAMESPACE_URL, batch['id'] + recipient)))
            except Exception:
                ok = False
            receipt['sent'] = bool(ok)
            if not ok:
                warnings.append('推荐标签个人消息发送未确认，后台将有限重试；通告业务不受影响。')
            self.store.put_document(NS, 'batch:' + batch['id'], batch)
        for job in jobs:
            job['message_warning'] = '；'.join(dict.fromkeys(warnings))
        self.store.put_documents(NS, {'job:' + job['id']: job for job in jobs})
        return not warnings

    async def tick(self):
        rows = await asyncio.to_thread(self.store.lease_outbox_events, CHANNEL, limit=12, lease_seconds=1800)
        display_rows = [row for row in rows if row['payload'].get('display_only')]
        rows = [row for row in rows if not row['payload'].get('display_only')]
        pending, batches = defaultdict(list), {}
        for row in rows:
            job = await asyncio.to_thread(self.store.get_document, NS, 'job:' + row['payload']['job_id'])
            if job.get('batch_id'):
                batch = await asyncio.to_thread(self.store.get_document, NS, 'batch:' + job['batch_id'])
                batches[batch['id']] = batch
            else:
                pending[job['group']].append(job)
        for jobs in pending.values():
            for start in range(0, len(jobs), 6):
                batch = await asyncio.to_thread(self.batch, jobs[start:start + 6])
                batches[batch['id']] = batch
        async def process(batch):
            try:
                ok = await self.process_batch(batch)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning('Notice tagging background task failed: %s', type(exc).__name__)
                ok = False
            for row in rows:
                if row['payload']['job_id'] in batch['job_ids']:
                    saved = await asyncio.to_thread(self.store.mark_outbox_event, row['id'], 'done' if ok else 'pending',
                        error='' if ok else '标签后台任务未完成；不影响通告', max_attempts=3)
                    if saved and saved['status'] == 'failed':
                        key = 'job:' + row['payload']['job_id']
                        job = await asyncio.to_thread(self.store.get_document, NS, key)
                        job['message_warning'] = '标签后台任务多次未完成，已停止自动重试；通告业务不受影响。'
                        if job['status'] == 'pending':
                            job.update(status='failed', error=fallback_text(job['notice_type']))
                        await asyncio.to_thread(self.store.put_document, NS, key, job)
        await asyncio.gather(*(process(batch) for batch in batches.values()), *(self.refresh_display(row) for row in display_rows))


def install_notice_alert_tag_routes(app, controller, runtime, recommend):
    from .server import _send_text_to_open_ids_guarded
    tagging = NoticeAlertTags(runtime.service, recommend, _send_text_to_open_ids_guarded, store=runtime.state_store)
    runtime.notice_alert_tags = tagging
    worker = None

    async def run():
        try:
            await asyncio.to_thread(runtime.state_store.release_outbox_leases, CHANNEL)
        except Exception as exc:
            LOG.warning('Notice tagging recovery deferred: %s', type(exc).__name__)
        while True:
            await asyncio.sleep(3)
            try:
                await tagging.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning('Notice tagging queue unavailable: %s', type(exc).__name__)
                await asyncio.sleep(15)

    async def startup():
        nonlocal worker
        worker = asyncio.create_task(run())

    async def shutdown():
        if worker:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    app.add_event_handler('startup', startup)
    app.add_event_handler('shutdown', shutdown)

    @app.get('/api/notice-alert-tags')
    async def status(request: Request):
        session = controller._current_session(request)
        if not session or session.get('is_guest') or str(session.get('role') or (session.get('user') or {}).get('role')).lower() == 'guest':
            return JSONResponse({'ok': False, 'error': '请登录后查看通告标签。'}, status_code=401)
        allowed = runtime.auth_manager.session_scopes(session)
        try:
            result = await asyncio.to_thread(tagging.latest, request.query_params.get('target_record_id', '')[:128],
                request.query_params.get('work_type', '')[:30], allowed)
            return controller._json_ok(request, session, result)
        except Exception:
            return JSONResponse({'ok': False, 'error': '推荐标签暂不可用，通告业务不受影响。'}, status_code=503)

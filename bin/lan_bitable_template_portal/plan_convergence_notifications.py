"""One post-send repair check, with durable and idempotent personal notifications."""
import asyncio
import hashlib
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from .plan_convergence_maintenance import ongoing_records

CHANNEL = 'notice_plan_checks'
LOG = logging.getLogger(__name__)


class NoticePlanChecks:
    def __init__(self, service, store, check, send):
        self.service, self.store, self.check, self.send = service, store, check, send
        self.stopped = False

    def enqueue(self, notice, *, operation_id, target_record_id, request=None):
        action = {'upload': 'start', 'upload_replace': 'update'}.get(notice.get('action'), notice.get('action'))
        if notice.get('notice_type') not in {'设备检修', '检修通告'} or action not in {'start', 'update'}:
            return
        if not operation_id or not target_record_id or notice.get('projection_superseded_by_terminal'):
            return
        scopes = notice.get('building_codes') or self.service._building_codes_from_value(notice.get('building') or (request or {}).get('scope'))
        _, recipients, warning = self.service._recipients_for_building_codes(scopes, fallback_building=notice.get('building', ''))
        snapshot = ongoing_records([{**notice, 'title': notice.get('title') or notice.get('name', ''), 'work_type': 'repair', 'target_record_id': target_record_id}])[0]
        identity = hashlib.sha256(f'{target_record_id}:{operation_id}'.encode()).hexdigest()
        if self.store.get_document(CHANNEL, identity):
            return
        job = {'id': identity, 'target_record_id': target_record_id, 'record': snapshot,
               'action': action, 'status': 'pending', 'recipients': list(dict.fromkeys(recipients)),
               'warning': warning, 'created_at': time.time(), 'receipts': {}}
        self.store.enqueue_outbox_event(CHANNEL, {'idempotency_key': identity, 'job_id': identity}, documents={
            (CHANNEL, identity): job, (CHANNEL, 'latest:' + target_record_id): {'job_id': identity}})

    @staticmethod
    def message(job):
        record = job['record']
        lines = ['【检修通告自动核对】', record['name'], '开始' if job['action'] == 'start' else '更新']
        if job['status'] == 'failed':
            lines.append('核对未完成：请检查主机 VPN 及智航登录，在计划收敛审查中重新核对。通告已正常发送。')
        else:
            hits = job.get('result', {}).get('records', [{}])[0].get('hits', [])
            lines.append(f'找到 {len(hits)} 条候选屏蔽记录。' if hits else '未找到匹配的屏蔽中记录，请现场核实是否已按要求屏蔽。')
            lines.extend(f"{hit['blockName'] or hit['blockId']}：{hit['reason']}" for hit in hits[:5])
            if len(hits) > 5:
                lines.append('其余候选请到计划收敛审查查看。')
            lines.append('此结果按房间、设备与名称匹配，仅供核对，不代表告警规则已完整覆盖。')
        return '\n'.join(lines)

    def process(self, row):
        job = self.store.get_document(CHANNEL, row['payload']['job_id'])
        if not job or self.stopped:
            return
        if job['status'] == 'pending':
            try:
                job['result'] = self.check(job['record'])
                job['status'] = 'ready'
            except Exception as exc:
                job.update(status='failed', error=type(exc).__name__)
                LOG.warning('检修自动核对未完成，通告不受影响: %s', type(exc).__name__)
            job['checked_at'] = time.time()
            self.store.put_document(CHANNEL, job['id'], job)
        complete = bool(job['recipients'])
        for recipient in job['recipients']:
            if self.stopped:
                return
            receipt = job['receipts'].setdefault(recipient, {})
            if receipt.get('sent'):
                continue
            if receipt.get('attempted_at') and time.time() - receipt['attempted_at'] > 3500:
                complete = False
                continue
            receipt.setdefault('attempted_at', time.time())
            self.store.put_document(CHANNEL, job['id'], job)
            try:
                result = self.send(self.message(job), [recipient], message_uuid=str(uuid.uuid5(uuid.NAMESPACE_URL, CHANNEL + job['id'] + recipient)))
                receipt['sent'] = bool(result[0])
            except Exception:
                receipt['sent'] = False
            complete = complete and receipt['sent']
            self.store.put_document(CHANNEL, job['id'], job)
        self.store.mark_outbox_event(row['id'], 'done' if complete else 'pending', error='' if complete else '核对通知待补发', max_attempts=3)


def install_notice_plan_checks(app, runtime, get_service):
    from .server import _send_text_to_open_ids_guarded
    from upload_event_module.services.process_lifetime import lower_current_thread_priority
    checks = NoticePlanChecks(runtime.service, runtime.state_store,
        lambda record: get_service().maintenance_check(records=[record]), _send_text_to_open_ids_guarded)
    runtime.notice_plan_checks = checks
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='NoticePlanCheck', initializer=lower_current_thread_priority)
    task = None

    async def run():
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(pool, lambda: runtime.state_store.release_outbox_leases(CHANNEL))
        except Exception as exc:
            LOG.warning('检修自动核对队列恢复稍后重试: %s', type(exc).__name__)
        while True:
            await asyncio.sleep(5)
            try:
                rows = await loop.run_in_executor(pool, lambda: runtime.state_store.lease_outbox_events(CHANNEL, limit=1, lease_seconds=300))
                for row in rows:
                    await loop.run_in_executor(pool, checks.process, row)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning('检修自动核对队列暂不可用: %s', type(exc).__name__)
                await asyncio.sleep(30)

    async def startup():
        nonlocal task
        task = asyncio.create_task(run())

    async def shutdown():
        checks.stopped = True
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        pool.shutdown(wait=False, cancel_futures=True)

    app.add_event_handler('startup', startup)
    app.add_event_handler('shutdown', shutdown)

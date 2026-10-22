"""Durable post-send repair/change checks and connection recovery notifications."""
import asyncio
import hashlib
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from .plan_convergence_maintenance import ongoing_records, excluded_buildings, business_fingerprint

CHANNEL = 'notice_plan_checks'
LOG = logging.getLogger(__name__)
RETRY_SECONDS = 300
DELIVERY_WINDOW = 3500
OUTAGE_KEY = 'connection-outage'


class IncompleteCheckResult(ValueError):
    """An incomplete per-record response is not proof the shared connection is down."""


class NoticePlanChecks:
    def __init__(self, service, store, check, send, *, get_convergence=None, get_engineers=None):
        self.service, self.store, self.check, self.send = service, store, check, send
        self.get_convergence = get_convergence
        self.get_engineers = get_engineers
        self.stopped = False

    def enqueue(self, notice, *, operation_id, target_record_id, request=None):
        action = {'upload': 'start', 'upload_replace': 'update'}.get(notice.get('action'), notice.get('action'))
        kind = notice.get('work_type') or {'设备检修': 'repair', '检修通告': 'repair',
            '变更通告': 'change', '设备变更': 'change'}.get(notice.get('notice_type'))
        if kind not in {'repair', 'change'} or action not in {'start', 'update'}:
            return
        if not operation_id or not target_record_id or notice.get('projection_superseded_by_terminal'):
            return
        scopes = notice.get('building_codes') or self.service._building_codes_from_value(notice.get('building') or (request or {}).get('scope'))
        records = ongoing_records([{**notice, 'title': notice.get('title') or notice.get('name', ''),
            'work_type': kind, 'target_record_id': target_record_id}], work_type=kind)
        if not records:
            return
        snapshot = dict(records[0], work_type=kind)
        if excluded_buildings(snapshot):
            return
        # Preserve repair UUIDs already queued before the change-check upgrade.
        identity = hashlib.sha256(f'{"change:" if kind == "change" else ""}{target_record_id}:{operation_id}'.encode()).hexdigest()
        try:
            _, recipients, warning = self.service._recipients_for_building_codes(scopes, fallback_building=notice.get('building', ''))
        except Exception as exc:
            recipients, warning = [], type(exc).__name__
        job = {'id': identity, 'target_record_id': target_record_id, 'record': snapshot,
               'action': action, 'status': 'pending', 'recipients': list(dict.fromkeys(recipients)),
               'duty_recipients': list(dict.fromkeys(recipients)),
               'work_type': kind, 'scopes': scopes, 'version': 2, 'warning': warning, 'created_at': time.time(), 'receipts': {}}
        with self.store._lock:
            if self.store.get_document(CHANNEL, identity):
                return
            self.store.enqueue_outbox_event(CHANNEL, {'idempotency_key': identity, 'job_id': identity}, documents={
                (CHANNEL, identity): job, (CHANNEL, self._latest_key(job)): {'job_id': identity}})

    @staticmethod
    def _latest_key(job):
        return 'latest:' + ('change:' if job.get('work_type') == 'change' else '') + job['target_record_id']

    def recover(self):
        self.store.release_outbox_leases(CHANNEL)
        # Old failures could already be done/failed in the outbox; requeue once locally.
        for document in self.store.list_documents(CHANNEL):
            job = document['payload']
            if not job.get('target_record_id') or job.get('version', 0) >= 2:
                continue
            if job.get('status') not in {'failed', 'pending', 'ready'}:
                continue
            if job['status'] == 'ready' and job.get('recipients') and all(
                    job.get('receipts', {}).get(person, {}).get('sent') for person in job['recipients']):
                continue
            if job['status'] == 'failed':
                job.update(status='pending', failure_receipts=job.get('receipts', {}),
                           receipts={}, receipt_suffix=':recovered')
            job.update(version=2, work_type=job.get('work_type') or 'repair')
            job['record']['work_type'] = job['work_type']
            self.store.enqueue_outbox_event(CHANNEL,
                {'idempotency_key': 'recovery:' + job['id'], 'job_id': job['id']},
                documents={(CHANNEL, job['id']): job})

    @staticmethod
    def message(job):
        record = job['record']
        label = '变更' if job.get('work_type') == 'change' else '检修'
        lines = [f'【{label}通告自动核对】', record['name'], '开始' if job['action'] == 'start' else '更新']
        if job['status'] != 'ready':
            lines.append('核对未完成：请检查主机 VPN 及智航登录，恢复后自动重新核对。通告已正常发送。')
        else:
            hits = job.get('result', {}).get('records', [{}])[0].get('hits', [])
            lines.append(f'找到 {len(hits)} 条候选屏蔽记录。' if hits else '未找到匹配的屏蔽中记录，请现场核实是否已按要求屏蔽。')
            lines.extend(f"{hit.get('blockName') or hit['blockId']}：{hit['reason']}" for hit in hits[:5])
            if len(hits) > 5:
                lines.append('其余候选请到计划收敛审查查看。')
            lines.append('此结果按房间、设备与名称匹配，仅供核对，不代表告警规则已完整覆盖。')
        return '\n'.join(lines)

    def _defer(self, row, reason):
        # A lease is the existing queue's durable delay; pending has no available_at.
        self.store.mark_outbox_event(row['id'], 'leased', error=reason, count_attempt=False)

    def _failed_check(self, job, exc):
        now = time.time()
        job.update(status='pending', error=type(exc).__name__, last_attempt_at=now,
                   check_attempts=int(job.get('check_attempts') or 0) + 1)
        if isinstance(exc, IncompleteCheckResult):
            self.store.put_document(CHANNEL, job['id'], job)
            return
        outage = self.store.get_document(CHANNEL, OUTAGE_KEY) or {}
        if outage.get('active'):
            outage['retry_at'] = now + RETRY_SECONDS
            self.store.put_documents(CHANNEL, {job['id']: job, OUTAGE_KEY: outage})
            return
        from .portal_service import BUILDING_OPEN_ID_MAP
        ident = 'outage:' + uuid.uuid4().hex
        outage = {'id': ident, 'active': True, 'started_at': now, 'retry_at': now + RETRY_SECONDS}
        alert = {'id': ident, 'recipients': [BUILDING_OPEN_ID_MAP['H']], 'receipts': {},
                 'text': '\n'.join(['【计划收敛自动核对故障提醒】', '以下为故障发生时的状态，消息可能延迟送达：', self.message(job),
                     '已发送的变更/检修通告核对任务已保留，不会重复发送业务通告。',
                     '建议检查：运行灯塔的电脑是否已连接 VPN；智航是否已登录且认证未过期；智航接口是否可访问。',
                     '修复后程序每5分钟自动重试未核对任务；同一次故障不重复告警。',
                     '若 VPN/智航现已恢复，无需重新发送通告，程序会自动继续核对。'])}
        event_id = self.store.enqueue_outbox_event(CHANNEL,
            {'idempotency_key': ident, 'outage_id': ident}, documents={
                (CHANNEL, job['id']): job, (CHANNEL, OUTAGE_KEY): outage, (CHANNEL, ident): alert})
        self._process_alert({'id': event_id, 'payload': {'outage_id': ident}})

    @staticmethod
    def _validate_result(result, record):
        rows = result.get('records') if isinstance(result, dict) else None
        if (not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict)
                or not isinstance(rows[0].get('hits'), list) or result.get('error')
                or rows[0].get('check_status') not in (None, 'ready')
                or rows[0].get('check_error')
                or rows[0].get('record_id', record['record_id']) != record['record_id']):
            raise IncompleteCheckResult('计划收敛核对结果不完整，尚未取得匹配结论')
        if any(not isinstance(hit, dict) or not hit.get('blockId') or not isinstance(hit.get('reason'), str)
               for hit in rows[0]['hits']):
            raise IncompleteCheckResult('计划收敛候选记录不完整，尚未取得匹配结论')

    def _publish_change_result(self, job):
        if job.get('work_type') != 'change':
            return
        # Share the manual-check lock so a slower check cannot replace a newer result.
        lock = self.get_convergence()._persist_lock if self.get_convergence else self.store._lock
        with lock:
            key = 'change:' + job['target_record_id']
            old = self.store.get_document('plan_convergence_checks', key) or {}
            if (old.get('checked_at') or 0) >= job['checked_at']:
                return
            self.store.put_document('plan_convergence_checks', key, {
                'work_type': 'change', 'source': 'auto', 'check_source': 'auto', 'check_status': 'ready',
                'checked_at': job['checked_at'], 'hits': job['result']['records'][0]['hits'],
                'fingerprint': business_fingerprint(job['record'])})

    def _deliver(self, document, message, *, can_send=None):
        complete = bool(document.get('recipients'))
        for recipient in document.get('recipients', []):
            if self.stopped:
                return False
            receipt = document.setdefault('receipts', {}).setdefault(recipient, {})
            if receipt.get('sent'):
                continue
            if can_send and not can_send(recipient):
                complete = False
                continue
            if receipt.get('attempted_at') and time.time() - receipt['attempted_at'] > DELIVERY_WINDOW:
                receipt['delivery_uncertain'] = True
                self.store.put_document(CHANNEL, document['id'], document)
                complete = False
                continue
            receipt.setdefault('message_uuid', str(uuid.uuid5(uuid.NAMESPACE_URL,
                CHANNEL + document['id'] + document.get('receipt_suffix', '') + recipient)))
            prior_unconfirmed = bool(receipt.get('attempted_at'))
            receipt.setdefault('attempted_at', time.time())
            receipt['attempts'] = int(receipt.get('attempts') or 0) + 1
            receipt['uncertain'] = True
            self.store.put_document(CHANNEL, document['id'], document)
            try:
                result = self.send(message, [recipient], message_uuid=receipt['message_uuid'])
                receipt['sent'] = bool(result[0])
                if receipt['sent']:
                    for key in ('error', 'failure_kind', 'code', 'delivery_uncertain'):
                        receipt.pop(key, None)
                for item in result[2] or []:
                    if item.get('open_id') == recipient:
                        receipt.update({key: item[key] for key in (
                            'message_id', 'chat_id', 'sender_app_id', 'accepted_at', 'failure_kind', 'code') if key in item})
                        # Explicit rejections never reached the recipient and can retry beyond the UUID window.
                        if not receipt['sent'] and not prior_unconfirmed and (
                                item.get('failure_kind') in {'bot_unavailable', 'not_sent', 'recipient_unavailable', 'blocked'}
                                or type(item.get('code')) is int and item['code'] != 0):
                            receipt.pop('attempted_at', None)
                receipt['uncertain'] = not receipt['sent'] and bool(receipt.get('attempted_at'))
            except Exception as exc:
                receipt.update(sent=False, uncertain=True, error=type(exc).__name__)
            complete = complete and receipt['sent']
            self.store.put_document(CHANNEL, document['id'], document)
        return complete

    def _refresh_recipients(self, job):
        # Old unfinished jobs only contained the original duty/admin recipients.
        duty = job.setdefault('duty_recipients', list(job.get('recipients', [])))
        scopes = job.get('scopes') or self.service._building_codes_from_value(job['record'].get('building'))
        if not duty:
            try:
                _, duty, warning = self.service._recipients_for_building_codes(scopes, fallback_building=job['record'].get('building', ''))
                job.update(duty_recipients=list(dict.fromkeys(duty)), warning=warning)
            except Exception as exc:
                job['warning'] = type(exc).__name__
        engineers, ready = [], True
        if self.get_engineers:
            try:
                engineers, job['engineer_warning'] = self.get_engineers(job['record'], scopes)
            except Exception as exc:
                ready = False
                job['engineer_warning'] = '工程师接收人待重新核对: ' + type(exc).__name__
        job.update(engineer_recipients=engineers, recipients=list(dict.fromkeys([*duty, *engineers])))
        self.store.put_document(CHANNEL, job['id'], job)
        return ready and bool(duty)

    def _can_send_result(self, job, recipient):
        if recipient in job.get('duty_recipients', []):
            return True
        # Recheck each unsent engineer; transfers during delivery/retries must take effect.
        try:
            scopes = job.get('scopes') or self.service._building_codes_from_value(job['record'].get('building'))
            engineers, _ = self.get_engineers(job['record'], scopes)
            return recipient in engineers
        except Exception:
            return False

    def _process_alert(self, row):
        alert = self.store.get_document(CHANNEL, row['payload']['outage_id'])
        if alert and not self.stopped:
            if self._deliver(alert, alert['text']):
                self.store.mark_outbox_event(row['id'], 'done')
            else:
                self._defer(row, 'H楼故障提醒待确认投递')

    def process(self, row):
        if row['payload'].get('outage_id'):
            self._process_alert(row)
            return
        job = self.store.get_document(CHANNEL, row['payload']['job_id'])
        if not job or self.stopped:
            return
        if job['status'] == 'pending':
            latest = self.store.get_document(CHANNEL, self._latest_key(job)) or {}
            excluded = excluded_buildings(job['record'])
            if excluded or latest.get('job_id') and latest['job_id'] != job['id']:
                job['status'] = 'skipped' if excluded else 'superseded'
                self.store.put_document(CHANNEL, job['id'], job)
                self.store.mark_outbox_event(row['id'], 'done')
                return
            outage = self.store.get_document(CHANNEL, OUTAGE_KEY) or {}
            if outage.get('active') and outage.get('retry_at', 0) > time.time():
                self._defer(row, '等待 VPN/智航恢复后重新核对')
                return
            try:
                checked_at = time.time()
                result = self.check(job['record'])
                self._validate_result(result, job['record'])
                job['result'] = result
                job['status'] = 'ready'
            except Exception as exc:
                self._failed_check(job, exc)
                reason = '核对结果不完整，等待自动重试' if isinstance(exc, IncompleteCheckResult) else '核对未完成，等待 VPN/智航恢复'
                self._defer(row, reason)
                LOG.warning('通告自动核对未完成，通告不受影响: %s', type(exc).__name__)
                return
            job.update(checked_at=checked_at, error='')
            self.store.put_documents(CHANNEL, {job['id']: job, OUTAGE_KEY: {**outage, 'active': False,
                'recovered_at': time.time()}})
        if job['status'] != 'ready':
            self.store.mark_outbox_event(row['id'], 'done')
            return
        self._publish_change_result(job)
        ready = self._refresh_recipients(job)
        delivered = self._deliver(job, self.message(job), can_send=lambda person: self._can_send_result(job, person))
        # Newly assigned engineers may appear while earlier recipients are being sent.
        if ready and delivered and self._refresh_recipients(job) and all(
                job.get('receipts', {}).get(person, {}).get('sent') for person in job['recipients']):
            self.store.mark_outbox_event(row['id'], 'done')
        else:
            self._defer(row, '核对已完成，通知待确认投递')


def install_notice_plan_checks(app, runtime, get_service):
    from .server import _send_text_to_open_ids_guarded
    from .plan_convergence_recipients import current_engineers
    from upload_event_module.services.process_lifetime import lower_current_thread_priority
    def check(record):
        service = get_service()
        method = service.change_check if record.get('work_type') == 'change' else service.maintenance_check
        return method(records=[record])
    checks = NoticePlanChecks(runtime.service, runtime.state_store, check,
        _send_text_to_open_ids_guarded, get_convergence=get_service,
        get_engineers=lambda record, scopes: current_engineers(runtime.auth_manager, record, scopes))
    runtime.notice_plan_checks = checks
    from .plan_convergence_send import NoticeConvergenceGuard
    from .plan_convergence_expiry import ConvergenceExpiryReminders
    guard = NoticeConvergenceGuard(runtime.service, runtime.state_store, get_service)
    runtime.service._notice_plan_guard = guard.check
    reminders = ConvergenceExpiryReminders(runtime.service, runtime.state_store, get_service, _send_text_to_open_ids_guarded)
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='NoticePlanCheck', initializer=lower_current_thread_priority)
    task = None

    async def run():
        loop = asyncio.get_running_loop()
        last_reminder = time.monotonic()
        recovered = False
        while True:
            await asyncio.sleep(5)
            try:
                if not recovered:
                    await loop.run_in_executor(pool, checks.recover)
                    recovered = True
                rows = await loop.run_in_executor(pool, lambda: runtime.state_store.lease_outbox_events(CHANNEL, limit=1, lease_seconds=RETRY_SECONDS))
                for row in rows:
                    await loop.run_in_executor(pool, checks.process, row)
                if time.monotonic() - last_reminder >= 60:
                    last_reminder = time.monotonic()
                    await loop.run_in_executor(pool, reminders.tick)
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

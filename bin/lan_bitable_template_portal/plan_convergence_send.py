"""Shared send-time guard: no write occurs before an explicit unmatched override."""
import hashlib
import json
import secrets
import threading
import time

from .plan_convergence_maintenance import ongoing_records, business_fingerprint
from .portal_service import PortalConfirmationRequiredError

NS = 'plan_convergence_send'


class NoticeConvergenceGuard:
    def __init__(self, service, store, get_convergence):
        self.service, self.store, self.get_convergence = service, store, get_convergence
        self.slots = threading.BoundedSemaphore(2)

    def check(self, payload):
        kind = payload.get('work_type') or {'设备检修': 'repair', '检修通告': 'repair', '变更通告': 'change'}.get(payload.get('notice_type'))
        action = {'upload': 'start', 'upload_replace': 'update'}.get(payload.get('action'), payload.get('action'))
        if kind not in {'repair', 'change'} or action not in {'start', 'update'}:
            return
        source = dict(payload)
        if source.get('text'):
            from .workbench_lite import parse_pasted_notice_to_draft
            _, _, draft = parse_pasted_notice_to_draft(source['text'], fallback_work_type=kind)
            source = {**draft, **{k: v for k, v in source.items() if v not in (None, '')}}
        scopes = self.service._building_codes_from_value(source.get('building') or source.get('scope'))
        scopes = sorted(set(scopes) - {'D', 'E'})
        if not scopes and self.service._building_codes_from_value(source.get('building') or source.get('scope')):
            return
        source.update(work_type=kind, building='、'.join(code + '楼' if code != '110' else '110站' for code in scopes),
                      record_id=source.get('target_record_id') or source.get('record_id') or 'preview',
                      title=source.get('title') or source.get('name') or '', status='开始' if action == 'start' else '更新')
        record = ongoing_records([source], work_type=kind)[0]
        owner = str(payload.get('_auth_open_id') or 'qt-local')
        digest = hashlib.sha256(json.dumps([owner, action, business_fingerprint(record), source.get('text', ''),
            source.get('source_record_id', ''), source.get('target_record_id', '')], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        ticket = str(payload.get('plan_convergence_confirmation') or '')
        approved = self.store.get_document(NS, 'ticket:' + ticket) if len(ticket) == 48 else None
        if approved and approved.get('fingerprint') == digest and approved.get('expires', 0) > time.time():
            return
        cached = self.store.get_document(NS, digest)
        if cached and time.time() - cached['at'] < 120 and cached.get('matched'):
            return
        matched, reason = False, ''
        if not scopes:
            reason = '楼栋无法确定，尚未完成计划收敛匹配。'
        elif not self.slots.acquire(timeout=.1):
            reason = '计划收敛核对繁忙，尚未取得匹配结果。'
        else:
            try:
                result = self.get_convergence()._check_records([record], kind, seconds=8, refresh=False)
                matched = bool(result['records'][0]['hits'])
                reason = '' if matched else '未匹配到有效的计划收敛。'
            except Exception:
                reason = '计划收敛核对未完成，请检查 VPN 和智航登录；不能判断为已匹配。'
            finally:
                self.slots.release()
        self.store.put_document(NS, digest, {'at': time.time(), 'matched': matched})
        if matched:
            return
        ticket = secrets.token_hex(24)
        self.store.put_document(NS, 'ticket:' + ticket, {'fingerprint': digest, 'expires': time.time() + 900})
        raise PortalConfirmationRequiredError(reason + '请再次确认是否仍要发送本条通告。当前尚未发送。',
            details={'kind': 'plan_convergence_unmatched', 'confirmation': ticket, 'notice_name': record['name'],
                     'scopes': scopes, 'reason': reason})

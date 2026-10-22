"""09:00 plan-deadline reminders; no notice submissions or source-table writes."""
import datetime as dt
import hashlib
import json
import logging
import math
import re
import threading
import uuid
from collections import defaultdict
from urllib.parse import urlencode

from .account_policy import categorize_position
from .notice_recipients import notification_directory
from .planned_notices import TYPES
from .portal_service import BUILDING_OPEN_ID_MAP, PERMISSION_DIRECTORY_SPECIALTY_LABELS

ZONE = dt.timezone(dt.timedelta(hours=8))
NS = 'planned_notice_deadlines'
RETRY_SECONDS = 300
DELIVERY_WINDOW = 3500
LOG = logging.getLogger(__name__)


def now():
    return dt.datetime.now(ZONE)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def date_time(value):
    if value is None or value == '' or isinstance(value, bool):
        return None
    try:
        if isinstance(value, (int, float)):
            if not math.isfinite(value):
                return None
            return dt.datetime.fromtimestamp(value / 1000 if value > 10 ** 12 else value, ZONE)
        parsed = dt.datetime.fromisoformat(str(value).strip().replace('/', '-').replace('Z', '+00:00'))
        return parsed.replace(tzinfo=ZONE) if parsed.tzinfo is None else parsed.astimezone(ZONE)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def plan_times(service, record):
    fields = {**(record.get('display_fields') or {}), **(record.get('raw_fields') or {})}
    source = service._record_source_work_type(record)
    if source == 'change':
        starts = ('变更开始日期（阿里）', '计划开始日期（阿里）', '计划开始', '计划开始时间', '计划延迟开始日期')
        ends = ('变更结束日期（阿里）', '计划结束日期（阿里）', '计划结束', '计划结束时间', '计划延迟结束日期')
    elif source == 'repair':
        starts = ('计划开始时间', '计划开始')
        ends = ('计划结束时间', '计划结束', '期望完成时间')
    else:
        starts = ('计划开始维护时间', '计划开始时间', '计划开始')
        ends = ('计划结束维护时间', '计划结束时间', '计划结束')
    # Never substitute actual completion/fault time for a missing planned deadline.
    return tuple(date_time(next((fields[key] for key in keys if fields.get(key) not in (None, '')), None))
                 for keys in (starts, ends))


class PlannedNoticeDeadlines:
    def __init__(self, service, store, auth, send, *, clock=now):
        self.service, self.store, self.auth, self.send = service, store, auth, send
        self.clock = clock
        self.lock = threading.Lock()
        self.closed = False

    def due(self, current, *, refresh=False):
        service = self.service
        if refresh:
            service.refresh_if_interval_elapsed(min_interval_seconds=RETRY_SECONDS)
        snapshot = self.store.get_source_scope_snapshot('ALL')
        statuses = (snapshot.get('meta') or {}).get('source_refresh_status') or service._source_refresh_status
        checked_at = self.clock().timestamp()
        if not snapshot.get('exists') or any(
                (statuses.get(kind) or {}).get('status') != 'success'
                or not 0 <= checked_at - float((statuses[kind]).get('refreshed_at') or 0) <= 600
                for kind in ('maintenance', 'change', 'repair')):
            raise RuntimeError('计划源表快照尚未完整核验')
        records = service._workbench_records(scope='ALL', source_snapshot=snapshot)
        candidates = []
        for record in records:
            kind = service._record_work_type(record)
            if kind not in TYPES or not record.get('record_id'):
                continue
            start, end = plan_times(service, record)
            if not end or end.date() != current.date() or start and start > end:
                continue
            status = service._source_record_progress(record)
            if any(word in status for word in ('取消', '撤销', '作废')) or not (
                    service._source_progress_allows_start(status) or service._source_progress_is_ongoing(status)):
                continue
            candidates.append((record, kind, start, end, status))
        summaries = {}
        for kind in TYPES:
            selected = [row for row, work, *_ in candidates if work == kind]
            if selected:
                summaries.update({(kind, key): value for key, value in service._work_status_by_records(selected, scope='ALL').items()})
        result = {}
        for record, kind, start, end, status in candidates:
            summary = summaries.get((kind, record['record_id'])) or {}
            ended = date_time(summary.get('ended_at'))
            if ended and start and start <= ended <= current and service._is_completed_work_status_item(summary):
                continue
            fields = record.get('display_fields') or {}
            if kind == 'change':
                scopes, specialty, title = service._change_record_building_codes(record), service._change_specialty(record), service._change_title(record)
            elif kind == 'repair':
                scopes, specialty, title = service._repair_record_building_codes(record), service._repair_specialty(record), service._repair_title(record)
            else:
                scopes = service._building_codes_from_value(fields.get('楼栋'))
                specialty = service._clean_source_text(fields.get('专业'))
                title = str(record.get('title') or fields.get('维护总项') or fields.get('名称') or record['record_id'])
            scopes = sorted(set(scopes) & set(BUILDING_OPEN_ID_MAP))
            if not scopes:
                continue
            key = digest([kind, record.get('source_app_token') or service.app_token,
                record.get('source_table_id') or service.table_id, record['record_id'], end.date().isoformat()])
            result[key] = {'id': key, 'source_record_id': record['record_id'], 'kind': kind,
                'title': title[:200], 'scopes': scopes, 'specialty': specialty,
                'start': start.isoformat(timespec='minutes') if start else '',
                'end': end.isoformat(timespec='minutes'), 'status': status[:50]}
        return result

    def audience(self, items):
        recipients = defaultdict(set)
        app_id = self.auth.current_feishu_app_id()
        waiting = False
        for item in items.values():
            for scope in item['scopes']:
                person = BUILDING_OPEN_ID_MAP[scope]
                if self.auth.current_feishu_open_id(person, app_id):
                    recipients[person].add(item['id'])
                else:
                    waiting = True
        try:
            with notification_directory(self.auth) as (directory_app, people):
                if directory_app != app_id:
                    raise RuntimeError('飞书应用已变化')
                engineers = defaultdict(set)
                profiles = {}
                for person in people:
                    if (not person.get('selectable') or person.get('inactive')
                            or categorize_position(person.get('position')) != 'engineer'):
                        continue
                    ids = person.get('login_ids', [])
                    for item in items.values():
                        major = set(filter(None, re.split(r'[\s,，、/;；]+', item['specialty'])))
                        homes = set(item['scopes']) & set(person.get('scopes', []))
                        if (not homes or not major or not major <= PERMISSION_DIRECTORY_SPECIALTY_LABELS
                                or not major.intersection(person.get('specialties', []))):
                            continue
                        if len(ids) != 1 or not self.auth.current_feishu_open_id(ids[0], app_id):
                            waiting = True
                            continue
                        if ids[0] not in profiles:
                            profiles[ids[0]] = self.auth.account_profile(ids[0])
                        profile = profiles[ids[0]]
                        allowed = set(profile.get('scope_rules', []))
                        if item['kind'] in profile.get('modules', []) and ('ALL' in allowed or homes & allowed):
                            engineers[ids[0]].add(item['id'])
            for person, keys in engineers.items():
                recipients[person].update(keys)
        except Exception as exc:
            waiting = True
            LOG.warning('计划到期工程师目录等待重试: %s', type(exc).__name__)
        if self.auth.current_feishu_app_id() != app_id:
            raise RuntimeError('飞书应用已变化，未发送计划提醒')
        return app_id, dict(recipients), waiting

    def message(self, items, current, recipient):
        lines = ['【计划通告今日到期提醒】', f'核对时间：{current:%Y-%m-%d %H:%M}（北京时间）',
                 f'以下 {len(items)} 项计划今天到期，尚未完成，请及时办理：']
        for number, item in enumerate(items, 1):
            duty = next((code for code, identity in BUILDING_OPEN_ID_MAP.items() if identity == recipient), '')
            allowed = set(self.auth.account_profile(recipient).get('scope_rules', [])) if not duty else {duty}
            scope = next(code for code in item['scopes'] if code in allowed or 'ALL' in allowed)
            base = self.service._signature_public_base_url(scope=scope)
            url = base.rstrip('/') + '/workbench-lite?' + urlencode({'scope': scope,
                'work_type': item['kind'], 'record_id': item['source_record_id']})
            buildings = '、'.join('110站' if code == '110' else code + '楼' for code in item['scopes'])
            lines.extend([f"{number}. {item['title']}", f"楼栋：{buildings}；专业：{item['specialty'][:80] or '待核对'}",
                f"计划开始：{item['start'].replace('T', ' ') or '未填写'}；计划结束：{item['end'].replace('T', ' ')}",
                f"当前状态：{item['status']}", f'办理入口：{url}'])
        lines.append('本消息仅作提醒，不自动开始、更新或结束通告。')
        return '\n'.join(lines)

    def tick(self):
        current = self.clock().astimezone(ZONE)
        if self.closed or current.hour < 9 or not self.lock.acquire(blocking=False):
            return
        day = current.date().isoformat()
        try:
            state = self.store.get_document(NS, day) or {'date': day, 'deliveries': {}}
            if state.get('complete') or state.get('retry_at', 0) > current.timestamp():
                return
            state['retry_at'] = current.timestamp() + RETRY_SECONDS
            self.store.put_document(NS, day, state)
            items = self.due(current, refresh=True)
            app_id, audience, waiting = self.audience(items) if items else ('', {}, False)
            deliveries = state['deliveries']
            reserved = defaultdict(set)
            for receipt in deliveries.values():
                if receipt.get('sent') or receipt.get('attempted_at'):
                    reserved[receipt['recipient']].update(receipt['items'])
                else:
                    receipt['state'] = 'cancelled'
            for recipient, keys in audience.items():
                unsent = sorted(keys - reserved[recipient])
                for offset in range(0, len(unsent), 20):
                    selected = unsent[offset:offset + 20]
                    key = digest([day, app_id, recipient, selected])
                    receipt = deliveries.setdefault(key, {'recipient': recipient, 'items': selected,
                        'app_id': app_id, 'uuid': str(uuid.uuid5(uuid.NAMESPACE_URL, NS + key))})
                    if not receipt.get('attempted_at') and not receipt.get('sent'):
                        receipt['snapshot'] = {key: items[key] for key in selected}
                        receipt['message'] = self.message([items[key] for key in selected], current, recipient)
                        receipt['state'] = 'queued'
            self.store.put_document(NS, day, state)
            attempts = 0
            for receipt in deliveries.values():
                if receipt.get('sent') or receipt.get('state') == 'cancelled':
                    continue
                if self.closed or attempts >= 40:
                    break
                if self.clock().astimezone(ZONE).date().isoformat() != day:
                    break
                fresh = self.due(self.clock().astimezone(ZONE))
                current_app, eligible, _ = self.audience(fresh) if fresh else ('', {}, False)
                selected = set(receipt['items'])
                if (current_app != receipt['app_id'] or not selected <= eligible.get(receipt['recipient'], set())
                        or any(fresh.get(key) != receipt['snapshot'][key] for key in selected)):
                    receipt['state'] = 'waiting_verification' if receipt.get('attempted_at') else 'cancelled'
                    continue
                if receipt.get('attempted_at') and self.clock().timestamp() - receipt['attempted_at'] > DELIVERY_WINDOW:
                    receipt['state'] = 'delivery_uncertain'
                    continue
                prior_unconfirmed = bool(receipt.get('attempted_at'))
                receipt.setdefault('attempted_at', self.clock().timestamp())
                receipt['state'] = 'sending'
                self.store.put_document(NS, day, state)
                attempts += 1
                try:
                    ok, _, details = self.send(receipt['message'], [receipt['recipient']], message_uuid=receipt['uuid'])
                    receipt['sent'] = bool(ok)
                    receipt['state'] = 'sent' if ok else 'retrying'
                    for result in details or []:
                        if result.get('open_id') != receipt['recipient']:
                            continue
                        receipt.update({key: result[key] for key in ('message_id', 'sender_app_id', 'code') if key in result})
                        if not ok and not prior_unconfirmed and (
                                result.get('failure_kind') in {'bot_unavailable', 'not_sent', 'recipient_unavailable', 'blocked'}
                                or type(result.get('code')) is int and result['code'] != 0):
                            receipt.pop('attempted_at', None)
                except Exception as exc:
                    receipt['state'], receipt['error'] = 'delivery_uncertain', type(exc).__name__
                self.store.put_document(NS, day, state)
            sent = defaultdict(set)
            for receipt in deliveries.values():
                if receipt.get('sent'):
                    sent[receipt['recipient']].update(receipt['items'])
            final_items = self.due(self.clock().astimezone(ZONE))
            _, audience, waiting = self.audience(final_items) if final_items else ('', {}, False)
            state.update(complete=not waiting and all(keys <= sent[person] for person, keys in audience.items()),
                         checked_at=current.isoformat(), warning='工程师或值班账号待核验' if waiting else '')
            self.store.put_document(NS, day, state)
        except Exception as exc:
            LOG.warning('计划通告到期提醒等待重试，不影响通告业务: %s', type(exc).__name__)
        finally:
            self.lock.release()

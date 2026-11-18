"""Per-building on-demand inspection capture and learned operator start times."""

import datetime as dt
import hashlib
import math
import statistics
import threading
import time
import uuid
from ipaddress import ip_address
from urllib.parse import urlsplit

import requests

from .risk_inspection import RiskError


NS = 'inspection_live_collection'
CN = dt.timezone(dt.timedelta(hours=8))
FRESH_SECONDS = 600
LEAD_SECONDS = 300


def lan_origin(url):
    try:
        target = urlsplit(url)
        valid_port = target.port is None or 1 <= target.port <= 65535
        if (target.scheme not in ('http', 'https') or not valid_port or target.username is not None
                or target.password is not None or target.query or target.fragment or target.path):
            return False
        if target.hostname == 'localhost':
            return True
        peer = ip_address(target.hostname)
        return peer.is_private and not (peer.is_unspecified or peer.is_reserved or peer.is_multicast)
    except ValueError:
        return False


def minimum_source_time(batch):
    from .daily_inspection import sampled_source_times
    stamps = list(sampled_source_times((batch or {}).get('rows', [])))
    return min(stamps) if stamps and all(type(value) is int and value > 0 for value in stamps) else 0


class InspectionCollection:
    def __init__(self, store, *, transport=None, clock=time.time):
        self.store, self.transport, self.clock = store, transport, clock
        self.locks = {scope: threading.RLock() for scope in ('D', 'E')}
        self.settings_lock = threading.RLock()

    def config(self):
        return self.store.get_document(NS, 'settings') or {'revision': 0, 'enabled': False, 'url': ''}

    def public_config(self):
        config = self.config()
        return {key: config.get(key) for key in ('revision', 'enabled', 'url')} | {
            'fresh_minutes': FRESH_SECONDS // 60,
            'learn_samples': 3, 'lead_minutes': LEAD_SECONDS // 60}

    def save_config(self, data):
        with self.settings_lock:
            current = self.config()
            if data.get('revision') != current.get('revision', 0):
                raise RiskError('采集联动配置已变化，请重新读取后保存。', 409)
            if type(data.get('enabled')) is not bool:
                raise RiskError('采集联动启用状态无效。', 400)
            url = str(data.get('url') or '').strip().rstrip('/')
            if (url or data['enabled']) and not lan_origin(url):
                raise RiskError('请填写外网端的局域网 IP 地址和端口，例如 http://192.168.224.157:18765。', 400)
            self.store.put_document(NS, 'settings', {'revision': current.get('revision', 0) + 1,
                'enabled': data['enabled'], 'url': url})
            return self.public_config()

    @staticmethod
    def capture_key(duty, number):
        return f"capture:{duty['scope']}:{duty['date']}:{duty['shift']}:{number}"

    def _request(self, config, method, path, payload=None):
        if self.transport:
            return self.transport(method, path, payload)
        if not lan_origin(config['url']):
            raise RiskError('请在表格设置中改用外网端的局域网地址。', 403)
        with requests.Session() as session:
            session.trust_env = False
            response = session.request(method, config['url'] + '/api/scheduler/inspection-data-upload/live' + path,
                json=payload, timeout=(3, 5), allow_redirects=False)
        if response.status_code in (401, 403):
            raise RiskError('外网端暂未允许巡检联动，请检查局域网地址和启用状态。', 403)
        if response.status_code >= 400 or 300 <= response.status_code < 400:
            raise RiskError('外网端暂未接受巡检采集，稍后可重试。', 503)
        return response.json()

    def ensure(self, duty, number, *, source_time=0):
        """Request/poll one durable capture per building and round. Never lock a round here."""
        scope = duty['scope']
        with self.locks[scope]:
            config = self.config()
            if not config.get('enabled'):
                return None
            now = int(self.clock() * 1000)
            threshold = now - FRESH_SECONDS * 1000
            if threshold <= source_time <= now + 60000:
                return {'state': 'ready', 'source_time': source_time, 'not_before': threshold}
            key = self.capture_key(duty, number)
            pending = self.store.get_document(NS, key) or {}
            if not pending or pending.get('service_url') != config['url'] or (
                    pending.get('state') == 'ready' and pending.get('source_time', 0) < threshold):
                pending = {'request_id': uuid.uuid4().hex, 'building': scope + '楼',
                    'state': 'pending', 'not_before': threshold, 'requested_at': now,
                    'service_url': config['url'], 'next_check_at': 0}
                self.store.put_document(NS, key, pending)
            if pending['state'] == 'ready':
                return pending
            if now < pending.get('next_check_at', 0):
                return pending
            if pending['state'] == 'failed':
                # Explicitly retry the same id. The producer owns upload deduplication.
                pending.pop('accepted', None)
            try:
                result = self._request(config, 'GET' if pending.get('accepted') else 'POST',
                    '/' + pending['request_id'] if pending.get('accepted') else '',
                    None if pending.get('accepted') else {k: pending[k] for k in ('request_id', 'building', 'not_before')})
                if (not isinstance(result, dict) or result.get('ok') is not True
                        or result.get('request_id') != pending['request_id'] or result.get('building') != scope + '楼'
                        or result.get('state') not in {'pending', 'running', 'uploaded', 'failed'}):
                    raise RiskError('采集回执与本楼请求不一致，尚未锁定巡检数据。', 503)
                state = result['state']
                if state == 'uploaded':
                    sampled, uploaded = result.get('source_time'), result.get('uploaded_at')
                    if (type(sampled) is not int or type(uploaded) is not int
                            or not pending['not_before'] <= sampled <= uploaded <= now + 60000):
                        raise RiskError('采集回执的新鲜度尚未核实，尚未锁定巡检数据。', 503)
                    pending.update(state='ready', source_time=sampled, uploaded_at=uploaded)
                else:
                    pending['state'] = state
                pending.update(accepted=True, next_check_at=now + 2000)
                pending.pop('error', None)
                if state == 'failed':
                    pending['error'] = '本楼采集或上传未完成，请重试原请求。'
                    pending['next_check_at'] = now + 10000
            except RiskError as error:
                pending.update(error=str(error), next_check_at=now + 5000)
                if error.status == 403:
                    pending['state'] = 'failed'
            except (requests.RequestException, ValueError, KeyError):
                pending.update(error='本楼采集暂未核实，稍后继续核对原请求。', next_check_at=now + 5000)
            self.store.put_document(NS, key, pending)
            return pending

    def remember_start(self, owner, duty, number, round_id, *, started_at=None):
        if not owner or not round_id:
            return
        scope = duty['scope']
        at = dt.datetime.fromtimestamp(started_at or self.clock(), CN)
        anchor = dt.datetime.fromisoformat(duty['date']).replace(hour=9 if duty['shift'] == '白' else 18, tzinfo=CN)
        offset = int((at - anchor).total_seconds())
        if offset < 0 or offset > (9 if duty['shift'] == '白' else 15) * 3600:
            return
        key = f"habit:{scope}:" + hashlib.sha256(f"{owner}:{duty['shift']}:{number}".encode()).hexdigest()
        with self.locks[scope]:
            habit = self.store.get_document(NS, key) or {
                'owner': owner, 'scope': scope, 'shift': duty['shift'], 'round': number, 'samples': []}
            if any(row['round_id'] == round_id for row in habit['samples']):
                return
            samples = [row for row in habit['samples'] if row['day'] != duty['date']
                       and row['day'] >= (at.date() - dt.timedelta(days=60)).isoformat()]
            habit['samples'] = [*samples, {'round_id': round_id, 'day': duty['date'], 'offset': offset}][-12:]
            self.store.put_document(NS, key, habit)

    @staticmethod
    def prediction(habit, day):
        samples = [row['offset'] for row in habit.get('samples', []) if row['day'] < day
                   and row['day'] >= (dt.date.fromisoformat(day) - dt.timedelta(days=60)).isoformat()]
        if len(samples) < 3:
            return None
        center = statistics.median(samples)
        stable = [value for value in samples if abs(value - center) <= 1800]
        if len(stable) < 3 or len(stable) < math.ceil(len(samples) * .7):
            return None
        anchor = dt.datetime.fromisoformat(day).replace(hour=9 if habit['shift'] == '白' else 18, tzinfo=CN)
        return anchor + dt.timedelta(seconds=statistics.median(stable))

    def prefetch(self, scope, eligible, source_time=0):
        if not self.config().get('enabled'):
            return
        from .daily_inspection import duty_period, DailyInspection
        from .daily_inspection_reminders import slot_time
        now = dt.datetime.fromtimestamp(self.clock(), CN)
        day, shift = duty_period(now)
        for entry in self.store.list_documents(NS, key_prefix=f'habit:{scope}:'):
            habit = entry['payload']
            if habit['shift'] != shift:
                continue
            expected = self.prediction(habit, day)
            duty, number = {'scope': scope, 'date': day, 'shift': shift}, habit['round']
            if (expected is None or not expected - dt.timedelta(seconds=LEAD_SECONDS) <= now <= expected
                    or not slot_time(day, shift, number) <= expected < DailyInspection.round_cutoff(duty, number)
                    or not eligible(habit['owner'], duty)):
                continue
            self.ensure(duty, number, source_time=source_time)

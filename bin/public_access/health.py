"""One background public-health probe; message senders read only its local result."""
from datetime import datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import threading
import time
from zoneinfo import ZoneInfo

from .config import DATA, atomic_json
from .provisioning import local_host

INTERVAL = 30
MAX_AGE = 75
STARTUP_GRACE = 90
HEALTH = DATA / 'health.json'


def read_result(path=HEALTH):
    try:
        path = Path(path)
        if path.stat().st_size > 16384:
            return {}
        value = json.loads(path.read_text(encoding='utf-8'))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def send_alert(message, recipient, message_uuid):
    from lan_bitable_template_portal.server import _send_text_to_open_ids_guarded
    return _send_text_to_open_ids_guarded(message, [recipient], message_uuid=message_uuid)[0]


def alert_recipients():
    from lan_bitable_template_portal.portal_service import LI_SHILONG_OPEN_ID, BUILDING_OPEN_ID_MAP
    return list(dict.fromkeys([LI_SHILONG_OPEN_ID, BUILDING_OPEN_ID_MAP['H']]))


class Monitor:
    def __init__(self, host, origin, *, data=DATA, notify=send_alert):
        self.host, self.origin, self.data, self.notify = host, origin, Path(data), notify
        self.stop_event = threading.Event()
        self.thread = self.client = None
        self.started_at = time.time()
        prior = read_result(self.data / 'health.json')
        self.prior = prior if prior.get('public_url') == origin and prior.get('deployment_host') == host else {}

    def start(self):
        if self.thread is not None or not local_host(self.host):
            return
        atomic_json(self.data / 'health.json', {**self.prior, 'ok': False, 'state': 'preparing',
            'checked_at': time.time(), 'public_url': self.origin, 'deployment_host': self.host})
        self.thread = threading.Thread(target=self._run, name='ClipFlowPublicHealth', daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()

    def probe(self):
        import httpx
        try:
            if self.client is None:
                from upload_event_module.services.http_client import verified_tls_context
                self.client = httpx.Client(verify=verified_tls_context(trust_env=False), trust_env=False,
                    timeout=3, follow_redirects=False,
                    limits=httpx.Limits(max_connections=2, max_keepalive_connections=1, keepalive_expiry=60))
            with self.client.stream('GET', self.origin + '/api/health?probe=1') as response:
                if response.status_code != 200:
                    return False, 'HTTP ' + str(response.status_code)
                content = bytearray()
                for chunk in response.iter_bytes(1024):
                    content.extend(chunk)
                    if len(content) > 2048:
                        return False, '健康接口返回内容异常'
                value = json.loads(content)
            if isinstance(value, dict) and value.get('ok') is True and value.get('service') == 'clipflow_backend':
                return True, ''
            return False, '返回的不是灯塔健康状态'
        except Exception as exc:
            return False, '公网连接或证书验证失败（' + type(exc).__name__ + '）'

    def alert_text(self, result):
        stamp = datetime.fromtimestamp(result['checked_at'], ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
        return ('【灯塔公网健康检查异常】\n检查时间：' + stamp + '\n'
            + self.origin + ' 已连续 ' + str(result['failures']) + ' 次检查失败：' + result['reason'] + '\n'
            '程序生成的入口链接已回退局域网；局域网业务不因公网检查失败而停止。\n'
            '建议依次检查与处理：\n'
            '1. 正式机 ' + self.host + '：确认灯塔正在运行；查看 FRP 连接日志，断线时重启灯塔即可，不要另开隧道。\n'
            '2. HTTP 502/504：检查正式机 18766、FRP 控制连接及服务器 frps/nginx 状态，恢复对应服务；不要开放 6100 公网直连。\n'
            '3. 连接超时或解析失败：检查正式机联网、域名 DNS 和服务器防火墙；证书错误时检查系统时间及证书自动续期。\n'
            '4. 返回非灯塔内容：核对灯塔独立站点转发是否仍指向 127.0.0.1:6100。\n'
            '恢复后检查结果会自动更新；无需重复发送通告或重新安装程序。')

    def check_once(self, *, now=None, notify=True):
        now = time.time() if now is None else now
        healthy, reason = self.probe()
        prior = self.prior
        failures = 0 if healthy else int(prior.get('failures', 0)) + 1
        recoveries = min(3, int(prior.get('recovery_successes', 0)) + 1) if healthy else 0
        # Restore links immediately, but keep notification deduplication through brief network flaps.
        recovered = healthy and recoveries >= 3
        incident = '' if recovered else prior.get('incident_id') or ('' if healthy else str(time.time_ns()))
        result = {'ok': healthy, 'state': 'healthy' if healthy else 'failed', 'public_url': self.origin,
            'deployment_host': self.host, 'owner_pid': os.getpid(), 'checked_at': now, 'reason': reason,
            'failures': failures, 'recovery_successes': recoveries, 'incident_id': incident,
            'notified': [] if recovered else list(prior.get('notified') or []),
            'alert_attempt_at': 0 if recovered else prior.get('alert_attempt_at', 0)}
        if self.stop_event.is_set():
            return result
        atomic_json(self.data / 'health.json', result)
        self.prior = result
        if (notify and not healthy and failures >= 3 and now - self.started_at >= STARTUP_GRACE
                and (not result['alert_attempt_at'] or now - result['alert_attempt_at'] >= 300)):
            result['alert_attempt_at'] = now
            atomic_json(self.data / 'health.json', result)
            for recipient in alert_recipients():
                if recipient in result['notified'] or self.stop_event.is_set():
                    continue
                key = hashlib.sha256(('public-health:' + incident + ':' + recipient).encode()).hexdigest()[:32]
                try:
                    if self.notify(self.alert_text(result), recipient, key):
                        result['notified'].append(recipient)
                        atomic_json(self.data / 'health.json', result)
                    else:
                        logging.warning('[ClipFlow][FRP] 公网故障通知暂未送达，将退避重试')
                except Exception as exc:
                    logging.warning('[ClipFlow][FRP] 公网故障通知暂未送达，将退避重试 | error=%s', type(exc).__name__)
        return result

    def _run(self):
        from upload_event_module.services.process_lifetime import lower_current_thread_priority
        lower_current_thread_priority()
        try:
            while not self.stop_event.is_set():
                try:
                    self.check_once()
                except Exception as exc:
                    logging.warning('[ClipFlow][FRP] 公网健康检查暂未完成 | error=%s', type(exc).__name__)
                if self.stop_event.wait(INTERVAL):
                    break
        finally:
            if self.client:
                self.client.close()
            try:
                atomic_json(self.data / 'health.json', {**self.prior, 'ok': False, 'state': 'stopped', 'checked_at': time.time()})
            except OSError:
                pass

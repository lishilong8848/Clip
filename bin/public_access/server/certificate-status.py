"""Publish only public TLS/renewal metadata; never publish ACME account or key files."""
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import time

BASE = Path('/opt/clipflow-public')
DOMAIN = 'dt.sjhl.online'
STATUS = BASE / 'status/certificate.json'


def output(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL, timeout=10).strip()


def acme_settings(path):
    allowed = {'Le_NextRenewTime', 'Le_ReloadCmd'}
    return {key: value.strip().strip("'") for line in path.read_text().splitlines()
            for key, _, value in [line.partition('=')] if key in allowed}


def live_fingerprint():
    with socket.create_connection(('127.0.0.1', 443), timeout=5) as raw:
        with ssl.create_default_context().wrap_socket(raw, server_hostname=DOMAIN) as tls:
            return hashlib.sha256(tls.getpeercert(binary_form=True)).hexdigest()


def collect():
    details = output('openssl', 'x509', '-in', str(BASE / 'tls/dt/fullchain.pem'),
                     '-noout', '-subject', '-issuer', '-dates', '-fingerprint', '-sha256', '-nameopt', 'RFC2253')
    fields = {key.lower(): value for line in details.splitlines() for key, value in [line.split('=', 1)]}
    match = output('openssl', 'x509', '-in', str(BASE / 'tls/dt/fullchain.pem'), '-noout', '-checkhost', DOMAIN)
    if 'does match certificate' not in match:
        raise ValueError('Certificate hostname mismatch')
    fingerprint = fields['sha256 fingerprint'].replace(':', '').lower()
    settings = acme_settings(BASE / 'acme-certs/dt.sjhl.online_ecc/dt.sjhl.online.conf')
    encoded = settings.get('Le_ReloadCmd', '').removeprefix('__ACME_BASE64__START_').removesuffix('__ACME_BASE64__END_')
    reload_command = base64.b64decode(encoded, validate=True).decode()
    timer = dict(line.split('=', 1) for line in output('systemctl', 'show', 'clipflow-cert-renew.timer',
                 '-p', 'ActiveState', '-p', 'UnitFileState', '-p', 'NextElapseUSecRealtime').splitlines())
    service_ok = (os.environ.get('SERVICE_RESULT') or output('systemctl', 'show', 'clipflow-cert-renew.service', '-p', 'Result', '--value')) == 'success'
    try:
        serving_current = live_fingerprint() == fingerprint
    except (OSError, ssl.SSLError):
        serving_current = False
    if not serving_current and service_ok:
        # Repair a missed reload without interrupting existing workers or changing site configuration.
        output('openssl', 'x509', '-in', str(BASE / 'tls/dt/fullchain.pem'), '-noout', '-checkend', '0')
        output('nginx', '-t')
        output('nginx', '-s', 'reload')
        for _ in range(20):
            time.sleep(.25)
            if live_fingerprint() == fingerprint:
                serving_current = True
                break
    next_check = timer.get('NextElapseUSecRealtime', '')
    return {'version': 1, 'domain': DOMAIN, 'checked_at': time.time(),
            'issuer': fields['issuer'], 'not_before': ssl.cert_time_to_seconds(fields['notbefore']),
            'not_after': ssl.cert_time_to_seconds(fields['notafter']), 'fingerprint': fingerprint,
            'renewal': {'enabled': timer.get('ActiveState') == 'active' and timer.get('UnitFileState') == 'enabled'
                       and reload_command == 'nginx -t && nginx -s reload',
                       'last_check_ok': service_ok, 'serving_current': serving_current,
                       'next_check': next_check, 'next_renewal_at': int(settings.get('Le_NextRenewTime') or 0)}}


def main():
    try:
        value = collect()
    except Exception:
        try:
            value = json.loads(STATUS.read_text())
        except (OSError, ValueError):
            value = {'version': 1, 'domain': DOMAIN}
        value.update(checked_at=time.time(), error='Certificate status check failed')
        value.setdefault('renewal', {})['last_check_ok'] = False
    STATUS.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', dir=STATUS.parent, delete=False) as pending:
        try:
            os.fchmod(pending.fileno(), 0o644)
            json.dump(value, pending)
            pending.close()
            os.replace(pending.name, STATUS)
        finally:
            Path(pending.name).unlink(missing_ok=True)


if __name__ == '__main__':
    main()

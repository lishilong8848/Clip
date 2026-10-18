"""Account-only, cached certificate metadata. No issuance or SSH credentials on clients."""
import asyncio
import hmac
import math
import threading
import time
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse

_lock = threading.Lock()
_saved = {}


def fetch_status(origin, access_key):
    import httpx
    from upload_event_module.services.http_client import verified_tls_context
    with httpx.Client(verify=verified_tls_context(trust_env=False), trust_env=False, timeout=4,
                      follow_redirects=False) as client:
        with client.stream('GET', origin + '/.well-known/clipflow-certificate.json',
                           headers={'Authorization': 'Bearer ' + access_key}) as response:
            response.raise_for_status()
            content = bytearray()
            for chunk in response.iter_bytes(1024):
                content.extend(chunk)
                if len(content) > 8192:
                    raise ValueError('Certificate metadata too large')
    import json
    return json.loads(content)


def validate(value, domain, now):
    if not isinstance(value, dict) or value.get('version') != 1 or value.get('domain') != domain:
        raise ValueError('Invalid certificate metadata')
    for field in ('checked_at', 'not_before', 'not_after'):
        stamp = value.get(field)
        if type(stamp) not in (int, float) or not math.isfinite(stamp) or stamp <= 0:
            raise ValueError('Invalid certificate date')
    if value['checked_at'] > now + 300 or value['not_after'] <= value['not_before']:
        raise ValueError('Invalid certificate dates')
    renewal = value.get('renewal')
    if not isinstance(renewal, dict) or any(type(renewal.get(key)) is not bool for key in ('enabled', 'last_check_ok', 'serving_current')):
        raise ValueError('Invalid renewal status')
    issuer = value.get('issuer')
    if not isinstance(issuer, str) or not issuer or len(issuer) > 512:
        raise ValueError('Invalid certificate issuer')
    next_renewal = renewal.get('next_renewal_at')
    next_check = renewal.get('next_check')
    if type(next_renewal) is not int or next_renewal < 0 or not isinstance(next_check, str) or len(next_check) > 128:
        raise ValueError('Invalid renewal schedule')
    return {'domain': domain, 'issuer': issuer, **{key: value[key] for key in ('checked_at', 'not_before', 'not_after')},
            'renewal': {key: renewal[key] for key in ('enabled', 'last_check_ok', 'serving_current', 'next_check', 'next_renewal_at')}}


def present(value, now):
    result = {**value, 'remaining_days': max(0, math.ceil((value['not_after'] - now) / 86400))}
    stale = now - value['checked_at'] > 36 * 3600
    result['state'] = ('expired' if value['not_after'] <= now else 'stale' if stale else
                       'warning' if value['not_before'] > now or result['remaining_days'] <= 14
                       or not all(value['renewal'][key] for key in ('enabled', 'last_check_ok', 'serving_current')) else 'healthy')
    return result


def certificate_status(settings):
    if not settings or not settings.token:
        return {'state': 'unavailable'}
    origin, now = settings.origin, time.time()
    access_key = hmac.digest(settings.token.encode(), b'clipflow-certificate-status-v1', 'sha256').hex()
    def cached():
        if _saved.get('key') != (origin, access_key):
            return None
        value = _saved['value']
        result = present(value, now) if 'not_after' in value else dict(value)
        if _saved.get('failed') and result['state'] not in {'expired', 'unavailable'}:
            result['state'] = 'stale'
        return result
    hit = cached()
    if hit is not None and now - _saved['at'] < _saved['ttl']:
        return hit
    if not _lock.acquire(blocking=False):
        return {**hit, 'state': 'expired' if hit['state'] == 'expired' else 'stale'} if hit else {'state': 'preparing'}
    try:
        try:
            value = validate(fetch_status(origin, access_key), urlsplit(origin).hostname, now)
            _saved.update(key=(origin, access_key), at=now, ttl=300, value=value, failed=False)
            return present(value, now)
        except Exception:
            value = ({**hit, 'state': 'expired' if hit['state'] == 'expired' else 'stale'}
                     if hit and 'not_after' in hit else {'state': 'unavailable'})
            _saved.update(key=(origin, access_key), at=now, ttl=60, value=value, failed=True)
            return value
    finally:
        _lock.release()


def install_certificate_routes(app, controller, runtime):
    @app.get('/api/auth/certificate')
    async def status(request: Request):
        session = await asyncio.to_thread(controller._current_session, request)
        if not session:
            return controller._auth_required_response()
        headers = {'Cache-Control': 'private, no-store'}
        user = await asyncio.to_thread(runtime.auth_manager.public_user, session)
        # The trusted account profile maps the fixed H duty account to L1.
        if session.get('role') in {'guest', 'temporary'} or user.get('level') not in {'L0', 'L1'} or user.get('role') != 'admin':
            return JSONResponse({'ok': False, 'error': '仅L0、L1及H楼值班账号可查看证书信息。'}, status_code=403, headers=headers)
        try:
            settings = getattr(getattr(controller, '_public_access', None), 'settings', None)
            value = await asyncio.to_thread(certificate_status, settings)
        except Exception:
            value = {'state': 'unavailable'}
        return JSONResponse({'ok': True, 'data': value}, headers=headers)

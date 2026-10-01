"""Zhihang credentials stored with the portal's local settings."""
import base64
import json
import threading
import time
from pathlib import Path
from urllib.parse import quote

from upload_event_module.utils import get_data_file_path

ZH_BASE = 'https://usability.meta42.indc.vnet.com'
CONFIG_FILE = Path(get_data_file_path('plan_convergence')) / 'auth.json'
DEFAULTS = {'login_name': '', 'display_name': '', 'token': '', 'token_owner_note': ''}
_store = None
_lock = threading.RLock()


def bind_store(store):
    global _store
    _store = store


def _settings_store():
    global _store
    if _store is None:
        from .state_store import LanPortalStateStore
        _store = LanPortalStateStore()
    return _store


def load_config(store=None):
    with _lock:
        store = store if store is not None else _settings_store()
        saved = store.get_document('plan_convergence', 'credentials')
        if saved is None:
            saved = {}
            if CONFIG_FILE.is_file():
                try:
                    legacy = json.loads(CONFIG_FILE.read_text(encoding='utf-8'))
                    if isinstance(legacy, dict):
                        saved = {k: legacy[k] for k in DEFAULTS if k in legacy}
                except (OSError, ValueError):
                    pass
            store.put_document('plan_convergence', 'credentials', saved)
        return {**DEFAULTS, **{key: saved[key] for key in DEFAULTS if key in saved}}


def save_config(updates, store=None):
    if not isinstance(updates, dict):
        raise ValueError('凭证设置须为对象')
    with _lock:
        cfg = load_config(store)
        for key in DEFAULTS:
            if key in updates and updates[key] is not None and (key != 'token' or updates[key] != ''):
                value = str(updates[key]).strip()
                if len(value) > (12000 if key == 'token' else 1000):
                    raise ValueError('凭证字段过长')
                cfg[key] = value
        if updates.get('clear_token'):
            cfg['token'] = ''
            cfg['token_owner_note'] = ''
        elif updates.get('token'):
            cfg['token'] = clean_token(updates['token'])
            cfg['token_owner_note'] = str(_jwt(cfg['token']).get('UserName') or cfg['login_name'])
        (store if store is not None else _settings_store()).put_document('plan_convergence', 'credentials', cfg)
        return cfg


def _jwt(token):
    try:
        part = token.split('.')[1]
        data = json.loads(base64.urlsafe_b64decode(part + '=' * (-len(part) % 4)))
        return data if isinstance(data, dict) else {}
    except (ValueError, IndexError, TypeError):
        return {}


def token_expiry(token):
    try:
        return max(0, int(_jwt(token).get('exp') or 0))
    except (ValueError, TypeError):
        return 0


def settings_view(store=None):
    cfg = load_config(store)
    owner = str(_jwt(cfg['token']).get('UserName') or cfg['token_owner_note'])
    expiry = token_expiry(cfg['token'])
    return {key: cfg[key] for key in ('login_name', 'display_name')} | {
        'has_token': bool(cfg['token']), 'token_owner': owner, 'expires_at': expiry,
        'expired': bool(expiry and expiry <= time.time()),
    }


def clean_token(value):
    if not isinstance(value, str):
        raise ValueError('智航 Token 格式无效')
    value = value.strip()
    if value[:7].casefold() == 'bearer ':
        value = value[7:].strip()
    if not value or len(value) > 12000 or any(ord(char) <= 32 or ord(char) == 127 for char in value):
        raise ValueError('智航 Token 格式无效')
    return value


def build_auth():
    with _lock:
        cfg = load_config()
        if not cfg['token']:
            raise ValueError('请管理员点击登录智航完成认证')
        return token_auth(cfg['token'], cfg)


def token_auth(token, cfg=None):
    token = clean_token(token)
    expiry = token_expiry(token)
    if expiry and expiry <= time.time():
        raise ValueError('智航登录已过期，请点击登录智航重新认证')
    meta, cfg = _jwt(token), cfg or {}
    headers = {'Authorization': 'Bearer ' + token, 'Accept': 'application/json',
               'Content-Type': 'application/json', 'Origin': ZH_BASE, 'Referer': ZH_BASE + '/'}
    cookies = {'access_token': token, 'token': token,
               'LOGINNAME': str(meta.get('UserName') or cfg.get('login_name') or ''),
               'CURRENTID': str(meta.get('jti') or ''), 'CURRENT_USERNAME': quote(str(cfg.get('display_name') or '')),
               'lang': 'zh', 'country': 'CN', 'theme': 'light'}
    return headers, cookies


def test_connection(token=None):
    import requests
    headers, cookies = token_auth(token) if token is not None else build_auth()
    response = requests.post(ZH_BASE + '/api/alarm/alarmBlock/getAlarmBlock', headers=headers, cookies=cookies,
                             json={'page': 1, 'size': 1}, timeout=(5, 15))
    if response.status_code in {401, 403}:
        raise ValueError('智航认证无效，请重新登录智航')
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get('code') != 200 or not payload.get('success'):
        raise ValueError('智航凭证无效或平台拒绝访问')
    return {'connected': True, 'message': '智航连接正常'}

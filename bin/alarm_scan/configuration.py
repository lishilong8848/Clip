"""Machine-local scan settings; app credentials remain owned by Qt settings."""
import json
import os
import re
import uuid
from pathlib import Path


def data_dir():
    from upload_event_module.utils import get_data_file_path
    return Path(get_data_file_path('alarm_scan'))


def validate(value):
    chat = value.get('FEISHU_CHAT_ID', '')
    key = value.get('SCAN_RELAY_SIGNING_KEY', '')
    if not isinstance(chat, str) or not re.fullmatch(r'oc_[A-Za-z0-9_]+', chat):
        raise ValueError('事件测试群标识无效。')
    if not isinstance(key, str) or len(key) < 32 or len(key) > 256:
        raise ValueError('二维码签名密钥长度无效。')
    return {'enabled': True, 'FEISHU_CHAT_ID': chat, 'SCAN_RELAY_SIGNING_KEY': key}


def install(value, destination=None):
    from openclaw_service.assistant.lighthouse_ai import protect_key
    clean = validate(value)
    path = Path(destination) if destination else data_dir() / 'config.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise ValueError('已有扫码配置，未覆盖。请先核对并备份原配置。')
    temp = path.with_name('.config-' + uuid.uuid4().hex + '.tmp')
    try:
        temp.write_text(json.dumps({'version': 1, 'protected': protect_key(json.dumps(clean))}), encoding='utf-8')
        # Windows rename fails if a concurrent installation already created the target.
        temp.rename(path)
    finally:
        temp.unlink(missing_ok=True)
    return path


def read(path):
    from openclaw_service.assistant.lighthouse_ai import unprotect_key
    value = json.loads(Path(path).read_text(encoding='utf-8-sig'))
    if 'protected' in value:
        value = json.loads(unprotect_key(value['protected']))
    if value.get('enabled') is not True:
        raise ValueError('扫码模块未启用。')
    return validate(value)


def qt_credentials():
    from upload_event_module.config import config
    config.refresh_credentials()
    return str(config.app_id or '').strip(), str(config.app_secret or '').strip()


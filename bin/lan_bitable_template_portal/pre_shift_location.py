"""Server-only AMap lookup; a failed lookup never blocks a photo upload."""
import hashlib
import json
import logging
import re
import threading
import time
from collections import OrderedDict

from .pre_shift_checks import CheckError, now
from .pre_shift_photo import photo_position


class _PrivateMapLog(logging.Filter):
    def filter(self, record):
        message = record.getMessage()
        if 'restapi.amap.com' in message:
            record.msg = re.sub(r'https://restapi\.amap\.com/[^\s\"\']+',
                                'https://restapi.amap.com/[private-location-request]', message)
            record.args = ()
        return True


# httpx includes GET query strings in its normal request log.
logging.getLogger('httpx').addFilter(_PrivateMapLog())


class PhotoLocation:
    def __init__(self, path, *, client=None):
        self.path = path
        self.client = client
        self.lock = threading.RLock()
        self.slots = threading.BoundedSemaphore(2)
        self.cache = OrderedDict()
        self.closed = False

    def _read(self):
        raw = self.path.read_bytes() if self.path.exists() else b'{}'
        config = json.loads(raw)
        if not isinstance(config, dict):
            raise ValueError()
        return config, hashlib.sha256(raw).hexdigest()

    def settings(self):
        with self.lock:
            try:
                config, revision = self._read()
                return {'configured': bool(config.get('key_cipher')), 'revision': revision}
            except (OSError, ValueError):
                raise CheckError('地点解析配置无法读取，请检查服务器配置文件。', 503) from None

    def save(self, body):
        from openclaw_service.assistant.lighthouse_ai import protect_key
        from openclaw_service.protocol import atomic_json
        if not isinstance(body, dict) or set(body) != {'key', 'revision'}:
            raise CheckError('地点解析配置参数无效。')
        key = body['key']
        if not isinstance(key, str) or not re.fullmatch(r'[a-fA-F0-9]{32}', key.strip()):
            raise CheckError('请填写高德 Web 服务类型的32位 Key。')
        with self.lock:
            if body['revision'] != self.settings()['revision']:
                raise CheckError('配置已被修改，请重新打开后保存。', 409)
            try:
                atomic_json(self.path, {'key_cipher': protect_key(key.strip())})
            except Exception:
                raise CheckError('地点解析配置保存失败，请检查本机密钥保护和目录权限。', 503) from None
            self.cache.clear()
            return self.settings()

    def resolve(self, capture, current=None):
        position = photo_position(capture, current or now())
        if not position or not self.slots.acquire(False):
            return ''
        try:
            from openclaw_service.assistant.lighthouse_ai import unprotect_key
            with self.lock:
                config, revision = self._read()
                if not config.get('key_cipher'):
                    return ''
                coordinates = f"{position['longitude']:.6f},{position['latitude']:.6f}"
                cache_key = (revision, coordinates)
                cached = self.cache.get(cache_key)
                if cached and cached[0] > time.monotonic():
                    self.cache.move_to_end(cache_key)
                    return cached[1]
                key = unprotect_key(config['key_cipher'])
                if self.client is None:
                    from upload_event_module.services.http_client import FeishuHttpClient
                    self.client = FeishuHttpClient(retries=0)
            address = self._lookup(key, coordinates)
            with self.lock:
                self.cache[cache_key] = (time.monotonic() + (600 if address else 30), address)
                while len(self.cache) > 256:
                    self.cache.popitem(last=False)
            return address
        except Exception:
            # Do not expose provider errors: they can contain the key and precise coordinates.
            return ''
        finally:
            self.slots.release()

    def _lookup(self, key, coordinates):
        converted = self.client.request_json('GET', 'https://restapi.amap.com/v3/assistant/coordinate/convert',
            params={'key': key, 'locations': coordinates, 'coordsys': 'gps', 'output': 'JSON'}, timeout=1.5)
        location = converted.get('locations')
        if (converted.get('status') != '1' or not isinstance(location, str)
                or not re.fullmatch(r'-?\d{1,3}(?:\.\d+)?,\s*-?\d{1,2}(?:\.\d+)?', location)):
            return ''
        lon, lat = map(float, location.split(','))
        if not (-180 <= lon <= 180 and -90 <= lat <= 90):
            return ''
        result = self.client.request_json('GET', 'https://restapi.amap.com/v3/geocode/regeo',
            params={'key': key, 'location': f'{lon:.6f},{lat:.6f}', 'extensions': 'base', 'output': 'JSON'}, timeout=1.5)
        detail = result.get('regeocode')
        address = detail.get('formatted_address') if isinstance(detail, dict) else None
        if (result.get('status') == '1' and isinstance(address, str) and 0 < len(address.strip()) <= 200
                and any('\u4e00' <= char <= '\u9fff' for char in address)
                and not any(ord(char) < 32 for char in address)):
            return address.strip()
        return ''

    def close(self):
        with self.lock:
            self.closed = True
            if self.client:
                self.client.close()

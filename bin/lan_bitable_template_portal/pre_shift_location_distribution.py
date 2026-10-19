"""Publish a private map credential; ship only its verified Feishu descriptor."""
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path

from openclaw_service.assistant.lighthouse_distribution import BASE, TABLE, FeishuRuntimeMirror, validate_spec
from openclaw_service.protocol import atomic_json, protect_state_directory
from .pre_shift_checks import CheckError

SPEC = Path(__file__).with_name('photo-location-distribution.json')
COMPONENT = 'photo_location_profile'


def _content(key):
    if not isinstance(key, str) or not re.fullmatch(r'[a-fA-F0-9]{32}', key):
        raise ValueError('Invalid photo location credential.')
    return json.dumps({'format': 1, 'key': key}, sort_keys=True, separators=(',', ':')).encode('utf-8')


def read_spec(path):
    if Path(path).stat().st_size > 8192:
        raise ValueError('Photo location descriptor is too large.')
    spec = json.loads(Path(path).read_bytes())
    validate_spec(spec, component=COMPONENT)
    if (set(spec) != {'format', 'component', 'version', 'platform', 'base', 'table', 'sha256', 'size', 'parts'}
            or set(spec['parts'][0]) != {'index', 'size', 'sha256', 'name', 'record_id', 'file_token'}
            or spec['parts'][0]['name'] != 'clipflow-photo-location.json'
            or spec['parts'][0]['sha256'] != spec['sha256']):
        raise ValueError('Invalid photo location descriptor.')
    return spec


def _decode(content, spec):
    if len(content) != spec['size'] or hashlib.sha256(content).hexdigest() != spec['sha256']:
        raise ValueError('Photo location credential integrity check failed.')
    value = json.loads(content)
    if (not isinstance(value, dict) or set(value) != {'format', 'key'}
            or type(value['format']) is not int or value['format'] != 1):
        raise ValueError('Invalid photo location credential.')
    _content(value['key'])
    return value['key']


def _mirror(factory):
    return factory() if factory else FeishuRuntimeMirror(timeout=8)


def publish_location_config(config_path=None, *, spec_path=SPEC, mirror_factory=None):
    from openclaw_service.assistant.lighthouse_ai import unprotect_key
    from upload_event_module.utils import get_data_file_path
    from .pre_shift_location import PhotoLocation
    if mirror_factory is None and os.environ.get('CLIPFLOW_BACKEND_MOCK_EXTERNAL') == '1':
        raise CheckError('隔离测试模式禁止发布地点解析密钥。', 403)
    config_path = Path(config_path or Path(get_data_file_path('pre_shift_checks')) / 'location_settings.json')
    settings = PhotoLocation(config_path)
    config, _ = settings._read()
    content = _content(unprotect_key(config['key_cipher'])) if config.get('key_cipher') else None
    if content is None:
        # Another build machine can reuse an already published descriptor, never a fake placeholder.
        spec = read_spec(spec_path)
    else:
        digest = hashlib.sha256(content).hexdigest()
        spec = {'format': 1, 'component': COMPONENT, 'version': digest, 'platform': 'windows_amd64',
                'base': BASE, 'table': TABLE, 'sha256': digest, 'size': len(content), 'parts': [
                    {'index': 1, 'name': 'clipflow-photo-location.json', 'size': len(content), 'sha256': digest}]}
        validate_spec(spec, published=False, component=COMPONENT)
    private = config_path.parent / 'location_bootstrap'
    protect_state_directory(private)
    journal = private / 'upload-journal.json'
    with tempfile.TemporaryDirectory(prefix='publish-', dir=private) as temp:
        folder = Path(temp)
        mirror = _mirror(mirror_factory)
        try:
            if content is not None:
                (folder / spec['parts'][0]['name']).write_bytes(content)
                if not journal.exists() and Path(spec_path).is_file():
                    previous = read_spec(spec_path)
                    if previous['sha256'] == spec['sha256']:
                        part = previous['parts'][0]
                        atomic_json(journal, {spec['sha256'] + ':1': {
                            'record_id': part['record_id'], 'file_token': part['file_token']}})
                mirror.publish(spec, folder, journal, component=COMPONENT)
            verified = folder / 'verified.json'
            mirror.download_part(spec['parts'][0], verified)
            _decode(verified.read_bytes(), spec)
            candidate = folder / 'descriptor.json'
            atomic_json(candidate, spec)
            read_spec(candidate)
        finally:
            mirror.close()
    atomic_json(spec_path, spec)
    read_spec(spec_path)
    return spec


def provision_location(settings, *, spec_path=SPEC, mirror_factory=None):
    from openclaw_service.assistant.lighthouse_ai import protect_key, unprotect_key
    if not Path(spec_path).is_file():
        return True
    spec = read_spec(spec_path)
    with settings.lock:
        if settings.closed:
            return False
        try:
            config, revision = settings._read()
        except ValueError:
            config, revision = {}, hashlib.sha256(settings.path.read_bytes()).hexdigest()
        try:
            current = _content(unprotect_key(config['key_cipher']))
            if hashlib.sha256(current).hexdigest() == spec['sha256'] or not config.get('distribution_sha256'):
                return True  # Preserve a valid explicitly configured local override.
        except Exception:
            pass
    if mirror_factory is None and os.environ.get('CLIPFLOW_BACKEND_MOCK_EXTERNAL') == '1':
        return True
    private = settings.path.parent / 'location_bootstrap'
    protect_state_directory(private)
    with tempfile.TemporaryDirectory(prefix='download-', dir=private) as temp:
        mirror = _mirror(mirror_factory)
        try:
            target = Path(temp) / 'credential.json'
            mirror.download_part(spec['parts'][0], target)
            key = _decode(target.read_bytes(), spec)
        finally:
            mirror.close()
    with settings.lock:
        raw = settings.path.read_bytes() if settings.path.exists() else b'{}'
        if settings.closed or hashlib.sha256(raw).hexdigest() != revision:
            return False
        atomic_json(settings.path, {'key_cipher': protect_key(key), 'distribution_sha256': spec['sha256']})
        settings.cache.clear()
    return True

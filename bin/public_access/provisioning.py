"""Host-bound bootstrap from the existing private Feishu dependency mirror."""
from dataclasses import asdict, replace
import hashlib
import ipaddress
import json
from pathlib import Path
import socket
import tempfile

from .config import DATA, DEPLOYMENT, Settings, atomic_json


def local_host(address):
    # Binding checks an assigned interface, not whether another LAN host is reachable.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        try:
            probe.bind((address, 0))
            return True
        except OSError:
            return False


def read_deployment(path=DEPLOYMENT):
    from openclaw_service.assistant.lighthouse_distribution import validate_spec
    profile = json.loads(Path(path).read_text(encoding='utf-8'))
    if not isinstance(profile, dict) or set(profile) != {'format', 'deployment_host', 'settings', 'credential'} \
            or type(profile['format']) is not int or profile['format'] != 1 \
            or not isinstance(profile['deployment_host'], str):
        raise ValueError('Invalid public deployment profile.')
    address = ipaddress.IPv4Address(profile['deployment_host'])
    if not address.is_private or address.is_loopback or address.is_unspecified or address.is_multicast:
        raise ValueError('Deployment host must be a LAN interface.')
    value = profile['settings']
    if not isinstance(value, dict) or 'token' in value or value.get('enabled') is not True:
        raise ValueError('Deployment manifest must not contain credentials.')
    settings = Settings.from_value({**value, 'enabled': False})
    settings.validate_enabled(require_token=False)
    validate_spec(profile['credential'], component='public_access_profile')
    return profile, replace(settings, enabled=True)


def decode_credential(content, profile):
    spec = profile['credential']
    if len(content) != spec['size'] or hashlib.sha256(content).hexdigest() != spec['sha256']:
        raise ValueError('Public credential integrity check failed.')
    value = json.loads(content)
    if not isinstance(value, dict) or set(value) != {'format', 'deployment_host', 'settings'} \
            or type(value['format']) is not int or value['format'] != 1 \
            or value['deployment_host'] != profile['deployment_host']:
        raise ValueError('Public credential belongs to a different deployment.')
    settings = Settings.from_value(value['settings'])
    expected = asdict(Settings(**profile['settings']))
    actual = asdict(settings)
    expected.pop('token')
    actual.pop('token')
    if not settings.enabled or actual != expected:
        raise ValueError('Public credential configuration mismatch.')
    return settings


def provision(profile, data=DATA, *, mirror_factory=None, cancelled=lambda: False):
    from openclaw_service.assistant.lighthouse_ai import protect_key, unprotect_key
    from openclaw_service.assistant.lighthouse_distribution import FeishuRuntimeMirror
    from openclaw_service.protocol import protect_state_directory
    if not local_host(profile['deployment_host']):
        raise ValueError('Not the designated public deployment host.')
    if cancelled():
        raise InterruptedError('Public provisioning cancelled.')
    data = Path(data)
    protect_state_directory(data)
    cache = data / 'deployment-credential.json'
    try:
        if cache.stat().st_size > 32768:
            raise ValueError('Invalid credential cache size.')
        saved = json.loads(cache.read_text(encoding='utf-8'))
        if saved.get('sha256') == profile['credential']['sha256']:
            return decode_credential(unprotect_key(saved['protected']).encode('utf-8'), profile)
    except Exception:
        pass
    mirror = (mirror_factory or FeishuRuntimeMirror)()
    try:
        with tempfile.TemporaryDirectory(prefix='credential-', dir=data) as temp:
            target = Path(temp) / 'profile.json'
            mirror.download_part(profile['credential']['parts'][0], target)
            content = target.read_bytes()
            settings = decode_credential(content, profile)
            if cancelled():
                raise InterruptedError('Public provisioning cancelled.')
            atomic_json(cache, {'sha256': profile['credential']['sha256'],
                                'protected': protect_key(content.decode('utf-8'))})
            return settings
    finally:
        mirror.close()

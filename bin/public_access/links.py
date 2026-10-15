"""Choose a live public portal for generated message links, never for internal RPC."""
from functools import lru_cache
import time
from urllib.parse import urlsplit

from .provisioning import local_host, read_deployment
from .health import HEALTH, MAX_AGE, read_result

@lru_cache(maxsize=1)
def deployment():
    try:
        profile, settings = read_deployment()
        return profile['deployment_host'], settings.origin
    except Exception:
        return None


def healthy_origin(host, origin):
    result = read_result(HEALTH)
    checked = result.get('checked_at')
    if (result.get('ok') is True and result.get('state') == 'healthy'
            and result.get('deployment_host') == host and result.get('public_url') == origin
            and type(checked) in {int, float} and 0 <= time.time() - checked <= MAX_AGE):
        return origin
    return ''


def preferred_base(lan_base):
    configured = deployment()
    if not configured or not local_host(configured[0]):
        # Signed links from a different installation must not point at production's token store.
        return lan_base
    host, origin = configured
    public = healthy_origin(host, origin)
    if public:
        return public
    if urlsplit(lan_base).hostname == urlsplit(origin).hostname:
        from upload_event_module.config import config
        return 'http://' + host + ':' + str(config.lan_template_portal_port or 18766)
    return lan_base

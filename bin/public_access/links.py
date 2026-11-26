"""Choose a live public portal for generated message links, never for internal RPC."""
from functools import lru_cache
import threading
import time
from urllib.parse import urlsplit

from .config import DEPLOYMENT
from .provisioning import local_host, read_deployment
from .health import HEALTH, MAX_AGE, Monitor, read_result

_probe_lock = threading.Lock()
_last_probe = None

@lru_cache(maxsize=1)
def _deployment_version(stamp, size):
    profile, settings = read_deployment()
    return profile['deployment_host'], settings.origin


def deployment():
    try:
        stat = DEPLOYMENT.stat()
        return _deployment_version(stat.st_mtime_ns, stat.st_size)
    except Exception:
        return None


def _probe_origin(host, origin):
    # One read-only check; do not start a monitor, write its state, or send alerts.
    monitor = Monitor(host, origin)
    try:
        return monitor.probe()[0]
    finally:
        if monitor.client is not None:
            monitor.client.close()


def healthy_origin(host, origin):
    global _last_probe
    result = read_result(HEALTH)
    checked = result.get('checked_at')
    if (result.get('ok') is True and result.get('state') == 'healthy'
            and result.get('deployment_host') == host and result.get('public_url') == origin
            and type(checked) in {int, float} and 0 <= time.time() - checked <= MAX_AGE):
        return origin
    # A missing/stale heartbeat (or a transient failure) does not prove that a
    # recipient cannot open the public portal. Share one bounded check per burst.
    key = (host, origin, checked)
    with _probe_lock:
        if _last_probe is not None:
            last_key, expires, reachable = _last_probe
            if last_key == key and time.monotonic() < expires:
                return origin if reachable else ''
        reachable = _probe_origin(host, origin)
        _last_probe = (key, time.monotonic() + (30 if reachable else 5), reachable)
        return origin if reachable else ''


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

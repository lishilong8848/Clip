"""Optional public access, isolated from portal readiness and business workers."""
import logging
import secrets


def attach(app, controller):
    from .config import Settings
    try:
        settings = Settings.read()
        if not settings.enabled:
            return app, None
        if settings.ingress_port == controller.bound_port:
            raise ValueError('公网隔离入口不能与局域网业务端口相同。')
        from .ingress import PublicGuard
        from .supervisor import PublicAccess
        token = secrets.token_urlsafe(32)
        manager = PublicAccess(settings, controller.get_url(), token)
        return PublicGuard(app, token, settings, controller._current_session), manager
    except Exception as exc:
        # Invalid optional config must not prevent the LAN app from starting.
        logging.warning('[ClipFlow][FRP] 公网配置未启用，局域网不受影响 | error=%s', type(exc).__name__)
        return app, None

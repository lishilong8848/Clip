"""Server-only attribution propagated across request and worker boundaries."""
from contextvars import ContextVar

CURRENT_IMPERSONATION = ContextVar('admin_impersonation', default=None)


def audit_metadata(grant):
    return {key: grant[key] for key in (
        'id', 'operator_open_id', 'operator_name', 'target_open_id', 'target_name')}

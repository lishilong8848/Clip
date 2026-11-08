"""Current duty identities; no historical OpenID translation at runtime."""
from .portal_service import BUILDING_OPEN_ID_MAP


TARGET_APP_ID = "cli_aa42dc770a381cb3"
TARGET_DUTY_OPEN_IDS = BUILDING_OPEN_ID_MAP


def canonical_duty_identity(open_id, app_id):
    return open_id


def duty_recipient_open_id(identity, app_id):
    if app_id == TARGET_APP_ID and identity in BUILDING_OPEN_ID_MAP.values():
        return identity
    return None

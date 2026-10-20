"""Current duty identities; no historical OpenID translation at runtime."""
from .portal_service import BUILDING_OPEN_ID_MAP


TARGET_APP_ID = "cli_aa42dc770a381cb3"
TARGET_DUTY_OPEN_IDS = {
    "A": "ou_822759df685e2807d538fb9cf5465465",
    "B": "ou_ac4e1a554a6cabab4d8eea7e3956bec3",
    "C": "ou_f2879fa04399d43bff26f92616678b6d",
    "D": "ou_18396043dc393fafd3b7f43f0e284396",
    "E": "ou_fb53efe356466339f4f3cc22f2c7f9fb",
    "H": "ou_3e7a45be43f809cd54e277243e9821ab",
    "110": "ou_ca8103bc15b06b448ab94958d5cfef43",
}


def canonical_duty_identity(open_id, app_id):
    return open_id


def duty_recipient_open_id(identity, app_id):
    if app_id == TARGET_APP_ID and identity in BUILDING_OPEN_ID_MAP.values():
        return identity
    return None

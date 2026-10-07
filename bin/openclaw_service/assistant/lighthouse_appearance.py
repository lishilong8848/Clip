"""Account-isolated bot appearance preferences backend.

Appearance values are kept completely separate from conversation, model
configuration and clear-chat operations.  The authenticated actor id is the
only identity source and is never taken from the client payload.
"""
import copy

from .lighthouse_ai import AssistantError

NAMESPACE = "lighthouse_appearance"

COLORS = frozenset({
    "encre", "creme", "brun", "rouge", "orange", "ambre", "vert",
    "turquoise", "bleu", "violet", "rose", "gris",
})

SHAPES = frozenset({
    "cercle", "galet", "squircle", "capsule", "triangle", "hexagone",
    "nuage", "goutte",
})

EXPRESSIONS = frozenset({
    "neutre", "attentif", "surpris", "excite", "heureux", "hilare",
    "colere", "triste", "effraye", "mefiant", "confus", "curieux",
    "fier", "timide", "blase", "somnolent",
})

STATES = frozenset({
    "cycle", "idle", "thinking", "wink", "wide", "alert", "notify",
    "exclaim", "sleep", "egg", "hexagon", "play", "orbit", "burst",
    "comet",
})

DEFAULT_APPEARANCE = {
    "color": "encre",
    "shape": "cercle",
    "expression": "neutre",
    "state": "idle",
    "size": 56,
    "animated": True,
    "follow": False,
    "snap_back": True,
}

FIELDS = frozenset(DEFAULT_APPEARANCE)
_ENUM_FIELDS = {
    "color": COLORS,
    "shape": SHAPES,
    "expression": EXPRESSIONS,
    "state": STATES,
}


def _valid_actor(actor_id):
    return isinstance(actor_id, str) and bool(actor_id.strip())


def _valid_value(key, value):
    if key in _ENUM_FIELDS:
        return isinstance(value, str) and value in _ENUM_FIELDS[key]
    if key == "size":
        return type(value) is int and 40 <= value <= 200
    if key in ("animated", "follow", "snap_back"):
        return type(value) is bool
    return False


def _normalize(values):
    """Return a normalized per-field appearance, falling back per field."""
    if not isinstance(values, dict):
        return copy.deepcopy(DEFAULT_APPEARANCE)
    normalized = {}
    for key, default in DEFAULT_APPEARANCE.items():
        value = values.get(key)
        normalized[key] = value if _valid_value(key, value) else default
    return normalized


def _validate_payload(payload):
    for key, value in payload.items():
        if key in _ENUM_FIELDS:
            if not isinstance(value, str) or value not in _ENUM_FIELDS[key]:
                raise AssistantError("外观设置包含无效的枚举值。")
        elif key == "size":
            if type(value) is not int or not (40 <= value <= 200):
                raise AssistantError("外观尺寸必须为 40 到 200 之间的整数。")
        elif key in ("animated", "follow", "snap_back"):
            if type(value) is not bool:
                raise AssistantError("外观开关必须为布尔值。")
        else:  # pragma: no cover - guarded by caller, kept defensive
            raise AssistantError("外观设置包含无效字段。")


def read_appearance(store, actor_id):
    """Return the account's appearance settings, or defaults for a new actor."""
    if not _valid_actor(actor_id):
        raise AssistantError("缺少登录身份，无法读取外观设置。", 403)
    stored = store.get_document(NAMESPACE, actor_id)
    return _normalize(stored)


def save_appearance(store, actor_id, payload):
    """Validate and persist partial appearance updates for one account."""
    if not _valid_actor(actor_id):
        raise AssistantError("缺少登录身份，无法保存外观设置。", 403)
    if not isinstance(payload, dict) or not payload:
        raise AssistantError("外观设置必须是非空的设置对象。")
    unknown = set(payload) - FIELDS
    if unknown:
        raise AssistantError("外观设置包含无效字段。")
    _validate_payload(payload)

    current = read_appearance(store, actor_id)
    current.update(copy.deepcopy(payload))
    # Writes must persist before returning; store exceptions are not swallowed.
    store.put_document(NAMESPACE, actor_id, current)
    return copy.deepcopy(current)

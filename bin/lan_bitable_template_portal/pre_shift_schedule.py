"""Pure schedule helpers for pre-shift frequency rules.

Everything is deterministic; no clock, IO, threads or databases.
Timezone is fixed UTC+8.
"""

import calendar
import datetime
import re

UTC8 = datetime.timezone(datetime.timedelta(hours=8))

_TIME_RE = re.compile(r"^([0-9]{2}):([0-9]{2})$")
_DATE_RE = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})$")

_WHITE = "\u767d"  # 白
_NIGHT = "\u591c"  # 夜

_KINDS = {
    "shift",
    "daily",
    "weekly",
    "month_start",
    "month_end",
    "interval",
    "temporary",
}

_ALLOWED = {
    "shift": {"kind", "day_time", "night_time", "start_date"},
    "daily": {"kind", "time", "start_date"},
    "weekly": {"kind", "weekday", "time", "start_date"},
    "month_start": {"kind", "time", "start_date"},
    "month_end": {"kind", "time", "start_date"},
    "interval": {"kind", "every_days", "time", "start_date"},
    "temporary_daily": {"kind", "cadence", "limit_type", "limit", "time", "start_date"},
    "temporary_shift": {"kind", "cadence", "limit_type", "limit", "day_time", "night_time", "start_date"},
}

_DAY_DEFAULT = "08:50"
_NIGHT_DEFAULT = "17:50"
_DAILY_TIME_DEFAULT = "08:50"


def _to_date(value):
    """Accept datetime.date/datetime.datetime/strict YYYY-MM-DD string.

    Strictly rejects compact (YYYYMMDD) and week ISO dates; only the exact
    ``YYYY-MM-DD`` shape is accepted and a real calendar date is required.
    """
    if isinstance(value, datetime.datetime):
        value = value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        match = _DATE_RE.match(value)
        if not match:
            raise ValueError("invalid date: {!r}".format(value))
        year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
        try:
            return datetime.date(year, month, day)
        except ValueError:
            raise ValueError("invalid date: {!r}".format(value))
    raise ValueError("invalid date: {!r}".format(value))


def _parse_time(text):
    """Strict HH:mm validation, returns minutes past midnight."""
    if not isinstance(text, str):
        raise ValueError("time must be a string HH:mm")
    match = _TIME_RE.match(text)
    if not match:
        raise ValueError("invalid time: {!r}".format(text))
    hh = int(match.group(1))
    mm = int(match.group(2))
    if hh > 23 or mm > 59:
        raise ValueError("invalid time: {!r}".format(text))
    return hh * 60 + mm


def _hhmm(text):
    hh, mm = text.split(":")
    return int(hh), int(mm)


def _validate_shift_clocks(day_time, night_time):
    day_min = _parse_time(day_time)
    night_min = _parse_time(night_time)
    if not (480 <= day_min < 1080):  # 08:00 .. 17:59
        raise ValueError("day_time must be >=08:00 and <18:00")
    if not (night_min >= 1070 or night_min < 480):  # >=17:50 or <08:00
        raise ValueError("night_time must be >=17:50 or <08:00")


def _check_int(value, name, lo, hi):
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("{} must be an integer".format(name))
    if not (lo <= value <= hi):
        raise ValueError("{} out of range".format(name))
    return value


def _reject_unknown(value, allowed):
    extra = set(value.keys()) - allowed
    if extra:
        raise ValueError("unknown fields: {}".format(sorted(extra)))


def _resolve_start(value, default):
    if "start_date" in value:
        return _to_date(value["start_date"]).isoformat()
    return default


def _require(value, key):
    if key not in value:
        raise ValueError("missing required field: {!r}".format(key))


def normalize_frequency(value, default_start_date):
    """Normalize a frequency rule into a fixed normalized schema.

    ``value`` is None (legacy shift default) or a dict. ``default_start_date``
    is a date or YYYY-MM-DD string used when the dict omits ``start_date``.
    The returned rule is fully JSON-serializable: ``start_date`` is an ISO
    ``YYYY-MM-DD`` string.
    """
    default = _to_date(default_start_date).isoformat()

    if value is None:
        return {
            "kind": "shift",
            "day_time": _DAY_DEFAULT,
            "night_time": _NIGHT_DEFAULT,
            "start_date": default,
        }

    if not isinstance(value, dict):
        raise ValueError("value must be a dict or None")

    kind = value.get("kind")
    if not isinstance(kind, str) or kind not in _KINDS:
        raise ValueError("unknown kind: {!r}".format(kind))

    if kind == "shift":
        _reject_unknown(value, _ALLOWED["shift"])
        day_time = value.get("day_time", _DAY_DEFAULT)
        night_time = value.get("night_time", _NIGHT_DEFAULT)
        _validate_shift_clocks(day_time, night_time)
        return {
            "kind": "shift",
            "day_time": day_time,
            "night_time": night_time,
            "start_date": _resolve_start(value, default),
        }

    if kind in ("daily", "weekly", "month_start", "month_end", "interval"):
        _reject_unknown(value, _ALLOWED[kind])
        _require(value, "time")
        _parse_time(value["time"])  # validate
        result = {
            "kind": kind,
            "time": value["time"],
            "start_date": _resolve_start(value, default),
        }
        if kind == "weekly":
            _require(value, "weekday")
            result["weekday"] = _check_int(value["weekday"], "weekday", 0, 6)
        if kind == "interval":
            _require(value, "every_days")
            result["every_days"] = _check_int(value["every_days"], "every_days", 1, 3660)
        return result

    # temporary
    cadence = value.get("cadence", "daily")
    if cadence not in ("daily", "shift"):
        raise ValueError("invalid cadence: {!r}".format(cadence))
    limit_type = value.get("limit_type", "occurrences")
    if limit_type not in ("occurrences", "days"):
        raise ValueError("invalid limit_type: {!r}".format(limit_type))
    limit = value.get("limit", 1)
    limit = _check_int(limit, "limit", 1, 366)

    if cadence == "daily":
        _reject_unknown(value, _ALLOWED["temporary_daily"])
        time_text = value.get("time", _DAILY_TIME_DEFAULT)
        _parse_time(time_text)  # validate
        return {
            "kind": "temporary",
            "cadence": "daily",
            "limit_type": limit_type,
            "limit": limit,
            "time": time_text,
            "start_date": _resolve_start(value, default),
        }

    _reject_unknown(value, _ALLOWED["temporary_shift"])
    day_time = value.get("day_time", _DAY_DEFAULT)
    night_time = value.get("night_time", _NIGHT_DEFAULT)
    _validate_shift_clocks(day_time, night_time)
    return {
        "kind": "temporary",
        "cadence": "shift",
        "limit_type": limit_type,
        "limit": limit,
        "day_time": day_time,
        "night_time": night_time,
        "start_date": _resolve_start(value, default),
    }


def _single_occurrence(rule, d):
    """One scheduled occurrence for a single-time rule on nominal day ``d``."""
    hh, mm = _hhmm(rule["time"])
    minutes = hh * 60 + mm
    due = datetime.datetime(d.year, d.month, d.day, hh, mm, tzinfo=UTC8)
    if 480 <= minutes < 1070:
        return {"due": due, "day": d.isoformat(), "shift": _WHITE}
    if minutes >= 1070:
        return {"due": due, "day": d.isoformat(), "shift": _NIGHT}
    prev = d - datetime.timedelta(days=1)
    return {"due": due, "day": prev.isoformat(), "shift": _NIGHT}


def _shift_occurrence_day(rule, d):
    hh, mm = _hhmm(rule["day_time"])
    due = datetime.datetime(d.year, d.month, d.day, hh, mm, tzinfo=UTC8)
    return {"due": due, "day": d.isoformat(), "shift": _WHITE}


def _shift_occurrence_night(rule, d):
    hh, mm = _hhmm(rule["night_time"])
    minutes = hh * 60 + mm
    if minutes < 480:
        due_d = d + datetime.timedelta(days=1)
    else:
        due_d = d
    due = datetime.datetime(due_d.year, due_d.month, due_d.day, hh, mm, tzinfo=UTC8)
    return {"due": due, "day": d.isoformat(), "shift": _NIGHT}


def _shift_occurrences(rule, d):
    return [_shift_occurrence_day(rule, d), _shift_occurrence_night(rule, d)]


def _is_on_day(rule, d, start):
    kind = rule["kind"]
    if kind == "daily":
        return True
    if kind == "weekly":
        return d.weekday() == rule["weekday"]
    if kind == "month_start":
        return d.day == 1
    if kind == "month_end":
        return d.day == calendar.monthrange(d.year, d.month)[1]
    if kind == "interval":
        diff = (d - start).days
        return diff >= 0 and diff % rule["every_days"] == 0
    raise ValueError("unexpected kind: {!r}".format(kind))


def _temporary_occurrences(rule, d, start):
    if d < start:
        return []
    diff = (d - start).days
    limit_type = rule["limit_type"]
    limit = rule["limit"]

    if rule["cadence"] == "daily":
        if diff >= limit:
            return []
        return [_single_occurrence(rule, d)]

    # shift cadence
    if limit_type == "days":
        if diff >= limit:
            return []
        return _shift_occurrences(rule, d)

    # shift cadence, occurrences limit
    day_idx = diff * 2 + 1
    night_idx = day_idx + 1
    result = []
    if day_idx <= limit:
        result.append(_shift_occurrence_day(rule, d))
    if night_idx <= limit:
        result.append(_shift_occurrence_night(rule, d))
    return result


def occurrences_for_date(rule, day):
    """Return scheduled occurrences for one nominal roster day.

    ``rule`` is a normalized dict (see ``normalize_frequency``) whose
    ``start_date`` may be a YYYY-MM-DD string or a date. ``day`` may be a
    date or YYYY-MM-DD string; both are parsed internally. Returns a list of
    dicts, possibly empty.
    """
    d = _to_date(day)
    start = _to_date(rule["start_date"])
    if d < start:
        return []

    if rule["kind"] == "shift":
        return _shift_occurrences(rule, d)
    if rule["kind"] == "temporary":
        return _temporary_occurrences(rule, d, start)
    if not _is_on_day(rule, d, start):
        return []
    return [_single_occurrence(rule, d)]

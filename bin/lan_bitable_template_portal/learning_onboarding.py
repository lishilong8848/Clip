"""Entry-date policy for assigned daily learning."""

import calendar
import datetime as dt
import re

from .personnel_source import TZ

ENTRY_DATE_FIELD = '入职日期'


def normalize_entry_date(value):
    """Accept Feishu date milliseconds or an unambiguous date value."""
    if isinstance(value, dict):
        for key in ('date', 'value', 'text'):
            if key in value:
                return normalize_entry_date(value[key])
        return ''
    if isinstance(value, list):
        dates = [normalize_entry_date(item) for item in value]
        return dates[0] if dates and dates[0] and len(set(dates)) == 1 else ''
    if isinstance(value, str):
        value = value.strip()
        if not re.fullmatch(r'\d{4}[-/]\d{1,2}[-/]\d{1,2}', value):
            return ''
        try:
            return dt.date(*map(int, re.split(r'[-/]', value))).isoformat()
        except ValueError:
            return ''
    if type(value) in (int, float) and value >= 100_000_000_000:
        try:
            return dt.datetime.fromtimestamp(value / 1000, TZ).date().isoformat()
        except (OSError, OverflowError, ValueError):
            return ''
    return ''


def directory_record(directory, person):
    """Resolve trusted record ids or unique login ids, never display names."""
    record_ids = {str(person['record_id'])} if person.get('record_id') else set()
    record_ids.update(alias.removeprefix('directory:') for alias in (person.get('aliases') or ())
                      if isinstance(alias, str) and alias.startswith('directory:'))
    login_ids = set(person.get('login_ids') or ())
    if person.get('open_id'):
        login_ids.add(person['open_id'])
    candidates = []
    for rid, row in directory.items():
        explicit = rid in record_ids or row.get('id') in record_ids
        linked = bool(login_ids.intersection(row.get('login_ids') or ()))
        if explicit or linked:
            candidates.append((row, explicit, linked))
    if len(candidates) != 1:
        return {}
    row, explicit, linked = candidates[0]
    if (record_ids and not explicit or login_ids and not linked
            or not row.get('selectable') or row.get('inactive')):
        return {}
    return row


def directory_for_learning(service):
    from .learning_personal import _login_directory

    try:
        people, valid = _login_directory(service)
        return people if valid else {}
    except Exception:
        return {}


def daily_eligibility(person, date, directory):
    """Return (state, resume date) using the Beijing roster date."""
    row = directory_record(directory, person)
    try:
        entered = dt.date.fromisoformat(row.get('entry_date') or '')
        duty_date = dt.date.fromisoformat(date)
        if entered > duty_date:
            return 'unknown', ''
        year, month = (entered.year + 1, 1) if entered.month == 12 else (entered.year, entered.month + 1)
        anniversary = dt.date(year, month, min(entered.day, calendar.monthrange(year, month)[1]))
    except (TypeError, ValueError, OverflowError):
        return 'unknown', ''
    return ('exempt' if duty_date < anniversary else 'eligible'), anniversary.isoformat()

"""Daily-learning deadlines; separate from the pre-shift checklist schedule."""
import datetime as dt

from .pre_shift_checks import windows as duty_windows


def windows(day, shift):
    start, end = duty_windows(day, shift)
    return start, end.replace(hour=9) if shift == '夜' else end


def active_night_date(service, person, current):
    """Keep a verified previous-night assignment visible until its learning deadline."""
    day = (current.date() - dt.timedelta(days=1)).isoformat()
    if not person or not windows(day, '夜')[0] <= current < windows(day, '夜')[1]:
        return None
    pid = person['id']
    paper = service._get('paper', 'personal:' + day + ':' + pid) or {}
    if paper.get('person_id') == pid and paper.get('shift') == '夜' and not paper.get('deleted_at'):
        return day
    from .learning_completion import _snapshot
    snapshot = _snapshot(service, day)
    if not snapshot:
        return None
    logins, scopes = set(person.get('login_ids') or []), set(person.get('scopes') or [])
    for duty in snapshot['people']:
        if (duty.get('shift') == '夜' and duty.get('scope') in scopes
                and (duty.get('person_id') == pid or duty.get('open_id') in logins)):
            return day
    return None

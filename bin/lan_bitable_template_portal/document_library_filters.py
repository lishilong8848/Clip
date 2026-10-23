"""Field filters shared by cached listing and cross-page maintenance selection."""
from datetime import datetime, timedelta, timezone
import json
import math

from openclaw_service.assistant.lighthouse_ai import AssistantError


def parse_filters(value, fields):
    try:
        if isinstance(value, str):
            if len(value) > 16000:
                raise ValueError()
            value = json.loads(value)
        if value is None:
            value = []
        if not isinstance(value, list) or len(value) > 20:
            raise ValueError()
        result = []
        for item in value:
            if not isinstance(item, dict) or set(item) - {'field', 'op', 'value'}:
                raise ValueError()
            field = fields.get(item.get('field'))
            op, term = item.get('op'), item.get('value', '')
            if field is None or op not in {'contains', 'eq', 'empty', 'not_empty', 'gte', 'lte'}:
                raise ValueError()
            kind = field.get('type')
            if op not in {'empty', 'not_empty'}:
                if not isinstance(term, (str, int, float, bool)) or len(str(term)) > 500 or term == '':
                    raise ValueError()
                if kind == 2:
                    if op not in {'eq', 'gte', 'lte'}:
                        raise ValueError()
                    term = float(term)
                    if not math.isfinite(term):
                        raise ValueError()
                elif kind in {5, 1001, 1002}:
                    if op not in {'eq', 'gte', 'lte'}:
                        raise ValueError()
                    term = datetime.strptime(str(term), '%Y-%m-%d').date().isoformat()
                elif op in {'gte', 'lte'}:
                    raise ValueError()
            result.append({**item, 'value': term, 'type': kind})
        return result
    except (ValueError, TypeError, KeyError):
        raise AssistantError('字段筛选条件无效，请重新选择字段和筛选值。') from None


def _terms(value):
    if isinstance(value, dict):
        if 'type' in value and 'value' in value:
            return _terms(value['value'])
        return [str(value[key]) for key in ('name', 'text', 'label', 'link', 'id') if value.get(key) is not None]
    if isinstance(value, list):
        return [term for item in value for term in _terms(item)]
    if value is None:
        return []
    if isinstance(value, bool):
        return ['true', '是'] if value else ['false', '否']
    return [str(value)]


def matches_filters(values, filters):
    for item in filters:
        raw = values.get(item['field'])
        if isinstance(raw, dict) and 'type' in raw and 'value' in raw:
            raw = raw['value']
        op, expected = item['op'], item['value']
        empty = raw is None or raw == '' or raw == [] or raw == {}
        if op in {'empty', 'not_empty'}:
            if empty != (op == 'empty'):
                return False
            continue
        if empty:
            return False
        if item['type'] in {2, 5, 1001, 1002}:
            try:
                actual = float(raw)
                if not math.isfinite(actual):
                    return False
                if item['type'] != 2:
                    actual = datetime.fromtimestamp(actual / 1000, timezone(timedelta(hours=8))).date().isoformat()
            except (ValueError, TypeError, OverflowError, OSError):
                return False
            if not {'eq': actual == expected, 'gte': actual >= expected, 'lte': actual <= expected}.get(op, False):
                return False
        else:
            terms, expected = [term.casefold() for term in _terms(raw)], str(expected).casefold()
            if not (any(expected in term for term in terms) if op == 'contains' else expected in terms):
                return False
    return True

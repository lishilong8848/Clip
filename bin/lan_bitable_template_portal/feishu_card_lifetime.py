"""Maximum callback lifetime; timestamps must come from persisted business data."""
from datetime import datetime, timedelta, timezone
import math
import time

MAX_CARD_AGE = 7 * 24 * 3600
EXPIRED_CARD_MESSAGE = '卡片已过期（最长7天有效），请打开灯塔处理；原记录和已提交结果保留。'


def timestamp(value):
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            result = float(value)
        else:
            parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone(timedelta(hours=8)))
            result = parsed.timestamp()
        return result if math.isfinite(result) and result > 0 else 0
    except (TypeError, ValueError, OverflowError):
        return 0


def expired(issued_at, *, deadline=None, now=None):
    issued = timestamp(issued_at)
    if not issued:
        return True  # An undated legacy capability cannot gain a new lifetime.
    end = issued + MAX_CARD_AGE
    if deadline is not None:
        limit = timestamp(deadline)
        if not limit:
            return True
        end = min(end, limit)
    return (time.time() if now is None else now) >= end


def expired_card(url=''):
    elements = [{'tag': 'div', 'text': {'tag': 'plain_text', 'content': EXPIRED_CARD_MESSAGE}}]
    if url:
        elements.append({'tag': 'button', 'text': {'tag': 'plain_text', 'content': '打开灯塔处理'},
                         'behaviors': [{'type': 'open_url', 'default_url': url}]})
    return {'schema': '2.0', 'header': {'title': {'tag': 'plain_text', 'content': '卡片已过期'},
                                     'template': 'grey'}, 'body': {'elements': elements}}

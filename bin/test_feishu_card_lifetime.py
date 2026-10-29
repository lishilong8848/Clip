"""Card age validation does not alter business deadlines or history."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.feishu_card_lifetime import expired, timestamp, MAX_CARD_AGE, expired_card


class LifetimeTests(unittest.TestCase):
    def test_exact_boundary_and_shorter_business_deadline(self):
        issued = timestamp('2026-10-29T10:00:00+08:00')
        self.assertFalse(expired(issued, now=issued + MAX_CARD_AGE - .001))
        self.assertTrue(expired(issued, now=issued + MAX_CARD_AGE))
        self.assertTrue(expired(issued, deadline=issued + 30 * 60, now=issued + 30 * 60))
        self.assertTrue(expired(issued, deadline=issued + MAX_CARD_AGE * 4, now=issued + MAX_CARD_AGE))

    def test_legacy_dates_use_business_timezone_and_bad_dates_fail_closed(self):
        self.assertEqual(timestamp('2026-10-29 10:00:00'), timestamp('2026-10-29T02:00:00Z'))
        for value in [None, '', 'bad', 0, True, float('nan'), float('inf')]:
            self.assertTrue(expired(value))

    def test_expired_card_has_only_web_entrance(self):
        card = expired_card('https://fixture.invalid/learning')
        import json
        self.assertNotIn('callback', json.dumps(card))
        self.assertEqual(card['body']['elements'][-1]['text']['content'], '打开灯塔处理')

    def test_notice_and_question_tickets_keep_stricter_existing_lifetime(self):
        from lan_bitable_template_portal.feishu_notice_cards import _TICKET_TTL_SECONDS
        self.assertLessEqual(_TICKET_TTL_SECONDS, MAX_CARD_AGE)


if __name__ == '__main__': unittest.main()

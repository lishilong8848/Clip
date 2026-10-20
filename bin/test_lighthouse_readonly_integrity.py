"""Read-only query completeness; synthetic payloads only, no business access."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_queries import query_result_state


class ReadonlyIntegrityTests(unittest.TestCase):
    def test_nonempty_page_is_partial_when_more_rows_are_known(self):
        for metadata in ({'has_more': True}, {'total': 55}, {'truncated': True},
                         {'page': 2, 'total': 1}, {'remaining': 54}, {'next_offset': 1}):
            with self.subTest(metadata=metadata):
                state, guidance = query_result_state({'ok': True, 'data': {
                    'items': [{'person_id': 'p1'}], **metadata}})
                self.assertEqual(state, 'partial')
                self.assertIn('完整', guidance)

    def test_learning_history_total_is_never_a_participant_count(self):
        state, guidance = query_result_state({'ok': True, 'api_id': 'GET /api/learning/history',
            'data': {'items': [{'state': 'pending'}], 'total': 1}})
        self.assertEqual(state, 'found')
        self.assertIn('view=participation', guidance)
        self.assertIn('total', guidance)
        self.assertIn('待答', guidance)

    def test_display_truncation_does_not_hide_retained_full_raw_list(self):
        self.assertEqual(query_result_state({'ok': True, 'truncated': True,
            '_raw': {'items': [{'id': 'r1'}], 'total': 1}})[0], 'found')
        self.assertEqual(query_result_state({'ok': True,
            'data': {'items': [], 'total': 55, 'page': 99}})[0], 'empty_page')


if __name__ == '__main__':
    unittest.main()

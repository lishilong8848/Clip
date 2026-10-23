"""ALL is a query filter, never an expansion of source or account permissions."""
import uuid

from bin.test_document_library import LibraryFixture, actor
from openclaw_service.assistant.lighthouse_ai import AssistantError


class AllScopesTests(LibraryFixture):
    def test_all_and_empty_filters_return_same_authorized_records(self):
        for user, count in ((actor(), 1), (self.admin, 2)):
            with self.subTest(user=user['id']):
                expected = self.service.list_records(user, 'manuals', {})
                result = self.service.list_records(user, 'manuals', {'scope': 'ALL'})
                self.assertEqual(result['total'], count)
                self.assertEqual(result['items'], expected['items'])
        self.assertEqual(self.service.list_records(self.admin, 'manuals', {'scope': 'B'})['total'], 1)

    def test_all_reindex_retains_record_permissions(self):
        for user, count in ((actor(), 1), (self.admin, 2)):
            result = self.service.reindex(user, 'manuals', {'operation_id': str(uuid.uuid4()),
                'expected_revision': self.service._table('manuals')['revision'], 'all': True, 'scope': 'ALL'})
            self.assertEqual(result['records'], count)

    def test_invalid_values_still_rejected(self):
        for scope in ('INVALID', ['A', 'B'], {'value': 'A'}):
            with self.subTest(scope=scope), self.assertRaises(AssistantError):
                self.service.list_records(self.admin, 'manuals', {'scope': scope})

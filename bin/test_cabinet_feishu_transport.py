"""Isolated cabinet transport checks; no real Feishu requests."""
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.cabinet_power import CabinetError, CabinetFeishu
from upload_event_module.services.http_client import FeishuHTTPError


class CabinetTransportTests(unittest.TestCase):
    def setUp(self):
        self.remote = CabinetFeishu('fixture-table', 'fixture-app')
        self.remote._http = Mock()
        self.remote.token = Mock(return_value='fixture-token')
        self.remote.require_write = Mock()
        self.sleeper = patch('lan_bitable_template_portal.cabinet_power.time.sleep')
        self.sleep = self.sleeper.start()
        self.addCleanup(self.sleeper.stop)

    def test_repeated_read_is_retried_with_bounded_backoff(self):
        self.remote._http.request_json.side_effect = [
            {'code': 1254608}, {'code': 1254608}, {'code': 0, 'data': {'items': []}},
        ]
        self.assertEqual(self.remote.list_all(), [])
        self.assertEqual(self.remote._http.request_json.call_count, 3)
        self.assertEqual([call.args[0] for call in self.sleep.call_args_list], [1, 2])
        self.remote.require_write.assert_not_called()

    def test_rejected_create_retains_original_body_and_idempotency_token(self):
        self.remote._http.request_json.side_effect = [
            {'code': 1254608}, {'code': 0, 'data': {'record': {'record_id': 'recFixture'}}},
        ]
        self.assertEqual(self.remote.create({'机架': 'A01'}, 'stable-cabinet-operation')['record_id'], 'recFixture')
        calls = self.remote._http.request_json.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(calls[0].kwargs['retries'], 0)
        self.assertTrue(calls[0].kwargs['params']['client_token'])

    def test_persistent_duplicate_rejection_remains_a_failure(self):
        self.remote._http.request_json.return_value = {'code': 1254608}
        with self.assertRaisesRegex(CabinetError, '1254608'):
            self.remote.update('recFixture', {'结果': '成功'})
        self.assertEqual(self.remote._http.request_json.call_count, 5)
        self.assertEqual(sum(call.args[0] for call in self.sleep.call_args_list), 15)

    def test_ambiguous_network_and_invalid_field_writes_are_not_retried(self):
        self.remote._http.request_json.side_effect = FeishuHTTPError('fixture response lost')
        with self.assertRaises(FeishuHTTPError):
            self.remote.update('recFixture', {'结果': '成功'})
        self.remote._http.request_json.assert_called_once()
        self.sleep.assert_not_called()
        self.remote._http.request_json.reset_mock(side_effect=True)
        self.remote._http.request_json.return_value = {'code': 1254060, 'msg': 'TextFieldConvFail'}
        with self.assertRaisesRegex(CabinetError, '1254060'):
            self.remote.update('recFixture', {'结果': '成功'})
        self.remote._http.request_json.assert_called_once()
        self.sleep.assert_not_called()


if __name__ == '__main__':
    unittest.main()

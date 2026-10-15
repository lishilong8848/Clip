"""Isolated public probes and incident notifications; never sends a real message."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from public_access.health import Monitor, read_result


class PublicHealthTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.sender = Mock(return_value=True)
        self.monitor = Monitor('192.168.224.130', 'https://portal.example.com', data=self.root, notify=self.sender)
        self.monitor.started_at = 1000
        self.monitor.probe = Mock(return_value=(False, 'HTTP 502'))
        context = patch('public_access.health.alert_recipients', return_value=['fixture-li', 'fixture-h'])
        context.start()
        self.addCleanup(context.stop)

    def fail_three(self, base=1100):
        for stamp in (base, base + 30, base + 60):
            result = self.monitor.check_once(now=stamp)
        return result

    def test_startup_grace_and_transient_failure_do_not_notify(self):
        for stamp in (1000, 1030, 1060):
            result = self.monitor.check_once(now=stamp)
        self.assertFalse(result['ok'])
        self.sender.assert_not_called()
        self.monitor.probe.return_value = (True, '')
        self.assertTrue(self.monitor.check_once(now=1090)['ok'])
        self.assertEqual(self.monitor.prior['failures'], 0)
        self.sender.assert_not_called()

    def test_failure_notifies_li_and_h_once_then_recovers(self):
        failed = self.fail_three()
        self.assertEqual(set(failed['notified']), {'fixture-li', 'fixture-h'})
        self.assertEqual(self.sender.call_count, 2)
        calls = self.sender.call_args_list
        self.assertNotEqual(calls[0].args[2], calls[1].args[2])
        self.assertIn('HTTP 502', calls[0].args[0])
        self.assertIn('6100', calls[0].args[0])
        for stamp in (1200, 1500, 1800):
            self.monitor.check_once(now=stamp)
        self.assertEqual(self.sender.call_count, 2)
        self.monitor.probe.return_value = (True, '')
        self.assertTrue(self.monitor.check_once(now=1900)['ok'])
        self.monitor.check_once(now=1930)
        self.monitor.check_once(now=1960)
        self.assertEqual(read_result(self.root / 'health.json')['notified'], [])
        self.monitor.probe.return_value = (False, 'HTTP 502')
        self.fail_three(base=2000)
        self.assertEqual(self.sender.call_count, 4)

    def test_flapping_and_restart_keep_one_notification_until_stable_recovery(self):
        incident = self.fail_three()['incident_id']
        for base in (1500, 2100):
            self.monitor.probe.return_value = (True, '')
            for stamp in (base, base + 30):
                result = self.monitor.check_once(now=stamp)
                self.assertTrue(result['ok'])
                self.assertEqual(result['incident_id'], incident)
            resumed = Monitor(self.monitor.host, self.monitor.origin, data=self.root, notify=self.sender)
            resumed.started_at = 1000
            resumed.probe = Mock(return_value=(False, 'HTTP 502'))
            self.monitor = resumed
            self.fail_three(base=base + 60)
        self.assertEqual(self.sender.call_count, 2)

    def test_live_result_restores_generated_links_without_restart_or_network_in_sender(self):
        from public_access import links
        lan = 'http://192.168.224.130:18766'
        with patch.object(links, 'deployment', return_value=(self.monitor.host, self.monitor.origin)), \
                patch.object(links, 'local_host', return_value=True), \
                patch.object(links, 'HEALTH', self.root / 'health.json'), \
                patch('public_access.links.time.time', return_value=1200), \
                patch('httpx.Client', side_effect=AssertionError('Sender must only read saved health')):
            self.assertEqual(links.preferred_base(lan), lan)
            self.fail_three()
            self.assertEqual(links.preferred_base(lan), lan)
            self.monitor.probe.return_value = (True, '')
            self.monitor.check_once(now=1200)
            self.assertEqual(links.preferred_base(lan), self.monitor.origin)
            self.assertEqual(links.preferred_base(self.monitor.origin), self.monitor.origin)
            self.monitor.probe.return_value = (False, 'HTTP 502')
            self.monitor.check_once(now=1200)
            self.assertEqual(links.preferred_base(lan), lan)
            self.assertEqual(links.preferred_base(self.monitor.origin), lan)
            self.assertEqual(self.sender.call_count, 2)

    def test_failed_recipient_retries_without_resending_successful_recipient(self):
        self.sender.side_effect = lambda text, recipient, key: recipient == 'fixture-li'
        failed = self.fail_three()
        first_h_key = self.sender.call_args_list[1].args[2]
        self.assertEqual(failed['notified'], ['fixture-li'])
        self.monitor.check_once(now=1200)
        self.assertEqual(self.sender.call_count, 2)
        self.sender.side_effect = None
        self.sender.return_value = True
        self.monitor.check_once(now=1500)
        self.assertEqual(self.sender.call_count, 3)
        self.assertEqual(self.sender.call_args.args[1:], ('fixture-h', first_h_key))

    def test_incident_deduplication_survives_restart(self):
        self.fail_three()
        resumed = Monitor(self.monitor.host, self.monitor.origin, data=self.root, notify=self.sender)
        resumed.started_at = 1000
        resumed.probe = Mock(return_value=(False, 'HTTP 502'))
        resumed.check_once(now=2000)
        self.assertEqual(self.sender.call_count, 2)

    def test_manual_check_has_no_notification_side_effect(self):
        for stamp in (1200, 1230, 1260):
            self.monitor.check_once(now=stamp, notify=False)
        self.sender.assert_not_called()

    def test_probe_rejects_wrong_service_redirects_and_transport_errors(self):
        monitor = Monitor(self.monitor.host, self.monitor.origin, data=self.root)
        response = {'status': 200, 'value': {'ok': True, 'service': 'clipflow_backend'}}
        def handle(request):
            return httpx.Response(response['status'], json=response['value'])
        monitor.client = httpx.Client(transport=httpx.MockTransport(handle))
        self.addCleanup(monitor.client.close)
        self.assertEqual(monitor.probe(), (True, ''))
        response['status'] = 302
        self.assertEqual(monitor.probe(), (False, 'HTTP 302'))
        response['status'] = 200
        response['value'] = {'ok': True, 'service': 'another-app'}
        self.assertFalse(monitor.probe()[0])
        response['value'] = {'large': 'x' * 4096}
        self.assertFalse(monitor.probe()[0])
        monitor.client.close()
        monitor.client = Mock()
        monitor.client.stream.side_effect = TimeoutError()
        self.assertFalse(monitor.probe()[0])

    def test_stop_does_not_write_a_late_success_or_send_an_alert(self):
        self.monitor.stop()
        self.monitor.probe.return_value = (True, '')
        self.monitor.check_once(now=2000)
        self.assertFalse((self.root / 'health.json').exists())
        self.sender.assert_not_called()

    def test_background_worker_is_single_stops_and_closes_http(self):
        monitor = self.monitor
        monitor.client = Mock()
        with patch('public_access.health.local_host', return_value=True):
            monitor.start()
            first = monitor.thread
            monitor.start()
            self.assertIs(monitor.thread, first)
            monitor.stop()
            first.join(4)
            self.assertFalse(first.is_alive())
        monitor.client.close.assert_called_once()
        self.assertEqual(read_result(self.root / 'health.json')['state'], 'stopped')
        self.sender.assert_not_called()


if __name__ == '__main__':
    unittest.main()

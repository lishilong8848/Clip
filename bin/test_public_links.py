"""Message senders read a local health result, without DNS/HTTP probes."""
import sys
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from public_access import links
from public_access.config import atomic_json


class PublicLinkTests(unittest.TestCase):
    def setUp(self):
        self.origin = 'https://portal.example.com'
        self.lan = 'http://192.168.224.130:18766'
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.file = Path(self.temp.name) / 'health.json'
        self.healthy = {'ok': True, 'state': 'healthy', 'public_url': self.origin,
            'deployment_host': '192.168.224.130', 'checked_at': time.time()}
        for target, options in (
            ('deployment', {'return_value': ('192.168.224.130', self.origin)}),
            ('local_host', {'return_value': True}), ('HEALTH', {'new': self.file}),
        ):
            context = patch.object(links, target, **options)
            context.start()
            self.addCleanup(context.stop)

    def test_public_links_only_read_the_local_file_without_network(self):
        atomic_json(self.file, self.healthy)
        with patch('httpx.Client', side_effect=AssertionError('Link generation must not probe')):
            for _ in range(50):
                self.assertEqual(links.preferred_base(self.lan), self.origin)

    def test_missing_failed_stale_wrong_origin_and_future_results_use_lan(self):
        self.assertEqual(links.preferred_base(self.lan), self.lan)
        for change in ({'ok': False}, {'state': 'preparing'}, {'state': 'stopped'},
                {'checked_at': time.time() - 76}, {'checked_at': time.time() + 100},
                {'public_url': 'https://wrong.example.com'}, {'deployment_host': '192.168.224.122'}, {'checked_at': True}):
            with self.subTest(change=change):
                atomic_json(self.file, {**self.healthy, **change})
                self.assertEqual(links.preferred_base(self.lan), self.lan)
        self.file.write_text('{broken', encoding='utf-8')
        self.assertEqual(links.preferred_base(self.lan), self.lan)

    def test_public_request_origin_falls_back_to_http_lan_not_https_lan(self):
        with patch('upload_event_module.config.config.lan_template_portal_port', 18766):
            self.assertEqual(links.preferred_base(self.origin), self.lan)
            self.assertEqual(links.preferred_base(self.origin + ':18766'), self.lan)

    def test_dev_host_does_not_probe_or_send_its_signed_links_to_production(self):
        atomic_json(self.file, self.healthy)
        links.local_host.return_value = False
        self.assertEqual(links.preferred_base(self.lan), self.lan)
        links.local_host.return_value = True
        links.deployment.return_value = None
        self.assertEqual(links.preferred_base(self.lan), self.lan)

    def test_signature_and_scheduled_links_share_the_selector(self):
        from lan_bitable_template_portal.portal_service import MaintenancePortalService
        service = Mock()
        service._signature_lan_base_url.return_value = self.lan
        atomic_json(self.file, self.healthy)
        self.assertEqual(MaintenancePortalService._signature_public_base_url(service, scope='E'), self.origin)
        service._signature_lan_base_url.assert_called_once_with(scope='E', request_base_url='')


if __name__ == '__main__':
    unittest.main()

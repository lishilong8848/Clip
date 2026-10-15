"""Stable frontend entry metadata and web MIME types, independent of Windows registry."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from clipflow_backend.main import FastAPIPortalController


class FrontendDeliveryTests(unittest.TestCase):
    def test_entry_is_not_cached_and_module_types_ignore_registry(self):
        controller = FastAPIPortalController(host='127.0.0.1', port=18766)
        with tempfile.TemporaryDirectory() as temp, \
                patch('clipflow_backend.main.portal_static_roots', return_value=[Path(temp).resolve()]), \
                patch('clipflow_backend.main.mimetypes.guess_type', return_value=('text/plain', None)):
            for name, media, cache in (
                ('index.html', 'text/html', 'no-store'),
                ('entry.json', 'application/json', 'no-store'),
                ('app-test.js', 'text/javascript', 'public, max-age=86400'),
                ('res-test.css', 'text/css', 'public, max-age=86400'),
            ):
                with self.subTest(name=name):
                    path = Path(temp) / name
                    path.write_text('fixture', encoding='utf-8')
                    response = controller._static_file_response(Mock(headers={}), path)
                    self.assertEqual(response.status_code, 200)
                    self.assertTrue(response.headers['content-type'].startswith(media))
                    self.assertEqual(response.headers['cache-control'], cache)
                    again = controller._static_file_response(Mock(headers={'if-none-match': response.headers['etag']}), path)
                    self.assertEqual(again.status_code, 304)
                    self.assertEqual(again.headers['cache-control'], cache)

    def test_v2_upgrade_rotates_names_away_from_the_old_flat_generation(self):
        source = (Path(__file__).parent / 'lan_bitable_template_portal/frontend/vite.workspace-v2.config.ts').read_text(encoding='utf-8')
        for pattern in ('app-v2-[hash].js', 'part-v2-[hash].js', 'res-v2-[hash][extname]'):
            self.assertIn(pattern, source)


if __name__ == '__main__':
    unittest.main()

"""Unified public entrypoints; internal workers require an exact parent."""
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service import __main__ as worker
from openclaw_service.protocol import BAT, PROJECT


class UnifiedEntryTests(unittest.TestCase):
    def test_only_public_main_entries_are_packaged(self):
        self.assertTrue((PROJECT / BAT).is_file())
        self.assertTrue((PROJECT / 'bin/refactored_main.py').is_file())
        self.assertFalse((PROJECT / '启动程序openclaw.bat').exists())
        self.assertFalse((PROJECT / '启动程序openclaw.py').exists())
        source = (PROJECT / BAT).read_text(encoding='utf-8')
        self.assertIn('refactored_main.py', source)
        self.assertNotIn('启动程序openclaw', source)
        self.assertNotIn('start ""', source.lower())

    def test_main_entry_reaches_background_assistant_through_portal(self):
        source = (PROJECT / 'bin/refactored_main.py').read_text(encoding='utf-8')
        self.assertIn('BackendProcessPortalController as PortalServerController', source)
        self.assertIn('target=_start_portal_worker', source)
        backend = (PROJECT / 'bin/clipflow_backend/main.py').read_text(encoding='utf-8')
        self.assertIn('install_lighthouse_routes(app, self, PortalRuntime)', backend)
        routes = (PROJECT / 'bin/lan_bitable_template_portal/lighthouse_routes.py').read_text(encoding='utf-8')
        self.assertIn('preparation = asyncio.create_task(connect())', routes)
        self.assertIn('await client.close()', routes)

    def test_internal_worker_cannot_start_without_a_parent(self):
        with self.assertRaises(SystemExit) as raised:
            worker.main([])
        self.assertEqual(raised.exception.code, 2)

    def test_closed_parent_does_not_prepare_or_start_service(self):
        with patch('upload_event_module.services.process_lifetime.start_parent_exit_watchdog', return_value=False) as watchdog, \
                patch.object(worker, 'serve', side_effect=AssertionError('dead parent must not start')), \
                patch.object(worker, 'InstanceLock', side_effect=AssertionError('dead parent must not lock')):
            self.assertEqual(worker.main(['--parent-pid', '42']), 1)
        watchdog.assert_called_once_with(42)

    def test_parent_watchdog_precedes_heavy_host_import(self):
        source = (PROJECT / 'bin/openclaw_service/__main__.py').read_text(encoding='utf-8')
        self.assertLess(source.index('if not start_parent_exit_watchdog(args.parent_pid)'),
                        source.index('with InstanceLock(args.project_root, args.state_root):'))
        self.assertIn("'parent_pid': args.parent_pid", source)
        self.assertNotIn('watch_existing', source)


if __name__ == '__main__':
    unittest.main()

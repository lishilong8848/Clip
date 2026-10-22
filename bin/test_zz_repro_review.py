"""Temporary reproduction harness - deleted after review."""
import sys
import tempfile
import unittest
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import plan_convergence_notifications as notifications
from lan_bitable_template_portal import plan_convergence_recipients as recipients
from lan_bitable_template_portal import plan_convergence_auth as auth_mod
from lan_bitable_template_portal.portal_auth import PortalAuthManager
from lan_bitable_template_portal.state_store import LanPortalStateStore


def accept(text, people, *, message_uuid):
    return True, 'ok', [{'open_id': people[0], 'code': 0, 'message_id': 'om_x'}]


class Base(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.store = LanPortalStateStore(Path(folder.name) / 's.sqlite3')
        self.addCleanup(self.store.shutdown_write_worker)
        self.addCleanup(auth_mod.bind_store, auth_mod._store)
        self.clock = patch.object(notifications.time, 'time', return_value=1800000000.0).start()
        self.addCleanup(patch.stopall)
        self.service = SimpleNamespace(
            _building_codes_from_value=lambda value: [c for c in '110ABCDEH' if c in (value or '')],
            _recipients_for_building_codes=Mock(side_effect=lambda codes, **_: ('', [c + '-duty' for c in codes], '')))
        self.check = Mock(side_effect=lambda record: {'records': [{**record, 'hits': []}]})
        self.send = Mock(side_effect=accept)
        self.worker = notifications.NoticePlanChecks(self.service, self.store, self.check, self.send)
        self.auth = None
        self.snapshot = {'schema': 4, 'app_id': 'cli_fixture', 'loaded_at': 1800000000.0, 'people': {'e': {
            'id': 'e', 'scopes': ['B'], 'specialties': ['暖通'], 'position': '工程师', 'inactive': False,
            'selectable': True, 'login_ids': ['ou_eng']}}}
        self.disabled = set()
        self.auth = SimpleNamespace(
            current_feishu_app_id=Mock(return_value='cli_fixture'),
            _account_directory_provider=lambda: self.snapshot,
            _account_directory_loader=Mock(side_effect=TimeoutError('hr down')),
            _account_read_state=lambda: None,
            account_profile=Mock(return_value={}),
            directory_block_reason=Mock(return_value=''),
            _permission_user=Mock(return_value={}),
            _account_index_cache=(None, {}, None),
            _open_id_explicitly_disabled=lambda identity: identity in self.disabled)
        self.auth.directory_person = MethodType(PortalAuthManager.directory_person, self.auth)
        self.auth.current_feishu_open_id = MethodType(PortalAuthManager.current_feishu_open_id, self.auth)
        self.worker.get_engineers = Mock(side_effect=lambda record, scopes: recipients.current_engineers(self.auth, record, scopes))

    def enqueue(self):
        self.worker.enqueue({'notice_type': '设备检修', 'work_type': 'repair', 'action': 'start',
            'title': 'B楼设备作业', 'building': 'B楼', 'building_codes': ['B'], 'specialty': '暖通'},
            operation_id='one', target_record_id='recB')
        return 'latest:recB'

    def drain(self):
        for _ in range(4):
            rows = self.store.lease_outbox_events(notifications.CHANNEL, limit=5, lease_seconds=60)
            for row in rows:
                self.worker.process(row)
            if not rows:
                return

    def job(self):
        return self.store.get_document(notifications.CHANNEL, self.store.get_document(
            notifications.CHANNEL, 'latest:recB')['job_id'])


class Repro(Base):
    def test_D3_hr_outage_sends_to_admin_disabled_engineer(self):
        self.enqueue()
        print('[D3] record major:', self.job()['record'].get('major'),
              '| engineers:', self.worker.get_engineers.call_count,
              self.worker.get_engineers.return_value if self.worker.get_engineers.call_count else None)
        self.send.side_effect = lambda t, p, **k: (accept(t, p, **k) if p == ['B-duty'] else
            (False, 'blocked', [{'open_id': p[0], 'code': 230013}]))
        rows = self.store.list_outbox_events(notifications.CHANNEL, status='pending', limit=50)
        print('[D3] pending rows before drain:', [(r['id'], r['payload']) for r in rows],
              '| workers get_engineers is mock:', self.worker.get_engineers)
        self.drain()                       # duty ok, engineer explicitly rejected -> retry
        print('[D3] get_engineers calls:', self.worker.get_engineers.call_count, '| lease state:', [(r['id'], r['payload'], r.get('status')) for r in
              self.store.list_outbox_events(notifications.CHANNEL, status=None, limit=50)])
        print('[D3] cycle1 sends:', [c.args[1] for c in self.send.call_args_list],
              '| receipts:', self.job()['receipts'])
        self.clock.return_value += 400     # directory now >300s old; HR is down
        self.disabled.add('ou_eng')        # admin disabled the account meanwhile
        sent = []
        self.send.side_effect = lambda t, p, **k: (sent.append(list(p)), accept(t, p, **k))[1]
        self.drain()
        print('[D3] resends:', sent, '| warn:', self.job().get('engineer_warning'))
        self.assertNotIn(['ou_eng'], sent)

    def test_D2_stale_snapshot_error_message(self):
        self.snapshot['loaded_at'] = 1800000000.0 - 301
        with self.assertRaises(RuntimeError) as ctx:
            recipients.current_engineers(self.auth, {'major': '暖通'}, ['B'])
        print('[D2] raised:', repr(ctx.exception))
        self.assertEqual(str(ctx.exception), 'TimeoutError')


if __name__ == '__main__':
    unittest.main()

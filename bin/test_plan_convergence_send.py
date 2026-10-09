"""No network, Feishu sends or business writes in these preflight checks."""
import copy
from pathlib import Path
import sys
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parent))
from bin.test_plan_convergence import MemoryStore
from lan_bitable_template_portal.plan_convergence_send import NoticeConvergenceGuard, NS
from lan_bitable_template_portal.portal_service import MaintenancePortalService, PortalConfirmationRequiredError
from lan_bitable_template_portal.plan_convergence import PlanConvergenceService
from lan_bitable_template_portal.plan_convergence_maintenance import ongoing_records, match_records
from lan_bitable_template_portal import plan_convergence_auth


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.matcher = SimpleNamespace(_check_records=Mock(return_value={'records': [{'hits': []}]}))
        self.service = SimpleNamespace(_building_codes_from_value=MaintenancePortalService._building_codes_from_value)
        self.guard = NoticeConvergenceGuard(self.service, self.store, lambda: self.matcher)
        self.payload = {'action': 'start', 'work_type': 'repair', 'notice_type': '设备检修', 'building': 'A楼',
            'title': 'A-201冷机检修', 'repair_device': 'A-201-CRAC-01', '_auth_open_id': 'ou_A', 'record_id': 'rec1'}

    def challenge(self, payload=None):
        with self.assertRaises(PortalConfirmationRequiredError) as caught:
            self.guard.check(payload or self.payload)
        self.assertEqual(caught.exception.details['kind'], 'plan_convergence_unmatched')
        return caught.exception.details['confirmation']

    def test_no_match_requires_real_ticket_and_exact_owner_and_target(self):
        ticket = self.challenge()
        self.guard.check({**self.payload, 'plan_convergence_confirmation': ticket})
        self.assertEqual(self.matcher._check_records.call_count, 1)
        for change in ({'_auth_open_id': 'ou_B'}, {'title': '另一设备'}, {'record_id': 'rec2'}):
            self.challenge({**self.payload, 'plan_convergence_confirmation': ticket, **change})

    def test_expired_or_fabricated_confirmation_cannot_bypass(self):
        ticket = self.challenge()
        self.store.documents[(NS, 'ticket:' + ticket)]['expires'] = 0
        self.challenge({**self.payload, 'plan_convergence_confirmation': ticket})
        self.challenge({**self.payload, 'plan_convergence_confirmation': 'x' * 48})

    def test_de_end_events_and_other_notices_do_not_contact_vpn(self):
        for change in ({'building': 'D楼'}, {'building': 'E楼'}, {'building': 'D、E楼'}, {'action': 'end'},
                       {'work_type': 'event'}, {'work_type': 'maintenance'}):
            self.guard.check({**self.payload, **change})
        self.matcher._check_records.assert_not_called()

    def test_mixed_buildings_keep_only_non_de_and_known_hits_skip_prompt(self):
        self.matcher._check_records.return_value = {'records': [{'hits': [{'blockId': '1'}]}]}
        self.guard.check({**self.payload, 'building': 'A、D楼'})
        record = self.matcher._check_records.call_args.args[0][0]
        self.assertEqual(record['building'], 'A楼')
        self.guard.check({**self.payload, 'building': 'A、D楼'})
        self.assertEqual(self.matcher._check_records.call_count, 1)

    def test_network_failure_is_unknown_not_matched_and_can_be_confirmed(self):
        self.matcher._check_records.side_effect = TimeoutError('VPN unavailable')
        ticket = self.challenge()
        self.guard.check({**self.payload, 'plan_convergence_confirmation': ticket})
        self.assertEqual(self.matcher._check_records.call_count, 1)

    def test_qt_raw_text_parser_and_repair_change(self):
        for kind, marker in (('repair', '设备检修'), ('change', '变更通告')):
            ticket = self.challenge({**self.payload, 'work_type': kind, 'notice_type': marker, 'action': 'upload',
                'text': f'【{marker}】状态：开始\n【名称】A楼A-201空调检修\n【位置】A楼\n【内容】空调维修'})
            self.assertEqual(len(ticket), 48)


class MatchAndPointTests(unittest.TestCase):
    def setUp(self):
        self.old = plan_convergence_auth._store
        self.store = MemoryStore()
        self.records = [{'target_record_id': 'recA', 'notice_type': '设备检修', 'work_type': 'repair',
            'building': 'A楼', 'title': 'A-201空调检修', 'status': '开始', 'repair_device': 'A-201-01'}]
        self.service = PlanConvergenceService(self.store, lambda _: copy.deepcopy(self.records))
        self.addCleanup(self.service.close)
        self.addCleanup(plan_convergence_auth.bind_store, self.old)

    def test_de_only_check_does_not_read_any_blocks(self):
        records = ongoing_records([{**self.records[0], 'building': 'D楼', 'title': 'D-201空调检修', 'repair_device': 'D-201-01'}])
        with patch.object(self.service, 'blocks', side_effect=AssertionError('D/E must not query')):
            result = self.service._check_records(records, 'maintenance')
        self.assertEqual(result['records'][0]['check_status'], 'skipped')

    def test_points_persist_and_open_directly_without_remote_fetch(self):
        details = {'alarmBlockDetailResultList': [{'instances': 'A-201-01', 'spaceModel': 'A-201',
            'relateConfig': '压差过大', 'alarmConfigId': 'p123', 'classifyModel': '空调'}]}
        with patch.object(self.service, 'blocks', return_value={'items': [{'blockId': '123', 'status': 1,
            'blockName': 'A-201空调', 'endTime': '2026-10-09 18:00'}]}), patch.object(self.service, 'block', return_value=details):
            self.service.maintenance_check('recA')
        with patch.object(self.service, 'block', side_effect=AssertionError('use persisted result')):
            result = self.service.notice_points('repair', 'recA', '123')
        self.assertEqual(result['items'][0]['point'], '压差过大')
        with self.assertRaises(ValueError): self.service.notice_points('repair', 'recA', '456')
        self.records = []
        with self.assertRaises(FileNotFoundError): self.service.notice_points('repair', 'recA', '123')


if __name__ == '__main__':
    unittest.main()

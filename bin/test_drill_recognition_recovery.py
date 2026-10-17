"""Upgrade recovery for persisted failed drill recognition and its read routes."""
import copy
import hashlib
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from .test_drill_management import _fixture_xlsx, _replace_zip_part
from lan_bitable_template_portal import drill_management as drills
from lan_bitable_template_portal.state_store import LanPortalStateStore


def recovery_source():
    return _replace_zip_part(_fixture_xlsx(with_evaluator=True), 'xl/worksheets/sheet1.xml',
        lambda data: data.replace('>3分<'.encode(), '>⊗<'.encode()).replace(
            '>0 时 10 分<'.encode(), '>7分<'.encode()))


class RecognitionRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = LanPortalStateStore(self.root/'state.sqlite3')
        self.service = drills.DrillManagementService(self.store, data_root=self.root/'drills')

    def fail_like_old_version(self, source=None):
        with patch.object(drills, '_step_duration_minutes', side_effect=drills.parse_duration_minutes):
            result = self.service.create_definition(name='旧版识别失败回归', year=2026, month=10,
                file_name='template.xlsx', source=source if source is not None else recovery_source(), actor='fixture-admin')
        self.assertEqual(result['detection_error'], '无法识别预估时间：⊗')
        self.assertEqual(result['configuration']['steps'], [])
        return result

    def test_existing_failed_draft_recovers_on_list_and_survives_restart_without_reupload(self):
        failed = self.fail_like_old_version()
        source = Path(failed['source']['path'])
        before = source.read_bytes()
        restarted = drills.DrillManagementService(self.store, data_root=self.root/'drills')
        with patch.object(drills, '_parse_workbook', wraps=drills._parse_workbook) as parse:
            recovered = restarted.list_definitions('2026-10')[0]
            reread = restarted.get_definition(failed['drill_id'])
            parse.assert_called_once()
        self.assertEqual(recovered['configuration']['steps'][1]['duration_text'], '⊗')
        self.assertEqual(len(recovered['configuration']['steps']), 3)
        self.assertEqual((recovered['version'], recovered['status'], recovered['detection_error']), (2, 'draft', ''))
        self.assertEqual(recovered['configuration'], reread['configuration'])
        self.assertEqual(recovered['source'], failed['source'])
        self.assertEqual(source.read_bytes(), before)
        self.assertEqual(recovered['created_by'], failed['created_by'])
        next_service = drills.DrillManagementService(self.store, data_root=self.root/'drills')
        with patch.object(drills, '_parse_workbook') as parse:
            after = next_service.get_definition(failed['drill_id'])
            parse.assert_not_called()
        self.assertEqual(after['version'], 2)
        self.assertEqual(len(next_service.list_definitions('2026-10')), 1)

    def test_recovery_does_not_overwrite_manual_published_or_executed_records(self):
        failed = self.fail_like_old_version()
        cases = [
            {**failed, 'configuration': {**failed['configuration'], 'record_sheet': '人工选择'}},
            {**failed, 'configuration': {**failed['configuration'], 'mapping': {'steps': {'header_row': 12}}}},
            {**failed, 'status': 'published'},
            {**failed, 'status': 'archived'},
        ]
        for original in cases:
            with self.subTest(status=original['status'], configuration=original['configuration']):
                self.store.put_document(drills.DRILL_DEFINITION_NAMESPACE, failed['drill_id'], original)
                with patch.object(drills, '_parse_workbook') as parse:
                    self.service.get_definition(failed['drill_id'])
                    parse.assert_not_called()
                self.assertEqual(self.store.get_document(drills.DRILL_DEFINITION_NAMESPACE, failed['drill_id']), original)
        self.store.put_document(drills.DRILL_DEFINITION_NAMESPACE, failed['drill_id'], failed)
        execution = {'drill_id': failed['drill_id'], 'scope': 'A', 'version': 4, 'status': 'draft'}
        self.store.put_document(drills.DRILL_EXECUTION_NAMESPACE, failed['drill_id']+':A', execution)
        with patch.object(drills, '_parse_workbook') as parse:
            locked = self.service.get_definition(failed['drill_id'])
            self.assertTrue(locked['configuration_locked'])
            parse.assert_not_called()
        self.assertEqual(self.store.get_document(drills.DRILL_EXECUTION_NAMESPACE, failed['drill_id']+':A'), execution)

    def test_stale_read_cannot_replace_newer_manual_configuration(self):
        stale = self.fail_like_old_version()
        edited = {**stale, 'version': 2, 'configuration': {'record_sheet': '人工配置'}}
        self.store.put_document(drills.DRILL_DEFINITION_NAMESPACE, stale['drill_id'], edited)
        result = self.service._with_template_mappings(stale)
        self.assertEqual(result, edited)
        self.assertEqual(self.store.get_document(drills.DRILL_DEFINITION_NAMESPACE, stale['drill_id']), edited)

    def test_failed_reread_is_throttled_then_recovers_after_temporary_read_error(self):
        failed = self.fail_like_old_version()
        original = copy.deepcopy(failed)
        real_parse = drills._parse_workbook
        with patch.object(drills.time, 'monotonic', return_value=100) as clock, patch.object(
            drills, '_parse_workbook', side_effect=OSError('temporary disk failure')) as parse:
            for _ in range(3):
                result = self.service.get_definition(failed['drill_id'])
                self.assertIn('源文件读取失败', result['detection_error'])
            parse.assert_called_once()
            self.assertEqual(self.store.get_document(drills.DRILL_DEFINITION_NAMESPACE, failed['drill_id']), original)
            clock.return_value = 401
            parse.side_effect = real_parse
            result = self.service.get_definition(failed['drill_id'])
            self.assertEqual(len(result['configuration']['steps']), 3)
            self.assertEqual(parse.call_count, 2)

    def test_two_readers_recover_once_and_reject_old_editor_version(self):
        failed = self.fail_like_old_version()
        other = drills.DrillManagementService(self.store, data_root=self.root/'drills')
        with patch.object(self.store, 'put_document', wraps=self.store.put_document) as save:
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda service: service.get_definition(failed['drill_id']), [self.service, other]))
            save.assert_called_once()
        self.assertEqual([r['version'] for r in results], [2, 2])
        with self.assertRaises(drills.DrillConflictError):
            self.service.save_configuration(failed['drill_id'], results[0]['configuration'], expected_version=1)
        published = self.service.publish(failed['drill_id'], expected_version=2)
        self.assertEqual(published['status'], 'published')

    def test_admin_bootstrap_and_list_deliver_repaired_steps_without_exposing_source_path(self):
        from fastapi.testclient import TestClient
        from clipflow_backend.main import FastAPIPortalController, PortalRuntime
        failed = self.fail_like_old_version()
        controller = FastAPIPortalController(host='127.0.0.1', port=18766)
        controller._drills = self.service
        session = {'session_id':'drill-recovery-fixture','user':{'name':'管理员','open_id':''},
                   'role':'admin','allowed_scopes':['A'],'expires_at':9999999999}
        with patch.object(controller, '_current_session', return_value=session), patch.object(
            PortalRuntime.auth_manager, 'account_profile', return_value={'modules':['drills']}), patch.object(
            PortalRuntime.auth_manager, 'is_admin', return_value=True):
            client = TestClient(controller._build_app())
            self.addCleanup(client.close)
            bootstrap = client.get('/api/drills/bootstrap?month=2026-10')
            self.assertEqual(bootstrap.status_code, 200, bootstrap.text)
            result = bootstrap.json()['data']['drills'][0]
            self.assertEqual(result['drill_id'], failed['drill_id'])
            self.assertEqual((result['version'], len(result['configuration']['steps'])), (2, 3))
            self.assertNotIn('path', result['source'])
            listing = client.get('/api/drills?month=2026-10')
            self.assertEqual(listing.status_code, 200, listing.text)
            self.assertEqual(listing.json()['data']['items'][0]['configuration'], result['configuration'])

    @unittest.skipUnless(Path('D:/下载/21V-JSNTFOC-EOP-KT-308 机房局部高温故障应急处理流程V1.5.xlsx').is_file(), 'local source unavailable')
    def test_actual_workbook_legacy_draft_recovers_seven_steps_without_changing_file(self):
        path = Path('D:/下载/21V-JSNTFOC-EOP-KT-308 机房局部高温故障应急处理流程V1.5.xlsx')
        before = hashlib.sha256(path.read_bytes()).hexdigest()
        with path.open('rb') as source:
            failed = self.fail_like_old_version(source)
        recovered = self.service.get_definition(failed['drill_id'])
        steps = recovered['configuration']['steps']
        self.assertEqual(len(steps), 7)
        self.assertEqual(sum(s['duration_minutes'] for s in steps), 14)
        self.assertEqual(next(s['duration_text'] for s in steps if s['row'] == 17), '⊗')
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)


if __name__ == '__main__':
    unittest.main()

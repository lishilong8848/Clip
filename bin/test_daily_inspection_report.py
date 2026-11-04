import copy
import concurrent.futures
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from openpyxl import load_workbook

from lan_bitable_template_portal.daily_inspection import (
    DailyInspection, RiskError, inspection_report, inspection_report_workbook, install_daily_inspection,
)
from lan_bitable_template_portal.module_access import module_denial
from lan_bitable_template_portal.portal_service import LI_SHILONG_OPEN_ID
from lan_bitable_template_portal.state_store import LanPortalStateStore
import test_daily_inspection as fixtures

DAY, NIGHT, InspectionCloud = fixtures.DAY, fixtures.NIGHT, fixtures.InspectionCloud


class InspectionReportTests(unittest.TestCase):
    def setUp(self):
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.store = LanPortalStateStore(self.folder / 'state.sqlite3')
        self.cloud = InspectionCloud()
        self.service = DailyInspection(self.store, self.cloud)
        self.service.download_archive = lambda attachment: self.cloud.files[attachment['file_token']]

    def test_day_and_overnight_rounds_export_same_cells_and_keep_local_drafts_local(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        self.service.change('a', '甲', DAY, 'finish', fixtures.DailyInspectionTests.submission(active))
        self.cloud.tables[fixtures.SOURCES['D'][0][0]][1]['fields']['湿度'] = ''
        night = self.service.change('b', '乙', NIGHT, 'start', {'duty': NIGHT})['active']
        first = night['batch']['devices'][0]
        first_row = first['parameters'][0]
        drafts = {first['id']: {'values': {first_row['id']: '0'}, 'result': '', 'note': '', 'revision': 0}}
        before = copy.deepcopy(self.service.load('D'))
        report = inspection_report(self.service, 'D', DAY['date'], 'b', '乙', include_active=True,
                                   drafts=drafts, active_id=night['id'])
        self.assertEqual([(item['shift'], item['status']) for item in report['rounds']], [('白', '已归档'), ('夜', '进行中')])
        self.assertTrue(report['rounds'][1]['local_draft'])
        self.assertEqual(self.service.load('D'), before)
        sheet = next(sheet for sheet in report['rounds'][1]['sheets'] if sheet['name'] == '现场核对')
        header, *rows = sheet['rows']
        value_column = header.index('现场读数')
        self.assertEqual(rows[0][value_column], '0')
        self.assertTrue(any(row[value_column] is None for row in rows[1:]))
        book = load_workbook(io.BytesIO(inspection_report_workbook(report)), read_only=True)
        try:
            self.assertIn('白1-现场核对', book.sheetnames)
            self.assertIn('夜1-现场核对', book.sheetnames)
            self.assertIn('夜1-D楼源表快照', book.sheetnames)
            self.assertEqual(list(book['夜1-现场核对'].values)[1][value_column], '0')
        finally:
            book.close()
        only_history = inspection_report(self.service, 'D', DAY['date'], 'c', '丙', drafts={})
        self.assertEqual(len(only_history['rounds']), 1)
        self.assertFalse(inspection_report(self.service, 'D', '2026-11-04', 'b', '乙', include_active=True, drafts={})['rounds'])

    def test_stale_draft_or_failed_archive_is_never_silently_omitted(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        device = active['batch']['devices'][0]
        draft = {device['id']: {'values': {device['parameters'][0]['id']: '1'}, 'result': '', 'note': '', 'revision': 3}}
        with self.assertRaisesRegex(RiskError, '设备|更新'):
            inspection_report(self.service, 'D', DAY['date'], 'a', '甲', include_active=True,
                              drafts=draft, active_id=active['id'])
        self.service.change('a', '甲', DAY, 'finish', fixtures.DailyInspectionTests.submission(active))
        def fail(_attachment):
            raise RiskError('模拟附件下载失败', 503)
        self.service.download_archive = fail
        with self.assertRaisesRegex(RiskError, '模拟附件下载失败'):
            inspection_report(self.service, 'D', DAY['date'], 'a', '甲', include_active=True, drafts={})

    def test_unconfirmed_archive_is_not_reported_as_completed(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        self.cloud.lose_receipt = True
        with self.assertRaisesRegex(RiskError, '模拟响应丢失'):
            self.service.change('a', '甲', DAY, 'finish', fixtures.DailyInspectionTests.submission(active))
        report = inspection_report(self.service, 'D', DAY['date'], 'a', '甲', include_active=True, drafts={})
        self.assertEqual(len(report['rounds']), 1)
        self.assertEqual(report['rounds'][0]['status'], '待归档核验')
        self.assertEqual(inspection_report(self.service, 'D', DAY['date'], 'b', '乙', drafts={})['rounds'][0]['status'], '待归档核验')
        self.cloud.tables[fixtures.ARCHIVE][0]['fields']['巡检快照'] = [{'file_token': 'different'}]
        with self.assertRaisesRegex(RiskError, '附件尚未核验'):
            inspection_report(self.service, 'D', DAY['date'], 'a', '甲', include_active=True, drafts={})


class InspectionProfessionTests(unittest.TestCase):
    def setUp(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.store = LanPortalStateStore(folder / 'state.sqlite3')
        self.cloud = InspectionCloud()
        self.cloud.tables[fixtures.SOURCES['D'][0][0]][1]['fields']['专业'] = '暖通'
        self.service = DailyInspection(self.store, self.cloud)

    def start(self, choices):
        return self.service.change('a', '甲', DAY, 'start', {'duty': DAY, 'professions': choices})['active']

    def confirm(self, active, profession, actor):
        return self.service.change(actor, actor, DAY, 'confirm-profession', {'id': active['id'],
            'profession': profession, 'profession_version': active['profession_versions'][profession], 'reviews': {}})

    def test_concurrent_professions_share_one_round_and_archive_once(self):
        active = self.start(['电气', '暖通'])
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(self.confirm, active, profession, actor)
                       for profession, actor in [('电气', '甲'), ('暖通', '乙')]]
            for future in futures:
                future.result()
        self.assertIsNone(self.service.load('D')['active'])
        self.assertEqual((self.cloud.creates, self.cloud.uploads), (1, 1))
        self.assertEqual(self.cloud.tables[fixtures.ARCHIVE][0]['fields']['设备数'], 2)
        book = load_workbook(io.BytesIO(self.cloud.file), read_only=True)
        try:
            self.assertEqual({(row[0], row[1]) for row in list(book['专业确认'].values)[1:]}, {('电气', '甲'), ('暖通', '乙')})
            columns, *rows = list(book['现场核对'].values)
            self.assertEqual({(row[columns.index('专业')], row[columns.index('填写人')]) for row in rows}, {('电气', '甲'), ('暖通', '乙')})
        finally:
            book.close()

    def test_change_after_confirmation_requires_that_profession_to_confirm_again(self):
        active = self.start(['电气', '暖通'])
        first = self.confirm(active, '电气', '甲')['active']
        electrical = next(device for device in first['batch']['devices'] if device['profession'] == '电气')
        change = {'values': {electrical['parameters'][0]['id']: '12'}, 'result': '', 'note': '', 'revision': 0}
        updated = self.service.change('b', '乙', DAY, 'draft', {'id': active['id'], 'reviews': {electrical['id']: change}})['active']
        self.assertNotIn('电气', updated['profession_confirmations'])
        with self.assertRaisesRegex(RiskError, '已变化'):
            self.confirm(active, '电气', '甲')
        after_other = self.confirm(updated, '暖通', '乙')['active']
        self.assertEqual(self.cloud.creates, 0)
        with self.assertRaisesRegex(RiskError, '尚未确认'):
            self.service.change('a', '甲', DAY, 'finish', {'id': active['id'], 'review_version': after_other['review_version'], 'reviews': {}})
        self.confirm(after_other, '电气', '甲')
        self.assertEqual(self.cloud.creates, 1)

    def test_two_people_can_edit_and_confirm_different_professions_concurrently(self):
        active = self.start(['电气', '暖通'])
        def inspect(profession, actor, value):
            device = next(item for item in active['batch']['devices'] if item['profession'] == profession)
            edit = {'values': {device['parameters'][0]['id']: value}, 'result': '', 'note': '', 'revision': 0}
            saved = self.service.change(actor, actor, DAY, 'draft', {'id': active['id'], 'reviews': {device['id']: edit}})['active']
            return self.confirm(saved, profession, actor)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            for future in [pool.submit(inspect, '电气', '甲', '11'), pool.submit(inspect, '暖通', '乙', '22')]:
                future.result()
        self.assertEqual((self.cloud.creates, self.cloud.uploads), (1, 1))
        book = load_workbook(io.BytesIO(self.cloud.file), read_only=True)
        try:
            columns, *rows = list(book['现场核对'].values)
            self.assertIn(('电气', '11', '甲'), {(row[columns.index('专业')], row[columns.index('现场读数')], row[columns.index('填写人')]) for row in rows})
            self.assertIn(('暖通', '22', '乙'), {(row[columns.index('专业')], row[columns.index('现场读数')], row[columns.index('填写人')]) for row in rows})
        finally:
            book.close()

    def test_one_profession_excludes_other_devices_and_preserves_other_order(self):
        all_active = self.start(['电气', '暖通'])
        identities = [device['id'] for device in all_active['batch']['devices']]
        self.service.change('a', '甲', DAY, 'order', {'id': all_active['id'], 'order': list(reversed(identities))})
        self.service.change('a', '甲', DAY, 'cancel', {'id': all_active['id'], 'review_version': 0})
        single = self.start(['电气'])
        self.assertEqual({item['profession'] for item in single['batch']['devices']}, {'电气'})
        self.service.change('a', '甲', DAY, 'order', {'id': single['id'], 'order': [single['batch']['devices'][0]['id']]})
        self.assertEqual(set(self.service.order('a', 'D')), set(identities))
        self.confirm(single, '电气', '甲')
        self.assertEqual(self.cloud.tables[fixtures.ARCHIVE][0]['fields']['设备数'], 1)
        book = load_workbook(io.BytesIO(self.cloud.file), read_only=True)
        try:
            self.assertEqual({row[3] for row in list(book['现场核对'].values)[1:]}, {'144-AH-109'})
            self.assertEqual(len(list(book['D楼源表快照'].values)), 4)
        finally:
            book.close()

    def test_all_and_each_profession_keep_independent_per_person_orders(self):
        source = self.cloud.tables[fixtures.SOURCES['D'][0][0]]
        extra = copy.deepcopy(source[0])
        extra['record_id'] = 'recDextra'
        extra['fields']['设备编号'] = '144-AH-111'
        source.append(extra)
        active = self.start(['电气', '暖通'])
        ids = [item['id'] for item in active['batch']['devices']]
        full = list(reversed(ids))
        self.service.change('a', '甲', DAY, 'order', {'id': active['id'], 'view': 'ALL', 'order': full})
        electrical = [item['id'] for item in active['batch']['devices'] if item['profession'] == '电气']
        desired = list(reversed([item for item in full if item in electrical]))
        view = self.service.change('a', '甲', DAY, 'order', {'id': active['id'], 'view': '电气', 'order': desired})['active']
        self.assertEqual([item for item in view['orders']['ALL'] if item in ids], full)
        self.assertEqual([item for item in view['orders']['电气'] if item in electrical], desired)
        coworker = self.service.view(self.service.load('D'), 'b', DAY)['active']
        self.assertEqual(coworker['orders']['ALL'], [])
        self.assertEqual(coworker['orders']['电气'], [])
        imported = self.service.change('b', '乙', DAY, 'order', {'id': active['id'], 'view': '电气', 'order': desired})['active']
        self.assertEqual([item for item in imported['orders']['电气'] if item in electrical], desired)
        self.assertEqual(self.service.order('b', 'D'), [])
        self.assertEqual([item for item in self.service.order('a', 'D') if item in ids], full)
        with self.assertRaisesRegex(RiskError, '排序专业'):
            self.service.change('a', '甲', DAY, 'order', {'id': active['id'], 'view': '不属于本轮', 'order': []})

    def test_last_confirmation_recovers_lost_archive_receipt_without_duplicate(self):
        active = self.start(['电气', '暖通'])
        after_first = self.confirm(active, '电气', '甲')['active']
        self.cloud.lose_receipt = True
        with self.assertRaisesRegex(RiskError, '模拟响应丢失'):
            self.confirm(after_first, '暖通', '乙')
        self.cloud.lose_receipt = False
        result = DailyInspection(self.store, self.cloud).change('b', '乙', DAY, 'finish', {'id': active['id']})
        self.assertIsNone(result['active'])
        self.assertEqual((self.cloud.creates, self.cloud.uploads), (1, 1))


class InspectionAccessTests(unittest.TestCase):
    def setUp(self):
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.store = LanPortalStateStore(self.folder / 'state.sqlite3')
        self.cloud = InspectionCloud()
        self.service = DailyInspection(self.store, self.cloud)
        self.service.change('a', '甲', DAY, 'start', {'duty': DAY})
        self.session = {'user': {'open_id': 'ou_person', 'name': '测试'}, 'role': 'user'}
        self.modules = set()
        self.scopes = ['D']
        self.home = 'D'
        self.duty = None
        owner = self

        class Auth:
            def account_profile(self, _owner): return {'enabled': True, 'home_scope': owner.home}
            def session_scopes(self, _session): return owner.scopes

        class Controller:
            def _current_session(self, _request): return owner.session
            def _request_base_url(self, _request): return 'http://testserver'
            def _auth_required_response(self): return JSONResponse({'error': '请登录'}, status_code=401)
            async def _read_json_request(self, request, **_kwargs): return await request.json()

        self.runtime = SimpleNamespace(auth_manager=Auth(), state_store=self.store, service=object())
        self.controller = Controller()
        app = FastAPI()
        install_daily_inspection(app, self.controller, self.runtime)

        @app.middleware('http')
        async def permission(request, call_next):
            denied = await module_denial(request, self.controller, self.runtime)
            return denied or await call_next(request)

        self.enterContext(patch('lan_bitable_template_portal.daily_inspection.RiskCloud', return_value=self.cloud))
        self.enterContext(patch('lan_bitable_template_portal.daily_inspection.allowed_modules', side_effect=lambda *_: self.modules))
        self.enterContext(patch('lan_bitable_template_portal.module_access.allowed_modules', side_effect=lambda *_: self.modules))
        self.enterContext(patch('lan_bitable_template_portal.daily_inspection.resolve_duty', side_effect=lambda *_: self.duty))
        self.client = self.enterContext(TestClient(app))

    def post(self, action, data):
        return self.client.post('/api/daily-inspection/' + action, json=data, headers={'origin': 'http://testserver'})

    def test_home_building_can_inspect_without_daily_module_or_roster(self):
        page = self.client.get('/api/daily-inspection').json()['data']
        self.assertEqual(page['history_scopes'], ['D'])
        self.assertEqual(page['active']['duty']['scope'], 'D')
        self.assertTrue(page['can_edit'])
        own = self.post('day-sheet', {'scope': 'D', 'date': DAY['date'], 'drafts': {}})
        self.assertEqual(own.status_code, 200)
        self.assertEqual(len(own.json()['data']['rounds']), 1)
        self.assertEqual(self.post('day-sheet', {'scope': 'E', 'date': DAY['date'], 'drafts': {}}).status_code, 403)
        device = page['active']['batch']['devices'][0]
        review = {'values': {device['parameters'][0]['id']: '8'}, 'result': '', 'note': '', 'revision': 0}
        saved = self.post('draft', {'id': page['active']['id'], 'reviews': {device['id']: review}})
        self.assertEqual(saved.status_code, 200)
        self.assertEqual(self.post('history-edit', {'id': 'recX', 'changes': {}}).status_code, 403)
        self.assertEqual(self.client.get('/api/daily-inspection?view_scope=E').status_code, 403)
        self.session['source'] = 'admin_impersonation'
        self.assertEqual(self.post('draft', {'id': page['active']['id'], 'reviews': {}}).status_code, 403)

    def test_history_is_readable_for_local_users_and_drafts_are_account_isolated(self):
        active = self.service.view(self.service.load('D'), 'a', DAY)['active']
        self.service.change('a', '甲', DAY, 'finish', fixtures.DailyInspectionTests.submission(active))
        rid = self.service.archive_history('D', DAY['date'])[0]['id']
        with patch.object(DailyInspection, 'download_archive', side_effect=lambda attachment: self.cloud.files[attachment['file_token']]):
            first = self.client.get('/api/daily-inspection/history-record', params={'scope': 'D', 'id': rid}).json()['data']
            first_recent = self.client.get('/api/daily-inspection').json()['data']['history'][0]['draft_key']
            self.session['user']['open_id'] = 'ou_other'
            second = self.client.get('/api/daily-inspection/history-record', params={'scope': 'D', 'id': rid}).json()['data']
            second_recent = self.client.get('/api/daily-inspection').json()['data']['history'][0]['draft_key']
        self.assertTrue(first['can_edit'])
        self.assertTrue(second['can_edit'])
        self.assertNotEqual(first['draft_key'], second['draft_key'])
        self.assertNotEqual(first_recent, second_recent)

    def test_local_non_roster_member_can_edit_own_history_only(self):
        active = self.service.view(self.service.load('D'), 'a', DAY)['active']
        submission = fixtures.DailyInspectionTests.submission(active)
        original = next(iter(submission['reviews'].values()))
        original.update(result='异常', note='旧备注保留')
        self.service.change('a', '甲', DAY, 'finish', submission)
        rid = self.service.archive_history('D', DAY['date'])[0]['id']
        self.scopes = ['D', 'E']
        with patch.object(DailyInspection, 'download_archive', side_effect=lambda attachment: self.cloud.files[attachment['file_token']]):
            detail = self.client.get('/api/daily-inspection/history-record', params={'scope': 'D', 'id': rid}).json()['data']
            device = detail['devices_data'][0]
            item = device['rows'][0]
            body = {'id': rid, 'version': detail['version'], 'changes': {device['id']: {
                'values': {item['id']: '12'}, 'result': device['result'], 'note': device['note']}}}
            edit = lambda scope: self.client.post('/api/daily-inspection/history-edit', params={'scope': scope},
                json=body, headers={'origin': 'http://testserver'})
            self.assertEqual(edit('E').status_code, 403)
            self.session['source'] = 'admin_impersonation'
            self.assertEqual(edit('D').status_code, 403)
            self.session.pop('source')
            result = edit('D')
            self.assertEqual(result.status_code, 200)
            self.assertTrue(result.json()['data']['can_edit'])
            self.assertEqual(result.json()['data']['devices_data'][0]['rows'][0]['value'], '12')
            self.assertEqual(result.json()['data']['devices_data'][0]['note'], '旧备注保留')
            self.assertEqual(result.json()['data']['devices_data'][0]['result'], '异常')
        self.assertEqual(self.cloud.updates, 1)

    def test_historical_value_edit_does_not_require_an_exception_note(self):
        active = self.service.view(self.service.load('D'), 'a', DAY)['active']
        submission = fixtures.DailyInspectionTests.submission(active)
        next(iter(submission['reviews'].values())).update(result='异常', note='')
        self.service.change('a', '甲', DAY, 'finish', submission)
        rid = self.service.archive_history('D', DAY['date'])[0]['id']
        with patch.object(DailyInspection, 'download_archive', side_effect=lambda attachment: self.cloud.files[attachment['file_token']]):
            detail = self.client.get('/api/daily-inspection/history-record', params={'scope': 'D', 'id': rid}).json()['data']
            device = detail['devices_data'][0]
            row = device['rows'][0]
            body = {'id': rid, 'version': detail['version'], 'changes': {device['id']: {
                'values': {row['id']: '18'}, 'result': device['result'], 'note': device['note']}}}
            response = self.client.post('/api/daily-inspection/history-edit', params={'scope': 'D'},
                json=body, headers={'origin': 'http://testserver'})
            self.assertEqual(response.status_code, 200)
            edited = response.json()['data']['devices_data'][0]
            self.assertEqual((edited['rows'][0]['value'], edited['result'], edited['note']), ('18', '异常', ''))

    def test_non_roster_building_member_can_start_and_confirm_a_profession(self):
        current = self.client.get('/api/daily-inspection').json()['data']
        cancel = self.post('cancel', {'id': current['active']['id'], 'review_version': current['active']['review_version']})
        self.assertEqual(cancel.status_code, 200)
        duty = cancel.json()['data']['duty']
        started = self.post('start', {'duty': duty, 'professions': ['电气']})
        self.assertEqual(started.status_code, 200)
        active = started.json()['data']['active']
        result = self.post('confirm-profession', {'id': active['id'], 'profession': '电气',
            'profession_version': active['profession_versions']['电气'], 'reviews': {}})
        self.assertEqual(result.status_code, 200)
        self.assertIsNone(result.json()['data']['active'])
        self.assertEqual(self.cloud.creates, 1)

    def test_reference_preview_returns_the_current_users_independent_orders(self):
        page = self.client.get('/api/daily-inspection').json()['data']
        active = page['active']
        ids = [device['id'] for device in active['batch']['devices']]
        response = self.post('order', {'id': active['id'], 'view': '电气', 'order': list(reversed(ids))})
        self.assertEqual(response.status_code, 200)
        preview = self.client.get('/api/daily-inspection/source', params={'view_scope': 'D'}).json()['data']
        self.assertEqual(preview['orders']['电气'], list(reversed(ids)))
        self.assertEqual(preview['orders']['ALL'], [])
        self.session['user']['open_id'] = 'ou_other'
        other = self.client.get('/api/daily-inspection/source', params={'view_scope': 'D'}).json()['data']
        self.assertEqual(other['orders']['电气'], [])

    def test_li_shilong_view_and_other_people_on_duty_without_write_expansion(self):
        self.modules, self.scopes = {'daily'}, ['D', 'E']
        self.home = 'H'
        self.session['user']['open_id'] = LI_SHILONG_OPEN_ID
        data = self.client.get('/api/daily-inspection?view_scope=D').json()['data']
        self.assertFalse(data['can_edit'])
        self.assertEqual(data['active']['duty']['scope'], 'D')
        identities = [item['id'] for item in data['active']['batch']['devices']]
        sorted_view = self.client.post('/api/daily-inspection/order?view_scope=D',
            json={'id': data['active']['id'], 'view': 'ALL', 'order': list(reversed(identities))},
            headers={'origin': 'http://testserver'})
        self.assertEqual(sorted_view.status_code, 200)
        self.assertFalse(sorted_view.json()['data']['can_edit'])
        report = self.post('day-sheet', {'scope': 'D', 'date': DAY['date'], 'drafts': {}}).json()['data']
        self.assertEqual(len(report['rounds']), 1)
        self.assertEqual(self.post('draft', {'id': data['active']['id'], 'reviews': {}}).status_code, 403)
        self.session['user']['open_id'] = 'ou_person'
        self.home = 'D'
        self.duty = DAY
        report = self.post('day-sheet', {'scope': 'D', 'date': DAY['date'], 'drafts': {}}).json()['data']
        self.assertEqual(len(report['rounds']), 1)
        self.assertEqual(self.client.get('/api/daily-inspection?view_scope=E').status_code, 403)


if __name__ == '__main__':
    unittest.main()

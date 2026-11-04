import copy
import datetime as dt
import io
import tempfile
import unittest
from pathlib import Path

from openpyxl import load_workbook

from lan_bitable_template_portal.daily_inspection import (
    ARCHIVE, ARCHIVE_VIEW, CN, SOURCES, TEMPLATES, DailyInspection, RiskError, duty_period, resolve_duty,
)
from lan_bitable_template_portal.daily_inspection_reminders import due_slots, process_reminders
from lan_bitable_template_portal.state_store import LanPortalStateStore


BATCH = 1793695864749
DAY = {'scope': 'D', 'date': '2026-11-03', 'shift': '白'}
NIGHT = {**DAY, 'shift': '夜'}


class InspectionCloud:
    def __init__(self):
        self.tables = {TEMPLATES[0]: [self.row('recTemplate', 楼栋='D楼', 专业='电气',
                                             附件=[{'name': '模板.xlsx'}])], ARCHIVE: []}
        for scope, ((table, _), (config, _)) in SOURCES.items():
            common = {'楼栋': scope + '楼', '专业': '电气', '上传日期': BATCH}
            self.tables[table] = [
                self.row('rec' + scope + '1', **common, 记录类型='设备巡检', 设备编号='144-AH-109',
                         位置='变电所', **{'电流(A)': 0}),
                self.row('rec' + scope + '2', **common, 记录类型='设备巡检', 设备编号='144-AH-110',
                         位置='变电所', **{'电流(A)': 6}),
                self.row('rec' + scope + '3', **common, 记录类型='测点明细',
                         设备编号=scope + '-144-AH-109_电量仪', 测点ID='p1', 测点值='0'),
            ]
            self.tables[config] = [self.row('recConfig' + scope, 专业='电气', 测点ID='p1',
                                            测点名称='电流', 目标巡检表='模板.xlsx', 目标工作表='表单', 目标单元格='B2', 启用=True)]
        self.schema = {'文本': 1}
        self.file = None
        self.files = {}
        self.uploads = self.creates = self.updates = 0
        self.lose_receipt = False
        self.fail_before_update = False

    @staticmethod
    def row(rid, **fields):
        return {'record_id': rid, 'fields': fields}

    @staticmethod
    def table_path(_base, table):
        return table

    def all(self, _path):
        return [{'field_name': name, 'type': kind} for name, kind in self.schema.items()]

    def request(self, method, path, **kwargs):
        if path == 'drive/v1/medias/upload_all':
            self.uploads += 1
            self.file = kwargs['files']['file'][1]
            token = f'archive_file_{self.uploads}'
            self.files[token] = self.file
            return {'file_token': token}
        table, operation, *extra = path.split('/')
        data = kwargs.get('json', {})
        if operation == 'fields':
            self.schema[data['field_name']] = data['type']
            return {}
        if extra == ['search']:
            rows = self.tables[table]
            if data.get('filter'):
                rows = [row for row in rows if all(row['fields'].get(condition['field_name']) == condition['value'][0]
                                                   for condition in data['filter']['conditions'])]
            return copy.deepcopy({'items': rows[:kwargs['params']['page_size']], 'total': len(rows), 'has_more': False})
        if method == 'GET':
            return {'record': copy.deepcopy(next(row for row in self.tables[table] if row['record_id'] == extra[0]))}
        if method == 'PUT':
            if self.fail_before_update:
                raise RiskError('模拟写入前断线', 503)
            self.updates += 1
            row = next(row for row in self.tables[table] if row['record_id'] == extra[0])
            row['fields'].update(copy.deepcopy(data['fields']))
            if self.lose_receipt:
                raise RiskError('模拟响应丢失', 503)
            return {'record': copy.deepcopy(row)}
        self.creates += 1
        row = self.row('recArchive' + str(self.creates), **data['fields'])
        self.tables[table].append(row)
        if self.lose_receipt:
            raise RiskError('模拟响应丢失', 503)
        return {'record': row}


class DailyInspectionTests(unittest.TestCase):
    def setUp(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.store = LanPortalStateStore(folder / 'state.sqlite3')
        self.cloud = InspectionCloud()
        self.service = DailyInspection(self.store, self.cloud)

    @staticmethod
    def submission(active):
        return {'id': active['id'], 'review_version': active['review_version'], 'reviews': {
            device['id']: {'values': {item['id']: '0' for item in device['parameters'] if not item.get('read_only')},
                           'result': '正常', 'note': '', 'revision': 0}
            for device in active['batch']['devices']}}

    def test_freezes_duty_building_and_recovers_same_archive_after_restart(self):
        active = self.service.change('owner', '值班员', DAY, 'start', {'duty': DAY})['active']
        self.assertEqual(active['round'], 1)
        first, second = active['batch']['devices']
        self.assertEqual(first['sections'][0]['values']['电流(A)'], 0)
        self.assertTrue(first['points'][0]['template_known'])
        self.service.change('owner', '', DAY, 'order', {'id': active['id'], 'order': [second['id'], first['id']]})
        coworker = self.service.view(self.service.load('D'), 'coworker', DAY)['active']
        self.assertEqual(coworker['batch']['devices'][0]['id'], first['id'])
        self.assertNotEqual(coworker['draft_key'], active['draft_key'])
        source = self.cloud.tables[SOURCES['D'][0][0]]
        for row in source:
            row['fields']['上传日期'] += 3600000
        source[0]['fields']['电流(A)'] = 99
        submitted = self.submission(active)
        submitted['reviews'][first['id']].update(result='异常', note='现场读数异常，已记录')
        self.cloud.lose_receipt = True
        with self.assertRaisesRegex(RiskError, '模拟响应丢失'):
            self.service.change('owner', '当班人员甲', DAY, 'finish', submitted)
        self.assertTrue(self.service.view(self.service.load('D'), 'owner', DAY)['active']['pending'])
        restarted = DailyInspection(self.store, self.cloud)
        result = restarted.change('coworker', '当班人员乙', NIGHT, 'finish', {'id': active['id']})
        self.assertIsNone(result['active'])
        self.assertEqual((self.cloud.creates, self.cloud.uploads), (1, 1))
        self.assertEqual(result['history'][0]['id'], active['id'])
        self.assertEqual(result['history'][0]['actor'], '当班人员甲')
        self.assertEqual(result['history'][0]['duty'], DAY)
        self.assertEqual(result['completed_rounds'], 0)
        archived = self.cloud.tables[ARCHIVE][0]['fields']
        self.assertEqual((archived['开始人员'], archived['巡检人员']), ('值班员', '当班人员甲'))
        self.assertEqual((archived['设备数'], archived['源表记录数']), (2, 3))
        self.assertEqual((archived['现场已核对设备数'], archived['异常设备数']), (2, 1))
        self.assertEqual(archived['开始时间'], active['started'])
        book = load_workbook(io.BytesIO(self.cloud.file), read_only=True)
        try:
            raw = list(book['D楼源表快照'].values)
            self.assertEqual(raw[1][raw[0].index('电流(A)')], 0)
            self.assertEqual(raw[1][raw[0].index('上传日期')], BATCH)
            self.assertNotIn('E楼源表快照', book.sheetnames)
            self.assertEqual(list(book['DE巡检表'].values)[1][2], '144-AH-110')
            columns, *checked = list(book['现场核对'].values)
            self.assertEqual(checked[0][columns.index('设备')], '144-AH-110')
            self.assertEqual(checked[1][columns.index('现场读数')], '0')
            self.assertEqual(checked[1][columns.index('核对结论')], '异常')
            self.assertEqual(checked[1][columns.index('现场备注')], '现场读数异常，已记录')
        finally:
            book.close()
        next_run = restarted.change('owner', '值班员', NIGHT, 'start', {'duty': NIGHT})['active']
        self.assertNotEqual(next_run['id'], active['id'])
        self.assertEqual(next_run['batch']['devices'][0]['id'], second['id'])
        self.assertEqual(next_run['batch']['devices'][1]['sections'][0]['values']['电流(A)'], 99)

    def test_denies_other_buildings_and_rejects_mixed_refresh(self):
        with self.assertRaisesRegex(RiskError, '安排已变化'):
            self.service.change('owner', '', DAY, 'start', {'duty': {**DAY, 'scope': 'E'}})
        self.cloud.tables[SOURCES['D'][0][0]][1]['fields']['上传日期'] += 3600000
        with self.assertRaisesRegex(RiskError, '单一批次'):
            self.service.change('owner', '', DAY, 'start', {'duty': DAY})
        self.assertIsNone(self.service.load('D')['active'])

    def test_two_rounds_are_shared_per_building_and_shift(self):
        for number in (1, 2):
            active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
            self.assertEqual(active['round'], number)
            self.assertEqual(self.service.view(self.service.load('D'), 'b', DAY)['active']['id'], active['id'])
            with self.assertRaisesRegex(RiskError, '已有'):
                self.service.change('b', '乙', DAY, 'start', {'duty': DAY})
            result = self.service.change('b', '乙', DAY, 'finish', self.submission(active))
            self.assertEqual(result['completed_rounds'], number)
        with self.assertRaisesRegex(RiskError, '两次巡检'):
            self.service.change('a', '甲', DAY, 'start', {'duty': DAY})
        self.assertEqual(self.service.change('a', '甲', NIGHT, 'start', {'duty': NIGHT})['active']['round'], 1)
        e_day = {**DAY, 'scope': 'E'}
        self.assertEqual(self.service.change('c', '丙', e_day, 'start', {'duty': e_day})['active']['round'], 1)
        with self.assertRaisesRegex(RiskError, '已变化'):
            self.service.change('c', '丙', e_day, 'finish', {'id': active['id']})

    def test_archive_records_roster_group_and_counts_same_round_across_group_metadata(self):
        duty = {**DAY, 'group': 'B班'}
        active = self.service.change('a', '甲', duty, 'start', {'duty': DAY})['active']
        result = self.service.change('a', '甲', duty, 'finish', self.submission(active))
        self.assertEqual(self.cloud.tables[ARCHIVE][0]['fields']['班组'], 'B班')
        self.assertIn(f'table={ARCHIVE}&view={ARCHIVE_VIEW}', result['history'][0]['url'])
        next_duty = {**DAY, 'group': 'B班、C班'}
        self.assertEqual(self.service.change('b', '乙', next_duty, 'start', {'duty': next_duty})['active']['round'], 2)

    def test_whole_round_uses_source_values_without_requiring_a_note(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        submitted = self.submission(active)
        review = next(iter(submitted['reviews'].values()))
        review['values'].clear()
        review['result'] = '异常'
        self.service.change('a', '甲', DAY, 'finish', submitted)
        book = load_workbook(io.BytesIO(self.cloud.file), read_only=True)
        try:
            header, *rows = list(book['现场核对'].values)
            self.assertEqual(rows[0][header.index('现场读数')], '0')
            self.assertEqual(rows[0][header.index('核对结论')], '异常')
            self.assertIsNone(rows[0][header.index('现场备注')])
        finally:
            book.close()

    def test_unmodified_sources_need_no_manual_input_and_empty_is_not_fabricated(self):
        self.cloud.tables[SOURCES['D'][0][0]][0]['fields']['湿度'] = ''
        self.cloud.tables[SOURCES['D'][0][0]][0]['fields']['缺少源数据'] = '湿度'
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        device = active['batch']['devices'][0]
        value = next(item for item in device['parameters'] if item['name'] == '电流(A)')
        draft = {device['id']: {'values': {value['id']: ''}, 'result': '', 'note': '', 'revision': 0}}
        self.service.change('a', '甲', DAY, 'finish', {'id': active['id'], 'review_version': 0, 'reviews': draft})
        book = load_workbook(io.BytesIO(self.cloud.file), read_only=True)
        try:
            columns, *rows = list(book['现场核对'].values)
            by_name = {(row[columns.index('设备')], row[columns.index('参数')]): row for row in rows}
            changed = by_name[(device['name'], '电流(A)')]
            missing = by_name[(device['name'], '湿度')]
            other = by_name[('144-AH-110', '电流(A)')]
            self.assertIsNone(changed[columns.index('现场读数')])
            self.assertIsNone(missing[columns.index('现场读数')])
            self.assertIn('源数据不完整', missing[columns.index('参数状态')])
            self.assertEqual(other[columns.index('现场读数')], '6')
            self.assertEqual(other[columns.index('核对结论')], '正常')
            self.assertEqual(self.cloud.tables[ARCHIVE][0]['fields']['现场已核对设备数'], 1)
        finally:
            book.close()

    def test_shared_drafts_survive_restart_and_prevent_stale_overwrite(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        submitted = self.submission(active)
        first, second = submitted['reviews']
        first_save = {'id': active['id'], 'reviews': {first: submitted['reviews'][first]}}
        saved = self.service.change('a', '甲', DAY, 'draft', first_save)['active']
        self.assertEqual(saved['review_version'], 1)
        restarted = DailyInspection(self.store, self.cloud)
        retried = restarted.change('a', '甲', DAY, 'draft', first_save)['active']
        self.assertEqual(retried['review_version'], 1)
        changed = copy.deepcopy(first_save)
        changed['reviews'][first]['note'] = '过时的写入'
        with self.assertRaisesRegex(RiskError, '已被其他页面更新'):
            restarted.change('b', '乙', DAY, 'draft', changed)
        with self.assertRaisesRegex(RiskError, '资料已变化'):
            restarted.change('b', '乙', DAY, 'cancel', {'id': active['id'], 'review_version': 0})
        with self.assertRaisesRegex(RiskError, '资料已变化'):
            restarted.change('b', '乙', DAY, 'finish', submitted)
        result = restarted.change('b', '乙', DAY, 'finish', {
            'id': active['id'], 'review_version': 1, 'reviews': {second: submitted['reviews'][second]},
        })
        self.assertEqual(result['completed_rounds'], 1)
        self.assertEqual((self.cloud.creates, self.cloud.uploads), (1, 1))
        book = load_workbook(io.BytesIO(self.cloud.file), read_only=True)
        try:
            rows = list(book['现场核对'].values)
            self.assertEqual({row[-1] for row in rows[1:]}, {'甲', '乙'})
        finally:
            book.close()

    def test_pending_archive_from_previous_version_can_still_be_recovered(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        state = self.service.load('D')
        self.cloud.lose_receipt = True
        with self.assertRaisesRegex(RiskError, '模拟响应丢失'):
            self.service.finish('building:D', state, state['active'], '甲', [])
        self.assertNotIn('现场已核对设备数', self.cloud.tables[ARCHIVE][0]['fields'])
        restarted = DailyInspection(self.store, self.cloud)
        result = restarted.change('b', '乙', NIGHT, 'finish', {'id': active['id']})
        self.assertEqual(result['history'][0]['duty'], DAY)
        self.assertEqual((self.cloud.creates, self.cloud.uploads), (1, 1))

    def test_archive_with_wrong_shift_stays_pending_until_original_record_is_verified(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        self.cloud.lose_receipt = True
        with self.assertRaisesRegex(RiskError, '模拟响应丢失'):
            self.service.change('a', '甲', DAY, 'finish', self.submission(active))
        fields = self.cloud.tables[ARCHIVE][0]['fields']
        fields['班次'] = '夜班'
        self.cloud.lose_receipt = False
        restarted = DailyInspection(self.store, self.cloud)
        with self.assertRaisesRegex(RiskError, '存档回读未确认'):
            restarted.change('a', '甲', DAY, 'finish', {'id': active['id']})
        self.assertIn('completed', restarted.load('D')['active'])
        fields['班次'] = [{'type': 'text', 'text': '白班'}]
        result = restarted.change('a', '甲', DAY, 'finish', {'id': active['id']})
        self.assertIsNone(result['active'])
        self.assertEqual(result['completed_rounds'], 1)
        self.assertEqual((self.cloud.creates, self.cloud.uploads), (1, 1))

    def test_historical_edit_updates_original_record_and_reconciles_lost_receipt(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        self.service.change('a', '甲', DAY, 'finish', self.submission(active))
        listed = self.service.archive_history('D', DAY['date'])
        self.assertEqual(len(listed), 1)
        self.assertFalse(self.service.archive_history('E', DAY['date']))
        self.assertFalse(self.service.archive_history('D', '2026-11-04'))
        rid = listed[0]['id']
        self.service.download_archive = lambda attachment: self.cloud.files[attachment['file_token']]
        with self.assertRaisesRegex(RiskError, '找不到本楼'):
            self.service.archive_detail('E', rid)
        detail = self.service.archive_detail('D', rid)
        device = detail['devices_data'][0]
        row = device['rows'][0]
        payload = {'version': detail['version'], 'changes': {
            device['id']: {'values': {row['id']: ''}, 'result': '异常', 'note': '等现场复测'}}}
        self.cloud.lose_receipt = True
        with self.assertRaisesRegex(RiskError, '模拟响应丢失'):
            self.service.edit_archive('D', rid, payload, '乙')
        restarted = DailyInspection(self.store, self.cloud)
        restarted.download_archive = self.service.download_archive
        archive_fields = self.cloud.tables[ARCHIVE][0]['fields']
        previous_attachment = archive_fields.pop('上版巡检快照')
        with self.assertRaisesRegex(RiskError, '尚未完整确认'):
            restarted.edit_archive('D', rid, payload, '乙')
        archive_fields['上版巡检快照'] = previous_attachment
        result = restarted.edit_archive('D', rid, payload, '乙')
        self.assertEqual((self.cloud.creates, self.cloud.updates, self.cloud.uploads), (1, 1, 2))
        self.assertEqual((result['id'], result['actor'], result['editor'], result['confirmed'], result['abnormal']),
                         (rid, '甲', '乙', 1, 1))
        saved = result['devices_data'][0]
        self.assertEqual((saved['rows'][0]['value'], saved['note'], saved['rows'][0]['reference']), ('', '等现场复测', 0))
        self.assertEqual(self.cloud.tables[ARCHIVE][0]['fields']['上版巡检快照'], [{'file_token': 'archive_file_1'}])
        with self.assertRaisesRegex(RiskError, '已被修改'):
            restarted.edit_archive('D', rid, {**payload, 'changes': {device['id']: {
                'values': {row['id']: '23'}, 'result': '正常', 'note': ''}}}, '丙')
        book = load_workbook(io.BytesIO(self.cloud.files['archive_file_2']), read_only=True)
        try:
            raw = list(book['D楼源表快照'].values)
            self.assertEqual(raw[1][raw[0].index('电流(A)')], 0)
        finally:
            book.close()

    def test_unconfirmed_archive_cannot_be_revised_before_original_retry(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        self.cloud.lose_receipt = True
        with self.assertRaisesRegex(RiskError, '模拟响应丢失'):
            self.service.change('a', '甲', DAY, 'finish', self.submission(active))
        rid = self.cloud.tables[ARCHIVE][0]['record_id']
        with self.assertRaisesRegex(RiskError, '本轮归档尚未确认'):
            self.service.edit_archive('D', rid, {}, '乙')
        result = self.service.change('a', '甲', DAY, 'finish', {'id': active['id']})
        self.assertEqual(result['history'][0]['id'], active['id'])
        self.assertEqual((self.cloud.creates, self.cloud.updates, self.cloud.uploads), (1, 0, 1))

    def test_later_historical_edit_does_not_replace_unconfirmed_write(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        self.service.change('a', '甲', DAY, 'finish', self.submission(active))
        rid = self.cloud.tables[ARCHIVE][0]['record_id']
        self.service.download_archive = lambda attachment: self.cloud.files[attachment['file_token']]
        detail = self.service.archive_detail('D', rid)
        device = detail['devices_data'][0]
        first = {'version': detail['version'], 'changes': {
            device['id']: {'values': {}, 'result': '异常', 'note': '第一份修订'}}}
        second = {'version': detail['version'], 'changes': {
            device['id']: {'values': {}, 'result': '异常', 'note': '另一份修订'}}}
        self.cloud.fail_before_update = True
        with self.assertRaisesRegex(RiskError, '模拟写入前断线'):
            self.service.edit_archive('D', rid, first, '乙')
        self.cloud.fail_before_update = False
        with self.assertRaisesRegex(RiskError, '上一份修改已确认'):
            self.service.edit_archive('D', rid, second, '丙')
        saved = self.service.edit_archive('D', rid, first, '乙')
        self.assertEqual(saved['editor'], '乙')
        self.assertEqual(saved['devices_data'][0]['note'], '第一份修订')
        self.assertEqual((self.cloud.creates, self.cloud.updates, self.cloud.uploads), (1, 1, 2))

    def test_invalid_excel_text_does_not_lock_round(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        submitted = self.submission(active)
        next(iter(submitted['reviews'].values()))['note'] = '复制内容\x0b'
        with self.assertRaisesRegex(RiskError, '现场填写无效'):
            self.service.change('a', '甲', DAY, 'finish', submitted)
        self.assertNotIn('completed', self.service.load('D')['active'])
        result = self.service.change('a', '甲', DAY, 'finish', self.submission(active))
        self.assertEqual(result['history'][0]['id'], active['id'])

    def test_legacy_archive_exposes_blank_review_without_replacing_references(self):
        active = self.service.change('a', '甲', DAY, 'start', {'duty': DAY})['active']
        state = self.service.load('D')
        self.service.finish('building:D', state, state['active'], '甲', [])
        book = load_workbook(io.BytesIO(self.cloud.file))
        del book['现场核对']
        legacy = io.BytesIO()
        book.save(legacy)
        book.close()
        self.cloud.files['archive_file_1'] = legacy.getvalue()
        self.service.download_archive = lambda attachment: self.cloud.files[attachment['file_token']]
        detail = self.service.archive_detail('D', self.cloud.tables[ARCHIVE][0]['record_id'])
        device = detail['devices_data'][0]
        self.assertEqual([(row['reference'], row['value']) for row in device['rows']], [(0, ''), ('0', '')])
        self.assertEqual(detail['round_id'], active['id'])

    def test_roster_and_cutoffs_decide_scope_without_manual_switch(self):
        at = lambda day, hour, minute=0: dt.datetime(2026, 11, day, hour, minute, tzinfo=CN)
        self.assertEqual(duty_period(at(3, 8, 59)), ('2026-11-02', '夜'))
        self.assertEqual(duty_period(at(3, 9)), ('2026-11-03', '白'))
        self.assertEqual(duty_period(at(3, 18)), ('2026-11-03', '夜'))
        self.assertEqual(duty_period(at(4, 8, 59)), ('2026-11-03', '夜'))
        people = [{'open_id': 'ou_a', 'scope': 'D', 'shift': '白'},
                  {'open_id': 'ou_b', 'scope': 'E', 'shift': '夜'}]
        roster = lambda day: {'date': day, 'complete': True, 'people': people}
        self.assertEqual(resolve_duty(roster, {'open_id': 'ou_a'}, ['D', 'E'], at(3, 9)), DAY)
        self.assertIsNone(resolve_duty(roster, {'open_id': 'ou_a'}, ['E'], at(3, 9)))
        self.assertIsNone(resolve_duty(roster, {'open_id': 'ou_a'}, ['D', 'E'], at(3, 18)))
        self.assertEqual(resolve_duty(roster, {'open_id': 'ou_b'}, ['D', 'E'], at(4, 8)), {**NIGHT, 'scope': 'E'})
        people[0]['group'] = 'A班'
        self.assertEqual(resolve_duty(roster, {'open_id': 'ou_a'}, ['D', 'E'], at(3, 9)), {**DAY, 'group': 'A班'})


class DailyInspectionReminderTests(unittest.TestCase):
    def test_changed_building_keeps_original_shift_and_retry_content(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        store = LanPortalStateStore(folder / 'state.sqlite3')
        person = {'record_id': 'recD', 'scope': 'D', 'shift': '白', 'open_id': 'ou_User'}
        cards = []
        def card_send(card, recipients, *, message_uuid):
            cards.append((card, message_uuid))
            if len(cards) == 1:
                return False, '', [{'open_id': recipients[0], 'ok': False, 'failure_kind': 'not_sent'}]
            raise TimeoutError('结果不确定')
        def tick(minute):
            now = dt.datetime(2026, 11, 3, 10, minute, tzinfo=CN)
            process_reminders(store, lambda day: {'date': day, 'complete': True, 'people': [person],
                                                  'observed_at': now.timestamp()},
                lambda **_: 'https://inspection.example', card_send, send_card=card_send,
                current=now)
        tick(0)
        person['scope'] = 'E'
        tick(5)
        self.assertEqual([item[0]['header']['title']['content'] for item in cards], ['D楼 · 日常巡检', 'D楼 · 日常巡检'])
        self.assertEqual(cards[0][1], cards[1][1], '补发应保留原班次内容和消息编号')
        person['scope'] = 'D'
        tick(10)
        self.assertEqual(len(cards), 3)
        self.assertEqual(cards[1], cards[2], '结果不确定时只能使用原内容和消息编号重试')

    def test_uncertain_legacy_text_retry_keeps_original_content_and_message_id(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        store = LanPortalStateStore(folder / 'state.sqlite3')
        old = '【日常巡检提醒】D楼 2026-11-03 白班 · 第 1 次\n旧内容\nhttps://inspection.example/daily-inspection'
        store.put_document('daily_inspection_reminders', '2026-11-03:白:1', {
            'targets': {'ou_DayD': old}, 'inflight_records': {'recD': 'ou_DayD'},
            'inflight_since': {'recD': dt.datetime(2026, 11, 3, 9, 59, tzinfo=CN).timestamp()}})
        sent = []
        def text_send(text, recipients, *, message_uuid):
            sent.append((text, recipients, message_uuid))
            return True, '', [{'open_id': recipients[0], 'ok': True}]
        def card_send(*_args, **_kwargs):
            self.fail('旧任务不能改变消息类型重发')
        process_reminders(store, lambda day: {'date': day, 'complete': True,
            'observed_at': dt.datetime(2026, 11, 3, 10, tzinfo=CN).timestamp(), 'people': [
            {'record_id': 'recD', 'scope': 'D', 'shift': '白', 'open_id': 'ou_DayD'}]},
            lambda **_: 'https://inspection.example', text_send, send_card=card_send,
            current=dt.datetime(2026, 11, 3, 10, tzinfo=CN))
        self.assertEqual(sent[0][0], old)
        self.assertEqual(len(sent), 1)
        self.assertEqual(store.get_document('daily_inspection_reminders', '2026-11-03:白:1')['sent'], ['ou_DayD'])

    def test_retries_with_corrected_identity_and_ignores_ambiguous_assignments(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        rows = [
            {'record_id': 'recD', 'scope': 'D', 'shift': '白', 'open_id': 'ou_OldD'},
            {'record_id': 'recE', 'scope': 'E', 'shift': '白', 'open_id': 'ou_DayE'},
            {'record_id': 'recOther', 'scope': 'C', 'shift': '夜', 'open_id': 'ou_DayE'},
        ]
        attempts = []

        def send(message, recipients, *, message_uuid):
            recipient, = recipients
            attempts.append(recipient)
            ok = recipient != 'ou_OldD'
            return ok, '', [{'open_id': recipient, 'ok': ok, 'failure_kind': '' if ok else 'not_sent',
                             'sender_app_id': 'cli_test', 'message_uuid': message_uuid,
                             'message_id': 'om_fixture', 'chat_id': 'oc_fixture'}]

        def resolve(record_id, _open_id, _app_id, _person):
            return next(row['open_id'] for row in rows if row['record_id'] == record_id)

        def tick(minute):
            now = dt.datetime(2026, 11, 3, 10, minute, tzinfo=CN)
            process_reminders(
                LanPortalStateStore(folder / 'state.sqlite3'),
                lambda day: {'date': day, 'app_id': 'cli_test', 'complete': True, 'people': rows,
                             'observed_at': now.timestamp()},
                lambda **_: 'https://inspection.example', send,
                current=now, resolve_recipient=resolve)

        tick(0)
        self.assertEqual(attempts, ['ou_OldD'])
        rows[0]['open_id'] = 'ou_NewD'
        tick(5)
        self.assertEqual(attempts, ['ou_OldD', 'ou_NewD'])
        rows[0]['open_id'] = 'ou_NewerD'
        rows.pop()
        tick(10)
        tick(15)
        self.assertEqual(attempts, ['ou_OldD', 'ou_NewD', 'ou_DayE'])

    def test_four_scheduled_times_and_midnight_duty_date(self):
        def at(day, hour, minute=0):
            return dt.datetime(2026, 11, day, hour, minute, tzinfo=CN)

        self.assertFalse(due_slots(at(3, 9, 29)))
        self.assertEqual(due_slots(at(3, 9, 30)), [('2026-11-03', '白', 1)])
        self.assertEqual(due_slots(at(3, 10, 40)), [('2026-11-03', '白', 1)])
        self.assertEqual(due_slots(at(3, 12)), [('2026-11-03', '白', 1), ('2026-11-03', '白', 2)])
        self.assertFalse(due_slots(at(3, 18)))
        self.assertEqual(due_slots(at(3, 20)), [('2026-11-03', '夜', 1)])
        self.assertEqual(due_slots(at(4, 0).astimezone(dt.timezone.utc)),
                         [('2026-11-03', '夜', 1), ('2026-11-03', '夜', 2)])
        self.assertFalse(due_slots(at(4, 9)))

    def test_unconfirmed_reminder_is_not_resent_to_changed_account_after_restart(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        person = {'record_id': 'recD', 'scope': 'D', 'shift': '白', 'open_id': 'ou_FirstD'}
        attempts = []

        def send(message, recipients, *, message_uuid):
            recipient, = recipients
            attempts.append((recipient, message_uuid))
            if len(attempts) == 1:
                raise TimeoutError
            return True, '', [{'open_id': recipient, 'ok': True, 'sender_app_id': 'cli_test',
                               'message_uuid': message_uuid, 'message_id': 'om_fixture', 'chat_id': 'oc_fixture'}]

        def tick(minute):
            now = dt.datetime(2026, 11, 3, 10, minute, tzinfo=CN)
            process_reminders(
                LanPortalStateStore(folder / 'state.sqlite3'),
                lambda day: {'date': day, 'app_id': 'cli_test', 'complete': True, 'people': [person],
                             'observed_at': now.timestamp()},
                lambda **_: 'https://inspection.example', send,
                current=now,
                resolve_recipient=lambda _rid, _old, _app, _person: person['open_id'])

        tick(0)
        person['open_id'] = 'ou_ChangedD'
        tick(5)
        self.assertEqual(len(attempts), 1)
        person['open_id'] = 'ou_FirstD'
        tick(10)
        tick(15)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(attempts[0], attempts[1])

    def test_midnight_routes_to_previous_night_staff_and_retries_only_unconfirmed(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        midnight = dt.datetime(2026, 11, 4, 0, tzinfo=CN)
        roster_dates, sent, deliveries, clock = [], [], set(), [midnight]

        def roster(day):
            roster_dates.append(day)
            return {'date': day, 'complete': True, 'observed_at': clock[0].timestamp(), 'people': [
                {'record_id': 'recNightD', 'scope': 'D', 'shift': '夜', 'open_id': 'ou_NightD'},
                {'record_id': 'recNightE', 'scope': 'E', 'shift': '夜', 'open_id': 'ou_NightE'},
                {'record_id': 'recDayD', 'scope': 'D', 'shift': '白', 'open_id': 'ou_DayD'},
                {'record_id': 'recNightC', 'scope': 'C', 'shift': '夜', 'open_id': 'ou_NightC'},
            ]}

        def send(message, recipients, *, message_uuid):
            recipient, = recipients
            key = (recipient, message_uuid)
            content = message['elements'][0]['text']['content']
            ok = recipient != 'ou_NightE' or '第 1 次' in content or key in deliveries
            deliveries.add(key)
            sent.append((recipient, message_uuid, message))
            return ok, '', [{'open_id': recipient, 'ok': ok}]

        def tick(minute):
            reopened = LanPortalStateStore(folder / 'state.sqlite3')
            clock[0] = midnight + dt.timedelta(minutes=minute)
            process_reminders(reopened, roster, lambda **_: 'https://inspection.example', send,
                              current=clock[0])

        for minute in (-240, 0, 1, 5, 10):
            tick(minute)
        self.assertEqual(roster_dates, ['2026-11-03'] * 2)
        self.assertEqual([item[0] for item in sent], ['ou_NightD', 'ou_NightE', 'ou_NightD', 'ou_NightE', 'ou_NightE'])
        self.assertEqual(sent[3], sent[4])
        self.assertEqual(len(deliveries), 4)
        self.assertIn('2026-11-03', sent[0][2]['elements'][0]['text']['content'])
        self.assertIn('夜班', sent[0][2]['elements'][0]['text']['content'])
        self.assertIn('第 1 次', sent[0][2]['elements'][0]['text']['content'])
        self.assertIn('第 2 次', sent[2][2]['elements'][0]['text']['content'])
        self.assertEqual(sent[2][2]['elements'][2]['actions'][0]['url'], 'https://inspection.example/daily-inspection')

    def test_restart_catches_up_both_day_slots_and_retries_late_identity(self):
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        sent, roster_dates, clock = [], [], [dt.datetime(2026, 11, 3, 14, 10, tzinfo=CN)]
        rows = [
            {'record_id': 'recD', 'scope': 'D', 'shift': '白', 'open_id': 'ou_DayD'},
            {'record_id': 'recE', 'scope': 'E', 'shift': '白', 'open_id': ''},
            {'scope': 'D', 'shift': '夜', 'open_id': 'ou_NightD'},
            {'scope': 'E', 'shift': '白', 'duty_kind': '长白', 'open_id': 'ou_OfficeE'},
        ]

        def roster(day):
            roster_dates.append(day)
            return {'date': day, 'app_id': 'cli_test', 'complete': True, 'people': rows,
                    'observed_at': clock[0].timestamp()}

        def send(message, recipients, *, message_uuid):
            recipient, = recipients
            sent.append((recipient, message_uuid, message))
            return True, '', [{'open_id': recipient, 'ok': True, 'sender_app_id': 'cli_test',
                               'message_uuid': message_uuid, 'message_id': 'om_fixture', 'chat_id': 'oc_fixture'}]

        def tick(hour, minute):
            clock[0] = dt.datetime(2026, 11, 3, hour, minute, tzinfo=CN)
            process_reminders(LanPortalStateStore(folder / 'state.sqlite3'), roster,
                              lambda **_: 'https://inspection.example', send,
                              current=clock[0],
                              resolve_recipient=lambda record_id, _old, _app, _person:
                                  next(row['open_id'] for row in rows if row.get('record_id') == record_id))

        tick(14, 10)
        rows[1]['open_id'] = 'ou_DayE'
        tick(14, 15)
        tick(15, 0)
        self.assertEqual(roster_dates, ['2026-11-03'])
        self.assertEqual([item[0] for item in sent], ['ou_DayD', 'ou_DayD', 'ou_DayE', 'ou_DayE'])
        self.assertEqual(len({item[1] for item in sent}), 4)
        self.assertEqual(sent[0][2]['header']['title']['content'], 'D楼 · 日常巡检')
        self.assertEqual(sent[3][2]['header']['title']['content'], 'E楼 · 日常巡检')
        self.assertIn('白班', sent[0][2]['elements'][0]['text']['content'])
        self.assertIn('如发现不符可直接修改', sent[0][2]['elements'][1]['text']['content'])
        self.assertEqual(sent[0][2]['elements'][2]['actions'][0]['text']['content'], '查看并核对巡检')
        self.assertIn('第 1 次', sent[0][2]['elements'][0]['text']['content'])
        self.assertIn('第 2 次', sent[1][2]['elements'][0]['text']['content'])

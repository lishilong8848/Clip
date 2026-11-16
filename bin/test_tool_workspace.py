import base64
import copy
import datetime as dt
import io
import tempfile
import unittest
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock, patch
from zipfile import ZipFile

from PIL import Image, ImageDraw
from openpyxl import load_workbook

from bin.test_tool_self_check import actor
from bin.lan_bitable_template_portal.risk_inspection import RiskError, RiskInspections
from bin.lan_bitable_template_portal.tool_management import ToolManagement
from bin.lan_bitable_template_portal.tool_self_check import TEMPLATES, TZ


class WorkspaceCloud:
    def __init__(self):
        self.content = next(TEMPLATES.glob('*.xlsx')).read_bytes()
        self.assets = [dict(id=f'recTool{i}', name=f'工具{i}', scope='A', model='测试型号', brand='测试品牌',
                            location='工具间', quantity=1, retired=False, template_id='',
                            self_cycle='每月' if i < 2 else '', external_cycle='一年' if i >= 2 else '')
                       for i in range(4)]
        self.bindings, self.writes, self.tokens = [], {}, []
        self.failures = set()

    def inventory(self):
        return copy.deepcopy(self.assets), {'A': ['ou_leader'], 'B': ['ou_other']}

    def templates(self):
        return [dict(id='recTemplate', name='记录单', file_token='templateToken', digest='fixture')]

    def download_template(self, template):
        return self.content

    def bind_template(self, asset_id, template_id, expected):
        row = next(row for row in self.assets if row['id'] == asset_id)
        if row['template_id'] == template_id:
            return template_id
        if row['template_id'] != expected:
            raise RiskError('关联冲突', 409)
        row['template_id'] = template_id
        self.bindings.append(asset_id)
        return template_id

    def write_workspace_item(self, session_id, item, name, report, actual, **kwargs):
        key = (session_id, item['asset_id'])
        token = kwargs['file_token']
        if not token:
            token = f'upload{len(self.tokens)}'
            self.tokens.append((token, name, report))
            kwargs['save_file'](token)
        if not kwargs['attempted_at']:
            kwargs['save_attempt'](1_790_000_000)
        if item['asset_id'] in self.failures:
            raise RiskError('临时网络失败', 503)
        self.writes.setdefault(key, (copy.deepcopy(item), name, report, actual, token))
        return {'remote_id': 'recArchive' + item['asset_id'], 'file_token': token}

    def write(self, dest, order, asset, job, person_for, save_file):
        key = (order['id'], dest)
        name, report = order['report_name'], order['report']
        token = job['file_token']
        if dest != 'change' and not token:
            token = f'upload{len(self.tokens)}'
            self.tokens.append((token, name, report))
            save_file(token)
        if not job['attempted_at']:
            job['save_attempt'](dt.datetime.now().timestamp())
        if asset['id'] in self.failures:
            raise RiskError('临时网络失败', 503)
        self.writes.setdefault(key, (dict(order), name, report, order['actual'], token))
        return 'recArchive' + asset['id']


class WorkspaceTests(unittest.TestCase):
    def setUp(self):
        folder = self.enterContext(tempfile.TemporaryDirectory())
        self.path = Path(folder) / 'tools.db'
        self.cloud = WorkspaceCloud()
        self.admin = actor(identity='admin', admin=True)
        self.filler = self.admin | {'_tool_management_full_access': True}
        self.leader = actor() | {'name': '本楼楼长'}
        self.service = self.restart()
        self.service.refresh()

    def restart(self):
        manager = self.admin | {'position': '园区经理', 'category': 'manager', 'level': 'L2'}
        return ToolManagement(RiskInspections(self.path, lambda _: manager), self.cloud)

    def open(self, kind='self', operation='open-one'):
        ids = ['recTool0', 'recTool1'] if kind == 'self' else ['recTool2', 'recTool3']
        return self.service.workspace_open(self.filler, dict(
            kind=kind, asset_ids=ids, template_id='recTemplate' if kind == 'self' else '', operation_id=operation))

    @staticmethod
    def signature():
        picture = Image.new('RGBA', (180, 60), 'white')
        ImageDraw.Draw(picture).line((10, 40, 50, 10, 140, 45), fill='black', width=3)
        image = io.BytesIO()
        picture.save(image, format='PNG')
        return 'data:image/png;base64,' + base64.b64encode(image.getvalue()).decode()

    def complete(self, session, outcomes=('normal', 'normal')):
        for item, outcome in zip(session['items'], outcomes):
            item.update(outcome=outcome, reason='检修测试' if outcome != 'normal' else '', quantity=1)
            schema = item['template']['schema']
            address = 'G11' if schema.get('layout') == 'hvac' else 'G8'
            item['values'][schema['sheet'] + '!' + address] = (dt.datetime.now(TZ).date() - dt.timedelta(days=1)).isoformat()
            item['checks'] = {sheet['name'] + '!' + cell['address']: '是'
                              for sheet in item['template']['sheets'] for row in sheet['rows']
                              for cell in row['cells'] if cell.get('check')}
        return self.service.workspace_save(self.filler, session['id'], {
            'revision': session['revision'], 'items': session['items']})

    def approve(self, session, reviewer=None):
        reviewer = reviewer or self.leader
        return self.service.workspace_review(reviewer, session['id'], {
            'signature': self.signature(), 'action': 'approve',
            'items': [{'asset_id': item['id'], 'revision': item['order_revision']} for item in session['items']]})

    def test_template_relation_and_open_retry_are_scoped_and_persistent(self):
        first = self.open()
        self.assertEqual(self.open()['id'], first['id'])
        self.assertEqual(self.cloud.bindings, ['recTool0', 'recTool1'])
        self.service = self.restart()
        board = self.service.workspace_list(self.admin)
        self.assertEqual([r['template_id'] for r in board['assets'][:2]], ['recTemplate'] * 2)
        second = self.service.workspace_open(self.admin, dict(kind='self', asset_ids=['recTool0'],
                                                              template_id='', operation_id='next-one'))
        self.assertEqual(second['items'][0]['template_id'], 'recTemplate')
        with self.assertRaises(RiskError):
            self.service.workspace_open(self.admin, dict(kind='external', asset_ids=['recTool0'], operation_id='bad'))
        with self.assertRaises(RiskError):
            self.service.workspace_get(actor(scope='B', identity='other'), first['id'])

    def test_grouped_templates_prefill_and_independent_completion_with_assigned_signature(self):
        from bin.lan_bitable_template_portal.tool_self_check_routes import compact_workspace

        ids = ['recTool0', 'recTool1']
        second = {'id': 'recTemplateTwo', 'name': '第二个模板', 'file_token': 'second', 'digest': 'second'}
        self.cloud.templates = lambda: [
            {'id': 'recTemplate', 'name': '记录单', 'file_token': 'templateToken', 'digest': 'fixture'}, second]
        self.service.person_for = lambda identity: self.leader if identity == self.leader['id'] else self.admin
        signer = Mock(return_value=base64.b64decode(self.signature().split(',', 1)[1]))
        self.service.workspace_signature = signer
        payload = dict(kind='self', asset_ids=ids, operation_id='mixed-mapping',
                       template_ids=dict(zip(ids, ('recTemplate', 'recTemplateTwo'))))
        session = self.service.workspace_open(self.filler, payload)
        self.assertEqual([row['template_id'] for row in self.cloud.assets[:2]], ['recTemplate', 'recTemplateTwo'])
        self.assertEqual([row['template_id'] for row in session['items']], ['recTemplate', 'recTemplateTwo'])
        self.assertEqual(self.service.workspace_open(self.filler, payload)['id'], session['id'])
        with self.assertRaisesRegex(RiskError, '同一操作编号'):
            self.service.workspace_open(self.filler, payload | {'template_ids': dict.fromkeys(ids, 'recTemplate')})
        compact = compact_workspace(copy.deepcopy(session))
        self.assertEqual(len(compact['layouts']), 1)
        self.assertTrue(all('template' not in item and item['layout_key'] in compact['layouts'] for item in compact['items']))
        for row in session['items']:
            form = row['template']['schema']['sheet'] + '!'
            addresses = row['template']['prefill_fields']
            self.assertEqual(row['values'][form + addresses['inspector']], self.filler['name'])
            self.assertEqual(row['values'][form + addresses['checked_at']], dt.datetime.now(TZ).date().isoformat())
            row['reviewer_id'] = self.leader['id']
            row['completion'] = 'incomplete'
        prepared = self.complete(session)
        pending = self.service.workspace_submit(self.filler, session['id'], {'revision': prepared['revision']})
        self.assertFalse(any(row['can_review'] for row in pending['items']))
        with self.assertRaisesRegex(RiskError, '指定的审核人'):
            self.approve(pending, self.filler)
        reviewed_at = (dt.datetime.now(TZ) - dt.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M')
        override_at = (dt.datetime.now(TZ) - dt.timedelta(days=2)).strftime('%Y-%m-%dT%H:%M')
        selected = [{'asset_id': item['id'], 'revision': item['order_revision']} for item in pending['items']]
        selected[1]['reviewed_at'] = override_at
        approved = self.service.workspace_review(self.leader, session['id'], {
            'action': 'approve', 'use_saved_signature': True, 'reviewed_at': reviewed_at,
            'items': selected,
        })
        signer.assert_called_once_with(self.leader['id'])
        dates = dict(zip(ids, (reviewed_at, override_at)))
        self.assertEqual({row['id']: row['reviewed_at'] for row in approved['items']}, dates)
        while self.service.sync_one():
            pass
        reopened = self.service.workspace_get(self.leader, session['id'])
        self.assertEqual({row['id']: row['reviewed_at'] for row in reopened['items']}, dates)
        with self.service.connect() as db:
            row, items = self.service._workspace_session(db, self.leader, session['id'], include_files=False)
            self.assertTrue({'report', 'package'}.isdisjoint(row.keys()))
            self.assertTrue(all('report' not in item.keys() for item in items))
        schema = session['items'][0]['template']['schema']
        for order, _, content, _, _ in self.cloud.writes.values():
            book = load_workbook(io.BytesIO(content))
            self.assertIn('未完成[√]', book[schema['sheet']][f'E{schema["result_row"]}'].value)
            self.assertEqual(book[schema['sheet']][f'G{schema["review_row"]}'].value,
                             dates[order['asset_id']].replace('T', ' '))
            book.close()

    def test_individual_signed_files_and_failed_item_do_not_block_others(self):
        session = self.complete(self.open())
        body = {'revision': session['revision']}
        submitted = self.service.workspace_submit(self.filler, session['id'], body)
        self.assertEqual(submitted['status'], 'review_pending')
        self.assertEqual(self.service.workspace_submit(self.filler, session['id'], body)['status'], 'review_pending')
        self.assertFalse(self.service.sync_one())
        with self.assertRaisesRegex(RiskError, '仅本楼楼长'):
            self.approve(submitted, self.admin)
        with self.assertRaisesRegex(RiskError, '仅本楼楼长'):
            self.approve(submitted, self.leader | {'_tool_impersonation': True})
        approved = self.approve(submitted)
        self.assertTrue(all(entry['reviewer'] == self.leader['name'] for entry in approved['items']))
        status = self.service.workspace_status(self.leader, session['id'])
        self.assertEqual(status['status'], 'pending')
        self.assertNotIn('template', status['items'][0])
        self.assertNotIn('signature', status['items'][0])
        self.cloud.failures.add('recTool0')
        self.service.sync_one()
        self.service.sync_one()
        self.assertEqual(len(self.cloud.writes), 1)
        self.service = self.restart()
        self.cloud.failures.clear()
        with self.service.connect(True) as db:
            db.execute('UPDATE tool_work_sync SET next_try=0')
        self.service.sync_one()
        self.assertEqual(self.service.workspace_get(self.admin, session['id'])['status'], 'archived')
        self.assertFalse(self.service.sync_one())
        self.assertEqual(len(self.cloud.writes), 2)
        self.assertEqual(len(self.cloud.tokens), 2)
        original = load_workbook(io.BytesIO(self.cloud.content))
        image_count = sum(len(sheet._images) for sheet in original)
        original.close()
        for _, name, data, _, _ in self.cloud.writes.values():
            self.assertTrue(name.endswith('.xlsx'))
            book = load_workbook(io.BytesIO(data))
            self.assertEqual(sum(len(sheet._images) for sheet in book), image_count + 1)
            book.close()
        with ZipFile(io.BytesIO(self.service.workspace_download(self.admin, session['id']))) as package:
            self.assertEqual(sum(name.endswith('.xlsx') for name in package.namelist()), 2)

    def test_external_one_upload_attaches_same_report_to_every_tool(self):
        session = self.open('external')
        report = b'%PDF-1.7\nvalidation report'
        session = self.service.workspace_report(self.filler, session['id'], session['revision'], '检验报告.pdf', report)
        actual = dt.datetime.now(TZ).date().isoformat()
        self.service.workspace_submit(self.filler, session['id'], {'revision': session['revision'], 'actual': actual})
        self.service.sync_one()
        self.service.sync_one()
        self.assertEqual(len(self.cloud.tokens), 1)
        self.assertEqual(len(self.cloud.writes), 2)
        self.assertTrue(all(data == report for _, _, data, _, _ in self.cloud.writes.values()))
        self.assertTrue(all(date == actual for _, _, _, date, _ in self.cloud.writes.values()))

    def test_report_scope_per_tool_conclusions_and_one_stored_attachment(self):
        self.cloud.assets[3]['scope'] = 'B'
        self.service.refresh()
        with self.assertRaisesRegex(RiskError, '同一楼栋'):
            self.open('external')
        self.cloud.assets[3]['scope'] = 'A'
        self.service.refresh()
        session = self.open('external')
        report = b'%PDF-1.7\nfixture'
        session = self.service.workspace_report(self.filler, session['id'], session['revision'], '报告.pdf', report)
        actual = dt.datetime.now(TZ).date().isoformat()
        ids = [item['id'] for item in session['items']]
        body = {'revision': session['revision'], 'actual': actual, 'inspection': {
            'report_no':'test', 'institution':'fixture', 'result':'qualified', 'expires_on': actual,
            'confirmed_asset_ids':ids, 'tools':{ids[1]:{'result':'unqualified', 'expires_on':''}},
        }}
        submitted = self.service.workspace_submit(self.filler, session['id'], body, require_report_check=True)
        self.assertEqual(submitted['inspection']['tools'][ids[1]], {'result':'unqualified', 'expires_on':''})
        self.service.workspace_submit(self.filler, session['id'], body, require_report_check=True)
        orders = [item['order_id'] for item in submitted['items']]
        self.assertEqual(self.service.report_attachment(self.leader, orders[0])[1], report)
        with ZipFile(io.BytesIO(self.service.workspace_download(self.leader, submitted['id']))) as archive:
            self.assertEqual(sum(name.endswith('.pdf') for name in archive.namelist()), 1)
        with self.service.connect(True) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM tool_work_orders WHERE report IS NOT NULL').fetchone()[0], 0)
            # Simulate a pre-upgrade mixed-floor batch with copied attachments.
            db.execute('UPDATE tool_work_orders SET scope=?,report=? WHERE id=?', ('B', report, orders[1]))
            db.execute('UPDATE tool_work_orders SET report=? WHERE id=?', (report, orders[0]))
        self.assertFalse(self.service.workspace_get(self.leader, submitted['id'])['report_available'])
        for reader, args in [(self.service.workspace_report_attachment, (submitted['id'],)),
                             (self.service.workspace_download, (submitted['id'],)),
                             (self.service.report_attachment, (orders[0],)),
                             (self.service.download, ('', orders[0]))]:
            with self.assertRaises(RiskError) as error:
                reader(self.leader, *args)
            self.assertEqual(error.exception.status, 403)

    def test_cancelled_monthly_orders_do_not_block_workspace_and_review_routes_per_tool(self):
        self.service.configure(self.admin, 'park-manager', {'manager_id': self.admin['id']})
        self.service.schedule()
        before = {order['asset_id']: order for order in self.service.list(self.leader)['orders']}
        for order in before.values():
            self.service.cancel_order(self.leader, order['id'], {'revision': order['revision']})
        self.assertTrue(all(asset['selectable'] for asset in self.service.workspace_list(self.filler)['assets']))
        draft = self.complete(self.open(), ('repair', 'scrap'))
        current = {item['id']: item['order_id'] for item in draft['items']}
        for item in draft['items']:
            self.assertNotEqual(item['order_id'], before[item['id']]['id'])
            self.assertEqual(self.service.detail(self.leader, before[item['id']]['id'])['status'], 'cancelled')
            info = self.service.detail(self.leader, item['order_id'])
            self.assertEqual(info['workspace_id'], draft['id'])
            with self.assertRaisesRegex(RiskError, '原表办理'):
                self.service.action(self.leader, item['order_id'], 'submit', {'revision': info['revision']})
        pending = self.service.workspace_submit(self.filler, draft['id'], {'revision': draft['revision']})
        approved = self.approve(pending)
        self.approve(pending)
        while self.service.sync_one():
            pass
        self.assertEqual(set(self.cloud.writes), {
            (current['recTool0'], 'repair'), (current['recTool0'], 'change'),
            (current['recTool1'], 'scrap'), (current['recTool1'], 'change'),
        })
        self.assertEqual({item['reviewer'] for item in approved['items']}, {self.leader['name']})

    def test_workspace_can_open_before_planned_date_and_reuses_active_batch(self):
        asset = self.cloud.assets[0]
        asset['self_first'] = '2099-01-01'
        self.service.refresh()
        with self.service.connect(True) as db:
            db.execute("UPDATE tool_inventory SET config=? WHERE id=?",
                       ('{"self_cycle":"停用","external_cycle":"一年"}', asset['id']))
        shown = next(row for row in self.service.workspace_list(self.filler)['assets'] if row['id'] == asset['id'])
        self.assertEqual((shown['self_cycle'], shown['external_cycle'], shown['eligible_kinds']), ('每月', '', ['self']))
        body = dict(kind='self', asset_ids=[asset['id']], template_id='recTemplate', operation_id='temporary')
        session = self.service.workspace_open(self.filler, body)
        self.service = self.restart()
        reopened = self.service.workspace_open(self.filler, body | {'operation_id': 'continue'})
        self.assertEqual(reopened['id'], session['id'])
        orders = [row for row in self.service.list(self.filler)['orders'] if row['asset_id'] == asset['id']]
        self.assertEqual([row['id'] for row in orders], [session['items'][0]['order_id']])

    def test_module_holder_can_fill_and_select_completion_without_review_permission(self):
        worker = actor(identity='operator')
        self.assertTrue(self.service.available_to(worker))
        self.assertFalse(self.service.available_to(worker | {'modules': []}))
        session = self.service.workspace_open(worker, {'kind': 'self', 'asset_ids': ['recTool0'],
                                                        'template_id': 'recTemplate', 'operation_id': 'worker-fill'})
        entry, = session['items']
        self.assertTrue(entry['can_edit'])
        self.assertFalse(entry['can_review'])
        saved = self.service.workspace_save(worker, session['id'], {'revision': session['revision'],
            'items': [{'asset_id': entry['id'], 'completion': 'incomplete'}]})
        self.assertEqual(saved['items'][0]['completion'], 'incomplete')
        with self.assertRaises(RiskError):
            self.service.workspace_get(actor(scope='B', identity='other-worker'), session['id'])
        self.assertFalse(self.service.workspace_list(actor(identity='different-worker'))['sessions'])

    def test_upgrade_keeps_old_batches_and_can_open_same_month_after_archive(self):
        from bin.lan_bitable_template_portal.tool_management import WORK_ORDER_SCHEMA

        active = self.open()
        old_schema = WORK_ORDER_SCHEMA.replace('workspace_direct INTEGER NOT NULL DEFAULT 0',
                                               'UNIQUE(month,asset_id,kind)')
        with self.service.connect(True) as db:
            db.execute('ALTER TABLE tool_work_orders RENAME TO upgrade_fixture')
            db.execute(old_schema)
            columns = ','.join(row['name'] for row in db.execute('PRAGMA table_info(tool_work_orders)'))
            db.execute(f'INSERT INTO tool_work_orders({columns}) SELECT {columns} FROM upgrade_fixture')
            db.execute('DROP TABLE upgrade_fixture')
        self.service = self.restart()
        self.assertEqual(self.service.workspace_get(self.filler, active['id'])['items'], active['items'])
        ready = self.complete(active)
        pending = self.service.workspace_submit(self.filler, ready['id'], {'revision': ready['revision']})
        self.approve(pending)
        while self.service.sync_one():
            pass
        self.assertEqual(self.service.workspace_get(self.filler, active['id'])['status'], 'archived')
        new_batch = self.open(operation='another-inspection')
        self.assertNotEqual(new_batch['id'], active['id'])
        self.assertEqual(len(self.cloud.writes), 2)

    def test_upgrade_reuses_verified_legacy_receipt_without_republishing(self):
        draft = self.complete(self.open())
        item = draft['items'][0]
        now = dt.datetime.now(TZ).isoformat(timespec='seconds')
        order_id = item['order_id']
        session_id = draft['id']
        report = self.cloud.content
        with self.service.connect(True) as db:
            db.execute('DELETE FROM tool_workspace_items WHERE session_id=? AND asset_id<>?', (session_id, item['id']))
            db.execute("UPDATE tool_workspace_sessions SET status='archived',submitted_at=? WHERE id=?", (now, session_id))
            db.execute("""UPDATE tool_workspace_items SET order_id='',remote_id='recArchive1',file_token='legacyToken',
                report_name='旧自检单.xlsx',report=?,draft=? WHERE session_id=?""", (
                report, '{"qualified":true,"reviewed_at":"' + now[:16] + '"}', session_id))
        self.service = self.restart()
        body = {'order_id': order_id, 'asset_id': item['id']}
        with self.assertRaises(RiskError):
            self.service.workspace_reconcile(self.leader, session_id, body)
        with patch.object(self.cloud, 'write_workspace_item', return_value={
                'remote_id': 'recArchive1', 'file_token': 'legacyToken'}) as verify:
            self.service.workspace_reconcile(self.admin, session_id, body)
            self.service.workspace_reconcile(self.admin, session_id, body)
        verify.assert_called_once()
        self.assertFalse(self.service.sync_one())
        self.assertEqual(self.service.detail(self.leader, order_id)['status'], 'submitted')
        self.assertEqual(self.cloud.writes, {})

    def test_interrupted_mapping_is_visible_and_can_resume_after_restart(self):
        real_bind = self.cloud.bind_template
        def fail_second(asset, template, old):
            if asset == 'recTool1':
                raise RiskError('临时断网', 503)
            return real_bind(asset, template, old)
        with patch.object(self.cloud, 'bind_template', side_effect=fail_second):
            opened = self.open()
            self.assertEqual(opened['items'][1]['binding_error'], '临时断网')
        self.service = self.restart()
        pending = self.service.workspace_list(self.admin)['sessions'][0]
        self.assertEqual(pending['status'], 'opening')
        restored = self.service.workspace_resume(self.filler, pending['id'])
        self.assertEqual(restored['status'], 'draft')
        self.assertEqual(self.cloud.bindings, ['recTool0', 'recTool1'])
        self.assertEqual(self.service.workspace_resume(self.admin, pending['id'])['id'], pending['id'])

    def test_stale_draft_incomplete_checks_and_moved_scope_are_rejected(self):
        original = self.open()
        complete = self.complete(original)
        with self.assertRaises(RiskError):
            self.service.workspace_save(self.admin, original['id'], {'revision': 0, 'items': []})
        item = complete['items'][0]
        item['checks'].pop(next(iter(item['checks'])))
        incomplete = self.service.workspace_save(self.filler, original['id'], {
            'revision': complete['revision'], 'items': [item]})
        with self.assertRaisesRegex(RiskError, '自检项目'):
            self.service.workspace_submit(self.filler, original['id'], {'revision': incomplete['revision']})
        complete = self.complete(incomplete)
        self.cloud.assets[0]['scope'] = 'B'
        self.service.refresh()
        with self.assertRaisesRegex(RiskError, '归属楼栋'):
            self.service.workspace_submit(self.filler, original['id'], {'revision': complete['revision']})
        receiver = actor(scope='B', identity='other')
        moved, = self.service.workspace_list(receiver)['assets']
        self.assertEqual(moved['plan']['mode'], 'transfer')
        transfer = {'asset_id': moved['id'], 'scope': 'B', 'revision': moved['plan']['revision']}
        with self.assertRaises(RiskError):
            self.service.workspace_transfer(self.leader, transfer)
        self.service.workspace_transfer(receiver, transfer)
        old = self.service.workspace_get(self.leader, original['id'])
        self.assertEqual(old['items'][0]['order_status'], 'cancelled')
        self.assertEqual(old['items'][0]['values'], complete['items'][0]['values'])
        self.assertEqual(self.service.workspace_submit(self.filler, old['id'], {'revision': old['revision']})['status'], 'review_pending')
        new = self.service.workspace_open(receiver, {'kind':'self', 'asset_ids':[moved['id']], 'operation_id':'moved'})
        self.assertEqual(new['items'][0]['asset']['scope'], 'B')

    def test_pending_bindings_allow_verified_tool_to_save_and_resume_after_restart(self):
        self.cloud.assets[0]['template_id'] = 'recTemplate'
        self.service.refresh()
        body = {'kind':'self', 'asset_ids':['recTool0', 'recTool1'], 'template_id':'recTemplate',
                'operation_id':'queued', 'defer_bindings': True}
        opened = self.service.workspace_open(self.filler, body)
        first, second = opened['items']
        self.assertTrue(first['can_edit'])
        self.assertFalse(second['can_edit'])
        self.assertTrue(second['binding_pending'])
        self.assertEqual(self.cloud.bindings, [])
        saved = self.service.workspace_save(self.filler, opened['id'], {'revision':opened['revision'],
            'items':[{'asset_id':first['id'], 'completion':'incomplete'}]})
        self.service = self.restart()
        self.assertTrue(self.service.workspace_binding_tick())
        draft = self.service.workspace_get(self.filler, saved['id'])
        self.assertEqual(draft['status'], 'draft')
        self.assertEqual(draft['revision'], saved['revision'])
        self.assertEqual(draft['items'][0]['completion'], 'incomplete')
        self.assertTrue(draft['items'][1]['can_edit'])
        self.assertEqual(self.cloud.bindings, ['recTool1'])

    def test_reassign_pending_and_preserve_original_draft(self):
        session = self.complete(self.open())
        row = session['items'][0]
        session = self.service.workspace_save(self.filler, session['id'], {'revision': session['revision'],
            'items': [{'asset_id': row['id'], 'reviewer_id': self.filler['id']}]})
        selected = self.service.workspace_submit(self.filler, session['id'], {'revision': session['revision']})
        received = self.service.workspace_get(self.leader, session['id'])
        self.assertFalse(received['items'][0]['can_review'])
        self.assertTrue(received['items'][0]['can_reassign'])
        body = {'asset_id': row['id'], 'revision': selected['items'][0]['order_revision'], 'action': 'reassign', 'reviewer_id': ''}
        with self.assertRaises(RiskError):
            self.service.workspace_manage_item(actor(scope='B', identity='other'), session['id'], body)
        changed = self.service.workspace_manage_item(self.leader, session['id'], body)
        self.assertTrue(changed['items'][0]['can_review'])
        self.assertEqual(changed['items'][0]['values'], row['values'])
        with self.assertRaises(RiskError):
            self.approve(selected, self.filler)
        self.approve(changed, self.leader)

    def test_routes_enforce_actor_and_preserve_report_bytes(self):
        from fastapi import FastAPI
        from fastapi.responses import JSONResponse
        from fastapi.testclient import TestClient
        from bin.lan_bitable_template_portal.tool_self_check_routes import install_tool_self_check_routes
        class Controller:
            async def _read_json_request(self, request, **kwargs):
                return await request.json()
        def read_actor(request):
            if request.headers.get('actor') != 'admin':
                raise RiskError('请登录', 401)
            return self.filler
        def same_origin(request):
            if request.headers.get('origin') not in (None, 'http://testserver'):
                raise RiskError('来源不允许', 403)
        def response(data=None, error=None):
            return JSONResponse({'ok': error is None, 'data': data, 'error': str(error) if error else ''},
                                status_code=error.status if error else 200)
        app = FastAPI()
        install_tool_self_check_routes(app, Controller(), lambda: self.service, read_actor, same_origin, response)
        with TestClient(app) as client:
            base = '/api/tool-self-check/workspace'
            self.assertEqual(client.get(base).status_code, 401)
            headers = {'actor': 'admin'}
            self.assertEqual(client.get(base, headers=headers).status_code, 200)
            self.assertEqual(client.post('/api/tool-self-check/orders/batch-create', headers=headers, json={}).status_code, 410)
            self.assertEqual(client.post('/api/tool-self-check/orders/clear-month', headers=headers, json={}).status_code, 410)
            self.assertEqual(client.post(base + '/refresh', headers={**headers, 'origin': 'https://wrong.invalid'}).status_code, 403)
            opened = client.post(base + '/open', headers=headers, json={
                'kind': 'external', 'asset_ids': ['recTool2'], 'operation_id': 'api-report',
            }).json()['data']
            saved = client.post(base + '/' + opened['id'] + f"/report?name=report.pdf&revision={opened['revision']}",
                                headers=headers, content=b'%PDF-1.7 report').json()
            self.assertTrue(saved['ok'], saved)
            self.assertEqual(saved['data']['report_name'], 'report.pdf')
            route = base + '/' + opened['id']
            preview = client.get(route + '/report-file', headers=headers)
            self.assertEqual(preview.content, b'%PDF-1.7 report')
            self.assertTrue(preview.headers['content-disposition'].startswith('inline'))
            self.assertEqual(client.get(route + '/report-file').status_code, 401)
            body = {'revision': saved['data']['revision'], 'actual': '2025-10-31'}
            self.assertEqual(client.post(route + '/submit', headers=headers, json=body).status_code, 400)
            body['inspection'] = {'report_no': 'REC-12', 'institution': '测试计量院', 'result': 'qualified',
                                  'expires_on': '2026-11-30', 'confirmed_asset_ids': []}
            self.assertEqual(client.post(route + '/submit', headers=headers, json=body).status_code, 400)
            body['inspection']['confirmed_asset_ids'] = ['recTool2']
            submitted = client.post(route + '/submit', headers=headers, json=body)
            self.assertEqual(submitted.status_code, 200, submitted.text)
            self.assertEqual(submitted.json()['data']['inspection_expires_on'], '2026-11-30')
            self.assertEqual(client.post(route + '/submit', headers=headers, json=body).status_code, 200)
            body['inspection']['report_no'] = 'OTHER'
            self.assertEqual(client.post(route + '/submit', headers=headers, json=body).status_code, 409)

    def test_background_loop_runs_workspace_when_monthly_queue_is_empty(self):
        from bin.lan_bitable_template_portal.tool_management_cloud import run_tool_jobs
        stop = Event()
        calls = []
        def archive():
            calls.append('workspace')
            stop.set()
            return True
        service = SimpleNamespace(refresh=lambda: None, sync_one=lambda: False, workspace_sync_one=archive)
        run_tool_jobs(service, stop)
        self.assertEqual(calls, ['workspace'])


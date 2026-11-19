import asyncio
import base64
import copy
import datetime as dt
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
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
        self.removed_records = set()
        self.delete_failure = False

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

    def delete_ledger_record(self, dest, identity, key):
        if self.delete_failure:
            raise RiskError('模拟飞书删除失败', 503)
        self.removed_records.add(identity)

    def ledger_record_ids(self, dest):
        return {'recArchive' + item[0]['asset_id'] for key, item in self.writes.items()
                if key[1] == dest} - self.removed_records

    def ledger_exists(self, dest, identity, key):
        return identity not in self.removed_records


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

    def test_followup_removal_hides_the_same_workbench_item_only(self):
        saved = self.complete(self.open(), ('repair', 'normal'))
        submitted = self.service.workspace_submit(self.filler, saved['id'], {'revision': saved['revision']})
        self.approve(submitted)
        while self.service.sync_one():
            pass
        order = self.service.list(self.leader)['followups'][0]
        self.assertEqual(order['workspace_id'], saved['id'])
        self.assertTrue(order['can_remove_followup'])
        repairing = next(item for item in self.service.workspace_history(self.leader)['sessions'][0]['history_items']
                         if item['order_id'] == order['id'])
        self.assertFalse(repairing['can_delete'])
        result = self.service.action(self.leader, order['id'], 'repaired', {'revision': order['revision'], 'reason': '试验通过'})
        while self.service.sync_one():
            pass
        self.service.action(self.leader, order['id'], 'remove-followup', {'revision': result['revision']})
        self.service = self.restart()
        self.assertTrue(self.service.tool_ledger_delete_one())
        self.assertFalse(self.service.list(self.leader)['followups'])
        history = self.service.workspace_history(self.leader)['sessions'][0]['history_items']
        self.assertEqual([item['id'] for item in history], ['recTool1'])
        self.assertEqual(self.service.detail(self.leader, order['id'])['outcome'], 'repaired')

    def test_remote_delete_reconciles_only_the_confirmed_missing_tool(self):
        saved = self.complete(self.open())
        submitted = self.service.workspace_submit(self.filler, saved['id'], {'revision': saved['revision']})
        self.approve(submitted)
        while self.service.sync_one():
            pass
        self.cloud.removed_records.add('recArchiverecTool0')
        self.service.reconcile_tool_ledger_deletions()
        batch, = self.service.workspace_history(self.filler)['sessions']
        self.assertEqual([item['id'] for item in batch['history_items']], ['recTool1'])
        self.assertEqual(len(self.service.workspace_get(self.filler, saved['id'])['items']), 1)

    def test_pending_ledger_delete_keeps_row_until_retry_is_confirmed(self):
        saved = self.complete(self.open(), ('repair', 'normal'))
        submitted = self.service.workspace_submit(self.filler, saved['id'], {'revision': saved['revision']})
        self.approve(submitted)
        while self.service.sync_one():
            pass
        row, = self.service.list(self.leader)['followups']
        self.service.action(self.leader, row['id'], 'remove-followup', {'revision': row['revision']})
        self.cloud.delete_failure = True
        self.assertTrue(self.service.tool_ledger_delete_one())
        pending, = self.service.list(self.leader)['followups']
        self.assertTrue(pending['deletion_pending'])
        self.assertIn('删除失败', pending['deletion_error'])
        with self.assertRaisesRegex(RiskError, '正在删除'):
            self.service.action(self.leader, row['id'], 'repaired', {'revision': row['revision'], 'reason': 'done'})
        self.service = self.restart()
        self.cloud.delete_failure = False
        with self.service.connect(True) as db:
            db.execute('UPDATE tool_ledger_deletions SET next_try=0')
        self.assertTrue(self.service.tool_ledger_delete_one())
        self.assertFalse(self.service.list(self.leader)['followups'])
        self.assertEqual(self.cloud.removed_records, {'recArchiverecTool0'})

    def test_inventory_refresh_does_not_block_or_replace_existing_history(self):
        saved = self.complete(self.open())
        submitted = self.service.workspace_submit(self.filler, saved['id'], {'revision': saved['revision']})
        self.approve(submitted)
        while self.service.sync_one():
            pass
        draft = self.complete(self.open(operation='draft-after-archive'))
        history = self.service.workspace_history(self.filler)
        warning = '组织架构表中 A 楼的楼长配置冲突，请核对楼长人员字段。'
        with self.service.connect(True) as db:
            self.service.set_state(db, 'inventory_error', warning)
        self.assertEqual(self.service.workspace_list(self.filler)['inventory_error'], warning)
        with patch.object(self.cloud, 'inventory', side_effect=RiskError(warning, 503)):
            with self.assertRaises(RiskError):
                self.service.workspace_refresh(self.filler)
            self.assertEqual(self.service.workspace_history(self.filler), history)
            self.assertEqual(self.service.workspace_get(self.filler, saved['id'])['status'], 'archived')
        self.cloud.assets[0]['brand'] = '更新后的品牌'
        refreshed = self.service.workspace_refresh(self.filler)
        self.assertEqual(refreshed['inventory_error'], '')
        self.assertIn('更新后的品牌', {asset['brand'] for asset in refreshed['assets']})
        self.assertEqual(self.service.workspace_history(self.filler), history)
        self.assertEqual(self.service.workspace_get(self.filler, draft['id'])['items'], draft['items'])

    def test_submit_existing_multi_tool_draft_with_seconds_and_unassigned_reviewer(self):
        draft = self.complete(self.open())
        first, second = copy.deepcopy(draft['items'])
        second['reviewer_id'] = None
        layout = second['template']
        checked_at = layout['schema']['sheet'] + '!' + layout['prefill_fields']['checked_at']
        second['values'][checked_at] += ' 08:30:45'
        saved = self.service.workspace_save(self.filler, draft['id'], {
            'revision': draft['revision'], 'items': [second]})
        self.assertEqual(saved['items'][1]['reviewer_id'], '')
        self.assertEqual(saved['items'][0]['values'], first['values'])
        restarted = self.restart()
        request = {'revision': saved['revision']}
        submitted = restarted.workspace_submit(self.filler, draft['id'], request)
        self.assertEqual(submitted['status'], 'review_pending')
        self.assertEqual(submitted['items'][1]['actual'], second['values'][checked_at][:10])
        self.assertEqual(restarted.workspace_submit(self.filler, draft['id'], request), submitted)
        reviewed = restarted.workspace_get(self.leader, draft['id'])
        self.assertTrue(all(item['can_review'] for item in reviewed['items']))

    def test_submit_invalid_check_date_names_tool_and_keeps_all_drafts(self):
        draft = self.complete(self.open())
        second = copy.deepcopy(draft['items'][1])
        checked_at = second['template']['schema']['sheet'] + '!' + second['template']['prefill_fields']['checked_at']
        second['values'][checked_at] = '2026-02-30 08:30:45'
        saved = self.service.workspace_save(self.filler, draft['id'], {
            'revision': draft['revision'], 'items': [second]})
        with self.assertRaisesRegex(RiskError, r'「工具1」检查日期.*无效'):
            self.service.workspace_submit(self.filler, draft['id'], {'revision': saved['revision']})
        reopened = self.service.workspace_get(self.filler, draft['id'])
        self.assertEqual(reopened['status'], 'draft')
        self.assertEqual([item['order_status'] for item in reopened['items']], ['pending', 'pending'])
        self.assertEqual(reopened['items'][1]['values'][checked_at], second['values'][checked_at])
        with self.service.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM tool_workspace_review_notices').fetchone()[0], 0)

    def test_review_cards_target_building_leader_or_selected_person_and_retry_same_card(self):
        self.cloud.assets[1]['scope'] = 'B'
        self.service.refresh()
        selected = actor('B', 'selected') | {'_tool_management_full_access': True}
        other = actor('B', 'other')
        self.service.person_for = {selected['id']: selected}.__getitem__
        default_leaders = {self.leader['open_id']: self.leader, other['open_id']: other}
        draft = self.complete(self.open(), outcomes=('normal', 'repair'))
        second = dict(draft['items'][1], reviewer_id=selected['id'])
        saved = self.service.workspace_save(self.filler, draft['id'], {
            'revision': draft['revision'], 'items': [second]})
        body = {'revision': saved['revision']}
        self.service.workspace_submit(self.filler, draft['id'], body)
        self.service.workspace_submit(self.filler, draft['id'], body)
        deliveries = []

        def link(scope, session, asset):
            return f'https://fixture.test/risk-inspections?view=tool-self-check&scope={scope}&tool_session={session}&tool_asset={asset}'

        def deliver(open_id, card, message_uuid):
            deliveries.append((open_id, copy.deepcopy(card), message_uuid))
            return open_id != selected['open_id']

        self.assertTrue(self.service.workspace_review_notice_one(deliver, link, default_leaders.__getitem__))
        self.assertFalse(self.service.workspace_review_notice_one(deliver, link, default_leaders.__getitem__))
        by_recipient = {item[0]: item for item in deliveries}
        self.assertEqual(set(by_recipient), {self.leader['open_id'], selected['open_id']})
        for recipient, scope, asset in ((self.leader['open_id'], 'A', 'recTool0'), (selected['open_id'], 'B', 'recTool1')):
            content = by_recipient[recipient][1]['body']['elements']
            self.assertIn(scope + '楼', content[1]['text']['content'])
            result = next(element['content'] for element in content if element['tag'] == 'markdown')
            self.assertIn('<font color="green">合格</font>' if scope == 'A'
                          else '**<font color="red">不合格·转检修</font>**', result)
            approve = next(element for element in content if element.get('text', {}).get('content') == '签名并确认')
            self.assertEqual(approve['behaviors'][0]['value']['decision'], 'approve')
            self.assertEqual(content[-1]['behaviors'][0]['default_url'], link(scope, draft['id'], asset))
        with self.service.connect(True) as db:
            db.execute('UPDATE tool_workspace_review_notices SET next_try=0')
        restarted = self.restart()
        restarted.person_for = self.service.person_for
        retry = []
        self.assertTrue(restarted.workspace_review_notice_one(
            lambda *args: retry.append(args) or True, lambda *_: '', default_leaders.__getitem__))
        self.assertEqual(retry, [by_recipient[selected['open_id']]])
        self.assertFalse(restarted.workspace_review_notice_one(deliver, link, default_leaders.__getitem__))

    def test_review_cards_follow_reassignment_and_skip_withdrawn_items(self):
        draft = self.complete(self.open())
        submitted = self.service.workspace_submit(self.filler, draft['id'], {'revision': draft['revision']})
        selected = actor('A', 'selected') | {'_tool_management_full_access': True}
        self.service.person_for = {selected['id']: selected}.__getitem__
        first, second = submitted['items']
        self.service.workspace_manage_item(self.leader, draft['id'], {
            'asset_id': first['id'], 'revision': first['order_revision'], 'action': 'reassign', 'reviewer_id': selected['id']})
        self.service.workspace_manage_item(self.leader, draft['id'], {
            'asset_id': second['id'], 'revision': second['order_revision'], 'action': 'withdraw'})
        calls = []
        notify = lambda: self.service.workspace_review_notice_one(
            lambda open_id, *_: calls.append(open_id) or True, lambda *_: 'https://fixture.test/record',
            lambda _: self.leader)
        self.assertTrue(notify())
        self.assertFalse(notify())
        self.assertEqual(calls, [selected['open_id']])

    def result_fixture(self, outcomes, *, inspector=None):
        inspector = inspector or actor(identity='checker') | {'name': '检查员甲', 'level': 'L4'}
        self.result_people = [inspector, self.leader]
        self.service.workspace_directory = lambda: [dict(record_id=p['id'], selectable=True, **p) for p in self.result_people]
        self.service.person_for = lambda identity: next(p for p in self.result_people if p['id'] == identity)
        draft = self.open()
        for item in draft['items']:
            template = item['template']
            item['values'][template['schema']['sheet']+'!'+template['prefill_fields']['inspector']] = inspector['name']
        saved = self.complete(draft, outcomes)
        return inspector, self.service.workspace_submit(self.filler, saved['id'], {'revision': saved['revision']})

    def result_deliveries(self, *, accepted=True):
        from bin.lan_bitable_template_portal.tool_result_notices import result_notice_one
        calls = []
        for _ in range(15):
            worked = result_notice_one(self.service, lambda *args: calls.append(args) or accepted,
                lambda scope, session, asset: f'https://fixture.test/{scope}?tool_session={session}&tool_asset={asset}',
                lambda scope, order: f'https://fixture.test/{scope}?tool_order={order}')
            if not worked:
                return calls
        self.fail('通知队列应在退避前完成本轮处理')

    def test_rejected_results_link_each_original_asset_and_retry_same_private_card(self):
        inspector, pending = self.result_fixture(('normal', 'repair'))
        entries = [{'revision': i['order_revision'], 'asset_id': i['id']} for i in pending['items']]
        review = {'action': 'reject', 'note': '请补充测量值', 'items': entries}
        self.service.workspace_review(self.leader, pending['id'], review)
        self.assertEqual(self.service.workspace_review(self.leader, pending['id'], review, from_card=True)['card_review_count'], 0)
        failed = self.result_deliveries(accepted=False)
        self.assertEqual([c[0] for c in failed], [inspector['open_id']])
        card = failed[0][1]['body']['elements']
        self.assertEqual([e['behaviors'][0]['default_url'] for e in card if e['tag']=='button'],
            [f"https://fixture.test/A?tool_session={pending['id']}&tool_asset={i['id']}" for i in pending['items']])
        self.assertIn('重新填写', card[0]['text']['content'])
        self.assertIn('请补充测量值', card[0]['text']['content'])
        detail = self.service.workspace_get(inspector, pending['id'])
        self.assertTrue(all(i['can_edit'] and '飞书未确认送达' in i['result_notice'] for i in detail['items']))
        with self.assertRaises(RiskError):
            self.service.workspace_get(inspector | {'home_scope':'B','scope_rules':['B']}, pending['id'])
        self.service = self.restart()
        with self.service.connect(True) as db:
            db.execute('UPDATE tool_result_messages SET next_try=0')
        self.assertEqual(self.result_deliveries(), failed)
        self.assertEqual(self.result_deliveries(), [])
        draft = self.service.workspace_save(inspector, detail['id'], {'revision':detail['revision'],'items':detail['items']})
        first, second = draft['items']
        request = {'revision':draft['revision'], 'resubmit_asset_id':second['id']}
        again = self.service.workspace_submit(inspector, draft['id'], request)
        self.assertEqual([(i['id'], i['review_status']) for i in again['items']],
                         [(first['id'], 'rejected'), (second['id'], 'pending')])
        self.assertEqual(self.service.workspace_submit(inspector, draft['id'], request)['revision'], again['revision'])
        with self.assertRaises(RiskError):
            self.service.workspace_submit(inspector, draft['id'], request | {'resubmit_asset_id':first['id']})

    def test_normal_waits_for_verified_archive_repair_can_notify_immediately(self):
        inspector, pending = self.result_fixture(('normal','repair'))
        self.approve(pending)
        self.cloud.failures.add('recTool0')
        while self.service.sync_one():
            pass
        sent = self.result_deliveries()
        self.assertEqual([c[0] for c in sent], [inspector['open_id']])
        self.assertIn('后续检修', sent[0][1]['body']['elements'][0]['text']['content'])
        self.assertNotIn('已归档', str(sent))
        self.cloud.failures.clear()
        with self.service.connect(True) as db:
            db.execute('UPDATE tool_work_sync SET next_try=0')
        self.assertTrue(self.service.sync_one())
        sent = self.result_deliveries()
        self.assertEqual(len(sent), 1)
        self.assertIn('检查记录已归档', sent[0][1]['body']['elements'][0]['text']['content'])

    def test_batch_rejection_keeps_inspectors_and_record_links_separate(self):
        from bin.lan_bitable_template_portal.tool_result_notices import result_notice_one
        people = [actor(identity='first') | {'name':'甲检查员','level':'L4'},
                  actor(identity='second') | {'name':'乙检查员','level':'L4'}]
        self.service.workspace_directory = lambda: [dict(record_id=p['id'], selectable=True, **p) for p in people]
        self.service.person_for = {p['id']:p for p in people}.__getitem__
        draft = self.open()
        for item, person in zip(draft['items'], people):
            form = item['template']
            item['values'][form['schema']['sheet']+'!'+form['prefill_fields']['inspector']] = person['name']
        saved = self.complete(draft, ('scrap','repair'))
        submitted = self.service.workspace_submit(self.filler, draft['id'], {'revision': saved['revision']})
        self.service.workspace_review(self.leader, draft['id'], {'action':'reject','note':'重新填写',
            'items':[{'revision':i['order_revision'],'asset_id':i['id']} for i in submitted['items']]})
        no_link = lambda *_: ''
        self.assertTrue(result_notice_one(self.service, self.fail, no_link, no_link))
        self.assertIn('链接暂不可用', self.service.workspace_get(self.filler, draft['id'])['items'][0]['result_notice'])
        with self.service.connect(True) as db:
            db.execute('UPDATE tool_result_targets SET next_try=0')
        calls = {recipient: card for recipient, card, _ in self.result_deliveries()}
        for person, original in zip(people, submitted['items']):
            detail = self.service.workspace_get(person, draft['id'])
            self.assertEqual([i['id'] for i in detail['items']], [original['id']])
            card = calls[person['open_id']]['body']['elements']
            self.assertEqual(len(card), 2)
            self.assertIn('已被驳回', card[0]['text']['content'])
            self.assertTrue(card[1]['behaviors'][0]['default_url'].endswith('tool_asset='+original['id']))

    def test_manager_results_private_to_inspector_and_actual_reviewer_with_shared_role_dedup(self):
        inspector, pending = self.result_fixture(('scrap','scrap'), inspector=self.leader)
        self.approve(pending)
        delegated = self.result_deliveries()
        self.assertEqual(len(delegated), 1)
        self.assertIn('移交园区经理', str(delegated))
        manager = actor(identity='manager') | {'name':'梅冰冰','employee_no':'14258'}
        with self.service.connect() as db:
            entries = [dict(order=i['order_id'], revision=self.service._scrap_votes(db,i['order_id'])[0]['request_revision'])
                       for i in pending['items']]
        for decision, entry in zip(('approve','reject'), entries):
            self.service.scrap_review_batch(manager, [entry], decision, '本次审批意见')
            self.service.scrap_review_batch(manager, [entry], decision, '本次审批意见')
            sent = self.result_deliveries()
            self.assertEqual([c[0] for c in sent], [inspector['open_id']])
            detail = sent[0][1]['body']['elements'][0]['text']['content']
            for part in (entry['order'], '已通过' if decision=='approve' else '已驳回', '梅冰冰', '审批时间',
                         '本次审批意见', '测试型号', '数量：1'):
                self.assertIn(part, detail)

    def test_same_inspector_gets_one_archive_card_when_worker_finishes_at_different_ticks(self):
        inspector, pending = self.result_fixture(('normal','normal'))
        self.approve(pending)
        self.assertTrue(self.service.sync_one())
        self.assertEqual(self.result_deliveries(), [])
        self.assertTrue(self.service.sync_one())
        sent = self.result_deliveries()
        self.assertEqual([c[0] for c in sent], [inspector['open_id']])
        self.assertEqual(len(sent[0][1]['body']['elements']), 4)

    def test_manager_card_uses_signer_after_one_tool_is_reassigned(self):
        inspector, pending = self.result_fixture(('scrap','scrap'))
        deputy = actor(identity='deputy') | {'name':'代审人员','_tool_management_full_access':True}
        self.result_people.append(deputy)
        first, second = pending['items']
        changed = self.service.workspace_manage_item(self.leader, pending['id'], {
            'asset_id':second['id'], 'revision':second['order_revision'], 'action':'reassign', 'reviewer_id':deputy['id']})
        for entry, signer in zip(changed['items'], (self.leader, deputy)):
            self.service.workspace_review(signer, pending['id'], {'action':'approve','signature':self.signature(),
                'items':[{'asset_id':entry['id'],'revision':entry['order_revision']}]})
        self.result_deliveries()
        reviewed = self.service.workspace_get(self.filler, pending['id'])
        manager = actor(identity='manager') | {'name':'马进宇','employee_no':'18871'}
        self.service.scrap_review_batch(manager, [dict(order=i['order_id'],revision=i['order_revision'])
                                                 for i in reviewed['items']], 'approve', '')
        sent = {recipient: card['body']['elements'] for recipient,card,_ in self.result_deliveries()}
        self.assertEqual(set(sent), {p['open_id'] for p in (inspector,self.leader,deputy)})
        for recipient, items in [(inspector,[first,second]),(self.leader,[first]),(deputy,[second])]:
            self.assertEqual({part['behaviors'][0]['default_url'].split('tool_asset=')[1]
                              for part in sent[recipient['open_id']] if part['tag']=='button'}, {i['id'] for i in items})

    def test_result_card_restart_recovers_real_sender_receipt_without_another_post(self):
        from bin.test_feishu_delivery import qt_credentials_fixture, TARGET_APP_ID
        from lan_bitable_template_portal.feishu_delivery import PublicFeishuSender
        from lan_bitable_template_portal.state_store import LanPortalStateStore
        from upload_event_module.services import robot_webhook as robot
        from bin.lan_bitable_template_portal.tool_result_notices import result_notice_one, send_result_card
        inspector, pending = self.result_fixture(('repair','repair'))
        self.approve(pending)
        link = lambda *_: 'https://fixture.invalid/tool'
        store_path = self.path.with_name('sender.sqlite3')
        store = LanPortalStateStore(db_path=store_path)
        store.put_document('feishu_assistant_sender', TARGET_APP_ID+':'+inspector['open_id'],
                           {'open_id':inspector['open_id'],'matched_by':'record_id'})
        self.enterContext(qt_credentials_fixture())
        self.enterContext(patch.object(robot, '_AGENT_SENDER', None))
        self.enterContext(patch.object(robot.token_manager, 'get_tenant_token', return_value='fixture-token'))
        wire = self.enterContext(patch.object(robot, '_request_json', return_value={
            'code':0,'data':{'message_id':'om_receipt','chat_id':'oc_checker'}}))
        factory = self.enterContext(patch('lan_bitable_template_portal.feishu_delivery.standalone_sender',
            return_value=PublicFeishuSender(SimpleNamespace(state_store=store))))
        def crash_after_delivery(*args):
            self.assertTrue(send_result_card(*args))
            raise SystemExit('模拟飞书收件后、业务回执落盘前重启')
        with self.assertRaises(SystemExit):
            result_notice_one(self.service, crash_after_delivery, link, link)
        self.service = self.restart()
        factory.return_value = PublicFeishuSender(SimpleNamespace(state_store=LanPortalStateStore(db_path=store_path)))
        with self.service.connect(True) as db:
            db.execute('UPDATE tool_result_messages SET next_try=0')
        self.assertTrue(result_notice_one(self.service, send_result_card, link, link))
        self.assertFalse(result_notice_one(self.service, send_result_card, link, link))
        wire.assert_called_once()

    def test_ambiguous_inspector_is_not_sent_to_submitter_and_late_resolution_notifies_only_missing_person(self):
        inspector, pending = self.result_fixture(('scrap','scrap'))
        self.result_people.append(inspector | {'id':'duplicate','open_id':'ou_duplicate'})
        reviewed = self.approve(pending)
        self.assertEqual(self.result_deliveries(), [])
        self.assertIn('同名', self.service.workspace_get(self.filler, pending['id'])['items'][0]['result_notice'])
        manager = actor(identity='manager') | {'name':'马进宇','employee_no':'18871'}
        entries = [dict(order=i['order_id'],revision=i['order_revision']) for i in reviewed['items']]
        self.service.scrap_review_batch(manager, entries, 'reject','材料待补')
        self.assertEqual([c[0] for c in self.result_deliveries()], [self.leader['open_id']])
        self.result_people.pop()
        with self.service.connect(True) as db:
            db.execute("UPDATE tool_result_targets SET next_try=0 WHERE open_id=''")
        sent = self.result_deliveries()
        self.assertEqual([c[0] for c in sent], [inspector['open_id']]*2)
        self.assertEqual(len(sent[-1][1]['body']['elements']), 4)

    def test_two_tools_share_one_card_per_manager_and_first_approval_finishes_both(self):
        from bin.lan_bitable_template_portal.feishu_assistant import FeishuAssistant
        from bin.lan_bitable_template_portal.tool_review_cards import send_scrap_review_one

        first = actor(identity='mei') | {'name': '梅冰冰', 'employee_no': '14258'}
        second = actor(identity='ma') | {'name': '马进宇', 'employee_no': '18871'}
        people = [self.leader, first, second]
        self.service.person_for = {person['id']: person for person in people}.__getitem__
        self.service.workspace_directory = lambda: [
            dict(record_id=person['id'], selectable=True, **person) for person in (first, second)]
        self.service.workspace_signature = lambda _: base64.b64decode(self.signature().split(',')[1])
        self.cloud.review_photo = Mock(return_value='img_fixture')
        self.runtime = SimpleNamespace(tool_management=lambda: self.service,
            risk_inspection_actor={person['open_id']: person for person in people}.__getitem__,
            auth_manager=SimpleNamespace(current_feishu_app_id=lambda: 'test_app'))
        channel = FeishuAssistant.__new__(FeishuAssistant)
        channel.inbox, channel.config, channel.runtime, channel.secret = object(), {'app_id': 'test_app'}, self.runtime, 'fixture'
        channel._configuration_current = AsyncMock(return_value=True)

        draft = self.complete(self.open(), ('scrap', 'scrap'))
        item = draft['items'][0] | {'scrap_photos': [self.signature()]}
        draft = self.service.workspace_save(self.filler, draft['id'], {'revision': draft['revision'], 'items': [item]})
        pending = self.service.workspace_submit(self.filler, draft['id'], {'revision': draft['revision']})
        out = []
        send = lambda *args: out.append(args) or True
        self.assertTrue(self.service.workspace_review_notice_one(send, lambda *_: 'https://fixture.test', lambda _: self.leader))
        self.assertEqual(len(out), 1)

        def approve(delivery, sender=None):
            recipient, card, _ = delivery
            values = [behavior['value'] for element in card['body']['elements']
                      for behavior in element.get('behaviors', []) if behavior.get('type') == 'callback']
            value = next(value for value in values if value['decision'] == 'approve')
            self.assertEqual(len(value['items']), 2)
            async def stream():
                yield json.dumps({'header': {'app_id': 'test_app', 'event_type': 'card.action.trigger'},
                    'event': {'action': {'value': value}, 'operator': {'open_id': sender or recipient}}}).encode()
            request = SimpleNamespace(headers={'authorization': 'Bearer fixture'},
                                      client=SimpleNamespace(host='127.0.0.1'), stream=stream)
            return json.loads(asyncio.run(channel.receive(request)).body)

        self.assertTrue(approve(out[0])['ok'])
        out.clear()
        for _ in range(2):
            self.assertTrue(send_scrap_review_one(self.service, send, lambda *_: 'https://fixture.test'))
        self.assertFalse(send_scrap_review_one(self.service, send, lambda *_: 'https://fixture.test'))
        self.assertEqual({entry[0] for entry in out}, {first['open_id'], second['open_id']})
        elements = out[0][1]['body']['elements']
        self.assertTrue(any(element.get('img_key') == 'img_fixture' and element.get('preview') is True
                            for element in elements))
        self.assertTrue(any('检查时间' in element.get('text', {}).get('content', '') for element in elements))
        self.assertTrue(any('任一位同意' in element.get('text', {}).get('content', '') for element in elements))
        ids = [entry['order_id'] for entry in pending['items']]
        self.assertFalse(approve(out[0], self.leader['open_id'])['ok'])
        self.assertIn('2 项已批准报废', approve(out[0])['toast'])
        finished = [self.service.detail(self.filler, identity) for identity in ids]
        self.assertEqual([detail['tool_status_label'] for detail in finished], ['报废', '报废'])
        self.assertIn('2 项已批准报废', approve(out[0])['toast'])
        self.assertIn('2 项已处理', approve(out[1])['toast'])
        self.assertEqual([self.service.detail(self.filler, identity) for identity in ids], finished)

    def test_lost_card_receipt_after_partial_web_review_replays_original_card(self):
        from bin.lan_bitable_template_portal.tool_review_cards import apply, send_scrap_review_one

        mei = actor(identity='mei') | {'name': '梅冰冰', 'employee_no': '14258'}
        ma = actor(identity='ma') | {'name': '马进宇', 'employee_no': '18871'}
        people = (self.leader, mei, ma)
        self.cloud.review_photo = Mock(return_value='img_fixture')

        def configure(service):
            service.person_for = {person['id']: person for person in people}.__getitem__
            service.workspace_directory = lambda: [
                dict(record_id=person['id'], selectable=True, **person) for person in (mei, ma)]
            service.workspace_signature = lambda _: base64.b64decode(self.signature().split(',')[1])
            return service

        configure(self.service)
        runtime = SimpleNamespace(tool_management=lambda: self.service,
            risk_inspection_actor={person['open_id']: person for person in people}.__getitem__,
            auth_manager=SimpleNamespace(current_feishu_app_id=lambda: 'test_app'))
        receipts, attempts = {}, []
        lost = {self.leader['open_id'], mei['open_id']}

        def deliver(recipient, card, identifier):
            attempts.append(identifier)
            self.assertEqual(receipts.setdefault(identifier, (recipient, card)), (recipient, card))
            if recipient in lost:
                lost.remove(recipient)
                return False
            return True

        def click(identifier):
            recipient, card = receipts[identifier]
            value = next(behavior['value'] for element in card['body']['elements']
                         for behavior in element.get('behaviors', [])
                         if behavior['type'] == 'callback' and behavior['value']['decision'] == 'approve')
            return apply(runtime, {'header': {'app_id': 'test_app', 'event_type': 'card.action.trigger'},
                'event': {'action': {'value': value}, 'operator': {'open_id': recipient}}}, 'test_app')['toast']

        link = lambda *_: 'https://fixture.test/original'
        draft = self.complete(self.open(), ('scrap', 'scrap'))
        first = draft['items'][0] | {'scrap_photos': [self.signature()]}
        draft = self.service.workspace_save(self.filler, draft['id'], {'revision': draft['revision'], 'items': [first]})
        pending = self.service.workspace_submit(self.filler, draft['id'], {'revision': draft['revision']})
        first, second = pending['items']
        self.assertTrue(self.service.workspace_review_notice_one(deliver, link, lambda _: self.leader))
        self.approve(pending | {'items': [first]})
        with self.service.connect(True) as db:
            db.execute('UPDATE tool_workspace_review_notices SET next_try=0')
        self.service = configure(self.restart())
        self.assertTrue(self.service.workspace_review_notice_one(deliver, lambda *_: '', lambda _: self.leader))
        self.assertEqual(attempts, [attempts[0]] * 2)
        self.assertIn('1 项自检审核通过', click(attempts[0]))
        self.assertIn('2 项已处理', click(attempts[0]))

        # The first manager received the two-item card, then the other manager rejects one in the web UI.
        for _ in range(2):
            self.assertTrue(send_scrap_review_one(self.service, deliver, link))
        manager_cards = {recipient: identifier for identifier, (recipient, _) in receipts.items()}
        mei_card = manager_cards[mei['open_id']]
        before = self.service.detail(ma, first['order_id'])
        self.service.action(ma, first['order_id'], 'reject', {'revision': before['revision'], 'reason': '单台驳回'})
        with self.service.connect(True) as db:
            db.execute('UPDATE tool_scrap_votes SET next_try=0')
        self.service = configure(self.restart())
        self.assertTrue(send_scrap_review_one(self.service, deliver, lambda *_: ''))
        self.assertFalse(send_scrap_review_one(self.service, deliver, link))
        self.assertEqual(attempts.count(mei_card), 2)
        self.assertEqual(len(receipts), 3)
        self.assertEqual(self.cloud.review_photo.call_count, 2)
        self.assertIn('1 项已批准报废', click(mei_card))
        self.assertEqual(self.service.detail(self.filler, second['order_id'])['outcome'], 'scrapped')
        self.assertIn('2 项已处理', click(manager_cards[ma['open_id']]))
        self.assertEqual(self.service.detail(self.filler, first['order_id'])['outcome'], 'rejected')
        self.assertEqual(self.service.detail(self.filler, second['order_id'])['outcome'], 'scrapped')

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
        self.assertEqual(set(self.cloud.writes), {(current['recTool0'], 'maintenance'), (current['recTool1'], 'maintenance')})
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

    def test_workbench_creates_independent_records_and_cancels_only_selected_record(self):
        worker = actor(identity='operator')
        payload = dict(kind='self', asset_ids=['recTool0'], template_id='recTemplate', new_record=True)
        first = self.service.workspace_open(worker, payload | {'operation_id': 'new-first'})
        first = self.service.workspace_save(worker, first['id'], {'revision': first['revision'],
            'items': [{'asset_id': 'recTool0', 'completion': 'completed'}]})
        self.assertIn('self', self.service.workspace_list(worker)['assets'][0]['eligible_kinds'])
        second_body = payload | {'operation_id': 'new-second'}
        second = self.service.workspace_open(worker, second_body)
        self.assertNotEqual(first['id'], second['id'])
        self.assertNotEqual(first['items'][0]['order_id'], second['items'][0]['order_id'])
        self.assertEqual(self.service.workspace_open(worker, second_body)['id'], second['id'])
        self.assertFalse(second['items'][0].get('completion'))
        history = self.service.workspace_history(worker, kind='self', asset_id='recTool0')
        self.assertEqual({row['id'] for row in history['sessions']}, {first['id'], second['id']})
        item = next(row['history_items'][0] for row in history['sessions'] if row['id'] == first['id'])
        self.assertEqual((item['asset_name'], item['scope'], item['can_release']), ('工具0', 'A', True))
        with self.assertRaisesRegex(RiskError, '不存在或无权'):
            self.service.workspace_manage_item(actor('B', 'other'), first['id'], {
                'asset_id': item['id'], 'revision': item['order_revision'], 'action': 'release', 'note': '测试取消'})
        self.service.workspace_manage_item(worker, first['id'], {
            'asset_id': item['id'], 'revision': item['order_revision'], 'action': 'release', 'note': '测试取消'})
        self.service = self.restart()
        cancelled = self.service.workspace_get(worker, first['id'])
        self.assertEqual(cancelled['status'], 'cancelled')
        self.assertEqual(cancelled['items'][0]['completion'], 'completed')
        self.assertEqual(self.service.workspace_get(worker, second['id'])['status'], 'draft')
        self.assertFalse(self.service.workspace_history(actor('B', 'other'), asset_id='recTool0')['sessions'])
        remove = {'action': 'delete', 'asset_id': 'recTool0', 'revision': cancelled['items'][0]['order_revision']}
        self.assertTrue(self.service.workspace_manage_item(worker, first['id'], remove)['batch_hidden'])
        self.assertTrue(self.service.workspace_manage_item(worker, first['id'], remove)['batch_hidden'])
        self.service = self.restart()
        self.assertEqual([row['id'] for row in self.service.workspace_history(worker)['sessions']], [second['id']])
        self.assertEqual(self.service.workspace_get(worker, first['id'])['items'][0]['completion'], 'completed')

    def test_migration_removes_full_month_constraint_when_direct_column_exists(self):
        from bin.lan_bitable_template_portal.tool_management import WORK_ORDER_SCHEMA

        first = self.open()
        schema = WORK_ORDER_SCHEMA.rstrip().removesuffix(')') + ',UNIQUE(month,asset_id,kind))'
        with self.service.connect(True) as db:
            db.execute('ALTER TABLE tool_work_orders RENAME TO old_tool_orders')
            db.execute(schema)
            db.execute('INSERT INTO tool_work_orders SELECT * FROM old_tool_orders')
            db.execute('DROP TABLE old_tool_orders')
        request = {'kind': 'self', 'asset_ids': ['recTool0'], 'template_id': 'recTemplate',
                   'operation_id': 'after-unique-cleanup', 'new_record': True}
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'UNIQUE constraint failed'):
            self.service.workspace_open(self.filler, request)
        self.service = self.restart()
        self.assertEqual(self.service.workspace_get(self.filler, first['id'])['items'], first['items'])
        additional = self.service.workspace_open(self.filler, request)
        self.assertNotEqual(additional['items'][0]['order_id'], first['items'][0]['order_id'])

    def test_transfer_with_two_old_records_releases_the_selected_one(self):
        payload = dict(kind='self', asset_ids=['recTool0'], template_id='recTemplate', new_record=True)
        sessions = [self.service.workspace_open(self.filler, payload | {'operation_id': operation})
                    for operation in ('before-move-one', 'before-move-two')]
        self.cloud.assets[0]['scope'] = 'B'
        self.service.refresh()
        receiver = actor(scope='B', identity='other')
        moved, = self.service.workspace_list(receiver)['assets']
        order_id = moved['plan']['order_id']
        self.service.workspace_transfer(receiver, {'asset_id': moved['id'], 'scope': 'B',
            'order_id': order_id, 'revision': moved['plan']['revision']})
        for original in sessions:
            item, = self.service.workspace_get(self.filler, original['id'])['items']
            self.assertEqual(item['order_status'], 'cancelled' if item['order_id'] == order_id else 'pending')

    def test_two_records_for_same_tool_approve_and_archive_separately(self):
        request = {'kind': 'self', 'asset_ids': ['recTool0'], 'template_id': 'recTemplate', 'new_record': True}
        pending = []
        for operation in ('first-check', 'second-check'):
            ready = self.complete(self.service.workspace_open(self.filler, request | {'operation_id': operation}))
            pending.append(self.service.workspace_submit(self.filler, ready['id'], {'revision': ready['revision']}))
        for session in pending:
            self.approve(session)
        first = self.service.workspace_get(self.filler, pending[0]['id'])
        remove = {'action': 'delete', 'asset_id': 'recTool0', 'revision': first['items'][0]['order_revision']}
        with self.assertRaisesRegex(RiskError, '归档'):
            self.service.workspace_manage_item(self.filler, first['id'], remove)
        while self.service.sync_one():
            pass
        self.assertEqual(set(self.cloud.writes), {
            (session['items'][0]['order_id'], 'maintenance') for session in pending})
        first = self.service.workspace_get(self.filler, first['id'])
        result = self.service.workspace_manage_item(self.filler, first['id'], remove | {'revision': first['items'][0]['order_revision']})
        self.assertTrue(result['deletion_pending'])
        self.assertTrue(self.service.tool_ledger_delete_one())
        self.assertEqual([row['id'] for row in self.service.workspace_history(self.filler)['sessions']], [pending[1]['id']])
        with self.assertRaisesRegex(RiskError, '不存在或无权'):
            self.service.workspace_get(self.filler, first['id'])
        self.assertEqual(len(self.cloud.writes), 2)

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
        old_schema = WORK_ORDER_SCHEMA.replace('workspace_direct INTEGER NOT NULL DEFAULT 0,', '')
        old_schema = old_schema.rstrip().removesuffix(')') + ',UNIQUE(month,asset_id,kind))'
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


"""Read-only local assistant projections: permissions, failure state and fresh reads."""
import hashlib
import json
import socket
import sqlite3
import sys
import tempfile
import threading
import unittest
from contextlib import ExitStack, closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_sources import LocalAssistantSources


NOTICE = {'maintenance', 'change', 'repair', 'polling', 'adjust', 'power'}
ALL = NOTICE | {'repair_management', 'event', 'daily', 'water', 'engineer_mop', 'drills',
                'signatures', 'critical_guard', 'cabinet', 'plan_convergence'}


class SharedSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / 'readonly-copy.sqlite3'
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE json_documents(namespace TEXT,key TEXT,payload_json TEXT);
                CREATE TABLE ongoing_items(payload_json TEXT,building_codes_json TEXT);
                CREATE TABLE repair_snapshot_records(source_key TEXT,record_id TEXT,title TEXT,status TEXT,
                    scope_codes_json TEXT,payload_json TEXT,search_text TEXT DEFAULT '');
                CREATE TABLE repair_snapshot_sources(source_key TEXT,status TEXT);
                CREATE TABLE source_table_snapshot_manifest(snapshot_id TEXT,status TEXT);
                CREATE TABLE source_table_snapshot_records(snapshot_id TEXT,scope TEXT,work_type TEXT,payload_json TEXT);
                CREATE TABLE qt_active_items(payload_json TEXT,deleted_at REAL,updated_at REAL);
                CREATE TABLE water_consumption_records(scope_code TEXT,payload_json TEXT);
                CREATE TABLE engineer_mop_local_files(payload_json TEXT,scope TEXT,notice_title TEXT,status TEXT,deleted_at REAL);
                CREATE TABLE critical_guard_tasks(task_id TEXT,task_name TEXT,status TEXT,target_scopes_json TEXT,created_by_name TEXT);
                CREATE TABLE critical_guard_responses(task_id TEXT,scope_code TEXT,status TEXT,check_date TEXT,cells_json TEXT,sheet_type TEXT);
            ''')
            keys = ['repair_projects', 'repair_events', 'repair_notices', *['notice_target.' + m for m in NOTICE - {'repair'}]]
            db.executemany('INSERT INTO repair_snapshot_sources VALUES (?,?)', [(k, 'active') for k in keys])
            db.execute('INSERT INTO source_table_snapshot_manifest VALUES (?,?)', ('current', 'active'))
        self.reader = LocalAssistantSources(SimpleNamespace(db_path=self.path))

    @staticmethod
    def actor(modules=ALL, scopes=('A',), **extra):
        return {'id': 'fixture-person', 'scopes': list(scopes), 'modules': ['assistant', *modules], 'is_admin': False, **extra}

    def insert(self, table, **values):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('INSERT INTO ' + table + '(' + ','.join(values) + ') VALUES (' + ','.join('?' for _ in values) + ')',
                       tuple(json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v for v in values.values()))

    def document(self, namespace, identity, **values):
        self.insert('json_documents', namespace=namespace, key=identity, payload_json=values)

    def ongoing(self, identity, module='maintenance', scopes=('A',)):
        self.insert('ongoing_items', payload_json={'record_id': identity, 'title': '柴发' + identity,
            'building_codes': list(scopes), 'work_type': module, 'status': '进行中'}, building_codes_json=list(scopes))

    def repair(self, identity, source='repair_projects', scopes=('A',)):
        self.insert('repair_snapshot_records', source_key=source, record_id=identity, title='柴发' + identity,
            status='进行中', scope_codes_json=list(scopes), payload_json={'record_id': identity, 'title': '柴发' + identity,
            'fields': {'楼栋': '、'.join(s + '楼' for s in scopes), '名称': '柴发' + identity}})

    def plan(self, identity, module='maintenance', scope='A'):
        self.insert('source_table_snapshot_records', snapshot_id='current', scope=scope, work_type=module,
            payload_json={'record_id': identity, 'title': '柴发' + identity, 'scope': scope, 'work_type': module,
                          'fields': {'维护实施状态': '未开始'}})

    def search(self, actor, query='A楼柴发'):
        return self.reader(query, actor)

    def pending(self, actor, query='未完成工作'):
        hits, warnings = self.search(actor, query)
        return {g['模块']: g for g in hits[0]['data']['分组']}, hits, warnings

    def test_overview_filters_modules_before_counts_and_details(self):
        self.ongoing('allowed-maint')
        self.ongoing('hidden-change', 'change')
        self.repair('hidden-repair')
        self.repair('hidden-event', 'repair_events')
        groups, hits, warnings = self.pending(self.actor({'maintenance'}))
        self.assertEqual(groups['进行中通告']['数量'], 1)
        self.assertNotIn('未完成维修项目', groups)
        self.assertNotIn('未闭环事件', groups)
        self.assertNotIn('演练任务', groups)
        self.assertNotIn('上下电待办', groups)
        text = json.dumps(hits, ensure_ascii=False)
        for marker in ('hidden-change', 'hidden-repair', 'hidden-event'):
            self.assertNotIn(marker, text)
        self.assertFalse(any('事件' in warning or '维修项目' in warning for warning in warnings))

    def test_search_sources_are_filtered_by_their_real_module(self):
        self.ongoing('maintenance-source')
        self.plan('change-source', 'change')
        self.repair('repair-management-source')
        self.repair('event-source', 'repair_events')
        self.document('polling_sop', 'sop', name='柴发polling-source', scope='A')
        self.document('daily_work_report', 'daily', reports={'A': '柴发daily-source'})
        self.document('drill_definition', 'drill', name='柴发drills-source', assigned_scopes=['A'], status='published')
        self.document('signature_management_person', 'person', name='柴发signatures-source', scope='A')
        self.insert('water_consumption_records', scope_code='A', payload_json={'title': '柴发water-source', 'scope': 'A'})
        self.insert('engineer_mop_local_files', payload_json={'title': '柴发engineer-mop-source'}, scope='A',
                    notice_title='柴发engineer-mop-source', status='draft', deleted_at=None)
        self.insert('critical_guard_tasks', task_id='guard', task_name='柴发critical-guard-source', status='active',
                    target_scopes_json=['A'], created_by_name='fixture')
        self.document('cabinet_power', 'export', title='柴发cabinet-source', scope='A')
        markers = {'maintenance': 'maintenance-source', 'change': 'change-source', 'repair_management': 'repair-management-source',
                   'event': 'event-source', 'polling': 'polling-source', 'daily': 'daily-source', 'drills': 'drills-source',
                   'signatures': 'signatures-source', 'water': 'water-source', 'engineer_mop': 'engineer-mop-source',
                   'critical_guard': 'critical-guard-source', 'cabinet': 'cabinet-source'}
        for module, expected in markers.items():
            with self.subTest(module=module):
                hits, _ = self.search(self.actor({module}))
                text = json.dumps(hits, ensure_ascii=False)
                self.assertIn(expected, text)
                for other, marker in markers.items():
                    if other != module:
                        self.assertNotIn(marker, text)

    def test_all_notice_types_and_sop_types_are_independently_filtered(self):
        for module in NOTICE:
            self.plan('plan-' + module, module)
            self.ongoing('ongoing-' + module, module)
        for module in ('maintenance', 'polling', 'adjust'):
            self.document('polling_work_order', module, name='柴发order-' + module, scope='A', work_type=module, state='active')
        for module in NOTICE:
            with self.subTest(module=module):
                text = json.dumps(self.search(self.actor({module}))[0], ensure_ascii=False)
                self.assertIn('plan-' + module, text)
                for other in NOTICE - {module}:
                    self.assertNotIn('plan-' + other, text)
                groups, hits, _ = self.pending(self.actor({module}))
                self.assertEqual(groups['进行中通告']['数量'], 1)
                for other in NOTICE - {module}:
                    self.assertNotIn('ongoing-' + other, json.dumps(hits))
                self.assertEqual(groups.get('SOP工单', {}).get('数量', 0), int(module in {'maintenance', 'polling', 'adjust'}))

    def test_different_people_and_requested_building_remain_isolated(self):
        self.ongoing('building-A', scopes=('A',))
        self.ongoing('building-B', scopes=('B',))
        self.document('daily_morning_meeting', 'A', file_name='柴发meeting-A', scope='A')
        self.document('daily_morning_meeting', 'B', file_name='柴发meeting-B', scope='B')
        for scope in ('A', 'B'):
            actor = self.actor({'maintenance', 'daily'}, (scope,), id='person-' + scope)
            text = json.dumps(self.search(actor, scope + '楼柴发')[0], ensure_ascii=False)
            self.assertIn('building-' + scope, text)
            self.assertIn('meeting-' + scope, text)
            self.assertNotIn('building-' + ('B' if scope == 'A' else 'A'), text)
            self.assertNotIn('meeting-' + ('B' if scope == 'A' else 'A'), text)
        text = json.dumps(self.search(self.actor({'maintenance', 'daily'}, ('A', 'B')), 'A楼柴发')[0])
        self.assertNotIn('building-B', text)
        self.assertNotIn('meeting-B', text)
        with self.assertRaises(AssistantError):
            self.search(self.actor({'maintenance'}), 'B楼柴发')

    def test_complete_row_scope_cannot_be_split_between_topic_participants(self):
        self.ongoing('maint-A', scopes=('A',))
        self.ongoing('maint-B', scopes=('B',))
        self.ongoing('maint-AB', scopes=('A', 'B'))
        self.repair('repair-A', scopes=('A',))
        self.repair('repair-B', scopes=('B',))
        actor = self.actor({'maintenance', 'repair_management'}, ('A', 'B'), _topic_capabilities=[
            {'modules': ['maintenance'], 'scopes': ['A']}, {'modules': ['repair_management'], 'scopes': ['B']}])
        for query in ('柴发资料', '未完成工作'):
            text = json.dumps(self.search(actor, query)[0])
            self.assertIn('maint-A', text)
            self.assertIn('repair-B', text)
            for marker in ('maint-B', 'maint-AB', 'repair-A'):
                self.assertNotIn(marker, text)

    def test_missing_modules_and_admin_flag_do_not_bypass_module_guards(self):
        self.repair('private-repair')
        for actor in (self.actor(set()), {'id': 'legacy', 'scopes': ['A'], 'is_admin': True},
                      self.actor({'maintenance'}, is_admin=True)):
            with self.subTest(actor=actor):
                text = json.dumps(self.search(actor)[0])
                self.assertNotIn('private-repair', text)
        with patch.object(self.reader, '_read', side_effect=AssertionError('No grants must not read source data')):
            for actor in (self.actor(set()), {'id': 'missing-modules', 'scopes': ['A'], 'is_admin': False}):
                for query in ('A楼柴发', 'A楼未完成工作'):
                    hits, warnings = self.search(actor, query)
                    self.assertEqual(hits, [])
                    self.assertTrue(any('模块权限' in warning for warning in warnings))

    def test_assigned_drill_title_is_projected_to_each_authorized_building(self):
        self.document('drill_definition', 'multi-building', name='柴发演练标题', assigned_scopes=['A', 'B'],
                      status='published', configuration={'A': 'local-detail', 'B': 'hidden-building-detail'})
        for actor in (self.actor({'drills'}), self.actor({'drills'}, ('A', 'B'), _topic_capabilities=[
                {'modules': ['drills'], 'scopes': ['A']}, {'modules': ['maintenance'], 'scopes': ['B']}])):
            hits, _ = self.search(actor)
            drill = [row for row in hits if row['title'].startswith('演练模板 ·')]
            self.assertEqual(len(drill), 1)
            self.assertEqual(drill[0]['scopes'], ['A'])
            self.assertEqual(drill[0]['data']['scope'], 'A')
            self.assertIn('柴发演练标题', drill[0]['title'])
            self.assertNotIn('configuration', drill[0]['data'])
            self.assertNotIn('hidden-building-detail', json.dumps(hits))
        groups, _, _ = self.pending(self.actor({'drills'}))
        self.assertEqual(groups['演练任务']['数量'], 1)

    def test_failed_empty_event_and_repair_sources_are_unknown_not_zero(self):
        for source, module, label in (('repair_events', 'event', '未闭环事件'),
                                      ('repair_projects', 'repair_management', '未完成维修项目'),
                                      ('notice_target.maintenance', 'maintenance', '进行中通告')):
            with self.subTest(source=source):
                with closing(sqlite3.connect(self.path)) as db, db:
                    db.execute('UPDATE repair_snapshot_sources SET status=? WHERE source_key=?', ('failed', source))
                groups, _, warnings = self.pending(self.actor({module}))
                self.assertIsNone(groups[label]['数量'])
                self.assertNotEqual(groups[label]['读取状态'], '已读取')
                self.assertTrue(any(label in warning for warning in warnings))

    def test_successfully_initialized_empty_source_is_a_valid_zero(self):
        groups, _, warnings = self.pending(self.actor({'event', 'repair_management', 'maintenance'}))
        for label in ('未闭环事件', '未完成维修项目', '进行中通告'):
            self.assertEqual(groups[label]['数量'], 0)
            self.assertEqual(groups[label]['读取状态'], '已读取')
            self.assertFalse(any(label in warning for warning in warnings))

    def test_other_module_or_building_cannot_prove_plan_snapshot_ready(self):
        self.plan('hidden-module', 'change')
        self.plan('hidden-building', 'maintenance', 'B')
        groups, _, warnings = self.pending(self.actor({'maintenance'}))
        self.assertIsNone(groups['待执行计划事项']['数量'])
        self.assertTrue(any('待执行计划事项' in warning for warning in warnings))

    def test_failed_retained_rows_are_samples_not_confirmed_complete_counts(self):
        self.repair('retained-event', 'repair_events')
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('UPDATE repair_snapshot_sources SET status=? WHERE source_key=?', ('failed', 'repair_events'))
        groups, _, _ = self.pending(self.actor({'event'}))
        self.assertIsNone(groups['未闭环事件']['数量'])

    def test_same_reader_sees_committed_updates_deletions_and_permission_revocation(self):
        self.ongoing('first')
        actor = self.actor({'maintenance'})
        self.assertEqual(self.pending(actor)[0]['进行中通告']['数量'], 1)
        self.ongoing('second')
        self.assertEqual(self.pending(actor)[0]['进行中通告']['数量'], 2)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DELETE FROM ongoing_items')
        self.assertEqual(self.pending(actor)[0]['进行中通告']['数量'], 0)
        self.repair('fresh-repair')
        actor['modules'] = ['assistant', 'repair_management']
        self.assertIn('fresh-repair', json.dumps(self.search(actor)[0]))
        actor['modules'] = ['assistant']
        self.assertEqual(self.search(actor)[0], [])

    def test_normal_search_updates_immediately_without_recreating_reader(self):
        self.repair('old-title')
        actor = self.actor({'repair_management'})
        self.assertIn('old-title', json.dumps(self.search(actor)[0]))
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('UPDATE repair_snapshot_records SET title=?,payload_json=?', ('柴发new-title',
                json.dumps({'record_id': 'old-title', 'title': '柴发new-title', 'fields': {'楼栋': 'A楼', '名称': '柴发new-title'}}, ensure_ascii=False)))
        text = json.dumps(self.search(actor)[0], ensure_ascii=False)
        self.assertIn('new-title', text)
        self.assertNotIn('柴发old-title', text)

    def test_unauthorized_notice_types_do_not_consume_search_limits(self):
        for n in range(90):
            self.plan('hidden-' + str(n), 'change')
            self.ongoing('hidden-' + str(n), 'change')
        self.plan('allowed-plan')
        self.ongoing('allowed-notice')
        text = json.dumps(self.search(self.actor({'maintenance'}))[0])
        self.assertIn('allowed-plan', text)
        self.assertIn('allowed-notice', text)
        self.assertNotIn('hidden-', text)

    def test_reader_has_no_network_background_work_or_business_writes(self):
        import httpx
        self.ongoing('readonly-notice')
        self.repair('readonly-project')
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        files = set(self.root.rglob('*'))
        connect = sqlite3.connect
        statements = []
        def read_only_connect(path, *args, **kwargs):
            self.assertIn('mode=ro', str(path))
            self.assertIs(kwargs.get('uri'), True)
            connection = connect(path, *args, **kwargs)
            connection.set_trace_callback(statements.append)
            return connection
        with ExitStack() as stack:
            stack.enter_context(patch('sqlite3.connect', side_effect=read_only_connect))
            stack.enter_context(patch.object(socket.socket, 'connect', side_effect=AssertionError('No network')))
            stack.enter_context(patch.object(threading.Thread, 'start', side_effect=AssertionError('No background work')))
            stack.enter_context(patch('requests.Session.request', side_effect=AssertionError('No Feishu request')))
            stack.enter_context(patch.object(httpx.Client, 'request', side_effect=AssertionError('No HTTP request')))
            for query in ('A楼柴发', 'A楼未完成工作'):
                self.assertTrue(self.search(self.actor({'maintenance', 'repair_management'}), query)[0])
        self.assertTrue(statements)
        self.assertTrue(all(sql.lstrip().upper().startswith('SELECT') for sql in statements))
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
        self.assertEqual(set(self.root.rglob('*')), files)


if __name__ == '__main__':
    unittest.main()

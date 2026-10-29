import copy
import datetime as dt
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import personnel_source as source
from lan_bitable_template_portal import personnel_signature_migration as migration


class Store:
    def __init__(self): self.docs = {}
    def get_document(self, ns, key): return copy.deepcopy(self.docs.get((ns, key)))
    def put_document(self, ns, key, value): self.docs[(ns, key)] = copy.deepcopy(value)


def row(rid, name='人员甲', number='10001', oid='ou_one', **fields):
    return {'record_id': rid, 'fields': {'姓名': [{'text': name}], '员工工号': [{'text': number}],
        '员工姓名': [{'id': oid, 'name': name}], '机楼/专业': ['B楼'], **fields}}


class PersonnelSourceTests(unittest.TestCase):
    def test_lookup_option_ids_are_resolved_from_metadata_not_hardcoded(self):
        service = Mock()
        source_fields = [{'field_name': '班次', 'property': {'filter_info': {'target_table': 'tblSchedule'}, 'target_field': 'fldShift'}},
            {'field_name': '班组', 'property': {'options': [{'id': 'optTeam', 'name': 'B班'}]}},
            {'field_name': '机楼/专业', 'property': {'options': [{'id': 'optBuilding', 'name': 'B楼'}]}}]
        service._load_raw_table_fields.side_effect = lambda **kw: source_fields if kw['table_id']==source.TABLE_ID else [
            {'field_id': 'fldShift', 'property': {'options': [{'id': 'optToday', 'name': '夜'}]}}]
        response = {'code':0,'data':{'items':[row('rec1', 班组='optTeam', 班次={'value':['optToday']}, **{'机楼/专业':['optBuilding']})], 'has_more':False}}
        service._request_json.side_effect = lambda *args, **kwargs: copy.deepcopy(response)
        actual=source.read_records(service)
        self.assertEqual(source.shift_kind(actual[0]['fields']), '夜')
        self.assertEqual(actual[0]['fields']['机楼/专业'], ['B楼'])
        self.assertEqual(service._request_json.call_count, 1)
        self.assertEqual(service._request_json.call_args.kwargs['table_id'], source.TABLE_ID)
        service._load_raw_table_fields.side_effect = lambda **kw: source_fields
        with self.assertRaisesRegex(ValueError, '班次.*选项定义'):
            source.read_records(service)

    def test_personnel_consumers_and_two_signature_stores_share_one_source(self):
        from lan_bitable_template_portal import portal_service, learning_reminders, assistant_tables, personnel_password_login
        for actual in (portal_service.PERMISSION_DIRECTORY_TABLE_ID, portal_service.SIGNATURE_TABLE_ID,
                       learning_reminders.TABLE_ID, assistant_tables.PERSONNEL_TABLE, personnel_password_login.TABLE_ID):
            self.assertEqual(actual, source.TABLE_ID)
        self.assertEqual(portal_service.TEMP_SIGNATURE_TABLE_ID, source.EXTERNAL_TABLE_ID)

    def test_roster_warning_counts_invalid_rows_and_excludes_long_day(self):
        from lan_bitable_template_portal.pre_shift_cloud import read_roster
        service=Mock()
        service.get_permission_directory_people.return_value={'items':[]}
        issues=[{'name':f'人员{i}','scope':'A','group':'A班','reason':'班次待核对'} for i in range(8)]
        issues.append({'name':'长白人员','scope':'B','group':'长白','reason':'班次待核对'})
        with patch.object(source,'daily_roster',return_value={'people':[],'issues':issues}):
            result=read_roster(service,'2026-10-27',scopes=['A','B'])
        self.assertIn('0/8人',result['warnings']['A'])
        self.assertIn('另有5项',result['warnings']['A'])
        self.assertNotIn('B',result['warnings'])

    def test_signature_cloud_has_only_two_fields_and_old_references_stay_stable(self):
        from lan_bitable_template_portal.portal_service import MaintenancePortalService, SIGNATURE_TABLE_ID, TEMP_SIGNATURE_TABLE_ID
        self.assertEqual(set(migration.FIELDS), {'手写签名','密钥'})
        with patch.object(source, 'signature_aliases', return_value={'recOld':'recCurrent'}):
            self.assertEqual(source.signature_person_id('recCurrent'), 'recOld')
            self.assertEqual(source.signature_person_id('recNewEmployee'), 'recNewEmployee')
            service=MaintenancePortalService.__new__(MaintenancePortalService)
            self.assertEqual(service._signature_storage_id(SIGNATURE_TABLE_ID,'recOld'),'recCurrent')
            self.assertEqual(service._signature_storage_id(TEMP_SIGNATURE_TABLE_ID,'recOld'),'recOld')

    def test_lookup_shift_values_and_team_rules(self):
        for group, shift, expected in [('长白', '白', '长白'), ('A班', '白', '白'),
                ('B班', '夜', '夜'), ('C班', '', ''), ('D班', None, ''), ('长白', '夜', '待核对')]:
            self.assertEqual(source.shift_kind({'班组': group, '班次': {'type': 3, 'value': [shift] if shift else []}}), expected)
        self.assertEqual(source.shift_kind({'班组':'A班', '班次':{'value':['白','夜']}}), '待核对')

    def test_full_table_read_projects_fields_and_rejects_incomplete_pages(self):
        service = Mock()
        service._request_json.side_effect = [
            {'code':0,'data':{'items':[row('rec1')],'has_more':True,'page_token':'next'}},
            {'code':0,'data':{'items':[row('rec2')],'has_more':False}},
        ]
        self.assertEqual(len(source.read_records(service)), 2)
        for call in service._request_json.call_args_list:
            self.assertEqual(call.kwargs['table_id'], source.TABLE_ID)
            self.assertNotIn('view_id', call.kwargs['params'])
            self.assertNotIn('密码', call.kwargs['params']['field_names'])
        service._request_json.side_effect = None
        service._request_json.return_value = {'code':0,'data':{'items':[row('rec1')]}}
        with self.assertRaises(ValueError): source.read_records(service)

    def test_empty_employee_link_is_explicit_even_with_old_text_openid(self):
        service=SimpleNamespace(_state_store=Store())
        today=dt.datetime.now(source.TZ).date().isoformat()
        rows=[row('recEmpty',班组='A班',班次='白',**{'员工姓名':[],'open_id':'ou_old'}),
              row('recNested',班组='A班',班次='白',**{'员工姓名':{'value':[]}})]
        with patch.object(source,'read_records',return_value=rows):
            people=source.daily_roster(service,today)['people']
        self.assertEqual([p['employee_linked'] for p in people],[False,False])
        self.assertEqual([p['open_id'] for p in people],['',''])

    def test_today_snapshot_keeps_long_day_separate_and_never_invents_past_roster(self):
        service = SimpleNamespace(_state_store=Store())
        today = dt.datetime.now(source.TZ).date()
        rows = [row('rec1', 班组='长白', 班次={'value':['白']}),
                row('rec2', name='人员乙', oid='ou_two', 班组='A班', 班次={'value':['白']}),
                row('rec3', name='休班', 班组='B班', 班次={'value':[]})]
        with patch.object(source, 'read_records', return_value=rows):
            data = source.daily_roster(service, today.isoformat())
            self.assertEqual([p['duty_kind'] for p in data['people']], ['长白', '白'])
            self.assertEqual({p['scope'] for p in data['people']}, {'B'})
            with self.assertRaises(ValueError): source.daily_roster(service, (today-dt.timedelta(days=1)).isoformat())
            from lan_bitable_template_portal.learning_reminders import read_roster
            self.assertEqual([p['name'] for p in read_roster(service, today.isoformat(), scopes=['B'])], ['人员乙'])
            self.assertEqual(len(read_roster(service, today.isoformat(), scopes=['B'], include_long_day=True)), 2)

    def test_migration_matches_existing_identity_without_overwriting_personnel(self):
        old = row('rec_old', 手写签名=[{'file_token':'old'}], 密钥='metadata', 楼栋=['D楼'])
        target = row('rec_target', 机楼专业=['B楼'])
        before = copy.deepcopy(target)
        pairs, unresolved = migration.match_people([old], [target])
        self.assertEqual(len(pairs), 1)
        self.assertEqual(unresolved, [])
        self.assertEqual(target, before)
        for altered in [row('rec_wrong', name='其他人'), row('rec_wrong', oid='ou_other')]:
            self.assertEqual(migration.match_people([old], [altered])[0], [])

    def test_daily_learning_includes_only_working_operations_interns_from_job_field(self):
        from lan_bitable_template_portal.learning_reminders import read_roster
        service = SimpleNamespace(_state_store=Store())
        today = dt.datetime.now(source.TZ).date().isoformat()
        rows = [
            row('recIntern', name='当班实习', 班组='长白', 班次={'value':['白']}, 岗位=['运维实习生'], 体系岗位=''),
            row('recRest', name='休班实习', 班组='长白', 班次={'value':[]}, 岗位='运维实习生'),
            row('recFoc', name='其他岗位', 班组='长白', 班次='白', 岗位='FOC', 体系岗位='实习生'),
            row('recEngineer', name='工程师', 班组='长白', 班次='白', 岗位='电气工程师'),
            row('recDuty', name='普通白班', 班组='A班', 班次='白'),
            row('recNight', name='普通夜班', 班组='B班', 班次='夜'),
            row('recLeft', name='离职实习', 班组='长白', 班次='白', 岗位='运维实习生', **{'离职/异动情况':'离职'}),
            row('recIssue', name='楼栋待核对实习', 班组='长白', 班次='白', 岗位='运维实习生', **{'机楼/专业':[]}),
        ]
        with patch.object(source, 'read_records', return_value=rows) as fetch:
            raw = source.daily_roster(service, today)
            self.assertEqual(next(p for p in raw['people'] if p['name']=='当班实习')['duty_kind'], '长白')
            eligible = read_roster(service, today, scopes=['B'], with_status=True)
            self.assertEqual([p['name'] for p in eligible['people']], ['当班实习','普通白班','普通夜班'])
            self.assertEqual([p['name'] for p in eligible['issues']], ['楼栋待核对实习'])
            self.assertEqual(len(read_roster(service, today, scopes=['B'], include_long_day=True)), 5)
            # Upgrade a still-fresh raw snapshot which predates the 岗位 field.
            old = copy.deepcopy(raw); old['parser_version'] = 2
            for person in old['people']: person.pop('position', None)
            service._state_store.put_document(source.ROSTER_NAMESPACE, today, old)
            self.assertEqual(read_roster(service, today, scopes=['B'])[0]['position'], '运维实习生')
            self.assertEqual(fetch.call_count, 2)

    def test_migration_does_not_merge_duplicate_names_or_employee_numbers(self):
        self.assertFalse(migration.match_people([row('old')], [row('one'),row('two')])[0])
        self.assertFalse(migration.match_people([row('old1'),row('old2')], [row('new')])[0])
        matched, unresolved = migration.match_people([row('old')], [row('one'),row('two', name='其他人', oid='ou_other')])
        self.assertEqual(matched[0][1]['record_id'], 'one')
        self.assertFalse(unresolved)

    def test_migration_copy_readback_resume_and_existing_signature_preservation(self):
        old = [row('rec_old', 手写签名=[{'file_token':'old','name':'one.sigenc'}], 密钥='metadata')]
        target = [row('rec_target')]
        service = SimpleNamespace(_state_store=Store(), signature_management=SimpleNamespace(invalidate=Mock()),
            _extract_signature_attachments=lambda f:f.get('手写签名',[]),
            _attachment_with_cache_context=lambda attachment,**kw:attachment,
            _download_mop_attachment=Mock(return_value=(b'encrypted-original','application/octet-stream')),
            _upload_encrypted_signature_file=Mock(return_value='new-token'), _auth_headers=lambda:{})
        def records(s, **kw): return copy.deepcopy(old if kw.get('table')==migration.OLD_TABLE else target)
        def write(method,url,**kw):
            self.assertTrue(url.endswith('/records/batch_update'))
            for item in kw['json_payload']['records']:
                self.assertEqual(item['record_id'], 'rec_target')
                self.assertEqual(set(item['fields']), {'手写签名','密钥'})
                target[0]['fields'].update(item['fields'])
            raise TimeoutError('response lost after commit')
        service._request_payload = Mock(side_effect=write)
        with patch.object(migration, 'ensure_fields'), patch.object(source, 'read_records', side_effect=records), patch.object(migration.time, 'sleep'):
            result = migration.migrate(service)
            self.assertTrue(result['complete'])
            self.assertEqual(result['verified_updates'], 1)
            self.assertEqual(target[0]['fields']['机楼/专业'], ['B楼'])
            migration.migrate(service, force=True)
            self.assertEqual(service._upload_encrypted_signature_file.call_count, 1)
            self.assertEqual(service._request_payload.call_count, 1)
            target[0]['fields']['手写签名'] = [{'file_token':'newer-user-signature'}]
            migration.migrate(service, force=True)
            self.assertEqual(target[0]['fields']['手写签名'][0]['file_token'], 'newer-user-signature')


if __name__ == '__main__': unittest.main()

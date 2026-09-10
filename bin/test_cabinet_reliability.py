import copy
import json
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
from . import test_cabinet_power as base
from .test_cabinet_power import FakeFeishu, MemoryStore, fixtures
from .lan_bitable_template_portal.cabinet_power import CabinetPowerService,SNAPSHOT_KEY
from .lan_bitable_template_portal.cabinet_power_data import from_feishu,to_fields,source_evidence,complete_source_record
from .lan_bitable_template_portal.cabinet_power_excel import CabinetError,Workbook,T,export_workbook,map_state_baseline,derive_records,project_layout,calculate


class CabinetReliabilityTests(unittest.TestCase):
    setUpClass=base.CabinetPowerTests.__dict__['setUpClass']
    setUp=base.CabinetPowerTests.setUp
    tearDown=base.CabinetPowerTests.tearDown

    def install_colors(self,scope):
        if scope=='C':
            evidence=source_evidence((base.TEMPLATES/'C.xlsm').read_bytes(),self.models['C'])
            for rid,record in list(self.remote.records.items()):
                if record['fields']['楼栋']=='C楼': self.remote.records[rid]=complete_source_record(record,evidence) or record
            self.service.do_refresh(scope,{},{})
        snap=self.service.snapshot(scope)
        baseline,_=map_state_baseline((base.TEMPLATES/(scope+'.xlsm')).read_bytes(),snap['config'],snap['operations'])
        for room in snap['config']['rooms']:
            record=self.service._directory.records[room['record_id']]
            geometry=json.loads(record['fields']['布局资料'])
            geometry['power_baseline']={'version':1,'template_hash':snap['config']['template_data']['hash'],'racks':{k.split('/')[1]:v for k,v in baseline.items() if k.startswith(room['id']+'/')}}
            if scope=='C' and room['id']=='202': geometry['region']='B1:AX49'
            record['fields']['布局资料']=json.dumps(geometry,ensure_ascii=False)
        self.service.do_refresh(scope,{},{})
        return self.service.snapshot(scope)

    def test_latest_success_overrides_physical_colors_and_formula_totals(self):
        expected={'A':970,'B':1022,'C':970,'D':673,'E':402}
        for scope,count in expected.items():
            snap=self.install_colors(scope)
            snap['config']['map_values']={}
            snap['config']['template_data']['summary_cells']={'bad':{'B4':99999,'C4':99999,'D4':99999}}
            self.assertEqual(derive_records(snap['config'],snap['operations'])['counts']['powered'],count)
        c=self.service.overview('C')['counts']
        self.assertEqual((c['formal'],c['test'],c['off'],c['unknown']),(659,311,28,0))

    def test_new_success_failure_correction_and_removal_after_color_baseline(self):
        snap=self.install_colors('D'); old=next(o for o in snap['operations'] if o['room']=='201' and o['rack']=='A01')
        group={'id':'color_new_event','action':'上正式电','actual':'2026-09-09 12:00:00','expected':'','result':'成功'}
        def state(groups):
            ops=copy.deepcopy(snap['operations']); record=next(o for o in ops if o['record_id']==old['record_id'])
            record.update(from_feishu({'record_id':old['record_id'],'fields':{**old['raw_fields'],**to_fields({**old,'groups':groups})}}))
            return next(r['state'] for r in derive_records(snap['config'],ops)['racks'] if r['room']=='201' and r['rack']=='A01')
        self.assertEqual(state([group]),'formal')
        self.assertEqual(state([{**group,'action':'上测试电'}]),'test')
        self.assertEqual(state([{**group,'result':'失败'}]),'off')
        self.assertEqual(state([]),'off')

    def test_c_map_room_and_building_counters_match_latest_success_on_page_and_export(self):
        snap=self.install_colors('C'); model=self.service.layout('C','202')['layout']; cells={c['ref']:c for c in model['cells']}
        room=next(r for r in self.service.overview('C')['rooms'] if r['id']=='202')['counts']
        expected={'F39':170,'F40':room['test'],'F41':room['formal'],'F42':0,'F43':170,'F45':311,'F46':659,'F47':28,'F48':970,'F49':998}
        for ref,count in expected.items(): self.assertEqual(cells[ref]['text'],str(count),ref)
        out=Workbook(export_workbook((base.TEMPLATES/'C.xlsm').read_bytes(),snap['config'],snap['operations']))
        exported=out.cells('202-M1机柜平面图')
        for ref,count in expected.items(): self.assertEqual(out.value(exported[ref]),count,ref)

    def test_c_merged_history_preserves_ids_continuations_and_original_cells(self):
        content=(base.TEMPLATES/'C.xlsm').read_bytes(); evidence=source_evidence(content,self.models['C'])
        records=[r for r in self.source_records if r['fields']['楼栋']=='C楼']
        corrected=[complete_source_record(r,evidence) or r for r in records]
        self.assertEqual([r['record_id'] for r in records],[r['record_id'] for r in corrected])
        ops=[from_feishu(r) for r in corrected]
        self.assertEqual(sum(len(o['meta'].get('continuations',[])) for o in ops),55)
        row=next(o for o in ops if o['source']=='机柜上下电时间统计' and o['source_row']==690)
        self.assertEqual(row['groups'][1]['action'],'上正式电')
        self.assertTrue(row['groups'][1]['actual'])
        original=Workbook(content); exported=Workbook(export_workbook(content,self.configs['C'],ops))
        for name in evidence:
            self.assertEqual([m.get('ref') for m in original.sheet(name).iter(T('mergeCell'))],[m.get('ref') for m in exported.sheet(name).iter(T('mergeCell'))])
        for ref in ('E32','G32','H32','J32','H689','J690'):
            self.assertEqual(original.value(original.cells('机柜上下电时间统计').get(ref)),exported.value(exported.cells('机柜上下电时间统计').get(ref)),ref)

    def test_carrier_counts_and_register_named_cabinet(self):
        overview=self.service.overview('B')
        rooms=[r for r in overview['rooms'] if r['carrier']]
        self.assertEqual(len(rooms),2)
        for room in rooms:
            self.assertEqual(room['counts']['off'],5)
            self.assertEqual(room['unlocated'],5)
            self.assertEqual(room['counts']['test'],1)
        payload={'operation_id':'carrier_new_12345678','room':'216','rack':'Z01','rack_type':'网络机柜','add_inventory':True,'groups':[]}
        self.service.save_operation('B',payload,'owner')
        after=self.service.overview('B'); room=next(r for r in after['rooms'] if r['id']=='216')
        self.assertEqual(room['unlocated'],4); self.assertEqual(room['counts']['off'],5)
        self.assertEqual(after['counts']['total'],1076)

    def row(self,scope='D'):
        return next(o for o in self.service.snapshot(scope)['operations'] if not o['empty'] and not o['issues'])

    def payload(self,old,**changes):
        return {'operation_id':'test_reliable_12345678','expected_version':old['version'],**changes}

    def wait_write(self,oid):
        for _ in range(200):
            status=self.service.write_status('D',oid,'owner')
            if status['status'] in ('completed','pending','conflict'): return status
            time.sleep(.025)
        self.fail('Upload did not finish')

    def test_deferred_save_receipt_read_isolation_and_duplicate_request(self):
        old=self.row(); payload=self.payload(old,power=12345)
        started=threading.Event(); release=threading.Event(); update=self.remote.update
        def held(rid,fields):
            started.set(); release.wait(5); return update(rid,fields)
        with patch.object(self.remote,'update',side_effect=held) as write:
            receipt=self.service.save_operation('D',payload,'owner',old['record_id'],defer=True)
            try:
                self.assertTrue(receipt['accepted']); self.assertTrue(started.wait(2))
                start=time.monotonic()
                same=self.service.save_operation('D',payload,'owner',old['record_id'],defer=True)
                self.assertLess(time.monotonic()-start,.5)
                self.assertEqual(same['operation_id'],receipt['operation_id'])
                self.assertEqual(next(o['power'] for o in self.service.snapshot('D')['operations'] if o['record_id']==old['record_id']),old['power'])
                self.assertEqual(self.service.write_status('D',payload['operation_id'],'owner',details=True)['request'],payload)
                with self.assertRaises(CabinetError): self.service.write_status('D',payload['operation_id'],'stranger',details=True)
            finally: release.set()
            self.assertEqual(self.wait_write(payload['operation_id'])['status'],'completed')
            self.assertEqual(write.call_count,1)
        self.assertEqual(next(o['power'] for o in self.service.snapshot('D')['operations'] if o['record_id']==old['record_id']),12345)

    def test_deferred_lost_response_retries_with_one_read_and_no_duplicate_write(self):
        old=self.row(); payload=self.payload(old,power=12345); update=self.remote.update
        def lose(rid,fields):
            update(rid,fields); raise TimeoutError('response lost')
        with patch.object(self.remote,'update',side_effect=lose):
            self.service.save_operation('D',payload,'owner',old['record_id'],defer=True)
            self.assertEqual(self.wait_write(payload['operation_id'])['status'],'pending')
        with patch.object(self.remote,'get',wraps=self.remote.get) as read, patch.object(self.remote,'update',wraps=update) as write:
            self.service.resume_write('D',payload['operation_id'],'owner',defer=True)
            self.assertEqual(self.wait_write(payload['operation_id'])['status'],'completed')
            self.assertEqual(write.call_count,0); self.assertEqual(read.call_count,1)

    def test_interrupted_upload_can_resume_and_export_does_not_block_upload(self):
        old=self.row(); payload=self.payload(old,power=12345)
        with patch.object(self.service._upload_pools['D'],'submit'):
            self.service.save_operation('D',payload,'owner',old['record_id'],defer=True)
        journal=self.service.local.document('D','write:'+payload['operation_id'])
        journal['worker_pid']=999999999; self.service.write('write:'+payload['operation_id'],journal)
        self.service._writing.clear()
        self.assertTrue(self.service.write_status('D',payload['operation_id'],'owner')['retryable'])
        started=threading.Event(); release=threading.Event()
        def export(): started.set(); release.wait(5)
        future=self.service._pools['D'].submit(export)
        try:
            self.assertTrue(started.wait(2))
            self.service.resume_write('D',payload['operation_id'],'owner',defer=True)
            self.assertEqual(self.wait_write(payload['operation_id'])['status'],'completed')
            self.assertFalse(future.done())
        finally: release.set(); future.result()

    def test_b03_uses_actual_completion_not_expected_or_column_position(self):
        detail=self.service.operations('D',{'room':'201','rack':'B03'})
        latest=detail['rack_state']['latest_success']
        self.assertEqual(detail['rack_state']['state'],'test')
        self.assertEqual(latest['action'],'正式电转测试电')
        self.assertTrue(latest['actual'].startswith('2026-08-19 20:00'))
        old=next(o for o in self.service.snapshot('D')['operations'] if o['room']=='201' and o['rack']=='B03')
        groups=copy.deepcopy(old['groups']); formal=next(g for g in groups if g['action']=='上正式电')
        formal['expected']='2026-08-30 20:00:00'
        saved=self.service.save_operation('D',self.payload(old,groups=groups),'owner',old['record_id'])
        self.assertEqual(self.service.operations('D',{'room':'201','rack':'B03'})['rack_state']['state'],'test')
        formal['actual']='2026-08-21 20:00:00'
        self.service.save_operation('D',{**self.payload(saved,groups=groups),'operation_id':'test_b03_next_12345678'},'owner',old['record_id'])
        self.assertEqual(self.service.operations('D',{'room':'201','rack':'B03'})['rack_state']['state'],'formal')

    def test_update_lost_response_resumes_without_duplicate_write(self):
        old=self.row(); payload=self.payload(old,power=12345); update=self.remote.update; calls=[]
        def lose(rid,fields):
            calls.append(rid); update(rid,fields); raise TimeoutError('response lost')
        self.remote.update=lose
        with self.assertRaises(TimeoutError): self.service.save_operation('D',payload,'owner',old['record_id'])
        self.remote.update=update
        saved=self.service.save_operation('D',payload,'owner',old['record_id'])
        self.assertEqual(saved['power'],12345); self.assertEqual(len(calls),1)
        self.assertEqual(self.service.pending_writes('D'),[])

    def test_create_failure_before_commit_can_retry_same_id(self):
        payload={'operation_id':'create_retry_123456789','room':'302','rack':'B03','rack_type':'网络机柜','groups':[{'action':'上正式电','actual':'2026-01-01 09:00:00','expected':'','result':'成功'}]}
        with patch.object(self.remote,'create',side_effect=ConnectionError('before write')):
            with self.assertRaises(ConnectionError): self.service.save_operation('A',payload,'owner')
        saved=self.service.save_operation('A',payload,'owner')
        self.assertEqual(self.remote.creates,1); self.assertEqual(saved['record_id'],'recNew1')

    def test_disk_failure_keeps_old_reads_and_recovers_after_restart(self):
        old=self.row(); payload=self.payload(old,power=12345)
        with patch.object(self.service.local,'commit_operation',side_effect=OSError('disk full')):
            with self.assertRaises(OSError): self.service.save_operation('D',payload,'owner',old['record_id'])
        self.assertEqual(next(o for o in self.service.snapshot('D')['operations'] if o['record_id']==old['record_id'])['power'],old['power'])
        other=CabinetPowerService(self.store,self.remote,self.tmp.name); other._directory=self.service._directory
        try:
            saved=other.resume_write('D',payload['operation_id'],'owner')
            self.assertEqual(saved['power'],12345)
            self.assertEqual(next(o for o in self.service.snapshot('D')['operations'] if o['record_id']==old['record_id'])['power'],12345)
        finally: other.shutdown()

    def test_recovery_does_not_publish_a_changed_verified_cloud_record(self):
        old=self.row(); payload=self.payload(old,power=12345)
        with patch.object(self.service.local,'commit_operation',side_effect=OSError('disk full')):
            with self.assertRaises(OSError): self.service.save_operation('D',payload,'owner',old['record_id'])
        self.remote.update(old['record_id'],{'机柜功率（W）':9999})
        with self.assertRaises(CabinetError): self.service.resume_write('D',payload['operation_id'],'owner')
        self.assertEqual(next(o['power'] for o in self.service.snapshot('D')['operations'] if o['record_id']==old['record_id']),old['power'])

    def test_cross_building_partial_commit_can_resume_from_source(self):
        old=self.row(); payload=self.payload(old,original_scope='D',room='302',rack='B03',source='机柜上电时间统计')
        commit=self.service.local.commit_operation
        def fail_target(scope,*args,**kwargs):
            if scope=='A': raise OSError('target unavailable')
            return commit(scope,*args,**kwargs)
        with patch.object(self.service.local,'commit_operation',side_effect=fail_target):
            with self.assertRaises(OSError): self.service.save_operation('A',payload,'owner',old['record_id'],can_move_scope=True)
        self.assertTrue(self.service.pending_writes('D'))
        self.assertTrue(self.service.pending_writes('A'))
        with self.assertRaises(CabinetError): self.service.resume_write('D',payload['operation_id'],'owner')
        self.service.resume_write('D',payload['operation_id'],'owner',admin=True)
        self.assertFalse(self.service.pending_writes('D')); self.assertFalse(self.service.pending_writes('A'))
        self.assertTrue(any(o['record_id']==old['record_id'] for o in self.service.snapshot('A')['operations']))

    def test_directory_partial_failure_resumes_both_tables(self):
        old=self.row(); kind='服务器机柜' if old['rack_type']=='网络机柜' else '网络机柜'; payload=self.payload(old,rack_type=kind)
        with patch.object(self.service._directory,'update',side_effect=TimeoutError('directory unavailable')):
            with self.assertRaises(TimeoutError): self.service.save_operation('D',payload,'owner',old['record_id'])
        self.service.resume_write('D',payload['operation_id'],'owner')
        rack=next(r for r in self.service.overview('D')['racks'] if (r['room'],r['rack'])==(old['room'],old['rack']))
        self.assertEqual(rack['rack_type'],kind)

    def test_clear_groups_does_not_restore_old_operations(self):
        old=self.row(); saved=self.service.save_operation('D',self.payload(old,groups=[]),'owner',old['record_id'])
        self.assertEqual(saved['groups'],[]); self.assertEqual(saved['events'],[])

    def test_failed_latest_operation_keeps_last_success(self):
        rack=next(r for r in self.service.overview('D')['racks'] if r['state']=='formal')
        old=next(o for o in self.service.snapshot('D')['operations'] if (o['room'],o['rack'])==(rack['room'],rack['rack']))
        groups=[{'id':'new_failed_event','action':'下正式电','actual':'2026-09-09 12:00:00','expected':'','result':'失败'},*old['groups']]
        self.service.save_operation('D',self.payload(old,groups=groups),'owner',old['record_id'])
        actual=next(r for r in self.service.overview('D')['racks'] if (r['room'],r['rack'])==(rack['room'],rack['rack']))
        self.assertEqual(actual['state'],'formal'); self.assertEqual(actual['last_operation'],rack['last_operation'])

    def test_duplicate_update_and_unknown_cabinet_rejected(self):
        old=self.row(); other=next(o for o in self.service.snapshot('D')['operations'] if o['room']==old['room'] and o['rack']!=old['rack'])
        for rack in (other['rack'],'Z99'):
            with self.assertRaises(CabinetError): self.service.save_operation('D',self.payload(old,rack=rack),'owner',old['record_id'])
        self.assertEqual(self.service.overview('D')['record_count'],988)

    def test_strict_new_values_and_mismatched_worksheet(self):
        old=self.row()
        for group in ({'action':'不要上正式电','actual':'2026-01-01 00:00:00'}, {'action':'上正式电\n下正式电','actual':'2026-01-01 00:00:00\n2026-01-02 00:00:00','expected':'2026-01-01 00:00:00'}):
            with self.assertRaises(CabinetError): self.service.validate_op('D',{'groups':[{**group,'result':'成功'}]},old)
        payload={'room':'302','rack':'B03','rack_type':'网络机柜','source':'机柜上电时间统计','groups':[{'action':'下正式电','actual':'2026-01-01 00:00:00','expected':'','result':'成功'}]}
        with self.assertRaises(CabinetError): self.service.validate_op('A',payload)

    def test_date_interval_matches_one_event_and_page_clamps(self):
        old=self.row('A'); changed=copy.deepcopy(old)
        changed['groups']=[{'id':'date_event1','result':'成功','action':'上正式电','actual':'2020-01-01 00:00:00','expected':''},{'id':'date_event2','result':'成功','action':'下正式电','actual':'2030-01-01 00:00:00','expected':''}]
        self.service._snapshot('A')['operations']=[{**from_feishu({'record_id':old['record_id'],'fields':{**old['raw_fields'],**to_fields(changed)}}),'ordinal':1,'display_sheet':old['source']}]
        self.assertEqual(self.service.operations('A',{'from':'2025-01-01','to':'2025-12-31'})['total'],0)
        self.assertEqual(self.service.operations('A',{'page':999})['page'],1)

    def test_cross_building_move_updates_unloaded_source(self):
        old=self.row('D'); other=CabinetPowerService(self.store,self.remote,self.tmp.name); other._directory=self.service._directory
        try:
            payload=self.payload(old,original_scope='D',source='机柜上电时间统计')
            other.save_operation('A',payload,'owner',old['record_id'],can_move_scope=True)
            self.assertFalse(any(o['record_id']==old['record_id'] for o in self.service.snapshot('D')['operations']))
            moved=next(o for o in self.service.snapshot('A')['operations'] if o['record_id']==old['record_id'])
            self.assertIsNone(moved['source_row']); self.assertIn('origin',moved['meta'])
            self.assertTrue(set(e['id'] for e in moved['events']).issubset(set(self.service.snapshot('A')['config']['baseline_event_ids'])))
        finally: other.shutdown()

    def test_cold_local_migrations_do_not_take_other_building_locks(self):
        import tempfile
        legacy=MemoryStore()
        for scope in 'AB': legacy.put_document('cabinet_power',SNAPSHOT_KEY+scope,{'snapshot':self.service.snapshot(scope)})
        with tempfile.TemporaryDirectory() as root:
            cold=CabinetPowerService(legacy,self.remote,root); cold._directory=self.service._directory
            try:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    results=list(pool.map(cold.overview,'AB'))
                self.assertEqual([r['record_count'] for r in results],[1031,1087])
            finally: cold.shutdown()

    def test_b_busy_jobs_do_not_block_d_or_readers(self):
        started=threading.Event(); release=threading.Event()
        def held(scope,payload,job): started.set(); release.wait(5); return {'scope':scope}
        with patch.object(self.service,'do_refresh',held), patch.object(self.service,'do_export',return_value={'ok':True}):
            self.service.job('B','refresh','owner',{}); self.assertTrue(started.wait(2))
            try:
                self.service.job('B','export','owner',{})
                job=self.service.job('D','export','owner',{})
                deadline=time.monotonic()+2
                while self.service.job_status(job['job_id'])['status']!='succeeded' and time.monotonic()<deadline: time.sleep(.02)
                self.assertEqual(self.service.job_status(job['job_id'])['status'],'succeeded')
                self.assertEqual(self.service.overview('A')['record_count'],1031)
            finally: release.set()

    def test_conflict_can_load_cloud_version_and_release_pending(self):
        old=self.row(); self.remote.records[old['record_id']]['fields']['机柜功率（W）']=9876
        with self.assertRaises(CabinetError): self.service.save_operation('D',self.payload(old,power=12345),'owner',old['record_id'])
        self.service.reconcile_write('D','test_reliable_12345678','owner')
        self.assertEqual(self.service.pending_writes('D'),[])
        self.assertEqual(next(o['power'] for o in self.service.snapshot('D')['operations'] if o['record_id']==old['record_id']),9876)

    def test_summary_formulas_baseline_increment_and_new_date_style(self):
        snap=self.service.snapshot('D'); old=next(o for o in snap['operations'] if o['room']=='201' and o['rack']=='A01')
        group={'id':'summary_new_event','action':'上正式电','actual':'2026-09-09 12:00:00','expected':'','result':'成功'}
        self.service.save_operation('D',self.payload(old,groups=[group,*old['groups']]),'owner',old['record_id'])
        snap=self.service.snapshot('D'); original=Path(snap['config']['path']).read_bytes(); before=Workbook(original)
        after=Workbook(export_workbook(original,snap['config'],snap['operations'])); name='机柜上电汇总表'; cells=after.cells(name)
        self.assertEqual(after.value(cells['C10']),674)
        self.assertEqual(after.value(cells['C10']),sum(after.value(cells['C'+str(i)]) for i in range(4,10)))
        self.assertIsNotNone(cells['C10'].find(T('f')))
        self.assertEqual(cells['C10'].find(T('f')).attrib,before.cells(name)['C10'].find(T('f')).attrib)
        self.assertEqual(after.value(after.cells('201-M2机柜平面图 ')['F39']),145)
        for ref,c in before.cells(name).items():
            if int(''.join(filter(str.isdigit,ref)))>=12: self.assertEqual(before.value(c),after.value(cells[ref]),ref)
        self.assertTrue(any(row.get(1)=='2026-09-09' and row.get(2)==1 for _,row in after.rows(name)))
        new=copy.deepcopy(snap['operations'][0]); new.update(record_id='recStyleNew',meta={},source_row=None)
        after=Workbook(export_workbook(original,snap['config'],[*snap['operations'],new]))
        sheet=new['source']; last=max(r for r,row in after.rows(sheet) if row.get(4)==new['rack'])
        self.assertNotEqual(after.cells(sheet)['G'+str(last)].get('s'),'0')


class CabinetStateOrderTests(unittest.TestCase):
    def test_only_latest_completed_success_controls_state(self):
        inventory=[{'room':'201','rack':'B03'}]
        def event(action,actual,result='成功'):
            return {'room':'201','rack':'B03','action':action,'actual':actual,'expected':'2030-01-01 00:00:00','result':result}
        def state(events): return calculate(inventory,events)['racks'][0]['state']
        formal=event('上正式电','2026-08-18 17:57:03')
        test=event('正式电转测试电','2026-08-19 20:00:38')
        self.assertEqual(state([test,formal]),'test')
        self.assertEqual(state([test,formal,event('下测试电','2026-08-21 20:00:00','失败'),event('上正式电','')]),'test')
        self.assertEqual(state([formal,event('上测试电',formal['actual'])]),'unknown')
        self.assertEqual(state([formal,event('测试电转正式电',formal['actual'])]),'formal')
        self.assertEqual(state([formal,event('上测试电',formal['actual']),test]),'test')
        self.assertEqual(state([event('上正式电','')]),'unknown')
        self.assertEqual(state([event('上正式电','2026-08-21 20:00:00','失败')]),'off')
        self.assertEqual(state([]),'off')


if __name__=='__main__': unittest.main()

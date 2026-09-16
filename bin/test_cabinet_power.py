import copy
import io
import json
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from .lan_bitable_template_portal.cabinet_power_data import source_rows, from_feishu, to_fields,source_evidence,complete_source_record
from .lan_bitable_template_portal.cabinet_power_excel import CabinetError, Workbook, T, calculate, export_workbook, dates, digest, map_state_baseline
from .lan_bitable_template_portal.cabinet_power import CabinetFeishu, CabinetPowerService, EXPORT_ARCHIVE_APP_TOKEN, EXPORT_ARCHIVE_FIELDS, EXPORT_ARCHIVE_TABLE_ID
TEMPLATES=Path(__file__).parent/"lan_bitable_template_portal/templates/cabinet_power"

class MemoryStore:
    db_path=Path("unused.sqlite")
    def __init__(self): self.documents={}
    def get_document(self,ns,key): return copy.deepcopy(self.documents.get((ns,key)))
    def put_document(self,ns,key,value): self.documents[(ns,key)]=copy.deepcopy(value)

class FakeFeishu:
    def __init__(self,records=()):
        self.records={r["record_id"]:copy.deepcopy(r) for r in records}; self.creates=0; self.fail_after_create=False; self.list_calls=0
    def list_all(self,path="records"): self.list_calls+=1; return copy.deepcopy(list(self.records.values()))
    def ensure_fields(self): return True
    def get(self,rid): return copy.deepcopy(self.records[rid])
    def create(self,fields,operation_id):
        self.creates+=1; rid="recNew"+str(self.creates)
        self.records[rid]={"record_id":rid,"fields":copy.deepcopy(fields)}
        if self.fail_after_create: raise TimeoutError("response lost")
        return self.get(rid)
    def update(self,rid,fields):
        self.records[rid]["fields"].update(copy.deepcopy(fields)); return self.get(rid)

class FakeExportFeishu:
    def __init__(self):
        self.fields={"自动编号":{"field_name":"自动编号","type":1005}}
        self.records={}; self.upload_calls=0; self.creates=0; self.fail_after_create=False; self.lock=threading.RLock()
    def list_all(self,path="records"):
        with self.lock: return copy.deepcopy(list(self.fields.values()) if path=="fields" else list(self.records.values()))
    def request(self,method,path,body=None,params=None):
        if method=="POST" and path=="fields":
            with self.lock:
                self.fields[body["field_name"]]={"field_name":body["field_name"],"type":body["type"],"field_id":"fld"+str(len(self.fields))}
                return {"field":copy.deepcopy(self.fields[body["field_name"]])}
        raise AssertionError((method,path,body,params))
    def upload_attachment(self,path,file_name):
        with self.lock: self.upload_calls+=1
        self.uploaded_path=Path(path); self.uploaded_name=file_name
        return "file-export-token"
    def create(self,fields,operation_id):
        with self.lock:
            self.creates+=1; rid="recExport"+str(self.creates)
            self.records[rid]={"record_id":rid,"fields":copy.deepcopy(fields),"operation_id":operation_id}
        if self.fail_after_create: raise TimeoutError("response lost")
        return self.get(rid)
    def update(self,rid,fields):
        with self.lock:
            self.records[rid]["fields"].update(copy.deepcopy(fields)); return self.get(rid)
    def get(self,rid):
        with self.lock: return copy.deepcopy(self.records[rid])

def fixtures(with_power_baseline=False):
    models={}; records=[]; directory=[]; configs={}
    for scope in "ABCDE":
        content=(TEMPLATES/(scope+".xlsm")).read_bytes(); model,rows=source_rows(content,scope); models[scope]=model
        records.extend({"record_id":"rec"+scope+str(i),"fields":r["fields"]} for i,r in enumerate(rows))
        if with_power_baseline and scope=='C':
            evidence=source_evidence(content,model)
            records=[(complete_source_record(r,evidence) or r) if r['fields']['楼栋']=='C楼' else r for r in records]
        book=Workbook(content)
        data={"hash":model["template_hash"],"formats":model["formats"],"summary_cells":{n:{ref:book.value(c) for ref,c in book.cells(n).items()} for n in book.sheets if "汇总" in n}}
        configs[scope]={**model,"template_data":data}
        map_values={n:{ref:book.value(c) for ref,c in book.cells(n).items()} for n in book.sheets if "平面图" in n}
        configs[scope]["map_values"]=map_values
        directory.append({"record_id":"mapvals"+scope,"fields":{"类别":"平面图数值","楼栋":scope+"楼","布局资料":json.dumps(map_values,ensure_ascii=False)}})
        directory.append({"record_id":"template"+scope,"fields":{"类别":"模板资料","楼栋":scope+"楼","布局资料":json.dumps(data,ensure_ascii=False)}})
        for room in model["rooms"]:
            geometry={k:v for k,v in room.items() if k!="layout"}
            if with_power_baseline:
                if 'power_baseline' not in configs[scope]: configs[scope]['power_baseline']=map_state_baseline(content,configs[scope],[from_feishu(r) for r in records if r['fields']['楼栋']==scope+'楼'])[0]
                geometry['power_baseline']={'version':1,'template_hash':model['template_hash'],'racks':{k.split('/')[1]:v for k,v in configs[scope]['power_baseline'].items() if k.startswith(room['id']+'/')}}
            directory.append({"record_id":"room"+scope+room["id"],"fields":{"类别":"房间","楼栋":scope+"楼","包间":room["id"],"名称":room["name"],"数量":room["total"],"布局资料":json.dumps(geometry,ensure_ascii=False)}})
        for rack in model["inventory"]:
            directory.append({"record_id":"rack"+scope+rack["room"]+rack["rack"],"fields":{"类别":"机柜","楼栋":scope+"楼","包间":rack["room"],"名称":rack["rack"],"机柜类型":rack["rack_type"],"布局资料":json.dumps({"positions":rack["positions"],"template_color":rack["template_color"]},ensure_ascii=False)}})
    return models,records,directory,configs

class CabinetPowerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.models,cls.source_records,cls.directory_records,cls.configs=fixtures()

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.store=MemoryStore()
        self.store.db_path=Path(self.tmp.name)/"state.sqlite"
        self.remote=FakeFeishu(self.source_records); self.service=CabinetPowerService(self.store,self.remote,self.tmp.name)
        self.service._directory=FakeFeishu(self.directory_records)
        self.service.do_refresh("",{}, {})

    def tearDown(self):
        self.service.shutdown(); self.tmp.cleanup()

    def test_create_uses_stable_uuid4_client_token_helper(self):
        self.assertIn("_stable_uuid4_client_token",CabinetFeishu.create.__code__.co_names)
        remote=CabinetFeishu(); captured={}
        def request(method,path,body=None,params=None): captured.update(method=method,path=path,body=body,params=params); return {"record":{"record_id":"recToken","fields":body["fields"]}}
        remote.request=request
        remote.create({"机架":"A01"},"manual_operation_123456")
        token=captured["params"]["client_token"]
        self.assertEqual((captured["method"],captured["path"],uuid.UUID(token).version),("POST","records",4))

    def test_export_attachment_uses_archive_base_parent(self):
        path=Path(self.tmp.name)/"sample.xlsm"; path.write_bytes(b"export")
        remote=CabinetFeishu(EXPORT_ARCHIVE_TABLE_ID,EXPORT_ARCHIVE_APP_TOKEN); captured={}
        remote.require_write=lambda:None; remote.token=lambda:"tenant-token"
        remote._http=SimpleNamespace(request_file_json=lambda *args,**kwargs:(captured.update(kwargs) or {"code":0,"data":{"file_token":"file-token"}}))
        self.assertEqual(remote.upload_attachment(path,path.name),"file-token")
        self.assertEqual(captured["data"]["parent_node"],EXPORT_ARCHIVE_APP_TOKEN)
        self.assertEqual(captured["data"]["parent_type"],"bitable_file")

    def test_editor_hides_blank_template_groups_and_names_real_transition(self):
        source=(Path(__file__).parent/"lan_bitable_template_portal/frontend/src/components/CabinetPowerPage.vue").read_text(encoding="utf-8")
        self.assertIn("map(() => newGroup(false))",source)
        self.assertIn("editorGroupLabel(group, i)",source)
        self.assertIn("stateAction(target)",source)
        self.assertIn("!!form.target_state && group._editing",source)

    def test_export_ui_supports_cloud_retry_and_parallel_all_buildings(self):
        source=(Path(__file__).parent/"lan_bitable_template_portal/frontend/src/components/CabinetPowerPage.vue").read_text(encoding="utf-8")
        self.assertIn("一键导出/上传所有楼栋",source)
        self.assertIn("await Promise.all(allExportItems.value.map",source)
        self.assertIn("retryExportUpload",source)
        self.assertIn("exports/' + record.export_id + '/upload",source)

    def test_all_source_rows_and_idle_cabinets_are_retained(self):
        counts={s:sum(r["fields"]["楼栋"]==s+"楼" for r in self.source_records) for s in "ABCDE"}
        self.assertEqual(counts,{"A":1031,"B":1087,"C":1220,"D":988,"E":1272})
        for s,n in (("D",221),("E",870)):
            self.assertEqual(sum(from_feishu(r)["empty"] for r in self.source_records if r["fields"]["楼栋"]==s+"楼"),n)
        self.assertEqual(len({r["fields"]["数据标识"] for r in self.source_records}),5598)

    def test_incomplete_history_remains_lossless(self):
        row=next(r for r in self.source_records if r["fields"]["楼栋"]=="C楼" and r["fields"]["来源工作表"]=="机柜上下电时间统计" and r["fields"]["来源行号"]==690)
        op=from_feishu(row)
        self.assertTrue(op["issues"])
        self.assertEqual(op["meta"]["cells"]["10"],44451.7603240741)
        self.assertTrue(any(g["actual"] and not g["action"] for g in op["groups"]))

    def test_template_history_and_visible_feishu_edits(self):
        row=next(r for r in self.source_records if r["fields"]["楼栋"]=="E楼" and r["fields"]["来源行号"]==2)
        op=from_feishu(row); self.assertEqual(len(op["events"]),3)
        row=copy.deepcopy(row); row["fields"]["操作类型"]="下正式电"; row["fields"]["实际完成时间"]=1788220800000
        changed=from_feishu(row); self.assertEqual(changed["groups"][0]["action"],"下正式电")
        self.assertEqual(changed["events"][0]["actual"][:10],"2026-09-01")
        self.assertEqual(changed["groups"][1:],op["groups"][1:])

    def test_only_remote_business_data_is_used(self):
        self.store.put_document("cabinet_power","config:A",{"inventory":[],"history_ready":False})
        overview=self.service.overview("D")
        self.assertEqual(overview["source"],"local"); self.assertEqual(overview["record_count"],988)
        self.assertEqual(overview["inventory_only"],221)
        self.assertEqual(overview["counts"]["unknown"],0)
        rid=next(r["record_id"] for r in self.source_records if r["fields"]["楼栋"]=="D楼")
        self.remote.records.pop(rid); self.service.do_refresh("D",{}, {})
        self.assertEqual(self.service.overview("D")["record_count"],987)

    def test_local_snapshot_opens_offline_without_background_sync(self):
        self.service.overview("D")
        for scope in "ABCDE":
            saved=self.service.local.load(scope)
            self.assertEqual(saved["config"]["scope"],scope)
        self.assertIsNot(self.service._scope_locks["A"],self.service._scope_locks["B"])
        remote=FakeFeishu(self.source_records)
        remote.list_all=lambda path="records": (_ for _ in ()).throw(TimeoutError("offline"))
        cached=CabinetPowerService(self.store,remote,self.tmp.name)
        cached._directory=self.service._directory
        started=time.perf_counter(); overview=cached.overview("D")
        self.assertLess(time.perf_counter()-started,1)
        self.assertEqual(overview["record_count"],988)
        cached.pool.shutdown(wait=True)

    def test_first_missing_building_initializes_all_buildings_from_feishu(self):
        fresh=CabinetPowerService(MemoryStore(),FakeFeishu(self.source_records),Path(self.tmp.name)/"fresh")
        fresh._directory=FakeFeishu(self.directory_records)
        try:
            overview=fresh.overview("D")
            self.assertEqual(overview["record_count"],988)
            self.assertTrue(all(fresh.local.version(scope) for scope in "ABCDE"))
            self.assertEqual(fresh.remote.list_calls,1)
            self.assertEqual(fresh._directory.list_calls,1)
        finally:
            fresh.shutdown()

    def test_first_bootstrap_reports_all_building_progress(self):
        fresh=CabinetPowerService(MemoryStore(),FakeFeishu(),Path(self.tmp.name)/"progress")
        fresh._directory=FakeFeishu()
        release=threading.Event()
        def refresh(scope,payload,job):
            release.wait(5)
            fresh.local.replace(scope,{"scope":scope,"rooms":[],"inventory":[]},[],[])
            return {"count":0}
        fresh.do_refresh=refresh
        try:
            started=fresh.bootstrap("owner",start=True)
            self.assertEqual(started["total"],5)
            self.assertEqual(started["status"],"running")
            self.assertTrue(all(item["status"] in ("pending","running") for item in started["buildings"]))
            release.set()
            deadline=time.time()+10
            while time.time()<deadline and fresh.bootstrap("owner")["status"]=="running": time.sleep(.02)
            completed=fresh.bootstrap("owner")
            self.assertEqual((completed["status"],completed["ready"]),("succeeded",5))
        finally:
            release.set(); fresh.shutdown()

    def test_bootstrap_retries_only_failed_buildings(self):
        fresh=CabinetPowerService(MemoryStore(),FakeFeishu(),Path(self.tmp.name)/"retry")
        fresh._directory=FakeFeishu()
        attempts={scope:0 for scope in "ABCDE"}
        def refresh(scope,payload,job):
            attempts[scope]+=1
            if scope!="A" and attempts[scope]==1: raise TimeoutError("read timed out")
            fresh.local.replace(scope,{"scope":scope,"rooms":[],"inventory":[]},[],[])
            return {"count":0}
        fresh.do_refresh=refresh
        try:
            fresh.bootstrap("owner",start=True)
            deadline=time.time()+10
            while time.time()<deadline and fresh.bootstrap("owner")["status"]=="running": time.sleep(.02)
            self.assertEqual((fresh.bootstrap("owner")["ready"],fresh.bootstrap("owner")["failed"]),(1,4))
            fresh.bootstrap("owner",start=True)
            self.assertEqual(attempts,{scope:1 for scope in "ABCDE"})
            fresh.bootstrap("owner",start=True,retry_failed=True)
            deadline=time.time()+10
            while time.time()<deadline and fresh.bootstrap("owner")["status"]=="running": time.sleep(.02)
            self.assertEqual(attempts,{"A":1,"B":2,"C":2,"D":2,"E":2})
        finally: fresh.shutdown()

    def test_layout_does_not_open_xlsm_at_runtime(self):
        self.service.overview("D")
        layout=self.service.layout("D","201")
        self.assertEqual(layout["layout"]["sheet"],layout["room"]["sheet"])
        self.assertTrue(layout["layout"]["cells"])

    def test_down_filter_includes_embedded_history_without_duplicate_records(self):
        down=self.service.operations("D",{"direction":"down","page_size":100})
        self.assertGreater(down["total"],0)
        self.assertTrue(all(any(e["action"].startswith("下") for e in r["events"]) for r in down["items"]))
        self.assertTrue(any(not r["action"].startswith("下") for r in down["items"]))
        self.assertEqual(len({r["record_id"] for r in down["items"]}),len(down["items"]))
        self.assertEqual(self.service.operations("E",{"direction":"empty"})["total"],870)

    def test_pc_table_columns_follow_each_building_workbook(self):
        for scope,lengths in (("A",[12,15]),("B",[12,13]),("C",[13,13]),("D",[31]),("E",[22])):
            formats=self.service.overview(scope)["sheet_formats"]
            self.assertEqual([len(f["columns"]) for f in formats],lengths)
            for fmt in formats:
                self.assertEqual([c["column"] for c in fmt["columns"]],list(range(1,len(fmt["columns"])+1)))
                result=self.service.operations(scope,{"sheet":fmt["sheet"],"page_size":100})
                self.assertEqual(result["total"],fmt["count"])
                self.assertTrue(all(o["display_sheet"]==fmt["sheet"] for o in result["items"]))

    def test_save_edits_groups_and_checks_versions(self):
        old=from_feishu(next(r for r in self.source_records if r["fields"]["楼栋"]=="E楼" and r["fields"]["来源行号"]==2))
        payload={"groups":copy.deepcopy(old["groups"]),"operation_id":"edit_test_123456789","expected_version":old["version"]}
        payload["groups"][0]["actual"]="2026-08-01 09:00:00"
        saved=self.service.save_operation("E",payload,"owner",old["record_id"])
        self.assertEqual(saved["groups"][0]["actual"],"2026-08-01 09:00:00")
        self.assertEqual(saved["groups"][1:],old["groups"][1:])
        with self.assertRaises(CabinetError):
            self.service.save_operation("E",{**payload,"operation_id":"edit_stale_12345678"},"owner",old["record_id"])

    def test_create_idempotence_response_loss(self):
        self.remote.fail_after_create=True
        payload={"room":"302","rack":"B03","rack_type":"网络机柜","power":16000,"result":"成功","groups":[{"action":"上正式电","actual":"2026-08-01 09:00:00","expected":""}],"operation_id":"create_test_123456789"}
        with self.assertRaises(TimeoutError): self.service.save_operation("A",payload,"owner")
        self.remote.fail_after_create=False
        saved=self.service.save_operation("A",payload,"owner")
        self.assertEqual(saved["record_id"],"recNew1"); self.assertEqual(self.remote.creates,1)
        self.assertEqual(self.service.save_operation("A",payload,"owner")["record_id"],"recNew1")
        with self.assertRaises(CabinetError): self.service.save_operation("A",payload,"someone_else")

    def test_invalid_new_history_rejected_before_write(self):
        payload={"room":"302","rack":"B03","rack_type":"网络机柜","result":"成功","groups":[{"action":"上正式电","actual":"","expected":""}],"operation_id":"invalid_test_123456"}
        with self.assertRaises(CabinetError): self.service.save_operation("A",payload,"owner")
        self.assertEqual(self.remote.creates,0)

    def test_de_cabinet_cannot_be_created_twice(self):
        payload={"room":"201","rack":"A01","rack_type":"网络机柜","result":"成功","groups":[{"action":"上正式电","actual":"2026-09-01 09:00:00","expected":""}],"operation_id":"duplicate_de_123456"}
        with self.assertRaises(CabinetError) as caught: self.service.save_operation("D",payload,"owner")
        self.assertEqual(caught.exception.status_code,409)
        self.assertEqual(self.remote.creates,0)

    def test_power_notice_batches_parse_samples_and_infer_only_from_prior_history(self):
        samples=[
            {
                "job_id":"job-notice-d","target_record_id":"rec-notice-d","notice_type":"上电通告","scope":"D",
                "title":"EA118机房D楼机柜上电通告","start_time":"2026-09-01 09:55","end_time":"2026-09-01 23:59","quantity":"40",
                "cabinet":"D-201包间B02、B04、B05、B07、B08、B10、B11、B13、B14、B16、C11、C13、C14、C16、D12、D14、D15、D17、E11、E13、E14、E16、F11、F13、F14、F16、G11、G13、G14、G16、H11、H13、H14、H16、I11、I13、I14、I16、J14、J16",
            },
            {
                "job_id":"job-notice-b-up","target_record_id":"rec-notice-b-up","notice_type":"上电通告","scope":"B",
                "title":"EA118机房B楼机柜上电通告","start_time":"2026-09-03 11:05","end_time":"2026-09-03 23:59","quantity":"4",
                "cabinet":"B-216运营商机房B04、B05，B-247运营商机房B04、B05",
            },
            {
                "job_id":"job-notice-b-down","target_record_id":"rec-notice-b-down","notice_type":"下电通告","scope":"B",
                "title":"EA118机房B楼机柜下电通告","start_time":"2026-09-03 14:55","end_time":"2026-09-03 19:00","quantity":"12",
                "cabinet":"B-402包间B15、B16、B17、B18，C13、C14、C15、C16，D15、D16、D17、D18",
            },
        ]
        batches=[self.service.batches.create_from_notice({**item,"owner_id":"owner"}) for item in samples]
        self.assertEqual([batch["stats"]["total"] for batch in batches],[40,4,12])
        self.assertEqual({(row["scope"],row["room"]) for row in batches[0]["rows"]},{("D","201")})
        self.assertEqual({(row["room"],row["rack_type"]) for row in batches[1]["rows"]},{("216","网络机柜"),("247","网络机柜")})
        self.assertEqual({row["action"] for row in batches[0]["rows"]},{""})
        self.assertEqual({row["action"] for row in batches[1]["rows"]},{""})
        self.assertEqual({row["action"] for row in batches[2]["rows"]},{"下测试电"})
        self.assertEqual({row["expected"] for row in batches[2]["rows"]},{"2026-09-03 19:00:00"})
        self.assertTrue(all(not row["actual"] and not row["result"] for batch in batches for row in batch["rows"]))
        repeated=self.service.batches.create_from_notice({**samples[0],"owner_id":"owner"})
        self.assertEqual(repeated["batch_id"],batches[0]["batch_id"])

    def test_power_notice_count_warning_requires_fresh_acknowledgement(self):
        batch=self.service.batches.create_from_notice({
            "job_id":"job-notice-warning","target_record_id":"rec-notice-warning","notice_type":"上电通告","scope":"B",
            "title":"EA118机房B楼机柜上电通告","start_time":"2026-09-03 11:05","end_time":"2026-09-03 23:59","quantity":"3",
            "cabinet":"B-216运营商机房B04、B05","owner_id":"owner",
        })
        self.assertIn("quantity_mismatch",{item["code"] for item in batch["blocking_warnings"]})
        acknowledged=self.service.batches.update(batch["batch_id"],{
            "version":batch["version"],"rows":[],"acknowledge_warnings":True,
        },"owner",["B"])
        self.assertTrue(acknowledged["warnings_acknowledged"])
        row=acknowledged["rows"][0]
        changed=self.service.batches.update(batch["batch_id"],{
            "version":acknowledged["version"],"rows":[{"row_id":row["row_id"],"rack":"B06"}],
        },"owner",["B"])
        self.assertFalse(changed["warnings_acknowledged"])

    def test_power_notice_outbox_handoff_never_raises_into_notice_flow(self):
        bin_path=str(Path(__file__).parent)
        added_path=bin_path not in sys.path
        if added_path: sys.path.insert(0,bin_path)
        from .lan_bitable_template_portal.server import PortalRuntime
        original=PortalRuntime.state_store
        class BrokenStore:
            def enqueue_outbox_event(self,*_args,**_kwargs): raise TimeoutError("locked")
        PortalRuntime.state_store=BrokenStore()
        try:
            event_id=PortalRuntime.enqueue_cabinet_notice_batch({
                "work_type":"power","action":"start","notice_type":"上电通告","scope":"B",
                "title":"test","start_time":"2026-09-03 11:05","end_time":"2026-09-03 23:59",
                "cabinet":"B-216运营商机房B04","quantity":"1",
            },job_id="job-safe",target_record_id="rec-safe",request_payload={"_auth_open_id":"owner"})
            self.assertEqual(event_id,0)
        finally:
            PortalRuntime.state_store=original
            if added_path: sys.path.remove(bin_path)

    def test_power_notice_outbox_creates_one_idempotent_batch(self):
        bin_path=str(Path(__file__).parent)
        added_path=bin_path not in sys.path
        if added_path: sys.path.insert(0,bin_path)
        from .lan_bitable_template_portal.server import PortalRuntime
        from .lan_bitable_template_portal.state_store import LanPortalStateStore
        original_store=PortalRuntime.state_store
        original_service=PortalRuntime.cabinet_power_service
        state=LanPortalStateStore(Path(self.tmp.name)/"notice-outbox.sqlite3")
        PortalRuntime.state_store=state
        PortalRuntime.cabinet_power_service=None
        try:
            prepared={
                "work_type":"power","action":"start","notice_type":"下电通告","scope":"B",
                "title":"EA118机房B楼机柜下电通告","start_time":"2026-09-03 14:55","end_time":"2026-09-03 19:00",
                "cabinet":"B-402包间B15、B16","quantity":"2",
            }
            first=PortalRuntime.enqueue_cabinet_notice_batch(prepared,job_id="job-outbox",target_record_id="rec-outbox",request_payload={"_auth_open_id":"owner"})
            second=PortalRuntime.enqueue_cabinet_notice_batch(prepared,job_id="job-outbox",target_record_id="rec-outbox",request_payload={"_auth_open_id":"owner"})
            self.assertEqual(first,second)
            PortalRuntime.cabinet_power_service=self.service
            result=PortalRuntime._process_cabinet_notice_queue_once()
            self.assertEqual(result["status"],"success")
            self.assertEqual(self.service.batches.get(result["batch_id"])["stats"]["total"],2)
            self.assertFalse(PortalRuntime._process_cabinet_notice_queue_once()["processed"])
        finally:
            PortalRuntime.stop_cabinet_notice_worker()
            PortalRuntime.state_store=original_store
            PortalRuntime.cabinet_power_service=original_service
            state.shutdown_write_worker(timeout=1)
            if added_path: sys.path.remove(bin_path)

    def test_batch_pdf_overlap_is_local_until_confirmed(self):
        operation=next(
            op for record in self.source_records
            if record["fields"]["楼栋"]=="A楼"
            for op in [from_feishu(record)]
            if any(event["result"]=="成功" and event["actual"]<="2026-09-15 00:00:00" for event in op["events"])
        )
        event=next(event for event in operation["events"] if event["result"]=="成功" and event["actual"]<="2026-09-15 00:00:00")
        rack_type=next(item["rack_type"] for item in self.configs["A"]["inventory"] if (item["room"],item["rack"])==(operation["room"],operation["rack"]))
        text=f"""机柜{event['action']}确认单
申请时间： 2026-09-14 10:00:00
申请单号： [Z260914001234567890] 操作类型： {event['action']}
EA118  A{operation['room'][0]}-{int(operation['room'][1:])}.EA118  {operation['rack']}  {operation['rack']}  {rack_type}  2UR  {event['actual']}  成功  {event['actual']}
"""
        class Page:
            def extract_text(self,**_kwargs): return text
        class Reader:
            is_encrypted=False; pages=[Page()]
        self.service.batches._pdf_reader=lambda _path:Reader()
        before=self.service.overview("A")["record_count"]
        batch=self.service.batches.recognize([("sample.pdf",b"%PDF-test")],"owner")
        deadline=time.time()+5
        while time.time()<deadline and (batch:=self.service.batches.get(batch["batch_id"]))["status"]=="recognizing": time.sleep(.01)
        self.assertEqual(batch["stats"]["duplicate"],1)
        repeated=self.service.batches.recognize([("renamed.pdf",b"%PDF-test")],"owner")
        self.assertEqual(repeated["batch_id"],batch["batch_id"])
        cleared=self.service.batches.clear_overlaps(batch["batch_id"],batch["version"],"owner",["A"])
        self.assertEqual(cleared["rows"][0]["status"],"excluded_duplicate")
        self.assertEqual(self.service.overview("A")["record_count"],before)
        cancelled=self.service.batches.cancel(batch["batch_id"],"owner")
        self.assertEqual(cancelled["status"],"cancelled")
        cleaned=self.service.batches.cleanup_file(batch["batch_id"],cancelled["files"][0]["file_id"],"owner")
        self.assertTrue(cleaned["files"][0]["cleaned_at"])

    def test_batch_confirm_creates_abc_and_appends_de(self):
        rows=[]
        for scope in ("A","D"):
            rack=self.configs[scope]["inventory"][0]
            rows.append({"scope":scope,"room":rack["room"],"rack":rack["rack"],"rack_type":rack["rack_type"],
                         "action":"上正式电","expected":"2026-09-14 01:02:03","actual":"2026-09-14 01:02:03","result":"成功"})
        before={scope:self.service.overview(scope)["record_count"] for scope in ("A","D")}
        batch=self.service.batches.create_manual(rows,"owner")
        with self.assertRaises(CabinetError) as denied:
            self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A"])
        self.assertEqual(denied.exception.status_code,403)
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A","D"])
        deadline=time.time()+10
        while time.time()<deadline:
            batch=self.service.batches.get(batch["batch_id"])
            if batch["status"]!="running": break
            time.sleep(.01)
        self.assertEqual(batch["stats"]["completed"],2)
        self.assertEqual(self.service.overview("A")["record_count"],before["A"]+1)
        self.assertEqual(self.service.overview("D")["record_count"],before["D"])
        self.assertTrue(any(event["actual"]=="2026-09-14 01:02:03" for op in self.service._snapshot("D")["operations"] for event in op["events"]))
        for scope in ("A","D"):
            record_id=next(row["record_id"] for row in batch["rows"] if row["scope"]==scope)
            saved=next(op for op in self.service._snapshot(scope)["operations"] if op["record_id"]==record_id)
            self.assertTrue(any(item["batch_id"]==batch["batch_id"] for item in saved["meta"]["batch_rows"]))

    def test_de_newest_group_stays_in_current_operation_columns(self):
        old=from_feishu(next(r for r in self.source_records if r["fields"]["楼栋"]=="E楼" and r["fields"]["来源行号"]==2))
        groups=old["groups"]+[{"action":"下正式电","actual":"2026-09-09 10:00:00","expected":""}]
        edited=self.service.validate_op("E",{"groups":groups},old)
        self.assertEqual(edited["groups"][0]["action"],"下正式电")
        self.assertEqual(to_fields(edited)["操作类型"],"下正式电")

    def test_original_export_preserves_all_five_formats_and_raw_rows(self):
        from openpyxl import load_workbook
        for scope in "ABCDE":
            original=(TEMPLATES/(scope+".xlsm")).read_bytes(); before=Workbook(original)
            ops=[from_feishu(r) for r in self.source_records if r["fields"]["楼栋"]==scope+"楼"]
            result=export_workbook(original,self.configs[scope],ops); after=Workbook(result)
            self.assertEqual(list(before.sheets),list(after.sheets))
            self.assertEqual(before.archive.read("xl/vbaProject.bin"),after.archive.read("xl/vbaProject.bin"))
            for name in before.sheets:
                original_merges=[x.get("ref") for x in before.sheet(name).iter(T("mergeCell"))]
                exported_merges=[x.get("ref") for x in after.sheet(name).iter(T("mergeCell"))]
                self.assertTrue(set(original_merges)<=set(exported_merges),name)
                if "平面图" not in name: self.assertEqual(original_merges,exported_merges)
            for fmt in self.models[scope]["formats"]:
                name=fmt["sheet"]; raw=dict(before.rows(name)); actual=dict(after.rows(name))
                for rn,row in raw.items():
                    if rn<=fmt["header"] or not row.get(fmt["rack"]): continue
                    for col,value in row.items():
                        self.assertEqual(actual[rn].get(col,""),value,(scope,name,rn,col))
            checked=load_workbook(io.BytesIO(result),keep_vba=True,data_only=True); checked.close()

    def test_export_uses_local_snapshot_and_all_buildings_can_run_together(self):
        self.service.overview("D")
        self.remote.list_all=lambda path="records": (_ for _ in ()).throw(AssertionError("export must not refresh Feishu"))
        result=self.service.do_export("D",{},{}); saved=self.service.read("export:"+result["export_id"])
        self.assertTrue(Path(saved["path"]).is_file())

        barrier=threading.Barrier(5)
        self.service.do_export=lambda scope,payload,job: {"scope":scope,"passed":barrier.wait(timeout=15)}
        jobs=[self.service.job(scope,"export","owner",{}) for scope in "ABCDE"]
        deadline=time.time()+20
        while time.time()<deadline and any(self.service.job_status(job["job_id"])["status"] not in ("succeeded","failed") for job in jobs): time.sleep(.01)
        states=[self.service.job_status(job["job_id"])["status"] for job in jobs]
        self.assertEqual(states,["succeeded"]*5,[self.service.job_status(job['job_id']) for job in jobs])

    def test_export_archive_schema_and_response_loss_retry_are_idempotent(self):
        archive=FakeExportFeishu(); archive.fail_after_create=True
        self.service.export_remote=archive; self.service._export_schema_ready=False
        result=self.service.do_export("D",{"batch_id":"all_12345678"},{"owner":"owner-open-id"})
        self.assertEqual(result["cloud_upload_status"],"failed")
        self.assertTrue(Path(self.service.read("export:"+result["export_id"])["path"]).is_file())
        self.assertEqual((archive.upload_calls,archive.creates,len(archive.records)),(1,1,1))
        self.assertTrue(set(EXPORT_ARCHIVE_FIELDS)<=set(archive.fields))
        archive.fail_after_create=False
        retried=self.service.upload_export("D",result["export_id"],"owner-open-id")
        self.assertEqual(retried["cloud_upload_status"],"succeeded")
        self.assertEqual((archive.upload_calls,archive.creates,len(archive.records)),(1,1,1))
        fields=next(iter(archive.records.values()))["fields"]
        self.assertEqual((fields["楼栋"],fields["批次标识"],fields["导出人"]),("D楼","all_12345678","owner-open-id"))
        self.assertEqual(fields["导出文件"],[{"file_token":"file-export-token"}])

    def test_export_remote_edit_overrides_original_cells_and_keeps_conversion(self):
        original=(TEMPLATES/"E.xlsm").read_bytes()
        rows=[from_feishu(r) for r in self.source_records if r["fields"]["楼栋"]=="E楼"]
        row=next(o for o in rows if o["source_row"]==2)
        row["groups"][0]["actual"]="2026-08-01 09:00:00"; row["groups"][0]["action"]="下正式电"
        row=from_feishu({"record_id":row["record_id"],"fields":{**row["raw_fields"],**to_fields(row)}})
        rows=[row if o["record_id"]==row["record_id"] else o for o in rows]
        exported=Workbook(export_workbook(original,self.configs["E"],rows))
        cells=exported.cells("机柜上下电时间统计")
        self.assertEqual(exported.value(cells["E2"]),"下正式电")
        self.assertEqual(dates(exported.value(cells["G2"])),["2026-08-01 09:00:00"])
        self.assertEqual(exported.value(cells["K2"]),"上正式电")
        self.assertEqual(exported.value(cells["N2"]),"正式电转测试电")

    def test_removed_import_routes_and_scope_permissions(self):
        from fastapi import FastAPI
        from fastapi.responses import JSONResponse
        from fastapi.testclient import TestClient
        from .lan_bitable_template_portal.cabinet_power_routes import install_cabinet_power_routes
        class Controller:
            def _current_session(self,request): return {"open_id":"test"} if request.headers.get("x-test-login") else None
            def _auth_required_response(self): return JSONResponse({},status_code=401)
            def _json_ok(self,request,session,data): return JSONResponse({"ok":True,"data":data})
            def _portal_error_response(self,exc,default_status): return JSONResponse({"error":str(exc)},status_code=default_status)
            async def _read_json_request(self,request,max_bytes): return await request.json()
        app=FastAPI(); controller=Controller()
        runtime=SimpleNamespace(state_store=self.store,auth_manager=SimpleNamespace(is_admin=lambda s:False,scope_allowed=lambda s,scope:scope=="A"))
        install_cabinet_power_routes(app,controller,runtime)
        self.assertIn("/api/cabinet-power/exports/{export_id}/upload",{route.path for route in app.routes})
        controller._cabinet_power.remote=self.remote; controller._cabinet_power._directory=self.service._directory
        controller._cabinet_power.root=Path(self.tmp.name)
        controller._cabinet_power.local=self.service.local
        with TestClient(app) as client:
            self.assertEqual(client.get("/api/cabinet-power/buildings").status_code,401)
            self.assertEqual(client.get("/api/cabinet-power/overview?scope=B",headers={"x-test-login":"1"}).status_code,403)
            self.assertEqual(client.post("/api/cabinet-power/imports/commit",headers={"x-test-login":"1"}).status_code,404)
            bootstrap=client.post("/api/cabinet-power/bootstrap",headers={"x-test-login":"1"},json={"scope":"A"}).json()["data"]
            self.assertEqual((bootstrap["status"],bootstrap["ready"]),("succeeded",5))
            data=client.get("/api/cabinet-power/overview?scope=A",headers={"x-test-login":"1"}).json()["data"]
            self.assertEqual(data["record_count"],1031)
            self.assertEqual(client.get("/api/cabinet-power/batches",headers={"x-test-login":"1"}).status_code,200)
            rack=self.configs["A"]["inventory"][0]
            created=client.post("/api/cabinet-power/batches",headers={"x-test-login":"1"},json={"rows":[{"scope":"A","room":rack["room"],"rack":rack["rack"],"rack_type":rack["rack_type"],"action":"上正式电","expected":"2026-09-14 12:00:00","actual":"2026-09-14 12:00:00","result":"成功"}]}).json()["data"]
            self.assertEqual(created["stats"]["total"],1)
            self.assertEqual(client.get("/api/cabinet-power/batches/"+created["batch_id"],headers={"x-test-login":"1"}).status_code,200)
            text=f"机柜上测试电确认单\n申请时间： 2026-09-14 10:00:00\n申请单号：[Z260914001234567890] 操作类型： 上测试电\nEA118  A{rack['room'][0]}-{int(rack['room'][1:])}.EA118  {rack['rack']}  {rack['rack']}  {rack['rack_type']}  2UR  2026-09-14 10:00:00  成功  2026-09-14 09:59:00"
            class Page:
                def extract_text(self,**_kwargs): return text
            controller._cabinet_power.batches._pdf_reader=lambda _path:SimpleNamespace(is_encrypted=False,pages=[Page()])
            recognized=client.post("/api/cabinet-power/batches/recognize",headers={"x-test-login":"1"},files=[("files",("sample.pdf",b"%PDF-route","application/pdf"))])
            self.assertEqual(recognized.status_code,202)
            recognized_id=recognized.json()["data"]["batch_id"]
            deadline=time.time()+5
            while time.time()<deadline:
                detail=client.get("/api/cabinet-power/batches/"+recognized_id,headers={"x-test-login":"1"}).json()["data"]
                if detail["status"]!="recognizing": break
                time.sleep(.01)
            self.assertEqual(detail["stats"]["total"],1)

if __name__=="__main__": unittest.main()

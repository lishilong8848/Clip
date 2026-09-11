import copy
import io
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from .lan_bitable_template_portal.cabinet_power_data import source_rows, from_feishu, to_fields,source_evidence,complete_source_record
from .lan_bitable_template_portal.cabinet_power_excel import CabinetError, Workbook, T, calculate, export_workbook, dates, digest, map_state_baseline
from .lan_bitable_template_portal.cabinet_power import CabinetPowerService
TEMPLATES=Path(__file__).parent/"lan_bitable_template_portal/templates/cabinet_power"

class MemoryStore:
    db_path=Path("unused.sqlite")
    def __init__(self): self.documents={}
    def get_document(self,ns,key): return copy.deepcopy(self.documents.get((ns,key)))
    def put_document(self,ns,key,value): self.documents[(ns,key)]=copy.deepcopy(value)

class FakeFeishu:
    def __init__(self,records=()):
        self.records={r["record_id"]:copy.deepcopy(r) for r in records}; self.creates=0; self.fail_after_create=False
    def list_all(self,path="records"): return copy.deepcopy(list(self.records.values()))
    def ensure_fields(self): return True
    def get(self,rid): return copy.deepcopy(self.records[rid])
    def create(self,fields,operation_id):
        self.creates+=1; rid="recNew"+str(self.creates)
        self.records[rid]={"record_id":rid,"fields":copy.deepcopy(fields)}
        if self.fail_after_create: raise TimeoutError("response lost")
        return self.get(rid)
    def update(self,rid,fields):
        self.records[rid]["fields"].update(copy.deepcopy(fields)); return self.get(rid)

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
        self.remote=FakeFeishu(self.source_records); self.service=CabinetPowerService(self.store,self.remote,self.tmp.name)
        self.service._directory=FakeFeishu(self.directory_records)
        self.service.do_refresh("",{}, {})

    def tearDown(self):
        self.service.shutdown(); self.tmp.cleanup()

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
        finally:
            fresh.shutdown()

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
                self.assertEqual([x.get("ref") for x in before.sheet(name).iter(T("mergeCell"))],[x.get("ref") for x in after.sheet(name).iter(T("mergeCell"))])
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
        app=FastAPI(); controller=Controller()
        runtime=SimpleNamespace(state_store=self.store,auth_manager=SimpleNamespace(is_admin=lambda s:False,scope_allowed=lambda s,scope:scope=="A"))
        install_cabinet_power_routes(app,controller,runtime)
        controller._cabinet_power.remote=self.remote; controller._cabinet_power._directory=self.service._directory
        controller._cabinet_power.root=Path(self.tmp.name)
        controller._cabinet_power.local=self.service.local
        with TestClient(app) as client:
            self.assertEqual(client.get("/api/cabinet-power/buildings").status_code,401)
            self.assertEqual(client.get("/api/cabinet-power/overview?scope=B",headers={"x-test-login":"1"}).status_code,403)
            self.assertEqual(client.post("/api/cabinet-power/imports/commit",headers={"x-test-login":"1"}).status_code,404)
            data=client.get("/api/cabinet-power/overview?scope=A",headers={"x-test-login":"1"}).json()["data"]
            self.assertEqual(data["record_count"],1031)

if __name__=="__main__": unittest.main()

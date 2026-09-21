import copy
import datetime as dt
import io
import json
import os
import re
import sys
import tempfile
import threading
import time
import unittest
import uuid
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from xml.etree import ElementTree as ET
from .lan_bitable_template_portal.cabinet_power_data import source_rows, from_feishu, to_fields,source_evidence,complete_source_record
from .lan_bitable_template_portal.cabinet_power_excel import CabinetError, Workbook, T, _notice_period_dates, baseline_correction_operations, calculate, completed_state_event, derive_records, export_workbook, dates, digest, map_state_baseline, room_code, system_name
from .lan_bitable_template_portal.cabinet_power import CabinetFeishu, CabinetPowerService, EXPORT_ARCHIVE_APP_TOKEN, EXPORT_ARCHIVE_FIELDS, EXPORT_ARCHIVE_TABLE_ID, equivalent
from .lan_bitable_template_portal.cabinet_power_batches import CabinetBatchService, POWER_ACTIONS_BY_STATE
from .lan_bitable_template_portal.cabinet_power_evidence import _rows_from_ocr
TEMPLATES=Path(__file__).parent/"lan_bitable_template_portal/templates/cabinet_power"

class CabinetBatchRecognitionTests(unittest.TestCase):
    def test_empty_batch_delete_route_is_registered(self):
        source=(Path(__file__).parent/"lan_bitable_template_portal/cabinet_power_routes.py").read_text(encoding="utf-8")
        self.assertIn('"batches/{batch_id}":["GET","PATCH","DELETE"]',source)

    def test_split_ocr_cells_produce_a_complete_batch_row(self):
        lines=[
            ("包间",[("包",245,45),("间",262,45)]),("包间系统名",[("包",350,34)]),
            ("操作类型",[("操作类型",521,45)]),("期望完成时间",[("期望完成时间",649,45)]),
            ("实际完成时间",[("实际完成时间",808,45)]),("营商机柜编",[("营商机柜编",983,34)]),
            ("E2-1EAI1",[("E2",246,112),("一",265,120),("1",272,112),("EAI",285,112),("1",316,112)]),
            ("8",[("8",280,131)]),("EAI18-E2-",[("EAI",354,112),("1",386,112),("8",394,112),("一",403,120),("E2",409,112),("一",428,120)]),
            ("AI6",[("AI",464,122),("6",485,122)]),("正式电转丬则试",[("正式电转丬则试",522,108)]),("电",[("电",565,131)]),
            ("2026-04-0420℃",[("2026",656,112),("04",697,112),("04",721,112),("20",743,112),("℃",762,112)]),
            ("0.22",[("0",700,131),("22",713,131)]),("2026-04-0320℃",[("2026",815,112),("04",856,112),("03",880,112),("20",902,112),("℃",921,112)]),
            ("0.41",[("0",859,131),("41",872,131)]),("AI6",[("AI",1002,122),("6",1023,122)]),("SUCCES",[("SUCCES",1097,112)]),
        ]
        rows=_rows_from_ocr(lines,1176)
        self.assertEqual(rows,[{"scope":"E","room":"201","rack":"A16","supplier_rack":"A16",
            "action":"正式电转测试电","expected":"2026-04-04 20:00:22","actual":"2026-04-03 20:00:41",
            "result":"成功","raw":rows[0]["raw"]}])

    def test_notice_period_range_uses_the_real_end_date(self):
        self.assertEqual(_notice_period_dates("2026.3.23-25"),["2026-03-23","2026-03-25"])
        self.assertEqual(_notice_period_dates("2026.3.27-4.1"),["2026-03-27","2026-04-01"])

    def test_b_carrier_legacy_aliases_display_as_separate_named_rooms(self):
        for room, old_name in (("216", "EA118-B2-16"), ("247", "EA118-B2-47")):
            with self.subTest(room=room):
                expected = f"EA118-B-{room}运营商机房"
                self.assertEqual(system_name("B", room), expected)
                self.assertEqual(room_code(old_name, "B"), (room, False))
                self.assertEqual(room_code(expected, "B"), (room, False))
                op = from_feishu({"record_id": "recCarrier", "fields": {
                    "楼栋": "B楼", "包间系统名称": old_name, "机架": "B02",
                    "操作类型": "上测试电", "实际完成时间": 1741859439000,
                    "结果": "成功",
                }})
                self.assertEqual((op["room"], op["system_name"]), (room, expected))
                self.assertEqual(op["raw_fields"]["包间系统名称"], old_name)
        self.assertEqual(system_name("B", "201"), "EA118-B2-1")

    def test_pdf_rows_keep_missing_values_and_split_glued_result(self):
        text=(
            "机柜下测试电确认单\n申请时间： 2026-08-31 17:49:03\n操作类型： 下测试电\n"
            "EA118 B4-2.EA118 B16 B16 服务器机柜 WholeRack 2026-08-31 15:03:55\n"
            "EA118 A4-2.EA118 G02 G02 服务器机柜 WholeRack 2026-09-03 10:12:20成功 2026-08-31 14:48:26"
        )
        batch=object.__new__(CabinetBatchService)
        batch.import_root=Path("unused")
        batch._pdf_reader=lambda _path:SimpleNamespace(pages=[SimpleNamespace(extract_text=lambda **_kwargs:text)])
        batch._set_file_progress=lambda *_args,**_kwargs:None
        rows=batch._parse_pdf("batch",{"file_id":"file","sha256":"hash","name":"机柜下测试电确认单.pdf"})
        self.assertEqual(len(rows),2)
        self.assertEqual((rows[0]["scope"],rows[0]["room"],rows[0]["actual"],rows[0]["result"]),("B","402","",""))
        self.assertEqual((rows[1]["actual"],rows[1]["result"]),("2026-09-03 10:12:20","成功"))
        self.assertTrue(all("order_time" not in row and "application_time" not in row for row in rows))

    def test_pdf_action_validation_uses_latest_successful_state(self):
        inventory=[{"room":"201","rack":rack,"rack_type":"服务器机柜"} for rack in ("A01","A02","A03")]
        events={"A02":[{"action":"上测试电","actual":"2026-09-14 10:00:00","result":"成功"}],
                "A03":[{"action":"上正式电","actual":"2026-09-14 10:00:00","result":"成功"}]}
        snapshot={"config":{"inventory":inventory},"operations":[{"room":"201","rack":rack,"events":items} for rack,items in events.items()]}
        batch=object.__new__(CabinetBatchService)
        batch.cabinet=SimpleNamespace(_snapshot=lambda _scope:snapshot)
        self.assertEqual(POWER_ACTIONS_BY_STATE["off"],{"上测试电","上正式电"})
        self.assertEqual(POWER_ACTIONS_BY_STATE["test"],{"测试电转正式电","下测试电"})
        self.assertEqual(POWER_ACTIONS_BY_STATE["formal"],{"正式电转测试电","下正式电"})
        rows=[]
        for index,(rack,action) in enumerate((("A01","上正式电"),("A02","下测试电"),("A03","正式电转测试电"),("A01","下正式电"),("A02","上正式电"),("A03","上测试电"))):
            actual="2026-09-15 11:00:00" if index<3 else "2026-09-15 11:01:00"
            rows.append({"row_id":f"row_{index}","operation_id":f"operation_{index}","scope":"A","room":"201","rack":rack,"rack_type":"服务器机柜","action":action,
                         "expected":actual,"actual":actual,"result":"成功","status":"ready"})
        payload={"source":"pdf","rows":rows}
        batch._validate_rows(payload)
        self.assertEqual([row["current_power_state"] for row in rows],["off","test","formal","off","test","formal"])
        self.assertEqual([row["status"] for row in rows],["ready"]*3+["invalid"]*3)
        with self.assertRaisesRegex(CabinetError,"当前为测试电"):
            batch._row_payload({"source":"pdf","batch_id":"test"},rows[4])

class MemoryStore:
    db_path=Path("unused.sqlite")
    def __init__(self): self.documents={}
    def get_document(self,ns,key): return copy.deepcopy(self.documents.get((ns,key)))
    def put_document(self,ns,key,value): self.documents[(ns,key)]=copy.deepcopy(value)

class FakeFeishu:
    def __init__(self,records=()):
        self.records={r["record_id"]:copy.deepcopy(r) for r in records}; self.creates=0; self.fail_after_create=False; self.fail_after_delete=False; self.list_calls=0
        self.batch_create_calls=0; self.batch_update_calls=0; self.fail_after_batch_create=False; self.fail_after_batch_update=False
        self.attachments={}
    def list_all(self,path="records",filters=None): self.list_calls+=1; return copy.deepcopy(list(self.records.values()))
    def ensure_fields(self): return True
    def get(self,rid): return copy.deepcopy(self.records[rid])
    def create(self,fields,operation_id):
        self.creates+=1; rid="recNew"+str(self.creates)
        self.records[rid]={"record_id":rid,"fields":copy.deepcopy(fields)}
        if self.fail_after_create: raise TimeoutError("response lost")
        return self.get(rid)
    def update(self,rid,fields):
        self.records[rid]["fields"].update(copy.deepcopy(fields)); return self.get(rid)
    def batch_create(self,rows):
        self.batch_create_calls+=1
        created=[self.create(fields,"batch") for fields in rows]
        if self.fail_after_batch_create: raise TimeoutError("batch response lost")
        return created
    def batch_update(self,rows):
        self.batch_update_calls+=1
        updated=[self.update(row["record_id"],row["fields"]) for row in rows]
        if self.fail_after_batch_update: raise TimeoutError("batch response lost")
        return updated
    def batch_get(self,record_ids):
        return [self.get(rid) for rid in record_ids if rid in self.records]
    def delete(self,rid):
        self.records.pop(rid)
        if self.fail_after_delete: raise TimeoutError("delete response lost")
    def upload_attachment(self,path,file_name):
        token="fileEvidence"+str(len(self.attachments)+1).zfill(12)
        self.attachments[token]=Path(path).read_bytes()
        return token
    def download_attachment(self,token): return self.attachments[token]

class FakeExportFeishu:
    def __init__(self):
        self.fields={"自动编号":{"field_name":"自动编号","type":1005}}
        self.records={}; self.upload_calls=0; self.creates=0; self.fail_after_create=False; self.lock=threading.RLock()
    def list_all(self,path="records",filters=None):
        with self.lock:
            items=list(self.fields.values()) if path=="fields" else list(self.records.values())
            if filters and path=="records":
                value=json.loads(filters.split("=",1)[1])
                items=[item for item in items if item.get("fields",{}).get("导出标识")==value]
            return copy.deepcopy(items)
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

    def test_attachment_readback_checks_file_tokens(self):
        expected={"上下电确认截图":[{"file_token":"fileEvidence12345"}]}
        self.assertFalse(equivalent(expected,{"上下电确认截图":[]}))
        self.assertTrue(equivalent(expected,{"上下电确认截图":[{"file_token":"fileEvidence12345","name":"proof.png"}]}))

    def test_b_carrier_rooms_stay_separate_from_standard_rooms(self):
        rooms = {room["id"]: room for room in self.service.overview("B")["rooms"]}
        for room in ("216", "247"):
            self.assertEqual(rooms[room]["name"], f"EA118-B-{room}运营商机房")
            self.assertTrue(rooms[room]["carrier"])
            self.assertEqual(rooms[room]["total"], 10)
        self.assertEqual(rooms["201"]["name"], "EA118-B2-1")
        self.assertFalse(rooms["201"]["carrier"])

    def test_current_state_uses_each_floorplan_baseline_then_new_operations(self):
        for scope in "ABCDE":
            content=(TEMPLATES/(scope+".xlsm")).read_bytes()
            operations=[from_feishu(record) for record in self.source_records if record["fields"]["楼栋"]==scope+"楼"]
            config=copy.deepcopy(self.configs[scope])
            config["power_baseline"]=map_state_baseline(content,config,operations)[0]
            expected=Counter(value["state"] for value in config["power_baseline"].values())
            actual=Counter(rack["state"] for rack in derive_records(config,operations)["racks"])
            self.assertEqual((actual["formal"],actual["test"],actual["off"]),
                             (expected["formal"],expected["test"],expected["off"]),scope)
        content=(TEMPLATES/"C.xlsm").read_bytes(); model,rows=source_rows(content,"C")
        evidence=source_evidence(content,model); records=[]
        for index,row in enumerate(rows):
            record={**row,"record_id":f"recCBaseline{index}"}
            records.append(complete_source_record(record,evidence) or record)
        operations=[from_feishu(record) for record in records]
        config={**model,"template_data":{"hash":model["template_hash"],"formats":model["formats"]}}
        config["power_baseline"]=map_state_baseline(content,config,operations)[0]
        corrections=baseline_correction_operations(config,operations)
        self.assertEqual((len(corrections),Counter(item["action"] for item in corrections)),
                         (25,Counter({"测试电转正式电":1,"上测试电":12,"下测试电":12})))
        self.service.local.replace("C",config,records,[]); self.service._cache.pop("C",None)
        snapshot=self.service._snapshot("C")
        self.assertEqual(sum(bool(item.get("meta",{}).get("baseline_correction")) for item in snapshot["operations"]),25)
        first=corrections[0]; visible=self.service.operations("C",{"room":first["room"],"rack":first["rack"],"page_size":100})
        self.assertTrue(any(item.get("meta",{}).get("baseline_correction") for item in visible["items"]))
        self.assertEqual((visible["rack_state"]["latest_success"]["action"],visible["rack_state"]["latest_success"]["actual"],
                          visible["rack_state"]["latest_success"]["baseline_correction"]),(first["action"],"",True))
        self.assertTrue(self.service.operations("C",{"sheet":first["source"],"page_size":100})["items"])
        exported=Workbook(export_workbook(content,snapshot["config"],snapshot["operations"]))
        fmt=next(item for item in model["formats"] if item["sheet"]==first["source"])
        action_column=fmt["groups"][-1 if first["category"]=="down" else 0]["action"]
        self.assertTrue(any(row.get(fmt["rack"])==first["rack"] and row.get(action_column)==first["action"]
                            for _number,row in exported.rows(first["source"])))
        self.assertEqual(derive_records(config,operations)["counts"],{
            "total":998,"formal":939,"test":31,"off":28,"unknown":0,"powered":970})
        no_time=next(op for op in operations if any(event["result"]=="成功" and not event["actual"] for event in op["events"]))
        self.assertFalse(any("未配对" in issue for issue in no_time["issues"]))
        room,rack=next(key.split("/") for key,value in config["power_baseline"].items() if value["state"]=="formal")
        operations.append({"record_id":"recCNewConversion","scope":"C","room":room,"rack":rack,
            "issues":[],"groups":[{}],"events":[{"id":"new_conversion:0","action":"正式电转测试电",
            "actual":"2026-09-21 10:00:00","expected":"","result":"成功","group":0}]})
        changed=derive_records(config,operations)["counts"]
        self.assertEqual((changed["formal"],changed["test"]),(938,32))

    def test_inconsistent_cloud_baseline_is_rebuilt_from_template(self):
        scope="A"; content=(TEMPLATES/"A.xlsm").read_bytes(); config=copy.deepcopy(self.configs[scope])
        records=[copy.deepcopy(record) for record in self.source_records if record["fields"]["楼栋"]=="A楼"]
        operations=[from_feishu(record) for record in records]
        config["power_baseline"]=map_state_baseline(content,config,operations)[0]
        key=next(key for key,value in config["power_baseline"].items() if value["state"]=="formal")
        config["power_baseline"][key].update(state="off",color="#00B050")
        self.service.local.replace(scope,config,records,[]); self.service._cache.pop(scope,None)
        room,rack=key.split("/"); state=self.service.operations(scope,{"room":room,"rack":rack,"page_size":100})["rack_state"]
        current=self.service.config(scope)["power_baseline"][key]
        self.assertEqual((current["color"],current["state"],state["state"],state["latest_success"]["action"]),
                         ("#FF0000","formal","formal","上正式电"))

    def test_floorplan_baseline_is_frozen_across_later_writes_and_restart(self):
        scope="A"; before=self.service._snapshot(scope)
        frozen=copy.deepcopy(self.service.local.document(scope,"power_baseline:frozen_v1"))
        rack=next(item for item in derive_records(before["config"],before["operations"])["racks"] if item["state"]=="off")
        saved=self.service.save_operation(scope,{"operation_id":"baseline_restart_write_01",
            "room":rack["room"],"rack":rack["rack"],"rack_type":rack["rack_type"],"result":"成功",
            "groups":[{"id":"later_event","action":"上正式电","expected":"2026-09-20 09:00:00",
                       "actual":"2026-09-20 09:01:00","result":"成功"}]},"owner")
        fresh=CabinetPowerService(self.store,self.remote,self.tmp.name); fresh._directory=self.service._directory
        try:
            current=next(item for item in fresh.overview(scope)["racks"]
                         if (item["room"],item["rack"])==(rack["room"],rack["rack"]))
            self.assertEqual(current["state"],"formal")
            self.assertEqual(fresh.local.document(scope,"power_baseline:frozen_v1"),frozen)
            self.assertNotIn(saved["events"][0]["id"],str(frozen))
        finally:
            fresh.shutdown()

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

    def test_packaged_template_replaces_compatible_old_feishu_hash_only(self):
        legacy=copy.deepcopy(self.configs["A"]); legacy["template_data"]["hash"]="0"*64
        current=self.service._packaged_config("A",legacy)
        self.assertNotEqual(current["template_data"]["hash"],legacy["template_data"]["hash"])
        self.assertTrue(current["map_values"])
        incompatible=copy.deepcopy(legacy); incompatible["inventory"][0]["positions"][0]["range"]="Z999:Z999"
        with self.assertRaisesRegex(CabinetError,"结构不一致"):
            self.service._packaged_config("A",incompatible)

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
        self.assertEqual({row["action"] for row in batches[0]["rows"]},{"上正式电"})
        self.assertEqual({row["action"] for row in batches[1]["rows"]},{"上正式电"})
        self.assertEqual({row["action"] for row in batches[2]["rows"]},{"下测试电"})
        self.assertEqual({row["expected"] for row in batches[2]["rows"]},{"2026-09-03 19:00:00"})
        self.assertTrue(all(not row["actual"] and row["result"]=="成功" for batch in batches for row in batch["rows"]))
        self.assertTrue(all(row["current_power_state"]=="off" for row in batches[0]["rows"]))
        self.assertEqual(self.service.batches._infer_notice_action({"config":{"inventory":[]},"operations":[]},"201","A01","up",""),("","机柜未匹配当前目录，无法自动识别操作类型","unknown"))
        legacy=self.service.batches.store.get(batches[0]["batch_id"])
        legacy["rows"][0].update(action="",result="")
        legacy["rows"][0].pop("current_power_state",None)
        self.service.batches.store.save(legacy,legacy["version"])
        upgraded=self.service.batches.get(legacy["batch_id"])
        self.assertEqual((upgraded["rows"][0]["action"],upgraded["rows"][0]["result"],upgraded["rows"][0]["current_power_state"]),("上正式电","成功","off"))
        repeated=self.service.batches.create_from_notice({**samples[0],"owner_id":"owner"})
        self.assertEqual(repeated["batch_id"],batches[0]["batch_id"])

    def test_notice_summary_counts_start_and_obeys_row_flag_and_lifecycle(self):
        source={"event_action":"start","target_record_id":"rec-notice-summary","notice_type":"上电通告",
                "scope":"B","cabinet":"B-216运营商机房B04、B05","quantity":"2","owner_id":"owner",
                "start_time":"2026-09-19 11:05","end_time":"2026-09-19 23:59","sent_at":"2026-09-19 11:06:00"}
        service=self.service.batches
        batch=service.apply_notice_event(source)
        summary=lambda: service.notice_summary("B",self.configs["B"])["items"]
        self.assertEqual(len(summary()),2)
        self.assertEqual({item["date"] for item in summary()},{"2026-09-19"})
        batch=service.apply_notice_event({**source,"event_action":"end","sent_at":"2026-09-20 00:05:00"})
        self.assertEqual(len(summary()),2)
        self.assertEqual({item["date"] for item in summary()},{"2026-09-19"})
        prior_version=service.notice_summary("B",self.configs["B"])["version"]
        row=batch["rows"][0]
        batch=service.update(batch["batch_id"],{"version":batch["version"],"rows":[
            {"row_id":row["row_id"],"exclude_notice_summary":True}]},"owner",["B"])
        self.assertEqual(len(summary()),1)
        self.assertNotEqual(prior_version,service.notice_summary("B",self.configs["B"])["version"])
        self.assertEqual(batch["rows"][0]["edits"][-1]["field"],"exclude_notice_summary")
        batch=service.apply_notice_event({**source,"event_action":"update","cabinet":"B-247运营商机房B05",
                                          "quantity":"1"})
        self.assertEqual([(row["room"],row["rack"]) for row in summary()],[('247','B05')])
        self.assertEqual(summary()[0]["date"],"2026-09-19")
        self.assertEqual(sum(bool(row.get("notice_removed")) for row in batch["rows"]),2)
        service.apply_notice_event({**source,"event_action":"undo_end"})
        self.assertEqual(len(summary()),1)
        service.apply_notice_event({**source,"event_action":"end","sent_at":"2026-09-20 09:00:00"})
        self.assertEqual(summary()[0]["date"],"2026-09-19")
        service.apply_notice_event({**source,"event_action":"delete"})
        self.assertEqual(summary(),[])
        self.assertFalse(service.list("owner",["B"])["items"])
        service.apply_notice_event({**source,"event_action":"undo_delete","prior_record_id":"rec-notice-summary",
                                    "target_record_id":"rec-notice-restored"})
        self.assertEqual(len(summary()),1)

    def test_notice_summary_freezes_template_history_and_requires_real_start_time(self):
        service=self.service.batches
        historical=service.create_from_notice({"target_record_id":"rec-notice-baseline","notice_type":"上电通告",
            "scope":"B","cabinet":"B-216运营商机房B04","quantity":"1","owner_id":"owner",
            "sent_at":"2026-09-03 10:00:00"})
        summary=service.notice_summary("B",self.configs["B"])
        self.assertEqual(summary["baseline_date"],"2026-09-03")
        self.assertEqual(summary["items"],[])
        pending=service.create_from_notice({"target_record_id":"rec-notice-no-time","notice_type":"上电通告",
            "scope":"B","cabinet":"B-247运营商机房B05","quantity":"1","owner_id":"owner"})
        self.assertEqual(service.notice_summary("B",self.configs["B"])["items"],[])
        service.apply_notice_event({"event_action":"start","target_record_id":"rec-notice-no-time",
            "notice_type":"上电通告","sent_at":"2026-09-19 10:00:00"})
        self.assertEqual([(item["room"],item["rack"]) for item in service.notice_summary("B",self.configs["B"])["items"]],
                         [("247","B05")])
        self.assertTrue(historical["batch_id"] and pending["batch_id"])

    def test_batch_store_version_compare_is_atomic_across_instances(self):
        batch=self.service.batches.create_manual([self._manual_batch_row("A","2026-09-19 01:02:03")],"owner")
        first=self.service.batches.store.get(batch["batch_id"])
        other=type(self.service.batches.store)(self.service.batches.store.path)
        stale=other.get(batch["batch_id"])
        first["error"]="first"
        self.service.batches.store.save(first,first["version"])
        stale["error"]="stale"
        with self.assertRaisesRegex(CabinetError,"其他操作更新"):
            other.save(stale,stale["version"])

    def test_batch_store_migrates_legacy_embedded_rows_without_loss(self):
        store=self.service.batches.store
        batch=self.service.batches.create_manual([self._manual_batch_row("A","2026-09-19 01:02:03")],"owner")
        current=store.get(batch["batch_id"])
        with store._connect() as conn,conn:
            payload=json.loads(conn.execute("SELECT payload_json FROM batches WHERE batch_id=?",(batch["batch_id"],)).fetchone()[0])
            payload["rows"]=current["rows"]
            conn.execute("UPDATE batches SET payload_json=? WHERE batch_id=?",(json.dumps(payload,ensure_ascii=False),batch["batch_id"]))
            conn.execute("DELETE FROM batch_rows WHERE batch_id=?",(batch["batch_id"],))
            conn.execute("DELETE FROM batch_meta WHERE key='batch_payload_normalization_version'")
        migrated=type(store)(store.path)
        restored=migrated.get(batch["batch_id"])
        self.assertEqual([(row["room"],row["rack"]) for row in restored["rows"]],
                         [(current["rows"][0]["room"],current["rows"][0]["rack"])])
        with migrated._connect() as conn:
            self.assertNotIn("rows",json.loads(conn.execute("SELECT payload_json FROM batches WHERE batch_id=?",(batch["batch_id"],)).fetchone()[0]))

    def test_legacy_notice_end_requires_finished_status_and_actual_time(self):
        service=self.service.batches
        batch=service.create_from_notice({"target_record_id":"rec-old-notice","notice_type":"下电通告",
            "scope":"B","cabinet":"B-402包间B15","quantity":"1","owner_id":"owner"})
        self.assertEqual(len(service.notice_summary("B",self.configs["B"])["items"]),0)
        self.assertEqual(service.reconcile_legacy_notice_end_times(lambda _id,_type:(True,{"fields":{
            "上电状态":"开始","实际结束时间":1790000000000}})),0)
        self.assertEqual(service.get(batch["batch_id"])["source_notice"]["end_time_check"]["status"],"pending")
        self.assertEqual(service.reconcile_legacy_notice_end_times(lambda _id,_type:(True,{"fields":{
            "上电状态":"结束","实际结束时间":1790000000000}}),force=True),1)
        self.assertEqual(service.get(batch["batch_id"])["source_notice"]["ended_at"],service._notice_datetime(1790000000000))

    def test_missing_notice_record_is_deleted_and_removed_from_summary(self):
        service=self.service.batches
        batch=service.create_from_notice({"target_record_id":"rec-deleted-notice","notice_type":"上电通告",
            "scope":"B","cabinet":"B-216运营商机房B04","quantity":"1","owner_id":"owner",
            "sent_at":"2026-09-19 10:00:00"})
        self.assertEqual(len(service.notice_summary("B",self.configs["B"])["items"]),1)
        service.reconcile_legacy_notice_end_times(
            lambda _id,_type:(False,"1254043 - RecordIdNotFound"),force=True)
        deleted=service.get(batch["batch_id"])
        self.assertTrue(deleted["source_notice"]["deleted_at"])
        self.assertEqual(deleted["source_notice"]["end_time_check"]["status"],"deleted")
        self.assertEqual(deleted["source_notice"]["lifecycle_audit"][-1]["action"],"remote_record_missing")
        self.assertEqual(service.notice_summary("B",self.configs["B"])["items"],[])

        legacy=service.create_from_notice({"target_record_id":"rec-already-missing","notice_type":"上电通告",
            "scope":"B","cabinet":"B-247运营商机房B05","quantity":"1","owner_id":"owner",
            "sent_at":"2026-09-19 11:00:00"})
        service._change(legacy["batch_id"],lambda current:current["source_notice"].update(
            end_time_check={"status":"failed","error":"1254043 - RecordIdNotFound",
                            "checked_at":dt.datetime.now().timestamp()}))
        service.reconcile_legacy_notice_end_times(
            lambda *_args: (_ for _ in ()).throw(AssertionError("saved missing record must not be fetched")))
        self.assertTrue(service.get(legacy["batch_id"])["source_notice"]["deleted_at"])
        self.assertEqual(service.notice_summary("B",self.configs["B"])["items"],[])

    def test_deleted_notice_rollback_failure_stays_visible_as_exception(self):
        service=self.service.batches
        source={"target_record_id":"rec-notice-delete","notice_type":"上电通告","scope":"B",
                "cabinet":"B-216运营商机房B04","quantity":"1","owner_id":"owner"}
        batch=service.create_from_notice(source)
        stored=service.store.get(batch["batch_id"])
        stored["rows"][0].update(status="completed",wrote_record=True,record_id="rec-written",
                                   operation_id="batch_written_operation")
        service.store.save(stored,stored["version"])
        self.service.rollback_batch_operation=lambda *_args: (_ for _ in ()).throw(CabinetError("已有后续操作",409))
        service.apply_notice_event({**source,"event_action":"delete"})
        result=self._wait_batch(batch["batch_id"])
        self.assertEqual(result["rows"][0]["status"],"rollback_blocked")
        self.assertEqual(service.list("owner",["B"])["total"],0)
        exceptions=service.list("owner",["B"],status="notice_rollback_error")
        self.assertEqual([item["batch_id"] for item in exceptions["items"]],[batch["batch_id"]])

    def test_notice_summary_checkbox_remains_editable_after_confirmation_or_cancel(self):
        service=self.service.batches
        batch=service.create_from_notice({"target_record_id":"rec-notice-flag","notice_type":"上电通告",
            "scope":"B","cabinet":"B-216运营商机房B04","quantity":"1","owner_id":"owner"})
        stored=service.store.get(batch["batch_id"])
        stored["rows"][0]["status"]="completed"
        stored=service.store.save(stored,stored["version"])
        visible=service.visible(stored,"owner",["B"])["rows"][0]
        self.assertFalse(visible["editable"])
        self.assertTrue(visible["can_edit_notice_summary"])
        row_id=visible["row_id"]
        changed=service.update(batch["batch_id"],{"version":stored["version"],"rows":[
            {"row_id":row_id,"exclude_notice_summary":True}]},"owner",["B"])
        self.assertTrue(changed["rows"][0]["exclude_notice_summary"])
        changed["status"]="cancelled"
        changed=service.store.save(changed,changed["version"])
        restored=service.update(batch["batch_id"],{"version":changed["version"],"rows":[
            {"row_id":row_id,"exclude_notice_summary":False}]},"owner",["B"])
        self.assertFalse(restored["rows"][0]["exclude_notice_summary"])
        with self.assertRaises(CabinetError):
            service.update(batch["batch_id"],{"version":restored["version"],"rows":[
                {"row_id":row_id,"exclude_notice_summary":True}]},"other",["A"])

    def test_notice_delete_waits_for_inflight_write_then_resumes_rollback(self):
        service=self.service.batches
        source={"target_record_id":"rec-notice-race","notice_type":"上电通告","scope":"B",
                "cabinet":"B-216运营商机房B04","quantity":"1","owner_id":"owner"}
        batch=service.create_from_notice(source)
        stored=service.store.get(batch["batch_id"])
        stored["rows"][0].update(status="writing",operation_started=True,operation_id="batch_race_operation")
        service.store.save(stored,stored["version"])
        with self.assertRaisesRegex(CabinetError,"正在写入"):
            service.apply_notice_event({**source,"event_action":"delete"})
        self.assertEqual(service.list("owner",["B"],status="notice_rollback_error")["total"],1)
        stored=service.store.get(batch["batch_id"])
        stored["rows"][0].update(status="completed",record_id="rec-race-written",wrote_record=True)
        service.store.save(stored,stored["version"])
        calls=[]
        self.service.rollback_batch_operation=lambda *args: calls.append(args)
        service._resume_notice_rollback_after_confirm(batch["batch_id"])
        result=self._wait_batch(batch["batch_id"])
        self.assertEqual(result["rows"][0]["status"],"rolled_back")
        self.assertEqual(len(calls),1)
        self.assertEqual(service.list("owner",["B"],status="notice_rollback_error")["total"],0)

    def test_notice_update_removed_confirmed_rack_rolls_back_without_counting_it(self):
        service=self.service.batches
        source={"target_record_id":"rec-notice-update","notice_type":"上电通告","scope":"B",
                "cabinet":"B-216运营商机房B04、B05","quantity":"2","owner_id":"owner",
                "sent_at":"2026-09-18 10:00:00"}
        batch=service.create_from_notice(source)
        stored=service.store.get(batch["batch_id"])
        stored["rows"][0].update(status="completed",record_id="rec-written-update",wrote_record=True,
                                   operation_id="batch_update_operation")
        service.store.save(stored,stored["version"])
        calls=[]
        self.service.rollback_batch_operation=lambda *args: calls.append(args)
        service.apply_notice_event({**source,"event_action":"update",
            "cabinet":"B-247运营商机房B05","quantity":"1"})
        self._wait_batch(batch["batch_id"])
        self.assertEqual(len(calls),1)
        rows=service.get(batch["batch_id"])["rows"]
        self.assertEqual({(row["room"],row["rack"]) for row in rows if not row.get("notice_removed")},
                         {("247","B05")})
        service.apply_notice_event({**source,"event_action":"end","sent_at":"2026-09-05 11:00:00"})
        self.assertEqual([(item["room"],item["rack"]) for item in service.notice_summary("B",self.configs["B"])["items"]],
                         [("247","B05")])

    def test_notice_update_keeps_committed_identity_when_original_row_was_corrected(self):
        service=self.service.batches
        source={"target_record_id":"rec-notice-corrected","notice_type":"上电通告","scope":"B",
                "cabinet":"B-216运营商机房B04","quantity":"1","owner_id":"owner"}
        batch=service.create_from_notice(source)
        stored=service.store.get(batch["batch_id"])
        stored["rows"][0].update(rack="B05",status="completed",wrote_record=True,
                                   record_id="rec-written-B05",operation_id="batch_corrected_operation")
        service.store.save(stored,stored["version"])
        self.service.rollback_batch_operation=lambda *_args: None
        service.apply_notice_event({**source,"event_action":"update"})
        rows=service.get(batch["batch_id"])["rows"]
        old=next(row for row in rows if row.get("record_id")=="rec-written-B05")
        self.assertEqual(old["rack"],"B05")
        self.assertTrue(old["notice_removed"])
        self.assertTrue(any(row["rack"]=="B04" and not row.get("notice_removed") for row in rows))

    def test_notice_end_uses_final_cabinets_even_when_prior_update_failed(self):
        service=self.service.batches
        source={"target_record_id":"rec-notice-final","notice_type":"上电通告","scope":"B",
                "cabinet":"B-216运营商机房B04","quantity":"1","owner_id":"owner",
                "sent_at":"2026-09-18 10:00:00"}
        service.create_from_notice(source)
        with self.assertRaises(CabinetError):
            service.apply_notice_event({**source,"event_action":"update","cabinet":"无法识别"})
        service.apply_notice_event({**source,"event_action":"end",
            "cabinet":"B-247运营商机房B05","cabinet_verified":True,"sent_at":"2026-09-18 18:00:00"})
        self.assertEqual([(row["room"],row["rack"]) for row in service.notice_summary("B",self.configs["B"])["items"]],
                         [("247","B05")])

    def test_notice_summary_does_not_count_other_building_invalid_row(self):
        service=self.service.batches
        source={"target_record_id":"rec-notice-scope","notice_type":"上电通告","scope":"A",
                "cabinet":"A-202包间B01","quantity":"1","owner_id":"owner"}
        batch=service.create_from_notice(source)
        row=batch["rows"][0]
        batch=service.update(batch["batch_id"],{"version":batch["version"],"rows":[
            {"row_id":row["row_id"],"scope":"B","room":"202","rack":"B01"}]},"owner",["A","B"],True)
        self.assertIn("notice_scope",{issue["code"] for issue in batch["rows"][0]["issues"]})
        service.apply_notice_event({**source,"event_action":"end","sent_at":"2026-09-18 18:00:00"})
        self.assertEqual(service.notice_summary("B",self.configs["B"])["items"],[])

    def test_notice_direction_change_resets_unconfirmed_action(self):
        service=self.service.batches
        source={"target_record_id":"rec-notice-direction","notice_type":"上电通告","scope":"D",
                "cabinet":"D-201包间B02","quantity":"1","owner_id":"owner"}
        batch=service.create_from_notice(source)
        self.assertEqual(batch["rows"][0]["action"],"上正式电")
        changed=service.apply_notice_event({**source,"event_action":"update","notice_type":"下电通告"})
        self.assertNotEqual(changed["rows"][0]["action"],"上正式电")

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
        self.assertEqual((changed["rows"][0]["current_power_state"],changed["rows"][0]["action"]),("unknown",""))
        manual=self.service.batches.update(batch["batch_id"],{
            "version":changed["version"],"rows":[{"row_id":row["row_id"],"action":"上测试电"}],
        },"owner",["B"])
        moved=self.service.batches.update(batch["batch_id"],{
            "version":manual["version"],"rows":[{"row_id":row["row_id"],"rack":"B04","action":"上测试电"}],
        },"owner",["B"])
        self.assertEqual(moved["rows"][0]["action"],"上测试电")

    def test_power_notice_outbox_handoff_never_raises_into_notice_flow(self):
        bin_path=str(Path(__file__).parent)
        added_path=bin_path not in sys.path
        if added_path: sys.path.insert(0,bin_path)
        from .lan_bitable_template_portal.server import PortalRuntime
        original=PortalRuntime.state_store
        original_service=PortalRuntime.cabinet_power_service
        class BrokenStore:
            def enqueue_outbox_event(self,*_args,**_kwargs): raise TimeoutError("locked")
        PortalRuntime.state_store=BrokenStore()
        PortalRuntime.cabinet_power_service=None
        try:
            event_id=PortalRuntime.enqueue_cabinet_notice_batch({
                "work_type":"power","action":"start","notice_type":"上电通告","scope":"B",
                "title":"test","start_time":"2026-09-03 11:05","end_time":"2026-09-03 23:59",
                "cabinet":"B-216运营商机房B04","quantity":"1",
            },job_id="job-safe",target_record_id="rec-safe",request_payload={"_auth_open_id":"owner"})
            self.assertEqual(event_id,0)
        finally:
            PortalRuntime.state_store=original
            PortalRuntime.cabinet_power_service=original_service
            if added_path: sys.path.remove(bin_path)

    def test_power_notice_handoff_survives_primary_outbox_failure(self):
        bin_path=str(Path(__file__).parent)
        added_path=bin_path not in sys.path
        if added_path: sys.path.insert(0,bin_path)
        from .lan_bitable_template_portal.server import PortalRuntime
        original_store,original_service=PortalRuntime.state_store,PortalRuntime.cabinet_power_service
        class BrokenStore:
            def enqueue_outbox_event(self,*_args,**_kwargs): raise TimeoutError("locked")
            def lease_outbox_events(self,*_args,**_kwargs): raise TimeoutError("locked")
            def append_event_async(self,*_args,**_kwargs): raise TimeoutError("locked")
        PortalRuntime.state_store=BrokenStore()
        PortalRuntime.cabinet_power_service=self.service
        try:
            with patch.object(PortalRuntime,"ensure_cabinet_notice_worker",return_value=None):
                queued=PortalRuntime.enqueue_cabinet_notice_batch({
                    "work_type":"power","action":"start","notice_type":"上电通告","scope":"B",
                    "cabinet":"B-216运营商机房B04","quantity":"1",
                },job_id="fallback-job",target_record_id="rec-fallback")
            self.assertEqual(queued,-1)
            self.assertIsNotNone(self.service.batches.store.next_notice_handoff())
            result=PortalRuntime._process_cabinet_notice_queue_once()
            self.assertEqual(result["status"],"success")
            self.assertIsNone(self.service.batches.store.next_notice_handoff())
            self.assertEqual(self.service.batches.get(result["batch_id"])["stats"]["total"],1)
        finally:
            PortalRuntime.state_store=original_store
            PortalRuntime.cabinet_power_service=original_service
            if added_path: sys.path.remove(bin_path)

    def test_failed_notice_handoff_can_resume_after_restart(self):
        store=self.service.batches.store
        store.queue_notice_handoff({"idempotency_key":"resume-notice-handoff","event_action":"start"})
        for _ in range(5):
            self.assertIsNotNone(store.next_notice_handoff())
            store.finish_notice_handoff("resume-notice-handoff","temporary failure")
        self.assertIsNone(store.next_notice_handoff())
        store.requeue_failed_notice_handoffs()
        self.assertEqual(store.next_notice_handoff()["attempts"],0)

    def test_stale_notice_update_does_not_overwrite_newer_end_snapshot(self):
        service=self.service.batches
        source={"target_record_id":"rec-ordered-notice","notice_type":"上电通告","scope":"B",
                "cabinet":"B-216运营商机房B04","quantity":"1","owner_id":"owner","event_at":1.0,
                "sent_at":"2026-09-18 10:00:00"}
        service.apply_notice_event(source)
        service.apply_notice_event({**source,"event_action":"end","event_at":3.0,
            "cabinet":"B-247运营商机房B05","cabinet_verified":True,"sent_at":"2026-09-18 18:00:00"})
        service.apply_notice_event({**source,"event_action":"update","event_at":2.0})
        self.assertEqual([(item["room"],item["rack"]) for item in service.notice_summary("B",self.configs["B"])["items"]],
                         [("247","B05")])

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

    def test_power_notice_outbox_updates_then_counts_successful_end(self):
        bin_path=str(Path(__file__).parent)
        added_path=bin_path not in sys.path
        if added_path: sys.path.insert(0,bin_path)
        from .lan_bitable_template_portal.server import PortalRuntime
        from .lan_bitable_template_portal.state_store import LanPortalStateStore
        old_store,old_service=PortalRuntime.state_store,PortalRuntime.cabinet_power_service
        state=LanPortalStateStore(Path(self.tmp.name)/"notice-lifecycle.sqlite3")
        PortalRuntime.state_store=state; PortalRuntime.cabinet_power_service=None
        try:
            base={"work_type":"power","notice_type":"上电通告","scope":"B",
                  "cabinet":"B-216运营商机房B04、B05","quantity":"2","response_time":"2026-09-19 11:05:00"}
            for action,job,changes in (
                ("start","start-job",{}),
                ("update","update-job",{"cabinet":"B-247运营商机房B05","quantity":"1"}),
                ("end","end-job",{"response_time":"2026-09-05 09:12:00"}),
            ):
                PortalRuntime.enqueue_cabinet_notice_batch({**base,**changes,"action":action},job_id=job,
                    target_record_id="rec-lifecycle",request_payload={"_auth_open_id":"owner"})
            PortalRuntime.cabinet_power_service=self.service
            with patch("bin.lan_bitable_template_portal.server.query_record_by_id",return_value=(True,{"fields":{
                "柜号":"B-247运营商机房B05","数量（个）":"1","上电状态":"结束","实际结束时间":"2026-09-05 09:12:00"}})):
                self.assertEqual([PortalRuntime._process_cabinet_notice_queue_once()["status"] for _ in range(3)],["success"]*3)
            summary=self.service.batches.notice_summary("B",self.configs["B"])
            self.assertEqual([(item["room"],item["rack"],item["date"]) for item in summary["items"]],
                             [("247","B05","2026-09-19")])
            self.assertFalse(PortalRuntime._process_cabinet_notice_queue_once()["processed"])
        finally:
            PortalRuntime.stop_cabinet_notice_worker()
            PortalRuntime.state_store=old_store; PortalRuntime.cabinet_power_service=old_service
            state.shutdown_write_worker(timeout=1)
            if added_path: sys.path.remove(bin_path)

    def test_notice_end_read_failure_keeps_start_cabinets_until_verified(self):
        bin_path=str(Path(__file__).parent)
        added_path=bin_path not in sys.path
        if added_path: sys.path.insert(0,bin_path)
        from .lan_bitable_template_portal.server import PortalRuntime
        from .lan_bitable_template_portal.state_store import LanPortalStateStore
        old_store,old_service=PortalRuntime.state_store,PortalRuntime.cabinet_power_service
        state=LanPortalStateStore(Path(self.tmp.name)/"notice-final-read.sqlite3")
        PortalRuntime.state_store=state; PortalRuntime.cabinet_power_service=self.service
        try:
            source={"target_record_id":"rec-notice-final-read","notice_type":"上电通告","scope":"B",
                    "cabinet":"B-216运营商机房B04","quantity":"1","owner_id":"owner",
                    "sent_at":"2026-09-18 10:00:00"}
            self.service.batches.create_from_notice(source)
            with patch.object(PortalRuntime,"ensure_cabinet_notice_worker",return_value=None):
                PortalRuntime.enqueue_cabinet_notice_batch({**source,"work_type":"power","action":"end",
                    "response_time":"2026-09-18 19:00:00"},job_id="final-read",target_record_id=source["target_record_id"])
            with patch("bin.lan_bitable_template_portal.server.query_record_by_id",return_value=(False,"timeout")):
                self.assertEqual(PortalRuntime._process_cabinet_notice_queue_once()["status"],"pending")
            self.assertEqual([(item["room"],item["rack"]) for item in self.service.batches.notice_summary("B",self.configs["B"])["items"]],[('216','B04')])
            with patch("bin.lan_bitable_template_portal.server.query_record_by_id",return_value=(True,{"fields":{
                "柜号":"B-216运营商机房B04","上电状态":"开始","实际结束时间":"2026-09-18 19:00:00"}})):
                self.assertEqual(PortalRuntime._process_cabinet_notice_queue_once()["status"],"pending")
            self.assertEqual([(item["room"],item["rack"]) for item in self.service.batches.notice_summary("B",self.configs["B"])["items"]],[('216','B04')])
            with patch("bin.lan_bitable_template_portal.server.query_record_by_id",return_value=(True,{"fields":{
                "柜号":"B-247运营商机房B05","数量（个）":"1","上电状态":"结束","实际结束时间":"2026-09-18 19:00:00"}})):
                self.assertEqual(PortalRuntime._process_cabinet_notice_queue_once()["status"],"success")
            self.assertEqual([(item["room"],item["rack"]) for item in self.service.batches.notice_summary("B",self.configs["B"])["items"]],[("247","B05")])
        finally:
            PortalRuntime.stop_cabinet_notice_worker()
            PortalRuntime.state_store=old_store; PortalRuntime.cabinet_power_service=old_service
            state.shutdown_write_worker(timeout=1)
            if added_path: sys.path.remove(bin_path)

    def test_power_notice_outbox_extracts_qt_notice_text(self):
        bin_path=str(Path(__file__).parent)
        added_path=bin_path not in sys.path
        if added_path: sys.path.insert(0,bin_path)
        from .lan_bitable_template_portal.server import PortalRuntime
        from .lan_bitable_template_portal.state_store import LanPortalStateStore
        old_store,old_service=PortalRuntime.state_store,PortalRuntime.cabinet_power_service
        state=LanPortalStateStore(Path(self.tmp.name)/"qt-notice.sqlite3")
        PortalRuntime.state_store=state; PortalRuntime.cabinet_power_service=None
        try:
            text="【下电通告】状态：开始\n【名称】EA118机房B楼机柜下电通告\n【时间】2026-09-03 14:55~2026-09-03 19:00\n【柜号】B-402包间B15、B16\n【数量】2\n【进度】准备工作已完成"
            PortalRuntime.enqueue_cabinet_notice_batch({"work_type":"power","action":"start",
                "notice_type":"下电通告","text":text},job_id="qt-start",target_record_id="rec-qt-power")
            PortalRuntime.cabinet_power_service=self.service
            result=PortalRuntime._process_cabinet_notice_queue_once()
            self.assertEqual(result["status"],"success")
            self.assertEqual(self.service.batches.get(result["batch_id"])["stats"]["total"],2)
        finally:
            PortalRuntime.stop_cabinet_notice_worker()
            PortalRuntime.state_store=old_store; PortalRuntime.cabinet_power_service=old_service
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

    def _wait_batch(self,batch_id):
        deadline=time.time()+15
        while time.time()<deadline:
            batch=self.service.batches.get(batch_id)
            if batch["status"]!="running": return batch
            time.sleep(.01)
        self.fail("批次处理超时")

    def _manual_batch_row(self,scope,actual,action="上正式电"):
        rack=self.configs[scope]["inventory"][0]
        return {"scope":scope,"room":rack["room"],"rack":rack["rack"],"rack_type":rack["rack_type"],
                "action":action,"expected":actual,"actual":actual,"result":"成功"}

    def test_batch_rollback_restores_abc_create_and_de_history(self):
        before_d=copy.deepcopy(next(op for op in self.service._snapshot("D")["operations"] if (op["room"],op["rack"])==(self.configs["D"]["inventory"][0]["room"],self.configs["D"]["inventory"][0]["rack"])))
        count_a=self.service.overview("A")["record_count"]
        batch=self.service.batches.create_manual([self._manual_batch_row(scope,"2026-09-14 01:02:03") for scope in ("A","D")],"owner")
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A","D"])
        done=self._wait_batch(batch["batch_id"])
        self.assertEqual(done["stats"]["completed"],2)
        self.service.batches.rollback(batch["batch_id"],{"version":done["version"],"all":True},"owner",["A","D"])
        undone=self._wait_batch(batch["batch_id"])
        self.assertEqual(undone["stats"]["rolled_back"],2,undone["rows"])
        self.assertEqual(self.service.overview("A")["record_count"],count_a)
        self.assertEqual(next(op for op in self.service._snapshot("D")["operations"] if op["record_id"]==before_d["record_id"])["groups"],before_d["groups"])
        self.assertNotIn(next(row["record_id"] for row in undone["rows"] if row["scope"]=="A"),self.remote.records)

    def test_rolled_back_rows_can_be_confirmed_with_new_operation_ids(self):
        batch=self.service.batches.create_manual(
            [self._manual_batch_row(scope,"2026-09-14 01:02:03") for scope in ("A","D")],"owner"
        )
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A","D"])
        first=self._wait_batch(batch["batch_id"])
        old_ids={row["scope"]:row["operation_id"] for row in first["rows"]}
        self.service.batches.rollback(batch["batch_id"],{"version":first["version"],"all":True},"owner",["A","D"])
        undone=self._wait_batch(batch["batch_id"])
        self.assertEqual(undone["stats"]["confirmable"],2)
        self.service.batches.confirm(batch["batch_id"],{"version":undone["version"],"all":True},"owner",["A","D"])
        repeated=self._wait_batch(batch["batch_id"])
        self.assertEqual(repeated["stats"]["completed"],2,repeated["rows"])
        for row in repeated["rows"]:
            self.assertNotEqual(row["operation_id"],old_ids[row["scope"]])
            self.assertEqual(row["attempts"][0]["operation_id"],old_ids[row["scope"]])

    def test_batch_image_reason_rollback_and_reconfirm(self):
        from PIL import Image
        rack=self.configs["A"]["inventory"][0]
        actual="2026-09-14 01:02:03"
        row={**self._manual_batch_row("A",actual),"result":"失败","failure_reason":""}
        batch=self.service.batches.create_manual([row],"owner")
        self.assertIn("failure_reason",{issue["code"] for issue in batch["rows"][0]["issues"]})
        batch=self.service.batches.update(batch["batch_id"],{"version":batch["version"],"rows":[{"row_id":batch["rows"][0]["row_id"],"failure_reason":"现场核验未通过"}]},"owner",["A"])
        self.assertEqual(batch["stats"]["confirmable"],1)
        image=Image.new("RGB",(160,80),"white"); output=io.BytesIO(); image.save(output,format="PNG")
        candidate={"scope":"A","room":rack["room"],"rack":rack["rack"],"supplier_rack":"",
                   "action":"上正式电","expected":actual,"actual":actual}
        with patch("bin.lan_bitable_template_portal.cabinet_power_evidence.recognize_image_with_timeout",return_value=[candidate]):
            batch=self.service.batches.add_images(batch["batch_id"],[("proof.png",output.getvalue())],"owner",["A"])
            deadline=time.time()+5
            while time.time()<deadline and self.service.batches.get(batch["batch_id"])["images"][0]["status"]=="recognizing": time.sleep(.01)
        batch=self.service.batches.get(batch["batch_id"])
        self.assertEqual(batch["images"][0]["suggestions"][0]["status"],"applied")
        image_id=batch["images"][0]["image_id"]
        self.assertEqual(batch["rows"][0]["evidence_images"],[image_id])
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A"])
        done=self._wait_batch(batch["batch_id"])
        self.assertEqual(done["stats"]["completed"],1,done["rows"])
        record_id=done["rows"][0]["record_id"]
        saved=self.remote.get(record_id)["fields"]
        self.assertEqual(saved["失败原因"],"现场核验未通过")
        self.assertEqual(len(saved["上下电确认截图"]),1)
        self.assertEqual(self.service.evidence_path("A",record_id,image_id)[0].read_bytes(),output.getvalue())
        thumbnail=self.service.evidence_path("A",record_id,image_id,True)[0]
        self.assertTrue(thumbnail.is_file())
        self.assertLess(thumbnail.stat().st_size,len(output.getvalue())+1024)
        local_image=self.service.evidence_path("A",record_id,image_id)[0]
        local_image.unlink()
        self.assertEqual(self.service.evidence_path("A",record_id,image_id)[0].read_bytes(),output.getvalue())
        self.assertEqual(self.service.batches.storage_status()["cloud_backed_files"],1)
        cleaned=self.service.batches.cleanup_evidence_cache()
        self.assertGreaterEqual(cleaned["deleted"],1)
        self.assertEqual(self.service.evidence_path("A",record_id,image_id)[0].read_bytes(),output.getvalue())
        self.service.batches.rollback(batch["batch_id"],{"version":done["version"],"all":True},"owner",["A"])
        undone=self._wait_batch(batch["batch_id"])
        self.assertEqual(undone["stats"]["rolled_back"],1,undone["rows"])
        self.service.batches.confirm(batch["batch_id"],{"version":undone["version"],"all":True},"owner",["A"])
        again=self._wait_batch(batch["batch_id"])
        self.assertEqual(again["stats"]["completed"],1,again["rows"])
        self.assertEqual(len(self.remote.attachments),1)
        self.assertEqual(self.remote.get(again["rows"][0]["record_id"])["fields"]["上下电确认截图"],saved["上下电确认截图"])

    def test_cross_scope_image_is_hidden_until_viewer_has_every_scope(self):
        from PIL import Image
        rows=[self._manual_batch_row(scope,"2026-09-14 01:02:03") for scope in ("A","B")]
        batch=self.service.batches.create_manual(rows,"owner")
        output=io.BytesIO();Image.new("RGB",(80,60),"white").save(output,format="PNG")
        candidates=[{"scope":row["scope"],"room":row["room"],"rack":row["rack"],"supplier_rack":"",
                     "action":row["action"],"expected":row["expected"],"actual":row["actual"]} for row in rows]
        with patch("bin.lan_bitable_template_portal.cabinet_power_evidence.recognize_image_with_timeout",return_value=candidates):
            self.service.batches.add_images(batch["batch_id"],[("mixed.png",output.getvalue())],"owner",["A","B"])
            deadline=time.time()+5
            while time.time()<deadline and self.service.batches.get(batch["batch_id"])["images"][0]["status"]=="recognizing":time.sleep(.01)
        batch=self.service.batches.get(batch["batch_id"]);image_id=batch["images"][0]["image_id"]
        limited=self.service.batches.visible(batch,"viewer",["A"])
        self.assertEqual(([row["scope"] for row in limited["rows"]],limited["images"],limited["files"]),(["A"],[],[]))
        with self.assertRaisesRegex(CabinetError,"无权查看截图"):
            self.service.batches.image_path(batch["batch_id"],image_id,"viewer",["A"])
        self.assertTrue(self.service.batches.image_path(batch["batch_id"],image_id,"viewer",["A","B"])[0].is_file())

    def test_batch_patch_delta_returns_only_changed_rows(self):
        rows=[self._manual_batch_row("A",f"2026-09-14 01:0{index}:03") for index in (2,3)]
        batch=self.service.batches.create_manual(rows,"owner")
        target=batch["rows"][0]
        changed=self.service.batches.update(batch["batch_id"],{
            "version":batch["version"],"response_mode":"delta",
            "rows":[{"row_id":target["row_id"],"expected":"2026-09-14 01:01:03"}],
        },"owner",["A"])
        self.assertTrue(changed["partial_rows"])
        self.assertEqual([row["row_id"] for row in changed["rows"]],[target["row_id"]])
        self.assertEqual(changed["rows"][0]["expected"],"2026-09-14 01:01:03")

    def test_existing_cloud_attachment_is_preserved_on_group_update(self):
        rack=self.configs["D"]["inventory"][0]
        existing=next(op for op in self.service._snapshot("D")["operations"] if (op["room"],op["rack"])==(rack["room"],rack["rack"]))
        operation=copy.deepcopy(existing)
        operation["raw_fields"]["上下电确认截图"]=[{"file_token":"fileOldEvidence123"}]
        operation["groups"].append({"id":"event_new","action":"上正式电","expected":"2026-09-14 01:02:03","actual":"2026-09-14 01:02:03","result":"成功","evidence_images":[{"image_id":"a"*64,"file_token":"fileNewEvidence123","extension":".png"}]})
        fields=to_fields(operation)
        self.assertEqual({item["file_token"] for item in fields["上下电确认截图"]},{"fileOldEvidence123","fileNewEvidence123"})

    def test_one_image_can_fill_multiple_existing_batch_rows_without_overwriting(self):
        from PIL import Image
        racks=self.configs["A"]["inventory"][:2]
        rows=[{"scope":"A","room":rack["room"],"rack":rack["rack"],"rack_type":rack["rack_type"],
               "action":"上正式电","expected":"","actual":"","result":"成功"} for rack in racks]
        rows[1]["expected"]="2026-09-13 01:00:00"
        batch=self.service.batches.create_manual(rows,"owner")
        image=Image.new("RGB",(160,80),"white"); output=io.BytesIO(); image.save(output,format="PNG")
        candidates=[{"scope":"A","room":rack["room"],"rack":rack["rack"],"supplier_rack":"",
                     "action":"上正式电","expected":"2026-09-14 01:02:03","actual":"2026-09-14 01:03:04"} for rack in racks]
        with patch("bin.lan_bitable_template_portal.cabinet_power_evidence.recognize_image_with_timeout",return_value=candidates):
            self.service.batches.add_images(batch["batch_id"],[("both.png",output.getvalue())],"owner",["A"])
            deadline=time.time()+5
            while time.time()<deadline and self.service.batches.get(batch["batch_id"])["images"][0]["status"]=="recognizing": time.sleep(.01)
        result=self.service.batches.get(batch["batch_id"])
        self.assertEqual(result["rows"][0]["expected"],candidates[0]["expected"])
        self.assertEqual(result["rows"][0]["actual"],candidates[0]["actual"])
        self.assertEqual(result["rows"][0]["evidence_images"],[result["images"][0]["image_id"]])
        self.assertEqual(result["rows"][1]["expected"],"2026-09-13 01:00:00")
        self.assertEqual(result["rows"][1]["actual"],"")
        self.assertEqual(result["images"][0]["suggestions"][1]["status"],"needs_review")
        self.assertEqual(len(result["rows"]),2)

    def test_pending_image_recognition_resumes_after_restart(self):
        batch=self.service.batches.create_manual([self._manual_batch_row("A","2026-09-14 01:02:03")],"owner")
        image_id="a"*64
        self.service.batches._change(batch["batch_id"],lambda current:current.setdefault("images",[]).append(
            {"image_id":image_id,"name":"proof.png","extension":".png","status":"recognizing","suggestions":[]}
        ))
        self.service.batches.shutdown(wait=True)
        with patch("bin.lan_bitable_template_portal.cabinet_power_evidence.recognize_image_with_timeout",return_value=[]):
            self.service._batches=CabinetBatchService(self.service,self.service.root)
            deadline=time.time()+5
            while time.time()<deadline and self.service.batches.get(batch["batch_id"])["images"][0]["status"]=="recognizing": time.sleep(.01)
        image=self.service.batches.get(batch["batch_id"])["images"][0]
        self.assertEqual(image["status"],"failed")

    def test_de_history_keeps_evidence_group_and_rollback_removes_new_attachment(self):
        from PIL import Image
        rack=self.configs["D"]["inventory"][0]
        original=copy.deepcopy(next(op for op in self.service._snapshot("D")["operations"] if (op["room"],op["rack"])==(rack["room"],rack["rack"])))
        row=self._manual_batch_row("D","2026-09-14 01:02:03")
        batch=self.service.batches.create_manual([row],"owner")
        image=Image.new("RGB",(80,60),"white"); output=io.BytesIO();image.save(output,format="PNG")
        candidate={"scope":"D","room":rack["room"],"rack":rack["rack"],"supplier_rack":"",
                   "action":"上正式电","expected":row["expected"],"actual":row["actual"]}
        with patch("bin.lan_bitable_template_portal.cabinet_power_evidence.recognize_image_with_timeout",return_value=[candidate]):
            self.service.batches.add_images(batch["batch_id"],[("proof.png",output.getvalue())],"owner",["D"])
            deadline=time.time()+5
            while time.time()<deadline and self.service.batches.get(batch["batch_id"])["images"][0]["status"]=="recognizing":time.sleep(.01)
        batch=self.service.batches.get(batch["batch_id"])
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["D"])
        done=self._wait_batch(batch["batch_id"])
        self.assertEqual(done["stats"]["completed"],1,done["rows"])
        saved=next(op for op in self.service._snapshot("D")["operations"] if op["record_id"]==original["record_id"])
        self.assertEqual(len(saved["groups"]),len(original["groups"])+1)
        self.assertTrue(any(group.get("evidence_images") for group in saved["groups"]))
        self.service.batches.rollback(batch["batch_id"],{"version":done["version"],"all":True},"owner",["D"])
        undone=self._wait_batch(batch["batch_id"])
        self.assertEqual(undone["stats"]["rolled_back"],1,undone["rows"])
        restored=next(op for op in self.service._snapshot("D")["operations"] if op["record_id"]==original["record_id"])
        self.assertEqual(restored["groups"],original["groups"])
        self.assertFalse(self.remote.get(original["record_id"])["fields"].get("上下电确认截图"))

    def test_pasted_image_can_be_removed_before_write_or_after_rollback(self):
        from PIL import Image
        rack=self.configs["A"]["inventory"][0]
        row=self._manual_batch_row("A","2026-09-14 01:02:03")
        batch=self.service.batches.create_manual([row],"owner")
        output=io.BytesIO();Image.new("RGB",(80,60),"white").save(output,format="PNG")
        candidate={"scope":"A","room":rack["room"],"rack":rack["rack"],"supplier_rack":"",
                   "action":row["action"],"expected":row["expected"],"actual":row["actual"]}
        with patch("bin.lan_bitable_template_portal.cabinet_power_evidence.recognize_image_with_timeout",return_value=[candidate]):
            for attempt in range(2):
                self.service.batches.add_images(batch["batch_id"],[("pasted.png",output.getvalue())],"owner",["A"])
                deadline=time.time()+5
                while time.time()<deadline and self.service.batches.get(batch["batch_id"])["images"][0]["status"]=="recognizing":time.sleep(.01)
                batch=self.service.batches.get(batch["batch_id"])
                image_id=batch["images"][0]["image_id"]
                self.assertEqual(batch["rows"][0]["evidence_images"],[image_id])
                if attempt==0:
                    with self.assertRaises(CabinetError):
                        self.service.batches.delete_image(batch["batch_id"],image_id,batch["version"],"other",["A"])
                    with self.assertRaises(CabinetError):
                        self.service.batches.delete_image(batch["batch_id"],image_id,batch["version"]-1,"owner",["A"])
                    removed=self.service.batches.delete_image(batch["batch_id"],image_id,batch["version"],"owner",["A"])
                    self.assertTrue(removed["images"][0]["deleted_at"])
                    self.assertEqual(removed["rows"][0]["evidence_images"],[])
                    self.assertTrue((self.service.root/"evidence"/(image_id+".png")).is_file())
                    restored=self.service.batches.restore_image(batch["batch_id"],image_id,removed["version"],"owner",["A"])
                    self.assertFalse(restored["images"][0]["deleted_at"])
                    self.assertEqual(restored["rows"][0]["evidence_images"],[image_id])
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A"])
        done=self._wait_batch(batch["batch_id"])
        self.assertEqual(done["stats"]["completed"],1,done["rows"])
        with self.assertRaisesRegex(CabinetError,"先回退"):
            self.service.batches.delete_image(batch["batch_id"],image_id,done["version"],"owner",["A"])
        self.service.batches.rollback(batch["batch_id"],{"version":done["version"],"all":True},"owner",["A"])
        undone=self._wait_batch(batch["batch_id"])
        removed=self.service.batches.delete_image(batch["batch_id"],image_id,undone["version"],"owner",["A"])
        self.assertEqual(removed["rows"][0]["evidence_images"],[])
        restored=self.service.batches.restore_image(batch["batch_id"],image_id,removed["version"],"owner",["A"])
        self.assertEqual(restored["rows"][0]["evidence_images"],[image_id])

    def test_image_registration_creates_rows_then_uses_existing_confirm_flow(self):
        from PIL import Image
        racks=self.configs["A"]["inventory"][:2]
        batch=self.service.batches.create_image_batch("owner",["A"])
        self.assertEqual((batch["source"],batch["rows"]),("image",[]))
        content=io.BytesIO();Image.new("RGB",(160,80),"white").save(content,format="PNG")
        states={(item["room"],item["rack"]):item["state"] for item in self.service.overview("A")["racks"]}
        candidates=[{"scope":"A","room":rack["room"],"rack":rack["rack"],"supplier_rack":"",
                     "action":{"formal":"下正式电","test":"下测试电"}.get(states.get((rack["room"],rack["rack"])),"上正式电"),"expected":"2026-09-14 01:02:03",
                     "actual":"2026-09-14 01:02:03","result":""} for rack in racks]
        with patch("bin.lan_bitable_template_portal.cabinet_power_evidence.recognize_image_with_timeout",return_value=candidates):
            self.service.batches.add_images(batch["batch_id"],[("mail.png",content.getvalue())],"owner",["A"])
            deadline=time.time()+5
            while time.time()<deadline and self.service.batches.get(batch["batch_id"])["images"][0]["status"]=="recognizing":time.sleep(.01)
        batch=self.service.batches.get(batch["batch_id"])
        self.assertEqual(len(batch["rows"]),2)
        image_id=batch["images"][0]["image_id"]
        self.assertEqual({row["evidence_images"][0] for row in batch["rows"]},{image_id})
        self.assertEqual([row["rack_type"] for row in batch["rows"]],[rack["rack_type"] for rack in racks])
        self.assertEqual(batch["stats"]["confirmable"],0)
        removed=self.service.batches.delete_image(batch["batch_id"],image_id,batch["version"],"owner",["A"])
        self.assertTrue(all(row["status"]=="excluded_image" for row in removed["rows"]))
        batch=self.service.batches.restore_image(batch["batch_id"],image_id,removed["version"],"owner",["A"])
        self.assertEqual({row["evidence_images"][0] for row in batch["rows"]},{image_id})
        with self.assertRaises(CabinetError):
            self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A"])
        batch=self.service.batches.get(batch["batch_id"])
        batch=self.service.batches.update(batch["batch_id"],{"version":batch["version"],"rows":[
            {"row_id":row["row_id"],"result":"成功"} for row in batch["rows"]]},"owner",["A"])
        self.assertEqual(batch["stats"]["confirmable"],2,batch["rows"])
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A"])
        done=self._wait_batch(batch["batch_id"])
        self.assertEqual(done["stats"]["completed"],2,done["rows"])
        self.assertEqual(len(self.remote.attachments),1)
        for row in done["rows"]:
            self.assertEqual(len(self.remote.get(row["record_id"])["fields"]["上下电确认截图"]),1)

    def test_empty_image_batch_can_be_permanently_deleted(self):
        batch=self.service.batches.create_image_batch("owner",["A"],"A")
        result=self.service.batches.delete_empty(batch["batch_id"],"owner",expected_version=batch["version"])
        self.assertTrue(result["deleted"])
        with self.assertRaisesRegex(CabinetError,"批次不存在"):
            self.service.batches.get(batch["batch_id"])

    def test_image_registration_keeps_out_of_scope_candidates_unsubmitted(self):
        from PIL import Image
        batch=self.service.batches.create_image_batch("owner",["A"])
        content=io.BytesIO();Image.new("RGB",(80,60),"white").save(content,format="PNG")
        rack=self.configs["B"]["inventory"][0]
        candidate={"scope":"B","room":rack["room"],"rack":rack["rack"],"supplier_rack":"",
                   "action":"上正式电","expected":"2026-09-14 01:02:03","actual":"2026-09-14 01:02:03","result":"成功"}
        with patch("bin.lan_bitable_template_portal.cabinet_power_evidence.recognize_image_with_timeout",return_value=[candidate]):
            self.service.batches.add_images(batch["batch_id"],[("wrong-building.png",content.getvalue())],"owner",["A"])
            deadline=time.time()+5
            while time.time()<deadline and self.service.batches.get(batch["batch_id"])["images"][0]["status"]=="recognizing":time.sleep(.01)
        batch=self.service.batches.get(batch["batch_id"])
        self.assertEqual(batch["rows"],[])
        self.assertEqual(batch["images"][0]["suggestions"][0]["status"],"unauthorized")

    def test_image_registration_keeps_distinct_events_for_same_cabinet(self):
        from PIL import Image
        rack=self.configs["A"]["inventory"][0]
        batch=self.service.batches.create_image_batch("owner",["A"])
        files=[]
        for color in ("white","gray"):
            content=io.BytesIO();Image.new("RGB",(80,60),color).save(content,format="PNG")
            files.append((color+".png",content.getvalue()))
        candidates=[{"scope":"A","room":rack["room"],"rack":rack["rack"],"supplier_rack":"",
                     "action":"上正式电","expected":stamp,"actual":stamp,"result":"成功"}
                    for stamp in ("2026-09-14 01:02:03","2026-09-15 01:02:03")]
        with patch("bin.lan_bitable_template_portal.cabinet_power_evidence.recognize_image_with_timeout",side_effect=[[item] for item in candidates]):
            self.service.batches.add_images(batch["batch_id"],files,"owner",["A"])
            deadline=time.time()+5
            while time.time()<deadline and any(item["status"]=="recognizing" for item in self.service.batches.get(batch["batch_id"])["images"]):time.sleep(.01)
        result=self.service.batches.get(batch["batch_id"])
        self.assertEqual(len(result["rows"]),2)
        self.assertEqual({row["actual"] for row in result["rows"]},{item["actual"] for item in candidates})
        self.assertEqual({row["evidence_images"][0] for row in result["rows"]},{item["image_id"] for item in result["images"]})

    def test_todo_batch_list_and_badge_are_scoped_to_building(self):
        a=self.service.batches.create_manual([self._manual_batch_row("A","2026-09-14 01:02:03")],"owner")
        b=self.service.batches.create_manual([self._manual_batch_row("B","2026-09-14 01:02:03")],"owner")
        pending_a=self.service.batches.list("owner",["A","B"],scope="A",status="todo")
        pending_b=self.service.batches.list("owner",["A","B"],scope="B",status="todo")
        self.assertEqual(([item["batch_id"] for item in pending_a["items"]],pending_a["pending_count"]),([a["batch_id"]],1))
        self.assertEqual(([item["batch_id"] for item in pending_b["items"]],pending_b["pending_count"]),([b["batch_id"]],1))
        image=self.service.batches.create_image_batch("owner",["A","B"],"A")
        self.assertEqual(self.service.batches.list("owner",["A","B"],scope="A",status="todo")["pending_count"],2)
        self.assertNotIn(image["batch_id"],[item["batch_id"] for item in self.service.batches.list("owner",["A","B"],scope="B")["items"]])
        mixed=self.service.batches.create_manual([self._manual_batch_row(scope,"2026-09-14 01:05:03") for scope in ("A","B")],"owner")
        limited=next(item for item in self.service.batches.list("viewer",["A"],scope="A")["items"] if item["batch_id"]==mixed["batch_id"])
        self.assertEqual((limited["scopes"],limited["stats"]["total"],limited["pending_rows"]),(["A"],1,1))
        self.service.batches.confirm(a["batch_id"],{"version":a["version"],"all":True},"owner",["A"])
        self._wait_batch(a["batch_id"])
        self.assertEqual(self.service.batches.list("owner",["A","B"],scope="A",status="todo")["pending_count"],2)

    def test_notice_rollback_error_filter_does_not_leak_other_building(self):
        service=self.service.batches
        batch=service.create_from_notice({"target_record_id":"rec-mixed-notice","notice_type":"上电通告",
            "scope":"CAMPUS","cabinet":"A-202包间B01、B-202包间B01","quantity":"2","owner_id":"owner"})
        stored=service.store.get(batch["batch_id"])
        target=next(row for row in stored["rows"] if row["scope"]=="B")
        target.update(status="rollback_blocked",error="后续操作")
        stored["source_notice"]["deleted_at"]="2026-09-18 19:00:00"
        service.store.save(stored,stored["version"])
        self.assertEqual(service.list("viewer",["A"],status="notice_rollback_error")["total"],0)
        self.assertEqual(service.list("owner",["A","B"],status="notice_rollback_error")["total"],1)

    def test_cancelled_batch_detail_and_selected_restore_stay_consistent(self):
        racks=self.configs["A"]["inventory"][:2]
        rows=[{"scope":"A","room":rack["room"],"rack":rack["rack"],"rack_type":rack["rack_type"],
               "action":"上正式电","expected":"2026-09-14 01:02:03","actual":"2026-09-14 01:02:03","result":"成功"}
              for rack in racks]
        batch=self.service.batches.create_manual(rows,"owner")
        cancelled=self.service.batches.cancel(batch["batch_id"],"owner",expected_version=batch["version"])
        self.assertEqual(cancelled["status"],"cancelled")
        self.assertEqual([row["status"] for row in self.service.batches.get(batch["batch_id"])["rows"]],["excluded_cancelled"]*2)
        listing=self.service.batches.list("owner",["A"],scope="A")
        self.assertEqual(next(item for item in listing["items"] if item["batch_id"]==batch["batch_id"])["status"],"cancelled")
        self.assertTrue(all(row["restorable"] for row in self.service.batches.visible(cancelled,"owner",["A"])["rows"]))
        first=cancelled["rows"][0]["row_id"]
        restored=self.service.batches.restore_rows(batch["batch_id"],{"version":cancelled["version"],"row_ids":[first]},"owner",["A"])
        self.assertEqual(restored["status"],"pending")
        self.assertEqual([row["status"] for row in restored["rows"]],["ready","excluded_cancelled"])
        self.assertEqual(self.service.batches.list("owner",["A"],scope="A",status="todo")["pending_count"],1)
        restored=self.service.batches.restore_rows(batch["batch_id"],{"version":restored["version"],"row_ids":[restored["rows"][1]["row_id"]]},"owner",["A"])
        self.assertEqual([row["status"] for row in restored["rows"]],["ready","ready"])

    def test_cancel_during_image_ocr_does_not_recreate_rows(self):
        from PIL import Image
        batch=self.service.batches.create_image_batch("owner",["A"])
        content=io.BytesIO();Image.new("RGB",(80,60),"white").save(content,format="PNG")
        rack=self.configs["A"]["inventory"][0]
        candidate={"scope":"A","room":rack["room"],"rack":rack["rack"],"supplier_rack":"",
                   "action":"上正式电","expected":"2026-09-14 01:02:03","actual":"2026-09-14 01:02:03","result":"成功"}
        entered=threading.Event();release=threading.Event()
        def slow_ocr(_content):
            entered.set();release.wait(5);return [candidate]
        with patch("bin.lan_bitable_template_portal.cabinet_power_evidence.recognize_image_with_timeout",side_effect=slow_ocr):
            self.service.batches.add_images(batch["batch_id"],[("mail.png",content.getvalue())],"owner",["A"])
            self.assertTrue(entered.wait(5))
            current=self.service.batches.get(batch["batch_id"])
            cancelled=self.service.batches.cancel(batch["batch_id"],"owner",expected_version=current["version"])
            release.set()
            self.service.batches._ocr_pool.shutdown(wait=True)
        final=self.service.batches.get(batch["batch_id"])
        self.assertEqual((cancelled["status"],final["status"],final["rows"],final["images"][0]["status"]),
                         ("cancelled","cancelled",[],"cancelled"))

    def test_cancel_rejects_unverified_write(self):
        batch=self.service.batches.create_manual([self._manual_batch_row("A","2026-09-14 01:02:03")],"owner")
        def pending(current):current["rows"][0].update(status="failed",operation_started=True)
        batch=self.service.batches._change(batch["batch_id"],pending)
        with self.assertRaisesRegex(CabinetError,"未核验"):
            self.service.batches.cancel(batch["batch_id"],"owner",expected_version=batch["version"])

    def test_cancel_then_restore_preserves_rolled_back_attempt(self):
        batch=self.service.batches.create_manual([self._manual_batch_row("A","2026-09-14 01:02:03")],"owner")
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A"])
        done=self._wait_batch(batch["batch_id"])
        self.service.batches.rollback(batch["batch_id"],{"version":done["version"],"all":True},"owner",["A"])
        undone=self._wait_batch(batch["batch_id"])
        old_id=undone["rows"][0]["operation_id"]
        cancelled=self.service.batches.cancel(batch["batch_id"],"owner",expected_version=undone["version"])
        self.assertEqual(cancelled["rows"][0]["status"],"excluded_cancelled")
        restored=self.service.batches.restore_rows(batch["batch_id"],{"version":cancelled["version"],"row_ids":[cancelled["rows"][0]["row_id"]]},"owner",["A"])
        self.assertEqual(restored["rows"][0]["status"],"rolled_back")
        self.service.batches.confirm(batch["batch_id"],{"version":restored["version"],"all":True},"owner",["A"])
        again=self._wait_batch(batch["batch_id"])
        self.assertEqual(again["stats"]["completed"],1,again["rows"])
        self.assertNotEqual(again["rows"][0]["operation_id"],old_id)

    def test_image_registration_validates_sequential_events_on_same_cabinet(self):
        from PIL import Image
        rack=self.configs["A"]["inventory"][0]
        current=next(item for item in self.service.overview("A")["racks"] if (item["room"],item["rack"])==(rack["room"],rack["rack"]))
        first,second={"formal":("下正式电","上正式电"),"test":("下测试电","上测试电"),"off":("上正式电","下正式电")}[current["state"]]
        candidates=[{"scope":"A","room":rack["room"],"rack":rack["rack"],"supplier_rack":"",
                     "action":action,"expected":stamp,"actual":stamp,"result":"成功"}
                    for action,stamp in ((first,"2026-09-18 00:01:00"),(second,"2026-09-18 00:02:00"))]
        files=[]
        for color in ("white","gray"):
            content=io.BytesIO();Image.new("RGB",(80,60),color).save(content,format="PNG")
            files.append((color+".png",content.getvalue()))
        batch=self.service.batches.create_image_batch("owner",["A"])
        with patch("bin.lan_bitable_template_portal.cabinet_power_evidence.recognize_image_with_timeout",side_effect=[[item] for item in candidates]):
            self.service.batches.add_images(batch["batch_id"],files,"owner",["A"])
            deadline=time.time()+5
            while time.time()<deadline and any(item["status"]=="recognizing" for item in self.service.batches.get(batch["batch_id"])["images"]):time.sleep(.01)
        result=self.service.batches.get(batch["batch_id"])
        self.assertEqual(result["stats"]["confirmable"],2,result["rows"])
        self.assertEqual(result["rows"][1]["current_power_state"],{"下正式电":"off","下测试电":"off","上正式电":"formal"}[first])

    def test_deleted_photo_cannot_be_restored_into_completed_record(self):
        from PIL import Image
        rack=self.configs["A"]["inventory"][0]
        row=self._manual_batch_row("A","2026-09-14 01:02:03")
        batch=self.service.batches.create_manual([row],"owner")
        content=io.BytesIO();Image.new("RGB",(80,60),"white").save(content,format="PNG")
        candidate={"scope":"A","room":rack["room"],"rack":rack["rack"],"supplier_rack":"",
                   "action":row["action"],"expected":row["expected"],"actual":row["actual"]}
        with patch("bin.lan_bitable_template_portal.cabinet_power_evidence.recognize_image_with_timeout",return_value=[candidate]):
            self.service.batches.add_images(batch["batch_id"],[("proof.png",content.getvalue())],"owner",["A"])
            deadline=time.time()+5
            while time.time()<deadline and self.service.batches.get(batch["batch_id"])["images"][0]["status"]=="recognizing":time.sleep(.01)
        batch=self.service.batches.get(batch["batch_id"])
        image_id=batch["images"][0]["image_id"]
        removed=self.service.batches.delete_image(batch["batch_id"],image_id,batch["version"],"owner",["A"])
        self.service.batches.confirm(batch["batch_id"],{"version":removed["version"],"all":True},"owner",["A"])
        done=self._wait_batch(batch["batch_id"])
        self.assertEqual(done["stats"]["completed"],1,done["rows"])
        with self.assertRaisesRegex(CabinetError,"先回退"):
            self.service.batches.restore_image(batch["batch_id"],image_id,done["version"],"owner",["A"])

    def test_batch_confirm_uses_one_cloud_write_per_building_and_rolls_back(self):
        rows=[]
        for scope in ("A","E"):
            for rack in self.configs[scope]["inventory"][:2]:
                rows.append({"scope":scope,"room":rack["room"],"rack":rack["rack"],
                             "rack_type":rack["rack_type"],"action":"上正式电",
                             "expected":"2026-09-14 01:02:03","actual":"2026-09-14 01:02:03","result":"成功"})
        batch=self.service.batches.create_manual(rows,"owner")
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A","E"])
        done=self._wait_batch(batch["batch_id"])
        self.assertEqual(done["stats"]["completed"],4,done["rows"])
        self.assertEqual((self.remote.batch_create_calls,self.remote.batch_update_calls),(1,1))
        original_update=self.remote.update
        def checked_update(record_id,fields):
            for field in ("机柜功率（W）","来源行号"):
                if fields.get(field) is not None:
                    self.assertIsInstance(fields[field],(int,float),field)
            return original_update(record_id,fields)
        self.remote.update=checked_update
        self.service.batches.rollback(batch["batch_id"],{"version":done["version"],"all":True},"owner",["A","E"])
        undone=self._wait_batch(batch["batch_id"])
        self.assertEqual(undone["stats"]["rolled_back"],4,undone["rows"])
        self.assertEqual(undone["status"],"rolled_back")

    def test_batch_create_lost_response_reconciles_without_duplicate(self):
        rows=[]
        for rack in self.configs["A"]["inventory"][:2]:
            rows.append({"scope":"A","room":rack["room"],"rack":rack["rack"],
                         "rack_type":rack["rack_type"],"action":"上正式电",
                         "expected":"2026-09-14 01:02:03","actual":"2026-09-14 01:02:03","result":"成功"})
        batch=self.service.batches.create_manual(rows,"owner")
        self.remote.fail_after_batch_create=True
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A"])
        done=self._wait_batch(batch["batch_id"])
        self.assertEqual(done["stats"]["completed"],2,done["rows"])
        self.assertEqual(self.remote.batch_create_calls,1)
        self.assertEqual(self.remote.creates,2)

    def test_batch_update_lost_response_reconciles_each_cabinet(self):
        rows=[]
        for rack in self.configs["E"]["inventory"][:2]:
            rows.append({"scope":"E","room":rack["room"],"rack":rack["rack"],
                         "rack_type":rack["rack_type"],"action":"下正式电",
                         "expected":"2026-09-14 01:02:03","actual":"2026-09-14 01:02:03","result":"成功"})
        batch=self.service.batches.create_manual(rows,"owner")
        before_count=len(self.remote.records)
        self.remote.fail_after_batch_update=True
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["E"])
        done=self._wait_batch(batch["batch_id"])
        self.assertEqual(done["stats"]["completed"],2,done["rows"])
        self.assertEqual(self.remote.batch_update_calls,1)
        self.assertEqual(len(self.remote.records),before_count)

    def test_all_failed_rollbacks_are_not_reported_as_partial(self):
        batch={"status":"partial","rows":[{"status":"rollback_failed","scope":"E"},
                                          {"status":"rollback_blocked","scope":"E"}]}
        self.service.batches._refresh_summary(batch)
        self.assertEqual(batch["status"],"failed")
        self.assertEqual(batch["stats"]["rolled_back"],0)

    def test_one_failed_rollback_does_not_block_other_cabinet(self):
        rows=[]
        for rack in self.configs["E"]["inventory"][:2]:
            rows.append({"scope":"E","room":rack["room"],"rack":rack["rack"],
                         "rack_type":rack["rack_type"],"action":"下正式电",
                         "expected":"2026-09-14 01:02:03","actual":"2026-09-14 01:02:03","result":"成功"})
        batch=self.service.batches.create_manual(rows,"owner")
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["E"])
        done=self._wait_batch(batch["batch_id"])
        self.assertEqual(done["stats"]["completed"],2)
        blocked_id=done["rows"][-1]["record_id"]
        original_update=self.remote.update
        def fail_one(record_id,fields):
            if record_id==blocked_id: raise CabinetError("模拟数字字段写入失败")
            return original_update(record_id,fields)
        self.remote.update=fail_one
        self.service.batches.rollback(batch["batch_id"],{"version":done["version"],"all":True},"owner",["E"])
        partial=self._wait_batch(batch["batch_id"])
        self.assertEqual(partial["stats"]["rollback_failed"],1,partial["rows"])
        self.assertEqual(partial["stats"]["rolled_back"],1,partial["rows"])
        self.remote.update=original_update
        self.service.batches.rollback(batch["batch_id"],{"version":partial["version"],"all":True},"owner",["E"])
        self.assertEqual(self._wait_batch(batch["batch_id"])["stats"]["rolled_back"],2)

    def test_batch_rollback_skips_cabinet_with_later_batch(self):
        first=self.service.batches.create_manual([self._manual_batch_row("A","2026-09-14 01:02:03")],"owner")
        self.service.batches.confirm(first["batch_id"],{"version":first["version"],"all":True},"owner",["A"])
        first=self._wait_batch(first["batch_id"])
        second=self.service.batches.create_manual([self._manual_batch_row("A","2026-09-14 01:03:03", "下正式电")],"owner")
        self.service.batches.confirm(second["batch_id"],{"version":second["version"],"all":True},"owner",["A"])
        second=self._wait_batch(second["batch_id"])
        self.service.batches.rollback(first["batch_id"],{"version":first["version"],"all":True},"owner",["A"])
        first=self._wait_batch(first["batch_id"])
        self.assertEqual(first["rows"][0]["status"],"rollback_blocked")
        self.assertIn(first["rows"][0]["record_id"],self.remote.records)
        self.service.batches.rollback(second["batch_id"],{"version":second["version"],"all":True},"owner",["A"])
        self.assertEqual(self._wait_batch(second["batch_id"])["rows"][0]["status"],"rolled_back")
        self.service.batches.rollback(first["batch_id"],{"version":first["version"],"all":True},"owner",["A"])
        self.assertEqual(self._wait_batch(first["batch_id"])["rows"][0]["status"],"rolled_back")

    def test_batch_rollback_recovers_lost_delete_response(self):
        batch=self.service.batches.create_manual([self._manual_batch_row("A","2026-09-14 01:02:03")],"owner")
        lists_before=self.remote.list_calls
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A"])
        batch=self._wait_batch(batch["batch_id"])
        self.assertEqual(self.remote.list_calls,lists_before,"首次创建不应先扫描飞书")
        self.remote.fail_after_delete=True
        self.service.batches.rollback(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A"])
        batch=self._wait_batch(batch["batch_id"])
        self.assertEqual(batch["rows"][0]["status"],"rollback_failed")
        self.remote.fail_after_delete=False
        self.service.batches.rollback(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["A"])
        batch=self._wait_batch(batch["batch_id"])
        self.assertEqual(batch["rows"][0]["status"],"rolled_back",batch["rows"][0])

    def test_batch_rollback_restores_directory_type_and_rejects_cloud_edits(self):
        row=self._manual_batch_row("D","2026-09-14 01:02:03")
        original_type=row["rack_type"]
        row["rack_type"]="网络机柜" if original_type=="服务器机柜" else "服务器机柜"
        row["type_resolution"]="sync_current"
        batch=self.service.batches.create_manual([row],"owner")
        self.assertEqual(batch["rows"][0]["status"],"ready",batch["rows"][0])
        self.service.batches.confirm(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["D"])
        batch=self._wait_batch(batch["batch_id"])
        self.assertEqual(batch["rows"][0]["status"],"completed",batch["rows"][0])
        self.assertEqual(next(item["rack_type"] for item in self.service._snapshot("D")["config"]["inventory"] if (item["room"],item["rack"])==(row["room"],row["rack"])),row["rack_type"])
        self.service.batches.rollback(batch["batch_id"],{"version":batch["version"],"all":True},"owner",["D"])
        batch=self._wait_batch(batch["batch_id"])
        self.assertEqual(batch["rows"][0]["status"],"rolled_back",batch["rows"][0])
        self.assertEqual(next(item["rack_type"] for item in self.service._snapshot("D")["config"]["inventory"] if (item["room"],item["rack"])==(row["room"],row["rack"])),original_type)

        other=self.service.batches.create_manual([self._manual_batch_row("A","2026-09-14 01:02:03")],"owner")
        self.service.batches.confirm(other["batch_id"],{"version":other["version"],"all":True},"owner",["A"])
        other=self._wait_batch(other["batch_id"])
        rid=other["rows"][0]["record_id"]
        self.remote.records[rid]["fields"]["操作类型"]="下正式电"
        self.service.batches.rollback(other["batch_id"],{"version":other["version"],"all":True},"owner",["A"])
        other=self._wait_batch(other["batch_id"])
        self.assertEqual(other["rows"][0]["status"],"rollback_blocked")
        self.assertIn(rid,self.remote.records)

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
            self.assertEqual(list(after.sheets),["机柜上电汇总表（邮件）","机柜上电汇总表（每月阿里统计）",*(name for name in before.sheets if name!="机柜上电汇总表")])
            self.assertEqual(before.archive.read("xl/vbaProject.bin"),after.archive.read("xl/vbaProject.bin"))
            for name in before.sheets:
                original_merges=[x.get("ref") for x in before.sheet(name).iter(T("mergeCell"))]
                exported_merges=[x.get("ref") for x in after.sheet("机柜上电汇总表（邮件）" if name=="机柜上电汇总表" else name).iter(T("mergeCell"))]
                self.assertTrue(set(original_merges)<=set(exported_merges),name)
                if "平面图" not in name: self.assertEqual(original_merges,exported_merges)
            for fmt in self.models[scope]["formats"]:
                name=fmt["sheet"]; raw=dict(before.rows(name)); actual=dict(after.rows(name))
                for rn,row in raw.items():
                    if rn<=fmt["header"] or not row.get(fmt["rack"]): continue
                    for col,value in row.items():
                        self.assertEqual(actual[rn].get(col,""),value,(scope,name,rn,col))
            checked=load_workbook(io.BytesIO(result),keep_vba=True,data_only=True); checked.close()

    def test_new_cycle_rows_use_real_table_end_and_fill_mail_periods(self):
        scope="B"; snapshot=self.service._snapshot(scope); config=snapshot["config"]
        rack=next(item for item in derive_records(config,snapshot["operations"])["racks"] if item["state"]=="off")
        group={"id":"event_new_floorplan_up","action":"上正式电","expected":"2026-09-20 11:00:00",
               "actual":"2026-09-20 10:00:00","result":"成功"}
        created=self.service.validate_op(scope,{"operation_id":"floorplan_new_up_1234","room":rack["room"],
            "rack":rack["rack"],"rack_type":rack["rack_type"],"groups":[group],"result":"成功","category":"up"})
        fields=to_fields(created); fields.update({"来源工作表":created["source"],"数据标识":"manual_floorplan_new_up"})
        operation=from_feishu({"record_id":"recFloorplanNewUp","fields":fields})
        workbook=Workbook(export_workbook((TEMPLATES/"B.xlsm").read_bytes(),config,[*snapshot["operations"],operation]))
        rows=dict(workbook.rows("机柜上电时间统计"))
        added=[(rn,row) for rn,row in rows.items() if dates(row.get(7))==["2026-09-20 10:00:00"]]
        self.assertEqual(len(added),1)
        self.assertEqual(added[0][0],1024)
        self.assertEqual(added[0][1][1],1023.0)
        self.assertEqual(max(rows),1024)
        mail=dict(workbook.rows("机柜上电汇总表（邮件）"))
        day=next(row for row in mail.values() if str(row.get(13))=="2026.9.20")
        self.assertEqual(day[14],1.0)
        self.assertFalse(any(str(value).startswith("系统新增上下电") for row in mail.values() for value in row.values()))
        notice=dict(workbook.rows("机柜上电汇总表（每月阿里统计）"))
        notice_day=next(row for row in notice.values() if str(row.get(13))=="2026.9.20")
        self.assertEqual(notice_day[14],1.0)

        notice_operation=copy.deepcopy(operation)
        notice_operation["meta"]["batch_rows"]=[{"source":"notice","source_notice":{"target_record_id":"recNotice"}}]
        notice_summary={"items":[{"room":rack["room"],"rack":rack["rack"],"action":"上正式电",
            "date":"2026-09-20","sent_at":"2026-09-20 10:00:00","direction":"up"}]}
        deduplicated=Workbook(export_workbook((TEMPLATES/"B.xlsm").read_bytes(),config,
            [*snapshot["operations"],notice_operation],notice_summary))
        notice_day=next(row for row in dict(deduplicated.rows("机柜上电汇总表（每月阿里统计）")).values()
                        if str(row.get(13))=="2026.9.20")
        self.assertEqual(notice_day[14],1.0)

    def test_manual_conversion_updates_both_sheet_stock_without_counting_an_event(self):
        scope="C"; snapshot=self.service._snapshot(scope); config=snapshot["config"]
        config=copy.deepcopy(config)
        config["power_baseline"]=map_state_baseline((TEMPLATES/"C.xlsm").read_bytes(),config,snapshot["operations"])[0]
        baseline=config["power_baseline"]
        current={(item["room"],item["rack"]):item["state"] for item in derive_records(config,snapshot["operations"])["racks"]}
        room_rack=next(tuple(key.split("/",1)) for key,value in baseline.items()
                       if value.get("state")=="formal" and current.get(tuple(key.split("/",1)))=="formal")
        rack=next(item for item in config["inventory"] if (item["room"],item["rack"])==room_rack)
        original=Workbook(export_workbook((TEMPLATES/"C.xlsm").read_bytes(),config,snapshot["operations"]))
        item=self.service.validate_op(scope,{"operation_id":"manual_transition_1234","room":rack["room"],
            "rack":rack["rack"],"rack_type":rack["rack_type"],"groups":[{"id":"transition_event",
            "action":"正式电转测试电","expected":"2026-09-20 12:00:00","actual":"2026-09-20 12:01:00",
            "result":"成功"}],"result":"成功","category":"up"})
        fields=to_fields(item); fields.update({"来源工作表":item["source"],"数据标识":"manual_transition_record"})
        operation=from_feishu({"record_id":"recManualTransition","fields":fields})
        workbook=Workbook(export_workbook((TEMPLATES/"C.xlsm").read_bytes(),config,[*snapshot["operations"],operation]))

        def metric(book,sheet,label):
            rows=dict(book.rows(sheet))
            row=next(value for value in rows.values() if any(label in str(cell) for cell in value.values()))
            column=next(column for column,value in row.items() if label in str(value))
            return next(float(row[index]) for index in range(column+1,column+5) if isinstance(row.get(index),(int,float)))

        for sheet in ("机柜上电汇总表（邮件）","机柜上电汇总表（每月阿里统计）"):
            self.assertEqual(metric(workbook,sheet,"测试电总数"),metric(original,sheet,"测试电总数")+1)
            self.assertEqual(metric(workbook,sheet,"正式电总数"),metric(original,sheet,"正式电总数")-1)
            before=sum("2026.9.20" in str(value) for row in original.rows(sheet) for value in row[1].values())
            after=sum("2026.9.20" in str(value) for row in workbook.rows(sheet) for value in row[1].values())
            self.assertEqual(after,before)

    def test_abc_cycle_history_fits_original_up_and_down_columns(self):
        scope="A"; snapshot=self.service._snapshot(scope); config=snapshot["config"]
        rack=next(item for item in derive_records(config,snapshot["operations"])["racks"] if item["state"]=="off")
        groups=[
            {"id":"cycle_up","action":"上正式电","expected":"2026-09-20 09:00:00","actual":"2026-09-20 09:01:00","result":"成功"},
            {"id":"cycle_to_test","action":"正式电转测试电","expected":"2026-09-20 10:00:00","actual":"2026-09-20 10:01:00","result":"成功"},
            {"id":"cycle_to_formal","action":"测试电转正式电","expected":"2026-09-20 11:00:00","actual":"2026-09-20 11:01:00","result":"成功"},
        ]
        def operation(record_id,category,items):
            payload={"operation_id":record_id+"_operation","room":rack["room"],"rack":rack["rack"],
                     "rack_type":rack["rack_type"],"groups":items,"result":"成功","category":category}
            item=self.service.validate_op(scope,payload); fields=to_fields(item)
            fields.update({"来源工作表":item["source"],"数据标识":"manual_"+record_id})
            return from_feishu({"record_id":record_id,"fields":fields})
        up=operation("recCycleUp","up",groups)
        down_group={"id":"cycle_down","action":"下正式电","expected":"2026-09-20 12:00:00","actual":"2026-09-20 12:01:00","result":"成功"}
        down=operation("recCycleDown","down",[*groups,down_group])
        workbook=Workbook(export_workbook((TEMPLATES/"A.xlsm").read_bytes(),config,[*snapshot["operations"],up,down]))
        up_row=next(row for row in dict(workbook.rows("机柜上电时间统计")).values() if dates(row.get(7))==["2026-09-20 09:01:00"])
        self.assertEqual(up_row[5],"上正式电")
        self.assertIn("正式电转测试电",str(up_row[8])); self.assertIn("测试电转正式电",str(up_row[8]))
        down_row=next(row for row in dict(workbook.rows("机柜下电时间统计")).values() if dates(row.get(7))==["2026-09-20 09:01:00"])
        self.assertEqual(down_row[5],"上正式电")
        self.assertIn("正式电转测试电",str(down_row[8])); self.assertIn("测试电转正式电",str(down_row[8]))
        self.assertEqual(down_row[10],"下正式电")

    def test_saving_an_empty_existing_record_deletes_cloud_and_local_rows(self):
        rack=self.configs["A"]["inventory"][0]
        created=self.service.save_operation("A",{"operation_id":"create_then_delete_01","room":rack["room"],
            "rack":rack["rack"],"rack_type":rack["rack_type"],"result":"成功","groups":[{
            "id":"created_event","action":"上正式电","expected":"2026-09-20 09:00:00",
            "actual":"2026-09-20 09:01:00","result":"成功"}]},"owner")
        record_id=created["record_id"]
        deleted=self.service.save_operation("A",{"operation_id":"delete_existing_record_01",
            "expected_version":created["version"],"groups":[{"id":"created_event","action":"","expected":"","actual":"","result":"成功"}]},
            "owner",record_id)
        self.assertTrue(deleted["deleted"])
        self.assertNotIn(record_id,self.remote.records)
        self.assertFalse(any(item["record_id"]==record_id for item in self.service._snapshot("A")["operations"]))

    def test_abc_batch_conversion_updates_active_row_and_down_copies_cycle(self):
        snapshot=self.service._snapshot("A"); state=derive_records(snapshot["config"],snapshot["operations"])
        rack=next(item for item in state["racks"] if item["state"] in ("formal","test") and item["last_operation"])
        latest=max(((event,operation) for operation in snapshot["operations"]
                    if (operation["room"],operation["rack"])==(rack["room"],rack["rack"])
                    for event in operation["events"] if completed_state_event(event)),
                   key=lambda item:(item[0]["actual"],item[0]["id"]))
        batch={"batch_id":"batch_cycle_test","source":"manual","images":[]}
        base={"scope":"A","room":rack["room"],"rack":rack["rack"],"rack_type":rack["rack_type"],
              "supplier_rack":"","type_resolution":"keep_current","expected":"2026-09-20 20:00:00",
              "actual":"2026-09-20 20:01:00","result":"成功","failure_reason":"","attempts":[],
              "file_name":"","file_sha256":"","application_ids":[],"page":0,"source_row":1,"edits":[]}
        conversion={**base,"row_id":"row_conversion","operation_id":"operation_conversion_01",
                    "action":"正式电转测试电" if rack["state"]=="formal" else "测试电转正式电"}
        (request,record_id),duplicate=self.service.batches._row_payload(batch,conversion)
        self.assertEqual(duplicate,"")
        self.assertEqual(record_id,latest[1]["record_id"])
        self.assertEqual(request["groups"][-1]["action"],conversion["action"])
        down={**base,"row_id":"row_down","operation_id":"operation_down_cycle_01",
              "action":"下正式电" if rack["state"]=="formal" else "下测试电"}
        (request,record_id),duplicate=self.service.batches._row_payload(batch,down)
        self.assertEqual((record_id,duplicate),("",""))
        self.assertEqual(request["groups"][-1]["action"],down["action"])
        self.assertGreater(len(request["groups"]),1)

    def test_exported_summary_formula_inputs_keep_empty_counts_numeric_blank(self):
        scope="A"; original=(TEMPLATES/(scope+".xlsm")).read_bytes()
        ops=[from_feishu(record) for record in self.source_records if record["fields"]["楼栋"]==scope+"楼"]
        exported=Workbook(export_workbook(original,self.configs[scope],ops))
        for sheet in ("机柜上电汇总表（邮件）","机柜上电汇总表（每月阿里统计）"):
            cells=exported.cells(sheet)
            self.assertIsNone(cells["E15"].get("t"),(sheet,"E15"))
            self.assertIsNone(cells["E15"].find(T("is")),(sheet,"E15"))
            self.assertEqual(cells["F15"].findtext(T("f")),"F14+C15-E15")

    def test_notice_export_counts_by_start_date_and_keeps_carrier_rooms_separate(self):
        scope="B"; original=(TEMPLATES/(scope+".xlsm")).read_bytes()
        ops=[from_feishu(r) for r in self.source_records if r["fields"]["楼栋"]==scope+"楼"]
        summary={"items":[
            {"room":"216","rack":"B04","date":"2026-09-03","direction":"up"},
            {"room":"247","rack":"B05","date":"2026-09-03","direction":"up"},
            {"room":"402","rack":"B15","date":"2026-09-04","direction":"down"},
        ]}
        book=Workbook(export_workbook(original,self.configs[scope],ops,summary))
        rows=dict(book.rows("机柜上电汇总表（每月阿里统计）"))
        by_name={str(values.get(1)):values for values in rows.values() if values.get(1)}
        self.assertIn("B-216运营商机房",by_name)
        self.assertIn("B-247运营商机房",by_name)
        self.assertEqual((rows[7][14],rows[7][15],rows[7][16]),(6.0,12.0,1022.0))
        self.assertEqual((rows[8].get(14,""),rows[8][15],rows[8][16]),("",1.0,1022.0))

    def test_finished_notice_reaches_both_download_sheets_and_archive(self):
        archive=FakeExportFeishu()
        self.service.export_remote=archive
        source={"target_record_id":"rec-export-chain","notice_type":"上电通告","scope":"B",
                "cabinet":"B-216运营商机房B04、B-247运营商机房B05","quantity":"2","owner_id":"owner",
                "sent_at":"2026-09-18 18:05:00"}
        self.service.batches.apply_notice_event(source)
        self.service.batches.apply_notice_event({**source,"event_action":"end","sent_at":"2026-09-18 19:05:00"})
        job=self.service.job("B","export","owner",{"batch_id":"all_"+"d"*32})
        deadline=time.time()+30
        while time.time()<deadline:
            state=self.service.job_status(job["job_id"])
            if state["status"] in ("succeeded","failed"): break
            time.sleep(.02)
        self.assertEqual(state["status"],"succeeded",state.get("error"))
        result=state["result"]
        self.assertEqual(result["cloud_upload_status"],"succeeded")
        saved=self.service.local.document("B","export:"+result["export_id"])
        workbook=Workbook(Path(saved["path"]).read_bytes())
        self.assertIn("机柜上电汇总表（邮件）",workbook.sheets)
        notice=dict(workbook.rows("机柜上电汇总表（每月阿里统计）"))
        added=next(values for values in notice.values() if values.get(13)=="2026.9.18")
        self.assertEqual((added[14],added.get(15,""),added[16]),(2.0,"",1022.0))
        self.assertEqual(len(archive.records),1)
        self.assertEqual(next(iter(archive.records.values()))["fields"]["文件SHA256"],result["sha256"])

    def test_notice_counts_do_not_change_mail_sheet_for_any_building(self):
        for scope in "ABCDE":
            with self.subTest(scope=scope):
                original=(TEMPLATES/(scope+".xlsm")).read_bytes()
                ops=[from_feishu(record) for record in self.source_records if record["fields"]["楼栋"]==scope+"楼"]
                baseline=map_state_baseline(original,self.configs[scope],ops)[0]
                room_rack=next((key.split("/") for key,state in baseline.items() if state["state"]=="off"),None)
                self.assertIsNotNone(room_rack,scope)
                room=next(item for item in self.configs[scope]["rooms"] if item["id"]==room_rack[0])
                rack=room_rack[1]
                items=[{"room":room["id"],"rack":rack,"date":"2026-09-17","direction":"up"},
                       {"room":room["id"],"rack":rack,"date":"2026-09-18","direction":"down"}]
                empty=Workbook(export_workbook(original,self.configs[scope],ops))
                filled=Workbook(export_workbook(original,self.configs[scope],ops,{"items":items}))
                mail="机柜上电汇总表（邮件）"
                def stable_mail(book):
                    root=book.sheet(mail)
                    for row in root.iter(T("row")):
                        cells=list(row)
                        for index,cell in enumerate(cells[:-1]):
                            if book.value(cell)=="数据更新":
                                row.remove(cells[index+1])
                    return ET.tostring(root)
                self.assertEqual(stable_mail(empty),stable_mail(filled))
                mail_root=filled.sheet("机柜上电汇总表（邮件）")
                notice_root=filled.sheet("机柜上电汇总表（每月阿里统计）")
                for tag in ("sheetPr","sheetFormatPr","printOptions","pageMargins","pageSetup","headerFooter"):
                    mail_node,notice_node=mail_root.find(T(tag)),notice_root.find(T(tag))
                    self.assertEqual(ET.tostring(mail_node) if mail_node is not None else None,
                                     ET.tostring(notice_node) if notice_node is not None else None,(scope,tag))
                def relation_types(book,name):
                    path=book.sheets[name]; rel=path.rsplit("/",1)[0]+"/_rels/"+path.rsplit("/",1)[1]+".rels"
                    if rel not in book.archive.namelist(): return []
                    return sorted(item.get("Type","").rsplit("/",1)[-1] for item in ET.fromstring(book.archive.read(rel)))
                self.assertEqual(relation_types(filled,mail),relation_types(filled,"机柜上电汇总表（每月阿里统计）"),(scope,"relationships"))
                mail_styles={ref:cell.get("s","0") for ref,cell in filled.cells("机柜上电汇总表（邮件）").items()}
                notice_styles={ref:cell.get("s","0") for ref,cell in filled.cells("机柜上电汇总表（每月阿里统计）").items()}
                for ref,style in mail_styles.items():
                    if ref in notice_styles:
                        self.assertEqual(notice_styles[ref],style,(scope,ref))
                errors={"#VALUE!","#REF!","#NAME?","#DIV/0!","#N/A","#NUM!","#NULL!","#SPILL!","#CALC!"}
                found=[(name,ref,filled.value(cell)) for name in filled.sheets for ref,cell in filled.cells(name).items()
                       if cell.get("t")=="e" or str(filled.value(cell)).strip().upper() in errors]
                self.assertFalse(found,(scope,found[:10]))

                rows=dict(filled.rows("机柜上电汇总表（每月阿里统计）"))
                columns={"A":(2,3,5,6),"B":(13,14,15,16),"C":(27,28,29,30),"D":(2,3,4,5),"E":(2,3,4,5)}[scope]
                date_col,up_col,down_col,total_col=columns
                def date_key(value):
                    if isinstance(value,(int,float)):
                        return (dt.datetime(1899,12,30)+dt.timedelta(days=value)).strftime("%Y-%m-%d")
                    match=re.search(r"(20\d{2})\D+(\d{1,2})\D+(\d{1,2})",str(value or ""))
                    return f"{int(match[1]):04d}-{int(match[2]):02d}-{int(match[3]):02d}" if match else ""
                up_row=next(values for values in rows.values() if date_key(values.get(date_col))=="2026-09-17")
                if scope=="A":
                    down_row=next(values for values in rows.values() if date_key(values.get(4))=="2026-09-18")
                else:
                    down_row=next(values for values in rows.values() if date_key(values.get(date_col))=="2026-09-18")
                self.assertEqual(up_row[up_col],1.0)
                self.assertEqual(down_row[down_col],1.0)
                self.assertEqual(down_row[total_col],{"A":970.0,"B":1022.0,"C":970.0,"D":673.0,"E":402.0}[scope])
                month_cols={"A":(9,10,11,12),"B":(19,20,21,22),"C":(33,34,35,36),"D":(8,9,10,11),"E":(8,9,10,11)}[scope]
                month_date,month_up,month_down,month_total=month_cols
                def month_key(raw):
                    if isinstance(raw,(int,float)):
                        return (dt.datetime(1899,12,30)+dt.timedelta(days=raw)).strftime("%Y-%m")
                    match=re.search(r"(20\d{2})\D+(\d{1,2})",str(raw or ""))
                    return f"{int(match[1]):04d}-{int(match[2]):02d}" if match else ""
                empty_rows=dict(empty.rows("机柜上电汇总表（每月阿里统计）"))
                base_month=next((values for values in empty_rows.values() if month_key(values.get(month_date))=="2026-09"),None)
                filled_month=next(values for values in rows.values() if month_key(values.get(month_date))=="2026-09")
                numeric=lambda raw: float(raw) if raw not in (None,"") else 0.0
                self.assertEqual(numeric(filled_month.get(month_up)),numeric((base_month or {}).get(month_up))+1)
                self.assertEqual(numeric(filled_month.get(month_down)),numeric((base_month or {}).get(month_down))+1)
                expected_total=numeric((base_month or {}).get(month_total)) or {"A":970.0,"B":1022.0,"C":970.0,"D":673.0,"E":402.0}[scope]
                self.assertEqual(filled_month[month_total],expected_total)

    def test_e_notice_summary_extends_original_daily_and_monthly_tables(self):
        scope="E"; original=(TEMPLATES/(scope+".xlsm")).read_bytes()
        ops=[from_feishu(record) for record in self.source_records if record["fields"]["楼栋"]==scope+"楼"]
        baseline=map_state_baseline(original,self.configs[scope],ops)[0]
        targets=[key.split("/") for key,state in baseline.items() if state["state"]=="off"][:6]
        items=[{"room":room,"rack":rack,"date":"2026-09-19","direction":"up","action":"上正式电"} for room,rack in targets]
        book=Workbook(export_workbook(original,self.configs[scope],ops,{"items":items}))
        rows=dict(book.rows("机柜上电汇总表（每月阿里统计）"))
        self.assertEqual(tuple(rows[48].get(col,"") for col in (1,2,3,4,5)),(32.0,"2026.9.19",6.0,"",408.0))
        month=(dt.datetime(1899,12,30)+dt.timedelta(days=rows[32][8])).strftime("%Y-%m")
        self.assertEqual((rows[32][7],month,rows[32][9],rows[32].get(10,""),rows[32][11]),(16.0,"2026-09",6.0,"",408.0))
        mail=dict(book.rows("机柜上电汇总表（邮件）"))
        self.assertEqual((mail[10][3],rows[10][3]),(402.0,408.0))
        self.assertEqual((mail[10][4],rows[10][4]),(870.0,864.0))
        self.assertEqual(rows[10][11],408.0)
        self.assertFalse(any(str(value).startswith("系统新增上下电") for row in rows.values() for value in row.values()))
        styles=book.cells("机柜上电汇总表（每月阿里统计）")
        self.assertEqual([styles[f"{col}48"].get("s") for col in "ABCDE"],[styles[f"{col}47"].get("s") for col in "ABCDE"])
        self.assertEqual([styles[f"{col}32"].get("s") for col in "GHIJK"],[styles[f"{col}31"].get("s") for col in "GHIJK"])

    def test_notice_summary_builds_missing_month_tables_and_future_c_year(self):
        scope="A"; original=(TEMPLATES/(scope+".xlsm")).read_bytes()
        ops=[from_feishu(record) for record in self.source_records if record["fields"]["楼栋"]==scope+"楼"]
        book=Workbook(export_workbook(original,self.configs[scope],ops))
        rows=dict(book.rows("机柜上电汇总表（每月阿里统计）"))
        def month_row(period):
            for values in rows.values():
                raw=values.get(9)
                key=(dt.datetime(1899,12,30)+dt.timedelta(days=raw)).strftime("%Y-%m") if isinstance(raw,(int,float)) else (
                    f"{int(match[1]):04d}-{int(match[2]):02d}" if (match:=re.search(r"(20\d{2})\D+(\d{1,2})",str(raw or ""))) else "")
                if key==period:
                    return values
            raise AssertionError(period)
        self.assertEqual(tuple(month_row("2025-11").get(col,"") for col in (10,11,12)),(4.0,3.0,964.0))
        self.assertEqual(tuple(month_row("2026-01").get(col,"") for col in (10,11,12)),(3.0,1.0,969.0))
        self.assertEqual(tuple(month_row("2026-03").get(col,"") for col in (10,11,12)),("",1.0,969.0))

        scope="C"; original=(TEMPLATES/(scope+".xlsm")).read_bytes()
        ops=[from_feishu(record) for record in self.source_records if record["fields"]["楼栋"]==scope+"楼"]
        baseline=map_state_baseline(original,self.configs[scope],ops)[0]
        room,rack=next(key.split("/") for key,state in baseline.items() if state["state"]=="off")
        summary={"items":[{"room":room,"rack":rack,"date":"2027-01-02","sent_at":"2027-01-02 09:00:00",
                           "direction":"up","action":"上测试电","rack_type":"服务器机柜"}]}
        book=Workbook(export_workbook(original,self.configs[scope],ops,summary))
        rows=dict(book.rows("机柜上电汇总表（每月阿里统计）"))
        self.assertTrue(any(values.get(1)=="C栋2027年上、下电总数量统计" for values in rows.values()))
        added=next(values for values in rows.values() if values.get(2)=="2027.1.2")
        self.assertEqual((added[3],added.get(4,""),added[5]),(1.0,"",971.0))
        self.assertEqual((rows[3][15],rows[4][15],rows[5][15],rows[6][15]),(32.0,939.0,27.0,971.0))

    def test_notice_exclusion_marks_old_export_stale_and_same_request_rebuilds(self):
        source={"target_record_id":"rec-stale-export","notice_type":"上电通告","scope":"B",
                "cabinet":"B-216运营商机房B04","quantity":"1","owner_id":"owner",
                "sent_at":"2026-09-19 08:30:00"}
        batch=self.service.batches.create_from_notice(source)
        before=self.service.batches.notice_summary("B",self.configs["B"])["version"]
        version=self.service.local.version("B")
        self.service.write("export:old-stale",{"export_id":"old-stale","scope":"B","path":str(Path(self.tmp.name)/"old.xlsm"),
            "filename":"old.xlsm","version":version,"notice_summary_version":before,"created_at":"2026-09-19 08:31:00"})
        calls=[]
        self.service.do_export=lambda scope,payload,job: calls.append(payload["notice_summary"]["version"]) or {"scope":scope,"version":payload["snapshot"]["version"]}
        request={"batch_id":"single_"+"f"*32}
        first=self.service.job("B","export","owner",request)
        deadline=time.time()+10
        while time.time()<deadline and self.service.job_status(first["job_id"])["status"] not in ("succeeded","failed"): time.sleep(.01)
        row=batch["rows"][0]
        changed=self.service.batches.update(batch["batch_id"],{"version":batch["version"],"rows":[
            {"row_id":row["row_id"],"exclude_notice_summary":True} ]},"owner",["B"])
        second=self.service.job("B","export","owner",request)
        self.assertNotEqual(first["job_id"],second["job_id"])
        deadline=time.time()+10
        while time.time()<deadline and self.service.job_status(second["job_id"])["status"] not in ("succeeded","failed"): time.sleep(.01)
        self.assertEqual(len(calls),2)
        self.assertNotEqual(calls[0],calls[1])
        history=self.service.export_history("B")
        old=next(item for item in history["items"] if item["export_id"]=="old-stale")
        self.assertTrue(old["is_stale"])
        self.assertIn("通告汇总已变化",old["stale_reason"])
        self.assertTrue(self.service.overview("B",False)["export_state"]["is_stale"])
        self.assertTrue(changed["rows"][0]["exclude_notice_summary"])

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

    def test_completed_all_export_request_reuses_same_batch_job(self):
        calls=[]
        self.service.do_export=lambda scope,payload,job: calls.append(payload["batch_id"]) or {"scope":scope}
        batch_id="all_"+"a"*32
        first=self.service.job("D","export","owner",{"batch_id":batch_id})
        deadline=time.time()+5
        while time.time()<deadline and self.service.job_status(first["job_id"])["status"]!="succeeded": time.sleep(.01)
        self.assertEqual(self.service.job_status(first["job_id"])["status"],"succeeded")
        retry=self.service.job("D","export","owner",{"batch_id":batch_id})
        self.assertEqual(retry["job_id"],first["job_id"])
        self.assertEqual(calls,[batch_id])
        second=self.service.job("D","export","owner",{"batch_id":"all_"+"b"*32})
        self.assertNotEqual(second["job_id"],first["job_id"])

    def test_export_start_response_is_immutable_when_worker_runs_immediately(self):
        def start_immediately(_func,job):
            job.update(status="running",started_at="2026-09-18 20:00:00")
        with patch.object(self.service,"snapshot",side_effect=AssertionError("export preparation must run in the worker")), \
                patch.object(self.service.local,"documents",side_effect=AssertionError("export start must not scan historical jobs")), \
                patch.object(self.service._pools["D"],"submit",side_effect=start_immediately):
            response=self.service.job("D","export","owner",{"batch_id":"all_"+"c"*32})
        self.assertEqual(response["status"],"pending")
        self.assertNotIn("started_at",response)
        self.assertNotIn("payload",response)
        self.service._running.pop(response["job_id"],None)

    def test_running_all_export_retry_keeps_original_job_after_summary_changes(self):
        started=threading.Event(); release=threading.Event()
        def slow_export(scope,payload,job):
            started.set(); self.assertTrue(release.wait(5)); return {"scope":scope}
        self.service.do_export=slow_export
        batch_id="all_"+"e"*32
        first=self.service.job("D","export","owner",{"batch_id":batch_id})
        self.assertTrue(started.wait(5))
        try:
            with patch.object(self.service.batches,"notice_summary",return_value={"version":"changed","items":[]}):
                retry=self.service.job("D","export","owner",{"batch_id":batch_id})
            self.assertEqual(retry["job_id"],first["job_id"])
        finally: release.set()

    def test_job_status_tolerates_short_worker_registration_gap(self):
        jid=uuid.uuid4().hex
        job={"job_id":jid,"scope":"D","kind":"export","status":"pending","pid":os.getpid(),
             "created_at":dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"version":1,"payload":{}}
        self.service.write("job:"+jid,job)
        self.assertEqual(self.service.job_status(jid)["status"],"pending")
        job["created_at"]="2020-01-01 00:00:00"
        self.service.write("job:"+jid,job)
        self.assertEqual(self.service.job_status(jid)["status"],"failed")

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

    def test_export_cleanup_waits_for_upload_before_removing_file(self):
        archive=FakeExportFeishu(); started=threading.Event(); release=threading.Event(); cleaned=threading.Event()
        original_upload=archive.upload_attachment
        def slow_upload(path,name):
            started.set(); self.assertTrue(release.wait(5)); return original_upload(path,name)
        archive.upload_attachment=slow_upload
        self.service.export_remote=archive
        eid="cleanup-during-upload"
        path=self.service.atomic_file(Path("exports")/eid/"test.xlsm",b"test")
        self.service.write("export:"+eid,{"export_id":eid,"scope":"D","path":path,"filename":"test.xlsm",
            "version":1,"created_at":"2026-09-18 20:00:00","sha256":"test","owner":"owner","batch_id":"",
            "cloud_upload_status":"pending"})
        uploads=[]
        upload=threading.Thread(target=lambda:uploads.append(self.service.upload_export("D",eid,"owner")))
        cleanup=threading.Thread(target=lambda:(self.service.cleanup_export("D",eid),cleaned.set()))
        upload.start(); self.assertTrue(started.wait(5)); cleanup.start()
        try:
            self.assertFalse(cleaned.wait(.1))
            self.assertTrue(Path(path).is_file())
        finally:
            release.set(); upload.join(5); cleanup.join(5)
        self.assertTrue(cleaned.is_set())
        self.assertEqual(uploads[0]["cloud_upload_status"],"succeeded")
        self.assertTrue(self.service.local.document("D","export:"+eid)["deleted"])
        self.assertFalse(Path(path).exists())
        history=next(item for item in self.service.export_history("D")["items"] if item["export_id"]==eid)
        self.assertFalse(history["file_available"])
        self.assertEqual(history["cloud_upload_status"],"succeeded")

    def test_export_history_is_paged_in_sqlite_order(self):
        version=self.service.local.version("D")
        notice_version=self.service.batches.notice_summary("D",self.configs["D"])["version"]
        for index in range(25):
            eid=f"paged-{index:02d}"
            self.service.write("export:"+eid,{"export_id":eid,"scope":"D","path":str(Path(self.tmp.name)/(eid+".xlsm")),
                "filename":eid+".xlsm","version":version,"notice_summary_version":notice_version,
                "created_at":f"2026-09-{index+1:02d} 10:00:00"})
        page=self.service.export_history("D",2,10)
        self.assertEqual((page["total"],page["page"],page["page_size"]),(25,2,10))
        self.assertEqual([item["export_id"] for item in page["items"]],
                         [f"paged-{index:02d}" for index in range(14,4,-1)])
        self.assertEqual(page["current"]["export_id"],"paged-24")

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
            compact=client.get("/api/cabinet-power/batches/"+created["batch_id"]+"/status",headers={"x-test-login":"1"}).json()["data"]
            self.assertEqual((compact["batch_id"],compact["version"],compact["status"]),(created["batch_id"],created["version"],created["status"]))
            self.assertNotIn("rows",compact)
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
            self.assertEqual(detail["rows"][0]["supplier_rack"],rack["rack"])

if __name__=="__main__": unittest.main()

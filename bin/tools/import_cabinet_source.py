"""One-time source migration. Restart-safe via stable keys and full readback."""
import argparse
import gzip
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from lan_bitable_template_portal.cabinet_power import CabinetFeishu, INITIAL_TEMPLATES
from lan_bitable_template_portal.cabinet_power_data import EXTRA_FIELDS, source_rows, source_evidence
from lan_bitable_template_portal.cabinet_power_excel import OPS,RACK_TYPES,digest,text_value,Workbook

DIRECTORY_NAME="机柜基础资料"
FILES={"A":"南通A栋机柜平面图及上下电数量汇总表(1).xlsm","B":"南通B楼机柜平面图及上下电数量汇总表(更新至2026.9.4).xlsm","C":"南通C栋机柜平面图及上下电数量汇总表2025.8.4.xlsm","D":"南通D栋机柜平面图及上下电数量汇总表2026年1.xlsm","E":"南通E栋机柜平面图及上下电数量汇总表2026年.xlsm"}


def ensure_fields(remote,definitions,options=None):
    existing={f["field_name"]:f for f in remote.list_all("fields")}
    for name,kind in definitions.items():
        if name not in existing:
            existing[name]=remote.request("POST","fields",{"field_name":name,"type":kind})["field"]
        if existing[name]["type"]!=kind: raise ValueError("字段类型不一致: "+name)
    for name,values in (options or {}).items():
        field=existing[name]; prop=field.get("property") or {}; current=prop.get("options",[])
        missing=sorted(set(values)-{x["name"] for x in current})
        if missing: remote.request("PUT","fields/"+field["field_id"],{"field_name":name,"type":3,"property":{**prop,"options":[*current,*[{"name":x} for x in missing]]}})


def fill(remote,planned,label):
    prior=remote.list_all(); keys=Counter(text_value(r["fields"].get("数据标识")) for r in prior if r["fields"].get("数据标识"))
    if any(v>1 for v in keys.values()): raise ValueError(label+"已存在重复数据标识")
    by_key={text_value(r["fields"].get("数据标识")):r for r in prior}
    for r in planned:
        f=r["fields"]; key=f["数据标识"]; existing=by_key.get(key)
        if not key.startswith("mapvals_") or not existing: continue
        current=text_value(existing["fields"].get("布局资料"))
        if current==f["布局资料"]: continue
        if current:
            compact={s:{ref:v for ref,v in values.items() if v not in (None,"")} for s,values in json.loads(current).items()}
            if compact!=json.loads(f["布局资料"]): raise ValueError("平面图数值已被修改: "+key)
        remote.update(existing["record_id"],{"布局资料":f["布局资料"]})
    # A changed original must be reviewed as a new migration, never overwrite remote edits.
    for r in planned:
        key=r["fields"]["数据标识"]
        if key in by_key and r["fields"].get("原始行数据")!=by_key[key]["fields"].get("原始行数据"):
            if "原始行数据" in r["fields"]: raise ValueError(label+"已有来源行内容变化: "+key)
    missing=[r for r in planned if r["fields"]["数据标识"] not in keys]
    for start in range(0,len(missing),200):
        batch=missing[start:start+200]
        # No blind retry on an ambiguous response; restart reconciles stable keys first.
        remote.request("POST","records/batch_create",{"records":[{"fields":{k:v for k,v in r["fields"].items() if v is not None}} for r in batch]})
        print(label+": "+str(min(start+200,len(missing)))+"/"+str(len(missing)),flush=True)
    actual=remote.list_all(); index={text_value(r["fields"].get("数据标识")):r for r in actual}
    for r in planned:
        found=index.get(r["fields"]["数据标识"])
        if not found: raise ValueError("回读缺失: "+r["fields"]["数据标识"])
        for k,v in r["fields"].items():
            got=found["fields"].get(k)
            if v in (None,"") and got in (None,""): continue
            if isinstance(v,(int,float)):
                if float(got)!=v: raise ValueError("回读数值不一致: "+k)
            elif text_value(got)!=text_value(v): raise ValueError("回读不一致: "+k)
    if len([r for r in actual if text_value(r["fields"].get("数据标识")) in {p['fields']['数据标识'] for p in planned}])!=len(planned): raise ValueError("回读存在重复来源行")
    return {"planned":len(planned),"created":len(missing),"total":len(actual),"verified":True}


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--commit",action="store_true"); parser.add_argument("--build-layout-cache",action="store_true"); args=parser.parse_args()
    rows=[]; directory=[]; report={}; layouts={}
    for scope,filename in FILES.items():
        path=Path("D:/下载")/filename
        try: content=path.read_bytes()
        except PermissionError: content=(INITIAL_TEMPLATES/(scope+".xlsm")).read_bytes()
        if hashlib.sha256(content).hexdigest()!=hashlib.sha256((INITIAL_TEMPLATES/(scope+".xlsm")).read_bytes()).hexdigest(): raise ValueError("原文件与布局模板不同: "+scope)
        model,items=source_rows(content,scope); rows.extend(items); report[scope]=len(items)
        layouts[scope]={room["id"]:room.get("layout") for room in model["rooms"]}
        book=Workbook(content)
        summaries={name:{ref:book.value(cell) for ref,cell in book.cells(name).items()} for name in book.sheets if "汇总" in name}
        map_values={name:{ref:book.value(cell) for ref,cell in book.cells(name).items() if book.value(cell) not in (None,"")} for name in book.sheets if "平面图" in name}
        directory.append({"fields":{"数据标识":"mapvals_"+scope,"类别":"平面图数值","楼栋":scope+"楼","名称":scope+"楼平面图原始数值","数量":0,"布局资料":json.dumps(map_values,ensure_ascii=False,separators=(",",":"))}})
        directory.append({"fields":{"数据标识":"template_"+scope,"类别":"模板资料","楼栋":scope+"楼","名称":filename,"数量":0,"布局资料":json.dumps({"hash":model["template_hash"],"formats":model["formats"],"summary_cells":summaries},ensure_ascii=False,separators=(",",":"))}})
        for room in model["rooms"]:
            geometry={k:v for k,v in room.items() if k not in ("layout",)}
            directory.append({"fields":{"数据标识":"room_"+digest([scope,room["id"]]),"类别":"房间","楼栋":scope+"楼","包间":room["id"],"名称":room["name"],"数量":room["total"],"布局资料":json.dumps(geometry,ensure_ascii=False,separators=(",",":"))}})
        for rack in model["inventory"]:
            directory.append({"fields":{"数据标识":"rack_"+digest([scope,rack["room"],rack["rack"]]),"类别":"机柜","楼栋":scope+"楼","包间":rack["room"],"名称":rack["rack"],"机柜类型":rack["rack_type"],"数量":1,"布局资料":json.dumps({"positions":rack["positions"],"template_color":rack["template_color"]},ensure_ascii=False,separators=(",",":"))}})
    assert report=={"A":1031,"B":1087,"C":1220,"D":988,"E":1272},report
    print(json.dumps({"records":report,"total":len(rows),"directory":len(directory)},ensure_ascii=False),flush=True)
    if args.build_layout_cache:
        target=INITIAL_TEMPLATES/"layouts.json.gz"
        target.write_bytes(gzip.compress(json.dumps(layouts,ensure_ascii=False,separators=(",",":")).encode(),compresslevel=9,mtime=0))
        print(json.dumps({"layout_cache":str(target),"bytes":target.stat().st_size},ensure_ascii=False),flush=True)
        for scope,rooms in layouts.items():
            content=(INITIAL_TEMPLATES/(scope+".xlsm")).read_bytes()
            model,_=source_rows(content,scope)
            (INITIAL_TEMPLATES/(scope+".layouts.json.gz")).write_bytes(gzip.compress(json.dumps({"hash":hashlib.sha256(content).hexdigest(),"rooms":rooms,"source_rows":source_evidence(content,model)},ensure_ascii=False,separators=(",",":")).encode(),mtime=0))
    if not args.commit: return
    main_remote=CabinetFeishu(); base=CabinetFeishu(""); tables=base.list_all("tables")
    matches=[t for t in tables if t["name"]==DIRECTORY_NAME]
    if len(matches)>1: raise ValueError("存在多个同名目录表")
    table_id=matches[0]["table_id"] if matches else base.request("POST","tables",{"table":{"name":DIRECTORY_NAME,"default_view_name":"全部资料","fields":[{"field_name":"名称","type":1}]}})["table_id"]
    secondary=CabinetFeishu(table_id)
    options={name:{r["fields"].get(name) for r in rows if r["fields"].get(name)} for name in ("操作类型","机柜类型","结果","机房","楼栋")}
    options["操作类型"].update(OPS); options["机柜类型"].update(RACK_TYPES)
    ensure_fields(main_remote,EXTRA_FIELDS,options)
    ensure_fields(secondary,{"数据标识":1,"类别":1,"楼栋":1,"包间":1,"名称":1,"机柜类型":1,"数量":2,"布局资料":1})
    results={"table_id":table_id,"records":fill(main_remote,rows,"上下电台账"),"directory":fill(secondary,directory,"机柜基础资料"),"buildings":report}
    print(json.dumps(results,ensure_ascii=False),flush=True)


if __name__=="__main__": main()

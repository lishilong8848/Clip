"""Lossless source rows and editable Feishu records; events are derived views."""
import copy
import datetime as dt
import json
import re
from .cabinet_power_excel import OPS, OP_PATTERN, dates, digest, room_code, text_value, Workbook, parse_template, T, bounds

EXTRA_FIELDS = {"数据标识": 1, "来源工作表": 1, "来源行号": 2, "原始行数据": 1, "历史期望时间": 1}
VISIBLE_FIELDS = ("操作类型", "实际完成时间", "期望完成时间", "操作类型（说明）", "完成时间", "历史期望时间")


def normalized_actions(value):
    value=re.sub(r"(?<!电)转正式电", "测试电转正式电", text_value(value).replace(" ", ""))
    return OP_PATTERN.findall(re.sub(r"(?<!电)转测试电", "正式电转测试电", value))


def source_sheet(op,formats):
    names=[f["sheet"] for f in formats]
    if op.get("source") in names: return op["source"]
    down=op.get("category")=="down"
    return next((n for n in names if ("下电" in n and "上下电" not in n)==down),names[0] if names else "")


def table_columns(fmt,scope):
    columns={1:{"label":"序号","field":"ordinal","width":65},2:{"label":"机房","field":"site","width":85},fmt["room"]:{"label":"包间系统名称","field":"system_name","width":150},fmt["rack"]:{"label":"机柜号" if scope in ("D","E") or "上下电" in fmt["sheet"] else "机架","field":"rack","width":85},fmt["type"]:{"label":"机柜类型","field":"rack_type","width":110},fmt["power"]:{"label":"机柜功率（W）","field":"power","width":115},fmt["result"]:{"label":"结果","field":"result","width":85}}
    for i,group in enumerate(fmt["groups"]):
        for key,label in (("action","操作类型"),("expected","期望完成时间"),("actual","实际完成时间")):
            if group.get(key): columns[group[key]]={"label":label,"field":key,"group":i,"width":180 if key=="action" else 185}
    return [{"column":col,**value} for col,value in sorted(columns.items()) if col]


def date_text(value):
    if isinstance(value, (int,float)) and value > 100000000000:
        return dt.datetime.fromtimestamp(value/1000,dt.timezone(dt.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    found=dates(value)
    return found[0] if len(found)==1 else text_value(value)


def millis(value):
    found=dates(value)
    if len(found)!=1: return None
    return int(dt.datetime.strptime(found[0],"%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone(dt.timedelta(hours=8))).timestamp()*1000)


def group_events(groups):
    events=[]; issues=[]
    for index,g in enumerate(groups):
        actions=normalized_actions(g.get("action")); actual=dates(g.get("actual")); expected=dates(g.get("expected"))
        if not any(text_value(g.get(k)) for k in ("action","actual","expected")): continue
        if not actions or len(actions)!=len(actual):
            issues.append(f"第{index+1}组操作与实际时间未配对")
        for i,action in enumerate(actions):
            events.append({"action":action,"actual":actual[i] if len(actions)==len(actual) else "", "expected":expected[i] if len(expected)==len(actions) else "", "group":index,
                           "id":str(g.get("id",index))+":"+str(i), "result":g.get("result", "")})
    return events,issues


def visible_groups(fields):
    groups=[{"action":text_value(fields.get("操作类型")),"actual":date_text(fields.get("实际完成时间")),"expected":date_text(fields.get("期望完成时间"))}]
    note=text_value(fields.get("操作类型（说明）")); times=text_value(fields.get("完成时间")); expects=text_value(fields.get("历史期望时间"))
    if note or times or expects: groups.append({"action":note,"actual":times,"expected":expects})
    return groups


def groups_to_fields(groups, primary=0):
    main=groups[primary] if groups else {}
    rest=[g for i,g in enumerate(groups) if i!=primary and any(text_value(g.get(k)) for k in ("action","actual","expected"))]
    action=text_value(main.get("action")); parsed=normalized_actions(action)
    # Multi-action primary cells remain intact in the raw group data.
    fields={"操作类型":parsed[-1] if parsed else None,"实际完成时间":millis(main.get("actual")),"期望完成时间":millis(main.get("expected"))}
    if len(parsed)>1: rest=[main,*rest]
    for column,key in (("操作类型（说明）","action"),("完成时间","actual"),("历史期望时间","expected")):
        fields[column]="\n".join(text_value(g.get(key)) for g in rest)
    return fields


def source_rows(content,scope):
    book=Workbook(content); model=parse_template(content,scope); records=[]
    for fmt in model["formats"]:
        sheet=fmt["sheet"]; category="down" if "下电" in sheet and "上下电" not in sheet else "mixed" if "上下电" in sheet else "up"
        for number,row in book.rows(sheet):
            if number<=fmt["header"] or not text_value(row.get(fmt["rack"])): continue
            groups=[{"action":text_value(row.get(g["action"])),"actual":date_text(row.get(g["actual"])),"expected":date_text(row.get(g["expected"])) if g["expected"] else ""} for g in fmt["groups"]]
            primary=next((i for i in reversed(range(len(groups))) if any(a.startswith("下") for a in normalized_actions(groups[i]["action"]))),0) if category=="down" else 0
            fields={"机房":text_value(row.get(2)) or "EA118","楼栋":scope+"楼","包间系统名称":text_value(row.get(fmt["room"])),"机架":text_value(row.get(fmt["rack"])),"机柜类型":text_value(row.get(fmt["type"])) or None,"机柜功率（W）":row.get(fmt["power"]) or None,"结果":text_value(row.get(fmt["result"])) or None,**groups_to_fields(groups,primary),"来源工作表":sheet,"来源行号":number,"数据标识":"source_"+digest([scope,sheet,number])}
            meta={"schema":2,"scope":scope,"hash":model["template_hash"],"sheet":sheet,"row":number,"category":category,"primary":primary,"groups":groups,"cells":{str(k):v for k,v in row.items()},"visible":{k:fields.get(k) for k in VISIBLE_FIELDS}}
            fields["原始行数据"]=json.dumps(meta,ensure_ascii=False,separators=(",",":"))
            records.append({"fields":fields})
    return model,records


def source_evidence(content,model):
    book=Workbook(content); output={}
    for fmt in model["formats"]:
        name=fmt["sheet"]; rows=dict(book.rows(name)); inherited={}
        for merge in book.sheet(name).iter(T("mergeCell")):
            x,y,x2,y2=bounds(merge.get("ref")); value=rows.get(y,{}).get(x,"")
            for yy in range(y,y2+1):
                for xx in range(x,x2+1): inherited[(yy,xx)]=(y,value)
        records={}
        for rn,row in rows.items():
            if rn<=fmt["header"]: continue
            def get(col): return row.get(col) if row.get(col) not in (None,"") else inherited.get((rn,col),(rn,""))[1]
            if not text_value(get(fmt["rack"])): continue
            owner=rn if text_value(row.get(fmt["rack"])) else inherited.get((rn,fmt["rack"]),(rn,""))[0]
            record=records.setdefault(str(owner),{"groups":[],"continuations":[]})
            if owner!=rn: record["continuations"].append({"row":rn,"cells":{str(c):v for c,v in row.items()}})
            for index,g in enumerate(fmt["groups"]):
                resolved={"action":text_value(get(g["action"])),"actual":date_text(get(g["actual"])),"expected":date_text(get(g["expected"])) if g.get("expected") else ""}
                raw={"action":text_value(row.get(g["action"])),"actual":date_text(row.get(g["actual"])),"expected":date_text(row.get(g["expected"])) if g.get("expected") else ""}
                record["groups"].append({**resolved,"source_row":rn,"source_group":index,"source_raw":raw,"source_resolved":resolved})
        output[name]=records
    return output


def complete_source_record(record,evidence):
    op=from_feishu(record); source=evidence.get(op["source"],{}).get(str(op["source_row"]))
    if not source or op["meta"].get("source_completed"): return None
    original=op["meta"].get("groups",[])
    if not original or op["meta"].get("schema",0)>=3: return None
    groups=[]; changed=False
    for item in source["groups"]:
        g=copy.deepcopy(item); i=g["source_group"]; continuation=g["source_row"]!=op["source_row"]
        if continuation:
            if not any(g.get(k) for k in ("action","actual","expected")): continue
            g["id"]="source_"+digest([op["record_id"],g["source_row"],i])[:24]; g["result"]=op["result"]; changed=True
        else:
            current=op["groups"][i]
            if any(current.get(k,"")!=original[i].get(k,"") for k in ("action","actual","expected")): g.update(current)
            else: changed=changed or any(g[k]!=current.get(k,"") for k in ("action","actual","expected"))
            g["id"]=current["id"]; g["result"]=current.get("result",op["result"])
        groups.append(g)
    if not changed: return None
    op["groups"]=groups; op["meta"].update(source_completed=True,continuations=source["continuations"])
    return {**record,"fields":{**record["fields"],**to_fields(op)}}


def from_feishu(record):
    f=record.get("fields",{}); scope=text_value(f.get("楼栋")).replace("楼",""); room,mismatch=room_code(text_value(f.get("包间系统名称")),scope)
    try: meta=json.loads(text_value(f.get("原始行数据")) or "{}")
    except (ValueError,TypeError): meta={}
    if not isinstance(meta,dict): meta={}
    groups=copy.deepcopy(meta.get("groups",[]))
    if not isinstance(groups,list) or any(not isinstance(g,dict) for g in groups): groups=[]; meta={}
    original=meta.get("visible",{})
    if not groups and not (meta.get("schema",0)>=3 and "groups" in meta): groups=visible_groups(f)
    elif groups:
        primary=meta.get("primary",0)
        if not isinstance(primary,int) or not 0<=primary<len(groups): primary=0
        for field,key in (("操作类型","action"),("实际完成时间","actual"),("期望完成时间","expected")):
            if f.get(field)!=original.get(field) and not (f.get(field) in (None,"") and original.get(field) in (None,"")):
                groups[primary][key]=date_text(f.get(field)) if key!="action" else text_value(f.get(field))
        if any(text_value(f.get(k))!=text_value(original.get(k)) for k in ("操作类型（说明）","完成时间","历史期望时间")):
            groups=[groups[primary],*visible_groups(f)[1:]]; meta["primary"]=0
    for index,group in enumerate(groups):
        group.setdefault("id","legacy_"+digest([record["record_id"],index])[:24])
        group.setdefault("result",text_value(f.get("结果")))
    primary=meta.get("primary",0)
    if groups and meta.get("schema",0)>=3 and "结果" in original and f.get("结果")!=original.get("结果"):
        groups[primary if isinstance(primary,int) and 0<=primary<len(groups) else 0]["result"]=text_value(f.get("结果"))
    events,issues=group_events(groups)
    if mismatch or scope not in "ABCDE" or not room: issues.append("包间与楼栋不一致")
    power=f.get("机柜功率（W）","")
    try: power=float(power) if power not in (None,"") else ""
    except (TypeError,ValueError): issues.append("功率格式异常")
    empty=not any(any(text_value(g.get(k)) for k in ("action","actual","expected")) for g in groups)
    op={"record_id":record["record_id"],"version":digest(f),"scope":scope,"room":room,"system_name":text_value(f.get("包间系统名称")),"rack":text_value(f.get("机架")).upper(),"rack_type":text_value(f.get("机柜类型")),"power":power,"result":text_value(f.get("结果")),"action":text_value(f.get("操作类型")),"actual":date_text(f.get("实际完成时间")),"expected":date_text(f.get("期望完成时间")),"action_note":text_value(f.get("操作类型（说明）")),"completion_time":text_value(f.get("完成时间")),"groups":groups,"events":events,"issues":issues,"empty":empty,"source":text_value(f.get("来源工作表")),"source_row":f.get("来源行号"),"category":meta.get("category") or ("down" if text_value(f.get("操作类型")).startswith("下") else "up"),"meta":meta,"raw_fields":f}
    if not re.fullmatch(r"[A-Z]\d{2}",op["rack"]): issues.append("机架号格式异常")
    if any(g.get("result") not in ("成功","失败") and any(g.get(k) for k in ("action","actual","expected")) for g in groups): issues.append("操作结果未确认")
    op["last_operation"]=max((e["actual"] for e in events if e["actual"]),default="")
    try: op["source_row"]=int(float(op["source_row"])) if op["source_row"] not in (None,"") else None
    except (ValueError,TypeError): op["source_row"]=None; issues.append("来源行号格式异常")
    return op


def to_fields(op):
    groups=op["groups"] if "groups" in op else [{"action":op.get("action",""),"actual":op.get("actual",""),"expected":op.get("expected","")}, {"action":op.get("action_note",""),"actual":op.get("completion_time",""),"expected":""}]
    meta=copy.deepcopy(op.get("meta",{})); primary=meta.get("primary",0)
    if not isinstance(primary,int) or not 0<=primary<len(groups): primary=0
    fields={"机房":"EA118","楼栋":op["scope"]+"楼","包间系统名称":op["system_name"],"机架":op["rack"],"机柜类型":op.get("rack_type") or None,"机柜功率（W）":op.get("power") if op.get("power") not in (None,"") else None,"结果":op.get("result") or None,**groups_to_fields(groups,primary)}
    fields["结果"]=(groups[primary].get("result") if groups else "") or None
    meta.update(schema=3,groups=groups,primary=primary,visible={k:fields.get(k) for k in (*VISIBLE_FIELDS,"结果")})
    fields["原始行数据"]=json.dumps(meta,ensure_ascii=False,separators=(",",":"))
    return fields

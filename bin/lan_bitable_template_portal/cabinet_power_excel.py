"""Cabinet-specific XLSM parsing. Never execute VBA or save through openpyxl.

The workbook is evidence, not authority: ambiguous dates, colours and room
identities are retained for the administrator's precheck.
"""
from __future__ import annotations

import copy
import ast
import datetime as dt
import hashlib
import io
import json
import math
import posixpath
import re
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict

from .drill_management import _cell_style_catalog, _shared_strings

NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG = "http://schemas.openxmlformats.org/package/2006/relationships"
CT = "http://schemas.openxmlformats.org/package/2006/content-types"
T = lambda name: f"{{{NS}}}{name}"
OPS = ("上正式电", "上测试电", "测试电转正式电", "正式电转测试电", "下正式电", "下测试电")
STATES = dict(zip(OPS, ("formal", "test", "formal", "test", "off", "off")))
COLORS = {"formal": "#FF0000", "test": "#FFC000", "off": "#00B050", "unknown": "#94A3B8"}
TOTALS = dict(zip("ABCDE", (988, 1076, 998, 988, 1272)))
RACK_TYPES = ("网络机柜", "服务器机柜")
OP_PATTERN = re.compile("|".join(sorted(OPS, key=len, reverse=True)))
DATE_PATTERN = re.compile(r"(20\d{2})[年/.-](\d{1,2})[月/.-](\d{1,2})日?(?:[ T\s]+(\d{1,2})[:：](\d{1,2})(?:[:：](\d{1,2}))?)?")


class CabinetError(ValueError):
    def __init__(self, message, status_code=400):
        super().__init__(message)
        self.status_code = status_code


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def col_number(name):
    n = 0
    for char in name:
        n = n * 26 + ord(char) - 64
    return n


def col_name(n):
    result = ""
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def coord(ref):
    match = re.fullmatch(r"\$?([A-Z]{1,3})\$?(\d{1,7})", ref)
    if not match:
        raise CabinetError(f"无效单元格位置：{ref}")
    return col_number(match[1]), int(match[2])


def bounds(ref):
    parts = ref.split(":")
    x, y = coord(parts[0]); x2, y2 = coord(parts[-1])
    if x2 < x or y2 < y or x2 > 16384 or y2 > 1048576:
        raise CabinetError("无效布局区域")
    return x, y, x2, y2


def room_code(value, scope):
    text = str(value or "").strip().upper()
    match = re.fullmatch(r"EA118[-_]([A-E])(\d)[-_](\d{1,2})", text)
    if match:
        return f"{match[2]}{int(match[3]):02d}", match[1] != scope
    carrier = re.fullmatch(r"EA118[-_]([A-E])[-_]([1-4]\d{2}).*", text)
    if carrier:
        return carrier[2], carrier[1] != scope
    match = re.search(r"(?<!\d)([1-4]\d{2})(?!\d)", text.replace("EA118", ""))
    if match:
        return match[1], False
    return "", bool(text)


def system_name(scope, room):
    return f"EA118-{scope}{room[0]}-{int(room[1:])}"


def dates(value):
    if value in (None, ""):
        return []
    if isinstance(value, (float, int)) and math.isfinite(value) and 30000 < value < 100000:
        return [(dt.datetime(1899, 12, 30) + dt.timedelta(days=value)).strftime("%Y-%m-%d %H:%M:%S")]
    result = []
    for match in DATE_PATTERN.finditer(str(value)):
        try:
            result.append(dt.datetime(*[int(v or 0) for v in match.groups()]).strftime("%Y-%m-%d %H:%M:%S"))
        except ValueError:
            return []
    return result


def text_value(value):
    if isinstance(value, list):
        return "".join(str(v.get("text", "")) if isinstance(v, dict) else str(v) for v in value)
    return str(value or "").strip()


def operation_key(op):
    return tuple(op.get(k, "") for k in ("scope", "room", "rack", "action", "actual"))


def unique_operations(ops):
    unique, duplicates, issues = {}, 0, []
    for op in ops:
        key = operation_key(op)
        if not op.get("actual"):
            key += (op.get("source", ""),)
        if key in unique:
            before = unique[key]
            if any(before.get(k) != op.get(k) for k in ("rack_type", "power", "result", "expected")):
                issues.append({"kind": "duplicate_conflict", "room": op["room"], "rack": op["rack"], "source": op.get("source"), "message": "同一操作时间的类型、功率、结果或期望时间不一致"})
            else:
                duplicates += 1
                before.setdefault("sources", []).append(op.get("source", ""))
        else:
            unique[key] = copy.deepcopy(op)
    return list(unique.values()), duplicates, issues


def import_rows(operations):
    grouped={}
    for op in operations:
        grouped.setdefault(op.get("source","").split(":")[0],[]).append(op)
    rows=[]
    for source,events in grouped.items():
        sheet=source.split("!")[0]; down_only="下电" in sheet and "上下电" not in sheet
        primary=next((op for op in reversed(events) if down_only and op["action"].startswith("下")),events[0])
        history=[op for op in events if op is not primary]
        row=copy.deepcopy(primary); row["source"]=source
        row["action_note"]="\n".join(f"{i}、{op['action']}" for i,op in enumerate(history,1))
        row["completion_time"]="\n".join(f"{i}、{op['actual']}" for i,op in enumerate(history,1))
        rows.append(row)
    return rows


class Workbook:
    def __init__(self, content):
        if len(content) > 32 * 1024 * 1024:
            raise CabinetError("机柜模板最大32MiB")
        try:
            self.archive = zipfile.ZipFile(io.BytesIO(content))
            entries = self.archive.infolist()
            if len(entries) > 8192 or sum(e.file_size for e in entries) > 256 * 1024 * 1024:
                raise CabinetError("模板解压体积超限")
            if len({e.filename for e in entries}) != len(entries):
                raise CabinetError("模板包含重复ZIP项")
            for e in entries:
                if e.filename.startswith(("/", "\\")) or ".." in e.filename.replace("\\", "/").split("/"):
                    raise CabinetError("模板文件路径不安全")
            if self.archive.testzip():
                raise CabinetError("模板ZIP损坏")
            types=ET.fromstring(self.archive.read("[Content_Types].xml"))
            if not any(n.get("PartName")=="/xl/workbook.xml" and "macroEnabled" in n.get("ContentType","") for n in types):
                raise CabinetError("请上传真实的.xlsm宏工作簿，不能只修改文件扩展名")
            self.root = ET.fromstring(self.archive.read("xl/workbook.xml"))
            rels = ET.fromstring(self.archive.read("xl/_rels/workbook.xml.rels"))
            targets = {r.get("Id"): posixpath.normpath(posixpath.join("xl", r.get("Target", ""))).lstrip("/") for r in rels if r.get("TargetMode") != "External"}
            self.sheets = {s.get("name"): targets[s.get(f"{{{REL}}}id")] for s in self.root.find(T("sheets"))}
            self.strings = _shared_strings(self.archive)
            self.styles = _cell_style_catalog(self.archive)
            self._roots = {}
        except (KeyError, zipfile.BadZipFile, ET.ParseError) as exc:
            raise CabinetError("不是完整的Excel机柜模板") from exc

    def sheet(self, name):
        if name not in self._roots:
            self._roots[name] = ET.fromstring(self.archive.read(self.sheets[name]))
        return self._roots[name]

    def cells(self, name):
        return {c.get("r"): c for c in self.sheet(name).iter(T("c"))}

    def value(self, cell):
        if cell is None:
            return ""
        if cell.get("t") == "inlineStr":
            return "".join(t.text or "" for t in cell.iter(T("t")))
        val = cell.findtext(T("v"), "")
        if cell.get("t") == "s":
            return self.strings[int(val)] if val else ""
        if cell.get("t") not in ("e", "str", "b") and val:
            try:
                return float(val)
            except ValueError:
                pass
        return val

    def rows(self, name):
        for row in self.sheet(name).find(T("sheetData")):
            yield int(row.get("r")), {coord(c.get("r"))[0]: self.value(c) for c in row if c.tag == T("c")}

    def layout(self, name, region):
        x1, y1, x2, y2 = bounds(region)
        if (x2-x1+1)*(y2-y1+1) > 20000:
            raise CabinetError("单房间布局最多20000个单元格，请缩小有效区域")
        root, cells = self.sheet(name), self.cells(name)
        widths = {x: 64 for x in range(x1, x2+1)}
        for col in root.findall(f"{T('cols')}/{T('col')}"):
            for x in range(max(x1, int(col.get("min"))), min(x2, int(col.get("max")))+1):
                widths[x] = 0 if col.get("hidden") == "1" else max(8, float(col.get("width", 8.43))*7+5)
        heights = {y: 20 for y in range(y1, y2+1)}
        for row in root.find(T("sheetData")):
            y = int(row.get("r"))
            if y in heights:
                heights[y] = 0 if row.get("hidden") == "1" else float(row.get("ht", 15))*4/3
        merges, hidden = {}, set()
        for merge in root.findall(f"{T('mergeCells')}/{T('mergeCell')}"):
            a,b,c,d = bounds(merge.get("ref"))
            if a < x1 or b < y1 or c > x2 or d > y2:
                continue
            merges[(a,b)] = (c,d)
            hidden.update((x,y) for x in range(a,c+1) for y in range(b,d+1) if (x,y)!=(a,b))
        xs, ys = {x1:0}, {y1:0}
        for x in widths: xs[x+1] = xs[x]+widths[x]
        for y in heights: ys[y+1] = ys[y]+heights[y]
        model = []
        for y in heights:
            for x in widths:
                if (x,y) in hidden:
                    continue
                ref = f"{col_name(x)}{y}"
                cell = cells.get(ref)
                if cell is None: continue
                cx,cy = merges.get((x,y), (x,y))
                style = self.styles[int(cell.get("s", 0))]
                value=self.value(cell)
                model.append({"ref":ref, "range":f"{ref}:{col_name(cx)}{cy}", "text":format(value,"g") if isinstance(value,float) else str(value), "formula":cell.findtext(T("f"),""), "x":xs[x], "y":ys[y], "width":xs[cx+1]-xs[x], "height":ys[cy+1]-ys[y], "style":style})
        return {"sheet":name, "region":region, "width":xs[x2+1], "height":ys[y2+1], "cells":model}


def parse_template(content, scope):
    if scope not in TOTALS:
        raise CabinetError("请选择A–E楼")
    book = Workbook(content)
    rooms, inventory, ops, records, issues, formats = {}, {}, [], [], [], []
    summary = next((s for s in book.sheets if "汇总" in s), "")
    if not summary:
        raise CabinetError("找不到机柜汇总表")
    for rn,row in book.rows(summary):
        match = re.search(r"(?<!\d)([1-4]\d{2})(?!\d)", str(row.get(1, "")))
        if match and isinstance(row.get(2), (int,float)):
            room = match[1]
            rooms[room] = {"id":room, "name":system_name(scope,room), "total":int(row[2]), "sheet":"", "region":"", "summary_row":rn}
    for name in book.sheets:
        if "平面图" not in name: continue
        match = re.search(r"[1-4]\d{2}", name)
        if not match: continue
        room = match[0]
        info = rooms.setdefault(room, {"id":room,"name":system_name(scope,room),"total":0})
        region = "B1:AP46" if scope == "D" and room == "202" else "B1:AX46"
        if scope=='C' and room=='202': region='B1:AX49'
        layout = book.layout(name, region)
        info.update(sheet=name, region=region, layout=layout)
        for c in layout["cells"]:
            rack = c["text"].strip().upper()
            if re.fullmatch(r"[A-Z]\d{2}", rack):
                key = f"{room}/{rack}"
                inv = inventory.setdefault(key, {"room":room, "rack":rack, "rack_type":"", "positions":[], "template_color":c["style"].get("fill", "")})
                inv["positions"].append({"sheet":name, "range":c["range"]})
        if scope == "D" and room == "202":
            issues.append({"kind":"layout_review", "room":room, "message":"202工作表AV列起另有201布局。默认有效范围B1:AP46，右侧不计数；请核对镜像范围。"})
    # Our standardized exports retain the full inventory, including racks with
    # no operations. Re-uploading one must not lose those rack types/identities.
    if "_柜状态" in book.sheets:
        for rn,row in book.rows("_柜状态"):
            room=str(row.get(1,"")).strip(); rack=str(row.get(2,"")).strip().upper(); kind=str(row.get(3,"")).strip()
            if rn==1 or room not in rooms or not re.fullmatch(r"[A-Z]\d{2}",rack) or kind not in RACK_TYPES: continue
            inv=inventory.setdefault(f"{room}/{rack}",{"room":room,"rack":rack,"positions":[],"template_color":""})
            inv["rack_type"]=kind
    for name in book.sheets:
        if "统计" not in name or "汇总" in name: continue
        rows = list(book.rows(name))
        header = next(((rn,row) for rn,row in rows[:8] if any(str(v).strip() in ("机架", "机柜号") for v in row.values())), None)
        if not header: continue
        hn, labels = header
        rack_col = next(k for k,v in labels.items() if str(v).strip() in ("机架", "机柜号"))
        room_col = next((k for k,v in labels.items() if "包间" in str(v)), rack_col-1)
        type_col = next((k for k,v in labels.items() if "机柜类型" in str(v)), 0)
        power_col = next((k for k,v in labels.items() if "功率" in str(v)), 0)
        result_col = next((k for k,v in labels.items() if str(v).strip() == "结果"), 0)
        groups = []
        for k,v in sorted(labels.items()):
            if "操作类型" not in str(v): continue
            end = next((kk for kk,vv in sorted(labels.items()) if kk>k and ("操作类型" in str(vv) or "机柜类型" in str(vv))), max(labels)+1)
            expected = next((kk for kk,vv in labels.items() if k<kk<end and "期望" in str(vv)), None)
            actual = next((kk for kk,vv in labels.items() if k<kk<end and ("实际" in str(vv) or "完成时间" in str(vv) and "期望" not in str(vv))), None)
            if actual: groups.append({"action":k, "expected":expected, "actual":actual})
        formats.append({"sheet":name,"header":hn,"rack":rack_col,"room":room_col,"type":type_col,"power":power_col,"result":result_col,"groups":groups})
        for rn,row in rows:
            if rn<=hn: continue
            rack = str(row.get(rack_col, "")).strip().upper()
            if not rack: continue
            rack_valid=bool(re.fullmatch(r"[A-Z]\d{2}",rack))
            raw_room = row.get(room_col, "")
            room,mismatch = room_code(raw_room,scope)
            source = f"{name}!{rn}"
            if not room or room not in rooms or mismatch:
                issues.append({"kind":"room_conflict","room":room,"rack":rack,"source":source,"message":f"包间与楼栋不一致或未知：{raw_room}","raw":{str(k):v for k,v in row.items() if v!=""}})
                continue
            if not rack_valid:
                issues.append({"kind":"rack_format","room":room,"rack":rack,"source":source,"message":"机架号格式异常，请按原表核对"})
            inv = inventory.setdefault(f"{room}/{rack}", {"room":room,"rack":rack,"rack_type":"","positions":[],"template_color":""}) if rack_valid else None
            kind = str(row.get(type_col, "")).strip()
            if inv is not None and kind in RACK_TYPES:
                if inv["rack_type"] and inv["rack_type"]!=kind:
                    issues.append({"kind":"type_conflict","room":room,"rack":rack,"source":source,"message":"历史机柜类型不一致"})
                inv["rack_type"] = kind
            row_ops=[]
            for group in groups:
                raw_action = str(row.get(group["action"], ""))
                normalized_action = re.sub(r"(?<!电)转正式电", "测试电转正式电", raw_action.replace(" ", ""))
                normalized_action = re.sub(r"(?<!电)转测试电", "正式电转测试电", normalized_action)
                actions = OP_PATTERN.findall(normalized_action)
                times = dates(row.get(group["actual"]))
                expects = dates(row.get(group["expected"])) if group["expected"] else []
                if not raw_action.strip():
                    if times: issues.append({"kind":"unpaired","room":room,"rack":rack,"source":source,"message":"只有时间，没有对应操作","raw":times})
                    continue
                if not actions or len(actions)!=len(times):
                    issues.append({"kind":"unpaired","room":room,"rack":rack,"source":source,"message":"操作与实际时间无法一一对应","raw_action":raw_action,"raw_time":row.get(group["actual"], "")})
                    continue
                for i,(action,actual) in enumerate(zip(actions,times)):
                    op = {"scope":scope,"room":room,"system_name":str(raw_room),"rack":rack,"action":action,"actual":actual,"expected":expects[i] if len(expects)==len(actions) else "", "rack_type":kind,"power":row.get(power_col, ""),"result":str(row.get(result_col, "")).strip(),"source":f"{source}:{col_name(group['action'])}:{i}"}
                    op["operation_id"] = digest(operation_key(op))
                    row_ops.append(op)
            records.extend(import_rows(row_ops))
            if not row_ops and any(str(row.get(g["action"],"")).strip() or str(row.get(g["actual"],"")).strip() for g in groups):
                records.append({"scope":scope,"room":room,"system_name":str(raw_room),"rack":rack,"action":"","actual":"","expected":"","rack_type":kind,"power":row.get(power_col,""),"result":str(row.get(result_col,"")).strip(),"source":source,"action_note":"","completion_time":""})
            if rack_valid: ops.extend(row_ops)
    ops,duplicates,conflicts = unique_operations(ops)
    issues += conflicts
    for room in rooms.values():
        items = [v for v in inventory.values() if v["room"]==room["id"]]
        room["identified"] = len(items)
        if len(items)!=room["total"]:
            issues.append({"kind":"inventory_count","room":room["id"],"message":f"模板汇总{room['total']}柜，已识别位置{len(items)}柜；请补充或核对目录，不生成虚构机架。"})
    result = {"scope":scope,"template_hash":hashlib.sha256(content).hexdigest(),"sheet_names":list(book.sheets),"rooms":list(rooms.values()),"inventory":list(inventory.values()),"operations":ops,"records":records,"formats":formats,"summary_sheet":summary,"issues":issues,"duplicates":duplicates,"expected_total":TOTALS[scope]}
    derived = calculate(result["inventory"],ops)
    for rack in derived["racks"]:
        inv = inventory[f"{rack['room']}/{rack['rack']}"]
        color = inv.get("template_color", "").upper()
        if color in ("#FF0000","#00B050","#92D050","#FFC000") and rack["state"]!="unknown" and (color=="#FF0000" and rack["state"]!="formal" or color=="#FFC000" and rack["state"]!="test" or color in ("#00B050","#92D050") and rack["state"]!="off"):
            issues.append({"kind":"color_conflict","room":rack["room"],"rack":rack["rack"],"message":"台账推导状态与模板颜色不一致"})
    result["version"] = digest({k:v for k,v in result.items() if k!="version"})
    return result


def calculate(inventory, operations, issues=()):
    grouped = defaultdict(list)
    for record in operations:
        for event in record.get("events") or [record]:
            op={**record,**event}; op.pop("events",None)
            grouped[(op.get("room"),op.get("rack"))].append(op)
    racks, daily = [], defaultdict(lambda: Counter())
    for item in inventory:
        rack = copy.deepcopy(item)
        history = grouped[(item["room"],item["rack"])]
        valid=[op for op in history if completed_state_event(op) and len(dates(op.get('actual')))==1]
        state, last = "off", ""
        by_time = defaultdict(list)
        for op in valid: by_time[op["actual"]].append(op)
        for stamp in sorted(by_time):
            actions = {o["action"] for o in by_time[stamp]}
            states={STATES[action] for action in actions}
            state=next(iter(states)) if len(states)==1 else 'unknown'; last=stamp
            if len(actions)==1:
                action=next(iter(actions))
                daily[stamp[:10]][action]+=1
        if not valid and any(op.get('result')!='失败' for op in history): state='unknown'
        rack.update(state=state,color=COLORS[state],last_operation=last,operation_count=len(history),state_source='operation' if valid else 'unconfirmed' if state=='unknown' else 'empty')
        racks.append(rack)
    counts=Counter(r["state"] for r in racks)
    return {"racks":racks,"counts":{"total":len(racks),"formal":counts["formal"],"test":counts["test"],"off":counts["off"],"unknown":counts["unknown"],"powered":counts["formal"]+counts["test"]},"daily":dict(daily)}


def state_event_hash(event):
    return digest([event.get(k) for k in ('id','action','actual','result','scope','room','rack')]+['包间与楼栋不一致' in event.get('issues',[])])[:32]


def completed_state_event(event):
    return event.get('result')=='成功' and event.get('action') in STATES and bool(event.get('actual')) and '包间与楼栋不一致' not in event.get('issues',[])


def map_state_baseline(content,config,operations):
    """Freeze physical rack fills, never the formula values printed below the drawing."""
    book=Workbook(content); cells={name:book.cells(name) for name in book.sheets}; histories=defaultdict(list)
    if hashlib.sha256(content).hexdigest()!=config['template_data']['hash']: raise CabinetError('平面图模板版本不一致')
    for op in operations:
        histories[(op['room'],op['rack'])].extend({**op,**e} for e in op['events'])
    baseline={}; counts=Counter()
    for rack in config['inventory']:
        history=histories[(rack['room'],rack['rack'])]; valid=[e for e in history if completed_state_event(e)]
        colors=set()
        for pos in rack.get('positions',[]):
            cell=cells[pos['sheet']].get(pos['range'].split(':')[0])
            if cell is None or cell.find(T('f')) is not None or text_value(book.value(cell)).upper()!=rack['rack']: raise CabinetError('平面图机柜位置无法核对：'+rack['room']+'/'+rack['rack'])
            colors.add(book.styles[int(cell.get('s',0))].get('fill','').upper())
        if len(colors)>1: raise CabinetError('同一机柜存在冲突颜色：'+rack['room']+'/'+rack['rack'])
        color=next(iter(colors),''); latest=max(valid,key=lambda e:e['actual'],default={})
        if color in ('#00B050','#92D050'): state='off'
        elif color=='#FFC000': state='test'
        elif color=='#FF0000':
            if config['scope']=='C': state='formal'
            else:
                # These templates use red for both supplies; history only identifies the supply type.
                powered=[e for e in valid if STATES[e['action']] in ('formal','test')]
                if not powered: raise CabinetError('红色机柜缺少可区分正式/测试电的记录：'+rack['room']+'/'+rack['rack'])
                state=STATES[max(powered,key=lambda e:e['actual'])['action']]
        elif not colors and config['scope']=='B' and rack['room'] in ('216','247'):
            state=STATES.get(latest.get('action'),'off')
        else: raise CabinetError('机柜颜色未识别：'+rack['room']+'/'+rack['rack'])
        baseline[rack['room']+'/'+rack['rack']]={'state':state,'color':color,'last_operation':latest.get('actual',''),'event_hashes':sorted({state_event_hash(e) for e in history})}
        counts['mapped_powered' if colors and state in ('formal','test') else 'outside_powered' if state in ('formal','test') else 'off']+=1
    return baseline,dict(counts)


def derive_records(config,operations):
    events=[]; issues=[]; known={(r["room"],r["rack"]) for r in config["inventory"]}
    for op in operations:
        problems=list(op.get("issues",[]))
        if (op["room"],op["rack"]) not in known: problems.append("机柜号未匹配平面图目录")
        for message in problems: issues.append({"room":op["room"],"rack":op["rack"],"record_id":op["record_id"],"message":message,"source":op.get("source",""),"source_row":op.get("source_row")})
        for event in op.get("events",[]):
            group=op.get("groups",[])[event.get("group",0)] if op.get("groups") else {}
            if not event.get("actual") and text_value(group.get("actual")) in ("/","－","-","未完成"): continue
            events.append({**op,**event,"events":[]})
    blocking=[issue for issue in issues if not (config["scope"]=="B" and issue["room"] in ("216","247") and "未配对" in issue["message"])]
    derived=calculate(config["inventory"],events,blocking)
    unlocated={}
    for room in config["rooms"]:
        missing=max(0,room["total"]-sum(r["room"]==room["id"] for r in config["inventory"]))
        off=0
        if config["scope"]=="B" and room["id"] in ("216","247"):
            off=missing
        unlocated[room["id"]]={"total":missing,"off":off,"unknown":missing-off}
        derived["counts"]["total"]+=missing; derived["counts"]["off"]+=off; derived["counts"]["unknown"]+=missing-off
    derived["unlocated"]=unlocated
    derived["issues"]=issues
    return derived


def project_layout(model, racks, building_racks=None):
    """Replace workbook colour counters/hand totals in the browser projection too."""
    model=copy.deepcopy(model)
    cells=model["cells"]; by_ref={c["ref"]:c for c in cells}
    states={r["rack"]:r for r in racks}
    unresolved=any(r["state"]=="unknown" for r in racks)
    for cell in cells:
        if cell["text"] in states:
            rack=states[cell["text"]]; cell.update(rack=rack["rack"],state=rack["state"],color=rack["color"])
        if cell.get("formula"):
            # No stale Excel formula cache is presented as current data.
            cell["text"]="—"
            match=re.search(r"getcolorcount\(\s*([A-Z0-9$]+:[A-Z0-9$]+)\s*[,;]\s*([A-Z0-9$]+)",cell["formula"],re.I)
            if match and not unresolved:
                x,y,x2,y2=bounds(match[1].replace("$",""))
                sample=by_ref.get(match[2].replace("$",""),{})
                color=sample.get("style",{}).get("fill","").upper()
                state={"#FF0000":"formal","#FFC000":"test","#00B050":"off","#92D050":"off"}.get(color)
                members={c["text"] for c in cells if re.fullmatch(r"[A-Z]\d{2}",c["text"]) and x<=coord(c["ref"])[0]<=x2 and y<=coord(c["ref"])[1]<=y2}
                if state: cell["text"]=str(sum(r["rack"] in members and r["state"]==state for r in racks))
    claimed=set()
    for cell in cells:
        label=cell["text"]; x,y=coord(cell["ref"])
        if y<32 or not re.search(r"机柜|上电|下电|测试电|正式电",label): continue
        metric="off" if "未上电" in label or "下电" in label else "test" if "测试电" in label else "formal" if "正式电" in label else "powered" if "上电" in label else "total" if "总" in label or "包间机柜数" in label else ""
        if not metric: continue
        target=next((c for xx in range(x+1,min(x+8,51)) if (c:=by_ref.get(f"{col_name(xx)}{y}")) and (c.get("formula") or re.fullmatch(r"\d+(?:\.\d+)?",c["text"]))),None)
        if target is None or target['ref'] in claimed: continue
        claimed.add(target['ref'])
        target["metric"]=metric
        target["metric_type"]="网络机柜" if "网络" in label else "服务器机柜" if "服务器" in label else ""
        target['metric_building']='楼' in label
        members=building_racks if target['metric_building'] and building_racks is not None else racks
        if any(r['state']=='unknown' for r in members) and metric!="total": target["text"]="—"; continue
        chosen=[r for r in members if ("网络" not in label or r["rack_type"]=="网络机柜") and ("服务器" not in label or r["rack_type"]=="服务器机柜")]
        value=len(chosen) if metric=="total" else sum(r["state"] in ("formal","test") for r in chosen) if metric=="powered" else sum(r["state"]==metric for r in chosen)
        target["text"]=str(value)
    return model


def xml_bytes(root, original=b""):
    # Preserve namespace declarations used only by mc:Ignorable/extension values.
    declarations = dict(re.findall(rb'xmlns:([\w]+)="([^"]+)"', original))
    for prefix, uri in declarations.items():
        if not re.fullmatch(rb"ns\d+", prefix):
            ET.register_namespace(prefix.decode(), uri.decode())
    raw = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    for prefix, uri in declarations.items():
        token = b"xmlns:" + prefix + b"="
        if token not in raw:
            pos = raw.index(b">", raw.index(b"?>")+2)
            raw = raw[:pos] + b' xmlns:' + prefix + b'="' + uri + b'"' + raw[pos:]
    return raw


def put_cell(root, ref, value, style=0, formula=None, cell=None):
    if cell is None:
        sheet_data = root.find(T("sheetData"))
        if sheet_data is None: sheet_data=ET.SubElement(root,T("sheetData"))
        y=coord(ref)[1]
        row=next((r for r in sheet_data if int(r.get("r"))==y),None)
        if row is None: row=ET.SubElement(sheet_data,T("row"),r=str(y))
        cell=next((c for c in row if c.get("r")==ref),None)
        if cell is None: cell=ET.SubElement(row,T("c"),r=ref,s=str(style))
    cell.attrib.pop("t",None)
    for child in list(cell): cell.remove(child)
    if formula is not None:
        if isinstance(value,str): cell.set("t","str")
        if isinstance(formula,ET.Element): cell.append(copy.deepcopy(formula))
        else: ET.SubElement(cell,T("f")).text=formula
        ET.SubElement(cell,T("v")).text=str(value)
    elif isinstance(value,(int,float)) and math.isfinite(value):
        ET.SubElement(cell,T("v")).text=str(value)
    else:
        cell.set("t","inlineStr")
        ET.SubElement(ET.SubElement(cell,T("is")),T("t"),{"{http://www.w3.org/XML/1998/namespace}space":"preserve"}).text=str(value or "")
    return cell


def refresh_formula_caches(book,cell_maps,config,write):
    """Evaluate the templates' SUM/ROW/arithmetic formulas without executing VBA."""
    from openpyxl.formula.tokenizer import Tokenizer
    done={}; visiting=set()
    summaries=set(config.get("template_data",{}).get("summary_cells",{}))
    summary_end=max((r.get("summary_row",0) for r in config["rooms"]),default=9)+1
    def value(name,ref):
        key=(name,ref)
        if key in done: return done[key]
        cell=cell_maps.get(name,{}).get(ref)
        if cell is None: return 0
        formula=cell.findtext(T("f"))
        if not formula or "getcolorcount" in formula.lower() or name in summaries and coord(ref)[1]>summary_end:
            return book.value(cell)
        if key in visiting: raise CabinetError("模板存在循环公式："+name+"!"+ref)
        visiting.add(key)
        bindings={}; expression=[]
        for token in Tokenizer("="+formula).items:
            if token.type=="OPERAND" and token.subtype=="RANGE":
                target=token.value; sheet=name
                if "!" in target: sheet,target=target.rsplit("!",1); sheet=sheet.strip("'").replace("''", "'")
                target=target.replace("$","")
                if ":" in target:
                    x,y,x2,y2=bounds(target)
                    v=[value(sheet,f"{col_name(xx)}{yy}") for yy in range(y,y2+1) for xx in range(x,x2+1)]
                else: v=value(sheet,target)
                var="r"+str(len(bindings)); bindings[var]=v; expression.append(var)
            elif token.type=="FUNC": expression.append(token.value.upper())
            elif token.type=="WSPACE": continue
            elif token.type in ("OPERAND","OPERATOR-INFIX","OPERATOR-PREFIX","PAREN","SEP"): expression.append(token.value.replace("^","**"))
            else: raise CabinetError("模板公式类型不支持："+formula)
        def number(v): return float(v) if isinstance(v,(float,int)) else 0 if v in (None,"") else float(v)
        def evaluate(node):
            if isinstance(node,ast.Constant) and isinstance(node.value,(int,float,str)): return node.value
            if isinstance(node,ast.Name) and node.id in bindings: return bindings[node.id]
            if isinstance(node,ast.UnaryOp) and isinstance(node.op,(ast.USub,ast.UAdd)):
                n=number(evaluate(node.operand)); return -n if isinstance(node.op,ast.USub) else n
            if isinstance(node,ast.BinOp):
                a,b=number(evaluate(node.left)),number(evaluate(node.right))
                if isinstance(node.op,ast.Add): return a+b
                if isinstance(node.op,ast.Sub): return a-b
                if isinstance(node.op,ast.Mult): return a*b
                if isinstance(node.op,ast.Div): return a/b
                if isinstance(node.op,ast.Pow): return a**b
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Name):
                if node.func.id=="ROW" and not node.args: return coord(ref)[1]
                if node.func.id=="SUM":
                    items=[evaluate(a) for a in node.args]
                    return sum(v for item in items for v in (item if isinstance(item,list) else [item]) if isinstance(v,(int,float)))
                if node.func.id=="COUNTA":
                    return sum(v not in (None,"") for a in node.args for item in [evaluate(a)] for v in (item if isinstance(item,list) else [item]))
                if node.func.id in ("COUNTIF","COUNTIFS"):
                    args=[evaluate(a) for a in node.args]
                    pairs=[(args[i] if isinstance(args[i],list) else [args[i]],args[i+1]) for i in range(0,len(args),2)]
                    return sum(all(values[i]==expected for values,expected in pairs) for i in range(len(pairs[0][0])))
            raise CabinetError("模板公式类型不支持："+formula)
        try: result=evaluate(ast.parse("".join(expression),mode="eval").body)
        except (ValueError,SyntaxError,ZeroDivisionError) as exc: raise CabinetError("模板公式计算失败："+name+"!"+ref) from exc
        finally: visiting.remove(key)
        write(name,ref,result,preserve_formula=True); done[key]=result
        return result
    for name,cells in cell_maps.items():
        for ref,cell in list(cells.items()):
            if cell.find(T("f")) is not None: value(name,ref)


def export_workbook(content, config, operations):
    """Fill the original sheets from Feishu rows; preserve layout, VBA and sheet identities."""
    book=Workbook(content)
    template=config.get("template_data")
    if not template: raise CabinetError("飞书缺少模板映射资料")
    if template["hash"]!=hashlib.sha256(content).hexdigest(): raise CabinetError("布局模板版本与飞书来源不一致")
    formats={f["sheet"]:copy.deepcopy(f) for f in template["formats"]}
    roots={name:book.sheet(name) for name in book.sheets}
    cell_maps={name:book.cells(name) for name in book.sheets}
    merge_values={name:{m.get("ref"):book.value(cell_maps[name].get(m.get("ref").split(":")[0])) for m in roots[name].iter(T("mergeCell"))} for name in formats}
    inherited_cells={name:{f"{col_name(xx)}{yy}":merge_values[name][m.get("ref")] for m in roots[name].iter(T("mergeCell")) for x,y,x2,y2 in [bounds(m.get("ref"))] for yy in range(y,y2+1) for xx in range(x,x2+1) if (xx,yy)!=(x,y)} for name in formats}
    from openpyxl.formula.translate import Translator
    shared_groups=defaultdict(list)
    for name,cells in cell_maps.items():
        masters={c.find(T("f")).get("si"):(ref,c.findtext(T("f"))) for ref,c in cells.items() if c.find(T("f")) is not None and c.find(T("f")).get("t")=="shared" and c.findtext(T("f"))}
        for ref,cell in cells.items():
            formula=cell.find(T("f"))
            if formula is not None and formula.get("t")=="shared":
                shared_groups[(name,formula.get("si"))].append((ref,copy.deepcopy(formula)))
                origin,text=masters[formula.get("si")]
                formula.text=Translator("="+text,origin=origin).translate_formula(ref)[1:]
                formula.attrib.clear()
    original_formulas={(name,ref):copy.deepcopy(c.find(T("f"))) for name,cells in cell_maps.items() for ref,c in cells.items() if c.find(T("f")) is not None}
    expanded_shared={(name,ref):original_formulas[(name,ref)].text for (name,_),members in shared_groups.items() for ref,_ in members}
    styles=ET.fromstring(book.archive.read("xl/styles.xml"))
    fills=styles.find(T("fills")); xfs=styles.find(T("cellXfs")); painted={}
    def write(name,ref,value,preserve_formula=False):
        cells=cell_maps[name]; prior=cells.get(ref)
        formula=original_formulas.get((name,ref)) if preserve_formula else None
        if prior is None:
            x,y=coord(ref); fmt=formats.get(name)
            if fmt and y>fmt["header"]:
                sample=cells.get(f"{col_name(x)}{fmt['header']+1}")
                if sample is not None:
                    prior=put_cell(roots[name],ref,"",int(sample.get("s",0)))
                    sheet_data=roots[name].find(T("sheetData")); source_row=next((r for r in sheet_data if r.get("r")==str(fmt['header']+1)),None)
                    target_row=next(r for r in sheet_data if r.get("r")==str(y))
                    if source_row is not None:
                        for key in ("ht","customHeight","s","customFormat"):
                            if key in source_row.attrib: target_row.set(key,source_row.get(key))
        cells[ref]=put_cell(roots[name],ref,value,int(prior.get("s",0)) if prior is not None else 0,formula=formula,cell=prior)
        return cells[ref]
    def value_for_date(value):
        found=dates(value)
        if len(found)==1:
            return (dt.datetime.strptime(found[0],"%Y-%m-%d %H:%M:%S")-dt.datetime(1899,12,30)).total_seconds()/86400
        return value or ""
    for name,values in config.get("map_values",{}).items():
        if name in roots:
            for ref in list(cell_maps[name]): write(name,ref,values.get(ref,""),preserve_formula=True)
    # Clear original business rows, including records removed in Feishu.
    for name,fmt in formats.items():
        for rn,row in list(book.rows(name)):
            if rn<=fmt["header"] or not text_value(row.get(fmt["rack"])): continue
            for col in row: write(name,f"{col_name(col)}{rn}","")
    next_rows={name:max((rn for rn,_ in book.rows(name)),default=1)+1 for name in formats}
    max_cols={name:max(coord(ref)[0] for ref in cell_maps[name]) for name in formats}
    used=set()
    group_cells={}
    for op in operations:
        name=op.get("source","")
        if name not in formats:
            category=op.get("category","mixed")
            name=next((n for n in formats if ("下电" in n and "上下电" not in n)==(category=="down")),next(iter(formats)))
        fmt=formats[name]; meta=op.get("meta",{})
        original_row=op.get("source_row")
        rn=int(original_row) if original_row and int(original_row)>fmt["header"] and meta.get("scope")==config["scope"] else next_rows[name]
        if (name,rn) in used: raise CabinetError("多条飞书记录指向同一个源表行")
        used.add((name,rn)); next_rows[name]=max(next_rows[name],rn+1)
        # Original ancillary values come from Feishu's lossless row payload.
        for col,value in meta.get("cells",{}).items():
            if str(col).isdigit() and 1<=int(col)<=16384: write(name,f"{col_name(int(col))}{rn}",value,preserve_formula=True)
        for col,value in ((1,meta.get("cells",{}).get("1",op.get("ordinal",rn-fmt["header"]))),(2,"EA118"),(fmt["room"],op["system_name"]),(fmt["rack"],op["rack"]),(fmt["type"],op["rack_type"]),(fmt["power"],op.get("power","")),(fmt["result"],op.get("result",""))):
            if col: write(name,f"{col_name(col)}{rn}",value,preserve_formula=text_value(value)==text_value(meta.get("cells",{}).get(str(col))))
        groups=copy.deepcopy(op.get("groups",[])); continuation_groups=[]
        if meta.get("source_completed"):
            source_count=len(template["formats"][next(i for i,f in enumerate(template["formats"]) if f["sheet"]==name)]["groups"])
            base_groups=[{} for _ in range(source_count)]; extras=[]
            for g in groups:
                if g.get("source_row") and g["source_row"]!=original_row: continuation_groups.append(g)
                elif g.get("source_group") is not None: base_groups[g["source_group"]]=g
                else: extras.append(g)
            groups=base_groups+extras
            for continuation in meta.get("continuations",[]):
                cr=continuation["row"]
                for col,value in continuation["cells"].items(): write(name,f"{col_name(int(col))}{cr}",value,preserve_formula=True)
                for gm in fmt["groups"][:source_count]:
                    for kind,col in gm.items():
                        if col: group_cells[(name,f"{col_name(col)}{cr}")]=""
        if not meta.get("sheet") and op.get("category")=="down" and len(groups)==1:
            groups=[{} for _ in fmt["groups"][:-1]]+groups
        while len(groups)>len(fmt["groups"]):
            last=max_cols[name]; max_cols[name]=last+3
            new={"action":last+1,"expected":last+2,"actual":last+3}; fmt["groups"].append(new)
            sample_group=fmt["groups"][-2]
            cols=roots[name].find(T("cols"))
            if cols is None:
                cols=ET.Element(T("cols")); roots[name].insert(list(roots[name]).index(roots[name].find(T("sheetData"))),cols)
            for k,label in (("action","操作类型"),("expected","期望完成时间"),("actual","实际完成时间")):
                source_col=sample_group.get(k) or sample_group["actual"]
                width=next((c.get("width") for c in cols if int(c.get("min"))<=source_col<=int(c.get("max"))),"22")
                ET.SubElement(cols,T("col"),min=str(new[k]),max=str(new[k]),width=width,customWidth="1")
                sample=cell_maps[name].get(f"{col_name(source_col)}{fmt['header']}")
                cell=write(name,f"{col_name(new[k])}{fmt['header']}",label)
                if sample is not None: cell.set("s",sample.get("s","0"))
                sample=cell_maps[name].get(f"{col_name(source_col)}{fmt['header']+1}")
                if sample is not None: write(name,f"{col_name(new[k])}{fmt['header']+1}","").set("s",sample.get("s","0"))
        targets=[(rn,gmap,groups[i] if i<len(groups) else {}) for i,gmap in enumerate(fmt["groups"])]
        targets.extend((g["source_row"],fmt["groups"][g["source_group"]],g) for g in continuation_groups)
        for target_row,gmap,g in targets:
            for kind in ("action","expected","actual"):
                col=gmap.get(kind)
                if not col: continue
                value=g.get(kind,"")
                # Preserve original Excel serial precision and original multi-line formatting.
                raw_cells=meta.get("cells",{}) if target_row==rn else next((r["cells"] for r in meta.get("continuations",[]) if r["row"]==target_row),{})
                raw=raw_cells.get(str(col))
                raw_text=text_value(raw)
                if kind!="action" and len(dates(raw))==1: raw_text=dates(raw)[0]
                if raw is not None and text_value(value)==raw_text: value=raw
                elif kind!="action": value=value_for_date(value)
                ref=f"{col_name(col)}{target_row}"
                if meta.get("schema",0)<3 and ref in inherited_cells[name] and raw in (None,"") and value in (None,""): continue
                if ref in inherited_cells[name] and g.get("source_resolved",{}).get(kind)==g.get(kind): value=inherited_cells[name][ref]
                group_cells[(name,ref)]=value
    # Retain unchanged source merges; split only a merge whose members were independently edited.
    for name in formats:
        merges=roots[name].find(T("mergeCells"))
        if merges is None: continue
        for merge in list(merges):
            x,y,x2,y2=bounds(merge.get("ref")); refs=[f"{col_name(xx)}{yy}" for yy in range(y,y2+1) for xx in range(x,x2+1)]
            if not any((name,ref) in group_cells for ref in refs): continue
            anchor=refs[0]; old=merge_values[name][merge.get("ref")]
            desired=[group_cells.get((name,ref),old) for ref in refs]
            if all(text_value(v)==text_value(desired[0]) for v in desired):
                for ref in refs: group_cells.pop((name,ref),None)
                group_cells[(name,anchor)]=desired[0]
            else:
                merges.remove(merge)
                for ref,value in zip(refs,desired): group_cells[(name,ref)]=value
        merges.set("count",str(len(merges)))
    for (name,ref),value in group_cells.items(): write(name,ref,value)
    for name in formats:
        fmt=formats[name]; root=roots[name]; dimension=root.find(T("dimension"))
        if dimension is not None:
            max_col=max_cols[name]
            dimension.set("ref",f"A1:{col_name(max_col)}{next_rows[name]-1}")
            filt=root.find(T("autoFilter"))
            if filt is not None: filt.set("ref",f"A{fmt['header']}:{col_name(max_col)}{next_rows[name]-1}")
    derived=derive_records(config,operations)
    by_room={room["id"]:[r for r in derived["racks"] if r["room"]==room["id"]] for room in config["rooms"]}
    for rack in derived["racks"]:
        for pos in rack.get("positions",[]):
            if pos["sheet"] not in roots: continue
            root=roots[pos["sheet"]]; cells=cell_maps[pos["sheet"]]
            x1,y1,x2,y2=bounds(pos["range"])
            for x in range(x1,x2+1):
                for y in range(y1,y2+1):
                    cell=cells.get(f"{col_name(x)}{y}")
                    if cell is None: continue
                    key=(cell.get("s","0"),rack["state"])
                    if key not in painted:
                        fill=ET.SubElement(fills,T("fill")); pat=ET.SubElement(fill,T("patternFill"),patternType="solid")
                        ET.SubElement(pat,T("fgColor"),rgb="FF"+COLORS[rack["state"]].lstrip("#")); ET.SubElement(pat,T("bgColor"),indexed="64")
                        xf=copy.deepcopy(xfs[int(key[0])]); xf.set("fillId",str(len(fills)-1)); xf.set("applyFill","1")
                        painted[key]=len(xfs); xfs.append(xf)
                        book.styles.append({**book.styles[int(key[0])],"fill":COLORS[rack["state"]]})
                    cell.set("s",str(painted[key]))
    # Preserve all original summary sections; refresh the labelled room metrics.
    for name,values in template.get("summary_cells",{}).items():
        if name not in roots: continue
        for ref,value in values.items(): write(name,ref,value,preserve_formula=True)
        metrics_rows=[]; total_rows=[]
        for rn,row in list(book.rows(name)):
            if rn>max((r.get("summary_row",0) for r in config["rooms"]),default=9)+1: continue
            label=text_value(row.get(1)); match=re.search(r"(?<!\d)([1-4]\d{2})(?!\d)",label)
            if label in ("总计","合计"): total_rows.append(rn)
            if not match or match[1] not in by_room: continue
            rr=by_room[match[1]]; room=next(r for r in config["rooms"] if r["id"]==match[1])
            net=[r for r in rr if r["rack_type"]==RACK_TYPES[0]]; servers=[r for r in rr if r["rack_type"]==RACK_TYPES[1]]
            powered=lambda items:sum(r["state"] in ("formal","test") for r in items)
            off=lambda items:sum(r["state"]=="off" for r in items)
            metrics=[room["total"],powered(rr),off(rr),len(net),powered(net),off(net),len(servers),powered(servers),off(servers)]
            gap=derived["unlocated"][room["id"]]
            metrics[2]+=gap["off"]
            if config["scope"]=="B" and room["id"] in ("216","247"):
                metrics[3]+=gap["total"]; metrics[5]+=gap["off"]
            metrics_rows.append(metrics)
            for col,value in enumerate(metrics,2): write(name,f"{col_name(col)}{rn}",value,preserve_formula=True)
        for rn in total_rows:
            for col,value in enumerate(map(sum,zip(*metrics_rows)),2): write(name,f"{col_name(col)}{rn}",value,preserve_formula=True)
        # Notes stay alongside the original summary instead of adding/replacing sheets.
        column=max(coord(ref)[0] for ref in values)+2
        write(name,f"{col_name(column)}1","未确认状态机柜")
        write(name,f"{col_name(column+1)}1",derived["counts"]["unknown"])
        write(name,f"{col_name(column)}2","数据更新")
        write(name,f"{col_name(column+1)}2",dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if "baseline_event_ids" in config:
            baseline=set(config["baseline_event_ids"]); baseline_keys=set(); increments={}
            for op in operations:
                for event in op.get("events",[]):
                    key=(op["room"],op["rack"],event["action"],event["actual"])
                    if event.get("id") in baseline: baseline_keys.add(key)
                    elif event.get("result",op.get("result"))=="成功" and event["actual"] and event["action"].startswith(("上","下")): increments[key]=event
            daily=defaultdict(Counter); monthly=defaultdict(Counter)
            for key,event in increments.items():
                if key in baseline_keys: continue
                direction="down" if event["action"].startswith("下") else "up"
                daily[event["actual"][:10]][direction]+=1; monthly[event["actual"][:7]][direction]+=1
            start=max(coord(ref)[1] for ref in values)+3
            for label,stats in (("系统新增上下电日统计",daily),("系统新增上下电月统计",monthly)):
                write(name,f"A{start}",label); start+=1
                for col,label in enumerate(("日期","上电数量","下电数量","净增数量"),1): write(name,f"{col_name(col)}{start}",label)
                for period,counts in sorted(stats.items()):
                    start+=1
                    for col,value in enumerate((period,counts["up"],counts["down"],counts["up"]-counts["down"]),1):
                        cell=write(name,f"{col_name(col)}{start}",value)
                        sample=cell_maps[name].get(f"{col_name(col)}4")
                        if sample is not None: cell.set("s",sample.get("s","0"))
                start+=3
    # Update cached colour counts on the existing drawings without executing VBA.
    for room in config["rooms"]:
        name=room.get("sheet")
        if not name: continue
        cells=cell_maps[name]
        for cell in cells.values():
            formula=cell.findtext(T("f"),"")
            match=re.search(r"getcolorcount\(\s*([A-Z0-9$]+:[A-Z0-9$]+)\s*[,;]\s*([A-Z0-9$]+)\s*\)",formula,re.I)
            if not match: continue
            region=bounds(match[1].replace("$","")); sample=book.cells(name).get(match[2].replace("$",""))
            if sample is None: continue
            # Legend sample styles are unchanged by cabinet state painting.
            sample_style=int(sample.get("s",0)); color=book.styles[sample_style].get("fill","").upper() if sample_style<len(book.styles) else ""
            state={"#FF0000":"formal","#FFC000":"test","#00B050":"off","#92D050":"off"}.get(color)
            if not state: continue
            count=0
            for ref,member in cells.items():
                x,y=coord(ref)
                if not (region[0]<=x<=region[2] and region[1]<=y<=region[3]): continue
                fill=fills[int(xfs[int(member.get("s",0))].get("fillId",0))]
                fg=fill.find(f"{T('patternFill')}/{T('fgColor')}")
                color="#"+fg.get("rgb","")[-6:].upper() if fg is not None else ""
                member_state={"#FF0000":"formal","#FFC000":"test","#00B050":"off","#92D050":"off"}.get(color)
                count+=member_state==state
            cache=cell.find(T("v"))
            if cache is None: cache=ET.SubElement(cell,T("v"))
            cache.text=str(count)
        projected=project_layout(book.layout(name,room["region"]),by_room[room["id"]],derived['racks'])
        whole_building=any(c.get('metric_building') for c in projected['cells'])
        members=derived['racks'] if whole_building else by_room[room["id"]]; helper=max(coord(ref)[0] for ref in cells)+2
        cols=roots[name].find(T("cols"))
        if cols is None:
            cols=ET.Element(T("cols")); roots[name].insert(list(roots[name]).index(roots[name].find(T("sheetData"))),cols)
        ET.SubElement(cols,T("col"),min=str(helper),max=str(helper+3),hidden="1",width="12",customWidth="1")
        for rn,rack in enumerate(members,1):
            for offset,key in enumerate(("rack","state","rack_type","room")): write(name,f"{col_name(helper+offset)}{rn}",rack[key])
        state_range=f"${col_name(helper+1)}$1:${col_name(helper+1)}${max(1,len(members))}"
        type_range=f"${col_name(helper+2)}$1:${col_name(helper+2)}${max(1,len(members))}"
        room_range=f"${col_name(helper+3)}$1:${col_name(helper+3)}${max(1,len(members))}"
        for item in projected["cells"]:
            if not item.get("metric"): continue
            metric=item["metric"]; kind=item.get("metric_type")
            def count(state):
                criteria=[]
                if state: criteria.extend((state_range,'"'+state+'"'))
                if kind: criteria.extend((type_range,'"'+kind+'"'))
                if whole_building and not item.get('metric_building'): criteria.extend((room_range,'"'+room['id']+'"'))
                return 'COUNTIFS('+','.join(criteria)+')' if criteria else f'COUNTA(${col_name(helper)}$1:${col_name(helper)}${max(1,len(members))})'
            if metric=="total": formula=count('')
            elif metric=="powered": formula=count("formal")+"+"+count("test")
            else: formula=count(metric)
            node=ET.Element(T("f")); node.text=formula; original_formulas[(name,item["ref"])]=node
            write(name,item["ref"],int(item["text"]) if item["text"].isdigit() else 0,preserve_formula=True)
    refresh_formula_caches(book,cell_maps,config,write)
    for (name,_),members in shared_groups.items():
        if all(cell_maps[name][ref].findtext(T("f"))==expanded_shared[(name,ref)] and cell_maps[name][ref].find(T("f")) is not None for ref,_ in members):
            for ref,original in members:
                cell=cell_maps[name][ref]; cell.remove(cell.find(T("f"))); cell.insert(0,original)
    fills.set("count",str(len(fills))); xfs.set("count",str(len(xfs)))
    # Retain style identities, VBA, drawings, relationships, print ranges and sheet names.
    parts={e.filename:book.archive.read(e.filename) for e in book.archive.infolist()}
    for name,root in roots.items():
        for row in root.find(T("sheetData")):
            row[:]=sorted(row,key=lambda c:coord(c.get("r"))[0] if c.tag==T("c") else 0)
        data=root.find(T("sheetData")); data[:]=sorted(data,key=lambda r:int(r.get("r")))
        dimension=root.find(T("dimension"))
        if dimension is not None:
            coords=[coord(ref) for ref in cell_maps[name]]
            dimension.set("ref",f"A1:{col_name(max(c[0] for c in coords))}{max(c[1] for c in coords)}")
        parts[book.sheets[name]]=xml_bytes(root,parts[book.sheets[name]])
    parts["xl/styles.xml"]=xml_bytes(styles,parts["xl/styles.xml"])
    out=io.BytesIO()
    with zipfile.ZipFile(out,"w",zipfile.ZIP_DEFLATED) as archive:
        for name,data in parts.items(): archive.writestr(name,data)
    verified=Workbook(out.getvalue())
    if list(verified.sheets)!=list(book.sheets): raise CabinetError("导出改变了原模板工作表")
    if "xl/vbaProject.bin" in parts and verified.archive.read("xl/vbaProject.bin")!=book.archive.read("xl/vbaProject.bin"): raise CabinetError("宏资源校验失败")
    return out.getvalue()

"""Feishu business data with local immutable map/export templates."""
from __future__ import annotations
import copy
import datetime as dt
import gzip
import hashlib
import json
import math
import multiprocessing
import os
import re
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path
from .cabinet_power_excel import CabinetError, COLORS, OPS, RACK_TYPES, STATES, TOTALS, baseline_correction_operations, baseline_matches_inventory, calculate, derive_records, dates, digest, export_workbook, inventory_state_baseline, operation_key, system_name, text_value, project_layout,completed_state_event
from .cabinet_power_data import from_feishu, to_fields, group_events, source_sheet, table_columns, normalized_actions
from .cabinet_power_store import CabinetStore

APP_TOKEN="ASLxbfESPahdTKs0A9NccgbrnXc"
TABLE_ID="tblPuXz8ONJQVrDe"
EXPORT_ARCHIVE_APP_TOKEN="MliKbC3fXa8PXrsndKscmxjdn1g"
EXPORT_ARCHIVE_TABLE_ID="tblghte5RCmMy24n"
EXPORT_ARCHIVE_URL=f"https://vnet.feishu.cn/base/{EXPORT_ARCHIVE_APP_TOKEN}?table={EXPORT_ARCHIVE_TABLE_ID}&view=vewtV2DWdh"
EXPORT_ARCHIVE_FIELDS={"导出标识":1,"批次标识":1,"楼栋":1,"文件名称":1,"数据版本":2,"导出时间":1,"文件SHA256":1,"导出人":1,"导出文件":17}
NAMESPACE="cabinet_power"
DIRECTORY_NAME="机柜基础资料"
INITIAL_TEMPLATES=Path(__file__).with_name("templates")/"cabinet_power"
LAYOUT_CACHE=INITIAL_TEMPLATES/"layouts.json.gz"
SNAPSHOT_KEY="feishu_snapshot_v1:"
FROZEN_BASELINE_KEY="power_baseline:frozen_v1"

def layout_identity(config):
    rooms=sorted((room["id"],int(room.get("total") or 0),room.get("sheet","").strip()) for room in config.get("rooms",[]))
    racks=sorted((rack["room"],rack["rack"],rack.get("template_color",""),
                  sorted((item.get("sheet","").strip(),item.get("range","")) for item in rack.get("positions",[])))
                 for rack in config.get("inventory",[]))
    return digest([rooms,racks])

EXPORT_FORMAT_VERSION = 1


def export_snapshot(config,operations):
    return export_workbook((INITIAL_TEMPLATES/(config["scope"]+".xlsm")).read_bytes(),config,operations)

def lower_export_priority():
    if os.name != "nt":
        return
    try:
        import ctypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.SetPriorityClass(kernel.GetCurrentProcess(), 0x00004000)  # BELOW_NORMAL_PRIORITY_CLASS
    except Exception:
        pass

def process_alive(pid):
    if not isinstance(pid,int) or pid<=0: return False
    if pid==os.getpid(): return True
    if os.name=="nt":
        import ctypes
        kernel=ctypes.WinDLL("kernel32",use_last_error=True)
        kernel.OpenProcess.restype=ctypes.c_void_p
        kernel.CloseHandle.argtypes=[ctypes.c_void_p]
        kernel.GetExitCodeProcess.argtypes=[ctypes.c_void_p,ctypes.POINTER(ctypes.c_ulong)]
        handle=kernel.OpenProcess(0x1000,False,pid)
        if not handle: return False
        try:
            code=ctypes.c_ulong()
            return bool(kernel.GetExitCodeProcess(handle,ctypes.byref(code))) and code.value==259
        finally: kernel.CloseHandle(handle)
    try: os.kill(pid,0); return True
    except ProcessLookupError: return False
    except PermissionError: return True

def stamp():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _stable_uuid4_client_token(value):
    raw=bytearray(hashlib.sha256(str(value or "").encode()).digest()[:16])
    raw[6]=(raw[6]&0x0F)|0x40; raw[8]=(raw[8]&0x3F)|0x80
    return str(uuid.UUID(bytes=bytes(raw)))

def equivalent(fields,actual):
    for key,value in fields.items():
        got=actual.get(key)
        if key=="上下电确认截图":
            tokens=lambda items:{str(item.get("file_token") or "") for item in items or [] if isinstance(item,dict) and item.get("file_token")}
            if tokens(value)==tokens(got): continue
            return False
        if value in (None,"") and got in (None,""): continue
        if isinstance(value,(int,float)):
            try:
                if float(got)==value: continue
            except (ValueError,TypeError): pass
        elif text_value(got)==text_value(value): continue
        return False
    return True

class CabinetFeishu:
    """Reuse the app's HTTP pool and token manager."""
    def __init__(self,table_id=TABLE_ID,app_token=APP_TOKEN):
        self.table_id=table_id; self.app_token=app_token
        self._token=""; self._expires=0
        self._schema_ready=False
        self._lock=threading.RLock()
        self._http=None

    def token(self):
        with self._lock:
            if self._token and time.monotonic()<self._expires: return self._token
            from upload_event_module.config import config
            from upload_event_module.services.feishu_token_manager import token_manager
            try: token=token_manager.get_tenant_token()
            except Exception as exc: raise CabinetError("机柜台账授权失败，请检查飞书应用配置与表权限") from exc
            if not token: raise CabinetError("机柜台账授权失败，请检查飞书应用配置与表权限")
            self._token=token; self._expires=time.monotonic()+3000
            return token

    @staticmethod
    def require_write():
        from .portal_service import external_real_write_guard
        guard=external_real_write_guard()
        if not guard["real_write_allowed"]: raise CabinetError(guard["reason"],403)

    def request(self,method,path,body=None,params=None):
        read_request=method=="GET" or method=="POST" and path=="records/batch_get"
        if not read_request: self.require_write()
        import httpx
        from upload_event_module.services.http_client import FeishuHttpClient
        if self._http is None:
            self._http=FeishuHttpClient(timeout=httpx.Timeout(connect=5,read=60,write=60,pool=10),retries=0)
        root=f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}"
        url=f"{root}/tables/{self.table_id}/{path}" if self.table_id else f"{root}/{path}"
        for attempt in range(3 if read_request else 1):
            data=self._http.request_json(method,url,headers={"Authorization":"Bearer "+self.token()},params=params,json_payload=body,retries=1 if read_request else 0)
            if not read_request or data.get("code") not in (1255002,1254290,1254291,1254607) or attempt==2: break
            time.sleep(0.5*(2**attempt))
        if data.get("code"):
            if data["code"] in (99991663,99991664,99991665): self._expires=0
            raise CabinetError(f"机柜台账请求失败：code={data['code']}，{data.get('msg','')}")
        return data.get("data",{})

    def list_all(self,path="records",filters=None):
        items=[]; token=""; seen=set()
        while True:
            page=self.request("GET",path,params={"page_size":500,**({"page_token":token} if token else {}),**({"filter":filters} if filters else {})})
            items.extend(page.get("items",[]))
            if not page.get("has_more"): return items
            token=page.get("page_token","")
            if not token or token in seen: raise CabinetError("飞书分页游标异常，保留上次快照")
            seen.add(token)

    def get(self,record_id):
        if not re.fullmatch(r"rec[A-Za-z0-9]+",record_id): raise CabinetError("无效记录ID")
        return self.request("GET",f"records/{record_id}")["record"]

    def create(self,fields,operation_id):
        client_token=_stable_uuid4_client_token("cabinet:"+operation_id)
        return self.request("POST","records",{"fields":fields},params={"client_token":client_token})["record"]

    def update(self,record_id,fields):
        return self.request("PUT",f"records/{record_id}",{"fields":fields})["record"]

    def batch_create(self,rows):
        token=_stable_uuid4_client_token("cabinet:batch:"+digest(rows))
        return self.request("POST","records/batch_create",{"records":[{"fields":fields} for fields in rows]},params={"client_token":token})["records"]

    def batch_update(self,rows):
        return self.request("POST","records/batch_update",{"records":rows})["records"]

    def batch_get(self,record_ids):
        return self.request("POST","records/batch_get",{"record_ids":record_ids})["records"]

    def delete(self,record_id):
        if not re.fullmatch(r"rec[A-Za-z0-9]+",record_id): raise CabinetError("无效记录ID")
        self.request("DELETE",f"records/{record_id}")

    def upload_attachment(self,path,file_name):
        self.require_write(); path=Path(path).resolve()
        if not path.is_file() or path.stat().st_size<=0: raise CabinetError("机柜导出文件不存在或为空")
        if self._http is None:
            import httpx
            from upload_event_module.services.http_client import FeishuHttpClient
            self._http=FeishuHttpClient(timeout=httpx.Timeout(connect=5,read=60,write=60,pool=10),retries=0)
        payload={}
        for attempt in range(2):
            try:
                payload=self._http.request_file_json(
                    "POST","https://open.feishu.cn/open-apis/drive/v1/medias/upload_all",
                    headers={"Authorization":"Bearer "+self.token()},
                    data={"file_name":file_name,"parent_type":"bitable_file","parent_node":self.app_token,"size":str(path.stat().st_size)},
                    file_path=str(path),file_name=file_name,retries=1,
                )
            except Exception as exc: raise CabinetError(f"机柜导出附件上传失败：{exc}") from exc
            code=int(payload.get("code") or 0)
            if code not in (99991663,99991664,99991665) or attempt: break
            self._expires=0
        token=str((payload.get("data") or {}).get("file_token") or "").strip()
        if int(payload.get("code") or 0) or not token: raise CabinetError(f"机柜导出附件上传失败：{payload.get('msg') or '未返回文件标识'}")
        return token

    def download_attachment(self,file_token):
        from urllib.parse import quote
        if not re.fullmatch(r"[A-Za-z0-9_-]{10,200}",str(file_token or "")):
            raise CabinetError("确认截图标识无效",400)
        if self._http is None:
            import httpx
            from upload_event_module.services.http_client import FeishuHttpClient
            self._http=FeishuHttpClient(timeout=httpx.Timeout(connect=5,read=60,write=60,pool=10),retries=0)
        content,_type=self._http.request_bytes(
            "GET","https://open.feishu.cn/open-apis/drive/v1/medias/"+quote(file_token,safe="")+"/download",
            headers={"Authorization":"Bearer "+self.token()},retries=1,max_bytes=10*1024*1024,
        )
        return content

    def ensure_fields(self):
        if self._schema_ready: return True
        fields={f["field_name"]:f for f in self.list_all("fields")}
        required={"机房":3,"楼栋":3,"包间系统名称":1,"机架":1,"操作类型":3,"期望完成时间":5,"实际完成时间":5,"机柜类型":3,"机柜功率（W）":2,"结果":3,"操作类型（说明）":1,"完成时间":1,"序号":1005}
        for name,kind in required.items():
            if name not in fields or fields[name]["type"]!=kind: raise CabinetError(f"台账字段缺失或类型错误：{name}")
        from .cabinet_power_data import EXTRA_FIELDS
        for name,kind in EXTRA_FIELDS.items():
            if name not in fields and name in ("上下电确认截图", "失败原因"):
                try:
                    self.request("POST", "fields", {"field_name": name, "type": kind})
                except CabinetError:
                    pass
                fields={f["field_name"]:f for f in self.list_all("fields")}
            if name not in fields or fields[name]["type"]!=kind: raise CabinetError(f"台账字段缺失或类型错误：{name}")
        for name,options in (("操作类型",OPS),("机柜类型",RACK_TYPES),("结果",("成功","失败"))):
            field=fields[name]; prop=copy.deepcopy(field.get("property",{})); current=prop.setdefault("options",[])
            missing=[n for n in options if n not in {o["name"] for o in current}]
            if missing:
                current.extend({"name":n} for n in missing)
                self.request("PUT",f"fields/{field['field_id']}",{"field_name":name,"type":3,"property":prop})
        self._schema_ready=True
        return True



class CabinetPowerService:
    def __init__(self,store,remote=None,root=None,export_remote=None):
        self.store=store; self.remote=remote or CabinetFeishu()
        self.export_remote=export_remote if export_remote is not None else (CabinetFeishu(EXPORT_ARCHIVE_TABLE_ID,EXPORT_ARCHIVE_APP_TOKEN) if isinstance(self.remote,CabinetFeishu) else None)
        self.root=(Path(root) if root else Path(store.db_path).parent/"cabinet_power").resolve()
        self.local=CabinetStore(self.root/"buildings")
        self._lock=threading.RLock()
        self._bootstrap_lock=threading.Lock()
        self._export_schema_lock=threading.Lock()
        self._export_schema_ready=False
        self._refresh_slots=threading.BoundedSemaphore(2)
        self._scope_locks={scope:threading.RLock() for scope in TOTALS}
        self._export_upload_locks={scope:threading.Lock() for scope in TOTALS}
        self._cache={}; self._directory=None; self._layouts={}; self._remotes={}; self._directories={}
        self._pools={s:ThreadPoolExecutor(max_workers=1,thread_name_prefix="cabinet-"+s) for s in TOTALS}
        self._upload_pools={s:ThreadPoolExecutor(max_workers=1,thread_name_prefix='cabinet-upload-'+s) for s in TOTALS}
        self._bootstrap_download_pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix="cabinet-bootstrap")
        self._bootstrap_batches={}
        self._writing=set()
        self._exports=None
        self._running={}
        self._batches=None

    def shutdown(self,wait=True,**_kwargs):
        if self._batches: self._batches.shutdown(wait=wait)
        for pool in self._upload_pools.values(): pool.shutdown(wait=wait,cancel_futures=not wait)
        for pool in self._pools.values(): pool.shutdown(wait=wait,cancel_futures=not wait)
        self._bootstrap_download_pool.shutdown(wait=wait,cancel_futures=not wait)
        if self._exports: self._exports.shutdown(wait=wait,cancel_futures=not wait)

    @property
    def pool(self): return self

    @property
    def batches(self):
        with self._lock:
            if self._batches is None:
                from .cabinet_power_batches import CabinetBatchService
                self._batches=CabinetBatchService(self,self.root)
            return self._batches

    def remote_for(self,scope):
        if not isinstance(self.remote,CabinetFeishu): return self.remote
        with self._lock: return self._remotes.setdefault(scope,CabinetFeishu())

    def read(self,key,default=None):
        for scope in TOTALS:
            value=self.local.document(scope,key)
            if value is not None: return value
        value=self.store.get_document(NAMESPACE,key)
        return value if value is not None else copy.deepcopy(default)

    def write(self,key,value):
        scope=value.get("scope")
        if scope not in TOTALS: raise CabinetError("本地任务缺少楼栋")
        self.local.document(scope,key,value)

    def atomic_file(self,name,data):
        path=self.root/name; path.parent.mkdir(parents=True,exist_ok=True)
        fd,temp=tempfile.mkstemp(dir=path.parent,prefix=".cabinet-")
        try:
            with os.fdopen(fd,"wb") as out: out.write(data); out.flush(); os.fsync(out.fileno())
            os.replace(temp,path)
        finally:
            if os.path.exists(temp): os.unlink(temp)
        return str(path)

    def directory(self,scope=None):
        if self._directory is None:
            with self._lock:
                if self._directory is None:
                    found=[t for t in CabinetFeishu("").list_all("tables") if t["name"]==DIRECTORY_NAME]
                    if len(found)!=1: raise CabinetError("飞书机柜基础资料表不存在或重名")
                    self._directory=CabinetFeishu(found[0]["table_id"])
        if scope and isinstance(self._directory,CabinetFeishu):
            with self._lock: return self._directories.setdefault(scope,CabinetFeishu(self._directory.table_id))
        return self._directory

    def _migrate_legacy_snapshot(self,scope):
        if self.local.version(scope): return True
        with self._scope_locks[scope]:
            if self.local.version(scope): return True
            saved=self.store.get_document(NAMESPACE,SNAPSHOT_KEY+scope) or {}
            cached=saved.get("snapshot")
            if cached:
                self.atomic_file(Path("backups")/(scope+"-legacy-snapshot.json"),json.dumps(saved,ensure_ascii=False).encode())
                records=[{"record_id":o["record_id"],"fields":o["raw_fields"]} for o in cached["operations"]]
                baseline=[e["id"] for r in records for e in from_feishu(r)["events"]]
                self.local.replace(scope,cached["config"],records,baseline)
                return True
        return False

    def ensure_loaded(self,scope):
        if self._migrate_legacy_snapshot(scope): return
        self._refresh_missing_buildings(scope)

    def _refresh_missing_buildings(self,requested_scope):
        self.bootstrap("system",start=True)
        while True:
            status=self.bootstrap("system")
            if status["status"] not in ("pending","running"): break
            time.sleep(.1)
        target=next(item for item in status["buildings"] if item["scope"]==requested_scope)
        if not self.local.version(requested_scope):
            raise CabinetError(target.get("error") or "该楼机柜资料从多维表初始化失败",503)

    def _latest_refresh_job(self,scope):
        jobs=sorted(
            (item for item in self.local.documents(scope,"job:") if item.get("kind")=="refresh"),
            key=lambda item:str(item.get("created_at") or ""),reverse=True,
        )
        return self.job_status(jobs[0]["job_id"],scope) if jobs else {}

    def _download_bootstrap_source(self):
        return self.remote.list_all(),self.directory().list_all()

    def _start_bootstrap_batch(self):
        batch_id=uuid.uuid4().hex
        with self._lock:
            self._bootstrap_batches={key:value for key,value in self._bootstrap_batches.items() if not value.done()}
            self._bootstrap_batches[batch_id]=self._bootstrap_download_pool.submit(self._download_bootstrap_source)
        return batch_id

    def _bootstrap_source(self,batch_id):
        with self._lock: future=self._bootstrap_batches.get(batch_id)
        if future is None: raise CabinetError("初始化下载任务已中断，请点击重试",503)
        return future.result()

    def bootstrap(self,owner="",start=False,retry_failed=False):
        if start:
            with self._bootstrap_lock:
                for scope in TOTALS: self._migrate_legacy_snapshot(scope)
                pending=[]
                for scope in TOTALS:
                    if self.local.version(scope): continue
                    current=self._latest_refresh_job(scope)
                    if current and current.get("status") in ("pending","running"): continue
                    if current and not retry_failed: continue
                    pending.append(scope)
                if pending:
                    batch_id=self._start_bootstrap_batch()
                    for scope in pending: self.job(scope,"refresh",owner or "system",{"bootstrap_batch_id":batch_id})
        buildings=[]
        for scope in TOTALS:
            if self.local.version(scope):
                buildings.append({"scope":scope,"status":"succeeded","error":""})
                continue
            current=self._latest_refresh_job(scope)
            status=str(current.get("status") or "idle")
            if status=="succeeded": status="failed"
            buildings.append({"scope":scope,"status":status,"error":str(current.get("error") or ("初始化未生成本地数据" if status=="failed" else ""))})
        ready=sum(item["status"]=="succeeded" for item in buildings)
        failed=sum(item["status"]=="failed" for item in buildings)
        active=any(item["status"] in ("pending","running") for item in buildings)
        if not active:
            with self._lock: self._bootstrap_batches={key:value for key,value in self._bootstrap_batches.items() if not value.done()}
        status="running" if active else "succeeded" if ready==len(TOTALS) else "partial" if ready else "failed" if failed else "idle"
        return {"status":status,"total":len(TOTALS),"completed":ready+failed,"ready":ready,"failed":failed,"buildings":buildings,"updated_at":stamp()}

    def _snapshot(self,scope):
        self.ensure_loaded(scope)
        with self._scope_locks[scope]:
            version=self.local.version(scope)
            if self._cache.get(scope,{}).get("version")==version: return self._cache[scope]
            for _attempt in range(3):
                saved=self.local.load(scope); config=self._packaged_config(scope,saved["config"])
                if layout_identity(config)!=layout_identity(saved["config"]):
                    config["layout_previous_identity"]=layout_identity(saved["config"])
                    self.local.extend_layout(scope,config,saved["version"])
                    continue
                extension=self._layout_data(scope).get("extension") or {}
                if extension.get("types_revision") and self.local.apply_inventory_types(
                        scope,extension["types_revision"],extension["inventory"]): continue
                break
            else: raise CabinetError("机柜目录正在更新，请稍后重试",409)
            config["path"]=str(INITIAL_TEMPLATES/(scope+".xlsm"))
            config["baseline_event_ids"]=saved["baseline"]
            config["version"]=digest([config["rooms"],config["inventory"],config.get("template_data"),config.get("map_values")])
            ops=[]
            for record in saved["records"]:
                op=from_feishu(record); op["ordinal"]=record["ordinal"]; ops.append(op)
            config["power_baseline"]=self._frozen_power_baseline(scope,config,ops)
            ordinal=max((op["ordinal"] for op in ops),default=0)
            for correction in baseline_correction_operations(config,ops):
                ordinal+=1; correction["ordinal"]=ordinal; ops.append(correction)
            for op in ops: op["display_sheet"]=source_sheet(op,config.get("template_data",{}).get("formats",[]))
            snapshot={"config":config,"operations":ops,"version":saved["version"],"updated_at":saved["updated_at"],"source":"local","error":""}
            self._cache[scope]=snapshot
            return snapshot

    def _frozen_power_baseline(self,scope,config,operations):
        identity=layout_identity(config); saved=self.local.document(scope,FROZEN_BASELINE_KEY)
        if saved:
            baseline=saved.get("racks") if isinstance(saved,dict) else None
            data=self._layout_data(scope); extension=data.get("extension") or {}
            if (isinstance(baseline,dict) and saved.get("layout_identity") in (extension.get("from_identity"),config.get("layout_previous_identity"))
                    and identity==data.get("layout_identity")):
                additions=inventory_state_baseline({"scope":scope,"inventory":[rack for rack in extension.get("inventory",[]) if rack["room"]+"/"+rack["rack"] not in baseline]},[])[0]
                extended={**baseline,**additions}
                if set(baseline)&set(additions) or not baseline_matches_inventory({**config,"power_baseline":extended}):
                    raise CabinetError("新增包间基线与现有机柜目录冲突，请先核对模板资料")
                saved={**saved,"layout_identity":identity,"racks":extended,"extended_at":stamp()}
                self.local.document(scope,FROZEN_BASELINE_KEY,saved)
                baseline=extended
            if saved.get("layout_identity")!=identity or not isinstance(baseline,dict):
                raise CabinetError("固定平面图基线与当前机柜目录不一致，请先核对模板资料")
            if not baseline_matches_inventory({**config,"power_baseline":baseline}):
                raise CabinetError("固定平面图基线内容不完整，请先核对模板资料")
            return copy.deepcopy(baseline)
        baseline=config.get("power_baseline") if config.get("power_baseline_frozen") else None
        if not baseline_matches_inventory({**config,"power_baseline":baseline}):
            source=[op for op in operations if text_value(op.get("raw_fields",{}).get("数据标识")).startswith("source_")]
            baseline=inventory_state_baseline(config,source)[0]
        self.local.document(scope,FROZEN_BASELINE_KEY,{"version":1,"layout_identity":identity,
            "source_hash":config.get("power_baseline_source_hash",""),"racks":baseline,"created_at":stamp()})
        return copy.deepcopy(baseline)

    def list_remote(self,remote,scope="",data_id=""):
        if isinstance(remote,CabinetFeishu):
            field,value=("数据标识",data_id) if data_id else ("楼栋",scope+"楼")
            return remote.list_all(filters="CurrentValue.["+field+"]="+json.dumps(value,ensure_ascii=False))
        records=remote if isinstance(remote,list) else remote.list_all()
        return [r for r in records if (text_value(r["fields"].get("数据标识"))==data_id if data_id else text_value(r["fields"].get("楼栋"))==scope+"楼")]

    def do_refresh(self,scope,payload,job):
        if not scope:
            source=self._download_bootstrap_source()
            results=[self.do_refresh(s,{**payload,"_bootstrap_source":source},job) for s in TOTALS]
            return {"updated_at":stamp(),"count":sum(r["count"] for r in results)}
        with self._refresh_slots, self.local.locked([scope]):
            if self.pending_writes(scope) or self.pending_rollbacks(scope): raise CabinetError("该楼有尚未完成的上传或回退，请先继续处理",409)
            source=payload.get("_bootstrap_source")
            if not source and payload.get("bootstrap_batch_id"): source=self._bootstrap_source(payload["bootstrap_batch_id"])
            if source:
                records=self.list_remote(source[0],scope); directory=self.list_remote(source[1],scope)
            else:
                records=self.list_remote(self.remote_for(scope),scope); directory=self.list_remote(self.directory(scope),scope)
            if len({r["record_id"] for r in records})!=len(records): raise CabinetError("飞书分页存在重复记录")
            ops=[from_feishu(r) for r in records]
            configs={scope:{"scope":scope,"rooms":[],"inventory":[],"history_ready":True,"issues":[]}}
            baseline_hashes=set(); baseline_frozen=set(); baseline_sources=set()
            for item in directory:
                f=item["fields"]; s=text_value(f.get("楼栋")).replace("楼","")
                if s not in configs: continue
                try: geometry=json.loads(text_value(f.get("布局资料")) or "{}")
                except ValueError: raise CabinetError("飞书布局资料格式无效："+item["record_id"])
                room=text_value(f.get("包间")); kind=text_value(f.get("类别"))
                if kind=="模板资料": configs[s]["template_data"]=geometry
                if kind=="平面图数值": configs[s]["map_values"]=geometry
                if kind=="房间":
                    baseline=geometry.pop('power_baseline',None)
                    if baseline:
                        if baseline.get('version')!=1 or not isinstance(baseline.get('racks'),dict): raise CabinetError('机柜颜色基线格式无效')
                        baseline_hashes.add(baseline.get('template_hash'))
                        baseline_frozen.add(bool(baseline.get('frozen')))
                        if baseline.get('source_hash'): baseline_sources.add(str(baseline['source_hash']))
                        for rack,value in baseline['racks'].items():
                            if not re.fullmatch(r'[A-Z]\d{2}',rack) or not isinstance(value,dict) or value.get('state') not in COLORS or not isinstance(value.get('last_operation'),str) or not isinstance(value.get('event_hashes'),list) or any(not isinstance(h,str) or not re.fullmatch(r'[0-9a-f]{32}',h) for h in value['event_hashes']): raise CabinetError('机柜颜色基线内容无效')
                        configs[s].setdefault('power_baseline',{}).update({room+'/'+rack:value for rack,value in baseline['racks'].items()})
                    configs[s]["rooms"].append({**geometry,"id":room,"name":text_value(f.get("名称")),"total":int(float(f.get("数量") or 0)),"record_id":item["record_id"]})
                elif kind=="机柜":
                    configs[s]["inventory"].append({**geometry,"room":room,"rack":text_value(f.get("名称")).upper(),"rack_type":text_value(f.get("机柜类型")),"record_id":item["record_id"]})
            now=stamp()
            for s,config in configs.items():
                if not config["rooms"]: raise CabinetError("飞书缺少"+s+"楼房间资料")
                if baseline_hashes and baseline_hashes!={config.get('template_data',{}).get('hash')}: raise CabinetError('颜色基线与原模板版本不一致')
                if len(baseline_frozen)>1 or len(baseline_sources)>1: raise CabinetError('机柜颜色基线版本不一致')
                config['power_baseline_frozen']=baseline_frozen=={True}
                config['power_baseline_source_hash']=next(iter(baseline_sources),'')
                keys=[(r["room"],r["rack"]) for r in config["inventory"]]
                if len(set(keys))!=len(keys): raise CabinetError(s+"楼飞书目录存在重复机柜")
                config["rooms"].sort(key=lambda r:r["id"]); config["version"]=digest([config["rooms"],config["inventory"],config.get("template_data"),config.get("map_values")])
            self.local.replace(scope,configs[scope],records,[e["id"] for op in ops for e in op["events"]])
            return {"updated_at":now,"count":len(records)}

    def _layout_data(self,scope):
        with self._scope_locks[scope]:
            if scope not in self._layouts:
                path=INITIAL_TEMPLATES/(scope+".layouts.json.gz")
                if not path.exists(): raise CabinetError("该楼平面图资源缺失，请重新构建布局缓存")
                with gzip.open(path,"rt",encoding="utf-8") as source: data=json.load(source)
                actual=hashlib.sha256((INITIAL_TEMPLATES/(scope+".xlsm")).read_bytes()).hexdigest()
                if data.get("hash")!=actual: raise CabinetError("平面图资源与当前模板版本不一致")
                self._layouts[scope]=data
            return self._layouts[scope]

    def _packaged_config(self,scope,config):
        data=self._layout_data(scope)
        if config.get("template_data",{}).get("hash")==data.get("hash"):
            return config
        current=copy.deepcopy(config)
        if data.get("layout_identity")!=layout_identity(current):
            extension=data.get("extension") or {}
            previous=copy.deepcopy(current)
            if extension.get("position_updates"):
                new_keys={(rack["room"],rack["rack"]) for rack in extension["inventory"]}
                previous["inventory"]=[rack for rack in previous["inventory"] if (rack["room"],rack["rack"]) not in new_keys]
                old_rooms={room["id"]:room for room in extension["previous_rooms"]}
                old_positions={(rack["room"],rack["rack"]):rack["previous_positions"] for rack in extension["position_updates"]}
                for room in previous["rooms"]:
                    if room["id"] in old_rooms: room.update(old_rooms[room["id"]])
                for rack in previous["inventory"]:
                    key=(rack["room"],rack["rack"])
                    if key in old_positions: rack["positions"]=copy.deepcopy(old_positions[key])
            if extension.get("from_identity")==layout_identity(previous):
                rooms={room["id"]:room for room in current["rooms"]}
                for room in extension["rooms"]:
                    if room["id"] in rooms: rooms[room["id"]].update(copy.deepcopy(room))
                    else: current["rooms"].append(copy.deepcopy(room))
                racks={(rack["room"],rack["rack"]):rack for rack in current["inventory"]}
                for rack in extension["inventory"]:
                    key=(rack["room"],rack["rack"])
                    if key in racks: racks[key]["positions"]=copy.deepcopy(rack["positions"])
                    else: current["inventory"].append(copy.deepcopy(rack))
                for rack in extension.get("position_updates",[]):
                    racks[(rack["room"],rack["rack"])]["positions"]=copy.deepcopy(rack["positions"])
                additions=inventory_state_baseline({"scope":scope,"inventory":[rack for rack in extension["inventory"] if rack["room"]+"/"+rack["rack"] not in current.get("power_baseline",{})]},[])[0]
                current.setdefault("power_baseline",{}).update(additions)
        if not data.get("template_data") or data.get("layout_identity")!=layout_identity(current):
            raise CabinetError("飞书布局与当前模板结构不一致，请重新同步模板资料")
        current["template_data"]=copy.deepcopy(data["template_data"])
        current["map_values"]=copy.deepcopy(data.get("map_values",{}))
        room_meta={room["id"]:room for room in data.get("rooms_meta",[])}
        for room in current.get("rooms",[]):
            if room["id"] in room_meta:
                room.update({key:value for key,value in room_meta[room["id"]].items() if key not in ("id","record_id")})
        return current

    def local_layout(self,scope,room_id):
        self._snapshot(scope)
        return copy.deepcopy(self._layout_data(scope)["rooms"].get(room_id))

    def config(self,scope):
        return copy.deepcopy(self._snapshot(scope)["config"])

    def snapshot(self,scope):
        return copy.deepcopy(self._snapshot(scope))

    def overview(self,scope,include_racks=True):
        snap=self._snapshot(scope); config=snap["config"]
        if "overview" in snap:
            result=copy.deepcopy(snap["overview"] if include_racks else {k:v for k,v in snap["overview"].items() if k!="racks"})
            result["export_state"]=self._export_state(scope,snap)
            return result
        business_ops=[op for op in snap["operations"] if op.get("events") or op.get("source_row")]
        derived=derive_records(config,business_ops); issues=derived["issues"]
        rooms=[]
        for room in config["rooms"]:
            rr=[r for r in derived["racks"] if r["room"]==room["id"]]
            counts={s:sum(r["state"]==s for r in rr) for s in COLORS}
            gap=derived["unlocated"][room["id"]]; counts["unknown"]+=gap["unknown"]; counts["off"]+=gap["off"]
            types={t:sum(r["rack_type"]==t for r in rr) for t in RACK_TYPES}
            carrier=scope=="B" and room["id"] in ("216","247")
            if carrier: types["网络机柜"]+=gap["total"]
            rooms.append({**room,"counts":counts,"types":types,"carrier":carrier,"unlocated":gap["total"],"unlocated_counts":gap})
        formats=[]
        for original in config.get("template_data",{}).get("formats",[]):
            f=copy.deepcopy(original); selected=[o for o in business_ops if o["display_sheet"]==f["sheet"]]
            count=max((len(o["groups"]) for o in selected),default=0)
            while len(f["groups"])<count:
                col=max(c["column"] for c in table_columns(f,scope)); f["groups"].append({"action":col+1,"expected":col+2,"actual":col+3})
            formats.append({**f,"columns":table_columns(f,scope),"count":len(selected)})
        operated={(op["room"],op["rack"]) for op in business_ops if not op.get("meta",{}).get("baseline_correction") and op.get("events")}
        inventory_only=len({(rack["room"],rack["rack"]) for rack in config["inventory"]}-operated)
        result={"scope":scope,"configured":True,"activated":True,"history_ready":True,"source":"local","counts":derived["counts"],"rooms":rooms,"racks":derived["racks"],"issues":issues,"version":snap["version"],"updated_at":snap["updated_at"],"error":snap.get("error",""),"daily":derived["daily"],"record_count":len(business_ops),"inventory_only":inventory_only,"sheet_formats":formats,"table_url":f"https://vnet.feishu.cn/base/{APP_TOKEN}?table={TABLE_ID}"}
        snap["overview"]=result
        response=copy.deepcopy(result if include_racks else {k:v for k,v in result.items() if k!="racks"})
        response["export_state"]=self._export_state(scope,snap)
        return response

    def racks(self,scope):
        overview=self.overview(scope)
        return {"items":overview["racks"],"version":overview["version"]}

    def layout(self,scope,room_id):
        config=self.config(scope); room=next((r for r in config["rooms"] if r["id"]==room_id),None)
        if not room: raise CabinetError("房间不存在",404)
        overview=self.overview(scope); racks=[r for r in overview["racks"] if r["room"]==room_id]
        room_overview=next(r for r in overview["rooms"] if r["id"]==room_id); counts=room_overview["counts"]
        summary={"total":room_overview["total"],"formal":counts["formal"],"test":counts["test"],"off":counts["off"],"powered":counts["formal"]+counts["test"],"unknown":counts["unknown"]}
        model=self.local_layout(scope,room_id) if room.get("sheet") else None
        if model:
            values=config.get("map_values",{}).get(room["sheet"],{})
            for cell in model["cells"]:
                value=values.get(cell["ref"],"")
                cell["text"]=format(value,"g") if isinstance(value,(int,float)) else str(value)
            model=project_layout(model,racks,overview['racks'])
        return {"room":room,"layout":model,"racks":racks,"summary":summary,"version":overview["version"]}

    def operations(self,scope,query):
        snap=self._snapshot(scope); ops=list(snap["operations"])
        for key in ("room","rack"):
            if query.get(key): ops=[o for o in ops if o.get(key)==query[key]]
        if query.get("sheet"): ops=[o for o in ops if o["display_sheet"]==query["sheet"]]
        if query.get("q"):
            q=query["q"].lower()
            ops=[o for o in ops if q in " ".join(str(o.get(k,"")) for k in ("system_name","room","rack","action","action_note","source")).lower()]
        if query.get("issues")=="true": ops=[o for o in ops if o["issues"]]
        if query.get("direction")=="empty": ops=[o for o in ops if o["empty"]]
        for key in ("from","to"):
            if query.get(key):
                try: dt.date.fromisoformat(query[key])
                except ValueError: raise CabinetError("筛选日期格式无效")
        if query.get("from") and query.get("to") and query["from"]>query["to"]: raise CabinetError("开始日期不能晚于结束日期")
        if query.get("direction") in ("up","down") or any(query.get(k) for k in ("action","from","to")):
            def match(e):
                return (query.get("direction") not in ("up","down") or e["action"].startswith("下")== (query["direction"]=="down")) and (not query.get("action") or e["action"]==query["action"]) and (not query.get("from") or bool(e["actual"]) and e["actual"][:10]>=query["from"]) and (not query.get("to") or bool(e["actual"]) and e["actual"][:10]<=query["to"])
            ops=[o for o in ops if any(match(e) for e in o["events"])]
        if query.get("sheet"): ops.sort(key=lambda o:(o.get("source_row") or o.get("ordinal",0),o["record_id"]))
        else: ops.sort(key=lambda o:(o["last_operation"],o["record_id"]),reverse=True)
        try: page=max(1,int(query.get("page",1))); size=min(100,max(1,int(query.get("page_size",50))))
        except (ValueError,TypeError): raise CabinetError("分页参数无效")
        page=min(page,max(1,math.ceil(len(ops)/size)))
        items=copy.deepcopy(ops[(page-1)*size:page*size]); inventory={(r["room"],r["rack"]):r for r in snap["config"]["inventory"]}
        for op in items: op["current_rack_type"]=inventory.get((op["room"],op["rack"]),{}).get("rack_type","")
        state=None
        if query.get("room") and query.get("rack"):
            business_ops=[op for op in snap["operations"] if op.get("events") or op.get("source_row")]
            derived=derive_records(snap["config"],business_ops)
            state=next((r for r in derived["racks"] if (r["room"],r["rack"])==(query["room"],query["rack"])),None)
            if state is not None:
                events=[{**o,**e} for o in snap['operations'] if (o['room'],o['rack'])==(query['room'],query['rack']) for e in o['events']]
                corrections=[e for e in events if e.get('meta',{}).get('baseline_correction') and e.get('result')=='成功']
                compatible=[e for e in events if completed_state_event(e) and STATES.get(e.get('action'))==state['state']]
                latest=max(corrections,key=lambda e:e['id'],default={}) or max(compatible,key=lambda e:(e['actual'],e['id']),default={})
                state['latest_success']={k:latest[k] for k in ('id','record_id','action','actual','expected','result') if k in latest}
                if latest.get('meta',{}).get('baseline_correction'): state['latest_success']['baseline_correction']=True
        return {"items":items,"total":len(ops),"page":page,"page_size":size,"version":snap["version"],"rack_state":state}

    def evidence_path(self,scope,record_id,image_id,thumbnail=False):
        import io
        from PIL import Image
        if not re.fullmatch(r"[a-f0-9]{64}",str(image_id or "")):
            raise CabinetError("确认截图标识无效",400)
        operation=next((item for item in self._snapshot(scope)["operations"] if item["record_id"]==record_id),None)
        if operation is None:
            raise CabinetError("机柜记录不存在",404)
        refs=[ref for group in operation["groups"] for ref in group.get("evidence_images", [])
              if isinstance(ref,dict) and ref.get("image_id")==image_id]
        if not refs:
            raise CabinetError("该机柜记录未关联此截图",404)
        extension=str(refs[0].get("extension") or "")
        if extension not in (".jpg",".png",".webp"):
            raise CabinetError("确认截图格式无效",400)
        path=self.root/"evidence"/(image_id+extension)
        if not path.is_file():
            token=str(refs[0].get("file_token") or "")
            content=self.remote_for(scope).download_attachment(token)
            if hashlib.sha256(content).hexdigest()!=image_id:
                raise CabinetError("飞书确认截图与机柜记录校验值不一致",409)
            with Image.open(io.BytesIO(content)) as image:
                if image.format!={".jpg":"JPEG",".png":"PNG",".webp":"WEBP"}[extension]:
                    raise CabinetError("飞书确认截图格式与记录不一致",409)
                image.verify()
            self.batches._atomic_write(path,content)
        if thumbnail:
            from .cabinet_power_evidence import ensure_thumbnail
            path=ensure_thumbnail(path,self.root/"evidence_thumbnails"/(image_id+".png"))
            return path,"image/png"
        return path, {".jpg":"image/jpeg",".png":"image/png",".webp":"image/webp"}[extension]

    def document_path(self,scope,record_id,file_id,allowed,admin=False):
        if not re.fullmatch(r"[a-f0-9]{64}",str(file_id or "")):
            raise CabinetError("PDF证明标识无效",400)
        operation=next((item for item in self._snapshot(scope)["operations"] if item["record_id"]==record_id),None)
        if operation is None: raise CabinetError("机柜记录不存在",404)
        document=next((item for group in operation["groups"] for item in group.get("evidence_files",[])
                       if isinstance(item,dict) and item.get("file_id")==file_id),None)
        if document is None or document.get("extension")!=".pdf":
            raise CabinetError("该机柜记录未关联此PDF",404)
        if not admin and not set(document.get("scopes") or [scope])<=set(allowed):
            raise CabinetError("无权查看包含其他楼栋的PDF证明",403)
        path=self.root/"evidence_documents"/(file_id+".pdf")
        if not path.is_file():
            content=self.remote_for(scope).download_attachment(document.get("file_token",""))
            if hashlib.sha256(content).hexdigest()!=file_id or not content.startswith(b"%PDF"):
                raise CabinetError("飞书PDF与机柜证明校验值不一致",409)
            self.batches._atomic_write(path,content)
        return path,Path(str(document.get("name") or file_id+".pdf")).name

    def validate_op(self,scope,payload,old=None):
        op=copy.deepcopy(old or {})
        op.setdefault("result","")
        for key in ("room","rack","system_name","rack_type","power","result"):
            if key in payload: op[key]=payload[key]
        op.update(scope=scope,room=str(op.get("room","")).strip(),rack=str(op.get("rack","")).strip().upper())
        if not re.fullmatch(r"[A-Z]\d{2}",op["rack"]) and (not old or op['rack']!=old['rack']): raise CabinetError("机架号须为字母加两位数字，如 J02")
        if not re.fullmatch(r"[1-4]\d{2}",op["room"]): raise CabinetError("包间格式无效")
        op["system_name"]=system_name(scope,op["room"])
        if op["room"] not in {r["id"] for r in self.config(scope)["rooms"]}: raise CabinetError("包间不属于当前楼栋")
        if op.get("rack_type") not in ("",*RACK_TYPES) and (not old or op.get('rack_type')!=old.get('rack_type')): raise CabinetError("机柜类型无效")
        config=self._snapshot(scope)["config"]
        if not any((r["room"],r["rack"])==(op["room"],op["rack"]) for r in config["inventory"]):
            carrier_add=payload.get("add_inventory") is True and scope=="B" and op["room"] in ("216","247")
            if carrier_add:
                room=next(r for r in config["rooms"] if r["id"]==op["room"])
                if sum(r["room"]==op["room"] for r in config["inventory"])>=room["total"]: raise CabinetError("该机房已达到原表登记总数，请核对机架编号")
                if op.get("rack_type") not in RACK_TYPES: raise CabinetError("新增机柜必须选择机柜类型")
            elif not old or (scope,op["room"],op["rack"])!=(old["scope"],old["room"],old["rack"]): raise CabinetError("机架不在该包间目录中")
        if "groups" in payload: op["groups"]=copy.deepcopy(payload["groups"])
        elif any(k in payload for k in ("action","actual","expected")):
            op["groups"]=[{k:payload.get(k,"") for k in ("action","actual","expected")}]
        groups=op.get("groups",[])
        if not isinstance(groups,list) or len(groups)>100: raise CabinetError("操作明细格式无效或超过100组")
        if old and groups and "result" in payload and payload["result"]!=old["result"]:
            primary=old.get("meta",{}).get("primary",0)
            groups[primary if isinstance(primary,int) and 0<=primary<len(groups) else 0]["result"]=payload["result"]
        ids=set(); old_groups={g["id"]:g for g in old["groups"]} if old else {}
        for i,g in enumerate(groups):
            if not isinstance(g,dict) or any(not isinstance(g.get(k,""),str) for k in ("action","actual","expected")): raise CabinetError("操作明细必须填写文本")
            g.setdefault("id","event_"+digest([payload.get("operation_id",uuid.uuid4().hex),i])[:24])
            if not isinstance(g["id"],str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}",g["id"]) or g["id"] in ids: raise CabinetError("操作标识无效或重复")
            ids.add(g["id"])
            g.setdefault("result",op.get("result",""))
            prior=old_groups.get(g["id"])
            for key in list(g):
                if key not in ('id','action','actual','expected','result','failure_reason','evidence_images','evidence_files'): g.pop(key)
            if prior:
                g.update({k:copy.deepcopy(v) for k,v in prior.items() if k.startswith('source_')})
                for key in ('failure_reason','evidence_images','evidence_files'):
                    if key not in g and key in prior: g[key]=copy.deepcopy(prior[key])
            reason=str(g.get('failure_reason') or '').strip()
            if len(reason)>1000: raise CabinetError('失败原因不能超过1000字')
            if reason or 'failure_reason' in g or g.get('result')=='失败':
                g['failure_reason']=reason if g.get('result')=='失败' else ''
            images=g.get('evidence_images') or []
            if not isinstance(images,list) or any(not isinstance(item,dict) or (
                item not in (prior or {}).get('evidence_images', []) and (
                    not re.fullmatch(r'[a-f0-9]{64}',str(item.get('image_id') or ''))
                    or not re.fullmatch(r'[A-Za-z0-9_-]{10,200}',str(item.get('file_token') or ''))
                )
            ) for item in images):
                raise CabinetError('确认截图引用无效')
            documents=g.get('evidence_files') or []
            if not isinstance(documents,list) or any(not isinstance(item,dict) or (
                item not in (prior or {}).get('evidence_files', []) and (
                    not re.fullmatch(r'[a-f0-9]{64}',str(item.get('file_id') or ''))
                    or not re.fullmatch(r'[A-Za-z0-9_-]{10,200}',str(item.get('file_token') or ''))
                    or item.get('extension')!='.pdf' or not isinstance(item.get('name'),str)
                    or not item['name'].strip() or len(item['name'])>255
                    or not isinstance(item.get('scopes'),list) or not item['scopes']
                    or any(not isinstance(value,str) or value not in TOTALS for value in item['scopes'])
                )
            ) for item in documents): raise CabinetError('PDF证明引用无效')
            if prior and all(g.get(k,"")==prior.get(k,"") for k in ("action","expected","actual","result","failure_reason","evidence_images","evidence_files")): continue
            if not any(g.get(k) for k in ("action","actual","expected")): continue
            if g.get("result") not in ("成功","失败"): raise CabinetError(f"第{i+1}组须选择成功或失败")
            if g['result']=='失败' and not reason: raise CabinetError(f'第{i+1}组操作失败时须填写失败原因')
            actions=[a.strip() for a in g.get("action","").splitlines() if a.strip()]
            if not actions or any(a not in OPS for a in actions): raise CabinetError(f"第{i+1}组操作类型无效")
            for field in ("actual","expected"):
                values=[v.strip() for v in g.get(field,"").splitlines() if v.strip()]
                if field=="actual" and len(values)!=len(actions) or field=="expected" and len(values) not in (0,len(actions)): raise CabinetError(f"第{i+1}组时间与操作数量不一致")
                if any(not re.fullmatch(r"20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?",v) for v in values): raise CabinetError(f"第{i+1}组时间格式无效")
                try:
                    for value in values: dt.datetime.fromisoformat(value)
                except ValueError: raise CabinetError(f"第{i+1}组日期无效")
            _events,problems=group_events([g])
            if problems: raise CabinetError(f"第{i+1}组操作和实际时间须一一对应")
            if g.get("expected") and not dates(g["expected"]): raise CabinetError("期望时间格式无效")
            if any(e["actual"]>stamp() for e in _events): raise CabinetError("实际完成时间不能晚于当前时间")
        if op.get("power") not in (None,"") and (not old or op.get('power')!=old.get('power')):
            if isinstance(op["power"],bool): raise CabinetError("功率必须为数字")
            try: op["power"]=float(op["power"])
            except (ValueError,TypeError): raise CabinetError("功率必须为数字")
            if not math.isfinite(op["power"]) or op["power"]<0: raise CabinetError("功率必须为非负有限数字")
        if op.get("result") not in ("","成功","失败"): raise CabinetError("操作结果无效")
        if not old: op["meta"]={"category":payload.get("category","mixed")}
        if "batch_meta" in payload:
            batch_meta=payload["batch_meta"]
            if not isinstance(batch_meta,dict): raise CabinetError("批次来源信息格式无效")
            history=op.setdefault("meta",{}).setdefault("batch_rows",[])
            if not isinstance(history,list): raise CabinetError("原批次来源信息格式无效")
            key=(str(batch_meta.get("batch_id") or ""),str(batch_meta.get("row_id") or ""))
            if not all(key): raise CabinetError("批次来源信息不完整")
            if not any((str(item.get("batch_id") or ""),str(item.get("row_id") or ""))==key for item in history if isinstance(item,dict)):
                history.append(copy.deepcopy(batch_meta))
        if payload.get("source"):
            config_formats=self.config(scope).get("template_data",{}).get("formats",[])
            fmt=next((f for f in config_formats if f["sheet"]==payload["source"]),None)
            if fmt is None: raise CabinetError("工作表不属于当前楼栋")
            op["source"]=fmt["sheet"]
            if not old and "下电" in fmt["sheet"] and "上下电" not in fmt["sheet"]:
                op["meta"]["primary"]=next((i for i in reversed(range(len(groups))) if any(a.startswith("下") for a in normalized_actions(groups[i].get("action")))),0)
        if not op.get("source"): op["source"]=source_sheet({"category":payload.get("category","up")},config.get("template_data",{}).get("formats",[]))
        if "primary_index" in payload:
            primary=int(payload["primary_index"])
            if not 0<=primary<len(groups): raise CabinetError("主操作位置无效")
            op.setdefault("meta",{})["primary"]=primary
        if scope in ("D","E") and groups:
            latest=max(range(len(groups)),key=lambda i:max(dates(groups[i].get("actual")),default=""))
            if latest:
                op["groups"]=[groups[latest],*groups[:latest],*groups[latest+1:]]
            op.setdefault("meta",{})["primary"]=0
        if not old or payload.get("target_state"):
            events,_=group_events(op.get("groups",[]))
            latest=max(events,key=lambda e:e["actual"],default={})
            down=latest.get("action","").startswith("下")
            source=op.get("source",""); category="mixed" if "上下电" in source else "down" if "下电" in source else "up"
            if scope not in ("D","E") and latest and ((category=="down")!=down): raise CabinetError("操作类型与工作表不一致，请选择对应的上下电工作表")
            if not old: op["meta"]["category"]=category
        return op

    def save_operation(self,scope,payload,owner,record_id="",can_move_scope=False,defer=False,**_unused):
        oid=str(payload.get("operation_id",""))
        if not re.fullmatch(r"[A-Za-z0-9_-]{16,128}",oid): raise CabinetError("缺少有效操作标识")
        self.ensure_loaded(scope)
        prior=self.local.document(scope,"write:"+oid)
        old_scope=(prior or {}).get("old_scope") or payload.get("original_scope") or scope
        if old_scope not in TOTALS: raise CabinetError("原楼栋无效")
        if old_scope!=scope and not can_move_scope: raise CabinetError("无权调整其他楼栋记录",403)
        self.ensure_loaded(old_scope)
        fingerprint=digest([owner,scope,record_id,payload])
        if prior and (prior.get('owner')!=owner or prior.get('request_hash')!=fingerprint): raise CabinetError('操作标识已用于其他内容或用户',409)
        if defer and prior and (self._write_active(prior) or self._write_finished(prior)): return self._write_receipt(prior)
        if not prior and (self.pending_writes(scope) or self.pending_rollbacks(scope) or old_scope!=scope and (self.pending_writes(old_scope) or self.pending_rollbacks(old_scope))): raise CabinetError('该楼有待完成的上传或回退，请先继续处理',409)
        with self.local.locked([scope,old_scope]):
            prior=self.local.document(scope,"write:"+oid)
            fingerprint=digest([owner,scope,record_id,payload])
            if prior:
                if prior.get("owner")!=owner or prior.get("request_hash")!=fingerprint: raise CabinetError("操作标识已用于其他内容或用户",409)
                if defer: return self._queue_write(prior)
                if prior.get("status")=="completed" and old_scope==scope:
                    return {"deleted":True,"record_id":prior["record_id"]} if prior.get("delete") else from_feishu(prior["record"])
                return self._resume_write(prior)
            if self.pending_writes(scope) or self.pending_rollbacks(scope) or old_scope!=scope and (self.pending_writes(old_scope) or self.pending_rollbacks(old_scope)): raise CabinetError("该楼有待完成的上传或回退，请先继续处理",409)
            old=next((o for o in self._snapshot(old_scope)["operations"] if o["record_id"]==record_id),None) if record_id else None
            if record_id and not old: raise CabinetError("本地记录不存在，请刷新对应楼栋",404)
            if old and old["version"]!=payload.get("expected_version"): raise CabinetError("记录已被修改，请重新打开后保存",409)
            delete_requested=bool(old and isinstance(payload.get("groups"),list) and not any(
                any(text_value(group.get(key)) for key in ("action","actual","expected"))
                for group in payload["groups"] if isinstance(group,dict)
            ))
            if delete_requested:
                if old_scope!=scope: raise CabinetError("删除记录时不能同时调整楼栋",409)
                journal={"operation_id":oid,"scope":scope,"old_scope":scope,"owner":owner,
                         "request_hash":fingerprint,"request":copy.deepcopy(payload),"record_id":record_id,
                         "delete":True,"stages":[{"kind":"main","record_id":record_id,
                         "before":old["raw_fields"],"verified":False}],"status":"intent",
                         "created_at":time.time(),"error":""}
                self.write("write:"+oid,journal)
                return self._queue_write(journal) if defer else self._resume_write(journal)
            op=self.validate_op(scope,payload,old)
            if scope in ("D","E") and any(o["record_id"]!=record_id and (o["room"],o["rack"])==(op["room"],op["rack"]) for o in self._snapshot(scope)["operations"]):
                raise CabinetError("该机柜已有台账，请编辑原记录；D/E楼每柜仅保留一条",409)
            if payload.get("target_state"):
                overview=self.overview(scope)
                rack=next((r for r in overview["racks"] if (r["room"],r["rack"])==(op["room"],op["rack"])),None)
                if not rack or rack["state"]!=payload.get("expected_state") or rack["last_operation"]!=payload.get("expected_latest_time",""): raise CabinetError("当前机柜状态已变化，请重新打开",409)
                events,problems=group_events(op["groups"])
                newest=max(events,key=lambda e:e["actual"],default={})
                transitions={("off","formal"):"上正式电",("off","test"):"上测试电",("formal","test"):"正式电转测试电",("test","formal"):"测试电转正式电",("formal","off"):"下正式电",("test","off"):"下测试电"}
                if problems or not newest or newest["actual"]<=rack["last_operation"]: raise CabinetError("状态变更时间必须晚于最近一次操作")
                if newest["action"]!=transitions.get((rack["state"],payload["target_state"])): raise CabinetError("状态变更操作不一致")
            fields=to_fields(op); fields["来源工作表"]=op["source"]
            baseline_transfer=[]
            if old_scope!=scope:
                baseline_transfer=[e["id"] for e in old["events"] if e["id"] in set(self._snapshot(old_scope)["config"]["baseline_event_ids"])]
                original=op["meta"]; op["meta"]={"origin":original,"primary":original.get("primary",0),"category":"down" if "下电" in op["source"] and "上下电" not in op["source"] else "up"}
                fields=to_fields(op); fields.update({"来源工作表":op["source"],"来源行号":None})
            if not old: fields["数据标识"]="manual_"+oid
            inventory=next((copy.deepcopy(r) for r in self._snapshot(scope)["config"]["inventory"] if (r["room"],r["rack"])==(op["room"],op["rack"])),None)
            stages=[{"kind":"main","record_id":record_id,"fields":fields,"before":old["raw_fields"] if old else None}]
            if inventory is None and payload.get("add_inventory"):
                inventory={"room":op["room"],"rack":op["rack"],"rack_type":op["rack_type"],"positions":[],"template_color":"","record_id":""}
                stages.append({"kind":"directory","record_id":"","fields":{"数据标识":"rack_"+digest([scope,op["room"],op["rack"]]),"类别":"机柜","楼栋":scope+"楼","包间":op["room"],"名称":op["rack"],"机柜类型":op["rack_type"],"数量":1,"布局资料":json.dumps({"positions":[],"template_color":""})},"before":None})
            elif inventory and "rack_type" in payload and (not old or payload["rack_type"]!=old["rack_type"]):
                if inventory["rack_type"]!=payload["rack_type"]:
                    directory_fields={"机柜类型":payload["rack_type"] or None}
                    if not inventory.get("record_id"):
                        directory_fields.update({"数据标识":"rack_"+digest([scope,op["room"],op["rack"]]),"类别":"机柜","楼栋":scope+"楼","包间":op["room"],"名称":op["rack"],"数量":1,
                            "布局资料":json.dumps({"positions":inventory.get("positions",[]),"template_color":inventory.get("template_color","")},ensure_ascii=False)})
                    stages.append({"kind":"directory","record_id":inventory.get("record_id", ""),"fields":directory_fields,"before_subset":{"机柜类型":inventory["rack_type"]},"lookup_data_id":not bool(inventory.get("record_id"))})
                    inventory["rack_type"]=payload["rack_type"]
                else: inventory=None
            else: inventory=None
            journal={"operation_id":oid,"scope":scope,"old_scope":old_scope,"owner":owner,"request_hash":fingerprint,"request":copy.deepcopy(payload),"record_id":record_id,"stages":stages,"inventory":inventory,"baseline_transfer":baseline_transfer,"status":"intent","created_at":time.time(),"error":""}
            self.write("write:"+oid,journal)
            if old_scope!=scope: self.local.document(old_scope,"write:"+oid,journal)
            return self._queue_write(journal) if defer else self._resume_write(journal)

    def save_batch_operations(self,scope,entries,owner):
        """Commit independent cabinet rows in one Feishu write and one readback."""
        self.ensure_loaded(scope)
        with self.local.locked([scope]):
            if self.pending_writes(scope) or self.pending_rollbacks(scope):
                raise CabinetError("该楼存在待完成操作，需逐条核验后继续",409)
            snap=self._snapshot(scope)
            inventory={(item["room"],item["rack"]):item for item in snap["config"]["inventory"]}
            journals=[]; seen_racks=set()
            for row_id,payload,record_id in entries:
                oid=str(payload.get("operation_id") or "")
                if not re.fullmatch(r"[A-Za-z0-9_-]{16,128}",oid) or self.local.document(scope,"write:"+oid):
                    raise CabinetError("批次中已有待核验操作，改为逐条继续",409)
                old=next((op for op in snap["operations"] if op["record_id"]==record_id),None) if record_id else None
                if record_id and (not old or old["version"]!=payload.get("expected_version")):
                    raise CabinetError("机柜记录已变化，需逐条核验",409)
                op=self.validate_op(scope,payload,old)
                rack_key=(op["room"],op["rack"])
                current=inventory.get(rack_key)
                if rack_key in seen_racks or current is None or current["rack_type"]!=op.get("rack_type"):
                    raise CabinetError("同柜多次操作或机柜类型变更需逐条处理",409)
                seen_racks.add(rack_key)
                if scope in ("D","E") and any(
                    item["record_id"]!=record_id and (item["room"],item["rack"])==rack_key
                    for item in snap["operations"]
                ):
                    raise CabinetError("该机柜已有台账，需逐条核验",409)
                fields=to_fields(op); fields["来源工作表"]=op["source"]
                if not old: fields["数据标识"]="manual_"+oid
                journal={"operation_id":oid,"scope":scope,"old_scope":scope,"owner":owner,
                         "request_hash":digest([owner,scope,record_id,payload]),"request":copy.deepcopy(payload),
                         "record_id":record_id,"stages":[{"kind":"main","record_id":record_id,"fields":fields,
                         "before":old["raw_fields"] if old else None}],"inventory":None,"baseline_transfer":[],
                         "status":"intent","created_at":time.time(),"error":""}
                journals.append((row_id,journal))
            if len(journals)<2: raise CabinetError("不足两条可合并的机柜记录",409)
            remote=self.remote_for(scope)
            remote.ensure_fields()
            updates=[journal for _,journal in journals if journal["record_id"]]
            if updates:
                current={item["record_id"]:item for item in remote.batch_get([j["record_id"] for j in updates])}
                for journal in updates:
                    stage=journal["stages"][0]
                    found=current.get(journal["record_id"])
                    if not found or digest(found["fields"])!=digest(stage["before"]):
                        raise CabinetError("云端记录已变化，需逐条核验",409)
            for _,journal in journals: self.write("write:"+journal["operation_id"],journal)
            try:
                creates=[j for _,j in journals if not j["record_id"]]
                for _,journal in journals:
                    journal["stages"][0]["attempted"]=True
                    journal["stages"][0]["batch_create"]=not bool(journal["record_id"])
                    journal.update(status="writing",error_stage="main")
                    self.write("write:"+journal["operation_id"],journal)
                created_records={}
                if creates:
                    created=remote.batch_create([{k:v for k,v in j["stages"][0]["fields"].items() if v is not None} for j in creates])
                    if len(created)!=len(creates): raise CabinetError("批量新增返回数量不一致，需逐条核验",409)
                    created_ids=[record["record_id"] for record in created]
                    returned={record.get("fields",{}).get("数据标识"):record for record in created}
                    for journal in creates:
                        record=returned.get(journal["stages"][0]["fields"]["数据标识"])
                        if not record: continue
                        journal["stages"][0]["record_id"]=record["record_id"]
                        journal["record_id"]=record["record_id"]
                        self.write("write:"+journal["operation_id"],journal)
                    created_records={record["fields"].get("数据标识"):record for record in remote.batch_get(created_ids)}
                    for journal in creates:
                        record=created_records.get(journal["stages"][0]["fields"]["数据标识"])
                        if not record: raise CabinetError("批量新增返回记录无法匹配，需逐条核验",409)
                        journal["stages"][0]["record_id"]=record["record_id"]
                        journal["record_id"]=record["record_id"]
                        self.write("write:"+journal["operation_id"],journal)
                if updates:
                    remote.batch_update([{"record_id":j["record_id"],"fields":j["stages"][0]["fields"]} for j in updates])
                records=(
                    {item["record_id"]:item for item in remote.batch_get([j["record_id"] for _,j in journals])}
                    if updates else {item["record_id"]:item for item in created_records.values()}
                )
                result={}
                for row_id,journal in journals:
                    stage=journal["stages"][0]; record=records.get(journal["record_id"])
                    if not record or not equivalent(stage["fields"],record["fields"]):
                        raise CabinetError("批量写入回读尚未一致，需逐条核验",409)
                    stage["verified"]=True; journal["record"]=record
                    journal.update(status="local_pending",error_stage="local_commit")
                    self.write("write:"+journal["operation_id"],journal)
                    self.local.commit_operation(scope,journal,record=record,complete=True)
                    result[row_id]=record["record_id"]
                return result
            except Exception:
                for _,journal in journals:
                    if (self.local.document(scope,"write:"+journal["operation_id"]) or {}).get("status")!="completed":
                        journal.update(status="pending",error_stage="batch_reconcile")
                        self.write("write:"+journal["operation_id"],journal)
                raise

    def _write_active(self,journal):
        pid=journal.get('worker_pid')
        return journal['status'] in ('queued','checking','writing','readback','local_pending') and process_alive(pid) and (pid!=os.getpid() or journal['operation_id'] in self._writing)

    def _write_finished(self,journal):
        return journal['status']=='completed' and (journal['old_scope']==journal['scope'] or (self.local.document(journal['old_scope'],'write:'+journal['operation_id']) or {}).get('status')=='completed')

    def _write_receipt(self,journal):
        return {'accepted':True,**self.write_status(journal['scope'],journal['operation_id'],journal['owner'])}

    def _queue_write(self,journal):
        if self._write_finished(journal) or self._write_active(journal): return self._write_receipt(journal)
        oid=journal['operation_id']; scope=journal['scope']
        with self._lock:
            if oid in self._writing: return self._write_receipt(journal)
            journal.update(status='queued',worker_pid=os.getpid(),error='',error_stage='queued')
            self.write('write:'+oid,journal); self._writing.add(oid)
            try: self._upload_pools[scope].submit(self._run_write,scope,journal['old_scope'],oid)
            except Exception:
                self._writing.discard(oid); journal.update(status='pending',error='上传队列正在停止，请稍后继续核验'); self.write('write:'+oid,journal); raise
        return self._write_receipt(journal)

    def _run_write(self,scope,old_scope,oid):
        acquired=False
        try:
            with self.local.locked([scope,old_scope]):
                acquired=True
                try:
                    journal=self.local.document(scope,'write:'+oid)
                    if not self._write_finished(journal): self._resume_write(journal)
                except Exception as exc:
                    journal=self.local.document(scope,'write:'+oid)
                    if not journal.get('error') and journal['status']!='completed':
                        journal.update(status='pending',error=str(exc)); self.write('write:'+oid,journal)
                finally:
                    with self._lock: self._writing.discard(oid)
        except Exception as exc:
            if not acquired:
                with self._lock:
                    self._writing.discard(oid)
                    journal=self.local.document(scope,'write:'+oid)
                    journal.update(status='pending',error=str(exc)); self.write('write:'+oid,journal)

    def pending_writes(self,scope):
        return self.local.documents(scope,"write:",pending_only=True)

    def pending_rollbacks(self,scope):
        return self.local.documents(scope,"rollback:",pending_only=True)

    def rollback_batch_operation(self,scope,operation_id,batch_id,record_id):
        self.ensure_loaded(scope)
        with self.local.locked([scope]):
            key="rollback:"+operation_id
            rollback=self.local.document(scope,key)
            if rollback and rollback.get("status")=="completed": return rollback
            forward=self.local.document(scope,"write:"+operation_id)
            if not forward or forward.get("status")!="completed" or forward.get("record_id")!=record_id:
                raise CabinetError("本行没有可回退的已完成上传记录",409)
            if (forward.get("request",{}).get("batch_meta") or {}).get("batch_id")!=batch_id:
                raise CabinetError("上传记录不属于当前批次",409)
            for pending in self.pending_rollbacks(scope):
                if pending["operation_id"]==operation_id: continue
                previous=self.local.document(scope,"write:"+pending["operation_id"])
                if not previous or (
                    previous["request"].get("room"),previous["request"].get("rack")
                )==(forward["request"].get("room"),forward["request"].get("rack")):
                    raise CabinetError("该机柜还有待完成的回退，请先处理",409)
            if self.pending_writes(scope): raise CabinetError("该楼仍有待完成上传，请先处理",409)
            main=next(stage for stage in forward["stages"] if stage["kind"]=="main")
            directory=next((stage for stage in forward["stages"] if stage["kind"]=="directory"),None)
            if directory and directory.get("before") is None and not directory.get("before_subset"):
                raise CabinetError("本批新建机柜目录需人工回退",409)
            after=(forward.get("record") or {}).get("fields") or main["fields"]
            restored={field:(main["before"] or {}).get(field) for field in (set(main["before"] or {})|set(main["fields"]))-{"序号"}}
            for field in ("机柜功率（W）","来源行号"):
                value=restored.get(field)
                if value in (None,""): restored[field]=None
                elif isinstance(value,str):
                    try: restored[field]=float(value) if field=="机柜功率（W）" else int(value)
                    except ValueError: raise CabinetError(f"原始{field}不是数字，请人工核对",409)
            if rollback is None:
                current=next((op for op in self._snapshot(scope)["operations"] if op["record_id"]==record_id),None)
                if current is None or digest(current["raw_fields"])!=digest(after):
                    raise CabinetError("机柜记录在本批后已被修改，不能自动回退",409)
                rollback={"operation_id":operation_id,"batch_id":batch_id,"record_id":record_id,"status":"intent","main_done":False,"directory_done":False,"error":"","created_at":time.time()}
                self.local.document(scope,key,rollback)
            remote=self.remote_for(scope)
            try:
                if directory and directory.get("before_subset"):
                    directory_fields=self.directory(scope).get(directory["record_id"])["fields"]
                    if not (equivalent(directory["before_subset"],directory_fields) or equivalent(directory["fields"],directory_fields)):
                        raise CabinetError("机柜基础资料在本批后已被修改",409)
                if rollback["main_done"]:
                    if main["before"] is None:
                        if any(item["record_id"]==record_id for item in self.list_remote(remote,data_id=main["fields"]["数据标识"])):
                            raise CabinetError("云端机柜记录在回退期间重新出现",409)
                    else:
                        restored_record=remote.get(record_id)
                        if not equivalent(restored,restored_record["fields"]):
                            raise CabinetError("云端机柜记录在回退期间已修改",409)
                if not rollback["main_done"]:
                    if main["before"] is None:
                        matches=self.list_remote(remote,data_id=main["fields"]["数据标识"])
                        target=next((item for item in matches if item["record_id"]==record_id),None)
                        if target is not None:
                            if digest(target["fields"])!=digest(after): raise CabinetError("云端机柜记录已被修改，不能自动回退",409)
                            rollback["status"]="deleting"; self.local.document(scope,key,rollback)
                            remote.delete(record_id)
                        if any(item["record_id"]==record_id for item in self.list_remote(remote,data_id=main["fields"]["数据标识"])):
                            raise CabinetError("云端删除尚未核验成功，请重试",409)
                    else:
                        current=remote.get(record_id)
                        if not equivalent(restored,current["fields"]):
                            if digest(current["fields"])!=digest(after): raise CabinetError("云端机柜记录已被修改，不能自动回退",409)
                            rollback["status"]="restoring"; self.local.document(scope,key,rollback)
                            remote.update(record_id,restored)
                        restored_record=remote.get(record_id)
                        if not equivalent(restored,restored_record["fields"]):
                            raise CabinetError("云端恢复尚未核验成功，请重试",409)
                    rollback.update(main_done=True,status="main_verified"); self.local.document(scope,key,rollback)
                inventory=None
                if directory:
                    if directory.get("before_subset"):
                        original_type=directory["before_subset"]["机柜类型"]
                        current=self.directory(scope).get(directory["record_id"])
                        if not equivalent({"机柜类型":original_type},current["fields"]):
                            if not equivalent(directory["fields"],current["fields"]): raise CabinetError("机柜基础资料在本批后已被修改",409)
                            rollback["status"]="restoring_directory"; self.local.document(scope,key,rollback)
                            self.directory(scope).update(directory["record_id"],{"机柜类型":original_type})
                        if not equivalent({"机柜类型":original_type},self.directory(scope).get(directory["record_id"])["fields"]):
                            raise CabinetError("机柜基础资料回读失败，请重试",409)
                        inventory=next((copy.deepcopy(item) for item in self._snapshot(scope)["config"]["inventory"] if (item["room"],item["rack"])==(forward["request"]["room"],forward["request"]["rack"])),None)
                        if inventory: inventory["rack_type"]=original_type
                rollback.update(directory_done=True,status="local_pending"); self.local.document(scope,key,rollback)
                return self.local.commit_rollback(scope,rollback,record=restored_record if main["before"] is not None else None,inventory=inventory,remove_id=record_id if main["before"] is None else "")
            except Exception as exc:
                rollback.update(error=str(exc),status="pending")
                self.local.document(scope,key,rollback)
                raise

    def _resume_delete(self,journal):
        scope=journal["scope"]; key="write:"+journal["operation_id"]; stage=journal["stages"][0]
        try:
            remote=self.remote_for(scope); before=stage.get("before") or {}; data_id=text_value(before.get("数据标识"))
            if not data_id: raise CabinetError("待删除记录缺少数据标识，不能安全核验",409)
            journal.update(status="checking",error_stage="main",error=""); self.write(key,journal)
            matches=self.list_remote(remote,data_id=data_id)
            if len(matches)>1: raise CabinetError("云端存在重复机柜记录，不能自动删除",409)
            current=next((item for item in matches if item["record_id"]==journal["record_id"]),None)
            if current is not None:
                if digest(current["fields"])!=digest(before): raise CabinetError("云端机柜记录已被修改，不能删除",409)
                journal.update(status="writing",error_stage="main"); stage["attempted"]=True; self.write(key,journal)
                remote.delete(journal["record_id"])
            journal.update(status="readback",error_stage="main"); self.write(key,journal)
            if any(item["record_id"]==journal["record_id"] for item in self.list_remote(remote,data_id=data_id)):
                raise CabinetError("云端删除结果尚未核验成功，请继续核验",409)
            stage["verified"]=True; journal.update(status="local_pending",error_stage="local_commit"); self.write(key,journal)
            final=self.local.commit_operation(scope,journal,remove_id=journal["record_id"],complete=True)
            return {"deleted":True,"record_id":journal["record_id"],"operation_id":journal["operation_id"],"status":final["status"]}
        except Exception as exc:
            journal.update(error=str(exc),error_stage=journal.get("error_stage") or "main",
                           status="conflict" if isinstance(exc,CabinetError) and exc.status_code==409 and "修改" in str(exc) else "pending")
            try: self.write(key,journal)
            except Exception: pass
            raise

    def _resume_write(self,journal):
        if journal.get("delete"): return self._resume_delete(journal)
        uncertain_batch = journal.get("error_stage") == "batch_reconcile"
        if uncertain_batch:
            for stage in journal["stages"]:
                if stage.get("attempted") and stage.get("before") is None and not stage.get("record_id"):
                    stage["batch_create"] = True
        scope=journal["scope"]; key="write:"+journal["operation_id"]; stage_name="prepare"
        try:
            main=self.remote_for(scope)
            journal.update(status='checking',error_stage='schema'); self.write(key,journal)
            main.ensure_fields()
            for stage in journal["stages"]:
                stage_name=stage["kind"]; remote=main if stage_name=="main" else self.directory(scope)
                journal.update(status='checking',error_stage=stage_name); self.write(key,journal)
                if stage.get("verified"):
                    current=remote.get(stage["record_id"])
                    if not equivalent(stage["fields"],current["fields"]): raise CabinetError("已上传记录在恢复前发生版本冲突，请核对",409)
                    if stage_name=="main": journal["record"]=current
                    continue
                current=remote.get(stage["record_id"]) if stage["record_id"] else None
                if current is None and (stage.get("attempted") or stage.get("lookup_data_id")):
                    found=self.list_remote(remote,data_id=stage["fields"]["数据标识"])
                    if len(found)>1: raise CabinetError("云端操作标识重复，请核对",409)
                    current=found[0] if found else None
                    # A missing search result is not proof that a batch create failed.
                    if current is None and (stage.get("batch_create") or uncertain_batch):
                        stage["batch_create"]=True
                        raise CabinetError("批量写入结果尚未确认，请稍后继续核验；不会重复创建机柜记录",409)
                already_verified=bool(current and equivalent(stage["fields"],current["fields"]))
                if already_verified: pass
                else:
                    if current:
                        if stage.get("before") is not None and digest(current["fields"])!=digest(stage["before"]): raise CabinetError("云端记录版本冲突，保留输入，请核对后处理",409)
                        if stage.get("before_subset") and not equivalent(stage["before_subset"],current["fields"]): raise CabinetError("云端机柜基础资料已变化，请核对",409)
                        if stage.get("before") is None and not stage.get("before_subset"): raise CabinetError("已有同标识记录内容冲突",409)
                    elif stage.get("before") is not None: raise CabinetError("云端记录已删除",409)
                    journal.update(status="writing",error="",error_stage=stage_name); stage["attempted"]=True; self.write(key,journal)
                    current=remote.update(current["record_id"],stage["fields"]) if current else remote.create({k:v for k,v in stage["fields"].items() if v is not None},journal["operation_id"]+"_"+stage_name)
                stage["record_id"]=current["record_id"]; journal["status"]="readback"; self.write(key,journal)
                if not already_verified: current=remote.get(stage["record_id"])
                if not equivalent(stage["fields"],current["fields"]): raise CabinetError("飞书回读尚未一致，请继续核验",409)
                stage["verified"]=True
                if stage_name=="main": journal["record"]=current; journal["record_id"]=current["record_id"]
                elif journal.get("inventory"): journal["inventory"]["record_id"]=current["record_id"]
                self.write(key,journal)
            stage_name="local_commit"; journal.update(status="local_pending",error="",error_stage=stage_name)
            self.write(key,journal)
            if journal["old_scope"]!=scope:
                self.local.commit_operation(journal["old_scope"],journal,remove_id=journal["record_id"])
            self.local.commit_operation(scope,journal,record=journal["record"],inventory=journal.get("inventory"),baseline_ids=journal.get("baseline_transfer",()),complete=True)
            if journal["old_scope"]!=scope: self.local.commit_operation(journal["old_scope"],journal,complete=True)
            return from_feishu(journal["record"])
        except Exception as exc:
            journal.update(error=str(exc),error_stage=stage_name,status="conflict" if isinstance(exc,CabinetError) and exc.status_code==409 and "冲突" in str(exc) else "pending")
            try: self.write(key,journal)
            except Exception: pass
            raise

    def write_status(self,scope,oid,owner,admin=False,details=False):
        journal=self.local.document(scope,"write:"+oid)
        if not journal: raise CabinetError("上传操作不存在",404)
        if journal.get("owner")!=owner and not admin: raise CabinetError("无权查看该上传",403)
        if journal['scope']!=scope: journal=self.local.document(journal['scope'],'write:'+oid) or journal
        result={k:v for k,v in journal.items() if k in ("operation_id","scope","old_scope","status","error","error_stage","created_at","completed_at","record_id","delete")}
        if journal['status']=='completed' and not self._write_finished(journal): result.update(status='pending',error='跨楼提交尚未全部完成，请继续核验')
        if journal.get('worker_pid') and journal['status'] in ('queued','checking','writing','readback','local_pending') and not self._write_active(journal): result.update(status='pending',error='上传已中断，请继续核验')
        result.update(retryable=result['status'] in ('pending','conflict'),elapsed_ms=max(0,int(((journal.get('completed_at') or time.time())-journal['created_at'])*1000)),stages=[{'kind':s['kind'],'verified':bool(s.get('verified'))} for s in journal['stages']])
        if details: result['request']=copy.deepcopy(journal['request'])
        return result

    def resume_write(self,scope,oid,owner,admin=False,defer=False):
        self.write_status(scope,oid,owner,admin)
        journal=self.local.document(scope,"write:"+oid)
        if journal["old_scope"]!=journal["scope"] and not admin: raise CabinetError("跨楼操作需要管理员",403)
        scope=journal['scope']; journal=self.local.document(scope,'write:'+oid)
        if defer and (self._write_active(journal) or self._write_finished(journal)): return self._write_receipt(journal)
        if journal["status"]=="completed" and journal["old_scope"]==journal["scope"]:
            return {"deleted":True,"record_id":journal["record_id"]} if journal.get("delete") else from_feishu(journal["record"])
        with self.local.locked([scope,journal["old_scope"]]):
            journal=self.local.document(scope,'write:'+oid)
            return self._queue_write(journal) if defer else self._resume_write(journal)

    def reconcile_write(self,scope,oid,owner,admin=False):
        self.write_status(scope,oid,owner,admin)
        journal=self.local.document(scope,"write:"+oid)
        if journal["old_scope"]!=journal["scope"] and not admin: raise CabinetError("跨楼操作需要管理员",403)
        scope=journal['scope']; journal=self.local.document(scope,'write:'+oid)
        with self.local.locked([scope,journal["old_scope"]]):
            if journal.get("delete"):
                stage=journal["stages"][0]; data_id=text_value((stage.get("before") or {}).get("数据标识"))
                matches=self.list_remote(self.remote_for(scope),data_id=data_id)
                current=next((item for item in matches if item["record_id"]==journal["record_id"]),None)
                if current is None: return self._resume_delete(journal)
                journal.update(status="cancelled",error="已保留云端记录，原删除请求保留在操作日志中")
                self.local.commit_operation(scope,journal,record=current)
                return {"status":"cancelled","operation_id":oid}
            main=journal["stages"][0]; remote=self.remote_for(scope)
            current=remote.get(main["record_id"]) if main["record_id"] else None
            if current is None:
                found=self.list_remote(remote,data_id=main["fields"].get("数据标识",""))
                if len(found)>1: raise CabinetError("云端存在重复记录，请先核实",409)
                current=found[0] if found else None
            if current is None and main.get("attempted") and (main.get("batch_create") or journal.get("error_stage")=="batch_reconcile"):
                raise CabinetError("批量写入结果仍未确认，请稍后继续核验，不能按空结果取消操作",409)
            current_scope=from_feishu(current)["scope"] if current else scope
            if current_scope not in (scope,journal["old_scope"]): raise CabinetError("记录被调整到其他楼栋，请由管理员核对",409)
            inventory=copy.deepcopy(journal.get("inventory"))
            if inventory:
                if not inventory.get("record_id"): raise CabinetError("机柜基础资料尚未创建，请继续完成原上传",409)
                fields=self.directory(scope).get(inventory["record_id"])["fields"]
                inventory["rack_type"]=text_value(fields.get("机柜类型"))
            journal.update(status="cancelled",error="已载入云端版本，原提交保留在操作日志中")
            self.local.commit_operation(scope,journal,record=current if current_scope==scope else None,inventory=inventory,remove_id=current["record_id"] if current and current_scope!=scope else "")
            if journal["old_scope"]!=scope:
                self.local.commit_operation(journal["old_scope"],journal,record=current if current_scope==journal["old_scope"] else None,remove_id=current["record_id"] if current and current_scope!=journal["old_scope"] else "")
            return {"status":"cancelled","operation_id":oid}

    @staticmethod
    def _public_export(record):
        return {k:v for k,v in record.items() if k!="path"}

    def ensure_export_archive_fields(self):
        if self.export_remote is None or self._export_schema_ready: return bool(self.export_remote)
        with self._export_schema_lock:
            if self._export_schema_ready: return True
            fields={str(item.get("field_name") or ""):item for item in self.export_remote.list_all("fields")}
            for name,kind in EXPORT_ARCHIVE_FIELDS.items():
                current=fields.get(name)
                if current and int(current.get("type") or 0)!=kind: raise CabinetError(f"导出归档表字段类型错误：{name}")
                if current: continue
                try: self.export_remote.request("POST","fields",{"field_name":name,"type":kind})
                except Exception:
                    current={str(item.get("field_name") or ""):item for item in self.export_remote.list_all("fields")}.get(name)
                    if not current or int(current.get("type") or 0)!=kind: raise
                fields[name]={"field_name":name,"type":kind}
            verified={str(item.get("field_name") or ""):item for item in self.export_remote.list_all("fields")}
            for name,kind in EXPORT_ARCHIVE_FIELDS.items():
                if name not in verified or int(verified[name].get("type") or 0)!=kind: raise CabinetError(f"导出归档表字段回读未通过：{name}")
            self._export_schema_ready=True
            return True

    def _find_export_archive_record(self,eid):
        records=self.export_remote.list_all(
            filters="CurrentValue.[导出标识]="+json.dumps(eid,ensure_ascii=False)
        )
        for record in records:
            if text_value((record.get("fields") or {}).get("导出标识"))==eid: return record
        return None

    @staticmethod
    def _verify_export_archive(record,eid,file_token):
        fields=record.get("fields") or {}
        tokens={str(item.get("file_token") or "") for item in fields.get("导出文件") or [] if isinstance(item,dict)}
        if text_value(fields.get("导出标识"))!=eid or file_token not in tokens: raise CabinetError("导出归档回读核验失败")

    def upload_export(self,scope,eid,owner=""):
        if scope not in TOTALS: raise CabinetError("楼栋无效")
        with self._export_upload_locks[scope]:
            record=self.local.document(scope,"export:"+eid)
            if not record: raise CabinetError("导出文件不存在",404)
            path=Path(record.get("path") or "")
            if record.get("deleted") or not path.is_file(): raise CabinetError("导出文件已清理或不可用",410)
            record["archive_url"]=EXPORT_ARCHIVE_URL
            if self.export_remote is None:
                record.update(cloud_upload_status="skipped",cloud_upload_error="")
                self.write("export:"+eid,record)
                return self._public_export(record)
            if record.get("cloud_upload_status")=="succeeded": return self._public_export(record)
            record.update(cloud_upload_status="uploading",phase="uploading",cloud_upload_error="",cloud_updated_at=stamp())
            self.write("export:"+eid,record)
            try:
                self.ensure_export_archive_fields()
                file_token=str(record.get("cloud_file_token") or "")
                if not file_token:
                    file_token=self.export_remote.upload_attachment(path,record["filename"])
                    record["cloud_file_token"]=file_token; record["cloud_updated_at"]=stamp()
                    self.write("export:"+eid,record)
                fields={
                    "导出标识":eid,"批次标识":str(record.get("batch_id") or ""),"楼栋":scope+"楼",
                    "文件名称":record["filename"],"数据版本":int(record.get("version") or 0),
                    "导出时间":str(record.get("created_at") or ""),"文件SHA256":str(record.get("sha256") or ""),
                    "导出人":str(record.get("owner") or owner or ""),"导出文件":[{"file_token":file_token}],
                }
                record["phase"]="archiving"; self.write("export:"+eid,record)
                cloud=self.export_remote.get(record["cloud_record_id"]) if record.get("cloud_record_id") else self._find_export_archive_record(eid)
                if cloud: cloud=self.export_remote.update(cloud["record_id"],fields)
                else: cloud=self.export_remote.create(fields,"export:"+eid)
                record["cloud_record_id"]=str(cloud.get("record_id") or "")
                record.update(cloud_updated_at=stamp(),phase="verifying"); self.write("export:"+eid,record)
                if not record["cloud_record_id"]: raise CabinetError("导出归档未返回记录ID")
                self._verify_export_archive(self.export_remote.get(record["cloud_record_id"]),eid,file_token)
                record.update(cloud_upload_status="succeeded",phase="completed",cloud_upload_error="",cloud_updated_at=stamp())
            except Exception as exc:
                record=self.local.document(scope,"export:"+eid) or record
                self._export_schema_ready=False
                record.update(cloud_upload_status="failed",cloud_upload_error=str(exc),cloud_updated_at=stamp(),archive_url=EXPORT_ARCHIVE_URL)
            self.write("export:"+eid,record)
            return self._public_export(record)

    def job(self,scope,kind,owner,payload):
        if scope not in TOTALS or kind not in ("refresh","export"): raise CabinetError("任务类型无效")
        payload=copy.deepcopy(payload)
        request_id=str(payload.get("batch_id") or "") if kind=="export" else kind
        request_key="job-request:"+kind+":"+request_id
        completed=None
        with self._lock:
            request=self.local.document(scope,request_key) or {}
            existing=self.local.document(scope,"job:"+str(request.get("job_id") or "")) if request.get("job_id") else None
            if existing:
                alive=process_alive(existing.get("pid")) and (existing.get("pid")!=os.getpid() or existing.get("job_id") in self._running)
                if existing.get("status") in ("pending","running") and alive:
                    return {k:v for k,v in existing.items() if k!="payload"}
                if kind=="export" and request_id and existing.get("status")=="succeeded": completed=existing
        if completed is not None:
            snapshot=self.snapshot(scope)
            old_payload=completed.get("payload") or {}
            old_snapshot_version=completed.get("snapshot_version") or (old_payload.get("snapshot") or {}).get("version")
            if old_snapshot_version==snapshot["version"] and completed.get("export_format_version")==EXPORT_FORMAT_VERSION:
                return {k:v for k,v in completed.items() if k!="payload"}
            version=snapshot["version"]
            payload.update(snapshot=snapshot)
        else: version=0
        with self._lock:
            request=self.local.document(scope,request_key) or {}
            existing=self.local.document(scope,"job:"+str(request.get("job_id") or "")) if request.get("job_id") else None
            if existing:
                alive=process_alive(existing.get("pid")) and (existing.get("pid")!=os.getpid() or existing.get("job_id") in self._running)
                if existing.get("status") in ("pending","running") and alive:
                    return {k:v for k,v in existing.items() if k!="payload"}
            jid=uuid.uuid4().hex; job={"job_id":jid,"scope":scope,"kind":kind,"owner":owner,"payload":payload,"batch_id":request_id if kind=="export" else "","status":"pending","created_at":stamp(),"pid":os.getpid(),"version":version}
            self.write("job:"+jid,job)
            self.local.document(scope,request_key,{"job_id":jid})
            response={k:v for k,v in job.items() if k!="payload"}
            self._running[jid]=job
            try: self._pools[scope].submit(self._run_job,job)
            except Exception:
                self._running.pop(jid,None)
                job.update(status="failed",error="服务正在停止，请稍后重试"); self.write("job:"+jid,job); raise
            return response

    def _run_job(self,job):
        try:
            job.update(status="running",phase="preparing",started_at=stamp()); self.write("job:"+job["job_id"],job)
            if job["kind"]=="export" and not job["payload"].get("snapshot"):
                snapshot=self.snapshot(job["scope"])
                job["payload"].update(snapshot=snapshot)
                job["version"]=snapshot["version"]
                self.write("job:"+job["job_id"],job)
            result=getattr(self,"do_"+job["kind"])(job["scope"],job["payload"],job)
            job.update(status="succeeded",result=result)
        except Exception as exc: job.update(status="failed",error=str(exc))
        finally:
            try:
                payload=job.get("payload") or {}
                if job.get("kind")=="export":
                    job["snapshot_version"]=(payload.get("snapshot") or {}).get("version") or job.get("version",0)
                    job["export_format_version"]=EXPORT_FORMAT_VERSION
                    job["payload"]={"batch_id":str(payload.get("batch_id") or job.get("batch_id") or "")}
                job["finished_at"]=stamp(); self.write("job:"+job["job_id"],job)
            finally:
                with self._lock: self._running.pop(job["job_id"],None)

    def job_status(self,jid,scope=None):
        job=self.local.document(scope,"job:"+jid) if scope in TOTALS else self.read("job:"+jid)
        if not job: raise CabinetError("任务不存在",404)
        created=job.get("started_at") or job.get("created_at") or ""
        try: stale=time.time()-dt.datetime.fromisoformat(created).timestamp()>30
        except (ValueError,TypeError): stale=True
        if job["status"] in ("pending","running") and (not process_alive(job.get("pid")) or job.get("pid")==os.getpid() and jid not in self._running and stale): job.update(status="failed",error="任务已中断，请重新执行")
        if job.get("kind")=="export" and (job.get("result") or {}).get("export_id"):
            export=self.local.document(job["scope"],"export:"+job["result"]["export_id"])
            if export:
                job.update(result=self._public_export(export),phase=export.get("phase","generated"))
        return {k:v for k,v in job.items() if k!="payload"}

    def do_export(self,scope,payload,job):
        snap=payload.get("snapshot") or self.snapshot(scope); config=snap["config"]
        if job.get("job_id"):
            job["phase"]="generating"; self.write("job:"+job["job_id"],job)
        with self._lock:
            if self._exports is None: self._exports=ProcessPoolExecutor(
                max_workers=5,mp_context=multiprocessing.get_context("spawn"),initializer=lower_export_priority)
        content=self._exports.submit(export_snapshot,config,snap["operations"]).result()
        batch_id=str(payload.get("batch_id") or "").strip()
        if batch_id and not re.fullmatch(r"[A-Za-z0-9_-]{8,128}",batch_id): raise CabinetError("一键导出批次标识无效")
        eid=uuid.uuid4().hex; filename=f"南通{scope}栋机柜平面图及上下电数量汇总表_{dt.datetime.now():%Y%m%d_%H%M%S}.xlsm"
        path=self.atomic_file(Path("exports")/eid/filename,content)
        result={"export_id":eid,"scope":scope,"path":path,"filename":filename,"version":snap["version"],"export_format_version":EXPORT_FORMAT_VERSION,"created_at":stamp(),"sha256":hashlib.sha256(content).hexdigest(),"owner":str(job.get("owner") or ""),"batch_id":batch_id,"cloud_upload_status":"pending","cloud_upload_error":"","archive_url":EXPORT_ARCHIVE_URL}
        self.write("export:"+eid,result)
        if job.get("job_id"):
            job.update(phase="generated",result=self._public_export(result)); self.write("job:"+job["job_id"],job)
        return self.upload_export(scope,eid,result["owner"])

    def cleanup_export(self,scope,eid):
        with self._export_upload_locks[scope]:
            record=self.local.document(scope,"export:"+eid)
            if not record: raise CabinetError("导出文件不存在",404)
            path=Path(record["path"]).resolve(); directory=(self.root/"exports").resolve()
            if not path.is_relative_to(directory): raise CabinetError("导出路径无效")
            if path.exists(): path.unlink()
            record["deleted"]=True; self.write("export:"+eid,record)
            return {"deleted":True,"export_id":eid}

    def pending_status(self,scope,owner,admin=False):
        return {"items":[self.write_status(scope,j["operation_id"],owner,admin) for j in self.pending_writes(scope) if admin or j.get("owner")==owner]}

    def _export_state(self,scope,snapshot=None):
        snap=snapshot or self._snapshot(scope)
        latest=self.local.latest_document(scope,"export:")
        if latest is None:
            return {"has_export":False,"is_stale":False,"current_version":snap["version"]}
        reasons=[]
        if str(latest.get("version"))!=str(snap["version"]): reasons.append("机柜台账已变化")
        if latest.get("export_format_version")!=EXPORT_FORMAT_VERSION: reasons.append("导出格式已更新")
        return {"has_export":True,"export_id":latest.get("export_id","") ,"created_at":latest.get("created_at",""),
                "is_stale":bool(reasons),"stale_reason":"；".join(reasons),"current_version":snap["version"]}

    def export_history(self,scope,page=1,page_size=20):
        state=self._export_state(scope)
        exports,total,page,page_size=self.local.documents_page(scope,"export:",page,page_size)
        items=[]
        for item in exports:
            public=self._public_export(item)
            public["file_available"]=not item.get("deleted") and Path(item.get("path") or "").is_file()
            reasons=[]
            if str(item.get("version"))!=str(state["current_version"]): reasons.append("机柜台账已变化")
            if item.get("export_format_version")!=EXPORT_FORMAT_VERSION: reasons.append("导出格式已更新")
            public.update(is_stale=bool(reasons),stale_reason="；".join(reasons))
            items.append(public)
        return {"items":items,"total":total,"page":page,"page_size":page_size,"current":state}

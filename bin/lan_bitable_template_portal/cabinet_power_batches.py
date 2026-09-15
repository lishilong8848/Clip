"""Local cabinet-power batch inbox and text-PDF recognition."""
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import tempfile
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

from .cabinet_power_excel import CabinetError, OPS, RACK_TYPES, digest


MAX_FILES = 10
MAX_FILE_BYTES = 10 * 1024 * 1024
MAX_TOTAL_BYTES = 30 * 1024 * 1024
MAX_PAGES = 100
MAX_ROWS = 2000
SCOPES = frozenset("ABCDE")
DATE_RE = re.compile(r"20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?")
ROW_RE = re.compile(
    r"^\s*EA118\s+([A-E])([1-4])-(\d{1,2})\.EA118\s+([A-Z]\d{2})\s+(\S+)\s+"
    r"(网络机柜|服务器机柜)\s+(\S+)\s+(20\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"(成功|失败)\s+(20\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s*$"
)
EDITABLE_FIELDS = {
    "scope", "room", "rack", "supplier_rack", "rack_type", "type_detail",
    "action", "expected", "actual", "result", "order_time", "type_resolution",
}
ACTIVE_ROW_STATUSES = {"ready", "failed"}
LOCKED_ROW_STATUSES = {"queued", "writing", "completed"}


def now():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


class CabinetBatchStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS batches(
                    batch_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source_hash TEXT UNIQUE,
                    scopes_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS batch_status_date ON batches(status, updated_at DESC);
            """)

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=3)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _decode(row):
        if row is None:
            return None
        payload = json.loads(row["payload_json"])
        payload.update(
            batch_id=row["batch_id"], owner_id=row["owner_id"], status=row["status"],
            source_hash=row["source_hash"] or "", scopes=json.loads(row["scopes_json"]),
            version=row["version"], created_at=row["created_at"], updated_at=row["updated_at"],
        )
        return payload

    def get(self, batch_id):
        with self._connect() as conn:
            return self._decode(conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone())

    def by_hash(self, source_hash):
        if not source_hash:
            return None
        with self._connect() as conn:
            return self._decode(conn.execute("SELECT * FROM batches WHERE source_hash=?", (source_hash,)).fetchone())

    def create(self, batch):
        created = now()
        with self._lock, self._connect() as conn, conn:
            conn.execute(
                "INSERT INTO batches VALUES(?,?,?,?,?,?,?,?,?)",
                (batch["batch_id"], batch["owner_id"], batch["status"], batch.get("source_hash") or None,
                 _json(batch.get("scopes", [])), _json(self._payload(batch)), 1, created, created),
            )
        return self.get(batch["batch_id"])

    @staticmethod
    def _payload(batch):
        columns = {"batch_id", "owner_id", "status", "source_hash", "scopes", "version", "created_at", "updated_at"}
        return {key: value for key, value in batch.items() if key not in columns}

    def save(self, batch, expected_version):
        updated = now()
        with self._lock, self._connect() as conn, conn:
            row = conn.execute("SELECT version FROM batches WHERE batch_id=?", (batch["batch_id"],)).fetchone()
            if row is None:
                raise CabinetError("批次不存在", 404)
            if int(row[0]) != int(expected_version):
                raise CabinetError("批次已被其他操作更新，请重新载入", 409)
            version = int(row[0]) + 1
            conn.execute(
                "UPDATE batches SET status=?,scopes_json=?,payload_json=?,version=?,updated_at=? WHERE batch_id=?",
                (batch["status"], _json(batch.get("scopes", [])), _json(self._payload(batch)),
                 version, updated, batch["batch_id"]),
            )
        return self.get(batch["batch_id"])

    def list(self, limit=200):
        with self._connect() as conn:
            return [self._decode(row) for row in conn.execute(
                "SELECT * FROM batches ORDER BY updated_at DESC LIMIT ?", (max(1, min(int(limit), 1000)),)
            )]


class CabinetBatchService:
    def __init__(self, cabinet_service, root):
        self.cabinet = cabinet_service
        self.root = Path(root)
        self.import_root = self.root / "imports"
        self.store = CabinetBatchStore(self.root / "batches.sqlite3")
        self._lock = threading.RLock()
        self._parse_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cabinet-pdf")
        self._confirm_pool = ThreadPoolExecutor(max_workers=5, thread_name_prefix="cabinet-batch")
        self._scope_locks = {scope: threading.Lock() for scope in "ABCDE"}
        self._recover_interrupted()

    def shutdown(self, wait=False):
        self._parse_pool.shutdown(wait=wait, cancel_futures=not wait)
        self._confirm_pool.shutdown(wait=wait, cancel_futures=not wait)

    def _recover_interrupted(self):
        for batch in self.store.list():
            if batch["status"] == "recognizing":
                batch["status"] = "failed"
                batch["error"] = "PDF识别因服务退出而中断，请重新上传文件"
            elif batch["status"] == "running":
                changed = False
                for row in batch.get("rows", []):
                    if row.get("status") in ("queued", "writing"):
                        row.update(status="failed", error="提交因服务退出而中断，可继续重试")
                        changed = True
                if changed:
                    self._refresh_summary(batch)
                else:
                    continue
            else:
                continue
            try:
                self.store.save(batch, batch["version"])
            except CabinetError:
                pass

    @staticmethod
    def _stats(rows):
        stats = {key: 0 for key in ("total", "ready", "duplicate", "conflict", "invalid", "completed", "failed", "excluded")}
        stats["total"] = len(rows)
        for row in rows:
            status = str(row.get("status") or "invalid")
            if status.startswith("excluded_"):
                stats["excluded"] += 1
            elif status in stats:
                stats[status] += 1
        stats["confirmable"] = stats["ready"] + stats["failed"]
        stats["new"] = sum(row.get("status") not in ("duplicate", "completed") and not str(row.get("status", "")).startswith("excluded_") for row in rows)
        return stats

    def _refresh_summary(self, batch):
        rows = batch.get("rows", [])
        batch["stats"] = self._stats(rows)
        batch["scopes"] = sorted({row.get("scope") for row in rows if row.get("scope") in SCOPES})
        if batch.get("status") in ("cancelled", "recognizing") or batch.get("status") == "failed" and not rows:
            return
        if any(row.get("status") in ("queued", "writing") for row in rows):
            batch["status"] = "running"
        elif rows and all(row.get("status") == "completed" or str(row.get("status", "")).startswith("excluded_") for row in rows):
            batch["status"] = "completed"
        elif any(row.get("status") == "completed" for row in rows):
            batch["status"] = "partial"
        elif rows:
            batch["status"] = "pending"

    def _change(self, batch_id, callback, *, expected_version=None, validate=False):
        for _attempt in range(3):
            with self._lock:
                batch = self.store.get(batch_id)
                if batch is None:
                    raise CabinetError("批次不存在", 404)
                if expected_version is not None and int(batch["version"]) != int(expected_version):
                    raise CabinetError("批次已被其他操作更新，请重新载入", 409)
                callback(batch)
                if validate:
                    self._validate_rows(batch)
                self._refresh_summary(batch)
                try:
                    return self.store.save(batch, batch["version"])
                except CabinetError as exc:
                    if exc.status_code != 409 or expected_version is not None:
                        raise
        raise CabinetError("批次正在被其他任务更新，请稍后重试", 409)

    @staticmethod
    def _atomic_write(path, content):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".upload-", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def recognize(self, files, owner):
        if not files or len(files) > MAX_FILES:
            raise CabinetError("每批须上传1至10份PDF")
        total = sum(len(content) for _name, content in files)
        if total > MAX_TOTAL_BYTES:
            raise CabinetError("单批PDF总大小不能超过30MiB", 413)
        metas = []
        for index, (filename, content) in enumerate(files):
            if len(content) > MAX_FILE_BYTES:
                raise CabinetError(f"{filename} 超过10MiB", 413)
            if not content.startswith(b"%PDF"):
                raise CabinetError(f"{filename} 不是有效PDF")
            sha = hashlib.sha256(content).hexdigest()
            metas.append({"file_id": f"f{index + 1}_{sha[:12]}", "name": Path(filename or f"file-{index + 1}.pdf").name,
                          "sha256": sha, "size": len(content), "status": "waiting", "pages": 0,
                          "processed_pages": 0, "error": ""})
        source_hash = hashlib.sha256("\n".join(sorted(item["sha256"] for item in metas)).encode()).hexdigest()
        existing = self.store.by_hash(source_hash)
        if existing:
            if existing["status"] == "failed" and all((self.import_root / existing["batch_id"] / f"{item['file_id']}.pdf").is_file() for item in existing.get("files", [])):
                def retry(batch):
                    batch.update(status="recognizing", error="", rows=[])
                    for item in batch.get("files", []): item.update(status="waiting", pages=0, processed_pages=0, error="")
                    batch["progress"]={"files_done":0,"files_total":len(batch.get("files",[])),"pages_done":0,"pages_total":0}
                existing=self._change(existing["batch_id"],retry)
                self._parse_pool.submit(self._parse_batch,existing["batch_id"])
            existing["duplicate_upload"] = True
            return existing
        batch_id = uuid.uuid4().hex
        batch = self.store.create({
            "batch_id": batch_id, "owner_id": owner, "status": "recognizing", "source_hash": source_hash,
            "scopes": [], "source": "pdf", "files": metas, "rows": [], "error": "",
            "progress": {"files_done": 0, "files_total": len(files), "pages_done": 0, "pages_total": 0},
        })
        folder = self.import_root / batch_id
        try:
            for meta, (_name, content) in zip(metas, files):
                self._atomic_write(folder / f"{meta['file_id']}.pdf", content)
        except Exception as exc:
            self._change(batch_id, lambda item: item.update(status="failed", error=f"原PDF保存失败：{exc}"))
            raise CabinetError("原PDF保存失败") from exc
        self._parse_pool.submit(self._parse_batch, batch_id)
        return batch

    @staticmethod
    def _pdf_reader(path):
        try:
            from pypdf import PdfReader
        except ImportError as exc:
            raise CabinetError("缺少PDF解析组件 pypdf，请运行依赖修复后重试", 503) from exc
        reader = PdfReader(path)
        if reader.is_encrypted:
            raise CabinetError("加密PDF暂不支持，请改用手工批量填写")
        if len(reader.pages) > MAX_PAGES:
            raise CabinetError("单份PDF不能超过100页")
        return reader

    @staticmethod
    def _extract_page(page):
        try:
            return page.extract_text(extraction_mode="layout") or ""
        except TypeError:
            return page.extract_text() or ""

    def _set_file_progress(self, batch_id, file_id, **values):
        def update(batch):
            target = next(item for item in batch["files"] if item["file_id"] == file_id)
            target.update(values)
            progress = batch["progress"]
            progress["files_done"] = sum(item["status"] in ("completed", "failed") for item in batch["files"])
            progress["pages_done"] = sum(int(item.get("processed_pages") or 0) for item in batch["files"])
            progress["pages_total"] = sum(int(item.get("pages") or 0) for item in batch["files"])
        self._change(batch_id, update)

    def _parse_pdf(self, batch_id, meta):
        path = self.import_root / batch_id / f"{meta['file_id']}.pdf"
        reader = self._pdf_reader(path)
        self._set_file_progress(batch_id, meta["file_id"], status="parsing", pages=len(reader.pages))
        texts = []
        for index, page in enumerate(reader.pages):
            text = self._extract_page(page)
            if not text.strip():
                raise CabinetError(f"第{index + 1}页没有可读取文本，扫描件请改用手工批量填写")
            texts.append(text)
            self._set_file_progress(batch_id, meta["file_id"], processed_pages=index + 1)
        complete_text = "\n".join(texts)
        title = re.search(r"机柜\s*([^\n]+?)确认单", complete_text)
        title_action = next((action for action in OPS if title and action in title.group(1)), "")
        header_action = next((action for action in OPS if re.search(r"操作类型\s*[：:]\s*" + re.escape(action), complete_text)), "")
        if not title_action or not header_action or title_action != header_action:
            raise CabinetError("文件标题与操作类型不一致或版式无法识别")
        application_time = (re.search(r"申请时间\s*[：:]\s*(" + DATE_RE.pattern + r")", complete_text) or [None, ""])[1]
        applicant = (re.search(r"申请人\s*[：:]\s*([^\s]+)", complete_text) or [None, ""])[1]
        if applicant == "[null]":
            applicant = ""
        application_ids = list(dict.fromkeys(re.findall(r"Z\d{12,}", complete_text)))
        parsed = []
        source_row = 0
        for page_number, text in enumerate(texts, 1):
            for line in text.splitlines():
                if not line.strip().startswith("EA118 "):
                    continue
                match = ROW_RE.match(line)
                if not match:
                    raise CabinetError(f"第{page_number}页存在无法识别的机柜行，请改用手工批量填写")
                scope, floor, room_number, rack, supplier, rack_type, detail, actual, result, order_time = match.groups()
                source_row += 1
                room = f"{floor}{int(room_number):02d}"
                current = {
                    "scope": scope, "room": room, "rack": rack, "supplier_rack": supplier,
                    "rack_type": rack_type, "type_detail": detail, "action": header_action,
                    "expected": actual, "actual": actual, "result": result, "order_time": order_time,
                }
                parsed.append({
                    **current, "row_id": "row_" + digest([meta["file_id"], meta["sha256"], page_number, source_row, current])[:24],
                    "source_index": 0, "file_id": meta["file_id"], "file_name": meta["name"],
                    "file_sha256": meta["sha256"], "page": page_number, "source_row": source_row,
                    "application_ids": application_ids, "application_time": application_time,
                    "applicant": applicant, "original": copy.deepcopy(current), "edits": [],
                    "status": "ready", "issues": [], "error": "", "type_resolution": "",
                })
        if not parsed:
            raise CabinetError("未识别到机柜明细；扫描件或未知版式请改用手工批量填写")
        return parsed

    def _parse_batch(self, batch_id):
        rows = []
        try:
            batch = self.store.get(batch_id)
            for meta in batch["files"]:
                try:
                    parsed = self._parse_pdf(batch_id, meta)
                    rows.extend(parsed)
                    self._set_file_progress(batch_id, meta["file_id"], status="completed", error="")
                except Exception as exc:
                    self._set_file_progress(batch_id, meta["file_id"], status="failed", error=str(exc))
            if len(rows) > MAX_ROWS:
                raise CabinetError("单批识别结果不能超过2000行")
            for index, row in enumerate(rows, 1):
                row["source_index"] = index
                row["operation_id"] = "batch_" + digest([batch_id, row["row_id"]])[:32]
            def complete(batch):
                batch["rows"] = rows
                errors = [item["name"] + "：" + item["error"] for item in batch["files"] if item["status"] == "failed"]
                batch["error"] = "；".join(errors)
                batch["validation_error"] = False
                batch["status"] = "recognizing" if rows else "failed"
            self._change(batch_id, complete)
            if rows:
                self._change(batch_id, lambda batch: batch.update(status="pending"), validate=True)
        except Exception as exc:
            def failed(batch):
                if rows and not batch.get("rows"):
                    for index, row in enumerate(rows, 1):
                        row["source_index"] = index
                        row.setdefault("operation_id", "batch_" + digest([batch_id, row["row_id"]])[:32])
                    batch["rows"] = rows
                if batch.get("rows"):
                    for row in batch["rows"]:
                        if row.get("status") not in LOCKED_ROW_STATUSES:
                            row.update(status="invalid", issues=[{"code":"validation_error","message":str(exc)}])
                    batch["status"] = "pending"
                else:
                    batch["status"] = "failed"
                batch["error"] = str(exc)
                batch["validation_error"] = bool(batch.get("rows"))
            self._change(batch_id, failed)

    def _normalize_row(self, row):
        for key in ("scope", "room", "rack", "supplier_rack", "rack_type", "type_detail", "action", "expected", "actual", "result", "order_time", "type_resolution"):
            row[key] = str(row.get(key) or "").strip()
        row["scope"] = row["scope"].upper().replace("楼", "")
        row["rack"] = row["rack"].upper()
        for key in ("expected", "actual", "order_time"):
            row[key] = row[key].replace("T", " ")
    @staticmethod
    def _valid_date(value, allow_future=False):
        if not DATE_RE.fullmatch(value):
            return False
        try:
            return allow_future or dt.datetime.fromisoformat(value) <= dt.datetime.now()
        except ValueError:
            return False

    def _validation_context(self, scopes):
        inventories, exact, slots = {}, set(), {}
        for scope in scopes:
            snap = self.cabinet._snapshot(scope)
            inventories[scope] = {(item["room"], item["rack"]): item for item in snap["config"]["inventory"]}
            for operation in snap["operations"]:
                for event in operation["events"]:
                    if not event.get("action") or not event.get("actual"):
                        continue
                    key = (scope, operation["room"], operation["rack"], event["action"], event["actual"])
                    exact.add(key)
                    slots.setdefault(key[:3] + (key[4],), set()).add(key[3])
        return inventories, exact, slots

    def _validate_rows(self, batch):
        rows = batch.get("rows", [])
        scopes = {str(row.get("scope") or "").upper().replace("楼", "") for row in rows}
        valid_scopes = {scope for scope in scopes if scope in SCOPES}
        inventories, existing_exact, existing_slots = self._validation_context(valid_scopes) if valid_scopes else ({}, set(), {})
        normalized = []
        batch_slots = {}
        for row in rows:
            if row.get("status") in LOCKED_ROW_STATUSES or str(row.get("status", "")).startswith("excluded_"):
                continue
            self._normalize_row(row)
            key = (row["scope"], row["room"], row["rack"], row["action"], row["actual"])
            slot = key[:3] + (key[4],)
            normalized.append((row, key, slot))
            if all(key):
                batch_slots.setdefault(slot, set()).add(row["action"])
        seen = set()
        for row, key, slot in normalized:
            issues = []
            scope, room, rack, action, actual = key
            if scope not in SCOPES:
                issues.append({"code": "scope", "message": "楼栋无效"})
            if not re.fullmatch(r"[1-4]\d{2}", room):
                issues.append({"code": "room", "message": "包间格式无效"})
            if not re.fullmatch(r"[A-Z]\d{2}", rack):
                issues.append({"code": "rack", "message": "机柜编号格式无效"})
            if action not in OPS:
                issues.append({"code": "action", "message": "操作类型无效"})
            if not row["expected"] or not self._valid_date(row["expected"], allow_future=True):
                issues.append({"code": "expected", "message": "期望完成时间必填且须为有效时间"})
            if not actual or not self._valid_date(actual):
                issues.append({"code": "actual", "message": "实际完成时间须为有效且不晚于当前的时间"})
            if row["result"] not in ("成功", "失败"):
                issues.append({"code": "result", "message": "操作结果须选择成功或失败"})
            inventory = inventories.get(scope, {}).get((room, rack))
            if inventory is None and scope in SCOPES and re.fullmatch(r"[1-4]\d{2}", room) and re.fullmatch(r"[A-Z]\d{2}", rack):
                issues.append({"code": "inventory", "message": "机柜不在当前楼栋目录中"})
            elif inventory:
                current_type = str(inventory.get("rack_type") or "")
                row["current_rack_type"] = current_type
                if row["rack_type"] not in RACK_TYPES:
                    issues.append({"code": "rack_type", "message": "机柜类型无效"})
                elif current_type and row["rack_type"] != current_type and row.get("type_resolution") not in ("keep_current", "sync_current"):
                    issues.append({"code": "type_mismatch", "message": f"文件类型为{row['rack_type']}，当前目录为{current_type}"})
            conflict_actions = set(existing_slots.get(slot, set())) | set(batch_slots.get(slot, set()))
            conflict = bool(all(slot) and any(candidate != action for candidate in conflict_actions))
            duplicate = bool(all(key) and (key in existing_exact or key in seen))
            if conflict:
                issues.append({"code": "time_conflict", "message": "同一机柜同一实际时间存在不同操作，须人工核对"})
                row["status"] = "conflict"
            elif duplicate:
                row["status"] = "duplicate"
                issues = [{"code": "overlap", "message": "与本批前序行或既有台账完全重叠"}]
            elif issues:
                row["status"] = "invalid"
            elif row.get("status") != "failed":
                row["status"] = "ready"
            row["issues"] = issues
            row["error"] = row.get("error", "") if row["status"] == "failed" else ""
            if all(key):
                seen.add(key)

    def create_manual(self, rows, owner):
        if not isinstance(rows, list) or not rows or len(rows) > MAX_ROWS:
            raise CabinetError("手工批量须包含1至2000行")
        batch_id = uuid.uuid4().hex
        prepared = []
        for index, source in enumerate(rows, 1):
            if not isinstance(source, dict):
                raise CabinetError(f"第{index}行格式无效")
            current = {key: str(source.get(key) or "").strip() for key in EDITABLE_FIELDS}
            if not current["expected"] and current["actual"]:
                current["expected"] = current["actual"]
            row_id = "row_" + digest([batch_id, index, current])[:24]
            prepared.append({
                **current, "row_id": row_id, "source_index": index, "file_id": "", "file_name": "手工批量",
                "file_sha256": "", "page": 0, "source_row": index, "application_ids": [],
                "application_time": "", "applicant": "", "original": copy.deepcopy(current), "edits": [],
                "status": "ready", "issues": [], "error": "", "operation_id": "batch_" + digest([batch_id, row_id])[:32],
            })
        batch = self.store.create({
            "batch_id": batch_id, "owner_id": owner, "status": "pending", "source_hash": "", "scopes": [],
            "source": "manual", "files": [], "rows": prepared, "error": "",
            "progress": {"files_done": 0, "files_total": 0, "pages_done": 0, "pages_total": 0},
        })
        return self._change(batch_id, lambda _batch: None, validate=True)

    def get(self, batch_id):
        batch = self.store.get(batch_id)
        if batch is None:
            raise CabinetError("批次不存在", 404)
        return batch

    def visible(self, batch, owner, allowed, admin=False):
        if not admin and batch["owner_id"] != owner and not set(batch.get("scopes", [])) & set(allowed):
            raise CabinetError("无权查看该批次", 403)
        result = copy.deepcopy(batch)
        if not admin and batch["owner_id"] != owner:
            result["rows"] = [row for row in result.get("rows", []) if row.get("scope") in allowed]
            result["stats"] = self._stats(result["rows"])
        for row in result.get("rows", []):
            row["editable"] = bool(admin or row.get("scope") in allowed) and row.get("status") not in LOCKED_ROW_STATUSES and not row.get("operation_started")
            row["confirmable"] = bool(admin or row.get("scope") in allowed) and row.get("status") in ACTIVE_ROW_STATUSES
        result["can_download_files"] = bool(admin or batch["owner_id"] == owner)
        result["allowed_scopes"] = [scope for scope in batch.get("scopes", []) if admin or scope in allowed]
        result["can_confirm_all"] = bool(admin or set(batch.get("scopes", [])) <= set(allowed))
        return result

    def list(self, owner, allowed, admin=False, scope="", status="", date_from="", date_to="", page=1, page_size=20):
        items = []
        for batch in self.store.list(1000):
            if not admin and batch["owner_id"] != owner and not set(batch.get("scopes", [])) & set(allowed):
                continue
            if scope and scope not in batch.get("scopes", []):
                continue
            if status and batch["status"] != status:
                continue
            if date_from and batch["created_at"][:10] < date_from:
                continue
            if date_to and batch["created_at"][:10] > date_to:
                continue
            items.append({key: batch.get(key) for key in ("batch_id", "owner_id", "status", "source", "scopes", "stats", "created_at", "updated_at", "error")})
        page_size = max(1, min(int(page_size), 100))
        pages = max(1, (len(items) + page_size - 1) // page_size)
        page = max(1, min(int(page), pages))
        return {"items": items[(page - 1) * page_size:page * page_size], "total": len(items), "page": page, "page_size": page_size,
                "pending_count": sum(item["status"] in ("recognizing", "pending", "running", "partial", "failed") for item in items)}

    def update(self, batch_id, payload, owner, allowed, admin=False):
        current = self.get(batch_id)
        if not admin and current["owner_id"] != owner and not set(current.get("scopes", [])) & set(allowed):
            raise CabinetError("无权查看该批次", 403)
        if current["status"] == "cancelled": raise CabinetError("已作废批次不能再修改",409)
        try:
            expected = int(payload.get("version"))
        except (TypeError, ValueError):
            raise CabinetError("缺少有效批次版本")
        patches = payload.get("rows", [])
        common = payload.get("common") or {}
        selected = set(str(value) for value in payload.get("row_ids", []))
        if not isinstance(patches, list) or not isinstance(common, dict):
            raise CabinetError("批次修改格式无效")

        def apply(batch):
            if batch.pop("validation_error",False): batch["error"]=""
            rows = {row["row_id"]: row for row in batch.get("rows", [])}
            requested = [(rows.get(str(patch.get("row_id"))), patch) for patch in patches if isinstance(patch, dict)]
            if common:
                requested.extend((row, common) for row in rows.values() if not selected or row["row_id"] in selected)
            for row, patch in requested:
                if row is None:
                    raise CabinetError("批次行不存在", 404)
                if not admin and row.get("scope") not in allowed:
                    raise CabinetError("无权修改该楼栋记录", 403)
                if row.get("status") in LOCKED_ROW_STATUSES or row.get("operation_started"):
                    raise CabinetError("该行已开始正式提交，不能再修改内容", 409)
                before_scope = row.get("scope")
                for field in EDITABLE_FIELDS:
                    if field not in patch:
                        continue
                    value = str(patch.get(field) or "").strip()
                    if value != str(row.get(field) or ""):
                        row.setdefault("edits", []).append({"field": field, "before": row.get(field, ""), "after": value, "owner": owner, "at": now()})
                        row[field] = value
                if not admin and row.get("scope", "").upper().replace("楼", "") not in allowed:
                    row["scope"] = before_scope
                    raise CabinetError("无权将记录调整到该楼栋", 403)
                if "excluded" in patch:
                    excluded = bool(patch["excluded"])
                    was_excluded = str(row.get("status", "")).startswith("excluded_")
                    row["status"] = "excluded_manual" if excluded else "ready"
                    if was_excluded != excluded:
                        row.setdefault("edits", []).append({"field": "excluded", "before": was_excluded, "after": excluded, "owner": owner, "at": now()})
        return self._change(batch_id, apply, expected_version=expected, validate=True)

    def clear_overlaps(self, batch_id, expected_version, owner, allowed, admin=False):
        current = self.get(batch_id)
        if not admin and current["owner_id"] != owner and not set(current.get("scopes", [])) & set(allowed):
            raise CabinetError("无权查看该批次", 403)
        if current["status"] == "cancelled": raise CabinetError("已作废批次不能再修改",409)
        def clear(batch):
            for row in batch.get("rows", []):
                if row.get("status") != "duplicate":
                    continue
                if not admin and row.get("scope") not in allowed:
                    continue
                row["status"] = "excluded_duplicate"
                row.setdefault("edits", []).append({"field": "excluded", "before": False, "after": True, "owner": owner, "at": now(), "reason": "overlap"})
        return self._change(batch_id, clear, expected_version=int(expected_version), validate=True)

    def _row_payload(self, batch, row):
        scope = row["scope"]
        snap = self.cabinet._snapshot(scope)
        inventory = next((item for item in snap["config"]["inventory"] if (item["room"], item["rack"]) == (row["room"], row["rack"])),None)
        if inventory is None: raise CabinetError("机柜目录已变化，请刷新批次后核对",409)
        rack_type = inventory.get("rack_type", "") if row.get("type_resolution") == "keep_current" else row["rack_type"]
        evidence = {
            "batch_id": batch["batch_id"], "row_id": row["row_id"], "file_name": row.get("file_name", ""),
            "file_sha256": row.get("file_sha256", ""), "application_ids": row.get("application_ids", []),
            "application_time": row.get("application_time", ""), "order_time": row.get("order_time", ""),
            "source_page": row.get("page", 0), "source_row": row.get("source_row", 0),
            "original": row.get("original", {}), "edits": row.get("edits", []),
        }
        group = {"id": "event_" + digest([batch["batch_id"], row["row_id"]])[:24], "action": row["action"],
                 "expected": row["expected"], "actual": row["actual"], "result": row["result"]}
        payload = {"room": row["room"], "rack": row["rack"], "rack_type": rack_type,
                   "result": row["result"], "groups": [group], "operation_id": row["operation_id"],
                   "category": "down" if row["action"].startswith("下") else "up", "batch_meta": evidence}
        exact = None
        for operation in snap["operations"]:
            if (operation["room"], operation["rack"]) != (row["room"], row["rack"]):
                continue
            if any((event["action"], event["actual"]) == (row["action"], row["actual"]) for event in operation["events"]):
                exact = operation
                break
        if exact:
            return None, exact["record_id"]
        if scope in ("D", "E"):
            old = next((operation for operation in snap["operations"] if (operation["room"], operation["rack"]) == (row["room"], row["rack"])), None)
            if old:
                payload["groups"] = copy.deepcopy(old["groups"]) + [group]
                payload["expected_version"] = old["version"]
                payload["source"] = old["source"]
                return (payload, old["record_id"]), ""
        return (payload, ""), ""

    def _confirm_scope(self, batch_id, scope, row_ids, owner):
        with self._scope_locks[scope]:
            for row_id in row_ids:
                try:
                    def writing(batch):
                        row = next(item for item in batch["rows"] if item["row_id"] == row_id)
                        if row.get("status") not in ("queued", "failed"):
                            raise CabinetError("该行当前不可提交", 409)
                        row.update(status="writing", error="", operation_started=True)
                    batch = self._change(batch_id, writing)
                    row = next(item for item in batch["rows"] if item["row_id"] == row_id)
                    request, existing_record_id = self._row_payload(batch, row)
                    if request is None:
                        saved_record_id = existing_record_id
                    else:
                        payload, record_id = request
                        saved = self.cabinet.save_operation(scope, payload, owner, record_id)
                        saved_record_id = saved["record_id"]
                    def completed(current):
                        target = next(item for item in current["rows"] if item["row_id"] == row_id)
                        target.update(status="completed", error="", record_id=saved_record_id, completed_at=now())
                    self._change(batch_id, completed)
                except Exception as exc:
                    def failed(current):
                        target = next((item for item in current.get("rows", []) if item["row_id"] == row_id), None)
                        if target and target.get("status") != "completed":
                            target.update(status="failed", error=str(exc), operation_started=True)
                    try:
                        self._change(batch_id, failed)
                    except Exception:
                        pass
            try:
                self._change(batch_id, lambda _batch: None, validate=True)
            except Exception:
                pass

    def confirm(self, batch_id, payload, owner, allowed, admin=False):
        expected = payload.get("version")
        batch = self.get(batch_id)
        if expected is None: raise CabinetError("缺少有效批次版本")
        if not admin and batch["owner_id"] != owner and not set(batch.get("scopes", [])) & set(allowed):
            raise CabinetError("无权查看该批次", 403)
        if expected is not None and int(expected) != int(batch["version"]):
            raise CabinetError("批次已更新，请重新载入", 409)
        row_ids = set(str(value) for value in payload.get("row_ids", []))
        scope = str(payload.get("scope") or "").upper().replace("楼", "")
        whole = bool(payload.get("all"))
        if whole and not admin and not set(batch.get("scopes", [])) <= set(allowed):
            raise CabinetError("整批确认需要拥有批次内全部楼栋权限", 403)
        if scope and scope not in allowed and not admin:
            raise CabinetError("无权确认该楼栋", 403)
        selected = []
        for row in batch.get("rows", []):
            wanted = whole or bool(scope and row.get("scope") == scope) or bool(row_ids and row["row_id"] in row_ids)
            if not wanted or row.get("status") not in ACTIVE_ROW_STATUSES:
                continue
            if not admin and row.get("scope") not in allowed:
                raise CabinetError("选中记录包含无权操作的楼栋", 403)
            selected.append(row)
        if not selected:
            raise CabinetError("没有可确认的有效记录", 409)

        selected_ids = {row["row_id"] for row in selected}
        def queue(current):
            for row in current["rows"]:
                if row["row_id"] in selected_ids and row.get("status") in ACTIVE_ROW_STATUSES:
                    row.update(status="queued", error="")
        queued = self._change(batch_id, queue, expected_version=int(expected) if expected is not None else None)
        grouped = {}
        for row in queued["rows"]:
            if row["row_id"] in selected_ids:
                grouped.setdefault(row["scope"], []).append(row)
        for building, rows in grouped.items():
            ordered = [row["row_id"] for row in sorted(rows, key=lambda item: (item.get("actual", ""), item.get("source_index", 0)))]
            self._confirm_pool.submit(self._confirm_scope, batch_id, building, ordered, owner)
        return queued

    def cancel(self, batch_id, owner, admin=False):
        batch = self.get(batch_id)
        if not admin and batch["owner_id"] != owner:
            raise CabinetError("仅上传者或管理员可作废批次", 403)
        def cancel_rows(current):
            if any(row.get("status") in ("queued", "writing") for row in current.get("rows", [])):
                raise CabinetError("批次正在提交，暂不能作废", 409)
            for row in current.get("rows", []):
                if row.get("status") != "completed":
                    row["status"] = "excluded_cancelled"
            current["status"] = "cancelled"
        return self._change(batch_id, cancel_rows)

    def file_path(self, batch_id, file_id, owner, admin=False):
        batch = self.get(batch_id)
        if not admin and batch["owner_id"] != owner:
            raise CabinetError("无权下载原确认单", 403)
        meta = next((item for item in batch.get("files", []) if item["file_id"] == file_id), None)
        if meta is None:
            raise CabinetError("原确认单不存在", 404)
        path = self.import_root / batch_id / f"{file_id}.pdf"
        if not path.is_file():
            raise CabinetError("原确认单文件已清理", 410)
        return path, meta["name"]

    def cleanup_file(self,batch_id,file_id,owner,admin=False):
        batch=self.get(batch_id)
        if not admin and batch["owner_id"]!=owner: raise CabinetError("无权清理原确认单",403)
        if batch["status"] not in ("completed","cancelled"): raise CabinetError("批次完成或作废后才能清理原确认单",409)
        meta=next((item for item in batch.get("files",[]) if item["file_id"]==file_id),None)
        if meta is None: raise CabinetError("原确认单不存在",404)
        path=self.import_root/batch_id/f"{file_id}.pdf"
        if path.exists(): path.unlink()
        def cleaned(current):
            target=next(item for item in current["files"] if item["file_id"]==file_id)
            target["cleaned_at"]=now()
        return self._change(batch_id,cleaned)

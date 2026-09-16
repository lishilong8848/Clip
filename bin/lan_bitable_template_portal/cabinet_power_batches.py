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

from .cabinet_power_excel import (
    CabinetError,
    OPS,
    RACK_TYPES,
    STATES,
    completed_state_event,
    digest,
)


MAX_FILES = 10
MAX_FILE_BYTES = 10 * 1024 * 1024
MAX_TOTAL_BYTES = 30 * 1024 * 1024
MAX_PAGES = 100
MAX_ROWS = 2000
SCOPES = frozenset("ABCDE")
DATE_RE = re.compile(r"20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?")
ROW_RE = re.compile(
    r"^\s*EA118\s+([A-E])([1-4])-(\d{1,2})\.EA118\s+([A-Z]\d{2})\s+(\S+)\s+"
    r"(网络机柜|服务器机柜)\s+(\S+)(.*)$"
)
POWER_ACTIONS_BY_STATE = {
    "off": frozenset(("上测试电", "上正式电")),
    "test": frozenset(("测试电转正式电", "下测试电")),
    "formal": frozenset(("正式电转测试电", "下正式电")),
}
POWER_STATE_LABELS = {"off": "未上电/已下电", "test": "测试电", "formal": "正式电"}
NOTICE_ROOM_RE = re.compile(
    r"(?<![A-Z0-9])([A-E])\s*[-_－—]?\s*([1-4]\d{2})\s*(?:包间|运营商机房|机房)?",
    re.IGNORECASE,
)
NOTICE_RACK_RE = re.compile(
    r"(?<![A-Z0-9])([A-Z])\s*[-_－—]?\s*(\d{1,2})(?!\d)",
    re.IGNORECASE,
)
EDITABLE_FIELDS = {
    "scope", "room", "rack", "supplier_rack", "rack_type", "type_detail",
    "action", "expected", "actual", "result", "type_resolution",
}
ACTIVE_ROW_STATUSES = {"ready", "failed"}
LOCKED_ROW_STATUSES = {"queued", "writing", "completed", "rollback_queued", "rolling_back", "rollback_failed", "rollback_blocked", "rolled_back"}


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

    def later_completed(self, batch_id, scope, room, rack):
        with self._connect() as conn:
            rows=conn.execute(
                "SELECT payload_json FROM batches WHERE rowid>(SELECT rowid FROM batches WHERE batch_id=?)",
                (batch_id,),
            )
            for item in rows:
                for row in json.loads(item[0]).get("rows", []):
                    if (row.get("scope"), row.get("room"), row.get("rack")) == (scope,room,rack) and row.get("wrote_record") is not False and row.get("status") in {"completed","rollback_queued","rolling_back","rollback_failed","rollback_blocked"}:
                        return True
        return False


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
                    elif row.get("status") in ("rollback_queued", "rolling_back"):
                        row.update(status="rollback_failed",error="回退因服务退出中断，可继续重试")
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
        stats = {key: 0 for key in ("total", "ready", "duplicate", "conflict", "invalid", "completed", "failed", "excluded", "rolled_back", "rollback_failed", "rollback_blocked")}
        stats["total"] = len(rows)
        for row in rows:
            status = str(row.get("status") or "invalid")
            if status.startswith("excluded_"):
                stats["excluded"] += 1
            elif status in stats:
                stats[status] += 1
        stats["confirmable"] = stats["ready"] + stats["failed"]
        stats["new"] = sum(row.get("status") not in ("duplicate", "completed", "rolled_back") and not str(row.get("status", "")).startswith("excluded_") for row in rows)
        return stats

    def _refresh_summary(self, batch):
        rows = batch.get("rows", [])
        batch["stats"] = self._stats(rows)
        batch["scopes"] = sorted({row.get("scope") for row in rows if row.get("scope") in SCOPES})
        if batch.get("status") == "cancelled" and not any(row.get("status") in ("rollback_queued", "rolling_back") for row in rows):
            return
        if batch.get("status") == "recognizing" or batch.get("status") == "failed" and not rows:
            return
        if any(row.get("status") in ("queued", "writing", "rollback_queued", "rolling_back") for row in rows):
            batch["status"] = "running"
        elif rows and all(row.get("status") == "rolled_back" or str(row.get("status", "")).startswith("excluded_") for row in rows):
            batch["status"] = "rolled_back"
        elif rows and all(row.get("status") == "completed" or str(row.get("status", "")).startswith("excluded_") for row in rows):
            batch["status"] = "completed"
        elif any(row.get("status") in ("completed", "rolled_back") for row in rows):
            batch["status"] = "partial"
        elif any(row.get("status") in ("rollback_failed", "rollback_blocked") for row in rows):
            batch["status"] = "failed"
        elif rows:
            batch["status"] = "pending"

    @staticmethod
    def _notice_datetime(value):
        text = str(value or "").strip().replace("T", " ").replace("：", ":")
        try:
            return dt.datetime.fromisoformat(text).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return ""

    @staticmethod
    def _notice_quantity(value):
        match = re.search(r"\d+", str(value or "").replace(",", ""))
        return int(match.group()) if match else None

    @staticmethod
    def _parse_notice_cabinets(value):
        text = str(value or "").strip().upper()
        anchors = list(NOTICE_ROOM_RE.finditer(text))
        if not anchors:
            return [], {
                "duplicate_count": 0,
                "unparsed_fragments": [text] if text else ["柜号未填写"],
            }
        rows = []
        duplicates = 0
        unparsed = []
        seen = set()
        prefix = text[: anchors[0].start()]
        if re.sub(r"[\s,，、;；:：/\\]+", "", prefix):
            unparsed.append(prefix.strip())
        for index, anchor in enumerate(anchors):
            end = anchors[index + 1].start() if index + 1 < len(anchors) else len(text)
            segment = text[anchor.end() : end]
            matched_spans = []
            for rack_match in NOTICE_RACK_RE.finditer(segment):
                rack = f"{rack_match.group(1).upper()}{int(rack_match.group(2)):02d}"
                key = (anchor.group(1).upper(), anchor.group(2), rack)
                matched_spans.append(rack_match.span())
                if key in seen:
                    duplicates += 1
                    continue
                seen.add(key)
                rows.append({"scope": key[0], "room": key[1], "rack": key[2]})
            remainder = segment
            for start, stop in reversed(matched_spans):
                remainder = remainder[:start] + remainder[stop:]
            remainder = re.sub(
                r"(?:柜号|机柜|机架|柜)|[\s,，、;；。:：/\\]+",
                "",
                remainder,
            )
            if remainder:
                unparsed.append(segment.strip())
        return rows, {
            "duplicate_count": duplicates,
            "unparsed_fragments": [item[:200] for item in unparsed if item],
        }

    @staticmethod
    def _infer_notice_action(snapshot, room, rack, direction, cutoff):
        if not any(
            (item.get("room"), item.get("rack")) == (room, rack)
            for item in (snapshot.get("config") or {}).get("inventory", [])
        ):
            return "", "机柜未匹配当前目录，无法自动识别操作类型", "unknown"
        history = []
        for operation in snapshot.get("operations", []):
            if (operation.get("room"), operation.get("rack")) != (room, rack):
                continue
            for event in operation.get("events", []):
                item = {**operation, **event}
                actual = str(item.get("actual") or "")
                if (
                    completed_state_event(item)
                    and (not cutoff or actual <= cutoff)
                ):
                    history.append(item)
        latest = max(history, key=lambda item: str(item.get("actual") or ""), default=None)
        if latest is None:
            if direction == "up":
                return "上正式电", "当前机柜未上电，默认上正式电，可人工改为上测试电", "off"
            return "", "当前机柜未上电，下电操作需人工核对", "off"
        latest_action = str(latest.get("action") or "")
        state = STATES.get(latest_action, "unknown")
        if direction == "down":
            if state == "formal":
                return "下正式电", f"根据最近成功操作 {latest_action} 推断", state
            if state == "test":
                return "下测试电", f"根据最近成功操作 {latest_action} 推断", state
            return "", "当前已下电或状态不明，请人工核对", state
        if state == "off":
            return "上正式电", "当前机柜未上电，默认上正式电，可人工改为上测试电", state
        if state in {"formal", "test"}:
            return "", "当前已上电，不自动猜测转换操作", state
        return "", "历史无法确定上电类型，请人工选择", state

    def _refresh_notice_warnings(self, batch):
        if batch.get("source") != "notice":
            return
        rows = [
            row
            for row in batch.get("rows", [])
            if not str(row.get("status") or "").startswith("excluded_")
        ]
        source = batch.get("source_notice") or {}
        diagnostics = batch.get("parse_diagnostics") or {}
        warnings = []
        declared = source.get("declared_quantity")
        if declared is None:
            warnings.append({"code": "quantity_missing", "message": "通告数量未填写或无法识别"})
        elif int(declared) != len(rows):
            warnings.append(
                {
                    "code": "quantity_mismatch",
                    "message": f"通告声明 {declared} 柜，当前待办为 {len(rows)} 柜",
                }
            )
        duplicate_count = int(diagnostics.get("duplicate_count") or 0)
        if duplicate_count:
            warnings.append(
                {
                    "code": "duplicate_tokens",
                    "message": f"柜号中有 {duplicate_count} 个重复机柜，待办已按唯一机柜保留",
                }
            )
        fragments = list(diagnostics.get("unparsed_fragments") or [])
        if fragments:
            warnings.append(
                {
                    "code": "unparsed_fragments",
                    "message": "柜号中存在未识别内容：" + "；".join(fragments[:3]),
                }
            )
        invalid_codes = {"scope", "room", "rack", "inventory", "notice_scope"}
        unmatched = sum(
            bool(invalid_codes & {str(issue.get("code") or "") for issue in row.get("issues", [])})
            for row in rows
        )
        batch["notice_counts"] = {
            "declared": declared,
            "unique": len(rows),
            "directory_matched": max(0, len(rows) - unmatched),
        }
        if unmatched:
            warnings.append(
                {
                    "code": "directory_mismatch",
                    "message": f"{unmatched} 个机柜未通过当前楼栋目录校验",
                }
            )
        fingerprint = digest(
            [
                source.get("target_record_id"),
                declared,
                [(row.get("scope"), row.get("room"), row.get("rack")) for row in rows],
                warnings,
            ]
        )
        acknowledgement = batch.get("warning_acknowledgement") or {}
        if not warnings or acknowledgement.get("fingerprint") != fingerprint:
            batch.pop("warning_acknowledgement", None)
        requested_by = str(batch.pop("_warning_acknowledger", "") or "")
        if requested_by and warnings:
            batch["warning_acknowledgement"] = {
                "fingerprint": fingerprint,
                "owner": requested_by,
                "at": now(),
            }
        batch["blocking_warnings"] = warnings
        batch["warning_fingerprint"] = fingerprint if warnings else ""
        batch["warnings_acknowledged"] = bool(
            warnings
            and (batch.get("warning_acknowledgement") or {}).get("fingerprint")
            == fingerprint
        )

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
            return page.extract_text() or ""
        except TypeError:
            return page.extract_text(extraction_mode="layout") or ""

    @staticmethod
    def _current_state(snapshot, room, rack):
        latest = max(
            (event for operation in snapshot["operations"] if (operation["room"], operation["rack"]) == (room, rack)
             for event in operation["events"] if completed_state_event(event)),
            key=lambda event: str(event.get("actual") or ""), default=None,
        )
        return STATES.get(latest["action"], "unknown") if latest else "off"

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
                scope, floor, room_number, rack, supplier, rack_type, detail, tail = match.groups()
                dates = DATE_RE.findall(tail)
                result_match = re.search(r"成功|失败", tail)
                result = result_match.group() if result_match else ""
                remainder = DATE_RE.sub("", tail)
                remainder = re.sub(r"成功|失败", "", remainder).strip()
                if remainder or len(dates) > 2:
                    raise CabinetError(f"第{page_number}页存在无法识别的机柜行，请改用手工批量填写")
                actual = dates[0] if len(dates) == 2 or len(dates) == 1 and result else ""
                order_time = dates[-1] if len(dates) == 2 or len(dates) == 1 and not result else ""
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
        if row["supplier_rack"].lower() in {"-", "/", "null", "[null]", "none"}:
            row["supplier_rack"] = ""
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
        inventories, exact, slots, states = {}, set(), {}, {}
        for scope in scopes:
            snap = self.cabinet._snapshot(scope)
            inventories[scope] = {(item["room"], item["rack"]): item for item in snap["config"]["inventory"]}
            latest = {}
            for operation in snap["operations"]:
                for event in operation["events"]:
                    if not event.get("action") or not event.get("actual"):
                        continue
                    key = (scope, operation["room"], operation["rack"], event["action"], event["actual"])
                    exact.add(key)
                    slots.setdefault(key[:3] + (key[4],), set()).add(key[3])
                    cabinet_key = (operation["room"], operation["rack"])
                    if completed_state_event(event) and (cabinet_key not in latest or event["actual"] > latest[cabinet_key]["actual"]):
                        latest[cabinet_key] = event
            states[scope] = {cabinet_key: STATES.get(event["action"], "unknown") for cabinet_key,event in latest.items()}
        return inventories, exact, slots, states

    def _validate_rows(self, batch):
        rows = batch.get("rows", [])
        notice_scope = str((batch.get("source_notice") or {}).get("scope") or "").upper()
        scopes = {str(row.get("scope") or "").upper().replace("楼", "") for row in rows}
        valid_scopes = {scope for scope in scopes if scope in SCOPES}
        inventories, existing_exact, existing_slots, current_states = self._validation_context(valid_scopes) if valid_scopes else ({}, set(), {}, {})
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
            if notice_scope in SCOPES and scope != notice_scope:
                issues.append({"code": "notice_scope", "message": f"柜号楼栋与通告{notice_scope}楼不一致"})
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
            if batch.get("source") == "pdf" and inventory is not None:
                state = current_states.get(scope, {}).get((room, rack), "off")
                row["current_power_state"] = state
                if action and action not in POWER_ACTIONS_BY_STATE.get(state, ()):
                    issues.append({"code": "state_action", "message": f"当前为{POWER_STATE_LABELS.get(state, '状态待核实')}，只能选择{'或'.join(sorted(POWER_ACTIONS_BY_STATE.get(state, ()))) or '核实状态后操作'}"})
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
        self._refresh_notice_warnings(batch)

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

    def create_from_notice(self, source):
        if not isinstance(source, dict):
            raise CabinetError("上下电通告待办来源无效")
        notice_type = str(source.get("notice_type") or "").strip()
        if notice_type not in ("上电通告", "下电通告"):
            raise CabinetError("仅支持上电通告或下电通告")
        target_record_id = str(source.get("target_record_id") or "").strip()
        if not target_record_id:
            raise CabinetError("上下电通告缺少目标记录ID")
        source_hash = hashlib.sha256(
            f"notice:v1:{notice_type}:{target_record_id}".encode("utf-8")
        ).hexdigest()
        existing = self.store.by_hash(source_hash)
        if existing:
            existing["duplicate_source"] = True
            return existing
        parsed, diagnostics = self._parse_notice_cabinets(source.get("cabinet"))
        if not parsed:
            raise CabinetError("通告柜号未识别到包间和机柜")
        direction = "up" if notice_type == "上电通告" else "down"
        expected = self._notice_datetime(source.get("end_time"))
        cutoff = self._notice_datetime(source.get("sent_at")) or self._notice_datetime(source.get("start_time"))
        snapshots = {scope: self.cabinet._snapshot(scope) for scope in {item["scope"] for item in parsed}}
        inventories = {
            scope: {
                (item["room"], item["rack"]): item
                for item in snapshot["config"]["inventory"]
            }
            for scope, snapshot in snapshots.items()
        }
        batch_id = uuid.uuid4().hex
        rows = []
        for index, item in enumerate(parsed, 1):
            inventory = inventories[item["scope"]].get((item["room"], item["rack"]))
            action, inference, current_power_state = self._infer_notice_action(
                snapshots[item["scope"]],
                item["room"],
                item["rack"],
                direction,
                cutoff,
            )
            current = {
                **item,
                "supplier_rack": "",
                "rack_type": str((inventory or {}).get("rack_type") or ""),
                "type_detail": "",
                "action": action,
                "expected": expected,
                "actual": "",
                "result": "成功",
                "order_time": "",
                "type_resolution": "",
                "current_power_state": current_power_state,
            }
            row_id = "row_" + digest([source_hash, item])[:24]
            rows.append(
                {
                    **current,
                    "row_id": row_id,
                    "source_index": index,
                    "file_id": "",
                    "file_name": "上下电通告",
                    "file_sha256": "",
                    "page": 0,
                    "source_row": index,
                    "application_ids": [],
                    "application_time": "",
                    "applicant": str(source.get("sender_name") or ""),
                    "inference": inference,
                    "original": copy.deepcopy(current),
                    "edits": [],
                    "status": "ready",
                    "issues": [],
                    "error": "",
                    "operation_id": "batch_" + digest([source_hash, row_id])[:32],
                }
            )
        scope = str(source.get("scope") or "").upper().replace("楼", "")
        source_notice = {
            "job_id": str(source.get("job_id") or ""),
            "target_record_id": target_record_id,
            "notice_type": notice_type,
            "title": str(source.get("title") or ""),
            "scope": scope,
            "start_time": self._notice_datetime(source.get("start_time")),
            "end_time": expected,
            "cabinet": str(source.get("cabinet") or ""),
            "quantity": str(source.get("quantity") or ""),
            "declared_quantity": self._notice_quantity(source.get("quantity")),
            "sender_open_id": str(source.get("sender_open_id") or ""),
            "sender_name": str(source.get("sender_name") or ""),
            "sent_at": self._notice_datetime(source.get("sent_at")) or now(),
        }
        try:
            batch = self.store.create(
                {
                    "batch_id": batch_id,
                    "owner_id": str(source.get("owner_id") or source.get("sender_open_id") or "system"),
                    "status": "pending",
                    "source_hash": source_hash,
                    "scopes": [],
                    "source": "notice",
                    "source_notice": source_notice,
                    "parse_diagnostics": diagnostics,
                    "files": [],
                    "rows": rows,
                    "error": "",
                    "progress": {
                        "files_done": 0,
                        "files_total": 0,
                        "pages_done": 0,
                        "pages_total": 0,
                    },
                }
            )
        except sqlite3.IntegrityError:
            existing = self.store.by_hash(source_hash)
            if existing:
                existing["duplicate_source"] = True
                return existing
            raise
        return self._change(batch["batch_id"], lambda _batch: None, validate=True)

    def get(self, batch_id):
        batch = self.store.get(batch_id)
        if batch is None:
            raise CabinetError("批次不存在", 404)
        if batch.get("status")=="partial" and not any(
            row.get("status") in ("completed","rolled_back") for row in batch.get("rows",[])
        ):
            self._refresh_summary(batch)
        if batch.get("source") == "notice":
            source = batch.get("source_notice") or {}
            direction = "up" if source.get("notice_type") == "上电通告" else "down"
            cutoff = self._notice_datetime(source.get("sent_at")) or self._notice_datetime(source.get("start_time"))
            snapshots = {}
            updates = {}
            for row in batch.get("rows", []):
                if row.get("status") in LOCKED_ROW_STATUSES:
                    continue
                patch = {}
                if not row.get("result"):
                    patch["result"] = "成功"
                if not row.get("current_power_state") and row.get("scope") in SCOPES:
                    scope = str(row.get("scope") or "")
                    snapshot = snapshots.get(scope)
                    if snapshot is None:
                        snapshot = self.cabinet._snapshot(scope)
                        snapshots[scope] = snapshot
                    action, inference, state = self._infer_notice_action(
                        snapshot, row.get("room"), row.get("rack"), direction, cutoff
                    )
                    patch["current_power_state"] = state
                    action_was_edited = any(
                        edit.get("field") == "action" for edit in row.get("edits", [])
                    )
                    if not row.get("action") and not action_was_edited:
                        patch.update(action=action, inference=inference)
                if patch:
                    updates[row["row_id"]] = patch
            if updates:
                def apply_defaults(current):
                    for row in current.get("rows", []):
                        row.update(updates.get(row.get("row_id"), {}))
                batch = self._change(
                    batch_id,
                    apply_defaults,
                    expected_version=batch["version"],
                    validate=True,
                )
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
            row["rollbackable"] = bool(admin or row.get("scope") in allowed) and row.get("wrote_record") is not False and row.get("status") in {"completed", "rollback_failed", "rollback_blocked"}
        result["can_download_files"] = bool(admin or batch["owner_id"] == owner)
        result["allowed_scopes"] = [scope for scope in batch.get("scopes", []) if admin or scope in allowed]
        result["can_confirm_all"] = bool(admin or set(batch.get("scopes", [])) <= set(allowed))
        return result

    def list(self, owner, allowed, admin=False, scope="", status="", date_from="", date_to="", page=1, page_size=20):
        items = []
        for batch in self.store.list(1000):
            if batch.get("status")=="partial" and not any(
                row.get("status") in ("completed","rolled_back") for row in batch.get("rows",[])
            ):
                self._refresh_summary(batch)
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
        acknowledge_warnings = payload.get("acknowledge_warnings") is True
        if not isinstance(patches, list) or not isinstance(common, dict):
            raise CabinetError("批次修改格式无效")
        if acknowledge_warnings and not admin and not set(current.get("scopes", [])) <= set(allowed):
            raise CabinetError("核对整批异常需要拥有批次内全部楼栋权限", 403)
        notice_snapshots = {}

        def apply(batch):
            if batch.pop("validation_error",False): batch["error"]=""
            if acknowledge_warnings:
                batch["_warning_acknowledger"] = owner
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
                action_was_edited = any(
                    edit.get("field") == "action" for edit in row.get("edits", [])
                )
                changed_fields = set()
                for field in EDITABLE_FIELDS:
                    if field not in patch:
                        continue
                    value = str(patch.get(field) or "").strip()
                    if value != str(row.get(field) or ""):
                        row.setdefault("edits", []).append({"field": field, "before": row.get(field, ""), "after": value, "owner": owner, "at": now()})
                        row[field] = value
                        changed_fields.add(field)
                if not admin and row.get("scope", "").upper().replace("楼", "") not in allowed:
                    row["scope"] = before_scope
                    raise CabinetError("无权将记录调整到该楼栋", 403)
                if batch.get("source") == "notice" and changed_fields & {"scope", "room", "rack"} and row.get("scope") in SCOPES:
                    scope = row["scope"]
                    snapshot = notice_snapshots.get(scope)
                    if snapshot is None:
                        snapshot = self.cabinet._snapshot(scope)
                        notice_snapshots[scope] = snapshot
                    notice = batch.get("source_notice") or {}
                    direction = "up" if notice.get("notice_type") == "上电通告" else "down"
                    cutoff = self._notice_datetime(notice.get("sent_at")) or self._notice_datetime(notice.get("start_time"))
                    action, inference, state = self._infer_notice_action(
                        snapshot, row.get("room"), row.get("rack"), direction, cutoff
                    )
                    row.update(current_power_state=state, inference=inference)
                    if "action" not in changed_fields and not action_was_edited:
                        row["action"] = action
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
            "application_time": row.get("application_time", ""),
            "source_page": row.get("page", 0), "source_row": row.get("source_row", 0),
            "source": batch.get("source", ""), "source_notice": batch.get("source_notice", {}),
            "inference": row.get("inference", ""), "original": row.get("original", {}),
            "edits": row.get("edits", []),
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
        if batch.get("source") == "pdf":
            state = self._current_state(snap, row["room"], row["rack"])
            if row["action"] not in POWER_ACTIONS_BY_STATE.get(state, ()):
                raise CabinetError(f"该机柜当前为{POWER_STATE_LABELS.get(state, '状态待核实')}，与确认单操作不匹配，请重新核对",409)
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
            rows={row["row_id"]:row for row in self.get(batch_id)["rows"] if row["row_id"] in row_ids}
            waves=[]; wave=[]; racks=set()
            for row_id in row_ids:
                row=rows[row_id]; rack=(row["room"],row["rack"])
                if rack in racks or len(wave)==100:
                    waves.append(wave); wave=[]; racks=set()
                wave.append(row_id); racks.add(rack)
            if wave: waves.append(wave)
            for wave in waves:
                pending=wave
                if len(wave)>1:
                    try:
                        batch=self.get(batch_id)
                        prepared=[]
                        for row_id in wave:
                            row=next(item for item in batch["rows"] if item["row_id"]==row_id)
                            request,existing_id=self._row_payload(batch,row)
                            if request is None: raise CabinetError("本组包含既有记录，逐条核验")
                            payload,record_id=request
                            prepared.append((row_id,payload,record_id))
                        def writing(current):
                            for row in current["rows"]:
                                if row["row_id"] in wave:
                                    row.update(status="writing",error="",operation_started=True)
                        self._change(batch_id,writing)
                        saved=self.cabinet.save_batch_operations(scope,prepared,owner)
                        for row_id,record_id in saved.items():
                            def completed(current):
                                row=next(item for item in current["rows"] if item["row_id"]==row_id)
                                row.update(status="completed",error="",record_id=record_id,completed_at=now(),wrote_record=True)
                            self._change(batch_id,completed)
                        pending=[]
                    except Exception:
                        def retry(current):
                            for row in current["rows"]:
                                if row["row_id"] in wave and row.get("status")=="writing":
                                    row["status"]="queued"
                        self._change(batch_id,retry)
                for row_id in pending:
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
                            target.update(status="completed", error="", record_id=saved_record_id, completed_at=now(),wrote_record=request is not None)
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

    def confirm(self, batch_id, payload, owner, allowed, admin=False):
        expected = payload.get("version")
        batch = self.get(batch_id)
        if expected is None: raise CabinetError("缺少有效批次版本")
        if not admin and batch["owner_id"] != owner and not set(batch.get("scopes", [])) & set(allowed):
            raise CabinetError("无权查看该批次", 403)
        if expected is not None and int(expected) != int(batch["version"]):
            raise CabinetError("批次已更新，请重新载入", 409)
        if batch.get("blocking_warnings") and not batch.get("warnings_acknowledged"):
            raise CabinetError("请先核对并确认通告数量或目录异常", 409)
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

    def _rollback_scope(self,batch_id,scope,row_ids):
        with self._scope_locks[scope]:
            for row_id in row_ids:
                try:
                    def start(batch):
                        row=next(item for item in batch["rows"] if item["row_id"]==row_id)
                        if row.get("status")!="rollback_queued": raise CabinetError("该行当前不可回退",409)
                        row.update(status="rolling_back",error="")
                    batch=self._change(batch_id,start)
                    row=next(item for item in batch["rows"] if item["row_id"]==row_id)
                    newer_in_batch=any(
                        other["row_id"]!=row_id and other.get("wrote_record") is not False and other.get("status") in {"completed","rollback_queued","rolling_back","rollback_failed","rollback_blocked"}
                        and (other.get("scope"),other.get("room"),other.get("rack"))==(scope,row["room"],row["rack"])
                        and (other.get("actual", ""),other.get("source_index",0))>(row.get("actual", ""),row.get("source_index",0))
                        for other in batch["rows"]
                    )
                    if newer_in_batch or self.store.later_completed(batch_id,scope,row["room"],row["rack"]):
                        raise CabinetError("该机柜已有后续批次或本批后续操作，不能回退",409)
                    self.cabinet.rollback_batch_operation(scope,row["operation_id"],batch_id,row["record_id"])
                    def finished(current):
                        target=next(item for item in current["rows"] if item["row_id"]==row_id)
                        target.update(status="rolled_back",error="",rolled_back_at=now())
                    self._change(batch_id,finished)
                except Exception as exc:
                    def failed(current):
                        target=next((item for item in current["rows"] if item["row_id"]==row_id),None)
                        if target and target.get("status")!="rolled_back":
                            target.update(status="rollback_blocked" if isinstance(exc,CabinetError) and exc.status_code==409 else "rollback_failed",error=str(exc))
                    try: self._change(batch_id,failed)
                    except Exception: pass

    def rollback(self,batch_id,payload,owner,allowed,admin=False):
        batch=self.get(batch_id)
        if not admin and batch["owner_id"]!=owner and not set(batch.get("scopes",[])) & set(allowed): raise CabinetError("无权查看该批次",403)
        if int(payload.get("version",-1))!=batch["version"]: raise CabinetError("批次已更新，请重新载入",409)
        if any(row.get("status") in {"queued","writing","rollback_queued","rolling_back"} for row in batch["rows"]): raise CabinetError("批次仍在处理中，请完成后再回退",409)
        selected_ids={str(value) for value in payload.get("row_ids",[])}
        if payload.get("all") and not admin and not set(batch.get("scopes",[]))<=set(allowed): raise CabinetError("整批回退需要拥有全部楼栋权限",403)
        selected=[]
        for row in batch["rows"]:
            if not (payload.get("all") or row["row_id"] in selected_ids) or row.get("wrote_record") is False or row.get("status") not in {"completed","rollback_failed","rollback_blocked"}: continue
            if not admin and row.get("scope") not in allowed: raise CabinetError("选中记录包含无权操作的楼栋",403)
            selected.append(row)
        if not selected: raise CabinetError("没有可回退的已完成记录",409)
        ids={row["row_id"] for row in selected}
        def queue(current):
            for row in current["rows"]:
                if row["row_id"] in ids: row.update(status="rollback_queued",error="")
        queued=self._change(batch_id,queue,expected_version=batch["version"])
        grouped={}
        for row in selected: grouped.setdefault(row["scope"],[]).append(row)
        for scope,rows in grouped.items():
            ordered=[row["row_id"] for row in sorted(rows,key=lambda item:(item.get("actual",""),item.get("source_index",0)),reverse=True)]
            self._confirm_pool.submit(self._rollback_scope,batch_id,scope,ordered)
        return queued

    def cancel(self, batch_id, owner, admin=False):
        batch = self.get(batch_id)
        if not admin and batch["owner_id"] != owner:
            raise CabinetError("仅上传者或管理员可作废批次", 403)
        def cancel_rows(current):
            if any(row.get("status") in ("queued", "writing", "rollback_queued", "rolling_back") for row in current.get("rows", [])):
                raise CabinetError("批次正在提交，暂不能作废", 409)
            for row in current.get("rows", []):
                if row.get("status") not in ("completed", "rolled_back", "rollback_failed", "rollback_blocked"):
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

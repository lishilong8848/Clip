"""Feishu storage for learning; normalization and attachment quotas live in core.

The administrator sets ``enabled`` before initialization or worker writes.
Reads never initialize schema. Question metadata contains extensions only;
source text, unknown columns and legacy answer attachments remain independent.
"""
from __future__ import annotations

import copy
import gzip
import hashlib
import io
import json
import math
import re
import tempfile
import threading
import time
import uuid
import zlib
from pathlib import Path

import httpx

from upload_event_module.services.http_client import FeishuHTTPError, FeishuHttpClient


APP_TOKEN = "GKyVb2Az6auMwvs7VzVc3GO0nob"
BANK_TABLES = {
    "written": "tbldEn2ODX9CbZ1n",
    "duty": "tblpM9nCRs4UJ8io",
    "professional": "tblnyQverizwaQcT",
}
META_FIELD = "学练配置"
MATERIAL_FIELD = "学练资料"
ENTITY_TABLE_NAME = "画像学练数据"
EXTRA_FIELDS = {META_FIELD: 1, MATERIAL_FIELD: 17}
ENTITY_FIELDS = {"业务键": 1, "类别": 1, "内容": 1, "附件": 17}
MAX_TEXT_LENGTH = 100_000
MAX_DOCUMENT_BYTES = 20 * 1024 * 1024
DOCUMENT_SCHEMA = "learning.entity.gzip"
API_ROOT = "https://open.feishu.cn/open-apis"
TYPE_LABELS = {"single": "单选题", "multiple": "多选题", "interview": "面试题"}
TOKEN_ERRORS = {99991663, 99991664, 99991665, 99991668, 99991677}
READ_RETRY_CODES = {1255001, 1255002, 1254290, 1254291, 1254607}


class LearningCloudError(RuntimeError):
    """A rejected request or incomplete read; no partial snapshot is returned."""


class LearningCloudWriteUncertain(LearningCloudError):
    """The write may have committed; retry with the same logical identity."""

    unknown_result = True


def _text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list) and all(isinstance(v, dict) and isinstance(v.get("text"), str) for v in value):
        return "".join(v["text"] for v in value)
    raise LearningCloudError("云端文本字段格式错误")


def _checked_text(value, field):
    if not isinstance(value, str):
        raise ValueError(f"{field} 必须是文本")
    if len(value) > MAX_TEXT_LENGTH:
        raise ValueError(f"{field} 超过飞书文本字段 {MAX_TEXT_LENGTH} 字符限额；未截断或写入")
    return value


def _json_text(value, field, *, check_length=True):
    try:
        value = json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError):
        raise ValueError(f"{field} 必须是完整 JSON 数据，不能包含二进制或非有限数值") from None
    return _checked_text(value, field) if check_length else value


def _document_pointer(value):
    if not isinstance(value, dict) or value.get("schema_marker") != DOCUMENT_SCHEMA:
        return None
    if (type(value.get("version")) is not int or value["version"] != 1
            or type(value.get("raw_bytes")) is not int or not 0 < value["raw_bytes"] <= MAX_DOCUMENT_BYTES
            or not isinstance(value.get("sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", value["sha256"])):
        raise LearningCloudError("压缩实体文档指针无效：版本、SHA256 或原始字节数不符合要求（最大20MiB）")
    try:
        _identifier(value.get("file_token"), "document file_token")
    except ValueError:
        raise LearningCloudError("压缩实体文档指针必须包含有效 file_token，禁止 URL") from None
    return value


def _nonempty(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} 不能为空")
    return value


def _identifier(value, name, pattern=r"[A-Za-z0-9_-]{1,200}"):
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError(f"{name} 格式错误")
    return value


def _client_token(*parts):
    seed = json.dumps(["learning", APP_TOKEN, *parts], ensure_ascii=False).encode("utf-8")
    return str(uuid.UUID(bytes=hashlib.sha256(seed).digest()[:16], version=4))


def _attachment_refs(items):
    if not isinstance(items, list):
        raise ValueError("attachments 必须是列表")
    tokens = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("附件必须包含 file_token")
        token = _identifier(item.get("file_token"), "file_token")
        if token not in tokens:
            tokens.append(token)
    return [{"file_token": token} for token in tokens]


def _same_fields(expected, actual):
    if not isinstance(actual, dict):
        return False
    for name, value in expected.items():
        got = actual.get(name)
        if isinstance(value, str) and isinstance(got, list):
            got = _text(got)
        if name in (MATERIAL_FIELD, "附件") and isinstance(value, list):
            if not isinstance(got, list):
                got = [] if got is None else got
            if not isinstance(got, list) or any(not isinstance(a, dict) for a in got):
                return False
            got = [{"file_token": a.get("file_token")} for a in got]
        if value != got:
            return False
    return True


class LearningCloud:
    def __init__(self, *, enabled=False, http_client=None):
        self.enabled = enabled
        self._http = http_client if http_client is not None else FeishuHttpClient(
            timeout=httpx.Timeout(connect=5, read=30, write=60, pool=5), retries=1,
        )
        self._root = f"{API_ROOT}/bitable/v1/apps/{APP_TOKEN}"
        self._schema_ready = False
        self._source_fields = {}
        self._entity_table_id = None
        self._table_create_uncertain = False
        # Keep acknowledged uploads until the entity write is confirmed.
        self._pending_documents = {}
        # ponytail: one worker lock; use per-entity locks only if throughput requires it.
        self._lock = threading.RLock()

    def _require_write(self):
        if self.enabled is not True:
            raise LearningCloudError("学练云端写入尚未由管理员启用")
        from .portal_service import external_real_write_guard

        guard = external_real_write_guard()
        if not guard.get("real_write_allowed"):
            raise LearningCloudError(guard.get("reason") or "真实飞书写入未确认")

    @staticmethod
    def _headers(force_refresh=False):
        from upload_event_module.services.feishu_token_manager import token_manager

        try:
            token = token_manager.get_tenant_token(force_refresh=force_refresh)
            if not token:
                raise ValueError("empty token")
        except Exception:
            raise LearningCloudError("学练飞书授权失败，请检查应用配置与表权限") from None
        return {"Authorization": "Bearer " + token}

    def _request(self, method, path, body=None, params=None):
        read = method == "GET" or (method == "POST" and path.endswith("/records/search"))
        if not read:
            self._require_write()
        retryable = read or method == "PUT" or bool((params or {}).get("client_token"))
        headers = self._headers()
        for attempt in range(2):
            try:
                payload = self._http.request_json(
                    method, f"{self._root}/{path}", headers=headers, params=params,
                    json_payload=body, retries=1 if retryable else 0,
                )
            except (FeishuHTTPError, httpx.HTTPError, OSError):
                error = LearningCloudError if read else LearningCloudWriteUncertain
                raise error("学练云端读取失败" if read else "学练云端写入结果未知，请保留题目 ID 或实体业务键核验重试") from None
            if not isinstance(payload, dict) or not isinstance(payload.get("code"), int):
                error = LearningCloudError if read else LearningCloudWriteUncertain
                raise error("飞书返回无效响应，无法确认结果")
            code = payload["code"]
            if code in TOKEN_ERRORS and not attempt:
                headers = self._headers(force_refresh=True)
                continue
            if read and code in READ_RETRY_CODES and not attempt:
                time.sleep(0.5)
                continue
            if code:
                error = LearningCloudWriteUncertain if not read and code in {1255001, 1255002} else LearningCloudError
                # Do not echo remote messages, URLs or exception text containing credentials.
                raise error(f"学练飞书请求失败：code={code}")
            if not isinstance(payload.get("data"), dict):
                error = LearningCloudError if read else LearningCloudWriteUncertain
                raise error("飞书响应缺少 data，无法确认结果")
            return payload["data"]

    def _list_all(self, path, body=None):
        items, seen = [], set()
        token = None
        while True:
            params = {"page_size": 100 if path == "tables" or path.endswith("/fields") else 500}
            if token:
                params["page_token"] = token
            page = self._request("GET" if body is None else "POST", path, body, params)
            batch = page.get("items")
            if batch is None and token is None and page.get("has_more") is False and type(page.get("total")) is int and page["total"] == 0:
                batch = []
            if not isinstance(batch, list) or not all(isinstance(item, dict) for item in batch):
                raise LearningCloudError("飞书分页内容无效，未返回部分数据")
            if not isinstance(page.get("has_more"), bool):
                raise LearningCloudError("飞书分页标志缺失，未返回部分数据")
            items.extend(batch)
            if not page["has_more"]:
                return items
            token = page.get("page_token")
            if not isinstance(token, str) or not token or token in seen:
                raise LearningCloudError("飞书分页游标异常，未返回部分数据")
            seen.add(token)

    def fetch_questions(self):
        result = []
        for bank, table in BANK_TABLES.items():
            for record in self._list_all(f"tables/{table}/records"):
                if not record.get("record_id") or not isinstance(record.get("fields"), dict):
                    raise LearningCloudError("飞书题目记录不完整，未返回部分数据")
                result.append({"bank": bank, "record_id": record["record_id"], "fields": record["fields"]})
        return result

    def _find_entity_table(self):
        tables = [t for t in self._list_all("tables") if t.get("name") == ENTITY_TABLE_NAME]
        if len(tables) > 1:
            raise LearningCloudError("画像学练数据存在多个同名表，无法确定写入目标")
        return _identifier(tables[0].get("table_id"), "table_id", r"tbl[A-Za-z0-9]+") if tables else None

    def _fields(self, table):
        result = {}
        for field in self._list_all(f"tables/{table}/fields"):
            name = field.get("field_name")
            if not isinstance(name, str) or name in result or not isinstance(field.get("type"), int):
                raise LearningCloudError("飞书字段定义无效或重复")
            result[name] = field
        return result

    @staticmethod
    def _check_fields(fields, expected, *, required=False):
        for name, kind in expected.items():
            if name not in fields:
                if required:
                    raise LearningCloudError(f"源表字段缺失：{name}")
            elif fields[name]["type"] != kind:
                raise LearningCloudError(f"飞书字段类型错误：{name} 应为 {kind}")

    def ensure_schema(self):
        self._require_write()
        with self._lock:
            if self._schema_ready:
                return
            table = self._find_entity_table()
            sources = {bank: self._fields(tid) for bank, tid in BANK_TABLES.items()}
            entity_fields = self._fields(table) if table else {}
            for bank, fields in sources.items():
                required = {"题目": 1, "答案": 1}
                required.update({"题型": 3, "选项": 1, "附件": 1} if bank == "written" else {"答案图片": 17})
                self._check_fields(fields, required, required=True)
                year = "年份" if bank == "written" else "年度"
                if fields.get(year, {}).get("type") not in (1, 2, 3, 4):
                    raise LearningCloudError(f"源表年份字段缺失或类型不支持：{year}")
                self._check_fields(fields, EXTRA_FIELDS)
            self._check_fields(entity_fields, ENTITY_FIELDS)
            if not table:
                if self._table_create_uncertain:
                    raise LearningCloudWriteUncertain("上次建表结果未知且尚未查到表，请管理员核验后再初始化")
                try:
                    data = self._request("POST", "tables", {"table": {
                        "name": ENTITY_TABLE_NAME,
                        "fields": [{"field_name": n, "type": t} for n, t in ENTITY_FIELDS.items()],
                    }})
                    table = data.get("table_id")
                    if not isinstance(table, str) or not re.fullmatch(r"tbl[A-Za-z0-9]+", table):
                        raise LearningCloudWriteUncertain("建表响应未包含有效 table_id")
                except LearningCloudWriteUncertain:
                    self._table_create_uncertain = True
                    raise
                entity_fields = {n: {"type": t} for n, t in ENTITY_FIELDS.items()}
            for tid, fields, expected in [
                *((BANK_TABLES[b], f, EXTRA_FIELDS) for b, f in sources.items()),
                (table, entity_fields, ENTITY_FIELDS),
            ]:
                for name, kind in expected.items():
                    if name not in fields:
                        self._request("POST", f"tables/{tid}/fields", {"field_name": name, "type": kind},
                                      {"client_token": _client_token("field", tid, name)})
            self._source_fields = sources
            self._entity_table_id = table
            self._table_create_uncertain = False
            self._schema_ready = True

    def _write_record(self, table, fields, operation_id, identity, record_id=None):
        path = f"tables/{table}/records"
        params = None
        if record_id:
            path += "/" + _identifier(record_id, "record_id", r"rec[A-Za-z0-9]+")
        else:
            params = {"client_token": _client_token("record", table, identity)}
        data = self._request("PUT" if record_id else "POST", path, {"fields": fields}, params)
        record = data.get("record")
        rid = record.get("record_id") if isinstance(record, dict) else None
        if not isinstance(rid, str) or not re.fullmatch(r"rec[A-Za-z0-9]+", rid) or (record_id and rid != record_id):
            raise LearningCloudWriteUncertain("写入响应未包含匹配的 record_id，请使用原 operation_id 核验")
        # An idempotent create may replay the earlier version after a lost response.
        if not record_id and not _same_fields(fields, record.get("fields")):
            return self._write_record(table, fields, operation_id, identity, rid)
        return record

    def save_question(self, question, operation_id):
        self._require_write()
        _nonempty(operation_id, "operation_id")
        if not isinstance(question, dict):
            raise ValueError("question 必须是对象")
        q = copy.deepcopy(question)
        bank = q.get("bank")
        if bank not in BANK_TABLES or q.get("type") not in TYPE_LABELS:
            raise ValueError("题库或题型无效")
        rid = q.get("record_id")
        if rid:
            _identifier(rid, "record_id", r"rec[A-Za-z0-9]+")
        qid = _nonempty(q.get("id") or (f"{bank}:{rid}" if rid else None), "question.id")
        options = q.get("options", [])
        if not isinstance(options, list) or not all(isinstance(o, dict) for o in options):
            raise ValueError("options 必须是包含稳定 id 和 text 的列表")
        ids = [_nonempty(o.get("id"), "option.id") for o in options]
        if len(set(ids)) != len(ids):
            raise ValueError("选项 id 不可重复")
        labels = [str(o.get("label") or (chr(65 + i) if i < 26 else i + 1)) for i, o in enumerate(options)]
        correct = q.get("correct_option_ids", [])
        if not isinstance(correct, list) or not all(isinstance(i, str) and i in ids for i in correct):
            raise ValueError("correct_option_ids 必须引用现有选项")
        label = _nonempty(q.get("type_label") or TYPE_LABELS[q["type"]], "type_label")
        attachments = _attachment_refs(q.get("attachments", []))
        if any(a.get("kind") not in ("question", "answer", "material") for a in q.get("attachments", [])):
            raise ValueError("附件 kind 无效")
        metadata = copy.deepcopy(q.get("metadata", {}))
        if not isinstance(metadata, dict):
            raise ValueError("metadata 必须是对象")
        metadata.update({k: v for k, v in q.items() if not k.startswith("_") and k not in {
            "bank", "record_id", "stem", "year", "metadata", "fields", "raw", "problems",
        }})
        metadata.update(id=qid, type_label=label)
        option_text = "\n".join(f"{label}. {_checked_text(o.get('text'), 'option.text')}" for label, o in zip(labels, options))
        answer = q.get("answer_text", "") if q["type"] == "interview" or not correct else ",".join(labels[ids.index(i)] for i in correct)
        metadata["source_answer"] = answer
        fields = {
            "题目": _checked_text(q.get("stem"), "stem"),
            "答案": _checked_text(answer, "答案"),
            META_FIELD: _json_text(metadata, META_FIELD), MATERIAL_FIELD: attachments,
        }
        if bank == "written":
            fields.update({"题型": label, "选项": _checked_text(option_text, "选项")})
        year_name = "年份" if bank == "written" else "年度"
        year = q.get("year", "")
        if year is not None and not isinstance(year, (str, int, float)):
            raise ValueError("year 必须是文本或数字")
        with self._lock:
            self.ensure_schema()
            year_type = self._source_fields[bank][year_name]["type"]
            if year_type == 2:
                try:
                    value = float(year) if year not in (None, "") else None
                    if value is not None and not math.isfinite(value):
                        raise ValueError()
                except (ValueError, OverflowError):
                    raise ValueError("源表年份字段要求有限数字") from None
            else:
                value = _checked_text("" if year is None else str(year), year_name)
                if year_type == 4:
                    value = [value] if value else []
            fields[year_name] = value
            record = self._write_record(BANK_TABLES[bank], fields, operation_id, qid, rid)
        return {**q, "id": qid, "record_id": record["record_id"]}

    def upsert_entity(self, kind, key, payload, operation_id):
        self._require_write()
        _nonempty(operation_id, "operation_id")
        fields = {"类别": _checked_text(_nonempty(kind, "kind"), "类别"),
                  "业务键": _checked_text(_nonempty(key, "key"), "业务键")}
        if isinstance(payload, dict) and payload.get("schema_marker") == DOCUMENT_SCHEMA:
            raise ValueError("实体 payload 不得使用保留的压缩文档 schema_marker")
        content = _json_text(payload, "内容", check_length=False)
        try:
            raw = content.encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError("实体 JSON 必须能完整编码为 UTF-8") from None
        if len(raw) > MAX_DOCUMENT_BYTES:
            raise ValueError("实体 JSON 原始字节超过20MiB；未截断或写入")
        if isinstance(payload, dict) and ("attachments" in payload or "file_token" in payload):
            items = payload.get("attachments", [])
            if not isinstance(items, list) or not all(isinstance(a, dict) for a in items):
                raise ValueError("attachments 必须是附件对象列表")
            items = [*items, *([payload] if payload.get("file_token") else [])]
            # Pending local references remain intact in the JSON document.
            fields["附件"] = _attachment_refs([a for a in items if a.get("file_token")])
        with self._lock:
            self.ensure_schema()
            table = self._entity_table_id
            rows = self._list_all(f"tables/{table}/records/search", {"filter": {
                "conjunction": "and", "conditions": [
                    {"field_name": name, "operator": "is", "value": [value]}
                    for name, value in (("类别", kind), ("业务键", key))
                ],
            }})
            for row in rows:
                actual = row.get("fields", {})
                if not row.get("record_id") or _text(actual.get("类别")) != kind or _text(actual.get("业务键")) != key:
                    raise LearningCloudError("实体查询结果与类别、业务键不匹配")
            if len(rows) > 1:
                raise LearningCloudError("同类别和业务键存在多条记录，禁止覆盖或新增")
            existing = rows[0]["fields"] if rows else {}
            previous = None
            if existing.get("内容"):
                try:
                    previous = _document_pointer(json.loads(_text(existing["内容"])))
                except (ValueError, TypeError):
                    raise LearningCloudError("云端实体内容不是有效 JSON，无法确认已有文档指针") from None
            refs = [a for a in _attachment_refs(existing.get("附件") or [])
                    if not previous or a.get("file_token") != previous["file_token"]]
            refs.extend(fields.get("附件", []))
            if len(content) > MAX_TEXT_LENGTH:
                sha = hashlib.sha256(raw).hexdigest()
                pointer = next((p for p in (previous, self._pending_documents.get((kind, key)))
                                if p and p["sha256"] == sha and p["raw_bytes"] == len(raw)), None)
                if pointer is None:
                    compressed = gzip.compress(raw, mtime=0)
                    if len(compressed) > MAX_DOCUMENT_BYTES:
                        raise ValueError("实体压缩附件超过20MiB；未截断或上传")
                    with tempfile.TemporaryDirectory(prefix="learning-document-") as directory:
                        path = Path(directory) / "document.json.gz"
                        path.write_bytes(compressed)
                        attachment = self.upload_attachment(path, f"learning-{sha}.json.gz")
                    pointer = {"schema_marker": DOCUMENT_SCHEMA, "version": 1,
                               "file_token": attachment["file_token"], "sha256": sha, "raw_bytes": len(raw)}
                self._pending_documents[(kind, key)] = pointer
                content = _json_text(pointer, "内容")
                refs.append({"file_token": pointer["file_token"]})
            fields["内容"] = content
            if refs or "附件" in fields or previous:
                fields["附件"] = _attachment_refs(refs)
            result = self._write_record(table, fields, operation_id, [kind, key], rows[0]["record_id"] if rows else None)
            self._pending_documents.pop((kind, key), None)
            return result

    def _load_entity_document(self, value):
        pointer = _document_pointer(value)
        if pointer is None:
            return value
        compressed = self.download_attachment(pointer["file_token"], MAX_DOCUMENT_BYTES)
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
                raw = stream.read(pointer["raw_bytes"] + 1)
        except (OSError, EOFError, zlib.error):
            raise LearningCloudError("压缩实体文档损坏或不完整，未返回部分数据") from None
        if len(raw) != pointer["raw_bytes"]:
            raise LearningCloudError("实体文档解压字节数不匹配或超过20MiB上限")
        if hashlib.sha256(raw).hexdigest() != pointer["sha256"]:
            raise LearningCloudError("实体文档 SHA256 校验失败，未返回部分数据")
        try:
            return json.loads(raw.decode("utf-8"))
        except (ValueError, RecursionError):
            raise LearningCloudError("解压实体文档不是有效 UTF-8 JSON，未返回部分数据") from None

    def load_entities(self):
        table = self._find_entity_table()
        if not table:
            return []
        result, seen = [], set()
        for row in self._list_all(f"tables/{table}/records"):
            fields = row.get("fields", {})
            kind, key = _text(fields.get("类别")), _text(fields.get("业务键"))
            if not kind.strip() or not key.strip() or (kind, key) in seen:
                raise LearningCloudError("实体类别、业务键为空或重复，未返回部分数据")
            try:
                payload = self._load_entity_document(json.loads(_text(fields.get("内容"))))
            except (ValueError, TypeError):
                raise LearningCloudError("云端实体内容不是有效 JSON，未返回部分数据") from None
            seen.add((kind, key))
            result.append({"kind": kind, "key": key, "payload": payload})
        return result

    def upload_attachment(self, path, name):
        self._require_write()
        path = Path(path)
        _nonempty(name, "name")
        if not path.is_file():
            raise ValueError("附件文件不存在")
        size = path.stat().st_size
        if size <= 0:
            raise ValueError("附件文件为空")
        headers = self._headers()
        try:
            payload = self._http.request_file_json(
                "POST", f"{API_ROOT}/drive/v1/medias/upload_all", headers=headers,
                file_path=str(path), file_name=name,
                data={"file_name": name, "parent_type": "bitable_file", "parent_node": APP_TOKEN, "size": str(size)},
                retries=0,
            )
        except (FeishuHTTPError, httpx.HTTPError, OSError):
            raise LearningCloudWriteUncertain("附件上传结果未知，未自动重复上传") from None
        if not isinstance(payload, dict) or not isinstance(payload.get("code"), int):
            raise LearningCloudWriteUncertain("附件上传响应无效，无法确认结果")
        if payload["code"]:
            error = LearningCloudWriteUncertain if payload["code"] in {1255001, 1255002} else LearningCloudError
            raise error(f"附件上传失败：code={payload['code']}")
        if not isinstance(payload.get("data"), dict):
            raise LearningCloudWriteUncertain("附件上传响应缺少 data，无法确认结果")
        token = payload["data"].get("file_token")
        if not isinstance(token, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", token):
            raise LearningCloudWriteUncertain("附件上传未返回有效 file_token")
        return {"file_token": token, "name": name, "size": size}

    def download_attachment(self, token, max_bytes):
        _identifier(token, "file_token")
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("max_bytes 必须是正整数")
        headers = self._headers()
        try:
            content, _ = self._http.request_bytes(
                "GET", f"{API_ROOT}/drive/v1/medias/{token}/download",
                headers=headers, retries=1, max_bytes=max_bytes,
            )
        except (FeishuHTTPError, httpx.HTTPError, OSError):
            raise LearningCloudError("附件下载失败或超过 max_bytes") from None
        if len(content) > max_bytes:
            raise LearningCloudError("附件超过 max_bytes")
        return content

    def close(self):
        self._http.close()

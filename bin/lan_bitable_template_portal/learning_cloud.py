"""Feishu storage for learning; normalization and attachment quotas live in core.

The administrator sets ``enabled`` before initialization or worker writes.
Reads never initialize schema. Question metadata contains extensions only;
source text, unknown columns and legacy answer attachments remain independent.
"""
from __future__ import annotations

import copy
import datetime as dt
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
from contextlib import contextmanager
from pathlib import Path

import httpx

from upload_event_module.services.http_client import FeishuHTTPError, FeishuHttpClient


APP_TOKEN = "GKyVb2Az6auMwvs7VzVc3GO0nob"
BANK_TABLES = {
    "written": "tbldEn2ODX9CbZ1n",
    "duty": "tblpM9nCRs4UJ8io",
    "professional": "tblnyQverizwaQcT",
    "supplemental": "tbl1x3DulQ3nFluq",
}
BANK_VIEWS = {"supplemental": "vew6IeZZgN"}
SHARING_TABLE_ID = "tblQevBJ9knFaBNh"
SHARING_FIELDS = {name: 1 for name in ("会议名称", "题目", "选项", "答案", "备注", "学练配置", "会议纪要链接")}
META_FIELD = "学练配置"
MATERIAL_FIELD = "学练资料"
ENTITY_TABLE_NAME = "画像学练数据"
EXTRA_FIELDS = {META_FIELD: 1, MATERIAL_FIELD: 17}
ENTITY_FIELDS = {"业务键": 1, "类别": 1, "内容": 1, "附件": 17}
ENTITY_KIND_LABELS = {
    "person": "人员档案", "reserve": "题目预留", "paper": "学练题单", "record": "作答记录",
    "issue": "题目质疑", "settings": "系统设置", "publication": "发布批次", "notification": "通知任务",
    "engagement": "学练提醒", "audit": "题目审计", "attachment": "附件资料", "question": "题目",
    "sharing_batch": "分享批次", "sharing_attempt": "分享作答", "sharing_delivery": "分享投递",
    "daily_progress": "每日完成情况", "shift_person": "班次人员汇总", "local": "本地状态",
}
ENTITY_KIND_ALIASES = {label: kind for kind, label in ENTITY_KIND_LABELS.items()}
SHIFT_ENTITY_KINDS = {"paper", "record", "reserve", "notification", "engagement",
                      "sharing_attempt", "sharing_delivery", "daily_progress"}
PERSONNEL_SUMMARY_LABELS = {
    "date": "日期", "shift": "班次", "person_id": "人员标识", "name": "姓名", "scopes": "楼栋",
    "status": "完成状态", "assigned": "应答题数", "answered": "已答题数", "correct": "正确题数",
    "objective_answered": "已答客观题数", "answer_viewed": "看答案后作答数",
    "answer_view_unknown": "辅助作答无法细分数", "practice_questions": "主动练习量",
    "review_count": "复习次数", "updated_at": "更新时间",
    "delivery_status": "题单发送状态",
}
PERSONNEL_FIELDS = {"题单发送状态": 1}
MAX_TEXT_LENGTH = 100_000
MAX_DOCUMENT_BYTES = 20 * 1024 * 1024
DOCUMENT_SCHEMA = "learning.entity.gzip"
API_ROOT = "https://open.feishu.cn/open-apis"
TYPE_LABELS = {"single": "单选题", "multiple": "多选题", "interview": "面试题"}
TOKEN_ERRORS = {99991663, 99991664, 99991665, 99991668, 99991677}
READ_RETRY_CODES = {1255001, 1255002, 1254290, 1254291, 1254607}
MAX_READ_PAGES = 1000


class LearningCloudError(RuntimeError):
    """A rejected request or incomplete read; no partial snapshot is returned."""


class LearningCloudConflict(LearningCloudError):
    """The source changed; recompute display fields from a fresh snapshot."""

    conflict = True


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
    if isinstance(value, dict) and "完整明细" in value and "记录说明" in value:
        details = value["完整明细"]
        if not isinstance(details, dict) or not isinstance(value["记录说明"], str):
            raise LearningCloudError("中文实体文档指针无效")
        value = {"schema_marker": DOCUMENT_SCHEMA, "version": details.get("版本"),
                 "file_token": details.get("附件标识"), "sha256": details.get("内容校验"),
                 "raw_bytes": details.get("原始字节数")}
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


def _entity_kind(value):
    return ENTITY_KIND_ALIASES.get(_nonempty(value, "kind"), value)


def _shift_entities(payload):
    if (not isinstance(payload, dict) or type(payload.get("schema_version")) is not int
            or payload["schema_version"] != 1 or not isinstance(payload.get("summary"), dict)
            or not isinstance(payload.get("entities"), list)):
        raise LearningCloudError("班次人员汇总结构无效，未返回部分数据")
    _payload_digest(payload)
    try:
        date = payload.get("date")
        if (not isinstance(date, str) or dt.date.fromisoformat(date).isoformat() != date
                or payload.get("shift") not in ("白", "夜", "未归班")):
            raise ValueError()
        _nonempty(payload.get("person_id"), "person_id")
        if any(payload["summary"].get(name) != payload[name] for name in ("date", "shift", "person_id")):
            raise ValueError()
    except (TypeError, ValueError):
        raise LearningCloudError("班次人员汇总日期、班次或人员标识无效，未返回部分数据") from None
    entities, seen = [], set()
    for item in payload["entities"]:
        try:
            if not isinstance(item, dict) or not isinstance(item.get("payload"), dict):
                raise ValueError()
            kind, key = _entity_kind(item.get("kind")), _nonempty(item.get("key"), "key")
            if (kind not in SHIFT_ENTITY_KINDS or (kind, key) in seen
                    or "person_id" in item["payload"] and item["payload"]["person_id"] != payload["person_id"]):
                raise ValueError()
        except (TypeError, ValueError):
            raise LearningCloudError("班次人员汇总内层实体无效、嵌套或业务键重复，未返回部分数据") from None
        seen.add((kind, key))
        entities.append({"kind": kind, "key": key, "payload": item["payload"]})
    return entities


def _display_field_types():
    from .learning_completion import DISPLAY_FIELDS
    return {**DISPLAY_FIELDS, "人员": 11}


def _payload_digest(payload):
    try:
        content = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(content.encode("utf-8")).hexdigest()
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise LearningCloudError("实体内容无法生成完整摘要") from None


def _checked_display_fields(fields):
    if fields is None:
        return {}
    if not isinstance(fields, dict):
        raise ValueError("汇总展示字段必须是对象")
    allowed, result = {**_display_field_types(), **PERSONNEL_FIELDS}, {}
    for name, value in fields.items():
        kind = allowed.get(name)
        if kind is None:
            raise ValueError("包含未允许的汇总展示字段")
        if kind == 1:
            value = _checked_text(value, name)
        elif kind in (2, 5):
            if value is None or isinstance(value, str) and not value.strip():
                value = None
            elif type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError(f"{name} 必须是有限数值或空值")
        elif kind == 11:
            if not isinstance(value, list) or len(value) > 1 or any(not isinstance(v, dict) for v in value):
                raise ValueError("人员必须是已核验的唯一当前应用身份，无法核验时使用空列表")
            value = [{"id": _identifier(v.get("id"), "人员", r"ou_[A-Za-z0-9_-]{1,197}")} for v in value]
        result[name] = value
    return result


def _same_fields(expected, actual):
    if not isinstance(actual, dict):
        return False
    for name, value in expected.items():
        got = actual.get(name)
        if isinstance(value, str) and isinstance(got, list):
            try:
                got = _text(got)
            except LearningCloudError:
                return False
        if value == "" and got is None:
            got = ""
        if value is None and got == "":
            got = None
        if name == "人员" and isinstance(value, list):
            got = [] if got is None else got
            if not isinstance(got, list) or any(not isinstance(v, dict) for v in got):
                return False
            got = [{"id": v.get("id")} for v in got]
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
    shift_storage_enabled = True
    personnel_only_storage = True

    def __init__(self, *, enabled=False, http_client=None):
        self.enabled = enabled
        self._http = http_client if http_client is not None else FeishuHttpClient(
            timeout=httpx.Timeout(connect=5, read=30, write=60, pool=5), retries=1,
        )
        self._root = f"{API_ROOT}/bitable/v1/apps/{APP_TOKEN}"
        self._schema_ready = False
        self._sharing_schema_ready = False
        self._source_fields = {}
        self._entity_table_id = None
        self.completion_schema_ready = False
        self.shift_schema_ready = False
        self._table_create_uncertain = False
        # Keep acknowledged uploads until the entity write is confirmed.
        self._pending_documents = {}
        self._pending_document_credentials = None
        self._entity_context = threading.local()
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
    def current_app_id():
        from upload_event_module.services.feishu_token_manager import token_manager
        try:
            return _nonempty(token_manager._credentials()[0], "app_id")
        except Exception:
            raise LearningCloudError("Qt 飞书应用身份暂不可用") from None

    @contextmanager
    def _entity_operation(self, expected_app_id=None):
        from upload_event_module.services.feishu_token_manager import token_manager
        with self._lock:
            try:
                credentials = token_manager._credentials()
            except Exception:
                raise LearningCloudError("Qt 飞书鉴权暂不可用，实体操作保留 pending") from None
            if expected_app_id is not None and expected_app_id != credentials[0]:
                raise LearningCloudConflict("Qt 飞书应用与人员目录核验身份不一致，请重新读取目录，实体操作保留 pending")
            if self._pending_document_credentials != credentials:
                self._pending_documents.clear()
                self._pending_document_credentials = credentials
            self._entity_context.credentials = credentials
            try:
                self._check_entity_credentials()
                yield
                self._check_entity_credentials()
            finally:
                self._entity_context.credentials = None

    def _check_entity_credentials(self):
        credentials = getattr(self._entity_context, "credentials", None)
        if credentials is None:
            return
        from upload_event_module.services.feishu_token_manager import token_manager
        try:
            matching = token_manager._credentials() == credentials
        except Exception:
            matching = False
        if not matching:
            self._pending_documents.clear()
            raise LearningCloudConflict("Qt 飞书鉴权已变化，实体操作保留 pending，未跨应用继续") from None

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
        from upload_event_module.services.feishu_token_manager import token_manager
        self._check_entity_credentials()
        read = method == "GET" or (method == "POST" and path.endswith("/records/search"))
        if not read:
            self._require_write()
        try:
            credentials = token_manager._credentials()
        except Exception:
            raise LearningCloudError("Qt 飞书鉴权暂不可用，未发送学练请求") from None

        def check_credentials():
            self._check_entity_credentials()
            try:
                matching = token_manager._credentials() == credentials
            except Exception:
                matching = False
            if not matching:
                raise LearningCloudError("Qt 飞书鉴权已变化，未跨应用重试学练请求") from None

        headers = self._headers()
        for attempt in range(2):
            check_credentials()
            try:
                payload = self._http.request_json(
                    method, f"{self._root}/{path}", headers=headers, params=params,
                    json_payload=body, retries=1 if read else 0,
                )
            except (FeishuHTTPError, httpx.HTTPError, OSError):
                error = LearningCloudError if read else LearningCloudWriteUncertain
                raise error("学练云端读取失败" if read else "学练云端写入结果未知，请保留题目 ID 或实体业务键核验重试") from None
            if not isinstance(payload, dict) or not isinstance(payload.get("code"), int):
                error = LearningCloudError if read else LearningCloudWriteUncertain
                raise error("飞书返回无效响应，无法确认结果")
            code = payload["code"]
            if code in TOKEN_ERRORS and not attempt:
                check_credentials()
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
            self._check_entity_credentials()
            return payload["data"]

    def _list_all(self, path, body=None, params=None):
        items, seen, identities = [], set(), set()
        token, expected_total = None, None
        id_field = 'table_id' if path == 'tables' else 'field_id' if path.endswith('/fields') else 'record_id'
        base_params = dict(params or {})
        # Bound unique cursor streams even when the server omits total.
        for _ in range(MAX_READ_PAGES):
            params = {**base_params, "page_size": 100 if path == "tables" or path.endswith("/fields") else 500}
            if token:
                params["page_token"] = token
            page = self._request("GET" if body is None else "POST", path, body, params)
            batch = page.get("items")
            if batch is None and token is None and page.get("has_more") is False and type(page.get("total")) is int and page["total"] == 0:
                batch = []
            if not isinstance(batch, list) or len(batch) > params['page_size'] or not all(isinstance(item, dict) for item in batch):
                raise LearningCloudError("飞书分页内容无效，未返回部分数据")
            if not isinstance(page.get("has_more"), bool):
                raise LearningCloudError("飞书分页标志缺失，未返回部分数据")
            if 'total' in page:
                total = page['total']
                if type(total) is not int or total < 0 or expected_total is not None and total != expected_total:
                    raise LearningCloudError("飞书分页总条数无效或发生变化，未返回部分数据")
                expected_total = total
            for item in batch:
                identity = item.get(id_field, item.get('field_name') if id_field == 'field_id' else None)
                if not isinstance(identity, str) or not identity.strip() or identity in identities:
                    raise LearningCloudError("飞书分页记录标识缺失或重复，未返回部分数据")
                identities.add(identity)
            items.extend(batch)
            if expected_total is not None and (len(items) > expected_total
                    or not page['has_more'] and len(items) != expected_total
                    or page['has_more'] and len(items) >= expected_total):
                raise LearningCloudError("飞书分页累计条数与total不一致，未返回部分数据")
            if not page["has_more"]:
                return items
            if not batch:
                raise LearningCloudError("飞书分页未取得新记录，未返回部分数据")
            token = page.get("page_token")
            if not isinstance(token, str) or not token or token in seen:
                raise LearningCloudError("飞书分页游标异常，未返回部分数据")
            seen.add(token)
        raise LearningCloudError("飞书分页超过只读页数上限，未返回部分数据")

    def fetch_questions(self):
        result = []
        for bank, table in BANK_TABLES.items():
            for record in self._list_all(f"tables/{table}/records", params={"view_id": BANK_VIEWS[bank]} if bank in BANK_VIEWS else None):
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
                required.update({"选项": 1, "专业": 3, "答案附件": 17} if bank == 'supplemental' else
                                {"题型": 3, "选项": 1, "附件": 1} if bank == "written" else {"答案图片": 17})
                self._check_fields(fields, required, required=True)
                year = "年份" if bank == "written" else "年度"
                if bank != 'supplemental' and fields.get(year, {}).get("type") not in (1, 2, 3, 4):
                    raise LearningCloudError(f"源表年份字段缺失或类型不支持：{year}")
                self._check_fields(fields, EXTRA_FIELDS)
            self._check_fields(entity_fields, ENTITY_FIELDS)
            from .learning_completion import DISPLAY_FIELDS
            self.completion_schema_ready = all(entity_fields.get(name, {}).get('type') == kind for name, kind in DISPLAY_FIELDS.items())
            self.shift_schema_ready = self.completion_schema_ready and entity_fields.get("人员", {}).get("type") == 11
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
        params = {"user_id_type": "open_id"} if "人员" in fields else {}
        if record_id:
            path += "/" + _identifier(record_id, "record_id", r"rec[A-Za-z0-9]+")
        else:
            params["client_token"] = _client_token("record", table, identity)
        data = self._request("PUT" if record_id else "POST", path, {"fields": fields}, params)
        record = data.get("record")
        rid = record.get("record_id") if isinstance(record, dict) else None
        if not isinstance(rid, str) or not re.fullmatch(r"rec[A-Za-z0-9]+", rid) or (record_id and rid != record_id):
            raise LearningCloudWriteUncertain("写入响应未包含匹配的 record_id，请使用原 operation_id 核验")
        # An idempotent create may replay the earlier version after a lost response.
        if not _same_fields(fields, record.get("fields")):
            if not record_id:
                return self._write_record(table, fields, operation_id, identity, rid)
            raise LearningCloudWriteUncertain("写入回执与预期字段不一致，请保留原业务键核验重试")
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
        elif bank == 'supplemental':
            fields.update({'选项': _checked_text(option_text, '选项'), '专业': _checked_text(q.get('specialty', ''), '专业') or None})
        year_name = "年份" if bank == "written" else "年度"
        year = q.get("year", "")
        if year is not None and not isinstance(year, (str, int, float)):
            raise ValueError("year 必须是文本或数字")
        with self._lock:
            self.ensure_schema()
            year_type = self._source_fields[bank].get(year_name, {}).get('type')
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
            if bank != 'supplemental':
                fields[year_name] = value
            record = self._write_record(BANK_TABLES[bank], fields, operation_id, qid, rid)
        return {**q, "id": qid, "record_id": record["record_id"]}

    def save_sharing_question(self, batch, question, operation_id):
        self._require_write()
        _nonempty(operation_id, "operation_id")
        from .learning_sharing import QUESTION_TYPES, validate_questions

        if not isinstance(batch, dict) or batch.get('status') not in {'published', 'closed'} or not isinstance(question, dict):
            raise LearningCloudError('只能将已发布的纪要题目同步到题库')
        bid = _identifier(batch.get('id'), 'batch.id', r'[0-9a-f]{32}')
        checked = validate_questions(batch.get('questions'), batch.get('source', {}).get('text', ''), batch.get('question_types'), images=batch.get('source', {}).get('images', []))
        q = next((item for item in checked if item['id'] == question.get('id')), None)
        if q is None or q != question:
            raise LearningCloudError('题库任务与已发布题目快照不一致，未写入')
        identity = f"sharing:{bid}:{q['id']}"
        metadata = {'source': 'meeting-sharing', 'sync_id': identity, 'batch_id': bid, 'question_id': q['id'], 'type': q['type']}
        if q.get('image_ids'):
            metadata['image_ids'] = q['image_ids']
            metadata['image_page_url'] = batch.get('image_page_url', '')
        sources = batch.get('source', {}).get('sources', [])
        if q.get('origin') == 'manual' or sources:
            metadata['origin'] = q.get('origin', 'ai')
            metadata['sources'] = [{name: source[name] for name in ('id', 'title', 'url', 'sha256', 'revision') if name in source}
                                   for source in sources]
        basis = '专业经验' if q.get('origin') == 'manual' else '纪要依据'
        fields = {
            '会议名称': _checked_text(_nonempty(batch.get('title'), 'title'), '会议名称'),
            '题目': q['stem'],
            '答案': q['reference_answer'] if q['type'] == 'interview' else '、'.join(chr(65 + i) for i in q['correct']),
            '备注': f"题型：{QUESTION_TYPES[q['type']]}\n解析：{q['explanation']}\n{basis}：{q['evidence']}",
            '会议纪要链接': _checked_text(batch.get('url') or '', '会议纪要链接'),
            META_FIELD: _json_text(metadata, META_FIELD),
        }
        if q['options']:
            fields['选项'] = '\n'.join(f'{chr(65 + i)}. {option}' for i, option in enumerate(q['options']))
        if q.get('image_ids'):
            fields['备注'] += '\n题图（登录后查看）：' + batch.get('image_page_url', '')
        with self._lock:
            if not self._sharing_schema_ready:
                self._check_fields(self._fields(SHARING_TABLE_ID), SHARING_FIELDS, required=True)
                self._sharing_schema_ready = True
            rows = self._list_all(f'tables/{SHARING_TABLE_ID}/records/search', {'filter': {
                'conjunction': 'and', 'conditions': [{'field_name': META_FIELD, 'operator': 'contains', 'value': [f'"{identity}"']}],
            }})
            if len(rows) > 1:
                raise LearningCloudError('该纪要题目存在多条入库记录，请核验；未重复新增')
            if rows:
                record = rows[0]
                if not _same_fields(fields, record.get('fields')) or (not q['options'] and record.get('fields', {}).get('选项')):
                    raise LearningCloudError('已入库题目与发布快照不一致，请核验；未覆盖或重复新增')
            else:
                data = self._request('POST', f'tables/{SHARING_TABLE_ID}/records', {'fields': fields},
                                     {'client_token': _client_token('record', SHARING_TABLE_ID, identity)})
                record = data.get('record') or {}
                if not _same_fields(fields, record.get('fields')):
                    raise LearningCloudWriteUncertain('题库写入内容尚未核验，下次将查询原题确认；未重复新增')
            rid = record.get('record_id')
            if not isinstance(rid, str) or not re.fullmatch(r'rec[A-Za-z0-9]+', rid):
                raise LearningCloudWriteUncertain('题库写入响应缺少记录ID，将核验原题后重试')
            return rid

    def ensure_completion_schema(self, *, create=False, expected_table_id=None):
        """Explicit deployment operation; ordinary reads/writes never create report columns."""
        from .learning_completion import DISPLAY_FIELDS
        if create:
            self._require_write()
        table = self._find_entity_table()
        if not table:
            raise LearningCloudError('画像学练数据表尚未就绪。')
        if expected_table_id is not None and table != expected_table_id:
            raise LearningCloudError('画像学练数据表与指定目标不一致，未新增字段。')
        fields = self._fields(table)
        expected = _display_field_types()
        self._check_fields(fields, expected)
        missing = {name: kind for name, kind in expected.items() if name not in fields}
        if create:
            for name, kind in missing.items():
                self._request('POST', f'tables/{table}/fields', {'field_name': name, 'type': kind},
                              {'client_token': _client_token('field', table, name)})
            fields = self._fields(table)
            self._check_fields(fields, expected)
            missing = {name: kind for name, kind in expected.items() if name not in fields}
        self.completion_schema_ready = not (missing.keys() & DISPLAY_FIELDS.keys())
        self.shift_schema_ready = not missing
        return {'table_id': table, 'ready': self.shift_schema_ready, 'missing': sorted(missing)}

    def _display_table(self, display_fields, *, shift=False):
        table = self._find_entity_table()
        if not table:
            raise LearningCloudError("画像学练数据原表尚未就绪，保留 pending 等待管理员核验")
        fields = self._fields(table)
        expected = {**_display_field_types(), **PERSONNEL_FIELDS}
        required = ({name: kind for name, kind in expected.items() if name not in PERSONNEL_FIELDS or name in display_fields}
                    if shift else {name: expected[name] for name in display_fields})
        try:
            self._check_fields(fields, {**ENTITY_FIELDS, **required}, required=True)
        except LearningCloudError as exc:
            raise LearningCloudError(f"{exc}；汇总保留 pending 等待管理员核验") from None
        return table

    def ensure_personnel_schema(self):
        self._require_write()
        with self._entity_operation():
            table = self._find_entity_table()
            if not table:
                raise LearningCloudError('画像学练原表尚未就绪，未接入其它表。')
            fields = self._fields(table)
            self._check_fields(fields, PERSONNEL_FIELDS)
            for name, kind in PERSONNEL_FIELDS.items():
                if name not in fields:
                    self._request('POST', f'tables/{table}/fields', {'field_name': name, 'type': kind},
                                  {'client_token': _client_token('field', table, name)})
            self._check_fields(self._fields(table), PERSONNEL_FIELDS, required=True)

    def upsert_entity(self, kind, key, payload, operation_id, *, display_fields=None, expected_app_id=None,
                      expected_previous_sha256=None):
        self._require_write()
        _nonempty(operation_id, "operation_id")
        kind = _entity_kind(kind)
        display = _checked_display_fields(display_fields)
        if kind == "shift_person":
            _shift_entities(payload)
        fields = {"类别": _checked_text(ENTITY_KIND_LABELS.get(kind, kind), "类别"),
                  "业务键": _checked_text(_nonempty(key, "key"), "业务键")}
        if isinstance(payload, dict) and (payload.get("schema_marker") == DOCUMENT_SCHEMA
                                         or "完整明细" in payload and "记录说明" in payload):
            raise ValueError("实体 payload 不得使用保留的压缩文档 schema_marker")
        content = _json_text(payload, "内容", check_length=False)
        personnel_summary = kind == "shift_person" and not payload["entities"]
        if personnel_summary:
            if set(payload["summary"]) - PERSONNEL_SUMMARY_LABELS.keys():
                raise LearningCloudError("人员汇总包含非汇总内容，未将内部资料写入原表")
            content = _json_text({"人员班次答题情况": {
                PERSONNEL_SUMMARY_LABELS[name]: value for name, value in payload["summary"].items()
            }}, "内容")
        try:
            raw = content.encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError("实体 JSON 必须能完整编码为 UTF-8") from None
        if len(raw) > MAX_DOCUMENT_BYTES:
            raise ValueError("实体 JSON 原始字节超过20MiB；未截断或写入")
        attachment_payloads = [payload]
        if kind == "shift_person":
            attachment_payloads.extend(item["payload"] for item in payload["entities"])
        for value in attachment_payloads:
            if not isinstance(value, dict) or not ("attachments" in value or "file_token" in value):
                continue
            items = value.get("attachments", [])
            if not isinstance(items, list) or not all(isinstance(a, dict) for a in items):
                raise ValueError("attachments 必须是附件对象列表")
            items = [*fields.get("附件", []), *items, *([value] if value.get("file_token") else [])]
            # Pending local references remain intact in the JSON document.
            fields["附件"] = _attachment_refs([a for a in items if a.get("file_token")])
        with self._entity_operation(expected_app_id):
            if kind == "shift_person" or display_fields is not None:
                table = self._display_table(display, shift=kind == "shift_person")
            else:
                self.ensure_schema()
                table = self._entity_table_id
            if kind == 'daily_progress':
                if payload.get('id') != key:
                    raise LearningCloudError('每日完成情况业务键不一致。')
                if display_fields is None:
                    from .learning_completion import display_fields as daily_display_fields
                    if not self.completion_schema_ready:
                        raise LearningCloudError('每日完成情况云列尚未由管理员初始化，日报保留本地等待重试。')
                    fields.update(daily_display_fields(payload))
            fields.update(display)
            rows = []
            for alias in dict.fromkeys((kind, ENTITY_KIND_LABELS.get(kind, kind))):
                found = self._list_all(f"tables/{table}/records/search", {"filter": {
                    "conjunction": "and", "conditions": [
                        {"field_name": name, "operator": "is", "value": [value]}
                        for name, value in (("类别", alias), ("业务键", key))
                    ],
                }})
                for row in found:
                    actual = row.get("fields")
                    if (not row.get("record_id") or not isinstance(actual, dict)
                            or _text(actual.get("类别")) != alias or _text(actual.get("业务键")) != key):
                        raise LearningCloudError("实体查询结果与类别、业务键不匹配")
                rows.extend(found)
            if len(rows) > 1:
                raise LearningCloudError("同类别和业务键存在多条记录，禁止覆盖或新增")
            existing = rows[0]["fields"] if rows else {}
            previous = None
            if existing.get("内容"):
                try:
                    original = json.loads(_text(existing["内容"]))
                    if expected_previous_sha256 is not None:
                        previous_payload = self._load_entity_document(original)
                        checksum = _payload_digest(previous_payload)
                        summary_only = (kind == 'shift_person' and isinstance(previous_payload, dict)
                                        and previous_payload.get('entities') == []
                                        and all(previous_payload.get(name) == payload.get(name)
                                                for name in ('date', 'shift', 'person_id')))
                        if not summary_only and checksum not in {expected_previous_sha256, _payload_digest(payload)}:
                            raise LearningCloudConflict("云端班次明细已变化，未覆盖；请先重新备份核验")
                    previous = _document_pointer(original)
                except (ValueError, TypeError):
                    raise LearningCloudError("云端实体内容不是有效 JSON，无法确认已有文档指针") from None
            refs = [a for a in _attachment_refs(existing.get("附件") or [])
                    if not previous or a.get("file_token") != previous["file_token"]]
            refs.extend(fields.get("附件", []))
            if kind == "shift_person" and not personnel_summary or len(content) > MAX_TEXT_LENGTH:
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
                        name = f"学练明细-{sha}.json.gz" if kind == "shift_person" else f"learning-{sha}.json.gz"
                        attachment = self.upload_attachment(path, name)
                    pointer = {"schema_marker": DOCUMENT_SCHEMA, "version": 1,
                               "file_token": attachment["file_token"], "sha256": sha, "raw_bytes": len(raw)}
                self._pending_documents[(kind, key)] = pointer
                visible_pointer = pointer
                if kind == "shift_person":
                    visible_pointer = {"记录说明": f"{payload['date']} {payload['shift']} {display.get('姓名') or '人员待核对'}",
                                       "完整明细": {"附件标识": pointer["file_token"], "原始字节数": pointer["raw_bytes"],
                                                    "内容校验": pointer["sha256"], "版本": pointer["version"]}}
                content = _json_text(visible_pointer, "内容")
                refs.append({"file_token": pointer["file_token"]})
            fields["内容"] = content
            if personnel_summary:
                # The original document has been archived locally before replacing its pointer.
                refs = []
                fields["附件"] = []
            if refs or "附件" in fields or previous:
                fields["附件"] = _attachment_refs(refs)
            result = self._write_record(table, fields, operation_id, [kind, key], rows[0]["record_id"] if rows else None)
            self._pending_documents.pop((kind, key), None)
            return result

    def enrich_entity_record(self, record_id, kind, key, display_fields, *, expected_payload_sha256=None, expected_app_id=None, expected_display_name=None):
        self._require_write()
        _identifier(record_id, "record_id", r"rec[A-Za-z0-9]+")
        kind, key = _entity_kind(kind), _nonempty(key, "key")
        display = _checked_display_fields(display_fields)
        if expected_payload_sha256 is not None:
            _identifier(expected_payload_sha256, "expected_payload_sha256", r"[0-9a-f]{64}")
        with self._entity_operation(expected_app_id):
            table = self._display_table(display)
            data = self._request("GET", f"tables/{table}/records/{record_id}", params={"user_id_type": "open_id"})
            record = data.get("record")
            actual = record.get("fields") if isinstance(record, dict) else None
            if (not isinstance(actual, dict) or record.get("record_id") != record_id
                    or _entity_kind(_text(actual.get("类别"))) != kind or _text(actual.get("业务键")) != key):
                raise LearningCloudConflict("历史记录身份与类别、业务键不匹配，未补列")
            if expected_payload_sha256 is not None:
                try:
                    payload = self._load_entity_document(json.loads(_text(actual.get("内容"))))
                except (ValueError, TypeError, RecursionError):
                    raise LearningCloudError("历史记录内容无效，未补列") from None
                if _payload_digest(payload) != expected_payload_sha256:
                    raise LearningCloudConflict("历史记录内容已变化，发生冲突；请重新计算汇总，未补列")
            if expected_display_name is not None and _text(actual.get('姓名') or '') != expected_display_name:
                raise LearningCloudConflict("历史记录姓名已变化，未补列；等待重新核对")
            fields = {**display, "类别": ENTITY_KIND_LABELS.get(kind, kind)}
            if _same_fields(fields, actual):
                return record
            return self._write_record(table, fields, "enrich", [kind, key], record_id)

    def _load_entity_document(self, value):
        if isinstance(value, dict) and set(value) == {"人员班次答题情况"}:
            summary = value["人员班次答题情况"]
            names = {label: name for name, label in PERSONNEL_SUMMARY_LABELS.items()}
            if not isinstance(summary, dict) or set(summary) - names.keys():
                raise LearningCloudError("人员班次汇总字段无效，未返回部分数据")
            summary = {names[name]: item for name, item in summary.items()}
            value = {"schema_version": 1, "entities": [], "summary": summary,
                     **{name: summary.get(name) for name in ("date", "shift", "person_id")}}
            _shift_entities(value)
            return value
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

    def list_entity_records(self, *, include_fields=False):
        table = self._find_entity_table()
        if not table:
            return []
        result, seen = [], set()
        for row in self._list_all(f"tables/{table}/records"):
            fields = row.get("fields")
            if not isinstance(fields, dict):
                raise LearningCloudError("云端实体字段无效，未返回部分数据")
            kind, key = _text(fields.get("类别")), _text(fields.get("业务键"))
            kind = ENTITY_KIND_ALIASES.get(kind, kind)
            if not kind.strip() or not key.strip() or (kind, key) in seen:
                raise LearningCloudError("实体类别、业务键为空或重复，未返回部分数据")
            try:
                payload = self._load_entity_document(json.loads(_text(fields.get("内容"))))
            except (ValueError, TypeError):
                raise LearningCloudError("云端实体内容不是有效 JSON，未返回部分数据") from None
            seen.add((kind, key))
            if kind == "shift_person":
                _shift_entities(payload)
            try:
                display_name = _text(fields.get('姓名') or '')
            except LearningCloudError:
                display_name = ''
            result.append({"record_id": row["record_id"], "kind": kind, "key": key, "display_name": display_name,
                           "payload": payload, "payload_sha256": _payload_digest(payload)})
            if include_fields:
                result[-1]["fields"] = copy.deepcopy(fields)
        return result

    def load_entities(self, records=None):
        result, bundled, groups = {}, {}, set()
        for row in self.list_entity_records() if records is None else records:
            kind, key, payload = row["kind"], row["key"], row["payload"]
            if kind != "shift_person":
                result[(kind, key)] = {"kind": kind, "key": key, "payload": payload}
                continue
            group = (payload["date"], payload["shift"], payload["person_id"])
            if group in groups:
                raise LearningCloudError("同日期、班次、人员存在多个聚合，未返回部分数据")
            groups.add(group)
            for entity in _shift_entities(payload):
                identity = (entity["kind"], entity["key"])
                if identity in bundled:
                    raise LearningCloudError("不同班次人员汇总存在重复内层实体冲突，未返回部分数据")
                bundled[identity] = {**entity, "storage_owner": list(group)}
        result.update(bundled)
        return list(result.values())

    def delete_archived_entity(self, row, *, expected_app_id):
        self._require_write()
        record_id = _identifier(row.get("record_id"), "record_id", r"rec[A-Za-z0-9]+")
        if row.get("kind") in {"shift_person", "paper", "record", "sharing_attempt", "daily_progress"}:
            raise LearningCloudError("人员答题历史及班次汇总不得作为系统记录清理")
        with self._entity_operation(expected_app_id):
            table = self._find_entity_table()
            if not table:
                raise LearningCloudError("原表尚未就绪，未清理历史记录")
            data = self._request("GET", f"tables/{table}/records/{record_id}", params={"user_id_type": "open_id"})
            actual = data.get("record")
            fields = actual.get("fields") if isinstance(actual, dict) else None
            if (not isinstance(fields, dict) or actual.get("record_id") != record_id
                    or _entity_kind(_text(fields.get("类别"))) != row.get("kind")
                    or _text(fields.get("业务键")) != row.get("key")
                    or _payload_digest(fields) != _payload_digest(row.get("fields"))):
                raise LearningCloudConflict("历史记录已变化，未清理；等待重新备份核验")
            payload = self._load_entity_document(json.loads(_text(fields.get("内容"))))
            if _payload_digest(payload) != row["payload_sha256"]:
                raise LearningCloudConflict("历史记录内容已变化，未清理")
            data = self._request("DELETE", f"tables/{table}/records/{record_id}")
            if data.get("deleted") is not True or data.get("record_id") != record_id:
                raise LearningCloudWriteUncertain("历史清理结果尚未确认，保留本地备份并稍后核对")
            return data

    def upload_attachment(self, path, name):
        self._require_write()
        self._check_entity_credentials()
        path = Path(path)
        _nonempty(name, "name")
        if not path.is_file():
            raise ValueError("附件文件不存在")
        size = path.stat().st_size
        if size <= 0:
            raise ValueError("附件文件为空")
        headers = self._headers()
        self._check_entity_credentials()
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
        self._check_entity_credentials()
        return {"file_token": token, "name": name, "size": size}

    def download_attachment(self, token, max_bytes):
        self._check_entity_credentials()
        _identifier(token, "file_token")
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("max_bytes 必须是正整数")
        headers = self._headers()
        for attempt in range(2):
            self._check_entity_credentials()
            try:
                content, content_type = self._http.request_bytes(
                    "GET", f"{API_ROOT}/drive/v1/medias/{token}/download",
                    headers=headers, retries=0, max_bytes=max_bytes,
                )
                self._check_entity_credentials()
                if not content or len(content) > max_bytes:
                    raise LearningCloudError("附件为空或超过 max_bytes")
                try:
                    payload = json.loads(content) if len(content) <= 65536 else {}
                except (ValueError, UnicodeError):
                    payload = {}
                if not isinstance(payload, dict) or type(payload.get("code")) is not int or payload["code"] not in TOKEN_ERRORS:
                    if ("application/json" in content_type.lower() and isinstance(payload, dict)
                            and type(payload.get("code")) is int and payload["code"] != 0):
                        raise LearningCloudError(f"附件下载被飞书拒绝：code={payload['code']}，未将错误响应保存为文件")
                    return content
                if attempt:
                    raise LearningCloudError("附件下载授权已失效")
            except FeishuHTTPError as exc:
                response = getattr(exc.__cause__, "response", None)
                try:
                    payload = response.json() if response is not None else {}
                except (ValueError, UnicodeError):
                    payload = {}
                expired = exc.category == "token" or (isinstance(payload, dict)
                    and type(payload.get("code")) is int and payload["code"] in TOKEN_ERRORS)
                if attempt or not expired:
                    raise LearningCloudError("附件下载失败或超过 max_bytes") from None
            except (httpx.HTTPError, OSError):
                raise LearningCloudError("附件下载失败或超过 max_bytes") from None
            self._check_entity_credentials()
            headers = self._headers(force_refresh=True)

    def close(self):
        self._http.close()

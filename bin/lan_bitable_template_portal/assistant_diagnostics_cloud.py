"""One diagnostic batch per record; transport/auth remain in DocumentLibraryRemote.

The worker owns capture and durable checkpoints. This boundary revalidates every
event and its canonical JSON hash. Schema creation is explicit, not publish().
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import re
import tempfile
import threading

from .document_library_remote import (
    DocumentLibraryRemote, MAX_PAGES, _data, _error, _is_file_token, _is_record_id,
)

WIKI_TOKEN = "HScCwZt9QiqPCUkSrHjcbBL1ngb"
BASE_TOKEN = "H7DLbpdQiaIw4ssg5sCcSoGMn5M"
TABLE_ID = "tbl9QVoQjICokUQz"
FIELDS = {
    "文本": 1, "批次标识": 1, "设备标识": 1, "程序版本": 1,
    "开始时间": 5, "结束时间": 5, "错误次数": 2, "涉及模块": 1,
    "诊断编号": 1, "错误摘要": 1, "内容SHA256": 1, "日志附件": 17,
}
_EVENT_KEYS = frozenset({"module", "stage", "version", "run_id", "category", "error_type",
    "http_status", "frames", "native_reason", "native_fingerprint", "details", "count", "first_at", "last_at"})
_FORMATS = {"run_id": r"(?:[0-9a-f]{8}|[0-9a-f]{32})", "category": r"[a-z][a-z_]{0,50}",
    "error_type": r"[A-Za-z_]{1,80}", "native_fingerprint": r"[0-9a-f]{64}"}
_LABELS = {"assistant": "灯塔助手", "feishu": "飞书智能体", "startup": "程序启动",
    'schedule_execution': '定时任务执行失败', 'schedule_operation_invalid': '定时任务会话编号校验失败',
    'schedule_answer_pending': '定时任务原回答待核验',
    "answer": "回答生成", "model_attempt": "模型调用", "delivery": "消息投递", "deadline": "请求超时",
    "startup_failed": "启动失败", "service_failed": "服务失败", "runtime_auth_failed": "运行环境认证失败",
    "tools_not_registered": "查询工具未注册", "context_limit": "模型上下文超限", "output_limit": "模型输出超限",
    "message_order": "模型消息顺序异常", "model_auth": "模型认证失败", "model_busy": "模型限流",
    "model_connection": "模型连接失败", "model_timeout": "模型调用超时", "image_unsupported": "模型不支持图片",
    "request_rejected": "模型请求被拒绝", "model_not_found": "模型不存在", "model_identity": "模型身份不匹配",
    "model_invocation_expired": "模型调用已过期", "unclassified": "未分类错误",
    "gateway": "运行层详情", "model_request": "模型接口请求", "incomplete_turn": "回答未完整结束",
    "reasoning_only": "模型仅返回思考内容", "empty_response": "模型返回空内容",
    "missing_response": "缺少最终回答", "invalid_provider_response": "模型接口返回格式异常",
    "stream_interrupted": "模型传输中断", "response_limit": "模型响应超限",
    "timeout": "模型调用超时", "knowledge_text_timeout": "资料原文读取超时",
    "model_request": "模型请求被拒绝", "model_protocol": "模型返回格式异常",
    "model_output": "模型输出未完整结束"}
_DETAIL_LABELS = {'stop_reason': '结束原因', 'payloads': '输出段数', 'tools': '工具数',
    'compactions': '上下文整理次数', 'reasoning_retries': '思考续答次数', 'empty_retries': '空答重试次数',
    'missing_retries': '缺答重试次数', 'has_assistant': '存在回答', 'has_current_assistant': '当前尝试存在回答',
    'replay_safe': '可安全重试', 'request_bytes': '请求字节', 'response_bytes': '响应字节',
    'response_type': '响应类型', 'model_family': '模型系列',
    'knowledge_documents': '引用文档数', 'knowledge_chunks': '引用片段数',
    'elapsed_ms': '实际耗时毫秒', 'budget_ms': '允许耗时毫秒', 'model_attempt': '模型尝试序号'}


def _seconds(value):
    return type(value) in (int, float) and 0 <= value <= 253402300799 and math.isfinite(value)


def _event_bytes(events):
    from openclaw_service.diagnostics import MODULES, STAGES, REASONS, VERSION_PATTERN, clean_details

    if not isinstance(events, list) or not events or len(events) > 10000:
        raise _error("诊断批次须包含有界的已脱敏事件。")
    for event in events:
        if not isinstance(event, dict) or set(event) - _EVENT_KEYS:
            raise _error("诊断事件包含未授权字段，未上传。")
        if (not isinstance(event.get("module"), str) or event["module"] not in MODULES or
                not isinstance(event.get("stage"), str) or event["stage"] not in STAGES or "version" not in event):
            raise _error("诊断事件来源无效，未上传。")
        for key, pattern in {"version": VERSION_PATTERN, **_FORMATS}.items():
            if key in event and (not isinstance(event[key], str) or not re.fullmatch(pattern, event[key])):
                raise _error("诊断事件代码格式无效，未上传。")
        if "native_reason" in event and (not isinstance(event["native_reason"], str) or
                event["native_reason"] not in {*REASONS.values(), "unclassified"}):
            raise _error("诊断事件原因不是白名单代码，未上传。")
        if "http_status" in event and (type(event["http_status"]) is not int or not 100 <= event["http_status"] <= 599):
            raise _error("诊断事件状态码无效。")
        if 'details' in event and (not isinstance(event['details'], dict) or clean_details(event['details']) != event['details']):
            raise _error('诊断详情包含非白名单字段，未上传。')
        if "frames" in event and (not isinstance(event["frames"], list) or len(event["frames"]) > 8 or any(
                not isinstance(frame, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}:[A-Za-z_<>]{1,100}:[1-9]\d{0,7}", frame)
                for frame in event["frames"])):
            raise _error("诊断事件代码位置无效，未上传。")
        if "count" in event and (type(event["count"]) is not int or not 1 <= event["count"] <= 2**53 - 1):
            raise _error("诊断事件次数无效。")
        if any(key in event and not _seconds(event[key]) for key in ("first_at", "last_at")):
            raise _error("诊断事件时间无效。")
        if "first_at" in event and "last_at" in event and event["last_at"] < event["first_at"]:
            raise _error("诊断事件时间范围无效。")
    return json.dumps(events, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _batch_key(batch_id, payload_hash):
    if not isinstance(batch_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", batch_id):
        raise _error("诊断批次标识无效。")
    if not isinstance(payload_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", payload_hash):
        raise _error("诊断批次内容校验值无效。")


def _cell(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list) and all(isinstance(part, dict) and isinstance(part.get("text"), str) for part in value):
        return "".join(part["text"] for part in value)
    raise _error("诊断记录字段响应不完整。", 502)


class _DiagnosticTable(DocumentLibraryRemote):
    def __init__(self, source):
        super().__init__(source)
        self._uncertain_fields = set()

    def _schema(self):
        fields = {}
        for field in self.fields():
            name, kind = field.get("field_name"), field.get("type")
            if not isinstance(name, str) or not name or name in fields or type(kind) is not int:
                raise _error("诊断表字段响应无效。", 502)
            if name in FIELDS and kind != FIELDS[name]:
                raise _error(f"诊断表字段类型不符：{name}。", 409)
            fields[name] = kind
        if fields.get("文本") != 1:
            raise _error("诊断表须保留现有文本列。", 409)
        return fields

    def ensure_fields(self):
        manager, identity = self._capture()
        fields, created = self._schema(), []
        for name, kind in FIELDS.items():
            self._check(manager, identity)
            if name in fields:
                self._uncertain_fields.discard(name)
                continue
            if name in self._uncertain_fields:
                raise _error(f"诊断字段创建结果仍未确认：{name}，请先核验原请求。", 409)
            # A lost response is resolved by reading, not by another field POST.
            self._uncertain_fields.add(name)
            body = {"field_name": name, "type": kind}
            if kind == 5:
                body["property"] = {"auto_fill": False, "date_formatter": "yyyy/MM/dd HH:mm"}
            try:
                self._parse(self._call("POST", self._records_url + "/fields",
                    body=body, retries=0, operation="诊断字段创建"))
            except Exception:
                self._check(manager, identity)
                fields = self._schema()
                if name not in fields:
                    raise _error(f"诊断字段创建结果未确认：{name}，未自动重建。", 503) from None
            else:
                fields = self._schema()
            self._check(manager, identity)
            if name not in fields:
                raise _error(f"诊断字段回读缺失：{name}，未自动重建。", 503)
            self._uncertain_fields.discard(name)
            created.append(name)
        return {"created_fields": created, "field_count": len(fields)}

    def find_batch(self, batch_id, payload_hash):
        _batch_key(batch_id, payload_hash)
        manager, identity = self._capture()
        cursor, seen, found = "", set(), None
        refreshed = False
        body = {"field_names": ["批次标识", "内容SHA256"], "filter": {
            "conjunction": "and", "conditions": [
                {"field_name": "批次标识", "operator": "is", "value": [batch_id]}]}}
        for _ in range(MAX_PAGES):
            self._check(manager, identity)
            def search():
                return _data(self._parse(self._call("POST", self._records_url + "/records/search",
                    params={"page_size": 100, **({"page_token": cursor} if cursor else {})},
                    body=body, retries=0, operation="诊断批次查重")))
            try:
                data = search()
            except Exception as exc:
                if refreshed or getattr(exc, "category", "") != "token_rejected":
                    raise
                self._check(manager, identity)
                refreshed = True
                self._token(manager, identity, force_refresh=True)
                data = search()
            self._check(manager, identity)
            rows = data.get("items")
            if rows is None and data.get("has_more") is False and type(data.get("total")) is int and data["total"] == 0:
                rows = []
            if not isinstance(rows, list) or type(data.get("has_more")) is not bool:
                raise _error("诊断批次查询响应不完整，未新增记录。", 502)
            for row in rows:
                if not isinstance(row, dict) or not _is_record_id(row.get("record_id")) or not isinstance(row.get("fields"), dict):
                    raise _error("诊断批次查询记录不完整。", 502)
                fields = row["fields"]
                if _cell(fields.get("批次标识")) != batch_id:
                    raise _error("诊断批次查询范围不符，未新增记录。", 502)
                if _cell(fields.get("内容SHA256")) != payload_hash:
                    raise _error("同一诊断批次内容冲突，未覆盖或新增。", 409)
                if found is not None:
                    raise _error("发现重复诊断批次记录，需人工核验。", 409)
                found = row["record_id"]
            if not data["has_more"]:
                return found
            cursor = data.get("page_token")
            if not isinstance(cursor, str) or not cursor or len(cursor) > 512 or cursor in seen:
                raise _error("诊断批次查询分页无效，未新增记录。", 502)
            seen.add(cursor)
        raise _error("诊断批次查询未到末页，未新增记录。", 502)


class DiagnosticRemote:
    """Lazy connection to the single user-authorized table, with no settings writes."""

    def __init__(self, source=None):
        if source is not None and (not isinstance(source, dict) or
                source.get("base_token") != BASE_TOKEN or source.get("table_id") != TABLE_ID):
            raise _error("诊断上报仅允许已授权的灯塔日志表。", 403)
        self._remote = None
        self._closed = False
        self._lock = threading.RLock()

    def _table(self):
        if self._closed:
            raise _error("诊断上报连接已关闭。", 409)
        if self._remote is None:
            self._remote = _DiagnosticTable({"base_token": BASE_TOKEN, "table_id": TABLE_ID})
        return self._remote

    def ensure_fields(self):
        with self._lock:
            return self._table().ensure_fields()

    def find_batch(self, batch_id, payload_hash):
        with self._lock:
            return self._table().find_batch(batch_id, payload_hash)

    def publish(self, batch, checkpoint):
        """Return record_id; checkpoint(file_token=..., auth_fingerprint=...) must persist atomically.

        Only allowlisted events enter the attachment. The frozen envelope hash
        uses diagnostic_store.batch_digest, not an independent transport hash.
        """
        if not isinstance(batch, dict) or not callable(checkpoint):
            raise _error("诊断批次或持久化回调无效。")
        batch = deepcopy(batch)
        _batch_key(batch.get("id"), batch.get("payload_hash"))
        events = batch.get("events")
        content = _event_bytes(events)
        if type(batch.get("error_count")) is not int or batch["error_count"] != sum(event.get("count", 1) for event in events):
            raise _error("诊断批次错误次数无效。")
        dates = [batch.get(key) for key in ("started_at", "ended_at")]
        if any(not _seconds(value) for value in dates) or dates[1] < dates[0]:
            raise _error("诊断批次时间无效，须为 Unix 秒。")
        if not isinstance(batch.get("machine_id"), str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", batch["machine_id"]):
            raise _error("诊断批次设备标识无效。")
        from openclaw_service.diagnostic_store import batch_digest
        if batch_digest(batch) != batch["payload_hash"]:
            raise _error("诊断批次内容校验不符，未上传。", 409)

        def joined(key, limit=2000):
            values = [event.get(key, "") for event in events]
            return "\n".join(dict.fromkeys(value[:8] if key == "run_id" else
                _LABELS.get(value, value) if key == "module" else value for value in values if value))[:limit]

        with self._lock:
            remote = self._table()
            manager, identity = remote._capture()
            fingerprint = hashlib.sha256(json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
            existing = remote.find_batch(batch["id"], batch["payload_hash"])
            remote._check(manager, identity)
            if existing:
                return existing
            token = batch.get("file_token")
            if not _is_file_token(token) or batch.get("auth_fingerprint") != fingerprint:
                with tempfile.TemporaryDirectory(prefix="assistant-diagnostics-") as folder:
                    path = Path(folder) / "diagnostics.json"
                    path.write_bytes(content)
                    token = remote.upload(path, "diagnostics.json")["file_token"]
                remote._check(manager, identity)
                checkpoint(file_token=token, auth_fingerprint=fingerprint)
            remote._check(manager, identity)
            fields = {"文本": f"灯塔诊断 {batch['id']} · {batch['error_count']} 次",
                "批次标识": batch["id"], "设备标识": batch["machine_id"],
                "程序版本": joined("version", 300),
                "开始时间": int(dates[0] * 1000), "结束时间": int(dates[1] * 1000),
                "错误次数": batch["error_count"], "涉及模块": joined("module"),
                "诊断编号": joined("run_id"), "错误摘要": "\n".join(dict.fromkeys(
                    " / ".join(_LABELS.get(event[key], event[key]) for key in ("native_reason", "category", "stage", "error_type") if event.get(key))
                    + ('；' + '，'.join(_DETAIL_LABELS[key] + '=' + str(value) for key, value in event['details'].items()) if event.get('details') else '')
                    for event in events))[:4000],
                "内容SHA256": batch["payload_hash"], "日志附件": [{"file_token": token}]}
            record = remote.create(fields, batch["id"])
            remote._check(manager, identity)
            return record["record_id"]

    def close(self):
        with self._lock:
            self._closed = True
            if self._remote is not None:
                self._remote.close()

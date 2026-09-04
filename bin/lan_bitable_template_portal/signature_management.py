"""Shared signature directory, one-use collection requests and reviewed merges.

Personnel and encrypted signatures remain in the two Bitable tables. Local
documents only journal requests/merges; a delivery retry never creates a person.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import io
import json
import re
import threading
import time
from contextlib import ExitStack
from urllib.parse import urlencode

from PIL import Image

ORIGIN_FIELD = "来源正式人员ID"
ALIASES_FIELD = "历史临时人员ID"
REQUEST_NS = "signature_management_request"
PERSON_NS = "signature_management_person"
MERGE_NS = "signature_management_merge"
MIGRATION_NS = "signature_management_migration"


class SignatureManagementError(RuntimeError):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def _hash(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def _text(value, limit=200) -> str:
    text = str(value or "").strip()
    if len(text) > limit:
        raise SignatureManagementError("填写内容过长。")
    return text


def _id(value) -> str:
    value = _text(value, 128)
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value):
        raise SignatureManagementError("记录或操作编号无效。")
    return value


def public_person(row: dict, source: str) -> dict:
    person = {k: copy.deepcopy(v) for k, v in row.items()
              if k not in {"raw_fields", "signature_file_token", "signature_preview_url", "signature_version"}}
    person["source"] = source
    person["person_key"] = f"{source}:{person['record_id']}"
    person["signature_status"] = ("signed" if person.get("has_signature") else
                                  "resign" if person.get("signature_count") or person.get("signature_requires_resign") else "unsigned")
    person["signature_reason"] = ("已有附件，但缺少可用加密信息，请重新签名。"
                                  if person["signature_status"] == "resign" else "")
    return person


def resolve_directory(staff: list[dict], external: list[dict]) -> tuple[list[dict], dict[str, dict]]:
    """No name-based identity merge. Explicit origins and aliases are authoritative."""
    staff_by_id = {str(p["record_id"]): public_person(p, "staff") for p in staff}
    external_by_id = {str(p["record_id"]): public_person(p, "external") for p in external}
    origins: dict[str, list[dict]] = {}
    aliases: dict[str, list[dict]] = {}
    for person in external_by_id.values():
        for old_id in person.get("historical_record_ids") or []:
            aliases.setdefault(str(old_id), []).append(person)
    # A verified merge mapping wins even while physical deletion is being retried.
    hidden = {rid for rid, rows in aliases.items() if len(rows) == 1 and rid != rows[0]["record_id"]}
    for rid, person in external_by_id.items():
        origin = str(person.get("origin_staff_record_id") or "")
        if origin and rid not in hidden:
            origins.setdefault(origin, []).append(person)
    result, resolved = [], {}
    selected_external = set()
    for rid, person in staff_by_id.items():
        choices = origins.get(rid, [])
        effective = person
        if not person.get("has_signature") and len(choices) == 1 and choices[0].get("has_signature"):
            effective = dict(choices[0])
            effective["name"] = person.get("name") or effective.get("name")
            effective["building"] = person.get("building") or effective.get("building")
            effective["employee_no"] = person.get("employee_no") or effective.get("employee_no")
        if len(choices) == 1:
            selected_external.add(choices[0]["record_id"])
            resolved[f"external:{choices[0]['record_id']}"] = effective
        elif len(choices) > 1:
            effective = dict(effective, identity_warning="关联了多个临时记录，请管理员核对。")
        resolved[f"staff:{rid}"] = effective
        result.append(effective)
    for rid, person in external_by_id.items():
        if rid in hidden:
            continue
        if rid not in selected_external:
            result.append(person)
            resolved[f"external:{rid}"] = person
    for old_id, rows in aliases.items():
        if len(rows) == 1 and old_id != rows[0]["record_id"]:
            resolved[f"external:{old_id}"] = resolved.get(f"external:{rows[0]['record_id']}", rows[0])
    for person in result:
        person["record_aliases"] = [key for key, target in resolved.items() if target["person_key"] == person["person_key"]]
    return result, resolved


class SignatureManagement:
    def __init__(self, service):
        from . import portal_service
        self.s = service
        self.d = portal_service
        self._locks_guard = threading.Lock()
        self._locks: dict[str, threading.RLock] = {}
        self._refresh_lock = threading.RLock()
        self._last_refresh = 0.0
        self._refresh_errors: dict[str, str] = {}
        self._schema_ready = False
        self._temporary_field_metas = {}

    @property
    def store(self):
        return self.s._state_store

    def lock(self, key: str):
        with self._locks_guard:
            return self._locks.setdefault(key, threading.RLock())

    def table(self, source: str) -> str:
        if source not in {"staff", "external"}:
            raise SignatureManagementError("人员来源无效。")
        return self.d.SIGNATURE_TABLE_ID if source == "staff" else self.d.TEMP_SIGNATURE_TABLE_ID

    def invalidate(self):
        with self.s._signature_people_cache_lock:
            self.s._signature_people_cache = None
        with self.s._external_signature_people_cache_lock:
            self.s._external_signature_people_cache = None
        self._last_refresh = 0.0

    def directory(self, refresh=False) -> dict:
        requested_at = time.monotonic()
        with self._refresh_lock:
            # Coalesce overlapping explicit refreshes; ordinary searches use existing TTL.
            force = bool(refresh and requested_at > self._last_refresh)
            rows, sources = {}, {}
            for source, loader, cache_name in (
                ("staff", self.s._load_signature_people, "_signature_people_cache"),
                ("external", self.s._load_external_signature_people, "_external_signature_people_cache"),
            ):
                error = ""
                try:
                    rows[source] = loader(force=force)
                    if force:
                        self._refresh_errors.pop(source, None)
                    error = self._refresh_errors.get(source, "")
                except Exception as exc:
                    error = str(exc)
                    self._refresh_errors[source] = error
                    rows[source] = copy.deepcopy((getattr(self.s, cache_name, None) or {}).get("people") or [])
                cache = getattr(self.s, cache_name, None) or {}
                sources[source] = {"ok": not error, "error": error, "loaded_at": float(cache.get("loaded_ts") or 0)}
            if force:
                self._last_refresh = time.monotonic()
            people, resolved = resolve_directory(rows["staff"], rows["external"])
            return {"people": people, "resolved": resolved, "staff": rows["staff"], "external": rows["external"],
                    "sources": sources, "revision": _hash([(k, v) for k, v in sources.items()])}

    def _require_directory(self) -> dict:
        data = self.directory(refresh=True)
        if any(not info["ok"] for info in data["sources"].values()):
            raise SignatureManagementError("人员表读取失败，请刷新后重试。", 503)
        return data

    def person(self, source, record_id, *, refresh=False, resolve=False) -> dict:
        source, record_id = _text(source), _id(record_id)
        self.table(source)
        data = self.directory(refresh=refresh)
        if not data["sources"][source]["ok"]:
            raise SignatureManagementError("人员表读取失败，不能核验签名。", 503)
        if resolve and f"{source}:{record_id}" in data["resolved"]:
            return data["resolved"][f"{source}:{record_id}"]
        for row in data[source]:
            if str(row.get("record_id")) == record_id:
                return public_person(row, source)
        raise SignatureManagementError("人员记录不存在，请刷新。", 404)

    def _fields(self, source, record_id) -> dict:
        try:
            payload = self.s._request_json(f"records/{_id(record_id)}", app_token=self.d.SIGNATURE_APP_TOKEN,
                                           table_id=self.table(source))
        except Exception as exc:
            if any(x in str(exc) for x in ("RecordIdNotFound", "1254043", "未找到记录", "记录不存在")):
                raise SignatureManagementError("人员记录不存在或已合并，请重新发送签名链接。", 404) from exc
            raise SignatureManagementError("人员记录读取失败，请稍后重试。", 503) from exc
        data = payload.get("data") or {}
        record = data.get("record") or data
        if not isinstance(record.get("fields"), dict):
            raise SignatureManagementError("人员记录响应不完整，无法核验签名。", 502)
        return record["fields"]

    def _version(self, fields):
        attachments = self.s._extract_signature_attachments(fields)
        return _hash([[a.get("file_token") or a.get("token") or a.get("name") for a in attachments],
                      self.s._signature_crypto.metadata_from_field(fields.get(self.d.SIGNATURE_KEY_FIELD))])

    def _identity_version(self, fields):
        return _hash([self.s._mop_field_text(fields, [name]) for name in
                      ("姓名", "员工姓名", "员工工号", "楼栋", ORIGIN_FIELD)])

    def ensure_fields(self):
        with self.lock("schema"):
            if self._schema_ready:
                return self._temporary_field_metas
            _, metas = self.s._load_table_fields(
                app_token=self.d.SIGNATURE_APP_TOKEN,
                table_id=self.d.TEMP_SIGNATURE_TABLE_ID,
            )
            created = False
            for name in (ORIGIN_FIELD, ALIASES_FIELD):
                if metas.get(name) is None:
                    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.d.SIGNATURE_APP_TOKEN}/tables/{self.d.TEMP_SIGNATURE_TABLE_ID}/fields"
                    result = self.s._request_payload("POST", url, context="签名关联字段创建",
                        headers={**self.s._auth_headers(), "Content-Type": "application/json"},
                        json_payload={"field_name": name, "type": 1}, http_client=self.s._write_http_client)
                    if int(result.get("code") or 0) in self.d.TOKEN_ERROR_CODES:
                        self.d.refresh_feishu_token()
                        result = self.s._request_payload("POST", url, context="签名关联字段创建重试",
                            headers={**self.s._auth_headers(), "Content-Type": "application/json"},
                            json_payload={"field_name": name, "type": 1}, http_client=self.s._write_http_client)
                    if int(result.get("code") or 0) not in {0, 1254014}:
                        raise SignatureManagementError(f"创建签名关联字段失败：{name}。")
                    created = True
            if created:
                _, metas = self.s._load_table_fields(
                    app_token=self.d.SIGNATURE_APP_TOKEN,
                    table_id=self.d.TEMP_SIGNATURE_TABLE_ID,
                )
            for name in (ORIGIN_FIELD, ALIASES_FIELD):
                meta = metas.get(name)
                if meta is None or int(meta.field_type or 0) != 1 or bool(meta.has_formula):
                    raise SignatureManagementError(f"临时人员表的“{name}”必须是可写文本字段。")
            self._temporary_field_metas = metas
            self._schema_ready = True
            return metas

    def people(self, payload) -> dict:
        data = self.directory(refresh=bool(payload.get("refresh")))
        source, status, scope = _text(payload.get("source")), _text(payload.get("status")), _text(payload.get("scope")).upper()
        message_capable = str(payload.get("message_capable") or "").strip().lower() in {"1", "true", "yes"}
        query = _text(payload.get("q"))
        query_terms = [
            term.casefold()
            for term in re.findall(r"[\w\u4e00-\u9fff]+", query, flags=re.UNICODE)
            if term
        ]
        visible_keys = {person["person_key"] for person in data["people"]}
        rows = [
            public_person(person, source)
            for source in ("staff", "external")
            for person in data[source]
            if f"{source}:{person.get('record_id')}" in visible_keys
        ]
        raw_by_key = {
            f"{source}:{person.get('record_id')}": person
            for source in ("staff", "external")
            for person in data[source]
        }
        latest = {r["key"]: r["payload"] for r in self.store.list_documents(PERSON_NS)}
        request_by_id = {r["key"]: r["payload"] for r in self.store.list_documents(REQUEST_NS)}
        for row in rows:
            effective = data["resolved"].get(row["person_key"], row)
            row["effective_person_key"] = effective["person_key"]
            row["effective_source"] = effective["source"]
            row["effective_has_signature"] = effective.get("has_signature", False)
            row["identity_version"] = self._identity_version(
                (raw_by_key.get(row["person_key"]) or {}).get("raw_fields") or {}
            )
            request_id = (latest.get(row["person_key"]) or {}).get("request_id")
            request = request_by_id.get(request_id) if request_id else None
            row["request"] = self._public_request(request) if request else None
        counts = {state: sum(p["signature_status"] == state for p in rows) for state in ("signed", "unsigned", "resign")}
        rows = [p for p in rows if
                (not source or source == "all" or p["source"] == source) and
                (not status or status == "all" or p["signature_status"] == status) and
                (not message_capable or (p["source"] == "staff" and p.get("can_receive_message"))) and
                (not scope or scope == "ALL" or scope in self.s._building_codes_from_value(p.get("building"))) and
                (not query_terms or all(
                    term in re.sub(
                        r"\s+", "",
                        " ".join(str(p.get(k) or "") for k in (
                            "record_id", "name", "employee_no", "building",
                            "specialty", "position", "team", "shift", "account_nature",
                        )),
                    ).casefold()
                    for term in query_terms
                ))]
        rows.sort(key=lambda p: (p.get("building") or "", p.get("name") or "", p["person_key"]))
        page = max(1, min(10000, int(payload.get("page") or 1)))
        page_size = max(1, min(500, int(payload.get("page_size") or 50)))
        return {"people": rows[(page - 1) * page_size:page * page_size], "count": len(rows), "page": page,
                "page_size": page_size, "counts": counts, "sources": data["sources"],
                "recipients": [
                    {"value": b, "label": "110站值班账号" if b == "110" else f"{b}楼值班账号"}
                    for b in ("A", "B", "C", "D", "E", "H", "110")
                ]}

    def refresh(self) -> dict:
        data = self.directory(refresh=True)
        return {k: data[k] for k in ("people", "resolved", "sources", "revision")}

    def references(self, references):
        data = self.directory()
        result, seen = [], set()
        for reference in references or []:
            item = dict(reference)
            source = str(item.get("source") or "staff")
            # Old task-bound temporary sessions still pass their original scope/owner checks.
            if source in {"staff", "external"} and not item.get("temp_id"):
                person = data["resolved"].get(f"{source}:{item.get('record_id')}")
                if person:
                    item.update(source=person["source"], record_id=person["record_id"])
            key = (item.get("role"), item.get("source"), item.get("temp_id") or item.get("record_id"))
            if key not in seen:
                result.append(item)
                seen.add(key)
        return result

    def _new_temporary(self, details: dict, operation_id: str, actor: str, origin="") -> dict:
        metas = self.ensure_fields()
        name, building = _text(details.get("name"), 80), _text(details.get("building"), 80)
        employee_no, specialty = _text(details.get("employee_no"), 80), _text(details.get("specialty"), 80)
        if not name or not building:
            raise SignatureManagementError("姓名和楼栋必填。")
        if not self.s._building_codes_from_value(building):
            raise SignatureManagementError("请选择有效楼栋。")
        key = f"create:{operation_id}"
        with self.lock("create:" + (origin or _hash([name, building, employee_no]))):
            prior = self.store.get_document(REQUEST_NS, key)
            fingerprint = _hash([name, building, employee_no, specialty, origin, actor])
            if prior and prior.get("fingerprint") != fingerprint:
                raise SignatureManagementError("操作编号已被其他请求使用。", 409)
            if prior and prior.get("record_id"):
                return self.person("external", prior["record_id"], refresh=True)
            external_people = self.s._load_external_signature_people(force=True)
            candidates = [p for p in external_people if origin and p.get("origin_staff_record_id") == origin]
            if len(candidates) > 1:
                raise SignatureManagementError("该正式人员已关联多个临时记录，请管理员先核对。", 409)
            if len(candidates) == 1:
                return public_person(candidates[0], "external")
            if prior:
                raise SignatureManagementError("上次创建结果待核验，请管理员确认临时表中的记录，勿重复新建。", 409)
            matches = [p for p in external_people if p.get("name") == name and
                       ((employee_no and p.get("employee_no") == employee_no) or
                        (not employee_no and p.get("building") == building))]
            if matches:
                raise SignatureManagementError("临时表已有同名或同工号记录，请先选择已有人员；需要关联时由管理员核对。", 409)
            fields = {self.d.TEMP_SIGNATURE_NAME_FIELD: name, ORIGIN_FIELD: origin}
            values = self.s._field_option_write_values(metas, self.d.TEMP_SIGNATURE_BUILDING_FIELD, [building])
            fields[self.d.TEMP_SIGNATURE_BUILDING_FIELD] = values if values else building
            if employee_no:
                fields[self.d.TEMP_SIGNATURE_EMPLOYEE_NO_FIELD] = employee_no
            if specialty:
                values = self.s._field_option_write_values(metas, self.d.TEMP_SIGNATURE_SPECIALTY_FIELD, [specialty])
                fields[self.d.TEMP_SIGNATURE_SPECIALTY_FIELD] = values if values else specialty
            journal = {"status": "creating", "fingerprint": fingerprint, "created_by": actor, "created_at": time.time()}
            self.store.put_document(REQUEST_NS, key, journal)
            result = self.s._create_record_fields(app_token=self.d.SIGNATURE_APP_TOKEN, table_id=self.d.TEMP_SIGNATURE_TABLE_ID, fields=fields)
            result = result.get("data") or {}
            rid = (result.get("record") or result).get("record_id")
            if not rid:
                raise SignatureManagementError("临时人员创建结果待核验，未返回记录ID。", 502)
            journal.update(status="created", record_id=rid)
            self.store.put_document(REQUEST_NS, key, journal)
            person = {
                "source": "external", "record_id": rid, "name": name,
                "display_name": name, "building": building, "scope_text": building,
                "specialty": specialty, "employee_no": employee_no, "certificate": "",
                "has_signature": False, "signature_count": 0, "signature_version": "",
                "signature_crypto_version": 0, "portable_signature": False,
                "signature_requires_resign": False, "origin_staff_record_id": origin,
                "historical_record_ids": [], "latest_publish_time": 0, "raw_fields": fields,
            }
            with self.s._external_signature_people_cache_lock:
                self.s._external_signature_people_cache = {
                    "loaded_ts": time.time(),
                    "people": copy.deepcopy([
                        item for item in external_people
                        if str(item.get("record_id") or "") != rid
                    ] + [person]),
                }
            return public_person(person, "external")

    def _public_request(self, request):
        public = {k: request.get(k) for k in ("request_id", "person_key", "name", "building", "status", "recipient_label",
                  "expires_at", "created_at", "saved_at", "error", "failure_kind", "destination")}
        if public["status"] not in {"completed", "revoked"} and float(public.get("expires_at") or 0) < time.time():
            public["status"] = "expired"
        return public

    def send(self, payload, *, actor: str, base_url: str, send_text) -> dict:
        operation_id = _id(payload.get("operation_id"))
        source, rid = _text(payload.get("source")), _id(payload.get("record_id"))
        recipient = _text(payload.get("recipient"))
        fingerprint = _hash([source, rid, recipient, actor])
        with ExitStack() as locks:
            locks.enter_context(self.lock(f"{source}:{rid}"))
            prior = self.store.get_document(REQUEST_NS, operation_id)
            if prior:
                if prior.get("fingerprint") != fingerprint:
                    raise SignatureManagementError("操作编号已被使用。", 409)
                return self._public_request(prior)
            person = self.person(source, rid)
            if source == "external":
                real = self.directory()["resolved"].get(f"external:{rid}", person)
                if rid in (real.get("historical_record_ids") or []):
                    raise SignatureManagementError("该人员已合并，请刷新后选择保留记录。", 409)
            target = person
            locks.enter_context(self.lock(target["person_key"]))
            normalized_recipient = recipient.strip()
            if normalized_recipient.lower().startswith("staff:"):
                relay = self.person("staff", _id(normalized_recipient.split(":", 1)[1]))
                if not relay.get("can_receive_message"):
                    raise SignatureManagementError("所选代收人员不是VNET账号或缺少openid。")
                open_id = str(relay.get("open_id") or "").strip()
                recipient_label = f"{relay.get('name') or 'VNET人员'}代收"
            elif normalized_recipient:
                duty_code = normalized_recipient.split(":", 1)[-1].upper()
                if duty_code not in set(self.d.BUILDING_OPEN_ID_MAP):
                    raise SignatureManagementError("请选择VNET人员或A–E、H、110站值班账号代收。")
                open_id = self.d.BUILDING_OPEN_ID_MAP.get(duty_code)
                recipient_label = "110站值班账号" if duty_code == "110" else f"{duty_code}楼值班账号"
            else:
                open_id = (
                    person.get("open_id")
                    if source == "staff" and person.get("can_receive_message")
                    else ""
                )
                recipient_label = person.get("name")
            if not open_id:
                return {"status": "recipient_required", "failure_kind": "bot_unavailable",
                        "error": "该人员的账号性质不是VNET，不能直接发送；请选择VNET人员或值班账号代收。"}
            fields = self._fields(target["source"], target["record_id"])
            person_key = target["person_key"]
            for key in {person["person_key"], person_key}:
                old = self.store.get_document(PERSON_NS, key) or {}
                if old.get("request_id"):
                    old_request = self.store.get_document(REQUEST_NS, old["request_id"])
                    if old_request and old_request.get("status") != "completed":
                        old_request["status"] = "revoked"
                        self.store.put_document(REQUEST_NS, old["request_id"], old_request)
            token = self.store.create_signature_link_token(record_id=operation_id, created_by=actor, ttl_seconds=86400,
                                                           payload={"source": target["source"], "record_id": target["record_id"]})
            request = {"request_id": operation_id, "fingerprint": fingerprint, "person_key": person_key,
                       "source": target["source"], "record_id": target["record_id"], "name": target["name"],
                       "building": target.get("building") or "", "person": target, "created_by": actor,
                       "token_hash": hashlib.sha256(token["token"].encode()).hexdigest(), "expires_at": token["expires_at"],
                       "created_at": time.time(), "base_version": self._version(fields), "status": "sending",
                       "identity_version": self._identity_version(fields),
                       "recipient_label": recipient_label, "destination": "正式人员表" if target["source"] == "staff" else "临时人员表"}
            self.store.put_document(REQUEST_NS, operation_id, request)
            for key in {person["person_key"], person_key}:
                self.store.put_document(PERSON_NS, key, {"request_id": operation_id})
            public_base = self.s._signature_public_base_url(scope=target.get("building") or "ALL", request_base_url=base_url)
            link = public_base + "/signature?" + urlencode({"request_id": operation_id}) + "#" + urlencode({"token": token["token"]})
            message = "\n".join(["【人员签名】请完成手写签名", f"签名人员：{target['name']}",
                f"楼栋：{target.get('building') or '未填写'}", f"接收：{recipient_label}",
                "请交由本人完成签名。链接有效期24小时，保存一次后失效。", "请连接本单位局域网后打开：", link])
            try:
                ok, error, results = send_text(message, [open_id])
                request.update(status="sent" if ok else "send_failed", error="" if ok else str(error),
                               failure_kind=next((r.get("failure_kind") for r in (results or []) if r.get("failure_kind")), ""))
            except Exception:
                request.update(status="send_unknown", error="消息发送结果未确认，请核对接收情况后重新发送。")
            self.store.put_document(REQUEST_NS, operation_id, request)
            return self._public_request(request)

    def _request(self, request_id, token, *, allow_completed=False) -> dict:
        request_id = _id(request_id)
        request = self.store.get_document(REQUEST_NS, request_id)
        if not request or not token or not hmac.compare_digest(str(request.get("token_hash") or ""), hashlib.sha256(str(token).encode()).hexdigest()):
            raise SignatureManagementError("签名链接无效。", 403)
        if request.get("status") == "revoked" or float(request.get("expires_at") or 0) < time.time():
            raise SignatureManagementError("签名链接已失效或过期，请从指纹入口重新发送。", 403)
        if request.get("status") == "completed" and not allow_completed:
            raise SignatureManagementError("签名已完成，此链接已失效。", 410)
        return request

    def session(self, payload):
        request = self._request(payload.get("request_id"), payload.get("token"))
        return self._public_request(request)

    def _verified(self, request, fields) -> bool:
        write = request.get("write") or {}
        tokens = [a.get("file_token") or a.get("token") for a in self.s._extract_signature_attachments(fields)]
        metadata = self.s._signature_crypto.metadata_from_field(fields.get(self.d.SIGNATURE_KEY_FIELD))
        return bool(write.get("file_token") and tokens == [write["file_token"]] and metadata == write.get("metadata"))

    def _complete(self, request):
        request.update(status="completed", saved_at=time.time(), error="")
        self.store.put_document(REQUEST_NS, request["request_id"], request)
        self.invalidate()
        return self._public_request(request)

    def submit(self, payload):
        request = self._request(payload.get("request_id"), payload.get("token"), allow_completed=True)
        if payload.get("confirmed_self") is not True:
            raise SignatureManagementError("请确认由所示人员本人完成签名。")
        with self.lock(request["person_key"]):
            request = self._request(payload.get("request_id"), payload.get("token"), allow_completed=True)
            if request["status"] == "completed":
                return self._public_request(request)
            fields = self._fields(request["source"], request["record_id"])
            if self._verified(request, fields):
                return self._complete(request)
            # Missing/moved original records never redirect a write through a historical ID.
            data = self._require_directory()
            if any(request["record_id"] in (p.get("historical_record_ids") or []) for p in data["external"]):
                raise SignatureManagementError("人员已合并，请重新发送签名链接。", 409)
            if self._version(fields) != request["base_version"]:
                raise SignatureManagementError("签名已被其他请求更新，请重新发送签名链接。", 409)
            if self._identity_version(fields) != request.get("identity_version"):
                raise SignatureManagementError("人员资料或关联已变化，请重新发送签名链接。", 409)
            raw = self.s._decode_signature_png(str(payload.get("signature_png") or ""))
            with Image.open(io.BytesIO(raw)) as original:
                if max(original.size) > 4096:
                    raise SignatureManagementError("签名图片尺寸过大。")
            content = self.s._transparent_signature_png(raw)
            with Image.open(io.BytesIO(content)) as image:
                rgba = image.convert("RGBA")
                if not rgba.getchannel("A").getbbox() or max(rgba.size) > 4096:
                    raise SignatureManagementError("请手写有效签名，不能提交空白或超大图片。")
            digest = hashlib.sha256(content).hexdigest()
            if request.get("write") and request["write"].get("sha256") != digest:
                raise SignatureManagementError("上次签名保存结果待核验，请重试原签名或重新发送链接。", 409)
            def before_write(file_token, metadata):
                request["write"] = {"file_token": file_token, "metadata": metadata, "sha256": digest}
                request["status"] = "saving"
                self.store.put_document(REQUEST_NS, request["request_id"], request)
            write = request.get("write")
            try:
                if write:
                    self.s._patch_record_fields(app_token=self.d.SIGNATURE_APP_TOKEN, table_id=self.table(request["source"]),
                        record_id=request["record_id"], fields={self.d.SIGNATURE_ATTACHMENT_FIELD: [{"file_token": write["file_token"]}],
                        self.d.SIGNATURE_KEY_FIELD: self.s._signature_crypto.metadata_to_text(write["metadata"])})
                else:
                    self.s._save_encrypted_signature_record(table_id=self.table(request["source"]), record_id=request["record_id"],
                        attachment_field=self.d.SIGNATURE_ATTACHMENT_FIELD, key_field=self.d.SIGNATURE_KEY_FIELD,
                        signature_bytes=content, display_name=request["name"], source=request["source"],
                        open_id=request["person"].get("open_id") or "", employee_no=request["person"].get("employee_no") or "",
                        before_write=before_write)
            except Exception:
                if self._verified(request, self._fields(request["source"], request["record_id"])):
                    return self._complete(request)
                raise
            if not self._verified(request, self._fields(request["source"], request["record_id"])):
                raise SignatureManagementError("签名写入后回读校验未通过，请保留画板并重试。", 502)
            return self._complete(request)

    def duplicates(self):
        data = self._require_directory()
        groups = {}
        for row in data["external"]:
            key = re.sub(r"\s+", "", str(row.get("name") or "")).casefold()
            groups.setdefault(key, []).append(public_person(row, "external"))
        return {"groups": [{"name": rows[0]["name"], "people": rows, "version": _hash(rows)}
                           for rows in groups.values() if len(rows) > 1]}

    def associate(self, payload):
        self.ensure_fields()
        staff_id = _id(payload.get("staff_record_id"))
        temporary_id = _id(payload.get("record_id"))
        if payload.get("confirmed_same_person") is not True:
            raise SignatureManagementError("请核对姓名、工号和楼栋，确认是同一个人。")
        # The chooser already loaded the directory. Point reads below are the
        # authoritative check; reloading both whole tables twice only causes timeouts.
        data = self.directory()
        staff = next((p for p in data["staff"] if str(p.get("record_id")) == staff_id), None)
        temporary = next((p for p in data["external"] if str(p.get("record_id")) == temporary_id), None)
        if not staff or not temporary:
            raise SignatureManagementError("人员记录不存在，请刷新后重新选择。", 404)
        with self.lock(f"external:{temporary_id}"):
            self._fields("staff", staff_id)
            current = self._fields("external", temporary_id)
            origin = self.s._mop_field_text(current, [ORIGIN_FIELD])
            if origin and origin != staff_id:
                raise SignatureManagementError("临时人员已关联其他正式人员。", 409)
            if payload.get("expected_version") != self._identity_version(current):
                raise SignatureManagementError("临时人员记录已变化，请刷新后重新核对。", 409)
            self.s._patch_record_fields(
                app_token=self.d.SIGNATURE_APP_TOKEN,
                table_id=self.d.TEMP_SIGNATURE_TABLE_ID,
                record_id=temporary_id,
                fields={ORIGIN_FIELD: staff_id},
            )
            if self._fields("external", temporary_id).get(ORIGIN_FIELD) != staff_id:
                raise SignatureManagementError("人员关联回读未通过。", 502)
        self.invalidate()
        return {"linked": True}

    def migrate_linked_signatures(self, actor: str) -> dict:
        """Copy explicitly linked temporary signatures into unsigned formal records."""
        with self.lock("linked-signature-migration"):
            data = self._require_directory()
            staff_by_id = {
                str(person.get("record_id") or ""): person
                for person in data["staff"]
            }
            results = []
            for temporary in data["external"]:
                temporary_id = str(temporary.get("record_id") or "").strip()
                staff_id = str(temporary.get("origin_staff_record_id") or "").strip()
                if not temporary.get("has_signature"):
                    continue
                if not staff_id or staff_id not in staff_by_id:
                    results.append({"temporary_record_id": temporary_id, "status": "unmatched"})
                    continue
                staff = staff_by_id[staff_id]
                key = f"{temporary_id}:{staff_id}"
                with self.lock(f"staff:{staff_id}"):
                    current = self._fields("staff", staff_id)
                    current_metadata = self.s._signature_crypto.metadata_from_field(
                        current.get(self.d.SIGNATURE_KEY_FIELD)
                    )
                    if (
                        self.s._extract_signature_attachments(current)
                        and self.s._signature_crypto.is_portable_metadata(current_metadata)
                    ):
                        results.append({"temporary_record_id": temporary_id, "staff_record_id": staff_id, "status": "already_signed"})
                        continue
                    journal = self.store.get_document(MIGRATION_NS, key) or {
                        "temporary_record_id": temporary_id,
                        "staff_record_id": staff_id,
                        "created_by": actor,
                        "created_at": time.time(),
                        "status": "preparing",
                    }
                    try:
                        if journal.get("write"):
                            write = journal["write"]
                            self.s._patch_record_fields(
                                app_token=self.d.SIGNATURE_APP_TOKEN,
                                table_id=self.d.SIGNATURE_TABLE_ID,
                                record_id=staff_id,
                                fields={
                                    self.d.SIGNATURE_ATTACHMENT_FIELD: [{"file_token": write["file_token"]}],
                                    self.d.SIGNATURE_KEY_FIELD: self.s._signature_crypto.metadata_to_text(write["metadata"]),
                                },
                            )
                        else:
                            content, _ = self.s.external_signature_image_bytes(record_id=temporary_id)

                            def before_write(file_token, metadata):
                                journal.update(
                                    status="writing",
                                    write={"file_token": file_token, "metadata": metadata},
                                )
                                self.store.put_document(MIGRATION_NS, key, journal)

                            self.s._save_encrypted_signature_record(
                                table_id=self.d.SIGNATURE_TABLE_ID,
                                record_id=staff_id,
                                attachment_field=self.d.SIGNATURE_ATTACHMENT_FIELD,
                                key_field=self.d.SIGNATURE_KEY_FIELD,
                                signature_bytes=content,
                                display_name=str(staff.get("name") or temporary.get("name") or "signature"),
                                source="staff",
                                open_id=str(staff.get("open_id") or ""),
                                employee_no=str(staff.get("employee_no") or ""),
                                before_write=before_write,
                            )
                        if not self._verified(journal, self._fields("staff", staff_id)):
                            raise SignatureManagementError("签名迁移回读校验未通过。", 502)
                        journal.update(status="completed", completed_at=time.time(), error="")
                        self.store.put_document(MIGRATION_NS, key, journal)
                        results.append({"temporary_record_id": temporary_id, "staff_record_id": staff_id, "status": "migrated"})
                    except Exception as exc:
                        journal.update(status="failed", error=str(exc), updated_at=time.time())
                        self.store.put_document(MIGRATION_NS, key, journal)
                        results.append({"temporary_record_id": temporary_id, "staff_record_id": staff_id, "status": "failed", "error": str(exc)})
            self.invalidate()
            return {
                "migrated": sum(item["status"] == "migrated" for item in results),
                "already_signed": sum(item["status"] == "already_signed" for item in results),
                "unmatched": sum(item["status"] == "unmatched" for item in results),
                "failed": sum(item["status"] == "failed" for item in results),
                "results": results,
            }

    def merge(self, payload, actor):
        if payload.get("all_clients_upgraded") is not True or payload.get("confirmed_same_person") is not True:
            raise SignatureManagementError("请确认所有电脑已升级，并核实这些记录属于同一个人。")
        ids = sorted({_id(rid) for rid in payload.get("record_ids") or []})
        keep, signature_id, operation_id = _id(payload.get("keep_record_id")), _id(payload.get("signature_record_id")), _id(payload.get("operation_id"))
        if not 2 <= len(ids) <= 20 or keep not in ids or signature_id not in ids:
            raise SignatureManagementError("请选择2至20条记录及其中的保留记录、保留签名。")
        with ExitStack() as stack:
            for rid in ids:
                stack.enter_context(self.lock(f"external:{rid}"))
            self.ensure_fields()
            journal = self.store.get_document(MERGE_NS, operation_id)
            fingerprint = _hash([ids, keep, signature_id, actor])
            if journal and journal["fingerprint"] != fingerprint:
                raise SignatureManagementError("合并操作编号已被使用。", 409)
            if journal and journal.get("status") == "completed":
                return {"status": "completed", "keep_record_id": keep, "deleted_ids": journal["deleted_ids"]}
            if not journal:
                group = next((g for g in self.duplicates()["groups"] if set(ids).issubset({p["record_id"] for p in g["people"]})), None)
                if not group or group["version"] != payload.get("expected_version"):
                    raise SignatureManagementError("重复记录已变化，请重新预检。", 409)
                fields_by_id = {rid: self._fields("external", rid) for rid in ids}
                origins = {self.s._mop_field_text(f, [ORIGIN_FIELD]) for f in fields_by_id.values()} - {""}
                employee_nos = {self.s._mop_field_text(f, [self.d.TEMP_SIGNATURE_EMPLOYEE_NO_FIELD]) for f in fields_by_id.values()} - {""}
                if len(origins) > 1 or len(employee_nos) > 1:
                    raise SignatureManagementError("记录的正式来源或工号冲突，不能合并。", 409)
                backup = {}
                for rid, fields in fields_by_id.items():
                    attachments = []
                    for attachment in self.s._extract_signature_attachments(fields):
                        raw, _ = self.s._download_mop_attachment(attachment)
                        attachments.append(base64.b64encode(raw).decode())
                    backup[rid] = {"fields": fields, "encrypted_attachments": attachments}
                aliases = set(ids) - {keep}
                for fields in fields_by_id.values():
                    aliases.update(re.findall(r"rec[A-Za-z0-9]+", self.s._mop_field_text(fields, [ALIASES_FIELD])))
                aliases.discard(keep)
                journal = {"fingerprint": fingerprint, "status": "backed_up", "backup": backup, "aliases": sorted(aliases),
                           "origin": next(iter(origins), ""), "created_by": actor, "created_at": time.time(), "deleted_ids": []}
                self.store.put_document(MERGE_NS, operation_id, journal)
            if journal["status"] == "backed_up":
                content, _ = self.s.external_signature_image_bytes(record_id=signature_id)
                person = self.person("external", keep)
                def persist_write(token, metadata):
                    journal["write"] = {"file_token": token, "metadata": metadata}
                    self.store.put_document(MERGE_NS, operation_id, journal)
                if not journal.get("write"):
                    self.s._save_encrypted_signature_record(table_id=self.d.TEMP_SIGNATURE_TABLE_ID, record_id=keep,
                        attachment_field=self.d.SIGNATURE_ATTACHMENT_FIELD, key_field=self.d.SIGNATURE_KEY_FIELD,
                        signature_bytes=content, display_name=person["name"], source="external",
                        employee_no=person.get("employee_no") or "", before_write=persist_write)
                write = journal["write"]
                self.s._patch_record_fields(app_token=self.d.SIGNATURE_APP_TOKEN, table_id=self.d.TEMP_SIGNATURE_TABLE_ID, record_id=keep,
                    fields={ALIASES_FIELD: "\n".join(journal["aliases"]), ORIGIN_FIELD: journal["origin"],
                            self.d.SIGNATURE_ATTACHMENT_FIELD: [{"file_token": write["file_token"]}],
                            self.d.SIGNATURE_KEY_FIELD: self.s._signature_crypto.metadata_to_text(write["metadata"])})
                checked = self._fields("external", keep)
                if not self._verified(journal, checked) or set(re.findall(r"rec[A-Za-z0-9]+", self.s._mop_field_text(checked, [ALIASES_FIELD]))) != set(journal["aliases"]):
                    raise SignatureManagementError("合并结果校验未通过，未删除任何重复行。", 502)
                journal["status"] = "verified"
                self.store.put_document(MERGE_NS, operation_id, journal)
            for rid in ids:
                if rid == keep or rid in journal["deleted_ids"]:
                    continue
                current = self._fields("external", keep)
                if not self._verified(journal, current):
                    raise SignatureManagementError("保留签名已变化，停止清理。", 409)
                try:
                    original = self._fields("external", rid)
                except SignatureManagementError as exc:
                    if exc.status != 404:
                        raise
                else:
                    if (self._version(original) != self._version(journal["backup"][rid]["fields"])
                            or self._identity_version(original) != self._identity_version(journal["backup"][rid]["fields"])):
                        raise SignatureManagementError("待删除记录已有新签名，停止清理，请管理员核对。", 409)
                    self.s._delete_record_fields(app_token=self.d.SIGNATURE_APP_TOKEN, table_id=self.d.TEMP_SIGNATURE_TABLE_ID, record_id=rid)
                journal["deleted_ids"].append(rid)
                self.store.put_document(MERGE_NS, operation_id, journal)
            journal["status"] = "completed"
            self.store.put_document(MERGE_NS, operation_id, journal)
            self.invalidate()
            return {"status": "completed", "keep_record_id": keep, "deleted_ids": journal["deleted_ids"]}


def dispatch(manager, method, operation, payload, *, actor="", is_admin=False, base_url="", send_text=None):
    """Both HTTP servers use exactly the same authorization/operation boundary."""
    allowed = {"GET": {"people", "request", "duplicates"}, "POST": {"refresh", "requests", "temporary", "submit", "merge", "associate", "migrate"}}
    if operation not in allowed.get(method, set()):
        raise SignatureManagementError("接口不存在。", 404)
    if operation not in {"request", "submit"} and not actor:
        raise SignatureManagementError("请先登录。", 401)
    if operation in {"duplicates", "merge", "associate", "migrate"} and not is_admin:
        raise SignatureManagementError("只有管理员可以核对、关联或清理人员。", 403)
    if operation == "people":
        return manager.people(payload)
    if operation == "refresh":
        return manager.refresh()
    if operation == "request":
        return manager.session(payload)
    if operation == "submit":
        return manager.submit(payload)
    if operation == "temporary":
        return manager._new_temporary(payload, _id(payload.get("operation_id")), actor)
    if operation == "requests":
        return manager.send(payload, actor=actor, base_url=base_url, send_text=send_text)
    if operation == "duplicates":
        return manager.duplicates()
    if operation == "associate":
        return manager.associate(payload)
    if operation == "migrate":
        return manager.migrate_linked_signatures(actor)
    return manager.merge(payload, actor)

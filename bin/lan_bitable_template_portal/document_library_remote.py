"""Transport adapter for the document-library Bitable table and Drive files.

The adapter is a thin, non-business layer:

* it owns no schema beyond a plain field passthrough;
* it holds no credential cache, it always asks the shared token manager;
* it only talks to fixed open.feishu.cn Bitable/Drive endpoints;
* every provider request re-verifies the credential identity captured at
  the start of the call both before and after the HTTP call (and around
  token retrieval) and fails closed when it changed;
* attachments up to 20 MiB use the streaming ``upload_all`` call, larger
  ones use the bounded ``upload_prepare/upload_part/upload_finish``
  protocol on the same pooled client (never an unbounded raw request);
* reads (GET) allow bounded retries, writes never retry.

Heavy modules (``httpx``, the HTTP client, the token manager, the write
guard, ``AssistantError``) are imported lazily so importing this module
reads no configuration and opens no connection.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import tempfile
import threading
import time
import uuid
import zlib
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit

READ_RETRIES = 1
PAGE_SIZE = 100
MAX_UPLOAD_BYTES = 100 * 1024 * 1024
MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024
MAX_PAGES = 10000
# upload_all is capped by the provider; larger files must use parts.
UPLOAD_ALL_MAX_BYTES = 20 * 1024 * 1024
# Largest part the provider may ask for; anything bigger is refused.
MAX_BLOCK_BYTES = 16 * 1024 * 1024
_READ_RETRY_DELAY_SECONDS = 0.3
FILE_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_-]{8,200}")
_RECORD_ID_PATTERN = re.compile(r"rec[A-Za-z0-9]{1,190}")
_TABLE_ID_PATTERN = re.compile(r"(?:tbl)?[A-Za-z0-9]{3,200}")
_UPLOAD_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,200}")
_TOKEN_ERROR_CODES = frozenset({99991663, 99991664, 99991665, 99991668, 99991677})
_NOT_FOUND_CODE = 1254043

BITABLE_ROOT = "https://open.feishu.cn/open-apis/bitable/v1/apps"
MEDIA_ROOT = "https://open.feishu.cn/open-apis/drive/v1/medias/"
MEDIA_UPLOAD_ALL_URL = MEDIA_ROOT + "upload_all"
MEDIA_UPLOAD_PREPARE_URL = MEDIA_ROOT + "upload_prepare"
MEDIA_UPLOAD_PART_URL = MEDIA_ROOT + "upload_part"
MEDIA_UPLOAD_FINISH_URL = MEDIA_ROOT + "upload_finish"
MEDIA_DOWNLOAD_URL = MEDIA_ROOT + "{file_token}/download"
MEDIA_FILE = "bitable_file"
FIELD_PAGE_SIZE = 100
RECORD_PAGE_SIZE_MAX = 500


def _error(message, status=400, category=""):
    from openclaw_service.assistant.lighthouse_ai import AssistantError

    return AssistantError(message, status, category=category)


def _invalid_response():
    return _error("资料库来源响应无效。", 502, category="bad_response")


def _code(payload):
    """Return the provider code; a malformed code fails closed.

    A rejected payload must never look like ``code == 0``: a missing code, a
    bool, a non-integer (``"garbage"``, ``0.0``, ``None``) or a negative
    value raises instead of being coerced to success.
    """
    if not isinstance(payload, dict) or "code" not in payload:
        raise _invalid_response()
    value = payload.get("code")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _invalid_response()
    return value


def _code_is(payload, expected):
    """Non-raising probe: True only when the code is exactly ``expected``."""
    try:
        return _code(payload) == expected
    except Exception:
        return False


def _data(payload):
    value = (payload or {}).get("data")
    return value if isinstance(value, dict) else {}


def _text(value):
    return value.strip() if isinstance(value, str) else ""


def _is_record_id(value):
    return isinstance(value, str) and bool(_RECORD_ID_PATTERN.fullmatch(value))


def _is_file_token(value):
    return isinstance(value, str) and bool(FILE_TOKEN_PATTERN.fullmatch(value))


def _is_table_id(value):
    return isinstance(value, str) and bool(_TABLE_ID_PATTERN.fullmatch(value))


def _is_upload_id(value):
    return isinstance(value, str) and bool(_UPLOAD_ID_PATTERN.fullmatch(value))


def _configured(value):
    """True only for a non-blank string credential (None/blank is missing)."""
    return isinstance(value, str) and bool(value.strip())


def _identity(credentials):
    """Plain (app_id, app_secret) pair, compared with ``==`` across a call."""
    if not isinstance(credentials, tuple) or len(credentials) != 2:
        raise ValueError("credential identity must be a (app_id, app_secret) tuple")
    if not _configured(credentials[0]) or not _configured(credentials[1]):
        raise ValueError("credential identity is empty")
    return (credentials[0], credentials[1])


def _client_token(namespace, base_token, table_id, operation_id):
    """Stable v4-shaped UUID: the same operation never gets two tokens."""
    seed = "document-library:{0}:{1}:{2}:{3}".format(
        namespace, base_token, table_id, operation_id
    ).encode("utf-8")
    raw = bytearray(hashlib.sha256(seed).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def _part_length(size, block_size, seq):
    return min(block_size, size - seq * block_size)


def _not_found(payload):
    """Only the provider record-id-not-found code means an actual absence.

    1254043 (RecordIdNotFound) is the code this repo already recognizes for a
    deleted or missing Bitable record; every other failure stays an error.
    """
    return _code_is(payload, _NOT_FOUND_CODE)


def _record(payload):
    record = _data(payload).get("record")
    if not isinstance(record, dict):
        return None
    return record if _is_record_id(record.get("record_id")) else None


def parse_table_link(value, table_id=""):
    if not isinstance(value, str) or len(value) > 4096 or not isinstance(table_id, str):
        raise _error("请粘贴有效的飞书多维表链接。")
    value = value.strip()
    try:
        url = urlsplit(value)
        match = re.fullmatch(r"/(base|wiki)/([A-Za-z0-9]{8,200})/?", url.path)
        if (url.scheme != "https" or not url.hostname or url.username or url.password
                or url.port not in (None, 443) or "\\" in value or any(c.isspace() or ord(c) < 32 for c in value)
                or not (url.hostname == "feishu.cn" or url.hostname.endswith(".feishu.cn")) or not match):
            raise ValueError()
        query = parse_qs(url.query, keep_blank_values=True)
        tables = query.get("table", [])
        if len(tables) > 1 or tables and not re.fullmatch(r"tbl[A-Za-z0-9]+", tables[0]):
            raise ValueError()
        if table_id and (not re.fullmatch(r"tbl[A-Za-z0-9]+", table_id) or tables and tables[0] != table_id):
            raise ValueError()
    except ValueError:
        raise _error("请提供有效的 HTTPS 飞书 Base 或 Wiki 多维表链接，数据表编号不能冲突。") from None
    return {"kind": match[1], "token": match[2], "table_id": table_id or (tables[0] if tables else ""),
            "origin": "https://" + url.hostname, "url": value}


class DocumentLibraryRemote:
    """Bitable records plus Drive attachments for one source table."""

    def __init__(self, source):
        if not isinstance(source, dict):
            raise _error("资料库来源配置无效。", category="invalid_source")
        base_token = _text(source.get("base_token"))
        table_id = _text(source.get("table_id"))
        if not _is_table_id(base_token):
            raise _error("资料库 Base Token 无效。", category="invalid_source")
        if not _is_table_id(table_id):
            raise _error("资料库数据表 ID 无效。", category="invalid_source")
        self.base_token = base_token
        self.table_id = table_id
        self._http = None
        self._lock = threading.RLock()
        self._closed = False
        self._records_url = "{0}/{1}/tables/{2}".format(BITABLE_ROOT, base_token, table_id)

    # -- shared helpers -------------------------------------------------

    @staticmethod
    def _token_manager():
        from upload_event_module.services.feishu_token_manager import token_manager

        return token_manager

    def _http_client(self):
        with self._lock:
            if self._http is None:
                import httpx

                from upload_event_module.services.http_client import FeishuHttpClient

                self._http = FeishuHttpClient(
                    timeout=httpx.Timeout(connect=5, read=60, write=60, pool=10),
                    retries=0,
                )
            return self._http

    def _ensure_open(self):
        if self._closed:
            raise _error("资料库连接已关闭，请重新读取。", 409, category="closed")

    def _require_write(self):
        from lan_bitable_template_portal.portal_service import external_real_write_guard

        guard = external_real_write_guard()
        if not isinstance(guard, dict) or not guard.get("real_write_allowed"):
            raise _error("当前外部写入保护已阻止资料库写入。", 403,
                         category="write_blocked")

    def _read(self, manager):
        """Read the current credential identity, never caching it."""
        try:
            return _identity(manager._credentials())
        except Exception:
            raise _error("资料库飞书凭据不可用，请联系管理员。",
                         category="credential_unavailable") from None

    def _check(self, manager, identity):
        """Fail closed unless the credential identity is unchanged."""
        if self._read(manager) != identity:
            raise _error("资料库飞书凭据已变化，本次请求已中止。",
                         401, category="credential_changed")

    def _capture(self):
        """Read the credential pair that every later check compares against."""
        self._ensure_open()
        manager = self._token_manager()
        return manager, self._read(manager)

    def _begin(self, *, write):
        """Capture the credential pair once; it is the reference for the call.

        Writes also pass the external write guard; the pair is re-checked
        right after the guard so a blocked or rotated write sends nothing.
        """
        manager, identity = self._capture()
        if write:
            self._require_write()
            self._check(manager, identity)
        return manager, identity

    def _token(self, manager, identity, *, force_refresh=False):
        try:
            token = manager.get_tenant_token(force_refresh=True) if force_refresh \
                else manager.get_tenant_token()
        except Exception:
            raise _error("资料库飞书授权失败，请联系管理员。",
                         category="token_unavailable") from None
        if not isinstance(token, str) or not token.strip():
            raise _error("资料库飞书授权失败，请联系管理员。",
                         category="token_unavailable")
        self._check(manager, identity)
        return token.strip()

    def _parse(self, payload):
        """Accept code==0 only; diagnostics never echo the provider response."""
        code = _code(payload)
        if code in _TOKEN_ERROR_CODES:
            raise _error("资料库飞书授权被拒绝，请联系管理员。",
                         401, category="token_rejected")
        if code:
            if code in {1254302, 1254304, 99991672}:
                raise _error(f"资料库来源读取权限不足（code={code}）。", 403, category="unauthorized")
            raise _error(f"资料库来源请求失败（code={code}）。", 502, category="provider_error")
        return payload

    def _call(self, method, url, *, params=None, body=None, retries, operation):
        """One logical request; identity is re-checked around every HTTP call.

        Provider non-zero ``code`` values are returned to the caller (rather
        than raised) so callers can recognize a specific not-found code; the
        token rejection path is still handled here because it is retryable
        exactly once for GET reads and never for writes.
        """
        manager, identity = self._begin(write=method != "GET")
        token = self._token(manager, identity)
        headers = {"Authorization": "Bearer " + token}
        for attempt in range(retries + 1):
            params_copy = dict(params) if params else None
            body_copy = dict(body) if body else None
            self._check(manager, identity)
            try:
                response = self._http_client().request_json(
                    method, url, headers=headers, params=params_copy,
                    json_payload=body_copy, retries=0,
                )
            except Exception:
                self._check(manager, identity)
                if not attempt and retries:
                    time.sleep(_READ_RETRY_DELAY_SECONDS * (2 ** attempt))
                    continue
                raise _error("资料库{0}未完成。".format(operation),
                             503, category="transport") from None
            self._check(manager, identity)
            if not isinstance(response, dict) or "code" not in response:
                raise _error("资料库{0}响应无效。".format(operation),
                             502, category="bad_response")
            code = _code(response)
            if code in _TOKEN_ERROR_CODES:
                if method == "GET" and not attempt:
                    # A single token rejection retry is allowed for reads only,
                    # and only with a forced refresh of the same credential.
                    token = self._token(manager, identity, force_refresh=True)
                    headers = {"Authorization": "Bearer " + token}
                    continue
                raise _error("资料库飞书授权被拒绝，请联系管理员。",
                             401, category="token_rejected")
            return response
        raise _error("资料库{0}未完成。".format(operation),
                     503, category="transport")

    # -- Bitable records ------------------------------------------------

    @classmethod
    def inspect_link(cls, url, table_id=""):
        link = parse_table_link(url, table_id)
        remote = cls({"base_token": link["token"], "table_id": link["table_id"] or "tblPending"})
        try:
            manager, identity = remote._capture()

            def read(endpoint, params=None):
                remote._check(manager, identity)
                payload = remote._call("GET", endpoint, params=params, retries=READ_RETRIES, operation="链接解析")
                remote._check(manager, identity)
                return _data(remote._parse(payload))

            base = link["token"]
            if link["kind"] == "wiki":
                node = read("https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node",
                            {"token": base}).get("node")
                if not isinstance(node, dict) or not isinstance(node.get("obj_type"), str):
                    raise _invalid_response()
                if node.get("obj_type") != "bitable":
                    kind = {"docx": "飞书文档", "doc": "飞书文档", "sheet": "电子表格"}.get(node.get("obj_type"), "非多维表内容")
                    raise _error(f"此 Wiki 链接指向{kind}，不是多维表，请选择知识库中的多维表节点。")
                base = node.get("obj_token")
                if not isinstance(base, str) or not re.fullmatch(r"[A-Za-z0-9]{8,200}", base):
                    raise _invalid_response()
            tables, cursor, seen, table_ids = [], "", set(), set()
            for _ in range(MAX_PAGES):
                data = read(f"{BITABLE_ROOT}/{base}/tables",
                            {"page_size": 100, **({"page_token": cursor} if cursor else {})})
                if not isinstance(data.get("items"), list) or type(data.get("has_more")) is not bool:
                    raise _invalid_response()
                for item in data["items"]:
                    if (not isinstance(item, dict) or not re.fullmatch(r"tbl[A-Za-z0-9]+", str(item.get("table_id") or ""))
                            or not isinstance(item.get("name"), str) or not item["name"].strip()
                            or item["table_id"] in table_ids):
                        raise _invalid_response()
                    table_ids.add(item["table_id"])
                    tables.append({"table_id": item["table_id"], "name": item["name"].strip()})
                if not data["has_more"]:
                    break
                cursor = _text(data.get("page_token"))
                if not cursor or cursor in seen:
                    raise _invalid_response()
                seen.add(cursor)
            else:
                raise _invalid_response()
            if not tables:
                raise _error("未读取到可访问的数据表，请检查智能体应用的文档访问权限。", 403)
            selected = link["table_id"] or (tables[0]["table_id"] if len(tables) == 1 else "")
            if not selected:
                return {"url": link["url"], "source": None, "tables": tables, "fields": []}
            table = next((item for item in tables if item["table_id"] == selected), None)
            if not table:
                raise _error("链接指定的数据表不存在或当前飞书应用无权读取。", 403)
            remote.base_token, remote.table_id = base, selected
            remote._records_url = f"{BITABLE_ROOT}/{base}/tables/{selected}"
            fields = remote.fields(credentials=(manager, identity))
            ids = set()
            for field in fields:
                fid = field.get("field_id")
                if (not isinstance(fid, str) or not re.fullmatch(r"fld[A-Za-z0-9]+", fid) or fid in ids
                        or not isinstance(field.get("field_name"), str) or not field["field_name"].strip()
                        or type(field.get("type")) is not int):
                    raise _invalid_response()
                ids.add(fid)
            if not fields:
                raise _error("未取得数据表字段，请检查权限后重新读取。", 502)
            canonical = f"{link['origin']}/base/{base}?" + urlencode({"table": selected})
            key = "table_" + hashlib.sha256((base + ":" + selected).encode()).hexdigest()[:20]
            return {"url": canonical, "source": {"id": key, "name": table["name"], "base_token": base,
                    "table_id": selected, "url": canonical}, "tables": tables, "fields": fields}
        finally:
            remote.close()

    def fields(self, *, credentials=None):
        """Return every field dict of this table, across all pages."""
        manager, identity = credentials or self._capture()
        items = []
        page_token = ""
        seen = set()
        for _page in range(MAX_PAGES):
            self._check(manager, identity)
            params = {"page_size": FIELD_PAGE_SIZE}
            if page_token:
                params["page_token"] = page_token
            data = _data(self._parse(self._call(
                "GET", self._records_url + "/fields", params=params,
                retries=READ_RETRIES, operation="字段读取")))
            self._check(manager, identity)
            page_items = data.get("items")
            if not isinstance(page_items, list) or type(data.get("has_more")) is not bool:
                raise _error("资料库来源字段信息不完整。", 502,
                             category="bad_response")
            for item in page_items:
                if not isinstance(item, dict):
                    raise _error("资料库来源字段信息不完整。", 502,
                                 category="bad_response")
                items.append(item)
            if not data.get("has_more"):
                return items
            page_token = _text(data.get("page_token"))
            if not page_token or page_token in seen:
                raise _error("资料库来源字段分页信息无效。", 502,
                             category="pagination")
            seen.add(page_token)
        raise _error("资料库来源字段分页信息无效。", 502,
                     category="pagination")

    def records(self, cursor="", page_size=PAGE_SIZE):
        """Return one raw record page: items, has_more, page_token."""
        if not isinstance(cursor, str) or len(cursor) > 512:
            raise _error("资料库分页游标无效。", category="invalid_cursor")
        if isinstance(page_size, bool) or not isinstance(page_size, int):
            raise _error("资料库分页大小无效。", category="invalid_page_size")
        if page_size < 1 or page_size > RECORD_PAGE_SIZE_MAX:
            raise _error("资料库分页大小无效。", category="invalid_page_size")
        params = {"page_size": page_size}
        if cursor:
            params["page_token"] = cursor
        data = _data(self._parse(self._call(
            "GET", self._records_url + "/records", params=params,
            retries=READ_RETRIES, operation="记录读取")))
        items = data.get("items")
        has_more = data.get("has_more")
        page_token = data.get("page_token") or ""
        if (not isinstance(items, list) or not isinstance(has_more, bool)
                or not isinstance(page_token, str)):
            raise _error("资料库来源记录信息不完整。", 502,
                         category="bad_response")
        for item in items:
            if (not isinstance(item, dict) or not _is_record_id(item.get("record_id"))
                    or not isinstance(item.get("fields"), dict)):
                raise _error("资料库来源记录信息不完整。", 502,
                             category="bad_response")
        if has_more and (not page_token or page_token == cursor):
            raise _error("资料库来源记录分页信息无效。", 502,
                         category="pagination")
        return {"items": items, "has_more": has_more,
                "page_token": page_token if has_more else ""}

    def get(self, record_id):
        """Return one record, or None only for a recognized record-not-found."""
        if not _is_record_id(record_id):
            raise _error("资料库记录 ID 无效。", category="invalid_record_id")
        payload = self._call("GET", self._records_url + "/records/" + record_id,
                             retries=READ_RETRIES, operation="记录读取")
        if _not_found(payload):
            return None
        record = _record(self._parse(payload))
        if record is None or record.get("record_id") != record_id:
            raise _error("资料库来源记录响应无效。", 502,
                         category="bad_response")
        return record

    def create(self, fields, operation_id):
        """Create one record with a stable client token; never retried."""
        field_payload = self._fields_payload(fields)
        operation = _text(operation_id) if isinstance(operation_id, str) else ""
        if not operation:
            raise _error("资料库操作标识无效。",
                         category="invalid_operation_id")
        token = _client_token("create", self.base_token, self.table_id, operation)
        payload = self._parse(self._call(
            "POST", self._records_url + "/records",
            params={"client_token": token}, body={"fields": field_payload},
            retries=0, operation="记录创建"))
        record = _record(payload)
        if record is None:
            raise _error("资料库来源记录响应无效。", 502,
                         category="bad_response")
        return record

    def update(self, record_id, fields):
        """Update one record; never retried."""
        if not _is_record_id(record_id):
            raise _error("资料库记录 ID 无效。", category="invalid_record_id")
        field_payload = self._fields_payload(fields, allow_empty=True)
        payload = self._parse(self._call(
            "PUT", self._records_url + "/records/" + record_id,
            body={"fields": field_payload}, retries=0,
            operation="记录更新"))
        record = _record(payload)
        if record is None or record.get("record_id") != record_id:
            raise _error("资料库来源记录响应无效。", 502,
                         category="bad_response")
        return record

    def _fields_payload(self, fields, *, allow_empty=False):
        """Validate the payload and fail early when the write guard blocks.

        Credential capture, the guard and the round-trip share one captured
        pair inside ``_call``; this pre-check is read-only so a blocked write
        still never reaches the token manager or the HTTP client.
        """
        if not isinstance(fields, dict) or (not fields and not allow_empty):
            raise _error("资料库字段信息无效。", category="invalid_fields")
        for name in fields:
            if not isinstance(name, str) or not name.strip():
                raise _error("资料库字段信息无效。", category="invalid_fields")
        self._ensure_open()
        self._require_write()
        return fields

    # -- Drive attachments ----------------------------------------------

    def upload(self, path, name):
        """Upload one file for this Bitable; returns file_token, name, size."""
        self._ensure_open()
        local = os.path.abspath(str(path or ""))
        try:
            size = os.path.getsize(local)
        except OSError:
            raise _error("无法读取待上传的资料文件。",
                         category="upload_source") from None
        if size <= 0 or size > MAX_UPLOAD_BYTES:
            raise _error("上传文件不能为空或超过100 MiB。", 413,
                         category="upload_source")
        file_name = _text(name) or os.path.basename(local)
        if not file_name or len(file_name) > 250 or any(ch in file_name for ch in "\r\n\0"):
            raise _error("上传文件名无效。", category="upload_source")
        manager, identity = self._begin(write=True)
        headers = self._upload_headers(manager, identity)
        if size <= UPLOAD_ALL_MAX_BYTES:
            payload = self._upload_all(manager, identity, headers, local, file_name, size)
        else:
            payload = self._upload_parts(manager, identity, headers, local, file_name, size)
        file_token = _text(_data(payload).get("file_token"))
        if not _is_file_token(file_token):
            raise _error("资料库附件上传响应无效。", 502,
                         category="bad_response")
        return {"file_token": file_token, "name": file_name, "size": size}

    def _upload_headers(self, manager, identity):
        token = self._token(manager, identity)
        self._check(manager, identity)
        return {"Authorization": "Bearer " + token}

    def _write(self, manager, identity, sender, operation):
        """Run one write attempt; no retry ever, identity checked around it."""
        self._check(manager, identity)
        try:
            response = sender()
        except Exception:
            self._check(manager, identity)
            raise _error("资料库{0}未完成。".format(operation), 503,
                         category="transport") from None
        self._check(manager, identity)
        return self._parse(response)

    def _upload_all(self, manager, identity, headers, local, file_name, size):
        """One streaming multipart write; the client streams the file itself."""
        request = self._http_client().request_file_json
        return self._write(manager, identity, lambda: request(
            "POST", MEDIA_UPLOAD_ALL_URL, headers=headers,
            data={"file_name": file_name, "parent_type": MEDIA_FILE,
                  "parent_node": self.base_token, "size": str(size)},
            file_path=local, file_name=file_name, file_field="file",
            content_type="application/octet-stream", retries=0,
        ), "附件上传")

    def _upload_parts(self, manager, identity, headers, local, file_name, size):
        """Bounded chunked upload for files above the ``upload_all`` limit.

        Every step is a single non-retried write on the shared pooled client.
        The provider decides the block size; it is validated (bounded, and
        the block count must exactly cover the file) before anything is
        sent. An unknown part or finish outcome stops the upload instead of
        restarting it.
        """
        request_json = self._http_client().request_json
        prepared = _data(self._write(manager, identity, lambda: request_json(
            "POST", MEDIA_UPLOAD_PREPARE_URL, headers=headers,
            json_payload={"file_name": file_name, "parent_type": MEDIA_FILE,
                          "parent_node": self.base_token, "size": size},
            retries=0,
        ), "分片上传准备"))
        upload_id = prepared.get("upload_id")
        block_size = prepared.get("block_size")
        block_num = prepared.get("block_num")
        if (not _is_upload_id(upload_id)
                or isinstance(block_size, bool) or not isinstance(block_size, int)
                or not 0 < block_size <= MAX_BLOCK_BYTES
                or isinstance(block_num, bool) or not isinstance(block_num, int)
                or block_num != -(-size // block_size)):
            raise _error("资料库附件分片上传参数无效。",
                         502, category="bad_response")
        request_file_json = self._http_client().request_file_json
        try:
            with open(local, "rb") as handle, tempfile.TemporaryDirectory(
                    prefix="document-library-part-") as scratch:
                part_path = os.path.join(scratch, "part.bin")
                for seq in range(block_num):
                    expected = _part_length(size, block_size, seq)
                    part = handle.read(expected)
                    if len(part) != expected:
                        raise _error("上传过程中资料文件已变化，上传已中止。",
                                     409, category="upload_source")
                    with open(part_path, "wb") as sink:
                        sink.write(part)
                    checksum = str(zlib.adler32(part) & 0xFFFFFFFF)
                    del part
                    self._write(manager, identity, lambda: request_file_json(
                        "POST", MEDIA_UPLOAD_PART_URL, headers=headers,
                        data={"upload_id": upload_id, "seq": str(seq),
                              "size": str(expected), "checksum": checksum},
                        file_path=part_path, file_name="part.bin", file_field="file",
                        content_type="application/octet-stream", retries=0,
                    ), "分片上传")
                if handle.read(1):
                    raise _error("上传过程中资料文件已变化，上传已中止。",
                                 409, category="upload_source")
        except OSError:
            raise _error("无法读取待上传的资料文件。",
                         category="upload_source") from None
        return self._write(manager, identity, lambda: request_json(
            "POST", MEDIA_UPLOAD_FINISH_URL, headers=headers,
            json_payload={"upload_id": upload_id, "block_num": block_num},
            retries=0,
        ), "分片上传完成")

    def _download_params(self, file_token, record_id, field_id):
        if not _is_file_token(file_token):
            raise _error("资料库附件标识无效。",
                         category="invalid_file_token")
        if record_id is None and field_id is None:
            return None
        if (not _is_record_id(record_id) or not isinstance(field_id, str)
                or not re.fullmatch(r'fld[A-Za-z0-9]+', field_id)
                or not re.fullmatch(r'tbl[A-Za-z0-9]+', self.table_id)):
            raise _error('附件所属记录或字段无效。', category='invalid_attachment_context')
        return {'extra': json.dumps({'bitablePerm': {'tableId': self.table_id,
            'attachments': {field_id: {record_id: [file_token]}}}}, separators=(',', ':'))}

    def _download_stream(self, url, *, headers, params):
        return self._http_client()._client_for_request().stream('GET', url,
            headers=headers, params=params, follow_redirects=False)

    @staticmethod
    def _download_redirect(url, location, api_path):
        try:
            if not isinstance(location, str) or not location or len(location) > 8192 or re.search(r'[\x00-\x20\\]', location):
                raise ValueError()
            target = urljoin(url, location)
            parsed = urlsplit(target)
            host = parsed.hostname or ''
            if (parsed.scheme != 'https' or parsed.username or parsed.password or parsed.fragment
                    or parsed.port not in (None, 443)
                    or not any(host == domain or host.endswith('.' + domain)
                        for domain in ('feishu.cn', 'feishucdn.com', 'feishu-attachment.com'))
                    or host == 'open.feishu.cn' and parsed.path != api_path):
                raise ValueError()
            return target
        except ValueError:
            raise _error('附件下载跳转目标不安全，已停止。', 502, category='unsafe_redirect') from None

    def _download(self, file_token, output, *, record_id=None, field_id=None):
        params = self._download_params(file_token, record_id, field_id)
        from urllib.parse import quote
        api_url = MEDIA_DOWNLOAD_URL.format(file_token=quote(file_token, safe=''))
        manager, identity = self._begin(write=False)
        token = self._token(manager, identity)
        auth_retry = network_retry = False
        while True:
            url, seen = api_url, set()
            output.seek(0)
            output.truncate()
            try:
                for _ in range(5):
                    self._check(manager, identity)
                    if url in seen:
                        raise _error('附件下载发生循环跳转，已停止。', 502, category='unsafe_redirect')
                    seen.add(url)
                    is_api = urlsplit(url).hostname == 'open.feishu.cn'
                    # Only the exact API endpoint receives bearer/context; never the attachment CDN.
                    with self._download_stream(api_url if is_api else url,
                            headers={'Accept-Encoding': 'identity', **({'Authorization': 'Bearer ' + token} if is_api else {})},
                            params=params if is_api else None) as response:
                        self._check(manager, identity)
                        status = response.status_code
                        if status in {301, 302, 303, 307, 308}:
                            url = self._download_redirect(url, response.headers.get('location'), urlsplit(api_url).path)
                            continue
                        code = None
                        if status != 200 or (is_api and 'json' in response.headers.get('content-type', '').lower()
                                and 'attachment' not in response.headers.get('content-disposition', '').lower()):
                            body = bytearray()
                            for chunk in response.iter_bytes(8192):
                                if len(body) + len(chunk) > 65536:
                                    raise _error('附件接口返回的错误信息无效。', 502, category='bad_response')
                                body.extend(chunk)
                            try:
                                payload = json.loads(body)
                                raw_code = payload.get('code') if isinstance(payload, dict) else None
                                code = raw_code if type(raw_code) is int else None
                            except (ValueError, UnicodeError):
                                pass
                            if status == 401 or type(code) is int and code in _TOKEN_ERROR_CODES:
                                raise _error('附件下载鉴权失效。', 401, category='token_rejected')
                            if status == 403 or code in {1254302, 1254304, 99991672, 99991679}:
                                raise _error('附件读取权限不足，请检查多维表高级权限与附件访问权限。', 403, category='unauthorized')
                            if status == 429 or code == 99991400:
                                raise _error('飞书附件下载限流，稍后自动重试。', 429, category='rate_limit')
                            if type(code) is int and code:
                                raise _error(f'附件下载被飞书拒绝（code={code}）。', 502, category='provider_error')
                            raise _error(f'附件接口未返回原文件（HTTP {status}）。', 502, category='bad_response')
                        declared = response.headers.get('content-length')
                        if declared is not None and (not declared.isdecimal() or int(declared) > MAX_DOWNLOAD_BYTES):
                            raise _error('附件大小无效或超过100 MiB。', 413, category='too_large')
                        size = 0
                        for chunk in response.iter_bytes(1024 * 1024):
                            self._check(manager, identity)
                            size += len(chunk)
                            if size > MAX_DOWNLOAD_BYTES:
                                raise _error('附件下载超过100 MiB。', 413, category='too_large')
                            output.write(chunk)
                        self._check(manager, identity)
                        encoded = response.headers.get('content-encoding', 'identity').lower() not in ('', 'identity')
                        if not size or not encoded and declared is not None and size != int(declared):
                            raise _error('附件未完整下载，未保存为成功缓存。', 502, category='bad_response')
                        return size
                raise _error('附件下载跳转次数过多，已停止。', 502, category='unsafe_redirect')
            except Exception as exc:
                self._check(manager, identity)
                category = getattr(exc, 'category', '')
                if category == 'token_rejected' and not auth_retry:
                    auth_retry = True
                    token = self._token(manager, identity, force_refresh=True)
                    continue
                if category:
                    raise
                import httpx
                if isinstance(exc, httpx.TransportError) and not network_retry:
                    network_retry = True
                    time.sleep(_READ_RETRY_DELAY_SECONDS)
                    continue
                if isinstance(exc, OSError) and not isinstance(exc, httpx.TransportError):
                    raise _error('附件本地文件写入失败，请检查磁盘空间与目录权限。', 503, category='local_storage') from None
                raise _error('附件下载连接失败或超时。', 503, category='transport') from None

    def download(self, file_token, *, record_id=None, field_id=None):
        """Compatibility reader; production caching uses the streamed file path."""
        with io.BytesIO() as output:
            self._download(file_token, output, record_id=record_id, field_id=field_id)
            return output.getvalue()

    def download_to_file(self, file_token, target, *, record_id=None, field_id=None):
        from pathlib import Path
        target = Path(target)
        try:
            with target.open('wb') as output:
                return self._download(file_token, output, record_id=record_id, field_id=field_id)
        except BaseException:
            target.unlink(missing_ok=True)
            raise

    # -- lifecycle ------------------------------------------------------

    def close(self):
        """Release the HTTP pool; safe to call more than once."""
        with self._lock:
            self._closed = True
            http_client = self._http
            self._http = None
        if http_client is not None:
            try:
                http_client.close()
            except Exception:
                pass

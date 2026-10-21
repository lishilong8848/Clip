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
import os
import re
import tempfile
import threading
import time
import uuid
import zlib

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
            raise _error("资料库来源请求失败。", 502, category="provider_error")
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

    def _bytes(self, url, *, params=None):
        """Read raw bytes (download); identity is re-checked around the call."""
        manager, identity = self._begin(write=False)
        token = self._token(manager, identity)
        headers = {"Authorization": "Bearer " + token}
        for attempt in range(READ_RETRIES + 1):
            self._check(manager, identity)
            try:
                content, _content_type = self._http_client().request_bytes(
                    "GET", url, headers=headers,
                    params=dict(params) if params else None,
                    retries=0, max_bytes=MAX_DOWNLOAD_BYTES + 1,
                )
            except Exception:
                self._check(manager, identity)
                if not attempt:
                    time.sleep(_READ_RETRY_DELAY_SECONDS)
                    continue
                raise _error("资料库附件下载未完成。",
                             503, category="transport") from None
            self._check(manager, identity)
            return content
        raise _error("资料库附件下载未完成。", 503, category="transport")

    # -- Bitable records ------------------------------------------------

    def fields(self):
        """Return every field dict of this table, across all pages."""
        items = []
        page_token = ""
        seen = set()
        for _page in range(MAX_PAGES):
            params = {"page_size": FIELD_PAGE_SIZE}
            if page_token:
                params["page_token"] = page_token
            data = _data(self._parse(self._call(
                "GET", self._records_url + "/fields", params=params,
                retries=READ_RETRIES, operation="字段读取")))
            page_items = data.get("items")
            if not isinstance(page_items, list):
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

    def download(self, file_token):
        """Download one attachment into memory, bounded to 100 MiB."""
        if not _is_file_token(file_token):
            raise _error("资料库附件标识无效。",
                         category="invalid_file_token")
        from urllib.parse import quote

        url = MEDIA_DOWNLOAD_URL.format(file_token=quote(file_token, safe=""))
        content = self._bytes(url)
        if not isinstance(content, (bytes, bytearray)):
            raise _error("资料库附件下载响应无效。", 502,
                         category="bad_response")
        if len(content) > MAX_DOWNLOAD_BYTES:
            raise _error("资料库附件下载超过100 MiB。", 413,
                         category="too_large")
        return bytes(content)

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

"""Shared link directory; cloud records are authoritative, local reads are cached."""
from __future__ import annotations

import copy
import hashlib
import json
import logging
import re
import threading
import time
import uuid
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

from .portal_service import PortalError, PortalConflictError, PortalNotFoundError

APP_TOKEN = "MliKbC3fXa8PXrsndKscmxjdn1g"
TABLE_ID = "tblKxPafpJzThkeE"
FIELDS = {"name": "表名", "url": "链接", "category": "所属分类", "purpose": "用途", "sort": "排序"}
SCHEMA = {"表名": 1, "链接": 15, "所属分类": 1, "用途": 1, "排序": 2, "目录标识": 1}


def plain(value):
    if isinstance(value, list):
        return "".join(plain(item) for item in value)
    if isinstance(value, dict):
        return str(value.get("text") or "")
    return str(value or "")


def normalize_url(value):
    if not isinstance(value, str) or len(value) > 2048:
        raise PortalError("请填写有效的多维表或网页链接。")
    try:
        value = value.strip()
        if any(character.isspace() or ord(character) < 32 for character in value):
            raise ValueError()
        parts = urlsplit(value)
        if (parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password
                or "\\" in parts.netloc or parts.port == 0):
            raise ValueError()
        if parts.hostname != "vnet.feishu.cn" or not re.match(r"^/(?:base|wiki)/", parts.path):
            return urlunsplit((parts.scheme, parts.netloc.lower(), parts.path or "/", parts.query, parts.fragment))
        params = parse_qs(parts.query)
        if (parts.scheme != "https" or parts.hostname != "vnet.feishu.cn"
                or parts.port not in (None, 443) or parts.username or parts.password
                or not re.fullmatch(r"/(?:base|wiki)/[A-Za-z0-9]+/?", parts.path)
                or len(params.get("table", [])) != 1
                or not re.fullmatch(r"tbl[A-Za-z0-9]+", params["table"][0])):
            raise ValueError()
        query = {"table": params["table"][0]}
        if "view" in params:
            if len(params["view"]) != 1 or not re.fullmatch(r"vew[A-Za-z0-9]+", params["view"][0]):
                raise ValueError()
            query["view"] = params["view"][0]
        return urlunsplit(("https", "vnet.feishu.cn", parts.path.rstrip("/"), urlencode(query), ""))
    except (ValueError, KeyError):
        raise PortalError("请填写完整的 HTTP/HTTPS 网页地址；飞书多维表链接须包含 table 参数。") from None


def identity(url):
    canonical = normalize_url(url)
    parts = urlsplit(canonical)
    if parts.hostname == "vnet.feishu.cn" and re.match(r"^/(?:base|wiki)/", parts.path):
        return "table:" + parse_qs(parts.query)["table"][0]
    return "web:" + hashlib.sha256(canonical.encode()).hexdigest()


def validate_item(data):
    if not isinstance(data, dict) or set(data) - {*FIELDS, "request_id"}:
        raise PortalError("链接填写内容无效。")
    result = {}
    for key, maximum in (("name", 120), ("category", 80), ("purpose", 2000)):
        value = data.get(key, "")
        if not isinstance(value, str) or len(value.strip()) > maximum:
            raise PortalError(f"{FIELDS[key]}最多填写 {maximum} 字。")
        result[key] = value.strip()
    if not result["name"]:
        raise PortalError("请填写表名。")
    result["url"] = normalize_url(data.get("url"))
    order = data.get("sort", 0)
    if isinstance(order, bool) or not isinstance(order, int) or not 0 <= order <= 1000000:
        raise PortalError("排序须为 0 至 1000000 的整数。")
    result["sort"] = order
    return result


def to_fields(item):
    return {**{label: item[key] for key, label in FIELDS.items() if key != "url"},
            "链接": {"text": item["name"], "link": item["url"]}, "目录标识": identity(item["url"])}


def from_record(record):
    fields = record.get("fields") or {}
    url = fields.get("链接")
    item = {key: plain(fields.get(label)) for key, label in FIELDS.items() if key not in {"url", "sort"}}
    order = fields.get("排序") or 0
    item.update(url=url.get("link", "") if isinstance(url, dict) else plain(url), sort=int(float(order)))
    return {**validate_item(item), "id": record["record_id"]}


class LinkRemote:
    def __init__(self, service):
        self.service = service

    def request(self, method, path, body=None, params=None):
        from .portal_service import TOKEN_ERROR_CODES, external_real_write_guard, refresh_feishu_token
        if method != "GET":
            guard = external_real_write_guard()
            if not guard["real_write_allowed"]:
                raise PortalError(guard["reason"])
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/{path}"
        for attempt in range(3):
            payload = self.service._http_client.request_json(
                method, url, headers=self.service._auth_headers(), params=params or {},
                json_payload=body, retries=1 if method == "GET" else 0)
            code = int(payload.get("code") or 0)
            if code in TOKEN_ERROR_CODES and attempt == 0:
                refresh_feishu_token()
                continue
            if code in {99991400, 1254608, 1254290, 1254291} and attempt < 2:
                time.sleep(1 + attempt)
                continue
            if code:
                raise PortalError(f"多维表导航同步失败：code={code}，{payload.get('msg') or '请稍后重试'}")
            return payload.get("data") or {}
        raise PortalError("多维表导航同步失败，请稍后重试。")

    def list_all(self, path="records"):
        items, seen, token = [], set(), ""
        while True:
            page = self.request("GET", path, params={"page_size": 100, **({"page_token": token} if token else {})})
            if not isinstance(page.get("items"), list) or not isinstance(page.get("has_more"), bool):
                raise PortalError("导航目录分页内容不完整，已保留原缓存。")
            items.extend(page["items"])
            if not page["has_more"]:
                return items
            token = page.get("page_token")
            if not token or token in seen:
                raise PortalError("导航目录分页异常，已保留原缓存。")
            seen.add(token)

    def create(self, item, request_id):
        fingerprint = request_id + json.dumps(item, sort_keys=True, ensure_ascii=False)
        token = str(uuid.UUID(hashlib.sha256(fingerprint.encode()).hexdigest()[:32], version=4))
        data = self.request("POST", "records", {"fields": to_fields(item)}, {"client_token": token})
        record_id = data.get("record", {}).get("record_id", "")
        if not re.fullmatch(r"rec[A-Za-z0-9]+", record_id):
            raise PortalError("飞书未返回导航记录编号，请刷新核对后重试。")
        return record_id

    def update(self, record_id, item):
        self.request("PUT", "records/" + record_id, {"fields": to_fields(item)})

    def delete(self, record_id):
        self.request("DELETE", "records/" + record_id)


class LinkDirectory:
    def __init__(self, store, remote):
        self.store, self.remote = store, remote
        self._snapshot = store.get_document("link_directory", "snapshot")
        self._write_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._refreshing = False
        self._last_attempt = 0.0
        self._error = ""

    def _save(self, items, updated_at=None):
        snapshot = {"items": sorted(items, key=lambda item: (item["sort"], item["name"], item["id"])),
                    "updated_at": time.time() if updated_at is None else updated_at}
        self.store.put_document("link_directory", "snapshot", snapshot)
        self._snapshot = snapshot

    def _refresh(self):
        items = [from_record(row) for row in self.remote.list_all() if any((row.get("fields") or {}).values())]
        self._save(items)
        self._error = ""

    def read(self, force=False):
        if force or self._snapshot is None:
            with self._write_lock:
                if force or self._snapshot is None:
                    self._last_attempt = time.monotonic()
                    try:
                        self._refresh()
                    except Exception:
                        if self._snapshot is None or force:
                            raise
        snapshot = copy.deepcopy(self._snapshot)
        stale = time.time() - snapshot["updated_at"] > 300
        if stale and not force:
            with self._state_lock:
                if not self._refreshing and time.monotonic() - self._last_attempt > 60:
                    self._refreshing = True
                    threading.Thread(target=self._background_refresh, name="LinkDirectoryRefresh", daemon=True).start()
        return {**snapshot, "stale": stale, "error": self._error}

    def _background_refresh(self):
        try:
            with self._write_lock:
                self._last_attempt = time.monotonic()
                self._refresh()
        except Exception:
            self._error = "云端目录暂时无法读取，正在显示本地已保存的链接。"
            logging.warning("Link directory refresh failed", exc_info=True)
        finally:
            with self._state_lock:
                self._refreshing = False

    def save(self, data, record_id=None):
        item = validate_item(data)
        if record_id and not re.fullmatch(r"rec[A-Za-z0-9]+", record_id):
            raise PortalError("导航记录编号无效。")
        request_id = data.get("request_id", "")
        if not record_id:
            try:
                request_id = str(uuid.UUID(request_id))
            except (ValueError, TypeError, AttributeError):
                raise PortalError("新增链接缺少有效请求编号，请重新打开新增窗口。") from None
        with self._write_lock:
            # Re-read before writes so another client or direct cloud edit is not lost.
            self._refresh()
            items = copy.deepcopy(self._snapshot["items"])
            matches = [row for row in items if identity(row["url"]) == identity(item["url"]) and row["id"] != record_id]
            if matches:
                # A lost create response can be retried without a second cloud record.
                if not record_id and len(matches) == 1 and all(matches[0][key] == value for key, value in item.items()):
                    return {"item": matches[0]}
                raise PortalConflictError("该链接已在目录中，请编辑已有链接。")
            if record_id:
                if not any(row["id"] == record_id for row in items):
                    raise PortalNotFoundError("链接已被删除，请刷新目录。")
                self.remote.update(record_id, item)
            else:
                record_id = self.remote.create(item, request_id)
            saved = {**item, "id": record_id}
            self._save([row for row in items if row["id"] != record_id] + [saved], self._snapshot["updated_at"])
            return {"item": saved}

    def delete(self, record_id):
        if not re.fullmatch(r"rec[A-Za-z0-9]+", record_id):
            raise PortalError("导航记录编号无效。")
        with self._write_lock:
            self._refresh()
            items = self._snapshot["items"]
            if any(row["id"] == record_id for row in items):
                self.remote.delete(record_id)
                self._save([row for row in items if row["id"] != record_id], self._snapshot["updated_at"])
            return {"deleted": True}

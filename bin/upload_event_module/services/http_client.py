# -*- coding: utf-8 -*-
from __future__ import annotations

import atexit
import email.utils
import os
import random
import ssl
import threading
import time
import weakref
from types import SimpleNamespace
from http.cookiejar import DefaultCookiePolicy
from typing import Any

import certifi


DEFAULT_TIMEOUT = None
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
_CLIENTS: "weakref.WeakSet[FeishuHttpClient]" = weakref.WeakSet()
_tls_contexts = {}
_tls_lock = threading.Lock()


def verified_tls_context(*, trust_env=True, cafile=None):
    with _tls_lock:
        ca_file = cafile or (os.environ.get('SSL_CERT_FILE') if trust_env else None)
        ca_dir = os.environ.get('SSL_CERT_DIR') if trust_env and not ca_file else None
        policy = (ca_file or '', ca_dir or '')
        if policy not in _tls_contexts:
            import httpx
            # Preserve the CA policy; loading identical PEM bytes avoids slow Windows file I/O.
            if cafile:
                kwargs = {'capath': cafile} if os.path.isdir(cafile) else {'cafile': cafile}
                _tls_contexts[policy] = ssl.create_default_context(**kwargs)
            elif ca_file or ca_dir:
                _tls_contexts[policy] = httpx.create_ssl_context(trust_env=True)
            else:
                _tls_contexts[policy] = ssl.create_default_context(cadata=certifi.contents())
        return _tls_contexts[policy]


class FeishuHTTPError(RuntimeError):
    def __init__(self, message: str, *, category: str = "network") -> None:
        super().__init__(message)
        self.category = category


def classify_feishu_error(status_code: int = 0, code: int | None = None) -> str:
    if status_code == 401 or code in {99991663, 99991664, 99991668}:
        return "token"
    if status_code == 403 or code in {99991671, 99991672, 1254002}:
        return "permission"
    if status_code == 429 or code == 99991400:
        return "rate_limit"
    if status_code >= 500:
        return "remote"
    if code:
        return "business"
    return "network"


class FeishuHttpClient:
    """Small shared-client wrapper for Feishu/Bitable HTTP calls.

    The old module-level request_json function is kept for compatibility. New
    code can keep one client instance per service so repeated Feishu calls reuse
    TCP/TLS connections instead of creating a fresh client for every request.
    """

    def __init__(
        self,
        *,
        timeout: Any = DEFAULT_TIMEOUT,
        retries: int = 2,
        transport: Any = None,
        verify: Any = None,
    ) -> None:
        self.timeout = timeout
        self.retries = max(0, int(retries or 0))
        self._transport = transport
        self._verify = verify
        self._client: Any = None
        self._lock = threading.RLock()
        _CLIENTS.add(self)

    def _ensure_client_locked(self):
        import httpx

        client = self._client
        if client is not None:
            return client
        timeout = self.timeout or httpx.Timeout(
            connect=3.0, read=15.0, write=60.0, pool=3.0
        )
        client_kwargs = {"timeout": timeout, "follow_redirects": False}
        if self._transport is not None:
            client_kwargs["transport"] = self._transport
        else:
            client_kwargs["verify"] = self._verify if self._verify is not None else verified_tls_context()
        client = httpx.Client(**client_kwargs)
        self._client = client
        return client

    def _client_for_request(self):
        with self._lock:
            return self._ensure_client_locked()

    @staticmethod
    def _retry_delay(response: Any, attempt: int) -> float:
        delays = []
        headers = response.headers if response is not None else {}
        for name in ("retry-after", "x-ogw-ratelimit-reset"):
            value = str(headers.get(name) or "").strip()
            if not value:
                continue
            try:
                delays.append(max(0.0, min(float(value), 60.0)))
            except ValueError:
                if name == "retry-after":
                    try:
                        target = email.utils.parsedate_to_datetime(value).timestamp()
                        delays.append(max(0.0, min(target - time.time(), 60.0)))
                    except (TypeError, ValueError, OverflowError):
                        pass
        if delays:
            return max(delays)
        return 0.35 * (2**attempt) + random.random() * 0.2

    def close(self) -> None:
        with self._lock:
            client = self._client
            self._client = None
            if client is not None:
                client.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        json_payload: Any = None,
        retries: int | None = None,
    ) -> dict[str, Any]:
        import httpx

        retry_count = self.retries if retries is None else max(0, int(retries or 0))
        last_error = ""
        for attempt in range(retry_count + 1):
            try:
                response = self._client_for_request().request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json_payload,
                )
                if response.status_code in RETRY_STATUS_CODES and attempt < retry_count:
                    time.sleep(self._retry_delay(response, attempt))
                    continue
                try:
                    payload = response.json()
                except ValueError:
                    response.raise_for_status()
                    raise FeishuHTTPError("接口返回不是 JSON 对象", category="business")
                if isinstance(payload, dict):
                    # Some Feishu endpoints report throttling as HTTP 200/400.
                    if str(payload.get("code")) == "99991400" and attempt < retry_count:
                        time.sleep(self._retry_delay(response, attempt))
                        continue
                    if response.status_code >= 400 and int(payload.get("code") or 0) == 0:
                        response.raise_for_status()
                    return payload
                response.raise_for_status()
                raise FeishuHTTPError("接口返回不是 JSON 对象", category="business")
            except httpx.HTTPStatusError as exc:
                status = int(exc.response.status_code if exc.response else 0)
                last_error = str(exc)
                if status in RETRY_STATUS_CODES and attempt < retry_count:
                    time.sleep(self._retry_delay(exc.response, attempt))
                    continue
                raise FeishuHTTPError(
                    last_error,
                    category=classify_feishu_error(status_code=status),
                ) from exc
            except Exception as exc:
                last_error = str(exc)
                if attempt < retry_count:
                    time.sleep(0.35 * (2**attempt) + random.random() * 0.2)
                    continue
                if isinstance(exc, FeishuHTTPError):
                    raise
                raise FeishuHTTPError(last_error, category="network") from exc
        raise FeishuHTTPError(last_error or "HTTP 请求失败", category="network")

    def request_file_json(
        self,
        method: str,
        url: str,
        *,
        file_path: str,
        file_name: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
        file_field: str = "file",
        content_type: str = "application/octet-stream",
        retries: int | None = None,
    ) -> dict[str, Any]:
        """Upload one file as multipart data and return a JSON object.

        The file is reopened for every retry so a partial request never leaves
        the next attempt reading from the previous file position.
        """

        import httpx

        retry_count = self.retries if retries is None else max(0, int(retries or 0))
        last_error = ""
        for attempt in range(retry_count + 1):
            try:
                with open(file_path, "rb") as file_obj:
                    files = {
                        str(file_field or "file"): (
                            str(file_name or "upload.bin"),
                            file_obj,
                            str(content_type or "application/octet-stream"),
                        )
                    }
                    response = self._client_for_request().request(
                        method,
                        url,
                        headers=headers,
                        data=data,
                        files=files,
                    )
                if response.status_code in RETRY_STATUS_CODES and attempt < retry_count:
                    time.sleep(self._retry_delay(response, attempt))
                    continue
                try:
                    payload = response.json()
                except ValueError:
                    response.raise_for_status()
                    raise FeishuHTTPError("接口返回不是 JSON 对象", category="business")
                if isinstance(payload, dict):
                    if response.status_code >= 400 and int(payload.get("code") or 0) == 0:
                        response.raise_for_status()
                    return payload
                response.raise_for_status()
                raise FeishuHTTPError("接口返回不是 JSON 对象", category="business")
            except httpx.HTTPStatusError as exc:
                status = int(exc.response.status_code if exc.response else 0)
                last_error = str(exc)
                if status in RETRY_STATUS_CODES and attempt < retry_count:
                    time.sleep(self._retry_delay(exc.response, attempt))
                    continue
                raise FeishuHTTPError(
                    last_error,
                    category=classify_feishu_error(status_code=status),
                ) from exc
            except Exception as exc:
                last_error = str(exc)
                if attempt < retry_count:
                    time.sleep(0.35 * (2**attempt) + random.random() * 0.2)
                    continue
                if isinstance(exc, FeishuHTTPError):
                    raise
                raise FeishuHTTPError(last_error, category="network") from exc
        raise FeishuHTTPError(last_error or "HTTP 上传失败", category="network")

    def request_bytes(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        retries: int | None = None,
        max_bytes: int = 15 * 1024 * 1024,
    ) -> tuple[bytes, str]:
        import httpx

        retry_count = self.retries if retries is None else max(0, int(retries or 0))
        last_error = ""
        for attempt in range(retry_count + 1):
            try:
                response = self._client_for_request().request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                )
                if response.status_code in RETRY_STATUS_CODES and attempt < retry_count:
                    time.sleep(self._retry_delay(response, attempt))
                    continue
                response.raise_for_status()
                content = response.content
                if len(content) > int(max_bytes or 0):
                    raise FeishuHTTPError("下载文件过大，已停止预览。", category="business")
                return content, str(response.headers.get("content-type") or "")
            except httpx.HTTPStatusError as exc:
                status = int(exc.response.status_code if exc.response else 0)
                last_error = str(exc)
                if status in RETRY_STATUS_CODES and attempt < retry_count:
                    time.sleep(self._retry_delay(exc.response, attempt))
                    continue
                raise FeishuHTTPError(
                    last_error,
                    category=classify_feishu_error(status_code=status),
                ) from exc
            except Exception as exc:
                last_error = str(exc)
                if attempt < retry_count:
                    time.sleep(0.35 * (2**attempt) + random.random() * 0.2)
                    continue
                if isinstance(exc, FeishuHTTPError):
                    raise
                raise FeishuHTTPError(last_error, category="network") from exc
        raise FeishuHTTPError(last_error or "HTTP 请求失败", category="network")


def close_all_clients() -> None:
    for client in list(_CLIENTS):
        try:
            client.close()
        except Exception:
            pass


atexit.register(close_all_clients)

_sdk_http_client = None
_sdk_http_lock = threading.Lock()


def _sdk_request(method, url, *, headers=None, params=None, data=None, timeout=None, files=None):
    """Keep SDK serialization/response handling, reuse verified TLS and sockets."""
    import httpx
    from requests.exceptions import ConnectionError, Timeout, ReadTimeout

    global _sdk_http_client
    with _sdk_http_lock:
        if _sdk_http_client is None:
            # Match requests' CA policy; never downgrade to an unverified context.
            ca = os.environ.get('REQUESTS_CA_BUNDLE') or os.environ.get('CURL_CA_BUNDLE')
            _sdk_http_client = FeishuHttpClient(retries=0, verify=verified_tls_context(trust_env=False, cafile=ca))
            _sdk_http_client._client_for_request().cookies.jar.set_policy(DefaultCookiePolicy(allowed_domains=()))
        client = _sdk_http_client
    headers = dict(headers or {})
    content = data
    if hasattr(data, 'read'):
        if getattr(data, 'len', None) is not None:
            headers.setdefault('Content-Length', str(data.len))
        content = iter(lambda: data.read(65536), b'')
    if isinstance(timeout, tuple):
        connect, read = timeout
        timeout = httpx.Timeout(connect=connect, read=read, write=read, pool=connect)
    try:
        return client._client_for_request().request(method, url, headers=headers,
            params=params, timeout=timeout, follow_redirects=True,
            **({"data": data, "files": files} if files is not None else {"content": content}))
    except httpx.ReadTimeout as exc:
        raise ReadTimeout(str(exc)) from exc
    except httpx.TimeoutException as exc:
        raise Timeout(str(exc)) from exc
    except httpx.RequestError as exc:
        raise ConnectionError(str(exc)) from exc


def reuse_lark_http_client():
    # Patch only the SDK's transport dependency, never the global requests module.
    from lark_oapi.core.http import transport
    transport.requests = SimpleNamespace(request=_sdk_request)


def request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_payload: Any = None,
    timeout: Any = DEFAULT_TIMEOUT,
    retries: int = 2,
) -> dict[str, Any]:
    client = FeishuHttpClient(timeout=timeout, retries=retries)
    try:
        return client.request_json(
            method,
            url,
            headers=headers,
            params=params,
            json_payload=json_payload,
            retries=retries,
        )
    finally:
        client.close()

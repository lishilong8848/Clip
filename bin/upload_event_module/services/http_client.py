# -*- coding: utf-8 -*-
from __future__ import annotations

import atexit
import random
import threading
import time
import weakref
from typing import Any

import httpx


DEFAULT_TIMEOUT = httpx.Timeout(connect=3.0, read=15.0, write=60.0, pool=3.0)
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
_CLIENTS: "weakref.WeakSet[FeishuHttpClient]" = weakref.WeakSet()


class FeishuHTTPError(RuntimeError):
    def __init__(self, message: str, *, category: str = "network") -> None:
        super().__init__(message)
        self.category = category


def classify_feishu_error(status_code: int = 0, code: int | None = None) -> str:
    if status_code == 401 or code in {99991663, 99991664, 99991668}:
        return "token"
    if status_code == 403 or code in {99991671, 99991672, 1254002}:
        return "permission"
    if status_code == 429:
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
        timeout: httpx.Timeout = DEFAULT_TIMEOUT,
        retries: int = 2,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.timeout = timeout
        self.retries = max(0, int(retries or 0))
        client_kwargs = {"timeout": timeout, "follow_redirects": False}
        if transport is not None:
            client_kwargs["transport"] = transport
        self._client = httpx.Client(**client_kwargs)
        self._lock = threading.RLock()
        _CLIENTS.add(self)

    def close(self) -> None:
        with self._lock:
            self._client.close()

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
        retry_count = self.retries if retries is None else max(0, int(retries or 0))
        last_error = ""
        for attempt in range(retry_count + 1):
            try:
                with self._lock:
                    response = self._client.request(
                        method,
                        url,
                        headers=headers,
                        params=params,
                        json=json_payload,
                    )
                if response.status_code in RETRY_STATUS_CODES and attempt < retry_count:
                    time.sleep(0.35 * (2**attempt) + random.random() * 0.2)
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
                    time.sleep(0.35 * (2**attempt) + random.random() * 0.2)
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
        retry_count = self.retries if retries is None else max(0, int(retries or 0))
        last_error = ""
        for attempt in range(retry_count + 1):
            try:
                with self._lock:
                    response = self._client.request(
                        method,
                        url,
                        headers=headers,
                        params=params,
                    )
                if response.status_code in RETRY_STATUS_CODES and attempt < retry_count:
                    time.sleep(0.35 * (2**attempt) + random.random() * 0.2)
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
                    time.sleep(0.35 * (2**attempt) + random.random() * 0.2)
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


def request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_payload: Any = None,
    timeout: httpx.Timeout = DEFAULT_TIMEOUT,
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

# -*- coding: utf-8 -*-
from __future__ import annotations

import random
import time
from typing import Any

import httpx


DEFAULT_TIMEOUT = httpx.Timeout(connect=3.0, read=15.0, write=60.0, pool=3.0)
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


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
    last_error = ""
    for attempt in range(max(1, int(retries or 0) + 1)):
        try:
            with httpx.Client(timeout=timeout, follow_redirects=False) as client:
                response = client.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json_payload,
                )
            if response.status_code in RETRY_STATUS_CODES and attempt < retries:
                time.sleep(0.35 * (2**attempt) + random.random() * 0.2)
                continue
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            raise FeishuHTTPError("接口返回不是 JSON 对象", category="business")
        except httpx.HTTPStatusError as exc:
            status = int(exc.response.status_code if exc.response else 0)
            last_error = str(exc)
            if status in RETRY_STATUS_CODES and attempt < retries:
                time.sleep(0.35 * (2**attempt) + random.random() * 0.2)
                continue
            raise FeishuHTTPError(
                last_error,
                category=classify_feishu_error(status_code=status),
            ) from exc
        except Exception as exc:
            last_error = str(exc)
            if attempt < retries:
                time.sleep(0.35 * (2**attempt) + random.random() * 0.2)
                continue
            if isinstance(exc, FeishuHTTPError):
                raise
            raise FeishuHTTPError(last_error, category="network") from exc
    raise FeishuHTTPError(last_error or "HTTP 请求失败", category="network")


# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import threading
import time
from typing import Any

from ..config import config
from ..logger import log_error, log_info
from .http_client import FeishuHTTPError, FeishuHttpClient, classify_feishu_error


TENANT_TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
APP_TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
USER_ACCESS_TOKEN_URL = "https://open.feishu.cn/open-apis/authen/v1/access_token"
USER_INFO_URL = "https://open.feishu.cn/open-apis/authen/v1/user_info"
TOKEN_REFRESH_MARGIN_SECONDS = 60
APP_TOKEN_TTL_FALLBACK_SECONDS = 7200
TOKEN_ERROR_CODES = {99991663, 99991664, 99991665, 99991668, 99991677}


class FeishuTokenError(RuntimeError):
    pass


class FeishuTokenManager:
    """Central token manager for Feishu tenant/app/user auth flows."""

    def __init__(self, http_client: FeishuHttpClient | None = None) -> None:
        self._http_client = http_client or FeishuHttpClient(retries=3)
        self._tenant_lock = threading.RLock()
        self._app_lock = threading.RLock()
        self._app_token_cache: dict[tuple[str, str], dict[str, Any]] = {}

    def tenant_token_refresh_needed(
        self, *, margin_seconds: int = TOKEN_REFRESH_MARGIN_SECONDS
    ) -> bool:
        if not str(config.user_token or "").strip():
            return True
        try:
            expire_time = int(float(config.token_expire_time or 0))
        except Exception:
            expire_time = 0
        if expire_time <= 0:
            return True
        return int(time.time()) + max(0, int(margin_seconds or 0)) >= expire_time

    def get_tenant_token(
        self,
        *,
        force_refresh: bool = False,
        margin_seconds: int = TOKEN_REFRESH_MARGIN_SECONDS,
    ) -> str:
        if not force_refresh and not self.tenant_token_refresh_needed(
            margin_seconds=margin_seconds
        ):
            return str(config.user_token or "").strip()
        with self._tenant_lock:
            if not force_refresh and not self.tenant_token_refresh_needed(
                margin_seconds=margin_seconds
            ):
                return str(config.user_token or "").strip()
            return self.refresh_tenant_token()

    def refresh_tenant_token(self) -> str:
        with self._tenant_lock:
            app_id = str(config.app_id or "").strip()
            app_secret = str(config.app_secret or "").strip()
            if not app_id or not app_secret:
                raise FeishuTokenError("未配置 App ID 或 App Secret")
            http_error = ""
            try:
                token, expire = self._request_tenant_token_http(app_id, app_secret)
            except Exception as exc:
                token, expire = "", 0
                http_error = str(exc)
            if not token:
                if http_error:
                    log_error(f"Token HTTP 刷新失败，尝试 SDK 兜底: {http_error}")
                token, expire = self._request_tenant_token_sdk(app_id, app_secret)
            return self._save_tenant_token(token, expire)

    def get_tenant_token_for_credentials(
        self, app_id: str, app_secret: str
    ) -> tuple[str, str]:
        app_id = str(app_id or "").strip()
        app_secret = str(app_secret or "").strip()
        if not app_id or not app_secret:
            return "", "missing app_id/app_secret"
        try:
            try:
                token, _expire = self._request_tenant_token_http(app_id, app_secret)
            except Exception:
                token, _expire = self._request_tenant_token_sdk(app_id, app_secret)
            if not token:
                return "", "empty tenant_access_token"
            return token, ""
        except Exception as exc:
            return "", str(exc)

    def get_app_access_token(self, *, force_refresh: bool = False) -> str:
        app_id = str(config.app_id or "").strip()
        app_secret = str(config.app_secret or "").strip()
        if not app_id or not app_secret:
            raise FeishuTokenError("未配置 App ID 或 App Secret")
        cache_key = (app_id, app_secret)
        now = time.time()
        with self._app_lock:
            cached = self._app_token_cache.get(cache_key) or {}
            token = str(cached.get("token") or "")
            expires_at = float(cached.get("expires_at") or 0)
            if not force_refresh and token and now < expires_at:
                return token

            payload = self._request_json(
                "POST",
                APP_TOKEN_URL,
                headers={"Content-Type": "application/json; charset=utf-8"},
                json_payload={"app_id": app_id, "app_secret": app_secret},
            )
            code = int(payload.get("code") or 0)
            if code != 0:
                raise FeishuTokenError(payload.get("msg") or f"code={code}")
            token = str(payload.get("app_access_token") or "").strip()
            if not token:
                raise FeishuTokenError("返回为空")
            expire = int(payload.get("expire") or APP_TOKEN_TTL_FALLBACK_SECONDS)
            self._app_token_cache[cache_key] = {
                "token": token,
                "expires_at": time.time() + expire - 300,
            }
            return token

    def exchange_login_code(self, code: str) -> dict[str, Any]:
        code = str(code or "").strip()
        if not code:
            raise FeishuTokenError("登录 code 为空")
        return self._exchange_login_code_once(code, force_refresh=False)

    def fetch_user_info(self, user_access_token: str) -> dict[str, Any]:
        user_access_token = str(user_access_token or "").strip()
        if not user_access_token:
            return {}
        try:
            payload = self._request_json(
                "GET",
                USER_INFO_URL,
                headers={"Authorization": f"Bearer {user_access_token}"},
                retries=2,
            )
        except Exception:
            return {}
        if int(payload.get("code") or 0) != 0:
            return {}
        data = payload.get("data")
        return dict(data) if isinstance(data, dict) else {}

    def _exchange_login_code_once(self, code: str, *, force_refresh: bool) -> dict[str, Any]:
        app_access_token = self.get_app_access_token(force_refresh=force_refresh)
        payload = self._request_json(
            "POST",
            USER_ACCESS_TOKEN_URL,
            headers={
                "Authorization": f"Bearer {app_access_token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json_payload={"grant_type": "authorization_code", "code": code},
            retries=2,
        )
        code_value = int(payload.get("code") or 0)
        if code_value in TOKEN_ERROR_CODES and not force_refresh:
            return self._exchange_login_code_once(code, force_refresh=True)
        if code_value != 0:
            raise FeishuTokenError(payload.get("msg") or f"code={code_value}")
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        user = dict(data)
        if not user.get("open_id") and user.get("access_token"):
            user.update(self.fetch_user_info(str(user.get("access_token") or "")))
        return user

    def _request_json(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        try:
            return self._http_client.request_json(*args, **kwargs)
        except FeishuHTTPError as exc:
            raise FeishuTokenError(str(exc)) from exc

    def _request_tenant_token_http(self, app_id: str, app_secret: str) -> tuple[str, int]:
        payload = self._request_json(
            "POST",
            TENANT_TOKEN_URL,
            headers={"Content-Type": "application/json; charset=utf-8"},
            json_payload={"app_id": app_id, "app_secret": app_secret},
            retries=3,
        )
        code = int(payload.get("code") or 0)
        if code != 0:
            category = classify_feishu_error(code=code)
            raise FeishuTokenError(f"{code} - {payload.get('msg') or category}")
        token = str(payload.get("tenant_access_token") or "").strip()
        if not token:
            raise FeishuTokenError("接口未返回 tenant_access_token")
        return token, int(payload.get("expire") or 7200)

    @staticmethod
    def _request_tenant_token_sdk(app_id: str, app_secret: str) -> tuple[str, int]:
        import lark_oapi as lark
        from lark_oapi.api.auth.v3 import (
            InternalTenantAccessTokenRequest,
            InternalTenantAccessTokenRequestBody,
        )

        client = lark.Client.builder().build()
        request = (
            InternalTenantAccessTokenRequest.builder()
            .request_body(
                InternalTenantAccessTokenRequestBody.builder()
                .app_id(app_id)
                .app_secret(app_secret)
                .build()
            )
            .build()
        )
        response = client.auth.v3.tenant_access_token.internal(request)
        if not response.success():
            raise FeishuTokenError(f"{response.code} - {response.msg}")
        data = json.loads(response.raw.content)
        token = str(data.get("tenant_access_token") or "").strip()
        if not token:
            raise FeishuTokenError("SDK 未返回 tenant_access_token")
        return token, int(data.get("expire") or 7200)

    @staticmethod
    def _save_tenant_token(new_token: str, expire_in: int | float | None = None) -> str:
        expire_seconds = int(expire_in or 7200)
        expire_time = int(time.time()) + expire_seconds - 300
        config.save(user_token=new_token, token_expire_time=expire_time)
        log_info(f"Token 刷新成功: {new_token[:10]}... (过期时间: {expire_time})")
        return new_token


token_manager = FeishuTokenManager()

# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import threading
import time
from typing import Any
from pathlib import Path

from ..config import config
from ..logger import log_error, log_info
from .http_client import FeishuHTTPError, FeishuHttpClient, classify_feishu_error
from ..utils import DATA_DIR


TENANT_TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
APP_TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
USER_ACCESS_TOKEN_URL = "https://open.feishu.cn/open-apis/authen/v1/access_token"
OAUTH_TOKEN_URL = "https://accounts.feishu.cn/oauth/v3/token"
USER_INFO_URL = "https://open.feishu.cn/open-apis/authen/v1/user_info"
TOKEN_REFRESH_MARGIN_SECONDS = 60
APP_TOKEN_TTL_FALLBACK_SECONDS = 7200
TOKEN_ERROR_CODES = {99991663, 99991664, 99991665, 99991668, 99991677}


class FeishuTokenError(RuntimeError):
    pass


class FeishuCredentialError(FeishuTokenError):
    def __init__(self):
        super().__init__("飞书应用密钥无效（10014 - app secret invalid）。请在当前程序的设置中更新 App Secret；重新扫码不能修复此配置错误。")


class FeishuTokenManager:
    """Central token manager for Feishu tenant/app/user auth flows."""

    def __init__(self, http_client: FeishuHttpClient | None = None) -> None:
        self._http_client = http_client or FeishuHttpClient(retries=3)
        self._tenant_lock = threading.RLock()
        self._app_lock = threading.RLock()
        self._app_token_cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._credential_lock = threading.RLock()
        self._rejected_credentials = None
        self._settings_stamp = None
        self._credential_identity = None

    def _credentials(self):
        refresher = getattr(config, 'refresh_credentials', None)
        loader = getattr(config, 'load', None)
        if callable(refresher):
            try:
                refresher()
            except Exception:
                raise FeishuTokenError("Qt 飞书鉴权暂时无法读取，未使用旧配置。") from None
        elif callable(loader):
            path = Path(DATA_DIR) / 'feishu_assistant.json'
            stamp = path.stat().st_mtime_ns if path.is_file() else 0
            if stamp != self._settings_stamp:
                # The derived agent file is a change signal; SQLite/Qt stays authoritative.
                with self._credential_lock:
                    if stamp != self._settings_stamp:
                        loader()
                        self._settings_stamp = stamp
        app_id, secret = str(config.app_id or '').strip(), str(config.app_secret or '').strip()
        identity = app_id, secret
        if self._credential_identity is not None and identity != self._credential_identity:
            config.user_token, config.token_expire_time = '', 0
            self._app_token_cache.clear()
        self._credential_identity = identity
        return app_id, secret

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
        self._credentials()
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
            app_id, app_secret = self._credentials()
            if not app_id or not app_secret:
                raise FeishuTokenError("未配置 App ID 或 App Secret")
            http_error = ""
            try:
                token, expire = self._request_tenant_token_http(app_id, app_secret)
            except FeishuCredentialError:
                raise
            except Exception as exc:
                token, expire = "", 0
                http_error = str(exc)
            if not token:
                if http_error:
                    log_error(f"Token HTTP 刷新失败，尝试 SDK 兜底: {http_error}")
                token, expire = self._request_tenant_token_sdk(app_id, app_secret)
            if self._credentials() != (app_id, app_secret):
                raise FeishuTokenError("飞书鉴权配置已变化，已丢弃原应用令牌，请重试。")
            return self._save_tenant_token(token, expire, expected_credentials=(app_id, app_secret))

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
            except FeishuCredentialError:
                raise
            except Exception:
                token, _expire = self._request_tenant_token_sdk(app_id, app_secret)
            if not token:
                return "", "empty tenant_access_token"
            return token, ""
        except Exception as exc:
            return "", str(exc)

    def get_app_access_token(self, *, force_refresh: bool = False) -> str:
        app_id, app_secret = self._credentials()
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
            if self._credentials() != cache_key:
                raise FeishuTokenError("飞书鉴权配置已变化，已丢弃原应用令牌，请重试。")
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

    def exchange_oauth_login_code(self, code: str, *, redirect_uri: str) -> dict[str, Any]:
        if not str(code or '').strip() or not redirect_uri:
            raise FeishuTokenError("登录授权码或回调地址为空")
        app_id, secret = self._credentials()
        payload = self._request_json(
            "POST", OAUTH_TOKEN_URL,
            headers={"Content-Type": "application/json; charset=utf-8"},
            json_payload={"grant_type": "authorization_code", "client_id": app_id,
                          "client_secret": secret, "code": code,
                          "redirect_uri": redirect_uri}, retries=1,
        )
        if self._credentials() != (app_id, secret):
            raise FeishuTokenError("登录期间飞书鉴权已变化，请重新扫码。")
        if payload.get('error') or int(payload.get('code') or 0) != 0:
            raise FeishuTokenError(payload.get('error_description') or payload.get('msg') or "飞书授权未完成")
        token = str(payload.get('access_token') or '').strip()
        if not token:
            raise FeishuTokenError("飞书授权未返回用户令牌")
        user = self.fetch_user_info(token)
        if self._credentials() != (app_id, secret):
            raise FeishuTokenError("登录期间飞书鉴权已变化，请重新扫码。")
        if not user.get('open_id'):
            raise FeishuTokenError("飞书授权未返回用户身份，请重新登录")
        return user

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
        credentials = self._credentials()
        app_access_token = self.get_app_access_token(force_refresh=force_refresh)
        if self._credentials() != credentials:
            raise FeishuTokenError("登录期间飞书鉴权已变化，请重新扫码。")
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
        if self._credentials() != credentials:
            raise FeishuTokenError("登录期间飞书鉴权已变化，请重新扫码。")
        code_value = int(payload.get("code") or 0)
        if code_value in TOKEN_ERROR_CODES and not force_refresh:
            return self._exchange_login_code_once(code, force_refresh=True)
        if code_value != 0:
            raise FeishuTokenError(payload.get("msg") or f"code={code_value}")
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        user = dict(data)
        if not user.get("open_id") and user.get("access_token"):
            user.update(self.fetch_user_info(str(user.get("access_token") or "")))
        if self._credentials() != credentials:
            raise FeishuTokenError("登录期间飞书鉴权已变化，请重新扫码。")
        return user

    def _request_json(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        try:
            if len(args) > 1 and args[1] in {TENANT_TOKEN_URL, APP_TOKEN_URL}:
                body = kwargs.get("json_payload") or {}
                credentials = (body.get("app_id"), body.get("app_secret"))
                # All auth callers share the rejection window; changed credentials retry immediately.
                with self._credential_lock:
                    rejected = self._rejected_credentials
                    if rejected and rejected[0] == credentials and time.monotonic() < rejected[1]:
                        raise FeishuCredentialError()
                    payload = self._http_client.request_json(*args, **kwargs)
                    if int(payload.get("code") or 0) == 10014:
                        self._rejected_credentials = (credentials, time.monotonic() + 60)
                        error = FeishuCredentialError()
                        log_error(str(error))
                        raise error
                    self._rejected_credentials = None
                    return payload
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
    def _save_tenant_token(new_token: str, expire_in: int | float | None = None, *, expected_credentials=None) -> str:
        expire_seconds = int(expire_in or 7200)
        expire_time = int(time.time()) + expire_seconds - 300
        saver = getattr(config, 'save_tenant_token', None)
        saved = (saver(new_token, expire_time, expected_credentials=expected_credentials) if callable(saver)
                 else config.save(user_token=new_token, token_expire_time=expire_time))
        if saved is False:
            raise FeishuTokenError("飞书鉴权已变化或令牌保存失败，请重试。")
        log_info(f"Token 刷新成功 (过期时间: {expire_time})")
        return new_token


token_manager = FeishuTokenManager()

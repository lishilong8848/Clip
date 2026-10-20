"""Admin channel switch; Feishu credentials are owned by Qt settings."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
import secrets
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse

from openclaw_service.assistant.lighthouse_ai import AssistantError, protect_key, unprotect_key
from openclaw_service.protocol import atomic_json


def read_config(path):
    if path is None:
        raise AssistantError("当前运行环境无法保存飞书智能体配置。", 503)
    from upload_event_module.utils import DATA_DIR
    from upload_event_module.config import config as qt_config
    from upload_event_module.services.feishu_app_migration import sync_agent_config
    canonical = Path(path).resolve() == (Path(DATA_DIR) / 'feishu_assistant.json').resolve()
    if canonical:
        try:
            qt_config.refresh_credentials()
            sync_agent_config(qt_config.app_id, qt_config.app_secret)
        except Exception:
            raise AssistantError('Qt 飞书鉴权同步尚未完成，未使用旧应用。', 503) from None
    raw = path.read_bytes() if path.exists() else b"{}"
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise AssistantError("飞书智能体配置文件格式不正确。", 503)
    if canonical and data.get('app_id'):
        try:
            secret_matches = hmac.compare_digest(unprotect_key(data.get('secret_cipher') or '').encode(),
                                                  qt_config.app_secret.encode())
        except Exception:
            secret_matches = False
        if data['app_id'] != qt_config.app_id or not secret_matches:
            raise AssistantError('飞书鉴权已变化，等待 Qt 配置同步；未使用旧应用。', 503)
    return data, hashlib.sha256(raw).hexdigest()


def validate_app_id(app_id):
    if not isinstance(app_id, str) or not re.fullmatch(r'cli_[A-Za-z0-9]{1,120}', app_id):
        raise AssistantError('飞书应用标识无效，请核对 Qt 设置中的飞书鉴权。')


def public_config(path, active):
    config, revision = read_config(path)
    return {"app_id": str(config.get("app_id") or ""), "enabled": config.get("enabled") is True,
            "has_secret": bool(config.get("secret_cipher")), "revision": revision,
            "restart_required": any(config.get(key) != active.get(key)
                for key in ("app_id", "enabled", "secret_cipher"))}


def save_config(path, payload, active):
    if not isinstance(payload, dict) or set(payload) - {"app_id", "app_secret", "enabled", "revision"}:
        raise AssistantError("配置内容无效。")
    from upload_event_module.utils import DATA_DIR
    if Path(path).resolve() == (Path(DATA_DIR) / 'feishu_assistant.json').resolve():
        from upload_event_module.services.feishu_app_migration import qt_settings_transaction, read_credentials
        if type(payload.get('enabled')) is not bool:
            raise AssistantError('启用状态必须为开关。')
        with qt_settings_transaction(DATA_DIR) as connection:
            credentials = read_credentials(connection)
            raw = path.read_bytes()
            config = json.loads(raw)
            if hashlib.sha256(raw).hexdigest() != payload.get('revision'):
                raise AssistantError('配置已变化，请重新读取后保存。', 409)
            secret = payload.get('app_secret', '')
            if (payload.get('app_id') != credentials['feishu_app_id'] or not isinstance(secret, str)
                    or secret and not hmac.compare_digest(secret.encode(), credentials['feishu_app_secret'].encode())):
                raise AssistantError('飞书鉴权统一取自 Qt 设置，请重新读取。', 409)
            try:
                matching = config.get('app_id') == credentials['feishu_app_id'] and hmac.compare_digest(
                    unprotect_key(config.get('secret_cipher') or '').encode(), credentials['feishu_app_secret'].encode())
            except Exception:
                matching = False
            if not matching:
                raise AssistantError('Qt 飞书鉴权正在同步，请重新读取后保存。', 409)
            atomic_json(path, {**config, 'enabled': payload['enabled']})
        return public_config(path, active)
    config, revision = read_config(path)
    if payload.get("revision") != revision:
        raise AssistantError("配置已被其他管理员修改，请重新读取后保存。", 409)
    app_id, secret = payload.get("app_id"), payload.get("app_secret", "")
    if not isinstance(app_id, str) or not re.fullmatch(r"cli_[A-Za-z0-9]{1,120}", app_id.strip()):
        raise AssistantError("请填写以 cli_ 开头的飞书应用 App ID。")
    if not isinstance(secret, str) or len(secret) > 500 or any(ord(char) < 32 for char in secret):
        raise AssistantError("App Secret 格式不正确。")
    if type(payload.get("enabled")) is not bool:
        raise AssistantError("启用状态必须为开关。")
    app_id, secret = app_id.strip(), secret.strip()
    if payload['enabled']:
        validate_app_id(app_id)
    changed_app = app_id != config.get("app_id")
    if not secret and (changed_app or not config.get("secret_cipher")):
        raise AssistantError("首次配置或更换 App ID 时必须填写对应的 App Secret。")
    if not secret:
        try:
            unprotect_key(config["secret_cipher"])
        except Exception:
            raise AssistantError("原密钥无法在本机读取，请重新填写 App Secret。") from None
    saved = {**config, "app_id": app_id, "enabled": payload["enabled"]}
    if secret:
        saved["secret_cipher"] = protect_key(secret)
    try:
        bridge_valid = bool(unprotect_key(saved.get("bridge_cipher") or ""))
    except Exception:
        bridge_valid = False
    if not bridge_valid or changed_app:
        saved["bridge_cipher"] = protect_key(secrets.token_urlsafe(32))
    if changed_app:
        # Old-app retries must never be delivered with the new app's credentials.
        saved["isolated_inbox"] = True
    atomic_json(path, saved)
    return public_config(path, active)


def install_feishu_assistant_settings(app, controller, runtime, channel):
    lock = asyncio.Lock()

    async def settings(request: Request):
        try:
            session = await asyncio.to_thread(controller._current_session, request)
            if not session:
                raise AssistantError("请先登录。", 401)
            if session.get("is_guest") or not runtime.auth_manager.is_admin(session):
                raise AssistantError("仅管理员可配置飞书智能体。", 403)
            async with lock:
                if request.method == "GET":
                    data = await asyncio.to_thread(public_config, channel.config_path, channel.config)
                else:
                    source = request.headers.get("origin") or request.headers.get("referer")
                    expected = urlsplit(controller._request_base_url(request))
                    actual = urlsplit(source) if source else None
                    if (not actual or (actual.scheme, actual.netloc) != (expected.scheme, expected.netloc)
                            or request.headers.get("sec-fetch-site", "").lower() == "cross-site"):
                        raise AssistantError("不允许跨来源提交。", 403)
                    body = bytearray()
                    async for chunk in request.stream():
                        body.extend(chunk)
                        if len(body) > 4096:
                            raise AssistantError("配置内容过大。", 413)
                    try:
                        payload = json.loads(body)
                    except (ValueError, UnicodeError, RecursionError):
                        raise AssistantError("配置格式无效。") from None
                    from upload_event_module.config import config as qt_config
                    await asyncio.to_thread(qt_config.load)
                    if (not isinstance(payload, dict) or payload.get('app_id') != qt_config.app_id
                            or payload.get('app_secret') and (not isinstance(payload['app_secret'], str)
                                or not hmac.compare_digest(payload['app_secret'].encode(), qt_config.app_secret.encode()))):
                        raise AssistantError('飞书应用统一取自 Qt 设置中的飞书鉴权，请在那里修改 App ID 和密钥。')
                    data = await asyncio.to_thread(save_config, channel.config_path, payload, channel.config)
            return JSONResponse({"ok": True, "data": data}, headers={"Cache-Control": "no-store"})
        except AssistantError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=exc.status,
                                headers={"Cache-Control": "no-store"})
        except Exception:
            return JSONResponse({"ok": False, "error": "飞书智能体配置读取或保存失败，请检查配置文件及本机密钥保护。"},
                                status_code=503, headers={"Cache-Control": "no-store"})

    app.add_api_route("/api/assistant/feishu-settings", settings, methods=["GET", "POST"])

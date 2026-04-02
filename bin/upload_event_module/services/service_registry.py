from __future__ import annotations

import importlib
from dataclasses import dataclass
from types import ModuleType

from ..logger import log_info


@dataclass
class ServiceSnapshot:
    feishu_module: ModuleType
    handlers_module: ModuleType


class ServiceRegistry:
    def __init__(self, feishu_module: ModuleType, handlers_module: ModuleType) -> None:
        self.feishu_module = feishu_module
        self.handlers_module = handlers_module

    def snapshot(self) -> ServiceSnapshot:
        return ServiceSnapshot(self.feishu_module, self.handlers_module)

    def restore(self, snapshot: ServiceSnapshot) -> None:
        self.feishu_module = snapshot.feishu_module
        self.handlers_module = snapshot.handlers_module

    def swap(self, new_registry: "ServiceRegistry") -> None:
        self.feishu_module = new_registry.feishu_module
        self.handlers_module = new_registry.handlers_module
        log_info("ServiceRegistry: 已切换到新服务")


def _load_feishu() -> ModuleType:
    return importlib.import_module("upload_event_module.services.feishu_service")


def _load_handlers() -> ModuleType:
    return importlib.import_module("upload_event_module.services.handlers")


def build_default_registry() -> ServiceRegistry:
    return ServiceRegistry(_load_feishu(), _load_handlers())


service_registry = build_default_registry()


def set_registry(new_registry: ServiceRegistry) -> None:
    global service_registry
    service_registry = new_registry


import threading

# 全局飞书 API 请求锁 (防止 SSL 多线程冲突)
_feishu_lock = threading.RLock()


def get_feishu_lock() -> threading.RLock:
    return _feishu_lock


def refresh_feishu_token():
    with _feishu_lock:
        return service_registry.feishu_module.refresh_feishu_token()


def resolve_bitable_app_token(app_id: str, app_secret: str, app_token: str):
    with _feishu_lock:
        return service_registry.feishu_module.resolve_bitable_app_token(
            app_id, app_secret, app_token
        )


def upload_media_to_feishu(image_bytes, file_name="screenshot.jpg", file_size=None):
    with _feishu_lock:
        return service_registry.feishu_module.upload_media_to_feishu(
            image_bytes, file_name=file_name, file_size=file_size
        )


def create_bitable_record(*args, **kwargs):
    with _feishu_lock:
        return service_registry.feishu_module.create_bitable_record(*args, **kwargs)


def create_bitable_record_by_payload(*args, **kwargs):
    with _feishu_lock:
        return service_registry.feishu_module.create_bitable_record_by_payload(
            *args, **kwargs
        )


def query_record_by_id(*args, **kwargs):
    with _feishu_lock:
        return service_registry.feishu_module.query_record_by_id(*args, **kwargs)


def update_bitable_record(*args, **kwargs):
    with _feishu_lock:
        return service_registry.feishu_module.update_bitable_record(*args, **kwargs)


def update_bitable_record_by_payload(*args, **kwargs):
    with _feishu_lock:
        return service_registry.feishu_module.update_bitable_record_by_payload(
            *args, **kwargs
        )


def update_bitable_record_fields(*args, **kwargs):
    with _feishu_lock:
        return service_registry.feishu_module.update_bitable_record_fields(
            *args, **kwargs
        )

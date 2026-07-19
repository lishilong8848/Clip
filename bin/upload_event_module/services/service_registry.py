from __future__ import annotations

import importlib
import threading
from dataclasses import dataclass
from types import ModuleType

from ..logger import log_info


class LazyServiceModule(ModuleType):
    """Module proxy that imports the real service on first attribute access."""

    def __init__(self, module_name: str) -> None:
        super().__init__(module_name)
        self.__dict__["_target_module_name"] = module_name
        self.__dict__["_target_module"] = None
        self.__dict__["_target_lock"] = threading.RLock()

    def _load(self) -> ModuleType:
        target = self.__dict__.get("_target_module")
        if isinstance(target, ModuleType):
            return target
        with self.__dict__["_target_lock"]:
            target = self.__dict__.get("_target_module")
            if not isinstance(target, ModuleType):
                target = importlib.import_module(self.__dict__["_target_module_name"])
                self.__dict__["_target_module"] = target
            return target

    def __getattr__(self, name: str):
        return getattr(self._load(), name)


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
    return ServiceRegistry(
        LazyServiceModule("upload_event_module.services.feishu_service"),
        LazyServiceModule("upload_event_module.services.handlers"),
    )


service_registry = build_default_registry()


def set_registry(new_registry: ServiceRegistry) -> None:
    global service_registry
    service_registry = new_registry


# 全局飞书 API 请求锁 (防止 SSL 多线程冲突)
_feishu_lock = threading.RLock()


def get_feishu_lock() -> threading.RLock:
    return _feishu_lock


def refresh_feishu_token():
    with _feishu_lock:
        return service_registry.feishu_module.refresh_feishu_token()


def check_token_status():
    with _feishu_lock:
        return service_registry.feishu_module.check_token_status()


def ensure_feishu_token(*args, **kwargs):
    with _feishu_lock:
        ensure_fn = getattr(service_registry.feishu_module, "ensure_feishu_token", None)
        if callable(ensure_fn):
            return ensure_fn(*args, **kwargs)
        return service_registry.feishu_module.check_token_status()


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


def create_bitable_record_fields(*args, **kwargs):
    with _feishu_lock:
        create_fn = getattr(service_registry.feishu_module, "create_bitable_record_fields", None)
        if callable(create_fn):
            return create_fn(*args, **kwargs)
        return False, "当前服务不支持按字段恢复创建记录"


def batch_create_bitable_records_by_payload(*args, **kwargs):
    with _feishu_lock:
        batch_fn = getattr(
            service_registry.feishu_module,
            "batch_create_bitable_records_by_payload",
            None,
        )
        if callable(batch_fn):
            return batch_fn(*args, **kwargs)
        notice_type = args[0] if args else kwargs.get("notice_type")
        payloads = args[1] if len(args) > 1 else kwargs.get("payloads")
        return [
            service_registry.feishu_module.create_bitable_record_by_payload(
                notice_type, payload
            )
            for payload in (payloads or [])
        ]


def query_record_by_id(*args, **kwargs):
    with _feishu_lock:
        return service_registry.feishu_module.query_record_by_id(*args, **kwargs)


def delete_bitable_record(*args, **kwargs):
    with _feishu_lock:
        return service_registry.feishu_module.delete_bitable_record(*args, **kwargs)


def update_bitable_record(*args, **kwargs):
    with _feishu_lock:
        return service_registry.feishu_module.update_bitable_record(*args, **kwargs)


def update_bitable_record_by_payload(*args, **kwargs):
    with _feishu_lock:
        return service_registry.feishu_module.update_bitable_record_by_payload(
            *args, **kwargs
        )


def batch_update_bitable_records_by_payload(*args, **kwargs):
    with _feishu_lock:
        batch_fn = getattr(
            service_registry.feishu_module,
            "batch_update_bitable_records_by_payload",
            None,
        )
        if callable(batch_fn):
            return batch_fn(*args, **kwargs)
        notice_type = args[0] if args else kwargs.get("notice_type")
        updates = args[1] if len(args) > 1 else kwargs.get("updates")
        return [
            service_registry.feishu_module.update_bitable_record_by_payload(
                record_id, notice_type, payload
            )
            for record_id, payload in (updates or [])
        ]


def update_bitable_record_fields(*args, **kwargs):
    with _feishu_lock:
        return service_registry.feishu_module.update_bitable_record_fields(
            *args, **kwargs
        )

import importlib
import sys
from pathlib import Path
from typing import Iterable

from ..logger import log_error, log_info
from ..services.service_registry import ServiceRegistry, ServiceSnapshot
from .utils import path_to_module


class ReloadTransaction:
    def __init__(self, registry: ServiceRegistry, module_names: Iterable[str]) -> None:
        self.registry = registry
        self.module_names = list(dict.fromkeys(module_names))
        self.snapshot: ServiceSnapshot | None = None
        self.reloaded = []

    def execute(self) -> None:
        self.snapshot = self.registry.snapshot()
        importlib.invalidate_caches()

        reloaded_modules = {}
        for name in self.module_names:
            try:
                if name in sys.modules:
                    reloaded_modules[name] = importlib.reload(sys.modules[name])
                else:
                    reloaded_modules[name] = importlib.import_module(name)
                self.reloaded.append(name)
            except Exception as exc:
                log_error(f"HotReload: reload module failed {name}: {exc}")
                raise

        feishu_module = reloaded_modules.get(
            "upload_event_module.services.feishu_service",
            self.registry.feishu_module,
        )
        handlers_module = reloaded_modules.get(
            "upload_event_module.services.handlers",
            self.registry.handlers_module,
        )

        new_registry = ServiceRegistry(feishu_module, handlers_module)
        self.registry.swap(new_registry)
        log_info(f"HotReload: reloaded modules: {', '.join(self.reloaded)}")

    def rollback(self) -> None:
        if self.snapshot:
            self.registry.restore(self.snapshot)
            log_info("HotReload: rollback completed")


def resolve_modules_from_paths(paths: Iterable[Path]) -> list[str]:
    modules = []
    for path in paths:
        name = path_to_module(path)
        if name:
            modules.append(name)
    # Always reload handler package to refresh HANDLER_CLASSES
    modules.append("upload_event_module.services.handlers")
    # And feishu service to rebind handler imports
    modules.append("upload_event_module.services.feishu_service")
    return list(dict.fromkeys(modules))

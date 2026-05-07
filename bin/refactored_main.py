import sys
import os
import faulthandler
from pathlib import Path

from upload_event_module.hot_reload.state_store import get_user_data_dir
from upload_event_module.utils import migrate_runtime_data_files
from upload_event_module.services.dependency_bootstrap import (
    DEFAULT_MIRRORS,
    DEFAULT_MODULE_TO_PACKAGE,
    DEFAULT_WINDOWS_MODULE_TO_PACKAGE,
    ensure_runtime_dependencies,
)
from upload_event_module.services.system_alert_webhook import send_system_alert

# Force software rendering on Windows to avoid GPU/driver-related access violations.
if sys.platform == "win32":
    os.environ.setdefault("QT_OPENGL", "software")

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication
from PyQt6.QtNetwork import QLocalServer, QLocalSocket
from upload_event_module.hot_reload.restart_guard import RestartGuard
from upload_event_module.config import (
    HOT_RELOAD_SAFE_MODE_MAX_RESTARTS,
    HOT_RELOAD_SAFE_MODE_WINDOW_S,
    config,
)
from lan_bitable_template_portal.server import PortalServerController

_CRASH_TRACE_FP = None


def _init_crash_trace():
    global _CRASH_TRACE_FP
    try:
        if faulthandler.is_enabled():
            return
    except Exception:
        pass
    try:
        trace_path = get_user_data_dir() / "crash_trace.log"
        try:
            trace_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        _CRASH_TRACE_FP = open(trace_path, "a", encoding="utf-8")
        _CRASH_TRACE_FP.write("\n=== Crash Trace Session Start ===\n")
        _CRASH_TRACE_FP.flush()
        faulthandler.enable(file=_CRASH_TRACE_FP, all_threads=True)
        # Periodic traceback dump to catch deadlocks/stalls
        faulthandler.dump_traceback_later(30, repeat=True, file=_CRASH_TRACE_FP)
    except Exception:
        _CRASH_TRACE_FP = None
        try:
            fallback = Path.cwd() / "crash_trace.log"
            _CRASH_TRACE_FP = open(fallback, "a", encoding="utf-8")
            _CRASH_TRACE_FP.write("\n=== Crash Trace Session Start (fallback) ===\n")
            _CRASH_TRACE_FP.flush()
            faulthandler.enable(file=_CRASH_TRACE_FP, all_threads=True)
            faulthandler.dump_traceback_later(30, repeat=True, file=_CRASH_TRACE_FP)
            print(f"[ClipFlow] crash_trace fallback: {fallback}")
        except Exception as exc:
            _CRASH_TRACE_FP = None
            print(f"[ClipFlow] crash_trace init failed: {exc}")


def _migrate_runtime_data():
    try:
        outcomes = migrate_runtime_data_files()
    except Exception as exc:
        print(f"[ClipFlow] runtime data migration failed: {exc}")
        return

    counts = {
        "moved": 0,
        "conflict_renamed": 0,
        "skipped": 0,
        "failed": 0,
    }
    for item in outcomes:
        status = item.get("status", "skipped")
        if status in counts:
            counts[status] += 1
        else:
            counts["skipped"] += 1

    print(
        "[ClipFlow] RuntimeData migration summary: "
        f"moved={counts['moved']} "
        f"conflict={counts['conflict_renamed']} "
        f"skipped={counts['skipped']} "
        f"failed={counts['failed']}"
    )
    if counts["failed"] > 0:
        failed_items = [
            f"{x.get('filename')}:{x.get('error')}"
            for x in outcomes
            if x.get("status") == "failed"
        ]
        if failed_items:
            print(f"[ClipFlow] RuntimeData migration failures: {' | '.join(failed_items)}")


def _startup_dependency_healthcheck():
    """Fast dependency health-check at startup: install only missing packages."""
    if not bool(getattr(config, "auto_install_dependencies", True)):
        print("[ClipFlow] Startup dep check skipped (auto_install_dependencies=false).")
        return
    python_exe = Path(sys.executable)
    manifest = _build_startup_dep_manifest()
    ok, detail = ensure_runtime_dependencies(
        manifest,
        python_exe,
        mirrors=getattr(config, "dependency_mirrors", list(DEFAULT_MIRRORS)),
        timeout_seconds=int(getattr(config, "dependency_install_timeout_seconds", 20)),
        retries_per_mirror=int(
            getattr(config, "dependency_install_retries_per_mirror", 1)
        ),
        allow_get_pip=bool(
            getattr(config, "dependency_bootstrap_allow_get_pip", True)
        ),
    )
    if ok:
        print(f"[ClipFlow] Startup dep check ok: {detail}")
    else:
        print(f"[ClipFlow] Startup dep check failed: {detail}")
        send_system_alert(
            event_code="dep.startup.install_failed",
            title="启动依赖检查失败",
            detail=detail,
            dedup_key="startup_dep_check",
        )


def _build_startup_dep_manifest() -> dict:
    module_map = dict(DEFAULT_MODULE_TO_PACKAGE)
    if sys.platform == "win32":
        module_map.update(DEFAULT_WINDOWS_MODULE_TO_PACKAGE)
    return {
        "module_to_package": module_map,
        # Empty list means "missing only" in dependency bootstrap.
        "required_packages": [],
    }

# 单实例标识符
SINGLE_INSTANCE_KEY = "ClipFlow_SingleInstance_Lock"


def is_already_running():
    """检查程序是否已经在运行"""
    # 使用 QLocalSocket 尝试连接已存在的实例
    socket = QLocalSocket()
    socket.connectToServer(SINGLE_INSTANCE_KEY)
    if socket.waitForConnected(500):
        # 已有实例在运行
        socket.disconnectFromServer()
        return True
    return False


def main():
    _migrate_runtime_data()
    _init_crash_trace()
    _startup_dependency_healthcheck()
    if "--clear-guard" in sys.argv:
        RestartGuard(
            window_seconds=HOT_RELOAD_SAFE_MODE_WINDOW_S,
            max_restarts=HOT_RELOAD_SAFE_MODE_MAX_RESTARTS,
        ).clear()
        sys.argv.remove("--clear-guard")

    if "--safe-mode" in sys.argv:
        os.environ["CLIPFLOW_SAFE_MODE"] = "1"
        sys.argv.remove("--safe-mode")

    if sys.platform == "win32":
        QApplication.setAttribute(Qt.ApplicationAttribute.AA_UseSoftwareOpenGL)

    app = QApplication(sys.argv)

    # 单实例检测
    if is_already_running():
        print("[ClipFlow] 程序已在运行，退出重复实例")
        sys.exit(0)

    # 创建本地服务器，供后续实例检测
    server = QLocalServer()
    # 清理可能存在的旧连接
    QLocalServer.removeServer(SINGLE_INSTANCE_KEY)
    if not server.listen(SINGLE_INSTANCE_KEY):
        print(f"[ClipFlow] 无法创建单实例锁: {server.errorString()}")

    from upload_event_module.ui.main_window import ClipboardTool

    portal_controller = None
    try:
        portal_controller = PortalServerController(
            host=getattr(config, "lan_template_portal_host", "0.0.0.0"),
            port=int(getattr(config, "lan_template_portal_port", 18766) or 18766),
        )
        portal_url = portal_controller.start()
        print(f"[ClipFlow] 局域网模板门户已随主程序启动: {portal_url}")
        app.aboutToQuit.connect(portal_controller.stop)
    except Exception as exc:
        portal_controller = None
        print(f"[ClipFlow] 局域网模板门户启动失败: {exc}")

    window = ClipboardTool()
    if portal_controller is not None:
        try:
            window.lan_template_portal_controller = portal_controller
            window.lan_template_portal_url = portal_controller.get_url()
            portal_controller.set_notice_callback(window.enqueue_lan_template_notice)
            portal_controller.set_ongoing_callback(
                window.get_lan_maintenance_ongoing_notices
            )
            portal_controller.set_maintenance_action_callback(
                window.enqueue_lan_maintenance_action
            )
            window.refresh_lan_template_portal_link()
        except Exception:
            pass

    # 设置初始位置 (添加空值检查防止访问违规)
    primary_screen = app.primaryScreen()
    if primary_screen:
        screen = primary_screen.geometry()
        window.move(screen.width() - 600, screen.height() - 790)
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()

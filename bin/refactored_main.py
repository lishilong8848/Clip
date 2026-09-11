import sys
import os
import faulthandler
import subprocess
import threading
import time
from pathlib import Path

_PROCESS_IMPORT_STARTED_AT = time.perf_counter()


def _reexec_with_project_python():
    if __name__ != "__main__" or getattr(sys, "frozen", False):
        return
    candidate = Path(__file__).resolve().parent / ".venv" / "Scripts" / "python.exe"
    if not candidate.is_file():
        return
    try:
        if candidate.samefile(Path(sys.executable)):
            return
    except OSError:
        if os.path.normcase(str(candidate.resolve())) == os.path.normcase(
            str(Path(sys.executable).resolve())
        ):
            return
    argv = [str(candidate), str(Path(__file__).resolve()), *sys.argv[1:]]
    if sys.platform == "win32":
        print(f"[ClipFlow] Switching to project Python: {candidate}", flush=True)
        raise SystemExit(subprocess.call(argv))
    os.execv(str(candidate), argv)


_reexec_with_project_python()

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


# The desktop app is the trusted operator entrypoint for real Feishu/Bitable
# writes. Direct backend/server scripts still require an explicit confirmation.
os.environ.setdefault("CLIPFLOW_REQUIRE_REAL_EXTERNAL_CONFIRM", "1")
os.environ.setdefault("CLIPFLOW_REAL_EXTERNAL_CONFIRMED", "1")

from PyQt6.QtCore import Qt, QObject, QTimer, pyqtSignal
from PyQt6.QtWidgets import QApplication
from PyQt6.QtNetwork import QLocalServer, QLocalSocket
from upload_event_module.hot_reload.restart_guard import RestartGuard
from upload_event_module.config import (
    HOT_RELOAD_SAFE_MODE_MAX_RESTARTS,
    HOT_RELOAD_SAFE_MODE_WINDOW_S,
    config,
)

from clipflow_backend.process_controller import (
    BackendProcessPortalController as PortalServerController,
)

_CRASH_TRACE_FP = None


class _PortalStartupBridge(QObject):
    finished = pyqtSignal(object, str)
    bootstrap_finished = pyqtSignal(object, str)


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
        # Periodic traceback dumps are useful for diagnosing freezes, but they
        # write every thread stack to disk and can make low-power machines feel
        # stuck during normal operation. Keep them opt-in for production builds.
        if os.environ.get("CLIPFLOW_ENABLE_PERIODIC_TRACEBACK") == "1":
            faulthandler.dump_traceback_later(300, repeat=True, file=_CRASH_TRACE_FP)
    except Exception:
        _CRASH_TRACE_FP = None
        try:
            fallback = Path.cwd() / "crash_trace.log"
            _CRASH_TRACE_FP = open(fallback, "a", encoding="utf-8")
            _CRASH_TRACE_FP.write("\n=== Crash Trace Session Start (fallback) ===\n")
            _CRASH_TRACE_FP.flush()
            faulthandler.enable(file=_CRASH_TRACE_FP, all_threads=True)
            if os.environ.get("CLIPFLOW_ENABLE_PERIODIC_TRACEBACK") == "1":
                faulthandler.dump_traceback_later(300, repeat=True, file=_CRASH_TRACE_FP)
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


def _wait_for_previous_instance(timeout_seconds=20.0):
    deadline = time.monotonic() + max(0.0, float(timeout_seconds or 0))
    while is_already_running():
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.2)
    return True


def main():
    print(
        "[ClipFlow] Main module import elapsed: "
        f"{(time.perf_counter() - _PROCESS_IMPORT_STARTED_AT) * 1000:.1f} ms"
    )
    stage_started_at = time.perf_counter()
    _migrate_runtime_data()
    print(
        "[ClipFlow] Runtime data migration elapsed: "
        f"{(time.perf_counter() - stage_started_at) * 1000:.1f} ms"
    )
    stage_started_at = time.perf_counter()
    _init_crash_trace()
    print(
        "[ClipFlow] Crash trace init elapsed: "
        f"{(time.perf_counter() - stage_started_at) * 1000:.1f} ms"
    )
    stage_started_at = time.perf_counter()
    _startup_dependency_healthcheck()
    print(
        "[ClipFlow] Dependency check elapsed: "
        f"{(time.perf_counter() - stage_started_at) * 1000:.1f} ms"
    )
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

    stage_started_at = time.perf_counter()
    app = QApplication(sys.argv)
    print(
        "[ClipFlow] QApplication init elapsed: "
        f"{(time.perf_counter() - stage_started_at) * 1000:.1f} ms"
    )

    wait_for_previous = "--wait-for-previous-instance" in sys.argv
    if wait_for_previous:
        sys.argv.remove("--wait-for-previous-instance")

    # 旧版更新器不会传等待参数；普通启动也给正在退出的上一实例一个
    # 短暂宽限期，确保首个修复补丁可以完成接管。
    if is_already_running():
        timeout_seconds = 20.0 if wait_for_previous else 10.0
        if not _wait_for_previous_instance(timeout_seconds):
            if wait_for_previous:
                print("[ClipFlow] 等待旧程序退出超时，请关闭旧程序后重新启动")
                sys.exit(1)
            print("[ClipFlow] 程序已在运行，退出重复实例")
            sys.exit(0)

    # 创建本地服务器，供后续实例检测
    server = QLocalServer()
    # 清理可能存在的旧连接
    QLocalServer.removeServer(SINGLE_INSTANCE_KEY)
    if not server.listen(SINGLE_INSTANCE_KEY):
        print(f"[ClipFlow] 无法创建单实例锁: {server.errorString()}")

    portal_holder = {"controller": None, "error": ""}
    portal_holder_lock = threading.Lock()
    portal_ready = threading.Event()
    portal_cancelled = threading.Event()

    def _start_portal_worker():
        started_at = time.perf_counter()
        controller = None
        error = ""
        try:
            controller = PortalServerController(
                host=getattr(config, "lan_template_portal_host", "0.0.0.0"),
                port=int(getattr(config, "lan_template_portal_port", 18766) or 18766),
            )
            controller.start()
            with portal_holder_lock:
                should_stop = portal_cancelled.is_set()
                if not should_stop:
                    portal_holder["controller"] = controller
            if should_stop:
                controller.stop()
                controller = None
        except Exception as exc:
            error = str(exc)
            controller = None
        with portal_holder_lock:
            portal_holder["controller"] = controller
            portal_holder["error"] = error
        portal_ready.set()
        print(
            "[ClipFlow] Portal startup elapsed: "
            f"{(time.perf_counter() - started_at) * 1000:.1f} ms"
        )

    portal_thread = threading.Thread(
        target=_start_portal_worker,
        name="ClipFlowPortalStartup",
        daemon=True,
    )
    portal_thread.start()

    import_started_at = time.perf_counter()
    try:
        from upload_event_module.ui.main_window import ClipboardTool
    except BaseException:
        portal_cancelled.set()
        portal_ready.wait(35.0)
        raise
    print(
        "[ClipFlow] Qt window import elapsed: "
        f"{(time.perf_counter() - import_started_at) * 1000:.1f} ms"
    )

    window_started_at = time.perf_counter()
    try:
        window = ClipboardTool()
    except BaseException:
        portal_cancelled.set()
        portal_ready.wait(35.0)
        raise
    print(
        "[ClipFlow] Qt window init elapsed: "
        f"{(time.perf_counter() - window_started_at) * 1000:.1f} ms"
    )

    portal_bridge = _PortalStartupBridge()

    def _stop_portal_if_started():
        portal_cancelled.set()
        with portal_holder_lock:
            controller = portal_holder.get("controller")
        if controller is None:
            return
        try:
            controller.stop()
        except Exception:
            pass

    app.aboutToQuit.connect(_stop_portal_if_started)

    def _finish_qt_shell_bootstrap(payload, error: str = ""):
        controller = portal_holder.get("controller")
        if controller is None:
            return
        if error:
            print(f"[ClipFlow] Qt shell bootstrap 加载失败: {error}")
        else:
            try:
                window.apply_qt_shell_bootstrap(payload)
            except Exception as exc:
                print(f"[ClipFlow] Qt shell bootstrap 应用失败: {exc}")
        if hasattr(controller, "set_shell_event_callback") and hasattr(
            window, "handle_qt_shell_event"
        ):
            controller.set_shell_event_callback(
                window.handle_qt_shell_event,
                initial_sync=False,
            )

    portal_bridge.bootstrap_finished.connect(_finish_qt_shell_bootstrap)

    def _attach_portal_controller(controller, error: str = ""):
        if controller is None:
            print(f"[ClipFlow] 局域网模板门户启动失败: {error}")
            try:
                window.lan_template_portal_url = (
                    window._build_lan_template_portal_url_from_config()
                )
                window.refresh_lan_template_portal_link()
            except Exception:
                pass
            return
        try:
            window.lan_template_portal_controller = controller
            window.lan_template_portal_url = controller.get_url()
            controller.set_notice_callback(window.enqueue_lan_template_notice)
            controller.set_ongoing_callback(
                window.get_lan_maintenance_ongoing_notices
            )
            if hasattr(controller, "set_ongoing_delete_callback"):
                controller.set_ongoing_delete_callback(
                    window.delete_lan_ongoing_notice
                )
            controller.set_maintenance_action_callback(
                window.enqueue_lan_maintenance_action
            )
            if hasattr(controller, "get_qt_shell_bootstrap") and hasattr(
                window, "apply_qt_shell_bootstrap"
            ):
                def _load_qt_shell_bootstrap():
                    last_error = ""
                    for attempt in range(2):
                        try:
                            payload = controller.get_qt_shell_bootstrap()
                            portal_bridge.bootstrap_finished.emit(payload, "")
                            return
                        except Exception as exc:
                            last_error = str(exc)
                            if attempt == 0:
                                time.sleep(0.5)
                    portal_bridge.bootstrap_finished.emit({}, last_error)

                bootstrap_thread = threading.Thread(
                    target=_load_qt_shell_bootstrap,
                    name="ClipFlowQtShellBootstrap",
                    daemon=True,
                )
                window._qt_shell_bootstrap_thread = bootstrap_thread
                bootstrap_thread.start()
            elif hasattr(controller, "set_shell_event_callback") and hasattr(
                window, "handle_qt_shell_event"
            ):
                controller.set_shell_event_callback(window.handle_qt_shell_event)
            if hasattr(window, "refresh_clipboard_backend_url"):
                try:
                    window.refresh_clipboard_backend_url()
                except Exception as exc:
                    print(f"[ClipFlow] 剪贴板后端地址刷新失败: {exc}")
            window.refresh_lan_template_portal_link()
            print(f"[ClipFlow] 局域网模板门户已随主程序启动: {controller.get_url()}")
            lan_urls = (
                list(controller.get_lan_urls() or [])
                if hasattr(controller, "get_lan_urls")
                else []
            )
            if lan_urls:
                print(f"[ClipFlow] 同一局域网访问地址: {', '.join(lan_urls)}")
            else:
                print("[ClipFlow] 未识别到可用的本机局域网 IPv4 地址。")
        except Exception as exc:
            print(f"[ClipFlow] 局域网模板门户回调绑定失败: {exc}")

    portal_bridge.finished.connect(_attach_portal_controller)
    window._portal_startup_bridge = portal_bridge

    def _relay_portal_startup_result():
        portal_ready.wait()
        portal_bridge.finished.emit(
            portal_holder.get("controller"),
            str(portal_holder.get("error") or ""),
        )

    portal_relay_thread = threading.Thread(
        target=_relay_portal_startup_result,
        name="ClipFlowPortalStartupRelay",
        daemon=True,
    )
    window._portal_startup_thread = portal_thread
    window._portal_startup_relay_thread = portal_relay_thread

    # 设置初始位置 (添加空值检查防止访问违规)
    primary_screen = app.primaryScreen()
    if primary_screen:
        screen = primary_screen.geometry()
        window.move(screen.width() - 600, screen.height() - 790)
    show_started_at = time.perf_counter()
    window.show()
    print(
        "[ClipFlow] Qt window show elapsed: "
        f"{(time.perf_counter() - show_started_at) * 1000:.1f} ms"
    )
    QTimer.singleShot(
        0,
        lambda: print(
            "[ClipFlow] Qt first event-loop turn elapsed: "
            f"{(time.perf_counter() - show_started_at) * 1000:.1f} ms"
        ),
    )
    portal_relay_thread.start()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()

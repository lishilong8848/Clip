import os
import sys
import threading
import subprocess
import time
import shutil
from pathlib import Path

from PyQt6.QtCore import QObject, Qt, QTimer

from ..config import (
    HOT_RELOAD_DEBOUNCE_MS,
    HOT_RELOAD_RELOAD_PATH_HINTS,
    HOT_RELOAD_SAFE_MODE_MAX_RESTARTS,
    HOT_RELOAD_SAFE_MODE_WINDOW_S,
)
from ..logger import log_error, log_info
from ..services.service_registry import service_registry
from .change_batcher import ChangeBatch, ChangeBatcher, Decision
from .commands import ReloadCommand, RestartCommand, ShowErrorCommand
from .dispatcher import MainThreadDispatcher
from .restart_guard import RestartGuard
from .state_store import (
    build_state_payload,
    cleanup_state,
    get_user_data_dir,
    write_state_atomic,
    read_state_safe,
)
from .transaction import ReloadTransaction, resolve_modules_from_paths
from .watcher import HotReloadWatcher


class HotReloadManager(QObject):
    def __init__(self, project_root: Path, ui_host: QObject | None = None) -> None:
        super().__init__()
        self.project_root = project_root
        self.ui_host = ui_host
        self.dispatcher = MainThreadDispatcher()
        self.dispatcher.command_posted.connect(
            self._handle_command, Qt.ConnectionType.QueuedConnection
        )

        self.restart_guard = RestartGuard(
            window_seconds=HOT_RELOAD_SAFE_MODE_WINDOW_S,
            max_restarts=HOT_RELOAD_SAFE_MODE_MAX_RESTARTS,
        )
        self.safe_mode = (
            bool(os.environ.get("CLIPFLOW_SAFE_MODE"))
            or self.restart_guard.should_enter_safe_mode()
        )

        self.batcher = ChangeBatcher(
            debounce_ms=HOT_RELOAD_DEBOUNCE_MS,
            classifier=self._classify_paths,
            on_batch=self._on_batch_ready,
        )
        self.watcher = HotReloadWatcher(project_root, self.batcher)
        self.state_path = get_user_data_dir() / "hot_reload_state.json"
        self._started = False

    def _show_hot_reload_overlay(self, text: str = "更新中...") -> None:
        if not self.ui_host:
            return
        try:
            if hasattr(self.ui_host, "update_overlay_label"):
                self.ui_host.update_overlay_label.setText(text)
            if hasattr(self.ui_host, "_show_update_overlay"):
                self.ui_host._show_update_overlay()
        except Exception as exc:
            log_error(f"HotReload: 显示更新遮罩失败: {exc}")

    def _hide_hot_reload_overlay(self) -> None:
        if not self.ui_host:
            return
        try:
            if hasattr(self.ui_host, "_hide_update_overlay"):
                self.ui_host._hide_update_overlay()
        except Exception as exc:
            log_error(f"HotReload: 隐藏更新遮罩失败: {exc}")

    def _has_patch_only_folder(self) -> bool:
        bin_dir = self.project_root / "bin"
        if not bin_dir.exists():
            return False
        try:
            for child in bin_dir.iterdir():
                if child.is_dir() and "_patch_only" in child.name:
                    return True
        except Exception:
            return False
        return False

    def start(self) -> None:
        if self._started:
            return
        if not self._has_patch_only_folder():
            log_info("HotReload: 未检测到 *_patch_only 补丁目录，Watcher 不启动")
            return
        cleanup_state(self.state_path)
        if self.safe_mode:
            log_info("HotReload: 安全模式已启用，Watcher 不自动启动")
            self._show_error(
                "热更新安全模式",
                "检测到频繁重启，已进入安全模式。请手动修复后重启或清空重启计数。",
            )
            return
        self.batcher.resume()
        self.watcher.start()
        self._started = True

    def stop(self) -> None:
        self.batcher.shutdown()
        self.watcher.stop()
        self._started = False

    def _on_batch_ready(self, batch: ChangeBatch) -> None:
        if batch.decision == Decision.IGNORE:
            return
        if batch.decision == Decision.RELOAD:
            self.dispatcher.post(ReloadCommand(batch))
        elif batch.decision == Decision.RESTART:
            self.dispatcher.post(RestartCommand(batch))

    def _collect_patch_only_dirs(self, paths: set[Path]) -> list[Path]:
        """从变更路径中提取 bin/*_patch_only 目录。"""
        bin_root = (self.project_root / "bin").resolve()
        result: dict[str, Path] = {}
        for path in paths:
            try:
                resolved = path.resolve()
                rel = resolved.relative_to(bin_root)
            except Exception:
                continue
            if not rel.parts:
                continue
            patch_dir_name = rel.parts[0]
            if "_patch_only" not in patch_dir_name:
                continue
            patch_dir = bin_root / patch_dir_name
            if patch_dir.exists() and patch_dir.is_dir():
                result[str(patch_dir)] = patch_dir
        return list(result.values())

    def _cleanup_patch_only_dirs(self, paths: set[Path]) -> None:
        patch_dirs = self._collect_patch_only_dirs(paths)
        for patch_dir in patch_dirs:
            removed = False
            last_exc = None
            for _ in range(3):
                try:
                    shutil.rmtree(patch_dir)
                    removed = True
                    break
                except Exception as exc:
                    last_exc = exc
                    time.sleep(0.2)
            if removed:
                log_info(f"HotReload: 已清理补丁目录 {patch_dir.name}")
            elif last_exc:
                log_error(f"HotReload: 清理补丁目录失败 {patch_dir}: {last_exc}")

    def _classify_paths(self, paths: set[Path]) -> tuple[Decision, str]:
        reload_paths = []
        restart_paths = []
        for path in paths:
            if path.suffix.lower() != ".py":
                continue
            norm = str(path).replace("\\", "/").lower()
            if any(
                hint.replace("\\", "/").lower() in norm
                for hint in HOT_RELOAD_RELOAD_PATH_HINTS
            ):
                reload_paths.append(path)
            else:
                restart_paths.append(path)

        if restart_paths and reload_paths:
            return Decision.RESTART, "升级: 同批次包含需要重启与可重载文件"
        if restart_paths:
            return Decision.RESTART, "Python 变更默认重启"
        if reload_paths:
            return Decision.RELOAD, "仅 handlers/config 变更"
        return Decision.IGNORE, "无可处理的变更"

    def _handle_command(self, command: object) -> None:
        thread_name = threading.current_thread().name
        log_info(f"HotReload: handle command on thread={thread_name}")

        if isinstance(command, ShowErrorCommand):
            self._show_error(command.title, command.detail)
            return
        if self.safe_mode:
            log_info("HotReload: 安全模式中，忽略热更新指令")
            if isinstance(command, (ReloadCommand, RestartCommand)):
                self._cleanup_patch_only_dirs(command.batch.paths)
            return
        if isinstance(command, ReloadCommand):
            self._handle_reload(command.batch)
        elif isinstance(command, RestartCommand):
            self._handle_restart(command.batch)

    def _precompile(self, paths: set[Path]) -> tuple[bool, str]:
        import py_compile

        for path in paths:
            if path.suffix.lower() != ".py":
                continue
            try:
                py_compile.compile(str(path), doraise=True)
            except Exception as exc:
                return False, f"预编译失败: {path}\n{exc}"
        return True, ""

    def _handle_reload(self, batch: ChangeBatch) -> None:
        self._show_hot_reload_overlay("热更新中...")
        try:
            ok, err = self._precompile(batch.paths)
            if not ok:
                self._handle_command(ShowErrorCommand("热重载失败", err))
                return

            modules = resolve_modules_from_paths(batch.paths)
            tx = ReloadTransaction(service_registry, modules)
            tx.execute()
            log_info("HotReload: reload completed")
        except Exception as exc:
            try:
                tx.rollback()
            except Exception:
                pass
            self._handle_command(ShowErrorCommand("热重载失败", str(exc)))
        finally:
            self._hide_hot_reload_overlay()
            self._cleanup_patch_only_dirs(batch.paths)

    def _handle_restart(self, batch: ChangeBatch) -> None:
        self._show_hot_reload_overlay("更新中...")
        should_restart = False
        try:
            ok, err = self._precompile(batch.paths)
            if not ok:
                self._handle_command(ShowErrorCommand("重启被阻止", err))
                return

            restart_count = self.restart_guard.record_restart()
            if restart_count > HOT_RELOAD_SAFE_MODE_MAX_RESTARTS:
                self.safe_mode = True
                self._handle_command(
                    ShowErrorCommand(
                        "进入安全模式",
                        "检测到频繁重启，已进入安全模式并停止自动重启。",
                    )
                )
                return
            should_restart = True
        finally:
            self._cleanup_patch_only_dirs(batch.paths)
            if should_restart:
                log_info("HotReload: restarting application")
                QTimer.singleShot(180, self._restart_application)
            else:
                self._hide_hot_reload_overlay()

    def _is_restart_overlay_ready(self) -> bool:
        try:
            state_path = get_user_data_dir() / "restart_overlay_state.json"
            state = read_state_safe(state_path)
            data = state.get("data") if isinstance(state, dict) else None
            return bool(isinstance(data, dict) and data.get("ready"))
        except Exception:
            return False

    def _start_restart_overlay(self) -> None:
        if not self.ui_host:
            return
        try:
            state_path = get_user_data_dir() / "restart_overlay_state.json"
            geometry = None
            if hasattr(self.ui_host, "geometry"):
                geo = self.ui_host.geometry()
                geometry = {
                    "x": int(geo.x()),
                    "y": int(geo.y()),
                    "w": int(geo.width()),
                    "h": int(geo.height()),
                }
            theme = getattr(self.ui_host, "current_theme", None)
            bg_path = None
            try:
                bg_path = get_user_data_dir() / "restart_overlay_bg.png"
                pixmap = self.ui_host.grab()
                if pixmap and not pixmap.isNull():
                    pixmap.save(str(bg_path))
                else:
                    bg_path = None
            except Exception:
                bg_path = None

            payload = build_state_payload(
                {
                    "reason": "restart_overlay",
                    "pid": os.getpid(),
                    "geometry": geometry,
                    "theme": theme,
                    "bg_path": str(bg_path) if bg_path else None,
                    "ready": False,
                },
                app_version=None,
            )
            write_state_atomic(state_path, payload)

            overlay_script = (
                Path(__file__).resolve().parents[1] / "ui" / "restart_overlay_window.py"
            )
            args = [sys.executable, str(overlay_script), str(state_path)]
            creationflags = 0
            startupinfo = None
            if os.name == "nt":
                # Windows进程独立性标志:
                # CREATE_NO_WINDOW (0x08000000): 不创建控制台窗口
                # CREATE_NEW_PROCESS_GROUP (0x00000200): 创建新进程组
                # DETACHED_PROCESS (0x00000008): 分离进程
                # CREATE_BREAKAWAY_FROM_JOB (0x01000000): 脱离Job对象
                creationflags = (
                    0x08000000  # CREATE_NO_WINDOW
                    | 0x00000200  # CREATE_NEW_PROCESS_GROUP
                    | 0x00000008  # DETACHED_PROCESS
                    | 0x01000000  # CREATE_BREAKAWAY_FROM_JOB
                )
                # 设置startupinfo隐藏窗口
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = 0  # SW_HIDE
            subprocess.Popen(
                args,
                close_fds=(os.name != "nt"),
                creationflags=creationflags,
                startupinfo=startupinfo,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as exc:
            log_error(f"HotReload: 启动重启遮罩失败: {exc}")

    def _restart_application(self) -> None:
        try:
            self.stop()
        except Exception:
            pass

        if getattr(sys, "frozen", False):
            executable = sys.executable
            args = [executable]
        else:
            executable = sys.executable
            args = [executable] + sys.argv

        if os.name == "nt":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 1  # SW_SHOWNORMAL
            subprocess.Popen(
                args,
                close_fds=False,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                startupinfo=startupinfo,
            )
            os._exit(0)
        else:
            os.execv(executable, args)

    def _show_error(self, title: str, detail: str) -> None:
        if self.ui_host and hasattr(self.ui_host, "show_message"):
            self.ui_host.show_message(f"{title}\n{detail}")
        else:
            log_error(f"{title}: {detail}")

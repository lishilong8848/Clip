# -*- coding: utf-8 -*-
import os
import json
import threading
import queue
import time
from PyQt6.QtWidgets import QMessageBox
from PyQt6.QtCore import Qt, QTimer, QUrl, qInstallMessageHandler
from PyQt6.QtGui import QDesktopServices

from ..config import config
from ..logger import log_info, log_error, log_warning, write_crash_trace_message
from ..utils import BASE_DIR
from ..hot_reload.manager import HotReloadManager
from ..services.system_alert_webhook import send_system_alert
from ..core.parser import extract_event_info
from ..core.speech import speech_manager

_QT_MESSAGE_HANDLER_INSTALLED = False
DEFAULT_DISPLAY_VERSION = "V1.0.20260210"

class MainWindowRuntimeMixin:
    def _build_display_version(self) -> str:
        meta_path = os.path.join(BASE_DIR, "build_meta.json")
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            return "未知版本"

        display_version = (meta.get("display_version") or "").strip()
        if display_version:
            return display_version

        major = meta.get("major_version")
        patch = meta.get("patch_version")
        if isinstance(major, int) and isinstance(patch, int):
            return f"v{major}.{patch}"

        build_id = (meta.get("build_id") or "").strip()
        return build_id or "未知版本"

    def _emit_system_alert(
        self,
        event_code: str,
        title: str,
        detail: str,
        *,
        severity: str = "error",
        dedup_key: str = "",
        extra: dict | None = None,
    ):
        try:
            send_system_alert(
                event_code=event_code,
                title=title,
                detail=detail,
                severity=severity,
                dedup_key=dedup_key,
                extra=extra or {},
            )
        except Exception as exc:
            log_warning(f"系统告警发送失败(本地异常): {exc}")

    def _on_clipboard_entry_received(self, entry: dict):
        if self._is_clipboard_listener_disabled():
            return
        if not entry:
            return
        entry_id = entry.get("entry_id", "")
        if entry_id and not self._has_clipboard_pending_entry(entry_id):
            if not self._add_clipboard_pending_entry(entry):
                return
        if self.current_screenshot_record_id or self.screenshot_dialog.isVisible():
            self._defer_event(
                {
                    "content": entry.get("content", ""),
                    "status": entry.get("status", ""),
                    "title": entry.get("title", ""),
                    "notice_type": entry.get("notice_type", ""),
                    "entry_id": entry_id,
                }
            )
            return
        self._process_event(
            entry.get("content", ""),
            entry.get("status", ""),
            entry.get("notice_type", ""),
            entry=entry,
        )
        self._remove_clipboard_pending_entry(entry.get("entry_id", ""))

    def _update_event_relay_status(self, status: str):
        label = getattr(self, "relay_status_label", None)
        if not label:
            return
        color = "#F59E0B"
        if "已启动" in status or "运行中" in status:
            color = "#10B981"
        elif "已关闭" in status or "已停止" in status:
            color = "#94A3B8"
        elif "失败" in status or "不可用" in status:
            color = "#EF4444"
        label.setText(f"事件中转: {status}")
        label.setStyleSheet(f"color: {color}; font-size: 11px;")

    def _on_event_relay_received(self, content: str, status: str, notice_type: str):
        if self.current_screenshot_record_id or self.screenshot_dialog.isVisible():
            info = extract_event_info(content) or {}
            self._defer_event(
                {
                    "content": info.get("content") or content,
                    "status": status or info.get("status", ""),
                    "title": info.get("title", ""),
                    "notice_type": notice_type or info.get("notice_type", ""),
                    "entry_id": "",
                }
            )
            return
        self._process_event(content, status, notice_type)

    def _stop_event_relay_bridge(self):
        bridge = getattr(self, "_event_relay_bridge", None)
        if not bridge:
            return
        try:
            bridge.stop()
        except Exception:
            pass

    def _is_event_relay_enabled(self) -> bool:
        return bool(getattr(config, "relay_enabled", False))

    def _apply_event_relay_setting(self, force_reload: bool = True):
        if force_reload:
            try:
                config.load()
            except Exception as exc:
                log_error(f"事件中转设置加载失败: {exc}")
        bridge = getattr(self, "_event_relay_bridge", None)
        if not bridge:
            return
        if self._is_event_relay_enabled():
            try:
                bridge.start()
            except Exception as exc:
                log_error(f"事件中转启动失败: {exc}")
                self._update_event_relay_status("启动失败")
        else:
            try:
                bridge.stop()
            except Exception:
                pass
            self._update_event_relay_status("已关闭")

    def refresh_event_relay_setting(self):
        self._apply_event_relay_setting(force_reload=True)

    def _init_hot_reload(self):
        try:
            from pathlib import Path

            if config.disable_hot_reload:
                log_info("HotReload: 已禁用")
                return

            project_root = Path(__file__).resolve().parents[3]
            self.hot_reload_manager = HotReloadManager(project_root, ui_host=self)
            self.hot_reload_manager.start()
        except Exception as exc:
            log_error(f"HotReload: 初始化失败: {exc}")

    def refresh_hot_reload_setting(self):
        try:
            from pathlib import Path

            config.load()
            if config.disable_hot_reload:
                if hasattr(self, "hot_reload_manager") and self.hot_reload_manager:
                    self.hot_reload_manager.stop()
                self.hot_reload_manager = None
                log_info("HotReload: 已禁用")
                return

            project_root = Path(__file__).resolve().parents[3]
            if (
                not hasattr(self, "hot_reload_manager")
                or self.hot_reload_manager is None
            ):
                self.hot_reload_manager = HotReloadManager(project_root, ui_host=self)
            else:
                self.hot_reload_manager.stop()
            self.hot_reload_manager.start()
            log_info("HotReload: 已启用")
        except Exception as exc:
            log_error(f"HotReload: 重新初始化失败: {exc}")

    def refresh_alert_setting(self):
        try:
            config.load()
            speech_manager.set_enabled(
                not bool(getattr(config, "disable_alerts", False))
                and not bool(getattr(config, "disable_speech", True))
            )
        except Exception as exc:
            log_error(f"提醒设置更新失败: {exc}")

    def _build_table_url(self, table_id: str) -> str:
        app_token = (config.app_token or "").strip()
        if not app_token:
            return ""
        if app_token.startswith("http://") or app_token.startswith("https://"):
            base_url = app_token
        else:
            base_url = f"https://feishu.cn/base/{app_token}"
        table_id = (table_id or "").strip()
        if not table_id:
            return base_url
        sep = "&" if "?" in base_url else "?"
        return f"{base_url}{sep}table={table_id}"

    def _show_info_message(self, title: str, text: str):
        msg = QMessageBox(self)
        msg.setWindowTitle(title)
        msg.setText(text)
        msg.setStandardButtons(QMessageBox.StandardButton.Ok)
        msg.button(QMessageBox.StandardButton.Ok).setText("确定")
        msg.exec()

    def _open_table_link(self, table_attr: str):
        table_id = getattr(config, table_attr, "")
        url = self._build_table_url(table_id)
        if not url:
            self._show_info_message("未配置表格", "请先在设置中填写表格ID或App Token。")
            return
        QDesktopServices.openUrl(QUrl(url))

    def refresh_table_links(self):
        if not hasattr(self, "table_link_buttons"):
            return
        for btn, attr in self.table_link_buttons:
            table_id = getattr(config, attr, "")
            url = self._build_table_url(table_id)
            if url:
                btn.setToolTip(url)
            else:
                btn.setToolTip("未配置")

    def _check_ocr_lang_pack(self):
        """检测系统是否安装了中文 OCR 语言包，如果没装则弹窗引导安装"""
        if self._closing:
            return

        def _do_check():
            try:
                from winocr import recognize_pil as _rp
                from PIL import Image as _Img
                import asyncio

                test_img = _Img.new("RGB", (60, 20), "white")
                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(_rp(test_img, "zh-Hans-CN"))
                    return True  # 成功，说明已安装
                except Exception:
                    return False  # 失败，进行下一步提示
                finally:
                    loop.close()
            except Exception:
                return True  # 其他异常暂不打扰

        def _on_check_result(is_installed):
            if is_installed or self._closing:
                return

            reply = QMessageBox.question(
                self,
                "OCR 组件缺失",
                "检测到您的系统缺少【中文 OCR 组件】，这将导致包含汉字的截屏日期无法被识别。\n\n是否立即自动下载并安装？（需要管理员权限，约需10~30秒）",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if reply == QMessageBox.StandardButton.Yes:
                self.install_ocr_lang_pack()

        import threading

        def _worker():
            res = _do_check()
            QTimer.singleShot(0, lambda: _on_check_result(res))

        threading.Thread(target=_worker, daemon=True).start()

    def install_ocr_lang_pack(self):
        """提权执行 PowerShell 安装中文 OCR 包"""
        import ctypes

        ps_script = (
            "Write-Host '正在为您安装 Windows 中文 OCR 语言包，请稍候...' -ForegroundColor Cyan;"
            "Add-WindowsCapability -Online -Name 'Language.OCR~~~zh-Hans~0.0.1.0';"
            "Write-Host '安装流程结束！本窗口将在 3 秒后关闭。' -ForegroundColor Green;"
            "Start-Sleep -Seconds 3"
        )
        ret = ctypes.windll.shell32.ShellExecuteW(
            None, "runas", "powershell.exe", f'-Command "{ps_script}"', None, 1
        )
        if ret <= 32:
            msg = QMessageBox(self)
            msg.setWindowFlags(msg.windowFlags() | Qt.WindowType.FramelessWindowHint)
            msg.setIcon(QMessageBox.Icon.Warning)
            msg.setWindowTitle("")
            msg.setText("未能获取管理员权限 或 安装被取消。")
            msg.exec()
        else:
            if hasattr(self, "_update_overlay") and self._update_overlay:
                self._update_overlay.show()
                self._update_overlay.set_blur_intensity(20)
                self.repaint()

            msg = QMessageBox(self)
            msg.setWindowFlags(msg.windowFlags() | Qt.WindowType.FramelessWindowHint)
            msg.setIcon(QMessageBox.Icon.Information)
            msg.setWindowTitle("")
            msg.setText(
                "已请求管理员权限开始自动安装 OCR 组件。\n\n请等待弹出的黑色窗口跑完进度条自动关闭，即可正常使用截图识别功能。"
            )
            msg.exec()

    def _install_qt_message_handler(self):
        global _QT_MESSAGE_HANDLER_INSTALLED
        if _QT_MESSAGE_HANDLER_INSTALLED:
            return

        def _qt_message_handler(msg_type, context, message):
            try:
                msg = f"QtMsg[{msg_type}] {message}"
                if any(
                    key in message
                    for key in ("QThreadStorage", "QObject", "QWidget", "QPainter")
                ):
                    log_warning(msg)
                else:
                    log_info(msg)
                write_crash_trace_message(msg)
            except Exception:
                pass

        try:
            qInstallMessageHandler(_qt_message_handler)
            _QT_MESSAGE_HANDLER_INSTALLED = True
        except Exception as exc:
            log_warning(f"Qt消息处理器安装失败: {exc}")

    def _enqueue_ui_mutation(self, tag: str, fn):
        if self._closing:
            return
        try:
            self._ui_mutation_queue.put_nowait((tag, fn, time.time()))
        except Exception:
            pass

    def _drain_ui_mutations(self):
        if self._closing or self._ui_update_in_progress:
            return
        max_count = max(1, int(self._ui_mutation_max_per_tick or 1))
        self._ui_update_in_progress = True
        try:
            for _ in range(max_count):
                try:
                    tag, fn, _ = self._ui_mutation_queue.get_nowait()
                except queue.Empty:
                    break
                try:
                    self._set_last_ui_op(f"ui_mutation:{tag}")
                    fn()
                except Exception as exc:
                    log_error(f"UI变更执行失败({tag}): {exc}")
                finally:
                    try:
                        self._ui_mutation_queue.task_done()
                    except Exception:
                        pass
        finally:
            self._ui_update_in_progress = False

    def _post_request_finished(self, name, success, msg, record_id):
        try:
            self._ui_signal_queue.put_nowait((name, bool(success), msg, record_id))
        except Exception:
            pass

    def _drain_ui_signal_queue(self):
        if self._closing or self._ui_update_in_progress:
            return
        max_count = max(1, int(self._ui_signal_max_per_tick or 1))
        for _ in range(max_count):
            try:
                item = self._ui_signal_queue.get_nowait()
            except queue.Empty:
                break
            try:
                name, success, msg, record_id = item
            except Exception:
                continue
            try:
                self.request_finished.emit(name, success, msg, record_id)
            except Exception:
                pass


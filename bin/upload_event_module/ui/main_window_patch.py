# -*- coding: utf-8 -*-
import json
import re
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

from PyQt6.QtWidgets import (
    QFrame,
    QVBoxLayout,
    QLabel,
    QGraphicsDropShadowEffect,
    QGraphicsOpacityEffect,
    QGraphicsBlurEffect,
    QMessageBox,
)
from PyQt6.QtCore import Qt, QTimer, QPropertyAnimation, QEasingCurve
from PyQt6.QtGui import QColor

from ..logger import log_error, log_info
from ..utils import BASE_DIR
from .styles import get_stylesheet
from ..hot_reload.state_store import (
    build_state_payload,
    cleanup_state,
    get_user_data_dir,
    read_state_safe,
    write_state_atomic,
)
from ..config import config
from ..services.remote_patch_updater import RemotePatchUpdater
from ..services.dependency_bootstrap import (
    DEFAULT_MODULE_TO_PACKAGE,
    DEFAULT_WINDOWS_MODULE_TO_PACKAGE,
    ensure_runtime_dependencies,
)
from ..services.system_alert_webhook import send_system_alert


class PatchUpdateMixin:
    def _init_remote_patch_updater(self):
        self._remote_ui_manifest = None
        self._remote_non_ui_manifest = None
        self._remote_update_checking = False
        self._remote_update_busy = False
        self._last_patch_source = "local"
        self._clipboard_paused_for_patch_update = False
        root_dir = self._get_app_root_dir()
        manifest_url = (getattr(config, "remote_update_manifest_url", "") or "").strip()
        data_dir = get_user_data_dir() / "remote_patch"
        self._remote_patch_updater = RemotePatchUpdater(root_dir, data_dir, manifest_url)
        self._set_remote_update_status("远程更新: 待检查")

        self.remote_update_timer = QTimer(self)
        self.remote_update_timer.timeout.connect(self._schedule_remote_update_check)
        interval_seconds = max(
            60, int(getattr(config, "remote_update_interval_seconds", 3600))
        )
        self.remote_update_timer.start(interval_seconds * 1000)
        QTimer.singleShot(20000, self._schedule_remote_update_check)

    def _set_remote_update_status(self, text: str):
        label = getattr(self, "remote_update_status_label", None)
        if label is not None:
            label.setText(text)

    def _emit_remote_update_phase(self, text: str):
        try:
            self.remote_update_phase.emit(text)
        except Exception:
            self._set_remote_update_status(text)

    def _can_apply_auto_non_ui_patch(self) -> bool:
        if getattr(self, "_closing", False):
            return False
        if getattr(self, "_ui_update_in_progress", False):
            return False
        if getattr(self, "current_screenshot_record_id", None):
            return False
        dialog = getattr(self, "screenshot_dialog", None)
        if dialog is not None and dialog.isVisible():
            return False
        return True

    def _schedule_remote_update_check(self):
        if not bool(getattr(config, "remote_update_enabled", True)):
            self._set_remote_update_status("远程更新: 已禁用")
            return
        if self._remote_update_busy or self._remote_update_checking:
            return
        if (
            self._remote_non_ui_manifest
            and bool(getattr(config, "remote_update_auto_apply_non_ui", True))
            and self._can_apply_auto_non_ui_patch()
        ):
            manifest = self._remote_non_ui_manifest
            self._remote_non_ui_manifest = None
            self._start_remote_patch_apply(manifest, auto_apply=True)
            return

        self._remote_update_checking = True
        self._set_remote_update_status("远程更新: 检查中...")
        threading.Thread(target=self._remote_update_check_worker, daemon=True).start()

    def _remote_update_check_worker(self):
        status_text = "远程更新: 已是最新"
        ui_manifest = None
        non_ui_manifest = None
        try:
            self._remote_patch_updater.manifest_url = (
                getattr(config, "remote_update_manifest_url", "") or ""
            ).strip()
            manifest = self._remote_patch_updater.fetch_manifest()
            if not manifest:
                status_text = "远程更新: 清单为空"
            else:
                local_meta = RemotePatchUpdater.load_local_build_meta(
                    self._get_app_root_dir()
                )
                if not RemotePatchUpdater.is_local_version_known(local_meta):
                    status_text = "远程更新: 本地版本未标记(跳过)"
                elif not self._remote_patch_updater.has_newer_patch(local_meta, manifest):
                    status_text = "远程更新: 已是最新"
                elif self._remote_patch_updater.is_ui_update(manifest):
                    ui_manifest = manifest
                    status_text = "远程更新: 发现可更新(UI需确认)"
                elif bool(getattr(config, "remote_update_auto_apply_non_ui", True)):
                    non_ui_manifest = manifest
                    status_text = "远程更新: 发现可更新(自动应用)"
                else:
                    status_text = "远程更新: 发现可更新(非UI)"
        except Exception as exc:
            status_text = "远程更新: 检查失败"
            log_error(f"远程更新检查失败: {exc}")

        self.remote_update_checked.emit(status_text, ui_manifest, non_ui_manifest)

    def _on_remote_update_checked(self, status_text: str, ui_manifest, non_ui_manifest):
        self._remote_update_checking = False
        if getattr(self, "_closing", False):
            return
        if ui_manifest:
            self._remote_ui_manifest = ui_manifest
        if non_ui_manifest:
            if self._find_patch_dir():
                self._remote_non_ui_manifest = non_ui_manifest
                status_text = "远程更新: 本地补丁优先，自动应用延后"
            elif self._can_apply_auto_non_ui_patch():
                self._start_remote_patch_apply(non_ui_manifest, auto_apply=True)
                status_text = "远程更新: 自动更新中..."
            else:
                self._remote_non_ui_manifest = non_ui_manifest
                status_text = "远程更新: 待空闲后自动应用"
        self._set_remote_update_status(status_text)
        self._refresh_patch_button()

    def _start_remote_patch_apply(self, manifest: dict, auto_apply: bool):
        if self._remote_update_busy:
            return
        if self._find_patch_dir() and auto_apply:
            self._remote_non_ui_manifest = manifest
            self._set_remote_update_status("远程更新: 本地补丁优先，自动应用延后")
            return
        self._pause_clipboard_for_patch_update()
        self._remote_update_busy = True
        self._set_remote_update_status("远程更新: 下载中...")
        if not auto_apply:
            self.patch_btn.setEnabled(False)
            self.patch_btn.setText("下载中")
        threading.Thread(
            target=self._remote_patch_download_worker,
            args=(manifest, auto_apply),
            daemon=True,
        ).start()

    def _remote_patch_download_worker(self, manifest: dict, auto_apply: bool):
        patch_dir = None
        error = ""
        try:
            patch_dir = self._remote_patch_updater.prepare_remote_patch(manifest)
        except Exception as exc:
            error = str(exc)
        self.remote_patch_downloaded.emit(manifest, patch_dir, error, auto_apply)

    def _on_remote_patch_downloaded(
        self, manifest: dict, patch_dir: Path | None, error: str, auto_apply: bool
    ):
        self._remote_update_busy = False
        self.patch_btn.setEnabled(True)
        self.patch_btn.setText("更新")
        if error:
            self._set_remote_update_status("远程更新: 下载失败")
            log_error(f"远程补丁下载失败: {error}")
            self._resume_clipboard_after_patch_update()
            if not auto_apply:
                self.show_message(f"远程更新下载失败: {error}")
            self._refresh_patch_button()
            return
        if not patch_dir:
            self._set_remote_update_status("远程更新: 下载失败")
            self._resume_clipboard_after_patch_update()
            self._refresh_patch_button()
            return

        if self._remote_ui_manifest is manifest:
            self._remote_ui_manifest = None
        if self._remote_non_ui_manifest is manifest:
            self._remote_non_ui_manifest = None
        self._patch_dir = patch_dir
        self._last_patch_source = "remote_auto" if auto_apply else "remote_ui"
        self._set_remote_update_status("远程更新: 应用中...")
        self.apply_patch_update()

    def _init_update_overlay(self):
        self.update_overlay = QFrame(self)
        self.update_overlay.setObjectName("UpdateOverlay")
        self.update_overlay.setAttribute(
            Qt.WidgetAttribute.WA_TransparentForMouseEvents, False
        )
        self.update_overlay.hide()
        overlay_layout = QVBoxLayout(self.update_overlay)
        overlay_layout.setContentsMargins(0, 0, 0, 0)
        overlay_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.update_overlay_card = QFrame(self.update_overlay)
        self.update_overlay_card.setObjectName("UpdateOverlayCard")
        card_layout = QVBoxLayout(self.update_overlay_card)
        card_layout.setContentsMargins(24, 20, 24, 20)
        card_layout.setSpacing(6)

        self.update_overlay_label = QLabel("更新中...")
        self.update_overlay_label.setObjectName("UpdateOverlayLabel")
        self.update_overlay_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        card_layout.addWidget(self.update_overlay_label)

        shadow = QGraphicsDropShadowEffect(self.update_overlay_card)
        shadow.setBlurRadius(25)
        shadow.setOffset(0, 6)
        shadow.setColor(QColor(0, 0, 0, 120))
        self.update_overlay_card.setGraphicsEffect(shadow)

        overlay_layout.addWidget(self.update_overlay_card)
        self.update_overlay.setGeometry(self.rect())
        self.update_overlay_effect = QGraphicsOpacityEffect(self.update_overlay)
        self.update_overlay.setGraphicsEffect(self.update_overlay_effect)
        self.update_overlay_effect.setOpacity(0.0)
        self.update_overlay_anim = QPropertyAnimation(
            self.update_overlay_effect, b"opacity", self
        )
        self.update_overlay_anim.setDuration(800)
        self.update_overlay_anim.setEasingCurve(QEasingCurve.Type.InOutCubic)
        self.update_overlay_anim.finished.connect(self._on_update_overlay_anim_finished)
        self._update_overlay_hiding = False
        self._update_overlay_show_ts = 0.0
        self._update_overlay_min_visible_ms = 1000
        self._update_overlay_fade_in_ms = 800
        self._update_overlay_fade_out_ms = 800
        self.update_blur_effect = QGraphicsBlurEffect(self.container)
        self.update_blur_effect.setBlurRadius(20)

        self.update_overlay_label.setGraphicsEffect(None)

    def _update_overlay_style(self):
        if not hasattr(self, "update_overlay"):
            return
        if self.current_theme == "dark":
            overlay_bg = "rgba(15, 23, 42, 0.70)"
            card_bg = "rgba(255, 255, 255, 0.18)"
            border = "rgba(255, 255, 255, 0.30)"
            text_color = "#F9FAFB"
        else:
            overlay_bg = "rgba(255, 255, 255, 0.75)"
            card_bg = "rgba(255, 255, 255, 0.95)"
            border = "rgba(15, 23, 42, 0.08)"
            text_color = "#111827"

        self.update_overlay.setStyleSheet(
            f"QFrame#UpdateOverlay {{ background-color: {overlay_bg}; border-radius: 16px; }}"
        )
        self.update_overlay_card.setStyleSheet(
            "QFrame#UpdateOverlayCard {"
            f"background-color: {card_bg};"
            f"border: 1px solid {border};"
            "border-radius: 14px;"
            "}"
        )
        self.update_overlay_label.setStyleSheet(
            "QLabel#UpdateOverlayLabel {"
            f"color: {text_color};"
            "font-size: 20px;"
            "font-weight: 600;"
            "letter-spacing: 1px;"
            "}"
        )

    def _show_update_overlay(self):
        if not hasattr(self, "update_overlay"):
            return
        self._update_overlay_hiding = False
        self._update_overlay_show_ts = time.time()
        self.update_overlay.setGeometry(self.rect())
        self.update_overlay.raise_()
        self.update_overlay.show()
        if hasattr(self, "patch_toast"):
            try:
                self.patch_toast.hide()
            except Exception:
                pass
        if hasattr(self, "update_blur_effect"):
            try:
                self.container.setGraphicsEffect(self.update_blur_effect)
            except RuntimeError:
                self.update_blur_effect = QGraphicsBlurEffect(self.container)
                self.update_blur_effect.setBlurRadius(20)
                self.container.setGraphicsEffect(self.update_blur_effect)
        if hasattr(self, "update_overlay_anim"):
            self.update_overlay_anim.stop()
            self.update_overlay_anim.setDuration(self._update_overlay_fade_in_ms)
            self.update_overlay_anim.setStartValue(self.update_overlay_effect.opacity())
            self.update_overlay_anim.setEndValue(1.0)
            self.update_overlay_anim.start()
        self.update_overlay_label.show()

    def _hide_update_overlay(self):
        if hasattr(self, "update_overlay"):
            elapsed_ms = int((time.time() - self._update_overlay_show_ts) * 1000)
            min_keep_ms = self._update_overlay_fade_in_ms + self._update_overlay_min_visible_ms
            delay_ms = max(0, min_keep_ms - elapsed_ms)

            def do_hide():
                self._update_overlay_hiding = True
                if hasattr(self, "update_overlay_anim"):
                    self.update_overlay_anim.stop()
                    self.update_overlay_anim.setDuration(self._update_overlay_fade_out_ms)
                    self.update_overlay_anim.setStartValue(
                        self.update_overlay_effect.opacity()
                    )
                    self.update_overlay_anim.setEndValue(0.0)
                    self.update_overlay_anim.start()
                else:
                    self.update_overlay.hide()
                self.container.setGraphicsEffect(None)
                self.update_overlay_label.show()
                if hasattr(self, "patch_toast"):
                    try:
                        self.patch_toast.hide()
                    except Exception:
                        pass

            if delay_ms > 0:
                QTimer.singleShot(delay_ms, do_hide)
            else:
                do_hide()

    def _on_update_overlay_anim_finished(self):
        if self._update_overlay_hiding and hasattr(self, "update_overlay"):
            self.update_overlay.hide()
            if self._pending_update_overlay_cleanup:
                cleanup_state(self.update_state_path)
                self._pending_update_overlay_cleanup = False

    def _restore_restart_overlay_geometry(self):
        state_path = getattr(self, "restart_overlay_state_path", None)
        if not state_path:
            return
        state = read_state_safe(state_path)
        if not state:
            return
        data = state.get("data") if isinstance(state, dict) else None
        if not isinstance(data, dict):
            return
        geometry = data.get("geometry")
        if not isinstance(geometry, dict):
            return
        try:
            self.setGeometry(
                int(geometry.get("x", 0)),
                int(geometry.get("y", 0)),
                int(geometry.get("w", self.width())),
                int(geometry.get("h", self.height())),
            )
        except Exception:
            pass

    def _restore_update_overlay_state(self):
        if not self.update_state_path:
            return
        state = read_state_safe(self.update_state_path)
        if not state:
            return
        data = state.get("data") if isinstance(state, dict) else None
        if not isinstance(data, dict) or data.get("reason") != "patch_update":
            return
        self._pending_update_overlay_cleanup = True
        self._show_update_overlay()
        QTimer.singleShot(1200, self._hide_update_overlay)

    def _close_restart_overlay_window(self):
        state_path = getattr(self, "restart_overlay_state_path", None)
        if not state_path:
            return
        state = read_state_safe(state_path)
        if not state:
            return
        data = state.get("data") if isinstance(state, dict) else None
        if not isinstance(data, dict):
            return
        if data.get("reason") in {"restart_overlay", "hot_restart", "restart"}:
            try:
                payload = build_state_payload({"reason": "close"}, app_version=None)
                write_state_atomic(state_path, payload)
            except Exception as exc:
                log_error(f"重启遮罩关闭状态写入失败: {exc}")

    def _read_json_file(self, path: Path) -> dict:
        if not path.exists():
            return {}
        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _load_patch_meta(self, patch_dir: Path) -> dict:
        return self._read_json_file(patch_dir / "bin" / "patch_meta.json")

    def _read_local_build_meta(self, root_dir: Path) -> dict:
        return self._read_json_file(root_dir / "bin" / "build_meta.json")

    def _validate_patch_meta(self, patch_dir: Path, root_dir: Path) -> tuple[bool, str, dict]:
        patch_meta = self._load_patch_meta(patch_dir)
        if not patch_meta:
            return True, "", {}
        min_version = patch_meta.get("min_version") or ""
        if not min_version:
            return True, "", patch_meta
        local_meta = self._read_local_build_meta(root_dir)
        local_build = local_meta.get("build_id") if isinstance(local_meta, dict) else ""
        if local_build and local_build != min_version:
            return False, f"补丁版本不匹配（当前: {local_build}，需要: {min_version}）", patch_meta
        if not local_build:
            return True, "无法验证版本，仍尝试更新。", patch_meta
        return True, "", patch_meta

    def _parse_build_id(self, build_id: str) -> int | None:
        if not build_id:
            return None
        match = re.search(r"(\d{8})_(\d{6})", build_id)
        if match:
            return int(match.group(1) + match.group(2))
        match = re.search(r"(\d{14})", build_id)
        if match:
            return int(match.group(1))
        return None

    def _show_patch_delete_message(self, text: str, patch_dir: Path):
        msg = QMessageBox(self)
        msg.setWindowFlags(Qt.WindowType.Popup | Qt.WindowType.FramelessWindowHint)
        msg.setStandardButtons(QMessageBox.StandardButton.Ok)
        msg.button(QMessageBox.StandardButton.Ok).setText("确定")
        msg.setText(text)
        try:
            msg.setStyleSheet(get_stylesheet(self.current_theme))
        except Exception:
            pass

        def on_close():
            cleanup_note = self._delete_patch_dir(patch_dir)
            self._refresh_patch_button()
            if cleanup_note:
                self.show_message(cleanup_note)

        msg.finished.connect(lambda _=None: on_close())
        msg.show()

    def _parse_deleted_files(self, patch_dir: Path) -> list[Path]:
        deleted: list[Path] = []
        manifest = patch_dir / "patch_manifest.txt"
        if not manifest.exists():
            return deleted
        try:
            lines = manifest.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            return deleted
        if "Deleted files (remove manually if needed):" not in lines:
            return deleted
        start = lines.index("Deleted files (remove manually if needed):") + 1
        for line in lines[start:]:
            line = line.strip()
            if not line:
                continue
            deleted.append(Path(line))
        return deleted

    def _collect_patch_files(self, patch_dir: Path) -> list[Path]:
        files = []
        for src in patch_dir.rglob("*"):
            if src.is_dir():
                continue
            if src.name in ("patch_manifest.txt", "patch_meta.json"):
                continue
            files.append(src)
        return files

    def _copy_with_retry(self, src: Path, dest: Path, attempts: int = 3) -> bool:
        for _ in range(attempts):
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dest)
                return True
            except Exception:
                time.sleep(0.2)
        return False

    def _delete_with_retry(self, target: Path, attempts: int = 3) -> bool:
        for _ in range(attempts):
            try:
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink(missing_ok=True)
                return True
            except Exception:
                time.sleep(0.2)
        return False

    def _backup_patch_targets(
        self,
        patch_dir: Path,
        patch_files: list[Path],
        deleted_files: list[Path],
        root_dir: Path,
    ) -> tuple[Path, list[Path], list[Path]]:
        backup_dir = root_dir / "bin" / ".patch_backup" / time.strftime("%Y%m%d_%H%M%S")
        backup_dir.mkdir(parents=True, exist_ok=True)
        new_files: list[Path] = []
        for src in patch_files:
            rel = src.relative_to(patch_dir)
            dest = root_dir / rel
            if dest.exists():
                backup_target = backup_dir / rel
                backup_target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(dest, backup_target)
            else:
                new_files.append(dest)
        for rel in deleted_files:
            target = root_dir / rel
            if target.exists():
                backup_target = backup_dir / rel
                backup_target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(target, backup_target)
        return backup_dir, new_files, deleted_files

    def _rollback_patch(self, backup_dir: Path, new_files: list[Path]):
        for path in new_files:
            try:
                if path.exists():
                    if path.is_dir():
                        shutil.rmtree(path)
                    else:
                        path.unlink()
            except Exception:
                pass
        for src in backup_dir.rglob("*"):
            if src.is_dir():
                continue
            rel = src.relative_to(backup_dir)
            dest = self._get_app_root_dir() / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy2(src, dest)
            except Exception:
                pass

    def _update_build_meta(self, root_dir: Path, patch_meta: dict):
        if not patch_meta:
            return
        target_version = patch_meta.get("target_version") or ""
        if not target_version:
            return
        meta = self._read_local_build_meta(root_dir)
        if not isinstance(meta, dict):
            meta = {}
        major_version = patch_meta.get("major_version", meta.get("major_version", 1))
        target_patch_version = patch_meta.get(
            "target_patch_version", meta.get("patch_version", 0)
        )
        target_ui_version = (patch_meta.get("target_ui_version") or "").strip()
        target_display_version = patch_meta.get("target_display_version")
        if not target_display_version:
            try:
                target_display_version = (
                    f"v{int(major_version)}.{int(target_patch_version)}"
                )
            except Exception:
                target_display_version = ""
        meta.update(
            {
                "app_name": patch_meta.get("app_name", meta.get("app_name", "")),
                "build_id": target_version,
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "base_build_id": patch_meta.get("min_version", ""),
                "major_version": major_version,
                "patch_version": target_patch_version,
                "display_version": target_display_version,
                "ui_version": target_ui_version or meta.get("ui_version", ""),
            }
        )
        try:
            meta_dir = root_dir / "bin"
            meta_dir.mkdir(parents=True, exist_ok=True)
            (meta_dir / "build_meta.json").write_text(
                json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            pass

    def _show_patch_toast(self, text: str, success: bool = True):
        return

    def _hide_patch_toast(self):
        return

    def _position_patch_toast(self):
        return

    def _get_app_root_dir(self) -> Path:
        base_dir = Path(BASE_DIR)
        if (base_dir / "启动程序.bat").exists():
            return base_dir
        parent = base_dir.parent
        if (parent / "启动程序.bat").exists():
            return parent
        return parent if (base_dir / "refactored_main.py").exists() else base_dir

    def _find_patch_dir(self) -> Path | None:
        root_dir = self._get_app_root_dir()
        candidates = [
            p for p in root_dir.iterdir() if p.is_dir() and p.name.endswith("_patch_only")
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda p: p.stat().st_mtime)

    def _refresh_patch_button(self):
        patch_dir = self._find_patch_dir()
        self._patch_dir = patch_dir
        if patch_dir:
            if self.patch_btn.isHidden():
                self.patch_btn.show()
            self.patch_btn.setToolTip(f"检测到补丁: {patch_dir.name}")
            self.patch_btn.setText("更新")
        else:
            remote_ui_manifest = getattr(self, "_remote_ui_manifest", None)
            if remote_ui_manifest:
                self.patch_btn.show()
                version = (
                    remote_ui_manifest.get("target_display_version")
                    or remote_ui_manifest.get("target_version")
                    or "未知版本"
                )
                self.patch_btn.setToolTip(f"检测到远程更新: {version}")
                self.patch_btn.setText("更新")
            elif not self.patch_btn.isHidden():
                self.patch_btn.hide()

    def apply_patch_update(self):
        patch_dir = self._patch_dir or self._find_patch_dir()
        if not patch_dir:
            remote_ui_manifest = getattr(self, "_remote_ui_manifest", None)
            if remote_ui_manifest:
                self._start_remote_patch_apply(remote_ui_manifest, auto_apply=False)
                return
            self.show_message("未检测到补丁文件夹。")
            return
        self._last_patch_source = "local"
        root_dir = self._get_app_root_dir()
        ok, warn_msg, patch_meta = self._validate_patch_meta(patch_dir, root_dir)
        if not ok:
            min_version = patch_meta.get("min_version") if patch_meta else ""
            local_meta = self._read_local_build_meta(root_dir)
            local_build = local_meta.get("build_id") if isinstance(local_meta, dict) else ""
            local_ts = self._parse_build_id(local_build)
            min_ts = self._parse_build_id(min_version)
            if local_ts is not None and min_ts is not None:
                if local_ts >= min_ts:
                    self._show_patch_delete_message("该补丁已更新，现将其删除。", patch_dir)
                    return
                ok = True
            if not ok:
                self.show_message(warn_msg)
                return
        self._last_patch_meta = patch_meta
        if warn_msg:
            pass
        if hasattr(self, "hot_reload_manager"):
            try:
                self.hot_reload_manager.stop()
            except Exception:
                pass
        self._pause_clipboard_for_patch_update()
        self.patch_btn.setEnabled(False)
        self.patch_btn.setText("更新中")
        self._show_update_overlay()
        threading.Thread(
            target=self._apply_patch_worker, args=(patch_dir,), daemon=True
        ).start()

    def _pause_clipboard_for_patch_update(self):
        if getattr(self, "_clipboard_paused_for_patch_update", False):
            return
        try:
            if hasattr(self, "_clipboard_auto_restart_blocked"):
                self._clipboard_auto_restart_blocked = True
            if hasattr(self, "_clipboard_resume_after_stop"):
                self._clipboard_resume_after_stop = False
            if hasattr(self, "_pause_clipboard_timer"):
                self._pause_clipboard_timer()
            if hasattr(self, "_clipboard_pending_lines"):
                self._clipboard_pending_lines.clear()
            if hasattr(self, "_clipboard_partial_line"):
                self._clipboard_partial_line = ""
            event_file = getattr(self, "clipboard_event_file", None)
            if event_file is not None:
                try:
                    event_file.write_text("", encoding="utf-8")
                    self._clipboard_file_index = 0
                    self._clipboard_partial_line = ""
                except Exception:
                    try:
                        self._clipboard_file_index = event_file.stat().st_size
                    except Exception:
                        pass
            if hasattr(self, "_stop_clipboard_process"):
                self._stop_clipboard_process()
            self._clipboard_paused_for_patch_update = True
            if hasattr(self, "_refresh_clipboard_toggle_ui"):
                self._refresh_clipboard_toggle_ui()
            log_info("补丁应用中，剪贴板监听已暂停")
        except Exception as exc:
            log_error(f"补丁应用前暂停剪贴板监听失败: {exc}")

    def _resume_clipboard_after_patch_update(self):
        if not getattr(self, "_clipboard_paused_for_patch_update", False):
            return
        try:
            if hasattr(self, "_clipboard_pending_lines"):
                self._clipboard_pending_lines.clear()
            if hasattr(self, "_clipboard_partial_line"):
                self._clipboard_partial_line = ""
            event_file = getattr(self, "clipboard_event_file", None)
            if event_file is not None:
                try:
                    self._clipboard_file_index = event_file.stat().st_size
                except Exception:
                    pass
            if hasattr(self, "_resume_clipboard_timer"):
                self._resume_clipboard_timer()
            process = getattr(self, "_clipboard_process", None)
            process_running = False
            if process:
                try:
                    process_running = int(process.state()) != 0
                except Exception:
                    process_running = True
            if process_running:
                if hasattr(self, "_clipboard_resume_after_stop"):
                    self._clipboard_resume_after_stop = True
                log_info("补丁应用完成，等待剪贴板监听进程停止后恢复")
            else:
                if hasattr(self, "_clipboard_auto_restart_blocked"):
                    self._clipboard_auto_restart_blocked = False
                if (
                    hasattr(self, "_start_clipboard_listener")
                    and hasattr(self, "_is_clipboard_listener_disabled")
                    and not self._is_clipboard_listener_disabled()
                ):
                    self._start_clipboard_listener()
                log_info("补丁应用完成，剪贴板监听已恢复")
        except Exception as exc:
            log_error(f"补丁应用后恢复剪贴板监听失败: {exc}")
        finally:
            self._clipboard_paused_for_patch_update = False
            if hasattr(self, "_refresh_clipboard_toggle_ui"):
                self._refresh_clipboard_toggle_ui()

    def _delete_patch_dir(self, patch_dir: Path, max_attempts: int = 5) -> str:
        for attempt in range(1, max_attempts + 1):
            try:
                shutil.rmtree(patch_dir)
                return ""
            except Exception as exc:
                log_error(f"补丁删除失败(第{attempt}次): {exc}")
                try:
                    for child in patch_dir.rglob("*"):
                        if child.is_file():
                            try:
                                child.chmod(0o666)
                            except Exception:
                                pass
                        elif child.is_dir():
                            try:
                                child.chmod(0o777)
                            except Exception:
                                pass
                    patch_dir.chmod(0o777)
                except Exception:
                    pass
                time.sleep(0.3)
        try:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            renamed = patch_dir.with_name(f"{patch_dir.name}_failed_{timestamp}")
            patch_dir.rename(renamed)
            return f"删除失败，已重命名为 {renamed.name}"
        except Exception as exc:
            log_error(f"补丁重命名失败: {exc}")
        return "删除补丁失败，请手动删除。"

    def _apply_patch_worker(self, patch_dir: Path):
        try:
            root_dir = self._get_app_root_dir()
            patch_meta = self._last_patch_meta if hasattr(self, "_last_patch_meta") else {}
            if bool(getattr(config, "auto_install_dependencies", True)):
                dep_manifest = dict(patch_meta) if isinstance(patch_meta, dict) else {}
                module_to_package = dict(DEFAULT_MODULE_TO_PACKAGE)
                if sys.platform == "win32":
                    module_to_package.update(DEFAULT_WINDOWS_MODULE_TO_PACKAGE)
                patch_modules = dep_manifest.get("module_to_package")
                if isinstance(patch_modules, dict):
                    for mod, pkg in patch_modules.items():
                        mod_name = str(mod or "").strip()
                        pkg_name = str(pkg or "").strip()
                        if mod_name and pkg_name:
                            module_to_package[mod_name] = pkg_name
                dep_manifest["module_to_package"] = module_to_package
                dep_ok, dep_detail = ensure_runtime_dependencies(
                    dep_manifest,
                    Path(sys.executable),
                    mirrors=getattr(config, "dependency_mirrors", None),
                    timeout_seconds=int(
                        getattr(config, "dependency_install_timeout_seconds", 20)
                    ),
                    retries_per_mirror=int(
                        getattr(
                            config,
                            "dependency_install_retries_per_mirror",
                            1,
                        )
                    ),
                    allow_get_pip=bool(
                        getattr(config, "dependency_bootstrap_allow_get_pip", True)
                    ),
                    status_callback=(
                        self._emit_remote_update_phase
                        if self._last_patch_source.startswith("remote")
                        else None
                    ),
                )
                if not dep_ok:
                    if self._last_patch_source.startswith("remote"):
                        self._emit_remote_update_phase("远程更新: 依赖安装失败")
                    send_system_alert(
                        event_code="dep.patch.install_failed",
                        title="补丁依赖安装失败",
                        detail=str(dep_detail),
                        dedup_key=f"{self._last_patch_source}:patch_dep_failed",
                    )
                    self.patch_update_finished.emit(False, f"依赖安装失败: {dep_detail}")
                    return
                if self._last_patch_source.startswith("remote"):
                    self._emit_remote_update_phase("远程更新: 依赖就绪，应用补丁中")
            patch_files = self._collect_patch_files(patch_dir)
            deleted_files = self._parse_deleted_files(patch_dir)
            backup_dir, new_files, deleted_files = self._backup_patch_targets(
                patch_dir, patch_files, deleted_files, root_dir
            )

            failed: list[str] = []
            for src in patch_files:
                rel = src.relative_to(patch_dir)
                dest = root_dir / rel
                if not self._copy_with_retry(src, dest):
                    failed.append(str(rel))

            for rel in deleted_files:
                target = root_dir / rel
                if target.exists() and not self._delete_with_retry(target):
                    failed.append(str(rel))

            if failed:
                self._rollback_patch(backup_dir, new_files)
                self.patch_update_finished.emit(
                    False,
                    "补丁更新失败，以下文件处理失败：\n" + "\n".join(failed[:20]),
                )
                return

            self._update_build_meta(root_dir, self._last_patch_meta if hasattr(self, "_last_patch_meta") else {})
            patch_meta = self._last_patch_meta or {}
            self._last_patch_requires_restart = bool(
                patch_meta.get("restart_required") or patch_meta.get("force_ui_update")
            )
            # 清理备份
            try:
                shutil.rmtree(backup_dir)
            except Exception:
                pass
            cleanup_note = self._delete_patch_dir(patch_dir)
            cleanup_msg = f"，{cleanup_note}" if cleanup_note else "，已删除补丁文件夹"
            self.patch_update_finished.emit(
                True, f"补丁更新完成: {patch_dir.name}{cleanup_msg}"
            )
        except Exception as exc:
            self.patch_update_finished.emit(False, f"补丁更新失败: {exc}")

    def _on_patch_update_finished(self, success: bool, message: str):
        self.patch_btn.setEnabled(True)
        self.patch_btn.setText("更新")
        self._refresh_patch_button()
        self._resume_clipboard_after_patch_update()
        if success:
            if self._last_patch_source.startswith("remote"):
                self._set_remote_update_status("远程更新: 已应用")
            else:
                self._set_remote_update_status("远程更新: 已是最新")
            self._hide_update_overlay()
            if getattr(self, "_last_patch_requires_restart", False):
                QTimer.singleShot(600, self._restart_after_patch)
        else:
            if self._last_patch_source.startswith("remote"):
                if isinstance(message, str) and message.startswith("依赖安装失败"):
                    self._set_remote_update_status("远程更新: 依赖安装失败")
                else:
                    self._set_remote_update_status("远程更新: 应用失败")
            self._hide_update_overlay()
            self.show_message(message)

    @staticmethod
    def _normalize_windows_launch_path(path_text: str) -> str:
        path_value = str(path_text or "").strip().strip('"')
        if not path_value:
            return ""
        path_value = path_value.replace("/", "\\")
        if path_value.startswith("\\\\?\\UNC\\"):
            path_value = "\\\\" + path_value[8:]
        elif path_value.startswith("\\\\?\\"):
            path_value = path_value[4:]
        return path_value

    @staticmethod
    def _windows_process_has_console() -> bool:
        if os.name != "nt":
            return False
        try:
            import ctypes

            return bool(ctypes.windll.kernel32.GetConsoleWindow())
        except Exception:
            return False

    def _restart_after_patch(self):
        if self._closing:
            return
        try:
            if getattr(sys, "frozen", False):
                args = [str(sys.executable)]
            else:
                script_path = (
                    Path(sys.argv[0]).resolve()
                    if sys.argv and str(sys.argv[0]).strip()
                    else (BASE_DIR / "refactored_main.py").resolve()
                )
                forwarded = [str(x) for x in sys.argv[1:] if x != "--safe-mode"]
                args = [str(sys.executable), str(script_path), *forwarded]

            if os.name == "nt":
                if args:
                    args[0] = self._normalize_windows_launch_path(args[0])
                if not args or not args[0] or args[0] in ("\\", "/"):
                    raise FileNotFoundError(f"无效的重启目标: {args}")
                if not os.path.exists(args[0]):
                    raise FileNotFoundError(f"重启目标不存在: {args[0]}")
                # Delay startup to avoid single-instance lock race:
                # start new process after current instance exits.
                cmdline = subprocess.list2cmdline(args)
                delay_cmd = f"ping 127.0.0.1 -n 3 >nul && {cmdline}"
                comspec = os.environ.get("COMSPEC") or "cmd.exe"
                # If current process is attached to a console (started via batch),
                # keep restart process in the same console so closing that console
                # still closes the UI process.
                creationflags = (
                    0
                    if self._windows_process_has_console()
                    else getattr(subprocess, "CREATE_NO_WINDOW", 0)
                )
                subprocess.Popen(
                    [comspec, "/d", "/c", delay_cmd],
                    close_fds=False,
                    creationflags=creationflags,
                )
            else:
                subprocess.Popen(args, close_fds=True)
        except Exception as exc:
            log_error(f"补丁重启失败: {exc}")
            try:
                self.show_message(f"自动重启失败：{exc}")
            except Exception:
                pass
            return
        try:
            self.quit_app()
        except Exception:
            pass

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if hasattr(self, "update_overlay"):
            self.update_overlay.setGeometry(self.rect())
        if hasattr(self, "patch_toast"):
            self._position_patch_toast()

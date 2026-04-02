# -*- coding: utf-8 -*-
import os
import sys
import time
from pathlib import Path

from PyQt6.QtWidgets import (
    QApplication,
    QWidget,
    QLabel,
    QVBoxLayout,
    QFrame,
    QGraphicsOpacityEffect,
    QGraphicsBlurEffect,
)
from PyQt6.QtCore import Qt, QTimer, QPropertyAnimation, QEasingCurve
from PyQt6.QtGui import QColor, QPixmap, QPainterPath, QRegion

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from upload_event_module.hot_reload.state_store import (  # noqa: E402
    get_user_data_dir,
    read_state_safe,
    cleanup_state,
    build_state_payload,
    write_state_atomic,
)


class RestartOverlayWindow(QWidget):
    def __init__(self, state_path: Path):
        super().__init__()
        self.state_path = state_path
        self._hiding = False
        self._bg_pixmap = None
        self._bg_path = None
        self._drag_offset = None
        self._persist_timer = QTimer(self)
        self._persist_timer.setSingleShot(True)
        self._persist_timer.timeout.connect(self._persist_geometry)

        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint
            | Qt.WindowType.WindowStaysOnTopHint
            | Qt.WindowType.Tool
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating)

        self.bg_label = QLabel(self)
        self.bg_label.setObjectName("RestartOverlayBackground")
        self.bg_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.bg_label.lower()
        self.bg_blur = QGraphicsBlurEffect(self.bg_label)
        self.bg_blur.setBlurRadius(24)
        self.bg_label.setGraphicsEffect(self.bg_blur)

        self.container = QFrame(self)
        self.container.setObjectName("RestartOverlayContainer")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.container)

        container_layout = QVBoxLayout(self.container)
        container_layout.setContentsMargins(0, 0, 0, 0)
        container_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.label = QLabel("正在更新，请稍候…")
        self.label.setObjectName("RestartOverlayLabel")
        self.label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        container_layout.addWidget(self.label)

        self.opacity_effect = QGraphicsOpacityEffect(self)
        self.setGraphicsEffect(self.opacity_effect)
        self.opacity_effect.setOpacity(0.0)
        self.anim = QPropertyAnimation(self.opacity_effect, b"opacity", self)
        self.anim.setDuration(400)
        self.anim.setEasingCurve(QEasingCurve.Type.OutCubic)
        self.anim.finished.connect(self._on_anim_finished)

        self._apply_from_state()
        self._apply_style()
        self._fade_in()

        self.check_timer = QTimer(self)
        self.check_timer.timeout.connect(self._check_close)
        self.check_timer.start(200)

    def showEvent(self, event):
        super().showEvent(event)
        self.raise_()
        self.activateWindow()
        self._mark_ready()
        self._persist_geometry()

    def _apply_from_state(self):
        state = read_state_safe(self.state_path)
        data = state.get("data") if isinstance(state, dict) else None
        geometry = data.get("geometry") if isinstance(data, dict) else None
        self._theme = data.get("theme") if isinstance(data, dict) else None
        self._bg_path = data.get("bg_path") if isinstance(data, dict) else None

        if self._bg_path:
            try:
                pix = QPixmap(self._bg_path)
                if not pix.isNull():
                    self._bg_pixmap = pix
            except Exception:
                self._bg_pixmap = None

        if isinstance(geometry, dict):
            x = geometry.get("x", 0)
            y = geometry.get("y", 0)
            w = geometry.get("w", 520)
            h = geometry.get("h", 600)
            self.setGeometry(int(x), int(y), int(w), int(h))
        else:
            screen = QApplication.primaryScreen()
            if screen:
                geo = screen.availableGeometry()
                self.setGeometry(geo)
            else:
                self.resize(520, 600)

        self._update_background()

    def _apply_style(self):
        if self._theme == "light":
            overlay_bg = "rgba(255, 255, 255, 0.85)"
            text_color = "#111827"
            text_shadow = "0 1px 3px rgba(0, 0, 0, 0.1)"
        else:
            overlay_bg = "rgba(15, 23, 42, 0.80)"
            text_color = "#F9FAFB"
            text_shadow = "0 2px 4px rgba(0, 0, 0, 0.3)"
        self.container.setStyleSheet(
            f"QFrame#RestartOverlayContainer {{ background-color: {overlay_bg}; }}"
        )
        self.label.setStyleSheet(
            "QLabel#RestartOverlayLabel {"
            f"color: {text_color};"
            "font-size: 20px;"
            "font-weight: 600;"
            "letter-spacing: 1px;"
            f"text-shadow: {text_shadow};"
            "}"
        )

    def _update_state(self, **updates):
        state = read_state_safe(self.state_path)
        data = state.get("data") if isinstance(state, dict) else {}
        if not isinstance(data, dict):
            data = {}
        if "reason" not in data:
            data["reason"] = "restart_overlay"
        data.update(updates)
        payload = build_state_payload(
            data,
            app_version=state.get("app_version") if isinstance(state, dict) else None,
        )
        write_state_atomic(self.state_path, payload)

    def _mark_ready(self):
        try:
            self._update_state(ready=True)
        except Exception:
            pass

    def _persist_geometry(self):
        geo = self.geometry()
        self._update_state(
            geometry={
                "x": int(geo.x()),
                "y": int(geo.y()),
                "w": int(geo.width()),
                "h": int(geo.height()),
            }
        )

    def _update_background(self):
        if not hasattr(self, "bg_label"):
            return
        self.bg_label.setGeometry(self.rect())
        if self._bg_pixmap is not None:
            scaled = self._bg_pixmap.scaled(
                self.size(),
                Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                Qt.TransformationMode.SmoothTransformation,
            )
            self.bg_label.setPixmap(scaled)
        self._apply_round_mask()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._update_background()

    def _apply_round_mask(self):
        radius = 16.0
        rect = self.rect()
        path = QPainterPath()
        path.addRoundedRect(rect, radius, radius)
        region = QRegion(path.toFillPolygon().toPolygon())
        self.setMask(region)

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self._drag_offset = (
                event.globalPosition().toPoint() - self.frameGeometry().topLeft()
            )
            event.accept()

    def mouseMoveEvent(self, event):
        if event.buttons() == Qt.MouseButton.LeftButton and self._drag_offset:
            self.move(event.globalPosition().toPoint() - self._drag_offset)
            if not self._persist_timer.isActive():
                self._persist_timer.start(120)
            event.accept()

    def mouseReleaseEvent(self, event):
        if self._drag_offset:
            self._drag_offset = None
            self._persist_geometry()
        event.accept()

    def _fade_in(self):
        self._hiding = False
        self.anim.stop()
        self.anim.setStartValue(self.opacity_effect.opacity())
        self.anim.setEndValue(1.0)
        self.anim.start()

    def _fade_out(self):
        if self._hiding:
            return
        self._hiding = True
        self.anim.stop()
        self.anim.setDuration(800)
        self.anim.setEasingCurve(QEasingCurve.Type.InCubic)
        self.anim.setStartValue(self.opacity_effect.opacity())
        self.anim.setEndValue(0.0)
        self.anim.start()

    def _on_anim_finished(self):
        if self._hiding:
            try:
                cleanup_state(self.state_path)
            except Exception:
                pass
            try:
                if self._bg_path and Path(self._bg_path).exists():
                    Path(self._bg_path).unlink()
            except Exception:
                pass
            self.close()
            QApplication.quit()

    def _check_close(self):
        state = read_state_safe(self.state_path)
        if not state:
            self._fade_out()
            return
        data = state.get("data") if isinstance(state, dict) else None
        if isinstance(data, dict) and data.get("reason") == "close":
            self._fade_out()


def main():
    state_path = None
    if len(sys.argv) > 1:
        state_path = Path(sys.argv[1])
    if not state_path:
        state_path = get_user_data_dir() / "restart_overlay_state.json"

    app = QApplication(sys.argv)
    window = RestartOverlayWindow(state_path)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

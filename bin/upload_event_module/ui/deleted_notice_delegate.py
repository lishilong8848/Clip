# -*- coding: utf-8 -*-
from __future__ import annotations

import time
import weakref

from PyQt6 import sip
from PyQt6.QtCore import QEvent, QRect, QSize, Qt, QTimer
from PyQt6.QtGui import QColor, QFont, QPainter, QPen
from PyQt6.QtWidgets import QStyle, QStyledItemDelegate

from .deleted_notice_model import DeletedNoticeModel


class DeletedNoticeDelegate(QStyledItemDelegate):
    """Delegate renderer for recently deleted notice restore rows."""

    CONFIRM_SECONDS = 3.0

    def __init__(self, parent=None):
        super().__init__(parent)
        self._confirm_until: dict[str, float] = {}

    @staticmethod
    def _button_rect(option) -> QRect:
        rect = option.rect.adjusted(6, 5, -6, -5)
        return QRect(rect.right() - 92, rect.top() + 22, 82, 28)

    @staticmethod
    def _key_for_record(record: dict) -> str:
        return str(record.get("undo_id") or record.get("title") or "").strip()

    def _confirm_active(self, record: dict) -> bool:
        key = self._key_for_record(record)
        if not key:
            return False
        expire_at = self._confirm_until.get(key, 0.0)
        if expire_at > time.monotonic():
            return True
        self._confirm_until.pop(key, None)
        return False

    def sizeHint(self, option, index):  # noqa: N802
        if bool(index.data(DeletedNoticeModel.EmptyRole)):
            return QSize(max(360, option.rect.width()), 54)
        return QSize(max(360, option.rect.width()), 76)

    def paint(self, painter: QPainter, option, index) -> None:
        painter.save()
        rect = option.rect.adjusted(6, 5, -6, -5)
        record = index.data(DeletedNoticeModel.DataRole) or {}
        is_empty = bool(index.data(DeletedNoticeModel.EmptyRole))
        selected = bool(option.state & QStyle.StateFlag.State_Selected)
        hovered = bool(option.state & QStyle.StateFlag.State_MouseOver)

        bg = QColor("#111827")
        if selected:
            bg = QColor("#1D4ED8")
        elif hovered:
            bg = QColor("#1F2937")
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setPen(QPen(QColor("#374151"), 1))
        painter.setBrush(bg)
        painter.drawRoundedRect(rect, 8, 8)

        x = rect.left() + 14
        y = rect.top() + 12
        right_pad = 18 if is_empty else 112
        painter.setPen(QColor("#F9FAFB"))
        painter.setFont(QFont("Microsoft YaHei", 10, QFont.Weight.Bold))
        painter.drawText(
            QRect(x, y, rect.width() - right_pad, 22),
            Qt.TextFlag.TextSingleLine,
            str(index.data(DeletedNoticeModel.TitleRole) or "")[:48],
        )
        meta = str(index.data(DeletedNoticeModel.MetaRole) or "").strip()
        if meta:
            painter.setPen(QColor("#9CA3AF"))
            painter.setFont(QFont("Microsoft YaHei", 9))
            painter.drawText(
                QRect(x, y + 28, rect.width() - right_pad, 20),
                Qt.TextFlag.TextSingleLine,
                meta[:64],
            )

        if not is_empty:
            button = self._button_rect(option)
            confirm = self._confirm_active(record)
            painter.setPen(QPen(QColor("#2563EB" if confirm else "#4B5563"), 1))
            painter.setBrush(QColor("#1D4ED8" if confirm else "#111827"))
            painter.drawRoundedRect(button, 12, 12)
            painter.setPen(QColor("#FFFFFF" if confirm else "#D1D5DB"))
            painter.setFont(QFont("Microsoft YaHei", 9, QFont.Weight.Bold))
            painter.drawText(
                button,
                Qt.AlignmentFlag.AlignCenter,
                "ok" if confirm else "回退删除",
            )
        painter.restore()

    def editorEvent(self, event, model, option, index):  # noqa: N802
        if (
            event.type() != QEvent.Type.MouseButtonRelease
            or event.button() != Qt.MouseButton.LeftButton
            or not index.isValid()
            or bool(index.data(DeletedNoticeModel.EmptyRole))
        ):
            return super().editorEvent(event, model, option, index)
        pos = event.position().toPoint() if hasattr(event, "position") else event.pos()
        if not self._button_rect(option).contains(pos):
            return True
        record = index.data(DeletedNoticeModel.DataRole) or {}
        key = self._key_for_record(record)
        if not key:
            return True
        if self._confirm_active(record):
            self._confirm_until.pop(key, None)
            if hasattr(model, "undoRequested"):
                model.undoRequested.emit(dict(record))
            return True
        self._confirm_until[key] = time.monotonic() + self.CONFIRM_SECONDS
        widget = getattr(option, "widget", None)
        if widget is not None:
            widget.viewport().update(option.rect)
            view_ref = weakref.ref(widget)
            update_rect = QRect(option.rect)

            def _refresh_after_timeout():
                view = view_ref()
                if view is None:
                    return
                try:
                    if sip.isdeleted(view):
                        return
                    view.viewport().update(update_rect)
                except Exception:
                    return

            QTimer.singleShot(int(self.CONFIRM_SECONDS * 1000) + 80, _refresh_after_timeout)
        return True

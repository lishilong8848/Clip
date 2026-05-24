# -*- coding: utf-8 -*-
from __future__ import annotations

import time
import weakref

from PyQt6.QtCore import QEvent, QRect, QSize, Qt, QTimer
from PyQt6.QtGui import QColor, QFont, QPainter, QPen
from PyQt6.QtWidgets import QStyle, QStyledItemDelegate, QStyleOptionViewItem
from PyQt6 import sip

from .active_notice_model import ActiveNoticeModel


class ActiveNoticeDelegate(QStyledItemDelegate):
    """Delegate renderer for future QListView active notice cards."""

    DELETE_CONFIRM_SECONDS = 3.0

    def __init__(self, parent=None):
        super().__init__(parent)
        self._delete_confirm_until: dict[str, float] = {}

    @staticmethod
    def _button_rects(option: QStyleOptionViewItem, index) -> dict[str, QRect]:
        rect = option.rect.adjusted(6, 5, -6, -5)
        record = index.data(ActiveNoticeModel.DataRole) or {}
        y = rect.top() + 48
        right = rect.right() - 14
        buttons: dict[str, QRect] = {}
        buttons["delete"] = QRect(right - 48, y, 48, 26)
        right -= 56
        label = ActiveNoticeModel.action_label_for_record(record)
        if label:
            buttons["action"] = QRect(right - 76, y, 76, 26)
            right -= 84
        if ActiveNoticeModel.supports_today_progress(record):
            buttons["today"] = QRect(right - 88, y, 88, 26)
        return buttons

    def _delete_key_for_record(self, record: dict) -> str:
        return ActiveNoticeModel.identity_for_record(record)

    def _delete_confirm_active(self, record: dict) -> bool:
        key = self._delete_key_for_record(record)
        if not key:
            return False
        expire_at = self._delete_confirm_until.get(key, 0)
        if expire_at > time.monotonic():
            return True
        self._delete_confirm_until.pop(key, None)
        return False

    def sizeHint(self, option, index):  # noqa: N802
        return QSize(max(360, option.rect.width()), 88)

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index) -> None:
        painter.save()
        rect = option.rect.adjusted(6, 5, -6, -5)
        record = index.data(ActiveNoticeModel.DataRole) or {}
        selected = bool(option.state & QStyle.StateFlag.State_Selected)
        hovered = bool(option.state & QStyle.StateFlag.State_MouseOver)
        origin = str(index.data(ActiveNoticeModel.OriginRole) or "")
        title = str(index.data(ActiveNoticeModel.TitleRole) or "")
        subtitle = str(index.data(ActiveNoticeModel.SubtitleRole) or "")
        notice_type = str(index.data(ActiveNoticeModel.NoticeTypeRole) or "")

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
        if origin == "portal":
            painter.setPen(QColor("#FACC15"))
            painter.setFont(QFont("Microsoft YaHei", 12, QFont.Weight.Bold))
            painter.drawText(QRect(x, y, 18, 18), Qt.AlignmentFlag.AlignCenter, "★")
            x += 24

        painter.setPen(QColor("#F9FAFB"))
        painter.setFont(QFont("Microsoft YaHei", 11, QFont.Weight.Bold))
        painter.drawText(
            QRect(x, y, rect.right() - x - 96, 22),
            Qt.TextFlag.TextSingleLine,
            title[:42],
        )

        chip_rect = QRect(rect.right() - 86, y, 72, 22)
        painter.setPen(QColor("#93C5FD"))
        painter.setBrush(QColor("#1E3A8A"))
        painter.drawRoundedRect(chip_rect, 11, 11)
        painter.setFont(QFont("Microsoft YaHei", 8, QFont.Weight.Medium))
        painter.drawText(chip_rect, Qt.AlignmentFlag.AlignCenter, notice_type or "通告")

        painter.setPen(QColor("#9CA3AF"))
        painter.setFont(QFont("Microsoft YaHei", 9))
        painter.drawText(
            QRect(rect.left() + 14, y + 28, rect.width() - 238, 20),
            Qt.TextFlag.TextSingleLine,
            subtitle[:58],
        )

        self._paint_buttons(painter, option, index, record)
        painter.restore()

    def _paint_buttons(self, painter: QPainter, option, index, record: dict) -> None:
        buttons = self._button_rects(option, index)

        if "today" in buttons:
            state = ActiveNoticeModel.normalize_today_progress_state(
                record.get("today_in_progress_state")
            )
            if state == "yes":
                border = QColor("#15803D")
                text_color = QColor("#FFFFFF")
                fill = QColor("#16A34A")
            elif state == "no":
                border = QColor("#B91C1C")
                text_color = QColor("#FCA5A5")
                fill = QColor("#450A0A")
            else:
                border = QColor("#4B5563")
                text_color = QColor("#D1D5DB")
                fill = QColor("#111827")
            painter.setPen(QPen(border, 1))
            painter.setBrush(fill)
            painter.drawRoundedRect(buttons["today"], 11, 11)
            painter.setPen(text_color)
            painter.setFont(QFont("Microsoft YaHei", 8, QFont.Weight.Medium))
            painter.drawText(
                buttons["today"],
                Qt.AlignmentFlag.AlignCenter,
                ActiveNoticeModel.today_progress_label(record),
            )

        if "action" in buttons:
            uploading = ActiveNoticeModel.is_uploading_record(record)
            uploaded = ActiveNoticeModel.is_uploaded_record(record)
            if uploading:
                border = QColor("#D97706")
                text_color = QColor("#FDE68A")
                fill = QColor("#451A03")
            elif uploaded:
                border = QColor("#16A34A")
                text_color = QColor("#BBF7D0")
                fill = QColor("#052E16")
            else:
                border = QColor("#2563EB")
                text_color = QColor("#FFFFFF")
                fill = QColor("#1D4ED8")
            painter.setPen(QPen(border, 1))
            painter.setBrush(fill)
            painter.drawRoundedRect(buttons["action"], 11, 11)
            painter.setPen(text_color)
            painter.setFont(QFont("Microsoft YaHei", 9, QFont.Weight.Bold))
            painter.drawText(
                buttons["action"],
                Qt.AlignmentFlag.AlignCenter,
                ActiveNoticeModel.action_label_for_record(record),
            )

        confirm_delete = self._delete_confirm_active(record)
        painter.setPen(QPen(QColor("#B91C1C" if confirm_delete else "#7F1D1D"), 1))
        painter.setBrush(QColor("#991B1B" if confirm_delete else "#3F1212"))
        painter.drawRoundedRect(buttons["delete"], 11, 11)
        painter.setPen(QColor("#FCA5A5"))
        painter.setFont(QFont("Microsoft YaHei", 8, QFont.Weight.Bold))
        painter.drawText(
            buttons["delete"],
            Qt.AlignmentFlag.AlignCenter,
            "确认移" if confirm_delete else "移",
        )

    def editorEvent(self, event, model, option, index):  # noqa: N802
        if (
            event.type() != QEvent.Type.MouseButtonRelease
            or event.button() != Qt.MouseButton.LeftButton
            or not index.isValid()
        ):
            return super().editorEvent(event, model, option, index)
        record = index.data(ActiveNoticeModel.DataRole) or {}
        pos = event.position().toPoint() if hasattr(event, "position") else event.pos()
        buttons = self._button_rects(option, index)
        if buttons.get("delete") and buttons["delete"].contains(pos):
            key = self._delete_key_for_record(record)
            if not key:
                return True
            if self._delete_confirm_active(record):
                self._delete_confirm_until.pop(key, None)
                if hasattr(model, "deleteRequested"):
                    model.deleteRequested.emit(dict(record))
            else:
                self._delete_confirm_until[key] = (
                    time.monotonic() + self.DELETE_CONFIRM_SECONDS
                )
                widget = getattr(option, "widget", None)
                if widget is not None:
                    widget.viewport().update(option.rect)
                    view_ref = weakref.ref(widget)
                    update_rect = QRect(option.rect)

                    def _refresh_after_confirm_timeout():
                        view = view_ref()
                        if view is None:
                            return
                        try:
                            if sip.isdeleted(view):
                                return
                            view.viewport().update(update_rect)
                        except Exception:
                            return

                    QTimer.singleShot(
                        int(self.DELETE_CONFIRM_SECONDS * 1000) + 80,
                        _refresh_after_confirm_timeout,
                    )
            return True
        if buttons.get("today") and buttons["today"].contains(pos):
            if hasattr(model, "todayProgressRequested"):
                model.todayProgressRequested.emit(
                    dict(record), ActiveNoticeModel.next_today_progress_state(record)
                )
            return True
        if buttons.get("action") and buttons["action"].contains(pos):
            action = ActiveNoticeModel.action_for_record(record)
            if action and hasattr(model, "actionRequested"):
                model.actionRequested.emit(dict(record), action)
            return True
        if hasattr(model, "recordActivated"):
            model.recordActivated.emit(dict(record))
            return True
        return super().editorEvent(event, model, option, index)

# -*- coding: utf-8 -*-
import os
import json
import time
from PyQt6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QListView,
    QListWidget,
    QStackedWidget,
    QSystemTrayIcon,
    QStyle,
    QMessageBox,
    QApplication,
    QFrame,
    QMenu,
    QAbstractItemView,
    QGraphicsOpacityEffect,
    QGraphicsDropShadowEffect,
    QCheckBox,
)
from PyQt6.QtCore import (
    Qt,
    QTimer,
    QPropertyAnimation,
    QEasingCurve,
)
from PyQt6.QtGui import QIcon, QCursor, QAction, QColor
from PyQt6 import sip

from ..config import config
from ..logger import log_info, log_error, log_warning
from ..utils import HISTORY_FILE, ICON_FILE
from ..core.parser import extract_event_info
from ..core.speech import speech_manager
from lan_bitable_template_portal.state_store import LanPortalStateStore
from .styles import get_stylesheet
from .dialogs import AddDialog
from .active_notice_delegate import ActiveNoticeDelegate
from .active_notice_model import ActiveNoticeListRoute
from .display_state import normalize_active_item_data
from .common import show_toast_message

HISTORY_RETENTION_DAYS = 7
HISTORY_RETENTION_MS = HISTORY_RETENTION_DAYS * 24 * 60 * 60 * 1000
HISTORY_STATE_NAMESPACE = "qt_notice_history"
HISTORY_STATE_KEY = "history"

class MainWindowUiMixin:
    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(5, 5, 5, 5)
        self.container = QFrame()
        self.container.setObjectName("MainWindow")
        container_layout = QVBoxLayout(self.container)

        # 事件中转状态栏
        status_layout = QHBoxLayout()
        self.relay_status_label = QLabel("事件中转: 初始化中...")
        self.relay_status_label.setStyleSheet("color: #F59E0B; font-size: 11px;")
        status_layout.addWidget(self.relay_status_label)
        self.remote_update_status_label = QLabel("远程更新: 初始化中...")
        self.remote_update_status_label.setStyleSheet(
            "color: #F59E0B; font-size: 11px;"
        )
        status_layout.addWidget(self.remote_update_status_label)
        self.version_label = QLabel(f"版本: {self._build_display_version()}")
        self.version_label.setStyleSheet("color: #94A3B8; font-size: 11px;")
        status_layout.addWidget(self.version_label)
        self.clipboard_toggle = QCheckBox("暂停剪贴板监听")
        self.clipboard_toggle.setStyleSheet("font-size: 11px;")
        self.clipboard_toggle.setChecked(self._is_clipboard_listener_disabled())
        self.clipboard_toggle.toggled.connect(self._on_clipboard_toggle_changed)
        status_layout.addWidget(self.clipboard_toggle)
        self.clipboard_status_label = QLabel("剪贴板监听: 初始化中...")
        self.clipboard_status_label.setStyleSheet("color: #F59E0B; font-size: 11px;")
        status_layout.addWidget(self.clipboard_status_label)
        self.clipboard_status_detail_label = QLabel("")
        self.clipboard_status_detail_label.setStyleSheet(
            "color: #F97316; font-size: 11px;"
        )
        self.clipboard_status_detail_label.setVisible(False)
        status_layout.addWidget(self.clipboard_status_detail_label)
        status_layout.addStretch()
        container_layout.addLayout(status_layout)

        notice_tab_container = QFrame()
        notice_tab_container.setObjectName("NoticeTabContainer")
        notice_tab_layout = QHBoxLayout(notice_tab_container)
        notice_tab_layout.setContentsMargins(6, 4, 6, 4)
        notice_tab_layout.setSpacing(6)

        self.event_tab_btn = QPushButton("事件通告")
        self.event_tab_btn.setObjectName("NoticeTabBtn")
        self.event_tab_btn.setFixedHeight(34)
        self.event_tab_btn.clicked.connect(lambda: self.set_notice_tab("event"))

        self.other_tab_btn = QPushButton("其他通告")
        self.other_tab_btn.setObjectName("NoticeTabBtn")
        self.other_tab_btn.setFixedHeight(34)
        self.other_tab_btn.clicked.connect(lambda: self.set_notice_tab("other"))

        notice_tab_layout.addWidget(self.event_tab_btn, 1)
        notice_tab_layout.addWidget(self.other_tab_btn, 1)
        container_layout.addWidget(notice_tab_container)

        nav_layout = QHBoxLayout()
        self.patch_btn = QPushButton("更新")
        self.patch_btn.setObjectName("PatchBtn")
        self.patch_btn.setFixedSize(50, 36)
        self.patch_btn.setToolTip("检测到补丁，点击更新")
        self.patch_btn.setStyleSheet(
            "background-color: #EF4444; color: white; border-radius: 6px;"
        )
        self.patch_btn.clicked.connect(self.apply_patch_update)
        self.patch_btn.hide()

        self.title_label = QLabel("剪贴板助手")
        self.title_label.setObjectName("TitleLabel")
        self.title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.add_btn = QPushButton("手动添加")
        self.add_btn.setObjectName("AddBtn")
        self.add_btn.setFixedSize(80, 40)
        self.add_btn.setToolTip("手动添加内容")
        self.add_btn.clicked.connect(self.open_manual_add)

        self.preview_btn = QPushButton("最近剪贴板")
        self.preview_btn.setObjectName("NavBtn")
        self.preview_btn.setFixedSize(92, 40)
        self.preview_btn.setToolTip("查看最后一条剪贴板内容")
        self.preview_btn.clicked.connect(self.open_clipboard_preview)

        self.theme_btn = QPushButton("☀️")
        self.theme_btn.setObjectName("ThemeBtn")
        self.theme_btn.setFixedSize(36, 36)
        self.theme_btn.setToolTip("切换主题")
        self.theme_btn.clicked.connect(self.toggle_theme)

        self.minimize_btn = QPushButton("─")
        self.minimize_btn.setObjectName("MinimizeBtn")
        self.minimize_btn.setFixedSize(36, 36)
        self.minimize_btn.setToolTip("最小化")
        self.minimize_btn.clicked.connect(self.hide)

        nav_layout.addWidget(self.patch_btn)
        nav_layout.addWidget(self.title_label, 1)
        nav_layout.addWidget(self.theme_btn)
        nav_layout.addWidget(self.minimize_btn)
        nav_layout.addWidget(self.preview_btn)
        nav_layout.addWidget(self.add_btn)

        self.stack = QStackedWidget()
        self.active_container = QWidget()
        active_layout = QVBoxLayout(self.active_container)
        active_layout.setContentsMargins(0, 0, 0, 0)

        self.active_stack = QStackedWidget()
        if self._active_model_view_visible():
            self.list_active_event = ActiveNoticeListRoute("event")
            self.list_active_other = ActiveNoticeListRoute("other")
            self.view_active_event = QListView()
            self._init_active_model_view(self.view_active_event, self.list_active_event)
            self.view_active_other = QListView()
            self._init_active_model_view(self.view_active_other, self.list_active_other)
            self.active_stack.addWidget(self.view_active_event)
            self.active_stack.addWidget(self.view_active_other)
        else:
            self.list_active_event = QListWidget()
            self._init_list_widget(self.list_active_event)
            self.list_active_other = QListWidget()
            self._init_list_widget(self.list_active_other)
            self.view_active_event = None
            self.view_active_other = None
            self.active_stack.addWidget(self.list_active_event)
            self.active_stack.addWidget(self.list_active_other)
        active_layout.addWidget(self.active_stack)

        self.stack.addWidget(self.active_container)
        container_layout.addLayout(nav_layout)
        container_layout.addWidget(self.stack)

        bottom_layout = QHBoxLayout()
        self.settings_btn = QPushButton("⚙ 设置")
        self.settings_btn.setObjectName("SettingsBtn")
        self.settings_btn.setFixedSize(70, 30)
        self.settings_btn.setToolTip("配置接口地址")
        self.settings_btn.clicked.connect(self.open_settings)

        self.ocr_install_btn = QPushButton("🔧 OCR修复")
        self.ocr_install_btn.setObjectName("OcrInstallBtn")
        self.ocr_install_btn.setFixedHeight(30)
        self.ocr_install_btn.setToolTip("一键安装 Windows 中文 OCR 语言包")
        self.ocr_install_btn.clicked.connect(self.install_ocr_lang_pack)

        self.lan_template_portal_btn = QPushButton("模板页面")
        self.lan_template_portal_btn.setObjectName("OcrInstallBtn")
        self.lan_template_portal_btn.setFixedHeight(30)
        self.lan_template_portal_btn.setToolTip("打开局域网模板页面")
        self.lan_template_portal_btn.clicked.connect(self._open_lan_template_portal)

        # 底部表格链接按钮（与设置同一水平）
        self.table_links = [
            ("维保表", "table_id_weibao"),
            ("变更表", "table_id_biangeng"),
            ("调整表", "table_id_tiaozheng"),
            ("事件表", "table_id_shijian"),
            ("上下电", "table_id_power"),
            ("轮巡表", "table_id_polling"),
            ("检修表", "table_id_overhaul"),
        ]
        self.table_link_widget = QWidget()
        table_link_layout = QHBoxLayout(self.table_link_widget)
        table_link_layout.setContentsMargins(6, 0, 0, 0)
        table_link_layout.setSpacing(6)
        self.table_link_buttons = []
        for label, attr in self.table_links:
            btn = QPushButton(label)
            btn.setObjectName("TableLinkBtn")
            btn.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
            btn.setFixedHeight(26)
            btn.clicked.connect(lambda _, a=attr: self._open_table_link(a))
            if not self._disable_effects:
                # 修复: shadow 的父对象应该是 btn,而不是 self,避免访问违规
                shadow = QGraphicsDropShadowEffect(btn)
                shadow.setBlurRadius(10)
                shadow.setOffset(0, 1)
                shadow.setColor(QColor(59, 130, 246, 90))
                btn.setGraphicsEffect(shadow)
            table_link_layout.addWidget(btn)
            self.table_link_buttons.append((btn, attr))

        bottom_layout.addWidget(self.settings_btn)
        bottom_layout.addWidget(self.ocr_install_btn)
        bottom_layout.addWidget(self.lan_template_portal_btn)
        bottom_layout.addWidget(self.table_link_widget)
        bottom_layout.addStretch()
        container_layout.addLayout(bottom_layout)

        main_layout.addWidget(self.container)
        self._init_update_overlay()
        self.apply_theme(self.current_theme)
        self.refresh_lan_template_portal_link()
        self.refresh_table_links()
        self.set_notice_tab("event")
        self._refresh_patch_button()

    def _active_model_view_visible(self) -> bool:
        if os.environ.get("CLIPFLOW_LEGACY_QT_WIDGET_LIST") == "1":
            return False
        if os.environ.get("CLIPFLOW_DISABLE_QT_MODEL_VIEW") == "1":
            return False
        enabled = getattr(self, "_active_notice_model_enabled", None)
        return bool(enabled() if callable(enabled) else True)

    def _active_stack_widget_for_tab(self, is_event: bool):
        if self._active_model_view_visible() and getattr(self, "view_active_event", None):
            return self.view_active_event if is_event else self.view_active_other
        return self.list_active_event if is_event else self.list_active_other

    def _current_visible_content_widget(self):
        return self._active_stack_widget_for_tab(self.notice_tab == "event")

    def _active_view_for_backing_list(self, list_widget):
        if not self._active_model_view_visible():
            return None
        if list_widget is getattr(self, "list_active_event", None):
            return getattr(self, "view_active_event", None)
        if list_widget is getattr(self, "list_active_other", None):
            return getattr(self, "view_active_other", None)
        return None

    def _scroll_active_model_view_to_item(self, list_widget, item):
        view = self._active_view_for_backing_list(list_widget)
        if view is None or item is None or not self._is_valid_list_item(item):
            return
        model = view.model()
        if model is None:
            return
        try:
            row = self._active_item_row(list_widget, item)
            if row < 0:
                return
            index = model.index(row, 0)
            if not index.isValid():
                return
            view.setCurrentIndex(index)
            view.scrollTo(index)
        except Exception:
            return

    def _init_active_model_view(self, view: QListView, backing_list):
        model = self._active_notice_model_for_list(backing_list)
        view.setModel(model)
        view.setItemDelegate(ActiveNoticeDelegate(view))
        view.setMouseTracking(True)
        view.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        view.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        view.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        view.setSpacing(2)
        view.setUniformItemSizes(False)
        view.setObjectName("ActiveNoticeView")
        view.setStyleSheet(
            "QListView#ActiveNoticeView { background: transparent; border: none; outline: 0; }"
        )
        try:
            view.viewport().setAutoFillBackground(False)
        except Exception:
            pass
        if model is not None:
            model.recordActivated.connect(
                lambda data, lw=backing_list: self._open_active_model_record(lw, data)
            )
            model.actionRequested.connect(
                lambda data, action, lw=backing_list: self._handle_active_model_action(
                    lw, data, action
                )
            )
            model.todayProgressRequested.connect(
                lambda data, state, lw=backing_list: self._handle_active_model_today(
                    lw, data, state
                )
            )
            model.deleteRequested.connect(
                lambda data, lw=backing_list: self._handle_active_model_delete(lw, data)
            )

    def _active_model_current_data(self, backing_list, data_dict: dict):
        item = self._active_model_backing_item(backing_list, data_dict)
        if item and self._is_valid_list_item(item):
            current = self._active_item_data(item)
            if isinstance(current, dict):
                return item, current
        return item, data_dict if isinstance(data_dict, dict) else {}

    def _active_model_backing_item(self, backing_list, data_dict: dict):
        if not isinstance(data_dict, dict):
            return None
        active_item_id = str(data_dict.get("active_item_id") or "").strip()
        record_id = str(data_dict.get("record_id") or "").strip()
        candidates = []
        if active_item_id:
            candidates.append(self._find_active_item_by_active_item_id(active_item_id))
        if record_id:
            candidates.append(self._find_active_item_by_record_id(record_id))
        for list_widget, item in candidates:
            if list_widget is backing_list and item and self._is_valid_list_item(item):
                return item
        return None

    def _open_active_model_record(self, backing_list, data_dict: dict):
        item, _current = self._active_model_current_data(backing_list, data_dict)
        if item and self._is_valid_list_item(item):
            self.on_item_clicked(item)

    def _handle_active_model_action(self, backing_list, data_dict: dict, action: str):
        _item, current = self._active_model_current_data(backing_list, data_dict)
        if isinstance(current, dict) and action:
            self.handle_action(dict(current), action)

    def _handle_active_model_today(self, backing_list, data_dict: dict, state: str):
        _item, current = self._active_model_current_data(backing_list, data_dict)
        if isinstance(current, dict) and state:
            self._handle_today_in_progress_toggle(dict(current), state)

    def _handle_active_model_delete(self, backing_list, data_dict: dict):
        _item, current = self._active_model_current_data(backing_list, data_dict)
        if isinstance(current, dict):
            self._delete_active_item(dict(current))

    def _current_list_widget(self):
        return (
            self.list_active_event
            if self.notice_tab == "event"
            else self.list_active_other
        )

    def _refresh_tab_button(self, button):
        button.style().unpolish(button)
        button.style().polish(button)

    def set_notice_tab(self, tab):
        if tab not in ("event", "other"):
            return
        self.notice_tab = tab
        is_event = tab == "event"
        self.event_tab_btn.setProperty("active", is_event)
        self.event_tab_btn.setProperty("attention", False)
        self.other_tab_btn.setProperty("active", not is_event)
        self.other_tab_btn.setProperty("attention", False)
        self._refresh_tab_button(self.event_tab_btn)
        self._refresh_tab_button(self.other_tab_btn)

        self.active_stack.setCurrentWidget(self._active_stack_widget_for_tab(is_event))
        current_list = self._current_list_widget()
        self._fade_in_widget(self._current_visible_content_widget())
        if hasattr(self, "_schedule_active_list_virtualization_refresh"):
            self._schedule_active_list_virtualization_refresh(current_list, 0)
            self._schedule_active_list_virtualization_refresh(current_list, 250)

    def _fade_in_widget(self, widget):
        if not widget:
            return
        if getattr(self, "_disable_effects", False):
            return
        if isinstance(widget, QAbstractItemView):
            # QGraphicsOpacityEffect can leave native item-view viewports white or
            # transparent after rapid stacked-widget switches. Keep list repainting
            # plain and immediate; the tab button state still provides feedback.
            if isinstance(widget.graphicsEffect(), QGraphicsOpacityEffect):
                widget.setGraphicsEffect(None)
            try:
                widget.viewport().update()
            except Exception:
                pass
            widget.update()
            return
        effect = widget.graphicsEffect()
        if not isinstance(effect, QGraphicsOpacityEffect):
            effect = QGraphicsOpacityEffect(widget)
            widget.setGraphicsEffect(effect)
        effect.setOpacity(0.0)
        anim = QPropertyAnimation(effect, b"opacity", widget)
        anim.setDuration(180)
        anim.setStartValue(0.0)
        anim.setEndValue(1.0)
        anim.setEasingCurve(QEasingCurve.Type.OutCubic)
        anim.start()
        widget._fade_anim = anim

    def _pin_item_to_top(self, list_widget, item):
        if list_widget is None or item is None:
            return
        if not self._is_valid_list_item(item):
            return
        row = self._active_item_row(list_widget, item)
        if row == -1:
            return
        if row <= 0:
            return
        if not self._move_active_item_to_row(list_widget, item, 0):
            return
        try:
            view = self._active_view_for_backing_list(list_widget)
            if view is not None:
                index = view.model().index(0, 0)
                view.setCurrentIndex(index)
                view.scrollTo(index)
            else:
                list_widget.setCurrentItem(item)
                list_widget.scrollToItem(item)
                self._scroll_active_model_view_to_item(list_widget, item)
        except Exception:
            return
        if hasattr(self, "_schedule_active_list_virtualization_refresh"):
            self._schedule_active_list_virtualization_refresh(list_widget, 0)
            self._schedule_active_list_virtualization_refresh(list_widget, 250)

    def _set_view_mode(self, active):
        self.stack.setCurrentWidget(self.active_container)
        self.title_label.setText("剪贴板助手")
        self.add_btn.show()
        current_list = self._current_list_widget()
        self._fade_in_widget(self._current_visible_content_widget())
        if hasattr(self, "_schedule_active_list_virtualization_refresh"):
            self._schedule_active_list_virtualization_refresh(current_list, 0)
            self._schedule_active_list_virtualization_refresh(current_list, 250)

    def _focus_event_tab(self, switch_to_active=True):
        self.set_notice_tab("event")
        if switch_to_active and not self._is_active_view():
            self._set_view_mode(True)

    def _notify_new_event(self, widget):
        if not self._alerts_enabled():
            return
        speech_manager.speak("来事件了，请及时处理")
        if widget:
            widget.trigger_flash()

    def _alerts_enabled(self):
        return not getattr(config, "disable_alerts", False)

    def _maybe_speak(self, text):
        if self._alerts_enabled():
            speech_manager.speak(text)

    def _maybe_flash(self, widget, list_widget=None, item=None):
        if not self._alerts_enabled():
            return
        if widget:
            try:
                if sip.isdeleted(widget):
                    return
            except Exception:
                return
            widget.trigger_flash()
        if list_widget is not None and item is not None:
            try:
                if self._active_item_row(list_widget, item) != -1:
                    if self._active_model_view_visible():
                        self._scroll_active_model_view_to_item(list_widget, item)
                    else:
                        list_widget.scrollToItem(item)
                        self._scroll_active_model_view_to_item(list_widget, item)
                        if hasattr(self, "_schedule_active_list_virtualization_refresh"):
                            self._schedule_active_list_virtualization_refresh(list_widget, 0)
            except Exception:
                return

    def toggle_theme(self):
        self.current_theme = "light" if self.current_theme == "dark" else "dark"
        self.theme_btn.setText("☀️" if self.current_theme == "dark" else "🌙")
        self.theme_btn.setToolTip(
            f"切换到{'暗色' if self.current_theme == 'light' else '亮色'}主题"
        )
        self.apply_theme(self.current_theme)
        log_info(f"UI操作: 切换主题到 {self.current_theme}")

    def apply_theme(self, theme):
        stylesheet = get_stylesheet(theme)
        self.setStyleSheet(stylesheet)
        if hasattr(self, "event_tab_btn"):
            self._refresh_tab_button(self.event_tab_btn)
            self._refresh_tab_button(self.other_tab_btn)
        self._update_overlay_style()
        if self.detail_dialog:
            self.detail_dialog.setStyleSheet(stylesheet)
        if self.add_dialog:
            self.add_dialog.apply_theme(theme)
        if self.clipboard_preview_dialog:
            self.clipboard_preview_dialog.apply_theme(theme)
        if self.settings_dialog:
            self.settings_dialog.setStyleSheet(stylesheet)

    def _format_clipboard_snapshot_time(self, timestamp_ms) -> str:
        try:
            ts = int(timestamp_ms or 0)
        except Exception:
            ts = 0
        if ts <= 0:
            return ""
        try:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts / 1000.0))
        except Exception:
            return ""

    def _update_last_clipboard_snapshot(self, content: str, timestamp_ms=None):
        text = str(content or "")
        if not text.strip():
            return
        try:
            ts = int(timestamp_ms or int(time.time() * 1000))
        except Exception:
            ts = int(time.time() * 1000)
        self._last_clipboard_snapshot_text = text
        self._last_clipboard_snapshot_ts = ts
        dialog = getattr(self, "clipboard_preview_dialog", None)
        if dialog:
            dialog.set_content(text, self._format_clipboard_snapshot_time(ts))
        if self.isVisible() and self._clipboard_preview_auto_show_enabled:
            self._sync_clipboard_preview_visibility()

    def _position_clipboard_preview_dialog(self):
        dialog = getattr(self, "clipboard_preview_dialog", None)
        if not dialog:
            return
        try:
            geo = self.geometry()
            screen = QApplication.screenAt(geo.center()) or QApplication.primaryScreen()
            if not screen:
                return
            bounds = screen.availableGeometry()
            x = geo.x() + geo.width() - dialog.width()
            y = geo.y() - dialog.height() - 12
            x = max(bounds.left(), min(x, bounds.right() - dialog.width()))
            y = max(bounds.top(), min(y, bounds.bottom() - dialog.height()))
            dialog.move(x, y)
        except Exception:
            return

    def _show_clipboard_preview(self, force_activate: bool = False):
        dialog = getattr(self, "clipboard_preview_dialog", None)
        if not dialog:
            return
        dialog.apply_theme(self.current_theme)
        dialog.set_content(
            self._last_clipboard_snapshot_text,
            self._format_clipboard_snapshot_time(self._last_clipboard_snapshot_ts),
        )
        if not dialog.isVisible():
            self._position_clipboard_preview_dialog()
            dialog.show()
        if force_activate:
            dialog.activateWindow()
            dialog.raise_()

    def _sync_clipboard_preview_visibility(self):
        dialog = getattr(self, "clipboard_preview_dialog", None)
        if not dialog:
            return
        dialog.set_content(
            self._last_clipboard_snapshot_text,
            self._format_clipboard_snapshot_time(self._last_clipboard_snapshot_ts),
        )
        if self.isVisible() and self._clipboard_preview_auto_show_enabled:
            self._show_clipboard_preview(force_activate=False)
        else:
            dialog.hide()

    def _on_clipboard_preview_closed_by_user(self):
        self._clipboard_preview_auto_show_enabled = False

    def open_clipboard_preview(self):
        self._clipboard_preview_auto_show_enabled = True
        self._show_clipboard_preview(force_activate=True)

    def _clipboard_preview_use_block_reason(self) -> str:
        if self._is_screenshot_dialog_active():
            return "截图上传进行中，暂时不能使用最近剪贴板。"
        if self._is_settings_dialog_active():
            return "设置窗口已打开，请先关闭后再使用最近剪贴板。"
        if self._is_manual_add_dialog_active():
            return "手动添加窗口已打开，请先关闭后再使用最近剪贴板。"
        return ""

    def _open_manual_add_prefilled(self, content: str, hint: str):
        self.open_manual_add()
        if not self.add_dialog:
            return
        self._reopen_manual_add_with_hint(content, hint)

    def _process_manual_clipboard_snapshot(self, content: str):
        text = str(content or "").strip()
        if not text:
            self.show_message("暂无可使用的剪贴板内容。")
            return
        result = self._submit_notice_text_to_backend_projection(
            text,
            source="manual_clipboard",
        )
        if not result.get("ok"):
            hint = "当前内容未识别为有效通告，请检查格式后再确认添加。"
            self.show_message(str(result.get("error") or hint))
            return
        self.show_message("通告已提交后端处理。")

    def _use_last_clipboard_snapshot(self, content: str = ""):
        block_reason = self._clipboard_preview_use_block_reason()
        if block_reason:
            self.show_message(block_reason)
            return
        text = str(content or self._last_clipboard_snapshot_text or "").strip()
        if not text:
            self.show_message("暂无可使用的剪贴板内容。")
            return
        self._process_manual_clipboard_snapshot(text)

    def open_settings(self):
        block_reason = self._dialog_block_reason("settings")
        if block_reason:
            self.show_message(block_reason)
            return
        self.settings_dialog.load_current_settings()
        self.settings_dialog.setStyleSheet(get_stylesheet(self.current_theme))
        if not self.settings_dialog.isVisible():
            geo = self.geometry()
            self.settings_dialog.move(
                geo.x() + (geo.width() - self.settings_dialog.width()) // 2,
                geo.y() + 50,
            )
            self.settings_dialog.show()
        self.settings_dialog.activateWindow()

    def _is_screenshot_dialog_active(self) -> bool:
        try:
            return bool(
                self.current_screenshot_record_id
                or (self.screenshot_dialog and self.screenshot_dialog.isVisible())
            )
        except Exception:
            return bool(self.current_screenshot_record_id)

    def _is_manual_add_dialog_active(self) -> bool:
        try:
            return bool(self.add_dialog and self.add_dialog.isVisible())
        except Exception:
            return False

    def _is_settings_dialog_active(self) -> bool:
        try:
            return bool(self.settings_dialog and self.settings_dialog.isVisible())
        except Exception:
            return False

    def _dialog_block_reason(self, target: str) -> str:
        # target: settings | manual_add | screenshot
        if target == "settings":
            if self._is_screenshot_dialog_active():
                return "截图上传进行中，暂时不能打开设置。"
            if self._is_manual_add_dialog_active():
                return "手动添加窗口已打开，请先关闭后再打开设置。"
            return ""
        if target == "manual_add":
            if self._is_screenshot_dialog_active():
                return "截图上传进行中，暂时不能打开手动添加。"
            if self._is_settings_dialog_active():
                return "设置窗口已打开，请先关闭后再打开手动添加。"
            return ""
        if target == "screenshot":
            if self._is_manual_add_dialog_active():
                return "手动添加窗口已打开，暂时不能打开截图上传。"
            if self._is_settings_dialog_active():
                return "设置窗口已打开，暂时不能打开截图上传。"
            return ""
        return ""

    def open_manual_add(self):
        block_reason = self._dialog_block_reason("manual_add")
        if block_reason:
            self.show_message(block_reason)
            return
        self._pause_clipboard_timer()
        if not self.add_dialog:
            self.add_dialog = AddDialog(self, theme=self.current_theme)
            self.add_dialog.set_record_validator(self._validate_manual_record_id)
            self.add_dialog.set_existing_checker(self._manual_update_has_target)
            self.add_dialog.finished.connect(self._on_manual_add_finished)
            self.add_dialog.accepted.connect(self._on_manual_add_accepted)
        if self.add_dialog.isVisible():
            self.add_dialog.activateWindow()
            return
        self._pause_hot_reload_for_manual_add()
        self.add_dialog.apply_theme(self.current_theme)
        self.add_dialog.reset_state()
        geo = self.geometry()
        self.add_dialog.move(
            geo.x() + (geo.width() - self.add_dialog.width()) // 2, geo.y() + 100
        )
        self.add_dialog.show()
        self.add_dialog.activateWindow()

    def _pause_hot_reload_for_manual_add(self):
        self._manual_add_hot_reload_paused = False
        mgr = getattr(self, "hot_reload_manager", None)
        if not mgr or self._closing or config.disable_hot_reload:
            return
        try:
            mgr.stop()
            self._manual_add_hot_reload_paused = True
            log_info("HotReload: 手动添加弹窗期间已临时暂停")
        except Exception:
            self._manual_add_hot_reload_paused = False

    def _resume_hot_reload_after_manual_add(self):
        if not self._manual_add_hot_reload_paused:
            return
        self._manual_add_hot_reload_paused = False
        if self._closing or config.disable_hot_reload:
            return
        mgr = getattr(self, "hot_reload_manager", None)
        if not mgr:
            return
        try:
            mgr.start()
            log_info("HotReload: 手动添加弹窗关闭后已恢复")
        except Exception as exc:
            log_error(f"HotReload: 恢复失败: {exc}")

    def _on_manual_add_finished(self):
        self._resume_clipboard_timer()
        self._resume_hot_reload_after_manual_add()
        self._try_process_pending_force_uploads()

    def _on_settings_closed(self):
        self._try_process_pending_force_uploads()

    def _reopen_manual_add_with_hint(self, content: str, hint: str):
        if not self.add_dialog:
            return
        try:
            self.add_dialog.text_input.setPlainText(content or "")
            self.add_dialog.id_input.setText("")
            self.add_dialog.hint_label.setText(hint)
        except Exception:
            pass
        self._pause_clipboard_timer()
        self._pause_hot_reload_for_manual_add()
        self.add_dialog.show()
        self.add_dialog.activateWindow()

    def on_item_clicked(self, item):
        if not self._is_valid_list_item(item):
            return
        is_active = self._is_active_view()
        data = item.data(Qt.ItemDataRole.UserRole)
        if data:
            record_id = data.get("record_id", "")
            display_data = self._load_record_from_cache(
                record_id
            ) or normalize_active_item_data(data)
            display_data = self._ensure_active_item_identity(display_data)
            self.detail_dialog.update_content(
                display_data,
                record_id,
                editable=is_active,
                active_item_id=display_data.get("active_item_id", ""),
            )
            if not self.detail_dialog.isVisible():
                geo = self.geometry()
                self.detail_dialog.move(geo.x() - 430, geo.y())
                self.detail_dialog.show()
            self.detail_dialog.activateWindow()

    def _show_screenshot_dialog(self, data, action_type):
        block_reason = self._dialog_block_reason("screenshot")
        if block_reason:
            self.show_message(block_reason)
            self.restore_button_state(record_id=data.get("record_id") if data else None)
            return
        if not config.user_token:
            self.show_message("未配置飞书用户令牌。")
            self.restore_button_state(record_id=data.get("record_id") if data else None)
            return
        if isinstance(data, dict):
            self._hydrate_data_from_cache(data)
        # 同一条记录同一动作已打开，避免重复弹窗
        if (
            self.screenshot_dialog.isVisible()
            and self.current_screenshot_record_id
            == (data.get("record_id") if data else None)
            and self.current_screenshot_action_type == action_type
        ):
            return
        # 并发控制：如果已经有正在进行的截图操作且不是同一个，先恢复上一个的状态
        if (
            self.current_screenshot_record_id
            and self.current_screenshot_record_id != data["record_id"]
        ):
            self.restore_button_state(record_id=self.current_screenshot_record_id)

        self.current_screenshot_record_id = data["record_id"]
        self.current_screenshot_action_type = action_type
        self._current_screenshot_dialog_session_id = ""
        controller = getattr(self, "lan_template_portal_controller", None)
        if controller is not None and hasattr(controller, "create_qt_dialog_session"):
            try:
                created = controller.create_qt_dialog_session(
                    {
                        "type": "screenshot_confirm",
                        "action_type": action_type,
                        "record_id": str(data.get("record_id") or ""),
                        "active_item_id": str(data.get("active_item_id") or ""),
                        "payload": {
                            "notice_type": str(data.get("notice_type") or ""),
                            "title": str(data.get("title") or ""),
                        },
                    }
                )
                self._current_screenshot_dialog_session_id = str(
                    (created or {}).get("session_id") or ""
                ).strip()
            except Exception as exc:
                log_warning(f"截图确认会话创建失败，继续显示本机弹窗: {exc}")
        self._set_delete_interaction_enabled(False)
        # 截图界面打开期间暂停剪贴板监听，避免替换逻辑与截图流程冲突
        self._pause_clipboard_timer()
        # 标记为处理中，避免截图期间触发替换
        list_widget, item = self._find_active_item_by_record_id(
            self.current_screenshot_record_id
        )
        if item and not self._is_valid_list_item(item):
            item = None
            list_widget = None
        if list_widget is not None and item is not None:
            data_dict = item.data(Qt.ItemDataRole.UserRole) or data
            pending_hash = data_dict.get("_pending_upload_hash")
            if not pending_hash:
                pending_hash = self._calc_text_hash(data_dict.get("text", ""))
                data_dict["_pending_upload_hash"] = pending_hash
            data_dict["_has_unuploaded_changes"] = False
            data_dict["_upload_in_progress"] = True
            item.setData(Qt.ItemDataRole.UserRole, data_dict)
            self._rebuild_active_item_widget(
                list_widget,
                item,
                data_dict,
                force_status=None,
                upload_in_progress=True,
                pending_upload_hash=pending_hash,
                has_unuploaded_changes=data_dict.get("_has_unuploaded_changes"),
            )
        self.screenshot_dialog.set_data(data, action_type, is_mandatory=False)
        if not self.screenshot_dialog.isVisible():
            self._position_screenshot_dialog()
            self.screenshot_dialog.show()
        self.screenshot_dialog.activateWindow()

    def _position_screenshot_dialog(self):
        """截图弹窗位置略向左，避免误点主界面后完全遮挡。"""
        geo = self.geometry()
        x = geo.x() + (geo.width() - self.screenshot_dialog.width()) // 2 - 120
        y = geo.y() + 50
        screen = QApplication.primaryScreen()
        if screen:
            bounds = screen.availableGeometry()
            x = max(
                bounds.left() + 10,
                min(x, bounds.right() - self.screenshot_dialog.width() - 10),
            )
            y = max(
                bounds.top() + 10,
                min(y, bounds.bottom() - self.screenshot_dialog.height() - 10),
            )
        self.screenshot_dialog.move(x, y)

    def _flush_pending_cache_refresh(self, *args):
        if self._pending_cache_refresh:
            self._refresh_ui_from_cache()

    def _on_screenshot_started(self):
        """截图开始时隐藏主界面，避免Z轴冲突"""
        self.hide()

    def _on_screenshot_finished(self):
        """截图完成后恢复显示主界面"""
        self.show()

    def on_screenshot_cancelled(self):
        record_id = self.current_screenshot_record_id
        session_id = str(getattr(self, "_current_screenshot_dialog_session_id", "") or "")
        if session_id:
            controller = getattr(self, "lan_template_portal_controller", None)
            if controller is not None and hasattr(controller, "submit_qt_dialog_result"):
                try:
                    controller.submit_qt_dialog_result(
                        session_id,
                        status="cancelled",
                        result_payload={"record_id": record_id or ""},
                    )
                except Exception:
                    pass
        self._current_screenshot_dialog_session_id = ""
        self._set_delete_interaction_enabled(True)
        self._resume_clipboard_timer()
        if record_id:
            self.restore_button_state(
                success=False, record_id=record_id
            )
            self.pending_new_by_record_id.pop(record_id, None)
        self.current_screenshot_record_id = None
        self.current_screenshot_action_type = None
        # 截图取消时恢复显示主界面
        self.show()
        self._flush_pending_cache_refresh()
        self._try_process_pending_force_uploads()
        self._try_process_deferred_events()

    def _get_history_state_store(self):
        store = getattr(self, "_lan_portal_state_store", None)
        if store is None:
            store = LanPortalStateStore()
            self._lan_portal_state_store = store
        return store

    def _load_history_payload(self):
        try:
            stored = self._get_history_state_store().get_document(
                HISTORY_STATE_NAMESPACE, HISTORY_STATE_KEY
            )
        except Exception:
            stored = None
        if isinstance(stored, dict):
            items = stored.get("items")
            if isinstance(items, list):
                return items
        if not os.path.exists(HISTORY_FILE):
            return []
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                history_data = json.load(f)
        except Exception:
            return []
        if not isinstance(history_data, list):
            return []
        self._save_history_payload(history_data)
        return history_data

    def _save_history_payload(self, history_data):
        payload = {
            "version": 1,
            "saved_at": int(time.time() * 1000),
            "items": history_data if isinstance(history_data, list) else [],
        }
        self._history_override_records = list(payload["items"])
        self._get_history_state_store().put_document(
            HISTORY_STATE_NAMESPACE, HISTORY_STATE_KEY, payload
        )
        self.last_history_mtime = payload["saved_at"]

    def _delete_history_payload(self):
        self._history_override_records = []
        self._get_history_state_store().delete_document(
            HISTORY_STATE_NAMESPACE, HISTORY_STATE_KEY
        )
        self.last_history_mtime = int(time.time() * 1000)

    def save_to_history_file(self, d):
        h = self.load_all_history()
        record = dict(d or {}) if isinstance(d, dict) else {}
        if record:
            record["history_saved_at"] = self._coerce_history_saved_at(
                record.get("history_saved_at"), fallback_now=True
            )
        h.insert(0, record)
        h = self._trim_history_by_age(h)
        try:
            self._save_history_payload(h)
        except Exception:
            pass

    def _coerce_history_saved_at(self, value, fallback_now: bool = False):
        try:
            if value is None or value == "":
                raise ValueError("empty history_saved_at")
            numeric = int(value)
            if numeric > 0:
                return numeric
        except Exception:
            pass
        return int(time.time() * 1000) if fallback_now else None

    def _trim_history_by_age(self, history_data):
        if not isinstance(history_data, list):
            return []
        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - HISTORY_RETENTION_MS
        trimmed = []
        for item in history_data:
            if not isinstance(item, dict):
                continue
            history_saved_at = self._coerce_history_saved_at(
                item.get("history_saved_at"), fallback_now=False
            )
            if history_saved_at is None or history_saved_at < cutoff_ms:
                continue
            trimmed.append(item)
        return trimmed

    def load_all_history(self):
        override = getattr(self, "_history_override_records", None)
        history_data = list(override) if isinstance(override, list) else self._load_history_payload()
        updated = False
        for item in history_data:
            if not isinstance(item, dict):
                continue
            text = item.get("text", "")
            info = extract_event_info(text)
            if info and info.get("content") and info["content"] != text:
                item["text"] = info["content"]
                updated = True
            history_saved_at = self._coerce_history_saved_at(
                item.get("history_saved_at"), fallback_now=False
            )
            if history_saved_at is None:
                history_saved_at = self._coerce_history_saved_at(
                    item.get("ts"), fallback_now=True
                )
                item["history_saved_at"] = history_saved_at
                updated = True
        trimmed_history = self._trim_history_by_age(history_data)
        if len(trimmed_history) != len(history_data):
            history_data = trimmed_history
            updated = True
        else:
            history_data = trimmed_history
        if updated:
            try:
                self._save_history_payload(history_data)
            except Exception:
                pass
        return history_data

    def _set_delete_interaction_enabled(self, enabled: bool):
        enabled = bool(enabled)
        self._delete_interaction_enabled = enabled
        try:
            entries = self._active_notice_store().entries()
        except Exception:
            entries = []
        for list_widget, item, _data in entries:
            widget = self._safe_item_widget(list_widget, item)
            if not widget or not hasattr(widget, "set_delete_interaction_enabled"):
                continue
            try:
                widget.set_delete_interaction_enabled(enabled)
            except Exception:
                continue

    def reload_history_view(self):
        return

    def toggle_view_mode(self):
        self._set_view_mode(True)

    def show_context_menu(self, pos, src):
        # 已移除右键删除功能，改为滑动删除
        pass

    def delete_item(self, item, src):
        w = self._safe_item_widget(src, item)
        if w and hasattr(w, "stop_timer"):
            w.stop_timer()
        src.takeItem(src.row(item))

    def setup_tray(self):
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(
            QIcon(ICON_FILE)
            if os.path.exists(ICON_FILE)
            else self.style().standardIcon(QStyle.StandardPixmap.SP_ComputerIcon)
        )
        self.tray_icon.setToolTip("剪贴板助手")

        menu = QMenu()
        show_act = QAction("显示/隐藏", self)
        show_act.triggered.connect(self.toggle_window)
        quit_act = QAction("退出", self)
        quit_act.triggered.connect(self.quit_app)
        menu.addAction(show_act)
        menu.addSeparator()
        menu.addAction(quit_act)
        self.tray_icon.setContextMenu(menu)
        self.tray_icon.show()
        self.tray_icon.activated.connect(
            lambda r: (
                self.toggle_window()
                if r == QSystemTrayIcon.ActivationReason.Trigger
                else None
            )
        )

    def toggle_window(self):
        if self.isVisible():
            self.hide()
            self.detail_dialog.hide()
            if self.clipboard_preview_dialog:
                self.clipboard_preview_dialog.hide()
            if self.add_dialog:
                self.add_dialog.hide()
        else:
            screen = QApplication.primaryScreen().geometry()
            self.move(
                screen.width() - self.width() - 20,
                screen.height() - self.height() - 100,
            )
            self.show()
            self.activateWindow()
            self._sync_clipboard_preview_visibility()

    def showEvent(self, event):
        super().showEvent(event)
        QTimer.singleShot(0, self._sync_clipboard_preview_visibility)
        QTimer.singleShot(0, self._flush_pending_cache_refresh)

    def hideEvent(self, event):
        try:
            if self.clipboard_preview_dialog:
                self.clipboard_preview_dialog.hide()
        except Exception:
            pass
        super().hideEvent(event)

    def _is_backgrounded_window(self) -> bool:
        try:
            if not self.isVisible() or self.isMinimized():
                return True
            return bool(self.windowState() & Qt.WindowState.WindowMinimized)
        except Exception:
            return True

    def quit_app(self):
        log_info("================ 应用程序退出 ================")
        self._closing = True
        try:
            remote_timer = getattr(self, "remote_update_timer", None)
            if remote_timer and remote_timer.isActive():
                remote_timer.stop()
        except Exception:
            pass
        try:
            speech_manager.shutdown()
        except Exception:
            pass
        self._stop_event_relay_bridge()
        if self.clipboard_preview_dialog:
            self.clipboard_preview_dialog.hide()
        self._shutdown_clipboard_ipc(wait_ms=1500)
        self.save_active_cache()
        QApplication.instance().quit()

    def closeEvent(self, event):
        try:
            self._closing = True
            try:
                remote_timer = getattr(self, "remote_update_timer", None)
                if remote_timer and remote_timer.isActive():
                    remote_timer.stop()
            except Exception:
                pass
            try:
                speech_manager.shutdown()
            except Exception:
                pass
            try:
                if self.clipboard_preview_dialog:
                    self.clipboard_preview_dialog.hide()
                self._shutdown_clipboard_ipc(wait_ms=1500)
                log_info("剪贴板监听进程已停止")
            except Exception as e:
                log_error(f"停止剪贴板监听进程失败: {e}")

            # 停止热重载管理器
            if hasattr(self, "hot_reload_manager"):
                self.hot_reload_manager.stop()
            self._stop_event_relay_bridge()
        except Exception as exc:
            log_error(f"关闭窗口清理失败: {exc}")
        super().closeEvent(event)

    def mousePressEvent(self, e):
        if e.button() == Qt.MouseButton.LeftButton:
            self.drag_position = (
                e.globalPosition().toPoint() - self.frameGeometry().topLeft()
            )
            e.accept()

    def mouseMoveEvent(self, e):
        if e.buttons() == Qt.MouseButton.LeftButton and self.drag_position:
            self.move(e.globalPosition().toPoint() - self.drag_position)
            e.accept()

    def show_message(self, text):
        text = str(text or "").strip()
        if not text:
            return
        now = time.monotonic()
        last = getattr(self, "_last_nonblocking_message", None) or {}
        if last.get("text") == text and now - float(last.get("ts") or 0.0) < 2.0:
            return
        self._last_nonblocking_message = {"text": text, "ts": now}
        if self._is_backgrounded_window():
            return

        msg = show_toast_message(
            self,
            text,
            duration_ms=2400,
            theme=getattr(self, "current_theme", "dark"),
        )
        msg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        self._active_messages.append(msg)

        def _cleanup(*_args):
            try:
                self._active_messages.remove(msg)
            except ValueError:
                pass

        msg.finished.connect(_cleanup)
        msg.destroyed.connect(_cleanup)

    def _prompt_i2_robot_group_choice(self, notice_type: str):
        msg = QMessageBox(self)
        msg.setWindowFlags(Qt.WindowType.Popup | Qt.WindowType.FramelessWindowHint)
        msg.setWindowTitle("")
        msg.setIcon(QMessageBox.Icon.Question)
        msg.setText(
            f"{notice_type or '该通告'}默认将发送到 I2 群。\n"
            "这次只覆盖群消息去向，不会修改多维里的等级。"
        )
        msg.setInformativeText("请选择本次群消息的发送目标。")
        msg.setStyleSheet(get_stylesheet(self.current_theme))
        btn_i2 = msg.addButton("发送I2群", QMessageBox.ButtonRole.AcceptRole)
        btn_i3 = msg.addButton("发送I3群", QMessageBox.ButtonRole.ActionRole)
        btn_skip = msg.addButton(
            "不发送群消息", QMessageBox.ButtonRole.DestructiveRole
        )
        btn_back = msg.addButton("关闭", QMessageBox.ButtonRole.RejectRole)
        msg.setEscapeButton(btn_back)
        msg.exec()
        clicked = msg.clickedButton()
        if clicked == btn_i2:
            return "i2"
        if clicked == btn_i3:
            return "i3"
        if clicked == btn_skip:
            return "skip"
        if clicked == btn_back:
            return None
        return None


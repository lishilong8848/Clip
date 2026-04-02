import re

import io

import os

import threading

import unicodedata

from PyQt6.QtWidgets import (

    QDialog,

    QVBoxLayout,

    QFrame,

    QHBoxLayout,

    QLabel,

    QPushButton,

    QTextEdit,

    QLineEdit,

    QWidget,

    QMenu,

    QComboBox,

    QFileDialog,

    QCalendarWidget,

    QSpinBox,

    QCheckBox,
    QMessageBox,
    QSizeGrip,
)
from PyQt6.QtCore import (

    Qt,

    pyqtSignal,

    pyqtSlot,

    QTimer,

    QEvent,

    QBuffer,

    QIODevice,

    QDateTime,

    QDate,

    QPoint,

    QMetaObject,

)

from PyQt6.QtGui import (

    QPixmap,

    QCursor,

    QAction,

    QGuiApplication,

    QKeySequence,

    QImage,

    QColor,

    QTextCharFormat,

    QFont,

)

from PyQt6 import sip

from PIL import ImageGrab, Image, ImageChops, ImageStat, ImageEnhance

import numpy as np

# RapidOCR 延迟导入，避免与 PyQt6 的 DLL 加载冲突



from .styles import get_stylesheet

from .common import show_simple_message

from ..building_normalizer import (

    normalize_building_name,

    normalize_buildings_value as normalize_buildings_list,

)

from ..config import (

    STATUS_VALUES,

    STATUS_START,

    STATUS_NEW,

    STATUS_UPDATE,

    STATUS_END,

    BUILDING_OPTIONS,

    BUILDING_PLACEHOLDER,

    BUILDING_A,

    BUILDING_B,

    BUILDING_C,

    BUILDING_D,

    BUILDING_E,

    BUILDING_110,

    BUILDING_DETECT_110_KEYWORDS,

    BUILDING_DETECT_ALIASES,

    EVENT_LEVEL_OPTIONS,

    EVENT_LEVEL_UPGRADE_I3_TO_I2,

    EVENT_LEVEL_UPGRADE_I3_TO_I1,

    EVENT_LEVEL_PLACEHOLDER,

    EVENT_SOURCE_OPTIONS,

    EVENT_SOURCE_PLACEHOLDER,

    EVENT_SOURCE_BA,

    EVENT_SOURCE_BMS,

    EVENT_SOURCE_PPM,

    EVENT_SOURCE_FIRE,

    EVENT_SOURCE_CHANGE,

    EVENT_SOURCE_PATROL,

    EVENT_SOURCE_CUSTOMER,

    EVENT_SOURCE_CCTV,

    EVENT_SOURCE_ACCESS,

    EVENT_SOURCE_DINGPING,

    OPTION_SLASH,

    CHANGE_ZHIHANG_LEVEL_OPTIONS,

    ALI_LEVEL_ULTRA_LOW,

    ALI_LEVEL_LOW,

    ALI_LEVEL_MEDIUM,

    ALI_LEVEL_HIGH,

    SPECIALTY_OPTIONS,

    SPECIALTY_PLACEHOLDER,

    SPECIALTY_ELECTRIC,

    SPECIALTY_HVAC,

    SPECIALTY_FIRE,

    SPECIALTY_WEAK,

    SPECIALTY_OTHER,

    OPTION_SLASH,

    LEVEL_I3,

    LEVEL_I2,

    LEVEL_I1,

    LEVEL_E0,

)





class AddDialog(QDialog):
    query_finished = pyqtSignal(bool, str, int)



    def __init__(self, parent=None, theme="dark"):

        super().__init__(parent)

        self.theme = theme

        self.setObjectName("AddWindow")

        self.setWindowFlags(

            Qt.WindowType.FramelessWindowHint

            | Qt.WindowType.WindowStaysOnTopHint

            | Qt.WindowType.Tool

        )

        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

        self.resize(450, 400)

        self.result_text = None

        self.result_record_id = ""

        self.result_status = ""

        self.record_validator = None

        self.existing_checker = None

        self._query_running = False

        self._query_cancelled = False

        self._query_token = 0

        self._pending_content = ""

        self._pending_status = ""

        self._pending_record_id = ""

        self.query_finished.connect(

            self._on_query_finished, Qt.ConnectionType.QueuedConnection

        )



        layout = QVBoxLayout(self)

        layout.setContentsMargins(5, 5, 5, 5)

        self.container = QFrame()

        self.container.setObjectName("AddWindow")

        inner_layout = QVBoxLayout(self.container)



        # 顶部栏

        top_bar = QHBoxLayout()

        title = QLabel("手动添加内容")

        title.setObjectName("TitleLabel")

        self.close_btn = QPushButton("关闭")

        self.close_btn.setFixedSize(40, 40)

        self.close_btn.setStyleSheet("border:none; font-size:20px; color:#999;")

        self.close_btn.clicked.connect(self.reject)

        top_bar.addWidget(title)

        top_bar.addStretch()

        top_bar.addWidget(self.close_btn)



        # 提示标签

        self.hint_label = QLabel("请确保格式正确")

        self.hint_label.setStyleSheet("color: #888; font-size: 11px;")

        self.hint_label.setWordWrap(True)



        # 记录ID输入（用于更新通告）

        id_row = QHBoxLayout()

        id_label = QLabel("记录ID(更新必填):")

        id_label.setStyleSheet("color: #9CA3AF; font-size: 12px;")

        self.id_input = QLineEdit()

        self.id_input.setPlaceholderText("更新状态需填写 record_id")

        self.id_input.setMaximumWidth(240)

        self.id_input.setStyleSheet("""

            QLineEdit {

                background-color: #2A2A3C;

                border: 1px solid #4F4F7A;

                border-radius: 4px;

                padding: 4px 8px;

                color: #E5E7EB;

                font-size: 12px;

            }

            QLineEdit:focus {

                border-color: #8B5CF6;

            }

        """)

        id_row.addWidget(id_label)

        id_row.addWidget(self.id_input)

        id_row.addStretch()



        # 文本输入区

        self.text_input = QTextEdit()

        self.text_input.setPlaceholderText("在此处粘贴符合格式的内容...")



        # 按钮区

        btn_layout = QHBoxLayout()



        self.btn_template = QPushButton("插入模板")

        self.btn_template.setObjectName("TemplateBtn")

        self.btn_template.setCursor(Qt.CursorShape.PointingHandCursor)

        self.btn_template.clicked.connect(self.insert_template)



        self.btn_confirm = QPushButton("确认添加")

        self.btn_confirm.setObjectName("ConfirmBtn")

        self.btn_confirm.setCursor(Qt.CursorShape.PointingHandCursor)

        self.btn_confirm.clicked.connect(self.on_confirm)



        self.btn_cancel = QPushButton("取消")

        self.btn_cancel.setObjectName("DiffCancelBtn")

        self.btn_cancel.setCursor(Qt.CursorShape.PointingHandCursor)

        self.btn_cancel.clicked.connect(self.reject)



        btn_layout.addWidget(self.btn_template)

        btn_layout.addStretch()

        btn_layout.addWidget(self.btn_cancel)

        btn_layout.addWidget(self.btn_confirm)



        inner_layout.addLayout(top_bar)

        inner_layout.addWidget(self.hint_label)

        inner_layout.addLayout(id_row)

        inner_layout.addWidget(self.text_input)

        inner_layout.addLayout(btn_layout)

        layout.addWidget(self.container)

        self.setStyleSheet(get_stylesheet(self.theme))



        self.drag_position = None

        self._apply_input_theme()



    def reset_state(self):

        """重置窗口状态：清空文本"""

        self.text_input.clear()

        self.text_input.setPlaceholderText("在此处粘贴符合格式的内容...")

        self.id_input.clear()

        self.result_text = None

        self.result_record_id = ""

        self.result_status = ""

        self._query_running = False

        self._query_cancelled = False

        self._query_token = 0

        self._pending_content = ""

        self._pending_status = ""

        self._pending_record_id = ""

        self._set_query_state(False)

        self._apply_input_theme()



    def set_record_validator(self, validator):

        self.record_validator = validator



    def set_existing_checker(self, checker):

        self.existing_checker = checker



    def apply_theme(self, theme: str):

        self.theme = theme

        self.setStyleSheet(get_stylesheet(self.theme))

        self._apply_input_theme()



    def _apply_input_theme(self):

        if self.theme == "light":

            bg = "#FAFAFA"

            border = "#E0E0E0"

            text_color = "#212529"

            focus_border = "#2196F3"

        else:

            bg = "#252535"

            border = "#3D3D5C"

            text_color = "#E4E4E7"

            focus_border = "#6366F1"



        self.text_input.setStyleSheet(

            f"""

            QTextEdit {{

                border: 1px solid {border};

                background-color: {bg};

                border-radius: 6px;

                padding: 8px;

                color: {text_color};

                selection-background-color: #3B82F6;

            }}

            QTextEdit:focus {{

                border-color: {focus_border};

            }}

            """

        )

        self.id_input.setStyleSheet(

            f"""

            QLineEdit {{

                background-color: {bg};

                border: 1px solid {border};

                border-radius: 4px;

                padding: 4px 8px;

                color: {text_color};

                font-size: 12px;

            }}

            QLineEdit:focus {{

                border-color: {focus_border};

            }}

            """

        )



    def _set_query_state(self, running: bool):

        if running:

            self.btn_confirm.setText("查询记录中...")

            self.btn_confirm.setEnabled(False)

            self.btn_template.setEnabled(False)

            self.id_input.setEnabled(False)

            self.text_input.setEnabled(False)

            self.hint_label.setText("正在查询记录，请稍候...")

        else:

            self.btn_confirm.setText("确认添加")

            self.btn_confirm.setEnabled(True)

            self.btn_template.setEnabled(True)

            self.id_input.setEnabled(True)

            self.text_input.setEnabled(True)

            self.hint_label.setText("请确保格式正确")



    def _start_record_query(self, record_id: str, content: str, status: str):

        if not self.record_validator:

            return

        self._query_running = True

        self._query_cancelled = False

        self._query_token += 1

        token = self._query_token

        self._pending_content = content

        self._pending_status = status

        self._pending_record_id = record_id

        self._set_query_state(True)



        def run():

            ok = False

            message = ""

            try:

                ok, message = self.record_validator(record_id, content)

            except Exception as exc:

                ok = False

                message = str(exc)

            self.query_finished.emit(ok, message, token)



        threading.Thread(target=run, daemon=True).start()



    def _on_query_finished(self, ok: bool, message: str, token: int):

        if self._query_cancelled or token != self._query_token:

            return

        self._query_running = False

        self._set_query_state(False)

        if ok:

            self.result_text = self._pending_content

            self.result_record_id = self._pending_record_id

            self.result_status = self._pending_status

            self.accept()

        else:

            show_simple_message(self, message or "未找到对应记录。", self.theme)



    def insert_template(self):

        """插入标准格式模板"""

        template = (

            "【事件通告】状态：新增\n"

            "【标题】EA118机房XXXX楼IXXX级事件通报\n"

            "【来源】XXXX系统\n"

            "【时间】2023-10-27 10:00:00\n"

            "【概述】XXXX报XXXX报警\n"

            "【影响】IT业务暂无影响XXXX\n"

            "【进展】1、值班工程师已前往现场查看,请知晓!\n"

        )

        self.text_input.setText(template)



    def on_confirm(self):

        content = self.text_input.toPlainText().strip()

        if not content:

            self.text_input.setPlaceholderText("内容不能为空！")

            return



        # --- 格式校验逻辑 ---

        # 1. 检查通告类型（支持具体的类型）

        valid_types = [

            "【事件通告】",

            "【设备变更】",

            "【设备调整】",

            "【维保通告】",

            "【上下电通告】",

            "【上电通告】",

            "【下电通告】",

            "【设备轮巡】",

            "【设备检修】",

        ]

        if not any(t in content for t in valid_types):

            show_simple_message(

                self,

                "内容必须包含通告类型，例如：【设备变更】、【维保通告】等。",

                self.theme,

            )

            return



        # 2. 检查标题/名称

        if "【标题】" not in content and "【名称】" not in content:

            show_simple_message(

                self, "内容必须包含【名称】或【标题】关键词。", self.theme

            )

            return



        # 3. 检查状态

        status_match = re.search(

            r"状态[：:]\s*(.*?)(?:\s*[\n【]|$)", content, re.DOTALL

        )

        if not status_match:

            show_simple_message(

                self,

                "无法提取【状态】字段。\n请检查是否包含 '状态：开始' 等字样。",

                self.theme,

            )

            return



        status = status_match.group(1).strip()

        # 支持 "开始" (对应新增), "新增", "更新", "结束"

        if status not in STATUS_VALUES:

            show_simple_message(

                self,

                (

                    f"识别到的状态为：'{status}'\n\n状态必须是以下之一：\n"

                    f"- {STATUS_START} (或 {STATUS_NEW})\n"

                    f"- {STATUS_UPDATE}\n"

                    f"- {STATUS_END}"

                ),

                self.theme,

            )

            return



        record_id = self.id_input.text().strip()

        if status == STATUS_UPDATE and not record_id:

            if self.existing_checker:

                exists = False

                try:

                    exists = bool(self.existing_checker(content))

                except Exception:

                    exists = False

                if not exists:

                    show_simple_message(

                        self, "未在列表中找到对应条目，请填写记录ID。", self.theme

                    )

                    return

            self.result_text = content

            self.result_record_id = ""

            self.result_status = status

            self.accept()

            return

        if status == STATUS_UPDATE:

            self._start_record_query(record_id, content, status)

            return



        # 4. 简单的结构检查 (名称/标题后要有内容)

        # 兼容 【名称】 或 【标题】

        key_pattern = r"(?<=【名称】)(.*?)(?=【)|(?<=【标题】)(.*?)(?=【)"

        # 如果是最后一行，可能没有后续的【

        if not re.search(key_pattern, content + "【", re.DOTALL):

            show_simple_message(

                self,

                "无法提取唯一标识（名称或标题）。\n请确保【名称】或【标题】后面紧跟其他标签（如【时间】）。",

                self.theme,

            )

            return



        self.result_text = content

        self.result_record_id = record_id

        self.result_status = status

        self.accept()



    def reject(self):

        self._query_cancelled = True

        if self._query_running:

            self._query_token += 1

            self._query_running = False

            self._set_query_state(False)

        super().reject()



    def mousePressEvent(self, event):

        if (

            getattr(self, "datetime_popup", None)

            and self.datetime_popup.isVisible()

            and event.button() == Qt.MouseButton.LeftButton

        ):

            pos = event.position().toPoint()

            parent_widget = getattr(self, "container", self)

            pos_in_parent = parent_widget.mapFrom(self, pos)

            time_pos = self.time_input.mapTo(parent_widget, QPoint(0, 0))

            time_rect = self.time_input.geometry()

            time_rect.moveTo(time_pos)

            if not self.datetime_popup.geometry().contains(

                pos_in_parent

            ) and not time_rect.contains(pos_in_parent):

                self.datetime_popup.hide()

        if event.button() == Qt.MouseButton.LeftButton:

            self.drag_position = (

                event.globalPosition().toPoint() - self.frameGeometry().topLeft()

            )

            event.accept()



    def mouseMoveEvent(self, event):
        if event.buttons() == Qt.MouseButton.LeftButton and self.drag_position:
            self.move(event.globalPosition().toPoint() - self.drag_position)
            event.accept()


class ClipboardPreviewDialog(QDialog):
    use_requested = pyqtSignal(str)
    closed_by_user = pyqtSignal()

    def __init__(self, parent=None, theme="dark"):
        super().__init__(parent)
        self.theme = theme
        self.snapshot_text = ""
        self.snapshot_time_text = ""
        self.drag_position = None
        self.setObjectName("ClipboardPreviewWindow")
        self.setWindowTitle("最近剪贴板")
        self.setWindowFlags(
            Qt.WindowType.Tool
            | Qt.WindowType.WindowStaysOnTopHint
            | Qt.WindowType.FramelessWindowHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.resize(420, 260)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        self.container = QFrame()
        self.container.setObjectName("ClipboardPreviewContainer")
        inner_layout = QVBoxLayout(self.container)
        inner_layout.setContentsMargins(12, 10, 12, 12)
        inner_layout.setSpacing(8)

        self.header_frame = QFrame()
        self.header_frame.setObjectName("ClipboardPreviewHeader")
        self.header_frame.setCursor(Qt.CursorShape.OpenHandCursor)
        header_layout = QHBoxLayout(self.header_frame)
        header_layout.setContentsMargins(0, 0, 0, 6)
        header_layout.setSpacing(8)
        self.title_label = QLabel("最近剪贴板")
        self.title_label.setObjectName("ClipboardPreviewTitle")
        self.title_label.setCursor(Qt.CursorShape.OpenHandCursor)
        self.time_label = QLabel("未记录")
        self.time_label.setObjectName("ClipboardPreviewMeta")
        self.time_label.setCursor(Qt.CursorShape.OpenHandCursor)
        header_layout.addWidget(self.title_label)
        header_layout.addStretch()
        header_layout.addWidget(self.time_label)

        self.text_preview = QTextEdit()
        self.text_preview.setObjectName("ClipboardPreviewText")
        self.text_preview.setReadOnly(True)
        self.text_preview.setPlaceholderText(
            "暂无剪贴板内容\n复制后的最后一条文本会显示在这里。"
        )

        button_layout = QHBoxLayout()
        button_layout.setContentsMargins(0, 2, 0, 0)
        button_layout.setSpacing(6)
        self.resize_hint_label = QLabel("拖拽缩放")
        self.resize_hint_label.setObjectName("ClipboardPreviewResizeHint")
        self.size_grip = QSizeGrip(self.container)
        self.size_grip.setObjectName("ClipboardPreviewSizeGrip")
        self.size_grip.setCursor(Qt.CursorShape.SizeFDiagCursor)
        self.close_btn = QPushButton("关闭")
        self.close_btn.setObjectName("ClipboardPreviewCloseBtn")
        self.close_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.close_btn.setMinimumHeight(32)
        self.close_btn.clicked.connect(self.close)

        self.use_btn = QPushButton("使用这条")
        self.use_btn.setObjectName("ClipboardPreviewUseBtn")
        self.use_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.use_btn.setMinimumHeight(32)
        self.use_btn.setMinimumWidth(104)
        self.use_btn.clicked.connect(self._on_use_clicked)

        button_layout.addStretch()
        button_layout.addWidget(self.close_btn)
        button_layout.addWidget(self.use_btn)
        button_layout.addWidget(self.resize_hint_label)
        button_layout.addWidget(self.size_grip)

        inner_layout.addWidget(self.header_frame)
        inner_layout.addWidget(self.text_preview, 1)
        inner_layout.addLayout(button_layout)
        layout.addWidget(self.container)

        self.apply_theme(self.theme)
        self.set_content("", "")

    def _build_preview_stylesheet(self) -> str:
        if self.theme == "light":
            panel_bg = "#FFFFFF"
            panel_border = "#D8DEE9"
            text_bg = "#F8FAFC"
            text_color = "#0F172A"
            muted = "#64748B"
            accent = "#2563EB"
            accent_hover = "#1D4ED8"
            accent_pressed = "#1E40AF"
            accent_text = "#FFFFFF"
            secondary_bg = "#E5E7EB"
            secondary_hover = "#CBD5E1"
            secondary_pressed = "#94A3B8"
            secondary_text = "#1F2937"
            selection_bg = "#BFDBFE"
        else:
            panel_bg = "#252535"
            panel_border = "#3D3D5C"
            text_bg = "#1F2232"
            text_color = "#E5E7EB"
            muted = "#9CA3AF"
            accent = "#22C55E"
            accent_hover = "#16A34A"
            accent_pressed = "#15803D"
            accent_text = "#FFFFFF"
            secondary_bg = "#363650"
            secondary_hover = "#44446A"
            secondary_pressed = "#2A2A3C"
            secondary_text = "#E5E7EB"
            selection_bg = "#3B82F6"

        return f"""
QFrame#ClipboardPreviewContainer {{
    background-color: {panel_bg};
    border: 1px solid {panel_border};
    border-radius: 10px;
}}
QFrame#ClipboardPreviewHeader {{
    background-color: transparent;
    border: none;
    border-bottom: 1px solid {panel_border};
}}
QLabel#ClipboardPreviewTitle {{
    font-size: 14px;
    font-weight: bold;
    color: {text_color};
}}
QLabel#ClipboardPreviewMeta, QLabel#ClipboardPreviewResizeHint {{
    color: {muted};
    font-size: 12px;
}}
QTextEdit#ClipboardPreviewText {{
    border: 1px solid {panel_border};
    background-color: {text_bg};
    border-radius: 8px;
    padding: 8px;
    color: {text_color};
    selection-background-color: {selection_bg};
}}
QPushButton#ClipboardPreviewUseBtn {{
    background-color: {accent};
    color: {accent_text};
    border: none;
    border-radius: 9px;
    padding: 6px 16px;
    font-weight: bold;
}}
QPushButton#ClipboardPreviewUseBtn:hover {{
    background-color: {accent_hover};
}}
QPushButton#ClipboardPreviewUseBtn:pressed {{
    background-color: {accent_pressed};
}}
QPushButton#ClipboardPreviewUseBtn:disabled {{
    background-color: {secondary_bg};
    color: {muted};
}}
QPushButton#ClipboardPreviewCloseBtn {{
    background-color: transparent;
    color: {muted};
    border: none;
    border-radius: 8px;
    padding: 4px 8px;
    font-weight: 500;
}}
QPushButton#ClipboardPreviewCloseBtn:hover {{
    background-color: {secondary_bg};
    color: {secondary_text};
}}
QPushButton#ClipboardPreviewCloseBtn:pressed {{
    background-color: {secondary_pressed};
}}
QPushButton#ClipboardPreviewCloseBtn:focus, QPushButton#ClipboardPreviewUseBtn:focus {{
    outline: none;
}}
QSizeGrip#ClipboardPreviewSizeGrip {{
    width: 22px;
    height: 22px;
    background-color: {secondary_bg};
    border: 1px solid {panel_border};
    border-radius: 6px;
    margin: 0 0 1px 0;
}}
"""

    def apply_theme(self, theme: str):
        self.theme = theme
        self.setStyleSheet(get_stylesheet(theme) + self._build_preview_stylesheet())

    def set_content(self, text: str, time_text: str = ""):
        self.snapshot_text = str(text or "")
        self.snapshot_time_text = str(time_text or "").strip()
        has_text = bool(self.snapshot_text.strip())
        if has_text:
            self.text_preview.setPlainText(self.snapshot_text)
            self.time_label.setText(f"更新 {self.snapshot_time_text or '未记录'}")
        else:
            self.text_preview.clear()
            self.time_label.setText("未记录")
        self.use_btn.setEnabled(has_text)

    def _on_use_clicked(self):
        text = self.snapshot_text.strip()
        if text:
            self.use_requested.emit(text)

    def closeEvent(self, event):
        self.closed_by_user.emit()
        super().closeEvent(event)

    def mousePressEvent(self, event):
        if (
            event.button() == Qt.MouseButton.LeftButton
            and self.header_frame.geometry().contains(event.position().toPoint())
        ):
            self.drag_position = (
                event.globalPosition().toPoint() - self.frameGeometry().topLeft()
            )
            self.header_frame.setCursor(Qt.CursorShape.ClosedHandCursor)
            event.accept()
            return
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if event.buttons() == Qt.MouseButton.LeftButton and self.drag_position:
            self.move(event.globalPosition().toPoint() - self.drag_position)
            event.accept()
            return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        self.drag_position = None
        self.header_frame.setCursor(Qt.CursorShape.OpenHandCursor)
        super().mouseReleaseEvent(event)


class ScreenshotConfirmOverlay(QWidget):
    """悬浮确认层 - 让用户决定UI状态是否准备好



    设计理念:

    - 不全屏: 仅显示必要的确认按钮

    - 不阻断: 鼠标可以自由移动触发hover

    - 用户驱动: 点击√时才执行真实截图

    """



    confirmed = pyqtSignal()  # 用户确认UI状态已准备好

    cancelled = pyqtSignal()  # 用户取消



    def __init__(self, crop_region, screen_geometry):

        super().__init__()



        # 无边框、置顶、工具窗口

        self.setWindowFlags(

            Qt.WindowType.FramelessWindowHint

            | Qt.WindowType.WindowStaysOnTopHint

            | Qt.WindowType.Tool

        )

        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)



        # 计算按钮位置（选区右下角偏移）

        x1, y1, x2, y2 = crop_region

        btn_x = x2 + 10

        btn_y = y2 + 10



        # 边界检测：确保不超出屏幕

        screen_w = screen_geometry.width()

        screen_h = screen_geometry.height()



        if btn_x + 150 > screen_w:

            btn_x = x2 - 160  # 放在选区左侧

        if btn_y + 60 > screen_h:

            btn_y = y2 - 70  # 放在选区上方



        self.setGeometry(btn_x, btn_y, 150, 60)



        self._init_ui()



    def _init_ui(self):

        """初始化UI布局"""

        main_layout = QVBoxLayout(self)

        main_layout.setContentsMargins(8, 8, 8, 8)

        main_layout.setSpacing(4)



        # 提示标签（显示ROI变化状态）

        self.hint_label = QLabel("等待UI变化...")

        self.hint_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.hint_label.setStyleSheet("""

            QLabel {

                color: rgba(255, 255, 255, 0.8);

                background: rgba(0, 0, 0, 0.5);

                border-radius: 4px;

                padding: 4px 8px;

                font-size: 12px;

            }

        """)



        # 按钮布局

        btn_layout = QHBoxLayout()

        btn_layout.setContentsMargins(0, 0, 0, 0)

        btn_layout.setSpacing(6)



        # √ 确认按钮

        btn_confirm = QPushButton("✓ 确认")

        btn_confirm.setFixedSize(80, 44)

        btn_confirm.setCursor(Qt.CursorShape.PointingHandCursor)

        btn_confirm.setStyleSheet("""

            QPushButton {

                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, 

                                            stop:0 rgba(34, 197, 94, 0.95), 

                                            stop:1 rgba(22, 163, 74, 0.95));

                color: white;

                border: 2px solid rgba(255, 255, 255, 0.3);

                border-radius: 8px;

                font-size: 16px;

                font-weight: bold;

            }

            QPushButton:hover {

                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, 

                                            stop:0 rgba(74, 222, 128, 1.0), 

                                            stop:1 rgba(34, 197, 94, 1.0));

                border-color: rgba(255, 255, 255, 0.5);

            }

            QPushButton:pressed {

                background: rgba(22, 163, 74, 1.0);

            }

        """)

        btn_confirm.clicked.connect(self.confirmed.emit)



        # × 取消按钮

        btn_cancel = QPushButton("✕")

        btn_cancel.setFixedSize(44, 44)

        btn_cancel.setCursor(Qt.CursorShape.PointingHandCursor)

        btn_cancel.setStyleSheet("""

            QPushButton {

                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, 

                                            stop:0 rgba(239, 68, 68, 0.95), 

                                            stop:1 rgba(220, 38, 38, 0.95));

                color: white;

                border: 2px solid rgba(255, 255, 255, 0.3);

                border-radius: 8px;

                font-size: 20px;

                font-weight: bold;

            }

            QPushButton:hover {

                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, 

                                            stop:0 rgba(248, 113, 113, 1.0), 

                                            stop:1 rgba(239, 68, 68, 1.0));

                border-color: rgba(255, 255, 255, 0.5);

            }

        """)

        btn_cancel.clicked.connect(self.cancelled.emit)



        btn_layout.addWidget(btn_confirm)

        btn_layout.addWidget(btn_cancel)



        main_layout.addWidget(self.hint_label)

        main_layout.addLayout(btn_layout)



    def update_hint(self, text, is_change=False):

        """更新提示文本"""

        self.hint_label.setText(text)

        if is_change:

            # 变化时使用绿色高亮

            self.hint_label.setStyleSheet("""

                QLabel {

                    color: white;

                    background: rgba(34, 197, 94, 0.9);

                    border-radius: 4px;

                    padding: 4px 8px;

                    font-size: 12px;

                    font-weight: bold;

                }

            """)

        else:

            # 默认样式

            self.hint_label.setStyleSheet("""

                QLabel {

                    color: rgba(255, 255, 255, 0.8);

                    background: rgba(0, 0, 0, 0.5);

                    border-radius: 4px;

                    padding: 4px 8px;

                    font-size: 12px;

                }

            """)



    def keyPressEvent(self, event):

        """快捷键支持"""

        if event.key() == Qt.Key.Key_Return or event.key() == Qt.Key.Key_Enter:

            self.confirmed.emit()

        elif event.key() == Qt.Key.Key_Escape:

            self.cancelled.emit()





class ScreenshotRegionSelector(QWidget):

    """全屏透明窗口用于选择截图区域 - 确认态截图版本



    设计思路:

    1. 框选阶段: 仅显示半透明遮罩供用户选区, 不截图

    2. 确认阶段: 显示确认层, 用户可自由hover, 确认后才执行真实截图

    3. 截图阶段: 用户点击√时, 执行真实截图并裁剪

    """



    screenshot_captured = pyqtSignal(object)  # 发送 PIL Image 对象

    selection_cancelled = pyqtSignal()



    def __init__(self):

        super().__init__()

        self.setWindowFlags(

            Qt.WindowType.FramelessWindowHint

            | Qt.WindowType.WindowStaysOnTopHint

            | Qt.WindowType.Tool

        )

        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)



        # 获取所有屏幕的组合几何形状

        from PyQt6.QtWidgets import QApplication



        total_rect = QApplication.primaryScreen().geometry()

        for screen in QApplication.screens():

            total_rect = total_rect.united(screen.geometry())



        self.setGeometry(total_rect)

        self.setCursor(Qt.CursorShape.CrossCursor)



        # 保存窗口几何信息用于后续坐标转换

        self._widget_geometry = total_rect



        # 不再预先截图! 截图将在框选完成后延迟执行

        self.full_screenshot = None



        # 待裁剪区域 (逻辑坐标)

        self._pending_crop = None



        # 确认层引用

        self.confirm_overlay = None



        # ===== ROI 动态感知新增成员变量 =====

        self._roi_watch_timer = None  # ROI变化检测定时器

        self._last_roi_gray = None  # 上一次灰度ROI（用于对比）

        self.latest_roi_image = None  # 最新有效候选截图

        self._roi_diff_threshold = 0.01  # ROI差异阈值（可配置）



        # 确保显示

        self.show()



        self.start_pos = None

        self.end_pos = None

        self.is_selecting = False

        self._selection_locked = False  # 锁定标志：首次框选后禁止再次框选



    def paintEvent(self, event):

        from PyQt6.QtGui import QPainter, QColor, QPen



        painter = QPainter(self)



        # 1. 绘制半透明黑色遮罩

        painter.fillRect(self.rect(), QColor(0, 0, 0, 100))



        # 2. 绘制选区（清除遮罩，变成透明，透出下面真实桌面）

        if self.start_pos and self.end_pos:

            x1 = min(self.start_pos.x(), self.end_pos.x())

            y1 = min(self.start_pos.y(), self.end_pos.y())

            w = abs(self.end_pos.x() - self.start_pos.x())

            h = abs(self.end_pos.y() - self.start_pos.y())



            painter.setCompositionMode(QPainter.CompositionMode.CompositionMode_Clear)

            painter.fillRect(x1, y1, w, h, QColor(0, 0, 0, 0))



            painter.setCompositionMode(

                QPainter.CompositionMode.CompositionMode_SourceOver

            )



            # 3. 绘制红框 border

            pen = QPen(QColor(239, 68, 68), 2)

            painter.setPen(pen)

            painter.drawRect(x1, y1, w, h)



    def mousePressEvent(self, event):

        if event.button() == Qt.MouseButton.LeftButton:

            if self._selection_locked:

                # 已锁定状态，点击灰色区域取消截图

                self._on_user_cancelled()

                return

            self.start_pos = event.pos()

            self.end_pos = event.pos()

            self.is_selecting = True

            self.update()



    def mouseMoveEvent(self, event):

        if self.is_selecting and not self._selection_locked:

            self.end_pos = event.pos()

            self.update()



    def mouseReleaseEvent(self, event):

        if event.button() == Qt.MouseButton.LeftButton and self.is_selecting:

            self.is_selecting = False

            self.end_pos = event.pos()



            # 计算选区（逻辑坐标，对应 widget 坐标）

            x1 = min(self.start_pos.x(), self.end_pos.x())

            y1 = min(self.start_pos.y(), self.end_pos.y())

            x2 = max(self.start_pos.x(), self.end_pos.x())

            y2 = max(self.start_pos.y(), self.end_pos.y())

            w = x2 - x1

            h = y2 - y1



            if w > 10 and h > 10:

                # 1. 保存选区坐标 (逻辑坐标)

                self._pending_crop = (x1, y1, x2, y2)



                # 锁定框选：首次框选后禁止再次框选

                self._selection_locked = True



                # 恢复鼠标样式为箭头

                self.setCursor(Qt.CursorShape.ArrowCursor)



                # 2. 立即截取初始候选截图（确保有可用的截图）

                self._capture_initial_roi()



                # 3. 不隐藏遮罩！保持遮罩存在，但用户可以hover



                # 4. 显示确认层（用户驱动截图）

                QTimer.singleShot(100, self._show_confirmation_overlay)

            else:

                self.close()

                self.selection_cancelled.emit()



    def _show_confirmation_overlay(self):

        """显示确认层，等待用户确认UI状态"""

        # 启动ROI监听循环

        self._start_roi_watching()



        # 显示确认按钮

        self.confirm_overlay = ScreenshotConfirmOverlay(

            crop_region=self._pending_crop, screen_geometry=self._widget_geometry

        )

        self.confirm_overlay.confirmed.connect(self._on_user_confirmed)

        self.confirm_overlay.cancelled.connect(self._on_user_cancelled)

        self.confirm_overlay.show()

        self.confirm_overlay.activateWindow()



    def _capture_initial_roi(self):

        """框选完成后立即截取初始候选截图"""

        try:

            kwargs = {}

            if (

                hasattr(ImageGrab, "grab")

                and "all_screens" in ImageGrab.grab.__code__.co_varnames

            ):

                kwargs["all_screens"] = True

            full_screenshot = ImageGrab.grab(**kwargs)



            x1, y1, x2, y2 = self._pending_crop

            img_w, img_h = full_screenshot.size

            wid_w, wid_h = self._widget_geometry.width(), self._widget_geometry.height()



            scale_x = img_w / wid_w

            scale_y = img_h / wid_h



            crop_box = (

                int(x1 * scale_x),

                int(y1 * scale_y),

                int(x2 * scale_x),

                int(y2 * scale_y),

            )



            # 设置初始候选截图

            self.latest_roi_image = full_screenshot.crop(crop_box)



            # 同时初始化灰度对比基准

            roi_top = self.latest_roi_image.crop(

                (0, 0, self.latest_roi_image.width, self.latest_roi_image.height // 2)

            )

            self._last_roi_gray = roi_top.convert("L").resize(

                (64, 64), Image.Resampling.LANCZOS

            )

        except Exception:

            pass



    def _on_user_confirmed(self):

        """用户点击√确认 - 使用最新的ROI候选截图（自动捕获hover变化）"""

        # 停止ROI监听

        if self._roi_watch_timer:

            self._roi_watch_timer.stop()



        if self.confirm_overlay:

            self.confirm_overlay.close()

            self.confirm_overlay = None



        # 直接使用最新的ROI候选截图（已自动捕获hover变化）

        if self.latest_roi_image:

            self.close()

            self.screenshot_captured.emit(self.latest_roi_image)

        else:

            # 如果没有候选截图（极端情况），降级为立即截图

            try:

                kwargs = {}

                if (

                    hasattr(ImageGrab, "grab")

                    and "all_screens" in ImageGrab.grab.__code__.co_varnames

                ):

                    kwargs["all_screens"] = True

                full_screenshot = ImageGrab.grab(**kwargs)



                x1, y1, x2, y2 = self._pending_crop

                img_w, img_h = full_screenshot.size

                wid_w, wid_h = (

                    self._widget_geometry.width(),

                    self._widget_geometry.height(),

                )



                scale_x = img_w / wid_w

                scale_y = img_h / wid_h



                crop_box = (

                    int(x1 * scale_x),

                    int(y1 * scale_y),

                    int(x2 * scale_x),

                    int(y2 * scale_y),

                )



                cropped = full_screenshot.crop(crop_box)

                self.close()

                self.screenshot_captured.emit(cropped)



            except Exception:

                self.close()

                self.selection_cancelled.emit()



    def _on_user_cancelled(self):

        """用户点击×取消截图"""

        if self.confirm_overlay:

            self.confirm_overlay.close()

            self.confirm_overlay = None



        # 停止ROI监听

        if self._roi_watch_timer:

            self._roi_watch_timer.stop()



        self.close()

        self.selection_cancelled.emit()



    # ===== ROI 动态感知新增方法 =====



    def _start_roi_watching(self):

        """启动ROI变化监听循环（每200ms检测一次）"""

        if not self._roi_watch_timer:

            self._roi_watch_timer = QTimer(self)

            self._roi_watch_timer.timeout.connect(self._check_roi_changes)



        # 启动定时器，200ms检测一次ROI变化

        self._roi_watch_timer.start(200)



    def _check_roi_changes(self):

        """检测ROI区域像素变化，自动更新候选截图"""

        try:

            # 1. 截取当前屏幕全屏（与初始截图策略保持一致，兼容多屏）

            kwargs = {}

            if (

                hasattr(ImageGrab, "grab")

                and "all_screens" in ImageGrab.grab.__code__.co_varnames

            ):

                kwargs["all_screens"] = True

            full_image = ImageGrab.grab(**kwargs)



            # 3. 裁剪ROI区域（DPI坐标转换）

            x1, y1, x2, y2 = self._pending_crop

            img_w, img_h = full_image.size

            wid_w, wid_h = self._widget_geometry.width(), self._widget_geometry.height()



            scale_x = img_w / wid_w

            scale_y = img_h / wid_h



            crop_box = (

                int(x1 * scale_x),

                int(y1 * scale_y),

                int(x2 * scale_x),

                int(y2 * scale_y),

            )

            roi_image = full_image.crop(crop_box)



            # 4. 转换为灰度+缩小（加速对比，避免大图计算开销）

            # 修改：仅检测上半区域的变化（避免下半部光标闪烁等干扰）

            rw, rh = roi_image.size

            roi_top_half = roi_image.crop((0, 0, rw, int(rh / 2)))

            roi_gray = roi_top_half.convert("L").resize(

                (64, 64), Image.Resampling.LANCZOS

            )



            # 5. 与上次ROI对比

            if self._last_roi_gray is not None:

                # 计算像素差异

                diff = ImageChops.difference(roi_gray, self._last_roi_gray)

                stat = ImageStat.Stat(diff)

                mean_diff = stat.mean[0]



                # 6. 超过阈值判定为有效变化（hover弹层、时间戳出现等）

                if mean_diff > self._roi_diff_threshold:

                    # 更新最新候选截图

                    self.latest_roi_image = roi_image



                    # 第一次检测到变化后停止ROI监听

                    if self._roi_watch_timer:

                        self._roi_watch_timer.stop()



                    # 通知用户检测到变化并已更新候选截图

                    if self.confirm_overlay:

                        self.confirm_overlay.update_hint(

                            "✅ 已捕获UI变化！", is_change=True

                        )

            else:

                # 首次检测，初始化

                self.latest_roi_image = roi_image

                self._last_roi_gray = roi_gray



        except Exception:

            # ROI检测失败，静默处理，不影响主流程

            pass



    def keyPressEvent(self, event):

        if event.key() == Qt.Key.Key_Escape:

            self.close()

            self.selection_cancelled.emit()





class ScreenshotConfirmDialog(QDialog):

    upload_confirmed = pyqtSignal(

        object, object, str, str, list, list, str, str, str, str, bool

    )  # (data_dict, screenshot_bytes, action_type, response_time, buildings, extra_images, specialty, change_level, event_level, event_source, recover_selected)

    cancelled = pyqtSignal()

    state_changed = pyqtSignal()

    screenshot_started = pyqtSignal()  # 通知父窗口隐藏，避免Z轴冲突

    screenshot_finished = pyqtSignal()  # 通知父窗口截图完成，可以显示



    def __init__(self, parent=None, theme="dark"):

        super().__init__(parent)

        self.theme = theme

        self.setObjectName("ScreenshotWindow")

        self.setWindowFlags(

            Qt.WindowType.FramelessWindowHint

            | Qt.WindowType.WindowStaysOnTopHint

            | Qt.WindowType.Tool

        )

        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

        self.resize(450, 400)

        self.screenshot_bytes = None

        self.screenshot_image = None

        self.extra_images = []

        self.enable_extra_upload = False

        self.enable_specialty_select = False

        self.enable_change_level_select = False

        self.enable_building_multi_select = True

        self.require_building = True

        self.enable_event_level_select = False

        self.enable_event_source_select = False

        self.enable_recover_select = False

        self.selected_specialty = ""

        self.selected_change_level = ""

        self.selected_event_level = ""

        self.selected_event_source = ""

        self.recover_selected = False

        self.transfer_to_overhaul_selected = False

        self.notice_type = ""

        self.cache_store = None

        # 延迟初始化OCR引擎（避免与PyQt6的DLL加载冲突）

        self.ocr_engine = None

        self._ocr_initialized = False

        self._ocr_cancelled = False

        self._suppress_ocr_cancel_on_hide = False

        self._ocr_vote_candidates = []

        self._ocr_vote_summary = {}



        layout = QVBoxLayout(self)

        layout.setContentsMargins(5, 5, 5, 5)

        self.container = QFrame()

        self.container.setObjectName("ScreenshotWindow")

        inner_layout = QVBoxLayout(self.container)



        top_bar = QHBoxLayout()

        title = QLabel("截图上传")

        title.setObjectName("TitleLabel")

        self.close_btn = QPushButton("×")

        self.close_btn.setObjectName("CloseBtn")

        self.close_btn.clicked.connect(self.cancel_upload)

        top_bar.addWidget(title)

        top_bar.addStretch()

        top_bar.addWidget(self.close_btn)



        self.hint_label = QLabel("是否需要截图？点击「开始截图」后框选屏幕区域。")

        self.hint_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.hint_label.setWordWrap(True)

        self.hint_label.setStyleSheet("color: #9CA3AF; margin: 10px 0;")



        self.preview_label = QLabel("截图预览区域")

        self.preview_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.preview_label.setMinimumHeight(200)

        self.preview_label.setStyleSheet(

            "background-color: #2A2A3C; border: 1px dashed #4F4F7A; border-radius: 8px; color: #6B7280;"

        )



        self.extra_upload_container = QWidget()

        extra_layout = QHBoxLayout(self.extra_upload_container)

        extra_layout.setContentsMargins(0, 0, 0, 0)

        extra_layout.setSpacing(6)



        extra_label = QLabel("现场照片:")

        extra_label.setStyleSheet("color: #9CA3AF; font-size: 12px;")



        self.extra_images_label = QLabel("未添加")

        self.extra_images_label.setStyleSheet("color: #EF4444; font-size: 11px;")

        self.extra_images_label.setWordWrap(True)



        self.btn_extra_upload = QPushButton("现场图片截图")

        self.btn_extra_upload.setObjectName("TemplateBtn")

        self.btn_extra_upload.clicked.connect(self.select_extra_images)



        self.extra_hint_label = QLabel("支持多张，点击截图 / Ctrl+V 粘贴")

        self.extra_hint_label.setStyleSheet("color: #6B7280; font-size: 11px;")



        extra_layout.addWidget(extra_label)

        extra_layout.addWidget(self.extra_images_label)

        extra_layout.addStretch()

        extra_layout.addWidget(self.btn_extra_upload)

        extra_layout.addWidget(self.extra_hint_label)



        btn_layout = QHBoxLayout()

        self.btn_screenshot = QPushButton("📸 开始截图")

        self.btn_screenshot.setObjectName("TemplateBtn")

        self.btn_screenshot.clicked.connect(self.start_screenshot)



        self.btn_skip = QPushButton("跳过截图")

        self.btn_skip.setObjectName("DiffCancelBtn")

        self.btn_skip.clicked.connect(self.skip_screenshot)

        self.btn_skip.setEnabled(False)  # 初始禁用，需要包含楼栋和时间



        self.btn_confirm = QPushButton("确认上传")

        self.btn_confirm.setObjectName("ConfirmBtn")

        self.btn_confirm.clicked.connect(self.confirm_upload)

        self.btn_confirm.setEnabled(False)



        btn_layout.addWidget(self.btn_screenshot)

        btn_layout.addStretch()

        btn_layout.addWidget(self.btn_skip)

        btn_layout.addWidget(self.btn_confirm)



        # OCR状态和时间输入行

        time_row = QHBoxLayout()

        self.ocr_status_label = QLabel("")

        self.ocr_status_label.setStyleSheet("color: #F59E0B; font-size: 12px;")



        time_input_label = QLabel("响应时间:")

        time_input_label.setStyleSheet("color: #9CA3AF; font-size: 12px;")



        self.time_input = QLineEdit()

        self.time_input.setPlaceholderText("YYYY-MM-DD HH:MM 或 HH:MM")

        self.time_input.setMaximumWidth(150)

        self.time_input.setStyleSheet("""

            QLineEdit {

                background-color: #2A2A3C;

                border: 1px solid #4F4F7A;

                border-radius: 4px;

                padding: 4px 8px;

                color: #E5E7EB;

                font-size: 12px;

            }

            QLineEdit:focus {

                border-color: #8B5CF6;

            }

        """)

        self.time_input.textChanged.connect(self._on_time_input_changed)

        self.time_input.installEventFilter(self)



        time_row.addWidget(self.ocr_status_label)

        time_row.addStretch()

        time_row.addWidget(time_input_label)

        time_row.addWidget(self.time_input)



        # 楼栋下拉框（可隐藏）

        self.building_container = QWidget()

        building_row = QHBoxLayout(self.building_container)

        building_row.setContentsMargins(0, 0, 0, 0)

        self.building_label = QLabel("楼栋:")

        self.building_label.setStyleSheet("color: #F59E0B; font-size: 12px;")



        self.building_combo = QComboBox()

        self.building_combo.setMaximumWidth(120)

        self.building_combo.setStyleSheet("""

            QComboBox {

                background-color: #2A2A3C;

                border: 1px solid #4F4F7A;

                border-radius: 4px;

                padding: 4px 8px;

                color: #E5E7EB;

                font-size: 12px;

            }

            QComboBox:focus {

                border-color: #8B5CF6;

            }

            QComboBox::drop-down {

                border: none;

            }

            QComboBox QAbstractItemView {

                background-color: #2A2A3C;

                border: 1px solid #4F4F7A;

                selection-background-color: #3B82F6;

            }

        """)



        # 楼栋选项

        self.building_options = list(BUILDING_OPTIONS)

        self.selected_buildings = set()

        self.building_combo.addItem(BUILDING_PLACEHOLDER)

        for building in self.building_options:

            self.building_combo.addItem(building)



        # 已选楼栋显示

        self.selected_buildings_label = QLabel("未选择")

        self.selected_buildings_label.setStyleSheet("color: #EF4444; font-size: 11px;")

        self.selected_buildings_label.setWordWrap(True)



        # 连接选择事件

        self.building_combo.activated.connect(self._on_building_selected)



        building_row.addWidget(self.building_label)

        building_row.addWidget(self.building_combo)

        building_row.addWidget(self.selected_buildings_label)

        building_row.addStretch()



        # 事件等级单选（事件通告专用）

        self.event_level_options = list(EVENT_LEVEL_OPTIONS)

        self.event_level_container = QWidget()

        event_level_layout = QHBoxLayout(self.event_level_container)

        event_level_layout.setContentsMargins(0, 0, 0, 0)

        event_level_layout.setSpacing(6)



        event_level_label = QLabel("事件等级:")

        event_level_label.setStyleSheet("color: #F59E0B; font-size: 12px;")



        self.event_level_combo = QComboBox()

        self.event_level_combo.setMaximumWidth(180)

        self.event_level_combo.setStyleSheet("""

            QComboBox {

                background-color: #2A2A3C;

                border: 1px solid #4F4F7A;

                border-radius: 4px;

                padding: 4px 8px;

                color: #E5E7EB;

                font-size: 12px;

            }

            QComboBox:focus {

                border-color: #8B5CF6;

            }

            QComboBox::drop-down {

                border: none;

            }

            QComboBox QAbstractItemView {

                background-color: #2A2A3C;

                border: 1px solid #4F4F7A;

                selection-background-color: #3B82F6;

            }

        """)

        self.event_level_combo.addItem(EVENT_LEVEL_PLACEHOLDER)

        for option in self.event_level_options:

            self.event_level_combo.addItem(option)

        self.event_level_combo.activated.connect(self._on_event_level_selected)



        self.event_level_selected_label = QLabel("未选择")

        self.event_level_selected_label.setStyleSheet(

            "color: #EF4444; font-size: 11px;"

        )



        event_level_layout.addWidget(event_level_label)

        event_level_layout.addWidget(self.event_level_combo)

        event_level_layout.addWidget(self.event_level_selected_label)

        event_level_layout.addStretch()



        # 事件发现来源单选（事件通告专用）

        self.event_source_options = list(EVENT_SOURCE_OPTIONS)

        self.event_source_container = QWidget()

        event_source_layout = QHBoxLayout(self.event_source_container)

        event_source_layout.setContentsMargins(0, 0, 0, 0)

        event_source_layout.setSpacing(6)



        event_source_label = QLabel("事件来源:")

        event_source_label.setStyleSheet("color: #F59E0B; font-size: 12px;")



        self.event_source_combo = QComboBox()

        self.event_source_combo.setMaximumWidth(200)

        self.event_source_combo.setStyleSheet("""

            QComboBox {

                background-color: #2A2A3C;

                border: 1px solid #4F4F7A;

                border-radius: 4px;

                padding: 4px 8px;

                color: #E5E7EB;

                font-size: 12px;

            }

            QComboBox:focus {

                border-color: #8B5CF6;

            }

            QComboBox::drop-down {

                border: none;

            }

            QComboBox QAbstractItemView {

                background-color: #2A2A3C;

                border: 1px solid #4F4F7A;

                selection-background-color: #3B82F6;

            }

        """)

        self.event_source_combo.addItem(EVENT_SOURCE_PLACEHOLDER)

        for option in self.event_source_options:

            self.event_source_combo.addItem(option)

        self.event_source_combo.activated.connect(self._on_event_source_selected)



        self.event_source_selected_label = QLabel("未选择")

        self.event_source_selected_label.setStyleSheet(

            "color: #EF4444; font-size: 11px;"

        )



        event_source_layout.addWidget(event_source_label)

        event_source_layout.addWidget(self.event_source_combo)

        event_source_layout.addWidget(self.event_source_selected_label)

        event_source_layout.addStretch()



        # 事件恢复开关（事件通告专用）

        self.recover_container = QWidget()

        recover_layout = QHBoxLayout(self.recover_container)

        recover_layout.setContentsMargins(0, 0, 0, 0)

        recover_layout.setSpacing(6)



        recover_label = QLabel("事件恢复:")

        recover_label.setStyleSheet("color: #F59E0B; font-size: 12px;")

        self.recover_btn = QCheckBox("事件恢复")

        self.recover_btn.setChecked(False)

        self.recover_btn.setStyleSheet("color: #E5E7EB; font-size: 12px;")

        self.recover_btn.stateChanged.connect(self._on_recover_toggled)



        self.transfer_overhaul_checkbox = QCheckBox("是否转检修")

        self.transfer_overhaul_checkbox.setChecked(False)

        self.transfer_overhaul_checkbox.setStyleSheet(

            "color: #E5E7EB; font-size: 12px;"

        )

        self.transfer_overhaul_checkbox.stateChanged.connect(

            self._on_transfer_overhaul_toggled

        )



        self.recover_state_label = QLabel("未选择")

        self.recover_state_label.setStyleSheet("color: #EF4444; font-size: 11px;")



        recover_layout.addWidget(recover_label)

        recover_layout.addWidget(self.recover_btn)

        recover_layout.addWidget(self.transfer_overhaul_checkbox)

        recover_layout.addWidget(self.recover_state_label)

        recover_layout.addStretch()



        # 变更等级单选（设备变更专用，智航等级）

        self.change_level_options = list(CHANGE_ZHIHANG_LEVEL_OPTIONS)

        self.change_level_container = QWidget()

        change_level_layout = QHBoxLayout(self.change_level_container)

        change_level_layout.setContentsMargins(0, 0, 0, 0)

        change_level_layout.setSpacing(6)



        change_level_label = QLabel("智航-变更等级:")

        change_level_label.setStyleSheet("color: #F59E0B; font-size: 12px;")



        self.change_level_combo = QComboBox()

        self.change_level_combo.setMaximumWidth(120)

        self.change_level_combo.setStyleSheet("""

            QComboBox {

                background-color: #2A2A3C;

                border: 1px solid #4F4F7A;

                border-radius: 4px;

                padding: 4px 8px;

                color: #E5E7EB;

                font-size: 12px;

            }

            QComboBox:focus {

                border-color: #8B5CF6;

            }

            QComboBox::drop-down {

                border: none;

            }

            QComboBox QAbstractItemView {

                background-color: #2A2A3C;

                border: 1px solid #4F4F7A;

                selection-background-color: #3B82F6;

            }

        """)

        for option in self.change_level_options:

            self.change_level_combo.addItem(option)

        self.change_level_combo.activated.connect(self._on_change_level_selected)



        self.change_level_selected_label = QLabel("未选择")

        self.change_level_selected_label.setStyleSheet(

            "color: #EF4444; font-size: 11px;"

        )



        change_level_layout.addWidget(change_level_label)

        change_level_layout.addWidget(self.change_level_combo)

        change_level_layout.addWidget(self.change_level_selected_label)

        change_level_layout.addStretch()



        # 专业单选（设备调整专用）

        self.specialty_options = list(SPECIALTY_OPTIONS)

        self.specialty_container = QWidget()

        specialty_layout = QHBoxLayout(self.specialty_container)

        specialty_layout.setContentsMargins(0, 0, 0, 0)

        specialty_layout.setSpacing(6)



        specialty_label = QLabel("专业:")

        specialty_label.setStyleSheet("color: #F59E0B; font-size: 12px;")



        self.specialty_combo = QComboBox()

        self.specialty_combo.setMaximumWidth(160)

        self.specialty_combo.setStyleSheet("""

            QComboBox {

                background-color: #2A2A3C;

                border: 1px solid #4F4F7A;

                border-radius: 4px;

                padding: 4px 8px;

                color: #E5E7EB;

                font-size: 12px;

            }

            QComboBox:focus {

                border-color: #8B5CF6;

            }

            QComboBox::drop-down {

                border: none;

            }

            QComboBox QAbstractItemView {

                background-color: #2A2A3C;

                border: 1px solid #4F4F7A;

                selection-background-color: #3B82F6;

            }

        """)

        self.specialty_combo.addItem(SPECIALTY_PLACEHOLDER)

        for option in self.specialty_options:

            self.specialty_combo.addItem(option)

        self.specialty_combo.activated.connect(self._on_specialty_selected)



        self.specialty_selected_label = QLabel("未选择")

        self.specialty_selected_label.setStyleSheet("color: #EF4444; font-size: 11px;")



        specialty_layout.addWidget(specialty_label)

        specialty_layout.addWidget(self.specialty_combo)

        specialty_layout.addWidget(self.specialty_selected_label)

        specialty_layout.addStretch()



        inner_layout.addLayout(top_bar)

        inner_layout.addWidget(self.hint_label)

        inner_layout.addWidget(self.preview_label, 1)

        inner_layout.addWidget(self.extra_upload_container)

        inner_layout.addLayout(time_row)

        inner_layout.addWidget(self.building_container)

        inner_layout.addWidget(self.event_level_container)

        inner_layout.addWidget(self.event_source_container)

        inner_layout.addWidget(self.recover_container)

        inner_layout.addWidget(self.change_level_container)

        inner_layout.addWidget(self.specialty_container)

        inner_layout.addLayout(btn_layout)

        layout.addWidget(self.container)

        self.setStyleSheet(get_stylesheet(self.theme))



        self.drag_position = None

        self.data_dict = None

        self.action_type = "upload"

        self.region_selector = None



    @staticmethod

    def _normalize_buildings_value(value):

        return normalize_buildings_list(value)



    def bind_cache_store(self, store):

        self.cache_store = store



    def _get_cache_identity(self):

        if not isinstance(self.data_dict, dict):

            return ""

        record_id = str(self.data_dict.get("record_id") or "").strip()

        return record_id



    def _load_cached_state(self):
        if not self.cache_store:
            return {}
        record_id = self._get_cache_identity()
        if not record_id:
            return {}

        try:

            return self.cache_store.get_record_fields(

                record_id=record_id,

                fields=[
                    "buildings",
                    "specialty",
                    "level",
                    "level_locked",
                    "event_source",
                    "transfer_to_overhaul",
                ],
            )
        except Exception:
            return {}


    def _patch_cache_fields(self, patch):
        if not isinstance(patch, dict) or not patch:
            return
        if not self.cache_store:

            return

        record_id = self._get_cache_identity()

        if not record_id:

            return

        try:

            self.cache_store.patch_record_fields(

                record_id=record_id,

                patch=patch,

            )

        except Exception:
            return

    def _is_level_locked(self, cache_state: dict | None = None) -> bool:
        if isinstance(cache_state, dict) and "level_locked" in cache_state:
            return bool(cache_state.get("level_locked"))
        return bool((self.data_dict or {}).get("level_locked"))

    def _lock_level_selection(self):
        self._update_data_dict_field("level_locked", True, remove_when_empty=False)
        self._patch_cache_fields({"level_locked": True})


    def set_data(self, data_dict, action_type="upload", is_mandatory=False):

        self.data_dict = data_dict

        self.action_type = action_type

        self.is_mandatory = is_mandatory

        self.notice_type = (data_dict or {}).get("notice_type", "")

        if isinstance(self.data_dict, dict):

            self.data_dict.pop("recover_selected", None)

        self.enable_extra_upload = self.notice_type in ("维保通告", "设备检修")

        self.enable_specialty_select = self.notice_type in (

            "设备调整",

            "设备轮巡",

            "设备轮询",

            "事件通告",

            "维保通告",

        )

        self.enable_change_level_select = self.notice_type in ("设备变更", "变更通告")

        self.enable_event_level_select = self.notice_type == "事件通告"

        self.enable_event_source_select = self.notice_type == "事件通告"

        self.enable_recover_select = self.notice_type == "事件通告"

        self.require_building = True

        self.enable_building_multi_select = self.notice_type not in ("事件通告",)

        self.reset_state()



    def reset_state(self):
        cache_state = self._load_cached_state()
        level_locked = self._is_level_locked(cache_state)
        self.screenshot_bytes = None
        self.screenshot_image = None
        self.extra_images = []
        self.selected_specialty = ""
        self.selected_change_level = ""
        self.selected_event_level = ""
        self.preview_label.setPixmap(QPixmap())

        self.preview_label.setText("截图预览区域")

        self.btn_confirm.setEnabled(False)

        self.hint_label.setText("是否需要截图？点击「开始截图」后框选屏幕区域。")

        self.time_input.clear()

        self.ocr_status_label.setText("")

        self.ocr_response_time = None

        self._ocr_vote_candidates = []

        self._ocr_vote_summary = {}

        self.extra_upload_container.setVisible(self.enable_extra_upload)

        self._update_extra_images_label()

        self.specialty_container.setVisible(self.enable_specialty_select)

        self.change_level_container.setVisible(self.enable_change_level_select)

        self.event_level_container.setVisible(self.enable_event_level_select)

        self.event_source_container.setVisible(self.enable_event_source_select)

        self.building_container.setVisible(self.require_building)

        self._update_recover_visibility()

        if not hasattr(self, "datetime_popup"):

            self.datetime_popup = None

        if self.datetime_popup:

            self.datetime_popup.hide()

        self._datetime_dialog_open = False



        self.recover_selected = False

        self.recover_btn.setChecked(False)

        self.recover_state_label.setText("未选择")

        self.recover_state_label.setStyleSheet("color: #EF4444; font-size: 11px;")

        if self.enable_recover_select:

            if "transfer_to_overhaul" in cache_state:

                saved_transfer = bool(cache_state.get("transfer_to_overhaul"))

            else:

                saved_transfer = bool(

                    (self.data_dict or {}).get("transfer_to_overhaul")

                )

            self.transfer_to_overhaul_selected = saved_transfer

            self.transfer_overhaul_checkbox.setChecked(saved_transfer)

            self._update_data_dict_field(

                "transfer_to_overhaul",

                self.transfer_to_overhaul_selected,

                remove_when_empty=False,

            )

        else:
            self.transfer_to_overhaul_selected = False
            self.transfer_overhaul_checkbox.setChecked(False)

        if self.enable_change_level_select:
            cached_level = str(cache_state.get("level") or "").strip()
            saved_level = (self.data_dict or {}).get("level")
            detected_level = self._detect_change_level(
                (self.data_dict or {}).get("text", "")
            )
            if level_locked and cached_level in self.change_level_options:
                level = cached_level
            elif level_locked:
                level = LEVEL_I3
            elif detected_level in self.change_level_options:
                level = detected_level
            elif cached_level in self.change_level_options:
                level = cached_level
            elif saved_level in self.change_level_options:
                level = saved_level
            else:
                level = LEVEL_I3
            self.selected_change_level = level
            idx = self.change_level_combo.findText(level)

            self.change_level_combo.setCurrentIndex(idx if idx != -1 else 0)

            self.change_level_selected_label.setText(level)

            self.change_level_selected_label.setStyleSheet(

                "color: #10B981; font-size: 11px;"

            )

        else:

            self.change_level_combo.setCurrentIndex(0)

            self.change_level_selected_label.setText("未选择")

            self.change_level_selected_label.setStyleSheet(

                "color: #EF4444; font-size: 11px;"

            )



        if self.enable_event_level_select:
            cached_level = str(cache_state.get("level") or "").strip()
            saved_level = (self.data_dict or {}).get("level")
            detected_level = self._detect_event_level(
                (self.data_dict or {}).get("text", "")
            )
            if level_locked:
                self._set_event_level(cached_level)
            elif cached_level:
                self._set_event_level(cached_level)
            elif saved_level:
                self._set_event_level(saved_level)
            else:
                self._set_event_level(detected_level)
        else:
            self.selected_event_level = ""

            self.event_level_combo.setCurrentIndex(0)

            self.event_level_selected_label.setText("未选择")

            self.event_level_selected_label.setStyleSheet(
                "color: #EF4444; font-size: 11px;"
            )
        if level_locked:
            self._update_data_dict_field("level_locked", True, remove_when_empty=False)
        elif isinstance(self.data_dict, dict):
            self.data_dict.pop("level_locked", None)
        self._update_recover_visibility()


        if self.enable_event_source_select:

            cached_source = str(cache_state.get("event_source") or "").strip()

            saved_source = (self.data_dict or {}).get("event_source")

            detected_source = self._detect_event_source(

                (self.data_dict or {}).get("text", "")

            )

            if cached_source:

                self._set_event_source(cached_source)

            elif saved_source:

                self._set_event_source(saved_source)

            else:

                self._set_event_source(detected_source)

        else:

            self.selected_event_source = ""

            self.event_source_combo.setCurrentIndex(0)

            self.event_source_selected_label.setText("未选择")

            self.event_source_selected_label.setStyleSheet(

                "color: #EF4444; font-size: 11px;"

            )



        if self.enable_specialty_select:

            self._refresh_specialty_options()

            cached_specialty = str(cache_state.get("specialty") or "").strip()

            if cached_specialty:

                self.selected_specialty = cached_specialty

                idx = self.specialty_combo.findText(self.selected_specialty)

                self.specialty_combo.setCurrentIndex(idx if idx != -1 else 0)

                self.specialty_selected_label.setText(self.selected_specialty)

                self.specialty_selected_label.setStyleSheet(

                    "color: #10B981; font-size: 11px;"

                )

                self._update_data_dict_field(

                    "specialty", self.selected_specialty, remove_when_empty=True

                )

            else:

                self.specialty_combo.setCurrentIndex(0)

                self.specialty_selected_label.setText("未选择")

                self.specialty_selected_label.setStyleSheet(

                    "color: #EF4444; font-size: 11px;"

                )

                self._update_data_dict_field("specialty", "", remove_when_empty=True)



        self.selected_buildings.clear()

        if self.require_building:

            cached_buildings = self._normalize_buildings_value(

                cache_state.get("buildings")

            )

            selected_buildings = []

            if cached_buildings:

                selected_buildings = cached_buildings

            if selected_buildings:

                if self.enable_building_multi_select:

                    self.selected_buildings = set(selected_buildings)

                else:

                    self.selected_buildings = {selected_buildings[0]}

        if self.selected_buildings:

            self.selected_buildings_label.setText(

                ", ".join(sorted(self.selected_buildings))

            )

            self.selected_buildings_label.setStyleSheet(

                "color: #10B981; font-size: 11px;"

            )

        else:

            self.selected_buildings_label.setText("未选择")

            self.selected_buildings_label.setStyleSheet(

                "color: #EF4444; font-size: 11px;"

            )



        if self.require_building:

            self._update_data_dict_field(

                "buildings", sorted(self.selected_buildings), remove_when_empty=False

            )



        if self.require_building:

            if self.enable_building_multi_select:

                self.building_combo.setCurrentIndex(0)  # 重置为"请选择楼栋..."

            else:

                current = next(iter(self.selected_buildings), "")

                idx = self.building_combo.findText(current)

                self.building_combo.setCurrentIndex(idx if idx != -1 else 0)

        self._ocr_completed = False



        # 强制模式处理

        if self.is_mandatory:

            self.close_btn.setEnabled(False)

            self.close_btn.setStyleSheet("color: #555; border: none;")

        else:

            self.close_btn.setEnabled(True)

            self.close_btn.setStyleSheet("")



        self._refresh_submit_state()



    def _refresh_specialty_options(self):

        if not self.enable_specialty_select:

            return

        if self.notice_type == "事件通告":

            options = [

                SPECIALTY_ELECTRIC,

                SPECIALTY_HVAC,

                SPECIALTY_FIRE,

                SPECIALTY_WEAK,

                OPTION_SLASH,

            ]

        elif self.notice_type in ("设备调整", "设备轮巡", "设备轮询", "维保通告"):

            options = [

                SPECIALTY_ELECTRIC,

                SPECIALTY_HVAC,

                SPECIALTY_FIRE,

                SPECIALTY_WEAK,

                SPECIALTY_OTHER,

                OPTION_SLASH,

            ]

        else:

            options = list(self.specialty_options)



        self.specialty_combo.blockSignals(True)

        self.specialty_combo.clear()

        self.specialty_combo.addItem(SPECIALTY_PLACEHOLDER)

        for option in options:

            self.specialty_combo.addItem(option)

        self.specialty_combo.blockSignals(False)



    def _extract_section(self, text: str, label: str) -> str:

        if not text:

            return ""

        pattern = re.compile(rf"【{label}】(.*?)(?=【|$)", re.DOTALL)

        match = pattern.search(text)

        if not match:

            return ""

        return match.group(1).strip()



    def _detect_event_level(self, text: str) -> str:

        title = self._extract_section(text or "", "标题") or (text or "")

        title_upper = title.upper()

        if re.search(rf"{LEVEL_I3}\s*[→>\\-]\s*{LEVEL_I2}", title_upper):

            return EVENT_LEVEL_UPGRADE_I3_TO_I2

        if re.search(rf"{LEVEL_I3}\s*[→>\\-]\s*{LEVEL_I1}", title_upper):

            return EVENT_LEVEL_UPGRADE_I3_TO_I1

        for option in EVENT_LEVEL_OPTIONS:

            if option == OPTION_SLASH:

                continue

            if option in title_upper:

                return option

        return ""



    def _detect_change_level(self, text: str) -> str:
        raw = self._extract_section(text or "", "等级")
        if ALI_LEVEL_ULTRA_LOW in raw:
            return LEVEL_I3
        if ALI_LEVEL_LOW in raw:
            return LEVEL_I3
        if ALI_LEVEL_MEDIUM in raw:
            return LEVEL_I2
        if ALI_LEVEL_HIGH in raw:
            return LEVEL_I1
        return ""


    def _detect_building_from_title(self, text: str) -> str:

        raw_text = text or ""

        title = self._extract_section(raw_text, "标题")

        if not title:

            title = self._extract_section(raw_text, "名称")



        # 先用标题识别；标题未命中时再用全文兜底，兼容维保通告中楼栋不在标题的场景。

        candidates = []

        if title:

            candidates.append(title)

        if raw_text and raw_text != title:

            candidates.append(raw_text)



        def _normalize(s: str) -> str:

            if not s:

                return ""

            # NFKC 将全角字母数字归一化，随后去空白并统一大写。

            return "".join(unicodedata.normalize("NFKC", s).split()).upper()



        for candidate in candidates:

            candidate_norm = _normalize(candidate)

            if not candidate_norm:

                continue



            for keyword in BUILDING_DETECT_110_KEYWORDS:

                if _normalize(keyword) in candidate_norm:

                    return BUILDING_110

            for key, value in BUILDING_DETECT_ALIASES:

                if _normalize(key) in candidate_norm:

                    return value

        return ""



    def _detect_event_source(self, text: str) -> str:

        source_text = self._extract_section(text or "", "来源")

        if not source_text:

            return ""



        source_upper = source_text.upper()

        if "盯屏" in source_text:

            return EVENT_SOURCE_DINGPING

        if "BA" in source_upper:

            return EVENT_SOURCE_BA

        if "BMS" in source_upper:

            return EVENT_SOURCE_BMS

        if "维护" in source_text or "维保" in source_text:

            return EVENT_SOURCE_PPM

        if "消防" in source_text:

            return EVENT_SOURCE_FIRE

        if "变更" in source_text:

            return EVENT_SOURCE_CHANGE

        if "巡检" in source_text:

            return EVENT_SOURCE_PATROL

        if "客户" in source_text:

            return EVENT_SOURCE_CUSTOMER

        if "CCTV" in source_upper:

            return EVENT_SOURCE_CCTV

        if "门禁" in source_text:

            return EVENT_SOURCE_ACCESS



        for option in self.event_source_options:

            if option == OPTION_SLASH:

                continue

            if option in source_text:

                return option

        return ""



    def _set_event_level(self, level: str):

        if not level:

            self.selected_event_level = ""

            self.event_level_combo.setCurrentIndex(0)

            self.event_level_selected_label.setText("未选择")

            self.event_level_selected_label.setStyleSheet(

                "color: #EF4444; font-size: 11px;"

            )

            self._update_data_dict_field("level", "", remove_when_empty=True)

            return



        idx = self.event_level_combo.findText(level)

        if idx == -1:

            self.selected_event_level = ""

            self.event_level_combo.setCurrentIndex(0)

            self.event_level_selected_label.setText("未选择")

            self.event_level_selected_label.setStyleSheet(

                "color: #EF4444; font-size: 11px;"

            )

            self._update_data_dict_field("level", "", remove_when_empty=True)

            return



        self.selected_event_level = level

        self.event_level_combo.setCurrentIndex(idx)

        self.event_level_selected_label.setText(level)

        self.event_level_selected_label.setStyleSheet(

            "color: #10B981; font-size: 11px;"

        )

        self._update_data_dict_field(

            "level", self.selected_event_level, remove_when_empty=True

        )



    def _set_event_source(self, source: str):

        if not source:

            self.selected_event_source = ""

            self.event_source_combo.setCurrentIndex(0)

            self.event_source_selected_label.setText("未选择")

            self.event_source_selected_label.setStyleSheet(

                "color: #EF4444; font-size: 11px;"

            )

            self._update_data_dict_field("event_source", "", remove_when_empty=True)

            return



        idx = self.event_source_combo.findText(source)

        if idx == -1:

            self.selected_event_source = ""

            self.event_source_combo.setCurrentIndex(0)

            self.event_source_selected_label.setText("未选择")

            self.event_source_selected_label.setStyleSheet(

                "color: #EF4444; font-size: 11px;"

            )

            self._update_data_dict_field("event_source", "", remove_when_empty=True)

            return



        self.selected_event_source = source

        self.event_source_combo.setCurrentIndex(idx)

        self.event_source_selected_label.setText(source)

        self.event_source_selected_label.setStyleSheet(

            "color: #10B981; font-size: 11px;"

        )

        self._update_data_dict_field(

            "event_source", self.selected_event_source, remove_when_empty=True

        )



    def _on_event_level_selected(self, index: int):
        if index <= 0:
            self._set_event_level("")
        else:
            self._set_event_level(self.event_level_combo.itemText(index))
        self._lock_level_selection()
        self._update_recover_visibility()
        self._update_data_dict_field(
            "level", self.selected_event_level, remove_when_empty=True
        )
        self._patch_cache_fields(
            {
                "level": self.selected_event_level or None,
                "level_locked": True,
            }
        )
        self._refresh_submit_state()
        self._notify_state_changed()


    def _on_event_source_selected(self, index: int):

        if index <= 0:

            self._set_event_source("")

        else:

            self._set_event_source(self.event_source_combo.itemText(index))

        self._refresh_submit_state()

        self._update_data_dict_field(

            "event_source", self.selected_event_source, remove_when_empty=True

        )

        self._patch_cache_fields({"event_source": self.selected_event_source or None})

        self._notify_state_changed()



    def _on_recover_toggled(self):

        self.recover_selected = bool(self.recover_btn.isChecked())

        if self.recover_selected:

            self.recover_state_label.setText("已选择")

            self.recover_state_label.setStyleSheet("color: #10B981; font-size: 11px;")

        else:

            self.recover_state_label.setText("未选择")

            self.recover_state_label.setStyleSheet("color: #EF4444; font-size: 11px;")

        self._refresh_submit_state()

        self._notify_state_changed()



    def _on_transfer_overhaul_toggled(self):

        self.transfer_to_overhaul_selected = bool(

            self.transfer_overhaul_checkbox.isChecked()

        )

        self._update_data_dict_field(

            "transfer_to_overhaul",

            self.transfer_to_overhaul_selected,

            remove_when_empty=False,

        )

        self._patch_cache_fields(

            {"transfer_to_overhaul": bool(self.transfer_to_overhaul_selected)}

        )

        self._refresh_submit_state()

        self._notify_state_changed()



    def _update_recover_visibility(self):

        if not self.enable_recover_select:

            self.recover_container.setVisible(False)

            self.recover_selected = False

            if hasattr(self, "recover_btn"):

                self.recover_btn.setChecked(False)

            self.transfer_to_overhaul_selected = False

            if hasattr(self, "transfer_overhaul_checkbox"):

                self.transfer_overhaul_checkbox.setChecked(False)

            if isinstance(self.data_dict, dict):

                self.data_dict.pop("transfer_to_overhaul", None)

            return



        self.recover_container.setVisible(True)



    def _reset_recover_selection(self):

        self.recover_selected = False

        if hasattr(self, "recover_btn"):

            self.recover_btn.setChecked(False)

        if hasattr(self, "recover_state_label"):

            self.recover_state_label.setText("未选择")

            self.recover_state_label.setStyleSheet("color: #EF4444; font-size: 11px;")



    def _update_data_dict_field(self, key, value, remove_when_empty=True):

        if not isinstance(self.data_dict, dict):

            return

        if remove_when_empty and (value is None or value == "" or value == []):

            self.data_dict.pop(key, None)

        else:

            self.data_dict[key] = value



    def _notify_state_changed(self):

        self.state_changed.emit()



    def _normalize_time_text(self, text: str) -> str:

        if not text:

            return ""

        normalized = text.strip()

        normalized = (

            normalized.replace("：", ":")

            .replace("．", ":")

            .replace("。", ":")

            .replace("点", ":")

            .replace("时", ":")

            .replace("分", "")

            .replace("年", "-")

            .replace("月", "-")

            .replace("日", "")

            .replace("/", "-")

            .replace("－", "-")

            .replace("—", "-")

        )

        normalized = re.sub(r"\s+", " ", normalized)

        return normalized.strip()



    def _parse_time_text(self, text: str):

        if not text:

            return None, ""

        normalized = self._normalize_time_text(text)

        if not normalized:

            return None, ""



        from datetime import datetime, timedelta



        today = datetime.now().date()

        base_date = today

        if "昨天" in normalized or "昨日" in normalized:

            base_date = today - timedelta(days=1)

            normalized = normalized.replace("昨天", "").replace("昨日", "").strip()

        if "今天" in normalized or "今日" in normalized:

            normalized = normalized.replace("今天", "").replace("今日", "").strip()



        match = re.search(

            r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})\s*(\d{1,2}):(\d{1,2})",

            normalized,

        )

        if match:

            try:

                dt = datetime(

                    int(match.group(1)),

                    int(match.group(2)),

                    int(match.group(3)),

                    int(match.group(4)),

                    int(match.group(5)),

                )

                return dt, dt.strftime("%Y-%m-%d %H:%M")

            except ValueError:

                try:

                    dt = datetime(

                        base_date.year,

                        base_date.month,

                        base_date.day,

                        int(match.group(4)),

                        int(match.group(5)),

                    )

                    return dt, dt.strftime("%Y-%m-%d %H:%M")

                except ValueError:

                    return None, ""



        match = re.search(

            r"(\d{1,2})[./-](\d{1,2})\s*(\d{1,2}):(\d{1,2})",

            normalized,

        )

        if match:

            try:

                dt = datetime(

                    base_date.year,

                    int(match.group(1)),

                    int(match.group(2)),

                    int(match.group(3)),

                    int(match.group(4)),

                )

                return dt, dt.strftime("%Y-%m-%d %H:%M")

            except ValueError:

                try:

                    dt = datetime(

                        base_date.year,

                        base_date.month,

                        base_date.day,

                        int(match.group(3)),

                        int(match.group(4)),

                    )

                    return dt, dt.strftime("%Y-%m-%d %H:%M")

                except ValueError:

                    return None, ""



        match = re.search(r"(\d{1,2}):(\d{1,2})", normalized)

        if match:

            try:

                dt = datetime(

                    base_date.year,

                    base_date.month,

                    base_date.day,

                    int(match.group(1)),

                    int(match.group(2)),

                )

                return dt, dt.strftime("%Y-%m-%d %H:%M")

            except ValueError:

                return None, ""



        return None, ""



    def _normalize_ocr_text_for_time(self, text: str) -> str:

        if not text:

            return ""



        # 汉字容错先执行，避免被后续拆解

        normalized = text.replace("听天", "昨天").replace("昨夭", "昨天")



        normalized = unicodedata.normalize("NFKC", str(normalized))

        lookalike_map = {

            "O": "0",

            "o": "0",

            "D": "0",

            "Q": "0",

            "〇": "0",

            "C": "0",

            "U": "0",

            "I": "1",

            "l": "1",

            "i": "1",

            "|": "1",

            "!": "1",

            "一": "1",

            "丨": "1",

            "亅": "1",

            "[": "1",

            "]": "1",

            "/": "1",

            "\\": "1",

            "Z": "2",

            "z": "2",

            "乙": "2",

            "己": "2",

            "ㄗ": "2",

            "E": "3",

            "З": "3",

            "彐": "3",

            "ヨ": "3",

            "}": "3",

            "A": "4",

            "H": "4",

            "Ч": "4",

            "+": "4",

            "S": "5",

            "s": "5",

            "$": "5",

            "ㄎ": "5",

            "G": "6",

            "b": "6",

            "占": "6",

            "T": "7",

            "┐": "7",

            "〉": "7",

            "了": "7",

            "B": "8",

            "S": "8",

            "&": "8",

            "吕": "8",

            "串": "8",

            "g": "9",

            "q": "9",

            "P": "9",

            "p": "9",

            ";": ":",

            ".": ":",

            ",": ":",

            "：": ":",

            "．": ":",

            "。": ":",

            "巳": ":",

            "听": "昨",

            "夭": "天",

            "大": "天",

            "夫": "天",

            "俞": "前",

            "口": "日",

            "闩": "月",

            "点": ":",

            "时": ":",

            "分": "",

            "，": ",",

        }



        sanitized = "".join([lookalike_map.get(char, char) for char in normalized])

        sanitized = re.sub(r"\s+", " ", sanitized)

        return sanitized.strip()



    def _is_time_section_line(self, text: str) -> bool:

        compact = re.sub(r"\s+", "", text or "")

        return bool(re.search(r"(?:^|【)时间(?:】|[:：]|$)", compact))



    def _is_time_range_line(self, normalized_text: str) -> bool:

        if not normalized_text:

            return False

        return bool(

            re.search(

                r"\d{1,2}\s*[:.]\s*\d{1,2}\s*[-~—–至到]+\s*\d{1,2}\s*[:.]\s*\d{1,2}",

                normalized_text,

            )

        )



    def _extract_time_candidates_from_text(self, normalized_text: str):

        if not normalized_text:

            return []

        extracted = []

        seen = set()



        def _append_candidate(hour, minute, second, raw_token, kind):

            try:

                h = int(hour)

                m = int(minute)

                s = int(second) if second is not None else 0

            except Exception:

                return

            if h > 23 or m > 59 or s > 59:

                return

            key = (h, m, s, kind, str(raw_token or ""))

            if key in seen:

                return

            seen.add(key)

            extracted.append(

                {

                    "hour": h,

                    "minute": m,

                    "second": s,

                    "time": f"{h:02d}:{m:02d}",

                    "raw_token": str(raw_token or ""),

                    "kind": kind,

                }

            )



        # 09:20 / 9:2 / 14.21 / 14:21:30 / 1 1: 06 (断裂版)

        for match in re.finditer(

            r"(?<!\d)(\d\s*\d?)\s*[:.]\s*(\d\s*\d?)(?:\s*[:.]\s*(\d\s*\d?))?(?!\d)",

            normalized_text,

        ):

            _append_candidate(

                match.group(1).replace(" ", ""),

                match.group(2).replace(" ", ""),

                match.group(3).replace(" ", "") if match.group(3) else None,

                match.group(0),

                "sep",

            )



        # 09 20 / 09,20

        for match in re.finditer(

            r"(?<!\d)(\d{1,2})\s*[, ]\s*(\d{2})(?!\d)", normalized_text

        ):

            _append_candidate(

                match.group(1),

                match.group(2),

                None,

                match.group(0),

                "space_or_comma",

            )



        # 0920

        for match in re.finditer(r"(?<!\d)(\d{4})(?!\d)", normalized_text):

            digits = match.group(1)

            _append_candidate(digits[:2], digits[2:], None, digits, "hhmm")



        return extracted



    def _score_response_time_candidate(

        self,

        raw_text: str,

        normalized_text: str,

        candidate: dict,

        confidence: float,

        y_ratio: float,

    ) -> float:

        conf = max(0.0, min(float(confidence or 0.0), 1.0))

        y = max(0.0, min(float(y_ratio or 0.5), 1.0))

        compact = re.sub(r"\s+", "", normalized_text or "")

        token = str(candidate.get("time") or "")

        token_compact = token.replace(":", "")

        raw_token = re.sub(r"\s+", "", str(candidate.get("raw_token") or ""))



        score = 0.0

        score += conf * 1.6

        score += (1.0 - y) * 1.2



        line_is_pure_time = False

        wrapped = compact.strip("[]()（）")

        if wrapped in {token, token_compact, raw_token}:

            line_is_pure_time = True



        if line_is_pure_time:

            score += 1.8

        elif len(compact) <= 10:

            score += 0.8

        elif len(compact) > 24:

            score -= 0.25



        kind = str(candidate.get("kind") or "")

        if kind == "sep":

            score += 0.7

        elif kind == "space_or_comma":

            score += 0.2

        elif kind == "hhmm":

            score -= 0.2



        if re.search(r"\d{4}\D+\d{1,2}\D+\d{1,2}", raw_text or ""):

            score -= 0.8

        if any(k in (raw_text or "") for k in ("计划", "维护", "通告", "开始", "结束")):

            score -= 0.5

        if "时间" in (raw_text or "") and not line_is_pure_time:

            score -= 0.2

        if y <= 0.15:

            score += 0.4



        return score



    def _collect_response_time_candidates(

        self,

        ocr_lines: list[dict],

        image_height: int,

        phase: str = "",

        source_zone: str = "",

        variant_id: str = "",

    ):

        stats = {

            "phase": phase or "",

            "lines_total": 0,

            "skip_empty": 0,

            "skip_section_label": 0,

            "skip_range_line": 0,

            "penalty_noisy": 0,

            "penalty_long_line": 0,

            "no_time_match": 0,

            "candidates": 0,

        }

        candidates = []

        image_h = max(1, int(image_height or 1))



        for idx, line in enumerate(ocr_lines or []):

            stats["lines_total"] += 1

            raw_text = str((line or {}).get("text") or "").strip()

            if not raw_text:

                stats["skip_empty"] += 1

                continue



            normalized_text = self._normalize_ocr_text_for_time(raw_text)

            if not normalized_text:

                stats["skip_empty"] += 1

                continue



            if self._is_time_section_line(raw_text):

                stats["skip_section_label"] += 1

                continue



            if self._is_time_range_line(normalized_text):

                stats["skip_range_line"] += 1

                continue



            line_candidates = self._extract_time_candidates_from_text(normalized_text)

            if not line_candidates:

                stats["no_time_match"] += 1

                continue

            compact_line = re.sub(r"\s+", "", normalized_text)

            if len(compact_line) > 20:

                stats["penalty_long_line"] += 1



            try:

                conf = float((line or {}).get("conf") or 0.0)

            except Exception:

                conf = 0.0



            try:

                y_center = float((line or {}).get("y_center"))

                y_ratio = y_center / image_h

            except Exception:

                y_ratio = 0.5



            for one in line_candidates:

                if one.get("kind") == "space_or_comma" and len(compact_line) > 8:

                    stats["penalty_noisy"] += 1

                score = self._score_response_time_candidate(

                    raw_text=raw_text,

                    normalized_text=normalized_text,

                    candidate=one,

                    confidence=conf,

                    y_ratio=y_ratio,

                )

                candidates.append(

                    {

                        "index": idx,

                        "text": raw_text,

                        "normalized_text": normalized_text,

                        "time": one.get("time"),

                        "hour": one.get("hour"),

                        "minute": one.get("minute"),

                        "second": one.get("second"),

                        "kind": one.get("kind"),

                        "token": one.get("raw_token"),

                        "confidence": conf,

                        "y_ratio": y_ratio,

                        "score": score,

                        "phase": phase or "",

                        "source_zone": source_zone or "",

                        "variant_id": variant_id or "",

                    }

                )

                stats["candidates"] += 1



        return candidates, stats



    def _select_best_response_time_candidate(self, candidates: list[dict]):

        if not candidates:

            return None

        return max(

            candidates,

            key=lambda item: (

                float(item.get("score") or 0.0),

                float(item.get("confidence") or 0.0),

                1.0 - float(item.get("y_ratio") or 1.0),

            ),

        )



    def _extract_top_30_percent_band(self, pil_image):

        if pil_image is None:

            return None

        try:

            width, height = pil_image.size

        except Exception:

            return None

        if width < 20 or height < 24:

            return None



        top_h = max(24, int(height * 0.30))

        top_h = min(top_h, height)

        band = pil_image.crop((0, 0, width, top_h))

        if band.width < 40 or band.height < 10:

            return None

        return band



    def _extract_top_30_percent_scan_windows(self, pil_image):

        band = self._extract_top_30_percent_band(pil_image)

        if band is None:

            return []

        width = int(band.width or 0)

        height = int(band.height or 0)

        if width < 40 or height < 10:

            return []



        windows = []

        window_specs = (

            ("left", 0.00, 0.55),

            ("center", 0.22, 0.78),

            ("right", 0.45, 1.00),

        )

        for name, start_ratio, end_ratio in window_specs:

            start_x = max(0, min(width - 1, int(width * start_ratio)))

            end_x = max(start_x + 1, min(width, int(width * end_ratio)))

            roi = band.crop((start_x, 0, end_x, height))

            if roi.width < 24 or roi.height < 10:

                continue

            windows.append((name, roi))

        return windows



    def _extract_header_time_band(self, pil_image):

        return self._extract_top_30_percent_band(pil_image)



    def _build_header_ocr_variants(self, roi_image):

        if roi_image is None:

            return []

        variants = []

        base_rgb = roi_image.convert("RGB")

        variants.append(("v1", base_rgb))



        resampling = getattr(Image, "Resampling", Image)

        bicubic = getattr(resampling, "BICUBIC", Image.BICUBIC)

        nearest = getattr(resampling, "NEAREST", Image.NEAREST)



        gray = roi_image.convert("L")

        w2 = max(1, gray.width * 2)

        h2 = max(1, gray.height * 2)



        enhanced = ImageEnhance.Contrast(gray).enhance(1.7).resize((w2, h2), bicubic)

        variants.append(("v2", enhanced.convert("RGB")))



        binary = gray.point(lambda p: 255 if p >= 170 else 0, mode="L").resize(

            (w2, h2), nearest

        )

        variants.append(("v3", binary.convert("RGB")))



        # v4: 高对比度增强，用于低对比度浅灰色文字（如钉钉聊天时间戳）

        high_contrast = (

            ImageEnhance.Contrast(gray).enhance(3.0).resize((w2, h2), bicubic)

        )

        variants.append(("v4", high_contrast.convert("RGB")))

        return variants



    def _aggregate_ocr_time_votes(self, per_variant_best: list[dict]) -> dict:

        buckets = {}

        for one in per_variant_best or []:

            if not isinstance(one, dict):

                continue

            best = one.get("best_candidate") or {}

            time_text = str(best.get("time") or "").strip()

            if not re.fullmatch(r"\d{2}:\d{2}", time_text):

                continue

            score = float(best.get("score") or 0.0)

            conf = float(best.get("confidence") or 0.0)

            vote_score = score + 0.5 * conf

            bucket = buckets.setdefault(

                time_text,

                {

                    "time": time_text,

                    "votes": 0,

                    "vote_score": 0.0,

                    "max_conf": 0.0,

                    "variants": [],

                    "best_candidate": None,

                    "ocr_lines": [],

                },

            )

            bucket["votes"] += 1

            bucket["vote_score"] += vote_score

            if conf >= bucket["max_conf"]:

                bucket["max_conf"] = conf

                bucket["best_candidate"] = best

                bucket["ocr_lines"] = list(one.get("ocr_lines") or [])

            variant = str(one.get("variant_id") or "").strip()

            if variant and variant not in bucket["variants"]:

                bucket["variants"].append(variant)



        ordered = sorted(

            buckets.values(),

            key=lambda item: (

                int(item.get("votes") or 0),

                float(item.get("vote_score") or 0.0),

                float(item.get("max_conf") or 0.0),

            ),

            reverse=True,

        )

        return {"ordered": ordered, "total_variants": len(per_variant_best or [])}



    def _is_strong_ocr_vote_bucket(self, bucket: dict) -> bool:

        best = (bucket or {}).get("best_candidate") or {}

        return (

            float(best.get("score") or 0.0) >= 2.0

            and float(best.get("confidence") or 0.0) >= 0.60

        )



    def _decide_voted_time(self, vote_result: dict) -> tuple[str, str, list[str]]:

        ordered = list((vote_result or {}).get("ordered") or [])

        if not ordered:

            return "", "low", []

        first = ordered[0]

        top_two = [

            str(item.get("time") or "") for item in ordered[:2] if item.get("time")

        ]



        if int(first.get("votes") or 0) >= 2:

            certainty = "high" if int(first.get("votes") or 0) >= 3 else "medium"

            return str(first.get("time") or ""), certainty, top_two

        if len(ordered) == 1 and self._is_strong_ocr_vote_bucket(first):

            return str(first.get("time") or ""), "medium", top_two

        return "", "low", top_two



    def _build_prompt_candidates_from_vote_result(self, vote_result: dict) -> list[str]:

        options = []

        for item in list((vote_result or {}).get("ordered") or [])[:2]:

            candidate = item.get("best_candidate")

            candidate_lines = list(item.get("ocr_lines") or [])

            parsed = (

                self._parse_ocr_datetime(candidate, candidate_lines)

                if candidate

                else ""

            )

            value = parsed or str(item.get("time") or "").strip()

            if value and value not in options:

                options.append(value)

        return options



    def _prompt_response_time_choice(self, candidates: list[str]) -> str:

        options = []

        for item in candidates or []:

            text = str(item or "").strip()

            if text and text not in options:

                options.append(text)

        if not options:

            return ""

        if len(options) == 1:

            return options[0]



        dialog = QMessageBox(self)

        dialog.setWindowTitle("响应时间识别")

        dialog.setIcon(QMessageBox.Icon.Question)

        dialog.setText("识别结果不一致，请选择候选时间或手动输入")

        dialog.setInformativeText(f"候选1: {options[0]}\n候选2: {options[1]}")

        btn1 = dialog.addButton(

            f"候选1 ({options[0]})", QMessageBox.ButtonRole.AcceptRole

        )

        btn2 = dialog.addButton(

            f"候选2 ({options[1]})", QMessageBox.ButtonRole.AcceptRole

        )

        btn_manual = dialog.addButton("手动输入", QMessageBox.ButtonRole.ActionRole)

        dialog.exec()

        clicked = dialog.clickedButton()

        if clicked == btn1:

            return options[0]

        if clicked == btn2:

            return options[1]

        if clicked == btn_manual:

            self.time_input.setFocus()

            self.time_input.selectAll()

        return ""



    def _parse_explicit_datetime_context(

        self,

        text: str,

        hour: int,

        minute: int,

        today,

    ):

        from datetime import datetime



        normalized = self._normalize_ocr_text_for_time(text or "")

        if not normalized:

            return None, ""



        # 年月日时分：2026-2-24 08:51 / 2026年2月24日08:51

        full_patterns = (

            re.compile(

                r"(\d\s*\d?\s*\d?\s*\d?)\s*(?:[-./]|年)\s*(\d\s*\d?)\s*(?:[-./]|月)\s*(\d\s*\d?)\s*(?:日|号)?\s*(\d\s*\d?)\s*[:.]\s*(\d\s*\d?)"

            ),

        )

        for pattern in full_patterns:

            match = pattern.search(normalized)

            if not match:

                continue

            try:

                y = int(match.group(1).replace(" ", ""))

                mo = int(match.group(2).replace(" ", ""))

                day = int(match.group(3).replace(" ", ""))

                hh = int(match.group(4).replace(" ", ""))

                mm = int(match.group(5).replace(" ", ""))

                if hh > 23 or mm > 59:

                    continue

                dt = datetime(y, mo, day, hour, minute)

                return dt.date(), "explicit_full"

            except ValueError:

                continue



        # 月日时分：2月24日 08:51 / 2-24 08:51

        md_patterns = (

            re.compile(

                r"(?<!\d)(\d\s*\d?)\s*(?:月|[-./])\s*(\d\s*\d?)\s*(?:日|号)?\s*(\d\s*\d?)\s*[:.]\s*(\d\s*\d?)"

            ),

        )

        for pattern in md_patterns:

            match = pattern.search(normalized)

            if not match:

                continue

            try:

                mo = int(match.group(1).replace(" ", ""))

                day = int(match.group(2).replace(" ", ""))

                hh = int(match.group(3).replace(" ", ""))

                mm = int(match.group(4).replace(" ", ""))

                if hh > 23 or mm > 59:

                    continue

                dt = datetime(today.year, mo, day, hour, minute)

                return dt.date(), "explicit_md"

            except ValueError:

                continue



        return None, ""



    def _extract_relative_day_hint(

        self,

        lines,

        anchor_index: int,

        window: int = 2,

    ):

        line_list = list(lines or [])

        if not line_list:

            return None, ""

        try:

            center = int(anchor_index)

        except Exception:

            center = -1

        if center < 0 or center >= len(line_list):

            center = 0

        win = max(0, int(window or 0))

        visited = set()

        for offset in range(0, win + 1):

            indexes = [center] if offset == 0 else [center - offset, center + offset]

            for idx in indexes:

                if idx < 0 or idx >= len(line_list) or idx in visited:

                    continue

                visited.add(idx)

                text = str((line_list[idx] or {}).get("text") or "").strip()

                if not text:

                    continue

                if self._is_time_section_line(text):

                    continue

                normalized = self._normalize_ocr_text_for_time(text)

                if self._is_time_range_line(normalized):

                    continue

                if "昨天" in text or "昨日" in text:

                    return -1, "relative_yesterday"

                if "今天" in text or "今日" in text:

                    return 0, "relative_today"

        return None, ""



    def _parse_ocr_datetime(self, best_candidate: dict, ocr_lines: list[dict]) -> str:

        if not isinstance(best_candidate, dict):

            return ""



        from datetime import datetime, timedelta



        try:

            hour = int(best_candidate.get("hour"))

            minute = int(best_candidate.get("minute"))

        except Exception:

            time_match = re.search(

                r"(\d{1,2})[:：.．](\d{1,2})",

                str(best_candidate.get("time") or ""),

            )

            if not time_match:

                return ""

            try:

                hour = int(time_match.group(1))

                minute = int(time_match.group(2))

            except Exception:

                return ""



        if hour > 23 or minute > 59:

            return ""



        today = datetime.now().date()

        detected_date = None

        date_source = ""

        try:

            anchor_index = int(best_candidate.get("index", -1))

        except Exception:

            anchor_index = -1

        anchor_text = str(best_candidate.get("text") or "").strip()

        line_list = list(ocr_lines or [])

        if not anchor_text and 0 <= anchor_index < len(line_list):

            anchor_text = str((line_list[anchor_index] or {}).get("text") or "").strip()



        # 1) 仅允许候选锚点行显式日期覆盖，避免正文计划日期污染响应时间。

        if anchor_text:

            detected_date, date_source = self._parse_explicit_datetime_context(

                text=anchor_text,

                hour=hour,

                minute=minute,

                today=today,

            )



        # 2) 邻域相对日期词（昨天/今日）仅作轻提示，不读取绝对日期。

        if detected_date is None:

            day_offset, relative_source = self._extract_relative_day_hint(

                lines=line_list,

                anchor_index=anchor_index,

                window=2,

            )

            if day_offset is not None:

                detected_date = today + timedelta(days=day_offset)

                date_source = relative_source



        # 3) 仅时分默认当天。

        if detected_date is None:

            detected_date = today

            date_source = "fallback_today"



        try:

            dt = datetime(

                detected_date.year,

                detected_date.month,

                detected_date.day,

                hour,

                minute,

            )

        except ValueError:

            return ""

        anchor_preview = re.sub(r"\s+", " ", anchor_text)[:80]

        print(

            "OCR日期绑定: date_source={src} anchor_index={idx} anchor_text={txt}".format(

                src=date_source or "unknown",

                idx=anchor_index,

                txt=anchor_preview,

            )

        )

        return dt.strftime("%Y-%m-%d %H:%M")



    def _refresh_submit_state(self):

        time_valid = self._is_valid_time(self.time_input.text().strip())

        buildings_valid = (not self.require_building) or len(

            self.selected_buildings

        ) > 0

        level_valid = (not self.enable_event_level_select) or bool(

            self.selected_event_level

        )

        source_valid = (not self.enable_event_source_select) or bool(

            self.selected_event_source

        )

        enable_btns = time_valid and buildings_valid and level_valid and source_valid

        self.btn_confirm.setEnabled(enable_btns)

        self.btn_skip.setEnabled(enable_btns)



    def _update_extra_images_label(self):

        count = len(self.extra_images)

        if count:

            self.extra_images_label.setText(f"已添加 {count} 张")

            self.extra_images_label.setStyleSheet("color: #10B981; font-size: 11px;")

        else:

            self.extra_images_label.setText("未添加")

            self.extra_images_label.setStyleSheet("color: #EF4444; font-size: 11px;")



    def _qimage_to_bytes(self, image: QImage):

        try:

            buffer = QBuffer()

            buffer.open(QIODevice.OpenModeFlag.WriteOnly)

            image.save(buffer, "PNG")

            return bytes(buffer.data())

        except Exception:

            return b""



    def _datetime_popup_stylesheet(self) -> str:

        return """

                    QFrame#DateTimePopup {

                        background-color: #1F1F2E;

                        border: 1px solid #4F4F7A;

                        border-radius: 8px;

                    }

                    QCalendarWidget QWidget {

                        background-color: #1F1F2E;

                        color: #E5E7EB;

                        font-size: 12px;

                    }

                    /* 覆盖日历表格底层 */

                    QCalendarWidget QTableView {

                        background-color: #2A2A3C;

                        alternate-background-color: #2A2A3C;

                        selection-background-color: transparent;

                        selection-color: #E5E7EB;

                        outline: 0px;

                        border: none;

                        font-size: 12px;

                    }

                    /* 定义格子样式 */

                    QCalendarWidget QTableView::item {

                        border-radius: 4px;

                        padding: 2px;

                        background-color: transparent;

                        color: #E5E7EB;

                    }

                    QCalendarWidget QTableView::item:hover {

                        background-color: transparent;

                        color: #E5E7EB;

                        border: 1px solid transparent;

                    }

                    QCalendarWidget QTableView::item:today {

                        background-color: transparent;

                        color: #E5E7EB;

                        border: 1px solid transparent;

                    }

                    QCalendarWidget QTableView::item:selected {

                        background-color: transparent;

                        color: #E5E7EB;

                        border: 1px solid transparent;

                        font-weight: normal;

                    }

                    QCalendarWidget QTableView::item:today:selected {

                        background-color: transparent;

                        color: #E5E7EB;

                        border: 1px solid transparent;

                        font-weight: normal;

                    }

                    QCalendarWidget QToolButton {

                        background: transparent;

                        color: #E5E7EB;

                        font-size: 12px;

                    }

                    QCalendarWidget QToolButton:hover {

                        color: #E5E7EB;

                    }

                    QCalendarWidget QSpinBox {

                        background: #2A2A3C;

                        border: 1px solid #4F4F7A;

                        border-radius: 4px;

                        color: #E5E7EB;

                        padding: 2px 6px;

                        font-size: 12px;

                    }

                    QSpinBox {

                        background: #2A2A3C;

                        border: 1px solid #4F4F7A;

                        border-radius: 4px;

                        color: #E5E7EB;

                        padding: 2px 6px;

                        font-size: 12px;

                    }

                    QPushButton {

                        background-color: #3B82F6;

                        color: white;

                        border: none;

                        border-radius: 6px;

                        padding: 4px 10px;

                        font-size: 11px;

                    }

                    QPushButton:hover {

                        background-color: #2563EB;

                    }

                    QPushButton:pressed {

                        background-color: #1D4ED8;

                    }

                    QPushButton[text="取消"] {

                        background-color: #374151;

                    }

                    QPushButton[text="取消"]:hover {

                        background-color: #4B5563;

                    }

                    """



    def _open_datetime_picker(self):

        if getattr(self, "_datetime_dialog_open", False):

            return

        self._datetime_dialog_open = True

        try:

            parent_widget = getattr(self, "container", self)

            if not self.datetime_popup:

                self.datetime_popup = QFrame(parent_widget)

                self.datetime_popup.setObjectName("DateTimePopup")

                popup_layout = QVBoxLayout(self.datetime_popup)

                popup_layout.setContentsMargins(8, 8, 8, 8)

                popup_layout.setSpacing(6)



                self.datetime_calendar = QCalendarWidget(self.datetime_popup)

                self.datetime_calendar.setGridVisible(True)

                self.datetime_calendar.setVerticalHeaderFormat(

                    QCalendarWidget.VerticalHeaderFormat.NoVerticalHeader

                )

                self.datetime_calendar.selectionChanged.connect(

                    self._apply_datetime_calendar_selection_style

                )

                popup_layout.addWidget(self.datetime_calendar)



                time_row = QHBoxLayout()

                time_row.setSpacing(6)

                time_label = QLabel("时间:")

                time_label.setStyleSheet("color: #9CA3AF; font-size: 11px;")

                self.hour_spin = QSpinBox()

                self.hour_spin.setRange(0, 23)

                self.minute_spin = QSpinBox()

                self.minute_spin.setRange(0, 59)

                self.hour_spin.setFixedWidth(56)

                self.minute_spin.setFixedWidth(56)

                time_sep = QLabel(":")

                time_sep.setStyleSheet("color: #9CA3AF; font-size: 11px;")

                time_row.addWidget(time_label)

                time_row.addWidget(self.hour_spin)

                time_row.addWidget(time_sep)

                time_row.addWidget(self.minute_spin)

                time_row.addStretch()

                popup_layout.addLayout(time_row)



                btn_row = QHBoxLayout()

                btn_row.addStretch()

                self.datetime_ok_btn = QPushButton("确定")

                self.datetime_cancel_btn = QPushButton("取消")

                self.datetime_ok_btn.clicked.connect(self._apply_datetime_picker)

                self.datetime_cancel_btn.clicked.connect(self.datetime_popup.hide)

                btn_row.addWidget(self.datetime_cancel_btn)

                btn_row.addWidget(self.datetime_ok_btn)

                popup_layout.addLayout(btn_row)



            # 每次打开都刷新样式，避免弹窗复用时保留旧样式。

            self.datetime_popup.setStyleSheet(self._datetime_popup_stylesheet())



            current_text = self.time_input.text().strip()

            parsed_dt, _ = self._parse_time_text(current_text)

            if parsed_dt:

                self.datetime_calendar.setSelectedDate(

                    QDateTime.fromSecsSinceEpoch(

                        int(parsed_dt.timestamp()),

                        Qt.TimeSpec.LocalTime,

                    ).date()

                )

                self.hour_spin.setValue(parsed_dt.hour)

                self.minute_spin.setValue(parsed_dt.minute)

            else:

                now = QDateTime.currentDateTime()

                self.datetime_calendar.setSelectedDate(now.date())

                self.hour_spin.setValue(now.time().hour())

                self.minute_spin.setValue(now.time().minute())

            self._apply_datetime_calendar_selection_style()



            self.datetime_popup.adjustSize()

            pos = self.time_input.mapTo(

                parent_widget, QPoint(0, self.time_input.height() + 6)

            )

            popup_size = self.datetime_popup.sizeHint()

            if pos.x() + popup_size.width() > parent_widget.width():

                pos.setX(max(0, parent_widget.width() - popup_size.width() - 8))

            if pos.y() + popup_size.height() > parent_widget.height():

                pos.setY(max(0, parent_widget.height() - popup_size.height() - 8))

            self.datetime_popup.move(pos)

            self.datetime_popup.show()

            self.datetime_popup.raise_()

        finally:

            self._datetime_dialog_open = False



    def _apply_datetime_calendar_selection_style(self):

        calendar = getattr(self, "datetime_calendar", None)

        if not calendar:

            return

        selected_date = calendar.selectedDate()

        if not selected_date.isValid():

            return

        prev_date = getattr(self, "_datetime_prev_selected_date", None)

        if prev_date and prev_date.isValid() and prev_date != selected_date:

            calendar.setDateTextFormat(prev_date, QTextCharFormat())



        selected_fmt = QTextCharFormat()

        selected_fmt.setBackground(QColor("#FACC15"))

        selected_fmt.setForeground(QColor("#111827"))

        selected_fmt.setFontWeight(int(QFont.Weight.Black))

        selected_fmt.setFontUnderline(True)

        calendar.setDateTextFormat(selected_date, selected_fmt)

        self._datetime_prev_selected_date = QDate(selected_date)

        calendar.updateCells()



    def _apply_datetime_picker(self):

        if not self.datetime_popup:

            return

        date = self.datetime_calendar.selectedDate()

        hour = self.hour_spin.value()

        minute = self.minute_spin.value()

        from datetime import datetime



        selected = datetime(date.year(), date.month(), date.day(), hour, minute)

        self.time_input.setText(selected.strftime("%Y-%m-%d %H:%M"))

        self.datetime_popup.hide()



    def _is_duplicate_extra_image(self, image_bytes: bytes) -> bool:

        if not image_bytes:

            return False

        for existing_bytes, _ in self.extra_images:

            if existing_bytes == image_bytes:

                return True

        return False



    def _try_paste_extra_image(self):

        if not self.enable_extra_upload:

            return False

        clipboard = QGuiApplication.clipboard()

        mime = clipboard.mimeData()

        if not mime or not mime.hasImage():

            return False

        image = clipboard.image()

        if image.isNull():

            return False

        image_bytes = self._qimage_to_bytes(image)

        if not image_bytes:

            return False

        if self._is_duplicate_extra_image(image_bytes):

            return True

        file_name = f"pasted_{len(self.extra_images) + 1}.png"

        self.extra_images.append((image_bytes, file_name))

        self._update_extra_images_label()

        return True



    def select_extra_images(self):

        if not self.enable_extra_upload:

            return

        self._start_extra_screenshot()



    def _start_extra_screenshot(self):

        """截取现场照片（支持多张）"""

        # 通知父窗口隐藏，避免Z轴冲突

        self.screenshot_started.emit()

        self._suppress_ocr_cancel_on_hide = True

        self.hide()

        QTimer.singleShot(200, self._show_extra_region_selector)



    def _show_extra_region_selector(self):

        self.extra_region_selector = ScreenshotRegionSelector()

        self.extra_region_selector.screenshot_captured.connect(

            self._on_extra_screenshot_captured

        )

        self.extra_region_selector.selection_cancelled.connect(

            self._on_extra_selection_cancelled

        )



    def _on_extra_screenshot_captured(self, pil_image):

        try:

            buffer = io.BytesIO()

            pil_image.save(buffer, format="PNG")

            image_bytes = buffer.getvalue()

            buffer.close()

            if image_bytes:

                file_name = f"extra_{len(self.extra_images) + 1}.png"

                self.extra_images.append((image_bytes, file_name))

                self._update_extra_images_label()

        except Exception:

            pass



        self.screenshot_finished.emit()

        self.show()

        self.activateWindow()

        self.raise_()

        self._suppress_ocr_cancel_on_hide = False



    def _on_extra_selection_cancelled(self):

        self.screenshot_finished.emit()

        self.show()

        self.activateWindow()

        self.raise_()

        self._suppress_ocr_cancel_on_hide = False



    def start_screenshot(self):

        # 通知父窗口隐藏，避免Z轴冲突

        self.screenshot_started.emit()

        self._suppress_ocr_cancel_on_hide = True

        self.hide()

        QTimer.singleShot(200, self._show_region_selector)



    def _show_region_selector(self):

        self.region_selector = ScreenshotRegionSelector()

        self.region_selector.screenshot_captured.connect(self._on_screenshot_captured)

        self.region_selector.selection_cancelled.connect(self._on_selection_cancelled)



    def _on_screenshot_captured(self, pil_image):

        try:

            buffer = io.BytesIO()

            pil_image.save(buffer, format="JPEG", quality=85)

            self.screenshot_bytes = buffer.getvalue()

            buffer.close()

            self.screenshot_image = pil_image

            self._show_preview(pil_image)

            # 先禁用确认按钮，等OCR完成或用户手动输入时间后再启用

            self.btn_confirm.setEnabled(False)

            self.hint_label.setText(

                f"截图成功！尺寸: {pil_image.width}x{pil_image.height}"

            )

            # 显示OCR识别中状态

            self.ocr_status_label.setText("🔄 识别发送时间中...")

            self.ocr_status_label.setStyleSheet("color: #F59E0B; font-size: 12px;")

            # 执行OCR识别

            self.perform_ocr(pil_image)

        except Exception as e:

            self.hint_label.setText(f"截图失败: {e}")

        # 先通知主界面显示，再显示截图预览（确保预览在主界面上方）

        self.screenshot_finished.emit()

        self.show()

        self.activateWindow()

        self.raise_()

        self._suppress_ocr_cancel_on_hide = False



    def _on_selection_cancelled(self):

        self.hint_label.setText("截图已取消。")

        # 先通知主界面显示，再显示截图预览（确保预览在主界面上方）

        self.screenshot_finished.emit()

        self.show()

        self.activateWindow()

        self.activateWindow()

        self.raise_()

        self._suppress_ocr_cancel_on_hide = False



    def perform_ocr(self, pil_image):

        """执行OCR识别（使用 Windows 内置 OCR 引擎，在后台线程中运行）"""

        import asyncio

        import threading

        import weakref



        # 初始化响应时间

        self.ocr_response_time = None

        self._ocr_cancelled = False

        self._ocr_vote_candidates = []

        self._ocr_vote_summary = {}



        try:

            _width, height = pil_image.size



            print("开始后台OCR识别(WinOCR)...")



            obj_ref = weakref.ref(self)



            _ocr_lang_cache = {}



            def _get_ocr_lang():

                """检测可用的 OCR 语言，优先中文，回退英文。"""

                if "lang" in _ocr_lang_cache:

                    return _ocr_lang_cache["lang"]

                from winocr import recognize_pil as _rp

                from PIL import Image as _Img



                test_img = _Img.new("RGB", (60, 20), "white")

                loop = asyncio.new_event_loop()

                try:

                    for lang in ("zh-Hans-CN", "en-US"):

                        try:

                            loop.run_until_complete(_rp(test_img, lang))

                            _ocr_lang_cache["lang"] = lang

                            print(f"WinOCR 语言: {lang}")

                            return lang

                        except Exception:

                            continue

                finally:

                    loop.close()

                _ocr_lang_cache["lang"] = "en-US"

                return "en-US"



            def _run_ocr_on_pil(pil_img):

                """使用 Windows 内置 OCR 识别 PIL Image，返回与旧格式兼容的 ocr_lines。"""

                from winocr import recognize_pil as _recognize_pil



                lang = _get_ocr_lang()

                loop = asyncio.new_event_loop()

                try:

                    ocr_result = loop.run_until_complete(

                        _recognize_pil(pil_img.convert("RGB"), lang)

                    )

                finally:

                    loop.close()

                records = []

                for line in ocr_result.lines:

                    text = str(line.text or "").strip()

                    if not text:

                        continue

                    # 计算行的 y_center（取首个 word 的 bounding_rect）

                    y_center = None

                    for w in line.words:

                        r = w.bounding_rect

                        y_center = float(r.y) + float(r.height) / 2.0

                        break

                    records.append({"text": text, "conf": 0.85, "y_center": y_center})

                return records



            def _run_ocr_phase(

                pil_img,

                phase_name: str,

                phase_height: int,

                source_zone: str = "",

                variant_id: str = "",

                stage_name: str = "",

            ):

                try:

                    ocr_lines = _run_ocr_on_pil(pil_img)

                except Exception as exc:

                    print(f"OCR 识别出错[{phase_name}]: {exc}")

                    return [], [], None, {"phase": phase_name, "error": 1}



                print("-" * 30)

                print(f"OCR 识别结果[{phase_name}]：")

                if ocr_lines:

                    for one in ocr_lines:

                        try:

                            conf = float(one.get("conf") or 0.0)

                        except Exception:

                            conf = 0.0

                        text = str(one.get("text") or "")

                        print(f"[{conf:.2f}] {text}")

                else:

                    print("未识别到文本")

                print("-" * 30)



                obj = obj_ref()

                if not obj or sip.isdeleted(obj):

                    return ocr_lines, [], None, {"phase": phase_name, "skipped": 1}



                candidates, stats = obj._collect_response_time_candidates(

                    ocr_lines=ocr_lines,

                    image_height=phase_height,

                    phase=phase_name,

                    source_zone=source_zone,

                    variant_id=variant_id,

                )

                best = obj._select_best_response_time_candidate(candidates)



                stat_text = ", ".join(f"{k}={v}" for k, v in stats.items())

                filtered_summary = ", ".join(

                    f"{k}={v}"

                    for k, v in stats.items()

                    if (

                        (str(k).startswith("skip_") or str(k).startswith("penalty_"))

                        and int(v or 0) > 0

                    )

                )

                best_summary = "none"

                if best:

                    best_summary = "{time}|score={score:.2f}|conf={conf:.2f}".format(

                        time=best.get("time") or "",

                        score=float(best.get("score") or 0.0),

                        conf=float(best.get("confidence") or 0.0),

                    )

                    print(

                        "OCR候选命中[{phase}] time={time} score={score:.2f} "

                        "conf={conf:.2f} y={y:.3f} text={text}".format(

                            phase=phase_name,

                            time=best.get("time"),

                            score=float(best.get("score") or 0.0),

                            conf=float(best.get("confidence") or 0.0),

                            y=float(best.get("y_ratio") or 0.0),

                            text=best.get("text") or "",

                        )

                    )

                else:

                    print(f"OCR候选命中[{phase_name}] none")

                print(

                    "OCR阶段[{phase}] stage={stage} variant={variant} "

                    "ocr_lines_count={line_count} candidates_count={cand_count} "

                    "best_candidate={best} filtered_reason_summary={filtered}".format(

                        phase=phase_name,

                        stage=stage_name or source_zone or phase_name,

                        variant=variant_id or phase_name,

                        line_count=len(ocr_lines or []),

                        cand_count=len(candidates or []),

                        best=best_summary,

                        filtered=filtered_summary or "none",

                    )

                )

                print(f"OCR候选统计[{phase_name}] {stat_text}")



                return ocr_lines, candidates, best, stats



            def run_ocr():

                try:

                    obj = obj_ref()

                    if not obj or sip.isdeleted(obj):

                        return



                    selected_best = None

                    selected_lines = []

                    stage_logs = []

                    stage_choice_candidates = []

                    final_failure_reason = ""



                    def _merge_choice_candidates(options):

                        for item in options or []:

                            text = str(item or "").strip()

                            if text and text not in stage_choice_candidates:

                                stage_choice_candidates.append(text)



                    def _run_stage(

                        stage_name: str, roi_entries: list[tuple[str, object]]

                    ):

                        per_variant_best = []

                        stage_variants = {}

                        for roi_label, roi_image in roi_entries or []:

                            variant_images = obj._build_header_ocr_variants(roi_image)

                            if not variant_images:

                                continue



                            def _run_variant(vid: str):

                                variant_image = next(

                                    (

                                        image

                                        for variant_name, image in variant_images

                                        if variant_name == vid

                                    ),

                                    None,

                                )

                                if variant_image is None:

                                    return None, None

                                variant_key = (

                                    vid

                                    if roi_label == stage_name

                                    else f"{roi_label}_{vid}"

                                )

                                ocr_lines, _, best, _ = _run_ocr_phase(

                                    variant_image,

                                    phase_name=variant_key,

                                    phase_height=variant_image.height,

                                    source_zone=stage_name,

                                    variant_id=variant_key,

                                    stage_name=stage_name,

                                )

                                stage_variants[variant_key] = (best or {}).get(

                                    "time", ""

                                )

                                if best:

                                    per_variant_best.append(

                                        {

                                            "variant_id": variant_key,

                                            "best_candidate": best,

                                            "ocr_lines": ocr_lines,

                                        }

                                    )

                                return ocr_lines, best



                            # 遍历所有变体（v1, v2, v3, v4, ...），连续两个一致时跳过后续

                            prev_best = None

                            for vid, _ in variant_images:

                                v_lines, v_best = _run_variant(vid)



                                # v1 无 OCR 文本 → 该区域无文字，跳过所有变体

                                if vid == "v1" and not v_lines:

                                    break



                                # 连续两个变体的最佳候选一致 → 跳过后续变体

                                if (

                                    isinstance(prev_best, dict)

                                    and isinstance(v_best, dict)

                                    and prev_best.get("time")

                                    and prev_best.get("time") == v_best.get("time")

                                ):

                                    break

                                prev_best = v_best



                            # 跨 ROI 早期终止：已有 ≥ 2 票一致，无需后续窗口

                            interim_vote = obj._aggregate_ocr_time_votes(

                                per_variant_best

                            )

                            interim_time, interim_cert, _ = obj._decide_voted_time(

                                interim_vote

                            )

                            if interim_time and interim_cert in ("high", "medium"):

                                print(

                                    f"OCR早期终止: stage={stage_name} "

                                    f"winner={interim_time} certainty={interim_cert} "

                                    f"after_roi={roi_label}"

                                )

                                break



                        vote_result = obj._aggregate_ocr_time_votes(per_variant_best)

                        chosen_time, certainty, top_two = obj._decide_voted_time(

                            vote_result

                        )

                        stage_log = {

                            "stage": stage_name,

                            "winner": chosen_time,

                            "certainty": certainty,

                            "variants": stage_variants,

                            "top_two": top_two,

                        }



                        if chosen_time:

                            winner = None

                            for item in vote_result.get("ordered", []):

                                if str(item.get("time") or "") == chosen_time:

                                    winner = item

                                    break

                            if winner:

                                print(

                                    "OCR投票结果: stage={stage} winner={w} votes={v} certainty={c}".format(

                                        stage=stage_name,

                                        w=chosen_time,

                                        v=int(winner.get("votes") or 0),

                                        c=certainty,

                                    )

                                )

                                return (

                                    winner.get("best_candidate"),

                                    list(winner.get("ocr_lines") or []),

                                    stage_log,

                                    [],

                                    "",

                                )



                        ordered = list(vote_result.get("ordered") or [])

                        if not ordered:

                            failure_reason = f"{stage_name}_no_candidate"

                            print(f"OCR阶段失败: {failure_reason}")

                            return None, [], stage_log, [], failure_reason



                        prompt_candidates = (

                            obj._build_prompt_candidates_from_vote_result(vote_result)

                        )

                        if len(ordered) == 1:

                            failure_reason = "only_weak_candidate"

                            print(

                                "OCR阶段失败: {reason} stage={stage} candidate={candidate}".format(

                                    reason=failure_reason,

                                    stage=stage_name,

                                    candidate=prompt_candidates[:1] or top_two[:1],

                                )

                            )

                            return None, [], stage_log, [], failure_reason



                        failure_reason = "no_consensus"

                        print(

                            "OCR无众数: stage={stage} no_consensus top2={top2}".format(

                                stage=stage_name,

                                top2=prompt_candidates or top_two,

                            )

                        )

                        return None, [], stage_log, prompt_candidates, failure_reason



                    top30_band = obj._extract_top_30_percent_band(pil_image)

                    if top30_band is not None:

                        stage_best, stage_lines, stage_log, choices, failure_reason = (

                            _run_stage("top30_full", [("top30_full", top30_band)])

                        )

                        stage_logs.append(stage_log)

                        _merge_choice_candidates(choices)

                        if stage_best:

                            selected_best = stage_best

                            selected_lines = stage_lines

                            obj._ocr_vote_summary = {

                                "winner": (selected_best or {}).get("time", ""),

                                "certainty": stage_log.get("certainty", ""),

                                "stage_logs": stage_logs,

                                "failure_reason": "",

                            }

                        else:

                            final_failure_reason = (

                                failure_reason or final_failure_reason

                            )



                    if not selected_best:

                        top30_windows = obj._extract_top_30_percent_scan_windows(

                            pil_image

                        )

                        if top30_windows:

                            (

                                stage_best,

                                stage_lines,

                                stage_log,

                                choices,

                                failure_reason,

                            ) = _run_stage("top30_windows", top30_windows)

                            stage_logs.append(stage_log)

                            _merge_choice_candidates(choices)

                            if stage_best:

                                selected_best = stage_best

                                selected_lines = stage_lines

                                obj._ocr_vote_summary = {

                                    "winner": (selected_best or {}).get("time", ""),

                                    "certainty": stage_log.get("certainty", ""),

                                    "stage_logs": stage_logs,

                                    "failure_reason": "",

                                }

                            elif failure_reason == "only_weak_candidate":

                                final_failure_reason = failure_reason

                            elif (

                                not final_failure_reason

                                or final_failure_reason == "top30_full_no_candidate"

                            ):

                                final_failure_reason = (

                                    failure_reason or final_failure_reason

                                )

                        elif not final_failure_reason:

                            final_failure_reason = "top30_windows_no_candidate"



                    if not selected_best and height < 24:

                        fallback_image = pil_image.convert("RGB")

                        fallback_lines, _, fallback_best, _ = _run_ocr_phase(

                            fallback_image,

                            phase_name="fallback_v1",

                            phase_height=height,

                            source_zone="fallback",

                            variant_id="fallback_v1",

                            stage_name="fallback",

                        )

                        if fallback_best and obj._is_strong_ocr_vote_bucket(

                            {"best_candidate": fallback_best}

                        ):

                            selected_best = fallback_best

                            selected_lines = fallback_lines

                            stage_logs.append(

                                {

                                    "stage": "fallback",

                                    "winner": fallback_best.get("time", ""),

                                    "certainty": "medium",

                                    "variants": {

                                        "fallback_v1": fallback_best.get("time", "")

                                    },

                                    "top_two": [fallback_best.get("time", "")],

                                }

                            )

                            obj._ocr_vote_summary = {

                                "winner": (selected_best or {}).get("time", ""),

                                "certainty": "medium",

                                "stage_logs": stage_logs,

                                "failure_reason": "",

                            }



                    if not selected_best:

                        obj._ocr_vote_candidates = stage_choice_candidates[:2]

                        obj._ocr_vote_summary = {

                            "winner": "",

                            "certainty": "low",

                            "stage_logs": stage_logs,

                            "failure_reason": final_failure_reason

                            or "top30_windows_no_candidate",

                        }



                    parsed_dt = obj._parse_ocr_datetime(selected_best, selected_lines)

                    if parsed_dt:

                        obj.ocr_response_time = parsed_dt

                        print(f"✅ 提取到事件响应时间: {obj.ocr_response_time}")

                    else:

                        print("⚠️ 未命中有效响应时间候选")

                except Exception as e:

                    print(f"OCR 识别出错: {e}")

                finally:

                    # 在主线程中更新UI

                    obj = obj_ref()

                    if (

                        not obj

                        or sip.isdeleted(obj)

                        or getattr(obj, "_ocr_cancelled", False)

                    ):

                        return

                    try:

                        QMetaObject.invokeMethod(

                            obj,

                            "_on_ocr_completed",

                            Qt.ConnectionType.QueuedConnection,

                        )

                    except Exception:

                        return



            # daemon线程：主程序退出时自动终止

            self._ocr_thread = threading.Thread(target=run_ocr, daemon=True)

            self._ocr_thread.start()



        except Exception as e:

            print(f"OCR 启动出错: {e}")



    def _is_valid_time(self, time_str):

        """验证时间格式是否有效（支持 YYYY-MM-DD HH:MM / HH:MM）"""

        parsed_dt, _ = self._parse_time_text(time_str)

        return bool(parsed_dt)



    @pyqtSlot()

    def _on_ocr_completed(self):

        """OCR识别完成后的回调处理"""

        self._ocr_completed = True

        if self._ocr_vote_summary:

            summary_parts = [

                f"winner={self._ocr_vote_summary.get('winner', '')}",

                f"certainty={self._ocr_vote_summary.get('certainty', '')}",

            ]

            failure_reason = str(

                self._ocr_vote_summary.get("failure_reason") or ""

            ).strip()

            if failure_reason:

                summary_parts.append(f"failure_reason={failure_reason}")

            for stage_log in list(self._ocr_vote_summary.get("stage_logs") or []):

                stage_name = str(stage_log.get("stage") or "").strip()

                variants = stage_log.get("variants") or {}

                if stage_name:

                    summary_parts.append(

                        "{stage}={variants}".format(

                            stage=stage_name,

                            variants=",".join(

                                f"{key}:{value}" for key, value in variants.items()

                            )

                            or "none",

                        )

                    )

            print("ocr_vote: " + " ".join(summary_parts))



        if not self.ocr_response_time and self._ocr_vote_candidates:

            chosen = self._prompt_response_time_choice(self._ocr_vote_candidates)

            if chosen:

                parsed_dt, normalized_full = self._parse_time_text(chosen)

                self.ocr_response_time = (

                    normalized_full if parsed_dt and normalized_full else chosen

                )



        if self.ocr_response_time:

            self.time_input.blockSignals(True)

            self.time_input.setText(self.ocr_response_time)

            self.time_input.blockSignals(False)

            self.ocr_status_label.setText(f"✅ 已识别: {self.ocr_response_time}")

            self.ocr_status_label.setStyleSheet("color: #10B981; font-size: 12px;")

        else:

            failure_reason = str(

                self._ocr_vote_summary.get("failure_reason") or ""

            ).strip()

            if self._ocr_vote_candidates:

                self.ocr_status_label.setText("⚠️ 识别结果不一致，请手动输入")

            elif failure_reason == "only_weak_candidate":

                self.ocr_status_label.setText("⚠️ 识别不稳定，请手动输入")

            else:

                self.ocr_status_label.setText("⚠️ 未识别到时间，请手动输入")

            self.ocr_status_label.setStyleSheet("color: #EF4444; font-size: 12px;")



        self._refresh_submit_state()



    def _on_time_input_changed(self, text):

        """用户手动输入时间时的回调"""

        normalized = self._normalize_time_text(text)

        if normalized != text:

            self.time_input.blockSignals(True)

            self.time_input.setText(normalized)

            self.time_input.blockSignals(False)

            text = normalized



        parsed_dt, normalized_full = self._parse_time_text(text)

        if parsed_dt:

            if normalized_full and normalized_full != text:

                self.time_input.blockSignals(True)

                self.time_input.setText(normalized_full)

                self.time_input.blockSignals(False)

                text = normalized_full

            self.ocr_response_time = normalized_full or text.strip()

            self.ocr_status_label.setText(f"✅ 时间有效: {text}")

            self.ocr_status_label.setStyleSheet("color: #10B981; font-size: 12px;")

        else:

            self.ocr_response_time = None

            if text:

                self.ocr_status_label.setText(

                    "⚠️ 时间格式无效 (需要 HH:MM 或 YYYY-MM-DD HH:MM)"

                )

                self.ocr_status_label.setStyleSheet("color: #EF4444; font-size: 12px;")



        self._refresh_submit_state()

        self._notify_state_changed()



    def _on_building_selected(self, index):

        """楼栋选择事件处理"""

        if index == 0:  # 选中了"请选择楼栋..."

            return



        current_normalized = set(

            self._normalize_buildings_value(self.selected_buildings)

        )

        self.selected_buildings = current_normalized

        building = normalize_building_name(self.building_combo.itemText(index))

        if not building:

            return

        if self.enable_building_multi_select:

            if building in self.selected_buildings:

                # 取消选择

                self.selected_buildings.remove(building)

            else:

                # 添加选择

                self.selected_buildings.add(building)

        else:

            self.selected_buildings = {building}



        # 更新显示

        if self.selected_buildings:

            self.selected_buildings_label.setText(

                ", ".join(sorted(self.selected_buildings))

            )

            self.selected_buildings_label.setStyleSheet(

                "color: #10B981; font-size: 11px;"

            )

        else:

            self.selected_buildings_label.setText("未选择")

            self.selected_buildings_label.setStyleSheet(

                "color: #EF4444; font-size: 11px;"

            )



        # 多选时重置下拉框到默认项，单选保留当前值

        if self.enable_building_multi_select:

            self.building_combo.setCurrentIndex(0)



        self._update_data_dict_field(

            "buildings", sorted(self.selected_buildings), remove_when_empty=False

        )

        self._patch_cache_fields({"buildings": sorted(self.selected_buildings)})

        self._refresh_submit_state()

        self._notify_state_changed()



    def _on_specialty_selected(self, index):

        """专业选择事件处理（单选）"""

        if index == 0:

            self.selected_specialty = ""

            self.specialty_selected_label.setText("未选择")

            self.specialty_selected_label.setStyleSheet(

                "color: #EF4444; font-size: 11px;"

            )

            self._update_data_dict_field("specialty", "", remove_when_empty=True)

            self._patch_cache_fields({"specialty": None})

            self._notify_state_changed()

            return



        self.selected_specialty = self.specialty_combo.itemText(index)

        self.specialty_selected_label.setText(self.selected_specialty)

        self.specialty_selected_label.setStyleSheet("color: #10B981; font-size: 11px;")

        self._update_data_dict_field(

            "specialty", self.selected_specialty, remove_when_empty=True

        )

        self._patch_cache_fields({"specialty": self.selected_specialty or None})

        self._notify_state_changed()



    def _on_change_level_selected(self, index):
        """变更等级选择事件处理（单选）"""
        if index < 0:
            return
        self.selected_change_level = self.change_level_combo.itemText(index)
        self.change_level_selected_label.setText(self.selected_change_level)

        self.change_level_selected_label.setStyleSheet(

            "color: #10B981; font-size: 11px;"

        )

        self._update_data_dict_field(
            "level", self.selected_change_level, remove_when_empty=True
        )
        self._lock_level_selection()
        self._patch_cache_fields(
            {
                "level": self.selected_change_level or None,
                "level_locked": True,
            }
        )
        self._notify_state_changed()


    def _show_preview(self, pil_image):

        try:

            buffer = io.BytesIO()

            pil_image.save(buffer, format="PNG")

            buffer.seek(0)

            pixmap = QPixmap()

            pixmap.loadFromData(buffer.getvalue())

            scaled_pixmap = pixmap.scaled(

                self.preview_label.width() - 10,

                self.preview_label.height() - 10,

                Qt.AspectRatioMode.KeepAspectRatio,

                Qt.TransformationMode.SmoothTransformation,

            )

            self.preview_label.setPixmap(scaled_pixmap)

            self.preview_label.setText("")

        except Exception as e:

            pass



    def skip_screenshot(self):

        self._suppress_ocr_cancel_on_hide = False

        self.screenshot_bytes = None

        self._remember_current_state()

        # 跳过截图时也需要传递buildings

        buildings = list(self.selected_buildings) if self.selected_buildings else []

        extra_images = list(self.extra_images)

        change_level = (

            self.selected_change_level if self.enable_change_level_select else ""

        )

        event_level = (

            self.selected_event_level if self.enable_event_level_select else ""

        )

        event_source = (

            self.selected_event_source if self.enable_event_source_select else ""

        )

        response_time = self.time_input.text().strip() or ""

        self.upload_confirmed.emit(

            self.data_dict,

            None,

            self.action_type,

            response_time,

            buildings,

            extra_images,

            self.selected_specialty,

            change_level,

            event_level,

            event_source,

            self.recover_selected if self.enable_recover_select else False,

        )

        self._reset_recover_selection()

        self.hide()



    def confirm_upload(self):

        self._suppress_ocr_cancel_on_hide = False

        self._remember_current_state()

        # 使用时间输入框的值（已验证有效性）

        response_time = self.time_input.text().strip() or ""

        # 楼栋列表

        buildings = list(self.selected_buildings) if self.selected_buildings else []



        extra_images = list(self.extra_images)

        change_level = (

            self.selected_change_level if self.enable_change_level_select else ""

        )

        event_level = (

            self.selected_event_level if self.enable_event_level_select else ""

        )

        event_source = (

            self.selected_event_source if self.enable_event_source_select else ""

        )

        self.upload_confirmed.emit(

            self.data_dict,

            self.screenshot_bytes,

            self.action_type,

            response_time,

            buildings,

            extra_images,

            self.selected_specialty,

            change_level,

            event_level,

            event_source,

            self.recover_selected if self.enable_recover_select else False,

        )

        self._reset_recover_selection()

        self.hide()



    def _remember_current_state(self):

        patch = {}

        buildings = sorted(self.selected_buildings) if self.selected_buildings else []

        self._update_data_dict_field("buildings", buildings, remove_when_empty=False)

        patch["buildings"] = buildings



        if self.enable_specialty_select:

            self._update_data_dict_field(

                "specialty", self.selected_specialty, remove_when_empty=True

            )

            patch["specialty"] = self.selected_specialty or None



        selected_level = ""
        if self.enable_change_level_select:
            selected_level = self.selected_change_level
        elif self.enable_event_level_select:
            selected_level = self.selected_event_level
        if self.enable_change_level_select or self.enable_event_level_select:
            self._update_data_dict_field(
                "level", selected_level, remove_when_empty=True
            )
            patch["level"] = selected_level or None
            if self._is_level_locked():
                self._update_data_dict_field(
                    "level_locked", True, remove_when_empty=False
                )
                patch["level_locked"] = True

        if self.enable_event_source_select:
            self._update_data_dict_field(
                "event_source", self.selected_event_source, remove_when_empty=True
            )
            patch["event_source"] = self.selected_event_source or None



        if self.enable_recover_select:

            transfer_to_overhaul = bool(self.transfer_to_overhaul_selected)

            self._update_data_dict_field(

                "transfer_to_overhaul",

                transfer_to_overhaul,

                remove_when_empty=False,

            )

            patch["transfer_to_overhaul"] = transfer_to_overhaul



        self._patch_cache_fields(patch)



    def cancel_upload(self):

        self._suppress_ocr_cancel_on_hide = False

        self._ocr_cancelled = True

        self._reset_recover_selection()

        self.hide()

        self.cancelled.emit()



    def keyPressEvent(self, event):

        """禁用ESC键关闭窗口"""

        if self.enable_extra_upload:

            is_paste = event.matches(QKeySequence.StandardKey.Paste)

            is_ctrl_v = (

                event.modifiers() & Qt.KeyboardModifier.ControlModifier

                and event.key() == Qt.Key.Key_V

            )

            if is_paste or is_ctrl_v:

                if self._try_paste_extra_image():

                    event.accept()

                    return



        if event.key() == Qt.Key.Key_Escape:

            event.ignore()

        else:

            super().keyPressEvent(event)



    def eventFilter(self, obj, event):

        if obj is self.time_input and event.type() == QEvent.Type.MouseButtonPress:

            if self.datetime_popup and self.datetime_popup.isVisible():

                self.datetime_popup.hide()

            else:

                self._open_datetime_picker()

            return True

        return super().eventFilter(obj, event)



    def mousePressEvent(self, event):

        if event.button() == Qt.MouseButton.LeftButton:

            self.drag_position = (

                event.globalPosition().toPoint() - self.frameGeometry().topLeft()

            )

            event.accept()



    def mouseMoveEvent(self, event):

        if self.drag_position:

            self.move(event.globalPosition().toPoint() - self.drag_position)

            event.accept()



    def hideEvent(self, event):

        # 标记 OCR 回调取消，避免对象已隐藏/销毁后回调

        if not self._suppress_ocr_cancel_on_hide:

            self._ocr_cancelled = True

        super().hideEvent(event)



    def mouseReleaseEvent(self, event):

        self.drag_position = None





class DiffDialog(QDialog):

    replace_confirmed = pyqtSignal(dict, str, bool)

    cancelled = pyqtSignal()



    def __init__(self, parent=None, theme="dark"):

        super().__init__(parent)

        self.theme = theme

        self.setObjectName("DiffWindow")

        self.setWindowFlags(

            Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint

        )

        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

        self.resize(600, 450)

        self.new_content_cache = ""

        self.target_record_id = ""

        self.target_old_data = None

        self.upload_old_first = False



        layout = QVBoxLayout(self)

        layout.setContentsMargins(5, 5, 5, 5)

        self.container = QFrame()

        self.container.setObjectName("DiffWindow")

        inner_layout = QVBoxLayout(self.container)



        top_bar = QHBoxLayout()

        title = QLabel("发现事件更新 - 请选择要替换的项目")

        title.setObjectName("TitleLabel")

        close_btn = QPushButton("×")

        close_btn.setFixedSize(30, 30)

        close_btn.setStyleSheet("border:none; font-size:20px; color:#999;")

        close_btn.clicked.connect(self.on_cancel)

        top_bar.addWidget(title)

        top_bar.addStretch()

        top_bar.addWidget(close_btn)



        hint = QLabel("提示：点击主界面的其他列表项，可切换右侧的对比目标。")

        hint.setStyleSheet("color: #FF9500; font-size: 12px;")



        diff_area = QHBoxLayout()

        left_layout = QVBoxLayout()

        left_label = QLabel("新内容 (可直接修改)")

        left_label.setObjectName("DiffHeader")

        self.left_text = QTextEdit()

        self.left_text.setStyleSheet(

            "background-color: #1E3A2F; border: 1px solid #22C55E; color: #E4E4E7;"

        )

        left_layout.addWidget(left_label)

        left_layout.addWidget(self.left_text)



        right_layout = QVBoxLayout()

        right_label = QLabel("将被替换的内容 (选中项)")

        right_label.setObjectName("DiffHeader")

        self.right_text = QTextEdit()

        self.right_text.setReadOnly(True)

        self.right_text.setStyleSheet(

            "background-color: #3D2A2A; border: 1px solid #EF4444; color: #E4E4E7;"

        )

        right_layout.addWidget(right_label)

        right_layout.addWidget(self.right_text)



        diff_area.addLayout(left_layout)

        diff_area.addLayout(right_layout)



        btn_layout = QHBoxLayout()

        self.btn_confirm = QPushButton("替换并更新")

        self.btn_confirm.setObjectName("ConfirmBtn")

        self.btn_confirm.setCursor(Qt.CursorShape.PointingHandCursor)

        self.btn_confirm.clicked.connect(self.on_confirm)

        btn_cancel = QPushButton("取消/忽略")

        btn_cancel.setObjectName("DiffCancelBtn")

        btn_cancel.setCursor(Qt.CursorShape.PointingHandCursor)

        btn_cancel.clicked.connect(self.on_cancel)



        btn_layout.addStretch()

        btn_layout.addWidget(btn_cancel)

        btn_layout.addWidget(self.btn_confirm)



        inner_layout.addLayout(top_bar)

        inner_layout.addWidget(hint)

        inner_layout.addLayout(diff_area)

        inner_layout.addLayout(btn_layout)

        layout.addWidget(self.container)

        self.setStyleSheet(get_stylesheet(self.theme))

        self.drag_position = None



    def init_diff(self, new_text, old_data, record_id, is_pending_upload=False):

        self.new_content_cache = new_text

        self.left_text.setPlainText(new_text)

        self.update_target(old_data, record_id, is_pending_upload)



    def update_target(self, old_data, record_id, is_pending_upload=False):

        self.target_old_data = old_data

        self.target_record_id = record_id

        self.upload_old_first = is_pending_upload



        if old_data:

            self.right_text.setPlainText(old_data["text"])

            self.btn_confirm.setEnabled(True)

            self.btn_confirm.setText(

                "上传并替换" if is_pending_upload else "替换并更新"

            )

            self.btn_confirm.setStyleSheet("")

        else:

            self.right_text.setPlainText("未选择任何项...")

            self.btn_confirm.setEnabled(False)

            self.btn_confirm.setText("请选择一项")

            self.btn_confirm.setStyleSheet("background-color: #ccc;")



    def on_confirm(self):

        if self.target_old_data:

            final_content = self.left_text.toPlainText().strip()

            new_data = self.target_old_data.copy()

            new_data["text"] = final_content

            self.replace_confirmed.emit(

                new_data, self.target_record_id, self.upload_old_first

            )

            self.hide()



    def on_cancel(self):

        self.hide()

        self.cancelled.emit()



    def mousePressEvent(self, event):

        if event.button() == Qt.MouseButton.LeftButton:

            self.drag_position = (

                event.globalPosition().toPoint() - self.frameGeometry().topLeft()

            )

            event.accept()



    def mouseMoveEvent(self, event):

        if event.buttons() == Qt.MouseButton.LeftButton and self.drag_position:

            self.move(event.globalPosition().toPoint() - self.drag_position)

            event.accept()





class DetailDialog(QDialog):

    record_id_bind_requested = pyqtSignal(str)

    content_changed = pyqtSignal(str, dict)



    def __init__(self, parent=None, theme="dark"):

        super().__init__(parent)

        self.theme = theme

        self.setObjectName("DetailWindow")

        self.setWindowFlags(

            Qt.WindowType.FramelessWindowHint

            | Qt.WindowType.Tool

            | Qt.WindowType.WindowStaysOnTopHint

        )

        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

        self.resize(420, 500)

        self.current_record_id = ""

        self.current_data = {}

        self.ignore_focus_loss = False

        self.original_text = ""



        layout = QVBoxLayout(self)

        layout.setContentsMargins(5, 5, 5, 5)

        self.container = QFrame()

        self.container.setObjectName("DetailWindow")

        inner_layout = QVBoxLayout(self.container)



        top_bar = QHBoxLayout()

        title = QLabel("详情预览 (可编辑)")

        title.setObjectName("TitleLabel")

        close_btn = QPushButton("×")

        close_btn.setFixedSize(30, 30)

        close_btn.setStyleSheet("border:none;font-size:20px;color:#999;")

        close_btn.clicked.connect(self.hide)

        top_bar.addWidget(title)

        top_bar.addStretch()

        top_bar.addWidget(close_btn)



        self.record_id_container = QWidget()

        record_id_layout = QHBoxLayout(self.record_id_container)

        record_id_layout.setContentsMargins(10, 0, 10, 0)

        self.record_id_label = QLabel()

        self.record_id_label.setObjectName("UUIDLabel")

        self.record_id_label.setContextMenuPolicy(

            Qt.ContextMenuPolicy.CustomContextMenu

        )

        self.record_id_label.customContextMenuRequested.connect(

            self.show_record_id_menu

        )



        self.edit_widget = QWidget()

        self.edit_widget.hide()



        record_id_layout.addWidget(QLabel("ID:"))

        record_id_layout.addWidget(self.record_id_label)

        record_id_layout.addWidget(self.edit_widget)

        record_id_layout.addStretch()

        self.record_binding_status_label = QLabel()

        self.record_binding_status_label.setObjectName("HintLabel")

        self.record_binding_status_label.hide()



        self.text_area = QTextEdit()



        btn_layout = QHBoxLayout()

        self.save_btn = QPushButton("保存修改")

        self.save_btn.setObjectName("ConfirmBtn")

        self.save_btn.clicked.connect(self.save_content)

        self.save_btn.setEnabled(False)



        self.cancel_btn = QPushButton("取消")

        self.cancel_btn.setObjectName("DiffCancelBtn")

        self.cancel_btn.clicked.connect(self.hide)



        btn_layout.addStretch()

        btn_layout.addWidget(self.cancel_btn)

        btn_layout.addWidget(self.save_btn)



        inner_layout.addLayout(top_bar)

        inner_layout.addWidget(self.record_id_container)

        inner_layout.addWidget(self.record_binding_status_label)

        inner_layout.addWidget(self.text_area)

        inner_layout.addLayout(btn_layout)

        layout.addWidget(self.container)

        self.setStyleSheet(get_stylesheet(self.theme))

        self.drag_position = None

        self.text_area.textChanged.connect(self.on_text_changed)



    def on_text_changed(self):

        current_text = self.text_area.toPlainText()

        if current_text != self.original_text:

            self.save_btn.setEnabled(True)

            self.save_btn.setText("保存修改 *")

        else:

            self.save_btn.setEnabled(False)

            self.save_btn.setText("保存修改")



    def save_content(self):

        if not self.current_record_id:

            return

        new_text = self.text_area.toPlainText().strip()

        if not new_text:

            return

        new_data = {"text": new_text}

        self.content_changed.emit(self.current_record_id, new_data)

        self.original_text = new_text

        self.save_btn.setEnabled(False)

        self.save_btn.setText("已保存 ✓")

        QTimer.singleShot(1500, lambda: self.save_btn.setText("保存修改"))



    def update_content(self, data, record_id, editable=True):

        self.current_data = dict(data or {})

        self.current_record_id = record_id

        self.original_text = data["text"]

        self.text_area.setPlainText(data["text"])

        self.text_area.moveCursor(self.text_area.textCursor().MoveOperation.Start)

        self.record_id_label.setText(data.get("record_id", ""))

        self.record_id_label.show()

        self.edit_widget.hide()

        binding_state = str(data.get("record_binding_state") or "").strip().lower()

        if binding_state == "conflicted":

            self.record_binding_status_label.setText("Record ID 冲突")

            self.record_binding_status_label.show()

        elif bool(data.get("_is_placeholder_record")):

            self.record_binding_status_label.setText("临时 Record ID")

            self.record_binding_status_label.show()

        else:

            self.record_binding_status_label.clear()

            self.record_binding_status_label.hide()

        self.text_area.setReadOnly(not editable)

        self.save_btn.setVisible(editable)

        self.cancel_btn.setVisible(editable)

        if editable:

            self.save_btn.setEnabled(False)

            self.save_btn.setText("保存修改")



    def show_record_id_menu(self, pos):

        self.ignore_focus_loss = True

        menu = QMenu()

        copy_action = QAction("复制 Record ID", self)

        copy_action.triggered.connect(self._copy_record_id)

        menu.addAction(copy_action)

        if self._supports_safe_record_binding():

            bind_action = QAction("绑定 Record ID", self)

            bind_action.triggered.connect(self._request_bind_record_id)

            menu.addAction(bind_action)

        menu.exec(QCursor.pos())

        self.ignore_focus_loss = False



    def _copy_record_id(self):

        record_id = str(self.current_record_id or self.record_id_label.text() or "").strip()

        if not record_id:

            return

        QGuiApplication.clipboard().setText(record_id)



    def _supports_safe_record_binding(self):

        if not self.current_record_id:

            return False

        data = self.current_data if isinstance(self.current_data, dict) else {}

        return bool(data.get("_is_placeholder_record")) or str(data.get("record_binding_state") or "").strip().lower() == "conflicted"



    def _request_bind_record_id(self):

        if not self.current_record_id:

            return

        self.record_id_bind_requested.emit(self.current_record_id)



    def changeEvent(self, event):

        if event.type() == QEvent.Type.ActivationChange:

            if not self.isActiveWindow() and not self.ignore_focus_loss:

                self.hide()

        super().changeEvent(event)



    def mousePressEvent(self, event):

        if event.button() == Qt.MouseButton.LeftButton:

            self.drag_position = (

                event.globalPosition().toPoint() - self.frameGeometry().topLeft()

            )

            event.accept()



    def mouseMoveEvent(self, event):

        if event.buttons() == Qt.MouseButton.LeftButton and self.drag_position:

            self.move(event.globalPosition().toPoint() - self.drag_position)

            event.accept()


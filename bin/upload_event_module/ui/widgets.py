from datetime import datetime, timedelta

import re
import weakref
from PyQt6.QtWidgets import (

    QWidget,

    QVBoxLayout,

    QLabel,

    QHBoxLayout,

    QPushButton,

    QFrame,

    QMenu,

)

from PyQt6.QtCore import Qt, QTimer, pyqtSignal
from PyQt6 import sip
from PyQt6.QtGui import QAction

from ..core.speech import speech_manager
from ..time_parser import parse_single_datetime, parse_time_only
from .display_state import build_notice_display_snapshot


# from ..utils import extract_hms  # 已迁移到 extract_datetime





from enum import Enum, auto





class ItemState(Enum):

    NORMAL = auto()  # 正常状态

    SLIDING = auto()  # 滑动动画中

    EXPANDED = auto()  # 展开删除区

    COLLAPSING = auto()  # 收回动画中

    DELETING = auto()  # 删除动画中





class SwipeManager:

    """滑动互斥管理器：确保同一时间只有一个条目展开"""



    _current_expanded_item = None



    @classmethod

    def set_expanded(cls, item):

        if cls._current_expanded_item and cls._current_expanded_item != item:

            # 收回上一个展开的条目

            try:

                cls._current_expanded_item.collapse()

            except RuntimeError:

                # 防止组件已被删除

                pass

        cls._current_expanded_item = item



    @classmethod

    def clear(cls, item):

        if cls._current_expanded_item == item:

            cls._current_expanded_item = None





class TimerWidget(QWidget):
    """计时器组件：显示倒计时（仅用于事件通告）"""

    alert_triggered = pyqtSignal()  # 语音提醒触发信号

    def __init__(self, parent=None):
        super().__init__(parent)
        self.start_time = None
        self.base_start_time = None
        self.target_minutes = 5
        self.is_running = False
        self.is_overtime = False
        self.stage = 0  # 0=首次展示,1=第一次更新,2=第二次更新,3=第三次及以后
        self.update_count = 0
        self.event_level = ""
        self.notice_text = ""
        self.building_text = ""
        self.last_update_response_time = None
        self.alerted_milestones = set()

        self.setFixedWidth(110)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setSpacing(2)

        self.timer_label = QLabel("--:--")
        self.timer_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.timer_label.setStyleSheet(
            "font-size: 18px; font-weight: bold; color: #6366F1;"
        )

        self.milestone_label = QLabel("▼ --分钟")
        self.milestone_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.milestone_label.setStyleSheet("font-size: 10px; color: #9CA3AF;")

        self.start_label = QLabel("开始: --:--")
        self.start_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.start_label.setStyleSheet("font-size: 10px; color: #6B7280;")

        layout.addWidget(self.timer_label)
        layout.addWidget(self.milestone_label)
        layout.addWidget(self.start_label)

        self.update_timer = QTimer(self)
        self.update_timer.timeout.connect(self.update_display)

    def parse_and_start(self, text, title, level=None, buildings=None):
        self.notice_text = text or ""
        self.event_level = (level or "").strip() or self._detect_level_from_title(title)
        self.building_text = self._format_buildings(buildings)

        parsed_dt = self._parse_notice_datetime(text)
        now = datetime.now()
        if not parsed_dt:
            parsed_dt = now
        if parsed_dt > now + timedelta(minutes=1):
            parsed_dt = now

        self.base_start_time = parsed_dt
        self.start_time = parsed_dt
        self.target_minutes = 5
        self.stage = 0
        self.update_count = 0
        self.last_update_response_time = None
        self.alerted_milestones = set()

        self._update_labels()
        self.start()

    def update_context(self, text=None, buildings=None, level=None):
        if text:
            self.notice_text = text
        if buildings is not None:
            self.building_text = self._format_buildings(buildings)
        if level:
            self.event_level = level

    @staticmethod
    def _serialize_datetime(value):
        if not value:
            return ""
        if isinstance(value, datetime):
            return value.isoformat(timespec="seconds")
        return str(value).strip()

    @staticmethod
    def _deserialize_datetime(value):
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text)
        except Exception:
            pass
        return parse_single_datetime(text)

    def export_state(self) -> dict:
        return {
            "_timer_stage": int(self.stage or 0),
            "_timer_target_minutes": int(self.target_minutes or 5),
            "_timer_update_count": int(self.update_count or 0),
            "_timer_event_level": str(self.event_level or "").strip(),
            "_timer_base_start_time": self._serialize_datetime(self.base_start_time),
            "_timer_start_time": self._serialize_datetime(self.start_time),
            "_timer_last_update_response_time": self._serialize_datetime(
                self.last_update_response_time
            ),
        }

    def restore_state(self, state: dict | None = None) -> bool:
        if not isinstance(state, dict):
            return False
        stage = int(state.get("_timer_stage") or 0)
        update_count = int(state.get("_timer_update_count") or 0)
        if stage <= 0 and update_count <= 0:
            return False
        base_start_time = self._deserialize_datetime(state.get("_timer_base_start_time"))
        start_time = self._deserialize_datetime(state.get("_timer_start_time"))
        if not base_start_time:
            return False
        self.stage = max(0, stage)
        self.update_count = max(0, update_count)
        self.target_minutes = max(1, int(state.get("_timer_target_minutes") or 5))
        self.base_start_time = base_start_time
        self.start_time = start_time or base_start_time
        self.last_update_response_time = self._deserialize_datetime(
            state.get("_timer_last_update_response_time")
        )
        persisted_level = str(state.get("_timer_event_level") or "").strip()
        if persisted_level:
            self.event_level = persisted_level
        self.alerted_milestones = set()
        self.is_overtime = False
        self._update_labels()
        self.start()
        return True

    def apply_action(self, action: str, response_time: str = ""):
        if not self.base_start_time:
            self.base_start_time = datetime.now()
        if action == "上传":
            # 上传不改变倒计时
            return
        if action == "更新":
            self.update_count += 1
            current_dt = self._parse_response_datetime(response_time)
            if self.update_count == 1:
                self.stage = 1
                self.target_minutes = 10
                self.start_time = self.base_start_time
            elif self.update_count == 2:
                self.stage = 2
                self.target_minutes = self._get_level_minutes()
                self.start_time = self.base_start_time
            else:
                self.stage = 3
                self.target_minutes = self._get_level_minutes()
                if current_dt:
                    self.start_time = current_dt
                else:
                    self.start_time = (
                        self.last_update_response_time or self.base_start_time
                    )

            if current_dt:
                self.last_update_response_time = current_dt
        else:
            return

        self.alerted_milestones = set()
        self.is_overtime = False
        self._update_labels()
        self.start()

    def reset(self, action: str = "更新", response_time: str = ""):
        self.apply_action(action, response_time)

    def start(self):
        self.is_running = True
        self.is_overtime = False
        self.update_display()
        self.update_timer.start(1000)

    def stop(self):
        self.is_running = False
        self.update_timer.stop()

    def update_display(self):
        if not self.start_time:
            return

        elapsed = datetime.now() - self.start_time
        elapsed_seconds = int(elapsed.total_seconds())
        remaining_seconds = self.target_minutes * 60 - elapsed_seconds

        if remaining_seconds >= 0:
            mins = remaining_seconds // 60
            secs = remaining_seconds % 60
            self.timer_label.setText(f"{mins:02d}:{secs:02d}")
            self.milestone_label.setText(f"▼ {self.target_minutes}分钟")
            self.timer_label.setStyleSheet(
                "font-size: 18px; font-weight: bold; color: #6366F1;"
            )
            self.is_overtime = False
            self._check_and_alert(remaining_seconds)
        else:
            overtime_seconds = abs(remaining_seconds)
            mins = overtime_seconds // 60
            if mins > 9999:
                mins = 9999
            secs = overtime_seconds % 60
            self.timer_label.setText(f"+{mins:02d}:{secs:02d}")
            self.milestone_label.setText("已超时")
            self.timer_label.setStyleSheet(
                "font-size: 18px; font-weight: bold; color: #EF4444;"
            )
            self.is_overtime = True

    def _update_labels(self):
        if self.start_time:
            self.start_label.setText(f"开始: {self.start_time.strftime('%H:%M')}")
        else:
            self.start_label.setText("开始: --:--")
        self.milestone_label.setText(f"▼ {self.target_minutes}分钟")

    def _check_and_alert(self, remaining_seconds: int):
        from ..logger import log_info
        from ..services.robot_webhook import send_event_prompt_message

        one_min_key = f"one_min_{self.stage}_{self.target_minutes}"
        if 58 <= remaining_seconds <= 62 and one_min_key not in self.alerted_milestones:
            log_info(
                f"触发1分钟提醒: stage={self.stage}, 目标={self.target_minutes}分钟"
            )
            self._play_alert("同志！该更新通告了！")
            import threading

            threading.Thread(
                target=send_event_prompt_message,
                args=(self.building_text, self.notice_text),
                daemon=True,
            ).start()
            self.alerted_milestones.add(one_min_key)

    def _play_alert(self, message="同志！该发通告了！"):
        self.alert_triggered.emit()
        speech_manager.speak(message)

    def _detect_level_from_title(self, title: str) -> str:
        title_upper = (title or "").upper()
        if "I3" in title_upper:
            return "I3"
        if "I2" in title_upper:
            return "I2"
        if "I1" in title_upper:
            return "I1"
        if "E3" in title_upper:
            return "E3"
        if "E2" in title_upper:
            return "E2"
        if "E1" in title_upper:
            return "E1"
        if "E0" in title_upper:
            return "E0"
        return ""

    def _get_level_minutes(self) -> int:
        digits = re.findall(r"\d", self.event_level or "")
        if digits and digits[-1] == "3":
            return 30
        return 15

    def _format_buildings(self, buildings) -> str:
        if not buildings:
            return ""
        if isinstance(buildings, (list, tuple)):
            return "、".join([str(item) for item in buildings if item])
        return str(buildings)

    def _parse_notice_datetime(self, text: str):
        if not text:
            return None
        return parse_single_datetime(text)

    def _parse_response_datetime(self, response_time: str):
        if not response_time:
            return None
        parsed_dt = parse_single_datetime(response_time)
        if parsed_dt:
            return parsed_dt
        time_parts = parse_time_only(response_time)
        if not time_parts:
            return None
        hour, minute, sec = time_parts
        try:
            now = datetime.now()
            return datetime(now.year, now.month, now.day, hour, minute, sec)
        except ValueError:
            return None




class ClipboardItemWidget(QWidget):
    action_clicked = pyqtSignal(dict, str)
    today_progress_clicked = pyqtSignal(dict, str)

    ended_signal = pyqtSignal(dict)

    delete_requested = pyqtSignal(dict)  # 删除请求信号
    _TIMER_STATE_KEYS = (
        "_timer_stage",
        "_timer_target_minutes",
        "_timer_update_count",
        "_timer_event_level",
        "_timer_base_start_time",
        "_timer_start_time",
        "_timer_last_update_response_time",
    )



    def __init__(self, data_dict, parent=None):
        super().__init__(parent)
        self.data = data_dict
        self.is_updated_status = False
        self.today_in_progress_state = self._coerce_today_progress_state(
            self.data.get("today_in_progress_state")
        )
        self._delete_interaction_enabled = True
        # 跟踪内容是否未上传（新建/更新后但未点上传按钮）
        self.has_unuploaded_changes = True
        # 上传/更新处理中标记
        self.upload_in_progress = False
        # 记录上传时的内容哈希，用于避免覆盖新内容
        self.pending_upload_hash = None


        # 状态管理

        self._state = ItemState.NORMAL

        self._delete_zone_width = 80



        # 动画对象（成员变量防止GC）

        self._slide_anim = None

        self._fade_anim = None



        # 自动收回定时器

        self._auto_collapse_timer = QTimer(self)

        self._auto_collapse_timer.setSingleShot(True)

        self._auto_collapse_timer.timeout.connect(self.collapse)



        self._init_ui()

        self._connect_signals()



    def _init_ui(self):

        # 主布局

        self.main_layout = QHBoxLayout(self)

        self.main_layout.setContentsMargins(0, 0, 0, 0)

        self.main_layout.setSpacing(0)



        # 1. 底层容器（用于显示红色背景，不作为布局容器）

        self.background_frame = QFrame(self)

        self.background_frame.setStyleSheet("""

            QFrame {

                background: qlineargradient(x1:0, y1:0, x2:1, y2:0, 

                                            stop:0 #DC2626, stop:1 #EF4444);

                border-radius: 8px;

            }

        """)



        # 2. 删除按钮容器（位于右侧，固定宽度）

        self.delete_zone = QFrame(self.background_frame)

        self.delete_zone.setFixedWidth(self._delete_zone_width)

        delete_layout = QVBoxLayout(self.delete_zone)

        delete_layout.setContentsMargins(0, 0, 0, 0)



        self.delete_btn = QPushButton("🗑️")

        self.delete_btn.setFixedSize(50, 50)

        self.delete_btn.setCursor(Qt.CursorShape.PointingHandCursor)

        self.delete_btn.setStyleSheet("""

            QPushButton {

                background: transparent;

                color: white;

                font-size: 20px;

                border: none;

            }

            QPushButton:hover {

                background: rgba(255,255,255,0.2);

                border-radius: 25px;

            }

        """)

        self.delete_btn.clicked.connect(self._on_delete_clicked)

        delete_layout.addWidget(self.delete_btn, 0, Qt.AlignmentFlag.AlignCenter)



        # 3. 前景滑动内容容器

        self.slide_container = QFrame(self)

        self.slide_container.setObjectName("SlideContainer")

        self.slide_container.setStyleSheet(

            "QFrame#SlideContainer { background: transparent; }"

        )



        # 内容布局

        content_layout = QHBoxLayout(self.slide_container)

        content_layout.setContentsMargins(0, 0, 0, 0)



        # 实际内容区域

        self.inner_frame = QFrame(self.slide_container)

        self.inner_frame.setObjectName("ItemFrame")

        content_layout.addWidget(self.inner_frame)



        self._setup_inner_content()



        # 添加到主布局：注意层级关系通过 resizeEvent 手动管理 geometry

        # 这里只添加 background_frame 占位，slide_container 覆盖在上面

        self.main_layout.addWidget(self.background_frame)

        self.delete_zone.hide()  # 初始隐藏删除区



    def _setup_inner_content(self):

        """初始化内部内容组件"""

        layout = QHBoxLayout(self.inner_frame)

        layout.setContentsMargins(15, 8, 10, 8)

        layout.setSpacing(10)



        # 文本部分

        text_layout = QVBoxLayout()

        text_layout.setSpacing(2)

        self.update_labels(self.data["text"])

        title_row = QHBoxLayout()
        title_row.setContentsMargins(0, 0, 0, 0)
        title_row.setSpacing(6)

        self.portal_origin_badge = QLabel("★")
        self.portal_origin_badge.setObjectName("PortalOriginBadge")
        self.portal_origin_badge.setFixedSize(18, 18)
        self.portal_origin_badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.portal_origin_badge.setToolTip("来自网页工作台")
        self.portal_origin_badge.setStyleSheet("""
            QLabel#PortalOriginBadge {
                background: #FBBF24;
                color: #111827;
                border: 1px solid #F59E0B;
                border-radius: 9px;
                font-size: 12px;
                font-weight: 800;
            }
        """)
        self._refresh_portal_origin_badge()

        title_row.addWidget(self.portal_origin_badge, 0, Qt.AlignmentFlag.AlignTop)
        title_row.addWidget(self.title_label, 1)

        text_layout.addLayout(title_row)

        text_layout.addWidget(self.subtitle_label)



        # 按钮部分

        self.action_btn = QPushButton("上传")

        self.action_btn.setObjectName("UploadBtn")

        self.action_btn.setCursor(Qt.CursorShape.PointingHandCursor)

        self.action_btn.setFixedSize(70, 28)

        self.action_btn.clicked.connect(self.on_btn_click)

        self.action_btn.clicked.connect(self.disable_highlight)

        self.action_btn.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)

        self.action_btn.customContextMenuRequested.connect(self.show_btn_menu)

        self.today_progress_btn = QPushButton("是否在进行")
        self.today_progress_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.today_progress_btn.setFixedSize(92, 28)
        self.today_progress_btn.clicked.connect(self.on_today_progress_click)

        # 倒计时组件（仅事件通告使用）

        self.timer_widget = None

        if self.data.get("notice_type") == "事件通告":

            try:

                from ..core.parser import PATTERN_TITLE_ALT



                self.timer_widget = TimerWidget()
                title_match = PATTERN_TITLE_ALT.search(self.data["text"])
                title = title_match.group(1) if title_match else ""
                self.timer_widget.parse_and_start(
                    self.data["text"],
                    title,
                    level=self.data.get("level"),
                    buildings=self.data.get("buildings"),
                )
                self._restore_timer_widget_state()
                self.timer_widget.alert_triggered.connect(self.enable_highlight)
                self.layout().addWidget(self.timer_widget)
            except Exception as e:

                # 记录错误但不中断UI加载

                print(f"TimerWidget initialization failed: {e}")

                self.timer_widget = None



        # 关闭按钮（增强可视效果）

        self.close_btn = QPushButton("×")

        self.close_btn.setFixedSize(26, 26)

        self.close_btn.setCursor(Qt.CursorShape.PointingHandCursor)

        self.close_btn.setStyleSheet("""

            QPushButton {

                background: rgba(255,255,255,0.15);

                color: #A1A1AA;

                border: 1px solid rgba(255,255,255,0.1);

                border-radius: 13px;

                font-size: 18px;

                font-weight: bold;

                padding-bottom: 2px;

            }

            QPushButton:hover {

                background: rgba(239,68,68,0.9);

                color: #FFFFFF;

                border-color: #EF4444;

            }

        """)

        self.close_btn.clicked.connect(self.toggle_expand)



        layout.addLayout(text_layout, 1)

        layout.addWidget(self.today_progress_btn)
        layout.addWidget(self.action_btn)

        if self.timer_widget:

            layout.addWidget(self.timer_widget)

        layout.addWidget(self.close_btn)
        self._refresh_today_progress_button()



    def _connect_signals(self):

        pass



    def resizeEvent(self, event):

        """手动管理 slide_container 的大小和位置"""

        super().resizeEvent(event)

        # 始终铺满

        if self._state == ItemState.NORMAL:

            self.slide_container.setGeometry(self.rect())



        # 调整删除按钮位置到最右侧

        bg_rect = self.background_frame.rect()

        self.delete_zone.move(bg_rect.width() - self._delete_zone_width, 0)

        self.delete_zone.setFixedHeight(bg_rect.height())



    def toggle_expand(self):
        """切换展开状态"""
        if not self._delete_interaction_enabled:
            return
        if self._state in (ItemState.EXPANDED, ItemState.SLIDING):
            self.collapse()
        elif self._state in (ItemState.NORMAL, ItemState.COLLAPSING):
            self.expand()


    def expand(self):

        """展开删除区"""

        if self._state in (ItemState.SLIDING, ItemState.DELETING):

            return



        # 互斥处理

        SwipeManager.set_expanded(self)



        self._state = ItemState.SLIDING

        self.delete_zone.show()



        from PyQt6.QtCore import QPropertyAnimation, QEasingCurve, QRect



        # 创建并保存动画引用，防止GC

        if self._slide_anim:

            self._slide_anim.stop()



        self._slide_anim = QPropertyAnimation(self.slide_container, b"geometry")

        self._slide_anim.setDuration(200)

        self._slide_anim.setEasingCurve(QEasingCurve.Type.OutCubic)



        current_rect = self.slide_container.geometry()

        target_rect = QRect(

            -self._delete_zone_width, 0, current_rect.width(), current_rect.height()

        )



        self._slide_anim.setStartValue(current_rect)

        self._slide_anim.setEndValue(target_rect)

        self._slide_anim.finished.connect(self._on_expand_finished)

        self._slide_anim.start()



    def _on_expand_finished(self):

        if self._state == ItemState.SLIDING:

            self._state = ItemState.EXPANDED

            # 3秒自动收回

            self._auto_collapse_timer.start(3000)



    def collapse(self):

        """收回删除区"""

        if self._state not in (ItemState.EXPANDED, ItemState.SLIDING):

            return



        self._state = ItemState.COLLAPSING

        self._auto_collapse_timer.stop()

        SwipeManager.clear(self)



        from PyQt6.QtCore import QPropertyAnimation, QEasingCurve



        if self._slide_anim:

            self._slide_anim.stop()



        self._slide_anim = QPropertyAnimation(self.slide_container, b"geometry")

        self._slide_anim.setDuration(150)

        self._slide_anim.setEasingCurve(QEasingCurve.Type.OutQuad)



        current_rect = self.slide_container.geometry()

        target_rect = self.rect()  # 回到初始位置



        self._slide_anim.setStartValue(current_rect)

        self._slide_anim.setEndValue(target_rect)

        self._slide_anim.finished.connect(self._on_collapse_finished)

        self._slide_anim.start()



    def _on_collapse_finished(self):

        if self._state == ItemState.COLLAPSING:

            self._state = ItemState.NORMAL

            self.delete_zone.hide()



    def _on_delete_clicked(self):
        """处理删除点击：先播放淡出动画再发送信号"""
        if not self._delete_interaction_enabled:
            return
        if self._state == ItemState.DELETING:
            return


        self._state = ItemState.DELETING

        self._auto_collapse_timer.stop()

        self.stop_timer()



        # 淡出且高度收缩动画

        # 不透明度动画 (需要 QGraphicsOpacityEffect，简化起见这里做高度收缩)

        # 用最大高度做收缩动画

        from PyQt6.QtCore import QPropertyAnimation



        self._fade_anim = QPropertyAnimation(self, b"maximumHeight")

        self._fade_anim.setDuration(200)

        self._fade_anim.setStartValue(self.height())

        self._fade_anim.setEndValue(0)

        self._fade_anim.finished.connect(self._finalize_delete)

        self._fade_anim.start()



    def _finalize_delete(self):
        """动画结束，真正删除"""
        self.delete_requested.emit(self.data)
        # 清除全局引用
        SwipeManager.clear(self)

    def cancel_delete_visual(self):
        """当外层拒绝删除时，恢复条目可见状态。"""
        if self._fade_anim:
            try:
                self._fade_anim.stop()
            except Exception:
                pass
            self._fade_anim = None
        try:
            self.setMaximumHeight(16777215)
        except Exception:
            pass
        if self._state == ItemState.DELETING:
            self._state = ItemState.NORMAL
        try:
            self.slide_container.setGeometry(self.rect())
        except Exception:
            pass
        try:
            self.delete_zone.hide()
        except Exception:
            pass
        self._auto_collapse_timer.stop()
        SwipeManager.clear(self)

    def _reset_swipe_visual(self):
        if self._slide_anim:
            try:
                self._slide_anim.stop()
            except Exception:
                pass
            self._slide_anim = None
        try:
            self.slide_container.setGeometry(self.rect())
        except Exception:
            pass
        try:
            self.delete_zone.hide()
        except Exception:
            pass
        self._auto_collapse_timer.stop()
        if self._state in (
            ItemState.SLIDING,
            ItemState.EXPANDED,
            ItemState.COLLAPSING,
        ):
            self._state = ItemState.NORMAL
        SwipeManager.clear(self)

    def set_delete_interaction_enabled(self, enabled: bool):
        self._delete_interaction_enabled = bool(enabled)
        try:
            self.close_btn.setEnabled(self._delete_interaction_enabled)
        except Exception:
            pass
        try:
            self.delete_btn.setEnabled(self._delete_interaction_enabled)
        except Exception:
            pass
        if self._delete_interaction_enabled:
            return
        # 禁用期间立即恢复任何滑动/删除中的视觉状态
        if self._state == ItemState.DELETING:
            self.cancel_delete_visual()
            return
        self._reset_swipe_visual()


    def update_labels(self, text):
        snapshot = build_notice_display_snapshot(self.data)
        display_title = snapshot.get("title") or "未知标题"
        subtitle_text = snapshot.get("subtitle") or ""

        if not hasattr(self, "title_label"):
            self.title_label = QLabel()
            self.title_label.setObjectName("ItemTitle")
        if not hasattr(self, "subtitle_label"):
            self.subtitle_label = QLabel()
            self.subtitle_label.setObjectName("ItemSubtitle")

        if len(display_title) > 30:
            display_title = display_title[:30] + "..."
        self.title_label.setText(display_title)
        self.subtitle_label.setText(subtitle_text)
        self.subtitle_label.setToolTip(snapshot.get("text") or text or "")

    def _refresh_portal_origin_badge(self):
        badge = getattr(self, "portal_origin_badge", None)
        if not badge:
            return
        try:
            if sip.isdeleted(badge):
                return
        except Exception:
            return
        is_portal_item = bool(self.data.get("lan_created_from_portal"))
        badge.setVisible(is_portal_item)


    def set_record_id(self, new_record_id):
        self.data["record_id"] = new_record_id
        self.data["_is_placeholder_record"] = False
        self._refresh_today_progress_button()


    def refresh_data(self, new_data_dict):
        self.data = new_data_dict
        self.today_in_progress_state = self._coerce_today_progress_state(
            new_data_dict.get("today_in_progress_state")
        )
        self.update_labels(new_data_dict["text"])
        self._refresh_portal_origin_badge()
        if self.timer_widget:
            self.timer_widget.update_context(
                text=new_data_dict.get("text"),
                buildings=new_data_dict.get("buildings"),
                level=new_data_dict.get("level"),
            )
            self._restore_timer_widget_state()
        # 内容被更新，标记为未上传
        self.has_unuploaded_changes = True
        self.pending_upload_hash = None
        if self._safe_action_btn():
            self._refresh_action_text()
        self._refresh_today_progress_button()

    def mark_as_uploaded(self):
        """标记当前内容已上传"""
        self.has_unuploaded_changes = False
        if self._safe_action_btn():
            self._refresh_action_text()


    def on_btn_click(self):

        # 根据当前状态决定操作类型

        if getattr(self, "is_end_status", False):

            action = "end"

        elif self.is_updated_status:

            action = "update"

        else:

            action = "upload"

        self.action_clicked.emit(self.data, action)

    @staticmethod
    def _coerce_today_progress_state(state):
        text = str(state or "").strip().lower()
        if text in ("yes", "no", "unknown"):
            return text
        if text in ("是", "在进行"):
            return "yes"
        if text in ("否", "未进行"):
            return "no"
        return "unknown"

    def _supports_today_progress(self):
        notice_type = str(self.data.get("notice_type") or "").strip()
        record_id = str(self.data.get("record_id") or "").strip()
        return (
            notice_type in ("设备变更", "变更通告")
            and bool(record_id)
            and not bool(self.data.get("_is_placeholder_record"))
        )

    def _safe_today_progress_btn(self):
        btn = getattr(self, "today_progress_btn", None)
        if not btn:
            return None
        try:
            if sip.isdeleted(btn):
                return None
        except Exception:
            return None
        return btn

    def _refresh_today_progress_button(self):
        btn = self._safe_today_progress_btn()
        if not btn:
            return
        visible = self._supports_today_progress()
        btn.setVisible(visible)
        if not visible:
            return
        self.set_today_progress_state(self.today_in_progress_state, enabled=btn.isEnabled())

    def set_today_progress_state(self, state: str, enabled: bool = True):
        btn = self._safe_today_progress_btn()
        if not btn:
            return
        normalized = self._coerce_today_progress_state(state)
        self.today_in_progress_state = normalized
        self.data["today_in_progress_state"] = normalized
        if normalized == "yes":
            text = "在进行"
            style = """
                QPushButton {
                    background: #16A34A;
                    color: #FFFFFF;
                    border: 1px solid #15803D;
                    border-radius: 14px;
                    font-size: 12px;
                    font-weight: 700;
                    padding: 0 10px;
                }
                QPushButton:disabled {
                    background: #166534;
                    color: rgba(255,255,255,0.75);
                    border-color: #166534;
                }
            """
        elif normalized == "no":
            text = "未进行"
            style = """
                QPushButton {
                    background: rgba(239,68,68,0.14);
                    color: #F87171;
                    border: 1px solid rgba(239,68,68,0.55);
                    border-radius: 14px;
                    font-size: 12px;
                    font-weight: 700;
                    padding: 0 10px;
                }
                QPushButton:disabled {
                    color: rgba(248,113,113,0.7);
                    border-color: rgba(239,68,68,0.35);
                }
            """
        else:
            text = "是否在进行"
            style = """
                QPushButton {
                    background: rgba(255,255,255,0.05);
                    color: #D1D5DB;
                    border: 1px solid rgba(255,255,255,0.16);
                    border-radius: 14px;
                    font-size: 12px;
                    font-weight: 600;
                    padding: 0 10px;
                }
                QPushButton:hover {
                    border-color: rgba(59,130,246,0.65);
                    color: #FFFFFF;
                }
                QPushButton:disabled {
                    color: rgba(209,213,219,0.7);
                }
            """
        btn.setText(text)
        btn.setStyleSheet(style)
        btn.setEnabled(enabled)

    def on_today_progress_click(self):
        if not self._supports_today_progress():
            return
        target_state = "no" if self.today_in_progress_state == "yes" else "yes"
        self.today_progress_clicked.emit(self.data, target_state)


    def set_action_enabled(self, enabled, text=None):
        """设置按钮启用状态和文本"""
        btn = self._safe_action_btn()
        if not btn:
            return
        btn.setEnabled(enabled)
        if text:
            btn.setText(text)
        elif enabled:
            self._refresh_action_text()

    def _safe_action_btn(self):
        btn = getattr(self, "action_btn", None)
        if not btn:
            return None
        try:
            if sip.isdeleted(btn):
                return None
        except Exception:
            return None
        return btn

    def _refresh_action_text(self):
        btn = self._safe_action_btn()
        if not btn:
            return
        if not getattr(self, "has_unuploaded_changes", True):
            self.set_uploaded_visual(True)
            return
        btn.setProperty("uploaded", "false")
        btn.setEnabled(True)
        # 恢复默认文本
        if getattr(self, "is_end_status", False):
            btn.setText("结束")
        else:
            btn.setText("更新" if self.is_updated_status else "上传")
        btn.style().unpolish(btn)
        btn.style().polish(btn)

    def set_uploaded_visual(self, active: bool):
        btn = self._safe_action_btn()
        if not btn:
            return
        if active:
            btn.setEnabled(False)
            btn.setProperty("uploaded", "true")
            btn.setText("已上传")
            # 清空局部样式，确保使用 QSS 的 [uploaded="true"] 样式
            btn.setStyleSheet("")
        else:
            btn.setProperty("uploaded", "false")
        btn.style().unpolish(btn)
        btn.style().polish(btn)

    # 【新增】强制设置按钮为“更新”状态的方法

    def set_button_to_update_mode(self):
        self.is_updated_status = True
        self.is_end_status = False  # 非结束状态
        btn = self._safe_action_btn()
        if btn:
            btn.setProperty("uploaded", "false")
            btn.setStyleSheet("")
        self._refresh_action_text()


    # 【新增】强制设置按钮为“结束”状态的方法

    def set_button_to_end_mode(self):
        self.is_updated_status = True
        self.is_end_status = True  # 结束状态，禁用右键菜单
        btn = self._safe_action_btn()
        if btn:
            btn.setProperty("uploaded", "false")
            btn.setStyleSheet("")
        self._refresh_action_text()


    def show_btn_menu(self, pos):

        # 右键菜单已移除，不再显示任何选项

        pass



    def on_end_triggered(self):

        self.stop_timer()  # 停止计时器

        self.ended_signal.emit(self.data)



    def reset_timer(self, action="更新", data_dict=None):
        """重置计时器（上传/更新时调用）"""
        self.disable_highlight()
        if data_dict:
            self.data = data_dict
            self.update_labels(data_dict.get("text", ""))
            if self.timer_widget:
                self.timer_widget.update_context(
                    text=data_dict.get("text"),
                    buildings=data_dict.get("buildings"),
                    level=data_dict.get("level"),
                )
        if hasattr(self, "timer_widget") and self.timer_widget:
            response_time = self.data.get("last_response_time", "") if self.data else ""
            self.timer_widget.reset(action, response_time)
            self._sync_timer_state_to_data()


    def stop_timer(self):

        """停止计时器（结束时调用）"""

        self.disable_highlight()  # 停止时取消高亮

        if hasattr(self, "timer_widget") and self.timer_widget:

            self.timer_widget.stop()

    def _sync_timer_state_to_data(self):
        if not isinstance(self.data, dict) or not self.timer_widget:
            return {}
        state = self.timer_widget.export_state()
        for key in self._TIMER_STATE_KEYS:
            self.data.pop(key, None)
        self.data.update(state)
        return state

    def _restore_timer_widget_state(self):
        if not self.timer_widget or not isinstance(self.data, dict):
            return False
        restored = self.timer_widget.restore_state(self.data)
        if restored:
            self._sync_timer_state_to_data()
        return restored



    def enable_highlight(self):

        """启用高亮样式"""

        self.inner_frame.setObjectName("HighlightFrame")

        self.inner_frame.style().unpolish(self.inner_frame)

        self.inner_frame.style().polish(self.inner_frame)



    def disable_highlight(self):

        """禁用高亮样式（恢复默认）"""

        # 如果当前是更新状态，应该不用 FlashFrame 也不用 HighlightFrame，恢复 ItemFrame

        # 下面的 update_status_style 会处理更新状态的样式，这里主要负责清除 Highlight

        if self.inner_frame.objectName() == "HighlightFrame":

            self.inner_frame.setObjectName("ItemFrame")

            self.inner_frame.style().unpolish(self.inner_frame)

            self.inner_frame.style().polish(self.inner_frame)



    def trigger_flash(self, count=0):
        if sip.isdeleted(self) or not getattr(self, "inner_frame", None):
            return
        if sip.isdeleted(self.inner_frame):
            return
        if count >= 6:
            try:
                self.inner_frame.setObjectName("ItemFrame")
                self.inner_frame.style().unpolish(self.inner_frame)
                self.inner_frame.style().polish(self.inner_frame)
            except RuntimeError:
                return
            return
        if count % 2 == 0:
            try:
                self.inner_frame.setObjectName("FlashFrame")
            except RuntimeError:
                return
        else:
            try:
                self.inner_frame.setObjectName("ItemFrame")
            except RuntimeError:
                return
        try:
            self.inner_frame.style().unpolish(self.inner_frame)
            self.inner_frame.style().polish(self.inner_frame)
        except RuntimeError:
            return
        ref = weakref.ref(self)

        def _next():
            obj = ref()
            if obj is None or sip.isdeleted(obj):
                return
            obj.trigger_flash(count + 1)

        QTimer.singleShot(200, _next)




class HistoryItemWidget(QWidget):
    def __init__(self, data_dict, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 8, 15, 8)
        layout.setSpacing(2)
        snapshot = build_notice_display_snapshot(data_dict)
        title = snapshot.get("title") or ""
        if len(title) > 30:
            title = title[:30] + "..."
        self.title_label = QLabel(title)
        self.title_label.setObjectName("HistoryTitle")
        self.subtitle_label = QLabel(snapshot.get("subtitle") or "")
        self.subtitle_label.setObjectName("ItemSubtitle")
        layout.addWidget(self.title_label)
        layout.addWidget(self.subtitle_label)



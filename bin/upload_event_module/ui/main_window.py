import os
import queue
import threading
from collections import deque

from PyQt6.QtCore import Qt, QTimer, pyqtSignal
from PyQt6.QtWidgets import QWidget

from ..config import config
from ..logger import log_info
from ..hot_reload.state_store import get_user_data_dir
from ..hot_reload.connection_registry import ConnectionRegistry
from ..services.service_registry import refresh_feishu_token
from .dialogs import ClipboardPreviewDialog, DetailDialog, ScreenshotConfirmDialog
from .settings_dialog import SettingsDialog
from .main_window_patch import PatchUpdateMixin
from .main_window_cache import ActiveCacheMixin
from .main_window_clipboard import MainWindowClipboardMixin
from .main_window_records import MainWindowRecordsMixin
from .main_window_workflow import MainWindowWorkflowMixin
from .main_window_ui import MainWindowUiMixin
from .main_window_runtime import MainWindowRuntimeMixin
from .event_relay_bridge import EventRelayBridge
from .active_cache_store import ActiveCacheStore
from ..utils import ACTIVE_CACHE_FILE


class ClipboardTool(
    MainWindowClipboardMixin,
    MainWindowRecordsMixin,
    MainWindowWorkflowMixin,
    MainWindowUiMixin,
    MainWindowRuntimeMixin,
    PatchUpdateMixin,
    ActiveCacheMixin,
    QWidget,
):
    # 缃戠粶璇锋眰瀹屾垚淇″彿锛?action_name, success, message, record_id)
    request_finished = pyqtSignal(str, bool, str, str)
    patch_update_finished = pyqtSignal(bool, str)
    clipboard_entry_received = pyqtSignal(dict)
    remote_update_checked = pyqtSignal(str, object, object)
    remote_patch_downloaded = pyqtSignal(object, object, str, bool)
    remote_update_phase = pyqtSignal(str)
    lan_template_notice_received = pyqtSignal(dict)
    lan_maintenance_action_received = pyqtSignal(dict)
    lan_maintenance_ongoing_query_received = pyqtSignal(dict)
    lan_ongoing_delete_received = pyqtSignal(dict)

    def __init__(self):
        super().__init__()
        self._disable_effects = bool(
            os.environ.get("CLIPFLOW_DISABLE_EFFECTS")
            or os.environ.get("CLIPFLOW_SAFE_MODE")
        )
        self.setObjectName("MainWindow")
        self.setWindowTitle("ClipFlow")
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint
            | Qt.WindowType.Tool
            | Qt.WindowType.WindowStaysOnTopHint
        )
        if not self._disable_effects:
            self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.resize(500, 550)

        log_info("================ 应用程序启动 ================")
        self._install_qt_message_handler()

        # 检查并刷新飞书 Token (初始化时，如有必要)
        if not config.user_token:
            # 可以在这里或者 load 时刷新，原代码是直接调用
            threading.Thread(target=refresh_feishu_token, daemon=True).start()

        # 剪贴板监听（独立进程 + 文件IPC）
        self._clipboard_process = None
        self._clipboard_restart_count = 0
        self._clipboard_pending_lines = []
        self._clipboard_max_lines_per_tick = 5
        self._clipboard_cooldown_until = 0.0
        self._ui_update_in_progress = False
        self._clipboard_pending_entries = []
        self._clipboard_pending_lock = threading.Lock()
        self._clipboard_retention = 50
        self._clipboard_recent_entries = {}
        self._clipboard_recent_lock = threading.Lock()
        self._clipboard_entry_dedupe_window_seconds = 3.0
        self._clipboard_file_index = 0
        self._clipboard_partial_line = ""
        self._clipboard_file_max_bytes = 5 * 1024 * 1024
        self.clipboard_event_file = get_user_data_dir() / "clipboard_events.jsonl"
        self._clipboard_apply_setting_pending = False
        self._clipboard_stopping = False
        self._clipboard_desired_disabled = bool(
            getattr(config, "disable_clipboard_listener", False)
        )
        self._clipboard_effective_running = False
        self._clipboard_toggle_transition_in_progress = False
        self._clipboard_toggle_target_disabled = self._clipboard_desired_disabled
        self._clipboard_toggle_request_seq = 0
        self._clipboard_restore_verify_timeout_ms = 3000
        self._clipboard_last_restore_failure_reason = ""
        self._clipboard_last_failure_reason = ""
        self._clipboard_last_failure_ts = 0.0
        self._clipboard_health_restart_attempted = False
        self._clipboard_auto_restart_blocked = False
        self._clipboard_resume_after_stop = False
        self._clipboard_last_error = ""
        self._clipboard_stderr_tail = []
        self._clipboard_stderr_tail_limit = 80
        self._clipboard_trace_file = (
            get_user_data_dir() / "clipboard_listener_trace.log"
        )
        self._clipboard_crash_window_seconds = 300.0
        self._clipboard_crash_threshold = 5
        self._clipboard_crash_timestamps = deque()
        self._clipboard_degraded = False
        self._closing = False
        self._last_ui_op = ""
        self._delete_interaction_enabled = True
        self._last_clipboard_snapshot_text = ""
        self._last_clipboard_snapshot_ts = 0
        self._clipboard_preview_auto_show_enabled = True
        self._lan_portal_jobs_by_record_id = {}
        self._lan_portal_jobs_by_active_item_id = {}

        self.current_theme = "dark"
        self.notice_tab = "event"
        self.connection_registry = ConnectionRegistry()
        self._event_relay_bridge = EventRelayBridge(
            self,
            host="0.0.0.0",
            port=62345,
        )
        self._ui_signal_queue = queue.Queue()
        self._ui_signal_max_per_tick = 20
        self._ui_signal_timer = QTimer(self)
        self.connection_registry.connect(
            "ui_signal_timer",
            self._ui_signal_timer,
            "timeout",
            self._drain_ui_signal_queue,
        )
        self._ui_signal_timer.start(100)
        self._ui_mutation_queue = queue.Queue()
        self._ui_mutation_max_per_tick = 10
        self._ui_mutation_timer = QTimer(self)
        self.connection_registry.connect(
            "ui_mutation_timer",
            self._ui_mutation_timer,
            "timeout",
            self._drain_ui_mutations,
        )
        self._ui_mutation_timer.start(80)
        self.detail_dialog = DetailDialog(None, theme=self.current_theme)
        self.connection_registry.connect(
            "detail_dialog",
            self.detail_dialog,
            "record_id_bind_requested",
            self._request_safe_bind_record_id,
        )
        self.connection_registry.connect(
            "detail_dialog",
            self.detail_dialog,
            "content_changed",
            self.sync_content_to_widget,
        )
        self.connection_registry.connect(
            "detail_dialog",
            self.detail_dialog,
            "finished",
            self._flush_pending_cache_refresh,
        )

        self.add_dialog = None
        self.clipboard_preview_dialog = ClipboardPreviewDialog(
            None, theme=self.current_theme
        )
        self.connection_registry.connect(
            "clipboard_preview_dialog",
            self.clipboard_preview_dialog,
            "use_requested",
            self._use_last_clipboard_snapshot,
        )
        self.connection_registry.connect(
            "clipboard_preview_dialog",
            self.clipboard_preview_dialog,
            "closed_by_user",
            self._on_clipboard_preview_closed_by_user,
        )
        self.settings_dialog = SettingsDialog(None)
        self.cache_store = ActiveCacheStore(ACTIVE_CACHE_FILE)
        self.connection_registry.connect(
            "settings_dialog",
            self.settings_dialog,
            "settings_saved",
            self.refresh_table_links,
        )
        self.connection_registry.connect(
            "settings_dialog",
            self.settings_dialog,
            "finished",
            self._on_settings_closed,
        )
        self.connection_registry.connect(
            "settings_dialog",
            self.settings_dialog,
            "settings_saved",
            self.refresh_hot_reload_setting,
        )
        self.connection_registry.connect(
            "settings_dialog",
            self.settings_dialog,
            "settings_saved",
            self.refresh_alert_setting,
        )
        self.connection_registry.connect(
            "settings_dialog",
            self.settings_dialog,
            "settings_saved",
            self.refresh_event_relay_setting,
        )
        self.connection_registry.connect(
            "settings_dialog",
            self.settings_dialog,
            "settings_saved",
            self.refresh_lan_template_portal_setting,
        )
        self.screenshot_dialog = ScreenshotConfirmDialog(None, theme=self.current_theme)
        self.screenshot_dialog.bind_cache_store(self.cache_store)
        self._active_messages = []
        self.connection_registry.connect(
            "screenshot_dialog",
            self.screenshot_dialog,
            "upload_confirmed",
            self.do_feishu_upload,
        )
        self.connection_registry.connect(
            "screenshot_dialog",
            self.screenshot_dialog,
            "cancelled",
            self.on_screenshot_cancelled,
        )
        self.connection_registry.connect(
            "screenshot_dialog",
            self.screenshot_dialog,
            "screenshot_started",
            self._on_screenshot_started,
        )
        self.connection_registry.connect(
            "screenshot_dialog",
            self.screenshot_dialog,
            "screenshot_finished",
            self._on_screenshot_finished,
        )
        self.connection_registry.connect(
            "screenshot_dialog",
            self.screenshot_dialog,
            "state_changed",
            self.save_active_cache,
        )

        # 已移除 item 引用缓存，结束流程按 record_id 动态查找
        self.last_history_mtime = 0
        self.pending_replace_by_record_id = {}
        self.pending_upload_rollback_by_record_id = {}
        self.pending_end_rollback_by_record_id = {}
        self.pending_new_by_record_id = {}
        self.pending_update_after_upload = {}
        self._pending_update_after_upload_scheduled = False
        self.pending_action_types = {}
        self.pending_action_record_ids = set()
        self._today_in_progress_pending_record_ids = set()
        self._today_in_progress_synced_record_ids = set()
        self._record_binding_validation_pending_ids = set()
        self._record_binding_validated_ids = set()
        self.current_screenshot_record_id = None
        self.current_screenshot_action_type = None
        self._pending_cache_refresh = False
        self._cache_refresh_single_shot_pending = False
        self._pending_force_uploads = []
        self._payload_store = {}
        self._payload_alias = {}
        # 已移除 item 缓存，重复判断统一按内容/标题动态查找
        self._upload_queues = {}
        self._upload_workers = {}
        self._upload_lock = threading.Lock()
        self._upload_key_alias = {}
        self._feishu_request_lock = threading.Lock()
        self._is_restoring_cache = False
        self.clipboard_paused = False
        self._deferred_events = []
        self._manual_add_hot_reload_paused = False
        self.update_state_path = get_user_data_dir() / "update_overlay_state.json"
        self.restart_overlay_state_path = (
            get_user_data_dir() / "restart_overlay_state.json"
        )
        self._pending_update_overlay_cleanup = False
        self._restore_restart_overlay_geometry()

        self.drag_position = None
        self.init_ui()
        self.setup_tray()
        self._cache_id_repair_result = self._validate_cache_record_ids_on_startup()
        self._restore_active_cache()
        self._repair_missing_item_widgets()
        self._reconcile_active_route_duplicates()
        self._validate_record_bindings_on_startup()
        if self._cache_id_repair_result.get("changed"):
            self.save_active_cache()
        self._init_active_cache_timer()
        self._init_runtime_maintenance_timer()
        self._init_clipboard_ipc()

        self.timer = None

        self.patch_check_timer = QTimer()
        self.connection_registry.connect(
            "patch_check_timer",
            self.patch_check_timer,
            "timeout",
            self._refresh_patch_button,
        )
        self.patch_check_timer.start(3000)
        self._init_remote_patch_updater()

        self.connection_registry.connect(
            "main_window",
            self,
            "request_finished",
            self.on_request_finished,
            Qt.ConnectionType.QueuedConnection,
        )
        self.connection_registry.connect(
            "main_window",
            self,
            "patch_update_finished",
            self._on_patch_update_finished,
            Qt.ConnectionType.QueuedConnection,
        )
        self.connection_registry.connect(
            "main_window",
            self,
            "remote_update_checked",
            self._on_remote_update_checked,
            Qt.ConnectionType.QueuedConnection,
        )
        self.connection_registry.connect(
            "main_window",
            self,
            "remote_patch_downloaded",
            self._on_remote_patch_downloaded,
            Qt.ConnectionType.QueuedConnection,
        )
        self.connection_registry.connect(
            "main_window",
            self,
            "remote_update_phase",
            self._set_remote_update_status,
            Qt.ConnectionType.QueuedConnection,
        )
        self.connection_registry.connect(
            "main_window",
            self,
            "lan_template_notice_received",
            self._on_lan_template_notice_received,
            Qt.ConnectionType.QueuedConnection,
        )
        self.connection_registry.connect(
            "main_window",
            self,
            "lan_maintenance_action_received",
            self._on_lan_maintenance_action_received,
            Qt.ConnectionType.QueuedConnection,
        )
        self.connection_registry.connect(
            "main_window",
            self,
            "lan_maintenance_ongoing_query_received",
            self._on_lan_maintenance_ongoing_query_received,
            Qt.ConnectionType.QueuedConnection,
        )
        self.connection_registry.connect(
            "main_window",
            self,
            "lan_ongoing_delete_received",
            self._on_lan_ongoing_delete_received,
            Qt.ConnectionType.QueuedConnection,
        )

        # 初始化事件中转服务
        self.connection_registry.connect(
            "event_relay_bridge",
            self._event_relay_bridge,
            "event_received",
            self._on_event_relay_received,
            Qt.ConnectionType.QueuedConnection,
        )
        self.connection_registry.connect(
            "event_relay_bridge",
            self._event_relay_bridge,
            "status_changed",
            self._update_event_relay_status,
            Qt.ConnectionType.QueuedConnection,
        )
        self._apply_event_relay_setting(force_reload=False)
        self._init_hot_reload()
        QTimer.singleShot(0, lambda: self._log_runtime_health_snapshot("startup"))
        QTimer.singleShot(0, self._restore_update_overlay_state)
        QTimer.singleShot(600, self._close_restart_overlay_window)
        QTimer.singleShot(1500, self._check_ocr_lang_pack)
        # 强制刷新和提升窗口，确保完全渲染
        QTimer.singleShot(0, self.repaint)
        QTimer.singleShot(0, self.raise_)


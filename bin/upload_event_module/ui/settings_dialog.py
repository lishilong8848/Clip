from concurrent.futures import ThreadPoolExecutor

from PyQt6.QtWidgets import (

    QDialog,

    QVBoxLayout,

    QFrame,

    QHBoxLayout,

    QLabel,

    QPushButton,

    QLineEdit,

    QScrollArea,

    QWidget,
    QCheckBox,
)
from PyQt6.QtCore import Qt, QTimer

from ..config import (

    config,

    DEFAULT_FEISHU_APP_TOKEN,

    DEFAULT_TABLE_ID_WEIBAO,

    DEFAULT_TABLE_ID_BIANGENG,

    DEFAULT_TABLE_ID_TIAOZHENG,

    DEFAULT_TABLE_ID_SHIJIAN,

    DEFAULT_TABLE_ID_POWER,

    DEFAULT_TABLE_ID_POLLING,

    DEFAULT_TABLE_ID_OVERHAUL,
    DEFAULT_GROUP_NAME_CHANGE_I3,
    DEFAULT_GROUP_NAME_MAINTENANCE,

    DEFAULT_GROUP_NAME_EVENT_I2,

    DEFAULT_GROUP_NAME_EVENT_I3,

    DEFAULT_GROUP_NAME_EVENT_PROMPT,
    DEFAULT_LAN_TEMPLATE_PORTAL_HOST,
    DEFAULT_LAN_TEMPLATE_PORTAL_PORT,
    DEFAULT_LAN_TEMPLATE_PUBLIC_HOST,
    DEFAULT_LAN_LOW_PERFORMANCE_MODE,
    DEFAULT_POLLING_WORK_ORDER_PUBLIC_RELAY_ENABLED,
    DEFAULT_POLLING_WORK_ORDER_PUBLIC_RELAY_URL,
    DEFAULT_DISABLE_HOT_RELOAD,
    DEFAULT_DISABLE_ALERTS,
    DEFAULT_DISABLE_SPEECH,
)
from ..services.service_registry import resolve_bitable_app_token

from .common import show_toast_message





class SettingsDialog(QDialog):
    # 配置保存成功信号
    from PyQt6.QtCore import pyqtSignal



    settings_saved = pyqtSignal()
    relay_health_test_finished = pyqtSignal(dict)


    def __init__(self, parent=None):

        super().__init__(parent)
        self._relay_health_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="RelayHealthTest"
        )
        self.relay_health_test_finished.connect(self._on_relay_health_test_finished)

        self.setObjectName("SettingsWindow")

        self.setWindowFlags(

            Qt.WindowType.FramelessWindowHint

            | Qt.WindowType.WindowStaysOnTopHint

            | Qt.WindowType.Tool

        )

        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

        self.resize(550, 680)  # 增加高度以容纳更多内容


        layout = QVBoxLayout(self)

        layout.setContentsMargins(5, 5, 5, 5)

        self.container = QFrame()

        self.container.setObjectName("AddWindow")  # 复用 AddWindow 样式
        inner_layout = QVBoxLayout(self.container)



        # 顶部栏
        top_bar = QHBoxLayout()

        title = QLabel("设置")
        title.setObjectName("TitleLabel")

        close_btn = QPushButton("关闭")
        close_btn.setObjectName("CloseBtn")

        close_btn.clicked.connect(self.hide)

        top_bar.addWidget(title)

        top_bar.addStretch()

        top_bar.addWidget(close_btn)



        # 使用可滚动区域
        scroll_area = QScrollArea()

        scroll_area.setWidgetResizable(True)

        scroll_area.setFrameShape(QFrame.Shape.NoFrame)

        scroll_area.setStyleSheet("background: transparent;")  # 透明背景
        scroll_widget = QWidget()

        scroll_widget.setStyleSheet("background: transparent;")  # 透明背景
        form_layout = QVBoxLayout(scroll_widget)

        form_layout.setSpacing(10)



        # 飞书配置标题
        feishu_title = QLabel("飞书 SDK 配置：")
        feishu_title.setStyleSheet("font-weight: bold; color: #6366F1;")

        form_layout.addWidget(feishu_title)



        # App ID

        app_id_label = QLabel("应用 ID (App ID)：")
        self.app_id_input = QLineEdit()

        self.app_id_input.setPlaceholderText("输入飞书自建应用 App ID")
        form_layout.addWidget(app_id_label)

        form_layout.addWidget(self.app_id_input)



        # App Secret

        app_secret_label = QLabel("应用密钥 (App Secret)：")
        self.app_secret_input = QLineEdit()

        self.app_secret_input.setPlaceholderText("输入飞书自建应用 App Secret")
        self.app_secret_input.setEchoMode(QLineEdit.EchoMode.PasswordEchoOnEdit)

        form_layout.addWidget(app_secret_label)

        form_layout.addWidget(self.app_secret_input)



        # 应用 Token
        app_token_label = QLabel("多维表格 Token (app_token)：")
        self.app_token_input = QLineEdit()

        self.app_token_input.setPlaceholderText("输入多维表格应用 Token")
        form_layout.addWidget(app_token_label)

        form_layout.addWidget(self.app_token_input)



        # ========== 表格ID配置 ==========
        table_title = QLabel("表格 ID 配置（按通告类型）：")
        table_title.setStyleSheet(

            "font-weight: bold; color: #F59E0B; margin-top: 10px;"

        )

        form_layout.addWidget(table_title)



        # 维保通告 Table ID
        weibao_label = QLabel("维保通告 Table ID：")
        self.table_id_weibao_input = QLineEdit()

        self.table_id_weibao_input.setPlaceholderText("输入维保通告表格 ID")
        form_layout.addWidget(weibao_label)

        form_layout.addWidget(self.table_id_weibao_input)



        # 变更通告 Table ID
        biangeng_label = QLabel("变更通告 Table ID：")
        self.table_id_biangeng_input = QLineEdit()

        self.table_id_biangeng_input.setPlaceholderText("输入变更通告表格 ID")
        form_layout.addWidget(biangeng_label)

        form_layout.addWidget(self.table_id_biangeng_input)



        # 设备调整 Table ID
        tiaozheng_label = QLabel("设备调整 Table ID：")
        self.table_id_tiaozheng_input = QLineEdit()

        self.table_id_tiaozheng_input.setPlaceholderText("输入设备调整表格 ID")
        form_layout.addWidget(tiaozheng_label)

        form_layout.addWidget(self.table_id_tiaozheng_input)



        # 事件通告 Table ID
        shijian_label = QLabel("事件通告 Table ID：")
        self.table_id_shijian_input = QLineEdit()

        self.table_id_shijian_input.setPlaceholderText("输入事件通告表格 ID")
        form_layout.addWidget(shijian_label)

        form_layout.addWidget(self.table_id_shijian_input)



        # 上下电通告 Table ID
        power_label = QLabel("上下电通告 Table ID：")
        self.table_id_power_input = QLineEdit()

        self.table_id_power_input.setPlaceholderText("输入上下电通告表格 ID")
        form_layout.addWidget(power_label)

        form_layout.addWidget(self.table_id_power_input)



        # 设备轮巡 Table ID
        polling_label = QLabel("设备轮巡 Table ID：")
        self.table_id_polling_input = QLineEdit()

        self.table_id_polling_input.setPlaceholderText("输入设备轮巡表格 ID")
        form_layout.addWidget(polling_label)

        form_layout.addWidget(self.table_id_polling_input)



        # 设备检修 Table ID
        overhaul_label = QLabel("设备检修 Table ID：")
        self.table_id_overhaul_input = QLineEdit()

        self.table_id_overhaul_input.setPlaceholderText("输入设备检修表格 ID")
        form_layout.addWidget(overhaul_label)

        form_layout.addWidget(self.table_id_overhaul_input)



        

        self.disable_hot_reload_checkbox = QCheckBox("禁用热更新（watchdog）")
        form_layout.addWidget(self.disable_hot_reload_checkbox)

        self.disable_alerts_checkbox = QCheckBox("禁用语音/闪烁提醒")
        form_layout.addWidget(self.disable_alerts_checkbox)
        self.disable_speech_checkbox = QCheckBox("禁用语音播报（默认开启）")
        form_layout.addWidget(self.disable_speech_checkbox)

        # ========== 局域网页面配置 ==========
        lan_portal_title = QLabel("局域网页面")
        lan_portal_title.setStyleSheet(
            "font-weight: bold; color: #10B981; margin-top: 10px;"
        )
        form_layout.addWidget(lan_portal_title)

        lan_portal_card = QFrame()
        lan_portal_card.setObjectName("LanPortalCard")
        lan_portal_card.setStyleSheet(
            "QFrame#LanPortalCard {"
            "background: rgba(16, 185, 129, 0.08);"
            "border: 1px solid rgba(16, 185, 129, 0.28);"
            "border-radius: 10px;"
            "}"
            "QLabel#LanPortalHint { color: #4B5563; font-size: 12px; }"
        )
        lan_portal_layout = QVBoxLayout(lan_portal_card)
        lan_portal_layout.setContentsMargins(12, 10, 12, 10)
        lan_portal_layout.setSpacing(8)

        lan_host_label = QLabel("局域网 IP")
        self.lan_template_portal_host_input = QLineEdit()
        self.lan_template_portal_host_input.setPlaceholderText(
            "例如 192.168.1.20；默认 0.0.0.0"
        )
        self.lan_template_portal_host_input.setMinimumHeight(34)
        lan_portal_layout.addWidget(lan_host_label)
        lan_portal_layout.addWidget(self.lan_template_portal_host_input)

        lan_public_host_label = QLabel("签名链接局域网地址")
        self.lan_template_public_host_input = QLineEdit()
        self.lan_template_public_host_input.setPlaceholderText(
            "例如 192.168.224.130；手机打开签名链接用"
        )
        self.lan_template_public_host_input.setMinimumHeight(34)
        lan_portal_layout.addWidget(lan_public_host_label)
        lan_portal_layout.addWidget(self.lan_template_public_host_input)

        lan_hint = QLabel(
            f"访问地址为 http://填写的IP:{DEFAULT_LAN_TEMPLATE_PORTAL_PORT}/；"
            "监听 IP 可填 0.0.0.0；签名链接局域网地址请填写手机能访问到的电脑 IP。"
        )
        lan_hint.setObjectName("LanPortalHint")
        lan_hint.setWordWrap(True)
        lan_portal_layout.addWidget(lan_hint)
        self.lan_low_performance_checkbox = QCheckBox(
            "低性能模式（多楼同时发通告时降低后台刷新、Qt显示和上传频率）"
        )
        self.lan_low_performance_checkbox.setToolTip(
            "勾选后会让通告卡片和多维上传更慢地排队处理，优先保证运行程序电脑不卡。"
        )
        lan_portal_layout.addWidget(self.lan_low_performance_checkbox)
        self.polling_work_order_public_relay_checkbox = QCheckBox("使用公网工单")
        self.polling_work_order_public_relay_checkbox.setToolTip(
            "仅影响之后新建的轮巡/维保工单；未勾选时继续使用当前局域网工单。"
        )
        lan_portal_layout.addWidget(self.polling_work_order_public_relay_checkbox)
        public_relay_url_label = QLabel("公网工单地址")
        self.polling_work_order_public_relay_url_input = QLineEdit()
        self.polling_work_order_public_relay_url_input.setPlaceholderText(
            DEFAULT_POLLING_WORK_ORDER_PUBLIC_RELAY_URL
        )
        self.polling_work_order_public_relay_url_input.setMinimumHeight(34)
        lan_portal_layout.addWidget(public_relay_url_label)
        lan_portal_layout.addWidget(self.polling_work_order_public_relay_url_input)
        relay_test_row = QHBoxLayout()
        self.polling_work_order_public_relay_test_button = QPushButton("测试连接")
        self.polling_work_order_public_relay_test_button.clicked.connect(
            self._test_polling_work_order_public_relay
        )
        self.polling_work_order_public_relay_test_status = QLabel("尚未检测")
        self.polling_work_order_public_relay_test_status.setWordWrap(True)
        relay_test_row.addWidget(self.polling_work_order_public_relay_test_button)
        relay_test_row.addWidget(
            self.polling_work_order_public_relay_test_status, 1
        )
        lan_portal_layout.addLayout(relay_test_row)
        public_relay_hint = QLabel(
            "局域网模拟可使用 http://192.168.224.122:18767；正式公网必须填写 HTTPS 根地址。"
        )
        public_relay_hint.setObjectName("LanPortalHint")
        public_relay_hint.setWordWrap(True)
        lan_portal_layout.addWidget(public_relay_hint)
        self.polling_work_order_public_relay_checkbox.toggled.connect(
            self.polling_work_order_public_relay_url_input.setEnabled
        )
        form_layout.addWidget(lan_portal_card)


        # ========== 群机器人配置 ==========
        robot_title = QLabel("群机器人配置：")
        robot_title.setStyleSheet("font-weight: bold; color: #EC4899; margin-top: 10px;")

        form_layout.addWidget(robot_title)

        change_i3_label = QLabel("I3变更群名称：")
        self.group_name_change_i3_input = QLineEdit()

        self.group_name_change_i3_input.setPlaceholderText("输入群名称（需与飞书群名一致）")
        form_layout.addWidget(change_i3_label)

        form_layout.addWidget(self.group_name_change_i3_input)


        maintenance_label = QLabel("维保群名称：")
        self.group_name_maintenance_input = QLineEdit()

        self.group_name_maintenance_input.setPlaceholderText("输入维保群名称（需与飞书群名一致）")
        form_layout.addWidget(maintenance_label)

        form_layout.addWidget(self.group_name_maintenance_input)



        event_i2_label = QLabel("I2事件群名称：")
        self.group_name_event_i2_input = QLineEdit()

        self.group_name_event_i2_input.setPlaceholderText("输入群名称（需与飞书群名一致）")
        form_layout.addWidget(event_i2_label)

        form_layout.addWidget(self.group_name_event_i2_input)



        event_i3_label = QLabel("I3事件群名称：")
        self.group_name_event_i3_input = QLineEdit()

        self.group_name_event_i3_input.setPlaceholderText("输入群名称（需与飞书群名一致）")
        form_layout.addWidget(event_i3_label)

        form_layout.addWidget(self.group_name_event_i3_input)



        event_prompt_label = QLabel("事件提示群名称：")
        self.group_name_event_prompt_input = QLineEdit()

        self.group_name_event_prompt_input.setPlaceholderText(

            "倒计时剩余1分钟提醒群（可留空）"
        )

        form_layout.addWidget(event_prompt_label)

        form_layout.addWidget(self.group_name_event_prompt_input)



        scroll_area.setWidget(scroll_widget)



        # 按钮区域
        btn_layout = QHBoxLayout()

        self.save_btn = QPushButton("保存")
        self.save_btn.setObjectName("ConfirmBtn")

        self.save_btn.clicked.connect(self.save_settings)



        self.cancel_btn = QPushButton("取消")
        self.cancel_btn.setObjectName("DiffCancelBtn")

        self.cancel_btn.clicked.connect(self.hide)



        self.reset_btn = QPushButton("恢复默认")
        self.reset_btn.setObjectName("ClearBtn")

        self.reset_btn.clicked.connect(self.reset_to_default)



        btn_layout.addWidget(self.reset_btn)

        btn_layout.addStretch()

        btn_layout.addWidget(self.cancel_btn)

        btn_layout.addWidget(self.save_btn)



        inner_layout.addLayout(top_bar)

        inner_layout.addWidget(scroll_area, 1)

        inner_layout.addLayout(btn_layout)

        layout.addWidget(self.container)



        self.drag_position = None
    def load_current_settings(self):
        """加载当前配置到输入框"""
        self.app_id_input.setText(config.app_id)

        self.app_secret_input.setText(config.app_secret)

        self.app_token_input.setText(config.app_token)

        self.table_id_weibao_input.setText(config.table_id_weibao)

        self.table_id_biangeng_input.setText(config.table_id_biangeng)

        self.table_id_tiaozheng_input.setText(config.table_id_tiaozheng)

        self.table_id_shijian_input.setText(config.table_id_shijian)

        self.table_id_power_input.setText(config.table_id_power)

        self.table_id_polling_input.setText(config.table_id_polling)
        self.table_id_overhaul_input.setText(config.table_id_overhaul)
        self.disable_hot_reload_checkbox.setChecked(bool(config.disable_hot_reload))
        self.disable_alerts_checkbox.setChecked(bool(config.disable_alerts))
        self.disable_speech_checkbox.setChecked(
            bool(getattr(config, "disable_speech", DEFAULT_DISABLE_SPEECH))
        )
        self.group_name_change_i3_input.setText(config.group_name_change_i3)
        self.group_name_maintenance_input.setText(
            getattr(config, "group_name_maintenance", "")
        )
        self.group_name_event_i2_input.setText(config.group_name_event_i2)
        self.group_name_event_i3_input.setText(config.group_name_event_i3)
        self.group_name_event_prompt_input.setText(config.group_name_event_prompt)
        self.lan_template_portal_host_input.setText(
            getattr(
                config,
                "lan_template_portal_host",
                DEFAULT_LAN_TEMPLATE_PORTAL_HOST,
            )
        )
        self.lan_template_public_host_input.setText(
            getattr(
                config,
                "lan_template_public_host",
                DEFAULT_LAN_TEMPLATE_PUBLIC_HOST,
            )
        )
        self.lan_low_performance_checkbox.setChecked(
            bool(getattr(config, "lan_low_performance_mode", False))
        )
        self.polling_work_order_public_relay_checkbox.setChecked(
            bool(
                getattr(
                    config,
                    "polling_work_order_public_relay_enabled",
                    DEFAULT_POLLING_WORK_ORDER_PUBLIC_RELAY_ENABLED,
                )
            )
        )
        self.polling_work_order_public_relay_url_input.setText(
            str(
                getattr(
                    config,
                    "polling_work_order_public_relay_url",
                    DEFAULT_POLLING_WORK_ORDER_PUBLIC_RELAY_URL,
                )
                or DEFAULT_POLLING_WORK_ORDER_PUBLIC_RELAY_URL
            )
        )
        self.polling_work_order_public_relay_url_input.setEnabled(
            self.polling_work_order_public_relay_checkbox.isChecked()
        )

    @staticmethod
    def _is_valid_lan_template_portal_host(value: str) -> bool:
        value = str(value or "").strip()
        if not value:
            return False
        if value.lower() == "localhost":
            return True
        try:
            import ipaddress

            ipaddress.ip_address(value)
            return True
        except Exception:
            return False

    @staticmethod
    def _is_valid_lan_template_public_host(value: str) -> bool:
        value = str(value or "").strip()
        if not value:
            return False
        if value.startswith(("http://", "https://")):
            try:
                from urllib.parse import urlparse

                parsed = urlparse(value)
                value = parsed.hostname or ""
            except Exception:
                return False
        if value.lower() == "localhost":
            return True
        try:
            import ipaddress

            ipaddress.ip_address(value)
            return True
        except Exception:
            import re

            return bool(
                re.fullmatch(
                    r"[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?",
                    value,
                )
            )

    @staticmethod
    def _is_valid_polling_work_order_public_relay_url(value: str) -> bool:
        from urllib.parse import urlsplit

        text = str(value or "").strip().rstrip("/")
        try:
            parsed = urlsplit(text)
        except Exception:
            return False
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            return False
        if parsed.scheme == "https":
            return True
        host = str(parsed.hostname or "").lower()
        if host == "localhost":
            return True
        try:
            import ipaddress

            address = ipaddress.ip_address(host)
            return address.is_loopback or address.is_private
        except Exception:
            return False

    def _test_polling_work_order_public_relay(self) -> None:
        relay_url = self.polling_work_order_public_relay_url_input.text().strip().rstrip("/")
        if not self._is_valid_polling_work_order_public_relay_url(relay_url):
            show_toast_message(self, "❌ 请先填写有效的公网工单根地址", duration_ms=2200)
            return
        self.polling_work_order_public_relay_test_button.setEnabled(False)
        self.polling_work_order_public_relay_test_status.setText("检测中...")

        def run() -> None:
            try:
                from lan_bitable_template_portal.polling_work_order_relay import (
                    probe_polling_relay_health,
                )

                result = probe_polling_relay_health(relay_url, timeout=2.0)
            except Exception as exc:
                result = {"ready": False, "error": str(exc), "elapsed_ms": 0.0}
            try:
                self.relay_health_test_finished.emit(result)
            except RuntimeError:
                pass

        self._relay_health_executor.submit(run)

    def _on_relay_health_test_finished(self, result: dict) -> None:
        self.polling_work_order_public_relay_test_button.setEnabled(True)
        elapsed = float((result or {}).get("elapsed_ms") or 0)
        if (result or {}).get("ready") is True:
            self.polling_work_order_public_relay_test_status.setText(
                f"连接正常 · 协议 v{int(result.get('protocol_version') or 0)} · {elapsed:.0f}ms"
            )
            return
        self.polling_work_order_public_relay_test_status.setText(
            f"连接失败 · {str((result or {}).get('error') or '服务未就绪')}"
        )

    def save_settings(self):
        """保存设置"""
        feishu_app_id = self.app_id_input.text().strip()

        feishu_app_secret = self.app_secret_input.text().strip()

        feishu_app_token = self.app_token_input.text().strip()

        table_id_weibao = self.table_id_weibao_input.text().strip()

        table_id_biangeng = self.table_id_biangeng_input.text().strip()

        table_id_tiaozheng = self.table_id_tiaozheng_input.text().strip()

        table_id_shijian = self.table_id_shijian_input.text().strip()

        table_id_power = self.table_id_power_input.text().strip()

        table_id_polling = self.table_id_polling_input.text().strip()

        table_id_overhaul = self.table_id_overhaul_input.text().strip()


        group_name_change_i3 = self.group_name_change_i3_input.text().strip()

        group_name_maintenance = self.group_name_maintenance_input.text().strip()

        group_name_event_i2 = self.group_name_event_i2_input.text().strip()

        group_name_event_i3 = self.group_name_event_i3_input.text().strip()

        group_name_event_prompt = self.group_name_event_prompt_input.text().strip()
        lan_template_portal_host = (
            self.lan_template_portal_host_input.text().strip()
            or DEFAULT_LAN_TEMPLATE_PORTAL_HOST
        )
        lan_template_public_host = self.lan_template_public_host_input.text().strip()
        lan_low_performance_mode = self.lan_low_performance_checkbox.isChecked()
        polling_work_order_public_relay_enabled = (
            self.polling_work_order_public_relay_checkbox.isChecked()
        )
        polling_work_order_public_relay_url = (
            self.polling_work_order_public_relay_url_input.text().strip().rstrip("/")
            or DEFAULT_POLLING_WORK_ORDER_PUBLIC_RELAY_URL
        )
        relay_url_block_reason = config.polling_work_order_relay_url_change_block_reason(
            polling_work_order_public_relay_url
        )
        if relay_url_block_reason:
            show_toast_message(
                self,
                f"❌ {relay_url_block_reason}",
                duration_ms=3200,
            )
            return
        if not self._is_valid_lan_template_portal_host(lan_template_portal_host):
            show_toast_message(
                self,
                "❌ 局域网 IP 格式无效，请填写如 192.168.1.20 或 0.0.0.0",
                duration_ms=2400,
            )
            return
        if lan_template_public_host and not self._is_valid_lan_template_public_host(
            lan_template_public_host
        ):
            show_toast_message(
                self,
                "❌ 签名链接局域网地址格式无效，请填写如 192.168.224.130",
                duration_ms=2400,
            )
            return
        if (
            polling_work_order_public_relay_enabled
            and not self._is_valid_polling_work_order_public_relay_url(
                polling_work_order_public_relay_url
            )
        ):
            show_toast_message(
                self,
                "❌ 公网工单地址无效：局域网模拟请用私有 IP，正式环境请用 HTTPS 根地址",
                duration_ms=2800,
            )
            return

        disable_hot_reload = self.disable_hot_reload_checkbox.isChecked()
        disable_alerts = self.disable_alerts_checkbox.isChecked()
        disable_speech = self.disable_speech_checkbox.isChecked()
        resolved_token, replaced = resolve_bitable_app_token(

            feishu_app_id, feishu_app_secret, feishu_app_token

        )

        if replaced:

            feishu_app_token = resolved_token

            self.app_token_input.setText(resolved_token)



        if config.save(

            app_id=feishu_app_id,

            app_secret=feishu_app_secret,

            app_token=feishu_app_token,

            table_id_weibao=table_id_weibao,

            table_id_biangeng=table_id_biangeng,

            table_id_tiaozheng=table_id_tiaozheng,

            table_id_shijian=table_id_shijian,

            table_id_power=table_id_power,

            table_id_polling=table_id_polling,

            table_id_overhaul=table_id_overhaul,
group_name_change_i3=group_name_change_i3,

            group_name_maintenance=group_name_maintenance,

            group_name_event_i2=group_name_event_i2,

            group_name_event_i3=group_name_event_i3,

            group_name_event_prompt=group_name_event_prompt,
            lan_template_portal_host=lan_template_portal_host,
            lan_template_portal_port=DEFAULT_LAN_TEMPLATE_PORTAL_PORT,
            lan_template_public_host=lan_template_public_host,
            lan_low_performance_mode=lan_low_performance_mode,
            polling_work_order_public_relay_enabled=(
                polling_work_order_public_relay_enabled
            ),
            polling_work_order_public_relay_url=polling_work_order_public_relay_url,
            disable_hot_reload=disable_hot_reload,
            disable_alerts=disable_alerts,
            disable_speech=disable_speech,
        ):
            show_toast_message(self, "✅ 设置已保存", duration_ms=1500)
            self.hide()
            QTimer.singleShot(0, self.settings_saved.emit)

    def reset_to_default(self):
        """恢复默认设置"""
        from ..config import DEFAULT_FEISHU_APP_ID, DEFAULT_FEISHU_APP_SECRET



        self.app_id_input.setText(DEFAULT_FEISHU_APP_ID)

        self.app_secret_input.setText(DEFAULT_FEISHU_APP_SECRET)

        self.app_token_input.setText(DEFAULT_FEISHU_APP_TOKEN)

        self.table_id_weibao_input.setText(DEFAULT_TABLE_ID_WEIBAO)

        self.table_id_biangeng_input.setText(DEFAULT_TABLE_ID_BIANGENG)

        self.table_id_tiaozheng_input.setText(DEFAULT_TABLE_ID_TIAOZHENG)

        self.table_id_shijian_input.setText(DEFAULT_TABLE_ID_SHIJIAN)

        self.table_id_power_input.setText(DEFAULT_TABLE_ID_POWER)

        self.table_id_polling_input.setText(DEFAULT_TABLE_ID_POLLING)
        self.table_id_overhaul_input.setText(DEFAULT_TABLE_ID_OVERHAUL)
        self.disable_hot_reload_checkbox.setChecked(DEFAULT_DISABLE_HOT_RELOAD)
        self.disable_alerts_checkbox.setChecked(DEFAULT_DISABLE_ALERTS)
        self.disable_speech_checkbox.setChecked(DEFAULT_DISABLE_SPEECH)
        self.group_name_change_i3_input.setText(DEFAULT_GROUP_NAME_CHANGE_I3)

        self.group_name_maintenance_input.setText(DEFAULT_GROUP_NAME_MAINTENANCE)

        self.group_name_event_i2_input.setText(DEFAULT_GROUP_NAME_EVENT_I2)

        self.group_name_event_i3_input.setText(DEFAULT_GROUP_NAME_EVENT_I3)

        self.group_name_event_prompt_input.setText(DEFAULT_GROUP_NAME_EVENT_PROMPT)
        self.lan_template_portal_host_input.setText(DEFAULT_LAN_TEMPLATE_PORTAL_HOST)
        self.lan_template_public_host_input.setText(DEFAULT_LAN_TEMPLATE_PUBLIC_HOST)
        self.lan_low_performance_checkbox.setChecked(DEFAULT_LAN_LOW_PERFORMANCE_MODE)
        self.polling_work_order_public_relay_checkbox.setChecked(
            DEFAULT_POLLING_WORK_ORDER_PUBLIC_RELAY_ENABLED
        )
        self.polling_work_order_public_relay_url_input.setText(
            DEFAULT_POLLING_WORK_ORDER_PUBLIC_RELAY_URL
        )



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



    def mouseReleaseEvent(self, event):

        self.drag_position = None






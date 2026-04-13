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
    QComboBox,
)
from PyQt6.QtCore import Qt

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

    DEFAULT_GROUP_NAME_EVENT_I2,

    DEFAULT_GROUP_NAME_EVENT_I3,

    DEFAULT_GROUP_NAME_EVENT_PROMPT,
    DEFAULT_DISABLE_HOT_RELOAD,
    DEFAULT_DISABLE_ALERTS,
    DEFAULT_DISABLE_SPEECH,
    DEFAULT_RELAY_ENABLED,
    DEFAULT_RELAY_WEBHOOK,
    DEFAULT_RELAY_WEBHOOK_FORMAT,
)
from ..services.service_registry import resolve_bitable_app_token

from .common import show_toast_message





class SettingsDialog(QDialog):
    # 配置保存成功信号
    from PyQt6.QtCore import pyqtSignal



    settings_saved = pyqtSignal()
    webhook_test_finished = pyqtSignal(bool, str)


    def __init__(self, parent=None):

        super().__init__(parent)

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



        # 设备变更 Table ID
        biangeng_label = QLabel("设备变更 Table ID：")
        self.table_id_biangeng_input = QLineEdit()

        self.table_id_biangeng_input.setPlaceholderText("输入设备变更表格 ID")
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

        # ========== 事件中转告警 ==========
        relay_title = QLabel("事件中转告警")
        relay_title.setStyleSheet("font-weight: bold; color: #0EA5E9; margin-top: 10px;")
        form_layout.addWidget(relay_title)
        self.relay_enabled_checkbox = QCheckBox("启用事件中转服务（关闭后不接收中转事件）")
        form_layout.addWidget(self.relay_enabled_checkbox)

        relay_card = QFrame()
        relay_card.setObjectName("RelayCard")
        relay_card.setStyleSheet(
            "QFrame#RelayCard {"
            "background: rgba(14, 165, 233, 0.08);"
            "border: 1px solid rgba(14, 165, 233, 0.28);"
            "border-radius: 10px;"
            "}"
            "QLabel#RelayHint { color: #4B5563; font-size: 12px; }"
        )
        relay_card_layout = QVBoxLayout(relay_card)
        relay_card_layout.setContentsMargins(12, 10, 12, 10)
        relay_card_layout.setSpacing(8)

        relay_webhook_label = QLabel("告警 Webhook URL")
        self.relay_webhook_input = QLineEdit()
        self.relay_webhook_input.setPlaceholderText(
            "https://open.feishu.cn/... 或 https://oapi.dingtalk.com/robot/send..."
        )
        self.relay_webhook_input.setMinimumHeight(34)
        relay_card_layout.addWidget(relay_webhook_label)
        relay_card_layout.addWidget(self.relay_webhook_input)

        relay_bottom_layout = QHBoxLayout()
        relay_bottom_layout.setSpacing(8)
        relay_format_label = QLabel("类型")
        self.relay_webhook_format_combo = QComboBox()
        self.relay_webhook_format_combo.addItem("飞书机器人", "feishu")
        self.relay_webhook_format_combo.addItem("钉钉机器人", "dingtalk")
        self.relay_webhook_format_combo.setMinimumHeight(34)
        self.relay_webhook_test_btn = QPushButton("测试 Webhook")
        self.relay_webhook_test_btn.setObjectName("ConfirmBtn")
        self.relay_webhook_test_btn.setMinimumHeight(34)
        self.relay_webhook_test_btn.clicked.connect(self.test_relay_webhook)
        relay_bottom_layout.addWidget(relay_format_label)
        relay_bottom_layout.addWidget(self.relay_webhook_format_combo, 1)
        relay_bottom_layout.addWidget(self.relay_webhook_test_btn)
        relay_card_layout.addLayout(relay_bottom_layout)

        relay_hint_label = QLabel("测试消息会包含安全词“事件通告”，用于验证机器人连通性。")
        relay_hint_label.setObjectName("RelayHint")
        relay_hint_label.setWordWrap(True)
        relay_card_layout.addWidget(relay_hint_label)
        form_layout.addWidget(relay_card)


        # ========== 群机器人配置 ==========
        robot_title = QLabel("群机器人配置：")
        robot_title.setStyleSheet("font-weight: bold; color: #EC4899; margin-top: 10px;")

        form_layout.addWidget(robot_title)

        change_i3_label = QLabel("I3变更群名称：")
        self.group_name_change_i3_input = QLineEdit()

        self.group_name_change_i3_input.setPlaceholderText("输入群名称（需与飞书群名一致）")
        form_layout.addWidget(change_i3_label)

        form_layout.addWidget(self.group_name_change_i3_input)



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
        self.webhook_test_finished.connect(self._on_webhook_test_finished)
        self.relay_webhook_input.textChanged.connect(self._sync_relay_test_btn_state)
        self._sync_relay_test_btn_state()


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
        self.relay_enabled_checkbox.setChecked(bool(getattr(config, "relay_enabled", False)))
        self.relay_webhook_input.setText(getattr(config, "relay_webhook", ""))
        fmt = getattr(config, "relay_webhook_format", "feishu")
        idx = self.relay_webhook_format_combo.findData(fmt)
        if idx >= 0:
            self.relay_webhook_format_combo.setCurrentIndex(idx)
        self._sync_relay_test_btn_state()
        self.group_name_change_i3_input.setText(config.group_name_change_i3)
        self.group_name_event_i2_input.setText(config.group_name_event_i2)
        self.group_name_event_i3_input.setText(config.group_name_event_i3)
        self.group_name_event_prompt_input.setText(config.group_name_event_prompt)

    def _sync_relay_test_btn_state(self):
        webhook = self.relay_webhook_input.text().strip()
        self.relay_webhook_test_btn.setEnabled(bool(webhook))


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

        group_name_event_i2 = self.group_name_event_i2_input.text().strip()

        group_name_event_i3 = self.group_name_event_i3_input.text().strip()

        group_name_event_prompt = self.group_name_event_prompt_input.text().strip()

        disable_hot_reload = self.disable_hot_reload_checkbox.isChecked()
        disable_alerts = self.disable_alerts_checkbox.isChecked()
        disable_speech = self.disable_speech_checkbox.isChecked()
        relay_enabled = self.relay_enabled_checkbox.isChecked()
        relay_webhook = self.relay_webhook_input.text().strip()
        relay_webhook_format = self.relay_webhook_format_combo.currentData()


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

            group_name_event_i2=group_name_event_i2,

            group_name_event_i3=group_name_event_i3,

            group_name_event_prompt=group_name_event_prompt,
            disable_hot_reload=disable_hot_reload,
            disable_alerts=disable_alerts,
            disable_speech=disable_speech,
            relay_enabled=relay_enabled,
            relay_webhook=relay_webhook,
            relay_webhook_format=relay_webhook_format,
        ):
            show_toast_message(self, "✅ 设置已保存", duration_ms=1500)
            self.settings_saved.emit()

            self.hide()



    def test_relay_webhook(self):
        webhook = self.relay_webhook_input.text().strip()
        webhook_format = self.relay_webhook_format_combo.currentData() or "feishu"
        if not webhook:
            show_toast_message(self, "❌ 请先填写 Webhook URL", duration_ms=1800)
            return

        self.relay_webhook_test_btn.setEnabled(False)
        self.relay_webhook_test_btn.setText("测试发送中...")

        def _worker():
            import json
            import urllib.error
            import urllib.request

            text = "事件通告 Webhook测试：这是一条连通性测试消息。"
            if webhook_format == "dingtalk":
                payload = {"msgtype": "text", "text": {"content": text}}
            else:
                payload = {"msg_type": "text", "content": {"text": text}}

            ok = False
            result = ""
            try:
                data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                req = urllib.request.Request(
                    webhook,
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=8) as resp:
                    body = resp.read().decode("utf-8", errors="ignore")
                    ok = 200 <= resp.status < 300
                    result = f"HTTP {resp.status} {body[:120]}".strip()
            except urllib.error.HTTPError as exc:
                try:
                    body = exc.read().decode("utf-8", errors="ignore")
                except Exception:
                    body = ""
                result = f"HTTP {exc.code} {body[:120]}".strip()
            except Exception as exc:
                result = str(exc)

            self.webhook_test_finished.emit(ok, result)

        import threading

        threading.Thread(target=_worker, daemon=True).start()

    def _on_webhook_test_finished(self, ok: bool, result: str):
        self.relay_webhook_test_btn.setText("测试 Webhook")
        self._sync_relay_test_btn_state()
        if ok:
            show_toast_message(self, "✅ Webhook 测试发送成功", duration_ms=1800)
        else:
            show_toast_message(
                self, f"❌ Webhook 测试失败: {result}", duration_ms=2600
            )

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
        self.relay_enabled_checkbox.setChecked(DEFAULT_RELAY_ENABLED)
        self.relay_webhook_input.setText(DEFAULT_RELAY_WEBHOOK)
        idx = self.relay_webhook_format_combo.findData(DEFAULT_RELAY_WEBHOOK_FORMAT)
        if idx >= 0:
            self.relay_webhook_format_combo.setCurrentIndex(idx)
        self._sync_relay_test_btn_state()
        self.group_name_change_i3_input.setText(DEFAULT_GROUP_NAME_CHANGE_I3)

        self.group_name_event_i2_input.setText(DEFAULT_GROUP_NAME_EVENT_I2)

        self.group_name_event_i3_input.setText(DEFAULT_GROUP_NAME_EVENT_I3)

        self.group_name_event_prompt_input.setText(DEFAULT_GROUP_NAME_EVENT_PROMPT)



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






import os
import json
import os
from .utils import (
    get_data_file_path,
    migrate_legacy_data_file,
)
from .logger import log_info, log_error

# 飞书 SDK 默认配置
DEFAULT_FEISHU_APP_TOKEN = ""
DEFAULT_FEISHU_USER_TOKEN = ""
DEFAULT_FEISHU_APP_ID = ""
DEFAULT_FEISHU_APP_SECRET = ""
DEFAULT_DISABLE_HOT_RELOAD = False
DEFAULT_DISABLE_ALERTS = False
DEFAULT_DISABLE_SPEECH = True
DEFAULT_DISABLE_CLIPBOARD_LISTENER = False
DEFAULT_RELAY_ENABLED = False
DEFAULT_RELAY_WEBHOOK = ""
DEFAULT_RELAY_WEBHOOK_FORMAT = "feishu"
DEFAULT_RELAY_BIND_HOST = "0.0.0.0"
DEFAULT_RELAY_ALLOW_LAN = False
DEFAULT_RELAY_ALLOWED_ORIGINS = [
    "http://127.0.0.1:62345",
    "http://localhost:62345",
]
DEFAULT_RELAY_ENABLE_PROXY_DINGTALK = False
DEFAULT_REMOTE_UPDATE_ENABLED = True
DEFAULT_REMOTE_UPDATE_INTERVAL_SECONDS = 3600
DEFAULT_REMOTE_UPDATE_MANIFEST_URL = (
    "https://gitee.com/myligitt/test/raw/master/updates/latest_patch.json"
)
DEFAULT_REMOTE_UPDATE_AUTO_APPLY_NON_UI = True
DEFAULT_AUTO_INSTALL_DEPENDENCIES = True
DEFAULT_DEPENDENCY_MIRRORS = [
    "https://pypi.tuna.tsinghua.edu.cn/simple",
    "https://mirrors.aliyun.com/pypi/simple",
    "https://pypi.mirrors.ustc.edu.cn/simple",
]
DEFAULT_DEPENDENCY_INSTALL_TIMEOUT_SECONDS = 20
DEFAULT_DEPENDENCY_INSTALL_RETRIES_PER_MIRROR = 1
DEFAULT_DEPENDENCY_BOOTSTRAP_ALLOW_GET_PIP = True

# 3个表格ID配置（根据通告类型使用不同表格）
DEFAULT_TABLE_ID_WEIBAO = ""  # 维保通告表格ID
DEFAULT_TABLE_ID_BIANGENG = ""  # 设备变更表格ID
DEFAULT_TABLE_ID_TIAOZHENG = ""  # 设备调整表格ID
DEFAULT_TABLE_ID_SHIJIAN = ""  # 事件通告表格ID
DEFAULT_TABLE_ID_POWER = ""  # 上下电通告表格ID
DEFAULT_TABLE_ID_POLLING = ""  # 设备轮巡表格ID
DEFAULT_TABLE_ID_OVERHAUL = ""  # 设备检修表格ID
DEFAULT_GROUP_NAME_CHANGE_I3 = ""  # I3变更群名称
DEFAULT_GROUP_NAME_MAINTENANCE = ""  # 维保群名称
DEFAULT_GROUP_NAME_EVENT_I2 = ""  # I2事件群名称
DEFAULT_GROUP_NAME_EVENT_I3 = ""  # I3事件群名称
DEFAULT_GROUP_NAME_EVENT_PROMPT = ""  # 事件提示群名称
DEFAULT_LAN_TEMPLATE_PORTAL_HOST = "0.0.0.0"  # 局域网模板页面监听IP
DEFAULT_LAN_TEMPLATE_PORTAL_PORT = 18766  # 局域网模板页面默认端口

CONFIG_FILE = get_data_file_path("config.json")

class ConfigManager:
    def __init__(self):
        self.app_token = DEFAULT_FEISHU_APP_TOKEN
        self.user_token = DEFAULT_FEISHU_USER_TOKEN
        self.app_id = DEFAULT_FEISHU_APP_ID
        self.app_secret = DEFAULT_FEISHU_APP_SECRET
        self.token_expire_time = 0
        # 3个表格ID
        self.table_id_weibao = DEFAULT_TABLE_ID_WEIBAO
        self.table_id_biangeng = DEFAULT_TABLE_ID_BIANGENG
        self.table_id_tiaozheng = DEFAULT_TABLE_ID_TIAOZHENG
        self.table_id_shijian = DEFAULT_TABLE_ID_SHIJIAN
        self.table_id_power = DEFAULT_TABLE_ID_POWER
        self.table_id_polling = DEFAULT_TABLE_ID_POLLING
        self.table_id_overhaul = DEFAULT_TABLE_ID_OVERHAUL
        self.disable_hot_reload = DEFAULT_DISABLE_HOT_RELOAD
        self.disable_alerts = DEFAULT_DISABLE_ALERTS
        self.disable_speech = DEFAULT_DISABLE_SPEECH
        self.disable_clipboard_listener = DEFAULT_DISABLE_CLIPBOARD_LISTENER
        self.relay_enabled = DEFAULT_RELAY_ENABLED
        self.relay_webhook = DEFAULT_RELAY_WEBHOOK
        self.relay_webhook_format = DEFAULT_RELAY_WEBHOOK_FORMAT
        self.relay_bind_host = DEFAULT_RELAY_BIND_HOST
        self.relay_allow_lan = DEFAULT_RELAY_ALLOW_LAN
        self.relay_allowed_origins = list(DEFAULT_RELAY_ALLOWED_ORIGINS)
        self.relay_enable_proxy_dingtalk = DEFAULT_RELAY_ENABLE_PROXY_DINGTALK
        self.remote_update_enabled = DEFAULT_REMOTE_UPDATE_ENABLED
        self.remote_update_interval_seconds = DEFAULT_REMOTE_UPDATE_INTERVAL_SECONDS
        self.remote_update_manifest_url = DEFAULT_REMOTE_UPDATE_MANIFEST_URL
        self.remote_update_auto_apply_non_ui = (
            DEFAULT_REMOTE_UPDATE_AUTO_APPLY_NON_UI
        )
        self.auto_install_dependencies = DEFAULT_AUTO_INSTALL_DEPENDENCIES
        self.dependency_mirrors = list(DEFAULT_DEPENDENCY_MIRRORS)
        self.dependency_install_timeout_seconds = (
            DEFAULT_DEPENDENCY_INSTALL_TIMEOUT_SECONDS
        )
        self.dependency_install_retries_per_mirror = (
            DEFAULT_DEPENDENCY_INSTALL_RETRIES_PER_MIRROR
        )
        self.dependency_bootstrap_allow_get_pip = (
            DEFAULT_DEPENDENCY_BOOTSTRAP_ALLOW_GET_PIP
        )
        self.group_name_change_i3 = DEFAULT_GROUP_NAME_CHANGE_I3
        self.group_name_maintenance = DEFAULT_GROUP_NAME_MAINTENANCE
        self.group_name_event_i2 = DEFAULT_GROUP_NAME_EVENT_I2
        self.group_name_event_i3 = DEFAULT_GROUP_NAME_EVENT_I3
        self.group_name_event_prompt = DEFAULT_GROUP_NAME_EVENT_PROMPT
        self.lan_template_portal_host = DEFAULT_LAN_TEMPLATE_PORTAL_HOST
        self.lan_template_portal_port = DEFAULT_LAN_TEMPLATE_PORTAL_PORT
        migrate_legacy_data_file("config.json")
        self.load()

    def get_table_id(self, notice_type):
        """根据通告类型获取对应的表格ID"""
        from .core.parser import (
            NOTICE_TYPE_WEIBAO,
            NOTICE_TYPE_BIANGENG,
            NOTICE_TYPE_TIAOZHENG,
            NOTICE_TYPE_SHIJIAN,
        )

        table_map = {
            NOTICE_TYPE_WEIBAO: self.table_id_weibao,
            NOTICE_TYPE_BIANGENG: self.table_id_biangeng,
            NOTICE_TYPE_TIAOZHENG: self.table_id_tiaozheng,
            NOTICE_TYPE_SHIJIAN: self.table_id_shijian,
            "变更通告": self.table_id_biangeng,
            "上下电通告": self.table_id_power,
            "设备轮巡": self.table_id_polling,
            "设备检修": self.table_id_overhaul,
        }
        return table_map.get(notice_type, "")

    def load(self):
        """加载配置文件"""
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    config_data = json.load(f)
                    self.app_token = config_data.get(
                        "feishu_app_token", self.app_token
                    )
                    self.user_token = config_data.get(
                        "feishu_user_token", DEFAULT_FEISHU_USER_TOKEN
                    )
                    self.app_id = config_data.get(
                        "feishu_app_id", self.app_id
                    )
                    self.app_secret = config_data.get(
                        "feishu_app_secret", self.app_secret
                    )
                    self.token_expire_time = config_data.get("token_expire_time", 0)
                    # 加载3个表格ID
                    self.table_id_weibao = config_data.get(
                        "table_id_weibao", DEFAULT_TABLE_ID_WEIBAO
                    )
                    self.table_id_biangeng = config_data.get(
                        "table_id_biangeng", DEFAULT_TABLE_ID_BIANGENG
                    )
                    self.table_id_tiaozheng = config_data.get(
                        "table_id_tiaozheng", DEFAULT_TABLE_ID_TIAOZHENG
                    )
                    self.table_id_shijian = config_data.get(
                        "table_id_shijian", DEFAULT_TABLE_ID_SHIJIAN
                    )
                    self.table_id_power = config_data.get(
                        "table_id_power", DEFAULT_TABLE_ID_POWER
                    )
                    self.table_id_polling = config_data.get(
                        "table_id_polling", DEFAULT_TABLE_ID_POLLING
                    )
                    self.table_id_overhaul = config_data.get(
                        "table_id_overhaul", DEFAULT_TABLE_ID_OVERHAUL
                    )
                    self.disable_hot_reload = config_data.get(
                        "disable_hot_reload", DEFAULT_DISABLE_HOT_RELOAD
                    )
                    self.disable_alerts = config_data.get(
                        "disable_alerts", DEFAULT_DISABLE_ALERTS
                    )
                    self.disable_speech = bool(
                        config_data.get("disable_speech", DEFAULT_DISABLE_SPEECH)
                    )
                    self.disable_clipboard_listener = config_data.get(
                        "disable_clipboard_listener",
                        DEFAULT_DISABLE_CLIPBOARD_LISTENER,
                    )
                    self.relay_enabled = bool(
                        config_data.get("relay_enabled", DEFAULT_RELAY_ENABLED)
                    )
                    self.relay_webhook = config_data.get(
                        "relay_webhook", DEFAULT_RELAY_WEBHOOK
                    )
                    self.relay_webhook_format = config_data.get(
                        "relay_webhook_format", DEFAULT_RELAY_WEBHOOK_FORMAT
                    )
                    self.relay_bind_host = config_data.get(
                        "relay_bind_host", DEFAULT_RELAY_BIND_HOST
                    )
                    self.relay_allow_lan = bool(
                        config_data.get("relay_allow_lan", DEFAULT_RELAY_ALLOW_LAN)
                    )
                    self.relay_allowed_origins = config_data.get(
                        "relay_allowed_origins",
                        list(DEFAULT_RELAY_ALLOWED_ORIGINS),
                    )
                    if not isinstance(self.relay_allowed_origins, list):
                        self.relay_allowed_origins = list(DEFAULT_RELAY_ALLOWED_ORIGINS)
                    self.relay_enable_proxy_dingtalk = bool(
                        config_data.get(
                            "relay_enable_proxy_dingtalk",
                            DEFAULT_RELAY_ENABLE_PROXY_DINGTALK,
                        )
                    )
                    self.remote_update_enabled = bool(
                        config_data.get(
                            "remote_update_enabled",
                            DEFAULT_REMOTE_UPDATE_ENABLED,
                        )
                    )
                    _interval_raw = config_data.get(
                        "remote_update_interval_seconds",
                        DEFAULT_REMOTE_UPDATE_INTERVAL_SECONDS,
                    )
                    try:
                        _interval_value = int(_interval_raw)
                    except Exception:
                        _interval_value = DEFAULT_REMOTE_UPDATE_INTERVAL_SECONDS
                    self.remote_update_interval_seconds = max(60, _interval_value)
                    self.remote_update_manifest_url = str(
                        config_data.get(
                            "remote_update_manifest_url",
                            DEFAULT_REMOTE_UPDATE_MANIFEST_URL,
                        )
                        or ""
                    ).strip()
                    self.remote_update_auto_apply_non_ui = bool(
                        config_data.get(
                            "remote_update_auto_apply_non_ui",
                            DEFAULT_REMOTE_UPDATE_AUTO_APPLY_NON_UI,
                        )
                    )
                    self.auto_install_dependencies = bool(
                        config_data.get(
                            "auto_install_dependencies",
                            DEFAULT_AUTO_INSTALL_DEPENDENCIES,
                        )
                    )
                    self.dependency_mirrors = config_data.get(
                        "dependency_mirrors",
                        list(DEFAULT_DEPENDENCY_MIRRORS),
                    )
                    if not isinstance(self.dependency_mirrors, list):
                        self.dependency_mirrors = list(DEFAULT_DEPENDENCY_MIRRORS)
                    _dep_timeout_raw = config_data.get(
                        "dependency_install_timeout_seconds",
                        DEFAULT_DEPENDENCY_INSTALL_TIMEOUT_SECONDS,
                    )
                    try:
                        _dep_timeout = int(_dep_timeout_raw)
                    except Exception:
                        _dep_timeout = DEFAULT_DEPENDENCY_INSTALL_TIMEOUT_SECONDS
                    self.dependency_install_timeout_seconds = max(5, _dep_timeout)
                    _dep_retry_raw = config_data.get(
                        "dependency_install_retries_per_mirror",
                        DEFAULT_DEPENDENCY_INSTALL_RETRIES_PER_MIRROR,
                    )
                    try:
                        _dep_retries = int(_dep_retry_raw)
                    except Exception:
                        _dep_retries = DEFAULT_DEPENDENCY_INSTALL_RETRIES_PER_MIRROR
                    self.dependency_install_retries_per_mirror = max(1, _dep_retries)
                    self.dependency_bootstrap_allow_get_pip = bool(
                        config_data.get(
                            "dependency_bootstrap_allow_get_pip",
                            DEFAULT_DEPENDENCY_BOOTSTRAP_ALLOW_GET_PIP,
                        )
                    )
                    self.group_name_change_i3 = config_data.get(
                        "group_name_change_i3", DEFAULT_GROUP_NAME_CHANGE_I3
                    )
                    self.group_name_maintenance = config_data.get(
                        "group_name_maintenance", DEFAULT_GROUP_NAME_MAINTENANCE
                    )
                    self.group_name_event_i2 = config_data.get(
                        "group_name_event_i2", DEFAULT_GROUP_NAME_EVENT_I2
                    )
                    self.group_name_event_i3 = config_data.get(
                        "group_name_event_i3", DEFAULT_GROUP_NAME_EVENT_I3
                    )
                    self.group_name_event_prompt = config_data.get(
                        "group_name_event_prompt", DEFAULT_GROUP_NAME_EVENT_PROMPT
                    )
                    self.lan_template_portal_host = str(
                        config_data.get(
                            "lan_template_portal_host",
                            DEFAULT_LAN_TEMPLATE_PORTAL_HOST,
                        )
                        or DEFAULT_LAN_TEMPLATE_PORTAL_HOST
                    ).strip()
                    try:
                        self.lan_template_portal_port = int(
                            config_data.get(
                                "lan_template_portal_port",
                                DEFAULT_LAN_TEMPLATE_PORTAL_PORT,
                            )
                        )
                    except Exception:
                        self.lan_template_portal_port = DEFAULT_LAN_TEMPLATE_PORTAL_PORT
                    if self.lan_template_portal_port <= 0:
                        self.lan_template_portal_port = DEFAULT_LAN_TEMPLATE_PORTAL_PORT
                    log_info("系统: 配置文件加载成功")
        except Exception as e:
            log_error(f"系统: 配置文件加载失败: {e}")

    def save(
        self,
        app_token=None,
        user_token=None,
        app_id=None,
        app_secret=None,
        token_expire_time=None,
        table_id_weibao=None,
        table_id_biangeng=None,
        table_id_tiaozheng=None,
        table_id_shijian=None,
        table_id_power=None,
        table_id_polling=None,
        table_id_overhaul=None,
        group_name_change_i3=None,
        group_name_maintenance=None,
        group_name_event_i2=None,
        group_name_event_i3=None,
        group_name_event_prompt=None,
        lan_template_portal_host=None,
        lan_template_portal_port=None,
        disable_hot_reload=None,
        disable_alerts=None,
        disable_speech=None,
        disable_clipboard_listener=None,
        relay_enabled=None,
        relay_webhook=None,
        relay_webhook_format=None,
        relay_bind_host=None,
        relay_allow_lan=None,
        relay_allowed_origins=None,
        relay_enable_proxy_dingtalk=None,
        remote_update_enabled=None,
        remote_update_interval_seconds=None,
        remote_update_manifest_url=None,
        remote_update_auto_apply_non_ui=None,
        auto_install_dependencies=None,
        dependency_mirrors=None,
        dependency_install_timeout_seconds=None,
        dependency_install_retries_per_mirror=None,
        dependency_bootstrap_allow_get_pip=None,
    ):
        """保存配置文件"""
        try:
            # 使用现有值作为默认值
            new_app_token = app_token if app_token is not None else self.app_token
            new_user_token = user_token if user_token is not None else self.user_token
            new_app_id = app_id if app_id is not None else self.app_id
            new_app_secret = app_secret if app_secret is not None else self.app_secret
            new_expire_time = (
                token_expire_time
                if token_expire_time is not None
                else self.token_expire_time
            )
            new_table_id_weibao = (
                table_id_weibao if table_id_weibao is not None else self.table_id_weibao
            )
            new_table_id_biangeng = (
                table_id_biangeng
                if table_id_biangeng is not None
                else self.table_id_biangeng
            )
            new_table_id_tiaozheng = (
                table_id_tiaozheng
                if table_id_tiaozheng is not None
                else self.table_id_tiaozheng
            )
            new_table_id_shijian = (
                table_id_shijian
                if table_id_shijian is not None
                else self.table_id_shijian
            )
            new_table_id_power = (
                table_id_power if table_id_power is not None else self.table_id_power
            )
            new_table_id_polling = (
                table_id_polling
                if table_id_polling is not None
                else self.table_id_polling
            )
            new_table_id_overhaul = (
                table_id_overhaul
                if table_id_overhaul is not None
                else self.table_id_overhaul
            )
            new_group_name_change_i3 = (
                group_name_change_i3
                if group_name_change_i3 is not None
                else self.group_name_change_i3
            )
            new_group_name_maintenance = (
                group_name_maintenance
                if group_name_maintenance is not None
                else self.group_name_maintenance
            )
            new_group_name_event_i2 = (
                group_name_event_i2
                if group_name_event_i2 is not None
                else self.group_name_event_i2
            )
            new_group_name_event_i3 = (
                group_name_event_i3
                if group_name_event_i3 is not None
                else self.group_name_event_i3
            )
            new_group_name_event_prompt = (
                group_name_event_prompt
                if group_name_event_prompt is not None
                else self.group_name_event_prompt
            )
            new_lan_template_portal_host = (
                str(lan_template_portal_host).strip()
                if lan_template_portal_host is not None
                else self.lan_template_portal_host
            )
            if not new_lan_template_portal_host:
                new_lan_template_portal_host = DEFAULT_LAN_TEMPLATE_PORTAL_HOST
            if lan_template_portal_port is not None:
                try:
                    new_lan_template_portal_port = int(lan_template_portal_port)
                except Exception:
                    new_lan_template_portal_port = DEFAULT_LAN_TEMPLATE_PORTAL_PORT
            else:
                new_lan_template_portal_port = self.lan_template_portal_port
            if new_lan_template_portal_port <= 0:
                new_lan_template_portal_port = DEFAULT_LAN_TEMPLATE_PORTAL_PORT
            new_disable_hot_reload = (
                disable_hot_reload
                if disable_hot_reload is not None
                else self.disable_hot_reload
            )
            new_disable_alerts = (
                disable_alerts if disable_alerts is not None else self.disable_alerts
            )
            new_disable_speech = (
                bool(disable_speech)
                if disable_speech is not None
                else self.disable_speech
            )
            new_disable_clipboard_listener = (
                disable_clipboard_listener
                if disable_clipboard_listener is not None
                else self.disable_clipboard_listener
            )
            new_relay_enabled = (
                bool(relay_enabled)
                if relay_enabled is not None
                else self.relay_enabled
            )
            new_relay_webhook = (
                relay_webhook if relay_webhook is not None else self.relay_webhook
            )
            new_relay_webhook_format = (
                relay_webhook_format
                if relay_webhook_format is not None
                else self.relay_webhook_format
            )
            new_relay_bind_host = (
                relay_bind_host
                if relay_bind_host is not None
                else self.relay_bind_host
            )
            new_relay_allow_lan = (
                bool(relay_allow_lan)
                if relay_allow_lan is not None
                else self.relay_allow_lan
            )
            new_relay_allowed_origins = (
                relay_allowed_origins
                if relay_allowed_origins is not None
                else self.relay_allowed_origins
            )
            if not isinstance(new_relay_allowed_origins, list):
                new_relay_allowed_origins = list(DEFAULT_RELAY_ALLOWED_ORIGINS)
            new_relay_enable_proxy_dingtalk = (
                bool(relay_enable_proxy_dingtalk)
                if relay_enable_proxy_dingtalk is not None
                else self.relay_enable_proxy_dingtalk
            )
            new_remote_update_enabled = (
                bool(remote_update_enabled)
                if remote_update_enabled is not None
                else self.remote_update_enabled
            )
            if remote_update_interval_seconds is not None:
                try:
                    _new_interval_value = int(remote_update_interval_seconds)
                except Exception:
                    _new_interval_value = DEFAULT_REMOTE_UPDATE_INTERVAL_SECONDS
                new_remote_update_interval_seconds = max(60, _new_interval_value)
            else:
                new_remote_update_interval_seconds = self.remote_update_interval_seconds
            new_remote_update_manifest_url = (
                str(remote_update_manifest_url).strip()
                if remote_update_manifest_url is not None
                else self.remote_update_manifest_url
            )
            new_remote_update_auto_apply_non_ui = (
                bool(remote_update_auto_apply_non_ui)
                if remote_update_auto_apply_non_ui is not None
                else self.remote_update_auto_apply_non_ui
            )
            new_auto_install_dependencies = (
                bool(auto_install_dependencies)
                if auto_install_dependencies is not None
                else self.auto_install_dependencies
            )
            new_dependency_mirrors = (
                dependency_mirrors
                if dependency_mirrors is not None
                else self.dependency_mirrors
            )
            if not isinstance(new_dependency_mirrors, list):
                new_dependency_mirrors = list(DEFAULT_DEPENDENCY_MIRRORS)
            if dependency_install_timeout_seconds is not None:
                try:
                    _new_dep_timeout = int(dependency_install_timeout_seconds)
                except Exception:
                    _new_dep_timeout = DEFAULT_DEPENDENCY_INSTALL_TIMEOUT_SECONDS
                new_dependency_install_timeout_seconds = max(5, _new_dep_timeout)
            else:
                new_dependency_install_timeout_seconds = (
                    self.dependency_install_timeout_seconds
                )
            if dependency_install_retries_per_mirror is not None:
                try:
                    _new_dep_retries = int(dependency_install_retries_per_mirror)
                except Exception:
                    _new_dep_retries = DEFAULT_DEPENDENCY_INSTALL_RETRIES_PER_MIRROR
                new_dependency_install_retries_per_mirror = max(1, _new_dep_retries)
            else:
                new_dependency_install_retries_per_mirror = (
                    self.dependency_install_retries_per_mirror
                )
            new_dependency_bootstrap_allow_get_pip = (
                bool(dependency_bootstrap_allow_get_pip)
                if dependency_bootstrap_allow_get_pip is not None
                else self.dependency_bootstrap_allow_get_pip
            )

            cfg = {
                "feishu_app_token": new_app_token,
                "feishu_user_token": new_user_token,
                "feishu_app_id": new_app_id,
                "feishu_app_secret": new_app_secret,
                "token_expire_time": new_expire_time,
                "table_id_weibao": new_table_id_weibao,
                "table_id_biangeng": new_table_id_biangeng,
                "table_id_tiaozheng": new_table_id_tiaozheng,
                "table_id_shijian": new_table_id_shijian,
                "table_id_power": new_table_id_power,
                "table_id_polling": new_table_id_polling,
                "table_id_overhaul": new_table_id_overhaul,
                "group_name_change_i3": new_group_name_change_i3,
                "group_name_maintenance": new_group_name_maintenance,
                "group_name_event_i2": new_group_name_event_i2,
                "group_name_event_i3": new_group_name_event_i3,
                "group_name_event_prompt": new_group_name_event_prompt,
                "lan_template_portal_host": new_lan_template_portal_host,
                "lan_template_portal_port": new_lan_template_portal_port,
                "disable_hot_reload": new_disable_hot_reload,
                "disable_alerts": new_disable_alerts,
                "disable_speech": new_disable_speech,
                "disable_clipboard_listener": new_disable_clipboard_listener,
                "relay_enabled": new_relay_enabled,
                "relay_webhook": new_relay_webhook,
                "relay_webhook_format": new_relay_webhook_format,
                "relay_bind_host": new_relay_bind_host,
                "relay_allow_lan": new_relay_allow_lan,
                "relay_allowed_origins": new_relay_allowed_origins,
                "relay_enable_proxy_dingtalk": new_relay_enable_proxy_dingtalk,
                "remote_update_enabled": new_remote_update_enabled,
                "remote_update_interval_seconds": new_remote_update_interval_seconds,
                "remote_update_manifest_url": new_remote_update_manifest_url,
                "remote_update_auto_apply_non_ui": new_remote_update_auto_apply_non_ui,
                "auto_install_dependencies": new_auto_install_dependencies,
                "dependency_mirrors": new_dependency_mirrors,
                "dependency_install_timeout_seconds": (
                    new_dependency_install_timeout_seconds
                ),
                "dependency_install_retries_per_mirror": (
                    new_dependency_install_retries_per_mirror
                ),
                "dependency_bootstrap_allow_get_pip": (
                    new_dependency_bootstrap_allow_get_pip
                ),
            }
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)

            # 更新实例变量
            self.app_token = new_app_token
            self.user_token = new_user_token
            self.app_id = new_app_id
            self.app_secret = new_app_secret
            self.token_expire_time = new_expire_time
            self.table_id_weibao = new_table_id_weibao
            self.table_id_biangeng = new_table_id_biangeng
            self.table_id_tiaozheng = new_table_id_tiaozheng
            self.table_id_shijian = new_table_id_shijian
            self.table_id_power = new_table_id_power
            self.table_id_polling = new_table_id_polling
            self.table_id_overhaul = new_table_id_overhaul
            self.group_name_change_i3 = new_group_name_change_i3
            self.group_name_maintenance = new_group_name_maintenance
            self.group_name_event_i2 = new_group_name_event_i2
            self.group_name_event_i3 = new_group_name_event_i3
            self.group_name_event_prompt = new_group_name_event_prompt
            self.lan_template_portal_host = new_lan_template_portal_host
            self.lan_template_portal_port = new_lan_template_portal_port
            self.disable_hot_reload = new_disable_hot_reload
            self.disable_alerts = new_disable_alerts
            self.disable_speech = new_disable_speech
            self.disable_clipboard_listener = new_disable_clipboard_listener
            self.relay_enabled = new_relay_enabled
            self.relay_webhook = new_relay_webhook
            self.relay_webhook_format = new_relay_webhook_format
            self.relay_bind_host = new_relay_bind_host
            self.relay_allow_lan = new_relay_allow_lan
            self.relay_allowed_origins = new_relay_allowed_origins
            self.relay_enable_proxy_dingtalk = new_relay_enable_proxy_dingtalk
            self.remote_update_enabled = new_remote_update_enabled
            self.remote_update_interval_seconds = new_remote_update_interval_seconds
            self.remote_update_manifest_url = new_remote_update_manifest_url
            self.remote_update_auto_apply_non_ui = (
                new_remote_update_auto_apply_non_ui
            )
            self.auto_install_dependencies = new_auto_install_dependencies
            self.dependency_mirrors = new_dependency_mirrors
            self.dependency_install_timeout_seconds = (
                new_dependency_install_timeout_seconds
            )
            self.dependency_install_retries_per_mirror = (
                new_dependency_install_retries_per_mirror
            )
            self.dependency_bootstrap_allow_get_pip = (
                new_dependency_bootstrap_allow_get_pip
            )

            log_info("系统: 配置文件保存成功")
            return True
        except Exception as e:
            log_error(f"系统: 配置文件保存失败: {e}")
            return False

# ========== UI Constants ==========
CLIPBOARD_CHECK_INTERVAL_MS = 1000
SAVE_FEEDBACK_DELAY_MS = 1500

# ========== Status Values ==========
STATUS_NEW = "新增"
STATUS_START = "开始"
STATUS_UPDATE = "更新"
STATUS_END = "结束"
STATUS_VALUES = [STATUS_START, STATUS_NEW, STATUS_UPDATE, STATUS_END]

# ========== Dropdown Options ==========
OPTION_SLASH = "/"

LEVEL_I3 = "I3"
LEVEL_I2 = "I2"
LEVEL_I1 = "I1"
LEVEL_E4 = "E4"
LEVEL_E3 = "E3"
LEVEL_E2 = "E2"
LEVEL_E1 = "E1"
LEVEL_E0 = "E0"

EVENT_LEVEL_UPGRADE_I3_TO_I2 = "I3→I2（升级）"
EVENT_LEVEL_UPGRADE_I3_TO_I1 = "I3→I1（升级）"
EVENT_LEVEL_OPTIONS = [
    EVENT_LEVEL_UPGRADE_I3_TO_I2,
    EVENT_LEVEL_UPGRADE_I3_TO_I1,
    LEVEL_I3,
    LEVEL_I2,
    LEVEL_I1,
    LEVEL_E4,
    LEVEL_E3,
    LEVEL_E2,
    LEVEL_E1,
    LEVEL_E0,
    OPTION_SLASH,
]

EVENT_SOURCE_BMS = "BMS动环系统告警"
EVENT_SOURCE_BA = "BA系统告警"
EVENT_SOURCE_PATROL = "巡检发现"
EVENT_SOURCE_CUSTOMER = "客户侧通知"
EVENT_SOURCE_PPM = "PPM维护发现"
EVENT_SOURCE_FIRE = "消防主机"
EVENT_SOURCE_CCTV = "CCTV监控系统"
EVENT_SOURCE_ACCESS = "门禁系统"
EVENT_SOURCE_CHANGE = "变更发现"
EVENT_SOURCE_BMS_SYS = "BMS系统"
EVENT_SOURCE_DINGPING = "盯屏"

EVENT_SOURCE_OPTIONS = [
    EVENT_SOURCE_BMS,
    EVENT_SOURCE_BA,
    EVENT_SOURCE_PATROL,
    EVENT_SOURCE_CUSTOMER,
    EVENT_SOURCE_PPM,
    EVENT_SOURCE_FIRE,
    EVENT_SOURCE_CCTV,
    EVENT_SOURCE_ACCESS,
    EVENT_SOURCE_CHANGE,
    EVENT_SOURCE_BMS_SYS,
    EVENT_SOURCE_DINGPING,
    OPTION_SLASH,
]

CHANGE_ZHIHANG_LEVEL_OPTIONS = [LEVEL_I3, LEVEL_I2, LEVEL_I1, LEVEL_E0, OPTION_SLASH]

ALI_LEVEL_ULTRA_LOW = "超低"
ALI_LEVEL_LOW = "低"
ALI_LEVEL_MEDIUM = "中"
ALI_LEVEL_HIGH = "高"
CHANGE_ALI_LEVEL_OPTIONS = [
    ALI_LEVEL_ULTRA_LOW,
    ALI_LEVEL_LOW,
    ALI_LEVEL_MEDIUM,
    ALI_LEVEL_HIGH,
    OPTION_SLASH,
]

OVERHAUL_URGENCY_OPTIONS = [
    ALI_LEVEL_ULTRA_LOW,
    ALI_LEVEL_LOW,
    ALI_LEVEL_MEDIUM,
    ALI_LEVEL_HIGH,
    OPTION_SLASH,
]
OVERHAUL_DISCOVERY_OPTIONS = [
    "巡检",
    "BMS",
    "维护",
    "BA",
    "盯屏",
    "变更",
    "方舟",
    "消防主机",
    OPTION_SLASH,
]

SPECIALTY_ELECTRIC = "电气"
SPECIALTY_HVAC = "暖通"
SPECIALTY_FIRE = "消防"
SPECIALTY_WEAK = "弱电"
SPECIALTY_OTHER = "其他"
SPECIALTY_OPTIONS = [
    SPECIALTY_ELECTRIC,
    SPECIALTY_HVAC,
    SPECIALTY_FIRE,
    SPECIALTY_WEAK,
    SPECIALTY_OTHER,
    OPTION_SLASH,
]
POWER_DEFAULT_SPECIALTY = SPECIALTY_ELECTRIC
POLLING_DEFAULT_SPECIALTY = SPECIALTY_HVAC

BUILDING_A = "A楼"
BUILDING_B = "B楼"
BUILDING_C = "C楼"
BUILDING_D = "D楼"
BUILDING_E = "E楼"
BUILDING_H = "H楼"
BUILDING_110 = "110站"
BUILDING_PARK = "园区（ABCDE楼）"

BUILDING_DETECT_110_KEYWORDS = ["110站", "110KV"]
BUILDING_DETECT_ALIASES = [
    (BUILDING_A, BUILDING_A),
    ("A栋", BUILDING_A),
    ("A-", BUILDING_A),
    (BUILDING_B, BUILDING_B),
    ("B栋", BUILDING_B),
    ("B-", BUILDING_B),
    (BUILDING_C, BUILDING_C),
    ("C栋", BUILDING_C),
    ("C-", BUILDING_C),
    (BUILDING_D, BUILDING_D),
    ("D栋", BUILDING_D),
    ("D-", BUILDING_D),
    (BUILDING_E, BUILDING_E),
    ("E栋", BUILDING_E),
    ("E-", BUILDING_E),
]

# ========== Hot Reload / Package Excludes (keep consistent with package_portable.py) ==========
PACKAGE_EXCLUDE_FILES = {
    "package_portable.py",
    "分发前检查清单.md",
    "静默启动.bat",
}

PACKAGE_EXCLUDE_TOP_LEVEL = {
    ".git",
    ".idea",
    ".vscode",
    "__pycache__",
    "build_output",
} | PACKAGE_EXCLUDE_FILES

PACKAGE_EXCLUDE_DIR_NAMES = {
    ".git",
    ".idea",
    ".vscode",
    "__pycache__",
}

HOT_RELOAD_IGNORE_DIR_NAMES = {
    "build_output",
    ".venv",
    "__pycache__",
    ".git",
    ".idea",
    ".vscode",
    "wheels",
    "build",
    "dist",
    "patches",
    "logs",
}

HOT_RELOAD_IGNORE_SUFFIXES = {
    ".pyc",
    ".pyo",
    ".zip",
}

HOT_RELOAD_IGNORE_GLOBS = [
    "*.swp",
    "*~",
    ".#*",
    "*.tmp",
    "*.bak",
    "*.orig",
    "*.rej",
]

HOT_RELOAD_DEBOUNCE_MS = 1000
HOT_RELOAD_SAFE_MODE_WINDOW_S = 30
HOT_RELOAD_SAFE_MODE_MAX_RESTARTS = 6

# 路径策略：仅 handlers/config/time_parser 做热重载，其余 Python 变更默认重启
HOT_RELOAD_RELOAD_PATH_HINTS = [
    os.path.join("upload_event_module", "services", "handlers"),
    os.path.join("upload_event_module", "config.py"),
    os.path.join("upload_event_module", "time_parser.py"),
]

BUILDING_OPTIONS = [
    BUILDING_A,
    BUILDING_B,
    BUILDING_C,
    BUILDING_D,
    BUILDING_E,
    BUILDING_H,
    BUILDING_110,
    BUILDING_PARK,
    OPTION_SLASH,
]

BUILDING_PLACEHOLDER = "请选择楼栋..."
EVENT_LEVEL_PLACEHOLDER = "请选择等级..."
EVENT_SOURCE_PLACEHOLDER = "请选择来源..."
SPECIALTY_PLACEHOLDER = "请选择专业..."

# ========== Handler Field Names ==========
EVENT_NOTICE_FIELDS = {
    "alarm_desc": "告警描述",
    "level": "事件等级",
    "building": "机楼",
    "specialty": "专业",
    "source": "事件发现来源",
    "occurrence_time": "事件发生时间",
    "response_time": "事件进展响应时间",
    "response_snapshot": "钉钉响应截图",
    "progress_update": "进展更新时间",
    "progress_snapshot": "进展更新截图",
    "transfer_to_overhaul": "是否转检修",
    "recover_time": "事件恢复时间",
    "recover_snapshot": "事件恢复截图",
    "end_time": "事件结束时间",
    "end_snapshot": "事件结束截图",
}

CHANGE_NOTICE_FIELDS = {
    "status": "变更状态",
    "start_snapshot": "变更开始钉钉截图",
    "start_time": "变更开始时间",
    "update_snapshot": "过程更新钉钉截图",
    "update_time": "过程更新时间",
    "end_snapshot": "变更结束钉钉截图",
    "end_time": "变更结束时间",
    "title": "名称",
    "level_zhihang": "智航-变更等级",
    "level_ali": "阿里-变更等级",
    "location": "位置",
    "content": "内容",
    "reason": "原因",
    "impact": "影响",
    "progress": "进度",
    "building": "楼栋",
    "today_in_progress": "今日是否进行",
}

MAINTENANCE_NOTICE_FIELDS = {
    "status": "维保状态",
    "name": "名称",
    "specialty": "专业",
    "building": "楼栋",
    "maintenance_cycle": "维保周期",
    "content": "内容",
    "impact": "影响",
    "progress": "进度",
    "reason": "原因",
    "location": "位置",
    "plan_start": "计划开始时间",
    "plan_end": "计划结束时间",
    "actual_start": "实际开始时间",
    "actual_end": "实际结束时间",
    "notice_images": "过程通告图片",
    "site_images": "过程现场图片",
}

OVERHAUL_NOTICE_FIELDS = {
    "building": "楼栋",
    "status": "检修状态",
    "title": "名称（标题）",
    "specialty": "专业",
    "location": "位置",
    "urgency": "紧急程度",
    "repair_device": "维修设备",
    "repair_fault": "维修故障",
    "fault_type": "故障类型",
    "repair_mode": "维修方式",
    "impact": "影响范围",
    "discovery": "故障发现方式（来源）",
    "symptom": "故障现象",
    "reason": "故障原因",
    "solution": "解决方案",
    "progress": "进度（完成情况）",
    "fault_time": "发生故障时间",
    "expected_time": "期望完成时间",
    "actual_start": "实际开始时间",
    "actual_end": "实际结束时间",
    "notice_images": "过程通告截图",
    "site_images": "过程现场图片",
}

POLLING_NOTICE_FIELDS = {
    "title": "标题（名称）",
    "building": "楼栋",
    "status": "轮巡状态",
    "specialty": "专业",
    "device": "设备",
    "content": "内容",
    "progress": "进度",
    "plan_start": "计划开始时间",
    "plan_end": "计划结束时间",
    "actual_start": "实际开始时间",
    "actual_end": "实际结束时间",
    "notice_images": "过程通告截图",
}

POWER_NOTICE_FIELDS = {
    "title": "名称",
    "building": "楼栋",
    "status": "上电状态",
    "specialty": "专业",
    "cabinet": "柜号",
    "quantity": "数量（个）",
    "progress": "进度",
    "plan_start": "计划开始时间",
    "plan_end": "计划结束时间",
    "actual_start": "实际开始时间",
    "actual_end": "实际结束时间",
    "notice_images": "过程通告截图",
}

ADJUST_NOTICE_FIELDS = {
    "specialty": "专业",
    "building": "楼栋",
    "status": "调整状态",
    "title": "名称",
    "location": "位置",
    "content": "内容",
    "reason": "原因",
    "impact": "影响",
    "progress": "进度",
    "plan_start": "计划开始时间",
    "plan_end": "计划结束时间",
    "actual_start": "实际开始时间",
    "actual_end": "实际结束时间",
    "notice_images": "过程通告截图",
}

# 全局配置单例
config = ConfigManager()

def get_field_config(notice_type: str) -> dict[str, str]:
    """
    获取指定通告类型的字段配置映射

    Args:
        notice_type: 通告类型（如 "事件通告"、"变更通告" 等）

    Returns:
        字段配置字典，key为字段标识符，value为飞书表格中的字段名
    """
    from .core.parser import (
        NOTICE_TYPE_WEIBAO,
        NOTICE_TYPE_BIANGENG,
        NOTICE_TYPE_TIAOZHENG,
        NOTICE_TYPE_SHIJIAN,
    )

    field_map = {
        NOTICE_TYPE_SHIJIAN: EVENT_NOTICE_FIELDS,
        "事件通告": EVENT_NOTICE_FIELDS,
        NOTICE_TYPE_BIANGENG: CHANGE_NOTICE_FIELDS,
        "变更通告": CHANGE_NOTICE_FIELDS,
        NOTICE_TYPE_WEIBAO: MAINTENANCE_NOTICE_FIELDS,
        "维保通告": MAINTENANCE_NOTICE_FIELDS,
        NOTICE_TYPE_TIAOZHENG: ADJUST_NOTICE_FIELDS,
        "设备调整": ADJUST_NOTICE_FIELDS,
        "上下电通告": POWER_NOTICE_FIELDS,
        "设备轮巡": POLLING_NOTICE_FIELDS,
        "设备检修": OVERHAUL_NOTICE_FIELDS,
    }
    return field_map.get(notice_type, {})

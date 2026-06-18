# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime as dt
import base64
import csv
import hashlib
import hmac
import io
import json
import os
import re
import secrets
import tempfile
import threading
import time
import uuid
import copy
import zipfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote, urlparse
import xml.etree.ElementTree as ET

import lark_oapi as lark
from lark_oapi.api.drive.v1 import UploadAllMediaRequest, UploadAllMediaRequestBody

from upload_event_module.config import config, get_field_config
from upload_event_module.services.feishu_service import (
    ensure_feishu_token,
    refresh_feishu_token,
)
from upload_event_module.services.feishu_token_manager import TOKEN_ERROR_CODES
from upload_event_module.services.http_client import FeishuHTTPError, FeishuHttpClient
from upload_event_module.services.robot_webhook import send_text_to_open_ids
from upload_event_module.utils import get_data_file_path

from .state_store import LanPortalStateStore
from .identity_utils import (
    canonical_source_record_id,
    canonical_target_record_id,
    normalize_notice_identity_payload,
)


DEFAULT_APP_TOKEN = "HU38bc1vnamMK9sCeOgclUvXnFc"
DEFAULT_TABLE_ID = "tblzk7WrXxNWQy6V"
CHANGE_SOURCE_APP_TOKEN = "JhiVwgfoIimAqEk8YwEc09sknGd"
CHANGE_SOURCE_TABLE_ID = "tblBvg6wCYSX3hcg"
ZHIHANG_CHANGE_APP_TOKEN = "IrIibPkUOa6udGsMhu2cbOqhnWg"
ZHIHANG_CHANGE_TABLE_ID = "tblqMJvYW5dxFFfU"
REPAIR_SOURCE_APP_TOKEN = "AnEBwJlvGiJfDdkOB32cUPuknzg"
REPAIR_SOURCE_TABLE_ID = "tblschT48zXwigUG"
REPAIR_SOURCE_VIEW_ID = "vewn2xWBED"
REPAIR_SYNC_TABLE_ID = "tblSA9euoote8aCA"
REPAIR_LINK_FIELD_NAME = "设备检修关联"
REPAIR_LINK_DELAY_SECONDS = 70 * 60
REPAIR_LINK_RETRY_SECONDS = 10 * 60
REPAIR_LINK_RETRY_SLOW_SECONDS = 20 * 60
REPAIR_LINK_FAST_RETRY_ATTEMPTS = 6
REPAIR_LINK_MAX_ATTEMPTS = 18
BITABLE_DATA_NOT_READY_CODE = 1254607
BITABLE_TRANSIENT_RETRY_DELAYS = (1.0, 2.5, 5.0)
MOP_CANDIDATE_CACHE_TTL_SECONDS = 10 * 60
MOP_SOURCE_APP_TOKEN = "MliKbC3fXa8PXrsndKscmxjdn1g"
MOP_SOURCE_TABLE_ID = "tblpqwu1kQ0bmi0i"
MOP_SOURCE_VIEW_ID = "vewrHJHl3v"
MOP_TITLE_FIELD_NAME = "文件名"
MOP_ATTACHMENT_FIELD_NAME = "文件"
SIGNATURE_APP_TOKEN = "HU38bc1vnamMK9sCeOgclUvXnFc"
SIGNATURE_TABLE_ID = "tbluozblhRAjbljX"
SIGNATURE_VIEW_ID = ""
SIGNATURE_NAME_FIELD = "姓名"
SIGNATURE_USER_FIELD = "员工姓名"
SIGNATURE_ATTACHMENT_FIELD = "手写签名"
SIGNATURE_INACTIVE_FIELD = "离职/异动情况"
SIGNATURE_PEOPLE_CACHE_TTL_SECONDS = 5 * 60
SIGNATURE_LINK_TOKEN_TTL_SECONDS = 60 * 60
TEMP_SIGNATURE_TABLE_ID = "tblC77nllNrprHBY"
TEMP_SIGNATURE_NAME_FIELD = "员工姓名"
TEMP_SIGNATURE_EMPLOYEE_NO_FIELD = "员工工号"
TEMP_SIGNATURE_CERT_FIELD = "持证"
TEMP_SIGNATURE_ATTACHMENT_FIELD = "手写签名"
TEMP_SIGNATURE_BUILDING_FIELD = "楼栋"
TEMP_SIGNATURE_SPECIALTY_FIELD = "专业"
MOP_SIGNED_ATTACHMENT_FIELD = "维护保养单"
MOP_ENGINEER_CONFIRM_FIELD = "工程师确认"
MOP_SUPERVISOR_CONFIRM_FIELD = "主管确认"
ATTACHMENT_LATEST_FIELD_NAMES = (
    "最新发布时间",
    "最新发布",
    "发布时间",
    "更新时间",
    "最后更新时间",
    "最新更新时间",
)
ATTACHMENT_CACHE_DAILY_MARKER = "attachment_cache_daily_scan.json"
REPAIR_SOURCE_PAGE_SIZE = 200
DEFAULT_IMPACT_TEXT = "对IT业务无影响，不会触发BA和BMS系统相关告警"
DEFAULT_PROGRESS_TEXT = "准备工作已完成，人员已就位，可否开始操作？"
DEFAULT_MAINTENANCE_STATUS = "未开始"
MAINTENANCE_STATUS_ONGOING = "进行中"
WORKBENCH_SOURCE_STATUSES = {DEFAULT_MAINTENANCE_STATUS, MAINTENANCE_STATUS_ONGOING}
WORK_TYPE_MAINTENANCE = "maintenance"
WORK_TYPE_CHANGE = "change"
WORK_TYPE_REPAIR = "repair"
WORK_TYPE_POWER = "power"
WORK_TYPE_POLLING = "polling"
WORK_TYPE_ADJUST = "adjust"
NOTICE_TYPE_MAINTENANCE = "维保通告"
NOTICE_TYPE_CHANGE = "变更通告"
NOTICE_HEADING_CHANGE = NOTICE_TYPE_CHANGE
NOTICE_TYPE_REPAIR = "设备检修"
NOTICE_TYPE_POWER = "上下电通告"
NOTICE_TYPE_POWER_UP = "上电通告"
NOTICE_TYPE_POWER_DOWN = "下电通告"
NOTICE_TYPE_POLLING = "设备轮巡"
NOTICE_TYPE_ADJUST = "设备调整"
NOTICE_TIME_SEPARATOR = "~"
NOTICE_TEXT_TEMPLATES = {
    WORK_TYPE_MAINTENANCE: {
        "heading": NOTICE_TYPE_MAINTENANCE,
        "fields": (
            ("名称", "title"),
            ("时间", "time_range"),
            ("位置", "location"),
            ("内容", "content"),
            ("原因", "reason"),
            ("影响", "impact"),
            ("进度", "progress"),
        ),
    },
    WORK_TYPE_CHANGE: {
        "heading": NOTICE_HEADING_CHANGE,
        "fields": (
            ("名称", "title"),
            ("等级", "level"),
            ("时间", "time_range"),
            ("位置", "location"),
            ("内容", "content"),
            ("原因", "reason"),
            ("影响", "impact"),
            ("进度", "progress"),
        ),
    },
    WORK_TYPE_REPAIR: {
        "heading": NOTICE_TYPE_REPAIR,
        "fields": (
            ("标题", "title"),
            ("地点", "location"),
            ("紧急程度", "level"),
            ("专业", "specialty"),
            ("发现故障时间", "fault_time"),
            ("期望完成时间", "expected_time"),
            ("维修设备", "repair_device"),
            ("维修故障", "repair_fault"),
            ("故障类型", "fault_type"),
            ("维修方式", "repair_mode"),
            ("影响范围", "impact"),
            ("故障发现方式", "discovery"),
            ("故障现象", "symptom"),
            ("故障原因", "reason"),
            ("解决方案", "solution"),
            ("备件更换情况", "spare_parts"),
            ("完成情况", "progress"),
        ),
    },
    WORK_TYPE_POWER: {
        "heading": "上电通告",
        "fields": (
            ("名称", "title"),
            ("时间", "time_range"),
            ("柜号", "cabinet"),
            ("数量", "quantity"),
            ("进度", "progress"),
        ),
    },
    WORK_TYPE_POLLING: {
        "heading": NOTICE_TYPE_POLLING,
        "fields": (
            ("标题", "title"),
            ("时间", "time_range"),
            ("设备", "device"),
            ("内容", "content"),
            ("影响", "impact"),
            ("进度", "progress"),
        ),
    },
    WORK_TYPE_ADJUST: {
        "heading": NOTICE_TYPE_ADJUST,
        "fields": (
            ("名称", "title"),
            ("时间", "time_range"),
            ("位置", "location"),
            ("内容", "content"),
            ("原因", "reason"),
            ("影响", "impact"),
            ("进度", "progress"),
        ),
    },
}
NOTICE_TYPE_KEYWORD_RULES = {
    WORK_TYPE_MAINTENANCE: (r"维保", r"维护"),
    WORK_TYPE_CHANGE: (r"变更",),
    WORK_TYPE_REPAIR: (r"检修", r"维修"),
    WORK_TYPE_POWER: (r"上下电", r"上电", r"下电"),
    WORK_TYPE_POLLING: (r"轮巡",),
    WORK_TYPE_ADJUST: (r"调整",),
}
NOTICE_WORK_TYPE_LABELS = {
    WORK_TYPE_MAINTENANCE: "维保",
    WORK_TYPE_CHANGE: "变更",
    WORK_TYPE_REPAIR: "检修",
    WORK_TYPE_POWER: "上电",
    WORK_TYPE_POLLING: "轮巡",
    WORK_TYPE_ADJUST: "调整",
}
NOTICE_TYPE_BY_WORK_TYPE = {
    WORK_TYPE_MAINTENANCE: NOTICE_TYPE_MAINTENANCE,
    WORK_TYPE_CHANGE: NOTICE_TYPE_CHANGE,
    WORK_TYPE_REPAIR: NOTICE_TYPE_REPAIR,
    WORK_TYPE_POWER: NOTICE_TYPE_POWER,
    WORK_TYPE_POLLING: NOTICE_TYPE_POLLING,
    WORK_TYPE_ADJUST: NOTICE_TYPE_ADJUST,
}
WORK_TYPE_BY_NOTICE_TYPE = {
    NOTICE_TYPE_MAINTENANCE: WORK_TYPE_MAINTENANCE,
    "维护通告": WORK_TYPE_MAINTENANCE,
    NOTICE_TYPE_CHANGE: WORK_TYPE_CHANGE,
    "设备变更": WORK_TYPE_CHANGE,
    NOTICE_TYPE_REPAIR: WORK_TYPE_REPAIR,
    "检修通告": WORK_TYPE_REPAIR,
    NOTICE_TYPE_POWER: WORK_TYPE_POWER,
    NOTICE_TYPE_POWER_UP: WORK_TYPE_POWER,
    NOTICE_TYPE_POWER_DOWN: WORK_TYPE_POWER,
    NOTICE_TYPE_POLLING: WORK_TYPE_POLLING,
    "轮巡通告": WORK_TYPE_POLLING,
    NOTICE_TYPE_ADJUST: WORK_TYPE_ADJUST,
    "调整通告": WORK_TYPE_ADJUST,
}
END_SITE_PHOTO_REQUIRED_WORK_TYPES = {
    WORK_TYPE_MAINTENANCE,
    WORK_TYPE_CHANGE,
    WORK_TYPE_REPAIR,
}
END_SITE_PHOTO_REQUIRED_NOTICE_TYPES = {
    NOTICE_TYPE_MAINTENANCE,
    NOTICE_TYPE_CHANGE,
    NOTICE_HEADING_CHANGE,
    "设备变更",
    NOTICE_TYPE_REPAIR,
}
CHANGE_PROGRESS_NOT_STARTED = "未开始"
CHANGE_PROGRESS_ONGOING = "进行中"
CHANGE_PROGRESS_ENDED = "已结束"
CHANGE_WORKBENCH_PROGRESS_VALUES = {CHANGE_PROGRESS_NOT_STARTED, CHANGE_PROGRESS_ONGOING}
CHANGE_DEFAULT_LEVEL = "I3"
CHANGE_PROGRESS_OPTION_FALLBACK = {
    "optXQcI0z3": "未开始",
    "optqHR2ClY": "进行中",
    "optZtdlcZy": "已结束",
    "optqGq0YcR": "退回",
}
ZHIHANG_PROGRESS_ENDED = "已结束"
REPAIR_BUILDING_OPTION_FALLBACK = {
    "optjAjpfmQ": "A楼",
    "optHCxNMDk": "B楼",
    "opt2iUueLp": "C楼",
    "optTUZGlCA": "D楼",
    "opthJp8Wnt": "E楼",
}
REPAIR_SPECIALTY_OPTION_FALLBACK = {
    "optAssNaw3": "电气",
    "opt3jdJJb7": "暖通",
    "opt509Mxgr": "弱电",
    "optyLRPdQS": "消防",
}
FIELD_OPTION_FALLBACK = {
    "所属数据中心/楼栋-使用": REPAIR_BUILDING_OPTION_FALLBACK,
    "专业（推送消息用）": REPAIR_SPECIALTY_OPTION_FALLBACK,
}
REPAIR_DATETIME_FIELDS = {
    "故障发生时间",
    "发现故障时间",
    "维修开始时间",
    "维修结束时间",
    "维修结束时间（2026）",
    "期望完成时间",
}
PLACEHOLDER_TEXT_VALUES = {"", "-", "--", "—", "——", "/", "无", "暂无"}
SOURCE_CACHE_TTL_SECONDS = 30 * 60
RECENT_MONTH_FILTER_LABEL = "本月+上月"
STATE_NS_MEMORY = "notice_memory"
STATE_NS_DAILY_SUMMARY = "notice_daily_summary"
STATE_NS_WORK_STATUS = "notice_work_status"
STATE_NS_HIDDEN_ONGOING = "notice_hidden_ongoing"
STATE_NS_ACTION_JOB = "notice_action_job"
ACTION_JOB_MAX_RETAINED = 120
ACTION_JOB_SUCCESS_RETENTION_SECONDS = 30 * 60
ACTION_JOB_FAILED_RETENTION_SECONDS = 14 * 24 * 60 * 60


def external_mock_enabled() -> bool:
    return os.environ.get("CLIPFLOW_BACKEND_MOCK_EXTERNAL") == "1"


def _payload_list(payload: dict[str, Any], key: str) -> list[Any]:
    value = payload.get(key) if isinstance(payload, dict) else None
    return value if isinstance(value, list) else []


def engineer_mop_fill_kwargs_from_payload(
    payload: dict[str, Any], *, scope: str
) -> dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    return {
        "scope": str(scope or payload.get("scope") or "ALL"),
        "local_file_path": str(payload.get("local_file_path") or ""),
        "mop_record_id": str(payload.get("mop_record_id") or ""),
        "mop_title": str(payload.get("mop_title") or ""),
        "sheet_name": str(payload.get("sheet_name") or ""),
        "fields": _payload_list(payload, "fields"),
        "checkboxes": _payload_list(payload, "checkboxes"),
        "cell_edits": _payload_list(payload, "cell_edits"),
        "signatures": _payload_list(payload, "signatures"),
    }


def engineer_mop_upload_signed_kwargs_from_payload(
    payload: dict[str, Any],
    *,
    scope: str,
    operator_open_id: str = "",
    operator_name: str = "",
) -> dict[str, Any]:
    kwargs = engineer_mop_fill_kwargs_from_payload(payload, scope=scope)
    kwargs.update(
        {
            "source_record_id": str(payload.get("source_record_id") or ""),
            "notice_title": str(payload.get("notice_title") or ""),
            "notice_key": str(payload.get("notice_key") or ""),
            "operator_open_id": str(operator_open_id or ""),
            "operator_name": str(operator_name or ""),
        }
    )
    return kwargs


def external_real_write_guard() -> dict[str, Any]:
    require_confirm = os.environ.get("CLIPFLOW_REQUIRE_REAL_EXTERNAL_CONFIRM", "1") != "0"
    confirmed = os.environ.get("CLIPFLOW_REAL_EXTERNAL_CONFIRMED") == "1"
    mock = external_mock_enabled()
    allowed = (not mock) and (not require_confirm or confirmed)
    reason = ""
    if mock:
        reason = "当前为外部接口 mock 模式，已拦截真实飞书/多维写入。"
    elif require_confirm and not confirmed:
        reason = (
            "真实飞书/多维写入保护已启用，需设置 "
            "CLIPFLOW_REAL_EXTERNAL_CONFIRMED=1 后才允许执行。"
        )
    return {
        "mock_external": mock,
        "require_confirm": require_confirm,
        "confirmed": confirmed,
        "real_write_allowed": allowed,
        "reason": reason,
    }
LI_SHILONG_OPEN_ID = "ou_902e364a6c2c6c20893c02abe505a7b2"
MA_JINYU_OPEN_ID = "ou_a6644e62a43b916c6bc26148cf74f208"
HANDOVER_PASSWORD_RESET_TTL_SECONDS = 10 * 60
HANDOVER_PASSWORD_RESET_MAX_ATTEMPTS = 5
BUILDING_OPEN_ID_MAP = {
    "110": "ou_913ff495ae8f5a0a55ae5670fd148bbd",
    "A": "ou_3e0a9399a037276f8449cc778f18bb8a",
    "B": "ou_160e991b0fd811cda7260de056d8a363",
    "C": "ou_99391b00c1406917930483d8500af400",
    "D": "ou_b9c26b4da3721c2de11ee175c707e756",
    "E": "ou_f0fe7137818e8b422b0594e51eab13f3",
    "H": "ou_55647926b7fbfe46507ef7e8afa76315",
}
SCOPE_OPTIONS = [
    {"value": "110", "label": "110站"},
    {"value": "A", "label": "A楼"},
    {"value": "B", "label": "B楼"},
    {"value": "C", "label": "C楼"},
    {"value": "D", "label": "D楼"},
    {"value": "E", "label": "E楼"},
    {"value": "H", "label": "H楼"},
    {"value": "CAMPUS", "label": "园区"},
    {"value": "ALL", "label": "全部"},
]
BUILDING_SCOPE_CODES = ("110", "A", "B", "C", "D", "E", "H")


class PortalError(RuntimeError):
    pass


@dataclass
class FieldMeta:
    field_id: str
    field_name: str
    ui_type: str
    field_type: int
    is_primary: bool
    options_map: dict[str, str]
    option_names: list[str]
    has_formula: bool


class MaintenancePortalService:
    _memory_lock = threading.RLock()
    _summary_lock = threading.RLock()
    _handover_lock = threading.RLock()
    _handover_reset_lock = threading.RLock()
    _hidden_ongoing_lock = threading.RLock()

    def __init__(
        self,
        *,
        app_token: str = DEFAULT_APP_TOKEN,
        table_id: str = DEFAULT_TABLE_ID,
    ) -> None:
        self.app_token = str(app_token or DEFAULT_APP_TOKEN).strip()
        self.table_id = str(table_id or DEFAULT_TABLE_ID).strip()
        self._http_client = FeishuHttpClient()
        self._field_meta_list: list[FieldMeta] = []
        self._field_meta_by_name: dict[str, FieldMeta] = {}
        self._records: list[dict[str, Any]] = []
        self._change_field_meta_list: list[FieldMeta] = []
        self._change_field_meta_by_name: dict[str, FieldMeta] = {}
        self._change_records: list[dict[str, Any]] = []
        self._zhihang_change_field_meta_list: list[FieldMeta] = []
        self._zhihang_change_field_meta_by_name: dict[str, FieldMeta] = {}
        self._zhihang_change_records: list[dict[str, Any]] = []
        self._repair_field_meta_list: list[FieldMeta] = []
        self._repair_field_meta_by_name: dict[str, FieldMeta] = {}
        self._repair_records: list[dict[str, Any]] = []
        self._maintenance_loaded_once = False
        self._change_loaded_once = False
        self._zhihang_change_loaded_once = False
        self._repair_loaded_once = False
        self._load_warnings: list[str] = []
        self._last_loaded_at = ""
        self._last_loaded_ts = 0.0
        self._refresh_lock = threading.RLock()
        # Legacy JSON paths are read only for one-time migration. Do not create
        # their directories on fresh SQLite-only installs.
        self._memory_dir = Path(get_data_file_path("lan_template_memory"))
        self._summary_dir = Path(get_data_file_path("lan_template_daily_summary"))
        self._work_status_dir = Path(get_data_file_path("lan_template_work_status"))
        self._handover_links_path = Path(get_data_file_path("lan_handover_links.json"))
        self._hidden_ongoing_path = Path(get_data_file_path("lan_template_hidden_ongoing.json"))
        self._state_store = LanPortalStateStore()
        self._state_version_lock = threading.RLock()
        self._state_version = 0
        self._legacy_summary_migrated = False
        self._legacy_work_status_migrated = False
        self._work_status_backfilled = False
        self._work_status_cache_signature: tuple[tuple[str, int], ...] | None = None
        self._work_status_cache_items: list[dict[str, Any]] | None = None
        self._target_record_cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._engineer_mop_cache_lock = threading.RLock()
        self._engineer_mop_cache: dict[str, Any] | None = None
        self._signature_people_cache_lock = threading.RLock()
        self._signature_people_cache: dict[str, Any] | None = None
        self._external_signature_people_cache_lock = threading.RLock()
        self._external_signature_people_cache: dict[str, Any] | None = None
        self._attachment_cache_lock = threading.RLock()
        self._attachment_cache_refresh_lock = threading.RLock()
        self._attachment_cache_refresh_running = False
        self._jobs_lock = threading.RLock()
        self._jobs: dict[str, dict[str, Any]] = self._load_action_jobs_from_state()
        self._handover_password_reset: dict[str, Any] | None = None

    def state_cache_version(self) -> int:
        with self._state_version_lock:
            return int(self._state_version)

    def _touch_state_cache_version(self) -> None:
        with self._state_version_lock:
            self._state_version += 1

    def _auth_headers(self) -> dict[str, str]:
        token = str(ensure_feishu_token() or config.user_token or "").strip()
        if not token:
            raise PortalError("未配置有效的飞书 user_token。")
        return {"Authorization": f"Bearer {token}"}

    def _request_payload(
        self,
        method: str,
        url: str,
        *,
        context: str,
        headers: dict[str, str],
        params: dict[str, Any] | None = None,
        json_payload: Any = None,
    ) -> dict[str, Any]:
        try:
            return self._http_client.request_json(
                method,
                url,
                headers=headers,
                params=params or {},
                json_payload=json_payload,
            )
        except FeishuHTTPError as exc:
            raise PortalError(f"{context} HTTP失败: {exc}") from exc

    def _request_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        app_token: str | None = None,
        table_id: str | None = None,
    ) -> dict[str, Any]:
        app_token = str(app_token or self.app_token or DEFAULT_APP_TOKEN).strip()
        table_id = str(table_id or self.table_id or DEFAULT_TABLE_ID).strip()
        if not app_token or not table_id:
            raise PortalError("飞书多维请求缺少 app_token/table_id，已阻止空源表请求。")
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{app_token}/tables/{table_id}/{path}"
        )

        def do_get() -> dict[str, Any]:
            return self._request_payload(
                "GET",
                url,
                context="飞书接口",
                headers=self._auth_headers(),
                params=params or {},
            )

        payload: dict[str, Any] = {}
        code = 0
        for attempt in range(len(BITABLE_TRANSIENT_RETRY_DELAYS) + 1):
            payload = do_get()
            code = int(payload.get("code") or 0)
            if code in TOKEN_ERROR_CODES:
                refresh_feishu_token()
                payload = do_get()
                code = int(payload.get("code") or 0)
            if (
                code == BITABLE_DATA_NOT_READY_CODE
                and attempt < len(BITABLE_TRANSIENT_RETRY_DELAYS)
            ):
                time.sleep(BITABLE_TRANSIENT_RETRY_DELAYS[attempt])
                continue
            break
        if code != 0:
            if code == BITABLE_DATA_NOT_READY_CODE:
                raise PortalError(
                    "飞书多维表正在计算，数据暂时未准备好，请稍后重试。"
                )
            raise PortalError(
                f"飞书接口失败: code={code}, msg={payload.get('msg') or 'unknown'}"
            )
        return payload

    def _patch_record_fields(
        self,
        *,
        app_token: str,
        table_id: str,
        record_id: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        app_token = str(app_token or "").strip()
        table_id = str(table_id or "").strip()
        record_id = str(record_id or "").strip()
        if not app_token or not table_id or not record_id:
            raise PortalError("更新多维记录缺少 app_token/table_id/record_id。")
        if not isinstance(fields, dict) or not fields:
            raise PortalError("更新多维记录字段不能为空。")
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{app_token}/tables/{table_id}/records/{record_id}"
        )

        def do_update() -> dict[str, Any]:
            return self._request_payload(
                "PUT",
                url,
                context="飞书记录更新",
                headers={**self._auth_headers(), "Content-Type": "application/json"},
                json_payload={"fields": fields},
            )

        payload = do_update()
        if int(payload.get("code") or 0) in TOKEN_ERROR_CODES:
            refresh_feishu_token()
            payload = do_update()
        code = payload.get("code", 0)
        if code != 0:
            raise PortalError(
                f"飞书记录更新失败: code={code}, msg={payload.get('msg') or 'unknown'}"
            )
        return payload

    def _create_record_fields(
        self,
        *,
        app_token: str,
        table_id: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        app_token = str(app_token or "").strip()
        table_id = str(table_id or "").strip()
        if not app_token or not table_id:
            raise PortalError("创建多维记录缺少 app_token/table_id。")
        if not isinstance(fields, dict) or not fields:
            raise PortalError("创建多维记录字段不能为空。")
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{app_token}/tables/{table_id}/records"
        )

        def do_create() -> dict[str, Any]:
            return self._request_payload(
                "POST",
                url,
                context="飞书记录创建",
                headers={**self._auth_headers(), "Content-Type": "application/json"},
                json_payload={"fields": fields},
            )

        payload = do_create()
        if int(payload.get("code") or 0) in TOKEN_ERROR_CODES:
            refresh_feishu_token()
            payload = do_create()
        code = payload.get("code", 0)
        if code != 0:
            raise PortalError(
                f"飞书记录创建失败: code={code}, msg={payload.get('msg') or 'unknown'}"
            )
        return payload

    @staticmethod
    def _extract_option_map(field: dict[str, Any]) -> tuple[dict[str, str], list[str]]:
        prop = field.get("property") or {}
        options = prop.get("options") or []
        if not options:
            nested = (((prop.get("type") or {}).get("ui_property") or {}).get("options")) or []
            options = nested
        option_map: dict[str, str] = {}
        option_names: list[str] = []
        for option in options:
            option_id = str(option.get("id") or "").strip()
            option_name = str(option.get("name") or "").strip()
            if not option_name:
                continue
            if option_id:
                option_map[option_id] = option_name
            option_names.append(option_name)
        deduped = list(dict.fromkeys(option_names))
        return option_map, deduped

    def _load_fields(self) -> list[FieldMeta]:
        payload = self._request_json("fields", params={"page_size": 500})
        metas = self._parse_field_meta(payload)
        self._field_meta_list = metas
        self._field_meta_by_name = {meta.field_name: meta for meta in metas}
        return metas

    def _load_change_fields(self) -> list[FieldMeta]:
        payload = self._request_json(
            "fields",
            params={"page_size": 500},
            app_token=CHANGE_SOURCE_APP_TOKEN,
            table_id=CHANGE_SOURCE_TABLE_ID,
        )
        metas = self._parse_field_meta(payload)
        self._change_field_meta_list = metas
        self._change_field_meta_by_name = {meta.field_name: meta for meta in metas}
        return metas

    def _load_zhihang_change_fields(self) -> list[FieldMeta]:
        payload = self._request_json(
            "fields",
            params={"page_size": 500},
            app_token=ZHIHANG_CHANGE_APP_TOKEN,
            table_id=ZHIHANG_CHANGE_TABLE_ID,
        )
        metas = self._parse_field_meta(payload)
        self._zhihang_change_field_meta_list = metas
        self._zhihang_change_field_meta_by_name = {
            meta.field_name: meta for meta in metas
        }
        return metas

    def _load_repair_fields(self) -> list[FieldMeta]:
        payload = self._request_json(
            "fields",
            params={"page_size": 500},
            app_token=REPAIR_SOURCE_APP_TOKEN,
            table_id=REPAIR_SOURCE_TABLE_ID,
        )
        metas = self._parse_field_meta(payload)
        self._repair_field_meta_list = metas
        self._repair_field_meta_by_name = {meta.field_name: meta for meta in metas}
        return metas

    def _parse_field_meta(self, payload: dict[str, Any]) -> list[FieldMeta]:
        items = payload.get("data", {}).get("items") or []
        return self._parse_field_metas(items)

    def _parse_field_metas(self, items: list[dict[str, Any]]) -> list[FieldMeta]:
        metas: list[FieldMeta] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            option_map, option_names = self._extract_option_map(item)
            prop = item.get("property") or {}
            metas.append(
                FieldMeta(
                    field_id=str(item.get("field_id") or ""),
                    field_name=str(item.get("field_name") or ""),
                    ui_type=str(item.get("ui_type") or ""),
                    field_type=int(item.get("type") or 0),
                    is_primary=bool(item.get("is_primary")),
                    options_map=option_map,
                    option_names=option_names,
                    has_formula=bool(
                        prop.get("formula_expression")
                        or prop.get("formula")
                        or prop.get("formatter")
                    ),
                )
            )
        return metas

    def _load_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        page_token = ""
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            payload = self._request_json("records", params=params)
            data = payload.get("data", {})
            for item in data.get("items") or []:
                normalized = self._normalize_record(
                    item,
                    meta_by_name=self._field_meta_by_name,
                    work_type=WORK_TYPE_MAINTENANCE,
                    notice_type=NOTICE_TYPE_MAINTENANCE,
                    source_app_token=self.app_token or DEFAULT_APP_TOKEN,
                    source_table_id=self.table_id or DEFAULT_TABLE_ID,
                )
                if self._source_record_matches_month_window(normalized):
                    records.append(normalized)
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "").strip()
            if not page_token:
                break
        self._records = records
        self._maintenance_loaded_once = True
        return records

    def _load_change_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        page_token = ""
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            payload = self._request_json(
                "records",
                params=params,
                app_token=CHANGE_SOURCE_APP_TOKEN,
                table_id=CHANGE_SOURCE_TABLE_ID,
            )
            data = payload.get("data", {})
            for item in data.get("items") or []:
                normalized = self._normalize_record(
                    item,
                    meta_by_name=self._change_field_meta_by_name,
                    work_type=WORK_TYPE_CHANGE,
                    notice_type=NOTICE_TYPE_CHANGE,
                    source_app_token=CHANGE_SOURCE_APP_TOKEN,
                    source_table_id=CHANGE_SOURCE_TABLE_ID,
                )
                if self._source_record_matches_month_window(normalized):
                    records.append(normalized)
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "").strip()
            if not page_token:
                break
        self._change_records = records
        self._change_loaded_once = True
        return records

    def _load_zhihang_change_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        page_token = ""
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            payload = self._request_json(
                "records",
                params=params,
                app_token=ZHIHANG_CHANGE_APP_TOKEN,
                table_id=ZHIHANG_CHANGE_TABLE_ID,
            )
            data = payload.get("data", {})
            for item in data.get("items") or []:
                normalized = self._normalize_record(
                    item,
                    meta_by_name=self._zhihang_change_field_meta_by_name,
                    work_type=WORK_TYPE_CHANGE,
                    notice_type=NOTICE_TYPE_CHANGE,
                    source_app_token=ZHIHANG_CHANGE_APP_TOKEN,
                    source_table_id=ZHIHANG_CHANGE_TABLE_ID,
                )
                if self._source_record_matches_month_window(normalized):
                    records.append(normalized)
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "").strip()
            if not page_token:
                break
        self._zhihang_change_records = records
        self._zhihang_change_loaded_once = True
        return records

    def _load_repair_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        page_token = ""
        while True:
            params = {
                "page_size": REPAIR_SOURCE_PAGE_SIZE,
                "view_id": REPAIR_SOURCE_VIEW_ID,
            }
            if page_token:
                params["page_token"] = page_token
            payload = self._request_json(
                "records",
                params=params,
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_SOURCE_TABLE_ID,
            )
            data = payload.get("data", {})
            for item in data.get("items") or []:
                normalized = self._normalize_record(
                    item,
                    meta_by_name=self._repair_field_meta_by_name,
                    work_type=WORK_TYPE_REPAIR,
                    notice_type=NOTICE_TYPE_REPAIR,
                    source_app_token=REPAIR_SOURCE_APP_TOKEN,
                    source_table_id=REPAIR_SOURCE_TABLE_ID,
                )
                if self._source_record_matches_month_window(normalized):
                    records.append(normalized)
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "").strip()
            if not page_token:
                break
        self._repair_records = records
        self._repair_loaded_once = True
        return records

    def _load_table_fields(
        self, *, app_token: str, table_id: str
    ) -> tuple[list[FieldMeta], dict[str, FieldMeta]]:
        payload = self._request_json(
            "fields",
            params={"page_size": 500},
            app_token=app_token,
            table_id=table_id,
        )
        metas = self._parse_field_metas(payload.get("data", {}).get("items") or [])
        return metas, {meta.field_name: meta for meta in metas}

    def _load_table_records(
        self,
        *,
        app_token: str,
        table_id: str,
        meta_by_name: dict[str, FieldMeta],
        work_type: str,
        notice_type: str,
        view_id: str = "",
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        page_token = ""
        while True:
            params = {"page_size": 500}
            if view_id:
                params["view_id"] = view_id
            if page_token:
                params["page_token"] = page_token
            payload = self._request_json(
                "records",
                params=params,
                app_token=app_token,
                table_id=table_id,
            )
            data = payload.get("data", {})
            for item in data.get("items") or []:
                records.append(
                    self._normalize_record(
                        item,
                        meta_by_name=meta_by_name,
                        work_type=work_type,
                        notice_type=notice_type,
                        source_app_token=app_token,
                        source_table_id=table_id,
                    )
                )
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "").strip()
            if not page_token:
                break
        return records

    def _source_cache_ttl_seconds(self) -> int:
        try:
            ttl = int(getattr(config, "lan_template_source_cache_ttl_seconds", 0) or 0)
        except (TypeError, ValueError):
            ttl = 0
        if ttl <= 0:
            ttl = SOURCE_CACHE_TTL_SECONDS
        return ttl

    def _source_cache_expired(self) -> bool:
        ttl = self._source_cache_ttl_seconds()
        if not self._last_loaded_ts:
            return True
        return (time.time() - self._last_loaded_ts) >= ttl

    @staticmethod
    def _is_obsolete_empty_source_warning(value: Any) -> bool:
        text = str(value or "")
        normalized = text.replace("\\/", "/")
        return "/apps//tables//" in normalized or "apps//tables//" in normalized

    @staticmethod
    def _is_bitable_data_not_ready_error(value: Any) -> bool:
        text = str(value or "")
        return (
            str(BITABLE_DATA_NOT_READY_CODE) in text
            or "Data not ready" in text
            or "数据暂时未准备好" in text
            or "多维表正在计算" in text
        )

    def _source_sync_warning(self, label: str, exc: Any) -> str:
        if self._is_bitable_data_not_ready_error(exc):
            return (
                f"{label}同步失败: 飞书多维表正在计算，已保留上次成功数据，"
                "请稍后再试。"
            )
        return f"{label}同步失败: {exc}"

    def _clean_load_warnings(self, warnings: list[Any] | None = None) -> list[str]:
        source = self._load_warnings if warnings is None else warnings
        cleaned: list[str] = []
        for item in source or []:
            text = str(item or "").strip()
            if not text or self._is_obsolete_empty_source_warning(text):
                continue
            if text not in cleaned:
                cleaned.append(text)
        return cleaned[-20:]

    def _current_load_warnings(self) -> list[str]:
        cleaned = self._clean_load_warnings()
        if cleaned != list(self._load_warnings or []):
            self._load_warnings = cleaned
        return cleaned

    def _hydrate_source_records_from_sqlite(self) -> bool:
        try:
            snapshot = self._state_store.get_source_scope_snapshot("ALL")
        except Exception as exc:
            if f"SQLite源表快照读取失败: {exc}" not in self._load_warnings:
                self._load_warnings.append(f"SQLite源表快照读取失败: {exc}")
            return False
        if not snapshot.get("exists"):
            return False
        records = [
            dict(item)
            for item in (snapshot.get("records") or [])
            if isinstance(item, dict)
        ]
        zhihang_records = [
            dict(item)
            for item in (snapshot.get("zhihang_records") or [])
            if isinstance(item, dict)
        ]
        self._records = [
            item for item in records if self._record_work_type(item) == WORK_TYPE_MAINTENANCE
        ]
        self._change_records = [
            item for item in records if self._record_work_type(item) == WORK_TYPE_CHANGE
        ]
        self._repair_records = [
            item for item in records if self._record_work_type(item) == WORK_TYPE_REPAIR
        ]
        self._zhihang_change_records = zhihang_records
        self._maintenance_loaded_once = True
        self._change_loaded_once = True
        self._repair_loaded_once = True
        self._zhihang_change_loaded_once = True
        meta = snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}
        self._last_loaded_at = str(meta.get("last_loaded_at") or self._last_loaded_at or "")
        try:
            self._last_loaded_ts = float(snapshot.get("updated_at") or self._last_loaded_ts or 0)
        except Exception:
            pass
        warnings = meta.get("warnings") if isinstance(meta, dict) else []
        if isinstance(warnings, list):
            self._load_warnings = self._clean_load_warnings(warnings)
        return True

    def _snapshot_meta(self) -> dict[str, Any]:
        return {
            "last_loaded_at": self._last_loaded_at,
            "last_loaded_ts": self._last_loaded_ts,
            "source_cache_ttl_seconds": self._source_cache_ttl_seconds(),
            "warnings": self._current_load_warnings(),
        }

    def _save_source_scope_snapshots(self) -> None:
        meta = self._snapshot_meta()
        snapshots: dict[str, dict[str, Any]] = {}
        for option in SCOPE_OPTIONS:
            scope = self._normalize_scope(option.get("value"))
            records = self._workbench_records_from_memory(
                month=RECENT_MONTH_FILTER_LABEL,
                scope=scope,
            )
            zhihang_records = self._filter_zhihang_change_records_from_memory(
                month=RECENT_MONTH_FILTER_LABEL,
                scope=scope,
            )
            snapshots[scope] = {
                "records": records,
                "zhihang_records": zhihang_records,
            }
        self._state_store.replace_all_source_scope_snapshots(snapshots, meta=meta)

    def _source_snapshot_records(self, scope: str) -> list[dict[str, Any]] | None:
        try:
            snapshot = self._state_store.get_source_scope_snapshot(scope)
        except Exception as exc:
            warning = f"SQLite源表快照读取失败: {exc}"
            if warning not in self._load_warnings:
                self._load_warnings.append(warning)
            return None
        if not snapshot.get("exists"):
            return None
        meta = snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}
        if meta.get("last_loaded_at"):
            self._last_loaded_at = str(meta.get("last_loaded_at") or "")
        warnings = meta.get("warnings") if isinstance(meta, dict) else []
        if isinstance(warnings, list):
            self._load_warnings = self._clean_load_warnings(warnings)
        records = [
            dict(item)
            for item in (snapshot.get("records") or [])
            if isinstance(item, dict)
        ]
        return records

    def _source_snapshot_zhihang_records(self, scope: str) -> list[dict[str, Any]] | None:
        try:
            snapshot = self._state_store.get_source_scope_snapshot(scope)
        except Exception as exc:
            warning = f"SQLite智航源表快照读取失败: {exc}"
            if warning not in self._load_warnings:
                self._load_warnings.append(warning)
            return None
        if not snapshot.get("exists"):
            return None
        meta = snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}
        warnings = meta.get("warnings") if isinstance(meta, dict) else []
        if isinstance(warnings, list):
            self._load_warnings = self._clean_load_warnings(warnings)
        return [
            dict(item)
            for item in (snapshot.get("zhihang_records") or [])
            if isinstance(item, dict)
        ]

    def _source_snapshot_exists(self, scope: str = "ALL") -> bool:
        try:
            return bool(self._state_store.get_source_scope_snapshot(scope).get("exists"))
        except Exception:
            return False

    def _payload_version(
        self,
        *,
        scope: str,
        records: list[dict[str, Any]],
        ongoing_items: list[dict[str, Any]],
        daily_summary: dict[str, Any] | None = None,
    ) -> str:
        stats = (daily_summary or {}).get("stats") if isinstance(daily_summary, dict) else {}
        return ":".join(
            [
                str(self.state_cache_version()),
                str(scope or ""),
                str(self._last_loaded_at or ""),
                str(len(records or [])),
                str(len(ongoing_items or [])),
                str((stats or {}).get("ended") or 0),
            ]
        )

    def clear_target_record_cache(self) -> None:
        with self._refresh_lock:
            self._target_record_cache.clear()

    def clear_engineer_mop_cache(self) -> None:
        with self._engineer_mop_cache_lock:
            self._engineer_mop_cache = None

    def refresh(self) -> None:
        with self._refresh_lock:
            warnings: list[str] = []
            self._target_record_cache.clear()
            self._load_fields()
            self._load_records()
            try:
                self._load_change_fields()
                self._load_change_records()
            except Exception as exc:
                self._change_loaded_once = True
                if not self._change_field_meta_list:
                    self._change_field_meta_list = []
                    self._change_field_meta_by_name = {}
                if not self._change_records:
                    self._change_records = []
                warnings.append(self._source_sync_warning("变更源表", exc))
            try:
                self._load_zhihang_change_fields()
                self._load_zhihang_change_records()
            except Exception as exc:
                self._zhihang_change_loaded_once = True
                if not self._zhihang_change_field_meta_list:
                    self._zhihang_change_field_meta_list = []
                    self._zhihang_change_field_meta_by_name = {}
                if not self._zhihang_change_records:
                    self._zhihang_change_records = []
                warnings.append(self._source_sync_warning("智航变更源表", exc))
            try:
                self._load_repair_fields()
                self._load_repair_records()
            except Exception as exc:
                self._repair_loaded_once = True
                if not self._repair_field_meta_list:
                    self._repair_field_meta_list = []
                    self._repair_field_meta_by_name = {}
                if not self._repair_records:
                    self._repair_records = []
                warnings.append(self._source_sync_warning("检修源表", exc))
            now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._last_loaded_at = now
            self._last_loaded_ts = time.time()
            self._load_warnings = warnings
            if warnings:
                self._state_store.record_failed_source_snapshot(
                    meta=self._snapshot_meta(),
                    error="；".join(warnings),
                )
            else:
                self._save_source_scope_snapshots()
                self._touch_state_cache_version()

    def refresh_repair_source(self) -> dict[str, Any]:
        """Refresh only the repair source table and rewrite SQLite snapshots."""
        with self._refresh_lock:
            if not (
                self._maintenance_loaded_once
                and self._change_loaded_once
                and self._repair_loaded_once
                and self._zhihang_change_loaded_once
            ):
                self._hydrate_source_records_from_sqlite()
            warnings = [
                str(item)
                for item in (self._load_warnings or [])
                if not str(item or "").startswith("检修源表同步失败")
            ]
            try:
                self._load_repair_fields()
                self._load_repair_records()
            except Exception as exc:
                warning = self._source_sync_warning("检修源表", exc)
                if warning not in warnings:
                    warnings.append(warning)
                self._load_warnings = warnings
                raise PortalError(warning) from exc
            now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._last_loaded_at = now
            self._last_loaded_ts = time.time()
            self._load_warnings = warnings
            self._save_source_scope_snapshots()
            self._touch_state_cache_version()
            return {
                "repair_refreshed_at": now,
                "repair_count": len(self._repair_records),
            }

    def refresh_change_source(self) -> dict[str, Any]:
        """Refresh only change-related source tables and rewrite SQLite snapshots."""
        with self._refresh_lock:
            if not (
                self._maintenance_loaded_once
                and self._change_loaded_once
                and self._repair_loaded_once
                and self._zhihang_change_loaded_once
            ):
                self._hydrate_source_records_from_sqlite()
            warnings = [
                str(item)
                for item in (self._load_warnings or [])
                if not (
                    str(item or "").startswith("变更源表同步失败")
                    or str(item or "").startswith("智航变更源表同步失败")
                )
            ]
            try:
                self._load_change_fields()
                self._load_change_records()
            except Exception as exc:
                warning = self._source_sync_warning("变更源表", exc)
                if warning not in warnings:
                    warnings.append(warning)
                self._load_warnings = warnings
                raise PortalError(warning) from exc
            try:
                self._load_zhihang_change_fields()
                self._load_zhihang_change_records()
            except Exception as exc:
                warning = self._source_sync_warning("智航变更源表", exc)
                if warning not in warnings:
                    warnings.append(warning)
                self._load_warnings = warnings
                raise PortalError(warning) from exc
            now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._last_loaded_at = now
            self._last_loaded_ts = time.time()
            self._load_warnings = warnings
            self._save_source_scope_snapshots()
            self._touch_state_cache_version()
            return {
                "change_refreshed_at": now,
                "change_count": len(self._change_records),
                "zhihang_change_count": len(self._zhihang_change_records),
            }

    def refresh_if_interval_elapsed(self, *, min_interval_seconds: int = 60) -> bool:
        min_interval = max(0, int(min_interval_seconds or 0))
        with self._refresh_lock:
            if (
                min_interval
                and self._last_loaded_ts
                and time.time() - float(self._last_loaded_ts) < min_interval
            ):
                return False
            self.refresh()
            return True

    def ensure_loaded(self, *, refresh_if_expired: bool = False) -> None:
        with self._refresh_lock:
            if (
                not self._maintenance_loaded_once
                or (refresh_if_expired and self._source_cache_expired())
            ):
                if (
                    not refresh_if_expired
                    and not self._maintenance_loaded_once
                    and self._hydrate_source_records_from_sqlite()
                ):
                    return
                self.refresh()
                return
            warnings: list[str] = []
            attempted_optional_load = False
            if not self._change_loaded_once:
                attempted_optional_load = True
                try:
                    self._load_change_fields()
                    self._load_change_records()
                except Exception as exc:
                    self._change_loaded_once = True
                    warnings.append(self._source_sync_warning("变更源表", exc))
            if not self._zhihang_change_loaded_once:
                attempted_optional_load = True
                try:
                    self._load_zhihang_change_fields()
                    self._load_zhihang_change_records()
                except Exception as exc:
                    self._zhihang_change_loaded_once = True
                    warnings.append(self._source_sync_warning("智航变更源表", exc))
            if not self._repair_loaded_once:
                attempted_optional_load = True
                try:
                    self._load_repair_fields()
                    self._load_repair_records()
                except Exception as exc:
                    self._repair_loaded_once = True
                    warnings.append(self._source_sync_warning("检修源表", exc))
            if attempted_optional_load:
                self._load_warnings = warnings

    def ensure_snapshot_loaded(self) -> None:
        if (
            self._maintenance_loaded_once
            and self._change_loaded_once
            and self._repair_loaded_once
            and self._zhihang_change_loaded_once
        ):
            return
        acquired = False
        try:
            acquired = self._refresh_lock.acquire(blocking=False)
            if not acquired:
                self._hydrate_source_records_from_sqlite()
                return
            if not (
                self._maintenance_loaded_once
                and self._change_loaded_once
                and self._repair_loaded_once
                and self._zhihang_change_loaded_once
            ):
                self._hydrate_source_records_from_sqlite()
        finally:
            if acquired:
                self._refresh_lock.release()

    def _normalize_field_value(
        self,
        field_name: str,
        raw_value: Any,
        meta_by_name: dict[str, FieldMeta] | None = None,
    ) -> Any:
        meta_lookup = meta_by_name or self._field_meta_by_name
        meta = meta_lookup.get(field_name)
        option_map = meta.options_map if meta else {}
        if raw_value is None:
            return ""

        fallback_map = FIELD_OPTION_FALLBACK.get(field_name, {})

        def normalize_option_text(value: Any) -> str:
            text = str(value or "").strip()
            if not text:
                return ""
            if field_name == "变更进度" and CHANGE_PROGRESS_OPTION_FALLBACK.get(text):
                return CHANGE_PROGRESS_OPTION_FALLBACK[text]
            if fallback_map.get(text):
                return fallback_map[text]
            return option_map.get(text, text)

        if isinstance(raw_value, str):
            text = raw_value.strip()
            if field_name in REPAIR_DATETIME_FIELDS:
                formatted = self._format_source_datetime(text)
                if formatted:
                    return formatted
            return normalize_option_text(text)
        if isinstance(raw_value, (int, float)):
            if field_name in REPAIR_DATETIME_FIELDS:
                formatted = self._format_source_datetime(raw_value)
                if formatted:
                    return formatted
            if meta and meta.ui_type in {
                "DateTime",
                "CreatedTime",
                "ModifiedTime",
            }:
                try:
                    return dt.datetime.fromtimestamp(raw_value / 1000).strftime(
                        "%Y-%m-%d %H:%M"
                    )
                except Exception:
                    return str(raw_value)
            return str(raw_value)
        if isinstance(raw_value, dict):
            users = raw_value.get("users")
            if isinstance(users, list):
                names = [str(user.get("name") or "").strip() for user in users]
                return "、".join([name for name in names if name])
            for key in ("name", "text", "value"):
                value = str(raw_value.get(key) or "").strip()
                if value:
                    return normalize_option_text(value)
            option_id = str(raw_value.get("id") or "").strip()
            if option_id:
                resolved = normalize_option_text(option_id)
                if resolved:
                    return resolved
            return json.dumps(raw_value, ensure_ascii=False)
        if isinstance(raw_value, list):
            normalized_items: list[str] = []
            for item in raw_value:
                if isinstance(item, dict):
                    if "text" in item:
                        value = normalize_option_text(item.get("text"))
                    elif "name" in item:
                        value = normalize_option_text(item.get("name"))
                    elif "value" in item:
                        value = normalize_option_text(item.get("value"))
                    elif "id" in item:
                        value = normalize_option_text(item.get("id"))
                    else:
                        value = ""
                    if value:
                        normalized_items.append(value)
                    continue
                if isinstance(item, (int, float)) and field_name in REPAIR_DATETIME_FIELDS:
                    item_text = self._format_source_datetime(item)
                    if item_text:
                        normalized_items.append(item_text)
                    continue
                item_text = str(item or "").strip()
                value = normalize_option_text(item_text)
                if value:
                    normalized_items.append(value)
            return "、".join(normalized_items)
        return str(raw_value)

    def _normalize_record(
        self,
        item: dict[str, Any],
        *,
        meta_by_name: dict[str, FieldMeta] | None = None,
        work_type: str = WORK_TYPE_MAINTENANCE,
        notice_type: str = NOTICE_TYPE_MAINTENANCE,
        source_app_token: str = "",
        source_table_id: str = "",
    ) -> dict[str, Any]:
        raw_fields = item.get("fields") or {}
        display_fields = {
            field_name: self._normalize_field_value(
                field_name, raw_value, meta_by_name=meta_by_name
            )
            for field_name, raw_value in raw_fields.items()
        }
        return {
            "record_id": str(item.get("record_id") or item.get("id") or ""),
            "created_time": item.get("created_time") or item.get("created_at") or "",
            "last_modified_time": item.get("last_modified_time")
            or item.get("updated_time")
            or item.get("updated_at")
            or "",
            "raw_fields": raw_fields,
            "display_fields": display_fields,
            "work_type": work_type,
            "notice_type": notice_type,
            "source_app_token": source_app_token,
            "source_table_id": source_table_id,
        }

    @staticmethod
    def _current_month_label() -> str:
        return f"{dt.datetime.now().month}月"

    @staticmethod
    def _month_start(value: dt.datetime | None = None) -> dt.datetime:
        value = value or dt.datetime.now()
        return dt.datetime(value.year, value.month, 1)

    @classmethod
    def _recent_month_starts(cls) -> list[dt.datetime]:
        current = cls._month_start()
        previous_last_day = current - dt.timedelta(days=1)
        previous = dt.datetime(previous_last_day.year, previous_last_day.month, 1)
        return [current, previous]

    @classmethod
    def _recent_month_pairs(cls) -> list[tuple[str, str]]:
        return [
            (f"{month_start.month}月", month_start.strftime("%Y-%m"))
            for month_start in cls._recent_month_starts()
        ]

    @classmethod
    def _recent_month_labels(cls) -> list[str]:
        return [label for label, _key in cls._recent_month_pairs()]

    @classmethod
    def _recent_month_keys(cls) -> set[str]:
        return {key for _label, key in cls._recent_month_pairs()}

    @staticmethod
    def _normalize_month_label(value: Any) -> str:
        text = str(value or "").strip()
        match = re.search(r"(\d{1,2})\s*月", text)
        return f"{int(match.group(1))}月" if match else text

    @classmethod
    def _specific_recent_month_label(cls, value: Any) -> str | None:
        text = cls._normalize_month_label(value)
        if not text or text == RECENT_MONTH_FILTER_LABEL:
            return ""
        return text if text in cls._recent_month_labels() else None

    @classmethod
    def _specific_recent_month_key(cls, value: Any) -> str | None:
        label = cls._specific_recent_month_label(value)
        if label is None:
            return None
        if not label:
            return ""
        for recent_label, recent_key in cls._recent_month_pairs():
            if recent_label == label:
                return recent_key
        return None

    @classmethod
    def _recent_month_filter_options(cls) -> list[str]:
        return [RECENT_MONTH_FILTER_LABEL, *cls._recent_month_labels()]

    @staticmethod
    def _format_input_datetime(value: Any) -> str:
        text = str(value or "").strip()
        return text.replace("T", " ")

    @classmethod
    def _parse_notice_datetime(cls, value: Any) -> dt.datetime | None:
        text = cls._format_input_datetime(value).replace("/", "-").strip()
        if not text:
            return None
        match = re.search(
            r"(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{1,2})",
            text,
        )
        if not match:
            match = re.search(
                r"(\d{4})[年-](\d{1,2})[月-](\d{1,2})日?\s*"
                r"(\d{1,2})(?:[：:点时.](\d{1,2}))?",
                text,
            )
        if not match:
            return None
        try:
            return dt.datetime(
                int(match.group(1)),
                int(match.group(2)),
                int(match.group(3)),
                int(match.group(4)),
                int(match.group(5) or 0),
            )
        except Exception:
            return None

    @classmethod
    def _validate_minimum_notice_duration(
        cls,
        start_time: Any,
        end_time: Any,
        *,
        start_label: str = "开始时间",
        end_label: str = "结束时间",
    ) -> None:
        start_dt = cls._parse_notice_datetime(start_time)
        end_dt = cls._parse_notice_datetime(end_time)
        if not start_dt or not end_dt:
            return
        if (end_dt - start_dt).total_seconds() < 3600:
            raise PortalError(f"{start_label}和{end_label}之间不能少于1小时。")

    @staticmethod
    def _has_site_photo_payload(payload: dict[str, Any] | None) -> bool:
        payload = payload if isinstance(payload, dict) else {}
        images = payload.get("extra_images")
        if not isinstance(images, list):
            images = payload.get("site_photos")
        if not isinstance(images, list):
            return False
        for item in images:
            if isinstance(item, dict):
                if str(
                    item.get("upload_id")
                    or item.get("file_token")
                    or item.get("token")
                    or ""
                ).strip():
                    return True
            elif str(item or "").strip():
                return True
        return False

    @classmethod
    def _end_site_photo_required(
        cls,
        *,
        notice_type: Any = "",
        work_type: Any = "",
    ) -> bool:
        notice_text = str(notice_type or "").strip()
        work_text = str(work_type or "").strip()
        if notice_text in END_SITE_PHOTO_REQUIRED_NOTICE_TYPES:
            return True
        if work_text in END_SITE_PHOTO_REQUIRED_WORK_TYPES:
            return True
        mapped = WORK_TYPE_BY_NOTICE_TYPE.get(notice_text, "")
        return mapped in END_SITE_PHOTO_REQUIRED_WORK_TYPES

    @classmethod
    def _require_end_site_photo(
        cls,
        request_payload: dict[str, Any],
        action: str,
        *,
        notice_type: Any = "",
        work_type: Any = "",
    ) -> None:
        if str(action or "").strip().lower() != "end":
            return
        if not cls._end_site_photo_required(
            notice_type=notice_type,
            work_type=work_type,
        ):
            return
        if cls._has_site_photo_payload(request_payload):
            return
        raise PortalError("结束通告前必须添加至少一张现场照片。")

    @staticmethod
    def _site_photo_payload(request_payload: dict[str, Any]) -> list[dict[str, Any]]:
        images = request_payload.get("extra_images")
        if not isinstance(images, list):
            images = request_payload.get("site_photos")
        if not isinstance(images, list):
            return []
        result: list[dict[str, Any]] = []
        for item in images:
            if isinstance(item, dict):
                copied = dict(item)
                if str(
                    copied.get("upload_id")
                    or copied.get("file_token")
                    or copied.get("token")
                    or ""
                ).strip():
                    result.append(copied)
        return result

    @staticmethod
    def _format_source_datetime(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return ""
            if re.fullmatch(r"\d+(\.\d+)?", text):
                try:
                    value = float(text)
                except Exception:
                    return text.replace("T", " ")
            else:
                return text.replace("T", " ")
        if isinstance(value, (int, float)):
            numeric = float(value)
            try:
                if numeric > 10_000_000_000:
                    return dt.datetime.fromtimestamp(numeric / 1000).strftime(
                        "%Y-%m-%d %H:%M"
                    )
                if numeric > 1_000_000_000:
                    return dt.datetime.fromtimestamp(numeric).strftime(
                        "%Y-%m-%d %H:%M"
                    )
                if 20_000 <= numeric <= 80_000:
                    base = dt.datetime(1899, 12, 30)
                    return (base + dt.timedelta(days=numeric)).strftime(
                        "%Y-%m-%d %H:%M"
                    )
            except Exception:
                return str(value)
        return str(value or "").strip()

    @staticmethod
    def _building_code_from_value(value: Any) -> str:
        text = str(value or "").strip().upper()
        if "110" in text:
            return "110"
        match = re.search(r"[ABCDEH]", text)
        return match.group(0) if match else ""

    @classmethod
    def _building_codes_from_value(cls, value: Any) -> list[str]:
        values = value if isinstance(value, (list, tuple, set)) else [value]
        codes: list[str] = []
        for raw in values:
            raw_text = str(raw or "").strip()
            mapped = REPAIR_BUILDING_OPTION_FALLBACK.get(raw_text)
            text = str(mapped or raw_text).upper()
            if not text:
                continue
            if "110" in text and "110" not in codes:
                codes.append("110")
            for code in re.findall(r"[ABCDEH]", text):
                if code not in codes:
                    codes.append(code)
        return [code for code in BUILDING_SCOPE_CODES if code in codes]

    @classmethod
    def _building_label_from_code(cls, code: str) -> str:
        code = str(code or "").strip().upper()
        if code == "110":
            return "110站"
        if code in {"A", "B", "C", "D", "E", "H"}:
            return f"{code}楼"
        if code == "CAMPUS":
            return "园区"
        return code

    @classmethod
    def _building_label_from_codes(cls, codes: list[str]) -> str:
        return "、".join(cls._building_label_from_code(code) for code in codes if code)

    @classmethod
    def _recipients_for_building(cls, building: str) -> tuple[str, list[str], str]:
        code = cls._building_code_from_value(building)
        if not code:
            return "", [], f"无法从楼栋字段识别 110/A-E/H: {building or '-'}"
        building_open_id = BUILDING_OPEN_ID_MAP.get(code, "")
        if not building_open_id:
            return code, [], f"未配置 {cls._scope_label(code)} openid"
        return code, list(dict.fromkeys([building_open_id, LI_SHILONG_OPEN_ID])), ""

    @classmethod
    def _recipients_for_building_codes(
        cls, building_codes: list[str], *, fallback_building: str = ""
    ) -> tuple[str, list[str], str]:
        codes = [
            str(code or "").strip().upper()
            for code in (building_codes or [])
            if str(code or "").strip()
        ]
        codes = [code for code in BUILDING_SCOPE_CODES if code in dict.fromkeys(codes)]
        if not codes and fallback_building:
            codes = cls._building_codes_from_value(fallback_building)
        if not codes:
            return "", [], f"无法从楼栋字段识别 110/A-E/H: {fallback_building or '-'}"
        recipients: list[str] = []
        missing: list[str] = []
        for code in codes:
            open_id = BUILDING_OPEN_ID_MAP.get(code, "")
            if open_id:
                recipients.append(open_id)
            else:
                missing.append(cls._scope_label(code))
        recipients.append(LI_SHILONG_OPEN_ID)
        recipients = [item for item in dict.fromkeys(str(open_id or "").strip() for open_id in recipients) if item]
        if missing:
            return codes[0] if len(codes) == 1 else "CAMPUS", recipients, f"未配置 {','.join(missing)} openid"
        return codes[0] if len(codes) == 1 else "CAMPUS", recipients, ""

    @staticmethod
    def _normalize_scope(scope: Any) -> str:
        text = str(scope or "ALL").strip().upper()
        if text in {"全部", "ALL", ""}:
            return "ALL"
        if text in {"园区", "CAMPUS", "PARK"}:
            return "CAMPUS"
        if "110" in text:
            return "110"
        match = re.search(r"[ABCDEH]", text)
        return match.group(0) if match else "ALL"

    @staticmethod
    def _scope_label(scope: Any) -> str:
        normalized = MaintenancePortalService._normalize_scope(scope)
        if normalized == "ALL":
            return "全部"
        if normalized == "CAMPUS":
            return "园区"
        if normalized == "110":
            return "110站"
        return f"{normalized}楼"

    @classmethod
    def _handover_scope_options(cls) -> list[dict[str, str]]:
        return [
            {"value": code, "label": cls._scope_label(code)}
            for code in BUILDING_SCOPE_CODES
        ]

    @classmethod
    def _scope_matches_building(cls, scope: Any, building: Any) -> bool:
        return cls._scope_matches_buildings(scope, cls._building_codes_from_value(building))

    @classmethod
    def _scope_matches_buildings(cls, scope: Any, building_codes: list[str] | tuple[str, ...] | set[str]) -> bool:
        normalized = cls._normalize_scope(scope)
        if normalized == "ALL":
            return True
        codes = [str(code or "").strip().upper() for code in building_codes if str(code or "").strip()]
        codes = [code for code in BUILDING_SCOPE_CODES if code in codes]
        if normalized == "CAMPUS":
            return len(codes) >= 2
        return len(codes) == 1 and codes[0] == normalized

    @classmethod
    def _scope_matches_item(cls, scope: Any, item: dict[str, Any]) -> bool:
        codes = item.get("building_codes")
        if not isinstance(codes, list):
            codes = []
        if not codes and str(item.get("building_code") or "").strip().upper() == "CAMPUS":
            codes = cls._building_codes_from_value(item.get("building"))
        if not codes:
            codes = cls._building_codes_from_value(item.get("building"))
        return cls._scope_matches_buildings(scope, codes)

    @classmethod
    def _clean_building_codes(cls, building_codes: Any) -> list[str]:
        codes = building_codes if isinstance(building_codes, (list, tuple, set)) else []
        normalized: list[str] = []
        for code in codes:
            value = str(code or "").strip().upper()
            if value in BUILDING_SCOPE_CODES and value not in normalized:
                normalized.append(value)
        return [code for code in BUILDING_SCOPE_CODES if code in normalized]

    def _building_codes_from_request_payload(
        self, request_payload: dict[str, Any]
    ) -> list[str]:
        codes = self._clean_building_codes(request_payload.get("building_codes"))
        if codes:
            return codes
        return self._building_codes_from_value(request_payload.get("building"))

    def _source_record_in_scope_snapshot(
        self, *, record_id: str, work_type: str, scope: str
    ) -> dict[str, Any] | None:
        record_id = str(record_id or "").strip()
        if not record_id:
            return None
        records = self._source_snapshot_records(scope)
        if records is None:
            return None
        for record in records:
            if (
                self._record_work_type(record) == work_type
                and str(record.get("record_id") or "").strip() == record_id
            ):
                return record
        return None

    def _resolve_scoped_source_building_codes(
        self,
        *,
        scope: str,
        work_type: str,
        record_id: str,
        source_codes: list[str] | tuple[str, ...] | set[str],
        payload_codes: list[str] | tuple[str, ...] | set[str],
    ) -> list[str]:
        source_building_codes = self._clean_building_codes(source_codes)
        payload_building_codes = self._clean_building_codes(payload_codes)
        if self._scope_matches_buildings(scope, source_building_codes):
            return source_building_codes
        if (
            payload_building_codes
            and self._scope_matches_buildings(scope, payload_building_codes)
            and (
                not source_building_codes
                or self._source_record_in_scope_snapshot(
                    record_id=record_id,
                    work_type=work_type,
                    scope=scope,
                )
                is not None
            )
        ):
            return payload_building_codes
        return source_building_codes or payload_building_codes

    def _sorted_unique_option_names(self, field_name: str) -> list[str]:
        if field_name == "计划维护月份":
            return self._recent_month_filter_options()
        records = self._source_snapshot_records("ALL")
        if records is None:
            records = list(self._records)
        values = {
            str(record["display_fields"].get(field_name) or "").strip()
            for record in records
            if self._maintenance_status_allows_workbench(record)
        }
        options = sorted([value for value in values if value])
        if options:
            return options
        meta = self._field_meta_by_name.get(field_name)
        if meta and meta.option_names:
            return [name for name in meta.option_names if name]
        return []

    def _sorted_unique_work_specialties(self) -> list[str]:
        records = self._source_snapshot_records("ALL")
        if records is None:
            maintenance_records = list(self._records)
            change_records = list(self._change_records)
            repair_records = list(self._repair_records)
        else:
            maintenance_records = [
                record for record in records if self._record_work_type(record) == WORK_TYPE_MAINTENANCE
            ]
            change_records = [
                record for record in records if self._record_work_type(record) == WORK_TYPE_CHANGE
            ]
            repair_records = [
                record for record in records if self._record_work_type(record) == WORK_TYPE_REPAIR
            ]
        values = {
            str(record["display_fields"].get("专业类别") or "").strip()
            for record in maintenance_records
            if self._maintenance_status_allows_workbench(record)
            and self._source_record_matches_month_window(record)
        }
        values.update(
            {
                self._change_specialty(record)
                for record in change_records
                if self._change_progress_allows_workbench(record)
                and self._source_record_matches_month_window(record)
            }
        )
        values.update(
            {
                self._repair_specialty(record)
                for record in repair_records
                if self._is_valid_repair_record(record)
                and self._source_record_matches_month_window(record)
            }
        )
        return sorted([value for value in values if value])

    def _filter_records(
        self,
        *,
        month: str = "",
        specialty: str = "",
        building: str = "",
        scope: str = "ALL",
    ) -> list[dict[str, Any]]:
        month = str(month or "").strip()
        specialty = str(specialty or "").strip()
        building = str(building or "").strip()
        scope = self._normalize_scope(scope)
        filtered = []
        for record in self._records:
            display_fields = record["display_fields"]
            if not self._maintenance_status_allows_workbench(record):
                continue
            if not self._maintenance_record_matches_month_window(record, month):
                continue
            if specialty and display_fields.get("专业类别") != specialty:
                continue
            if building and display_fields.get("楼栋") != building:
                continue
            if not self._scope_matches_building(scope, display_fields.get("楼栋")):
                continue
            filtered.append(record)
        return filtered

    @staticmethod
    def _maintenance_status_value(record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        return str(fields.get("维护实施状态") or "").strip()

    def _maintenance_status_allows_workbench(self, record: dict[str, Any]) -> bool:
        return self._maintenance_status_value(record) in WORKBENCH_SOURCE_STATUSES

    def _change_record_building_codes(self, record: dict[str, Any]) -> list[str]:
        fields = record.get("display_fields") or {}
        raw_building = (
            fields.get("变更楼栋")
            or fields.get("楼栋")
            or record.get("building_codes")
            or ""
        )
        return self._building_codes_from_value(raw_building)

    @staticmethod
    def _change_progress_value(record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        return str(fields.get("变更进度") or "").strip()

    def _change_progress_allows_workbench(self, record: dict[str, Any]) -> bool:
        return self._change_progress_value(record) in CHANGE_WORKBENCH_PROGRESS_VALUES

    def _change_specialty(self, record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        return self._clean_source_text(fields.get("专业"))

    @classmethod
    def _repair_building_codes_from_value(cls, value: Any) -> list[str]:
        values = value if isinstance(value, (list, tuple, set)) else [value]
        codes: list[str] = []
        for raw in values:
            text = str(raw or "").strip().upper()
            if not text:
                continue
            if re.search(r"110\s*(?:站|楼|机房|数据中心)?", text) and "110" not in codes:
                codes.append("110")
            for code in ("A", "B", "C", "D", "E", "H"):
                patterns = (
                    rf"(?<![A-Z0-9]){code}\s*(?:楼|栋|座|区|机房|数据中心|DC)",
                    rf"(?:楼栋|楼宇|数据中心)\s*{code}(?![A-Z0-9])",
                    rf"^(?:南通)?{code}$",
                )
                if any(re.search(pattern, text) for pattern in patterns) and code not in codes:
                    codes.append(code)
        return [code for code in BUILDING_SCOPE_CODES if code in codes]

    @staticmethod
    def _clean_source_text(value: Any) -> str:
        text = str(value or "").strip()
        if text in PLACEHOLDER_TEXT_VALUES:
            return ""
        if re.fullmatch(r"opt[A-Za-z0-9]{6,}", text):
            return ""
        return text

    def _repair_notice_title(self, fields: dict[str, Any]) -> str:
        return self._clean_source_text(
            fields.get("检修通告名称") or fields.get("维修名称")
        )

    def _repair_title(self, record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        title = self._repair_notice_title(fields)
        if title:
            return title
        fallback_title = str(record.get("title") or "").strip()
        if fallback_title:
            return fallback_title
        return str(record.get("record_id") or "").strip()

    @staticmethod
    def _repair_level_from_event_level(value: Any) -> str:
        text = str(value or "").strip().upper()
        if re.search(r"(^|[^A-Z0-9])I3([^A-Z0-9]|$)", text) or text == "低":
            return "低"
        if re.search(r"(^|[^A-Z0-9])I2([^A-Z0-9]|$)", text) or text == "中":
            return "中"
        return ""

    def _repair_level(self, fields: dict[str, Any]) -> str:
        return self._repair_level_from_event_level(fields.get("对应事件等级"))

    def _repair_specialty(self, record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        specialty = self._clean_source_text(fields.get("所属专业"))
        if specialty:
            return specialty
        pushed_raw = str(fields.get("专业（推送消息用）") or "").strip()
        return REPAIR_SPECIALTY_OPTION_FALLBACK.get(
            pushed_raw, self._clean_source_text(pushed_raw)
        )

    def _repair_record_building_codes(self, record: dict[str, Any]) -> list[str]:
        fields = record.get("display_fields") or {}
        for field_name in (
            "所属数据中心/楼栋-使用",
            "所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）",
        ):
            codes = self._repair_building_codes_from_value(fields.get(field_name))
            if codes:
                return codes
        fallback_codes = self._repair_building_codes_from_value(record.get("building_codes"))
        if fallback_codes:
            return fallback_codes
        return (
            self._repair_building_codes_from_value(self._repair_title(record))
            or self._repair_building_codes_from_value(fields.get("维修名称"))
        )

    def _is_valid_repair_record(self, record: dict[str, Any]) -> bool:
        fields = record.get("display_fields") or {}
        title = self._repair_notice_title(fields)
        if not title:
            return False
        return bool(self._repair_record_building_codes(record))

    def _repair_time_range(self, record: dict[str, Any]) -> tuple[str, str, str]:
        fields = record.get("display_fields") or {}
        start_time = self._clean_source_text(
            fields.get("维修开始时间")
            or fields.get("故障发生时间")
            or fields.get("发现故障时间")
        )
        end_time = self._clean_source_text(
            fields.get("维修结束时间")
            or fields.get("维修结束时间（2026）")
            or fields.get("期望完成时间")
        )
        time_text = f"{start_time}~{end_time}" if start_time or end_time else ""
        return start_time, end_time, time_text

    def _repair_first_field(self, fields: dict[str, Any], *field_names: str) -> str:
        for field_name in field_names:
            value = self._clean_source_text(fields.get(field_name))
            if value:
                return value
        return ""

    def _repair_device_text(self, fields: dict[str, Any]) -> str:
        device_no = self._clean_source_text(fields.get("设备编号"))
        device_name = self._clean_source_text(fields.get("设备名称"))
        if device_no and device_name:
            return f"{device_no}{device_name}"
        if device_no or device_name:
            return device_no or device_name
        return self._repair_first_field(fields, "维修设备", "资产名称", "设备")

    def _repair_location_text(self, fields: dict[str, Any]) -> str:
        return self._repair_first_field(
            fields,
            "地点",
            "位置",
            "区域",
            "数据中心",
            "所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）",
            "所属数据中心/楼栋-使用",
        )

    def _repair_target_record_id(self, record: dict[str, Any]) -> str:
        raw_value = (record.get("raw_fields") or {}).get("设备检修关联")
        values = raw_value if isinstance(raw_value, list) else [raw_value]
        for item in values:
            if not isinstance(item, dict):
                continue
            record_ids = item.get("record_ids")
            if isinstance(record_ids, list):
                for record_id in record_ids:
                    text = str(record_id or "").strip()
                    if text:
                        return text
            text = str(item.get("record_id") or item.get("id") or "").strip()
            if text:
                return text
        return ""

    @staticmethod
    def _decode_repair_sync_source_id(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            padded = text + ("=" * (-len(text) % 4))
            return base64.b64decode(padded).decode("utf-8", errors="replace")
        except Exception:
            return ""

    def _repair_sync_record_matches_target(
        self, record: dict[str, Any], target_record_id: str
    ) -> bool:
        target_record_id = str(target_record_id or "").strip()
        if not target_record_id:
            return False
        fields = record.get("display_fields") or record.get("fields") or {}
        source_id = fields.get("SourceID")
        if not isinstance(source_id, str):
            source_id = self._normalize_field_value("SourceID", source_id, {})
        decoded = self._decode_repair_sync_source_id(source_id)
        parts = [part.strip() for part in decoded.split(":")]
        return len(parts) >= 2 and parts[1] == target_record_id

    def _find_repair_sync_record_id(
        self,
        *,
        target_record_id: str,
        sync_app_token: str = REPAIR_SOURCE_APP_TOKEN,
        sync_table_id: str = REPAIR_SYNC_TABLE_ID,
        max_pages: int = 40,
    ) -> str:
        target_record_id = str(target_record_id or "").strip()
        if not target_record_id:
            return ""
        page_token = ""
        pages = 0
        while pages < max_pages:
            pages += 1
            params: dict[str, Any] = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            payload = self._request_json(
                "records",
                params=params,
                app_token=sync_app_token,
                table_id=sync_table_id,
            )
            data = payload.get("data") or {}
            for item in data.get("items") or []:
                if not isinstance(item, dict):
                    continue
                if self._repair_sync_record_matches_target(item, target_record_id):
                    return str(item.get("record_id") or item.get("id") or "").strip()
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "").strip()
            if not page_token:
                break
        return ""

    @staticmethod
    def _linked_record_ids_from_value(value: Any) -> list[str]:
        linked: list[str] = []

        def add(candidate: Any) -> None:
            text = str(candidate or "").strip()
            if text and text not in linked:
                linked.append(text)

        if isinstance(value, dict):
            for key in ("link_record_ids", "record_ids"):
                ids = value.get(key)
                if isinstance(ids, list):
                    for item in ids:
                        add(item)
            add(value.get("record_id") or value.get("id"))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    for key in ("link_record_ids", "record_ids"):
                        ids = item.get(key)
                        if isinstance(ids, list):
                            for record_id in ids:
                                add(record_id)
                    add(item.get("record_id") or item.get("id"))
                else:
                    add(item)
        else:
            add(value)
        return linked

    def _source_repair_link_record_ids(
        self, *, source_app_token: str, source_table_id: str, source_record_id: str
    ) -> list[str]:
        payload = self._request_json(
            f"records/{source_record_id}",
            app_token=source_app_token,
            table_id=source_table_id,
        )
        record = (payload.get("data") or {}).get("record") or {}
        fields = record.get("fields") or {}
        return self._linked_record_ids_from_value(fields.get(REPAIR_LINK_FIELD_NAME))

    def _write_source_repair_link(
        self,
        *,
        source_app_token: str,
        source_table_id: str,
        source_record_id: str,
        sync_record_id: str,
    ) -> None:
        linked_ids = self._source_repair_link_record_ids(
            source_app_token=source_app_token,
            source_table_id=source_table_id,
            source_record_id=source_record_id,
        )
        if sync_record_id not in linked_ids:
            linked_ids.append(sync_record_id)
        self._patch_record_fields(
            app_token=source_app_token,
            table_id=source_table_id,
            record_id=source_record_id,
            fields={REPAIR_LINK_FIELD_NAME: {"link_record_ids": linked_ids}},
        )

    def _repair_has_started(self, record: dict[str, Any]) -> bool:
        fields = record.get("display_fields") or {}
        return bool(self._clean_source_text(fields.get("维修开始时间")))

    def _repair_has_ended(self, record: dict[str, Any]) -> bool:
        fields = record.get("display_fields") or {}
        return bool(
            self._clean_source_text(
                fields.get("维修结束时间") or fields.get("维修结束时间（2026）")
            )
        )

    def _repair_source_status(self, record: dict[str, Any]) -> str:
        if self._repair_has_ended(record):
            return "已结束"
        return MAINTENANCE_STATUS_ONGOING if self._repair_has_started(record) else DEFAULT_MAINTENANCE_STATUS

    def _filter_change_records(
        self,
        *,
        month: str = "",
        specialty: str = "",
        scope: str = "ALL",
        progress_values: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        specialty = str(specialty or "").strip()
        scope = self._normalize_scope(scope)
        filtered: list[dict[str, Any]] = []
        for record in self._change_records:
            fields = record.get("display_fields") or {}
            progress = self._change_progress_value(record)
            if progress_values is None:
                if not self._change_progress_allows_workbench(record):
                    continue
            elif progress not in progress_values:
                continue
            if not self._source_record_matches_month_window(record, month):
                continue
            if specialty and specialty != self._change_specialty(record):
                continue
            codes = self._change_record_building_codes(record)
            if not self._scope_matches_buildings(scope, codes):
                continue
            filtered.append(record)
        return filtered

    def _zhihang_change_primary_field_name(self) -> str:
        for meta in self._zhihang_change_field_meta_list:
            if meta.is_primary:
                return meta.field_name
        return ""

    def _zhihang_change_title(self, record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        for name in (
            "标题",
            "名称",
            "变更名称",
            "变更标题",
            "变更简述",
            "工作内容",
            self._zhihang_change_primary_field_name(),
        ):
            value = str(fields.get(name) or "").strip()
            if value:
                return value
        return str(record.get("record_id") or "").strip()

    def _zhihang_change_progress(self, record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        return str(fields.get("进度") or fields.get("进展") or "").strip()

    def _zhihang_change_building_codes(self, record: dict[str, Any]) -> list[str]:
        title = self._zhihang_change_title(record).upper()
        if not title:
            return []
        codes: list[str] = []
        if "110" in title:
            codes.append("110")
        for match in re.findall(
            r"(?<![A-Z0-9])([ABCDEH]{2,})(?=楼|栋|座|区|园区|[^A-Z0-9]|$)",
            title,
        ):
            for code in match:
                if code not in codes:
                    codes.append(code)
        for code in ("A", "B", "C", "D", "E", "H"):
            if (
                f"{code}楼" in title
                or f"{code}栋" in title
                or f"{code}座" in title
            ) and code not in codes:
                codes.append(code)
        if "园区" in title and len(codes) < 2:
            codes.extend([code for code in ("A", "B", "C") if code not in codes])
        return [code for code in BUILDING_SCOPE_CODES if code in codes]

    def _zhihang_change_scope_all(self, record: dict[str, Any]) -> bool:
        return not self._zhihang_change_building_codes(record)

    def _scope_matches_zhihang_change_record(
        self, scope: Any, record: dict[str, Any]
    ) -> bool:
        scope = self._normalize_scope(scope)
        if scope == "ALL":
            return True
        if self._zhihang_change_scope_all(record):
            return True
        return self._scope_matches_buildings(
            scope, self._zhihang_change_building_codes(record)
        )

    def _linked_zhihang_record_ids(
        self, ongoing_items: list[dict[str, Any]] | None = None
    ) -> set[str]:
        linked: set[str] = set()
        for item in ongoing_items or []:
            if not isinstance(item, dict):
                continue
            for field in ("zhihang_record_id", "lan_zhihang_record_id"):
                value = str(item.get(field) or "").strip()
                if value:
                    linked.add(value)
        with self._summary_lock:
            status_items = self._load_work_status_items_locked("ALL")
        for item in status_items:
            value = str(item.get("zhihang_record_id") or "").strip()
            if value:
                linked.add(value)
        return linked

    def _filter_zhihang_change_records(
        self,
        *,
        month: str = "",
        scope: str = "ALL",
        exclude_record_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        snapshot_records = self._source_snapshot_zhihang_records(scope)
        if snapshot_records is not None:
            return self._filter_zhihang_change_records_list(
                snapshot_records,
                month=month,
                exclude_record_ids=exclude_record_ids,
            )
        return self._filter_zhihang_change_records_from_memory(
            month=month,
            scope=scope,
            exclude_record_ids=exclude_record_ids,
        )

    def _filter_zhihang_change_records_from_memory(
        self,
        *,
        month: str = "",
        scope: str = "ALL",
        exclude_record_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        records = [
            record
            for record in self._zhihang_change_records
            if self._scope_matches_zhihang_change_record(scope, record)
        ]
        return self._filter_zhihang_change_records_list(
            records,
            month=month,
            exclude_record_ids=exclude_record_ids,
        )

    def _filter_zhihang_change_records_list(
        self,
        records: list[dict[str, Any]],
        *,
        month: str = "",
        exclude_record_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        excluded = {str(value or "").strip() for value in (exclude_record_ids or set())}
        filtered: list[dict[str, Any]] = []
        for record in records or []:
            record_id = str(record.get("record_id") or "").strip()
            if record_id and record_id in excluded:
                continue
            progress = self._zhihang_change_progress(record)
            if progress == ZHIHANG_PROGRESS_ENDED:
                continue
            if not self._source_record_matches_month_window(record, month):
                continue
            filtered.append(record)
        return filtered

    def _find_zhihang_change_record_by_id(self, record_id: str) -> dict[str, Any]:
        record_id = str(record_id or "").strip()
        for record in self._zhihang_change_records:
            if str(record.get("record_id") or "").strip() == record_id:
                return record
        snapshot_records = self._source_snapshot_zhihang_records("ALL")
        for record in snapshot_records or []:
            if str(record.get("record_id") or "").strip() == record_id:
                return record
        raise PortalError(f"未找到智航侧变更记录: {record_id}")

    def _filter_repair_records(
        self,
        *,
        month: str = "",
        specialty: str = "",
        scope: str = "ALL",
        include_started: bool | None = None,
    ) -> list[dict[str, Any]]:
        specialty = str(specialty or "").strip()
        scope = self._normalize_scope(scope)
        filtered: list[dict[str, Any]] = []
        for record in self._repair_records:
            if not self._is_valid_repair_record(record):
                continue
            if self._repair_has_ended(record):
                continue
            if include_started is not None and self._repair_has_started(record) != bool(include_started):
                continue
            if self._repair_source_status(record) not in WORKBENCH_SOURCE_STATUSES:
                continue
            if not self._source_record_matches_month_window(record, month):
                continue
            if specialty and specialty != self._repair_specialty(record):
                continue
            if not self._scope_matches_buildings(
                scope, self._repair_record_building_codes(record)
            ):
                continue
            filtered.append(record)
        return filtered

    @staticmethod
    def _normalize_memory_key(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "").strip()).lower()

    @staticmethod
    def _safe_memory_filename(building: str) -> str:
        raw = str(building or "").strip()
        name = re.sub(r"[^\w\u4e00-\u9fff.-]+", "_", raw)
        name = name.strip("._") or "unknown"
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
        return f"{name[:70]}_{digest}.json"

    def _building_memory_path(self, building: str) -> Path:
        return self._memory_dir / self._safe_memory_filename(building)

    def _building_memory_key(self, building: str) -> str:
        raw = str(building or "").strip()
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
        return raw or f"unknown:{digest}"

    def _load_building_memory_locked(self, building: str) -> dict[str, Any]:
        key = self._building_memory_key(building)
        stored = self._state_store.get_document(STATE_NS_MEMORY, key)
        if isinstance(stored, dict):
            items = stored.get("items")
            if not isinstance(items, dict):
                stored["items"] = {}
            stored["building"] = str(stored.get("building") or building or "")
            return stored
        path = self._building_memory_path(building)
        if not path.exists():
            return {"building": building, "items": {}}
        try:
            with path.open("r", encoding="utf-8") as fp:
                payload = json.load(fp)
            if not isinstance(payload, dict):
                return {"building": building, "items": {}}
            items = payload.get("items")
            if not isinstance(items, dict):
                payload["items"] = {}
            self._state_store.put_document(STATE_NS_MEMORY, key, payload)
            return payload
        except Exception:
            return {"building": building, "items": {}}

    def _save_building_memory_locked(self, building: str, payload: dict[str, Any]) -> None:
        self._state_store.put_document(
            STATE_NS_MEMORY, self._building_memory_key(building), payload
        )

    def _summary_path(self, day: str | None = None) -> Path:
        day = str(day or dt.datetime.now().strftime("%Y-%m-%d")).strip()
        day = re.sub(r"[^0-9-]+", "", day) or dt.datetime.now().strftime("%Y-%m-%d")
        return self._summary_dir / f"{day}.json"

    def _load_day_summary_locked(self, day: str | None = None) -> dict[str, Any]:
        path = self._summary_path(day)
        stored = self._state_store.get_document(STATE_NS_DAILY_SUMMARY, path.stem)
        if isinstance(stored, dict):
            if not isinstance(stored.get("items"), list):
                stored["items"] = []
            stored["date"] = str(stored.get("date") or path.stem)
            return stored
        if not path.exists():
            return {"date": path.stem, "items": []}
        try:
            with path.open("r", encoding="utf-8") as fp:
                payload = json.load(fp)
            if not isinstance(payload, dict):
                return {"date": path.stem, "items": []}
            if not isinstance(payload.get("items"), list):
                payload["items"] = []
            payload["date"] = str(payload.get("date") or path.stem)
            self._state_store.put_document(STATE_NS_DAILY_SUMMARY, path.stem, payload)
            return payload
        except Exception:
            return {"date": path.stem, "items": []}

    def _save_day_summary_locked(self, payload: dict[str, Any], day: str | None = None) -> None:
        path = self._summary_path(day or payload.get("date"))
        self._state_store.put_document(STATE_NS_DAILY_SUMMARY, path.stem, payload)
        self._touch_state_cache_version()

    @staticmethod
    def _summary_key(
        *,
        source_record_id: str = "",
        title: str = "",
        building: str = "",
        reason: str = "",
        work_type: str = "",
    ) -> str:
        source_record_id = str(source_record_id or "").strip()
        work_type = str(work_type or "").strip()
        prefix = f"{work_type}:" if work_type else ""
        if source_record_id:
            return f"{prefix}source:{source_record_id}"
        title_key = re.sub(r"\s+", "", str(title or ""))
        building_key = re.sub(r"\s+", "", str(building or ""))
        reason_key = re.sub(r"\s+", "", str(reason or ""))
        suffix = f":reason:{reason_key}" if reason_key else ""
        return f"{prefix}title:{building_key}:{title_key}{suffix}" if title_key else ""

    @staticmethod
    def _work_status_fallback_key(
        *,
        title: str = "",
        building: str = "",
        plan_month: str = "",
        reason: str = "",
    ) -> str:
        title_key = re.sub(r"\s+", "", str(title or ""))
        building_key = re.sub(r"\s+", "", str(building or ""))
        month_key = re.sub(r"\s+", "", str(plan_month or ""))
        reason_key = re.sub(r"\s+", "", str(reason or ""))
        suffix = f":reason:{reason_key}" if reason_key else ""
        return f"{building_key}:{month_key}:{title_key}{suffix}" if title_key else ""

    def _record_fallback_key(self, record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        building = str(fields.get("楼栋") or "").strip()
        maintenance_total = str(fields.get("维护总项") or "").strip()
        title = f"EA118机房{building}{maintenance_total}" if maintenance_total else ""
        if record.get("work_type") == WORK_TYPE_CHANGE:
            fields = record.get("display_fields") or {}
            return self._work_status_fallback_key(
                title=str(fields.get("变更简述") or record.get("record_id") or ""),
                building=str(fields.get("变更楼栋") or ""),
                plan_month="",
            )
        if record.get("work_type") == WORK_TYPE_REPAIR:
            return self._work_status_fallback_key(
                title=self._repair_title(record),
                building=self._building_label_from_codes(
                    self._repair_record_building_codes(record)
                ),
                plan_month="",
            )
        return self._work_status_fallback_key(
            title=title,
            building=building,
            plan_month=str(fields.get("计划维护月份") or "").strip(),
        )

    def _record_legacy_summary_key(self, record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        building = str(fields.get("楼栋") or "").strip()
        maintenance_total = str(fields.get("维护总项") or "").strip()
        title = f"EA118机房{building}{maintenance_total}" if maintenance_total else ""
        return self._summary_key(title=title, building=building)

    def _work_status_path(self, building: str = "", building_code: str = "") -> Path:
        code = str(building_code or self._building_code_from_value(building) or "").strip()
        if code:
            name = code
        else:
            raw = str(building or "").strip()
            name = re.sub(r"[^\w\u4e00-\u9fff.-]+", "_", raw).strip("._") or "unknown"
            name = f"{name[:70]}_{hashlib.sha1(raw.encode('utf-8')).hexdigest()[:10]}"
        return self._work_status_dir / f"{name}.json"

    def _load_work_status_locked(self, building: str = "", building_code: str = "") -> dict[str, Any]:
        path = self._work_status_path(building, building_code)
        stored = self._state_store.get_document(STATE_NS_WORK_STATUS, path.stem)
        if isinstance(stored, dict):
            if not isinstance(stored.get("items"), list):
                stored["items"] = []
            stored["version"] = int(stored.get("version") or 1)
            stored["building"] = str(stored.get("building") or building or "")
            stored["building_code"] = str(
                stored.get("building_code")
                or building_code
                or self._building_code_from_value(stored.get("building") or building)
                or ""
            )
            return stored
        if not path.exists():
            code = str(building_code or self._building_code_from_value(building) or "").strip()
            return {"version": 1, "building": building, "building_code": code, "items": []}
        try:
            with path.open("r", encoding="utf-8") as fp:
                payload = json.load(fp)
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}
        if not isinstance(payload.get("items"), list):
            payload["items"] = []
        payload["version"] = int(payload.get("version") or 1)
        payload["building"] = str(payload.get("building") or building or "")
        payload["building_code"] = str(
            payload.get("building_code")
            or building_code
            or self._building_code_from_value(payload.get("building") or building)
            or ""
        )
        self._state_store.put_document(STATE_NS_WORK_STATUS, path.stem, payload)
        return payload

    def _save_work_status_locked(
        self, payload: dict[str, Any], *, building: str = "", building_code: str = ""
    ) -> None:
        path = self._work_status_path(
            building or payload.get("building") or "",
            building_code or payload.get("building_code") or "",
        )
        self._state_store.put_document(STATE_NS_WORK_STATUS, path.stem, payload)
        self._work_status_cache_signature = None
        self._work_status_cache_items = None
        self._touch_state_cache_version()

    def _migrate_legacy_day_summaries_locked(self) -> None:
        if self._legacy_summary_migrated:
            return
        self._legacy_summary_migrated = True
        for path in sorted(self._summary_dir.glob("*.json")):
            key = path.stem
            if self._state_store.get_document(STATE_NS_DAILY_SUMMARY, key):
                continue
            try:
                with path.open("r", encoding="utf-8") as fp:
                    payload = json.load(fp)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            if not isinstance(payload.get("items"), list):
                payload["items"] = []
            payload["date"] = str(payload.get("date") or key)
            self._state_store.put_document(STATE_NS_DAILY_SUMMARY, key, payload)

    def _iter_day_summary_payloads_locked(
        self, *, month: str = ""
    ) -> list[dict[str, Any]]:
        self._migrate_legacy_day_summaries_locked()
        documents = self._state_store.list_documents(
            STATE_NS_DAILY_SUMMARY, key_prefix=month
        )
        payloads: list[dict[str, Any]] = []
        for document in documents:
            payload = document.get("payload")
            if not isinstance(payload, dict):
                continue
            if not isinstance(payload.get("items"), list):
                payload["items"] = []
            payload["date"] = str(payload.get("date") or document.get("key") or "")
            payloads.append(payload)
        return payloads

    def _migrate_legacy_work_status_locked(self) -> None:
        if self._legacy_work_status_migrated:
            return
        self._legacy_work_status_migrated = True
        for path in sorted(self._work_status_dir.glob("*.json")):
            key = path.stem
            if self._state_store.get_document(STATE_NS_WORK_STATUS, key):
                continue
            try:
                with path.open("r", encoding="utf-8") as fp:
                    payload = json.load(fp)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            if not isinstance(payload.get("items"), list):
                payload["items"] = []
            payload["version"] = int(payload.get("version") or 1)
            self._state_store.put_document(STATE_NS_WORK_STATUS, key, payload)

    def _iter_work_status_payloads_locked(self) -> list[dict[str, Any]]:
        self._migrate_legacy_work_status_locked()
        payloads: list[dict[str, Any]] = []
        for document in self._state_store.list_documents(STATE_NS_WORK_STATUS):
            payload = document.get("payload")
            if not isinstance(payload, dict):
                continue
            if not isinstance(payload.get("items"), list):
                payload["items"] = []
            payloads.append(payload)
        return payloads

    def _find_summary_item(
        self,
        items: list[dict[str, Any]],
        *,
        key: str = "",
        active_item_id: str = "",
        title: str = "",
        building: str = "",
        reason: str = "",
        work_type: str = "",
    ) -> dict[str, Any] | None:
        key = str(key or "").strip()
        active_item_id = str(active_item_id or "").strip()
        reason_key = re.sub(r"\s+", "", str(reason or ""))
        if key:
            for item in items:
                if str(item.get("key") or "") == key:
                    return item
        if active_item_id:
            for item in items:
                if str(item.get("active_item_id") or "") == active_item_id:
                    return item
        fallback_key = self._summary_key(
            title=title, building=building, reason=reason, work_type=work_type
        )
        if fallback_key:
            for item in items:
                if str(item.get("fallback_key") or "") == fallback_key:
                    return item
        legacy_fallback_key = self._summary_key(title=title, building=building)
        if legacy_fallback_key and legacy_fallback_key != fallback_key:
            for item in items:
                item_reason_key = re.sub(r"\s+", "", str(item.get("reason") or ""))
                if (
                    str(item.get("fallback_key") or "") == legacy_fallback_key
                    and (not reason_key or item_reason_key == reason_key)
                ):
                    return item
        return None

    def _find_work_status_item(
        self,
        items: list[dict[str, Any]],
        *,
        source_record_id: str = "",
        active_item_id: str = "",
        key: str = "",
        fallback_key: str = "",
        work_fallback_key: str = "",
        work_type: str = "",
    ) -> dict[str, Any] | None:
        source_record_id = str(source_record_id or "").strip()
        active_item_id = str(active_item_id or "").strip()
        key = str(key or "").strip()
        fallback_key = str(fallback_key or "").strip()
        work_fallback_key = str(work_fallback_key or "").strip()
        work_type = str(work_type or WORK_TYPE_MAINTENANCE).strip()

        def _matches_work_type(item: dict[str, Any]) -> bool:
            item_work_type = str(
                item.get("work_type") or WORK_TYPE_MAINTENANCE
            ).strip()
            return item_work_type == work_type

        if source_record_id:
            for item in items:
                if (
                    _matches_work_type(item)
                    and str(item.get("source_record_id") or "").strip()
                    == source_record_id
                ):
                    return item
        if active_item_id:
            for item in items:
                if (
                    _matches_work_type(item)
                    and str(item.get("active_item_id") or "").strip()
                    == active_item_id
                ):
                    return item
        if key:
            for item in items:
                if _matches_work_type(item) and str(item.get("key") or "").strip() == key:
                    return item
        if fallback_key:
            for item in items:
                if (
                    _matches_work_type(item)
                    and str(item.get("fallback_key") or "").strip() == fallback_key
                ):
                    return item
        if work_fallback_key:
            for item in items:
                if (
                    _matches_work_type(item)
                    and str(item.get("work_fallback_key") or "").strip()
                    == work_fallback_key
                ):
                    return item
        return None

    @staticmethod
    def _date_part(value: Any) -> str:
        match = re.search(r"\d{4}-\d{1,2}-\d{1,2}", str(value or ""))
        return match.group(0) if match else ""

    def _upsert_work_status_item_locked(
        self,
        incoming: dict[str, Any],
        *,
        action: str = "",
        now: str = "",
    ) -> None:
        if not isinstance(incoming, dict):
            return
        action = str(action or "").strip().lower()
        now = str(now or dt.datetime.now().strftime("%Y-%m-%d %H:%M"))
        building = str(incoming.get("building") or "").strip()
        building_code = str(incoming.get("building_code") or "").strip()
        work_type = str(incoming.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        payload = self._load_work_status_locked(building, building_code)
        if building and not payload.get("building"):
            payload["building"] = building
        if building_code and not payload.get("building_code"):
            payload["building_code"] = building_code
        items = payload.setdefault("items", [])
        source_record_id = str(incoming.get("source_record_id") or "").strip()
        active_item_id = str(incoming.get("active_item_id") or "").strip()
        key = str(incoming.get("key") or "").strip()
        fallback_key = str(incoming.get("fallback_key") or "").strip()
        work_fallback_key = str(incoming.get("work_fallback_key") or "").strip()
        work_fallback_key = work_fallback_key or self._work_status_fallback_key(
            title=str(incoming.get("title") or ""),
            building=building,
            plan_month=str(incoming.get("plan_month") or ""),
            reason=str(incoming.get("reason") or ""),
        )
        item = self._find_work_status_item(
            items,
            source_record_id=source_record_id,
            active_item_id=active_item_id,
            key=key,
            fallback_key=fallback_key,
            work_fallback_key=work_fallback_key,
            work_type=work_type,
        )
        if item is None:
            item = {
                "key": key or fallback_key or uuid.uuid4().hex,
                "fallback_key": fallback_key,
                "work_fallback_key": work_fallback_key,
                "source_record_id": source_record_id,
                "active_item_id": active_item_id,
                "work_type": work_type,
                "actions": [],
            }
            items.append(item)
        item["work_type"] = str(item.get("work_type") or work_type)
        if work_fallback_key and not item.get("work_fallback_key"):
            item["work_fallback_key"] = work_fallback_key
        for field in (
            "key",
            "fallback_key",
            "work_fallback_key",
            "work_type",
            "notice_type",
            "source_app_token",
            "source_table_id",
            "source_record_id",
            "target_record_id",
            "active_item_id",
            "feishu_record_id",
            "title",
            "building",
            "building_code",
            "building_codes",
            "specialty",
            "level",
            "source_progress",
            "zhihang_involved",
            "zhihang_record_id",
            "zhihang_title",
            "zhihang_progress",
            "zhihang_source_app_token",
            "zhihang_source_table_id",
            "plan_month",
            "maintenance_total",
            "maintenance_no",
            "maintenance_project",
            "maintenance_cycle",
            "start_time",
            "end_time",
            "location",
            "content",
            "reason",
            "impact",
            "progress",
            "repair_device",
            "repair_fault",
            "fault_type",
            "repair_mode",
            "discovery",
            "symptom",
            "solution",
            "fault_time",
            "expected_time",
            "text",
        ):
            value = incoming.get(field)
            if value not in (None, ""):
                item[field] = value
        if incoming.get("started_at") and not item.get("started_at"):
            item["started_at"] = incoming["started_at"]
        if incoming.get("started_at"):
            item["started_date"] = self._date_part(incoming.get("started_at"))
        if incoming.get("last_updated_at"):
            item["last_updated_at"] = incoming["last_updated_at"]
        if incoming.get("ended_at"):
            item["ended_at"] = incoming["ended_at"]
            item["completed_date"] = self._date_part(incoming.get("ended_at"))
        if action == "end":
            item["status"] = "已结束"
            item["ended_at"] = str(incoming.get("ended_at") or now)
            item["completed_date"] = self._date_part(item.get("ended_at"))
        elif action in {"start", "update"}:
            if item.get("status") != "已结束":
                item["status"] = "进行中"
        elif incoming.get("status"):
            item["status"] = incoming["status"]
        for incoming_action in incoming.get("actions") or []:
            if not isinstance(incoming_action, dict):
                continue
            job_id = str(incoming_action.get("job_id") or "").strip()
            actions = item.setdefault("actions", [])
            if job_id and any(str(existing.get("job_id") or "").strip() == job_id for existing in actions):
                continue
            actions.append(copy.deepcopy(incoming_action))
        item["updated_at"] = now
        payload["updated_at"] = now
        self._save_work_status_locked(payload, building=building, building_code=building_code)

    def mark_action_upload_result(
        self,
        job_id: str,
        *,
        success: bool,
        message: str = "",
        record_id: str = "",
        active_item_id: str = "",
    ) -> None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return
        job = self.get_job(job_id) or {}
        prepared = job.get("prepared") if isinstance(job.get("prepared"), dict) else {}
        notice_text = str(job.get("notice_text") or prepared.get("text") or "").strip()
        message_error = str(job.get("message_error") or "").strip()
        message_warning = str(job.get("message_warning") or "").strip()
        if success and message_error:
            message_warning = (
                f"多维上传成功；个人消息发送失败，可复制通告文本：{message_error}"
            )
            message = message_warning
        phase = "success" if success else "failed"
        patch = {
            "phase": phase,
            "error": "" if success else str(message or "上传失败"),
            "upload_message": str(message or ""),
            "record_id": str(record_id or ""),
        }
        if notice_text:
            patch["notice_text"] = notice_text
            patch["copy_text"] = notice_text
        if message_warning:
            patch["message_warning"] = message_warning
        if message_error:
            patch["message_error"] = message_error
            patch["message_failed"] = True
            patch["message_failed_continue"] = True
        if active_item_id:
            patch["active_item_id"] = str(active_item_id or "")
        if success:
            self.mark_job(job_id, _persist=False, **patch)
            self._record_successful_action(job_id, record_id=record_id, active_item_id=active_item_id)
            with self._jobs_lock:
                self._compact_completed_job_locked(job_id)
        else:
            self.mark_job(job_id, **patch)

    def _record_successful_action(
        self, job_id: str, *, record_id: str = "", active_item_id: str = ""
    ) -> None:
        job = self.get_job(job_id) or {}
        prepared = job.get("prepared") or {}
        if not isinstance(prepared, dict):
            prepared = {}
        action = str(prepared.get("action") or "").strip().lower()
        if action not in {"start", "update", "end"}:
            return
        now = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        source_record_id = str(prepared.get("record_id") or "").strip()
        title = str(prepared.get("title") or "").strip()
        reason = str(prepared.get("reason") or "").strip()
        building = str(prepared.get("building") or "").strip()
        work_type = str(prepared.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        key = self._summary_key(
            source_record_id=source_record_id,
            title=title,
            building=building,
            reason=reason,
            work_type=work_type,
        )
        fallback_key = self._summary_key(
            title=title, building=building, reason=reason, work_type=work_type
        )
        active_item_id = str(active_item_id or prepared.get("active_item_id") or job.get("active_item_id") or "").strip()
        feishu_record_id = str(record_id or job.get("record_id") or "").strip()
        with self._summary_lock:
            payload = self._load_day_summary_locked()
            items = payload.setdefault("items", [])
            item = self._find_summary_item(
                items,
                key=key,
                active_item_id=active_item_id,
                title=title,
                building=building,
                reason=reason,
                work_type=work_type,
            )
            if item is None:
                item = {
                    "key": key or fallback_key or uuid.uuid4().hex,
                    "fallback_key": fallback_key,
                    "work_type": work_type,
                    "notice_type": str(prepared.get("notice_type") or ""),
                    "source_app_token": str(prepared.get("source_app_token") or ""),
                    "source_table_id": str(prepared.get("source_table_id") or ""),
                    "source_record_id": source_record_id,
                    "target_record_id": str(prepared.get("target_record_id") or ""),
                    "active_item_id": active_item_id,
                    "feishu_record_id": feishu_record_id,
                    "title": title,
                    "building": building,
                    "building_code": str(prepared.get("building_code") or ""),
                    "building_codes": list(prepared.get("building_codes") or []),
                    "specialty": str(prepared.get("specialty") or ""),
                    "status": "进行中",
                    "actions": [],
                }
                items.append(item)
            item["fallback_key"] = item.get("fallback_key") or fallback_key
            item["work_type"] = str(item.get("work_type") or work_type)
            for field in (
                "notice_type",
                "source_app_token",
                "source_table_id",
                "building_code",
                "building_codes",
                "target_record_id",
                "level",
                "source_progress",
            ):
                value = prepared.get(field)
                if value not in (None, ""):
                    item[field] = value
            if source_record_id and not item.get("source_record_id"):
                item["source_record_id"] = source_record_id
            if active_item_id:
                item["active_item_id"] = active_item_id
            if feishu_record_id:
                item["feishu_record_id"] = feishu_record_id
            for field in (
                "title",
                "building",
                "building_code",
                "specialty",
                "start_time",
                "end_time",
                "location",
                "content",
                "reason",
                "impact",
                "progress",
                "text",
                "plan_month",
                "maintenance_total",
                "maintenance_no",
                "maintenance_project",
                "maintenance_cycle",
                "level",
                "source_progress",
                "zhihang_involved",
                "zhihang_record_id",
                "zhihang_title",
                "zhihang_progress",
                "zhihang_source_app_token",
                "zhihang_source_table_id",
                "sync_maintenance_target",
                "paired_maintenance_target_record_id",
                "paired_maintenance_original_title",
                "paired_maintenance_actual_start_time",
                "paired_upload_status",
                "paired_upload_warning",
                "repair_device",
                "repair_fault",
                "fault_type",
                "repair_mode",
                "discovery",
                "symptom",
                "solution",
                "fault_time",
                "expected_time",
            ):
                value = prepared.get(field)
                if value not in (None, ""):
                    item[field] = value
            if action == "start":
                item["started_at"] = item.get("started_at") or now
                item["status"] = "进行中"
            elif action == "update":
                item["last_updated_at"] = now
                if item.get("status") != "已结束":
                    item["status"] = "进行中"
            elif action == "end":
                item["ended_at"] = now
                item["status"] = "已结束"
            action_label = {"start": "开始", "update": "更新", "end": "结束"}[action]
            item.setdefault("actions", []).append(
                {
                    "action": action,
                    "label": action_label,
                    "time": now,
                    "job_id": job_id,
                    "record_id": feishu_record_id,
                    "progress": str(prepared.get("progress") or ""),
                    "text": str(prepared.get("text") or ""),
                }
            )
            payload["updated_at"] = now
            self._save_day_summary_locked(payload)
            self._upsert_work_status_item_locked(item, action=action, now=now)
            if action == "end":
                try:
                    self._state_store.delete_qt_active_item(
                        active_item_id=active_item_id,
                        record_id=feishu_record_id,
                    )
                except Exception:
                    pass
        try:
            identity_payload = dict(prepared)
            if active_item_id:
                identity_payload["active_item_id"] = active_item_id
            if source_record_id:
                identity_payload["source_record_id"] = source_record_id
            if feishu_record_id:
                identity_payload["target_record_id"] = feishu_record_id
                identity_payload["feishu_record_id"] = feishu_record_id
                identity_payload["record_id"] = (
                    source_record_id if source_record_id else feishu_record_id
                )
            identity_payload["status"] = "已结束" if action == "end" else "进行中"
            self._state_store.upsert_notice_identity(
                identity_payload,
                origin=str(prepared.get("origin") or "action_success"),
            )
        except Exception:
            pass
        self._schedule_repair_link_task_after_success(
            prepared,
            action=action,
            target_record_id=feishu_record_id,
            job_id=job_id,
        )

    def _schedule_repair_link_task_after_success(
        self,
        prepared: dict[str, Any],
        *,
        action: str,
        target_record_id: str,
        job_id: str = "",
    ) -> None:
        if action != "start":
            return
        if str((prepared or {}).get("work_type") or "").strip() != WORK_TYPE_REPAIR:
            return
        source_record_id = str((prepared or {}).get("record_id") or "").strip()
        target_record_id = str(target_record_id or "").strip()
        if not source_record_id or not target_record_id:
            return
        source_app_token = str(
            (prepared or {}).get("source_app_token") or REPAIR_SOURCE_APP_TOKEN
        ).strip()
        source_table_id = str(
            (prepared or {}).get("source_table_id") or REPAIR_SOURCE_TABLE_ID
        ).strip()
        task_key = f"repair_link:{source_record_id}:{target_record_id}"
        now = time.time()
        self._state_store.upsert_repair_link_task(
            {
                "task_key": task_key,
                "status": "pending",
                "source_app_token": source_app_token,
                "source_table_id": source_table_id,
                "source_record_id": source_record_id,
                "sync_app_token": REPAIR_SOURCE_APP_TOKEN,
                "sync_table_id": REPAIR_SYNC_TABLE_ID,
                "target_app_token": str(config.app_token or "").strip(),
                "target_table_id": str(config.table_id_overhaul or "").strip(),
                "target_record_id": target_record_id,
                "link_field_name": REPAIR_LINK_FIELD_NAME,
                "due_at": now + REPAIR_LINK_DELAY_SECONDS,
                "attempts": 0,
                "max_attempts": REPAIR_LINK_MAX_ATTEMPTS,
                "last_error": "",
                "job_id": str(job_id or ""),
                "created_at": now,
            }
        )

    @staticmethod
    def _repair_link_retry_delay_seconds(attempts: int) -> int:
        attempts = max(1, int(attempts or 1))
        if attempts <= REPAIR_LINK_FAST_RETRY_ATTEMPTS:
            return REPAIR_LINK_RETRY_SECONDS
        return REPAIR_LINK_RETRY_SLOW_SECONDS

    def process_due_repair_link_tasks(self, *, limit: int = 3) -> dict[str, int]:
        tasks = self._state_store.claim_due_repair_link_tasks(
            limit=limit,
            lease_seconds=5 * 60,
        )
        stats = {"checked": 0, "linked": 0, "rescheduled": 0, "failed": 0}
        for task in tasks:
            stats["checked"] += 1
            task_key = str(task.get("task_key") or "").strip()
            attempts = int(task.get("attempts") or 0) + 1
            max_attempts = int(task.get("max_attempts") or REPAIR_LINK_MAX_ATTEMPTS)
            try:
                source_app_token = str(
                    task.get("source_app_token") or REPAIR_SOURCE_APP_TOKEN
                ).strip()
                source_table_id = str(
                    task.get("source_table_id") or REPAIR_SOURCE_TABLE_ID
                ).strip()
                source_record_id = str(task.get("source_record_id") or "").strip()
                target_record_id = str(task.get("target_record_id") or "").strip()
                sync_app_token = str(
                    task.get("sync_app_token") or REPAIR_SOURCE_APP_TOKEN
                ).strip()
                sync_table_id = str(
                    task.get("sync_table_id") or REPAIR_SYNC_TABLE_ID
                ).strip()
                sync_record_id = str(task.get("sync_record_id") or "").strip()
                if not sync_record_id:
                    sync_record_id = self._find_repair_sync_record_id(
                        target_record_id=target_record_id,
                        sync_app_token=sync_app_token,
                        sync_table_id=sync_table_id,
                    )
                if not sync_record_id:
                    raise PortalError(
                        f"同步表尚未出现目标检修记录 {target_record_id}"
                    )
                linked_ids = self._source_repair_link_record_ids(
                    source_app_token=source_app_token,
                    source_table_id=source_table_id,
                    source_record_id=source_record_id,
                )
                if sync_record_id not in linked_ids:
                    self._write_source_repair_link(
                        source_app_token=source_app_token,
                        source_table_id=source_table_id,
                        source_record_id=source_record_id,
                        sync_record_id=sync_record_id,
                    )
                self._state_store.update_repair_link_task(
                    task_key,
                    {
                        "status": "linked",
                        "attempts": attempts,
                        "sync_record_id": sync_record_id,
                        "last_error": "",
                    },
                )
                stats["linked"] += 1
            except Exception as exc:
                next_status = "failed" if attempts >= max_attempts else "pending"
                retry_delay = self._repair_link_retry_delay_seconds(attempts)
                now = time.time()
                self._state_store.update_repair_link_task(
                    task_key,
                    {
                        "status": next_status,
                        "attempts": attempts,
                        "due_at": now + retry_delay,
                        "last_checked_at": now,
                        "next_retry_seconds": retry_delay if next_status == "pending" else 0,
                        "last_error": str(exc),
                    },
                )
                if next_status == "failed":
                    stats["failed"] += 1
                else:
                    stats["rescheduled"] += 1
        return stats

    def get_daily_summary(
        self, *, scope: str = "ALL", ongoing_items: list[dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        scope = self._normalize_scope(scope)
        with self._summary_lock:
            payload = self._load_day_summary_locked()
            raw_items = [item for item in payload.get("items") or [] if isinstance(item, dict)]
        items = [
            copy.deepcopy(item)
            for item in raw_items
            if self._scope_matches_item(scope, item)
        ]
        ongoing_count = len(ongoing_items or [])
        completed_count = sum(1 for item in items if item.get("status") == "已结束")
        started_count = sum(1 for item in items if item.get("started_at"))
        updated_count = sum(
            1
            for item in items
            if any(str(action.get("action") or "") == "update" for action in item.get("actions") or [])
        )
        items.sort(key=lambda item: str(item.get("ended_at") or item.get("last_updated_at") or item.get("started_at") or ""), reverse=True)
        return {
            "date": payload.get("date") or dt.datetime.now().strftime("%Y-%m-%d"),
            "items": items,
            "stats": {
                "started": started_count,
                "updated": updated_count,
                "ended": completed_count,
                "ongoing": ongoing_count,
            },
        }

    @staticmethod
    def _work_type_counts(items: list[dict[str, Any]]) -> dict[str, int]:
        counts = {
            WORK_TYPE_MAINTENANCE: 0,
            WORK_TYPE_CHANGE: 0,
            WORK_TYPE_REPAIR: 0,
            WORK_TYPE_POWER: 0,
            WORK_TYPE_POLLING: 0,
            WORK_TYPE_ADJUST: 0,
        }
        for item in items or []:
            work_type = str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
            if work_type not in counts:
                continue
            counts[work_type] += 1
        return counts

    def get_scope_overview(
        self,
        *,
        ongoing_items: list[dict[str, Any]] | None = None,
        scopes: list[str] | tuple[str, ...] | set[str] | None = None,
        include_prepared: bool = False,
    ) -> dict[str, Any]:
        self.ensure_snapshot_loaded()
        default_month = RECENT_MONTH_FILTER_LABEL
        overview: dict[str, dict[str, Any]] = {}
        prepared_workbenches: dict[str, dict[str, Any]] = {}
        has_scope_filter = scopes is not None
        requested_scopes = {
            self._normalize_scope(scope)
            for scope in (scopes or [])
            if str(scope or "").strip()
        }
        for option in SCOPE_OPTIONS:
            scope = self._normalize_scope(option.get("value"))
            if has_scope_filter and scope not in requested_scopes:
                continue
            records = self._workbench_records(month=default_month, scope=scope)
            merged_ongoing = self._merge_ongoing_items(scope, ongoing_items or [])
            daily_summary = self.get_daily_summary(
                scope=scope, ongoing_items=merged_ongoing
            )
            summary_by_record = self._work_status_by_records(
                records,
                scope=scope,
                daily_items=daily_summary.get("items") or [],
            )
            ongoing_sources = {
                (
                    str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip(),
                    str(item.get("source_record_id") or "").strip(),
                )
                for item in merged_ongoing
                if str(item.get("source_record_id") or "").strip()
            }
            pending_records = []
            for record in records:
                record_id = str(record.get("record_id") or "").strip()
                work_type = self._record_work_type(record)
                summary = summary_by_record.get(record_id) or {}
                if summary.get("started_at"):
                    continue
                if (work_type, record_id) in ongoing_sources:
                    continue
                pending_records.append(record)
            pending_counts = self._work_type_counts(pending_records)
            ongoing_counts = self._work_type_counts(merged_ongoing)
            stats = daily_summary.get("stats") or {}
            overview[scope] = {
                "scope": scope,
                "scope_label": self._scope_label(scope),
                "maintenance_pending": pending_counts[WORK_TYPE_MAINTENANCE],
                "change_pending": pending_counts[WORK_TYPE_CHANGE],
                "repair_pending": pending_counts[WORK_TYPE_REPAIR],
                "maintenance_ongoing": ongoing_counts[WORK_TYPE_MAINTENANCE],
                "change_ongoing": ongoing_counts[WORK_TYPE_CHANGE],
                "repair_ongoing": ongoing_counts[WORK_TYPE_REPAIR],
                "closed_today": int(stats.get("ended") or 0),
            }
            if include_prepared:
                prepared_workbenches[scope] = self.get_bootstrap(
                    scope=scope, ongoing_items=ongoing_items or []
                )
        payload = {
            "scope_options": SCOPE_OPTIONS,
            "scopes": overview,
            "last_loaded_at": self._last_loaded_at,
            "payload_version": self._payload_version(
                scope="overview",
                records=[],
                ongoing_items=ongoing_items or [],
            ),
            "source_snapshot_ready": self._source_snapshot_exists("ALL"),
            "source_cache_ttl_seconds": self._source_cache_ttl_seconds(),
            "warnings": self._current_load_warnings(),
        }
        if include_prepared:
            payload["prepared_workbenches"] = prepared_workbenches
        return payload

    def get_handover_links(self) -> dict[str, Any]:
        options = self._handover_scope_options()
        with self._handover_lock:
            payload = self._load_handover_payload_locked()
        links = payload.get("links") if isinstance(payload, dict) else {}
        if not isinstance(links, dict):
            links = {}
        normalized_links = {
            option["value"]: str(links.get(option["value"]) or "").strip()
            for option in options
        }
        return {
            "scope_options": options,
            "links": normalized_links,
            "updated_at": str(payload.get("updated_at") or "") if isinstance(payload, dict) else "",
        }

    def _load_handover_payload_locked(self) -> dict[str, Any]:
        stored = self._state_store.get_handover_payload()
        if isinstance(stored, dict):
            return stored
        try:
            with self._handover_links_path.open("r", encoding="utf-8") as fp:
                payload = json.load(fp)
        except FileNotFoundError:
            return {}
        except Exception:
            return {}
        if not isinstance(payload, dict):
            return {}
        self._state_store.put_handover_payload(payload)
        return payload

    def _save_handover_payload_locked(self, payload: dict[str, Any]) -> None:
        self._state_store.put_handover_payload(payload)

    @staticmethod
    def _hash_handover_password(password: str, salt: str) -> str:
        raw = f"{salt}:{password}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    @staticmethod
    def _new_handover_password_salt() -> str:
        return secrets.token_hex(16)

    def _set_handover_password_locked(self, payload: dict[str, Any], password: str) -> None:
        salt = self._new_handover_password_salt()
        payload["password_salt"] = salt
        payload["password_hash"] = self._hash_handover_password(password, salt)
        payload["password_updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def verify_handover_settings_password(self, password: str) -> bool:
        password = str(password or "")
        with self._handover_lock:
            payload = self._load_handover_payload_locked()
        salt = str(payload.get("password_salt") or "")
        password_hash = str(payload.get("password_hash") or "")
        if salt and password_hash:
            digest = self._hash_handover_password(password, salt)
            return hmac.compare_digest(digest, password_hash)
        expected = str(getattr(config, "lan_handover_settings_password", "") or "").strip()
        if not expected:
            return False
        return hmac.compare_digest(password, expected)

    def save_handover_links(self, links: dict[str, Any], *, password: str = "") -> dict[str, Any]:
        if not self.verify_handover_settings_password(password):
            raise PortalError("设置密码错误。")
        if not isinstance(links, dict):
            raise PortalError("交接班链接配置格式错误。")
        options = self._handover_scope_options()
        allowed = {option["value"] for option in options}
        normalized_links: dict[str, str] = {}
        for code in allowed:
            url = str(links.get(code) or "").strip()
            if url and not re.match(r"^https?://", url, flags=re.IGNORECASE):
                raise PortalError(f"{self._scope_label(code)} 链接必须以 http:// 或 https:// 开头。")
            normalized_links[code] = url
        with self._handover_lock:
            payload = self._load_handover_payload_locked()
            payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload["links"] = normalized_links
            self._save_handover_payload_locked(payload)
        return self.get_handover_links()

    def _handover_reset_open_ids(self) -> list[str]:
        configured = getattr(config, "lan_handover_reset_open_ids", None)
        recipients: list[str]
        if isinstance(configured, str):
            recipients = re.split(r"[,，;\s]+", configured)
        elif isinstance(configured, (list, tuple, set)):
            recipients = [str(item or "") for item in configured]
        else:
            recipients = [LI_SHILONG_OPEN_ID, MA_JINYU_OPEN_ID]
        recipients = list(dict.fromkeys([str(item or "").strip() for item in recipients]))
        recipients = [item for item in recipients if item]
        if len(recipients) < 2:
            raise PortalError("未配置李世龙和马进宇两个 openid，无法发送改密验证码。")
        return recipients

    def _clear_expired_handover_reset_locked(self) -> None:
        reset = self._handover_password_reset or {}
        expires_at = float(reset.get("expires_at_ts") or 0)
        if expires_at and time.time() >= expires_at:
            self._handover_password_reset = None

    def request_handover_password_reset(self) -> dict[str, Any]:
        recipients = self._handover_reset_open_ids()
        with self._handover_reset_lock:
            self._clear_expired_handover_reset_locked()
            if self._handover_password_reset:
                raise PortalError("已有密码修改流程进行中，请稍后再试。")
            code = f"{secrets.randbelow(1000000):06d}"
            reset_id = secrets.token_urlsafe(18)
            now = dt.datetime.now()
            expires_at = now + dt.timedelta(seconds=HANDOVER_PASSWORD_RESET_TTL_SECONDS)
            self._handover_password_reset = {
                "reset_id": reset_id,
                "code": code,
                "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S"),
                "expires_at_ts": time.time() + HANDOVER_PASSWORD_RESET_TTL_SECONDS,
                "attempts": 0,
            }
        text = (
            "南通基地-运维灯塔工作台交接班链接设置密码重置验证码：\n"
            f"{code}\n"
            f"有效期至：{expires_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
            "如果不是本人操作，请忽略。"
        )
        guard = external_real_write_guard()
        if guard["mock_external"]:
            ok, message = True, "mock external send skipped"
        elif not guard["real_write_allowed"]:
            ok, message = False, str(guard["reason"] or "真实外部写入未确认。")
        else:
            ok, message, _ = send_text_to_open_ids(text, recipients)
        if not ok:
            with self._handover_reset_lock:
                reset = self._handover_password_reset or {}
                if reset.get("reset_id") == reset_id:
                    self._handover_password_reset = None
            raise PortalError(f"验证码发送失败: {message}")
        return {
            "reset_id": reset_id,
            "expires_at": expires_at.strftime("%Y-%m-%d %H:%M:%S"),
            "recipients_count": len(recipients),
        }

    def reset_handover_password_with_code(
        self, *, reset_id: str, code: str, new_password: str
    ) -> dict[str, Any]:
        reset_id = str(reset_id or "").strip()
        code = str(code or "").strip()
        new_password = str(new_password or "")
        if len(new_password) < 6:
            raise PortalError("新密码长度不能少于 6 位。")
        with self._handover_reset_lock:
            self._clear_expired_handover_reset_locked()
            reset = self._handover_password_reset
            if not reset:
                raise PortalError("当前没有有效的密码修改流程，请重新获取验证码。")
            if str(reset.get("reset_id") or "") != reset_id:
                raise PortalError("密码修改会话不匹配，请重新获取验证码。")
            attempts = int(reset.get("attempts") or 0) + 1
            reset["attempts"] = attempts
            if attempts > HANDOVER_PASSWORD_RESET_MAX_ATTEMPTS:
                self._handover_password_reset = None
                raise PortalError("验证码错误次数过多，请重新获取验证码。")
            if not hmac.compare_digest(str(reset.get("code") or ""), code):
                raise PortalError("验证码错误。")
            with self._handover_lock:
                payload = self._load_handover_payload_locked()
                self._set_handover_password_locked(payload, new_password)
                self._save_handover_payload_locked(payload)
            self._handover_password_reset = None
        return {"password_updated": True}

    @staticmethod
    def _summary_item_work_type(item: dict[str, Any]) -> str:
        return str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()

    @staticmethod
    def _summary_item_sort_key(item: dict[str, Any]) -> str:
        return str(
            item.get("ended_at")
            or item.get("last_updated_at")
            or item.get("updated_at")
            or item.get("started_at")
            or ""
        )

    def get_history_summary(
        self,
        *,
        scope: str = "ALL",
        month: str = "",
        work_type: str = "all",
    ) -> dict[str, Any]:
        scope = self._normalize_scope(scope)
        month = str(month or dt.datetime.now().strftime("%Y-%m")).strip()
        if not re.fullmatch(r"\d{4}-\d{2}", month):
            raise PortalError("历史月份格式必须是 YYYY-MM。")
        work_type = str(work_type or "all").strip()
        if work_type not in {
            "all",
            WORK_TYPE_MAINTENANCE,
            WORK_TYPE_CHANGE,
            WORK_TYPE_REPAIR,
        }:
            raise PortalError("历史通告类型必须是 all/maintenance/change/repair。")

        days: list[dict[str, Any]] = []
        with self._summary_lock:
            for payload in self._iter_day_summary_payloads_locked(month=f"{month}-"):
                day = str(payload.get("date") or "")
                items = []
                for raw_item in payload.get("items") or []:
                    if not isinstance(raw_item, dict):
                        continue
                    item = copy.deepcopy(raw_item)
                    item_work_type = self._summary_item_work_type(item)
                    if work_type != "all" and item_work_type != work_type:
                        continue
                    item["work_type"] = item_work_type
                    if not self._scope_matches_item(scope, item):
                        continue
                    items.append(item)
                if not items:
                    continue
                items.sort(key=self._summary_item_sort_key, reverse=True)
                days.append(
                    {
                        "date": day,
                        "items": items,
                        "stats": {
                            "started": sum(1 for item in items if item.get("started_at")),
                            "updated": sum(
                                1
                                for item in items
                                if any(
                                    str(action.get("action") or "") == "update"
                                    for action in item.get("actions") or []
                                    if isinstance(action, dict)
                                )
                            ),
                            "ended": sum(
                                1 for item in items if item.get("status") == "已结束"
                            ),
                            "ongoing": sum(
                                1
                                for item in items
                                if item.get("started_at")
                                and item.get("status") != "已结束"
                            ),
                        },
                    }
                )
        days.sort(key=lambda item: str(item.get("date") or ""), reverse=True)
        return {
            "scope": scope,
            "scope_label": self._scope_label(scope),
            "month": month,
            "work_type": work_type,
            "days": days,
            "last_loaded_at": self._last_loaded_at,
            "warnings": self._current_load_warnings(),
        }

    def _summary_by_record_id(self, summary_items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for item in summary_items:
            record_id = str(item.get("source_record_id") or "").strip()
            if record_id:
                result[record_id] = item
        return result

    def _backfill_work_status_from_daily_summaries_locked(self) -> None:
        if self._work_status_backfilled:
            return
        self._work_status_backfilled = True
        for payload in self._iter_day_summary_payloads_locked():
            day = str(payload.get("date") or "")
            for raw_item in payload.get("items") or []:
                if not isinstance(raw_item, dict):
                    continue
                item = copy.deepcopy(raw_item)
                actions = [action for action in item.get("actions") or [] if isinstance(action, dict)]
                last_action = str(actions[-1].get("action") or "").strip().lower() if actions else ""
                if not item.get("started_at") and last_action == "start":
                    item["started_at"] = f"{day} 00:00"
                if item.get("status") == "已结束" and not item.get("ended_at"):
                    item["ended_at"] = f"{day} 00:00"
                self._upsert_work_status_item_locked(
                    item,
                    action=last_action,
                    now=str(payload.get("updated_at") or item.get("updated_at") or day),
                )

    def _load_work_status_items_locked(self, scope: str = "ALL") -> list[dict[str, Any]]:
        self._backfill_work_status_from_daily_summaries_locked()
        self._migrate_legacy_work_status_locked()
        scope = self._normalize_scope(scope)
        if hasattr(self._state_store, "list_document_meta"):
            documents_meta = self._state_store.list_document_meta(STATE_NS_WORK_STATUS)
        else:
            documents_meta = self._state_store.list_documents(STATE_NS_WORK_STATUS)
        signature = tuple(
            sorted(
                (
                    str(document.get("key") or ""),
                    int(float(document.get("updated_at") or 0) * 1000000),
                )
                for document in documents_meta
            )
        )
        if (
            self._work_status_cache_signature == signature
            and self._work_status_cache_items is not None
        ):
            all_items = self._work_status_cache_items
        else:
            documents = self._state_store.list_documents(STATE_NS_WORK_STATUS)
            all_items: list[dict[str, Any]] = []
            for document in documents:
                payload = document.get("payload")
                if not isinstance(payload, dict):
                    continue
                for item in payload.get("items") or []:
                    if isinstance(item, dict):
                        all_items.append(copy.deepcopy(item))
            self._work_status_cache_signature = signature
            self._work_status_cache_items = all_items
        return [
            copy.deepcopy(item)
            for item in all_items
            if self._scope_matches_item(scope, item)
        ]

    def _work_status_by_records(
        self,
        records: list[dict[str, Any]],
        *,
        scope: str = "ALL",
        daily_items: list[dict[str, Any]] | None = None,
    ) -> dict[str, dict[str, Any]]:
        with self._summary_lock:
            status_items = self._load_work_status_items_locked(scope)
        by_source = {
            (
                str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip(),
                str(item.get("source_record_id") or "").strip(),
            ): item
            for item in status_items
            if str(item.get("source_record_id") or "").strip()
        }
        by_fallback: dict[str, dict[str, Any]] = {}
        for item in status_items:
            for key_name in ("work_fallback_key", "fallback_key"):
                fallback_key = str(item.get(key_name) or "").strip()
                if fallback_key and fallback_key not in by_fallback:
                    by_fallback[fallback_key] = item
        for item in daily_items or []:
            if not isinstance(item, dict):
                continue
            source_record_id = str(item.get("source_record_id") or "").strip()
            item_work_type = str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
            source_key = (item_work_type, source_record_id)
            if source_record_id and source_key not in by_source:
                by_source[source_key] = copy.deepcopy(item)
            fallback_key = str(item.get("fallback_key") or "").strip()
            if fallback_key and fallback_key not in by_fallback:
                by_fallback[fallback_key] = copy.deepcopy(item)
            work_fallback_key = str(item.get("work_fallback_key") or "").strip()
            if work_fallback_key and work_fallback_key not in by_fallback:
                by_fallback[work_fallback_key] = copy.deepcopy(item)
        result: dict[str, dict[str, Any]] = {}
        for record in records:
            record_id = str(record.get("record_id") or "").strip()
            if not record_id:
                continue
            work_type = str(record.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
            item = by_source.get((work_type, record_id))
            if item is None and work_type == WORK_TYPE_MAINTENANCE:
                item = by_source.get(("", record_id))
            if item is None:
                item = by_fallback.get(self._record_fallback_key(record))
            if item is None:
                item = by_fallback.get(self._record_legacy_summary_key(record))
            if item is not None:
                result[record_id] = copy.deepcopy(item)
        return result

    def _work_status_identity_keys(self, item: dict[str, Any]) -> set[str]:
        if not isinstance(item, dict):
            return set()
        item = normalize_notice_identity_payload(item)
        work_type = str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        keys: set[str] = set()
        for kind, field_names in {
            "active": ("active_item_id",),
            "source": ("source_record_id",),
            "target": ("target_record_id", "feishu_record_id", "raw_record_id"),
            "key": ("key", "fallback_key", "work_fallback_key"),
        }.items():
            for field_name in field_names:
                value = str(item.get(field_name) or "").strip()
                if value:
                    keys.add(f"{work_type}:{kind}:{value}")
        return keys

    @staticmethod
    def _is_completed_work_status_item(item: dict[str, Any]) -> bool:
        return str(item.get("status") or "").strip() == "已结束" or bool(
            item.get("ended_at")
        )

    def _target_record_exists_for_status_item(
        self,
        item: dict[str, Any],
        target_cache: dict[tuple[str, str], set[str] | None],
    ) -> bool | None:
        notice_type = str(item.get("notice_type") or NOTICE_TYPE_MAINTENANCE).strip()
        work_type = str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        target_record_id = canonical_target_record_id(item)
        if not target_record_id:
            return None
        table_id = str(config.get_table_id(notice_type) or "").strip()
        app_token = str(config.app_token or "").strip()
        if not app_token or not table_id:
            return None
        cache_key = (notice_type, work_type)
        if cache_key not in target_cache:
            try:
                records = self._target_records_for_notice_type(
                    notice_type, work_type, force_refresh=False
                )
            except Exception as exc:
                if not self._is_transient_feishu_error(exc):
                    warning = f"{notice_type}目标表孤儿状态校验失败: {exc}"
                    if warning not in self._load_warnings:
                        self._load_warnings.append(warning)
                target_cache[cache_key] = None
            else:
                target_cache[cache_key] = {
                    str(record.get("record_id") or "").strip()
                    for record in records
                    if str(record.get("record_id") or "").strip()
                }
        target_ids = target_cache.get(cache_key)
        if target_ids is None:
            return None
        return target_record_id in target_ids

    @staticmethod
    def _is_transient_feishu_error(exc: Exception) -> bool:
        text = str(exc or "")
        return "code=1254002" in text or "msg=Fail" in text

    def _is_orphan_started_item(
        self,
        item: dict[str, Any],
        *,
        ongoing_keys: set[str],
        target_cache: dict[tuple[str, str], set[str] | None],
    ) -> bool:
        if not isinstance(item, dict):
            return False
        if self._is_completed_work_status_item(item):
            return False
        if not item.get("started_at"):
            return False
        if self._work_status_identity_keys(item) & ongoing_keys:
            return False
        target_exists = self._target_record_exists_for_status_item(item, target_cache)
        return target_exists is False

    def reconcile_orphan_started_items(
        self, *, scope: str = "ALL", ongoing_items: list[dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        scope = self._normalize_scope(scope)
        ongoing_keys: set[str] = set()
        for item in ongoing_items or []:
            if isinstance(item, dict) and self._scope_matches_item(scope, item):
                ongoing_keys.update(self._work_status_identity_keys(item))
        if not ongoing_keys:
            ongoing_keys = set()
        target_cache: dict[tuple[str, str], set[str] | None] = {}
        removed_keys: set[str] = set()
        removed_count = 0
        with self._summary_lock:
            self._backfill_work_status_from_daily_summaries_locked()
            self._migrate_legacy_work_status_locked()
            for document in self._state_store.list_documents(STATE_NS_WORK_STATUS):
                payload = document.get("payload")
                if not isinstance(payload, dict):
                    continue
                items = payload.get("items")
                if not isinstance(items, list):
                    continue
                kept: list[dict[str, Any]] = []
                changed = False
                for item in items:
                    if not isinstance(item, dict):
                        kept.append(item)
                        continue
                    if not self._scope_matches_item(scope, item):
                        kept.append(item)
                        continue
                    if self._is_orphan_started_item(
                        item, ongoing_keys=ongoing_keys, target_cache=target_cache
                    ):
                        removed_keys.update(self._work_status_identity_keys(item))
                        removed_count += 1
                        changed = True
                        continue
                    kept.append(item)
                if changed:
                    payload["items"] = kept
                    payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
                    self._state_store.put_document(
                        STATE_NS_WORK_STATUS, str(document.get("key") or ""), payload
                    )
            if removed_keys:
                for payload in self._iter_day_summary_payloads_locked():
                    if not isinstance(payload, dict):
                        continue
                    items = payload.get("items")
                    if not isinstance(items, list):
                        continue
                    kept = []
                    changed = False
                    for item in items:
                        if (
                            isinstance(item, dict)
                            and not self._is_completed_work_status_item(item)
                            and self._scope_matches_item(scope, item)
                            and (self._work_status_identity_keys(item) & removed_keys)
                        ):
                            changed = True
                            continue
                        kept.append(item)
                    if changed:
                        payload["items"] = kept
                        payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
                        self._save_day_summary_locked(payload)
                self._work_status_cache_signature = None
                self._work_status_cache_items = None
        return {"removed": removed_count}

    def _get_record_memory(self, record: dict[str, Any]) -> dict[str, str]:
        work_type = str(record.get("work_type") or WORK_TYPE_MAINTENANCE)
        fields = record.get("display_fields") or {}
        if work_type == WORK_TYPE_CHANGE:
            building = self._building_label_from_codes(
                self._change_record_building_codes(record)
            )
            memory_name = self._change_title(record)
        elif work_type == WORK_TYPE_REPAIR:
            building = self._building_label_from_codes(
                self._repair_record_building_codes(record)
            )
            memory_name = self._repair_title(record)
        else:
            building = str(fields.get("楼栋") or "").strip()
            memory_name = str(fields.get("维护总项") or "").strip()
            maintenance_cycle = str(
                fields.get("维护周期") or record.get("maintenance_cycle") or ""
            ).strip()
        if not building or not memory_name:
            return {}
        key = self._memory_item_key(
            work_type,
            memory_name,
            maintenance_cycle if work_type == WORK_TYPE_MAINTENANCE else "",
        )
        with self._memory_lock:
            payload = self._load_building_memory_locked(building)
            items = payload.get("items") or {}
            item = items.get(key) or {}
            if not item and work_type == WORK_TYPE_MAINTENANCE:
                item = items.get(self._normalize_memory_key(memory_name)) or {}
        if not isinstance(item, dict):
            return {}
        return {
            "location": str(item.get("location") or ""),
            "content": str(item.get("content") or ""),
            "reason": str(item.get("reason") or ""),
            "impact": str(item.get("impact") or ""),
            "progress": str(item.get("progress") or ""),
            "maintenance_cycle": str(item.get("maintenance_cycle") or ""),
            "specialty": str(item.get("specialty") or ""),
            "level": str(item.get("level") or ""),
            "repair_device": str(item.get("repair_device") or ""),
            "repair_fault": str(item.get("repair_fault") or ""),
            "fault_type": str(item.get("fault_type") or ""),
            "repair_mode": str(item.get("repair_mode") or ""),
            "discovery": str(item.get("discovery") or ""),
            "symptom": str(item.get("symptom") or ""),
            "solution": str(item.get("solution") or ""),
            "zhihang_involved": str(item.get("zhihang_involved") or ""),
            "zhihang_record_id": str(item.get("zhihang_record_id") or ""),
            "zhihang_title": str(item.get("zhihang_title") or ""),
            "zhihang_progress": str(item.get("zhihang_progress") or ""),
            "updated_at": str(item.get("updated_at") or ""),
        }

    def _memory_item_key(
        self, work_type: str, item_name: str, maintenance_cycle: str = ""
    ) -> str:
        normalized = self._normalize_memory_key(item_name)
        work_type = str(work_type or WORK_TYPE_MAINTENANCE).strip() or WORK_TYPE_MAINTENANCE
        if work_type == WORK_TYPE_MAINTENANCE:
            cycle = self._normalize_memory_key(maintenance_cycle)
            if cycle:
                return f"{normalized}|cycle:{cycle}"
            return normalized
        return f"{work_type}:{normalized}"

    def _remember_draft_fields(
        self,
        *,
        building: str,
        maintenance_total: str,
        location: str,
        content: str,
        reason: str,
        impact: str,
        work_type: str = WORK_TYPE_MAINTENANCE,
        item_name: str = "",
        maintenance_cycle: str = "",
        extra_fields: dict[str, Any] | None = None,
    ) -> None:
        work_type = str(work_type or WORK_TYPE_MAINTENANCE).strip() or WORK_TYPE_MAINTENANCE
        building = str(building or "").strip()
        memory_name = str(item_name or maintenance_total or "").strip()
        maintenance_total = str(maintenance_total or memory_name).strip()
        maintenance_cycle = str(maintenance_cycle or "").strip()
        if not building or not memory_name:
            return
        key = self._memory_item_key(work_type, memory_name, maintenance_cycle)
        now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        remembered = {
            "work_type": work_type,
            "memory_name": memory_name,
            "maintenance_total": maintenance_total,
            "maintenance_cycle": maintenance_cycle,
            "location": str(location or ""),
            "content": str(content or ""),
            "reason": str(reason or ""),
            "impact": str(impact or ""),
            "updated_at": now,
        }
        for extra_key, extra_value in (extra_fields or {}).items():
            if extra_key in remembered:
                continue
            remembered[str(extra_key)] = str(extra_value or "")
        with self._memory_lock:
            payload = self._load_building_memory_locked(building)
            payload["building"] = building
            payload["updated_at"] = now
            items = payload.setdefault("items", {})
            items[key] = remembered
            self._save_building_memory_locked(building, payload)

    @staticmethod
    def _split_notice_text_blocks(text: str) -> list[str]:
        text = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
        if not text:
            return []
        pattern = re.compile(
            r"(?=【(?:维保通告|设备变更|变更通告|设备检修|上电通告|上下电通告|设备轮巡|设备调整)】\s*状态[:：])"
        )
        parts = [part.strip() for part in pattern.split(text) if part.strip()]
        if len(parts) > 1:
            return parts
        separator_parts = [
            part.strip()
            for part in re.split(r"\n\s*(?:-{4,}|={4,}|#{4,})\s*\n", text)
            if part.strip()
        ]
        return separator_parts or [text]

    @staticmethod
    def _parse_notice_sections(text: str) -> dict[str, str]:
        sections: dict[str, str] = {}
        matches = list(re.finditer(r"【([^】]+)】", text or ""))
        for index, match in enumerate(matches):
            label = str(match.group(1) or "").strip()
            if not label:
                continue
            start = match.end()
            end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            value = str((text or "")[start:end]).strip()
            value = re.sub(r"^[：:]\s*", "", value).strip()
            sections[label] = value
        return sections

    @staticmethod
    def _notice_section_value(
        sections: dict[str, str], names: list[str], fallback: str = ""
    ) -> str:
        for name in names:
            value = str(sections.get(name) or "").strip()
            if value:
                return value
        return str(fallback or "").strip()

    @classmethod
    def _notice_work_type_from_text(
        cls, text: str, sections: dict[str, str]
    ) -> str:
        raw = str(text or "")
        if "【设备检修】" in raw or cls._notice_section_value(
            sections, ["维修设备", "维修故障", "故障现象", "解决方案"]
        ):
            return WORK_TYPE_REPAIR
        if "【上电通告】" in raw or "【上下电通告】" in raw or cls._notice_section_value(
            sections, ["柜号", "数量"]
        ):
            return WORK_TYPE_POWER
        if "【设备轮巡】" in raw or cls._notice_section_value(
            sections, ["设备"]
        ):
            return WORK_TYPE_POLLING
        if "【设备调整】" in raw:
            return WORK_TYPE_ADJUST
        if "【设备变更】" in raw or "【变更通告】" in raw or cls._notice_section_value(
            sections, ["变更等级", "变更楼栋"]
        ):
            return WORK_TYPE_CHANGE
        return WORK_TYPE_MAINTENANCE

    @classmethod
    def _building_codes_from_notice_text(cls, *values: Any) -> list[str]:
        codes: list[str] = []
        text = "\n".join(str(value or "") for value in values if str(value or "").strip())
        upper = text.upper()
        if re.search(r"110\s*(?:站|楼|机房|数据中心|DC)?", upper):
            codes.append("110")
        for code in ("A", "B", "C", "D", "E", "H"):
            patterns = (
                rf"(?<![A-Z0-9]){code}\s*(?:楼|栋|座|区|机房|数据中心|DC)",
                rf"(?:楼栋|楼宇|数据中心)\s*{code}(?![A-Z0-9])",
                rf"(?<![A-Z0-9]){code}[-－]\d",
            )
            if any(re.search(pattern, upper) for pattern in patterns):
                codes.append(code)
        return [code for code in BUILDING_SCOPE_CODES if code in dict.fromkeys(codes)]

    @classmethod
    def _strip_notice_title_suffix(cls, title: str, work_type: str) -> str:
        text = re.sub(r"\s+", " ", str(title or "").strip())
        if not text:
            return ""
        if work_type == WORK_TYPE_MAINTENANCE:
            text = re.sub(r"^EA118机房", "", text, flags=re.IGNORECASE).strip()
            for code in BUILDING_SCOPE_CODES:
                label = cls._building_label_from_code(code)
                text = re.sub(rf"^{re.escape(label)}", "", text).strip()
            text = re.sub(r"(?:维保|维护)?通告$", "", text).strip()
            return text or str(title or "").strip()
        if work_type == WORK_TYPE_CHANGE:
            stripped = re.sub(r"(?:变更)?通告$", "", text).strip()
            return stripped or text
        return text

    def _find_zhihang_record_by_title(self, title: str) -> dict[str, Any] | None:
        target = self._match_text(title)
        if not target:
            return None
        candidates: list[dict[str, Any]] = []
        try:
            candidates.extend(self._source_snapshot_zhihang_records("ALL") or [])
        except Exception:
            pass
        candidates.extend(self._zhihang_change_records)
        seen: set[str] = set()
        for record in candidates:
            if not isinstance(record, dict):
                continue
            record_id = str(record.get("record_id") or "").strip()
            if record_id and record_id in seen:
                continue
            if record_id:
                seen.add(record_id)
            title_text = str(record.get("title") or "").strip() or self._zhihang_change_title(record)
            if self._match_text(title_text) == target:
                return record
        return None

    def _historical_notice_memory_item(
        self, block: str, *, scope: str
    ) -> tuple[dict[str, Any] | None, str]:
        sections = self._parse_notice_sections(block)
        work_type = self._notice_work_type_from_text(block, sections)
        title = self._notice_section_value(sections, ["标题", "名称", "通告名称"])
        location = self._notice_section_value(sections, ["地点", "位置"])
        building_text = self._notice_section_value(
            sections,
            ["楼栋", "变更楼栋", "所属楼栋", "所属数据中心/楼栋-使用"],
        )
        codes = self._building_codes_from_notice_text(building_text, title, location, block)
        if not codes:
            return None, "无法识别楼栋"
        if not self._scope_matches_buildings(scope, codes):
            return None, f"不属于当前入口 {self._scope_label(scope)}"
        building = self._building_label_from_codes(codes)
        specialty = self._notice_section_value(sections, ["专业", "专业类别", "所属专业"])
        level = self._notice_section_value(sections, ["等级", "变更等级", "紧急程度"])
        content = self._notice_section_value(sections, ["内容"], title)
        reason = self._notice_section_value(sections, ["原因", "故障原因"])
        impact = self._notice_section_value(sections, ["影响", "影响范围"])
        maintenance_cycle = self._notice_section_value(sections, ["维保周期", "维护周期"])
        if work_type == WORK_TYPE_MAINTENANCE:
            memory_name = self._notice_section_value(
                sections,
                ["维护总项", "维保总项", "项目", "维护项目"],
                self._strip_notice_title_suffix(title, work_type),
            )
            if not memory_name:
                return None, "维保通告缺少标题或维护总项"
            return {
                "work_type": work_type,
                "building": building,
                "memory_name": memory_name,
                "maintenance_cycle": maintenance_cycle,
                "location": location,
                "content": content,
                "reason": reason,
                "impact": impact,
                "extra_fields": {"specialty": specialty},
            }, ""
        if work_type == WORK_TYPE_CHANGE:
            memory_name = self._notice_section_value(
                sections,
                ["变更简述", "变更标题"],
                self._strip_notice_title_suffix(title, work_type) or title,
            )
            if not memory_name:
                return None, "变更通告缺少标题"
            zhihang_title = self._notice_section_value(
                sections, ["智航变更", "智航侧变更", "互联变更", "互联侧变更"]
            )
            zhihang_record_id = self._notice_section_value(
                sections, ["智航记录ID", "互联记录ID"]
            )
            zhihang_progress = ""
            if zhihang_title and not zhihang_record_id:
                zhihang_record = self._find_zhihang_record_by_title(zhihang_title)
                if zhihang_record:
                    zhihang_record_id = str(zhihang_record.get("record_id") or "").strip()
                    zhihang_title = str(zhihang_record.get("title") or "").strip() or self._zhihang_change_title(zhihang_record)
                    zhihang_progress = str(zhihang_record.get("progress") or "").strip() or self._zhihang_change_progress(zhihang_record)
            return {
                "work_type": work_type,
                "building": building,
                "memory_name": memory_name,
                "location": location,
                "content": content or memory_name,
                "reason": reason,
                "impact": impact,
                "extra_fields": {
                    "specialty": specialty,
                    "level": level,
                    "zhihang_involved": "1" if zhihang_record_id else "",
                    "zhihang_record_id": zhihang_record_id,
                    "zhihang_title": zhihang_title,
                    "zhihang_progress": zhihang_progress,
                },
            }, ""
        if work_type in {WORK_TYPE_POWER, WORK_TYPE_POLLING, WORK_TYPE_ADJUST}:
            memory_name = self._strip_notice_title_suffix(title, work_type) or title
            if not memory_name:
                return None, f"{self._history_work_type_label(work_type)}通告缺少标题"
            return {
                "work_type": work_type,
                "building": building,
                "memory_name": memory_name,
                "location": location,
                "content": content or memory_name,
                "reason": reason,
                "impact": impact,
                "extra_fields": {
                    "specialty": specialty,
                    "device": self._notice_section_value(sections, ["设备"]),
                    "cabinet": self._notice_section_value(sections, ["柜号"]),
                    "quantity": self._notice_section_value(sections, ["数量"]),
                    "progress": self._notice_section_value(sections, ["进度"]),
                },
            }, ""
        memory_name = self._notice_section_value(
            sections,
            ["检修通告名称", "维修名称"],
            title,
        )
        if not memory_name:
            return None, "检修通告缺少标题"
        return {
            "work_type": work_type,
            "building": building,
            "memory_name": memory_name,
            "location": location,
            "content": content or memory_name,
            "reason": reason,
            "impact": impact,
            "extra_fields": {
                "specialty": specialty,
                "level": level,
                "repair_device": self._notice_section_value(sections, ["维修设备"]),
                "repair_fault": self._notice_section_value(sections, ["维修故障"]),
                "fault_type": self._notice_section_value(sections, ["故障类型"]),
                "repair_mode": self._notice_section_value(sections, ["维修方式"]),
                "discovery": self._notice_section_value(sections, ["故障发现方式"]),
                "symptom": self._notice_section_value(sections, ["故障现象"]),
                "solution": self._notice_section_value(sections, ["解决方案"]),
                "spare_parts": self._notice_section_value(
                    sections, ["备件更换情况", "备件使用情况"]
                ),
            },
        }, ""

    def import_historical_notice_memory(
        self,
        *,
        text: str,
        scope: str = "ALL",
        allowed_scopes: list[str] | None = None,
        is_admin: bool = False,
        imported_by: str = "",
    ) -> dict[str, Any]:
        scope = self._normalize_scope(scope)
        blocks = self._split_notice_text_blocks(text)
        if not blocks:
            raise PortalError("请粘贴至少一条历史通告。")
        if len(blocks) > 200:
            raise PortalError("一次最多导入 200 条历史通告，请分批导入。")
        allowed = {self._normalize_scope(item) for item in (allowed_scopes or [])}
        imported: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for index, block in enumerate(blocks, start=1):
            item, reason = self._historical_notice_memory_item(block, scope=scope)
            if not item:
                skipped.append({"index": index, "reason": reason or "无法解析"})
                continue
            item_codes = self._building_codes_from_value(item.get("building"))
            item_scope = "CAMPUS" if len(item_codes) >= 2 else (item_codes[0] if item_codes else "")
            if not is_admin and "ALL" not in allowed and item_scope not in allowed:
                skipped.append(
                    {
                        "index": index,
                        "title": item.get("memory_name") or "",
                        "reason": f"无权导入 {self._scope_label(item_scope)} 历史通告",
                    }
                )
                continue
            extra_fields = item.get("extra_fields") if isinstance(item.get("extra_fields"), dict) else {}
            self._remember_draft_fields(
                work_type=str(item.get("work_type") or WORK_TYPE_MAINTENANCE),
                building=str(item.get("building") or ""),
                maintenance_total=str(item.get("memory_name") or ""),
                item_name=str(item.get("memory_name") or ""),
                maintenance_cycle=str(item.get("maintenance_cycle") or ""),
                location=str(item.get("location") or ""),
                content=str(item.get("content") or ""),
                reason=str(item.get("reason") or ""),
                impact=str(item.get("impact") or ""),
                extra_fields={
                    **extra_fields,
                    "imported_by": imported_by,
                    "imported_from": "historical_notice",
                },
            )
            imported.append(
                {
                    "index": index,
                    "work_type": item.get("work_type") or "",
                    "building": item.get("building") or "",
                    "title": item.get("memory_name") or "",
                    "maintenance_cycle": item.get("maintenance_cycle") or "",
                }
            )
        return {
            "imported_count": len(imported),
            "skipped_count": len(skipped),
            "imported": imported,
            "skipped": skipped,
        }

    @staticmethod
    def _history_work_type_label(work_type: str) -> str:
        return {
            WORK_TYPE_MAINTENANCE: "维保",
            WORK_TYPE_CHANGE: "变更",
            WORK_TYPE_REPAIR: "检修",
            WORK_TYPE_POWER: "上电",
            WORK_TYPE_POLLING: "轮巡",
            WORK_TYPE_ADJUST: "调整",
        }.get(str(work_type or ""), "维保")

    @classmethod
    def _coerce_history_work_types(cls, work_types: Any) -> list[str]:
        if isinstance(work_types, str):
            raw_values = [item.strip() for item in work_types.split(",")]
        elif isinstance(work_types, (list, tuple, set)):
            raw_values = [str(item or "").strip() for item in work_types]
        else:
            raw_values = []
        aliases = {
            "maintenance": WORK_TYPE_MAINTENANCE,
            "维保": WORK_TYPE_MAINTENANCE,
            "维保通告": WORK_TYPE_MAINTENANCE,
            "change": WORK_TYPE_CHANGE,
            "变更": WORK_TYPE_CHANGE,
            "设备变更": WORK_TYPE_CHANGE,
            "repair": WORK_TYPE_REPAIR,
            "检修": WORK_TYPE_REPAIR,
            "设备检修": WORK_TYPE_REPAIR,
            "power": WORK_TYPE_POWER,
            "上电": WORK_TYPE_POWER,
            "上电通告": WORK_TYPE_POWER,
            "上下电通告": WORK_TYPE_POWER,
            "polling": WORK_TYPE_POLLING,
            "轮巡": WORK_TYPE_POLLING,
            "设备轮巡": WORK_TYPE_POLLING,
            "adjust": WORK_TYPE_ADJUST,
            "调整": WORK_TYPE_ADJUST,
            "设备调整": WORK_TYPE_ADJUST,
        }
        result: list[str] = []
        for value in raw_values:
            normalized = aliases.get(value.lower()) or aliases.get(value)
            if normalized and normalized not in result:
                result.append(normalized)
        return result or [WORK_TYPE_MAINTENANCE, WORK_TYPE_CHANGE, WORK_TYPE_REPAIR]

    @staticmethod
    def _history_month_floor(months: int) -> dt.datetime:
        count = max(1, min(int(months or 3), 12))
        current = dt.datetime(dt.datetime.now().year, dt.datetime.now().month, 1)
        month = current.month - (count - 1)
        year = current.year
        while month <= 0:
            month += 12
            year -= 1
        return dt.datetime(year, month, 1)

    def _history_target_config(self, work_type: str) -> dict[str, str]:
        work_type = str(work_type or WORK_TYPE_MAINTENANCE).strip()
        if work_type == WORK_TYPE_CHANGE:
            return {
                "work_type": WORK_TYPE_CHANGE,
                "notice_type": NOTICE_TYPE_CHANGE,
                "app_token": str(config.app_token or "").strip(),
                "table_id": str(config.table_id_biangeng or "").strip(),
            }
        if work_type == WORK_TYPE_REPAIR:
            return {
                "work_type": WORK_TYPE_REPAIR,
                "notice_type": NOTICE_TYPE_REPAIR,
                "app_token": str(config.app_token or "").strip(),
                "table_id": str(config.table_id_overhaul or "").strip(),
            }
        return {
            "work_type": WORK_TYPE_MAINTENANCE,
            "notice_type": NOTICE_TYPE_MAINTENANCE,
            "app_token": str(config.app_token or "").strip(),
            "table_id": str(config.table_id_weibao or "").strip(),
        }

    def _history_datetime_from_values(self, *values: Any) -> dt.datetime | None:
        for value in values:
            formatted = self._format_source_datetime(value)
            if not formatted:
                continue
            match = re.search(
                r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})"
                r"(?:[日\sT]*(\d{1,2})[:点时](\d{1,2})?)?",
                formatted,
            )
            if not match:
                continue
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            hour = int(match.group(4) or 0)
            minute = int(match.group(5) or 0)
            try:
                return dt.datetime(year, month, day, hour, minute)
            except ValueError:
                continue
        return None

    def _target_history_datetime(
        self, record: dict[str, Any], work_type: str
    ) -> tuple[dt.datetime | None, str]:
        fields = record.get("display_fields") or {}
        raw_values: list[Any] = []
        if work_type == WORK_TYPE_CHANGE:
            raw_values.extend(
                [
                    fields.get("变更开始时间"),
                    fields.get("过程更新时间"),
                    fields.get("变更结束时间"),
                    fields.get("计划开始时间"),
                    fields.get("计划开始"),
                ]
            )
        elif work_type == WORK_TYPE_REPAIR:
            raw_values.extend(
                [
                    fields.get("实际开始时间"),
                    fields.get("发生故障时间"),
                    fields.get("期望完成时间"),
                    fields.get("实际结束时间"),
                ]
            )
        else:
            raw_values.extend(
                [
                    fields.get("实际开始时间"),
                    fields.get("计划开始时间"),
                    fields.get("实际结束时间"),
                    fields.get("计划结束时间"),
                ]
            )
        raw_values.extend([record.get("created_time"), record.get("last_modified_time")])
        parsed = self._history_datetime_from_values(*raw_values)
        return parsed, parsed.strftime("%Y-%m-%d %H:%M") if parsed else ""

    def _canonical_history_notice_title(self, value: Any, work_type: str) -> str:
        text = str(value or "").strip()
        text = re.sub(r"【[^】]+】\s*状态[：:]\s*\S+", "", text)
        if work_type == WORK_TYPE_MAINTENANCE:
            text = re.sub(r"(?:通告)+$", "", text).strip()
        else:
            text = self._strip_notice_title_suffix(text, work_type)
        text = re.sub(r"^EA118\s*(?:机房)?", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"^(?:南通基地|南通)?", "", text).strip()
        for code in BUILDING_SCOPE_CODES:
            label = self._building_label_from_code(code)
            text = re.sub(rf"^{re.escape(label)}", "", text).strip()
        text = re.sub(r"(?:通告)+$", "", text).strip()
        text = re.sub(r"[\s,，;；:：。.【】（）()《》<>\"'“”‘’\-－_/\\]+", "", text)
        return text.lower()

    def _history_target_memory_fields(
        self, record: dict[str, Any], work_type: str
    ) -> dict[str, str]:
        fields = record.get("display_fields") or {}
        if work_type == WORK_TYPE_REPAIR:
            return {
                "location": str(fields.get("位置") or "").strip(),
                "content": str(fields.get("名称（标题）") or fields.get("名称") or "").strip(),
                "reason": str(fields.get("故障原因") or "").strip(),
                "impact": str(fields.get("影响范围") or fields.get("影响") or "").strip(),
                "progress": str(fields.get("进度（完成情况）") or fields.get("进度") or "").strip(),
                "specialty": self._clean_source_text(fields.get("专业")),
                "level": str(fields.get("紧急程度") or "").strip(),
                "repair_device": str(fields.get("维修设备") or "").strip(),
                "repair_fault": str(fields.get("维修故障") or "").strip(),
                "fault_type": str(fields.get("故障类型") or "").strip(),
                "repair_mode": str(fields.get("维修方式") or "").strip(),
                "discovery": str(fields.get("故障发现方式（来源）") or fields.get("故障发现方式") or "").strip(),
                "symptom": str(fields.get("故障现象") or "").strip(),
                "solution": str(fields.get("解决方案") or "").strip(),
            }
        return {
            "location": str(fields.get("位置") or "").strip(),
            "content": str(fields.get("内容") or "").strip(),
            "reason": str(fields.get("原因") or "").strip(),
            "impact": str(fields.get("影响") or fields.get("影响范围") or "").strip(),
            "progress": str(fields.get("进度") or "").strip(),
            "specialty": self._clean_source_text(fields.get("专业")),
            "level": str(fields.get("阿里-变更等级") or fields.get("智航-变更等级") or "").strip(),
        }

    def _history_candidate_from_target_record(
        self, record: dict[str, Any], *, work_type: str, since: dt.datetime
    ) -> tuple[dict[str, Any] | None, str]:
        fields = record.get("display_fields") or {}
        if work_type == WORK_TYPE_CHANGE:
            title = str(fields.get("名称") or record.get("record_id") or "").strip()
            building_codes = self._building_codes_from_value(fields.get("楼栋") or title)
            maintenance_cycle = ""
        elif work_type == WORK_TYPE_REPAIR:
            title = str(fields.get("名称（标题）") or fields.get("名称") or record.get("record_id") or "").strip()
            building_codes = self._building_codes_from_value(fields.get("楼栋") or title)
            maintenance_cycle = ""
        else:
            title = str(fields.get("名称") or record.get("record_id") or "").strip()
            building_codes = self._building_codes_from_value(fields.get("楼栋") or title)
            maintenance_cycle = str(fields.get("维保周期") or "").strip()
        if not title:
            return None, "历史记录缺少标题"
        if not building_codes:
            return None, f"历史记录无法识别楼栋: {title}"
        business_dt, business_time = self._target_history_datetime(record, work_type)
        if business_dt is None or business_dt < since:
            return None, "不在近3个月范围内"
        if work_type == WORK_TYPE_MAINTENANCE:
            memory_name = re.sub(r"(?:通告)+$", "", title).strip() or title
        else:
            memory_name = self._strip_notice_title_suffix(title, work_type) or title
        canonical = self._canonical_history_notice_title(memory_name, work_type)
        if not canonical:
            return None, "标题标准化后为空"
        memory_fields = self._history_target_memory_fields(record, work_type)
        if work_type == WORK_TYPE_CHANGE:
            memory_fields["zhihang_title"] = str(fields.get("智航变更") or "").strip()
        return {
            "id": f"{work_type}:{record.get('record_id') or uuid.uuid4().hex}",
            "record_id": str(record.get("record_id") or ""),
            "work_type": work_type,
            "work_type_label": self._history_work_type_label(work_type),
            "notice_type": str(record.get("notice_type") or ""),
            "title": title,
            "memory_name": memory_name,
            "canonical_title": canonical,
            "building": self._building_label_from_codes(building_codes),
            "building_codes": building_codes,
            "maintenance_cycle": maintenance_cycle,
            "business_time": business_time,
            "fields": memory_fields,
            "display_fields": fields,
        }, ""

    def _history_source_current_fields(self, record: dict[str, Any]) -> dict[str, str]:
        fields = record.get("display_fields") or {}
        work_type = self._record_work_type(record)
        if work_type == WORK_TYPE_REPAIR:
            return {
                "location": self._repair_location_text(fields),
                "content": self._repair_title(record),
                "reason": self._repair_first_field(fields, "故障原因", "故障维修原因"),
                "impact": self._repair_first_field(fields, "影响范围", "影响"),
                "progress": self._repair_first_field(fields, "完成情况", "进度", "维修进展"),
                "specialty": self._repair_specialty(record),
                "level": self._repair_level(fields),
                "repair_device": self._repair_device_text(fields),
                "repair_fault": self._repair_first_field(fields, "维修故障", "故障维修原因"),
                "fault_type": self._repair_first_field(fields, "故障类型") or "设备故障",
                "repair_mode": self._repair_first_field(fields, "维修方式", "维修方", "供应商名称"),
                "discovery": self._repair_first_field(fields, "对应来源"),
                "symptom": self._repair_first_field(fields, "故障发生现象描述", "故障现象"),
                "solution": self._repair_first_field(fields, "解决方案", "维修方案", "后续整改措施"),
            }
        if work_type == WORK_TYPE_CHANGE:
            return {
                "location": str(fields.get("位置") or fields.get("变更楼栋") or "").strip(),
                "content": self._change_title(record),
                "reason": str(fields.get("变更原因") or fields.get("原因") or "").strip(),
                "impact": str(fields.get("影响") or fields.get("影响范围") or "").strip(),
                "progress": str(fields.get("进度") or fields.get("变更进度") or "").strip(),
                "specialty": self._change_specialty(record),
                "level": str(fields.get("变更等级（阿里）") or fields.get("变更等级") or "").strip(),
                "zhihang_title": "",
                "zhihang_record_id": "",
                "zhihang_progress": "",
            }
        maintenance_total = str(fields.get("维护总项") or "").strip()
        return {
            "location": str(fields.get("位置") or fields.get("楼栋") or "").strip(),
            "content": str(fields.get("内容") or fields.get("维护内容") or maintenance_total).strip(),
            "reason": str(fields.get("原因") or fields.get("维护原因") or "").strip(),
            "impact": str(fields.get("影响") or fields.get("影响范围") or "").strip(),
            "progress": str(fields.get("进度") or fields.get("维护进度") or "").strip(),
            "specialty": self._clean_source_text(fields.get("专业类别"))
            or self._clean_source_text(fields.get("专业")),
            "level": "",
            "maintenance_cycle": str(fields.get("维护周期") or "").strip(),
        }

    def _history_source_item_from_record(self, record: dict[str, Any]) -> dict[str, Any] | None:
        work_type = self._record_work_type(record)
        fields = record.get("display_fields") or {}
        if work_type == WORK_TYPE_CHANGE:
            title = self._change_title(record)
            memory_name = title
            building_codes = self._change_record_building_codes(record)
            maintenance_cycle = ""
            specialty = self._change_specialty(record)
        elif work_type == WORK_TYPE_REPAIR:
            title = self._repair_title(record)
            memory_name = title
            building_codes = self._repair_record_building_codes(record)
            maintenance_cycle = ""
            specialty = self._repair_specialty(record)
        else:
            maintenance_total = str(fields.get("维护总项") or "").strip()
            building = str(fields.get("楼栋") or "").strip()
            maintenance_cycle = str(fields.get("维护周期") or "").strip()
            title = f"{building}{maintenance_total}{('-' + maintenance_cycle) if maintenance_cycle else ''}".strip()
            memory_name = maintenance_total
            building_codes = self._building_codes_from_value(building)
            specialty = str(fields.get("专业类别") or "").strip()
        if not memory_name:
            return None
        canonical = self._canonical_history_notice_title(memory_name, work_type)
        source_id = f"{work_type}:{record.get('record_id') or uuid.uuid4().hex}"
        if work_type == WORK_TYPE_CHANGE:
            source_status = self._change_progress_value(record)
        elif work_type == WORK_TYPE_REPAIR:
            source_status = self._repair_source_status(record)
        else:
            source_status = self._maintenance_status_value(record)
        return {
            "id": source_id,
            "source_record_id": str(record.get("record_id") or ""),
            "work_type": work_type,
            "work_type_label": self._history_work_type_label(work_type),
            "title": title or memory_name,
            "memory_name": memory_name,
            "canonical_title": canonical,
            "building": self._building_label_from_codes(building_codes),
            "building_codes": building_codes,
            "maintenance_cycle": maintenance_cycle,
            "specialty": specialty,
            "source_status": source_status,
            "memory": self._get_record_memory(record),
            "current_fields": self._history_source_current_fields(record),
            "display_fields": fields,
        }

    def _history_source_items(self, work_types: list[str]) -> list[dict[str, Any]]:
        records = self._source_snapshot_records("ALL") or []
        current_month = self._current_month_label()
        items: list[dict[str, Any]] = []
        for record in records:
            work_type = self._record_work_type(record)
            if work_type not in work_types:
                continue
            if not self._source_record_matches_month_window(record, current_month):
                continue
            item = self._history_source_item_from_record(record)
            if item:
                items.append(item)
        return items

    def _history_current_month_source_items(
        self, work_types: list[str]
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Load current-month source records for the admin memory import page.

        This intentionally does not reuse the workbench active snapshot because
        that snapshot is filtered for day-to-day issuing. The memory import page
        needs all current-month maintenance/change source records, including
        records that are already ended.
        """
        current_month = self._current_month_label()
        records: list[dict[str, Any]] = []
        warnings: list[str] = []
        with self._refresh_lock:
            if WORK_TYPE_MAINTENANCE in work_types:
                try:
                    self._load_fields()
                    app_token = str(self.app_token or DEFAULT_APP_TOKEN or "").strip()
                    table_id = str(self.table_id or DEFAULT_TABLE_ID or "").strip()
                    loaded = self._load_table_records(
                        app_token=app_token,
                        table_id=table_id,
                        meta_by_name=self._field_meta_by_name,
                        work_type=WORK_TYPE_MAINTENANCE,
                        notice_type=NOTICE_TYPE_MAINTENANCE,
                    )
                    records.extend(
                        item
                        for item in loaded
                        if self._maintenance_record_matches_month_window(item, current_month)
                    )
                except Exception as exc:
                    warnings.append(f"维保当前月源表全量读取失败: {exc}")
            if WORK_TYPE_CHANGE in work_types:
                try:
                    self._load_change_fields()
                    loaded = self._load_table_records(
                        app_token=CHANGE_SOURCE_APP_TOKEN,
                        table_id=CHANGE_SOURCE_TABLE_ID,
                        meta_by_name=self._change_field_meta_by_name,
                        work_type=WORK_TYPE_CHANGE,
                        notice_type=NOTICE_TYPE_CHANGE,
                    )
                    records.extend(
                        item
                        for item in loaded
                        if self._source_record_matches_month_window(item, current_month)
                    )
                except Exception as exc:
                    warnings.append(f"变更当前月源表全量读取失败: {exc}")
        if WORK_TYPE_REPAIR in work_types:
            records.extend(
                record
                for record in (self._source_snapshot_records("ALL") or [])
                if self._record_work_type(record) == WORK_TYPE_REPAIR
                and self._source_record_matches_month_window(record, current_month)
            )
        items: list[dict[str, Any]] = []
        seen: set[str] = set()
        for record in records:
            item = self._history_source_item_from_record(record)
            if not item:
                continue
            key = str(item.get("id") or "")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            items.append(item)
        items.sort(
            key=lambda item: (
                str(item.get("work_type") or ""),
                str(item.get("building") or ""),
                str(item.get("title") or ""),
            )
        )
        return items, warnings

    def _scan_target_history_candidates(
        self, *, work_type: str, months: int
    ) -> tuple[list[dict[str, Any]], list[str]]:
        target = self._history_target_config(work_type)
        app_token = target["app_token"]
        table_id = target["table_id"]
        if not app_token or not table_id:
            return [], [f"{self._history_work_type_label(work_type)}目标表未配置，已跳过。"]
        metas, meta_by_name = self._load_table_fields(app_token=app_token, table_id=table_id)
        records = self._load_table_records(
            app_token=app_token,
            table_id=table_id,
            meta_by_name=meta_by_name,
            work_type=work_type,
            notice_type=target["notice_type"],
        )
        since = self._history_month_floor(months)
        candidates: list[dict[str, Any]] = []
        skipped: dict[str, int] = {}
        for record in records:
            candidate, reason = self._history_candidate_from_target_record(
                record, work_type=work_type, since=since
            )
            if not candidate:
                skipped[reason or "无法解析"] = skipped.get(reason or "无法解析", 0) + 1
                continue
            candidates.append(candidate)
        deduped: dict[tuple, dict[str, Any]] = {}
        for candidate in candidates:
            key = (
                candidate.get("work_type"),
                tuple(candidate.get("building_codes") or []),
                candidate.get("canonical_title"),
                candidate.get("maintenance_cycle") if work_type == WORK_TYPE_MAINTENANCE else "",
            )
            current = deduped.get(key)
            if current is None or str(candidate.get("business_time") or "") >= str(current.get("business_time") or ""):
                deduped[key] = candidate
        warnings = [
            f"{self._history_work_type_label(work_type)}历史记录跳过 {count} 条：{reason}"
            for reason, count in sorted(skipped.items())
            if reason != "不在近3个月范围内"
        ]
        return list(deduped.values()), warnings

    @staticmethod
    def _history_building_score(source: dict[str, Any], candidate: dict[str, Any]) -> int:
        source_codes = set(source.get("building_codes") or [])
        candidate_codes = set(candidate.get("building_codes") or [])
        if not source_codes or not candidate_codes:
            return 5
        if source_codes == candidate_codes:
            return 20
        if source_codes & candidate_codes:
            return 10
        return -100

    def _recommend_history_memory_matches(
        self, source_items: list[dict[str, Any]], candidates: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        by_type: dict[str, list[dict[str, Any]]] = {}
        for candidate in candidates:
            by_type.setdefault(str(candidate.get("work_type") or ""), []).append(candidate)
        for source in source_items:
            best: dict[str, Any] | None = None
            best_score = -999
            best_reason = ""
            for candidate in by_type.get(str(source.get("work_type") or ""), []):
                score = self._history_building_score(source, candidate)
                if score < 0:
                    continue
                source_title = str(source.get("canonical_title") or "")
                candidate_title = str(candidate.get("canonical_title") or "")
                if source_title and source_title == candidate_title:
                    score += 70
                    reason = "标题与楼栋匹配"
                elif source_title and candidate_title and (
                    source_title in candidate_title or candidate_title in source_title
                ):
                    score += 45
                    reason = "标题相似"
                else:
                    reason = "楼栋相同"
                if source.get("work_type") == WORK_TYPE_MAINTENANCE:
                    source_cycle = self._normalize_memory_key(str(source.get("maintenance_cycle") or ""))
                    candidate_cycle = self._normalize_memory_key(str(candidate.get("maintenance_cycle") or ""))
                    if source_cycle and candidate_cycle and source_cycle == candidate_cycle:
                        score += 10
                        reason += "，周期一致"
                    elif source_cycle and candidate_cycle and source_cycle != candidate_cycle:
                        score -= 25
                        reason += "，周期不同"
                if score > best_score:
                    best = candidate
                    best_score = score
                    best_reason = reason
            if not best:
                continue
            selected = best_score >= 80
            matches.append(
                {
                    "source_id": source.get("id") or "",
                    "candidate_id": best.get("id") or "",
                    "score": best_score,
                    "confidence": "high" if best_score >= 90 else "medium" if best_score >= 70 else "low",
                    "selected": selected,
                    "reason": best_reason,
                    "fields": copy.deepcopy(best.get("fields") or {}),
                }
            )
        return matches

    def scan_historical_notice_memory_candidates(
        self, *, work_types: Any = None, months: int = 3
    ) -> dict[str, Any]:
        work_type_list = self._coerce_history_work_types(work_types)
        months = max(1, min(int(months or 3), 12))
        candidates: list[dict[str, Any]] = []
        source_items, warnings = self._history_current_month_source_items(work_type_list)
        for work_type in work_type_list:
            try:
                items, item_warnings = self._scan_target_history_candidates(
                    work_type=work_type, months=months
                )
                candidates.extend(items)
                warnings.extend(item_warnings)
            except Exception as exc:
                warnings.append(f"{self._history_work_type_label(work_type)}历史扫描失败: {exc}")
        candidates.sort(
            key=lambda item: (
                str(item.get("work_type") or ""),
                str(item.get("building") or ""),
                str(item.get("business_time") or ""),
            ),
            reverse=True,
        )
        matches = self._recommend_history_memory_matches(source_items, candidates)
        return {
            "months": months,
            "work_types": work_type_list,
            "source_snapshot_ready": bool(self._source_snapshot_records("ALL") is not None),
            "source_items_full_current_month": True,
            "source_items": source_items,
            "candidates": candidates,
            "matches": matches,
            "counts": {
                "source": len(source_items),
                "candidates": len(candidates),
                "recommended": sum(1 for item in matches if item.get("selected")),
            },
            "warnings": warnings,
        }

    def save_historical_notice_memory_matches(
        self, *, matches: list[dict[str, Any]], imported_by: str = ""
    ) -> dict[str, Any]:
        if not isinstance(matches, list):
            raise PortalError("保存参数格式错误。")
        saved: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        overwritten_count = 0
        now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for index, match in enumerate(matches, start=1):
            if not isinstance(match, dict) or not bool(match.get("selected", True)):
                skipped.append({"index": index, "reason": "未勾选"})
                continue
            source = match.get("source_item") if isinstance(match.get("source_item"), dict) else {}
            fields = match.get("fields") if isinstance(match.get("fields"), dict) else {}
            work_type = str(source.get("work_type") or match.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
            building = str(source.get("building") or match.get("building") or "").strip()
            memory_name = str(source.get("memory_name") or source.get("title") or match.get("memory_name") or "").strip()
            maintenance_cycle = str(
                source.get("maintenance_cycle")
                or fields.get("maintenance_cycle")
                or match.get("maintenance_cycle")
                or ""
            ).strip()
            if not building or not memory_name:
                skipped.append({"index": index, "reason": "缺少楼栋或记忆名称"})
                continue
            key = self._memory_item_key(work_type, memory_name, maintenance_cycle)
            with self._memory_lock:
                payload = self._load_building_memory_locked(building)
                existed = key in (payload.get("items") or {})
            if existed:
                overwritten_count += 1
            extra_fields = {
                "specialty": str(fields.get("specialty") or ""),
                "level": str(fields.get("level") or ""),
                "progress": str(fields.get("progress") or ""),
                "repair_device": str(fields.get("repair_device") or ""),
                "repair_fault": str(fields.get("repair_fault") or ""),
                "fault_type": str(fields.get("fault_type") or ""),
                "repair_mode": str(fields.get("repair_mode") or ""),
                "discovery": str(fields.get("discovery") or ""),
                "symptom": str(fields.get("symptom") or ""),
                "solution": str(fields.get("solution") or ""),
                "zhihang_involved": str(fields.get("zhihang_involved") or ""),
                "zhihang_record_id": str(fields.get("zhihang_record_id") or ""),
                "zhihang_title": str(fields.get("zhihang_title") or ""),
                "zhihang_progress": str(fields.get("zhihang_progress") or ""),
                "imported_by": imported_by,
                "imported_from": "target_history_scan",
                "history_candidate_id": str(match.get("candidate_id") or ""),
                "history_imported_at": now,
            }
            self._remember_draft_fields(
                work_type=work_type,
                building=building,
                maintenance_total=memory_name,
                item_name=memory_name,
                maintenance_cycle=maintenance_cycle,
                location=str(fields.get("location") or ""),
                content=str(fields.get("content") or ""),
                reason=str(fields.get("reason") or ""),
                impact=str(fields.get("impact") or ""),
                extra_fields=extra_fields,
            )
            saved.append(
                {
                    "index": index,
                    "work_type": work_type,
                    "building": building,
                    "title": memory_name,
                    "maintenance_cycle": maintenance_cycle,
                    "overwritten": existed,
                }
            )
        if saved:
            self._touch_state_cache_version()
        return {
            "saved_count": len(saved),
            "skipped_count": len(skipped),
            "overwritten_count": overwritten_count,
            "saved": saved,
            "skipped": skipped,
        }

    def _serialize_record(
        self, record: dict[str, Any], summary_by_record: dict[str, dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        summary_by_record = summary_by_record or {}
        work_type = str(record.get("work_type") or WORK_TYPE_MAINTENANCE)
        source_work_type = self._record_source_work_type(record)
        if source_work_type == WORK_TYPE_MAINTENANCE:
            source_progress = self._maintenance_status_value(record)
        elif source_work_type == WORK_TYPE_CHANGE:
            source_progress = self._change_progress_value(record)
        elif source_work_type == WORK_TYPE_REPAIR:
            source_progress = self._repair_source_status(record)
        else:
            source_progress = self._maintenance_status_value(record)
        title = (
            self._change_title(record)
            if work_type == WORK_TYPE_CHANGE
            else self._repair_title(record)
            if work_type == WORK_TYPE_REPAIR
            else ""
        )
        return {
            "record_id": record["record_id"],
            "source_record_id": record["record_id"],
            "source_work_type": source_work_type,
            "converted_from_work_type": str(record.get("converted_from_work_type") or ""),
            "converted_to_work_type": str(record.get("converted_to_work_type") or ""),
            "work_type_override_key": str(record.get("work_type_override_key") or ""),
            "work_type_override_title_key": str(record.get("work_type_override_title_key") or ""),
            "work_type": work_type,
            "notice_type": str(record.get("notice_type") or NOTICE_TYPE_MAINTENANCE),
            "source_app_token": str(record.get("source_app_token") or self.app_token),
            "source_table_id": str(record.get("source_table_id") or self.table_id),
            "title": title,
            "display_fields": record["display_fields"],
            "memory": self._get_record_memory(record),
            "work_summary": summary_by_record.get(record["record_id"]) or {},
            "source_progress": source_progress,
            "source_status": source_progress,
            "building_codes": (
                self._change_record_building_codes(record)
                if record.get("work_type") == WORK_TYPE_CHANGE
                else self._repair_record_building_codes(record)
                if record.get("work_type") == WORK_TYPE_REPAIR
                else self._building_codes_from_value(
                    (record.get("display_fields") or {}).get("楼栋")
                )
            ),
        }

    def _serialize_zhihang_change_record(self, record: dict[str, Any]) -> dict[str, Any]:
        fields = record.get("display_fields") or {}
        codes = self._zhihang_change_building_codes(record)
        scope_all = not codes
        return {
            "record_id": str(record.get("record_id") or ""),
            "source_record_id": str(record.get("record_id") or ""),
            "source_app_token": ZHIHANG_CHANGE_APP_TOKEN,
            "source_table_id": ZHIHANG_CHANGE_TABLE_ID,
            "title": self._zhihang_change_title(record),
            "progress": self._zhihang_change_progress(record),
            "building": str(fields.get("楼栋") or ""),
            "building_codes": codes,
            "scope_all": scope_all,
            "scope_label": "全部楼栋" if scope_all else self._building_label_from_codes(codes),
            "level": str(fields.get("变更等级") or ""),
            "change_type": str(fields.get("变更类型") or fields.get("变更类别") or ""),
            "plan_start": str(fields.get("计划开始") or fields.get("计划开始时间") or ""),
            "plan_end": str(fields.get("计划结束") or fields.get("计划结束时间") or ""),
            "display_fields": fields,
        }

    @staticmethod
    def _record_work_type(record: dict[str, Any]) -> str:
        return str(record.get("work_type") or WORK_TYPE_MAINTENANCE).strip()

    @staticmethod
    def _record_source_work_type(record: dict[str, Any]) -> str:
        return str(
            record.get("source_work_type")
            or record.get("original_work_type")
            or record.get("work_type")
            or WORK_TYPE_MAINTENANCE
        ).strip()

    def _maintenance_title(self, record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        building = str(fields.get("楼栋") or "").strip()
        maintenance_total = str(fields.get("维护总项") or "").strip()
        title = f"EA118机房{building}{maintenance_total}" if maintenance_total else ""
        return self._normalize_110_station_notice_title(
            title or str(record.get("title") or record.get("record_id") or "").strip(),
            building=building,
            building_codes=self._building_codes_from_value(building),
        )

    def _work_type_override_title_key(
        self, title: Any, source_work_type: str = WORK_TYPE_MAINTENANCE
    ) -> str:
        canonical = self._canonical_history_notice_title(title, source_work_type)
        if canonical:
            return canonical
        return re.sub(r"[\s,，;；:：。.【】（）()《》<>\"'“”‘’\-－_/\\]+", "", str(title or "")).lower()

    def _work_type_override_key_for_record(self, record: dict[str, Any]) -> str:
        source_work_type = self._record_source_work_type(record)
        if source_work_type == WORK_TYPE_MAINTENANCE:
            return self._work_type_override_title_key(
                self._maintenance_title(record), source_work_type
            )
        return ""

    def _copy_record_as_change_override(
        self, record: dict[str, Any], override: dict[str, Any]
    ) -> dict[str, Any]:
        copied = copy.deepcopy(record)
        fields = dict(copied.get("display_fields") or {})
        title = self._maintenance_title(record)
        fields.setdefault("变更简述", title)
        fields.setdefault("变更楼栋", fields.get("楼栋") or "")
        fields.setdefault("变更进度", self._maintenance_status_value(record) or DEFAULT_MAINTENANCE_STATUS)
        fields.setdefault("专业", fields.get("专业类别") or "")
        copied["display_fields"] = fields
        copied["source_work_type"] = WORK_TYPE_MAINTENANCE
        copied["original_work_type"] = WORK_TYPE_MAINTENANCE
        copied["work_type"] = WORK_TYPE_CHANGE
        copied["notice_type"] = NOTICE_TYPE_CHANGE
        copied["title"] = title
        copied["converted_from_work_type"] = WORK_TYPE_MAINTENANCE
        copied["converted_to_work_type"] = WORK_TYPE_CHANGE
        copied["work_type_override_key"] = str(override.get("override_key") or "")
        copied["work_type_override_title_key"] = str(override.get("normalized_title") or "")
        return copied

    def _apply_work_type_overrides(
        self, records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        overrides = {
            str(item.get("normalized_title") or ""): item
            for item in self._state_store.list_work_type_overrides(
                source_work_type=WORK_TYPE_MAINTENANCE
            )
            if str(item.get("target_work_type") or "") == WORK_TYPE_CHANGE
        }
        if not overrides:
            return records
        converted: list[dict[str, Any]] = []
        for record in records:
            if self._record_source_work_type(record) != WORK_TYPE_MAINTENANCE:
                converted.append(record)
                continue
            title_key = self._work_type_override_key_for_record(record)
            override = overrides.get(title_key)
            if override:
                converted.append(self._copy_record_as_change_override(record, override))
            else:
                converted.append(record)
        return converted

    def _change_title(self, record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        return str(
            fields.get("变更简述")
            or record.get("title")
            or record.get("record_id")
            or ""
        ).strip()

    def _change_time_range(self, record: dict[str, Any]) -> tuple[str, str, str]:
        fields = record.get("display_fields") or {}
        start_time = str(
            fields.get("变更开始日期（阿里）")
            or fields.get("计划开始日期（阿里）")
            or fields.get("计划开始")
            or fields.get("计划开始时间")
            or fields.get("计划延迟开始日期")
            or ""
        ).strip()
        end_time = str(
            fields.get("变更结束日期（阿里）")
            or fields.get("计划结束日期（阿里）")
            or fields.get("计划结束")
            or fields.get("计划结束时间")
            or fields.get("计划延迟结束日期")
            or ""
        ).strip()
        time_text = f"{start_time}至{end_time}" if start_time or end_time else ""
        return start_time, end_time, time_text

    @staticmethod
    def _match_text(value: Any) -> str:
        return re.sub(r"\s+", "", str(value or "")).strip()

    @staticmethod
    def _truthy_flag(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value or "").strip().lower()
        return text in {"1", "true", "yes", "y", "on", "是", "涉及"}

    @staticmethod
    def _normalize_110_station_notice_title(
        title: Any,
        *,
        building: Any = "",
        building_codes: list[str] | tuple[str, ...] | None = None,
    ) -> str:
        value = str(title or "").strip()
        if not value:
            return value
        target_prefix = "EA118-110KV阿里中天变"
        if value.startswith(target_prefix):
            return value
        codes = {
            str(code or "").strip().upper()
            for code in (building_codes or [])
            if str(code or "").strip()
        }
        building_text = str(building or "").strip()
        looks_like_110 = bool(
            "110" in codes
            or "110站" in building_text
            or re.match(r"^EA118\s*(?:机房)?\s*[-－]?\s*110\s*(?:站|KV)?", value, re.I)
        )
        if not looks_like_110:
            return value
        normalized = re.sub(
            r"^EA118\s*(?:机房)?\s*[-－]?\s*110\s*(?:站|KV)?\s*",
            target_prefix,
            value,
            count=1,
            flags=re.I,
        ).strip()
        return normalized or value

    def _date_keys_from_values(self, *values: Any) -> set[str]:
        keys: set[str] = set()
        for value in values:
            formatted = self._format_source_datetime(value)
            for year, month, day in re.findall(
                r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})", formatted
            ):
                keys.add(f"{int(year):04d}-{int(month):02d}-{int(day):02d}")
        return keys

    def _year_month_keys_from_values(self, *values: Any) -> set[str]:
        keys: set[str] = set()
        for value in values:
            formatted = self._format_source_datetime(value)
            if not formatted:
                continue
            for year, month in re.findall(
                r"(\d{4})[-/年](\d{1,2})(?=[-/月\s日]|$)", formatted
            ):
                keys.add(f"{int(year):04d}-{int(month):02d}")
        return keys

    def _maintenance_record_matches_month_window(
        self, record: dict[str, Any], month: str = ""
    ) -> bool:
        label = self._specific_recent_month_label(month)
        if label is None:
            return False
        fields = record.get("display_fields") or {}
        record_month = self._normalize_month_label(fields.get("计划维护月份"))
        if not record_month:
            return False
        if label:
            return record_month == label
        return record_month in set(self._recent_month_labels())

    def _source_record_month_keys(self, record: dict[str, Any]) -> set[str]:
        fields = record.get("display_fields") or {}
        work_type = self._record_work_type(record)
        if work_type == WORK_TYPE_CHANGE:
            start_time, end_time, _ = self._change_time_range(record)
            return self._year_month_keys_from_values(
                start_time,
                end_time,
                fields.get("变更开始日期（阿里）至变更结束日期（阿里）"),
                fields.get("计划开始日期（阿里）"),
                fields.get("计划结束日期（阿里）"),
                fields.get("计划开始"),
                fields.get("计划开始时间"),
                fields.get("计划结束"),
                fields.get("计划结束时间"),
                fields.get("计划延迟开始日期"),
                fields.get("计划延迟结束日期"),
            )
        if work_type == WORK_TYPE_REPAIR:
            return self._year_month_keys_from_values(
                fields.get("维修开始时间"),
                fields.get("故障发生时间"),
                fields.get("发现故障时间"),
                fields.get("期望完成时间"),
                fields.get("维修结束时间"),
                fields.get("维修结束时间（2026）"),
            )
        return set()

    def _source_record_matches_month_window(
        self, record: dict[str, Any], month: str = ""
    ) -> bool:
        work_type = self._record_work_type(record)
        if work_type == WORK_TYPE_MAINTENANCE:
            return self._maintenance_record_matches_month_window(record, month)
        specific_key = self._specific_recent_month_key(month)
        if specific_key is None:
            return False
        record_keys = self._source_record_month_keys(record)
        if not record_keys:
            return False
        if specific_key:
            return specific_key in record_keys
        return bool(record_keys & self._recent_month_keys())

    @staticmethod
    def _target_status_is_finished(status: Any) -> bool:
        return "结束" in str(status or "").strip()

    def _ongoing_hidden_keys(self, item: dict[str, Any]) -> list[str]:
        if not isinstance(item, dict):
            return []
        work_type = str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        active_item_id = str(item.get("active_item_id") or "").strip()
        if active_item_id:
            return [f"{work_type}:active:{active_item_id}"]
        values = {
            "source": str(item.get("source_record_id") or "").strip(),
            "record": str(
                item.get("target_record_id") or item.get("record_id") or ""
            ).strip(),
        }
        return [
            f"{work_type}:{kind}:{value}"
            for kind, value in values.items()
            if value
        ]

    def _load_hidden_ongoing_locked(self) -> dict[str, Any]:
        stored = self._state_store.get_document(STATE_NS_HIDDEN_ONGOING, "global")
        if isinstance(stored, dict):
            hidden = stored.get("hidden")
            if not isinstance(hidden, dict):
                stored["hidden"] = {}
            return stored
        try:
            with self._hidden_ongoing_path.open("r", encoding="utf-8") as fp:
                payload = json.load(fp)
        except FileNotFoundError:
            payload = {}
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        hidden = payload.get("hidden")
        if not isinstance(hidden, dict):
            hidden = {}
        payload["hidden"] = hidden
        self._state_store.put_document(STATE_NS_HIDDEN_ONGOING, "global", payload)
        return payload

    def _save_hidden_ongoing_locked(self, payload: dict[str, Any]) -> None:
        payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._state_store.put_document(STATE_NS_HIDDEN_ONGOING, "global", payload)
        self._touch_state_cache_version()

    def _is_ongoing_hidden(self, item: dict[str, Any]) -> bool:
        keys = self._ongoing_hidden_keys(item)
        if not keys:
            return False
        with self._hidden_ongoing_lock:
            payload = self._load_hidden_ongoing_locked()
            hidden = payload.get("hidden") or {}
            return any(key in hidden for key in keys)

    def hide_ongoing_item(
        self, item: dict[str, Any], *, scope: str = "ALL", deleted_by: str = ""
    ) -> dict[str, Any]:
        if not isinstance(item, dict):
            raise PortalError("删除参数格式错误。")
        scope = self._normalize_scope(scope)
        item = copy.deepcopy(item)
        item.setdefault("work_type", WORK_TYPE_MAINTENANCE)
        item.setdefault("notice_type", NOTICE_TYPE_MAINTENANCE)
        keys = self.validate_ongoing_delete_item(item, scope=scope)
        now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._hidden_ongoing_lock:
            payload = self._load_hidden_ongoing_locked()
            hidden = payload.setdefault("hidden", {})
            for key in keys:
                hidden[key] = {
                    "deleted_at": now,
                    "deleted_by": str(deleted_by or ""),
                    "scope": scope,
                    "work_type": str(item.get("work_type") or WORK_TYPE_MAINTENANCE),
                    "notice_type": str(item.get("notice_type") or ""),
                    "title": str(item.get("title") or ""),
                    "source_record_id": str(item.get("source_record_id") or ""),
                    "active_item_id": str(item.get("active_item_id") or ""),
                    "record_id": str(
                        item.get("target_record_id") or item.get("record_id") or ""
                    ),
                }
            self._save_hidden_ongoing_locked(payload)
        return {"deleted": True, "keys": keys, "deleted_at": now}

    def discard_deleted_ongoing_state(
        self, item: dict[str, Any], *, scope: str = "ALL"
    ) -> dict[str, Any]:
        if not isinstance(item, dict):
            return {"work_status_removed": 0, "daily_summary_removed": 0}
        scope = self._normalize_scope(scope)
        item = copy.deepcopy(item)
        item.setdefault("work_type", WORK_TYPE_MAINTENANCE)
        item.setdefault("notice_type", NOTICE_TYPE_MAINTENANCE)
        deleted_keys = self._work_status_identity_keys(item)
        if not deleted_keys:
            return {"work_status_removed": 0, "daily_summary_removed": 0}
        work_status_removed = 0
        daily_summary_removed = 0
        with self._summary_lock:
            self._migrate_legacy_work_status_locked()
            for document in self._state_store.list_documents(STATE_NS_WORK_STATUS):
                payload = document.get("payload")
                if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
                    continue
                kept: list[dict[str, Any]] = []
                changed = False
                for status_item in payload.get("items") or []:
                    if (
                        isinstance(status_item, dict)
                        and self._scope_matches_item(scope, status_item)
                        and (self._work_status_identity_keys(status_item) & deleted_keys)
                    ):
                        work_status_removed += 1
                        changed = True
                        continue
                    kept.append(status_item)
                if changed:
                    payload["items"] = kept
                    payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
                    self._state_store.put_document(
                        STATE_NS_WORK_STATUS, str(document.get("key") or ""), payload
                    )

            summary_payload = self._load_day_summary_locked()
            summary_items = summary_payload.get("items")
            if isinstance(summary_items, list):
                kept_summary: list[dict[str, Any]] = []
                changed = False
                for summary_item in summary_items:
                    if (
                        isinstance(summary_item, dict)
                        and self._scope_matches_item(scope, summary_item)
                        and (self._work_status_identity_keys(summary_item) & deleted_keys)
                    ):
                        daily_summary_removed += 1
                        changed = True
                        continue
                    kept_summary.append(summary_item)
                if changed:
                    summary_payload["items"] = kept_summary
                    summary_payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
                    self._save_day_summary_locked(summary_payload)
            self._work_status_cache_signature = None
            self._work_status_cache_items = None
        return {
            "work_status_removed": work_status_removed,
            "daily_summary_removed": daily_summary_removed,
        }

    @staticmethod
    def _undo_action_label(action_type: str) -> str:
        return {
            "update": "更新",
            "end": "结束",
            "delete": "删除",
        }.get(str(action_type or "").strip().lower(), "操作")

    def _undo_identity_from_context(
        self, context: dict[str, Any], *, action_type: str = ""
    ) -> dict[str, Any]:
        context = normalize_notice_identity_payload(
            context if isinstance(context, dict) else {},
            action=action_type,
        )
        work_type = str(context.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        notice_type = str(context.get("notice_type") or NOTICE_TYPE_MAINTENANCE).strip()
        active_item_id = str(context.get("active_item_id") or "").strip()
        source_record_id = str(context.get("source_record_id") or "").strip()
        target_record_id = canonical_target_record_id(context)
        if not source_record_id:
            source_record_id = str(context.get("source_id") or "").strip()
        title = str(context.get("title") or context.get("name") or "").strip()
        reason = str(context.get("reason") or "").strip()
        building = str(context.get("building") or "").strip()
        building_codes = context.get("building_codes")
        if not isinstance(building_codes, list):
            building_codes = self._building_codes_from_value(building)
        identity_item = {
            "work_type": work_type,
            "notice_type": notice_type,
            "active_item_id": active_item_id,
            "source_record_id": source_record_id,
            "target_record_id": target_record_id,
            "feishu_record_id": target_record_id,
            "title": title,
            "reason": reason,
            "building": building,
            "building_code": str(context.get("building_code") or "").strip(),
            "building_codes": building_codes,
        }
        identity_keys = self._work_status_identity_keys(identity_item)
        if target_record_id:
            identity_keys.add(f"{work_type}:record:{target_record_id}")
        fallback_key = self._summary_key(
            title=title,
            building=building,
            reason=reason,
            work_type=work_type,
        )
        if fallback_key:
            identity_item["fallback_key"] = fallback_key
            identity_keys.add(f"{work_type}:key:{fallback_key}")
        if active_item_id:
            identity_key = f"{work_type}:active:{active_item_id}"
        elif target_record_id:
            identity_key = f"{work_type}:target:{target_record_id}"
        elif source_record_id:
            identity_key = f"{work_type}:source:{source_record_id}"
        elif fallback_key:
            identity_key = f"{work_type}:key:{fallback_key}"
        else:
            identity_key = f"{work_type}:title:{hashlib.sha1(title.encode('utf-8', errors='ignore')).hexdigest()}"
        return {
            "identity_key": identity_key,
            "identity_item": identity_item,
            "identity_keys": sorted(identity_keys),
            "work_type": work_type,
            "notice_type": notice_type,
            "active_item_id": active_item_id,
            "source_record_id": source_record_id,
            "target_record_id": target_record_id,
            "title": title,
            "reason": reason,
            "building": building,
            "building_codes": building_codes,
        }

    @staticmethod
    def _items_identity_intersects(keys: set[str], item_keys: set[str]) -> bool:
        return bool(keys and item_keys and keys.intersection(item_keys))

    def _find_qt_active_snapshot(self, identity: dict[str, Any]) -> dict[str, Any] | None:
        active_item_id = str(identity.get("active_item_id") or "").strip()
        target_record_id = str(identity.get("target_record_id") or "").strip()
        source_record_id = str(identity.get("source_record_id") or "").strip()
        title = str(identity.get("title") or "").strip()
        for row in self._state_store.list_qt_active_items(include_deleted=True):
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            if active_item_id and (
                str(row.get("active_item_id") or "") == active_item_id
                or str(payload.get("active_item_id") or "") == active_item_id
            ):
                return copy.deepcopy(row)
            if target_record_id and (
                str(row.get("record_id") or "") == target_record_id
                or str(payload.get("record_id") or "") == target_record_id
                or str(payload.get("target_record_id") or "") == target_record_id
            ):
                return copy.deepcopy(row)
            if source_record_id and str(payload.get("source_record_id") or "") == source_record_id:
                return copy.deepcopy(row)
            if title and title == str(payload.get("title") or "").strip():
                return copy.deepcopy(row)
        return None

    @staticmethod
    def _first_text_value(*values: Any) -> str:
        for value in values:
            text = str(value or "").strip()
            if text:
                return text
        return ""

    def _source_record_building_codes_for_delete(
        self, work_type: str, source_record_id: str, scope: str
    ) -> list[str]:
        source_record_id = str(source_record_id or "").strip()
        if not source_record_id:
            return []
        record = self._source_record_in_scope_snapshot(
            record_id=source_record_id,
            work_type=work_type,
            scope=scope,
        ) or self._source_record_in_scope_snapshot(
            record_id=source_record_id,
            work_type=work_type,
            scope="ALL",
        )
        if not isinstance(record, dict):
            return []
        if work_type == WORK_TYPE_CHANGE:
            return self._change_record_building_codes(record)
        if work_type == WORK_TYPE_REPAIR:
            return self._repair_record_building_codes(record)
        fields = record.get("display_fields") if isinstance(record.get("display_fields"), dict) else {}
        return self._building_codes_from_value(
            record.get("building_codes")
            or record.get("building")
            or fields.get("楼栋")
            or fields.get("所属楼栋")
        )

    def _enrich_ongoing_identity_item(
        self, item: dict[str, Any], *, scope: str = "ALL"
    ) -> dict[str, Any]:
        enriched = normalize_notice_identity_payload(
            copy.deepcopy(item) if isinstance(item, dict) else {}
        )
        enriched.setdefault("work_type", WORK_TYPE_MAINTENANCE)
        enriched.setdefault("notice_type", NOTICE_TYPE_MAINTENANCE)
        work_type = str(enriched.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        identity = self._undo_identity_from_context(enriched, action_type="delete")
        qt_snapshot = self._find_qt_active_snapshot(identity)
        qt_payload = normalize_notice_identity_payload(
            qt_snapshot.get("payload")
            if isinstance(qt_snapshot, dict) and isinstance(qt_snapshot.get("payload"), dict)
            else {}
        )

        def set_text_if_missing(field: str, *values: Any) -> None:
            if str(enriched.get(field) or "").strip():
                return
            value = self._first_text_value(*values)
            if value:
                enriched[field] = value

        set_text_if_missing("active_item_id", qt_payload.get("active_item_id"), (qt_snapshot or {}).get("active_item_id"))
        set_text_if_missing("notice_type", qt_payload.get("notice_type"), (qt_snapshot or {}).get("notice_type"))
        set_text_if_missing("source_record_id", qt_payload.get("source_record_id"))
        set_text_if_missing(
            "target_record_id",
            canonical_target_record_id(qt_payload),
            (qt_snapshot or {}).get("record_id"),
        )
        qt_sections = self._parse_notice_sections(str(qt_payload.get("text") or ""))
        set_text_if_missing(
            "title",
            qt_payload.get("title"),
            self._notice_section_value(qt_sections, ["名称", "标题", "事件描述"]),
            qt_payload.get("content"),
        )
        set_text_if_missing(
            "reason",
            qt_payload.get("reason"),
            self._notice_section_value(qt_sections, ["原因", "故障原因", "故障维修原因"]),
        )
        set_text_if_missing("building", qt_payload.get("building"))
        set_text_if_missing("building_code", qt_payload.get("building_code"))

        building_codes = self._building_codes_from_request_payload(enriched)
        if not building_codes:
            building_codes = self._clean_building_codes(qt_payload.get("building_codes"))
        if not building_codes:
            building_codes = self._building_codes_from_value(
                qt_payload.get("building")
                or qt_payload.get("title")
                or qt_payload.get("content")
            )
        if not building_codes:
            building_codes = self._source_record_building_codes_for_delete(
                work_type,
                str(enriched.get("source_record_id") or "").strip(),
                scope,
            )
        if building_codes:
            enriched["building_codes"] = building_codes
            if not str(enriched.get("building") or "").strip():
                enriched["building"] = self._building_label_from_codes(building_codes)

        target_record_id = canonical_target_record_id(enriched)
        source_record_id = str(enriched.get("source_record_id") or "").strip()
        current_record_id = str(enriched.get("record_id") or "").strip()
        if not target_record_id:
            target_record_id = self._target_record_id_from_identity_map(
                work_type=work_type,
                active_item_id=str(enriched.get("active_item_id") or "").strip(),
                source_record_id=source_record_id,
                target_record_id="",
            )
        if target_record_id:
            enriched["target_record_id"] = target_record_id
            enriched["record_id"] = target_record_id
        elif source_record_id and current_record_id == source_record_id:
            enriched["_source_only_delete"] = True
        return enriched

    def create_notice_undo_checkpoint(
        self,
        action_type: str,
        context: dict[str, Any],
        *,
        remote_fields: dict[str, Any] | None = None,
        remote_missing: bool = False,
        job_id: str = "",
        created_by: str = "",
        scope: str = "ALL",
    ) -> str:
        action_type = str(action_type or "").strip().lower()
        if action_type not in {"update", "end", "delete"}:
            return ""
        context = copy.deepcopy(context) if isinstance(context, dict) else {}
        scope = self._normalize_scope(scope or context.get("scope") or "ALL")
        context = self._enrich_ongoing_identity_item(context, scope=scope)
        identity = self._undo_identity_from_context(context, action_type=action_type)
        identity_keys = set(identity.get("identity_keys") or [])
        if not identity.get("identity_key") or not identity_keys:
            raise PortalError("创建回退点失败：缺少通告身份。")
        qt_active = self._find_qt_active_snapshot(identity)
        daily_item = None
        daily_document_key = ""
        work_items: list[dict[str, Any]] = []
        with self._summary_lock:
            summary_payload = self._load_day_summary_locked()
            for item in summary_payload.get("items") or []:
                if isinstance(item, dict) and self._items_identity_intersects(
                    identity_keys, self._work_status_identity_keys(item)
                ):
                    daily_item = copy.deepcopy(item)
                    daily_document_key = str(summary_payload.get("date") or "").strip()
                    break
            if daily_item is None:
                for document in self._state_store.list_documents(STATE_NS_DAILY_SUMMARY):
                    payload = document.get("payload")
                    if not isinstance(payload, dict):
                        continue
                    for item in payload.get("items") or []:
                        if isinstance(item, dict) and self._items_identity_intersects(
                            identity_keys, self._work_status_identity_keys(item)
                        ):
                            daily_item = copy.deepcopy(item)
                            daily_document_key = str(document.get("key") or "").strip()
                            break
                    if daily_item is not None:
                        break
            self._migrate_legacy_work_status_locked()
            for document in self._state_store.list_documents(STATE_NS_WORK_STATUS):
                payload = document.get("payload")
                if not isinstance(payload, dict):
                    continue
                for item in payload.get("items") or []:
                    if isinstance(item, dict) and self._items_identity_intersects(
                        identity_keys, self._work_status_identity_keys(item)
                    ):
                        work_items.append(
                            {
                                "document_key": str(document.get("key") or ""),
                                "item": copy.deepcopy(item),
                            }
                        )
        now = time.time()
        title = str(identity.get("title") or (daily_item or {}).get("title") or "").strip()
        payload = {
            "undo_id": uuid.uuid4().hex,
            "identity_key": str(identity.get("identity_key") or ""),
            "identity_keys": sorted(identity_keys),
            "status": "available",
            "action_type": action_type,
            "action_label": self._undo_action_label(action_type),
            "scope": scope,
            "work_type": str(identity.get("work_type") or ""),
            "notice_type": str(identity.get("notice_type") or ""),
            "active_item_id": str(identity.get("active_item_id") or ""),
            "source_record_id": str(identity.get("source_record_id") or ""),
            "target_record_id": str(identity.get("target_record_id") or ""),
            "title": title,
            "reason": str(identity.get("reason") or ""),
            "building": str(identity.get("building") or ""),
            "building_codes": list(identity.get("building_codes") or []),
            "created_by": str(created_by or context.get("_auth_open_id") or ""),
            "job_id": str(job_id or ""),
            "created_at": now,
            "expires_at": now + 7 * 24 * 60 * 60,
            "context": context,
            "remote": {
                "missing": bool(remote_missing),
                "fields": copy.deepcopy(remote_fields or {}),
            },
            "local": {
                "qt_active": qt_active,
                "daily_document_key": daily_document_key,
                "daily_item": daily_item,
                "work_items": work_items,
            },
        }
        undo_id = self._state_store.create_notice_undo_action(payload)
        if not undo_id:
            raise PortalError("创建回退点失败：SQLite 写入失败。")
        self._state_store.append_event_async(
            "notice_undo",
            {
                "event": "checkpoint_created",
                "undo_id": undo_id,
                "action_type": action_type,
                "title": title,
                "target_record_id": str(identity.get("target_record_id") or ""),
            },
        )
        return undo_id

    def _undo_key_candidates(self, item: dict[str, Any]) -> set[str]:
        identity = self._undo_identity_from_context(item, action_type="delete")
        return set(identity.get("identity_keys") or [])

    def _available_undo_map(self, scope: str = "ALL") -> dict[str, dict[str, Any]]:
        scope = self._normalize_scope(scope)
        result: dict[str, dict[str, Any]] = {}
        for undo in self._state_store.list_notice_undo_actions(scope=scope):
            enriched_undo = self._enrich_ongoing_identity_item(undo, scope=scope)
            if not self._scope_matches_item(scope, enriched_undo):
                continue
            keys = set(enriched_undo.get("identity_keys") or undo.get("identity_keys") or [])
            for key in keys:
                if key and key not in result:
                    result[key] = enriched_undo
        return result

    @staticmethod
    def _public_undo_fields(undo: dict[str, Any]) -> dict[str, Any]:
        created_at = float((undo or {}).get("created_at") or 0)
        action_type = str((undo or {}).get("action_type") or "")
        label = MaintenancePortalService._undo_action_label(action_type)
        return {
            "undo_available": True,
            "undo_id": str((undo or {}).get("undo_id") or ""),
            "undo_action_type": action_type,
            "undo_created_at": created_at,
            "undo_label": f"回退上一步{label}",
        }

    def _annotate_undo_items(
        self, items: list[dict[str, Any]], *, scope: str = "ALL"
    ) -> list[dict[str, Any]]:
        undo_map = self._available_undo_map(scope)
        annotated: list[dict[str, Any]] = []
        for item in items or []:
            row = copy.deepcopy(item)
            for key in self._undo_key_candidates(row):
                undo = undo_map.get(key)
                if undo:
                    row.update(self._public_undo_fields(undo))
                    break
            annotated.append(row)
        return annotated

    def list_available_notice_undos(
        self,
        *,
        scope: str = "ALL",
        action_type: str = "",
        since_seconds: float = 0,
    ) -> list[dict[str, Any]]:
        scope = self._normalize_scope(scope)
        action_type = str(action_type or "").strip().lower()
        cutoff = time.time() - float(since_seconds or 0) if float(since_seconds or 0) > 0 else 0
        items: list[dict[str, Any]] = []
        for undo in self._state_store.list_notice_undo_actions(scope=scope):
            enriched_undo = self._enrich_ongoing_identity_item(undo, scope=scope)
            if not self._scope_matches_item(scope, enriched_undo):
                continue
            undo_action_type = str(enriched_undo.get("action_type") or "").strip().lower()
            created_at = float(enriched_undo.get("created_at") or 0)
            if action_type and undo_action_type != action_type:
                continue
            if cutoff and created_at < cutoff:
                continue
            items.append(
                {
                    **self._public_undo_fields(enriched_undo),
                    "title": str(enriched_undo.get("title") or ""),
                    "scope": str(enriched_undo.get("scope") or ""),
                    "work_type": str(enriched_undo.get("work_type") or ""),
                    "notice_type": str(enriched_undo.get("notice_type") or ""),
                    "building": str(enriched_undo.get("building") or ""),
                }
            )
        items.sort(key=lambda row: float(row.get("undo_created_at") or 0), reverse=True)
        return items

    def create_notice_undo_job(
        self,
        undo_id: str,
        *,
        scope: str = "ALL",
        auth_open_id: str = "",
        auth_user_name: str = "",
    ) -> str:
        undo = self._state_store.get_notice_undo_action(undo_id)
        if not undo or str(undo.get("status") or "") != "available":
            raise PortalError("该回退记录不可用或已过期。")
        scope = self._normalize_scope(scope or undo.get("scope") or "ALL")
        undo = self._enrich_ongoing_identity_item(undo, scope=scope)
        if not self._scope_matches_item(scope, undo):
            raise PortalError("当前账号无权回退该通告。")
        request_payload = {
            "action": "undo",
            "undo_id": str(undo.get("undo_id") or ""),
            "scope": scope,
            "work_type": str(undo.get("work_type") or WORK_TYPE_MAINTENANCE),
            "notice_type": str(undo.get("notice_type") or ""),
            "active_item_id": str(undo.get("active_item_id") or ""),
            "source_record_id": str(undo.get("source_record_id") or ""),
            "target_record_id": str(undo.get("target_record_id") or ""),
            "_auth_open_id": str(auth_open_id or ""),
            "_auth_user_name": str(auth_user_name or ""),
        }
        job = self._base_job(request_payload)
        job["phase"] = "undo_queued"
        job["operation_id"] = f"undo:{undo_id}"
        job["target_key"] = f"undo:{str(undo.get('identity_key') or undo_id)}"
        with self._jobs_lock:
            for existing in self._jobs.values():
                if str(existing.get("target_key") or "") != job["target_key"]:
                    continue
                if str(existing.get("phase") or "") in {
                    "accepted",
                    "queued",
                    "sending_message",
                    "message_sent",
                    "upload_queued",
                    "qt_queued",
                    "qt_displaying",
                    "upload_waiting",
                    "uploading",
                    "undo_queued",
                    "undoing_remote",
                    "undoing_local",
                }:
                    raise PortalError("该通告正在处理，请稍后再试。")
            self._jobs[job["job_id"]] = job
            self._persist_action_job_locked(job)
            self._trim_jobs_locked()
        return str(job["job_id"])

    def _remove_hidden_ongoing_keys(self, keys: set[str]) -> None:
        if not keys:
            return
        with self._hidden_ongoing_lock:
            payload = self._load_hidden_ongoing_locked()
            hidden = payload.get("hidden") if isinstance(payload.get("hidden"), dict) else {}
            removed = False
            for key in list(keys):
                if key in hidden:
                    hidden.pop(key, None)
                    removed = True
            if removed:
                payload["hidden"] = hidden
                self._save_hidden_ongoing_locked(payload)

    def restore_notice_undo_local(
        self,
        undo: dict[str, Any],
        *,
        target_record_id: str = "",
        applied_by: str = "",
        job_id: str = "",
    ) -> dict[str, Any]:
        if not isinstance(undo, dict):
            raise PortalError("回退记录格式错误。")
        local = undo.get("local") if isinstance(undo.get("local"), dict) else {}
        identity_keys = set(str(key or "") for key in (undo.get("identity_keys") or []) if str(key or ""))
        now = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        restored_active = False
        removed_active = False
        with self._summary_lock:
            daily_item = local.get("daily_item") if isinstance(local.get("daily_item"), dict) else None
            daily_document_key = str(
                local.get("daily_document_key") or (daily_item or {}).get("date") or ""
            ).strip()
            summary_payload = self._load_day_summary_locked(daily_document_key or None)
            current_summary = summary_payload.get("items")
            if not isinstance(current_summary, list):
                current_summary = []
            summary_payload["items"] = [
                item
                for item in current_summary
                if not (
                    isinstance(item, dict)
                    and self._items_identity_intersects(
                        identity_keys, self._work_status_identity_keys(item)
                    )
                )
            ]
            if daily_item:
                restored_daily = copy.deepcopy(daily_item)
                if target_record_id:
                    restored_daily["target_record_id"] = target_record_id
                    restored_daily["feishu_record_id"] = target_record_id
                summary_payload["items"].append(restored_daily)
            summary_payload["updated_at"] = now
            self._save_day_summary_locked(summary_payload)

            self._migrate_legacy_work_status_locked()
            for document in self._state_store.list_documents(STATE_NS_WORK_STATUS):
                payload = document.get("payload")
                if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
                    continue
                kept = [
                    item
                    for item in payload.get("items") or []
                    if not (
                        isinstance(item, dict)
                        and self._items_identity_intersects(
                            identity_keys, self._work_status_identity_keys(item)
                        )
                    )
                ]
                if len(kept) != len(payload.get("items") or []):
                    payload["items"] = kept
                    payload["updated_at"] = now
                    self._state_store.put_document(
                        STATE_NS_WORK_STATUS, str(document.get("key") or ""), payload
                    )
            for saved in local.get("work_items") or []:
                if not isinstance(saved, dict) or not isinstance(saved.get("item"), dict):
                    continue
                document_key = str(saved.get("document_key") or "ALL").strip() or "ALL"
                payload = self._state_store.get_document(STATE_NS_WORK_STATUS, document_key)
                if not isinstance(payload, dict):
                    payload = {"version": 1, "items": []}
                if not isinstance(payload.get("items"), list):
                    payload["items"] = []
                item = copy.deepcopy(saved.get("item") or {})
                if target_record_id:
                    item["target_record_id"] = target_record_id
                    item["feishu_record_id"] = target_record_id
                payload["items"].append(item)
                payload["updated_at"] = now
                self._state_store.put_document(STATE_NS_WORK_STATUS, document_key, payload)
            self._work_status_cache_signature = None
            self._work_status_cache_items = None
        qt_snapshot = local.get("qt_active") if isinstance(local.get("qt_active"), dict) else None
        if qt_snapshot and isinstance(qt_snapshot.get("payload"), dict):
            qt_payload = copy.deepcopy(qt_snapshot.get("payload") or {})
            if target_record_id:
                qt_payload["record_id"] = target_record_id
                qt_payload["target_record_id"] = target_record_id
            restored_active = self._state_store.upsert_qt_active_item(
                qt_payload,
                section=str(qt_snapshot.get("section") or ""),
                sort_order=int(qt_snapshot.get("sort_order") or 0),
                origin=str(qt_snapshot.get("origin") or ""),
            )
        else:
            removed_active = self._state_store.delete_qt_active_item(
                active_item_id=str(undo.get("active_item_id") or ""),
                record_id=str(target_record_id or undo.get("target_record_id") or ""),
            )
        self._remove_hidden_ongoing_keys(identity_keys)
        self._touch_state_cache_version()
        with suppress(Exception):
            self._state_store.append_event(
                "notice_undo",
                {
                    "event": "undo_applied_local",
                    "undo_id": str(undo.get("undo_id") or ""),
                    "job_id": str(job_id or ""),
                    "applied_by": str(applied_by or ""),
                    "target_record_id": str(target_record_id or undo.get("target_record_id") or ""),
                },
            )
        return {
            "restored_active": restored_active,
            "removed_active": removed_active,
            "active_payload": (qt_snapshot or {}).get("payload") if qt_snapshot else {},
        }

    def validate_ongoing_delete_item(
        self, item: dict[str, Any], *, scope: str = "ALL"
    ) -> list[str]:
        if not isinstance(item, dict):
            raise PortalError("删除参数格式错误。")
        scope = self._normalize_scope(scope)
        enriched = self._enrich_ongoing_identity_item(item, scope=scope)
        item.clear()
        item.update(enriched)
        if not self._scope_matches_item(scope, enriched):
            raise PortalError("当前账号无权删除该楼栋的进行中通告。")
        keys = self._ongoing_hidden_keys(enriched)
        if not keys:
            raise PortalError("该进行中通告缺少可删除身份。")
        return keys

    def _target_record_is_finished(
        self, *, work_type: str, notice_type: str, record_id: str
    ) -> bool:
        record_id = str(record_id or "").strip()
        if not record_id:
            return False
        field_config = get_field_config(notice_type)
        status_field = field_config.get("status", "")
        if not status_field:
            return False
        try:
            target_records = self._target_records_for_notice_type(notice_type, work_type)
        except Exception as exc:
            warning = f"{notice_type}目标表完成状态检查失败: {exc}"
            if warning not in self._load_warnings:
                self._load_warnings.append(warning)
            return False
        for target in target_records:
            if str(target.get("record_id") or "").strip() != record_id:
                continue
            fields = target.get("display_fields") or {}
            return self._target_status_is_finished(fields.get(status_field))
        return False

    def _target_record_building_codes(
        self, fields: dict[str, Any], field_config: dict[str, str]
    ) -> list[str]:
        building_field = (
            field_config.get("building")
            or field_config.get("building_codes")
            or "楼栋"
        )
        return self._building_codes_from_value(fields.get(building_field))

    def _source_target_match_profile(
        self, work_type: str, record: dict[str, Any]
    ) -> tuple[str, set[str], list[str]]:
        fields = record.get("display_fields") or {}
        if work_type == WORK_TYPE_CHANGE:
            start_time, end_time, _ = self._change_time_range(record)
            return (
                self._change_title(record),
                self._date_keys_from_values(start_time, end_time),
                self._change_record_building_codes(record),
            )
        if work_type == WORK_TYPE_REPAIR:
            return (
                self._repair_title(record),
                self._date_keys_from_values(
                    fields.get("维修开始时间"),
                    fields.get("故障发生时间"),
                    fields.get("期望完成时间"),
                    fields.get("维修结束时间"),
                    fields.get("维修结束时间（2026）"),
                ),
                self._repair_record_building_codes(record),
            )
        building = str(fields.get("楼栋") or "").strip()
        title = f"EA118机房{building}{str(fields.get('维护总项') or '').strip()}"
        return (
            title,
            self._date_keys_from_values(
                fields.get("计划开始时间"),
                fields.get("计划结束时间"),
                fields.get("实际开始时间"),
            ),
            self._building_codes_from_value(building),
        )

    def _target_match_date_fields(
        self, work_type: str, field_config: dict[str, str]
    ) -> list[str]:
        if work_type == WORK_TYPE_CHANGE:
            return [
                field_config.get("start_time", ""),
                field_config.get("end_time", ""),
                field_config.get("update_time", ""),
            ]
        if work_type == WORK_TYPE_REPAIR:
            return [
                field_config.get("actual_start", ""),
                field_config.get("fault_time", ""),
                field_config.get("expected_time", ""),
            ]
        if work_type in {WORK_TYPE_POWER, WORK_TYPE_POLLING, WORK_TYPE_ADJUST}:
            return [
                field_config.get("plan_start", ""),
                field_config.get("plan_end", ""),
                field_config.get("actual_start", ""),
                field_config.get("actual_end", ""),
            ]
        return [
            field_config.get("actual_start", ""),
            field_config.get("plan_start", ""),
            field_config.get("plan_end", ""),
        ]

    def _target_records_for_notice_type(
        self, notice_type: str, work_type: str, *, force_refresh: bool = False
    ) -> list[dict[str, Any]]:
        table_id = str(config.get_table_id(notice_type) or "").strip()
        app_token = str(config.app_token or "").strip()
        if not app_token or not table_id:
            return []
        cache_key = (notice_type, table_id)
        with self._refresh_lock:
            cached = self._target_record_cache.get(cache_key) or {}
            if (
                not force_refresh
                and
                cached.get("records") is not None
                and time.time() - float(cached.get("loaded_ts") or 0)
                < self._source_cache_ttl_seconds()
            ):
                return list(cached.get("records") or [])
            _metas, meta_by_name = self._load_table_fields(
                app_token=app_token, table_id=table_id
            )
            records = self._load_table_records(
                app_token=app_token,
                table_id=table_id,
                meta_by_name=meta_by_name,
                work_type=work_type,
                notice_type=notice_type,
            )
            self._target_record_cache[cache_key] = {
                "loaded_ts": time.time(),
                "records": records,
            }
            return list(records)

    def _resolve_target_record_reference(
        self,
        *,
        work_type: str,
        notice_type: str,
        source_record: dict[str, Any],
        include_finished: bool = False,
        only_finished: bool = False,
    ) -> str:
        field_config = get_field_config(notice_type)
        title_field = (
            field_config.get("title")
            or field_config.get("name")
            or "名称"
        )
        status_field = field_config.get("status", "")
        source_title, source_dates, source_codes = self._source_target_match_profile(
            work_type, source_record
        )
        source_title_key = self._match_text(source_title)
        if not source_title_key or not source_dates:
            return ""
        date_fields = [
            field_name
            for field_name in self._target_match_date_fields(work_type, field_config)
            if field_name
        ]
        matches: list[str] = []
        try:
            target_records = self._target_records_for_notice_type(notice_type, work_type)
        except Exception as exc:
            self._load_warnings = list(self._load_warnings or [])
            warning = f"{notice_type}目标表自动引用失败: {exc}"
            if warning not in self._load_warnings:
                self._load_warnings.append(warning)
            return ""
        for target in target_records:
            fields = target.get("display_fields") or {}
            target_finished = bool(
                status_field and self._target_status_is_finished(fields.get(status_field))
            )
            if only_finished and not target_finished:
                continue
            if target_finished and not include_finished and not only_finished:
                continue
            if self._match_text(fields.get(title_field)) != source_title_key:
                continue
            target_dates = self._date_keys_from_values(
                *[fields.get(field_name) for field_name in date_fields]
            )
            if not target_dates or not (source_dates & target_dates):
                continue
            target_codes = self._target_record_building_codes(fields, field_config)
            if source_codes and target_codes and source_codes != target_codes:
                continue
            record_id = str(target.get("record_id") or "").strip()
            if record_id:
                matches.append(record_id)
        return matches[0] if len(set(matches)) == 1 else ""

    def lookup_change_target_candidates(
        self,
        *,
        scope: str,
        title: str,
        start_time: str = "",
        end_time: str = "",
        action: str = "update",
        content: str = "",
        reason: str = "",
        impact: str = "",
        progress: str = "",
        text: str = "",
        limit: int = 30,
    ) -> dict[str, Any]:
        scope = self._normalize_scope(scope)
        title = str(title or "").strip()
        if not title:
            raise PortalError("变更通告缺少【名称】，无法查询目标记录。")
        self.ensure_snapshot_loaded()
        title_key = self._match_text(title)
        query_dates = self._date_keys_from_values(start_time, end_time)
        field_config = get_field_config(NOTICE_TYPE_CHANGE)
        title_field = field_config.get("title") or field_config.get("name") or "名称"
        status_field = field_config.get("status", "")
        date_fields = [
            field_name
            for field_name in self._target_match_date_fields(WORK_TYPE_CHANGE, field_config)
            if field_name
        ]
        lookup_text_values = [content, reason, impact, progress]
        if not any(str(value or "").strip() for value in lookup_text_values):
            lookup_text_values.append(text)
        try:
            target_records = self._target_records_for_notice_type(
                NOTICE_TYPE_CHANGE,
                WORK_TYPE_CHANGE,
                force_refresh=True,
            )
        except Exception as exc:
            raise PortalError(f"查询设备变更目标表失败：{exc}") from exc
        candidates: list[dict[str, Any]] = []
        for target in target_records:
            fields = target.get("display_fields") or {}
            target_title = str(fields.get(title_field) or "").strip()
            title_matched = self._source_candidate_title_matches(title, target_title, WORK_TYPE_CHANGE)
            business_match_count = self._target_business_match_count(
                fields,
                lookup_text_values,
            ) + self._target_named_business_match_count(
                fields,
                {
                    "title": title,
                    "content": content,
                    "reason": reason,
                    "impact": impact,
                    "progress": progress,
                },
                field_config,
            )
            if not title_matched and business_match_count <= 0:
                continue
            target_dates = self._date_keys_from_values(
                *[fields.get(field_name) for field_name in date_fields],
                fields.get("时间"),
                fields.get("计划时间"),
            )
            building_codes = self._target_record_building_codes(fields, field_config)
            if building_codes and not self._scope_matches_buildings(scope, building_codes):
                continue
            record_id = str(target.get("record_id") or "").strip()
            if not record_id:
                continue
            target_start = str(fields.get(field_config.get("start_time", "")) or "").strip()
            target_end = str(fields.get(field_config.get("end_time", "")) or "").strip()
            status = str(fields.get(status_field) or "").strip() if status_field else ""
            detail_fields = self._target_candidate_detail_fields(fields)
            date_matched = bool(query_dates and target_dates and (query_dates & target_dates))
            match_reason = self._target_candidate_match_reason(
                date_matched=date_matched,
                title_matched=title_matched,
                business_match_count=business_match_count,
                building_matched=bool(building_codes),
                status=status,
            )
            candidates.append(
                {
                    "record_id": record_id,
                    "target_record_id": record_id,
                    "title": target_title or title,
                    "building": self._building_label_from_codes(building_codes),
                    "building_codes": building_codes,
                    "status": status,
                    "start_time": target_start,
                    "end_time": target_end,
                    "date_matched": date_matched,
                    "title_matched": title_matched,
                    "business_text_matched": business_match_count > 0,
                    "business_match_count": business_match_count,
                    "match_reason": match_reason,
                    "fields": detail_fields,
                    "field_items": [
                        {"label": key, "value": value}
                        for key, value in detail_fields.items()
                    ],
                }
            )
        candidates.sort(
            key=lambda item: (
                0 if item.get("date_matched") else 1,
                0 if str(item.get("status") or "") != "结束" else 1,
                str(item.get("start_time") or ""),
            )
        )
        candidates, result_meta = self._candidate_result_meta(candidates, limit)
        return {
            "scope": scope,
            "action": str(action or "update").strip().lower(),
            "title": title,
            "start_time": str(start_time or "").strip(),
            "end_time": str(end_time or "").strip(),
            **result_meta,
            "candidates": candidates,
            "source_candidates": self._lookup_change_source_candidates(
                scope=scope,
                title=title,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            ),
        }

    def lookup_notice_target_candidates(
        self,
        *,
        work_type: str,
        scope: str,
        title: str,
        start_time: str = "",
        end_time: str = "",
        action: str = "update",
        content: str = "",
        reason: str = "",
        impact: str = "",
        progress: str = "",
        text: str = "",
        limit: int = 30,
    ) -> dict[str, Any]:
        work_type = str(work_type or WORK_TYPE_MAINTENANCE).strip()
        aliases = {
            "maintenance": WORK_TYPE_MAINTENANCE,
            "维保": WORK_TYPE_MAINTENANCE,
            "维保通告": WORK_TYPE_MAINTENANCE,
            "change": WORK_TYPE_CHANGE,
            "变更": WORK_TYPE_CHANGE,
            "设备变更": WORK_TYPE_CHANGE,
            "repair": WORK_TYPE_REPAIR,
            "检修": WORK_TYPE_REPAIR,
            "设备检修": WORK_TYPE_REPAIR,
            "power": WORK_TYPE_POWER,
            "上电": WORK_TYPE_POWER,
            "上电通告": WORK_TYPE_POWER,
            "上下电通告": WORK_TYPE_POWER,
            "polling": WORK_TYPE_POLLING,
            "轮巡": WORK_TYPE_POLLING,
            "设备轮巡": WORK_TYPE_POLLING,
            "adjust": WORK_TYPE_ADJUST,
            "调整": WORK_TYPE_ADJUST,
            "设备调整": WORK_TYPE_ADJUST,
        }
        work_type = aliases.get(work_type.lower()) or aliases.get(work_type) or WORK_TYPE_MAINTENANCE
        if work_type == WORK_TYPE_CHANGE:
            return self.lookup_change_target_candidates(
                scope=scope,
                title=title,
                start_time=start_time,
                end_time=end_time,
                action=action,
                content=content,
                reason=reason,
                impact=impact,
                progress=progress,
                text=text,
                limit=limit,
            )
        notice_type = (
            NOTICE_TYPE_REPAIR
            if work_type == WORK_TYPE_REPAIR
            else NOTICE_TYPE_POWER
            if work_type == WORK_TYPE_POWER
            else NOTICE_TYPE_POLLING
            if work_type == WORK_TYPE_POLLING
            else NOTICE_TYPE_ADJUST
            if work_type == WORK_TYPE_ADJUST
            else NOTICE_TYPE_MAINTENANCE
        )
        scope = self._normalize_scope(scope)
        title = str(title or "").strip()
        if not title:
            raise PortalError(f"{self._history_work_type_label(work_type)}通告缺少标题，无法查询目标记录。")
        title_key = self._match_text(title)
        query_dates = self._date_keys_from_values(start_time, end_time)
        field_config = get_field_config(notice_type)
        title_field = (
            field_config.get("title")
            or field_config.get("name")
            or "名称"
        )
        status_field = field_config.get("status", "")
        date_fields = [
            field_name
            for field_name in self._target_match_date_fields(work_type, field_config)
            if field_name
        ]
        lookup_text_values = [content, reason, impact, progress]
        if not any(str(value or "").strip() for value in lookup_text_values):
            lookup_text_values.append(text)
        try:
            target_records = self._target_records_for_notice_type(
                notice_type,
                work_type,
                force_refresh=True,
            )
        except Exception as exc:
            raise PortalError(f"查询{notice_type}目标表失败：{exc}") from exc
        candidates: list[dict[str, Any]] = []
        for target in target_records:
            fields = target.get("display_fields") or {}
            target_title = str(fields.get(title_field) or fields.get("名称") or "").strip()
            title_matched = self._source_candidate_title_matches(title, target_title, work_type)
            business_match_count = self._target_business_match_count(
                fields,
                lookup_text_values,
            ) + self._target_named_business_match_count(
                fields,
                {
                    "title": title,
                    "content": content,
                    "reason": reason,
                    "impact": impact,
                    "progress": progress,
                },
                field_config,
            )
            if not title_matched and business_match_count <= 0:
                continue
            building_codes = self._target_record_building_codes(fields, field_config)
            if building_codes and not self._scope_matches_buildings(scope, building_codes):
                continue
            record_id = str(target.get("record_id") or "").strip()
            if not record_id:
                continue
            target_dates = self._date_keys_from_values(
                *[fields.get(field_name) for field_name in date_fields],
                fields.get("时间"),
                fields.get("计划时间"),
            )
            target_start = str(
                fields.get(field_config.get("plan_start", ""))
                or fields.get(field_config.get("actual_start", ""))
                or fields.get(field_config.get("start_time", ""))
                or fields.get(field_config.get("expected_time", ""))
                or ""
            ).strip()
            target_end = str(
                fields.get(field_config.get("plan_end", ""))
                or fields.get(field_config.get("actual_end", ""))
                or fields.get(field_config.get("end_time", ""))
                or fields.get(field_config.get("fault_time", ""))
                or ""
            ).strip()
            status = str(fields.get(status_field) or "").strip() if status_field else ""
            detail_fields = self._target_candidate_detail_fields(fields)
            date_matched = bool(query_dates and target_dates and (query_dates & target_dates))
            match_reason = self._target_candidate_match_reason(
                date_matched=date_matched,
                title_matched=title_matched,
                business_match_count=business_match_count,
                building_matched=bool(building_codes),
                status=status,
            )
            candidates.append(
                {
                    "record_id": record_id,
                    "target_record_id": record_id,
                    "work_type": work_type,
                    "notice_type": notice_type,
                    "title": target_title or title,
                    "building": self._building_label_from_codes(building_codes),
                    "building_codes": building_codes,
                    "status": status,
                    "start_time": target_start,
                    "end_time": target_end,
                    "date_matched": date_matched,
                    "title_matched": title_matched,
                    "business_text_matched": business_match_count > 0,
                    "business_match_count": business_match_count,
                    "match_reason": match_reason,
                    "fields": detail_fields,
                    "field_items": [
                        {"label": key, "value": value}
                        for key, value in detail_fields.items()
                    ],
                }
            )
        candidates.sort(
            key=lambda item: (
                0 if item.get("date_matched") else 1,
                0 if not self._target_status_is_finished(item.get("status")) else 1,
                str(item.get("start_time") or ""),
            )
        )
        candidates, result_meta = self._candidate_result_meta(candidates, limit)
        return {
            "scope": scope,
            "work_type": work_type,
            "notice_type": notice_type,
            "action": str(action or "update").strip().lower(),
            "title": title,
            "start_time": str(start_time or "").strip(),
            "end_time": str(end_time or "").strip(),
            **result_meta,
            "candidates": candidates,
            "source_candidates": self._lookup_notice_source_candidates(
                work_type=work_type,
                scope=scope,
                title=title,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            ),
        }

    @staticmethod
    def _target_candidate_match_reason(
        *,
        date_matched: bool,
        title_matched: bool,
        business_match_count: int,
        building_matched: bool,
        status: str,
    ) -> str:
        reasons: list[str] = []
        if date_matched:
            reasons.append("时间匹配")
        if title_matched:
            reasons.append("标题匹配")
        if business_match_count > 0:
            reasons.append(f"内容字段匹配 {business_match_count} 项")
        if building_matched:
            reasons.append("楼栋匹配")
        if str(status or "").strip() not in {"结束", "已结束"}:
            reasons.append("未结束优先")
        return " / ".join(reasons) or "相似记录"

    @staticmethod
    def _candidate_result_limit(limit: Any, *, default: int = 30, maximum: int = 80) -> int:
        try:
            value = int(limit if limit not in (None, "") else default)
        except Exception:
            value = default
        return max(1, min(value, maximum))

    @classmethod
    def _candidate_result_meta(cls, candidates: list, limit: Any) -> tuple[list, dict[str, Any]]:
        limit_value = cls._candidate_result_limit(limit)
        total_matched = len(candidates)
        visible = candidates[:limit_value]
        return visible, {
            "count": len(visible),
            "returned_count": len(visible),
            "total_matched": total_matched,
            "limit": limit_value,
            "limited": total_matched > len(visible),
        }

    def _target_business_match_count(
        self,
        fields: dict[str, Any],
        lookup_values: list[Any],
    ) -> int:
        if not isinstance(fields, dict):
            return 0
        blob = self._match_text(
            " ".join(self._clean_source_text(value) for value in fields.values())
        )
        if not blob:
            return 0
        matched = 0
        seen: set[str] = set()
        for value in lookup_values or []:
            key = self._match_text(value)
            if not key or key in seen:
                continue
            seen.add(key)
            if len(key) < 4:
                continue
            if key in blob or (len(key) >= 12 and blob in key):
                matched += 1
        return matched

    def _target_named_business_match_count(
        self,
        fields: dict[str, Any],
        lookup_values: dict[str, Any],
        field_config: dict[str, Any],
    ) -> int:
        if not isinstance(fields, dict) or not isinstance(lookup_values, dict):
            return 0
        aliases = {
            "title": [
                field_config.get("title"),
                field_config.get("name"),
                "名称",
                "名称（标题）",
                "标题",
            ],
            "content": [field_config.get("content"), "内容", "工作内容"],
            "reason": [field_config.get("reason"), "原因", "故障原因"],
            "impact": [field_config.get("impact"), "影响", "影响范围"],
            "progress": [field_config.get("progress"), "进度", "完成情况"],
        }
        matched = 0
        for key, names in aliases.items():
            source_value = str(lookup_values.get(key) or "").strip()
            if not source_value:
                continue
            source_key = self._match_text(source_value)
            if len(source_key) < 4:
                continue
            target_values = []
            for name in names:
                name = str(name or "").strip()
                if name and name in fields:
                    target_values.append(fields.get(name))
            for target_value in target_values:
                target_key = self._match_text(self._clean_source_text(target_value))
                if not target_key:
                    continue
                if source_key in target_key or target_key in source_key:
                    matched += 1
                    break
        return matched

    def _target_candidate_detail_fields(self, fields: dict[str, Any]) -> dict[str, str]:
        if not isinstance(fields, dict):
            return {}
        detail_fields: dict[str, str] = {}
        for key, value in fields.items():
            label = str(key or "").strip()
            if not label:
                continue
            text = self._clean_source_text(value)
            if not text:
                continue
            if len(text) > 500:
                text = text[:500] + "..."
            detail_fields[label] = text
        return detail_fields

    def _lookup_change_source_candidates(
        self,
        *,
        scope: str,
        title: str,
        start_time: str = "",
        end_time: str = "",
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        title_key = self._match_text(title)
        if not title_key:
            return []
        query_dates = self._date_keys_from_values(start_time, end_time)
        candidates: list[dict[str, Any]] = []
        for record in list(self._change_records or []):
            if not isinstance(record, dict):
                continue
            source_title = self._change_title(record)
            if not self._source_candidate_title_matches(title, source_title, WORK_TYPE_CHANGE):
                continue
            building_codes = self._change_record_building_codes(record)
            if not self._scope_matches_buildings(scope, building_codes):
                continue
            source_start, source_end, _ = self._change_time_range(record)
            source_dates = self._date_keys_from_values(source_start, source_end)
            fields = record.get("display_fields") or {}
            detail_fields = self._target_candidate_detail_fields(fields)
            candidates.append(
                {
                    "record_id": str(record.get("record_id") or ""),
                    "source_record_id": str(record.get("record_id") or ""),
                    "source_app_token": CHANGE_SOURCE_APP_TOKEN,
                    "source_table_id": CHANGE_SOURCE_TABLE_ID,
                    "title": source_title or title,
                    "building": self._building_label_from_codes(building_codes),
                    "building_codes": building_codes,
                    "status": self._change_progress_value(record),
                    "start_time": source_start,
                    "end_time": source_end,
                    "date_matched": bool(query_dates and source_dates and (query_dates & source_dates)),
                    "fields": detail_fields,
                    "field_items": [
                        {"label": key, "value": value}
                        for key, value in detail_fields.items()
                    ],
                }
            )
        candidates.sort(
            key=lambda item: (
                0 if item.get("date_matched") else 1,
                0 if str(item.get("status") or "") != CHANGE_PROGRESS_ENDED else 1,
                str(item.get("start_time") or ""),
            )
        )
        return candidates[: max(1, int(limit or 30))]

    def _source_records_for_notice_lookup(self, work_type: str) -> list[dict[str, Any]]:
        if work_type == WORK_TYPE_REPAIR:
            primary = list(self._repair_records or [])
        elif work_type == WORK_TYPE_MAINTENANCE:
            primary = list(self._records or [])
        else:
            return []
        by_id: dict[str, dict[str, Any]] = {}
        for record in primary + list(self._source_snapshot_records("ALL") or []):
            if not isinstance(record, dict):
                continue
            if self._record_work_type(record) != work_type:
                continue
            record_id = str(record.get("record_id") or "").strip()
            if record_id and record_id not in by_id:
                by_id[record_id] = record
        return list(by_id.values())

    def _source_candidate_title(self, record: dict[str, Any], work_type: str) -> str:
        fields = record.get("display_fields") or {}
        if work_type == WORK_TYPE_REPAIR:
            return self._repair_title(record)
        if work_type == WORK_TYPE_MAINTENANCE:
            explicit = str(record.get("title") or fields.get("名称") or fields.get("标题") or "").strip()
            if explicit:
                return explicit
            building = str(fields.get("楼栋") or "").strip()
            total = str(fields.get("维护总项") or "").strip()
            return f"EA118机房{building}{total}".strip()
        return str(record.get("title") or record.get("record_id") or "").strip()

    def _source_candidate_building_codes(self, record: dict[str, Any], work_type: str) -> list[str]:
        if work_type == WORK_TYPE_REPAIR:
            return self._repair_record_building_codes(record)
        if work_type == WORK_TYPE_MAINTENANCE:
            return self._building_codes_from_value((record.get("display_fields") or {}).get("楼栋"))
        return []

    def _source_candidate_status(self, record: dict[str, Any], work_type: str) -> str:
        if work_type == WORK_TYPE_REPAIR:
            return self._repair_source_status(record)
        if work_type == WORK_TYPE_MAINTENANCE:
            return self._maintenance_status_value(record)
        return ""

    def _source_candidate_time_range(self, record: dict[str, Any], work_type: str) -> tuple[str, str]:
        fields = record.get("display_fields") or {}
        if work_type == WORK_TYPE_REPAIR:
            start, end, _ = self._repair_time_range(record)
            return start, end
        if work_type == WORK_TYPE_MAINTENANCE:
            return str(fields.get("计划维护月份") or "").strip(), ""
        return "", ""

    def _source_candidate_title_matches(self, title: str, candidate_title: str, work_type: str) -> bool:
        if self._match_text(candidate_title) == self._match_text(title):
            return True
        return (
            self._canonical_history_notice_title(candidate_title, work_type)
            == self._canonical_history_notice_title(title, work_type)
        )

    def _lookup_notice_source_candidates(
        self,
        *,
        work_type: str,
        scope: str,
        title: str,
        start_time: str = "",
        end_time: str = "",
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        if work_type == WORK_TYPE_CHANGE:
            return self._lookup_change_source_candidates(
                scope=scope,
                title=title,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )
        if work_type not in {WORK_TYPE_MAINTENANCE, WORK_TYPE_REPAIR}:
            return []
        title = str(title or "").strip()
        if not title:
            return []
        query_dates = self._date_keys_from_values(start_time, end_time)
        source_app_token = self.app_token if work_type == WORK_TYPE_MAINTENANCE else REPAIR_SOURCE_APP_TOKEN
        source_table_id = self.table_id if work_type == WORK_TYPE_MAINTENANCE else REPAIR_SOURCE_TABLE_ID
        candidates: list[dict[str, Any]] = []
        for record in self._source_records_for_notice_lookup(work_type):
            source_title = self._source_candidate_title(record, work_type)
            if not self._source_candidate_title_matches(title, source_title, work_type):
                continue
            building_codes = self._source_candidate_building_codes(record, work_type)
            if not self._scope_matches_buildings(scope, building_codes):
                continue
            source_start, source_end = self._source_candidate_time_range(record, work_type)
            source_dates = self._date_keys_from_values(source_start, source_end)
            fields = record.get("display_fields") or {}
            detail_fields = self._target_candidate_detail_fields(fields)
            record_id = str(record.get("record_id") or "").strip()
            if not record_id:
                continue
            candidates.append(
                {
                    "record_id": record_id,
                    "source_record_id": record_id,
                    "source_app_token": source_app_token,
                    "source_table_id": source_table_id,
                    "work_type": work_type,
                    "title": source_title or title,
                    "building": self._building_label_from_codes(building_codes),
                    "building_codes": building_codes,
                    "status": self._source_candidate_status(record, work_type),
                    "start_time": source_start,
                    "end_time": source_end,
                    "date_matched": bool(query_dates and source_dates and (query_dates & source_dates)),
                    "fields": detail_fields,
                    "field_items": [
                        {"label": key, "value": value}
                        for key, value in detail_fields.items()
                    ],
                }
            )
        candidates.sort(
            key=lambda item: (
                0 if item.get("date_matched") else 1,
                0 if not self._target_status_is_finished(item.get("status")) else 1,
                str(item.get("start_time") or ""),
            )
        )
        return candidates[: max(1, int(limit or 30))]

    def _target_record_id_from_work_status(
        self, *, work_type: str, source_record_id: str = "", active_item_id: str = ""
    ) -> str:
        work_type = str(work_type or WORK_TYPE_MAINTENANCE).strip()
        source_record_id = str(source_record_id or "").strip()
        active_item_id = str(active_item_id or "").strip()
        if not source_record_id and not active_item_id:
            return ""
        with self._summary_lock:
            status_items = self._load_work_status_items_locked("ALL")
        fallback = ""
        for item in status_items:
            if str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip() != work_type:
                continue
            if source_record_id and str(item.get("source_record_id") or "").strip() != source_record_id:
                continue
            if active_item_id and str(item.get("active_item_id") or "").strip() != active_item_id:
                continue
            target_record_id = canonical_target_record_id(item)
            if not target_record_id:
                continue
            if not fallback:
                fallback = target_record_id
            if not self._is_completed_work_status_item(item):
                return target_record_id
        return fallback

    def _target_record_id_from_identity_map(
        self,
        *,
        work_type: str,
        active_item_id: str = "",
        source_record_id: str = "",
        target_record_id: str = "",
    ) -> str:
        try:
            identity = self._state_store.resolve_notice_identity(
                work_type=work_type,
                active_item_id=active_item_id,
                source_record_id=source_record_id,
                target_record_id=target_record_id,
            )
        except Exception:
            return ""
        if not isinstance(identity, dict):
            return ""
        return str(identity.get("target_record_id") or "").strip()

    def _target_record_id_from_request_payload(
        self, request_payload: dict[str, Any], *, source_record_id: str = ""
    ) -> str:
        payload = dict(request_payload or {})
        if source_record_id and not str(payload.get("source_record_id") or "").strip():
            payload["source_record_id"] = source_record_id
        return canonical_target_record_id(payload)

    def _target_record_id_from_unique_candidate(
        self,
        *,
        work_type: str,
        scope: str,
        title: str,
        start_time: str = "",
        end_time: str = "",
        action: str = "update",
    ) -> str:
        title = str(title or "").strip()
        if not title:
            return ""
        try:
            result = self.lookup_notice_target_candidates(
                work_type=work_type,
                scope=scope,
                title=title,
                start_time=start_time,
                end_time=end_time,
                action=action,
                limit=10,
            )
        except Exception:
            return ""
        candidates = [
            item
            for item in (result.get("candidates") or [])
            if isinstance(item, dict)
            and str(item.get("target_record_id") or item.get("record_id") or "").strip()
        ]
        if len(candidates) != 1:
            return ""
        return str(
            candidates[0].get("target_record_id") or candidates[0].get("record_id") or ""
        ).strip()

    def _resolve_target_record_id_for_source_update(
        self,
        *,
        work_type: str,
        notice_type: str,
        source_record: dict[str, Any] | None,
    ) -> str:
        if not isinstance(source_record, dict):
            return ""
        source_record_id = str(source_record.get("record_id") or "").strip()
        target_record_id = self._target_record_id_from_identity_map(
            work_type=work_type,
            source_record_id=source_record_id,
        )
        if target_record_id:
            return target_record_id
        target_record_id = self._target_record_id_from_work_status(
            work_type=work_type, source_record_id=source_record_id
        )
        if target_record_id:
            return target_record_id
        if work_type == WORK_TYPE_REPAIR:
            target_record_id = self._repair_target_record_id(source_record)
            if target_record_id:
                return target_record_id
        return self._resolve_target_record_reference(
            work_type=work_type,
            notice_type=notice_type,
            source_record=source_record,
            include_finished=False,
        )

    def _merge_ongoing_items(
        self,
        scope: str,
        ongoing_items: list[dict[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        scope = self._normalize_scope(scope)
        merged: list[dict[str, Any]] = []
        index_by_key: dict[str, int] = {}
        for item in ongoing_items or []:
            if not isinstance(item, dict):
                continue
            if not self._scope_matches_item(scope, item):
                continue
            copied = normalize_notice_identity_payload(copy.deepcopy(item))
            copied.setdefault("work_type", WORK_TYPE_MAINTENANCE)
            copied.setdefault("notice_type", NOTICE_TYPE_MAINTENANCE)
            if self._is_ongoing_hidden(copied):
                continue
            identity_keys = self._ongoing_merge_identity_keys(copied)
            duplicate_indexes = [
                index_by_key[key]
                for key in identity_keys
                if key in index_by_key
            ]
            target_index = next(
                (
                    index
                    for index in sorted(set(duplicate_indexes))
                    if not self._ongoing_identity_conflicts(merged[index], copied)
                ),
                None,
            )
            if target_index is not None:
                merged[target_index] = self._merge_duplicate_ongoing_item(
                    merged[target_index],
                    copied,
                )
                for key in identity_keys | self._ongoing_merge_identity_keys(merged[target_index]):
                    index_by_key[key] = target_index
                continue
            merged.append(copied)
            item_index = len(merged) - 1
            for key in identity_keys:
                index_by_key[key] = item_index
        return merged

    def _ongoing_identity_conflicts(
        self, existing: dict[str, Any], incoming: dict[str, Any]
    ) -> bool:
        existing = normalize_notice_identity_payload(existing or {})
        incoming = normalize_notice_identity_payload(incoming or {})
        existing_work_type = str(
            existing.get("work_type") or existing.get("lan_work_type") or ""
        ).strip()
        incoming_work_type = str(
            incoming.get("work_type") or incoming.get("lan_work_type") or ""
        ).strip()
        if (
            existing_work_type
            and incoming_work_type
            and existing_work_type != incoming_work_type
        ):
            return True
        existing_notice_type = str(existing.get("notice_type") or "").strip()
        incoming_notice_type = str(incoming.get("notice_type") or "").strip()
        if (
            existing_notice_type
            and incoming_notice_type
            and existing_notice_type != incoming_notice_type
        ):
            return True
        existing_active = str(existing.get("active_item_id") or "").strip()
        incoming_active = str(incoming.get("active_item_id") or "").strip()
        if existing_active and incoming_active and existing_active == incoming_active:
            return False
        existing_signature = self._ongoing_exact_duplicate_signature(existing)
        if (
            existing_signature
            and existing_signature == self._ongoing_exact_duplicate_signature(incoming)
        ):
            return False
        existing_target = canonical_target_record_id(existing)
        incoming_target = canonical_target_record_id(incoming)
        if existing_target and incoming_target:
            return existing_target != incoming_target
        existing_source = canonical_source_record_id(existing)
        incoming_source = canonical_source_record_id(incoming)
        if existing_source and incoming_source:
            return existing_source != incoming_source
        if existing_active and incoming_active:
            return existing_active != incoming_active
        return False

    def _ongoing_exact_duplicate_signature(self, item: dict[str, Any]) -> tuple[str, ...]:
        item = normalize_notice_identity_payload(item or {})
        title_key = self._ongoing_business_text_key(
            item.get("title") or item.get("content") or item.get("name") or ""
        )
        if not title_key:
            return ()
        item_fields = item.get("fields") if isinstance(item.get("fields"), dict) else {}
        return (
            str(item.get("work_type") or item.get("lan_work_type") or "").strip(),
            str(item.get("notice_type") or "").strip(),
            title_key,
            self._ongoing_business_text_key(item.get("building") or ""),
            self._ongoing_business_text_key(
                item.get("maintenance_cycle") or item_fields.get("维护周期") or ""
            ),
            self._ongoing_business_time_key(item),
            self._ongoing_business_text_key(item.get("location") or ""),
            self._ongoing_business_text_key(item.get("content") or ""),
            self._ongoing_business_text_key(item.get("reason") or ""),
            self._ongoing_business_text_key(item.get("impact") or ""),
        )

    def _ongoing_merge_identity_keys(self, item: dict[str, Any]) -> set[str]:
        item = normalize_notice_identity_payload(item)
        work_type = str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        keys = set(self._work_status_identity_keys(item))
        fallback_key = str(
            item.get("work_fallback_key") or item.get("fallback_key") or ""
        ).strip()
        if not fallback_key:
            fallback_key = self._work_status_fallback_key(
                title=str(item.get("title") or item.get("content") or ""),
                building=str(item.get("building") or ""),
                plan_month=str(item.get("plan_month") or ""),
                reason=str(item.get("reason") or ""),
            )
        if fallback_key:
            keys.add(f"{work_type}:fallback:{fallback_key}")
        title_key = re.sub(r"\s+", "", str(item.get("title") or item.get("content") or ""))
        building_key = re.sub(r"\s+", "", str(item.get("building") or ""))
        start_key = re.sub(r"\s+", "", str(item.get("start_time") or item.get("time_str") or ""))
        end_key = re.sub(r"\s+", "", str(item.get("end_time") or ""))
        reason_key = re.sub(r"\s+", "", str(item.get("reason") or ""))
        if title_key and (start_key or end_key):
            keys.add(
                f"{work_type}:title-time:{building_key}:{title_key}:{start_key}:{end_key}:reason:{reason_key}"
            )
        keys.update(self._ongoing_business_merge_keys(item, work_type=work_type))
        return {key for key in keys if key}

    def _ongoing_business_merge_keys(
        self, item: dict[str, Any], *, work_type: str = ""
    ) -> set[str]:
        """Best-effort merge keys for the same notice coming from different projections.

        ID-based keys remain authoritative. These keys only bridge cases where the
        same notice is present once from a source/work-status projection and once
        from the Qt active projection, but one side has not yet received the other
        side's ID. Reason is kept in the key when present so same-name maintenance
        items with different reasons do not collapse into one row.
        """

        work_type = str(work_type or item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        title_key = self._ongoing_business_text_key(
            item.get("title") or item.get("content") or item.get("name") or ""
        )
        if not title_key:
            return set()
        building_key = self._ongoing_business_text_key(item.get("building") or "")
        reason_key = self._ongoing_business_text_key(item.get("reason") or "")
        cycle_key = ""
        if work_type == WORK_TYPE_MAINTENANCE:
            item_fields = item.get("fields") if isinstance(item.get("fields"), dict) else {}
            cycle_key = self._ongoing_business_text_key(
                item.get("maintenance_cycle") or item_fields.get("维护周期") or ""
            )
        time_key = self._ongoing_business_time_key(item)
        keys: set[str] = set()
        if building_key and reason_key:
            keys.add(f"{work_type}:business:title-building-reason:{building_key}:{title_key}:{cycle_key}:{reason_key}")
        if building_key and time_key and reason_key:
            keys.add(f"{work_type}:business:title-time-reason:{building_key}:{title_key}:{cycle_key}:{time_key}:{reason_key}")
        elif building_key and time_key:
            keys.add(f"{work_type}:business:title-time:{building_key}:{title_key}:{cycle_key}:{time_key}")
        return keys

    @staticmethod
    def _ongoing_business_text_key(value: Any) -> str:
        return re.sub(
            r"[\s,，;；:：。.【】（）()《》<>\"'“”‘’\-－_/\\]+",
            "",
            str(value or ""),
        ).strip().lower()

    @staticmethod
    def _ongoing_business_time_key(item: dict[str, Any]) -> str:
        parts = [
            str(item.get("start_time") or ""),
            str(item.get("time_str") or ""),
            str(item.get("time") or ""),
            str(item.get("end_time") or ""),
        ]
        digits = re.findall(r"\d+", "".join(parts))
        return "".join(chunk.zfill(2) if len(chunk) <= 2 else chunk for chunk in digits)

    @staticmethod
    def _ongoing_item_score(item: dict[str, Any]) -> int:
        item = normalize_notice_identity_payload(item)
        score = 0
        for field in (
            "target_record_id",
            "active_item_id",
            "source_record_id",
            "title",
            "building",
            "specialty",
            "maintenance_cycle",
            "start_time",
            "end_time",
            "location",
            "content",
            "reason",
            "impact",
            "progress",
        ):
            if str(item.get(field) or "").strip():
                score += 1
        if item.get("extra_images"):
            score += 1
        return score

    def _merge_duplicate_ongoing_item(
        self,
        existing: dict[str, Any],
        incoming: dict[str, Any],
    ) -> dict[str, Any]:
        existing = normalize_notice_identity_payload(copy.deepcopy(existing))
        incoming = normalize_notice_identity_payload(copy.deepcopy(incoming))
        if self._ongoing_item_score(incoming) > self._ongoing_item_score(existing):
            base, supplement = incoming, existing
        else:
            base, supplement = existing, incoming
        for key, value in supplement.items():
            if key not in base or base.get(key) in (None, "", [], {}):
                base[key] = copy.deepcopy(value)
        return base

    def _source_record_matches_filters(
        self, record: dict[str, Any], *, month: str = "", specialty: str = ""
    ) -> bool:
        work_type = self._record_work_type(record)
        if work_type == WORK_TYPE_MAINTENANCE:
            if not self._maintenance_status_allows_workbench(record):
                return False
        elif work_type == WORK_TYPE_CHANGE:
            if not self._change_progress_allows_workbench(record):
                return False
        elif work_type == WORK_TYPE_REPAIR:
            if not self._is_valid_repair_record(record):
                return False
            if self._repair_has_ended(record):
                return False
            if self._repair_source_status(record) not in WORKBENCH_SOURCE_STATUSES:
                return False
        if not self._source_record_matches_month_window(record, month):
            return False
        specialty = str(specialty or "").strip()
        if not specialty:
            return True
        fields = record.get("display_fields") or {}
        if work_type == WORK_TYPE_MAINTENANCE:
            return self._clean_source_text(fields.get("专业类别")) == specialty
        if work_type == WORK_TYPE_CHANGE:
            return self._change_specialty(record) == specialty
        if work_type == WORK_TYPE_REPAIR:
            return self._repair_specialty(record) == specialty
        return True

    def _workbench_records_from_memory(
        self, *, month: str = "", specialty: str = "", scope: str = "ALL"
    ) -> list[dict[str, Any]]:
        maintenance_records = self._filter_records(
            month=month, specialty=specialty, scope=scope
        )
        change_records = self._filter_change_records(
            month=month,
            specialty=specialty,
            scope=scope,
        )
        repair_records = self._filter_repair_records(
            month=month, specialty=specialty, scope=scope
        )
        return maintenance_records + change_records + repair_records

    def _workbench_records(
        self, *, month: str = "", specialty: str = "", scope: str = "ALL"
    ) -> list[dict[str, Any]]:
        snapshot_records = self._source_snapshot_records(scope)
        if snapshot_records is not None:
            return self._apply_work_type_overrides([
                record
                for record in snapshot_records
                if self._source_record_matches_filters(
                    record, month=month, specialty=specialty
                )
            ])
        return self._apply_work_type_overrides(
            self._workbench_records_from_memory(
                month=month, specialty=specialty, scope=scope
            )
        )

    def _maintenance_options_for_records(self, records: list[dict[str, Any]]) -> list[dict[str, str]]:
        options = []
        for record in records:
            if self._record_work_type(record) != WORK_TYPE_MAINTENANCE:
                continue
            fields = record["display_fields"]
            options.append(
                {
                    "record_id": record["record_id"],
                    "label": fields.get("维护总项") or record["record_id"],
                    "sub_label": " | ".join(
                        filter(
                            None,
                            [
                                fields.get("维护编号", ""),
                                fields.get("维护项目", ""),
                            ],
                        )
                    ),
                }
            )
        return options

    def assert_generated_drafts_allowed(
        self, drafts: list[dict[str, Any]], *, scope: str = "ALL"
    ) -> None:
        self.ensure_snapshot_loaded()
        scope = self._normalize_scope(scope)
        for draft in drafts or []:
            record_id = str((draft or {}).get("record_id") or "").strip()
            if not record_id:
                raise PortalError("存在缺少 record_id 的待生成记录。")
            record = self._find_record_by_id(record_id)
            fields = record.get("display_fields") or {}
            if not self._scope_matches_building(scope, fields.get("楼栋")):
                raise PortalError(
                    f"当前账号无权生成 {record_id} 对应楼栋的通告。"
                )

    def assert_generated_items_allowed(
        self, items: list[dict[str, Any]], *, scope: str = "ALL"
    ) -> None:
        self.ensure_snapshot_loaded()
        scope = self._normalize_scope(scope)
        for item in items or []:
            record_id = str((item or {}).get("record_id") or "").strip()
            if not record_id:
                raise PortalError("存在缺少 record_id 的待发送通告。")
            record = self._find_record_by_id(record_id)
            fields = record.get("display_fields") or {}
            if not self._scope_matches_building(scope, fields.get("楼栋")):
                raise PortalError(
                    f"当前账号无权发送 {record_id} 对应楼栋的通告。"
                )

    def get_bootstrap(
        self,
        *,
        scope: str = "ALL",
        ongoing_items: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        self.ensure_snapshot_loaded()
        default_month = RECENT_MONTH_FILTER_LABEL
        scope = self._normalize_scope(scope)
        merged_ongoing = self._merge_ongoing_items(scope, ongoing_items or [])
        filtered_records = self._workbench_records(month=default_month, scope=scope)
        linked_zhihang_ids = self._linked_zhihang_record_ids(merged_ongoing)
        zhihang_records = self._filter_zhihang_change_records(
            month=default_month, scope=scope, exclude_record_ids=linked_zhihang_ids
        )
        daily_summary = self.get_daily_summary(
            scope=scope, ongoing_items=merged_ongoing
        )
        merged_ongoing = self._annotate_undo_items(merged_ongoing, scope=scope)
        daily_summary["items"] = self._annotate_undo_items(
            daily_summary.get("items") or [], scope=scope
        )
        summary_by_record = self._work_status_by_records(
            filtered_records,
            scope=scope,
            daily_items=daily_summary.get("items") or [],
        )
        record_type_counts = self._work_type_counts(filtered_records)
        ongoing_type_counts = self._work_type_counts(merged_ongoing)
        return {
            "app_token": self.app_token,
            "table_id": self.table_id,
            "scope": scope,
            "scope_label": self._scope_label(scope),
            "scope_options": SCOPE_OPTIONS,
            "default_work_type": (
                WORK_TYPE_CHANGE if scope == "CAMPUS" else WORK_TYPE_MAINTENANCE
            ),
            "last_loaded_at": self._last_loaded_at,
            "payload_version": self._payload_version(
                scope=scope,
                records=filtered_records,
                ongoing_items=merged_ongoing,
                daily_summary=daily_summary,
            ),
            "source_snapshot_ready": self._source_snapshot_exists(scope),
            "source_cache_ttl_seconds": self._source_cache_ttl_seconds(),
            "warnings": self._current_load_warnings(),
            "maintenance_status": DEFAULT_MAINTENANCE_STATUS,
            "field_order": [meta.field_name for meta in self._field_meta_list],
            "fields": [
                {
                    "field_name": meta.field_name,
                    "ui_type": meta.ui_type,
                    "type": meta.field_type,
                    "is_primary": meta.is_primary,
                    "options": meta.option_names,
                    "has_formula": meta.has_formula,
                }
                for meta in self._field_meta_list
            ],
            "filters": {
                "default_month": default_month,
                "months": self._sorted_unique_option_names("计划维护月份"),
                "specialties": self._sorted_unique_work_specialties(),
                "buildings": self._sorted_unique_option_names("楼栋"),
            },
            "records": [
                self._serialize_record(record, summary_by_record)
                for record in filtered_records
            ],
            "zhihang_change_records": [
                self._serialize_zhihang_change_record(record)
                for record in zhihang_records
            ],
            "ongoing": merged_ongoing,
            "daily_summary": daily_summary,
            "record_type_counts": record_type_counts,
            "ongoing_type_counts": ongoing_type_counts,
            "maintenance_options": self._maintenance_options_for_records(filtered_records),
            "defaults": {
                "impact": DEFAULT_IMPACT_TEXT,
                "progress": DEFAULT_PROGRESS_TEXT,
            },
        }

    def query_records(
        self,
        *,
        month: str = "",
        specialty: str = "",
        building: str = "",
        scope: str = "ALL",
        ongoing_items: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        self.ensure_snapshot_loaded()
        scope = self._normalize_scope(scope)
        merged_ongoing = self._merge_ongoing_items(scope, ongoing_items or [])
        filtered_records = self._workbench_records(
            month=month, specialty=specialty, scope=scope
        )
        linked_zhihang_ids = self._linked_zhihang_record_ids(merged_ongoing)
        zhihang_records = self._filter_zhihang_change_records(
            month=month, scope=scope, exclude_record_ids=linked_zhihang_ids
        )
        if building:
            filtered_records = [
                record
                for record in filtered_records
                if str((record.get("display_fields") or {}).get("楼栋") or "") == building
            ]
        daily_summary = self.get_daily_summary(
            scope=scope, ongoing_items=merged_ongoing
        )
        merged_ongoing = self._annotate_undo_items(merged_ongoing, scope=scope)
        daily_summary["items"] = self._annotate_undo_items(
            daily_summary.get("items") or [], scope=scope
        )
        summary_by_record = self._work_status_by_records(
            filtered_records,
            scope=scope,
            daily_items=daily_summary.get("items") or [],
        )
        record_type_counts = self._work_type_counts(filtered_records)
        ongoing_type_counts = self._work_type_counts(merged_ongoing)
        return {
            "maintenance_status": DEFAULT_MAINTENANCE_STATUS,
            "scope": scope,
            "scope_label": self._scope_label(scope),
            "default_work_type": (
                WORK_TYPE_CHANGE if scope == "CAMPUS" else WORK_TYPE_MAINTENANCE
            ),
            "last_loaded_at": self._last_loaded_at,
            "payload_version": self._payload_version(
                scope=scope,
                records=filtered_records,
                ongoing_items=merged_ongoing,
                daily_summary=daily_summary,
            ),
            "source_snapshot_ready": self._source_snapshot_exists(scope),
            "source_cache_ttl_seconds": self._source_cache_ttl_seconds(),
            "warnings": self._current_load_warnings(),
            "records": [
                self._serialize_record(record, summary_by_record)
                for record in filtered_records
            ],
            "zhihang_change_records": [
                self._serialize_zhihang_change_record(record)
                for record in zhihang_records
            ],
            "ongoing": merged_ongoing,
            "daily_summary": daily_summary,
            "record_type_counts": record_type_counts,
            "ongoing_type_counts": ongoing_type_counts,
            "maintenance_options": self._maintenance_options_for_records(filtered_records),
            "count": len(filtered_records),
        }

    def _engineer_mop_settings(self) -> dict[str, str]:
        settings = self._state_store.get_settings() or {}
        def first_text(*values: Any) -> str:
            for value in values:
                text = str(value or "").strip()
                if text and text.lower() not in {"none", "null", "undefined"}:
                    return text
            return ""

        app_token = first_text(
            settings.get("mop_app_token"),
            getattr(config, "mop_app_token", ""),
            MOP_SOURCE_APP_TOKEN,
        )
        table_id = first_text(
            settings.get("mop_table_id"),
            getattr(config, "mop_table_id", ""),
            MOP_SOURCE_TABLE_ID,
        )
        view_id = first_text(
            settings.get("mop_view_id"),
            getattr(config, "mop_view_id", ""),
            MOP_SOURCE_VIEW_ID,
        )
        return {
            "app_token": app_token,
            "table_id": table_id,
            "view_id": view_id,
            "title_field": first_text(settings.get("mop_title_field"), MOP_TITLE_FIELD_NAME),
            "attachment_field": first_text(settings.get("mop_attachment_field"), MOP_ATTACHMENT_FIELD_NAME),
        }

    def _engineer_notice_key(self, item: dict[str, Any]) -> str:
        work_type = str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        target_record_id = canonical_target_record_id(item)
        source_record_id = str(item.get("source_record_id") or "").strip()
        active_item_id = str(item.get("active_item_id") or "").strip()
        if target_record_id:
            return f"{work_type}:target:{target_record_id}"
        if source_record_id:
            return f"{work_type}:source:{source_record_id}"
        if active_item_id:
            return f"{work_type}:active:{active_item_id}"
        seed = json.dumps(
            [
                work_type,
                str(item.get("notice_type") or ""),
                str(item.get("title") or item.get("content") or ""),
                str(item.get("building") or ""),
                str(item.get("started_at") or item.get("start_time") or ""),
                str(item.get("ended_at") or item.get("end_time") or ""),
                str(item.get("reason") or ""),
            ],
            ensure_ascii=False,
            sort_keys=True,
        )
        return f"{work_type}:generated:{hashlib.sha1(seed.encode('utf-8')).hexdigest()}"

    @staticmethod
    def _normalize_engineer_mop_template_text(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = re.sub(r"【[^】]*】", "", text)
        text = re.sub(r"^(EA118|南通基地|机房)+", "", text, flags=re.IGNORECASE)
        text = re.sub(r"(维保通告|维护通告|通告)$", "", text)
        text = re.sub(r"[（）()\[\]【】《》<>，,。；;：:\s_\-~至/]+", "", text)
        return text.lower()

    def _engineer_mop_template_key(
        self,
        item: dict[str, Any],
        *,
        title: str = "",
        building: str = "",
        maintenance_total: str = "",
        maintenance_cycle: str = "",
    ) -> str:
        work_type = str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        if work_type != WORK_TYPE_MAINTENANCE:
            return ""
        fields = item.get("display_fields") if isinstance(item.get("display_fields"), dict) else {}
        building_value = (
            building
            or item.get("building")
            or fields.get("楼栋")
            or fields.get("变更楼栋")
            or ""
        )
        codes = item.get("building_codes") if isinstance(item.get("building_codes"), list) else []
        if not codes:
            codes = self._building_codes_from_value(building_value)
        building_key = codes[0] if len(codes) == 1 else self._normalize_scope(building_value)
        if building_key == "ALL":
            building_key = ""
        total_value = (
            maintenance_total
            or item.get("maintenance_total")
            or item.get("memory_name")
            or fields.get("维护总项")
            or title
            or item.get("title")
            or item.get("content")
            or ""
        )
        cycle_value = (
            maintenance_cycle
            or item.get("maintenance_cycle")
            or fields.get("维护周期")
            or ""
        )
        normalized_total = self._normalize_engineer_mop_template_text(total_value)
        normalized_cycle = self._normalize_engineer_mop_template_text(cycle_value) or "/"
        if not building_key or not normalized_total:
            return ""
        seed = json.dumps(
            ["maintenance", building_key, normalized_total, normalized_cycle],
            ensure_ascii=False,
            sort_keys=True,
        )
        return f"mop-template:{hashlib.sha1(seed.encode('utf-8')).hexdigest()}"

    def _serialize_engineer_notice(self, item: dict[str, Any], *, status: str = "") -> dict[str, Any]:
        work_type = str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        fields = item.get("display_fields") if isinstance(item.get("display_fields"), dict) else {}
        title = str(item.get("title") or item.get("content") or item.get("name") or "").strip()
        start_time = str(item.get("started_at") or item.get("start_time") or item.get("time") or "").strip()
        end_time = str(item.get("ended_at") or item.get("end_time") or "").strip()
        notice_status = status or str(item.get("status") or "").strip() or ("已结束" if end_time else "进行中")
        building = str(item.get("building") or fields.get("楼栋") or "")
        maintenance_total = str(
            item.get("maintenance_total")
            or item.get("memory_name")
            or fields.get("维护总项")
            or ""
        ).strip()
        maintenance_cycle = str(item.get("maintenance_cycle") or fields.get("维护周期") or "").strip()
        mop_template_key = self._engineer_mop_template_key(
            item,
            title=title,
            building=building,
            maintenance_total=maintenance_total,
            maintenance_cycle=maintenance_cycle,
        )
        return {
            "notice_key": self._engineer_notice_key(item),
            "mop_template_key": mop_template_key,
            "work_type": work_type,
            "notice_type": str(item.get("notice_type") or NOTICE_TYPE_MAINTENANCE),
            "title": title,
            "status": notice_status,
            "building": building,
            "building_codes": item.get("building_codes") if isinstance(item.get("building_codes"), list) else [],
            "specialty": str(item.get("specialty") or ""),
            "maintenance_total": maintenance_total,
            "maintenance_cycle": maintenance_cycle,
            "source_record_id": str(item.get("source_record_id") or ""),
            "target_record_id": canonical_target_record_id(item),
            "active_item_id": str(item.get("active_item_id") or ""),
            "start_time": start_time,
            "end_time": end_time,
            "location": str(item.get("location") or ""),
            "content": str(item.get("content") or ""),
            "reason": str(item.get("reason") or ""),
            "progress": str(item.get("progress") or ""),
            "updated_at": str(item.get("ended_at") or item.get("last_updated_at") or item.get("updated_at") or item.get("started_at") or ""),
        }

    def _mop_source_flag(self, value: Any) -> bool:
        if isinstance(value, (list, tuple, set)):
            return any(self._mop_source_flag(item) for item in value)
        if isinstance(value, dict):
            for key in ("checked", "value", "text", "name"):
                if key in value and self._mop_source_flag(value.get(key)):
                    return True
            return bool(value)
        return self._truthy_flag(value)

    def _serialize_engineer_source_maintenance_notice(
        self, record: dict[str, Any]
    ) -> dict[str, Any] | None:
        fields = record.get("display_fields") if isinstance(record.get("display_fields"), dict) else {}
        maintenance_total = str(fields.get("维护总项") or "").strip()
        building = str(fields.get("楼栋") or "").strip()
        if not maintenance_total and not building:
            return None
        maintenance_cycle = str(fields.get("维护周期") or "").strip()
        title = f"{building}{maintenance_total}{('-' + maintenance_cycle) if maintenance_cycle else ''}".strip()
        signed_attachments = self._extract_mop_attachments(fields, MOP_SIGNED_ATTACHMENT_FIELD)
        item = {
            "work_type": WORK_TYPE_MAINTENANCE,
            "notice_type": NOTICE_TYPE_MAINTENANCE,
            "title": title or maintenance_total or str(record.get("record_id") or ""),
            "building": building,
            "building_codes": self._building_codes_from_value(building),
            "specialty": str(fields.get("专业类别") or fields.get("专业") or "").strip(),
            "maintenance_total": maintenance_total,
            "maintenance_cycle": maintenance_cycle,
            "source_record_id": str(record.get("record_id") or ""),
            "display_fields": fields,
            "location": str(fields.get("位置") or building or "").strip(),
            "content": str(fields.get("内容") or fields.get("维护内容") or maintenance_total).strip(),
            "reason": str(fields.get("原因") or fields.get("维护原因") or "").strip(),
            "progress": str(fields.get("进度") or fields.get("维护进度") or "").strip(),
            "updated_at": str(fields.get("更新时间") or fields.get("计划维护月份") or ""),
        }
        notice = self._serialize_engineer_notice(
            item,
            status=self._maintenance_status_value(record) or DEFAULT_MAINTENANCE_STATUS,
        )
        notice.update(
            {
                "mop_uploaded": bool(signed_attachments),
                "mop_attachment_count": len(signed_attachments),
                "mop_engineer_confirmed": self._mop_source_flag(fields.get(MOP_ENGINEER_CONFIRM_FIELD)),
                "mop_supervisor_confirmed": self._mop_source_flag(fields.get(MOP_SUPERVISOR_CONFIRM_FIELD)),
                "mop_source_record": True,
            }
        )
        return notice

    @staticmethod
    def _merge_engineer_mop_notice(
        existing: dict[str, Any], incoming: dict[str, Any]
    ) -> dict[str, Any]:
        merged = dict(existing)
        for key, value in incoming.items():
            if value in (None, "", [], {}):
                continue
            if key in {
                "status",
                "target_record_id",
                "active_item_id",
                "start_time",
                "end_time",
                "location",
                "content",
                "reason",
                "progress",
                "updated_at",
            }:
                merged[key] = value
            elif key not in merged or merged.get(key) in (None, "", [], {}):
                merged[key] = value
        for key in (
            "mop_uploaded",
            "mop_engineer_confirmed",
            "mop_supervisor_confirmed",
            "mop_source_record",
        ):
            merged[key] = bool(existing.get(key) or incoming.get(key))
        merged["mop_attachment_count"] = max(
            int(existing.get("mop_attachment_count") or 0),
            int(incoming.get("mop_attachment_count") or 0),
        )
        return merged

    @staticmethod
    def _engineer_notice_is_ended_status(status: Any) -> bool:
        text = str(status or "").strip()
        if not text or any(token in text for token in ("未结束", "未完成", "未闭环")):
            return False
        return any(token in text for token in ("已结束", "正常结束", "维修完成", "已完成", "闭环"))

    @classmethod
    def _engineer_maintenance_notice_sort_key(cls, item: dict[str, Any]) -> tuple[Any, ...]:
        bound = bool(item.get("mop_binding"))
        uploaded = bool(item.get("mop_uploaded"))
        ended = cls._engineer_notice_is_ended_status(item.get("status"))
        needs_action = not bound or not uploaded
        if not bound and not uploaded:
            priority = 0
        elif not bound:
            priority = 1
        elif not uploaded:
            priority = 2
        else:
            priority = 3
        return (
            0 if ended or needs_action else 1,
            priority,
            0 if ended else 1,
            str(item.get("building") or ""),
            str(item.get("maintenance_cycle") or ""),
            str(item.get("title") or ""),
            str(item.get("updated_at") or ""),
        )

    def _engineer_month_maintenance_notices(
        self, *, scope: str, ongoing_items: list[dict[str, Any]] | None
    ) -> list[dict[str, Any]]:
        merged_ongoing = self._merge_ongoing_items(scope, ongoing_items or [])
        notices_by_key: dict[str, dict[str, Any]] = {}

        def upsert_notice(notice: dict[str, Any]) -> None:
            if not notice:
                return
            match_key = str(notice.get("notice_key") or "").strip()
            source_record_id = str(notice.get("source_record_id") or "").strip()
            template_key = str(notice.get("mop_template_key") or "").strip()
            if source_record_id:
                for key, existing in notices_by_key.items():
                    if str(existing.get("source_record_id") or "").strip() == source_record_id:
                        match_key = key
                        break
            if match_key not in notices_by_key and template_key:
                for key, existing in notices_by_key.items():
                    if str(existing.get("mop_template_key") or "").strip() == template_key:
                        match_key = key
                        break
            if match_key in notices_by_key:
                notices_by_key[match_key] = self._merge_engineer_mop_notice(
                    notices_by_key[match_key],
                    notice,
                )
            else:
                notices_by_key[match_key or uuid.uuid4().hex] = notice

        current_month = self._current_month_label()
        source_records = [
            record
            for record in list(self._records or [])
            if self._record_work_type(record) == WORK_TYPE_MAINTENANCE
            and self._maintenance_record_matches_month_window(record, current_month)
            and self._scope_matches_building(
                scope,
                (record.get("display_fields") or {}).get("楼栋"),
            )
        ]
        if not source_records:
            source_records = [
                record
                for record in (self._source_snapshot_records(scope) or [])
                if self._record_work_type(record) == WORK_TYPE_MAINTENANCE
                and self._maintenance_record_matches_month_window(record, current_month)
            ]
        for record in source_records:
            notice = self._serialize_engineer_source_maintenance_notice(record)
            if notice:
                upsert_notice(notice)

        for item in merged_ongoing:
            if str(item.get("work_type") or WORK_TYPE_MAINTENANCE) != WORK_TYPE_MAINTENANCE:
                continue
            notice = self._serialize_engineer_notice(item, status="进行中")
            upsert_notice(notice)
        daily = self.get_daily_summary(scope=scope, ongoing_items=merged_ongoing)
        for item in daily.get("items") or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("work_type") or WORK_TYPE_MAINTENANCE) != WORK_TYPE_MAINTENANCE:
                continue
            notice = self._serialize_engineer_notice(item)
            existing = notices_by_key.get(notice["notice_key"])
            if existing and not self._engineer_notice_is_ended_status(existing.get("status")):
                continue
            upsert_notice(notice)
        notices = list(notices_by_key.values())
        notices.sort(key=self._engineer_maintenance_notice_sort_key)
        return notices

    @staticmethod
    def _mop_field_text(fields: dict[str, Any], names: list[str]) -> str:
        for name in names:
            value = fields.get(name)
            if isinstance(value, list):
                text = "、".join(str(item.get("text") or item.get("name") or item) if isinstance(item, dict) else str(item) for item in value)
            else:
                text = str(value or "")
            text = text.strip()
            if text:
                return text
        return ""

    @staticmethod
    def _extract_mop_attachments(fields: dict[str, Any], preferred_field: str = "") -> list[dict[str, Any]]:
        candidates: list[Any] = []
        if preferred_field and preferred_field in fields:
            candidates.append(fields.get(preferred_field))
        else:
            candidates.extend(fields.values())
        attachments: list[dict[str, Any]] = []
        seen: set[str] = set()
        for value in candidates:
            values = value if isinstance(value, list) else [value]
            for item in values:
                if not isinstance(item, dict):
                    continue
                token = str(
                    item.get("file_token")
                    or item.get("token")
                    or item.get("tmp_url")
                    or item.get("url")
                    or item.get("download_url")
                    or ""
                ).strip()
                name = str(item.get("name") or item.get("file_name") or item.get("filename") or "").strip()
                if not token and not name:
                    continue
                dedupe_key = token or name
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                attachments.append(
                    {
                        "file_token": str(item.get("file_token") or item.get("token") or "").strip(),
                        "name": name or "MOP表格",
                        "size": item.get("size") or item.get("file_size") or "",
                        "mime_type": str(item.get("mime_type") or item.get("type") or "").strip(),
                        "url": str(item.get("download_url") or item.get("url") or item.get("tmp_url") or "").strip(),
                    }
                )
        return attachments

    @classmethod
    def _attachment_latest_publish_time(cls, fields: dict[str, Any]) -> str:
        return cls._mop_field_text(fields, list(ATTACHMENT_LATEST_FIELD_NAMES))

    @staticmethod
    def _attachment_cache_signature(
        *,
        category: str,
        app_token: str,
        table_id: str,
        record_id: str,
        attachment: dict[str, Any],
        latest_publish_time: str = "",
    ) -> dict[str, str]:
        return {
            "category": str(category or ""),
            "app_token": str(app_token or ""),
            "table_id": str(table_id or ""),
            "record_id": str(record_id or ""),
            "file_token": str(attachment.get("file_token") or ""),
            "url": str(attachment.get("url") or ""),
            "name": str(attachment.get("name") or ""),
            "size": str(attachment.get("size") or ""),
            "mime_type": str(attachment.get("mime_type") or ""),
            "latest_publish_time": str(latest_publish_time or attachment.get("latest_publish_time") or ""),
        }

    def _attachment_with_cache_context(
        self,
        attachment: dict[str, Any],
        *,
        category: str,
        app_token: str,
        table_id: str,
        record_id: str,
        latest_publish_time: str = "",
    ) -> dict[str, Any]:
        item = dict(attachment or {})
        item["cache_category"] = category
        item["app_token"] = app_token
        item["table_id"] = table_id
        item["record_id"] = record_id
        item["latest_publish_time"] = latest_publish_time
        return item

    @classmethod
    def _extract_signature_attachments(cls, fields: dict[str, Any]) -> list[dict[str, Any]]:
        attachments = cls._extract_mop_attachments(fields, SIGNATURE_ATTACHMENT_FIELD)
        usable: list[dict[str, Any]] = []
        for item in attachments:
            if not isinstance(item, dict):
                continue
            token = str(item.get("file_token") or "").strip()
            name = str(item.get("name") or "").strip().lower()
            mime_type = str(item.get("mime_type") or "").strip().lower()
            size_text = str(item.get("size") or "").strip()
            if size_text in {"0", "0.0"}:
                continue
            image_like = (
                mime_type.startswith("image/")
                or name.endswith((".png", ".jpg", ".jpeg", ".webp", ".bmp"))
            )
            if token and image_like:
                usable.append(item)
        return usable

    @staticmethod
    def _signature_person_inactive(value: Any) -> bool:
        """Return True when the signature person is marked resigned/transferred."""

        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, (list, tuple, set)):
            return any(MaintenancePortalService._signature_person_inactive(item) for item in value)
        if isinstance(value, dict):
            if any(
                MaintenancePortalService._signature_person_inactive(value.get(key))
                for key in ("checked", "value", "text", "name")
                if key in value
            ):
                return True
            return bool(value)
        text = str(value or "").strip().lower()
        if not text:
            return False
        if text in {"false", "0", "否", "未勾选", "未选", "no", "none", "null"}:
            return False
        return True

    def _load_engineer_mop_candidates(self, *, force: bool = False) -> tuple[list[dict[str, Any]], list[str], dict[str, str]]:
        settings = self._engineer_mop_settings()
        if not settings["app_token"] or not settings["table_id"]:
            return [], ["未配置 MOP 多维表 app_token/table_id，请先在 SQLite settings 中配置 mop_app_token 和 mop_table_id。"], settings
        signature = json.dumps(settings, ensure_ascii=False, sort_keys=True)
        now = time.time()
        with self._engineer_mop_cache_lock:
            cached = self._engineer_mop_cache
            if (
                not force
                and cached
                and cached.get("signature") == signature
                and now - float(cached.get("loaded_ts") or 0.0) < MOP_CANDIDATE_CACHE_TTL_SECONDS
            ):
                return (
                    copy.deepcopy(cached.get("candidates") or []),
                    list(cached.get("warnings") or []),
                    dict(cached.get("settings") or settings),
                )
        warnings: list[str] = []
        metas, meta_by_name = self._load_table_fields(
            app_token=settings["app_token"],
            table_id=settings["table_id"],
        )
        records: list[dict[str, Any]] = []
        page_token = ""
        while True:
            params = {"page_size": 500}
            if settings.get("view_id"):
                params["view_id"] = settings["view_id"]
            if page_token:
                params["page_token"] = page_token
            payload = self._request_json(
                "records",
                params=params,
                app_token=settings["app_token"],
                table_id=settings["table_id"],
            )
            data = payload.get("data", {})
            for item in data.get("items") or []:
                fields = item.get("fields") if isinstance(item.get("fields"), dict) else {}
                records.append(
                    {
                        "record_id": str(item.get("record_id") or ""),
                        "display_fields": fields,
                    }
                )
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "").strip()
            if not page_token:
                break
        candidates: list[dict[str, Any]] = []
        title_names = [
            settings["title_field"],
            "文件名",
            "MOP名称",
            "MOP表格",
            "名称",
            "标题",
            "维护总项",
        ]
        for record in records:
            fields = record.get("display_fields") if isinstance(record.get("display_fields"), dict) else {}
            record_id = str(record.get("record_id") or "")
            latest_publish_time = self._attachment_latest_publish_time(fields)
            attachments = [
                self._attachment_with_cache_context(
                    attachment,
                    category="mop",
                    app_token=settings["app_token"],
                    table_id=settings["table_id"],
                    record_id=record_id,
                    latest_publish_time=latest_publish_time,
                )
                for attachment in self._extract_mop_attachments(fields, settings["attachment_field"])
            ]
            title = self._mop_field_text(fields, title_names) or str(record.get("record_id") or "MOP表格")
            candidates.append(
                {
                    "record_id": record_id,
                    "title": title,
                    "fields": fields,
                    "attachments": attachments,
                    "attachment_count": len(attachments),
                    "latest_publish_time": latest_publish_time,
                    "app_token": settings["app_token"],
                    "table_id": settings["table_id"],
                    "view_id": settings.get("view_id", ""),
                    "file_no": self._mop_field_text(fields, ["文件编号"]),
                    "specialty": self._mop_field_text(fields, ["专业"]),
                    "maintenance_type": self._mop_field_text(fields, ["维护类型"]),
                    "version": self._mop_field_text(fields, ["版本号"]),
                    "file_status": self._mop_field_text(fields, ["文件状态"]),
                }
            )
        if metas and settings["attachment_field"] not in {meta.field_name for meta in metas}:
            warnings.append(f"MOP附件字段「{settings['attachment_field']}」未在表中找到，已自动扫描所有附件字段。")
        with self._engineer_mop_cache_lock:
            self._engineer_mop_cache = {
                "signature": signature,
                "loaded_ts": now,
                "candidates": copy.deepcopy(candidates),
                "warnings": list(warnings),
                "settings": dict(settings),
            }
        return candidates, warnings, settings

    def engineer_mop_bootstrap(
        self, *, scope: str = "ALL", ongoing_items: list[dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        scope = self._normalize_scope(scope)
        notices = self._engineer_month_maintenance_notices(
            scope=scope,
            ongoing_items=ongoing_items,
        )
        mop_candidates, warnings, settings = self._load_engineer_mop_candidates()
        self._maybe_start_daily_attachment_cache_refresh()
        bindings = self._state_store.list_mop_notice_bindings(
            scope=scope,
            notice_keys=[item["notice_key"] for item in notices],
            template_keys=[str(item.get("mop_template_key") or "") for item in notices],
        )
        bindings_by_notice: dict[str, dict[str, Any]] = {}
        for item in bindings:
            notice_key = str(item.get("notice_key") or "")
            if notice_key and notice_key not in bindings_by_notice:
                bindings_by_notice[notice_key] = item
        bindings_by_template: dict[str, dict[str, Any]] = {}
        for binding in bindings:
            template_key = str(binding.get("template_key") or "").strip()
            if template_key and template_key not in bindings_by_template:
                bindings_by_template[template_key] = binding
        for notice in notices:
            binding = bindings_by_notice.get(notice["notice_key"])
            template_key = str(notice.get("mop_template_key") or "").strip()
            if binding and template_key and not str(binding.get("template_key") or "").strip() and template_key not in bindings_by_template:
                try:
                    backfill_payload = dict(binding.get("payload") or {})
                    backfill_payload.update(
                        {
                            "scope": binding.get("scope") or scope,
                            "notice_key": notice["notice_key"],
                            "template_key": template_key,
                            "notice_title": notice.get("title") or binding.get("notice_title") or "",
                            "notice_status": notice.get("status") or binding.get("notice_status") or "",
                            "building": notice.get("building") or "",
                            "maintenance_total": notice.get("maintenance_total") or "",
                            "maintenance_cycle": notice.get("maintenance_cycle") or "",
                            "source_record_id": notice.get("source_record_id") or binding.get("source_record_id") or "",
                            "target_record_id": notice.get("target_record_id") or binding.get("target_record_id") or "",
                            "active_item_id": notice.get("active_item_id") or binding.get("active_item_id") or "",
                            "mop_app_token": binding.get("mop_app_token") or "",
                            "mop_table_id": binding.get("mop_table_id") or "",
                            "mop_record_id": binding.get("mop_record_id") or "",
                            "mop_title": binding.get("mop_title") or "",
                            "mop_attachment_token": binding.get("mop_attachment_token") or "",
                            "mop_attachment_name": binding.get("mop_attachment_name") or "",
                            "selected_sheet": binding.get("selected_sheet") or "",
                            "updated_by": binding.get("updated_by") or "template-backfill",
                        }
                    )
                    backfilled = self._state_store.upsert_mop_notice_binding(backfill_payload)
                    binding = backfilled
                    bindings_by_template[template_key] = backfilled
                except Exception as exc:
                    warnings.append(f"MOP绑定模板补齐失败: {exc}")
            if not binding:
                template_binding = bindings_by_template.get(template_key)
                if template_binding:
                    binding = dict(template_binding)
                    binding["inherited"] = True
            notice["mop_binding"] = binding or None
        notices.sort(key=self._engineer_maintenance_notice_sort_key)
        return {
            "scope": scope,
            "scope_label": self._scope_label(scope),
            "date": dt.date.today().isoformat(),
            "notices": notices,
            "mop_candidates": mop_candidates,
            "bindings": bindings,
            "mop_settings": {
                "configured": bool(settings.get("app_token") and settings.get("table_id")),
                "title_field": settings.get("title_field", ""),
                "attachment_field": settings.get("attachment_field", ""),
            },
            "warnings": warnings,
        }

    def bind_engineer_mop_notice(
        self, *, payload: dict[str, Any], updated_by: str = ""
    ) -> dict[str, Any]:
        payload = dict(payload or {})
        if not str(payload.get("template_key") or payload.get("mop_template_key") or "").strip():
            payload["template_key"] = self._engineer_mop_template_key(
                {
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "title": payload.get("notice_title") or payload.get("title") or "",
                    "building": payload.get("building") or "",
                    "maintenance_total": payload.get("maintenance_total") or "",
                    "maintenance_cycle": payload.get("maintenance_cycle") or "",
                },
                title=str(payload.get("notice_title") or payload.get("title") or ""),
                building=str(payload.get("building") or ""),
                maintenance_total=str(payload.get("maintenance_total") or ""),
                maintenance_cycle=str(payload.get("maintenance_cycle") or ""),
            )
        payload["updated_by"] = updated_by
        binding = self._state_store.upsert_mop_notice_binding(payload)
        self._touch_state_cache_version()
        return {"binding": binding}

    @staticmethod
    def _lark_response_is_token_error(response: Any) -> bool:
        code = int(getattr(response, "code", 0) or 0)
        msg = str(getattr(response, "msg", "") or "").lower()
        return code in TOKEN_ERROR_CODES or "token" in msg or "access_token" in msg

    @classmethod
    def _signature_open_id_from_any(cls, value: Any) -> str:
        """Return a Feishu open_id from user fields, text fields, or nested payloads."""

        if isinstance(value, dict):
            candidates = (
                value.get("open_id"),
                value.get("openId"),
                value.get("openid"),
                value.get("user_id"),
                value.get("userId"),
                value.get("id"),
            )
            for candidate in candidates:
                text = str(candidate or "").strip()
                if text.startswith("ou_"):
                    return text
            for nested in value.values():
                found = cls._signature_open_id_from_any(nested)
                if found:
                    return found
            return ""
        if isinstance(value, list):
            for item in value:
                found = cls._signature_open_id_from_any(item)
                if found:
                    return found
            return ""
        match = re.search(r"ou_[A-Za-z0-9_-]+", str(value or ""))
        return match.group(0) if match else ""

    @classmethod
    def _signature_open_id_from_user(cls, value: dict[str, Any]) -> str:
        """Return a real Feishu open_id from a bitable user field payload."""

        return cls._signature_open_id_from_any(value)

    @classmethod
    def _signature_user_info(cls, value: Any) -> dict[str, str]:
        if isinstance(value, list) and value:
            first = value[0]
            if isinstance(first, dict):
                return {
                    "open_id": cls._signature_open_id_from_user(first),
                    "name": str(first.get("name") or first.get("en_name") or "").strip(),
                    "email": str(first.get("email") or "").strip(),
                }
        if isinstance(value, dict):
            return {
                "open_id": cls._signature_open_id_from_user(value),
                "name": str(value.get("name") or value.get("en_name") or "").strip(),
                "email": str(value.get("email") or "").strip(),
            }
        return {"open_id": "", "name": "", "email": ""}

    @staticmethod
    def _decode_signature_png(signature_png: str) -> bytes:
        text = str(signature_png or "").strip()
        if not text:
            raise PortalError("签名图片为空，请先手写签名。")
        if "," in text and text.lower().startswith("data:image/"):
            header, text = text.split(",", 1)
            if "png" not in header.lower():
                raise PortalError("签名图片必须是 PNG 格式。")
        try:
            raw = base64.b64decode(text, validate=True)
        except Exception as exc:
            raise PortalError("签名图片格式无效，请重新签名。") from exc
        if len(raw) < 64:
            raise PortalError("签名图片内容过小，请重新签名。")
        if len(raw) > 2 * 1024 * 1024:
            raise PortalError("签名图片超过 2MB，请清空后重新签名。")
        if not raw.startswith(b"\x89PNG\r\n\x1a\n"):
            raise PortalError("签名图片必须是 PNG 格式。")
        return raw

    @staticmethod
    def _signature_attachment_version(attachment: dict[str, Any]) -> str:
        seed = "|".join(
            str(attachment.get(key) or "")
            for key in ("file_token", "url", "name", "size", "tmp_url")
        )
        return hashlib.sha1(seed.encode("utf-8", errors="ignore")).hexdigest()[:12]

    @staticmethod
    def _signature_preview_url(*, record_id: str, signature_version: str, link_token: str = "") -> str:
        record_id = str(record_id or "").strip()
        signature_version = str(signature_version or "").strip()
        if not record_id or not signature_version:
            return ""
        url = f"/api/signatures/image?record_id={quote(record_id, safe='')}&v={quote(signature_version, safe='')}"
        link_token = str(link_token or "").strip()
        if link_token:
            url += f"&token={quote(link_token, safe='')}"
        return url

    @classmethod
    def _transparent_signature_png(cls, signature_bytes: bytes) -> bytes:
        """Normalize a canvas/image signature to black ink on transparent background."""

        try:
            from PIL import Image, ImageChops
        except Exception as exc:  # pragma: no cover - dependency bootstrap should provide Pillow.
            raise PortalError("缺少 Pillow 依赖，无法处理透明签名图片。") from exc

        try:
            image = Image.open(io.BytesIO(signature_bytes)).convert("RGBA")
        except Exception as exc:
            raise PortalError("签名图片无法读取，请重新签名。") from exc
        gray = image.convert("L")
        ink_alpha = gray.point(lambda p: 0 if p >= 246 else min(255, max(0, (246 - int(p)) * 5)))
        source_alpha = image.getchannel("A")
        alpha = ImageChops.multiply(ink_alpha, source_alpha)
        transparent = Image.new("RGBA", image.size, (0, 0, 0, 0))
        transparent.putalpha(alpha)
        bbox = alpha.point(lambda p: 255 if p > 8 else 0).getbbox()
        if not bbox:
            raise PortalError("签名图片未识别到有效笔迹，请重新签名。")
        left = max(0, bbox[0] - 8)
        top = max(0, bbox[1] - 8)
        right = min(image.size[0], bbox[2] + 8)
        bottom = min(image.size[1], bbox[3] + 8)
        transparent = transparent.crop((left, top, right, bottom))
        output = io.BytesIO()
        transparent.save(output, format="PNG", optimize=True)
        return output.getvalue()

    def _upload_signature_image(self, *, signature_bytes: bytes, file_name: str) -> str:
        token = str(ensure_feishu_token() or config.user_token or "").strip()
        if not token:
            raise PortalError("未配置有效的飞书 user_token，无法上传签名。")
        temp_file_path = ""
        try:
            with tempfile.NamedTemporaryFile(
                delete=False,
                prefix="clipflow_signature_",
                suffix=".png",
            ) as tmp:
                tmp.write(signature_bytes)
                temp_file_path = tmp.name

            def attempt_upload(upload_token: str) -> Any:
                with open(temp_file_path, "rb") as file_obj:
                    client = (
                        lark.Client.builder()
                        .enable_set_token(True)
                        .log_level(lark.LogLevel.ERROR)
                        .build()
                    )
                    request = (
                        UploadAllMediaRequest.builder()
                        .request_body(
                            UploadAllMediaRequestBody.builder()
                            .file_name(file_name)
                            .parent_type("bitable_image")
                            .parent_node(SIGNATURE_APP_TOKEN)
                            .size(str(len(signature_bytes)))
                            .file(file_obj)
                            .build()
                        )
                        .build()
                    )
                    option = lark.RequestOption.builder().user_access_token(upload_token).build()
                    return client.drive.v1.media.upload_all(request, option)

            response = attempt_upload(token)
            if not response.success() and self._lark_response_is_token_error(response):
                token = str(refresh_feishu_token() or config.user_token or "").strip()
                response = attempt_upload(token)
            if not response.success():
                raise PortalError(
                    f"签名图片上传失败: {response.code} - {response.msg}"
                )
            file_token = str(getattr(response.data, "file_token", "") or "").strip()
            if not file_token:
                raise PortalError("签名图片上传成功但未返回 file_token。")
            return file_token
        finally:
            if temp_file_path:
                with suppress(Exception):
                    os.remove(temp_file_path)

    def _upload_bitable_file(
        self,
        *,
        file_path: str | Path,
        file_name: str = "",
        app_token: str = DEFAULT_APP_TOKEN,
    ) -> str:
        token = str(ensure_feishu_token() or config.user_token or "").strip()
        if not token:
            raise PortalError("未配置有效的飞书 user_token，无法上传 MOP 文件。")
        path = Path(str(file_path or "")).resolve()
        if not path.is_file():
            raise PortalError("待上传的已签名 MOP 文件不存在。")
        upload_name = str(file_name or path.name).strip() or path.name
        size = path.stat().st_size
        if size <= 0:
            raise PortalError("待上传的已签名 MOP 文件为空。")

        def attempt_upload(upload_token: str) -> Any:
            with open(path, "rb") as file_obj:
                client = (
                    lark.Client.builder()
                    .enable_set_token(True)
                    .log_level(lark.LogLevel.ERROR)
                    .build()
                )
                request = (
                    UploadAllMediaRequest.builder()
                    .request_body(
                        UploadAllMediaRequestBody.builder()
                        .file_name(upload_name)
                        .parent_type("bitable_file")
                        .parent_node(str(app_token or DEFAULT_APP_TOKEN))
                        .size(str(size))
                        .file(file_obj)
                        .build()
                    )
                    .build()
                )
                option = lark.RequestOption.builder().user_access_token(upload_token).build()
                return client.drive.v1.media.upload_all(request, option)

        response = attempt_upload(token)
        if not response.success() and self._lark_response_is_token_error(response):
            token = str(refresh_feishu_token() or config.user_token or "").strip()
            response = attempt_upload(token)
        if not response.success():
            raise PortalError(f"MOP 文件上传失败: {response.code} - {response.msg}")
        file_token = str(getattr(response.data, "file_token", "") or "").strip()
        if not file_token:
            raise PortalError("MOP 文件上传成功但未返回 file_token。")
        return file_token

    def _load_signature_people(self, *, force: bool = False) -> list[dict[str, Any]]:
        now = time.time()
        with self._signature_people_cache_lock:
            cached = self._signature_people_cache
            if (
                not force
                and cached
                and now - float(cached.get("loaded_ts") or 0.0)
                < SIGNATURE_PEOPLE_CACHE_TTL_SECONDS
            ):
                return copy.deepcopy(cached.get("people") or [])

        people: list[dict[str, Any]] = []
        page_token = ""
        while True:
            params = {"page_size": 500}
            if SIGNATURE_VIEW_ID:
                params["view_id"] = SIGNATURE_VIEW_ID
            if page_token:
                params["page_token"] = page_token
            payload = self._request_json(
                "records",
                params=params,
                app_token=SIGNATURE_APP_TOKEN,
                table_id=SIGNATURE_TABLE_ID,
            )
            data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
            for item in data.get("items") or []:
                fields = item.get("fields") if isinstance(item.get("fields"), dict) else {}
                if self._signature_person_inactive(fields.get(SIGNATURE_INACTIVE_FIELD)):
                    continue
                user_info = self._signature_user_info(fields.get(SIGNATURE_USER_FIELD))
                open_id = user_info.get("open_id", "") or self._signature_open_id_from_any(
                    fields.get("openid")
                    or fields.get("OpenID")
                    or fields.get("open_id")
                    or fields.get("飞书OpenID")
                    or fields.get("飞书 openid")
                    or fields.get("飞书openid")
                    or fields.get("人员openid")
                )
                name = (
                    self._mop_field_text(fields, [SIGNATURE_NAME_FIELD])
                    or user_info.get("name")
                    or str(item.get("record_id") or "")
                )
                record_id = str(item.get("record_id") or "")
                latest_publish_time = self._attachment_latest_publish_time(fields)
                attachments = [
                    self._attachment_with_cache_context(
                        attachment,
                        category="signature",
                        app_token=SIGNATURE_APP_TOKEN,
                        table_id=SIGNATURE_TABLE_ID,
                        record_id=record_id,
                        latest_publish_time=latest_publish_time,
                    )
                    for attachment in self._extract_signature_attachments(fields)
                ]
                first_signature = attachments[0] if attachments else None
                signature_version = (
                    self._signature_attachment_version(first_signature)
                    if isinstance(first_signature, dict) and first_signature
                    else ""
                )
                building = self._mop_field_text(fields, ["楼栋", "机楼/专业"])
                has_signature = bool(signature_version)
                person = {
                    "record_id": record_id,
                    "name": name,
                    "open_id": open_id,
                    "email": user_info.get("email", ""),
                    "employee_no": self._mop_field_text(fields, ["员工工号", "工号"]),
                    "building": building,
                    "scope_text": building,
                    "position": self._mop_field_text(fields, ["岗位"]),
                    "team": self._mop_field_text(fields, ["班组"]),
                    "shift": self._mop_field_text(fields, ["班次"]),
                    "has_signature": has_signature,
                    "signature_count": len(attachments) if has_signature else 0,
                    "signature_version": signature_version,
                    "latest_publish_time": latest_publish_time,
                    "signature_preview_url": (
                        self._signature_preview_url(
                            record_id=record_id,
                            signature_version=signature_version,
                        )
                        if record_id and has_signature
                        else ""
                    ),
                    "raw_fields": fields,
                }
                people.append(person)
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "").strip()
            if not page_token:
                break

        people.sort(
            key=lambda item: (
                0 if item.get("name") else 1,
                str(item.get("building") or ""),
                str(item.get("name") or ""),
            )
        )
        with self._signature_people_cache_lock:
            self._signature_people_cache = {
                "loaded_ts": now,
                "people": copy.deepcopy(people),
            }
        return people

    def signature_people(
        self,
        *,
        scope: str = "",
        query: str = "",
        record_id: str = "",
        link_token: str = "",
        limit: int = 80,
        refresh: bool = False,
    ) -> dict[str, Any]:
        scope = self._normalize_scope(scope or "ALL")
        query_text = re.sub(r"\s+", "", str(query or "")).lower()
        record_id = str(record_id or "").strip()
        people = self._load_signature_people(force=bool(refresh or record_id))
        self._maybe_start_daily_attachment_cache_refresh()

        def matches_query(person: dict[str, Any]) -> bool:
            if record_id and str(person.get("record_id") or "") != record_id:
                return False
            if not query_text:
                return True
            haystack = re.sub(
                r"\s+",
                "",
                "|".join(
                    str(person.get(key) or "")
                    for key in (
                        "record_id",
                        "name",
                        "open_id",
                        "email",
                        "employee_no",
                        "building",
                        "position",
                        "team",
                        "shift",
                    )
                ),
            ).lower()
            return query_text in haystack

        filtered = [
            {
                key: value
                for key, value in person.items()
                if key != "raw_fields"
            }
            for person in people
            if matches_query(person)
        ]
        limited = filtered[: max(1, min(500, int(limit or 80)))]
        link_token = str(link_token or "").strip()
        if link_token and record_id:
            for person in limited:
                if str(person.get("record_id") or "") != record_id:
                    continue
                signature_version = str(person.get("signature_version") or "")
                if signature_version:
                    person["signature_preview_url"] = self._signature_preview_url(
                        record_id=record_id,
                        signature_version=signature_version,
                        link_token=link_token,
                    )
        return {
            "people": limited,
            "count": len(filtered),
            "returned": len(limited),
            "scope": scope,
            "loaded_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    @staticmethod
    def _external_signature_preview_url(*, record_id: str, signature_version: str) -> str:
        record_id = str(record_id or "").strip()
        signature_version = str(signature_version or "").strip()
        if not record_id or not signature_version:
            return ""
        return (
            f"/api/signatures/temporary/image?record_id={quote(record_id, safe='')}"
            f"&v={quote(signature_version, safe='')}"
        )

    def _load_external_signature_people(self, *, force: bool = False) -> list[dict[str, Any]]:
        now = time.time()
        with self._external_signature_people_cache_lock:
            cached = self._external_signature_people_cache
            if (
                not force
                and cached
                and now - float(cached.get("loaded_ts") or 0.0)
                < SIGNATURE_PEOPLE_CACHE_TTL_SECONDS
            ):
                return copy.deepcopy(cached.get("people") or [])

        people: list[dict[str, Any]] = []
        page_token = ""
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            payload = self._request_json(
                "records",
                params=params,
                app_token=SIGNATURE_APP_TOKEN,
                table_id=TEMP_SIGNATURE_TABLE_ID,
            )
            data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
            for item in data.get("items") or []:
                fields = item.get("fields") if isinstance(item.get("fields"), dict) else {}
                record_id = str(item.get("record_id") or "").strip()
                if not record_id:
                    continue
                latest_publish_time = self._attachment_latest_publish_time(fields)
                attachments = [
                    self._attachment_with_cache_context(
                        attachment,
                        category="temporary_signature",
                        app_token=SIGNATURE_APP_TOKEN,
                        table_id=TEMP_SIGNATURE_TABLE_ID,
                        record_id=record_id,
                        latest_publish_time=latest_publish_time,
                    )
                    for attachment in self._extract_signature_attachments(fields)
                ]
                if not attachments:
                    continue
                first_signature = attachments[0]
                signature_version = self._signature_attachment_version(first_signature)
                name = (
                    self._mop_field_text(fields, [TEMP_SIGNATURE_NAME_FIELD, "姓名", "名称"])
                    or record_id
                )
                building = self._mop_field_text(fields, [TEMP_SIGNATURE_BUILDING_FIELD, "楼栋"])
                specialty = self._mop_field_text(fields, [TEMP_SIGNATURE_SPECIALTY_FIELD, "专业"])
                people.append(
                    {
                        "source": "external",
                        "record_id": record_id,
                        "name": name,
                        "display_name": name,
                        "building": building,
                        "scope_text": building,
                        "specialty": specialty,
                        "employee_no": self._mop_field_text(fields, [TEMP_SIGNATURE_EMPLOYEE_NO_FIELD, "工号"]),
                        "certificate": self._mop_field_text(fields, [TEMP_SIGNATURE_CERT_FIELD, "持证"]),
                        "has_signature": True,
                        "signature_count": len(attachments),
                        "signature_version": signature_version,
                        "latest_publish_time": latest_publish_time,
                        "signature_preview_url": self._external_signature_preview_url(
                            record_id=record_id,
                            signature_version=signature_version,
                        ),
                        "raw_fields": fields,
                    }
                )
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "").strip()
            if not page_token:
                break

        people.sort(
            key=lambda item: (
                str(item.get("building") or ""),
                str(item.get("specialty") or ""),
                str(item.get("name") or ""),
            )
        )
        with self._external_signature_people_cache_lock:
            self._external_signature_people_cache = {
                "loaded_ts": now,
                "people": copy.deepcopy(people),
            }
        return people

    def temporary_signature_people(
        self,
        *,
        scope: str = "ALL",
        query: str = "",
        limit: int = 80,
        refresh: bool = False,
    ) -> dict[str, Any]:
        scope = self._normalize_scope(scope or "ALL")
        query_text = re.sub(r"\s+", "", str(query or "")).lower()
        people = self._load_external_signature_people(force=bool(refresh))

        def matches_query(person: dict[str, Any]) -> bool:
            if not query_text:
                return True
            haystack = re.sub(
                r"\s+",
                "",
                "|".join(
                    str(person.get(key) or "")
                    for key in (
                        "record_id",
                        "name",
                        "display_name",
                        "building",
                        "specialty",
                        "employee_no",
                        "certificate",
                    )
                ),
            ).lower()
            return query_text in haystack

        filtered = [
            {key: value for key, value in person.items() if key != "raw_fields"}
            for person in people
            if matches_query(person)
        ]
        limited = filtered[: max(1, min(500, int(limit or 80)))]
        return {
            "people": limited,
            "count": len(filtered),
            "returned": len(limited),
            "scope": scope,
            "loaded_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def create_signature_link_token(self, *, record_id: str, created_by: str = "") -> dict[str, Any]:
        return self._state_store.create_signature_link_token(
            record_id=record_id,
            created_by=created_by,
            ttl_seconds=SIGNATURE_LINK_TOKEN_TTL_SECONDS,
            payload={"purpose": "signature_link"},
        )

    def validate_signature_link_token(self, *, record_id: str, token: str) -> bool:
        return self._state_store.validate_signature_link_token(
            record_id=record_id,
            token=token,
        )

    def mark_signature_link_token_used(self, *, record_id: str, token: str) -> None:
        self._state_store.mark_signature_link_token_used(
            record_id=record_id,
            token=token,
        )

    def save_signature_for_person(
        self,
        *,
        record_id: str,
        signature_png: str,
        signer_name: str = "",
        link_token: str = "",
    ) -> dict[str, Any]:
        record_id = str(record_id or "").strip()
        if not record_id:
            raise PortalError("请选择要保存签名的人员。")
        signature_bytes = self._transparent_signature_png(
            self._decode_signature_png(signature_png)
        )
        people = self._load_signature_people(force=True)
        person = next(
            (item for item in people if str(item.get("record_id") or "") == record_id),
            None,
        )
        if not person:
            raise PortalError("签名人员记录不存在或无权访问。")
        safe_name = self._safe_mop_path_part(
            signer_name or str(person.get("name") or "signature"),
            "signature",
        )
        file_name = f"{safe_name}_{dt.datetime.now().strftime('%Y%m%d%H%M%S')}.png"
        file_token = self._upload_signature_image(
            signature_bytes=signature_bytes,
            file_name=file_name,
        )
        self._patch_record_fields(
            app_token=SIGNATURE_APP_TOKEN,
            table_id=SIGNATURE_TABLE_ID,
            record_id=record_id,
            fields={SIGNATURE_ATTACHMENT_FIELD: [{"file_token": file_token}]},
        )
        with self._signature_people_cache_lock:
            self._signature_people_cache = None
        signature_version = hashlib.sha1(file_token.encode("utf-8")).hexdigest()[:12]
        return {
            "record_id": record_id,
            "name": str(person.get("name") or signer_name or ""),
            "file_token": file_token,
            "signature_version": signature_version,
            "signature_preview_url": self._signature_preview_url(
                record_id=record_id,
                signature_version=signature_version,
                link_token=link_token,
            ),
            "has_signature": True,
            "saved_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def signature_image_bytes(self, *, record_id: str) -> tuple[bytes, str]:
        record_id = str(record_id or "").strip()
        if not record_id:
            raise PortalError("缺少签名人员记录。")
        people = self._load_signature_people(force=False)
        person = next(
            (item for item in people if str(item.get("record_id") or "") == record_id),
            None,
        )
        if not person:
            people = self._load_signature_people(force=True)
            person = next(
                (item for item in people if str(item.get("record_id") or "") == record_id),
                None,
            )
        if not person:
            raise PortalError("签名人员记录不存在。")
        fields = person.get("raw_fields") if isinstance(person.get("raw_fields"), dict) else {}
        attachments = [
            self._attachment_with_cache_context(
                attachment,
                category="signature",
                app_token=SIGNATURE_APP_TOKEN,
                table_id=SIGNATURE_TABLE_ID,
                record_id=record_id,
                latest_publish_time=str(person.get("latest_publish_time") or self._attachment_latest_publish_time(fields)),
            )
            for attachment in self._extract_signature_attachments(fields)
        ]
        if not attachments:
            raise PortalError("该人员还没有可用签名。")
        content, _content_type = self._download_mop_attachment(attachments[0])
        return self._transparent_signature_png(content), "image/png"

    def external_signature_image_bytes(self, *, record_id: str) -> tuple[bytes, str]:
        record_id = str(record_id or "").strip()
        if not record_id:
            raise PortalError("缺少其他人员签名记录。")
        people = self._load_external_signature_people(force=False)
        person = next(
            (item for item in people if str(item.get("record_id") or "") == record_id),
            None,
        )
        if not person:
            people = self._load_external_signature_people(force=True)
            person = next(
                (item for item in people if str(item.get("record_id") or "") == record_id),
                None,
            )
        if not person:
            raise PortalError("其他人员签名记录不存在。")
        fields = person.get("raw_fields") if isinstance(person.get("raw_fields"), dict) else {}
        attachments = [
            self._attachment_with_cache_context(
                attachment,
                category="temporary_signature",
                app_token=SIGNATURE_APP_TOKEN,
                table_id=TEMP_SIGNATURE_TABLE_ID,
                record_id=record_id,
                latest_publish_time=str(person.get("latest_publish_time") or self._attachment_latest_publish_time(fields)),
            )
            for attachment in self._extract_signature_attachments(fields)
        ]
        if not attachments:
            raise PortalError("该其他人员还没有可用签名。")
        content, _content_type = self._download_mop_attachment(attachments[0])
        return self._transparent_signature_png(content), "image/png"

    @staticmethod
    def _url_host_for_display(host: str) -> str:
        host = str(host or "").strip()
        if not host:
            return ""
        if ":" in host and not host.startswith("[") and not host.endswith("]"):
            return f"[{host}]"
        return host

    def _signature_public_base_url(
        self,
        *,
        scope: str = "",
        request_base_url: str = "",
    ) -> str:
        """Build the public portal URL used in signature links.

        A browser-origin URL from the current page has the highest priority,
        because phones must open the same LAN address the operator is using.
        Legacy callers without request context still fall back to configured
        handover/portal host values.
        """

        base_text = str(request_base_url or "").strip().rstrip("/")
        if base_text and "://" not in base_text:
            base_text = f"http://{base_text}"
        parsed_base = urlparse(base_text) if base_text else None
        if (
            parsed_base
            and parsed_base.scheme in {"http", "https"}
            and parsed_base.netloc
            and str(parsed_base.hostname or "").strip() not in {"0.0.0.0", "::"}
        ):
            return f"{parsed_base.scheme}://{parsed_base.netloc}"
        scheme = (
            parsed_base.scheme
            if parsed_base and parsed_base.scheme in {"http", "https"}
            else "http"
        )
        port = (
            parsed_base.port
            if parsed_base and parsed_base.port
            else int(getattr(config, "lan_template_portal_port", 18766) or 18766)
        )
        host = parsed_base.hostname if parsed_base and parsed_base.hostname else ""

        normalized_scope = self._normalize_scope(scope or "ALL")
        handover_links = self.get_handover_links().get("links") or {}
        handover_url = ""
        if isinstance(handover_links, dict):
            handover_url = str(handover_links.get(normalized_scope) or "").strip()
            if not handover_url and normalized_scope != "ALL":
                handover_url = str(handover_links.get("ALL") or "").strip()
        try:
            parsed_handover = urlparse(handover_url)
            handover_host = str(parsed_handover.hostname or "").strip()
            if handover_host and handover_host not in {"0.0.0.0", "::"}:
                host = handover_host
        except Exception:
            pass

        configured_host = str(getattr(config, "lan_template_portal_host", "") or "").strip()
        if (
            (not host or host in {"127.0.0.1", "localhost", "0.0.0.0", "::"})
            and configured_host
            and configured_host not in {"0.0.0.0", "::"}
        ):
            host = configured_host
        if not host or host in {"0.0.0.0", "::"}:
            host = "127.0.0.1"
        return f"{scheme}://{self._url_host_for_display(host)}:{port}"

    def _signature_person_for_link(self, *, record_id: str) -> dict[str, Any]:
        record_id = str(record_id or "").strip()
        if not record_id:
            raise PortalError("请选择要发送签名链接的人员。")
        people = self._load_signature_people(force=False)
        person = next(
            (item for item in people if str(item.get("record_id") or "") == record_id),
            None,
        )
        if not person:
            people = self._load_signature_people(force=True)
            person = next(
                (item for item in people if str(item.get("record_id") or "") == record_id),
                None,
            )
        if not person:
            raise PortalError("签名人员记录不存在。")
        open_id = str(person.get("open_id") or "").strip()
        if not open_id:
            raise PortalError("该人员记录缺少员工姓名 openid，无法发送签名链接。")
        return {
            key: value
            for key, value in person.items()
            if key != "raw_fields"
        }

    def build_signature_link_message(
        self,
        *,
        record_id: str,
        signer_name: str = "",
        scope: str = "",
        request_base_url: str = "",
        created_by: str = "",
    ) -> dict[str, Any]:
        person = self._signature_person_for_link(record_id=record_id)
        base_url = self._signature_public_base_url(
            scope=scope,
            request_base_url=request_base_url,
        )
        token_data = self.create_signature_link_token(
            record_id=record_id,
            created_by=created_by,
        )
        link_token = str(token_data.get("token") or "").strip()
        link_url = (
            f"{base_url}/signature?record_id={quote(str(record_id or '').strip(), safe='')}"
            f"&token={quote(link_token, safe='')}"
        )
        name = str(signer_name or person.get("name") or "签名人员").strip()
        text = "\n".join(
            [
                "【线上签名】请完成 MOP 手写签名",
                "",
                f"签名人员：{name}",
                f"签名链接：{link_url}",
                "",
                "请用手机打开链接，在页面手写签名并保存。",
            ]
        )
        return {
            "record_id": str(record_id or "").strip(),
            "person": person,
            "open_id": str(person.get("open_id") or "").strip(),
            "link_url": link_url,
            "expires_at": token_data.get("expires_at"),
            "text": text,
        }

    @staticmethod
    def _mop_role_label(role: str) -> str:
        return "维护审核人" if str(role or "") == "auditor" else "维护实施人"

    def _temporary_signature_public_url(
        self,
        *,
        temp_id: str,
        token: str,
        scope: str = "",
        request_base_url: str = "",
    ) -> str:
        base_url = self._signature_public_base_url(
            scope=scope,
            request_base_url=request_base_url,
        )
        return (
            f"{base_url}/signature?temporary_id={quote(str(temp_id or '').strip(), safe='')}"
            f"&token={quote(str(token or '').strip(), safe='')}"
        )

    def _temporary_signature_display_name(
        self,
        *,
        scope: str,
        notice_key: str,
    ) -> str:
        sessions = self._state_store.list_mop_temporary_signature_sessions(
            scope=scope,
            notice_key=notice_key,
            include_expired=True,
        )
        return f"临时人员{len(sessions) + 1}"

    def build_temporary_signature_link_message(
        self,
        *,
        scope: str = "ALL",
        notice_key: str = "",
        role: str = "implementer",
        recipient_open_ids: list[str] | None = None,
        notice_title: str = "",
        specialty: str = "",
        display_name: str = "",
        request_base_url: str = "",
        created_by: str = "",
    ) -> dict[str, Any]:
        scope = self._normalize_scope(scope or "ALL")
        notice_key = str(notice_key or "").strip()
        role = str(role or "implementer").strip()
        if role not in {"implementer", "auditor"}:
            raise PortalError("临时签名角色无效。")
        recipients = [
            str(item or "").strip()
            for item in (recipient_open_ids or [])
            if str(item or "").strip()
        ]
        if not recipients:
            raise PortalError("请先选择维护实施人，再发送其他人员签名链接。")
        display_name = str(display_name or "").strip() or self._temporary_signature_display_name(
            scope=scope,
            notice_key=notice_key,
        )
        session = self._state_store.create_mop_temporary_signature_session(
            scope=scope,
            notice_key=notice_key,
            role=role,
            display_name=display_name,
            recipient_open_ids=recipients,
            created_by=created_by,
            ttl_seconds=SIGNATURE_LINK_TOKEN_TTL_SECONDS,
            payload={
                "notice_title": str(notice_title or ""),
                "specialty": str(specialty or ""),
            },
        )
        link_url = self._temporary_signature_public_url(
            temp_id=str(session.get("temp_id") or ""),
            token=str(session.get("token") or ""),
            scope=scope,
            request_base_url=request_base_url,
        )
        role_label = self._mop_role_label(role)
        text = "\n".join(
            [
                "【线上签名】请现场完成 MOP 其他人员签名",
                "",
                f"签名角色：{role_label}",
                f"临时人员：{display_name}",
                f"维护通告：{notice_title or '未命名维保通告'}",
                f"签名链接：{link_url}",
                "",
                "请用手机打开链接，让现场人员在页面手写签名并保存。",
            ]
        )
        return {
            **{key: value for key, value in session.items() if key != "token"},
            "link_url": link_url,
            "open_ids": recipients,
            "text": text,
            "signature": self._public_temporary_signature_session(
                session,
                link_token=str(session.get("token") or ""),
            ),
        }

    def _public_temporary_signature_session(
        self,
        session: dict[str, Any],
        *,
        link_token: str = "",
    ) -> dict[str, Any]:
        payload = session.get("payload") if isinstance(session.get("payload"), dict) else {}
        file_token = str(session.get("signature_file_token") or "").strip()
        temp_id = str(session.get("temp_id") or "").strip()
        status = str(session.get("status") or "pending").strip() or "pending"
        preview_url = ""
        if file_token and temp_id:
            preview_url = (
                f"/api/signatures/temporary/image?temporary_id={quote(temp_id, safe='')}"
                f"&v={quote(hashlib.sha1(file_token.encode('utf-8')).hexdigest()[:12], safe='')}"
            )
            if link_token:
                preview_url += f"&token={quote(link_token, safe='')}"
        return {
            "source": "temporary",
            "temp_id": temp_id,
            "record_id": str(session.get("temporary_record_id") or ""),
            "role": str(session.get("role") or ""),
            "display_name": str(session.get("display_name") or ""),
            "name": str(session.get("display_name") or ""),
            "status": status,
            "has_signature": bool(file_token and status == "signed"),
            "signature_file_token": file_token,
            "signature_preview_url": preview_url,
            "expires_at": session.get("expires_at"),
            "notice_title": str(payload.get("notice_title") or ""),
            "specialty": str(payload.get("specialty") or ""),
        }

    def temporary_signature_session(
        self,
        *,
        temp_id: str,
        token: str,
    ) -> dict[str, Any]:
        session = self._state_store.get_mop_temporary_signature_session(
            temp_id=temp_id,
            token=token,
            require_valid_token=True,
        )
        if not session:
            raise PortalError("临时签名链接无效或已过期。")
        return self._public_temporary_signature_session(session, link_token=token)

    def list_temporary_signatures(
        self,
        *,
        scope: str = "ALL",
        notice_key: str = "",
    ) -> dict[str, Any]:
        scope = self._normalize_scope(scope or "ALL")
        sessions = self._state_store.list_mop_temporary_signature_sessions(
            scope=scope,
            notice_key=str(notice_key or "").strip(),
        )
        return {
            "items": [
                self._public_temporary_signature_session(session)
                for session in sessions
            ],
            "count": len(sessions),
        }

    def _field_option_write_values(
        self,
        meta_by_name: dict[str, FieldMeta],
        field_name: str,
        values: list[str],
    ) -> list[str]:
        meta = meta_by_name.get(field_name)
        if not meta:
            return []
        available = set(meta.option_names or [])
        result: list[str] = []
        for value in values:
            text = str(value or "").strip()
            if text and text in available and text not in result:
                result.append(text)
        return result

    def save_temporary_signature(
        self,
        *,
        temp_id: str,
        token: str,
        signature_png: str,
    ) -> dict[str, Any]:
        session = self._state_store.get_mop_temporary_signature_session(
            temp_id=temp_id,
            token=token,
            require_valid_token=True,
        )
        if not session:
            raise PortalError("临时签名链接无效或已过期。")
        signature_bytes = self._transparent_signature_png(
            self._decode_signature_png(signature_png)
        )
        display_name = str(session.get("display_name") or "临时人员").strip()
        file_name = f"{self._safe_mop_path_part(display_name, 'temporary')}_{dt.datetime.now().strftime('%Y%m%d%H%M%S')}.png"
        file_token = self._upload_signature_image(
            signature_bytes=signature_bytes,
            file_name=file_name,
        )
        _metas, meta_by_name = self._load_table_fields(
            app_token=SIGNATURE_APP_TOKEN,
            table_id=TEMP_SIGNATURE_TABLE_ID,
        )
        payload = session.get("payload") if isinstance(session.get("payload"), dict) else {}
        building_values = self._field_option_write_values(
            meta_by_name,
            TEMP_SIGNATURE_BUILDING_FIELD,
            [self._scope_label(str(session.get("scope") or "")), str(session.get("scope") or "")],
        )
        specialty_values = self._field_option_write_values(
            meta_by_name,
            TEMP_SIGNATURE_SPECIALTY_FIELD,
            [str(payload.get("specialty") or "")],
        )
        fields: dict[str, Any] = {
            TEMP_SIGNATURE_NAME_FIELD: display_name,
            TEMP_SIGNATURE_ATTACHMENT_FIELD: [{"file_token": file_token}],
        }
        if building_values:
            fields[TEMP_SIGNATURE_BUILDING_FIELD] = building_values
        if specialty_values:
            fields[TEMP_SIGNATURE_SPECIALTY_FIELD] = specialty_values
        existing_record_id = str(session.get("temporary_record_id") or "").strip()
        if existing_record_id:
            self._patch_record_fields(
                app_token=SIGNATURE_APP_TOKEN,
                table_id=TEMP_SIGNATURE_TABLE_ID,
                record_id=existing_record_id,
                fields=fields,
            )
            record_id = existing_record_id
        else:
            created = self._create_record_fields(
                app_token=SIGNATURE_APP_TOKEN,
                table_id=TEMP_SIGNATURE_TABLE_ID,
                fields=fields,
            )
            data = created.get("data") if isinstance(created.get("data"), dict) else {}
            record = data.get("record") if isinstance(data.get("record"), dict) else data
            record_id = str(record.get("record_id") or record.get("id") or "").strip()
            if not record_id:
                raise PortalError("临时签名保存成功但未返回记录 ID。")
        updated = self._state_store.update_mop_temporary_signature_session(
            temp_id=str(session.get("temp_id") or ""),
            status="signed",
            temporary_record_id=record_id,
            signature_file_token=file_token,
            payload_patch={"saved_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
        )
        with self._external_signature_people_cache_lock:
            self._external_signature_people_cache = None
        return self._public_temporary_signature_session(
            updated or {},
            link_token=token,
        )

    def temporary_signature_image_bytes(
        self,
        *,
        temp_id: str,
    ) -> tuple[bytes, str]:
        session = self._state_store.get_mop_temporary_signature_session(temp_id=temp_id)
        if not session:
            raise PortalError("临时签名记录不存在。")
        file_token = str(session.get("signature_file_token") or "").strip()
        if not file_token:
            raise PortalError("临时人员还没有可用签名。")
        content, _content_type = self._download_mop_attachment(
            {"file_token": file_token},
            cache_category="temporary_signature",
            app_token=SIGNATURE_APP_TOKEN,
            table_id=TEMP_SIGNATURE_TABLE_ID,
            record_id=str(session.get("temporary_record_id") or session.get("temp_id") or ""),
            latest_publish_time=str(session.get("updated_at") or ""),
        )
        return self._transparent_signature_png(content), "image/png"

    @staticmethod
    def _column_index(cell_ref: str) -> int:
        letters = "".join(ch for ch in str(cell_ref or "") if ch.isalpha()).upper()
        index = 0
        for ch in letters:
            index = index * 26 + (ord(ch) - 64)
        return max(1, index)

    @classmethod
    def _cell_position(cls, cell_ref: str) -> tuple[int, int] | None:
        match = re.match(r"^\$?([A-Za-z]+)\$?(\d+)$", str(cell_ref or "").strip())
        if not match:
            return None
        col = cls._column_index(match.group(1))
        row = int(match.group(2))
        if row <= 0 or col <= 0:
            return None
        return row, col

    @classmethod
    def _parse_merge_range(cls, ref: str) -> tuple[int, int, int, int] | None:
        parts = str(ref or "").split(":")
        if len(parts) != 2:
            return None
        start = cls._cell_position(parts[0])
        end = cls._cell_position(parts[1])
        if not start or not end:
            return None
        row1, col1 = start
        row2, col2 = end
        return min(row1, row2), min(col1, col2), max(row1, row2), max(col1, col2)

    @staticmethod
    def _column_label(index: int) -> str:
        index = max(1, int(index or 1))
        label = ""
        while index:
            index, remainder = divmod(index - 1, 26)
            label = chr(65 + remainder) + label
        return label

    @classmethod
    def _mop_signature_target_column(cls, worksheet: Any, *, row: int, label_col: int) -> int:
        """Return the column immediately to the right of the signature label cell.

        If the label cell is part of a merged range, the signature goes into the
        first cell after that merged range. This keeps the label text intact and
        places signatures in the user's expected fill area.
        """

        row = max(1, int(row or 1))
        label_col = max(1, int(label_col or 1))
        label_ref = f"{cls._column_label(label_col)}{row}"
        for merged_range in getattr(getattr(worksheet, "merged_cells", None), "ranges", []) or []:
            try:
                if label_ref in merged_range:
                    return int(getattr(merged_range, "max_col", label_col)) + 1
            except Exception:
                continue
        return label_col + 1

    def _attachment_cache_paths(
        self,
        *,
        category: str,
        app_token: str,
        table_id: str,
        record_id: str,
        attachment: dict[str, Any],
    ) -> tuple[Path, Path]:
        name = str(attachment.get("name") or "attachment").strip() or "attachment"
        suffix = Path(name).suffix
        if not suffix:
            mime_type = str(attachment.get("mime_type") or "").lower()
            if "png" in mime_type:
                suffix = ".png"
            elif "jpeg" in mime_type or "jpg" in mime_type:
                suffix = ".jpg"
            elif "webp" in mime_type:
                suffix = ".webp"
            elif "csv" in mime_type:
                suffix = ".csv"
            else:
                suffix = ".xlsx"
        stable_seed = json.dumps(
            [
                str(category or ""),
                str(app_token or ""),
                str(table_id or ""),
                str(record_id or ""),
                str(attachment.get("file_token") or ""),
                str(attachment.get("url") or ""),
                name,
            ],
            ensure_ascii=False,
            sort_keys=True,
        )
        digest = hashlib.sha1(stable_seed.encode("utf-8", errors="ignore")).hexdigest()[:16]
        cache_dir = Path(
            get_data_file_path(
                os.path.join(
                    "attachment_cache",
                    self._safe_mop_path_part(category or "attachment", "attachment"),
                    self._safe_mop_path_part(app_token or "app", "app"),
                    self._safe_mop_path_part(table_id or "table", "table"),
                    self._safe_mop_path_part(record_id or "record", "record"),
                )
            )
        )
        cache_name = f"{self._safe_mop_path_part(Path(name).stem or 'attachment', 'attachment')}_{digest}{suffix}"
        return cache_dir / cache_name, cache_dir / f"{cache_name}.meta.json"

    @staticmethod
    def _attachment_meta_matches(expected: dict[str, str], actual: dict[str, Any]) -> bool:
        for key in ("category", "app_token", "table_id", "record_id", "file_token", "url", "name", "size"):
            if str(actual.get(key) or "") != str(expected.get(key) or ""):
                return False
        expected_time = str(expected.get("latest_publish_time") or "")
        actual_time = str(actual.get("latest_publish_time") or "")
        if expected_time and expected_time != actual_time:
            return False
        return True

    def _read_cached_attachment(
        self,
        *,
        category: str,
        app_token: str,
        table_id: str,
        record_id: str,
        attachment: dict[str, Any],
        latest_publish_time: str = "",
    ) -> tuple[bytes, str] | None:
        expected = self._attachment_cache_signature(
            category=category,
            app_token=app_token,
            table_id=table_id,
            record_id=record_id,
            attachment=attachment,
            latest_publish_time=latest_publish_time,
        )
        file_path, meta_path = self._attachment_cache_paths(
            category=category,
            app_token=app_token,
            table_id=table_id,
            record_id=record_id,
            attachment=attachment,
        )
        with self._attachment_cache_lock:
            if not file_path.is_file() or not meta_path.is_file():
                return None
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                return None
            if not isinstance(meta, dict) or not self._attachment_meta_matches(expected, meta):
                return None
            try:
                return file_path.read_bytes(), str(meta.get("content_type") or attachment.get("mime_type") or "")
            except Exception:
                return None

    def _write_cached_attachment(
        self,
        *,
        category: str,
        app_token: str,
        table_id: str,
        record_id: str,
        attachment: dict[str, Any],
        latest_publish_time: str = "",
        content: bytes,
        content_type: str,
    ) -> None:
        if not content:
            return
        expected = self._attachment_cache_signature(
            category=category,
            app_token=app_token,
            table_id=table_id,
            record_id=record_id,
            attachment=attachment,
            latest_publish_time=latest_publish_time,
        )
        file_path, meta_path = self._attachment_cache_paths(
            category=category,
            app_token=app_token,
            table_id=table_id,
            record_id=record_id,
            attachment=attachment,
        )
        meta = {
            **expected,
            "content_type": str(content_type or attachment.get("mime_type") or ""),
            "cached_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "size_bytes": len(content),
            "file_name": file_path.name,
        }
        with self._attachment_cache_lock:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_bytes(content)
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _mop_checkbox_value(original: Any, state: str) -> str:
        return MaintenancePortalService._mop_choice_value(original, state)

    @classmethod
    def _mop_choice_options(cls, value: Any) -> list[dict[str, Any]]:
        text = cls._mop_plain_text(value)
        if not text:
            return []
        options: list[dict[str, Any]] = []
        seen: set[str] = set()
        prefix_pattern = re.compile(r"([□☐■☑√✔✓])([^□☐■☑√✔✓\[\]；;，,、/]+)")
        for match in prefix_pattern.finditer(text):
            label = match.group(2).strip()
            if not label or label in seen:
                continue
            seen.add(label)
            checked = match.group(1) in {"☑", "■", "√", "✔", "✓"}
            options.append(
                {
                    "key": f"opt{len(options)}",
                    "label": label,
                    "checked": checked,
                    "style": "prefix_checkbox",
                }
            )
        if len(options) >= 2:
            return options

        options = []
        seen.clear()
        bracket_pattern = re.compile(r"([^\[\]□☐■☑√✔✓；;，,、/]{1,24})\[(.*?)\]")
        for match in bracket_pattern.finditer(text):
            label = match.group(1).strip()
            if "：" in label or ":" in label:
                label = re.split(r"[:：]", label)[-1].strip()
            if not label or label in seen:
                continue
            seen.add(label)
            checked = bool(str(match.group(2) or "").strip())
            options.append(
                {
                    "key": f"opt{len(options)}",
                    "label": label,
                    "checked": checked,
                    "style": "suffix_bracket",
                }
            )
        return options if len(options) >= 2 else []

    @classmethod
    def _mop_choice_label_for_state(cls, value: Any, state: str) -> str:
        state_text = str(state or "").strip()
        state_lower = state_text.lower()
        options = cls._mop_choice_options(value)
        if not options:
            if state_lower == "normal":
                return "正常"
            if state_lower == "abnormal":
                return "异常"
            return state_text
        for option in options:
            label = str(option.get("label") or "")
            key = str(option.get("key") or "")
            if state_text and state_text in {label, key}:
                return label
        if state_lower == "normal":
            for option in options:
                label = str(option.get("label") or "")
                if "正常" in label:
                    return label
        if state_lower == "abnormal":
            for option in options:
                label = str(option.get("label") or "")
                if "异常" in label:
                    return label
        checked = next((str(item.get("label") or "") for item in options if item.get("checked")), "")
        return checked or str(options[0].get("label") or "")

    @classmethod
    def _mop_choice_value(cls, original: Any, state: str) -> str:
        raw_text = str(original or "")
        text = raw_text or "□正常 □异常"
        selected_label = cls._mop_choice_label_for_state(text, state)
        options = cls._mop_choice_options(text)
        if not selected_label and options:
            selected_label = str(options[0].get("label") or "")

        prefix_pattern = re.compile(r"([□☐■☑√✔✓])(\s*)([^□☐■☑√✔✓\[\]；;，,、/\s]+)")
        prefix_changed = False

        def replace_prefix(match: re.Match[str]) -> str:
            nonlocal prefix_changed
            label = match.group(3).strip()
            if not any(str(item.get("label") or "") == label for item in options):
                return match.group(0)
            prefix_changed = True
            mark = "☑" if label == selected_label else "□"
            return f"{mark}{match.group(2)}{match.group(3)}"

        text = prefix_pattern.sub(replace_prefix, text)
        if prefix_changed:
            return text

        bracket_pattern = re.compile(r"([^\[\]□☐■☑√✔✓；;，,、/]{1,24})\[(.*?)\]")
        bracket_changed = False

        def replace_bracket(match: re.Match[str]) -> str:
            nonlocal bracket_changed
            label = match.group(1).strip()
            option_label = re.split(r"[:：]", label)[-1].strip()
            if not any(str(item.get("label") or "") == option_label for item in options):
                return match.group(0)
            bracket_changed = True
            mark = "√" if option_label == selected_label else " "
            return f"{match.group(1)}[{mark}]"

        text = bracket_pattern.sub(replace_bracket, text)
        if bracket_changed:
            return text

        if options:
            return " ".join(
                f"{'☑' if str(item.get('label') or '') == selected_label else '□'}{item.get('label') or ''}"
                for item in options
            )
        normal_mark = "☑" if str(state or "").strip().lower() == "normal" else "□"
        abnormal_mark = "☑" if str(state or "").strip().lower() == "abnormal" else "□"
        return f"{normal_mark}正常 {abnormal_mark}异常"

    @staticmethod
    def _mop_cell_int(value: Any, fallback: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return fallback

    @staticmethod
    def _safe_mop_path_part(value: str, fallback: str = "MOP") -> str:
        text = str(value or "").strip() or fallback
        text = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", text)
        text = re.sub(r"\s+", " ", text).strip(" ._")
        return (text or fallback)[:96]

    @staticmethod
    def _mop_plain_text(value: Any) -> str:
        return re.sub(r"\s+", "", str(value or "").replace("\u3000", "")).strip()

    @classmethod
    def _is_mop_cover_sheet(cls, sheet_name: str, rows: list[list[str]]) -> bool:
        name = cls._mop_plain_text(sheet_name)
        if any(token in name for token in ("封面", "目录", "说明")):
            return True
        first_cells = "".join(
            cls._mop_plain_text(cell)
            for row in rows[:20]
            for cell in row[:8]
        )
        cover_tokens = ("文件封面", "封面", "版本履历", "修订记录")
        return any(token in first_cells for token in cover_tokens) and not any(
            token in first_cells for token in ("□正常", "□异常", "维护实施人", "维护开始时间", "维护完成时间")
        )

    @classmethod
    def _is_mop_checkbox_cell(cls, value: Any) -> bool:
        return bool(cls._mop_choice_options(value))

    @classmethod
    def _is_mop_datetime_placeholder(cls, value: Any) -> bool:
        text = cls._mop_plain_text(value)
        if not text:
            return False
        return all(token in text for token in ("年", "月", "日", "时")) and (
            "__" in text
            or "年月日时" in text
            or re.search(r"年\s*月\s*日\s*时", str(value or ""))
            or not re.search(r"\d{4}年\d{1,2}月\d{1,2}日\d{1,2}时", text)
        )

    @staticmethod
    def _mop_merged_target_col(
        *,
        row_index: int,
        col_index: int,
        merges: list[dict[str, int]] | None,
    ) -> int:
        for merge in merges or []:
            try:
                merge_row = int(merge.get("row") or 0)
                merge_col = int(merge.get("col") or 0)
                rowspan = max(1, int(merge.get("rowspan") or 1))
                colspan = max(1, int(merge.get("colspan") or 1))
            except Exception:
                continue
            if (
                merge_row <= row_index < merge_row + rowspan
                and merge_col <= col_index < merge_col + colspan
            ):
                return merge_col + colspan
        return col_index + 1

    @classmethod
    def _extract_mop_sheet_targets(
        cls,
        *,
        sheet_name: str,
        rows: list[list[str]],
        merges: list[dict[str, int]] | None = None,
    ) -> dict[str, Any]:
        is_cover = cls._is_mop_cover_sheet(sheet_name, rows)
        checkbox_cells: list[dict[str, Any]] = []
        maintenance_fields: list[dict[str, Any]] = []
        if is_cover:
            return {
                "is_cover": True,
                "checkbox_cells": checkbox_cells,
                "maintenance_fields": maintenance_fields,
                "fillable_count": 0,
            }
        labels = (
            "维护实施人",
            "维护开始时间",
            "维护完成时间",
            "维护完成情况",
            "维护审核人",
            "审核确认时间",
        )
        same_column_time_labels = {"维护完成时间", "审核确认时间"}
        checkbox_columns: list[int] = []
        for row_index, row in enumerate(rows):
            for col_zero, value in enumerate(row):
                if cls._is_mop_checkbox_cell(value):
                    if col_zero not in checkbox_columns:
                        checkbox_columns.append(col_zero)
                    options = cls._mop_choice_options(value)
                    option_labels = {str(item.get("label") or "") for item in options}
                    checkbox_cells.append(
                        {
                            "sheet": sheet_name,
                            "row": row_index,
                            "row_number": row_index + 1,
                            "col": col_zero,
                            "column": cls._column_label(col_zero + 1),
                            "cell_ref": f"{cls._column_label(col_zero + 1)}{row_index + 1}",
                            "value": str(value or ""),
                            "kind": (
                                "normal_abnormal"
                                if {"正常", "异常"}.issubset(option_labels)
                                else "choice_group"
                            ),
                            "options": options,
                        }
                    )
        for row_index, row in enumerate(rows):
            for col_index, value in enumerate(row):
                text = cls._mop_plain_text(value)
                if not text:
                    continue
                if cls._is_mop_checkbox_cell(value):
                    continue
                datetime_placeholder = cls._is_mop_datetime_placeholder(value)
                for label in labels:
                    if label not in text:
                        continue
                    inline_value = ""
                    label_pos = text.find(label)
                    if label_pos >= 0:
                        inline_value = text[label_pos + len(label) :].lstrip(":：")
                    value_col = cls._mop_merged_target_col(
                        row_index=row_index,
                        col_index=col_index,
                        merges=merges,
                    )
                    value_text = ""
                    if label not in {"维护实施人", "维护审核人"}:
                        for candidate_col in range(col_index + 1, min(len(row), col_index + 8)):
                            candidate = str(row[candidate_col] or "").strip()
                            if candidate:
                                value_col = candidate_col
                                value_text = candidate
                                break
                        if inline_value and not value_text:
                            value_col = col_index
                            value_text = inline_value
                    target_value_cols = [value_col]
                    if label in same_column_time_labels and checkbox_columns:
                        target_value_cols = [col_index] if col_index in checkbox_columns else list(checkbox_columns)
                    for target_value_col in target_value_cols:
                        target_value = ""
                        if target_value_col < len(row):
                            target_value = str(row[target_value_col] or "").strip()
                        if target_value_col == value_col:
                            target_value = value_text
                        if inline_value and target_value_col == col_index and not target_value:
                            target_value = inline_value
                        value_col = target_value_col
                        value_text = target_value
                        maintenance_fields.append(
                            {
                                "sheet": sheet_name,
                                "label": label,
                                "row": row_index,
                                "row_number": row_index + 1,
                                "label_col": col_index,
                                "label_cell_ref": f"{cls._column_label(col_index + 1)}{row_index + 1}",
                                "value_col": value_col,
                                "value_column": cls._column_label(value_col + 1),
                                "value_cell_ref": f"{cls._column_label(value_col + 1)}{row_index + 1}",
                                "value": value_text,
                                "kind": "maintenance_meta",
                            }
                        )
                    break
                else:
                    if not datetime_placeholder:
                        continue
                    if any(
                        int(field.get("row") or -1) == row_index
                        and int(field.get("value_col") or -1) == col_index
                        for field in maintenance_fields
                    ):
                        continue
                    label = "日期时间"
                    for candidate_col in range(col_index - 1, max(-1, col_index - 5), -1):
                        candidate = cls._mop_plain_text(row[candidate_col] if candidate_col < len(row) else "")
                        if candidate and not cls._is_mop_checkbox_cell(candidate):
                            label = candidate.rstrip(":：")
                            break
                    maintenance_fields.append(
                        {
                            "sheet": sheet_name,
                            "label": label,
                            "row": row_index,
                            "row_number": row_index + 1,
                            "label_col": col_index,
                            "label_cell_ref": f"{cls._column_label(col_index + 1)}{row_index + 1}",
                            "value_col": col_index,
                            "value_column": cls._column_label(col_index + 1),
                            "value_cell_ref": f"{cls._column_label(col_index + 1)}{row_index + 1}",
                            "value": str(value or ""),
                            "kind": "datetime_placeholder",
                        }
                    )
        return {
            "is_cover": False,
            "checkbox_cells": checkbox_cells,
            "maintenance_fields": maintenance_fields,
            "fillable_count": len(checkbox_cells) + len(maintenance_fields),
        }

    def _save_mop_attachment_copy(
        self,
        content: bytes,
        *,
        scope: str,
        mop_record_id: str,
        file_name: str,
        content_type: str = "",
    ) -> dict[str, Any]:
        scope_part = self._safe_mop_path_part(self._scope_label(self._normalize_scope(scope)) or scope, "ALL")
        date_part = dt.date.today().isoformat()
        original_name = self._safe_mop_path_part(file_name or "MOP表格.xlsx", "MOP表格.xlsx")
        suffix = Path(original_name).suffix
        if not suffix:
            if "csv" in str(content_type or "").lower():
                suffix = ".csv"
            else:
                suffix = ".xlsx"
        stem = self._safe_mop_path_part(Path(original_name).stem or "MOP表格", "MOP表格")
        digest = hashlib.sha1(content).hexdigest()[:10]
        save_dir = Path(get_data_file_path(os.path.join("engineer_mop_files", date_part, scope_part)))
        save_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_dir / f"{stem}_{digest}{suffix}"
        save_path.write_bytes(content)
        data_root = Path(get_data_file_path("")).resolve()
        try:
            relative_path = str(save_path.resolve().relative_to(data_root))
        except Exception:
            relative_path = save_path.name
        return {
            "file_name": save_path.name,
            "path": str(save_path),
            "relative_path": relative_path,
            "size": len(content),
            "saved_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    @staticmethod
    def _mop_editable_summary(sheets: list[dict[str, Any]]) -> dict[str, Any]:
        checkbox_count = 0
        maintenance_field_count = 0
        editable_sheet_count = 0
        for sheet in sheets:
            if not isinstance(sheet, dict) or sheet.get("is_cover"):
                continue
            sheet_checkbox_count = len(sheet.get("checkbox_cells") or [])
            sheet_field_count = len(sheet.get("maintenance_fields") or [])
            checkbox_count += sheet_checkbox_count
            maintenance_field_count += sheet_field_count
            if sheet_checkbox_count or sheet_field_count:
                editable_sheet_count += 1
        return {
            "editable_sheet_count": editable_sheet_count,
            "checkbox_count": checkbox_count,
            "maintenance_field_count": maintenance_field_count,
            "total": checkbox_count + maintenance_field_count,
        }

    @staticmethod
    def _xml_text(element: ET.Element | None) -> str:
        if element is None:
            return ""
        return "".join(element.itertext()).strip()

    def _parse_xlsx_preview(self, content: bytes, *, max_rows: int = 0, max_cols: int = 0) -> dict[str, Any]:
        ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        rel_ns = {"rel": "http://schemas.openxmlformats.org/package/2006/relationships"}
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            shared_strings: list[str] = []
            if "xl/sharedStrings.xml" in archive.namelist():
                root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
                for si in root.findall("main:si", ns):
                    shared_strings.append("".join(si.itertext()))
            workbook = ET.fromstring(archive.read("xl/workbook.xml"))
            rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
            rel_map = {
                rel.attrib.get("Id"): rel.attrib.get("Target", "")
                for rel in rels.findall("rel:Relationship", rel_ns)
            }
            sheets: list[dict[str, Any]] = []
            for sheet in workbook.findall("main:sheets/main:sheet", ns):
                name = str(sheet.attrib.get("name") or "Sheet")
                rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
                target = str(rel_map.get(rel_id) or "").lstrip("/")
                path = target if target.startswith("xl/") else f"xl/{target}"
                if path not in archive.namelist():
                    continue
                sheet_root = ET.fromstring(archive.read(path))
                rows_out: list[list[str]] = []
                max_width = 0
                truncated = False
                merges: list[dict[str, int]] = []
                for merge in sheet_root.findall("main:mergeCells/main:mergeCell", ns):
                    parsed = self._parse_merge_range(merge.attrib.get("ref", ""))
                    if not parsed:
                        continue
                    row1, col1, row2, col2 = parsed
                    if (max_rows and row1 > max_rows) or (max_cols and col1 > max_cols):
                        continue
                    row2 = min(row2, max_rows) if max_rows else row2
                    col2 = min(col2, max_cols) if max_cols else col2
                    if row2 <= row1 and col2 <= col1:
                        continue
                    max_width = max(max_width, col2)
                    merges.append(
                        {
                            "row": row1 - 1,
                            "col": col1 - 1,
                            "rowspan": row2 - row1 + 1,
                            "colspan": col2 - col1 + 1,
                        }
                    )
                for row in sheet_root.findall("main:sheetData/main:row", ns):
                    row_values: list[str] = []
                    for cell in row.findall("main:c", ns):
                        col_index = self._column_index(cell.attrib.get("r", "")) or len(row_values) + 1
                        while len(row_values) < col_index - 1:
                            row_values.append("")
                        value = ""
                        cell_type = cell.attrib.get("t")
                        if cell_type == "inlineStr":
                            value = self._xml_text(cell.find("main:is", ns))
                        else:
                            raw = self._xml_text(cell.find("main:v", ns))
                            if cell_type == "s":
                                try:
                                    value = shared_strings[int(raw)]
                                except Exception:
                                    value = raw
                            else:
                                value = raw
                        row_values.append(value)
                    max_width = max(max_width, len(row_values))
                    rows_out.append(row_values[:max_cols] if max_cols else row_values)
                    if max_rows and len(rows_out) >= max_rows:
                        truncated = True
                        break
                targets = self._extract_mop_sheet_targets(sheet_name=name, rows=rows_out, merges=merges)
                target_width = max(
                    [max_width]
                    + [
                        int(field.get("value_col") or 0) + 1
                        for field in targets.get("maintenance_fields", [])
                        if isinstance(field, dict)
                    ]
                    + [
                        int(cell.get("col") or 0) + 1
                        for cell in targets.get("checkbox_cells", [])
                        if isinstance(cell, dict)
                    ]
                )
                sheets.append(
                    {
                        "name": name,
                        "rows": rows_out,
                        "row_count": len(rows_out),
                        "column_count": min(target_width, max_cols) if max_cols else target_width,
                        "columns": [
                            self._column_label(index + 1)
                            for index in range(min(target_width, max_cols) if max_cols else target_width)
                        ],
                        "merges": merges,
                        "truncated": truncated or bool(max_cols and target_width > max_cols),
                        **targets,
                    }
                )
        return {"sheets": sheets, "parser": "xlsx"}

    def _parse_csv_preview(self, content: bytes, *, file_name: str = "") -> dict[str, Any]:
        text = ""
        for encoding in ("utf-8-sig", "gb18030", "utf-16"):
            try:
                text = content.decode(encoding)
                break
            except Exception:
                continue
        if not text:
            text = content.decode("utf-8", errors="replace")
        rows = [row for row in csv.reader(io.StringIO(text))]
        sheet_name = Path(file_name or "CSV").stem or "CSV"
        targets = self._extract_mop_sheet_targets(sheet_name=sheet_name, rows=rows)
        max_width = max(
            [max([len(row) for row in rows] or [0])]
            + [
                int(field.get("value_col") or 0) + 1
                for field in targets.get("maintenance_fields", [])
                if isinstance(field, dict)
            ]
            + [
                int(cell.get("col") or 0) + 1
                for cell in targets.get("checkbox_cells", [])
                if isinstance(cell, dict)
            ]
        )
        return {
            "sheets": [
                {
                    "name": sheet_name,
                    "rows": rows,
                    "row_count": len(rows),
                    "column_count": max_width,
                    "columns": [self._column_label(index + 1) for index in range(max_width)],
                    "merges": [],
                    "truncated": False,
                    **targets,
                }
            ],
            "parser": "csv",
        }

    def _download_mop_attachment(
        self,
        attachment: dict[str, Any],
        *,
        cache_category: str = "",
        app_token: str = "",
        table_id: str = "",
        record_id: str = "",
        latest_publish_time: str = "",
        force_download: bool = False,
    ) -> tuple[bytes, str]:
        cache_category = str(cache_category or attachment.get("cache_category") or "mop").strip()
        app_token = str(app_token or attachment.get("app_token") or "").strip()
        table_id = str(table_id or attachment.get("table_id") or "").strip()
        record_id = str(record_id or attachment.get("record_id") or "").strip()
        latest_publish_time = str(latest_publish_time or attachment.get("latest_publish_time") or "").strip()
        if cache_category and app_token and table_id and record_id and not force_download:
            cached = self._read_cached_attachment(
                category=cache_category,
                app_token=app_token,
                table_id=table_id,
                record_id=record_id,
                attachment=attachment,
                latest_publish_time=latest_publish_time,
            )
            if cached is not None:
                return cached
        url = str(attachment.get("url") or "").strip()
        token = str(attachment.get("file_token") or "").strip()
        headers = self._auth_headers()
        if url:
            content, content_type = self._http_client.request_bytes("GET", url, headers=headers)
        else:
            if not token:
                raise PortalError("MOP附件缺少 file_token 或下载地址。")
            encoded = quote(token, safe="")
            download_url = f"https://open.feishu.cn/open-apis/drive/v1/medias/{encoded}/download"
            content, content_type = self._http_client.request_bytes("GET", download_url, headers=headers)
        if cache_category and app_token and table_id and record_id:
            self._write_cached_attachment(
                category=cache_category,
                app_token=app_token,
                table_id=table_id,
                record_id=record_id,
                attachment=attachment,
                latest_publish_time=latest_publish_time,
                content=content,
                content_type=content_type,
            )
        return content, content_type

    def _attachment_cache_daily_marker_path(self) -> Path:
        return Path(get_data_file_path(os.path.join("attachment_cache", ATTACHMENT_CACHE_DAILY_MARKER)))

    def _maybe_start_daily_attachment_cache_refresh(self) -> None:
        today = dt.date.today().isoformat()
        now = time.time()
        marker_path = self._attachment_cache_daily_marker_path()
        try:
            marker = json.loads(marker_path.read_text(encoding="utf-8")) if marker_path.is_file() else {}
        except Exception:
            marker = {}
        if isinstance(marker, dict) and marker.get("date") == today:
            status = str(marker.get("status") or "").strip()
            if status == "success":
                return
            try:
                next_retry_at = float(marker.get("next_retry_at") or 0)
            except Exception:
                next_retry_at = 0.0
            if next_retry_at > now:
                return
        with self._attachment_cache_refresh_lock:
            if self._attachment_cache_refresh_running:
                return
            self._attachment_cache_refresh_running = True
        thread = threading.Thread(
            target=self._refresh_attachment_cache_daily,
            args=(today,),
            name="clipflow-attachment-cache-refresh",
            daemon=True,
        )
        thread.start()

    def _refresh_attachment_cache_daily(self, today: str) -> None:
        stats = {
            "date": today,
            "started_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mop_checked": 0,
            "signature_checked": 0,
            "failed": 0,
            "errors": [],
        }
        try:
            try:
                candidates, _warnings, _settings = self._load_engineer_mop_candidates(force=True)
                for candidate in candidates:
                    for attachment in candidate.get("attachments") or []:
                        if not isinstance(attachment, dict):
                            continue
                        stats["mop_checked"] += 1
                        try:
                            self._download_mop_attachment(attachment)
                        except Exception as exc:
                            stats["failed"] += 1
                            if len(stats["errors"]) < 10:
                                stats["errors"].append(f"MOP:{candidate.get('title') or candidate.get('record_id')}: {exc}")
            except Exception as exc:
                stats["failed"] += 1
                stats["errors"].append(f"MOP扫描失败: {exc}")
            try:
                people = self._load_signature_people(force=True)
                for person in people:
                    fields = person.get("raw_fields") if isinstance(person.get("raw_fields"), dict) else {}
                    for attachment in self._extract_signature_attachments(fields):
                        contextual = self._attachment_with_cache_context(
                            attachment,
                            category="signature",
                            app_token=SIGNATURE_APP_TOKEN,
                            table_id=SIGNATURE_TABLE_ID,
                            record_id=str(person.get("record_id") or ""),
                            latest_publish_time=str(person.get("latest_publish_time") or ""),
                        )
                        stats["signature_checked"] += 1
                        try:
                            self._download_mop_attachment(contextual)
                        except Exception as exc:
                            stats["failed"] += 1
                            if len(stats["errors"]) < 10:
                                stats["errors"].append(f"签名:{person.get('name') or person.get('record_id')}: {exc}")
            except Exception as exc:
                stats["failed"] += 1
                stats["errors"].append(f"签名扫描失败: {exc}")
        finally:
            stats["finished_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            stats["status"] = "success" if not stats["failed"] else "partial" if (stats["mop_checked"] or stats["signature_checked"]) else "failed"
            if stats["failed"]:
                stats["next_retry_at"] = time.time() + 30 * 60
                stats["next_retry_at_text"] = dt.datetime.fromtimestamp(
                    float(stats["next_retry_at"])
                ).strftime("%Y-%m-%d %H:%M:%S")
            with suppress(Exception):
                marker_path = self._attachment_cache_daily_marker_path()
                marker_path.parent.mkdir(parents=True, exist_ok=True)
                marker_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
            with self._attachment_cache_refresh_lock:
                self._attachment_cache_refresh_running = False

    def preview_engineer_mop_attachment(
        self,
        *,
        scope: str = "ALL",
        mop_record_id: str,
        file_token: str = "",
        file_name: str = "",
    ) -> dict[str, Any]:
        mop_candidates, warnings, settings = self._load_engineer_mop_candidates()
        if not settings.get("app_token") or not settings.get("table_id"):
            raise PortalError("未配置 MOP 多维表，无法预览。")
        record = next(
            (item for item in mop_candidates if str(item.get("record_id") or "") == str(mop_record_id or "")),
            None,
        )
        if not record:
            raise PortalError("未找到选择的 MOP 表格记录。")
        attachments = record.get("attachments") if isinstance(record.get("attachments"), list) else []
        attachment = None
        for item in attachments:
            if not isinstance(item, dict):
                continue
            if file_token and str(item.get("file_token") or "") == str(file_token):
                attachment = item
                break
            if file_token and str(item.get("url") or "") == str(file_token):
                attachment = item
                break
            if file_token and str(item.get("name") or "") == str(file_token):
                attachment = item
                break
        if attachment is None and attachments:
            attachment = attachments[0]
        if not isinstance(attachment, dict):
            raise PortalError("该 MOP 记录没有可预览的表格附件。")
        resolved_name = str(file_name or attachment.get("name") or "").strip()
        content, content_type = self._download_mop_attachment(attachment)
        local_file = self._save_mop_attachment_copy(
            content,
            scope=scope,
            mop_record_id=str(record.get("record_id") or mop_record_id or ""),
            file_name=resolved_name or str(attachment.get("name") or "MOP表格.xlsx"),
            content_type=content_type,
        )
        lower_name = resolved_name.lower()
        if lower_name.endswith(".csv") or "csv" in content_type:
            parsed = self._parse_csv_preview(content, file_name=resolved_name)
        elif lower_name.endswith(".xlsx") or content[:2] == b"PK":
            parsed = self._parse_xlsx_preview(content)
        else:
            raise PortalError("暂只支持预览 xlsx/csv 格式的 MOP 表格附件。")
        parsed.update(
            {
                "mop_record_id": record.get("record_id"),
                "mop_title": record.get("title"),
                "attachment": attachment,
                "local_file": local_file,
                "editable_summary": self._mop_editable_summary(parsed.get("sheets") or []),
                "warnings": warnings,
            }
        )
        return parsed

    def fill_engineer_mop_file(
        self,
        *,
        scope: str = "ALL",
        local_file_path: str = "",
        mop_record_id: str = "",
        mop_title: str = "",
        sheet_name: str = "",
        fields: list[dict[str, Any]] | None = None,
        checkboxes: list[dict[str, Any]] | None = None,
        cell_edits: list[dict[str, Any]] | None = None,
        signatures: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        try:
            from openpyxl import load_workbook
            from openpyxl.drawing.image import Image as ExcelImage
            from PIL import Image
        except Exception as exc:  # pragma: no cover - dependency bootstrap should provide these.
            raise PortalError("缺少 openpyxl/Pillow 依赖，无法生成已签名 MOP。") from exc

        data_root = Path(get_data_file_path("")).resolve()
        source_path = Path(str(local_file_path or "")).resolve()
        try:
            source_path.relative_to(data_root)
        except Exception as exc:
            raise PortalError("MOP 本地文件路径无效。") from exc
        if not source_path.is_file():
            raise PortalError("MOP 本地文件不存在，请重新查看表格后再生成。")
        if source_path.suffix.lower() not in {".xlsx", ".xlsm"}:
            raise PortalError("当前仅支持对 xlsx/xlsm MOP 文件插入签名。")

        fields = [item for item in (fields or []) if isinstance(item, dict)]
        checkboxes = [item for item in (checkboxes or []) if isinstance(item, dict)]
        cell_edits = [item for item in (cell_edits or []) if isinstance(item, dict)]
        signatures = [item for item in (signatures or []) if isinstance(item, dict)]
        if not signatures:
            raise PortalError("请选择至少一个维护实施人或维护审核人签名。")

        role_to_label = {
            "implementer": "维护实施人",
            "auditor": "维护审核人",
        }
        role_fields: dict[str, dict[str, Any]] = {}
        for field in fields:
            label = str(field.get("label") or "")
            for role, role_label in role_to_label.items():
                if role_label in label and role not in role_fields:
                    role_fields[role] = field
        missing_roles = sorted(
            {
                str(item.get("role") or "")
                for item in signatures
                if str(item.get("role") or "") in role_to_label
            }
            - set(role_fields)
        )
        if missing_roles:
            raise PortalError("当前 Sheet 未识别到对应签名位置，请切换到非封面填写页。")

        workbook = load_workbook(source_path)
        worksheet = workbook[sheet_name] if sheet_name and sheet_name in workbook.sheetnames else workbook.active
        protected_cells: set[tuple[int, int]] = set()
        for checkbox in checkboxes:
            row = self._mop_cell_int(checkbox.get("row"), -1) + 1
            col = self._mop_cell_int(checkbox.get("col"), -1) + 1
            if row > 0 and col > 0:
                protected_cells.add((row, col))
        for field in fields:
            row = self._mop_cell_int(field.get("row"), -1) + 1
            label_col = self._mop_cell_int(field.get("label_col"), -1) + 1
            value_col = self._mop_cell_int(
                field.get("value_col"),
                self._mop_cell_int(field.get("label_col"), -1),
            ) + 1
            if row > 0 and label_col > 0:
                protected_cells.add((row, label_col))
            if row > 0 and value_col > 0:
                protected_cells.add((row, value_col))
        inserted = 0
        updated_cells = 0
        temp_paths: list[str] = []
        try:
            for edit in cell_edits:
                row = self._mop_cell_int(edit.get("row"), -1) + 1
                col = self._mop_cell_int(edit.get("col"), -1) + 1
                if row <= 0 or col <= 0 or (row, col) in protected_cells:
                    continue
                worksheet.cell(row=row, column=col).value = str(edit.get("value") or "")
                updated_cells += 1
            for checkbox in checkboxes:
                state = str(
                    checkbox.get("selection")
                    or checkbox.get("selected_label")
                    or checkbox.get("selected_key")
                    or checkbox.get("state")
                    or ""
                ).strip()
                if not state:
                    continue
                row = self._mop_cell_int(checkbox.get("row"), -1) + 1
                col = self._mop_cell_int(checkbox.get("col"), -1) + 1
                if row <= 0 or col <= 0:
                    continue
                cell = worksheet.cell(row=row, column=col)
                cell.value = self._mop_choice_value(cell.value, state)
                updated_cells += 1
            for field in fields:
                fill_value = str(field.get("fill_value") or "").strip()
                if not fill_value:
                    continue
                label = str(field.get("label") or "").strip()
                if label in {"维护实施人", "维护审核人"}:
                    continue
                row = self._mop_cell_int(field.get("row"), -1) + 1
                value_col_zero = self._mop_cell_int(
                    field.get("value_col"),
                    self._mop_cell_int(field.get("label_col"), -1),
                )
                col = value_col_zero + 1
                if row <= 0 or col <= 0:
                    continue
                label_col = self._mop_cell_int(field.get("label_col"), -1) + 1
                cell = worksheet.cell(row=row, column=col)
                value_only = "维护完成时间" in label or "审核确认时间" in label
                cell.value = f"{label}：{fill_value}" if label and label_col == col and not value_only else fill_value
                updated_cells += 1
            for role in ("implementer", "auditor"):
                field = role_fields.get(role)
                if not field:
                    continue
                role_signatures = [
                    item for item in signatures
                    if str(item.get("role") or "") == role
                    and (
                        str(item.get("record_id") or "").strip()
                        or str(item.get("temp_id") or "").strip()
                    )
                ]
                if not role_signatures:
                    continue
                base_row = int(field.get("row") or 0) + 1
                label_col = self._mop_cell_int(field.get("label_col"), -1) + 1
                base_col = self._mop_signature_target_column(
                    worksheet,
                    row=base_row,
                    label_col=label_col,
                )
                role_images = []
                for signature in role_signatures:
                    source = str(signature.get("source") or "").strip()
                    temp_id = str(signature.get("temp_id") or "").strip()
                    record_id = str(signature.get("record_id") or "").strip()
                    if source == "temporary" or temp_id:
                        image_bytes, _content_type = self.temporary_signature_image_bytes(temp_id=temp_id)
                    elif source == "external":
                        image_bytes, _content_type = self.external_signature_image_bytes(record_id=record_id)
                    else:
                        image_bytes, _content_type = self.signature_image_bytes(record_id=record_id)
                    image = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
                    max_width, max_height = 150, 46
                    ratio = min(max_width / max(1, image.width), max_height / max(1, image.height), 1.0)
                    if ratio < 1.0:
                        image = image.resize(
                            (max(1, int(image.width * ratio)), max(1, int(image.height * ratio)))
                        )
                    role_images.append(image)
                if not role_images:
                    continue
                gap = 8
                combined_width = max(1, sum(image.width for image in role_images) + gap * (len(role_images) - 1))
                combined_height = max(1, max(image.height for image in role_images))
                combined = Image.new("RGBA", (combined_width, combined_height), (255, 255, 255, 0))
                offset_x = 0
                for image in role_images:
                    combined.alpha_composite(image, (offset_x, max(0, (combined_height - image.height) // 2)))
                    offset_x += image.width + gap
                temp = tempfile.NamedTemporaryFile(delete=False, suffix=".png", prefix="clipflow_mop_signature_")
                temp.close()
                combined.save(temp.name, format="PNG")
                temp_paths.append(temp.name)
                excel_image = ExcelImage(temp.name)
                excel_image.width = combined.width
                excel_image.height = combined.height
                worksheet.add_image(excel_image, f"{self._column_label(base_col)}{base_row}")
                inserted += len(role_images)
            if not inserted:
                raise PortalError("没有可插入的签名。")
            scope_part = self._safe_mop_path_part(self._scope_label(self._normalize_scope(scope)) or scope, "ALL")
            date_part = dt.date.today().isoformat()
            stem = self._safe_mop_path_part(
                Path(source_path.name).stem or mop_title or mop_record_id or "MOP",
                "MOP",
            )
            output_dir = Path(get_data_file_path(os.path.join("engineer_mop_filled", date_part, scope_part)))
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / (
                f"{stem}_已签名_{dt.datetime.now().strftime('%H%M%S')}_{uuid.uuid4().hex[:8]}"
                f"{source_path.suffix}"
            )
            workbook.save(output_path)
        finally:
            for path in temp_paths:
                with suppress(Exception):
                    os.remove(path)

        try:
            relative_path = str(output_path.resolve().relative_to(data_root))
        except Exception:
            relative_path = output_path.name
        return {
            "file_name": output_path.name,
            "path": str(output_path),
            "relative_path": relative_path,
            "inserted": inserted,
            "updated_cells": updated_cells,
            "saved_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _mop_signature_people_for_upload(
        self, signatures: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        role_to_label = {
            "implementer": "维护实施人",
            "auditor": "维护审核人",
        }
        roles_by_record: dict[str, set[str]] = {}
        temporary_by_id: dict[str, set[str]] = {}
        external_by_record: dict[str, set[str]] = {}
        for item in signatures:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "").strip()
            record_id = str(item.get("record_id") or "").strip()
            temp_id = str(item.get("temp_id") or "").strip()
            source = str(item.get("source") or "").strip()
            if role not in role_to_label:
                continue
            if source == "temporary" or temp_id:
                if temp_id:
                    temporary_by_id.setdefault(temp_id, set()).add(role)
                continue
            if source == "external":
                if record_id:
                    external_by_record.setdefault(record_id, set()).add(role)
                continue
            if record_id:
                roles_by_record.setdefault(record_id, set()).add(role)
        present_roles = {
            role
            for roles in list(roles_by_record.values()) + list(temporary_by_id.values()) + list(external_by_record.values())
            for role in roles
        }
        missing = [label for role, label in role_to_label.items() if role not in present_roles]
        if missing:
            raise PortalError(f"上传已签名 MOP 前请先选择可用签名：{'、'.join(missing)}。")

        people = self._load_signature_people(force=False)
        people_by_id = {
            str(item.get("record_id") or "").strip(): item
            for item in people
            if str(item.get("record_id") or "").strip()
        }
        missing_records = [record_id for record_id in roles_by_record if record_id not in people_by_id]
        if missing_records:
            people = self._load_signature_people(force=True)
            people_by_id = {
                str(item.get("record_id") or "").strip(): item
                for item in people
                if str(item.get("record_id") or "").strip()
            }
        entries: list[dict[str, Any]] = []
        for temp_id, roles in temporary_by_id.items():
            session = self._state_store.get_mop_temporary_signature_session(temp_id=temp_id)
            if not session:
                raise PortalError("临时签名记录不存在，请重新发送签名链接。")
            if not str(session.get("signature_file_token") or "").strip():
                raise PortalError(f"{session.get('display_name') or '临时人员'} 暂无可用签名。")
            # 临时人员没有 openid，只参与 MOP 签名完整性校验和插图，不发送通知。
            continue
        for record_id in external_by_record:
            try:
                self.external_signature_image_bytes(record_id=record_id)
            except Exception as exc:
                raise PortalError("其他人员签名不可用，请重新选择或重新签名。") from exc
            # 其他人员没有可通知 openid，只参与 MOP 签名完整性校验和插图。
            continue
        for record_id, roles in roles_by_record.items():
            person = people_by_id.get(record_id)
            if not person:
                raise PortalError("签名人员记录不存在，请刷新后重试。")
            if not person.get("has_signature"):
                raise PortalError(f"{person.get('name') or record_id} 暂无可用签名。")
            role_labels = [role_to_label[role] for role in ("implementer", "auditor") if role in roles]
            entries.append(
                {
                    "record_id": record_id,
                    "name": str(person.get("name") or record_id),
                    "open_id": str(person.get("open_id") or "").strip(),
                    "roles": [role for role in ("implementer", "auditor") if role in roles],
                    "role_labels": role_labels,
                }
            )
        return entries

    def _send_mop_signature_upload_notifications(
        self,
        *,
        people: list[dict[str, Any]],
        notice_title: str,
        building: str = "",
        maintenance_cycle: str = "",
        file_name: str = "",
        operator_name: str = "",
        operator_open_id: str = "",
    ) -> tuple[list[dict[str, Any]], str]:
        grouped: dict[str, dict[str, Any]] = {}
        results: list[dict[str, Any]] = []
        for person in people:
            open_id = str(person.get("open_id") or "").strip()
            key = open_id or f"missing:{person.get('record_id') or person.get('name')}"
            target = grouped.setdefault(
                key,
                {
                    "open_id": open_id,
                    "name": str(person.get("name") or ""),
                    "role_labels": [],
                },
            )
            for label in person.get("role_labels") or []:
                if label not in target["role_labels"]:
                    target["role_labels"].append(label)

        for item in grouped.values():
            open_id = str(item.get("open_id") or "").strip()
            role_text = "、".join(item.get("role_labels") or []) or "签名人员"
            if not open_id:
                results.append(
                    {
                        "open_id": "",
                        "name": item.get("name") or "",
                        "roles": role_text,
                        "ok": False,
                        "message": "该人员缺少 openid，无法发送个人消息。",
                    }
                )
                continue
            text_lines = [
                "【MOP签名确认】已上传维护保养单",
                "",
                f"签名角色：{role_text}",
                f"维护通告：{notice_title or '未命名维保通告'}",
            ]
            if building:
                text_lines.append(f"楼栋：{building}")
            if maintenance_cycle:
                text_lines.append(f"维护周期：{maintenance_cycle}")
            if file_name:
                text_lines.append(f"MOP文件：{file_name}")
            text_lines.extend(
                [
                    f"操作人：{operator_name or '未知'}"
                    + (f"（{operator_open_id}）" if operator_open_id else ""),
                    f"时间：{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                ]
            )
            if external_mock_enabled():
                ok, message, send_results = True, "mock external send skipped", [
                    {"open_id": open_id, "ok": True, "message": "mock external send skipped"}
                ]
            else:
                ok, message, send_results = send_text_to_open_ids("\n".join(text_lines), [open_id])
            send_result = (send_results or [{}])[0] if send_results else {}
            results.append(
                {
                    "open_id": open_id,
                    "name": item.get("name") or "",
                    "roles": role_text,
                    "ok": bool(ok and send_result.get("ok", ok)),
                    "message": str(send_result.get("message") or message or ""),
                }
            )
        failed = [item for item in results if not item.get("ok")]
        warning = ""
        if failed:
            warning = "签名人员通知失败：" + "；".join(
                f"{item.get('name') or item.get('open_id') or '未知人员'}({item.get('roles')})：{item.get('message') or '发送失败'}"
                for item in failed
            )
        return results, warning

    def upload_signed_engineer_mop_file(
        self,
        *,
        scope: str = "ALL",
        source_record_id: str = "",
        notice_title: str = "",
        notice_key: str = "",
        operator_open_id: str = "",
        operator_name: str = "",
        local_file_path: str = "",
        mop_record_id: str = "",
        mop_title: str = "",
        sheet_name: str = "",
        fields: list[dict[str, Any]] | None = None,
        checkboxes: list[dict[str, Any]] | None = None,
        cell_edits: list[dict[str, Any]] | None = None,
        signatures: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        self.ensure_snapshot_loaded()
        source_record_id = str(source_record_id or "").strip()
        if not source_record_id:
            raise PortalError("缺少维保源表记录 ID，无法上传已签名 MOP。")
        record = self._find_record_by_id(source_record_id, WORK_TYPE_MAINTENANCE)
        display_fields = record.get("display_fields") or {}
        building_codes = self._building_codes_from_value(display_fields.get("楼栋"))
        normalized_scope = self._normalize_scope(scope)
        if not self._scope_matches_buildings(normalized_scope, building_codes):
            raise PortalError("当前账号无权上传该楼栋的 MOP。")

        signature_people = self._mop_signature_people_for_upload(
            [item for item in (signatures or []) if isinstance(item, dict)]
        )
        filled = self.fill_engineer_mop_file(
            scope=scope,
            local_file_path=local_file_path,
            mop_record_id=mop_record_id,
            mop_title=mop_title,
            sheet_name=sheet_name,
            fields=fields or [],
            checkboxes=checkboxes or [],
            cell_edits=cell_edits or [],
            signatures=signatures or [],
        )
        file_token = self._upload_bitable_file(
            file_path=str(filled.get("path") or ""),
            file_name=str(filled.get("file_name") or ""),
            app_token=DEFAULT_APP_TOKEN,
        )
        update_fields = {
            MOP_SIGNED_ATTACHMENT_FIELD: [{"file_token": file_token}],
            MOP_ENGINEER_CONFIRM_FIELD: True,
            MOP_SUPERVISOR_CONFIRM_FIELD: True,
        }
        self._patch_record_fields(
            app_token=DEFAULT_APP_TOKEN,
            table_id=DEFAULT_TABLE_ID,
            record_id=source_record_id,
            fields=update_fields,
        )
        self._touch_state_cache_version()
        title = (
            str(notice_title or "").strip()
            or self._maintenance_title(record)
            or str(display_fields.get("维护总项") or "")
        )
        building = self._building_label_from_codes(building_codes)
        maintenance_cycle = str(display_fields.get("维护周期") or "").strip()
        notification_results, notification_warning = self._send_mop_signature_upload_notifications(
            people=signature_people,
            notice_title=title,
            building=building,
            maintenance_cycle=maintenance_cycle,
            file_name=str(filled.get("file_name") or ""),
            operator_name=operator_name,
            operator_open_id=operator_open_id,
        )
        return {
            "source_record_id": source_record_id,
            "notice_key": str(notice_key or ""),
            "notice_title": title,
            "building": building,
            "maintenance_cycle": maintenance_cycle,
            "file_token": file_token,
            "filled_file": filled,
            "updated_fields": list(update_fields.keys()),
            "notification_results": notification_results,
            "notification_warning": notification_warning,
            "uploaded_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def reset_engineer_mop_file(
        self,
        *,
        scope: str = "ALL",
        filled_file_path: str = "",
        mop_record_id: str = "",
        file_token: str = "",
        file_name: str = "",
    ) -> dict[str, Any]:
        data_root = Path(get_data_file_path("")).resolve()
        deleted = False
        filled_text = str(filled_file_path or "").strip()
        if filled_text:
            filled_path = Path(filled_text).resolve()
            try:
                relative = filled_path.relative_to(data_root)
                parts = {part.lower() for part in relative.parts}
                if "engineer_mop_filled" in parts and filled_path.is_file():
                    filled_path.unlink()
                    deleted = True
            except Exception as exc:
                raise PortalError("已签名 MOP 文件路径无效，已停止重新签名。") from exc
        preview = self.preview_engineer_mop_attachment(
            scope=scope,
            mop_record_id=mop_record_id,
            file_token=file_token,
            file_name=file_name,
        )
        preview["reset"] = {
            "deleted_old_file": deleted,
            "reset_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        return preview

    def _find_record_by_id(self, record_id: str, work_type: str = WORK_TYPE_MAINTENANCE) -> dict[str, Any]:
        if work_type == WORK_TYPE_CHANGE:
            records = self._change_records
        elif work_type == WORK_TYPE_REPAIR:
            records = self._repair_records
        else:
            records = self._records
        for record in records:
            if record["record_id"] == record_id:
                return record
        snapshot_records = self._source_snapshot_records("ALL")
        for record in snapshot_records or []:
            if (
                self._record_work_type(record) == work_type
                and str(record.get("record_id") or "") == record_id
            ):
                return record
        raise PortalError(f"未找到记录: {record_id}")

    def set_notice_work_type_override(
        self,
        *,
        record_id: str,
        source_work_type: str = WORK_TYPE_MAINTENANCE,
        target_work_type: str = WORK_TYPE_CHANGE,
        scope: str = "ALL",
        updated_by: str = "",
    ) -> dict[str, Any]:
        self.ensure_snapshot_loaded()
        record_id = str(record_id or "").strip()
        source_work_type = str(source_work_type or WORK_TYPE_MAINTENANCE).strip()
        target_work_type = str(target_work_type or WORK_TYPE_CHANGE).strip()
        if source_work_type != WORK_TYPE_MAINTENANCE:
            raise PortalError("当前仅支持将维保源记录转为变更。")
        if target_work_type not in {WORK_TYPE_CHANGE, WORK_TYPE_MAINTENANCE}:
            raise PortalError("转换目标只支持变更或维保。")
        if not record_id:
            raise PortalError("缺少源记录 ID。")
        record = self._find_record_by_id(record_id, WORK_TYPE_MAINTENANCE)
        fields = record.get("display_fields") or {}
        building_codes = self._building_codes_from_value(fields.get("楼栋"))
        normalized_scope = self._normalize_scope(scope)
        if not self._scope_matches_buildings(normalized_scope, building_codes):
            raise PortalError("当前账号无权转换该楼栋通告。")
        title = self._maintenance_title(record)
        normalized_title = self._work_type_override_title_key(
            title, WORK_TYPE_MAINTENANCE
        )
        if not normalized_title:
            raise PortalError("该维保记录缺少可用于长期转换的标题。")
        if target_work_type == WORK_TYPE_MAINTENANCE:
            changed = self._state_store.disable_work_type_override(
                source_work_type=WORK_TYPE_MAINTENANCE,
                normalized_title=normalized_title,
                updated_by=updated_by,
            )
            self._touch_state_cache_version()
            return {
                "record_id": record_id,
                "source_work_type": WORK_TYPE_MAINTENANCE,
                "target_work_type": WORK_TYPE_MAINTENANCE,
                "title": title,
                "normalized_title": normalized_title,
                "changed": changed,
            }
        override = self._state_store.upsert_work_type_override(
            source_work_type=WORK_TYPE_MAINTENANCE,
            normalized_title=normalized_title,
            target_work_type=WORK_TYPE_CHANGE,
            title=title,
            payload={
                "record_id": record_id,
                "building": self._building_label_from_codes(building_codes),
                "building_codes": building_codes,
                "maintenance_total": str(fields.get("维护总项") or ""),
                "maintenance_cycle": str(fields.get("维护周期") or ""),
            },
            updated_by=updated_by,
        )
        self._touch_state_cache_version()
        return {
            "record_id": record_id,
            "source_work_type": WORK_TYPE_MAINTENANCE,
            "target_work_type": WORK_TYPE_CHANGE,
            "title": title,
            "normalized_title": normalized_title,
            "changed": True,
            "override": override,
        }

    def generate_templates(self, drafts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        self.ensure_snapshot_loaded()
        generated: list[dict[str, Any]] = []
        for draft in drafts:
            record_id = str(draft.get("record_id") or "").strip()
            if not record_id:
                raise PortalError("存在缺少 record_id 的待生成记录。")
            record = self._find_record_by_id(record_id)
            fields = record["display_fields"]
            building = str(fields.get("楼栋") or "").strip()
            maintenance_total = str(fields.get("维护总项") or "").strip()
            maintenance_cycle = str(fields.get("维护周期") or "").strip()
            start_time = self._format_input_datetime(draft.get("start_time"))
            end_time = self._format_input_datetime(draft.get("end_time"))
            location = str(draft.get("location") or "").strip()
            content = str(draft.get("content") or "").strip()
            reason = str(draft.get("reason") or "").strip()
            impact = (
                str(draft.get("impact") or "").strip() or DEFAULT_IMPACT_TEXT
            )
            self._remember_draft_fields(
                building=building,
                maintenance_total=maintenance_total,
                location=location,
                content=content,
                reason=reason,
                impact=impact,
                maintenance_cycle=maintenance_cycle,
            )
            text = (
                self.build_notice_text(
                    status="开始",
                    title=f"EA118机房{building}{maintenance_total}",
                    start_time=start_time,
                    end_time=end_time,
                    location=location,
                    content=content,
                    reason=reason,
                    impact=impact,
                    progress=(
                        str(draft.get("progress") or "").strip()
                        or DEFAULT_PROGRESS_TEXT
                    ),
                )
            )
            generated.append(
                {
                    "record_id": record_id,
                    "title": f"EA118机房{building}{maintenance_total}",
                    "building": building,
                    "target_building": (
                        f"{self._building_code_from_value(building)}楼"
                        if self._building_code_from_value(building)
                        else ""
                    ),
                    "text": text,
                }
            )
        return generated

    @staticmethod
    def _format_notice_time_range(start_time: Any, end_time: Any) -> str:
        start_text = MaintenancePortalService._format_input_datetime(start_time)
        end_text = MaintenancePortalService._format_input_datetime(end_time)
        if start_text and end_text:
            return f"{start_text}{NOTICE_TIME_SEPARATOR}{end_text}"
        return start_text or end_text

    @staticmethod
    def _render_notice_text(
        *,
        work_type: str,
        status: str,
        values: dict[str, Any],
        heading_override: str = "",
    ) -> str:
        template = NOTICE_TEXT_TEMPLATES.get(
            str(work_type or "").strip(),
            NOTICE_TEXT_TEMPLATES[WORK_TYPE_MAINTENANCE],
        )
        heading = str(heading_override or template["heading"]).strip()
        lines = [f"【{heading}】状态：{str(status or '').strip()}"]
        for label, key in template["fields"]:
            value = values.get(key, "")
            lines.append(f"【{label}】{str(value or '').strip()}")
        return "\n".join(lines)

    @staticmethod
    def build_notice_text(
        *,
        status: str,
        title: str,
        start_time: str,
        end_time: str,
        location: str,
        content: str,
        reason: str,
        impact: str,
        progress: str,
    ) -> str:
        return MaintenancePortalService._render_notice_text(
            work_type=WORK_TYPE_MAINTENANCE,
            status=status,
            values={
                "title": title,
                "time_range": MaintenancePortalService._format_notice_time_range(
                    start_time, end_time
                ),
                "location": location,
                "content": content,
                "reason": reason,
                "impact": str(impact or "").strip() or DEFAULT_IMPACT_TEXT,
                "progress": str(progress or "").strip() or DEFAULT_PROGRESS_TEXT,
            },
        )

    def _base_job(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        now = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        now_ts = time.time()
        return {
            "job_id": uuid.uuid4().hex,
            "phase": "accepted",
            "created_at": now,
            "updated_at": now,
            "accepted_at": now_ts,
            "message_started_at": 0.0,
            "message_finished_at": 0.0,
            "upload_queued_at": 0.0,
            "upload_started_at": 0.0,
            "upload_finished_at": 0.0,
            "upload_release_at": 0.0,
            "request": copy.deepcopy(request_payload),
            "operation_id": str(request_payload.get("operation_id") or "").strip(),
            "target_key": self._action_target_key(request_payload),
            "depends_on_job_id": "",
            "depends_on_phase": "",
            "message_sent": False,
            "message_signature": "",
            "message_queue_position": 0,
            "message_queue_size": 0,
            "qt_phase": "",
            "qt_queue_position": 0,
            "qt_queue_size": 0,
            "queue_position": 0,
            "queue_size": 0,
            "upload_queue_position": 0,
            "upload_queue_size": 0,
            "record_id": "",
            "active_item_id": str(request_payload.get("active_item_id") or ""),
            "error": "",
            "error_category": "",
            "error_retryable": False,
            "upload_message": "",
            "prepared": {},
        }

    @staticmethod
    def classify_job_error(message: str) -> dict[str, Any]:
        text = str(message or "").strip()
        lower = text.lower()
        if not text:
            return {"error_category": "", "error_retryable": False}
        if any(key in lower for key in ("timeout", "timed out", "超时", "read timed")):
            return {"error_category": "network_timeout", "error_retryable": True}
        if any(key in lower for key in ("429", "rate limit", "too many requests", "限流", "频率")):
            return {"error_category": "remote_rate_limited", "error_retryable": True}
        if any(key in lower for key in ("connection", "network", "连接", "网络", "10053", "10054")):
            return {"error_category": "network_error", "error_retryable": True}
        if any(key in lower for key in ("重启", "restart")):
            return {"error_category": "process_restart", "error_retryable": False}
        if any(key in lower for key in ("token", "tenant_access_token", "access_token", "授权", "登录态")):
            return {"error_category": "token_or_auth", "error_retryable": False}
        if any(key in lower for key in ("permission", "forbidden", "无权限", "权限不足", "403")):
            return {"error_category": "permission_denied", "error_retryable": False}
        if any(key in lower for key in ("field", "字段", "维保周期", "参数", "invalid")):
            return {"error_category": "field_or_payload", "error_retryable": False}
        if any(key in lower for key in ("record not found", "记录不存在", "远端缺失", "1254002")):
            return {"error_category": "remote_record_missing", "error_retryable": False}
        if any(key in lower for key in ("qt", "主窗口", "主界面", "回调")):
            return {"error_category": "qt_bridge", "error_retryable": True}
        return {"error_category": "unknown", "error_retryable": True}

    @classmethod
    def _manual_payload_notice_work_type(cls, request_payload: dict[str, Any], fallback: str) -> str:
        work_type = str(fallback or WORK_TYPE_MAINTENANCE).strip() or WORK_TYPE_MAINTENANCE
        requested_type = cls._requested_manual_work_type(request_payload)
        if requested_type:
            return requested_type
        return cls._manual_payload_notice_work_type_inferred(request_payload, work_type)

    @classmethod
    def _requested_manual_work_type(cls, request_payload: dict[str, Any]) -> str:
        payload = request_payload if isinstance(request_payload, dict) else {}
        work_type = str(payload.get("work_type") or payload.get("lan_work_type") or "").strip()
        valid_types = {
            WORK_TYPE_MAINTENANCE,
            WORK_TYPE_CHANGE,
            WORK_TYPE_REPAIR,
            WORK_TYPE_POWER,
            WORK_TYPE_POLLING,
            WORK_TYPE_ADJUST,
        }
        return work_type if work_type in valid_types else ""

    @classmethod
    def _manual_payload_notice_work_type_inferred(
        cls, request_payload: dict[str, Any], fallback: str
    ) -> str:
        work_type = str(fallback or WORK_TYPE_MAINTENANCE).strip() or WORK_TYPE_MAINTENANCE
        if work_type not in NOTICE_TEXT_TEMPLATES:
            work_type = WORK_TYPE_MAINTENANCE
        if not cls._truthy_flag((request_payload or {}).get("manual")):
            return work_type
        if work_type != WORK_TYPE_MAINTENANCE:
            return work_type
        head_text = "\n".join(
            str((request_payload or {}).get(name) or "").strip()
            for name in ("notice_type", "title", "content")
            if str((request_payload or {}).get(name) or "").strip()
        )
        if re.search(r"设备检修|检修通告", head_text):
            return WORK_TYPE_REPAIR
        if re.search(r"上电通告|上下电通告|下电通告", head_text):
            return WORK_TYPE_POWER
        if re.search(r"设备轮巡|轮巡通告", head_text):
            return WORK_TYPE_POLLING
        if re.search(r"设备调整|调整通告", head_text):
            return WORK_TYPE_ADJUST
        if re.search(r"设备变更|变更通告", head_text):
            return WORK_TYPE_CHANGE
        return work_type

    @classmethod
    def _manual_payload_title_text(cls, request_payload: dict[str, Any]) -> str:
        payload = request_payload if isinstance(request_payload, dict) else {}
        direct_title = str(
            payload.get("title")
            or payload.get("name")
            or payload.get("notice_title")
            or ""
        ).strip()
        if direct_title:
            return direct_title
        text = str(payload.get("text") or payload.get("content") or "").strip()
        if text:
            sections = cls._parse_notice_sections(text)
            title = cls._notice_section_value(
                sections,
                ["名称", "标题", "维修名称", "通告名称"],
            )
            if title:
                return title
        return ""

    @classmethod
    def _manual_notice_type_conflict(
        cls, request_payload: dict[str, Any]
    ) -> dict[str, str] | None:
        payload = request_payload if isinstance(request_payload, dict) else {}
        if not cls._truthy_flag(payload.get("manual")):
            return None
        expected_type = (
            cls._requested_manual_work_type(payload)
            or cls._manual_payload_notice_work_type_inferred(
                payload,
                cls._request_work_type_fallback(payload),
            )
        )
        title = re.sub(r"\s+", "", cls._manual_payload_title_text(payload))
        if not title:
            return None
        for work_type, patterns in NOTICE_TYPE_KEYWORD_RULES.items():
            if work_type == expected_type:
                continue
            for pattern in patterns:
                match = re.search(pattern, title)
                if match:
                    return {
                        "expected_type": expected_type,
                        "actual_type": work_type,
                        "keyword": match.group(0) or NOTICE_WORK_TYPE_LABELS.get(work_type, work_type),
                    }
        return None

    @classmethod
    def validate_manual_notice_type_consistency(
        cls, request_payload: dict[str, Any]
    ) -> None:
        conflict = cls._manual_notice_type_conflict(request_payload)
        if not conflict:
            return
        expected = NOTICE_WORK_TYPE_LABELS.get(
            conflict.get("expected_type", ""),
            conflict.get("expected_type", "") or "当前",
        )
        actual = NOTICE_WORK_TYPE_LABELS.get(
            conflict.get("actual_type", ""),
            conflict.get("actual_type", "") or "其他",
        )
        keyword = conflict.get("keyword", "") or actual
        raise PortalError(
            f"当前选择的是{expected}通告，但标题/名称包含“{keyword}”，"
            f"像是{actual}通告。请改标题或重新选择通告类型。"
        )

    @classmethod
    def _notice_type_for_work_type(cls, work_type: str) -> str:
        return NOTICE_TYPE_BY_WORK_TYPE.get(
            str(work_type or "").strip(),
            NOTICE_TYPE_MAINTENANCE,
        )

    @classmethod
    def _work_type_for_notice_type(cls, notice_type: str) -> str:
        return WORK_TYPE_BY_NOTICE_TYPE.get(str(notice_type or "").strip(), "")

    @classmethod
    def _request_work_type_fallback(cls, request_payload: dict[str, Any]) -> str:
        payload = request_payload if isinstance(request_payload, dict) else {}
        return (
            str(payload.get("work_type") or payload.get("lan_work_type") or "").strip()
            or cls._work_type_for_notice_type(str(payload.get("notice_type") or ""))
            or WORK_TYPE_MAINTENANCE
        )

    @classmethod
    def _normalize_request_notice_type(
        cls, request_payload: dict[str, Any]
    ) -> dict[str, Any]:
        payload = dict(request_payload or {})
        requested_notice_type = str(payload.get("notice_type") or "").strip()
        work_type = cls._manual_payload_notice_work_type(
            payload,
            cls._request_work_type_fallback(payload),
        )
        payload["work_type"] = work_type
        if work_type == WORK_TYPE_POWER and requested_notice_type in {
            NOTICE_TYPE_POWER_UP,
            NOTICE_TYPE_POWER_DOWN,
            NOTICE_TYPE_POWER,
        }:
            if requested_notice_type in {NOTICE_TYPE_POWER_UP, NOTICE_TYPE_POWER_DOWN}:
                payload["power_notice_heading"] = requested_notice_type
            payload["notice_type"] = NOTICE_TYPE_POWER
        else:
            payload["notice_type"] = cls._notice_type_for_work_type(work_type)
        return payload

    @classmethod
    def _action_target_key(cls, request_payload: dict[str, Any]) -> str:
        request_payload = normalize_notice_identity_payload(request_payload)
        action = str((request_payload or {}).get("action") or "").strip().lower()
        work_type = cls._manual_payload_notice_work_type(
            request_payload,
            cls._request_work_type_fallback(request_payload),
        )
        if action == "start":
            record_id = str((request_payload or {}).get("record_id") or "").strip()
            manual_id = str((request_payload or {}).get("manual_id") or "").strip()
            if MaintenancePortalService._truthy_flag((request_payload or {}).get("manual")) and manual_id:
                return f"{work_type}:manual-start:{manual_id}"
            return f"{work_type}:start:{record_id}" if record_id else ""
        if action in {"update", "end"}:
            target_record_id = canonical_target_record_id(request_payload)
            if target_record_id:
                return f"{work_type}:target:{target_record_id}"
            active_item_id = str(
                (request_payload or {}).get("active_item_id") or ""
            ).strip()
            if active_item_id:
                return f"{work_type}:active:{active_item_id}"
            source_record_id = str(
                (request_payload or {}).get("source_record_id") or ""
            ).strip()
            if action == "update" and source_record_id:
                return f"{work_type}:source-update:{source_record_id}"
        return ""

    def _load_action_jobs_from_state(self) -> dict[str, dict[str, Any]]:
        jobs: dict[str, dict[str, Any]] = {}
        for document in self._state_store.list_documents(STATE_NS_ACTION_JOB):
            job = document.get("payload")
            if not isinstance(job, dict):
                continue
            job_id = str(job.get("job_id") or document.get("key") or "").strip()
            if not job_id:
                continue
            job["job_id"] = job_id
            phase = str(job.get("phase") or "").strip()
            if phase in {"accepted", "queued"} and not float(job.get("message_started_at") or 0):
                job["phase"] = "accepted"
                job["error"] = ""
                job["error_category"] = ""
                job["error_retryable"] = False
                job["restart_recovered"] = True
                job["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
                self._state_store.put_document(STATE_NS_ACTION_JOB, job_id, job)
            elif phase in {
                "accepted",
                "queued",
                "sending_message",
                "message_sent",
                "upload_queued",
                "qt_queued",
                "qt_displaying",
                "upload_waiting",
                "uploading",
            }:
                job["phase"] = "failed"
                job["error"] = "程序已重启，任务未完成，请核对多维后重试。"
                classified = self.classify_job_error(job["error"])
                job["error_category"] = classified["error_category"]
                job["error_retryable"] = classified["error_retryable"]
                job["restart_recovered"] = False
                job["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
                self._state_store.put_document(STATE_NS_ACTION_JOB, job_id, job)
            jobs[job_id] = job
        if len(jobs) > ACTION_JOB_MAX_RETAINED:
            terminal_phases = {"success", "failed"}
            terminal_jobs = [
                job
                for job in jobs.values()
                if str(job.get("phase") or "") in terminal_phases
            ]
            terminal_jobs.sort(key=lambda item: str(item.get("created_at") or ""))
            remove_count = min(
                max(0, len(jobs) - ACTION_JOB_MAX_RETAINED),
                len(terminal_jobs),
            )
            for job in terminal_jobs[:remove_count]:
                job_id = str(job.get("job_id") or "")
                jobs.pop(job_id, None)
                self._state_store.delete_document(STATE_NS_ACTION_JOB, job_id)
        return jobs

    def recoverable_action_job_ids(self) -> list[str]:
        with self._jobs_lock:
            return [
                str(job.get("job_id") or "")
                for job in self._jobs.values()
                if isinstance(job, dict)
                and str(job.get("phase") or "") == "accepted"
                and bool(job.get("restart_recovered"))
                and str(job.get("job_id") or "")
            ]

    def _persist_action_job_locked(self, job: dict[str, Any]) -> None:
        job_id = str((job or {}).get("job_id") or "").strip()
        if not job_id:
            return
        self._state_store.put_document(STATE_NS_ACTION_JOB, job_id, job)

    def create_action_job(self, request_payload: dict[str, Any]) -> tuple[str, bool]:
        if not isinstance(request_payload, dict):
            raise PortalError("请求体格式错误。")
        request_payload = normalize_notice_identity_payload(request_payload)
        self.validate_manual_notice_type_consistency(request_payload)
        request_payload = self._normalize_request_notice_type(request_payload)
        action = str(request_payload.get("action") or "").strip().lower()
        if action not in {"start", "update", "end"}:
            raise PortalError("动作必须是 start/update/end。")
        if action == "start" and not (
            str(request_payload.get("record_id") or "").strip()
            or (
                self._truthy_flag(request_payload.get("manual"))
                and str(request_payload.get("manual_id") or "").strip()
            )
        ):
            raise PortalError("开始通告缺少计划记录ID。")
        if action in {"update", "end"} and not (
            str(request_payload.get("active_item_id") or "").strip()
            or str(request_payload.get("source_record_id") or "").strip()
            or str(request_payload.get("target_record_id") or "").strip()
        ):
            raise PortalError("更新/结束通告缺少主界面条目ID、源记录ID或目标多维record_id。")
        operation_id = str(request_payload.get("operation_id") or "").strip()
        job = self._base_job(request_payload)
        with self._jobs_lock:
            if operation_id:
                for existing in self._jobs.values():
                    if str(existing.get("operation_id") or "") != operation_id:
                        continue
                    phase = str(existing.get("phase") or "")
                    if phase in {
                        "accepted",
                        "queued",
                        "sending_message",
                        "message_sent",
                        "upload_queued",
                        "qt_queued",
                        "qt_displaying",
                        "upload_waiting",
                        "uploading",
                        "success",
                    }:
                        return str(existing.get("job_id") or ""), False
                    if phase == "failed":
                        existing["request"] = copy.deepcopy(request_payload)
                        existing["phase"] = "accepted"
                        existing["error"] = ""
                        existing["error_category"] = ""
                        existing["error_retryable"] = False
                        existing["accepted_at"] = time.time()
                        existing["message_started_at"] = 0.0
                        existing["message_finished_at"] = 0.0
                        existing["upload_queued_at"] = 0.0
                        existing["upload_started_at"] = 0.0
                        existing["upload_finished_at"] = 0.0
                        existing["upload_release_at"] = 0.0
                        existing["qt_phase"] = ""
                        existing["qt_queue_position"] = 0
                        existing["qt_queue_size"] = 0
                        existing["updated_at"] = dt.datetime.now().strftime(
                            "%Y-%m-%d %H:%M"
                        )
                        self._persist_action_job_locked(existing)
                        return str(existing.get("job_id") or ""), True
            target_key = str(job.get("target_key") or "")
            if target_key:
                blocking_phase_order = {
                    "accepted": 1,
                    "queued": 2,
                    "sending_message": 3,
                    "message_sent": 4,
                    "upload_queued": 5,
                    "qt_queued": 6,
                    "qt_displaying": 7,
                    "upload_waiting": 8,
                    "uploading": 9,
                }
                blocking_job_id = ""
                blocking_phase = ""
                blocking_rank = -1
                blocking_epoch = 0.0
                for existing in self._jobs.values():
                    if str(existing.get("target_key") or "") != target_key:
                        continue
                    phase = str(existing.get("phase") or "")
                    if phase not in blocking_phase_order:
                        continue
                    rank = int(blocking_phase_order.get(phase) or 0)
                    epoch = self._job_epoch(existing) or 0.0
                    if rank > blocking_rank or (rank == blocking_rank and epoch >= blocking_epoch):
                        blocking_job_id = str(existing.get("job_id") or "").strip()
                        blocking_phase = phase
                        blocking_rank = rank
                        blocking_epoch = epoch
                if blocking_job_id:
                    job["depends_on_job_id"] = blocking_job_id
                    job["depends_on_phase"] = blocking_phase
            self._jobs[job["job_id"]] = job
            self._persist_action_job_locked(job)
            self._trim_jobs_locked()
        return job["job_id"], True

    def _trim_jobs_locked(self) -> None:
        if len(self._jobs) <= ACTION_JOB_MAX_RETAINED:
            return
        terminal_phases = {"success", "failed"}
        terminal_jobs = [
            job
            for job in self._jobs.values()
            if str(job.get("phase") or "") in terminal_phases
        ]
        terminal_jobs.sort(key=lambda item: self._job_epoch(item) or 0)
        remove_count = max(0, len(self._jobs) - ACTION_JOB_MAX_RETAINED)
        for job in terminal_jobs[:remove_count]:
            job_id = str(job.get("job_id") or "")
            self._jobs.pop(job_id, None)
            self._state_store.delete_document(STATE_NS_ACTION_JOB, job_id)

    def delete_action_job(self, job_id: str) -> bool:
        job_id = str(job_id or "").strip()
        if not job_id:
            return False
        with self._jobs_lock:
            existed = job_id in self._jobs
            self._jobs.pop(job_id, None)
            self._state_store.delete_document(STATE_NS_ACTION_JOB, job_id)
        return existed

    def retry_action_job(self, job_id: str) -> dict[str, Any]:
        job_id = str(job_id or "").strip()
        if not job_id:
            raise PortalError("任务ID不能为空。")
        now_text = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        now_ts = time.time()
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if not isinstance(job, dict):
                stored = self._state_store.get_document(STATE_NS_ACTION_JOB, job_id)
                if isinstance(stored, dict):
                    stored["job_id"] = str(stored.get("job_id") or job_id)
                    self._jobs[job_id] = stored
                    job = stored
            if not isinstance(job, dict):
                raise PortalError("任务不存在或已清理。")
            if str(job.get("phase") or "") != "failed":
                raise PortalError("只能重试失败任务。")
            if not bool(job.get("error_retryable")):
                raise PortalError("该失败类型不可自动重试，请人工核对后重新发起。")
            request_payload = (
                job.get("retry_request")
                if isinstance(job.get("retry_request"), dict)
                else job.get("request")
                if isinstance(job.get("request"), dict)
                else {}
            )
            if not request_payload.get("action") or not request_payload.get("work_type"):
                raise PortalError("任务缺少可重试请求内容，请重新发起通告。")
            job["request"] = copy.deepcopy(request_payload)
            job["phase"] = "accepted"
            job["accepted_at"] = now_ts
            job["message_started_at"] = 0.0
            job["message_finished_at"] = 0.0
            job["upload_queued_at"] = 0.0
            job["upload_started_at"] = 0.0
            job["upload_finished_at"] = 0.0
            job["upload_release_at"] = 0.0
            job["message_queue_position"] = 0
            job["message_queue_size"] = 0
            job["qt_phase"] = ""
            job["qt_queue_position"] = 0
            job["qt_queue_size"] = 0
            job["queue_position"] = 0
            job["queue_size"] = 0
            job["upload_queue_position"] = 0
            job["upload_queue_size"] = 0
            job["error"] = ""
            job["error_category"] = ""
            job["error_retryable"] = False
            job["upload_message"] = ""
            job["retry_count"] = int(job.get("retry_count") or 0) + 1
            job["updated_at"] = now_text
            self._persist_action_job_locked(job)
            return copy.deepcopy(job)

    @staticmethod
    def _job_epoch(job: dict[str, Any]) -> float:
        if not isinstance(job, dict):
            return 0.0
        for key in (
            "upload_finished_at",
            "upload_started_at",
            "upload_queued_at",
            "message_finished_at",
            "accepted_at",
        ):
            try:
                value = float(job.get(key) or 0)
            except Exception:
                value = 0.0
            if value > 0:
                return value
        for key in ("updated_at", "created_at"):
            text = str(job.get(key) or "").strip()
            if not text:
                continue
            try:
                return dt.datetime.strptime(text, "%Y-%m-%d %H:%M").timestamp()
            except Exception:
                continue
        return 0.0

    def cleanup_action_jobs(
        self,
        *,
        success_retention_seconds: int = ACTION_JOB_SUCCESS_RETENTION_SECONDS,
        failed_retention_seconds: int = ACTION_JOB_FAILED_RETENTION_SECONDS,
        max_delete: int = 200,
    ) -> dict[str, int]:
        now = time.time()
        success_retention_seconds = max(60, int(success_retention_seconds or 0))
        failed_retention_seconds = max(60, int(failed_retention_seconds or 0))
        max_delete = max(1, min(int(max_delete or 200), 1000))
        removed_success = 0
        removed_failed = 0
        candidates: list[tuple[float, str, str]] = []
        documents = self._state_store.list_documents(STATE_NS_ACTION_JOB)
        for document in documents:
            job = document.get("payload") if isinstance(document, dict) else {}
            if not isinstance(job, dict):
                continue
            job_id = str(job.get("job_id") or document.get("key") or "").strip()
            phase = str(job.get("phase") or "").strip()
            if not job_id or phase not in {"success", "failed"}:
                continue
            epoch = self._job_epoch(job)
            if not epoch:
                continue
            retention = (
                success_retention_seconds
                if phase == "success"
                else failed_retention_seconds
            )
            if now - epoch >= retention:
                candidates.append((epoch, job_id, phase))
        candidates.sort(key=lambda item: item[0])
        with self._jobs_lock:
            for _, job_id, phase in candidates[:max_delete]:
                self._jobs.pop(job_id, None)
                self._state_store.delete_document(STATE_NS_ACTION_JOB, job_id)
                if phase == "success":
                    removed_success += 1
                else:
                    removed_failed += 1
        return {
            "removed_success": removed_success,
            "removed_failed": removed_failed,
            "removed_total": removed_success + removed_failed,
        }

    @staticmethod
    def _compact_job_request(request: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(request, dict):
            return {}
        keep_keys = (
            "_auth_open_id",
            "scope",
            "action",
            "work_type",
            "notice_type",
            "manual",
            "manual_id",
            "record_id",
            "source_record_id",
            "target_record_id",
            "active_item_id",
            "operation_id",
        )
        return {
            key: request.get(key)
            for key in keep_keys
            if request.get(key) not in (None, "")
        }

    def _compact_completed_job_locked(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if not isinstance(job, dict) or str(job.get("phase") or "") != "success":
            return
        compacted = {
            "job_id": str(job.get("job_id") or job_id),
            "phase": "success",
            "created_at": str(job.get("created_at") or ""),
            "updated_at": str(job.get("updated_at") or ""),
            "accepted_at": float(job.get("accepted_at") or 0),
            "message_started_at": float(job.get("message_started_at") or 0),
            "message_finished_at": float(job.get("message_finished_at") or 0),
            "upload_queued_at": float(job.get("upload_queued_at") or 0),
            "upload_started_at": float(job.get("upload_started_at") or 0),
            "upload_finished_at": float(job.get("upload_finished_at") or 0),
            "upload_release_at": float(job.get("upload_release_at") or 0),
            "request": self._compact_job_request(job.get("request") or {}),
            "operation_id": str(job.get("operation_id") or ""),
            "target_key": str(job.get("target_key") or ""),
            "depends_on_job_id": str(job.get("depends_on_job_id") or ""),
            "depends_on_phase": "",
            "message_sent": bool(job.get("message_sent")),
            "message_signature": str(job.get("message_signature") or ""),
            "message_failed": bool(job.get("message_failed")),
            "message_failed_continue": bool(job.get("message_failed_continue")),
            "message_error": str(job.get("message_error") or ""),
            "message_warning": str(job.get("message_warning") or ""),
            "message_queue_position": 0,
            "message_queue_size": 0,
            "qt_phase": str(job.get("qt_phase") or ""),
            "qt_queue_position": 0,
            "qt_queue_size": 0,
            "queue_position": 0,
            "queue_size": 0,
            "upload_queue_position": 0,
            "upload_queue_size": 0,
            "record_id": str(job.get("record_id") or ""),
            "active_item_id": str(job.get("active_item_id") or ""),
            "error": "",
            "error_category": "",
            "error_retryable": False,
            "upload_message": str(job.get("upload_message") or ""),
            "paired_upload_status": str(job.get("paired_upload_status") or ""),
            "paired_upload_warning": str(job.get("paired_upload_warning") or ""),
            "paired_maintenance_target_record_id": str(
                job.get("paired_maintenance_target_record_id") or ""
            ),
            "notice_text": str(
                job.get("notice_text")
                or (job.get("prepared") or {}).get("text")
                or ""
            ),
            "copy_text": str(
                job.get("copy_text")
                or job.get("notice_text")
                or (job.get("prepared") or {}).get("text")
                or ""
            ),
            "prepared": {},
        }
        self._jobs[job_id] = compacted
        self._persist_action_job_locked(compacted)

    def _compact_failed_job_locked(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if not isinstance(job, dict) or str(job.get("phase") or "") != "failed":
            return
        retryable = bool(job.get("error_retryable"))
        compacted = {
            "job_id": str(job.get("job_id") or job_id),
            "phase": "failed",
            "created_at": str(job.get("created_at") or ""),
            "updated_at": str(job.get("updated_at") or ""),
            "accepted_at": float(job.get("accepted_at") or 0),
            "message_started_at": float(job.get("message_started_at") or 0),
            "message_finished_at": float(job.get("message_finished_at") or 0),
            "upload_queued_at": float(job.get("upload_queued_at") or 0),
            "upload_started_at": float(job.get("upload_started_at") or 0),
            "upload_finished_at": float(job.get("upload_finished_at") or 0),
            "upload_release_at": float(job.get("upload_release_at") or 0),
            "request": self._compact_job_request(job.get("request") or {}),
            "operation_id": str(job.get("operation_id") or ""),
            "target_key": str(job.get("target_key") or ""),
            "depends_on_job_id": str(job.get("depends_on_job_id") or ""),
            "depends_on_phase": str(job.get("depends_on_phase") or ""),
            "message_sent": bool(job.get("message_sent")),
            "message_signature": str(job.get("message_signature") or ""),
            "message_failed": bool(job.get("message_failed")),
            "message_failed_continue": bool(job.get("message_failed_continue")),
            "message_error": str(job.get("message_error") or ""),
            "message_warning": str(job.get("message_warning") or ""),
            "message_queue_position": 0,
            "message_queue_size": 0,
            "qt_phase": str(job.get("qt_phase") or ""),
            "qt_queue_position": 0,
            "qt_queue_size": 0,
            "queue_position": 0,
            "queue_size": 0,
            "upload_queue_position": 0,
            "upload_queue_size": 0,
            "record_id": str(job.get("record_id") or ""),
            "active_item_id": str(job.get("active_item_id") or ""),
            "error": str(job.get("error") or ""),
            "error_category": str(job.get("error_category") or ""),
            "error_retryable": retryable,
            "upload_message": str(job.get("upload_message") or ""),
            "retry_count": int(job.get("retry_count") or 0),
            "notice_text": str(
                job.get("notice_text")
                or (job.get("prepared") or {}).get("text")
                or ""
            ),
            "copy_text": str(
                job.get("copy_text")
                or job.get("notice_text")
                or (job.get("prepared") or {}).get("text")
                or ""
            ),
            "prepared": {},
        }
        if retryable:
            compacted["retry_request"] = copy.deepcopy(job.get("request") or {})
        self._jobs[job_id] = compacted
        self._persist_action_job_locked(compacted)

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return None
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if not job:
                stored = self._state_store.get_document(STATE_NS_ACTION_JOB, job_id)
                if isinstance(stored, dict):
                    stored["job_id"] = str(stored.get("job_id") or job_id)
                    self._jobs[job_id] = stored
                    job = stored
            return copy.deepcopy(job) if job else None

    def mark_job(self, job_id: str, **patch: Any) -> None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return
        persist = bool(patch.pop("_persist", True))
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            phase = str(patch.get("phase") or "").strip()
            prior_phase = str(job.get("phase") or "").strip()
            if phase == "accepted":
                patch.setdefault("error_category", "")
                patch.setdefault("error_retryable", False)
            if phase == "failed" or ("error" in patch and str(patch.get("error") or "").strip()):
                classified = self.classify_job_error(str(patch.get("error") or job.get("error") or ""))
                patch.setdefault("error_category", classified["error_category"])
                patch.setdefault("error_retryable", classified["error_retryable"])
            if phase and phase != prior_phase:
                now_ts = time.time()
                if phase == "accepted" and not job.get("accepted_at"):
                    patch["accepted_at"] = now_ts
                elif phase == "sending_message":
                    patch["message_started_at"] = now_ts
                elif phase in {"message_sent", "qt_queued"}:
                    if not job.get("message_finished_at") and job.get("message_started_at"):
                        patch["message_finished_at"] = now_ts
                elif phase == "upload_queued":
                    if not job.get("message_finished_at") and job.get("message_started_at"):
                        patch["message_finished_at"] = now_ts
                    if not job.get("upload_queued_at"):
                        patch["upload_queued_at"] = now_ts
                elif phase == "upload_waiting":
                    if not job.get("upload_queued_at"):
                        patch["upload_queued_at"] = now_ts
                elif phase == "uploading":
                    if not job.get("upload_queued_at"):
                        patch["upload_queued_at"] = now_ts
                    patch["upload_started_at"] = now_ts
                elif phase in {"success", "failed"}:
                    patch["upload_finished_at"] = now_ts
            changed = any(job.get(key) != value for key, value in patch.items())
            if not changed:
                return
            job.update(patch)
            job["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
            if persist:
                self._persist_action_job_locked(job)
                if str(job.get("phase") or "") == "failed":
                    self._compact_failed_job_locked(job_id)

    @staticmethod
    def _action_message_signature(prepared: dict[str, Any]) -> str:
        payload = {
            "text": str((prepared or {}).get("text") or ""),
            "recipients": sorted(
                {
                    str(open_id or "").strip()
                    for open_id in ((prepared or {}).get("recipients") or [])
                    if str(open_id or "").strip()
                }
            ),
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def prepare_action_job(self, job_id: str) -> dict[str, Any]:
        job = self.get_job(job_id)
        if not job:
            raise PortalError("任务不存在。")
        request_payload = job.get("request") or {}
        prepared = self.prepare_workbench_action(request_payload, job_id=job_id)
        message_signature = self._action_message_signature(prepared)
        prepared["message_signature"] = message_signature
        prepared["message_sent"] = bool(job.get("message_sent")) and (
            str(job.get("message_signature") or "") == message_signature
        )
        self.mark_job(
            job_id,
            prepared=prepared,
            record_id=str(prepared.get("record_id") or ""),
            active_item_id=str(prepared.get("active_item_id") or ""),
            message_signature=message_signature,
        )
        return prepared

    def prepare_workbench_action(
        self, request_payload: dict[str, Any], *, job_id: str
    ) -> dict[str, Any]:
        request_payload = normalize_notice_identity_payload(request_payload)
        work_type = self._manual_payload_notice_work_type(
            request_payload,
            self._request_work_type_fallback(request_payload),
        )
        if work_type != str(request_payload.get("work_type") or "").strip():
            request_payload = {**request_payload, "work_type": work_type}
        if work_type == WORK_TYPE_CHANGE:
            return self.prepare_change_action(request_payload, job_id=job_id)
        if work_type == WORK_TYPE_REPAIR:
            return self.prepare_repair_action(request_payload, job_id=job_id)
        if work_type in {WORK_TYPE_POWER, WORK_TYPE_POLLING, WORK_TYPE_ADJUST}:
            return self.prepare_simple_manual_notice_action(request_payload, job_id=job_id)
        return self.prepare_maintenance_action(request_payload, job_id=job_id)

    @staticmethod
    def _simple_notice_profile(work_type: str) -> dict[str, str]:
        work_type = str(work_type or "").strip()
        if work_type == WORK_TYPE_POWER:
            return {
                "notice_type": NOTICE_TYPE_POWER,
                "heading": "上电通告",
                "status_label": "上电状态",
                "title_label": "名称",
            }
        if work_type == WORK_TYPE_POLLING:
            return {
                "notice_type": NOTICE_TYPE_POLLING,
                "heading": NOTICE_TYPE_POLLING,
                "status_label": "轮巡状态",
                "title_label": "标题",
            }
        if work_type == WORK_TYPE_ADJUST:
            return {
                "notice_type": NOTICE_TYPE_ADJUST,
                "heading": NOTICE_TYPE_ADJUST,
                "status_label": "调整状态",
                "title_label": "名称",
            }
        return {
            "notice_type": NOTICE_TYPE_MAINTENANCE,
            "heading": NOTICE_TYPE_MAINTENANCE,
            "status_label": "状态",
            "title_label": "名称",
        }

    @staticmethod
    def build_simple_notice_text(
        *,
        work_type: str,
        status: str,
        title: str,
        start_time: str,
        end_time: str,
        building: str,
        specialty: str,
        location: str = "",
        content: str = "",
        reason: str = "",
        impact: str = "",
        progress: str = "",
        device: str = "",
        cabinet: str = "",
        quantity: str = "",
        notice_type: str = "",
    ) -> str:
        heading = (
            notice_type
            if work_type == WORK_TYPE_POWER
            and str(notice_type or "").strip() in {NOTICE_TYPE_POWER_UP, NOTICE_TYPE_POWER_DOWN}
            else ""
        )
        return MaintenancePortalService._render_notice_text(
            work_type=work_type,
            status=status,
            heading_override=heading,
            values={
                "title": title,
                "time_range": MaintenancePortalService._format_notice_time_range(
                    start_time, end_time
                ),
                "location": location,
                "content": content,
                "reason": reason,
                "impact": impact,
                "progress": progress,
                "device": device,
                "cabinet": cabinet,
                "quantity": quantity,
            },
        )

    def prepare_simple_manual_notice_action(
        self, request_payload: dict[str, Any], *, job_id: str
    ) -> dict[str, Any]:
        request_payload = normalize_notice_identity_payload(request_payload)
        action = str(request_payload.get("action") or "").strip().lower()
        status_map = {"start": "开始", "update": "更新", "end": "结束"}
        status = status_map.get(action)
        if not status:
            raise PortalError("动作必须是 start/update/end。")
        work_type = str(request_payload.get("work_type") or "").strip()
        if work_type not in {WORK_TYPE_POWER, WORK_TYPE_POLLING, WORK_TYPE_ADJUST}:
            raise PortalError("不支持的手填通告类型。")
        profile = self._simple_notice_profile(work_type)
        request_notice_type = str(request_payload.get("notice_type") or "").strip()
        heading_notice_type = (
            request_notice_type
            if work_type == WORK_TYPE_POWER
            and request_notice_type in {NOTICE_TYPE_POWER_UP, NOTICE_TYPE_POWER_DOWN}
            else str(request_payload.get("power_notice_heading") or "").strip()
        )
        notice_type = profile["notice_type"]
        scope = self._normalize_scope(request_payload.get("scope"))
        manual = self._truthy_flag(request_payload.get("manual"))
        record_id = str(
            request_payload.get("record_id")
            or request_payload.get("manual_id")
            or request_payload.get("active_item_id")
            or ""
        ).strip()
        target_record_id = self._target_record_id_from_request_payload(request_payload)
        if not manual and action == "start":
            raise PortalError(f"{self._history_work_type_label(work_type)}通告目前仅支持前端纯手填或解析发送。")
        if not manual and action != "start" and not target_record_id:
            raise PortalError(f"{self._history_work_type_label(work_type)}通告缺少目标多维 record_id，不能更新/结束。")
        if action != "start" and target_record_id:
            manual = True
        if action == "start" and not record_id:
            raise PortalError("开始通告缺少手填记录ID。")
        if action != "start" and not target_record_id:
            raise PortalError(f"{self._history_work_type_label(work_type)}通告缺少目标多维 record_id，不能更新/结束。")
        title = str(request_payload.get("title") or request_payload.get("content") or "").strip()
        if not title:
            raise PortalError(f"{self._history_work_type_label(work_type)}通告缺少标题。")
        building_codes = self._building_codes_from_request_payload(request_payload)
        if not building_codes:
            building_codes = self._building_codes_from_value(request_payload.get("building"))
        building = (
            str(request_payload.get("building") or "").strip()
            or self._building_label_from_codes(building_codes)
        )
        if not building_codes:
            raise PortalError(f"{self._history_work_type_label(work_type)}通告缺少楼栋。")
        if not self._scope_matches_buildings(scope, building_codes):
            raise PortalError(f"当前入口与通告楼栋不匹配: {building or '-'}")
        start_time = self._format_input_datetime(request_payload.get("start_time"))
        end_time = self._format_input_datetime(request_payload.get("end_time"))
        if not start_time or not end_time:
            raise PortalError("计划开始时间和计划结束时间不能为空。")
        self._validate_minimum_notice_duration(start_time, end_time)
        self._require_end_site_photo(
            request_payload,
            action,
            notice_type=notice_type,
            work_type=work_type,
        )
        specialty = self._clean_source_text(request_payload.get("specialty"))
        location = str(request_payload.get("location") or "").strip()
        content = str(request_payload.get("content") or "").strip()
        reason = str(request_payload.get("reason") or "").strip()
        impact = str(request_payload.get("impact") or "").strip()
        progress = str(request_payload.get("progress") or "").strip()
        device = str(request_payload.get("device") or "").strip()
        cabinet = str(request_payload.get("cabinet") or "").strip()
        quantity = str(request_payload.get("quantity") or "").strip()
        title = self._normalize_110_station_notice_title(
            title,
            building=building,
            building_codes=building_codes,
        )
        text = self.build_simple_notice_text(
            work_type=work_type,
            status=status,
            title=title,
            start_time=start_time,
            end_time=end_time,
            building=building,
            specialty=specialty,
            location=location,
            content=content,
            reason=reason,
            impact=impact,
            progress=progress,
            device=device,
            cabinet=cabinet,
            quantity=quantity,
            notice_type=heading_notice_type,
        )
        building_code, recipients, recipient_error = self._recipients_for_building_codes(
            building_codes,
            fallback_building=building,
        )
        if recipient_error:
            raise PortalError(recipient_error)
        response_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        return {
            "job_id": job_id,
            "work_type": work_type,
            "notice_type": notice_type,
            "manual": True,
            "action": action,
            "status": status,
            "scope": scope,
            "scope_label": self._scope_label(scope),
            "record_id": record_id,
            "target_record_id": target_record_id,
            "active_item_id": str(request_payload.get("active_item_id") or "").strip(),
            "title": title,
            "building": building,
            "building_code": building_code,
            "building_codes": building_codes,
            "target_building": self._building_label_from_codes(building_codes),
            "specialty": specialty,
            "start_time": start_time,
            "end_time": end_time,
            "location": location,
            "content": content,
            "reason": reason,
            "impact": impact,
            "progress": progress,
            "device": device,
            "cabinet": cabinet,
            "quantity": quantity,
            "text": text,
            "extra_images": self._site_photo_payload(request_payload),
            "recipients": recipients,
            "skip_personal_message": False,
            "response_time": response_time,
            "operation_id": str(request_payload.get("operation_id") or "").strip() or job_id,
        }

    def prepare_maintenance_action(
        self, request_payload: dict[str, Any], *, job_id: str
    ) -> dict[str, Any]:
        request_payload = normalize_notice_identity_payload(request_payload)
        action = str(request_payload.get("action") or "").strip().lower()
        status_map = {"start": "开始", "update": "更新", "end": "结束"}
        status = status_map.get(action)
        if not status:
            raise PortalError("动作必须是 start/update/end。")
        self.ensure_snapshot_loaded()
        scope = self._normalize_scope(request_payload.get("scope"))
        specialty = self._clean_source_text(request_payload.get("specialty"))
        manual = self._truthy_flag(request_payload.get("manual"))

        record = None
        fields: dict[str, Any] = {}
        record_id = str(
            request_payload.get("record_id")
            or (request_payload.get("manual_id") if manual else "")
            or ""
        ).strip()
        source_record_id = str(request_payload.get("source_record_id") or "").strip()
        target_record_id = self._target_record_id_from_request_payload(
            request_payload,
            source_record_id=source_record_id,
        )
        active_item_id = str(request_payload.get("active_item_id") or "").strip()
        plan_month = ""
        maintenance_total = ""
        maintenance_no = ""
        maintenance_project = ""
        maintenance_cycle = str(request_payload.get("maintenance_cycle") or "").strip()
        if action == "start":
            if not record_id:
                raise PortalError("开始通告缺少计划记录ID。")
            if manual:
                building = str(request_payload.get("building") or "").strip()
                title = str(request_payload.get("title") or "").strip()
                if not title:
                    raise PortalError("纯手填维保通告缺少名称。")
                if not building:
                    raise PortalError("纯手填维保通告缺少楼栋。")
                if not specialty or specialty == "全部":
                    raise PortalError("纯手填维保通告缺少专业。")
                if not maintenance_cycle:
                    raise PortalError("纯手填维保通告必须选择维保周期。")
            else:
                record = self._find_record_by_id(record_id)
                fields = record.get("display_fields") or {}
                current_status = str(fields.get("维护实施状态") or "").strip()
                if current_status != DEFAULT_MAINTENANCE_STATUS:
                    raise PortalError(
                        f"该计划维护项当前状态不是{DEFAULT_MAINTENANCE_STATUS}: {current_status or '-'}"
                    )
                building = str(fields.get("楼栋") or "").strip()
                maintenance_total = str(fields.get("维护总项") or "").strip()
                plan_month = str(fields.get("计划维护月份") or "").strip()
                maintenance_no = str(fields.get("维护编号") or "").strip()
                maintenance_project = str(fields.get("维护项目") or "").strip()
                maintenance_cycle = str(fields.get("维护周期") or maintenance_cycle).strip()
                if not specialty or specialty == "全部":
                    specialty = str(fields.get("专业类别") or "").strip()
                if not specialty or specialty == "全部":
                    raise PortalError("该计划维护项缺少专业类别，无法上传。")
                title = f"EA118机房{building}{maintenance_total}"
        else:
            if source_record_id:
                try:
                    record = self._find_record_by_id(source_record_id)
                    fields = record.get("display_fields") or {}
                except PortalError:
                    record = None
                    fields = {}
            if record is not None:
                building = str(request_payload.get("building") or fields.get("楼栋") or "").strip()
                maintenance_total = str(fields.get("维护总项") or "").strip()
                plan_month = str(fields.get("计划维护月份") or "").strip()
                maintenance_no = str(fields.get("维护编号") or "").strip()
                maintenance_project = str(fields.get("维护项目") or "").strip()
                maintenance_cycle = str(fields.get("维护周期") or maintenance_cycle).strip()
                if not target_record_id:
                    target_record_id = self._resolve_target_record_id_for_source_update(
                        work_type=WORK_TYPE_MAINTENANCE,
                        notice_type=NOTICE_TYPE_MAINTENANCE,
                        source_record=record,
                    )
            else:
                building = str(request_payload.get("building") or "").strip()
            building = str(request_payload.get("building") or building or "").strip()
            title = str(request_payload.get("title") or "").strip()
            if not title:
                raise PortalError("更新/结束通告缺少名称。")
            if not target_record_id:
                target_record_id = self._target_record_id_from_identity_map(
                    work_type=WORK_TYPE_MAINTENANCE,
                    active_item_id=active_item_id,
                    source_record_id=source_record_id,
                    target_record_id=self._target_record_id_from_request_payload(
                        request_payload,
                        source_record_id=source_record_id,
                    ),
                )
            if not target_record_id and active_item_id:
                target_record_id = self._target_record_id_from_work_status(
                    work_type=WORK_TYPE_MAINTENANCE,
                    active_item_id=active_item_id,
                )
            if not target_record_id and source_record_id:
                target_record_id = self._target_record_id_from_work_status(
                    work_type=WORK_TYPE_MAINTENANCE,
                    source_record_id=source_record_id,
                )
            if not target_record_id:
                target_record_id = self._target_record_id_from_unique_candidate(
                    work_type=WORK_TYPE_MAINTENANCE,
                    scope=scope,
                    title=title,
                    start_time=self._format_input_datetime(request_payload.get("start_time")),
                    end_time=self._format_input_datetime(request_payload.get("end_time")),
                    action=action,
                )
            if not target_record_id:
                target_record_id = self._target_record_id_from_request_payload(
                    request_payload,
                    source_record_id=source_record_id,
                )
            record_id = source_record_id or record_id
            if action != "start" and not target_record_id:
                raise PortalError(
                    "该维保源记录未找到目标维保通告表 record_id，不能更新/结束。"
                )

        if not self._scope_matches_building(scope, building):
            raise PortalError(f"当前楼栋入口与通告楼栋不匹配: {building or '-'}")
        title = self._normalize_110_station_notice_title(
            title,
            building=building,
            building_codes=self._building_codes_from_value(building),
        )

        start_time = self._format_input_datetime(request_payload.get("start_time"))
        end_time = self._format_input_datetime(request_payload.get("end_time"))
        if not start_time or not end_time:
            raise PortalError("开始时间和结束时间不能为空。")
        self._validate_minimum_notice_duration(start_time, end_time)
        self._require_end_site_photo(
            request_payload,
            action,
            notice_type=NOTICE_TYPE_MAINTENANCE,
            work_type=WORK_TYPE_MAINTENANCE,
        )
        location = str(request_payload.get("location") or "").strip()
        content = str(request_payload.get("content") or "").strip()
        reason = str(request_payload.get("reason") or "").strip()
        impact = str(request_payload.get("impact") or "").strip() or DEFAULT_IMPACT_TEXT
        progress = (
            str(request_payload.get("progress") or "").strip()
            or (DEFAULT_PROGRESS_TEXT if action == "start" else "")
        )
        if action == "start":
            memory_total = (
                str(maintenance_total or fields.get("维护总项") or "").strip()
                or str(request_payload.get("title") or "").strip()
            )
            if not maintenance_total:
                maintenance_total = memory_total
            self._remember_draft_fields(
                building=building,
                maintenance_total=memory_total,
                location=location,
                content=content,
                reason=reason,
                impact=impact,
                maintenance_cycle=maintenance_cycle,
            )

        text = self.build_notice_text(
            status=status,
            title=title,
            start_time=start_time,
            end_time=end_time,
            location=location,
            content=content,
            reason=reason,
            impact=impact,
            progress=progress,
        )
        building_code, recipients, recipient_error = self._recipients_for_building(
            building
        )
        if recipient_error:
            raise PortalError(recipient_error)

        response_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        return {
            "job_id": job_id,
            "work_type": WORK_TYPE_MAINTENANCE,
            "notice_type": NOTICE_TYPE_MAINTENANCE,
            "source_app_token": self.app_token if (record_id and not manual) else "",
            "source_table_id": self.table_id if (record_id and not manual) else "",
            "target_app_token": str(config.app_token or "").strip(),
            "target_table_id": str(config.get_table_id(NOTICE_TYPE_MAINTENANCE) or "").strip(),
            "manual": manual,
            "action": action,
            "status": status,
            "scope": scope,
            "scope_label": self._scope_label(scope),
            "record_id": record_id,
            "target_record_id": target_record_id,
            "active_item_id": active_item_id,
            "title": title,
            "building": building,
            "building_code": building_code,
            "building_codes": [building_code] if building_code else [],
            "target_building": self._scope_label(building_code),
            "specialty": specialty,
            "plan_month": plan_month,
            "maintenance_total": maintenance_total,
            "maintenance_no": maintenance_no,
            "maintenance_project": maintenance_project,
            "maintenance_cycle": maintenance_cycle,
            "source_progress": (
                self._maintenance_status_value(record)
                if record is not None
                else str(request_payload.get("source_progress") or "")
            ),
            "start_time": start_time,
            "end_time": end_time,
            "location": location,
            "content": content,
            "reason": reason,
            "impact": impact,
            "progress": progress,
            "text": text,
            "extra_images": self._site_photo_payload(request_payload),
            "recipients": recipients,
            "response_time": response_time,
            "operation_id": str(request_payload.get("operation_id") or "").strip()
            or job_id,
        }

    @staticmethod
    def build_change_notice_text(
        *,
        status: str,
        title: str,
        level: str,
        start_time: str,
        end_time: str,
        location: str,
        content: str,
        reason: str,
        impact: str,
        progress: str,
    ) -> str:
        return MaintenancePortalService._render_notice_text(
            work_type=WORK_TYPE_CHANGE,
            status=status,
            values={
                "title": title,
                "level": level,
                "time_range": MaintenancePortalService._format_notice_time_range(
                    start_time, end_time
                ),
                "location": location,
                "content": content,
                "reason": reason,
                "impact": str(impact or "").strip() or DEFAULT_IMPACT_TEXT,
                "progress": str(progress or "").strip() or DEFAULT_PROGRESS_TEXT,
            },
        )

    @staticmethod
    def _default_change_level(level: Any) -> str:
        return str(level or "").strip() or CHANGE_DEFAULT_LEVEL

    def _sync_maintenance_target_requested(
        self, request_payload: dict[str, Any], *, source_work_type: str
    ) -> bool:
        if source_work_type != WORK_TYPE_MAINTENANCE:
            return False
        if "sync_maintenance_target" not in request_payload:
            return True
        return self._truthy_flag(request_payload.get("sync_maintenance_target"))

    def _build_paired_maintenance_upload(
        self,
        *,
        request_payload: dict[str, Any],
        job_id: str,
        action: str,
        status: str,
        source_record_id: str,
        target_record_id: str,
        active_item_id: str,
        original_title: str,
        building: str,
        building_codes: list[str],
        specialty: str,
        maintenance_cycle: str,
        start_time: str,
        end_time: str,
        location: str,
        content: str,
        reason: str,
        impact: str,
        progress: str,
        response_time: str,
    ) -> dict[str, Any]:
        paired_actual_start = str(
            request_payload.get("paired_maintenance_actual_start_time") or ""
        ).strip()
        if action == "start" and not paired_actual_start:
            paired_actual_start = response_time
        title = self._normalize_110_station_notice_title(
            original_title or request_payload.get("paired_maintenance_original_title") or "",
            building=building,
            building_codes=building_codes,
        )
        if not title:
            title = self._normalize_110_station_notice_title(
                request_payload.get("title") or "",
                building=building,
                building_codes=building_codes,
            )
        text = self.build_notice_text(
            status=status,
            title=title,
            start_time=start_time,
            end_time=end_time,
            location=location,
            content=content,
            reason=reason,
            impact=impact,
            progress=progress,
        )
        return {
            "job_id": f"{job_id}:paired-maintenance",
            "work_type": WORK_TYPE_MAINTENANCE,
            "notice_type": NOTICE_TYPE_MAINTENANCE,
            "source_work_type": WORK_TYPE_MAINTENANCE,
            "source_app_token": self.app_token if source_record_id else "",
            "source_table_id": self.table_id if source_record_id else "",
            "target_app_token": str(config.app_token or "").strip(),
            "target_table_id": str(config.get_table_id(NOTICE_TYPE_MAINTENANCE) or "").strip(),
            "manual": False,
            "action": action,
            "status": status,
            "scope": self._normalize_scope(request_payload.get("scope")),
            "scope_label": self._scope_label(self._normalize_scope(request_payload.get("scope"))),
            "record_id": source_record_id,
            "source_record_id": source_record_id,
            "target_record_id": target_record_id,
            "active_item_id": active_item_id,
            "title": title,
            "building": building,
            "buildings": [building] if building else [],
            "building_code": (
                "CAMPUS"
                if len(building_codes) >= 2
                else (building_codes[0] if building_codes else "")
            ),
            "building_codes": building_codes,
            "target_building": self._building_label_from_codes(building_codes),
            "specialty": specialty,
            "maintenance_cycle": maintenance_cycle,
            "start_time": start_time,
            "end_time": end_time,
            "location": location,
            "content": content,
            "reason": reason,
            "impact": impact,
            "progress": progress,
            "text": text,
            "extra_images": self._site_photo_payload(request_payload),
            "recipients": [],
            "skip_personal_message": True,
            "response_time": response_time,
            "paired_maintenance_actual_start_time": paired_actual_start,
            "operation_id": (
                str(request_payload.get("operation_id") or "").strip() or job_id
            )
            + ":paired-maintenance",
        }

    def prepare_change_action(
        self, request_payload: dict[str, Any], *, job_id: str
    ) -> dict[str, Any]:
        request_payload = normalize_notice_identity_payload(request_payload)
        action = str(request_payload.get("action") or "").strip().lower()
        status_map = {"start": "开始", "update": "更新", "end": "结束"}
        status = status_map.get(action)
        if not status:
            raise PortalError("动作必须是 start/update/end。")
        self.ensure_snapshot_loaded()
        scope = self._normalize_scope(request_payload.get("scope"))
        manual = self._truthy_flag(request_payload.get("manual"))
        source_work_type = str(
            request_payload.get("source_work_type")
            or request_payload.get("converted_from_work_type")
            or WORK_TYPE_CHANGE
        ).strip()
        if source_work_type not in {WORK_TYPE_MAINTENANCE, WORK_TYPE_CHANGE}:
            source_work_type = WORK_TYPE_CHANGE

        record_id = str(
            request_payload.get("record_id")
            or (request_payload.get("manual_id") if manual else "")
            or ""
        ).strip()
        source_record_id = str(request_payload.get("source_record_id") or "").strip()
        target_record_id = ""
        fields: dict[str, Any] = {}
        paired_maintenance_original_title = str(
            request_payload.get("paired_maintenance_original_title") or ""
        ).strip()
        paired_maintenance_target_record_id = str(
            request_payload.get("paired_maintenance_target_record_id") or ""
        ).strip()
        maintenance_cycle = str(request_payload.get("maintenance_cycle") or "").strip()
        if action == "start":
            if not record_id:
                raise PortalError("开始通告缺少变更源记录ID。")
            if manual:
                title = str(request_payload.get("title") or "").strip()
                if not title:
                    raise PortalError("纯手填变更通告缺少名称。")
                building_codes = [
                    str(code or "").strip().upper()
                    for code in (request_payload.get("building_codes") or [])
                    if str(code or "").strip()
                ]
                if not building_codes:
                    building_codes = self._building_codes_from_value(
                        request_payload.get("building")
                    )
                building = (
                    str(request_payload.get("building") or "").strip()
                    or self._building_label_from_codes(building_codes)
                )
                specialty = self._clean_source_text(request_payload.get("specialty"))
                level = str(request_payload.get("level") or "").strip()
                default_start = ""
                default_end = ""
                source_progress = ""
            elif source_work_type == WORK_TYPE_MAINTENANCE:
                record = self._find_record_by_id(record_id, WORK_TYPE_MAINTENANCE)
                source_record_id = record_id
                fields = record.get("display_fields") or {}
                source_progress = self._maintenance_status_value(record)
                if source_progress != DEFAULT_MAINTENANCE_STATUS:
                    raise PortalError(
                        f"该维保源记录当前状态不是{DEFAULT_MAINTENANCE_STATUS}: {source_progress or '-'}"
                    )
                title = (
                    str(request_payload.get("title") or "").strip()
                    or self._maintenance_title(record)
                )
                paired_maintenance_original_title = (
                    paired_maintenance_original_title or self._maintenance_title(record)
                )
                source_codes = self._building_codes_from_value(fields.get("楼栋"))
                building_codes = self._resolve_scoped_source_building_codes(
                    scope=scope,
                    work_type=WORK_TYPE_MAINTENANCE,
                    record_id=record_id,
                    source_codes=source_codes,
                    payload_codes=self._building_codes_from_request_payload(
                        request_payload
                    ),
                )
                building = (
                    str(request_payload.get("building") or "").strip()
                    or self._building_label_from_codes(building_codes)
                )
                specialty = (
                    self._clean_source_text(request_payload.get("specialty"))
                    or self._clean_source_text(fields.get("专业类别"))
                )
                maintenance_cycle = str(fields.get("维护周期") or maintenance_cycle).strip()
                level = str(request_payload.get("level") or "").strip()
                default_start = ""
                default_end = ""
            else:
                record = self._find_record_by_id(record_id, WORK_TYPE_CHANGE)
                source_record_id = record_id
                fields = record.get("display_fields") or {}
                source_progress = self._change_progress_value(record)
                if source_progress != CHANGE_PROGRESS_NOT_STARTED:
                    raise PortalError(
                        f"该变更当前源进度不是{CHANGE_PROGRESS_NOT_STARTED}: {source_progress or '-'}"
                    )
                title = self._change_title(record)
                building_codes = self._resolve_scoped_source_building_codes(
                    scope=scope,
                    work_type=WORK_TYPE_CHANGE,
                    record_id=record_id,
                    source_codes=self._change_record_building_codes(record),
                    payload_codes=self._building_codes_from_request_payload(
                        request_payload
                    ),
                )
                building = self._building_label_from_codes(building_codes)
                specialty = self._change_specialty(record)
                level = str(fields.get("变更等级（阿里）") or "").strip()
                default_start, default_end, _ = self._change_time_range(record)
        else:
            active_item_id = str(request_payload.get("active_item_id") or "").strip()
            source_record = None
            if source_record_id:
                try:
                    source_record = self._find_record_by_id(
                        source_record_id, source_work_type
                    )
                    fields = source_record.get("display_fields") or {}
                except PortalError:
                    source_record = None
                    fields = {}
            title = str(request_payload.get("title") or "").strip()
            if not title and source_record is not None:
                title = (
                    self._maintenance_title(source_record)
                    if source_work_type == WORK_TYPE_MAINTENANCE
                    else self._change_title(source_record)
                )
            if source_work_type == WORK_TYPE_MAINTENANCE:
                if source_record is not None:
                    paired_maintenance_original_title = (
                        paired_maintenance_original_title
                        or self._maintenance_title(source_record)
                    )
                    maintenance_cycle = str(
                        fields.get("维护周期") or maintenance_cycle
                    ).strip()
                else:
                    paired_maintenance_original_title = (
                        paired_maintenance_original_title or title
                    )
            if not title:
                raise PortalError("更新/结束通告缺少名称。")
            payload_building_codes = self._building_codes_from_request_payload(
                request_payload
            )
            source_building_codes = (
                (
                    self._building_codes_from_value(fields.get("楼栋"))
                    if source_work_type == WORK_TYPE_MAINTENANCE
                    else self._change_record_building_codes(source_record)
                )
                if source_record is not None
                else []
            )
            building_codes = payload_building_codes or source_building_codes
            if source_record is not None:
                building_codes = self._resolve_scoped_source_building_codes(
                    scope=scope,
                    work_type=source_work_type,
                    record_id=source_record_id,
                    source_codes=source_building_codes,
                    payload_codes=payload_building_codes,
                )
            if not building_codes:
                building_codes = self._building_codes_from_value(request_payload.get("building"))
            building = (
                str(request_payload.get("building") or "").strip()
                or self._building_label_from_codes(building_codes)
            )
            specialty = self._clean_source_text(request_payload.get("specialty"))
            if not specialty and source_record is not None:
                specialty = (
                    self._clean_source_text(fields.get("专业类别"))
                    if source_work_type == WORK_TYPE_MAINTENANCE
                    else self._change_specialty(source_record)
                )
            level = str(request_payload.get("level") or "").strip()
            if not level and source_record is not None:
                level = (
                    ""
                    if source_work_type == WORK_TYPE_MAINTENANCE
                    else str(fields.get("变更等级（阿里）") or "").strip()
                )
            source_progress = str(request_payload.get("source_progress") or "").strip()
            if not source_progress and source_record is not None:
                source_progress = (
                    self._maintenance_status_value(source_record)
                    if source_work_type == WORK_TYPE_MAINTENANCE
                    else self._change_progress_value(source_record)
                )
            default_start = ""
            default_end = ""
            record_id = source_record_id
            target_record_id = self._target_record_id_from_request_payload(
                request_payload,
                source_record_id=source_record_id,
            )
            if not target_record_id:
                target_record_id = self._target_record_id_from_identity_map(
                    work_type=WORK_TYPE_CHANGE,
                    active_item_id=active_item_id,
                    source_record_id=source_record_id,
                    target_record_id=self._target_record_id_from_request_payload(
                        request_payload,
                        source_record_id=source_record_id,
                    ),
                )
            if (
                not target_record_id
                and source_record is not None
                and source_work_type == WORK_TYPE_CHANGE
            ):
                target_record_id = self._resolve_target_record_id_for_source_update(
                    work_type=WORK_TYPE_CHANGE,
                    notice_type=NOTICE_TYPE_CHANGE,
                    source_record=source_record,
                )
            if not target_record_id and active_item_id:
                target_record_id = self._target_record_id_from_work_status(
                    work_type=WORK_TYPE_CHANGE,
                    active_item_id=active_item_id,
                )
            if not target_record_id:
                target_record_id = self._target_record_id_from_unique_candidate(
                    work_type=WORK_TYPE_CHANGE,
                    scope=scope,
                    title=title,
                    start_time=self._format_input_datetime(request_payload.get("start_time")),
                    end_time=self._format_input_datetime(request_payload.get("end_time")),
                    action=action,
                )
            if not target_record_id:
                target_record_id = self._target_record_id_from_request_payload(
                    request_payload,
                    source_record_id=source_record_id,
                )

        if not self._scope_matches_buildings(scope, building_codes):
            raise PortalError(f"当前入口与通告楼栋不匹配: {building or '-'}")
        if action != "start" and source_progress == CHANGE_PROGRESS_ENDED:
            raise PortalError("该变更当前源进度已结束，不能更新/结束。")
        if action != "start" and not target_record_id:
            raise PortalError("该变更缺少目标设备变更表 record_id，不能更新/结束。")
        title = self._normalize_110_station_notice_title(
            title,
            building=building,
            building_codes=building_codes,
        )
        if source_work_type == WORK_TYPE_MAINTENANCE:
            paired_maintenance_original_title = self._normalize_110_station_notice_title(
                paired_maintenance_original_title or title,
                building=building,
                building_codes=building_codes,
            )
        level = self._default_change_level(level)
        zhihang_involved = self._truthy_flag(request_payload.get("zhihang_involved"))
        zhihang_record_id = str(request_payload.get("zhihang_record_id") or "").strip()
        zhihang_title = str(request_payload.get("zhihang_title") or "").strip()
        zhihang_progress = str(request_payload.get("zhihang_progress") or "").strip()
        if action == "start" and zhihang_involved:
            if not zhihang_record_id:
                raise PortalError("该变更已标记涉及智航，请选择右侧智航侧变更记录。")
            zhihang_record = self._find_zhihang_change_record_by_id(zhihang_record_id)
            zhihang_progress = self._zhihang_change_progress(zhihang_record)
            if zhihang_progress == ZHIHANG_PROGRESS_ENDED:
                raise PortalError("所选智航侧变更已结束，不能绑定。")
            if not self._scope_matches_zhihang_change_record(scope, zhihang_record):
                raise PortalError("所选智航侧变更与当前入口不匹配。")
            if zhihang_record_id in self._linked_zhihang_record_ids():
                raise PortalError("所选智航侧变更已绑定到其他阿里侧变更。")
            zhihang_title = self._zhihang_change_title(zhihang_record)

        start_time = (
            self._format_input_datetime(request_payload.get("start_time"))
            or default_start
        )
        end_time = (
            self._format_input_datetime(request_payload.get("end_time"))
            or default_end
        )
        if not start_time or not end_time:
            raise PortalError("开始时间和结束时间不能为空。")
        self._validate_minimum_notice_duration(start_time, end_time)
        self._require_end_site_photo(
            request_payload,
            action,
            notice_type=NOTICE_TYPE_CHANGE,
            work_type=WORK_TYPE_CHANGE,
        )
        location = str(request_payload.get("location") or "").strip()
        content = str(request_payload.get("content") or "").strip() or title
        reason = str(request_payload.get("reason") or "").strip()
        impact = str(request_payload.get("impact") or "").strip() or DEFAULT_IMPACT_TEXT
        progress = (
            str(request_payload.get("progress") or "").strip()
            or (DEFAULT_PROGRESS_TEXT if action == "start" else "")
        )
        if action == "start":
            self._remember_draft_fields(
                work_type=WORK_TYPE_CHANGE,
                building=building,
                maintenance_total=title,
                item_name=title,
                location=location,
                content=content,
                reason=reason,
                impact=impact,
                extra_fields={
                    "specialty": specialty,
                    "level": level,
                    "zhihang_involved": "1" if zhihang_involved else "",
                    "zhihang_record_id": zhihang_record_id,
                    "zhihang_title": zhihang_title,
                    "zhihang_progress": zhihang_progress,
                },
            )
        building_code = "CAMPUS" if len(building_codes) >= 2 else (building_codes[0] if building_codes else "")
        text = self.build_change_notice_text(
            status=status,
            title=title,
            level=level,
            start_time=start_time,
            end_time=end_time,
            location=location,
            content=content,
            reason=reason,
            impact=impact,
            progress=progress,
        )
        _, recipients, recipient_error = self._recipients_for_building_codes(
            building_codes,
            fallback_building=building,
        )
        if recipient_error:
            raise PortalError(recipient_error)
        skip_personal_message = False
        response_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        sync_maintenance_target = self._sync_maintenance_target_requested(
            request_payload,
            source_work_type=source_work_type,
        )
        paired_maintenance_upload: dict[str, Any] = {}
        paired_maintenance_actual_start_time = str(
            request_payload.get("paired_maintenance_actual_start_time") or ""
        ).strip()
        if sync_maintenance_target:
            paired_maintenance_upload = self._build_paired_maintenance_upload(
                request_payload=request_payload,
                job_id=job_id,
                action=action,
                status=status,
                source_record_id=source_record_id or record_id,
                target_record_id=paired_maintenance_target_record_id,
                active_item_id=str(request_payload.get("active_item_id") or "").strip(),
                original_title=paired_maintenance_original_title or title,
                building=building,
                building_codes=building_codes,
                specialty=specialty,
                maintenance_cycle=maintenance_cycle,
                start_time=start_time,
                end_time=end_time,
                location=location,
                content=content,
                reason=reason,
                impact=impact,
                progress=progress,
                response_time=response_time,
            )
            paired_maintenance_actual_start_time = str(
                paired_maintenance_upload.get("paired_maintenance_actual_start_time")
                or paired_maintenance_actual_start_time
            ).strip()
        return {
            "job_id": job_id,
            "work_type": WORK_TYPE_CHANGE,
            "notice_type": NOTICE_TYPE_CHANGE,
            "source_work_type": source_work_type,
            "source_app_token": (
                self.app_token
                if (record_id and not manual and source_work_type == WORK_TYPE_MAINTENANCE)
                else CHANGE_SOURCE_APP_TOKEN
                if (record_id and not manual)
                else ""
            ),
            "source_table_id": (
                self.table_id
                if (record_id and not manual and source_work_type == WORK_TYPE_MAINTENANCE)
                else CHANGE_SOURCE_TABLE_ID
                if (record_id and not manual)
                else ""
            ),
            "target_app_token": str(config.app_token or "").strip(),
            "target_table_id": str(config.get_table_id(NOTICE_TYPE_CHANGE) or "").strip(),
            "manual": manual,
            "action": action,
            "status": status,
            "scope": scope,
            "scope_label": self._scope_label(scope),
            "record_id": record_id,
            "target_record_id": target_record_id,
            "active_item_id": str(request_payload.get("active_item_id") or "").strip(),
            "title": title,
            "building": building,
            "building_code": building_code,
            "building_codes": building_codes,
            "target_building": self._building_label_from_code(building_code),
            "specialty": specialty,
            "level": level,
            "source_progress": source_progress,
            "zhihang_involved": zhihang_involved,
            "zhihang_record_id": zhihang_record_id,
            "zhihang_title": zhihang_title,
            "zhihang_progress": zhihang_progress,
            "zhihang_source_app_token": ZHIHANG_CHANGE_APP_TOKEN if zhihang_record_id else "",
            "zhihang_source_table_id": ZHIHANG_CHANGE_TABLE_ID if zhihang_record_id else "",
            "sync_maintenance_target": sync_maintenance_target,
            "paired_maintenance_target_record_id": paired_maintenance_target_record_id,
            "paired_maintenance_original_title": paired_maintenance_original_title,
            "paired_maintenance_actual_start_time": paired_maintenance_actual_start_time,
            "paired_maintenance_upload": paired_maintenance_upload,
            "paired_upload_status": "pending" if paired_maintenance_upload else "skipped",
            "start_time": start_time,
            "end_time": end_time,
            "location": location,
            "content": content,
            "reason": reason,
            "impact": impact,
            "progress": progress,
            "text": text,
            "extra_images": self._site_photo_payload(request_payload),
            "recipients": recipients,
            "skip_personal_message": skip_personal_message,
            "response_time": response_time,
            "operation_id": str(request_payload.get("operation_id") or "").strip()
            or job_id,
        }

    @staticmethod
    def _combine_title_supplement(title: Any, supplement: Any) -> str:
        base = str(title or "").strip()
        extra = str(supplement or "").strip()
        if not base:
            return extra
        if not extra or extra in base:
            return base
        return f"{base}{extra}"

    @staticmethod
    def build_repair_notice_text(
        *,
        status: str,
        title: str,
        specialty: str,
        level: str,
        start_time: str,
        end_time: str,
        location: str,
        content: str,
        reason: str,
        impact: str,
        progress: str,
        repair_device: str,
        repair_fault: str,
        fault_type: str,
        repair_mode: str,
        discovery: str,
        symptom: str,
        solution: str,
        spare_parts: str,
        fault_time: str,
        expected_time: str,
    ) -> str:
        progress_text = str(progress or "").strip()
        reason_text = str(reason or "").strip()
        impact_text = str(impact or "").strip()
        title_text = MaintenancePortalService._combine_title_supplement(title, content)
        return MaintenancePortalService._render_notice_text(
            work_type=WORK_TYPE_REPAIR,
            status=status,
            values={
                "title": title_text,
                "location": location,
                "level": level,
                "specialty": specialty,
                "fault_time": MaintenancePortalService._format_input_datetime(fault_time),
                "expected_time": MaintenancePortalService._format_input_datetime(expected_time),
                "repair_device": repair_device,
                "repair_fault": repair_fault,
                "fault_type": fault_type,
                "repair_mode": repair_mode,
                "impact": impact_text,
                "discovery": discovery,
                "symptom": symptom,
                "reason": reason_text,
                "solution": solution,
                "spare_parts": spare_parts,
                "progress": progress_text,
            },
        )

    def prepare_repair_action(
        self, request_payload: dict[str, Any], *, job_id: str
    ) -> dict[str, Any]:
        request_payload = normalize_notice_identity_payload(request_payload)
        action = str(request_payload.get("action") or "").strip().lower()
        status_map = {"start": "开始", "update": "更新", "end": "结束"}
        status = status_map.get(action)
        if not status:
            raise PortalError("动作必须是 start/update/end。")
        self.ensure_snapshot_loaded()
        scope = self._normalize_scope(request_payload.get("scope"))

        def request_text(name: str, default: str = "") -> str:
            if name in request_payload:
                return self._clean_source_text(request_payload.get(name))
            return self._clean_source_text(default)

        def request_first_text(names: tuple[str, ...], default: str = "") -> str:
            for name in names:
                if name in request_payload:
                    value = str(request_payload.get(name) or "").strip()
                    if value:
                        return value
            return str(default or "").strip()

        record_id = str(request_payload.get("record_id") or "").strip()
        manual = self._truthy_flag(request_payload.get("manual"))
        if manual and not record_id:
            record_id = str(request_payload.get("manual_id") or "").strip()
        fields: dict[str, Any] = {}
        record = None
        source_record = None
        if action == "start":
            if not record_id:
                raise PortalError("开始通告缺少检修源记录ID。")
            if manual:
                title = request_first_text(("title", "content"))
                if not title:
                    raise PortalError("纯手填检修通告缺少标题。")
                building_codes = [
                    str(code or "").strip().upper()
                    for code in (request_payload.get("building_codes") or [])
                    if str(code or "").strip()
                ]
                if not building_codes:
                    building_codes = self._repair_building_codes_from_value(
                        request_payload.get("building")
                    )
                building = (
                    str(request_payload.get("building") or "").strip()
                    or self._building_label_from_codes(building_codes)
                )
                specialty = self._clean_source_text(request_payload.get("specialty"))
                level = request_text("level")
                default_start = ""
                default_end = ""
                repair_device = request_text("repair_device")
                repair_fault = request_text("repair_fault")
                fault_type = request_text("fault_type")
                repair_mode = request_text("repair_mode")
                discovery = request_text("discovery")
                symptom = request_text("symptom")
                solution = request_text("solution")
                spare_parts = request_text("spare_parts")
                fault_time = request_text("fault_time")
                target_record_id = ""
            else:
                try:
                    record = self._find_record_by_id(record_id, WORK_TYPE_REPAIR)
                except PortalError as exc:
                    record = None
                    fields = {}
                    title = request_first_text(("title", "content"))
                    building_codes = [
                        str(code or "").strip().upper()
                        for code in (request_payload.get("building_codes") or [])
                        if str(code or "").strip()
                    ]
                    if not building_codes:
                        building_codes = self._repair_building_codes_from_value(
                            request_payload.get("building")
                        )
                    if not title or not building_codes:
                        raise PortalError(
                            "未找到检修源记录，且前端字段不足，不能发起检修通告。"
                        ) from exc
                    source_progress = request_text(
                        "source_progress", DEFAULT_MAINTENANCE_STATUS
                    ) or DEFAULT_MAINTENANCE_STATUS
                    if source_progress != DEFAULT_MAINTENANCE_STATUS:
                        raise PortalError(
                            f"该检修当前源状态不是{DEFAULT_MAINTENANCE_STATUS}: {source_progress or '-'}"
                        ) from exc
                    building = (
                        str(request_payload.get("building") or "").strip()
                        or self._building_label_from_codes(building_codes)
                    )
                    specialty = self._clean_source_text(request_payload.get("specialty"))
                    level = request_text("level")
                    default_start = ""
                    default_end = ""
                    repair_device = request_text("repair_device")
                    repair_fault = request_text("repair_fault")
                    fault_type = request_text("fault_type") or "设备故障"
                    repair_mode = request_text("repair_mode")
                    discovery = request_text("discovery")
                    symptom = request_text("symptom")
                    solution = request_text("solution")
                    spare_parts = request_text("spare_parts")
                    fault_time = request_text("fault_time")
                    target_record_id = ""
                else:
                    fields = record.get("display_fields") or {}
                    if not self._is_valid_repair_record(record):
                        raise PortalError("该检修记录缺少有效检修通告名称/维修名称或楼栋，不能发起。")
                    source_progress = self._repair_source_status(record)
                    if source_progress != DEFAULT_MAINTENANCE_STATUS:
                        raise PortalError(
                            f"该检修当前源状态不是{DEFAULT_MAINTENANCE_STATUS}: {source_progress or '-'}"
                        )
                    title = request_first_text(
                        ("title", "content"), default=self._repair_title(record)
                    )
                    building_codes = self._repair_record_building_codes(record)
                    building = self._building_label_from_codes(building_codes)
                    specialty = self._clean_source_text(request_payload.get("specialty"))
                    if not specialty or specialty == "全部":
                        specialty = self._repair_specialty(record)
                    level = request_text("level", self._repair_level(fields))
                    default_start, default_end, _ = self._repair_time_range(record)
                    repair_device = request_text("repair_device", self._repair_device_text(fields))
                    repair_fault = request_text(
                        "repair_fault",
                        self._repair_first_field(fields, "维修故障", "故障维修原因"),
                    )
                    fault_type = request_text(
                        "fault_type", self._repair_first_field(fields, "故障类型") or "设备故障"
                    )
                    repair_mode = request_text(
                        "repair_mode",
                        self._repair_first_field(fields, "维修方式", "维修方", "供应商名称"),
                    )
                    discovery = request_text(
                        "discovery", self._repair_first_field(fields, "对应来源")
                    )
                    symptom = request_text(
                        "symptom", self._repair_first_field(fields, "故障发生现象描述", "故障现象")
                    )
                    solution = request_text(
                        "solution",
                        self._repair_first_field(fields, "解决方案", "维修方案", "后续整改措施"),
                    )
                    spare_parts = request_text(
                        "spare_parts",
                        self._repair_first_field(fields, "备件更换情况", "备件使用情况"),
                    )
                    source_fault_time = self._repair_first_field(
                        fields, "故障发生时间", "发现故障时间"
                    )
                    fault_time = request_text("fault_time", source_fault_time)
                    target_record_id = self._repair_target_record_id(record)
        else:
            source_record_id = str(request_payload.get("source_record_id") or "").strip()
            active_item_id = str(request_payload.get("active_item_id") or "").strip()
            if source_record_id:
                try:
                    source_record = self._find_record_by_id(source_record_id, WORK_TYPE_REPAIR)
                    fields = source_record.get("display_fields") or {}
                except PortalError:
                    source_record = None
                    fields = {}
            title = str(request_payload.get("title") or "").strip()
            if not title and source_record is not None:
                title = self._repair_title(source_record)
            if not title:
                raise PortalError("更新/结束通告缺少名称。")
            building_codes = [
                str(code or "").strip().upper()
                for code in (request_payload.get("building_codes") or [])
                if str(code or "").strip()
            ]
            if not building_codes and source_record is not None:
                building_codes = self._repair_record_building_codes(source_record)
            if not building_codes:
                building_codes = self._repair_building_codes_from_value(
                    request_payload.get("building")
                )
            building = (
                str(request_payload.get("building") or "").strip()
                or self._building_label_from_codes(building_codes)
            )
            specialty = self._clean_source_text(request_payload.get("specialty"))
            if not specialty and source_record is not None:
                specialty = self._repair_specialty(source_record)
            level = str(request_payload.get("level") or "").strip()
            if not level and source_record is not None:
                level = self._repair_level(fields)
            default_start = ""
            default_end = ""
            record_id = source_record_id
            target_record_id = self._target_record_id_from_request_payload(
                request_payload,
                source_record_id=source_record_id,
            )
            if not target_record_id:
                target_record_id = self._target_record_id_from_identity_map(
                    work_type=WORK_TYPE_REPAIR,
                    active_item_id=active_item_id,
                    source_record_id=source_record_id,
                    target_record_id=self._target_record_id_from_request_payload(
                        request_payload,
                        source_record_id=source_record_id,
                    ),
                )
            if not target_record_id and source_record is not None:
                target_record_id = self._resolve_target_record_id_for_source_update(
                    work_type=WORK_TYPE_REPAIR,
                    notice_type=NOTICE_TYPE_REPAIR,
                    source_record=source_record,
                )
            if not target_record_id and active_item_id:
                target_record_id = self._target_record_id_from_work_status(
                    work_type=WORK_TYPE_REPAIR,
                    active_item_id=active_item_id,
                )
            if not target_record_id:
                target_record_id = self._target_record_id_from_unique_candidate(
                    work_type=WORK_TYPE_REPAIR,
                    scope=scope,
                    title=title,
                    start_time=(
                        self._format_input_datetime(request_payload.get("expected_time"))
                        or self._format_input_datetime(request_payload.get("start_time"))
                    ),
                    end_time=(
                        self._format_input_datetime(request_payload.get("fault_time"))
                        or self._format_input_datetime(request_payload.get("end_time"))
                    ),
                    action=action,
                )
            if not target_record_id:
                target_record_id = self._target_record_id_from_request_payload(
                    request_payload,
                    source_record_id=source_record_id,
                )
            repair_device = request_text("repair_device")
            if not repair_device and source_record is not None:
                repair_device = self._repair_device_text(fields)
            repair_fault = request_text("repair_fault")
            if not repair_fault and source_record is not None:
                repair_fault = self._repair_first_field(fields, "维修故障", "故障维修原因")
            fault_type = (
                request_text("fault_type")
                if "fault_type" in request_payload
                else "设备故障"
            )
            if source_record is not None and fault_type == "设备故障":
                fault_type = self._repair_first_field(fields, "故障类型") or fault_type
            repair_mode = request_text("repair_mode")
            if not repair_mode and source_record is not None:
                repair_mode = self._repair_first_field(fields, "维修方式", "维修方", "供应商名称")
            discovery = request_text("discovery")
            if not discovery and source_record is not None:
                discovery = self._repair_first_field(fields, "对应来源")
            symptom = request_text("symptom")
            if not symptom and source_record is not None:
                symptom = self._repair_first_field(fields, "故障发生现象描述", "故障现象")
            solution = request_text("solution")
            if not solution and source_record is not None:
                solution = self._repair_first_field(fields, "解决方案", "维修方案", "后续整改措施")
            spare_parts = request_text("spare_parts")
            if not spare_parts and source_record is not None:
                spare_parts = self._repair_first_field(fields, "备件更换情况", "备件使用情况")
            fault_time = request_text("fault_time")
            if not fault_time and source_record is not None:
                fault_time = self._repair_first_field(fields, "故障发生时间", "发现故障时间")

        if not self._scope_matches_buildings(scope, building_codes):
            raise PortalError(f"当前入口与通告楼栋不匹配: {building or '-'}")
        if action != "start" and source_record is not None and self._repair_has_ended(source_record):
            raise PortalError("该检修当前源状态已结束，不能更新/结束。")
        if action != "start" and not target_record_id:
            raise PortalError("该检修缺少目标设备检修表 record_id，不能更新/结束。")

        requested_expected_time = self._format_input_datetime(
            request_payload.get("expected_time")
        )
        requested_fault_time = self._format_input_datetime(
            request_payload.get("fault_time")
        )
        requested_start_time = self._format_input_datetime(
            request_payload.get("start_time")
        )
        requested_end_time = self._format_input_datetime(request_payload.get("end_time"))
        expected_time = requested_expected_time or requested_start_time or default_end
        fault_time = requested_fault_time or requested_end_time or self._format_input_datetime(fault_time)
        start_time = expected_time or default_start
        end_time = fault_time or default_end
        now_text = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        if not start_time:
            start_time = now_text
        if not end_time and action == "end":
            end_time = now_text
        self._validate_minimum_notice_duration(
            end_time,
            start_time,
            start_label="发现故障时间",
            end_label="期望完成时间",
        )
        self._require_end_site_photo(
            request_payload,
            action,
            notice_type=NOTICE_TYPE_REPAIR,
            work_type=WORK_TYPE_REPAIR,
        )
        location = request_text("location")
        default_content = self._repair_first_field(
            fields, "标题/补充内容", "标题补充内容"
        )
        content = request_text("content", default_content) or default_content
        if content and title.endswith(content):
            title = title[: -len(content)].strip() or title
        title = self._normalize_110_station_notice_title(
            title,
            building=building,
            building_codes=building_codes,
        )
        reason = request_text(
            "reason", self._repair_first_field(fields, "故障原因", "故障维修原因")
        )
        impact = request_text("impact")
        progress = request_text(
            "progress", self._repair_first_field(fields, "维修进展描述", "当前维修进度")
        )
        if action == "start":
            self._remember_draft_fields(
                work_type=WORK_TYPE_REPAIR,
                building=building,
                maintenance_total=title,
                item_name=title,
                location=location,
                content=content,
                reason=reason,
                impact=impact,
                extra_fields={
                    "specialty": specialty,
                    "level": level,
                    "repair_device": repair_device,
                    "repair_fault": repair_fault,
                    "fault_type": fault_type,
                    "repair_mode": repair_mode,
                    "discovery": discovery,
                    "symptom": symptom,
                    "solution": solution,
                    "spare_parts": spare_parts,
                },
            )
        building_code = (
            "CAMPUS"
            if len(building_codes) >= 2
            else (building_codes[0] if building_codes else "")
        )
        text = self.build_repair_notice_text(
            status=status,
            title=title,
            specialty=specialty,
            level=level,
            start_time=start_time,
            end_time=end_time,
            location=location,
            content=content,
            reason=reason,
            impact=impact,
            progress=progress,
            repair_device=repair_device,
            repair_fault=repair_fault,
            fault_type=fault_type,
            repair_mode=repair_mode,
            discovery=discovery,
            symptom=symptom,
            solution=solution,
            spare_parts=spare_parts,
            fault_time=fault_time,
            expected_time=expected_time,
        )
        _, recipients, recipient_error = self._recipients_for_building_codes(
            building_codes,
            fallback_building=building,
        )
        if recipient_error:
            raise PortalError(recipient_error)
        skip_personal_message = False
        response_time = now_text
        return {
            "job_id": job_id,
            "work_type": WORK_TYPE_REPAIR,
            "notice_type": NOTICE_TYPE_REPAIR,
            "source_app_token": REPAIR_SOURCE_APP_TOKEN if (record_id and not manual) else "",
            "source_table_id": REPAIR_SOURCE_TABLE_ID if (record_id and not manual) else "",
            "target_app_token": str(config.app_token or "").strip(),
            "target_table_id": str(config.get_table_id(NOTICE_TYPE_REPAIR) or "").strip(),
            "manual": manual,
            "action": action,
            "status": status,
            "scope": scope,
            "scope_label": self._scope_label(scope),
            "record_id": record_id,
            "target_record_id": target_record_id,
            "active_item_id": str(request_payload.get("active_item_id") or "").strip(),
            "title": title,
            "building": building,
            "building_code": building_code,
            "building_codes": building_codes,
            "target_building": self._building_label_from_code(building_code),
            "specialty": specialty,
            "level": level,
            "source_progress": str(
                request_payload.get("source_progress")
                or (
                    self._repair_source_status(record)
                    if action == "start" and isinstance(record, dict)
                    else self._repair_source_status(source_record)
                    if isinstance(source_record, dict)
                    else ""
                )
                or self._repair_first_field(fields, "流程", "当前维修进度")
                or ""
            ),
            "start_time": start_time,
            "end_time": end_time,
            "location": location,
            "content": content,
            "reason": reason,
            "impact": impact,
            "progress": progress,
            "repair_device": repair_device,
            "repair_fault": repair_fault,
            "fault_type": fault_type,
            "repair_mode": repair_mode,
            "discovery": discovery,
            "symptom": symptom,
            "solution": solution,
            "spare_parts": spare_parts,
            "fault_time": fault_time,
            "expected_time": expected_time,
            "text": text,
            "extra_images": self._site_photo_payload(request_payload),
            "recipients": recipients,
            "skip_personal_message": skip_personal_message,
            "response_time": response_time,
            "operation_id": str(request_payload.get("operation_id") or "").strip()
            or job_id,
        }

    def send_action_personal_message(self, prepared: dict[str, Any]) -> tuple[bool, str]:
        if (prepared or {}).get("skip_personal_message"):
            return True, "无需发送个人消息"
        text = str((prepared or {}).get("text") or "").strip()
        recipients = list(prepared.get("recipients") or [])
        if not text:
            return False, "通告文本为空。"
        if not recipients:
            return False, "缺少飞书接收人。"
        job_id = str((prepared or {}).get("job_id") or "")
        message_signature = str((prepared or {}).get("message_signature") or "")
        job = self.get_job(job_id) or {}
        prior_results = []
        if str(job.get("message_signature") or "") == message_signature:
            prior_results = [
                item
                for item in (job.get("recipient_results") or [])
                if isinstance(item, dict)
            ]
        succeeded = {
            str(item.get("open_id") or "").strip()
            for item in prior_results
            if item.get("ok") and str(item.get("open_id") or "").strip()
        }
        pending_recipients = [
            str(open_id or "").strip()
            for open_id in recipients
            if str(open_id or "").strip() and str(open_id or "").strip() not in succeeded
        ]
        if not pending_recipients:
            self.mark_job(job_id, recipient_results=prior_results)
            return True, "ok"
        guard = external_real_write_guard()
        if guard["mock_external"]:
            new_results = [
                {"open_id": open_id, "ok": True, "message": "mock external send skipped"}
                for open_id in pending_recipients
            ]
            ok = True
            message = "mock external send skipped"
        elif not guard["real_write_allowed"]:
            return False, str(guard["reason"] or "真实外部写入未确认。")
        else:
            ok, message, new_results = send_text_to_open_ids(text, pending_recipients)
        by_open_id = {
            str(item.get("open_id") or "").strip(): dict(item)
            for item in prior_results
            if str(item.get("open_id") or "").strip()
        }
        for item in new_results:
            open_id = str(item.get("open_id") or "").strip()
            if open_id:
                by_open_id[open_id] = dict(item)
        recipient_results = [by_open_id.get(str(open_id).strip()) for open_id in recipients]
        recipient_results = [item for item in recipient_results if item]
        ok = ok and all(bool(item.get("ok")) for item in recipient_results)
        self.mark_job(
            job_id,
            recipient_results=recipient_results,
        )
        return ok, message

    def _validate_generated_item(
        self, item: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str, str, str, str]:
        record_id = str(item.get("record_id") or "").strip()
        text = str(item.get("text") or "").strip()
        if not record_id:
            return None, "", "", "", "缺少 record_id"
        if not text:
            return None, record_id, "", "", "缺少生成后的通告文本"
        if not text.startswith("【维保通告】状态：开始"):
            return None, record_id, "", "", "通告文本不是维保开始模板"
        record = self._find_record_by_id(record_id)
        fields = record.get("display_fields") or {}
        building = str(fields.get("楼栋") or "").strip()
        maintenance_total = str(fields.get("维护总项") or "").strip()
        expected_name = f"EA118机房{building}{maintenance_total}"
        if expected_name and expected_name not in text:
            return None, record_id, building, expected_name, "通告名称与多维记录不一致"
        return record, record_id, building, expected_name, ""

    def send_generated_templates(
        self,
        items: list[dict[str, Any]],
        *,
        notice_callback: Callable[[dict[str, Any]], Any] | None = None,
    ) -> list[dict[str, Any]]:
        self.ensure_snapshot_loaded()
        if notice_callback is None:
            raise PortalError("主窗口未连接，无法发送后回填主界面。")
        if not isinstance(items, list) or not items:
            raise PortalError("缺少待发送通告。")

        results: list[dict[str, Any]] = []
        for item in items:
            base_result = {
                "record_id": str((item or {}).get("record_id") or "").strip(),
                "title": str((item or {}).get("title") or "").strip(),
                "text": str((item or {}).get("text") or "").strip(),
                "building": "",
                "target_building": "",
                "ok": False,
                "error": "",
                "recipients": [],
            }
            try:
                record, record_id, building, expected_name, error = (
                    self._validate_generated_item(item or {})
                )
                base_result["record_id"] = record_id
                base_result["building"] = building
                base_result["title"] = expected_name or base_result["title"]
                if error:
                    base_result["error"] = error
                    results.append(base_result)
                    continue

                building_code, recipients, recipient_error = self._recipients_for_building(
                    building
                )
                base_result["target_building"] = f"{building_code}楼" if building_code else ""
                base_result["recipients"] = recipients
                if recipient_error:
                    base_result["error"] = recipient_error
                    results.append(base_result)
                    continue

                guard = external_real_write_guard()
                if guard["mock_external"]:
                    ok, message, recipient_results = True, "mock external send skipped", [
                        {
                            "open_id": str(open_id or "").strip(),
                            "ok": True,
                            "message": "mock external send skipped",
                        }
                        for open_id in recipients
                        if str(open_id or "").strip()
                    ]
                elif not guard["real_write_allowed"]:
                    ok, message, recipient_results = False, str(
                        guard["reason"] or "真实外部写入未确认。"
                    ), []
                else:
                    ok, message, recipient_results = send_text_to_open_ids(
                        base_result["text"], recipients
                    )
                base_result["recipient_results"] = recipient_results
                if not ok:
                    base_result["error"] = message
                    results.append(base_result)
                    continue

                notice_callback(
                    {
                        "source": "lan_template_portal",
                        "record_id": record_id,
                        "title": base_result["title"],
                        "building": building,
                        "target_building": base_result["target_building"],
                        "text": base_result["text"],
                    }
                )
                base_result["ok"] = True
                results.append(base_result)
            except Exception as exc:
                base_result["error"] = str(exc)
                results.append(base_result)
        return results

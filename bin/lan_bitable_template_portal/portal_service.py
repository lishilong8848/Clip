# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import re
import secrets
import threading
import time
import uuid
import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import requests

from upload_event_module.config import config, get_field_config
from upload_event_module.services.feishu_service import refresh_feishu_token
from upload_event_module.services.robot_webhook import send_text_to_open_ids
from upload_event_module.utils import get_data_file_path


DEFAULT_APP_TOKEN = "HU38bc1vnamMK9sCeOgclUvXnFc"
DEFAULT_TABLE_ID = "tblzk7WrXxNWQy6V"
CHANGE_SOURCE_APP_TOKEN = "JhiVwgfoIimAqEk8YwEc09sknGd"
CHANGE_SOURCE_TABLE_ID = "tblBvg6wCYSX3hcg"
ZHIHANG_CHANGE_APP_TOKEN = "IrIibPkUOa6udGsMhu2cbOqhnWg"
ZHIHANG_CHANGE_TABLE_ID = "tblqMJvYW5dxFFfU"
REPAIR_SOURCE_APP_TOKEN = "AnEBwJlvGiJfDdkOB32cUPuknzg"
REPAIR_SOURCE_TABLE_ID = "tblschT48zXwigUG"
REPAIR_SOURCE_VIEW_ID = "vewn2xWBED"
DEFAULT_IMPACT_TEXT = "对IT业务无影响，不会触发BA和BMS系统相关告警"
DEFAULT_PROGRESS_TEXT = "准备工作已完成，人员已就位，可否开始操作？"
DEFAULT_MAINTENANCE_STATUS = "未开始"
MAINTENANCE_STATUS_ONGOING = "进行中"
WORKBENCH_SOURCE_STATUSES = {DEFAULT_MAINTENANCE_STATUS, MAINTENANCE_STATUS_ONGOING}
WORK_TYPE_MAINTENANCE = "maintenance"
WORK_TYPE_CHANGE = "change"
WORK_TYPE_REPAIR = "repair"
NOTICE_TYPE_MAINTENANCE = "维保通告"
NOTICE_TYPE_CHANGE = "设备变更"
NOTICE_TYPE_REPAIR = "设备检修"
CHANGE_PROGRESS_NOT_STARTED = "未开始"
CHANGE_PROGRESS_ONGOING = "进行中"
CHANGE_PROGRESS_ENDED = "已结束"
CHANGE_WORKBENCH_PROGRESS_VALUES = {CHANGE_PROGRESS_NOT_STARTED, CHANGE_PROGRESS_ONGOING}
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
SOURCE_CACHE_TTL_SECONDS = 5 * 60
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
        self.app_token = app_token
        self.table_id = table_id
        self._session = requests.Session()
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
        self._memory_dir = Path(get_data_file_path("lan_template_memory"))
        self._memory_dir.mkdir(parents=True, exist_ok=True)
        self._summary_dir = Path(get_data_file_path("lan_template_daily_summary"))
        self._summary_dir.mkdir(parents=True, exist_ok=True)
        self._work_status_dir = Path(get_data_file_path("lan_template_work_status"))
        self._work_status_dir.mkdir(parents=True, exist_ok=True)
        self._handover_links_path = Path(get_data_file_path("lan_handover_links.json"))
        self._handover_links_path.parent.mkdir(parents=True, exist_ok=True)
        self._hidden_ongoing_path = Path(get_data_file_path("lan_template_hidden_ongoing.json"))
        self._hidden_ongoing_path.parent.mkdir(parents=True, exist_ok=True)
        self._work_status_backfilled = False
        self._work_status_cache_signature: tuple[tuple[str, int, int], ...] | None = None
        self._work_status_cache_items: list[dict[str, Any]] | None = None
        self._target_record_cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._jobs: dict[str, dict[str, Any]] = {}
        self._jobs_lock = threading.RLock()
        self._handover_password_reset: dict[str, Any] | None = None

    def _auth_headers(self) -> dict[str, str]:
        if not config.user_token:
            refresh_feishu_token()
        token = str(config.user_token or "").strip()
        if not token:
            raise PortalError("未配置有效的飞书 user_token。")
        return {"Authorization": f"Bearer {token}"}

    def _request_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        app_token: str | None = None,
        table_id: str | None = None,
    ) -> dict[str, Any]:
        app_token = str(app_token or self.app_token).strip()
        table_id = str(table_id or self.table_id).strip()
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{app_token}/tables/{table_id}/{path}"
        )
        response = self._session.get(
            url, headers=self._auth_headers(), params=params or {}, timeout=30
        )
        payload = response.json()
        if payload.get("code") == 99991663:
            refresh_feishu_token()
            response = self._session.get(
                url, headers=self._auth_headers(), params=params or {}, timeout=30
            )
            payload = response.json()
        code = payload.get("code", 0)
        if code != 0:
            raise PortalError(
                f"飞书接口失败: code={code}, msg={payload.get('msg') or 'unknown'}"
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
                records.append(
                    self._normalize_record(
                        item,
                        meta_by_name=self._field_meta_by_name,
                        work_type=WORK_TYPE_MAINTENANCE,
                        notice_type=NOTICE_TYPE_MAINTENANCE,
                        source_app_token=self.app_token,
                        source_table_id=self.table_id,
                    )
                )
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
                records.append(
                    self._normalize_record(
                        item,
                        meta_by_name=self._change_field_meta_by_name,
                        work_type=WORK_TYPE_CHANGE,
                        notice_type=NOTICE_TYPE_CHANGE,
                        source_app_token=CHANGE_SOURCE_APP_TOKEN,
                        source_table_id=CHANGE_SOURCE_TABLE_ID,
                    )
                )
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
                records.append(
                    self._normalize_record(
                        item,
                        meta_by_name=self._zhihang_change_field_meta_by_name,
                        work_type=WORK_TYPE_CHANGE,
                        notice_type=NOTICE_TYPE_CHANGE,
                        source_app_token=ZHIHANG_CHANGE_APP_TOKEN,
                        source_table_id=ZHIHANG_CHANGE_TABLE_ID,
                    )
                )
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
            params = {"page_size": 500, "view_id": REPAIR_SOURCE_VIEW_ID}
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
                records.append(
                    self._normalize_record(
                        item,
                        meta_by_name=self._repair_field_meta_by_name,
                        work_type=WORK_TYPE_REPAIR,
                        notice_type=NOTICE_TYPE_REPAIR,
                        source_app_token=REPAIR_SOURCE_APP_TOKEN,
                        source_table_id=REPAIR_SOURCE_TABLE_ID,
                    )
                )
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
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        page_token = ""
        while True:
            params = {"page_size": 500}
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

    def clear_target_record_cache(self) -> None:
        with self._refresh_lock:
            self._target_record_cache.clear()

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
                warnings.append(f"变更源表同步失败: {exc}")
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
                warnings.append(f"智航变更源表同步失败: {exc}")
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
                warnings.append(f"检修源表同步失败: {exc}")
            now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._last_loaded_at = now
            self._last_loaded_ts = time.time()
            self._load_warnings = warnings

    def ensure_loaded(self, *, refresh_if_expired: bool = True) -> None:
        with self._refresh_lock:
            if (
                not self._field_meta_list
                or not self._maintenance_loaded_once
                or (refresh_if_expired and self._source_cache_expired())
            ):
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
                    warnings.append(f"变更源表同步失败: {exc}")
            if not self._zhihang_change_loaded_once:
                attempted_optional_load = True
                try:
                    self._load_zhihang_change_fields()
                    self._load_zhihang_change_records()
                except Exception as exc:
                    self._zhihang_change_loaded_once = True
                    warnings.append(f"智航变更源表同步失败: {exc}")
            if not self._repair_loaded_once:
                attempted_optional_load = True
                try:
                    self._load_repair_fields()
                    self._load_repair_records()
                except Exception as exc:
                    self._repair_loaded_once = True
                    warnings.append(f"检修源表同步失败: {exc}")
            if attempted_optional_load:
                self._load_warnings = warnings

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
    def _format_input_datetime(value: Any) -> str:
        text = str(value or "").strip()
        return text.replace("T", " ")

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

    def _sorted_unique_option_names(self, field_name: str) -> list[str]:
        values = {
            str(record["display_fields"].get(field_name) or "").strip()
            for record in self._records
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
        values = {
            str(record["display_fields"].get("专业类别") or "").strip()
            for record in self._records
            if self._maintenance_status_allows_workbench(record)
        }
        values.update(
            {
                str(record["display_fields"].get("专业") or "").strip()
                for record in self._change_records
                if self._change_progress_allows_workbench(record)
            }
        )
        values.update(
            {
                self._repair_specialty(record)
                for record in self._repair_records
                if self._is_valid_repair_record(record)
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
            if month and display_fields.get("计划维护月份") != month:
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
        raw_building = fields.get("变更楼栋") or fields.get("楼栋") or ""
        return self._building_codes_from_value(raw_building)

    @staticmethod
    def _change_progress_value(record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        return str(fields.get("变更进度") or "").strip()

    def _change_progress_allows_workbench(self, record: dict[str, Any]) -> bool:
        return self._change_progress_value(record) in CHANGE_WORKBENCH_PROGRESS_VALUES

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
        return "" if text in PLACEHOLDER_TEXT_VALUES else text

    def _repair_title(self, record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        return (
            self._clean_source_text(fields.get("事件描述"))
            or self._clean_source_text(fields.get("维修名称"))
            or self._clean_source_text(record.get("record_id"))
        )

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
        specialty = self._clean_source_text(
            fields.get("所属专业") or fields.get("专业（推送消息用）") or ""
        )
        return REPAIR_SPECIALTY_OPTION_FALLBACK.get(specialty, specialty)

    def _repair_record_building_codes(self, record: dict[str, Any]) -> list[str]:
        fields = record.get("display_fields") or {}
        for field_name in (
            "所属数据中心/楼栋-使用",
            "所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）",
        ):
            codes = self._repair_building_codes_from_value(fields.get(field_name))
            if codes:
                return codes
        return (
            self._repair_building_codes_from_value(self._repair_title(record))
            or self._repair_building_codes_from_value(fields.get("维修名称"))
        )

    def _is_valid_repair_record(self, record: dict[str, Any]) -> bool:
        fields = record.get("display_fields") or {}
        repair_name = self._clean_source_text(fields.get("维修名称"))
        if not repair_name or repair_name in PLACEHOLDER_TEXT_VALUES:
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
            if specialty and specialty != str(fields.get("专业") or "").strip():
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
        scope: str = "ALL",
        exclude_record_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        excluded = {str(value or "").strip() for value in (exclude_record_ids or set())}
        filtered: list[dict[str, Any]] = []
        for record in self._zhihang_change_records:
            record_id = str(record.get("record_id") or "").strip()
            if record_id and record_id in excluded:
                continue
            progress = self._zhihang_change_progress(record)
            if progress == ZHIHANG_PROGRESS_ENDED:
                continue
            if not self._scope_matches_zhihang_change_record(scope, record):
                continue
            filtered.append(record)
        return filtered

    def _find_zhihang_change_record_by_id(self, record_id: str) -> dict[str, Any]:
        record_id = str(record_id or "").strip()
        for record in self._zhihang_change_records:
            if str(record.get("record_id") or "").strip() == record_id:
                return record
        raise PortalError(f"未找到智航侧变更记录: {record_id}")

    def _filter_repair_records(
        self,
        *,
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

    def _load_building_memory_locked(self, building: str) -> dict[str, Any]:
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
            return payload
        except Exception:
            return {"building": building, "items": {}}

    def _save_building_memory_locked(self, building: str, payload: dict[str, Any]) -> None:
        path = self._building_memory_path(building)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
        tmp_path.replace(path)

    def _summary_path(self, day: str | None = None) -> Path:
        day = str(day or dt.datetime.now().strftime("%Y-%m-%d")).strip()
        day = re.sub(r"[^0-9-]+", "", day) or dt.datetime.now().strftime("%Y-%m-%d")
        return self._summary_dir / f"{day}.json"

    def _load_day_summary_locked(self, day: str | None = None) -> dict[str, Any]:
        path = self._summary_path(day)
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
            return payload
        except Exception:
            return {"date": path.stem, "items": []}

    def _save_day_summary_locked(self, payload: dict[str, Any], day: str | None = None) -> None:
        path = self._summary_path(day or payload.get("date"))
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
        tmp_path.replace(path)

    @staticmethod
    def _summary_key(
        *,
        source_record_id: str = "",
        title: str = "",
        building: str = "",
        work_type: str = "",
    ) -> str:
        source_record_id = str(source_record_id or "").strip()
        work_type = str(work_type or "").strip()
        prefix = f"{work_type}:" if work_type else ""
        if source_record_id:
            return f"{prefix}source:{source_record_id}"
        title_key = re.sub(r"\s+", "", str(title or ""))
        building_key = re.sub(r"\s+", "", str(building or ""))
        return f"{prefix}title:{building_key}:{title_key}" if title_key else ""

    @staticmethod
    def _work_status_fallback_key(*, title: str = "", building: str = "", plan_month: str = "") -> str:
        title_key = re.sub(r"\s+", "", str(title or ""))
        building_key = re.sub(r"\s+", "", str(building or ""))
        month_key = re.sub(r"\s+", "", str(plan_month or ""))
        return f"{building_key}:{month_key}:{title_key}" if title_key else ""

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
        return payload

    def _save_work_status_locked(
        self, payload: dict[str, Any], *, building: str = "", building_code: str = ""
    ) -> None:
        path = self._work_status_path(
            building or payload.get("building") or "",
            building_code or payload.get("building_code") or "",
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
        tmp_path.replace(path)
        self._work_status_cache_signature = None
        self._work_status_cache_items = None

    def _find_summary_item(
        self,
        items: list[dict[str, Any]],
        *,
        key: str = "",
        active_item_id: str = "",
        title: str = "",
        building: str = "",
        work_type: str = "",
    ) -> dict[str, Any] | None:
        key = str(key or "").strip()
        active_item_id = str(active_item_id or "").strip()
        if key:
            for item in items:
                if str(item.get("key") or "") == key:
                    return item
        if active_item_id:
            for item in items:
                if str(item.get("active_item_id") or "") == active_item_id:
                    return item
        fallback_key = self._summary_key(
            title=title, building=building, work_type=work_type
        )
        if fallback_key:
            for item in items:
                if str(item.get("fallback_key") or "") == fallback_key:
                    return item
        legacy_fallback_key = self._summary_key(title=title, building=building)
        if legacy_fallback_key and legacy_fallback_key != fallback_key:
            for item in items:
                if str(item.get("fallback_key") or "") == legacy_fallback_key:
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
        phase = "success" if success else "failed"
        patch = {
            "phase": phase,
            "error": "" if success else str(message or "上传失败"),
            "upload_message": str(message or ""),
            "record_id": str(record_id or ""),
        }
        if active_item_id:
            patch["active_item_id"] = str(active_item_id or "")
        self.mark_job(job_id, **patch)
        if success:
            self._record_successful_action(job_id, record_id=record_id, active_item_id=active_item_id)

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
        building = str(prepared.get("building") or "").strip()
        work_type = str(prepared.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        key = self._summary_key(
            source_record_id=source_record_id,
            title=title,
            building=building,
            work_type=work_type,
        )
        fallback_key = self._summary_key(
            title=title, building=building, work_type=work_type
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
                "level",
                "source_progress",
                "zhihang_involved",
                "zhihang_record_id",
                "zhihang_title",
                "zhihang_progress",
                "zhihang_source_app_token",
                "zhihang_source_table_id",
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
        }
        for item in items or []:
            work_type = str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
            if work_type not in counts:
                continue
            counts[work_type] += 1
        return counts

    def get_scope_overview(
        self, *, ongoing_items: list[dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        self.ensure_loaded()
        default_month = self._current_month_label()
        overview: dict[str, dict[str, Any]] = {}
        for option in SCOPE_OPTIONS:
            scope = self._normalize_scope(option.get("value"))
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
        return {
            "scope_options": SCOPE_OPTIONS,
            "scopes": overview,
            "last_loaded_at": self._last_loaded_at,
            "source_cache_ttl_seconds": self._source_cache_ttl_seconds(),
            "warnings": list(self._load_warnings),
        }

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
        try:
            with self._handover_links_path.open("r", encoding="utf-8") as fp:
                payload = json.load(fp)
        except FileNotFoundError:
            return {}
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _save_handover_payload_locked(self, payload: dict[str, Any]) -> None:
        with self._handover_links_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)

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
            paths = sorted(self._summary_dir.glob(f"{month}-*.json"))
            for path in paths:
                try:
                    with path.open("r", encoding="utf-8") as fp:
                        payload = json.load(fp)
                except Exception:
                    continue
                if not isinstance(payload, dict):
                    continue
                day = str(payload.get("date") or path.stem)
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
            "warnings": list(self._load_warnings),
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
        for path in sorted(self._summary_dir.glob("*.json")):
            try:
                with path.open("r", encoding="utf-8") as fp:
                    payload = json.load(fp)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            day = str(payload.get("date") or path.stem)
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
        scope = self._normalize_scope(scope)
        paths = sorted(self._work_status_dir.glob("*.json"))
        signature_items: list[tuple[str, int, int]] = []
        for path in paths:
            try:
                stat = path.stat()
                signature_items.append((path.name, int(stat.st_mtime_ns), int(stat.st_size)))
            except OSError:
                continue
        signature = tuple(signature_items)
        if self._work_status_cache_signature == signature and self._work_status_cache_items is not None:
            all_items = self._work_status_cache_items
        else:
            all_items: list[dict[str, Any]] = []
            for path in paths:
                try:
                    with path.open("r", encoding="utf-8") as fp:
                        payload = json.load(fp)
                except Exception:
                    continue
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
        work_type = str(item.get("work_type") or WORK_TYPE_MAINTENANCE).strip()
        keys: set[str] = set()
        for kind, field_names in {
            "active": ("active_item_id",),
            "source": ("source_record_id",),
            "target": ("target_record_id", "feishu_record_id", "record_id", "raw_record_id"),
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
        target_record_id = str(
            item.get("target_record_id")
            or item.get("feishu_record_id")
            or item.get("record_id")
            or ""
        ).strip()
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
                    notice_type, work_type, force_refresh=True
                )
            except Exception as exc:
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
            for path in sorted(self._work_status_dir.glob("*.json")):
                try:
                    with path.open("r", encoding="utf-8") as fp:
                        payload = json.load(fp)
                except Exception:
                    continue
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
                    with path.open("w", encoding="utf-8") as fp:
                        json.dump(payload, fp, ensure_ascii=False, indent=2)
            if removed_keys:
                for path in sorted(self._summary_dir.glob("*.json")):
                    try:
                        with path.open("r", encoding="utf-8") as fp:
                            payload = json.load(fp)
                    except Exception:
                        continue
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
                        with path.open("w", encoding="utf-8") as fp:
                            json.dump(payload, fp, ensure_ascii=False, indent=2)
                self._work_status_cache_signature = None
                self._work_status_cache_items = None
        return {"removed": removed_count}

    def _get_record_memory(self, record: dict[str, Any]) -> dict[str, str]:
        fields = record.get("display_fields") or {}
        building = str(fields.get("楼栋") or "").strip()
        maintenance_total = str(fields.get("维护总项") or "").strip()
        if not building or not maintenance_total:
            return {}
        key = self._normalize_memory_key(maintenance_total)
        with self._memory_lock:
            payload = self._load_building_memory_locked(building)
            items = payload.get("items") or {}
            item = items.get(key) or {}
        if not isinstance(item, dict):
            return {}
        return {
            "location": str(item.get("location") or ""),
            "content": str(item.get("content") or ""),
            "reason": str(item.get("reason") or ""),
            "impact": str(item.get("impact") or ""),
            "updated_at": str(item.get("updated_at") or ""),
        }

    def _remember_draft_fields(
        self,
        *,
        building: str,
        maintenance_total: str,
        location: str,
        content: str,
        reason: str,
        impact: str,
    ) -> None:
        building = str(building or "").strip()
        maintenance_total = str(maintenance_total or "").strip()
        if not building or not maintenance_total:
            return
        key = self._normalize_memory_key(maintenance_total)
        now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._memory_lock:
            payload = self._load_building_memory_locked(building)
            payload["building"] = building
            payload["updated_at"] = now
            items = payload.setdefault("items", {})
            items[key] = {
                "maintenance_total": maintenance_total,
                "location": location,
                "content": content,
                "reason": reason,
                "impact": impact,
                "updated_at": now,
            }
            self._save_building_memory_locked(building, payload)

    def _serialize_record(
        self, record: dict[str, Any], summary_by_record: dict[str, dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        summary_by_record = summary_by_record or {}
        work_type = str(record.get("work_type") or WORK_TYPE_MAINTENANCE)
        if work_type == WORK_TYPE_CHANGE:
            source_progress = self._change_progress_value(record)
        elif work_type == WORK_TYPE_REPAIR:
            source_progress = self._repair_source_status(record)
        else:
            source_progress = self._maintenance_status_value(record)
        return {
            "record_id": record["record_id"],
            "source_record_id": record["record_id"],
            "work_type": work_type,
            "notice_type": str(record.get("notice_type") or NOTICE_TYPE_MAINTENANCE),
            "source_app_token": str(record.get("source_app_token") or self.app_token),
            "source_table_id": str(record.get("source_table_id") or self.table_id),
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

    def _change_title(self, record: dict[str, Any]) -> str:
        fields = record.get("display_fields") or {}
        return str(fields.get("变更简述") or record.get("record_id") or "").strip()

    def _change_time_range(self, record: dict[str, Any]) -> tuple[str, str, str]:
        fields = record.get("display_fields") or {}
        start_time = str(fields.get("变更开始日期（阿里）") or "").strip()
        end_time = str(fields.get("变更结束日期（阿里）") or "").strip()
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

    def _date_keys_from_values(self, *values: Any) -> set[str]:
        keys: set[str] = set()
        for value in values:
            formatted = self._format_source_datetime(value)
            for year, month, day in re.findall(
                r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})", formatted
            ):
                keys.add(f"{int(year):04d}-{int(month):02d}-{int(day):02d}")
        return keys

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
        return payload

    def _save_hidden_ongoing_locked(self, payload: dict[str, Any]) -> None:
        payload["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._hidden_ongoing_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)

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
            for path in sorted(self._work_status_dir.glob("*.json")):
                try:
                    with path.open("r", encoding="utf-8") as fp:
                        payload = json.load(fp)
                except Exception:
                    continue
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
                    with path.open("w", encoding="utf-8") as fp:
                        json.dump(payload, fp, ensure_ascii=False, indent=2)

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

    def validate_ongoing_delete_item(
        self, item: dict[str, Any], *, scope: str = "ALL"
    ) -> list[str]:
        if not isinstance(item, dict):
            raise PortalError("删除参数格式错误。")
        scope = self._normalize_scope(scope)
        item = copy.deepcopy(item)
        item.setdefault("work_type", WORK_TYPE_MAINTENANCE)
        item.setdefault("notice_type", NOTICE_TYPE_MAINTENANCE)
        if not self._scope_matches_item(scope, item):
            raise PortalError("当前账号无权删除该楼栋的进行中通告。")
        keys = self._ongoing_hidden_keys(item)
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
            target_record_id = str(
                item.get("target_record_id")
                or item.get("feishu_record_id")
                or item.get("record_id")
                or ""
            ).strip()
            if not target_record_id:
                continue
            if not fallback:
                fallback = target_record_id
            if not self._is_completed_work_status_item(item):
                return target_record_id
        return fallback

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
        for item in ongoing_items or []:
            if not isinstance(item, dict):
                continue
            if not self._scope_matches_item(scope, item):
                continue
            copied = copy.deepcopy(item)
            copied.setdefault("work_type", WORK_TYPE_MAINTENANCE)
            copied.setdefault("notice_type", NOTICE_TYPE_MAINTENANCE)
            if self._is_ongoing_hidden(copied):
                continue
            merged.append(copied)
        return merged

    def _workbench_records(
        self, *, month: str = "", specialty: str = "", scope: str = "ALL"
    ) -> list[dict[str, Any]]:
        maintenance_records = self._filter_records(
            month=month, specialty=specialty, scope=scope
        )
        change_records = self._filter_change_records(
            specialty=specialty,
            scope=scope,
        )
        repair_records = self._filter_repair_records(specialty=specialty, scope=scope)
        return maintenance_records + change_records + repair_records

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
        self.ensure_loaded()
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
        self.ensure_loaded()
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
        self.ensure_loaded()
        default_month = self._current_month_label()
        scope = self._normalize_scope(scope)
        merged_ongoing = self._merge_ongoing_items(scope, ongoing_items or [])
        filtered_records = self._workbench_records(month=default_month, scope=scope)
        linked_zhihang_ids = self._linked_zhihang_record_ids(merged_ongoing)
        zhihang_records = self._filter_zhihang_change_records(
            scope=scope, exclude_record_ids=linked_zhihang_ids
        )
        daily_summary = self.get_daily_summary(
            scope=scope, ongoing_items=merged_ongoing
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
            "source_cache_ttl_seconds": self._source_cache_ttl_seconds(),
            "warnings": list(self._load_warnings),
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
        self.ensure_loaded()
        scope = self._normalize_scope(scope)
        merged_ongoing = self._merge_ongoing_items(scope, ongoing_items or [])
        filtered_records = self._workbench_records(
            month=month, specialty=specialty, scope=scope
        )
        linked_zhihang_ids = self._linked_zhihang_record_ids(merged_ongoing)
        zhihang_records = self._filter_zhihang_change_records(
            scope=scope, exclude_record_ids=linked_zhihang_ids
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
            "source_cache_ttl_seconds": self._source_cache_ttl_seconds(),
            "warnings": list(self._load_warnings),
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
        raise PortalError(f"未找到记录: {record_id}")

    def generate_templates(self, drafts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        self.ensure_loaded()
        generated: list[dict[str, Any]] = []
        for draft in drafts:
            record_id = str(draft.get("record_id") or "").strip()
            if not record_id:
                raise PortalError("存在缺少 record_id 的待生成记录。")
            record = self._find_record_by_id(record_id)
            fields = record["display_fields"]
            building = str(fields.get("楼栋") or "").strip()
            maintenance_total = str(fields.get("维护总项") or "").strip()
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
        return (
            f"【维保通告】状态：{str(status or '').strip()}\n"
            f"【名称】{str(title or '').strip()}\n"
            f"【时间】{MaintenancePortalService._format_input_datetime(start_time)}~"
            f"{MaintenancePortalService._format_input_datetime(end_time)}\n"
            f"【位置】{str(location or '').strip()}\n"
            f"【内容】{str(content or '').strip()}\n"
            f"【原因】{str(reason or '').strip()}\n"
            f"【影响】{str(impact or '').strip() or DEFAULT_IMPACT_TEXT}\n"
            f"【进度】{str(progress or '').strip() or DEFAULT_PROGRESS_TEXT}"
        )

    def _base_job(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        now = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        return {
            "job_id": uuid.uuid4().hex,
            "phase": "queued",
            "created_at": now,
            "updated_at": now,
            "request": copy.deepcopy(request_payload),
            "operation_id": str(request_payload.get("operation_id") or "").strip(),
            "target_key": self._action_target_key(request_payload),
            "message_sent": False,
            "message_signature": "",
            "record_id": "",
            "active_item_id": str(request_payload.get("active_item_id") or ""),
            "error": "",
            "upload_message": "",
            "prepared": {},
        }

    @staticmethod
    def _action_target_key(request_payload: dict[str, Any]) -> str:
        action = str((request_payload or {}).get("action") or "").strip().lower()
        work_type = str(
            (request_payload or {}).get("work_type") or WORK_TYPE_MAINTENANCE
        ).strip()
        if action == "start":
            record_id = str((request_payload or {}).get("record_id") or "").strip()
            return f"{work_type}:start:{record_id}" if record_id else ""
        if action in {"update", "end"}:
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

    def create_action_job(self, request_payload: dict[str, Any]) -> tuple[str, bool]:
        if not isinstance(request_payload, dict):
            raise PortalError("请求体格式错误。")
        action = str(request_payload.get("action") or "").strip().lower()
        if action not in {"start", "update", "end"}:
            raise PortalError("动作必须是 start/update/end。")
        if action == "start" and not str(request_payload.get("record_id") or "").strip():
            raise PortalError("开始通告缺少计划记录ID。")
        if action == "update" and not (
            str(request_payload.get("active_item_id") or "").strip()
            or str(request_payload.get("source_record_id") or "").strip()
        ):
            raise PortalError("更新通告缺少主界面条目ID或源记录ID。")
        if action == "end" and not str(request_payload.get("active_item_id") or "").strip():
            raise PortalError("更新/结束通告缺少主界面条目ID。")
        operation_id = str(request_payload.get("operation_id") or "").strip()
        job = self._base_job(request_payload)
        with self._jobs_lock:
            if operation_id:
                for existing in self._jobs.values():
                    if str(existing.get("operation_id") or "") != operation_id:
                        continue
                    phase = str(existing.get("phase") or "")
                    if phase in {"queued", "sending_message", "uploading", "success"}:
                        return str(existing.get("job_id") or ""), False
                    if phase == "failed":
                        existing["request"] = copy.deepcopy(request_payload)
                        existing["phase"] = "queued"
                        existing["error"] = ""
                        existing["updated_at"] = dt.datetime.now().strftime(
                            "%Y-%m-%d %H:%M"
                        )
                        return str(existing.get("job_id") or ""), True
            target_key = str(job.get("target_key") or "")
            if target_key:
                for existing in self._jobs.values():
                    if str(existing.get("target_key") or "") != target_key:
                        continue
                    phase = str(existing.get("phase") or "")
                    if phase in {"queued", "sending_message", "uploading"}:
                        raise PortalError("该通告已有进行中的发送/上传任务，请等待完成后再操作。")
            self._jobs[job["job_id"]] = job
            self._trim_jobs_locked()
        return job["job_id"], True

    def _trim_jobs_locked(self) -> None:
        if len(self._jobs) <= 300:
            return
        ordered = sorted(
            self._jobs.values(), key=lambda item: str(item.get("created_at") or "")
        )
        for job in ordered[: len(self._jobs) - 300]:
            self._jobs.pop(str(job.get("job_id") or ""), None)

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return None
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            return copy.deepcopy(job) if job else None

    def mark_job(self, job_id: str, **patch: Any) -> None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return
        with self._jobs_lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job.update(patch)
            job["updated_at"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M")

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
            phase="queued",
            prepared=prepared,
            record_id=str(prepared.get("record_id") or ""),
            active_item_id=str(prepared.get("active_item_id") or ""),
            message_signature=message_signature,
        )
        return prepared

    def prepare_workbench_action(
        self, request_payload: dict[str, Any], *, job_id: str
    ) -> dict[str, Any]:
        work_type = str(
            request_payload.get("work_type") or WORK_TYPE_MAINTENANCE
        ).strip()
        if work_type == WORK_TYPE_CHANGE:
            return self.prepare_change_action(request_payload, job_id=job_id)
        if work_type == WORK_TYPE_REPAIR:
            return self.prepare_repair_action(request_payload, job_id=job_id)
        return self.prepare_maintenance_action(request_payload, job_id=job_id)

    def prepare_maintenance_action(
        self, request_payload: dict[str, Any], *, job_id: str
    ) -> dict[str, Any]:
        action = str(request_payload.get("action") or "").strip().lower()
        status_map = {"start": "开始", "update": "更新", "end": "结束"}
        status = status_map.get(action)
        if not status:
            raise PortalError("动作必须是 start/update/end。")
        self.ensure_loaded()
        scope = self._normalize_scope(request_payload.get("scope"))
        specialty = str(request_payload.get("specialty") or "").strip()

        record = None
        fields: dict[str, Any] = {}
        record_id = str(request_payload.get("record_id") or "").strip()
        source_record_id = str(request_payload.get("source_record_id") or "").strip()
        target_record_id = str(request_payload.get("target_record_id") or "").strip()
        active_item_id = str(request_payload.get("active_item_id") or "").strip()
        plan_month = ""
        maintenance_total = ""
        maintenance_no = ""
        maintenance_project = ""
        if action == "start":
            if not record_id:
                raise PortalError("开始通告缺少计划记录ID。")
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
                target_record_id = record_id
            if not target_record_id and active_item_id:
                target_record_id = self._target_record_id_from_work_status(
                    work_type=WORK_TYPE_MAINTENANCE,
                    active_item_id=active_item_id,
                )
            record_id = source_record_id or record_id
            if action != "start" and not target_record_id:
                raise PortalError(
                    "该维保源记录未找到目标维保通告表 record_id，不能更新/结束。"
                )

        if not self._scope_matches_building(scope, building):
            raise PortalError(f"当前楼栋入口与通告楼栋不匹配: {building or '-'}")

        start_time = self._format_input_datetime(request_payload.get("start_time"))
        end_time = self._format_input_datetime(request_payload.get("end_time"))
        if not start_time or not end_time:
            raise PortalError("开始时间和结束时间不能为空。")
        location = str(request_payload.get("location") or "").strip()
        content = str(request_payload.get("content") or "").strip()
        reason = str(request_payload.get("reason") or "").strip()
        impact = str(request_payload.get("impact") or "").strip() or DEFAULT_IMPACT_TEXT
        progress = (
            str(request_payload.get("progress") or "").strip()
            or (DEFAULT_PROGRESS_TEXT if action == "start" else "")
        )
        if action == "start":
            maintenance_total = str(fields.get("维护总项") or "").strip()
            self._remember_draft_fields(
                building=building,
                maintenance_total=maintenance_total,
                location=location,
                content=content,
                reason=reason,
                impact=impact,
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
            "source_app_token": self.app_token,
            "source_table_id": self.table_id,
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
        return (
            f"【设备变更】状态：{str(status or '').strip()}\n"
            f"【名称】{str(title or '').strip()}\n"
            f"【等级】{str(level or '').strip()}\n"
            f"【时间】{MaintenancePortalService._format_input_datetime(start_time)}至"
            f"{MaintenancePortalService._format_input_datetime(end_time)}\n"
            f"【位置】{str(location or '').strip()}\n"
            f"【内容】{str(content or '').strip()}\n"
            f"【原因】{str(reason or '').strip()}\n"
            f"【影响】{str(impact or '').strip() or DEFAULT_IMPACT_TEXT}\n"
            f"【进度】{str(progress or '').strip() or DEFAULT_PROGRESS_TEXT}"
        )

    def prepare_change_action(
        self, request_payload: dict[str, Any], *, job_id: str
    ) -> dict[str, Any]:
        action = str(request_payload.get("action") or "").strip().lower()
        status_map = {"start": "开始", "update": "更新", "end": "结束"}
        status = status_map.get(action)
        if not status:
            raise PortalError("动作必须是 start/update/end。")
        self.ensure_loaded()
        scope = self._normalize_scope(request_payload.get("scope"))

        record_id = str(request_payload.get("record_id") or "").strip()
        target_record_id = ""
        fields: dict[str, Any] = {}
        if action == "start":
            if not record_id:
                raise PortalError("开始通告缺少变更源记录ID。")
            record = self._find_record_by_id(record_id, WORK_TYPE_CHANGE)
            fields = record.get("display_fields") or {}
            source_progress = self._change_progress_value(record)
            if source_progress != CHANGE_PROGRESS_NOT_STARTED:
                raise PortalError(
                    f"该变更当前源进度不是{CHANGE_PROGRESS_NOT_STARTED}: {source_progress or '-'}"
                )
            title = self._change_title(record)
            building_codes = self._change_record_building_codes(record)
            building = self._building_label_from_codes(building_codes)
            specialty = str(fields.get("专业") or "").strip()
            level = str(fields.get("变更等级（阿里）") or "").strip()
            default_start, default_end, _ = self._change_time_range(record)
        else:
            source_record_id = str(request_payload.get("source_record_id") or "").strip()
            source_record = None
            if source_record_id:
                try:
                    source_record = self._find_record_by_id(source_record_id, WORK_TYPE_CHANGE)
                    fields = source_record.get("display_fields") or {}
                except PortalError:
                    source_record = None
                    fields = {}
            title = str(request_payload.get("title") or "").strip()
            if not title and source_record is not None:
                title = self._change_title(source_record)
            if not title:
                raise PortalError("更新/结束通告缺少名称。")
            building_codes = [
                str(code or "").strip().upper()
                for code in (request_payload.get("building_codes") or [])
                if str(code or "").strip()
            ]
            if not building_codes and source_record is not None:
                building_codes = self._change_record_building_codes(source_record)
            if not building_codes:
                building_codes = self._building_codes_from_value(
                    request_payload.get("building")
                )
            building = (
                str(request_payload.get("building") or "").strip()
                or self._building_label_from_codes(building_codes)
            )
            specialty = str(request_payload.get("specialty") or "").strip()
            if not specialty and source_record is not None:
                specialty = str(fields.get("专业") or "").strip()
            level = str(request_payload.get("level") or "").strip()
            if not level and source_record is not None:
                level = str(fields.get("变更等级（阿里）") or "").strip()
            source_progress = str(request_payload.get("source_progress") or "").strip()
            if not source_progress and source_record is not None:
                source_progress = self._change_progress_value(source_record)
            default_start = ""
            default_end = ""
            record_id = source_record_id
            target_record_id = str(
                request_payload.get("target_record_id")
                or request_payload.get("record_id")
                or ""
            ).strip()
            if not target_record_id and source_record is not None:
                target_record_id = self._resolve_target_record_id_for_source_update(
                    work_type=WORK_TYPE_CHANGE,
                    notice_type=NOTICE_TYPE_CHANGE,
                    source_record=source_record,
                )

        if not self._scope_matches_buildings(scope, building_codes):
            raise PortalError(f"当前入口与通告楼栋不匹配: {building or '-'}")
        if action != "start" and source_progress == CHANGE_PROGRESS_ENDED:
            raise PortalError("该变更当前源进度已结束，不能更新/结束。")
        if action != "start" and not target_record_id:
            raise PortalError("该变更缺少目标设备变更表 record_id，不能更新/结束。")
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
        location = str(request_payload.get("location") or "").strip()
        content = str(request_payload.get("content") or "").strip() or title
        reason = str(request_payload.get("reason") or "").strip()
        impact = str(request_payload.get("impact") or "").strip() or DEFAULT_IMPACT_TEXT
        progress = (
            str(request_payload.get("progress") or "").strip()
            or (DEFAULT_PROGRESS_TEXT if action == "start" else "")
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
        response_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        return {
            "job_id": job_id,
            "work_type": WORK_TYPE_CHANGE,
            "notice_type": NOTICE_TYPE_CHANGE,
            "source_app_token": CHANGE_SOURCE_APP_TOKEN,
            "source_table_id": CHANGE_SOURCE_TABLE_ID,
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
            "start_time": start_time,
            "end_time": end_time,
            "location": location,
            "content": content,
            "reason": reason,
            "impact": impact,
            "progress": progress,
            "text": text,
            "recipients": [],
            "skip_personal_message": True,
            "response_time": response_time,
            "operation_id": str(request_payload.get("operation_id") or "").strip()
            or job_id,
        }

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
        fault_time: str,
        expected_time: str,
    ) -> str:
        progress_text = str(progress or "").strip()
        reason_text = str(reason or "").strip()
        impact_text = str(impact or "").strip()
        sections = [
            ("标题", title),
            ("地点", location),
            ("紧急程度", level),
            ("专业", specialty),
            ("发现故障时间", fault_time),
            ("期望完成时间", expected_time),
            ("维修设备", repair_device),
            ("维修故障", repair_fault),
            ("故障类型", fault_type),
            ("维修方式", repair_mode),
            ("影响范围", impact_text),
            ("故障发现方式", discovery),
            ("故障现象", symptom),
            ("故障原因", reason_text),
            ("解决方案", solution),
            ("完成情况", progress_text),
        ]
        lines = [f"【设备检修】状态：{str(status or '').strip()}"]
        for label, value in sections:
            lines.append(f"【{label}】{str(value or '').strip()}")
        return "\n\n".join(lines)

    def prepare_repair_action(
        self, request_payload: dict[str, Any], *, job_id: str
    ) -> dict[str, Any]:
        action = str(request_payload.get("action") or "").strip().lower()
        status_map = {"start": "开始", "update": "更新", "end": "结束"}
        status = status_map.get(action)
        if not status:
            raise PortalError("动作必须是 start/update/end。")
        self.ensure_loaded()
        scope = self._normalize_scope(request_payload.get("scope"))

        def request_text(name: str, default: str = "") -> str:
            if name in request_payload:
                return str(request_payload.get(name) or "").strip()
            return str(default or "").strip()

        def request_first_text(names: tuple[str, ...], default: str = "") -> str:
            for name in names:
                if name in request_payload:
                    value = str(request_payload.get(name) or "").strip()
                    if value:
                        return value
            return str(default or "").strip()

        record_id = str(request_payload.get("record_id") or "").strip()
        fields: dict[str, Any] = {}
        source_record = None
        if action == "start":
            if not record_id:
                raise PortalError("开始通告缺少检修源记录ID。")
            record = self._find_record_by_id(record_id, WORK_TYPE_REPAIR)
            fields = record.get("display_fields") or {}
            if not self._is_valid_repair_record(record):
                raise PortalError("该检修记录缺少有效维修名称或楼栋，不能发起。")
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
            specialty = str(request_payload.get("specialty") or "").strip()
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
            source_fault_time = self._repair_first_field(
                fields, "故障发生时间", "发现故障时间"
            )
            fault_time = request_text("fault_time", source_fault_time)
            target_record_id = self._repair_target_record_id(record)
        else:
            source_record_id = str(request_payload.get("source_record_id") or "").strip()
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
            specialty = str(request_payload.get("specialty") or "").strip()
            if not specialty and source_record is not None:
                specialty = self._repair_specialty(source_record)
            level = str(request_payload.get("level") or "").strip()
            if not level and source_record is not None:
                level = self._repair_level(fields)
            default_start = ""
            default_end = ""
            record_id = source_record_id
            target_record_id = str(
                request_payload.get("target_record_id")
                or request_payload.get("record_id")
                or ""
            ).strip()
            if not target_record_id and source_record is not None:
                target_record_id = self._resolve_target_record_id_for_source_update(
                    work_type=WORK_TYPE_REPAIR,
                    notice_type=NOTICE_TYPE_REPAIR,
                    source_record=source_record,
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
        location = request_text("location")
        content = request_first_text(("content", "title"), default=title)
        reason = request_text(
            "reason", self._repair_first_field(fields, "故障原因", "故障维修原因")
        )
        impact = request_text("impact")
        progress = request_text(
            "progress", self._repair_first_field(fields, "维修进展描述", "当前维修进度")
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
            fault_time=fault_time,
            expected_time=expected_time,
        )
        response_time = now_text
        return {
            "job_id": job_id,
            "work_type": WORK_TYPE_REPAIR,
            "notice_type": NOTICE_TYPE_REPAIR,
            "source_app_token": REPAIR_SOURCE_APP_TOKEN,
            "source_table_id": REPAIR_SOURCE_TABLE_ID,
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
            "fault_time": fault_time,
            "expected_time": expected_time,
            "text": text,
            "recipients": [],
            "skip_personal_message": True,
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
        self.ensure_loaded()
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

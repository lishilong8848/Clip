# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import Any

import requests

from upload_event_module.config import config
from upload_event_module.services.feishu_service import refresh_feishu_token


DEFAULT_APP_TOKEN = "HU38bc1vnamMK9sCeOgclUvXnFc"
DEFAULT_TABLE_ID = "tblzk7WrXxNWQy6V"
DEFAULT_IMPACT_TEXT = "对IT业务无影响，不会触发BA和BMS系统相关告警"
DEFAULT_PROGRESS_TEXT = "准备工作已完成，人员已就位，可否开始操作？"
DEFAULT_MAINTENANCE_STATUS = "未开始"


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
        self._last_loaded_at = ""

    def _auth_headers(self) -> dict[str, str]:
        if not config.user_token:
            refresh_feishu_token()
        token = str(config.user_token or "").strip()
        if not token:
            raise PortalError("未配置有效的飞书 user_token。")
        return {"Authorization": f"Bearer {token}"}

    def _request_json(
        self, path: str, *, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        url = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{self.app_token}/tables/{self.table_id}/{path}"
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
        items = payload.get("data", {}).get("items") or []
        metas: list[FieldMeta] = []
        for item in items:
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
        self._field_meta_list = metas
        self._field_meta_by_name = {meta.field_name: meta for meta in metas}
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
                records.append(self._normalize_record(item))
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "").strip()
            if not page_token:
                break
        self._records = records
        self._last_loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return records

    def refresh(self) -> None:
        self._load_fields()
        self._load_records()

    def ensure_loaded(self) -> None:
        if not self._field_meta_list or not self._records:
            self.refresh()

    def _normalize_field_value(self, field_name: str, raw_value: Any) -> Any:
        meta = self._field_meta_by_name.get(field_name)
        option_map = meta.options_map if meta else {}
        if raw_value is None:
            return ""
        if isinstance(raw_value, str):
            return raw_value.strip()
        if isinstance(raw_value, (int, float)):
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
                    return value
            return json.dumps(raw_value, ensure_ascii=False)
        if isinstance(raw_value, list):
            normalized_items: list[str] = []
            for item in raw_value:
                if isinstance(item, dict):
                    if "text" in item:
                        value = str(item.get("text") or "").strip()
                    elif "name" in item:
                        value = str(item.get("name") or "").strip()
                    elif "id" in item and option_map.get(str(item.get("id"))):
                        value = option_map[str(item.get("id"))]
                    else:
                        value = ""
                    if value:
                        normalized_items.append(value)
                    continue
                item_text = str(item or "").strip()
                if option_map.get(item_text):
                    normalized_items.append(option_map[item_text])
                elif item_text:
                    normalized_items.append(item_text)
            return "、".join(normalized_items)
        return str(raw_value)

    def _normalize_record(self, item: dict[str, Any]) -> dict[str, Any]:
        raw_fields = item.get("fields") or {}
        display_fields = {
            field_name: self._normalize_field_value(field_name, raw_value)
            for field_name, raw_value in raw_fields.items()
        }
        return {
            "record_id": str(item.get("record_id") or item.get("id") or ""),
            "raw_fields": raw_fields,
            "display_fields": display_fields,
        }

    @staticmethod
    def _current_month_label() -> str:
        return f"{dt.datetime.now().month}月"

    @staticmethod
    def _format_input_datetime(value: Any) -> str:
        text = str(value or "").strip()
        return text.replace("T", " ")

    def _sorted_unique_option_names(self, field_name: str) -> list[str]:
        values = {
            str(record["display_fields"].get(field_name) or "").strip()
            for record in self._records
            if record["display_fields"].get("维护实施状态") == DEFAULT_MAINTENANCE_STATUS
        }
        options = sorted([value for value in values if value])
        if options:
            return options
        meta = self._field_meta_by_name.get(field_name)
        if meta and meta.option_names:
            return [name for name in meta.option_names if name]
        return []

    def _filter_records(
        self,
        *,
        month: str = "",
        specialty: str = "",
        building: str = "",
    ) -> list[dict[str, Any]]:
        month = str(month or "").strip()
        specialty = str(specialty or "").strip()
        building = str(building or "").strip()
        filtered = []
        for record in self._records:
            display_fields = record["display_fields"]
            if display_fields.get("维护实施状态") != DEFAULT_MAINTENANCE_STATUS:
                continue
            if month and display_fields.get("计划维护月份") != month:
                continue
            if specialty and display_fields.get("专业类别") != specialty:
                continue
            if building and display_fields.get("楼栋") != building:
                continue
            filtered.append(record)
        return filtered

    def _serialize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        return {
            "record_id": record["record_id"],
            "display_fields": record["display_fields"],
        }

    def get_bootstrap(self) -> dict[str, Any]:
        self.ensure_loaded()
        default_month = self._current_month_label()
        filtered_records = self._filter_records(month=default_month)
        return {
            "app_token": self.app_token,
            "table_id": self.table_id,
            "last_loaded_at": self._last_loaded_at,
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
                "specialties": self._sorted_unique_option_names("专业类别"),
                "buildings": self._sorted_unique_option_names("楼栋"),
            },
            "records": [self._serialize_record(record) for record in filtered_records],
            "maintenance_options": [
                {
                    "record_id": record["record_id"],
                    "label": record["display_fields"].get("维护总项") or record["record_id"],
                    "sub_label": " | ".join(
                        filter(
                            None,
                            [
                                record["display_fields"].get("维护编号", ""),
                                record["display_fields"].get("维护项目", ""),
                            ],
                        )
                    ),
                }
                for record in filtered_records
            ],
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
    ) -> dict[str, Any]:
        self.ensure_loaded()
        filtered_records = self._filter_records(
            month=month, specialty=specialty, building=building
        )
        return {
            "maintenance_status": DEFAULT_MAINTENANCE_STATUS,
            "records": [self._serialize_record(record) for record in filtered_records],
            "maintenance_options": [
                {
                    "record_id": record["record_id"],
                    "label": record["display_fields"].get("维护总项") or record["record_id"],
                    "sub_label": " | ".join(
                        filter(
                            None,
                            [
                                record["display_fields"].get("维护编号", ""),
                                record["display_fields"].get("维护项目", ""),
                            ],
                        )
                    ),
                }
                for record in filtered_records
            ],
            "count": len(filtered_records),
        }

    def _find_record_by_id(self, record_id: str) -> dict[str, Any]:
        for record in self._records:
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
            text = (
                "【维保通告】状态：开始\n"
                f"【名称】EA118机房{building}{maintenance_total}\n"
                f"【时间】{start_time}~{end_time}\n"
                f"【位置】{location}\n"
                f"【内容】{content}\n"
                f"【原因】{reason}\n"
                f"【影响】{impact}\n"
                f"【进度】{DEFAULT_PROGRESS_TEXT}"
            )
            generated.append(
                {
                    "record_id": record_id,
                    "title": f"EA118机房{building}{maintenance_total}",
                    "text": text,
                }
            )
        return generated

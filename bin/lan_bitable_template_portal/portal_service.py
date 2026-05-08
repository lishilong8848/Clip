# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import threading
import uuid
import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import requests

from upload_event_module.config import config
from upload_event_module.services.feishu_service import refresh_feishu_token
from upload_event_module.services.robot_webhook import send_text_to_open_ids
from upload_event_module.utils import get_data_file_path


DEFAULT_APP_TOKEN = "HU38bc1vnamMK9sCeOgclUvXnFc"
DEFAULT_TABLE_ID = "tblzk7WrXxNWQy6V"
CHANGE_SOURCE_APP_TOKEN = "JhiVwgfoIimAqEk8YwEc09sknGd"
CHANGE_SOURCE_TABLE_ID = "tblBvg6wCYSX3hcg"
DEFAULT_IMPACT_TEXT = "对IT业务无影响，不会触发BA和BMS系统相关告警"
DEFAULT_PROGRESS_TEXT = "准备工作已完成，人员已就位，可否开始操作？"
DEFAULT_MAINTENANCE_STATUS = "未开始"
WORK_TYPE_MAINTENANCE = "maintenance"
WORK_TYPE_CHANGE = "change"
NOTICE_TYPE_MAINTENANCE = "维保通告"
NOTICE_TYPE_CHANGE = "设备变更"
CHANGE_PROGRESS_NOT_STARTED = "未开始"
CHANGE_PROGRESS_ONGOING = "进行中"
CHANGE_PROGRESS_OPTION_FALLBACK = {
    "optXQcI0z3": "未开始",
    "optqHR2ClY": "进行中",
    "optZtdlcZy": "已结束",
    "optqGq0YcR": "退回",
}
LI_SHILONG_OPEN_ID = "ou_902e364a6c2c6c20893c02abe505a7b2"
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
        self._maintenance_loaded_once = False
        self._change_loaded_once = False
        self._load_warnings: list[str] = []
        self._last_loaded_at = ""
        self._memory_dir = Path(get_data_file_path("lan_template_memory"))
        self._memory_dir.mkdir(parents=True, exist_ok=True)
        self._summary_dir = Path(get_data_file_path("lan_template_daily_summary"))
        self._summary_dir.mkdir(parents=True, exist_ok=True)
        self._work_status_dir = Path(get_data_file_path("lan_template_work_status"))
        self._work_status_dir.mkdir(parents=True, exist_ok=True)
        self._work_status_backfilled = False
        self._jobs: dict[str, dict[str, Any]] = {}
        self._jobs_lock = threading.RLock()

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

    def _parse_field_meta(self, payload: dict[str, Any]) -> list[FieldMeta]:
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
        self._last_loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

    def refresh(self) -> None:
        warnings: list[str] = []
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
        self._load_warnings = warnings

    def ensure_loaded(self) -> None:
        if not self._field_meta_list or not self._maintenance_loaded_once:
            self.refresh()
            return
        if not self._change_loaded_once:
            warnings: list[str] = []
            try:
                self._load_change_fields()
                self._load_change_records()
            except Exception as exc:
                self._change_loaded_once = True
                warnings.append(f"变更源表同步失败: {exc}")
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
        if isinstance(raw_value, str):
            text = raw_value.strip()
            if field_name == "变更进度" and CHANGE_PROGRESS_OPTION_FALLBACK.get(text):
                return CHANGE_PROGRESS_OPTION_FALLBACK[text]
            return option_map.get(text, text)
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
                    if field_name == "变更进度" and CHANGE_PROGRESS_OPTION_FALLBACK.get(value):
                        return CHANGE_PROGRESS_OPTION_FALLBACK[value]
                    return option_map.get(value, value)
            option_id = str(raw_value.get("id") or "").strip()
            if option_id:
                if field_name == "变更进度" and CHANGE_PROGRESS_OPTION_FALLBACK.get(option_id):
                    return CHANGE_PROGRESS_OPTION_FALLBACK[option_id]
                if option_map.get(option_id):
                    return option_map[option_id]
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
                    elif (
                        field_name == "变更进度"
                        and "id" in item
                        and CHANGE_PROGRESS_OPTION_FALLBACK.get(str(item.get("id")))
                    ):
                        value = CHANGE_PROGRESS_OPTION_FALLBACK[str(item.get("id"))]
                    else:
                        value = ""
                    if value:
                        if (
                            field_name == "变更进度"
                            and CHANGE_PROGRESS_OPTION_FALLBACK.get(value)
                        ):
                            value = CHANGE_PROGRESS_OPTION_FALLBACK[value]
                        elif option_map.get(value):
                            value = option_map[value]
                        normalized_items.append(value)
                    continue
                item_text = str(item or "").strip()
                if field_name == "变更进度" and CHANGE_PROGRESS_OPTION_FALLBACK.get(item_text):
                    normalized_items.append(CHANGE_PROGRESS_OPTION_FALLBACK[item_text])
                elif option_map.get(item_text):
                    normalized_items.append(option_map[item_text])
                elif item_text:
                    normalized_items.append(item_text)
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
            text = str(raw or "").strip().upper()
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
            if record["display_fields"].get("维护实施状态") == DEFAULT_MAINTENANCE_STATUS
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
            if record["display_fields"].get("维护实施状态") == DEFAULT_MAINTENANCE_STATUS
        }
        values.update(
            {
                str(record["display_fields"].get("专业") or "").strip()
                for record in self._change_records
                if str(record["display_fields"].get("变更进度") or "").strip()
                in {CHANGE_PROGRESS_NOT_STARTED, CHANGE_PROGRESS_ONGOING}
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
            if display_fields.get("维护实施状态") != DEFAULT_MAINTENANCE_STATUS:
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

    def _change_record_building_codes(self, record: dict[str, Any]) -> list[str]:
        fields = record.get("display_fields") or {}
        raw_building = fields.get("变更楼栋") or fields.get("楼栋") or ""
        return self._building_codes_from_value(raw_building)

    def _filter_change_records(
        self,
        *,
        specialty: str = "",
        scope: str = "ALL",
        progress_values: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        specialty = str(specialty or "").strip()
        scope = self._normalize_scope(scope)
        progress_values = progress_values or {
            CHANGE_PROGRESS_NOT_STARTED,
            CHANGE_PROGRESS_ONGOING,
        }
        filtered: list[dict[str, Any]] = []
        for record in self._change_records:
            fields = record.get("display_fields") or {}
            progress = str(fields.get("变更进度") or "").strip()
            if progress not in progress_values:
                continue
            if specialty and specialty != str(fields.get("专业") or "").strip():
                continue
            codes = self._change_record_building_codes(record)
            if not self._scope_matches_buildings(scope, codes):
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
            "active_item_id",
            "feishu_record_id",
            "title",
            "building",
            "building_code",
            "building_codes",
            "specialty",
            "level",
            "source_progress",
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
        counts = {WORK_TYPE_MAINTENANCE: 0, WORK_TYPE_CHANGE: 0}
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
                "maintenance_ongoing": ongoing_counts[WORK_TYPE_MAINTENANCE],
                "change_ongoing": ongoing_counts[WORK_TYPE_CHANGE],
                "closed_today": int(stats.get("ended") or 0),
            }
        return {
            "scope_options": SCOPE_OPTIONS,
            "scopes": overview,
            "last_loaded_at": self._last_loaded_at,
            "warnings": list(self._load_warnings),
        }

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
        if work_type not in {"all", WORK_TYPE_MAINTENANCE, WORK_TYPE_CHANGE}:
            raise PortalError("历史通告类型必须是 all/maintenance/change。")

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
        items: list[dict[str, Any]] = []
        for path in sorted(self._work_status_dir.glob("*.json")):
            try:
                with path.open("r", encoding="utf-8") as fp:
                    payload = json.load(fp)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            for item in payload.get("items") or []:
                if not isinstance(item, dict):
                    continue
                if self._scope_matches_item(scope, item):
                    items.append(copy.deepcopy(item))
        return items

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
        return {
            "record_id": record["record_id"],
            "source_record_id": record["record_id"],
            "work_type": str(record.get("work_type") or WORK_TYPE_MAINTENANCE),
            "notice_type": str(record.get("notice_type") or NOTICE_TYPE_MAINTENANCE),
            "source_app_token": str(record.get("source_app_token") or self.app_token),
            "source_table_id": str(record.get("source_table_id") or self.table_id),
            "display_fields": record["display_fields"],
            "memory": self._get_record_memory(record),
            "work_summary": summary_by_record.get(record["record_id"]) or {},
            "building_codes": (
                self._change_record_building_codes(record)
                if record.get("work_type") == WORK_TYPE_CHANGE
                else self._building_codes_from_value(
                    (record.get("display_fields") or {}).get("楼栋")
                )
            ),
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

    def _merge_ongoing_items(
        self,
        scope: str,
        ongoing_items: list[dict[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        scope = self._normalize_scope(scope)
        merged: list[dict[str, Any]] = []
        seen_sources: set[str] = set()
        seen_titles: set[str] = set()
        for item in ongoing_items or []:
            if not isinstance(item, dict):
                continue
            if not self._scope_matches_item(scope, item):
                continue
            copied = copy.deepcopy(item)
            copied.setdefault("work_type", WORK_TYPE_MAINTENANCE)
            copied.setdefault("notice_type", NOTICE_TYPE_MAINTENANCE)
            merged.append(copied)
            source_id = str(copied.get("source_record_id") or "").strip()
            if source_id:
                seen_sources.add(source_id)
            title_key = re.sub(r"\s+", "", str(copied.get("title") or ""))
            if title_key:
                seen_titles.add(title_key)

        for record in self._filter_change_records(
            scope=scope, progress_values={CHANGE_PROGRESS_ONGOING}
        ):
            fields = record.get("display_fields") or {}
            record_id = str(record.get("record_id") or "").strip()
            title = self._change_title(record)
            if record_id and record_id in seen_sources:
                continue
            title_key = re.sub(r"\s+", "", title)
            if title_key and title_key in seen_titles:
                continue
            codes = self._change_record_building_codes(record)
            start_time, end_time, time_text = self._change_time_range(record)
            merged.append(
                {
                    "active_item_id": f"source-change-{record_id}",
                    "record_id": "",
                    "raw_record_id": "",
                    "source_record_id": record_id,
                    "source_app_token": CHANGE_SOURCE_APP_TOKEN,
                    "source_table_id": CHANGE_SOURCE_TABLE_ID,
                    "work_type": WORK_TYPE_CHANGE,
                    "notice_type": NOTICE_TYPE_CHANGE,
                    "source_only": True,
                    "title": title,
                    "status": CHANGE_PROGRESS_ONGOING,
                    "source_progress": CHANGE_PROGRESS_ONGOING,
                    "building": self._building_label_from_codes(codes),
                    "building_codes": codes,
                    "specialty": str(fields.get("专业") or ""),
                    "level": str(fields.get("变更等级（阿里）") or ""),
                    "time": time_text,
                    "start_time": start_time,
                    "end_time": end_time,
                    "location": "",
                    "content": title,
                    "reason": "",
                    "impact": DEFAULT_IMPACT_TEXT,
                    "progress": "源表显示进行中",
                    "can_update": False,
                    "can_end": False,
                    "block_reason": "源表显示进行中；本地未发起，不能更新/结束",
                    "upload_state": "源表进行中",
                }
            )
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
            progress_values={CHANGE_PROGRESS_NOT_STARTED},
        )
        return maintenance_records + change_records

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
            "warnings": list(self._load_warnings),
            "records": [
                self._serialize_record(record, summary_by_record)
                for record in filtered_records
            ],
            "ongoing": merged_ongoing,
            "daily_summary": daily_summary,
            "record_type_counts": record_type_counts,
            "ongoing_type_counts": ongoing_type_counts,
            "maintenance_options": self._maintenance_options_for_records(filtered_records),
            "count": len(filtered_records),
        }

    def _find_record_by_id(self, record_id: str, work_type: str = WORK_TYPE_MAINTENANCE) -> dict[str, Any]:
        records = self._change_records if work_type == WORK_TYPE_CHANGE else self._records
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
            return f"{work_type}:active:{active_item_id}" if active_item_id else ""
        return ""

    def create_action_job(self, request_payload: dict[str, Any]) -> tuple[str, bool]:
        if not isinstance(request_payload, dict):
            raise PortalError("请求体格式错误。")
        action = str(request_payload.get("action") or "").strip().lower()
        if action not in {"start", "update", "end"}:
            raise PortalError("动作必须是 start/update/end。")
        if action == "start" and not str(request_payload.get("record_id") or "").strip():
            raise PortalError("开始通告缺少计划记录ID。")
        if action in {"update", "end"} and not str(
            request_payload.get("active_item_id") or ""
        ).strip():
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
        return self.prepare_maintenance_action(request_payload, job_id=job_id)

    def prepare_maintenance_action(
        self, request_payload: dict[str, Any], *, job_id: str
    ) -> dict[str, Any]:
        action = str(request_payload.get("action") or "").strip().lower()
        status_map = {"start": "开始", "update": "更新", "end": "结束"}
        status = status_map.get(action)
        if not status:
            raise PortalError("动作必须是 start/update/end。")
        if action == "start":
            self.refresh()
        else:
            self.ensure_loaded()
        scope = self._normalize_scope(request_payload.get("scope"))
        specialty = str(request_payload.get("specialty") or "").strip()

        record = None
        fields: dict[str, Any] = {}
        record_id = str(request_payload.get("record_id") or "").strip()
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
            building = str(request_payload.get("building") or "").strip()
            title = str(request_payload.get("title") or "").strip()
            if not title:
                raise PortalError("更新/结束通告缺少名称。")

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
            "active_item_id": str(request_payload.get("active_item_id") or "").strip(),
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
        if action == "start":
            self.refresh()
        else:
            self.ensure_loaded()
        scope = self._normalize_scope(request_payload.get("scope"))

        record_id = str(request_payload.get("record_id") or "").strip()
        fields: dict[str, Any] = {}
        if action == "start":
            if not record_id:
                raise PortalError("开始通告缺少变更源记录ID。")
            record = self._find_record_by_id(record_id, WORK_TYPE_CHANGE)
            fields = record.get("display_fields") or {}
            source_progress = str(fields.get("变更进度") or "").strip()
            if source_progress != CHANGE_PROGRESS_NOT_STARTED:
                raise PortalError(
                    f"该变更当前源进度不是{CHANGE_PROGRESS_NOT_STARTED}: {source_progress or '-'}"
                )
            title = self._change_title(record)
            building_codes = self._change_record_building_codes(record)
            building = self._building_label_from_codes(building_codes)
            specialty = str(fields.get("专业") or "").strip()
            level = str(fields.get("变更等级（阿里）") or "").strip()
            source_progress = CHANGE_PROGRESS_NOT_STARTED
            default_start, default_end, _ = self._change_time_range(record)
        else:
            title = str(request_payload.get("title") or "").strip()
            if not title:
                raise PortalError("更新/结束通告缺少名称。")
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
            specialty = str(request_payload.get("specialty") or "").strip()
            level = str(request_payload.get("level") or "").strip()
            source_progress = str(request_payload.get("source_progress") or "").strip()
            default_start = ""
            default_end = ""
            record_id = str(request_payload.get("source_record_id") or "").strip()

        if not self._scope_matches_buildings(scope, building_codes):
            raise PortalError(f"当前入口与通告楼栋不匹配: {building or '-'}")

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
            "active_item_id": str(request_payload.get("active_item_id") or "").strip(),
            "title": title,
            "building": building,
            "building_code": building_code,
            "building_codes": building_codes,
            "target_building": self._building_label_from_code(building_code),
            "specialty": specialty,
            "level": level,
            "source_progress": source_progress,
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

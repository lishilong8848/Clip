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
DEFAULT_IMPACT_TEXT = "对IT业务无影响，不会触发BA和BMS系统相关告警"
DEFAULT_PROGRESS_TEXT = "准备工作已完成，人员已就位，可否开始操作？"
DEFAULT_MAINTENANCE_STATUS = "未开始"
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
    {"value": "ALL", "label": "全部"},
]


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
        self._last_loaded_at = ""
        self._memory_dir = Path(get_data_file_path("lan_template_memory"))
        self._memory_dir.mkdir(parents=True, exist_ok=True)
        self._summary_dir = Path(get_data_file_path("lan_template_daily_summary"))
        self._summary_dir.mkdir(parents=True, exist_ok=True)
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

    @staticmethod
    def _building_code_from_value(value: Any) -> str:
        text = str(value or "").strip().upper()
        if "110" in text:
            return "110"
        match = re.search(r"[ABCDEH]", text)
        return match.group(0) if match else ""

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
        if "110" in text:
            return "110"
        match = re.search(r"[ABCDEH]", text)
        return match.group(0) if match else "ALL"

    @staticmethod
    def _scope_label(scope: Any) -> str:
        normalized = MaintenancePortalService._normalize_scope(scope)
        if normalized == "ALL":
            return "全部"
        if normalized == "110":
            return "110站"
        return f"{normalized}楼"

    @classmethod
    def _scope_matches_building(cls, scope: Any, building: Any) -> bool:
        normalized = cls._normalize_scope(scope)
        if normalized == "ALL":
            return True
        code = cls._building_code_from_value(building)
        return code == normalized

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
    def _summary_key(*, source_record_id: str = "", title: str = "", building: str = "") -> str:
        source_record_id = str(source_record_id or "").strip()
        if source_record_id:
            return f"source:{source_record_id}"
        title_key = re.sub(r"\s+", "", str(title or ""))
        building_key = re.sub(r"\s+", "", str(building or ""))
        return f"title:{building_key}:{title_key}" if title_key else ""

    def _find_summary_item(
        self,
        items: list[dict[str, Any]],
        *,
        key: str = "",
        active_item_id: str = "",
        title: str = "",
        building: str = "",
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
        fallback_key = self._summary_key(title=title, building=building)
        if fallback_key:
            for item in items:
                if str(item.get("fallback_key") or "") == fallback_key:
                    return item
        return None

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
        key = self._summary_key(source_record_id=source_record_id, title=title, building=building)
        fallback_key = self._summary_key(title=title, building=building)
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
            )
            if item is None:
                item = {
                    "key": key or fallback_key or uuid.uuid4().hex,
                    "fallback_key": fallback_key,
                    "source_record_id": source_record_id,
                    "active_item_id": active_item_id,
                    "feishu_record_id": feishu_record_id,
                    "title": title,
                    "building": building,
                    "building_code": str(prepared.get("building_code") or ""),
                    "specialty": str(prepared.get("specialty") or ""),
                    "status": "进行中",
                    "actions": [],
                }
                items.append(item)
            item["fallback_key"] = item.get("fallback_key") or fallback_key
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
            if self._scope_matches_building(scope, item.get("building"))
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

    def _summary_by_record_id(self, summary_items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for item in summary_items:
            record_id = str(item.get("source_record_id") or "").strip()
            if record_id:
                result[record_id] = item
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
            "display_fields": record["display_fields"],
            "memory": self._get_record_memory(record),
            "work_summary": summary_by_record.get(record["record_id"]) or {},
        }

    def get_bootstrap(
        self,
        *,
        scope: str = "ALL",
        ongoing_items: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        self.ensure_loaded()
        default_month = self._current_month_label()
        scope = self._normalize_scope(scope)
        filtered_records = self._filter_records(month=default_month, scope=scope)
        daily_summary = self.get_daily_summary(
            scope=scope, ongoing_items=ongoing_items or []
        )
        summary_by_record = self._summary_by_record_id(daily_summary.get("items") or [])
        return {
            "app_token": self.app_token,
            "table_id": self.table_id,
            "scope": scope,
            "scope_label": self._scope_label(scope),
            "scope_options": SCOPE_OPTIONS,
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
            "records": [
                self._serialize_record(record, summary_by_record)
                for record in filtered_records
            ],
            "ongoing": list(ongoing_items or []),
            "daily_summary": daily_summary,
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
        scope: str = "ALL",
        ongoing_items: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        self.ensure_loaded()
        scope = self._normalize_scope(scope)
        filtered_records = self._filter_records(
            month=month, specialty=specialty, building=building, scope=scope
        )
        daily_summary = self.get_daily_summary(
            scope=scope, ongoing_items=ongoing_items or []
        )
        summary_by_record = self._summary_by_record_id(daily_summary.get("items") or [])
        return {
            "maintenance_status": DEFAULT_MAINTENANCE_STATUS,
            "scope": scope,
            "scope_label": self._scope_label(scope),
            "records": [
                self._serialize_record(record, summary_by_record)
                for record in filtered_records
            ],
            "ongoing": list(ongoing_items or []),
            "daily_summary": daily_summary,
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
        if action == "start":
            record_id = str((request_payload or {}).get("record_id") or "").strip()
            return f"start:{record_id}" if record_id else ""
        if action in {"update", "end"}:
            active_item_id = str(
                (request_payload or {}).get("active_item_id") or ""
            ).strip()
            return f"active:{active_item_id}" if active_item_id else ""
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
        prepared = self.prepare_maintenance_action(request_payload, job_id=job_id)
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
            "action": action,
            "status": status,
            "scope": scope,
            "scope_label": self._scope_label(scope),
            "record_id": record_id,
            "active_item_id": str(request_payload.get("active_item_id") or "").strip(),
            "title": title,
            "building": building,
            "building_code": building_code,
            "target_building": self._scope_label(building_code),
            "specialty": specialty,
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

    def send_action_personal_message(self, prepared: dict[str, Any]) -> tuple[bool, str]:
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

# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import uuid
from pathlib import Path

from fastapi.responses import RedirectResponse

BIN_DIR = Path(__file__).resolve().parents[1]
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from clipflow_backend.main import FastAPIPortalController, _wait_until_listening  # noqa: E402
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME, PortalAuthManager  # noqa: E402
from lan_bitable_template_portal.portal_service import SCOPE_OPTIONS  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime, find_available_port  # noqa: E402
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class _SmokePortalService:
    _last_loaded_at = "smoke"
    _last_loaded_ts = 0.0
    _load_warnings: list[str] = []

    def __init__(self) -> None:
        self._jobs: dict[str, dict] = {}
        self._created_repair_followups: dict[str, list[dict]] = {}

    def _normalize_scope(self, scope: str) -> str:
        text = str(scope or "").strip().upper()
        if text in {"ALL", "CAMPUS", "110"}:
            return text
        for code in ("A", "B", "C", "D", "E", "H"):
            if code in text:
                return code
        return "ALL"

    def _scope_matches_item(self, scope: str, item: dict) -> bool:
        normalized = self._normalize_scope(scope)
        if normalized == "ALL":
            return True
        codes = [
            self._normalize_scope(code)
            for code in (item.get("building_codes") or [])
            if self._normalize_scope(code) not in {"", "ALL"}
        ]
        if not codes:
            return True
        if normalized == "CAMPUS":
            return len(set(codes)) >= 2
        return len(set(codes)) == 1 and codes[0] == normalized

    def get_scope_overview(
        self,
        *,
        ongoing_items: list[dict] | None = None,
        scopes: list[str] | None = None,
        include_prepared: bool = False,
    ) -> dict:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        scope_values = [str(scope or "").strip() for scope in (scopes or []) if str(scope or "").strip()]
        if not scope_values:
            scope_values = ["A", "B"]
        items: dict[str, dict] = {}
        for scope in scope_values:
            items[scope] = {
                "scope": scope,
                "maintenance_pending": 1,
                "change_pending": 1 if scope == "A" else 0,
                "repair_pending": 0,
                "maintenance_ongoing": 0,
                "change_ongoing": 0,
                "repair_ongoing": 0,
                "closed_today": 0,
                "last_loaded_at": now,
                "warnings": [],
            }
        return {
            "items": items,
            "scopes": items,
            "last_loaded_at": now,
            "warnings": [],
            "source_snapshot_ready": True,
        }

    def get_handover_links(self) -> dict:
        return {
            "scope_options": SCOPE_OPTIONS,
            "links": {
                "A": "https://example.invalid/a",
                "B": "https://example.invalid/b",
            }
        }

    def query_records(
        self,
        *,
        month: str = "",
        specialty: str = "",
        search: str = "",
        scope: str = "ALL",
        ongoing_items: list[dict] | None = None,
        work_type: str = "",
        sections: set[str] | list[str] | tuple[str, ...] | None = None,
        **_ignored: object,
    ) -> dict:
        today = time.strftime("%Y-%m-%d")
        records = [
            {
                "record_id": "src-maint-a-001",
                "source_record_id": "src-maint-a-001",
                "work_type": "maintenance",
                "notice_type": "维保通告",
                "building_codes": ["A"],
                "source_progress": "未开始",
                "maintenance_cycle": "每月",
                "display_fields": {
                    "楼栋": "A楼",
                    "维护总项": "冷机月度巡检",
                    "维护周期": "每月",
                    "维护实施状态": "未开始",
                    "专业类别": "暖通",
                },
            },
            {
                "record_id": "src-change-a-001",
                "source_record_id": "src-change-a-001",
                "work_type": "change",
                "notice_type": "变更通告",
                "building_codes": ["A"],
                "source_progress": "未开始",
                "display_fields": {
                    "变更简述": "A楼网络设备变更测试",
                    "变更楼栋": "A楼",
                    "变更进度": "未开始",
                    "专业": "网络",
                    "变更等级（阿里）": "I3",
                },
            },
        ]
        work_type_samples = [
            ("maintenance", "维保通告", "冷站过滤器维护", "每月"),
            ("change", "变更通告", "配电系统切换变更", ""),
            ("repair", "设备检修", "BA通讯中断检修", ""),
            ("power", "上电通告", "机柜上电", ""),
            ("polling", "设备轮巡", "精密空调轮巡", ""),
            ("adjust", "设备调整", "制冷单元调整", ""),
        ]
        for index in range(72):
            sample_work_type, notice_type, title, cycle = work_type_samples[index % len(work_type_samples)]
            record_id = f"src-{sample_work_type}-a-bulk-{index:03d}"
            specialty_value = ["暖通", "电气", "消防", "弱电"][index % 4]
            records.append(
                {
                    "record_id": record_id,
                    "source_record_id": record_id,
                    "work_type": sample_work_type,
                    "notice_type": notice_type,
                    "building_codes": ["A"],
                    "source_progress": "未开始",
                    "maintenance_cycle": cycle,
                    "display_fields": {
                        "楼栋": "A楼",
                        "维护总项": f"{title}{index:03d}",
                        "维护周期": cycle,
                        "维护实施状态": "未开始",
                        "专业类别": specialty_value,
                        "变更简述": f"A楼{title}{index:03d}",
                        "变更楼栋": "A楼",
                        "变更进度": "未开始",
                        "专业": specialty_value,
                        "变更等级（阿里）": "I3",
                        "检修通告名称": f"A楼{title}{index:03d}",
                        "维修名称": f"A楼{title}{index:03d}",
                        "所属数据中心/楼栋-使用": "A楼",
                    },
                }
            )
        ongoing = [
            {
                "active_item_id": "active-change-a-001",
                "record_id": "target-change-a-001",
                "target_record_id": "target-change-a-001",
                "source_record_id": "src-change-active-a-001",
                "work_type": "change",
                "notice_type": "变更通告",
                "title": "A楼UPS旁路切换变更测试",
                "building": "A楼",
                "building_codes": ["A"],
                "specialty": "供配电",
                "level": "I3",
                "start_time": f"{today}T09:30",
                "end_time": f"{today}T18:30",
                "location": "A楼配电间",
                "content": "测试测试测试",
                "reason": "测试测试测试",
                "impact": "测试测试测试",
                "progress": "测试测试测试",
                "status": "已开始",
                "uploaded": True,
            },
            {
                "active_item_id": "active-maint-manual-a-001",
                "record_id": "target-maint-manual-a-001",
                "target_record_id": "target-maint-manual-a-001",
                "work_type": "maintenance",
                "notice_type": "维保通告",
                "title": "A楼纯手填待关联维保通告",
                "building": "A楼",
                "building_codes": ["A"],
                "specialty": "暖通",
                "maintenance_cycle": "每月",
                "start_time": f"{today}T09:30",
                "end_time": f"{today}T18:30",
                "location": "A楼冷站",
                "content": "测试测试测试",
                "reason": "测试测试测试",
                "impact": "测试测试测试",
                "progress": "测试测试测试",
                "status": "已开始",
                "uploaded": True,
            }
        ]
        for index in range(54):
            sample_work_type, notice_type, title, cycle = work_type_samples[index % len(work_type_samples)]
            specialty_value = ["暖通", "电气", "消防", "弱电"][index % 4]
            ongoing.append(
                {
                    "active_item_id": f"active-{sample_work_type}-a-bulk-{index:03d}",
                    "record_id": f"target-{sample_work_type}-a-bulk-{index:03d}",
                    "target_record_id": f"target-{sample_work_type}-a-bulk-{index:03d}",
                    "source_record_id": f"src-{sample_work_type}-active-a-bulk-{index:03d}",
                    "work_type": sample_work_type,
                    "notice_type": notice_type,
                    "title": f"A楼{title}{index:03d}",
                    "building": "A楼",
                    "building_codes": ["A"],
                    "specialty": specialty_value,
                    "level": "I3",
                    "maintenance_cycle": cycle,
                    "start_time": f"{today}T09:30",
                    "end_time": f"{today}T18:30",
                    "location": "A楼测试区域",
                    "content": "测试测试测试",
                    "reason": "测试测试测试",
                    "impact": "测试测试测试",
                    "progress": "测试测试测试",
                    "device": "测试设备",
                    "cabinet": "A01",
                    "quantity": "1个",
                    "repair_device": "测试设备",
                    "repair_fault": "测试故障",
                    "fault_type": "设备故障",
                    "repair_mode": "自维",
                    "discovery": "告警自动发现",
                    "symptom": "测试现象",
                    "solution": "测试方案",
                    "status": "已开始",
                    "uploaded": True,
                }
            )
        def counts(items: list[dict]) -> dict[str, int]:
            result = {
                "maintenance": 0,
                "change": 0,
                "repair": 0,
                "power": 0,
                "polling": 0,
                "adjust": 0,
            }
            for item in items:
                key = str(item.get("work_type") or "maintenance")
                if key in result:
                    result[key] += 1
            return result

        requested_sections = {
            str(item or "").strip().lower()
            for item in (sections or [])
            if str(item or "").strip()
        } or {"records", "ongoing", "stats", "zhihang"}
        all_record_counts = counts(records)
        specialty_text = str(specialty or "").strip()
        search_text = str(search or "").strip().lower()
        filtered_records = [
            item for item in records
            if (not work_type or str(item.get("work_type") or "maintenance") == work_type)
            and (
                not specialty_text
                or specialty_text in {
                    str((item.get("display_fields") or {}).get("专业类别") or "").strip(),
                    str((item.get("display_fields") or {}).get("专业") or "").strip(),
                }
            )
            and (
                not search_text
                or search_text in " ".join(
                    str(value or "")
                    for value in [
                        item.get("record_id"),
                        item.get("notice_type"),
                        *list((item.get("display_fields") or {}).values()),
                    ]
                ).lower()
            )
        ]
        filtered_zhihang = [
            {
                "record_id": "zhihang-a-001",
                "title": "A楼智航侧变更测试",
                "building": "A楼",
                "progress": "进行中",
            }
        ] if ("zhihang" in requested_sections or work_type in {"", "change"}) else []
        return {
            "scope": scope,
            "records": filtered_records if "records" in requested_sections else [],
            "ongoing": ongoing if "ongoing" in requested_sections else [],
            "zhihang_change_records": filtered_zhihang,
            "daily_summary": {
                "date": today,
                "items": [],
                "stats": {"started": 1, "updated": 0, "ended": 0, "ongoing": 1},
            },
            "record_type_counts": all_record_counts,
            "ongoing_type_counts": counts(ongoing),
            "defaults": {
                "impact": "对业务无影响",
                "progress": "准备工作已完成",
            },
            "filters": {
                "specialties": ["暖通", "电气", "消防", "弱电"],
            },
            "warnings": [],
            "last_loaded_at": "smoke",
            "source_snapshot_ready": True,
        }

    def create_action_job(self, request_payload: dict) -> tuple[str, bool]:
        job_id = uuid.uuid4().hex
        now = time.time()
        job = {
            "job_id": job_id,
            "operation_id": str(request_payload.get("operation_id") or job_id),
            "scope": str(request_payload.get("scope") or "A"),
            "work_type": str(request_payload.get("work_type") or "maintenance"),
            "notice_type": str(request_payload.get("notice_type") or ""),
            "phase": "success",
            "status": "success",
            "accepted_at": now,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "notice_text": str(request_payload.get("notice_text") or request_payload.get("text") or ""),
            "prepared": {
                "text": str(request_payload.get("notice_text") or request_payload.get("text") or ""),
                "title": str(request_payload.get("title") or ""),
                "action": str(request_payload.get("action") or "start"),
            },
            "request": dict(request_payload),
        }
        self._jobs[job_id] = job
        return job_id, False

    def expand_workbench_action_command(
        self,
        payload: dict,
        *,
        scope: str = "ALL",
        ongoing_items: list[dict] | None = None,
    ) -> dict:
        if not isinstance(payload, dict):
            return {}
        if str(payload.get("command_format") or "") != "notice_command":
            return payload
        patch = payload.get("patch") if isinstance(payload.get("patch"), dict) else {}
        expanded = dict(patch)
        for key in (
            "scope",
            "action",
            "work_type",
            "notice_type",
            "active_item_id",
            "source_record_id",
            "target_record_id",
            "record_id",
            "operation_id",
        ):
            value = payload.get(key)
            if value not in (None, ""):
                expanded[key] = value
        expanded["scope"] = str(expanded.get("scope") or scope or "ALL")
        expanded["action"] = str(expanded.get("action") or "start").strip().lower()
        expanded["work_type"] = str(expanded.get("work_type") or "maintenance")
        return expanded

    def get_job(self, job_id: str) -> dict | None:
        return self._jobs.get(str(job_id or "").strip())

    def list_available_notice_undos(
        self,
        *,
        scope: str = "ALL",
        action_type: str = "",
        since_seconds: float = 0,
    ) -> list[dict]:
        return [
            {
                "undo_id": "undo-smoke-delete-a-001",
                "undo_action_type": "delete",
                "work_type": "change",
                "notice_type": "变更通告",
                "title": "A楼UPS旁路切换变更测试",
                "building": "A楼",
                "building_codes": ["A"],
                "undo_created_at": time.time(),
                "active_item_id": "active-change-a-001",
                "target_record_id": "target-change-a-001",
                "undo_label": "撤销删除",
            }
        ]

    def get_repair_management_records(
        self,
        *,
        scope: str = "ALL",
        query: str = "",
        limit: int = 200,
        offset: int = 0,
        focus_record_id: str = "",
        force_refresh: bool = False,
    ) -> dict:
        records = [
            {
                "record_id": "repair-mgmt-a-001",
                "title": "A楼测试检修管理记录",
                "created_time": "2026-06-01 09:00:00",
                "last_modified_time": "2026-06-01 10:00:00",
                "display_fields": {
                    "维修名称": "A楼测试检修管理记录",
                    "故障发生时间": "2026-06-25 10:00",
                    "所属数据中心/楼栋-使用": "南通A楼",
                    "所属专业": "弱电",
                    "当前维修进度": "50%",
                    "关联事件单": "A楼测试事件转检修",
                    "设备检修关联": "A楼测试设备检修",
                },
                "raw_fields": {
                    "维修名称": "A楼测试检修管理记录",
                    "故障发生时间": "2026-06-25 10:00",
                    "所属数据中心/楼栋-使用": "南通A楼",
                    "所属专业": "弱电",
                    "当前维修进度": 0.5,
                    "关联事件单": "rec-smoke-event-a-001",
                    "设备检修关联": "rec-smoke-repair-a-001",
                },
                "building_codes": ["A"],
                "source_event_id": "rec-smoke-event-a-001",
                "source_repair_ids": ["rec-smoke-repair-a-001"],
            },
            {
                "record_id": "repair-mgmt-a-002",
                "title": "A楼第二测试检修管理记录",
                "created_time": "2026-06-01 09:30:00",
                "last_modified_time": "2026-06-01 10:30:00",
                "display_fields": {
                    "维修名称": "A楼第二测试检修管理记录",
                    "所属数据中心/楼栋-使用": "南通A楼",
                    "所属专业": "电气",
                    "当前维修进度": "50%",
                    "关联事件单": "",
                    "设备检修关联": "",
                },
                "raw_fields": {
                    "维修名称": "A楼第二测试检修管理记录",
                    "所属数据中心/楼栋-使用": "南通A楼",
                    "所属专业": "电气",
                    "当前维修进度": 0.5,
                },
                "building_codes": ["A"],
                "source_event_id": "",
                "source_repair_ids": [],
            }
        ]
        for record in records:
            record_id = str(record.get("record_id") or "")
            created_followups = self._created_repair_followups.get(record_id, [])
            record["followup_count"] = 1 + len(created_followups)
            record["progress_percent"] = 50
            record["latest_followup_time"] = "2026-06-01 10:10:00"
        if str(scope or "").strip().upper() not in {"", "ALL", "A"}:
            records = []
        query_text = str(query or "").strip().lower()
        if query_text:
            records = [
                record
                for record in records
                if query_text
                in json.dumps(record, ensure_ascii=False, default=str).lower()
            ]
        page_records = records[
            max(0, int(offset or 0)) :
            max(0, int(offset or 0)) + max(1, min(int(limit or 200), 500))
        ]
        return {
            "app_token": "AnEBwJlvGiJfDdkOB32cUPuknzg",
            "table_id": "tblschT48zXwigUG",
            "fields": [
                {"field_name": "维修名称", "editable": True, "is_primary": True, "ui_type": "text"},
                {"field_name": "故障发生时间", "editable": True, "ui_type": "datetime"},
                {"field_name": "故障维修原因", "editable": True, "ui_type": "text"},
                {"field_name": "所属数据中心/楼栋-使用", "editable": True, "ui_type": "text"},
                {"field_name": "所属专业", "editable": True, "ui_type": "single_select", "options": ["弱电"]},
                {"field_name": "当前维修进度", "editable": False, "auto_filled": True, "field_type": 2, "ui_type": "progress"},
                {"field_name": "关联事件单", "editable": False, "auto_filled": True, "ui_type": "text"},
                {"field_name": "设备检修关联", "editable": False, "auto_filled": True, "ui_type": "text"},
                {"field_name": "维修跟进记录", "editable": False, "auto_filled": True, "ui_type": "text"},
                {"field_name": "创建日期", "editable": False, "ui_type": "datetime"},
            ],
            "records": page_records,
            "total": len(records),
            "returned": len(page_records),
        }

    def get_repair_management_record(
        self,
        record_id: str,
        *,
        scope: str = "ALL",
        force_refresh: bool = False,
    ) -> dict:
        payload = self.get_repair_management_records(
            scope=scope,
            limit=500,
            offset=0,
        )
        record = next(
            (
                item
                for item in payload.get("records", [])
                if str(item.get("record_id") or "") == str(record_id or "")
            ),
            None,
        )
        if record is None:
            raise RuntimeError("维修项目记录不存在")
        return {
            "record": record,
            "fields": payload.get("fields", []),
            "schema_warnings": [],
        }

    def get_repair_management_status(
        self,
        *,
        scope: str = "ALL",
        query: str = "",
        state: str = "all",
        period: str = "all",
        limit: int = 100,
        offset: int = 0,
        force_refresh: bool = False,
    ) -> dict:
        all_records = [
            {
                "record_id": "repair-mgmt-a-001",
                "title": "A楼测试事件告警描述",
                "repair_title": "A楼测试检修管理记录",
                "building": "A楼",
                "specialty": "弱电",
                "state": "in_progress",
                "status_label": "维修进行中",
                "followup_count": 1,
                "completed_followup_count": 0,
                "progress_percent": 50,
                "event_sent_time": "2026-06-01 09:50",
                "latest_followup_time": "2026-06-01 10:00",
                "latest_followup": "测试维修进展",
            },
            {
                "record_id": "repair-mgmt-a-002",
                "title": "A楼今日完成事件告警描述",
                "repair_title": "A楼今日完成检修记录",
                "building": "A楼",
                "specialty": "电气",
                "state": "completed",
                "status_label": "历史已完成",
                "followup_count": 1,
                "completed_followup_count": 1,
                "progress_percent": 100,
                "event_sent_time": "2026-06-01 10:50",
                "latest_followup_time": "2026-06-01 11:00",
                "latest_followup": "维修完成",
            },
        ]
        if str(scope or "").strip().upper() not in {"", "ALL", "A"}:
            all_records = []
        active_records = [item for item in all_records if item.get("state") != "completed"]
        completed_records = [item for item in all_records if item.get("state") == "completed"]
        if state == "completed":
            records = list(completed_records)
        elif state in {"without_followup", "in_progress"}:
            records = [item for item in active_records if item.get("state") == state]
        else:
            records = list(active_records)
        return {
            "scope": scope,
            "state": state,
            "period": period,
            "records": records,
            "total": len(records),
            "returned": len(records),
            "offset": offset,
            "has_more": False,
            "stats": {
                "total": len(active_records),
                "without_followup": sum(
                    1 for item in active_records if item.get("state") == "without_followup"
                ),
                "in_progress": sum(
                    1 for item in active_records if item.get("state") == "in_progress"
                ),
                "completed_total": len(completed_records),
                "completed_month": len(completed_records),
                "completed_week": len(completed_records),
                "completed_today": len(completed_records),
                "average_progress": 50 if active_records else 0,
            },
        }

    def create_repair_management_record(
        self,
        fields: dict,
        *,
        operation_id: str = "",
        source_event_id: str = "",
        source_repair_ids=None,
        source_month: str = "",
        scope: str = "ALL",
    ) -> dict:
        return {"record_id": "repair-mgmt-created-001", "fields": dict(fields or {}), "field_count": len(fields or {})}

    def repair_management_notice_prefill(
        self,
        record_id: str,
        *,
        scope: str = "ALL",
    ) -> dict:
        return {
            "repair_management_record_id": record_id,
            "source_record_id": record_id,
            "target_record_id": "rec-smoke-repair-a-001",
            "action": "update",
            "draft": {
                "work_type": "repair",
                "notice_type": "设备检修",
                "title": "A楼测试检修管理记录",
                "location": "A楼弱电间",
                "level": "低",
                "specialty": "弱电",
                "start_time": "2026-06-25 18:00",
                "end_time": "2026-06-25 10:00",
                "repair_device": "A-219-CRAH-01",
                "repair_fault": "测试告警",
                "fault_type": "设备故障",
                "repair_mode": "自维",
                "impact": "不影响业务",
                "discovery": "BMS",
                "symptom": "通讯中断",
                "reason": "测试原因",
                "solution": "测试方案",
                "spare_parts": "无",
                "progress": "处理中",
            },
        }

    def update_repair_management_record(
        self,
        record_id: str,
        fields: dict,
        *,
        source_event_id: str = "",
        source_repair_ids=None,
        replace_source_relations: bool = False,
        source_month: str = "",
        scope: str = "ALL",
    ) -> dict:
        return {"record_id": record_id, "fields": dict(fields or {}), "field_count": len(fields or {})}

    def delete_repair_management_record(self, record_id: str, *, scope: str = "ALL") -> dict:
        return {"record_id": record_id, "deleted": True}

    def list_repair_management_event_candidates(
        self,
        *,
        scope: str = "ALL",
        month: str = "",
        query: str = "",
        limit: int = 50,
    ) -> dict:
        return {
            "scope": scope,
            "month": month,
            "records": [
                {
                    "record_id": "rec-smoke-event-a-001",
                    "title": "A楼测试事件转检修",
                    "building": "A楼",
                    "building_codes": ["A"],
                    "specialty": "弱电",
                    "level": "I3",
                    "source": "BMS",
                    "status": "处理中",
                    "occurrence_time": "2026-06-25 10:00",
                }
            ],
            "total": 1,
            "returned": 1,
        }

    def repair_management_event_prefill(
        self,
        *,
        scope: str = "ALL",
        record_id: str = "",
        month: str = "",
    ) -> dict:
        return {
            "event": {
                "record_id": record_id,
                "title": "A楼测试事件转检修",
                "building": "A楼",
                "specialty": "弱电",
            },
            "fields": {
                "维修名称": "A楼测试事件转检修",
                "楼栋": "A楼",
                "专业": "弱电",
            },
        }

    def list_repair_management_repair_candidates(
        self,
        *,
        scope: str = "ALL",
        event_record_id: str = "",
        month: str = "",
        query: str = "",
        limit: int = 80,
    ) -> dict:
        return {
            "scope": scope,
            "event_record_id": event_record_id,
            "records": [
                {
                    "record_id": "rec-smoke-repair-a-001",
                    "title": "A楼测试设备检修",
                    "building": "A楼",
                    "specialty": "弱电",
                    "status": "开始",
                    "score": 96,
                    "recommended": True,
                }
            ],
            "auto_selected_ids": ["rec-smoke-repair-a-001"],
        }

    def list_repair_management_cmdb_candidates(
        self,
        *,
        scope: str = "ALL",
        query: str = "",
        limit: int = 160,
    ) -> dict:
        return {
            "scope": scope,
            "records": [
                {
                    "record_id": "rec-smoke-cmdb-a-001",
                    "title": "A-219-CRAH-01",
                    "unique_id": "ZH-A-219-01",
                    "category": "精密空调",
                    "building": "A楼",
                    "location": "A楼A-219空调间靠近北侧配电柜的精密空调设备区域",
                },
                {
                    "record_id": "rec-smoke-cmdb-a-002",
                    "title": "A-220-CRAH-02",
                    "unique_id": "ZH-A-220-02",
                    "category": "精密空调",
                    "building": "A楼",
                    "location": "A楼A-220空调间靠近南侧配电柜的精密空调设备区域",
                },
            ],
            "total": 2,
            "returned": 2,
        }

    def get_repair_followup_records(
        self,
        *,
        summary_record_id: str,
        scope: str = "ALL",
        query: str = "",
        limit: int = 100,
        offset: int = 0,
        focus_record_id: str = "",
        force_refresh: bool = False,
    ) -> dict:
        is_second_project = summary_record_id == "repair-mgmt-a-002"
        if is_second_project:
            time.sleep(0.35)
        followup_record_id = (
            "rec-smoke-followup-a-002"
            if is_second_project
            else "rec-smoke-followup-a-001"
        )
        followup_title = (
            "A楼第二项目跟进记录"
            if is_second_project
            else "A楼测试维修跟进记录"
        )
        records = [
            {
                "record_id": followup_record_id,
                "title": followup_title,
                "created_time": "2026-06-25 10:10",
                "progress": "0.5",
                "cmdb_record_ids": ["rec-smoke-cmdb-a-001"],
                "display_fields": {
                    "设备名称": "精密空调",
                    "设备编号": "A-219-CRAH-01",
                    "设备品牌": "双登",
                    "设备型号": "GFMHR-1250W",
                    "随工人员（我方维修人员）": "张宇航",
                    "维修进展描述": "处理中",
                    "维修进度": "0.5",
                    "超链接": "https://example.invalid/followup",
                },
                "raw_fields": {
                    "随工人员（我方维修人员）": [
                        {"id": "ou-smoke-zhang", "name": "张宇航"}
                    ]
                },
            },
            *self._created_repair_followups.get(summary_record_id, []),
        ]
        query_text = str(query or "").strip().lower()
        if query_text:
            records = [
                item
                for item in records
                if query_text in str(item.get("title") or "").lower()
                or query_text in str(item.get("display_fields") or "").lower()
            ]
        total = len(records)
        page_limit = max(1, limit)
        page_offset = max(0, offset)
        if focus_record_id:
            focus_index = next(
                (
                    index
                    for index, item in enumerate(records)
                    if str(item.get("record_id") or "") == focus_record_id
                ),
                -1,
            )
            if focus_index >= 0:
                page_offset = (focus_index // page_limit) * page_limit
        paged_records = records[page_offset : page_offset + page_limit]
        return {
            "summary_record_id": summary_record_id,
            "fields": [
                {"field_name": "设备名称", "field_type": 3, "ui_type": "SingleSelect", "options": ["精密空调"], "editable": True},
                {"field_name": "设备编号", "field_type": 1, "ui_type": "Text", "options": [], "editable": True},
                {"field_name": "设备品牌", "field_type": 3, "ui_type": "SingleSelect", "options": ["双登", "康明斯", "圣阳"], "editable": True},
                {"field_name": "设备型号", "field_type": 3, "ui_type": "SingleSelect", "options": ["GFMHR-1250W", "C2500 D5A", "SP12-100", "SP12-200"], "editable": True},
                {"field_name": "随工人员（我方维修人员）", "field_type": 11, "ui_type": "User", "options": [], "editable": True},
                {"field_name": "维修进展描述", "field_type": 1, "ui_type": "Text", "options": [], "editable": True},
                {"field_name": "维修进度", "field_type": 2, "ui_type": "Progress", "options": [], "editable": True},
                {"field_name": "超链接", "field_type": 15, "ui_type": "Url", "options": [], "editable": True},
            ],
            "records": paged_records,
            "total": total,
            "returned": len(paged_records),
            "offset": page_offset,
            "has_more": page_offset + len(paged_records) < total,
            "brand_model_options": {
                "双登": ["GFMHR-1250W"],
                "康明斯": ["C2500 D5A"],
                "圣阳": ["SP12-100", "SP12-200"],
            },
        }

    def list_repair_followup_people(
        self,
        *,
        scope: str = "ALL",
        query: str = "",
        limit: int = 80,
        refresh: bool = False,
    ) -> dict:
        people = [
            {
                "person_record_id": "rec-smoke-person-zhang",
                "user_id": "ou-smoke-zhang",
                "name": "张宇航",
                "employee_no": "10001",
                "building": "A楼",
                "position": "运维值班员",
                "selectable": True,
            }
        ]
        query_text = str(query or "").strip()
        if query_text:
            people = [item for item in people if query_text in item["name"]]
        return {
            "scope": scope,
            "people": people[:limit],
            "total": len(people),
            "returned": min(len(people), limit),
        }

    def create_repair_followup_record(
        self,
        *,
        summary_record_id: str,
        fields: dict,
        cmdb_record_ids: list[str] | None = None,
        operation_id: str = "",
        scope: str = "ALL",
    ) -> dict:
        created_records = self._created_repair_followups.setdefault(summary_record_id, [])
        record_id = f"rec-smoke-followup-created-{len(created_records) + 1}"
        display_fields = dict(fields or {})
        created_records.append(
            {
                "record_id": record_id,
                "title": str(display_fields.get("维修进展描述") or "新增维修跟进记录"),
                "created_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "progress": str(display_fields.get("维修进度") or "0"),
                "cmdb_record_ids": list(cmdb_record_ids or []),
                "display_fields": display_fields,
                "raw_fields": {},
            }
        )
        return {"record_id": record_id, "warnings": []}

    def update_repair_followup_record(
        self,
        record_id: str,
        *,
        summary_record_id: str,
        fields: dict,
        cmdb_record_ids: list[str] | None = None,
        scope: str = "ALL",
    ) -> dict:
        for item in self._created_repair_followups.get(summary_record_id, []):
            if str(item.get("record_id") or "") != record_id:
                continue
            item["display_fields"] = {
                **dict(item.get("display_fields") or {}),
                **dict(fields or {}),
            }
            if "维修进度" in (fields or {}):
                item["progress"] = str((fields or {}).get("维修进度") or "0")
            if cmdb_record_ids is not None:
                item["cmdb_record_ids"] = list(cmdb_record_ids)
            break
        return {"record_id": record_id, "warnings": []}

    def delete_repair_followup_record(
        self,
        record_id: str,
        *,
        summary_record_id: str,
        scope: str = "ALL",
    ) -> dict:
        records = self._created_repair_followups.get(summary_record_id, [])
        self._created_repair_followups[summary_record_id] = [
            item for item in records if str(item.get("record_id") or "") != record_id
        ]
        return {"record_id": record_id, "deleted": True, "warnings": []}

    def repair_management_combined_prefill(
        self,
        *,
        scope: str = "ALL",
        event_record_id: str = "",
        repair_record_ids=None,
        month: str = "",
    ) -> dict:
        return {
            "event": {
                "record_id": event_record_id,
                "title": "A楼测试事件转检修",
                "building": "A楼",
                "specialty": "弱电",
            },
            "fields": {
                "维修名称": "A楼测试事件转检修",
                "故障维修原因": "测试告警",
                "所属数据中心/楼栋-使用": "南通A楼",
                "所属专业": "弱电",
                "关联事件单": event_record_id,
                "设备检修关联": str((repair_record_ids or [""])[0] or ""),
            },
            "relations": {
                "event_record_id": event_record_id,
                "repair_record_ids": list(repair_record_ids or []),
            },
            "warnings": [],
        }


def _build_playwright_script(url: str, session_id: str) -> str:
    no_scope_session_id = f"{session_id}-no-scope"
    payload = {
        "url": url,
        "session_id": session_id,
        "no_scope_session_id": no_scope_session_id,
        "cookie_name": AUTH_COOKIE_NAME,
    }
    return textwrap.dedent(
        f"""
        const {{ chromium }} = require('playwright');
        const cfg = {json.dumps(payload, ensure_ascii=False)};

        (async () => {{
          const browser = await chromium.launch({{ headless: true }});
          const errors = [];
          const failedResponses = [];

          function attachDiagnostics(targetPage, label) {{
            targetPage.on('console', msg => {{
              if (msg.type() === 'error') errors.push(`${{label}}: ${{msg.text()}}`);
            }});
            targetPage.on('pageerror', err => errors.push(`${{label}}: ${{String(err)}}`));
            targetPage.on('response', response => {{
              if (response.status() >= 400) {{
                failedResponses.push(`${{label}}: ${{response.status()}} ${{response.url()}}`);
              }}
            }});
          }}

          async function assertLayout(targetPage, stage) {{
            const issues = await targetPage.evaluate(() => {{
              const result = [];
              const rectOf = selector => {{
                const node = document.querySelector(selector);
                if (!node) return null;
                const rect = node.getBoundingClientRect();
                return {{
                  left: rect.left,
                  right: rect.right,
                  top: rect.top,
                  bottom: rect.bottom,
                  width: rect.width,
                  height: rect.height,
                }};
              }};
              const intersects = (a, b) => {{
                if (!a || !b) return false;
                return !(a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top);
              }};
              if (document.documentElement.scrollWidth > window.innerWidth + 4) {{
                result.push(`horizontal overflow ${{document.documentElement.scrollWidth}}>${{window.innerWidth}}`);
              }}
              const titleRect = rectOf('.brand h1');
              if (!titleRect || titleRect.width < 240) result.push('brand title missing or too narrow');
              if (titleRect && titleRect.height > 46) result.push(`brand title wrapped: ${{Math.round(titleRect.height)}}px`);
              const brandRect = rectOf('.brand');
              const actionsRect = rectOf('.topbar-actions');
              if (intersects(brandRect, actionsRect)) result.push('topbar brand overlaps actions');
              const summaryCards = Array.from(document.querySelectorAll('.summary-strip article'));
              if (summaryCards.length) {{
                const narrow = summaryCards
                  .map((node, index) => [index + 1, node.getBoundingClientRect().width])
                  .filter(([, width]) => width < 190);
                if (narrow.length) result.push(`summary cards too narrow: ${{JSON.stringify(narrow)}}`);
              }}
              const panels = Array.from(document.querySelectorAll('.workspace > .panel'));
              if (panels.length) {{
                const rails = Array.from(document.querySelectorAll('.rail-fold .rail-panel, .result-rail .rail-panel'));
                const noticeDrawer = document.querySelector('.notice-detail-overlay > .notice-detail-drawer');
                if ((panels.length < 2 && !noticeDrawer) || rails.length < 1) {{
                  result.push(`workspace panel count ${{panels.length}}, folded rail count ${{rails.length}}`);
                }}
                const narrow = panels
                  .map((node, index) => [index + 1, node.getBoundingClientRect().width])
                  .filter(([, width]) => width < 250);
                if (narrow.length) result.push(`workspace panels too narrow: ${{JSON.stringify(narrow)}}`);
              }}
              return result;
            }});
            if (issues.length) throw new Error(`layout issues at ${{stage}}: ${{issues.join('; ')}}`);
          }}
          async function assertVnetSkin(targetPage, stage) {{
            await targetPage.waitForFunction(() => Boolean(document.querySelector(
              '.center-state, .feature-card, .panel, .permission-row, .match-layout, .module-card, .home-metrics article'
            )), null, {{ timeout: 10000 }}).catch(() => null);
            const issues = await targetPage.evaluate(() => {{
              const result = [];
              const styleText = (node, prop) => node ? String(getComputedStyle(node)[prop] || '') : '';
              const backgroundText = (node) => {{
                const image = styleText(node, 'backgroundImage');
                if (image && image !== 'none') return image;
                return styleText(node, 'backgroundColor');
              }};
              const topbar = document.querySelector('.app-topbar, .topbar');
              const logo = document.querySelector('.brand-logo');
              const card = document.querySelector('.center-state, .feature-card, .panel, .permission-row, .match-layout, .module-card, .home-metrics article');
              const primaryButton = document.querySelector('.btn.blue, button.primary, a.primary, .module-actions button.primary, .scope-actions .primary');
              const topbarBackground = backgroundText(topbar);
              const cardRadius = styleText(card, 'borderRadius');
              const cardBackground = backgroundText(card);
              const primaryBackground = backgroundText(primaryButton);
              if (!logo) result.push('official logo missing');
              if (!topbarBackground.includes('gradient')) result.push(`topbar is not gradient: ${{topbarBackground}}`);
              if (!card) result.push('main white card missing');
              const numericRadius = Number.parseFloat(cardRadius || '0');
              if (card && (!Number.isFinite(numericRadius) || numericRadius < 12 || numericRadius > 28)) {{
                result.push(`card radius not VNET-like: ${{cardRadius}}`);
              }}
              if (card && !/(255, 255, 255|#fff|white)/i.test(cardBackground)) {{
                result.push(`card is not white/light: ${{cardBackground}}`);
              }}
              if (primaryButton && !primaryBackground.includes('gradient')) {{
                result.push(`primary button is not gradient: ${{primaryBackground}}`);
              }}
              return result;
            }});
            if (issues.length) throw new Error(`VNET skin issues at ${{stage}}: ${{issues.join('; ')}}`);
          }}
          async function assertHeaderSubtitle(targetPage, expected, stage) {{
            await targetPage.waitForFunction(
              expectedText => Boolean(document.querySelector('.brand')?.innerText.includes(expectedText)),
              expected,
              {{ timeout: 10000 }},
            );
            const headerText = await targetPage.locator('.brand').innerText({{ timeout: 10000 }});
            if (!headerText.includes(expected)) {{
              throw new Error(`header subtitle mismatch at ${{stage}}: expected "${{expected}}", got "${{headerText}}"`);
            }}
            if (/HTTP\\s+\\d+/i.test(headerText)) {{
              throw new Error(`header exposes technical HTTP status at ${{stage}}: ${{headerText}}`);
            }}
          }}
          async function eventSourceUrls(targetPage) {{
            return await targetPage.evaluate(() => Array.from(window.__clipflowActiveEventSourceUrls || []));
          }}
          async function waitForTextOrDump(targetPage, text, stage) {{
            try {{
              await targetPage.waitForSelector(`text=${{text}}`, {{ timeout: 10000 }});
            }} catch (err) {{
              const bodyText = await targetPage.locator('body').innerText({{ timeout: 10000 }}).catch(() => '');
              const workbenchPayload = await targetPage.evaluate(async () => {{
                try {{
                  const params = new URLSearchParams(window.location.search);
                  const scope = params.get('scope') || 'A';
                  const workType = params.get('work_type') || '';
                  const url = `/api/workbench?scope=${{encodeURIComponent(scope)}}&work_type=${{encodeURIComponent(workType)}}&sections=records,ongoing,stats`;
                  const response = await fetch(url, {{ credentials: 'same-origin' }});
                  return await response.json();
                }} catch (fetchErr) {{
                  return {{ fetch_error: String(fetchErr) }};
                }}
              }}).catch((fetchErr) => ({{ evaluate_error: String(fetchErr) }}));
              throw new Error(`${{stage}} missing text "${{text}}". url=${{targetPage.url()}} workbench=${{JSON.stringify(workbenchPayload).slice(0, 1200)}} body=${{bodyText.slice(0, 1400)}}`);
            }}
          }}

          const unauthContext = await browser.newContext();
          const unauthPage = await unauthContext.newPage();
          attachDiagnostics(unauthPage, 'unauth');
          const unauthResponse = await unauthPage.goto(cfg.url, {{
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          }});
          if (!unauthResponse || !unauthResponse.ok()) {{
            throw new Error(`unauth page load failed: ${{unauthResponse && unauthResponse.status()}}`);
          }}
          await unauthPage.waitForSelector('text=南通基地-运维灯塔工作台', {{ timeout: 10000 }});
          await unauthPage.waitForSelector('text=飞书扫码登录', {{ timeout: 10000 }});
          const unauthText = await unauthPage.locator('body').innerText({{ timeout: 10000 }});
          if (!unauthText.includes('请先使用飞书登录')) throw new Error('missing unauth login prompt');
          await assertHeaderSubtitle(unauthPage, '功能选择 · 请先登录', 'unauth');
          await assertLayout(unauthPage, 'unauth');
          await assertVnetSkin(unauthPage, 'unauth');
          await unauthContext.close();

          const noScopeContext = await browser.newContext();
          await noScopeContext.addCookies([{{
            name: cfg.cookie_name,
            value: cfg.no_scope_session_id,
            domain: '127.0.0.1',
            path: '/',
          }}]);
          const noScopePage = await noScopeContext.newPage();
          attachDiagnostics(noScopePage, 'no-scope');
          const noScopeResponse = await noScopePage.goto(cfg.url, {{
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          }});
          if (!noScopeResponse || !noScopeResponse.ok()) {{
            throw new Error(`no-scope page load failed: ${{noScopeResponse && noScopeResponse.status()}}`);
          }}
          await noScopePage.waitForSelector('text=当前账号暂无门户权限', {{ timeout: 10000 }});
          await noScopePage.waitForSelector('text=提交给管理员', {{ timeout: 10000 }});
          await assertHeaderSubtitle(noScopePage, '功能选择 · 申请访问权限', 'no-scope');
          const scopePillCount = await noScopePage.locator('.scope-pill').count();
          if (scopePillCount < 8) throw new Error(`permission request scope pills too few: ${{scopePillCount}}`);
          await noScopePage.locator('.scope-pill').filter({{ hasText: 'A楼' }}).click();
          const selectedPillText = await noScopePage.locator('.scope-pill.selected').innerText({{ timeout: 10000 }});
          if (!selectedPillText.includes('A楼')) throw new Error(`permission request pill selection failed: ${{selectedPillText}}`);
          await assertLayout(noScopePage, 'no-scope');
          await assertVnetSkin(noScopePage, 'no-scope');
          await noScopeContext.close();

          const context = await browser.newContext();
          await context.addInitScript(() => {{
            const NativeEventSource = window.EventSource;
            if (!NativeEventSource || window.__clipflowEventSourcePatched) return;
            window.__clipflowEventSourceUrls = [];
            window.__clipflowActiveEventSourceUrls = [];
            window.EventSource = function(url, options) {{
              const urlText = String(url);
              window.__clipflowEventSourceUrls.push(urlText);
              window.__clipflowActiveEventSourceUrls.push(urlText);
              const source = new NativeEventSource(url, options);
              const nativeClose = source.close.bind(source);
              source.close = function() {{
                const index = window.__clipflowActiveEventSourceUrls.indexOf(urlText);
                if (index >= 0) window.__clipflowActiveEventSourceUrls.splice(index, 1);
                return nativeClose();
              }};
              return source;
            }};
            window.EventSource.prototype = NativeEventSource.prototype;
            window.__clipflowEventSourcePatched = true;
          }});
          await context.addCookies([{{
            name: cfg.cookie_name,
            value: cfg.session_id,
            domain: '127.0.0.1',
            path: '/',
          }}]);
          const page = await context.newPage();
          let repairRecordRequestCount = 0;
          let repairStatusRequestCount = 0;
          let repairEventCandidateRequestCount = 0;
          let repairNoticeCandidateRequestCount = 0;
          page.on('request', request => {{
            if (request.url().includes('/api/repair-management/records?')) {{
              repairRecordRequestCount += 1;
            }}
            if (request.url().includes('/api/repair-management/status?')) {{
              repairStatusRequestCount += 1;
            }}
            if (request.url().includes('/api/repair-management/event-candidates?')) {{
              repairEventCandidateRequestCount += 1;
            }}
            if (request.url().includes('/api/repair-management/repair-candidates?')) {{
              repairNoticeCandidateRequestCount += 1;
            }}
          }});
          attachDiagnostics(page, 'auth');
          const response = await page.goto(cfg.url, {{
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          }});
          if (!response || !response.ok()) {{
            throw new Error(`page load failed: ${{response && response.status()}}`);
          }}
          await page.waitForSelector('text=南通基地-运维灯塔工作台', {{ timeout: 10000 }});
          await page.waitForSelector('text=功能选择', {{ timeout: 10000 }});
          await assertHeaderSubtitle(page, '功能选择 · 请选择功能', 'home');
          await assertVnetSkin(page, 'home');
          const bodyText = await page.locator('body').innerText({{ timeout: 10000 }});
          const required = ['功能选择', '事件管理', '维护管理', '检修管理', '变更管理', '其他工具'];
          for (const marker of required) {{
            if (!bodyText.includes(marker)) throw new Error(`missing marker: ${{marker}}`);
          }}
          const forbidden = ['Vue migration workspace', '当前生产页面仍由 legacy index.html 提供'];
          for (const marker of forbidden) {{
            if (bodyText.includes(marker)) throw new Error(`legacy marker visible: ${{marker}}`);
          }}
          await assertLayout(page, 'home');
          await page.locator('.module-card.slate').getByRole('button', {{ name: '交接班', exact: true }}).click();
          try {{
            await page.waitForFunction(() => document.body.innerText.includes('选择楼栋打开交接班审核页'), null, {{ timeout: 10000 }});
          }} catch (err) {{
            const currentText = await page.locator('body').innerText({{ timeout: 10000 }});
            throw new Error(`handover entry did not open. Current page text: ${{currentText.slice(0, 800)}}`);
          }}
          const handoverScopeCard = page.locator('article.scope-card').filter({{ hasText: 'A楼' }}).first();
          await handoverScopeCard.getByRole('link', {{ name: '打开审核页' }}).waitFor({{ timeout: 10000 }});
          const handoverHref = await handoverScopeCard.getByRole('link', {{ name: '打开审核页' }}).getAttribute('href');
          if (handoverHref !== 'https://example.invalid/a') {{
            throw new Error(`handover link href mismatch: ${{handoverHref}}`);
          }}
          await assertLayout(page, 'handover-feature');
          await page.getByRole('button', {{ name: /^返回$/ }}).first().click();
          try {{
            await page.waitForSelector('text=业务工作台', {{ timeout: 2500 }});
          }} catch (err) {{
            await page.getByRole('button', {{ name: /^返回$/ }}).first().click();
            await page.waitForSelector('text=业务工作台', {{ timeout: 10000 }});
          }}
          await assertHeaderSubtitle(page, '功能选择 · 请选择功能', 'home-after-handover');
          await page.getByRole('button', {{ name: '进入检修单管理', exact: true }}).click();
          await page.waitForSelector('text=选择楼栋进入检修单管理', {{ timeout: 10000 }});
          const repairScopeCard = page.locator('article.scope-card').filter({{ hasText: 'A楼' }}).first();
          await repairScopeCard.getByRole('button', {{ name: '进入检修单管理' }}).click();
          await waitForTextOrDump(page, '维修项目与跟进', 'repair-management-entry');
          await waitForTextOrDump(page, 'A楼测试检修管理记录', 'repair-management-entry');
          if (repairEventCandidateRequestCount !== 0 || repairNoticeCandidateRequestCount !== 0) {{
            throw new Error(
              `repair source candidates were eagerly loaded: event=${{repairEventCandidateRequestCount}}, repair=${{repairNoticeCandidateRequestCount}}`,
            );
          }}
          await page.getByRole('button', {{ name: '检修状态', exact: true }}).click();
          await waitForTextOrDump(page, '当前状态项目', 'repair-status-entry');
          await waitForTextOrDump(page, 'A楼测试事件告警描述', 'repair-status-entry');
          await page.getByText('2026-06-01 09:50', {{ exact: true }}).first().waitFor({{ state: 'visible' }});
          await page.getByRole('button', {{ name: /^历史已完成/ }}).click();
          await page.locator('.status-workspace').getByText('A楼今日完成事件告警描述', {{ exact: true }}).waitFor({{ state: 'visible' }});
          await page.locator('.history-period-tabs').getByRole('button', {{ name: /^全部/ }}).waitFor({{ state: 'visible' }});
          await page.locator('.history-period-tabs').getByRole('button', {{ name: /^本月/ }}).click();
          await page.locator('.status-workspace').getByText('A楼今日完成事件告警描述', {{ exact: true }}).waitFor({{ state: 'visible' }});
          await page.getByRole('button', {{ name: /^当前状态项目/ }}).click();
          await page.locator('.status-workspace').getByText('A楼测试事件告警描述', {{ exact: true }}).waitFor({{ state: 'visible' }});
          const repairRequestsBeforeOpen = repairRecordRequestCount;
          await page.locator('.status-row').filter({{ hasText: 'A楼测试事件告警描述' }}).getByRole('button', {{ name: '跟进检修管理' }}).click();
          await waitForTextOrDump(page, 'A楼测试检修管理记录', 'repair-management-return');
          await page.waitForTimeout(250);
          if (repairRecordRequestCount !== repairRequestsBeforeOpen) {{
            throw new Error(`cached repair management page reloaded: ${{repairRequestsBeforeOpen}} -> ${{repairRecordRequestCount}}`);
          }}
          await page.getByRole('button', {{ name: '关闭维修项目', exact: true }}).click();
          const statusRequestsBeforeReturn = repairStatusRequestCount;
          await page.getByRole('button', {{ name: '检修状态', exact: true }}).click();
          await waitForTextOrDump(page, '当前状态项目', 'repair-status-cached-return');
          await page.waitForTimeout(250);
          if (repairStatusRequestCount !== statusRequestsBeforeReturn) {{
            throw new Error(`cached repair status page reloaded: ${{statusRequestsBeforeReturn}} -> ${{repairStatusRequestCount}}`);
          }}
          await page.locator('.status-row').filter({{ hasText: 'A楼测试事件告警描述' }}).getByRole('button', {{ name: '跟进检修管理' }}).click();
          await waitForTextOrDump(page, 'A楼测试检修管理记录', 'repair-management-second-return');
          await page.getByRole('button', {{ name: /^跟进记录/ }}).first().click();
          const crossProjectFollowupPanel = page.locator('.followup-panel');
          await crossProjectFollowupPanel.locator('.followup-editor-head strong').filter({{ hasText: 'A楼测试维修跟进记录' }}).waitFor({{ state: 'visible' }});
          await page.getByRole('button', {{ name: '关闭维修项目', exact: true }}).click();
          await page.locator('.record-row').filter({{ hasText: 'A楼第二测试检修管理记录' }}).click();
          await page.getByRole('button', {{ name: /^跟进记录/ }}).first().click();
          await crossProjectFollowupPanel.getByText('正在读取当前维修项目的跟进记录...', {{ exact: true }}).waitFor({{ state: 'visible' }});
          if (await crossProjectFollowupPanel.getByText('A楼测试维修跟进记录', {{ exact: true }}).count()) {{
            throw new Error('previous project followup leaked into the newly selected repair project');
          }}
          await crossProjectFollowupPanel.locator('.followup-editor-head strong').filter({{ hasText: 'A楼第二项目跟进记录' }}).waitFor({{ state: 'visible' }});
          await page.getByRole('button', {{ name: '关闭维修项目', exact: true }}).click();
          await page.locator('.record-row').filter({{ hasText: 'A楼测试检修管理记录' }}).click();
          await page.getByRole('button', {{ name: '维修单信息', exact: true }}).click();
          await page.getByRole('button', {{ name: '更改事件检修关联', exact: true }}).click();
          const sourceRelationPanel = page.locator('.source-relation-panel');
          await sourceRelationPanel.getByText('A楼测试事件转检修', {{ exact: true }}).waitFor({{ state: 'visible' }});
          await sourceRelationPanel.getByText('A楼测试设备检修', {{ exact: true }}).waitFor({{ state: 'visible' }});
          await page.locator('.source-relation-item').filter({{ hasText: '关联事件单' }}).getByRole('button', {{ name: '重新选择', exact: true }}).click();
          if (repairEventCandidateRequestCount !== 1) {{
            throw new Error(`event candidates should load on demand once, got ${{repairEventCandidateRequestCount}}`);
          }}
          const eventPicker = page.getByRole('dialog', {{ name: '选择关联事件单' }});
          await eventPicker.locator('tbody tr').filter({{ hasText: 'A楼测试事件转检修' }}).click();
          await eventPicker.getByRole('button', {{ name: '确认', exact: true }}).click();
          if (repairNoticeCandidateRequestCount !== 0) {{
            throw new Error(`repair candidates loaded before opening their picker: ${{repairNoticeCandidateRequestCount}}`);
          }}
          await page.locator('.source-relation-item').filter({{ hasText: '设备检修关联' }}).getByRole('button', {{ name: '重新选择', exact: true }}).click();
          if (repairNoticeCandidateRequestCount !== 1) {{
            throw new Error(`repair candidates should load on demand once, got ${{repairNoticeCandidateRequestCount}}`);
          }}
          const repairPicker = page.getByRole('dialog', {{ name: '选择设备检修通告' }});
          await repairPicker.getByText('A楼测试设备检修', {{ exact: true }}).waitFor({{ state: 'visible' }});
          await repairPicker.getByRole('button', {{ name: '确认', exact: true }}).click();
          await waitForTextOrDump(page, 'A楼测试设备检修', 'repair-management-autofill');
          const projectColumnCount = await page.locator('.project-field-grid').first().evaluate((node) => (
            getComputedStyle(node).gridTemplateColumns.split(' ').filter(Boolean).length
          ));
          if (projectColumnCount !== 4) {{
            throw new Error(`repair project form should use four desktop columns, got ${{projectColumnCount}}`);
          }}
          const currentProgress = page.locator('[data-field-name="当前维修进度"]');
          await currentProgress.waitFor({{ state: 'visible' }});
          if (String(await currentProgress.locator('[data-progress-value]').textContent() || '').trim() !== '50') {{
            throw new Error('current repair progress did not render the latest followup percentage');
          }}
          if (await currentProgress.locator('input').count()) {{
            throw new Error('current repair progress must remain read-only');
          }}
          if (await currentProgress.getByRole('progressbar').getAttribute('aria-valuenow') !== '50') {{
            throw new Error('current repair progress accessibility value is incorrect');
          }}
          await page.getByRole('button', {{ name: /^跟进记录/ }}).first().click();
          await waitForTextOrDump(page, 'A楼测试维修跟进记录', 'repair-management-autofill');
          const followupPanel = page.locator('.followup-panel');
          if (await followupPanel.getByRole('button', {{ name: '新增跟进记录', exact: true }}).count() !== 1) {{
            throw new Error('followup create action should have exactly one entry');
          }}
          if (await followupPanel.getByText('超链接', {{ exact: true }}).count()) {{
            throw new Error('followup hyperlink field must stay hidden');
          }}
          await followupPanel.getByRole('button', {{ name: '新增跟进记录', exact: true }}).click();
          const followupColumnCount = await followupPanel.locator('.followup-field-grid').first().evaluate((node) => (
            getComputedStyle(node).gridTemplateColumns.split(' ').filter(Boolean).length
          ));
          if (followupColumnCount !== 4) {{
            throw new Error(`repair followup form should use four desktop columns, got ${{followupColumnCount}}`);
          }}
          const tallestCompactField = await followupPanel.locator('.repair-field-control.compact').evaluateAll((nodes) => (
            Math.max(0, ...nodes.map((node) => node.getBoundingClientRect().height))
          ));
          if (tallestCompactField > 60) {{
            throw new Error(`compact followup field is too tall: ${{Math.round(tallestCompactField)}}px`);
          }}
          await followupPanel.getByRole('button', {{ name: '选择设备', exact: true }}).click();
          const cmdbDialog = page.getByRole('dialog', {{ name: '选择 CMDB 设备', exact: true }});
          await cmdbDialog.waitFor({{ state: 'visible' }});
          await cmdbDialog.getByText('A-219-CRAH-01', {{ exact: true }}).waitFor({{ state: 'visible' }});
          const cmdbCheckboxes = cmdbDialog.locator('tbody input[type="checkbox"]');
          const cmdbCheckboxCount = await cmdbCheckboxes.count();
          if (cmdbCheckboxCount !== 2) {{
            throw new Error(`CMDB picker must expose every candidate as a multi-select checkbox, got ${{cmdbCheckboxCount}}`);
          }}
          const locationWhiteSpace = await cmdbDialog.locator('td.wrap-column span').first().evaluate(
            (node) => getComputedStyle(node).whiteSpace,
          );
          if (locationWhiteSpace !== 'normal') {{
            throw new Error(`CMDB location column must wrap instead of truncate, got ${{locationWhiteSpace}}`);
          }}
          await cmdbCheckboxes.nth(0).click();
          await cmdbCheckboxes.nth(1).click();
          await cmdbDialog.getByRole('button', {{ name: '确认', exact: true }}).click();
          const selectedCmdbText = String(await followupPanel.locator('.cmdb-line').textContent() || '');
          if (!selectedCmdbText.includes('A-219-CRAH-01') || !selectedCmdbText.includes('A-220-CRAH-02')) {{
            throw new Error(`CMDB multi-selection summary is incomplete: ${{selectedCmdbText}}`);
          }}
          await followupPanel.getByRole('button', {{ name: '重新选择', exact: true }}).click();
          const clearCmdbDialog = page.getByRole('dialog', {{ name: '选择 CMDB 设备', exact: true }});
          await clearCmdbDialog.locator('tbody tr').filter({{ hasText: 'A-219-CRAH-01' }}).click();
          await clearCmdbDialog.locator('tbody tr').filter({{ hasText: 'A-220-CRAH-02' }}).click();
          await clearCmdbDialog.getByRole('button', {{ name: '清空关联', exact: true }}).click();
          const clearedCmdbText = String(await followupPanel.locator('.cmdb-line').textContent() || '');
          if (!clearedCmdbText.includes('未选择')) {{
            throw new Error(`CMDB relation was not cleared: ${{clearedCmdbText}}`);
          }}
          const followupBrand = followupPanel.getByRole('combobox', {{ name: '设备品牌', exact: true }});
          const followupModel = followupPanel.getByRole('combobox', {{ name: '设备型号', exact: true }});
          await followupBrand.click();
          await page.getByRole('option', {{ name: '圣阳', exact: true }}).click();
          await followupModel.click();
          await page.getByRole('option', {{ name: 'SP12-100', exact: true }}).waitFor({{ state: 'visible' }});
          await page.getByRole('option', {{ name: 'SP12-200', exact: true }}).waitFor({{ state: 'visible' }});
          if (await page.getByRole('option', {{ name: 'C2500 D5A', exact: true }}).count()) {{
            throw new Error('followup model options were not filtered by selected brand');
          }}
          await page.getByRole('option', {{ name: 'SP12-100', exact: true }}).click();
          await followupBrand.click();
          await page.getByRole('option', {{ name: '康明斯', exact: true }}).click();
          if (await followupModel.inputValue() !== '') {{
            throw new Error('changing followup brand did not clear the stale model');
          }}
          await followupModel.click();
          await page.getByRole('option', {{ name: 'C2500 D5A', exact: true }}).click();
          await followupModel.fill('现场手填新型号');
          if (await followupModel.inputValue() !== '现场手填新型号') {{
            throw new Error('followup model combobox did not retain a custom model');
          }}
          const followupDescription = followupPanel.locator('[data-field-name="维修进展描述"] textarea');
          await followupDescription.fill('烟测新增跟进');
          const unsavedNotice = page.locator('.page-unsaved-notice');
          await unsavedNotice.waitFor({{ state: 'visible' }});
          if (!String(await unsavedNotice.textContent() || '').includes('未保存修改')) {{
            throw new Error('page-level unsaved notice is missing its status text');
          }}
          await page.getByRole('button', {{ name: '关闭维修项目', exact: true }}).click();
          await page.getByText('放弃未保存修改？', {{ exact: true }}).waitFor({{ state: 'visible' }});
          await page.getByRole('button', {{ name: '取消', exact: true }}).click();
          if (await followupDescription.inputValue() !== '烟测新增跟进') {{
            throw new Error('cancelling repair drawer close discarded unsaved followup draft');
          }}
          if (await followupPanel.getByRole('button', {{ name: '新增跟进记录', exact: true }}).count() !== 1) {{
            throw new Error('followup create mode must show one submit action');
          }}
          await followupPanel.getByRole('button', {{ name: '新增跟进记录', exact: true }}).click();
          await followupPanel.getByText('维修跟进记录已新增。', {{ exact: true }}).waitFor({{ state: 'visible' }});
          await page.getByRole('button', {{ name: /^跟进记录 2$/ }}).first().waitFor({{ state: 'visible' }});
          const followupOptions = followupPanel.locator('.followup-timeline-list [role="option"]');
          await followupOptions.first().waitFor({{ state: 'visible' }});
          await followupOptions.last().click();
          const progressInput = followupPanel.locator('[data-field-name="维修进度"] [data-progress-number]');
          await progressInput.fill('60');
          const progressControl = followupPanel.locator('[data-field-name="维修进度"] .repair-percentage-control');
          if (String(await progressControl.locator('.percentage-stage').textContent() || '').trim() !== '维修中') {{
            throw new Error('repair progress stage did not update to 维修中 at 60%');
          }}
          if (await progressControl.locator('.percentage-range').inputValue() !== '60') {{
            throw new Error('repair progress slider did not stay in sync with the number input');
          }}
          await followupPanel.getByRole('button', {{ name: '更新跟进记录', exact: true }}).click();
          await followupPanel.getByText('维修跟进记录已更新。', {{ exact: true }}).waitFor({{ state: 'visible' }});
          await assertLayout(page, 'repair-management-entry');
          await page.getByRole('button', {{ name: '维修单信息' }}).click();
          const linkedEventFaultTime = page.locator('[data-field-name="故障发生时间"] input[type="datetime-local"]');
          if (await linkedEventFaultTime.inputValue() !== '2026-06-25T10:00') {{
            throw new Error(`linked event occurrence time was not applied: ${{await linkedEventFaultTime.inputValue()}}`);
          }}
          if (!(await linkedEventFaultTime.isDisabled())) {{
            throw new Error('linked event occurrence time must be source-controlled');
          }}
          await page.locator('[data-field-name="维修名称"] input').fill('A楼测试检修管理记录-烟测修改');
          await page.getByRole('button', {{ name: '检修通告', exact: true }}).click();
          await page.getByText('保存后填写检修通告？', {{ exact: true }}).waitFor({{ state: 'visible' }});
          await page.getByRole('button', {{ name: '保存并继续', exact: true }}).click();
          await waitForTextOrDump(page, '维修单生成检修通告', 'repair-notice-prefill');
          if (!page.url().includes('repair_management_record_id=repair-mgmt-a-001')) {{
            throw new Error(`repair management prefill id missing: ${{page.url()}}`);
          }}
          if (!page.url().includes('record_id=repair-mgmt-a-001')) {{
            throw new Error(`repair source selection id missing: ${{page.url()}}`);
          }}
          const repairSourceId = await page.locator('input[name="source_record_id"]').getAttribute('value');
          const repairTargetId = await page.locator('input[name="target_record_id"]').getAttribute('value');
          const repairTitleValue = await page.locator('input[name="title"]').getAttribute('value');
          if (repairSourceId !== 'repair-mgmt-a-001') throw new Error(`repair source id mismatch: ${{repairSourceId}}`);
          if (repairTargetId !== 'rec-smoke-repair-a-001') throw new Error(`repair target id mismatch: ${{repairTargetId}}`);
          if (repairTitleValue !== 'A楼测试检修管理记录') throw new Error(`repair title prefill mismatch: ${{repairTitleValue}}`);
          await page.goto(cfg.url, {{
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          }});
          await page.waitForSelector('text=业务工作台', {{ timeout: 10000 }});
          await assertHeaderSubtitle(page, '功能选择 · 请选择功能', 'home-after-repair-management');
          await page.getByRole('button', {{ name: '检修通告管理', exact: true }}).click();
          await page.waitForSelector('text=选择楼栋进入检修通告管理', {{ timeout: 10000 }});
          const repairNoticeScopeCard = page.locator('article.scope-card').filter({{ hasText: 'A楼' }}).first();
          await repairNoticeScopeCard.getByRole('button', {{ name: '进入检修通告管理' }}).click();
          await waitForTextOrDump(page, '待发起事项', 'repair-notice-entry');
          if (!page.url().includes('work_type=repair')) {{
            throw new Error(`repair notice entry work_type mismatch: ${{page.url()}}`);
          }}
          await page.goto(cfg.url, {{ waitUntil: 'domcontentloaded', timeout: 20000 }});
          await page.waitForSelector('text=业务工作台', {{ timeout: 10000 }});
          await page.getByRole('button', {{ name: '进入维护管理', exact: true }}).click();
          await page.waitForSelector('text=选择楼栋进入维护管理', {{ timeout: 10000 }});
          const scopeCard = page.locator('article.scope-card').filter({{ hasText: 'A楼' }}).first();
          await scopeCard.getByRole('button', {{ name: '进入维护管理' }}).click();
          await waitForTextOrDump(page, '待发起事项', 'maintenance-entry');
          await waitForTextOrDump(page, '冷机月度巡检', 'maintenance-entry');
          await waitForTextOrDump(page, '已开始未结束', 'maintenance-entry');
          await waitForTextOrDump(page, 'A楼冷站过滤器维护000', 'maintenance-entry');
          const isLiteWorkbench = page.url().includes('/workbench-lite');
          if (isLiteWorkbench) {{
            const litePanels = await page.locator('.workspace > .panel').count();
            if (litePanels < 1) throw new Error(`lite workbench panel count ${{litePanels}}`);
            const liteDrawerCount = await page.locator('#lite-notice-detail-overlay > #detail-panel').count();
            if (liteDrawerCount !== 1) throw new Error(`lite notice drawer count ${{liteDrawerCount}}`);
            const liteDrawerInitiallyOpen = await page.locator('#lite-notice-detail-overlay').evaluate(node => node.classList.contains('open'));
            if (liteDrawerInitiallyOpen) throw new Error('lite notice drawer opened before selecting a notice');
            const liteRails = await page.locator('.rail-fold .rail-panel, .result-rail .rail-panel').count();
            if (liteRails < 1) throw new Error(`lite workbench folded rail count ${{liteRails}}`);
            const typeCounts = await page.locator('.type-tab .type-count').count();
            if (typeCounts < 6) throw new Error(`lite type counts did not render: ${{typeCounts}}`);
            const panelCounts = await page.locator('.panel-title .panel-count, .inbox-section h3 b, .inbox-summary b').count();
            if (panelCounts < 2) throw new Error(`lite panel counts did not render: ${{panelCounts}}`);
            const liteOngoingRows = await page.locator('.ongoing-row').count();
            if (liteOngoingRows < 1) throw new Error(`lite ongoing rows did not render: ${{liteOngoingRows}}`);
            const liteA11yProbe = await page.evaluate(() => ({{
              manualControls: document.querySelector('#manual-open')?.getAttribute('aria-controls') || '',
              manualExpanded: document.querySelector('#manual-open')?.getAttribute('aria-expanded') || '',
              refreshControls: document.querySelector('#refresh-open')?.getAttribute('aria-controls') || '',
              searchLabel: document.querySelector('label[for="lite-search-input"]')?.textContent || '',
              specialtyLabel: document.querySelector('#lite-specialty-select')?.getAttribute('aria-label') || '',
              jobStatusLive: document.querySelector('#lite-job-status')?.getAttribute('aria-live') || '',
            }}));
            if (
              liteA11yProbe.manualControls !== 'manual-menu'
              || liteA11yProbe.manualExpanded !== 'false'
              || liteA11yProbe.refreshControls !== 'refresh-menu'
              || !liteA11yProbe.searchLabel.includes('搜索')
              || !liteA11yProbe.specialtyLabel.includes('专业')
              || liteA11yProbe.jobStatusLive !== 'polite'
            ) {{
              throw new Error(`lite workbench accessibility markers missing: ${{JSON.stringify(liteA11yProbe)}}`);
            }}
            const liteSubmitScriptProbe = await page.evaluate(() => {{
              const scriptText = Array.from(document.scripts).map(node => node.textContent || '').join('\\n');
              return {{
                hasSchedule: scriptText.includes('function schedulePostSubmitRefresh'),
                hasJobPolling: scriptText.includes('pollSubmittedJob(jobId'),
                hasDirtyGuard: scriptText.includes('if (liteFormDirty)'),
                hasStatusSummaryRefresh: scriptText.includes("refreshCurrentLite('后台状态校准中...', ['.status', '.summary'])"),
                hasCurrentLiteRefresh: scriptText.includes('function refreshCurrentLite'),
              }};
            }});
            if (
              !liteSubmitScriptProbe.hasSchedule
              || !liteSubmitScriptProbe.hasJobPolling
              || !liteSubmitScriptProbe.hasDirtyGuard
              || !liteSubmitScriptProbe.hasStatusSummaryRefresh
              || !liteSubmitScriptProbe.hasCurrentLiteRefresh
            ) {{
              throw new Error(`lite submit workspace refresh guards missing: ${{JSON.stringify(liteSubmitScriptProbe)}}`);
            }}
            await page.evaluate(() => {{
              window.__clipflowLiteNoReloadMarker = 'alive';
              const topbar = document.querySelector('.topbar');
              if (topbar) topbar.setAttribute('data-smoke-stable', 'stable');
              document.querySelectorAll('.workspace .list').forEach(node => {{ node.scrollTop = 120; }});
            }});
            await page.locator('.ongoing-row').filter({{ hasText: 'A楼纯手填待关联维保通告' }}).first().click();
            await page.waitForSelector('text=目标多维关系', {{ state: 'attached', timeout: 10000 }});
            await page.waitForSelector('#lite-notice-detail-overlay.open', {{ timeout: 10000 }});
            const markerAfterOngoingClick = await page.evaluate(() => window.__clipflowLiteNoReloadMarker || '');
            if (markerAfterOngoingClick !== 'alive') {{
              throw new Error('lite ongoing click caused full page reload');
            }}
            const topbarStillStable = await page.evaluate(() => document.querySelector('.topbar')?.getAttribute('data-smoke-stable') || '');
            if (topbarStillStable !== 'stable') {{
              throw new Error('lite ongoing click rebuilt topbar instead of only updating work area');
            }}
            const restoredScroll = await page.evaluate(() => Array.from(document.querySelectorAll('.workspace .list')).map(node => node.scrollTop || 0));
            if (!restoredScroll.some(value => value >= 80)) {{
              throw new Error(`lite row click did not preserve list scroll: ${{JSON.stringify(restoredScroll)}}`);
            }}
            const sourceLinkProbe = await page.evaluate(() => {{
              const field = document.querySelector('.source-link-field');
              const select = document.querySelector('select[name="source_record_id"]');
              const hidden = document.querySelector('input[type="hidden"][name="source_record_id"]');
              return {{
                hasField: !!field,
                text: field?.textContent || '',
                hasSelect: !!select,
                hiddenValue: hidden?.value || '',
                options: select ? Array.from(select.options).map(node => node.textContent || '') : [],
              }};
            }});
            if (
              !sourceLinkProbe.hasField
               || (!sourceLinkProbe.hiddenValue
               && !sourceLinkProbe.options.some(text => text.includes('冷机月度巡检'))
              && !sourceLinkProbe.text.includes('源表未关联')
              && !sourceLinkProbe.text.includes('未关联'))
            ) {{
              throw new Error(`source link options missing expected record: ${{JSON.stringify(sourceLinkProbe)}}`);
            }}
            const sourceSelectDirty = await page.evaluate(() => {{
              const select = document.querySelector('select[name="source_record_id"]');
              if (!select) return true;
              const option = Array.from(select.options).find(node => node.value);
              if (option) select.value = option.value;
              select.dispatchEvent(new Event('change', {{ bubbles: true }}));
              return document.body.classList.contains('has-dirty-lite-form')
                && (document.querySelector('#lite-job-status')?.textContent || '').includes('未发送修改');
            }});
            if (!sourceSelectDirty) {{
              throw new Error('lite source link select did not mark form dirty');
            }}
            await page.locator('#lite-notice-form textarea[name="progress"]').fill('未发送修改测试');
            await page.waitForSelector('text=有未发送修改', {{ timeout: 10000 }});
            await page.locator('#lite-notice-drawer-close').click();
            await page.waitForSelector('#lite-discard-confirm:not([hidden])', {{ timeout: 10000 }});
            const sawDirtyConfirm = await page.locator('#lite-discard-confirm').innerText();
            if (!sawDirtyConfirm.includes('未发送修改')) throw new Error('lite dirty form warning did not appear before closing the drawer');
            await page.locator('#lite-discard-cancel').click();
            await page.waitForFunction(() => document.querySelector('#lite-discard-confirm')?.hidden === true, null, {{ timeout: 10000 }});
            await page.waitForFunction(() => document.activeElement?.id === 'lite-notice-drawer-close', null, {{ timeout: 10000 }});
            const dirtyCancelState = await page.evaluate(() => ({{
              drawerOpen: document.querySelector('#lite-notice-detail-overlay')?.classList.contains('open') || false,
              focusedId: document.activeElement?.id || '',
            }}));
            if (!dirtyCancelState.drawerOpen || dirtyCancelState.focusedId !== 'lite-notice-drawer-close') {{
              throw new Error(`lite dirty cancel did not restore drawer focus: ${{JSON.stringify(dirtyCancelState)}}`);
            }}
            await page.locator('#lite-notice-drawer-close').click();
            await page.waitForSelector('#lite-discard-confirm:not([hidden])', {{ timeout: 10000 }});
            await page.locator('#lite-discard-confirm-button').click();
            await page.waitForFunction(() => !document.querySelector('#lite-notice-detail-overlay')?.classList.contains('open'), null, {{ timeout: 10000 }});
            await page.waitForFunction(() => document.activeElement?.classList.contains('ongoing-row') || false, null, {{ timeout: 10000 }});
            const triggerFocusedAfterClose = await page.evaluate(() => document.activeElement?.classList.contains('ongoing-row') || false);
            if (!triggerFocusedAfterClose) throw new Error('lite notice drawer did not restore focus to its triggering row');
            await page.locator('.notice-row').first().click();
            await page.waitForSelector('#lite-notice-detail-overlay.open', {{ timeout: 10000 }});
            await page.evaluate(() => {{
              const body = document.querySelector('.notice-drawer-body');
              if (body) body.scrollTop = Math.min(420, body.scrollHeight);
            }});
            await assertLayout(page, 'lite-workbench');
            await assertVnetSkin(page, 'lite-workbench');
            await page.locator('#lite-notice-drawer-close').click();
            await page.waitForFunction(() => !document.querySelector('#lite-notice-detail-overlay')?.classList.contains('open'), null, {{ timeout: 10000 }});
            await page.locator('.notice-row').first().click();
            await page.waitForSelector('#lite-notice-detail-overlay.open', {{ timeout: 10000 }});
            await page.waitForFunction(() => (document.querySelector('.notice-drawer-body')?.scrollTop || 0) === 0, null, {{ timeout: 10000 }});
            await page.locator('#lite-notice-drawer-close').click();
            await page.waitForFunction(() => !document.querySelector('#lite-notice-detail-overlay')?.classList.contains('open'), null, {{ timeout: 10000 }});
            await page.getByRole('button', {{ name: '刷新数据' }}).click();
            await page.waitForSelector('text=刷新本页', {{ timeout: 10000 }});
            await page.waitForSelector('text=刷新检修', {{ timeout: 10000 }});
            await page.waitForSelector('text=刷新变更', {{ timeout: 10000 }});
            const oldRefreshCopyVisible = await page.locator('text=只重新读取当前楼栋和类型').count();
            if (oldRefreshCopyVisible) throw new Error('lite refresh menu still shows verbose helper copy');
            await page.keyboard.press('Escape');
            const refreshClosedByEscape = await page.evaluate(() => ({{
              open: document.querySelector('#refresh-picker')?.classList.contains('open') || false,
              expanded: document.querySelector('#refresh-open')?.getAttribute('aria-expanded') || '',
            }}));
            if (refreshClosedByEscape.open || refreshClosedByEscape.expanded !== 'false') {{
              throw new Error(`lite refresh menu did not close on Escape: ${{JSON.stringify(refreshClosedByEscape)}}`);
            }}
            await page.getByRole('button', {{ name: '刷新数据' }}).click();
            await page.locator('#lite-refresh-page').click();
            await page.waitForFunction(() => document.querySelector('#lite-refresh-page')?.disabled === false, null, {{ timeout: 10000 }});
            let pageRefreshEnabled = false;
            for (let attempt = 0; attempt < 4; attempt += 1) {{
              try {{
                if (!page.url().includes('/workbench-lite')) {{
                  throw new Error(`lite page refresh navigated away: ${{page.url()}}`);
                }}
                pageRefreshEnabled = await page.evaluate(() => document.querySelector('#lite-refresh-page')?.disabled === false);
                break;
              }} catch (refreshProbeError) {{
                if (!String(refreshProbeError).includes('Execution context was destroyed')) throw refreshProbeError;
                await page.waitForLoadState('domcontentloaded', {{ timeout: 5000 }}).catch(() => null);
                await page.waitForTimeout(300);
              }}
            }}
            if (!pageRefreshEnabled) {{
              throw new Error('lite page refresh button stayed disabled after successful refresh');
            }}
            await page.locator('.notice-row').first().click();
            await page.waitForSelector('#lite-notice-detail-overlay.open', {{ timeout: 10000 }});
            await page.locator('#lite-paste-toggle').click();
            await page.locator('form[action="/workbench-lite/parse"] textarea[name="paste_text"]').fill(`【设备轮巡】状态：开始
【标题】EA118机房A楼制冷单元轮巡通告
【时间】2026-06-24 09:30~2026-06-24 18:30
【设备】A-127冷冻站制冷单元
【内容】测试测试
【影响】测试测试
【进度】测试测试`);
            const parseSubmitButton = page.getByRole('button', {{ name: '解析到当前通告' }});
            await parseSubmitButton.scrollIntoViewIfNeeded();
            const parseSubmitGeometry = await parseSubmitButton.evaluate(button => {{
              const summarize = node => {{
                if (!(node instanceof Element)) return null;
                const rect = node.getBoundingClientRect();
                const style = getComputedStyle(node);
                return {{
                  tag: node.tagName.toLowerCase(),
                  id: node.id || '',
                  className: String(node.className || ''),
                  rect: {{
                    left: Math.round(rect.left),
                    top: Math.round(rect.top),
                    right: Math.round(rect.right),
                    bottom: Math.round(rect.bottom),
                    width: Math.round(rect.width),
                    height: Math.round(rect.height),
                  }},
                  display: style.display,
                  position: style.position,
                  overflow: style.overflow,
                  zIndex: style.zIndex,
                }};
              }};
              const rect = button.getBoundingClientRect();
              const x = rect.left + rect.width / 2;
              const y = rect.top + rect.height / 2;
              const hitNodes = document.elementsFromPoint(x, y).slice(0, 8);
              return {{
                button: summarize(button),
                parseForm: summarize(button.closest('form')),
                parseDrawer: summarize(button.closest('.paste-drawer')),
                drawerHead: summarize(document.querySelector('.notice-drawer-head')),
                drawerBody: summarize(document.querySelector('.notice-drawer-body')),
                detailForm: summarize(document.querySelector('#lite-notice-form')),
                bodyScrollTop: document.querySelector('.notice-drawer-body')?.scrollTop || 0,
                hitNodes: hitNodes.map(summarize),
                buttonHit: hitNodes.includes(button),
              }};
            }});
            if (!parseSubmitGeometry.buttonHit) {{
              throw new Error(`lite parse submit is visually covered: ${{JSON.stringify(parseSubmitGeometry)}}`);
            }}
            await parseSubmitButton.click();
            await page.waitForSelector('text=EA118机房A楼制冷单元轮巡通告', {{ timeout: 10000 }});
            const markerAfterParse = await page.evaluate(() => window.__clipflowLiteNoReloadMarker || '');
            if (markerAfterParse !== 'alive') {{
              throw new Error('lite paste parse caused full page reload');
            }}
            await page.waitForSelector('text=设备轮巡', {{ timeout: 10000 }});
            await page.waitForSelector('text=当前通告', {{ timeout: 10000 }});
            const parsedSpecialtyInput = page.locator('#lite-notice-form input[name="specialty"]');
            if (await parsedSpecialtyInput.count() === 1) {{
              await parsedSpecialtyInput.fill('暖通');
            }}
            const datalistProbe = await page.evaluate(() => ({{
              specialties: Array.from(document.querySelectorAll('#specialty-options option')).map(node => node.getAttribute('value') || ''),
              cycles: Array.from(document.querySelectorAll('#maintenance-cycle-options option')).map(node => node.getAttribute('value') || ''),
            }}));
            if (!datalistProbe.specialties.includes('电气') || !datalistProbe.cycles.includes('/')) {{
              throw new Error(`lite datalist options missing: ${{JSON.stringify(datalistProbe)}}`);
            }}
            const smokeSendButton = page.getByRole('button', {{ name: /发送.*开始/ }});
            await smokeSendButton.waitFor({{ timeout: 10000 }});
            await page.evaluate(() => {{
              const topbar = document.querySelector('.topbar');
              if (topbar) topbar.setAttribute('data-smoke-submit-stable', 'stable');
            }});
            const submitStart = Date.now();
            await smokeSendButton.click({{ timeout: 5000 }});
            const submitClickMs = Date.now() - submitStart;
            if (submitClickMs > 1200) {{
              throw new Error(`lite workbench action click blocked main thread for ${{submitClickMs}}ms`);
            }}
            await page.waitForSelector('text=已受理', {{ timeout: 10000 }});
            const probeStart = Date.now();
            const probe = await page.evaluate(() => ({{
              hasWorkspace: document.body.innerText.includes('待发起事项') && document.body.innerText.includes('已开始未结束'),
              statusText: document.querySelector('#lite-job-status')?.textContent || '',
            }}));
            const probeMs = Date.now() - probeStart;
            if (probeMs > 500 || !probe.hasWorkspace || !probe.statusText.includes('已受理')) {{
              throw new Error(`lite workbench became sluggish after submit: probeMs=${{probeMs}} probe=${{JSON.stringify(probe)}}`);
            }}
            await page.waitForTimeout(1800);
            const topbarAfterSubmitRefresh = await page.evaluate(() => document.querySelector('.topbar')?.getAttribute('data-smoke-submit-stable') || '');
            if (topbarAfterSubmitRefresh !== 'stable') {{
              throw new Error('lite submit refresh rebuilt topbar instead of only updating status/summary/workspace');
            }}
            await page.setViewportSize({{ width: 900, height: 900 }});
            await assertLayout(page, 'lite-workbench-narrow');
            await page.setViewportSize({{ width: 1440, height: 900 }});
            const noticeDrawerOpenBeforeReturn = await page.locator('#lite-notice-detail-overlay').evaluate(node => node.classList.contains('open'));
            if (noticeDrawerOpenBeforeReturn) {{
              await page.locator('#lite-notice-drawer-close').click();
              await page.waitForFunction(() => !document.querySelector('#lite-notice-detail-overlay')?.classList.contains('open'), null, {{ timeout: 10000 }});
            }}
            await page.getByRole('link', {{ name: /^返回$/ }}).click();
            await page.waitForSelector('text=业务工作台', {{ timeout: 10000 }});
            await assertHeaderSubtitle(page, '功能选择 · 请选择功能', 'lite-home-after-return');
            if (errors.length || failedResponses.length) {{
              throw new Error(`browser runtime errors: ${{errors.join(' | ')}} failedResponses=${{failedResponses.join(' | ')}}`);
            }}
            const pageTitle = await page.title().catch(() => '');
            await browser.close();
            console.log(JSON.stringify({{
              ok: true,
              title: pageTitle,
              mode: 'workbench-lite',
              markers: ['飞书扫码登录', ...required, 'A楼轻量工作台', '待发起事项', '已开始未结束', 'VNET蓝白皮肤', 'worker提交不卡顿'],
            }}));
            return;
          }}
          const initialOngoingCards = await page.locator('article.ongoing-card').count();
          if (initialOngoingCards < 1) {{
            throw new Error(`ongoing cards did not render: ${{initialOngoingCards}}`);
          }}
          await page.getByRole('button', {{ name: '刷新数据' }}).click();
          await page.waitForSelector('text=刷新检修', {{ timeout: 10000 }});
          await page.waitForSelector('text=刷新变更', {{ timeout: 10000 }});
          await page.keyboard.press('Escape');
          await page.waitForSelector('text=纯手填', {{ timeout: 10000 }});
          await page.waitForSelector('text=解析粘贴通告', {{ timeout: 10000 }});
          await page.waitForSelector('text=近三天可回退', {{ timeout: 10000 }});
          await page.locator('.recent-undo-panel').getByRole('button', {{ name: /展开/ }}).click();
          await page.waitForSelector('text=回退删除', {{ timeout: 10000 }});
          await page.getByRole('button', {{ name: '纯手填' }}).click();
          await page.waitForSelector('text=选择纯手填通告类型', {{ timeout: 10000 }});
          const manualAdjustButton = page.locator('.manual-type-grid button').filter({{ hasText: '调整' }});
          if (await manualAdjustButton.count() !== 1) throw new Error('manual adjust type button missing or ambiguous');
          await manualAdjustButton.click();
          await page.waitForSelector('text=待发起通告', {{ timeout: 10000 }});
          await page.locator('.drafts-panel').getByRole('button', {{ name: '编辑' }}).click();
          const draftPanelText = await page.locator('.drafts-panel').innerText({{ timeout: 10000 }});
          if (!draftPanelText.includes('通告类型') || !draftPanelText.includes('调整')) {{
            throw new Error(`manual adjust draft not visible: ${{draftPanelText}}`);
          }}
          await page.getByRole('button', {{ name: '解析粘贴' }}).click();
          await page.waitForSelector('text=解析到待发起通告', {{ timeout: 10000 }});
          const pastePanel = page.locator('.paste-panel');
          const pasteTextarea = pastePanel.locator('textarea[placeholder*="粘贴完整"]');
          await pasteTextarea.waitFor({{ timeout: 10000 }});
          await pasteTextarea.fill(`【设备调整】状态：开始
【名称】EA118机房A楼空调调整通告
【时间】2026-06-12 09:30~2026-06-12 18:30
【位置】A-101空调间
【内容】测试测试测试
【原因】测试测试测试
【影响】测试测试测试
【进度】测试测试测试`);
          await pastePanel.getByRole('button', {{ name: '解析到待发起通告' }}).click();
          await page.waitForSelector('text=EA118机房A楼空调调整通告', {{ timeout: 10000 }});
          const parsedDraftPanelText = await page.locator('.drafts-panel').innerText({{ timeout: 10000 }});
          if (!parsedDraftPanelText.includes('EA118机房A楼空调调整通告') || !parsedDraftPanelText.includes('调整')) {{
            throw new Error(`parsed adjust draft not visible: ${{parsedDraftPanelText}}`);
          }}
          const maintenanceSegment = page.locator('.segmented button').filter({{ hasText: '维保' }});
          if (await maintenanceSegment.count() !== 1) throw new Error('maintenance segment missing or ambiguous');
          await maintenanceSegment.click();
          await page.waitForSelector('text=冷机月度巡检', {{ timeout: 10000 }});
          await assertHeaderSubtitle(page, 'A楼 · 通告工作台', 'workbench');
          await assertLayout(page, 'workbench');
          await assertVnetSkin(page, 'workbench');
          await page.getByRole('button', {{ name: /^返回$/ }}).click();
          await page.waitForSelector('text=业务工作台', {{ timeout: 10000 }});
          await assertHeaderSubtitle(page, '功能选择 · 请选择功能', 'home-after-return-from-workbench');
          await assertVnetSkin(page, 'home-after-return-from-workbench');
          await page.getByRole('button', {{ name: '进入维护管理', exact: true }}).click();
          await page.waitForSelector('text=选择楼栋进入维护管理', {{ timeout: 10000 }});
          const returnScopeCard = page.locator('article.scope-card').filter({{ hasText: 'A楼' }}).first();
          await returnScopeCard.getByRole('button', {{ name: '进入维护管理' }}).click();
          await page.waitForSelector('text=待发起事项', {{ timeout: 10000 }});
          await page.waitForSelector('text=A楼冷站过滤器维护000', {{ timeout: 10000 }});
          await assertHeaderSubtitle(page, 'A楼 · 通告工作台', 'workbench-after-return');
          const secondPage = await context.newPage();
          attachDiagnostics(secondPage, 'auth-second-tab');
          await secondPage.goto(new URL('/?scope=A', cfg.url).toString(), {{
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          }});
          await secondPage.waitForSelector('text=待发起事项', {{ timeout: 10000 }});
          const allSegment = secondPage.locator('.segmented button').filter({{ hasText: '全部' }});
          if (await allSegment.count() !== 1) throw new Error('all work type segment missing or ambiguous');
          const allSegmentClass = await allSegment.first().getAttribute('class');
          if (!String(allSegmentClass || '').includes('active')) {{
            throw new Error(`all work type segment is not active by default: ${{allSegmentClass}}`);
          }}
          await secondPage.waitForSelector('text=冷机月度巡检', {{ timeout: 10000 }});
          await secondPage.waitForSelector('text=A楼UPS旁路切换变更测试', {{ timeout: 10000 }});
          await assertLayout(secondPage, 'second-tab-workbench');
          await page.waitForTimeout(1000);
          const streamUrls = [...await eventSourceUrls(page), ...await eventSourceUrls(secondPage)];
          const jobStreamCount = streamUrls.filter(url => url.includes('/api/jobs/stream')).length;
          const activeItemsStreamCount = streamUrls.filter(url => url.includes('/api/qt-active-items/stream')).length;
          if (jobStreamCount > 1 || activeItemsStreamCount > 1) {{
            throw new Error(`cross-tab stream coordination failed: jobs=${{jobStreamCount}} active=${{activeItemsStreamCount}} urls=${{streamUrls.join(',')}}`);
          }}
          const secondTabText = await secondPage.locator('body').innerText({{ timeout: 10000 }});
          if (secondTabText.includes('实时同步正在重连')) {{
            throw new Error('second tab should not show realtime reconnect warning while shared stream is active');
          }}
          await secondPage.close();
          await page.locator('article.notice-row').filter({{ hasText: '冷机月度巡检' }}).first().click();
          await page.waitForSelector('text=待发起通告', {{ timeout: 10000 }});
          const smokeSendButton = page.getByRole('button', {{ name: /发送.*开始/ }});
          await smokeSendButton.waitFor({{ timeout: 10000 }});
          const submitStart = Date.now();
          await smokeSendButton.click({{ timeout: 5000 }});
          const submitClickMs = Date.now() - submitStart;
          if (submitClickMs > 1800) {{
            throw new Error(`workbench action click blocked main thread for ${{submitClickMs}}ms`);
          }}
          await page.waitForTimeout(700);
          const probeStart = Date.now();
          const probe = await page.evaluate(() => {{
            const buttons = Array.from(document.querySelectorAll('button')).map((node) => node.textContent || '');
            return {{
              status: document.querySelector('[role="status"]')?.textContent || '',
              buttonCount: buttons.length,
              hasWorkbench: document.body.innerText.includes('待发起通告') && document.body.innerText.includes('已开始未结束'),
            }};
          }});
          const probeMs = Date.now() - probeStart;
          if (probeMs > 800 || !probe.hasWorkbench) {{
            throw new Error(`page became sluggish after submit: probeMs=${{probeMs}} probe=${{JSON.stringify(probe)}}`);
          }}
          await page.locator('article.ongoing-card').filter({{ hasText: 'A楼冷站过滤器维护000' }}).first().click();
          await page.waitForSelector('text=更新', {{ timeout: 10000 }});
          await page.waitForSelector('text=结束', {{ timeout: 10000 }});
          await page.getByRole('button', {{ name: '管理/诊断' }}).click();
          await page.waitForSelector('text=管理员工具', {{ timeout: 10000 }});
          await page.waitForSelector('text=查看详细诊断数据', {{ timeout: 10000 }});
          const rawDiagnosticOpen = await page.evaluate(() => {{
            const node = document.querySelector('.raw-diagnostic');
            return Boolean(node && node.hasAttribute('open'));
          }});
          if (rawDiagnosticOpen) throw new Error('admin raw diagnostic should be collapsed by default');
          await page.getByRole('button', {{ name: '权限' }}).click();
          await page.waitForSelector('text=保存权限', {{ timeout: 10000 }});
          await page.waitForSelector('text=添加用户', {{ timeout: 10000 }});
          let permissionSkin = await page.evaluate(() => {{
            const rows = Array.from(document.querySelectorAll('.permission-row'));
            const scopePills = Array.from(document.querySelectorAll('.permission-row .scope-checks label'));
            const activePills = scopePills.filter((node) => {{
              const input = node.querySelector('input[type="checkbox"]');
              return Boolean(input && input.checked);
            }});
            const firstRowStyle = rows[0] ? getComputedStyle(rows[0]) : null;
            return {{
              rowCount: rows.length,
              scopePillCount: scopePills.length,
              activePillCount: activePills.length,
              rowRadius: firstRowStyle?.borderRadius || '',
              rowBackground: firstRowStyle?.backgroundImage || firstRowStyle?.backgroundColor || '',
            }};
          }});
          if (permissionSkin.rowCount < 1) {{
            await page.getByRole('button', {{ name: '添加用户' }}).click();
            await page.waitForSelector('.permission-row', {{ timeout: 10000 }});
            permissionSkin = await page.evaluate(() => {{
              const rows = Array.from(document.querySelectorAll('.permission-row'));
              const scopePills = Array.from(document.querySelectorAll('.permission-row .scope-checks label'));
              const activePills = scopePills.filter((node) => {{
                const input = node.querySelector('input[type="checkbox"]');
                return Boolean(input && input.checked);
              }});
              const firstRowStyle = rows[0] ? getComputedStyle(rows[0]) : null;
              return {{
                rowCount: rows.length,
                scopePillCount: scopePills.length,
                activePillCount: activePills.length,
                rowRadius: firstRowStyle?.borderRadius || '',
                rowBackground: firstRowStyle?.backgroundImage || firstRowStyle?.backgroundColor || '',
              }};
            }});
          }}
          if (permissionSkin.rowCount < 1 || permissionSkin.scopePillCount < 1) {{
            throw new Error(`admin permission rows missing: ${{JSON.stringify(permissionSkin)}}`);
          }}
          const permissionRowRadius = Number.parseFloat(permissionSkin.rowRadius || '0');
          if (!Number.isFinite(permissionRowRadius) || permissionRowRadius < 12 || permissionRowRadius > 28 || !permissionSkin.rowBackground.includes('gradient')) {{
            throw new Error(`admin permission VNET skin missing: ${{JSON.stringify(permissionSkin)}}`);
          }}
          await page.getByRole('button', {{ name: '状态' }}).click();
          await page.waitForSelector('text=查看详细诊断数据', {{ timeout: 10000 }});
          await assertLayout(page, 'admin');
          await assertVnetSkin(page, 'admin');
          await page.goto(new URL('/admin/history-memory', cfg.url).toString(), {{
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          }});
          await page.waitForSelector('text=历史通告记忆导入', {{ timeout: 10000 }});
          await page.waitForSelector('text=扫描历史通告', {{ timeout: 10000 }});
          await page.waitForSelector('text=当前月事项', {{ timeout: 10000 }});
          await assertHeaderSubtitle(page, '管理工具 · 历史通告记忆导入', 'history-memory');
          await assertLayout(page, 'history-memory');
          await assertVnetSkin(page, 'history-memory');
          if (errors.length || failedResponses.length) {{
            throw new Error(`browser runtime errors: ${{errors.join(' | ')}} failedResponses=${{failedResponses.join(' | ')}}`);
          }}
          const pageTitle = await page.title().catch(() => '');
          await browser.close();
          console.log(JSON.stringify({{
            ok: true,
            title: pageTitle,
            markers: ['飞书扫码登录', ...required, 'A楼工作台', '待发起事项', '已开始未结束', '多标签SSE降噪', 'VNET蓝白皮肤', '管理员工具', '历史通告记忆导入', '大列表不卡顿', 'worker提交不卡顿'],
          }}));
        }})().catch(async err => {{
          console.error(String(err && err.stack || err));
          process.exit(1);
        }});
        """
    ).strip()


def run_smoke(*, port: int = 18976, keep_server_seconds: float = 0.0) -> dict:
    node = shutil.which("node")
    npx = shutil.which("npx")
    frontend_dir = BIN_DIR / "lan_bitable_template_portal" / "frontend"
    frontend_node_modules = frontend_dir / "node_modules"
    local_playwright = frontend_node_modules / "playwright"
    if not node and not npx:
        raise RuntimeError("node/npx 不可用，无法运行 Playwright 浏览器 smoke。")

    previous_mock = os.environ.get("CLIPFLOW_BACKEND_MOCK_EXTERNAL")
    os.environ["CLIPFLOW_BACKEND_MOCK_EXTERNAL"] = "1"

    original_service = PortalRuntime.service
    original_auth = PortalRuntime.auth_manager
    original_store = PortalRuntime.state_store
    original_callbacks = (
        PortalRuntime.notice_callback,
        PortalRuntime.ongoing_callback,
        PortalRuntime.ongoing_delete_callback,
        PortalRuntime.maintenance_action_callback,
    )
    temp_dir = tempfile.TemporaryDirectory()
    server = None
    server_thread: threading.Thread | None = None
    try:
        import uvicorn

        state_store = LanPortalStateStore(Path(temp_dir.name) / "state.sqlite3")
        auth_manager = PortalAuthManager()
        auth_manager._state_store = state_store
        auth_manager.upsert_permission_user(
            open_id="ou_frontend_smoke",
            name="frontend-smoke-admin",
            role="admin",
            scopes=["ALL"],
            enabled=True,
            updated_by="frontend-runtime-smoke",
        )
        session_id = "frontend-runtime-smoke-session"
        no_scope_session_id = f"{session_id}-no-scope"
        with auth_manager._lock:
            auth_manager._sessions[session_id] = {
                "session_id": session_id,
                "user": {
                    "name": "frontend-smoke-admin",
                    "open_id": "ou_frontend_smoke",
                },
                "role": "admin",
                "allowed_scopes": ["ALL"],
                "scope_options": SCOPE_OPTIONS,
                "expires_at": time.time() + 3600,
            }
            auth_manager._sessions[no_scope_session_id] = {
                "session_id": no_scope_session_id,
                "user": {
                    "name": "frontend-smoke-no-scope",
                    "open_id": "ou_frontend_smoke_no_scope",
                },
                "role": "building",
                "allowed_scopes": [],
                "scope_options": [],
                "expires_at": time.time() + 3600,
            }

        PortalRuntime.service = _SmokePortalService()
        PortalRuntime.state_store = state_store
        PortalRuntime.auth_manager = auth_manager
        PortalRuntime.notice_callback = None
        PortalRuntime.ongoing_callback = None
        PortalRuntime.ongoing_delete_callback = None
        PortalRuntime.maintenance_action_callback = None

        controller = FastAPIPortalController(host="127.0.0.1", port=port)
        app = controller._build_app()

        @app.middleware("http")
        async def _frontend_smoke_login(request, call_next):
            if request.url.path != "/__frontend_smoke_login":
                return await call_next(request)
            response = RedirectResponse(
                url="/repair-management?scope=A",
                status_code=302,
            )
            response.set_cookie(
                AUTH_COOKIE_NAME,
                session_id,
                httponly=True,
                samesite="lax",
                path="/",
            )
            return response

        bound_port = find_available_port("127.0.0.1", int(port or 18976))
        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=bound_port,
            log_level="warning",
            access_log=False,
            log_config=None,
        )
        server = uvicorn.Server(config)

        def _serve() -> None:
            server.run()

        server_thread = threading.Thread(
            target=_serve,
            name="ClipFlowFrontendRuntimeSmoke",
            daemon=True,
        )
        server_thread.start()
        if not _wait_until_listening("127.0.0.1", bound_port, timeout_s=5.0):
            raise RuntimeError("smoke FastAPI 服务启动超时")

        url = f"http://127.0.0.1:{bound_port}/"
        with tempfile.TemporaryDirectory() as script_dir:
            script_path = Path(script_dir) / "frontend_smoke.js"
            script_path.write_text(_build_playwright_script(url, session_id), encoding="utf-8")
            env = dict(os.environ)
            env["npm_config_cache"] = str(Path(script_dir) / "npm-cache")
            env["npm_config_prefix"] = str(Path(script_dir) / "npm-prefix")
            if local_playwright.is_dir() and node:
                command = [node, str(script_path)]
                env["NODE_PATH"] = str(frontend_node_modules)
            elif npx:
                command = [
                    npx,
                    "--yes",
                    "--package",
                    "playwright",
                    "node",
                    str(script_path),
                ]
            else:
                raise RuntimeError(
                    "未找到本地 playwright 依赖，且 npx 不可用；请先在 frontend 执行 npm install。"
                )
            completed = subprocess.run(
                command,
                cwd=str(BIN_DIR.parent),
                text=True,
                encoding="utf-8",
                errors="replace",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                timeout=60,
            )
        if completed.returncode != 0:
            raise RuntimeError(
                (completed.stderr or completed.stdout or "Playwright smoke failed").strip()
            )
        if keep_server_seconds > 0:
            time.sleep(float(keep_server_seconds))
        output = (completed.stdout or "").strip().splitlines()[-1]
        data = json.loads(output)
        data["url"] = url
        return data
    finally:
        if server is not None:
            server.should_exit = True
        if server_thread and server_thread.is_alive():
            server_thread.join(timeout=3)
        PortalRuntime.service = original_service
        PortalRuntime.auth_manager = original_auth
        PortalRuntime.state_store = original_store
        (
            PortalRuntime.notice_callback,
            PortalRuntime.ongoing_callback,
            PortalRuntime.ongoing_delete_callback,
            PortalRuntime.maintenance_action_callback,
        ) = original_callbacks
        temp_dir.cleanup()
        if previous_mock is None:
            os.environ.pop("CLIPFLOW_BACKEND_MOCK_EXTERNAL", None)
        else:
            os.environ["CLIPFLOW_BACKEND_MOCK_EXTERNAL"] = previous_mock


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a real-browser smoke test for the Vue portal dist.")
    parser.add_argument("--port", type=int, default=18976)
    parser.add_argument("--keep-server-seconds", type=float, default=0.0)
    args = parser.parse_args()
    try:
        result = run_smoke(port=args.port, keep_server_seconds=args.keep_server_seconds)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

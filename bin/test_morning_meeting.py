# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime as dt
import io
import sys
import tempfile
import unittest
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from unittest.mock import patch


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.morning_meeting import (  # noqa: E402
    MORNING_MEETING_TEMPLATE_SHA256,
    morning_meeting_template_bytes,
)
from lan_bitable_template_portal.portal_service import (  # noqa: E402
    MaintenancePortalService,
    PortalConflictError,
)


_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


class _MorningStore:
    def __init__(self, active_items: list[dict[str, Any]]) -> None:
        self.active_items = active_items
        self.documents: dict[tuple[str, str], dict[str, Any]] = {}

    def get_source_scope_snapshot(self, scope: str) -> dict[str, Any]:
        return {"exists": scope == "ALL", "updated_at": 1_788_200_000.0}

    def list_visible_qt_active_items(self) -> list[dict[str, Any]]:
        return [dict(item) for item in self.active_items]

    def get_critical_guard_weather_state(self) -> dict[str, Any]:
        return {
            "snapshot": {
                "weather": {
                    "condition": "多云",
                    "temperature": 26.9,
                    "wet_bulb": 26.5,
                    "observed_at": "2026-09-01 08:00:00",
                }
            }
        }

    def get_document(self, namespace: str, key: str) -> dict[str, Any] | None:
        value = self.documents.get((namespace, key))
        return dict(value) if isinstance(value, dict) else None

    def put_document(
        self,
        namespace: str,
        key: str,
        value: dict[str, Any],
    ) -> None:
        self.documents[(namespace, key)] = dict(value)


class MorningMeetingTests(unittest.TestCase):
    def _service(self) -> MaintenancePortalService:
        today = dt.date.today()
        date_key = today.isoformat()
        source_records = [
            {
                "work_type": "maintenance",
                "source_record_id": "source-maintenance",
                "title": "A楼水质检查",
                "building": "A楼",
                "building_codes": ["A"],
                "start_time": f"{date_key} 08:00",
                "end_time": f"{date_key} 12:00",
            },
            {
                "work_type": "change",
                "source_record_id": "source-change",
                "title": "A、B楼联合变更",
                "building": "A楼、B楼",
                "building_codes": ["A", "B"],
                "start_time": f"{date_key} 09:00",
                "end_time": f"{date_key} 18:00",
            },
            {
                "work_type": "repair",
                "source_record_id": "future-repair",
                "title": "未来检修",
                "building": "C楼",
                "building_codes": ["C"],
                "start_time": f"{today + dt.timedelta(days=1)} 09:00",
                "end_time": f"{today + dt.timedelta(days=1)} 18:00",
            },
        ]
        active_items = [
            {
                "work_type": "change",
                "source_record_id": "source-change",
                "target_record_id": "target-change",
                "title": "A、B楼联合变更",
                "building_codes": ["A", "B"],
                "status": "进行中",
                "start_time": f"{date_key} 09:00",
            },
            {
                "work_type": "polling",
                "target_record_id": "target-polling",
                "title": "H楼设备轮巡",
                "building_codes": ["H"],
                "status": "进行中",
            },
            {
                "work_type": "power",
                "active_item_id": "manual-power",
                "title": "园区上电操作",
                "building": "园区",
                "building_code": "CAMPUS",
                "status": "开始",
            },
            {
                "work_type": "adjust",
                "active_item_id": "manual-unknown",
                "title": "待确认楼栋调整",
                "status": "开始",
            },
            {
                "work_type": "power",
                "target_record_id": "target-110",
                "title": "110站上电操作",
                "building_codes": ["110"],
                "status": "进行中",
            },
            {
                "work_type": "event",
                "target_record_id": "target-event",
                "title": "不应出现的事件",
                "building_codes": ["E"],
                "status": "处理中",
            },
        ]
        service = object.__new__(MaintenancePortalService)
        service._state_store = _MorningStore(active_items)
        service._load_warnings = []
        service._workbench_records = lambda **_kwargs: [  # type: ignore[method-assign]
            dict(item) for item in source_records
        ]
        service._source_snapshot_active_payload = lambda record: dict(record)  # type: ignore[method-assign]
        service._project_ongoing_items = lambda _scope, items: list(items)  # type: ignore[method-assign]
        service._current_load_warnings = lambda: []  # type: ignore[method-assign]
        service.get_daily_summary = lambda **_kwargs: {  # type: ignore[method-assign]
            "items": [
                {
                    "work_type": "repair",
                    "source_record_id": "daily-repair",
                    "target_record_id": "target-repair",
                    "title": "E楼当天结束检修",
                    "building_codes": ["E"],
                    "status": "已结束",
                    "actions": [{"action": "end", "time": f"{date_key} 11:00"}],
                },
                {
                    "work_type": "adjust",
                    "title": "已删除调整",
                    "building_codes": ["D"],
                    "status": "已删除",
                    "actions": [{"action": "delete", "time": f"{date_key} 10:00"}],
                },
            ]
        }
        return service

    @staticmethod
    def _cell_text(root: ET.Element, reference: str) -> str:
        cell = root.find(f".//{{{_MAIN_NS}}}c[@r='{reference}']")
        if cell is None:
            return ""
        inline = cell.find(f"{{{_MAIN_NS}}}is")
        if inline is not None:
            return "".join(inline.itertext())
        value = cell.find(f"{{{_MAIN_NS}}}v")
        return "" if value is None else str(value.text or "")

    def test_preview_merges_plans_active_and_completed_by_building(self) -> None:
        preview = self._service().get_morning_meeting_preview(
            date=dt.date.today().isoformat()
        )
        rows = {item["scope"]: item["lines"] for item in preview["rows"]}

        self.assertEqual(rows["A"], ["值班巡检", "A楼水质检查", "A、B楼联合变更"])
        self.assertEqual(rows["B"], ["值班巡检", "A、B楼联合变更"])
        self.assertEqual(rows["C"], ["值班巡检"])
        self.assertEqual(rows["E"], ["值班巡检", "E楼当天结束检修"])
        self.assertEqual(
            rows["H"],
            [
                "值班巡检",
                "H楼设备轮巡",
                "【楼栋待确认】待确认楼栋调整",
                "【园区】园区上电操作",
            ],
        )
        self.assertEqual(rows["110"], ["值班巡检", "110站上电操作"])
        self.assertNotIn("未来检修", str(rows))
        self.assertNotIn("不应出现的事件", str(rows))
        self.assertNotIn("已删除调整", str(rows))
        self.assertEqual(rows["A"].count("A、B楼联合变更"), 1)
        self.assertEqual(preview["weather_condition"], "多云")

    def test_historical_date_is_rejected(self) -> None:
        with self.assertRaises(PortalConflictError):
            self._service().get_morning_meeting_preview(date="2020-01-01")

    def test_generation_preserves_template_and_is_idempotent(self) -> None:
        service = self._service()
        date_key = dt.date.today().isoformat()
        template = morning_meeting_template_bytes()
        self.assertEqual(
            __import__("hashlib").sha256(template).hexdigest(),
            MORNING_MEETING_TEMPLATE_SHA256,
        )
        with tempfile.TemporaryDirectory() as temporary, patch(
            "lan_bitable_template_portal.portal_service.get_data_file_path",
            side_effect=lambda name: str(Path(temporary) / name),
        ):
            generated = service.generate_morning_meeting(
                date=date_key,
                weather_condition="晴",
                dry_bulb_temperature=27.1,
                wet_bulb_temperature=25.2,
                operation_id="stable-operation",
                actor_name="测试用户",
            )
            repeated = service.generate_morning_meeting(
                date=date_key,
                weather_condition="雨",
                operation_id="stable-operation",
            )
            self.assertEqual(generated["generated_at"], repeated["generated_at"])
            output = Path(generated["file_path"])
            self.assertTrue(output.is_file())
            self.assertEqual(output.name, f"EA118-H楼{dt.date.today().month}月{dt.date.today().day}日晨会.xlsx")
            with zipfile.ZipFile(io.BytesIO(template)) as source, zipfile.ZipFile(output) as result:
                self.assertEqual(result.testzip(), None)
                for name in (
                    "xl/styles.xml",
                    "xl/workbook.xml",
                    "xl/media/image1.png",
                ):
                    self.assertEqual(result.read(name), source.read(name))
                sheet = ET.fromstring(result.read("xl/worksheets/sheet1.xml"))
            self.assertEqual(self._cell_text(sheet, "D2"), "晴")
            self.assertEqual(self._cell_text(sheet, "F2"), "27.1")
            self.assertEqual(self._cell_text(sheet, "H2"), "25.2")
            self.assertIn("1、值班巡检", self._cell_text(sheet, "B4"))
            self.assertIn("A楼水质检查", self._cell_text(sheet, "B4"))
            page_setup = sheet.find(f"{{{_MAIN_NS}}}pageSetup")
            self.assertEqual(page_setup.attrib.get("orientation"), "landscape")
            self.assertEqual(page_setup.attrib.get("fitToWidth"), "1")
            self.assertEqual(page_setup.attrib.get("fitToHeight"), "1")
            self.assertNotIn("scale", page_setup.attrib)
            restored = service.get_generated_morning_meeting(date=date_key)
            self.assertEqual(restored["file_path"], str(output.resolve()))
            reopened = service.get_morning_meeting_preview(date=date_key)
            self.assertEqual(reopened["weather_condition"], "晴")
            self.assertTrue(reopened["generated"])
            self.assertTrue(reopened["download_url"])


if __name__ == "__main__":
    unittest.main()

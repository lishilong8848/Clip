# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime as dt
import copy
import io
import json
import struct
import sys
import tempfile
import unittest
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.morning_meeting import (  # noqa: E402
    MORNING_MEETING_TEMPLATE_SHA256,
    MorningMeetingError,
    build_morning_meeting_workbook,
    morning_meeting_template_bytes,
)
from lan_bitable_template_portal import morning_meeting  # noqa: E402
from lan_bitable_template_portal.portal_service import (  # noqa: E402
    MaintenancePortalService,
    PortalConflictError,
    PortalError,
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
                "target_record_id": "target-power",
                "title": "园区上电操作",
                "building": "园区",
                "building_code": "CAMPUS",
                "status": "开始",
            },
            {
                "work_type": "adjust",
                "active_item_id": "manual-unknown",
                "target_record_id": "target-unknown",
                "title": "待确认楼栋调整",
                "status": "开始",
            },
            {
                "work_type": "maintenance",
                "active_item_id": "draft-maintenance",
                "title": "未上传本地草稿",
                "building_codes": ["D"],
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
        service._request_morning_meeting_environment = lambda duty_date, duty_shift: {  # type: ignore[method-assign]
            "ok": True,
            "status": "success",
            "duty_date": duty_date,
            "duty_shift": duty_shift,
            "batch_key": f"{duty_date}|{duty_shift}",
            "timezone": "Asia/Shanghai",
            "data": {"weather": "晴", "dry_bulb_temperature": 29.89, "wet_bulb_temperature": 26.46, "relative_humidity": 77.0},
            "sources": {
                "temperature": {"updated_at": "2026-09-03 12:01:29", "revision": 1},
                "weather": {"fetched_at": "2026-09-03 12:05:00"},
            },
            "warnings": [],
        }
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

    def test_preview_only_includes_ongoing_targets_and_completed_today(self) -> None:
        service = self._service()
        service._request_morning_meeting_environment = Mock(side_effect=AssertionError("通告预览不能等待交接班接口"))
        preview = service.get_morning_meeting_preview(
            date=dt.date.today().isoformat()
        )
        rows = {item["scope"]: item["lines"] for item in preview["rows"]}

        self.assertEqual(rows["A"], ["值班巡检", "A、B楼联合变更"])
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
        self.assertNotIn("A楼水质检查", str(rows))
        self.assertNotIn("未上传本地草稿", str(rows))
        self.assertNotIn("不应出现的事件", str(rows))
        self.assertNotIn("已删除调整", str(rows))
        self.assertEqual(rows["A"].count("A、B楼联合变更"), 1)
        self.assertIsNone(preview["weather_condition"], "不能借用旧重保天气")
        self.assertIsNone(preview.get("dry_bulb_temperature"))
        self.assertIsNone(preview.get("wet_bulb_temperature"))
        service._request_morning_meeting_environment.assert_not_called()

    def test_temperature_only_request_skips_notice_summary_and_preserves_shift_validation(self) -> None:
        service = self._service()
        with patch.object(service, "_morning_meeting_active_candidates", side_effect=AssertionError("温度请求不应重复汇总通告")):
            data = service.get_morning_meeting_preview(date=dt.date.today().isoformat(), temperature_only=True)
        self.assertEqual(data["dry_bulb_temperature"], 29.89)
        self.assertEqual(data["wet_bulb_temperature"], 26.46)
        self.assertEqual(data["weather_condition"], "晴")
        self.assertEqual(data["temperature_source"], "handover_environment")
        self.assertNotIn("rows", data)
        with self.assertRaises(PortalConflictError):
            service.get_morning_meeting_preview(date="2020-01-01", temperature_only=True)

    def test_historical_date_is_rejected(self) -> None:
        with self.assertRaises(PortalConflictError):
            self._service().get_morning_meeting_preview(date="2020-01-01")

    def test_handover_uses_shift_start_date_and_shanghai_clock(self) -> None:
        for now, expected_date, expected_shift in (
            (dt.datetime(2026, 9, 4, 2), "2026-09-03", "night"),
            (dt.datetime(2026, 9, 4, 8, 59, 59), "2026-09-03", "night"),
            (dt.datetime(2026, 9, 4, 9), "2026-09-04", "day"),
            (dt.datetime(2026, 9, 4, 17, 59, 59), "2026-09-04", "day"),
            (dt.datetime(2026, 9, 4, 18), "2026-09-04", "night"),
            (dt.datetime(2026, 9, 3, 18, tzinfo=dt.timezone.utc), "2026-09-03", "night"),
        ):
            with self.subTest(now=now):
                service = self._service()
                request = Mock(wraps=service._request_morning_meeting_environment)
                service._request_morning_meeting_environment = request
                data, _warnings = service._morning_meeting_environment(now=now)
                request.assert_called_once_with(expected_date, expected_shift)
                self.assertEqual(data["dry_bulb_temperature"], 29.89)

    def test_temperature_parsing_keeps_zero_but_rejects_empty_or_ambiguous_values(self) -> None:
        for raw, expected in (
            ("0", 0.0), (" 27.7 摄氏度 ", 27.7), ("-1.5℃", -1.5), ("25.8 °c", 25.8),
            (None, None), ("", None), ("--", None), (True, None), ("NaN", None),
            ("27/28", None), ("27F", None), ("81℃", None), ("-51", None),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(MaintenancePortalService._morning_meeting_saved_temperature(raw), expected)

    def test_wrong_environment_shift_or_status_does_not_use_any_values(self) -> None:
        now = dt.datetime(2026, 9, 3, 12)
        valid = self._service()._request_morning_meeting_environment("2026-09-03", "day")
        for changes in (
            {"duty_date": "2026-09-02"}, {"duty_shift": "night"},
            {"batch_key": "2026-09-02|night"},
            {"timezone": "UTC"}, {"status": "bad"}, {"ok": False}, {"data": None},
        ):
            with self.subTest(changes=changes):
                service = self._service()
                service._request_morning_meeting_environment = Mock(return_value={**valid, **changes})
                data, warnings = service._morning_meeting_environment(now=now)
                self.assertIsNone(data["weather_condition"])
                self.assertIsNone(data["dry_bulb_temperature"])
                self.assertIsNone(data["wet_bulb_temperature"])
                self.assertIn("读取失败", " ".join(warnings))

    def test_partial_environment_accepts_each_available_field_without_zero_fill(self) -> None:
        service = self._service()
        valid = service._request_morning_meeting_environment("2026-09-03", "day")
        for status, values, expected in (
            ("partial", {"weather": "小雨", "dry_bulb_temperature": None, "wet_bulb_temperature": None}, ("小雨", None, None)),
            ("partial", {"weather": None, "dry_bulb_temperature": 29, "wet_bulb_temperature": 26}, (None, 29, 26)),
            ("partial", {"weather": "晴", "dry_bulb_temperature": "bad", "wet_bulb_temperature": 0}, ("晴", None, 0)),
            ("unavailable", {}, (None, None, None)),
            ("success", {"weather": "晴", "dry_bulb_temperature": 20, "wet_bulb_temperature": 21}, ("晴", 20, 21)),
        ):
            with self.subTest(status=status, values=values):
                service._request_morning_meeting_environment = Mock(return_value={**valid, "status": status, "data": values, "warnings": ["weather_refreshing", "wet_bulb_exceeds_dry_bulb"]})
                data, warnings = service._morning_meeting_environment(now=dt.datetime(2026, 9, 3, 12))
                self.assertEqual(tuple(data[key] for key in ("weather_condition", "dry_bulb_temperature", "wet_bulb_temperature")), expected)
                self.assertIn("可60秒后", " ".join(warnings))
                self.assertIn("湿球温度高于干球温度", " ".join(warnings))

    def test_environment_reads_updated_values_and_never_reuses_old_success_on_failure(self) -> None:
        service = self._service()
        original = service._request_morning_meeting_environment
        request = Mock(wraps=service._request_morning_meeting_environment)
        service._request_morning_meeting_environment = request
        now = dt.datetime(2026, 9, 3, 12)
        data, _ = service._morning_meeting_environment(now=now)
        self.assertEqual(data["dry_bulb_temperature"], 29.89)
        updated = original("2026-09-03", "day")
        updated["data"].update(weather="小雨", dry_bulb_temperature=25)
        request.return_value = updated
        data, _ = service._morning_meeting_environment(now=now)
        self.assertEqual(data["weather_condition"], "小雨")
        self.assertEqual(data["dry_bulb_temperature"], 25)
        self.assertEqual(request.call_count, 2)
        request.side_effect = TimeoutError("timeout")
        data, warnings = service._morning_meeting_environment(now=now)
        self.assertIsNone(data["weather_condition"])
        self.assertIsNone(data["dry_bulb_temperature"])
        self.assertIn("连接5秒、读取15秒", " ".join(warnings))
        request.side_effect = original
        service._morning_meeting_environment(now=dt.datetime(2026, 9, 3, 18))
        request.assert_called_with("2026-09-03", "night")

    def test_handover_http_request_is_bounded_read_only_and_has_no_feishu_credentials(self) -> None:
        payload = self._service()._request_morning_meeting_environment("2026-09-03", "day")
        with patch("lan_bitable_template_portal.portal_service.HTTPConnection") as constructor:
            connection = constructor.return_value
            response = connection.getresponse.return_value
            response.status = 200
            response.read.return_value = json.dumps(payload).encode()
            result = MaintenancePortalService._request_morning_meeting_environment("2026-09-03", "day")
            self.assertEqual(result, payload)
            constructor.assert_called_once_with("192.168.224.157", 18765, timeout=5.0)
            connection.connect.assert_called_once()
            connection.sock.settimeout.assert_called_once_with(15.0)
            connection.request.assert_called_once_with(
                "GET", "/api/handover/review/environment?duty_date=2026-09-03&duty_shift=day",
                headers={"Accept": "application/json"},
            )
            connection.close.assert_called_once()
            for status in (404, 409, 503):
                response.status = status
                response.read.return_value = b'{"detail":"not ready"}'
                with self.subTest(status=status), self.assertRaisesRegex(PortalError, f"HTTP {status}.*not ready"):
                    MaintenancePortalService._request_morning_meeting_environment("2026-09-03", "day")

    def test_summary_date_switches_at_nine(self) -> None:
        previous_day = dt.date(2026, 9, 1).isoformat()
        current_day = dt.date(2026, 9, 2).isoformat()

        self.assertEqual(
            MaintenancePortalService._morning_meeting_summary_date(
                dt.datetime(2026, 9, 2, 8, 59, 59)
            ),
            previous_day,
        )
        self.assertEqual(
            MaintenancePortalService._morning_meeting_summary_date(
                dt.datetime(2026, 9, 2, 9, 0, 0)
            ),
            current_day,
        )

    def test_generation_preserves_template_and_is_idempotent(self) -> None:
        service = self._service()
        service._request_morning_meeting_environment = Mock(side_effect=AssertionError("表格生成不能等待交接班接口"))
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
            service._request_morning_meeting_environment.assert_not_called()
            output = Path(generated["file_path"])
            self.assertTrue(output.is_file())
            self.assertEqual(output.name, f"EA118-H楼{dt.date.today().month}月{dt.date.today().day}日晨会.xlsx")
            with zipfile.ZipFile(io.BytesIO(template)) as source, zipfile.ZipFile(output) as result:
                self.assertEqual(result.testzip(), None)
                for name in (
                    "xl/styles.xml",
                    "xl/workbook.xml",
                    "xl/drawings/_rels/drawing1.xml.rels",
                ):
                    self.assertEqual(result.read(name), source.read(name))
                web_logo = morning_meeting._WEB_LOGO_PATH.read_bytes()
                self.assertEqual(result.read("xl/media/image1.png"), web_logo)
                self.assertNotEqual(result.read("xl/media/image1.png"), source.read("xl/media/image1.png"))
                for name in source.namelist():
                    if name not in {
                        "xl/worksheets/sheet1.xml",
                        "xl/media/image1.png",
                        "xl/drawings/drawing1.xml",
                    }:
                        self.assertEqual(result.read(name), source.read(name), name)
                drawing = ET.fromstring(result.read("xl/drawings/drawing1.xml"))
                ns = {"xdr": morning_meeting._DRAWING_NS}
                anchor = drawing.find("xdr:oneCellAnchor", ns)
                self.assertIsNotNone(anchor)
                extent = anchor.find("xdr:ext", ns)
                width, height = int(extent.attrib["cx"]), int(extent.attrib["cy"])
                image_width, image_height = struct.unpack(">II", web_logo[16:24])
                self.assertAlmostEqual(width / height, image_width / image_height, places=4)
                self.assertLessEqual(width, 1235710)
                self.assertLessEqual(height, 345440)
                self.assertEqual(anchor.find("xdr:from/xdr:row", ns).text, "0")
                self.assertEqual(anchor.find("xdr:from/xdr:col", ns).text, "0")
                sheet = ET.fromstring(result.read("xl/worksheets/sheet1.xml"))
            self.assertEqual(self._cell_text(sheet, "D2"), "晴")
            self.assertEqual(self._cell_text(sheet, "F2"), "27.1")
            self.assertEqual(self._cell_text(sheet, "H2"), "25.2")
            self.assertIn("1、值班巡检", self._cell_text(sheet, "B4"))
            self.assertIn("A、B楼联合变更", self._cell_text(sheet, "B4"))
            self.assertNotIn("A楼水质检查", self._cell_text(sheet, "B4"))
            page_setup = sheet.find(f"{{{_MAIN_NS}}}pageSetup")
            self.assertEqual(page_setup.attrib.get("orientation"), "landscape")
            self.assertEqual(page_setup.attrib.get("fitToWidth"), "1")
            self.assertEqual(page_setup.attrib.get("fitToHeight"), "1")
            self.assertNotIn("scale", page_setup.attrib)
            restored = service.get_generated_morning_meeting(date=date_key)
            self.assertEqual(restored["file_path"], str(output.resolve()))
            with patch.object(service, "_morning_meeting_environment", side_effect=AssertionError("应复用生成快照")):
                reopened = service.get_morning_meeting_preview(date=date_key)
            self.assertEqual(reopened["weather_condition"], "晴")
            self.assertTrue(reopened["generated"])
            self.assertTrue(reopened["download_url"])

    def test_regeneration_reads_environment_once_and_preserves_old_file_on_failure(self) -> None:
        service = self._service()
        original_request = service._request_morning_meeting_environment
        request = Mock(wraps=original_request)
        service._request_morning_meeting_environment = request
        date_key = dt.date.today().isoformat()
        with tempfile.TemporaryDirectory() as temporary, patch(
            "lan_bitable_template_portal.portal_service.get_data_file_path",
            side_effect=lambda name: str(Path(temporary) / name),
        ):
            service.generate_morning_meeting(date=date_key, weather_condition="旧天气", dry_bulb_temperature=30, wet_bulb_temperature=28, operation_id="first")
            request.assert_not_called()
            regenerated = service.generate_morning_meeting(date=date_key, weather_condition="旧天气", dry_bulb_temperature=30, wet_bulb_temperature=28, operation_id="regenerate")
            self.assertEqual(request.call_count, 1)
            model = regenerated["model"]
            self.assertEqual(model["weather_condition"], "晴")
            self.assertEqual(model["dry_bulb_temperature"], 29.9)
            self.assertEqual(model["wet_bulb_temperature"], 26.5)
            output = Path(regenerated["file_path"])
            with zipfile.ZipFile(output) as workbook:
                sheet = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
            self.assertEqual(self._cell_text(sheet, "D2"), "晴")
            self.assertEqual(self._cell_text(sheet, "F2"), "29.9")
            self.assertEqual(self._cell_text(sheet, "H2"), "26.5")
            self.assertEqual(service.generate_morning_meeting(date=date_key, operation_id="regenerate"), regenerated)
            self.assertEqual(request.call_count, 1, "响应丢失后重试同一操作不能再次生成")
            previous_file = output.read_bytes()
            request.side_effect = TimeoutError("timeout")
            with self.assertRaisesRegex(PortalError, "未重新生成，原文件已保留"):
                service.generate_morning_meeting(date=date_key, operation_id="retry-new")
            self.assertEqual(output.read_bytes(), previous_file)
            self.assertEqual(service.get_generated_morning_meeting(date=date_key), regenerated)
            def partial(duty_date, duty_shift):
                payload = original_request(duty_date, duty_shift)
                payload["status"] = "partial"
                payload["data"]["weather"] = None
                return payload
            request.side_effect = partial
            with self.assertRaisesRegex(PortalError, "有效的天气"):
                service.generate_morning_meeting(date=date_key, operation_id="retry-new")
            self.assertEqual(output.read_bytes(), previous_file)
            request.side_effect = original_request
            recovered = service.generate_morning_meeting(date=date_key, operation_id="retry-new")
            self.assertEqual(recovered["operation_id"], "retry-new")
            self.assertEqual(recovered["model"]["weather_condition"], "晴")

    def test_missing_or_invalid_web_logo_does_not_replace_existing_workbook(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "existing.xlsx"
            output.write_bytes(b"previous-workbook")
            invalid_logo = root / "invalid.png"
            invalid_logo.write_bytes(b"not-an-image")
            for logo_path in (root / "missing.png", invalid_logo):
                with self.subTest(path=logo_path.name), patch.object(morning_meeting, "_WEB_LOGO_PATH", logo_path):
                    with self.assertRaisesRegex(MorningMeetingError, "网页 Logo 无法读取"):
                        build_morning_meeting_workbook(output, {"date": dt.date.today().isoformat()})
                    self.assertEqual(output.read_bytes(), b"previous-workbook")


if __name__ == "__main__":
    unittest.main()

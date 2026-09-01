# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime as dt
import json
import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    MaintenancePortalService,
    PortalError,
    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
    REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
    REPAIR_SNAPSHOT_SOURCE_PROJECTS,
)
from upload_event_module.services import robot_webhook  # noqa: E402


TEST_DATE = "2026-07-28"


class _DailyTaskStore:
    def __init__(self) -> None:
        self.documents: dict[tuple[str, str], dict[str, Any]] = {}
        self.repair_snapshots = {
            REPAIR_SNAPSHOT_SOURCE_PROJECTS: [
                {
                    "record_id": "rec_project_private",
                    "created_time": f"{TEST_DATE} 09:00",
                    "display_fields": {
                        "维修名称": "E楼冷机维修",
                        "所属数据中心/楼栋-使用": "南通E楼",
                        "所属专业": "暖通",
                        "流程": "维修中",
                    },
                    "raw_fields": {},
                }
            ],
            REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS: [
                {
                    "record_id": "rec_followup_private",
                    "created_time": f"{TEST_DATE} 10:00",
                    "display_fields": {
                        REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: (
                            "rec_project_private"
                        ),
                        "创建时间": f"{TEST_DATE} 10:00",
                        "维修进度": "60%",
                    },
                    "raw_fields": {
                        REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: (
                            "rec_project_private"
                        )
                    },
                }
            ],
        }
        self.water_record = {
            "record_id": "rec_water_private",
            "building": "E楼",
            "meter": "总水表",
            "frequency": "每日",
            "shift": "白班",
            "statistic_date": TEST_DATE,
            "created_time": f"{TEST_DATE} 07:30",
            "computed_usage": 12.5,
            "meter_value": 123.5,
            "previous_value_text": "120",
        }
        self.previous_water_record = {
            "record_id": "rec_water_previous",
            "meter_value": 110,
            "statistic_date": "2026-07-27",
        }
        self.previous_water_lookup: dict[str, Any] = {}
        self.audits = [
            {
                "audit_id": "audit_mop_private",
                "domain": "mop",
                "action": "upload_signed",
                "status": "success",
                "scope": "E",
                "source_record_id": "rec_maintenance_private",
                "metadata": {"file_name": "E楼冷机维护单.xlsx"},
                "completed_at": 1785196800.0,
                "updated_at": 1785196800.0,
                "started_at": 1785196799.0,
            },
            {
                "audit_id": "audit_water_private",
                "domain": "water_consumption",
                "action": "create_record",
                "status": "success",
                "scope": "E",
                "target_record_id": "rec_water_private",
                "metadata": {},
                "completed_at": 1785180000.0,
                "updated_at": 1785180000.0,
                "started_at": 1785179999.0,
            },
        ]

    def get_document(self, namespace: str, key: str) -> dict[str, Any] | None:
        payload = self.documents.get((namespace, key))
        return dict(payload) if isinstance(payload, dict) else None

    def put_document(
        self, namespace: str, key: str, payload: dict[str, Any]
    ) -> None:
        self.documents[(namespace, key)] = dict(payload)

    def list_documents(
        self, namespace: str, **_kwargs: Any
    ) -> list[dict[str, Any]]:
        return [
            {"key": key, "payload": dict(payload)}
            for (stored_namespace, key), payload in self.documents.items()
            if stored_namespace == namespace
        ]

    def get_repair_snapshot(
        self, source_key: str, **_kwargs: Any
    ) -> dict[str, Any]:
        return {"records": list(self.repair_snapshots.get(source_key, []))}

    def list_business_operation_audits(
        self,
        *,
        domain: str = "",
        scope: str = "",
        **_kwargs: Any,
    ) -> list[dict[str, Any]]:
        return [
            dict(item)
            for item in self.audits
            if (not domain or item.get("domain") == domain)
            and (not scope or item.get("scope") == scope)
        ]

    def query_water_consumption_records(
        self, **_kwargs: Any
    ) -> dict[str, Any]:
        return {
            "records": [dict(self.water_record)],
            "total": 1,
            "has_more": False,
        }

    def get_water_consumption_record(
        self, record_id: str
    ) -> dict[str, Any] | None:
        return (
            dict(self.water_record)
            if record_id == self.water_record["record_id"]
            else None
        )

    def find_previous_water_consumption_record(
        self, **kwargs: Any
    ) -> dict[str, Any] | None:
        self.previous_water_lookup = dict(kwargs)
        return dict(self.previous_water_record)


class DailyTaskChecklistTests(unittest.TestCase):
    def _service(self) -> MaintenancePortalService:
        service = object.__new__(MaintenancePortalService)
        service._state_store = _DailyTaskStore()
        service._repair_snapshots_enabled = True
        service._load_day_summary_locked = lambda day=None: {  # type: ignore[method-assign]
            "date": day or TEST_DATE,
            "items": [
                {
                    "key": "notice-private",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "title": "E楼月度维护",
                    "building": "E楼",
                    "building_codes": ["E"],
                    "specialty": "电气",
                    "status": "进行中",
                    "started_at": f"{TEST_DATE} 08:00",
                    "actions": [
                        {
                            "action": "start",
                            "time": f"{TEST_DATE} 08:00",
                        }
                    ],
                }
            ],
        }
        service.get_event_monthly_snapshot = lambda **_kwargs: {  # type: ignore[method-assign]
            "records": [
                {
                    "source_record_id": "rec_event_private",
                    "title": "E楼UPS告警",
                    "alarm_desc": "E楼UPS告警",
                    "building": "E楼",
                    "building_codes": ["E"],
                    "specialty": "电气",
                    "level": "I3",
                    "occurrence_time": f"{TEST_DATE} 08:30",
                    "status": "处理中",
                }
            ]
        }
        return service

    def test_daily_tasks_are_grouped_and_do_not_expose_record_ids(self) -> None:
        payload = self._service().get_daily_task_checklist(
            scope="E",
            date=TEST_DATE,
        )

        self.assertEqual(payload["stats"]["total"], 5)
        self.assertEqual(
            {item["key"]: item["count"] for item in payload["categories"]},
            {
                "notice": 1,
                "event": 1,
                "repair": 1,
                "mop": 1,
                "water": 1,
            },
        )
        repair = next(
            item for item in payload["tasks"] if item["category"] == "repair"
        )
        self.assertEqual(repair["progress_percent"], 60)
        self.assertIn("维修跟进", repair["action_summary"])
        serialized = json.dumps(payload, ensure_ascii=False)
        self.assertNotIn("rec_project_private", serialized)
        self.assertNotIn("rec_event_private", serialized)
        self.assertNotIn("rec_water_private", serialized)

    def test_water_report_includes_meter_values_and_change_rate(self) -> None:
        service = self._service()
        water = service._daily_water_tasks(scope="E", date=TEST_DATE)[0]

        self.assertEqual(water["level"], "12.5 t")
        self.assertEqual(water["water_current_value"], "123.5")
        self.assertEqual(water["water_previous_value"], "120")
        self.assertEqual(water["water_change_ratio"], "+2.92%")

        report = {
            "window_start": "2026-07-27 17:50:00",
            "window_end": "2026-07-28 17:50:00",
            "stats": service._daily_report_stats([water]),
            "categories": service._daily_report_groups([water]),
            "event_sla": {"stats": {}, "events": []},
            "warnings": [],
        }
        card = service.build_daily_work_report_card(
            report,
            scope_label="E楼",
            link_scope="E",
            public_base="",
        )
        content = card["elements"][0]["text"]["content"]
        self.assertIn("当期耗水量 12.5 t", content)
        self.assertIn("当前数值 123.5 · 上次数值 120 · 变化率 +2.92%", content)

    def test_water_report_falls_back_to_same_type_previous_record(self) -> None:
        service = self._service()
        service._state_store.water_record["previous_value_text"] = ""

        water = service._daily_water_tasks(scope="E", date=TEST_DATE)[0]

        self.assertEqual(water["water_previous_value"], "110")
        self.assertEqual(water["water_change_ratio"], "+12.27%")
        self.assertEqual(
            service._state_store.previous_water_lookup,
            {
                "scope": "E",
                "meter": "总水表",
                "frequency": "每日",
                "shift": "白班",
                "statistic_date": TEST_DATE,
                "exclude_record_id": "rec_water_private",
            },
        )

    def test_water_report_handles_zero_and_missing_baselines(self) -> None:
        service = self._service()
        service._state_store.water_record["previous_value_text"] = "0"
        zero = service._daily_water_tasks(scope="E", date=TEST_DATE)[0]
        self.assertEqual(
            zero["water_change_ratio"], "原值为 0，按大幅变化处理"
        )

        service._state_store.water_record["previous_value_text"] = ""
        service._state_store.previous_water_record = {}
        missing = service._daily_water_tasks(scope="E", date=TEST_DATE)[0]
        self.assertEqual(missing["water_previous_value"], "未填写")
        self.assertEqual(missing["water_change_ratio"], "无法计算")

    def test_invalid_date_is_rejected(self) -> None:
        with self.assertRaisesRegex(PortalError, "YYYY-MM-DD"):
            self._service().get_daily_task_checklist(
                scope="E",
                date="2026/07/28",
            )

    def test_same_day_event_summary_merges_with_snapshot(self) -> None:
        service = self._service()
        original_summary = service._load_day_summary_locked()
        service._load_day_summary_locked = lambda day=None: {  # type: ignore[method-assign]
            "date": day or TEST_DATE,
            "items": [
                *original_summary["items"],
                {
                    "key": "event-summary-private",
                    "work_type": "event",
                    "notice_type": "事件通告",
                    "target_record_id": "rec_event_private",
                    "title": "E楼UPS告警",
                    "building": "E楼",
                    "building_codes": ["E"],
                    "specialty": "电气",
                    "level": "I3",
                    "status": "处理中",
                    "occurrence_time": f"{TEST_DATE} 08:30",
                    "last_updated_at": f"{TEST_DATE} 09:10",
                    "actions": [
                        {
                            "action": "update",
                            "time": f"{TEST_DATE} 09:10",
                        }
                    ],
                },
            ],
        }

        payload = service.get_daily_task_checklist(
            scope="E",
            date=TEST_DATE,
        )
        event_tasks = [
            item for item in payload["tasks"] if item["category"] == "event"
        ]

        self.assertEqual(len(event_tasks), 1)
        self.assertIn("更新", event_tasks[0]["action_summary"])
        self.assertEqual(event_tasks[0]["time"], "09:10")

    def test_event_sla_excludes_recovery_and_uses_previous_response_after_fourth(self) -> None:
        service = self._service()
        occurrence = dt.datetime(2026, 7, 28, 10, 0, 0)
        service._daily_report_event_timelines = lambda **_kwargs: [  # type: ignore[method-assign]
            {
                "event_key": "event-i3",
                "title": "I3事件",
                "building": "E楼",
                "building_codes": ["E"],
                "initial_level": "I3",
                "occurrence_dt": occurrence,
                "occurrence_precision": "second",
                "ended": True,
                "actions": [
                    {"action": "start", "response_dt": occurrence + dt.timedelta(minutes=1), "precision": "second"},
                    {"action": "update", "response_dt": occurrence + dt.timedelta(minutes=4), "precision": "second"},
                    {"action": "recover", "response_dt": occurrence + dt.timedelta(minutes=6), "precision": "second"},
                    {"action": "update", "response_dt": occurrence + dt.timedelta(minutes=9), "precision": "second"},
                    {"action": "update", "response_dt": occurrence + dt.timedelta(minutes=29), "precision": "second"},
                    {"action": "end", "response_dt": occurrence + dt.timedelta(minutes=60, seconds=5), "precision": "second"},
                ],
            }
        ]

        payload = service._daily_report_event_sla(
            scope="E",
            window_start=occurrence - dt.timedelta(hours=1),
            window_end=occurrence + dt.timedelta(hours=2),
        )
        event = payload["events"][0]

        self.assertEqual([row["number"] for row in event["rows"]], [1, 2, 3, 4, 5])
        self.assertEqual(len(event["recoveries"]), 1)
        self.assertEqual(event["rows"][4]["deadline"], "2026-07-28 10:59:00")
        self.assertEqual(event["rows"][4]["late_text"], "1分5秒")
        self.assertIsNone(event["pending"])

    def test_non_i3_pending_is_marked_without_overdue_duration(self) -> None:
        service = self._service()
        occurrence = dt.datetime(2026, 7, 28, 10, 0, 0)
        service._daily_report_event_timelines = lambda **_kwargs: [  # type: ignore[method-assign]
            {
                "event_key": "event-i2",
                "title": "I2事件",
                "building": "E楼",
                "building_codes": ["E"],
                "initial_level": "I2",
                "occurrence_dt": occurrence,
                "occurrence_precision": "second",
                "ended": False,
                "actions": [
                    {"action": "start", "response_dt": occurrence + dt.timedelta(minutes=1), "precision": "second"},
                    {"action": "update", "response_dt": occurrence + dt.timedelta(minutes=4), "precision": "second"},
                    {"action": "update", "response_dt": occurrence + dt.timedelta(minutes=9), "precision": "second"},
                    {"action": "update", "response_dt": occurrence + dt.timedelta(minutes=14), "precision": "second"},
                ],
            }
        ]

        event = service._daily_report_event_sla(
            scope="E",
            window_start=occurrence - dt.timedelta(hours=1),
            window_end=occurrence + dt.timedelta(hours=2),
        )["events"][0]

        self.assertEqual(event["pending"]["number"], 5)
        self.assertEqual(event["pending"]["deadline"], "2026-07-28 10:29:00")
        self.assertNotIn("late_seconds", event["pending"])

    def test_event_timeline_persists_exact_seconds_and_recovery_kind(self) -> None:
        service = self._service()
        payload = {
            "target_record_id": "rec_event_timeline",
            "title": "E楼UPS告警",
            "building": "E楼",
            "time_str": "2026-07-28 23:59:30",
            "response_time": "00:01:05",
            "event_level": "I3",
            "recover_selected": True,
        }

        service._record_event_timeline_action(
            job_id="job-recover",
            action="update",
            payload=payload,
        )
        stored = service._state_store.get_document(
            "daily_event_timeline", "rec_event_timeline"
        )

        self.assertEqual(stored["building_codes"], ["E"])
        self.assertEqual(stored["actions"][0]["action"], "recover")
        self.assertEqual(
            stored["actions"][0]["response_time"], "2026-07-29 00:01:05"
        )
        self.assertEqual(stored["actions"][0]["precision"], "second")

    def test_rolling_window_excludes_both_boundaries_and_merges_task_ids(self) -> None:
        service = self._service()
        start = dt.datetime(2026, 7, 27, 17, 50)
        end = dt.datetime(2026, 7, 28, 17, 50)

        def checklist(*, scope: str, date: str) -> dict[str, Any]:
            del scope
            rows = {
                "2026-07-27": [
                    {"task_id": "before", "title": "过早", "category": "notice", "sort_time": start.timestamp() - 1},
                    {"task_id": "same", "title": "跨日任务", "category": "notice", "sort_time": start.timestamp(), "action_summary": "开始"},
                ],
                "2026-07-28": [
                    {"task_id": "same", "title": "跨日任务", "category": "notice", "sort_time": end.timestamp() - 1, "action_summary": "更新"},
                    {"task_id": "at-end", "title": "下个周期", "category": "notice", "sort_time": end.timestamp()},
                ],
            }
            return {"tasks": rows[date], "warnings": []}

        service.get_daily_task_checklist = checklist  # type: ignore[method-assign]
        tasks, _warnings = service._daily_report_tasks_for_window(
            scope="E", window_start=start, window_end=end
        )

        self.assertEqual([item["task_id"] for item in tasks], ["same"])
        self.assertEqual(tasks[0]["action_summary"], "开始；更新")

    def test_daily_report_delivery_is_idempotent_across_retries(self) -> None:
        service = self._service()
        service._critical_guard_public_base_url = lambda: ""  # type: ignore[method-assign]
        now = dt.datetime(2026, 7, 28, 17, 50)
        document = {
            "report_key": "2026-07-28",
            "cards": {"E": {"header": {}, "elements": []}},
            "deliveries": {
                "E": {
                    "card_key": "E",
                    "open_id": "ou_test",
                    "status": "pending",
                    "attempts": 0,
                    "message_uuid": "stable-uuid",
                }
            },
            "status": "pending",
        }
        service._new_daily_work_report_document = lambda **_kwargs: dict(document)  # type: ignore[method-assign]

        with patch(
            "lan_bitable_template_portal.portal_service.send_interactive_to_open_ids",
            return_value=(True, "ok", []),
        ) as sender:
            first = service.process_daily_work_reports(now=now, force=True)
            second = service.process_daily_work_reports(now=now, force=True)

        self.assertEqual(first["sent"], 1)
        self.assertEqual(second["sent"], 0)
        sender.assert_called_once()
        self.assertEqual(sender.call_args.kwargs["message_uuid"], "stable-uuid")

    def test_interactive_sender_forwards_stable_uuid(self) -> None:
        with patch.object(
            robot_webhook,
            "_get_tenant_access_token",
            return_value=("token", ""),
        ), patch.object(
            robot_webhook,
            "_request_json",
            return_value={"code": 0},
        ) as request_json:
            ok, _message, _results = robot_webhook.send_interactive_to_open_ids(
                {"header": {}, "elements": []},
                ["ou_test"],
                message_uuid="stable-card-uuid",
            )

        self.assertTrue(ok)
        self.assertEqual(
            request_json.call_args.kwargs["json_payload"]["uuid"],
            "stable-card-uuid",
        )

    def test_empty_card_still_lists_every_summary_category(self) -> None:
        service = self._service()
        service._critical_guard_public_base_url = lambda: ""  # type: ignore[method-assign]
        report = {
            "window_start": "2026-07-27 17:50:00",
            "window_end": "2026-07-28 17:50:00",
            "stats": {"total": 0, "ongoing": 0, "completed": 0, "attention": 0},
            "categories": service._daily_report_groups([]),
            "event_sla": {"stats": {}, "events": []},
            "warnings": [],
        }

        card = service.build_daily_work_report_card(
            report, scope_label="E楼", link_scope="E"
        )
        content = card["elements"][0]["text"]["content"]

        self.assertIn(
            "分类汇总** 通告 0 · 事件 0 · 检修 0 · 维护单 0 · 水耗 0",
            content,
        )

    def test_full_report_groups_tasks_by_building_without_duplicates(self) -> None:
        service = self._service()

        def task(task_id: str, title: str, building: str, category: str = "notice") -> dict[str, Any]:
            return {
                "task_id": task_id,
                "title": title,
                "building": building,
                "category": category,
                "category_label": "事件" if category == "event" else "通告",
                "type_label": "事件通告" if category == "event" else "维保通告",
                "status": "进行中",
                "status_tone": "ongoing",
                "time": "10:00",
                "sort_time": 10,
                "action_summary": "开始",
            }

        cross = task("cross", "跨楼事项", "A楼、B楼")
        event = {
            "event_key": "event-c",
            "title": "C楼事件",
            "building": "C楼",
            "building_codes": ["C"],
            "level": "I3",
            "status": "进行中",
            "occurrence_time": "2026-07-28 09:00:00",
            "rows": [],
            "pending": {"number": 1, "deadline": "2026-07-28 09:02:00"},
        }
        reports = [
            {"scope": "A", "window_start": "start", "window_end": "end", "tasks": [task("a", "A楼事项", "A楼"), cross], "event_sla": {"events": []}},
            {"scope": "B", "window_start": "start", "window_end": "end", "tasks": [cross], "event_sla": {"events": []}},
            {"scope": "C", "window_start": "start", "window_end": "end", "tasks": [task("event-c", "C楼事件", "C楼", "event")], "event_sla": {"events": [event]}},
            {"scope": "D", "window_start": "start", "window_end": "end", "tasks": [], "event_sla": {"events": []}},
            {"scope": "E", "window_start": "start", "window_end": "end", "tasks": [task("unknown", "待确认事项", "")], "event_sla": {"events": []}},
        ]

        combined = service._combine_daily_work_reports(reports)
        sections = combined["building_sections"]

        self.assertEqual(
            [item["key"] for item in sections],
            ["A", "B", "C", "D", "E", "CROSS", "UNKNOWN"],
        )
        tasks_by_section = {
            item["key"]: [task["task_id"] for task in item["tasks"]]
            for item in sections
        }
        self.assertEqual(tasks_by_section["A"], ["a"])
        self.assertEqual(tasks_by_section["C"], ["event-c"])
        self.assertEqual(tasks_by_section["CROSS"], ["cross"])
        self.assertEqual(tasks_by_section["UNKNOWN"], ["unknown"])
        self.assertEqual(sum(row == "cross" for rows in tasks_by_section.values() for row in rows), 1)
        self.assertEqual(sections[2]["event_sla"]["stats"]["pending"], 1)

        card = service.build_daily_work_report_card(
            combined,
            scope_label="A-E楼全楼",
            link_scope="ALL",
            public_base="",
        )
        content = card["elements"][0]["text"]["content"]
        positions = [
            content.index(f"**{label} ·")
            for label in (
                "A楼",
                "B楼",
                "C楼",
                "D楼",
                "E楼",
                "跨楼栋/园区",
                "楼栋待确认",
            )
        ]
        self.assertEqual(positions, sorted(positions))
        self.assertEqual(content.count("跨楼事项"), 1)

    def test_manual_all_report_builds_the_same_grouped_card(self) -> None:
        service = self._service()
        service._critical_guard_public_base_url = lambda: ""  # type: ignore[method-assign]
        service._load_signature_people = lambda: [  # type: ignore[method-assign]
            {"name": "甲", "open_id": "ou_a"}
        ]

        def snapshot(*, scope: str, report_end: dt.datetime) -> dict[str, Any]:
            del report_end
            task = {
                "task_id": f"task-{scope}",
                "category": "notice",
                "title": f"{scope}楼事项",
                "building": f"{scope}楼",
                "status": "进行中",
                "status_tone": "ongoing",
                "sort_time": 1,
            }
            return {
                "scope": scope,
                "window_start": "start",
                "window_end": "end",
                "tasks": [task],
                "event_sla": {"events": []},
                "warnings": [],
            }

        service.get_daily_work_report_snapshot = snapshot  # type: ignore[method-assign]
        with patch(
            "lan_bitable_template_portal.portal_service.send_interactive_to_open_ids",
            return_value=(True, "ok", []),
        ) as sender:
            service.send_daily_work_report_to_people(
                scope="ALL",
                date=dt.date.today().isoformat(),
                recipient_open_ids=["ou_a"],
                operation_id="manual-all",
            )

        content = sender.call_args.args[0]["elements"][0]["text"]["content"]
        self.assertLess(content.index("**A楼 ·"), content.index("**E楼 ·"))
        self.assertIn("**跨楼栋/园区 · 0项**", content)
        self.assertIn("**楼栋待确认 · 0项**", content)

    def test_manual_today_report_validates_people_and_sends_each_recipient(self) -> None:
        service = self._service()
        service._critical_guard_public_base_url = lambda: ""  # type: ignore[method-assign]
        service._load_signature_people = lambda: [  # type: ignore[method-assign]
            {"name": "甲", "open_id": "ou_a"},
            {"name": "乙", "open_id": "ou_b"},
        ]
        service.get_daily_work_report_snapshot = lambda **_kwargs: {  # type: ignore[method-assign]
            "scope": "E",
            "window_start": f"{dt.date.today().isoformat()} 00:00:00",
            "window_end": f"{dt.date.today().isoformat()} 23:59:59",
            "stats": {"total": 0, "ongoing": 0, "completed": 0, "attention": 0},
            "categories": service._daily_report_groups([]),
            "tasks": [],
            "event_sla": {"stats": {}, "events": []},
            "warnings": [],
        }

        with patch(
            "lan_bitable_template_portal.portal_service.send_interactive_to_open_ids",
            return_value=(True, "ok", []),
        ) as sender:
            result = service.send_daily_work_report_to_people(
                scope="E",
                date=dt.date.today().isoformat(),
                recipient_open_ids=["ou_a", "ou_b", "ou_a"],
                operation_id="manual-operation",
            )

        self.assertEqual(result["sent_count"], 2)
        self.assertEqual(sender.call_count, 2)
        self.assertNotEqual(
            sender.call_args_list[0].kwargs["message_uuid"],
            sender.call_args_list[1].kwargs["message_uuid"],
        )

    def test_manual_today_report_rejects_historical_date(self) -> None:
        service = self._service()
        with self.assertRaisesRegex(PortalError, "今日"):
            service.send_daily_work_report_to_people(
                scope="E",
                date="2020-01-01",
                recipient_open_ids=["ou_a"],
            )

    def test_manual_today_report_reuses_scheduled_card_snapshot(self) -> None:
        service = self._service()
        today = dt.date.today().isoformat()
        scheduled_card = {"header": {"title": {"content": "E楼每日工作汇总"}}, "elements": []}
        service._state_store.put_document(
            "daily_work_report",
            today,
            {
                "reports": {"E": {"stats": {"total": 7}}},
                "cards": {"E": scheduled_card},
            },
        )
        service._load_signature_people = lambda: [  # type: ignore[method-assign]
            {"name": "甲", "open_id": "ou_a"}
        ]
        service.get_daily_work_report_snapshot = lambda **_kwargs: self.fail(  # type: ignore[method-assign]
            "stored snapshot should be reused"
        )

        with patch(
            "lan_bitable_template_portal.portal_service.send_interactive_to_open_ids",
            return_value=(True, "ok", []),
        ) as sender:
            result = service.send_daily_work_report_to_people(
                scope="E",
                date=today,
                recipient_open_ids=["ou_a"],
                operation_id="reuse-snapshot",
            )

        self.assertEqual(result["stats"]["total"], 7)
        self.assertEqual(sender.call_args.args[0], scheduled_card)


if __name__ == "__main__":
    unittest.main()

# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from typing import Any


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


TEST_DATE = "2026-07-28"


class _DailyTaskStore:
    def __init__(self) -> None:
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
            "statistic_date": TEST_DATE,
            "created_time": f"{TEST_DATE} 07:30",
            "computed_usage": 12.5,
        }
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


if __name__ == "__main__":
    unittest.main()

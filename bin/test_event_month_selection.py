# -*- coding: utf-8 -*-
"""
Regression tests for the October event-management month selection bug.

The stored monthly snapshot remains activity-based (any event that touched the
month, regardless of its occurrence date). The event management UI must count
records by *occurrence* month, while other/internal callers that use the default
monthly snapshot API (no date_field) must keep receiving the broad activity-based
data.  These tests exercise MaintenancePortalService with fake state only.

Covered:
  * default snapshot keeps broad activity-based records (unknown_time_count 0)
  * occurrence_time selection excludes Sept-began events that were updated/ended
    in October; excludes unknown/previous-year records
  * end_time selection includes Sept-began events ended in October
  * unknown occurrence timestamps go to unknown_time_count and are not treated
    as current month
  * previous years excluded
  * UTC-offset boundary normalised to Beijing time
  * invalid date_field rejected
  * stored snapshot not mutated
  * EventManagementPage monthly fetch callsites request occurrence_time
"""
from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    MaintenancePortalService,
    PortalError,
)

MONTH = "2026-10"

# ---- Shared activity-based snapshot records (stored, not occurrence-filtered) ----
RECORDS = [
    {
        "record_id": "rec_a_sept_ended_oct",
        "occurrence_time": "2026-09-15 10:00",
        "end_time": "2026-10-03 12:00",
        "building_codes": ["A"],
        "status": "已结束",
        "level": "I3",
        "source": "BMS",
        "specialty": "电气",
    },
    {
        "record_id": "rec_b_oct_processing",
        "occurrence_time": "2026-10-05 09:00",
        "end_time": "",
        "building_codes": ["B"],
        "status": "处理中",
        "level": "I2",
        "source": "BMS",
        "specialty": "暖通",
    },
    {
        "record_id": "rec_c_oct_ended_oct",
        "occurrence_time": "2026-10-01 08:00",
        "end_time": "2026-10-09 18:00",
        "building": "C楼",
        "status": "已结束",
        "level": "I3",
        "source": "BA",
        "specialty": "电气",
    },
    {
        "record_id": "rec_d_sept_progress_oct",
        "occurrence_time": "2026-09-20 10:00",
        "end_time": "",
        "building_codes": ["D"],
        "status": "处理中",
        "level": "I3",
        "source": "BMS",
        "specialty": "电力",
    },
    {
        "record_id": "rec_e_unknown_occurrence",
        "occurrence_time": "",
        "end_time": "2026-10-15 09:00",
        "building_codes": ["E"],
        "status": "已结束",
        "level": "I2",
        "source": "MES",
        "specialty": "自动化",
    },
    {
        "record_id": "rec_f_prev_year",
        "occurrence_time": "2025-10-01 10:00",
        "end_time": "2025-11-01 10:00",
        "building_codes": ["H"],
        "status": "已结束",
        "level": "I1",
        "source": "BMS",
        "specialty": "电气",
    },
    {
        # 2026-09-30 16:30Z == 2026-10-01 00:30 Beijing -> belongs to October.
        "record_id": "rec_g_utc_flip_in",
        "occurrence_time": "2026-09-30T16:30:00Z",
        "end_time": "",
        "building_codes": ["A"],
        "status": "处理中",
        "level": "I3",
        "source": "BA",
        "specialty": "暖通",
    },
    {
        # 2026-10-31 16:00-12:00 == 2026-11-01 12:00 Beijing -> NOT October.
        "record_id": "rec_h_utc_flip_out",
        "occurrence_time": "2026-10-31T16:00:00-12:00",
        "end_time": "",
        "building_codes": ["A"],
        "status": "处理中",
        "level": "I2",
        "source": "MES",
        "specialty": "给排水",
    },
    {
        # Unparseable non-empty occurrence time -> unknown, not current month.
        "record_id": "rec_i_unparseable",
        "occurrence_time": "2026-10-30 99:99",
        "end_time": "",
        "building_codes": ["H"],
        "status": "已结束",
        "level": "I3",
        "source": "BA",
        "specialty": "电气",
    },
]


class _FakeStateStore:
    """Fake _state_store exposing only get_event_month_snapshot (deep copied)."""

    def __init__(self, snapshot: dict):
        self._snapshot = copy.deepcopy(snapshot)

    def get_event_month_snapshot(self, month: str) -> dict:
        return copy.deepcopy(self._snapshot)


def _make_snapshot() -> dict:
    return {
        "exists": True,
        "month": MONTH,
        "records": copy.deepcopy(RECORDS),
        "meta": {"source": "fake"},
        "updated_at": 1780000000.0,
        "snapshot_id": "snap_2026_10",
        "last_failed": {},
    }


def _make_service(store=None):
    service = object.__new__(MaintenancePortalService)
    service._state_store = store or _FakeStateStore(_make_snapshot())

    def _config_ok():
        return ("app_token_fake", "table_id_fake", "event_notice")

    service._event_source_config = _config_ok
    return service


def _ids(records) -> list[str]:
    return [str(item.get("record_id") or "") for item in records]


class EventMonthSelectionServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = _make_service()

    def test_default_keeps_activity_based_broad_data(self):
        """Default snapshot API (no date_field) remains broad/activity-based."""
        result = self.service.get_event_monthly_snapshot(
            scope="ALL", month=MONTH, date_field=""
        )
        self.assertEqual(result["date_field"], "")
        self.assertEqual(result["unknown_time_count"], 0)
        # Every stored activity record is present, including Sept-began/Oct-end,
        # unknown-occurrence and previous-year entries.
        self.assertEqual(len(result["records"]), len(RECORDS))
        self.assertEqual(_ids(result["records"]), _ids(RECORDS))
        self.assertEqual(result["stats"]["total"], len(RECORDS))

    def test_occurrence_time_excludes_sept_oct_progress_and_unknown(self):
        result = self.service.get_event_monthly_snapshot(
            scope="ALL", month=MONTH, date_field="occurrence_time"
        )
        self.assertEqual(result["date_field"], "occurrence_time")
        selected = _ids(result["records"])
        self.assertIn("rec_b_oct_processing", selected)
        self.assertIn("rec_c_oct_ended_oct", selected)
        self.assertIn("rec_g_utc_flip_in", selected)
        # Sept-based events with Oct activity are excluded by occurrence month.
        self.assertNotIn("rec_a_sept_ended_oct", selected)
        self.assertNotIn("rec_d_sept_progress_oct", selected)
        # Feb/previous-year and unknown occurrence records excluded.
        self.assertNotIn("rec_f_prev_year", selected)
        self.assertNotIn("rec_e_unknown_occurrence", selected)
        self.assertEqual(result["stats"]["total"], 3)

    def test_unknown_occurrence_counted_not_assumed_current_month(self):
        result = self.service.get_event_monthly_snapshot(
            scope="ALL", month=MONTH, date_field="occurrence_time"
        )
        # Empty and unparseable occurrence times are unknown (counted), not October.
        self.assertEqual(result["unknown_time_count"], 2)
        self.assertNotIn("rec_e_unknown_occurrence", _ids(result["records"]))
        self.assertNotIn("rec_i_unparseable", _ids(result["records"]))

    def test_end_time_october_includes_sept_occurrence(self):
        result = self.service.get_event_monthly_snapshot(
            scope="ALL", month=MONTH, date_field="end_time"
        )
        self.assertEqual(result["date_field"], "end_time")
        selected = _ids(result["records"])
        self.assertIn("rec_a_sept_ended_oct", selected)
        self.assertIn("rec_c_oct_ended_oct", selected)
        self.assertIn("rec_e_unknown_occurrence", selected)
        self.assertNotIn("rec_d_sept_progress_oct", selected)
        self.assertNotIn("rec_b_oct_processing", selected)
        # Empty end_time is not unknown; end_time unknown only counts non-empty
        # unparseable values, of which there are none here.
        self.assertEqual(result["unknown_time_count"], 0)
        self.assertEqual(result["stats"]["total"], 3)

    def test_previous_years_excluded(self):
        occurrence = self.service.get_event_monthly_snapshot(
            scope="ALL", month=MONTH, date_field="occurrence_time"
        )
        self.assertNotIn("rec_f_prev_year", _ids(occurrence["records"]))
        ended = self.service.get_event_monthly_snapshot(
            scope="ALL", month=MONTH, date_field="end_time"
        )
        self.assertNotIn("rec_f_prev_year", _ids(ended["records"]))

    def test_utc_offset_boundary_normalized_beijing(self):
        result = self.service.get_event_monthly_snapshot(
            scope="ALL", month=MONTH, date_field="occurrence_time"
        )
        selected = _ids(result["records"])
        # UTC 2026-09-30 16:30 -> Beijing 2026-10-01 00:30 belongs to October.
        self.assertIn("rec_g_utc_flip_in", selected)
        # -12:00 2026-10-31 16:00 -> Beijing 2026-11-01 12:00 is NOT October.
        self.assertNotIn("rec_h_utc_flip_out", selected)

    def test_invalid_date_field_rejected(self):
        with self.assertRaises(PortalError):
            self.service.get_event_monthly_snapshot(
                scope="ALL", month=MONTH, date_field="progress_time"
            )

    def test_snapshot_not_mutated(self):
        original = _make_snapshot()
        store = _FakeStateStore(original)
        service = _make_service(store)
        service.get_event_monthly_snapshot(
            scope="ALL", month=MONTH, date_field="occurrence_time"
        )
        # Already copied in the fake; compare stored snapshot to a fresh deep copy
        # of the original to prove the service did not mutate stored data.
        self.assertEqual(store._snapshot["records"], copy.deepcopy(RECORDS))
        self.assertEqual(
            copy.deepcopy(store._snapshot),
            original,
        )

    def test_overview_counts_occurrence_month_and_buildings(self):
        result = self.service.get_event_monthly_overview(month=MONTH)
        self.assertEqual(result["date_field"], "occurrence_time")
        self.assertEqual(result["unknown_time_count"], 2)
        self.assertEqual(result["stats"]["total"], 3)
        building = {card["code"]: card for card in result["building_stats"]}
        self.assertEqual(building["A"]["total"], 1)
        self.assertEqual(building["B"]["total"], 1)
        self.assertEqual(building["C"]["total"], 1)
        # Sept-began / unknown / previous-year records are not counted.
        for code in ("110", "D", "E", "H"):
            self.assertEqual(building[code]["total"], 0)


class EventManagementPageStaticTests(unittest.TestCase):
    def test_assistant_catalog_exposes_the_native_month_date_field(self):
        from test_lighthouse_frontend_contracts import build_native_catalog
        catalog = build_native_catalog()
        self.assertIn("date_field", catalog.get("GET /api/events/monthly")["schema"]["query"])

    def test_monthly_fetch_calls_request_occurrence_time(self):
        vue_path = (
            BIN_DIR
            / "lan_bitable_template_portal"
            / "frontend"
            / "src"
            / "components"
            / "EventManagementPage.vue"
        )
        source = vue_path.read_text(encoding="utf-8")
        # There are exactly two /api/events/monthly fetch callsites, and both
        # explicitly request date_field=occurrence_time. The overview endpoint
        # is inherently occurrence-based on the backend.
        self.assertEqual(source.count('requestJson(`/api/events/monthly?'), 2)
        self.assertEqual(source.count('date_field: "occurrence_time"'), 2)


if __name__ == "__main__":
    unittest.main()

"""Regressions for update/end silent unique-candidate binding and daily card dates.

Background: when an update/end notice action carried no target record id, the
prepare step could silently bind the only title-matching target record.  A
leftover record from an earlier cycle (or an already finished one) therefore
produced a successful "end" job and the daily work summary displayed a
completed notice that was never sent in the current cycle.  The card also
rendered only HH:MM, so a cross-day end action looked like a same-day one.
"""
import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bin.test_notice_memory_history import _HistoryMemoryService
from lan_bitable_template_portal.portal_service import MaintenancePortalService
from lan_bitable_template_portal.state_store import LanPortalStateStore


class NoticeTargetAutoBindGuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        store = LanPortalStateStore(Path(self.tmp.name) / "state.sqlite3")
        with (
            patch(
                "lan_bitable_template_portal.portal_service.LanPortalStateStore",
                return_value=store,
            ),
            patch(
                "lan_bitable_template_portal.portal_service.get_data_file_path",
                side_effect=lambda name: str(Path(self.tmp.name) / name),
            ),
            patch.object(MaintenancePortalService, "_ensure_signature_crypto_ready"),
        ):
            self.service = _HistoryMemoryService(
                Path(self.tmp.name) / "state.sqlite3"
            )

    def tearDown(self):
        self.service._state_store.shutdown_write_worker(timeout=2)
        self.tmp.cleanup()

    def stub_lookup(self, candidates):
        self.service.lookup_notice_target_candidates = lambda **kwargs: {
            "candidates": candidates
        }

    def bind(self, **overrides):
        params = {
            "work_type": "maintenance",
            "scope": "C",
            "title": "EA118机房C楼HVDC均充维护通告(非计划性)",
            "action": "end",
        }
        params.update(overrides)
        return self.service._target_record_id_from_unique_candidate(**params)

    @staticmethod
    def candidate(record_id, status, start, end=""):
        return {
            "record_id": record_id,
            "target_record_id": record_id,
            "status": status,
            "start_time": start,
            "end_time": end,
        }

    def test_finished_unique_candidate_is_not_auto_bound(self):
        self.stub_lookup(
            [self.candidate("rec-old", "已结束", "2026-09-08 09:00", "2026-09-08 18:00")]
        )
        self.assertEqual(self.bind(), "")

    def test_stale_ongoing_unique_candidate_is_not_auto_bound(self):
        stale = dt.datetime.now() - dt.timedelta(days=45)
        self.stub_lookup(
            [self.candidate("rec-stale", "进行中", stale.strftime("%Y-%m-%d %H:%M"))]
        )
        self.assertEqual(self.bind(), "")

    def test_future_only_unique_candidate_is_not_auto_bound(self):
        future = dt.datetime.now() + dt.timedelta(days=3)
        self.stub_lookup(
            [self.candidate("rec-future", "进行中", future.strftime("%Y-%m-%d %H:%M"))]
        )
        self.assertEqual(self.bind(), "")

    def test_fresh_ongoing_unique_candidate_still_auto_bound(self):
        fresh = dt.datetime.now() - dt.timedelta(hours=3)
        self.stub_lookup(
            [self.candidate("rec-fresh", "进行中", fresh.strftime("%Y-%m-%d %H:%M"))]
        )
        self.assertEqual(self.bind(), "rec-fresh")

    def test_recent_previous_month_candidate_still_auto_bound(self):
        recent = dt.datetime.now() - dt.timedelta(days=10)
        self.stub_lookup(
            [self.candidate("rec-recent", "进行中", recent.strftime("%Y-%m-%d %H:%M"))]
        )
        self.assertEqual(self.bind(), "rec-recent")

    def test_candidate_without_dates_keeps_legacy_binding(self):
        self.stub_lookup([self.candidate("rec-undated", "进行中", "")])
        self.assertEqual(self.bind(), "rec-undated")

    def test_multiple_candidates_never_auto_bound(self):
        now = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
        self.stub_lookup(
            [
                self.candidate("rec-a", "进行中", now),
                self.candidate("rec-b", "进行中", now),
            ]
        )
        self.assertEqual(self.bind(), "")

    def test_card_shows_date_for_cross_day_end_only_notice(self):
        report = {
            "window_start": "2026-11-08 17:50",
            "window_end": "2026-11-09 17:50",
            "generated_at": "2026-11-09 17:50:00",
            "stats": {"total": 2, "completed": 2, "ongoing": 0},
            "event_sla": {
                "stats": {"on_time": 0, "late": 0, "pending": 0},
                "events": [],
            },
            "warnings": [],
            "categories": [
                {
                    "key": "notice",
                    "label": "通告",
                    "tasks": [
                        {
                            "category": "notice",
                            "type_key": "maintenance",
                            "title": "EA118机房C楼HVDC均充维护通告(非计划性)",
                            "status": "已结束",
                            "status_tone": "completed",
                            "time": "17:52",
                            "sort_time": 0.0,
                            "sort_date": "2026-11-08",
                            "action_summary": "今日结束",
                            "specialty": "电气",
                            "building": "C楼",
                        },
                        {
                            "category": "notice",
                            "type_key": "adjust",
                            "title": "EA118机房C楼制冷单元运行模式调整通告",
                            "status": "已结束",
                            "status_tone": "completed",
                            "time": "08:19",
                            "sort_time": 0.0,
                            "sort_date": "2026-11-09",
                            "action_summary": "今日开始、结束",
                            "specialty": "暖通",
                            "building": "C楼",
                        },
                    ],
                }
            ],
        }
        card = self.service.build_daily_work_report_card(
            report,
            scope_label="C楼",
            link_scope="C",
            public_base="https://example.invalid",
        )
        blob = json.dumps(card, ensure_ascii=False)
        self.assertIn("11-08 17:52结束，电气", blob)
        self.assertIn("08:19开始、结束，暖通", blob)
        self.assertNotIn("11-09 08:19", blob)


if __name__ == "__main__":
    unittest.main()

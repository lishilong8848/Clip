"""Offline unit tests for lighthouse_people_stats pure helpers.

All data is synthetic; no IO, credentials, live data or network access.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_people_stats import (
    personnel_count_request,
    summarize_staff,
)
from openclaw_service.assistant.lighthouse_sources import SCOPES

OBSERVED_AT = "2026-10-09T08:00:00"
SOURCE_URL = "https://example.invalid/signature-management?snapshot=1"

ADMIN = {"id": "admin", "scopes": list(SCOPES), "is_admin": True}
FULL = {"id": "full", "scopes": list(SCOPES)}
A_ONLY = {"id": "a", "scopes": ["A"]}
B_ONLY = {"id": "b", "scopes": ["B"]}
EMPTY_SCOPES = {"id": "none", "scopes": []}
D_ADMIN = {"id": "d", "scopes": ["D"], "is_admin": True}


def person(record_id, *, name="张三", building="A", employee_no=None, open_id=None, inactive=False):
    return {
        "record_id": record_id,
        "name": name,
        "building": building,
        "employee_no": employee_no,
        "open_id": open_id,
        "inactive": inactive,
    }


def call(rows, actor, **kw):
    return summarize_staff(
        rows, actor, observed_at=kw.get("observed_at", OBSERVED_AT), source_url=kw.get("source_url", SOURCE_URL)
    )


class PersonnelCountRequestTests(unittest.TestCase):
    def test_recognized_in_service_headcounts(self):
        for text in (
            "南通基地总共有多少人在职？",  # user example
            "A楼现在有几名员工",  # user example
            "在岗人数",
            "B楼在职员工总数是多少",
            "C楼现在有多少名人员",
            "园区共有多少人在岗",
            "110站在职多少人",
        ):
            with self.subTest(text=text):
                self.assertTrue(personnel_count_request(text), text)

    def test_rejects_generic_people_count(self):
        for text in ("电影院里有多少人", "今天有几个人", "食堂有多少人"):
            with self.subTest(text=text):
                self.assertFalse(personnel_count_request(text), text)

    def test_rejects_attendance_and_status_requests(self):
        for text in (
            "今天多少人请假",
            "A楼有多少人值班",
            "南通基地有多少人离职",
            "今天多少人上班",
            "今天多少人值班",
            "今天有多少人上班",
            "昨天多少人请假",
            "值班人员数量",
        ):
            with self.subTest(text=text):
                self.assertFalse(personnel_count_request(text), text)

    def test_rejects_gender_and_subset_qualifiers(self):
        for text in ("A楼有多少女性员工", "A楼有多少工程师", "B楼有多少部门主管"):
            with self.subTest(text=text):
                self.assertFalse(personnel_count_request(text), text)

    def test_rejects_compound_send_request(self):
        for text in ("在职多少人并把结果发给李世龙", "统计在职人数后发给组长"):
            with self.subTest(text=text):
                self.assertFalse(personnel_count_request(text), text)

    def test_rejects_how_to_counting(self):
        for text in ("如何统计在岗员工数量？", "怎么数一下现场有多少人", "统计人数的方法和步骤是什么", "请告诉我如何计算在职人数"):
            with self.subTest(text=text):
                self.assertFalse(personnel_count_request(text), text)

    def test_rejects_weather(self):
        for text in ("今天天气怎么样？", "在岗人数的多少和天气有关吗", "A楼外面的气温是多少", "是否会下雨影响在岗员工"):
            with self.subTest(text=text):
                self.assertFalse(personnel_count_request(text), text)

    def test_rejects_generic_career(self):
        for text in ("怎么找到一份好工作？", "职场发展有什么建议", "简历怎么写", "这份工作的薪酬如何"):
            with self.subTest(text=text):
                self.assertFalse(personnel_count_request(text), text)

    def test_rejects_non_people_counts(self):
        for text in ("A楼有多少台机柜？", "今天有几条通告", "设备数量是多少"):
            with self.subTest(text=text):
                self.assertFalse(personnel_count_request(text), text)


class SummarizeStaffTests(unittest.TestCase):
    def test_order_independence(self):
        rows = [
            person("r1", name="王五", building="A", open_id="ou-1"),
            person("r2", name="李四", building="B", open_id="ou-2"),
            person("r3", name="赵六", building="A", open_id="ou-3"),
        ]
        forward = call(rows, FULL)
        reverse = call(list(reversed(rows)), FULL)
        self.assertEqual(forward["total"], reverse["total"])
        self.assertEqual(forward["raw_active_records"], reverse["raw_active_records"])
        self.assertEqual(forward["per_building"], reverse["per_building"])
        self.assertEqual(forward["basis"], reverse["basis"])
        self.assertEqual(forward["total"], 3)

    def test_conflicting_duplicate_record_id_raises_in_any_order(self):
        rows = [person("r1", name="张三", open_id="ou-a"), person("r1", name="李四", open_id="ou-b")]
        for shuffled in (rows, list(reversed(rows))):
            with self.subTest(order=shuffled):
                with self.assertRaises(AssistantError):
                    call(shuffled, FULL)

    def test_exact_identical_duplicate_dedup_before_status(self):
        rows = [person("r1", open_id="ou-a"), person("r1", open_id="ou-a")]
        out = call(rows, FULL)
        self.assertEqual(out["raw_active_records"], 1)
        self.assertEqual(out["total"], 1)
        self.assertEqual(out["record_id_duplicates"], 1)

    def test_unknown_identical_duplicate_deduped(self):
        rows = [person("r1", inactive=None), person("r1", inactive=None)]
        out = call(rows, FULL)
        self.assertEqual(out["raw_active_records"], 0)
        self.assertEqual(out["unknown_status_count"], 1)
        self.assertEqual(out["record_id_duplicates"], 1)

    def test_missing_record_id_raises(self):
        rows = [{"name": "张三", "building": "A", "inactive": False}]
        with self.assertRaises(AssistantError):
            call(rows, FULL)

    def test_duplicate_open_id_shared_identity(self):
        rows = [
            person("r1", open_id="ou-shared"),
            person("r2", open_id="ou-shared"),
            person("r3", open_id="ou-diff"),
        ]
        out = call(rows, FULL)
        self.assertEqual(out["raw_active_records"], 3)
        self.assertEqual(out["total"], 2)
        self.assertEqual(out["duplicate_open_id_count"], 1)
        self.assertEqual(out["duplicate_record_count"], 1)

    def test_missing_open_id_record_id_fallback(self):
        rows = [person("r1", open_id=""), person("r2", open_id=None), person("r3", open_id="ou-x")]
        out = call(rows, FULL)
        self.assertEqual(out["raw_active_records"], 3)
        self.assertEqual(out["total"], 3)

    def test_identity_prefix_avoids_collision(self):
        # open_id "abc" on one person must not collide with record_id "abc" of another.
        rows = [person("rX", open_id="abc"), person("abc", open_id="")]
        out = call(rows, FULL)
        self.assertEqual(out["total"], 2)

    def test_scoped_exclusion_and_no_cross_building_leak(self):
        rows = [person("ra1", building="A"), person("rb1", building="B"), person("ra2", building="A"), person("rb2", building="B")]
        out = call(rows, A_ONLY)
        self.assertEqual(out["raw_active_records"], 2)
        self.assertEqual(out["per_building"], {"A": 2})
        self.assertNotIn("B", out["per_building"])
        self.assertEqual(out["basis"].count("B"), 0)

    def test_inactive_and_unknown_excluded(self):
        rows = [
            person("r1", inactive=False),
            person("r2", inactive=True),
            person("r3", inactive=None),
            person("r4", inactive="离职"),
            person("r5", inactive=1),
        ]
        out = call(rows, FULL)
        self.assertEqual(out["raw_active_records"], 1)
        self.assertEqual(out["total"], 1)
        self.assertEqual(out["unknown_status_count"], 3)

    def test_empty_source(self):
        out = call([], FULL)
        self.assertEqual(out["raw_active_records"], 0)
        self.assertEqual(out["total"], 0)
        self.assertEqual(out["per_building"], {})
        self.assertEqual(out["unknown_status_count"], 0)

    def test_missing_employee_no_warning(self):
        rows = [person("r1", employee_no="E1"), person("r2", employee_no=None), person("r3", employee_no="")]
        out = call(rows, FULL)
        self.assertEqual(out["missing_employee_no"], 2)
        self.assertTrue(any("缺少员工工号" in w for w in out["warnings"]))

    def test_ambiguous_same_name_warning_only(self):
        rows = [person("r1", name="同名", open_id="ou-1"), person("r2", name="同名", open_id="ou-2"), person("r3", name="唯一名", open_id="ou-3")]
        out = call(rows, FULL)
        self.assertEqual(out["total"], 3)
        self.assertEqual(out["ambiguous_name_duplicates"], 1)
        self.assertTrue(any("重名" in w for w in out["warnings"]))

    def test_unassigned_visible_only_for_full_scopes(self):
        unassigned = [person("r1", building="南通基地"), person("r2", building="")]
        for actor, expected in ((ADMIN, 2), (FULL, 2), (A_ONLY, 0)):
            with self.subTest(actor=actor["id"]):
                out = call(unassigned, actor)
                self.assertEqual(out["raw_active_records"], expected)
        self.assertEqual(call(unassigned, A_ONLY)["missing_building_count"], 0)

    def test_narrow_admin_scopes_exclude_unassigned(self):
        rows = [person("r1", building="D"), person("r2", building="南通基地")]
        out = call(rows, D_ADMIN)
        self.assertEqual(out["raw_active_records"], 1)
        self.assertEqual(out["per_building"], {"D": 1})
        self.assertEqual(out["missing_building_count"], 0)

    def test_multibuilding_person_via_combined_building(self):
        rows = [
            person("r1", name="多楼", open_id="ou-multi", building="A楼、B楼"),
            person("r2", name="多楼", open_id="ou-multi", building="B楼、A楼"),
        ]
        out = call(rows, FULL)
        self.assertEqual(out["raw_active_records"], 2)
        self.assertEqual(out["total"], 1)
        self.assertEqual(out["per_building"], {"A": 1, "B": 1})
        self.assertEqual(out["duplicate_record_count"], 1)

    def test_separator_building_codes_retained(self):
        rows = [
            person("r1", building="A/B楼"),
            person("r2", building="A、B楼"),
            person("r3", building="南通A楼"),
        ]
        out = call(rows, FULL)
        self.assertEqual(out["raw_active_records"], 3)
        self.assertEqual(out["per_building"], {"A": 3, "B": 2})
        # A-only actor must not see the A+B records (leak guard), only pure-A.
        out_a = call(rows, A_ONLY)
        self.assertEqual(out_a["per_building"], {"A": 1})
        self.assertEqual(out_a["raw_active_records"], 1)

    def test_110_label_in_basis(self):
        out = call([person("k1", building="110站")], FULL)
        self.assertIn("110站", out["basis"])
        self.assertNotIn("110楼", out["basis"])

    def test_no_scopes_raises(self):
        with self.assertRaises(AssistantError):
            call([person("r1")], EMPTY_SCOPES)

    def test_invalid_scope_raises(self):
        with self.assertRaises(AssistantError):
            call([person("r1")], {"id": "bad", "scopes": ["Z"]})

    def test_scope_mismatch_no_leak(self):
        rows = [person("ra1", building="A"), person("rb1", building="B")]
        out = call(rows, B_ONLY)
        self.assertEqual(out["per_building"], {"B": 1})
        self.assertNotIn("A", out["basis"])

    def test_forbidden_info_absent_from_output(self):
        rows = [
            person("r1", name="张三", employee_no="E001", open_id="ou-secret", building="A"),
            person("r2", name="李四", employee_no="E002", open_id="ou-2", building="B"),
        ]
        out = call(rows, FULL)
        blob = str(out)
        for forbidden in ("ou-secret", "ou-2", "张三", "李四", "E001", "E002", "r1", "r2"):
            self.assertNotIn(forbidden, blob)
        self.assertEqual(out["observed_at"], OBSERVED_AT)
        self.assertEqual(out["source_url"], SOURCE_URL)


if __name__ == "__main__":
    unittest.main(verbosity=2)
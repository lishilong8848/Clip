# -*- coding: utf-8 -*-
"""Isolated unit tests for lighthouse_planned_match.name matcher."""
from __future__ import annotations

import copy
import math
import unittest

from openclaw_service.assistant.lighthouse_planned_match import match_candidates


def item(record_id, title, work_type="maintenance", building="", progress="planned",
         maintenance_cycle=None):
    row = {
        "source_record_id": record_id,
        "title": title,
        "work_type": work_type,
        "building": building,
        "progress": progress,
    }
    if maintenance_cycle is not None:
        row["maintenance_cycle"] = maintenance_cycle
    return row


class DirectExactMatchTests(unittest.TestCase):
    def test_unique_exact_normalized_match_auto_selects(self):
        items = [
            item("rec_1", "EA118 A楼 A-127-2 维保通告", "maintenance", "A楼"),
            item("rec_2", "EA118 B楼 B-90 变更通告", "change", "B楼"),
            item("rec_3", "EA118 A楼 C-3 维保通告", "maintenance", "A楼"),
        ]
        result = match_candidates("EA118 A楼 A-127-2 维保通告", items)
        self.assertEqual(result["status"], "unique")
        self.assertEqual(result["selected_id"], "rec_1")
        self.assertEqual(len(result["candidates"]), 1)
        self.assertEqual(result["candidates"][0]["source_record_id"], "rec_1")
        self.assertTrue(result["candidates"][0]["exact"])
        self.assertGreaterEqual(result["candidates"][0]["score"], 70)
        self.assertIsInstance(result["candidates"][0]["match_reason"], str)
        self.assertEqual(result["candidates"][0]["title"], "EA118 A楼 A-127-2 维保通告")

    def test_shorthand_device_normalization_uniqueness(self):
        items = [
            item("rec_1", "A127 维保", "maintenance", "A楼"),
            item("rec_2", "B88 变更", "change", "B楼"),
        ]
        result = match_candidates("A-127 维保通告", items)
        self.assertEqual(result["status"], "unique")
        self.assertEqual(result["selected_id"], "rec_1")

    def test_synonym_term_normalization(self):
        items = [
            item("rec_1", "A-9 维护通告", "maintenance", "A楼"),
            item("rec_2", "A-9 变更通告", "change", "A楼"),
        ]
        result = match_candidates("A-9 维保", items)
        self.assertEqual(result["status"], "unique")
        self.assertEqual(result["selected_id"], "rec_1")

    def test_ambiguous_titles_require_choice(self):
        items = [
            item("rec_1", "A-5 维保通告", "maintenance", "A楼"),
            item("rec_2", "A楼 A-5 维保", "maintenance"),
        ]
        result = match_candidates("A-5 维保", items)
        self.assertEqual(result["status"], "choose")
        self.assertEqual(result["selected_id"], "")
        self.assertGreaterEqual(len(result["candidates"]), 2)
        self.assertTrue(all(c["exact"] for c in result["candidates"]))

    def test_bare_generic_maintenance_is_not_unique(self):
        items = [
            item("rec_1", "A-1 维保通告", "maintenance", "A楼"),
            item("rec_2", "B-2 维保通告", "maintenance", "B楼"),
        ]
        result = match_candidates("维保通告", items)
        self.assertEqual(result["status"], "no_match")
        self.assertEqual(result["selected_id"], "")
        self.assertEqual(result["candidates"], [])

    def test_bare_generic_trailing_notice_word(self):
        items = [item("rec_1", "A-1 维保", "maintenance", "A楼")]
        result = match_candidates("通告", items)
        self.assertEqual(result["status"], "no_match")
        self.assertEqual(result["selected_id"], "")
        self.assertEqual(result["candidates"], [])

    def test_chinese_prefix_exact_match(self):
        # Chinese prefix words have no \w word boundaries; they must be
        # stripped in both query and item so this is an exact unique match.
        items = [item("x", "EA118机房E楼冷水机组月度维护", "maintenance", "E楼")]
        result = match_candidates("冷水机组月度维护", items)
        self.assertEqual(result["status"], "unique")
        self.assertEqual(result["selected_id"], "x")
        self.assertTrue(result["candidates"][0]["exact"])

    def test_meaningful_noun_not_generic(self):
        # A meaningful noun phrase without device/cycle is not generic.
        items = [item("x", "电池内阻刷新", "maintenance")]
        result = match_candidates("电池内阻刷新", items)
        self.assertEqual(result["status"], "unique")
        self.assertEqual(result["selected_id"], "x")

    def test_consecutive_generic_words_still_generic(self):
        items = [item("x", "A-1 维保通告", "maintenance", "A楼")]
        result = match_candidates("维护通告", items)
        self.assertEqual(result["status"], "no_match")
        self.assertEqual(result["candidates"], [])


class NoMatchTests(unittest.TestCase):
    def test_no_reliable_match_returns_no_match(self):
        items = [
            item("rec_1", "A-1 维保通告", "maintenance", "A楼"),
            item("rec_2", "B-2 变更通告", "change", "B楼"),
        ]
        result = match_candidates("EA118 C楼 Z-9 设备调整", items)
        self.assertEqual(result["status"], "no_match")
        self.assertEqual(result["selected_id"], "")
        self.assertEqual(result["candidates"], [])
        self.assertLessEqual(len(result["recall"]), 20)


class HardConflictTests(unittest.TestCase):
    def test_different_equipment_number_rejected(self):
        items = [item("rec_2", "EA118 A楼 A-127-22 维保通告", "maintenance", "A楼")]
        result = match_candidates("EA118 A楼 A-127-2 维保通告", items)
        self.assertEqual(result["status"], "no_match")
        self.assertEqual(result["selected_id"], "")
        self.assertNotIn("rec_2", [c["source_record_id"] for c in result["candidates"]])

    def test_alphanumeric_equipment_id_hard_conflict(self):
        items = [item("rec_2", "A-445-TRB-202-4 维保通告", "maintenance", "A楼")]
        result = match_candidates("A-445-TRB-202-3 维保通告", items)
        self.assertEqual(result["status"], "no_match")
        self.assertEqual(result["selected_id"], "")
        # Even a 99 semantic score for the incompatible device is outside recall.
        with self.assertRaises(ValueError):
            match_candidates(
                "A-445-TRB-202-3 维保通告", items,
                [{"source_record_id": "rec_2", "score": 99}],
            )

    def test_hash_suffixed_equipment_id_hard_conflict(self):
        items = [item("rec_2", "A-127-22# 维保通告", "maintenance", "A楼")]
        result = match_candidates("A-127-2# 维保通告", items)
        self.assertEqual(result["status"], "no_match")
        self.assertEqual(result["selected_id"], "")
        with self.assertRaises(ValueError):
            match_candidates(
                "A-127-2# 维保通告", items,
                [{"source_record_id": "rec_2", "score": 99}],
            )

    def test_different_cycle_rejected(self):
        items = [item("rec_1", "A-3 维保通告", "maintenance", "A楼", maintenance_cycle="每季")]
        result = match_candidates("A-3 维保 每月", items)
        self.assertEqual(result["status"], "no_match")
        self.assertEqual(result["selected_id"], "")

    def test_different_notice_type_rejected(self):
        items = [item("rec_1", "A-3 维保通告", "maintenance", "A楼")]
        result = match_candidates("A-3 变更通告", items)
        self.assertEqual(result["status"], "no_match")
        self.assertNotIn("rec_1", [c["source_record_id"] for c in result["candidates"]])

    def test_different_power_direction_rejected(self):
        items = [item("rec_1", "A-6 下电通告", "power", "A楼")]
        result = match_candidates("A-6 上电", items)
        self.assertEqual(result["status"], "no_match")
        self.assertEqual(result["selected_id"], "")

    def test_missing_query_constraints_not_conflicts(self):
        items = [item("rec_1", "A-7 维保通告", "maintenance", "A楼", maintenance_cycle="每月")]
        result = match_candidates("A-7", items)
        self.assertEqual(result["status"], "unique")
        self.assertEqual(result["selected_id"], "rec_1")


class SemanticScoreTests(unittest.TestCase):
    def _semantic_items(self):
        return [
            item("rec_1", "冷水机组 冷凝器 维保", "maintenance", "E楼"),
            item("rec_2", "冷水机组 蒸发器 维保", "maintenance", "E楼"),
        ]

    def test_semantic_auto_selects_when_top_high_and_margin_large(self):
        semantic = [
            {"source_record_id": "rec_1", "score": 95},
            {"source_record_id": "rec_2", "score": 55},
        ]
        result = match_candidates("冷水机组 维保 机房 E楼", self._semantic_items(), semantic)
        self.assertEqual(result["status"], "unique")
        self.assertEqual(result["selected_id"], "rec_1")

    def test_semantic_below_margin_requires_choice(self):
        semantic = [
            {"source_record_id": "rec_1", "score": 95},
            {"source_record_id": "rec_2", "score": 90},
        ]
        result = match_candidates("冷水机组 维保 机房 E楼", self._semantic_items(), semantic)
        self.assertEqual(result["status"], "choose")
        self.assertEqual(result["selected_id"], "")
        self.assertGreaterEqual(len(result["candidates"]), 1)

    def test_partial_semantic_never_auto_selects(self):
        # rec_2 is a recalled candidate with no semantic score -> no lead.
        semantic = [{"source_record_id": "rec_1", "score": 99}]
        result = match_candidates("冷水机组 维保 机房 E楼", self._semantic_items(), semantic)
        self.assertNotEqual(result["status"], "unique")
        self.assertEqual(result["selected_id"], "")

    def test_semantic_id_outside_recall_rejected(self):
        # rec_b exists as an item but is hard-rejected (not in recall).
        items = [
            item("rec_a", "A-1 维保通告", "maintenance", "A楼"),
            item("rec_b", "B-2 变更通告", "change", "B楼"),
        ]
        with self.assertRaises(ValueError):
            match_candidates("A-1 维保", items, [{"source_record_id": "rec_b", "score": 95}])

    def test_semantic_id_eligible_but_beyond_recall_rejected(self):
        # rec_24 is eligible (in items) but ranks outside the 20-row recall;
        # that is still a foreign id and must be rejected.
        items = [
            item(f"rec_{i}", f"冷水机组 {i} 维保", "maintenance", "A楼")
            for i in range(25)
        ]
        with self.assertRaises(ValueError):
            match_candidates("冷水机组 维保 机房 A楼", items,
                             [{"source_record_id": "rec_24", "score": 95}])

    def test_foreign_semantic_id_raises(self):
        items = [item("rec_1", "A-1 维保", "maintenance", "A楼")]
        with self.assertRaises(ValueError):
            match_candidates("A-1", items, [{"source_record_id": "unknown_id", "score": 90}])

    def test_duplicate_semantic_id_raises(self):
        items = [item("rec_1", "A-1 维保", "maintenance", "A楼")]
        semantic = [
            {"source_record_id": "rec_1", "score": 90},
            {"source_record_id": "rec_1", "score": 95},
        ]
        with self.assertRaises(ValueError):
            match_candidates("A-1", items, semantic)

    def test_nonfinite_semantic_score_raises(self):
        items = [item("rec_1", "A-1 维保", "maintenance", "A楼")]
        for bad in (math.nan, math.inf, -math.inf, 101, -1):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    match_candidates("A-1", items, [{"source_record_id": "rec_1", "score": bad}])

    def test_local_fuzzy_without_semantic_never_auto_selects(self):
        items = [
            item("rec_1", "EA118 A-10 维保通告", "maintenance", "A楼"),
            item("rec_2", "EA118 A-10 设备维护方案", "maintenance", "A楼"),
        ]
        result = match_candidates("EA118 A-10 维保通告 机房巡检", items)
        self.assertEqual(result["status"], "choose")
        self.assertEqual(result["selected_id"], "")
        self.assertGreaterEqual(len(result["candidates"]), 1)
        self.assertTrue(all(c["score"] >= 70 for c in result["candidates"]))

    def test_chinese_substring_recall_no_auto_select(self):
        # Similar, non-exact Chinese noun phrasing should surface a >=70
        # candidate without auto-selecting.
        items = [item("x", "冷水机组月度维护", "maintenance", "E楼")]
        result = match_candidates("冷水机维护", items)
        self.assertNotEqual(result["status"], "unique")
        self.assertEqual(result["selected_id"], "")
        self.assertGreaterEqual(len(result["candidates"]), 1)
        target = next(c for c in result["candidates"] if c["source_record_id"] == "x")
        self.assertGreaterEqual(target["score"], 70)


class ScaleDeterminismTests(unittest.TestCase):
    def test_larger_than_200_input_complete_uniqueness(self):
        items = []
        for i in range(220):
            suffix = i if i else 9
            items.append(item(f"rec_{i}", f"EA118 A-A {suffix} 维保通告", "maintenance", "A楼"))
        items.append(item("target_rec", "EA118 A楼 X-1 维保通告", "maintenance", "A楼"))
        result = match_candidates("EA118 A楼 X-1 维保通告", items)
        self.assertEqual(result["status"], "unique")
        self.assertEqual(result["selected_id"], "target_rec")
        self.assertEqual(len(result["recall"]), min(20, len(items)))

    def test_many_exact_duplicates_preserved(self):
        items = [item(f"dup_{i}", "A-1 维保通告", "maintenance", "A楼") for i in range(25)]
        result = match_candidates("A-1 维保", items)
        self.assertEqual(result["status"], "choose")
        self.assertEqual(result["selected_id"], "")
        # Exact duplicates must not be silently capped at 20.
        self.assertEqual(len(result["candidates"]), 25)
        self.assertTrue(all(c["exact"] and c["score"] == 100 for c in result["candidates"]))

    def test_deterministic_output(self):
        items = [
            item("rec_b", "B-2 维保通告", "maintenance", "B楼"),
            item("rec_a", "A-1 维保通告", "maintenance", "A楼"),
            item("rec_c", "C-3 维保通告", "maintenance", "C楼"),
        ]
        first = match_candidates("维保 机房 A", items)
        second = match_candidates("维保 机房 A", items)
        self.assertEqual(first, second)
        self.assertEqual([c["source_record_id"] for c in first["candidates"]],
                         [c["source_record_id"] for c in second["candidates"]])
        self.assertEqual([r["source_record_id"] for r in first["recall"]],
                         [r["source_record_id"] for r in second["recall"]])

    def test_recall_never_exceeds_20(self):
        items = [item(f"rec_{i}", f"EA118 A楼 A-{i} 维保通告", "maintenance", "A楼") for i in range(120)]
        result = match_candidates("EA118 A楼 A-5 维保", items)
        self.assertLessEqual(len(result["recall"]), 20)

    def test_candidates_ordered_by_effective_score(self):
        # rec_higher_local scores higher locally than rec_higher_sem, but the
        # final semantic score inverts that order.  The margin (90-85) is below
        # the auto-select threshold so this stays a "choose".
        items = [
            item("rec_higher_local", "冷水机 维保", "maintenance", "E楼"),
            item("rec_higher_sem", "冷水机组 蒸发器 维保", "maintenance", "E楼"),
        ]
        semantic = [
            {"source_record_id": "rec_higher_local", "score": 85},
            {"source_record_id": "rec_higher_sem", "score": 90},
        ]
        result = match_candidates("冷水机组 维保 机房 E楼", items, semantic)
        self.assertEqual(result["status"], "choose")
        scored = [c["score"] for c in result["candidates"]]
        self.assertEqual(scored, sorted(scored, reverse=True))
        self.assertEqual(result["candidates"][0]["source_record_id"], "rec_higher_sem")

    def test_input_immutability(self):
        items = [
            item("rec_1", "A-1 维保通告", "maintenance", "A楼", maintenance_cycle="每月"),
            item("rec_2", "B-2 变更通告", "change", "B楼"),
        ]
        items_before = copy.deepcopy(items)
        semantic = [{"source_record_id": "rec_1", "score": 95}]
        semantic_before = copy.deepcopy(semantic)
        match_candidates("A-1 维保", items, semantic)
        self.assertEqual(items, items_before)
        self.assertEqual(semantic, semantic_before)
        for row in items_before:
            self.assertNotIn("score", row)
            self.assertNotIn("exact", row)

    def test_no_invented_ids(self):
        items = [item(None, "A-1 维保通告", "maintenance", "A楼")]
        result = match_candidates("A-1 维保", items)
        self.assertEqual(result["selected_id"], "")
        self.assertEqual(result["candidates"][0]["source_record_id"], None)


if __name__ == "__main__":
    unittest.main(verbosity=2)
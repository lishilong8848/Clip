"""Isolated scope and repair overview checks; callbacks return synthetic rows only."""
import copy
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_pending import collect_repair_overview, pending_reply
from lan_bitable_template_portal.lighthouse_scope import resolve_scopes
from lan_bitable_template_portal.lighthouse_sources import SCOPES

D_ACTOR = {"id": "fixture-d", "scopes": ["D"]}


class ScopeTests(unittest.TestCase):
    def test_d_only_unqualified(self):
        self.assertEqual(resolve_scopes(D_ACTOR, "现在还有哪些检修"), ["D"])
        self.assertEqual(resolve_scopes(D_ACTOR, "选择A还是B？110加2是多少"), ["D"])
        self.assertEqual(resolve_scopes({"scopes": list(SCOPES)}, "把这些记录全部列出来", ["D"]), ["D"])

    def test_all_authorized_unqualified(self):
        self.assertEqual(resolve_scopes({"scopes": list(SCOPES)}, "现在还有哪些检修"), sorted(SCOPES))

    def test_all_to_d_followup_replaces_previous(self):
        actor = {"scopes": list(SCOPES)}
        previous = resolve_scopes(actor, "ALL")
        self.assertEqual(resolve_scopes(actor, "只看D楼", previous), ["D"])
        self.assertEqual(resolve_scopes(actor, "还有呢", ["D"]), ["D"])
        self.assertEqual(resolve_scopes(actor, "也包括E楼", ["D"]), ["D", "E"])
        self.assertEqual(resolve_scopes(actor, "只看E楼", ["D"]), ["E"])

    def test_denied_e_before_read(self):
        for text in ("E楼检修", "ALL 和 E楼", "园区E栋", "只看E", "F楼"):
            with self.subTest(text=text), self.assertRaises(AssistantError) as caught:
                resolve_scopes(D_ACTOR, text)
            self.assertEqual(caught.exception.status, 403)

    def test_all_words_with_d_only(self):
        for text in ("ALL", "ALL的检修", "全部", "全部楼栋检修", "所有楼栋", "全部D楼检修"):
            with self.subTest(text=text):
                self.assertEqual(resolve_scopes(D_ACTOR, text, previous=[]), ["D"])

    def test_campus_and_previous_intersections_stay_empty(self):
        self.assertEqual(resolve_scopes({"scopes": ["110", "H"]}, "园区"), [])
        self.assertEqual(resolve_scopes(D_ACTOR, "继续", previous=["E"]), [])
        self.assertEqual(resolve_scopes(D_ACTOR, "继续", previous=[]), [])
        self.assertEqual(resolve_scopes({"scopes": ["A", "D", "110", "H"]}, "CAMPUS"), ["A", "D"])
        self.assertEqual(resolve_scopes(D_ACTOR, "园区"), ["D"])

    def test_authority_is_not_expanded_from_aliases_or_admin_flag(self):
        self.assertEqual(resolve_scopes({"scopes": ["D", "ALL", "CAMPUS", "X"], "is_admin": True}, "ALL"), ["D"])
        self.assertEqual(resolve_scopes({"scopes": []}, "还有呢"), [])
        self.assertEqual(resolve_scopes({"scopes": ["D", "H", "110"]}, "110站、H楼", ["D"]), ["110", "H"])


def record(identity, **fields):
    return {"record_id": identity, "title": identity, "building_codes": ["D"], **fields}


class RepairOverviewTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.calls = []
        self.page_size = 2
        self.plan_count = 2
        self.plans = [record("p1", work_type="repair", source_progress="未开始", source_event_id="same-event"),
                      record("p2", work_type="repair", source_progress="进行中", source_event_id="same-event"),
                      record("linked", work_type="repair", source_progress="未开始"),
                      record("ended-plan", work_type="repair", source_progress="已结束"),
                      record("blocked-plan", work_type="repair", source_progress="待审批")]
        self.notices = [record("n1", work_type="repair", status="进行中", source_record_id="linked", start_time="2025-01-01"),
                        record("draft", work_type="repair", status="结束", _has_unuploaded_changes=True),
                        record("ended-notice", work_type="repair", status="已结束"),
                        record("deleted-notice", work_type="repair", status="进行中", deleted_at="2026-10-01"),
                        record("maintenance", work_type="maintenance", status="进行中")]
        self.repairs = [record("r1", workflow="未开始", is_completed=False, source_event_id="same-event", target_record_id="same-notice"),
                        record("r2", workflow="维修中", is_completed=False, source_event_id="same-event", target_record_id="same-notice"),
                        record("completed", workflow="维修完成", is_completed=True),
                        record("workflow-completed", workflow="维修完成", is_completed=False)]
        self.addCleanup(patch.stopall)
        patch("socket.create_connection", side_effect=AssertionError("No real requests")).start()
        patch("socket.socket.connect", side_effect=AssertionError("No real requests")).start()

    async def invoke(self, op):
        self.calls.append(copy.deepcopy(op))
        params = op["params"]
        if op["api_id"] == "GET /api/workbench":
            self.assertEqual(params["sections"], "records,ongoing")
            self.assertEqual(params["work_type"], "repair")
            self.assertFalse(set(params) & {"month", "date", "refresh"})
            data = {"source_snapshot_ready": True, "last_loaded_at": "2026-10-01T08:00:00+08:00",
                    "source_cache_ttl_seconds": 180, "record_type_counts": {"repair": self.plan_count}}
            for key, rows in (("records", self.plans), ("ongoing", self.notices)):
                page = params.get(key + "_page", 1)
                offset = (page - 1) * self.page_size
                data[key] = copy.deepcopy(rows[offset:offset + self.page_size])
                data[key + "_pagination"] = {"total": len(rows), "page": page, "offset": offset}
        elif op["api_id"] == "GET /api/repair-management/records":
            self.assertEqual(params["state"], "active")
            self.assertEqual(params["period"], "all")
            self.assertNotIn("refresh", params)
            offset = params["offset"]
            data = {"records": copy.deepcopy(self.repairs[offset:offset + self.page_size]), "total": len(self.repairs),
                    "offset": offset, "has_more": offset + self.page_size < len(self.repairs)}
        else:
            raise AssertionError("Unexpected callback: " + op["api_id"])
        return {"ok": True, "_raw": data, "data": {"records": []}, "truncated": True}

    async def overview(self, invoke=None, actor=None):
        data = await collect_repair_overview(actor or D_ACTOR, invoke or self.invoke)
        self.assertIn("检修与维修", pending_reply(data))
        return data, {group["key"]: group for group in data["groups"]}

    async def test_native_paging_filters_and_own_ids_keep_groups_separate(self):
        for rows in (self.plans, self.notices, self.repairs):
            rows.append(copy.deepcopy(rows[0]))
            rows.append({**rows[0], "record_id": "foreign", "building_codes": ["E"]})
            rows.append({**rows[0], "record_id": "mixed", "building_codes": ["D", "E"]})
        data, groups = await self.overview()
        self.assertTrue(data["complete"])
        self.assertEqual(data["scopes"], ["D"])
        self.assertEqual([group["count"] for group in data["groups"]], [2, 2, 2])
        self.assertNotIn("total", data)
        self.assertIn("不相加", data["coverage"])
        self.assertEqual({row["id"] for row in groups["repairs"]["items"]}, {"r1", "r2"})
        self.assertEqual({row["id"] for row in groups["planned_repairs"]["items"]}, {"p1", "p2"})
        self.assertIn("仍未结束", groups["repair_notices"]["items"][1]["status"])
        self.assertEqual(groups["planned_repairs"]["source_freshness"][0]["last_loaded_at"], "2026-10-01T08:00:00+08:00")
        self.assertTrue(all(call["params"]["scope"] == "D" for call in self.calls))
        self.assertEqual([call["params"]["records_page"] for call in self.calls if "records_page" in call["params"]], [1, 2, 3, 4])
        self.assertEqual([call["params"]["ongoing_page"] for call in self.calls if "ongoing_page" in call["params"]], [1, 2, 3, 4])
        self.assertEqual([call["params"]["offset"] for call in self.calls if "offset" in call["params"]], [0, 2, 4, 6])

    async def test_counts_include_every_page_but_items_stop_at_thirty(self):
        self.plans = [record("p" + str(i), work_type="repair", source_progress="未开始") for i in range(37)]
        self.notices = [record("n" + str(i), work_type="repair", status="进行中") for i in range(37)]
        self.repairs = [record("r" + str(i), workflow="未开始", source_event_id="same-event") for i in range(37)]
        self.plan_count = 37
        data, _ = await self.overview()
        self.assertTrue(data["complete"])
        for group in data["groups"]:
            self.assertEqual((group["count"], group["known_count"], len(group["items"]), group["remaining"]), (37, 37, 30, 7))

    async def test_empty_scope_never_invokes_callback(self):
        invoke = AsyncMock(side_effect=AssertionError("No reads for empty scope"))
        data, _ = await self.overview(invoke, {"scopes": resolve_scopes({"scopes": ["H"]}, "园区")})
        self.assertTrue(data["complete"])
        self.assertEqual([group["count"] for group in data["groups"]], [0, 0, 0])
        invoke.assert_not_called()

    async def test_denied_scope_prevents_collection(self):
        invoke = AsyncMock(side_effect=AssertionError("No unauthorized reads"))
        with self.assertRaises(AssistantError) as caught:
            actor = {**D_ACTOR, "scopes": resolve_scopes(D_ACTOR, "只看E楼")}
            await collect_repair_overview(actor, invoke)
        self.assertEqual(caught.exception.status, 403)
        invoke.assert_not_called()

    async def test_all_scopes_uses_all_and_overlapping_authorized_scopes_dedup(self):
        data, _ = await self.overview(actor={"scopes": list(SCOPES)})
        self.assertTrue(data["complete"])
        self.assertTrue(all(call["params"]["scope"] == "ALL" for call in self.calls))
        self.calls.clear()
        for rows in (self.plans, self.notices, self.repairs):
            for row in rows:
                row["building_codes"] = ["A", "D"]
        data, _ = await self.overview(actor={"scopes": ["A", "D"]})
        self.assertTrue(data["complete"])
        self.assertEqual([group["count"] for group in data["groups"]], [2, 2, 2])
        self.assertEqual({call["params"]["scope"] for call in self.calls}, {"A", "D"})

    async def test_transient_failure_keeps_known_rows_and_unknown_total(self):
        async def failing(op):
            if op["params"].get("offset") == 2:
                raise OSError("private upstream details must not escape")
            return await self.invoke(op)
        data, groups = await self.overview(failing)
        self.assertFalse(data["complete"])
        self.assertEqual(groups["repairs"]["known_count"], 2)
        self.assertIsNone(groups["repairs"]["count"])
        self.assertTrue(groups["planned_repairs"]["available"])
        self.assertNotIn("private upstream", str(data))

    async def test_failed_notice_read_cannot_claim_unlinked_plans(self):
        async def failing(op):
            if "ongoing_page" in op["params"]:
                return {"ok": False, "status": 503}
            return await self.invoke(op)
        _, groups = await self.overview(failing)
        self.assertIsNone(groups["repair_notices"]["count"])
        self.assertIsNone(groups["planned_repairs"]["count"])
        self.assertEqual(groups["planned_repairs"]["known_count"], 0)
        self.assertEqual(groups["repairs"]["count"], 2)

    async def test_bad_pagination_and_unready_sources_are_unknown(self):
        for failure in ("missing_total", "repeat", "empty", "changed_total", "wrong_offset", "truncated", "unready", "timeout"):
            async def failing(op):
                result = await self.invoke(op)
                if op["api_id"] != "GET /api/repair-management/records":
                    return result
                raw, offset = result["_raw"], op["params"]["offset"]
                if failure == "missing_total":
                    raw.pop("total")
                elif failure == "repeat":
                    raw["records"] = copy.deepcopy(self.repairs[:2])
                elif failure == "empty" and offset:
                    raw["records"] = []
                elif failure == "changed_total" and offset:
                    raw["total"] += 1
                elif failure == "wrong_offset":
                    raw["offset"] += 1
                elif failure == "truncated":
                    result.pop("_raw")
                elif failure == "unready":
                    raw["source_snapshot_ready"] = False
                elif failure == "timeout":
                    raise TimeoutError()
                return result
            with self.subTest(failure=failure):
                data, groups = await self.overview(failing)
                self.assertFalse(data["complete"])
                self.assertIsNone(groups["repairs"]["count"])
                self.assertTrue(groups["repairs"]["error"])

    async def test_changed_workbench_snapshot_is_not_a_complete_count(self):
        for change_during in ("notices", "plans"):
            async def changed(op):
                result = await self.invoke(op)
                if op["api_id"] == "GET /api/workbench":
                    params = op["params"]
                    changed_page = ("records_page" in params if change_during == "plans" else params.get("ongoing_page", 1) > 1)
                    result["_raw"]["payload_version"] = "v2" if changed_page else "v1"
                return result
            with self.subTest(change_during=change_during):
                data, groups = await self.overview(changed)
                self.assertFalse(data["complete"])
                self.assertIsNone(groups["planned_repairs"]["count"])
                affected = groups["planned_repairs" if change_during == "plans" else "repair_notices"]
                self.assertIn("快照变化", affected["error"])
                self.assertEqual(groups["repairs"]["count"], 2)

    async def test_plan_native_count_mismatch_is_documented_not_invented(self):
        self.plan_count = 9
        data, groups = await self.overview()
        self.assertFalse(data["complete"])
        self.assertIsNone(groups["planned_repairs"]["count"])
        self.assertEqual(groups["planned_repairs"]["known_count"], 2)
        self.assertIn("不一致", " ".join(groups["planned_repairs"]["warnings"]))

    async def test_missing_native_state_identity_scope_and_followup_are_unknown(self):
        self.plan_count = None
        self.plans[0].pop("source_progress")
        self.repairs[0].pop("record_id")
        self.repairs[1]["followup_state_verified"] = False
        self.notices[0].pop("building_codes")
        _, groups = await self.overview()
        self.assertTrue(all(group["count"] is None for group in groups.values()))
        self.assertEqual(groups["repairs"]["known_count"], 0)
        self.assertTrue(all(group["warnings"] for group in groups.values()))


if __name__ == "__main__":
    unittest.main()

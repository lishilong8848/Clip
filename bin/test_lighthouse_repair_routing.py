"""Regression tests for collect_repair_overview(actor, invoke, *, only_keys=None).

Verifies the routing contract being added by Codex: the default reads all three
native groups, while each only_keys subset is isolated so unused groups neither
read the network nor poison the requested group's complete/count. Planned repairs
still depend on the ongoing (repair_notices) projection. Uses the same isolated
synthetic invoker fixtures as RepairOverviewTests (no real network/credentials).
"""
import copy
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.lighthouse_pending import collect_repair_overview, pending_reply

D_ACTOR = {"id": "fixture-d", "scopes": ["D"]}
ALL_GROUPS = {"planned_repairs", "repair_notices", "repairs"}


def record(identity, **fields):
    return {"record_id": identity, "title": identity, "building_codes": ["D"], **fields}


class RepairRoutingTests(unittest.IsolatedAsyncioTestCase):
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

    async def overview(self, invoke=None, *, only_keys=None):
        data = await collect_repair_overview(D_ACTOR, invoke or self.invoke, only_keys=only_keys)
        return data, {group["key"]: group for group in data["groups"]}

    async def test_default_reads_and_returns_all_three_groups(self):
        data, groups = await self.overview()
        self.assertTrue(data["complete"])
        self.assertEqual(set(groups), ALL_GROUPS)
        self.assertEqual([group["count"] for group in data["groups"]], [2, 2, 2])
        self.assertIn("检修与维修", pending_reply(data))

    async def test_only_repairs_calls_only_repair_management(self):
        data, groups = await self.overview(only_keys={"repairs"})
        self.assertTrue(data["complete"])
        self.assertEqual(set(groups), {"repairs"})
        self.assertEqual(groups["repairs"]["count"], 2)
        self.assertEqual({c["api_id"] for c in self.calls}, {"GET /api/repair-management/records"})
        self.assertEqual({row["id"] for row in groups["repairs"]["items"]}, {"r1", "r2"})

    async def test_only_notices_queries_workbench_ongoing_only(self):
        data, groups = await self.overview(only_keys={"repair_notices"})
        self.assertTrue(data["complete"])
        self.assertEqual(set(groups), {"repair_notices"})
        self.assertEqual(groups["repair_notices"]["count"], 2)
        self.assertEqual({c["api_id"] for c in self.calls}, {"GET /api/workbench"})
        self.assertTrue(all("ongoing_page" in c["params"] for c in self.calls))
        self.assertFalse(any("records_page" in c["params"] or "offset" in c["params"] for c in self.calls))

    async def test_only_planned_reads_ongoing_dependency_and_excludes_linked_started(self):
        data, groups = await self.overview(only_keys={"planned_repairs"})
        self.assertTrue(data["complete"])
        self.assertEqual(set(groups), {"planned_repairs"})
        self.assertEqual(groups["planned_repairs"]["count"], 2)
        self.assertEqual({row["id"] for row in groups["planned_repairs"]["items"]}, {"p1", "p2"})
        # Ongoing is read as a dependency even though notices are not returned.
        self.assertTrue(any("ongoing_page" in c["params"] for c in self.calls))
        self.assertTrue(any("records_page" in c["params"] for c in self.calls))
        self.assertEqual({c["api_id"] for c in self.calls}, {"GET /api/workbench"})
        self.assertFalse(any("offset" in c["params"] for c in self.calls))

    async def test_failed_ongoing_dependency_makes_planned_unknown_not_zero(self):
        async def failing(op):
            if "ongoing_page" in op["params"]:
                return {"ok": False, "status": 503}
            return await self.invoke(op)
        data, groups = await self.overview(failing, only_keys={"planned_repairs"})
        self.assertEqual(set(groups), {"planned_repairs"})
        self.assertFalse(data["complete"])
        self.assertIsNone(groups["planned_repairs"]["count"])
        self.assertNotEqual(groups["planned_repairs"]["count"], 0)
        self.assertFalse(any("offset" in c["params"] for c in self.calls))

    async def test_requested_group_not_poisoned_by_unrelated_group_errors(self):
        async def failing(op):
            if op["api_id"] != "GET /api/repair-management/records":
                raise AssertionError("Unused group must not be read")
            return await self.invoke(op)
        data, groups = await self.overview(failing, only_keys={"repairs"})
        self.assertTrue(data["complete"])
        self.assertEqual(set(groups), {"repairs"})
        self.assertEqual(groups["repairs"]["count"], 2)
        self.assertFalse(groups["repairs"]["error"])
        self.assertTrue(groups["repairs"]["available"])

    async def test_only_repairs_keeps_actor_scope_and_full_paging(self):
        self.repairs = [record("r" + str(i), workflow="未开始", is_completed=False) for i in range(7)]
        data, groups = await self.overview(only_keys={"repairs"})
        self.assertTrue(data["complete"])
        self.assertEqual(groups["repairs"]["count"], 7)
        self.assertTrue(all(c["params"]["scope"] == "D" for c in self.calls))
        self.assertEqual({c["api_id"] for c in self.calls}, {"GET /api/repair-management/records"})
        self.assertEqual([c["params"]["offset"] for c in self.calls], [0, 2, 4, 6])


if __name__ == "__main__":
    unittest.main()
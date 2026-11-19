"""Offline contracts for the five Zhihang adapters and Feishu row reconciliation."""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import zhihang_data_sources as sources
from lan_bitable_template_portal import zhihang_h_drill as h_drill


NOW = datetime(2026, 11, 13, 2, 30, tzinfo=timezone.utc)
ORIGINS = {
    "agilebpm.meta42.indc.vnet.com", "change2.meta42.indc.vnet.com", "emergencydrill.meta42.indc.vnet.com",
    "event-live.meta42.indc.vnet.com", "inspect2.meta42.indc.vnet.com",
}


def page(records, *, key="records", total=None, **meta):
    data = {key: records, "total": len(records) if total is None else total} | meta
    return {"code": "200", "success": True, "data": data}


class ZhihangSourcesTests(unittest.TestCase):
    def pull(self, kind, responder, **options):
        def respond(method, url, *, headers, cookies, timeout, follow_redirects, **kwargs):
            parsed = urlsplit(url)
            self.assertEqual(parsed.scheme, "https")
            self.assertIn(parsed.netloc, ORIGINS)
            self.assertEqual(parsed.query, "")
            self.assertFalse(follow_redirects)
            self.assertEqual(headers["Origin"], f"https://{parsed.netloc}")
            self.assertEqual(headers["Authorization"], "Bearer synthetic")
            self.assertEqual(cookies, {"token": "synthetic"})
            response = responder(method, parsed.netloc, parsed.path, kwargs)
            if isinstance(response, httpx.Response):
                return response
            return httpx.Response(200, json=response)

        with patch.object(sources.auth, "build_auth", return_value=({"Authorization": "Bearer synthetic"}, {"token": "synthetic"})):
            with patch.object(sources.auth, "request", side_effect=respond):
                result = sources.pull(kind, NOW, **options)
        self.assertEqual(tuple(result), sources.TARGET_KEYS[kind])
        for table, rows in result.items():
            for key, fields in rows.items():
                self.assertEqual(sources.identity(table, fields), key)
        return result

    def assert_error(self, kind, responder, code, **options):
        with self.assertRaises(sources.ZhihangSourceError) as context:
            self.pull(kind, responder, **options)
        self.assertEqual(context.exception.code, code)
        return context.exception

    def test_auth_errors_and_redirect_never_forward_credentials(self):
        with patch.object(sources.auth, "build_auth", side_effect=ValueError("synthetic-secret")):
            with self.assertRaises(sources.ZhihangSourceError) as context:
                sources.pull("event", NOW)
        self.assertEqual(context.exception.code, "auth_required")
        self.assertNotIn("synthetic-secret", str(context.exception))

        redirect = httpx.Response(302, headers={"Location": "https://elsewhere.invalid/"})
        self.assertEqual(self.assert_error("event", lambda *args: redirect, "auth_required").status, 302)
        forbidden = httpx.Response(403, text="synthetic-secret")
        error = self.assert_error("inspection", lambda *args: forbidden, "access_denied")
        self.assertNotIn("synthetic-secret", str(error))
        self.assert_error("event", lambda *args: httpx.Response(200, json={"code": "401"}), "auth_required")
        with patch.object(sources.time, "sleep") as sleep:
            self.assert_error("event", lambda *args: httpx.Response(429, text="rate limit"), "source_unavailable")
        self.assertEqual([args.args[0] for args in sleep.call_args_list], [1.2, 2.4])
        with self.assertRaises(sources.ZhihangSourceError) as context:
            sources.pull("https://elsewhere.invalid", NOW)
        self.assertEqual(context.exception.code, "invalid_kind")

    def test_risk_failure_reports_business_code_without_returning_response_text(self):
        error = self.assert_error(
            "risk", lambda *args: {"code": "50001", "isOk": False, "message": "fixture-private"}, "incomplete",
        )
        self.assertIn("业务码 50001", str(error))
        self.assertIn("isOk 未确认成功", str(error))
        self.assertNotIn("fixture-private", str(error))
        self.assert_error("risk", lambda *args: {"code": "403", "isOk": True}, "access_denied")
        self.assert_error("risk", lambda *args: {"code": 200, "isOk": "false"}, "incomplete")
        self.assert_error("risk", lambda *args: {"code": "SUCCESS", "isOk": True}, "incomplete")

    def test_risk_monthly_a_to_e_parents_and_original_mapping(self):
        calls = []
        checks = {}

        def respond(method, host, path, kwargs):
            self.assertEqual((method, host), ("POST", "agilebpm.meta42.indc.vnet.com"))
            payload = kwargs["json"]
            self.assertEqual((payload["currentPage"], payload["pageSize"]), (1, 50))
            calls.append(path)
            if path.endswith("/fxgl_xcydfxpcjlsjlb"):
                return httpx.Response(404)
            if path.endswith("/list_fxgl_xcydfxpcjlsjlb"):
                self.assertEqual(payload["queryData"], [])
                parents = [{"id_": f"parent-{number}", "ldyh_": str(number), "date": "2026-11-02"}
                           for number in range(5, 10)]
                parents.insert(0, {"id_": "last-month", "ldyh_": "5", "date": "2026-10-31"})
                return {"code": "SUCCESS", "isOk": True, "data": {"list": parents, "count": 6}}
            self.assertTrue(path.endswith("/list_fxgl_xcydfxpcmxsjlb"))
            selection, = payload["queryData"]
            self.assertEqual((selection["name"], selection["con"]), ("pcjlid_", "like"))
            self.assertTrue(selection["val"].startswith("parent-"))
            return {"isOk": True, "data": {"count": 1, "list": [{
                "id_": selection["val"] + "-detail", "fxxbh_": "R1", "jcxms_": "核对",
                "fxdj_": "2", "pczq_": "mo", "cqfxzt_": "w", "dqfxzt_": "unknown", "pcsj_": "2026-11-12",
            }]}}

        rows = self.pull("risk", respond, report=lambda stage, **facts: checks.setdefault(stage, {}).update(facts))["risk"]
        self.assertEqual({building: checks[f"风险排查明细 · {building}"]["已读取"]
                          for building in ("A楼", "B楼", "C楼", "D楼", "E楼")}, dict.fromkeys(
                              ("A楼", "B楼", "C楼", "D楼", "E楼"), 1))
        self.assertEqual(len(rows), 5)
        self.assertEqual(len(calls), 7)
        fields = rows['risk:["A楼","R1"]']
        self.assertEqual([fields[column] for column in ("风险等级", "排查周期", "此前风险状态", "当前风险状态")],
                         ["中", "月度", "无", "有"])
        self.assertTrue(sources.in_refresh_scope("risk", fields, NOW))
        self.assertFalse(sources.in_refresh_scope("risk", fields | {"排查时间": "2026-10-12"}, NOW))
        self.assertFalse(sources.in_refresh_scope("risk", fields | {"排查时间": "", "审核时间": "2026-11-12"}, NOW))

    def test_risk_does_not_use_previous_month_when_current_parent_missing(self):
        self.assert_error("risk", lambda *args: {"isOk": True, "data": {
            "count": 1, "list": [{"id_": "old", "ldyh_": "5", "date": "2026-10-31"}],
        }}, "incomplete")

    def test_change_shared_order_identity_and_detail_mapping(self):
        def respond(method, host, path, kwargs):
            self.assertEqual((method, host), ("POST", "change2.meta42.indc.vnet.com"))
            payload = kwargs["json"]
            if path.endswith("/listPage"):
                self.assertEqual((payload["pageNo"], payload["pageSize"], payload["operType"], payload["dc"]),
                                 (1, 15, "4", "INSTANCE-7691"))
                self.assertEqual(payload["actualEndTimeEnd"], "")
                return page([{"id": "1234", "orderCode": "C-001", "title": "工单摘要", "status": 13, "changeCategory": "2"}])
            self.assertEqual(path, "/api/change/changeDetails/getBasicInformation")
            self.assertEqual(payload, {"orderId": "1234", "userId": "18871"})
            return {"code": "200", "success": True, "data": {
                "title": "工单基础详情", "type": "2", "leadTime": "I2级提前通知", "progress": "61", "status": "0",
                "actual_start_time": "2026-11-13 10:00:00",
            }}

        rows = self.pull("change", respond)
        order, detail = (rows[key]["order:C-001"] for key in ("change_workorder", "change_basic"))
        self.assertEqual((order["状态"], order["变更类别"]), ("实施成功", "预防问题"))
        self.assertEqual((detail["工单号"], detail["变更等级"], detail["当前节点"], detail["状态"]),
                         ("C-001", "I2级", "交付机房经理审批", "草稿"))
        self.assertEqual(sources.identity("change_basic", {"工单号": [{"text": "C-001", "type": "text"}]}), "order:C-001")

    def test_change_incomplete_pages_and_failed_details_return_no_batch(self):
        item = {"id": "1234", "orderCode": "C-001"}
        self.assert_error("change", lambda *args: page([item], total=2), "incomplete")

        def respond(method, host, path, kwargs):
            if path.endswith("/listPage"):
                return page([item])
            return {"code": "200", "success": True, "data": {}}

        self.assert_error("change", respond, "incomplete")

    def test_change_fetches_all_pages_and_stops_on_missing_second_page(self):
        def respond(method, host, path, kwargs):
            payload = kwargs["json"]
            if path.endswith("/listPage"):
                number = payload["pageNo"]
                start, end = (1, 16) if number == 1 else (16, 17)
                return page([{"id": str(i), "orderCode": f"C-{i}"} for i in range(start, end)], total=16)
            return {"code": "200", "success": True, "data": {"orderCode": f"C-{payload['orderId']}"}}

        self.assertEqual(len(self.pull("change", respond)["change_basic"]), 16)

        def no_second_page(method, host, path, kwargs):
            if kwargs["json"].get("pageNo") == 2:
                return page([], total=16)
            return respond(method, host, path, kwargs)

        self.assert_error("change", no_second_page, "incomplete")

    def test_drill_backfill_joins_evaluation_events_and_sums_detail(self):
        scopes = set()

        def respond(method, host, path, kwargs):
            self.assertEqual(host, "emergencydrill.meta42.indc.vnet.com")
            if path.endswith("/queryExercisePlanList"):
                self.assertEqual(method, "POST")
                payload = kwargs["json"]
                scopes.add((payload["dcCode"], payload["exerciseMonth"]))
                self.assertEqual(payload["pageSize"], 100)
                return page([{
                    "id": "D1", "operationPlanNumber": "P01", "orderStatus": "11", "exerciseCategory": "plan",
                    "exerciseCycle": "quarter", "evaluationUserList": [{"name": "评估人甲", "jobNumber": "123"}],
                    "exerciseMonth": "Nov", "exerciseYear": 2026, "dcCode": "5.1.2.3.1.1",
                }])
            if path.endswith("/selEventMsg"):
                self.assertTrue(kwargs["json"]["drill"])
                return page([{"id": "E1", "eventNumber": "NO-01", "drillId": "plan_id_D1", "eventTitle": "评估事件",
                              "orderStatus": "11", "updateTime": "2026-11-12 10:00:00"}], key="dataList")
            self.assertEqual(method, "GET")
            self.assertTrue(path.endswith("/selExerciseEvaluationTemplate"))
            self.assertEqual(kwargs["params"], {
                "drillId": "plan_id_D1", "eventId": "E1", "jobNumber": "123", "viewRole": "evaluator",
                "endTime": "2026-11-12 10:00:00",
            })
            return {"code": "200", "data": {"evaluationContentList": [
                {"actualScore": 12, "detailList": [{"actualScore": 5}]}, {"actualScore": 8},
            ]}}

        fields = self.pull("drill", respond)["drill"]["id:D1"]
        self.assertEqual(len(scopes), 12)
        self.assertIn(("9.1.2.3.1.1", "Nov"), scopes)
        self.assertEqual({month for _, month in scopes}, {"Oct", "Nov"})
        self.assertEqual((fields["实施区域"], fields["演练类别"], fields["演练评分"], fields["评估事件状态"]),
                         ("A楼", "计划演练", "25", "完成"))

        def no_detail(method, host, path, kwargs):
            if method == "GET":
                return {"code": "200", "data": {}}
            return respond(method, host, path, kwargs)

        self.assert_error("drill", no_detail, "incomplete")

    def test_recent_window_boundaries_across_new_year(self):
        now = datetime(2027, 1, 1, 3, 0, tzinfo=timezone.utc)
        def job(day):
            return {"工单ID": 1, "计划区间": day + " 08:00-" + day + " 09:00"}
        self.assertTrue(sources.in_refresh_scope("inspection", job("2026/12/30"), now))
        self.assertFalse(sources.in_refresh_scope("inspection", job("2026/12/29"), now))
        self.assertFalse(sources.in_refresh_scope("inspection", job("2027/01/02"), now))
        plan = {"计划ID": "example", "演练年份": "2026", "演练月份": "Dec"}
        self.assertTrue(sources.in_refresh_scope("drill", plan, now))
        self.assertFalse(sources.in_refresh_scope("drill", plan | {"演练年份": "2025"}, now))
        self.assertFalse(sources.in_refresh_scope("drill", plan | {"演练月份": "Nov"}, now))
        self.assertTrue(sources.in_refresh_scope("drill", {"计划ID": "example", "计划演练时间": "2027-01-31"}, now))

    def test_event_two_30_day_filters_merging_and_historical_exclusion(self):
        queries = []

        def respond(method, host, path, kwargs):
            self.assertEqual((method, host, path), ("POST", "event-live.meta42.indc.vnet.com", "/api/event/eventOrder/selEventMsg"))
            request = kwargs["json"]
            queries.append(request)
            self.assertEqual(request["domainCodeList"], ["5", "6", "7", "8", "9"])
            recent = {"id": "E1", "eventNumber": "EV1", "creatTime": "2026-10-15 00:00:00", "orderStatus": 2,
                      "eventHighestLevelName": "I1级", "realEvent": False}
            old = {"id": "E0", "happenTime": "2026-10-14 23:59:59"}
            happened = {"id": "E2", "happenTime": "2026-11-13 23:59:59", "eventStatus": 6}
            rows = [recent, old] if request["startDate"] else [recent, happened]
            return page(rows[:request["pageBean"]["pageSize"]], key="dataList", total=len(rows))

        rows = self.pull("event", respond)["event"]
        self.assertEqual(set(rows), {"event:E1", "event:E2"})
        self.assertEqual(queries[0]["startDate"], "2026-10-15 00:00:00")
        self.assertEqual(queries[2]["endHappendDate"], "2026-11-13 23:59:59")
        self.assertEqual(rows["event:E1"]["同步关注类型"], "重要、未完成、非真实")
        self.assertTrue(sources.in_refresh_scope("event", rows["event:E1"], NOW))
        self.assertFalse(sources.in_refresh_scope("event", {"事件ID": "old", "发生时间": "2026-10-14"}, NOW))
        empty = self.assert_error("event", lambda *args: page([], key="dataList"), "empty")
        self.assertIn("按创建时间查询返回 0 条，按发生时间查询返回 0 条", str(empty))
        outside = self.assert_error("event", lambda *args: page([
            {"id": "outside", "happenTime": "2026-10-14 23:59:59"},
        ], key="dataList"), "empty")
        self.assertIn("按创建时间查询返回 1 条，按发生时间查询返回 1 条", str(outside))
        self.assert_error("event", lambda *args: {"code": "200", "data": {"dataList": []}}, "incomplete")

    def test_empty_event_preview_collects_counts_without_using_sample_rows(self):
        queries = []
        checks = {}

        def report(stage, **facts):
            checks.setdefault(stage, {}).update(facts)

        def respond(method, host, path, kwargs):
            query = kwargs["json"]
            queries.append(query)
            self.assertEqual(query["pageBean"]["pageNumber"], 1)
            self.assertEqual(query["pageBean"]["pageSize"], 1)
            if (query["startDate"] or query["startHappendDate"]) and query["orderStatus"]:
                return page([], key="dataList")
            return page([{"id": "sample-only", "orderStatus": "8"}], key="dataList",
                        total=9 if not query["domainCodeList"] else 3)

        self.assert_error("event", respond, "empty", preview=True, report=report)
        self.assertEqual(len(queries), 9)
        self.assertEqual(checks["事件诊断 · 清空日期、楼栋和状态筛选"]["源端总数"], 9)
        self.assertEqual(checks["近30天事件 · 创建时间"]["已读取"], 0)
        self.assertEqual(checks["事件近30天结果核验"]["有效记录"], 0)
        self.assertNotIn("sample-only", str(checks))
        for label, with_status, without_status in zip(("创建时间", "发生时间"), queries[5::2], queries[6::2]):
            self.assertEqual(with_status | {"orderStatus": ""}, without_status)
            self.assertEqual(checks[f"事件诊断 · A–E 楼{label}，原状态"]["源端总数"], 0)
            self.assertEqual(checks[f"事件诊断 · A–E 楼{label}，不限状态"]["源端总数"], 3)
            self.assertEqual(checks[f"事件诊断 · A–E 楼{label}，不限状态"]["样本状态编码"], "8")
        queries.clear()
        self.assert_error("event", respond, "empty")
        self.assertEqual(len(queries), 2)

    def test_event_page_length_is_verified_against_the_93_record_baseline(self):
        checks = {}
        events = [{"id": str(n), "happenTime": "2026-11-13 08:00:00"} for n in range(93)]

        def respond(method, host, path, kwargs):
            query = kwargs["json"]
            size = query["pageBean"]["pageSize"]
            number = query["pageBean"]["pageNumber"]
            self.assertEqual(query["orderStatus"], "1,2,3,4,5,6")
            self.assertNotEqual(size, 300)
            if size == 50:
                return page([], key="dataList")
            start = (number - 1) * size
            return page(events[start:start + size], key="dataList", total=93)

        rows = self.pull("event", respond, report=lambda stage, **facts: checks.setdefault(stage, {}).update(facts))
        self.assertEqual(len(rows["event"]), 93)
        for label in ("创建时间", "发生时间"):
            self.assertIn("与基准 93 条不一致", checks[f"近30天事件 · {label}每页50条"]["结果"])
            self.assertEqual(checks[f"近30天事件 · {label}每页15条"]["唯一事件数"], 93)

    def test_event_occurrence_recovers_overlapping_pages_with_date_partitions(self):
        checks = {}
        events = [{"id": str(index), "happenTime": f"2026-11-{1 + index // 8:02d} 08:00:00"}
                  for index in range(93)]
        events[-1]["happenTime"] = "2026-10-30 00:00:00"

        def fixture(skip, ignored_partition=False):
            def respond(method, host, path, kwargs):
                query = kwargs["json"]
                self.assertEqual(query["orderStatus"], "1,2,3,4,5,6")
                self.assertEqual(query["domainCodeList"], ["5", "6", "7", "8", "9"])
                size = query["pageBean"]["pageSize"]
                number = query["pageBean"]["pageNumber"]
                since, until = query["startHappendDate"], query["endHappendDate"]
                full = not since or (since.startswith("2026-10-15") and until.startswith("2026-11-13"))
                selected = events if full or ignored_partition else [
                    row for row in events if since <= row["happenTime"] <= until and row["id"] != skip]
                offset = (number - 1) * size
                rows = selected[offset:offset + size]
                if full and since and number == 2:
                    rows = [selected[0], *rows[1:]]
                return page(rows, key="dataList", total=len(selected))
            return respond

        rows = self.pull("event", fixture(None),
                         report=lambda stage, **facts: checks.setdefault(stage, {}).update(facts))
        self.assertEqual(len(rows["event"]), 93)
        self.assertEqual(checks["近30天事件 · 创建时间每页50条"]["唯一事件数"], 93)
        self.assertEqual(checks["近30天事件 · 发生时间每页50条"]["重复条数"], 1)
        self.assertEqual(checks["近30天事件 · 发生时间分段核验"]["唯一事件数"], 93)
        self.assertEqual(checks["近30天事件 · 发生时间分段核验"]["分段返回"], 94)
        error = self.assert_error("event", fixture("17"), "incomplete")
        self.assertIn("分段返回 92 条唯一事件，与基准 93 条不一致", str(error))
        error = self.assert_error("event", fixture(None, ignored_partition=True), "incomplete")
        self.assertIn("区间外事件", str(error))

    def test_h_duty_drill_only_pulls_verified_month_buildings_and_creator(self):
        row = {"id": "1", "eventNumber": "21V-HD8FOC-SJ-20261113-001", "eventTitle": "机柜单路断电",
               "happenTime": "2026-11-13 08:10:10", "domainCode": "7", "dcName": "江苏南通保税区数据中心",
               "orderStatus": 5, "creator": h_drill.CREATOR, "exerciseObjectList": [{"classGroup": "A"}],
               "eventType": "电气", "hasRecord": False, "drill": True, "practice": False}

        class Client:
            def report(self, *args, **facts):
                pass

            def request(self, endpoint, query):
                if endpoint == "h_drill_buildings":
                    return {"data": [{"domainCode": code} for code in "56789"]}
                self_test.assertEqual(endpoint, "h_drill_list")
                self_test.assertEqual(query["creator"], h_drill.CREATOR)
                self_test.assertEqual(query["domainCodeList"], list("56789"))
                self_test.assertEqual((query["drill"], query["practice"]), (True, False))
                self_test.assertEqual((query["startHappendDate"], query["endHappendDate"]),
                                      ("2026-11-01 00:00:00", "2026-11-30 23:59:59"))
                return page([row], key="dataList")

        self_test = self
        result = next(iter(h_drill.pull_h_drills(Client(), NOW, "h-login")[h_drill.KIND].values()))
        self.assertEqual((result["演练状态"], result["班组"], result["专业"], result["录制状态"]),
                         ("待评估", "A班", "电气", "未录制"))
        for domain, center in zip("56789", ("A楼", "B楼", "C楼", "D楼", "E楼")):
            mapped = h_drill.map_h_drills([row | {"domainCode": domain}], NOW)
            self.assertEqual(next(iter(mapped.values()))["楼栋"], center)
        self.assertEqual(len(h_drill.map_h_drills([row, row | {"id": "different-api-id"}], NOW)), 1)
        with self.assertRaisesRegex(sources.ZhihangSourceError, "同一演练编号存在不同内容"):
            h_drill.map_h_drills([row, row | {"id": "different-api-id", "orderStatus": 11}], NOW)
        row["creator"] = "其他账号"
        with self.assertRaisesRegex(sources.ZhihangSourceError, "其他账号"):
            h_drill.pull_h_drills(Client(), NOW, "h-login")
        row["creator"] = h_drill.CREATOR
        row["happenTime"] = "2026-10-31 23:59:59"
        with self.assertRaisesRegex(sources.ZhihangSourceError, "超出当月"):
            h_drill.pull_h_drills(Client(), NOW, "h-login")

    def test_h_duty_drill_rejects_shared_non_h_session(self):
        with patch.object(h_drill.auth, "_jwt", return_value={"UserName": "staff-login", "RealName": "其他账号"}):
            with self.assertRaisesRegex(sources.ZhihangSourceError, "未核验"):
                h_drill._credentials({"token": "fixture"}, "h-login")
        with patch.object(h_drill.auth, "_jwt", return_value={"UserName": "h-login"}), patch.object(
                h_drill.auth, "token_auth", return_value=({}, {})) as build:
            self.assertEqual(h_drill._credentials({"token": "fixture"}, "h-login"), ({}, {}))
            build.assert_called_once()

    def test_inspection_recent_three_days_and_stable_natural_identity(self):
        def respond(method, host, path, kwargs):
            self.assertEqual((method, host, path), ("GET", "inspect2.meta42.indc.vnet.com", "/api/inspect/inspection/job/list"))
            params = kwargs["params"]
            self.assertEqual((params["datacenterCode"], params["pageNum"], params["pageSize"]), ("0.5.2.2.1.1", 1, 15))
            self.assertEqual((params["planStartDatetime"], params["planEndDatetime"]),
                             ("2026-11-11 00:00:00", "2026-11-13 23:59:59"))
            return {"code": 0, "total": 1, "rows": [{
                "id": "J1", "planName": "配电巡检", "buildingName": "南通D栋", "executeCycle": "day",
                "planStartDatetime": "2026-11-13 08:00:00", "planEndDatetime": "2026-11-13 09:00:00",
                "jobExecuteStatusName": "待提交", "params": {"statMap": {"total": 5, "zc": 4, "yc": 1}},
            }]}

        key, fields = next(iter(self.pull("inspection", respond)["inspection"].items()))
        self.assertEqual((fields["楼栋"], fields["巡检结果"], fields["异常标"]), ("D楼", "总:5 正常:4 异常:1", "异常"))
        legacy = fields | {"序号": 99, "巡检人": "其他人员", "计划区间": [{"text": fields["计划区间"]}]}
        self.assertEqual(sources.identity("inspection", legacy), key)
        self.assertEqual(sources.identity("inspection", legacy | {"工单ID": 23.0}), "id:23")
        self.assertTrue(sources.in_refresh_scope("inspection", legacy, NOW))
        self.assertFalse(sources.in_refresh_scope("inspection", fields | {"计划区间": "2026/11/10 08:00-2026/11/10 09:00"}, NOW))
        self.assertFalse(sources.in_refresh_scope("inspection", fields | {"计划区间": "2026/10/31 21:00-2026/11/01 09:00"}, NOW))
        self.assert_error("inspection", lambda *args: {"code": 0, "rows": []}, "incomplete")

    def test_inspection_retries_overlapping_pages_as_a_complete_pass(self):
        for recovers in (True, False):
            with self.subTest(recovers=recovers):
                calls = []

                def respond(method, host, path, kwargs):
                    number, size = (kwargs["params"][key] for key in ("pageNum", "pageSize"))
                    calls.append((number, size))
                    if size >= 50:
                        indices = [*range(1, 16), 16 if recovers else 1]
                    else:
                        indices = range(1, 16) if number == 1 else [1]
                    rows = [{
                        "id": n, "planName": f"巡检 {n}", "buildingName": "D楼", "executeCycle": "day",
                        "planStartDatetime": "2026-11-13 08:00:00",
                        "planEndDatetime": "2026-11-13 09:00:00",
                    } for n in indices]
                    return {"code": 0, "total": 16, "rows": rows}

                if recovers:
                    self.assertEqual(len(self.pull("inspection", respond)["inspection"]), 16)
                else:
                    error = self.assert_error("inspection", respond, "incomplete")
                    self.assertIn("应有 16 张工单，去重后仅 15 张", str(error))
                self.assertEqual(calls, [(1, 15), (2, 15), (1, 50)] + ([] if recovers else [(1, 500)]))

    def test_inspection_keeps_complete_unique_batch_with_redundant_rows(self):
        checks = {}
        job = {
            "id": "J1", "planName": "配电巡检", "buildingName": "D楼", "executeCycle": "day",
            "planStartDatetime": "2026-11-13 08:00:00", "planEndDatetime": "2026-11-13 09:00:00",
        }
        rows = self.pull("inspection", lambda *args: {"code": 0, "total": 1, "rows": [job, job]},
                         report=lambda stage, **facts: checks.setdefault(stage, {}).update(facts))["inspection"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(checks["巡检首次分页"]["重复条数"], 1)
        self.assert_error("inspection", lambda *args: {
            "code": 0, "total": 1, "rows": [job, job | {"planName": "不一致"}],
        }, "incomplete")

    def test_inspection_falls_back_to_verified_building_partitions(self):
        jobs = [{
            "id": n, "planName": f"巡检 {n}", "buildingName": f"{'DE'[n % 2]}楼",
            "buildingCode": f"building-{n % 2}", "executeCycle": "day",
            "planStartDatetime": "2026-11-13 08:00:00", "planEndDatetime": "2026-11-13 09:00:00",
        } for n in range(532)]

        def fixture(skip_job, *, ignores_building=False):
            def respond(method, host, path, kwargs):
                number, size, building = (kwargs["params"][key] for key in ("pageNum", "pageSize", "buildingCode"))
                if building:
                    self.assertEqual(size, 50)
                    matched = (jobs if ignores_building else
                               [job for job in jobs if job["buildingCode"] == building and job["id"] != skip_job])
                else:
                    matched = jobs
                start = (number - 1) * size
                rows = matched[start:start + size]
                if not building and number == 2:
                    rows = [matched[0], *rows[1:]]
                return {"code": 0, "total": len(matched), "rows": rows}
            return respond

        self.assertEqual(len(self.pull("inspection", fixture(None))["inspection"]), 532)
        error = self.assert_error("inspection", fixture(1), "incomplete")
        self.assertIn("531 张唯一工单", str(error))
        error = self.assert_error("inspection", fixture(None, ignores_building=True), "incomplete")
        self.assertIn("无法确认按楼过滤生效", str(error))


if __name__ == "__main__":
    unittest.main()

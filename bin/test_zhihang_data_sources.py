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


NOW = datetime(2026, 11, 13, 2, 30, tzinfo=timezone.utc)
ORIGINS = {
    "agilebpm.meta42.indc.vnet.com", "change2.meta42.indc.vnet.com", "emergencydrill.meta42.indc.vnet.com",
    "event-live.meta42.indc.vnet.com", "inspect2.meta42.indc.vnet.com",
}


def page(records, *, key="records", total=None, **meta):
    data = {key: records, "total": len(records) if total is None else total} | meta
    return {"code": "200", "success": True, "data": data}


class ZhihangSourcesTests(unittest.TestCase):
    def pull(self, kind, responder):
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
                result = sources.pull(kind, NOW)
        self.assertEqual(tuple(result), sources.TARGET_KEYS[kind])
        for table, rows in result.items():
            for key, fields in rows.items():
                self.assertEqual(sources.identity(table, fields), key)
        return result

    def assert_error(self, kind, responder, code):
        with self.assertRaises(sources.ZhihangSourceError) as context:
            self.pull(kind, responder)
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

    def test_risk_monthly_a_to_e_parents_and_original_mapping(self):
        calls = []

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
                return {"isOk": True, "data": {"list": parents, "count": 6}}
            self.assertTrue(path.endswith("/list_fxgl_xcydfxpcmxsjlb"))
            selection, = payload["queryData"]
            self.assertEqual((selection["name"], selection["con"]), ("pcjlid_", "like"))
            self.assertTrue(selection["val"].startswith("parent-"))
            return {"isOk": True, "data": {"count": 1, "list": [{
                "id_": selection["val"] + "-detail", "fxxbh_": "R1", "jcxms_": "核对",
                "fxdj_": "2", "pczq_": "mo", "cqfxzt_": "w", "dqfxzt_": "unknown", "pcsj_": "2026-11-12",
            }]}}

        rows = self.pull("risk", respond)["risk"]
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
        self.assertEqual((fields["实施区域"], fields["演练类别"], fields["演练评分"], fields["评估事件状态"]),
                         ("A楼", "计划演练", "25", "完成"))

        def no_detail(method, host, path, kwargs):
            if method == "GET":
                return {"code": "200", "data": {}}
            return respond(method, host, path, kwargs)

        self.assert_error("drill", no_detail, "incomplete")

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
            return page([recent, old] if request["startDate"] else [recent, happened], key="dataList")

        rows = self.pull("event", respond)["event"]
        self.assertEqual(set(rows), {"event:E1", "event:E2"})
        self.assertEqual(queries[0]["startDate"], "2026-10-15 00:00:00")
        self.assertEqual(queries[1]["endHappendDate"], "2026-11-13 23:59:59")
        self.assertEqual(rows["event:E1"]["同步关注类型"], "重要、未完成、非真实")
        self.assertTrue(sources.in_refresh_scope("event", rows["event:E1"], NOW))
        self.assertFalse(sources.in_refresh_scope("event", {"事件ID": "old", "发生时间": "2026-10-14"}, NOW))
        self.assert_error("event", lambda *args: page([], key="dataList"), "empty")
        self.assert_error("event", lambda *args: {"code": "200", "data": {"dataList": []}}, "incomplete")

    def test_inspection_original_month_query_and_stable_natural_identity(self):
        def respond(method, host, path, kwargs):
            self.assertEqual((method, host, path), ("GET", "inspect2.meta42.indc.vnet.com", "/api/inspect/inspection/job/list"))
            params = kwargs["params"]
            self.assertEqual((params["datacenterCode"], params["pageNum"], params["pageSize"]), ("0.5.2.2.1.1", 1, 15))
            self.assertEqual((params["planStartDatetime"], params["planEndDatetime"]),
                             ("2026-11-01 00:00:00", "2026-11-30 23:59:59"))
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
        self.assertFalse(sources.in_refresh_scope("inspection", fields | {"计划区间": "2026/10/31 21:00-2026/11/01 09:00"}, NOW))
        self.assert_error("inspection", lambda *args: {"code": 0, "rows": []}, "incomplete")


if __name__ == "__main__":
    unittest.main()

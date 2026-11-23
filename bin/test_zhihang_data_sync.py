import datetime as dt
import unittest
from copy import deepcopy
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from lan_bitable_template_portal import zhihang_data_routes as routes
from lan_bitable_template_portal.zhihang_data_sources import ZhihangSourceError, identity
from lan_bitable_template_portal import zhihang_h_drill as h_drill
from lan_bitable_template_portal.zhihang_data_sync import NS, TZ, SyncBusy, SyncError, ZhihangBitable, ZhihangSync

TODAY = dt.datetime.now(TZ).date().isoformat()


class Store:
    def __init__(self):
        self.data = {}

    def get_document(self, ns, key):
        return deepcopy(self.data.get((ns, key)))

    def put_document(self, ns, key, payload):
        self.data[ns, key] = deepcopy(payload)

    def put_documents(self, ns, items):
        for key, payload in items.items():
            self.put_document(ns, key, payload)

    def list_documents(self, ns, *, key_prefix):
        return [{"key": key, "payload": deepcopy(payload)} for (namespace, key), payload in self.data.copy().items()
                if namespace == ns and key.startswith(key_prefix)]

    def delete_document(self, ns, key):
        self.data.pop((ns, key), None)


class Cloud:
    def __init__(self):
        self.rows = {"recManual": {"事件ID": "manual", "事件标题": "现场手填", "发生时间": TODAY + " 08:00"}}
        self.created = 0
        self.fail_after_create = False
        self.deleted = []
        self.create_batches = []
        self.update_batches = []

    def list_all(self, path="records"):
        if path == "fields":
            return [{"field_name": field, "type": 1} for field in ("事件ID", "事件标题", "发生时间")]
        return [self.get(key) for key in self.rows]

    def get(self, key):
        return {"record_id": key, "fields": deepcopy(self.rows[key])}

    def request(self, method, path, body, params):
        if path == "records/batch_create":
            self.create_batches.append((deepcopy(body["records"]), params["client_token"]))
            result = [self._create(row["fields"]) for row in body["records"]]
            if self.fail_after_create:
                raise ConnectionError("connection dropped after batch write")
            return {"records": result[::-1]}
        assert (method, path) == ("POST", "records")
        record = self._create(body["fields"])
        if self.fail_after_create:
            raise ConnectionError("connection dropped after write")
        return {"record": record}

    def _create(self, fields):
        self.created += 1
        record = f"recNew{self.created}"
        self.rows[record] = deepcopy(fields)
        return self.get(record)

    def update(self, key, fields):
        self.rows[key].update(deepcopy(fields))

    def batch_update(self, changes):
        self.update_batches.append([row["record_id"] for row in changes])
        for row in changes:
            self.update(row["record_id"], row["fields"])
        return [self.get(row["record_id"]) for row in changes]

    def delete(self, key):
        self.deleted.append(key)
        self.rows.pop(key)

    def batch_get(self, keys):
        return [self.get(key) for key in keys if key in self.rows]


class ZhihangSyncTests(unittest.TestCase):
    def setUp(self):
        self.store, self.cloud = Store(), Cloud()
        self.records = {"event:1": {"事件ID": "1", "事件标题": "事件一", "发生时间": TODAY + " 08:30"}}
        self.notifications = []
        self.worker = self.make_service()
        self.addCleanup(self.worker.close)
        self.worker.configure("event", {"confirmed": True, "notify": True})

    def make_service(self):
        worker = ZhihangSync(
            self.store, cloud=lambda table: self.cloud, pull=lambda kind, now: {"event": self.records},
            notify=lambda *args: self.notifications.append(args) or {"status": "sent"},
        )
        return worker

    def enqueue(self, mode, preview_id=""):
        run, future = self.worker.start_now("event", mode, preview_id)
        if future:
            future.result(5)
        return self.worker.detail(run["id"])

    def test_five_previews_start_together_and_duplicate_reuses_running_batch(self):
        arrived = {kind: Event() for kind in self.worker.tasks}
        release = Event()
        futures = []

        def execute(run):
            arrived[run["kind"]].set()
            self.assertTrue(release.wait(5))

        with patch.object(self.worker, "execute", side_effect=execute):
            try:
                for kind in self.worker.tasks:
                    run, future = self.worker.start_now(kind, "preview")
                    futures.append(future)
                    self.assertEqual(run["status"], "running")
                    if kind == "event":
                        event_run = run, future
                self.assertTrue(all(event.wait(2) for event in arrived.values()))
                self.assertEqual(self.worker.start_now("event", "preview"), event_run)
                with self.assertRaises(SyncBusy):
                    self.worker.enqueue("event", "scheduled", slot=TODAY + "T08:37:00+08:00")
            finally:
                release.set()
            self.assertEqual([item.result(5)["status"] for item in futures], ["success"] * 5)

    def test_old_manual_job_is_claimed_immediately_without_creating_another(self):
        old = self.worker.new_run("event", "preview", "")
        self.worker.persist(dict(old, status="queued"))
        receipt = self.enqueue("preview")
        self.assertEqual((receipt["id"], receipt["status"]), (old["id"], "success"))
        self.assertEqual(len(self.worker.history()), 1)

    def test_schedules_start_on_separate_targets_and_the_same_slot_is_reused(self):
        release = Event()
        scheduled = {}

        def execute(run):
            self.assertTrue(release.wait(5))

        with patch.object(self.worker, "execute", side_effect=execute):
            try:
                slot = TODAY + "T08:30:00+08:00"
                for kind in self.worker.tasks:
                    self.worker.configure(kind, {"confirmed": True})
                    scheduled[kind] = self.worker.enqueue(kind, "scheduled", slot=slot)
                self.assertTrue(all(run["status"] == "running" for run in scheduled.values()))
                self.assertEqual(self.worker.enqueue("risk", "scheduled", slot=slot), scheduled["risk"])
                with self.worker.lock:
                    futures = [future for _, future in self.worker.active.values()]
                self.assertEqual(len(futures), 5)
            finally:
                release.set()
            for future in futures:
                self.assertEqual(future.result(5)["status"], "success")

    def test_manual_http_returns_result_or_running_receipt_for_the_same_batch(self):
        app = FastAPI()
        controller = SimpleNamespace(
            _current_session=lambda request: {"user": {"open_id": "test-admin"}},
            _request_base_url=lambda request: str(request.base_url),
            _json_response=lambda request, session, body: JSONResponse(body),
        )
        runtime = SimpleNamespace(
            state_store=self.store, auth_manager=SimpleNamespace(is_admin=lambda session: True),
        )
        with patch.object(routes, "ZhihangSync", return_value=self.worker), patch.object(
            routes, "BrowserLogin", return_value=Mock()
        ), patch.object(routes, "allowed_modules", return_value={"accounts"}), patch.object(routes.auth, "bind_store"):
            routes.install_zhihang_data(app, controller, runtime, Mock())
            with TestClient(app) as client:
                response = client.post("/api/zhihang-data/event/preview")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["data"]["status"], "success")
                release = Event()

                def execute(run):
                    self.assertTrue(release.wait(5))

                with patch.object(routes, "MANUAL_RESULT_WAIT", 0), patch.object(self.worker, "execute", side_effect=execute):
                    try:
                        response = client.post("/api/zhihang-data/risk/preview")
                        receipt = response.json()["data"]
                        self.assertEqual(receipt["status"], "running")
                        with self.worker.lock:
                            _, future = self.worker.active[self.worker.tasks["risk"][2][0]]
                        self.assertEqual(client.post("/api/zhihang-data/risk/preview").json()["data"]["id"], receipt["id"])
                    finally:
                        release.set()
                    self.assertEqual(future.result(5)["status"], "success")
                    detail = client.get("/api/zhihang-data/runs/" + receipt["id"]).json()["data"]
                    self.assertEqual(detail["status"], "success")

    def test_overview_pages_all_retained_runs_without_changing_task_summary(self):
        self.assertEqual(self.worker.overview(history_page=9)["history_pagination"],
                         {"page": 1, "page_size": 10, "total": 0})
        for index in range(54, -1, -1):
            self.worker.persist({"id": f"{index:032x}", "kind": "event", "mode": "preview",
                                 "status": "success", "created_at": TODAY + f"T08:{index:02d}:00+08:00"})
        first = self.worker.overview(history_page=1)
        last = self.worker.overview(history_page=7)
        self.assertEqual(first["history"][0]["id"], f"{54:032x}")
        self.assertEqual(len(first["history"]), 10)
        self.assertEqual(last["history_pagination"], {"page": 6, "page_size": 10, "total": 55})
        self.assertEqual([row["id"] for row in last["history"]], [f"{index:032x}" for index in range(4, -1, -1)])
        self.assertEqual(next(task for task in last["tasks"] if task["kind"] == "event")["last_preview"], first["history"][0])
        self.assertEqual(len(self.worker.overview()["history"]), 50)

    def test_preview_is_read_only_and_confirmed_sync_uses_preview(self):
        preview = self.enqueue("preview")
        self.assertEqual(preview["counts"]["create"], 1)
        self.assertEqual(preview["counts"]["unmanaged"], 1)
        self.assertEqual(self.cloud.created, 0)
        self.assertEqual(self.notifications, [])
        self.records.clear()
        result = self.enqueue("sync", preview["id"])
        self.assertEqual(result["status"], "success")
        self.assertEqual(self.cloud.created, 1)
        self.assertEqual(len(self.notifications), 1)
        self.assertEqual(self.worker.enqueue("event", "sync", preview["id"])["id"], result["id"])

    def test_uncertain_create_resumes_without_claiming_ownership(self):
        preview = self.enqueue("preview")
        self.cloud.fail_after_create = True
        result = self.enqueue("sync", preview["id"])
        self.assertEqual(result["status"], "pending_verify")
        run = self.store.get_document(NS, "run:" + result["id"])
        self.store.put_document(NS, "run:" + result["id"], dict(run, next_attempt=""))
        restarted = self.make_service()
        self.addCleanup(restarted.close)
        for future in restarted.kick():
            future.result(5)
        self.assertEqual(restarted.detail(result["id"])["status"], "success")
        self.assertEqual(self.cloud.created, 1)
        self.cloud.fail_after_create = False
        self.records = {"event:2": {"事件ID": "2", "事件标题": "事件二", "发生时间": TODAY + " 08:30"}}
        preview = self.enqueue("preview")
        self.assertEqual(preview["counts"]["delete"], 0)
        result = self.enqueue("sync", preview["id"])
        self.assertEqual(result["status"], "success")
        self.assertEqual(self.cloud.deleted, [])
        self.assertIn("recManual", self.cloud.rows)

    def test_multirow_create_and_update_use_batches_with_keyed_readback(self):
        self.records = {f"event:{i}": {"事件ID": str(i), "事件标题": f"测试{i}", "发生时间": TODAY + " 08:30"}
                        for i in range(205)}
        preview = self.enqueue("preview")
        self.assertEqual(self.enqueue("sync", preview["id"])["status"], "success")
        self.assertEqual([len(rows) for rows, _ in self.cloud.create_batches], [100, 100, 5])
        self.assertEqual(len(self.worker.managed("event")), 205)
        for fields in self.records.values():
            fields["事件标题"] += "更新"
        preview = self.enqueue("preview")
        result = self.enqueue("sync", preview["id"])
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["checks"]["同步飞书原表"]["已核验更新"], 205)
        self.assertEqual([len(ids) for ids in self.cloud.update_batches], [100, 100, 5])
        self.assertEqual(self.cloud.created, 205)

    def test_scheduled_risk_and_drill_ignore_feishu_read_format_differences(self):
        for kind, key_field, status_field in (("risk", "风险ID", "排查状态"), ("drill", "计划ID", "执行状态")):
            with self.subTest(kind=kind):
                cloud = Cloud()
                before = {key_field: "fixture", status_field: "待处理"}
                target = before | {status_field: "已完成"}
                cloud.rows = {"recKnown": before | {"公式编码": ["编码"], "备注": "人工数据"}}
                worker = ZhihangSync(Store(), cloud=lambda _: cloud,
                                     pull=lambda *_: {kind: {identity(kind, target): target}})
                self.addCleanup(worker.close)
                worker.configure(kind, {"confirmed": True})
                list_all = cloud.list_all

                def list_rows(path="records"):
                    return ([{"field_name": name, "type": 1} for name in before]
                            if path == "fields" else list_all(path))

                def read_batch(keys):
                    return [{"record_id": key, "fields": cloud.rows[key] | {
                        status_field: [{"text": cloud.rows[key][status_field], "type": "text"}],
                        "公式编码": {"value": ["编码"]},
                    }} for key in keys]

                with (
                    patch.object(cloud, "list_all", side_effect=list_rows),
                    patch.object(cloud, "batch_get", side_effect=read_batch),
                    patch.object(cloud, "batch_update", wraps=cloud.batch_update) as write,
                ):
                    run, future = worker.start_now(kind, "scheduled")
                    self.assertEqual(future.result(5)["status"], "success", worker.detail(run["id"]))
                    write.assert_called_once_with([{"record_id": "recKnown", "fields": {status_field: "已完成"}}])
                self.assertEqual(cloud.rows["recKnown"]["备注"], "人工数据")

    def test_concurrent_edit_waits_without_writing_and_retries_original_batch(self):
        record_id = "recExisting"
        before = self.records["event:1"] | {"事件标题": "原记录"}
        self.cloud.rows[record_id] = before.copy()

        def edit_before_read(ids):
            self.cloud.rows[record_id]["事件标题"] = "人工修改"
            return [self.cloud.get(key) for key in ids]

        with patch.object(self.cloud, "batch_get", side_effect=edit_before_read):
            receipt, future = self.worker.start_now("event", "scheduled")
            self.assertEqual(future.result(5)["status"], "pending_verify")
        run_id = receipt["id"]
        self.assertEqual(self.cloud.rows[record_id]["事件标题"], "人工修改")
        self.assertEqual(self.cloud.update_batches, [])
        self.assertEqual(self.worker.detail(run_id)["checks"]["同步飞书原表"]["冲突字段"], "事件标题")

        self.cloud.rows[record_id] = before.copy()
        run = self.store.get_document(NS, "run:" + run_id)
        self.store.put_document(NS, "run:" + run_id, dict(run, next_attempt=""))
        self.assertEqual([f.result(5)["status"] for f in self.worker.kick()], ["success"])
        self.assertEqual([row["id"] for row in self.worker.history()], [run_id])
        self.assertEqual(self.cloud.rows[record_id], self.records["event:1"])
        self.assertEqual(self.cloud.update_batches, [[record_id]])
        self.assertEqual(self.cloud.created, 0)

    def test_partial_create_visibility_waits_for_original_receipt(self):
        self.records["event:2"] = self.records["event:1"] | {"事件ID": "2"}
        preview = self.enqueue("preview")
        self.cloud.fail_after_create = True
        interrupted = self.enqueue("sync", preview["id"])
        self.assertEqual(interrupted["status"], "pending_verify")
        delayed = self.cloud.rows.pop("recNew2")
        self.cloud.fail_after_create = False
        self.assertEqual(self.enqueue("sync", preview["id"])["status"], "pending_verify")
        self.cloud.rows["recNew2"] = delayed
        recovered = self.enqueue("sync", preview["id"])
        self.assertEqual((recovered["id"], recovered["status"]), (interrupted["id"], "success"))
        self.assertEqual(len(self.cloud.create_batches), 1)
        self.assertEqual(self.worker.managed("event"), {})
        self.assertIn("recNew1", self.cloud.rows)

    def test_cleans_only_owned_rows_with_verified_creation_receipt(self):
        first = self.enqueue("preview")
        self.assertEqual(self.enqueue("sync", first["id"])["status"], "success")
        self.records = {"event:2": {"事件ID": "2", "事件标题": "事件二", "发生时间": TODAY + " 08:30"}}
        preview = self.enqueue("preview")
        self.assertEqual(preview["counts"]["delete"], 1)
        self.assertEqual(self.enqueue("sync", preview["id"])["status"], "success")
        self.assertEqual(self.cloud.deleted, ["recNew1"])
        self.assertIn("recManual", self.cloud.rows)

    def test_preserves_a_manual_column_changed_during_update_on_later_cleanup(self):
        first = self.enqueue("preview")
        self.assertEqual(self.enqueue("sync", first["id"])["status"], "success")
        self.records["event:1"]["事件标题"] = "更新后的事件标题"

        def read_batch(ids):
            self.cloud.rows["recNew1"]["备注"] = "现场手填"
            return [self.cloud.get(key) for key in ids]

        with patch.object(self.cloud, "batch_get", side_effect=read_batch):
            _, future = self.worker.start_now("event", "scheduled")
            self.assertEqual(future.result(5)["status"], "success")
        self.assertEqual(self.cloud.rows["recNew1"]["事件标题"], "更新后的事件标题")
        self.assertEqual(self.cloud.rows["recNew1"]["备注"], "现场手填")

        self.records = {"event:2": {"事件ID": "2", "事件标题": "事件二", "发生时间": TODAY + " 08:30"}}
        preview = self.enqueue("preview")
        self.assertEqual(preview["counts"]["delete"], 0)
        self.assertEqual(self.enqueue("sync", preview["id"])["status"], "success")
        self.assertIn("recNew1", self.cloud.rows)

    def test_empty_data_and_disabled_settings_leave_cloud_unchanged(self):
        self.records.clear()
        self.assertEqual(self.enqueue("preview")["status"], "failed")
        self.assertEqual(self.cloud.created, 0)
        self.worker.configure("event", {"confirmed": False})
        with self.assertRaises(SyncError):
            self.worker.configure("event", {"enabled": True})
        with self.assertRaises(SyncError):
            self.worker.enqueue("event", "sync", "unconfirmed")

    def test_empty_event_diagnostics_remain_on_failed_receipt_and_cannot_sync(self):
        self.worker.pull = None

        def pull(kind, now, *, report, preview):
            self.assertTrue(preview)
            report("事件近30天结果核验", 有效记录=0)
            raise ZhihangSourceError("empty", "没有可同步记录")

        with patch.object(self.worker, "_pull", side_effect=pull), patch.object(
                self.worker, "cloud", side_effect=AssertionError("empty result reached Bitable")):
            result = self.enqueue("preview")
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["stage"], "事件近30天结果核验")
        self.assertEqual(result["checks"], {"事件近30天结果核验": {"有效记录": 0}})
        self.assertEqual(result["rows"], [])
        with self.assertRaises(SyncError):
            self.worker.enqueue("event", "sync", result["id"])

    def test_inspection_preview_accepts_equivalent_legacy_rows_and_preserves_metadata(self):
        old = {"楼栋": "A楼", "工单名称": "电气日常巡检", "巡检类型": "日常巡检",
               "计划区间": TODAY + " 08:00-" + TODAY + " 09:00", "工单状态": "待巡检"}
        current = old | {"工单状态": "正常完成", "序号": 1}
        self.cloud.rows = {
            "first": old | {"序号": 4, "自动编码": "one"},
            "second": old | {"序号": 8, "自动编码": "two"},
        }
        original_list_all = self.cloud.list_all
        self.cloud.list_all = lambda path="records": (
            [{"field_name": field, "type": 2 if field == "序号" else 1} for field in current]
            if path == "fields" else original_list_all(path))
        self.worker.pull = lambda kind, now: {"inspection": {identity("inspection", current): current}}
        self.worker.configure("inspection", {"confirmed": True})

        def run(mode, preview_id=""):
            started, future = self.worker.start_now("inspection", mode, preview_id)
            if future:
                future.result(5)
            return self.worker.detail(started["id"])

        snapshot = deepcopy(self.cloud.rows)
        preview = run("preview")
        self.assertEqual(preview["status"], "success")
        self.assertEqual(preview["counts"]["update"], 2)
        self.assertEqual(preview["checks"]["核对飞书业务键"]["业务相同的额外原表记录"], 1)
        self.assertEqual(self.cloud.rows, snapshot)

        original_update = self.cloud.update

        def fail_second(record_id, fields):
            if record_id == "second":
                raise ConnectionError("connection dropped before second write")
            original_update(record_id, fields)

        with patch.object(self.cloud, "update", side_effect=fail_second):
            interrupted = run("sync", preview["id"])
        self.assertEqual(interrupted["status"], "pending_verify")
        saved = self.store.get_document(NS, "run:" + interrupted["id"])
        self.store.put_document(NS, "run:" + interrupted["id"], dict(saved, next_attempt=""))
        restarted = self.make_service()
        self.addCleanup(restarted.close)
        restarted.pull = self.worker.pull
        with patch.object(self.cloud, "update", wraps=original_update) as update:
            for future in restarted.kick():
                future.result(5)
        self.assertEqual(restarted.detail(interrupted["id"])["status"], "success")
        update.assert_called_once()
        self.assertEqual(update.call_args.args[0], "second")
        self.assertEqual(self.cloud.created, 0)
        self.assertEqual(self.cloud.deleted, [])
        self.assertEqual(self.cloud.rows, {
            record_id: row | {"工单状态": "正常完成"} for record_id, row in snapshot.items()})
        self.assertEqual(run("preview")["counts"]["update"], 0)

        self.cloud.rows["second"]["工单状态"] = "人工核对"
        snapshot = deepcopy(self.cloud.rows)
        conflict = run("preview")
        self.assertEqual(conflict["status"], "failed")
        self.assertEqual(conflict["checks"]["核对飞书业务键"]["同键原表记录"], 2)
        self.assertEqual(self.cloud.rows, snapshot)

    def test_bitable_pagination_refuses_a_truncated_batch(self):
        client = ZhihangBitable()
        pages = iter([{"items": [{"record_id": "rec1"}], "has_more": True, "total": 3, "page_token": "next"},
                      {"items": [{"record_id": "rec2"}], "has_more": False, "total": 3}])
        client.request = lambda *args, **kwargs: next(pages)
        with self.assertRaisesRegex(SyncError, "分页未读全"):
            client.list_all()

    def test_h_duty_drill_preview_never_creates_schema_and_sync_is_repeatable(self):
        class DrillTable(Cloud):
            def __init__(self):
                super().__init__()
                self.rows = {"empty": {}, "old": {"演练编号": "old", "文本": "历史资料"}}
                self.schema = {"文本": 1}

            def list_all(self, path="records"):
                if path == "fields":
                    return [{"field_name": name, "type": field_type} for name, field_type in self.schema.items()]
                return super().list_all(path)

            def request(self, method, path, body, params=None):
                if path == "fields":
                    self.schema[body["field_name"]] = body["type"]
                    return {}
                return super().request(method, path, body, params)

        cloud = DrillTable()
        current = {"演练编号": "new-drill", "文本": "机柜单路断电", "创建人": h_drill.CREATOR}
        worker = h_drill.HDutyDrillSync(self.store, cloud=lambda table: cloud,
                                       pull=lambda kind, now: {kind: {identity(kind, current): current}})
        self.addCleanup(worker.close)
        worker._session_revision = lambda: "fixture"

        def run(mode, preview_id=""):
            started = worker.enqueue(h_drill.KIND, mode, preview_id=preview_id)
            with worker.lock:
                active = worker.active.get(h_drill.TABLE)
            if active:
                active[1].result(5)
            return worker.detail(started["id"])

        preview = run("preview")
        self.assertEqual(preview["status"], "success")
        self.assertEqual(cloud.schema, {"文本": 1})
        self.assertEqual(cloud.created, 0)
        self.assertEqual(run("sync", preview["id"])["status"], "success")
        self.assertEqual(cloud.created, 1)
        self.assertEqual(cloud.rows["old"], {"演练编号": "old", "文本": "历史资料"})
        self.assertEqual(cloud.rows["empty"], {})
        self.assertEqual(run("preview")["counts"]["create"], 0)
        self.assertEqual(len(worker.settings(h_drill.KIND)["times"]), 48)
        self.assertFalse(worker.settings(h_drill.KIND)["enabled"])

    def test_h_duty_login_does_not_replace_shared_zhihang_session(self):
        worker = h_drill.HDutyDrillSync(self.store)
        self.addCleanup(worker.close)
        shared = h_drill.auth.DEFAULTS | {"token": "shared-token"}
        self.store.put_document("plan_convergence", "credentials", shared)
        with patch.object(h_drill, "_credentials", return_value=({}, {})):
            worker.save_login("h-token")
        revision = worker.config_revision()
        self.assertEqual(self.store.get_document("plan_convergence", "credentials"), shared)
        self.store.put_document("plan_convergence", "credentials", shared | {"token": "new-shared-token"})
        self.assertEqual(worker._config()["token"], "h-token")
        self.assertEqual(worker.config_revision(), revision)

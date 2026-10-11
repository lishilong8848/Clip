"""Isolated directory tests; no network or production records."""
import copy
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

sys.path.insert(0, str(Path(__file__).parent))
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from lan_bitable_template_portal.link_directory import (
    APP_TOKEN, TABLE_ID, LinkDirectory, LinkRemote, from_record, identity, normalize_url, to_fields, validate_item,
)
from lan_bitable_template_portal.link_directory_routes import install_link_directory_routes
from lan_bitable_template_portal.portal_service import PortalError, PortalConflictError, PortalNotFoundError


class Store:
    def __init__(self): self.values = {}
    def get_document(self, namespace, key): return copy.deepcopy(self.values.get((namespace, key)))
    def put_document(self, namespace, key, value): self.values[(namespace, key)] = copy.deepcopy(value)


def item(**kwargs):
    return {"name": "维修项目", "url": "https://vnet.feishu.cn/base/testBase?table=tblTest", "category": "维修",
            "purpose": "跟进查询", "sort": 1, **kwargs}


class Remote:
    def __init__(self):
        self.rows = {}; self.reads = 0; self.creates = 0; self.fail = False; self.lose_create = False
        self.update_orders_calls = []; self.batch_fail = False; self.batch_partial = False; self.lost_update = False
    def list_all(self):
        self.reads += 1
        if self.fail: raise RuntimeError("offline")
        return [{"record_id": key, "fields": to_fields(value)} for key, value in self.rows.items()]
    def create(self, value, request_id):
        self.creates += 1
        key = "rec" + str(len(self.rows) + 1)
        self.rows[key] = copy.deepcopy(value)
        if self.lose_create: self.lose_create = False; raise TimeoutError("lost response")
        return key
    def update(self, key, value): self.rows[key] = copy.deepcopy(value)
    def delete(self, key): del self.rows[key]
    def update_orders(self, updates):
        order = list(updates.items())
        self.update_orders_calls.append(copy.deepcopy(updates))
        if self.batch_fail:
            raise RuntimeError("offline")
        if self.batch_partial:
            for key, sort in order[:max(1, len(order) // 2)]:
                self.rows[key] = {**self.rows[key], "sort": sort}
            raise RuntimeError("partial success")
        if self.lost_update:
            for key, sort in order:
                self.rows[key] = {**self.rows[key], "sort": sort}
            raise TimeoutError("lost response")
        for key, sort in order:
            self.rows[key] = {**self.rows[key], "sort": sort}


class DirectoryTests(unittest.TestCase):
    def setUp(self):
        self.store, self.remote = Store(), Remote()
        self.service = LinkDirectory(self.store, self.remote)

    def test_validation_and_view_retention(self):
        url = normalize_url("https://vnet.feishu.cn/wiki/abc?view=vew123&table=tblTest&extra=1")
        self.assertEqual(url, "https://vnet.feishu.cn/wiki/abc?table=tblTest&view=vew123")
        self.assertEqual(identity(url), identity(item()["url"]))
        for url in ["javascript:alert(1)", "file:///C:/test", "https://user:password@example.com/path", "https://example.com:999999/test",
                    "http://vnet.feishu.cn/base/a?table=tbl1", "https://vnet.feishu.cn/base/a", "https://vnet.feishu.cn:999/base/a?table=tbl1",
                    "https://u:p@vnet.feishu.cn/base/a?table=tbl1", "https://vnet.feishu.cn/base/a?table=tbl1&table=tbl2"]:
            with self.subTest(url=url), self.assertRaises(PortalError): normalize_url(url)

    def test_websites_keep_ports_and_query_and_have_distinct_identity(self):
        url = "https://www.sm.sjhl.online:3001/training-certification-management?module=certificate"
        self.assertEqual(normalize_url(url), url)
        self.assertTrue(identity(url).startswith("web:"))
        self.assertNotEqual(identity(url), identity(url.replace("certificate", "training")))
        self.assertNotEqual(identity("https://www.sm.sjhl.online:3001/"), identity("https://www.sm.sjhl.online:8787/"))

    def test_field_validation(self):
        for bad in [{"name": ""}, {"category": "a" * 81}, {"purpose": "a" * 2001}, {"sort": True}, {"sort": -1}, {"sort": 1.1}, {"unknown": 1}]:
            with self.subTest(bad=bad), self.assertRaises(PortalError): validate_item(item(**bad))

    def test_cloud_field_roundtrip(self):
        fields = to_fields(item())
        fields["表名"] = [{"type": "text", "text": "维修项目"}]
        self.assertEqual(from_record({"record_id": "rec1", "fields": fields}), {**item(), "id": "rec1"})

    def test_cache_and_restart_are_local_reads(self):
        self.remote.rows["rec1"] = item()
        self.assertEqual(len(self.service.read()["items"]), 1)
        self.service.read()
        restored = LinkDirectory(self.store, self.remote)
        restored.read()
        self.assertEqual(self.remote.reads, 1)

    def test_failed_refresh_keeps_previous_snapshot(self):
        self.remote.rows["rec1"] = item()
        before = self.service.read()
        self.remote.fail = True
        with self.assertRaises(RuntimeError): self.service.read(True)
        self.assertEqual(self.service.read()["items"], before["items"])

    def test_invalid_cloud_page_does_not_replace_snapshot(self):
        self.remote.rows["rec1"] = item()
        self.service.read()
        with patch.object(self.remote, "list_all", return_value=[{"record_id": "rec2", "fields": {"表名": "损坏链接"}}]):
            with self.assertRaises(PortalError): self.service.read(True)
        self.assertEqual(self.service.read()["items"][0]["id"], "rec1")

    def test_empty_cloud_rows_are_not_navigation_entries(self):
        with patch.object(self.remote, "list_all", return_value=[{"record_id": "recEmpty", "fields": {}}]):
            self.assertEqual(self.service.read()["items"], [])

    def test_complete_crud_and_deleted_items_do_not_reappear(self):
        created = self.service.save({**item(), "request_id": str(uuid.uuid4())})["item"]
        self.assertEqual(self.remote.creates, 1)
        updated = self.service.save(item(name="新名称"), created["id"])["item"]
        self.assertEqual(updated["name"], "新名称")
        self.assertEqual(self.service.read()["items"], [updated])
        self.service.delete(created["id"])
        self.assertEqual(LinkDirectory(self.store, self.remote).read(True)["items"], [])
        self.assertTrue(self.service.delete(created["id"])["deleted"])

    def test_lost_create_response_retry_does_not_duplicate(self):
        data = {**item(), "request_id": str(uuid.uuid4())}
        self.remote.lose_create = True
        with self.assertRaises(TimeoutError): self.service.save(data)
        self.assertEqual(self.service.save(data)["item"]["id"], "rec1")
        self.assertEqual(self.remote.creates, 1)

    def test_concurrent_creates_for_same_table(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda _: self.service.save({**item(), "request_id": str(uuid.uuid4())}), range(6)))
        self.assertEqual({r["item"]["id"] for r in results}, {"rec1"})
        self.assertEqual(self.remote.creates, 1)

    def test_duplicate_edit_is_rejected(self):
        self.remote.rows = {"rec1": item(), "rec2": item(url="https://vnet.feishu.cn/base/test?table=tblOther")}
        with self.assertRaises(PortalConflictError): self.service.save(item(), "rec2")

    def test_remote_targets_only_directory_and_preserves_token(self):
        client = SimpleNamespace(request_json=lambda *args, **kwargs: {"code": 0, "data": {"record": {"record_id": "rec1"}}})
        service = SimpleNamespace(_http_client=client, _auth_headers=lambda: {"Authorization": "test"})
        remote = LinkRemote(service)
        with patch.object(client, "request_json", wraps=client.request_json) as request, patch(
                "lan_bitable_template_portal.portal_service.external_real_write_guard", return_value={"real_write_allowed": True}):
            token = str(uuid.uuid4())
            remote.create(item(), token)
            self.assertEqual(request.call_args.args[1], f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records")
            first_token = request.call_args.kwargs["params"]["client_token"]
            self.assertEqual(uuid.UUID(first_token).version, 4)
            remote.create(item(), token)
            self.assertEqual(request.call_args.kwargs["params"]["client_token"], first_token)
            remote.create(item(name="changed"), token)
            self.assertNotEqual(request.call_args.kwargs["params"]["client_token"], first_token)
            self.assertEqual(request.call_args.kwargs["retries"], 0)

    def test_incomplete_pagination_rejected(self):
        remote = LinkRemote(None)
        for pages in [[{"items": []}], [{"items": [], "has_more": True}],
                      [{"items": [], "has_more": True, "page_token": "a"}] * 2]:
            with patch.object(remote, "request", side_effect=pages), self.assertRaises(PortalError): remote.list_all()

    def test_pagination_reads_all_pages(self):
        remote = LinkRemote(None)
        with patch.object(remote, "request", side_effect=[{"items": [1], "has_more": True, "page_token": "a"}, {"items": [2], "has_more": False}]):
            self.assertEqual(remote.list_all(), [1, 2])

    def _seed(self, count=5, gap=10):
        for index in range(count):
            self.remote.rows["rec" + str(index + 1)] = item(name="条目" + str(index + 1), sort=index * gap)

    def _ids(self):
        return [row["id"] for row in self.service.read(True)["items"]]

    def test_reorder_moves_item_down_and_up(self):
        self._seed()
        self.assertEqual(self._ids(), ["rec1", "rec2", "rec3", "rec4", "rec5"])
        # move rec1 after rec3
        self.service.reorder("rec1", "rec3", "after")
        self.assertEqual(self._ids(), ["rec2", "rec3", "rec1", "rec4", "rec5"])
        # move rec4 before rec2
        self.service.reorder("rec4", "rec2", "before")
        self.assertEqual(self._ids(), ["rec4", "rec2", "rec3", "rec1", "rec5"])

    def test_reorder_cross_many_pages_preserves_hidden_order(self):
        count = 2500
        self._seed(count, gap=10)
        ids = self._ids()
        self.assertEqual(len(ids), count)
        # Move a record near the front to just after a target near the end
        # (guaranteed to be on a different page for a real paginated read).
        self.service.reorder("rec1", "rec2499", "after")
        result = self._ids()
        expected = [rid for rid in ids if rid != "rec1"]
        expected.insert(expected.index("rec2499") + 1, "rec1")
        self.assertEqual(result, expected)
        # Hidden / untouched items keep their original relative order.
        kept = [rid for rid in result if rid not in {"rec1", "rec2499"}]
        self.assertEqual(kept, [rid for rid in ids if rid not in {"rec1", "rec2499"}])

    def test_reorder_minimal_updates_only_moved_item(self):
        self._seed()
        before = self.remote.reads
        self.service.reorder("rec1", "rec3", "after")
        self.assertEqual(self._ids(), ["rec2", "rec3", "rec1", "rec4", "rec5"])
        self.assertEqual([self.remote.rows["rec1"]["sort"], self.remote.rows["rec2"]["sort"],
                          self.remote.rows["rec3"]["sort"], self.remote.rows["rec4"]["sort"],
                          self.remote.rows["rec5"]["sort"]], [21, 10, 20, 30, 40])
        # only the moved item's sort changed -> single cloud write
        self.assertEqual(self.remote.update_orders_calls[-1], {"rec1": 21})

    def test_reorder_with_duplicate_sorts(self):
        self.remote.rows = {"rec1": item(name="A", sort=10), "rec2": item(name="B", sort=10),
                            "rec3": item(name="C", sort=10)}
        self.service.reorder("rec1", "rec3", "after")
        self.assertEqual(self._ids(), ["rec2", "rec3", "rec1"])
        # The moved item lands in the empty gap above its new upper neighbor.
        self.assertEqual(self.remote.update_orders_calls[-1], {"rec1": 11})

    def test_reorder_success_reads_once_and_persists(self):
        self._seed()
        before = self.remote.reads
        self.service.reorder("rec1", "rec3", "after")
        # The reorder did exactly one cloud read (the initial refresh), with no post-write pull.
        self.assertEqual(self.remote.reads, before + 1)
        self.assertEqual([row["id"] for row in self.service.read()["items"]],
                         ["rec2", "rec3", "rec1", "rec4", "rec5"])
        self.assertEqual(self.remote.reads, before + 1)
        # Restarting from the same store restores the reordered snapshot without a cloud pull.
        restored = LinkDirectory(self.store, self.remote)
        self.assertEqual([row["id"] for row in restored.read()["items"]],
                         ["rec2", "rec3", "rec1", "rec4", "rec5"])
        self.assertEqual(self.remote.reads, before + 1)

    def test_reorder_failure_re_read_also_fails(self):
        self._seed()
        self.remote.batch_fail = True
        calls = {"count": 0}

        def flaky_list_all():
            calls["count"] += 1
            if calls["count"] >= 2:
                raise RuntimeError("re-read offline")
            return [{"record_id": key, "fields": to_fields(value)} for key, value in self.remote.rows.items()]

        with patch.object(self.remote, "list_all", side_effect=flaky_list_all):
            with self.assertRaises(PortalError) as ctx:
                self.service.reorder("rec1", "rec3", "after")
        self.assertIn("云端回读失败", str(ctx.exception))

    def test_reorder_renumbers_when_no_gap(self):
        self.remote.rows = {"rec1": item(name="A", sort=10), "rec2": item(name="B", sort=11),
                            "rec3": item(name="C", sort=50)}
        # Move rec3 between rec1(sort10) and rec2(sort11): there is no integer gap,
        # so the full expected sequence is renumbered 10,20,30.
        self.service.reorder("rec3", "rec2", "before")
        self.assertEqual(self._ids(), ["rec1", "rec3", "rec2"])
        self.assertEqual(self.remote.rows["rec1"]["sort"], 10)
        self.assertEqual(self.remote.rows["rec3"]["sort"], 20)
        self.assertEqual(self.remote.rows["rec2"]["sort"], 30)

    def test_reorder_uses_zero_boundary(self):
        self.remote.rows = {"rec1": item(name="A", sort=5), "rec2": item(name="B", sort=10)}
        self.service.reorder("rec2", "rec1", "before")
        self.assertEqual(self._ids(), ["rec2", "rec1"])
        self.assertEqual(self.remote.rows["rec2"]["sort"], 0)

    def test_reorder_duplicate_request_does_not_write(self):
        self._seed()
        self.service.reorder("rec1", "rec2", "before")
        writes = len(self.remote.update_orders_calls)
        result = self.service.reorder("rec1", "rec2", "before")
        self.assertEqual(self._ids(), ["rec1", "rec2", "rec3", "rec4", "rec5"])
        self.assertEqual(len(self.remote.update_orders_calls), writes)
        # can_edit is added by the route, not by the service result.
        self.assertNotIn("can_edit", result)
        self.assertFalse(result["stale"])
        self.assertEqual(result["error"], "")

    def test_reorder_invalid_inputs_and_self(self):
        for args in [("bad", "rec1", "before"), ("rec1", "bad", "after"), ("rec1", "rec1", "before"),
                     ("rec1", "rec2", "middle"), ("rec1", "rec2", ["after"]), ("rec1", "rec2", {"p": "after"}),
                     ("rec1", "rec2", 1)]:
            with self.subTest(args=args), self.assertRaises(PortalError): self.service.reorder(*args)

    def test_reorder_missing_target_and_record(self):
        self._seed()
        with self.assertRaises(PortalNotFoundError): self.service.reorder("rec1", "rec999", "after")
        with self.assertRaises(PortalNotFoundError): self.service.reorder("rec999", "rec1", "after")

    def test_reorder_lost_response_reads_cloud_and_retry_matches(self):
        self._seed()
        self.remote.lost_update = True
        with self.assertRaises(PortalError) as ctx:
            self.service.reorder("rec1", "rec3", "after")
        self.assertIn("未确认", str(ctx.exception))
        # committed to cloud already; retry sees it adjacent and does not write again
        writes = len(self.remote.update_orders_calls)
        result = self.service.reorder("rec1", "rec3", "after")
        self.assertEqual(self._ids(), ["rec2", "rec3", "rec1", "rec4", "rec5"])
        self.assertEqual(len(self.remote.update_orders_calls), writes)
        self.assertEqual(result["error"], "")

    def test_reorder_partial_success_reflects_real_partial_state(self):
        self.remote.rows = {"rec1": item(name="A", sort=10), "rec2": item(name="B", sort=11),
                            "rec3": item(name="C", sort=12), "rec4": item(name="D", sort=40)}
        # Move rec4 before rec2 -> expected seq [rec1, rec4, rec2, rec3]. rec1 and rec2 are
        # adjacent (10, 11), so no gap exists and the renumber changes at least three records.
        self.remote.batch_partial = True
        with self.assertRaises(PortalError):
            self.service.reorder("rec4", "rec2", "before")
        # Only the first update committed on the cloud; the local re-read reflects the real
        # partial state instead of faking the whole batch as successful.
        self.assertEqual(self.remote.rows["rec4"]["sort"], 20)
        self.assertEqual(self.remote.rows["rec2"]["sort"], 11)
        self.assertEqual(self.remote.rows["rec3"]["sort"], 12)
        self.assertEqual(self.remote.rows["rec1"]["sort"], 10)
        local = self.service.read()["items"]
        self.assertEqual([row["id"] for row in local], ["rec1", "rec2", "rec3", "rec4"])
        self.assertEqual([row["sort"] for row in local], [10, 11, 12, 20])

    def test_reorder_only_writes_sort_field(self):
        self.remote.rows = {"rec1": item(name="A", sort=10, purpose="p1", category="c1"),
                            "rec2": item(name="B", sort=20, purpose="p2", category="c2")}
        self.service.reorder("rec2", "rec1", "before")
        self.assertEqual(self._ids(), ["rec2", "rec1"])
        self.assertEqual(self.remote.rows["rec2"]["sort"], 0)
        # Only the moved item's sort may change; other fields and other rows stay intact.
        self.assertEqual(self.remote.rows["rec1"]["sort"], 10)
        self.assertEqual(self.remote.rows["rec1"]["name"], "A")
        self.assertEqual(self.remote.rows["rec1"]["purpose"], "p1")
        self.assertEqual(self.remote.rows["rec1"]["category"], "c1")
        self.assertEqual(self.remote.rows["rec1"]["url"], item()["url"])
        self.assertEqual(self.remote.rows["rec2"]["name"], "B")
        self.assertEqual(self.remote.rows["rec2"]["purpose"], "p2")
        self.assertEqual(self.remote.rows["rec2"]["category"], "c2")
        self.assertEqual(self.remote.rows["rec2"]["url"], item()["url"])

    def test_concurrent_reorders_of_different_items_do_not_lose(self):
        self._seed()
        def move_a(): self.service.reorder("rec1", "rec3", "after")
        def move_b(): self.service.reorder("rec5", "rec4", "before")
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda fn: fn(), [move_a, move_b]))
        self.assertEqual(self._ids(), ["rec2", "rec3", "rec1", "rec5", "rec4"])

    def test_update_orders_verifies_returned_ids(self):
        guard = {"real_write_allowed": True}
        updates = {f"rec{i}": i for i in range(1, 1002)}
        recorded = []

        def responder(method, url, **kwargs):
            records = (kwargs.get("json_payload") or {}).get("records") or []
            recorded.append((url, records))
            return {"code": 0, "data": {"records": [{"record_id": r["record_id"]} for r in records]}}

        client = SimpleNamespace(request_json=responder)
        service = SimpleNamespace(_http_client=client, _auth_headers=lambda: {})
        remote = LinkRemote(service)
        expected_url = (f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}"
                        f"/tables/{TABLE_ID}/records/batch_update")
        with patch("lan_bitable_template_portal.portal_service.external_real_write_guard", return_value=guard):
            remote.update_orders(updates)
        self.assertEqual([url for url, _ in recorded], [expected_url] * 3)
        # >500 updates are batched in 500-record chunks.
        self.assertEqual([len(records) for _, records in recorded], [500, 500, 1])
        for _, records in recorded:
            for record in records:
                # Only the sort field is requested, and the URL only ever targets the directory table.
                self.assertEqual(set(record), {"record_id", "fields"})
                self.assertEqual(set(record["fields"]), {"排序"})
                self.assertEqual(int(record["fields"]["排序"]), int(record["record_id"][3:]))

    def test_update_orders_rejects_missing_duplicate_and_bad_ids(self):
        guard = {"real_write_allowed": True}
        updates = {"rec1": 1, "rec2": 2}

        def make_remote(records_value):
            client = SimpleNamespace(request_json=lambda method, url, **kwargs: {"code": 0,
                                                                                 "data": {"records": records_value}})
            service = SimpleNamespace(_http_client=client, _auth_headers=lambda: {})
            return LinkRemote(service)

        cases = [
            None,  # 响应 records 不是 list
            [],  # 返回记录缺失
            [{"record_id": "rec1"}],  # 缺失一个 id
            [{"record_id": "rec1"}, {"record_id": "rec1"}],  # 重复 id
            [{"record_id": "rec1"}, "bad"],  # 记录不是 dict
            [{"record_id": "rec1"}, {"fields": {}}],  # 缺少 record_id
        ]
        for value in cases:
            with self.subTest(value=value), patch(
                    "lan_bitable_template_portal.portal_service.external_real_write_guard", return_value=guard):
                with self.assertRaises(PortalError): make_remote(value).update_orders(updates)


class RouteTests(unittest.TestCase):
    def setUp(self):
        self.remote = Remote()
        self.remote.rows["rec1"] = item()
        self.session = {"role": "admin", "user": {"open_id": "test"}}
        controller = SimpleNamespace(_current_session=lambda request: self.session,
            _request_base_url=lambda request: "http://testserver",
            _auth_required_response=lambda: JSONResponse({"ok": False}, status_code=401))
        runtime = SimpleNamespace(state_store=Store(), service=None,
            auth_manager=SimpleNamespace(is_admin=lambda s: s.get("role") == "admin"))
        self.patch = patch("lan_bitable_template_portal.link_directory_routes.LinkRemote", return_value=self.remote)
        self.patch.start(); self.addCleanup(self.patch.stop)
        app = FastAPI(); install_link_directory_routes(app, controller, runtime)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)
        self.origin = {"Origin": "http://testserver"}

    def test_admin_create_update_delete(self):
        data = item(url="https://vnet.feishu.cn/base/test?table=tblNew", request_id=str(uuid.uuid4()))
        result = self.client.post("/api/link-directory", json=data, headers=self.origin)
        self.assertEqual(result.status_code, 200, result.text)
        key = result.json()["data"]["item"]["id"]
        self.assertEqual(self.client.put("/api/link-directory/"+key, json={**data, "name": "已修改"}, headers=self.origin).status_code, 200)
        self.assertEqual(self.client.delete("/api/link-directory/"+key, headers=self.origin).status_code, 200)

    def test_normal_user_reads_but_cannot_write(self):
        self.session["role"] = "building"
        result = self.client.get("/api/link-directory")
        self.assertFalse(result.json()["data"]["can_edit"])
        for method, path in [("post", ""), ("put", "/rec1"), ("delete", "/rec1")]:
            self.assertEqual(getattr(self.client, method)("/api/link-directory"+path, headers=self.origin).status_code, 403)
        self.assertEqual(self.client.post("/api/link-directory/refresh", headers=self.origin).status_code, 200)

    def test_anonymous_guest_temporary_denied_before_cloud(self):
        for session, code in [(None, 401), ({"role": "guest", "user": {"open_id": "guest"}}, 403),
                              ({"role": "temporary", "user": {"open_id": "temp"}}, 403), ({"user": {}}, 403)]:
            self.session = session
            self.assertEqual(self.client.get("/api/link-directory").status_code, code)
        self.assertEqual(self.remote.reads, 0)

    def test_cross_origin_missing_origin_and_oversize_blocked(self):
        for headers in [{}, {"Origin": "https://evil.test"}, {**self.origin, "Sec-Fetch-Site": "cross-site"}]:
            self.assertEqual(self.client.post("/api/link-directory", json=item(), headers=headers).status_code, 403)
        result = self.client.post("/api/link-directory", content=b" " * 17000, headers=self.origin)
        self.assertEqual(result.status_code, 400)
        self.assertEqual(self.remote.reads, 0)

    def test_admin_reorder_and_returns_expected_shape(self):
        for index in range(4):
            self.remote.rows["rec" + str(index + 1)] = item(name="条目" + str(index + 1), sort=index * 10)
        result = self.client.post("/api/link-directory/reorder",
                                  json={"record_id": "rec1", "target_id": "rec3", "placement": "after"},
                                  headers=self.origin)
        self.assertEqual(result.status_code, 200, result.text)
        data = result.json()["data"]
        self.assertEqual([row["id"] for row in data["items"]], ["rec2", "rec3", "rec1", "rec4"])
        self.assertTrue(data["can_edit"])
        self.assertFalse(data["stale"])
        self.assertEqual(data["error"], "")

    def test_reorder_requires_admin_formal_and_same_origin(self):
        self.remote.rows["rec2"] = item(name="B", sort=20)
        payload = {"record_id": "rec1", "target_id": "rec2", "placement": "after"}
        self.session["role"] = "building"
        self.assertEqual(self.client.post("/api/link-directory/reorder", json=payload, headers=self.origin).status_code, 403)
        self.session["role"] = "admin"
        for headers in [{}, {"Origin": "https://evil.test"}, {**self.origin, "Sec-Fetch-Site": "cross-site"}]:
            self.assertEqual(self.client.post("/api/link-directory/reorder", json=payload, headers=headers).status_code, 403)
        self.assertEqual(self.remote.update_orders_calls, [])

    def test_reorder_guest_and_temporary_denied(self):
        self.remote.rows["rec2"] = item(name="B", sort=20)
        payload = {"record_id": "rec1", "target_id": "rec2", "placement": "after"}
        for session in [{"role": "guest", "user": {"open_id": "g"}}, {"role": "temporary", "user": {"open_id": "t"}}]:
            self.session = session
            self.assertEqual(self.client.post("/api/link-directory/reorder", json=payload,
                                              headers=self.origin).status_code, 403)
        self.assertEqual(self.remote.update_orders_calls, [])

    def test_reorder_invalid_payload_and_strict_keys(self):
        for body in [{}, {"record_id": "rec1", "target_id": "rec2"}, {"record_id": "rec1", "target_id": "rec2",
                     "placement": "x"}, {"record_id": "rec1", "target_id": "rec2", "placement": "after", "extra": 1},
                     {"record_id": "rec1", "target_id": "rec2", "placement": ["after"]},
                     {"record_id": "rec1", "target_id": "rec2", "placement": {"p": "after"}}]:
            with self.subTest(body=body):
                self.assertEqual(self.client.post("/api/link-directory/reorder", json=body, headers=self.origin).status_code, 400)
        self.assertEqual(self.remote.update_orders_calls, [])


if __name__ == "__main__": unittest.main()

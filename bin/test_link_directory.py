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
from lan_bitable_template_portal.portal_service import PortalError, PortalConflictError


class Store:
    def __init__(self): self.values = {}
    def get_document(self, namespace, key): return copy.deepcopy(self.values.get((namespace, key)))
    def put_document(self, namespace, key, value): self.values[(namespace, key)] = copy.deepcopy(value)


def item(**kwargs):
    return {"name": "维修项目", "url": "https://vnet.feishu.cn/base/testBase?table=tblTest", "category": "维修",
            "purpose": "跟进查询", "sort": 1, **kwargs}


class Remote:
    def __init__(self): self.rows = {}; self.reads = 0; self.creates = 0; self.fail = False; self.lose_create = False
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


if __name__ == "__main__": unittest.main()

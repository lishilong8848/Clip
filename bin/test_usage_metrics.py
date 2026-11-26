import sqlite3
import tempfile
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from lan_bitable_template_portal import usage_metrics as usage
from lan_bitable_template_portal.usage_routes import install_usage_routes


class CatalogTests(unittest.TestCase):
    def test_shared_modules_group_under_the_same_feishu_name(self):
        web, qt = usage._catalog(False), usage._catalog(True)
        for module in web.keys() & qt.keys():
            self.assertEqual(web[module]["label"], qt[module]["label"], module)
        for catalog in (web, qt):
            for spec in catalog.values():
                self.assertTrue(spec["label"] and all(spec["pages"].values()))
                self.assertLessEqual(set(spec.get("public_pages", ())), spec["pages"].keys())


class UsageTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        catalog = {"test": {"label": "测试功能", "pages": {"list": "清单", "public": "公开入口"},
                            "public_pages": ["public"]}}
        mapping = patch.object(usage, "_catalog", return_value=catalog)
        mapping.start()
        self.addCleanup(mapping.stop)

    @staticmethod
    def event(**changes):
        return {"event_id": str(uuid.uuid4()), "module": "test", "page": "list",
                "client": "web_v2", **changes}

    def read(self):
        with closing(sqlite3.connect(self.root / "page_usage.sqlite3")) as db:
            return db.execute("SELECT day,client,count,synced,record_id FROM daily ORDER BY day,client").fetchall()

    def test_retry_parallel_and_midnight(self):
        before = int(datetime(2026,11,25,15,59,59,tzinfo=timezone.utc).timestamp() * 1000)
        events = [self.event(occurred_at=before), self.event(occurred_at=before+1000),
                  self.event(client="qt", occurred_at=before)]
        with ThreadPoolExecutor(4) as pool:
            list(pool.map(lambda _: usage.record_usage_events(self.root,events,now=before/1000+100),range(8)))
        self.assertEqual(self.read(), [
            ("2026-11-25","qt",1,0,""), ("2026-11-25","web_v2",1,0,""), ("2026-11-26","web_v2",1,0,"")])

    def test_unacknowledged_write_and_concurrent_visit(self):
        usage.record_usage_events(self.root,[self.event()])
        arrived, release = threading.Event(), threading.Event()
        attempts = []
        def write(row):
            attempts.append((row["key"],row["count"]))
            if len(attempts)==1:
                arrived.set()
                if not release.wait(5):
                    raise AssertionError("test writer stalled")
                raise TimeoutError("模拟丢回执")
            return "remote-row"
        sink=SimpleNamespace(upsert=write)
        with ThreadPoolExecutor(2) as pool:
            future=pool.submit(usage.sync_usage,self.root,sink)
            try:
                self.assertTrue(arrived.wait(5))
                self.assertEqual(usage.sync_usage(self.root,sink),0)
                usage.record_usage_events(self.root,[self.event()])
            finally:
                release.set()
            self.assertEqual(future.result(5),0)
        self.assertEqual(usage.sync_usage(self.root,sink),0, "respect backoff")
        with closing(sqlite3.connect(self.root/"page_usage.sqlite3")) as db, db:
            db.execute("UPDATE daily SET next_try=0")
        self.assertEqual(usage.sync_usage(self.root,sink),1)
        self.assertEqual(attempts,[(attempts[0][0],1),(attempts[0][0],2)])
        self.assertEqual(self.read()[0][2:],(2,2,"remote-row"))
        self.assertEqual(usage.sync_usage(self.root,sink),0)

    def test_visit_arriving_during_success_is_not_acknowledged_early(self):
        event=self.event()
        usage.record_usage_events(self.root,[event])
        def slow_success(row):
            usage.record_usage_events(self.root,[event,self.event()])
            return "record"
        usage.sync_usage(self.root,SimpleNamespace(upsert=slow_success))
        self.assertEqual(self.read()[0][2:],(2,1,"record"))
        sink=Mock()
        sink.upsert.return_value="record"
        self.assertEqual(usage.sync_usage(self.root,sink),1)
        self.assertEqual(sink.upsert.call_args.args[0]["record_id"],"record")
        self.assertEqual(self.read()[0][2:],(2,2,"record"))

    def test_route_validation_and_deduplication(self):
        class Controller:
            session=None
            def _current_session(self, _request):
                return self.session
            def _request_base_url(self, request):
                return str(request.base_url)
            async def _read_json_request(self, request, *, max_bytes):
                if len(await request.body()) > max_bytes:
                    raise ValueError("size")
                return await request.json()
        controller=Controller()
        app=FastAPI()
        install_usage_routes(app,controller,SimpleNamespace(state_store=SimpleNamespace(db_path=self.root/"portal.db")))
        with TestClient(app) as client:
            def post(events,origin="http://testserver"):
                return client.post("/api/usage/events",json={"events":events},headers={"Origin":origin})
            event=self.event(page="public")
            self.assertEqual(post([event],origin="https://elsewhere.invalid").status_code,403)
            self.assertEqual(post([event,self.event()]).status_code,401)
            self.assertEqual(post([event]).json()["data"]["accepted"],1)
            controller.session={"role":"guest"}
            self.assertEqual(post([self.event()]).status_code,401)
            self.assertEqual(post([event]).json()["data"]["accepted"],1)
            controller.session={"user":{"open_id":"isolated"}}
            for invalid in [self.event(client="qt"),self.event(module="unknown"),self.event(page="unknown"),
                            self.event(url="private"),self.event(event_id="invalid")]:
                self.assertEqual(post([self.event(),invalid]).status_code,400)
            self.assertEqual(post([event,event]).json()["data"]["accepted"],2)
            self.assertEqual(self.read()[0][2],1)

    def test_busy_page_does_not_starve_unsynced_pages(self):
        a=self.event()
        sink=Mock()
        sink.upsert.return_value="record"
        usage.record_usage_events(self.root,[a,self.event(page="public")])
        usage.sync_usage(self.root,sink,limit=1)
        usage.record_usage_events(self.root,[self.event()])
        usage.sync_usage(self.root,sink,limit=1)
        self.assertEqual([c.args[0]["page"] for c in sink.upsert.call_args_list],["list","public"])
        usage.sync_usage(self.root,sink,limit=1)
        self.assertEqual(sink.upsert.call_args.args[0]["count"],2)

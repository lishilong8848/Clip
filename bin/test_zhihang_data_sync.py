import datetime as dt
import unittest
from copy import deepcopy

from lan_bitable_template_portal.zhihang_data_sync import NS, TZ, SyncError, ZhihangBitable, ZhihangSync

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
        return [{"key": key, "payload": deepcopy(payload)} for (namespace, key), payload in self.data.items()
                if namespace == ns and key.startswith(key_prefix)]

    def delete_document(self, ns, key):
        self.data.pop((ns, key), None)


class Cloud:
    def __init__(self):
        self.rows = {"recManual": {"事件ID": "manual", "事件标题": "现场手填", "发生时间": TODAY + " 08:00"}}
        self.created = 0
        self.fail_after_create = False
        self.deleted = []

    def list_all(self, path="records"):
        if path == "fields":
            return [{"field_name": field, "type": 1} for field in ("事件ID", "事件标题", "发生时间")]
        return [self.get(key) for key in self.rows]

    def get(self, key):
        return {"record_id": key, "fields": deepcopy(self.rows[key])}

    def request(self, method, path, body, params):
        self.created += 1
        record = f"recNew{self.created}"
        self.rows[record] = deepcopy(body["fields"])
        if self.fail_after_create:
            raise ConnectionError("connection dropped after write")
        return {"record": self.get(record)}

    def update(self, key, fields):
        self.rows[key].update(deepcopy(fields))

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
        worker.kick = lambda: None
        return worker

    def enqueue(self, mode, preview_id=""):
        run = self.worker.enqueue("event", mode, preview_id)
        self.worker.drain()
        return self.worker.detail(run["id"])

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
        restarted.drain()
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

    def test_empty_data_and_disabled_settings_leave_cloud_unchanged(self):
        self.records.clear()
        self.assertEqual(self.enqueue("preview")["status"], "failed")
        self.assertEqual(self.cloud.created, 0)
        self.worker.configure("event", {"confirmed": False})
        with self.assertRaises(SyncError):
            self.worker.configure("event", {"enabled": True})
        with self.assertRaises(SyncError):
            self.worker.enqueue("event", "sync", "unconfirmed")

    def test_bitable_pagination_refuses_a_truncated_batch(self):
        client = ZhihangBitable()
        pages = iter([{"items": [{"record_id": "rec1"}], "has_more": True, "total": 3, "page_token": "next"},
                      {"items": [{"record_id": "rec2"}], "has_more": False, "total": 3}])
        client.request = lambda *args, **kwargs: next(pages)
        with self.assertRaisesRegex(SyncError, "分页未读全"):
            client.list_all()

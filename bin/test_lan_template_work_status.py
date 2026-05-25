import os
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import json
import datetime as dt
import base64
import ast
import re
from pathlib import Path
from unittest.mock import patch

import httpx

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import MaintenancePortalService  # noqa: E402
from lan_bitable_template_portal.portal_service import PortalError  # noqa: E402
from lan_bitable_template_portal.portal_service import RECENT_MONTH_FILTER_LABEL  # noqa: E402
from lan_bitable_template_portal.portal_service import WORK_TYPE_CHANGE  # noqa: E402
from lan_bitable_template_portal.portal_service import WORK_TYPE_REPAIR  # noqa: E402
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME  # noqa: E402
from lan_bitable_template_portal.portal_auth import PortalAuthManager  # noqa: E402
import lan_bitable_template_portal.server as portal_server_module  # noqa: E402
from lan_bitable_template_portal.server import PortalHandler  # noqa: E402
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402
from clipflow_backend.main import FastAPIPortalController  # noqa: E402
from clipflow_backend.process_controller import BackendProcessPortalController  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
import upload_event_module.config as config_module  # noqa: E402
from upload_event_module.config import ConfigManager  # noqa: E402
from upload_event_module.config import MAINTENANCE_NOTICE_FIELDS  # noqa: E402
from upload_event_module.services.http_client import FeishuHttpClient  # noqa: E402
from upload_event_module.services.handlers.base import NoticePayload  # noqa: E402
from upload_event_module.services.handlers.maintenance_notice import (  # noqa: E402
    MaintenanceNoticeHandler,
)
from upload_event_module.core.parser import extract_event_info  # noqa: E402
import upload_event_module.services.feishu_service as feishu_service_module  # noqa: E402
from upload_event_module.ui.active_cache_store import ActiveCacheStore  # noqa: E402
from upload_event_module.ui.main_window_workflow import MainWindowWorkflowMixin  # noqa: E402
from upload_event_module.ui.main_window_ui import MainWindowUiMixin  # noqa: E402

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _TestMaintenancePortalService(MaintenancePortalService):
    def refresh(self) -> None:
        return

    def ensure_loaded(self) -> None:
        return


class _TargetLookupService(_TestMaintenancePortalService):
    def __init__(self):
        super().__init__()
        self.target_record_ids: set[str] = set()

    def _target_record_exists_for_status_item(self, item, target_cache):
        target_record_id = str(
            item.get("target_record_id")
            or item.get("feishu_record_id")
            or item.get("record_id")
            or ""
        ).strip()
        if not target_record_id:
            return None
        return target_record_id in self.target_record_ids


class _ChangeSourceFailureService(MaintenancePortalService):
    def _load_fields(self):
        self._field_meta_list = []
        self._field_meta_by_name = {}
        return []

    def _load_records(self):
        self._records = [_build_record("m1", "A楼", "过滤网维护", "5月")]
        self._maintenance_loaded_once = True
        return self._records

    def _load_change_fields(self):
        raise RuntimeError("mock change source down")


class _WorkflowRuntimeQueueHarness(MainWindowWorkflowMixin):
    def __init__(self, store):
        self._lan_portal_state_store = store


class _WorkflowBackendDelegateHarness(MainWindowWorkflowMixin):
    def __init__(self, controller):
        self.lan_template_portal_controller = controller
        self.finished = []

    def _post_request_finished(self, name, success, msg, record_id):
        self.finished.append((name, bool(success), msg, record_id))


class _HistoryMigrationHarness(MainWindowUiMixin):
    def __init__(self, store):
        self._lan_portal_state_store = store
        self.last_history_mtime = 0


class _NativeFastAPIRouteService:
    _last_loaded_at = ""
    _load_warnings: list[str] = []

    def state_cache_version(self):
        return 1

    def _scope_matches_item(self, scope, item):
        return True

    def reconcile_orphan_started_items(self, *, scope, ongoing_items):
        return {"removed": 0}

    def get_bootstrap(self, *, scope, ongoing_items):
        return {
            "scope": scope,
            "scope_options": [{"value": "ALL", "label": "全部"}],
            "ongoing": ongoing_items,
            "route": "bootstrap",
        }

    def refresh(self):
        self.refreshed = True

    def refresh_if_interval_elapsed(self, *, min_interval_seconds):
        self.refreshed_if_interval = min_interval_seconds
        return True

    def process_due_repair_link_tasks(self, *, limit):
        self.repair_link_limit = limit
        return {"processed": 0}

    def refresh_repair_source(self):
        return {"repair_refresh_reused": False, "repair_refresh_mock": True}

    def get_scope_overview(self, *, ongoing_items, scopes, include_prepared):
        return {
            "scope_options": [{"value": "ALL", "label": "全部"}],
            "scopes": scopes,
            "ongoing": ongoing_items,
            "route": "scope-overview",
        }

    def query_records(self, *, month, specialty, scope, ongoing_items):
        return {
            "scope": scope,
            "month": month,
            "specialty": specialty,
            "ongoing": ongoing_items,
            "route": "workbench",
            "scope_options": [{"value": "ALL", "label": "全部"}],
        }

    def get_history_summary(self, *, scope, month, work_type):
        return {
            "scope": scope,
            "month": month,
            "work_type": work_type,
            "items": [],
            "route": "history-summary",
        }

    def get_handover_links(self):
        return {
            "links": {"ALL": "http://example.test"},
            "scope_options": [{"value": "ALL", "label": "全部"}],
            "route": "handover-links",
        }

    def save_handover_links(self, links, *, password):
        self.saved_handover_links = dict(links or {})
        self.saved_handover_password = password
        return {
            "links": self.saved_handover_links,
            "scope_options": [{"value": "ALL", "label": "全部"}],
            "route": "handover-links-save",
        }

    def verify_handover_settings_password(self, password):
        return str(password or "") == "ok"

    def request_handover_password_reset(self):
        return {"reset_id": "reset1", "expires_at": "2026-05-22 15:00:00"}

    def reset_handover_password_with_code(self, *, reset_id, code, new_password):
        return {
            "reset_id": reset_id,
            "code_ok": code == "123456",
            "new_password_len": len(new_password),
        }

    def create_action_job(self, payload):
        self.last_action_payload = dict(payload)
        return "job-native", False

    def get_job(self, job_id):
        if job_id == "job-native":
            return {
                "job_id": job_id,
                "accepted_at": 123,
                "phase": "accepted",
                "request": getattr(self, "last_action_payload", {}),
            }
        return None

    def mark_job(self, job_id, **patch):
        self.last_marked_job_id = job_id
        self.last_marked_job_patch = dict(patch)

    def validate_ongoing_delete_item(self, payload, *, scope):
        self.last_delete_payload = dict(payload)
        return None

    def hide_ongoing_item(self, payload, *, scope, deleted_by):
        return {
            "deleted": True,
            "scope": scope,
            "deleted_by": deleted_by,
            "active_item_id": payload.get("active_item_id") or "",
        }

    def discard_deleted_ongoing_state(self, payload, *, scope):
        return {"discarded": True}

    def assert_generated_drafts_allowed(self, drafts, *, scope):
        self.generated_draft_scope = scope
        self.generated_drafts = list(drafts or [])

    def generate_templates(self, drafts):
        return [
            {
                "record_id": str((draft or {}).get("record_id") or ""),
                "text": "generated",
            }
            for draft in drafts or []
        ]

    def assert_generated_items_allowed(self, items, *, scope):
        self.generated_item_scope = scope
        self.generated_items = list(items or [])

    def send_generated_templates(self, items, *, notice_callback=None):
        results = []
        for item in items or []:
            if notice_callback is not None:
                notice_callback({"title": item.get("title") or "", "text": item.get("text") or ""})
            results.append(
                {
                    "record_id": str((item or {}).get("record_id") or ""),
                    "ok": True,
                }
            )
        return results

    def _load_zhihang_change_fields(self):
        self._zhihang_change_field_meta_list = []
        self._zhihang_change_field_meta_by_name = {}
        return []

    def _load_zhihang_change_records(self):
        self._zhihang_change_records = []
        self._zhihang_change_loaded_once = True
        return []

    def _load_repair_fields(self):
        self._repair_field_meta_list = []
        self._repair_field_meta_by_name = {}
        return []

    def _load_repair_records(self):
        self._repair_records = []
        self._repair_loaded_once = True
        return []


class _HistoryHarness(MainWindowUiMixin):
    pass


def _build_record(
    record_id: str,
    building: str,
    maintenance_total: str,
    month: str,
    status: str = "未开始",
    maintenance_cycle: str = "",
):
    return {
        "record_id": record_id,
        "raw_fields": {},
        "display_fields": {
            "楼栋": building,
            "维护总项": maintenance_total,
            "维护实施状态": status,
            "计划维护月份": month,
            "专业类别": "电气",
            "维护编号": "WB-001",
            "维护项目": "例行维护",
            "维护周期": maintenance_cycle,
        },
    }


def _build_change_record(
    record_id: str,
    *,
    building: str,
    progress: str,
    title: str = "测试变更",
    start_time: str = "2026-05-08 09:00",
    end_time: str = "2026-05-08 18:00",
):
    return {
        "record_id": record_id,
        "raw_fields": {},
        "work_type": "change",
        "notice_type": "设备变更",
        "source_app_token": "JhiVwgfoIimAqEk8YwEc09sknGd",
        "source_table_id": "tblBvg6wCYSX3hcg",
        "display_fields": {
            "变更简述": title,
            "变更进度": progress,
            "变更楼栋": building,
            "专业": "电气",
            "变更等级（阿里）": "低",
            "变更开始日期（阿里）": start_time,
            "变更结束日期（阿里）": end_time,
        },
    }


def _build_repair_record(
    record_id: str,
    *,
    title: str = "测试检修",
    repair_name: str | None = None,
    event_description: str = "",
    event_level: str = "",
    source: str = "",
    building: str = "D楼",
    specialty: str = "电气",
    fault_time: str = "2026-05-08 08:20",
    expected_time: str = "",
    started: bool = False,
    ended: bool = False,
    target_record_id: str = "",
):
    raw_fields = {}
    if target_record_id:
        raw_fields["设备检修关联"] = [
            {
                "record_ids": [target_record_id],
                "table_id": "tblSA9euoote8aCA",
                "text": f"{title}-目标",
                "type": "text",
            }
        ]
    return {
        "record_id": record_id,
        "raw_fields": raw_fields,
        "work_type": "repair",
        "notice_type": "设备检修",
        "source_app_token": "AnEBwJlvGiJfDdkOB32cUPuknzg",
        "source_table_id": "tblschT48zXwigUG",
        "display_fields": {
            "检修通告名称": title,
            "维修名称": title if repair_name is None else repair_name,
            "事件描述": event_description,
            "对应事件等级": event_level,
            "对应来源": source,
            "所属数据中心/楼栋-使用": building,
            "所属专业": specialty,
            "设备名称": "UPS",
            "设备编号": "UPS-01",
            "故障维修原因": "测试故障原因",
            "故障发生现象描述": "测试故障现象",
            "故障发生时间": fault_time,
            "期望完成时间": expected_time,
            "维修开始时间": "2026-05-08 09:00" if started else "",
            "维修结束时间": "2026-05-08 18:00" if ended else "",
            "维修进展描述": "维修准备中",
            "流程": "流程",
            "区域": "D-UPS间",
        },
    }


def _build_zhihang_change_record(
    record_id: str,
    *,
    title: str,
    progress: str = "未开始",
    plan_start: str = "2026-05-08 09:00",
    plan_end: str = "2026-05-08 18:00",
):
    return {
        "record_id": record_id,
        "raw_fields": {},
        "work_type": "change",
        "notice_type": "设备变更",
        "source_app_token": "IrIibPkUOa6udGsMhu2cbOqhnWg",
        "source_table_id": "tblqMJvYW5dxFFfU",
        "display_fields": {
            "标题": title,
            "进度": progress,
            "变更等级": "低",
            "变更类型": "普通变更",
            "计划开始": plan_start,
            "计划结束": plan_end,
        },
    }


class LanTemplateWorkStatusTests(unittest.TestCase):
    def _new_temp_service(self, root: Path, service_cls=_TestMaintenancePortalService):
        def fake_data_path(name):
            return str(root / name)

        patcher = patch(
            "lan_bitable_template_portal.portal_service.get_data_file_path",
            side_effect=fake_data_path,
        )
        store_patcher = patch(
            "lan_bitable_template_portal.state_store.get_data_file_path",
            side_effect=fake_data_path,
        )
        patcher.start()
        store_patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(store_patcher.stop)
        return service_cls()

    def test_portal_service_empty_source_config_falls_back_to_defaults(self):
        service = MaintenancePortalService(app_token="", table_id="")

        self.assertEqual(service.app_token, "HU38bc1vnamMK9sCeOgclUvXnFc")
        self.assertEqual(service.table_id, "tblzk7WrXxNWQy6V")

    def test_portal_service_request_uses_defaults_if_source_attrs_are_cleared(self):
        service = MaintenancePortalService()
        service.app_token = ""
        service.table_id = ""
        captured = {}

        class _Response:
            ok = True
            text = ""

            @staticmethod
            def json():
                return {"code": 0, "data": {"items": []}}

        class _Session:
            @staticmethod
            def get(url, **kwargs):
                captured["url"] = url
                return _Response()

        service._session = _Session()
        service._auth_headers = lambda: {"Authorization": "Bearer test"}

        service._request_json("fields", params={"page_size": 500})

        self.assertIn("/apps/HU38bc1vnamMK9sCeOgclUvXnFc/", captured["url"])
        self.assertIn("/tables/tblzk7WrXxNWQy6V/", captured["url"])
        self.assertNotIn("/apps//", captured["url"])
        self.assertNotIn("/tables//", captured["url"])

    def test_feishu_http_client_returns_business_json_on_http_error(self):
        transport = httpx.MockTransport(
            lambda request: httpx.Response(
                400,
                json={"code": 99991663, "msg": "token expired"},
                request=request,
            )
        )
        client = FeishuHttpClient(transport=transport, retries=0)
        try:
            payload = client.request_json("GET", "https://open.feishu.cn/mock")
        finally:
            client.close()

        self.assertEqual(payload["code"], 99991663)

    def test_portal_service_request_uses_unified_http_client_by_default(self):
        service = MaintenancePortalService()
        captured = {}

        class _HttpClient:
            def request_json(self, method, url, **kwargs):
                captured["method"] = method
                captured["url"] = url
                captured["kwargs"] = kwargs
                return {"code": 0, "data": {"items": []}}

        service._http_client = _HttpClient()
        service._auth_headers = lambda: {"Authorization": "Bearer test"}

        payload = service._request_json("fields", params={"page_size": 500})

        self.assertEqual(payload["code"], 0)
        self.assertEqual(captured["method"], "GET")
        self.assertIn("/fields", captured["url"])
        self.assertEqual(captured["kwargs"]["params"]["page_size"], 500)

    def test_lan_portal_state_store_replaces_ongoing_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            item = {
                "active_item_id": "active-1",
                "record_id": "target-1",
                "source_record_id": "source-1",
                "work_type": "change",
                "notice_type": "设备变更",
                "title": "A楼变更",
                "building": "A楼",
                "building_codes": ["A"],
            }

            result = store.replace_ongoing_items([item])
            snapshot = store.get_ongoing_snapshot()

            self.assertEqual(result["count"], 1)
            self.assertTrue(snapshot["exists"])
            self.assertEqual(snapshot["count"], 1)
            self.assertEqual(snapshot["items"][0]["title"], "A楼变更")
            self.assertEqual(snapshot["items"][0]["building_codes"], ["A"])

            store.replace_ongoing_items([])
            snapshot = store.get_ongoing_snapshot()
            self.assertTrue(snapshot["exists"])
            self.assertEqual(snapshot["items"], [])

    def test_lan_portal_state_store_reports_database_stats(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            store.put_backend_runtime("probe", {"ok": True})

            stats = store.get_database_stats()

            self.assertTrue(stats["exists"])
            self.assertGreater(stats["db_bytes"], 0)
            self.assertGreaterEqual(stats["total_bytes"], stats["db_bytes"])
            self.assertEqual(stats["journal_mode"].lower(), "wal")
            self.assertIn("backend_runtime", stats["table_counts"])
            self.assertGreaterEqual(stats["table_counts"]["backend_runtime"], 1)

    def test_lan_portal_state_store_background_write_worker(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")

            self.assertTrue(store.put_backend_runtime_async("async_probe", {"ok": True}))
            self.assertTrue(store.append_event_async("async_event", {"value": 1}))
            store.shutdown_write_worker(timeout=2.0)

            runtime = store.get_backend_runtime("async_probe")
            events = store.list_events_after("async_event", 0, limit=10)
            stats = store.get_write_worker_stats()

            self.assertEqual(runtime["ok"], True)
            self.assertEqual(events[-1]["payload"]["value"], 1)
            self.assertGreaterEqual(stats["written"], 2)
            self.assertFalse(stats["alive"])

    def test_lan_portal_state_store_background_write_worker_coalesces_runtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            with patch.dict(
                os.environ,
                {"CLIPFLOW_SQLITE_WRITE_BATCH_WINDOW_MS": "50"},
                clear=False,
            ):
                self.assertTrue(store.put_backend_runtime_async("heartbeat", {"seq": 1}))
                self.assertTrue(store.put_backend_runtime_async("heartbeat", {"seq": 2}))
                store.shutdown_write_worker(timeout=2.0)

            runtime = store.get_backend_runtime("heartbeat")
            stats = store.get_write_worker_stats()

            self.assertEqual(runtime["seq"], 2)
            self.assertGreaterEqual(stats.get("coalesced", 0), 1)

    def test_lan_portal_state_store_runtime_queue_counts_and_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")

            self.assertTrue(store.upsert_runtime_queue_item("message", "job-1"))
            self.assertTrue(
                store.upsert_runtime_queue_item(
                    "message",
                    "job-2",
                    status="processing",
                )
            )
            self.assertTrue(store.mark_runtime_queue_item("message", "job-1", "done"))
            counts = store.runtime_queue_counts()

            self.assertEqual(counts["message"]["done"], 1)
            self.assertEqual(counts["message"]["processing"], 1)
            reset = store.reset_runtime_queue_incomplete()
            reset_counts = store.runtime_queue_counts()

            self.assertEqual(reset, 1)
            self.assertEqual(reset_counts["message"]["interrupted"], 1)
            removed = store.cleanup_runtime_queue_items(
                retention_seconds=60,
                max_delete=100,
            )
            self.assertEqual(removed, 0)

    def test_lan_portal_state_store_runtime_queue_lease_and_requeue(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")

            now = time.time()
            self.assertTrue(store.upsert_runtime_queue_item("message", "job-1"))
            self.assertTrue(
                store.upsert_runtime_queue_item(
                    "message",
                    "job-future",
                    available_at=now + 60,
                )
            )
            leased = store.lease_runtime_queue_items("message", limit=5, lease_seconds=10)

            self.assertEqual([item["job_id"] for item in leased], ["job-1"])
            self.assertEqual(leased[0]["status"], "processing")
            self.assertEqual(leased[0]["attempts"], 1)
            counts = store.runtime_queue_counts()
            self.assertEqual(counts["message"]["processing"], 1)
            self.assertEqual(counts["message"]["queued"], 1)
            details = store.runtime_queue_details()
            self.assertEqual(details["message"]["queued_future"], 1)
            self.assertEqual(details["message"]["processing_active"], 1)

            self.assertTrue(store.requeue_runtime_queue_item("message", "job-1"))
            requeued_counts = store.runtime_queue_counts()
            self.assertEqual(requeued_counts["message"]["queued"], 2)
            leased_again = store.lease_runtime_queue_items("message", limit=1, lease_seconds=10)
            self.assertEqual(leased_again[0]["job_id"], "job-1")
            self.assertEqual(leased_again[0]["attempts"], 2)

    def test_qt_upload_runtime_queue_marks_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            harness = _WorkflowRuntimeQueueHarness(store)

            harness._mark_qt_upload_runtime_queue(
                "record-1",
                "queued",
                payload={"record_id": "record-1"},
            )
            harness._mark_qt_upload_runtime_queue("record-1", "processing")
            harness._mark_qt_upload_runtime_queue("record-1", "done")

            counts = store.runtime_queue_counts()
            self.assertEqual(counts["qt_upload"]["done"], 1)

    def test_backend_controller_batches_active_item_delta(self):
        controller = BackendProcessPortalController(host="127.0.0.1", port=9)
        calls = []

        def fake_request(method, path, *, payload=None, timeout=5.0):
            calls.append(
                {
                    "method": method,
                    "path": path,
                    "payload": payload or {},
                    "timeout": timeout,
                }
            )
            return {"ok": True}

        controller._request_json = fake_request
        controller.post_active_items_delta(
            upserts=[{"data": {"active_item_id": "active-1"}}]
        )
        controller.post_active_items_delta(deletes=[{"active_item_id": "active-2"}])

        deadline = time.time() + 2.0
        while time.time() < deadline and not calls:
            time.sleep(0.02)
        controller._stop_event.set()
        thread = controller._active_delta_thread
        if thread and thread.is_alive():
            thread.join(timeout=1.0)

        self.assertTrue(calls)
        payloads = [call["payload"] for call in calls]
        self.assertEqual(calls[0]["method"], "POST")
        self.assertEqual(calls[0]["path"], "/api/qt/active-items/delta")
        self.assertEqual(
            sum(len(payload.get("upserts") or []) for payload in payloads),
            1,
        )
        self.assertEqual(
            sum(len(payload.get("deletes") or []) for payload in payloads),
            1,
        )

    def test_local_qt_notice_upload_create_path_returns_real_record_id(self):
        request_payload = {
            "action_type": "upload",
            "data_dict": {
                "record_id": "placeholder-1",
                "notice_type": "维保通告",
                "text": "【维保通告】状态：开始\n【标题】测试测试测试\n【时间】2026-05-24 09:30~2026-05-24 18:30",
                "buildings": ["A楼"],
                "specialty": "测试",
                "time_str": "2026-05-24 09:30~2026-05-24 18:30",
                "maintenance_cycle": "/",
            },
            "response_time": "2026-05-24 09:30",
            "recover_selected": False,
            "robot_group_choice": "auto",
        }
        with patch.object(
            portal_server_module,
            "create_bitable_record_by_payload",
            return_value=(True, "rec-backend-1"),
        ):
            result = PortalHandler.execute_local_notice_upload(request_payload)
        self.assertTrue(result["ok"])
        self.assertEqual(result["name"], "上传")
        self.assertEqual(result["record_id"], "placeholder-1")
        self.assertEqual(result["real_record_id"], "rec-backend-1")

    def test_workflow_delegate_qt_notice_upload_uses_backend_result(self):
        class _Controller:
            def execute_qt_notice_upload(self, payload):
                return {
                    "ok": True,
                    "name": "上传",
                    "message": "ignored",
                    "record_id": "placeholder-2",
                    "real_record_id": "rec-backend-2",
                }

        harness = _WorkflowBackendDelegateHarness(_Controller())
        handled = harness._delegate_qt_notice_upload_to_backend(
            data_snapshot={
                "record_id": "placeholder-2",
                "notice_type": "维保通告",
                "text": "测试测试测试",
            },
            screenshot_bytes=None,
            extra_images=[],
            action_type="upload",
            response_time="2026-05-24 09:30",
            recover_selected=False,
            robot_group_choice="auto",
        )
        self.assertTrue(handled)
        self.assertEqual(
            harness.finished,
            [("上传", True, "rec-backend-2", "placeholder-2")],
        )

    def test_portal_handler_dequeue_falls_back_to_sqlite_queue(self):
        class _FakeService:
            def get_job(self, job_id):
                return {"phase": "accepted"} if job_id == "job-sqlite" else {}

        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            old_store = PortalHandler.state_store
            old_service = PortalHandler.service
            old_queue = list(PortalHandler.message_queue)
            old_event = PortalHandler.message_queue_event
            try:
                PortalHandler.state_store = store
                PortalHandler.service = _FakeService()
                PortalHandler.message_queue = []
                PortalHandler.message_queue_event = threading.Event()
                store.upsert_runtime_queue_item("message", "job-sqlite")

                job_id = PortalHandler._dequeue_runtime_job("message")

                self.assertEqual(job_id, "job-sqlite")
                counts = store.runtime_queue_counts()
                self.assertEqual(counts["message"]["processing"], 1)
            finally:
                PortalHandler.state_store = old_store
                PortalHandler.service = old_service
                PortalHandler.message_queue = old_queue
                PortalHandler.message_queue_event = old_event

    def test_portal_handler_dequeue_skips_terminal_sqlite_jobs(self):
        class _FakeService:
            def get_job(self, job_id):
                if job_id == "job-done":
                    return {"phase": "success"}
                if job_id == "job-valid":
                    return {"phase": "accepted"}
                return {}

        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            old_store = PortalHandler.state_store
            old_service = PortalHandler.service
            old_queue = list(PortalHandler.message_queue)
            old_event = PortalHandler.message_queue_event
            try:
                PortalHandler.state_store = store
                PortalHandler.service = _FakeService()
                PortalHandler.message_queue = []
                PortalHandler.message_queue_event = threading.Event()
                store.upsert_runtime_queue_item("message", "job-done")
                store.upsert_runtime_queue_item("message", "job-valid")

                job_id = PortalHandler._dequeue_runtime_job("message")

                self.assertEqual(job_id, "job-valid")
                counts = store.runtime_queue_counts()
                self.assertEqual(counts["message"]["done"], 1)
                self.assertEqual(counts["message"]["processing"], 1)
            finally:
                PortalHandler.state_store = old_store
                PortalHandler.service = old_service
                PortalHandler.message_queue = old_queue
                PortalHandler.message_queue_event = old_event

    def test_lan_portal_state_store_checkpoints_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            store.put_backend_runtime("probe", {"ok": True})

            result = store.checkpoint_database()

            self.assertIn("before", result)
            self.assertIn("after", result)
            self.assertEqual(result["checkpoint"]["mode"], "TRUNCATE")
            self.assertGreaterEqual(result["reclaimed_bytes"], 0)

    def test_active_cache_store_imports_legacy_file_to_qt_items(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "active_cache.json"
            cache_path.write_text(
                json.dumps(
                    {
                        "version": 2,
                        "event": [],
                        "other": [
                            {
                                "data": {
                                    "active_item_id": "legacy-active-1",
                                    "record_id": "legacy-record-1",
                                    "notice_type": "维保通告",
                                    "text": "【维保通告】状态：开始\n\n【标题】旧通告",
                                }
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            state_store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            cache_store = ActiveCacheStore(str(cache_path), state_store)

            result = cache_store.migrate_legacy_cache_file_to_sqlite()
            payload = cache_store.load_payload()
            qt_items = state_store.list_qt_active_items()

            self.assertEqual(result["status"], "imported")
            self.assertEqual(result["imported"], 1)
            self.assertEqual(payload["other"][0]["data"]["record_id"], "legacy-record-1")
            self.assertEqual(qt_items[0]["active_item_id"], "legacy-active-1")

    def test_active_cache_store_does_not_overwrite_existing_qt_items_with_legacy_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "active_cache.json"
            cache_path.write_text(
                json.dumps(
                    {
                        "version": 2,
                        "event": [],
                        "other": [
                            {
                                "data": {
                                    "active_item_id": "stale-active",
                                    "record_id": "stale-record",
                                    "notice_type": "维保通告",
                                    "text": "旧文件中的过期通告",
                                }
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            state_store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            state_store.upsert_qt_active_item(
                {
                    "active_item_id": "current-active",
                    "record_id": "current-record",
                    "notice_type": "维保通告",
                    "text": "当前 SQLite 通告",
                },
                section="other",
                origin="qt",
            )
            cache_store = ActiveCacheStore(str(cache_path), state_store)

            result = cache_store.migrate_legacy_cache_file_to_sqlite()
            payload = cache_store.load_payload()
            qt_items = state_store.list_qt_active_items()

            self.assertEqual(result["status"], "skipped")
            self.assertEqual(result["reason"], "sqlite_already_initialized")
            self.assertEqual(payload["other"][0]["data"]["record_id"], "current-record")
            self.assertEqual([item["active_item_id"] for item in qt_items], ["current-active"])

    def test_backend_clipboard_notice_projects_to_active_upsert_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalHandler.state_store
            PortalHandler.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                controller = FastAPIPortalController()
                entry = controller._clipboard_entry_from_content(
                    "【维保通告】状态：开始\n\n【标题】测试测试测试\n\n【时间】2026-05-25 09:30~2026-05-25 18:30\n\n【位置】A楼"
                )
                self.assertIsNotNone(entry)
                result = controller._project_clipboard_entry_to_active(entry)
                qt_items = PortalHandler.state_store.list_qt_active_items()
                events = PortalHandler.state_store.lease_outbox_events(
                    "qt_action", limit=1, lease_seconds=5
                )

                self.assertTrue(result["ok"])
                self.assertEqual(result["item"]["active_item_id"], result["active_item_id"])
                self.assertEqual(result["item"]["payload"]["title"], "测试测试测试")
                self.assertEqual(len(qt_items), 1)
                self.assertEqual(qt_items[0]["payload"]["title"], "测试测试测试")
                self.assertIn("测试测试测试", qt_items[0]["payload"]["text"])
                self.assertEqual(events[0]["payload"]["kind"], "active_upsert")
            finally:
                PortalHandler.state_store = previous_store

    def test_backend_ongoing_reads_qt_active_items_without_snapshot_delay(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalHandler.state_store
            PortalHandler.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                controller = FastAPIPortalController()
                entry = controller._clipboard_entry_from_content(
                    "【维保通告】状态：开始\n\n"
                    "【标题】A楼测试测试测试维保\n\n"
                    "【时间】2026-05-25 09:30~2026-05-25 18:30\n\n"
                    "【位置】A楼"
                )
                result = controller._project_clipboard_entry_to_active(entry)

                ongoing_a = controller._get_ongoing("A")
                ongoing_b = controller._get_ongoing("B")

                self.assertTrue(result["ok"])
                self.assertEqual(len(ongoing_a), 1)
                self.assertEqual(ongoing_a[0]["title"], "A楼测试测试测试维保")
                self.assertEqual(ongoing_a[0]["work_type"], "maintenance")
                self.assertEqual(ongoing_b, [])
            finally:
                PortalHandler.state_store = previous_store

    def test_backend_ongoing_filters_deleted_qt_item_from_stale_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalHandler.state_store
            PortalHandler.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                stale_item = {
                    "active_item_id": "active-stale-delete",
                    "record_id": "rec-stale-delete",
                    "target_record_id": "rec-stale-delete",
                    "notice_type": "维保通告",
                    "work_type": "maintenance",
                    "title": "A楼已删除测试维保",
                    "building_codes": ["A"],
                    "text": "【维保通告】状态：开始\n\n【标题】A楼已删除测试维保",
                }
                PortalHandler.state_store.replace_ongoing_items([stale_item])
                PortalHandler.state_store.upsert_qt_active_item(
                    stale_item,
                    section="other",
                    origin="qt",
                )
                PortalHandler.state_store.delete_qt_active_item(
                    active_item_id="active-stale-delete",
                    record_id="rec-stale-delete",
                )

                controller = FastAPIPortalController()
                ongoing = controller._get_ongoing("A")

                self.assertEqual(ongoing, [])
            finally:
                PortalHandler.state_store = previous_store

    def test_parser_accepts_legacy_change_notice_marker(self):
        info = extract_event_info(
            "【变更通告】状态：开始\n\n"
            "【标题】测试测试测试变更\n\n"
            "【等级】低\n\n"
            "【时间】2026-05-25 09:30~2026-05-25 18:30"
        )

        self.assertIsNotNone(info)
        self.assertEqual(info["notice_type"], "设备变更")
        self.assertEqual(info["title"], "测试测试测试变更")
        self.assertEqual(info["status"], "开始")

    def test_backend_clipboard_target_record_requires_same_notice_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalHandler.state_store
            PortalHandler.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                PortalHandler.state_store.upsert_qt_active_item(
                    {
                        "active_item_id": "active-change-1",
                        "record_id": "rec-same-id",
                        "notice_type": "设备变更",
                        "text": "【设备变更】状态：开始\n\n【名称】原变更",
                    },
                    section="other",
                    origin="qt",
                )
                controller = FastAPIPortalController()
                entry = controller._clipboard_entry_from_content(
                    "【维保通告】状态：更新\n\n【标题】测试维保\n\n【时间】2026-05-25 09:30~2026-05-25 18:30"
                )
                entry["target_record_id"] = "rec-same-id"
                result = controller._project_clipboard_entry_to_active(entry)
                qt_items = PortalHandler.state_store.list_qt_active_items()

                self.assertTrue(result["ignored"])
                self.assertEqual(len(qt_items), 1)
                self.assertEqual(qt_items[0]["payload"]["notice_type"], "设备变更")
            finally:
                PortalHandler.state_store = previous_store

    def test_history_payload_imports_legacy_file_to_sqlite(self):
        with tempfile.TemporaryDirectory() as tmp:
            history_path = Path(tmp) / "history.json"
            history_path.write_text(
                json.dumps(
                    [
                        {
                            "record_id": "history-record-1",
                            "text": "【维保通告】状态：结束\n\n【标题】旧闭环",
                            "history_saved_at": int(time.time() * 1000),
                        }
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            state_store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            harness = _HistoryMigrationHarness(state_store)

            with patch("upload_event_module.ui.main_window_ui.HISTORY_FILE", str(history_path)):
                items = harness._load_history_payload()
            stored = state_store.get_document("qt_notice_history", "history")

            self.assertEqual(items[0]["record_id"], "history-record-1")
            self.assertEqual(stored["items"][0]["record_id"], "history-record-1")

    def test_history_payload_does_not_overwrite_existing_sqlite_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            history_path = Path(tmp) / "history.json"
            history_path.write_text(
                json.dumps(
                    [{"record_id": "stale-history", "text": "旧文件"}],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            state_store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            state_store.put_document(
                "qt_notice_history",
                "history",
                {"version": 1, "items": [{"record_id": "current-history", "text": "当前"}]},
            )
            harness = _HistoryMigrationHarness(state_store)

            with patch("upload_event_module.ui.main_window_ui.HISTORY_FILE", str(history_path)):
                items = harness._load_history_payload()

            self.assertEqual(items[0]["record_id"], "current-history")

    def test_portal_frontend_dist_defaults_to_ready_vue_and_supports_legacy_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dist = root / "frontend" / "dist"
            static = root / "static"
            (dist / "assets").mkdir(parents=True)
            (static / "assets").mkdir(parents=True)
            (dist / "index.html").write_text("vue", encoding="utf-8")
            (dist / "assets" / "app.js").write_text("dist", encoding="utf-8")
            (static / "assets" / "logo.png").write_text("static", encoding="utf-8")
            ready_marker = dist / "clipflow-frontend-ready.json"

            with patch.object(portal_server_module, "FRONTEND_DIST_DIR", dist), patch.object(
                portal_server_module, "STATIC_DIR", static
            ), patch.object(
                portal_server_module, "FRONTEND_READY_MARKER", ready_marker
            ), patch.dict(os.environ, {"CLIPFLOW_FRONTEND_LEGACY": ""}, clear=False):
                self.assertEqual(portal_server_module.portal_index_file(), static / "index.html")
                self.assertEqual(
                    portal_server_module.portal_asset_file(Path("logo.png")),
                    static / "assets" / "logo.png",
                )

            ready_marker.write_text(
                json.dumps({"app": "clipflow-lan-portal", "productionReady": True}),
                encoding="utf-8",
            )

            with patch.object(portal_server_module, "FRONTEND_DIST_DIR", dist), patch.object(
                portal_server_module, "STATIC_DIR", static
            ), patch.object(
                portal_server_module, "FRONTEND_READY_MARKER", ready_marker
            ), patch.dict(os.environ, {"CLIPFLOW_FRONTEND_LEGACY": ""}, clear=False):
                self.assertEqual(portal_server_module.portal_index_file(), dist / "index.html")
                self.assertEqual(
                    portal_server_module.portal_asset_file(Path("app.js")),
                    dist / "assets" / "app.js",
                )
                self.assertEqual(
                    portal_server_module.portal_asset_file(Path("logo.png")),
                    static / "assets" / "logo.png",
                )

            with patch.object(portal_server_module, "FRONTEND_DIST_DIR", dist), patch.object(
                portal_server_module, "STATIC_DIR", static
            ), patch.object(
                portal_server_module, "FRONTEND_READY_MARKER", ready_marker
            ), patch.dict(os.environ, {"CLIPFLOW_FRONTEND_LEGACY": "1"}, clear=False):
                self.assertEqual(portal_server_module.portal_index_file(), static / "index.html")

    def test_lan_portal_state_store_replaces_source_scope_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            record = _build_record(
                "rec-a",
                "A楼",
                "过滤网维护",
                f"{dt.datetime.now().month}月",
                maintenance_cycle="每月",
            )
            zhihang = _build_zhihang_change_record(
                "zh-a", title="A楼智航变更"
            )

            store.replace_source_scope_snapshot(
                "A",
                records=[record],
                zhihang_records=[zhihang],
                meta={"last_loaded_at": "2026-05-20 19:00:00"},
            )
            snapshot = store.get_source_scope_snapshot("A")

            self.assertTrue(snapshot["exists"])
            self.assertEqual(snapshot["records"][0]["record_id"], "rec-a")
            self.assertEqual(snapshot["zhihang_records"][0]["record_id"], "zh-a")
            self.assertEqual(
                snapshot["meta"]["last_loaded_at"], "2026-05-20 19:00:00"
            )

            store.replace_source_scope_snapshot("A", records=[], zhihang_records=[])
            snapshot = store.get_source_scope_snapshot("A")
            self.assertTrue(snapshot["exists"])
            self.assertEqual(snapshot["records"], [])
            self.assertEqual(snapshot["zhihang_records"], [])

    def test_query_records_reads_sqlite_scope_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            record = _build_record(
                "rec-a",
                "A楼",
                "过滤网维护",
                f"{dt.datetime.now().month}月",
                maintenance_cycle="每月",
            )
            service._state_store.replace_source_scope_snapshot(
                "A",
                records=[record],
                zhihang_records=[],
                meta={"last_loaded_at": "2026-05-20 19:00:00"},
            )
            service._records = []

            payload = service.query_records(scope="A", ongoing_items=[])

            self.assertEqual(payload["records"][0]["record_id"], "rec-a")
            self.assertEqual(payload["records"][0]["display_fields"]["维护周期"], "每月")
            self.assertEqual(payload["last_loaded_at"], "2026-05-20 19:00:00")

    def test_obsolete_empty_source_warning_is_not_shown_from_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            record = _build_record(
                "rec-a",
                "A楼",
                "过滤网维护",
                f"{dt.datetime.now().month}月",
                maintenance_cycle="每月",
            )
            stale_warning = (
                "源表后台同步失败: 飞书接口 HTTP失败: 404 Client Error: "
                "Not Found for url: https://open.feishu.cn/open-apis/bitable/v1/apps//tables//fields?page_size=500"
            )
            service._state_store.replace_source_scope_snapshot(
                "A",
                records=[record],
                zhihang_records=[],
                meta={
                    "last_loaded_at": "2026-05-20 19:00:00",
                    "warnings": [stale_warning, "检修源表同步失败: token 失效"],
                },
            )

            payload = service.query_records(scope="A", ongoing_items=[])

            self.assertNotIn(stale_warning, payload["warnings"])
            self.assertEqual(payload["warnings"], ["检修源表同步失败: token 失效"])

    def test_repair_link_task_is_scheduled_after_repair_start_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            prepared = {
                "action": "start",
                "work_type": "repair",
                "record_id": "source-repair-1",
                "source_app_token": "source-app",
                "source_table_id": "source-table",
            }

            service._schedule_repair_link_task_after_success(
                prepared,
                action="start",
                target_record_id="target-repair-1",
                job_id="job-1",
            )

            tasks = service._state_store.list_due_repair_link_tasks(
                now=time.time() + 71 * 60,
                limit=10,
            )
            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0]["source_record_id"], "source-repair-1")
            self.assertEqual(tasks[0]["target_record_id"], "target-repair-1")
            self.assertEqual(tasks[0]["sync_table_id"], "tblSA9euoote8aCA")

    def test_process_due_repair_link_task_links_source_record_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            source_record_id = "source-repair-1"
            target_record_id = "target-repair-1"
            sync_record_id = "sync-repair-1"
            encoded_source_id = base64.b64encode(
                f"7612849140323470524:{target_record_id}:hash:1".encode("utf-8")
            ).decode("ascii")
            service._state_store.upsert_repair_link_task(
                {
                    "task_key": f"repair_link:{source_record_id}:{target_record_id}",
                    "status": "pending",
                    "source_app_token": "source-app",
                    "source_table_id": "source-table",
                    "source_record_id": source_record_id,
                    "sync_app_token": "source-app",
                    "sync_table_id": "sync-table",
                    "target_app_token": "target-app",
                    "target_table_id": "target-table",
                    "target_record_id": target_record_id,
                    "link_field_name": "设备检修关联",
                    "due_at": time.time() - 1,
                    "attempts": 0,
                    "max_attempts": 12,
                }
            )
            updates = []

            def fake_request(path, *, params=None, app_token=None, table_id=None):
                if path == "records":
                    return {
                        "data": {
                            "items": [
                                {
                                    "record_id": sync_record_id,
                                    "fields": {"SourceID": encoded_source_id},
                                }
                            ],
                            "has_more": False,
                        }
                    }
                if path == f"records/{source_record_id}":
                    return {
                        "data": {
                            "record": {
                                "record_id": source_record_id,
                                "fields": {"设备检修关联": []},
                            }
                        }
                    }
                raise AssertionError(path)

            def fake_patch(**kwargs):
                updates.append(kwargs)
                return {"code": 0, "msg": "success"}

            service._request_json = fake_request
            service._patch_record_fields = fake_patch

            stats = service.process_due_repair_link_tasks(limit=3)

            self.assertEqual(stats["linked"], 1)
            self.assertEqual(len(updates), 1)
            self.assertEqual(updates[0]["record_id"], source_record_id)
            self.assertEqual(
                updates[0]["fields"],
                {"设备检修关联": {"link_record_ids": [sync_record_id]}},
            )
            self.assertEqual(
                service._state_store.list_due_repair_link_tasks(
                    now=time.time() + 3600,
                    limit=10,
                ),
                [],
            )

    def test_repair_link_task_claim_has_recoverable_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            store.upsert_repair_link_task(
                {
                    "task_key": "repair_link:source-1:target-1",
                    "status": "pending",
                    "source_app_token": "source-app",
                    "source_table_id": "source-table",
                    "source_record_id": "source-1",
                    "sync_app_token": "source-app",
                    "sync_table_id": "sync-table",
                    "target_app_token": "target-app",
                    "target_table_id": "target-table",
                    "target_record_id": "target-1",
                    "link_field_name": "设备检修关联",
                    "due_at": 999,
                    "attempts": 0,
                    "max_attempts": 18,
                }
            )

            first = store.claim_due_repair_link_tasks(now=1000, lease_seconds=300)
            second = store.claim_due_repair_link_tasks(now=1001, lease_seconds=300)
            recovered = store.claim_due_repair_link_tasks(now=1301, lease_seconds=300)

            self.assertEqual(len(first), 1)
            self.assertEqual(second, [])
            self.assertEqual(len(recovered), 1)

    def test_repair_link_task_reschedules_when_sync_record_is_not_ready(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            source_record_id = "source-repair-1"
            target_record_id = "target-repair-1"
            service._state_store.upsert_repair_link_task(
                {
                    "task_key": f"repair_link:{source_record_id}:{target_record_id}",
                    "status": "pending",
                    "source_app_token": "source-app",
                    "source_table_id": "source-table",
                    "source_record_id": source_record_id,
                    "sync_app_token": "source-app",
                    "sync_table_id": "sync-table",
                    "target_app_token": "target-app",
                    "target_table_id": "target-table",
                    "target_record_id": target_record_id,
                    "link_field_name": "设备检修关联",
                    "due_at": time.time() - 1,
                    "attempts": 0,
                    "max_attempts": 18,
                }
            )

            def fake_request(path, *, params=None, app_token=None, table_id=None):
                if path == "records":
                    return {"data": {"items": [], "has_more": False}}
                raise AssertionError(path)

            service._request_json = fake_request
            before = time.time()

            stats = service.process_due_repair_link_tasks(limit=3)

            self.assertEqual(stats["rescheduled"], 1)
            self.assertEqual(
                service._state_store.list_due_repair_link_tasks(
                    now=before + 9 * 60,
                    limit=10,
                ),
                [],
            )
            tasks = service._state_store.list_due_repair_link_tasks(
                now=before + 11 * 60,
                limit=10,
            )
            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0]["attempts"], 1)
            self.assertIn("同步表尚未出现目标检修记录", tasks[0]["last_error"])

    def test_server_get_ongoing_reads_sqlite_before_qt_callback(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            store.replace_ongoing_items(
                [
                    {
                        "active_item_id": "active-a",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "title": "A楼维保",
                        "building": "A楼",
                        "building_codes": ["A"],
                    },
                    {
                        "active_item_id": "active-b",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "title": "B楼维保",
                        "building": "B楼",
                        "building_codes": ["B"],
                    },
                ]
            )

            def unexpected_callback(scope):
                raise AssertionError(f"Qt callback should not be called: {scope}")

            dummy = type(
                "_DummyPortalHandler",
                (),
                {"service": _TestMaintenancePortalService()},
            )()
            with patch.object(PortalHandler, "state_store", store), patch.object(
                PortalHandler, "ongoing_callback", unexpected_callback
            ):
                result = PortalHandler._get_ongoing(dummy, "A")

            self.assertEqual([item["active_item_id"] for item in result], ["active-a"])

    def test_append_events_import_legacy_jsonl_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_path = root / "system_alerts.jsonl"
            event_path.write_text(
                "\n".join(
                    [
                        json.dumps({"ts": 1716000000000, "event_code": "old-1"}),
                        "{bad json",
                        json.dumps({"ts": 1716000001000, "event_code": "old-2"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            store = LanPortalStateStore(root / "lan_portal_state.sqlite3")

            first = store.import_jsonl_events_once("system_alerts", event_path)
            second = store.import_jsonl_events_once("system_alerts", event_path)
            events = store.list_events_after("system_alerts", 0)

            self.assertEqual(first["imported"], 2)
            self.assertEqual(first["skipped"], 1)
            self.assertTrue(second["already_imported"])
            self.assertEqual([item["payload"]["event_code"] for item in events], ["old-1", "old-2"])
            self.assertEqual(store.get_last_event_id("system_alerts"), events[-1]["id"])

    def test_notice_daily_summary_migrates_to_sqlite_without_overwriting_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = self._new_temp_service(root)
            summary_dir = root / "lan_template_daily_summary"
            summary_dir.mkdir(parents=True, exist_ok=True)
            legacy_path = summary_dir / "2026-05-18.json"
            legacy_payload = {
                "date": "2026-05-18",
                "items": [
                    {
                        "work_type": "maintenance",
                        "title": "旧维保",
                        "building": "A楼",
                        "building_codes": ["A"],
                        "started_at": "2026-05-18 09:30",
                    }
                ],
            }
            original = json.dumps(legacy_payload, ensure_ascii=False, indent=2)
            legacy_path.write_text(original, encoding="utf-8")

            loaded = service._load_day_summary_locked("2026-05-18")
            loaded["items"].append(
                {
                    "work_type": "change",
                    "title": "SQLite 新变更",
                    "building": "A楼",
                    "building_codes": ["A"],
                    "started_at": "2026-05-18 10:30",
                }
            )
            service._save_day_summary_locked(loaded, "2026-05-18")

            stored = service._state_store.get_document(
                "notice_daily_summary", "2026-05-18"
            )
            self.assertEqual(len(stored["items"]), 2)
            self.assertEqual(legacy_path.read_text(encoding="utf-8"), original)

    def test_notice_memory_and_work_status_migrate_to_sqlite(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = self._new_temp_service(root)

            memory_dir = root / "lan_template_memory"
            memory_dir.mkdir(parents=True, exist_ok=True)
            memory_path = service._building_memory_path("A楼")
            memory_payload = {
                "building": "A楼",
                "items": {
                    "过滤网维护": {
                        "maintenance_total": "过滤网维护",
                        "location": "旧位置",
                        "content": "旧内容",
                        "reason": "旧原因",
                        "impact": "旧影响",
                    }
                },
            }
            memory_path.write_text(
                json.dumps(memory_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            service._records = [_build_record("m1", "A楼", "过滤网维护", "5月")]
            memory = service._get_record_memory(service._records[0])
            self.assertEqual(memory["location"], "旧位置")

            service._remember_draft_fields(
                building="A楼",
                maintenance_total="过滤网维护",
                location="新位置",
                content="新内容",
                reason="新原因",
                impact="新影响",
            )
            stored_memory = service._state_store.get_document(
                "notice_memory", service._building_memory_key("A楼")
            )
            self.assertEqual(
                stored_memory["items"]["过滤网维护"]["location"], "新位置"
            )

            status_dir = root / "lan_template_work_status"
            status_dir.mkdir(parents=True, exist_ok=True)
            status_path = status_dir / "A.json"
            original = json.dumps(
                {
                    "version": 1,
                    "building": "A楼",
                    "building_code": "A",
                    "items": [
                        {
                            "work_type": "maintenance",
                            "source_record_id": "m1",
                            "title": "EA118机房A楼过滤网维护",
                            "building": "A楼",
                            "building_codes": ["A"],
                            "started_at": "2026-05-18 09:30",
                            "status": "进行中",
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
            status_path.write_text(original, encoding="utf-8")
            items = service._load_work_status_items_locked("A")
            self.assertEqual(items[0]["source_record_id"], "m1")
            service._upsert_work_status_item_locked(
                {
                    "work_type": "maintenance",
                    "source_record_id": "m1",
                    "title": "EA118机房A楼过滤网维护",
                    "building": "A楼",
                    "building_code": "A",
                    "building_codes": ["A"],
                    "ended_at": "2026-05-18 18:30",
                },
                action="end",
                now="2026-05-18 18:30",
            )
            stored_status = service._state_store.get_document("notice_work_status", "A")
            self.assertEqual(stored_status["items"][0]["status"], "已结束")
            self.assertEqual(status_path.read_text(encoding="utf-8"), original)

    def test_action_jobs_persist_in_sqlite_and_restart_recovers_pre_send_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = self._new_temp_service(root)
            job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "scope": "A",
                    "record_id": "m1",
                    "work_type": "maintenance",
                    "operation_id": "job-persist",
                }
            )
            self.assertTrue(should_start)
            self.assertEqual(service.get_job(job_id)["phase"], "accepted")

            restarted = self._new_temp_service(root)
            restored = restarted.get_job(job_id)
            self.assertEqual(restored["phase"], "accepted")
            self.assertTrue(restored["restart_recovered"])
            self.assertEqual(restarted.recoverable_action_job_ids(), [job_id])

    def test_qt_active_cache_migrates_to_sqlite_without_overwriting_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_file = root / "active_cache.json"
            legacy_payload = {
                "version": 2,
                "saved_at": 1,
                "event": [],
                "other": [
                    {
                        "data": {
                            "record_id": "active-1",
                            "notice_type": "维保通告",
                            "text": "旧通告",
                        }
                    }
                ],
                "clipboard_queue": [],
            }
            original = json.dumps(legacy_payload, ensure_ascii=False, indent=2)
            cache_file.write_text(original, encoding="utf-8")
            store = LanPortalStateStore(root / "lan_portal_state.sqlite3")
            cache_store = ActiveCacheStore(str(cache_file), state_store=store)

            payload = cache_store.load_payload()
            self.assertEqual(payload["other"][0]["data"]["record_id"], "active-1")
            self.assertTrue(
                cache_store.patch_record_fields(
                    record_id="active-1", patch={"text": "SQLite 通告"}
                )
            )
            stored = store.get_document("qt_active_cache", "active_cache")

            self.assertEqual(
                stored["other"][0]["data"]["text"],
                "SQLite 通告",
            )
            self.assertEqual(cache_file.read_text(encoding="utf-8"), original)

    def test_qt_history_migrates_to_sqlite_without_overwriting_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            history_file = root / "history.json"
            legacy_payload = [
                {
                    "record_id": "history-1",
                    "notice_type": "维保通告",
                    "text": "旧历史",
                    "history_saved_at": int(time.time() * 1000),
                }
            ]
            original = json.dumps(legacy_payload, ensure_ascii=False, indent=2)
            history_file.write_text(original, encoding="utf-8")
            harness = _HistoryHarness()
            harness._lan_portal_state_store = LanPortalStateStore(
                root / "lan_portal_state.sqlite3"
            )
            harness.last_history_mtime = 0

            with patch(
                "upload_event_module.ui.main_window_ui.HISTORY_FILE",
                str(history_file),
            ):
                loaded = harness.load_all_history()
                harness.save_to_history_file(
                    {
                        "record_id": "history-2",
                        "notice_type": "设备变更",
                        "text": "SQLite 历史",
                    }
                )

            stored = harness._lan_portal_state_store.get_document(
                "qt_notice_history", "history"
            )
            self.assertEqual(loaded[0]["record_id"], "history-1")
            self.assertEqual(stored["items"][0]["record_id"], "history-2")
            self.assertEqual(stored["items"][1]["record_id"], "history-1")
            self.assertEqual(history_file.read_text(encoding="utf-8"), original)

    def test_config_migrates_to_sqlite_and_stops_writing_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_file = root / "config.json"
            original_payload = {
                "feishu_app_id": "legacy-app",
                "feishu_app_secret": "legacy-secret",
                "lan_template_portal_port": 18888,
            }
            original_text = json.dumps(original_payload, ensure_ascii=False, indent=2)
            config_file.write_text(original_text, encoding="utf-8")

            def fake_data_path(name):
                return str(root / name)

            with patch.object(config_module, "CONFIG_FILE", str(config_file)), patch.object(
                config_module,
                "migrate_legacy_data_file",
                side_effect=lambda name: str(root / name),
            ), patch(
                "lan_bitable_template_portal.state_store.get_data_file_path",
                side_effect=fake_data_path,
            ):
                manager = ConfigManager()
                self.assertEqual(manager.app_id, "legacy-app")
                store = LanPortalStateStore(root / "lan_portal_state.sqlite3")
                self.assertEqual(store.get_settings()["feishu_app_id"], "legacy-app")

                changed_legacy = {
                    "feishu_app_id": "changed-json",
                    "lan_template_portal_port": 19999,
                }
                changed_text = json.dumps(changed_legacy, ensure_ascii=False, indent=2)
                config_file.write_text(changed_text, encoding="utf-8")

                manager.load()
                self.assertEqual(manager.app_id, "legacy-app")
                self.assertEqual(manager.lan_template_portal_port, 18888)

                self.assertTrue(manager.save(app_id="sqlite-app"))
                self.assertEqual(store.get_settings()["feishu_app_id"], "sqlite-app")
                self.assertEqual(config_file.read_text(encoding="utf-8"), changed_text)

    def test_reconcile_orphan_started_items_migrates_legacy_work_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = self._new_temp_service(root, service_cls=_TargetLookupService)
            status_dir = root / "lan_template_work_status"
            status_dir.mkdir(parents=True, exist_ok=True)
            (status_dir / "A.json").write_text(
                json.dumps(
                    {
                        "version": 1,
                        "building": "A楼",
                        "building_code": "A",
                        "items": [
                            {
                                "work_type": "maintenance",
                                "notice_type": "维保通告",
                                "source_record_id": "legacy-m1",
                                "target_record_id": "missing-target",
                                "title": "A楼遗留维保",
                                "building": "A楼",
                                "building_codes": ["A"],
                                "started_at": "2026-05-18 09:30",
                                "status": "进行中",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            removed = service.reconcile_orphan_started_items(
                scope="A", ongoing_items=[]
            )
            items = service._load_work_status_items_locked("A")

            self.assertEqual(removed["removed"], 1)
            self.assertEqual(items, [])

    def test_successful_actions_persist_building_work_status_for_month_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [
                _build_record(
                    "rec1",
                    "A楼",
                    "过滤网维护",
                    "4月",
                    maintenance_cycle="每月",
                )
            ]

            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "scope": "A",
                    "record_id": "rec1",
                    "specialty": "全部",
                    "start_time": "2026-04-30T09:30",
                    "end_time": "2026-04-30T18:30",
                    "location": "测试位置",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备开始",
                    "operation_id": "op-start",
                }
            )
            self.assertTrue(should_start)
            service.prepare_action_job(start_job_id)
            service.mark_action_upload_result(
                start_job_id,
                success=True,
                record_id="bitable-rec-1",
                active_item_id="active-1",
            )

            end_job_id, should_start = service.create_action_job(
                {
                    "action": "end",
                    "scope": "A",
                    "active_item_id": "active-1",
                    "title": "EA118机房A楼过滤网维护",
                    "building": "A楼",
                    "specialty": "电气",
                    "start_time": "2026-04-30T09:30",
                    "end_time": "2026-04-30T18:30",
                    "location": "测试位置",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "已结束",
                    "operation_id": "op-end",
                }
            )
            self.assertTrue(should_start)
            service.prepare_action_job(end_job_id)
            service.mark_action_upload_result(
                end_job_id,
                success=True,
                record_id="bitable-rec-1",
                active_item_id="active-1",
            )

            result = service.query_records(month="4月", scope="A")
            summary = result["records"][0]["work_summary"]
            self.assertEqual(summary["status"], "已结束")
            self.assertEqual(summary["source_record_id"], "rec1")
            self.assertEqual(summary["active_item_id"], "active-1")
            self.assertEqual(summary["feishu_record_id"], "bitable-rec-1")
            self.assertEqual(summary["maintenance_cycle"], "每月")
            self.assertRegex(summary["completed_date"], r"^\d{4}-\d{2}-\d{2}$")
            self.assertEqual(summary["completed_date"], summary["ended_at"][:10])
            work_status_items = service._load_work_status_items_locked("A")
            self.assertEqual(work_status_items[0]["maintenance_cycle"], "每月")
            self.assertEqual([item["action"] for item in summary["actions"]], ["start", "end"])

    def test_orphan_started_item_is_pruned_when_qt_and_target_record_are_gone(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), service_cls=_TargetLookupService)
            service._records = [_build_record("rec1", "A楼", "过滤网维护", "5月")]

            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "scope": "A",
                    "record_id": "rec1",
                    "specialty": "全部",
                    "start_time": "2026-05-15T09:30",
                    "end_time": "2026-05-15T18:30",
                    "location": "误发位置",
                    "content": "误发内容",
                    "reason": "误发原因",
                    "impact": "测试影响",
                    "progress": "准备开始",
                    "operation_id": "orphan-start",
                }
            )
            self.assertTrue(should_start)
            service.prepare_action_job(start_job_id)
            service.mark_action_upload_result(
                start_job_id,
                success=True,
                record_id="target-deleted",
                active_item_id="active-deleted",
            )

            removed = service.reconcile_orphan_started_items(
                scope="A", ongoing_items=[]
            )
            result = service.query_records(month="5月", scope="A", ongoing_items=[])
            daily = service.get_daily_summary(scope="A", ongoing_items=[])

            self.assertEqual(removed["removed"], 1)
            self.assertEqual(result["records"][0]["work_summary"], {})
            self.assertEqual(daily["items"], [])

    def test_started_item_is_kept_when_target_record_still_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), service_cls=_TargetLookupService)
            service._records = [_build_record("rec1", "A楼", "过滤网维护", "5月")]
            service.target_record_ids = {"target-existing"}

            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "scope": "A",
                    "record_id": "rec1",
                    "specialty": "全部",
                    "start_time": "2026-05-15T09:30",
                    "end_time": "2026-05-15T18:30",
                    "location": "测试位置",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备开始",
                    "operation_id": "kept-start",
                }
            )
            self.assertTrue(should_start)
            service.prepare_action_job(start_job_id)
            service.mark_action_upload_result(
                start_job_id,
                success=True,
                record_id="target-existing",
                active_item_id="active-missing",
            )

            removed = service.reconcile_orphan_started_items(
                scope="A", ongoing_items=[]
            )
            result = service.query_records(month="5月", scope="A", ongoing_items=[])

            self.assertEqual(removed["removed"], 0)
            self.assertEqual(result["records"][0]["work_summary"]["status"], "进行中")

    def test_orphan_reconcile_suppresses_transient_feishu_fail_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("rec1", "A楼", "过滤网维护", "5月")]
            start_job_id, _ = service.create_action_job(
                {
                    "action": "start",
                    "scope": "A",
                    "record_id": "rec1",
                    "specialty": "全部",
                    "start_time": "2026-05-15T09:30",
                    "end_time": "2026-05-15T18:30",
                    "location": "测试位置",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备开始",
                    "operation_id": "transient-fail-start",
                }
            )
            service.prepare_action_job(start_job_id)
            service.mark_action_upload_result(
                start_job_id,
                success=True,
                record_id="target-unknown",
                active_item_id="active-missing",
            )

            with patch.object(
                service,
                "_target_records_for_notice_type",
                side_effect=PortalError("飞书接口失败: code=1254002, msg=Fail"),
            ):
                removed = service.reconcile_orphan_started_items(
                    scope="A", ongoing_items=[]
                )

            result = service.query_records(month="5月", scope="A", ongoing_items=[])
            self.assertEqual(removed["removed"], 0)
            self.assertEqual(result["records"][0]["work_summary"]["status"], "进行中")
            self.assertFalse(
                any("目标表孤儿状态校验失败" in item for item in service._load_warnings)
            )

    def test_deleted_ongoing_item_clears_today_summary_and_work_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("rec1", "A楼", "过滤网维护", "5月")]

            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "scope": "A",
                    "record_id": "rec1",
                    "specialty": "全部",
                    "start_time": "2026-05-15T09:30",
                    "end_time": "2026-05-15T18:30",
                    "location": "误发位置",
                    "content": "误发内容",
                    "reason": "误发原因",
                    "impact": "测试影响",
                    "progress": "准备开始",
                    "operation_id": "delete-cleanup-start",
                }
            )
            self.assertTrue(should_start)
            service.prepare_action_job(start_job_id)
            service.mark_action_upload_result(
                start_job_id,
                success=True,
                record_id="target-delete-cleanup",
                active_item_id="active-delete-cleanup",
            )

            removed = service.discard_deleted_ongoing_state(
                {
                    "scope": "A",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "active_item_id": "active-delete-cleanup",
                    "source_record_id": "rec1",
                    "record_id": "target-delete-cleanup",
                    "building": "A楼",
                    "building_codes": ["A"],
                },
                scope="A",
            )
            result = service.query_records(month="5月", scope="A", ongoing_items=[])
            daily = service.get_daily_summary(scope="A", ongoing_items=[])

            self.assertEqual(removed["work_status_removed"], 1)
            self.assertEqual(removed["daily_summary_removed"], 1)
            self.assertEqual(result["records"][0]["work_summary"], {})
            self.assertEqual(daily["items"], [])

    def test_daily_summary_backfills_historical_completion_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summary_dir = root / "lan_template_daily_summary"
            summary_dir.mkdir(parents=True)
            (summary_dir / "2026-04-30.json").write_text(
                """
{
  "date": "2026-04-30",
  "items": [
    {
      "key": "source:rec1",
      "fallback_key": "title:A楼:EA118机房A楼过滤网维护",
      "source_record_id": "rec1",
      "active_item_id": "active-1",
      "feishu_record_id": "bitable-rec-1",
      "title": "EA118机房A楼过滤网维护",
      "building": "A楼",
      "building_code": "A",
      "specialty": "电气",
      "status": "已结束",
      "started_at": "2026-04-30 09:30",
      "ended_at": "2026-04-30 18:30",
      "actions": [
        {"action": "start", "label": "开始", "time": "2026-04-30 09:30", "job_id": "job-start"},
        {"action": "end", "label": "结束", "time": "2026-04-30 18:30", "job_id": "job-end"}
      ]
    }
  ]
}
""".strip(),
                encoding="utf-8",
            )
            service = self._new_temp_service(root)
            service._records = [_build_record("rec1", "A楼", "过滤网维护", "4月")]

            result = service.query_records(month="4月", scope="A")
            summary = result["records"][0]["work_summary"]
            self.assertEqual(summary["status"], "已结束")
            self.assertEqual(summary["completed_date"], "2026-04-30")
            self.assertEqual(summary["ended_at"], "2026-04-30 18:30")

    def test_maintenance_source_ongoing_is_displayed_and_updates_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [
                _build_record("m-running", "A楼", "过滤网维护", "5月", status="进行中")
            ]
            service._upsert_work_status_item_locked(
                {
                    "source_record_id": "m-running",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "target_record_id": "target-m-running",
                    "feishu_record_id": "target-m-running",
                    "title": "EA118机房A楼过滤网维护",
                    "building": "A楼",
                    "building_code": "A",
                    "building_codes": ["A"],
                    "specialty": "电气",
                    "started_at": "2026-05-08 09:30",
                },
                action="start",
                now="2026-05-08 09:30",
            )

            result = service.query_records(month="5月", scope="A", ongoing_items=[])
            self.assertEqual([item["record_id"] for item in result["records"]], ["m-running"])
            self.assertEqual(result["records"][0]["source_progress"], "进行中")

            job_id, should_start = service.create_action_job(
                {
                    "action": "update",
                    "scope": "A",
                    "source_record_id": "m-running",
                    "title": "EA118机房A楼过滤网维护",
                    "building": "A楼",
                    "specialty": "电气",
                    "start_time": "2026-05-08T09:30",
                    "end_time": "2026-05-08T18:30",
                    "location": "A楼",
                    "content": "更新内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "更新进展",
                    "operation_id": "maintenance-update",
                }
            )
            self.assertTrue(should_start)
            prepared = service.prepare_action_job(job_id)
            self.assertEqual(prepared["action"], "update")
            self.assertEqual(prepared["record_id"], "m-running")
            self.assertEqual(prepared["target_record_id"], "target-m-running")

    def test_change_records_use_precise_scope_and_allowed_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("m1", "A楼", "过滤网维护", "5月")]
            service._change_records = [
                _build_change_record("c1", building="A楼", progress="未开始", title="单楼变更"),
                _build_change_record("c2", building="A楼、B楼", progress="未开始", title="园区变更"),
                _build_change_record("c3", building="A楼、B楼", progress="进行中", title="进行中园区变更"),
                _build_change_record("c4", building="A楼、B楼", progress="退回", title="退回变更"),
                _build_change_record("c5", building="A楼、B楼", progress="已结束", title="已结束变更"),
            ]

            with patch.object(
                service, "_target_records_for_notice_type", return_value=[]
            ):
                campus = service.query_records(month="5月", scope="CAMPUS")
            self.assertEqual(
                [item["record_id"] for item in campus["records"]],
                ["c2", "c3"],
            )
            self.assertEqual(campus["ongoing"], [])

            with patch.object(
                service, "_target_records_for_notice_type", return_value=[]
            ):
                single = service.query_records(month="5月", scope="A")
            self.assertEqual(
                [item["record_id"] for item in single["records"]],
                ["m1", "c1"],
            )

    def test_workbench_records_are_limited_to_current_and_previous_month(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_start, previous_start = service._recent_month_starts()
            old_start = previous_start - dt.timedelta(days=1)
            current_label = f"{current_start.month}月"
            previous_label = f"{previous_start.month}月"
            old_label = f"{old_start.month}月"
            current_key = current_start.strftime("%Y-%m")
            previous_key = previous_start.strftime("%Y-%m")
            old_key = old_start.strftime("%Y-%m")
            service._records = [
                _build_record("m-current", "A楼", "当前月维保", current_label),
                _build_record("m-previous", "A楼", "上月维保", previous_label),
                _build_record("m-old", "A楼", "旧月维保", old_label),
            ]
            service._change_records = [
                _build_change_record(
                    "c-current",
                    building="A楼",
                    progress="未开始",
                    start_time=f"{current_key}-08 09:00",
                    end_time=f"{current_key}-08 18:00",
                ),
                _build_change_record(
                    "c-previous",
                    building="A楼",
                    progress="未开始",
                    start_time=f"{previous_key}-08 09:00",
                    end_time=f"{previous_key}-08 18:00",
                ),
                _build_change_record(
                    "c-old",
                    building="A楼",
                    progress="未开始",
                    start_time=f"{old_key}-08 09:00",
                    end_time=f"{old_key}-08 18:00",
                ),
            ]
            service._repair_records = [
                _build_repair_record("r-current", building="A楼", fault_time=f"{current_key}-08 08:20"),
                _build_repair_record("r-previous", building="A楼", fault_time=f"{previous_key}-08 08:20"),
                _build_repair_record("r-old", building="A楼", fault_time=f"{old_key}-08 08:20"),
            ]

            result = service.query_records(month=RECENT_MONTH_FILTER_LABEL, scope="A")
            self.assertEqual(
                [item["record_id"] for item in result["records"]],
                [
                    "m-current",
                    "m-previous",
                    "c-current",
                    "c-previous",
                    "r-current",
                    "r-previous",
                ],
            )

            previous = service.query_records(month=previous_label, scope="A")
            self.assertEqual(
                [item["record_id"] for item in previous["records"]],
                ["m-previous", "c-previous", "r-previous"],
            )

    def test_change_start_rejects_source_progress_that_is_not_not_started(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record("c-e", building="E楼", progress="进行中", title="E楼进行中变更")
            ]

            result = service.query_records(month="5月", scope="E")
            self.assertEqual([item["record_id"] for item in result["records"]], ["c-e"])

            job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "work_type": "change",
                    "scope": "E",
                    "record_id": "c-e",
                    "operation_id": "change-e-start",
                }
            )
            self.assertTrue(should_start)
            with self.assertRaises(PortalError):
                service.prepare_action_job(job_id)

    def test_change_source_ongoing_update_resolves_target_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record("c-e", building="E楼", progress="进行中", title="E楼进行中变更")
            ]
            target_records = [
                {
                    "record_id": "target-change-e",
                    "display_fields": {
                        "名称": "E楼进行中变更",
                        "变更状态": "开始",
                        "变更开始时间": "2026-05-08 09:00",
                        "楼栋": "E楼",
                    },
                }
            ]

            with patch.object(
                service, "_target_records_for_notice_type", return_value=target_records
            ):
                job_id, should_start = service.create_action_job(
                    {
                        "action": "update",
                        "work_type": "change",
                        "scope": "E",
                        "source_record_id": "c-e",
                        "title": "E楼进行中变更",
                        "building": "E楼",
                        "building_codes": ["E"],
                        "start_time": "2026-05-08T09:00",
                        "end_time": "2026-05-08T18:00",
                        "content": "更新内容",
                        "progress": "更新进展",
                        "operation_id": "change-e-update",
                    }
                )
                self.assertTrue(should_start)
                prepared = service.prepare_action_job(job_id)

            self.assertEqual(prepared["action"], "update")
            self.assertEqual(prepared["record_id"], "c-e")
            self.assertEqual(prepared["target_record_id"], "target-change-e")
            self.assertEqual(prepared["source_progress"], "进行中")

    def test_change_successful_action_persists_work_type_and_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record("c2", building="A楼、B楼", progress="未开始", title="园区变更")
            ]

            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "work_type": "change",
                    "scope": "CAMPUS",
                    "record_id": "c2",
                    "operation_id": "change-start",
                }
            )
            self.assertTrue(should_start)
            prepared = service.prepare_action_job(start_job_id)
            self.assertEqual(prepared["work_type"], WORK_TYPE_CHANGE)
            self.assertTrue(prepared["skip_personal_message"])
            service.mark_action_upload_result(
                start_job_id,
                success=True,
                record_id="change-target-1",
                active_item_id="active-change-1",
            )

            result = service.query_records(month="5月", scope="CAMPUS")
            summary = result["records"][0]["work_summary"]
            self.assertEqual(summary["work_type"], WORK_TYPE_CHANGE)
            self.assertEqual(summary["notice_type"], "设备变更")
            self.assertEqual(summary["source_record_id"], "c2")
            self.assertEqual(summary["feishu_record_id"], "change-target-1")
            self.assertEqual(summary["status"], "进行中")

    def test_repair_records_filter_placeholders_and_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record("r1", building="D楼", title="D楼UPS检修"),
                _build_repair_record("r2", building="D楼", title="——"),
                _build_repair_record("r3", building="CMDB唯一ID关联", title="无法识别楼栋"),
            ]

            result = service.query_records(month="5月", scope="D")

            self.assertEqual([item["record_id"] for item in result["records"]], ["r1"])
            self.assertEqual(result["records"][0]["work_type"], WORK_TYPE_REPAIR)
            self.assertEqual(result["records"][0]["notice_type"], "设备检修")
            self.assertEqual(result["records"][0]["building_codes"], ["D"])

    def test_repair_start_action_builds_overhaul_text_and_skips_personal_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record("r1", building="D楼", title="D楼UPS检修")
            ]

            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "work_type": "repair",
                    "scope": "D",
                    "record_id": "r1",
                    "specialty": "暖通",
                    "title": "手动检修标题",
                    "level": "低",
                    "start_time": "2026-05-08T23:50",
                    "end_time": "2026-05-08T08:20",
                    "expected_time": "2026-05-08T23:50",
                    "fault_time": "2026-05-08T08:20",
                    "location": "",
                    "content": "手动检修标题",
                    "reason": "测试故障原因",
                    "impact": "测试影响",
                    "progress": "",
                    "repair_device": "B-418-HUM-01恒湿机",
                    "repair_fault": "循环泵",
                    "fault_type": "设备故障",
                    "repair_mode": "厂家",
                    "discovery": "巡检发现",
                    "symptom": "加湿异常",
                    "solution": "更换循环泵",
                    "operation_id": "repair-start",
                }
            )

            self.assertTrue(should_start)
            prepared = service.prepare_action_job(start_job_id)
            self.assertEqual(prepared["work_type"], WORK_TYPE_REPAIR)
            self.assertEqual(prepared["notice_type"], "设备检修")
            self.assertEqual(prepared["recipients"], [])
            self.assertTrue(prepared["skip_personal_message"])
            self.assertIn("【设备检修】状态：开始", prepared["text"])
            self.assertIn("【标题】手动检修标题", prepared["text"])
            self.assertIn("【地点】", prepared["text"])
            self.assertIn("【紧急程度】低", prepared["text"])
            self.assertIn("【专业】暖通", prepared["text"])
            self.assertIn("【发现故障时间】2026-05-08 08:20", prepared["text"])
            self.assertIn("【期望完成时间】2026-05-08 23:50", prepared["text"])
            self.assertIn("【维修设备】B-418-HUM-01恒湿机", prepared["text"])
            self.assertIn("【维修故障】循环泵", prepared["text"])
            self.assertIn("【维修方式】厂家", prepared["text"])
            self.assertIn("【故障发现方式】巡检发现", prepared["text"])
            self.assertIn("【故障现象】加湿异常", prepared["text"])
            self.assertIn("【解决方案】更换循环泵", prepared["text"])
            self.assertIn("【完成情况】", prepared["text"])
            self.assertNotIn("【内容】", prepared["text"])
            self.assertEqual(prepared["location"], "")
            self.assertEqual(prepared["progress"], "")
            self.assertEqual(prepared["expected_time"], "2026-05-08 23:50")
            self.assertEqual(prepared["fault_time"], "2026-05-08 08:20")
            self.assertEqual(prepared["building_code"], "D")

    def test_repair_defaults_use_notice_title_formula_level_and_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record(
                    "r1",
                    building="D楼",
                    title="检修通告名称公式字段作为检修标题",
                    repair_name="维修名称不应作为检修标题",
                    event_description="事件描述不作为检修标题",
                    event_level="I2",
                    source="监控发现",
                )
            ]

            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "work_type": "repair",
                    "scope": "D",
                    "record_id": "r1",
                    "specialty": "全部",
                    "start_time": "2026-05-08T23:50",
                    "end_time": "2026-05-08T08:20",
                    "location": "",
                    "reason": "测试故障原因",
                    "impact": "",
                    "progress": "",
                    "operation_id": "repair-start-defaults",
                }
            )

            self.assertTrue(should_start)
            prepared = service.prepare_action_job(start_job_id)
            self.assertEqual(prepared["title"], "检修通告名称公式字段作为检修标题")
            self.assertEqual(prepared["level"], "中")
            self.assertEqual(prepared["discovery"], "监控发现")
            self.assertEqual(service._repair_level_from_event_level("I3"), "低")
            self.assertEqual(service._repair_level_from_event_level("I1"), "")
            self.assertIn("【标题】检修通告名称公式字段作为检修标题", prepared["text"])
            self.assertNotIn("维修名称不应作为检修标题", prepared["text"])
            self.assertNotIn("事件描述不作为检修标题", prepared["text"])
            self.assertIn("【紧急程度】中", prepared["text"])
            self.assertIn("【故障发现方式】监控发现", prepared["text"])

    def test_started_repair_source_record_is_displayed_as_update_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record(
                    "r1",
                    building="D楼",
                    title="D楼UPS检修",
                    started=True,
                    target_record_id="target-r1",
                )
            ]

            result = service.query_records(month="5月", scope="D")

            self.assertEqual([item["record_id"] for item in result["records"]], ["r1"])
            self.assertEqual(result["records"][0]["source_progress"], "进行中")
            self.assertEqual(result["ongoing"], [])

    def test_repair_source_ongoing_update_uses_linked_target_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record(
                    "r1",
                    building="D楼",
                    title="D楼UPS检修",
                    started=True,
                    target_record_id="target-r1",
                )
            ]

            job_id, should_start = service.create_action_job(
                {
                    "action": "update",
                    "work_type": "repair",
                    "scope": "D",
                    "source_record_id": "r1",
                    "title": "D楼UPS检修",
                    "building": "D楼",
                    "building_codes": ["D"],
                    "specialty": "电气",
                    "start_time": "2026-05-08T23:50",
                    "end_time": "2026-05-08T08:20",
                    "content": "D楼UPS检修",
                    "progress": "维修更新",
                    "operation_id": "repair-update",
                }
            )
            self.assertTrue(should_start)
            prepared = service.prepare_action_job(job_id)

            self.assertEqual(prepared["action"], "update")
            self.assertEqual(prepared["record_id"], "r1")
            self.assertEqual(prepared["target_record_id"], "target-r1")
            self.assertEqual(prepared["source_progress"], "进行中")

    def test_change_source_failure_does_not_block_maintenance_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), _ChangeSourceFailureService)
            service.refresh()

            result = service.query_records(month="5月", scope="A")
            self.assertEqual([item["record_id"] for item in result["records"]], ["m1"])
            self.assertIn("变更源表同步失败", result["warnings"][0])

    def test_start_action_uses_cached_records_without_forced_refresh(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("rec1", "A楼", "过滤网维护", "5月")]

            def fail_refresh():
                raise AssertionError("start action should not force source refresh")

            service.refresh = fail_refresh
            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "scope": "A",
                    "record_id": "rec1",
                    "specialty": "全部",
                    "start_time": "2026-05-08T09:30",
                    "end_time": "2026-05-08T18:30",
                    "location": "测试位置",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备开始",
                    "operation_id": "no-refresh-start",
                }
            )

            self.assertTrue(should_start)
            prepared = service.prepare_action_job(start_job_id)
            self.assertEqual(prepared["record_id"], "rec1")

    def test_ongoing_callback_failure_is_exposed_as_runtime_warning(self):
        handler = object.__new__(PortalHandler)

        def fail_ongoing(scope):
            raise RuntimeError(f"{scope} unavailable")

        old_state_store = PortalHandler.state_store
        try:
            with tempfile.TemporaryDirectory() as tmp:
                PortalHandler.state_store = LanPortalStateStore(
                    Path(tmp) / "lan_portal_state.sqlite3"
                )
                PortalHandler.ongoing_callback = fail_ongoing
                result = handler._get_ongoing("ALL")
                payload = PortalHandler._with_runtime_warnings({"warnings": []})
        finally:
            PortalHandler.state_store = old_state_store
            PortalHandler.ongoing_callback = None
            PortalHandler.last_ongoing_error = ""

        self.assertEqual(result, [])
        self.assertIn("主界面进行中状态读取失败", payload["warnings"][0])

    def test_work_status_reads_use_sqlite_after_legacy_import(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = self._new_temp_service(root)
            status_dir = root / "lan_template_work_status"
            status_dir.mkdir(parents=True, exist_ok=True)
            (status_dir / "2026-05.json").write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "work_type": "maintenance",
                                "source_record_id": "rec1",
                                "title": "D楼维保",
                                "building": "D楼",
                                "building_codes": ["D"],
                                "status": "进行中",
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch(
                "lan_bitable_template_portal.portal_service.json.load",
                wraps=json.load,
            ) as load_mock:
                first = service._load_work_status_items_locked("D")
                self.assertEqual(load_mock.call_count, 1)
                first[0]["title"] = "mutated outside cache"
                load_mock.reset_mock()

                second = service._load_work_status_items_locked("D")

            self.assertEqual(load_mock.call_count, 0)
            self.assertEqual(second[0]["title"], "D楼维保")

            service._upsert_work_status_item_locked(
                {
                    "work_type": "maintenance",
                    "source_record_id": "rec1",
                    "title": "D楼维保已更新",
                    "building": "D楼",
                    "building_code": "D",
                    "building_codes": ["D"],
                    "status": "进行中",
                },
                action="update",
                now="2026-05-12 22:00",
            )
            with patch(
                "lan_bitable_template_portal.portal_service.json.load",
                wraps=json.load,
            ) as reload_mock:
                third = service._load_work_status_items_locked("D")

            self.assertEqual(reload_mock.call_count, 0)
            self.assertTrue(any(item["title"] == "D楼维保已更新" for item in third))

    def test_invalid_source_cache_ttl_falls_back_to_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._last_loaded_ts = 0
            with patch(
                "lan_bitable_template_portal.portal_service.config.lan_template_source_cache_ttl_seconds",
                "not-a-number",
                create=True,
            ):
                self.assertEqual(service._source_cache_ttl_seconds(), 1800)
                self.assertTrue(service._source_cache_expired())
            with patch(
                "lan_bitable_template_portal.portal_service.config.lan_template_source_cache_ttl_seconds",
                120,
                create=True,
            ):
                self.assertEqual(service._source_cache_ttl_seconds(), 120)

    def test_workbench_queries_reuse_expired_source_cache_until_explicit_refresh(self):
        class CountingSourceService(MaintenancePortalService):
            def __init__(self):
                super().__init__()
                self.record_load_count = 0

            def _load_fields(self):
                self._field_meta_list = [object()]
                self._field_meta_by_name = {}
                return self._field_meta_list

            def _load_records(self):
                self.record_load_count += 1
                self._records = [_build_record("m1", "A楼", "过滤网维护", "5月")]
                self._maintenance_loaded_once = True
                return self._records

            def _load_change_fields(self):
                self._change_field_meta_list = []
                self._change_field_meta_by_name = {}
                return []

            def _load_change_records(self):
                self._change_records = []
                self._change_loaded_once = True
                return []

            def _load_zhihang_change_fields(self):
                self._zhihang_change_field_meta_list = []
                self._zhihang_change_field_meta_by_name = {}
                return []

            def _load_zhihang_change_records(self):
                self._zhihang_change_records = []
                self._zhihang_change_loaded_once = True
                return []

            def _load_repair_fields(self):
                self._repair_field_meta_list = []
                self._repair_field_meta_by_name = {}
                return []

            def _load_repair_records(self):
                self._repair_records = []
                self._repair_loaded_once = True
                return []

        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), CountingSourceService)
            service.refresh()
            service._last_loaded_ts = time.time() - 9999

            result = service.query_records(month="5月", scope="A")

            self.assertEqual([item["record_id"] for item in result["records"]], ["m1"])
            self.assertEqual(service.record_load_count, 1)

            service.ensure_loaded(refresh_if_expired=True)
            self.assertEqual(service.record_load_count, 2)

    def test_scope_overview_counts_pending_and_ongoing_by_work_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = service._current_month_label()
            service._records = [
                _build_record("m1", "D楼", "过滤网维护", current_month),
                _build_record("m2", "E楼", "照明维护", current_month),
            ]
            service._change_records = [
                _build_change_record("c1", building="D楼", progress="未开始", title="D楼变更"),
                _build_change_record("c2", building="A楼、B楼", progress="未开始", title="园区变更"),
                _build_change_record("c3", building="B楼", progress="进行中", title="B楼进行中变更"),
            ]
            service._repair_records = [
                _build_repair_record("r1", building="D楼", title="D楼UPS检修")
            ]

            with patch.object(
                service, "_target_records_for_notice_type", return_value=[]
            ):
                result = service.get_scope_overview(
                    ongoing_items=[
                        {
                            "active_item_id": "active-m",
                            "work_type": "maintenance",
                            "notice_type": "维保通告",
                            "title": "EA118机房D楼过滤网维护",
                            "building": "D楼",
                            "building_codes": ["D"],
                        }
                    ]
                )

            self.assertEqual(result["scopes"]["D"]["maintenance_pending"], 1)
            self.assertEqual(result["scopes"]["D"]["change_pending"], 1)
            self.assertEqual(result["scopes"]["D"]["repair_pending"], 1)
            self.assertEqual(result["scopes"]["D"]["maintenance_ongoing"], 1)
            self.assertEqual(result["scopes"]["CAMPUS"]["change_pending"], 1)
            self.assertEqual(result["scopes"]["B"]["change_ongoing"], 0)

    def test_scope_overview_can_preload_authorized_workbenches(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = service._current_month_label()
            service._records = [
                _build_record("m1", "D楼", "过滤网维护", current_month),
                _build_record("m2", "E楼", "照明维护", current_month),
            ]
            service._change_records = []
            service._repair_records = []

            result = service.get_scope_overview(
                scopes=["D"],
                include_prepared=True,
                ongoing_items=[],
            )

            self.assertEqual(set(result["scopes"].keys()), {"D"})
            self.assertEqual(set(result["prepared_workbenches"].keys()), {"D"})
            self.assertEqual(
                [item["record_id"] for item in result["prepared_workbenches"]["D"]["records"]],
                ["m1"],
            )

    def test_source_ongoing_change_is_not_surfaced_without_qt_item(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record(
                    "source-change-1",
                    building="C楼",
                    progress="进行中",
                    title="C楼柴油发电机测试变更",
                )
            ]
            target_records = [
                {
                    "record_id": "target-change-1",
                    "display_fields": {
                        "名称": "C楼柴油发电机测试变更",
                        "变更状态": "开始",
                        "变更开始时间": "2026-05-08 09:00",
                        "楼栋": "C楼",
                    },
                }
            ]

            with patch.object(
                service, "_target_records_for_notice_type", return_value=target_records
            ):
                merged = service._merge_ongoing_items("C", [])

            self.assertEqual(merged, [])

    def test_zhihang_change_records_filter_by_progress_and_title_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._zhihang_change_records = [
                _build_zhihang_change_record("z-a", title="EA118机房A楼网络割接"),
                _build_zhihang_change_record("z-campus", title="ABC楼链路调整"),
                _build_zhihang_change_record("z-ab", title="AB楼网络调整"),
                _build_zhihang_change_record("z-abc", title="ABC网络调整"),
                _build_zhihang_change_record("z-all", title="通用平台升级"),
                _build_zhihang_change_record(
                    "z-ended", title="EA118机房A楼已结束割接", progress="已结束"
                ),
            ]

            a_records = service._filter_zhihang_change_records(scope="A")
            campus_records = service._filter_zhihang_change_records(scope="CAMPUS")

            self.assertEqual([record["record_id"] for record in a_records], ["z-a", "z-all"])
            self.assertEqual(
                [record["record_id"] for record in campus_records],
                ["z-campus", "z-ab", "z-abc", "z-all"],
            )

    def test_zhihang_binding_is_hidden_after_local_work_status_link(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._zhihang_change_records = [
                _build_zhihang_change_record("z-c", title="EA118机房C楼链路调整")
            ]
            with service._summary_lock:
                service._upsert_work_status_item_locked(
                    {
                        "work_type": WORK_TYPE_CHANGE,
                        "notice_type": "设备变更",
                        "source_record_id": "ali-c",
                        "title": "C楼阿里侧变更",
                        "building": "C楼",
                        "building_code": "C",
                        "building_codes": ["C"],
                        "zhihang_record_id": "z-c",
                        "zhihang_title": "EA118机房C楼链路调整",
                    },
                    action="start",
                    now="2026-05-08 09:00",
                )

            linked = service._linked_zhihang_record_ids([])
            records = service._filter_zhihang_change_records(
                scope="C", exclude_record_ids=linked
            )

            self.assertEqual(linked, {"z-c"})
            self.assertEqual(records, [])

    def test_prepare_change_action_requires_and_keeps_zhihang_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record(
                    "ali-c",
                    building="C楼",
                    progress="未开始",
                    title="C楼阿里侧变更",
                )
            ]
            service._zhihang_change_records = [
                _build_zhihang_change_record("z-c", title="EA118机房C楼链路调整")
            ]
            base_payload = {
                "action": "start",
                "scope": "C",
                "work_type": WORK_TYPE_CHANGE,
                "record_id": "ali-c",
                "specialty": "电气",
                "start_time": "2026-05-08T09:00",
                "end_time": "2026-05-08T18:00",
                "location": "C楼",
                "content": "测试内容",
                "reason": "测试原因",
                "impact": "测试影响",
                "progress": "准备开始",
                "zhihang_involved": True,
            }

            with self.assertRaises(PortalError):
                service.prepare_change_action(base_payload, job_id="job-z-missing")

            not_involved = service.prepare_change_action(
                {**base_payload, "zhihang_involved": "false"},
                job_id="job-z-not-involved",
            )
            self.assertFalse(not_involved["zhihang_involved"])
            self.assertEqual(not_involved["zhihang_record_id"], "")

            prepared = service.prepare_change_action(
                {**base_payload, "zhihang_record_id": "z-c"},
                job_id="job-z-ok",
            )

            self.assertTrue(prepared["zhihang_involved"])
            self.assertEqual(prepared["zhihang_record_id"], "z-c")
            self.assertEqual(prepared["zhihang_title"], "EA118机房C楼链路调整")

    def test_source_ongoing_repair_is_not_surfaced_without_qt_item(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record(
                    "source-repair-1",
                    title="D楼UPS故障检修",
                    building="D楼",
                    started=True,
                    target_record_id="",
                )
            ]
            target_records = [
                {
                    "record_id": "target-repair-1",
                    "display_fields": {
                        "名称（标题）": "D楼UPS故障检修",
                        "检修状态": "开始",
                        "实际开始时间": "2026-05-08 09:00",
                        "楼栋": "D楼",
                    },
                }
            ]

            with patch.object(
                service, "_target_records_for_notice_type", return_value=target_records
            ):
                merged = service._merge_ongoing_items("D", [])

            self.assertEqual(merged, [])

    def test_hidden_ongoing_item_is_removed_from_refresh_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record(
                    "source-change-hide",
                    building="C楼",
                    progress="进行中",
                    title="C楼需隐藏变更",
                )
            ]
            service.hide_ongoing_item(
                {
                    "scope": "C",
                    "work_type": WORK_TYPE_CHANGE,
                    "notice_type": "设备变更",
                    "active_item_id": "source-change-source-change-hide",
                    "source_record_id": "source-change-hide",
                    "building": "C楼",
                    "building_codes": ["C"],
                    "title": "C楼需隐藏变更",
                },
                scope="C",
                deleted_by="tester",
            )

            with patch.object(service, "_target_records_for_notice_type", return_value=[]):
                merged = service._merge_ongoing_items("C", [])

            self.assertEqual(merged, [])

    def test_hidden_ongoing_uses_active_item_identity_for_qt_items(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service.hide_ongoing_item(
                {
                    "scope": "C",
                    "work_type": WORK_TYPE_CHANGE,
                    "notice_type": "设备变更",
                    "active_item_id": "active-old",
                    "record_id": "target-same",
                    "building": "C楼",
                    "building_codes": ["C"],
                    "title": "C楼同一目标记录",
                },
                scope="C",
                deleted_by="tester",
            )

            merged = service._merge_ongoing_items(
                "C",
                [
                    {
                        "active_item_id": "active-new",
                        "work_type": WORK_TYPE_CHANGE,
                        "notice_type": "设备变更",
                        "record_id": "target-same",
                        "title": "C楼同一目标记录",
                        "building": "C楼",
                        "building_codes": ["C"],
                    }
                ],
            )

            self.assertEqual(len(merged), 1)
            self.assertEqual(merged[0]["active_item_id"], "active-new")

    def test_validate_ongoing_delete_rejects_wrong_scope_before_side_effect(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))

            with self.assertRaises(PortalError):
                service.validate_ongoing_delete_item(
                    {
                        "active_item_id": "active-a",
                        "work_type": WORK_TYPE_CHANGE,
                        "notice_type": "设备变更",
                        "record_id": "target-a",
                        "title": "A楼变更",
                        "building": "A楼",
                        "building_codes": ["A"],
                    },
                    scope="C",
                )

            hidden_file = Path(tmp) / "lan_template_hidden_ongoing.json"
            self.assertFalse(hidden_file.exists())

    def test_qt_ongoing_item_is_kept_even_if_target_record_is_completed(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            target_records = [
                {
                    "record_id": "target-change-ended",
                    "display_fields": {
                        "名称": "C楼已完成变更",
                        "变更状态": "已结束",
                        "变更开始时间": "2026-05-08 09:00",
                        "楼栋": "C楼",
                    },
                }
            ]

            with patch.object(
                service, "_target_records_for_notice_type", return_value=target_records
            ):
                merged = service._merge_ongoing_items(
                    "C",
                    [
                        {
                            "active_item_id": "active-ended",
                            "work_type": WORK_TYPE_CHANGE,
                            "notice_type": "设备变更",
                            "record_id": "target-change-ended",
                            "title": "C楼已完成变更",
                            "building": "C楼",
                            "building_codes": ["C"],
                        }
                    ],
                )

            self.assertEqual(len(merged), 1)
            self.assertEqual(merged[0]["active_item_id"], "active-ended")

    def test_source_ongoing_with_completed_target_match_is_not_displayed(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record(
                    "source-change-ended",
                    building="C楼",
                    progress="进行中",
                    title="C楼目标已完成变更",
                )
            ]
            target_records = [
                {
                    "record_id": "target-change-ended",
                    "display_fields": {
                        "名称": "C楼目标已完成变更",
                        "变更状态": "已结束",
                        "变更开始时间": "2026-05-08 09:00",
                        "楼栋": "C楼",
                    },
                }
            ]

            with patch.object(
                service, "_target_records_for_notice_type", return_value=target_records
            ):
                merged = service._merge_ongoing_items("C", [])

            self.assertEqual(merged, [])

    def test_history_summary_filters_month_scope_and_work_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summary_dir = root / "lan_template_daily_summary"
            summary_dir.mkdir(parents=True)
            (summary_dir / "2026-04-30.json").write_text(
                """
{
  "date": "2026-04-30",
  "items": [
    {
      "key": "change:source:c1",
      "work_type": "change",
      "source_record_id": "c1",
      "feishu_record_id": "target-c1",
      "title": "园区变更",
      "building": "A楼、B楼",
      "building_codes": ["A", "B"],
      "specialty": "电气",
      "status": "已结束",
      "started_at": "2026-04-30 09:00",
      "ended_at": "2026-04-30 18:00",
      "actions": [
        {"action": "start", "label": "开始", "time": "2026-04-30 09:00"},
        {"action": "update", "label": "更新", "time": "2026-04-30 12:00"},
        {"action": "end", "label": "结束", "time": "2026-04-30 18:00"}
      ]
    },
    {
      "key": "maintenance:source:m1",
      "work_type": "maintenance",
      "source_record_id": "m1",
      "title": "D楼维保",
      "building": "D楼",
      "building_codes": ["D"],
      "status": "已结束",
      "started_at": "2026-04-30 09:00",
      "ended_at": "2026-04-30 18:00",
      "actions": [{"action": "end", "label": "结束", "time": "2026-04-30 18:00"}]
    }
  ]
}
""".strip(),
                encoding="utf-8",
            )
            (summary_dir / "2026-05-01.json").write_text(
                '{"date":"2026-05-01","items":[{"work_type":"change","title":"五月变更","building_codes":["A","B"],"status":"已结束","started_at":"2026-05-01 09:00","ended_at":"2026-05-01 18:00"}]}',
                encoding="utf-8",
            )
            service = self._new_temp_service(root)

            result = service.get_history_summary(
                scope="CAMPUS", month="2026-04", work_type="change"
            )

            self.assertEqual(result["month"], "2026-04")
            self.assertEqual(len(result["days"]), 1)
            day = result["days"][0]
            self.assertEqual(day["date"], "2026-04-30")
            self.assertEqual(day["stats"]["started"], 1)
            self.assertEqual(day["stats"]["updated"], 1)
            self.assertEqual(day["stats"]["ended"], 1)
            self.assertEqual(day["items"][0]["title"], "园区变更")

    def test_handover_links_are_shared_and_validate_http_urls(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))

            self.assertFalse(service.verify_handover_settings_password("Nantong@2026"))
            with patch(
                "lan_bitable_template_portal.portal_service.config.lan_handover_settings_password",
                "configured-pass",
                create=True,
            ):
                saved = service.save_handover_links(
                    {"A": "https://example.com/a", "B": "http://example.com/b"},
                    password="configured-pass",
                )

                self.assertEqual(saved["links"]["A"], "https://example.com/a")
                self.assertEqual(saved["links"]["B"], "http://example.com/b")
                reloaded = service.get_handover_links()
                self.assertEqual(reloaded["links"]["A"], "https://example.com/a")
                self.assertEqual(
                    [item["value"] for item in reloaded["scope_options"]],
                    ["110", "A", "B", "C", "D", "E", "H"],
                )
                with self.assertRaises(Exception):
                    service.save_handover_links({"A": "https://example.com/a"})
                with self.assertRaises(Exception):
                    service.save_handover_links(
                        {"A": "ftp://example.com/a"},
                        password="configured-pass",
                    )

    def test_handover_password_reset_is_single_active_flow_and_updates_password(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))

            def fake_send(text, open_ids):
                self.assertIn("验证码", text)
                self.assertEqual(
                    open_ids,
                    [
                        "ou_902e364a6c2c6c20893c02abe505a7b2",
                        "ou_ma_jinyu",
                    ],
                )
                return True, "ok", [{"open_id": item, "ok": True} for item in open_ids]

            with patch(
                "lan_bitable_template_portal.portal_service.MA_JINYU_OPEN_ID",
                "ou_ma_jinyu",
            ), patch(
                "lan_bitable_template_portal.portal_service.send_text_to_open_ids",
                side_effect=fake_send,
            ), patch.dict(
                os.environ,
                {
                    "CLIPFLOW_REQUIRE_REAL_EXTERNAL_CONFIRM": "1",
                    "CLIPFLOW_REAL_EXTERNAL_CONFIRMED": "1",
                },
                clear=False,
            ):
                reset = service.request_handover_password_reset()
                self.assertTrue(reset["reset_id"])
                with self.assertRaises(Exception):
                    service.request_handover_password_reset()
                code = service._handover_password_reset["code"]
                service.reset_handover_password_with_code(
                    reset_id=reset["reset_id"],
                    code=code,
                    new_password="new-pass-123",
                )

            self.assertTrue(service.verify_handover_settings_password("new-pass-123"))
            self.assertFalse(service.verify_handover_settings_password("Nantong@2026"))
            saved = service.save_handover_links(
                {"A": "https://example.com/a"},
                password="new-pass-123",
            )
            self.assertEqual(saved["links"]["A"], "https://example.com/a")

    def test_portal_auth_migrates_permissions_to_sqlite_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                manager = PortalAuthManager()
                permission_path = root / "lan_portal_auth.json"
                permission_path.write_text(
                    json.dumps(
                        {
                            "version": 1,
                            "default_scopes": [],
                            "users": {
                                "ou_test": {
                                    "name": "测试用户",
                                    "role": "building",
                                    "scopes": ["A"],
                                }
                            },
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
                with manager._lock:
                    manager._sessions["sid"] = {
                        "session_id": "sid",
                        "user": {"open_id": "ou_test", "name": "测试用户"},
                        "allowed_scopes": ["A"],
                        "expires_at": time.time() + 3600,
                    }

                session = manager.get_session("sid")
                self.assertEqual(manager.session_scopes(session), ["A"])

                stored = manager._state_store.get_auth_permissions()
                self.assertEqual(stored["users"]["ou_test"]["scopes"], ["A"])

                permission_path.write_text(
                    json.dumps(
                        {
                            "version": 1,
                            "default_scopes": [],
                            "users": {
                                "ou_test": {
                                    "name": "测试用户",
                                    "role": "building",
                                    "scopes": ["B"],
                                }
                            },
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )
                session = manager.get_session("sid")
                self.assertEqual(manager.session_scopes(session), ["A"])

                permission_path.write_text("{bad json", encoding="utf-8")
                self.assertEqual(manager.scopes_for_open_id("ou_unknown"), [])
                self.assertTrue(permission_path.exists())
                self.assertFalse(list(root.glob("lan_portal_auth.bad.*.json")))

    def test_h_building_has_all_scope_access_without_admin_role(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                manager = PortalAuthManager()
                h_open_id = "ou_55647926b7fbfe46507ef7e8afa76315"
                scopes = manager.scopes_for_open_id(h_open_id)
                self.assertNotIn("ALL", scopes)
                for scope in ["CAMPUS", "110", "A", "B", "C", "D", "E", "H"]:
                    self.assertIn(scope, scopes)

                session = {
                    "user": {"open_id": h_open_id, "name": "H楼"},
                    "role": manager.role_for_open_id(h_open_id),
                    "allowed_scopes": scopes,
                }
                self.assertFalse(manager.is_admin(session))
                self.assertTrue(manager.scope_allowed(session, "ALL"))
                self.assertEqual(manager.default_scope(session), "ALL")

    def test_portal_auth_required_admins_and_disabled_user_permissions(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                manager = PortalAuthManager()
                payload, changed = manager.save_permissions_payload(
                    [
                        {
                            "open_id": "ou_disabled",
                            "name": "禁用用户",
                            "role": "building",
                            "scopes": ["A"],
                            "enabled": False,
                        },
                        {
                            "open_id": "ou_admin",
                            "name": "新增管理员",
                            "role": "admin",
                            "scopes": [],
                            "enabled": True,
                        },
                    ],
                    updated_by="ou_actor",
                )

                users = {item["open_id"]: item for item in payload["users"]}
                self.assertIn("ou_902e364a6c2c6c20893c02abe505a7b2", users)
                self.assertIn("ou_a6644e62a43b916c6bc26148cf74f208", users)
                self.assertTrue(users["ou_902e364a6c2c6c20893c02abe505a7b2"]["locked"])
                self.assertEqual(manager.scopes_for_open_id("ou_disabled"), [])
                self.assertEqual(manager.role_for_open_id("ou_admin"), "admin")
                self.assertTrue(manager.scope_allowed({
                    "role": "admin",
                    "allowed_scopes": users["ou_admin"]["scopes"],
                }, "ALL"))
                self.assertIn("ou_disabled", changed)

    def test_portal_auth_permission_request_code_approves_user(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                manager = PortalAuthManager()
                request = manager.create_permission_request(
                    open_id="ou_requester",
                    name="申请人",
                    scopes=["A", "CAMPUS", "ALL"],
                    reason="值班需要",
                )
                self.assertEqual(manager.get_current_permission_request("ou_requester"), {})
                manager.activate_permission_request(request["request_id"])
                self.assertEqual(
                    manager.get_current_permission_request("ou_requester")["request_id"],
                    request["request_id"],
                )
                self.assertEqual(request["requested_scopes"], ["A", "CAMPUS"])
                code = request["code"]
                stored = manager._state_store.get_permission_request(request["request_id"])
                self.assertNotIn("code", stored)
                self.assertNotEqual(stored.get("code_hash"), code)

                with self.assertRaises(PortalError):
                    manager.confirm_permission_request(
                        open_id="ou_requester",
                        request_id=request["request_id"],
                        code="000000",
                    )
                stored = manager._state_store.get_permission_request(request["request_id"])
                self.assertEqual(stored["attempts"], 1)

                with self.assertRaises(PortalError):
                    manager.confirm_permission_request(
                        open_id="ou_other",
                        request_id=request["request_id"],
                        code=code,
                    )

                manager.confirm_permission_request(
                    open_id="ou_requester",
                    request_id=request["request_id"],
                    code=code,
                )
                self.assertEqual(manager.scopes_for_open_id("ou_requester"), ["A", "CAMPUS"])
                approved = manager._state_store.get_permission_request(request["request_id"])
                self.assertEqual(approved["status"], "approved")

    def test_portal_auth_permission_request_replaces_and_expires(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                manager = PortalAuthManager()
                old_request = manager.create_permission_request(
                    open_id="ou_pending",
                    name="申请人",
                    scopes=["B"],
                    reason="第一次",
                )
                manager.activate_permission_request(old_request["request_id"])
                new_request = manager.create_permission_request(
                    open_id="ou_pending",
                    name="申请人",
                    scopes=["C"],
                    reason="第二次",
                )
                manager.activate_permission_request(new_request["request_id"])
                manager.supersede_other_permission_requests(
                    open_id="ou_pending",
                    keep_request_id=new_request["request_id"],
                )
                current = manager.get_current_permission_request("ou_pending")
                self.assertEqual(current["request_id"], new_request["request_id"])
                with self.assertRaises(PortalError):
                    manager.confirm_permission_request(
                        open_id="ou_pending",
                        request_id=old_request["request_id"],
                        code=old_request["code"],
                    )

                expired_request = manager.create_permission_request(
                    open_id="ou_expired",
                    name="过期申请人",
                    scopes=["D"],
                    reason="过期",
                )
                manager.activate_permission_request(expired_request["request_id"])
                payload = manager._state_store.get_permission_request(
                    expired_request["request_id"]
                )
                payload["expires_at_ts"] = time.time() - 1
                manager._state_store.put_permission_request(payload)
                with self.assertRaises(PortalError):
                    manager.confirm_permission_request(
                        open_id="ou_expired",
                        request_id=expired_request["request_id"],
                        code=expired_request["code"],
                )
                self.assertEqual(manager.scopes_for_open_id("ou_expired"), [])

    def test_portal_auth_permission_request_notify_failure_keeps_old_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                manager = PortalAuthManager()
                old_request = manager.create_permission_request(
                    open_id="ou_retry",
                    name="申请人",
                    scopes=["B"],
                    reason="第一次",
                )
                manager.activate_permission_request(old_request["request_id"])
                failed_request = manager.create_permission_request(
                    open_id="ou_retry",
                    name="申请人",
                    scopes=["C"],
                    reason="通知失败",
                )
                manager.mark_permission_request_notify_failed(failed_request["request_id"])
                current = manager.get_current_permission_request("ou_retry")
                self.assertEqual(current["request_id"], old_request["request_id"])
                manager.confirm_permission_request(
                    open_id="ou_retry",
                    request_id=old_request["request_id"],
                    code=old_request["code"],
                )
                self.assertEqual(manager.scopes_for_open_id("ou_retry"), ["B"])

    def test_portal_auth_permission_request_reenables_disabled_user(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                manager = PortalAuthManager()
                manager.save_permissions_payload(
                    [
                        {
                            "open_id": "ou_disabled_apply",
                            "name": "禁用后申请",
                            "role": "building",
                            "scopes": ["A"],
                            "enabled": False,
                        }
                    ],
                    updated_by="ou_actor",
                )
                request = manager.create_permission_request(
                    open_id="ou_disabled_apply",
                    name="禁用后申请",
                    scopes=["E"],
                    reason="恢复权限",
                )
                manager.activate_permission_request(request["request_id"])
                manager.confirm_permission_request(
                    open_id="ou_disabled_apply",
                    request_id=request["request_id"],
                    code=request["code"],
                )
                self.assertEqual(manager.scopes_for_open_id("ou_disabled_apply"), ["E"])

    def test_portal_auth_rejects_reserved_meta_open_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                manager = PortalAuthManager()
                with self.assertRaises(PortalError):
                    manager.save_permissions_payload(
                        [
                            {
                                "open_id": "__meta__",
                                "name": "错误用户",
                                "role": "building",
                                "scopes": ["A"],
                            }
                        ],
                        updated_by="ou_actor",
                    )

    def test_scope_overview_filter_removes_unauthorized_preloaded_workbenches(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                manager = PortalAuthManager()
                session = {
                    "user": {"open_id": "ou_a", "name": "A楼"},
                    "role": "building",
                    "allowed_scopes": ["A"],
                }
                payload = {
                    "scope_options": [
                        {"value": "A", "label": "A楼"},
                        {"value": "B", "label": "B楼"},
                    ],
                    "scopes": {"A": {"change_pending": 1}, "B": {"change_pending": 2}},
                    "prepared_workbenches": {
                        "A": {"records": [{"record_id": "a1"}], "scope_options": []},
                        "B": {"records": [{"record_id": "b1"}], "scope_options": []},
                    },
                }

                result = manager.filter_scope_overview(payload, session)

            self.assertEqual(set(result["scopes"].keys()), {"A"})
            self.assertEqual(set(result["prepared_workbenches"].keys()), {"A"})
            self.assertEqual(
                result["prepared_workbenches"]["A"]["scope_options"],
                result["scope_options"],
            )

    def test_portal_auth_redirect_host_sanitization(self):
        self.assertEqual(
            PortalAuthManager._normalize_next_path("/?code=old&state=old&scope=E"),
            "/?scope=E",
        )
        self.assertEqual(
            PortalAuthManager._normalize_next_path("/?code=old&state=old"),
            "/",
        )
        self.assertEqual(
            PortalAuthManager._normalize_next_path("/api/auth/feishu/callback?code=x&state=y"),
            "/",
        )
        self.assertEqual(
            PortalAuthManager._normalize_next_path("/api/auth/status?next=/"),
            "/",
        )
        self.assertEqual(
            PortalAuthManager._normalize_next_path("/api/bootstrap?scope=E"),
            "/",
        )
        self.assertEqual(
            PortalHandler._safe_host_value("127.0.0.1:18766", "fallback:1"),
            "127.0.0.1:18766",
        )
        self.assertEqual(
            PortalHandler._safe_host_value("[::1]:18766", "fallback:1"),
            "[::1]:18766",
        )
        self.assertEqual(
            PortalHandler._safe_host_value("evil.example\r\nX-Test: 1", "fallback:1"),
            "fallback:1",
        )
        self.assertEqual(
            PortalHandler._safe_host_value("evil.example/path", "fallback:1"),
            "fallback:1",
        )
        self.assertEqual(
            PortalHandler._safe_host_value("evil.example@127.0.0.1", "fallback:1"),
            "fallback:1",
        )
        self.assertEqual(
            PortalHandler._safe_host_value("0.0.0.0:18766", "fallback:1"),
            "fallback:1",
        )
        self.assertEqual(PortalHandler._safe_proto_value("https, http"), "https")
        self.assertEqual(PortalHandler._safe_proto_value("javascript"), "http")

    def test_parse_field_metas_accepts_items_list(self):
        service = _TestMaintenancePortalService()
        metas = service._parse_field_metas(
            [
                {
                    "field_id": "fld1",
                    "field_name": "进展",
                    "ui_type": "SingleSelect",
                    "type": 3,
                    "property": {
                        "options": [
                            {"id": "opt1", "name": "进行中"},
                            {"id": "opt2", "name": "已结束"},
                        ]
                    },
                    "is_primary": False,
                }
            ]
        )
        self.assertEqual(len(metas), 1)
        self.assertEqual(metas[0].field_name, "进展")
        self.assertEqual(metas[0].options_map["opt1"], "进行中")

    def test_maintenance_cycle_written_to_target_fields(self):
        handler = MaintenanceNoticeHandler("维保通告")
        fields = handler.build_create_fields(
            NoticePayload(
                text=(
                    "【维保通告】状态：开始\n"
                    "【名称】EA118机房A楼测试维保\n"
                    "【时间】2026-05-15 09:30~2026-05-15 18:30\n"
                    "【位置】A楼\n"
                    "【内容】测试\n"
                    "【原因】测试\n"
                    "【影响】无\n"
                    "【进度】准备开始"
                ),
                buildings=["A楼"],
                specialty="电气",
                maintenance_cycle="月度",
            )
        )
        self.assertEqual(
            fields[MAINTENANCE_NOTICE_FIELDS["maintenance_cycle"]],
            "月度",
        )

    def test_maintenance_cycle_options_match_qt_and_portal(self):
        dialogs_source = (BIN_DIR / "upload_event_module" / "ui" / "dialogs.py").read_text(
            encoding="utf-8"
        )
        tree = ast.parse(dialogs_source)
        qt_options = []
        for node in tree.body:
            if isinstance(node, ast.Assign):
                names = [target.id for target in node.targets if isinstance(target, ast.Name)]
                if "MAINTENANCE_CYCLE_OPTIONS" in names:
                    qt_options = list(ast.literal_eval(node.value))
                    break
        self.assertIn("/", qt_options)

        html_source = (
            BIN_DIR / "lan_bitable_template_portal" / "static" / "index.html"
        ).read_text(encoding="utf-8")
        match = re.search(
            r"const\s+MAINTENANCE_CYCLE_OPTIONS\s*=\s*\[(?P<body>.*?)\];",
            html_source,
            re.S,
        )
        self.assertIsNotNone(match)
        portal_options = re.findall(r'"([^"]*)"', match.group("body"))
        self.assertEqual(portal_options, qt_options)

    def test_outbox_events_are_leased_and_acknowledged(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            event_id = store.enqueue_outbox_event(
                "qt_action",
                {"kind": "maintenance_action", "job_id": "job1"},
            )
            leased = store.lease_outbox_events("qt_action", limit=1, lease_seconds=30)
            self.assertEqual(len(leased), 1)
            self.assertEqual(leased[0]["id"], event_id)
            self.assertEqual(leased[0]["status"], "leased")
            self.assertEqual(store.list_outbox_events("qt_action"), [])

            store.mark_outbox_event(event_id, "done")
            done = store.list_outbox_events("qt_action", status="done")
            self.assertEqual(len(done), 1)
            self.assertEqual(done[0]["id"], event_id)

            failed_id = store.enqueue_outbox_event(
                "qt_action",
                {"kind": "maintenance_action", "job_id": "job2"},
            )
            first = store.mark_outbox_event(
                failed_id,
                "pending",
                error="callback down",
                max_attempts=2,
            )
            self.assertEqual(first["status"], "pending")
            self.assertEqual(first["attempts"], 1)
            self.assertEqual(first["last_error"], "callback down")
            second = store.mark_outbox_event(
                failed_id,
                "pending",
                error="callback still down",
                max_attempts=2,
            )
            self.assertEqual(second["status"], "failed")
            self.assertEqual(second["attempts"], 2)
            failed = store.list_outbox_events("qt_action", status="failed")
            self.assertEqual(len(failed), 1)
            self.assertEqual(failed[0]["id"], failed_id)
            self.assertEqual(failed[0]["last_error"], "callback still down")
            counts = store.count_outbox_events("qt_action")
            self.assertEqual(counts.get("done"), 1)
            self.assertEqual(counts.get("failed"), 1)

            stale_id = store.enqueue_outbox_event(
                "qt_action",
                {"kind": "notice", "job_id": "job3"},
            )
            stale_lease = store.lease_outbox_events("qt_action", limit=1)
            self.assertEqual(stale_lease[0]["id"], stale_id)
            conn = sqlite3.connect(Path(tmp) / "state.sqlite3")
            try:
                conn.execute(
                    "UPDATE event_outbox SET updated_at = 0 WHERE id = ?",
                    (stale_id,),
                )
                conn.commit()
            finally:
                conn.close()
            counts = store.count_outbox_events("qt_action", stale_lease_seconds=5)
            self.assertEqual(counts.get("pending"), 1)
            self.assertIsNone(counts.get("leased"))

    def test_source_refresh_singleflight_returns_inflight_status(self):
        acquired = PortalHandler.source_refresh_run_lock.acquire(blocking=False)
        self.assertTrue(acquired)
        try:
            with patch.dict(os.environ, {"CLIPFLOW_BACKEND_MOCK_EXTERNAL": ""}, clear=False):
                result = PortalHandler.refresh_sources_once(force=True)
            self.assertFalse(result["refreshed"])
            self.assertTrue(result["source_refresh_inflight"])
            self.assertTrue(result["source_refresh_reused"])
        finally:
            PortalHandler.source_refresh_run_lock.release()

    def test_source_refresh_failure_records_failed_snapshot_manifest(self):
        class _FailingSourceRefreshService:
            _load_warnings: list[str] = []

            def _snapshot_meta(self):
                return {"warnings": list(self._load_warnings)}

            def refresh(self):
                raise RuntimeError("boom")

            def process_due_repair_link_tasks(self, *, limit):
                return {"processed": 0}

        original_store = PortalHandler.state_store
        original_service = PortalHandler.service
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalHandler.state_store = store
            PortalHandler.service = _FailingSourceRefreshService()
            try:
                with patch.dict(os.environ, {"CLIPFLOW_BACKEND_MOCK_EXTERNAL": ""}, clear=False):
                    result = PortalHandler.refresh_sources_once(force=True)
                stats = store.source_snapshot_stats()
            finally:
                PortalHandler.state_store = original_store
                PortalHandler.service = original_service
        self.assertFalse(result["refreshed"])
        self.assertTrue(result["warnings"])
        self.assertEqual(stats["last_failed"]["status"], "failed")
        self.assertIn("源表后台同步失败", stats["last_failed"]["error"])
        self.assertFalse(stats["active"])

    def test_action_jobs_recover_only_before_external_side_effects(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            accepted = {
                "job_id": "job-accepted",
                "phase": "accepted",
                "message_started_at": 0.0,
                "request": {"action": "start", "record_id": "m1"},
            }
            uploading = {
                "job_id": "job-uploading",
                "phase": "uploading",
                "message_started_at": time.time(),
                "request": {"action": "start", "record_id": "m2"},
            }
            store.put_document("notice_action_job", "job-accepted", accepted)
            store.put_document("notice_action_job", "job-uploading", uploading)
            service = _TestMaintenancePortalService()
            service._state_store = store
            service._jobs = service._load_action_jobs_from_state()

            self.assertEqual(service.get_job("job-accepted")["phase"], "accepted")
            self.assertTrue(service.get_job("job-accepted")["restart_recovered"])
            self.assertEqual(service.recoverable_action_job_ids(), ["job-accepted"])
            failed = service.get_job("job-uploading")
            self.assertEqual(failed["phase"], "failed")
            self.assertEqual(failed["error_category"], "process_restart")
            self.assertFalse(failed["error_retryable"])

    def test_cleanup_action_jobs_only_removes_expired_terminal_jobs(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            now = time.time()

            def create_job(operation_id: str) -> str:
                job_id, should_start = service.create_action_job(
                    {
                        "action": "start",
                        "scope": "A",
                        "record_id": operation_id,
                        "operation_id": operation_id,
                    }
                )
                self.assertTrue(should_start)
                return job_id

            old_success = create_job("old-success")
            old_failed = create_job("old-failed")
            old_running = create_job("old-running")
            recent_success = create_job("recent-success")

            with service._jobs_lock:
                for job_id, phase, finished_at in [
                    (old_success, "success", now - 500),
                    (old_failed, "failed", now - 500),
                    (old_running, "uploading", now - 500),
                    (recent_success, "success", now),
                ]:
                    job = service._jobs[job_id]
                    job["phase"] = phase
                    job["upload_finished_at"] = finished_at
                    service._persist_action_job_locked(job)

            result = service.cleanup_action_jobs(
                success_retention_seconds=120,
                failed_retention_seconds=120,
            )

            self.assertEqual(result["removed_success"], 1)
            self.assertEqual(result["removed_failed"], 1)
            self.assertIsNone(service.get_job(old_success))
            self.assertIsNone(service.get_job(old_failed))
            self.assertEqual(service.get_job(old_running)["phase"], "uploading")
            self.assertEqual(service.get_job(recent_success)["phase"], "success")

    def test_same_target_jobs_are_queued_instead_of_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))

            first_job_id, should_start = service.create_action_job(
                {
                    "action": "update",
                    "scope": "A",
                    "work_type": "maintenance",
                    "active_item_id": "active-queue-1",
                    "operation_id": "queue-op-1",
                }
            )
            self.assertTrue(should_start)

            second_job_id, should_start = service.create_action_job(
                {
                    "action": "update",
                    "scope": "A",
                    "work_type": "maintenance",
                    "active_item_id": "active-queue-1",
                    "operation_id": "queue-op-2",
                }
            )
            self.assertTrue(should_start)
            second_job = service.get_job(second_job_id)
            self.assertEqual(second_job["depends_on_job_id"], first_job_id)
            self.assertEqual(second_job["depends_on_phase"], "accepted")

    def test_runtime_queue_processable_waits_for_dependent_job(self):
        original_store = PortalHandler.state_store
        original_service = PortalHandler.service
        try:
            with tempfile.TemporaryDirectory() as tmp:
                service = self._new_temp_service(Path(tmp))
                PortalHandler.state_store = service._state_store
                PortalHandler.service = service
                first_job_id, should_start = service.create_action_job(
                    {
                        "action": "update",
                        "scope": "A",
                        "work_type": "maintenance",
                        "active_item_id": "active-queue-2",
                        "operation_id": "queue-runtime-1",
                    }
                )
                self.assertTrue(should_start)
                second_job_id, should_start = service.create_action_job(
                    {
                        "action": "update",
                        "scope": "A",
                        "work_type": "maintenance",
                        "active_item_id": "active-queue-2",
                        "operation_id": "queue-runtime-2",
                    }
                )
                self.assertTrue(should_start)

                self.assertFalse(
                    PortalHandler._runtime_queue_job_processable("message", second_job_id)
                )
                self.assertEqual(
                    service.get_job(second_job_id)["depends_on_phase"], "accepted"
                )

                service.mark_job(first_job_id, phase="success")
                self.assertTrue(
                    PortalHandler._runtime_queue_job_processable("message", second_job_id)
                )
                self.assertEqual(service.get_job(second_job_id)["depends_on_phase"], "")
        finally:
            PortalHandler.state_store = original_store
            PortalHandler.service = original_service

    def test_failed_action_jobs_are_compacted_after_persist(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "scope": "A",
                    "record_id": "rec-failed",
                    "work_type": "maintenance",
                    "location": "x" * 2000,
                    "operation_id": "failed-compact",
                }
            )
            self.assertTrue(should_start)
            service.mark_job(
                job_id,
                phase="failed",
                message_sent=True,
                message_signature="sig-1",
                prepared={"text": "y" * 5000, "work_type": "maintenance"},
                error="网络连接失败",
            )

            stored = service.get_job(job_id)
            self.assertEqual(stored["phase"], "failed")
            self.assertEqual(stored["error_category"], "network_error")
            self.assertTrue(stored["error_retryable"])
            self.assertTrue(stored["message_sent"])
            self.assertEqual(stored["message_signature"], "sig-1")
            self.assertEqual(stored["prepared"], {})
            self.assertNotIn("location", stored["request"])
            self.assertIn("location", stored["retry_request"])

    def test_job_error_classification(self):
        self.assertEqual(
            MaintenancePortalService.classify_job_error("请求 timeout")["error_category"],
            "network_timeout",
        )
        self.assertTrue(
            MaintenancePortalService.classify_job_error("接口限流 429")["error_retryable"]
        )
        self.assertFalse(
            MaintenancePortalService.classify_job_error("字段 维保周期 缺失")["error_retryable"]
        )

    def test_runtime_settings_are_bounded(self):
        original_store = PortalHandler.state_store
        original_count = PortalHandler.message_worker_count
        original_upload_seconds = getattr(PortalHandler, "upload_seconds_per_record", 2.0)
        original_batch_wait = getattr(PortalHandler, "upload_batch_wait_seconds", 5.0)
        original_batch_max = getattr(PortalHandler, "upload_batch_max_records", 5)
        original_qt_interval = getattr(PortalHandler, "qt_action_interval_ms", 250)
        original_source_defer = getattr(PortalHandler, "source_refresh_defer_when_busy", True)
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.put_settings(
                {
                    "lan_message_worker_count": 99,
                    "lan_upload_seconds_per_record": -1,
                    "lan_upload_batch_wait_seconds": 100,
                    "lan_upload_batch_max_records": 99,
                    "lan_qt_action_interval_ms": 1,
                }
            )
            PortalHandler.state_store = store
            try:
                limits = PortalHandler.apply_runtime_settings()
            finally:
                PortalHandler.state_store = original_store
                PortalHandler.message_worker_count = original_count
                PortalHandler.upload_seconds_per_record = original_upload_seconds
                PortalHandler.upload_batch_wait_seconds = original_batch_wait
                PortalHandler.upload_batch_max_records = original_batch_max
                PortalHandler.qt_action_interval_ms = original_qt_interval
                PortalHandler.source_refresh_defer_when_busy = original_source_defer
            self.assertEqual(limits["message_worker_count"], 5)
            self.assertEqual(limits["upload_seconds_per_record"], 0.5)
            self.assertEqual(limits["upload_batch_wait_seconds"], 30.0)
            self.assertEqual(limits["upload_batch_max_records"], 99)
            self.assertEqual(limits["qt_tick_interval_ms"], 100)

    def test_source_refresh_defers_when_runtime_busy(self):
        original_store = PortalHandler.state_store
        original_service = PortalHandler.service
        original_defer = getattr(PortalHandler, "source_refresh_defer_when_busy", True)
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            service = _NativeFastAPIRouteService()
            PortalHandler.state_store = store
            PortalHandler.service = service
            PortalHandler.source_refresh_defer_when_busy = True
            try:
                store.upsert_runtime_queue_item("qt_action", "job-busy")
                with patch.dict(os.environ, {"CLIPFLOW_BACKEND_MOCK_EXTERNAL": ""}, clear=False):
                    result = PortalHandler.refresh_sources_once(
                        force=True,
                        defer_if_busy=True,
                    )
            finally:
                PortalHandler.state_store = original_store
                PortalHandler.service = original_service
                PortalHandler.source_refresh_defer_when_busy = original_defer
        self.assertFalse(result["refreshed"])
        self.assertTrue(result["source_refresh_deferred"])

    def test_repair_source_refresh_request_is_background_singleflight(self):
        class _SlowRepairRefreshService:
            _load_warnings: list[str] = []

            def __init__(self):
                self.calls = 0
                self.entered = threading.Event()
                self.release = threading.Event()

            def refresh_repair_source(self):
                self.calls += 1
                self.entered.set()
                self.release.wait(timeout=5)
                return {"repair_count": 3, "repair_refreshed_at": "17:01"}

        original_service = PortalHandler.service
        service = _SlowRepairRefreshService()
        with PortalHandler.repair_refresh_lock:
            PortalHandler.repair_refresh_inflight = False
            PortalHandler.repair_refresh_event = threading.Event()
            PortalHandler.repair_refresh_last_result = {}
            PortalHandler.repair_refresh_last_error = ""
            PortalHandler.repair_refresh_last_finished = 0.0
        PortalHandler.service = service
        try:
            first = PortalHandler.request_repair_source_refresh()
            self.assertTrue(first["repair_refresh_started"])
            self.assertTrue(service.entered.wait(timeout=2))
            second = PortalHandler.request_repair_source_refresh()
            self.assertFalse(second["repair_refresh_started"])
            self.assertTrue(second["repair_refresh_inflight"])
            service.release.set()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                with PortalHandler.repair_refresh_lock:
                    if not PortalHandler.repair_refresh_inflight:
                        break
                time.sleep(0.02)
            with PortalHandler.repair_refresh_lock:
                self.assertFalse(PortalHandler.repair_refresh_inflight)
                self.assertEqual(PortalHandler.repair_refresh_last_result["repair_count"], 3)
            self.assertEqual(service.calls, 1)
        finally:
            service.release.set()
            PortalHandler.service = original_service
            with PortalHandler.repair_refresh_lock:
                PortalHandler.repair_refresh_inflight = False
                PortalHandler.repair_refresh_event = threading.Event()
                PortalHandler.repair_refresh_last_result = {}
                PortalHandler.repair_refresh_last_error = ""
                PortalHandler.repair_refresh_last_finished = 0.0

    def test_source_refresh_request_is_background_singleflight(self):
        class _SlowSourceRefreshService:
            _load_warnings: list[str] = []

            def __init__(self):
                self.calls = 0
                self.link_calls = 0
                self.entered = threading.Event()
                self.release = threading.Event()

            def refresh(self):
                self.calls += 1
                self.entered.set()
                self.release.wait(timeout=5)

            def refresh_if_interval_elapsed(self, *, min_interval_seconds):
                self.refresh()
                return True

            def process_due_repair_link_tasks(self, *, limit):
                self.link_calls += 1
                return {"processed": 0}

        original_service = PortalHandler.service
        service = _SlowSourceRefreshService()
        with PortalHandler.source_refresh_lock:
            PortalHandler.source_refresh_inflight = False
            PortalHandler.source_refresh_last_result = {}
            PortalHandler.source_refresh_last_finished = 0.0
        PortalHandler.service = service
        try:
            first = PortalHandler.request_source_refresh(force=True)
            self.assertTrue(first["source_refresh_started"])
            self.assertTrue(service.entered.wait(timeout=2))
            second = PortalHandler.request_source_refresh(force=True)
            self.assertFalse(second["source_refresh_started"])
            self.assertTrue(second["source_refresh_inflight"])
            service.release.set()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                with PortalHandler.source_refresh_lock:
                    inflight = PortalHandler.source_refresh_inflight
                if not inflight and not PortalHandler.source_refresh_run_lock.locked():
                    break
                time.sleep(0.02)
            with PortalHandler.source_refresh_lock:
                self.assertFalse(PortalHandler.source_refresh_inflight)
                self.assertTrue(PortalHandler.source_refresh_last_result["refreshed"])
            self.assertFalse(PortalHandler.source_refresh_run_lock.locked())
            self.assertEqual(service.calls, 1)
            self.assertEqual(service.link_calls, 1)
        finally:
            service.release.set()
            PortalHandler.service = original_service
            with PortalHandler.source_refresh_lock:
                PortalHandler.source_refresh_inflight = False
                PortalHandler.source_refresh_last_result = {}
                PortalHandler.source_refresh_last_finished = 0.0

    def test_fastapi_qt_local_endpoints_work_without_auth(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        client = TestClient(controller._build_app())
        root = client.get("/")
        self.assertEqual(root.status_code, 200)
        self.assertIn("text/html", root.headers.get("content-type", ""))
        asset = client.get("/assets/vnet-logo.png")
        self.assertEqual(asset.status_code, 200)
        self.assertEqual(asset.headers.get("cache-control"), "public, max-age=86400")
        self.assertEqual(
            client.get("/assets/%2e%2e/%2e%2e/server.py").status_code,
            404,
        )
        logout_get = client.get("/api/auth/logout", follow_redirects=False)
        self.assertEqual(logout_get.status_code, 302)
        logout_post = client.post("/api/auth/logout")
        self.assertEqual(logout_post.status_code, 200)
        self.assertTrue(logout_post.json().get("ok"))
        self.assertEqual(client.get("/api/health").status_code, 200)
        self.assertEqual(client.get("/api/qt/events").status_code, 200)
        heartbeat = client.post(
            "/api/qt/bridge-heartbeat",
            json={"notice_callback": True, "maintenance_action_callback": True},
        )
        self.assertEqual(heartbeat.status_code, 200)
        self.assertEqual(client.get("/api/backend/stats").status_code, 401)
        session_id = "local-admin-stats-session"
        original_sessions = dict(PortalHandler.auth_manager._sessions)
        with PortalHandler.auth_manager._lock:
            PortalHandler.auth_manager._sessions[session_id] = {
                "session_id": session_id,
                "user": {"name": "admin", "open_id": ""},
                "role": "admin",
                "allowed_scopes": ["ALL"],
                "expires_at": time.time() + 3600,
            }
        try:
            stats = client.get(
                "/api/backend/stats",
                headers={"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"},
            )
            with PortalHandler.auth_manager._lock:
                PortalHandler.auth_manager._sessions["local-user-stats-session"] = {
                    "session_id": "local-user-stats-session",
                    "user": {"name": "building", "open_id": ""},
                    "role": "building",
                    "allowed_scopes": ["A"],
                    "expires_at": time.time() + 3600,
                }
            non_admin_stats = client.get(
                "/api/backend/stats",
                headers={"Cookie": f"{AUTH_COOKIE_NAME}=local-user-stats-session"},
            )
            non_admin_stream = client.get(
                "/api/jobs/stream",
                headers={"Cookie": f"{AUTH_COOKIE_NAME}=local-user-stats-session"},
            )
        finally:
            with PortalHandler.auth_manager._lock:
                PortalHandler.auth_manager._sessions = original_sessions
        self.assertEqual(stats.status_code, 200)
        self.assertEqual(non_admin_stats.status_code, 403)
        self.assertEqual(non_admin_stream.status_code, 200)
        self.assertIn("只有管理员可以查看任务队列状态", non_admin_stream.text)
        self.assertTrue(stats.json()["data"]["qt_bridge"]["connected"])
        self.assertTrue(stats.json()["data"]["qt_bridge"]["notice_callback"])
        self.assertIn("sqlite", stats.json()["data"])
        self.assertIn("total_bytes", stats.json()["data"]["sqlite"])
        response = client.post("/api/qt/ongoing-snapshot", json={"items": []})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(client.get("/api/qt/jobs/missing").status_code, 404)
        self.assertEqual(client.get("/api/jobs/missing").status_code, 401)

    def test_fastapi_recent_jobs_returns_compact_admin_summary(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalHandler.service
        original_state_store = PortalHandler.state_store
        original_auth_state_store = PortalHandler.auth_manager._state_store
        original_sessions = dict(PortalHandler.auth_manager._sessions)
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            PortalHandler.service = service
            PortalHandler.state_store = service._state_store
            PortalHandler.auth_manager._state_store = service._state_store
            PortalHandler.auth_manager.upsert_permission_user(
                open_id="ou_admin",
                name="admin",
                role="admin",
                scopes=["ALL"],
                enabled=True,
                updated_by="test",
            )
            job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "scope": "A",
                    "record_id": "recent-1",
                    "work_type": "maintenance",
                    "location": "x" * 5000,
                    "operation_id": "recent-compact",
                }
            )
            self.assertTrue(should_start)
            service.mark_job(
                job_id,
                phase="failed",
                prepared={"text": "y" * 5000, "work_type": "maintenance"},
                error="网络连接失败",
            )
            session_id = "recent-jobs-session"
            with PortalHandler.auth_manager._lock:
                PortalHandler.auth_manager._sessions[session_id] = {
                    "session_id": session_id,
                    "user": {"name": "admin", "open_id": "ou_admin"},
                    "role": "building",
                    "allowed_scopes": [],
                    "expires_at": time.time() + 3600,
                }
            client = TestClient(controller._build_app())
            try:
                response = client.get(
                    "/api/jobs/recent?phase=failed&limit=1",
                    headers={"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"},
                )
                self.assertEqual(response.status_code, 200)
                item = response.json()["data"]["items"][0]
                self.assertEqual(item["job_id"], job_id)
                self.assertEqual(item["phase"], "failed")
                self.assertEqual(item["error_category"], "network_error")
                self.assertTrue(item["can_retry"])
                self.assertNotIn("prepared", item)
                self.assertNotIn("request", item)
                retryable = client.get(
                    "/api/jobs/recent?phase=failed&retryable=true&limit=5",
                    headers={"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"},
                )
                self.assertEqual(retryable.status_code, 200)
                self.assertEqual(
                    retryable.json()["data"]["items"][0]["job_id"],
                    job_id,
                )
                with patch.object(
                    PortalHandler,
                    "enqueue_initial_message_or_upload_job",
                    return_value=None,
                ) as enqueue_job:
                    retried = client.post(
                        f"/api/jobs/{job_id}/retry",
                        headers={"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"},
                    )
                self.assertEqual(retried.status_code, 200)
                self.assertEqual(retried.json()["data"]["phase"], "accepted")
                enqueue_job.assert_called_once_with(job_id)
                self.assertEqual(service.get_job(job_id)["phase"], "accepted")
                self.assertIn("location", service.get_job(job_id)["request"])
                service.mark_job(job_id, phase="failed", error="网络连接失败")
                cleared = client.post(
                    f"/api/jobs/{job_id}/clear",
                    headers={"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"},
                )
                self.assertEqual(cleared.status_code, 200)
                self.assertIsNone(service.get_job(job_id))
            finally:
                PortalHandler.service = original_service
                PortalHandler.state_store = original_state_store
                PortalHandler.auth_manager._state_store = original_auth_state_store
                with PortalHandler.auth_manager._lock:
                    PortalHandler.auth_manager._sessions = original_sessions

    def test_fastapi_backend_admin_tools_are_native_and_safe(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalHandler.service
        original_state_store = PortalHandler.state_store
        original_auth_state_store = PortalHandler.auth_manager._state_store
        original_sessions = dict(PortalHandler.auth_manager._sessions)
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))

            class _Meta:
                def __init__(self, field_name):
                    self.field_name = field_name

            def fake_load_table_fields(*, app_token, table_id):
                names = [
                    "楼栋",
                    "维护总项",
                    "维护实施状态",
                    "计划维护月份",
                    "专业类别",
                    "维护周期",
                    "变更简述",
                    "变更进度",
                    "变更楼栋",
                    "专业",
                    "变更等级（阿里）",
                    "检修通告名称",
                    "维修名称",
                    "所属数据中心/楼栋-使用",
                    "进展",
                    "维保状态",
                    "名称",
                    "内容",
                    "影响",
                    "进度",
                    "原因",
                    "位置",
                    "计划开始时间",
                    "计划结束时间",
                    "实际开始时间",
                    "实际结束时间",
                    "过程通告图片",
                    "过程现场图片",
                    "变更状态",
                    "变更开始钉钉截图",
                    "变更开始时间",
                    "过程更新钉钉截图",
                    "过程更新时间",
                    "变更结束钉钉截图",
                    "变更结束时间",
                    "智航-变更等级",
                    "阿里-变更等级",
                    "楼栋",
                    "今日是否进行",
                    "检修状态",
                    "名称（标题）",
                    "紧急程度",
                    "维修设备",
                    "维修故障",
                    "故障类型",
                    "维修方式",
                    "影响范围",
                    "故障发现方式（来源）",
                    "故障现象",
                    "故障原因",
                    "解决方案",
                    "进度（完成情况）",
                    "发生故障时间",
                    "期望完成时间",
                    "过程通告截图",
                    "过程现场图片",
                ]
                metas = [_Meta(name) for name in names]
                return metas, {meta.field_name: meta for meta in metas}

            service._load_table_fields = fake_load_table_fields
            PortalHandler.service = service
            PortalHandler.state_store = service._state_store
            PortalHandler.auth_manager._state_store = service._state_store
            PortalHandler.auth_manager.upsert_permission_user(
                open_id="ou_admin",
                name="admin",
                role="admin",
                scopes=["ALL"],
                enabled=True,
                updated_by="test",
            )
            session_id = "backend-tools-session"
            with PortalHandler.auth_manager._lock:
                PortalHandler.auth_manager._sessions[session_id] = {
                    "session_id": session_id,
                    "user": {"name": "admin", "open_id": "ou_admin"},
                    "role": "building",
                    "allowed_scopes": [],
                    "expires_at": time.time() + 3600,
                }
            client = TestClient(controller._build_app())
            headers = {"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"}
            try:
                preflight = client.post("/api/backend/preflight", headers=headers)
                self.assertEqual(preflight.status_code, 200)
                self.assertTrue(preflight.json()["data"]["checks"])
                self.assertIn("status", preflight.json()["data"])
                stats = client.get("/api/backend/stats", headers=headers)
                self.assertEqual(stats.status_code, 200)
                self.assertIn("preflight", stats.json()["data"])

                checkpoint = client.post(
                    "/api/backend/sqlite/checkpoint",
                    headers=headers,
                    json={},
                )
                self.assertEqual(checkpoint.status_code, 200)
                self.assertIn("reclaimed_bytes", checkpoint.json()["data"])

                cleanup = client.post(
                    "/api/backend/jobs/cleanup",
                    headers=headers,
                    json={},
                )
                self.assertEqual(cleanup.status_code, 200)
                self.assertIn("removed_total", cleanup.json()["data"])

                pressure = client.post(
                    "/api/backend/mock-pressure",
                    headers=headers,
                    json={"count": 1, "concurrency": 1, "scenario": "accepted"},
                )
                self.assertEqual(pressure.status_code, 200)
                self.assertEqual(pressure.json()["data"]["accepted"], 1)
            finally:
                PortalHandler.service = original_service
                PortalHandler.state_store = original_state_store
                PortalHandler.auth_manager._state_store = original_auth_state_store
                with PortalHandler.auth_manager._lock:
                    PortalHandler.auth_manager._sessions = original_sessions

    def test_compacted_failed_job_remains_visible_to_owner_only(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalHandler.service
        original_state_store = PortalHandler.state_store
        original_auth_state_store = PortalHandler.auth_manager._state_store
        original_sessions = dict(PortalHandler.auth_manager._sessions)
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            PortalHandler.service = service
            PortalHandler.state_store = service._state_store
            PortalHandler.auth_manager._state_store = service._state_store
            job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "scope": "A",
                    "record_id": "owner-record",
                    "work_type": "maintenance",
                    "operation_id": "owner-visible-failed",
                    "_auth_open_id": "ou_owner",
                    "location": "x" * 5000,
                }
            )
            self.assertTrue(should_start)
            service.mark_job(
                job_id,
                phase="failed",
                prepared={"text": "y" * 5000},
                error="网络连接失败",
            )
            with PortalHandler.auth_manager._lock:
                PortalHandler.auth_manager._sessions["owner-session"] = {
                    "session_id": "owner-session",
                    "user": {"name": "owner", "open_id": "ou_owner"},
                    "role": "building",
                    "allowed_scopes": ["A"],
                    "expires_at": time.time() + 3600,
                }
                PortalHandler.auth_manager._sessions["other-session"] = {
                    "session_id": "other-session",
                    "user": {"name": "other", "open_id": "ou_other"},
                    "role": "building",
                    "allowed_scopes": ["A"],
                    "expires_at": time.time() + 3600,
                }
            client = TestClient(controller._build_app())
            try:
                owner = client.get(
                    f"/api/jobs/{job_id}",
                    headers={"Cookie": f"{AUTH_COOKIE_NAME}=owner-session"},
                )
                other = client.get(
                    f"/api/jobs/{job_id}",
                    headers={"Cookie": f"{AUTH_COOKIE_NAME}=other-session"},
                )
                self.assertEqual(owner.status_code, 200)
                self.assertEqual(owner.json()["data"]["phase"], "failed")
                self.assertEqual(
                    owner.json()["data"]["request"].get("_auth_open_id"),
                    "ou_owner",
                )
                self.assertEqual(owner.json()["data"].get("prepared"), {})
                self.assertEqual(other.status_code, 403)
            finally:
                PortalHandler.service = original_service
                PortalHandler.state_store = original_state_store
                PortalHandler.auth_manager._state_store = original_auth_state_store
                with PortalHandler.auth_manager._lock:
                    PortalHandler.auth_manager._sessions = original_sessions

    def test_external_write_guard_mock_and_confirm_modes(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            prepared = {
                "job_id": "guard-job",
                "text": "测试消息",
                "recipients": ["ou_guard"],
                "message_signature": "sig",
            }
            with patch.dict(os.environ, {"CLIPFLOW_BACKEND_MOCK_EXTERNAL": "1"}, clear=False):
                ok, message = service.send_action_personal_message(prepared)
            self.assertTrue(ok)
            self.assertIn("mock", message)

            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("CLIPFLOW_BACKEND_MOCK_EXTERNAL", None)
                os.environ.pop("CLIPFLOW_REQUIRE_REAL_EXTERNAL_CONFIRM", None)
                os.environ.pop("CLIPFLOW_REAL_EXTERNAL_CONFIRMED", None)
                ok, message = service.send_action_personal_message(prepared)
            self.assertFalse(ok)
            self.assertIn("CLIPFLOW_REAL_EXTERNAL_CONFIRMED", message)

            with patch.dict(os.environ, {"CLIPFLOW_REQUIRE_REAL_EXTERNAL_CONFIRM": "1"}, clear=False):
                os.environ.pop("CLIPFLOW_BACKEND_MOCK_EXTERNAL", None)
                os.environ.pop("CLIPFLOW_REAL_EXTERNAL_CONFIRMED", None)
                ok, message = service.send_action_personal_message(prepared)
            self.assertFalse(ok)
            self.assertIn("CLIPFLOW_REAL_EXTERNAL_CONFIRMED", message)

    def test_bitable_http_400_token_error_refreshes_and_retries(self):
        class FakeResponse:
            def __init__(self, status_code, payload):
                self.status_code = status_code
                self._payload = payload
                self.ok = 200 <= status_code < 400
                self.text = json.dumps(payload, ensure_ascii=False)

            def json(self):
                return self._payload

            def raise_for_status(self):
                if not self.ok:
                    raise RuntimeError(f"{self.status_code} Client Error")

        class FakeSession:
            def __init__(self):
                self.calls = 0

            def get(self, *args, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    return FakeResponse(
                        400,
                        {
                            "code": 99991663,
                            "msg": "Invalid access token for authorization.",
                        },
                    )
                return FakeResponse(200, {"code": 0, "data": {"items": []}})

        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            fake_session = FakeSession()
            service._session = fake_session
            old_token = config_module.config.user_token
            config_module.config.user_token = "expired-token"
            try:
                with patch(
                    "lan_bitable_template_portal.portal_service.refresh_feishu_token"
                ) as refresh_token:
                    payload = service._request_json(
                        "fields", params={"page_size": 500}
                    )
                self.assertEqual(payload["code"], 0)
                self.assertEqual(fake_session.calls, 2)
                refresh_token.assert_called_once()
            finally:
                config_module.config.user_token = old_token

    def test_token_status_refreshes_once_when_expiring(self):
        old_token = config_module.config.user_token
        old_expire = config_module.config.token_expire_time
        config_module.config.user_token = "old-token"
        config_module.config.token_expire_time = int(time.time()) + 10
        try:
            with patch.object(
                feishu_service_module,
                "refresh_feishu_token",
                return_value="new-token",
            ) as refresh_token:
                token = feishu_service_module.check_token_status()
            self.assertEqual(token, "new-token")
            refresh_token.assert_called_once()

            config_module.config.user_token = "fresh-token"
            config_module.config.token_expire_time = int(time.time()) + 3600
            with patch.object(
                feishu_service_module,
                "refresh_feishu_token",
                return_value="should-not-call",
            ) as refresh_token:
                token = feishu_service_module.check_token_status()
            self.assertEqual(token, "fresh-token")
            refresh_token.assert_not_called()
        finally:
            config_module.config.user_token = old_token
            config_module.config.token_expire_time = old_expire

    def test_package_runtime_data_scan_blocks_sqlite_outputs(self):
        import package_portable

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            safe_file = root / "bin" / "refactored_main.py"
            safe_file.parent.mkdir(parents=True, exist_ok=True)
            safe_file.write_text("print('ok')\n", encoding="utf-8")
            package_portable._assert_no_runtime_data_in_output(root, "测试产物")

            db_file = root / "bin" / "data" / "lan_portal_state.sqlite3"
            db_file.parent.mkdir(parents=True, exist_ok=True)
            db_file.write_text("runtime", encoding="utf-8")
            self.assertIn(db_file, package_portable._scan_runtime_data_files(root))
            with self.assertRaises(RuntimeError):
                package_portable._assert_no_runtime_data_in_output(root, "测试产物")

    def test_mock_lan_portal_pressure_covers_job_queries_and_stats(self):
        from tools.mock_lan_portal_pressure import run_pressure

        result = run_pressure(6, 3)
        self.assertEqual(result["accepted"], 6)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["job_query_ok"], 6)
        self.assertEqual(result["job_phase_counts"], {"accepted": 6})
        self.assertEqual(result["stats_status"], 200)
        self.assertEqual(result["stats"]["message_worker_count"], 5)
        self.assertEqual(result["stats"]["job_count"], 6)
        self.assertEqual(result["stats"]["job_phase_counts"], {"accepted": 6})
        self.assertIn("qt_outbox_pending", result["stats"])

        mixed = run_pressure(6, 3, scenario="mixed")
        self.assertEqual(mixed["accepted"], 6)
        self.assertEqual(mixed["job_query_ok"], 6)
        self.assertEqual(mixed["job_phase_counts"], {"accepted": 2, "failed": 4})
        self.assertEqual(mixed["stats"]["job_phase_counts"], {"accepted": 2, "failed": 4})

    def test_fastapi_qt_event_ack_fails_job_after_retries(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalHandler.service
        original_controller_state_store = controller._state_store
        original_portal_state_store = PortalHandler.state_store
        temp_dir = tempfile.TemporaryDirectory()
        store = LanPortalStateStore(Path(temp_dir.name) / "state.sqlite3")
        controller._state_store = store
        PortalHandler.state_store = store
        PortalHandler.service = _NativeFastAPIRouteService()
        client = TestClient(controller._build_app())
        try:
            event_id = store.enqueue_outbox_event(
                "qt_action",
                {"kind": "maintenance_action", "job_id": "job-native", "payload": {}},
            )
            for attempt in range(1, 4):
                response = client.post(
                    f"/api/qt/events/{event_id}/ack",
                    json={"ok": False, "error": "Qt callback down"},
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["data"]["attempts"], attempt)
            self.assertEqual(response.json()["data"]["status"], "failed")
            self.assertEqual(
                PortalHandler.service.last_marked_job_patch["phase"],
                "failed",
            )
            self.assertIn(
                "Qt callback down",
                PortalHandler.service.last_marked_job_patch["error"],
            )
        finally:
            PortalHandler.service = original_service
            controller._state_store = original_controller_state_store
            PortalHandler.state_store = original_portal_state_store
            temp_dir.cleanup()

    def test_fastapi_auth_redirect_routes_are_native(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        client = TestClient(controller._build_app())
        with patch.object(controller, "_proxy_request", side_effect=AssertionError("proxy used")):
            with patch.object(
                PortalHandler.auth_manager,
                "start_login",
                return_value="https://login.example.test/oauth",
            ) as start_login:
                login = client.get("/api/auth/login?next=/x", follow_redirects=False)
            self.assertEqual(login.status_code, 302)
            self.assertEqual(login.headers["location"], "https://login.example.test/oauth")
            self.assertEqual(start_login.call_args.kwargs["next_path"], "/x")

            with patch.object(
                PortalHandler.auth_manager,
                "complete_login",
                return_value=("session123", "/after"),
            ) as complete_login:
                callback = client.get(
                    "/api/auth/feishu/callback?code=c&state=s",
                    follow_redirects=False,
                )
            self.assertEqual(callback.status_code, 302)
            self.assertEqual(callback.headers["location"], "/after")
            self.assertIn(AUTH_COOKIE_NAME, callback.headers.get("set-cookie", ""))
            self.assertEqual(complete_login.call_args.kwargs["code"], "c")

            with patch.object(
                PortalHandler.auth_manager,
                "complete_login",
                return_value=("session456", "/root-after"),
            ):
                root_callback = client.get(
                    "/?code=c&state=s&keep=1",
                    follow_redirects=False,
                )
            self.assertEqual(root_callback.status_code, 302)
            self.assertEqual(root_callback.headers["location"], "/root-after")

    def test_fastapi_native_read_routes_do_not_use_legacy_proxy(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalHandler.service
        original_state_store = PortalHandler.state_store
        original_sessions = dict(PortalHandler.auth_manager._sessions)
        PortalHandler.service = _NativeFastAPIRouteService()
        temp_dir = tempfile.TemporaryDirectory()
        PortalHandler.state_store = LanPortalStateStore(
            Path(temp_dir.name) / "state.sqlite3"
        )
        session_id = "native-route-session"
        with PortalHandler.auth_manager._lock:
            PortalHandler.auth_manager._sessions[session_id] = {
                "session_id": session_id,
                "user": {"name": "测试用户", "open_id": ""},
                "role": "admin",
                "allowed_scopes": ["ALL"],
                "expires_at": time.time() + 3600,
            }
        client = TestClient(controller._build_app())
        try:
            with patch.object(controller, "_proxy_request", side_effect=AssertionError("proxy used")):
                headers = {"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"}
                bootstrap = client.get("/api/bootstrap?scope=ALL", headers=headers)
                overview = client.get("/api/scope-overview", headers=headers)
                workbench = client.get(
                    "/api/workbench?scope=ALL&month=本月&specialty=电气",
                    headers=headers,
                )
                records = client.get("/api/records?scope=ALL", headers=headers)
                auth_status = client.get("/api/auth/status?next=/", headers=headers)
                backend_stats = client.get("/api/backend/stats", headers=headers)
                history = client.get(
                    "/api/history-summary?scope=ALL&month=2026-05&work_type=all",
                    headers=headers,
                )
                handover = client.get("/api/handover-links", headers=headers)
                current_request = client.get(
                    "/api/auth/permission-requests/current",
                    headers=headers,
                )
                refresh = client.get("/api/refresh?scope=ALL", headers=headers)
                repair_refresh = client.get(
                    "/api/repair-refresh?scope=ALL",
                    headers=headers,
                )
                action = client.post(
                    "/api/workbench-actions",
                    headers=headers,
                    json={"scope": "ALL", "work_type": "change", "action": "start"},
                )
                PortalHandler.state_store.put_backend_runtime(
                    "qt_bridge",
                    {
                        "heartbeat_at": time.time(),
                        "ongoing_delete_callback": True,
                        "event_thread_alive": True,
                    },
                )
                delete = client.post(
                    "/api/ongoing-items/delete",
                    headers=headers,
                    json={"scope": "ALL", "active_item_id": "active1"},
                )
                save_links = client.post(
                    "/api/handover-links",
                    headers=headers,
                    json={"password": "ok", "links": {"ALL": "http://example.test/new"}},
                )
                links_auth = client.post(
                    "/api/handover-links-auth",
                    headers=headers,
                    json={"password": "ok"},
                )
                reset_request = client.post(
                    "/api/handover-password-reset/request",
                    headers=headers,
                    json={},
                )
                reset_confirm = client.post(
                    "/api/handover-password-reset/confirm",
                    headers=headers,
                    json={
                        "reset_id": "reset1",
                        "code": "123456",
                        "new_password": "new-pass",
                    },
                )
                generated = client.post(
                    "/api/generate",
                    headers=headers,
                    json={"scope": "ALL", "drafts": [{"record_id": "m1"}]},
                )
                send_generated_without_callback = client.post(
                    "/api/send-generated",
                    headers=headers,
                    json={"scope": "ALL", "items": [{"record_id": "m1"}]},
                )
            for response in [
                bootstrap,
                overview,
                workbench,
                records,
                auth_status,
                backend_stats,
                history,
                handover,
                current_request,
                refresh,
                repair_refresh,
            ]:
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.json().get("ok"))
            self.assertIn("qt_outbox_pending", backend_stats.json()["data"])
            self.assertEqual(action.status_code, 202)
            self.assertTrue(action.json().get("ok"))
            self.assertEqual(action.json()["data"]["job_id"], "job-native")
            self.assertEqual(
                PortalHandler.service.last_action_payload["_auth_user_name"],
                "测试用户",
            )
            self.assertEqual(delete.status_code, 200)
            self.assertTrue(delete.json().get("ok"))
            self.assertEqual(delete.json()["data"]["qt_event_id"], 1)
            self.assertFalse(delete.json()["data"]["remote_deleted"])
            leased_response = client.get("/api/qt/events?limit=1")
            self.assertEqual(leased_response.status_code, 200)
            leased = leased_response.json()["data"]["items"]
            self.assertEqual(len(leased), 1)
            self.assertEqual(leased[0]["payload"]["kind"], "active_delete")
            ack_delete = client.post(
                f"/api/qt/events/{leased[0]['id']}/ack",
                json={"ok": True},
            )
            self.assertEqual(ack_delete.status_code, 200)
            for response in [save_links, links_auth, reset_request, reset_confirm]:
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.json().get("ok"))
            self.assertEqual(save_links.json()["data"]["route"], "handover-links-save")
            self.assertTrue(links_auth.json()["data"]["authorized"])
            self.assertEqual(reset_request.json()["data"]["reset_id"], "reset1")
            self.assertTrue(reset_confirm.json()["data"]["code_ok"])
            self.assertEqual(generated.status_code, 200)
            self.assertTrue(generated.json().get("ok"))
            self.assertEqual(generated.json()["data"]["items"][0]["text"], "generated")
            self.assertEqual(send_generated_without_callback.status_code, 200)
            self.assertTrue(send_generated_without_callback.json().get("ok"))
            notice_response = client.get("/api/qt/events?limit=1")
            self.assertEqual(notice_response.status_code, 200)
            notice_items = notice_response.json()["data"]["items"]
            self.assertEqual(len(notice_items), 1)
            self.assertEqual(notice_items[0]["payload"]["kind"], "notice")
            for response in [
                bootstrap,
                overview,
                workbench,
                records,
                history,
                handover,
                current_request,
            ]:
                self.assertIn("auth", response.json().get("data") or {})
            self.assertEqual(bootstrap.json()["data"]["route"], "bootstrap")
            self.assertEqual(overview.json()["data"]["route"], "scope-overview")
            self.assertEqual(workbench.json()["data"]["route"], "workbench")
            self.assertEqual(records.json()["data"]["route"], "workbench")
            self.assertEqual(history.json()["data"]["route"], "history-summary")
            self.assertEqual(handover.json()["data"]["route"], "handover-links")
            self.assertTrue(
                refresh.json()["data"].get("source_refresh_started")
                or refresh.json()["data"].get("source_refresh_inflight")
                or refresh.json()["data"].get("source_refresh_reused")
            )
            self.assertTrue(repair_refresh.json()["data"]["repair_source_refreshed"])

            notices = []
            PortalHandler.notice_callback = lambda payload: notices.append(payload)
            try:
                with patch.object(controller, "_proxy_request", side_effect=AssertionError("proxy used")):
                    sent = client.post(
                        "/api/send-generated",
                        headers=headers,
                        json={
                            "scope": "ALL",
                            "items": [{"record_id": "m1", "title": "t", "text": "x"}],
                        },
                    )
            finally:
                PortalHandler.notice_callback = None
            self.assertEqual(sent.status_code, 200)
            self.assertTrue(sent.json().get("ok"))
            self.assertEqual(sent.json()["data"]["items"][0]["record_id"], "m1")
            self.assertEqual(len(notices), 1)
        finally:
            PortalHandler.service = original_service
            PortalHandler.state_store = original_state_store
            temp_dir.cleanup()
            with PortalHandler.auth_manager._lock:
                PortalHandler.auth_manager._sessions = original_sessions

    def test_fastapi_ongoing_delete_runs_backend_delete_before_qt_projection(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalHandler.service
        original_state_store = PortalHandler.state_store
        original_auth_state_store = PortalHandler.auth_manager._state_store
        original_sessions = dict(PortalHandler.auth_manager._sessions)
        PortalHandler.service = _NativeFastAPIRouteService()
        temp_dir = tempfile.TemporaryDirectory()
        PortalHandler.state_store = LanPortalStateStore(
            Path(temp_dir.name) / "state.sqlite3"
        )
        PortalHandler.auth_manager._state_store = PortalHandler.state_store
        PortalHandler.auth_manager.upsert_permission_user(
            open_id="ou_delete",
            name="测试用户",
            role="admin",
            scopes=["ALL"],
            enabled=True,
            updated_by="test",
        )
        session_id = "delete-route-session"
        with PortalHandler.auth_manager._lock:
            PortalHandler.auth_manager._sessions[session_id] = {
                "session_id": session_id,
                "user": {"name": "测试用户", "open_id": "ou_delete"},
                "role": "admin",
                "allowed_scopes": ["ALL"],
                "expires_at": time.time() + 3600,
            }
        client = TestClient(controller._build_app())
        try:
            headers = {"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"}
            with patch.object(
                portal_server_module,
                "delete_bitable_record",
                return_value=(True, "deleted"),
            ) as delete_record:
                response = client.post(
                    "/api/ongoing-items/delete",
                    headers=headers,
                    json={
                        "scope": "ALL",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "active_item_id": "active-del",
                        "record_id": "rec-del",
                        "target_record_id": "rec-del",
                    },
                )
            self.assertEqual(response.status_code, 200)
            payload = response.json()["data"]
            self.assertTrue(payload["deleted"])
            self.assertTrue(payload["remote_deleted"])
            delete_record.assert_called_once_with("rec-del", "维保通告")
            leased_response = client.get("/api/qt/events?limit=1")
            self.assertEqual(leased_response.status_code, 200)
            leased = leased_response.json()["data"]["items"]
            self.assertEqual(len(leased), 1)
            self.assertEqual(leased[0]["payload"]["kind"], "active_delete")
            self.assertEqual(
                leased[0]["payload"]["payload"]["active_item_id"],
                "active-del",
            )
        finally:
            PortalHandler.service = original_service
            PortalHandler.state_store = original_state_store
            PortalHandler.auth_manager._state_store = original_auth_state_store
            temp_dir.cleanup()
            with PortalHandler.auth_manager._lock:
                PortalHandler.auth_manager._sessions = original_sessions

    def test_fastapi_qt_command_delete_clears_active_item_and_read_cache(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalHandler.service
        original_state_store = PortalHandler.state_store
        PortalHandler.service = _NativeFastAPIRouteService()
        temp_dir = tempfile.TemporaryDirectory()
        PortalHandler.state_store = LanPortalStateStore(
            Path(temp_dir.name) / "state.sqlite3"
        )
        PortalHandler.state_store.upsert_qt_active_item(
            {
                "active_item_id": "active-qt-delete",
                "record_id": "rec-qt-delete",
                "notice_type": "维保通告",
                "work_type": "maintenance",
                "title": "Qt 删除测试",
                "building_codes": ["A"],
            },
            section="other",
            origin="qt",
        )
        controller._read_cache_put(("health",), {"ok": True})
        client = TestClient(controller._build_app())
        try:
            with patch.object(
                portal_server_module,
                "delete_bitable_record",
                return_value=(True, "deleted"),
            ) as delete_record:
                response = client.post(
                    "/api/qt/commands",
                    json={
                        "command": "delete_active_item",
                        "payload": {
                            "data_dict": {
                                "scope": "A",
                                "work_type": "maintenance",
                                "notice_type": "维保通告",
                                "active_item_id": "active-qt-delete",
                                "record_id": "rec-qt-delete",
                            }
                        },
                    },
                )

            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json().get("ok"))
            delete_record.assert_called_once_with("rec-qt-delete", "维保通告")
            self.assertEqual(PortalHandler.state_store.list_qt_active_items(), [])
            self.assertEqual(controller._read_cache_stats()["entries"], 0)
            leased = PortalHandler.state_store.lease_outbox_events(
                "qt_action", limit=1, lease_seconds=5
            )
            self.assertEqual(leased[0]["payload"]["kind"], "active_delete")
        finally:
            PortalHandler.service = original_service
            PortalHandler.state_store = original_state_store
            temp_dir.cleanup()

    def test_fastapi_permission_request_routes_are_native(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_state_store = PortalHandler.auth_manager._state_store
        original_sessions = dict(PortalHandler.auth_manager._sessions)
        code_holder: dict[str, str] = {}
        with tempfile.TemporaryDirectory() as tmp:
            PortalHandler.auth_manager._state_store = LanPortalStateStore(
                Path(tmp) / "state.sqlite3"
            )
            session_id = "permission-route-session"
            with PortalHandler.auth_manager._lock:
                PortalHandler.auth_manager._sessions[session_id] = {
                    "session_id": session_id,
                    "user": {"name": "申请人", "open_id": "ou_requester"},
                    "role": "building",
                    "allowed_scopes": [],
                    "expires_at": time.time() + 3600,
                }
            client = TestClient(controller._build_app())

            def fake_send(text, recipients):
                match = re.search(r"验证码：(?P<code>\d{6})", text)
                if match:
                    code_holder["code"] = match.group("code")
                return True, "ok", [{"open_id": item, "ok": True} for item in recipients]

            try:
                with patch.object(controller, "_proxy_request", side_effect=AssertionError("proxy used")):
                    with patch(
                        "clipflow_backend.main.send_text_to_open_ids",
                        side_effect=fake_send,
                    ), patch.dict(
                        os.environ,
                        {
                            "CLIPFLOW_REQUIRE_REAL_EXTERNAL_CONFIRM": "1",
                            "CLIPFLOW_REAL_EXTERNAL_CONFIRMED": "1",
                        },
                        clear=False,
                    ):
                        headers = {"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"}
                        created = client.post(
                            "/api/auth/permission-requests",
                            headers=headers,
                            json={"scopes": ["A"], "reason": "测试申请"},
                        )
                        self.assertEqual(created.status_code, 200)
                        request_id = created.json()["data"]["request"]["request_id"]
                        self.assertRegex(code_holder.get("code", ""), r"^\d{6}$")
                        current = client.get(
                            "/api/auth/permission-requests/current",
                            headers=headers,
                        )
                        self.assertEqual(current.status_code, 200)
                        self.assertEqual(
                            current.json()["data"]["request"]["request_id"],
                            request_id,
                        )
                        confirmed = client.post(
                            "/api/auth/permission-requests/confirm",
                            headers=headers,
                            json={
                                "request_id": request_id,
                                "code": code_holder["code"],
                            },
                        )
                self.assertEqual(confirmed.status_code, 200)
                self.assertTrue(confirmed.json()["data"]["approved"])
                self.assertIn("A", confirmed.json()["data"]["auth"]["allowed_scopes"])
            finally:
                PortalHandler.auth_manager._state_store = original_state_store
                with PortalHandler.auth_manager._lock:
                    PortalHandler.auth_manager._sessions = original_sessions

    def test_backend_process_controller_prefers_portable_python(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            python_path = root / ".venv" / "Scripts" / "python.exe"
            python_path.parent.mkdir(parents=True, exist_ok=True)
            python_path.write_text("", encoding="utf-8")
            with patch.dict(os.environ, {"CLIPFLOW_BACKEND_PYTHON": ""}, clear=False):
                self.assertEqual(
                    BackendProcessPortalController._python_executable(root),
                    python_path,
                )

    def test_backend_process_controller_dispatches_notice_outbox_event(self):
        controller = BackendProcessPortalController(host="127.0.0.1", port=18766)
        received: list[dict] = []
        ack_payloads: list[dict] = []

        def fake_request(method, path, *, payload=None, timeout=5.0):
            ack_payloads.append(
                {
                    "method": method,
                    "path": path,
                    "payload": dict(payload or {}),
                }
            )
            return {"ok": True}

        controller.notice_callback = lambda payload: received.append(payload) or True
        with patch.object(controller, "_request_json", side_effect=fake_request):
            controller._dispatch_event(
                {
                    "id": 9,
                    "payload": {
                        "kind": "notice",
                        "payload": {"title": "测试通告", "text": "内容"},
                    },
                }
            )
        self.assertEqual(received, [{"title": "测试通告", "text": "内容"}])
        self.assertEqual(ack_payloads[0]["path"], "/api/qt/events/9/ack")
        self.assertTrue(ack_payloads[0]["payload"]["ok"])

    def test_backend_process_controller_retries_qt_event_ack(self):
        controller = BackendProcessPortalController(host="127.0.0.1", port=18766)
        attempts = {"count": 0}

        def flaky_ack(method, path, *, payload=None, timeout=5.0):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise OSError("temporary ack failure")
            return {"ok": True}

        with patch.object(controller, "_request_json", side_effect=flaky_ack):
            with patch("clipflow_backend.process_controller.time.sleep") as sleep:
                controller._ack_event(7, ok=True)
        self.assertEqual(attempts["count"], 3)
        self.assertEqual(sleep.call_count, 2)

    def test_backend_process_controller_posts_bridge_heartbeat(self):
        controller = BackendProcessPortalController(host="127.0.0.1", port=18766)
        payloads: list[dict] = []

        def fake_request(method, path, *, payload=None, timeout=5.0):
            payloads.append({"method": method, "path": path, "payload": dict(payload or {})})
            return {"ok": True}

        controller.notice_callback = lambda payload: True
        controller.maintenance_action_callback = lambda payload: {"ok": True}
        with patch.object(controller, "_request_json", side_effect=fake_request):
            controller._post_bridge_heartbeat(force=True)
        self.assertEqual(payloads[0]["path"], "/api/qt/bridge-heartbeat")
        self.assertTrue(payloads[0]["payload"]["notice_callback"])
        self.assertTrue(payloads[0]["payload"]["maintenance_action_callback"])

    def test_backend_process_controller_starts_and_stops_in_mock_external_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            port = _free_tcp_port()
            controller = BackendProcessPortalController(host="127.0.0.1", port=port)
            with patch.dict(
                os.environ,
                {
                    "CLIPFLOW_BACKEND_MOCK_EXTERNAL": "1",
                    "CLIPFLOW_DATA_DIR": tmp,
                    "CLIPFLOW_BACKEND_PYTHON": sys.executable,
                },
                clear=False,
            ):
                url = controller.start()
                process = controller._process
                try:
                    self.assertIn(str(port), url)
                    health = controller._request_json("GET", "/api/health", timeout=3.0)
                    self.assertTrue(health.get("ok"))
                    self.assertEqual(
                        (health.get("data") or {}).get("backend"),
                        "fastapi",
                    )
                    self.assertFalse((health.get("data") or {}).get("legacy_adapter"))
                    self.assertTrue((health.get("data") or {}).get("mock_external"))
                    events = controller._request_json("GET", "/api/qt/events", timeout=3.0)
                    self.assertTrue(events.get("ok"))
                finally:
                    controller.stop()
                if process is not None:
                    self.assertIsNotNone(process.poll())

    def test_manual_maintenance_requires_and_carries_cycle(self):
        service = _TestMaintenancePortalService()
        payload = {
            "manual": True,
            "manual_id": "manual:maintenance:1",
            "action": "start",
            "scope": "A",
            "work_type": "maintenance",
            "title": "手动维保",
            "building": "A楼",
            "specialty": "电气",
            "maintenance_cycle": "每月",
            "start_time": "2026-05-15T09:30",
            "end_time": "2026-05-15T18:30",
            "location": "A楼",
            "content": "手动内容",
            "reason": "手动原因",
            "impact": "无",
            "progress": "准备开始",
        }
        prepared = service.prepare_maintenance_action(payload, job_id="job1")
        self.assertTrue(prepared["manual"])
        self.assertEqual(prepared["record_id"], "manual:maintenance:1")
        self.assertEqual(prepared["maintenance_cycle"], "每月")
        self.assertEqual(prepared["source_app_token"], "")

        payload["maintenance_cycle"] = ""
        with self.assertRaises(PortalError):
            service.prepare_maintenance_action(payload, job_id="job2")

    def test_manual_change_and_repair_do_not_require_source_records(self):
        service = _TestMaintenancePortalService()
        change = service.prepare_change_action(
            {
                "manual": True,
                "manual_id": "manual:change:1",
                "action": "start",
                "scope": "A",
                "title": "手动变更",
                "building": "A楼",
                "specialty": "电气",
                "start_time": "2026-05-15T09:30",
                "end_time": "2026-05-15T18:30",
                "location": "A楼",
            },
            job_id="job3",
        )
        self.assertTrue(change["manual"])
        self.assertEqual(change["record_id"], "manual:change:1")
        self.assertEqual(change["source_app_token"], "")

        repair = service.prepare_repair_action(
            {
                "manual": True,
                "manual_id": "manual:repair:1",
                "action": "start",
                "scope": "A",
                "title": "手动检修",
                "building": "A楼",
                "specialty": "暖通",
                "expected_time": "2026-05-15T23:50",
                "fault_time": "2026-05-15T08:00",
                "location": "A楼",
            },
            job_id="job4",
        )
        self.assertTrue(repair["manual"])
        self.assertEqual(repair["record_id"], "manual:repair:1")
        self.assertEqual(repair["source_app_token"], "")


if __name__ == "__main__":
    unittest.main()

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
import ast
import re
import gc
from types import SimpleNamespace
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import httpx

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import MaintenancePortalService  # noqa: E402
from lan_bitable_template_portal.portal_service import PortalError  # noqa: E402
from lan_bitable_template_portal.portal_service import NOTICE_TEXT_TEMPLATES  # noqa: E402
from lan_bitable_template_portal.portal_service import RECENT_MONTH_FILTER_LABEL  # noqa: E402
from lan_bitable_template_portal.portal_service import FieldMeta  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_SOURCE_APP_TOKEN  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_SOURCE_TABLE_ID  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_MANAGEMENT_TABLE_ID  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_MANAGEMENT_RETIRED_FIELD_NAMES  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_MANAGEMENT_REPAIR_TABLE_ID  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_MANAGEMENT_REPAIR_LINK_STORAGE_FIELD_NAME  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_FOLLOWUP_TABLE_ID  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_EQUIPMENT_CATALOG_TABLE_ID  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_FOLLOWUP_CATALOG_MAX_RECORDS  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_FOLLOWUP_CATALOG_RUNTIME_KEY  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_FOLLOWUP_BACKFILL_RUNTIME_KEY  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_CMDB_TABLE_ID  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_FOLLOWUP_CMDB_FIELD_NAME  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS  # noqa: E402
from lan_bitable_template_portal.portal_service import REPAIR_SNAPSHOT_SOURCE_PROJECTS  # noqa: E402
from lan_bitable_template_portal.portal_service import WORK_TYPE_CHANGE  # noqa: E402
from lan_bitable_template_portal.portal_service import WORK_TYPE_MAINTENANCE  # noqa: E402
from lan_bitable_template_portal.portal_service import WORK_TYPE_POWER  # noqa: E402
from lan_bitable_template_portal.portal_service import WORK_TYPE_REPAIR  # noqa: E402
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME  # noqa: E402
from lan_bitable_template_portal.portal_auth import PortalAuthManager  # noqa: E402
from lan_bitable_template_portal.workbench_lite import (  # noqa: E402
    MAINTENANCE_CYCLE_OPTIONS as PORTAL_MAINTENANCE_CYCLE_OPTIONS,
)
import lan_bitable_template_portal.workbench_lite as workbench_lite_module  # noqa: E402
from lan_bitable_template_portal.workbench_lite import parse_pasted_notice_to_draft  # noqa: E402
import lan_bitable_template_portal.server as portal_server_module  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402
from clipflow_backend.main import FastAPIPortalController  # noqa: E402
from clipflow_backend.process_controller import BackendProcessPortalController  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
import upload_event_module.config as config_module  # noqa: E402
from upload_event_module.config import ConfigManager  # noqa: E402
from upload_event_module.config import get_field_config  # noqa: E402
from upload_event_module.config import ADJUST_NOTICE_FIELDS  # noqa: E402
from upload_event_module.config import CHANGE_NOTICE_FIELDS  # noqa: E402
from upload_event_module.config import MAINTENANCE_NOTICE_FIELDS  # noqa: E402
from upload_event_module.config import POLLING_NOTICE_FIELDS  # noqa: E402
from upload_event_module.config import POWER_NOTICE_FIELDS  # noqa: E402
from upload_event_module.config import STATUS_START  # noqa: E402
from upload_event_module.services.http_client import FeishuHttpClient  # noqa: E402
from upload_event_module.services.feishu_token_manager import (  # noqa: E402
    FeishuTokenManager,
)
from upload_event_module.services.remote_patch_updater import RemotePatchUpdater  # noqa: E402
from upload_event_module.services.handlers.base import NoticePayload  # noqa: E402
from upload_event_module.services.handlers.maintenance_notice import (  # noqa: E402
    MaintenanceNoticeHandler,
)
from upload_event_module.services.handlers.change_notice import (  # noqa: E402
    ChangeNoticeHandler,
)
from upload_event_module.services.handlers.event_notice import (  # noqa: E402
    EventNoticeHandler,
)
from upload_event_module.services.handlers.polling_notice import (  # noqa: E402
    PollingNoticeHandler,
)
from upload_event_module.services.handlers.power_notice import (  # noqa: E402
    PowerNoticeHandler,
)
from upload_event_module.services.handlers.overhaul_notice import (  # noqa: E402
    OverhaulNoticeHandler,
)
from upload_event_module.services.handlers.device_adjust_notice import (  # noqa: E402
    AdjustNoticeHandler,
)
from upload_event_module.core.parser import extract_event_info  # noqa: E402
import upload_event_module.services.feishu_service as feishu_service_module  # noqa: E402
from upload_event_module.ui.active_cache_store import ActiveCacheStore  # noqa: E402
from upload_event_module.ui.dialogs import ScreenshotConfirmDialog  # noqa: E402
from upload_event_module.ui.main_window_records import MainWindowRecordsMixin  # noqa: E402
from upload_event_module.ui.main_window_workflow import MainWindowWorkflowMixin  # noqa: E402
from upload_event_module.ui.main_window_ui import MainWindowUiMixin  # noqa: E402
from upload_event_module.ui.main_window_patch import PatchUpdateMixin  # noqa: E402

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

_TEST_MONTH_START = MaintenancePortalService._recent_month_starts()[0]
_TEST_MONTH_LABEL = f"{_TEST_MONTH_START.month}月"
_TEST_MONTH_KEY = _TEST_MONTH_START.strftime("%Y-%m")


def _test_datetime(day: int = 8, time_text: str = "09:00") -> str:
    return f"{_TEST_MONTH_KEY}-{day:02d} {time_text}"


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
        self._records = [_build_record("m1", "A楼", "过滤网维护", _TEST_MONTH_LABEL)]
        self._maintenance_loaded_once = True
        return self._records

    def _load_change_fields(self):
        raise RuntimeError("mock change source down")

    def _load_zhihang_change_fields(self):
        self._zhihang_change_field_meta_list = []
        self._zhihang_change_field_meta_by_name = {}
        return []

    def _load_zhihang_change_records(self):
        self._zhihang_change_records = []
        self._zhihang_change_loaded_once = True
        return self._zhihang_change_records

    def _load_repair_fields(self):
        self._repair_field_meta_list = []
        self._repair_field_meta_by_name = {}
        return []

    def _load_repair_records(self):
        self._repair_records = []
        self._repair_loaded_once = True
        return self._repair_records


class _MemoryUploadFieldCache:
    def __init__(self, fields=None):
        self.fields = dict(fields or {})
        self.patches = []

    def get_record_fields(self, *, record_id="", active_item_id="", fields=None):
        return {key: self.fields[key] for key in (fields or []) if key in self.fields}

    def patch_record_fields(self, *, record_id="", active_item_id="", patch=None):
        patch = dict(patch or {})
        self.patches.append(patch)
        for key, value in patch.items():
            if value is None:
                self.fields.pop(key, None)
            else:
                self.fields[key] = value


class _UploadFieldResolverHarness(MainWindowRecordsMixin):
    def __init__(self, cache_fields=None):
        self.cache_store = _MemoryUploadFieldCache(cache_fields)

    def _normalize_buildings_value(self, value):
        if isinstance(value, (list, tuple, set)):
            return [str(item).strip() for item in value if str(item).strip()]
        text = str(value or "").strip()
        return [text] if text else []

    def _get_cache_identity(self, data_dict):
        return str(data_dict.get("target_record_id") or data_dict.get("record_id") or "")

    def _reconcile_unlocked_change_notice_level(
        self, data_dict, *, level="", level_locked=False, persist=False
    ):
        return str(level or data_dict.get("level") or "").strip()


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

    def _merge_ongoing_items(self, scope, ongoing_items):
        return list(ongoing_items or [])

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

    def refresh_change_source(self):
        return {"change_refresh_reused": False, "change_refresh_mock": True}

    def get_scope_overview(self, *, ongoing_items, scopes, include_prepared):
        return {
            "scope_options": [{"value": "ALL", "label": "全部"}],
            "scopes": scopes,
            "ongoing": ongoing_items,
            "route": "scope-overview",
        }

    def query_records(
        self,
        *,
        month="",
        specialty="",
        search="",
        scope,
        ongoing_items,
        work_type="",
        sections=None,
        records_page=1,
        records_page_size=0,
        ongoing_page=1,
        ongoing_page_size=0,
    ):
        return {
            "scope": scope,
            "month": month,
            "specialty": specialty,
            "search": search,
            "work_type": work_type,
            "sections": list(sections or []),
            "records_page": records_page,
            "records_page_size": records_page_size,
            "ongoing_page": ongoing_page,
            "ongoing_page_size": ongoing_page_size,
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
    start_time: str | None = None,
    end_time: str | None = None,
):
    if start_time is None:
        start_time = _test_datetime(8, "09:00")
    if end_time is None:
        end_time = _test_datetime(8, "18:00")
    return {
        "record_id": record_id,
        "raw_fields": {},
        "work_type": "change",
        "notice_type": "变更通告",
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
    fault_time: str | None = None,
    expected_time: str = "",
    started: bool = False,
    ended: bool = False,
    target_record_id: str = "",
):
    if fault_time is None:
        fault_time = _test_datetime(8, "08:20")
    raw_fields = {}
    if target_record_id:
        raw_fields["设备检修关联"] = target_record_id
    return {
        "record_id": record_id,
        "raw_fields": raw_fields,
        "work_type": "repair",
        "notice_type": "设备检修",
        "source_app_token": "AnEBwJlvGiJfDdkOB32cUPuknzg",
        "source_table_id": REPAIR_SOURCE_TABLE_ID,
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
            "维修开始时间": _test_datetime(8, "09:00") if started else "",
            "维修结束时间": _test_datetime(8, "18:00") if ended else "",
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
    plan_start: str | None = None,
    plan_end: str | None = None,
):
    if plan_start is None:
        plan_start = _test_datetime(8, "09:00")
    if plan_end is None:
        plan_end = _test_datetime(8, "18:00")
    return {
        "record_id": record_id,
        "raw_fields": {},
        "work_type": "change",
        "notice_type": "变更通告",
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

        class _Client:
            @staticmethod
            def request_json(method, url, **kwargs):
                captured["method"] = method
                captured["url"] = url
                captured["kwargs"] = kwargs
                return {"code": 0, "data": {"items": []}}

        service._http_client = _Client()
        service._auth_headers = lambda: {"Authorization": "Bearer test"}

        service._request_json("fields", params={"page_size": 500})

        self.assertEqual(captured["method"], "GET")
        self.assertIn("/apps/HU38bc1vnamMK9sCeOgclUvXnFc/", captured["url"])
        self.assertIn("/tables/tblzk7WrXxNWQy6V/", captured["url"])
        self.assertNotIn("/apps//", captured["url"])
        self.assertNotIn("/tables//", captured["url"])

    def test_table_field_loader_reads_all_pages(self):
        service = MaintenancePortalService()
        calls = []

        def fake_request(path, *, params=None, app_token=None, table_id=None):
            self.assertEqual(path, "fields")
            calls.append(dict(params or {}))
            if not (params or {}).get("page_token"):
                return {
                    "code": 0,
                    "data": {
                        "items": [
                            {
                                "field_id": "fld_first",
                                "field_name": "第一页字段",
                                "type": 1,
                                "ui_type": "Text",
                            }
                        ],
                        "has_more": True,
                        "page_token": "next_fields_page",
                    },
                }
            return {
                "code": 0,
                "data": {
                    "items": [
                        {
                            "field_id": "fld_second",
                            "field_name": "第二页字段",
                            "type": 3,
                            "ui_type": "SingleSelect",
                            "property": {"options": [{"id": "opt1", "name": "电气"}]},
                        }
                    ],
                    "has_more": False,
                },
            }

        service._request_json = fake_request  # type: ignore[method-assign]

        metas, meta_by_name = service._load_table_fields(
            app_token="app_test",
            table_id="table_test",
        )

        self.assertEqual([meta.field_name for meta in metas], ["第一页字段", "第二页字段"])
        self.assertEqual(meta_by_name["第二页字段"].option_names, ["电气"])
        self.assertEqual(
            calls,
            [
                {"page_size": 500},
                {"page_size": 500, "page_token": "next_fields_page"},
            ],
        )

    def test_repair_management_uses_original_tables_only(self):
        self.assertEqual(REPAIR_SOURCE_TABLE_ID, "tblschT48zXwigUG")
        self.assertEqual(REPAIR_MANAGEMENT_TABLE_ID, "tblschT48zXwigUG")
        self.assertEqual(REPAIR_FOLLOWUP_TABLE_ID, "tblkJByibuNWWGJh")

    def test_repair_suffix_fields_are_exposed_as_business_names(self):
        service = MaintenancePortalService()
        metas = service._parse_field_metas(
            [
                {
                    "field_id": "fld_formula",
                    "field_name": "所属专业",
                    "type": 20,
                    "ui_type": "Formula",
                    "property": {"formula_expression": "legacy"},
                },
                {
                    "field_id": "fld_writable",
                    "field_name": "所属专业-L",
                    "type": 3,
                    "ui_type": "SingleSelect",
                    "property": {
                        "options": [{"id": "opt_hvac", "name": "暖通"}]
                    },
                },
                {
                    "field_id": "fld_legacy_stage",
                    "field_name": "流程",
                    "type": 24,
                    "ui_type": "Stage",
                },
                {
                    "field_id": "fld_workflow_l",
                    "field_name": "流程-L",
                    "type": 3,
                    "ui_type": "SingleSelect",
                    "property": {
                        "options": [
                            {"id": "opt_pending", "name": "未开始"},
                            {"id": "opt_running", "name": "维修中"},
                            {"id": "opt_done", "name": "维修完成"},
                        ]
                    },
                },
                {
                    "field_id": "fld_retired_repair_link",
                    "field_name": "设备检修关联",
                    "type": 21,
                    "ui_type": "DuplexLink",
                },
                {
                    "field_id": "fld_repair_link_l",
                    "field_name": "设备检修关联-L",
                    "type": 1,
                    "ui_type": "Text",
                },
            ]
        )

        projected = service._repair_logical_field_metas(
            REPAIR_MANAGEMENT_TABLE_ID,
            metas,
        )
        by_name = {meta.field_name: meta for meta in projected}

        self.assertNotIn("所属专业-L", by_name)
        self.assertEqual(by_name["所属专业"].field_id, "fld_writable")
        self.assertEqual(by_name["所属专业"].field_type, 3)
        self.assertEqual(by_name["所属专业"].option_names, ["暖通"])
        self.assertEqual(by_name["流程"].field_id, "fld_workflow_l")
        self.assertEqual(by_name["流程"].field_type, 3)
        self.assertEqual(
            by_name["流程"].option_names,
            ["未开始", "维修中", "维修完成"],
        )
        self.assertNotIn("流程-L", by_name)
        self.assertEqual(
            by_name["设备检修关联"].field_id,
            "fld_repair_link_l",
        )
        self.assertEqual(by_name["设备检修关联"].field_type, 1)
        self.assertNotIn("设备检修关联-L", by_name)

    def test_repair_suffix_record_read_prefers_writable_value_with_legacy_fallback(self):
        service = MaintenancePortalService()
        preferred = service._repair_logical_record_fields(
            REPAIR_MANAGEMENT_TABLE_ID,
            {
                "所属专业": "电气",
                "所属专业-L": "暖通",
                "流程": "旧流程状态",
                "流程-L": "维修中",
                "设备检修关联": [{"record_id": "rec_retired_relation"}],
                "设备检修关联-L": "rec_current_target",
            },
        )
        fallback = service._repair_logical_record_fields(
            REPAIR_MANAGEMENT_TABLE_ID,
            {
                "所属专业": "消防",
                "所属专业-L": "",
                "流程": "旧流程状态",
                "流程-L": "",
                "设备检修关联": [{"record_id": "rec_retired_relation"}],
                "设备检修关联-L": "",
            },
        )

        self.assertEqual(preferred["所属专业"], "暖通")
        self.assertEqual(fallback["所属专业"], "消防")
        self.assertNotIn("所属专业-L", preferred)
        self.assertEqual(preferred["流程"], "维修中")
        self.assertNotIn("流程", fallback)
        self.assertNotIn("流程-L", preferred)
        self.assertEqual(preferred["设备检修关联"], "rec_current_target")
        self.assertNotIn("设备检修关联", fallback)
        self.assertEqual(
            service._repair_logical_display_value(
                REPAIR_MANAGEMENT_TABLE_ID,
                "维修名称",
                {
                    "type": 1,
                    "value": [{"text": "历史维修名称", "type": "text"}],
                },
            ),
            "历史维修名称",
        )

    def test_repair_record_skips_retired_fields_and_writes_active_suffix_fields(self):
        summary_fields = MaintenancePortalService._repair_physical_record_fields(
            REPAIR_MANAGEMENT_TABLE_ID,
            {
                "维修名称": "测试维修",
                "所属专业": "电气",
                "流程": "处理中",
                "设备检修关联": "rec_repair_target",
                "CMDB唯一id": "ZH-CMDB-1、ZH-CMDB-2",
            },
        )
        followup_fields = MaintenancePortalService._repair_physical_record_fields(
            REPAIR_FOLLOWUP_TABLE_ID,
            {
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary",
                "设备名称": "A-219-CRAH-01",
                "设备编号": "A-219-CRAH-01",
                "维修进度": 50,
            },
        )

        self.assertEqual(summary_fields["所属专业-L"], "电气")
        self.assertEqual(summary_fields["流程-L"], "处理中")
        self.assertEqual(
            summary_fields["设备检修关联-L"],
            "rec_repair_target",
        )
        self.assertEqual(
            summary_fields["CMDB唯一id-L"],
            "ZH-CMDB-1、ZH-CMDB-2",
        )
        self.assertNotIn("设备检修关联", summary_fields)
        self.assertNotIn("流程", summary_fields)
        self.assertNotIn("维修名称", summary_fields)
        self.assertNotIn("维修名称-L", summary_fields)
        self.assertEqual(
            followup_fields[f"{REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME}-L"],
            "rec_summary",
        )
        self.assertEqual(followup_fields["设备名称-L"], "A-219-CRAH-01")
        self.assertEqual(followup_fields["设备编号-L"], "A-219-CRAH-01")
        self.assertEqual(followup_fields["维修进度"], 50)

    def test_repair_create_request_sends_only_physical_suffix_fields(self):
        service = MaintenancePortalService()
        captured = {}
        service._auth_headers = lambda: {"Authorization": "Bearer test"}  # type: ignore[method-assign]

        def fake_request_payload(*_args, **kwargs):
            captured.update(kwargs.get("json_payload") or {})
            return {"code": 0, "data": {"record": {"record_id": "rec_test"}}}

        service._request_payload = fake_request_payload  # type: ignore[method-assign]

        service._create_record_fields(
            app_token=REPAIR_SOURCE_APP_TOKEN,
            table_id=REPAIR_MANAGEMENT_TABLE_ID,
            fields={
                "维修名称": "测试维修",
                "所属专业": "暖通",
                "流程": "处理中",
                "设备检修关联": "rec_repair_target",
            },
        )

        self.assertEqual(
            captured["fields"],
            {
                "所属专业-L": "暖通",
                "流程-L": "处理中",
                "设备检修关联-L": "rec_repair_target",
            },
        )

    def test_repair_snapshot_ignores_legacy_stage_workflow(self):
        service = MaintenancePortalService()
        legacy_snapshot = {
            "table_id": REPAIR_MANAGEMENT_TABLE_ID,
            "fields": [
                {
                    "field_id": "fld_legacy_stage",
                    "field_name": "流程",
                    "ui_type": "Stage",
                    "field_type": 24,
                }
            ],
            "records": [
                {
                    "record_id": "rec_legacy_stage",
                    "raw_fields": {"流程": "opt_legacy"},
                    "display_fields": {"流程": "旧流程状态"},
                }
            ],
        }

        metas, meta_by_name, records = service._repair_snapshot_from_local(
            legacy_snapshot
        )

        self.assertNotIn("流程", meta_by_name)
        self.assertNotIn("流程", records[0]["raw_fields"])
        self.assertNotIn("流程", records[0]["display_fields"])
        self.assertEqual(metas, [])

    def test_repair_snapshot_refresh_bypasses_fresh_legacy_workflow_cache(self):
        service = MaintenancePortalService()
        legacy_snapshot = {
            "app_token": REPAIR_SOURCE_APP_TOKEN,
            "table_id": REPAIR_MANAGEMENT_TABLE_ID,
            "refreshed_at": time.time(),
            "fields": [
                {
                    "field_id": "fld_legacy_stage",
                    "field_name": "流程",
                    "ui_type": "Stage",
                    "field_type": 24,
                }
            ],
            "records": [
                {
                    "record_id": "rec_legacy_stage",
                    "raw_fields": {"流程": "opt_legacy"},
                    "display_fields": {"流程": "旧流程状态"},
                }
            ],
        }
        replacement = {}
        service._state_store.get_repair_snapshot_meta = (  # type: ignore[method-assign]
            lambda _source_key: {"refreshed_at": time.time()}
        )
        service._state_store.get_repair_snapshot = (  # type: ignore[method-assign]
            lambda _source_key, **_kwargs: legacy_snapshot
        )
        service._state_store.replace_repair_snapshot = (  # type: ignore[method-assign]
            lambda source_key, **kwargs: replacement.update(
                {"source_key": source_key, **kwargs}
            )
        )
        loader_calls = []
        workflow_meta = FieldMeta(
            "fld_workflow_l",
            "流程",
            "SingleSelect",
            3,
            False,
            {
                "opt_pending": "未开始",
                "opt_running": "维修中",
                "opt_done": "维修完成",
            },
            ["未开始", "维修中", "维修完成"],
            False,
        )

        def loader():
            loader_calls.append(True)
            return [workflow_meta], []

        metas, meta_by_name, _records = service._refresh_repair_snapshot_source(
            source_key=REPAIR_SNAPSHOT_SOURCE_PROJECTS,
            app_token=REPAIR_SOURCE_APP_TOKEN,
            table_id=REPAIR_MANAGEMENT_TABLE_ID,
            loader=loader,
        )

        self.assertEqual(len(loader_calls), 1)
        self.assertEqual(metas[0].field_type, 3)
        self.assertEqual(meta_by_name["流程"].ui_type, "SingleSelect")
        self.assertEqual(replacement["fields"][0]["field_type"], 3)

    def test_repair_search_requests_only_existing_legacy_fallback_fields(self):
        service = MaintenancePortalService()
        captured = []
        service._auth_headers = lambda: {"Authorization": "Bearer test"}  # type: ignore[method-assign]

        def fake_request_payload(*_args, **kwargs):
            captured.append(kwargs.get("json_payload") or {})
            return {"code": 0, "data": {"items": [], "has_more": False}}

        service._request_payload = fake_request_payload  # type: ignore[method-assign]
        name_meta = FieldMeta(
            "fld_name_l",
            "维修名称",
            "Text",
            1,
            False,
            {},
            [],
            False,
            read_fallback_field_name="维修名称",
        )
        parent_meta = FieldMeta(
            "fld_parent_l",
            REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
            "Text",
            1,
            False,
            {},
            [],
            False,
        )

        service._search_table_records(
            app_token=REPAIR_SOURCE_APP_TOKEN,
            table_id=REPAIR_MANAGEMENT_TABLE_ID,
            meta_by_name={"维修名称": name_meta},
            work_type=WORK_TYPE_REPAIR,
            notice_type="设备检修",
            field_names=("维修名称",),
            limit=1,
        )
        service._search_table_records(
            app_token=REPAIR_SOURCE_APP_TOKEN,
            table_id=REPAIR_FOLLOWUP_TABLE_ID,
            meta_by_name={REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: parent_meta},
            work_type=WORK_TYPE_REPAIR,
            notice_type="设备检修",
            field_names=(REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,),
            limit=1,
        )

        self.assertEqual(captured[0]["field_names"], ["维修名称"])
        self.assertEqual(
            captured[1]["field_names"],
            [f"{REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME}-L"],
        )

    def test_repair_management_retired_fields_are_hidden_and_never_written(self):
        service = MaintenancePortalService()
        retired_physical_names = {
            f"{name}-L" for name in REPAIR_MANAGEMENT_RETIRED_FIELD_NAMES
        }
        metas = service._parse_field_metas(
            [
                {
                    "field_id": "fld_formula_name",
                    "field_name": "维修名称",
                    "type": 20,
                    "ui_type": "Formula",
                    "property": {"formula_expression": "legacy"},
                },
                {
                    "field_id": "fld_retired_name",
                    "field_name": "维修名称-L",
                    "type": 1,
                    "ui_type": "Text",
                },
                {
                    "field_id": "fld_specialty",
                    "field_name": "所属专业-L",
                    "type": 3,
                    "ui_type": "SingleSelect",
                    "property": {"options": [{"id": "opt1", "name": "电气"}]},
                },
            ]
        )

        projected = service._repair_logical_field_metas(
            REPAIR_MANAGEMENT_TABLE_ID,
            metas,
        )
        projected_names = {meta.field_name for meta in projected}
        physical = service._repair_physical_record_fields(
            REPAIR_MANAGEMENT_TABLE_ID,
            {
                **{name: "不应写入" for name in REPAIR_MANAGEMENT_RETIRED_FIELD_NAMES},
                **{name: "不应写入" for name in retired_physical_names},
                "所属专业": "电气",
            },
        )

        self.assertIn("维修名称", projected_names)
        self.assertNotIn("维修名称-L", projected_names)
        self.assertFalse(retired_physical_names & set(physical))
        self.assertFalse(REPAIR_MANAGEMENT_RETIRED_FIELD_NAMES & set(physical))
        self.assertEqual(physical["所属专业-L"], "电气")

    def test_end_site_photo_required_only_for_core_notice_types(self):
        self.assertTrue(
            MaintenancePortalService._end_site_photo_required(
                notice_type="维保通告",
            )
        )
        self.assertTrue(
            MaintenancePortalService._end_site_photo_required(
                notice_type="变更通告",
            )
        )
        self.assertTrue(
            MaintenancePortalService._end_site_photo_required(
                notice_type="设备检修",
            )
        )
        self.assertFalse(
            MaintenancePortalService._end_site_photo_required(
                notice_type="上下电通告",
                work_type="power",
            )
        )
        self.assertFalse(
            MaintenancePortalService._end_site_photo_required(
                notice_type="设备轮巡",
                work_type="polling",
            )
        )
        self.assertFalse(
            MaintenancePortalService._end_site_photo_required(
                notice_type="设备调整",
                work_type="adjust",
            )
        )
        MaintenancePortalService._require_end_site_photo(
            {},
            "end",
            notice_type="上下电通告",
            work_type="power",
        )
        with self.assertRaises(PortalError):
            MaintenancePortalService._require_end_site_photo(
                {},
                "end",
                notice_type="变更通告",
                work_type="change",
            )

    def test_cumulative_end_site_photo_allows_previous_upload(self):
        service = MaintenancePortalService()
        service._has_existing_site_photo_for_notice = lambda *args, **kwargs: True
        service._require_end_site_photo_cumulative(
            {},
            "end",
            notice_type="维保通告",
            work_type="maintenance",
        )

        service._has_existing_site_photo_for_notice = lambda *args, **kwargs: False
        with self.assertRaises(PortalError):
            service._require_end_site_photo_cumulative(
                {},
                "end",
                notice_type="维保通告",
                work_type="maintenance",
            )

    def test_portal_runtime_routes_change_site_photos_to_site_field(self):
        self.assertTrue(PortalRuntime._notice_supports_site_image_field("变更通告"))
        self.assertTrue(PortalRuntime._notice_supports_site_image_field("变更通告"))
        self.assertTrue(PortalRuntime._end_site_photo_required("维护通告"))
        self.assertTrue(PortalRuntime._end_site_photo_required("检修通告"))
        self.assertFalse(PortalRuntime._notice_supports_site_image_field("设备调整"))
        self.assertTrue(PortalRuntime._end_site_photo_required("设备检修"))
        self.assertFalse(PortalRuntime._end_site_photo_required("设备轮巡"))

    def test_portal_runtime_maps_all_notice_types_to_work_types(self):
        self.assertEqual(PortalRuntime._notice_work_type_from_notice_type("维保通告"), "maintenance")
        self.assertEqual(PortalRuntime._notice_work_type_from_notice_type("变更通告"), "change")
        self.assertEqual(PortalRuntime._notice_work_type_from_notice_type("设备检修"), "repair")
        self.assertEqual(PortalRuntime._notice_work_type_from_notice_type("上电通告"), "power")
        self.assertEqual(PortalRuntime._notice_work_type_from_notice_type("下电通告"), "power")
        self.assertEqual(PortalRuntime._notice_work_type_from_notice_type("设备轮巡"), "polling")
        self.assertEqual(PortalRuntime._notice_work_type_from_notice_type("设备调整"), "adjust")

    def test_target_record_id_from_payload_only_falls_back_for_update_or_end(self):
        service = MaintenancePortalService()
        self.assertEqual(
            service._target_record_id_from_request_payload(
                {"action": "update", "record_id": "rec-target-1"}
            ),
            "rec-target-1",
        )
        self.assertEqual(
            service._target_record_id_from_request_payload(
                {"action": "end", "record_id": "rec-target-1"}
            ),
            "rec-target-1",
        )
        self.assertEqual(
            service._target_record_id_from_request_payload(
                {"action": "start", "record_id": "rec-source-1"}
            ),
            "",
        )
        self.assertEqual(
            service._target_record_id_from_request_payload(
                {
                    "action": "update",
                    "record_id": "rec-source-1",
                    "source_record_id": "rec-source-1",
                },
                source_record_id="rec-source-1",
            ),
            "",
        )

    def test_portal_runtime_reads_existing_change_site_photos(self):
        _, existing_extra_tokens, _ = PortalRuntime._existing_tokens_for_notice_type(
            "变更通告",
            {
                "过程更新钉钉截图": [{"file_token": "notice_token"}],
                "过程现场图片": [{"file_token": "site_token"}],
            },
        )

        self.assertEqual(existing_extra_tokens, ["site_token"])

    def test_merge_ongoing_items_dedupes_web_active_and_target_projection(self):
        service = MaintenancePortalService()

        merged = service._merge_ongoing_items(
            "A",
            [
                {
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "active_item_id": "active-1",
                    "source_record_id": "src-1",
                    "title": "测试维保通告",
                    "building": "A楼",
                    "start_time": "2026-06-15 09:30",
                    "end_time": "2026-06-15 18:30",
                    "progress": "准备中",
                },
                {
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "source_record_id": "src-1",
                    "target_record_id": "target-1",
                    "title": "测试维保通告",
                    "building": "A楼",
                    "start_time": "2026-06-15 09:30",
                    "end_time": "2026-06-15 18:30",
                    "location": "A楼",
                },
            ],
        )

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["target_record_id"], "target-1")
        self.assertEqual(merged[0]["active_item_id"], "active-1")

    def test_merge_ongoing_items_keeps_distinct_uploaded_records_with_different_reason(self):
        service = MaintenancePortalService()

        merged = service._merge_ongoing_items(
            "A",
            [
                {
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "active_item_id": "active-1",
                    "target_record_id": "target-1",
                    "title": "A楼测试维保通告",
                    "building": "A楼",
                    "start_time": "2026-06-15 09:30",
                    "end_time": "2026-06-15 18:30",
                    "reason": "测试原因一",
                },
                {
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "active_item_id": "active-2",
                    "target_record_id": "target-2",
                    "title": "A楼测试维保通告",
                    "building": "A楼",
                    "start_time": "2026-06-15 09:30",
                    "end_time": "2026-06-15 18:30",
                    "reason": "测试原因二",
                },
            ],
        )

        self.assertEqual(len(merged), 2)
        self.assertEqual(
            {item["target_record_id"] for item in merged},
            {"target-1", "target-2"},
        )

    def test_merge_ongoing_items_collapses_exact_duplicates_but_keeps_period_or_time_changes(self):
        service = MaintenancePortalService()
        base = {
            "work_type": "maintenance",
            "notice_type": "维保通告",
            "title": "EA118机房C楼交直流列头柜及PDU维护",
            "building": "C楼",
            "start_time": "2026-06-18 09:30",
            "end_time": "2026-06-18 18:30",
            "location": "C楼",
            "content": "按计划对C栋直流列头柜及PDU季度维护",
            "reason": "按计划对C栋直流列头柜及PDU季度维护，保证供电正常",
            "impact": "对IT设备无影响，不会产生动环报警",
            "progress": "准备工作已完成，人员已就位，是否可以操作？",
        }
        monthly = {
            **base,
            "active_item_id": "active-2",
            "target_record_id": "target-2",
            "content": "按计划对C栋直流列头柜及PDU月度维护",
            "reason": "按计划对C栋直流列头柜及PDU月度维护，保证供电正常",
        }
        duplicate_quarterly = {
            **base,
            "active_item_id": "active-3",
            "target_record_id": "target-3",
        }
        later_quarterly = {
            **base,
            "active_item_id": "active-4",
            "target_record_id": "target-4",
            "start_time": "2026-06-18 10:30",
        }
        merged = service._merge_ongoing_items(
            "C",
            [
                {**base, "active_item_id": "active-1", "target_record_id": "target-1"},
                monthly,
                duplicate_quarterly,
                later_quarterly,
            ],
        )

        self.assertEqual(len(merged), 3)
        signatures = {
            (
                item.get("start_time"),
                item.get("content"),
                item.get("reason"),
            )
            for item in merged
        }
        self.assertIn(
            (
                "2026-06-18 09:30",
                "按计划对C栋直流列头柜及PDU季度维护",
                "按计划对C栋直流列头柜及PDU季度维护，保证供电正常",
            ),
            signatures,
        )
        self.assertIn(
            (
                "2026-06-18 09:30",
                "按计划对C栋直流列头柜及PDU月度维护",
                "按计划对C栋直流列头柜及PDU月度维护，保证供电正常",
            ),
            signatures,
        )
        self.assertIn(
            (
                "2026-06-18 10:30",
                "按计划对C栋直流列头柜及PDU季度维护",
                "按计划对C栋直流列头柜及PDU季度维护，保证供电正常",
            ),
            signatures,
        )

    def test_merge_ongoing_items_treats_progress_only_change_as_same_notice(self):
        service = MaintenancePortalService()
        base = {
            "work_type": "maintenance",
            "notice_type": "维保通告",
            "title": "EA118机房C楼交直流列头柜及PDU维护",
            "building": "C楼",
            "start_time": "2026-06-18 09:30",
            "end_time": "2026-06-18 18:30",
            "location": "C楼",
            "content": "按计划对C栋直流列头柜及PDU季度维护",
            "reason": "按计划对C栋直流列头柜及PDU季度维护，保证供电正常",
            "impact": "对IT设备无影响，不会产生动环报警",
        }

        merged = service._merge_ongoing_items(
            "C",
            [
                {
                    **base,
                    "active_item_id": "active-1",
                    "target_record_id": "target-1",
                    "progress": "准备工作已完成，人员已就位，是否可以操作？",
                },
                {
                    **base,
                    "active_item_id": "active-2",
                    "target_record_id": "target-2",
                    "progress": "工作已开始，正在检查设备状态",
                },
            ],
        )

        self.assertEqual(len(merged), 1)

    def test_change_handler_maps_extra_tokens_to_site_images(self):
        handler = ChangeNoticeHandler("变更通告")
        payload = NoticePayload(
            text=(
                "【变更通告】状态：结束\n"
                "【名称】测试变更\n"
                "【等级】低风险\n"
                "【时间】2026-06-12 09:30~2026-06-12 18:30\n"
                "【进度】测试完成"
            ),
            existing_extra_file_tokens=["old_site_token"],
            extra_file_tokens=["new_site_token"],
            response_time="2026-06-12 18:35",
        )

        fields = handler.build_update_fields(payload)

        self.assertEqual(
            fields[CHANGE_NOTICE_FIELDS["site_images"]],
            [
                {"file_token": "old_site_token"},
                {"file_token": "new_site_token"},
            ],
        )

    def test_change_handler_writes_single_select_specialty_on_create_and_update(self):
        handler = ChangeNoticeHandler("变更通告")
        create_fields = handler.build_create_fields(
            NoticePayload(
                text=(
                    "【变更通告】状态：开始\n"
                    "【名称】测试变更\n"
                    "【等级】低风险\n"
                    "【时间】2026-06-12 09:30~2026-06-12 18:30"
                ),
                specialty="电气专业",
            )
        )
        update_fields = handler.build_update_fields(
            NoticePayload(
                text=(
                    "【变更通告】状态：更新\n"
                    "【名称】测试变更\n"
                    "【等级】低风险\n"
                    "【时间】2026-06-12 09:30~2026-06-12 18:30"
                ),
                specialty="其他",
            )
        )
        end_fields = handler.build_update_fields(
            NoticePayload(
                text=(
                    "【变更通告】状态：结束\n"
                    "【名称】测试变更\n"
                    "【等级】低风险\n"
                    "【时间】2026-06-12 09:30~2026-06-12 18:30"
                ),
                specialty="消防",
                response_time="2026-06-12 18:35",
            )
        )

        self.assertEqual(create_fields[CHANGE_NOTICE_FIELDS["specialty"]], "电气")
        self.assertEqual(update_fields[CHANGE_NOTICE_FIELDS["specialty"]], "其它")
        self.assertEqual(end_fields[CHANGE_NOTICE_FIELDS["specialty"]], "消防")

        restored_fields = PortalRuntime._undo_restore_fields(
            "变更通告",
            {"专业": "暖通", "不存在字段": "不应恢复"},
        )
        self.assertEqual(restored_fields["专业"], "暖通")
        self.assertNotIn("不存在字段", restored_fields)

    def test_update_notice_screenshots_append_to_type_specific_fields(self):
        cases = [
            (
                "维保通告",
                MaintenanceNoticeHandler("维保通告"),
                "【维保通告】状态：更新\n【名称】测试维保\n【时间】2026-06-12 09:30~2026-06-12 18:30\n【进度】更新中",
                MAINTENANCE_NOTICE_FIELDS["notice_images"],
            ),
            (
                "事件通告",
                EventNoticeHandler("事件通告"),
                "【事件通告】状态：更新\n【标题】测试事件\n【事件发生时间】2026-06-12 09:30\n【进展】更新中",
                "进展更新截图",
            ),
            (
                "变更通告",
                ChangeNoticeHandler("变更通告"),
                "【变更通告】状态：更新\n【名称】测试变更\n【等级】低风险\n【时间】2026-06-12 09:30~2026-06-12 18:30\n【进度】更新中",
                CHANGE_NOTICE_FIELDS["update_snapshot"],
            ),
            (
                "上电通告",
                PowerNoticeHandler("上电通告"),
                "【上电通告】状态：更新\n【名称】测试上电\n【时间】2026-06-12 09:30~2026-06-12 18:30\n【进度】更新中",
                POWER_NOTICE_FIELDS["notice_images"],
            ),
            (
                "设备轮巡",
                PollingNoticeHandler("设备轮巡"),
                "【设备轮巡】状态：更新\n【标题】测试轮巡\n【时间】2026-06-12 09:30~2026-06-12 18:30\n【进度】更新中",
                POLLING_NOTICE_FIELDS["notice_images"],
            ),
            (
                "设备调整",
                AdjustNoticeHandler("设备调整"),
                "【设备调整】状态：更新\n【名称】测试调整\n【时间】2026-06-12 09:30~2026-06-12 18:30\n【进度】更新中",
                ADJUST_NOTICE_FIELDS["notice_images"],
            ),
            (
                "设备检修",
                OverhaulNoticeHandler("设备检修"),
                "【设备检修】状态：更新\n【标题】测试检修\n【发现故障时间】2026-06-12 09:30\n【期望完成时间】2026-06-12 18:30\n【完成情况】更新中",
                "过程通告截图",
            ),
        ]

        for notice_type, handler, text, field_name in cases:
            with self.subTest(notice_type=notice_type):
                fields = handler.build_update_fields(
                    NoticePayload(
                        text=text,
                        existing_file_tokens=["old-token"],
                        file_tokens=["new-token"],
                        response_time="2026-06-12 10:00",
                    )
                )

                self.assertEqual(
                    fields[field_name],
                    [{"file_token": "old-token"}, {"file_token": "new-token"}],
                )

    def test_change_handler_writes_planned_end_before_end_state(self):
        handler = ChangeNoticeHandler("变更通告")
        payload = NoticePayload(
            text=(
                "【变更通告】状态：开始\n"
                "【名称】测试变更\n"
                "【等级】低风险\n"
                "【时间】2026-06-12 09:30~2026-06-12 18:30\n"
                "【进度】准备开始"
            ),
            response_time="2026-06-12 09:35",
        )
        expected_plan_end = int(dt.datetime(2026, 6, 12, 18, 30).timestamp() * 1000)

        create_fields = handler.build_create_fields(payload)
        update_fields = handler.build_update_fields(
            NoticePayload(
                text=payload.text.replace("状态：开始", "状态：更新").replace("准备开始", "执行中"),
                response_time="2026-06-12 10:00",
            )
        )

        self.assertEqual(
            create_fields[CHANGE_NOTICE_FIELDS["end_time"]],
            expected_plan_end,
        )
        self.assertEqual(
            update_fields[CHANGE_NOTICE_FIELDS["end_time"]],
            expected_plan_end,
        )

    def test_change_handler_end_state_overwrites_end_time_with_response_time(self):
        handler = ChangeNoticeHandler("变更通告")
        payload = NoticePayload(
            text=(
                "【变更通告】状态：结束\n"
                "【名称】测试变更\n"
                "【等级】低风险\n"
                "【时间】2026-06-12 09:30~2026-06-12 18:30\n"
                "【进度】执行完成"
            ),
            response_time="2026-06-12 18:35",
        )
        expected_actual_end = int(dt.datetime(2026, 6, 12, 18, 35).timestamp() * 1000)

        fields = handler.build_update_fields(payload)

        self.assertEqual(fields[CHANGE_NOTICE_FIELDS["end_time"]], expected_actual_end)

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

    def test_portal_service_retries_bitable_data_not_ready(self):
        service = MaintenancePortalService()
        calls = []

        class _HttpClient:
            def request_json(self, method, url, **kwargs):
                calls.append((method, url, kwargs))
                if len(calls) < 3:
                    return {
                        "code": 1254607,
                        "msg": "Data not ready, please try again later",
                    }
                return {"code": 0, "data": {"items": []}}

        service._http_client = _HttpClient()
        service._auth_headers = lambda: {"Authorization": "Bearer test"}

        with patch("lan_bitable_template_portal.portal_service.time.sleep") as sleep_mock:
            payload = service._request_json("fields", params={"page_size": 500})

        self.assertEqual(payload["code"], 0)
        self.assertEqual(len(calls), 3)
        self.assertEqual([call.args[0] for call in sleep_mock.call_args_list], [1.0, 2.5])

    def test_portal_service_data_not_ready_error_is_user_friendly(self):
        service = MaintenancePortalService()

        class _HttpClient:
            def request_json(self, method, url, **kwargs):
                return {
                    "code": 1254607,
                    "msg": "Data not ready, please try again later",
                }

        service._http_client = _HttpClient()
        service._auth_headers = lambda: {"Authorization": "Bearer test"}

        with patch("lan_bitable_template_portal.portal_service.time.sleep"):
            with self.assertRaises(PortalError) as ctx:
                service._request_json("records", params={"page_size": 500})

        self.assertIn("飞书多维表正在计算", str(ctx.exception))
        self.assertIn("已保留上次成功数据", service._source_sync_warning("检修源表", ctx.exception))

    def test_repair_source_record_page_size_is_reduced(self):
        service = MaintenancePortalService()
        captured_params = []
        service._repair_field_meta_by_name = {}

        def fake_request(path, *, params=None, **kwargs):
            captured_params.append(dict(params or {}))
            return {"data": {"items": [], "has_more": False}}

        service._request_json = fake_request

        service._load_repair_records()

        self.assertEqual(captured_params[0]["page_size"], 200)
        self.assertNotIn("view_id", captured_params[0])

    def test_lan_portal_state_store_replaces_ongoing_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            item = {
                "active_item_id": "active-1",
                "record_id": "target-1",
                "source_record_id": "source-1",
                "work_type": "change",
                "notice_type": "变更通告",
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

    def test_repair_management_operation_ledger_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")

            created = store.begin_repair_management_operation(
                "repair-project-op-1",
                operation_type="project_create",
                scope="E",
                payload_hash="hash-1",
            )
            self.assertTrue(created["created"])
            self.assertEqual(created["status"], "started")

            self.assertTrue(
                store.update_repair_management_operation(
                    "repair-project-op-1",
                    status="completed",
                    record_id="rec-project-1",
                    result={"record_id": "rec-project-1"},
                )
            )
            replay = store.begin_repair_management_operation(
                "repair-project-op-1",
                operation_type="project_create",
                scope="E",
                payload_hash="hash-1",
            )

            self.assertFalse(replay["created"])
            self.assertEqual(replay["status"], "completed")
            self.assertEqual(replay["record_id"], "rec-project-1")
            self.assertEqual(replay["result"]["record_id"], "rec-project-1")
            with self.assertRaisesRegex(ValueError, "payload"):
                store.begin_repair_management_operation(
                    "repair-project-op-1",
                    operation_type="project_create",
                    scope="E",
                    payload_hash="hash-2",
                )

            failed = store.begin_repair_management_operation(
                "repair-project-op-2",
                operation_type="project_create",
                scope="E",
                payload_hash="failed-hash-1",
            )
            self.assertTrue(failed["created"])
            store.update_repair_management_operation(
                "repair-project-op-2",
                status="failed",
                error="mock write failure",
            )
            restarted = store.begin_repair_management_operation(
                "repair-project-op-2",
                operation_type="project_create",
                scope="E",
                payload_hash="failed-hash-2",
            )
            self.assertTrue(restarted["created"])
            self.assertEqual(restarted["status"], "started")
            self.assertEqual(restarted["payload_hash"], "failed-hash-2")
            self.assertTrue(store.schema_health()["ok"])

    def test_lan_portal_state_store_dedupes_ongoing_snapshot_by_notice_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")

            result = store.replace_ongoing_items(
                [
                    {
                        "active_item_id": "active-1",
                        "source_record_id": "source-1",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "title": "A楼维保",
                        "building": "A楼",
                        "start_time": "2026-06-15 09:30",
                        "end_time": "2026-06-15 18:30",
                        "progress": "准备中",
                    },
                    {
                        "source_record_id": "source-1",
                        "target_record_id": "target-1",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "title": "A楼维保",
                        "building": "A楼",
                        "start_time": "2026-06-15 09:30",
                        "end_time": "2026-06-15 18:30",
                        "location": "A楼",
                    },
                ]
            )
            snapshot = store.get_ongoing_snapshot()

            self.assertEqual(result["count"], 1)
            self.assertEqual(snapshot["count"], 1)
            self.assertEqual(snapshot["items"][0]["active_item_id"], "active-1")
            self.assertEqual(snapshot["items"][0]["target_record_id"], "target-1")

    def test_lan_portal_state_store_dedupes_qt_active_item_after_target_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            store.upsert_qt_active_item(
                {
                    "active_item_id": "active-1",
                    "source_record_id": "source-1",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "title": "A楼维保",
                    "building": "A楼",
                    "text": "【维保通告】状态：开始\n【名称】A楼维保",
                },
                section="other",
                origin="portal",
            )
            store.upsert_qt_active_item(
                {
                    "source_record_id": "source-1",
                    "target_record_id": "target-1",
                    "record_id": "target-1",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "title": "A楼维保",
                    "building": "A楼",
                    "text": "【维保通告】状态：开始\n【名称】A楼维保",
                },
                section="other",
                origin="portal",
            )

            items = store.list_qt_active_items()

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["active_item_id"], "active-1")
            self.assertEqual(items[0]["record_id"], "target-1")
            self.assertEqual(items[0]["payload"]["target_record_id"], "target-1")

    def test_lan_portal_state_store_preserves_bound_target_on_later_local_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            store.upsert_qt_active_item(
                {
                    "active_item_id": "active-keep-target",
                    "target_record_id": "target-keep",
                    "record_id": "target-keep",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "title": "A楼维保",
                    "text": "【维保通告】状态：开始\n【名称】A楼维保",
                },
                section="other",
                origin="qt_upload",
            )

            store.upsert_qt_active_item(
                {
                    "active_item_id": "active-keep-target",
                    "record_id": "local_active_keep_target",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "title": "A楼维保",
                    "text": "【维保通告】状态：更新\n【名称】A楼维保",
                    "_is_placeholder_record": True,
                },
                section="other",
                origin="clipboard",
            )

            items = store.list_qt_active_items()
            identity = store.resolve_notice_identity(
                work_type="maintenance",
                active_item_id="active-keep-target",
            )

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["record_id"], "target-keep")
            self.assertEqual(items[0]["payload"]["record_id"], "target-keep")
            self.assertEqual(items[0]["payload"]["target_record_id"], "target-keep")
            self.assertFalse(items[0]["payload"]["_is_placeholder_record"])
            self.assertIsNotNone(identity)
            self.assertEqual(identity["target_record_id"], "target-keep")

    def test_lan_portal_state_store_deletes_unbound_legacy_identity_rows(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = Path(tmp) / "lan_portal_state.sqlite3"
            store = LanPortalStateStore(db_path)
            store.runtime_health_report()
            now = time.time()
            with closing(sqlite3.connect(str(db_path))) as conn:
                conn.execute(
                    """
                    INSERT INTO notice_identity_map(
                        identity_id, work_type, notice_type, active_item_id,
                        source_record_id, target_record_id, title,
                        building_codes_json, payload_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "maintenance:active:legacy-local-only",
                        "maintenance",
                        "维保通告",
                        "legacy-local-only",
                        "",
                        "local_legacy_placeholder",
                        "旧本地占位",
                        "[]",
                        json.dumps(
                            {
                                "active_item_id": "legacy-local-only",
                                "record_id": "local_legacy_placeholder",
                                "target_record_id": "local_legacy_placeholder",
                                "work_type": "maintenance",
                            },
                            ensure_ascii=False,
                        ),
                        now,
                        now,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO notice_identity_map(
                        identity_id, work_type, notice_type, active_item_id,
                        source_record_id, target_record_id, title,
                        building_codes_json, payload_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "maintenance:source:source-keep-only",
                        "maintenance",
                        "维保通告",
                        "source-active",
                        "source-keep-only",
                        "",
                        "源表待发起",
                        "[]",
                        json.dumps(
                            {
                                "active_item_id": "source-active",
                                "source_record_id": "source-keep-only",
                                "work_type": "maintenance",
                            },
                            ensure_ascii=False,
                        ),
                        now,
                        now,
                    ),
                )
                conn.execute(
                    "DELETE FROM meta WHERE key = 'notice_identity_unbound_cleanup_v1_done'"
                )
                conn.commit()

            reloaded = LanPortalStateStore(db_path)
            legacy = reloaded.resolve_notice_identity(
                work_type="maintenance",
                active_item_id="legacy-local-only",
            )
            source_only = reloaded.resolve_notice_identity(
                work_type="maintenance",
                source_record_id="source-keep-only",
            )

            self.assertIsNone(legacy)
            self.assertIsNotNone(source_only)
            self.assertEqual(source_only["source_record_id"], "source-keep-only")
            del reloaded
            del store
            gc.collect()

    def test_lan_portal_state_store_keeps_different_reason_active_items_with_different_targets(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            base = {
                "work_type": "maintenance",
                "notice_type": "维保通告",
                "title": "A楼测试维保通告",
                "building": "A楼",
                "start_time": "2026-06-15 09:30",
                "end_time": "2026-06-15 18:30",
                "reason": "测试原因",
            }
            store.upsert_qt_active_item(
                {
                    **base,
                    "active_item_id": "active-1",
                    "target_record_id": "target-1",
                    "record_id": "target-1",
                    "reason": "测试原因一",
                },
                section="other",
                origin="portal",
            )
            store.upsert_qt_active_item(
                {
                    **base,
                    "active_item_id": "active-2",
                    "target_record_id": "target-2",
                    "record_id": "target-2",
                    "reason": "测试原因二",
                },
                section="other",
                origin="portal",
            )

            items = store.list_qt_active_items()

            self.assertEqual(len(items), 2)
            self.assertEqual(
                {item["record_id"] for item in items},
                {"target-1", "target-2"},
            )

    def test_lan_portal_state_store_keeps_exact_duplicate_active_items(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            base = {
                "work_type": "maintenance",
                "notice_type": "维保通告",
                "title": "EA118机房C楼交直流列头柜及PDU维护",
                "building": "C楼",
                "start_time": "2026-06-18 09:30",
                "end_time": "2026-06-18 18:30",
                "location": "C楼",
                "content": "按计划对C栋直流列头柜及PDU季度维护",
                "reason": "按计划对C栋直流列头柜及PDU季度维护，保证供电正常",
                "impact": "对IT设备无影响，不会产生动环报警",
                "progress": "准备工作已完成，人员已就位，是否可以操作？",
            }
            store.upsert_qt_active_item(
                {
                    **base,
                    "active_item_id": "active-1",
                    "target_record_id": "target-1",
                    "record_id": "target-1",
                },
                section="other",
                origin="portal",
            )
            store.upsert_qt_active_item(
                {
                    **base,
                    "active_item_id": "active-2",
                    "target_record_id": "target-2",
                    "record_id": "target-2",
                },
                section="other",
                origin="portal",
            )

            items = store.list_qt_active_items()

            self.assertEqual(len(items), 2)
            self.assertEqual(
                {item["active_item_id"] for item in items},
                {"active-1", "active-2"},
            )

    def test_lan_portal_state_store_keeps_progress_only_active_item_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            base = {
                "work_type": "maintenance",
                "notice_type": "维保通告",
                "title": "EA118机房C楼交直流列头柜及PDU维护",
                "building": "C楼",
                "start_time": "2026-06-18 09:30",
                "end_time": "2026-06-18 18:30",
                "location": "C楼",
                "content": "按计划对C栋直流列头柜及PDU季度维护",
                "reason": "按计划对C栋直流列头柜及PDU季度维护，保证供电正常",
                "impact": "对IT设备无影响，不会产生动环报警",
            }
            store.upsert_qt_active_item(
                {
                    **base,
                    "active_item_id": "active-1",
                    "target_record_id": "target-1",
                    "record_id": "target-1",
                    "progress": "准备工作已完成，人员已就位，是否可以操作？",
                },
                section="other",
                origin="portal",
            )
            store.upsert_qt_active_item(
                {
                    **base,
                    "active_item_id": "active-2",
                    "target_record_id": "target-2",
                    "record_id": "target-2",
                    "progress": "工作已开始，正在检查设备状态",
                },
                section="other",
                origin="portal",
            )

            self.assertEqual(len(store.list_qt_active_items()), 2)

    def test_c_building_seven_similar_maintenance_notices_remain_visible(self):
        notices = [
            ("EA118机房C楼火灾报警系统维护", "厂家对C栋消防月度维护"),
            ("EA118机房C楼交直流列头柜及PDU维护", "按计划对C栋直流列头柜及PDU季度维护，保证供电正常"),
            ("EA118机房C楼消火栓系统维护", "厂家对C栋消防月度维护"),
            ("EA118机房C楼消防排烟系统维护", "厂家对C栋消防半年度维护"),
            ("EA118机房C楼消防排烟系统维护", "厂家对C栋消防月度维护"),
            ("EA118机房C楼自动喷淋灭火系统维护", "按计划对C楼自动喷淋灭火系统进行月度维护"),
            ("EA118机房C楼冷站Y型过滤器检查", "按维保计划工程师对C楼冷站Y型过滤器进行检查、清洗，确保制冷单元运行正常"),
        ]
        items = [
            {
                "work_type": "maintenance",
                "notice_type": "维保通告",
                "active_item_id": f"active-{index}",
                "target_record_id": f"target-{index}",
                "record_id": f"target-{index}",
                "title": title,
                "building": "C楼",
                "start_time": "2026-06-18 09:30",
                "end_time": "2026-06-18 18:30",
                "reason": reason,
                "progress": "准备工作已完成，人员已就位，是否可以操作？",
            }
            for index, (title, reason) in enumerate(notices, start=1)
        ]

        service = MaintenancePortalService()
        merged = service._merge_ongoing_items("C", items)

        self.assertEqual(len(merged), 7)
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            for item in items:
                store.upsert_qt_active_item(item, section="other", origin="portal")

            stored = store.list_qt_active_items()

        self.assertEqual(len(stored), 7)
        self.assertEqual(
            {item["record_id"] for item in stored},
            {f"target-{index}" for index in range(1, 8)},
        )

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

    def test_lan_portal_state_store_notice_upload_attachments(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            saved = store.put_notice_upload_attachment(
                open_id="ou_test",
                file_name="现场.png",
                mime_type="image/png",
                content=b"fake-image-bytes",
                ttl_seconds=120,
            )

            loaded = store.get_notice_upload_attachment(saved["upload_id"])
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded["open_id"], "ou_test")
            self.assertEqual(loaded["file_name"], "现场.png")
            self.assertEqual(loaded["mime_type"], "image/png")
            self.assertEqual(loaded["content"], b"fake-image-bytes")
            self.assertTrue(store.mark_notice_upload_attachment_used(saved["upload_id"]))
            self.assertEqual(store.cleanup_notice_upload_attachments(now=time.time() + 7200), 1)
            self.assertIsNone(store.get_notice_upload_attachment(saved["upload_id"]))

    def test_lan_portal_state_store_cleans_clipboard_and_dialog_sessions(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            store.upsert_clipboard_candidate(
                "cand-old",
                content="旧剪贴板",
                payload={"title": "旧剪贴板"},
                status="done",
            )
            store.upsert_clipboard_candidate(
                "cand-pending",
                content="新剪贴板",
                payload={"title": "新剪贴板"},
                status="pending",
            )
            store.upsert_dialog_session(
                "dlg-old",
                session_type="screenshot",
                payload={"title": "旧弹窗"},
                status="completed",
            )
            store.upsert_dialog_session(
                "dlg-pending",
                session_type="screenshot",
                payload={"title": "新弹窗"},
                status="pending",
            )

            old_ts = time.time() - 10 * 24 * 3600
            with store._lock:
                conn = sqlite3.connect(store.db_path)
                try:
                    conn.execute(
                        "UPDATE clipboard_candidates SET updated_at = ? WHERE candidate_id = ?",
                        (old_ts, "cand-old"),
                    )
                    conn.execute(
                        "UPDATE dialog_sessions SET updated_at = ? WHERE session_id = ?",
                        (old_ts, "dlg-old"),
                    )
                    conn.commit()
                finally:
                    conn.close()

            clipboard_cleanup = store.cleanup_clipboard_candidates()
            dialog_cleanup = store.cleanup_dialog_sessions()

            self.assertEqual(clipboard_cleanup["removed_total"], 1)
            self.assertEqual(dialog_cleanup["removed_total"], 1)
            self.assertEqual(
                [item["candidate_id"] for item in store.list_clipboard_candidates(status="", limit=10)],
                ["cand-pending"],
            )
            self.assertEqual(
                [item["session_id"] for item in store.list_dialog_sessions(status="", limit=10)],
                ["dlg-pending"],
            )
            store.shutdown_write_worker(timeout=1.0)

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

    def test_lan_portal_state_store_shutdown_drains_large_write_backlog(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            total = 250
            for index in range(total):
                self.assertTrue(
                    store.append_event_async(
                        "shutdown_drain",
                        {"index": index},
                    )
                )

            store.shutdown_write_worker(timeout=5.0)
            events = store.list_events_after("shutdown_drain", 0, limit=1000)
            stats = store.get_write_worker_stats()

            self.assertEqual(len(events), total)
            self.assertEqual(events[-1]["payload"]["index"], total - 1)
            self.assertFalse(stats["alive"])
            self.assertEqual(stats["queue_size"], 0)

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
            queued_items = store.list_runtime_queue_items("message", statuses=("queued",))
            self.assertEqual([item["job_id"] for item in queued_items], ["job-future"])
            self.assertFalse(store.lease_runtime_queue_item("message", "job-future"))

            self.assertTrue(store.upsert_runtime_queue_item("message", "job-specific"))
            self.assertTrue(store.lease_runtime_queue_item("message", "job-specific"))
            counts_after_specific = store.runtime_queue_counts()
            self.assertEqual(counts_after_specific["message"]["processing"], 2)

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

    def test_upload_wait_tracking_uses_sqlite_queue_only(self):
        class _FakeService:
            def get_job(self, job_id):
                return {"phase": "uploading"} if job_id == "job-uploading" else {}

        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            old_store = PortalRuntime.state_store
            old_service = PortalRuntime.service
            old_thread = PortalRuntime.upload_wait_thread
            old_timeout = PortalRuntime.action_upload_timeout_s
            try:
                PortalRuntime.state_store = store
                PortalRuntime.service = _FakeService()
                PortalRuntime.upload_wait_thread = type(
                    "_AliveWorker", (), {"is_alive": lambda self: True}
                )()
                PortalRuntime.action_upload_timeout_s = 60

                PortalRuntime.track_upload_wait_job("job-uploading")

                self.assertFalse(hasattr(PortalRuntime, "upload_wait_jobs"))
                counts = store.runtime_queue_counts()
                self.assertEqual(counts["upload_wait"]["queued"], 1)
                queued = store.list_runtime_queue_items("upload_wait", statuses=("queued",))
                self.assertGreater(float(queued[0]["available_at"]), time.time())
                self.assertTrue(PortalRuntime.upload_wait_event.is_set())
            finally:
                PortalRuntime.state_store = old_store
                PortalRuntime.service = old_service
                PortalRuntime.upload_wait_thread = old_thread
                PortalRuntime.action_upload_timeout_s = old_timeout

    def test_upload_wait_scan_clears_completed_and_times_out_uploading_jobs(self):
        class _FakeService:
            def __init__(self):
                self.jobs = {
                    "job-success": {"phase": "success"},
                    "job-timeout": {"phase": "uploading"},
                    "job-pending": {"phase": "uploading"},
                }
                self.marked = {}

            def get_job(self, job_id):
                return dict(self.jobs.get(job_id) or {})

            def mark_job(self, job_id, **patch):
                self.marked[job_id] = dict(patch)
                self.jobs.setdefault(job_id, {}).update(patch)

        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            now = time.time()
            store.upsert_runtime_queue_item(
                "upload_wait",
                "job-success",
                payload={"deadline_at": now - 10},
                available_at=now - 10,
            )
            store.upsert_runtime_queue_item(
                "upload_wait",
                "job-timeout",
                payload={"deadline_at": now - 10},
                available_at=now - 10,
            )
            store.upsert_runtime_queue_item(
                "upload_wait",
                "job-pending",
                payload={"deadline_at": now + 60},
                available_at=now + 60,
            )
            old_store = PortalRuntime.state_store
            old_service = PortalRuntime.service
            try:
                fake_service = _FakeService()
                PortalRuntime.state_store = store
                PortalRuntime.service = fake_service

                pending = PortalRuntime._scan_upload_wait_queue_once()

                self.assertEqual(pending, 1)
                counts = store.runtime_queue_counts()
                self.assertEqual(counts["upload_wait"]["done"], 1)
                self.assertEqual(counts["upload_wait"]["failed"], 1)
                self.assertEqual(counts["upload_wait"]["queued"], 1)
                self.assertEqual(fake_service.marked["job-timeout"]["phase"], "failed")
                self.assertIn("上传多维超时", fake_service.marked["job-timeout"]["error"])
            finally:
                PortalRuntime.state_store = old_store
                PortalRuntime.service = old_service

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
                "active_item_id": "active-placeholder-1",
                "record_id": "local-placeholder-1",
                "notice_type": "维保通告",
                "text": "【维保通告】状态：开始\n【名称】EA118机房A楼B楼测试维保\n【时间】2026-05-24 09:30~2026-05-24 18:30",
                "buildings": ["A楼", "B楼"],
                "specialty": "消防",
                "time_str": "2026-05-24 09:30~2026-05-24 18:30",
                "maintenance_cycle": "/",
                "_is_placeholder_record": True,
                "_has_unuploaded_changes": True,
                "_upload_in_progress": True,
                "_last_upload_error": "旧错误",
            },
            "response_time": "2026-05-24 09:30",
            "recover_selected": False,
            "robot_group_choice": "auto",
        }
        old_store = PortalRuntime.state_store
        stored_payload = {}
        uploaded_fields = {}

        def fake_create(notice_type, payload):
            uploaded_fields.update(
                MaintenanceNoticeHandler(notice_type).build_create_fields(payload)
            )
            return True, "rec-backend-1"

        with tempfile.TemporaryDirectory() as tmp:
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                with patch.object(
                    portal_server_module,
                    "create_bitable_record_by_payload",
                    side_effect=fake_create,
                ):
                    result = PortalRuntime.execute_local_notice_upload(request_payload)
                items = PortalRuntime.state_store.list_qt_active_items()
                stored_payload = dict((items[0] or {}).get("payload") or {}) if items else {}
            finally:
                PortalRuntime.state_store = old_store
        self.assertTrue(result["ok"])
        self.assertEqual(result["name"], "上传")
        self.assertEqual(result["record_id"], "local-placeholder-1")
        self.assertEqual(result["real_record_id"], "rec-backend-1")
        self.assertEqual(stored_payload["record_id"], "rec-backend-1")
        self.assertEqual(stored_payload["target_record_id"], "rec-backend-1")
        self.assertFalse(stored_payload["_is_placeholder_record"])
        self.assertFalse(stored_payload["_has_unuploaded_changes"])
        self.assertFalse(stored_payload["_upload_in_progress"])
        self.assertEqual(stored_payload["_last_upload_error"], "")
        self.assertEqual(stored_payload["binding_status"], "bound")
        self.assertEqual(stored_payload["buildings"], ["A楼", "B楼"])
        self.assertEqual(stored_payload["specialty"], "消防")
        self.assertEqual(
            uploaded_fields[MAINTENANCE_NOTICE_FIELDS["building"]],
            ["A楼", "B楼"],
        )
        self.assertEqual(
            uploaded_fields[MAINTENANCE_NOTICE_FIELDS["specialty"]],
            "消防",
        )

    def test_qt_upload_field_resolver_preserves_saved_fields_when_dialog_empty(self):
        harness = _UploadFieldResolverHarness({})
        data = {
            "record_id": "rec-saved-fields",
            "active_item_id": "active-saved-fields",
            "buildings": ["A楼"],
            "specialty": "消防",
            "maintenance_cycle": "月度",
        }
        resolved = harness._resolve_upload_fields_from_cache(
            data,
            {"buildings": [], "specialty": "", "maintenance_cycle": ""},
        )

        self.assertEqual(resolved["buildings"], ["A楼"])
        self.assertEqual(resolved["specialty"], "消防")
        self.assertEqual(resolved["maintenance_cycle"], "月度")
        self.assertEqual(data["specialty"], "消防")
        self.assertEqual(data["maintenance_cycle"], "月度")
        self.assertTrue(
            any(
                patch.get("specialty") == "消防"
                and patch.get("maintenance_cycle") == "月度"
                for patch in harness.cache_store.patches
            )
        )

    def test_qt_upload_field_resolver_preserves_event_source_when_cache_is_empty(self):
        harness = _UploadFieldResolverHarness({"event_source": ""})
        data = {
            "record_id": "rec-event-source",
            "active_item_id": "active-event-source",
            "notice_type": "事件通告",
            "event_source": "BMS动环系统告警",
            "source": "BMS动环系统告警",
        }

        resolved = harness._resolve_upload_fields_from_cache(
            data,
            {"event_source": ""},
        )

        self.assertEqual(resolved["event_source"], "BMS动环系统告警")
        self.assertEqual(data["event_source"], "BMS动环系统告警")
        self.assertEqual(data["source"], "BMS动环系统告警")

    def test_qt_upload_field_resolver_preserves_transfer_to_overhaul(self):
        harness = _UploadFieldResolverHarness({"transfer_to_overhaul": True})
        data = {
            "record_id": "rec-event-transfer-cache",
            "active_item_id": "active-event-transfer-cache",
            "notice_type": "事件通告",
        }

        resolved = harness._resolve_upload_fields_from_cache(data, {})

        self.assertTrue(resolved["transfer_to_overhaul"])
        self.assertTrue(data["transfer_to_overhaul"])

    def test_qt_upload_field_resolver_allows_explicit_transfer_cancel(self):
        harness = _UploadFieldResolverHarness({"transfer_to_overhaul": True})
        data = {
            "record_id": "rec-event-transfer-cancel",
            "active_item_id": "active-event-transfer-cancel",
            "notice_type": "事件通告",
            "transfer_to_overhaul": True,
        }

        resolved = harness._resolve_upload_fields_from_cache(
            data,
            {"transfer_to_overhaul": False},
        )

        self.assertFalse(resolved["transfer_to_overhaul"])
        self.assertFalse(data["transfer_to_overhaul"])
        self.assertFalse(harness.cache_store.fields["transfer_to_overhaul"])

    def test_qt_upload_field_resolver_only_clears_saved_fields_when_explicit(self):
        harness = _UploadFieldResolverHarness(
            {"specialty": "消防", "maintenance_cycle": "月度"}
        )
        data = {
            "record_id": "rec-clear-fields",
            "active_item_id": "active-clear-fields",
            "buildings": ["A楼"],
            "specialty": "消防",
            "maintenance_cycle": "月度",
            "_upload_specialty_cleared": True,
            "_upload_maintenance_cycle_cleared": True,
        }
        resolved = harness._resolve_upload_fields_from_cache(
            data,
            {"buildings": [], "specialty": "", "maintenance_cycle": ""},
        )

        self.assertEqual(resolved["specialty"], "")
        self.assertEqual(resolved["maintenance_cycle"], "")
        self.assertNotIn("specialty", data)
        self.assertNotIn("maintenance_cycle", data)
        self.assertTrue(data["_upload_specialty_cleared"])
        self.assertTrue(data["_upload_maintenance_cycle_cleared"])
        self.assertTrue(
            any(
                patch.get("specialty") is None
                and patch.get("maintenance_cycle") is None
                for patch in harness.cache_store.patches
            )
        )

    def test_local_qt_notice_upload_create_without_record_id_is_failure(self):
        request_payload = {
            "action_type": "upload",
            "data_dict": {
                "active_item_id": "active-placeholder-empty-result",
                "record_id": "placeholder-empty-result",
                "notice_type": "维保通告",
                "text": "【维保通告】状态：开始\n【名称】测试测试测试",
                "buildings": ["A楼"],
                "specialty": "测试",
                "maintenance_cycle": "/",
            },
            "response_time": "2026-05-24 09:30",
            "recover_selected": False,
            "robot_group_choice": "auto",
        }
        old_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                with patch.object(
                    portal_server_module,
                    "create_bitable_record_by_payload",
                    return_value=(True, ""),
                ):
                    result = PortalRuntime.execute_local_notice_upload(request_payload)
            finally:
                PortalRuntime.state_store = old_store
        self.assertFalse(result["ok"])
        self.assertIn("未返回 record_id", result["message"])
        self.assertEqual(result["real_record_id"], "")

    def test_local_qt_notice_upload_uses_attachment_upload_ids(self):
        old_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            screenshot = PortalRuntime.state_store.put_notice_upload_attachment(
                open_id="qt-local",
                file_name="screenshot.png",
                mime_type="image/png",
                content=b"screenshot-bytes",
            )
            site_photo = PortalRuntime.state_store.put_notice_upload_attachment(
                open_id="qt-local",
                file_name="site.png",
                mime_type="image/png",
                content=b"site-photo-bytes",
            )
            request_payload = {
                "action_type": "upload",
                "data_dict": {
                    "active_item_id": "active-upload-id",
                    "record_id": "placeholder-upload-id",
                    "notice_type": "维保通告",
                    "text": "【维保通告】状态：开始\n【名称】测试测试测试",
                    "buildings": ["A楼"],
                    "specialty": "测试",
                    "maintenance_cycle": "/",
                },
                "screenshot_upload_id": screenshot["upload_id"],
                "extra_images": [
                    {"upload_id": site_photo["upload_id"], "file_name": "site.png"}
                ],
                "response_time": "2026-05-24 09:30",
                "recover_selected": False,
                "robot_group_choice": "auto",
            }
            media_calls = []
            created_payloads = []

            def _fake_upload(image_bytes, file_name=""):
                media_calls.append((bytes(image_bytes), file_name))
                return True, f"file-token-{len(media_calls)}"

            def _fake_create(notice_type, payload):
                created_payloads.append(payload)
                return True, "rec-upload-id"

            try:
                with patch.object(
                    portal_server_module,
                    "upload_media_to_feishu",
                    _fake_upload,
                ), patch.object(
                    portal_server_module,
                    "create_bitable_record_by_payload",
                    _fake_create,
                ):
                    result = PortalRuntime.execute_local_notice_upload(request_payload)
            finally:
                PortalRuntime.state_store = old_store

        self.assertTrue(result["ok"])
        self.assertEqual(result["real_record_id"], "rec-upload-id")
        self.assertEqual(
            media_calls,
            [
                (b"screenshot-bytes", "screenshot.png"),
                (b"site-photo-bytes", "site.png"),
            ],
        )
        self.assertEqual(created_payloads[0].file_tokens, ["file-token-1"])
        self.assertEqual(created_payloads[0].extra_file_tokens, ["file-token-2"])

    def test_local_event_upload_is_idempotent_for_same_active_item(self):
        request_payload = {
            "action_type": "upload",
            "data_dict": {
                "active_item_id": "active-event-1",
                "record_id": "local_event_1",
                "_is_placeholder_record": True,
                "notice_type": "事件通告",
                "text": (
                    "【事件通告】状态：开始\n"
                    "【标题】测试测试测试事件\n"
                    "【时间】2026-05-24 09:30~2026-05-24 18:30\n"
                    "【概述】测试测试测试"
                ),
                "time_str": "2026-05-24 09:30~2026-05-24 18:30",
                "level": "I3",
            },
            "response_time": "2026-05-24 09:30",
            "recover_selected": False,
            "robot_group_choice": "auto",
        }
        old_store = PortalRuntime.state_store
        old_locks = PortalRuntime.local_upload_locks
        old_created_targets = PortalRuntime.local_upload_created_targets
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            PortalRuntime.local_upload_locks = {}
            PortalRuntime.local_upload_created_targets = {}
            try:
                with patch.object(
                    portal_server_module,
                    "create_bitable_record_by_payload",
                    return_value=(True, "rec-event-1"),
                ) as create_record, patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": {"标题": "测试测试测试事件"}}),
                ) as query_record:
                    first = PortalRuntime.execute_local_notice_upload(request_payload)
                    second = PortalRuntime.execute_local_notice_upload(request_payload)
            finally:
                store.shutdown_write_worker(timeout=2.0)
                PortalRuntime.state_store = old_store
                PortalRuntime.local_upload_locks = old_locks
                PortalRuntime.local_upload_created_targets = old_created_targets

        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertEqual(first["real_record_id"], "rec-event-1")
        self.assertEqual(second["real_record_id"], "rec-event-1")
        self.assertTrue(second.get("deduped"))
        create_record.assert_called_once()
        query_record.assert_not_called()

    def test_local_event_upload_dedupes_different_local_ids_by_notice_text(self):
        def payload(local_id: str) -> dict:
            return {
                "action_type": "upload",
                "data_dict": {
                    "active_item_id": local_id,
                    "record_id": local_id,
                    "_is_placeholder_record": True,
                    "notice_type": "事件通告",
                    "text": (
                        "【事件通告】状态：开始\n"
                        "【标题】测试测试测试事件\n"
                        "【时间】2026-05-24 09:30~2026-05-24 18:30\n"
                        "【概述】测试测试测试"
                    ),
                    "time_str": "2026-05-24 09:30~2026-05-24 18:30",
                    "level": "I3",
                },
                "response_time": "2026-05-24 09:30",
                "recover_selected": False,
                "robot_group_choice": "auto",
            }

        old_store = PortalRuntime.state_store
        old_locks = PortalRuntime.local_upload_locks
        old_created_targets = PortalRuntime.local_upload_created_targets
        with tempfile.TemporaryDirectory() as tmp:
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            PortalRuntime.local_upload_locks = {}
            PortalRuntime.local_upload_created_targets = {}
            try:
                with patch.object(
                    portal_server_module,
                    "create_bitable_record_by_payload",
                    return_value=(True, "rec-event-1"),
                ) as create_record, patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": {"标题": "测试测试测试事件"}}),
                ) as query_record:
                    first = PortalRuntime.execute_local_notice_upload(payload("localid-event-1"))
                    second = PortalRuntime.execute_local_notice_upload(payload("localid-event-2"))
            finally:
                PortalRuntime.state_store = old_store
                PortalRuntime.local_upload_locks = old_locks
                PortalRuntime.local_upload_created_targets = old_created_targets

        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertEqual(first["real_record_id"], "rec-event-1")
        self.assertEqual(second["real_record_id"], "rec-event-1")
        self.assertTrue(second.get("deduped"))
        create_record.assert_called_once()
        query_record.assert_not_called()

    def test_local_event_update_and_end_reuse_created_target_across_new_local_ids(self):
        def event_text(status: str, progress: str = "测试测试测试") -> str:
            return (
                f"【事件通告】状态：{status}\n"
                "【标题】测试测试测试事件\n"
                "【事件发生时间】2026-05-24 09:30\n"
                "【机楼】A楼\n"
                "【来源】BMS\n"
                "【等级】I3\n"
                "【概述】测试测试测试\n"
                f"【进展】{progress}"
            )

        def payload(action_type: str, local_id: str, status: str) -> dict:
            return {
                "action_type": action_type,
                "data_dict": {
                    "active_item_id": local_id,
                    "record_id": local_id,
                    "_is_placeholder_record": True,
                    "notice_type": "事件通告",
                    "text": event_text(status, f"{status}进展"),
                    "time_str": "2026-05-24 09:30",
                    "building": "A楼",
                    "event_source": "BMS",
                    "level": "I3",
                },
                "response_time": "2026-05-24 09:30",
                "recover_selected": False,
                "robot_group_choice": "auto",
            }

        old_store = PortalRuntime.state_store
        old_locks = PortalRuntime.local_upload_locks
        old_created_targets = PortalRuntime.local_upload_created_targets
        update_calls = []
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            PortalRuntime.local_upload_locks = {}
            PortalRuntime.local_upload_created_targets = {}
            screenshot = store.put_notice_upload_attachment(
                open_id="qt-local",
                file_name="event-update.png",
                mime_type="image/png",
                content=b"event-update-bytes",
            )
            end_screenshot = store.put_notice_upload_attachment(
                open_id="qt-local",
                file_name="event-end.png",
                mime_type="image/png",
                content=b"event-end-bytes",
            )

            def fake_update(record_id, notice_type, notice_payload):
                update_calls.append((record_id, notice_type, notice_payload))
                return True, record_id

            try:
                with patch.object(
                    portal_server_module,
                    "create_bitable_record_by_payload",
                    return_value=(True, "rec-event-1"),
                ) as create_record, patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": {"标题": "测试测试测试事件"}}),
                ) as query_record, patch.object(
                    portal_server_module,
                    "update_bitable_record_by_payload",
                    fake_update,
                ), patch.object(
                    portal_server_module,
                    "upload_media_to_feishu",
                    return_value=(True, "event-update-token"),
                ):
                    started = PortalRuntime.execute_local_notice_upload(
                        payload("upload", "localid-event-start", "开始")
                    )
                    update_payload = payload("update", "localid-event-update", "更新")
                    update_payload["screenshot_upload_id"] = screenshot["upload_id"]
                    updated = PortalRuntime.execute_local_notice_upload(update_payload)
                    end_payload = payload("end", "localid-event-end", "结束")
                    end_payload["screenshot_upload_id"] = end_screenshot["upload_id"]
                    ended = PortalRuntime.execute_local_notice_upload(end_payload)
            finally:
                store.shutdown_write_worker(timeout=2.0)
                PortalRuntime.state_store = old_store
                PortalRuntime.local_upload_locks = old_locks
                PortalRuntime.local_upload_created_targets = old_created_targets

        self.assertTrue(started["ok"])
        self.assertTrue(updated["ok"])
        self.assertTrue(ended["ok"])
        self.assertEqual(started["real_record_id"], "rec-event-1")
        self.assertEqual(updated["real_record_id"], "rec-event-1")
        self.assertEqual(ended["real_record_id"], "rec-event-1")
        self.assertEqual([call[0] for call in update_calls], ["rec-event-1", "rec-event-1"])
        create_record.assert_called_once()
        self.assertGreaterEqual(query_record.call_count, 2)

    def test_local_event_end_requires_screenshot_before_remote_update(self):
        request_payload = {
            "action_type": "end",
            "data_dict": {
                "active_item_id": "event-active-end-no-shot",
                "record_id": "rec-event-end-no-shot",
                "target_record_id": "rec-event-end-no-shot",
                "_is_placeholder_record": False,
                "notice_type": "事件通告",
                "text": (
                    "【事件通告】状态：结束\n"
                    "【标题】测试测试测试事件\n"
                    "【事件发生时间】2026-05-24 09:30\n"
                    "【机楼】A楼\n"
                    "【来源】BMS\n"
                    "【等级】I3\n"
                    "【概述】测试测试测试\n"
                    "【进展】结束"
                ),
                "time_str": "2026-05-24 09:30",
                "building": "A楼",
                "event_source": "BMS",
                "level": "I3",
            },
            "response_time": "2026-05-24 09:30",
            "recover_selected": False,
            "robot_group_choice": "auto",
        }

        old_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                with patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": {"标题": "测试测试测试事件"}}),
                ), patch.object(
                    portal_server_module,
                    "update_bitable_record_by_payload",
                ) as update_record:
                    result = PortalRuntime.execute_local_notice_upload(request_payload)
            finally:
                PortalRuntime.state_store = old_store

        self.assertFalse(result["ok"])
        self.assertEqual(result["name"], "结束")
        self.assertIn("必须上传通告截图", result["message"])
        update_record.assert_not_called()

    def test_local_event_end_with_transfer_queues_repair_project_after_remote_write(self):
        previous_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            screenshot = store.put_notice_upload_attachment(
                open_id="qt-local",
                file_name="event-end.png",
                mime_type="image/png",
                content=b"event-end-bytes",
            )
            request_payload = {
                "action_type": "end",
                "data_dict": {
                    "active_item_id": "event-active-transfer",
                    "record_id": "rec-event-transfer",
                    "target_record_id": "rec-event-transfer",
                    "_is_placeholder_record": False,
                    "work_type": "event",
                    "notice_type": "事件通告",
                    "scope": "E",
                    "text": (
                        "【事件通告】状态：结束\n"
                        "【标题】E楼压缩机高压报警\n"
                        "【事件发生时间】2026-07-16 09:30\n"
                        "【机楼】E楼\n"
                        "【专业】暖通\n"
                        "【来源】BMS\n"
                        "【等级】I3\n"
                        "【进展】事件结束"
                    ),
                    "time_str": "2026-07-16 09:30",
                    "building": "E楼",
                    "specialty": "暖通",
                    "event_source": "BMS",
                    "level": "I3",
                    "transfer_to_overhaul": True,
                },
                "screenshot_upload_id": screenshot["upload_id"],
            }
            remote_fields = {
                "告警描述": "E楼压缩机高压报警",
                "机楼": "E楼",
                "专业": "暖通",
                "事件等级": "I3",
                "事件发现来源": "BMS",
                "事件发生时间": "2026-07-16 09:30",
                "是否转检修": True,
            }
            try:
                with patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": remote_fields}),
                ), patch.object(
                    portal_server_module,
                    "upload_media_to_feishu",
                    return_value=(True, "event-end-token"),
                ), patch.object(
                    portal_server_module,
                    "update_bitable_record_by_payload",
                    return_value=(True, "rec-event-transfer"),
                ), patch.object(
                    PortalRuntime,
                    "enqueue_event_repair_project",
                    return_value=17,
                ) as enqueue_project:
                    result = PortalRuntime.execute_local_notice_upload(request_payload)
            finally:
                store.shutdown_write_worker(timeout=2.0)
                PortalRuntime.state_store = previous_store

        self.assertTrue(result["ok"])
        self.assertTrue(result["repair_project_queued"])
        self.assertEqual(result["repair_project_queue_id"], 17)
        self.assertEqual(result["repair_project_status"], "queued")
        self.assertIn("进入后台创建", result["message"])
        enqueue_project.assert_called_once()
        self.assertEqual(
            enqueue_project.call_args.kwargs["event_record_id"],
            "rec-event-transfer",
        )
        self.assertEqual(
            enqueue_project.call_args.kwargs["remote_fields"],
            remote_fields,
        )

    def test_local_event_end_keeps_success_when_repair_project_queue_fails(self):
        previous_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            screenshot = store.put_notice_upload_attachment(
                open_id="qt-local",
                file_name="event-end-failed-project.png",
                mime_type="image/png",
                content=b"event-end-bytes",
            )
            request_payload = {
                "action_type": "end",
                "data_dict": {
                    "active_item_id": "event-active-transfer-warning",
                    "record_id": "rec-event-transfer-warning",
                    "target_record_id": "rec-event-transfer-warning",
                    "_is_placeholder_record": False,
                    "notice_type": "事件通告",
                    "scope": "E",
                    "text": (
                        "【事件通告】状态：结束\n"
                        "【标题】E楼测试事件\n"
                        "【事件发生时间】2026-07-16 09:30\n"
                        "【机楼】E楼\n"
                        "【专业】暖通\n"
                        "【来源】BMS\n"
                        "【等级】I3"
                    ),
                    "time_str": "2026-07-16 09:30",
                    "building": "E楼",
                    "specialty": "暖通",
                    "event_source": "BMS",
                    "level": "I3",
                    "transfer_to_overhaul": True,
                },
                "screenshot_upload_id": screenshot["upload_id"],
            }
            try:
                with patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": {"是否转检修": True}}),
                ), patch.object(
                    portal_server_module,
                    "upload_media_to_feishu",
                    return_value=(True, "event-end-token"),
                ), patch.object(
                    portal_server_module,
                    "update_bitable_record_by_payload",
                    return_value=(True, "rec-event-transfer-warning"),
                ), patch.object(
                    PortalRuntime,
                    "enqueue_event_repair_project",
                    side_effect=PortalError("维修单字段配置不完整"),
                ), patch.object(
                    PortalRuntime,
                    "_enqueue_active_delete_for_ended_notice",
                    return_value="1",
                ) as enqueue_active_delete:
                    result = PortalRuntime.execute_local_notice_upload(request_payload)
            finally:
                store.shutdown_write_worker(timeout=2.0)
                PortalRuntime.state_store = previous_store

        self.assertTrue(result["ok"])
        self.assertFalse(result["repair_project_queued"])
        self.assertEqual(result["repair_project_status"], "failed")
        self.assertIn("转检修任务创建失败", result["repair_project_warning"])
        self.assertIn("维修单字段配置不完整", result["message"])
        enqueue_active_delete.assert_called_once()

    def test_local_event_end_explicit_transfer_cancel_overrides_remote_true(self):
        previous_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            screenshot = store.put_notice_upload_attachment(
                open_id="qt-local",
                file_name="event-end-no-transfer.png",
                mime_type="image/png",
                content=b"event-end-bytes",
            )
            request_payload = {
                "action_type": "end",
                "data_dict": {
                    "active_item_id": "event-active-no-transfer",
                    "record_id": "rec-event-no-transfer",
                    "target_record_id": "rec-event-no-transfer",
                    "_is_placeholder_record": False,
                    "notice_type": "事件通告",
                    "scope": "E",
                    "text": (
                        "【事件通告】状态：结束\n"
                        "【标题】E楼测试事件\n"
                        "【事件发生时间】2026-07-16 09:30\n"
                        "【机楼】E楼\n"
                        "【来源】BMS\n"
                        "【等级】I3"
                    ),
                    "time_str": "2026-07-16 09:30",
                    "building": "E楼",
                    "event_source": "BMS",
                    "level": "I3",
                    "transfer_to_overhaul": False,
                },
                "screenshot_upload_id": screenshot["upload_id"],
            }
            try:
                with patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": {"是否转检修": True}}),
                ), patch.object(
                    portal_server_module,
                    "upload_media_to_feishu",
                    return_value=(True, "event-end-token"),
                ), patch.object(
                    portal_server_module,
                    "update_bitable_record_by_payload",
                    return_value=(True, "rec-event-no-transfer"),
                ), patch.object(
                    PortalRuntime,
                    "enqueue_event_repair_project",
                ) as enqueue_project:
                    result = PortalRuntime.execute_local_notice_upload(request_payload)
            finally:
                store.shutdown_write_worker(timeout=2.0)
                PortalRuntime.state_store = previous_store

        self.assertTrue(result["ok"])
        self.assertEqual(result["repair_project_status"], "not_requested")
        enqueue_project.assert_not_called()

    def test_event_repair_queue_worker_creates_project_and_marks_task_done(self):
        previous_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            event_id = store.enqueue_outbox_event(
                PortalRuntime.event_repair_queue_channel,
                {
                    "event_record_id": "rec-event-queued",
                    "notice_data": {"notice_type": "事件通告"},
                    "remote_fields": {"是否转检修": True},
                    "scope": "E",
                    "source_month": "2026-07",
                },
            )
            try:
                with patch.object(
                    PortalRuntime.service,
                    "ensure_repair_management_record_for_event_notice",
                    return_value={
                        "record_id": "rec-repair-queued",
                        "created": True,
                    },
                ) as ensure_project:
                    result = PortalRuntime._process_event_repair_queue_once()
                done = store.list_outbox_events(
                    PortalRuntime.event_repair_queue_channel,
                    status="done",
                )
            finally:
                store.shutdown_write_worker(timeout=2.0)
                PortalRuntime.state_store = previous_store

        self.assertTrue(result["processed"])
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["repair_record_id"], "rec-repair-queued")
        self.assertEqual([item["id"] for item in done], [event_id])
        ensure_project.assert_called_once_with(
            event_record_id="rec-event-queued",
            notice_data={"notice_type": "事件通告"},
            remote_fields={"是否转检修": True},
            scope="E",
            source_month="2026-07",
        )

    def test_event_repair_queue_survives_state_store_reopen(self):
        previous_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "lan_portal_state.sqlite3"
            first_store = LanPortalStateStore(db_path)
            event_id = first_store.enqueue_outbox_event(
                PortalRuntime.event_repair_queue_channel,
                {
                    "event_record_id": "rec-event-after-restart",
                    "notice_data": {"notice_type": "事件通告"},
                    "remote_fields": {"是否转检修": True},
                    "scope": "E",
                    "source_month": "2026-07",
                },
            )
            first_store.shutdown_write_worker(timeout=2.0)

            reopened_store = LanPortalStateStore(db_path)
            PortalRuntime.state_store = reopened_store
            try:
                with patch.object(
                    PortalRuntime.service,
                    "ensure_repair_management_record_for_event_notice",
                    return_value={
                        "record_id": "rec-repair-after-restart",
                        "created": True,
                    },
                ):
                    result = PortalRuntime._process_event_repair_queue_once()
                done = reopened_store.list_outbox_events(
                    PortalRuntime.event_repair_queue_channel,
                    status="done",
                )
            finally:
                reopened_store.shutdown_write_worker(timeout=2.0)
                PortalRuntime.state_store = previous_store

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["repair_record_id"], "rec-repair-after-restart")
        self.assertEqual([item["id"] for item in done], [event_id])

    def test_event_repair_queue_failure_is_isolated_and_marked_failed(self):
        previous_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            store.enqueue_outbox_event(
                PortalRuntime.event_repair_queue_channel,
                {
                    "event_record_id": "rec-event-repair-failed",
                    "notice_data": {},
                    "remote_fields": {},
                    "scope": "E",
                    "source_month": "2026-07",
                },
            )
            try:
                with patch.object(
                    PortalRuntime.service,
                    "ensure_repair_management_record_for_event_notice",
                    side_effect=PortalError("维修单字段配置不完整"),
                ):
                    result = PortalRuntime._process_event_repair_queue_once()
                failed = store.list_outbox_events(
                    PortalRuntime.event_repair_queue_channel,
                    status="failed",
                )
            finally:
                store.shutdown_write_worker(timeout=2.0)
                PortalRuntime.state_store = previous_store

        self.assertEqual(result["status"], "failed")
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["attempts"], 1)
        self.assertIn("维修单字段配置不完整", failed[0]["last_error"])

    def test_backend_source_start_reuses_existing_target_record(self):
        old_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            store.upsert_notice_identity(
                {
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "notice_type": "维保通告",
                    "source_record_id": "source-maint-1",
                    "target_record_id": "rec-maint-existing",
                    "title": "A楼测试维保",
                },
                origin="test",
            )
            prepared = {
                "job_id": "job-source-repeat",
                "action": "start",
                "work_type": WORK_TYPE_MAINTENANCE,
                "notice_type": "维保通告",
                "source_app_token": "source-app",
                "source_table_id": "source-table",
                "record_id": "source-maint-1",
                "source_record_id": "source-maint-1",
                "title": "A楼测试维保",
                "building": "A楼",
                "building_codes": ["A"],
                "start_time": "2026-05-24 09:30",
                "end_time": "2026-05-24 18:30",
                "text": "【维保通告】状态：开始\n【名称】A楼测试维保",
            }
            try:
                with patch(
                    "lan_bitable_template_portal.server.external_real_write_guard",
                    return_value={
                        "mock_external": False,
                        "real_write_allowed": True,
                        "reason": "",
                    },
                ), patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": {"名称": "A楼测试维保"}}),
                ) as query_record, patch.object(
                    portal_server_module,
                    "create_bitable_record_by_payload",
                    return_value=(True, "rec-maint-duplicate"),
                ) as create_record:
                    ok, message, record_id = PortalRuntime._execute_backend_prepared_upload(
                        prepared
                    )
            finally:
                PortalRuntime.state_store = old_store

        self.assertTrue(ok)
        self.assertEqual(message, "rec-maint-existing")
        self.assertEqual(record_id, "rec-maint-existing")
        query_record.assert_called_once_with("rec-maint-existing", "维保通告")
        create_record.assert_not_called()

    def test_backend_manual_start_reuses_existing_active_target_by_semantic_key(self):
        old_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            store.upsert_qt_active_item(
                {
                    "action": "start",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "notice_type": "维保通告",
                    "active_item_id": "rec-manual-existing",
                    "record_id": "rec-manual-existing",
                    "target_record_id": "rec-manual-existing",
                    "manual": True,
                    "title": "测试测试E楼维保通告",
                    "building": "E楼",
                    "building_codes": ["E"],
                    "specialty": "暖通",
                    "maintenance_cycle": "非计划性",
                    "start_time": "2026-06-25 14:00",
                    "end_time": "2026-06-25 15:00",
                    "location": "E楼测试位置",
                    "content": "测试测试维保内容",
                    "reason": "测试测试原因",
                    "impact": "测试测试无影响",
                    "progress": "原开始进度",
                },
                section="other",
                origin="portal",
            )
            prepared = {
                "job_id": "job-manual-repeat",
                "action": "start",
                "work_type": WORK_TYPE_MAINTENANCE,
                "notice_type": "维保通告",
                "record_id": "manual:lite",
                "manual_id": "manual:lite",
                "manual": True,
                "title": "EA118机房E楼测试测试E楼维保通告",
                "building": "E楼",
                "building_codes": ["E"],
                "specialty": "暖通",
                "maintenance_cycle": "非计划性",
                "start_time": "2026-06-25T14:00",
                "end_time": "2026-06-25T15:00",
                "location": "E楼测试位置",
                "content": "测试测试维保内容",
                "reason": "测试测试原因",
                "impact": "测试测试无影响",
                "progress": "更新后的进度不参与开始去重",
                "text": "【维保通告】状态：开始\n【名称】EA118机房E楼测试测试E楼维保通告",
            }
            try:
                with patch(
                    "lan_bitable_template_portal.server.external_real_write_guard",
                    return_value={
                        "mock_external": False,
                        "real_write_allowed": True,
                        "reason": "",
                    },
                ), patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": {"名称": "测试测试E楼维保通告"}}),
                ) as query_record, patch.object(
                    portal_server_module,
                    "create_bitable_record_by_payload",
                    return_value=(True, "rec-manual-duplicate"),
                ) as create_record:
                    ok, message, record_id = PortalRuntime._execute_backend_prepared_upload(
                        prepared
                    )
            finally:
                PortalRuntime.state_store = old_store

        self.assertTrue(ok)
        self.assertEqual(message, "rec-manual-existing")
        self.assertEqual(record_id, "rec-manual-existing")
        query_record.assert_called_once_with("rec-manual-existing", "维保通告")
        create_record.assert_not_called()

    def test_backend_active_projection_preserves_existing_site_photo_count(self):
        old_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            try:
                store.upsert_qt_active_item(
                    {
                        "action": "start",
                        "work_type": WORK_TYPE_MAINTENANCE,
                        "notice_type": "维保通告",
                        "active_item_id": "rec-site-existing",
                        "record_id": "rec-site-existing",
                        "target_record_id": "rec-site-existing",
                        "title": "E楼测试维保",
                        "extra_images": [
                            {"upload_id": "site-photo-1", "file_name": "site.png"}
                        ],
                    },
                    section="other",
                    origin="portal",
                )
                projected = PortalRuntime._prepared_to_qt_ui_payload(
                    {
                        "action": "update",
                        "work_type": WORK_TYPE_MAINTENANCE,
                        "notice_type": "维保通告",
                        "active_item_id": "rec-site-existing",
                        "record_id": "rec-site-existing",
                        "target_record_id": "rec-site-existing",
                        "title": "E楼测试维保",
                    },
                    remote_record_id="rec-site-existing",
                )
            finally:
                PortalRuntime.state_store = old_store

        self.assertEqual(projected.get("site_photo_count"), 1)
        self.assertEqual(projected.get("extra_images")[0]["upload_id"], "site-photo-1")

    def test_backend_end_allows_existing_site_photo_without_new_upload(self):
        prepared = {
            "action": "end",
            "work_type": WORK_TYPE_MAINTENANCE,
            "notice_type": "维保通告",
            "record_id": "rec-maint-end",
            "target_record_id": "rec-maint-end",
            "active_item_id": "rec-maint-end",
            "title": "E楼测试维保",
            "text": "【维保通告】状态：结束\n【名称】E楼测试维保",
        }
        with patch.object(
            PortalRuntime.service,
            "_has_existing_site_photo_for_notice",
            return_value=True,
        ), patch(
            "lan_bitable_template_portal.server.external_real_write_guard",
            return_value={"mock_external": False, "real_write_allowed": True, "reason": ""},
        ), patch.object(
            portal_server_module,
            "query_record_by_id",
            return_value=(True, {"fields": {}}),
        ), patch.object(
            portal_server_module,
            "update_bitable_record_by_payload",
            return_value=(True, "rec-maint-end"),
        ):
            ok, message, record_id = PortalRuntime._execute_backend_prepared_upload(prepared)

        self.assertTrue(ok)
        self.assertEqual(message, "rec-maint-end")
        self.assertEqual(record_id, "rec-maint-end")

    def test_local_qt_end_allows_existing_site_photo_without_new_upload(self):
        old_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            request_payload = {
                "action_type": "end",
                "data_dict": {
                    "record_id": "rec-maint-qt-end",
                    "target_record_id": "rec-maint-qt-end",
                    "active_item_id": "rec-maint-qt-end",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "notice_type": "维保通告",
                    "title": "E楼测试维保",
                    "text": "【维保通告】状态：结束\n【名称】E楼测试维保",
                },
                "response_time": "2026-06-25 09:30",
                "robot_group_choice": "auto",
            }
            try:
                with patch.object(
                    PortalRuntime.service,
                    "_has_existing_site_photo_for_notice",
                    return_value=True,
                ), patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": {}}),
                ), patch.object(
                    portal_server_module,
                    "update_bitable_record_by_payload",
                    return_value=(True, "rec-maint-qt-end"),
                ):
                    result = PortalRuntime.execute_local_notice_upload(request_payload)
            finally:
                PortalRuntime.state_store = old_store

        self.assertTrue(result["ok"])
        self.assertEqual(result["name"], "结束")
        self.assertEqual(result["real_record_id"], "rec-maint-qt-end")

    def test_local_qt_notice_upload_with_target_record_updates_instead_of_creating(self):
        old_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            screenshot = PortalRuntime.state_store.put_notice_upload_attachment(
                open_id="qt-local",
                file_name="event-update.png",
                mime_type="image/png",
                content=b"event-update-bytes",
            )
            request_payload = {
                "action_type": "upload",
                "data_dict": {
                    "record_id": "rec-existing-event",
                    "target_record_id": "rec-existing-event",
                    "_is_placeholder_record": False,
                    "notice_type": "事件通告",
                    "text": (
                        "【事件通告】状态：更新\n"
                        "【标题】测试测试测试事件\n"
                        "【时间】2026-05-24 09:30~2026-05-24 18:30\n"
                        "【进展】测试测试测试"
                    ),
                    "time_str": "2026-05-24 09:30~2026-05-24 18:30",
                    "level": "I3",
                },
                "screenshot_upload_id": screenshot["upload_id"],
                "response_time": "2026-05-24 09:30",
                "recover_selected": False,
                "robot_group_choice": "auto",
            }
            try:
                with patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": {}}),
                ) as query_record, patch.object(
                    portal_server_module,
                    "upload_media_to_feishu",
                    return_value=(True, "event-update-token"),
                ), patch.object(
                    portal_server_module,
                    "create_bitable_record_by_payload",
                    return_value=(True, "rec-duplicate"),
                ) as create_record, patch.object(
                    portal_server_module,
                    "update_bitable_record_by_payload",
                    return_value=(True, "rec-existing-event"),
                ) as update_record:
                    result = PortalRuntime.execute_local_notice_upload(request_payload)
            finally:
                PortalRuntime.state_store = old_store

        self.assertTrue(result["ok"])
        self.assertEqual(result["name"], "更新")
        self.assertEqual(result["record_id"], "rec-existing-event")
        self.assertEqual(result["real_record_id"], "rec-existing-event")
        query_record.assert_called_once_with("rec-existing-event", "事件通告")
        update_record.assert_called_once()
        self.assertEqual(update_record.call_args.args[0], "rec-existing-event")
        self.assertEqual(update_record.call_args.args[2].file_tokens, ["event-update-token"])
        create_record.assert_not_called()

    def test_local_qt_notice_update_syncs_active_item_projection(self):
        old_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            store.upsert_qt_active_item(
                {
                    "active_item_id": "active-change-b",
                    "record_id": "rec-change-b",
                    "target_record_id": "rec-change-b",
                    "notice_type": "变更通告",
                    "work_type": "change",
                    "title": "B楼灯塔变更",
                    "text": "【变更通告】状态：开始\n【名称】B楼灯塔变更\n【进度】开始",
                    "_is_placeholder_record": False,
                    "_has_unuploaded_changes": False,
                },
                section="other",
                origin="clipboard",
            )
            screenshot = store.put_notice_upload_attachment(
                open_id="qt-local",
                file_name="change-update.png",
                mime_type="image/png",
                content=b"change-update-bytes",
            )
            update_text = (
                "【变更通告】状态：更新\n"
                "【名称】B楼灯塔变更\n"
                "【等级】I3\n"
                "【时间】2026-06-24 10:00~2026-06-24 18:00\n"
                "【位置】B楼\n"
                "【内容】测试测试\n"
                "【原因】测试测试\n"
                "【影响】无影响\n"
                "【进度】更新完成"
            )
            request_payload = {
                "action_type": "update",
                "data_dict": {
                    "active_item_id": "active-change-b",
                    "record_id": "rec-change-b",
                    "target_record_id": "rec-change-b",
                    "_is_placeholder_record": False,
                    "notice_type": "变更通告",
                    "work_type": "change",
                    "title": "B楼灯塔变更",
                    "text": update_text,
                    "level": "I3",
                },
                "screenshot_upload_id": screenshot["upload_id"],
                "response_time": "2026-06-24 10:30",
                "recover_selected": False,
                "robot_group_choice": "auto",
            }
            try:
                with patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": {}}),
                ), patch.object(
                    portal_server_module,
                    "upload_media_to_feishu",
                    return_value=(True, "change-update-token"),
                ), patch.object(
                    portal_server_module,
                    "update_bitable_record_by_payload",
                    return_value=(True, "rec-change-b"),
                ):
                    result = PortalRuntime.execute_local_notice_upload(request_payload)
                    items = store.list_qt_active_items()
                    outbox = store.list_outbox_events("qt_action")
            finally:
                PortalRuntime.state_store = old_store

        self.assertTrue(result["ok"])
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["record_id"], "rec-change-b")
        self.assertIn("状态：更新", items[0]["payload"]["text"])
        self.assertIn("更新完成", items[0]["payload"]["text"])
        self.assertFalse(items[0]["payload"].get("_has_unuploaded_changes"))
        self.assertTrue(
            any(
                event["payload"].get("kind") == "active_upsert"
                and event["payload"].get("payload", {}).get("item", {}).get("active_item_id")
                == "active-change-b"
                for event in outbox
            )
        )

    def test_local_qt_notice_update_syncs_active_item_projection_for_all_notice_types(self):
        notice_types = [
            ("维保通告", "maintenance", "other"),
            ("变更通告", "change", "other"),
            ("设备检修", "repair", "other"),
            ("上电通告", "power", "other"),
            ("设备轮巡", "polling", "other"),
            ("设备调整", "adjust", "other"),
            ("事件通告", "event", "event"),
        ]
        old_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            try:
                for index, (notice_type, work_type, section) in enumerate(notice_types, start=1):
                    active_item_id = f"active-all-update-{index}"
                    record_id = f"rec-all-update-{index}"
                    store.upsert_qt_active_item(
                        {
                            "active_item_id": active_item_id,
                            "record_id": record_id,
                            "target_record_id": record_id,
                            "notice_type": notice_type,
                            "work_type": work_type,
                            "title": f"{notice_type}测试",
                            "text": f"【{notice_type}】状态：开始\n【名称】{notice_type}测试\n【进度】开始",
                            "_is_placeholder_record": False,
                            "_has_unuploaded_changes": False,
                        },
                        section=section,
                        origin="clipboard",
                    )
                    screenshot = store.put_notice_upload_attachment(
                        open_id="qt-local",
                        file_name=f"notice-update-{index}.png",
                        mime_type="image/png",
                        content=b"notice-update-bytes",
                    )
                    request_payload = {
                        "action_type": "update",
                        "data_dict": {
                            "active_item_id": active_item_id,
                            "record_id": record_id,
                            "target_record_id": record_id,
                            "_is_placeholder_record": False,
                            "notice_type": notice_type,
                            "work_type": work_type,
                            "title": f"{notice_type}测试",
                            "text": f"【{notice_type}】状态：更新\n【名称】{notice_type}测试\n【进度】更新完成",
                        },
                        "screenshot_upload_id": screenshot["upload_id"],
                        "response_time": "2026-06-24 10:30",
                        "recover_selected": False,
                        "robot_group_choice": "auto",
                    }
                    with patch.object(
                        portal_server_module,
                        "query_record_by_id",
                        return_value=(True, {"fields": {}}),
                    ), patch.object(
                        portal_server_module,
                        "upload_media_to_feishu",
                        return_value=(True, f"notice-update-token-{index}"),
                    ), patch.object(
                        portal_server_module,
                        "update_bitable_record_by_payload",
                        return_value=(True, record_id),
                    ):
                        result = PortalRuntime.execute_local_notice_upload(request_payload)
                    self.assertTrue(result["ok"], notice_type)

                items = store.list_qt_active_items()
                outbox = store.list_outbox_events("qt_action", limit=100)
            finally:
                PortalRuntime.state_store = old_store

        self.assertEqual(len(items), len(notice_types))
        payloads_by_notice = {
            str(item["payload"].get("notice_type") or ""): item["payload"]
            for item in items
        }
        for notice_type, _work_type, _section in notice_types:
            with self.subTest(notice_type=notice_type):
                payload = payloads_by_notice[notice_type]
                self.assertIn("状态：更新", payload.get("text") or "")
                self.assertIn("更新完成", payload.get("text") or "")
                self.assertFalse(payload.get("_has_unuploaded_changes"))
        upsert_notice_types = {
            str(
                event["payload"].get("payload", {}).get("item", {}).get("notice_type")
                or ""
            )
            for event in outbox
            if event["payload"].get("kind") == "active_upsert"
        }
        for notice_type, _work_type, _section in notice_types:
            self.assertIn(notice_type, upsert_notice_types)

    def test_local_qt_notice_update_accepts_record_id_as_target_when_verified(self):
        old_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            screenshot = PortalRuntime.state_store.put_notice_upload_attachment(
                open_id="qt-local",
                file_name="change-update.png",
                mime_type="image/png",
                content=b"change-update-bytes",
            )
            request_payload = {
                "action_type": "update",
                "data_dict": {
                    "record_id": "rec-existing-change-no-target-field",
                    "_is_placeholder_record": False,
                    "notice_type": "变更通告",
                    "text": "【变更通告】状态：更新\n【名称】测试测试测试变更",
                    "level": "I3",
                },
                "screenshot_upload_id": screenshot["upload_id"],
                "response_time": "2026-05-24 09:30",
                "recover_selected": False,
                "robot_group_choice": "auto",
            }
            try:
                with patch.object(
                    portal_server_module,
                    "query_record_by_id",
                    return_value=(True, {"fields": {}}),
                ) as query_record, patch.object(
                    portal_server_module,
                    "upload_media_to_feishu",
                    return_value=(True, "change-update-token"),
                ), patch.object(
                    portal_server_module,
                    "update_bitable_record_by_payload",
                    return_value=(True, "rec-existing-change-no-target-field"),
                ) as update_record:
                    result = PortalRuntime.execute_local_notice_upload(request_payload)
            finally:
                PortalRuntime.state_store = old_store

        self.assertTrue(result["ok"])
        self.assertEqual(result["record_id"], "rec-existing-change-no-target-field")
        self.assertEqual(result["real_record_id"], "rec-existing-change-no-target-field")
        query_record.assert_called_once_with(
            "rec-existing-change-no-target-field",
            "变更通告",
        )
        update_record.assert_called_once()
        self.assertEqual(
            update_record.call_args.args[0],
            "rec-existing-change-no-target-field",
        )

    def test_local_qt_notice_update_requires_notice_screenshot(self):
        request_payload = {
            "action_type": "update",
            "data_dict": {
                "record_id": "rec-existing-change",
                "target_record_id": "rec-existing-change",
                "_is_placeholder_record": False,
                "notice_type": "变更通告",
                "text": "【变更通告】状态：更新\n【名称】测试测试测试变更",
                "level": "I3",
            },
            "response_time": "2026-05-24 09:30",
            "recover_selected": False,
            "robot_group_choice": "auto",
        }
        with patch.object(
            portal_server_module,
            "query_record_by_id",
            return_value=(True, {"fields": {}}),
        ), patch.object(
            portal_server_module,
            "update_bitable_record_by_payload",
            return_value=(True, "rec-existing-change"),
        ) as update_record:
            result = PortalRuntime.execute_local_notice_upload(request_payload)

        self.assertFalse(result["ok"])
        self.assertEqual(result["name"], "更新")
        self.assertIn("必须上传通告截图", result["message"])
        update_record.assert_not_called()

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

    def test_workflow_delegate_qt_notice_upload_uses_attachment_refs(self):
        class _Controller:
            def __init__(self):
                self.uploads = []
                self.payload = None

            def upload_notice_attachment(self, content, *, file_name="", mime_type="image/png"):
                self.uploads.append((bytes(content), file_name, mime_type))
                return {
                    "upload_id": f"upload-{len(self.uploads)}",
                    "file_name": file_name,
                    "mime_type": mime_type,
                    "size": len(content),
                }

            def execute_qt_notice_upload(self, payload):
                self.payload = dict(payload)
                return {
                    "ok": True,
                    "name": "上传",
                    "message": "ignored",
                    "record_id": "placeholder-3",
                    "real_record_id": "rec-backend-3",
                }

        controller = _Controller()
        harness = _WorkflowBackendDelegateHarness(controller)
        handled = harness._delegate_qt_notice_upload_to_backend(
            data_snapshot={
                "record_id": "placeholder-3",
                "notice_type": "维保通告",
                "text": "测试测试测试",
            },
            screenshot_bytes=b"screenshot",
            extra_images=[(b"site-photo", "site.png")],
            action_type="upload",
            response_time="2026-05-24 09:30",
            recover_selected=False,
            robot_group_choice="auto",
        )

        self.assertTrue(handled)
        self.assertEqual(
            controller.uploads,
            [
                (b"screenshot", "notice_screenshot.png", "image/png"),
                (b"site-photo", "site.png", "image/png"),
            ],
        )
        self.assertEqual(controller.payload["screenshot_upload_id"], "upload-1")
        self.assertNotIn("screenshot_bytes_b64", controller.payload)
        self.assertEqual(controller.payload["extra_images"][0]["upload_id"], "upload-2")
        self.assertNotIn("bytes_b64", controller.payload["extra_images"][0])

    def test_workflow_delegate_qt_notice_upload_fails_without_attachment_id(self):
        class _Controller:
            def upload_notice_attachment(self, content, *, file_name="", mime_type="image/png"):
                return {"file_name": file_name, "mime_type": mime_type, "size": len(content)}

            def execute_qt_notice_upload(self, payload):
                raise AssertionError("大图附件缺 upload_id 时不应继续提交通告")

        harness = _WorkflowBackendDelegateHarness(_Controller())
        handled = harness._delegate_qt_notice_upload_to_backend(
            data_snapshot={
                "record_id": "placeholder-4",
                "notice_type": "维保通告",
                "text": "测试测试测试",
            },
            screenshot_bytes=b"screenshot",
            extra_images=[],
            action_type="upload",
            response_time="2026-05-24 09:30",
            recover_selected=False,
            robot_group_choice="auto",
        )

        self.assertTrue(handled)
        self.assertEqual(len(harness.finished), 1)
        self.assertFalse(harness.finished[0][1])
        self.assertIn("截图暂存失败", harness.finished[0][2])

    def test_portal_handler_dequeue_falls_back_to_sqlite_queue(self):
        class _FakeService:
            def get_job(self, job_id):
                return {"phase": "accepted"} if job_id == "job-sqlite" else {}

            def mark_job(self, *args, **kwargs):
                return None

        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            old_store = PortalRuntime.state_store
            old_service = PortalRuntime.service
            old_event = PortalRuntime.message_queue_event
            try:
                PortalRuntime.state_store = store
                PortalRuntime.service = _FakeService()
                PortalRuntime.message_queue_event = threading.Event()
                store.upsert_runtime_queue_item("message", "job-sqlite")

                job_id = PortalRuntime._dequeue_runtime_job("message")

                self.assertEqual(job_id, "job-sqlite")
                counts = store.runtime_queue_counts()
                self.assertEqual(counts["message"]["processing"], 1)
            finally:
                PortalRuntime.state_store = old_store
                PortalRuntime.service = old_service
                PortalRuntime.message_queue_event = old_event

    def test_portal_handler_dequeue_skips_terminal_sqlite_jobs(self):
        class _FakeService:
            def get_job(self, job_id):
                if job_id == "job-done":
                    return {"phase": "success"}
                if job_id == "job-valid":
                    return {"phase": "accepted"}
                return {}

            def mark_job(self, *args, **kwargs):
                return None

        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            old_store = PortalRuntime.state_store
            old_service = PortalRuntime.service
            old_event = PortalRuntime.message_queue_event
            try:
                PortalRuntime.state_store = store
                PortalRuntime.service = _FakeService()
                PortalRuntime.message_queue_event = threading.Event()
                store.upsert_runtime_queue_item("message", "job-done")
                store.upsert_runtime_queue_item("message", "job-valid")

                job_id = PortalRuntime._dequeue_runtime_job("message")

                self.assertEqual(job_id, "job-valid")
                counts = store.runtime_queue_counts()
                self.assertEqual(counts["message"]["done"], 1)
                self.assertEqual(counts["message"]["processing"], 1)
            finally:
                PortalRuntime.state_store = old_store
                PortalRuntime.service = old_service
                PortalRuntime.message_queue_event = old_event

    def test_portal_message_enqueue_uses_sqlite_queue_only(self):
        class _FakeService:
            def __init__(self):
                self.marked = []

            def get_job(self, job_id):
                return {
                    "phase": "accepted",
                    "request": {"work_type": "maintenance", "scope": "A"},
                }

            def mark_job(self, job_id, **kwargs):
                self.marked.append((job_id, kwargs))

        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            old_store = PortalRuntime.state_store
            old_service = PortalRuntime.service
            old_event = PortalRuntime.message_queue_event
            old_workers = list(PortalRuntime.message_worker_threads)
            old_stop = PortalRuntime.message_worker_stop
            old_count = PortalRuntime.message_worker_count
            try:
                PortalRuntime.state_store = store
                PortalRuntime.service = _FakeService()
                PortalRuntime.message_queue_event = threading.Event()
                PortalRuntime.message_worker_threads = []
                PortalRuntime.message_worker_count = 0
                PortalRuntime.message_worker_stop = False

                PortalRuntime.enqueue_message_job("job-a")

                self.assertFalse(hasattr(PortalRuntime, "message_queue"))
                counts = store.runtime_queue_counts()
                self.assertEqual(counts["message"]["queued"], 1)
                self.assertTrue(PortalRuntime.message_queue_event.is_set())
            finally:
                PortalRuntime.state_store = old_store
                PortalRuntime.service = old_service
                PortalRuntime.message_queue_event = old_event
                PortalRuntime.message_worker_threads = old_workers
                PortalRuntime.message_worker_stop = old_stop
                PortalRuntime.message_worker_count = old_count

    def test_initial_action_enqueue_routes_directly_to_upload_queue(self):
        class _FakeService:
            def get_job(self, job_id):
                work_type = {"job-change": "change", "job-repair": "repair"}.get(job_id)
                if not work_type:
                    return {}
                return {
                    "phase": "accepted",
                    "request": {"work_type": work_type, "scope": "A"},
                }

            def mark_job(self, *args, **kwargs):
                return None

        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            old_store = PortalRuntime.state_store
            old_service = PortalRuntime.service
            old_event = PortalRuntime.action_queue_event
            old_ensure = PortalRuntime.ensure_action_worker
            try:
                PortalRuntime.state_store = store
                PortalRuntime.service = _FakeService()
                PortalRuntime.action_queue_event = threading.Event()
                PortalRuntime.ensure_action_worker = classmethod(lambda cls: None)

                PortalRuntime.enqueue_initial_message_or_upload_job("job-change")
                PortalRuntime.enqueue_initial_message_or_upload_job("job-repair")

                counts = store.runtime_queue_counts()
                self.assertEqual(counts.get("message", {}).get("queued", 0), 0)
                self.assertEqual(counts.get("qt_action", {}).get("queued", 0), 2)
                self.assertTrue(PortalRuntime.action_queue_event.is_set())
            finally:
                PortalRuntime.state_store = old_store
                PortalRuntime.service = old_service
                PortalRuntime.action_queue_event = old_event
                PortalRuntime.ensure_action_worker = old_ensure

    def test_legacy_message_batch_defers_personal_message_until_upload(self):
        class _FakeService:
            def __init__(self):
                self.marked = []
                self.sent = []

            def send_action_personal_message(self, prepared):
                self.sent.append(prepared)
                return False, "openid unavailable"

            def mark_job(self, job_id, **kwargs):
                self.marked.append((job_id, kwargs))

        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            service = _FakeService()
            old_store = PortalRuntime.state_store
            old_service = PortalRuntime.service
            old_ensure = PortalRuntime.ensure_action_worker
            old_event = PortalRuntime.action_queue_event
            try:
                PortalRuntime.state_store = store
                PortalRuntime.service = service
                PortalRuntime.action_queue_event = threading.Event()
                PortalRuntime.ensure_action_worker = classmethod(lambda cls: None)
                store.upsert_runtime_queue_item("message", "job-a")

                PortalRuntime._process_message_batch(
                    [
                        (
                            "job-a",
                            {
                                "text": "【维保通告】状态：开始\n【名称】测试",
                                "message_signature": "sig-a",
                            },
                        )
                    ]
                )

                counts = store.runtime_queue_counts()
                self.assertEqual(counts["message"]["done"], 1)
                self.assertEqual(counts["qt_action"]["queued"], 1)
                self.assertEqual(service.sent, [])
                patches = [patch for job_id, patch in service.marked if job_id == "job-a"]
                self.assertTrue(
                    any(
                        patch.get("phase") == "upload_queued"
                        and patch.get("message_sent") is False
                        for patch in patches
                    )
                )
            finally:
                PortalRuntime.state_store = old_store
                PortalRuntime.service = old_service
                PortalRuntime.ensure_action_worker = old_ensure
                PortalRuntime.action_queue_event = old_event

    def test_upload_success_after_message_failure_preserves_copyable_notice_text(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = _TestMaintenancePortalService()
            service._state_store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            service._jobs = {
                "job-a": {
                    "job_id": "job-a",
                    "phase": "uploading",
                    "created_at": "2026-06-17 10:00",
                    "updated_at": "2026-06-17 10:00",
                    "message_error": "openid unavailable",
                    "message_warning": "个人消息发送失败，已继续上传多维，可复制通告文本：openid unavailable",
                    "message_failed": True,
                    "message_failed_continue": True,
                    "prepared": {
                        "text": "【维保通告】状态：开始\n【名称】测试",
                    },
                }
            }

            service.mark_action_upload_result(
                "job-a",
                success=True,
                message="上传成功",
                record_id="rec-test",
            )

            job = service.get_job("job-a") or {}
            self.assertEqual(job.get("phase"), "success")
            self.assertIn("多维上传成功", job.get("upload_message") or "")
            self.assertTrue(job.get("message_failed"))
            self.assertEqual(job.get("record_id"), "rec-test")
            self.assertIn("【维保通告】", job.get("notice_text") or "")
            self.assertEqual(job.get("prepared"), {})

    def test_portal_message_batch_collects_same_scope_from_sqlite_queue(self):
        class _FakeService:
            def __init__(self):
                self.marked = []

            def get_job(self, job_id):
                scope = {
                    "job-primary": "A",
                    "job-a-2": "A",
                    "job-b-1": "B",
                }.get(job_id, "")
                return {
                    "phase": "accepted",
                    "request": {"work_type": "maintenance", "scope": scope},
                } if scope else {}

            def mark_job(self, job_id, **kwargs):
                self.marked.append((job_id, kwargs))

        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            old_store = PortalRuntime.state_store
            old_service = PortalRuntime.service
            old_wait = PortalRuntime.message_batch_wait_seconds
            old_max = PortalRuntime.message_batch_max_jobs
            try:
                PortalRuntime.state_store = store
                PortalRuntime.service = _FakeService()
                PortalRuntime.message_batch_wait_seconds = 0
                PortalRuntime.message_batch_max_jobs = 10
                store.upsert_runtime_queue_item("message", "job-a-2")
                store.upsert_runtime_queue_item("message", "job-b-1")

                batch = PortalRuntime._collect_message_batch_job_ids("job-primary")

                self.assertEqual(batch, ["job-primary", "job-a-2"])
                counts = store.runtime_queue_counts()
                self.assertEqual(counts["message"]["processing"], 1)
                self.assertEqual(counts["message"]["queued"], 1)
                queued = store.list_runtime_queue_items("message", statuses=("queued",))
                self.assertEqual([item["job_id"] for item in queued], ["job-b-1"])
            finally:
                PortalRuntime.state_store = old_store
                PortalRuntime.service = old_service
                PortalRuntime.message_batch_wait_seconds = old_wait
                PortalRuntime.message_batch_max_jobs = old_max

    def test_portal_qt_action_enqueue_uses_sqlite_queue_only(self):
        class _AliveWorker:
            def is_alive(self):
                return True

        class _FakeService:
            def __init__(self):
                self.marked = []

            def get_job(self, job_id):
                return {"phase": "message_sent", "request": {}} if job_id == "job-qt" else {}

            def mark_job(self, job_id, **kwargs):
                self.marked.append((job_id, kwargs))

        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            old_store = PortalRuntime.state_store
            old_service = PortalRuntime.service
            old_event = PortalRuntime.action_queue_event
            old_thread = PortalRuntime.action_worker_thread
            old_stop = PortalRuntime.action_worker_stop
            try:
                PortalRuntime.state_store = store
                PortalRuntime.service = _FakeService()
                PortalRuntime.action_queue_event = threading.Event()
                PortalRuntime.action_worker_thread = _AliveWorker()
                PortalRuntime.action_worker_stop = False

                PortalRuntime.enqueue_action_job("job-qt")

                self.assertFalse(hasattr(PortalRuntime, "action_queue"))
                counts = store.runtime_queue_counts()
                self.assertEqual(counts["qt_action"]["queued"], 1)
                self.assertTrue(PortalRuntime.action_queue_event.is_set())
            finally:
                PortalRuntime.state_store = old_store
                PortalRuntime.service = old_service
                PortalRuntime.action_queue_event = old_event
                PortalRuntime.action_worker_thread = old_thread
                PortalRuntime.action_worker_stop = old_stop

    def test_portal_qt_action_dequeue_uses_sqlite_queue(self):
        class _FakeService:
            def get_job(self, job_id):
                if job_id == "job-done":
                    return {"phase": "success"}
                if job_id == "job-qt":
                    return {"phase": "message_sent"}
                return {}

            def mark_job(self, *args, **kwargs):
                return None

        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            old_store = PortalRuntime.state_store
            old_service = PortalRuntime.service
            old_event = PortalRuntime.action_queue_event
            try:
                PortalRuntime.state_store = store
                PortalRuntime.service = _FakeService()
                PortalRuntime.action_queue_event = threading.Event()
                store.upsert_runtime_queue_item("qt_action", "job-done")
                store.upsert_runtime_queue_item("qt_action", "job-qt")

                job_id = PortalRuntime._dequeue_runtime_job("qt_action")

                self.assertEqual(job_id, "job-qt")
                counts = store.runtime_queue_counts()
                self.assertEqual(counts["qt_action"]["done"], 1)
                self.assertEqual(counts["qt_action"]["processing"], 1)
            finally:
                PortalRuntime.state_store = old_store
                PortalRuntime.service = old_service
                PortalRuntime.action_queue_event = old_event

    def test_lan_portal_state_store_checkpoints_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            store.put_backend_runtime("probe", {"ok": True})

            result = store.checkpoint_database()

            self.assertIn("before", result)
            self.assertIn("after", result)
            self.assertEqual(result["checkpoint"]["mode"], "TRUNCATE")
            self.assertGreaterEqual(result["reclaimed_bytes"], 0)

    def test_active_cache_store_ignores_stale_file_without_sqlite_items(self):
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

            payload = cache_store.load_payload()
            qt_items = state_store.list_qt_active_items()

            self.assertEqual(payload["other"], [])
            self.assertEqual(qt_items, [])

    def test_active_cache_store_uses_existing_qt_items_and_ignores_stale_file(self):
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

            payload = cache_store.load_payload()
            qt_items = state_store.list_qt_active_items()

            self.assertEqual(payload["other"][0]["data"]["record_id"], "current-record")
            self.assertEqual([item["active_item_id"] for item in qt_items], ["current-active"])

    def test_active_cache_store_does_not_merge_stale_file_items_when_sqlite_has_fewer(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_path = Path(tmp) / "active_cache.json"
            legacy_items = [
                {
                    "data": {
                        "active_item_id": f"legacy-active-{index}",
                        "record_id": f"legacy-record-{index}",
                        "target_record_id": f"legacy-record-{index}",
                        "notice_type": "维保通告",
                        "work_type": "maintenance",
                        "building_codes": ["C"],
                        "text": (
                            "【维保通告】状态：开始\n"
                            f"【名称】C楼旧缓存通告{index}\n"
                            "【时间】2026-06-18 09:30~2026-06-18 18:30\n"
                            f"【原因】旧缓存原因{index}"
                        ),
                    }
                }
                for index in range(1, 8)
            ]
            cache_path.write_text(
                json.dumps({"version": 2, "event": [], "other": legacy_items}, ensure_ascii=False),
                encoding="utf-8",
            )
            state_store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            for entry in legacy_items[:5]:
                state_store.upsert_qt_active_item(
                    entry["data"],
                    section="other",
                    origin="qt",
                )
            cache_store = ActiveCacheStore(str(cache_path), state_store)

            payload = cache_store.load_payload()
            qt_items = state_store.list_qt_active_items()

            self.assertEqual(len(payload["other"]), 5)
            self.assertEqual(len(qt_items), 5)
            self.assertEqual(
                {item["record_id"] for item in qt_items},
                {f"legacy-record-{index}" for index in range(1, 6)},
            )

            state_store.delete_qt_active_item(active_item_id="legacy-active-5")
            restarted_cache_store = ActiveCacheStore(str(cache_path), state_store)
            restarted_payload = restarted_cache_store.load_payload()
            qt_items_after_delete = state_store.list_qt_active_items()

            self.assertEqual(len(restarted_payload["other"]), 4)
            self.assertEqual(len(qt_items_after_delete), 4)
            self.assertNotIn(
                "legacy-record-5",
                {item["record_id"] for item in qt_items_after_delete},
            )

    def test_backend_clipboard_notice_projects_to_active_upsert_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                controller = FastAPIPortalController()
                entry = controller._clipboard_entry_from_content(
                    "【维保通告】状态：开始\n\n【标题】测试测试测试\n\n【时间】2026-05-25 09:30~2026-05-25 18:30\n\n【位置】A楼"
                )
                self.assertIsNotNone(entry)
                result = controller._project_clipboard_entry_to_active(entry)
                qt_items = PortalRuntime.state_store.list_qt_active_items()
                events = PortalRuntime.state_store.lease_outbox_events(
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
                PortalRuntime.state_store = previous_store

    def test_backend_event_projection_preserves_saved_event_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                controller = FastAPIPortalController()
                started = controller._clipboard_entry_from_content(
                    "【事件通告】状态：开始\n"
                    "【标题】A楼测试事件\n"
                    "【事件发生时间】2026-07-10 09:30\n"
                    "【机楼】A楼\n"
                    "【来源】BMS\n"
                    "【等级】I3\n"
                    "【进展】开始处理"
                )
                self.assertIsNotNone(started)
                start_result = controller._project_clipboard_entry_to_active(started)
                first_item = PortalRuntime.state_store.list_qt_active_items()[0]
                first_payload = dict(first_item["payload"])
                first_payload["source"] = "BMS动环系统告警"
                first_payload["event_source"] = "BMS动环系统告警"
                PortalRuntime.state_store.upsert_qt_active_item(
                    first_payload,
                    section="event",
                    origin="qt_upload",
                )

                updated = controller._clipboard_entry_from_content(
                    "【事件通告】状态：更新\n"
                    "【标题】A楼测试事件\n"
                    "【事件发生时间】2026-07-10 09:30\n"
                    "【机楼】A楼\n"
                    "【来源】BMS\n"
                    "【等级】I3\n"
                    "【进展】继续处理"
                )
                self.assertIsNotNone(updated)
                update_result = controller._project_clipboard_entry_to_active(updated)
                items = PortalRuntime.state_store.list_qt_active_items()

                self.assertTrue(start_result["ok"])
                self.assertTrue(update_result["ok"])
                self.assertEqual(len(items), 1)
                self.assertEqual(
                    items[0]["payload"]["event_source"],
                    "BMS动环系统告警",
                )
                self.assertEqual(
                    items[0]["payload"]["source"],
                    "BMS动环系统告警",
                )
            finally:
                PortalRuntime.state_store = previous_store

    def test_backend_manual_update_with_target_record_projects_to_bound_active_item(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                controller = FastAPIPortalController()
                entry = controller._clipboard_entry_from_content(
                    "【维保通告】状态：更新\n"
                    "【名称】EA118机房A楼灭火器维护\n"
                    "【时间】2026-06-24 09:00~2026-06-24 18:00\n"
                    "【位置】A楼\n"
                    "【进度】更新测试",
                    source="manual_add",
                )
                self.assertIsNotNone(entry)
                entry["target_record_id"] = "recvnexkknTrwz"

                result = controller._project_clipboard_entry_to_active(entry)
                qt_items = PortalRuntime.state_store.list_qt_active_items()

                self.assertTrue(result["ok"])
                self.assertFalse(result.get("ignored"))
                self.assertEqual(result["record_id"], "recvnexkknTrwz")
                self.assertEqual(len(qt_items), 1)
                payload = qt_items[0]["payload"]
                identity = PortalRuntime.state_store.resolve_notice_identity(
                    work_type="maintenance",
                    active_item_id=payload.get("active_item_id", ""),
                    source_record_id="",
                    target_record_id="recvnexkknTrwz",
                )
                self.assertEqual(qt_items[0]["record_id"], "recvnexkknTrwz")
                self.assertEqual(payload["record_id"], "recvnexkknTrwz")
                self.assertEqual(payload["target_record_id"], "recvnexkknTrwz")
                self.assertFalse(payload["_is_placeholder_record"])
                self.assertEqual(payload["work_type"], "maintenance")
                self.assertIsNotNone(identity)
                self.assertEqual(identity["target_record_id"], "recvnexkknTrwz")
            finally:
                PortalRuntime.state_store = previous_store

    def test_backend_clipboard_projection_clears_stale_upload_failure_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                controller = FastAPIPortalController()
                old_text = (
                    "【维保通告】状态：开始\n"
                    "【名称】EA118机房C楼消防排烟系统维护\n"
                    "【时间】2026-06-18 09:30~2026-06-18 18:30\n"
                    "【位置】C栋\n"
                    "【原因】厂家对C栋消防月度维护\n"
                    "【进度】旧进度"
                )
                PortalRuntime.state_store.upsert_qt_active_item(
                    {
                        "active_item_id": "active-stale-upload-error",
                        "record_id": "local_active_stale_upload_error",
                        "notice_type": "维保通告",
                        "work_type": "maintenance",
                        "title": "EA118机房C楼消防排烟系统维护",
                        "text": old_text,
                        "reason": "厂家对C栋消防月度维护",
                        "_has_unuploaded_changes": True,
                        "_upload_in_progress": False,
                        "_pending_upload_hash": None,
                        "_last_upload_error": "上一条上传失败，可重试。",
                        "_upload_started_monotonic": 123.0,
                    },
                    section="other",
                    origin="qt",
                )
                entry = controller._clipboard_entry_from_content(
                    "【维保通告】状态：开始\n"
                    "【名称】EA118机房C楼消防排烟系统维护\n"
                    "【时间】2026-06-18 09:30~2026-06-18 18:30\n"
                    "【位置】C栋\n"
                    "【原因】厂家对C栋消防月度维护\n"
                    "【进度】准备工作已完成"
                )

                result = controller._project_clipboard_entry_to_active(entry)
                qt_items = PortalRuntime.state_store.list_qt_active_items()
                payload = qt_items[0]["payload"]

                self.assertTrue(result["ok"])
                self.assertEqual(len(qt_items), 1)
                self.assertEqual(payload["active_item_id"], "active-stale-upload-error")
                self.assertIn("准备工作已完成", payload["text"])
                self.assertTrue(payload["_has_unuploaded_changes"])
                self.assertFalse(payload["_upload_in_progress"])
                self.assertIsNone(payload["_pending_upload_hash"])
                self.assertNotIn("_last_upload_error", payload)
                self.assertNotIn("_upload_started_monotonic", payload)
            finally:
                PortalRuntime.state_store = previous_store

    def test_backend_uploaded_notice_projects_to_active_upsert_event_for_all_notice_types(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            previous_service = PortalRuntime.service
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            PortalRuntime.service = _TestMaintenancePortalService()
            try:
                cases = [
                    ("maintenance", "维保通告", "【维保通告】状态：开始\n【名称】测试测试A楼维保通告"),
                    ("change", "变更通告", "【变更通告】状态：开始\n【名称】测试测试A楼变更通告"),
                    ("repair", "设备检修", "【设备检修】状态：开始\n【标题】测试测试A楼检修通告"),
                    ("power", "上电通告", "【上电通告】状态：开始\n【名称】测试测试A楼上电通告"),
                    ("polling", "设备轮巡", "【设备轮巡】状态：开始\n【标题】测试测试A楼轮巡通告"),
                    ("adjust", "设备调整", "【设备调整】状态：开始\n【名称】测试测试A楼调整通告"),
                ]
                event_ids = []
                for index, (work_type, notice_type, text) in enumerate(cases, start=1):
                    prepared = {
                        "job_id": f"job-active-upsert-{work_type}",
                        "work_type": work_type,
                        "notice_type": notice_type,
                        "record_id": f"src-{work_type}-1",
                        "source_record_id": f"src-{work_type}-1",
                        "title": f"测试测试A楼{notice_type}",
                        "building": "A楼",
                        "building_code": "A",
                        "building_codes": ["A"],
                        "status": "开始",
                        "text": text,
                    }

                    event_id = PortalRuntime._upsert_backend_active_notice(
                        prepared,
                        remote_record_id=f"rec-target-{index}",
                        job_id=f"job-active-upsert-{work_type}",
                    )
                    event_ids.append(event_id)

                qt_items = PortalRuntime.state_store.list_qt_active_items()
                events = PortalRuntime.state_store.lease_outbox_events(
                    "qt_action", limit=20, lease_seconds=5
                )

                self.assertEqual(len(event_ids), len(cases))
                self.assertTrue(all(event_ids))
                self.assertEqual(len(qt_items), len(cases))
                items_by_notice = {
                    str(item["payload"].get("notice_type") or ""): item
                    for item in qt_items
                }
                upsert_record_ids = {
                    str(
                        event["payload"].get("payload", {}).get("item", {}).get("record_id")
                        or ""
                    )
                    for event in events
                    if event["payload"].get("kind") == "active_upsert"
                }
                for index, (work_type, notice_type, _text) in enumerate(cases, start=1):
                    with self.subTest(notice_type=notice_type):
                        item = items_by_notice[notice_type]
                        self.assertEqual(item["record_id"], f"rec-target-{index}")
                        self.assertEqual(item["active_item_id"], f"rec-target-{index}")
                        self.assertEqual(item["payload"]["target_record_id"], f"rec-target-{index}")
                        self.assertEqual(item["payload"]["source_record_id"], f"src-{work_type}-1")
                        self.assertEqual(item["payload"]["work_type"], work_type)
                        self.assertIn(f"rec-target-{index}", upsert_record_ids)
            finally:
                PortalRuntime.state_store = previous_store
                PortalRuntime.service = previous_service

    def test_backend_clipboard_maintenance_same_title_different_reason_is_distinct(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                controller = FastAPIPortalController()
                first = controller._clipboard_entry_from_content(
                    "【维保通告】状态：开始\n\n"
                    "【名称】EA118机房C楼冷却塔清洗\n\n"
                    "【时间】2026-05-27 10:40~2026-05-27 18:00\n\n"
                    "【位置】C楼楼顶屋面\n\n"
                    "【原因】5#冷却塔脏堵\n\n"
                    "【进度】准备工作已完成"
                )
                second = controller._clipboard_entry_from_content(
                    "【维保通告】状态：开始\n\n"
                    "【名称】EA118机房C楼冷却塔清洗\n\n"
                    "【时间】2026-05-27 10:40~2026-05-27 18:00\n\n"
                    "【位置】C楼楼顶屋面\n\n"
                    "【原因】1#冷却塔脏堵\n\n"
                    "【进度】准备工作已完成"
                )
                self.assertIsNotNone(first)
                self.assertIsNotNone(second)
                self.assertNotEqual(first["entry_id"], second["entry_id"])
                self.assertTrue(controller._project_clipboard_entry_to_active(first)["ok"])
                self.assertTrue(controller._project_clipboard_entry_to_active(second)["ok"])

                qt_items = PortalRuntime.state_store.list_qt_active_items()
                reasons = sorted(item["payload"].get("reason") for item in qt_items)
                self.assertEqual(len(qt_items), 2)
                self.assertEqual(reasons, ["1#冷却塔脏堵", "5#冷却塔脏堵"])
            finally:
                PortalRuntime.state_store = previous_store

    def test_backend_ongoing_reads_qt_active_items_without_snapshot_delay(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(
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
                PortalRuntime.state_store = previous_store

    def test_backend_ongoing_keeps_live_qt_item_when_deleted_row_has_same_text(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            try:
                text = (
                    "【事件通告】状态：新增\n"
                    "【标题】EA118机房B楼重复告警测试\n"
                    "【来源】BMS系统\n"
                    "【时间】2026-07-23 08:30:00\n"
                    "【概述】B楼重复告警测试\n"
                    "【进展】值班工程师已前往现场"
                )
                deleted_item = {
                    "active_item_id": "active-deleted-same-text",
                    "record_id": "local_deleted_same_text",
                    "notice_type": "事件通告",
                    "work_type": "maintenance",
                    "title": "EA118机房B楼重复告警测试",
                    "building_codes": ["B"],
                    "status": "新增",
                    "text": text,
                }
                store.upsert_qt_active_item(
                    deleted_item, section="event", origin="qt"
                )
                store.delete_qt_active_item(
                    active_item_id=deleted_item["active_item_id"],
                    record_id=deleted_item["record_id"],
                )
                live_item = {
                    "active_item_id": "active-live-same-text",
                    "record_id": "local_live_same_text",
                    "notice_type": "事件通告",
                    "work_type": "maintenance",
                    "building_codes": ["B"],
                    "status": "新增",
                    "text": text,
                }
                store.upsert_qt_active_item(live_item, section="event", origin="qt")

                ongoing = FastAPIPortalController._get_ongoing("B")

                self.assertEqual(len(ongoing), 1)
                self.assertEqual(ongoing[0]["active_item_id"], "active-live-same-text")
                self.assertEqual(ongoing[0]["work_type"], "event")
            finally:
                PortalRuntime.state_store = previous_store

    def test_backend_ongoing_uses_explicit_scope_when_building_codes_are_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            try:
                store.upsert_qt_active_item(
                    {
                        "active_item_id": "active-explicit-scope-d",
                        "record_id": "local_explicit_scope_d",
                        "notice_type": "维保通告",
                        "work_type": "maintenance",
                        "scope": "D",
                        "building_codes": [],
                        "status": "开始",
                        "text": "【维保通告】状态：开始\n【名称】测试维护通告",
                    },
                    section="other",
                    origin="qt",
                )

                self.assertEqual(
                    len(FastAPIPortalController._get_ongoing("D")), 1
                )
                self.assertEqual(FastAPIPortalController._get_ongoing("A"), [])
            finally:
                PortalRuntime.state_store = previous_store

    def test_workbench_lite_defaults_to_all_notice_types(self):
        main_text = (BIN_DIR / "clipflow_backend" / "main.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            'request.query_params.get("work_type") or "all"', main_text
        )
        self.assertIn(
            'if work_type != "all" and work_type not in NOTICE_TYPE_BY_WORK_TYPE',
            main_text,
        )

    def test_event_ongoing_is_visible_only_in_all_and_opens_event_management(self):
        from lan_bitable_template_portal.workbench_lite import _ongoing_rows

        item = {
            "active_item_id": "active-event-b",
            "record_id": "recv_event_b",
            "notice_type": "事件通告",
            "work_type": "event",
            "scope": "B",
            "building_codes": ["B"],
            "status": "新增",
            "title": "EA118机房B楼测试事件",
            "text": "【事件通告】状态：新增\n【标题】EA118机房B楼测试事件",
        }

        all_rows = _ongoing_rows(
            [item],
            scope="B",
            work_type="all",
            month="2026-07",
            selected_id="",
            pending_page=1,
            ongoing_page=1,
        )
        maintenance_rows = _ongoing_rows(
            [item],
            scope="B",
            work_type="maintenance",
            month="2026-07",
            selected_id="",
            pending_page=1,
            ongoing_page=1,
        )

        self.assertIn('data-work-type="event"', all_rows)
        self.assertIn('data-direct-navigation="1"', all_rows)
        self.assertIn('mode=events', all_rows)
        self.assertEqual(maintenance_rows, '<div class="empty">当前没有未结束通告</div>')

    def test_qt_failed_end_rollback_persists_the_restored_active_item(self):
        workflow_text = (
            BIN_DIR / "upload_event_module" / "ui" / "main_window_workflow.py"
        ).read_text(encoding="utf-8")
        rollback_block = workflow_text.split("    def _rollback_end(", 1)[1].split(
            "    def _replace_record_id_everywhere(", 1
        )[0]
        self.assertNotIn("skip_cache=True", rollback_block)
        self.assertIn("self._commit_active_record(", rollback_block)

    def test_qt_route_reconciliation_removes_discarded_row_from_sqlite(self):
        records_text = (
            BIN_DIR / "upload_event_module" / "ui" / "main_window_records.py"
        ).read_text(encoding="utf-8")
        reconcile_block = records_text.split(
            "    def _reconcile_active_route_duplicates(", 1
        )[1].split(
            "    @staticmethod\n    def _normalize_today_in_progress_state", 1
        )[0]
        self.assertIn("self._delete_active_cache_record(data)", reconcile_block)
        self.assertIn("self._remove_active_item_widget_only", reconcile_block)

    def test_backend_ongoing_filters_deleted_qt_item_from_stale_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(
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
                PortalRuntime.state_store.replace_ongoing_items([stale_item])
                PortalRuntime.state_store.upsert_qt_active_item(
                    stale_item,
                    section="other",
                    origin="qt",
                )
                PortalRuntime.state_store.delete_qt_active_item(
                    active_item_id="active-stale-delete",
                    record_id="rec-stale-delete",
                )

                controller = FastAPIPortalController()
                ongoing = controller._get_ongoing("A")

                self.assertEqual(ongoing, [])
            finally:
                PortalRuntime.state_store = previous_store

    def test_backend_ongoing_merges_current_qt_active_items_over_stale_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                items = [
                    {
                        "active_item_id": f"active-{index}",
                        "record_id": f"target-{index}",
                        "target_record_id": f"target-{index}",
                        "notice_type": "维保通告",
                        "work_type": "maintenance",
                        "title": f"C楼测试维保{index}",
                        "building": "C楼",
                        "building_codes": ["C"],
                        "start_time": "2026-06-18 09:30",
                        "end_time": "2026-06-18 18:30",
                        "reason": f"原因{index}",
                        "text": f"【维保通告】状态：开始\n【名称】C楼测试维保{index}",
                    }
                    for index in range(1, 8)
                ]
                PortalRuntime.state_store.replace_ongoing_items(items[:5])
                for item in items:
                    PortalRuntime.state_store.upsert_qt_active_item(
                        item,
                        section="other",
                        origin="qt",
                    )

                controller = FastAPIPortalController()
                ongoing = controller._get_ongoing("C")

                self.assertEqual(len(ongoing), 7)
                self.assertEqual(
                    {item["target_record_id"] for item in ongoing},
                    {f"target-{index}" for index in range(1, 8)},
                )
            finally:
                PortalRuntime.state_store = previous_store

    def test_backend_ongoing_preserves_text_only_qt_items_with_different_notice_body(self):
        notices = [
            ("EA118机房C楼火灾报警系统维护", "C栋", "C栋消防厂家月度维护", "厂家对C栋消防月度维护"),
            ("EA118机房C楼交直流列头柜及PDU维护", "C楼", "按计划对C栋直流列头柜及PDU季度维护", "按计划对C栋直流列头柜及PDU季度维护，保证供电正常"),
            ("EA118机房C楼消火栓系统维护", "C栋", "C栋消防厂家月度维护", "厂家对C栋消防月度维护"),
            ("EA118机房C楼消防排烟系统维护", "C栋", "C栋消防厂家半年度维护", "厂家对C栋消防半年度维护"),
            ("EA118机房C楼消防排烟系统维护", "C栋", "C栋消防厂家月度维护", "厂家对C栋消防月度维护"),
            ("EA118机房C楼自动喷淋灭火系统维护", "C栋", "厂家对C楼自动喷淋灭火系统月度维护", "按计划对C楼自动喷淋灭火系统进行月度维护"),
            ("EA118机房C楼冷站Y型过滤器检查", "C楼C-127冷冻站", "工程师对C楼冷站Y型过滤器进行年度检查", "按维保计划工程师对C楼冷站Y型过滤器进行检查、清洗，确保制冷单元运行正常"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                for index, (title, location, content, reason) in enumerate(notices, start=1):
                    text = (
                        "【维保通告】状态：开始\n"
                        f"【名称】{title}\n"
                        "【时间】2026-06-18 09:30~2026-06-18 18:30\n"
                        f"【位置】{location}\n"
                        f"【内容】{content}\n"
                        f"【原因】{reason}\n"
                        "【影响】对IT业务无影响，不会触发BA和BMS系统相关告警\n"
                        "【进度】准备工作已完成，人员已就位，是否可以操作？"
                    )
                    PortalRuntime.state_store.upsert_qt_active_item(
                        {
                            "active_item_id": f"active-text-only-{index}",
                            "record_id": f"target-text-only-{index}",
                            "target_record_id": f"target-text-only-{index}",
                            "notice_type": "维保通告",
                            "work_type": "maintenance",
                            "title": title,
                            "building": "C楼",
                            "building_codes": ["C"],
                            "time_str": "2026-06-18 09:30~2026-06-18 18:30",
                            "text": text,
                        },
                        section="other",
                        origin="qt",
                    )

                qt_items = PortalRuntime.state_store.list_qt_active_items()
                ongoing = FastAPIPortalController._get_ongoing("C")

                self.assertEqual(len(qt_items), 7)
                self.assertEqual(len(ongoing), 7)
            finally:
                PortalRuntime.state_store = previous_store

    def test_qt_active_scope_signature_changes_with_current_qt_active_items(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                empty_signature, empty_count = FastAPIPortalController._scoped_qt_active_signature("C")
                PortalRuntime.state_store.upsert_qt_active_item(
                    {
                        "active_item_id": "active-sse-c-1",
                        "target_record_id": "target-sse-c-1",
                        "record_id": "target-sse-c-1",
                        "notice_type": "维保通告",
                        "work_type": "maintenance",
                        "title": "C楼SSE签名测试",
                        "building": "C楼",
                        "building_codes": ["C"],
                        "text": "【维保通告】状态：开始\n【名称】C楼SSE签名测试",
                    },
                    section="other",
                    origin="qt",
                )

                signature, count = FastAPIPortalController._scoped_qt_active_signature("C")

                self.assertEqual(empty_count, 0)
                self.assertEqual(count, 1)
                self.assertNotEqual(signature, empty_signature)
            finally:
                PortalRuntime.state_store = previous_store

    def test_backend_ongoing_does_not_restore_stale_snapshot_when_qt_store_is_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                PortalRuntime.state_store.replace_ongoing_items(
                    [
                        {
                            "active_item_id": "active-ended-change",
                            "record_id": "rec-ended-change",
                            "target_record_id": "rec-ended-change",
                            "notice_type": "变更通告",
                            "work_type": "change",
                            "title": "D楼已结束变更",
                            "building_codes": ["D"],
                            "text": "【变更通告】状态:结束【名称】D楼已结束变更",
                        },
                        {
                            "active_item_id": "active-running-change",
                            "record_id": "rec-running-change",
                            "target_record_id": "rec-running-change",
                            "notice_type": "变更通告",
                            "work_type": "change",
                            "title": "D楼进行中变更",
                            "building_codes": ["D"],
                            "text": "【变更通告】状态:更新【名称】D楼进行中变更",
                        },
                    ]
                )

                controller = FastAPIPortalController()
                ongoing = controller._get_ongoing("D")

                self.assertEqual(ongoing, [])
            finally:
                PortalRuntime.state_store = previous_store

    def test_backend_ongoing_keeps_active_item_when_deleted_duplicate_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            PortalRuntime.state_store = store
            try:
                active_payload = {
                    "active_item_id": "active-survivor",
                    "record_id": "rec-survivor",
                    "target_record_id": "rec-survivor",
                    "notice_type": "维保通告",
                    "work_type": "maintenance",
                    "title": "C楼软删除重复项测试",
                    "building": "C楼",
                    "building_codes": ["C"],
                    "time_str": "2026-06-18 09:30~2026-06-18 18:30",
                    "content": "按计划维护",
                    "reason": "按计划维护",
                    "text": (
                        "【维保通告】状态：开始\n"
                        "【名称】C楼软删除重复项测试\n"
                        "【时间】2026-06-18 09:30~2026-06-18 18:30\n"
                        "【内容】按计划维护\n"
                        "【原因】按计划维护"
                    ),
                }
                store.upsert_qt_active_item(active_payload, section="other", origin="qt")
                now = time.time()
                duplicate_payload = dict(active_payload)
                duplicate_payload["active_item_id"] = "active-soft-deleted-duplicate"
                duplicate_payload["record_id"] = "rec-survivor"
                duplicate_payload["target_record_id"] = "rec-survivor"
                conn = store._connect()
                try:
                    conn.execute(
                        """
                        INSERT INTO qt_active_items(
                            active_item_id, record_id, notice_type, section, sort_order,
                            origin, payload_json, updated_at, deleted_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "active-soft-deleted-duplicate",
                            "rec-survivor",
                            "维保通告",
                            "other",
                            0,
                            "qt",
                            json.dumps(duplicate_payload, ensure_ascii=False),
                            now,
                            now,
                        ),
                    )
                    conn.commit()
                finally:
                    conn.close()

                ongoing = FastAPIPortalController()._get_ongoing("C")

                self.assertEqual(len(ongoing), 1)
                self.assertEqual(ongoing[0]["active_item_id"], "active-survivor")
            finally:
                PortalRuntime.state_store = previous_store

    def test_qt_shell_bootstrap_removes_ended_active_item(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_state_store = PortalRuntime.state_store
        temp_dir = tempfile.TemporaryDirectory()
        PortalRuntime.state_store = LanPortalStateStore(
            Path(temp_dir.name) / "state.sqlite3"
        )
        PortalRuntime.state_store.upsert_qt_active_item(
            {
                "active_item_id": "active-ended-bootstrap",
                "record_id": "rec-ended-bootstrap",
                "notice_type": "变更通告",
                "work_type": "change",
                "title": "D楼已结束变更",
                "building_codes": ["D"],
                "status": "结束",
                "text": "【变更通告】状态:结束【名称】D楼已结束变更",
            },
            section="other",
            origin="qt",
        )
        client = TestClient(controller._build_app())
        try:
            response = client.get("/api/qt/shell/bootstrap")

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["data"]["active_items"], [])
            self.assertEqual(PortalRuntime.state_store.list_qt_active_items(), [])
            deleted = PortalRuntime.state_store.list_qt_active_items(include_deleted=True)
            self.assertEqual(len(deleted), 1)
            self.assertIsNotNone(deleted[0].get("deleted_at"))
        finally:
            PortalRuntime.state_store = original_state_store
            temp_dir.cleanup()

    def test_parser_accepts_legacy_change_notice_marker(self):
        info = extract_event_info(
            "【变更通告】状态：开始\n\n"
            "【标题】测试测试测试变更\n\n"
            "【等级】低\n\n"
            "【时间】2026-05-25 09:30~2026-05-25 18:30"
        )

        self.assertIsNotNone(info)
        self.assertEqual(info["notice_type"], "变更通告")
        self.assertEqual(info["title"], "测试测试测试变更")
        self.assertEqual(info["status"], "开始")

    def test_parser_accepts_compact_ended_change_notice(self):
        info = extract_event_info(
            "【变更通告】状态:结束【名称】EA118-D楼直流屏蓄电池整组更换变更"
        )

        self.assertIsNotNone(info)
        self.assertEqual(info["notice_type"], "变更通告")
        self.assertEqual(info["status"], "结束")
        self.assertEqual(info["title"], "EA118-D楼直流屏蓄电池整组更换变更")

    def test_backend_clipboard_target_record_requires_same_notice_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                PortalRuntime.state_store.upsert_qt_active_item(
                    {
                        "active_item_id": "active-change-1",
                        "record_id": "rec-same-id",
                        "notice_type": "变更通告",
                        "text": "【变更通告】状态：开始\n\n【名称】原变更",
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
                qt_items = PortalRuntime.state_store.list_qt_active_items()

                self.assertTrue(result["ignored"])
                self.assertEqual(len(qt_items), 1)
                self.assertEqual(qt_items[0]["payload"]["notice_type"], "变更通告")
            finally:
                PortalRuntime.state_store = previous_store

    def test_state_store_migrates_legacy_change_notice_labels(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = Path(tmp) / "lan_portal_state.sqlite3"
            store = LanPortalStateStore(db_path)
            store.schema_health()
            now = time.time()
            legacy_payload = {
                "active_item_id": "legacy-change-active",
                "record_id": "target-legacy-change",
                "target_record_id": "target-legacy-change",
                "work_type": "change",
                "notice_type": "设备变更",
                "title": "A楼网络设备变更测试",
                "text": "【设备变更】状态：开始\n【名称】A楼网络设备变更测试",
            }
            with closing(sqlite3.connect(str(db_path))) as conn:
                conn.execute(
                    """
                    INSERT INTO qt_active_items(
                        active_item_id, record_id, notice_type, section, sort_order,
                        origin, payload_json, updated_at, deleted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        "legacy-change-active",
                        "target-legacy-change",
                        "设备变更",
                        "other",
                        0,
                        "qt",
                        json.dumps(legacy_payload, ensure_ascii=False),
                        now,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO notice_identity_map(
                        identity_id, work_type, notice_type, active_item_id,
                        target_record_id, title, building_codes_json, payload_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "change:target:target-legacy-change",
                        "change",
                        "设备变更",
                        "legacy-change-active",
                        "target-legacy-change",
                        "A楼网络设备变更测试",
                        "[]",
                        json.dumps(legacy_payload, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO notice_actions(key, payload_json, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    (
                        "legacy-job",
                        json.dumps({"prepared": legacy_payload}, ensure_ascii=False),
                        now,
                    ),
                )
                conn.commit()

            migrated = LanPortalStateStore(db_path)
            items = migrated.list_qt_active_items()
            identity = migrated.resolve_notice_identity(
                work_type="change",
                target_record_id="target-legacy-change",
            )
            job_doc = migrated.get_document("notice_action_job", "legacy-job")

            self.assertEqual(items[0]["notice_type"], "变更通告")
            self.assertEqual(items[0]["payload"]["notice_type"], "变更通告")
            self.assertIn("【变更通告】状态：开始", items[0]["payload"]["text"])
            self.assertNotIn("【设备变更】", items[0]["payload"]["text"])
            self.assertIn("网络设备变更测试", items[0]["payload"]["title"])
            self.assertIn("设备变更测试", items[0]["payload"]["title"])
            self.assertEqual(identity["notice_type"], "变更通告")
            self.assertEqual(identity["payload"]["notice_type"], "变更通告")
            self.assertEqual(job_doc["prepared"]["notice_type"], "变更通告")
            self.assertIn("【变更通告】状态：开始", job_doc["prepared"]["text"])
            self.assertNotIn("【设备变更】", job_doc["prepared"]["text"])
            migrated.shutdown_write_worker(timeout=1.0)
            store.shutdown_write_worker(timeout=1.0)
            del migrated
            del store
            gc.collect()

    def test_state_store_canonicalizes_legacy_change_notice_on_upsert(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            store.upsert_qt_active_item(
                {
                    "active_item_id": "legacy-change-upsert",
                    "record_id": "target-legacy-upsert",
                    "work_type": "change",
                    "notice_type": "设备变更",
                    "text": "【设备变更】状态：开始\n【名称】A楼网络设备变更测试",
                },
                section="other",
                origin="qt",
            )
            item = store.list_qt_active_items()[0]

            self.assertEqual(item["notice_type"], "变更通告")
            self.assertEqual(item["payload"]["notice_type"], "变更通告")
            self.assertIn("【变更通告】状态：开始", item["payload"]["text"])
            self.assertNotIn("【设备变更】", item["payload"]["text"])

    def test_history_payload_imports_legacy_file_to_sqlite(self):
        self.assertFalse(hasattr(MainWindowUiMixin, "_load_history_payload"))
        self.assertFalse(hasattr(MainWindowUiMixin, "load_all_history"))

    def test_history_payload_does_not_overwrite_existing_sqlite_history(self):
        self.assertFalse(hasattr(MainWindowUiMixin, "_save_history_payload"))
        self.assertFalse(hasattr(MainWindowUiMixin, "save_to_history_file"))

    def test_portal_frontend_dist_is_production_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dist = root / "frontend" / "dist"
            (dist / "assets").mkdir(parents=True)
            (dist / "index.html").write_text("vue", encoding="utf-8")
            (dist / "assets" / "app.js").write_text("dist", encoding="utf-8")
            (dist / "assets" / "logo.png").write_text("dist-logo", encoding="utf-8")

            with patch.object(portal_server_module, "FRONTEND_DIST_DIR", dist):
                self.assertEqual(portal_server_module.portal_index_file(), dist / "index.html")
                self.assertEqual(
                    portal_server_module.portal_asset_file(Path("app.js")),
                    dist / "assets" / "app.js",
                )
                self.assertEqual(
                    portal_server_module.portal_asset_file(Path("logo.png")),
                    dist / "assets" / "logo.png",
                )

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
                "record_id": "rec_source_repair_1",
                "source_record_id": "rec_source_repair_1",
                "source_app_token": "source-app",
                "source_table_id": "source-table",
            }

            with patch.object(
                service,
                "_process_repair_link_tasks_async",
            ) as process_async:
                service._schedule_repair_link_task_after_success(
                    prepared,
                    action="start",
                    target_record_id="rec_target_repair_1",
                    job_id="job-1",
                )

            tasks = service._state_store.list_due_repair_link_tasks(
                now=time.time() + 1,
                limit=10,
            )
            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0]["source_record_id"], "rec_source_repair_1")
            self.assertEqual(tasks[0]["target_record_id"], "rec_target_repair_1")
            self.assertEqual(tasks[0]["sync_table_id"], "")
            process_async.assert_called_once_with()

    def test_repair_link_task_is_retried_after_repair_update_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            prepared = {
                "work_type": "repair",
                "repair_management_record_id": "rec_source_repair_update",
            }

            with patch.object(
                service,
                "_process_repair_link_tasks_async",
            ) as process_async:
                service._schedule_repair_link_task_after_success(
                    prepared,
                    action="update",
                    target_record_id="rec_target_repair_update",
                    job_id="job-update",
                )

            tasks = service._state_store.list_due_repair_link_tasks(
                now=time.time() + 1,
                limit=10,
            )
            self.assertEqual(len(tasks), 1)
            self.assertEqual(
                tasks[0]["source_record_id"],
                "rec_source_repair_update",
            )
            self.assertEqual(
                tasks[0]["target_record_id"],
                "rec_target_repair_update",
            )
            self.assertEqual(tasks[0]["action"], "update")
            process_async.assert_called_once_with()

    def test_process_due_repair_link_task_rejects_relation_field_without_sync_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            source_record_id = "source-repair-1"
            target_record_id = "target-repair-1"
            service._state_store.upsert_repair_link_task(
                {
                    "task_key": f"repair_link:{source_record_id}:{target_record_id}",
                    "status": "pending",
                    "source_app_token": REPAIR_SOURCE_APP_TOKEN,
                    "source_table_id": REPAIR_MANAGEMENT_TABLE_ID,
                    "source_record_id": source_record_id,
                    "sync_app_token": "source-app",
                    "sync_table_id": "sync-table",
                    "target_app_token": "target-app",
                    "target_table_id": "target-table",
                    "target_record_id": target_record_id,
                    "link_field_name": "设备检修关联",
                    "due_at": time.time() - 1,
                    "attempts": 0,
                    "max_attempts": 1,
                }
            )
            service._request_json = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: self.fail(
                    "retired repair sync tables must never be queried"
                )
            )
            service._patch_record_fields_exact = (  # type: ignore[method-assign]
                lambda **_kwargs: self.fail(
                    "relation fields must not receive a text target id"
                )
            )
            link_meta = FieldMeta(
                "fld_link",
                "设备检修关联",
                "DuplexLink",
                21,
                False,
                {},
                [],
                False,
            )
            service._load_table_fields = (  # type: ignore[method-assign]
                lambda **_kwargs: ([link_meta], {"设备检修关联": link_meta})
            )

            stats = service.process_due_repair_link_tasks(limit=3)

            self.assertEqual(stats["failed"], 1)
            self.assertEqual(
                service._state_store.list_due_repair_link_tasks(
                    now=time.time() + 3600,
                    limit=10,
                ),
                [],
            )

    def test_process_due_repair_link_task_writes_text_record_id_directly(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            source_record_id = "rec_source_text_link"
            target_record_id = "rec_target_text_link"
            service._state_store.upsert_repair_link_task(
                {
                    "task_key": f"repair_link:{source_record_id}:{target_record_id}",
                    "status": "pending",
                    "source_app_token": REPAIR_SOURCE_APP_TOKEN,
                    "source_table_id": REPAIR_MANAGEMENT_TABLE_ID,
                    "source_record_id": source_record_id,
                    "sync_app_token": "source-app",
                    "sync_table_id": "sync-table",
                    "target_record_id": target_record_id,
                    "link_field_name": "设备检修关联",
                    "due_at": time.time() - 1,
                }
            )
            link_meta = FieldMeta(
                "fld_link",
                "设备检修关联",
                "Text",
                1,
                False,
                {},
                [],
                False,
            )
            service._load_table_fields = (  # type: ignore[method-assign]
                lambda **_kwargs: ([link_meta], {"设备检修关联": link_meta})
            )
            service._request_json = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: self.fail(
                    "text ID fields must not query the repair sync table"
                )
            )
            updates: list[dict] = []
            service._patch_record_fields_exact = (  # type: ignore[method-assign]
                lambda **kwargs: updates.append(kwargs) or {"code": 0}
            )

            stats = service.process_due_repair_link_tasks(limit=3)

            self.assertEqual(stats["linked"], 1)
            self.assertEqual(len(updates), 1)
            self.assertEqual(
                updates[0]["fields"],
                {"设备检修关联-L": target_record_id},
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

    def test_pending_repair_link_task_is_expedited_after_upgrade(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            store.upsert_repair_link_task(
                {
                    "task_key": "repair_link:source-upgrade:target-upgrade",
                    "status": "pending",
                    "source_record_id": "source-upgrade",
                    "target_record_id": "target-upgrade",
                    "due_at": 5200,
                    "attempts": 0,
                    "max_attempts": 18,
                }
            )

            updated = store.expedite_pending_repair_link_tasks(now=1000)
            tasks = store.list_due_repair_link_tasks(now=1000, limit=10)

            self.assertEqual(updated, 1)
            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0]["source_record_id"], "source-upgrade")
            self.assertEqual(float(tasks[0]["due_at"]), 1000)

    def test_repair_link_task_reschedules_when_text_write_temporarily_fails(self):
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

            link_meta = FieldMeta(
                "fld_link",
                "设备检修关联",
                "Text",
                1,
                False,
                {},
                [],
                False,
            )
            service._load_table_fields = (  # type: ignore[method-assign]
                lambda **_kwargs: ([link_meta], {"设备检修关联": link_meta})
            )
            service._request_json = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: self.fail(
                    "text ID retries must not query a repair sync table"
                )
            )
            service._patch_record_fields_exact = (  # type: ignore[method-assign]
                lambda **_kwargs: (_ for _ in ()).throw(
                    PortalError("飞书临时不可用")
                )
            )
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
            self.assertIn("飞书临时不可用", tasks[0]["last_error"])

    def test_local_remove_active_item_never_deletes_remote_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            item = {
                "active_item_id": "active-local-remove",
                "record_id": "recv-local-remove",
                "target_record_id": "recv-local-remove",
                "notice_type": "维保通告",
                "work_type": "maintenance",
                "title": "A楼本地移除测试",
                "building_codes": ["A"],
                "status": "开始",
                "text": "【维保通告】状态：开始\n【名称】A楼本地移除测试",
            }
            store.upsert_qt_active_item(item, section="other", origin="qt")
            with patch.object(PortalRuntime, "state_store", store), patch(
                "lan_bitable_template_portal.server.delete_bitable_record"
            ) as remote_delete:
                first = PortalRuntime.execute_local_remove_active_item(
                    {"data_dict": item}
                )
                second = PortalRuntime.execute_local_remove_active_item(
                    {"data_dict": item}
                )

            self.assertTrue(first["ok"])
            self.assertTrue(first["qt_removed"])
            self.assertFalse(first["remote_deleted"])
            self.assertTrue(second["ok"])
            self.assertTrue(second["already_absent"])
            self.assertFalse(second["remote_deleted"])
            self.assertEqual(store.list_qt_active_items(), [])
            remote_delete.assert_not_called()

    def test_local_remove_active_item_reports_sqlite_delete_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            item = {
                "active_item_id": "active-local-remove-failure",
                "record_id": "recv-local-remove-failure",
                "target_record_id": "recv-local-remove-failure",
                "notice_type": "维保通告",
                "work_type": "maintenance",
                "building_codes": ["A"],
                "status": "开始",
                "text": "【维保通告】状态：开始\n【名称】A楼本地移除失败测试",
            }
            store.upsert_qt_active_item(item, section="other", origin="qt")
            with patch.object(PortalRuntime, "state_store", store), patch.object(
                store,
                "delete_qt_active_item",
                side_effect=RuntimeError("sqlite locked"),
            ):
                result = PortalRuntime.execute_local_remove_active_item(
                    {"data_dict": item}
                )

            self.assertFalse(result["ok"])
            self.assertFalse(result["remote_deleted"])
            self.assertIn("sqlite locked", result["message"])
            self.assertEqual(len(store.list_qt_active_items()), 1)

    def test_server_get_ongoing_reads_qt_active_items_before_qt_callback(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            for item in (
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
            ):
                store.upsert_qt_active_item(item, section="other", origin="qt")

            def unexpected_callback(scope):
                raise AssertionError(f"Qt callback should not be called: {scope}")

            dummy = type(
                "_DummyPortalRuntime",
                (),
                {"service": _TestMaintenancePortalService()},
            )()
            with patch.object(PortalRuntime, "state_store", store), patch.object(
                PortalRuntime, "ongoing_callback", unexpected_callback
            ):
                result = PortalRuntime._get_ongoing(dummy, "A")

            self.assertEqual([item["active_item_id"] for item in result], ["active-a"])

    def test_server_get_ongoing_filters_ended_items(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            for item in (
                {
                    "active_item_id": "active-ended",
                    "work_type": "change",
                    "notice_type": "变更通告",
                    "title": "A楼已结束变更",
                    "building": "A楼",
                    "building_codes": ["A"],
                    "text": "【变更通告】状态:结束【名称】A楼已结束变更",
                },
                {
                    "active_item_id": "active-running",
                    "work_type": "change",
                    "notice_type": "变更通告",
                    "title": "A楼进行中变更",
                    "building": "A楼",
                    "building_codes": ["A"],
                    "text": "【变更通告】状态:更新【名称】A楼进行中变更",
                },
            ):
                store.upsert_qt_active_item(item, section="other", origin="qt")

            dummy = type(
                "_DummyPortalRuntime",
                (),
                {"service": _TestMaintenancePortalService()},
            )()
            with patch.object(PortalRuntime, "state_store", store), patch.object(
                PortalRuntime, "ongoing_callback", None
            ):
                result = PortalRuntime._get_ongoing(dummy, "A")

            self.assertEqual([item["active_item_id"] for item in result], ["active-running"])

    def test_server_get_ongoing_keeps_empty_qt_store_authoritative(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "lan_portal_state.sqlite3")
            store.replace_ongoing_items(
                [
                    {
                        "active_item_id": "stale-snapshot-item",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "title": "A楼旧快照维保",
                        "building_codes": ["A"],
                    }
                ]
            )
            dummy = type(
                "_DummyPortalRuntime",
                (),
                {"service": _TestMaintenancePortalService()},
            )()
            with patch.object(PortalRuntime, "state_store", store), patch.object(
                PortalRuntime, "ongoing_callback", None
            ):
                result = PortalRuntime._get_ongoing(dummy, "A")

            self.assertEqual(result, [])

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

            service._records = [_build_record("m1", "A楼", "过滤网维护", _TEST_MONTH_LABEL)]
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

    def test_maintenance_memory_reuses_same_name_and_cycle_across_months(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._remember_draft_fields(
                building="C楼",
                maintenance_total="EA118机房C楼空气质量检测月度维护",
                item_name="EA118机房C楼空气质量检测月度维护",
                maintenance_cycle="月度",
                location="C楼检测区域",
                content="按月度计划开展空气质量检测",
                reason="例行月度维护",
                impact="对业务无影响",
                extra_fields={
                    "specialty": "暖通",
                    "progress": "准备工作已完成，人员已就位",
                },
            )
            next_month = _build_record(
                "maintenance-next-month",
                "C楼",
                "空气质量检测月度维护",
                "8月",
                maintenance_cycle="每月",
            )
            next_month["display_fields"].update(
                {
                    "计划开始时间": "2026-08-18 09:30",
                    "计划结束时间": "2026-08-18 18:30",
                }
            )

            serialized = service._serialize_record(next_month)
            memory = serialized["memory"]
            draft = workbench_lite_module._draft_from_record(
                serialized,
                work_type="maintenance",
            )

            self.assertEqual(memory["location"], "C楼检测区域")
            self.assertEqual(memory["content"], "按月度计划开展空气质量检测")
            self.assertEqual(memory["specialty"], "暖通")
            self.assertEqual(draft["progress"], "准备工作已完成，人员已就位")
            self.assertEqual(draft["start_time"], f"{dt.date.today().isoformat()}T09:30")
            self.assertEqual(draft["end_time"], f"{dt.date.today().isoformat()}T18:30")

            different_cycle = _build_record(
                "maintenance-next-quarter",
                "C楼",
                "空气质量检测月度维护",
                "8月",
                maintenance_cycle="每季",
            )
            self.assertFalse(
                any(service._get_record_memory(different_cycle).get(key) for key in (
                    "location",
                    "content",
                    "reason",
                    "impact",
                ))
            )

    def test_current_month_source_record_reuses_month_independent_notice_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = service._current_month_label()
            service._remember_draft_fields(
                building="A楼",
                maintenance_total="UPS厂商巡检维护",
                item_name="UPS厂商巡检维护",
                maintenance_cycle="月度",
                location="A-245配电室",
                content="按上月记忆执行UPS巡检",
                reason="按月度计划开展维护",
                impact="对IT设备无影响",
                extra_fields={
                    "progress": "准备工作已完成，人员已就位",
                    "specialty": "电气",
                },
            )
            service._records = [
                _build_record(
                    "maintenance-current-memory",
                    "A楼",
                    "UPS厂商巡检维护",
                    current_month,
                    maintenance_cycle="月度",
                )
            ]
            service._records[0]["display_fields"].update(
                {
                    "计划开始时间": "2026-06-18 09:30",
                    "计划结束时间": "2026-06-18 18:30",
                }
            )

            result = service.query_records(
                month=current_month,
                scope="A",
                work_type=WORK_TYPE_MAINTENANCE,
            )

            self.assertEqual(len(result["records"]), 1)
            memory = result["records"][0]["memory"]
            self.assertEqual(memory["content"], "按上月记忆执行UPS巡检")
            self.assertEqual(memory["location"], "A-245配电室")
            self.assertEqual(memory["maintenance_cycle"], "月度")

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

    def test_qt_active_cache_uses_target_record_id_without_overwriting_json(self):
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
                            "target_record_id": "active-1",
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
            store.upsert_qt_active_item(
                {
                    "active_item_id": "active-local-1",
                    "record_id": "active-1",
                    "target_record_id": "active-1",
                    "notice_type": "维保通告",
                    "text": "SQLite 原始通告",
                },
                section="other",
                origin="qt",
            )
            cache_store = ActiveCacheStore(str(cache_file), state_store=store)

            payload = cache_store.load_payload()
            self.assertEqual(payload["other"][0]["data"]["record_id"], "active-1")
            self.assertTrue(
                cache_store.patch_record_fields(
                    record_id="active-1", patch={"text": "SQLite 通告"}
                )
            )
            qt_items = store.list_qt_active_items()
            stored = store.get_document("qt_active_cache", "active_cache")

            self.assertEqual(
                qt_items[0]["payload"]["text"],
                "SQLite 通告",
            )
            self.assertIsNone(stored)
            self.assertEqual(cache_file.read_text(encoding="utf-8"), original)

    def test_qt_history_migrates_to_sqlite_without_overwriting_json(self):
        self.assertFalse(hasattr(MainWindowUiMixin, "_delete_history_payload"))
        self.assertFalse(hasattr(MainWindowUiMixin, "_trim_history_by_age"))

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
            previous_start = service._recent_month_starts()[1]
            month_label = f"{previous_start.month}月"
            action_day = previous_start.replace(
                day=min(28, previous_start.day + 1),
                hour=9,
                minute=30,
                second=0,
                microsecond=0,
            )
            end_day = action_day.replace(hour=18, minute=30)
            start_text = action_day.strftime("%Y-%m-%dT%H:%M")
            end_text = end_day.strftime("%Y-%m-%dT%H:%M")
            service._records = [
                _build_record(
                    "rec1",
                    "A楼",
                    "过滤网维护",
                    month_label,
                    maintenance_cycle="每月",
                )
            ]

            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "scope": "A",
                    "record_id": "rec1",
                    "specialty": "全部",
                    "start_time": start_text,
                    "end_time": end_text,
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
                    "start_time": start_text,
                    "end_time": end_text,
                    "location": "测试位置",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "已结束",
                    "extra_images": [{"file_token": "site-photo-token"}],
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

            result = service.query_records(month=month_label, scope="A")
            summary = result["records"][0]["work_summary"]
            self.assertEqual(summary["status"], "已结束")
            self.assertEqual(summary["source_record_id"], "rec1")
            self.assertEqual(summary["active_item_id"], "active-1")
            self.assertEqual(summary["target_record_id"], "bitable-rec-1")
            self.assertEqual(summary["maintenance_cycle"], "每月")
            self.assertRegex(summary["completed_date"], r"^\d{4}-\d{2}-\d{2}$")
            self.assertEqual(summary["completed_date"], summary["ended_at"][:10])
            work_status_items = service._load_work_status_items_locked("A")
            self.assertEqual(work_status_items[0]["maintenance_cycle"], "每月")
            self.assertEqual([item["action"] for item in summary["actions"]], ["start", "end"])

    def test_orphan_started_item_is_pruned_when_qt_and_target_record_are_gone(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), service_cls=_TargetLookupService)
            service._records = [_build_record("rec1", "A楼", "过滤网维护", _TEST_MONTH_LABEL)]

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
            result = service.query_records(month=_TEST_MONTH_LABEL, scope="A", ongoing_items=[])
            daily = service.get_daily_summary(scope="A", ongoing_items=[])

            self.assertEqual(removed["removed"], 1)
            self.assertEqual(result["records"][0]["work_summary"], {})
            self.assertEqual(daily["items"], [])

    def test_started_item_is_kept_when_target_record_still_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), service_cls=_TargetLookupService)
            service._records = [_build_record("rec1", "A楼", "过滤网维护", _TEST_MONTH_LABEL)]
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
            result = service.query_records(month=_TEST_MONTH_LABEL, scope="A", ongoing_items=[])

            self.assertEqual(removed["removed"], 0)
            self.assertEqual(result["records"][0]["work_summary"]["status"], "进行中")

    def test_orphan_reconcile_suppresses_transient_feishu_fail_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("rec1", "A楼", "过滤网维护", _TEST_MONTH_LABEL)]
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

            result = service.query_records(month=_TEST_MONTH_LABEL, scope="A", ongoing_items=[])
            self.assertEqual(removed["removed"], 0)
            self.assertEqual(result["records"][0]["work_summary"]["status"], "进行中")
            self.assertFalse(
                any("目标表孤儿状态校验失败" in item for item in service._load_warnings)
            )

    def test_deleted_ongoing_item_clears_today_summary_and_work_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("rec1", "A楼", "过滤网维护", _TEST_MONTH_LABEL)]

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
            result = service.query_records(month=_TEST_MONTH_LABEL, scope="A", ongoing_items=[])
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
            service = self._new_temp_service(root)
            previous_start = service._recent_month_starts()[1]
            completed_day = previous_start.replace(day=min(28, previous_start.day + 1))
            completed_date = completed_day.strftime("%Y-%m-%d")
            started_at = f"{completed_date} 09:30"
            ended_at = f"{completed_date} 18:30"
            (summary_dir / f"{completed_date}.json").write_text(
                json.dumps(
                    {
                        "date": completed_date,
                        "items": [
                            {
                                "key": "source:rec1",
                                "fallback_key": "title:A楼:EA118机房A楼过滤网维护",
                                "source_record_id": "rec1",
                                "active_item_id": "active-1",
                                "target_record_id": "bitable-rec-1",
                                "title": "EA118机房A楼过滤网维护",
                                "building": "A楼",
                                "building_code": "A",
                                "specialty": "电气",
                                "status": "已结束",
                                "started_at": started_at,
                                "ended_at": ended_at,
                                "actions": [
                                    {
                                        "action": "start",
                                        "label": "开始",
                                        "time": started_at,
                                        "job_id": "job-start",
                                    },
                                    {
                                        "action": "end",
                                        "label": "结束",
                                        "time": ended_at,
                                        "job_id": "job-end",
                                    },
                                ],
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            month_label = f"{previous_start.month}月"
            service._records = [_build_record("rec1", "A楼", "过滤网维护", month_label)]

            result = service.query_records(month=month_label, scope="A")
            summary = result["records"][0]["work_summary"]
            self.assertEqual(summary["status"], "已结束")
            self.assertEqual(summary["completed_date"], completed_date)
            self.assertEqual(summary["ended_at"], ended_at)

    def test_maintenance_source_ongoing_is_displayed_and_updates_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [
                _build_record("m-running", "A楼", "过滤网维护", _TEST_MONTH_LABEL, status="进行中")
            ]
            service._upsert_work_status_item_locked(
                {
                    "source_record_id": "m-running",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "target_record_id": "target-m-running",
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

            result = service.query_records(month=_TEST_MONTH_LABEL, scope="A", ongoing_items=[])
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

    def test_maintenance_end_uses_active_item_target_when_frontend_record_is_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [
                _build_record("m-running", "A楼", "过滤网维护", _TEST_MONTH_LABEL, status="进行中")
            ]
            service._upsert_work_status_item_locked(
                {
                    "source_record_id": "m-running",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "target_record_id": "target-m-running",
                    "active_item_id": "active-m-running",
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

            job_id, should_start = service.create_action_job(
                {
                    "action": "end",
                    "scope": "A",
                    "record_id": "m-running",
                    "source_record_id": "m-running",
                    "active_item_id": "active-m-running",
                    "title": "EA118机房A楼过滤网维护",
                    "building": "A楼",
                    "specialty": "电气",
                    "start_time": "2026-05-08T09:30",
                    "end_time": "2026-05-08T18:30",
                    "progress": "已完成",
                    "extra_images": [{"file_token": "site-photo-token"}],
                    "operation_id": "maintenance-end-source-record",
                }
            )

            self.assertTrue(should_start)
            prepared = service.prepare_action_job(job_id)
            self.assertEqual(prepared["action"], "end")
            self.assertEqual(prepared["record_id"], "m-running")
            self.assertEqual(prepared["target_record_id"], "target-m-running")

    def test_change_records_use_precise_scope_and_allowed_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("m1", "A楼", "过滤网维护", _TEST_MONTH_LABEL)]
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
                campus = service.query_records(month=_TEST_MONTH_LABEL, scope="CAMPUS")
            self.assertEqual(
                [item["record_id"] for item in campus["records"]],
                ["c2", "c3"],
            )
            self.assertEqual(campus["ongoing"], [])

            with patch.object(
                service, "_target_records_for_notice_type", return_value=[]
            ):
                single = service.query_records(month=_TEST_MONTH_LABEL, scope="A")
            self.assertEqual(
                [item["record_id"] for item in single["records"]],
                ["m1", "c1", "c2", "c3"],
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

            current = service.query_records(
                scope="A",
                ongoing_items=[
                    {
                        "active_item_id": "active-previous-month",
                        "source_record_id": "m-previous",
                        "target_record_id": "target-previous-month",
                        "record_id": "target-previous-month",
                        "work_type": WORK_TYPE_MAINTENANCE,
                        "notice_type": "维保通告",
                        "title": "上月跨月进行中维保",
                        "building": "A楼",
                        "building_codes": ["A"],
                        "status": "进行中",
                    }
                ],
            )
            self.assertEqual(
                [item["record_id"] for item in current["records"]],
                ["m-current", "c-current", "r-current"],
            )
            self.assertEqual(
                current["filters"]["default_month"],
                current_label,
            )
            self.assertEqual(
                [item["active_item_id"] for item in current["ongoing"]],
                ["active-previous-month"],
            )

            previous = service.query_records(month=previous_label, scope="A")
            self.assertEqual(
                [item["record_id"] for item in previous["records"]],
                ["m-previous", "c-previous", "r-previous"],
            )

    def test_change_source_ongoing_can_start_without_active_item(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record("c-e", building="E楼", progress="进行中", title="E楼进行中变更")
            ]

            result = service.query_records(month=_TEST_MONTH_LABEL, scope="E")
            self.assertEqual([item["record_id"] for item in result["records"]], ["c-e"])

            job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "work_type": "change",
                    "scope": "E",
                    "record_id": "c-e",
                    "source_record_id": "c-e",
                    "title": "E楼进行中变更",
                    "level": "I3",
                    "start_time": _test_datetime(8, "09:00"),
                    "end_time": _test_datetime(8, "18:00"),
                    "location": "E楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备工作已完成",
                    "operation_id": "change-e-start",
                }
            )
            self.assertTrue(should_start)
            prepared = service.prepare_action_job(job_id)
            self.assertEqual(prepared["action"], "start")
            self.assertEqual(prepared["source_progress"], "进行中")

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

    def test_change_end_uses_active_item_target_when_frontend_record_is_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record("c-e", building="E楼", progress="进行中", title="E楼进行中变更")
            ]
            service._upsert_work_status_item_locked(
                {
                    "source_record_id": "c-e",
                    "work_type": "change",
                    "notice_type": "变更通告",
                    "target_record_id": "target-change-e",
                    "active_item_id": "active-change-e",
                    "title": "E楼进行中变更",
                    "building": "E楼",
                    "building_code": "E",
                    "building_codes": ["E"],
                    "specialty": "网络",
                    "started_at": "2026-05-08 09:00",
                },
                action="start",
                now="2026-05-08 09:00",
            )

            job_id, should_start = service.create_action_job(
                {
                    "action": "end",
                    "work_type": "change",
                    "scope": "E",
                    "record_id": "c-e",
                    "source_record_id": "c-e",
                    "active_item_id": "active-change-e",
                    "title": "E楼进行中变更",
                    "building": "E楼",
                    "building_codes": ["E"],
                    "start_time": "2026-05-08T09:00",
                    "end_time": "2026-05-08T18:00",
                    "progress": "已完成",
                    "extra_images": [{"file_token": "site-photo-token"}],
                    "operation_id": "change-end-source-record",
                }
            )

            self.assertTrue(should_start)
            prepared = service.prepare_action_job(job_id)
            self.assertEqual(prepared["action"], "end")
            self.assertEqual(prepared["record_id"], "c-e")
            self.assertEqual(prepared["target_record_id"], "target-change-e")

    def test_successful_end_removes_qt_active_item(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record("c-e", building="E楼", progress="进行中", title="E楼进行中变更")
            ]
            service._state_store.upsert_qt_active_item(
                {
                    "active_item_id": "active-change-e",
                    "record_id": "target-change-e",
                    "target_record_id": "target-change-e",
                    "source_record_id": "c-e",
                    "notice_type": "变更通告",
                    "work_type": "change",
                    "title": "E楼进行中变更",
                    "text": "【变更通告】状态：开始\n【名称】E楼进行中变更",
                    "building": "E楼",
                    "building_codes": ["E"],
                },
                origin="portal",
            )
            self.assertEqual(len(service._state_store.list_qt_active_items()), 1)

            job_id, should_start = service.create_action_job(
                {
                    "action": "end",
                    "work_type": "change",
                    "scope": "E",
                    "record_id": "c-e",
                    "source_record_id": "c-e",
                    "target_record_id": "target-change-e",
                    "active_item_id": "active-change-e",
                    "title": "E楼进行中变更",
                    "building": "E楼",
                    "building_codes": ["E"],
                    "start_time": "2026-05-08T09:00",
                    "end_time": "2026-05-08T18:00",
                    "progress": "已完成",
                    "extra_images": [{"file_token": "site-photo-token"}],
                    "operation_id": "change-end-removes-active",
                }
            )
            self.assertTrue(should_start)
            service.prepare_action_job(job_id)
            service.mark_action_upload_result(
                job_id,
                success=True,
                record_id="target-change-e",
                active_item_id="active-change-e",
            )

            self.assertEqual(service._state_store.list_qt_active_items(), [])
            deleted = service._state_store.list_qt_active_items(include_deleted=True)
            self.assertEqual(len(deleted), 1)
            self.assertIsNotNone(deleted[0].get("deleted_at"))

    def test_backend_end_upload_creates_undo_checkpoint_before_update(self):
        original_service = PortalRuntime.service
        original_state_store = PortalRuntime.state_store
        try:
            with tempfile.TemporaryDirectory() as tmp:
                service = self._new_temp_service(Path(tmp))
                PortalRuntime.service = service
                PortalRuntime.state_store = service._state_store
                service._state_store.upsert_qt_active_item(
                    {
                        "active_item_id": "active-end-undo",
                        "record_id": "target-end-undo",
                        "target_record_id": "target-end-undo",
                        "source_record_id": "source-end-undo",
                        "notice_type": "变更通告",
                        "work_type": "change",
                        "title": "A楼结束回退测试",
                        "building": "A楼",
                        "building_codes": ["A"],
                        "text": "【变更通告】状态：开始\n【名称】A楼结束回退测试",
                    },
                    section="other",
                    origin="web",
                )
                prepared = {
                    "job_id": "job-end-undo",
                    "action": "end",
                    "work_type": "change",
                    "notice_type": "变更通告",
                    "scope": "A",
                    "title": "A楼结束回退测试",
                    "source_record_id": "source-end-undo",
                    "target_record_id": "target-end-undo",
                    "active_item_id": "active-end-undo",
                    "start_time": "2026-05-27 09:00",
                    "end_time": "2026-05-27 18:00",
                    "location": "A楼",
                    "progress": "工作已完成",
                    "text": "【变更通告】状态：结束\n【名称】A楼结束回退测试",
                    "extra_images": [{"file_token": "site-photo-token"}],
                }
                with patch(
                    "lan_bitable_template_portal.server.external_real_write_guard",
                    return_value={"mock_external": False, "real_write_allowed": True, "reason": ""},
                ), patch(
                    "lan_bitable_template_portal.server.query_record_by_id",
                    return_value=(True, {"fields": {"名称": "A楼结束回退测试", "变更状态": "进行中"}}),
                ) as query_record, patch(
                    "lan_bitable_template_portal.server.update_bitable_record_by_payload",
                    return_value=(True, "更新成功"),
                ) as update_record:
                    ok, message, record_id = PortalRuntime._execute_backend_prepared_upload(prepared)

                self.assertTrue(ok)
                self.assertEqual(message, "更新成功")
                self.assertEqual(record_id, "target-end-undo")
                query_record.assert_called_once_with("target-end-undo", "变更通告")
                update_record.assert_called_once()
                undos = service.list_available_notice_undos(scope="A", action_type="end")
                self.assertEqual(len(undos), 1)
                stored_undo = service._state_store.get_notice_undo_action(undos[0]["undo_id"])
                self.assertEqual(stored_undo["target_record_id"], "target-end-undo")
                self.assertEqual(stored_undo["remote"]["fields"]["变更状态"], "进行中")
        finally:
            try:
                service._state_store.shutdown_write_worker(timeout=2.0)
            except Exception:
                pass
            PortalRuntime.service = original_service
            PortalRuntime.state_store = original_state_store

    def test_change_target_candidates_match_title_without_required_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            target_records = [
                {
                    "record_id": "target-a",
                    "display_fields": {
                        "名称": "A楼测试变更",
                        "楼栋": "A楼",
                        "变更状态": "结束",
                        "变更开始时间": "2026-05-08 09:00",
                        "变更结束时间": "2026-05-08 18:00",
                        "实际结束时间": "2026-05-08 18:10",
                        "原因": "测试原因",
                    },
                }
            ]
            with patch.object(
                service,
                "_target_records_for_notice_type",
                return_value=target_records,
            ):
                result = service.lookup_change_target_candidates(
                    scope="A",
                    title="A楼测试变更",
                    action="update",
                )

            self.assertEqual(result["count"], 1)
            candidate = result["candidates"][0]
            self.assertEqual(candidate["record_id"], "target-a")
            self.assertFalse(candidate["date_matched"])
            self.assertEqual(candidate["fields"]["实际结束时间"], "2026-05-08 18:10")
            self.assertIn({"label": "原因", "value": "测试原因"}, candidate["field_items"])

            service._change_records = [
                _build_change_record(
                    "source-a",
                    building="A楼",
                    progress="进行中",
                    title="A楼测试变更",
                    start_time="2026-05-08 09:00",
                    end_time="2026-05-08 18:00",
                )
            ]
            with patch.object(
                service,
                "_target_records_for_notice_type",
                return_value=target_records,
            ):
                result = service.lookup_change_target_candidates(
                    scope="A",
                    title="A楼测试变更",
                    action="update",
                )

            self.assertEqual(result["source_candidates"][0]["source_record_id"], "source-a")
            self.assertEqual(
                result["source_candidates"][0]["source_app_token"],
                "JhiVwgfoIimAqEk8YwEc09sknGd",
            )

    def test_update_action_job_allows_target_record_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            job_id, should_start = service.create_action_job(
                {
                    "action": "update",
                    "work_type": "change",
                    "notice_type": "变更通告",
                    "scope": "A",
                    "manual": True,
                    "manual_id": "manual-change-update",
                    "record_id": "target-change-only",
                    "target_record_id": "target-change-only",
                    "title": "A楼测试变更",
                    "building": "A楼",
                    "building_codes": ["A"],
                    "start_time": "2026-05-08T09:00",
                    "end_time": "2026-05-08T18:00",
                    "operation_id": "target-only-update",
                }
            )

            self.assertTrue(job_id)
            self.assertTrue(should_start)

    def test_notice_target_candidates_support_maintenance_lookup(self):
        service = _TestMaintenancePortalService()
        service._records = [
            _build_record(
                "maint-source",
                "A楼",
                "过滤网维护",
                "5月",
                status="进行中",
                maintenance_cycle="每月",
            )
        ]
        target_records = [
            {
                "record_id": "maint-target",
                "display_fields": {
                    "名称": "EA118机房A楼过滤网维护",
                    "楼栋": "A楼",
                    "维保状态": "开始",
                    "计划开始时间": "2026-05-08 09:00",
                    "计划结束时间": "2026-05-08 18:00",
                    "维保周期": "每月",
                    "专业": "暖通",
                    "位置": "A-127冷站",
                    "内容": "清洗过滤网",
                    "原因": "月度维护",
                    "影响": "无影响",
                    "进度": "准备工作已完成",
                },
            }
        ]
        with patch.object(
            service,
            "_target_records_for_notice_type",
            return_value=target_records,
        ):
            result = service.lookup_notice_target_candidates(
                work_type="maintenance",
                scope="A",
                title="EA118机房A楼过滤网维护",
                action="update",
            )
        self.assertEqual(result["candidates"][0]["target_record_id"], "maint-target")
        self.assertEqual(result["candidates"][0]["notice_type"], "维保通告")
        self.assertEqual(
            result["candidates"][0]["form_fields"],
            {
                "title": "EA118机房A楼过滤网维护",
                "building": "A楼",
                "specialty": "暖通",
                "maintenance_cycle": "每月",
                "location": "A-127冷站",
                "content": "清洗过滤网",
                "reason": "月度维护",
                "impact": "无影响",
                "progress": "准备工作已完成",
                "repair_device": "",
                "repair_fault": "",
                "fault_type": "",
                "repair_mode": "",
                "discovery": "",
                "symptom": "",
                "solution": "",
                "spare_parts": "",
                "device": "",
                "cabinet": "",
                "quantity": "",
                "start_time": "2026-05-08T09:00",
                "end_time": "2026-05-08T18:00",
            },
        )
        self.assertEqual(result["source_candidates"][0]["source_record_id"], "maint-source")
        self.assertEqual(result["source_candidates"][0]["source_app_token"], service.app_token)

    def test_target_record_form_fields_cover_repair_editable_fields(self):
        service = _TestMaintenancePortalService()
        mapped = service._target_record_form_fields(
            work_type="repair",
            notice_type="设备检修",
            target_record={
                "display_fields": {
                    "名称（标题）": "A楼UPS检修",
                    "楼栋": "A楼",
                    "专业": "电气",
                    "位置": "A-245配电室",
                    "紧急程度": "低",
                    "发生故障时间": "2026-07-22 08:15",
                    "期望完成时间": "2026-07-22 18:45",
                    "维修设备": "A-245-UPS-01",
                    "维修故障": "通讯中断",
                    "故障类型": "设备故障",
                    "维修方式": "自维",
                    "影响范围": "无业务影响",
                    "故障发现方式（来源）": "BMS系统",
                    "故障现象": "通讯中断告警",
                    "故障原因": "串口异常",
                    "解决方案": "更换串口",
                    "进度（完成情况）": "处理中",
                }
            },
        )

        self.assertEqual(mapped["title"], "A楼UPS检修")
        self.assertEqual(mapped["level"], "低")
        self.assertEqual(mapped["start_time"], "2026-07-22T18:45")
        self.assertEqual(mapped["end_time"], "2026-07-22T08:15")
        self.assertEqual(mapped["repair_device"], "A-245-UPS-01")
        self.assertEqual(mapped["reason"], "串口异常")
        self.assertEqual(mapped["symptom"], "通讯中断告警")
        self.assertEqual(mapped["progress"], "处理中")

    def test_notice_target_candidates_support_repair_source_lookup(self):
        service = _TestMaintenancePortalService()
        service._repair_records = [
            _build_repair_record(
                "repair-source",
                title="A楼UPS检修通告",
                building="A楼",
                started=True,
            )
        ]
        with patch.object(
            service,
            "_target_records_for_notice_type",
            return_value=[],
        ):
            result = service.lookup_notice_target_candidates(
                work_type="repair",
                scope="A",
                title="A楼UPS检修通告",
                action="update",
            )
        self.assertFalse(result["candidates"])
        self.assertEqual(result["source_candidates"][0]["source_record_id"], "repair-source")
        self.assertEqual(
            result["source_candidates"][0]["source_app_token"],
            "AnEBwJlvGiJfDdkOB32cUPuknzg",
        )

    def test_notice_target_candidates_support_event_lookup(self):
        service = _TestMaintenancePortalService()
        target_records = [
            {
                "record_id": "event-target",
                "display_fields": {
                    "告警描述": "BMS报A楼冷机高压告警",
                    "机楼": "A楼",
                    "事件等级": "I2",
                    "事件发现来源": "BMS",
                    "事件发生时间": "2026-06-24 10:00",
                    "事件结束时间": "",
                },
            }
        ]
        called_notice_types: list[str] = []

        def fake_target_records(notice_type: str, *_args, **_kwargs):
            called_notice_types.append(notice_type)
            return target_records

        with patch.object(
            service,
            "_target_records_for_notice_type",
            side_effect=fake_target_records,
        ):
            result = service.lookup_notice_target_candidates(
                work_type="event",
                scope="A",
                title="BMS报A楼冷机高压告警",
                start_time="2026-06-24 10:00",
                action="update",
            )

        self.assertEqual(called_notice_types, ["事件通告"])
        self.assertEqual(result["candidates"][0]["target_record_id"], "event-target")
        self.assertEqual(result["candidates"][0]["notice_type"], "事件通告")
        self.assertEqual(result["candidates"][0]["status"], "处理中")
        self.assertEqual(result["candidates"][0]["start_time"], "2026-06-24 10:00")

    def test_simple_manual_power_action_prepares_upload_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            payload = {
                "action": "start",
                "work_type": WORK_TYPE_POWER,
                "notice_type": "上电通告",
                "scope": "A",
                "manual": True,
                "manual_id": "manual-power-start",
                "record_id": "manual-power-start",
                "title": "A楼PDU上电",
                "building": "A楼",
                "building_codes": ["A"],
                "specialty": "电气",
                "start_time": "2026-05-08T09:00",
                "end_time": "2026-05-08T18:00",
                "cabinet": "A-101",
                "quantity": "2",
                "progress": "准备上电",
                "operation_id": "manual-power-start",
            }
            job_id, should_start = service.create_action_job(payload)
            prepared = service.prepare_workbench_action(
                {
                    **payload,
                    "operation_id": "manual-power-prepare",
                },
                job_id=job_id,
            )

            self.assertTrue(should_start)
            self.assertEqual(prepared.get("work_type"), WORK_TYPE_POWER)
            self.assertEqual(prepared.get("notice_type"), "上电通告")
            self.assertFalse(prepared.get("skip_personal_message"))
            self.assertGreaterEqual(len(prepared.get("recipients") or []), 2)
            self.assertIn("【上电通告】状态：开始", prepared.get("text") or "")
            self.assertIn("【时间】2026-05-08 09:00~2026-05-08 18:00", prepared.get("text") or "")
            self.assertIn("【柜号】A-101", prepared.get("text") or "")
            self.assertIn("【数量】2", prepared.get("text") or "")
            self.assertNotIn("【楼栋】", prepared.get("text") or "")
            self.assertNotIn("【专业】", prepared.get("text") or "")

    def test_simple_manual_power_down_action_keeps_down_notice_heading(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            payload = {
                "action": "start",
                "work_type": WORK_TYPE_POWER,
                "notice_type": "下电通告",
                "scope": "A",
                "manual": True,
                "manual_id": "manual-power-down-start",
                "record_id": "manual-power-down-start",
                "title": "A楼PDU下电",
                "building": "A楼",
                "building_codes": ["A"],
                "specialty": "电气",
                "start_time": "2026-05-08T09:00",
                "end_time": "2026-05-08T18:00",
                "cabinet": "A-101",
                "quantity": "2",
                "progress": "准备下电",
                "operation_id": "manual-power-down-start",
            }

            prepared = service.prepare_workbench_action(payload, job_id="job-power-down")

            self.assertEqual(prepared.get("work_type"), WORK_TYPE_POWER)
            self.assertEqual(prepared.get("notice_type"), "下电通告")
            self.assertIn("【下电通告】状态：开始", prepared.get("text") or "")
            self.assertIn("【名称】A楼PDU下电", prepared.get("text") or "")
            self.assertNotIn("【上电通告】", prepared.get("text") or "")

    def test_power_notice_aliases_share_table_and_field_config(self):
        cfg = ConfigManager()
        cfg.table_id_power = "tbl-power-test"
        for notice_type in ("上下电通告", "上电通告", "下电通告"):
            with self.subTest(notice_type=notice_type):
                self.assertEqual(cfg.get_table_id(notice_type), "tbl-power-test")
                self.assertIs(get_field_config(notice_type), POWER_NOTICE_FIELDS)

    def test_simple_manual_polling_and_adjust_prepare_complete_text(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            polling = service.prepare_workbench_action(
                {
                    "action": "start",
                    "work_type": "polling",
                    "scope": "B",
                    "manual": True,
                    "manual_id": "manual-polling-start",
                    "record_id": "manual-polling-start",
                    "title": "B楼冷源设备轮巡",
                    "building": "B楼",
                    "building_codes": ["B"],
                    "specialty": "暖通",
                    "start_time": "2026-05-08T09:00",
                    "end_time": "2026-05-08T18:00",
                    "device": "B-CH-01",
                    "content": "检查冷机运行参数",
                    "progress": "轮巡准备中",
                },
                job_id="job-polling",
            )
            self.assertEqual(polling.get("notice_type"), "设备轮巡")
            self.assertFalse(polling.get("skip_personal_message"))
            self.assertGreaterEqual(len(polling.get("recipients") or []), 2)
            self.assertIn("【设备轮巡】状态：开始", polling.get("text") or "")
            self.assertIn("【时间】2026-05-08 09:00~2026-05-08 18:00", polling.get("text") or "")
            self.assertIn("【设备】B-CH-01", polling.get("text") or "")
            self.assertIn("【内容】检查冷机运行参数", polling.get("text") or "")
            self.assertIn("【影响】", polling.get("text") or "")
            self.assertNotIn("【楼栋】", polling.get("text") or "")
            self.assertNotIn("【专业】", polling.get("text") or "")

            adjust = service.prepare_workbench_action(
                {
                    "action": "start",
                    "work_type": "adjust",
                    "scope": "C",
                    "manual": True,
                    "manual_id": "manual-adjust-start",
                    "record_id": "manual-adjust-start",
                    "title": "C楼配电设备调整",
                    "building": "C楼",
                    "building_codes": ["C"],
                    "specialty": "电气",
                    "start_time": "2026-05-08T09:00",
                    "end_time": "2026-05-08T18:00",
                    "location": "C楼配电室",
                    "content": "调整开关柜回路",
                    "reason": "负载优化",
                    "impact": "无业务影响",
                    "progress": "准备调整",
                },
                job_id="job-adjust",
            )
            self.assertEqual(adjust.get("notice_type"), "设备调整")
            self.assertFalse(adjust.get("skip_personal_message"))
            self.assertGreaterEqual(len(adjust.get("recipients") or []), 2)
            self.assertIn("【设备调整】状态：开始", adjust.get("text") or "")
            self.assertIn("【时间】2026-05-08 09:00~2026-05-08 18:00", adjust.get("text") or "")
            self.assertIn("【位置】C楼配电室", adjust.get("text") or "")
            self.assertIn("【原因】负载优化", adjust.get("text") or "")
            self.assertIn("【影响】无业务影响", adjust.get("text") or "")
            self.assertNotIn("【楼栋】", adjust.get("text") or "")
            self.assertNotIn("【专业】", adjust.get("text") or "")

    def test_manual_repair_text_includes_spare_parts_but_skips_missing_target_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            prepared = service.prepare_workbench_action(
                {
                    "action": "start",
                    "work_type": "repair",
                    "scope": "B",
                    "manual": True,
                    "manual_id": "manual-repair-spare",
                    "record_id": "manual-repair-spare",
                    "title": "B楼恒湿机检修",
                    "building": "B楼",
                    "building_codes": ["B"],
                    "specialty": "暖通",
                    "level": "低",
                    "start_time": "2026-05-08T23:50",
                    "end_time": "2026-05-08T08:20",
                    "location": "B-418",
                    "repair_device": "B-418-HUM-01",
                    "repair_fault": "循环泵",
                    "fault_type": "设备故障",
                    "repair_mode": "自维",
                    "impact": "无业务影响",
                    "discovery": "巡检发现",
                    "symptom": "加湿异常",
                    "reason": "循环泵故障",
                    "solution": "更换循环泵",
                    "spare_parts": "已更换串口模块",
                    "progress": "人员已就位",
                },
                job_id="job-repair-spare",
            )
            self.assertIn("【设备检修】状态：开始", prepared.get("text") or "")
            self.assertIn("【备件更换情况】已更换串口模块", prepared.get("text") or "")

            handler = OverhaulNoticeHandler("设备检修")
            fields = handler.build_create_fields(NoticePayload(text=prepared["text"]))
            self.assertNotIn("spare_parts", config_module.OVERHAUL_NOTICE_FIELDS)
            self.assertNotIn("备件更换情况", fields)

    def test_repair_update_uses_latest_web_form_fault_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            prepared = service.prepare_workbench_action(
                {
                    "action": "update",
                    "work_type": "repair",
                    "scope": "B",
                    "active_item_id": "active-repair-update",
                    "target_record_id": "rec-target-repair-update",
                    "record_id": "rec-target-repair-update",
                    "title": "B楼恒湿机检修",
                    "building": "B楼",
                    "building_codes": ["B"],
                    "specialty": "暖通",
                    "level": "低",
                    "start_time": "2026-05-08T23:50",
                    "end_time": "2026-05-08T10:30",
                    "expected_time": "2026-05-08 23:00",
                    "fault_time": "2026-05-08 08:20",
                    "location": "B-418",
                    "repair_device": "B-418-HUM-01",
                    "repair_fault": "循环泵",
                    "fault_type": "设备故障",
                    "repair_mode": "自维",
                    "impact": "无业务影响",
                    "discovery": "巡检发现",
                    "symptom": "加湿异常",
                    "reason": "循环泵故障",
                    "solution": "更换循环泵",
                    "spare_parts": "无",
                    "progress": "更新处理中",
                },
                job_id="job-repair-update-time",
            )

            text = prepared.get("text") or ""
            self.assertEqual(prepared.get("fault_time"), "2026-05-08 10:30")
            self.assertEqual(prepared.get("expected_time"), "2026-05-08 23:50")
            self.assertIn("【发现故障时间】2026-05-08 10:30", text)
            self.assertIn("【期望完成时间】2026-05-08 23:50", text)
            self.assertNotIn("【发现故障时间】2026-05-08 08:20", text)

    def test_workbench_lite_dom_snapshot_covers_notice_template_fields(self):
        expected_fields = {
            field
            for fields in workbench_lite_module.REQUIRED_UPLOAD_FIELDS_BY_WORK_TYPE.values()
            for field in fields
        }
        for template in NOTICE_TEXT_TEMPLATES.values():
            for _, key in template["fields"]:
                if key == "time_range":
                    expected_fields.update({"start_time", "end_time"})
                elif key == "fault_time":
                    expected_fields.update({"fault_time", "end_time"})
                elif key == "expected_time":
                    expected_fields.update({"expected_time", "start_time"})
                else:
                    expected_fields.add(key)

        missing = expected_fields - set(workbench_lite_module._DRAFT_DOM_KEYS)
        self.assertFalse(missing, f"字段未进入轻量页面草稿快照: {sorted(missing)}")

    def test_manual_maintenance_payload_with_adjust_title_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            payload = {
                "action": "start",
                "work_type": "maintenance",
                "scope": "C",
                "manual": True,
                "manual_id": "manual-adjust-from-maintenance",
                "record_id": "manual-adjust-from-maintenance",
                "title": "EA118机房C楼制冷单元运行模式调整通告（非计划性）",
                "building": "C楼",
                "building_codes": ["C"],
                "specialty": "暖通",
                "start_time": "2026-06-12T09:15",
                "end_time": "2026-06-12T18:30",
                "location": "C-127冷站",
                "content": "C楼制冷单元由预冷模式切换为制冷模式",
                "reason": "室外湿球温度升高，预冷模式不满足制冷需求",
                "impact": "对机房运行无影响，设备调整会引起BMS和BA告警",
                "progress": "准备工作已完成，人员已就位，是否可以开始操作？",
            }

            target_key = MaintenancePortalService._action_target_key(payload)
            self.assertTrue(target_key.startswith("maintenance:manual-start:"))
            self.assertNotIn("manual-adjust-from-maintenance", target_key)
            with self.assertRaisesRegex(PortalError, "像是调整通告"):
                service.create_action_job(payload)

    def test_simple_manual_ongoing_update_accepts_target_record_without_manual_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            cases = [
                (
                    "power",
                    "update",
                    "A",
                    "A楼PDU上电",
                    "A楼",
                    ["A"],
                    "上电通告",
                    "上电通告",
                    {"cabinet": "A-101", "quantity": "2", "progress": "上电更新中"},
                ),
                (
                    "polling",
                    "update",
                    "B",
                    "B楼冷源设备轮巡",
                    "B楼",
                    ["B"],
                    "设备轮巡",
                    "设备轮巡",
                    {"device": "B-CH-01", "content": "检查冷机运行参数", "progress": "轮巡更新中"},
                ),
                (
                    "adjust",
                    "end",
                    "C",
                    "C楼配电设备调整",
                    "C楼",
                    ["C"],
                    "设备调整",
                    "设备调整",
                    {"location": "C楼配电室", "reason": "负载优化", "progress": "调整完成"},
                ),
            ]
            for work_type, action, scope, title, building, building_codes, notice_type, heading, extra in cases:
                with self.subTest(work_type=work_type, action=action):
                    target_record_id = f"rec-target-{work_type}-1"
                    prepared = service.prepare_workbench_action(
                        {
                            "action": action,
                            "work_type": work_type,
                            "scope": scope,
                            "active_item_id": f"active-{work_type}-1",
                            "target_record_id": target_record_id,
                            "title": title,
                            "building": building,
                            "building_codes": building_codes,
                            "specialty": "电气" if work_type != "polling" else "暖通",
                            "start_time": "2026-05-08T09:00",
                            "end_time": "2026-05-08T18:00",
                            **extra,
                            **(
                                {"extra_images": [{"file_token": "site-photo-token"}]}
                                if action == "end"
                                else {}
                            ),
                        },
                        job_id=f"job-{work_type}-{action}",
                    )

                    self.assertTrue(prepared.get("manual"))
                    self.assertEqual(prepared.get("action"), action)
                    self.assertEqual(prepared.get("status"), "结束" if action == "end" else "更新")
                    self.assertEqual(prepared.get("notice_type"), notice_type)
                    self.assertEqual(prepared.get("target_record_id"), target_record_id)
                    self.assertFalse(prepared.get("skip_personal_message"))
                    self.assertGreaterEqual(len(prepared.get("recipients") or []), 2)
                    self.assertIn(f"【{heading}】状态：{prepared.get('status')}", prepared.get("text") or "")

            with self.assertRaises(PortalError):
                service.prepare_workbench_action(
                    {
                        "action": "update",
                        "work_type": "polling",
                        "scope": "B",
                        "title": "B楼冷源设备轮巡",
                        "building": "B楼",
                        "building_codes": ["B"],
                        "start_time": "2026-05-08T09:00",
                        "end_time": "2026-05-08T18:00",
                    },
                    job_id="job-polling-update-missing-target",
                )

    def test_polling_and_adjust_handlers_map_frontend_fields_to_bitable_fields(self):
        power_text = MaintenancePortalService.build_simple_notice_text(
            work_type="power",
            status="开始",
            title="A楼PDU上电",
            start_time="2026-05-08 09:00",
            end_time="2026-05-08 18:00",
            building="A楼",
            specialty="电气",
            cabinet="A-101",
            quantity="2",
            progress="准备上电",
        )
        power_fields = PowerNoticeHandler().build_create_fields(
            NoticePayload(
                text=power_text,
                buildings=["A楼"],
                specialty="电气",
                response_time="2026-05-08 09:05",
            )
        )
        self.assertEqual(power_fields[POWER_NOTICE_FIELDS["title"]], "A楼PDU上电")
        self.assertEqual(power_fields[POWER_NOTICE_FIELDS["building"]], ["A楼"])
        self.assertEqual(power_fields[POWER_NOTICE_FIELDS["status"]], STATUS_START)
        self.assertEqual(power_fields[POWER_NOTICE_FIELDS["specialty"]], "电气")
        self.assertEqual(power_fields[POWER_NOTICE_FIELDS["cabinet"]], "A-101")
        self.assertEqual(power_fields[POWER_NOTICE_FIELDS["quantity"]], 2)
        self.assertEqual(power_fields[POWER_NOTICE_FIELDS["progress"]], "准备上电")
        self.assertIn(POWER_NOTICE_FIELDS["plan_start"], power_fields)
        self.assertIn(POWER_NOTICE_FIELDS["plan_end"], power_fields)

        polling_text = MaintenancePortalService.build_simple_notice_text(
            work_type="polling",
            status="开始",
            title="B楼冷源设备轮巡",
            start_time="2026-05-08 09:00",
            end_time="2026-05-08 18:00",
            building="B楼",
            specialty="暖通",
            device="B-CH-01",
            content="检查冷机运行参数",
            progress="轮巡准备中",
        )
        polling_fields = PollingNoticeHandler().build_create_fields(
            NoticePayload(
                text=polling_text,
                buildings=["B楼"],
                specialty="暖通",
                response_time="2026-05-08 09:05",
            )
        )
        self.assertEqual(polling_fields[POLLING_NOTICE_FIELDS["title"]], "B楼冷源设备轮巡")
        self.assertEqual(polling_fields[POLLING_NOTICE_FIELDS["building"]], ["B楼"])
        self.assertEqual(polling_fields[POLLING_NOTICE_FIELDS["status"]], STATUS_START)
        self.assertEqual(polling_fields[POLLING_NOTICE_FIELDS["specialty"]], "暖通")
        self.assertEqual(polling_fields[POLLING_NOTICE_FIELDS["device"]], "B-CH-01")
        self.assertEqual(polling_fields[POLLING_NOTICE_FIELDS["content"]], "检查冷机运行参数")
        self.assertEqual(polling_fields[POLLING_NOTICE_FIELDS["progress"]], "轮巡准备中")
        self.assertIn(POLLING_NOTICE_FIELDS["plan_start"], polling_fields)
        self.assertIn(POLLING_NOTICE_FIELDS["plan_end"], polling_fields)

        adjust_text = MaintenancePortalService.build_simple_notice_text(
            work_type="adjust",
            status="开始",
            title="C楼配电设备调整",
            start_time="2026-05-08 09:00",
            end_time="2026-05-08 18:00",
            building="C楼",
            specialty="电气",
            location="C楼配电室",
            content="调整开关柜回路",
            reason="负载优化",
            impact="无业务影响",
            progress="准备调整",
        )
        adjust_fields = AdjustNoticeHandler().build_create_fields(
            NoticePayload(
                text=adjust_text,
                buildings=["C楼"],
                specialty="电气",
                response_time="2026-05-08 09:05",
            )
        )
        self.assertEqual(adjust_fields[ADJUST_NOTICE_FIELDS["title"]], "C楼配电设备调整")
        self.assertEqual(adjust_fields[ADJUST_NOTICE_FIELDS["building"]], ["C楼"])
        self.assertEqual(adjust_fields[ADJUST_NOTICE_FIELDS["status"]], STATUS_START)
        self.assertEqual(adjust_fields[ADJUST_NOTICE_FIELDS["specialty"]], "电气")
        self.assertEqual(adjust_fields[ADJUST_NOTICE_FIELDS["location"]], "C楼配电室")
        self.assertEqual(adjust_fields[ADJUST_NOTICE_FIELDS["content"]], "调整开关柜回路")
        self.assertEqual(adjust_fields[ADJUST_NOTICE_FIELDS["reason"]], "负载优化")
        self.assertEqual(adjust_fields[ADJUST_NOTICE_FIELDS["impact"]], "无业务影响")
        self.assertEqual(adjust_fields[ADJUST_NOTICE_FIELDS["progress"]], "准备调整")
        self.assertIn(ADJUST_NOTICE_FIELDS["plan_start"], adjust_fields)
        self.assertIn(ADJUST_NOTICE_FIELDS["plan_end"], adjust_fields)

    def test_history_memory_scan_loads_current_month_ended_maintenance_and_change(self):
        service = _TestMaintenancePortalService()
        service._load_fields = lambda: []
        service._load_change_fields = lambda: []
        current_month = service._current_month_label()
        current_start = service._recent_month_starts()[0]
        change_start = current_start.replace(day=min(28, current_start.day + 1), hour=9, minute=0)
        change_end = change_start.replace(hour=18, minute=0)

        def fake_load_table_records(**kwargs):
            if kwargs.get("work_type") == WORK_TYPE_CHANGE:
                return [
                    _build_change_record(
                        "change-ended",
                        building="A楼",
                        progress="已结束",
                        title="A楼测试变更",
                        start_time=change_start.strftime("%Y-%m-%d %H:%M"),
                        end_time=change_end.strftime("%Y-%m-%d %H:%M"),
                    )
                ]
            return [
                _build_record(
                    "maint-ended",
                    "A楼",
                    "过滤网维护",
                    current_month,
                    status="已结束",
                    maintenance_cycle="每月",
                )
            ]

        service._load_table_records = fake_load_table_records
        service._scan_target_history_candidates = lambda **_kwargs: ([], [])
        result = service.scan_historical_notice_memory_candidates(
            work_types=["maintenance", "change"],
            months=3,
        )
        source_ids = {item.get("source_record_id") for item in result["source_items"]}
        self.assertIn("maint-ended", source_ids)
        self.assertIn("change-ended", source_ids)
        self.assertGreaterEqual(result["counts"]["source"], 2)
        self.assertTrue(result["source_items_full_current_month"])

    def test_confirm_change_target_candidate_clears_actual_end_fields(self):
        class _ConfirmService:
            def __init__(self):
                self.cache_cleared = False

            def lookup_change_target_candidates(self, **_kwargs):
                return {
                    "candidates": [
                        {
                            "record_id": "target-change",
                            "target_record_id": "target-change",
                            "title": "A楼测试变更",
                            "building": "A楼",
                            "building_codes": ["A"],
                            "fields": {
                                "名称": "A楼测试变更",
                                "实际结束时间": "2026-05-08 18:10",
                                "变更结束时间": "2026-05-08 18:00",
                            },
                        }
                    ]
                }

            def clear_target_record_cache(self):
                self.cache_cleared = True

        previous_service = PortalRuntime.service
        fake_service = _ConfirmService()
        PortalRuntime.service = fake_service
        try:
            with patch(
                "lan_bitable_template_portal.server.external_real_write_guard",
                return_value={"mock_external": False, "real_write_allowed": True},
            ), patch(
                "lan_bitable_template_portal.server.update_bitable_record_fields",
                return_value=(True, "target-change"),
            ) as update_fields:
                result = PortalRuntime.confirm_change_target_candidate(
                    scope="A",
                    title="A楼测试变更",
                    action="update",
                    record_id="target-change",
                )

            self.assertTrue(result["ok"])
            self.assertTrue(result["clear_actual_end"]["cleared"])
            self.assertTrue(fake_service.cache_cleared)
            update_fields.assert_called_once_with(
                "target-change",
                "变更通告",
                {"实际结束时间": None, "变更结束时间": None},
            )
        finally:
            PortalRuntime.service = previous_service

    def test_change_update_rebinds_missing_target_record_from_unique_candidate(self):
        class _RebindService:
            def __init__(self):
                self.lookup_calls = []

            def lookup_notice_target_candidates(self, **kwargs):
                self.lookup_calls.append(kwargs)
                return {
                    "candidates": [
                        {
                            "record_id": "fresh-change-target",
                            "target_record_id": "fresh-change-target",
                            "title": "A楼测试变更",
                            "building_codes": ["A"],
                            "date_matched": True,
                        }
                    ]
                }

        class _StateStore:
            def __init__(self):
                self.identity_payloads = []

            def upsert_notice_identity(self, payload, *, origin=""):
                self.identity_payloads.append((dict(payload), origin))
                return dict(payload)

        previous_service = PortalRuntime.service
        previous_state_store = PortalRuntime.state_store
        fake_service = _RebindService()
        fake_state = _StateStore()
        PortalRuntime.service = fake_service
        PortalRuntime.state_store = fake_state
        prepared = {
            "job_id": "job-rebind",
            "action": "update",
            "work_type": "change",
            "notice_type": "变更通告",
            "scope": "A",
            "title": "A楼测试变更",
            "source_record_id": "source-change",
            "target_record_id": "stale-change-target",
            "active_item_id": "active-change",
            "start_time": "2026-05-27 10:40",
            "end_time": "2026-05-27 18:00",
            "text": "【变更通告】状态：更新\n【名称】A楼测试变更",
        }
        try:
            with patch(
                "lan_bitable_template_portal.server.external_real_write_guard",
                return_value={"mock_external": False, "real_write_allowed": True, "reason": ""},
            ), patch(
                "lan_bitable_template_portal.server.query_record_by_id",
                side_effect=[
                    (False, "查询记录失败：1254043-RecordidNotFound"),
                    (True, {"fields": {"名称": "A楼测试变更"}}),
                ],
            ) as query_record, patch(
                "lan_bitable_template_portal.server.update_bitable_record_by_payload",
                return_value=(True, "更新成功"),
            ) as update_record:
                ok, message, record_id = PortalRuntime._execute_backend_prepared_upload(prepared)

            self.assertTrue(ok)
            self.assertEqual(message, "更新成功")
            self.assertEqual(record_id, "fresh-change-target")
            self.assertEqual(query_record.call_args_list[0].args[0], "stale-change-target")
            self.assertEqual(query_record.call_args_list[1].args[0], "fresh-change-target")
            update_record.assert_called_once()
            self.assertEqual(update_record.call_args.args[0], "fresh-change-target")
            self.assertEqual(fake_service.lookup_calls[0]["work_type"], "change")
            self.assertEqual(fake_service.lookup_calls[0]["title"], "A楼测试变更")
            self.assertTrue(fake_state.identity_payloads)
            identity_payload, origin = fake_state.identity_payloads[0]
            self.assertEqual(origin, "auto_rebind_target")
            self.assertEqual(identity_payload["target_record_id"], "fresh-change-target")
        finally:
            PortalRuntime.service = previous_service
            PortalRuntime.state_store = previous_state_store

    def test_qt_update_missing_target_returns_selection_before_media_upload(self):
        class _SelectionService:
            def lookup_notice_target_candidates(self, **_kwargs):
                self.last_kwargs = dict(_kwargs)
                return {
                    "candidates": [
                        {
                            "record_id": "fresh-change-target",
                            "target_record_id": "fresh-change-target",
                            "title": "A楼测试变更",
                            "building": "A楼",
                            "building_codes": ["A"],
                            "status": "进行中",
                            "start_time": "2026-05-27 10:40",
                            "end_time": "2026-05-27 18:00",
                            "date_matched": True,
                            "field_items": [{"label": "名称", "value": "A楼测试变更"}],
                        }
                    ]
                }

        previous_service = PortalRuntime.service
        PortalRuntime.service = _SelectionService()
        try:
            with patch(
                "lan_bitable_template_portal.server.query_record_by_id",
                return_value=(False, "查询记录失败：1254043-RecordIdNotFound"),
            ), patch(
                "lan_bitable_template_portal.server.upload_media_to_feishu"
            ) as upload_media:
                result = PortalRuntime.execute_local_notice_upload(
                    {
                        "action_type": "update",
                        "data_dict": {
                            "record_id": "stale-change-target",
                            "target_record_id": "stale-change-target",
                            "source_record_id": "source-change",
                            "active_item_id": "active-change",
                            "work_type": "change",
                            "notice_type": "变更通告",
                            "scope": "A",
                            "title": "A楼测试变更",
                            "building_codes": ["A"],
                            "start_time": "2026-05-27 10:40",
                            "end_time": "2026-05-27 18:00",
                            "text": "【变更通告】状态：更新\n【名称】A楼测试变更",
                            "content": "工程师对A楼设备进行变更",
                        },
                    }
                )

            self.assertFalse(result["ok"])
            self.assertTrue(result["needs_target_selection"])
            self.assertTrue(result["target_record_missing"])
            self.assertEqual(
                result["target_candidates"][0]["target_record_id"],
                "fresh-change-target",
            )
            self.assertEqual(
                PortalRuntime.service.last_kwargs["content"],
                "工程师对A楼设备进行变更",
            )
            upload_media.assert_not_called()
        finally:
            PortalRuntime.service = previous_service

    def test_qt_update_missing_target_uses_notice_text_for_selection_lookup(self):
        class _TextLookupService:
            def lookup_notice_target_candidates(self, **_kwargs):
                self.last_kwargs = dict(_kwargs)
                return {
                    "candidates": [
                        {
                            "record_id": "fresh-by-text",
                            "target_record_id": "fresh-by-text",
                            "title": "A楼真实目标记录",
                            "building": "A楼",
                            "building_codes": ["A"],
                            "status": "进行中",
                            "date_matched": True,
                            "business_match_count": 1,
                        }
                    ]
                }

            @staticmethod
            def _parse_notice_sections(text):
                return MaintenancePortalService._parse_notice_sections(text)

        previous_service = PortalRuntime.service
        fake_service = _TextLookupService()
        PortalRuntime.service = fake_service
        notice_text = (
            "【变更通告】状态：更新\n"
            "【名称】A楼UPS设备变更\n"
            "【时间】2026年5月27日10：40-18：00\n"
            "【内容】工程师对A楼UPS设备进行变更测试\n"
            "【原因】测试测试测试\n"
            "【影响】无影响\n"
            "【进度】准备更新"
        )
        try:
            with patch(
                "lan_bitable_template_portal.server.query_record_by_id",
                return_value=(False, "查询记录失败：1254043-RecordIdNotFound"),
            ):
                result = PortalRuntime.execute_local_notice_upload(
                    {
                        "action_type": "update",
                        "data_dict": {
                            "record_id": "stale-change-target",
                            "target_record_id": "stale-change-target",
                            "work_type": "change",
                            "notice_type": "变更通告",
                            "scope": "A",
                            "text": notice_text,
                        },
                    }
                )

            self.assertFalse(result["ok"])
            self.assertTrue(result["needs_target_selection"])
            self.assertEqual(result["target_candidates"][0]["target_record_id"], "fresh-by-text")
            self.assertEqual(fake_service.last_kwargs["title"], "A楼UPS设备变更")
            self.assertIn("2026年5月27日", fake_service.last_kwargs["start_time"])
            self.assertEqual(
                fake_service.last_kwargs["content"],
                "工程师对A楼UPS设备进行变更测试",
            )
        finally:
            PortalRuntime.service = previous_service

    def test_change_target_lookup_can_match_by_content_when_title_differs(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._target_records_for_notice_type = lambda *_args, **_kwargs: [
                {
                    "record_id": "target-by-content",
                    "display_fields": {
                        "名称": "A楼旧标题",
                        "内容": "工程师对A楼UPS设备进行变更测试",
                        "原因": "测试测试测试",
                        "时间": "2026-05-27 10:40 至 2026-05-27 18:00",
                        "状态": "进行中",
                    },
                }
            ]
            service._target_record_building_codes = lambda _fields, _config: []

            result = service.lookup_change_target_candidates(
                scope="A",
                title="A楼新标题",
                start_time="2026-05-27 10:40",
                end_time="2026-05-27 18:00",
                action="update",
                content="工程师对A楼UPS设备进行变更测试",
                reason="测试测试测试",
            )

            self.assertEqual(result["count"], 1)
            candidate = result["candidates"][0]
            self.assertEqual(candidate["target_record_id"], "target-by-content")
            self.assertFalse(candidate["title_matched"])
            self.assertTrue(candidate["business_text_matched"])
            self.assertGreaterEqual(candidate["business_match_count"], 2)
            self.assertTrue(candidate["date_matched"])

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
            self.assertFalse(prepared["skip_personal_message"])
            self.assertGreaterEqual(len(prepared["recipients"]), 2)
            service.mark_action_upload_result(
                start_job_id,
                success=True,
                record_id="change-target-1",
                active_item_id="active-change-1",
            )

            result = service.query_records(month=_TEST_MONTH_LABEL, scope="CAMPUS")
            summary = result["records"][0]["work_summary"]
            self.assertEqual(summary["work_type"], WORK_TYPE_CHANGE)
            self.assertEqual(summary["notice_type"], "变更通告")
            self.assertEqual(summary["source_record_id"], "c2")
            self.assertEqual(summary["target_record_id"], "change-target-1")
            self.assertEqual(summary["status"], "进行中")

    def test_repair_records_filter_placeholders_and_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record("r1", building="D楼", title="D楼UPS检修"),
                _build_repair_record("r2", building="D楼", title="——"),
                _build_repair_record("r3", building="CMDB唯一ID关联", title="无法识别楼栋"),
            ]

            result = service.query_records(month=_TEST_MONTH_LABEL, scope="D")

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
            self.assertGreaterEqual(len(prepared["recipients"]), 2)
            self.assertFalse(prepared["skip_personal_message"])
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

    def test_repair_start_can_build_text_from_payload_when_snapshot_record_is_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = []
            service._repair_loaded_once = True

            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "work_type": "repair",
                    "scope": "D",
                    "record_id": "missing-repair-source",
                    "source_record_id": "missing-repair-source",
                    "source_progress": "未开始",
                    "building": "D楼",
                    "building_codes": ["D"],
                    "specialty": "电气",
                    "title": "D楼UPS检修",
                    "level": "低",
                    "expected_time": "2026-05-08T23:50",
                    "fault_time": "2026-05-08T08:20",
                    "location": "D-UPS间",
                    "reason": "测试故障原因",
                    "impact": "测试影响",
                    "progress": "准备开始",
                    "repair_device": "UPS-01",
                    "repair_fault": "UPS故障",
                    "fault_type": "设备故障",
                    "repair_mode": "自维",
                    "discovery": "巡检发现",
                    "symptom": "通讯中断",
                    "solution": "更换模块",
                    "operation_id": "repair-start-payload-fallback",
                }
            )

            self.assertTrue(should_start)
            prepared = service.prepare_action_job(start_job_id)
            self.assertEqual(prepared["record_id"], "missing-repair-source")
            self.assertEqual(prepared["source_app_token"], "AnEBwJlvGiJfDdkOB32cUPuknzg")
            self.assertGreaterEqual(len(prepared["recipients"]), 2)
            self.assertFalse(prepared["skip_personal_message"])
            self.assertIn("【设备检修】状态：开始", prepared["text"])
            self.assertIn("【标题】D楼UPS检修", prepared["text"])
            self.assertIn("【地点】D-UPS间", prepared["text"])
            self.assertIn("【维修设备】UPS-01", prepared["text"])
            self.assertIn("【解决方案】更换模块", prepared["text"])

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

    def test_repair_title_supplement_and_raw_option_values_are_cleaned(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            record = _build_repair_record(
                "r-clean",
                building="D楼",
                title="D楼UPS检修",
                source="optABCDEFG",
                specialty="optZYX987",
            )
            record["display_fields"]["标题/补充内容"] = "（补充内容）"
            record["display_fields"]["对应来源"] = "optABCDEFG"
            record["display_fields"]["所属专业"] = ""
            record["display_fields"]["专业（推送消息用）"] = "optZYX987"
            service._repair_records = [record]

            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "work_type": "repair",
                    "scope": "D",
                    "record_id": "r-clean",
                    "specialty": "全部",
                    "start_time": "2026-06-12T23:50",
                    "end_time": "2026-06-12T10:44",
                    "location": "D楼",
                    "reason": "测试故障原因",
                    "impact": "测试影响",
                    "progress": "人员已就位",
                    "operation_id": "repair-clean",
                }
            )

            self.assertTrue(should_start)
            prepared = service.prepare_action_job(start_job_id)
            self.assertEqual(prepared["title"], "D楼UPS检修")
            self.assertEqual(prepared["content"], "（补充内容）")
            self.assertEqual(prepared["specialty"], "")
            self.assertEqual(prepared["discovery"], "")
            self.assertIn("【标题】D楼UPS检修（补充内容）", prepared["text"])
            self.assertIn("【专业】", prepared["text"])
            self.assertIn("【故障发现方式】", prepared["text"])
            self.assertNotIn("optABCDEFG", prepared["text"])
            self.assertNotIn("optZYX987", prepared["text"])

    def test_repair_empty_frontend_content_keeps_source_title_supplement(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            record = _build_repair_record(
                "r-front",
                building="D楼",
                title="D楼UPS检修",
            )
            record["display_fields"]["标题/补充内容"] = "（补充内容）"
            service._repair_records = [record]

            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "work_type": "repair",
                    "scope": "D",
                    "record_id": "r-front",
                    "title": "D楼UPS检修",
                    "content": "",
                    "specialty": "",
                    "start_time": "2026-06-12T23:50",
                    "end_time": "2026-06-12T10:44",
                    "location": "D楼",
                    "reason": "测试故障原因",
                    "impact": "测试影响",
                    "progress": "人员已就位",
                    "operation_id": "repair-front-content",
                }
            )

            self.assertTrue(should_start)
            prepared = service.prepare_action_job(start_job_id)
            self.assertEqual(prepared["title"], "D楼UPS检修")
            self.assertEqual(prepared["content"], "（补充内容）")
            self.assertIn("【标题】D楼UPS检修（补充内容）", prepared["text"])

    def test_repair_legacy_combined_frontend_title_is_split_from_supplement(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            record = _build_repair_record(
                "r-legacy-title",
                building="D楼",
                title="D楼UPS检修",
            )
            record["display_fields"]["标题/补充内容"] = "（补充内容）"
            service._repair_records = [record]

            start_job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "work_type": "repair",
                    "scope": "D",
                    "record_id": "r-legacy-title",
                    "title": "D楼UPS检修（补充内容）",
                    "content": "（补充内容）",
                    "start_time": "2026-06-12T23:50",
                    "end_time": "2026-06-12T10:44",
                    "location": "D楼",
                    "reason": "测试故障原因",
                    "impact": "测试影响",
                    "progress": "人员已就位",
                    "operation_id": "repair-legacy-title",
                }
            )

            self.assertTrue(should_start)
            prepared = service.prepare_action_job(start_job_id)
            self.assertEqual(prepared["title"], "D楼UPS检修")
            self.assertEqual(prepared["content"], "（补充内容）")
            self.assertEqual(prepared["text"].count("（补充内容）"), 1)
            self.assertIn("【标题】D楼UPS检修（补充内容）", prepared["text"])

    def test_repair_frontend_content_is_not_used_as_title_supplement(self):
        service = _TestMaintenancePortalService()
        prepared = service.prepare_repair_action(
            {
                "action": "update",
                "work_type": "repair",
                "scope": "C",
                "record_id": "target-repair-content",
                "target_record_id": "target-repair-content",
                "active_item_id": "active-repair-content",
                "title": "C楼测试测试检修",
                "content": "测试测试普通内容",
                "building": "C楼",
                "building_codes": ["C"],
                "specialty": "电气",
                "level": "低",
                "fault_time": "2026-06-21T19:30",
                "expected_time": "2026-06-21T23:30",
                "location": "C楼",
                "repair_device": "测试设备",
                "repair_fault": "测试故障",
                "fault_type": "设备故障",
                "repair_mode": "自维",
                "discovery": "巡检发现",
                "symptom": "故障现象",
                "reason": "故障原因",
                "solution": "解决方案",
                "progress": "更新进展",
            },
            job_id="job-repair-content-not-title",
        )
        self.assertIn("【标题】C楼测试测试检修", prepared["text"])
        self.assertNotIn("【标题】C楼测试测试检修测试测试普通内容", prepared["text"])

    def test_manual_repair_notice_command_expands_patch_before_queueing(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            job_id, should_start = service.create_action_job(
                {
                    "command_format": "notice_command",
                    "action": "start",
                    "scope": "A",
                    "work_type": "repair",
                    "notice_type": "设备检修",
                    "record_id": "manual:lite",
                    "operation_id": "manual-repair-command-start",
                    "patch": {
                        "action": "start",
                        "scope": "A",
                        "work_type": "repair",
                        "notice_type": "设备检修",
                        "manual": "1",
                        "manual_id": "manual:lite",
                        "record_id": "manual:lite",
                        "title": "A楼测试检修",
                        "building": "A楼",
                        "building_codes": ["A"],
                        "specialty": "电气",
                        "level": "低",
                        "fault_time": "2026-06-12T08:00",
                        "expected_time": "2026-06-12T23:50",
                        "location": "A楼",
                        "repair_device": "测试设备",
                        "repair_fault": "测试故障",
                        "fault_type": "设备故障",
                        "repair_mode": "自维",
                        "discovery": "人工发现",
                        "symptom": "测试现象",
                        "reason": "测试原因",
                        "solution": "测试方案",
                        "spare_parts": "无",
                        "progress": "准备工作已完成",
                    },
                }
            )

            self.assertTrue(should_start)
            job = service.get_job(job_id) or {}
            self.assertNotEqual(job.get("request", {}).get("command_format"), "notice_command")
            self.assertTrue(job.get("request", {}).get("manual"))
            self.assertEqual(job.get("request", {}).get("manual_id"), "manual:lite")
            prepared = service.prepare_action_job(job_id)
            self.assertEqual(prepared["work_type"], WORK_TYPE_REPAIR)
            self.assertEqual(prepared["title"], "A楼测试检修")
            self.assertIn("【设备检修】状态：开始", prepared["text"])
            self.assertIn("【备件更换情况】无", prepared["text"])

    def test_started_repair_source_record_stays_in_source_list_without_active_item(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record(
                    "r1",
                    building="D楼",
                    title="D楼UPS检修",
                    started=True,
                    target_record_id="rec_target_r1",
                )
            ]

            result = service.query_records(month=_TEST_MONTH_LABEL, scope="D")

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
                    target_record_id="rec_target_r1",
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
            self.assertEqual(prepared["target_record_id"], "rec_target_r1")
            self.assertEqual(prepared["source_progress"], "进行中")

    def test_repair_end_uses_active_item_target_when_frontend_record_is_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record(
                    "r1",
                    building="D楼",
                    title="D楼UPS检修",
                    started=True,
                    target_record_id="",
                )
            ]
            service._upsert_work_status_item_locked(
                {
                    "source_record_id": "r1",
                    "work_type": "repair",
                    "notice_type": "设备检修",
                    "target_record_id": "target-r1",
                    "active_item_id": "active-r1",
                    "title": "D楼UPS检修",
                    "building": "D楼",
                    "building_code": "D",
                    "building_codes": ["D"],
                    "specialty": "电气",
                    "started_at": "2026-05-08 09:00",
                },
                action="start",
                now="2026-05-08 09:00",
            )

            job_id, should_start = service.create_action_job(
                {
                    "action": "end",
                    "work_type": "repair",
                    "scope": "D",
                    "record_id": "r1",
                    "source_record_id": "r1",
                    "active_item_id": "active-r1",
                    "title": "D楼UPS检修",
                    "building": "D楼",
                    "building_codes": ["D"],
                    "start_time": "2026-05-08T23:50",
                    "end_time": "2026-05-08T08:20",
                    "progress": "已完成",
                    "extra_images": [{"file_token": "site-photo-token"}],
                    "operation_id": "repair-end-source-record",
                }
            )

            self.assertTrue(should_start)
            prepared = service.prepare_action_job(job_id)
            self.assertEqual(prepared["action"], "end")
            self.assertEqual(prepared["record_id"], "r1")
            self.assertEqual(prepared["target_record_id"], "target-r1")

    def test_change_source_failure_does_not_block_maintenance_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), _ChangeSourceFailureService)
            service.refresh()

            result = service.query_records(month=_TEST_MONTH_LABEL, scope="A")
            self.assertEqual([item["record_id"] for item in result["records"]], ["m1"])
            self.assertIn("变更源表同步失败", result["warnings"][0])

    def test_start_action_uses_cached_records_without_forced_refresh(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("rec1", "A楼", "过滤网维护", _TEST_MONTH_LABEL)]

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
        handler = object.__new__(PortalRuntime)

        def fail_ongoing(scope):
            raise RuntimeError(f"{scope} unavailable")

        old_state_store = PortalRuntime.state_store
        try:
            with tempfile.TemporaryDirectory() as tmp:
                PortalRuntime.state_store = LanPortalStateStore(
                    Path(tmp) / "lan_portal_state.sqlite3"
                )
                PortalRuntime.ongoing_callback = fail_ongoing
                result = handler._get_ongoing("ALL")
                payload = PortalRuntime._with_runtime_warnings({"warnings": []})
        finally:
            PortalRuntime.state_store = old_state_store
            PortalRuntime.ongoing_callback = None
            PortalRuntime.last_ongoing_error = ""

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
                self._records = [_build_record("m1", "A楼", "过滤网维护", _TEST_MONTH_LABEL)]
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

            result = service.query_records(month=_TEST_MONTH_LABEL, scope="A")

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

    def test_query_records_keeps_source_ongoing_change_out_of_active_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record(
                    "source-change-recovered",
                    building="C楼",
                    progress="进行中",
                    title="C楼柴油发电机测试变更",
                )
            ]

            result = service.query_records(scope="C", ongoing_items=[])

            self.assertEqual(
                [item["record_id"] for item in result["records"]],
                ["source-change-recovered"],
            )
            self.assertEqual(result["records"][0]["source_progress"], "进行中")
            self.assertEqual(result["ongoing"], [])

    def test_query_records_does_not_duplicate_recovered_source_ongoing(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record(
                    "source-change-existing",
                    building="C楼",
                    progress="进行中",
                    title="C楼柴油发电机测试变更",
                )
            ]
            ongoing_item = {
                "active_item_id": "active-change-existing",
                "source_record_id": "source-change-existing",
                "target_record_id": "target-change-existing",
                "record_id": "target-change-existing",
                "work_type": WORK_TYPE_CHANGE,
                "notice_type": "变更通告",
                "scope": "C",
                "building": "C楼",
                "building_codes": ["C"],
                "title": "C楼柴油发电机测试变更",
                "status": "进行中",
            }

            result = service.query_records(
                scope="C",
                ongoing_items=[ongoing_item],
            )

            self.assertEqual(len(result["ongoing"]), 1)
            self.assertEqual(
                result["ongoing"][0]["active_item_id"],
                "active-change-existing",
            )
            self.assertFalse(
                bool(result["ongoing"][0].get("recovered_from_source"))
            )

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

            self.assertEqual(
                [record["record_id"] for record in a_records],
                ["z-a", "z-campus", "z-ab", "z-abc", "z-all"],
            )
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
                        "notice_type": "变更通告",
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

    def test_maintenance_work_type_override_moves_record_to_change_and_back(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = MaintenancePortalService._current_month_label()
            previous_month = MaintenancePortalService._recent_month_labels()[1]
            service._records = [
                _build_record(
                    "m-convert-a",
                    "A楼",
                    "冷却塔清洗",
                    current_month,
                    maintenance_cycle="每月",
                ),
                _build_record(
                    "m-convert-a-next-month",
                    "A楼",
                    "冷却塔清洗",
                    previous_month,
                    maintenance_cycle="每月",
                ),
            ]
            service._change_records = []
            service._repair_records = []
            service._zhihang_change_records = []
            service._maintenance_loaded_once = True
            service._change_loaded_once = True
            service._repair_loaded_once = True
            service._zhihang_change_loaded_once = True

            original = service._workbench_records(
                month=RECENT_MONTH_FILTER_LABEL, scope="A"
            )
            self.assertEqual(service._record_work_type(original[0]), WORK_TYPE_MAINTENANCE)

            result = service.set_notice_work_type_override(
                record_id="m-convert-a",
                source_work_type=WORK_TYPE_MAINTENANCE,
                target_work_type=WORK_TYPE_CHANGE,
                scope="A",
                updated_by="tester",
            )
            converted = service._workbench_records(
                month=RECENT_MONTH_FILTER_LABEL, scope="A"
            )
            converted_by_id = {item["record_id"]: item for item in converted}
            serialized = service._serialize_record(converted_by_id["m-convert-a"], {})

            self.assertTrue(result["changed"])
            self.assertEqual(converted_by_id["m-convert-a"].get("work_type"), WORK_TYPE_CHANGE)
            self.assertEqual(
                converted_by_id["m-convert-a-next-month"].get("work_type"),
                WORK_TYPE_CHANGE,
            )
            self.assertEqual(converted_by_id["m-convert-a"].get("source_work_type"), WORK_TYPE_MAINTENANCE)
            self.assertEqual(converted_by_id["m-convert-a"]["display_fields"]["变更楼栋"], "A楼")
            self.assertEqual(converted_by_id["m-convert-a"]["display_fields"]["变更进度"], "未开始")
            self.assertEqual(
                converted_by_id["m-convert-a"]["display_fields"]["变更简述"],
                "EA118机房A楼冷却塔清洗",
            )
            self.assertEqual(serialized["work_type"], WORK_TYPE_CHANGE)
            self.assertEqual(serialized["source_work_type"], WORK_TYPE_MAINTENANCE)
            self.assertEqual(serialized["source_progress"], "未开始")
            self.assertEqual(
                serialized["converted_from_work_type"], WORK_TYPE_MAINTENANCE
            )

            reverted = service.set_notice_work_type_override(
                record_id="m-convert-a",
                source_work_type=WORK_TYPE_MAINTENANCE,
                target_work_type=WORK_TYPE_MAINTENANCE,
                scope="A",
                updated_by="tester",
            )
            restored = service._workbench_records(
                month=RECENT_MONTH_FILTER_LABEL, scope="A"
            )
            restored_by_id = {item["record_id"]: item for item in restored}

            self.assertTrue(reverted["changed"])
            self.assertEqual(
                service._record_work_type(restored_by_id["m-convert-a"]),
                WORK_TYPE_MAINTENANCE,
            )
            self.assertEqual(
                service._record_work_type(restored_by_id["m-convert-a-next-month"]),
                WORK_TYPE_MAINTENANCE,
            )

    def test_prepare_change_action_accepts_converted_maintenance_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = MaintenancePortalService._current_month_label()
            service._records = [
                _build_record(
                    "m-convert-start-a",
                    "A楼",
                    "冷却塔清洗",
                    current_month,
                    maintenance_cycle="每月",
                )
            ]
            service._change_records = []
            service._repair_records = []
            service._zhihang_change_records = []
            service._maintenance_loaded_once = True
            service._change_loaded_once = True
            service._repair_loaded_once = True
            service._zhihang_change_loaded_once = True

            prepared = service.prepare_change_action(
                {
                    "action": "start",
                    "scope": "A",
                    "work_type": WORK_TYPE_CHANGE,
                    "source_work_type": WORK_TYPE_MAINTENANCE,
                    "record_id": "m-convert-start-a",
                    "source_record_id": "m-convert-start-a",
                    "start_time": "2026-06-12T09:30",
                    "end_time": "2026-06-12T18:30",
                    "location": "A楼楼顶",
                    "content": "工程师进行冷却塔清洗",
                    "reason": "按计划维护",
                    "impact": "对IT设备无影响",
                    "progress": "准备工作已完成",
                },
                job_id="job-converted-change",
            )

            self.assertEqual(prepared["work_type"], WORK_TYPE_CHANGE)
            self.assertEqual(prepared["notice_type"], "变更通告")
            self.assertEqual(prepared["source_work_type"], WORK_TYPE_MAINTENANCE)
            self.assertEqual(prepared["source_app_token"], service.app_token)
            self.assertEqual(prepared["source_table_id"], service.table_id)
            self.assertEqual(
                prepared["target_table_id"],
                config_module.config.get_table_id("变更通告"),
            )
            self.assertEqual(prepared["record_id"], "m-convert-start-a")
            self.assertEqual(prepared["building_codes"], ["A"])
            self.assertEqual(prepared["specialty"], "电气")
            change_payload = PortalRuntime._prepared_to_notice_payload(prepared)
            change_fields = ChangeNoticeHandler("变更通告").build_create_fields(
                change_payload
            )
            self.assertEqual(
                change_fields[CHANGE_NOTICE_FIELDS["specialty"]],
                "电气",
            )
            self.assertEqual(prepared["level"], "I3")
            self.assertIn("【变更通告】状态：开始", prepared["text"])
            self.assertIn("【名称】EA118机房A楼冷却塔清洗", prepared["text"])
            self.assertNotIn("【维保通告】", prepared["text"])
            self.assertFalse(prepared["skip_personal_message"])
            self.assertGreaterEqual(len(prepared["recipients"]), 2)
            self.assertTrue(prepared["sync_maintenance_target"])
            self.assertEqual(prepared["paired_upload_status"], "pending")
            self.assertEqual(
                prepared["paired_maintenance_original_title"],
                "EA118机房A楼冷却塔清洗",
            )
            paired = prepared["paired_maintenance_upload"]
            self.assertEqual(paired["notice_type"], "维保通告")
            self.assertEqual(paired["work_type"], WORK_TYPE_MAINTENANCE)
            self.assertEqual(
                paired["target_table_id"],
                config_module.config.get_table_id("维保通告"),
            )
            self.assertEqual(paired["title"], "EA118机房A楼冷却塔清洗")
            self.assertEqual(paired["text"].splitlines()[0], "【维保通告】状态：开始")
            self.assertIn("【名称】EA118机房A楼冷却塔清洗", paired["text"])
            self.assertEqual(paired["maintenance_cycle"], "每月")
            self.assertEqual(
                paired["paired_maintenance_actual_start_time"],
                prepared["response_time"],
            )
            self.assertEqual(paired["recipients"], [])
            self.assertTrue(paired["skip_personal_message"])
            self.assertEqual(paired["robot_group_choice"], "skip")
            paired_payload = PortalRuntime._prepared_to_notice_payload(paired)
            self.assertEqual(paired_payload.robot_group_choice, "skip")

    def test_converted_change_update_recovers_paired_maintenance_target_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = MaintenancePortalService._current_month_label()
            service._records = [
                _build_record(
                    "m-convert-update-a",
                    "A楼",
                    "冷却塔清洗",
                    current_month,
                    status="进行中",
                    maintenance_cycle="每月",
                )
            ]
            service._change_records = []
            service._repair_records = []
            service._zhihang_change_records = []
            service._maintenance_loaded_once = True
            service._change_loaded_once = True
            service._repair_loaded_once = True
            service._zhihang_change_loaded_once = True
            service._state_store.upsert_notice_identity(
                {
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "notice_type": "维保通告",
                    "source_record_id": "m-convert-update-a",
                    "target_record_id": "rec-paired-maintenance",
                },
                origin="test",
            )

            prepared = service.prepare_change_action(
                {
                    "action": "update",
                    "scope": "A",
                    "work_type": WORK_TYPE_CHANGE,
                    "source_work_type": WORK_TYPE_MAINTENANCE,
                    "active_item_id": "active-converted-a",
                    "source_record_id": "m-convert-update-a",
                    "target_record_id": "rec-main-change",
                    "title": "EA118机房A楼冷却塔清洗变更",
                    "start_time": "2026-06-12T09:30",
                    "end_time": "2026-06-12T18:30",
                    "location": "A楼楼顶",
                    "content": "工程师进行冷却塔清洗",
                    "reason": "按计划维护",
                    "impact": "对IT设备无影响",
                    "progress": "清洗进行中",
                },
                job_id="job-converted-change-update",
            )

            self.assertEqual(prepared["target_record_id"], "rec-main-change")
            self.assertEqual(prepared["specialty"], "电气")
            prepared_payload = PortalRuntime._prepared_to_notice_payload(prepared)
            prepared_fields = ChangeNoticeHandler("变更通告").build_update_fields(
                prepared_payload
            )
            self.assertEqual(
                prepared_fields[CHANGE_NOTICE_FIELDS["specialty"]],
                "电气",
            )
            self.assertEqual(
                prepared["paired_maintenance_target_record_id"],
                "rec-paired-maintenance",
            )
            self.assertEqual(
                prepared["paired_maintenance_upload"]["target_record_id"],
                "rec-paired-maintenance",
            )
            self.assertEqual(
                prepared["paired_maintenance_upload"]["robot_group_choice"],
                "skip",
            )

            prepared_end = service.prepare_change_action(
                {
                    "action": "end",
                    "scope": "A",
                    "work_type": WORK_TYPE_CHANGE,
                    "source_work_type": WORK_TYPE_MAINTENANCE,
                    "active_item_id": "active-converted-a",
                    "source_record_id": "m-convert-update-a",
                    "target_record_id": "rec-main-change",
                    "title": "EA118机房A楼冷却塔清洗变更",
                    "start_time": "2026-06-12T09:30",
                    "end_time": "2026-06-12T18:30",
                    "location": "A楼楼顶",
                    "content": "工程师进行冷却塔清洗",
                    "reason": "按计划维护",
                    "impact": "对IT设备无影响",
                    "progress": "清洗已完成",
                    "extra_images": [{"file_token": "site-photo-token"}],
                },
                job_id="job-converted-change-end",
            )
            self.assertEqual(prepared_end["target_record_id"], "rec-main-change")
            self.assertEqual(prepared_end["specialty"], "电气")
            prepared_end_payload = PortalRuntime._prepared_to_notice_payload(
                prepared_end
            )
            prepared_end_fields = ChangeNoticeHandler(
                "变更通告"
            ).build_update_fields(prepared_end_payload)
            self.assertEqual(
                prepared_end_fields[CHANGE_NOTICE_FIELDS["specialty"]],
                "电气",
            )
            self.assertEqual(
                prepared_end["paired_maintenance_target_record_id"],
                "rec-paired-maintenance",
            )
            self.assertEqual(
                prepared_end["paired_maintenance_upload"]["target_record_id"],
                "rec-paired-maintenance",
            )
            self.assertEqual(
                prepared_end["paired_maintenance_upload"]["robot_group_choice"],
                "skip",
            )

    def test_paired_maintenance_upload_result_persists_its_target_identity(self):
        old_store = PortalRuntime.state_store
        with tempfile.TemporaryDirectory() as tmp:
            PortalRuntime.state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            try:
                result = PortalRuntime._apply_paired_maintenance_upload_result(
                    {
                        "sync_maintenance_target": True,
                        "paired_maintenance_upload": {
                            "action": "start",
                            "work_type": WORK_TYPE_MAINTENANCE,
                            "notice_type": "维保通告",
                            "source_record_id": "m-paired-identity-a",
                            "target_record_id": "",
                            "title": "EA118机房A楼测试维保",
                        },
                    },
                    success=True,
                    message="",
                    record_id="rec-paired-identity-a",
                )
                identity = PortalRuntime.state_store.resolve_notice_identity(
                    work_type=WORK_TYPE_MAINTENANCE,
                    source_record_id="m-paired-identity-a",
                )
            finally:
                PortalRuntime.state_store = old_store

            self.assertEqual(
                result["paired_maintenance_target_record_id"],
                "rec-paired-identity-a",
            )
            self.assertIsInstance(identity, dict)
            self.assertEqual(identity["target_record_id"], "rec-paired-identity-a")

    def test_reverted_change_uses_maintenance_table_and_notice_text(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = MaintenancePortalService._current_month_label()
            service._records = [
                _build_record(
                    "m-convert-back-a",
                    "A楼",
                    "UPS厂商巡检维护",
                    current_month,
                    maintenance_cycle="每月",
                )
            ]
            service._change_records = []
            service._repair_records = []
            service._zhihang_change_records = []
            service._maintenance_loaded_once = True
            service._change_loaded_once = True
            service._repair_loaded_once = True
            service._zhihang_change_loaded_once = True

            service.set_notice_work_type_override(
                record_id="m-convert-back-a",
                source_work_type=WORK_TYPE_MAINTENANCE,
                target_work_type=WORK_TYPE_CHANGE,
                scope="A",
                updated_by="tester",
            )
            service.set_notice_work_type_override(
                record_id="m-convert-back-a",
                source_work_type=WORK_TYPE_MAINTENANCE,
                target_work_type=WORK_TYPE_MAINTENANCE,
                scope="A",
                updated_by="tester",
            )

            prepared = service.prepare_maintenance_action(
                {
                    "action": "start",
                    "scope": "A",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "record_id": "m-convert-back-a",
                    "source_record_id": "m-convert-back-a",
                    "start_time": "2026-06-12T09:30",
                    "end_time": "2026-06-12T18:30",
                    "location": "A楼",
                    "content": "工程师进行UPS巡检维护",
                    "reason": "按计划维护",
                    "impact": "对IT设备无影响",
                    "progress": "准备工作已完成",
                },
                job_id="job-converted-back-maintenance",
            )

            self.assertEqual(prepared["work_type"], WORK_TYPE_MAINTENANCE)
            self.assertEqual(prepared["notice_type"], "维保通告")
            self.assertEqual(
                prepared["target_table_id"],
                config_module.config.get_table_id("维保通告"),
            )
            self.assertEqual(prepared["text"].splitlines()[0], "【维保通告】状态：开始")
            self.assertNotIn("【变更通告】", prepared["text"])

    def test_prepare_change_action_recovers_converted_source_type_and_allows_first_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = MaintenancePortalService._current_month_label()
            service._records = [
                _build_record(
                    "m-convert-running",
                    "A楼",
                    "冷却塔清洗",
                    current_month,
                    status="进行中",
                    maintenance_cycle="每月",
                )
            ]
            service._change_records = []
            service._repair_records = []
            service._zhihang_change_records = []
            service._maintenance_loaded_once = True
            service._change_loaded_once = True
            service._repair_loaded_once = True
            service._zhihang_change_loaded_once = True
            service.set_notice_work_type_override(
                record_id="m-convert-running",
                source_work_type=WORK_TYPE_MAINTENANCE,
                target_work_type=WORK_TYPE_CHANGE,
                scope="A",
                updated_by="tester",
            )

            prepared = service.prepare_change_action(
                {
                    "action": "start",
                    "scope": "A",
                    "work_type": WORK_TYPE_CHANGE,
                    "record_id": "m-convert-running",
                    "source_record_id": "m-convert-running",
                    "start_time": "2026-06-12T09:30",
                    "end_time": "2026-06-12T18:30",
                    "location": "A楼楼顶",
                    "content": "工程师进行冷却塔清洗",
                    "reason": "按计划维护",
                    "impact": "对IT设备无影响",
                    "progress": "准备工作已完成",
                },
                job_id="job-converted-running",
            )

            self.assertEqual(prepared["source_work_type"], WORK_TYPE_MAINTENANCE)
            self.assertEqual(prepared["source_record_id"], "m-convert-running")
            self.assertEqual(prepared["source_progress"], "进行中")
            self.assertEqual(prepared["notice_type"], "变更通告")

            service._state_store.upsert_notice_identity(
                {
                    "work_type": WORK_TYPE_CHANGE,
                    "notice_type": "变更通告",
                    "source_record_id": "m-convert-running",
                    "target_record_id": "rec-existing-change",
                },
                origin="test",
            )
            prepared_again = service.prepare_change_action(
                {
                    "action": "start",
                    "scope": "A",
                    "work_type": WORK_TYPE_CHANGE,
                    "record_id": "m-convert-running",
                    "source_record_id": "m-convert-running",
                    "start_time": "2026-06-12T09:30",
                    "end_time": "2026-06-12T18:30",
                    "location": "A楼楼顶",
                    "content": "工程师进行冷却塔清洗",
                    "reason": "按计划维护",
                    "impact": "对IT设备无影响",
                    "progress": "准备工作已完成",
                },
                job_id="job-converted-running-recover",
            )
            with patch.object(
                PortalRuntime.state_store,
                "resolve_notice_identity",
                return_value={"target_record_id": "rec-existing-change"},
            ), patch(
                "lan_bitable_template_portal.server.external_real_write_guard",
                return_value={"mock_external": True},
            ):
                existing_target = PortalRuntime._existing_target_for_prepared_start(
                    prepared_again,
                    "变更通告",
                )

            self.assertEqual(existing_target, "rec-existing-change")

    def test_workbench_lite_contains_auth_heartbeat(self):
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite

        html = render_workbench_lite(
            payload={
                "records": [],
                "ongoing": [],
                "daily_summary": {"stats": {}},
                "record_type_counts": {},
                "ongoing_type_counts": {},
            },
            session={"user": {"name": "测试"}},
            scope="E",
            work_type="maintenance",
            scope_options=[{"value": "E", "label": "E楼"}],
        )

        self.assertIn("function checkLiteAuthStatus()", html)
        self.assertIn("/api/auth/status?next=", html)
        self.assertIn("window.addEventListener('focus', checkLiteAuthStatus)", html)
        self.assertIn("document.addEventListener('visibilitychange'", html)
        self.assertIn("<span>通告处理</span>", html)
        self.assertIn("<span>计划通告列表</span>", html)
        self.assertIn("<span>未结束通告</span>", html)
        self.assertNotIn("任务收件箱", html)
        self.assertNotIn("已开始未结束", html)

    def test_workbench_lite_ongoing_rows_hide_ids_and_normalize_status(self):
        notice_specs = [
            ("maintenance", "维保通告", "未命名维保通告", "开始"),
            ("change", "变更通告", "未命名变更通告", "发送成功"),
            ("repair", "设备检修", "未命名设备检修", "已上传"),
            ("power", "下电通告", "未命名下电通告", "更新"),
            ("polling", "设备轮巡", "未命名设备轮巡", "开始"),
            ("adjust", "设备调整", "未命名设备调整", "发送成功"),
        ]
        items = []
        for index, (work_type, notice_type, _label, status) in enumerate(notice_specs):
            items.append(
                {
                    "active_item_id": f"local_active_internal_{index}",
                    "target_record_id": f"rec_target_internal_{index}",
                    "record_id": f"rec_target_internal_{index}",
                    "work_type": work_type,
                    "notice_type": notice_type,
                    "status": status,
                    "building": "E楼",
                    "specialty": "暖通",
                    "maintenance_cycle": "每月",
                }
            )
        rendered = workbench_lite_module._ongoing_rows(
            items,
            scope="E",
            work_type="all",
            selected_id="",
        )
        visible_text = re.sub(r"<[^>]+>", "", rendered)

        for index, (_work_type, _notice_type, label, _status) in enumerate(notice_specs):
            self.assertIn(label, visible_text)
            self.assertNotIn(f"local_active_internal_{index}", visible_text)
            self.assertNotIn(f"rec_target_internal_{index}", visible_text)
        self.assertNotIn("发送成功", visible_text)
        self.assertNotIn("开始", visible_text)
        self.assertEqual(visible_text.count("进行中"), len(notice_specs))

    def test_workbench_lite_submission_success_restores_ongoing_business_status(self):
        html = workbench_lite_module.render_workbench_lite(
            payload={
                "records": [],
                "ongoing": [],
                "daily_summary": {"stats": {}},
                "record_type_counts": {},
                "ongoing_type_counts": {},
            },
            session={"user": {"name": "测试用户"}},
            scope="E",
            work_type="maintenance",
        )

        self.assertIn("function setOngoingRowStatus(row, text, tone)", html)
        self.assertIn("function ongoingDisplayTitle(draft)", html)
        self.assertIn("const title = ongoingDisplayTitle(draft);", html)
        self.assertIn("setOngoingRowStatus(row, '进行中', 'working');", html)
        self.assertIn("setLiteStatus(successfulNoticeActionText(payload?.action));", html)
        self.assertNotIn("setMetaChip(meta, status, 'ready');", html)
        self.assertNotIn("setMetaChip(meta, String(message).slice", html)
        self.assertNotIn("setMetaChip(meta, String(message || patch.message", html)
        self.assertNotIn("status.textContent = ok ? '发送成功' : '发送失败';", html)

    def test_qt_manual_update_check_exposes_update_without_auto_apply(self):
        manifest = {
            "target_patch_version": 12,
            "target_display_version": "测试新版本",
            "ui_changed": False,
        }

        class SignalRecorder:
            def __init__(self):
                self.calls = []

            def emit(self, *args):
                self.calls.append(args)

        signal = SignalRecorder()
        updater = SimpleNamespace(
            manifest_url="",
            fetch_manifest=lambda: manifest,
            has_newer_patch=lambda _local, _remote: True,
            is_ui_update=lambda _remote: False,
        )
        window = SimpleNamespace(
            _remote_patch_updater=updater,
            remote_update_checked=signal,
            _get_app_root_dir=lambda: Path("."),
        )

        with patch.object(
            RemotePatchUpdater,
            "load_local_build_meta",
            return_value={"major_version": 1, "patch_version": 11},
        ):
            PatchUpdateMixin._remote_update_check_worker(window, manual=True)

        self.assertEqual(len(signal.calls), 1)
        status, ui_manifest, non_ui_manifest = signal.calls[0]
        self.assertIn("发现新版本", status)
        self.assertIs(ui_manifest, manifest)
        self.assertIsNone(non_ui_manifest)

    def test_qt_update_check_clears_stale_remote_button_when_latest(self):
        status_values = []
        button_states = []
        refresh_calls = []
        window = SimpleNamespace(
            _remote_update_checking=True,
            _remote_update_busy=False,
            _closing=False,
            _remote_ui_manifest={"target_patch_version": 11},
            _remote_non_ui_manifest={"target_patch_version": 11},
            _set_remote_update_status=lambda value: status_values.append(value),
            _set_remote_update_check_button=lambda value=False: button_states.append(value),
            _refresh_patch_button=lambda: refresh_calls.append(True),
        )

        PatchUpdateMixin._on_remote_update_checked(
            window,
            "远程更新: 已是最新",
            None,
            None,
        )

        self.assertFalse(window._remote_update_checking)
        self.assertIsNone(window._remote_ui_manifest)
        self.assertIsNone(window._remote_non_ui_manifest)
        self.assertEqual(status_values, ["远程更新: 已是最新"])
        self.assertEqual(button_states, [False])
        self.assertEqual(refresh_calls, [True])

    def test_qt_update_check_button_uses_existing_theme_style(self):
        ui_source = (
            BIN_DIR / "upload_event_module" / "ui" / "main_window_ui.py"
        ).read_text(encoding="utf-8")

        self.assertIn('self.check_update_btn = QPushButton("检查更新")', ui_source)
        self.assertIn('self.check_update_btn.setObjectName("NavBtn")', ui_source)
        self.assertIn(
            "self.check_update_btn.clicked.connect(self.check_remote_update_now)",
            ui_source,
        )

    def test_qt_update_check_button_is_disabled_when_remote_update_is_disabled(self):
        class FakeButton:
            def __init__(self):
                self.enabled = True
                self.text = ""
                self.tooltip = ""

            def setEnabled(self, value):
                self.enabled = bool(value)

            def setText(self, value):
                self.text = value

            def setToolTip(self, value):
                self.tooltip = value

        button = FakeButton()
        window = SimpleNamespace(
            check_update_btn=button,
            _remote_update_busy=False,
            _ui_update_in_progress=False,
            _patch_apply_in_progress=False,
        )

        with patch.object(config_module.config, "remote_update_enabled", False):
            PatchUpdateMixin._set_remote_update_check_button(window, False)

        self.assertFalse(button.enabled)
        self.assertEqual(button.text, "检查更新")
        self.assertIn("已在设置中关闭", button.tooltip)

    def test_qt_patch_button_keeps_busy_state_during_periodic_refresh(self):
        class FakePatchButton:
            def __init__(self):
                self.enabled = True
                self.text = "更新"

            def isHidden(self):
                return False

            def setEnabled(self, value):
                self.enabled = bool(value)

            def setText(self, value):
                self.text = value

        downloading = SimpleNamespace(
            patch_btn=FakePatchButton(),
            _remote_update_busy=True,
            _patch_apply_in_progress=False,
        )
        applying = SimpleNamespace(
            patch_btn=FakePatchButton(),
            _remote_update_busy=False,
            _patch_apply_in_progress=True,
        )

        PatchUpdateMixin._refresh_patch_button(downloading)
        PatchUpdateMixin._refresh_patch_button(applying)

        self.assertFalse(downloading.patch_btn.enabled)
        self.assertEqual(downloading.patch_btn.text, "下载中")
        self.assertFalse(applying.patch_btn.enabled)
        self.assertEqual(applying.patch_btn.text, "更新中")

    def test_qt_update_worker_does_not_emit_after_window_closes(self):
        fetch_calls = []
        window = SimpleNamespace(
            _closing=True,
            _remote_patch_updater=SimpleNamespace(
                fetch_manifest=lambda: fetch_calls.append(True)
            ),
        )

        PatchUpdateMixin._remote_update_check_worker(window, manual=True)

        self.assertEqual(fetch_calls, [])

    def test_qt_delayed_update_check_does_nothing_after_window_closes(self):
        status_values = []
        window = SimpleNamespace(
            _closing=True,
            _remote_update_busy=False,
            _remote_update_checking=False,
            _set_remote_update_status=lambda value: status_values.append(value),
            _set_remote_update_check_button=lambda _value=False: None,
        )

        PatchUpdateMixin._schedule_remote_update_check(window, manual=False)

        self.assertFalse(window._remote_update_checking)
        self.assertEqual(status_values, [])

    def test_qt_update_clears_remote_manifest_already_satisfied_by_local_patch(self):
        updater = SimpleNamespace(
            has_newer_patch=lambda _local, _manifest: False,
        )
        window = SimpleNamespace(
            _remote_patch_updater=updater,
            _remote_ui_manifest={"target_patch_version": 12},
            _remote_non_ui_manifest={"target_patch_version": 12},
            _get_app_root_dir=lambda: Path("."),
        )

        with patch.object(
            RemotePatchUpdater,
            "load_local_build_meta",
            return_value={"major_version": 1, "patch_version": 12},
        ):
            PatchUpdateMixin._discard_satisfied_remote_manifests(window)

        self.assertIsNone(window._remote_ui_manifest)
        self.assertIsNone(window._remote_non_ui_manifest)

    def test_workbench_lite_repair_prefill_selects_only_its_source_record(self):
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite

        payload = {
            "records": [
                {
                    "record_id": "rec_repair_wrong",
                    "source_record_id": "rec_repair_wrong",
                    "work_type": "repair",
                    "notice_type": "设备检修",
                    "title": "A楼其他检修",
                    "building": "A楼",
                },
                {
                    "record_id": "rec_repair_expected",
                    "source_record_id": "rec_repair_expected",
                    "work_type": "repair",
                    "notice_type": "设备检修",
                    "title": "A楼当前维修项目",
                    "building": "A楼",
                },
            ],
            "ongoing": [],
            "daily_summary": {"stats": {}},
            "record_type_counts": {"repair": 2},
            "ongoing_type_counts": {"repair": 0},
        }
        html = render_workbench_lite(
            payload=payload,
            session={"user": {"name": "测试用户"}, "is_admin": False},
            scope="A",
            work_type="repair",
            prefill_draft={"work_type": "repair", "title": "A楼当前维修项目"},
            prefill_source_record_id="rec_repair_expected",
            prefill_context_id="rec_repair_expected",
        )

        self.assertRegex(
            html,
            r'class="notice-row active"[^>]+data-record-id="rec_repair_expected"',
        )
        self.assertNotRegex(
            html,
            r'class="notice-row active"[^>]+data-record-id="rec_repair_wrong"',
        )

        missing_html = render_workbench_lite(
            payload=payload,
            session={"user": {"name": "测试用户"}, "is_admin": False},
            scope="A",
            work_type="repair",
            prefill_draft={"work_type": "repair", "title": "未在当前分页"},
            prefill_source_record_id="rec_repair_not_on_page",
            prefill_context_id="rec_repair_not_on_page",
        )
        self.assertNotIn('class="notice-row active"', missing_html)

    def test_workbench_lite_notice_detail_uses_right_drawer(self):
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite

        payload = {
            "records": [
                {
                    "record_id": "rec_maintenance",
                    "source_record_id": "rec_maintenance",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "title": "E楼UPS维护",
                    "building": "E楼",
                }
            ],
            "ongoing": [],
            "daily_summary": {"stats": {}},
            "record_type_counts": {"maintenance": 1},
            "ongoing_type_counts": {"maintenance": 0},
        }
        default_html = render_workbench_lite(
            payload=payload,
            session={"user": {"name": "测试用户"}, "is_admin": False},
            scope="E",
            work_type="maintenance",
        )

        self.assertIn('id="lite-notice-detail-overlay"', default_html)
        self.assertIn('data-open-on-load="0"', default_html)
        self.assertIn('class="panel detail-panel notice-detail-drawer"', default_html)
        self.assertIn('id="lite-notice-drawer-close"', default_html)
        self.assertIn("function openNoticeDrawer(title, trigger)", default_html)
        self.assertIn("function requestCloseNoticeDrawer()", default_html)
        self.assertIn("let pendingDiscardPromise = null", default_html)
        self.assertIn("lastNoticeDrawerTrigger = trigger", default_html)
        self.assertIn("if (body) body.scrollTop = 0", default_html)
        self.assertNotIn('class="notice-row active"', default_html)

        selected_html = render_workbench_lite(
            payload=payload,
            session={"user": {"name": "测试用户"}, "is_admin": False},
            scope="E",
            work_type="maintenance",
            record_id="rec_maintenance",
        )
        self.assertIn(
            'class="notice-detail-overlay open" id="lite-notice-detail-overlay"',
            selected_html,
        )
        self.assertIn('data-open-on-load="1"', selected_html)
        self.assertIn(
            '<h2 id="lite-notice-drawer-title">EA118机房E楼UPS维护</h2>',
            selected_html,
        )
        self.assertRegex(
            selected_html,
            r'class="notice-row active"[^>]+data-record-id="rec_maintenance"',
        )

    def test_workbench_lite_pending_repair_can_prefill_from_incomplete_event_project(self):
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite

        payload = {
            "records": [
                {
                    "record_id": "rec_repair_pending",
                    "source_record_id": "rec_repair_pending",
                    "work_type": "repair",
                    "notice_type": "设备检修",
                    "title": "E楼精密空调检修",
                    "building": "E楼",
                    "display_fields": {
                        "事件描述": "E楼精密空调告警",
                    },
                    "raw_fields": {
                        "关联事件单": "rec_event_current",
                    },
                }
            ],
            "ongoing": [],
            "daily_summary": {"stats": {}},
            "record_type_counts": {"repair": 1},
            "ongoing_type_counts": {"repair": 0},
        }

        html = render_workbench_lite(
            payload=payload,
            session={"user": {"name": "测试用户"}, "is_admin": False},
            scope="E",
            work_type="repair",
            record_id="rec_repair_pending",
        )

        self.assertIn('id="lite-repair-event-open"', html)
        self.assertIn('data-source-event-id="rec_event_current"', html)
        self.assertIn("E楼精密空调告警", html)
        self.assertRegex(
            html,
            r'<section class="repair-event-link-panel" data-repair-event-link>',
        )
        self.assertIn('id="lite-repair-event-candidates"', html)
        self.assertIn("/api/workbench/repair-event-candidates", html)
        self.assertIn("/api/workbench/repair-event-bind", html)
        self.assertIn("保存关联并填入", html)
        self.assertIn("const draftComplete = result.draft_complete === true", html)
        self.assertIn("if (draftComplete)", html)
        self.assertIn("事件关联已保存；当前通告已切换", html)
        self.assertIn("function confirmRepairEventCandidate()", html)
        self.assertIn("let liteRepairEventRequestController = null", html)
        self.assertIn("liteRepairEventRequestController.abort()", html)
        self.assertIn("let liteRepairEventPrefillController = null", html)
        self.assertIn("liteRepairEventPrefillController.abort()", html)
        self.assertIn(
            "requestSequence !== liteRepairEventRequestSequence",
            html,
        )
        self.assertIn(
            "previewValue(form, 'source_record_id') !== sourceRecordIdAtStart",
            html,
        )
        self.assertIn(
            "resetRepairEventSelection(form, !linkedOngoing && workType === 'repair')",
            html,
        )
        self.assertNotIn(
            "setFormValue(form, 'source_record_id', candidate.event_record_id",
            html,
        )
        self.assertNotIn(
            "setFormValue(form, 'target_record_id', candidate.event_record_id",
            html,
        )
        self.assertIn(
            "candidate_project_record_id: repairManagementRecordId",
            html,
        )

    def test_converted_maintenance_change_allows_empty_maintenance_cycle_for_pair_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = MaintenancePortalService._current_month_label()
            service._records = [
                _build_record(
                    "m-convert-no-cycle",
                    "A楼",
                    "电池内阻刷新维护",
                    current_month,
                    maintenance_cycle="",
                )
            ]
            service._change_records = []
            service._repair_records = []
            service._zhihang_change_records = []
            service._maintenance_loaded_once = True
            service._change_loaded_once = True
            service._repair_loaded_once = True
            service._zhihang_change_loaded_once = True

            prepared = service.prepare_change_action(
                {
                    "action": "start",
                    "scope": "A",
                    "work_type": WORK_TYPE_CHANGE,
                    "source_work_type": WORK_TYPE_MAINTENANCE,
                    "record_id": "m-convert-no-cycle",
                    "source_record_id": "m-convert-no-cycle",
                    "start_time": "2026-06-12T09:30",
                    "end_time": "2026-06-12T18:30",
                    "location": "A楼",
                    "content": "工程师进行维护",
                    "reason": "按计划维护",
                    "impact": "无影响",
                    "progress": "准备工作已完成",
                },
                job_id="job-converted-no-cycle",
            )

            self.assertTrue(prepared["sync_maintenance_target"])
            self.assertEqual(prepared["paired_maintenance_upload"]["maintenance_cycle"], "")

    def test_110_station_notice_titles_use_aliyun_zhongtian_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = MaintenancePortalService._current_month_label()
            service._records = [
                _build_record(
                    "m-110",
                    "110站",
                    "电池内阻刷新维护通告",
                    current_month,
                    maintenance_cycle="每月",
                )
            ]
            service._maintenance_loaded_once = True
            prepared = service.prepare_maintenance_action(
                {
                    "action": "start",
                    "scope": "110",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "record_id": "m-110",
                    "source_record_id": "m-110",
                    "start_time": "2026-06-12T09:30",
                    "end_time": "2026-06-12T18:30",
                    "location": "110站",
                    "content": "工程师进行维护",
                    "reason": "按计划维护",
                    "impact": "无影响",
                    "progress": "准备工作已完成",
                },
                job_id="job-110-title",
            )

            self.assertEqual(
                prepared["title"],
                "EA118-110KV阿里中天变电池内阻刷新维护通告",
            )
            self.assertIn(
                "【名称】EA118-110KV阿里中天变电池内阻刷新维护通告",
                prepared["text"],
            )

    def test_source_maintenance_start_keeps_edited_notice_title(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = MaintenancePortalService._current_month_label()
            service._records = [
                _build_record(
                    "m-edited-title",
                    "E楼",
                    "水系统维护",
                    current_month,
                    maintenance_cycle="每月",
                )
            ]
            service._maintenance_loaded_once = True
            edited_title = "EA118机房E楼测试测试用户编辑维保标题"

            prepared = service.prepare_maintenance_action(
                {
                    "action": "start",
                    "scope": "E",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "record_id": "m-edited-title",
                    "source_record_id": "m-edited-title",
                    "title": edited_title,
                    "specialty": "电气",
                    "start_time": "2026-06-25T09:30",
                    "end_time": "2026-06-25T18:30",
                    "location": "E楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备工作已完成",
                },
                job_id="job-source-maint-edited-title",
            )

            self.assertEqual(prepared["title"], edited_title)
            self.assertIn(f"【名称】{edited_title}", prepared["text"])
            self.assertEqual(prepared["source_record_id"], "m-edited-title")
            self.assertEqual(prepared["target_record_id"], "")

    def test_source_change_start_keeps_edited_notice_title(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = []
            service._change_records = [
                _build_change_record(
                    "c-edited-title",
                    building="E楼",
                    progress="未开始",
                    title="E楼源表变更标题",
                )
            ]
            service._change_loaded_once = True
            edited_title = "EA118机房E楼测试测试用户编辑变更标题"

            prepared = service.prepare_change_action(
                {
                    "action": "start",
                    "scope": "E",
                    "work_type": WORK_TYPE_CHANGE,
                    "source_work_type": WORK_TYPE_CHANGE,
                    "record_id": "c-edited-title",
                    "source_record_id": "c-edited-title",
                    "title": edited_title,
                    "level": "I3",
                    "start_time": "2026-06-25T09:30",
                    "end_time": "2026-06-25T18:30",
                    "location": "E楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备工作已完成",
                },
                job_id="job-source-change-edited-title",
            )

            self.assertEqual(prepared["title"], edited_title)
            self.assertIn(f"【名称】{edited_title}", prepared["text"])
            self.assertEqual(prepared["source_record_id"], "c-edited-title")
            self.assertEqual(prepared["target_record_id"], "")

    def test_maintenance_delayed_not_started_can_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = MaintenancePortalService._current_month_label()
            service._records = [
                _build_record(
                    "m-delayed-not-started",
                    "A楼",
                    "冷却塔清洗",
                    current_month,
                    status="延期未开始",
                    maintenance_cycle="每月",
                )
            ]
            service._maintenance_loaded_once = True

            prepared = service.prepare_maintenance_action(
                {
                    "action": "start",
                    "scope": "A",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "record_id": "m-delayed-not-started",
                    "source_record_id": "m-delayed-not-started",
                    "specialty": "电气",
                    "start_time": "2026-06-12T09:30",
                    "end_time": "2026-06-12T18:30",
                    "location": "A楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备工作已完成",
                },
                job_id="job-maint-delayed-not-started",
            )

            self.assertEqual(prepared["action"], "start")
            self.assertEqual(prepared["source_progress"], "延期未开始")

    def test_unlinked_source_ongoing_records_can_start_for_all_source_types(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = MaintenancePortalService._current_month_label()
            service._records = [
                _build_record(
                    "m-source-ongoing",
                    "A楼",
                    "冷却塔清洗",
                    current_month,
                    status="进行中",
                    maintenance_cycle="每月",
                )
            ]
            service._change_records = [
                _build_change_record(
                    "c-source-ongoing",
                    building="A楼",
                    progress="进行中",
                    title="A楼源表进行中变更",
                )
            ]
            service._repair_records = [
                _build_repair_record(
                    "r-source-ongoing",
                    building="A楼",
                    title="A楼源表进行中检修",
                    started=True,
                    target_record_id="recTargetSourceOngoing",
                )
            ]
            service._maintenance_loaded_once = True
            service._change_loaded_once = True
            service._repair_loaded_once = True

            maintenance = service.prepare_maintenance_action(
                {
                    "action": "start",
                    "scope": "A",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "record_id": "m-source-ongoing",
                    "source_record_id": "m-source-ongoing",
                    "specialty": "电气",
                    "start_time": "2026-06-12T09:30",
                    "end_time": "2026-06-12T18:30",
                    "location": "A楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备工作已完成",
                },
                job_id="job-maint-source-ongoing",
            )
            change = service.prepare_change_action(
                {
                    "action": "start",
                    "scope": "A",
                    "work_type": WORK_TYPE_CHANGE,
                    "source_work_type": WORK_TYPE_CHANGE,
                    "record_id": "c-source-ongoing",
                    "source_record_id": "c-source-ongoing",
                    "title": "A楼源表进行中变更",
                    "level": "I3",
                    "start_time": "2026-06-12T09:30",
                    "end_time": "2026-06-12T18:30",
                    "location": "A楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备工作已完成",
                },
                job_id="job-change-source-ongoing",
            )
            repair = service.prepare_repair_action(
                {
                    "action": "start",
                    "scope": "A",
                    "work_type": WORK_TYPE_REPAIR,
                    "record_id": "r-source-ongoing",
                    "source_record_id": "r-source-ongoing",
                    "title": "A楼源表进行中检修",
                    "fault_time": _test_datetime(8, "08:20"),
                    "expected_time": _test_datetime(8, "18:00"),
                    "location": "A楼",
                    "specialty": "电气",
                    "repair_device": "UPS",
                    "repair_fault": "测试故障",
                    "fault_type": "设备故障",
                    "repair_mode": "自维",
                    "discovery": "人工发现",
                    "symptom": "测试现象",
                    "reason": "测试原因",
                    "solution": "测试方案",
                    "progress": "准备工作已完成",
                },
                job_id="job-repair-source-ongoing",
            )

            self.assertEqual(maintenance["source_progress"], "进行中")
            self.assertEqual(change["source_progress"], "进行中")
            self.assertEqual(repair["source_progress"], "进行中")
            self.assertEqual(
                repair["target_record_id"],
                "recTargetSourceOngoing",
            )

    def test_prepare_actions_reject_time_range_shorter_than_one_hour(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = MaintenancePortalService._current_month_label()
            service._records = [
                _build_record(
                    "m-short-time",
                    "A楼",
                    "冷却塔清洗",
                    current_month,
                    maintenance_cycle="每月",
                )
            ]
            service._change_records = [
                _build_change_record(
                    "c-short-time",
                    building="A楼",
                    progress="未开始",
                    title="A楼短时变更",
                )
            ]
            service._repair_records = [
                _build_repair_record(
                    "r-short-time",
                    building="A楼",
                    title="A楼短时检修",
                )
            ]
            service._zhihang_change_records = []
            service._maintenance_loaded_once = True
            service._change_loaded_once = True
            service._repair_loaded_once = True
            service._zhihang_change_loaded_once = True

            with self.assertRaisesRegex(PortalError, "不能少于1小时"):
                service.prepare_maintenance_action(
                    {
                        "action": "start",
                        "scope": "A",
                        "record_id": "m-short-time",
                        "specialty": "电气",
                        "start_time": "2026-06-12T09:30",
                        "end_time": "2026-06-12T10:00",
                        "location": "A楼",
                        "content": "测试内容",
                        "reason": "测试原因",
                        "impact": "测试影响",
                        "progress": "测试进度",
                    },
                    job_id="job-maint-short-time",
                )

            with self.assertRaisesRegex(PortalError, "不能少于1小时"):
                service.prepare_change_action(
                    {
                        "action": "start",
                        "scope": "A",
                        "work_type": WORK_TYPE_CHANGE,
                        "record_id": "c-short-time",
                        "source_record_id": "c-short-time",
                        "start_time": "2026-06-12T09:30",
                        "end_time": "2026-06-12T10:00",
                        "location": "A楼",
                        "content": "测试内容",
                        "reason": "测试原因",
                        "impact": "测试影响",
                        "progress": "测试进度",
                    },
                    job_id="job-change-short-time",
                )

            with self.assertRaisesRegex(PortalError, "不能少于1小时"):
                service.prepare_repair_action(
                    {
                        "action": "start",
                        "scope": "A",
                        "work_type": WORK_TYPE_REPAIR,
                        "record_id": "r-short-time",
                        "start_time": "2026-06-12T10:00",
                        "end_time": "2026-06-12T09:30",
                        "location": "A楼",
                        "reason": "测试原因",
                        "impact": "测试影响",
                        "progress": "测试进度",
                    },
                    job_id="job-repair-short-time",
                )

            with self.assertRaisesRegex(PortalError, "不能少于1小时"):
                service.prepare_simple_manual_notice_action(
                    {
                        "action": "start",
                        "scope": "A",
                        "work_type": WORK_TYPE_POWER,
                        "manual": True,
                        "manual_id": "manual-power-short",
                        "title": "A楼短时上电",
                        "building": "A楼",
                        "building_codes": ["A"],
                        "start_time": "2026-06-12T09:30",
                        "end_time": "2026-06-12T10:00",
                        "cabinet": "A01",
                        "quantity": "1",
                        "progress": "测试进度",
                    },
                    job_id="job-power-short-time",
                )

    def test_prepare_end_actions_require_site_photo_payload(self):
        service = _TestMaintenancePortalService()
        site_photo = [{"upload_id": "upload-site-photo", "file_name": "site.png"}]

        maintenance_payload = {
            "manual": True,
            "manual_id": "manual-maint-end",
            "record_id": "target-maint-end",
            "target_record_id": "target-maint-end",
            "active_item_id": "active-maint-end",
            "action": "end",
            "scope": "A",
            "work_type": "maintenance",
            "title": "手动维保结束",
            "building": "A楼",
            "specialty": "电气",
            "maintenance_cycle": "每月",
            "start_time": "2026-06-12T09:30",
            "end_time": "2026-06-12T18:30",
            "location": "A楼",
            "content": "测试内容",
            "reason": "测试原因",
            "impact": "测试影响",
            "progress": "测试进度",
        }
        with self.assertRaisesRegex(PortalError, "现场照片"):
            service.prepare_maintenance_action(maintenance_payload, job_id="job-maint-end-missing-photo")
        prepared_maintenance = service.prepare_maintenance_action(
            {**maintenance_payload, "extra_images": site_photo},
            job_id="job-maint-end-photo",
        )
        self.assertEqual(prepared_maintenance["extra_images"], site_photo)

        change_payload = {
            "manual": True,
            "manual_id": "manual-change-end",
            "record_id": "target-change-end",
            "target_record_id": "target-change-end",
            "active_item_id": "active-change-end",
            "action": "end",
            "scope": "A",
            "work_type": "change",
            "title": "手动变更结束",
            "building": "A楼",
            "building_codes": ["A"],
            "start_time": "2026-06-12T09:30",
            "end_time": "2026-06-12T18:30",
            "location": "A楼",
        }
        with self.assertRaisesRegex(PortalError, "现场照片"):
            service.prepare_change_action(change_payload, job_id="job-change-end-missing-photo")
        prepared_change = service.prepare_change_action(
            {**change_payload, "extra_images": site_photo},
            job_id="job-change-end-photo",
        )
        self.assertEqual(prepared_change["extra_images"], site_photo)

        repair_payload = {
            "manual": True,
            "manual_id": "manual-repair-end",
            "record_id": "target-repair-end",
            "target_record_id": "target-repair-end",
            "active_item_id": "active-repair-end",
            "action": "end",
            "scope": "A",
            "work_type": "repair",
            "title": "手动检修结束",
            "building": "A楼",
            "building_codes": ["A"],
            "specialty": "暖通",
            "expected_time": "2026-06-12T23:50",
            "fault_time": "2026-06-12T08:00",
            "location": "A楼",
        }
        with self.assertRaisesRegex(PortalError, "现场照片"):
            service.prepare_repair_action(repair_payload, job_id="job-repair-end-missing-photo")
        prepared_repair = service.prepare_repair_action(
            {**repair_payload, "extra_images": site_photo},
            job_id="job-repair-end-photo",
        )
        self.assertEqual(prepared_repair["extra_images"], site_photo)

        power_payload = {
            "manual": True,
            "manual_id": "manual-power-end",
            "record_id": "target-power-end",
            "target_record_id": "target-power-end",
            "active_item_id": "active-power-end",
            "action": "end",
            "scope": "A",
            "work_type": WORK_TYPE_POWER,
            "title": "手动上电结束",
            "building": "A楼",
            "building_codes": ["A"],
            "start_time": "2026-06-12T09:30",
            "end_time": "2026-06-12T18:30",
            "cabinet": "A01",
            "quantity": "1",
            "progress": "测试进度",
        }
        prepared_power_without_photo = service.prepare_simple_manual_notice_action(
            power_payload,
            job_id="job-power-end-missing-photo",
        )
        self.assertEqual(prepared_power_without_photo["extra_images"], [])
        prepared_power = service.prepare_simple_manual_notice_action(
            {**power_payload, "extra_images": site_photo},
            job_id="job-power-end-photo",
        )
        self.assertEqual(prepared_power["extra_images"], site_photo)

    def test_backend_extra_image_upload_accepts_upload_id(self):
        original_store = PortalRuntime.state_store
        try:
            with tempfile.TemporaryDirectory() as tmp:
                PortalRuntime.state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
                saved = PortalRuntime.state_store.put_notice_upload_attachment(
                    open_id="ou_test",
                    file_name="site.png",
                    mime_type="image/png",
                    content=b"fake-image-bytes",
                    ttl_seconds=120,
                )
                calls = []

                def _fake_upload(image_bytes, file_name=""):
                    calls.append((image_bytes, file_name))
                    return True, "file-token-1"

                with patch.object(portal_server_module, "upload_media_to_feishu", _fake_upload):
                    ok, error, file_tokens, extra_tokens = PortalRuntime._upload_extra_images_for_notice(
                        {"extra_images": [{"upload_id": saved["upload_id"], "file_name": "site.png"}]},
                        "维保通告",
                    )

                self.assertTrue(ok, error)
                self.assertEqual(file_tokens, [])
                self.assertEqual(extra_tokens, ["file-token-1"])
                self.assertEqual(calls, [(b"fake-image-bytes", "site.png")])
                loaded = PortalRuntime.state_store.get_notice_upload_attachment(saved["upload_id"])
                self.assertIsNotNone(loaded)
                self.assertTrue(loaded["used_at"])
        finally:
            PortalRuntime.state_store = original_store

    def test_change_raw_option_specialty_is_not_used_as_specialty(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            record = _build_change_record(
                "c-raw-specialty",
                building="A楼",
                progress="未开始",
                title="A楼变更",
            )
            record["display_fields"]["专业"] = "optABC123XYZ"
            service._change_records = [record]
            service._records = []
            service._repair_records = []
            service._zhihang_change_records = []
            service._maintenance_loaded_once = True
            service._change_loaded_once = True
            service._repair_loaded_once = True
            service._zhihang_change_loaded_once = True

            prepared = service.prepare_change_action(
                {
                    "action": "start",
                    "scope": "A",
                    "work_type": WORK_TYPE_CHANGE,
                    "record_id": "c-raw-specialty",
                    "source_record_id": "c-raw-specialty",
                    "start_time": "2026-06-12T09:30",
                    "end_time": "2026-06-12T18:30",
                    "location": "A楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备工作已完成",
                },
                job_id="job-change-raw-specialty",
            )

            self.assertEqual(prepared["specialty"], "")
            self.assertNotIn("optABC123XYZ", prepared["text"])

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

    def test_prepare_change_action_defaults_missing_level_to_i3_group_route(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            prepared = service.prepare_change_action(
                {
                    "action": "start",
                    "scope": "A",
                    "work_type": WORK_TYPE_CHANGE,
                    "manual": True,
                    "manual_id": "manual-change-a",
                    "title": "A楼手动变更",
                    "building": "A楼",
                    "building_codes": ["A"],
                    "specialty": "电气",
                    "start_time": "2026-05-08T09:00",
                    "end_time": "2026-05-08T18:00",
                    "location": "A楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备开始",
                },
                job_id="job-change-default-i3",
            )

            self.assertEqual(prepared["level"], "I3")
            self.assertIn("【等级】I3", prepared["text"])
            self.assertIn("【时间】2026-05-08 09:00~2026-05-08 18:00", prepared["text"])
            self.assertNotIn("至", prepared["text"])
            _, _, _, route_level = ChangeNoticeHandler().build_robot_message(
                NoticePayload(text=prepared["text"], level=prepared["level"])
            )
            self.assertEqual(route_level, "I3")

    def test_prepare_change_action_keeps_explicit_ali_medium_level_out_of_i3_group(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            record = _build_change_record(
                "ali-medium",
                building="A楼",
                progress="未开始",
                title="A楼中等级变更",
            )
            record["display_fields"]["变更等级（阿里）"] = "中"
            service._change_records = [record]

            prepared = service.prepare_change_action(
                {
                    "action": "start",
                    "scope": "A",
                    "work_type": WORK_TYPE_CHANGE,
                    "record_id": "ali-medium",
                    "specialty": "电气",
                    "start_time": "2026-05-08T09:00",
                    "end_time": "2026-05-08T18:00",
                    "location": "A楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备开始",
                },
                job_id="job-change-medium",
            )

            self.assertEqual(prepared["level"], "中")
            _, _, _, route_level = ChangeNoticeHandler().build_robot_message(
                NoticePayload(text=prepared["text"], level=prepared["level"])
            )
            self.assertEqual(route_level, "")

    def test_change_start_uses_scoped_snapshot_codes_for_zhihang_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            stale_memory_record = _build_change_record(
                "ali-e",
                building="A楼、B楼",
                progress="未开始",
                title="E楼阿里侧变更",
            )
            scoped_record = _build_change_record(
                "ali-e",
                building="E楼",
                progress="未开始",
                title="E楼阿里侧变更",
            )
            service._change_records = [stale_memory_record]
            service._zhihang_change_records = [
                _build_zhihang_change_record("z-e", title="EA118机房E楼链路调整")
            ]
            zhihang_record = service._zhihang_change_records[0]
            service._state_store.replace_all_source_scope_snapshots(
                {
                    "ALL": {
                        "records": [stale_memory_record],
                        "zhihang_records": [zhihang_record],
                    },
                    "E": {"records": [scoped_record], "zhihang_records": [zhihang_record]},
                },
                meta={},
            )

            prepared = service.prepare_change_action(
                {
                    "action": "start",
                    "scope": "E",
                    "work_type": WORK_TYPE_CHANGE,
                    "record_id": "ali-e",
                    "building_codes": ["E"],
                    "specialty": "电气",
                    "start_time": "2026-05-08T09:00",
                    "end_time": "2026-05-08T18:00",
                    "location": "E楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "测试影响",
                    "progress": "准备开始",
                    "zhihang_involved": True,
                    "zhihang_record_id": "z-e",
                },
                job_id="job-z-e",
            )

            self.assertEqual(prepared["building_codes"], ["E"])
            self.assertEqual(prepared["building"], "E楼")
            self.assertEqual(prepared["zhihang_record_id"], "z-e")

    def test_change_start_rejects_payload_codes_without_scoped_snapshot_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record(
                    "ali-ab",
                    building="A楼、B楼",
                    progress="未开始",
                    title="园区阿里侧变更",
                )
            ]

            with self.assertRaisesRegex(PortalError, "当前入口是E楼，但变更通告属于"):
                service.prepare_change_action(
                    {
                        "action": "start",
                        "scope": "E",
                        "work_type": WORK_TYPE_CHANGE,
                        "record_id": "ali-ab",
                        "building_codes": ["E"],
                        "specialty": "电气",
                        "start_time": "2026-05-08T09:00",
                        "end_time": "2026-05-08T18:00",
                        "location": "E楼",
                        "content": "测试内容",
                        "reason": "测试原因",
                        "impact": "测试影响",
                        "progress": "准备开始",
                    },
                    job_id="job-z-reject",
                )

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

    def test_query_records_keeps_source_ongoing_repair_out_of_active_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record(
                    "source-repair-recovered",
                    title="D楼UPS故障检修",
                    building="D楼",
                    started=True,
                    target_record_id="",
                )
            ]

            result = service.query_records(scope="D", ongoing_items=[])

            self.assertEqual(
                [item["record_id"] for item in result["records"]],
                ["source-repair-recovered"],
            )
            self.assertEqual(result["records"][0]["source_progress"], "进行中")
            self.assertEqual(result["ongoing"], [])

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
                    "notice_type": "变更通告",
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
            result = service.query_records(scope="C", ongoing_items=[])
            self.assertEqual(result["ongoing"], [])

    def test_hidden_ongoing_uses_active_item_identity_for_qt_items(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service.hide_ongoing_item(
                {
                    "scope": "C",
                    "work_type": WORK_TYPE_CHANGE,
                    "notice_type": "变更通告",
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
                        "notice_type": "变更通告",
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
                        "notice_type": "变更通告",
                        "record_id": "target-a",
                        "title": "A楼变更",
                        "building": "A楼",
                        "building_codes": ["A"],
                    },
                    scope="C",
                )

            hidden_file = Path(tmp) / "lan_template_hidden_ongoing.json"
            self.assertFalse(hidden_file.exists())

    def test_validate_ongoing_delete_enriches_scope_from_qt_active_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._state_store.upsert_qt_active_item(
                {
                    "active_item_id": "active-a-delete",
                    "record_id": "target-a-delete",
                    "target_record_id": "target-a-delete",
                    "source_record_id": "source-a-delete",
                    "work_type": WORK_TYPE_CHANGE,
                    "notice_type": "变更通告",
                    "title": "A楼删除校验变更",
                    "building": "A楼",
                    "building_codes": ["A"],
                },
                origin="portal",
            )
            payload = {
                "active_item_id": "active-a-delete",
                "record_id": "source-a-delete",
                "target_record_id": "target-a-delete",
                "source_record_id": "source-a-delete",
                "work_type": WORK_TYPE_CHANGE,
                "notice_type": "变更通告",
            }

            keys = service.validate_ongoing_delete_item(payload, scope="A")

            self.assertIn("change:active:active-a-delete", keys)
            self.assertEqual(payload["record_id"], "target-a-delete")
            self.assertEqual(payload["building_codes"], ["A"])
            self.assertEqual(payload["building"], "A楼")

    def test_validate_ongoing_delete_keeps_rejecting_other_scope_after_enrich(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._state_store.upsert_qt_active_item(
                {
                    "active_item_id": "active-a-delete",
                    "record_id": "target-a-delete",
                    "target_record_id": "target-a-delete",
                    "work_type": WORK_TYPE_CHANGE,
                    "notice_type": "变更通告",
                    "title": "A楼删除校验变更",
                    "building": "A楼",
                    "building_codes": ["A"],
                },
                origin="portal",
            )

            with self.assertRaises(PortalError):
                service.validate_ongoing_delete_item(
                    {
                        "active_item_id": "active-a-delete",
                        "target_record_id": "target-a-delete",
                        "work_type": WORK_TYPE_CHANGE,
                        "notice_type": "变更通告",
                    },
                    scope="C",
                )

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
                            "notice_type": "变更通告",
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
      "target_record_id": "target-c1",
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

    def test_portal_auth_bulk_directory_grant_merges_existing_scopes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                manager = PortalAuthManager()
                manager.upsert_permission_user(
                    open_id="ou_directory_user",
                    name="目录人员",
                    scopes=["A"],
                    updated_by="ou_admin",
                )

                payload, changed, results = manager.bulk_merge_permission_users(
                    [
                        {
                            "open_id": "ou_directory_user",
                            "name": "目录人员新名称",
                            "scopes": ["B", "C"],
                        }
                    ],
                    updated_by="ou_admin",
                )

                by_open_id = {item["open_id"]: item for item in payload["users"]}
                self.assertEqual(
                    by_open_id["ou_directory_user"]["scopes"],
                    ["A", "B", "C"],
                )
                self.assertEqual(
                    by_open_id["ou_directory_user"]["name"],
                    "目录人员新名称",
                )
                self.assertEqual(changed, ["ou_directory_user"])
                self.assertEqual(results[0]["added_scopes"], ["B", "C"])

    def test_portal_auth_remove_permission_user_persists_and_protects_admin(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_data_path(name):
                return str(root / name)

            with patch(
                "lan_bitable_template_portal.portal_auth.get_data_file_path",
                side_effect=fake_data_path,
            ):
                manager = PortalAuthManager()
                manager.upsert_permission_user(
                    open_id="ou_remove_me",
                    name="待删除用户",
                    scopes=["A"],
                    updated_by="ou_admin",
                )

                payload, removed = manager.remove_permission_user(
                    "ou_remove_me",
                    updated_by="ou_admin",
                )

                self.assertTrue(removed)
                self.assertNotIn(
                    "ou_remove_me",
                    {item["open_id"] for item in payload["users"]},
                )
                self.assertNotIn(
                    "ou_remove_me",
                    {
                        item["open_id"]
                        for item in manager.get_permissions_payload()["users"]
                    },
                )
                locked = next(
                    item
                    for item in payload["users"]
                    if item.get("locked")
                )
                with self.assertRaisesRegex(PortalError, "固定管理员不能删除"):
                    manager.remove_permission_user(locked["open_id"])

    def test_permission_directory_extracts_position_buildings_and_specialties(self):
        person = MaintenancePortalService._permission_directory_person(
            {
                "record_id": "rec_directory_user",
                "raw_fields": {
                    "员工姓名": [{"id": "ou_directory", "name": "测试人员"}],
                    "机楼/专业": ["A楼", "C楼", "暖通"],
                    "离职/异动情况": False,
                },
                "display_fields": {
                    "姓名": "测试人员",
                    "员工工号": "90001",
                    "岗位": "运维工程师",
                    "机楼/专业": "A楼、C楼、暖通",
                },
            }
        )

        self.assertEqual(person["open_id"], "ou_directory")
        self.assertEqual(person["position"], "运维工程师")
        self.assertEqual(person["scopes"], ["A", "C"])
        self.assertEqual(person["specialties"], ["暖通"])
        self.assertTrue(person["selectable"])

        departed = MaintenancePortalService._permission_directory_person(
            {
                "record_id": "rec_departed",
                "raw_fields": {
                    "员工姓名": [{"id": "ou_departed", "name": "离职人员"}],
                    "机楼/专业": ["B楼", "电气"],
                    "离职/异动情况": True,
                },
                "display_fields": {"姓名": "离职人员", "岗位": "运维值班员"},
            }
        )
        self.assertFalse(departed["selectable"])
        self.assertEqual(departed["unavailable_reason"], "已离职或异动")

        no_table_building = MaintenancePortalService._permission_directory_person(
            {
                "record_id": "rec_no_table_building",
                "raw_fields": {
                    "员工姓名": [{"id": "ou_no_table_building", "name": "未配机楼人员"}],
                    "机楼/专业": ["消防"],
                    "离职/异动情况": False,
                },
                "display_fields": {"姓名": "未配机楼人员", "岗位": "运维工程师"},
            }
        )
        self.assertTrue(no_table_building["selectable"])
        self.assertEqual(no_table_building["scopes"], [])

    def test_permission_directory_hides_shift_leader_and_operator_positions(self):
        service = _TestMaintenancePortalService()
        service._load_table_fields = lambda **_kwargs: (  # type: ignore[method-assign]
            [],
            {"姓名": {}, "员工姓名": {}, "机楼/专业": {}, "岗位": {}},
        )
        service._load_table_records = lambda **_kwargs: [  # type: ignore[method-assign]
            {
                "record_id": "rec_shift_leader",
                "raw_fields": {
                    "员工姓名": [{"id": "ou_leader", "name": "值班长"}],
                    "机楼/专业": ["A楼", "电气"],
                },
                "display_fields": {"姓名": "值班长", "岗位": "运维值班长"},
            },
            {
                "record_id": "rec_shift_operator",
                "raw_fields": {
                    "员工姓名": [{"id": "ou_operator", "name": "值班员"}],
                    "机楼/专业": ["B楼", "暖通"],
                },
                "display_fields": {"姓名": "值班员", "岗位": "运维值班员"},
            },
            {
                "record_id": "rec_engineer",
                "raw_fields": {
                    "员工姓名": [{"id": "ou_engineer", "name": "工程师"}],
                    "机楼/专业": ["C楼", "消防"],
                },
                "display_fields": {"姓名": "工程师", "岗位": "运维工程师"},
            },
        ]

        payload = service.get_permission_directory_people(force_refresh=True)

        self.assertEqual(
            [item["record_id"] for item in payload["items"]],
            ["rec_engineer"],
        )
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["selectable_total"], 1)

    def test_permission_directory_grants_merge_duplicate_person_records(self):
        service = _TestMaintenancePortalService()
        service.get_permission_directory_people = lambda **_kwargs: {  # type: ignore[method-assign]
            "items": [
                {
                    "record_id": "rec_a",
                    "open_id": "ou_same",
                    "name": "同一人员",
                    "scopes": ["A"],
                    "selectable": True,
                },
                {
                    "record_id": "rec_b",
                    "open_id": "ou_same",
                    "name": "同一人员",
                    "scopes": ["B"],
                    "selectable": True,
                },
            ]
        }

        result = service.resolve_permission_directory_grants(
            ["rec_a", "rec_b"],
            ["C", "H"],
        )

        self.assertEqual(result["selected_count"], 2)
        self.assertEqual(result["failed"], [])
        self.assertEqual(len(result["grants"]), 1)
        self.assertEqual(result["grants"][0]["scopes"], ["C", "H"])
        self.assertEqual(result["grants"][0]["record_ids"], ["rec_a", "rec_b"])

        with self.assertRaisesRegex(PortalError, "至少选择一个需要授予的楼栋"):
            service.resolve_permission_directory_grants(["rec_a"], [])

    def test_portal_auth_permission_request_admin_approves_user(self):
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
                self.assertEqual(
                    manager.get_current_permission_request("ou_requester")["request_id"],
                    request["request_id"],
                )
                self.assertEqual(request["requested_scopes"], ["A", "CAMPUS"])
                stored = manager._state_store.get_permission_request(request["request_id"])
                self.assertNotIn("code", stored)
                self.assertEqual(stored.get("code_hash"), "")

                with self.assertRaises(PortalError):
                    manager.approve_permission_request(
                        "missing-request",
                        scopes=["A"],
                        updated_by="ou_admin",
                    )

                result = manager.approve_permission_request(
                    request["request_id"],
                    scopes=["A", "CAMPUS"],
                    updated_by="ou_admin",
                )
                self.assertEqual(result["request"]["status"], "approved")
                self.assertEqual(
                    manager.scopes_for_open_id("ou_requester"), ["A", "CAMPUS"]
                )
                approved = manager._state_store.get_permission_request(
                    request["request_id"]
                )
                self.assertEqual(approved["approved_scopes"], ["A", "CAMPUS"])
                self.assertEqual(approved["status"], "approved")
                self.assertEqual(manager.get_current_permission_request("ou_requester"), {})

    def test_portal_auth_permission_request_replaces_pending_request(self):
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
                new_request = manager.create_permission_request(
                    open_id="ou_pending",
                    name="申请人",
                    scopes=["C"],
                    reason="第二次",
                )
                current = manager.get_current_permission_request("ou_pending")
                self.assertEqual(current["request_id"], new_request["request_id"])
                self.assertEqual(
                    manager._state_store.get_permission_request(old_request["request_id"])[
                        "status"
                    ],
                    "superseded",
                )
                with self.assertRaises(PortalError):
                    manager.approve_permission_request(
                        old_request["request_id"],
                        scopes=["B"],
                        updated_by="ou_admin",
                    )

                rejected_request = manager.create_permission_request(
                    open_id="ou_rejected",
                    name="被拒绝申请人",
                    scopes=["D"],
                    reason="待拒绝",
                )
                manager.reject_permission_request(
                    rejected_request["request_id"],
                    reason="不需要",
                    updated_by="ou_admin",
                )
                rejected = manager._state_store.get_permission_request(
                    rejected_request["request_id"]
                )
                self.assertEqual(rejected["status"], "rejected")
                self.assertEqual(manager.scopes_for_open_id("ou_rejected"), [])

    def test_portal_auth_permission_request_notify_failure_does_not_grant_scope(self):
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
                failed_request = manager.create_permission_request(
                    open_id="ou_retry",
                    name="申请人",
                    scopes=["C"],
                    reason="通知失败",
                )
                manager.mark_permission_request_notify_failed(failed_request["request_id"])
                self.assertEqual(manager.get_current_permission_request("ou_retry"), {})
                self.assertEqual(manager.scopes_for_open_id("ou_retry"), [])
                self.assertEqual(
                    manager._state_store.get_permission_request(old_request["request_id"])[
                        "status"
                    ],
                    "superseded",
                )

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
                manager.approve_permission_request(
                    request["request_id"],
                    updated_by="ou_admin",
                )
                self.assertEqual(manager.scopes_for_open_id("ou_disabled_apply"), ["E"])

    def test_portal_auth_permission_request_adds_missing_scopes(self):
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
                            "open_id": "ou_add_scope",
                            "name": "追加楼栋",
                            "role": "building",
                            "scopes": ["A"],
                            "enabled": True,
                        }
                    ],
                    updated_by="ou_actor",
                )
                request = manager.create_permission_request(
                    open_id="ou_add_scope",
                    name="追加楼栋",
                    scopes=["A", "B"],
                    reason="临时支援B楼",
                )
                self.assertEqual(request["requested_scopes"], ["B"])
                manager.approve_permission_request(
                    request["request_id"],
                    updated_by="ou_admin",
                )
                self.assertEqual(manager.scopes_for_open_id("ou_add_scope"), ["A", "B"])

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
            PortalRuntime._safe_host_value("127.0.0.1:18766", "fallback:1"),
            "127.0.0.1:18766",
        )
        self.assertEqual(
            PortalRuntime._safe_host_value("[::1]:18766", "fallback:1"),
            "[::1]:18766",
        )
        self.assertEqual(
            PortalRuntime._safe_host_value("evil.example\r\nX-Test: 1", "fallback:1"),
            "fallback:1",
        )
        self.assertEqual(
            PortalRuntime._safe_host_value("evil.example/path", "fallback:1"),
            "fallback:1",
        )
        self.assertEqual(
            PortalRuntime._safe_host_value("evil.example@127.0.0.1", "fallback:1"),
            "fallback:1",
        )
        self.assertEqual(
            PortalRuntime._safe_host_value("0.0.0.0:18766", "fallback:1"),
            "fallback:1",
        )
        self.assertEqual(PortalRuntime._safe_proto_value("https, http"), "https")
        self.assertEqual(PortalRuntime._safe_proto_value("javascript"), "http")

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

    def test_formatted_number_fields_remain_editable(self):
        service = _TestMaintenancePortalService()
        metas = service._parse_field_metas(
            [
                {
                    "field_id": "fld-progress",
                    "field_name": "维修进度",
                    "ui_type": "Number",
                    "type": 2,
                    "property": {"formatter": "0%"},
                },
                {
                    "field_id": "fld-cost",
                    "field_name": "故障维修总费用",
                    "ui_type": "Number",
                    "type": 2,
                    "property": {"formatter": "0.00"},
                },
                {
                    "field_id": "fld-formula",
                    "field_name": "公式结果",
                    "ui_type": "Formula",
                    "type": 20,
                    "property": {
                        "formula_expression": "1 + 1",
                        "formatter": "0",
                    },
                },
                {
                    "field_id": "fld-lookup",
                    "field_name": "查找结果",
                    "ui_type": "Lookup",
                    "type": 19,
                    "property": {},
                },
            ]
        )
        by_name = {meta.field_name: meta for meta in metas}
        self.assertFalse(by_name["维修进度"].has_formula)
        self.assertFalse(service._field_meta_is_readonly(by_name["维修进度"]))
        self.assertFalse(service._field_meta_is_readonly(by_name["故障维修总费用"]))
        self.assertTrue(service._field_meta_is_readonly(by_name["公式结果"]))
        self.assertTrue(service._field_meta_is_readonly(by_name["查找结果"]))

    def test_repair_management_form_only_exposes_manual_fields(self):
        service = _TestMaintenancePortalService()

        def meta(name, field_type=1, ui_type="Text"):
            return FieldMeta(
                f"fld_{name}",
                name,
                ui_type,
                field_type,
                False,
                {},
                [],
                False,
            )

        manual_names = {
            "故障发生时间",
            "故障维修原因",
            "故障发生现象描述",
            "所属专业",
            "所属数据中心/楼栋-使用",
        }
        derived_names = {
            "区域",
            "数据中心",
            "专业（推送消息用）",
            "事件描述",
            "检修通告名称",
            "当前维修进度",
        }
        payloads = {
            name: service._repair_management_field_payload(meta(name))
            for name in manual_names | derived_names
        }
        self.assertEqual(
            {name for name, payload in payloads.items() if payload["editable"]},
            manual_names,
        )
        self.assertTrue(all(payloads[name]["auto_filled"] for name in derived_names))

    def test_repair_followup_form_exposes_complete_business_fields_only(self):
        service = _TestMaintenancePortalService()
        expected = {
            "设备名称",
            "设备编号",
            "设备品牌",
            "设备型号",
            "维修方",
            "供应商名称",
            "供应商维修人员",
            "设备生产日期",
            "设备使用年限",
            "设备容量KW/AH",
            "是否质保期内",
            "随工人员（我方维修人员）",
            "更换备件名称",
            "更换备件数量",
            "维修进展描述",
            "维修进度",
            "故障维修总费用",
            "跟进项（如有）",
            "后续整改措施（如有）",
        }
        hidden = {
            "是否本维修单第一次提交跟进记录",
            "费用举证（如：发票/支付记录）",
            "超链接",
            "维修汇总记录ID",
            "CMDB设备唯一ID",
        }
        payloads = {}
        for name in expected | hidden:
            field_type, ui_type = (2, "Number") if name in {
                "设备使用年限",
                "更换备件数量",
                "维修进度",
                "故障维修总费用",
            } else (1, "Text")
            meta = FieldMeta(
                f"fld_{name}",
                name,
                ui_type,
                field_type,
                False,
                {},
                [],
                False,
            )
            payloads[name] = service._repair_followup_field_payload(meta)
        self.assertEqual(
            {name for name, payload in payloads.items() if payload["editable"]},
            expected,
        )

    def test_new_repair_followup_applies_editable_equipment_defaults(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta("fld_parent", REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME, "Text", 1, False, {}, [], False),
            FieldMeta("fld_date", "设备生产日期", "DateTime", 5, False, {}, [], False),
            FieldMeta("fld_years", "设备使用年限", "Number", 2, False, {}, [], False),
            FieldMeta("fld_capacity", "设备容量KW/AH", "Text", 1, False, {}, [], False),
        ]
        prepared, warnings = service._prepare_repair_followup_fields(
            summary_record_id="rec_summary_defaults",
            fields={},
            summary_record={"display_fields": {}, "raw_fields": {}},
            meta_by_name={meta.field_name: meta for meta in metas},
        )

        self.assertEqual(warnings, [])
        self.assertEqual(prepared["设备生产日期"], 1617120000000)
        self.assertEqual(prepared["设备使用年限"], 4)
        self.assertEqual(prepared["设备容量KW/AH"], "/")

    def test_repair_followup_duplicate_field_migration_is_disabled_for_active_table(self):
        service = _TestMaintenancePortalService()

        def raw_field(field_id, name, field_type, options=()):
            return {
                "field_id": field_id,
                "field_name": name,
                "type": field_type,
                "ui_type": "SingleSelect" if field_type == 3 else "Text",
                "property": {
                    "options": [
                        {"id": option_id, "name": option_name, "color": index}
                        for index, (option_id, option_name) in enumerate(options)
                    ]
                }
                if field_type == 3
                else {},
            }

        raw_fields = [
            raw_field("old_name", "设备名称-1", 3, [("opt_name", "铅酸阀控蓄电池")]),
            raw_field("new_name", "设备名称", 1),
            raw_field("old_no", "设备编号-1", 1),
            raw_field("new_no", "设备编号", 1),
            raw_field("old_brand", "设备品牌 -1", 3, [("opt_brand", "双登")]),
            raw_field("new_brand", "设备品牌", 1),
            raw_field("old_model", "设备型号 -1", 3, [("opt_model", "GFMHR-1250W")]),
            raw_field("new_model", "设备型号", 1),
            raw_field("old_party", "维修方 -1", 3, [("opt_party", "我方")]),
            raw_field("new_party", "维修方", 3, [("opt_bad", "opt_party")]),
            raw_field("old_supplier", "供应商名称 -1", 1),
            raw_field("new_supplier", "供应商名称", 1),
            raw_field("old_person", "供应商维修人员-1", 1),
            raw_field("new_person", "供应商维修人员", 1),
            raw_field("warranty", "是否质保期内", 3, [("opt_yes", "是"), ("opt_no", "否")]),
        ]
        records = [
            {
                "record_id": "rec_followup",
                "display_fields": {
                    "设备名称-1": "铅酸阀控蓄电池",
                    "设备名称": "opt_name",
                    "设备编号-1": "A-446-BAT-01",
                    "设备编号": "A-446-BAT-01",
                    "设备品牌 -1": "双登",
                    "设备品牌": "opt_brand",
                    "设备型号 -1": "GFMHR-1250W",
                    "设备型号": "opt_model",
                    "维修方 -1": "我方",
                    "维修方": "opt_party",
                    "供应商名称 -1": "测试供应商",
                    "供应商维修人员-1": "测试工程师",
                    "是否质保期内": "是",
                },
                "raw_fields": {},
            }
        ]
        runtime = {}

        class RuntimeStore:
            @staticmethod
            def get_backend_runtime(key):
                return runtime.get(key)

            @staticmethod
            def put_backend_runtime(key, value):
                runtime[key] = dict(value)

        service._state_store = RuntimeStore()  # type: ignore[assignment]

        def load_fields(**_kwargs):
            metas = service._parse_field_metas(raw_fields)
            return metas, {meta.field_name: meta for meta in metas}

        service._load_table_fields = load_fields  # type: ignore[method-assign]
        service._request_json = (  # type: ignore[method-assign]
            lambda path, **_kwargs: {
                "data": {"items": raw_fields if path == "fields" else []}
            }
        )
        service._load_table_records = (  # type: ignore[method-assign]
            lambda **_kwargs: records
        )

        def patch_record_fields(**kwargs):
            record = records[0]
            record["display_fields"].update(kwargs["fields"])
            record["raw_fields"].update(kwargs["fields"])
            return {}

        def update_field(raw, *, field_type, options=None):
            raw["type"] = field_type
            raw["ui_type"] = "SingleSelect" if field_type == 3 else "Text"
            if field_type == 3:
                raw["property"] = {
                    "options": [
                        {
                            **option,
                            "id": option.get("id") or f"generated_{index}",
                        }
                        for index, option in enumerate(options or [])
                    ]
                }

        failed_delete_once = {"设备名称-1": False}

        def delete_field(raw):
            if (
                raw["field_name"] == "设备名称-1"
                and not failed_delete_once["设备名称-1"]
            ):
                failed_delete_once["设备名称-1"] = True
                raise PortalError("mock transient delete failure")
            raw_fields.remove(raw)
            name = raw["field_name"]
            records[0]["display_fields"].pop(name, None)
            records[0]["raw_fields"].pop(name, None)

        service._patch_record_fields = patch_record_fields  # type: ignore[method-assign]
        service._update_repair_followup_field_definition = update_field  # type: ignore[method-assign]
        service._delete_repair_followup_field_definition = delete_field  # type: ignore[method-assign]

        result = service.migrate_repair_followup_duplicate_fields()
        second = service.migrate_repair_followup_duplicate_fields()

        self.assertEqual(result["status"], "not_required")
        self.assertEqual(second["status"], "not_required")
        remaining_names = {field["field_name"] for field in raw_fields}
        self.assertIn("设备名称-1", remaining_names)
        self.assertIn("设备品牌 -1", remaining_names)
        self.assertEqual(records[0]["display_fields"]["设备名称"], "opt_name")
        self.assertEqual(records[0]["display_fields"]["设备品牌"], "opt_brand")
        self.assertEqual(records[0]["display_fields"]["设备型号"], "opt_model")
        self.assertEqual(records[0]["display_fields"]["维修方"], "opt_party")
        self.assertEqual(records[0]["display_fields"]["是否质保期内"], "是")

    def test_repair_management_progress_field_matches_followup_progress_format(self):
        service = _TestMaintenancePortalService()
        summary_meta = FieldMeta(
            "fld_summary_progress",
            "当前维修进度",
            "Number",
            2,
            False,
            {},
            [],
            False,
            {"formatter": "0.0"},
        )
        followup_meta = FieldMeta(
            "fld_followup_progress",
            "维修进度",
            "Progress",
            2,
            False,
            {},
            [],
            False,
            {"formatter": "0%", "max": 1, "min": 0, "range_customize": False},
        )
        updates = []
        runtime = {}

        class RuntimeStore:
            @staticmethod
            def put_backend_runtime(key, value):
                runtime[key] = dict(value)

        service._state_store = RuntimeStore()  # type: ignore[assignment]
        service._update_bitable_field_definition = (  # type: ignore[method-assign]
            lambda raw, **kwargs: updates.append((dict(raw), dict(kwargs)))
        )

        changed = service._ensure_repair_management_progress_field_definition(
            {"当前维修进度": summary_meta},
            {"维修进度": followup_meta},
        )
        repeated = service._ensure_repair_management_progress_field_definition(
            {"当前维修进度": summary_meta},
            {"维修进度": followup_meta},
        )

        self.assertTrue(changed)
        self.assertFalse(repeated)
        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0][1]["ui_type"], "Progress")
        self.assertEqual(updates[0][1]["property_payload"], followup_meta.property)
        self.assertTrue(runtime)

    def test_repair_followup_people_searches_by_name_and_hides_open_id(self):
        service = _TestMaintenancePortalService()
        service._load_signature_people = (  # type: ignore[method-assign]
            lambda force=False: [
                {
                    "record_id": "rec_zhang",
                    "name": "张宇航",
                    "open_id": "ou_zhang",
                    "employee_no": "10001",
                    "building": "A楼",
                    "position": "运维值班员",
                },
                {
                    "record_id": "rec_li",
                    "name": "李世龙",
                    "open_id": "ou_li",
                    "employee_no": "10002",
                    "building": "A楼",
                    "position": "运维值班长",
                },
            ]
        )

        payload = service.list_repair_followup_people(
            scope="A",
            query="张宇",
            limit=20,
        )

        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["people"][0]["name"], "张宇航")
        self.assertEqual(payload["people"][0]["user_id"], "ou_zhang")
        self.assertNotIn("open_id", payload["people"][0])

    def test_repair_followup_coerces_selected_people_to_user_field(self):
        meta = FieldMeta(
            "fld_workers",
            "随工人员（我方维修人员）",
            "User",
            11,
            False,
            {},
            [],
            False,
        )

        prepared, warnings = MaintenancePortalService._coerce_repair_management_fields(
            {"随工人员（我方维修人员）": [{"id": "ou_zhang", "name": "张宇航"}]},
            {meta.field_name: meta},
        )

        self.assertEqual(prepared[meta.field_name], [{"id": "ou_zhang"}])
        self.assertEqual(warnings, [])

    def test_repair_management_form_derives_only_active_fields(self):
        service = _TestMaintenancePortalService()
        legacy_building = FieldMeta(
            "fld_legacy_building",
            "所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）",
            "SingleSelect",
            3,
            False,
            {},
            ["南通A楼", "南通E楼"],
            False,
        )
        metas = {
            legacy_building.field_name: legacy_building,
            "专业（推送消息用）": FieldMeta(
                "fld_push_specialty", "专业（推送消息用）", "Text", 1, False, {}, [], False
            ),
            "区域": FieldMeta("fld_region", "区域", "Text", 1, False, {}, [], False),
            "数据中心": FieldMeta("fld_dc", "数据中心", "Text", 1, False, {}, [], False),
        }
        derived = service._apply_repair_management_form_derivatives(
            {
                "所属专业": "电气",
                "所属数据中心/楼栋-使用": "A楼",
            },
            metas,
        )
        self.assertEqual(derived["专业（推送消息用）"], "电气")
        self.assertNotIn("区域", derived)
        self.assertNotIn("数据中心", derived)
        self.assertEqual(
            derived["所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）"],
            "南通A楼",
        )

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
        self.assertEqual(
            fields[MAINTENANCE_NOTICE_FIELDS["building"]],
            ["A楼"],
        )
        self.assertEqual(
            fields[MAINTENANCE_NOTICE_FIELDS["specialty"]],
            "电气",
        )

    def test_maintenance_target_select_fields_are_normalized_for_create_and_update(self):
        handler = MaintenanceNoticeHandler("维保通告")
        create_fields = handler.build_create_fields(
            NoticePayload(
                text=(
                    "【维保通告】状态：开始\n"
                    "【名称】EA118机房A楼B楼测试维保\n"
                    "【时间】2026-05-15 09:30~2026-05-15 18:30"
                ),
                buildings=["南通A楼、B栋"],
                specialty="电气专业",
            )
        )
        update_fields = handler.build_update_fields(
            NoticePayload(
                text=(
                    "【维保通告】状态：更新\n"
                    "【名称】EA118机房110站测试维保\n"
                    "【时间】2026-05-15 09:30~2026-05-15 18:30"
                ),
                buildings=["110"],
                specialty="暖通",
            )
        )

        self.assertEqual(
            create_fields[MAINTENANCE_NOTICE_FIELDS["building"]],
            ["A楼", "B楼"],
        )
        self.assertEqual(
            create_fields[MAINTENANCE_NOTICE_FIELDS["specialty"]],
            "电气",
        )
        self.assertEqual(
            update_fields[MAINTENANCE_NOTICE_FIELDS["building"]],
            ["110站"],
        )
        self.assertEqual(
            update_fields[MAINTENANCE_NOTICE_FIELDS["specialty"]],
            "暖通",
        )

    def test_maintenance_target_select_fields_use_only_writable_options(self):
        handler = MaintenanceNoticeHandler("维保通告")
        fields = handler.build_create_fields(
            NoticePayload(
                text=(
                    "【维保通告】状态：开始\n"
                    "【名称】EA118机房园区测试维保\n"
                    "【时间】2026-05-15 09:30~2026-05-15 18:30"
                ),
                buildings=["园区（ABCDE楼）"],
                specialty="/",
            )
        )

        self.assertEqual(
            fields[MAINTENANCE_NOTICE_FIELDS["building"]],
            ["A楼", "B楼", "C楼", "D楼", "E楼"],
        )
        self.assertEqual(
            fields[MAINTENANCE_NOTICE_FIELDS["specialty"]],
            "其他",
        )
        self.assertNotIn(
            "/",
            ScreenshotConfirmDialog._building_options_for_notice("维保通告"),
        )
        self.assertNotIn(
            "/",
            ScreenshotConfirmDialog._specialty_options_for_notice("维保通告"),
        )

    def test_maintenance_payload_uses_building_codes_for_multi_select(self):
        payload = PortalRuntime._prepared_to_notice_payload(
            {
                "notice_type": "维保通告",
                "text": "【维保通告】状态：开始\n【名称】测试维保",
                "building": "A楼、B楼",
                "building_codes": ["A", "B"],
                "specialty": "消防",
            }
        )

        self.assertEqual(payload.buildings, ["A楼", "B楼"])
        self.assertEqual(payload.specialty, "消防")

    def test_paired_maintenance_payload_keeps_all_selected_buildings(self):
        service = _TestMaintenancePortalService()
        paired = service._build_paired_maintenance_upload(
            request_payload={"scope": "A", "operation_id": "paired-multi"},
            job_id="job-paired-multi",
            action="start",
            status="开始",
            source_record_id="source-paired-multi",
            target_record_id="",
            active_item_id="active-paired-multi",
            original_title="EA118机房A楼B楼测试维保",
            building="A楼、B楼",
            building_codes=["A", "B"],
            specialty="消防",
            maintenance_cycle="每月",
            start_time="2026-05-15T09:30",
            end_time="2026-05-15T18:30",
            location="A楼、B楼",
            content="测试",
            reason="测试",
            impact="无",
            progress="准备开始",
            response_time="2026-05-15 09:30",
        )
        payload = PortalRuntime._prepared_to_notice_payload(paired)
        fields = MaintenanceNoticeHandler("维保通告").build_create_fields(payload)

        self.assertEqual(paired["buildings"], ["A楼", "B楼"])
        self.assertEqual(payload.buildings, ["A楼", "B楼"])
        self.assertEqual(
            fields[MAINTENANCE_NOTICE_FIELDS["building"]],
            ["A楼", "B楼"],
        )
        self.assertEqual(
            fields[MAINTENANCE_NOTICE_FIELDS["specialty"]],
            "消防",
        )

    def test_maintenance_undo_restore_preserves_select_field_shapes(self):
        fields = PortalRuntime._undo_restore_fields(
            "维保通告",
            {
                "楼栋": ["A楼", "B楼"],
                "专业": "暖通",
                "不存在字段": "不应恢复",
            },
        )

        self.assertEqual(fields["楼栋"], ["A楼", "B楼"])
        self.assertEqual(fields["专业"], "暖通")
        self.assertNotIn("不存在字段", fields)

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

        self.assertEqual(list(PORTAL_MAINTENANCE_CYCLE_OPTIONS), qt_options)

    def test_qt_screenshot_confirm_enables_when_specialty_is_selected_last(self):
        from PyQt6.QtWidgets import QApplication

        app = QApplication.instance() or QApplication([])
        dialog = ScreenshotConfirmDialog(theme="dark")
        try:
            for notice_type in (
                "维保通告",
                "变更通告",
                "设备检修",
                "设备调整",
                "设备轮巡",
                "上电通告",
                "下电通告",
                "事件通告",
            ):
                with self.subTest(notice_type=notice_type):
                    data = {
                        "notice_type": notice_type,
                        "text": f"【{notice_type}】状态：开始\n【名称】EA118机房A楼测试通告",
                        "buildings": ["A楼"],
                    }
                    if notice_type == "维保通告":
                        data["maintenance_cycle"] = "每月"
                    dialog.set_data(data, action_type="upload")
                    if notice_type == "事件通告":
                        dialog.event_level_combo.setCurrentIndex(1)
                        dialog._on_event_level_selected(1)
                        dialog.event_source_combo.setCurrentIndex(1)
                        dialog._on_event_source_selected(1)
                    dialog.time_input.setText("2026-07-22 10:30")

                    self.assertFalse(dialog.btn_confirm.isEnabled())
                    self.assertIn("专业", dialog.btn_confirm.toolTip())

                    specialty_index = dialog.specialty_combo.findText("电气")
                    self.assertGreater(specialty_index, 0)
                    dialog.specialty_combo.setCurrentIndex(specialty_index)
                    dialog._on_specialty_selected(specialty_index)
                    app.processEvents()

                    self.assertTrue(dialog.btn_confirm.isEnabled())
                    self.assertEqual(dialog.btn_confirm.toolTip(), "")
        finally:
            dialog.close()
            dialog.deleteLater()
            app.processEvents()

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
        acquired = PortalRuntime.source_refresh_run_lock.acquire(blocking=False)
        self.assertTrue(acquired)
        try:
            with patch.dict(os.environ, {"CLIPFLOW_BACKEND_MOCK_EXTERNAL": ""}, clear=False):
                result = PortalRuntime.refresh_sources_once(force=True)
            self.assertFalse(result["refreshed"])
            self.assertTrue(result["source_refresh_inflight"])
            self.assertTrue(result["source_refresh_reused"])
        finally:
            PortalRuntime.source_refresh_run_lock.release()

    def test_source_refresh_failure_records_failed_snapshot_manifest(self):
        class _FailingSourceRefreshService:
            _load_warnings: list[str] = []

            def _snapshot_meta(self):
                return {"warnings": list(self._load_warnings)}

            def refresh(self):
                raise RuntimeError("boom")

            def process_due_repair_link_tasks(self, *, limit):
                return {"processed": 0}

        original_store = PortalRuntime.state_store
        original_service = PortalRuntime.service
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            PortalRuntime.state_store = store
            PortalRuntime.service = _FailingSourceRefreshService()
            try:
                with patch.dict(os.environ, {"CLIPFLOW_BACKEND_MOCK_EXTERNAL": ""}, clear=False):
                    result = PortalRuntime.refresh_sources_once(force=True)
                stats = store.source_snapshot_stats()
            finally:
                PortalRuntime.state_store = original_store
                PortalRuntime.service = original_service
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
        original_store = PortalRuntime.state_store
        original_service = PortalRuntime.service
        try:
            with tempfile.TemporaryDirectory() as tmp:
                service = self._new_temp_service(Path(tmp))
                PortalRuntime.state_store = service._state_store
                PortalRuntime.service = service
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
                    PortalRuntime._runtime_queue_job_processable("message", second_job_id)
                )
                self.assertEqual(
                    service.get_job(second_job_id)["depends_on_phase"], "accepted"
                )

                service.mark_job(first_job_id, phase="success")
                self.assertTrue(
                    PortalRuntime._runtime_queue_job_processable("message", second_job_id)
                )
                self.assertEqual(service.get_job(second_job_id)["depends_on_phase"], "")
        finally:
            PortalRuntime.state_store = original_store
            PortalRuntime.service = original_service

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
        original_store = PortalRuntime.state_store
        original_count = PortalRuntime.message_worker_count
        original_upload_seconds = getattr(PortalRuntime, "upload_seconds_per_record", 2.0)
        original_batch_wait = getattr(PortalRuntime, "upload_batch_wait_seconds", 5.0)
        original_batch_max = getattr(PortalRuntime, "upload_batch_max_records", 5)
        original_qt_interval = getattr(PortalRuntime, "qt_action_interval_ms", 250)
        original_source_defer = getattr(PortalRuntime, "source_refresh_defer_when_busy", True)
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
            PortalRuntime.state_store = store
            try:
                limits = PortalRuntime.apply_runtime_settings()
            finally:
                PortalRuntime.state_store = original_store
                PortalRuntime.message_worker_count = original_count
                PortalRuntime.upload_seconds_per_record = original_upload_seconds
                PortalRuntime.upload_batch_wait_seconds = original_batch_wait
                PortalRuntime.upload_batch_max_records = original_batch_max
                PortalRuntime.qt_action_interval_ms = original_qt_interval
                PortalRuntime.source_refresh_defer_when_busy = original_source_defer
            self.assertEqual(limits["message_worker_count"], 5)
            self.assertEqual(limits["upload_seconds_per_record"], 0.5)
            self.assertEqual(limits["upload_batch_wait_seconds"], 30.0)
            self.assertEqual(limits["upload_batch_max_records"], 99)
            self.assertEqual(limits["qt_tick_interval_ms"], 100)

    def test_source_refresh_defers_when_runtime_busy(self):
        original_store = PortalRuntime.state_store
        original_service = PortalRuntime.service
        original_defer = getattr(PortalRuntime, "source_refresh_defer_when_busy", True)
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            service = _NativeFastAPIRouteService()
            PortalRuntime.state_store = store
            PortalRuntime.service = service
            PortalRuntime.source_refresh_defer_when_busy = True
            try:
                store.upsert_runtime_queue_item("qt_action", "job-busy")
                with patch.dict(os.environ, {"CLIPFLOW_BACKEND_MOCK_EXTERNAL": ""}, clear=False):
                    result = PortalRuntime.refresh_sources_once(
                        force=True,
                        defer_if_busy=True,
                    )
            finally:
                PortalRuntime.state_store = original_store
                PortalRuntime.service = original_service
                PortalRuntime.source_refresh_defer_when_busy = original_defer
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

        original_service = PortalRuntime.service
        service = _SlowRepairRefreshService()
        with PortalRuntime.repair_refresh_lock:
            PortalRuntime.repair_refresh_inflight = False
            PortalRuntime.repair_refresh_event = threading.Event()
            PortalRuntime.repair_refresh_last_result = {}
            PortalRuntime.repair_refresh_last_error = ""
            PortalRuntime.repair_refresh_last_finished = 0.0
        PortalRuntime.service = service
        try:
            first = PortalRuntime.request_repair_source_refresh()
            self.assertTrue(first["repair_refresh_started"])
            self.assertTrue(service.entered.wait(timeout=2))
            second = PortalRuntime.request_repair_source_refresh()
            self.assertFalse(second["repair_refresh_started"])
            self.assertTrue(second["repair_refresh_inflight"])
            service.release.set()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                with PortalRuntime.repair_refresh_lock:
                    if not PortalRuntime.repair_refresh_inflight:
                        break
                time.sleep(0.02)
            with PortalRuntime.repair_refresh_lock:
                self.assertFalse(PortalRuntime.repair_refresh_inflight)
                self.assertEqual(PortalRuntime.repair_refresh_last_result["repair_count"], 3)
            self.assertEqual(service.calls, 1)
        finally:
            service.release.set()
            PortalRuntime.service = original_service
            with PortalRuntime.repair_refresh_lock:
                PortalRuntime.repair_refresh_inflight = False
                PortalRuntime.repair_refresh_event = threading.Event()
                PortalRuntime.repair_refresh_last_result = {}
                PortalRuntime.repair_refresh_last_error = ""
                PortalRuntime.repair_refresh_last_finished = 0.0

    def test_change_source_refresh_request_is_blocking_singleflight(self):
        class _SlowChangeRefreshService:
            _load_warnings: list[str] = []

            def __init__(self):
                self.calls = 0
                self.entered = threading.Event()
                self.release = threading.Event()

            def refresh_change_source(self):
                self.calls += 1
                self.entered.set()
                self.release.wait(timeout=5)
                return {
                    "change_count": 4,
                    "zhihang_change_count": 2,
                    "change_refreshed_at": "17:02",
                }

        original_service = PortalRuntime.service
        service = _SlowChangeRefreshService()
        with PortalRuntime.change_refresh_lock:
            PortalRuntime.change_refresh_inflight = False
            PortalRuntime.change_refresh_event = threading.Event()
            PortalRuntime.change_refresh_last_result = {}
            PortalRuntime.change_refresh_last_error = ""
            PortalRuntime.change_refresh_last_finished = 0.0
        PortalRuntime.service = service
        results = []
        errors = []

        def _call_refresh():
            try:
                results.append(PortalRuntime.request_change_source_refresh())
            except Exception as exc:
                errors.append(str(exc))

        try:
            first_thread = threading.Thread(target=_call_refresh, daemon=True)
            first_thread.start()
            self.assertTrue(service.entered.wait(timeout=2))
            second_thread = threading.Thread(target=_call_refresh, daemon=True)
            second_thread.start()
            time.sleep(0.05)
            with PortalRuntime.change_refresh_lock:
                self.assertTrue(PortalRuntime.change_refresh_inflight)
            service.release.set()
            first_thread.join(timeout=2)
            second_thread.join(timeout=2)
            self.assertFalse(errors)
            self.assertEqual(service.calls, 1)
            self.assertEqual(len(results), 2)
            self.assertTrue(any(item.get("change_refresh_reused") for item in results))
            self.assertTrue(any(item.get("change_refresh_started") for item in results))
            self.assertEqual(results[0]["change_count"], 4)
        finally:
            service.release.set()
            PortalRuntime.service = original_service
            with PortalRuntime.change_refresh_lock:
                PortalRuntime.change_refresh_inflight = False
                PortalRuntime.change_refresh_event = threading.Event()
                PortalRuntime.change_refresh_last_result = {}
                PortalRuntime.change_refresh_last_error = ""
                PortalRuntime.change_refresh_last_finished = 0.0

    def test_refresh_change_source_preserves_other_snapshot_records(self):
        class _ChangeRefreshService(MaintenancePortalService):
            def _load_change_fields(self):
                self._change_field_meta_list = []
                self._change_field_meta_by_name = {}
                return []

            def _load_change_records(self):
                self._change_records = [
                    _build_change_record("c-new", building="A楼", progress="未开始")
                ]
                self._change_loaded_once = True
                return self._change_records

            def _load_zhihang_change_fields(self):
                self._zhihang_change_field_meta_list = []
                self._zhihang_change_field_meta_by_name = {}
                return []

            def _load_zhihang_change_records(self):
                self._zhihang_change_records = [
                    _build_zhihang_change_record("z-new", title="EA118机房A楼割接")
                ]
                self._zhihang_change_loaded_once = True
                return self._zhihang_change_records

        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), _ChangeRefreshService)
            service._state_store.replace_all_source_scope_snapshots(
                {
                    "ALL": {
                        "records": [
                            _build_record("m-old", "A楼", "维护", _TEST_MONTH_LABEL),
                            _build_repair_record("r-old", building="A楼"),
                        ],
                        "zhihang_records": [],
                    }
                },
                meta={"last_loaded_at": "old"},
            )

            result = service.refresh_change_source()
            snapshot = service._state_store.get_source_scope_snapshot("ALL")
            record_ids = {item["record_id"] for item in snapshot["records"]}
            zhihang_ids = {item["record_id"] for item in snapshot["zhihang_records"]}

            self.assertEqual(result["change_count"], 1)
            self.assertIn("m-old", record_ids)
            self.assertIn("r-old", record_ids)
            self.assertIn("c-new", record_ids)
            self.assertIn("z-new", zhihang_ids)

    def test_change_source_month_window_accepts_plan_date_aliases(self):
        service = MaintenancePortalService()
        month_start = service._recent_month_starts()[0]
        plan_start = month_start.replace(day=min(27, month_start.day), hour=9, minute=0)
        plan_end = month_start.replace(day=min(27, month_start.day), hour=23, minute=0)
        plan_start_text = plan_start.strftime("%Y-%m-%d %H:%M")
        plan_end_text = plan_end.strftime("%Y-%m-%d %H:%M")
        month_key = month_start.strftime("%Y-%m")
        record = _build_change_record(
            "c-plan-date",
            building="A楼",
            progress="未开始",
            start_time="",
            end_time="",
        )
        fields = record["display_fields"]
        fields.pop("变更开始日期（阿里）", None)
        fields.pop("变更结束日期（阿里）", None)
        fields["计划开始日期（阿里）"] = plan_start_text
        fields["计划结束日期（阿里）"] = plan_end_text

        self.assertEqual(
            service._change_time_range(record),
            (
                plan_start_text,
                plan_end_text,
                f"{plan_start_text}至{plan_end_text}",
            ),
        )
        self.assertIn(month_key, service._source_record_month_keys(record))
        self.assertTrue(service._source_record_matches_month_window(record))

    def test_refresh_change_source_failure_keeps_active_snapshot(self):
        class _FailingChangeRefreshService(MaintenancePortalService):
            def _load_change_fields(self):
                raise RuntimeError("change down")

        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), _FailingChangeRefreshService)
            service._state_store.replace_all_source_scope_snapshots(
                {
                    "ALL": {
                        "records": [_build_record("m-old", "A楼", "维护", "5月")],
                        "zhihang_records": [],
                    }
                },
                meta={"last_loaded_at": "old"},
            )
            before = service._state_store.source_snapshot_stats()["active"]["snapshot_id"]

            with self.assertRaisesRegex(PortalError, "变更源表同步失败"):
                service.refresh_change_source()

            after = service._state_store.source_snapshot_stats()["active"]["snapshot_id"]
            snapshot = service._state_store.get_source_scope_snapshot("ALL")
            self.assertEqual(after, before)
            self.assertEqual(snapshot["records"][0]["record_id"], "m-old")

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

        original_service = PortalRuntime.service
        service = _SlowSourceRefreshService()
        with PortalRuntime.source_refresh_lock:
            PortalRuntime.source_refresh_inflight = False
            PortalRuntime.source_refresh_last_result = {}
            PortalRuntime.source_refresh_last_finished = 0.0
        PortalRuntime.service = service
        try:
            first = PortalRuntime.request_source_refresh(force=True)
            self.assertTrue(first["source_refresh_started"])
            self.assertTrue(service.entered.wait(timeout=2))
            second = PortalRuntime.request_source_refresh(force=True)
            self.assertFalse(second["source_refresh_started"])
            self.assertTrue(second["source_refresh_inflight"])
            service.release.set()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                with PortalRuntime.source_refresh_lock:
                    inflight = PortalRuntime.source_refresh_inflight
                if not inflight and not PortalRuntime.source_refresh_run_lock.locked():
                    break
                time.sleep(0.02)
            with PortalRuntime.source_refresh_lock:
                self.assertFalse(PortalRuntime.source_refresh_inflight)
                self.assertTrue(PortalRuntime.source_refresh_last_result["refreshed"])
            self.assertFalse(PortalRuntime.source_refresh_run_lock.locked())
            self.assertEqual(service.calls, 1)
            self.assertEqual(service.link_calls, 1)
        finally:
            service.release.set()
            PortalRuntime.service = original_service
            with PortalRuntime.source_refresh_lock:
                PortalRuntime.source_refresh_inflight = False
                PortalRuntime.source_refresh_last_result = {}
                PortalRuntime.source_refresh_last_finished = 0.0

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
        original_sessions = dict(PortalRuntime.auth_manager._sessions)
        with PortalRuntime.auth_manager._lock:
            PortalRuntime.auth_manager._sessions[session_id] = {
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
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions["local-user-stats-session"] = {
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
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions = original_sessions
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
        original_service = PortalRuntime.service
        original_state_store = PortalRuntime.state_store
        original_auth_state_store = PortalRuntime.auth_manager._state_store
        original_sessions = dict(PortalRuntime.auth_manager._sessions)
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            PortalRuntime.service = service
            PortalRuntime.state_store = service._state_store
            PortalRuntime.auth_manager._state_store = service._state_store
            PortalRuntime.auth_manager.upsert_permission_user(
                open_id="ou_workbench",
                name="测试用户",
                role="user",
                scopes=["C"],
                enabled=True,
                updated_by="test",
            )
            PortalRuntime.auth_manager.upsert_permission_user(
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
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions[session_id] = {
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
                    PortalRuntime,
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
                service._state_store.upsert_runtime_queue_item("message", job_id)
                service._state_store.upsert_runtime_queue_item("qt_action", job_id)
                stuck_failed = client.post(
                    f"/api/jobs/{job_id}/mark-stuck-failed",
                    json={"reason": "管理员手动标记卡住任务，请核对后重试。"},
                    headers={"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"},
                )
                self.assertEqual(stuck_failed.status_code, 200)
                failed_job = service.get_job(job_id)
                self.assertEqual(failed_job["phase"], "failed")
                self.assertTrue(failed_job["error_retryable"])
                self.assertEqual(failed_job["error_category"], "admin_stuck_reset")
                queue_counts = service._state_store.runtime_queue_counts()
                self.assertEqual(queue_counts["message"]["failed"], 1)
                self.assertEqual(queue_counts["qt_action"]["failed"], 1)
                cleared = client.post(
                    f"/api/jobs/{job_id}/clear",
                    headers={"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"},
                )
                self.assertEqual(cleared.status_code, 200)
                self.assertIsNone(service.get_job(job_id))
            finally:
                PortalRuntime.service = original_service
                PortalRuntime.state_store = original_state_store
                PortalRuntime.auth_manager._state_store = original_auth_state_store
                with PortalRuntime.auth_manager._lock:
                    PortalRuntime.auth_manager._sessions = original_sessions

    def test_fastapi_backend_admin_tools_are_native_and_safe(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalRuntime.service
        original_state_store = PortalRuntime.state_store
        original_auth_state_store = PortalRuntime.auth_manager._state_store
        original_sessions = dict(PortalRuntime.auth_manager._sessions)
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
            PortalRuntime.service = service
            PortalRuntime.state_store = service._state_store
            PortalRuntime.auth_manager._state_store = service._state_store
            PortalRuntime.auth_manager.upsert_permission_user(
                open_id="ou_admin",
                name="admin",
                role="admin",
                scopes=["ALL"],
                enabled=True,
                updated_by="test",
            )
            session_id = "backend-tools-session"
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions[session_id] = {
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
                PortalRuntime.service = original_service
                PortalRuntime.state_store = original_state_store
                PortalRuntime.auth_manager._state_store = original_auth_state_store
                with PortalRuntime.auth_manager._lock:
                    PortalRuntime.auth_manager._sessions = original_sessions

    def test_compacted_failed_job_remains_visible_to_owner_only(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalRuntime.service
        original_state_store = PortalRuntime.state_store
        original_auth_state_store = PortalRuntime.auth_manager._state_store
        original_sessions = dict(PortalRuntime.auth_manager._sessions)
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            PortalRuntime.service = service
            PortalRuntime.state_store = service._state_store
            PortalRuntime.auth_manager._state_store = service._state_store
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
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions["owner-session"] = {
                    "session_id": "owner-session",
                    "user": {"name": "owner", "open_id": "ou_owner"},
                    "role": "building",
                    "allowed_scopes": ["A"],
                    "expires_at": time.time() + 3600,
                }
                PortalRuntime.auth_manager._sessions["other-session"] = {
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
                PortalRuntime.service = original_service
                PortalRuntime.state_store = original_state_store
                PortalRuntime.auth_manager._state_store = original_auth_state_store
                with PortalRuntime.auth_manager._lock:
                    PortalRuntime.auth_manager._sessions = original_sessions

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
        class FakeHttpClient:
            def __init__(self):
                self.calls = 0

            def request_json(self, *args, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    return {
                        "code": 99991663,
                        "msg": "Invalid access token for authorization.",
                    }
                return {"code": 0, "data": {"items": []}}

        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            fake_client = FakeHttpClient()
            service._http_client = fake_client
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
                self.assertEqual(fake_client.calls, 2)
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

    def test_token_manager_refreshes_once_for_concurrent_callers(self):
        calls = []

        def handler(request):
            calls.append(request.url.path)
            time.sleep(0.03)
            return httpx.Response(
                200,
                json={"code": 0, "tenant_access_token": "shared-token", "expire": 7200},
            )

        client = FeishuHttpClient(
            transport=httpx.MockTransport(handler),
            retries=0,
        )
        manager = FeishuTokenManager(http_client=client)
        old_app_id = config_module.config.app_id
        old_app_secret = config_module.config.app_secret
        old_token = config_module.config.user_token
        old_expire = config_module.config.token_expire_time

        def fake_save(**kwargs):
            if "user_token" in kwargs:
                config_module.config.user_token = kwargs["user_token"]
            if "token_expire_time" in kwargs:
                config_module.config.token_expire_time = kwargs["token_expire_time"]
            return True

        try:
            config_module.config.app_id = "cli_a"
            config_module.config.app_secret = "secret"
            config_module.config.user_token = ""
            config_module.config.token_expire_time = 0
            results: list[str] = []
            threads = [
                threading.Thread(target=lambda: results.append(manager.get_tenant_token()))
                for _ in range(5)
            ]
            with patch.object(config_module.config, "save", side_effect=fake_save):
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
            self.assertEqual(results, ["shared-token"] * 5)
            self.assertEqual(len(calls), 1)
        finally:
            client.close()
            config_module.config.app_id = old_app_id
            config_module.config.app_secret = old_app_secret
            config_module.config.user_token = old_token
            config_module.config.token_expire_time = old_expire

    def test_robot_webhook_uses_shared_tenant_token_manager(self):
        from upload_event_module.services import robot_webhook

        with patch.object(
            robot_webhook.token_manager,
            "get_tenant_token",
            return_value="shared-token",
        ) as get_token, patch.object(
            robot_webhook,
            "_send_message_to_open_id",
            return_value=(True, "ok"),
        ) as send_message:
            ok, message, results = robot_webhook.send_text_to_open_ids(
                "测试消息",
                ["ou_one", "ou_two"],
            )
        self.assertTrue(ok)
        self.assertEqual(message, "ok")
        self.assertEqual(len(results), 2)
        get_token.assert_called_once()
        self.assertEqual(send_message.call_count, 2)
        for call_args in send_message.call_args_list:
            self.assertEqual(call_args.args[0], "shared-token")

    def test_robot_webhook_refreshes_token_once_on_message_token_error(self):
        from upload_event_module.services import robot_webhook

        auth_headers: list[str] = []

        def fake_request(method, url, *, headers=None, **kwargs):
            auth_headers.append((headers or {}).get("Authorization", ""))
            if len(auth_headers) == 1:
                return {"code": 99991663, "msg": "Invalid access token"}
            return {"code": 0, "data": {"message_id": "msg1"}}

        with patch.object(
            robot_webhook,
            "_request_json",
            side_effect=fake_request,
        ), patch.object(
            robot_webhook.token_manager,
            "get_tenant_token",
            return_value="fresh-token",
        ) as get_token:
            ok, message = robot_webhook._send_message_to_open_id(
                "stale-token",
                "ou_one",
                "测试消息",
            )

        self.assertTrue(ok)
        self.assertEqual(message, "ok")
        self.assertEqual(auth_headers, ["Bearer stale-token", "Bearer fresh-token"])
        get_token.assert_called_once_with(force_refresh=True)

    def test_portal_auth_login_exchange_uses_shared_token_manager(self):
        auth = PortalAuthManager()
        with patch.object(
            config_module.config,
            "app_id",
            "cli_a",
        ), patch.object(
            config_module.config,
            "app_secret",
            "secret",
        ), patch(
            "lan_bitable_template_portal.portal_auth.token_manager.exchange_login_code",
            return_value={"open_id": "ou_login", "name": "测试用户"},
        ) as exchange:
            user = auth._exchange_login_code("login-code")
        self.assertEqual(user["open_id"], "ou_login")
        exchange.assert_called_once_with("login-code")

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
        original_service = PortalRuntime.service
        original_controller_state_store = controller._state_store
        original_portal_state_store = PortalRuntime.state_store
        temp_dir = tempfile.TemporaryDirectory()
        store = LanPortalStateStore(Path(temp_dir.name) / "state.sqlite3")
        controller._state_store = store
        PortalRuntime.state_store = store
        PortalRuntime.service = _NativeFastAPIRouteService()
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
                PortalRuntime.service.last_marked_job_patch["phase"],
                "failed",
            )
            self.assertIn(
                "Qt callback down",
                PortalRuntime.service.last_marked_job_patch["error"],
            )
        finally:
            PortalRuntime.service = original_service
            controller._state_store = original_controller_state_store
            PortalRuntime.state_store = original_portal_state_store
            temp_dir.cleanup()

    def test_fastapi_auth_redirect_routes_are_native(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        client = TestClient(controller._build_app())
        with patch.object(controller, "_proxy_request", side_effect=AssertionError("proxy used")):
            with patch.object(
                PortalRuntime.auth_manager,
                "start_login",
                return_value="https://login.example.test/oauth",
            ) as start_login:
                login = client.get("/api/auth/login?next=/x", follow_redirects=False)
            self.assertEqual(login.status_code, 302)
            self.assertEqual(login.headers["location"], "https://login.example.test/oauth")
            self.assertEqual(start_login.call_args.kwargs["next_path"], "/x")

            with patch.object(
                PortalRuntime.auth_manager,
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
                PortalRuntime.auth_manager,
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
        original_service = PortalRuntime.service
        original_state_store = PortalRuntime.state_store
        original_sessions = dict(PortalRuntime.auth_manager._sessions)
        PortalRuntime.service = _NativeFastAPIRouteService()
        temp_dir = tempfile.TemporaryDirectory()
        PortalRuntime.state_store = LanPortalStateStore(
            Path(temp_dir.name) / "state.sqlite3"
        )
        session_id = "native-route-session"
        with PortalRuntime.auth_manager._lock:
            PortalRuntime.auth_manager._sessions[session_id] = {
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
                change_refresh = client.get(
                    "/api/change-refresh?scope=ALL",
                    headers=headers,
                )
                action = client.post(
                    "/api/workbench-actions",
                    headers=headers,
                    json={"scope": "ALL", "work_type": "change", "action": "start"},
                )
                PortalRuntime.state_store.put_backend_runtime(
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
                change_refresh,
            ]:
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.json().get("ok"))
            self.assertIn("qt_outbox_pending", backend_stats.json()["data"])
            self.assertEqual(action.status_code, 202)
            self.assertTrue(action.json().get("ok"))
            self.assertEqual(action.json()["data"]["job_id"], "job-native")
            self.assertEqual(
                PortalRuntime.service.last_action_payload["_auth_user_name"],
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
            PortalRuntime.notice_callback = lambda payload: notices.append(payload)
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
                PortalRuntime.notice_callback = None
            self.assertEqual(sent.status_code, 200)
            self.assertTrue(sent.json().get("ok"))
            self.assertEqual(sent.json()["data"]["items"][0]["record_id"], "m1")
            self.assertEqual(len(notices), 1)
        finally:
            PortalRuntime.service = original_service
            PortalRuntime.state_store = original_state_store
            temp_dir.cleanup()
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions = original_sessions

    def test_fastapi_remove_local_updates_sqlite_and_qt_outbox_without_remote_delete(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalRuntime.service
        original_state_store = PortalRuntime.state_store
        original_sessions = dict(PortalRuntime.auth_manager._sessions)
        temp_dir = tempfile.TemporaryDirectory()
        store = LanPortalStateStore(Path(temp_dir.name) / "state.sqlite3")
        PortalRuntime.state_store = store
        PortalRuntime.service = _NativeFastAPIRouteService()
        item = {
            "active_item_id": "active-api-local-remove",
            "record_id": "recv-api-local-remove",
            "target_record_id": "recv-api-local-remove",
            "notice_type": "维保通告",
            "work_type": "maintenance",
            "title": "A楼接口本地移除测试",
            "building_codes": ["A"],
            "status": "开始",
            "text": "【维保通告】状态：开始\n【名称】A楼接口本地移除测试",
        }
        store.upsert_qt_active_item(item, section="other", origin="qt")
        session_id = "local-remove-admin-session"
        admin_open_id = PortalRuntime.auth_manager.admin_open_ids()[0]
        with PortalRuntime.auth_manager._lock:
            PortalRuntime.auth_manager._sessions[session_id] = {
                "session_id": session_id,
                "user": {"name": "测试管理员", "open_id": admin_open_id},
                "role": "admin",
                "allowed_scopes": ["ALL"],
                "expires_at": time.time() + 3600,
            }
        client = TestClient(controller._build_app())
        try:
            headers = {"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"}
            with patch(
                "lan_bitable_template_portal.server.delete_bitable_record"
            ) as remote_delete:
                response = client.post(
                    "/api/ongoing-items/remove-local",
                    headers=headers,
                    json={
                        "scope": "ALL",
                        "active_item_id": item["active_item_id"],
                        "target_record_id": item["target_record_id"],
                        "record_id": item["record_id"],
                        "work_type": item["work_type"],
                        "notice_type": item["notice_type"],
                    },
                )

            self.assertEqual(response.status_code, 200, response.text)
            data = response.json()["data"]
            self.assertTrue(data["qt_deleted"])
            self.assertFalse(data["remote_deleted"])
            self.assertEqual(store.list_qt_active_items(), [])
            remote_delete.assert_not_called()

            events = client.get("/api/qt/events?limit=1").json()["data"]["items"]
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["payload"]["kind"], "active_delete")
            self.assertEqual(
                events[0]["payload"]["payload"]["active_item_id"],
                item["active_item_id"],
            )
        finally:
            PortalRuntime.service = original_service
            PortalRuntime.state_store = original_state_store
            temp_dir.cleanup()
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions = original_sessions

    def test_fastapi_workbench_returns_current_qt_active_items(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalRuntime.service
        original_state_store = PortalRuntime.state_store
        original_auth_state_store = PortalRuntime.auth_manager._state_store
        original_sessions = dict(PortalRuntime.auth_manager._sessions)
        temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        service = self._new_temp_service(Path(temp_dir.name))
        try:
            service.ensure_snapshot_loaded = lambda: None
            PortalRuntime.service = service
            PortalRuntime.state_store = service._state_store
            PortalRuntime.auth_manager._state_store = service._state_store
            items = [
                {
                    "active_item_id": f"api-active-{index}",
                    "record_id": f"api-target-{index}",
                    "target_record_id": f"api-target-{index}",
                    "notice_type": "维保通告",
                    "work_type": "maintenance",
                    "title": f"C楼接口进行中通告{index}",
                    "building": "C楼",
                    "building_codes": ["C"],
                    "time_str": "2026-06-18 09:30~2026-06-18 18:30",
                    "content": f"维护内容{index}",
                    "reason": f"维护原因{index}",
                    "text": (
                        "【维保通告】状态：开始\n"
                        f"【名称】C楼接口进行中通告{index}\n"
                        "【时间】2026-06-18 09:30~2026-06-18 18:30\n"
                        f"【内容】维护内容{index}\n"
                        f"【原因】维护原因{index}"
                    ),
                }
                for index in range(1, 8)
            ]
            PortalRuntime.state_store.replace_ongoing_items(items[:5])
            for item in items:
                PortalRuntime.state_store.upsert_qt_active_item(
                    item,
                    section="other",
                    origin="qt",
                )
            session_id = "workbench-current-qt-session"
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions[session_id] = {
                    "session_id": session_id,
                    "user": {"name": "测试用户", "open_id": ""},
                    "role": "admin",
                    "allowed_scopes": ["ALL"],
                    "expires_at": time.time() + 3600,
                }
            client = TestClient(controller._build_app())
            response = client.get(
                "/api/workbench?scope=C",
                headers={"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"},
            )

            self.assertEqual(response.status_code, 200, response.text)
            payload = response.json()["data"]
            self.assertEqual(len(payload["ongoing"]), 7)
            self.assertEqual(
                {item["target_record_id"] for item in payload["ongoing"]},
                {f"api-target-{index}" for index in range(1, 8)},
            )
        finally:
            service._state_store.shutdown_write_worker()
            PortalRuntime.service = original_service
            PortalRuntime.state_store = original_state_store
            PortalRuntime.auth_manager._state_store = original_auth_state_store
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions = original_sessions
            temp_dir.cleanup()

    def test_fastapi_ongoing_delete_runs_backend_delete_before_qt_projection(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalRuntime.service
        original_state_store = PortalRuntime.state_store
        original_auth_state_store = PortalRuntime.auth_manager._state_store
        original_sessions = dict(PortalRuntime.auth_manager._sessions)
        PortalRuntime.service = _NativeFastAPIRouteService()
        temp_dir = tempfile.TemporaryDirectory()
        PortalRuntime.state_store = LanPortalStateStore(
            Path(temp_dir.name) / "state.sqlite3"
        )
        PortalRuntime.auth_manager._state_store = PortalRuntime.state_store
        PortalRuntime.auth_manager.upsert_permission_user(
            open_id="ou_delete",
            name="测试用户",
            role="admin",
            scopes=["ALL"],
            enabled=True,
            updated_by="test",
        )
        session_id = "delete-route-session"
        with PortalRuntime.auth_manager._lock:
            PortalRuntime.auth_manager._sessions[session_id] = {
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
            PortalRuntime.service = original_service
            PortalRuntime.state_store = original_state_store
            PortalRuntime.auth_manager._state_store = original_auth_state_store
            temp_dir.cleanup()
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions = original_sessions

    def test_fastapi_qt_command_delete_clears_active_item_and_read_cache(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalRuntime.service
        original_state_store = PortalRuntime.state_store
        PortalRuntime.service = _NativeFastAPIRouteService()
        temp_dir = tempfile.TemporaryDirectory()
        PortalRuntime.state_store = LanPortalStateStore(
            Path(temp_dir.name) / "state.sqlite3"
        )
        PortalRuntime.state_store.upsert_qt_active_item(
            {
                "active_item_id": "active-qt-delete",
                "record_id": "rec-qt-delete",
                "target_record_id": "rec-qt-delete",
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
                                "target_record_id": "rec-qt-delete",
                            }
                        },
                    },
                )

            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json().get("ok"))
            delete_record.assert_called_once_with("rec-qt-delete", "维保通告")
            self.assertEqual(PortalRuntime.state_store.list_qt_active_items(), [])
            self.assertEqual(controller._read_cache_stats()["entries"], 0)
            leased = PortalRuntime.state_store.lease_outbox_events(
                "qt_action", limit=1, lease_seconds=5
            )
            self.assertEqual(leased[0]["payload"]["kind"], "active_delete")
        finally:
            PortalRuntime.service = original_service
            PortalRuntime.state_store = original_state_store
            temp_dir.cleanup()

    def test_delete_creates_undo_and_restore_reprojects_qt_active_item(self):
        original_service = PortalRuntime.service
        original_state_store = PortalRuntime.state_store
        service = None
        temp_dir = tempfile.TemporaryDirectory()
        try:
            tmp = temp_dir.name
            service = self._new_temp_service(Path(tmp))
            PortalRuntime.service = service
            PortalRuntime.state_store = service._state_store
            active_payload = {
                "active_item_id": "active-delete-undo",
                "record_id": "target-delete-undo",
                "target_record_id": "target-delete-undo",
                "source_record_id": "source-delete-undo",
                "notice_type": "维保通告",
                "work_type": "maintenance",
                "title": "A楼删除回退测试",
                "building": "A楼",
                "building_codes": ["A"],
                "text": "【维保通告】状态：开始\n【名称】A楼删除回退测试",
            }
            service._state_store.upsert_qt_active_item(
                active_payload,
                section="other",
                origin="web",
            )

            with patch(
                "lan_bitable_template_portal.server.external_real_write_guard",
                return_value={
                    "mock_external": True,
                    "real_write_allowed": False,
                    "reason": "mock",
                },
            ), patch(
                "lan_bitable_template_portal.server.delete_bitable_record",
                return_value=(True, "deleted"),
            ) as delete_record:
                deleted = PortalRuntime.execute_local_delete_active_item(
                    {
                        "scope": "A",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "active_item_id": "active-delete-undo",
                        "record_id": "target-delete-undo",
                        "target_record_id": "target-delete-undo",
                        "source_record_id": "source-delete-undo",
                        "building_codes": ["A"],
                    }
                )

                self.assertTrue(deleted["ok"])
                self.assertTrue(deleted["remote_deleted"])
                self.assertTrue(deleted["undo_available"])
                delete_record.assert_called_once_with("target-delete-undo", "维保通告")
                self.assertEqual(service._state_store.list_qt_active_items(), [])

                undo_id = deleted["undo_id"]
                available = service.list_available_notice_undos(scope="A")
                self.assertEqual(available[0]["undo_id"], undo_id)

                restored = PortalRuntime.execute_notice_undo(
                    undo_id,
                    job_id="job-undo-delete",
                    requested_by="ou-admin",
                )

            self.assertTrue(restored["ok"])
            self.assertTrue(restored["restored_active"])
            self.assertEqual(restored["record_id"], "target-delete-undo")
            restored_items = service._state_store.list_qt_active_items()
            self.assertEqual(len(restored_items), 1)
            self.assertEqual(
                restored_items[0]["payload"]["active_item_id"],
                "active-delete-undo",
            )
            undo = service._state_store.get_notice_undo_action(undo_id)
            self.assertEqual(undo["status"], "undone")
            events = service._state_store.lease_outbox_events(
                "qt_action",
                limit=10,
                lease_seconds=5,
            )
            kinds = [event["payload"]["kind"] for event in events]
            self.assertIn("active_upsert", kinds)
        finally:
            if service is not None:
                try:
                    service._state_store.shutdown_write_worker(timeout=2.0)
                except Exception:
                    pass
            PortalRuntime.service = original_service
            PortalRuntime.state_store = original_state_store
            temp_dir.cleanup()

    def test_notice_undo_active_event_uses_recreated_target_record_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            undo = {
                "undo_id": "undo-recreated-target",
                "action_type": "delete",
                "work_type": "maintenance",
                "notice_type": "维保通告",
                "active_item_id": "active-recreated-target",
                "source_record_id": "source-recreated-target",
                "target_record_id": "target-deleted-old",
                "identity_keys": [
                    "active:active-recreated-target",
                    "source:maintenance:source-recreated-target",
                ],
                "local": {
                    "daily_item": None,
                    "work_items": [],
                    "qt_active": {
                        "section": "maintenance",
                        "sort_order": 0,
                        "origin": "web",
                        "payload": {
                            "active_item_id": "active-recreated-target",
                            "source_record_id": "source-recreated-target",
                            "target_record_id": "target-deleted-old",
                            "record_id": "target-deleted-old",
                            "work_type": "maintenance",
                            "notice_type": "维保通告",
                            "title": "A楼回退重建记录",
                            "building": "A楼",
                            "building_codes": ["A"],
                        },
                    },
                },
            }

            restored = service.restore_notice_undo_local(
                undo,
                target_record_id="target-recreated-new",
                applied_by="ou-admin",
                job_id="job-recreated-target",
            )

            self.assertTrue(restored["restored_active"])
            self.assertEqual(
                restored["active_payload"]["target_record_id"],
                "target-recreated-new",
            )
            self.assertEqual(
                restored["active_payload"]["record_id"],
                "target-recreated-new",
            )
            active_items = service._state_store.list_qt_active_items()
            self.assertEqual(
                active_items[0]["payload"]["target_record_id"],
                "target-recreated-new",
            )

    def test_fastapi_permission_request_routes_are_native(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_state_store = PortalRuntime.auth_manager._state_store
        original_sessions = dict(PortalRuntime.auth_manager._sessions)
        code_holder: dict[str, str] = {}
        with tempfile.TemporaryDirectory() as tmp:
            PortalRuntime.auth_manager._state_store = LanPortalStateStore(
                Path(tmp) / "state.sqlite3"
            )
            session_id = "permission-route-session"
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions[session_id] = {
                    "session_id": session_id,
                    "user": {"name": "申请人", "open_id": "ou_requester"},
                    "role": "building",
                    "allowed_scopes": [],
                    "expires_at": time.time() + 3600,
                }
                PortalRuntime.auth_manager._sessions["permission-route-admin"] = {
                    "session_id": "permission-route-admin",
                    "user": {
                        "name": "管理员",
                        "open_id": "ou_902e364a6c2c6c20893c02abe505a7b2",
                    },
                    "role": "admin",
                    "allowed_scopes": ["ALL"],
                    "expires_at": time.time() + 3600,
                }
            client = TestClient(controller._build_app())

            try:
                with patch.object(controller, "_proxy_request", side_effect=AssertionError("proxy used")):
                    with patch.object(
                        controller,
                        "_submit_background",
                        return_value=True,
                    ), patch(
                        "clipflow_backend.main._send_text_to_open_ids_guarded",
                        return_value=(True, "ok", []),
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
                            f"/api/auth/permission-requests/{request_id}/approve",
                            headers={
                                "Cookie": (
                                    f"{AUTH_COOKIE_NAME}=permission-route-admin"
                                )
                            },
                            json={
                                "scopes": ["A"],
                            },
                        )
                self.assertEqual(confirmed.status_code, 200)
                self.assertEqual(
                    confirmed.json()["data"]["request"]["status"],
                    "approved",
                )
                self.assertIn(
                    "A",
                    PortalRuntime.auth_manager.scopes_for_open_id("ou_requester"),
                )
            finally:
                PortalRuntime.auth_manager._state_store = original_state_store
                with PortalRuntime.auth_manager._lock:
                    PortalRuntime.auth_manager._sessions = original_sessions

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

    def test_backend_process_controller_qt_notice_upload_uses_command_endpoint(self):
        controller = BackendProcessPortalController(host="127.0.0.1", port=18766)
        payloads: list[dict] = []

        def fake_request(method, path, *, payload=None, timeout=5.0):
            payloads.append({"method": method, "path": path, "payload": dict(payload or {})})
            return {"ok": True, "data": {"ok": True, "name": "更新"}}

        with patch.object(controller, "_request_json", side_effect=fake_request):
            result = controller.execute_qt_notice_upload(
                {"action_type": "update", "data_dict": {"title": "测试"}}
            )
        self.assertEqual(result["name"], "更新")
        self.assertEqual(payloads[0]["method"], "POST")
        self.assertEqual(payloads[0]["path"], "/api/qt/commands")
        self.assertEqual(payloads[0]["payload"]["command"], "notice_update")
        self.assertEqual(
            payloads[0]["payload"]["payload"]["data_dict"]["title"],
            "测试",
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

    def test_manual_maintenance_notice_command_expands_patch_without_source_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            job_id, should_start = service.create_action_job(
                {
                    "command_format": "notice_command",
                    "action": "start",
                    "scope": "E",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "record_id": "manual-maintenance-codex-api",
                    "source_record_id": "manual-maintenance-codex-api",
                    "operation_id": "manual-maintenance-command-start",
                    "patch": {
                        "action": "start",
                        "scope": "E",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "manual": "1",
                        "manual_id": "manual-maintenance-codex-api",
                        "record_id": "manual-maintenance-codex-api",
                        "source_record_id": "manual-maintenance-codex-api",
                        "title": "EA118机房E楼测试维保",
                        "specialty": "电气",
                        "maintenance_cycle": "月度",
                        "start_time": "2026-06-25T09:00",
                        "end_time": "2026-06-25T18:00",
                        "location": "E楼",
                        "content": "测试内容",
                        "reason": "测试原因",
                        "impact": "无影响",
                        "progress": "准备工作已完成",
                    },
                }
            )

            self.assertTrue(should_start)
            job = service.get_job(job_id) or {}
            request = job.get("request") or {}
            self.assertTrue(request.get("manual"))
            self.assertEqual(request.get("manual_id"), "manual-maintenance-codex-api")
            self.assertNotIn("source_record_id", request)
            prepared = service.prepare_action_job(job_id)
            self.assertEqual(prepared["work_type"], WORK_TYPE_MAINTENANCE)
            self.assertEqual(prepared["source_app_token"], "")
            self.assertEqual(prepared["title"], "EA118机房E楼测试维保")
            self.assertIn("【维保通告】状态：开始", prepared["text"])

    def test_source_start_notice_command_does_not_treat_source_as_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            source_record_id = "recSourceMaintenanceStart"
            job_id, should_start = service.create_action_job(
                {
                    "command_format": "notice_command",
                    "action": "start",
                    "scope": "E",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "record_id": source_record_id,
                    "source_record_id": source_record_id,
                    "target_record_id": source_record_id,
                    "operation_id": "source-maintenance-command-start",
                    "patch": {
                        "action": "start",
                        "scope": "E",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "record_id": source_record_id,
                        "source_record_id": source_record_id,
                        "target_record_id": source_record_id,
                        "title": "EA118机房E楼测试源表维保",
                        "specialty": "电气",
                        "maintenance_cycle": "月度",
                        "start_time": "2026-06-25T09:00",
                        "end_time": "2026-06-25T18:00",
                        "location": "E楼",
                        "content": "测试内容",
                        "reason": "测试原因",
                        "impact": "无影响",
                        "progress": "准备工作已完成",
                    },
                }
            )

            self.assertTrue(should_start)
            job = service.get_job(job_id) or {}
            request = job.get("request") or {}
            self.assertEqual(request.get("record_id"), source_record_id)
            self.assertEqual(request.get("source_record_id"), source_record_id)
            self.assertNotEqual(request.get("target_record_id"), source_record_id)
            self.assertNotEqual(request.get("command_format"), "notice_command")

    def test_manual_start_notice_command_uses_semantic_target_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))

            def payload(*, title="测试测试E楼维保通告", content="测试内容", progress="准备完成", operation="op"):
                return {
                    "command_format": "notice_command",
                    "action": "start",
                    "scope": "E",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "record_id": "manual:lite",
                    "operation_id": operation,
                    "patch": {
                        "action": "start",
                        "scope": "E",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "manual": "1",
                        "manual_id": "manual:lite",
                        "record_id": "manual:lite",
                        "title": title,
                        "building": "E楼",
                        "specialty": "电气",
                        "maintenance_cycle": "月度",
                        "start_time": "2026-06-25T09:00",
                        "end_time": "2026-06-25T18:00",
                        "location": "E楼",
                        "content": content,
                        "reason": "测试原因",
                        "impact": "无影响",
                        "progress": progress,
                    },
                }

            first_job, first_should_start = service.create_action_job(payload(operation="op-1"))
            self.assertTrue(first_should_start)

            prefixed_duplicate = payload(
                title="EA118机房E楼测试测试E楼维保通告",
                progress="只改进度仍视为同一条通告",
                operation="op-2",
            )
            second_job, second_should_start = service.create_action_job(prefixed_duplicate)
            self.assertEqual(second_job, first_job)
            self.assertFalse(second_should_start)

            changed_content_job, changed_content_should_start = service.create_action_job(
                payload(content="测试内容已变化", operation="op-3")
            )
            self.assertNotEqual(changed_content_job, first_job)
            self.assertTrue(changed_content_should_start)

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
        self.assertFalse(change["skip_personal_message"])
        self.assertGreaterEqual(len(change["recipients"]), 2)

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
        self.assertFalse(repair["skip_personal_message"])
        self.assertGreaterEqual(len(repair["recipients"]), 2)

    def test_all_manual_notice_commands_expand_before_queueing(self):
        cases = [
            (
                "maintenance",
                "维保通告",
                {
                    "title": "EA118机房E楼测试维保",
                    "specialty": "电气",
                    "maintenance_cycle": "月度",
                    "start_time": "2026-06-25T09:00",
                    "end_time": "2026-06-25T18:30",
                    "location": "E楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "无影响",
                    "progress": "准备工作已完成",
                },
                "【维保通告】状态：开始",
            ),
            (
                "change",
                "变更通告",
                {
                    "title": "EA118机房E楼测试变更",
                    "specialty": "电气",
                    "level": "I3",
                    "zhihang_involved": "0",
                    "start_time": "2026-06-25T09:00",
                    "end_time": "2026-06-25T18:30",
                    "location": "E楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "无影响",
                    "progress": "准备工作已完成",
                },
                "【变更通告】状态：开始",
            ),
            (
                "repair",
                "设备检修",
                {
                    "title": "EA118机房E楼测试检修",
                    "specialty": "暖通",
                    "fault_time": "2026-06-25T09:00",
                    "expected_time": "2026-06-25T18:30",
                    "location": "E楼",
                    "urgency": "低",
                    "repair_device": "测试设备",
                    "fault": "测试故障",
                    "fault_type": "设备故障",
                    "repair_mode": "自维",
                    "impact_scope": "无影响",
                    "discovery": "人工发现",
                    "symptom": "测试现象",
                    "reason": "测试原因",
                    "solution": "测试方案",
                    "spare_parts": "无",
                    "completion": "准备工作已完成",
                },
                "【设备检修】状态：开始",
            ),
            (
                "power",
                "上电通告",
                {
                    "title": "EA118机房E楼测试上电",
                    "start_time": "2026-06-25T09:00",
                    "end_time": "2026-06-25T18:30",
                    "cabinet": "E-201",
                    "count": "1个",
                    "progress": "准备工作已完成",
                },
                "【上电通告】状态：开始",
            ),
            (
                "polling",
                "设备轮巡",
                {
                    "title": "EA118机房E楼测试轮巡",
                    "start_time": "2026-06-25T09:00",
                    "end_time": "2026-06-25T18:30",
                    "device": "测试设备",
                    "content": "测试内容",
                    "impact": "无影响",
                    "progress": "准备工作已完成",
                },
                "【设备轮巡】状态：开始",
            ),
            (
                "adjust",
                "设备调整",
                {
                    "title": "EA118机房E楼测试调整",
                    "start_time": "2026-06-25T09:00",
                    "end_time": "2026-06-25T18:30",
                    "location": "E楼",
                    "content": "测试内容",
                    "reason": "测试原因",
                    "impact": "无影响",
                    "progress": "准备工作已完成",
                },
                "【设备调整】状态：开始",
            ),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            for work_type, notice_type, patch_payload, expected_header in cases:
                manual_id = f"manual:{work_type}:command"
                with self.subTest(work_type=work_type):
                    job_id, should_start = service.create_action_job(
                        {
                            "command_format": "notice_command",
                            "action": "start",
                            "scope": "E",
                            "work_type": work_type,
                            "notice_type": notice_type,
                            "manual": "1",
                            "manual_id": manual_id,
                            "record_id": manual_id,
                            "source_record_id": manual_id,
                            "operation_id": f"{manual_id}:start",
                            "patch": {
                                "action": "start",
                                "scope": "E",
                                "work_type": work_type,
                                "notice_type": notice_type,
                                "manual": "1",
                                "manual_id": manual_id,
                                "record_id": manual_id,
                                "source_record_id": manual_id,
                                **patch_payload,
                            },
                        }
                    )

                    self.assertTrue(should_start)
                    job = service.get_job(job_id) or {}
                    request = job.get("request") or {}
                    self.assertEqual(request.get("record_id"), manual_id)
                    self.assertNotIn("source_record_id", request)
                    self.assertNotEqual(request.get("command_format"), "notice_command")
                    for field_name, field_value in patch_payload.items():
                        self.assertEqual(
                            request.get(field_name),
                            field_value,
                            f"{work_type} command expansion lost {field_name}",
                        )
                    prepared = service.prepare_action_job(job_id)
                    self.assertEqual(prepared["work_type"], work_type)
                    self.assertEqual(prepared["notice_type"], notice_type)
                    self.assertFalse(prepared.get("source_app_token"))
                    self.assertFalse(prepared.get("source_table_id"))
                    self.assertIn(expected_header, prepared["text"])
                    self.assertIn(str(patch_payload["title"]), prepared["text"])
                    self.assertIn("18:30", prepared["text"])
                    self.assertNotIn("18:00", prepared["text"])
                    if work_type == "repair":
                        self.assertIn("【备件更换情况】无", prepared["text"])

    def test_workbench_lite_parse_iso_datetime_range_keeps_both_times(self):
        work_type, action, draft = parse_pasted_notice_to_draft(
            "【设备轮巡】状态：开始\n"
            "【标题】EA118机房A楼制冷单元轮巡通告\n"
            "【时间】2026-06-24 09:30~2026-06-24 18:30\n"
            "【设备】A-127冷冻站制冷单元\n"
            "【内容】测试测试\n"
            "【影响】测试测试\n"
            "【进度】测试测试"
        )

        self.assertEqual(work_type, "polling")
        self.assertEqual(action, "start")
        self.assertEqual(draft.get("start_time"), "2026-06-24T09:30")
        self.assertEqual(draft.get("end_time"), "2026-06-24T18:30")

    def test_notice_command_latest_form_values_override_ongoing_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            for work_type in (
                "maintenance",
                "change",
                "repair",
                "power",
                "polling",
                "adjust",
            ):
                with self.subTest(work_type=work_type):
                    active_item_id = f"active-{work_type}-latest-form"
                    target_record_id = f"rec-{work_type}-latest-form"
                    base = {
                        "active_item_id": active_item_id,
                        "target_record_id": target_record_id,
                        "record_id": target_record_id,
                        "work_type": work_type,
                        "scope": "E",
                        "manual": True,
                        "title": "旧标题",
                        "start_time": "2026-07-22T09:00",
                        "end_time": "2026-07-22T18:00",
                        "progress": "旧进度",
                    }
                    patch_payload = {
                        "work_type": work_type,
                        "scope": "E",
                        "manual": "1",
                        "active_item_id": active_item_id,
                        "target_record_id": target_record_id,
                        "record_id": target_record_id,
                        "title": "用户刚修改的标题",
                        "start_time": "2026-07-22T09:30",
                        "end_time": "2026-07-22T18:30",
                        "progress": "用户刚修改的进度",
                    }
                    if work_type == "repair":
                        patch_payload.update(
                            {
                                "fault_time": "2026-07-22T18:30",
                                "expected_time": "2026-07-22T09:30",
                            }
                        )
                    expanded = service.expand_workbench_action_command(
                        {
                            "command_format": "notice_command",
                            "action": "update",
                            "scope": "E",
                            "work_type": work_type,
                            "active_item_id": active_item_id,
                            "target_record_id": target_record_id,
                            "record_id": target_record_id,
                            "patch": patch_payload,
                        },
                        scope="E",
                        ongoing_items=[base],
                    )
                    self.assertEqual(expanded.get("title"), "用户刚修改的标题")
                    self.assertEqual(expanded.get("start_time"), "2026-07-22T09:30")
                    self.assertEqual(expanded.get("end_time"), "2026-07-22T18:30")
                    self.assertEqual(expanded.get("progress"), "用户刚修改的进度")

    def test_workbench_captures_form_before_yielding_browser_turn(self):
        source = (
            BIN_DIR / "lan_bitable_template_portal" / "workbench_lite.py"
        ).read_text(encoding="utf-8")
        handler = source.split("document.addEventListener('submit', async (event) => {{", 1)[1]
        handler = handler.split("document.addEventListener('input', (event) => {{", 1)[0]
        self.assertIn("captureNoticeFormValues(form)", source)
        self.assertLess(
            handler.index("payload = formPayload(form, submitter, submitAction)"),
            handler.index("await nextBrowserTurn()"),
        )

    def test_maintenance_update_uses_latest_edited_end_minute_end_to_end(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            active_item_id = "active-maintenance-minute"
            target_record_id = "rec-maintenance-minute"
            base = {
                "active_item_id": active_item_id,
                "target_record_id": target_record_id,
                "record_id": target_record_id,
                "work_type": "maintenance",
                "notice_type": "维保通告",
                "scope": "E",
                "manual": True,
                "building": "E楼",
                "title": "EA118机房E楼测试维保",
                "specialty": "电气",
                "maintenance_cycle": "每月",
                "start_time": "2026-07-22T09:30",
                "end_time": "2026-07-22T18:00",
                "location": "E楼",
                "content": "测试内容",
                "reason": "测试原因",
                "impact": "无影响",
                "progress": "旧进度",
            }
            command = {
                "command_format": "notice_command",
                "action": "update",
                "scope": "E",
                "work_type": "maintenance",
                "notice_type": "维保通告",
                "active_item_id": active_item_id,
                "target_record_id": target_record_id,
                "record_id": target_record_id,
                "operation_id": "maintenance-minute-update",
                "patch": {
                    **base,
                    "end_time": "2026-07-22T18:30",
                    "progress": "用户刚修改的进度",
                },
            }
            expanded = service.expand_workbench_action_command(
                command,
                scope="E",
                ongoing_items=[base],
            )
            job_id, should_start = service.create_action_job(expanded)
            self.assertTrue(should_start)
            prepared = service.prepare_action_job(job_id)
            self.assertEqual(prepared.get("end_time"), "2026-07-22 18:30")
            self.assertEqual(prepared.get("progress"), "用户刚修改的进度")
            self.assertIn(
                "【时间】2026-07-22 09:30~2026-07-22 18:30",
                prepared.get("text") or "",
            )
            self.assertNotIn("2026-07-22 18:00", prepared.get("text") or "")

    def test_workbench_lite_parse_power_down_keeps_notice_type(self):
        work_type, action, draft = parse_pasted_notice_to_draft(
            "【下电通告】状态：开始\n"
            "【名称】EA118机房A楼PDU下电通告\n"
            "【时间】2026-06-24 09:30~2026-06-24 18:30\n"
            "【柜号】A-101\n"
            "【数量】2\n"
            "【进度】准备下电"
        )

        self.assertEqual(work_type, "power")
        self.assertEqual(action, "start")
        self.assertEqual(draft.get("notice_type"), "下电通告")
        self.assertEqual(draft.get("title"), "EA118机房A楼PDU下电通告")

    def test_workbench_lite_blank_detail_form_defaults_to_manual(self):
        from lan_bitable_template_portal.workbench_lite import _detail_form

        html = _detail_form(
            record=None,
            ongoing_item=None,
            scope="E",
            work_type="polling",
            manual=False,
        )

        self.assertIn('name="manual" value="1"', html)
        self.assertIn('name="manual_id" value="manual:lite:E:polling"', html)
        self.assertIn("纯手填通告", html)
        self.assertNotIn('class="detail-status-board"', html)
        self.assertNotIn("发送创建", html)
        self.assertIn('name="manual_binding_required" value="1"', html)
        self.assertIn('name="manual_binding_choice" value=""', html)
        self.assertIn("绑定计划通告", html)
        self.assertIn("不绑定", html)

    def test_workbench_source_ongoing_row_is_startable_until_active_item_exists(self):
        from lan_bitable_template_portal.workbench_lite import _record_rows

        service = _TestMaintenancePortalService()
        ended_record = _build_repair_record(
            "repair-ended-source",
            title="D楼已结束检修",
            building="D楼",
        )
        ongoing_record = _build_repair_record(
            "repair-ongoing-source",
            title="D楼进行中检修",
            building="D楼",
        )
        serialized_ended = service._serialize_record(
            ended_record,
            {
                "repair-ended-source": {
                    "source_record_id": "repair-ended-source",
                    "status": "已结束",
                    "ended_at": "2026-07-18 12:00",
                }
            },
        )
        serialized_ongoing = service._serialize_record(
            ongoing_record,
            {
                "repair-ongoing-source": {
                    "source_record_id": "repair-ongoing-source",
                    "status": "进行中",
                    "started_at": "2026-07-18 09:00",
                }
            },
        )

        self.assertEqual(serialized_ended["source_progress"], "已结束")
        self.assertEqual(serialized_ongoing["source_progress"], "进行中")
        html = _record_rows(
            [serialized_ended, serialized_ongoing],
            ongoing_items=[],
            scope="D",
            work_type="repair",
            search="",
            specialty="",
            selected_id="",
        )
        self.assertEqual(html.count('class="notice-row is-disabled"'), 1)
        self.assertEqual(html.count('aria-disabled="true"'), 1)
        self.assertIn('data-action="start"', html)
        self.assertIn('data-source-record-id="repair-ongoing-source"', html)
        self.assertIn("该事项已结束，只保留查看状态，不可再次发起。", html)
        self.assertNotIn("该事项已在“未结束通告”中", html)

    def test_workbench_source_terminal_status_beats_stale_processing_summary(self):
        record = {
            "source_progress": "已结束",
            "source_status": "已结束",
            "status": "处理中",
            "work_summary": {"status": "处理中"},
        }

        self.assertEqual(workbench_lite_module._record_progress(record), "已结束")

    def test_target_binding_fills_fields_without_overwriting_source_record_id(self):
        source = (
            BIN_DIR / "lan_bitable_template_portal" / "workbench_lite.py"
        ).read_text(encoding="utf-8")
        handler = source.split("async function confirmTargetCandidateBinding()", 1)[1]
        handler = handler.split("function hydrateLitePreview()", 1)[0]

        self.assertIn("applyTargetRecordFormFields(form, targetFormFields)", handler)
        self.assertIn("setFormValue(form, 'target_record_id', targetRecordId)", handler)
        self.assertNotIn("setFormValue(form, 'record_id', targetRecordId)", handler)

    def test_source_status_does_not_inherit_same_title_historical_work_status(self):
        service = _TestMaintenancePortalService()
        record = _build_record(
            "maintenance-current",
            "B楼",
            "UPS厂商巡检维护",
            "7月",
            status="未开始",
            maintenance_cycle="半年",
        )
        historical_status = {
            "source_record_id": "maintenance-old",
            "work_type": "maintenance",
            "work_fallback_key": service._record_fallback_key(record),
            "fallback_key": service._record_legacy_summary_key(record),
            "status": "已结束",
            "ended_at": "2025-07-20 18:00",
        }

        with patch.object(
            service,
            "_load_work_status_items_locked",
            return_value=[historical_status],
        ):
            summaries = service._work_status_by_records([record], scope="B")

        self.assertEqual(summaries, {})
        serialized = service._serialize_record(
            record,
            {"maintenance-current": historical_status},
        )
        self.assertEqual(serialized["source_progress"], "未开始")
        self.assertEqual(serialized["work_summary"], {})

    def test_manual_source_options_only_return_pending_unlinked_items(self):
        service = _TestMaintenancePortalService()
        pending = _build_repair_record(
            "repair-pending",
            title="D楼待发起检修",
            building="D楼",
        )
        local_ended = _build_repair_record(
            "repair-local-ended",
            title="D楼本地已结束检修",
            building="D楼",
        )
        remote_ended = _build_repair_record(
            "repair-remote-ended",
            title="D楼源表已结束检修",
            building="D楼",
            ended=True,
        )
        linked_ongoing = _build_repair_record(
            "repair-linked-ongoing",
            title="D楼已关联进行中检修",
            building="D楼",
        )
        unlinked_ongoing = _build_repair_record(
            "repair-unlinked-ongoing",
            title="D楼未关联进行中检修",
            building="D楼",
            started=True,
        )
        ongoing_items = [
            {
                "active_item_id": "active-repair-linked",
                "source_record_id": "repair-linked-ongoing",
                "target_record_id": "target-repair-linked",
                "record_id": "target-repair-linked",
                "work_type": "repair",
                "notice_type": "设备检修",
                "building": "D楼",
                "building_codes": ["D"],
                "status": "进行中",
            }
        ]
        records = [
            pending,
            unlinked_ongoing,
            local_ended,
            remote_ended,
            linked_ongoing,
        ]
        summaries = {
            "repair-local-ended": {
                "source_record_id": "repair-local-ended",
                "status": "已结束",
                "ended_at": "2026-07-18 12:00",
            }
        }
        with patch.object(service, "ensure_snapshot_loaded", return_value=None), patch.object(
            service,
            "_workbench_records",
            return_value=records,
        ), patch.object(
            service,
            "_work_status_by_records",
            return_value=summaries,
        ), patch.object(
            service,
            "_project_ongoing_items",
            return_value=ongoing_items,
        ):
            options = service.list_bindable_source_items(
                scope="D",
                work_type="repair",
                ongoing_items=ongoing_items,
            )

        self.assertEqual(
            {
                item["source_record_id"]
                for item in options
            },
            {"repair-pending", "repair-unlinked-ongoing"},
        )

    def test_workbench_lite_repair_management_prefill_keeps_ids_distinct(self):
        from lan_bitable_template_portal.workbench_lite import _detail_form

        html = _detail_form(
            record=None,
            ongoing_item=None,
            scope="E",
            work_type="repair",
            manual=False,
            prefill_draft={
                "work_type": "repair",
                "title": "EA118机房E楼精密空调检修",
                "specialty": "暖通",
                "start_time": "2026-07-10 18:30",
                "end_time": "2026-07-10 09:30",
            },
            prefill_source_record_id="rec_repair_summary",
            prefill_target_record_id="rec_repair_target",
            prefill_action="update",
            prefill_context_id="rec_repair_summary",
        )

        self.assertIn("维修单生成检修通告", html)
        self.assertIn('name="record_id" value="rec_repair_summary"', html)
        self.assertIn('name="source_record_id" value="rec_repair_summary"', html)
        self.assertIn('name="target_record_id" value="rec_repair_target"', html)
        self.assertIn('name="manual_id" value=""', html)
        self.assertIn('value="update"', html)
        self.assertIn('name="title" type="text" value="EA118机房E楼精密空调检修"', html)
        self.assertIn('name="start_time" type="datetime-local" value="2026-07-10T18:30"', html)

    def test_workbench_lite_specialty_field_uses_fixed_select_options(self):
        from lan_bitable_template_portal.workbench_lite import _detail_form

        html = _detail_form(
            record=None,
            ongoing_item=None,
            scope="E",
            work_type="maintenance",
            manual=True,
        )

        self.assertIn('<select name="specialty" required aria-required="true">', html)
        for option in ("电气", "暖通", "消防", "弱电"):
            self.assertIn(f'<option value="{option}"', html)
        self.assertNotIn('name="specialty" type="text"', html)

    def test_workbench_lite_ongoing_detail_exposes_delete_actions(self):
        from lan_bitable_template_portal.workbench_lite import _detail_form

        ongoing_item = {
            "active_item_id": "active-delete",
            "target_record_id": "rec-delete",
            "record_id": "rec-delete",
            "source_record_id": "src-delete",
            "work_type": "maintenance",
            "notice_type": "维保通告",
            "title": "删除入口测试",
        }

        user_html = _detail_form(
            record=None,
            ongoing_item=ongoing_item,
            scope="E",
            work_type="maintenance",
            manual=False,
            is_admin=False,
        )
        admin_html = _detail_form(
            record=None,
            ongoing_item=ongoing_item,
            scope="E",
            work_type="maintenance",
            manual=False,
            is_admin=True,
        )

        self.assertIn('data-ongoing-delete-mode="remote"', user_html)
        self.assertIn("删除通告", user_html)
        self.assertNotIn('data-ongoing-delete-mode="local"', user_html)
        self.assertIn('data-ongoing-delete-mode="local"', admin_html)
        self.assertIn("移除显示", admin_html)

    def test_workbench_lite_clears_only_the_completed_current_notice(self):
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite

        html = render_workbench_lite(
            payload={"records": [], "ongoing": [], "daily_summary": {"stats": {}}},
            session={"name": "测试用户", "is_admin": False},
            scope="E",
            work_type="maintenance",
        )

        self.assertIn("function currentNoticeMatchesDraft(form, draft)", html)
        self.assertIn("function clearCompletedCurrentNotice(draft)", html)
        self.assertIn("if (!currentNoticeMatchesDraft(form, draft)) return false", html)
        self.assertEqual(html.count("clearCompletedCurrentNotice(draft);"), 2)
        self.assertIn("form.replaceWith(empty);", html)
        self.assertIn("url.searchParams.delete(key);", html)

    def test_workbench_lite_undo_requires_confirmation_and_waits_for_job_success(self):
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite

        html = render_workbench_lite(
            payload={
                "records": [],
                "ongoing": [],
                "daily_summary": {"stats": {}},
                "record_type_counts": {},
                "ongoing_type_counts": {},
            },
            session={"user": {"name": "测试用户"}, "is_admin": False},
            scope="D",
            work_type="repair",
            notice_undos=[
                {
                    "undo_id": "undo-repair-end",
                    "title": "D楼检修结束回退",
                    "undo_label": "撤销结束",
                    "undo_created_at": "2026-07-18 12:30",
                }
            ],
        )

        self.assertIn('data-undo-id="undo-repair-end"', html)
        self.assertIn('id="lite-undo-confirm"', html)
        self.assertIn("确认回退这条通告", html)
        self.assertIn("openUndoConfirm(undoButton);", html)
        self.assertIn("await pollUndoJob(jobId);", html)
        self.assertIn(
            "await refreshCurrentLite('回退成功，正在更新列表...', ['.status', '.summary', '.workspace']);",
            html,
        )

    def test_workbench_lite_linked_source_uses_ongoing_actions_and_target_identity(self):
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite

        linked_ongoing = {
            "active_item_id": "active-converted-change",
            "target_record_id": "rec-converted-change-target",
            "record_id": "rec-converted-change-target",
            "source_record_id": "rec-maintenance-source",
            "work_type": "change",
            "notice_type": "变更通告",
            "source_work_type": "maintenance",
            "converted_from_work_type": "maintenance",
            "title": "EA118机房A楼冷却塔清洗变更",
            "building": "A楼",
            "status": "进行中",
        }
        html = render_workbench_lite(
            payload={
                "records": [
                    {
                        "record_id": "rec-maintenance-source",
                        "source_record_id": "rec-maintenance-source",
                        "work_type": "change",
                        "notice_type": "变更通告",
                        "source_work_type": "maintenance",
                        "converted_from_work_type": "maintenance",
                        "title": "EA118机房A楼冷却塔清洗变更",
                        "building": "A楼",
                        "source_progress": "进行中",
                        "linked_ongoing": linked_ongoing,
                    }
                ],
                # The matching item can be outside the current ongoing page. The
                # source row must still retain the linked active projection.
                "ongoing": [],
                "daily_summary": {"stats": {}},
                "record_type_counts": {"change": 1},
                "ongoing_type_counts": {"change": 1},
            },
            session={"user": {"name": "测试用户"}, "is_admin": False},
            scope="A",
            work_type="change",
            record_id="rec-maintenance-source",
            scope_options=[{"value": "A", "label": "A楼"}],
        )

        form_html = html.split('<form id="lite-notice-form"', 1)[1].split("</form>", 1)[0]
        self.assertIn('name="source_record_id" value="rec-maintenance-source"', form_html)
        self.assertIn('name="target_record_id" value="rec-converted-change-target"', form_html)
        self.assertIn('name="active_item_id" value="active-converted-change"', form_html)
        self.assertIn('name="submit_action" value="update"', form_html)
        self.assertIn('name="submit_action" value="end"', form_html)
        self.assertIn('data-ongoing-delete-mode="remote"', form_html)
        self.assertNotIn('data-revert-to-maintenance="1"', form_html)
        self.assertIn('data-linked-ongoing="1"', html)

    def test_query_records_links_unique_converted_source_to_active_projection(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            current_month = MaintenancePortalService._current_month_label()
            service._records = [
                _build_record(
                    "rec-maintenance-source",
                    "A楼",
                    "冷却塔清洗",
                    current_month,
                    status="进行中",
                    maintenance_cycle="每月",
                )
            ]
            service._change_records = []
            service._repair_records = []
            service._zhihang_change_records = []
            service._maintenance_loaded_once = True
            service._change_loaded_once = True
            service._repair_loaded_once = True
            service._zhihang_change_loaded_once = True
            service.set_notice_work_type_override(
                record_id="rec-maintenance-source",
                source_work_type=WORK_TYPE_MAINTENANCE,
                target_work_type=WORK_TYPE_CHANGE,
                scope="A",
                updated_by="tester",
            )

            payload = service.query_records(
                scope="A",
                work_type=WORK_TYPE_CHANGE,
                sections=("records", "ongoing", "stats"),
                ongoing_items=[
                    {
                        "active_item_id": "active-converted-change",
                        "target_record_id": "rec-converted-change-target",
                        "record_id": "rec-converted-change-target",
                        "source_record_id": "rec-maintenance-source",
                        "work_type": WORK_TYPE_CHANGE,
                        "notice_type": "变更通告",
                        "title": "EA118机房A楼冷却塔清洗变更",
                        "building": "A楼",
                        "building_codes": ["A"],
                        "status": "进行中",
                    }
                ],
            )

            self.assertEqual(len(payload["records"]), 1)
            linked = payload["records"][0].get("linked_ongoing") or {}
            self.assertEqual(linked.get("source_record_id"), "rec-maintenance-source")
            self.assertEqual(linked.get("target_record_id"), "rec-converted-change-target")
            self.assertEqual(linked.get("active_item_id"), "active-converted-change")

    def test_workbench_lite_all_tab_counts_and_keeps_real_item_types(self):
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite

        html = render_workbench_lite(
            payload={
                "records": [
                    {
                        "record_id": "src-maintenance",
                        "source_record_id": "src-maintenance",
                        "work_type": "maintenance",
                        "notice_type": "维保通告",
                        "title": "C楼维保待发起",
                        "building": "C楼",
                    },
                    {
                        "record_id": "src-change",
                        "source_record_id": "src-change",
                        "work_type": "change",
                        "notice_type": "变更通告",
                        "title": "C楼变更待发起",
                        "building": "C楼",
                    },
                ],
                "ongoing": [
                    {
                        "active_item_id": "active-repair",
                        "target_record_id": "target-repair",
                        "work_type": "repair",
                        "notice_type": "设备检修",
                        "title": "C楼检修进行中",
                        "building": "C楼",
                    },
                    {
                        "active_item_id": "active-polling",
                        "target_record_id": "target-polling",
                        "work_type": "polling",
                        "notice_type": "设备轮巡",
                        "title": "C楼轮巡进行中",
                        "building": "C楼",
                    },
                ],
                "daily_summary": {"stats": {}},
                "record_type_counts": {},
                "ongoing_type_counts": {},
            },
            session={"user": {"name": "管理员"}, "is_admin": True},
            scope="C",
            work_type="all",
            scope_options=[{"value": "C", "label": "C楼"}],
        )

        self.assertIn("<span>全部</span>", html)
        self.assertIn('aria-current="page"', html)
        self.assertIn('title="待发起 2，进行中 2"', html)
        self.assertIn('name="work_type" value="all"', html)
        self.assertIn('data-work-type="maintenance"', html)
        self.assertIn('data-work-type="change"', html)
        self.assertIn('data-work-type="repair"', html)
        self.assertIn('data-work-type="polling"', html)
        self.assertIn("C楼变更待发起", html)
        self.assertIn("C楼轮巡进行中", html)
        self.assertIn("const liteIsAdmin = true", html)
        self.assertIn("deleteButton.setAttribute('data-ongoing-delete-mode', 'remote')", html)
        self.assertIn("localRemoveButton.setAttribute('data-ongoing-delete-mode', 'local')", html)

    def test_workbench_lite_defaults_to_current_month_and_preserves_month_selection(self):
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite

        current_month = MaintenancePortalService._current_month_label()
        previous_month = MaintenancePortalService._recent_month_labels()[1]
        html = render_workbench_lite(
            payload={
                "records": [],
                "ongoing": [],
                "daily_summary": {"stats": {}},
                "record_type_counts": {},
                "ongoing_type_counts": {},
                "filters": {
                    "default_month": current_month,
                    "months": [current_month, previous_month],
                },
            },
            session={"user": {"name": "测试用户"}, "is_admin": False},
            scope="A",
            work_type="maintenance",
            scope_options=[{"value": "A", "label": "A楼"}],
        )

        self.assertIn('id="lite-month-select" name="month"', html)
        self.assertIn(
            f'<option value="{current_month}" selected>{current_month}</option>',
            html,
        )
        self.assertIn(
            f'<input type="hidden" name="source_month" value="{current_month}">',
            html,
        )
        self.assertIn("params.set('month', event.target.value);", html)
        self.assertIn("params.delete('record_id');", html)
        self.assertIn("params.delete('active_item_id');", html)

    def test_workbench_lite_applies_history_memory_and_moves_source_times_to_today(self):
        from lan_bitable_template_portal.workbench_lite import (
            _draft_from_record,
            _memory_status_chip,
        )

        record = {
            "record_id": "source-memory-current",
            "source_record_id": "source-memory-current",
            "work_type": "maintenance",
            "notice_type": "维保通告",
            "display_fields": {
                "楼栋": "A楼",
                "维护总项": "UPS厂商巡检维护",
                "维护周期": "月度",
                "专业类别": "暖通",
                "计划开始时间": "2026-06-18 09:30",
                "计划结束时间": "2026-06-18 18:30",
            },
            "memory": {
                "location": "A-245配电室",
                "content": "工程师按历史记忆执行UPS巡检",
                "reason": "按月度维护计划执行",
                "impact": "对IT设备无影响",
                "progress": "准备工作已完成，人员已就位",
                "maintenance_cycle": "月度",
                "specialty": "电气",
                "updated_at": "2026-06-18 18:30:00",
            },
        }

        draft = _draft_from_record(record, work_type="maintenance")
        today = dt.date.today().isoformat()

        self.assertEqual(draft["location"], "A-245配电室")
        self.assertEqual(draft["content"], "工程师按历史记忆执行UPS巡检")
        self.assertEqual(draft["reason"], "按月度维护计划执行")
        self.assertEqual(draft["impact"], "对IT设备无影响")
        self.assertEqual(draft["progress"], "准备工作已完成，人员已就位")
        self.assertEqual(draft["specialty"], "电气")
        self.assertEqual(draft["start_time"], f"{today}T09:30")
        self.assertEqual(draft["end_time"], f"{today}T18:30")
        self.assertIn("已用历史记忆", _memory_status_chip(record))

        ongoing_record = {
            **record,
            "active_item_id": "active-memory-current",
        }
        ongoing_draft = _draft_from_record(
            ongoing_record,
            work_type="maintenance",
        )
        self.assertEqual(ongoing_draft["start_time"], "2026-06-18T09:30")
        self.assertEqual(ongoing_draft["end_time"], "2026-06-18T18:30")

        metadata_only = {
            **record,
            "memory": {"updated_at": "2026-06-18 18:30:00"},
        }
        self.assertNotIn("已用历史记忆", _memory_status_chip(metadata_only))

    def test_api_workbench_all_is_not_treated_as_real_work_type(self):
        main_text = (BIN_DIR / "clipflow_backend" / "main.py").read_text(encoding="utf-8")

        self.assertIn("if work_type != \"all\" and work_type not in NOTICE_TYPE_BY_WORK_TYPE", main_text)
        self.assertIn("work_type in NOTICE_TYPE_BY_WORK_TYPE", main_text)

    def test_workbench_lite_active_title_falls_back_to_notice_text(self):
        from lan_bitable_template_portal.workbench_lite import _record_title

        self.assertEqual(
            _record_title(
                {
                    "record_id": "recv-title-fallback",
                    "work_type": "polling",
                    "text": "【设备轮巡】状态：更新\n【标题】EA118机房E楼制冷单元轮巡通告\n【进度】测试",
                }
            ),
            "EA118机房E楼制冷单元轮巡通告",
        )
        self.assertEqual(
            _record_title(
                {
                    "record_id": "recv-change-fallback",
                    "work_type": "change",
                    "text": "【变更通告】状态：开始\n【名称】EA118机房E楼测试变更\n【进度】测试",
                }
            ),
            "EA118机房E楼测试变更",
        )

    def test_workbench_notice_command_does_not_reuse_stale_site_photo_uploads(self):
        service = _TestMaintenancePortalService()

        expanded = service.expand_workbench_action_command(
            {
                "command_format": "notice_command",
                "action": "update",
                "scope": "E",
                "work_type": "maintenance",
                "active_item_id": "active-1",
                "target_record_id": "target-1",
                "record_id": "target-1",
                "patch": {
                    "scope": "E",
                    "work_type": "maintenance",
                    "action": "update",
                    "progress": "测试更新",
                },
            },
            scope="E",
            ongoing_items=[
                {
                    "active_item_id": "active-1",
                    "record_id": "target-1",
                    "target_record_id": "target-1",
                    "work_type": "maintenance",
                    "title": "测试维保",
                    "extra_images": [
                        {
                            "upload_id": "expired-upload",
                            "file_name": "old.png",
                        }
                    ],
                }
            ],
        )

        self.assertEqual(expanded.get("target_record_id"), "target-1")
        self.assertEqual(expanded.get("progress"), "测试更新")
        self.assertNotIn("extra_images", expanded)
        self.assertNotIn("site_photos", expanded)

    def test_manual_notice_command_requires_explicit_source_binding_choice(self):
        service = _TestMaintenancePortalService()
        base_payload = {
            "command_format": "notice_command",
            "action": "start",
            "scope": "D",
            "work_type": "repair",
            "record_id": "manual:lite:D:repair",
            "patch": {
                "manual": True,
                "manual_id": "manual:lite:D:repair",
                "manual_binding_required": True,
                "scope": "D",
                "work_type": "repair",
                "title": "D楼纯手填检修",
            },
        }

        with self.assertRaisesRegex(PortalError, "必须选择绑定计划通告或不绑定"):
            service.expand_workbench_action_command(
                base_payload,
                scope="D",
                ongoing_items=[],
            )

        unbound_payload = json.loads(json.dumps(base_payload))
        unbound_payload["source_record_id"] = "repair-source-must-be-cleared"
        unbound_payload["patch"]["source_record_id"] = "repair-source-must-be-cleared"
        unbound_payload["patch"]["manual_binding_choice"] = "unbound"
        expanded_unbound = service.expand_workbench_action_command(
            unbound_payload,
            scope="D",
            ongoing_items=[],
        )
        self.assertNotIn("source_record_id", expanded_unbound)
        self.assertEqual(expanded_unbound["manual_binding_choice"], "unbound")

        bound_payload = json.loads(json.dumps(base_payload))
        bound_payload["source_record_id"] = "repair-pending-source"
        bound_payload["record_id"] = "repair-pending-source"
        bound_payload["patch"]["source_record_id"] = "repair-pending-source"
        bound_payload["patch"]["record_id"] = "repair-pending-source"
        bound_payload["patch"]["manual_binding_choice"] = "bind"
        with patch.object(
            service,
            "validate_manual_source_binding",
            return_value={"source_record_id": "repair-pending-source"},
        ) as validate_binding:
            expanded_bound = service.expand_workbench_action_command(
                bound_payload,
                scope="D",
                ongoing_items=[],
            )
        validate_binding.assert_called_once()
        self.assertEqual(
            expanded_bound.get("source_record_id"),
            "repair-pending-source",
        )
        self.assertEqual(
            expanded_bound.get("repair_management_record_id"),
            "repair-pending-source",
        )
        self.assertFalse(expanded_bound.get("target_record_id"))
        self.assertEqual(expanded_bound["manual_binding_choice"], "bind")

    def test_repair_notice_command_recovers_management_relation_from_ongoing(self):
        service = _TestMaintenancePortalService()

        expanded = service.expand_workbench_action_command(
            {
                "command_format": "notice_command",
                "action": "update",
                "scope": "E",
                "work_type": WORK_TYPE_REPAIR,
                "active_item_id": "active-repair-linked",
                "target_record_id": "rec_repair_target",
                "record_id": "rec_repair_target",
                "patch": {
                    "scope": "E",
                    "work_type": WORK_TYPE_REPAIR,
                    "action": "update",
                    "progress": "维修工作继续进行",
                },
            },
            scope="E",
            ongoing_items=[
                {
                    "active_item_id": "active-repair-linked",
                    "target_record_id": "rec_repair_target",
                    "record_id": "rec_repair_target",
                    "repair_management_record_id": "rec_repair_summary",
                    "work_type": WORK_TYPE_REPAIR,
                    "notice_type": "设备检修",
                    "title": "EA118机房E楼精密空调检修",
                    "building": "E楼",
                }
            ],
        )

        self.assertEqual(
            expanded.get("repair_management_record_id"),
            "rec_repair_summary",
        )
        self.assertEqual(expanded.get("target_record_id"), "rec_repair_target")

    def test_qt_pending_update_does_not_reuse_stale_extra_images(self):
        workflow_path = BIN_DIR / "upload_event_module" / "ui" / "main_window_workflow.py"
        text = workflow_path.read_text(encoding="utf-8", errors="ignore")

        self.assertNotIn(
            'if not payload.get("extra_images") and existing.get("extra_images")',
            text,
        )
        self.assertNotIn('merged["extra_images"] = existing.get("extra_images")', text)

    def test_workbench_lite_blocks_invalid_duration_before_submit(self):
        from lan_bitable_template_portal.workbench_lite import render_workbench_lite

        html = render_workbench_lite(
            payload={
                "records": [],
                "ongoing": [],
                "daily_summary": {"stats": {}},
                "record_type_counts": {},
                "ongoing_type_counts": {},
            },
            session={"user": {"name": "测试"}},
            scope="E",
            work_type="polling",
            manual=True,
            scope_options=[{"value": "E", "label": "E楼"}],
        )

        self.assertIn("function noticeDurationIssue(form)", html)
        self.assertIn("startLabel", html)
        self.assertIn("endLabel", html)
        self.assertIn("开始时间", html)
        self.assertIn("结束时间", html)
        self.assertIn("发现故障时间", html)
        self.assertIn("期望完成时间", html)
        self.assertIn("不能少于1小时", html)
        self.assertIn("const durationIssue = noticeDurationIssue(form)", html)
        self.assertIn("const durationIssue = noticeDurationIssue(targetForm)", html)

    def test_repair_management_records_use_full_repair_source_table(self):
        service = _TestMaintenancePortalService()
        meta = FieldMeta(
            field_id="fld_title",
            field_name="维修名称",
            ui_type="Text",
            field_type=1,
            is_primary=True,
            options_map={},
            option_names=[],
            has_formula=False,
        )
        calls = {}

        def fake_load_fields(*, app_token, table_id):
            calls["fields"] = (app_token, table_id)
            return [meta], {meta.field_name: meta}

        def fake_load_records(**kwargs):
            calls["records"] = kwargs
            return [
                {
                    "record_id": "rec_repair_1",
                    "created_time": "1",
                    "last_modified_time": "2",
                    "display_fields": {"维修名称": "测试检修"},
                    "raw_fields": {"维修名称": "测试检修"},
                }
            ]

        service._load_table_fields = fake_load_fields  # type: ignore[method-assign]
        service._load_table_records = fake_load_records  # type: ignore[method-assign]
        service._load_repair_followup_snapshot = (  # type: ignore[method-assign]
            lambda **_kwargs: ([], {}, [])
        )

        payload = service.get_repair_management_records(query="测试", limit=20)

        self.assertEqual(calls["fields"], (REPAIR_SOURCE_APP_TOKEN, REPAIR_SOURCE_TABLE_ID))
        self.assertEqual(calls["records"]["app_token"], REPAIR_SOURCE_APP_TOKEN)
        self.assertEqual(calls["records"]["table_id"], REPAIR_SOURCE_TABLE_ID)
        self.assertEqual(calls["records"].get("view_id", ""), "")
        self.assertEqual(payload["records"][0]["record_id"], "rec_repair_1")

    def test_repair_management_records_filter_by_scope(self):
        service = _TestMaintenancePortalService()
        meta = FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False)

        def fake_load_fields(*, app_token, table_id):
            return [meta], {meta.field_name: meta}

        def fake_load_records(**_kwargs):
            return [
                {
                    "record_id": "rec_repair_e",
                    "display_fields": {
                        "维修名称": "EA118机房E楼冷源检修",
                        "所属数据中心/楼栋-使用": "南通E楼",
                    },
                    "raw_fields": {},
                },
                {
                    "record_id": "rec_repair_a",
                    "display_fields": {
                        "维修名称": "EA118机房A楼冷源检修",
                        "所属数据中心/楼栋-使用": "南通A楼",
                    },
                    "raw_fields": {},
                },
            ]

        service._load_table_fields = fake_load_fields  # type: ignore[method-assign]
        service._load_table_records = fake_load_records  # type: ignore[method-assign]

        payload = service.get_repair_management_records(scope="E", limit=20)

        self.assertEqual([item["record_id"] for item in payload["records"]], ["rec_repair_e"])

    def test_repair_management_records_include_followup_summary(self):
        service = _TestMaintenancePortalService()
        title_meta = FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False)

        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: ([title_meta], {title_meta.field_name: title_meta})
        )
        service._load_table_records = (  # type: ignore[method-assign]
            lambda **_kwargs: [
                {
                    "record_id": "rec_repair_summary",
                    "display_fields": {
                        "维修名称": "测试维修项目",
                        "当前维修进度": "99%",
                        "最新维修跟进时间": "2026-07-15 10:20",
                    },
                    "raw_fields": {
                        "维修跟进记录": "rec_followup_1,rec_followup_2",
                    },
                }
            ]
        )
        service._load_repair_followup_snapshot = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [],
                {},
                [
                    {
                        "record_id": "rec_followup_1",
                        "raw_fields": {
                            REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_repair_summary",
                            "维修进度": "20%",
                        },
                        "display_fields": {"创建时间": "2026-07-15 09:20"},
                    },
                    {
                        "record_id": "rec_followup_2",
                        "raw_fields": {
                            REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_repair_summary",
                            "维修进度": "65%",
                        },
                        "display_fields": {"创建时间": "2026-07-15 10:20"},
                    },
                ],
            )
        )

        payload = service.get_repair_management_records(limit=20)

        record = payload["records"][0]
        self.assertEqual(record["followup_count"], 2)
        self.assertEqual(record["progress_percent"], 65)
        self.assertEqual(record["latest_followup_time"], "2026-07-15 10:20")

    def test_repair_management_record_payload_uses_flow_l_and_latest_followup(self):
        service = _TestMaintenancePortalService()
        workflow_meta = FieldMeta(
            "fld_flow_l",
            "流程-L",
            "SingleSelect",
            3,
            False,
            {"opt_in_progress": "维修中"},
            ["维修中"],
            False,
        )
        project = {
            "record_id": "rec_repair_latest",
            "display_fields": {
                "维修名称": "最新跟进进度测试",
                "当前维修进度": "100%",
            },
            "raw_fields": {"流程-L": "opt_in_progress"},
        }
        followups = [
            {
                "record_id": "rec_followup_new",
                "created_time": "2026-07-22 11:00",
                "raw_fields": {"维修进度": "45%"},
                "display_fields": {},
            },
            {
                "record_id": "rec_followup_old",
                "created_time": "2026-07-22 10:00",
                "raw_fields": {"维修进度": "80%"},
                "display_fields": {},
            },
        ]

        payload = service._repair_management_record_payload(
            project,
            meta_by_name={"流程-L": workflow_meta},
            authoritative_followups=followups,
        )

        self.assertEqual(payload["workflow"], "维修中")
        self.assertEqual(payload["followup_count"], 2)
        self.assertEqual(payload["progress_percent"], 45)
        self.assertEqual(payload["display_fields"]["当前维修进度"], "45%")

    def test_repair_management_record_payload_without_followup_is_zero_percent(self):
        service = _TestMaintenancePortalService()

        payload = service._repair_management_record_payload(
            {
                "record_id": "rec_repair_no_followup",
                "display_fields": {
                    "维修名称": "无跟进项目",
                    "当前维修进度": "100%",
                },
                "raw_fields": {"流程-L": "未开始"},
            },
            authoritative_followups=[],
        )

        self.assertEqual(payload["workflow"], "未开始")
        self.assertEqual(payload["followup_count"], 0)
        self.assertEqual(payload["progress_percent"], 0)
        self.assertEqual(payload["display_fields"]["当前维修进度"], "0%")

    def test_repair_management_record_payload_formats_epoch_times(self):
        service = _TestMaintenancePortalService()
        created = dt.datetime(2026, 7, 16, 8, 30)
        modified = dt.datetime(2026, 7, 16, 9, 45)

        payload = service._repair_management_record_payload(
            {
                "record_id": "rec_repair_time",
                "created_time": str(int(created.timestamp() * 1000)),
                "last_modified_time": str(int(modified.timestamp() * 1000)),
                "display_fields": {"维修名称": "时间格式测试"},
                "raw_fields": {},
            }
        )

        self.assertEqual(payload["created_time"], "2026-07-16 08:30")
        self.assertEqual(payload["last_modified_time"], "2026-07-16 09:45")

    def test_repair_management_record_payload_uses_workflow_field_option(self):
        service = _TestMaintenancePortalService()
        workflow_meta = FieldMeta(
            "fld_flow",
            "流程",
            "SingleSelect",
            3,
            False,
            {"opt_in_progress": "维修进行中"},
            ["维修进行中"],
            False,
        )

        payload = service._repair_management_record_payload(
            {
                "record_id": "rec_repair_workflow",
                "display_fields": {"维修名称": "流程字段测试", "当前维修进度": "100%"},
                "raw_fields": {"流程": "opt_in_progress"},
            },
            meta_by_name={"流程": workflow_meta},
        )

        self.assertEqual(payload["workflow"], "维修进行中")
        self.assertEqual(payload["progress_percent"], 100)

    def test_repair_management_followup_count_ignores_stale_backlink(self):
        service = _TestMaintenancePortalService()
        title_meta = FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False)
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: ([title_meta], {title_meta.field_name: title_meta})
        )
        service._load_table_records = (  # type: ignore[method-assign]
            lambda **_kwargs: [
                {
                    "record_id": "rec_repair_stale",
                    "display_fields": {"维修名称": "无实际跟进的项目"},
                    "raw_fields": {"维修跟进记录": "rec_deleted_followup"},
                }
            ]
        )
        service._load_repair_followup_snapshot = (  # type: ignore[method-assign]
            lambda **_kwargs: ([], {}, [])
        )

        payload = service.get_repair_management_records(limit=20)

        self.assertEqual(payload["records"][0]["followup_count"], 0)

    def test_repair_followup_snapshot_parent_uses_record_id_parser(self):
        envelope = MaintenancePortalService._repair_snapshot_record_payload(
            {
                "record_id": "rec_followup_parent",
                "display_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "维修项目显示名称",
                },
                "raw_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: {
                        "link_record_ids": ["rec_summary_parent"]
                    },
                },
            },
            parent_field_name=REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
        )

        self.assertEqual(envelope["parent_record_id"], "rec_summary_parent")

    def test_repair_followup_local_snapshot_count_verifies_record_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_snapshots_enabled = True
            service._state_store.replace_repair_snapshot(
                REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS,
                records=[
                    {
                        "record_id": "rec_followup_stale_index",
                        "parent_record_id": "rec_summary_wrong",
                        "payload": {
                            "record_id": "rec_followup_stale_index",
                            "display_fields": {
                                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary_actual"
                            },
                            "raw_fields": {
                                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary_actual"
                            },
                        },
                    }
                ],
            )

            counts = service._repair_followup_counts_from_local_snapshot(
                ["rec_summary_wrong", "rec_summary_actual"]
            )

            self.assertEqual(
                counts,
                {
                    "rec_summary_wrong": 0,
                    "rec_summary_actual": 1,
                },
            )

    def test_repair_management_records_support_offset_pagination(self):
        service = _TestMaintenancePortalService()
        meta = FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False)

        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: ([meta], {meta.field_name: meta})
        )
        service._load_table_records = (  # type: ignore[method-assign]
            lambda **_kwargs: [
                {
                    "record_id": f"rec_repair_{index}",
                    "last_modified_time": str(index),
                    "display_fields": {"维修名称": f"维修项目{index}"},
                    "raw_fields": {},
                }
                for index in range(5)
            ]
        )

        payload = service.get_repair_management_records(limit=2, offset=1)

        self.assertEqual(
            [item["record_id"] for item in payload["records"]],
            ["rec_repair_3", "rec_repair_2"],
        )
        self.assertEqual(payload["total"], 5)
        self.assertEqual(payload["offset"], 1)
        self.assertTrue(payload["has_more"])

    def test_repair_management_records_focus_record_selects_containing_page(self):
        service = _TestMaintenancePortalService()
        meta = FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False)

        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: ([meta], {meta.field_name: meta})
        )
        service._load_table_records = (  # type: ignore[method-assign]
            lambda **_kwargs: [
                {
                    "record_id": f"rec_repair_{index}",
                    "last_modified_time": str(index),
                    "display_fields": {"维修名称": f"维修项目{index}"},
                    "raw_fields": {},
                }
                for index in range(5)
            ]
        )

        payload = service.get_repair_management_records(
            limit=2,
            offset=0,
            focus_record_id="rec_repair_1",
        )

        self.assertEqual(
            [item["record_id"] for item in payload["records"]],
            ["rec_repair_2", "rec_repair_1"],
        )
        self.assertEqual(payload["offset"], 2)

    def test_repair_management_records_support_active_completed_and_all_states(self):
        service = _TestMaintenancePortalService()
        title_meta = FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False)
        workflow_meta = FieldMeta(
            "fld_flow_l",
            "流程-L",
            "SingleSelect",
            3,
            False,
            {"opt_completed": "维修完成"},
            ["维修完成"],
            False,
        )
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [title_meta, workflow_meta],
                {title_meta.field_name: title_meta, workflow_meta.field_name: workflow_meta},
            )
        )
        service._load_table_records = (  # type: ignore[method-assign]
            lambda **_kwargs: [
                {
                    "record_id": "rec_active_new",
                    "last_modified_time": "3",
                    "display_fields": {"维修名称": "新项目", "流程-L": "维修中"},
                    "raw_fields": {},
                },
                {
                    "record_id": "rec_completed",
                    "last_modified_time": "2",
                    "display_fields": {"维修名称": "已完成项目"},
                    "raw_fields": {"流程-L": "opt_completed"},
                },
                {
                    "record_id": "rec_active_old",
                    "last_modified_time": "1",
                    "display_fields": {"维修名称": "旧项目"},
                    "raw_fields": {"流程-L": "未开始"},
                },
            ]
        )

        all_payload = service.get_repair_management_records(limit=10, offset=0)
        active_payload = service.get_repair_management_records(
            state="active", limit=1, offset=0
        )
        completed_payload = service.get_repair_management_records(
            state="completed", limit=10, offset=0
        )

        self.assertEqual(all_payload["total"], 3)
        self.assertEqual(all_payload["state"], "all")
        self.assertEqual(active_payload["total"], 2)
        self.assertEqual(
            [item["record_id"] for item in active_payload["records"]],
            ["rec_active_new"],
        )
        self.assertEqual(active_payload["records"][0]["workflow"], "维修中")
        self.assertFalse(active_payload["records"][0]["read_only"])
        self.assertTrue(active_payload["has_more"])
        self.assertEqual(completed_payload["total"], 1)
        self.assertEqual(completed_payload["state"], "completed")
        self.assertEqual(
            [item["record_id"] for item in completed_payload["records"]],
            ["rec_completed"],
        )
        self.assertTrue(completed_payload["records"][0]["is_completed"])
        self.assertTrue(completed_payload["records"][0]["read_only"])

    def test_repair_management_status_separates_active_and_completed_projects(self):
        service = _TestMaintenancePortalService()
        projects = [
            {
                "record_id": "rec_without_followup",
                "display_fields": {
                    "维修名称": "E楼待首次跟进",
                    "所属数据中心/楼栋-使用": "南通E楼",
                    "所属专业": "暖通",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_in_progress",
                "display_fields": {
                    "维修名称": "E楼维修进行中",
                    "所属数据中心/楼栋-使用": "南通E楼",
                    "所属专业": "电气",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_completed",
                "display_fields": {
                    "维修名称": "E楼已完成",
                    "所属数据中心/楼栋-使用": "南通E楼",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_completed_today",
                "display_fields": {
                    "维修名称": "E楼今日完成",
                    "所属数据中心/楼栋-使用": "南通E楼",
                    "流程": "维修完成",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_other_scope",
                "display_fields": {
                    "维修名称": "A楼待首次跟进",
                    "所属数据中心/楼栋-使用": "南通A楼",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_workflow_completed_stale",
                "display_fields": {
                    "维修名称": "E楼流程已完成但跟进缺失",
                    "所属数据中心/楼栋-使用": "南通E楼",
                    "流程": "维修完成",
                },
                "raw_fields": {},
            },
        ]
        followups = [
            {
                "record_id": "rec_followup_old_done",
                "display_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_in_progress",
                    "维修进度": "100%",
                    "维修进展描述": "上一阶段已完成",
                    "创建时间": "2026-07-13 09:00",
                },
                "raw_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_in_progress",
                },
            },
            {
                "record_id": "rec_followup_half",
                "display_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_in_progress",
                    "维修进度": "50%",
                    "维修进展描述": "处理中",
                    "创建时间": "2026-07-13 10:00",
                },
                "raw_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_in_progress",
                },
            },
            {
                "record_id": "rec_followup_done",
                "display_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_completed",
                    "维修进度": "1",
                    "创建时间": "2026-07-13 11:00",
                },
                "raw_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_completed",
                },
            },
            {
                "record_id": "rec_followup_done_today",
                "display_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_completed_today",
                    "维修进度": "100%",
                    "维修结束时间": f"{dt.datetime.now().astimezone():%Y-%m-%d} 12:00",
                    "创建时间": f"{dt.datetime.now().astimezone():%Y-%m-%d} 11:00",
                },
                "raw_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_completed_today",
                },
            },
        ]
        service._load_repair_management_status_sources = (  # type: ignore[method-assign]
            lambda **_kwargs: (projects, followups)
        )

        payload = service.get_repair_management_status(scope="E")

        self.assertEqual(
            [item["record_id"] for item in payload["records"]],
            ["rec_without_followup", "rec_in_progress"],
        )
        self.assertEqual(payload["stats"]["total"], 2)
        self.assertEqual(payload["stats"]["without_followup"], 1)
        self.assertEqual(payload["stats"]["in_progress"], 1)
        self.assertEqual(payload["stats"]["completed_total"], 3)
        self.assertEqual(payload["stats"]["completed_today"], 1)
        self.assertEqual(payload["records"][1]["progress_percent"], 50)

        today_payload = service.get_repair_management_status(
            scope="E",
            state="completed",
            period="today",
        )
        self.assertEqual(
            [item["record_id"] for item in today_payload["records"]],
            ["rec_completed_today"],
        )
        self.assertEqual(today_payload["records"][0]["state"], "completed")
        self.assertEqual(
            today_payload["records"][0]["status_label"],
            "历史已完成",
        )

    def test_repair_management_scope_overview_uses_project_and_followup_state(self):
        service = _TestMaintenancePortalService()
        now_text = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M")
        projects = [
            {
                "record_id": "rec_e_pending",
                "display_fields": {
                    "维修名称": "E楼待检修",
                    "所属数据中心/楼栋-使用": "南通E楼",
                    "故障发生时间": now_text,
                    "流程": "未开始",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_e_progress",
                "display_fields": {
                    "维修名称": "E楼进行中",
                    "所属数据中心/楼栋-使用": "南通E楼",
                    "故障发生时间": now_text,
                    "流程": "维修中",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_a_completed",
                "display_fields": {
                    "维修名称": "A楼已完成",
                    "所属数据中心/楼栋-使用": "南通A楼",
                    "故障发生时间": now_text,
                    "流程": "维修完成",
                },
                "raw_fields": {},
            },
        ]
        followups = [
            {
                "record_id": "rec_followup_e",
                "display_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_e_progress",
                    "维修进度": "40%",
                },
                "raw_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_e_progress",
                },
            }
        ]
        service._load_repair_management_status_sources = (  # type: ignore[method-assign]
            lambda **_kwargs: (projects, followups)
        )

        payload = service.get_repair_management_scope_overview(scopes=["E", "A"])

        self.assertEqual(
            payload["scopes"]["E"],
            {
                "scope": "E",
                "pending": 1,
                "in_progress": 1,
                "year_total": 2,
                "month_total": 2,
            },
        )
        self.assertEqual(payload["scopes"]["A"]["pending"], 0)
        self.assertEqual(payload["scopes"]["A"]["in_progress"], 0)
        self.assertEqual(payload["scopes"]["A"]["year_total"], 1)
        self.assertEqual(payload["scopes"]["A"]["month_total"], 1)
        self.assertEqual(
            payload["aggregate"],
            {
                "pending": 1,
                "in_progress": 1,
                "year_total": 3,
                "month_total": 3,
            },
        )

    def test_repair_management_status_uses_event_alarm_and_sent_time(self):
        service = _TestMaintenancePortalService()
        service._load_repair_management_status_sources = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [
                    {
                        "record_id": "rec_project",
                        "display_fields": {
                            "维修名称": "内部维修单名称",
                            "所属数据中心/楼栋-使用": "南通E楼",
                        },
                        "raw_fields": {"关联事件单": ["rec_event"]},
                    }
                ],
                [],
            )
        )
        service._load_repair_management_event_records = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [],
                {},
                [
                    {
                        "record_id": "rec_event",
                        "display_fields": {
                            "事件简述": "内部事件简述",
                            "告警描述": "E楼冷机高压告警",
                            "事件进展响应时间": "2026-07-14 09:30",
                            "机楼": "E楼",
                        },
                        "raw_fields": {},
                    }
                ],
            )
        )

        payload = service.get_repair_management_status(scope="E")

        self.assertEqual(payload["records"][0]["title"], "E楼冷机高压告警")
        self.assertEqual(
            payload["records"][0]["event_sent_time"],
            "2026-07-14 09:30",
        )
        self.assertEqual(
            payload["records"][0]["repair_title"],
            "内部维修单名称",
        )

        search_payload = service.get_repair_management_status(
            scope="E",
            query="冷机高压",
        )
        self.assertEqual(search_payload["total"], 1)
        self.assertEqual(
            search_payload["records"][0]["record_id"],
            "rec_project",
        )

    def test_repair_management_status_skips_archived_event_relation(self):
        service = _TestMaintenancePortalService()
        service._load_repair_management_event_records = (  # type: ignore[method-assign]
            lambda **_kwargs: ([], {}, [])
        )
        service._event_source_config = (  # type: ignore[method-assign]
            lambda: ("event_app", "event_table", "events")
        )
        requested_ids: list[str] = []

        def load_by_ids(**kwargs):
            record_id = str((kwargs.get("record_ids") or [""])[0])
            requested_ids.append(record_id)
            raise PortalError(
                "飞书接口失败: code=1254043, msg=RecordIdNotFound"
            )

        service._load_table_records_by_ids = load_by_ids  # type: ignore[method-assign]
        records = [
            {
                "record_id": "rec_project",
                "source_event_id": "rec_archived_event",
                "title": "维修单中保存的告警描述",
                "event_sent_time": "2026-07-14 09:30",
            }
        ]

        warnings = service._enrich_repair_management_status_events(records)

        self.assertEqual(warnings, [])
        self.assertEqual(records[0]["title"], "维修单中保存的告警描述")
        self.assertTrue(records[0]["event_relation_stale"])
        self.assertEqual(
            records[0]["event_relation_status"],
            "来源事件已归档，当前显示维修单中已保存的信息",
        )
        self.assertEqual(requested_ids, ["rec_archived_event"])

        service._enrich_repair_management_status_events(records)
        self.assertEqual(requested_ids, ["rec_archived_event"])

        service._enrich_repair_management_status_events(
            records,
            force_refresh=True,
        )
        self.assertEqual(
            requested_ids,
            ["rec_archived_event", "rec_archived_event"],
        )

    def test_repair_management_datetime_ms_accepts_numeric_text(self):
        service = _TestMaintenancePortalService()
        epoch_ms = 1_752_470_200_000

        self.assertEqual(
            service._repair_management_datetime_ms(str(epoch_ms)),
            epoch_ms,
        )
        self.assertEqual(
            service._repair_management_datetime_ms(str(epoch_ms // 1000)),
            epoch_ms,
        )

    def test_repair_management_datetime_handles_excel_serial_values(self):
        service = _TestMaintenancePortalService()
        excel_serial = 46_218.5507407407
        expected = dt.datetime(1899, 12, 30) + dt.timedelta(days=excel_serial)

        self.assertEqual(
            service._repair_management_datetime_ms(excel_serial),
            int(expected.timestamp() * 1000),
        )
        self.assertEqual(
            service._normalize_field_value(
                "最新维修跟进时间",
                excel_serial,
                meta_by_name={
                    "最新维修跟进时间": FieldMeta(
                        "fld_latest",
                        "最新维修跟进时间",
                        "DateTime",
                        5,
                        False,
                        {},
                        [],
                        False,
                    )
                },
            ),
            expected.strftime("%Y-%m-%d %H:%M"),
        )

    def test_repair_snapshot_sort_time_does_not_use_windows_timestamp(self):
        payload = MaintenancePortalService._repair_snapshot_record_payload(
            {
                "record_id": "rec_epoch",
                "display_fields": {
                    "维修名称": "A楼测试维修",
                    "最新维修跟进时间": "1970-01-01 08:00",
                },
            }
        )

        self.assertEqual(payload["record_id"], "rec_epoch")
        self.assertEqual(payload["sort_time"], 8 * 60 * 60)

    def test_repair_followup_create_retries_summary_sync_without_duplicate_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = _TestMaintenancePortalService()
            service._state_store = LanPortalStateStore(
                Path(tmp) / "lan_portal_state.sqlite3"
            )
            summary = {
                "record_id": "rec_summary",
                "raw_fields": {},
                "display_fields": {},
            }
            meta = FieldMeta(
                "fld_progress",
                "维修进展描述",
                "Text",
                1,
                False,
                {},
                [],
                False,
            )
            service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: summary
            )
            service.get_repair_followup_records = (  # type: ignore[method-assign]
                lambda **_kwargs: {"total": 0}
            )
            service._ensure_repair_followup_parent_id_field = (  # type: ignore[method-assign]
                lambda: ([meta], {meta.field_name: meta})
            )
            service._prepare_repair_followup_fields = (  # type: ignore[method-assign]
                lambda **_kwargs: ({"维修进展描述": "处理中"}, [])
            )
            service._ensure_repair_followup_select_options = (  # type: ignore[method-assign]
                lambda prepared, meta_by_name: ([meta], meta_by_name)
            )
            calls = {"create": 0, "sync": 0}

            def fake_create(fields, _meta_by_name):
                calls["create"] += 1
                return (
                    {"data": {"record": {"record_id": "rec_followup_1"}}},
                    dict(fields),
                    [],
                )

            def fake_sync(**_kwargs):
                calls["sync"] += 1
                if calls["sync"] == 1:
                    raise PortalError("mock summary sync failure")
                return []

            service._create_repair_followup_fields = fake_create  # type: ignore[method-assign]
            service._sync_repair_management_from_followup = fake_sync  # type: ignore[method-assign]

            first = service.create_repair_followup_record(
                summary_record_id="rec_summary",
                fields={"维修进展描述": "处理中"},
                operation_id="followup-op-1",
                scope="E",
            )
            second = service.create_repair_followup_record(
                summary_record_id="rec_summary",
                fields={"维修进展描述": "处理中"},
                operation_id="followup-op-1",
                scope="E",
            )

            self.assertTrue(first["summary_sync_pending"])
            self.assertFalse(second["summary_sync_pending"])
            self.assertTrue(second["idempotent_replay"])
            self.assertEqual(first["record_id"], second["record_id"])
            self.assertFalse(
                any("暂未同步" in warning for warning in second["warnings"])
            )
            self.assertEqual(calls, {"create": 1, "sync": 2})

    def test_repair_followup_records_support_offset_pagination(self):
        service = _TestMaintenancePortalService()
        meta = FieldMeta("fld_progress", "维修进展描述", "Text", 1, False, {}, [], False)
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {"record_id": "rec_summary"}
        )
        service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: (
                [meta],
                {meta.field_name: meta},
                [
                    {
                        "record_id": f"rec_followup_{index}",
                        "raw_fields": {
                            REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary",
                        },
                        "display_fields": {
                            REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary",
                            "维修进展描述": f"进展{index}",
                            "创建时间": f"2026-07-0{index + 1} 10:00",
                        },
                    }
                    for index in range(3)
                ],
            )
        )
        service._load_repair_followup_brand_model_catalog = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {}
        )

        payload = service.get_repair_followup_records(
            summary_record_id="rec_summary",
            limit=1,
            offset=0,
            focus_record_id="rec_followup_1",
        )

        self.assertEqual(
            [item["record_id"] for item in payload["records"]],
            ["rec_followup_1"],
        )
        self.assertEqual(payload["total"], 3)
        self.assertEqual(payload["offset"], 1)
        self.assertTrue(payload["has_more"])

    def test_repair_followup_records_format_top_level_created_time(self):
        service = _TestMaintenancePortalService()
        created = dt.datetime(2026, 7, 16, 10, 5)
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {"record_id": "rec_summary"}
        )
        service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: (
                [],
                {},
                [
                    {
                        "record_id": "rec_followup_time",
                        "created_time": str(int(created.timestamp() * 1000)),
                        "raw_fields": {
                            REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary",
                        },
                        "display_fields": {"维修进展描述": "处理中"},
                    }
                ],
            )
        )
        service._load_repair_followup_brand_model_catalog = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {}
        )

        payload = service.get_repair_followup_records(
            summary_record_id="rec_summary",
        )

        self.assertEqual(payload["records"][0]["created_time"], "2026-07-16 10:05")

    def test_repair_followup_explicit_empty_cmdb_selection_clears_relation(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta(
                "fld_parent",
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
            FieldMeta(
                "fld_cmdb",
                REPAIR_FOLLOWUP_CMDB_FIELD_NAME,
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
        ]
        meta_by_name = {item.field_name: item for item in metas}

        prepared, warnings = service._prepare_repair_followup_fields(
            summary_record_id="rec_summary",
            fields={},
            cmdb_record_ids=[],
            summary_record={"display_fields": {}, "raw_fields": {}},
            existing_record={
                "display_fields": {},
                "raw_fields": {
                    REPAIR_FOLLOWUP_CMDB_FIELD_NAME: ["rec_cmdb_old"],
                },
            },
            meta_by_name=meta_by_name,
        )

        self.assertEqual(warnings, [])
        self.assertIsNone(prepared[REPAIR_FOLLOWUP_CMDB_FIELD_NAME])

    def test_repair_management_coerce_skips_invalid_select_values(self):
        single = FieldMeta(
            "fld_device",
            "设备名称",
            "SingleSelect",
            3,
            False,
            {"opt_water": "水冷型精密空调"},
            ["水冷型精密空调", "风冷型精密空调"],
            False,
        )
        multi = FieldMeta(
            "fld_tags",
            "所属专业",
            "MultiSelect",
            4,
            False,
            {"opt_electric": "电气"},
            ["电气", "暖通"],
            False,
        )

        prepared, warnings = MaintenancePortalService._coerce_repair_management_fields(
            {
                "设备名称": "精密空调",
                "所属专业": ["opt_electric", "不存在专业"],
            },
            {single.field_name: single, multi.field_name: multi},
        )

        self.assertNotIn("设备名称", prepared)
        self.assertEqual(prepared["所属专业"], ["电气"])
        self.assertTrue(any("设备名称" in warning for warning in warnings))
        self.assertTrue(any("不存在专业" in warning for warning in warnings))

    def test_repair_management_coerce_accepts_select_option_id_and_case(self):
        meta = FieldMeta(
            "fld_device",
            "设备名称",
            "SingleSelect",
            3,
            False,
            {"opt_water": "水冷型精密空调"},
            ["水冷型精密空调"],
            False,
        )

        by_id, id_warnings = MaintenancePortalService._coerce_repair_management_fields(
            {"设备名称": "opt_water"},
            {meta.field_name: meta},
        )
        by_name, name_warnings = MaintenancePortalService._coerce_repair_management_fields(
            {"设备名称": "水冷型精密空调"},
            {meta.field_name: meta},
        )

        self.assertEqual(by_id["设备名称"], "水冷型精密空调")
        self.assertEqual(by_name["设备名称"], "水冷型精密空调")
        self.assertEqual(id_warnings, [])
        self.assertEqual(name_warnings, [])

    def test_repair_management_coerce_normalizes_python_list_multiselect(self):
        source = FieldMeta(
            "fld_source",
            "对应来源",
            "MultiSelect",
            4,
            False,
            {"opt_bms": "BMS系统"},
            ["方舟系统", "BMS系统"],
            False,
        )

        prepared, warnings = MaintenancePortalService._coerce_repair_management_fields(
            {"对应来源": "['BMS系统']"},
            {source.field_name: source},
        )

        self.assertEqual(prepared["对应来源"], ["BMS系统"])
        self.assertEqual(warnings, [])

    def test_repair_management_coerce_accepts_new_source_and_normalizes_alias(self):
        source = FieldMeta(
            "fld_source",
            "对应来源",
            "MultiSelect",
            4,
            False,
            {"opt_bms": "BMS系统"},
            ["BMS系统", "方舟系统", "['巡检发现']"],
            False,
        )

        prepared, warnings = MaintenancePortalService._coerce_repair_management_fields(
            {"对应来源": ["巡检发现", "BMS"]},
            {source.field_name: source},
        )

        self.assertEqual(prepared["对应来源"], ["巡检发现", "BMS系统"])
        self.assertEqual(warnings, [])

    def test_repair_management_coerce_allows_custom_followup_model(self):
        model = FieldMeta(
            "fld_model",
            "设备型号",
            "SingleSelect",
            3,
            False,
            {"opt_known": "已登记型号"},
            ["已登记型号"],
            False,
        )

        prepared, warnings = MaintenancePortalService._coerce_repair_management_fields(
            {"设备型号": "现场手填新型号"},
            {model.field_name: model},
            custom_single_select_fields={"设备型号"},
        )

        self.assertEqual(prepared["设备型号"], "现场手填新型号")
        self.assertEqual(warnings, [])

    def test_repair_management_notice_prefill_maps_current_project_fields(self):
        service = _TestMaintenancePortalService()
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {
                "record_id": "rec_summary",
                "display_fields": {
                    "维修名称": "EA118机房E楼精密空调检修",
                    "所属数据中心/楼栋-使用": "南通E楼",
                    "所属专业": "opt3jdJJb7",
                    "对应事件等级": "opt8k46U3S",
                    "对应来源": "optt3FGRvz",
                    "故障发生时间": "2026-07-10 09:30",
                    "期望完成时间": "2026-07-10 18:30",
                    "设备编号": "E-219-CRAH-01",
                    "设备名称": "精密空调",
                    "故障发生现象描述": "压差偏大",
                    "故障维修原因": "过滤器堵塞",
                    "影响范围": "不影响业务",
                    "维修方": "自维",
                    "解决方案": "清洗过滤器",
                    "更换备件名称": "过滤器",
                    "更换备件数量": "2",
                    "维修进展描述": "人员已就位",
                },
                "raw_fields": {
                    REPAIR_MANAGEMENT_REPAIR_LINK_STORAGE_FIELD_NAME: "rec_repair_source"
                },
            }
        )

        payload = service.repair_management_notice_prefill(
            "rec_summary",
            scope="E",
        )

        self.assertEqual(payload["repair_management_record_id"], "rec_summary")
        self.assertEqual(payload["source_record_id"], "rec_summary")
        self.assertEqual(payload["target_record_id"], "rec_repair_source")
        self.assertEqual(payload["action"], "update")
        self.assertEqual(payload["source_record"]["record_id"], "rec_summary")
        self.assertEqual(payload["source_record"]["work_type"], WORK_TYPE_REPAIR)
        draft = payload["draft"]
        self.assertEqual(draft["title"], "EA118机房E楼精密空调检修")
        self.assertEqual(draft["specialty"], "暖通")
        self.assertEqual(draft["level"], "中")
        self.assertEqual(draft["end_time"], "2026-07-10 09:30")
        self.assertEqual(draft["start_time"], "2026-07-10 18:30")
        self.assertEqual(draft["repair_device"], "E-219-CRAH-01精密空调")
        self.assertEqual(draft["reason"], "过滤器堵塞")
        self.assertEqual(draft["spare_parts"], "过滤器 × 2")
        self.assertEqual(draft["discovery"], "BMS系统")

    def test_repair_notice_event_candidates_only_include_incomplete_linked_projects(self):
        service = _TestMaintenancePortalService()
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [],
                {},
                [
                    {
                        "record_id": "rec_project_incomplete",
                        "last_modified_time": "2026-07-18 10:20",
                        "display_fields": {
                            "维修名称": "E楼精密空调维修",
                            "所属数据中心/楼栋-使用": "南通E楼",
                            "所属专业": "暖通",
                            "当前维修进度": "65%",
                        },
                        "raw_fields": {
                            "关联事件单": ["rec_event_incomplete"],
                            "当前维修进度": 0.65,
                        },
                    },
                    {
                        "record_id": "rec_project_complete",
                        "display_fields": {
                            "维修名称": "E楼已完成维修",
                            "所属数据中心/楼栋-使用": "南通E楼",
                            "当前维修进度": "100%",
                        },
                        "raw_fields": {
                            "关联事件单": ["rec_event_complete"],
                            "当前维修进度": 1,
                        },
                    },
                    {
                        "record_id": "rec_project_without_event",
                        "display_fields": {
                            "维修名称": "E楼未关联事件维修",
                            "所属数据中心/楼栋-使用": "南通E楼",
                            "当前维修进度": "20%",
                        },
                        "raw_fields": {},
                    },
                    {
                        "record_id": "rec_project_other_scope",
                        "display_fields": {
                            "维修名称": "A楼维修",
                            "所属数据中心/楼栋-使用": "南通A楼",
                            "当前维修进度": "30%",
                        },
                        "raw_fields": {
                            "关联事件单": ["rec_event_other_scope"],
                        },
                    },
                    {
                        "record_id": "rec_project_mismatched_event_scope",
                        "display_fields": {
                            "维修名称": "E楼错误关联维修",
                            "所属数据中心/楼栋-使用": "南通E楼",
                            "当前维修进度": "30%",
                        },
                        "raw_fields": {
                            "关联事件单": ["rec_event_a_scope"],
                            "当前维修进度": 0.3,
                        },
                    },
                ],
            )
        )
        service._load_repair_management_event_records = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [],
                {},
                [
                    {
                        "record_id": "rec_event_incomplete",
                        "display_fields": {
                            "事件简述": "E楼精密空调高压告警",
                            "告警描述": "压缩机高压告警",
                            "机楼": "E楼",
                            "专业": "暖通",
                            "事件等级": "I3",
                            "事件发现来源（统一）": "BMS系统",
                            "事件发生时间": "2026-07-18 09:30",
                        },
                        "raw_fields": {},
                    },
                    {
                        "record_id": "rec_event_a_scope",
                        "display_fields": {
                            "事件简述": "A楼越权事件",
                            "机楼": "A楼",
                            "事件发生时间": "2026-07-18 09:40",
                        },
                        "raw_fields": {},
                    }
                ],
            )
        )

        payload = service.list_repair_notice_event_candidates(
            scope="E",
            query="精密空调",
        )

        self.assertEqual(payload["total"], 1)
        candidate = payload["records"][0]
        self.assertEqual(candidate["event_record_id"], "rec_event_incomplete")
        self.assertEqual(
            candidate["repair_management_record_id"],
            "rec_project_incomplete",
        )
        self.assertEqual(candidate["title"], "E楼精密空调高压告警")
        self.assertEqual(candidate["progress_percent"], 65)
        self.assertEqual(candidate["progress_label"], "65%")
        self.assertEqual(candidate["building"], "E楼")
        self.assertEqual(candidate["specialty"], "暖通")

    def test_repair_notice_event_candidates_fall_back_when_event_snapshot_fails(self):
        service = _TestMaintenancePortalService()
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [],
                {},
                [
                    {
                        "record_id": "rec_project_snapshot_only",
                        "display_fields": {
                            "维修名称": "E楼精密空调维修",
                            "事件描述": "E楼精密空调告警",
                            "所属数据中心/楼栋-使用": "南通E楼",
                            "所属专业": "暖通",
                            "当前维修进度": "40%",
                        },
                        "raw_fields": {
                            "关联事件单": ["rec_event_snapshot_unavailable"],
                            "当前维修进度": 0.4,
                        },
                    }
                ],
            )
        )

        def fail_event_snapshot(**_kwargs):
            raise PortalError("事件快照暂不可用")

        service._load_repair_management_event_records = fail_event_snapshot  # type: ignore[method-assign]

        payload = service.list_repair_notice_event_candidates(scope="E")

        self.assertEqual(payload["total"], 1)
        self.assertEqual(
            payload["records"][0]["event_record_id"],
            "rec_event_snapshot_unavailable",
        )
        self.assertEqual(payload["records"][0]["progress_percent"], 40)
        self.assertIn("事件快照暂不可用", payload["warnings"][0])

    def test_repair_notice_event_binding_updates_current_project_without_sending(self):
        service = _TestMaintenancePortalService()
        records = {
            "rec_current_project": {
                "record_id": "rec_current_project",
                "display_fields": {
                    "维修名称": "E楼当前待发起检修",
                    "所属数据中心/楼栋-使用": "南通E楼",
                },
                "raw_fields": {},
            },
            "rec_candidate_project": {
                "record_id": "rec_candidate_project",
                "display_fields": {
                    "维修名称": "E楼事件转检修项目",
                    "所属数据中心/楼栋-使用": "南通E楼",
                    "当前维修进度": "60%",
                },
                "raw_fields": {
                    "关联事件单": ["rec_event_bind"],
                    "当前维修进度": 0.6,
                },
            },
        }
        event_link_meta = FieldMeta(
            "fld_event",
            "关联事件单",
            "Text",
            1,
            False,
            {},
            [],
            False,
        )
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [event_link_meta],
                {"关联事件单": event_link_meta},
                list(records.values()),
            )
        )
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda record_id, _scope, **_kwargs: records[record_id]
        )
        service._event_snapshot_record_for_repair = (  # type: ignore[method-assign]
            lambda **_kwargs: {
                "source_record_id": "rec_event_bind",
                "record_id": "rec_event_bind",
                "title": "E楼精密空调高压告警",
                "alarm_desc": "压缩机高压告警",
                "building": "E楼",
                "building_codes": ["E"],
                "specialty": "暖通",
                "occurrence_time": "2026-07-18 09:30",
                "display_fields": {},
                "raw_fields": {},
            }
        )
        update_calls = []
        service.update_repair_management_record = (  # type: ignore[method-assign]
            lambda record_id, fields, **kwargs: (
                update_calls.append(
                    {
                        "record_id": record_id,
                        "fields": fields,
                        **kwargs,
                    }
                )
                or {
                    "fields": {
                        "关联事件单": "rec_event_bind",
                        "故障维修原因": "压缩机高压告警",
                    },
                    "warnings": [],
                }
            )
        )
        service.repair_management_notice_prefill = (  # type: ignore[method-assign]
            lambda record_id, **_kwargs: {
                "source_record": records[record_id],
                "draft": {
                    "title": "E楼当前待发起检修",
                    "reason": "压缩机高压告警",
                },
            }
        )

        payload = service.bind_repair_notice_event(
            source_record_id="rec_current_project",
            event_record_id="rec_event_bind",
            candidate_project_record_id="rec_candidate_project",
            scope="E",
        )

        self.assertTrue(payload["saved"])
        self.assertEqual(payload["source_record_id"], "rec_current_project")
        self.assertEqual(payload["event_record_id"], "rec_event_bind")
        self.assertEqual(len(update_calls), 1)
        self.assertEqual(update_calls[0]["record_id"], "rec_current_project")
        self.assertEqual(update_calls[0]["fields"], {})
        self.assertEqual(update_calls[0]["source_event_id"], "rec_event_bind")
        self.assertEqual(update_calls[0]["source_repair_ids"], [])
        self.assertFalse(update_calls[0]["replace_source_relations"])
        self.assertFalse(update_calls[0]["validate_required"])
        self.assertTrue(payload["draft_complete"])
        self.assertEqual(payload["draft"]["reason"], "压缩机高压告警")
        self.assertEqual(payload["draft"]["solution"], "")
        self.assertEqual(payload["draft"]["spare_parts"], "")
        self.assertNotIn("target_record_id", payload)

        service.repair_management_notice_prefill = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                PortalError("事件详情暂不可用")
            )
        )
        saved_without_prefill = service.bind_repair_notice_event(
            source_record_id="rec_current_project",
            event_record_id="rec_event_bind",
            candidate_project_record_id="rec_candidate_project",
            scope="E",
        )

        self.assertTrue(saved_without_prefill["saved"])
        self.assertFalse(saved_without_prefill["draft_complete"])
        self.assertEqual(saved_without_prefill["draft"], {})
        self.assertEqual(
            saved_without_prefill["source_record"]["raw_fields"]["关联事件单"],
            "rec_event_bind",
        )
        self.assertIn("事件关联已保存", saved_without_prefill["warnings"][-1])
        self.assertIn("事件详情暂不可用", saved_without_prefill["warnings"][-1])

    def test_repair_notice_event_binding_rejects_completed_candidate(self):
        service = _TestMaintenancePortalService()
        records = {
            "rec_current_project": {
                "record_id": "rec_current_project",
                "display_fields": {
                    "所属数据中心/楼栋-使用": "南通E楼",
                },
                "raw_fields": {},
            },
            "rec_completed_project": {
                "record_id": "rec_completed_project",
                "display_fields": {
                    "所属数据中心/楼栋-使用": "南通E楼",
                    "当前维修进度": "100%",
                },
                "raw_fields": {
                    "关联事件单": ["rec_event_completed"],
                    "当前维修进度": 1,
                },
            },
        }
        event_link_meta = FieldMeta(
            "fld_event",
            "关联事件单",
            "Text",
            1,
            False,
            {},
            [],
            False,
        )
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [event_link_meta],
                {"关联事件单": event_link_meta},
                list(records.values()),
            )
        )
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda record_id, _scope, **_kwargs: records[record_id]
        )

        with self.assertRaisesRegex(PortalError, "检修进展已完成"):
            service.bind_repair_notice_event(
                source_record_id="rec_current_project",
                event_record_id="rec_event_completed",
                candidate_project_record_id="rec_completed_project",
                scope="E",
            )

    def test_repair_notice_event_binding_serializes_same_current_project(self):
        service = _TestMaintenancePortalService()
        records = {
            "rec_current_project": {
                "record_id": "rec_current_project",
                "display_fields": {
                    "维修名称": "E楼当前待发起检修",
                    "所属数据中心/楼栋-使用": "南通E楼",
                },
                "raw_fields": {},
            },
            "rec_candidate_one": {
                "record_id": "rec_candidate_one",
                "display_fields": {
                    "所属数据中心/楼栋-使用": "南通E楼",
                    "当前维修进度": "20%",
                },
                "raw_fields": {
                    "关联事件单": ["rec_event_one"],
                    "当前维修进度": 0.2,
                },
            },
            "rec_candidate_two": {
                "record_id": "rec_candidate_two",
                "display_fields": {
                    "所属数据中心/楼栋-使用": "南通E楼",
                    "当前维修进度": "40%",
                },
                "raw_fields": {
                    "关联事件单": ["rec_event_two"],
                    "当前维修进度": 0.4,
                },
            },
        }
        event_link_meta = FieldMeta(
            "fld_event",
            "关联事件单",
            "Text",
            1,
            False,
            {},
            [],
            False,
        )
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [event_link_meta],
                {"关联事件单": event_link_meta},
                list(records.values()),
            )
        )
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda record_id, _scope, **_kwargs: records[record_id]
        )
        service._event_snapshot_record_for_repair = (  # type: ignore[method-assign]
            lambda **kwargs: {
                "source_record_id": kwargs["record_id"],
                "record_id": kwargs["record_id"],
                "title": kwargs["record_id"],
                "building": "E楼",
                "building_codes": ["E"],
                "display_fields": {},
                "raw_fields": {},
            }
        )
        state = {"active": 0, "max_active": 0}
        state_lock = threading.Lock()

        def slow_update(_record_id, _fields, **kwargs):
            with state_lock:
                state["active"] += 1
                state["max_active"] = max(state["max_active"], state["active"])
            time.sleep(0.03)
            with state_lock:
                state["active"] -= 1
            return {
                "fields": {"关联事件单": kwargs["source_event_id"]},
                "warnings": [],
            }

        service.update_repair_management_record = slow_update  # type: ignore[method-assign]
        service.repair_management_notice_prefill = (  # type: ignore[method-assign]
            lambda record_id, **_kwargs: {
                "source_record": records[record_id],
                "draft": {"title": "E楼当前待发起检修"},
            }
        )
        results = []
        errors = []

        def bind(event_record_id, candidate_record_id):
            try:
                results.append(
                    service.bind_repair_notice_event(
                        source_record_id="rec_current_project",
                        event_record_id=event_record_id,
                        candidate_project_record_id=candidate_record_id,
                        scope="E",
                    )
                )
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(
                target=bind,
                args=("rec_event_one", "rec_candidate_one"),
            ),
            threading.Thread(
                target=bind,
                args=("rec_event_two", "rec_candidate_two"),
            ),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2.0)

        self.assertFalse(errors)
        self.assertEqual(len(results), 2)
        self.assertEqual(state["max_active"], 1)

    def test_repair_target_record_id_accepts_writable_text_mirror(self):
        service = _TestMaintenancePortalService()

        self.assertEqual(
            service._repair_target_record_id(
                {
                    "raw_fields": {
                        "设备检修关联": "rec_retired_relation",
                    },
                    "display_fields": {},
                }
            ),
            "",
        )
        self.assertEqual(
            service._repair_target_record_id(
                {
                    "source_table_id": REPAIR_MANAGEMENT_TABLE_ID,
                    "raw_fields": {
                        "设备检修关联": "rec_repair_target_text",
                    },
                    "display_fields": {},
                }
            ),
            "rec_repair_target_text",
        )
        self.assertEqual(
            service._repair_target_record_id(
                {
                    "raw_fields": {
                        "设备检修关联-L": "rec_repair_target_physical",
                    },
                    "display_fields": {},
                }
            ),
            "rec_repair_target_physical",
        )

    def test_repair_notice_actions_sync_project_link_title_and_action_times(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            metas = [
                FieldMeta("fld_link", "设备检修关联", "Text", 1, False, {}, [], False),
                FieldMeta("fld_title", "检修通告名称", "Text", 1, False, {}, [], False),
                FieldMeta("fld_start", "维修开始时间", "DateTime", 5, False, {}, [], False),
                FieldMeta(
                    "fld_end",
                    "维修结束时间（2026）",
                    "DateTime",
                    5,
                    False,
                    {},
                    [],
                    False,
                ),
                FieldMeta("fld_progress", "当前维修进度", "Progress", 2, False, {}, [], False),
                FieldMeta(
                    "fld_workflow_l",
                    "流程",
                    "SingleSelect",
                    3,
                    False,
                    {},
                    ["未开始", "维修中", "维修完成"],
                    False,
                ),
            ]
            meta_by_name = {meta.field_name: meta for meta in metas}
            current_record = {
                "record_id": "rec_repair_summary",
                "raw_fields": {},
                "display_fields": {},
            }
            captured: list[dict] = []
            workflow_updates: list[dict] = []
            snapshots: list[dict] = []
            service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: current_record
            )
            service._load_repair_management_project_records = (  # type: ignore[method-assign]
                lambda **_kwargs: (metas, meta_by_name, [current_record])
            )
            def fake_patch_record_fields(**kwargs):
                if set(kwargs.get("fields") or {}) == {"流程"}:
                    workflow_updates.append(kwargs)
                else:
                    captured.append(kwargs)
                return {"code": 0}

            service._patch_record_fields = fake_patch_record_fields  # type: ignore[method-assign]
            service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: (
                    [],
                    {},
                    [
                        {
                            "record_id": "rec_followup_complete",
                            "raw_fields": {"维修进度": 1},
                            "display_fields": {"创建时间": "2026-07-18 11:40"},
                        }
                    ],
                )
            )
            service._upsert_repair_snapshot_fields = (  # type: ignore[method-assign]
                lambda **kwargs: snapshots.append(kwargs)
            )
            service._invalidate_repair_management_status_cache = (  # type: ignore[method-assign]
                lambda: None
            )
            prepared = {
                "work_type": WORK_TYPE_REPAIR,
                "scope": "E",
                "source_record_id": "rec_repair_summary",
                "source_table_id": REPAIR_MANAGEMENT_TABLE_ID,
                "title": "EA118机房E楼精密空调检修",
                "response_time": "2026-07-18 09:15",
            }

            start_result = service.sync_repair_management_notice_action(
                prepared,
                action="start",
                target_record_id="rec_repair_target",
            )
            start_fields = captured[-1]["fields"]
            self.assertTrue(start_result["synced"])
            self.assertEqual(start_fields["设备检修关联"], "rec_repair_target")
            self.assertEqual(
                start_fields["检修通告名称"],
                "EA118机房E楼精密空调检修",
            )
            self.assertEqual(
                start_fields["维修开始时间"],
                MaintenancePortalService._repair_management_datetime_ms(
                    "2026-07-18 09:15"
                ),
            )
            self.assertNotIn("维修结束时间（2026）", start_fields)
            self.assertNotIn("当前维修进度", start_fields)
            self.assertEqual(workflow_updates[-1]["fields"], {"流程": "维修中"})

            captured.clear()
            current_record["raw_fields"] = {
                "维修开始时间": MaintenancePortalService._repair_management_datetime_ms(
                    "2026-07-18 09:15"
                )
            }
            service.sync_repair_management_notice_action(
                {**prepared, "response_time": "2026-07-18 10:30"},
                action="update",
                target_record_id="rec_repair_target",
            )
            update_fields = captured[-1]["fields"]
            self.assertEqual(
                set(update_fields),
                {"设备检修关联", "检修通告名称"},
            )

            captured.clear()
            service.sync_repair_management_notice_action(
                {**prepared, "response_time": "2026-07-18 11:45"},
                action="end",
                target_record_id="rec_repair_target",
            )
            end_fields = captured[-1]["fields"]
            self.assertEqual(
                end_fields["维修结束时间（2026）"],
                MaintenancePortalService._repair_management_datetime_ms(
                    "2026-07-18 11:45"
                ),
            )
            self.assertNotIn("维修开始时间", end_fields)
            self.assertNotIn("当前维修进度", end_fields)
            self.assertEqual(workflow_updates[-1]["fields"], {"流程": "维修完成"})
            self.assertEqual(len(snapshots), 5)

    def test_repair_workflow_requires_end_time_and_complete_followup(self):
        resolver = MaintenancePortalService._repair_management_workflow_for_state

        self.assertEqual(
            resolver(
                start_value="",
                end_value="",
                has_followup=False,
                latest_progress_percent=None,
            ),
            "未开始",
        )
        self.assertEqual(
            resolver(
                start_value="2026-07-18 09:15",
                end_value="",
                has_followup=False,
                latest_progress_percent=None,
            ),
            "维修中",
        )
        self.assertEqual(
            resolver(
                start_value="2026-07-18 09:15",
                end_value="2026-07-18 11:45",
                has_followup=True,
                latest_progress_percent=99,
            ),
            "维修中",
        )
        self.assertEqual(
            resolver(
                start_value="2026-07-18 09:15",
                end_value="2026-07-18 11:45",
                has_followup=True,
                latest_progress_percent=100,
            ),
            "维修完成",
        )

    def test_repair_workflow_failure_does_not_rollback_saved_project_data(self):
        service = _TestMaintenancePortalService()
        workflow_meta = FieldMeta(
            "fld_workflow_l",
            "流程",
            "SingleSelect",
            3,
            False,
            {},
            ["未开始", "维修中", "维修完成"],
            False,
        )
        service._patch_record_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: (_ for _ in ()).throw(PortalError("流程字段不可写"))
        )

        synced, warnings = service._sync_repair_management_workflow(
            record_id="rec_summary_flow_warning",
            workflow="维修中",
            meta_by_name={"流程": workflow_meta},
        )

        self.assertFalse(synced)
        self.assertEqual(len(warnings), 1)
        self.assertIn("流程暂未更新为“维修中”", warnings[0])

    def test_event_stats_use_repair_checkbox_completion_and_exact_levels(self):
        records = [
            {
                "level": "I2",
                "transfer_to_overhaul": True,
                "repair_completion_time": "",
                "status": "处理中",
            },
            {
                "level": "I1",
                "transfer_to_overhaul": True,
                "repair_completion_time": "2026-07-18 12:00",
                "status": "已结束",
            },
            {
                "level": "I3→I2（升级）",
                "transfer_to_overhaul": False,
                "repair_completion_time": "",
                "status": "处理中",
            },
            {
                "level": "I3",
                "transfer_to_overhaul": False,
                "repair_completion_time": "",
                "status": "处理中",
            },
        ]

        stats = MaintenancePortalService._event_stats_for_records(records)

        self.assertEqual(stats["under_repair"], 1)
        self.assertEqual(stats["i2_or_higher"], 3)
        self.assertEqual(stats["i3"], 1)

    def test_repair_notice_action_sync_uses_explicit_management_relation(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            metas = [
                FieldMeta("fld_link", "设备检修关联", "Text", 1, False, {}, [], False),
                FieldMeta("fld_title", "检修通告名称", "Text", 1, False, {}, [], False),
            ]
            meta_by_name = {meta.field_name: meta for meta in metas}
            current_record = {
                "record_id": "rec_repair_summary_explicit",
                "raw_fields": {},
                "display_fields": {},
            }
            ensured_ids: list[str] = []
            captured: list[dict] = []
            service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
                lambda record_id, _scope: (
                    ensured_ids.append(record_id) or current_record
                )
            )
            service._load_repair_management_project_records = (  # type: ignore[method-assign]
                lambda **_kwargs: (metas, meta_by_name, [current_record])
            )
            service._patch_record_fields = (  # type: ignore[method-assign]
                lambda **kwargs: captured.append(kwargs) or {"code": 0}
            )
            service._upsert_repair_snapshot_fields = (  # type: ignore[method-assign]
                lambda **_kwargs: None
            )
            service._invalidate_repair_management_status_cache = (  # type: ignore[method-assign]
                lambda: None
            )

            result = service.sync_repair_management_notice_action(
                {
                    "work_type": WORK_TYPE_REPAIR,
                    "scope": "E",
                    "manual": True,
                    "repair_management_record_id": "rec_repair_summary_explicit",
                    "title": "EA118机房E楼精密空调检修",
                },
                action="update",
                target_record_id="rec_repair_target_explicit",
            )

            self.assertTrue(result["synced"])
            self.assertEqual(ensured_ids, ["rec_repair_summary_explicit"])
            self.assertEqual(
                captured[-1]["record_id"],
                "rec_repair_summary_explicit",
            )
            self.assertEqual(
                captured[-1]["fields"]["设备检修关联"],
                "rec_repair_target_explicit",
            )

    def test_repair_notice_action_defers_duplex_link_without_blocking_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            metas = [
                FieldMeta(
                    "fld_link",
                    "设备检修关联",
                    "DuplexLink",
                    21,
                    False,
                    {},
                    [],
                    False,
                ),
                FieldMeta(
                    "fld_title",
                    "检修通告名称",
                    "Text",
                    1,
                    False,
                    {},
                    [],
                    False,
                ),
                FieldMeta(
                    "fld_start",
                    "维修开始时间",
                    "DateTime",
                    5,
                    False,
                    {},
                    [],
                    False,
                ),
            ]
            meta_by_name = {meta.field_name: meta for meta in metas}
            current_record = {
                "record_id": "rec_repair_summary_duplex",
                "raw_fields": {},
                "display_fields": {},
            }
            captured: list[dict] = []
            service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: current_record
            )
            service._load_repair_management_project_records = (  # type: ignore[method-assign]
                lambda **_kwargs: (metas, meta_by_name, [current_record])
            )
            service._patch_record_fields = (  # type: ignore[method-assign]
                lambda **kwargs: captured.append(kwargs) or {"code": 0}
            )
            service._upsert_repair_snapshot_fields = (  # type: ignore[method-assign]
                lambda **_kwargs: None
            )
            service._invalidate_repair_management_status_cache = (  # type: ignore[method-assign]
                lambda: None
            )

            result = service.sync_repair_management_notice_action(
                {
                    "work_type": WORK_TYPE_REPAIR,
                    "scope": "E",
                    "repair_management_record_id": "rec_repair_summary_duplex",
                    "title": "EA118机房E楼精密空调检修",
                    "response_time": "2026-07-18 09:15",
                },
                action="start",
                target_record_id="rec_repair_target_duplex",
            )

            self.assertTrue(result["synced"])
            self.assertTrue(result["link_deferred"])
            self.assertNotIn("设备检修关联", captured[-1]["fields"])
            self.assertEqual(
                captured[-1]["fields"]["检修通告名称"],
                "EA118机房E楼精密空调检修",
            )
            self.assertIn("维修开始时间", captured[-1]["fields"])

    def test_repair_management_prefill_form_carries_management_relation(self):
        html = workbench_lite_module.render_workbench_lite(
            payload={
                "records": [],
                "ongoing": [],
                "daily_summary": {"stats": {}},
                "record_type_counts": {},
                "ongoing_type_counts": {},
            },
            session={"user": {"name": "测试用户"}},
            scope="E",
            work_type=WORK_TYPE_REPAIR,
            prefill_draft={
                "work_type": WORK_TYPE_REPAIR,
                "title": "EA118机房E楼精密空调检修",
            },
            prefill_source_record_id="rec_repair_summary_form",
            prefill_context_id="rec_repair_summary_form",
            scope_options=[{"value": "E", "label": "E楼"}],
        )

        self.assertIn(
            'name="repair_management_record_id" value="rec_repair_summary_form"',
            html,
        )
        self.assertNotIn("related_repair_management_record_id", html)

    def test_repair_management_notice_prefill_without_notice_link_starts_new_notice(self):
        service = _TestMaintenancePortalService()
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {
                "record_id": "rec_summary_manual",
                "display_fields": {
                    "维修名称": "E楼精密空调维修单",
                    "检修通告名称": "EA118机房E楼精密空调检修",
                    "所属数据中心/楼栋-使用": "南通E楼",
                    "所属专业": "暖通",
                    "故障发生时间": "2026-07-15 09:00",
                    "维修结束时间": "2026-07-15 18:00",
                    "设备名称": "精密空调",
                    "故障发生现象描述": "高压告警",
                    "故障维修原因": "过滤器堵塞",
                    "维修方": "我方",
                    "后续整改措施": "清洗过滤器",
                    "维修进展描述": "人员已就位",
                },
                "raw_fields": {"设备检修关联": ""},
            }
        )

        payload = service.repair_management_notice_prefill(
            "rec_summary_manual",
            scope="E",
        )

        self.assertEqual(payload["target_record_id"], "")
        self.assertEqual(payload["action"], "start")
        self.assertEqual(payload["draft"]["title"], "EA118机房E楼精密空调检修")
        self.assertEqual(payload["draft"]["start_time"], "2026-07-15 18:00")
        self.assertEqual(payload["draft"]["end_time"], "2026-07-15 09:00")
        self.assertEqual(payload["draft"]["repair_device"], "精密空调")
        self.assertEqual(payload["draft"]["solution"], "清洗过滤器")
        self.assertEqual(payload["draft"]["progress"], "人员已就位")

    def test_repair_management_unlinked_notice_fields_are_writable_only_without_link(self):
        device = FieldMeta("fld_device", "设备名称", "Text", 1, False, {}, [], False)
        cycle = FieldMeta("fld_cycle", "维修周期", "Number", 2, False, {}, [], False)
        workers = FieldMeta(
            "fld_workers",
            "随工人员（或我方维修人员）",
            "User",
            11,
            False,
            {},
            [],
            False,
        )
        progress = FieldMeta(
            "fld_progress",
            "当前维修进度",
            "Progress",
            2,
            False,
            {},
            [],
            False,
        )
        formula = FieldMeta(
            "fld_formula",
            "检修通告名称",
            "Formula",
            20,
            False,
            {},
            [],
            True,
        )
        meta_by_name = {
            device.field_name: device,
            cycle.field_name: cycle,
            workers.field_name: workers,
            progress.field_name: progress,
            formula.field_name: formula,
        }

        linked = MaintenancePortalService._clean_repair_management_fields(
            {"设备名称": "精密空调", "当前维修进度": "50%"},
            meta_by_name,
            allow_empty=True,
        )
        unlinked = MaintenancePortalService._clean_repair_management_fields(
            {
                "设备名称": "精密空调",
                "维修周期": 2,
                "随工人员（或我方维修人员）": [{"id": "ou_worker"}],
                "当前维修进度": "50%",
                "检修通告名称": "公式字段不可写",
            },
            meta_by_name,
            allow_empty=True,
            allow_unlinked_repair_fields=True,
        )

        self.assertEqual(linked, {})
        self.assertEqual(
            unlinked,
            {
                "设备名称": "精密空调",
                "随工人员（或我方维修人员）": [{"id": "ou_worker"}],
            },
        )
        self.assertTrue(
            MaintenancePortalService._repair_management_field_payload(device)[
                "editable_without_repair_link"
            ]
        )
        self.assertFalse(
            MaintenancePortalService._repair_management_field_payload(formula)[
                "editable_without_repair_link"
            ]
        )
        self.assertFalse(
            MaintenancePortalService._repair_management_field_payload(cycle)[
                "editable_without_repair_link"
            ]
        )
        self.assertTrue(
            MaintenancePortalService._repair_management_field_payload(workers)[
                "editable_without_repair_link"
            ]
        )

    def test_repair_management_update_filters_readonly_fields(self):
        service = _TestMaintenancePortalService()
        editable = FieldMeta("fld_reason", "故障维修原因", "Text", 1, False, {}, [], False)
        retired = FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False)
        readonly = FieldMeta("fld_formula", "检修通告名称", "Formula", 20, False, {}, [], True)
        captured = {}

        def fake_load_fields(*, app_token, table_id):
            return [editable, retired, readonly], {
                editable.field_name: editable,
                retired.field_name: retired,
                readonly.field_name: readonly,
            }

        def fake_patch_record_fields(**kwargs):
            captured.update(kwargs)
            return {"code": 0}

        service._load_table_fields = fake_load_fields  # type: ignore[method-assign]
        service._patch_record_fields = fake_patch_record_fields  # type: ignore[method-assign]
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {"record_id": "rec_repair_1", "raw_fields": {}}
        )
        service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: ([], {}, [])
        )

        result = service.update_repair_management_record(
            "rec_repair_1",
            {
                "故障维修原因": "测试原因",
                "维修名称": "旧字段不可写",
                "检修通告名称": "公式字段不可写",
            },
        )

        self.assertEqual(captured["app_token"], REPAIR_SOURCE_APP_TOKEN)
        self.assertEqual(captured["table_id"], REPAIR_SOURCE_TABLE_ID)
        self.assertEqual(captured["record_id"], "rec_repair_1")
        self.assertEqual(captured["fields"], {"故障维修原因": "测试原因"})
        self.assertEqual(result["field_count"], 1)

    def test_repair_management_update_cannot_clear_required_field(self):
        service = _TestMaintenancePortalService()
        title = FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False)
        specialty = FieldMeta(
            "fld_specialty",
            "所属专业",
            "SingleSelect",
            3,
            False,
            {},
            ["电气", "暖通"],
            False,
        )
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [title, specialty],
                {title.field_name: title, specialty.field_name: specialty},
            )
        )
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {
                "record_id": "rec_repair_1",
                "display_fields": {
                    "维修名称": "A楼测试检修",
                    "所属专业": "电气",
                },
                "raw_fields": {},
            }
        )
        service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: ([], {}, [])
        )
        service._patch_record_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("required-field validation must run before patch")
        )

        with self.assertRaisesRegex(PortalError, "所属专业"):
            service.update_repair_management_record(
                "rec_repair_1",
                {"所属专业": ""},
                scope="A",
            )

    def test_repair_management_update_can_explicitly_clear_source_relations(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False),
            FieldMeta("fld_fault_time", "故障发生时间", "Text", 1, False, {}, [], False),
            FieldMeta("fld_reason", "故障维修原因", "Text", 1, False, {}, [], False),
            FieldMeta("fld_specialty", "所属专业", "Text", 1, False, {}, [], False),
            FieldMeta(
                "fld_building",
                "所属数据中心/楼栋-使用",
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
            FieldMeta("fld_event", "关联事件单", "DuplexLink", 21, False, {}, [], False),
            FieldMeta("fld_repair", "设备检修关联", "DuplexLink", 21, False, {}, [], False),
        ]
        meta_by_name = {meta.field_name: meta for meta in metas}
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: (metas, meta_by_name)
        )
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {
                "record_id": "rec_repair_1",
                "display_fields": {
                    "维修名称": "A楼测试检修",
                    "故障发生时间": "2026-07-01 10:00",
                    "故障维修原因": "测试原因",
                    "所属专业": "电气",
                    "所属数据中心/楼栋-使用": "南通A楼",
                },
                "raw_fields": {
                    "关联事件单": ["rec_event_1"],
                    "设备检修关联": ["rec_notice_1"],
                },
            }
        )
        service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: ([], {}, [])
        )
        service._repair_management_fields_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: True
        )
        captured = {}
        service._patch_record_fields = (  # type: ignore[method-assign]
            lambda **kwargs: captured.update(kwargs)
        )

        service.update_repair_management_record(
            "rec_repair_1",
            {"维修名称": "A楼测试检修"},
            source_event_id="",
            source_repair_ids=[],
            replace_source_relations=True,
            scope="A",
        )

        self.assertEqual(captured["fields"]["关联事件单"], [])
        self.assertEqual(captured["fields"]["设备检修关联"], [])

    def test_repair_management_update_rejects_cross_scope_record(self):
        service = _TestMaintenancePortalService()
        editable = FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False)
        building = FieldMeta(
            "fld_building",
            "所属数据中心/楼栋-使用",
            "SingleSelect",
            3,
            False,
            {},
            ["南通A楼", "南通E楼"],
            False,
        )

        def fake_load_fields(*, app_token, table_id):
            return [editable, building], {
                editable.field_name: editable,
                building.field_name: building,
            }

        def fake_load_records(**_kwargs):
            return [
                {
                    "record_id": "rec_repair_a",
                    "display_fields": {
                        "维修名称": "EA118机房A楼检修",
                        "所属数据中心/楼栋-使用": "南通A楼",
                    },
                    "raw_fields": {},
                }
            ]

        service._load_table_fields = fake_load_fields  # type: ignore[method-assign]
        service._load_table_records = fake_load_records  # type: ignore[method-assign]

        with self.assertRaisesRegex(PortalError, "无权操作"):
            service.update_repair_management_record(
                "rec_repair_a",
                {"维修名称": "跨楼修改"},
                scope="E",
            )

    def test_repair_management_create_filters_readonly_fields(self):
        service = _TestMaintenancePortalService()
        editable = FieldMeta("fld_reason", "故障维修原因", "Text", 1, False, {}, [], False)
        retired = FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False)
        readonly = FieldMeta("fld_formula", "检修通告名称", "Formula", 20, False, {}, [], True)
        captured = {}

        def fake_load_fields(*, app_token, table_id):
            return [editable, retired, readonly], {
                editable.field_name: editable,
                retired.field_name: retired,
                readonly.field_name: readonly,
            }

        def fake_create_record_fields(**kwargs):
            captured.update(kwargs)
            return {"data": {"record": {"record_id": "rec_repair_created"}}}

        service._load_table_fields = fake_load_fields  # type: ignore[method-assign]
        service._create_record_fields = fake_create_record_fields  # type: ignore[method-assign]

        result = service.create_repair_management_record(
            {
                "故障维修原因": "测试原因",
                "维修名称": "旧字段不可写",
                "检修通告名称": "公式字段不可写",
            }
        )

        self.assertEqual(captured["app_token"], REPAIR_SOURCE_APP_TOKEN)
        self.assertEqual(captured["table_id"], REPAIR_SOURCE_TABLE_ID)
        self.assertEqual(captured["fields"], {"故障维修原因": "测试原因"})
        self.assertEqual(result["record_id"], "rec_repair_created")
        self.assertEqual(result["field_count"], 1)

    def test_repair_management_create_requires_available_core_fields(self):
        service = _TestMaintenancePortalService()
        reason = FieldMeta("fld_reason", "故障维修原因", "Text", 1, False, {}, [], False)
        specialty = FieldMeta("fld_specialty", "所属专业", "SingleSelect", 3, False, {}, ["电气"], False)

        def fake_load_fields(*, app_token, table_id):
            return [reason, specialty], {
                reason.field_name: reason,
                specialty.field_name: specialty,
            }

        service._load_table_fields = fake_load_fields  # type: ignore[method-assign]

        with self.assertRaisesRegex(PortalError, "专业"):
            service.create_repair_management_record({"故障维修原因": "测试原因"})

    def test_repair_management_create_writes_latest_text_event_id_field(self):
        service = _TestMaintenancePortalService()
        retired = FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False)
        source_event = FieldMeta("fld_event", "关联事件单", "Text", 1, False, {}, [], False)
        captured = {}

        def fake_load_fields(*, app_token, table_id):
            return [retired, source_event], {
                retired.field_name: retired,
                source_event.field_name: source_event,
            }

        def fake_create_record_fields(**kwargs):
            captured.update(kwargs)
            return {"data": {"record": {"record_id": "rec_repair_created"}}}

        service._load_table_fields = fake_load_fields  # type: ignore[method-assign]
        service._create_record_fields = fake_create_record_fields  # type: ignore[method-assign]
        service._build_repair_management_prefill = (  # type: ignore[method-assign]
            lambda **_kwargs: {"fields": {}, "warnings": []}
        )

        service.create_repair_management_record(
            {"维修名称": "事件转检修"},
            source_event_id="rec_event_1",
        )

        self.assertEqual(
            captured["fields"],
            {"关联事件单": "rec_event_1"},
        )

    def test_repair_management_event_candidates_filter_scope(self):
        service = _TestMaintenancePortalService()
        records = [
            {
                "record_id": "rec_event_e",
                "display_fields": {
                    "事件简述": "E楼压缩机高压报警",
                    "机楼": "E楼",
                    "专业": "暖通",
                    "事件等级": "I3",
                    "事件发现来源（统一）": "BMS系统",
                    "事件状态": "处理中",
                    "事件发生时间": "2026-06-25 10:00",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_event_e_not_transferred",
                "display_fields": {
                    "事件简述": "E楼未转检修事件",
                    "机楼": "E楼",
                    "专业": "暖通",
                    "事件等级": "I3",
                    "事件发现来源（统一）": "BMS系统",
                    "事件状态": "处理中",
                    "是否转检修": "未转检修",
                    "事件发生时间": "2026-06-25 11:00",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_event_e_closed_transferred",
                "display_fields": {
                    "事件简述": "E楼已闭环并转检修事件",
                    "机楼": "E楼",
                    "事件状态": "事件闭环转检修完成",
                    "是否转检修": "是",
                    "事件发生时间": "2026-06-25 10:30",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_event_e_closed_without_transfer",
                "display_fields": {
                    "事件简述": "E楼已闭环未转检修事件",
                    "机楼": "E楼",
                    "事件状态": "事件闭环未转检修",
                    "是否转检修": "是",
                    "事件发生时间": "2026-06-25 10:20",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_event_a",
                "display_fields": {
                    "事件简述": "A楼网络报警",
                    "机楼": "A楼",
                    "专业": "弱电",
                    "事件等级": "I3",
                    "事件发现来源（统一）": "BMS系统",
                    "事件状态": "处理中",
                    "事件发生时间": "2026-06-25 09:00",
                },
                "raw_fields": {},
            },
        ]
        service._load_repair_management_event_records = (  # type: ignore[method-assign]
            lambda: ([], {}, records)
        )
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda: (
                [],
                {},
                [
                    {
                        "record_id": "rec_project_e",
                        "display_fields": {
                            "维修名称": "E楼压缩机维修单",
                            "关联事件单": "rec_event_e",
                        },
                        "raw_fields": {"关联事件单": "rec_event_e"},
                    }
                ],
            )
        )

        payload = service.list_repair_management_event_candidates(scope="E", month="2026-06")

        self.assertEqual([item["record_id"] for item in payload["records"]], ["rec_event_e"])
        self.assertFalse(payload["records"][0]["selectable"])
        self.assertIn("已创建维修单", payload["records"][0]["selection_status"])

    def test_repair_management_event_prefill_maps_event_fields_and_link(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False),
            FieldMeta("fld_event", "关联事件单", "Text", 1, False, {}, [], False),
            FieldMeta("fld_source", "对应来源", "MultiSelect", 4, False, {}, ["BMS系统"], False),
            FieldMeta("fld_level", "对应事件等级", "SingleSelect", 3, False, {}, ["I3"], False),
            FieldMeta("fld_fault_time", "故障发生时间", "DateTime", 5, False, {}, [], False),
            FieldMeta("fld_reason", "故障维修原因", "Text", 1, False, {}, [], False),
            FieldMeta("fld_symptom", "故障发生现象描述", "Text", 1, False, {}, [], False),
            FieldMeta("fld_specialty", "所属专业", "SingleSelect", 3, False, {}, ["暖通"], False),
            FieldMeta("fld_push_specialty", "专业（推送消息用）", "SingleSelect", 3, False, {}, ["暖通"], False),
            FieldMeta("fld_event_desc", "事件描述", "Text", 1, False, {}, [], False),
            FieldMeta("fld_building_text", "所属数据中心/楼栋-使用", "Text", 1, False, {}, [], False),
            FieldMeta("fld_building", "所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）", "SingleSelect", 3, False, {}, ["南通E楼"], False),
        ]

        def fake_load_fields(*, app_token, table_id):
            return metas, {meta.field_name: meta for meta in metas}

        event = {
            "source_record_id": "rec_event_e",
            "record_id": "rec_event_e",
            "title": "BMS报E-217-CRAC-02压缩机高压报警: 告警",
            "alarm_desc": "BMS报E-217-CRAC-02压缩机高压报警: 告警",
            "building": "E楼",
            "building_codes": ["E"],
            "specialty": "暖通",
            "level": "I3",
            "source": "BMS系统",
            "status": "处理中",
            "occurrence_time": "2026-06-25 10:00",
            "display_fields": {
                "事件简述": "E楼压缩机高压报警",
                "告警描述": "BMS报E-217-CRAC-02压缩机高压报警: 告警",
                "故障现象": "压缩机高压保护触发",
                "事件等级": "I3",
                "事件发现来源（统一）": "BMS系统",
                "事件发生时间": "2026-06-25 10:00",
                "机楼": "E楼",
                "专业": "暖通",
            },
            "raw_fields": {},
        }

        service._load_table_fields = fake_load_fields  # type: ignore[method-assign]
        service._event_snapshot_record_for_repair = (  # type: ignore[method-assign]
            lambda **_kwargs: event
        )

        payload = service.repair_management_event_prefill(
            scope="E",
            record_id="rec_event_e",
            month="2026-06",
        )

        fields = payload["fields"]
        self.assertNotIn("维修名称", fields)
        self.assertEqual(fields["关联事件单"], "rec_event_e")
        self.assertEqual(fields["对应来源"], ["BMS系统"])
        self.assertEqual(fields["对应事件等级"], "I3")
        self.assertIsInstance(fields["故障发生时间"], int)
        self.assertEqual(fields["故障维修原因"], "BMS报E-217-CRAC-02压缩机高压报警: 告警")
        self.assertEqual(
            fields["故障发生现象描述"],
            "BMS报E-217-CRAC-02压缩机高压报警: 告警",
        )
        self.assertEqual(fields["所属专业"], "暖通")
        self.assertEqual(fields["所属数据中心/楼栋-使用"], "南通E楼")

    def test_repair_management_event_item_uses_alarm_description_for_fault_phenomenon(self):
        item = MaintenancePortalService._repair_management_event_item(
            {
                "record_id": "rec_event_symptom",
                "display_fields": {
                    "事件简述": "压缩机高压报警",
                    "告警描述": "BMS告警文本",
                    "故障现象": "压缩机高压保护触发",
                    "机楼": "E楼",
                },
                "raw_fields": {},
            }
        )

        self.assertEqual(item["fault_phenomenon"], "BMS告警文本")

    def test_repair_management_repair_candidates_rank_matching_record(self):
        service = _TestMaintenancePortalService()
        event = {
            "source_record_id": "rec_event_e",
            "title": "BMS报E-217-CRAC-02压缩机高压报警",
            "alarm_desc": "BMS报E-217-CRAC-02压缩机高压报警",
            "building": "E楼",
            "building_codes": ["E"],
            "specialty": "暖通",
            "occurrence_time": "2026-06-25 10:00",
        }
        records = [
            {
                "record_id": "rec_repair_e",
                "display_fields": {
                    "名称（标题）": "EA118机房E楼E-217-CRAC-02压缩机高压报警检修通告",
                    "楼栋": "E楼",
                    "专业": "暖通",
                    "发生故障时间": "2026-06-25 10:00",
                    "维修设备": "E-217-CRAC-02",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_repair_a",
                "display_fields": {
                    "名称（标题）": "A楼网络设备通信中断检修",
                    "楼栋": "A楼",
                    "专业": "弱电",
                    "发生故障时间": "2026-06-25 10:00",
                },
                "raw_fields": {},
            },
        ]
        service._event_snapshot_record_for_repair = (  # type: ignore[method-assign]
            lambda **_kwargs: event
        )
        service._load_repair_management_target_records = (  # type: ignore[method-assign]
            lambda: ([], {}, records)
        )

        payload = service.list_repair_management_repair_candidates(
            scope="E",
            event_record_id="rec_event_e",
            month="2026-06",
        )

        self.assertEqual([item["record_id"] for item in payload["records"]], ["rec_repair_e"])
        self.assertGreaterEqual(payload["records"][0]["score"], 90)
        self.assertEqual(payload["auto_selected_ids"], ["rec_repair_e"])

    def test_repair_management_repair_candidates_require_event_selection(self):
        service = _TestMaintenancePortalService()
        service._load_repair_management_target_records = (  # type: ignore[method-assign]
            lambda: self.fail("repair candidates must not load without an event")
        )

        with self.assertRaisesRegex(PortalError, "请先选择关联事件单"):
            service.list_repair_management_repair_candidates(scope="E")

    def test_repair_management_candidates_keep_working_when_event_record_is_stale(self):
        service = _TestMaintenancePortalService()

        def missing_event(**_kwargs):
            raise PortalError(
                "飞书接口失败: code=1254043, msg=RecordIdNotFound"
            )

        service._event_snapshot_record_for_repair = missing_event  # type: ignore[method-assign]
        service._load_repair_management_target_records = (  # type: ignore[method-assign]
            lambda: (
                [],
                {},
                [
                    {
                        "record_id": "rec_repair_e",
                        "display_fields": {
                            "名称（标题）": "E楼测试检修",
                            "楼栋": "E楼",
                            "专业": "暖通",
                        },
                        "raw_fields": {},
                    }
                ],
            )
        )

        payload = service.list_repair_management_repair_candidates(
            scope="E",
            event_record_id="rec_stale_event",
            month="2026-07",
        )

        self.assertEqual(payload["records"][0]["record_id"], "rec_repair_e")
        self.assertTrue(payload["event_context_missing"])
        self.assertIn("仍可手动选择", payload["warnings"][0])

    def test_repair_management_event_resolver_uses_saved_fields_for_stale_relation(self):
        service = _TestMaintenancePortalService()
        service._load_repair_management_event_records = (  # type: ignore[method-assign]
            lambda: ([], {}, [])
        )
        service._request_json = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: self.fail(
                "stale relation must not query the current event table"
            )
        )
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda: (
                [],
                {},
                [
                    {
                        "record_id": "rec_project",
                        "display_fields": {
                            "维修名称": "E楼压缩机高压报警",
                            "故障维修原因": "压缩机高压报警",
                            "故障发生现象描述": "压缩机保护",
                            "故障发生时间": "2026-07-16 09:00",
                            "所属专业": "暖通",
                            "所属数据中心/楼栋-使用": "南通E楼",
                        },
                        "raw_fields": {
                            "关联事件单": [
                                {
                                    "record_ids": ["rec_stale_event"],
                                    "table_id": "tbl_retired_event",
                                }
                            ]
                        },
                    }
                ],
            )
        )

        event = service._event_snapshot_record_for_repair(
            scope="E",
            record_id="rec_stale_event",
            month="2026-07",
        )

        self.assertEqual(event["record_id"], "rec_stale_event")
        self.assertEqual(event["building_codes"], ["E"])
        self.assertEqual(event["specialty"], "暖通")
        self.assertIn("当前事件表", event["resolution_warning"])

    def test_event_notice_repair_project_helper_is_idempotent(self):
        service = _TestMaintenancePortalService()
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda: ([], {}, [])
        )
        source_event = {
            "record_id": "rec_event_end",
            "title": "E楼压缩机高压报警",
            "building_codes": ["E"],
        }
        service._repair_management_event_from_notice_payload = (  # type: ignore[method-assign]
            lambda **_kwargs: source_event
        )
        with patch.object(
            service,
            "create_repair_management_record",
            return_value={"record_id": "rec_project_created", "warnings": []},
        ) as create_record:
            result = service.ensure_repair_management_record_for_event_notice(
                event_record_id="rec_event_end",
                notice_data={"scope": "E"},
                remote_fields={"告警描述": "E楼压缩机高压报警"},
                scope="E",
                source_month="2026-07",
            )

        self.assertTrue(result["created"])
        self.assertEqual(result["record_id"], "rec_project_created")
        self.assertEqual(
            create_record.call_args.kwargs["operation_id"],
            f"event-end-transfer:{REPAIR_MANAGEMENT_TABLE_ID}:rec_event_end",
        )
        self.assertEqual(create_record.call_args.kwargs["source_event_record"], source_event)
        self.assertFalse(create_record.call_args.kwargs["sync_event_transfer_status"])

        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda: (
                [],
                {},
                [
                    {
                        "record_id": "rec_project_created",
                        "raw_fields": {"关联事件单": "rec_event_end"},
                        "display_fields": {},
                    }
                ],
            )
        )
        with patch.object(service, "create_repair_management_record") as duplicate_create:
            replay = service.ensure_repair_management_record_for_event_notice(
                event_record_id="rec_event_end",
                notice_data={},
                remote_fields={},
                scope="E",
                source_month="2026-07",
            )
        self.assertFalse(replay["created"])
        self.assertEqual(replay["record_id"], "rec_project_created")
        duplicate_create.assert_not_called()

    def test_event_notice_repair_prefill_uses_remote_alarm_description_as_phenomenon(self):
        event = MaintenancePortalService._repair_management_event_from_notice_payload(
            record_id="rec_event_alarm",
            notice_data={
                "title": "事件标题",
                "symptom": "旧故障现象不应写入维修单",
            },
            remote_fields={
                "告警描述": "BMS报A-219-CRAH-01压差偏大",
                "机楼": "A楼",
            },
            scope="A",
        )

        self.assertEqual(event["alarm_desc"], "BMS报A-219-CRAH-01压差偏大")
        self.assertEqual(
            event["fault_phenomenon"],
            "BMS报A-219-CRAH-01压差偏大",
        )
        self.assertEqual(
            event["display_fields"]["故障现象"],
            "BMS报A-219-CRAH-01压差偏大",
        )

    def test_repair_management_uses_current_event_and_repair_target_tables(self):
        service = _TestMaintenancePortalService()
        field = FieldMeta("fld_title", "名称（标题）", "Text", 1, False, {}, [], False)
        field_calls: list[tuple[str, str]] = []
        record_calls: list[tuple[str, str]] = []

        def fake_load_fields(*, app_token, table_id):
            field_calls.append((app_token, table_id))
            return [field], {field.field_name: field}

        def fake_search_records(**kwargs):
            record_calls.append((kwargs["app_token"], kwargs["table_id"]))
            return []

        service._load_table_fields = fake_load_fields  # type: ignore[method-assign]
        service._search_table_records = fake_search_records  # type: ignore[method-assign]
        with (
            patch.object(config_module.config, "app_token", "app_current"),
            patch.object(config_module.config, "table_id_shijian", "tblj9XJLq5QzTAqX"),
            patch.object(config_module.config, "table_id_overhaul", REPAIR_MANAGEMENT_REPAIR_TABLE_ID),
        ):
            service._load_repair_management_event_records(force_refresh=True)
            service._load_repair_management_target_records(force_refresh=True)

        self.assertIn(("app_current", "tblj9XJLq5QzTAqX"), field_calls)
        self.assertIn(
            ("app_current", REPAIR_MANAGEMENT_REPAIR_TABLE_ID),
            field_calls,
        )
        self.assertIn(("app_current", "tblj9XJLq5QzTAqX"), record_calls)
        self.assertIn(
            ("app_current", REPAIR_MANAGEMENT_REPAIR_TABLE_ID),
            record_calls,
        )

    def test_repair_management_combined_prefill_aggregates_event_and_repairs(self):
        service = _TestMaintenancePortalService()

        def meta(name, ui_type="Text", field_type=1, options=None):
            return FieldMeta(
                f"fld_{len(name)}_{name}",
                name,
                ui_type,
                field_type,
                False,
                {},
                list(options or []),
                False,
            )

        metas = [
            meta("维修名称"),
            meta("关联事件单"),
            meta("设备检修关联"),
            meta("维修跟进记录"),
            meta("对应来源", "MultiSelect", 4, ["BMS系统", "方舟系统"]),
            meta("对应事件等级", "SingleSelect", 3, ["I3"]),
            meta("故障发生时间", "DateTime", 5),
            meta("故障维修原因"),
            meta("故障发生现象描述"),
            meta("所属专业", "SingleSelect", 3, ["暖通"]),
            meta("专业（推送消息用）", "SingleSelect", 3, ["暖通"]),
            meta("事件描述"),
            meta("所属数据中心/楼栋-使用"),
            meta("所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）", "SingleSelect", 3, ["南通E楼"]),
            meta("区域"),
            meta("数据中心"),
            meta("维修开始时间", "DateTime", 5),
            meta("开始时间", "DateTime", 5),
            meta("维修结束时间（2026）", "DateTime", 5),
            meta("维修结束时间", "DateTime", 5),
            meta("当前维修进度", "Number", 2),
            meta("维修周期", "Number", 2),
            meta("检修通告名称"),
            meta("设备名称"),
            meta("维修进展描述"),
            meta("最新维修跟进时间", "DateTime", 5),
            meta("设备编号"),
            meta("设备品牌"),
            meta("设备型号"),
            meta("设备容量KW/AH"),
            meta("设备生产日期", "DateTime", 5),
            meta("设备使用年限", "Number", 2),
            meta("是否质保期内", "SingleSelect", 3, ["是", "否"]),
            meta("供应商名称"),
            meta("供应商维修人员"),
            meta("更换备件名称"),
            meta("更换备件数量", "Number", 2),
            meta("故障维修总费用（跟进完成的维修项）", "Number", 2),
            meta("跟进项"),
            meta("后续整改措施"),
            meta("子项链接", "Url", 15),
            meta("CMDB唯一id"),
            meta("是否有唯一id"),
            meta("智航设备名称"),
            meta("维修方案附件", "Number", 2),
            meta("维修审批人", "User", 11),
            meta("推送群组"),
            meta("当前日期"),
            meta("维修方", "SingleSelect", 3, ["我方", "供应商"]),
            meta("值班账号", "User", 11),
            meta("随工人员（或我方维修人员）", "User", 11),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        event = {
            "source_record_id": "rec_event_e",
            "record_id": "rec_event_e",
            "title": "E楼压缩机高压报警",
            "alarm_desc": "旧告警描述不可回填",
            "building": "E楼",
            "building_codes": ["E"],
            "specialty": "暖通",
            "level": "I3",
            "source": "方舟系统",
            "occurrence_time": "2026-06-25 10:00",
            "fault_reason": "旧故障原因不可回填",
            "display_fields": {
                "事件简述": "E楼压缩机高压报警",
                "告警描述": "BMS报E-217-CRAC-02压缩机高压报警",
                "故障现象": "压缩机高压报警",
                "事件等级": "I3",
                "事件发现来源": "方舟系统",
                "事件发现来源（统一）": "BMS系统",
                "事件发生原因": "旧事件原因不可回填",
                "事件发生时间": "2026-06-25 10:00",
                "机楼": "E楼",
                "专业": "暖通",
            },
            "raw_fields": {
                "值班账号": {
                    "users": [{"id": "ou_duty", "name": "E楼值班"}]
                }
            },
        }
        repair = {
            "record_id": "rec_repair_e",
            "raw_fields": {
                "实际开始时间": 1782363600000,
                "实际结束时间": 1782370800000,
                "涉及值班账号": [
                    {"id": "ou_engineer", "name": "检修工程师"}
                ],
            },
            "display_fields": {
                "名称（标题）": "E楼压缩机高压报警检修",
                "楼栋": "E楼",
                "专业": "暖通",
                "检修状态": "结束",
                "维修设备": "E-217-CRAC-02",
                "故障现象": "压缩机高压报警",
                "故障原因": "高压保护触发",
                "维修方式": "自维",
                "进度（完成情况）": "处理完成",
            },
        }
        followup = {
            "record_id": "rec_followup_e",
            "raw_fields": {
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary_e",
                "随工人员（我方维修人员）": [
                    {"id": "ou_followup", "name": "跟进工程师"}
                ],
                "维修审批人": {
                    "users": [{"id": "ou_approver", "name": "审批人"}]
                },
                "推送群组": {"groups": [{"id": "oc_group", "name": "E楼检修群"}]},
                "CMDB设备唯一ID": ["rec_cmdb_e"],
            },
            "display_fields": {
                "维修名称": "E楼压缩机高压报警维修单",
                "所属数据中心": "南通E楼",
                "所属专业": "暖通",
                "维修开始时间": "2026-06-25 10:10",
                "维修结束时间": "2026-06-25 12:30",
                "创建时间": "2026-06-25 12:35",
                "维修进度": "1",
                "维修进展描述": "已更换压力传感器并恢复",
                "设备名称": "E-217-CRAC-02",
                "设备编号": "CRAC-02",
                "设备品牌": "测试品牌",
                "设备型号": "TEST-02",
                "设备生产日期": "2020-06-25 00:00",
                "设备使用年限": "6",
                "设备容量KW/AH": "120KW",
                "是否质保期内": "否",
                "维修方": "供应商",
                "供应商名称": "测试供应商",
                "供应商维修人员": "供应商工程师",
                "更换备件名称": "压力传感器",
                "更换备件数量": "1",
                "故障维修总费用": "350",
                "跟进项（如有）": "观察运行状态",
                "后续整改措施（如有）": "下月复测",
                "超链接": "https://example.test/followup",
            },
        }
        cmdb = {
            "record_id": "rec_cmdb_e",
            "raw_fields": {},
            "display_fields": {
                "智航唯一ID": "ZH-E-217-02",
                "设备名称": "E-217-CRAC-02",
                "分类名称": "精密空调",
                "楼栋": "E楼",
            },
        }
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: (metas, meta_by_name)
        )
        service._event_snapshot_record_for_repair = (  # type: ignore[method-assign]
            lambda **_kwargs: event
        )
        service._load_repair_management_target_records = (  # type: ignore[method-assign]
            lambda: ([], {}, [repair])
        )
        followup_parent = FieldMeta(
            "fld_parent",
            REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
            "Text",
            1,
            False,
            {},
            [],
            False,
        )
        service._ensure_repair_followup_parent_id_field = (  # type: ignore[method-assign]
            lambda: ([followup_parent], {followup_parent.field_name: followup_parent})
        )
        service._load_repair_management_cmdb_records = (  # type: ignore[method-assign]
            lambda: ([], {}, [cmdb])
        )
        service._load_table_records_by_ids = (  # type: ignore[method-assign]
            lambda **kwargs: [
                followup
                if kwargs.get("table_id") == REPAIR_FOLLOWUP_TABLE_ID
                else cmdb
                if kwargs.get("table_id") == REPAIR_CMDB_TABLE_ID
                else repair
            ]
        )
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {"record_id": "rec_summary_e", "raw_fields": {}}
        )

        payload = service._build_repair_management_prefill(
            scope="E",
            event_record_id="rec_event_e",
            repair_record_ids=["rec_repair_e"],
            followup_record_ids=["rec_followup_e"],
            month="2026-06",
            meta_by_name=meta_by_name,
            allow_multiple_followups=True,
        )

        fields = payload["fields"]
        self.assertEqual(fields["关联事件单"], "rec_event_e")
        self.assertIn("rec_repair_e", fields["设备检修关联"])
        self.assertFalse(REPAIR_MANAGEMENT_RETIRED_FIELD_NAMES & set(fields))
        self.assertEqual(fields["对应来源"], ["BMS系统"])
        self.assertEqual(
            fields["故障维修原因"],
            "BMS报E-217-CRAC-02压缩机高压报警",
        )
        self.assertEqual(
            fields["故障发生现象描述"],
            "BMS报E-217-CRAC-02压缩机高压报警",
        )
        self.assertEqual(fields["所属数据中心/楼栋-使用"], "南通E楼")
        self.assertEqual(fields["所属专业"], "暖通")
        self.assertEqual(fields["维修开始时间"], 1782363600000)
        self.assertEqual(fields["维修结束时间（2026）"], 1782370800000)
        self.assertEqual(fields["当前维修进度"], 1)
        self.assertEqual(fields["设备名称"], "E-217-CRAC-02")
        self.assertEqual(fields["设备编号"], "CRAC-02")
        self.assertEqual(fields["更换备件名称"], "压力传感器")
        self.assertEqual(fields["更换备件数量"], 1)
        self.assertEqual(fields["故障维修总费用（跟进完成的维修项）"], 350)
        self.assertEqual(fields["跟进项"], "观察运行状态")
        self.assertEqual(fields["维修方"], "供应商")
        self.assertNotIn("维修方案附件", fields)
        self.assertEqual(
            fields["随工人员（或我方维修人员）"],
            [{"id": "ou_followup"}],
        )

    def test_repair_management_event_fields_do_not_fall_back_to_legacy_values(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta(
                "fld_source",
                "对应来源",
                "MultiSelect",
                4,
                False,
                {},
                ["BMS系统"],
                False,
            ),
            FieldMeta(
                "fld_reason",
                "故障维修原因",
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
            FieldMeta(
                "fld_building",
                "所属数据中心/楼栋-使用",
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        service._event_snapshot_record_for_repair = (  # type: ignore[method-assign]
            lambda **_kwargs: {
                "record_id": "rec_event_legacy",
                "source": "BMS系统",
                "alarm_desc": "旧告警描述",
                "fault_reason": "旧故障原因",
                "building": "A楼",
                "building_codes": ["A"],
                "display_fields": {
                    "事件发现来源": "BMS系统",
                    "事件发生原因": "旧事件原因",
                    "南通楼栋": "A楼",
                },
            }
        )

        payload = service._build_repair_management_prefill(
            scope="ALL",
            event_record_id="rec_event_legacy",
            repair_record_ids=[],
            followup_record_ids=[],
            month="2026-07",
            meta_by_name=meta_by_name,
        )

        self.assertIsNone(payload["fields"]["对应来源"])
        self.assertIsNone(payload["fields"]["故障维修原因"])
        self.assertIsNone(payload["fields"]["所属数据中心/楼栋-使用"])
        self.assertIn(
            "关联事件缺少“事件发现来源（统一）”，对应来源未回填。",
            payload["warnings"],
        )
        self.assertIn(
            "关联事件缺少“告警描述”，故障维修原因未回填。",
            payload["warnings"],
        )

    def test_repair_management_historical_event_uses_saved_project_fields_without_warning(self):
        service = _TestMaintenancePortalService()
        project = {
            "record_id": "rec_project_history",
            "raw_fields": {
                "关联事件单": "rec_event_history",
                "故障发生时间": 1783123200000,
            },
            "display_fields": {
                "关联事件单": "rec_event_history",
                "对应来源": ["BMS系统"],
                "对应事件等级": "I3",
                "故障维修原因": "历史事件告警描述",
                "故障发生现象描述": "历史事件故障现象",
                "故障发生时间": "2026-07-04 08:00",
                "所属专业": "暖通",
                "所属数据中心/楼栋-使用": "南通A楼",
            },
        }
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda: ([], {}, [project])
        )
        service._repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: True
        )

        event = service._repair_management_event_from_project_snapshot(
            scope="ALL",
            record_id="rec_event_history",
        )
        self.assertTrue(event["historical_fallback"])
        self.assertEqual(
            event["display_fields"]["事件发现来源（统一）"],
            "BMS系统",
        )
        self.assertEqual(
            event["display_fields"]["告警描述"],
            "历史事件告警描述",
        )
        self.assertEqual(event["display_fields"]["机楼"], "A楼")

        metas = [
            FieldMeta(
                "fld_source",
                "对应来源",
                "MultiSelect",
                4,
                False,
                {},
                ["BMS系统"],
                False,
            ),
            FieldMeta(
                "fld_reason",
                "故障维修原因",
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
            FieldMeta(
                "fld_building",
                "所属数据中心/楼栋-使用",
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        service._event_snapshot_record_for_repair = (  # type: ignore[method-assign]
            lambda **_kwargs: event
        )

        payload = service._build_repair_management_prefill(
            scope="ALL",
            event_record_id="rec_event_history",
            repair_record_ids=[],
            followup_record_ids=[],
            month="2026-07",
            meta_by_name=meta_by_name,
        )

        self.assertEqual(payload["fields"]["对应来源"], ["BMS系统"])
        self.assertEqual(
            payload["fields"]["故障维修原因"],
            "历史事件告警描述",
        )
        self.assertEqual(
            payload["fields"]["所属数据中心/楼栋-使用"],
            "南通A楼",
        )
        warning_text = "；".join(payload["warnings"])
        self.assertNotIn("来源事件属于历史事件表", warning_text)
        self.assertNotIn("事件发现来源（统一）", warning_text)
        self.assertNotIn("缺少“告警描述”", warning_text)

    def test_repair_management_building_uses_repair_fallback_and_rejects_conflict(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta(
                "fld_building",
                "所属数据中心/楼栋-使用",
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
            FieldMeta(
                "fld_repair",
                "设备检修关联",
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        event_building = {"value": ""}
        repair_building = {"value": "A楼"}
        service._event_snapshot_record_for_repair = (  # type: ignore[method-assign]
            lambda **_kwargs: {
                "record_id": "rec_event_building",
                "display_fields": {
                    "告警描述": "测试告警",
                    "事件发现来源（统一）": "BMS系统",
                    "机楼": event_building["value"],
                },
            }
        )
        service._load_repair_management_target_records = (  # type: ignore[method-assign]
            lambda: ([], {}, [])
        )
        service._load_table_records_by_ids = (  # type: ignore[method-assign]
            lambda **_kwargs: [
                {
                    "record_id": "rec_repair_building",
                    "raw_fields": {},
                    "display_fields": {
                        "楼栋": repair_building["value"],
                        "名称（标题）": "测试检修",
                    },
                }
            ]
        )

        payload = service._build_repair_management_prefill(
            scope="ALL",
            event_record_id="rec_event_building",
            repair_record_ids=["rec_repair_building"],
            followup_record_ids=[],
            month="2026-07",
            meta_by_name=meta_by_name,
        )
        self.assertEqual(
            payload["fields"]["所属数据中心/楼栋-使用"],
            "南通A楼",
        )

        event_building["value"] = "A楼"
        repair_building["value"] = "B楼"
        with self.assertRaisesRegex(PortalError, "关联事件机楼.*检修通告楼栋.*不一致"):
            service._build_repair_management_prefill(
                scope="ALL",
                event_record_id="rec_event_building",
                repair_record_ids=["rec_repair_building"],
                followup_record_ids=[],
                month="2026-07",
                meta_by_name=meta_by_name,
            )

    def test_repair_management_source_controlled_fields_override_client_patch(self):
        meta_by_name = {
            name: FieldMeta(
                f"fld_{index}",
                name,
                "Text",
                1,
                False,
                {},
                [],
                False,
            )
            for index, name in enumerate(
                (
                    "对应来源",
                    "故障发生时间",
                    "故障维修原因",
                    "所属数据中心/楼栋-使用",
                    "维修开始时间",
                    "维修结束时间（2026）",
                )
            )
        }
        auto_fields = {
            "对应来源": ["BMS系统"],
            "故障发生时间": 500,
            "故障维修原因": "事件告警描述",
            "所属数据中心/楼栋-使用": "南通A楼",
            "维修开始时间": 1000,
            "维修结束时间（2026）": 2000,
        }
        result = MaintenancePortalService._apply_repair_management_source_controlled_fields(
            {
                "对应来源": ["旧来源"],
                "故障发生时间": 999,
                "故障维修原因": "用户旧值",
                "所属数据中心/楼栋-使用": "南通B楼",
                "维修开始时间": 3000,
                "维修结束时间（2026）": 4000,
            },
            auto_fields,
            meta_by_name,
            source_event_id="rec_event",
            source_repair_ids=["rec_repair"],
        )

        self.assertEqual(result, auto_fields)

    def test_repair_followup_times_copy_only_from_summary(self):
        start_meta = FieldMeta(
            "fld_start",
            "维修开始时间",
            "DateTime",
            5,
            False,
            {},
            [],
            False,
        )
        end_meta = FieldMeta(
            "fld_end",
            "维修结束时间",
            "DateTime",
            5,
            False,
            {},
            [],
            False,
        )
        copied = MaintenancePortalService._repair_followup_summary_copy_fields(
            {
                "raw_fields": {
                    "维修开始时间": 1782363600000,
                    "维修结束时间（2026）": 1782370800000,
                },
                "display_fields": {},
            },
            {
                start_meta.field_name: start_meta,
                end_meta.field_name: end_meta,
            },
        )

        self.assertEqual(copied["维修开始时间"], 1782363600000)
        self.assertEqual(copied["维修结束时间"], 1782370800000)

        cleared = MaintenancePortalService._repair_followup_summary_copy_fields(
            {"raw_fields": {}, "display_fields": {}},
            {
                start_meta.field_name: start_meta,
                end_meta.field_name: end_meta,
            },
        )
        self.assertIsNone(cleared["维修开始时间"])
        self.assertIsNone(cleared["维修结束时间"])

    def test_repair_target_record_lookup_reuses_loaded_snapshot(self):
        service = _TestMaintenancePortalService()
        title_meta = FieldMeta(
            "fld_title",
            "名称（标题）",
            "Text",
            1,
            False,
            {},
            [],
            False,
        )
        cached_record = {
            "record_id": "rec_repair_cached",
            "display_fields": {"名称（标题）": "缓存检修通告"},
            "raw_fields": {},
        }
        service._load_repair_management_target_records = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [title_meta],
                {title_meta.field_name: title_meta},
                [cached_record],
            )
        )
        service._load_table_records_by_ids = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("cached record must not be fetched again")
        )

        _metas, _meta_by_name, records = (
            service._load_repair_management_target_records_by_ids(
                ["rec_repair_cached"]
            )
        )

        self.assertEqual(records, [cached_record])

    def test_repair_target_completed_status_is_explicit(self):
        service = _TestMaintenancePortalService()

        for status in ("结束", "已结束", "正常结束", "维修完成"):
            with self.subTest(status=status):
                self.assertTrue(
                    service._repair_management_target_record_is_completed(
                        {"display_fields": {"检修状态": status}}
                    )
                )
        for status in ("", "开始", "更新", "进行中", "未结束"):
            with self.subTest(status=status):
                self.assertFalse(
                    service._repair_management_target_record_is_completed(
                        {"display_fields": {"检修状态": status}}
                    )
                )

    def test_completed_repair_notice_syncs_followup_end_fields_and_progress(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta(
                "fld_parent",
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
            FieldMeta("fld_device", "设备名称", "Text", 1, False, {}, [], False),
            FieldMeta("fld_start", "维修开始时间", "DateTime", 5, False, {}, [], False),
            FieldMeta("fld_end", "维修结束时间", "DateTime", 5, False, {}, [], False),
            FieldMeta("fld_end_alias", "结束时间", "DateTime", 5, False, {}, [], False),
            FieldMeta("fld_progress", "维修进度", "Number", 2, False, {}, [], False),
            FieldMeta("fld_detail", "维修进展描述", "Text", 1, False, {}, [], False),
        ]
        meta_by_name = {meta.field_name: meta for meta in metas}
        followups = [
            {
                "record_id": "rec_followup_1",
                "raw_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary",
                },
                "display_fields": {"维修进展描述": "保留第一条进展"},
            },
            {
                "record_id": "rec_followup_2",
                "raw_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary",
                },
                "display_fields": {"维修进展描述": "保留第二条进展"},
            },
        ]
        patched: list[dict] = []
        snapshots: list[dict] = []
        service._ensure_repair_followup_parent_id_field = (  # type: ignore[method-assign]
            lambda: (metas, meta_by_name)
        )
        service._ensure_repair_followup_select_options = (  # type: ignore[method-assign]
            lambda _fields, source_meta_by_name, **_kwargs: (
                list(source_meta_by_name.values()),
                source_meta_by_name,
            )
        )
        service._patch_record_fields = (  # type: ignore[method-assign]
            lambda **kwargs: patched.append(kwargs) or {"code": 0}
        )
        service._upsert_repair_snapshot_fields = (  # type: ignore[method-assign]
            lambda **kwargs: snapshots.append(kwargs)
        )

        result = service._sync_repair_followups_from_summary(
            summary_record_id="rec_summary",
            summary_record={
                "raw_fields": {
                    "维修开始时间": 1784707200000,
                    "维修结束时间（2026）": 1784714400000,
                },
                "display_fields": {"设备名称": "A-101-UPS"},
            },
            completed=True,
            linked_followups=followups,
        )

        self.assertEqual(result["synced_count"], 2)
        self.assertEqual(len(patched), 2)
        for update in patched:
            fields = update["fields"]
            self.assertEqual(fields["维修结束时间"], 1784714400000)
            self.assertEqual(fields["结束时间"], 1784714400000)
            self.assertEqual(fields["维修进度"], 1)
            self.assertNotIn("设备名称", fields)
            self.assertNotIn("维修进展描述", fields)
        self.assertEqual(len(snapshots), 2)

    def test_repair_project_save_marks_completed_notice_and_syncs_followups(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False),
            FieldMeta("fld_link", "设备检修关联", "Text", 1, False, {}, [], False),
            FieldMeta(
                "fld_end",
                "维修结束时间（2026）",
                "DateTime",
                5,
                False,
                {},
                [],
                False,
            ),
            FieldMeta(
                "fld_workflow",
                "流程",
                "SingleSelect",
                3,
                False,
                {},
                ["未开始", "维修中", "维修完成"],
                False,
            ),
        ]
        meta_by_name = {meta.field_name: meta for meta in metas}
        existing = {
            "record_id": "rec_summary",
            "raw_fields": {"设备检修关联": "rec_repair"},
            "display_fields": {"维修名称": "A楼测试维修单"},
        }
        linked_followup = {
            "record_id": "rec_followup",
            "raw_fields": {
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary",
            },
            "display_fields": {"维修进度": "50%"},
        }
        sync_calls: list[dict] = []
        workflow_calls: list[dict] = []
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda **_kwargs: (metas, meta_by_name, [existing])
        )
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: existing
        )
        service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: ([], {}, [linked_followup])
        )
        service._build_repair_management_prefill = (  # type: ignore[method-assign]
            lambda **_kwargs: {
                "fields": {
                    "设备检修关联": "rec_repair",
                    "维修结束时间（2026）": 1784714400000,
                },
                "repair_completed": True,
                "warnings": [],
            }
        )
        service._ensure_repair_followup_select_options = (  # type: ignore[method-assign]
            lambda _fields, source_meta_by_name, **_kwargs: (
                list(source_meta_by_name.values()),
                source_meta_by_name,
            )
        )
        service._patch_record_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: {"code": 0}
        )
        service._upsert_repair_snapshot_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: None
        )
        service._sync_repair_followups_from_summary = (  # type: ignore[method-assign]
            lambda **kwargs: sync_calls.append(kwargs)
            or {"synced_count": 1, "failed_count": 0, "warnings": []}
        )
        service._sync_repair_management_workflow = (  # type: ignore[method-assign]
            lambda **kwargs: workflow_calls.append(kwargs) or (True, [])
        )
        service._invalidate_repair_management_status_cache = (  # type: ignore[method-assign]
            lambda: None
        )

        result = service.update_repair_management_record(
            "rec_summary",
            {"维修名称": "A楼测试维修单"},
            source_repair_ids=["rec_repair"],
            replace_source_relations=True,
            validate_required=False,
        )

        self.assertTrue(sync_calls[-1]["completed"])
        self.assertEqual(
            workflow_calls[-1]["workflow"],
            "维修完成",
        )
        self.assertEqual(result["workflow"], "维修完成")
        self.assertEqual(result["followup_synced_count"], 1)

    def test_repair_management_relations_reject_multiple_records(self):
        service = _TestMaintenancePortalService()
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: ([], {})
        )

        with self.assertRaisesRegex(PortalError, "设备检修关联只能选择一条"):
            service.repair_management_combined_prefill(
                scope="E",
                repair_record_ids=["rec_repair_1", "rec_repair_2"],
            )

    def test_repair_management_relation_rejects_repair_without_event(self):
        service = _TestMaintenancePortalService()
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: ([], {})
        )

        with self.assertRaisesRegex(PortalError, "请先选择关联事件单"):
            service.repair_management_combined_prefill(
                scope="E",
                repair_record_ids=["rec_repair_1"],
            )

    def test_repair_followup_prefill_ignores_missing_linked_repair_record(self):
        service = _TestMaintenancePortalService()

        def meta(name: str) -> FieldMeta:
            return FieldMeta(f"fld_{name}", name, "Text", 1, False, {}, [], False)

        metas = [
            meta("故障维修原因"),
            meta("维修进展描述"),
            meta("后续整改措施"),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        parent_meta = meta(REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME)
        followup = {
            "record_id": "rec_followup_current",
            "raw_fields": {
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary",
            },
            "display_fields": {
                "创建时间": "2026-07-22 15:00",
                "维修进展描述": "已完成现场处理",
                "后续整改措施（如有）": "下周复查",
            },
        }
        service._load_repair_management_target_records = (  # type: ignore[method-assign]
            lambda: ([], {}, [])
        )
        service._ensure_repair_followup_parent_id_field = (  # type: ignore[method-assign]
            lambda: ([parent_meta], {parent_meta.field_name: parent_meta})
        )
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {"record_id": "rec_summary"}
        )

        def load_records_by_ids(**kwargs):
            if kwargs.get("table_id") == REPAIR_MANAGEMENT_REPAIR_TABLE_ID:
                raise PortalError(
                    "飞书接口失败: code=1254043, msg=RecordIdNotFound"
                )
            return [followup]

        service._load_table_records_by_ids = load_records_by_ids  # type: ignore[method-assign]

        result = service._build_repair_management_prefill(
            scope="E",
            event_record_id="rec_event",
            repair_record_ids=["rec_missing_repair"],
            followup_record_ids=["rec_followup_current"],
            month=None,
            meta_by_name=meta_by_name,
            allow_multiple_followups=True,
            event_override={
                "display_fields": {
                    "告警描述": "空调高压告警",
                    "机楼": "E楼",
                }
            },
        )

        self.assertEqual(result["fields"]["故障维修原因"], "空调高压告警")
        self.assertEqual(result["fields"]["维修进展描述"], "已完成现场处理")
        self.assertEqual(result["fields"]["后续整改措施"], "下周复查")
        self.assertTrue(
            any("原关联检修通告已不存在" in item for item in result["warnings"])
        )

    def test_repair_followup_parent_does_not_fall_back_to_old_summary_link(self):
        old_only = {
            "raw_fields": {
                "维修汇总表": [{"record_ids": ["rec_old_summary"]}],
            }
        }
        new_link = {
            "raw_fields": {
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: [
                    {"text": "rec_new_summary", "type": "text"}
                ],
                "维修汇总表": [{"record_ids": ["rec_old_summary"]}],
            }
        }

        self.assertEqual(
            MaintenancePortalService._repair_followup_parent_ids(old_only),
            [],
        )
        self.assertEqual(
            MaintenancePortalService._repair_followup_parent_ids(new_link),
            ["rec_new_summary"],
        )

    def test_repair_management_sync_uses_latest_followup_for_current_fields(self):
        service = _TestMaintenancePortalService()

        def meta(name, ui_type="Text", field_type=1):
            return FieldMeta(
                f"fld_{name}", name, ui_type, field_type, False, {}, [], False
            )

        metas = [
            meta("维修跟进记录"),
            meta("CMDB唯一id"),
            meta("更换备件名称"),
            meta("更换备件数量", "Number", 2),
            meta("故障维修总费用（跟进完成的维修项）", "Number", 2),
            meta("当前维修进度", "Number", 2),
            meta("维修方案附件", "Number", 2),
            meta("维修进展描述"),
            meta("后续整改措施"),
            meta("最新维修跟进时间", "DateTime", 5),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        followups = [
            {
                "record_id": "rec_followup_new",
                "raw_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary",
                    REPAIR_FOLLOWUP_CMDB_FIELD_NAME: [
                        "rec_cmdb_2",
                        "rec_cmdb_1",
                    ],
                },
                "display_fields": {
                    "创建时间": "2026-07-10 11:00",
                    "维修进度": "0.5",
                    "维修进展描述": "处理中",
                    "后续整改措施（如有）": "下周复查运行参数",
                    "更换备件名称": "未完成备件",
                    "更换备件数量": "5",
                    "故障维修总费用": "999",
                },
            },
            {
                "record_id": "rec_followup_done",
                "raw_fields": {
                    REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary",
                    REPAIR_FOLLOWUP_CMDB_FIELD_NAME: [
                        "rec_cmdb_1",
                        "rec_cmdb_3",
                    ],
                },
                "display_fields": {
                    "创建时间": "2026-07-10 10:00",
                    "维修进度": "1",
                    "维修进展描述": "已完成子项",
                    "后续整改措施（如有）": "持续观察",
                    "更换备件名称": "完成备件",
                    "更换备件数量": "2",
                    "故障维修总费用": "100",
                },
            },
        ]
        captured = {}
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {
                "record_id": "rec_summary",
                "raw_fields": {},
            }
        )
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: (metas, meta_by_name)
        )
        service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: ([], {}, followups)
        )
        followup_parent = FieldMeta(
            "fld_parent",
            REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
            "Text",
            1,
            False,
            {},
            [],
            False,
        )
        service._ensure_repair_followup_parent_id_field = (  # type: ignore[method-assign]
            lambda: ([followup_parent], {followup_parent.field_name: followup_parent})
        )
        cmdb_records = [
            {
                "record_id": "rec_cmdb_1",
                "raw_fields": {},
                "display_fields": {"智航唯一ID": "ZH-CMDB-1"},
            },
            {
                "record_id": "rec_cmdb_2",
                "raw_fields": {},
                "display_fields": {"智航唯一ID": "ZH-CMDB-2"},
            },
            {
                "record_id": "rec_cmdb_3",
                "raw_fields": {},
                "display_fields": {"智航唯一ID": "ZH-CMDB-3"},
            },
        ]
        service._load_repair_management_cmdb_records = (  # type: ignore[method-assign]
            lambda: ([], {}, cmdb_records)
        )
        service._load_table_records_by_ids = (  # type: ignore[method-assign]
            lambda **kwargs: [
                next(
                    item
                    for item in followups
                    if item["record_id"] == record_id
                )
                for record_id in kwargs.get("record_ids") or []
            ]
        )
        service._patch_record_fields = (  # type: ignore[method-assign]
            lambda **kwargs: captured.update(kwargs) or {"code": 0}
        )

        service._sync_repair_management_from_followup(
            summary_record_id="rec_summary",
            scope="ALL",
        )

        fields = captured["fields"]
        self.assertEqual(
            fields["维修跟进记录"],
            "rec_followup_new,rec_followup_done",
        )
        self.assertEqual(
            fields["CMDB唯一id"],
            "ZH-CMDB-2、ZH-CMDB-1、ZH-CMDB-3",
        )
        self.assertEqual(fields["当前维修进度"], 0.5)
        self.assertEqual(fields["维修进展描述"], "处理中")
        self.assertEqual(fields["后续整改措施"], "下周复查运行参数")
        self.assertEqual(fields["更换备件名称"], "未完成备件")
        self.assertEqual(fields["更换备件数量"], 5)
        self.assertEqual(fields["故障维修总费用（跟进完成的维修项）"], 100)
        self.assertNotIn("维修方案附件", fields)

    def test_repair_management_cmdb_unique_ids_keep_order_and_skip_missing_values(self):
        service = _TestMaintenancePortalService()
        cmdb_records = [
            {
                "record_id": "rec_cmdb_1",
                "raw_fields": {},
                "display_fields": {"智航唯一ID": "ZH-CMDB-1"},
            },
            {
                "record_id": "rec_cmdb_2",
                "raw_fields": {"智航唯一ID": "ZH-CMDB-1"},
                "display_fields": {},
            },
            {
                "record_id": "rec_cmdb_3",
                "raw_fields": {},
                "display_fields": {},
            },
        ]
        service._load_repair_management_cmdb_records = (  # type: ignore[method-assign]
            lambda: ([], {}, cmdb_records)
        )

        unique_ids, missing_records, missing_unique_ids = (
            service._resolve_repair_management_cmdb_unique_ids(
                ["rec_cmdb_2", "rec_cmdb_1", "rec_cmdb_3"]
            )
        )

        self.assertEqual(unique_ids, ["ZH-CMDB-1"])
        self.assertEqual(missing_records, [])
        self.assertEqual(missing_unique_ids, ["rec_cmdb_3"])

    def test_repair_management_cmdb_meta_refreshes_stale_relation_snapshot(self):
        service = _TestMaintenancePortalService()
        stale_relation = FieldMeta(
            "fld_cmdb_relation",
            "CMDB唯一id",
            "DuplexLink",
            21,
            False,
            {},
            [],
            False,
        )
        writable_text = FieldMeta(
            "fld_cmdb_text",
            "CMDB唯一id",
            "Text",
            1,
            False,
            {},
            [],
            False,
        )
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: ([writable_text], {"CMDB唯一id": writable_text})
        )

        refreshed, selected = service._repair_management_cmdb_summary_meta(
            {"CMDB唯一id": stale_relation}
        )

        self.assertIs(selected, writable_text)
        self.assertIs(refreshed["CMDB唯一id"], writable_text)

    def test_repair_management_sync_adds_missing_warranty_option(self):
        service = _TestMaintenancePortalService()
        warranty_meta = FieldMeta(
            "fld_warranty",
            "是否质保期内",
            "SingleSelect",
            3,
            False,
            {"opt_no": "否"},
            ["否"],
            False,
        )
        workflow_meta = FieldMeta(
            "fld_workflow_l",
            "流程",
            "SingleSelect",
            3,
            False,
            {
                "opt_pending": "未开始",
                "opt_running": "维修中",
                "opt_done": "维修完成",
            },
            ["未开始", "维修中", "维修完成"],
            False,
        )
        cmdb_meta = FieldMeta(
            "fld_cmdb_l",
            "CMDB唯一id",
            "Text",
            1,
            False,
            {},
            [],
            False,
        )
        meta_by_name = {
            warranty_meta.field_name: warranty_meta,
            workflow_meta.field_name: workflow_meta,
            cmdb_meta.field_name: cmdb_meta,
        }
        captured: list[dict] = []
        ensured = {}
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {"record_id": "rec_summary", "raw_fields": {}}
        )
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda **_kwargs: ([warranty_meta, workflow_meta], meta_by_name, [])
        )
        service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: ([], {}, [])
        )
        service._build_repair_management_prefill = (  # type: ignore[method-assign]
            lambda **_kwargs: {"fields": {"是否质保期内": "是"}, "warnings": []}
        )

        def fake_ensure_options(fields, source_meta_by_name, **kwargs):
            ensured.update({"fields": dict(fields), **kwargs})
            updated = dict(source_meta_by_name)
            updated["是否质保期内"] = FieldMeta(
                "fld_warranty",
                "是否质保期内",
                "SingleSelect",
                3,
                False,
                {"opt_no": "否", "opt_yes": "是"},
                ["否", "是"],
                False,
            )
            return list(updated.values()), updated

        service._ensure_repair_followup_select_options = fake_ensure_options  # type: ignore[method-assign]
        service._patch_record_fields = (  # type: ignore[method-assign]
            lambda **kwargs: captured.append(kwargs) or {"code": 0}
        )

        warnings = service._sync_repair_management_from_followup(
            summary_record_id="rec_summary",
            scope="ALL",
        )

        self.assertEqual(warnings, [])
        self.assertEqual(ensured["table_id"], REPAIR_MANAGEMENT_TABLE_ID)
        self.assertEqual(ensured["context_label"], "维修项目")
        self.assertEqual(captured[0]["fields"]["是否质保期内"], "是")
        self.assertIsNone(captured[0]["fields"]["CMDB唯一id"])
        self.assertEqual(captured[-1]["fields"], {"流程": "opt_pending"})

    def test_repair_followup_create_prefills_summary_and_multiple_cmdb_fields(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta(
                "fld_parent",
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
            FieldMeta("fld_cmdb", "CMDB设备唯一ID", "DuplexLink", 21, False, {}, [], False),
            FieldMeta("fld_name", "设备名称", "Text", 1, False, {}, [], False),
            FieldMeta("fld_no", "设备编号", "Text", 1, False, {}, [], False),
            FieldMeta("fld_brand", "设备品牌", "Text", 1, False, {}, [], False),
            FieldMeta("fld_supplier", "供应商名称", "Text", 1, False, {}, [], False),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        summary = {
            "display_fields": {
                "设备名称": "旧设备类别",
                "设备编号": "旧设备编号",
                "设备品牌": "汇总品牌",
                "供应商名称": "汇总供应商",
            },
            "raw_fields": {},
        }
        cmdb_records = [
            {
                "record_id": "rec_cmdb_1",
                "display_fields": {
                    "分类名称": "精密空调",
                    "设备名称": "A-219-CRAH-01",
                },
                "raw_fields": {},
            },
            {
                "record_id": "rec_cmdb_2",
                "display_fields": {
                    "分类名称": "精密空调",
                    "设备名称": "A-220-CRAH-02",
                },
                "raw_fields": {},
            },
        ]
        service._ensure_repair_followup_parent_id_field = (  # type: ignore[method-assign]
            lambda: (metas, meta_by_name)
        )
        service._load_repair_management_cmdb_records = (  # type: ignore[method-assign]
            lambda: ([], {}, cmdb_records)
        )
        service._load_table_records_by_ids = (  # type: ignore[method-assign]
            lambda **kwargs: [
                record
                for record_id in kwargs.get("record_ids") or []
                for record in cmdb_records
                if record["record_id"] == record_id
            ]
        )

        prepared, warnings = service._prepare_repair_followup_fields(
            summary_record_id="rec_summary",
            fields={"设备品牌": "用户品牌"},
            cmdb_record_ids=["rec_cmdb_1", "rec_cmdb_2"],
            summary_record=summary,
        )

        self.assertEqual(warnings, [])
        self.assertEqual(prepared[REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME], "rec_summary")
        self.assertEqual(
            prepared["CMDB设备唯一ID"],
            ["rec_cmdb_1", "rec_cmdb_2"],
        )
        self.assertEqual(prepared["设备名称"], "A-219-CRAH-01、A-220-CRAH-02")
        self.assertEqual(prepared["设备编号"], "A-219-CRAH-01、A-220-CRAH-02")
        self.assertEqual(prepared["设备品牌"], "用户品牌")
        self.assertEqual(prepared["供应商名称"], "汇总供应商")

    def test_repair_followup_update_accepts_multiple_cmdb_records(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta(
                "fld_parent",
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
            FieldMeta(
                "fld_cmdb",
                REPAIR_FOLLOWUP_CMDB_FIELD_NAME,
                "DuplexLink",
                21,
                False,
                {},
                [],
                False,
            ),
            FieldMeta("fld_name", "设备名称", "Text", 1, False, {}, [], False),
            FieldMeta("fld_no", "设备编号", "Text", 1, False, {}, [], False),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        existing_followup = {
            "record_id": "rec_followup",
            "raw_fields": {
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary",
                REPAIR_FOLLOWUP_CMDB_FIELD_NAME: ["rec_cmdb_old"],
            },
            "display_fields": {},
        }
        cmdb_records = [
            {
                "record_id": "rec_cmdb_1",
                "raw_fields": {},
                "display_fields": {"设备名称": "A-UPS-01"},
            },
            {
                "record_id": "rec_cmdb_2",
                "raw_fields": {},
                "display_fields": {"设备名称": "A-UPS-02"},
            },
        ]
        captured: list[dict] = []
        service._ensure_repair_followup_parent_id_field = (  # type: ignore[method-assign]
            lambda: (metas, meta_by_name)
        )

        def load_records(**kwargs):
            if kwargs.get("table_id") == REPAIR_FOLLOWUP_TABLE_ID:
                return [existing_followup]
            return [
                record
                for record_id in kwargs.get("record_ids") or []
                for record in cmdb_records
                if record["record_id"] == record_id
            ]

        service._load_table_records_by_ids = load_records  # type: ignore[method-assign]
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {
                "record_id": "rec_summary",
                "raw_fields": {},
                "display_fields": {},
            }
        )
        service._load_repair_management_cmdb_records = (  # type: ignore[method-assign]
            lambda: ([], {}, cmdb_records)
        )
        service._ensure_repair_followup_select_options = (  # type: ignore[method-assign]
            lambda fields, source_meta_by_name, **_kwargs: (
                list(source_meta_by_name.values()),
                source_meta_by_name,
            )
        )
        service._patch_record_fields = (  # type: ignore[method-assign]
            lambda **kwargs: captured.append(kwargs) or {"code": 0}
        )
        service._upsert_repair_snapshot_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: None
        )
        service._sync_repair_management_from_followup = (  # type: ignore[method-assign]
            lambda **_kwargs: []
        )

        result = service.update_repair_followup_record(
            "rec_followup",
            summary_record_id="rec_summary",
            fields={},
            cmdb_record_ids=["rec_cmdb_1", "rec_cmdb_2"],
            scope="A",
        )

        self.assertFalse(result["summary_sync_pending"])
        self.assertEqual(
            captured[0]["fields"][REPAIR_FOLLOWUP_CMDB_FIELD_NAME],
            ["rec_cmdb_1", "rec_cmdb_2"],
        )
        self.assertEqual(captured[0]["fields"]["设备名称"], "A-UPS-01、A-UPS-02")

    def test_repair_followup_internal_party_clears_supplier_fields(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta(
                "fld_parent",
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
            FieldMeta(
                "fld_party",
                "维修方",
                "SingleSelect",
                3,
                False,
                {},
                ["我方", "厂商"],
                False,
            ),
            FieldMeta("fld_supplier", "供应商名称", "Text", 1, False, {}, [], False),
            FieldMeta("fld_supplier_user", "供应商维修人员", "Text", 1, False, {}, [], False),
        ]
        meta_by_name = {item.field_name: item for item in metas}

        prepared, warnings = service._prepare_repair_followup_fields(
            summary_record_id="rec_summary",
            fields={
                "维修方": "我方",
                "供应商名称": "不应保留的供应商",
                "供应商维修人员": "不应保留的人员",
            },
            summary_record={"display_fields": {}, "raw_fields": {}},
            meta_by_name=meta_by_name,
        )

        self.assertEqual(warnings, [])
        self.assertEqual(prepared["维修方"], "我方")
        self.assertIsNone(prepared["供应商名称"])
        self.assertIsNone(prepared["供应商维修人员"])

    def test_repair_followup_uses_canonical_fields_and_builds_brief(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta("fld_parent", REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME, "Text", 1, False, {}, [], False),
            FieldMeta("fld_brief", "维修简述", "Text", 1, True, {}, [], False),
            FieldMeta("fld_name", "维修名称", "Text", 1, False, {}, [], False),
            FieldMeta("fld_start", "维修开始时间", "DateTime", 5, False, {}, [], False),
            FieldMeta("fld_worker", "随工人员（我方维修人员）", "User", 11, False, {}, [], False),
            FieldMeta("fld_device", "设备编号", "Text", 1, False, {}, [], False),
            FieldMeta("fld_brand", "设备品牌", "Text", 1, False, {}, [], False),
            FieldMeta("fld_vendor", "供应商名称", "Text", 1, False, {}, [], False),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        start_ms = int(dt.datetime(2026, 7, 12, 10, 30).timestamp() * 1000)
        summary = {
            "display_fields": {
                "维修名称": "A楼蓄电池维修",
                "设备编号": "A-245-HVDC-01",
                "设备品牌": "双登",
                "供应商名称": "测试供应商",
                "维修开始时间": "2026-07-12 10:30",
                "随工人员（或我方维修人员）": "张三",
            },
            "raw_fields": {
                "维修开始时间": start_ms,
                "随工人员（或我方维修人员）": [{"id": "ou_worker", "name": "张三"}],
            },
        }

        prepared, warnings = service._prepare_repair_followup_fields(
            summary_record_id="rec_summary",
            fields={"设备品牌": "南都"},
            summary_record=summary,
            meta_by_name=meta_by_name,
        )

        self.assertEqual(warnings, [])
        self.assertEqual(prepared["维修简述"], "2026/07/12 - 张三")
        self.assertEqual(prepared["设备编号"], "A-245-HVDC-01")
        self.assertEqual(prepared["设备品牌"], "南都")
        self.assertEqual(prepared["供应商名称"], "测试供应商")

    def test_repair_followup_backfill_links_unique_summary_and_writes_backlink(self):
        service = _TestMaintenancePortalService()
        summary_metas = [
            FieldMeta("fld_summary_name", "维修名称", "Text", 1, True, {}, [], False),
            FieldMeta("fld_summary_no", "维修单号", "Text", 1, False, {}, [], False),
            FieldMeta("fld_summary_links", "维修跟进记录", "Text", 1, False, {}, [], False),
        ]
        followup_metas = [
            FieldMeta("fld_parent", REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME, "Text", 1, False, {}, [], False),
            FieldMeta("fld_follow_name", "维修名称", "Text", 1, False, {}, [], False),
            FieldMeta("fld_follow_no", "维修单号", "Text", 1, False, {}, [], False),
            FieldMeta("fld_follow_brief", "维修简述", "Text", 1, True, {}, [], False),
        ]
        summary_meta_by_name = {item.field_name: item for item in summary_metas}
        followup_meta_by_name = {item.field_name: item for item in followup_metas}
        summaries = [
            {
                "record_id": "rec_summary",
                "display_fields": {"维修名称": "A楼测试维修", "维修单号": "WX-001"},
                "raw_fields": {"维修名称": "A楼测试维修", "维修单号": "WX-001"},
            }
        ]
        followups = [
            {
                "record_id": "rec_followup",
                "display_fields": {"维修名称": "A楼测试维修", "维修单号": "WX-001"},
                "raw_fields": {},
            }
        ]
        patches = []
        persisted = []
        service._state_store.get_backend_runtime = lambda _key: None  # type: ignore[method-assign]
        service._state_store.put_backend_runtime = (  # type: ignore[method-assign]
            lambda key, payload: persisted.append((key, payload))
        )
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **kwargs: (
                (summary_metas, summary_meta_by_name)
                if kwargs.get("table_id") == REPAIR_MANAGEMENT_TABLE_ID
                else (followup_metas, followup_meta_by_name)
            )
        )
        service._ensure_repair_followup_parent_id_field = (  # type: ignore[method-assign]
            lambda: (followup_metas, followup_meta_by_name)
        )
        service._load_table_records = (  # type: ignore[method-assign]
            lambda **kwargs: summaries
            if kwargs.get("table_id") == REPAIR_MANAGEMENT_TABLE_ID
            else followups
        )
        service._ensure_repair_followup_select_options = (  # type: ignore[method-assign]
            lambda _fields, meta_by_name: (list(meta_by_name.values()), meta_by_name)
        )
        service._patch_record_fields = (  # type: ignore[method-assign]
            lambda **kwargs: patches.append(kwargs) or {"code": 0}
        )

        result = service.backfill_repair_followup_records(force=True)

        followup_patch = next(
            item for item in patches if item["table_id"] == REPAIR_FOLLOWUP_TABLE_ID
        )
        summary_patch = next(
            item for item in patches if item["table_id"] == REPAIR_MANAGEMENT_TABLE_ID
        )
        self.assertEqual(
            followup_patch["fields"][REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME],
            "rec_summary",
        )
        self.assertEqual(summary_patch["fields"]["维修跟进记录"], "rec_followup")
        self.assertEqual(result["updated"], 1)
        self.assertEqual(result["backlink_updates"], 1)
        self.assertEqual(persisted[0][0], REPAIR_FOLLOWUP_BACKFILL_RUNTIME_KEY)

    def test_repair_followup_select_options_are_added_before_record_write(self):
        service = _TestMaintenancePortalService()
        initial = FieldMeta(
            "fld_specialty",
            "所属专业",
            "SingleSelect",
            3,
            False,
            {"opt_electric": "电气"},
            ["电气"],
            False,
        )
        refreshed = FieldMeta(
            "fld_specialty",
            "所属专业",
            "SingleSelect",
            3,
            False,
            {"opt_electric": "电气", "opt_hvac": "暖通"},
            ["电气", "暖通"],
            False,
        )
        field_updates = []
        service._request_json = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {
                "data": {
                    "items": [
                        {
                            "field_id": "fld_specialty",
                            "field_name": "所属专业",
                            "type": 3,
                            "ui_type": "SingleSelect",
                            "property": {
                                "options": [
                                    {"id": "opt_electric", "name": "电气", "color": 0}
                                ]
                            },
                        }
                    ]
                }
            }
        )
        service._request_payload = (  # type: ignore[method-assign]
            lambda *args, **kwargs: field_updates.append((args, kwargs)) or {"code": 0}
        )
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: ([refreshed], {"所属专业": refreshed})
        )

        _metas, meta_by_name = service._ensure_repair_followup_select_options(
            {"所属专业": "暖通"},
            {"所属专业": initial},
        )

        options = field_updates[0][1]["json_payload"]["property"]["options"]
        self.assertEqual(
            [item["name"] for item in options],
            ["电气", "暖通", "弱电", "消防"],
        )
        self.assertEqual(meta_by_name["所属专业"].option_names, ["电气", "暖通"])

    def test_repair_followup_legacy_option_ids_use_business_labels(self):
        self.assertEqual(
            MaintenancePortalService._repair_management_canonical_select_text(
                "所属专业",
                "optAssNaw3",
            ),
            "电气",
        )
        self.assertEqual(
            MaintenancePortalService._repair_management_canonical_select_text(
                "对应来源",
                "optt3FGRvz",
            ),
            "BMS系统",
        )
        self.assertEqual(
            MaintenancePortalService._repair_management_canonical_select_text(
                "对应事件等级",
                "optYJ1jQeh",
            ),
            "I3",
        )
        self.assertEqual(
            MaintenancePortalService._repair_management_canonical_select_text(
                "维修方",
                "optt2azmKP",
            ),
            "我方",
        )
        service = _TestMaintenancePortalService()
        payload = service._repair_management_record_payload(
            {
                "record_id": "rec_legacy_options",
                "display_fields": {
                    "所属专业": "opt3jdJJb7",
                    "对应来源": "optt3FGRvz",
                    "对应事件等级": "optYJ1jQeh",
                },
                "raw_fields": {},
            }
        )
        self.assertEqual(payload["display_fields"]["所属专业"], "暖通")
        self.assertEqual(payload["display_fields"]["对应来源"], "BMS系统")
        self.assertEqual(payload["display_fields"]["对应事件等级"], "I3")

    def test_repair_followup_multiple_cmdb_records_use_selected_device_names(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta(
                "fld_parent",
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
            FieldMeta("fld_cmdb", "CMDB设备唯一ID", "DuplexLink", 21, False, {}, [], False),
            FieldMeta("fld_name", "设备名称", "Text", 1, False, {}, [], False),
            FieldMeta("fld_no", "设备编号", "Text", 1, False, {}, [], False),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        cmdb_records = [
            {
                "record_id": "rec_cmdb_1",
                "display_fields": {"分类名称": "精密空调", "设备名称": "A-219-CRAH-01"},
                "raw_fields": {},
            },
            {
                "record_id": "rec_cmdb_2",
                "display_fields": {"分类名称": "蓄电池", "设备名称": "A-220-BAT-01"},
                "raw_fields": {},
            },
        ]
        service._ensure_repair_followup_parent_id_field = (  # type: ignore[method-assign]
            lambda: (metas, meta_by_name)
        )
        service._load_repair_management_cmdb_records = (  # type: ignore[method-assign]
            lambda: ([], {}, cmdb_records)
        )
        service._load_table_records_by_ids = (  # type: ignore[method-assign]
            lambda **_kwargs: cmdb_records
        )

        prepared, warnings = service._prepare_repair_followup_fields(
            summary_record_id="rec_summary",
            fields={},
            cmdb_record_ids=["rec_cmdb_1", "rec_cmdb_2"],
            summary_record={"display_fields": {}, "raw_fields": {}},
        )

        self.assertEqual(prepared["CMDB设备唯一ID"], ["rec_cmdb_1", "rec_cmdb_2"])
        self.assertEqual(prepared["设备名称"], "A-219-CRAH-01、A-220-BAT-01")
        self.assertEqual(prepared["设备编号"], "A-219-CRAH-01、A-220-BAT-01")
        self.assertEqual(warnings, [])

    def test_repair_followup_device_name_fills_blank_device_number(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta(
                "fld_parent",
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
            FieldMeta(
                "fld_name",
                "设备名称",
                "SingleSelect",
                3,
                False,
                {"opt_crac": "精密空调"},
                ["精密空调"],
                False,
            ),
            FieldMeta("fld_no", "设备编号", "Text", 1, False, {}, [], False),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        service._ensure_repair_followup_parent_id_field = (  # type: ignore[method-assign]
            lambda: (metas, meta_by_name)
        )

        prepared, warnings = service._prepare_repair_followup_fields(
            summary_record_id="rec_summary",
            fields={"设备名称": "精密空调"},
            summary_record={"display_fields": {}, "raw_fields": {}},
        )

        self.assertEqual(prepared["设备名称"], "精密空调")
        self.assertEqual(prepared["设备编号"], "精密空调")
        self.assertEqual(warnings, [])

    def test_repair_followup_uses_parent_id_and_copies_writable_summary_snapshot(self):
        metas = [
            FieldMeta(
                "fld_parent",
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
                "Text",
                1,
                False,
                {},
                [],
                False,
            ),
            FieldMeta("fld_title", "维修简述", "Text", 1, True, {}, [], False),
            FieldMeta("fld_name", "维修名称", "Text", 1, False, {}, [], False),
            FieldMeta("fld_start", "维修开始时间", "DateTime", 5, False, {}, [], False),
            FieldMeta("fld_end", "结束时间", "DateTime", 5, False, {}, [], False),
            FieldMeta(
                "fld_source",
                "维修来源",
                "MultiSelect",
                4,
                False,
                {"opt_bms": "BMS系统"},
                ["BMS系统"],
                False,
            ),
            FieldMeta(
                "fld_device",
                "设备名称",
                "SingleSelect",
                3,
                False,
                {},
                ["E-217-HVDC-202"],
                False,
            ),
            FieldMeta("fld_old", "维修汇总表", "DuplexLink", 21, False, {}, [], False),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        summary = {
            "display_fields": {
                "维修名称": "E楼蓄电池维修",
                "对应来源": "BMS系统",
                "设备名称": "E-217-HVDC-202",
                "维修开始时间": "2026-07-11 10:00",
                "维修结束时间（2026）": "2026-07-11 12:00",
            },
            "raw_fields": {
                "对应来源": "['BMS系统']",
                "维修开始时间": 1783735200000,
                "维修结束时间（2026）": 1783742400000,
            },
        }

        prepared, warnings = _TestMaintenancePortalService()._prepare_repair_followup_fields(
            summary_record_id="rec_summary",
            fields={},
            summary_record=summary,
            meta_by_name=meta_by_name,
        )

        self.assertEqual(warnings, [])
        self.assertEqual(prepared[REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME], "rec_summary")
        self.assertEqual(prepared["维修简述"], "E楼蓄电池维修")
        self.assertEqual(prepared["维修名称"], "E楼蓄电池维修")
        self.assertEqual(prepared["维修开始时间"], 1783735200000)
        self.assertEqual(prepared["结束时间"], 1783742400000)
        self.assertEqual(prepared["维修来源"], ["BMS系统"])
        self.assertEqual(prepared["设备名称"], "E-217-HVDC-202")
        self.assertNotIn("维修汇总表", prepared)

    def test_repair_followup_brand_model_options_are_scoped_by_brand(self):
        brand = FieldMeta(
            "fld_brand",
            "设备品牌",
            "SingleSelect",
            3,
            False,
            {"opt_a": "品牌A", "opt_b": "品牌B"},
            ["品牌A", "品牌B"],
            False,
        )
        model = FieldMeta(
            "fld_model",
            "设备型号",
            "SingleSelect",
            3,
            False,
            {"opt_a1": "A-100", "opt_a2": "A-200", "opt_b1": "B-100"},
            ["A-100", "A-200", "B-100"],
            False,
        )
        records = [
            {"display_fields": {"设备品牌": "品牌A", "设备型号": "A-200"}},
            {"display_fields": {"设备品牌": "品牌B", "设备型号": "B-100"}},
            {"display_fields": {"设备品牌": "品牌A", "设备型号": "不存在型号"}},
        ]
        summary = {"display_fields": {"设备品牌": "品牌A", "设备型号": "A-100"}}

        result = MaintenancePortalService._repair_followup_brand_model_options(
            records=records,
            summary_record=summary,
            meta_by_name={brand.field_name: brand, model.field_name: model},
        )

        self.assertEqual(result["品牌A"], ["A-100", "A-200"])
        self.assertEqual(result["品牌B"], ["B-100"])

    def test_repair_followup_brand_model_catalog_reads_all_records_once(self):
        service = _TestMaintenancePortalService()
        catalog_brand = FieldMeta(
            "fld_catalog_brand",
            "设备品牌",
            "SingleSelect",
            3,
            False,
            {},
            ["圣阳", "双登", "南都"],
            False,
        )
        catalog_model = FieldMeta(
            "fld_catalog_model",
            "设备型号",
            "SingleSelect",
            3,
            False,
            {},
            ["SP12-100", "SP12-200", "GFMHR-1250W", "6-GFM-150HR"],
            False,
        )
        calls = []
        persisted = []

        def fake_search(**kwargs):
            calls.append(kwargs)
            return [
                {"display_fields": {"设备品牌": "圣阳", "设备型号": "SP12-100"}},
                {"display_fields": {"设备品牌": "双登", "设备型号": "GFMHR-1250W"}},
                {"display_fields": {"设备品牌": "圣阳", "设备型号": "SP12-200"}},
                {"display_fields": {"设备品牌": "南都", "设备型号": "6-GFM-150HR"}},
            ]

        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: (
                [catalog_brand, catalog_model],
                {
                    catalog_brand.field_name: catalog_brand,
                    catalog_model.field_name: catalog_model,
                },
            )
        )
        service._search_table_records = fake_search  # type: ignore[method-assign]
        service._state_store.get_backend_runtime = (  # type: ignore[method-assign]
            lambda _key: None
        )
        service._state_store.put_backend_runtime = (  # type: ignore[method-assign]
            lambda key, payload: persisted.append((key, payload))
        )

        first = service._load_repair_followup_brand_model_catalog({})
        second = service._load_repair_followup_brand_model_catalog({})

        self.assertEqual(first["圣阳"], ["SP12-100", "SP12-200"])
        self.assertEqual(first["南都"], ["6-GFM-150HR"])
        self.assertEqual(second, first)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["table_id"], REPAIR_EQUIPMENT_CATALOG_TABLE_ID)
        self.assertEqual(calls[0]["limit"], REPAIR_FOLLOWUP_CATALOG_MAX_RECORDS)
        self.assertEqual(persisted[0][0], REPAIR_FOLLOWUP_CATALOG_RUNTIME_KEY)
        self.assertEqual(persisted[0][1]["record_count"], 4)

    def test_repair_followup_brand_model_catalog_uses_fresh_sqlite_snapshot(self):
        service = _TestMaintenancePortalService()
        service._state_store.get_backend_runtime = (  # type: ignore[method-assign]
            lambda key: {
                "refreshed_at": time.time(),
                "mapping": {"南都": ["6-GFM-150HR", "GFM-360E"]},
            }
            if key == REPAIR_FOLLOWUP_CATALOG_RUNTIME_KEY
            else None
        )
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("fresh local snapshot must avoid remote fields read")
        )
        service._search_table_records = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("fresh local snapshot must avoid remote records read")
        )

        result = service._load_repair_followup_brand_model_catalog({})

        self.assertEqual(result["南都"], ["6-GFM-150HR", "GFM-360E"])

    def test_repair_followup_create_recovers_single_select_conversion_failure(self):
        service = _TestMaintenancePortalService()
        parent = FieldMeta(
            "fld_parent",
            REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME,
            "Text",
            1,
            False,
            {},
            [],
            False,
        )
        brand = FieldMeta(
            "fld_brand",
            "设备品牌",
            "SingleSelect",
            3,
            False,
            {},
            ["品牌A"],
            False,
        )
        model = FieldMeta(
            "fld_model",
            "设备型号",
            "SingleSelect",
            3,
            False,
            {},
            ["A-100"],
            False,
        )
        create_calls = []
        patch_calls = []

        def fake_create_record_fields(**kwargs):
            create_calls.append(dict(kwargs["fields"]))
            if len(create_calls) == 1:
                raise PortalError("飞书记录创建失败: code=1254062, msg=SingleSelectFieldConvFail")
            return {"data": {"record": {"record_id": "rec_followup_created"}}}

        def fake_patch_record_fields(**kwargs):
            patch_calls.append(dict(kwargs["fields"]))
            if "设备型号" in kwargs["fields"]:
                raise PortalError("飞书记录更新失败: code=1254062, msg=SingleSelectFieldConvFail")
            return {}

        service._create_record_fields = fake_create_record_fields  # type: ignore[method-assign]
        service._patch_record_fields = fake_patch_record_fields  # type: ignore[method-assign]
        service._delete_record_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("recoverable select failure must not delete the base record")
        )

        payload, effective, warnings = service._create_repair_followup_fields(
            {
                REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary",
                "设备品牌": "品牌A",
                "设备型号": "A-100",
            },
            {parent.field_name: parent, brand.field_name: brand, model.field_name: model},
        )

        self.assertEqual(service._created_record_id(payload), "rec_followup_created")
        self.assertEqual(create_calls[1], {REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME: "rec_summary"})
        self.assertEqual(patch_calls, [{"设备品牌": "品牌A"}, {"设备型号": "A-100"}])
        self.assertEqual(effective["设备品牌"], "品牌A")
        self.assertNotIn("设备型号", effective)
        self.assertTrue(any("设备型号" in warning for warning in warnings))

    def test_repair_management_update_clears_stale_auto_end_time(self):
        service = _TestMaintenancePortalService()
        metas = [
            FieldMeta("fld_title", "维修名称", "Text", 1, True, {}, [], False),
            FieldMeta("fld_repairs", "设备检修关联", "Text", 1, False, {}, [], False),
            FieldMeta("fld_end", "维修结束时间", "DateTime", 5, False, {}, [], False),
        ]
        meta_by_name = {item.field_name: item for item in metas}
        captured = {}
        service._load_table_fields = (  # type: ignore[method-assign]
            lambda **_kwargs: (metas, meta_by_name)
        )
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {
                "record_id": "rec_summary",
                "raw_fields": {"设备检修关联": '["rec_repair"]'},
            }
        )
        service._build_repair_management_prefill = (  # type: ignore[method-assign]
            lambda **_kwargs: {
                "fields": {"设备检修关联": '["rec_repair"]'},
                "warnings": [],
            }
        )
        service._patch_record_fields = (  # type: ignore[method-assign]
            lambda **kwargs: captured.update(kwargs) or {"code": 0}
        )
        service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: ([], {}, [])
        )

        service.update_repair_management_record(
            "rec_summary",
            {"维修名称": "E楼检修"},
            source_repair_ids=["rec_repair"],
        )

        self.assertNotIn("维修结束时间", captured["fields"])
        self.assertEqual(captured["fields"]["设备检修关联"], "rec_repair")

    def test_repair_management_delete_cascades_followup_records(self):
        service = _TestMaintenancePortalService()
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda **_kwargs: ([], {}, [])
        )
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {"record_id": "rec_summary", "raw_fields": {}}
        )
        service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: (
                [],
                {},
                [
                    {"record_id": "rec_followup_1"},
                    {"record_id": "rec_followup_2"},
                ],
            )
        )
        deleted = []
        snapshot_deleted = []
        service._delete_record_fields = (  # type: ignore[method-assign]
            lambda **kwargs: deleted.append(
                (kwargs["table_id"], kwargs["record_id"])
            ) or {"code": 0}
        )
        service._delete_repair_snapshot_item = (  # type: ignore[method-assign]
            lambda source_key, record_id: snapshot_deleted.append(
                (source_key, record_id)
            )
        )

        result = service.delete_repair_management_record("rec_summary")

        self.assertEqual(
            deleted,
            [
                (REPAIR_FOLLOWUP_TABLE_ID, "rec_followup_1"),
                (REPAIR_FOLLOWUP_TABLE_ID, "rec_followup_2"),
                (REPAIR_MANAGEMENT_TABLE_ID, "rec_summary"),
            ],
        )
        self.assertEqual(result["deleted_followup_count"], 2)
        self.assertEqual(
            result["deleted_followup_ids"],
            ["rec_followup_1", "rec_followup_2"],
        )
        self.assertIn(
            (REPAIR_SNAPSHOT_SOURCE_PROJECTS, "rec_summary"),
            snapshot_deleted,
        )

    def test_repair_management_delete_keeps_parent_when_followup_delete_fails(self):
        service = _TestMaintenancePortalService()
        service._load_repair_management_project_records = (  # type: ignore[method-assign]
            lambda **_kwargs: ([], {}, [])
        )
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {"record_id": "rec_summary", "raw_fields": {}}
        )
        service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: (
                [],
                {},
                [{"record_id": "rec_followup_1"}, {"record_id": "rec_followup_2"}],
            )
        )
        deleted = []

        def fake_delete(**kwargs):
            deleted.append((kwargs["table_id"], kwargs["record_id"]))
            if kwargs["record_id"] == "rec_followup_2":
                raise PortalError("模拟删除失败")
            return {"code": 0}

        service._delete_record_fields = fake_delete  # type: ignore[method-assign]

        with self.assertRaisesRegex(PortalError, "已删除 1/2 条跟进记录"):
            service.delete_repair_management_record("rec_summary")

        self.assertNotIn((REPAIR_MANAGEMENT_TABLE_ID, "rec_summary"), deleted)

    def test_repair_management_create_filters_system_readonly_field_names(self):
        service = _TestMaintenancePortalService()
        editable = FieldMeta("fld_building", "所属数据中心/楼栋-使用", "Text", 1, False, {}, [], False)
        stage = FieldMeta("fld_stage", "流程", "Stage", 1, False, {}, [], False)
        created_at = FieldMeta("fld_created_at", "创建日期", "DateTime", 5, False, {}, [], False)
        formula_named = FieldMeta("fld_formula_named", "公式", "Text", 1, False, {}, [], False)
        captured = {}

        def fake_load_fields(*, app_token, table_id):
            return [editable, stage, created_at, formula_named], {
                editable.field_name: editable,
                stage.field_name: stage,
                created_at.field_name: created_at,
                formula_named.field_name: formula_named,
            }

        def fake_create_record_fields(**kwargs):
            captured.update(kwargs)
            return {"data": {"record": {"record_id": "rec_repair_created"}}}

        service._load_table_fields = fake_load_fields  # type: ignore[method-assign]
        service._create_record_fields = fake_create_record_fields  # type: ignore[method-assign]

        result = service.create_repair_management_record(
            {
                "所属数据中心/楼栋-使用": "E楼",
                "流程": "待处理",
                "创建日期": "2026-06-25",
                "公式": "不可写",
            }
        )

        self.assertEqual(captured["fields"], {"所属数据中心/楼栋-使用": "E楼"})
        self.assertEqual(result["field_count"], 1)

    def test_repair_management_delete_uses_repair_source_table(self):
        service = _TestMaintenancePortalService()
        captured = {}

        def fake_delete_record_fields(**kwargs):
            captured.update(kwargs)
            return {"code": 0}

        service._delete_record_fields = fake_delete_record_fields  # type: ignore[method-assign]
        service._ensure_repair_management_record_in_scope = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: {"record_id": "rec_repair_delete", "raw_fields": {}}
        )
        service._load_repair_followups_for_summary = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: ([], {}, [])
        )

        result = service.delete_repair_management_record("rec_repair_delete")

        self.assertEqual(captured["app_token"], REPAIR_SOURCE_APP_TOKEN)
        self.assertEqual(captured["table_id"], REPAIR_SOURCE_TABLE_ID)
        self.assertEqual(captured["record_id"], "rec_repair_delete")
        self.assertTrue(result["deleted"])
        self.assertEqual(result["deleted_followup_count"], 0)

    def test_event_transfer_to_repair_updates_only_transfer_field(self):
        service = _TestMaintenancePortalService()
        captured = {}
        refreshed = {}
        service._repair_management_event_cache = {"records": [{"record_id": "stale"}]}

        def fake_event_source_config():
            return "event_app", "event_table", "event_notice"

        def fake_patch_record_fields(**kwargs):
            captured.update(kwargs)
            return {"code": 0}

        def fake_refresh_event_month_snapshot(month):
            refreshed["month"] = month
            return {"month": month, "status": "active"}

        service._event_source_config = fake_event_source_config  # type: ignore[method-assign]
        service._patch_record_fields = fake_patch_record_fields  # type: ignore[method-assign]
        service.refresh_event_month_snapshot = fake_refresh_event_month_snapshot  # type: ignore[method-assign]

        result = service.mark_event_transferred_to_repair(
            record_id="rec_event_1",
            month="2026-06",
        )

        self.assertEqual(captured["app_token"], "event_app")
        self.assertEqual(captured["table_id"], "event_table")
        self.assertEqual(captured["record_id"], "rec_event_1")
        self.assertEqual(captured["fields"], {"是否转检修": True})
        self.assertEqual(refreshed["month"], "2026-06")
        self.assertIsNone(service._repair_management_event_cache)
        self.assertTrue(result["transfer_to_overhaul"])


if __name__ == "__main__":
    unittest.main()

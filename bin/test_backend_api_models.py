import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from clipflow_backend.api_models import (  # noqa: E402
    OngoingDeleteRequest,
    JobMarkStuckFailedRequest,
    PermissionRequestBulkReviewRequest,
    PermissionRequestCreate,
    PermissionRequestReviewRequest,
    RepairManagementPrefillRequest,
    RepairManagementRecordRequest,
    RepairFollowupRecordRequest,
    QtClipboardAckRequest,
    QtActiveItemsDeltaRequest,
    QtCommandRequest,
    QtDialogSessionRequest,
    QtEventAckRequest,
    QtJobProgressRequest,
    WorkbenchActionRequest,
    parse_api_model,
)
from clipflow_backend.main import FastAPIPortalController  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class _FakeRepairEventRouteService:
    def __init__(self):
        self.calls: list[tuple] = []

    def get_repair_management_records(
        self,
        scope: str = "ALL",
        query: str = "",
        limit: int = 200,
        offset: int = 0,
        focus_record_id: str = "",
    ) -> dict:
        self.calls.append(("list_repair", scope, query, limit, focus_record_id))
        return {
            "records": [],
            "fields": [{"field_name": "维修名称", "readonly": False}],
            "total": 0,
        }

    def get_repair_management_status(
        self,
        *,
        scope: str = "ALL",
        query: str = "",
        state: str = "all",
        period: str = "all",
        limit: int = 100,
        offset: int = 0,
        force_refresh: bool = False,
    ) -> dict:
        self.calls.append(
            (
                "repair_status",
                scope,
                query,
                state,
                period,
                limit,
                offset,
                force_refresh,
            )
        )
        return {
            "records": [],
            "total": 0,
            "stats": {
                "total": 0,
                "without_followup": 0,
                "in_progress": 0,
                "average_progress": 0,
            },
        }

    def create_repair_management_record(
        self,
        fields: dict,
        *,
        operation_id: str = "",
        source_event_id: str = "",
        source_repair_ids: list[str] | None = None,
        source_month: str = "",
        scope: str = "ALL",
    ) -> dict:
        self.calls.append(
            (
                "create_repair",
                scope,
                dict(fields or {}),
                source_event_id,
                tuple(source_repair_ids or []),
                source_month,
            )
        )
        return {"record_id": "rec-new", "fields": dict(fields or {})}

    def update_repair_management_record(
        self,
        record_id: str,
        fields: dict,
        *,
        source_event_id: str = "",
        source_repair_ids: list[str] | None = None,
        replace_source_relations: bool = False,
        source_month: str = "",
        scope: str = "ALL",
    ) -> dict:
        self.calls.append(
            (
                "update_repair",
                scope,
                record_id,
                dict(fields or {}),
                source_event_id,
                tuple(source_repair_ids or []),
                replace_source_relations,
                source_month,
            )
        )
        return {"record_id": record_id, "fields": dict(fields or {})}

    def delete_repair_management_record(self, record_id: str, *, scope: str = "ALL") -> dict:
        self.calls.append(("delete_repair", scope, record_id))
        return {"record_id": record_id, "deleted": True}

    def list_repair_management_event_candidates(
        self,
        *,
        scope: str = "ALL",
        month: str = "",
        query: str = "",
        limit: int = 50,
    ) -> dict:
        self.calls.append(("repair_events", scope, month, query, limit))
        return {"records": [{"record_id": "rec-event"}], "total": 1}

    def repair_management_event_prefill(
        self,
        *,
        scope: str = "ALL",
        record_id: str = "",
        month: str = "",
    ) -> dict:
        self.calls.append(("repair_prefill", scope, record_id, month))
        return {"event": {"record_id": record_id}, "fields": {"维修名称": "事件检修"}}

    def list_repair_management_repair_candidates(
        self,
        *,
        scope: str = "ALL",
        event_record_id: str = "",
        month: str = "",
        query: str = "",
        limit: int = 80,
    ) -> dict:
        self.calls.append(("repair_candidates", scope, event_record_id, month, query, limit))
        return {"records": [{"record_id": "rec-repair"}], "auto_selected_ids": ["rec-repair"]}

    def repair_management_combined_prefill(
        self,
        *,
        scope: str = "ALL",
        event_record_id: str = "",
        repair_record_ids: list[str] | None = None,
        month: str = "",
    ) -> dict:
        self.calls.append(
            (
                "combined_prefill",
                scope,
                event_record_id,
                tuple(repair_record_ids or []),
                month,
            )
        )
        return {"fields": {"维修名称": "组合预填"}, "warnings": []}

    def list_repair_management_cmdb_candidates(
        self,
        *,
        scope: str = "ALL",
        query: str = "",
        limit: int = 160,
    ) -> dict:
        self.calls.append(("cmdb_candidates", scope, query, limit))
        return {"records": [{"record_id": "rec-cmdb"}], "total": 1}

    def list_repair_followup_people(
        self,
        *,
        scope: str = "ALL",
        query: str = "",
        limit: int = 80,
        refresh: bool = False,
    ) -> dict:
        self.calls.append(("followup_people", scope, query, limit, refresh))
        return {
            "people": [{"user_id": "ou-zhang", "name": "张宇航"}],
            "total": 1,
        }

    def get_repair_followup_records(
        self,
        *,
        summary_record_id: str,
        scope: str = "ALL",
        query: str = "",
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        self.calls.append(("list_followups", scope, summary_record_id, query, limit))
        return {"records": [{"record_id": "rec-followup"}], "fields": [], "total": 1}

    def create_repair_followup_record(
        self,
        *,
        summary_record_id: str,
        fields: dict,
        cmdb_record_ids: list[str] | None = None,
        operation_id: str = "",
        scope: str = "ALL",
    ) -> dict:
        self.calls.append(
            (
                "create_followup",
                scope,
                summary_record_id,
                tuple(cmdb_record_ids or []),
                dict(fields or {}),
            )
        )
        return {"record_id": "rec-followup-new"}

    def update_repair_followup_record(
        self,
        record_id: str,
        *,
        summary_record_id: str,
        fields: dict,
        cmdb_record_ids: list[str] | None = None,
        scope: str = "ALL",
    ) -> dict:
        self.calls.append(
            (
                "update_followup",
                scope,
                record_id,
                summary_record_id,
                tuple(cmdb_record_ids or []),
                dict(fields or {}),
            )
        )
        return {"record_id": record_id}

    def delete_repair_followup_record(
        self,
        record_id: str,
        *,
        summary_record_id: str,
        scope: str = "ALL",
    ) -> dict:
        self.calls.append(("delete_followup", scope, record_id, summary_record_id))
        return {"record_id": record_id, "deleted": True}

    def mark_event_transferred_to_repair(self, record_id: str = "", month: str = "") -> dict:
        self.calls.append(("transfer_repair", record_id, month))
        return {"record_id": record_id, "transfer_to_overhaul": True}


class BackendApiModelTests(unittest.TestCase):
    def test_repair_management_relations_are_single_select(self):
        for model in (RepairManagementRecordRequest, RepairManagementPrefillRequest):
            with self.assertRaises(ValueError):
                parse_api_model(
                    model,
                    {
                        "source_repair_ids": ["rec-repair-1", "rec-repair-2"],
                    },
                )
            with self.assertRaises(ValueError):
                parse_api_model(
                    model,
                    {
                        "source_followup_ids": ["rec-followup-1"],
                    },
                )

        with self.assertRaises(ValueError):
            parse_api_model(RepairFollowupRecordRequest, {"scope": "E"})

    def test_repair_management_and_event_transfer_routes_are_native_and_authorized(self):
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        original_service = PortalRuntime.service
        original_sessions = dict(PortalRuntime.auth_manager._sessions)
        service = _FakeRepairEventRouteService()
        session_id = "repair-event-route-session"
        PortalRuntime.service = service
        with PortalRuntime.auth_manager._lock:
            PortalRuntime.auth_manager._sessions[session_id] = {
                "session_id": session_id,
                "user": {"name": "测试管理员", "open_id": ""},
                "role": "admin",
                "allowed_scopes": ["E"],
                "expires_at": 9999999999,
            }
        client = TestClient(controller._build_app())
        headers = {"Cookie": f"{AUTH_COOKIE_NAME}={session_id}"}
        try:
            with patch.object(controller, "_proxy_request", side_effect=AssertionError("proxy used")):
                unauth = client.get("/api/repair-management/records?scope=E")
                denied = client.get(
                    "/api/repair-management/records?scope=A",
                    headers=headers,
                )
                listed = client.get(
                    "/api/repair-management/records?scope=E&q=冷站&limit=3&focus_record_id=rec-focus",
                    headers=headers,
                )
                repair_status = client.get(
                    "/api/repair-management/status?scope=E&q=冷站&state=in_progress&limit=12&offset=3&refresh=1",
                    headers=headers,
                )
                created = client.post(
                    "/api/repair-management/records",
                    headers=headers,
                    json={
                        "scope": "E",
                        "source_event_id": "rec-event",
                        "source_repair_ids": ["rec-repair"],
                        "source_month": "2026-06",
                        "fields": {"维修名称": "测试检修"},
                    },
                )
                updated = client.put(
                    "/api/repair-management/records/rec-1",
                    headers=headers,
                    json={
                        "scope": "E",
                        "source_event_id": "rec-event",
                        "source_repair_ids": ["rec-repair"],
                        "replace_source_relations": True,
                        "source_month": "2026-06",
                        "fields": {"专业": "电气"},
                    },
                )
                deleted = client.delete(
                    "/api/repair-management/records/rec-1?scope=E",
                    headers=headers,
                )
                repair_events = client.get(
                    "/api/repair-management/event-candidates?scope=E&month=2026-06&q=告警&limit=5",
                    headers=headers,
                )
                repair_prefill = client.get(
                    "/api/repair-management/event-prefill?scope=E&month=2026-06&record_id=rec-event",
                    headers=headers,
                )
                repair_candidates = client.get(
                    "/api/repair-management/repair-candidates?scope=E&month=2026-06&event_record_id=rec-event&q=压缩机&limit=6",
                    headers=headers,
                )
                cmdb_candidates = client.get(
                    "/api/repair-management/cmdb-candidates?scope=E&q=CRAH&limit=8",
                    headers=headers,
                )
                followup_people = client.get(
                    "/api/repair-management/people?scope=E&q=张宇&limit=7",
                    headers=headers,
                )
                followups = client.get(
                    "/api/repair-management/followups?scope=E&summary_record_id=rec-summary&q=进展&limit=9",
                    headers=headers,
                )
                followup_created = client.post(
                    "/api/repair-management/followups",
                    headers=headers,
                    json={
                        "scope": "E",
                        "summary_record_id": "rec-summary",
                        "cmdb_record_ids": ["rec-cmdb-1", "rec-cmdb-2"],
                        "fields": {"维修进展描述": "处理中"},
                    },
                )
                followup_updated = client.put(
                    "/api/repair-management/followups/rec-followup",
                    headers=headers,
                    json={
                        "scope": "E",
                        "summary_record_id": "rec-summary",
                        "cmdb_record_ids": ["rec-cmdb-1", "rec-cmdb-2"],
                        "fields": {"维修进度": 1},
                    },
                )
                followup_deleted = client.delete(
                    "/api/repair-management/followups/rec-followup?scope=E&summary_record_id=rec-summary",
                    headers=headers,
                )
                combined_prefill = client.post(
                    "/api/repair-management/prefill",
                    headers=headers,
                    json={
                        "scope": "E",
                        "source_event_id": "rec-event",
                        "source_repair_ids": ["rec-repair"],
                        "source_month": "2026-06",
                    },
                )
                transferred = client.post(
                    "/api/events/transfer-repair",
                    headers=headers,
                    json={"scope": "E", "record_id": "rec-event", "month": "2026-06"},
                )

            self.assertEqual(unauth.status_code, 401)
            self.assertEqual(denied.status_code, 403)
            for response in [
                listed,
                repair_status,
                created,
                updated,
                deleted,
                repair_events,
                repair_prefill,
                repair_candidates,
                cmdb_candidates,
                followup_people,
                followups,
                followup_created,
                followup_updated,
                followup_deleted,
                combined_prefill,
                transferred,
            ]:
                self.assertEqual(response.status_code, 200, response.text)
                self.assertTrue(response.json().get("ok"), response.text)
            self.assertEqual(
                service.calls,
                [
                    ("list_repair", "E", "冷站", 3, "rec-focus"),
                    (
                        "repair_status",
                        "E",
                        "冷站",
                        "in_progress",
                        "all",
                        12,
                        3,
                        True,
                    ),
                    (
                        "create_repair",
                        "E",
                        {"维修名称": "测试检修"},
                        "rec-event",
                        ("rec-repair",),
                        "2026-06",
                    ),
                    (
                        "update_repair",
                        "E",
                        "rec-1",
                        {"专业": "电气"},
                        "rec-event",
                        ("rec-repair",),
                        True,
                        "2026-06",
                    ),
                    ("delete_repair", "E", "rec-1"),
                    ("repair_events", "E", "2026-06", "告警", 5),
                    ("repair_prefill", "E", "rec-event", "2026-06"),
                    ("repair_candidates", "E", "rec-event", "2026-06", "压缩机", 6),
                    ("cmdb_candidates", "E", "CRAH", 8),
                    ("followup_people", "E", "张宇", 7, False),
                    ("list_followups", "E", "rec-summary", "进展", 9),
                    (
                        "create_followup",
                        "E",
                        "rec-summary",
                        ("rec-cmdb-1", "rec-cmdb-2"),
                        {"维修进展描述": "处理中"},
                    ),
                    (
                        "update_followup",
                        "E",
                        "rec-followup",
                        "rec-summary",
                        ("rec-cmdb-1", "rec-cmdb-2"),
                        {"维修进度": 1},
                    ),
                    ("delete_followup", "E", "rec-followup", "rec-summary"),
                    ("combined_prefill", "E", "rec-event", ("rec-repair",), "2026-06"),
                    ("transfer_repair", "rec-event", "2026-06"),
                ],
            )
            self.assertTrue(transferred.json()["data"]["transfer_to_overhaul"])
        finally:
            PortalRuntime.service = original_service
            with PortalRuntime.auth_manager._lock:
                PortalRuntime.auth_manager._sessions = original_sessions

    def test_workbench_action_keeps_extra_fields_for_compatibility(self):
        model = parse_api_model(
            WorkbenchActionRequest,
            {
                "scope": "A",
                "action": "start",
                "record_id": "rec-1",
                "custom_field": "kept",
            },
        )
        payload = model.to_payload()
        self.assertEqual(payload["scope"], "A")
        self.assertEqual(payload["record_id"], "rec-1")
        self.assertEqual(payload["custom_field"], "kept")

    def test_permission_request_requires_scope_list_shape(self):
        with self.assertRaises(ValueError):
            parse_api_model(PermissionRequestCreate, {"scopes": "A"})

    def test_permission_review_models_keep_safe_defaults(self):
        review = parse_api_model(PermissionRequestReviewRequest, {}).to_payload()
        bulk = parse_api_model(PermissionRequestBulkReviewRequest, {}).to_payload()
        self.assertEqual(review["scopes"], [])
        self.assertEqual(review["reason"], "")
        self.assertEqual(bulk["request_ids"], [])
        self.assertEqual(bulk["scopes_by_request_id"], {})

    def test_ongoing_delete_defaults_are_stable(self):
        payload = parse_api_model(OngoingDeleteRequest, {}).to_payload()
        self.assertEqual(payload["scope"], "ALL")
        self.assertEqual(payload["work_type"], "maintenance")

    def test_job_mark_stuck_failed_default_reason_is_stable(self):
        payload = parse_api_model(JobMarkStuckFailedRequest, {}).to_payload()
        self.assertIn("卡住任务", payload["reason"])

    def test_qt_command_payload_must_be_object(self):
        with self.assertRaises(ValueError):
            parse_api_model(QtCommandRequest, {"command": "x", "payload": "bad"})

    def test_qt_local_models_keep_safe_defaults(self):
        dialog = parse_api_model(QtDialogSessionRequest, {}).to_payload()
        clipboard_ack = parse_api_model(QtClipboardAckRequest, {}).to_payload()
        event_ack = parse_api_model(QtEventAckRequest, {}).to_payload()
        self.assertEqual(dialog["payload"], {})
        self.assertTrue(clipboard_ack["ok"])
        self.assertTrue(event_ack["ok"])

    def test_qt_delta_requires_list_shapes(self):
        with self.assertRaises(ValueError):
            parse_api_model(QtActiveItemsDeltaRequest, {"upserts": {"bad": True}})

    def test_qt_progress_keeps_extra_fields_for_bridge_compatibility(self):
        payload = parse_api_model(
            QtJobProgressRequest,
            {"phase": "uploading", "custom": "kept"},
        ).to_payload()
        self.assertEqual(payload["phase"], "uploading")
        self.assertEqual(payload["custom"], "kept")

    def test_qt_notice_upload_command_is_pass_through(self):
        payload = parse_api_model(
            QtCommandRequest,
            {
                "command": "notice_upload",
                "payload": {"data_dict": {"record_id": "rid-1"}, "action_type": "upload"},
            },
        ).to_payload()
        self.assertEqual(payload["command"], "notice_upload")
        self.assertEqual(payload["payload"]["data_dict"]["record_id"], "rid-1")
        self.assertEqual(payload["payload"]["action_type"], "upload")

    def test_inline_image_payload_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "upload_id"):
            FastAPIPortalController._reject_large_inline_images(
                {"extra_images": [{"bytes_b64": "a" * (260 * 1024)}]}
            )
        with self.assertRaisesRegex(ValueError, "base64"):
            FastAPIPortalController._reject_large_inline_images(
                {"extra_images": [{"upload_id": "up-1", "bytes_b64": "small"}]}
            )
        FastAPIPortalController._reject_large_inline_images({"extra_images": [{"upload_id": "up-1"}]})

    def test_notice_upload_attachment_stats(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.put_notice_upload_attachment(
                open_id="ou-test",
                file_name="site.png",
                mime_type="image/png",
                content=b"12345",
            )
            stats = store.notice_upload_attachment_stats()
            self.assertEqual(stats["total"], 1)
            self.assertEqual(stats["pending"], 1)
            self.assertEqual(stats["total_bytes"], 5)

    def test_notice_upload_attachment_capacity_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
            store.put_notice_upload_attachment(content=b"12345", max_pending_bytes=10)
            with self.assertRaisesRegex(ValueError, "暂存空间已满"):
                store.put_notice_upload_attachment(content=b"678901", max_pending_bytes=10)

    def test_batch_job_visibility_uses_auth_open_id(self):
        session = {"user": {"open_id": "ou-allowed"}, "role": "user", "is_admin": False}
        visible_job = {"request": {"_auth_open_id": "ou-allowed"}}
        denied_job = {"request": {"_auth_open_id": "ou-other"}}
        self.assertTrue(FastAPIPortalController._job_visible_to_session(visible_job, session))
        self.assertFalse(FastAPIPortalController._job_visible_to_session(denied_job, session))


if __name__ == "__main__":
    unittest.main()

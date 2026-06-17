import os
import sys
import tempfile
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    DEFAULT_APP_TOKEN,
    DEFAULT_TABLE_ID,
    MOP_ENGINEER_CONFIRM_FIELD,
    MOP_SIGNED_ATTACHMENT_FIELD,
    MOP_SUPERVISOR_CONFIRM_FIELD,
    MaintenancePortalService,
    PortalError,
    WORK_TYPE_MAINTENANCE,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class FakeMopUploadService(MaintenancePortalService):
    def __init__(self, tmpdir: str, *, missing_auditor_open_id: bool = False):
        super().__init__()
        self.tmpdir = Path(tmpdir)
        self.missing_auditor_open_id = missing_auditor_open_id
        self.upload_calls = []
        self.patch_calls = []
        self.external_image_calls = []
        self._state_store = LanPortalStateStore(self.tmpdir / "state.sqlite3")

    def ensure_snapshot_loaded(self, *, refresh_if_expired: bool = False) -> None:
        return None

    def _find_record_by_id(self, record_id: str, work_type: str) -> dict:
        assert record_id == "src-1"
        assert work_type == WORK_TYPE_MAINTENANCE
        return {
            "record_id": "src-1",
            "display_fields": {
                "楼栋": "A楼",
                "维护周期": "每月",
                "维护总项": "测试维保",
            },
        }

    def _load_signature_people(self, *, force: bool = False) -> list[dict]:
        return [
            {
                "record_id": "person-1",
                "name": "实施人",
                "open_id": "ou_impl",
                "has_signature": True,
            },
            {
                "record_id": "person-2",
                "name": "审核人",
                "open_id": "" if self.missing_auditor_open_id else "ou_audit",
                "has_signature": True,
            },
        ]

    def _load_engineer_mop_candidates(self, *, force: bool = False):
        return [], [], {
            "app_token": "mop-app",
            "table_id": "mop-table",
            "view_id": "",
            "title_field": "文件名",
            "attachment_field": "文件",
        }

    def external_signature_image_bytes(self, *, record_id: str):
        self.external_image_calls.append(record_id)
        if record_id != "external-rec-1":
            raise PortalError("其他人员签名不存在")
        return b"fake png", "image/png"

    def fill_engineer_mop_file(self, **kwargs) -> dict:
        output = self.tmpdir / "测试_已签名.xlsx"
        output.write_bytes(b"fake signed mop")
        return {
            "path": str(output),
            "file_name": output.name,
            "relative_path": output.name,
        }

    def _upload_bitable_file(self, **kwargs) -> str:
        self.upload_calls.append(dict(kwargs))
        return "file-token-1"

    def _patch_record_fields(self, **kwargs) -> dict:
        self.patch_calls.append(dict(kwargs))
        return {"ok": True}


class EngineerMopUploadTests(unittest.TestCase):
    def setUp(self):
        self._old_mock = os.environ.get("CLIPFLOW_BACKEND_MOCK_EXTERNAL")
        os.environ["CLIPFLOW_BACKEND_MOCK_EXTERNAL"] = "1"

    def tearDown(self):
        if self._old_mock is None:
            os.environ.pop("CLIPFLOW_BACKEND_MOCK_EXTERNAL", None)
        else:
            os.environ["CLIPFLOW_BACKEND_MOCK_EXTERNAL"] = self._old_mock

    def _signatures(self) -> list[dict]:
        return [
            {"role": "implementer", "record_id": "person-1"},
            {"role": "auditor", "record_id": "person-2"},
        ]

    def test_upload_signed_mop_overwrites_attachment_and_confirms(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FakeMopUploadService(tmpdir)
            result = service.upload_signed_engineer_mop_file(
                scope="A",
                source_record_id="src-1",
                notice_title="A楼测试维保",
                operator_open_id="ou_operator",
                operator_name="操作人",
                local_file_path=str(Path(tmpdir) / "source.xlsx"),
                mop_record_id="mop-1",
                mop_title="MOP",
                sheet_name="Sheet1",
                signatures=self._signatures(),
            )

            self.assertEqual(result["file_token"], "file-token-1")
            self.assertEqual(result["notification_warning"], "")
            self.assertEqual(len(service.upload_calls), 1)
            self.assertEqual(len(service.patch_calls), 1)
            patch = service.patch_calls[0]
            self.assertEqual(patch["app_token"], DEFAULT_APP_TOKEN)
            self.assertEqual(patch["table_id"], DEFAULT_TABLE_ID)
            self.assertEqual(patch["record_id"], "src-1")
            self.assertEqual(
                patch["fields"][MOP_SIGNED_ATTACHMENT_FIELD],
                [{"file_token": "file-token-1"}],
            )
            self.assertIs(patch["fields"][MOP_ENGINEER_CONFIRM_FIELD], True)
            self.assertIs(patch["fields"][MOP_SUPERVISOR_CONFIRM_FIELD], True)

    def test_missing_required_signature_blocks_upload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FakeMopUploadService(tmpdir)
            with self.assertRaises(PortalError):
                service.upload_signed_engineer_mop_file(
                    scope="A",
                    source_record_id="src-1",
                    local_file_path=str(Path(tmpdir) / "source.xlsx"),
                    signatures=[{"role": "implementer", "record_id": "person-1"}],
                )
            self.assertEqual(service.upload_calls, [])
            self.assertEqual(service.patch_calls, [])

    def test_notification_failure_does_not_block_upload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FakeMopUploadService(tmpdir, missing_auditor_open_id=True)
            result = service.upload_signed_engineer_mop_file(
                scope="A",
                source_record_id="src-1",
                notice_title="A楼测试维保",
                local_file_path=str(Path(tmpdir) / "source.xlsx"),
                signatures=self._signatures(),
            )

            self.assertEqual(result["file_token"], "file-token-1")
            self.assertIn("签名人员通知失败", result["notification_warning"])
            self.assertEqual(len(service.patch_calls), 1)

    def test_temporary_signature_counts_for_required_roles_without_notification(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FakeMopUploadService(tmpdir)
            session = service._state_store.create_mop_temporary_signature_session(
                scope="A",
                notice_key="notice-1",
                role="implementer",
                display_name="临时人员1",
                recipient_open_ids=["ou_impl"],
                payload={"notice_title": "A楼测试维保", "specialty": "电气"},
            )
            service._state_store.update_mop_temporary_signature_session(
                temp_id=session["temp_id"],
                status="signed",
                temporary_record_id="temp-rec-1",
                signature_file_token="temp-file-token",
            )

            result = service.upload_signed_engineer_mop_file(
                scope="A",
                source_record_id="src-1",
                notice_title="A楼测试维保",
                local_file_path=str(Path(tmpdir) / "source.xlsx"),
                signatures=[
                    {
                        "source": "temporary",
                        "role": "implementer",
                        "temp_id": session["temp_id"],
                        "record_id": "temp-rec-1",
                    },
                    {"source": "staff", "role": "auditor", "record_id": "person-2"},
                ],
            )

            self.assertEqual(result["file_token"], "file-token-1")
            self.assertEqual(result["notification_warning"], "")
            self.assertEqual(len(result["notification_results"]), 1)
            self.assertEqual(result["notification_results"][0]["open_id"], "ou_audit")

    def test_external_signature_counts_for_required_roles_without_notification(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FakeMopUploadService(tmpdir)
            result = service.upload_signed_engineer_mop_file(
                scope="A",
                source_record_id="src-1",
                notice_title="A楼测试维保",
                local_file_path=str(Path(tmpdir) / "source.xlsx"),
                signatures=[
                    {
                        "source": "external",
                        "role": "implementer",
                        "record_id": "external-rec-1",
                    },
                    {"source": "staff", "role": "auditor", "record_id": "person-2"},
                ],
            )

            self.assertEqual(result["file_token"], "file-token-1")
            self.assertEqual(result["notification_warning"], "")
            self.assertEqual(service.external_image_calls, ["external-rec-1"])
            self.assertEqual(len(result["notification_results"]), 1)
            self.assertEqual(result["notification_results"][0]["open_id"], "ou_audit")

    def test_temporary_signature_link_uses_custom_display_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FakeMopUploadService(tmpdir)
            data = service.build_temporary_signature_link_message(
                scope="A",
                notice_key="notice-1",
                role="implementer",
                recipient_open_ids=["ou_impl"],
                notice_title="A楼测试维保",
                display_name="张三",
                request_base_url="http://192.168.224.130:18766",
            )

            self.assertEqual(data["signature"]["display_name"], "张三")
            self.assertIn("临时人员：张三", data["text"])

    def test_signature_link_prefers_current_page_origin(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FakeMopUploadService(tmpdir)
            service.get_handover_links = lambda: {"links": {"A": "http://10.0.0.8:18766/audit"}}

            base_url = service._signature_public_base_url(
                scope="A",
                request_base_url="http://192.168.224.130:18766",
            )

            self.assertEqual(base_url, "http://192.168.224.130:18766")

    def test_engineer_mop_bootstrap_includes_current_month_source_records_first(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = FakeMopUploadService(tmpdir)
            service._records = [
                {
                    "record_id": "src-bound",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "notice_type": "维保通告",
                    "display_fields": {
                        "楼栋": "A楼",
                        "计划维护月份": service._current_month_label(),
                        "维护周期": "每月",
                        "维护总项": "已绑定已上传",
                        MOP_SIGNED_ATTACHMENT_FIELD: [{"file_token": "file-1", "name": "mop.xlsx"}],
                        MOP_ENGINEER_CONFIRM_FIELD: True,
                        MOP_SUPERVISOR_CONFIRM_FIELD: True,
                    },
                },
                {
                    "record_id": "src-ended",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "notice_type": "维保通告",
                    "display_fields": {
                        "楼栋": "A楼",
                        "计划维护月份": service._current_month_label(),
                        "维护周期": "每月",
                        "维护总项": "正常结束未绑定未上传",
                        "维护实施状态": "已结束",
                    },
                },
                {
                    "record_id": "src-pending",
                    "work_type": WORK_TYPE_MAINTENANCE,
                    "notice_type": "维保通告",
                    "display_fields": {
                        "楼栋": "A楼",
                        "计划维护月份": service._current_month_label(),
                        "维护周期": "每月",
                        "维护总项": "未绑定未上传",
                    },
                },
            ]
            pending_notice = service._serialize_engineer_source_maintenance_notice(service._records[2])
            assert pending_notice is not None
            service._state_store.upsert_mop_notice_binding(
                {
                    "scope": "A",
                    "notice_key": service._serialize_engineer_source_maintenance_notice(service._records[0])["notice_key"],
                    "template_key": service._serialize_engineer_source_maintenance_notice(service._records[0])["mop_template_key"],
                    "notice_title": "已绑定已上传",
                    "mop_record_id": "mop-1",
                    "mop_title": "MOP",
                }
            )

            data = service.engineer_mop_bootstrap(scope="A", ongoing_items=[])
            notices = data["notices"]

            self.assertGreaterEqual(len(notices), 2)
            self.assertEqual(notices[0]["source_record_id"], "src-ended")
            self.assertEqual(notices[0]["status"], "已结束")
            self.assertFalse(notices[0]["mop_uploaded"])
            self.assertEqual(notices[1]["source_record_id"], "src-pending")
            uploaded = next(item for item in notices if item["source_record_id"] == "src-bound")
            self.assertTrue(uploaded["mop_uploaded"])
            self.assertEqual(uploaded["mop_attachment_count"], 1)
            self.assertTrue(uploaded["mop_binding"])


if __name__ == "__main__":
    unittest.main()

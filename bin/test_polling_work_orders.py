# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

from lan_bitable_template_portal.local_notice_images import LocalNoticeImageStore
from lan_bitable_template_portal.polling_work_orders import (
    PollingWorkOrderService,
    PollingWorkOrderTokenError,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore
import lan_bitable_template_portal.server as portal_server
from lan_bitable_template_portal.server import PortalRuntime
from lan_bitable_template_portal.workbench_lite import render_workbench_lite
from upload_event_module.services.handlers.base import NoticePayload
from upload_event_module.services.handlers.polling_notice import PollingNoticeHandler


class PollingWorkOrderTests(unittest.TestCase):
    def test_polling_sop_button_is_in_notice_panel_and_uses_floating_editor(self) -> None:
        html = render_workbench_lite(
            payload={"records": [], "ongoing": []},
            session={"role": "admin"},
            scope="A",
            work_type="polling",
        )
        self.assertIn(
            '<h2 class="inbox-title"><span>通告处理</span><button class="btn ghost" id="lite-polling-sop-open"',
            html,
        )
        self.assertIn(
            'class="end-check-backdrop" id="lite-polling-sop-modal" hidden',
            html,
        )
        self.assertIn("/api/polling-sops?scope=", html)

    def test_polling_end_is_blocked_before_write_until_work_order_attachment_exists(self) -> None:
        prepared = {
            "action": "end",
            "work_type": "polling",
            "notice_type": "设备轮巡",
            "record_id": "recTarget1",
            "target_record_id": "recTarget1",
            "text": "【设备轮巡】状态：结束\n【标题】轮巡测试",
        }
        with patch.object(
            portal_server,
            "external_real_write_guard",
            return_value={"mock_external": False, "real_write_allowed": True, "reason": ""},
        ), patch.object(
            portal_server,
            "query_record_by_id",
            return_value=(
                True,
                {
                    "fields": {
                        "是否涉及重要操作": True,
                        "操作人": "操作员",
                        "现场复核人": "审核员",
                        "工单附件": [],
                    }
                },
            ),
        ):
            ok, message, _record_id = PortalRuntime._execute_backend_prepared_upload(
                prepared
            )
        self.assertFalse(ok)
        self.assertIn("尚未全部完成", message)

    def test_polling_start_writes_work_order_fields_but_not_h_confirmation(self) -> None:
        fields = PollingNoticeHandler().build_create_fields(
            NoticePayload(
                text=(
                    "【设备轮巡】状态：开始\n"
                    "【标题】轮巡测试\n【时间】2026-08-24 09:00~2026-08-24 18:00"
                ),
                polling_work_order_required=True,
                polling_operator_name="操作员",
                polling_reviewer_name="审核员",
            )
        )
        self.assertTrue(fields["是否涉及重要操作"])
        self.assertEqual(fields["操作人"], "操作员")
        self.assertEqual(fields["现场复核人"], "审核员")
        self.assertNotIn("重要操作H楼确认", fields)

    def test_sop_snapshot_and_serial_role_confirmations(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = LanPortalStateStore(root / "state.sqlite3")
            service = PollingWorkOrderService(store)
            service.sop_root = root / "sops"
            service.work_order_root = root / "orders"
            sop = service.save_sop(
                {
                    "scope": "A",
                    "name": "制冷单元切换",
                    "steps": [
                        {"content": "将{{from}}切换至{{to}}", "operator_required": True, "reviewer_required": True},
                        {"content": "复查{{to}}", "operator_required": False, "reviewer_required": True},
                    ],
                }
            )
            sop = service.add_sop_attachment(
                sop["sop_id"],
                file_name="原始SOP.txt",
                content=b"sop",
                expected_version=sop["version"],
            )
            prepared = service.prepare_start(
                {
                    "work_type": "polling",
                    "scope": "A",
                    "action": "start",
                    "_web_action_request": True,
                    "polling_sop_id": sop["sop_id"],
                    "polling_sop_version": sop["version"],
                    "polling_run_count": 1,
                    "polling_runs": [{"from_unit": "2#", "to_unit": "3#"}],
                    "polling_operator_record_id": "operator",
                    "polling_reviewer_record_id": "reviewer",
                },
                job_id="job-1",
                people=[
                    {"record_id": "operator", "name": "操作员", "open_id": "ou_operator"},
                    {"record_id": "reviewer", "name": "审核员", "open_id": "ou_reviewer"},
                ],
            )
            with self.assertRaisesRegex(Exception, "不属于当前楼栋"):
                service.prepare_start(
                    {
                        "work_type": "polling",
                        "scope": "B",
                        "action": "start",
                        "_web_action_request": True,
                        "polling_sop_id": sop["sop_id"],
                        "polling_sop_version": sop["version"],
                    },
                    job_id="job-wrong-building",
                    people=[],
                )
            group = service.create_group(
                prepared,
                target_record_id="recTarget1",
                title="轮巡测试",
                public_base_url="http://127.0.0.1:18766",
            )
            self.assertEqual(group["steps"][0]["content"], "将2#切换至3#")
            operator_token = service.role_token("recTarget1", "operator")
            reviewer_token = service.role_token("recTarget1", "reviewer")
            with self.assertRaises(Exception):
                service.confirm(reviewer_token, step_key="1:1", expected_version=1)
            session = service.confirm(operator_token, step_key="1:1", expected_version=1)
            session = service.confirm(reviewer_token, step_key="1:1", expected_version=session["version"])
            self.assertEqual(session["current_index"], 1)
            session = service.confirm(reviewer_token, step_key="1:2", expected_version=session["version"])
            self.assertEqual(session["state"], "upload_pending")
            service.mark_upload_result("recTarget1", success=True, file_tokens=["file-token"])
            with self.assertRaises(PollingWorkOrderTokenError):
                service.session(operator_token)

            other_building = service.save_sop(
                {
                    "scope": "B",
                    "name": "制冷单元切换",
                    "steps": [
                        {"content": "B楼步骤", "operator_required": True, "reviewer_required": False}
                    ],
                }
            )
            self.assertEqual([item["sop_id"] for item in service.list_sops("A")], [sop["sop_id"]])
            self.assertEqual([item["sop_id"] for item in service.list_sops("B")], [other_building["sop_id"]])

    def test_local_notice_image_survives_store_reload(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = LanPortalStateStore(root / "state.sqlite3")
            images = LocalNoticeImageStore(store)
            images.root = root / "images"
            saved = images.save(
                identity="change:rec1",
                kind="ali",
                content=b"png-bytes",
                file_name="confirm.png",
                mime_type="image/png",
                scope="A",
            )
            images.mark_feishu_uploaded(
                saved["local_image_id"], file_token="file-token", target_written=True
            )
            reloaded = LocalNoticeImageStore(store)
            reloaded.root = root / "images"
            item = reloaded.list("change:rec1", scope="A")[0]
            self.assertEqual(item["preview_url"], f"/api/notice-images/{saved['local_image_id']}")
            self.assertTrue(item["target_written"])
            self.assertNotIn("file_token", item)
            self.assertNotIn("feishu_file_token", item)
            self.assertEqual(reloaded.content(saved["local_image_id"])[0], b"png-bytes")


if __name__ == "__main__":
    unittest.main()

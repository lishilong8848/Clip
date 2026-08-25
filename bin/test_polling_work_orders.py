# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

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
from lan_bitable_template_portal.workbench_lite import (
    render_polling_work_order_page,
    render_workbench_lite,
)
from upload_event_module.services.handlers.base import NoticePayload
from upload_event_module.services.handlers.polling_notice import PollingNoticeHandler


class PollingWorkOrderTests(unittest.TestCase):
    def test_work_order_page_uses_global_step_number_without_fake_run_count(self) -> None:
        html = render_polling_work_order_page()

        self.assertIn("第 ${Number(step.global_index||0)+1} 步", html)
        self.assertIn(
            "已完成 ${Math.min(data.current_index,data.total_steps)} 步",
            html,
        )
        self.assertNotIn("工单 ${step.run_index}/${step.run_count}", html)
        self.assertNotIn("步骤 ${step.step_index}/${step.step_count}", html)

    def test_notice_content_prefills_polling_runs(self) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("node is unavailable")
        html = render_workbench_lite(
            payload={"records": [], "ongoing": []},
            session={"role": "admin"},
            scope="A",
            work_type="polling",
        )
        functions = []
        for name in (
            "pollingShiftFromHour",
            "pollingUnitGroup",
            "pollingUnitNumbersFromContent",
            "pollingRunsFromContent",
        ):
            match = re.search(rf"function {name}\(.*?\n    \}}", html, re.S)
            self.assertIsNotNone(match, name)
            functions.append(match.group(0))
        script = "\n".join(
            [
                "const litePollingUnits=['1#','2#','3#','4#','5#','6#'];",
                *functions,
                "console.log(JSON.stringify([pollingRunsFromContent('起点1终点3'),pollingRunsFromContent('1 3 4 6'),pollingRunsFromContent('1# 4号 3 6'),pollingRunsFromContent('1 4 6 3'),[8,9,17,18,23,0].map(pollingShiftFromHour)]));",
            ]
        )
        result = subprocess.run(
            [node, "-e", script], check=True, capture_output=True, text=True
        )
        self.assertEqual(
            json.loads(result.stdout),
            [
                [{"from_unit": "1#", "to_unit": "3#"}],
                [
                    {"from_unit": "1#", "to_unit": "3#"},
                    {"from_unit": "4#", "to_unit": "6#"},
                ],
                [
                    {"from_unit": "1#", "to_unit": "3#"},
                    {"from_unit": "4#", "to_unit": "6#"},
                ],
                [
                    {"from_unit": "1#", "to_unit": "3#"},
                    {"from_unit": "4#", "to_unit": "6#"},
                ],
                ["夜", "白", "白", "夜", "夜", "夜"],
            ],
        )

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
        self.assertIn("插入其他点", html)
        self.assertIn("['插入起点','{{from}}']", html)
        self.assertIn("['插入终点','{{to}}']", html)
        self.assertIn("['插入其他点','{{other}}']", html)
        self.assertIn("可先配置设备指向", html)
        self.assertIn("设备指向（1#–3# / 4#–6# 组内选择）", html)
        self.assertIn("第${index+1}次起点", html)
        self.assertIn("SOP 必须至少包含一个附件", html)
        self.assertIn('id="lite-polling-sop-feedback"', html)
        self.assertIn('id="lite-polling-sop-delete-confirm"', html)
        self.assertIn("确认删除这份 SOP", html)
        self.assertIn("已经生成的工单不受影响", html)
        self.assertIn("openPollingSopDeleteConfirm(sop,del)", html)
        self.assertIn("setPollingSopFeedback(`已保存：${saved.name}`)", html)
        self.assertNotIn("confirm('确认删除该 SOP", html)
        self.assertIn("本地附件（必填）", html)
        self.assertIn("点击选择或将附件拖到这里", html)
        self.assertIn("drop.ondrop", html)
        self.assertIn("function pollingBindPersonSearch", html)
        self.assertIn("()=>[reviewer.dataset.recordId,'h_duty_account']", html)
        self.assertIn("()=>[operator.dataset.recordId]", html)
        self.assertIn("搜索姓名、工号、岗位或楼栋", html)
        self.assertIn("快捷选择H楼值班账号", html)
        self.assertNotIn("快捷筛选 H楼人员", html)
        self.assertIn("h_duty_account", html)
        self.assertIn("后续工单不能再选择前面已使用的设备编号", html)
        self.assertIn("SOP 已加载，正在读取人员", html)
        self.assertIn("人员加载失败", html)
        self.assertIn("SOP 加载失败", html)
        self.assertIn("renderPollingSopList();", html)
        self.assertIn("retry.onclick=()=>openPollingSopModal(mode)", html)
        self.assertNotIn("showLiteError(error.message);closePollingSopModal()", html)
        self.assertIn("pollingUnitGroup(run.from_unit).includes(run.to_unit)", html)
        self.assertIn("run.from_unit}→${run.to_unit", html)
        open_source = html.split("async function openPollingSopModal", 1)[1].split(
            "function currentUrlScope", 1
        )[0]
        self.assertLess(
            open_source.index("await loadPollingSops()"),
            open_source.index("renderPollingSopList()"),
        )
        self.assertLess(
            open_source.index("renderPollingSopList()"),
            open_source.index("await loadPollingPeople()"),
        )

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

    def test_work_order_message_falls_back_to_current_account(self) -> None:
        manager = MagicMock()
        manager.group_with_links.return_value = {
            "target_record_id": "recTarget1",
            "title": "轮巡测试",
            "sop_name": "切换 SOP",
            "runs": [{"from_unit": "1#", "to_unit": "2#"}],
            "operator": {"name": "操作员", "open_id": "ou_unreachable"},
            "reviewer": {"name": "审核员", "open_id": "ou_reviewer"},
            "initiator_open_id": "ou_current",
            "operator_link": "http://127.0.0.1/operator",
            "reviewer_link": "http://127.0.0.1/reviewer",
        }
        manager.update_notifications.side_effect = lambda _record_id, data: data
        recipients = []

        def send(_text, open_ids):
            recipients.append(open_ids)
            return open_ids != ["ou_unreachable"], "ok", []

        with patch.object(
            PortalRuntime, "polling_work_orders", return_value=manager
        ), patch.object(portal_server, "_send_text_to_open_ids_guarded", side_effect=send):
            notifications = PortalRuntime._send_polling_work_order_links({})

        self.assertEqual(recipients[:2], [["ou_unreachable"], ["ou_current"]])
        self.assertTrue(notifications["operator"]["sent"])
        self.assertTrue(notifications["operator"]["fallback"])
        self.assertEqual(
            notifications["operator"]["recipient_open_id"], "ou_current"
        )

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
                        {"content": "将{{from}}切换至{{to}}，检查{{other}}", "operator_required": True, "reviewer_required": True},
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
            prior_version = sop["version"]
            sop = service.save_sop(
                {
                    "sop_id": sop["sop_id"],
                    "scope": "A",
                    "name": "制冷单元切换",
                    "expected_version": prior_version,
                    "steps": [
                        {
                            "content": "将{{from}}切换至{{to}}，检查{{other}}",
                            "operator_required": True,
                            "reviewer_required": True,
                        },
                        {
                            "content": "修改后复查{{to}}",
                            "operator_required": False,
                            "reviewer_required": True,
                        },
                    ],
                }
            )
            self.assertEqual(sop["version"], prior_version + 1)
            self.assertEqual(sop["steps"][1]["content"], "修改后复查{{to}}")
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
            with self.assertRaisesRegex(Exception, "不能跨越"):
                service.prepare_start(
                    {
                        "work_type": "polling",
                        "scope": "A",
                        "action": "start",
                        "_web_action_request": True,
                        "polling_sop_id": sop["sop_id"],
                        "polling_sop_version": sop["version"],
                        "polling_run_count": 1,
                        "polling_runs": [{"from_unit": "1#", "to_unit": "5#"}],
                    },
                    job_id="job-cross-group",
                    people=[],
                )
            with self.assertRaisesRegex(Exception, "已选择的设备编号"):
                service.prepare_start(
                    {
                        "work_type": "polling",
                        "scope": "A",
                        "action": "start",
                        "_web_action_request": True,
                        "polling_sop_id": sop["sop_id"],
                        "polling_sop_version": sop["version"],
                        "polling_run_count": 2,
                        "polling_runs": [
                            {"from_unit": "1#", "to_unit": "2#"},
                            {"from_unit": "2#", "to_unit": "3#"},
                        ],
                    },
                    job_id="job-reused-unit",
                    people=[],
                )
            h_duty = service._person_by_id(
                [], "h_duty_account", allow_h_duty=True
            )
            self.assertEqual(h_duty["name"], "H楼值班账号")
            self.assertEqual(h_duty["building"], "H楼")
            with self.assertRaisesRegex(Exception, "不能是同一人"):
                service.prepare_start(
                    {
                        "work_type": "polling",
                        "scope": "A",
                        "action": "start",
                        "_web_action_request": True,
                        "polling_sop_id": sop["sop_id"],
                        "polling_sop_version": sop["version"],
                        "polling_run_count": 1,
                        "polling_runs": [{"from_unit": "1#", "to_unit": "2#"}],
                        "polling_operator_record_id": "same-person",
                        "polling_reviewer_record_id": "same-person",
                    },
                    job_id="job-same-person",
                    people=[
                        {
                            "record_id": "same-person",
                            "name": "同一人员",
                            "open_id": "ou_same",
                        }
                    ],
                )
            group = service.create_group(
                prepared,
                target_record_id="recTarget1",
                title="轮巡测试",
                public_base_url="http://127.0.0.1:18766",
            )
            self.assertEqual(group["steps"][0]["content"], "将2#切换至3#，检查1#")
            self.assertEqual(group["steps"][1]["content"], "修改后复查3#")
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
            self.assertEqual(service.session(operator_token)["state"], "completed")
            self.assertEqual(service.open_groups()[0]["state"], "completed")
            service.cancel_group("recTarget1", reason="target_terminal")
            with self.assertRaises(PollingWorkOrderTokenError):
                service.session(operator_token)

            with self.assertRaisesRegex(Exception, "必须使用"):
                service.save_sop(
                    {
                        "scope": "A",
                        "name": "旧占位符",
                        "steps": [
                            {
                                "content": "将{from}切换至{to}",
                                "operator_required": True,
                            }
                        ],
                    }
                )

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

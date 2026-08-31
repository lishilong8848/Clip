# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile
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
from clipflow_backend.main import FastAPIPortalController
from fastapi.testclient import TestClient
from lan_bitable_template_portal.workbench_lite import (
    render_polling_work_order_page,
    render_polling_work_order_steps_page,
    render_workbench_lite,
)
from upload_event_module.services.handlers.base import NoticePayload
from upload_event_module.services.handlers.polling_notice import PollingNoticeHandler


def _png_bytes(color: str = "#1678ff", size: tuple[int, int] = (160, 100)) -> bytes:
    from PIL import Image

    output = io.BytesIO()
    Image.new("RGB", size, color).save(output, format="PNG")
    return output.getvalue()


class PollingWorkOrderTests(unittest.TestCase):
    def test_polling_step_photo_rejects_oversized_dimensions_before_storage(self) -> None:
        service = object.__new__(PollingWorkOrderService)
        with self.assertRaisesRegex(Exception, "像素或尺寸超过限制"):
            service.add_step_photo(
                "unused-token",
                step_key="1:1",
                expected_version=1,
                file_name="too-wide.png",
                mime_type="image/png",
                content=_png_bytes(size=(12001, 1)),
            )

    def test_polling_sop_rejects_more_than_thirty_steps(self) -> None:
        with self.assertRaisesRegex(Exception, "SOP 步骤不能超过 30 条"):
            PollingWorkOrderService._normalized_steps(
                [
                    {
                        "content": f"步骤{index + 1}",
                        "operator_required": True,
                    }
                    for index in range(31)
                ]
            )

    def test_attachment_result_keeps_unsaved_sop_name_and_steps(self) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("node is unavailable")
        html = render_workbench_lite(
            payload={"records": [], "ongoing": []},
            session={"role": "admin"},
            scope="A",
            work_type="polling",
        )
        match = re.search(
            r"function pollingMergeSopAttachmentResult\(.*?\n    \}",
            html,
            re.S,
        )
        self.assertIsNotNone(match)
        self.assertEqual(
            html.count(
                "litePollingEditingSop=pollingMergeSopAttachmentResult(draft,remote)"
            ),
            2,
        )
        script = "\n".join(
            [
                match.group(0),
                "const draft={name:'修改后的名称',steps:[{content:'修改后的步骤'}]};",
                "const remote={name:'旧名称',steps:[{content:'旧步骤'}],version:3,attachments:[{name:'附件.pdf'}]};",
                "console.log(JSON.stringify(pollingMergeSopAttachmentResult(draft,remote)));",
            ]
        )
        result = subprocess.run(
            [node, "-e", script], check=True, capture_output=True, text=True
        )

        merged = json.loads(result.stdout)
        self.assertEqual(merged["name"], "修改后的名称")
        self.assertEqual(merged["steps"][0]["content"], "修改后的步骤")
        self.assertEqual(merged["version"], 3)
        self.assertEqual(merged["attachments"][0]["name"], "附件.pdf")

    def test_work_order_overview_and_steps_are_separate_pages(self) -> None:
        overview = render_polling_work_order_page()
        steps = render_polling_work_order_steps_page()

        self.assertIn('id="overview"', overview)
        self.assertNotIn('id="steps"', overview)
        self.assertIn("function workOrderCard(order)", overview)
        self.assertIn("另一工单执行中", overview)
        self.assertIn("button.disabled=!order.selectable||busy", overview)
        self.assertIn("/api/polling-work-orders/activate", overview)
        self.assertIn("/polling-work-order/steps?token=", overview)
        self.assertIn("取消当前选择", overview)
        self.assertIn("/api/polling-work-orders/release", overview)
        self.assertIn("全部工单已完成，工单表格已上传。", overview)

        self.assertIn('id="steps"', steps)
        self.assertNotIn('id="overview"', steps)
        self.assertNotIn("function workOrderCard(order)", steps)
        self.assertIn("第 ${Number(step.step_index||0)} 步", steps)
        self.assertNotIn("工单 ${step.run_index}/${step.run_count}", steps)
        self.assertNotIn("步骤 ${step.step_index}/${step.step_count}", steps)
        self.assertIn("倒计时 ${wait} 秒", steps)
        self.assertIn("Number(step.remaining_seconds||0)", steps)
        self.assertIn("function refreshCountdown()", steps)
        self.assertIn("setInterval(refreshCountdown,1000)", steps)
        self.assertIn("const next=body.data,sameView=", steps)
        self.assertNotIn("setInterval(()=>{if(current&&!busy&&!navigating)render(current)},1000)", steps)
        self.assertIn("function leaveWorkOrder()", steps)
        self.assertIn("/api/polling-work-orders/release", steps)
        self.assertIn("退出当前工单并重新选择", steps)
        self.assertIn("浏览器返回不会取消当前选择", steps)
        self.assertIn("拍照/上传操作照片", steps)
        self.assertIn("继续添加照片", steps)
        self.assertIn("操作照片 ${photoCount} 张", steps)
        self.assertIn("操作照片待拍", steps)
        self.assertIn("回退上一步", steps)
        self.assertIn("/api/polling-work-orders/rollback", steps)
        self.assertIn("上一步需重新倒计时、拍照并确认", steps)
        self.assertIn("busy=false;if(current&&!navigating)render(current)", steps)
        self.assertIn("capture','environment", steps)
        self.assertIn("image.loading='eager'", steps)
        self.assertIn("缩略图加载失败", steps)

        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        client = TestClient(controller._build_app())
        overview_response = client.get("/polling-work-order?token=test")
        steps_response = client.get(
            "/polling-work-order/steps?token=test&run_index=1"
        )
        self.assertEqual(overview_response.status_code, 200)
        self.assertEqual(steps_response.status_code, 200)
        self.assertNotEqual(overview_response.text, steps_response.text)

    def test_polling_step_photo_api_accepts_image_and_serves_preview(self) -> None:
        manager = MagicMock()
        manager.add_step_photo.return_value = {"version": 2, "steps": []}
        manager.step_photo_content.return_value = (
            b"photo-bytes",
            "image/png",
            "step.png",
        )
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        client = TestClient(controller._build_app())
        with patch.object(PortalRuntime, "polling_work_orders", return_value=manager):
            response = client.post(
                "/api/polling-work-orders/photo?token=role-token"
                "&step_key=1%3A1&expected_version=1&file_name=step.png",
                content=b"photo-bytes",
                headers={"Content-Type": "image/png"},
            )
            preview = client.get(
                "/api/polling-work-orders/photos/photo-id?token=role-token"
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertEqual(preview.content, b"photo-bytes")
        self.assertEqual(
            manager.add_step_photo.call_args.kwargs["expected_version"], 1
        )
        self.assertEqual(manager.add_step_photo.call_args.kwargs["content"], b"photo-bytes")

    def test_polling_work_order_activate_api_starts_selected_run(self) -> None:
        manager = MagicMock()
        manager.activate.return_value = {
            "version": 2,
            "current_run_index": 1,
            "steps": [{"timer_started": True, "remaining_seconds": 30}],
        }
        manager.release_selection.return_value = {
            "version": 3,
            "current_run_index": 0,
            "steps": [],
        }
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        client = TestClient(controller._build_app())
        with patch.object(PortalRuntime, "polling_work_orders", return_value=manager):
            response = client.post(
                "/api/polling-work-orders/activate",
                json={"token": "r" * 32, "run_index": 1, "expected_version": 1},
            )
            release_response = client.post(
                "/api/polling-work-orders/release",
                json={"token": "r" * 32, "run_index": 1, "expected_version": 2},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["data"]["steps"][0]["timer_started"])
        self.assertEqual(manager.activate.call_args.kwargs["run_index"], 1)
        self.assertEqual(manager.activate.call_args.kwargs["expected_version"], 1)
        self.assertEqual(release_response.status_code, 200, release_response.text)
        self.assertEqual(manager.release_selection.call_args.kwargs["run_index"], 1)

    def test_last_confirmation_queues_attachment_upload(self) -> None:
        manager = MagicMock()
        manager.confirm.return_value = {
            "state": "upload_pending",
            "group_id": "recQueued",
        }
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        client = TestClient(controller._build_app())
        with patch.object(
            PortalRuntime, "polling_work_orders", return_value=manager
        ), patch.object(
            controller, "_submit_background", return_value=True
        ) as submit, patch.object(
            PortalRuntime, "finalize_polling_work_order_group"
        ) as finalize:
            response = client.post(
                "/api/polling-work-orders/confirm",
                json={"token": "r" * 32, "step_key": "1:1", "expected_version": 1},
            )

        self.assertEqual(response.status_code, 200, response.text)
        completion = response.json()["data"]["completion"]
        self.assertEqual(completion["state"], "upload_pending")
        self.assertTrue(completion["queued"])
        finalize.assert_not_called()
        self.assertEqual(submit.call_args.args[0], "PollingWorkOrderFinalize")
        self.assertEqual(submit.call_args.args[2], "recQueued")

    def test_polling_work_order_rollback_api_uses_reviewer_token(self) -> None:
        manager = MagicMock()
        manager.rollback_previous.return_value = {
            "version": 3,
            "current_index": 0,
            "can_rollback_previous": False,
        }
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        client = TestClient(controller._build_app())
        with patch.object(PortalRuntime, "polling_work_orders", return_value=manager):
            response = client.post(
                "/api/polling-work-orders/rollback",
                json={"token": "r" * 32, "step_key": "1:2", "expected_version": 2},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["data"]["current_index"], 0)
        self.assertEqual(manager.rollback_previous.call_args.kwargs["step_key"], "1:2")

    def test_reviewer_can_rollback_repeatedly_and_operator_session_tracks_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = LanPortalStateStore(Path(temp) / "state.sqlite3")
            service = PollingWorkOrderService(store)
            service.work_order_root = Path(temp) / "orders"
            target_record_id = "recRepeatedRollback"
            photo_root = service._group_directory(target_record_id) / "photos"
            photo_root.mkdir(parents=True)
            steps = []
            for index in range(3):
                photo_path = photo_root / f"step-{index + 1}.png"
                photo_path.write_bytes(_png_bytes())
                done = index < 2
                steps.append(
                    {
                        "step_key": f"1:{index + 1}",
                        "global_index": index,
                        "run_index": 1,
                        "step_index": index + 1,
                        "content": f"步骤{index + 1}",
                        "operator_required": True,
                        "reviewer_required": True,
                        "activated_at_ts": time.time(),
                        "photos": [{"photo_id": str(index), "path": str(photo_path)}],
                        "operator_confirmation": {"confirmed_at": "2026-08-27 09:00:00"} if done else {},
                        "reviewer_confirmation": {"confirmed_at": "2026-08-27 09:01:00"} if done else {},
                    }
                )
            operator_token = service.role_token(target_record_id, "operator")
            reviewer_token = service.role_token(target_record_id, "reviewer")
            store.put_document(
                "polling_work_order",
                target_record_id,
                {
                    "group_id": target_record_id,
                    "target_record_id": target_record_id,
                    "state": "active",
                    "version": 1,
                    "current_index": 2,
                    "selected_run_index": 1,
                    "selected_by_role": "reviewer",
                    "runs": [{"from_unit": "1#", "to_unit": "2#"}],
                    "steps": steps,
                    "operator": {"name": "操作员"},
                    "reviewer": {"name": "审核员"},
                    "token_hashes": {
                        "operator": hashlib.sha256(operator_token.encode()).hexdigest(),
                        "reviewer": hashlib.sha256(reviewer_token.encode()).hexdigest(),
                    },
                },
            )

            reviewer_session = service.rollback_previous(
                reviewer_token,
                step_key="1:3",
                expected_version=1,
            )
            self.assertEqual(reviewer_session["current_index"], 1)
            self.assertTrue(reviewer_session["can_rollback_previous"])
            operator_session = service.session(operator_token)
            self.assertEqual(operator_session["current_index"], 1)
            self.assertEqual(operator_session["version"], reviewer_session["version"])

            reviewer_session = service.rollback_previous(
                reviewer_token,
                step_key="1:2",
                expected_version=reviewer_session["version"],
            )
            self.assertEqual(reviewer_session["current_index"], 0)
            self.assertFalse(reviewer_session["can_rollback_previous"])
            operator_session = service.session(operator_token)
            self.assertEqual(operator_session["current_index"], 0)
            self.assertEqual(operator_session["version"], reviewer_session["version"])

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
        self.assertIn("非制冷单元/二次泵轮巡", html)
        self.assertIn('name="polling_work_order_exempt"', html)
        self.assertIn("function pollingWorkOrderExempt(form)", html)
        self.assertIn("patch.polling_work_order_exempt = pollingWorkOrderExempt(form)", html)
        self.assertIn("&& !pollingWorkOrderExempt(form)", html)
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
        self.assertIn("时间限制（秒）", html)
        self.assertIn("time_limit_seconds", html)
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

    def test_polling_end_requires_completed_local_group_and_exact_attachment(self) -> None:
        prepared = {
            "action": "end",
            "work_type": "polling",
            "notice_type": "设备轮巡",
            "record_id": "recTargetExact",
            "target_record_id": "recTargetExact",
            "text": "【设备轮巡】状态：结束\n【标题】轮巡测试",
        }
        manager = MagicMock()
        manager.get_group.return_value = {
            "state": "completed",
            "uploaded_file_tokens": ["expected-workbook-token"],
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
                        "工单附件": [{"file_token": "manual-file-token"}],
                    }
                },
            ),
        ), patch.object(
            PortalRuntime,
            "polling_work_orders",
            return_value=manager,
        ):
            ok, message, _record_id = PortalRuntime._execute_backend_prepared_upload(
                prepared
            )
        self.assertFalse(ok)
        self.assertIn("与本次完成记录不一致", message)

    def test_exempt_polling_start_clears_stale_work_order_fields(self) -> None:
        prepared = {
            "action": "start",
            "work_type": "polling",
            "notice_type": "设备轮巡",
            "record_id": "recTarget1",
            "target_record_id": "recTarget1",
            "polling_work_order_exempt": True,
            "text": "【设备轮巡】状态：开始\n【标题】非工单轮巡",
        }
        patches = []
        manager = MagicMock()

        def update_fields(_record_id, _notice_type, fields):
            patches.append(fields)
            return True, "ok"

        with patch.object(
            portal_server,
            "external_real_write_guard",
            return_value={"mock_external": False, "real_write_allowed": True, "reason": ""},
        ), patch.object(
            PortalRuntime,
            "_existing_target_for_prepared_start",
            return_value="recTarget1",
        ), patch.object(
            PortalRuntime,
            "_upload_change_confirmation_images",
            return_value=(True, "", [], []),
        ), patch.object(
            portal_server,
            "update_bitable_record_fields",
            side_effect=update_fields,
        ), patch.object(
            portal_server,
            "query_record_by_id",
            return_value=(
                True,
                {
                    "fields": {
                        "是否涉及重要操作": False,
                        "操作人": "",
                        "现场复核人": "",
                    }
                },
            ),
        ), patch.object(
            PortalRuntime,
            "_create_backend_undo_checkpoint",
            return_value="",
        ), patch.object(
            PortalRuntime,
            "_mark_local_notice_images_target_written",
        ), patch.object(
            PortalRuntime,
            "polling_work_orders",
            return_value=manager,
        ):
            ok, _message, record_id = PortalRuntime._execute_backend_prepared_upload(
                prepared
            )

        self.assertTrue(ok)
        self.assertEqual(record_id, "recTarget1")
        self.assertIn(
            {"是否涉及重要操作": False, "操作人": "", "现场复核人": ""},
            patches,
        )
        manager.cancel_group.assert_called_once_with(
            "recTarget1", reason="work_order_exempt"
        )

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

    def test_failed_work_order_link_notifications_are_throttled(self) -> None:
        manager = MagicMock()
        manager.group_with_links.side_effect = lambda group, _base: group
        manager.update_notifications.side_effect = lambda _record_id, data: data
        group = {
            "target_record_id": "recNotifyThrottle",
            "title": "轮巡测试",
            "sop_name": "SOP",
            "runs": [{}],
            "operator": {"name": "操作员", "open_id": "ou_op"},
            "reviewer": {"name": "审核员", "open_id": "ou_re"},
            "initiator_open_id": "ou_sender",
            "operator_link": "https://relay.example/op",
            "reviewer_link": "https://relay.example/re",
        }
        with patch.object(
            PortalRuntime, "polling_work_orders", return_value=manager
        ), patch.object(
            portal_server,
            "_send_text_to_open_ids_guarded",
            return_value=(False, "network down", []),
        ) as send:
            notifications = PortalRuntime._send_polling_work_order_links(group)
            PortalRuntime._send_polling_work_order_links(
                {**group, "notifications": notifications}
            )
        self.assertEqual(send.call_count, 4)
        self.assertGreater(notifications["operator"]["next_retry_at"], time.time())

    def test_polling_start_creates_lan_group_and_sends_lan_links(self) -> None:
        manager = MagicMock()
        group = {"target_record_id": "recLanStart", "state": "active"}
        manager.create_group.return_value = group
        prepared = {
            "work_type": "polling",
            "action": "start",
            "polling_work_order_required": True,
            "title": "局域网轮巡测试",
        }
        with patch.object(
            PortalRuntime, "polling_work_orders", return_value=manager
        ), patch.object(
            PortalRuntime,
            "_polling_work_order_public_base_url",
            return_value="http://192.168.1.10:18766",
        ), patch.object(
            PortalRuntime,
            "polling_work_order_public_relay_enabled",
            return_value=False,
        ), patch.object(
            PortalRuntime,
            "polling_work_order_relay",
        ) as relay_connector, patch.object(
            PortalRuntime, "_send_polling_work_order_links"
        ) as send_links, patch.dict(
            os.environ,
            {"CLIPFLOW_POLLING_RELAY_URL": "https://relay.example"},
        ):
            result = PortalRuntime._create_polling_work_order_group(
                prepared, "recLanStart"
            )
        self.assertEqual(result, group)
        self.assertEqual(
            manager.create_group.call_args.kwargs["public_base_url"],
            "http://192.168.1.10:18766",
        )
        self.assertNotIn("public_relay", manager.create_group.call_args.kwargs)
        relay_connector.assert_not_called()
        send_links.assert_called_once_with(group)

    def test_polling_start_can_use_preserved_public_relay_path(self) -> None:
        manager = MagicMock()
        relay = MagicMock(enabled=True)
        group = {"target_record_id": "recPublicStart", "state": "active"}
        manager.create_group.return_value = group
        prepared = {
            "work_type": "polling",
            "action": "start",
            "polling_work_order_required": True,
            "title": "公网轮巡测试",
        }
        with patch.object(
            PortalRuntime, "polling_work_orders", return_value=manager
        ), patch.object(
            PortalRuntime,
            "polling_work_order_public_relay_enabled",
            return_value=True,
        ), patch.object(
            PortalRuntime,
            "polling_work_order_relay",
            return_value=relay,
        ), patch.object(
            PortalRuntime, "_send_polling_work_order_links"
        ) as send_links:
            result = PortalRuntime._create_polling_work_order_group(
                prepared, "recPublicStart"
            )
        self.assertEqual(result, group)
        self.assertEqual(manager.create_group.call_args.kwargs["public_base_url"], "")
        self.assertTrue(manager.create_group.call_args.kwargs["public_relay"])
        send_links.assert_not_called()

    def test_existing_public_group_is_migrated_to_lan_links(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = LanPortalStateStore(Path(temp) / "state.sqlite3")
            previous_store = PortalRuntime.state_store
            PortalRuntime.state_store = store
            manager = MagicMock()
            group = {
                "target_record_id": "recLegacyRelay",
                "state": "active",
                "relay": {"mode": "public_relay"},
            }
            store.put_document("polling_work_order", "recLegacyRelay", group)
            manager.open_groups.return_value = [group]
            try:
                with patch.object(
                    PortalRuntime, "polling_work_orders", return_value=manager
                ), patch.object(
                    PortalRuntime, "_send_polling_work_order_links"
                ) as send_links, patch.object(
                    portal_server,
                    "query_record_by_id",
                    return_value=(True, {"fields": {}}),
                ), patch.object(
                    PortalRuntime.service,
                    "_target_record_lifecycle",
                    return_value={"active": True, "finished": False},
                ):
                    PortalRuntime.process_polling_work_order_uploads()
                migrated = store.get_document(
                    "polling_work_order", "recLegacyRelay"
                )
                self.assertNotIn("relay", migrated)
                send_links.assert_called_once()
            finally:
                PortalRuntime.state_store = previous_store

    def test_resend_links_checks_scope_and_never_returns_capabilities(self) -> None:
        controller = FastAPIPortalController(host="127.0.0.1", port=18766)
        client = TestClient(controller._build_app())
        manager = MagicMock()
        manager.get_group.return_value = {
            "target_record_id": "recRelaySafe",
            "scope": "E",
        }
        sensitive_group = {
            "notifications": {
                "operator": {"sent": True, "fallback": False},
                "reviewer": {"sent": True, "fallback": True},
            },
            "operator_link": "https://relay.example/#secret",
            "reviewer_link": "https://relay.example/#secret2",
            "attachments": [{"path": "D:/private/file.xlsx"}],
        }
        with patch.object(
            controller, "_current_session", return_value={"user": {"open_id": "ou_a"}}
        ), patch.object(
            PortalRuntime, "polling_work_orders", return_value=manager
        ), patch.object(
            controller,
            "_authorized_scope_or_error",
            side_effect=portal_server.PortalError("无权访问 E 楼"),
        ), patch.object(
            PortalRuntime, "_send_polling_work_order_links"
        ) as denied_send:
            denied = client.post(
                "/api/polling-work-orders/recRelaySafe/resend-links"
            )
        self.assertEqual(denied.status_code, 403, denied.text)
        denied_send.assert_not_called()
        with patch.object(
            controller, "_current_session", return_value={"user": {"open_id": "ou_a"}}
        ), patch.object(
            PortalRuntime, "polling_work_orders", return_value=manager
        ), patch.object(
            controller, "_authorized_scope_or_error", return_value="E"
        ), patch.object(
            PortalRuntime,
            "_send_polling_work_order_links",
            return_value=sensitive_group,
        ):
            response = client.post(
                "/api/polling-work-orders/recRelaySafe/resend-links"
            )
        self.assertEqual(response.status_code, 200, response.text)
        response_text = response.text
        self.assertNotIn("operator_link", response_text)
        self.assertNotIn("reviewer_link", response_text)
        self.assertNotIn("D:/private", response_text)

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
            exempt = service.prepare_start(
                {
                    "work_type": "polling",
                    "scope": "A",
                    "action": "start",
                    "_web_action_request": True,
                    "polling_work_order_exempt": True,
                },
                job_id="job-exempt",
                people=[],
            )
            self.assertTrue(exempt["polling_work_order_exempt"])
            self.assertNotIn("polling_work_order_required", exempt)
            self.assertNotIn("polling_work_order_spec", exempt)
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
            with self.assertRaisesRegex(Exception, "工单总览"):
                service.confirm(reviewer_token, step_key="1:1", expected_version=1)
            session = service.activate(
                operator_token, run_index=1, expected_version=1
            )
            with self.assertRaises(Exception):
                service.confirm(
                    reviewer_token,
                    step_key="1:1",
                    expected_version=session["version"],
                )
            session = service.add_step_photo(
                operator_token,
                step_key="1:1",
                expected_version=session["version"],
                file_name="第一步.png",
                mime_type="image/png",
                content=_png_bytes(),
            )
            session = service.confirm(
                operator_token,
                step_key="1:1",
                expected_version=session["version"],
            )
            session = service.confirm(reviewer_token, step_key="1:1", expected_version=session["version"])
            self.assertEqual(session["current_index"], 1)
            first_photo_path = Path(
                service.get_group("recTarget1")["steps"][0]["photos"][0]["path"]
            )
            with self.assertRaisesRegex(Exception, "只有现场审核人"):
                service.rollback_previous(
                    operator_token,
                    step_key="1:2",
                    expected_version=session["version"],
                )
            session = service.rollback_previous(
                reviewer_token,
                step_key="1:2",
                expected_version=session["version"],
            )
            self.assertEqual(session["current_index"], 0)
            self.assertFalse(first_photo_path.exists())
            self.assertFalse(service.get_group("recTarget1")["steps"][0]["photos"])
            session = service.add_step_photo(
                operator_token,
                step_key="1:1",
                expected_version=session["version"],
                file_name="第一步重做.png",
                mime_type="image/png",
                content=_png_bytes("#ff8a00"),
            )
            session = service.confirm(
                operator_token, step_key="1:1", expected_version=session["version"]
            )
            session = service.confirm(
                reviewer_token, step_key="1:1", expected_version=session["version"]
            )
            with self.assertRaisesRegex(Exception, "拍摄并上传"):
                service.confirm(
                    reviewer_token,
                    step_key="1:2",
                    expected_version=session["version"],
                )
            session = service.add_step_photo(
                reviewer_token,
                step_key="1:2",
                expected_version=session["version"],
                file_name="第二步.png",
                mime_type="image/png",
                content=_png_bytes("#12a150"),
            )
            session = service.confirm(reviewer_token, step_key="1:2", expected_version=session["version"])
            self.assertEqual(session["state"], "upload_pending")
            service.mark_upload_result("recTarget1", success=True, file_tokens=["file-token"])
            self.assertEqual(service.session(operator_token)["state"], "completed")
            self.assertEqual(service.open_groups()[0]["state"], "completed")
            service.cancel_group("recTarget1", reason="target_terminal")
            with self.assertRaises(PollingWorkOrderTokenError):
                service.session(operator_token)

            prepared_two_runs = service.prepare_start(
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
                        {"from_unit": "4#", "to_unit": "5#"},
                    ],
                    "polling_operator_record_id": "operator",
                    "polling_reviewer_record_id": "reviewer",
                },
                job_id="job-two-runs",
                people=[
                    {"record_id": "operator", "name": "操作员"},
                    {"record_id": "reviewer", "name": "审核员"},
                ],
            )
            service.create_group(
                prepared_two_runs,
                target_record_id="recTargetTwoRuns",
                title="两次轮巡测试",
                public_base_url="",
            )
            operator_two = service.role_token("recTargetTwoRuns", "operator")
            reviewer_two = service.role_token("recTargetTwoRuns", "reviewer")
            overview = service.session(operator_two)
            self.assertEqual(
                [item["state"] for item in overview["work_orders"]],
                ["available", "available"],
            )
            second_run_started = service.activate(
                operator_two,
                run_index=2,
                expected_version=overview["version"],
            )
            with self.assertRaisesRegex(Exception, "已选择其他工单"):
                service.activate(
                    reviewer_two,
                    run_index=1,
                    expected_version=overview["version"],
                )
            self.assertTrue(service.session(operator_two)["can_release_selection"])
            self.assertFalse(service.session(reviewer_two)["can_release_selection"])
            with self.assertRaisesRegex(Exception, "不能代为退出"):
                service.release_selection(
                    reviewer_two,
                    run_index=2,
                    expected_version=second_run_started["version"],
                )
            released = service.release_selection(
                operator_two,
                run_index=2,
                expected_version=second_run_started["version"],
            )
            self.assertEqual(
                [item["state"] for item in released["work_orders"]],
                ["available", "available"],
            )
            self.assertEqual(
                service.get_group("recTargetTwoRuns")["steps"][2]["activated_at_ts"],
                0.0,
            )
            second_run_started = service.activate(
                reviewer_two,
                run_index=2,
                expected_version=released["version"],
            )
            sop = service.save_sop(
                {
                    "sop_id": sop["sop_id"],
                    "scope": "A",
                    "name": "制冷单元切换",
                    "expected_version": sop["version"],
                    "steps": [
                        {**step, "time_limit_seconds": seconds}
                        for step, seconds in zip(sop["steps"], (4, 6))
                    ],
                }
            )
            two_run_group = service.get_group("recTargetTwoRuns")
            self.assertEqual(
                [step["time_limit_seconds"] for step in two_run_group["steps"][:2]],
                [4, 6],
            )
            self.assertEqual(
                [step["time_limit_seconds"] for step in two_run_group["steps"][2:]],
                [0, 0],
            )
            self.assertEqual(two_run_group["version"], second_run_started["version"])
            first_run = service.session(operator_two)
            self.assertEqual(
                [item["state"] for item in first_run["work_orders"]],
                ["locked", "active"],
            )
            self.assertTrue(first_run["steps"])
            self.assertTrue(
                all(step["run_index"] == 2 for step in first_run["steps"])
            )
            first_run = service.add_step_photo(
                operator_two,
                step_key="2:1",
                expected_version=first_run["version"],
                file_name="工单2第一步.png",
                mime_type="image/png",
                content=_png_bytes(),
            )
            first_run = service.confirm(
                operator_two, step_key="2:1", expected_version=first_run["version"]
            )
            first_run = service.confirm(
                reviewer_two, step_key="2:1", expected_version=first_run["version"]
            )
            first_run = service.add_step_photo(
                reviewer_two,
                step_key="2:2",
                expected_version=first_run["version"],
                file_name="工单2第二步.png",
                mime_type="image/png",
                content=_png_bytes("#12a150"),
            )
            second_run = service.confirm(
                reviewer_two, step_key="2:2", expected_version=first_run["version"]
            )
            self.assertEqual(
                [item["state"] for item in second_run["work_orders"]],
                ["available", "completed"],
            )
            self.assertEqual(second_run["current_run_index"], 0)
            self.assertFalse(second_run["steps"])
            activated_second_run = service.activate(
                operator_two,
                run_index=1,
                expected_version=second_run["version"],
            )
            self.assertEqual(activated_second_run["steps"][0]["step_index"], 1)
            self.assertEqual(
                activated_second_run["steps"][0]["content"],
                "将1#切换至2#，检查3#",
            )
            self.assertTrue(activated_second_run["steps"][0]["timer_started"])
            self.assertGreater(activated_second_run["steps"][0]["remaining_seconds"], 0)
            idempotent_activation = service.activate(
                reviewer_two,
                run_index=1,
                expected_version=second_run["version"],
            )
            self.assertEqual(
                idempotent_activation["version"], activated_second_run["version"]
            )

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

    def test_step_timer_photo_and_execution_workbook(self) -> None:
        from openpyxl import load_workbook

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = LanPortalStateStore(root / "state.sqlite3")
            service = PollingWorkOrderService(store)
            service.sop_root = root / "sops"
            service.work_order_root = root / "orders"
            sop = service.save_sop(
                {
                    "scope": "A",
                    "name": "倒计时拍照 SOP",
                    "steps": [
                        {
                            "content": "将{{from}}切换到{{to}}",
                            "operator_required": True,
                            "reviewer_required": True,
                            "time_limit_seconds": 2,
                        }
                    ],
                }
            )
            sop = service.add_sop_attachment(
                sop["sop_id"],
                file_name="原SOP.pdf",
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
                    "polling_runs": [{"from_unit": "1#", "to_unit": "2#"}],
                    "polling_operator_record_id": "operator",
                    "polling_reviewer_record_id": "reviewer",
                },
                job_id="timer-photo-job",
                people=[
                    {"record_id": "operator", "name": "操作员", "open_id": "ou_operator"},
                    {"record_id": "reviewer", "name": "审核员", "open_id": "ou_reviewer"},
                ],
            )
            service.create_group(
                prepared,
                target_record_id="recTimerPhoto",
                title="轮巡倒计时拍照测试",
                public_base_url="http://127.0.0.1:18766",
            )
            operator_token = service.role_token("recTimerPhoto", "operator")
            reviewer_token = service.role_token("recTimerPhoto", "reviewer")
            first_session = service.session(operator_token)
            self.assertFalse(first_session["steps"])
            self.assertEqual(first_session["work_orders"][0]["state"], "available")
            with self.assertRaisesRegex(Exception, "工单总览"):
                service.confirm(
                    operator_token,
                    step_key="1:1",
                    expected_version=first_session["version"],
                )
            activated_session = service.activate(
                operator_token,
                run_index=1,
                expected_version=first_session["version"],
            )
            self.assertTrue(activated_session["steps"][0]["timer_started"])
            self.assertGreater(activated_session["steps"][0]["remaining_seconds"], 0)
            with self.assertRaisesRegex(Exception, "还需等待"):
                service.confirm(
                    operator_token,
                    step_key="1:1",
                    expected_version=activated_session["version"],
                )
            with self.assertRaisesRegex(Exception, "内容损坏"):
                service.add_step_photo(
                    operator_token,
                    step_key="1:1",
                    expected_version=activated_session["version"],
                    file_name="损坏.png",
                    mime_type="image/png",
                    content=b"not-an-image",
                )
            photo_bytes = _png_bytes()
            photo_session = service.add_step_photo(
                operator_token,
                step_key="1:1",
                expected_version=activated_session["version"],
                file_name="现场.png",
                mime_type="image/png",
                content=photo_bytes,
            )
            photo = photo_session["steps"][0]["photos"][0]
            self.assertIn("/api/polling-work-orders/photos/", photo["preview_url"])
            self.assertEqual(
                service.step_photo_content(
                    operator_token, photo_id=photo["photo_id"]
                )[0],
                photo_bytes,
            )
            with self.assertRaisesRegex(Exception, "已有操作记录"):
                service.release_selection(
                    operator_token,
                    run_index=1,
                    expected_version=photo_session["version"],
                )
            stored_before_replace = service.get_group("recTimerPhoto")["steps"][0][
                "photos"
            ][0]
            photo_directory = Path(stored_before_replace["path"]).parent
            files_before_failed_replace = set(photo_directory.iterdir())
            with patch.object(
                store, "put_document", side_effect=RuntimeError("forced write failure")
            ), self.assertRaisesRegex(RuntimeError, "forced write failure"):
                service.add_step_photo(
                    operator_token,
                    step_key="1:1",
                    expected_version=photo_session["version"],
                    file_name="替换照片.png",
                    mime_type="image/png",
                    content=_png_bytes("#ff0000"),
                )
            stored_photo = service.get_group("recTimerPhoto")["steps"][0]["photos"][0]
            self.assertEqual(stored_photo["photo_id"], photo["photo_id"])
            self.assertEqual(set(photo_directory.iterdir()), files_before_failed_replace)
            multi_photo_session = service.add_step_photo(
                operator_token,
                step_key="1:1",
                expected_version=photo_session["version"],
                file_name="补充照片.png",
                mime_type="image/png",
                content=_png_bytes("#ff0000"),
            )
            self.assertEqual(len(multi_photo_session["steps"][0]["photos"]), 2)
            self.assertTrue(Path(stored_photo["path"]).is_file())
            duplicate_session = service.add_step_photo(
                operator_token,
                step_key="1:1",
                expected_version=multi_photo_session["version"],
                file_name="重复照片.png",
                mime_type="image/png",
                content=photo_bytes,
            )
            self.assertEqual(len(duplicate_session["steps"][0]["photos"]), 2)
            with patch(
                "lan_bitable_template_portal.polling_work_orders.POLLING_STEP_MAX_PHOTOS",
                2,
            ), self.assertRaisesRegex(Exception, "最多上传 2 张"):
                service.add_step_photo(
                    operator_token,
                    step_key="1:1",
                    expected_version=multi_photo_session["version"],
                    file_name="超限照片.png",
                    mime_type="image/png",
                    content=_png_bytes("#00ff00"),
                )
            with patch(
                "lan_bitable_template_portal.polling_work_orders.POLLING_WORK_ORDER_MAX_PHOTOS",
                2,
            ), self.assertRaisesRegex(Exception, "整个工单组最多上传 2 张"):
                service.add_step_photo(
                    operator_token,
                    step_key="1:1",
                    expected_version=multi_photo_session["version"],
                    file_name="工单总量超限照片.png",
                    mime_type="image/png",
                    content=_png_bytes("#00ff00"),
                )
            group = service.get_group("recTimerPhoto")
            group["steps"][0]["activated_at_ts"] = time.time() - 3
            store.put_document("polling_work_order", "recTimerPhoto", group)
            operator_confirmed_session = service.confirm(
                operator_token,
                step_key="1:1",
                expected_version=multi_photo_session["version"],
            )
            with self.assertRaisesRegex(Exception, "已有确认"):
                service.add_step_photo(
                    operator_token,
                    step_key="1:1",
                    expected_version=operator_confirmed_session["version"],
                    file_name="确认后替换.png",
                    mime_type="image/png",
                    content=_png_bytes("#00ff00"),
                )
            completed_session = service.confirm(
                reviewer_token,
                step_key="1:1",
                expected_version=operator_confirmed_session["version"],
            )
            self.assertEqual(completed_session["state"], "upload_pending")
            workbook_info = service.build_execution_workbook("recTimerPhoto")
            workbook = load_workbook(workbook_info["path"], data_only=False)
            sheet = workbook["工单1 1#→2#"]
            values = [cell.value for row in sheet.iter_rows() for cell in row]
            self.assertEqual(sheet.max_row, 9)
            self.assertEqual(sheet["C3"].value, "操作员")
            self.assertEqual(sheet["D3"].value, "审核员")
            self.assertIsNotNone(sheet["E3"].value)
            self.assertEqual(sheet["C9"].value, 1)
            self.assertEqual(sheet["D9"].value, "将1#切换到2#")
            self.assertEqual(len(sheet._images), 3)
            self.assertIn(5, [image.anchor._from.col for image in sheet._images])
            self.assertIn("$A$1:$F$9", str(sheet.print_area))
            workbook.close()
            self.assertIn("倒计时拍照 SOP · 1#→2#", values)
            self.assertIn("将1#切换到2#", values)
            self.assertRegex(
                workbook_info["name"],
                r"^A楼\d{4}年\d{1,2}月\d{1,2}日-倒计时拍照 SOP-1#轮巡至2#-操作人-操作员-审核人-审核员-操作记录\.xlsx$",
            )
            with patch(
                "lan_bitable_template_portal.polling_work_orders.POLLING_WORK_ORDER_MAX_BYTES",
                1,
            ), self.assertRaisesRegex(Exception, "超过允许大小"):
                service.build_execution_workbook("recTimerPhoto")

    def test_template_workbook_expands_steps_and_uses_one_sheet_per_run(self) -> None:
        from openpyxl import load_workbook

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = LanPortalStateStore(root / "state.sqlite3")
            service = PollingWorkOrderService(store)
            service.work_order_root = root / "orders"
            target_record_id = "recTemplateRuns"
            photo_root = service._group_directory(target_record_id) / "photos"
            photo_root.mkdir(parents=True)
            first_photo = photo_root / "first.png"
            second_photo = photo_root / "second.png"
            first_photo.write_bytes(_png_bytes("#1678ff", (240, 120)))
            second_photo.write_bytes(_png_bytes("#12a150", (120, 240)))
            steps = []
            for run_index, label in ((1, "1#→2#"), (2, "4#→5#")):
                step_count = 2 if run_index == 1 else 3
                for step_index in range(1, step_count + 1):
                    content = (
                        '=WEBSERVICE("https://example.invalid")'
                        if run_index == 1 and step_index == 1
                        else f"{label}操作步骤{step_index}"
                    )
                    photo_path = first_photo if run_index == 1 else second_photo
                    steps.append(
                        {
                            "step_key": f"{run_index}:{step_index}",
                            "run_index": run_index,
                            "run_label": label,
                            "step_index": step_index,
                            "content": content,
                            "operator_required": True,
                            "reviewer_required": False,
                            "operator_confirmation": {
                                "confirmed_at": (
                                    f"2026-08-26 {9 + run_index:02d}:0{step_index}:00"
                                )
                            },
                            "reviewer_confirmation": {},
                            "photos": (
                                [
                                    {
                                        "photo_id": f"photo-{run_index}",
                                        "name": photo_path.name,
                                        "path": str(photo_path),
                                    }
                                ]
                                if photo_path
                                else []
                            ),
                        }
                    )
            store.put_document(
                "polling_work_order",
                target_record_id,
                {
                    "group_id": target_record_id,
                    "target_record_id": target_record_id,
                    "state": "upload_pending",
                    "scope": "A",
                    "title": "双工单模板测试",
                    "sop_name": "冷机切换SOP",
                    "operator": {"name": "操作员甲"},
                    "reviewer": {"name": "审核员乙"},
                    "runs": [
                        {"from_unit": "1#", "to_unit": "2#"},
                        {"from_unit": "4#", "to_unit": "5#"},
                    ],
                    "steps": steps,
                    "updated_at": "2026-08-26 11:03:00",
                },
            )

            workbook_info = service.build_execution_workbook(target_record_id)
            workbook = load_workbook(workbook_info["path"], data_only=False)
            self.assertEqual(
                workbook.sheetnames,
                ["工单1 1#→2#", "工单2 4#→5#"],
            )
            self.assertEqual(
                workbook_info["name"],
                "A楼2026年8月26日-冷机切换SOP-1#4#轮巡至2#5#-操作人-操作员甲-审核人-审核员乙-操作记录.xlsx",
            )
            first_sheet, second_sheet = workbook.worksheets
            self.assertEqual(first_sheet.max_row, 10)
            self.assertEqual(second_sheet.max_row, 11)
            self.assertEqual(
                [first_sheet.cell(row=row, column=3).value for row in range(9, 11)],
                [1, 2],
            )
            self.assertEqual(
                [second_sheet.cell(row=row, column=3).value for row in range(9, 12)],
                [1, 2, 3],
            )
            self.assertIn(
                "A11:B11", {str(item) for item in second_sheet.merged_cells.ranges}
            )
            self.assertIn(
                "D11:E11", {str(item) for item in second_sheet.merged_cells.ranges}
            )
            self.assertEqual(second_sheet["C10"].style_id, second_sheet["C11"].style_id)
            self.assertEqual(
                second_sheet.row_dimensions[10].height,
                second_sheet.row_dimensions[11].height,
            )
            for sheet, last_row, image_count in (
                (first_sheet, 10, 3),
                (second_sheet, 11, 4),
            ):
                self.assertEqual(len(sheet._images), image_count)
                self.assertIn(f"$A$1:$E${last_row}", str(sheet.print_area))
                self.assertEqual(sheet.print_title_rows, "$8:$8")
            self.assertEqual(first_sheet["C3"].value, "操作员甲")
            self.assertEqual(first_sheet["D3"].value, "审核员乙")
            self.assertEqual(first_sheet["D9"].value, "'=WEBSERVICE(\"https://example.invalid\")")
            self.assertEqual(second_sheet["D9"].value, "4#→5#操作步骤1")
            self.assertEqual(first_sheet["E3"].number_format, "yyyy-mm-dd hh:mm:ss")
            self.assertLess(first_sheet["E3"].value, second_sheet["E3"].value)
            workbook.close()
            from PIL import Image

            with zipfile.ZipFile(workbook_info["path"]) as archive:
                embedded_sizes = []
                for name in archive.namelist():
                    if not name.startswith("xl/media/"):
                        continue
                    with Image.open(io.BytesIO(archive.read(name))) as image:
                        embedded_sizes.append(image.size)
            self.assertTrue(embedded_sizes)
            self.assertTrue(
                all(width <= 230 and height <= 190 for width, height in embedded_sizes)
            )
            self.assertNotIn((240, 120), embedded_sizes)
            self.assertNotIn((120, 240), embedded_sizes)

    def test_template_workbook_fails_when_photo_metadata_file_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = LanPortalStateStore(root / "state.sqlite3")
            service = PollingWorkOrderService(store)
            service.work_order_root = root / "orders"
            target_record_id = "recMissingPhoto"
            missing_path = service._group_directory(target_record_id) / "photos" / "missing.png"
            store.put_document(
                "polling_work_order",
                target_record_id,
                {
                    "group_id": target_record_id,
                    "target_record_id": target_record_id,
                    "state": "upload_pending",
                    "sop_name": "照片缺失SOP",
                    "operator": {"name": "操作员"},
                    "reviewer": {"name": "审核员"},
                    "runs": [{"from_unit": "1#", "to_unit": "2#"}],
                    "steps": [
                        {
                            "run_index": 1,
                            "step_index": 1,
                            "content": "检查照片",
                            "photos": [{"name": "missing.png", "path": str(missing_path)}],
                        }
                    ],
                    "updated_at": "2026-08-26 12:00:00",
                },
            )

            with self.assertRaisesRegex(Exception, "本地照片不存在"):
                service.build_execution_workbook(target_record_id)
            self.assertFalse(
                list(service._group_directory(target_record_id).glob("*.xlsx"))
            )

    def test_finalize_uploads_only_generated_workbook(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = LanPortalStateStore(root / "state.sqlite3")
            service = PollingWorkOrderService(store)
            service.sop_root = root / "sops"
            service.work_order_root = root / "orders"
            sop = service.save_sop(
                {
                    "scope": "A",
                    "name": "最终上传 SOP",
                    "steps": [
                        {
                            "content": "执行{{from}}到{{to}}",
                            "operator_required": True,
                            "reviewer_required": False,
                        }
                    ],
                }
            )
            sop = service.add_sop_attachment(
                sop["sop_id"],
                file_name="不会直接上传.pdf",
                content=b"source-sop",
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
                    "polling_runs": [{"from_unit": "1#", "to_unit": "2#"}],
                    "polling_operator_record_id": "operator",
                    "polling_reviewer_record_id": "reviewer",
                },
                job_id="finalize-job",
                people=[
                    {"record_id": "operator", "name": "操作员"},
                    {"record_id": "reviewer", "name": "审核员"},
                ],
            )
            service.create_group(
                prepared,
                target_record_id="recFinalize",
                title="最终上传测试",
                public_base_url="",
            )
            token = service.role_token("recFinalize", "operator")
            active_session = service.activate(
                token,
                run_index=1,
                expected_version=service.session(token)["version"],
            )
            photo_session = service.add_step_photo(
                token,
                step_key="1:1",
                expected_version=active_session["version"],
                file_name="步骤照片.png",
                mime_type="image/png",
                content=_png_bytes(),
            )
            service.confirm(
                token,
                step_key="1:1",
                expected_version=photo_session["version"],
            )
            service.mark_upload_progress(
                "recFinalize",
                token_by_sha256={"old-workbook-digest": "old-workbook-token"},
            )
            expected_workbook_name = service.build_execution_workbook("recFinalize")[
                "name"
            ]
            uploaded_names = []
            patches = []

            def upload_file(*, file_path, file_name, app_token):
                del file_path, app_token
                uploaded_names.append(file_name)
                return "workbook-token" if file_name.endswith(".xlsx") else "photo-token"

            active_fields = {
                "轮巡状态": "开始",
                "工单附件": [
                    {"file_token": "manual-attachment-token"},
                    {"file_token": "old-workbook-token"},
                ],
                "操作照片": [{"file_token": "historical-photo-token"}],
            }
            verified_fields = {
                **active_fields,
                "工单附件": [
                    {"file_token": "manual-attachment-token"},
                    {"file_token": "workbook-token"},
                ],
            }
            with patch.object(
                PortalRuntime, "polling_work_orders", return_value=service
            ), patch.object(
                PortalRuntime.service,
                "_upload_bitable_file",
                side_effect=upload_file,
            ), patch.object(
                portal_server,
                "query_record_by_id",
                side_effect=[
                    (True, {"fields": active_fields}),
                    (True, {"fields": verified_fields}),
                ],
            ), patch.object(
                portal_server,
                "update_bitable_record_fields",
                side_effect=lambda _record_id, _notice_type, fields: (
                    patches.append(fields) or (True, "recFinalize")
                ),
            ):
                result = PortalRuntime.finalize_polling_work_order_group(
                    "recFinalize"
                )
            self.assertTrue(result["ok"], result)
            self.assertEqual(uploaded_names, [expected_workbook_name])
            self.assertEqual(
                patches[0]["工单附件"],
                [
                    {"file_token": "manual-attachment-token"},
                    {"file_token": "workbook-token"},
                ],
            )
            self.assertNotIn("操作照片", patches[0])

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

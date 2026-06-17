import sys
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    MaintenancePortalService,
    NOTICE_TEXT_TEMPLATES,
    NOTICE_TIME_SEPARATOR,
    WORK_TYPE_ADJUST,
    WORK_TYPE_CHANGE,
    WORK_TYPE_MAINTENANCE,
    WORK_TYPE_POLLING,
    WORK_TYPE_POWER,
    WORK_TYPE_REPAIR,
)
from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402
from upload_event_module.core.parser import extract_event_info  # noqa: E402


class NoticeTemplateTests(unittest.TestCase):
    def _assert_lines(self, text: str, expected: list[str]) -> None:
        self.assertEqual(text.splitlines(), expected)

    def test_maintenance_notice_text_contract(self):
        text = MaintenancePortalService.build_notice_text(
            status="开始",
            title="EA118机房A楼过滤网维护",
            start_time="2026-06-12 09:30",
            end_time="2026-06-12 18:30",
            location="A楼空调间",
            content="更换过滤网",
            reason="周期维保",
            impact="无业务影响",
            progress="准备工作已完成",
        )
        self._assert_lines(
            text,
            [
                "【维保通告】状态：开始",
                "【名称】EA118机房A楼过滤网维护",
                "【时间】2026-06-12 09:30~2026-06-12 18:30",
                "【位置】A楼空调间",
                "【内容】更换过滤网",
                "【原因】周期维保",
                "【影响】无业务影响",
                "【进度】准备工作已完成",
            ],
        )

    def test_change_notice_text_contract(self):
        text = MaintenancePortalService.build_change_notice_text(
            status="更新",
            title="EA118机房C楼蓄电池放电变更",
            level="I3",
            start_time="2026-06-12 09:00",
            end_time="2026-06-12 18:00",
            location="C楼电池室",
            content="蓄电池放电",
            reason="容量测试",
            impact="无业务影响",
            progress="执行中",
        )
        self._assert_lines(
            text,
            [
                "【设备变更】状态：更新",
                "【名称】EA118机房C楼蓄电池放电变更",
                "【等级】I3",
                "【时间】2026-06-12 09:00~2026-06-12 18:00",
                "【位置】C楼电池室",
                "【内容】蓄电池放电",
                "【原因】容量测试",
                "【影响】无业务影响",
                "【进度】执行中",
            ],
        )
        info = extract_event_info(text)
        self.assertIsNotNone(info)
        self.assertEqual(info["notice_type"], "设备变更")
        self.assertEqual(info["title"], "EA118机房C楼蓄电池放电变更")

    def test_repair_notice_text_contract(self):
        text = MaintenancePortalService.build_repair_notice_text(
            status="结束",
            title="EA118机房B楼恒湿机检修",
            specialty="暖通",
            level="低",
            start_time="",
            end_time="",
            location="B-418",
            content="",
            reason="循环泵故障",
            impact="无业务影响",
            progress="设备恢复正常",
            repair_device="B-418-HUM-01",
            repair_fault="循环泵",
            fault_type="设备故障",
            repair_mode="自维",
            discovery="巡检发现",
            symptom="加湿异常",
            solution="更换循环泵",
            spare_parts="无",
            fault_time="2026-06-12 10:44",
            expected_time="2026-06-12 23:30",
        )
        self._assert_lines(
            text,
            [
                "【设备检修】状态：结束",
                "【标题】EA118机房B楼恒湿机检修",
                "【地点】B-418",
                "【紧急程度】低",
                "【专业】暖通",
                "【发现故障时间】2026-06-12 10:44",
                "【期望完成时间】2026-06-12 23:30",
                "【维修设备】B-418-HUM-01",
                "【维修故障】循环泵",
                "【故障类型】设备故障",
                "【维修方式】自维",
                "【影响范围】无业务影响",
                "【故障发现方式】巡检发现",
                "【故障现象】加湿异常",
                "【故障原因】循环泵故障",
                "【解决方案】更换循环泵",
                "【备件更换情况】无",
                "【完成情况】设备恢复正常",
            ],
        )

    def test_change_qt_upload_payload_defaults_to_i3_when_level_missing(self):
        payload = PortalRuntime._prepared_to_notice_payload(
            {
                "notice_type": "设备变更",
                "text": (
                    "【设备变更】状态：开始\n"
                    "【名称】EA118机房A楼测试变更\n"
                    "【时间】2026-06-12 09:00~2026-06-12 18:00"
                ),
            }
        )
        self.assertEqual(payload.level, "I3")

    def test_change_qt_upload_payload_respects_text_level(self):
        payload = PortalRuntime._prepared_to_notice_payload(
            {
                "notice_type": "设备变更",
                "text": (
                    "【设备变更】状态：开始\n"
                    "【名称】EA118机房A楼测试变更\n"
                    "【等级】中风险\n"
                    "【时间】2026-06-12 09:00~2026-06-12 18:00"
                ),
            }
        )
        self.assertEqual(payload.level, "I2")

    def test_message_batch_does_not_merge_all_scope_maintenance(self):
        original_service = PortalRuntime.service

        class DummyService:
            def get_job(self, job_id):
                return {
                    "job_id": job_id,
                    "request": {
                        "work_type": "maintenance",
                        "scope": "ALL",
                    },
                }

        try:
            PortalRuntime.service = DummyService()
            self.assertEqual(
                PortalRuntime._collect_message_batch_job_ids("job_primary"),
                ["job_primary"],
            )
        finally:
            PortalRuntime.service = original_service

    def test_simple_notice_text_contracts_do_not_include_backend_only_fields(self):
        cases = [
            (
                WORK_TYPE_POWER,
                "开始",
                {
                    "title": "EA118机房E楼上电通告",
                    "cabinet": "E-201 B01",
                    "quantity": "2",
                    "progress": "准备上电",
                },
                [
                    "【上电通告】状态：开始",
                    "【名称】EA118机房E楼上电通告",
                    "【时间】2026-06-12 09:00~2026-06-12 18:00",
                    "【柜号】E-201 B01",
                    "【数量】2",
                    "【进度】准备上电",
                ],
            ),
            (
                WORK_TYPE_POLLING,
                "更新",
                {
                    "title": "EA118机房B楼制冷单元轮巡通告",
                    "device": "B-127制冷单元",
                    "content": "3号轮巡至2号运行",
                    "impact": "无业务影响",
                    "progress": "轮巡中",
                },
                [
                    "【设备轮巡】状态：更新",
                    "【标题】EA118机房B楼制冷单元轮巡通告",
                    "【时间】2026-06-12 09:00~2026-06-12 18:00",
                    "【设备】B-127制冷单元",
                    "【内容】3号轮巡至2号运行",
                    "【影响】无业务影响",
                    "【进度】轮巡中",
                ],
            ),
            (
                WORK_TYPE_ADJUST,
                "结束",
                {
                    "title": "EA118机房D楼空调调整通告",
                    "location": "D-440",
                    "content": "调整空调参数",
                    "reason": "环境优化",
                    "impact": "无业务影响",
                    "progress": "调整完成",
                },
                [
                    "【设备调整】状态：结束",
                    "【名称】EA118机房D楼空调调整通告",
                    "【时间】2026-06-12 09:00~2026-06-12 18:00",
                    "【位置】D-440",
                    "【内容】调整空调参数",
                    "【原因】环境优化",
                    "【影响】无业务影响",
                    "【进度】调整完成",
                ],
            ),
        ]
        for work_type, status, values, expected in cases:
            with self.subTest(work_type=work_type):
                text = MaintenancePortalService.build_simple_notice_text(
                    work_type=work_type,
                    status=status,
                    title=values.get("title", ""),
                    start_time="2026-06-12 09:00",
                    end_time="2026-06-12 18:00",
                    building="D楼",
                    specialty="电气",
                    location=values.get("location", ""),
                    content=values.get("content", ""),
                    reason=values.get("reason", ""),
                    impact=values.get("impact", ""),
                    progress=values.get("progress", ""),
                    device=values.get("device", ""),
                    cabinet=values.get("cabinet", ""),
                    quantity=values.get("quantity", ""),
                )
                self._assert_lines(text, expected)
                self.assertNotIn("【楼栋】", text)
                self.assertNotIn("【专业】", text)
                self.assertNotIn("【维保周期】", text)
                self.assertNotIn("record_id", text)
                self.assertNotIn("target_record_id", text)
                self.assertNotIn("source_record_id", text)

    def test_all_notice_templates_use_unified_time_separator(self):
        self.assertEqual(NOTICE_TIME_SEPARATOR, "~")

    def test_target_candidate_match_reason_is_readable(self):
        reason = MaintenancePortalService._target_candidate_match_reason(
            date_matched=True,
            title_matched=True,
            business_match_count=2,
            building_matched=True,
            status="进行中",
        )
        self.assertIn("时间匹配", reason)
        self.assertIn("标题匹配", reason)
        self.assertIn("内容字段匹配 2 项", reason)
        self.assertIn("楼栋匹配", reason)
        self.assertIn("未结束优先", reason)

    def test_candidate_result_meta_distinguishes_returned_and_total(self):
        visible, meta = MaintenancePortalService._candidate_result_meta(
            [{"record_id": str(index)} for index in range(5)],
            limit=3,
        )
        self.assertEqual(len(visible), 3)
        self.assertEqual(meta["count"], 3)
        self.assertEqual(meta["returned_count"], 3)
        self.assertEqual(meta["total_matched"], 5)
        self.assertEqual(meta["limit"], 3)
        self.assertTrue(meta["limited"])
        for work_type in (
            WORK_TYPE_MAINTENANCE,
            WORK_TYPE_CHANGE,
            WORK_TYPE_POWER,
            WORK_TYPE_POLLING,
            WORK_TYPE_ADJUST,
        ):
            with self.subTest(work_type=work_type):
                fields = dict(NOTICE_TEXT_TEMPLATES[work_type]["fields"])
                self.assertEqual(fields.get("时间"), "time_range")
        repair_labels = [label for label, _ in NOTICE_TEXT_TEMPLATES[WORK_TYPE_REPAIR]["fields"]]
        self.assertIn("发现故障时间", repair_labels)
        self.assertIn("期望完成时间", repair_labels)
        self.assertNotIn("时间", repair_labels)


if __name__ == "__main__":
    unittest.main()

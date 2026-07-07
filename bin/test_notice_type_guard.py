import sys
import tempfile
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    MaintenancePortalService,
    NOTICE_TYPE_ADJUST,
    NOTICE_TYPE_CHANGE,
    NOTICE_TYPE_MAINTENANCE,
    NOTICE_TYPE_POLLING,
    NOTICE_TYPE_POWER,
    NOTICE_TYPE_REPAIR,
    PortalError,
    WORK_TYPE_ADJUST,
    WORK_TYPE_CHANGE,
    WORK_TYPE_MAINTENANCE,
    WORK_TYPE_POLLING,
    WORK_TYPE_POWER,
    WORK_TYPE_REPAIR,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class NoticeTypeGuardTests(unittest.TestCase):
    def _service(self, tmp: str) -> MaintenancePortalService:
        service = MaintenancePortalService(app_token="", table_id="")
        service._state_store = LanPortalStateStore(Path(tmp) / "state.sqlite3")
        service._jobs = {}
        return service

    def test_manual_notice_rejects_obvious_type_conflict(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._service(tmp)
            with self.assertRaisesRegex(PortalError, "像是变更通告"):
                service.create_action_job(
                    {
                        "manual": True,
                        "manual_id": "manual-adjust-bad",
                        "scope": "C",
                        "action": "start",
                        "work_type": "adjust",
                        "title": "EA118机房C楼蓄电池放电变更通告",
                        "operation_id": "manual-adjust-bad:start",
                    }
                )

    def test_manual_notice_accepts_matching_simple_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._service(tmp)
            job_id, should_start = service.create_action_job(
                {
                    "manual": True,
                    "manual_id": "manual-adjust-ok",
                    "scope": "C",
                    "action": "start",
                    "work_type": "adjust",
                    "title": "EA118机房C楼空调模式调整通告",
                    "operation_id": "manual-adjust-ok:start",
                }
            )
            self.assertTrue(job_id)
            self.assertTrue(should_start)

    def test_notice_type_is_normalized_from_work_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._service(tmp)
            job_id, _ = service.create_action_job(
                {
                    "manual": True,
                    "manual_id": "manual-adjust-normalize",
                    "scope": "C",
                    "action": "start",
                    "work_type": "adjust",
                    "notice_type": "维保通告",
                    "title": "EA118机房C楼空调模式调整通告",
                    "operation_id": "manual-adjust-normalize:start",
                }
            )
            job = service.get_job(job_id)
            self.assertEqual(job["request"]["work_type"], "adjust")
            self.assertEqual(job["request"]["notice_type"], "设备调整")

    def test_manual_notice_extracts_title_from_text_for_conflict_guard(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._service(tmp)
            with self.assertRaisesRegex(PortalError, "像是轮巡通告"):
                service.create_action_job(
                    {
                        "manual": True,
                        "manual_id": "manual-power-bad",
                        "scope": "B",
                        "action": "start",
                        "work_type": "power",
                        "text": "【设备轮巡】状态：开始\n【标题】EA118机房B楼冷站轮巡通告",
                        "operation_id": "manual-power-bad:start",
                    }
                )

    def test_explicit_maintenance_does_not_silently_become_adjust(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._service(tmp)
            with self.assertRaisesRegex(PortalError, "像是调整通告"):
                service.create_action_job(
                    {
                        "manual": True,
                        "manual_id": "manual-maintenance-bad",
                        "scope": "D",
                        "action": "start",
                        "work_type": "maintenance",
                        "title": "EA118机房D楼空调模式调整通告",
                        "operation_id": "manual-maintenance-bad:start",
                    }
                )

    def test_missing_work_type_infers_text_type_and_uses_semantic_target_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._service(tmp)
            job_id, should_start = service.create_action_job(
                {
                    "manual": True,
                    "manual_id": "manual-legacy-adjust",
                    "scope": "D",
                    "action": "start",
                    "title": "EA118机房D楼空调模式调整通告",
                    "operation_id": "manual-legacy-adjust:start",
                }
            )
            self.assertTrue(job_id)
            self.assertTrue(should_start)
            job = service.get_job(job_id)
            self.assertTrue(job["target_key"].startswith("adjust:manual-start:"))
            self.assertNotIn("manual-legacy-adjust", job["target_key"])

    def test_missing_work_type_uses_notice_type_and_semantic_target_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._service(tmp)
            job_id, should_start = service.create_action_job(
                {
                    "manual": True,
                    "manual_id": "manual-legacy-notice-type",
                    "scope": "D",
                    "action": "start",
                    "notice_type": "设备调整",
                    "title": "EA118机房D楼空调模式调整通告",
                    "operation_id": "manual-legacy-notice-type:start",
                }
            )
            self.assertTrue(should_start)
            job = service.get_job(job_id)
            self.assertEqual(job["request"]["work_type"], "adjust")
            self.assertEqual(job["request"]["notice_type"], "设备调整")
            self.assertTrue(job["target_key"].startswith("adjust:manual-start:"))
            self.assertNotIn("manual-legacy-notice-type", job["target_key"])

    def test_missing_work_type_update_target_key_uses_notice_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._service(tmp)
            job_id, _ = service.create_action_job(
                {
                    "manual": True,
                    "scope": "C",
                    "action": "update",
                    "notice_type": "变更通告",
                    "target_record_id": "rec-change-1",
                    "title": "EA118机房C楼蓄电池放电变更通告",
                    "operation_id": "legacy-change-update:update",
                }
            )
            job = service.get_job(job_id)
            self.assertEqual(job["request"]["work_type"], "change")
            self.assertEqual(job["request"]["notice_type"], "变更通告")
            self.assertEqual(job["target_key"], "change:target:rec-change-1")

    def test_manual_notice_type_guard_rejects_cross_type_keywords_for_all_types(self):
        cases = [
            (WORK_TYPE_MAINTENANCE, "EA118机房A楼蓄电池放电变更通告", "变更通告"),
            (WORK_TYPE_CHANGE, "EA118机房A楼过滤网维护通告", "维保通告"),
            (WORK_TYPE_REPAIR, "EA118机房B楼制冷单元轮巡通告", "轮巡通告"),
            (WORK_TYPE_POWER, "EA118机房C楼空调模式调整通告", "调整通告"),
            (WORK_TYPE_POLLING, "EA118机房D楼设备上电通告", "上电通告"),
            (WORK_TYPE_ADJUST, "EA118机房E楼恒湿机检修通告", "检修通告"),
        ]
        for expected_work_type, title, expected_error in cases:
            with self.subTest(work_type=expected_work_type, title=title):
                with tempfile.TemporaryDirectory() as tmp:
                    service = self._service(tmp)
                    with self.assertRaisesRegex(PortalError, expected_error):
                        service.create_action_job(
                            {
                                "manual": True,
                                "manual_id": f"manual-{expected_work_type}-bad",
                                "scope": "A",
                                "action": "start",
                                "work_type": expected_work_type,
                                "title": title,
                                "operation_id": f"manual-{expected_work_type}-bad:start",
                            }
                        )

    def test_explicit_work_type_always_controls_notice_type_for_manual_jobs(self):
        cases = [
            (WORK_TYPE_MAINTENANCE, NOTICE_TYPE_MAINTENANCE, "EA118机房A楼过滤网维护通告"),
            (WORK_TYPE_CHANGE, NOTICE_TYPE_CHANGE, "EA118机房A楼蓄电池放电变更通告"),
            (WORK_TYPE_REPAIR, NOTICE_TYPE_REPAIR, "EA118机房B楼恒湿机检修通告"),
            (WORK_TYPE_POWER, "上电通告", "EA118机房E楼设备上电通告"),
            (WORK_TYPE_POLLING, NOTICE_TYPE_POLLING, "EA118机房B楼制冷单元轮巡通告"),
            (WORK_TYPE_ADJUST, NOTICE_TYPE_ADJUST, "EA118机房C楼空调模式调整通告"),
        ]
        for work_type, expected_notice_type, title in cases:
            with self.subTest(work_type=work_type):
                with tempfile.TemporaryDirectory() as tmp:
                    service = self._service(tmp)
                    job_id, should_start = service.create_action_job(
                        {
                            "manual": True,
                            "manual_id": f"manual-{work_type}-ok",
                            "scope": "A",
                            "action": "start",
                            "work_type": work_type,
                            "notice_type": NOTICE_TYPE_MAINTENANCE,
                            "title": title,
                            "operation_id": f"manual-{work_type}-ok:start",
                        }
                    )
                    self.assertTrue(should_start)
                    job = service.get_job(job_id)
                    self.assertEqual(job["request"]["work_type"], work_type)
                    self.assertEqual(job["request"]["notice_type"], expected_notice_type)


if __name__ == "__main__":
    unittest.main()

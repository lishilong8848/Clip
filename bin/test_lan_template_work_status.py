import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import MaintenancePortalService  # noqa: E402

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


class _TestMaintenancePortalService(MaintenancePortalService):
    def refresh(self) -> None:
        return

    def ensure_loaded(self) -> None:
        return


def _build_record(record_id: str, building: str, maintenance_total: str, month: str):
    return {
        "record_id": record_id,
        "raw_fields": {},
        "display_fields": {
            "楼栋": building,
            "维护总项": maintenance_total,
            "维护实施状态": "未开始",
            "计划维护月份": month,
            "专业类别": "电气",
            "维护编号": "WB-001",
            "维护项目": "例行维护",
        },
    }


class LanTemplateWorkStatusTests(unittest.TestCase):
    def _new_temp_service(self, root: Path):
        def fake_data_path(name):
            return str(root / name)

        patcher = patch(
            "lan_bitable_template_portal.portal_service.get_data_file_path",
            side_effect=fake_data_path,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        return _TestMaintenancePortalService()

    def test_successful_actions_persist_building_work_status_for_month_query(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("rec1", "A楼", "过滤网维护", "4月")]

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
            self.assertRegex(summary["completed_date"], r"^\d{4}-\d{2}-\d{2}$")
            self.assertEqual(summary["completed_date"], summary["ended_at"][:10])
            self.assertEqual([item["action"] for item in summary["actions"]], ["start", "end"])

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


if __name__ == "__main__":
    unittest.main()

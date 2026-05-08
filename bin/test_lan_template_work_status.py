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
from lan_bitable_template_portal.portal_service import WORK_TYPE_CHANGE  # noqa: E402

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


class _TestMaintenancePortalService(MaintenancePortalService):
    def refresh(self) -> None:
        return

    def ensure_loaded(self) -> None:
        return


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


def _build_change_record(
    record_id: str,
    *,
    building: str,
    progress: str,
    title: str = "测试变更",
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
            "变更开始日期（阿里）": "2026-05-08 09:00",
            "变更结束日期（阿里）": "2026-05-08 18:00",
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
        patcher.start()
        self.addCleanup(patcher.stop)
        return service_cls()

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

    def test_change_records_use_precise_scope_and_source_ongoing_is_readonly(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("m1", "A楼", "过滤网维护", "5月")]
            service._change_records = [
                _build_change_record("c1", building="A楼", progress="未开始", title="单楼变更"),
                _build_change_record("c2", building="A楼、B楼", progress="未开始", title="园区变更"),
                _build_change_record("c3", building="A楼、B楼", progress="进行中", title="进行中园区变更"),
                _build_change_record("c4", building="A楼、B楼", progress="退回", title="退回变更"),
            ]

            campus = service.query_records(month="5月", scope="CAMPUS")
            self.assertEqual(
                [item["record_id"] for item in campus["records"]],
                ["c2"],
            )
            self.assertEqual(len(campus["ongoing"]), 1)
            self.assertEqual(campus["ongoing"][0]["source_record_id"], "c3")
            self.assertTrue(campus["ongoing"][0]["source_only"])
            self.assertFalse(campus["ongoing"][0]["can_update"])

            single = service.query_records(month="5月", scope="A")
            self.assertEqual(
                [item["record_id"] for item in single["records"]],
                ["m1", "c1"],
            )

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

    def test_change_source_failure_does_not_block_maintenance_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), _ChangeSourceFailureService)
            service.refresh()

            result = service.query_records(month="5月", scope="A")
            self.assertEqual([item["record_id"] for item in result["records"]], ["m1"])
            self.assertIn("变更源表同步失败", result["warnings"][0])

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
            self.assertEqual(result["scopes"]["D"]["maintenance_ongoing"], 1)
            self.assertEqual(result["scopes"]["CAMPUS"]["change_pending"], 1)
            self.assertEqual(result["scopes"]["B"]["change_ongoing"], 1)

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


if __name__ == "__main__":
    unittest.main()

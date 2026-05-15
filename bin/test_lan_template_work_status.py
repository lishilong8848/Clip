import os
import sys
import tempfile
import time
import unittest
import json
from pathlib import Path
from unittest.mock import patch

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import MaintenancePortalService  # noqa: E402
from lan_bitable_template_portal.portal_service import PortalError  # noqa: E402
from lan_bitable_template_portal.portal_service import WORK_TYPE_CHANGE  # noqa: E402
from lan_bitable_template_portal.portal_service import WORK_TYPE_REPAIR  # noqa: E402
from lan_bitable_template_portal.portal_auth import PortalAuthManager  # noqa: E402
from lan_bitable_template_portal.server import PortalHandler  # noqa: E402
from upload_event_module.config import MAINTENANCE_NOTICE_FIELDS  # noqa: E402
from upload_event_module.services.handlers.base import NoticePayload  # noqa: E402
from upload_event_module.services.handlers.maintenance_notice import (  # noqa: E402
    MaintenanceNoticeHandler,
)

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


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
            or item.get("feishu_record_id")
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
        self._records = [_build_record("m1", "A楼", "过滤网维护", "5月")]
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
        return []

    def _load_repair_fields(self):
        self._repair_field_meta_list = []
        self._repair_field_meta_by_name = {}
        return []

    def _load_repair_records(self):
        self._repair_records = []
        self._repair_loaded_once = True
        return []


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


def _build_repair_record(
    record_id: str,
    *,
    title: str = "测试检修",
    event_description: str = "",
    event_level: str = "",
    source: str = "",
    building: str = "D楼",
    specialty: str = "电气",
    started: bool = False,
    ended: bool = False,
    target_record_id: str = "",
):
    raw_fields = {}
    if target_record_id:
        raw_fields["设备检修关联"] = [
            {
                "record_ids": [target_record_id],
                "table_id": "tblSA9euoote8aCA",
                "text": f"{title}-目标",
                "type": "text",
            }
        ]
    return {
        "record_id": record_id,
        "raw_fields": raw_fields,
        "work_type": "repair",
        "notice_type": "设备检修",
        "source_app_token": "AnEBwJlvGiJfDdkOB32cUPuknzg",
        "source_table_id": "tblschT48zXwigUG",
        "display_fields": {
            "维修名称": title,
            "事件描述": event_description,
            "对应事件等级": event_level,
            "对应来源": source,
            "所属数据中心/楼栋-使用": building,
            "所属专业": specialty,
            "设备名称": "UPS",
            "设备编号": "UPS-01",
            "故障维修原因": "测试故障原因",
            "故障发生现象描述": "测试故障现象",
            "故障发生时间": "2026-05-08 08:20",
            "维修开始时间": "2026-05-08 09:00" if started else "",
            "维修结束时间": "2026-05-08 18:00" if ended else "",
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
):
    return {
        "record_id": record_id,
        "raw_fields": {},
        "work_type": "change",
        "notice_type": "设备变更",
        "source_app_token": "IrIibPkUOa6udGsMhu2cbOqhnWg",
        "source_table_id": "tblqMJvYW5dxFFfU",
        "display_fields": {
            "标题": title,
            "进度": progress,
            "变更等级": "低",
            "变更类型": "普通变更",
            "计划开始": "2026-05-08 09:00",
            "计划结束": "2026-05-08 18:00",
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

    def test_orphan_started_item_is_pruned_when_qt_and_target_record_are_gone(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), service_cls=_TargetLookupService)
            service._records = [_build_record("rec1", "A楼", "过滤网维护", "5月")]

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
            result = service.query_records(month="5月", scope="A", ongoing_items=[])
            daily = service.get_daily_summary(scope="A", ongoing_items=[])

            self.assertEqual(removed["removed"], 1)
            self.assertEqual(result["records"][0]["work_summary"], {})
            self.assertEqual(daily["items"], [])

    def test_started_item_is_kept_when_target_record_still_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), service_cls=_TargetLookupService)
            service._records = [_build_record("rec1", "A楼", "过滤网维护", "5月")]
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
            result = service.query_records(month="5月", scope="A", ongoing_items=[])

            self.assertEqual(removed["removed"], 0)
            self.assertEqual(result["records"][0]["work_summary"]["status"], "进行中")

    def test_deleted_ongoing_item_clears_today_summary_and_work_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("rec1", "A楼", "过滤网维护", "5月")]

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
            result = service.query_records(month="5月", scope="A", ongoing_items=[])
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

    def test_maintenance_source_ongoing_is_displayed_and_updates_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [
                _build_record("m-running", "A楼", "过滤网维护", "5月", status="进行中")
            ]
            service._upsert_work_status_item_locked(
                {
                    "source_record_id": "m-running",
                    "work_type": "maintenance",
                    "notice_type": "维保通告",
                    "target_record_id": "target-m-running",
                    "feishu_record_id": "target-m-running",
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

            result = service.query_records(month="5月", scope="A", ongoing_items=[])
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

    def test_change_records_use_precise_scope_and_allowed_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("m1", "A楼", "过滤网维护", "5月")]
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
                campus = service.query_records(month="5月", scope="CAMPUS")
            self.assertEqual(
                [item["record_id"] for item in campus["records"]],
                ["c2", "c3"],
            )
            self.assertEqual(campus["ongoing"], [])

            with patch.object(
                service, "_target_records_for_notice_type", return_value=[]
            ):
                single = service.query_records(month="5月", scope="A")
            self.assertEqual(
                [item["record_id"] for item in single["records"]],
                ["m1", "c1"],
            )

    def test_change_start_rejects_source_progress_that_is_not_not_started(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._change_records = [
                _build_change_record("c-e", building="E楼", progress="进行中", title="E楼进行中变更")
            ]

            result = service.query_records(month="5月", scope="E")
            self.assertEqual([item["record_id"] for item in result["records"]], ["c-e"])

            job_id, should_start = service.create_action_job(
                {
                    "action": "start",
                    "work_type": "change",
                    "scope": "E",
                    "record_id": "c-e",
                    "operation_id": "change-e-start",
                }
            )
            self.assertTrue(should_start)
            with self.assertRaises(PortalError):
                service.prepare_action_job(job_id)

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

    def test_repair_records_filter_placeholders_and_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record("r1", building="D楼", title="D楼UPS检修"),
                _build_repair_record("r2", building="D楼", title="——"),
                _build_repair_record("r3", building="CMDB唯一ID关联", title="无法识别楼栋"),
            ]

            result = service.query_records(month="5月", scope="D")

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
            self.assertEqual(prepared["recipients"], [])
            self.assertTrue(prepared["skip_personal_message"])
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

    def test_repair_defaults_use_event_description_level_and_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record(
                    "r1",
                    building="D楼",
                    title="维修名称不作为标题",
                    event_description="事件描述作为检修标题",
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
            self.assertEqual(prepared["title"], "事件描述作为检修标题")
            self.assertEqual(prepared["level"], "中")
            self.assertEqual(prepared["discovery"], "监控发现")
            self.assertEqual(service._repair_level_from_event_level("I3"), "低")
            self.assertEqual(service._repair_level_from_event_level("I1"), "")
            self.assertIn("【标题】事件描述作为检修标题", prepared["text"])
            self.assertIn("【紧急程度】中", prepared["text"])
            self.assertIn("【故障发现方式】监控发现", prepared["text"])

    def test_started_repair_source_record_is_displayed_as_update_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._repair_records = [
                _build_repair_record(
                    "r1",
                    building="D楼",
                    title="D楼UPS检修",
                    started=True,
                    target_record_id="target-r1",
                )
            ]

            result = service.query_records(month="5月", scope="D")

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
                    target_record_id="target-r1",
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
            self.assertEqual(prepared["target_record_id"], "target-r1")
            self.assertEqual(prepared["source_progress"], "进行中")

    def test_change_source_failure_does_not_block_maintenance_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp), _ChangeSourceFailureService)
            service.refresh()

            result = service.query_records(month="5月", scope="A")
            self.assertEqual([item["record_id"] for item in result["records"]], ["m1"])
            self.assertIn("变更源表同步失败", result["warnings"][0])

    def test_start_action_uses_cached_records_without_forced_refresh(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service._records = [_build_record("rec1", "A楼", "过滤网维护", "5月")]

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
        handler = object.__new__(PortalHandler)

        def fail_ongoing(scope):
            raise RuntimeError(f"{scope} unavailable")

        try:
            PortalHandler.ongoing_callback = fail_ongoing
            result = handler._get_ongoing("ALL")
            payload = PortalHandler._with_runtime_warnings({"warnings": []})
        finally:
            PortalHandler.ongoing_callback = None
            PortalHandler.last_ongoing_error = ""

        self.assertEqual(result, [])
        self.assertIn("主界面进行中状态读取失败", payload["warnings"][0])

    def test_work_status_reads_are_cached_by_file_signature(self):
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

            self.assertGreaterEqual(reload_mock.call_count, 1)
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
                self.assertEqual(service._source_cache_ttl_seconds(), 300)
                self.assertTrue(service._source_cache_expired())
            with patch(
                "lan_bitable_template_portal.portal_service.config.lan_template_source_cache_ttl_seconds",
                120,
                create=True,
            ):
                self.assertEqual(service._source_cache_ttl_seconds(), 120)

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

            self.assertEqual([record["record_id"] for record in a_records], ["z-a", "z-all"])
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
                        "notice_type": "设备变更",
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
                    "notice_type": "设备变更",
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

    def test_hidden_ongoing_uses_active_item_identity_for_qt_items(self):
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            service.hide_ongoing_item(
                {
                    "scope": "C",
                    "work_type": WORK_TYPE_CHANGE,
                    "notice_type": "设备变更",
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
                        "notice_type": "设备变更",
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
                        "notice_type": "设备变更",
                        "record_id": "target-a",
                        "title": "A楼变更",
                        "building": "A楼",
                        "building_codes": ["A"],
                    },
                    scope="C",
                )

            hidden_file = Path(tmp) / "lan_template_hidden_ongoing.json"
            self.assertFalse(hidden_file.exists())

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
                            "notice_type": "设备变更",
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

    def test_portal_auth_reloads_permissions_and_backs_up_corrupt_file(self):
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
                self.assertEqual(manager.session_scopes(session), ["B"])

                permission_path.write_text("{bad json", encoding="utf-8")
                self.assertEqual(manager.scopes_for_open_id("ou_unknown"), [])
                self.assertTrue(permission_path.exists())
                self.assertTrue(list(root.glob("lan_portal_auth.bad.*.json")))

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
            PortalHandler._safe_host_value("127.0.0.1:18766", "fallback:1"),
            "127.0.0.1:18766",
        )
        self.assertEqual(
            PortalHandler._safe_host_value("[::1]:18766", "fallback:1"),
            "[::1]:18766",
        )
        self.assertEqual(
            PortalHandler._safe_host_value("evil.example\r\nX-Test: 1", "fallback:1"),
            "fallback:1",
        )
        self.assertEqual(
            PortalHandler._safe_host_value("evil.example/path", "fallback:1"),
            "fallback:1",
        )
        self.assertEqual(
            PortalHandler._safe_host_value("evil.example@127.0.0.1", "fallback:1"),
            "fallback:1",
        )
        self.assertEqual(
            PortalHandler._safe_host_value("0.0.0.0:18766", "fallback:1"),
            "fallback:1",
        )
        self.assertEqual(PortalHandler._safe_proto_value("https, http"), "https")
        self.assertEqual(PortalHandler._safe_proto_value("javascript"), "http")

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


if __name__ == "__main__":
    unittest.main()

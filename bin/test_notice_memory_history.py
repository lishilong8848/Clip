import tempfile
import unittest
import datetime as dt
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
import sys

if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    MaintenancePortalService,
    NOTICE_TYPE_CHANGE,
    NOTICE_TYPE_MAINTENANCE,
    NOTICE_TYPE_REPAIR,
    WORK_TYPE_CHANGE,
    WORK_TYPE_MAINTENANCE,
    WORK_TYPE_REPAIR,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class _HistoryMemoryService(MaintenancePortalService):
    def __init__(self, db_path):
        super().__init__()
        self._state_store = LanPortalStateStore(db_path)
        self.table_records = {}
        self.target_records = {}
        self.source_records = []

    def _history_target_config(self, work_type: str) -> dict[str, str]:
        return {
            "work_type": work_type,
            "notice_type": {
                WORK_TYPE_MAINTENANCE: NOTICE_TYPE_MAINTENANCE,
                WORK_TYPE_CHANGE: NOTICE_TYPE_CHANGE,
                WORK_TYPE_REPAIR: NOTICE_TYPE_REPAIR,
            }[work_type],
            "app_token": "app",
            "table_id": work_type,
        }

    def _load_table_fields(self, *, app_token: str, table_id: str):
        return [], {}

    def _load_fields(self):
        self._field_meta_by_name = {}

    def _load_change_fields(self):
        self._change_field_meta_by_name = {}

    def _load_table_records(self, *, app_token: str, table_id: str, meta_by_name, work_type: str, notice_type: str):
        if table_id != work_type:
            return [
                record
                for record in self.source_records
                if record.get("work_type") == work_type
            ]
        return list(self.table_records.get(work_type, []))

    def _source_snapshot_records(self, scope: str):
        return list(self.source_records)

    def _target_records_for_notice_type(self, notice_type: str, work_type: str, *, force_refresh: bool = False):
        return list(self.target_records.get(work_type, []))


def _source_record(record_id, work_type, fields):
    return {
        "record_id": record_id,
        "work_type": work_type,
        "notice_type": {
            WORK_TYPE_MAINTENANCE: NOTICE_TYPE_MAINTENANCE,
            WORK_TYPE_CHANGE: NOTICE_TYPE_CHANGE,
            WORK_TYPE_REPAIR: NOTICE_TYPE_REPAIR,
        }[work_type],
        "display_fields": fields,
    }


def _target_record(record_id, work_type, fields):
    return {
        "record_id": record_id,
        "work_type": work_type,
        "display_fields": fields,
        "created_time": dt.datetime.now().strftime("%Y-%m-10 08:00"),
    }


class NoticeMemoryHistoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = _HistoryMemoryService(Path(self.tmp.name) / "state.sqlite3")

    def tearDown(self):
        self.tmp.cleanup()

    def test_maintenance_history_title_suffix_matches_source_item(self):
        month_label = self.service._current_month_label()
        date_text = dt.datetime.now().strftime("%Y-%m-10 09:30")
        self.service.source_records = [
            _source_record(
                "src1",
                WORK_TYPE_MAINTENANCE,
                {
                    "楼栋": "110站",
                    "维护总项": "EA118-110KV阿里中天变变电站安全工器具检查月度维护",
                    "维护周期": "每月",
                    "维护实施状态": "未开始",
                    "计划维护月份": month_label,
                    "专业类别": "电气",
                },
            )
        ]
        self.service.table_records[WORK_TYPE_MAINTENANCE] = [
            _target_record(
                "his1",
                WORK_TYPE_MAINTENANCE,
                {
                    "名称": "EA118-110KV阿里中天变变电站安全工器具检查月度维护通告",
                    "楼栋": "110站",
                    "维保周期": "每月",
                    "专业": "电气",
                    "位置": "110站",
                    "内容": "检查安全工器具",
                    "原因": "月度维护",
                    "影响": "无影响",
                    "进度": "准备完成",
                    "计划开始时间": date_text,
                },
            )
        ]

        payload = self.service.scan_historical_notice_memory_candidates(
            work_types=[WORK_TYPE_MAINTENANCE],
            months=3,
        )

        self.assertEqual(payload["counts"]["source"], 1)
        self.assertEqual(payload["counts"]["candidates"], 1)
        self.assertEqual(payload["counts"]["recommended"], 1)
        match = payload["matches"][0]
        self.assertTrue(match["selected"])
        self.assertGreaterEqual(match["score"], 80)

    def test_save_selected_match_writes_memory_with_progress(self):
        match = {
            "selected": True,
            "candidate_id": "maintenance:his1",
            "source_item": {
                "work_type": WORK_TYPE_MAINTENANCE,
                "building": "A楼",
                "memory_name": "过滤网维护",
                "maintenance_cycle": "每月",
            },
            "fields": {
                "location": "A楼机房",
                "content": "清洁过滤网",
                "reason": "月度维护",
                "impact": "无影响",
                "progress": "准备完成",
                "specialty": "暖通",
            },
        }

        result = self.service.save_historical_notice_memory_matches(
            matches=[match],
            imported_by="tester",
        )

        self.assertEqual(result["saved_count"], 1)
        memory = self.service._get_record_memory(
            _source_record(
                "src1",
                WORK_TYPE_MAINTENANCE,
                {"楼栋": "A楼", "维护总项": "过滤网维护", "维护周期": "每月"},
            )
        )
        self.assertEqual(memory["location"], "A楼机房")
        self.assertEqual(memory["progress"], "准备完成")
        self.assertEqual(memory["specialty"], "暖通")

    def test_scan_source_item_carries_existing_memory_and_current_fields(self):
        month_label = self.service._current_month_label()
        self.service.source_records = [
            _source_record(
                "src1",
                WORK_TYPE_MAINTENANCE,
                {
                    "楼栋": "A楼",
                    "维护总项": "过滤网维护",
                    "维护周期": "每月",
                    "维护实施状态": "未开始",
                    "计划维护月份": month_label,
                    "专业类别": "暖通",
                    "内容": "源表内容",
                    "原因": "源表原因",
                },
            )
        ]
        self.service._remember_draft_fields(
            work_type=WORK_TYPE_MAINTENANCE,
            building="A楼",
            maintenance_total="过滤网维护",
            item_name="过滤网维护",
            maintenance_cycle="每月",
            location="记忆位置",
            content="记忆内容",
            reason="记忆原因",
            impact="记忆影响",
            extra_fields={"specialty": "暖通", "progress": "记忆进度"},
        )

        payload = self.service.scan_historical_notice_memory_candidates(
            work_types=[WORK_TYPE_MAINTENANCE],
            months=3,
        )

        self.assertEqual(payload["counts"]["source"], 1)
        source = payload["source_items"][0]
        self.assertEqual(source["memory"]["content"], "记忆内容")
        self.assertEqual(source["memory"]["progress"], "记忆进度")
        self.assertEqual(source["current_fields"]["content"], "源表内容")
        self.assertEqual(source["current_fields"]["reason"], "源表原因")

    def test_history_source_items_include_finished_current_month_records(self):
        month_label = self.service._current_month_label()
        self.service.source_records = [
            _source_record(
                "src_maintenance_done",
                WORK_TYPE_MAINTENANCE,
                {
                    "楼栋": "A楼",
                    "维护总项": "已结束维保",
                    "维护周期": "每月",
                    "维护实施状态": "已结束",
                    "计划维护月份": month_label,
                    "专业类别": "暖通",
                },
            ),
            _source_record(
                "src_change_done",
                WORK_TYPE_CHANGE,
                {
                    "变更简述": "A楼已结束变更",
                    "变更楼栋": "A楼",
                    "变更进度": "已结束",
                    "变更开始日期（阿里）": dt.datetime.now().strftime("%Y-%m-10 09:30"),
                    "专业": "弱电",
                },
            ),
        ]

        payload = self.service.scan_historical_notice_memory_candidates(
            work_types=[WORK_TYPE_MAINTENANCE, WORK_TYPE_CHANGE],
            months=3,
        )

        titles = {item["memory_name"] for item in payload["source_items"]}
        self.assertIn("已结束维保", titles)
        self.assertIn("A楼已结束变更", titles)
        status_by_title = {item["memory_name"]: item["source_status"] for item in payload["source_items"]}
        self.assertEqual(status_by_title["已结束维保"], "已结束")
        self.assertEqual(status_by_title["A楼已结束变更"], "已结束")

    def test_history_source_items_include_current_month_repair_even_if_ended(self):
        self.service.source_records = [
            _source_record(
                "src_repair_done",
                WORK_TYPE_REPAIR,
                {
                    "检修通告名称": "A楼冷机检修",
                    "所属数据中心/楼栋-使用": "A楼",
                    "维修开始时间": dt.datetime.now().strftime("%Y-%m-10 09:30"),
                    "维修结束时间": dt.datetime.now().strftime("%Y-%m-10 18:30"),
                    "所属专业": "暖通",
                },
            )
        ]

        payload = self.service.scan_historical_notice_memory_candidates(
            work_types=[WORK_TYPE_REPAIR],
            months=3,
        )

        self.assertEqual(payload["counts"]["source"], 1)
        source = payload["source_items"][0]
        self.assertEqual(source["memory_name"], "A楼冷机检修")
        self.assertEqual(source["source_status"], "已结束")

    def test_history_source_items_ignore_candidate_work_type_filter(self):
        month_label = self.service._current_month_label()
        self.service.source_records = [
            _source_record(
                "src_maintenance",
                WORK_TYPE_MAINTENANCE,
                {
                    "楼栋": "A楼",
                    "维护总项": "A楼维保",
                    "维护周期": "每月",
                    "维护实施状态": "未开始",
                    "计划维护月份": month_label,
                },
            ),
            _source_record(
                "src_change",
                WORK_TYPE_CHANGE,
                {
                    "变更简述": "A楼变更",
                    "变更楼栋": "A楼",
                    "变更进度": "未开始",
                    "变更开始日期（阿里）": dt.datetime.now().strftime("%Y-%m-10 09:30"),
                },
            ),
        ]

        payload = self.service.scan_historical_notice_memory_candidates(
            work_types=[WORK_TYPE_MAINTENANCE],
            months=3,
        )

        titles = {item["memory_name"] for item in payload["source_items"]}
        self.assertIn("A楼维保", titles)
        self.assertIn("A楼变更", titles)

    def test_lookup_change_target_candidates_by_name_and_time(self):
        self.service.target_records[WORK_TYPE_CHANGE] = [
            _target_record(
                "chg_target_1",
                WORK_TYPE_CHANGE,
                {
                    "名称": "A楼网络设备变更",
                    "楼栋": "A楼",
                    "变更状态": "进行中",
                    "变更开始时间": "2026-05-27 09:30",
                    "变更结束时间": "2026-05-27 18:30",
                },
            ),
            _target_record(
                "chg_target_other",
                WORK_TYPE_CHANGE,
                {
                    "名称": "A楼其他变更",
                    "楼栋": "A楼",
                    "变更状态": "进行中",
                    "变更开始时间": "2026-05-27 09:30",
                    "变更结束时间": "2026-05-27 18:30",
                },
            ),
        ]

        result = self.service.lookup_change_target_candidates(
            scope="A",
            title="A楼网络设备变更",
            start_time="2026-05-27T09:30",
            end_time="2026-05-27T18:30",
            action="update",
        )

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["candidates"][0]["record_id"], "chg_target_1")


if __name__ == "__main__":
    unittest.main()

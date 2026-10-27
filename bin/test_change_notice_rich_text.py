"""Offline regressions for the first change-notice read and shared send text."""
import copy
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal import portal_service as portal
from lan_bitable_template_portal.notice_alert_tags import NoticeAlertTags
from lan_bitable_template_portal.notice_panel_data import ongoing_item, submission
from lan_bitable_template_portal.planned_notices import prefill_record
from lan_bitable_template_portal.workbench_lite import _draft_from_record
from upload_event_module.config import CHANGE_NOTICE_FIELDS
from upload_event_module.services.handlers.base import NoticePayload, notice_field_text
from upload_event_module.services.handlers.change_notice import ChangeNoticeHandler


VALUES = dict(title="EA118-E楼HVDC电池测试", level="I3", location="E楼电池室",
              content="检查电池\n记录结果", reason="计划测试", impact="无业务影响", progress="已完成一半")
TIMES = dict(start_time="2026-10-27 09:00", end_time="2026-10-27 18:00")


def rich(value):
    return [{"text": value[:3], "type": "text"}, {"text": value[3:], "type": "text"}]


class ChangeNoticeRichTextTests(unittest.TestCase):
    def setUp(self):
        self.service = object.__new__(portal.MaintenancePortalService)
        self.service._field_meta_by_name = {}
        self.service.ensure_snapshot_loaded = Mock()
        self.service._recipients_for_building_codes = Mock(return_value=("", ["offline-user"], ""))
        self.raw = {key: rich(value) for key, value in VALUES.items()}

    def assert_notice(self, notice, values=None):
        values = values or VALUES
        for label, key in portal.NOTICE_TEXT_TEMPLATES["change"]["fields"]:
            if key in values:
                self.assertIn(f"【{label}】{values[key]}", notice["text"])
        for key, value in values.items():
            if key in notice:
                self.assertEqual(notice[key], value)
        self.assertNotIn("'type': 'text'", notice["text"])

    def test_helper_preserves_runs_whitespace_repetitions_and_plain_strings(self):
        value = [{"text": "HVDC ", "type": "text"}, {"text": "A\n", "type": "text"},
                 None, {"text": None}, {"text": "A\n", "type": "text"}]
        before = copy.deepcopy(value)
        self.assertEqual(notice_field_text(value), "HVDC A\nA\n")
        self.assertEqual(value, before)
        literal = "[{'text': '用户手填示例', 'type': 'text'}]"
        self.assertEqual(notice_field_text(literal), literal)
        for empty in (None, [], {}, {"text": None}, [{"text": ""}, None]):
            self.assertEqual(notice_field_text(empty), "")
        self.assertEqual(notice_field_text({"text": "单段"}), "单段")
        self.assertEqual(notice_field_text(["A楼", "B楼"]), "A楼、B楼")

    def test_first_remote_read_preserves_multirun_text_and_select_options(self):
        for wrap in (lambda value: value, lambda value: {"type": 1, "value": value}):
            with self.subTest(wrapped=isinstance(wrap([]), dict)):
                record = self.service._normalize_record({"record_id": "rec-source", "fields": {
                    "变更简述": wrap(rich(VALUES["title"])), "内容": wrap(rich(VALUES["content"])), "楼栋": ["A楼", "B楼"]}},
                    work_type="change", notice_type="变更通告")
                self.assertEqual(record["display_fields"]["变更简述"], VALUES["title"])
                self.assertEqual(record["display_fields"]["内容"], VALUES["content"])
                self.assertEqual(record["display_fields"]["楼栋"], "A楼、B楼")

    def test_ordinary_lists_keep_head_deduplication_semantics(self):
        for values in (["A", "A", "B"], [{"name": "A"}, {"name": "A"}, {"name": "B"}]):
            with self.subTest(values=values):
                self.assertEqual(self.service._normalize_field_value("内容", values), "A、A、B")
            for key in ("value", "name", "text"):
                wrapped = {"type": 3, key: copy.deepcopy(values)}
                before = copy.deepcopy(wrapped)
                with self.subTest(key=key, values=values):
                    self.assertEqual(self.service._normalize_field_value("内容", wrapped), "A、B")
                    self.assertEqual(wrapped, before)

    def test_rich_text_runs_keep_repetitions_without_added_separators(self):
        values = [{"text": "A ", "type": "text"}, {"text": "A\n", "type": "text"},
                  {"text": "B", "type": "text"}]
        for wrapped in (values, {"type": 1, "value": values}):
            before = copy.deepcopy(wrapped)
            with self.subTest(wrapped=wrapped):
                self.assertEqual(self.service._normalize_field_value("内容", wrapped), "A A\nB")
                self.assertEqual(wrapped, before)

    def test_source_snapshot_to_shared_qt_and_web_text(self):
        fields = {"变更简述": self.raw["title"], "变更进度": "进行中", "楼栋": "E楼",
                  "位置": self.raw["location"], "变更内容": self.raw["content"],
                  "变更原因": self.raw["reason"], "变更影响": self.raw["impact"],
                  "当前进展": self.raw["progress"], "变更开始日期（阿里）": TIMES["start_time"],
                  "变更结束日期（阿里）": TIMES["end_time"]}
        projected = self.service._source_snapshot_active_payload({"record_id": "rec-source",
            "work_type": "change", "display_fields": fields})
        self.assert_notice(projected, {k: v for k, v in VALUES.items() if k != "level"})

    def test_target_form_fields_are_normalized_before_prefill(self):
        fields = {CHANGE_NOTICE_FIELDS["level_ali" if key == "level" else key]: value
                  for key, value in self.raw.items()}
        target = {"record_id": "rec-target", "display_fields": fields}
        before = copy.deepcopy(target)
        form = self.service._target_record_form_fields(work_type="change", notice_type="变更通告", target_record=target)
        for key, value in VALUES.items():
            self.assertEqual(form[key], value)
        self.assertEqual(target, before)

    def test_existing_plan_and_old_page_prefill(self):
        source = {"record_id": "rec-source", "work_type": "change", "title": self.raw["title"],
                  "display_fields": {"名称": self.raw["title"], "楼栋": "E楼",
                     **{label: self.raw[key] for label, key in portal.NOTICE_TEXT_TEMPLATES["change"]["fields"]
                        if key in self.raw}}, **TIMES}
        draft = _draft_from_record(source, work_type="change")
        for key in ("title", "location", "content", "reason", "impact", "progress"):
            self.assertEqual(draft[key], VALUES[key])
        with patch('lan_bitable_template_portal.planned_notice_recommendations.build_recommendations', return_value={"recommendations": []}):
            planned = prefill_record(self.service, source, {"title": VALUES["title"], "source_record_id": "rec-source"},
                scope="E", month="10月", work_type="change", source=source)
        self.assertEqual(planned["draft"]["content"], VALUES["content"])
        self.assertEqual(planned["draft"]["progress"], "")

    def test_new_page_preview_and_manual_progress(self):
        item = ongoing_item(self.service, "E", "change", {**self.raw, **TIMES,
            "work_type": "change", "notice_type": "变更通告", "target_record_id": "rec-target"})
        item["fields"] = []
        item["draft"]["progress"] = "现场手填进度"
        body, preview = submission(self.service, item)
        self.assert_notice({**body, "text": preview}, {**VALUES, "progress": "现场手填进度"})

    def test_send_preparation_and_second_update_have_identical_plain_fields(self):
        request = {**self.raw, **TIMES, "action": "update", "scope": "E", "work_type": "change",
                   "target_record_id": "rec-target", "building_codes": ["E"], "response_time": TIMES["start_time"]}
        before = copy.deepcopy(request)
        first = self.service.prepare_change_action(request, job_id="first")
        self.assert_notice(first)
        second = self.service.prepare_change_action({**request, **VALUES}, job_id="second")
        self.assertEqual(first["text"], second["text"])
        self.assertEqual(request, before)

    def test_render_and_final_sync_do_not_stringify_fields(self):
        text = self.service.build_change_notice_text(status="更新", **TIMES, **self.raw)
        self.assert_notice({"text": text})
        prepared = self.service._synchronize_prepared_notice_text({**self.raw, **TIMES,
            "work_type": "change", "notice_type": "变更通告", "status": "更新"})
        self.assert_notice(prepared)

    def test_empty_rich_text_uses_existing_defaults(self):
        for value in (None, [], {}, {"text": None}, {"type": 1, "value": []}, {"type": 1, "value": None}):
            with self.subTest(value=value):
                self.assertEqual(self.service._normalize_field_value("内容", value), "")
        text = self.service.build_change_notice_text(status="更新", **TIMES,
            **{key: [{"text": None, "type": "text"}] for key in VALUES})
        self.assertIn("【影响】" + portal.DEFAULT_IMPACT_TEXT, text)
        self.assertIn("【进度】" + portal.DEFAULT_PROGRESS_TEXT, text)
        self.assertIn("【名称】\n", text)

    def test_submitted_manual_fields_win_over_plan(self):
        manual = {key: "手填" + value for key, value in VALUES.items()}
        manual["content"] = "[{'text': '用户自己的示例'}]"
        draft = _draft_from_record({**self.raw, "work_type": "change", "submitted_draft": manual}, work_type="change")
        for key, value in manual.items():
            self.assertEqual(draft[key], value)
        prepared = self.service._synchronize_prepared_notice_text({**draft, **TIMES, "work_type": "change", "status": "更新"})
        self.assert_notice(prepared, manual)

    def test_qt_handler_and_recommendation_receive_same_clean_text(self):
        raw_fields = {CHANGE_NOTICE_FIELDS["level_ali" if key == "level" else key]: {"type": 1, "value": value}
                      for key, value in self.raw.items()}
        target = self.service._normalize_record({"record_id": "rec-target", "fields": raw_fields},
            work_type="change", notice_type="变更通告")
        form = self.service._target_record_form_fields(work_type="change", notice_type="变更通告", target_record=target)
        prepared = self.service.prepare_workbench_action({**form, **TIMES, "scope": "E", "target_record_id": "rec-target",
            "work_type": "change", "notice_type": "变更通告", "action": "update", "building_codes": ["E"]}, job_id="offline")
        handler = ChangeNoticeHandler()
        payload = NoticePayload(text=prepared["text"])
        fields = handler.build_update_fields(payload)
        self.assertEqual(fields[CHANGE_NOTICE_FIELDS["title"]], VALUES["title"])
        robot_text = handler.build_robot_message(payload)[1]
        self.assertIn(VALUES["content"], robot_text)
        store = SimpleNamespace(get_document=Mock(return_value=None), enqueue_outbox_event=Mock())
        sender, recommend = Mock(), Mock()
        tags = NoticeAlertTags(self.service, recommend, sender, store=store)
        tags.enqueue(prepared, operation_id="offline-operation", target_record_id="rec-target")
        documents = store.enqueue_outbox_event.call_args.kwargs["documents"]
        job = next(value for (_, key), value in documents.items() if key.startswith("job:"))
        self.assertEqual(job["title"], VALUES["title"])
        self.assertEqual(job["text"], prepared["text"])
        self.assert_notice(job)
        sender.assert_not_called()
        recommend.assert_not_called()
        self.assertEqual(handler._normalize_response_text([{"text": "1、确认\n"}, {"text": "2、更新"}]), "1、确认\n2、更新")


if __name__ == "__main__":
    unittest.main()

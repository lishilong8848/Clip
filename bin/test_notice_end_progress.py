# -*- coding: utf-8 -*-
"""Bounded ``本次进度`` end-action default tests.

Covers the assistant to-do panel (``notice_panel_data``/``notice_panel``), the
chat/manual/Feishu plan form (``lighthouse_agent._notice_form``) and the shared
field metadata (``default_on_end``).  Only native work types that already carry a
progress control (maintenance/change/repair/polling/power) are defaulted; device
adjust is never touched.  No sends/writes happen here.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant import lighthouse_notice_identity as identity
import openclaw_service.assistant.lighthouse_agent as agent
from lan_bitable_template_portal import notice_panel_data as data
from lan_bitable_template_portal.notice_panel import NoticePanel

DEFAULT = data.END_PROGRESS_DEFAULT
PROGRESS_TYPES = ("maintenance", "change", "repair", "polling", "power")


# --------------------------------------------------------------------------- #
# Native field/body builders for unit-level panel checks                      #
# --------------------------------------------------------------------------- #
def _base_draft(work):
    d = {
        "title": "测试通告", "content": "设备检查完成", "progress": "", "scope": "A",
        "work_type": work, "action": "update", "source_record_id": "src",
        "notice_action": "更新", "building_codes": ["A"],
        "start_time": "2026-10-09 09:00", "end_time": "2026-10-09 10:00",
        "specialty": "暖通", "location": "位置", "reason": "原因", "impact": "影响",
    }
    if work == "change":
        d.update(execution_party="厂维", level="低")
    elif work == "polling":
        d["device"] = "设备"
    elif work == "power":
        d.update(cabinet="柜", quantity="1", notice_type="上电通告")
    elif work == "maintenance":
        d.update(execution_party="厂维", maintenance_cycle="月度")
    elif work == "repair":
        d.update(
            level="低", repair_device="设备", repair_fault="故障", fault_type="类型",
            repair_mode="方式", discovery="发现", symptom="现象", solution="方案",
            location="地点",
        )
    return d


def _make_item(work="maintenance", progress="", notice_action="结束"):
    draft = _base_draft(work)
    if work != "adjust":
        draft["progress"] = progress
    draft["notice_action"] = notice_action
    fields = data.fields_for(work, draft, "update")
    return {"key": "k", "title": "测试通告", "action": "update",
            "draft": draft, "fields": fields}


def _mock_service():
    svc = Mock()
    svc._format_input_datetime.side_effect = lambda value: value
    svc._validate_minimum_notice_duration.return_value = None
    svc._require_end_site_photo_cumulative.return_value = None

    def sync(body):
        return {"text": "preview action=%s progress=%s" % (
            body.get("action", ""), body.get("progress", ""))}
    svc._synchronize_prepared_notice_text.side_effect = sync
    return svc


# --------------------------------------------------------------------------- #
# Chat plan form helpers (lighthouse_agent._notice_form)                      #
# --------------------------------------------------------------------------- #
def _chat_form(work, action, patch_progress="__MISSING__", row_progress="旧进度"):
    body = {
        "command_format": "notice_command", "scope": "A", "work_type": work,
        "action": action, "active_item_id": "rec-act", "target_record_id": "rec-tgt",
    }
    if patch_progress != "__MISSING__":
        body["patch"] = {"progress": patch_progress}
    row = {
        "active_item_id": "rec-act", "target_record_id": "rec-tgt",
        "source_record_id": "rec-src", "record_id": "rec-tgt", "work_type": work,
        "building_codes": ["A"], "building": "A楼", "title": "测试通告",
        "progress": row_progress, "status": "开始",
    }
    operation = {"body": body}
    queries = {"q": {"ongoing": [row]}}

    def fake_current(_actor, _body, _queries):
        return {
            "active_item_id": "rec-act", "target_record_id": "rec-tgt",
            "source_record_id": "rec-src", "record_id": "rec-tgt",
        }

    actor = {"scopes": ["A"]}
    with patch.object(identity, "current_notice_body", side_effect=fake_current):
        form = agent.PortalAgent._notice_form(actor, operation, queries, {}, 0)
    return form["_initial_form"].get("progress")


class PanelDataEndProgressTests(unittest.TestCase):
    def test_each_progress_type_defaults_on_end_blank(self):
        for work in PROGRESS_TYPES:
            with self.subTest(work=work):
                item = _make_item(work, progress="", notice_action="结束")
                data.apply_end_default(item["draft"], item)
                self.assertEqual(item["draft"]["progress"], DEFAULT)

    def test_adjust_never_exposes_or_defaults_progress(self):
        item = _make_item("adjust", notice_action="结束")
        item["draft"]["progress"] = "旧进度"
        self.assertNotIn("progress", {f["key"] for f in item["fields"]})
        data.apply_end_default(item["draft"], item)
        # Device adjust exposes no progress control: an old value stays untouched.
        self.assertEqual(item["draft"]["progress"], "旧进度")

    def test_custom_progress_preserved_on_end(self):
        for work in PROGRESS_TYPES:
            with self.subTest(work=work):
                item = _make_item(work, progress="我方已复测", notice_action="结束")
                data.apply_end_default(item["draft"], item)
                self.assertEqual(item["draft"]["progress"], "我方已复测")

    def test_blank_whitespace_is_treated_as_blank(self):
        for spacer in ("   ", "\t\n", "  "):
            with self.subTest(spacer=repr(spacer)):
                item = _make_item("maintenance", progress=spacer, notice_action="结束")
                data.apply_end_default(item["draft"], item)
                self.assertEqual(item["draft"]["progress"], DEFAULT)

    def test_fields_for_metadata_and_adjustment_exclusion(self):
        for work in PROGRESS_TYPES:
            with self.subTest(work=work):
                fields = data.fields_for(work, _base_draft(work), "update")
                progress = next(f for f in fields if f["key"] == "progress")
                self.assertEqual(progress["default_on_end"], DEFAULT)
        adjust_fields = data.fields_for("adjust", _base_draft("adjust"), "update")
        self.assertNotIn("progress", {f["key"] for f in adjust_fields})

    def test_submission_defaults_before_missing_check_and_preview(self):
        svc = _mock_service()
        for work in PROGRESS_TYPES:
            with self.subTest(work=work):
                item = _make_item(work, progress="", notice_action="结束")
                body, preview = data.submission(svc, item)
                self.assertEqual(body["action"], "end")
                self.assertEqual(body["progress"], DEFAULT)
                self.assertIn(DEFAULT, preview)

    def test_submission_keeps_custom_and_whitespace_blank_defaults(self):
        svc = _mock_service()
        custom = _make_item("maintenance", progress="自定义", notice_action="结束")
        body, _ = data.submission(svc, custom)
        self.assertEqual(body["progress"], "自定义")

        blank = _make_item("maintenance", progress="   ", notice_action="结束")
        body, _ = data.submission(svc, blank)
        self.assertEqual(body["progress"], DEFAULT)

    def test_submission_update_never_clears_default_equaling_progress(self):
        # A submission is an arbitrary update payload: a progress value that merely
        # equals the canned text must never be erased (it may be an intentional entry).
        svc = _mock_service()
        item = _make_item("maintenance", progress=DEFAULT, notice_action="更新")
        body, _ = data.submission(svc, item)
        self.assertEqual(body["progress"], DEFAULT)

    def test_apply_end_default_clears_only_real_end_to_update_transition(self):
        # Arbitrary update with no previous action preserves the default-equal value.
        item = _make_item("maintenance", progress=DEFAULT, notice_action="更新")
        data.apply_end_default(item["draft"], item)
        self.assertEqual(item["draft"]["progress"], DEFAULT)

        # A genuine 结束→更新 transition clears only the untouched canned default.
        item = _make_item("maintenance", progress=DEFAULT, notice_action="更新")
        data.apply_end_default(item["draft"], item, previous_action="结束")
        self.assertEqual(item["draft"]["progress"], "")

        # A non-结束 previous action (or update→update) never clears the value.
        item = _make_item("maintenance", progress=DEFAULT, notice_action="更新")
        data.apply_end_default(item["draft"], item, previous_action="取消")
        self.assertEqual(item["draft"]["progress"], DEFAULT)

        # Reverting a real 结束→更新 clears the default-equal value; any explicit
        # custom text is preserved by the merge path which sets a different value.
        item = _make_item("maintenance", progress=DEFAULT, notice_action="更新")
        data.apply_end_default(item["draft"], item, previous_action="结束")
        self.assertEqual(item["draft"]["progress"], "")


class ChatFormEndProgressTests(unittest.TestCase):
    def test_end_without_explicit_progress_replaces_history(self):
        # Old record progress must never be reused for an end action.
        for work in PROGRESS_TYPES:
            with self.subTest(work=work):
                self.assertEqual(_chat_form(work, "end"), DEFAULT)

    def test_end_blank_or_whitespace_patch_uses_default(self):
        for raw in ("", "   ", "\n   "):
            with self.subTest(raw=repr(raw)):
                self.assertEqual(_chat_form("maintenance", "end", raw), DEFAULT)

    def test_end_explicit_custom_progress_wins(self):
        self.assertEqual(_chat_form("maintenance", "end", "今日完成备份"), "今日完成备份")

    def test_update_keeps_original_history(self):
        # Only the end action replaces history; updates keep yesterday's text.
        self.assertEqual(_chat_form("maintenance", "update"), "旧进度")

    def test_adjust_end_is_not_defaulted(self):
        self.assertEqual(_chat_form("adjust", "end"), "旧进度")


class PanelApplyEndProgressTests(unittest.TestCase):
    def setUp(self):
        self.manager = Mock(spec=NoticePanel)
        self.method = NoticePanel._apply_changes

    def _run(self, changes, item):
        doc = {"items": [item]}
        self.method(self.manager, doc, changes)
        return item["draft"]

    def test_selection_reversal_end_then_update_then_end(self):
        item = _make_item()
        self._run([{"key": "k", "draft": {"notice_action": "结束"}}], item)
        self.assertEqual(item["draft"]["progress"], DEFAULT)
        self._run([{"key": "k", "draft": {"notice_action": "更新"}}], item)
        self.assertEqual(item["draft"]["progress"], "")
        self._run([{"key": "k", "draft": {"notice_action": "结束"}}], item)
        self.assertEqual(item["draft"]["progress"], DEFAULT)

    def test_revert_to_update_preserves_custom_progress(self):
        item = _make_item(progress="我方核验通过", notice_action="结束")
        self._run([{"key": "k", "draft": {"notice_action": "更新"}}], item)
        self.assertEqual(item["draft"]["progress"], "我方核验通过")

    def test_adjust_reversal_never_defaults_or_clears_progress(self):
        item = _make_item("adjust", notice_action="结束")
        item["draft"]["progress"] = "旧进度"
        self.assertNotIn("progress", {f["key"] for f in item["fields"]})
        self._run([{"key": "k", "draft": {"notice_action": "更新"}}], item)
        # Device adjust exposes no progress control: it is never defaulted or cleared.
        self.assertEqual(item["draft"]["progress"], "旧进度")


if __name__ == "__main__":
    unittest.main(verbosity=2)
# -*- coding: utf-8 -*-
"""Isolated unit tests for the pure notice-identity assistant adapter.

Fake records only -- no product mocks that hide helper behavior.  The native
contracts we depend on (workbench_lite helpers, identity_utils,
MaintenancePortalService._target_status_is_finished) are the real implementations.
"""
import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_notice_identity import (
    BIND_API,
    binding_fields,
    identity_choices,
    apply_identity_choice,
    _month,
    deletion_body,
    check_deletion_anchor,
)
from lan_bitable_template_portal.lighthouse_sources import SCOPES

ACTOR = {
    "id": "actor-1",
    "scopes": ["A", "B"],
    "is_admin": False,
}

PLANNED_SOURCE = {
    "source_record_id": "src-1",
    "record_id": "src-1",
    "title": "EA118机房A楼设备调整",
    "work_type": "maintenance",
    "notice_type": "维保通告",
    "building": "A楼",
    "building_codes": ["A"],
    "start_time": "2026-10-03 09:00",
    "end_time": "2026-10-03 11:00",
    "reason": "更换备件",
}

ONGOING_RECORD = {
    "active_item_id": "act-1",
    "source_record_id": "src-1",
    "target_record_id": "rec-target-1",
    "record_id": "rec-target-1",
    "work_type": "maintenance",
    "notice_type": "维保通告",
    "title": "EA118机房A楼设备调整",
    "building_codes": ["A"],
    "status": "进行中",
    "start_time": "2026-10-03 09:00",
    "end_time": "2026-10-03 11:00",
    "reason": "更换备件",
}


def make_operation(body):
    return {"api_id": BIND_API, "body": body}


class BindingFieldsTests(unittest.TestCase):
    def test_planned_anchors_on_queried_source_and_frees_invented_fields(self):
        op = make_operation({
            "scope": "A",
            "binding_context": "planned",
            "work_type": "maintenance",
            "source_record_id": "src-1",
            "title": "模型乱写的标题",
            "target_record_id": "rec-forged",
        })
        fields = binding_fields(ACTOR, op, {"q1": {"records": [PLANNED_SOURCE]}}, 0)
        self.assertNotIn("rec-forged", op["body"].get("target_record_id", "") or op["body"].get("record_id", ""))
        self.assertEqual(op["body"]["source_record_id"], "src-1")
        self.assertEqual(op["body"]["notice_type"], "维保通告")
        self.assertEqual(op["body"]["title"], PLANNED_SOURCE["title"])
        self.assertEqual(op["body"]["reason"], "更换备件")
        self.assertEqual(op["body"]["binding_context"], "planned")
        self.assertEqual(op["body"]["source_binding_only"], False)
        self.assertEqual(op["body"]["target_record_id"], "")
        # Selector is a target chooser, empty until the human picks.
        selector = fields[0]
        self.assertEqual(selector["path"], "target_record_id")
        self.assertEqual(selector["options_source"], "notice_identity_targets")
        self.assertTrue(selector["native_notice_identity"])
        self.assertEqual(selector["value"], "")
        self.assertTrue(selector["_anchor"]["record"]["source_record_id"], "src-1")

    def test_planned_source_in_buildings_nested_data(self):
        op = make_operation({
            "scope": "A",
            "binding_context": "planned",
            "work_type": "maintenance",
            "source_record_id": "src-1",
        })
        queries = {
            "q1": {
                "buildings": [
                    {"data": {"records": [PLANNED_SOURCE]}},
                ],
            },
        }
        fields = binding_fields(ACTOR, op, queries, 0)
        self.assertEqual(op["body"]["source_record_id"], "src-1")

    def test_planned_missing_source_rejected(self):
        op = make_operation({"scope": "A", "binding_context": "planned", "work_type": "maintenance"})
        with self.assertRaises(AssistantError):
            binding_fields(ACTOR, op, {}, 0)

    def test_unverified_sourceid_rejected(self):
        op = make_operation({
            "scope": "A", "binding_context": "planned", "work_type": "maintenance",
            "source_record_id": "src-forged",
        })
        with self.assertRaisesRegex(AssistantError, "未找到"):
            binding_fields(ACTOR, op, {"q1": {"records": [PLANNED_SOURCE]}}, 0)

    def test_out_of_scope_source_rejected(self):
        row = {**PLANNED_SOURCE, "building_codes": ["C"]}
        op = make_operation({
            "scope": "A", "binding_context": "planned", "work_type": "maintenance",
            "source_record_id": "src-1",
        })
        with self.assertRaises(AssistantError):
            binding_fields(ACTOR, op, {"q1": {"records": [row]}}, 0)

    def test_unknown_body_key_rejected(self):
        op = make_operation({
            "scope": "A", "binding_context": "planned", "work_type": "maintenance",
            "source_record_id": "src-1", "hacker_field": 1,
        })
        with self.assertRaisesRegex(AssistantError, "不支持的字段"):
            binding_fields(ACTOR, op, {"q1": {"records": [PLANNED_SOURCE]}}, 0)

    def test_event_work_type_rejected(self):
        op = make_operation({"scope": "A", "binding_context": "planned", "work_type": "event"})
        with self.assertRaises(AssistantError):
            binding_fields(ACTOR, op, {}, 0)

    def test_source_binding_only_requires_ongoing(self):
        op = make_operation({
            "scope": "A", "binding_context": "planned", "work_type": "maintenance",
            "source_binding_only": True, "source_record_id": "src-1",
        })
        with self.assertRaisesRegex(AssistantError, "进行中"):
            binding_fields(ACTOR, op, {"q1": {"records": [PLANNED_SOURCE]}}, 0)

    def test_ongoing_target_binding_anchors_on_active_item(self):
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1",
            "title": "乱写", "target_record_id": "rec-forged",
        })
        fields = binding_fields(ACTOR, op, {"q1": {"ongoing": [ONGOING_RECORD]}}, 1)
        self.assertEqual(op["body"]["active_item_id"], "act-1")
        self.assertEqual(op["body"]["target_record_id"], "rec-target-1")
        self.assertEqual(op["body"]["title"], ONGOING_RECORD["title"])
        self.assertEqual(op["body"]["source_binding_only"], False)
        selector = fields[0]
        self.assertEqual(selector["path"], "target_record_id")
        self.assertEqual(selector["options_source"], "notice_identity_targets")

    def test_ongoing_source_binding_adds_month_field(self):
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1", "target_record_id": "rec-target-1",
            "source_binding_only": True,
        })
        fields = binding_fields(ACTOR, op, {"q1": {"ongoing": [ONGOING_RECORD]}}, 0)
        self.assertEqual(op["body"]["source_binding_only"], True)
        self.assertEqual(op["body"]["source_record_id"], "")
        self.assertEqual(op["body"]["target_record_id"], "rec-target-1")
        self.assertEqual(op["body"]["active_item_id"], "act-1")
        # Month field comes first; the real source selector still follows it.
        self.assertEqual(fields[0]["path"], "source_month")
        selector = next(f for f in fields if f["path"] == "source_record_id")
        self.assertEqual(selector["options_source"], "notice_identity_sources")
        self.assertTrue(selector["required"])
        self.assertIn(ONGOING_RECORD["title"], selector["question_text"])
        month = next(f for f in fields if f["path"] == "source_month")
        self.assertEqual(month["value"], "10月")
        self.assertEqual(month["options"], [{"value": f"{m}月", "label": f"{m}月"} for m in range(1, 13)])
        self.assertEqual(month.get("_options_month"), None)
        self.assertEqual(op["body"]["source_month"], "10月")

    def test_ongoing_ambiguous_rejected(self):
        row_a = dict(ONGOING_RECORD)
        row_b = {**ONGOING_RECORD, "active_item_id": "act-1", "status": "进行中", "record_id": "rec-other"}
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1",
        })
        with self.assertRaisesRegex(AssistantError, "多条"):
            binding_fields(ACTOR, op, {"q1": {"ongoing": [row_a, row_b]}}, 0)

    def test_ongoing_ended_rejected(self):
        row = {**ONGOING_RECORD, "status": "已结束"}
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1",
        })
        with self.assertRaises(AssistantError):
            binding_fields(ACTOR, op, {"q1": {"ongoing": [row]}}, 0)

    def test_source_binding_requires_real_target(self):
        row = {**ONGOING_RECORD, "target_record_id": "localid-abc"}
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "source_binding_only": True, "target_record_id": "localid-abc",
        })
        with self.assertRaisesRegex(AssistantError, "真实目标"):
            binding_fields(ACTOR, op, {"q1": {"ongoing": [row]}}, 0)


def target_field(*, source=False, ongoing=True, work_type="maintenance", scopes=("A",), anchor=None, source_binding_only=False):
    source_opt = "notice_identity_sources" if source else "notice_identity_targets"
    return {
        "path": "source_record_id" if source else "target_record_id",
        "options_source": source_opt,
        "work_type": work_type,
        "binding_context": "ongoing" if ongoing else "planned",
        "source_binding_only": source_binding_only,
        "_anchor": anchor or {
            "work_type": work_type,
            "scopes": list(scopes),
            "binding_context": "ongoing" if ongoing else "planned",
            "source_binding_only": source_binding_only,
            "title": "A楼设备调整",
        },
        "_records": {},
    }


class IdentityChoicesTests(unittest.TestCase):
    def test_source_rows_choices(self):
        rows = [
            {"source_record_id": "src-1", "record_id": "src-1", "title": "A楼维保", "building": "A楼", "building_codes": ["A"], "status": "未开始"},
            {"source_record_id": "src-1", "title": "dup", "building_codes": ["A"]},
            {"source_record_id": "src-2", "record_id": "src-2", "title": "B楼维保", "building_codes": ["B"]},
        ]
        options, records = identity_choices(target_field(source=True), rows, ACTOR)
        # src-2 belongs to B, but the binding anchor is scoped to A only.
        self.assertEqual([o["value"] for o in options], ["src-1"])
        self.assertIn("src-1", records)
        self.assertEqual(records["src-1"]["title"], "A楼维保")

    def test_source_foreign_scope_skipped(self):
        rows = [
            {"source_record_id": "src-c", "record_id": "src-c", "title": "C楼", "building_codes": ["C"]},
        ]
        options, records = identity_choices(target_field(source=True), rows, ACTOR)
        self.assertEqual(options, [])
        self.assertEqual(records, {})

    def test_source_unknown_building_requires_admin_all_and_selection_all(self):
        rows = [
            {"source_record_id": "src-x", "record_id": "src-x", "title": "未知楼", "building_codes": []},
        ]
        options, _ = identity_choices(target_field(source=True), rows, ACTOR)
        self.assertEqual(options, [])
        admin_all = {**ACTOR, "is_admin": True, "scopes": ["110", "A", "B", "C", "D", "E", "H"]}
        # Admin whose selection scope is only A must NOT be allowed unknown candidates.
        field_a = target_field(source=True)
        field_a["scope"] = "A"
        options, _ = identity_choices(field_a, rows, admin_all)
        self.assertEqual(options, [])
        # Admin with the ALL alias but only an actual-A anchor is NOT allowed.
        field_alias_a = target_field(source=True)
        field_alias_a["scope"] = "ALL"
        options, _ = identity_choices(field_alias_a, rows, admin_all)
        self.assertEqual(options, [])
        # Admin with full scopes AND selection scope ALL AND full anchor scopes is allowed.
        field_all = target_field(source=True)
        field_all["scope"] = "ALL"
        field_all["_anchor"]["scopes"] = sorted(SCOPES)
        options, _ = identity_choices(field_all, rows, admin_all)
        self.assertEqual([o["value"] for o in options], ["src-x"])

    def test_work_type_mismatch_skipped(self):
        rows = [
            {"source_record_id": "src-1", "title": "变更", "work_type": "change", "building_codes": ["A"]},
        ]
        options, _ = identity_choices(target_field(source=True, work_type="maintenance"), rows, ACTOR)
        self.assertEqual(options, [])

    def test_target_ongoing_excludes_ended(self):
        rows = [
            {"target_record_id": "rec-1", "record_id": "rec-1", "title": "A楼", "building_codes": ["A"], "status": "进行中"},
            {"target_record_id": "rec-2", "record_id": "rec-2", "title": "A楼", "building_codes": ["A"], "status": "已结束"},
        ]
        options, records = identity_choices(target_field(ongoing=True), rows, ACTOR)
        self.assertEqual([o["value"] for o in options], ["rec-1"])
        self.assertEqual(records["rec-1"]["status"], "进行中")

    def test_target_planned_excludes_finished_for_all_types(self):
        rows = [
            {"target_record_id": "rec-1", "record_id": "rec-1", "title": "A楼", "building_codes": ["A"], "status": "已结束"},
        ]
        options, records = identity_choices(target_field(ongoing=False, work_type="maintenance"), rows, ACTOR)
        self.assertEqual(options, [])
        self.assertEqual(records, {})

    def test_target_planned_excludes_finished_for_non_bindable_type(self):
        rows = [
            {"target_record_id": "rec-1", "record_id": "rec-1", "title": "A楼", "building_codes": ["A"], "status": "已结束"},
        ]
        options, _ = identity_choices(target_field(ongoing=False, work_type="power"), rows, ACTOR)
        self.assertEqual(options, [])

    def test_malformed_rows_raise_instead_of_silently_empty(self):
        # Non-dict rows are a contract violation and must raise, not be skipped.
        with self.assertRaisesRegex(AssistantError, "无效记录"):
            identity_choices(target_field(), ["not-a-dict"], ACTOR)
        # A row without any usable candidate id is an unknown id and must raise too.
        with self.assertRaisesRegex(AssistantError, "记录ID"):
            identity_choices(target_field(), [{"nonsense": True}], ACTOR)

    def test_local_id_skipped(self):
        rows = [
            {"target_record_id": "localid-abc", "record_id": "localid-abc", "title": "x", "building_codes": ["A"]},
        ]
        options, _ = identity_choices(target_field(), rows, ACTOR)
        self.assertEqual(options, [])


class ApplyIdentityChoiceTests(unittest.TestCase):
    def _source_field_and_operation(self):
        field = target_field(source=True, source_binding_only=True, ongoing=True)
        field["scope"] = "A"
        field["_anchor"]["scope"] = "A"
        field["_anchor"]["month"] = "10月"
        field["_anchor"]["identity"] = {
            "active_item_id": "act-1",
            "source_record_id": "src-orig",
            "target_record_id": "rec-target-1",
            "record_id": "rec-target-1",
        }
        field["_anchor"]["record"] = dict(ONGOING_RECORD)
        options, records = identity_choices(field, [
            {"source_record_id": "src-9", "record_id": "src-9", "title": "A楼维保", "building_codes": ["A"], "status": "未开始"},
        ], ACTOR)
        field["_records"] = records
        field["options"] = options
        op = make_operation({
            "scope": "A",
            "binding_context": "ongoing",
            "work_type": "maintenance",
            "active_item_id": "act-1",
            "target_record_id": "rec-target-1",
            "record_id": "rec-target-1",
            "source_binding_only": True,
            "source_record_id": "",
            "title": "A楼设备调整",
            "source_month": "10月",
        })
        return field, op

    def _target_field_and_operation(self):
        field = target_field(ongoing=False, work_type="maintenance")
        field["scope"] = "A"
        field["_anchor"]["scope"] = "A"
        field["_anchor"]["identity"] = {
            "active_item_id": "act-1",
            "source_record_id": "src-1",
            "target_record_id": "",
            "record_id": "",
        }
        field["_anchor"]["record"] = dict(PLANNED_SOURCE)
        options, records = identity_choices(field, [
            {"target_record_id": "rec-t", "record_id": "rec-t", "title": "A楼目标", "building_codes": ["A"], "status": "进行中"},
        ], ACTOR)
        field["_records"] = records
        field["options"] = options
        op = make_operation({
            "scope": "A",
            "binding_context": "planned",
            "work_type": "maintenance",
            "source_record_id": "src-1",
            "active_item_id": "act-1",
            "target_record_id": "",
            "record_id": "",
            "title": "A楼设备调整",
        })
        return field, op

    def test_source_selection_preserves_target_and_active(self):
        field, op = self._source_field_and_operation()
        labels = apply_identity_choice(field, op, ACTOR, "src-9")
        self.assertEqual(op["body"]["source_record_id"], "src-9")
        self.assertEqual(op["body"]["target_record_id"], "rec-target-1")
        self.assertEqual(op["body"]["active_item_id"], "act-1")
        self.assertEqual(op["body"]["source_month"], "10月")
        self.assertEqual(labels["source_record_id"], field["options"][0]["label"])
        self.assertIn("original_notice", labels)

    def test_target_selection_sets_ids_and_status(self):
        field, op = self._target_field_and_operation()
        labels = apply_identity_choice(field, op, ACTOR, "rec-t")
        self.assertEqual(op["body"]["target_record_id"], "rec-t")
        self.assertEqual(op["body"]["record_id"], "rec-t")
        self.assertEqual(op["body"]["active_item_id"], "act-1")
        self.assertEqual(op["body"]["source_record_id"], "src-1")
        self.assertEqual(op["body"]["status"], "进行中")
        self.assertIn("target_record_id", labels)
        self.assertIn("original_notice", labels)

    def test_unloaded_selected_rejected(self):
        field, op = self._source_field_and_operation()
        with self.assertRaisesRegex(AssistantError, "已读取"):
            apply_identity_choice(field, op, ACTOR, "src-forged")


class ReproduceDefectTests(unittest.TestCase):
    """Reproducing tests for the concrete defects under review.

    These are written against the TARGET contract (fixes expected). They are
    intentionally added first so they fail on the current implementation.
    """

    def test_wrapper_contract_records_are_raw_rows(self):
        field, records = identity_choices(target_field(source=True), [
            {"source_record_id": "src-1", "record_id": "src-1", "title": "A楼维保",
             "building_codes": ["A"], "status": "未开始"},
        ], ACTOR)
        self.assertIn("src-1", records)
        self.assertEqual(records["src-1"]["record_id"], "src-1")
        self.assertNotIn("raw", records["src-1"])
        self.assertNotIn("label", records["src-1"])

    def test_month_is_native_label_and_preserved_before_clear(self):
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1", "target_record_id": "rec-target-1",
            "source_binding_only": True, "source_month": "3月",
        })
        fields = binding_fields(ACTOR, op, {"q1": {"ongoing": [ONGOING_RECORD]}}, 0)
        self.assertEqual(op["body"]["source_month"], "3月")
        month = next(f for f in fields if f["path"] == "source_month")
        self.assertEqual(month["value"], "3月")
        self.assertEqual(month["options"], [{"value": f"{m}月", "label": f"{m}月"} for m in range(1, 13)])
        self.assertNotIn("options_source", month)

    def test_invalid_scope_rejected(self):
        op = make_operation({
            "scope": "Z", "binding_context": "planned", "work_type": "maintenance",
            "source_record_id": "src-1",
        })
        with self.assertRaises(AssistantError):
            binding_fields(ACTOR, op, {"q1": {"records": [PLANNED_SOURCE]}}, 0)

    def test_blank_scope_anchor_rejected(self):
        row = {k: v for k, v in PLANNED_SOURCE.items()
               if k not in ("building", "building_codes", "scope", "scope_code", "building_name", "scope_codes")}
        op = make_operation({
            "scope": "A", "binding_context": "planned", "work_type": "maintenance",
            "source_record_id": "src-1",
        })
        with self.assertRaises(AssistantError):
            binding_fields(ACTOR, op, {"q1": {"records": [row]}}, 0)

    def test_lifecycle_flags_rejected_for_candidates(self):
        rows = [
            {**ONGOING_RECORD, "target_record_id": "rec-t1", "record_id": "rec-t1", "target_finished": True, "status": "进行中"},
            {**ONGOING_RECORD, "target_record_id": "rec-t2", "record_id": "rec-t2", "target_active": False, "status": "进行中"},
            {**ONGOING_RECORD, "target_record_id": "rec-t3", "record_id": "rec-t3", "deleted_at": "2026-01-01 00:00:00", "status": "进行中"},
            {**ONGOING_RECORD, "target_record_id": "rec-ok", "record_id": "rec-ok", "status": "进行中"},
        ]
        options, records = identity_choices(target_field(ongoing=True), rows, ACTOR)
        self.assertEqual([o["value"] for o in options], ["rec-ok"])

    def test_source_with_ended_status_not_valid_new_binding(self):
        rows = [
            {"source_record_id": "src-1", "record_id": "src-1", "title": "A楼", "building_codes": ["A"], "status": "已结束"},
            {"source_record_id": "src-2", "record_id": "src-2", "title": "A楼", "building_codes": ["A"], "status": "未开始"},
        ]
        options, records = identity_choices(target_field(source=True), rows, ACTOR)
        self.assertEqual([o["value"] for o in options], ["src-2"])

    def test_progress_is_not_used_as_status(self):
        row = {"target_record_id": "rec-1", "record_id": "rec-1", "title": "A楼",
               "building_codes": ["A"], "progress": "已完成60%", "status": "进行中"}
        options, records = identity_choices(target_field(ongoing=True), [row], ACTOR)
        self.assertEqual([o["value"] for o in options], ["rec-1"])

    def test_unknown_building_requires_admin_full_and_selection_all(self):
        row = {"source_record_id": "src-x", "record_id": "src-x", "title": "未知楼", "building_codes": []}
        admin_a = {**ACTOR, "is_admin": True}
        field_a = target_field(source=True)
        field_a["scope"] = "A"
        options, _ = identity_choices(field_a, [row], admin_a)
        self.assertEqual(options, [])
        field_all = target_field(source=True)
        field_all["scope"] = "ALL"
        options, _ = identity_choices(field_all, [row], ACTOR)
        self.assertEqual(options, [])
        admin_full = {**ACTOR, "is_admin": True, "scopes": sorted(SCOPES)}
        # Full-admin with the ALL alias but only an actual-A anchor is NOT allowed.
        field_alias_a = target_field(source=True)
        field_alias_a["scope"] = "ALL"
        options, _ = identity_choices(field_alias_a, [row], admin_full)
        self.assertEqual(options, [])
        # Full-admin with ALL alias AND full anchor scopes is allowed.
        field_all2 = target_field(source=True)
        field_all2["scope"] = "ALL"
        field_all2["_anchor"]["scopes"] = sorted(SCOPES)
        options, _ = identity_choices(field_all2, [row], admin_full)
        self.assertEqual([o["value"] for o in options], ["src-x"])

    def test_duplicate_identical_snapshot_not_ambiguous(self):
        queries = {
            "q1": {"ongoing": [ONGOING_RECORD]},
            "q2": {"buildings": [{"data": {"ongoing": [dict(ONGOING_RECORD)]}}]},
        }
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1",
        })
        fields = binding_fields(ACTOR, op, queries, 0)
        self.assertEqual(op["body"]["active_item_id"], "act-1")

    def test_planned_source_falls_back_to_record_id(self):
        row = {**PLANNED_SOURCE}
        row.pop("source_record_id")
        row["record_id"] = "src-1"
        op = make_operation({
            "scope": "A", "binding_context": "planned", "work_type": "maintenance",
            "source_record_id": "src-1",
        })
        fields = binding_fields(ACTOR, op, {"q1": {"records": [row]}}, 0)
        self.assertEqual(op["body"]["source_record_id"], "src-1")

    def test_source_binding_only_must_be_actual_bool(self):
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1", "source_binding_only": "false",
        })
        with self.assertRaises(AssistantError):
            binding_fields(ACTOR, op, {"q1": {"ongoing": [ONGOING_RECORD]}}, 0)

    def test_apply_rebuilds_immutable_body(self):
        field = target_field(source=True, source_binding_only=True, ongoing=True)
        field["scope"] = "A"
        field["_anchor"]["month"] = "10月"
        field["_anchor"]["identity"] = {
            "active_item_id": "act-1",
            "source_record_id": "src-orig",
            "target_record_id": "rec-target-1",
            "record_id": "rec-target-1",
        }
        field["_anchor"]["record"] = dict(ONGOING_RECORD)
        _, records = identity_choices(field, [
            {"source_record_id": "src-9", "record_id": "src-9", "title": "A楼维保",
             "building_codes": ["A"], "status": "未开始"},
        ], ACTOR)
        field["_records"] = records
        field["options"] = [{"value": "src-9", "label": "A楼维保 · A楼 · 未开始"}]
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1", "target_record_id": "rec-target-1",
            "source_binding_only": True, "source_record_id": "", "source_month": "10月",
            "title": "被篡改标题", "notice_type": "被篡改类型", "reason": "被篡改原因",
            "start_time": "被篡改", "end_time": "被篡改",
        })
        apply_identity_choice(field, op, ACTOR, "src-9")
        self.assertEqual(op["body"]["source_record_id"], "src-9")
        self.assertEqual(op["body"]["target_record_id"], "rec-target-1")
        self.assertEqual(op["body"]["active_item_id"], "act-1")
        self.assertEqual(op["body"]["source_month"], "10月")
        self.assertEqual(op["body"]["title"], ONGOING_RECORD["title"])
        self.assertEqual(op["body"]["notice_type"], "维保通告")
        self.assertEqual(op["body"]["reason"], "更换备件")
        self.assertNotEqual(op["body"]["title"], "被篡改标题")

    def test_selector_and_month_fields_are_required(self):
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1", "target_record_id": "rec-target-1",
            "source_binding_only": True, "source_month": "3月",
        })
        fields = binding_fields(ACTOR, op, {"q1": {"ongoing": [ONGOING_RECORD]}}, 0)
        selector = fields[0]
        self.assertTrue(selector["required"])
        month = next(f for f in fields if f["path"] == "source_month")
        self.assertTrue(month["required"])

    def test_source_option_keeps_progress_as_source_status(self):
        # /source-options rows only carry progress; non-maintenance bindings must accept them.
        row = {"source_record_id": "rec-source-new", "title": "A楼新计划", "building": "A楼", "progress": "未开始"}
        for work_type in ("maintenance", "change", "repair", "power", "polling", "adjust"):
            options, records = identity_choices(target_field(source=True, work_type=work_type), [row], ACTOR)
            self.assertEqual([o["value"] for o in options], ["rec-source-new"], work_type)
            self.assertNotIn("status", records["rec-source-new"])
        # Explicit mismatch is still rejected.
        explicit = {"source_record_id": "src-c", "title": "变更", "work_type": "change", "building_codes": ["A"]}
        options, _ = identity_choices(target_field(source=True, work_type="maintenance"), [explicit], ACTOR)
        self.assertEqual(options, [])

    def test_source_only_falls_back_to_queried_real_target(self):
        # Body omits target_record_id but knows the active item; the queried row's real target is used.
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1", "source_binding_only": True,
        })
        fields = binding_fields(ACTOR, op, {"q1": {"ongoing": [ONGOING_RECORD]}}, 0)
        self.assertEqual(op["body"]["target_record_id"], "rec-target-1")
        self.assertEqual(op["body"]["active_item_id"], "act-1")

    def test_source_only_rejects_conflicting_target(self):
        # The queried ongoing row really points at rec-target-1, so asking for a different target must fail.
        row = {**ONGOING_RECORD}
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1", "target_record_id": "rec-other",
            "source_binding_only": True,
        })
        with self.assertRaisesRegex(AssistantError, "不一致"):
            binding_fields(ACTOR, op, {"q1": {"ongoing": [row]}}, 0)

    def test_newer_deleted_snapshot_does_not_fall_through_to_older_live(self):
        deleted_new = {**ONGOING_RECORD, "record_id": "rec-deleted", "deleted_at": "2026-01-01 00:00:00"}
        # Earlier snapshot has the same active/source with a real live target.
        older_live = {**ONGOING_RECORD}
        queries = {
            "q1_older": {"ongoing": [older_live]},
            "q2_newer": {"ongoing": [deleted_new]},
        }
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1",
        })
        # The newest queried record is deleted; it must NOT silently fall back to the older live one.
        with self.assertRaisesRegex(AssistantError, "已删除"):
            binding_fields(ACTOR, op, queries, 0)

    def test_campus_subset_scopes_allowed_when_actor_covers_campus(self):
        row = {**ONGOING_RECORD, "building_codes": ["A", "B"]}
        campus_actor = {**ACTOR, "scopes": ["A", "B", "C", "D", "E"]}
        op = make_operation({
            "scope": "ALL", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1",
        })
        fields = binding_fields(campus_actor, op, {"q1": {"ongoing": [row]}}, 0)
        self.assertEqual(fields[0]["scope"], "CAMPUS")
        self.assertEqual(op["body"]["scope"], "CAMPUS")
        # Candidates are still filtered to the actual A/B scopes, not widened to all ABCDE.
        rows = [
            {"source_record_id": "src-ab", "record_id": "src-ab", "building_codes": ["A", "B"]},
            {"source_record_id": "src-c", "record_id": "src-c", "building_codes": ["C"]},
        ]
        field_ab = target_field(source=True)
        field_ab["_anchor"]["scopes"] = ["A", "B"]
        field_ab["scope"] = "CAMPUS"
        options, _ = identity_choices(field_ab, rows, campus_actor)
        self.assertEqual([o["value"] for o in options], ["src-ab"])

    def test_month_rejects_bad_string(self):
        with self.assertRaises(AssistantError):
            _month("bad1junk")
        self.assertEqual(_month("2026-10"), "10月")
        self.assertEqual(_month("3月"), "3月")

    def test_native_title_falls_back_to_display_fields(self):
        # A real workbench row keeps its native title inside display_fields (Chinese keys),
        # not at the top level; _canonical_body must derive it through _draft_from_record.
        record = {
            "active_item_id": "act-1",
            "source_record_id": "src-1",
            "target_record_id": "rec-target-1",
            "record_id": "rec-target-1",
            "display_fields": {
                "维护总项": "EA118机房A楼设备调整",
                "计划开始时间": "2026-10-03 09:00",
                "计划结束时间": "2026-10-03 11:00",
                "原因": "更换备件",
            },
            "building_codes": ["A"],
            "status": "进行中",
        }
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1",
        })
        fields = binding_fields(ACTOR, op, {"q1": {"ongoing": [record]}}, 0)
        self.assertEqual(op["body"]["title"], "EA118机房A楼设备调整")
        self.assertEqual(op["body"]["start_time"], "2026-10-03T09:00")
        self.assertEqual(op["body"]["reason"], "更换备件")
        self.assertEqual(fields[0]["_anchor"]["title"], "EA118机房A楼设备调整")

    def test_original_type_mismatch_rejected(self):
        # Defect: a maintenance original bound as "repair" must be rejected, not silently rebound.
        op = make_operation({
            "scope": "A", "binding_context": "planned", "work_type": "repair",
            "source_record_id": "src-1",
        })
        with self.assertRaises(AssistantError):
            binding_fields(ACTOR, op, {"q1": {"records": [PLANNED_SOURCE]}}, 0)

    def test_active_identity_priority_over_conflicting_target(self):
        # Defect: when the body carries an active item AND a target that belongs to a
        # different ongoing row, the active identity is authoritative and resolves to
        # that row's real target instead of failing ambiguously / mixing identities.
        ongoing = dict(ONGOING_RECORD)
        other = {**ONGOING_RECORD, "active_item_id": "other-active",
                 "target_record_id": "rec-other", "record_id": "rec-other"}
        op = make_operation({
            "scope": "A", "binding_context": "ongoing", "work_type": "maintenance",
            "active_item_id": "act-1", "target_record_id": "rec-other",
        })
        fields = binding_fields(ACTOR, op, {"q1": {"ongoing": [ongoing, other]}}, 0)
        self.assertEqual(op["body"]["active_item_id"], "act-1")
        self.assertEqual(op["body"]["target_record_id"], "rec-target-1")


class DeletionIdentityTests(unittest.TestCase):
    def test_all_non_event_types_keep_actual_identity_pair(self):
        for work, notice in [('maintenance', '维保通告'), ('change', '变更通告'), ('repair', '设备检修'), ('power', '上电通告'), ('power', '下电通告'), ('polling', '设备轮巡'), ('adjust', '设备调整')]:
            with self.subTest(work=work, notice=notice):
                row = {**ONGOING_RECORD, 'work_type': work, 'notice_type': notice}
                body = deletion_body(ACTOR, {'scope': 'A', 'work_type': work, 'target_record_id': row['target_record_id']}, {'q': {'ongoing': [row]}})
                self.assertEqual(body['active_item_id'], row['active_item_id'])
                self.assertEqual(body['target_record_id'], row['target_record_id'])
                self.assertEqual(body['source_record_id'], row['source_record_id'])
                self.assertEqual(body['title'], row['title'])
                self.assertEqual(body['notice_type'], notice)
                check_deletion_anchor(body, copy.deepcopy(body))
                with self.assertRaises(AssistantError):
                    check_deletion_anchor({**body, 'target_record_id': 'rec-other'}, body)

    def test_mixed_ids_cannot_be_silently_corrected(self):
        for patch in ({'active_item_id': 'act-1', 'target_record_id': 'rec-old'},
                      {'target_record_id': 'rec-target-1', 'record_id': 'rec-old'},
                      {'target_record_id': 'rec-target-1', 'source_record_id': 'src-other'}):
            with self.subTest(patch=patch), self.assertRaises(AssistantError):
                deletion_body(ACTOR, {'scope': 'A', 'work_type': 'maintenance', **patch}, {'q': {'ongoing': [ONGOING_RECORD]}})

    def test_invalid_and_unrelated_rows_are_never_deletion_evidence(self):
        body = {'scope': 'A', 'work_type': 'maintenance', 'target_record_id': 'rec-target-1'}
        for change in ({'deleted_at': 1}, {'status': '已删除'}, {'status': 'deleted'}, {'target_active': False}, {'status': '结束'}, {'work_type': 'event', 'notice_type': '事件通告'}, {'building_codes': ['B']}):
            with self.subTest(change=change), self.assertRaises(AssistantError):
                deletion_body(ACTOR, body, {'q': {'ongoing': [{**ONGOING_RECORD, **change}]}})
        with self.assertRaises(AssistantError):
            deletion_body(ACTOR, body, {'q': {'records': [ONGOING_RECORD]}})

    def test_name_only_or_mismatched_name_is_rejected(self):
        for patch in ({'title': ONGOING_RECORD['title']}, {'target_record_id': 'rec-target-1', 'title': '另一条通告'}):
            with self.subTest(patch=patch), self.assertRaises(AssistantError):
                deletion_body(ACTOR, {'scope': 'A', 'work_type': 'maintenance', **patch}, {'q': {'ongoing': [ONGOING_RECORD]}})

    def test_source_plan_status_does_not_hide_a_current_active_target(self):
        row = {**ONGOING_RECORD, 'status': '开始', 'source_status': '已结束'}
        body = deletion_body(ACTOR, {'scope': 'A', 'target_record_id': row['target_record_id']}, {'q': {'ongoing': [row]}})
        self.assertEqual(body['target_record_id'], row['target_record_id'])

    def test_visible_unuploaded_item_remains_a_valid_local_delete(self):
        row = {**ONGOING_RECORD, 'active_item_id': 'manual_visible', 'record_id': 'manual_visible',
               'source_record_id': '', 'target_record_id': '', 'target_active': False, 'status': '未上传首条'}
        body = deletion_body(ACTOR, {'scope': 'A', 'active_item_id': 'manual_visible'}, {'q': {'ongoing': [row]}})
        self.assertEqual(body['active_item_id'], 'manual_visible')
        self.assertEqual(body['target_record_id'], '')

    def test_latest_snapshot_rebind_does_not_fall_back_to_old_target(self):
        queries = {'old': {'ongoing': [ONGOING_RECORD]}, 'new': {'ongoing': [{**ONGOING_RECORD, 'target_record_id': 'rec-new', 'record_id': 'rec-new'}]}}
        with self.assertRaises(AssistantError):
            deletion_body(ACTOR, {'scope': 'A', 'work_type': 'maintenance', 'active_item_id': 'act-1', 'target_record_id': 'rec-target-1'}, queries)

    def test_shared_target_ambiguity_blocks_deletion(self):
        queries = {'q': {'ongoing': [ONGOING_RECORD, {**ONGOING_RECORD, 'active_item_id': 'other-active'}]}}
        with self.assertRaises(AssistantError):
            deletion_body(ACTOR, {'scope': 'A', 'target_record_id': 'rec-target-1'}, queries)

    def test_conflicting_active_mapping_in_same_list_is_not_last_row_wins(self):
        other = {**ONGOING_RECORD, 'target_record_id': 'rec-other', 'record_id': 'rec-other'}
        with self.assertRaises(AssistantError):
            deletion_body(ACTOR, {'scope': 'A', 'active_item_id': 'act-1'}, {'q': {'ongoing': [ONGOING_RECORD, other]}})
        body = deletion_body(ACTOR, {'scope': 'A', 'active_item_id': 'act-1'}, {'q': {'ongoing': [ONGOING_RECORD, copy.deepcopy(ONGOING_RECORD)]}})
        self.assertEqual(body['target_record_id'], 'rec-target-1')


if __name__ == "__main__":
    unittest.main(verbosity=2)

# -*- coding: utf-8 -*-
"""纯助手侧水耗表单的隔离测试（不改业务文件、不写 API）。

覆盖 ``build_water_form`` / ``water_payload`` 的楼栋校验、POST 管理员约束、
bootstrap 选项、PATCH 原始详情与版本冻结、可编辑 children、以及填写结果校验。
只用内存构造数据；不调用任何真实业务服务。
"""
import sys
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.lighthouse_ai import AssistantError  # noqa: E402
from lan_bitable_template_portal.lighthouse_water import (  # noqa: E402
    WATER_SCOPE_CODES,
    build_water_form,
    water_payload,
)


def _actor(scopes=("A",), is_admin=True):
    return {"scopes": list(scopes), "is_admin": is_admin, "id": "user-a"}


def _bootstrap(scope="A", can_create=True, meters=None, frequencies=None, shifts=None):
    return {
        "scope": scope,
        "building": "{}楼".format(scope),
        "options": {
            "meters": meters or ["中水", "市政水"],
            "frequencies": frequencies or ["日", "周", "月"],
            "shifts": shifts or ["白", "夜"],
        },
        "permissions": {"is_admin": True, "can_create": can_create},
    }


def _photo(image_id, name, **extra):
    row = {"image_id": image_id, "name": name}
    row.update(extra)
    return row


def _detail(scope="A", record_id="rec-water", version="water-v1", can_edit=True,
            meter="中水", frequency="日", shift="白", date="2026-10-01",
            meter_value=100.5, corrected_usage=None, photos=None):
    return {
        "record_id": record_id,
        "scope_code": scope,
        "meter": meter,
        "frequency": frequency,
        "shift": shift,
        "statistic_date": date,
        "statistic_date_key": date,
        "meter_value": meter_value,
        "corrected_usage": corrected_usage,
        "version": version,
        "photos": photos or [
            _photo("img-a1", "table-a.jpg", file_token="tok-a1", url="https://example/a1"),
            _photo("img-b1", "table-b.jpg"),
        ],
        "edit_policy": {"can_edit": can_edit},
    }


def _queries(*values):
    return {"q{}".format(i): value for i, value in enumerate(values)}


def _child(control, path):
    for item in control.get("children") or []:
        if item.get("path") == path:
            return item
    raise AssertionError("missing child path: {}".format(path))


class BuildWaterFormControlTest(unittest.TestCase):
    def test_control_shape_literal_children(self):
        queries = _queries(_bootstrap("A"))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": "A"}}
        native_body, control = build_water_form(_actor(), operation, queries, 2)
        self.assertEqual(control["type"], "object")
        self.assertTrue(control["native_water_record"])
        self.assertEqual(control["name"], "step2.water")
        self.assertEqual(control["section"], "body")
        self.assertEqual(control["operation_index"], 2)
        paths = {item["path"] for item in control["children"]}
        self.assertEqual(paths, {"statistic_date", "meter", "frequency", "shift", "meter_value",
                                 "corrected_usage", "retained_image_ids"})
        self.assertTrue(all(item.get("literal_key") for item in control["children"]))

    def test_children_field_types(self):
        queries = _queries(_bootstrap("A"))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": "A"}}
        _, control = build_water_form(_actor(), operation, queries, 0)
        self.assertEqual(_child(control, "statistic_date")["type"], "date")
        self.assertTrue(_child(control, "statistic_date")["required"])
        self.assertEqual(_child(control, "meter")["type"], "select")
        self.assertEqual(_child(control, "frequency")["type"], "select")
        self.assertEqual(_child(control, "shift")["type"], "select")
        meter_value = _child(control, "meter_value")
        self.assertEqual(meter_value["type"], "number")
        self.assertTrue(meter_value["required"])
        self.assertEqual(meter_value["min"], 0)
        self.assertEqual(meter_value["step"], "any")
        corrected = _child(control, "corrected_usage")
        self.assertTrue(corrected.get("nullable"))
        self.assertTrue(corrected.get("allow_negative"))
        self.assertEqual(corrected.get("step"), "any")
        retained = _child(control, "retained_image_ids")
        self.assertEqual(retained["type"], "multiselect")
        self.assertTrue(retained.get("choice_group"))
        self.assertNotIn("control_type", retained)

    def test_options_from_bootstrap(self):
        queries = _queries(_bootstrap("A", meters=["中水"], frequencies=["日", "月"], shifts=["白"]))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": "A"}}
        _, control = build_water_form(_actor(), operation, queries, 0)
        self.assertEqual([o["value"] for o in _child(control, "meter")["options"]], ["中水"])
        self.assertEqual([o["value"] for o in _child(control, "frequency")["options"]], ["日", "月"])
        self.assertEqual([o["value"] for o in _child(control, "shift")["options"]], ["白"])

    def test_initial_defaults_beijing_today_and_first_native(self):
        queries = _queries(_bootstrap("A", frequencies=["日", "周"], shifts=["白", "夜"]))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": "A"}}
        _, control = build_water_form(_actor(), operation, queries, 0)
        initial = control["_initial_form"]
        self.assertRegex(initial["statistic_date"], r"^\d{4}-\d{2}-\d{2}$")
        self.assertEqual(initial["frequency"], "日")
        self.assertEqual(initial["shift"], "白")
        self.assertEqual(initial["meter"], "")
        self.assertEqual(initial["retained_image_ids"], [])

    def test_initial_contains_only_editable_fields(self):
        queries = _queries(_bootstrap("A"))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": "A"}}
        _, control = build_water_form(_actor(), operation, queries, 0)
        self.assertEqual(set(control["_initial_form"]), {"statistic_date", "meter", "frequency", "shift",
                                                         "meter_value", "corrected_usage", "retained_image_ids"})
        self.assertNotIn("expected_version", control["_initial_form"])
        self.assertNotIn("operation_id", control["_initial_form"])


class BuildWaterFormCreateTest(unittest.TestCase):
    def test_non_admin_cannot_create(self):
        queries = _queries(_bootstrap("A"))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": "A"}}
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(is_admin=False), operation, queries, 0)
        self.assertIn("管理员", str(cm.exception))

    def test_bootstrap_can_create_false_rejected(self):
        queries = _queries(_bootstrap("A", can_create=False))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": "A"}}
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), operation, queries, 0)
        self.assertIn("新增水耗记录权限", str(cm.exception))

    def test_missing_bootstrap_rejected(self):
        queries = _queries({})
        operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": "A"}}
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), operation, queries, 0)
        self.assertIn("完整的水耗基础数据", str(cm.exception))

    def test_native_body_resets_confirmation_and_freeze_metadata(self):
        queries = _queries(_bootstrap("A"))
        operation = {"api_id": "POST /api/capacity/water/records",
                     "body": {"scope": "A", "operation_id": "op-123", "upload_ids": ["u-1"],
                              "large_change_confirmed": True, "abnormal_note": "x"}}
        native_body, _ = build_water_form(_actor(), operation, queries, 0)
        self.assertEqual(native_body["operation_id"], "op-123")
        self.assertEqual(native_body["upload_ids"], ["u-1"])
        self.assertFalse(native_body["large_change_confirmed"])
        self.assertEqual(native_body["abnormal_note"], "")
        self.assertEqual(native_body["expected_version"], "")
        self.assertEqual(native_body["scope"], "A")
        self.assertEqual(set(native_body), {
            "operation_id", "expected_version", "large_change_confirmed", "abnormal_note", "scope",
            "meter", "frequency", "shift", "statistic_date", "meter_value", "corrected_usage",
            "upload_ids", "retained_image_ids"})

    def test_unknown_business_field_rejected(self):
        operation = {"api_id": "POST /api/capacity/water/records",
                     "body": {"scope": "A", "forged": "x"}}
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), operation, _queries(_bootstrap("A")), 0)
        self.assertIn("不支持的字段", str(cm.exception))


class BuildWaterFormScopeTest(unittest.TestCase):
    def test_unique_authorized_scope_default(self):
        queries = _queries(_bootstrap("B"))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {}}
        native_body, _ = build_water_form(_actor(scopes=("B",)), operation, queries, 0)
        self.assertEqual(native_body["scope"], "B")

    def test_cross_scope_rejected(self):
        queries = _queries(_bootstrap("A"))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": "B"}}
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(scopes=("A",)), operation, queries, 0)
        self.assertEqual(cm.exception.status, 403)

    def test_unsupported_scope_letter_rejected(self):
        queries = _queries(_bootstrap("A"))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": "F"}}
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(scopes=("A", "B", "C", "D", "E", "H")), operation, queries, 0)
        self.assertIn("A、B、C、D、E、H", str(cm.exception))

    def test_ambiguous_scope_asks(self):
        queries = _queries(_bootstrap("A"))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {}}
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(scopes=("A", "B")), operation, queries, 0)
        self.assertIn("请明确", str(cm.exception))

    def test_all_scope_rejected_not_defaulted_to_actor(self):
        queries = _queries(_bootstrap("A"))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": "ALL"}}
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(scopes=("A",)), operation, queries, 0)
        self.assertIn("A、B、C、D、E、H", str(cm.exception))

    def test_garbage_scope_rejected_not_defaulted_to_actor(self):
        queries = _queries(_bootstrap("A"))
        for bad in ("F", "AB"):
            operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": bad}}
            with self.assertRaises(AssistantError, msg=repr(bad)):
                build_water_form(_actor(scopes=("A",)), operation, queries, 0)

    def test_bootstrap_object_option_rejected_not_stringified(self):
        queries = _queries(_bootstrap("A", meters=[{"value": "x"}]))
        operation = {"api_id": "POST /api/capacity/water/records", "body": {"scope": "A"}}
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), operation, queries, 0)
        self.assertIn("选项", str(cm.exception))

    def test_many_scoped_query_wrapper_unique_detail_scope(self):
        bootstrap_a = _bootstrap("A")
        detail_a = _detail("A", record_id="rec1")
        detail_b = _detail("B", record_id="rec2")
        queries = _queries(
            {"buildings": [{"scope": "B", "ok": True, "data": detail_b},
                           {"scope": "A", "ok": True, "data": detail_a}]},
            bootstrap_a,
        )
        operation = {"api_id": "PATCH /api/capacity/water/records/{record_id}",
                     "path_params": {"record_id": "rec1"}, "body": {"expected_version": "water-v1"}}
        native_body, _ = build_water_form(_actor(scopes=("A", "B")), operation, queries, 0)
        self.assertEqual(native_body["scope"], "A")


class BuildWaterFormPatchTest(unittest.TestCase):
    def _patch_operation(self, record_id="rec-water", body=None):
        body = {"expected_version": "water-v1", **(body or {})}
        return {"api_id": "PATCH /api/capacity/water/records/{record_id}",
                "path_params": {"record_id": record_id}, "body": body}

    def test_patch_requires_bootstrap_and_detail(self):
        queries = _queries(_bootstrap("A"), _detail("A"))
        _, control = build_water_form(_actor(), self._patch_operation(), queries, 0)
        self.assertEqual(control["_scope"], "A")
        self.assertEqual(control["_record_id"], "rec-water")
        self.assertEqual(control["_version"], "water-v1")

    def test_patch_missing_detail_rejected(self):
        queries = _queries(_bootstrap("A"))
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), self._patch_operation(), queries, 0)
        self.assertIn("完整详情", str(cm.exception))

    def test_patch_missing_version_rejected(self):
        detail = _detail("A")
        del detail["version"]
        queries = _queries(_bootstrap("A"), detail)
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), self._patch_operation(), queries, 0)
        self.assertIn("版本号", str(cm.exception))

    def test_patch_missing_photos_rejected(self):
        detail = _detail("A")
        del detail["photos"]
        queries = _queries(_bootstrap("A"), detail)
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), self._patch_operation(), queries, 0)
        self.assertIn("照片", str(cm.exception))

    def test_patch_missing_edit_policy_rejected(self):
        detail = _detail("A")
        del detail["edit_policy"]
        queries = _queries(_bootstrap("A"), detail)
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), self._patch_operation(), queries, 0)
        self.assertIn("编辑权限", str(cm.exception))

    def test_patch_cannot_edit_rejected(self):
        queries = _queries(_bootstrap("A"), _detail("A", can_edit=False))
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), self._patch_operation(), queries, 0)
        self.assertEqual(cm.exception.status, 409)

    def test_patch_crossscope_rejected(self):
        queries = _queries(_bootstrap("A"), _detail("B", record_id="rec-water"))
        with self.assertRaises(AssistantError):
            build_water_form(_actor(scopes=("A", "B")), self._patch_operation(), queries, 0)

    def test_patch_version_mismatch_rejected(self):
        queries = _queries(_bootstrap("A"), _detail("A", version="water-v2"))
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), self._patch_operation(body={"expected_version": "water-v1"}), queries, 0)
        self.assertEqual(cm.exception.status, 409)

    def test_patch_prefill_from_detail_and_retained_all_photos(self):
        photos = [_photo("img-a1", "table-a.jpg"), _photo("img-b1", "table-b.jpg")]
        queries = _queries(_bootstrap("A"), _detail("A", date="2026-09-30", meter_value=88.0, corrected_usage=7.5, photos=photos))
        _, control = build_water_form(_actor(), self._patch_operation(), queries, 0)
        initial = control["_initial_form"]
        self.assertEqual(initial["statistic_date"], "2026-09-30")
        self.assertEqual(initial["meter"], "中水")
        self.assertEqual(initial["frequency"], "日")
        self.assertEqual(initial["shift"], "白")
        self.assertEqual(initial["meter_value"], 88.0)
        self.assertEqual(initial["corrected_usage"], 7.5)
        self.assertEqual(initial["retained_image_ids"], ["img-a1", "img-b1"])
        self.assertEqual([p["image_id"] for p in control["_photos"]], ["img-a1", "img-b1"])

    def test_patch_explicit_null_overlay_preserved(self):
        queries = _queries(_bootstrap("A"), _detail("A", corrected_usage=7.5))
        operation = self._patch_operation(body={"expected_version": "water-v1", "corrected_usage": None})
        _, control = build_water_form(_actor(), operation, queries, 0)
        self.assertIsNone(control["_initial_form"]["corrected_usage"])

    def test_private_photos_only_safe_fields(self):
        photos = [_photo("img-a1", "table-a.jpg", file_token="tok", url="https://x", tmp_url="tmp", download_url="dl")]
        queries = _queries(_bootstrap("A"), _detail("A", photos=photos))
        _, control = build_water_form(_actor(), self._patch_operation(), queries, 0)
        for photo in control["_photos"]:
            self.assertEqual(set(photo), {"image_id", "name"})
        public = str(control)
        self.assertNotIn("tok", public)
        self.assertNotIn("https://", public)

    def test_expected_version_not_editable(self):
        queries = _queries(_bootstrap("A"), _detail("A"))
        _, control = build_water_form(_actor(), self._patch_operation(), queries, 0)
        for child in control["children"]:
            self.assertNotEqual(child["path"], "expected_version")
        self.assertEqual(control["_version"], "water-v1")

    def test_newest_invalid_detail_does_not_fall_back_to_older_can_edit(self):
        older_ok = _detail("A", record_id="rec-water", version="water-v1", can_edit=True)
        newer_denied = _detail("A", record_id="rec-water", version="water-v2", can_edit=False)
        queries = _queries(_bootstrap("A"), older_ok, newer_denied)
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), self._patch_operation(), queries, 0)
        self.assertEqual(cm.exception.status, 409)

    def test_params_scope_normalized_into_native_body(self):
        queries = _queries(_bootstrap("A"), _detail("A"))
        operation = {"api_id": "PATCH /api/capacity/water/records/{record_id}",
                     "path_params": {"record_id": "rec-water"}, "params": {"scope": "A"},
                     "body": {"expected_version": "water-v1"}}
        native_body, _ = build_water_form(_actor(), operation, queries, 0)
        self.assertEqual(native_body["scope"], "A")

    def test_params_extra_keys_rejected(self):
        queries = _queries(_bootstrap("A"), _detail("A"))
        operation = self._patch_operation()
        operation["params"] = {"scope": "A", "forged": 1}
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), operation, queries, 0)
        self.assertIn("查询参数", str(cm.exception))

    def test_files_rejected_on_record_operation(self):
        queries = _queries(_bootstrap("A"), _detail("A"))
        operation = self._patch_operation()
        operation["files"] = {"photo": ["tok"]}
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), operation, queries, 0)
        self.assertIn("文件上传", str(cm.exception))

    def test_path_params_other_than_record_id_rejected(self):
        queries = _queries(_bootstrap("A"), _detail("A"))
        operation = self._patch_operation()
        operation["path_params"] = {"record_id": "rec-water", "forged": "x"}
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), operation, queries, 0)
        self.assertIn("路径参数", str(cm.exception))

    def test_patch_legacy_value_is_visible_selectable_option(self):
        detail = _detail("A", meter="旧表", frequency="半月", shift="中班")
        queries = _queries(_bootstrap("A"), detail)
        _, control = build_water_form(_actor(), self._patch_operation(), queries, 0)
        for path, old in (("meter", "旧表"), ("frequency", "半月"), ("shift", "中班")):
            child = _child(control, path)
            option = next((o for o in child["options"] if o["value"] == old), None)
            self.assertIsNotNone(option, "legacy value missing from {}".format(path))
            self.assertIn(old, option["label"])
        # 任意新值仍被拒绝。
        filled = {"meter": "任意新表", "meter_value": 1}
        with self.assertRaises(AssistantError):
            water_payload(control, filled, _actor())

    def test_patch_malformed_photo_entry_rejected(self):
        detail = _detail("A", photos=[_photo("img-a1", "a.jpg"), {"name": "broken"}])
        queries = _queries(_bootstrap("A"), detail)
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), self._patch_operation(), queries, 0)
        self.assertIn("照片", str(cm.exception))

    def test_patch_non_string_image_id_rejected(self):
        detail = _detail("A", photos=[{"image_id": ["x"], "name": "a.jpg"}])
        queries = _queries(_bootstrap("A"), detail)
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), self._patch_operation(), queries, 0)
        self.assertIn("照片", str(cm.exception))

    def test_patch_duplicate_image_id_rejected(self):
        detail = _detail("A", photos=[_photo("img-a1", "a.jpg"), _photo("img-a1", "b.jpg")])
        queries = _queries(_bootstrap("A"), detail)
        with self.assertRaises(AssistantError) as cm:
            build_water_form(_actor(), self._patch_operation(), queries, 0)
        self.assertIn("重复", str(cm.exception))

    def test_photo_name_prefers_file_name(self):
        detail = _detail("A", photos=[{"image_id": "img-a1", "file_name": "F.jpg", "name": "N.jpg"}])
        queries = _queries(_bootstrap("A"), detail)
        _, control = build_water_form(_actor(), self._patch_operation(), queries, 0)
        self.assertEqual(control["_photos"][0]["name"], "F.jpg")

    def test_photo_name_fallback_suffix_numbering(self):
        detail = _detail("A", photos=[{"image_id": "img-a1"}, {"image_id": "img-a2", "name": ""}])
        queries = _queries(_bootstrap("A"), detail)
        _, control = build_water_form(_actor(), self._patch_operation(), queries, 0)
        self.assertEqual([p["name"] for p in control["_photos"]], ["水表照片1", "水表照片2"])


class WaterPayloadTest(unittest.TestCase):
    def _post_field(self):
        operation = {"api_id": "POST /api/capacity/water/records",
                     "body": {"scope": "A", "operation_id": "op-post", "upload_ids": ["u-new"]}}
        _, field = build_water_form(_actor(), operation, _queries(_bootstrap("A")), 0)
        return field

    def _patch_field(self):
        operation = {"api_id": "PATCH /api/capacity/water/records/{record_id}",
                     "path_params": {"record_id": "rec-water"},
                     "body": {"expected_version": "water-v1", "operation_id": "op-patch", "upload_ids": ["u-old"]}}
        _, field = build_water_form(
            _actor(),
            operation,
            _queries(_bootstrap("A"), _detail("A", corrected_usage=4.5)),
            0,
        )
        return field

    def test_create_payload_requires_all_fields(self):
        field = self._post_field()
        filled = {}
        with self.assertRaises(AssistantError) as cm:
            water_payload(field, filled, _actor())
        self.assertIn("统计日期", str(cm.exception))

    def test_create_payload_full_valid(self):
        field = self._post_field()
        filled = {
            "statistic_date": "2026-10-02",
            "meter": "中水",
            "frequency": "日",
            "shift": "白",
            "meter_value": "120.5",
            "corrected_usage": "",
            "retained_image_ids": [],
        }
        body = water_payload(field, filled, _actor())
        self.assertEqual(set(body), {"meter", "frequency", "shift", "statistic_date",
                                     "meter_value", "corrected_usage", "retained_image_ids"})
        self.assertEqual(body["meter_value"], 120.5)
        self.assertIsNone(body["corrected_usage"])
        self.assertEqual(body["retained_image_ids"], [])
        for forbidden in ("scope", "upload_ids", "operation_id", "expected_version",
                          "large_change_confirmed", "abnormal_note"):
            self.assertNotIn(forbidden, body)

    def test_create_rejects_unknown_or_readonly_ids(self):
        field = self._post_field()
        bad = {
            "statistic_date": "2026-10-02", "meter": "中水", "frequency": "日", "shift": "白",
            "meter_value": 1, "corrected_usage": None, "retained_image_ids": [], "expected_version": "x",
        }
        with self.assertRaises(AssistantError) as cm:
            water_payload(field, bad, _actor())
        self.assertIn("只读或系统字段", str(cm.exception))

    def test_create_rejects_arbitrary_meter(self):
        field = self._post_field()
        filled = {"statistic_date": "2026-10-02", "meter": "虚构水表", "frequency": "日", "shift": "白",
                  "meter_value": 1, "corrected_usage": None, "retained_image_ids": []}
        with self.assertRaises(AssistantError):
            water_payload(field, filled, _actor())

    def test_payload_rejects_arbitrary_object_filled_value(self):
        field = self._post_field()
        filled = {"statistic_date": "2026-10-02", "meter": {"a": 1}, "frequency": "日", "shift": "白",
                  "meter_value": 1, "corrected_usage": None, "retained_image_ids": []}
        with self.assertRaises(AssistantError):
            water_payload(field, filled, _actor())

    def test_date_must_be_real_yyyy_mm_dd(self):
        field = self._post_field()
        for bad_date in ("2026-13-01", "2026/10/02", "2026-02-30", 20261002, True):
            filled = {"statistic_date": bad_date, "meter": "中水", "frequency": "日", "shift": "白",
                      "meter_value": 1, "corrected_usage": None, "retained_image_ids": []}
            with self.assertRaises(AssistantError, msg=repr(bad_date)):
                water_payload(field, filled, _actor())

    def test_number_bool_and_nonfinite_rejected(self):
        field = self._post_field()
        base = {"statistic_date": "2026-10-02", "meter": "中水", "frequency": "日", "shift": "白",
                "retained_image_ids": []}
        for value in (True, float("inf"), float("nan"), "abc"):
            filled = {**base, "meter_value": value, "corrected_usage": None}
            with self.assertRaises(AssistantError, msg=repr(value)):
                water_payload(field, filled, _actor())
        filled = {**base, "meter_value": 1, "corrected_usage": float("inf")}
        with self.assertRaises(AssistantError):
            water_payload(field, filled, _actor())

    def test_meter_value_non_negative(self):
        field = self._post_field()
        filled = {"statistic_date": "2026-10-02", "meter": "中水", "frequency": "日", "shift": "白",
                  "meter_value": -1, "corrected_usage": None, "retained_image_ids": []}
        with self.assertRaises(AssistantError):
            water_payload(field, filled, _actor())

    def test_corrected_usage_allow_negative_and_nullable(self):
        field = self._post_field()
        filled = {"statistic_date": "2026-10-02", "meter": "中水", "frequency": "日", "shift": "白",
                  "meter_value": 1, "corrected_usage": "-2.5", "retained_image_ids": []}
        body = water_payload(field, filled, _actor())
        self.assertEqual(body["corrected_usage"], -2.5)
        filled["corrected_usage"] = None
        body = water_payload(field, filled, _actor())
        self.assertIsNone(body["corrected_usage"])

    def test_patch_preserves_original_baseline_for_missing_fields(self):
        field = self._patch_field()
        filled = {"meter_value": 150.0}
        body = water_payload(field, filled, _actor())
        self.assertEqual(set(body), {"meter", "frequency", "shift", "statistic_date",
                                     "meter_value", "corrected_usage", "retained_image_ids"})
        self.assertEqual(body["meter"], "中水")
        self.assertEqual(body["frequency"], "日")
        self.assertEqual(body["shift"], "白")
        self.assertEqual(body["statistic_date"], "2026-10-01")
        self.assertEqual(body["meter_value"], 150.0)
        self.assertEqual(body["corrected_usage"], 4.5)
        for forbidden in ("scope", "upload_ids", "operation_id", "expected_version",
                          "large_change_confirmed", "abnormal_note"):
            self.assertNotIn(forbidden, body)

    def test_patch_required_field_not_dropped(self):
        # 必填项缺失时保留原基线（不静默丢弃）；显式清空必填项则报错。
        field = self._patch_field()
        filled = {"meter": "市政水", "frequency": "周"}
        body = water_payload(field, filled, _actor())
        self.assertEqual(body["meter"], "市政水")
        self.assertEqual(body["frequency"], "周")
        self.assertEqual(body["shift"], "白")  # 原基线保留
        with self.assertRaises(AssistantError) as cm:
            water_payload(field, {**filled, "shift": ""}, _actor())
        self.assertIn("必填字段不能省略", str(cm.exception))

    def test_patch_legacy_original_choice_retained_but_arbitrary_rejected(self):
        # 详情水表“旧表”不在当前 bootstrap 选项，但可保留原值；不能新选任意值。
        detail = _detail("A", meter="旧表")
        operation = {"api_id": "PATCH /api/capacity/water/records/{record_id}",
                     "path_params": {"record_id": "rec-water"}, "body": {"expected_version": "water-v1"}}
        _, field = build_water_form(_actor(), operation, _queries(_bootstrap("A"), detail), 0)
        body = water_payload(field, {"meter_value": 1}, _actor())
        self.assertEqual(body["meter"], "旧表")
        filled = {"meter": "任意新表", "meter_value": 1}
        with self.assertRaises(AssistantError):
            water_payload(field, filled, _actor())

    def test_retained_image_ids_must_come_from_original_photos(self):
        field = self._patch_field()
        filled = {"meter_value": 150.0, "retained_image_ids": ["forged-id"]}
        with self.assertRaises(AssistantError):
            water_payload(field, filled, _actor())
        filled["retained_image_ids"] = ["img-a1"]
        body = water_payload(field, filled, _actor())
        self.assertEqual(body["retained_image_ids"], ["img-a1"])

    def test_photos_may_be_empty(self):
        field = self._post_field()
        filled = {"statistic_date": "2026-10-02", "meter": "中水", "frequency": "日", "shift": "白",
                  "meter_value": 1, "corrected_usage": None, "retained_image_ids": []}
        body = water_payload(field, filled, _actor())
        self.assertEqual(body["retained_image_ids"], [])

    def test_upload_ids_never_returned_even_when_frozen(self):
        # 契约：可编辑结果永不返回 upload_ids；冻结的引用交由 native_body 保持，不在此覆盖。
        field = self._post_field()
        filled = {"statistic_date": "2026-10-02", "meter": "中水", "frequency": "日", "shift": "白",
                  "meter_value": 1, "corrected_usage": None, "retained_image_ids": []}
        body = water_payload(field, filled, _actor())
        self.assertNotIn("upload_ids", body)
        # native_body 仍冻结上传引用，供业务侧沿用。
        operation = {"api_id": "POST /api/capacity/water/records",
                     "body": {"scope": "A", "operation_id": "op-post", "upload_ids": [{"$result": {"step": 0, "path": "upload_id"}}]}}
        native, _ = build_water_form(_actor(), operation, _queries(_bootstrap("A")), 0)
        self.assertEqual(native["upload_ids"], [{"$result": {"step": 0, "path": "upload_id"}}])

    def test_water_payload_rejects_out_of_scope_actor(self):
        field = self._post_field()
        filled = {"statistic_date": "2026-10-02", "meter": "中水", "frequency": "日", "shift": "白",
                  "meter_value": 1, "corrected_usage": None, "retained_image_ids": []}
        with self.assertRaises(AssistantError) as cm:
            water_payload(field, filled, _actor(scopes=("B",)))
        self.assertEqual(cm.exception.status, 403)

    def test_water_payload_rejects_non_native_form(self):
        with self.assertRaises(AssistantError):
            water_payload({"native_water_record": False, "_scope": "A"}, {}, _actor())


if __name__ == "__main__":
    unittest.main()
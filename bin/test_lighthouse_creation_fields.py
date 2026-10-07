# -*- coding: utf-8 -*-
"""Focused tests for the assistant creation-field metadata helpers.

Covers the pure metadata builders ``_morning_meeting_fields`` /
``_drill_create_fields``, the assistant-only descriptor override for the
native ``POST /api/drills`` multipart endpoint, and the gateway-side
``_build_multipart`` enforcement (xlsx / non-empty / bounded 64MiB).

Only ``lighthouse_api.py`` is imported; no real business service is called and
nothing is persisted to the project. Temporary files are only used to exercise
the bounded path read and are cleaned up afterwards.
"""
import asyncio
import datetime as dt
import json
import os
import sys
import tempfile
import unittest
from contextlib import suppress
from pathlib import Path
from types import SimpleNamespace

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from fastapi import FastAPI, File, Form, UploadFile  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402

from lan_bitable_template_portal.lighthouse_api import (  # noqa: E402
    PortalAPICatalog,
    _DRILL_MAX,
    _beijing_datetime,
    _drill_create_fields,
    _morning_meeting_fields,
)
from lan_bitable_template_portal.lighthouse_ai import AssistantError  # noqa: E402


def _child(control, path):
    for item in control.get("children") or []:
        if item.get("path") == path:
            return item
    raise AssertionError(f"missing child {path}")


def _invoke_kwargs(api_id, body, files):
    request = SimpleNamespace(
        base_url="http://testserver/",
        headers={"cookie": "session=test", "origin": "http://testserver"},
        client=None,
    )
    return request, {"api_id": api_id, "body": body, "files": files}


class MorningMeetingFieldsTest(unittest.TestCase):
    def test_shape_and_literal_children(self):
        control = _morning_meeting_fields({})
        self.assertEqual(control["type"], "object")
        self.assertTrue(control["native_morning_meeting"])
        self.assertEqual({item["path"] for item in control["children"]},
                         {"date", "weather_condition", "dry_bulb_temperature", "wet_bulb_temperature"})
        for item in control["children"]:
            self.assertTrue(item.get("literal_key"))

    def test_date_required_and_weather_maxlength(self):
        control = _morning_meeting_fields({})
        self.assertTrue(_child(control, "date")["required"])
        weather = _child(control, "weather_condition")
        self.assertFalse(weather["required"])
        self.assertEqual(weather["maxlength"], 40)

    def test_temperature_nullable_number_bounds(self):
        control = _morning_meeting_fields({})
        for path in ("dry_bulb_temperature", "wet_bulb_temperature"):
            child = _child(control, path)
            self.assertTrue(child.get("nullable"))
            self.assertEqual(child["type"], "number")
            self.assertEqual(child["min"], -50)
            self.assertEqual(child["max"], 80)
            self.assertEqual(child["step"], 0.1)

    def test_default_date_is_beijing_today_and_no_zero_default(self):
        control = _morning_meeting_fields({})
        form = control["_initial_form"]
        self.assertRegex(form["date"], r"^\d{4}-\d{2}-\d{2}$")
        self.assertIsNone(form["dry_bulb_temperature"])
        self.assertIsNone(form["wet_bulb_temperature"])

    def test_default_date_is_today_not_old_preview_date(self):
        today = _beijing_datetime().strftime("%Y-%m-%d")
        control = _morning_meeting_fields({}, {"date": "2020-01-01", "weather_condition": "多云"})
        self.assertEqual(control["_initial_form"]["date"], today)
        self.assertEqual(control["_initial_form"]["weather_condition"], "")

    def test_date_metadata_min_max_today(self):
        today = _beijing_datetime().strftime("%Y-%m-%d")
        date_child = _child(_morning_meeting_fields({}), "date")
        self.assertEqual(date_child["min"], today)
        self.assertEqual(date_child["max"], today)

    def test_invalid_explicit_date_raises(self):
        with self.assertRaises(AssistantError):
            _morning_meeting_fields({"date": "不是日期"})

    def test_invalid_explicit_number_raises(self):
        with self.assertRaises(AssistantError):
            _morning_meeting_fields({"date": "2026-10-03", "dry_bulb_temperature": "abc"})

    def test_weather_over_max_length_rejects_not_truncates(self):
        with self.assertRaises(AssistantError):
            _morning_meeting_fields({"date": "2026-10-03", "weather_condition": "晴" * 41})

    def test_explicit_values_override_preview(self):
        body = {"date": "2026-10-03", "weather_condition": "晴",
                "dry_bulb_temperature": 25.5, "wet_bulb_temperature": 22.0}
        preview = {"date": "2026-10-03", "weather_condition": "多云",
                   "dry_bulb_temperature": 11.0, "wet_bulb_temperature": 10.0}
        form = _morning_meeting_fields(body, preview)["_initial_form"]
        self.assertEqual(form["weather_condition"], "晴")
        self.assertEqual(form["dry_bulb_temperature"], 25.5)
        self.assertEqual(form["wet_bulb_temperature"], 22.0)

    def test_preview_only_when_same_date(self):
        form = _morning_meeting_fields(
            {"date": "2026-10-03"},
            {"date": "2026-10-03", "weather_condition": "多云",
             "dry_bulb_temperature": 25.5, "wet_bulb_temperature": 22.0},
        )["_initial_form"]
        self.assertEqual(form["weather_condition"], "多云")
        self.assertEqual(form["dry_bulb_temperature"], 25.5)
        self.assertEqual(form["wet_bulb_temperature"], 22.0)

        form = _morning_meeting_fields(
            {"date": "2026-10-04"},
            {"date": "2026-10-03", "weather_condition": "多云",
             "dry_bulb_temperature": 25.5, "wet_bulb_temperature": 22.0},
        )["_initial_form"]
        self.assertEqual(form["weather_condition"], "")
        self.assertIsNone(form["dry_bulb_temperature"])
        self.assertIsNone(form["wet_bulb_temperature"])

    def test_explicit_null_clears_temperature_but_not_zero(self):
        form = _morning_meeting_fields(
            {"date": "2026-10-03", "dry_bulb_temperature": None, "wet_bulb_temperature": None},
            {"date": "2026-10-03", "dry_bulb_temperature": 25.5, "wet_bulb_temperature": 22.0},
        )["_initial_form"]
        self.assertIsNone(form["dry_bulb_temperature"])
        self.assertIsNone(form["wet_bulb_temperature"])

    def test_only_safe_allowed_fields_consumed(self):
        control = _morning_meeting_fields(
            {"date": "2026-10-03", "operation_id": "secret", "actor_name": "x", "access_token": "y"},
            {"date": "2026-10-03", "operation_id": "leak"},
        )
        self.assertEqual(set(control["_initial_form"]), {
            "date", "weather_condition", "dry_bulb_temperature", "wet_bulb_temperature"})
        self.assertNotIn("operation_id", control["_initial_form"])
        self.assertNotIn("actor_name", control["_initial_form"])

    def test_explicit_temperature_bounds_and_bool_rejected(self):
        for change in ({"dry_bulb_temperature": 81}, {"wet_bulb_temperature": -51},
                       {"dry_bulb_temperature": 80.1}, {"wet_bulb_temperature": -50.1},
                       {"dry_bulb_temperature": True}, {"wet_bulb_temperature": False},
                       {"dry_bulb_temperature": float("inf")}, {"wet_bulb_temperature": float("nan")}):
            with self.subTest(change=change), self.assertRaises(AssistantError):
                _morning_meeting_fields({"date": "2026-10-03", **change})

    def test_explicit_temperature_boundaries_accepted(self):
        form = _morning_meeting_fields({"date": "2026-10-03",
                                        "dry_bulb_temperature": 80, "wet_bulb_temperature": -50})["_initial_form"]
        self.assertEqual(form["dry_bulb_temperature"], 80.0)
        self.assertEqual(form["wet_bulb_temperature"], -50.0)

    def test_preview_invalid_temperature_does_not_prefill(self):
        for bad in (81, -51, True, False, float("inf"), float("nan")):
            preview = {"date": "2026-10-03",
                       "dry_bulb_temperature": bad, "wet_bulb_temperature": bad}
            form = _morning_meeting_fields({"date": "2026-10-03"}, preview)["_initial_form"]
            with self.subTest(bad=bad):
                self.assertIsNone(form["dry_bulb_temperature"])
                self.assertIsNone(form["wet_bulb_temperature"])

    def test_preview_temperature_boundaries_prefill(self):
        preview = {"date": "2026-10-03", "dry_bulb_temperature": 80, "wet_bulb_temperature": -50}
        form = _morning_meeting_fields({"date": "2026-10-03"}, preview)["_initial_form"]
        self.assertEqual(form["dry_bulb_temperature"], 80.0)
        self.assertEqual(form["wet_bulb_temperature"], -50.0)

    def test_date_rejects_datetime_and_nonstring(self):
        for value in (dt.datetime(2026, 10, 3), dt.datetime(2026, 10, 3, 12, 0),
                      dt.date(2026, 10, 3), ["2026-10-03"], {"date": "2026-10-03"}, 1780531200):
            with self.subTest(value=value), self.assertRaises(AssistantError):
                _morning_meeting_fields({"date": value})

    def test_weather_condition_rejects_arbitrary_object(self):
        for value in ({"sky": "晴"}, ["晴"], 123, True):
            with self.subTest(value=value), self.assertRaises(AssistantError):
                _morning_meeting_fields({"date": "2026-10-03", "weather_condition": value})

    def test_preview_weather_arbitrary_object_not_prefilled(self):
        for value in ({"sky": "晴"}, ["晴"], 123, True):
            preview = {"date": "2026-10-03", "weather_condition": value}
            form = _morning_meeting_fields({"date": "2026-10-03"}, preview)["_initial_form"]
            with self.subTest(value=value):
                self.assertEqual(form["weather_condition"], "")


class DrillCreateFieldsTest(unittest.TestCase):
    def test_shape_and_children(self):
        control = _drill_create_fields({}, ["A", "B", "C", "D", "E"])
        self.assertEqual(control["type"], "object")
        self.assertTrue(control["native_drill_create"])
        paths = {item["path"] for item in control["children"]}
        self.assertEqual(paths, {"name", "month", "assigned_scopes"})
        self.assertTrue(all(item.get("literal_key") for item in control["children"]))
        self.assertNotIn("year", paths)

    def test_children_constraints(self):
        control = _drill_create_fields({}, ["A", "B", "C", "D", "E"])
        self.assertFalse(_child(control, "name")["required"])
        month = _child(control, "month")
        self.assertTrue(month["required"])
        self.assertEqual(month["type"], "month")
        assigned = _child(control, "assigned_scopes")
        self.assertTrue(assigned["required"])
        self.assertEqual(assigned["type"], "multiselect")
        self.assertTrue(assigned["choice_group"])
        self.assertTrue(assigned["multi"])
        self.assertEqual(assigned["minItems"], 1)
        self.assertEqual([item["value"] for item in assigned["options"]],
                         ["A", "B", "C", "D", "E"])

    def test_default_authorized_scopes_a_to_e_when_absent(self):
        form = _drill_create_fields({"year": "2026", "month": "10"}, ["A", "B", "C", "D", "E"])["_initial_form"]
        self.assertEqual(form["assigned_scopes"], ["A", "B", "C", "D", "E"])

    def test_default_intersects_authorized_scopes(self):
        form = _drill_create_fields({}, ["B", "D"])["_initial_form"]
        self.assertEqual(form["assigned_scopes"], ["B", "D"])

    def test_explicit_empty_scopes_preserved(self):
        form = _drill_create_fields({"assigned_scopes": []}, ["A", "B", "C"])["_initial_form"]
        self.assertEqual(form["assigned_scopes"], [])

    def test_default_month_is_beijing_current(self):
        from lan_bitable_template_portal.lighthouse_api import _beijing_datetime
        form = _drill_create_fields({}, ["A", "B"])["_initial_form"]
        self.assertEqual(form["month"], _beijing_datetime().strftime("%Y-%m"))

    def test_accepts_year_and_numeric_month(self):
        form = _drill_create_fields({"year": "2026", "month": "10"}, ["A"])["_initial_form"]
        self.assertEqual(form["month"], "2026-10")

    def test_accepts_month_yyyy_mm_and_coherence_error(self):
        form = _drill_create_fields({"year": "2026", "month": "2026-10"}, ["A"])["_initial_form"]
        self.assertEqual(form["month"], "2026-10")
        with self.assertRaises(AssistantError):
            _drill_create_fields({"year": "2025", "month": "2026-10"}, ["A"])

    def test_rejects_malformed_year(self):
        with self.assertRaises(AssistantError):
            _drill_create_fields({"year": "abc2026", "month": "10"}, ["A"])

    def test_rejects_year_out_of_range(self):
        with self.assertRaises(AssistantError):
            _drill_create_fields({"year": "1999", "month": "10"}, ["A"])
        with self.assertRaises(AssistantError):
            _drill_create_fields({"year": "2101", "month": "10"}, ["A"])

    def test_rejects_malformed_month_string(self):
        with self.assertRaises(AssistantError):
            _drill_create_fields({"year": "2026", "month": "1x"}, ["A"])

    def test_absent_scopes_deny_not_grant_all(self):
        form_none = _drill_create_fields({"year": "2026", "month": "10"}, None)["_initial_form"]
        self.assertEqual(form_none["assigned_scopes"], [])
        control = _drill_create_fields({"year": "2026", "month": "10"}, "not-a-list")
        self.assertEqual(_child(control, "assigned_scopes")["options"], [])
        self.assertEqual(control["_initial_form"]["assigned_scopes"], [])

    def test_assigned_scopes_reject_unknown_duplicate_and_unauthorized(self):
        with self.assertRaises(AssistantError):
            _drill_create_fields({"year": "2026", "month": "10", "assigned_scopes": ["A", "Z"]}, ["A"])
        with self.assertRaises(AssistantError):
            _drill_create_fields({"year": "2026", "month": "10", "assigned_scopes": ["A", "A"]}, ["A"])
        with self.assertRaises(AssistantError):
            _drill_create_fields({"year": "2026", "month": "10", "assigned_scopes": ["B"]}, ["A"])

    def test_drill_name_rejects_arbitrary_object(self):
        for value in ({"text": "演练"}, ["演练"], 123, True):
            with self.subTest(value=value), self.assertRaises(AssistantError):
                _drill_create_fields({"year": "2026", "month": "10", "name": value}, ["A"])

    def test_drill_scopes_reject_arbitrary_object_items(self):
        for value in ([{"code": "A"}], [(1,)], [None]):
            with self.subTest(value=value), self.assertRaises(AssistantError):
                _drill_create_fields({"year": "2026", "month": "10", "assigned_scopes": value}, ["A"])


class DrillDescriptorTest(unittest.TestCase):
    def setUp(self):
        self.received = {}

        app = FastAPI()

        @app.post("/api/drills")
        async def drill_create(
            file: UploadFile = File(...),
            name: str = Form(""),
            year: str = Form(""),
            month: str = Form(""),
            assigned_scopes: str = Form(""),
        ):
            content = await file.read()
            self.received.update({
                "name": name,
                "year": year,
                "month": month,
                "assigned_scopes": assigned_scopes,
                "filename": file.filename or "",
                "content": content,
            })
            return JSONResponse({"ok": True, "data": {"size": len(content)}})

        self.app = app
        self.catalog = PortalAPICatalog(app)
        self.desc = self.catalog.get("POST /api/drills")

    def test_descriptor_body_schema_after_multipart_inference(self):
        schema = self.desc["schema"]["body"]
        self.assertEqual(set(schema["properties"]), {"name", "year", "month", "assigned_scopes"})
        self.assertEqual(schema["required"], ["year", "month", "assigned_scopes"])
        self.assertEqual(schema["properties"]["year"]["type"], "integer")
        self.assertEqual(schema["properties"]["year"]["minimum"], 2000)
        self.assertEqual(schema["properties"]["year"]["maximum"], 2100)
        self.assertEqual(schema["properties"]["month"]["type"], "string")
        scopes = schema["properties"]["assigned_scopes"]
        self.assertEqual(scopes["items"]["enum"], ["A", "B", "C", "D", "E"])
        self.assertEqual(scopes["minItems"], 1)
        self.assertTrue(scopes["uniqueItems"])

    def test_descriptor_file_metadata_and_max_one_file(self):
        self.assertTrue(self.desc["multipart"])
        self.assertEqual(self.desc["files"], ["file"])
        self.assertEqual(self.desc["file_max_items"], {"file": 1})
        limits = self.desc["file_limits"]
        self.assertEqual(limits["file"]["accept"], ".xlsx")
        self.assertEqual(limits["file"]["max_bytes"], _DRILL_MAX)
        self.assertEqual(limits["file"]["purpose"], "drill_template")

    def _invoke(self, body, files, provider):
        request, op = _invoke_kwargs("POST /api/drills", body, files)
        return asyncio.run(self.catalog.invoke(op, request, file_provider=provider))


class DrillMultipartUploadTest(DrillDescriptorTest):
    def test_serializes_assigned_scopes_and_scalars_to_fake_route(self):
        xlsx = b"PK\x03\x04" + b"\x00" * 128
        provider = lambda aid: {"name": "drill.xlsx", "mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "bytes": xlsx}  # noqa: E731
        result = self._invoke(
            {"name": "测试演练", "year": 2026, "month": "2026-10", "assigned_scopes": ["A", "B"]},
            {"file": ["attach-1"]},
            provider,
        )
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.received["name"], "测试演练")
        self.assertEqual(self.received["year"], "2026")
        self.assertEqual(self.received["month"], "2026-10")
        self.assertEqual(json.loads(self.received["assigned_scopes"]), ["A", "B"])
        self.assertEqual(self.received["filename"], "drill.xlsx")
        self.assertEqual(self.received["content"], xlsx)

    def test_rejects_non_xlsx_drill_file(self):
        provider = {"name": "drill.txt", "mime": "text/plain", "bytes": b"PK\x03\x04"}
        request, op = _invoke_kwargs("POST /api/drills",
                                     {"year": 2026, "month": "2026-10", "assigned_scopes": ["A"]},
                                     {"file": ["attach-1"]})
        with self.assertRaises(AssistantError):
            asyncio.run(self.catalog.invoke(op, request, file_provider=lambda aid: provider))

    def test_rejects_empty_drill_file(self):
        provider = {"name": "empty.xlsx", "mime": "application/octet-stream", "bytes": b""}
        request, op = _invoke_kwargs("POST /api/drills",
                                     {"year": 2026, "month": "2026-10", "assigned_scopes": ["A"]},
                                     {"file": ["attach-1"]})
        with self.assertRaises(AssistantError):
            asyncio.run(self.catalog.invoke(op, request, file_provider=lambda aid: provider))

    def test_accepts_64_mib_xlsx_bytes(self):
        xlsx = b"PK\x03\x04" + b"\x00" * (_DRILL_MAX - 4)
        provider = {"name": "big.xlsx", "mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "bytes": xlsx}
        request, op = _invoke_kwargs("POST /api/drills",
                                     {"year": 2026, "month": "2026-10", "assigned_scopes": ["A"]},
                                     {"file": ["attach-1"]})
        result = asyncio.run(self.catalog.invoke(op, request, file_provider=lambda aid: provider))
        self.assertTrue(result["ok"], result)
        self.assertEqual(len(self.received["content"]), len(xlsx))

    def test_rejects_over_64_mib_drill_bytes(self):
        provider = {"name": "huge.xlsx", "mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "bytes": b"PK\x03\x04" + b"\x00" * _DRILL_MAX}
        request, op = _invoke_kwargs("POST /api/drills",
                                     {"year": 2026, "month": "2026-10", "assigned_scopes": ["A"]},
                                     {"file": ["attach-1"]})
        with self.assertRaises(AssistantError):
            asyncio.run(self.catalog.invoke(op, request, file_provider=lambda aid: provider))

    def test_rejects_over_64_mib_drill_path_via_bounded_read(self):
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as handle:
            handle.write(b"PK\x03\x04" + b"\x00" * (_DRILL_MAX + 1))
            path = handle.name
        try:
            provider = {"name": "huge_path.xlsx", "path": path, "mime": "application/octet-stream"}
            request, op = _invoke_kwargs("POST /api/drills",
                                         {"year": 2026, "month": "2026-10", "assigned_scopes": ["A"]},
                                         {"file": ["attach-1"]})
            with self.assertRaises(AssistantError):
                asyncio.run(self.catalog.invoke(op, request, file_provider=lambda aid: provider))
        finally:
            with suppress(OSError):
                os.unlink(path)


class OtherMultipartLimitTest(unittest.TestCase):
    def test_other_api_still_uses_20_mib(self):
        received = {}
        app = FastAPI()

        @app.post("/api/engineer/mop/upload")
        async def mop_upload(file: UploadFile = File(...), note: str = Form("")):
            received["content"] = await file.read()
            return {"ok": True, "data": {"size": len(received["content"])}}

        catalog = PortalAPICatalog(app)
        request, op = _invoke_kwargs("POST /api/engineer/mop/upload",
                                     {"note": "x"}, {"file": ["attach-1"]})
        big = b"\x00" * (20 * 1024 * 1024 + 1)
        with self.assertRaises(AssistantError):
            asyncio.run(catalog.invoke(op, request, file_provider=lambda aid: {
                "name": "x.bin", "mime": "application/octet-stream", "bytes": big}))


if __name__ == "__main__":
    unittest.main(verbosity=2)
# -*- coding: utf-8 -*-
"""Bounded tests for ``_notice_frontend_fields`` in lighthouse_api.py.

The helper mirrors ``workbench_lite._form_fields`` for the six non-event native
work types (maintenance/change/repair/power/polling/adjust).  These tests render
the REAL native HTML and parse it with the stdlib ``HTMLParser``, then verify the
descriptor children agree on keys, labels, required flags, control types and
select/multiselect options.  They also check:

  * building scopes are limited to the authorized list,
  * event (and unknown) work types are rejected,
  * ``maintenance_cycle`` stays a plain custom-text control surfaced with a
    suggestion list, never a restrictive select.
"""
import sys
import unittest
from html.parser import HTMLParser
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal.lighthouse_api import AssistantError, _notice_frontend_fields
from lan_bitable_template_portal.workbench_lite import BUILDING_FORM_OPTIONS, MAINTENANCE_CYCLE_OPTIONS, _form_fields

NON_EVENT_WORK_TYPES = ("maintenance", "change", "repair", "power", "polling", "adjust")
ALL_SCOPES = ["110", "A", "B", "C", "D", "E", "H"]


class _NativeFormParser(HTMLParser):
    """Parse the native ``_form_fields`` HTML into ``{name: field}``.

    The hidden derived ``building`` input is excluded and the ``building_codes``
    checkboxes are unified into a single multiselect whose option values are the
    checkbox values.  Every regular control is the sole control inside its
    ``<label>``, whose first ``<span>`` holds the label; the building picker is
    a ``<fieldset data-building-picker>`` whose ``<legend>`` holds its label.
    """

    def __init__(self):
        super().__init__()
        self.fields = {}
        self.order = []
        # regular <label> state
        self._in_label = False
        self._label_required = False
        self._label_span = []
        self._in_label_span = False
        self._label_name = None
        self._label_control = None
        self._select_options = None
        self._in_select = False
        # Hint text (small inside the label span) is presentation-only and is
        # stripped from the label comparison.
        self._in_hint = False
        # building <fieldset> state
        self._in_building = False
        self._building_legend = []
        self._building_options = []

    def _append_order(self, name):
        if name not in self.order:
            self.order.append(name)

    def _finish_label(self):
        if self._label_name:
            entry = {
                "name": self._label_name,
                "label": "".join(self._label_span).strip(),
                "required": self._label_required,
                "type": self._label_control or "text",
                "options": self._select_options if self._label_control == "select" else None,
            }
            self.fields[self._label_name] = entry
            self._append_order(self._label_name)
        self._in_label = False
        self._label_required = False
        self._label_span = []
        self._in_label_span = False
        self._label_name = None
        self._label_control = None
        self._select_options = None
        self._in_select = False

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        attrs = dict(attrs)
        if tag == "fieldset" and "data-building-picker" in attrs:
            self._in_building = True
            self._building_legend = []
            self._building_options = []
            return
        if self._in_building:
            if tag == "legend":
                self._label_span = []
                self._in_label_span = True
            elif tag == "input":
                name = attrs.get("name")
                if name == "building_codes":
                    value = attrs.get("value", "")
                    if value not in self._building_options:
                        self._building_options.append(value)
                # hidden input[name=building] is intentionally skipped
            return
        if tag == "label":
            self._in_label = True
            classes = (attrs.get("class") or "").split()
            self._label_required = "required" in classes
            self._label_span = []
            self._in_label_span = False
            self._label_name = None
            self._label_control = None
            self._select_options = None
            self._in_select = False
            return
        if not self._in_label:
            return
        if tag == "span":
            self._in_label_span = True
        elif tag == "small":
            self._in_hint = True
        elif tag == "input":
            self._label_name = attrs.get("name")
            self._label_control = attrs.get("type", "text")
        elif tag == "textarea":
            self._label_name = attrs.get("name")
            self._label_control = "textarea"
        elif tag == "select":
            self._label_name = attrs.get("name")
            self._label_control = "select"
            self._select_options = []
            self._in_select = True
        elif tag == "option" and self._in_select:
            self._select_options.append(attrs.get("value", ""))

    def handle_startendtag(self, tag, attrs):
        tag = tag.lower()
        if self._in_select and tag == "option":
            self._select_options.append(dict(attrs).get("value", ""))

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag == "fieldset" and self._in_building:
            self.fields["building_codes"] = {
                "name": "building_codes",
                "label": "".join(self._building_legend).strip() or "楼栋/范围",
                "required": True,
                "type": "multiselect",
                "options": self._building_options,
            }
            self._append_order("building_codes")
            self._in_building = False
            self._in_label_span = False
            return
        if self._in_building:
            if tag == "legend":
                self._in_label_span = False
            return
        if tag == "span" and self._in_label:
            self._in_label_span = False
        elif tag == "small" and self._in_hint:
            self._in_hint = False
        elif tag == "select" and self._in_select:
            self._in_select = False
        elif tag == "label" and self._in_label:
            self._finish_label()

    def handle_data(self, data):
        if self._in_building and self._in_label_span:
            self._building_legend.append(data)
        elif self._in_label and self._in_label_span and not self._in_hint:
            self._label_span.append(data)


def _parse_native(work_type, scope="A"):
    html = _form_fields(work_type, {}, scope=scope)
    parser = _NativeFormParser()
    parser.feed(html)
    return parser.fields


def _descriptor_by_path(desc):
    return {child["path"]: child for child in desc["children"]}


def _option_values(entries):
    return [entry["value"] for entry in entries]


class NoticeFrontendFieldsConsistency(unittest.TestCase):
    """Descriptor children must reconcile with the native _form_fields."""

    def test_all_six_native_work_types_match_native_fields(self):
        for work_type in NON_EVENT_WORK_TYPES:
            with self.subTest(work_type=work_type):
                desc = _notice_frontend_fields(work_type, list(ALL_SCOPES))
                native = _parse_native(work_type)
                desc_fields = _descriptor_by_path(desc)
                # Same input keys as the native form (hidden building excluded).
                self.assertEqual(
                    set(desc_fields), set(native),
                    f"work_type={work_type} field mismatch",
                )
                for key, child in desc_fields.items():
                    native_field = native[key]
                    self.assertEqual(
                        child["label"], native_field["label"],
                        f"{work_type}/{key} label",
                    )
                    self.assertEqual(
                        child["required"], native_field["required"],
                        f"{work_type}/{key} required",
                    )
                    self.assertEqual(
                        child["type"], native_field["type"],
                        f"{work_type}/{key} type",
                    )
                    if child["type"] == "select":
                        self.assertEqual(
                            _option_values(child["options"]),
                            native_field["options"],
                            f"{work_type}/{key} options",
                        )

    def test_native_notice_object_shape(self):
        for work_type in NON_EVENT_WORK_TYPES:
            with self.subTest(work_type=work_type):
                desc = _notice_frontend_fields(work_type, list(ALL_SCOPES))
                self.assertEqual(desc["type"], "object")
                self.assertIs(desc["native_notice"], True)
                self.assertTrue(desc["children"])
                for child in desc["children"]:
                    self.assertIs(child["literal_key"], True)
                    # No values / no identity files or tokens.
                    self.assertNotIn("value", child)
                    self.assertNotIn("file_token", child)
                    self.assertNotIn("open_id", child)

    def test_building_codes_is_unified_required_multiselect(self):
        for work_type in NON_EVENT_WORK_TYPES:
            with self.subTest(work_type=work_type):
                desc = _notice_frontend_fields(work_type, list(ALL_SCOPES))
                building = _descriptor_by_path(desc)["building_codes"]
                self.assertEqual(building["type"], "multiselect")
                self.assertIs(building["choice_group"], True)
                self.assertIs(building["required"], True)
                self.assertEqual(building["minItems"], 1)
                self.assertEqual(
                    set(_option_values(building["options"])), set(dict(BUILDING_FORM_OPTIONS))
                )

    def test_building_scopes_are_limited(self):
        scopes = ["B", "D"]
        expected = [{"value": "B", "label": "B楼"}, {"value": "D", "label": "D楼"}]
        for work_type in NON_EVENT_WORK_TYPES:
            with self.subTest(work_type=work_type):
                desc = _notice_frontend_fields(work_type, list(scopes))
                building = _descriptor_by_path(desc)["building_codes"]
                self.assertEqual(building["options"], expected)
                # Unauthorized scopes never leak into any other work type.
                for child in desc["children"]:
                    if child["type"] == "multiselect":
                        self.assertTrue(
                            all(value in scopes for value in _option_values(child["options"]))
                        )

    def test_event_and_unknown_work_types_rejected(self):
        for bad in ("event", "unknown", "", "  "):
            with self.subTest(bad=repr(bad)):
                with self.assertRaises(AssistantError):
                    _notice_frontend_fields(bad, list(ALL_SCOPES))

    def test_maintenance_cycle_custom_text_not_restricted(self):
        desc = _notice_frontend_fields("maintenance", list(ALL_SCOPES))
        cycle = _descriptor_by_path(desc)["maintenance_cycle"]
        # Plain text control so a valid custom cycle is never constrained.
        self.assertEqual(cycle["type"], "text")
        self.assertNotIn("options", cycle)
        self.assertNotIn("enum", cycle)
        self.assertNotIn("minItems", cycle)
        self.assertNotIn("maxItems", cycle)
        self.assertNotIn("datalist", cycle)
        # Only the non-restrictive native cycle suggestions are consumed by the
        # frontend; no legacy datalist name is emitted.
        self.assertEqual(cycle.get("suggestions"), list(MAINTENANCE_CYCLE_OPTIONS))

    def test_adjustment_keeps_native_progress_but_does_not_show_it_in_chat(self):
        for kind in NON_EVENT_WORK_TYPES:
            progress = _descriptor_by_path(_notice_frontend_fields(kind, list(ALL_SCOPES)))['progress']
            self.assertEqual(bool(progress.get('hidden')), kind == 'adjust')


if __name__ == "__main__":
    unittest.main(verbosity=2)

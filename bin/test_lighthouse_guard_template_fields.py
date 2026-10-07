# -*- coding: utf-8 -*-
"""Contract tests for ``lighthouse_api._guard_template_frontend_fields``.

Codex is adding ``_guard_template_frontend_fields(sheet_type)`` as a bounded
PURE descriptor builder: it accepts a native critical-guard check sheet name,
validates it through the native ``critical_guard.sheet_definition``, and returns
an ``array`` frontend descriptor whose ``item`` lets the editor create/edit one
check item.  This suite is a small, pure local contract test -- it reads the
local ``重保戒备检查表.xlsx`` via ``critical_guard_catalog`` /
``default_check_items`` and never starts a service, hits the network or touches
credentials.

Covered assertions:
- every native check sheet returns an array descriptor with identity_key='key',
  native minItems=1/maxItems=200 and an object ``item``;
- item children use the contract child types/limits: hidden ``key`` with
  generate='uuid'; text ``category`` maxlength=200 optional; textarea
  ``content`` maxlength=2000 required;
- non-check sheets (materials/contacts) raise the portal ``AssistantError``;
- the descriptor stays JSON-serializable.
"""
import json
import sys
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal import critical_guard  # noqa: E402
from lan_bitable_template_portal.lighthouse_api import (  # noqa: E402
    AssistantError,
    _guard_template_frontend_fields,
)


def _item_child(item: dict, path: str) -> dict:
    for child in item["children"]:
        if child.get("path") == path:
            return child
    raise AssertionError(
        f"item children 缺少 {path!r}: {[c.get('path') for c in item['children']]}"
    )


class GuardTemplateFrontendFieldsTests(unittest.TestCase):
    """Pure descriptor contract coverage for the guard template fields helper."""

    def test_all_native_check_sheets_build_array_descriptor(self) -> None:
        check_sheets = sorted(critical_guard.CRITICAL_GUARD_CHECK_SHEETS)
        self.assertTrue(check_sheets, "应有原生检查表")
        for sheet in check_sheets:
            with self.subTest(sheet=sheet):
                desc = _guard_template_frontend_fields(sheet)
                self.assertEqual(desc["type"], "array")
                # Native bounds surfaced on the descriptor.
                self.assertEqual(desc["minItems"], 1)
                self.assertEqual(desc["maxItems"], 200)
                self.assertEqual(desc["identity_key"], "key")
                item = desc["item"]
                self.assertEqual(item["type"], "object")
                child_paths = {child["path"] for child in item["children"]}
                self.assertIn("key", child_paths)
                self.assertIn("category", child_paths)
                self.assertIn("content", child_paths)

    def test_item_key_is_hidden_uuid_not_editable(self) -> None:
        desc = _guard_template_frontend_fields("设备安全")
        key = _item_child(desc["item"], "key")
        # The renderer (LighthouseStructuredField.vue) filters children by the
        # ``hidden`` flag and renders a "text" scalar with a native text input.
        self.assertIs(key["hidden"], True)
        self.assertEqual(key["type"], "text")
        self.assertEqual(key["generate"], "uuid")
        # Hidden generated key must never be required to be typed by the editor.
        self.assertIs(key.get("required", False), False)

    def test_item_category_text_maxlength200_optional(self) -> None:
        desc = _guard_template_frontend_fields("环境安全")
        category = _item_child(desc["item"], "category")
        self.assertEqual(category["type"], "text")
        self.assertEqual(category["maxlength"], 200)
        self.assertIs(category.get("required", False), False)

    def test_item_content_textarea_maxlength2000_required(self) -> None:
        desc = _guard_template_frontend_fields("客户重保")
        content = _item_child(desc["item"], "content")
        self.assertEqual(content["type"], "textarea")
        self.assertEqual(content["maxlength"], 2000)
        self.assertIs(content["required"], True)

    def test_descriptor_uses_native_min_max_values(self) -> None:
        # Native template contract: at least one item, at most the native cap.
        for sheet in critical_guard.CRITICAL_GUARD_CHECK_SHEETS:
            with self.subTest(sheet=sheet):
                desc = _guard_template_frontend_fields(sheet)
                self.assertEqual(desc["minItems"], 1)
                self.assertEqual(
                    desc["maxItems"],
                    critical_guard.CRITICAL_GUARD_MAX_CHECK_ITEMS,
                )
                self.assertEqual(critical_guard.CRITICAL_GUARD_MAX_CHECK_ITEMS, 200)

    def test_non_check_sheets_raise_assistant_error(self) -> None:
        for sheet in sorted(critical_guard.CRITICAL_GUARD_SHEET_NAMES):
            definition = critical_guard.sheet_definition(sheet)
            if definition["kind"] == "check":
                continue
            with self.subTest(sheet=sheet):
                with self.assertRaises(AssistantError):
                    _guard_template_frontend_fields(sheet)

    def test_descriptor_is_json_serializable(self) -> None:
        for sheet in critical_guard.CRITICAL_GUARD_CHECK_SHEETS:
            with self.subTest(sheet=sheet):
                desc = _guard_template_frontend_fields(sheet)
                payload = json.dumps(desc, ensure_ascii=False)
                self.assertIn('"identity_key": "key"', payload)
                self.assertIn('"generate": "uuid"', payload)


if __name__ == "__main__":
    unittest.main(verbosity=2)
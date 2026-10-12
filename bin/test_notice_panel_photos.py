"""Isolated photo-upload regression tests for the assistant notice panel.

No Feishu cards, no network, no cloud writes. Exercises the native photo field
descriptors, backend receipt validation, the legacy edit/failed migration, and
the module-gated panel filtering/rejection in isolation with local mocks.
"""
from __future__ import annotations

import copy
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))

from lan_bitable_template_portal import notice_panel as panel
from lan_bitable_template_portal import notice_panel_data as data
from lan_bitable_template_portal.portal_service import PortalError
from lan_bitable_template_portal.portal_service import MaintenancePortalService

from test_notice_panel import PanelTestBase, make_session


def receipt(upload_id="u-1"):
    return {"upload_id": upload_id, "file_name": "a.png", "mime_type": "image/png",
            "size": 100, "preview_url": "blob:local"}


class PhotoFieldDescriptorTests(PanelTestBase):
    """fields_for adds optional photo descriptors without touching others."""

    def test_site_photos_added_to_all_native_work_types(self):
        for work in ("maintenance", "change", "repair", "power", "polling", "adjust"):
            fields = data.fields_for(work, {"work_type": work}, "start")
            keys = [f["key"] for f in fields]
            self.assertIn("extra_images", keys)
            site = next(f for f in fields if f["key"] == "extra_images")
            self.assertFalse(site["required"])
            self.assertEqual(site["kind"], "photos")
            self.assertEqual(site["maxItems"], data.SITE_PHOTO_MAX)

    def test_ali_confirmation_only_for_change(self):
        for work in ("maintenance", "repair", "power", "polling", "adjust"):
            keys = [f["key"] for f in data.fields_for(work, {"work_type": work}, "start")]
            self.assertNotIn("ali_confirmation_images", keys)
        fields = data.fields_for("change", {"work_type": "change"}, "start")
        ali = next(f for f in fields if f["key"] == "ali_confirmation_images")
        self.assertFalse(ali["required"])
        self.assertEqual(ali["maxItems"], data.ALI_CONFIRMATION_MAX)


class PhotoReceiptValidationTests(PanelTestBase):
    """Exact arrays/counts validated before the native submission path."""

    def test_missing_arrays_default_to_empty(self):
        body = {"work_type": "maintenance"}
        data._validate_photo_arrays(body, "maintenance")
        self.assertEqual(body["extra_images"], [])
        self.assertNotIn("ali_confirmation_images", body)

    def test_invalid_photo_array_rejected(self):
        for work in ("maintenance", "change"):
            with self.assertRaisesRegex(PortalError, "格式无效"):
                data._validate_photo_arrays({"extra_images": "x"}, work)

    def test_site_photo_count_bounded(self):
        body = {"extra_images": [receipt(f"u-{i}") for i in range(data.SITE_PHOTO_MAX + 1)]}
        with self.assertRaisesRegex(PortalError, f"最多 {data.SITE_PHOTO_MAX} 张"):
            data._validate_photo_arrays(body, "maintenance")

    def test_invalid_receipt_rejected(self):
        with self.assertRaisesRegex(PortalError, "无效回执"):
            data._validate_photo_arrays({"extra_images": [{"file_name": "x"}]}, "maintenance")

    def test_native_end_validator_reads_panel_receipts(self):
        body = {"extra_images": [receipt("fixture-upload")]}
        data._validate_photo_arrays(body, "maintenance")
        service = MaintenancePortalService.__new__(MaintenancePortalService)
        service._has_existing_site_photo_for_notice = lambda *args, **kwargs: False
        with self.assertRaises(PortalError):
            service._require_end_site_photo_cumulative({}, "end", work_type="maintenance")
        service._require_end_site_photo_cumulative(body, "end", work_type="maintenance")
        self.assertEqual(service._site_photo_payload(body)[0]["upload_id"], "fixture-upload")

    def test_token_is_not_a_staged_upload_and_owner_cannot_be_forged(self):
        with self.assertRaises(PortalError):
            data._validate_photo_arrays({"extra_images": [{"file_token": "untrusted"}]}, "maintenance")
        clean = data._photo_receipt_value({**receipt(), "owner_open_id": "another-person"}, "现场照片")
        self.assertNotIn("owner_open_id", clean)

    def test_ali_only_for_change(self):
        with self.assertRaisesRegex(PortalError, "仅适用于变更"):
            data._validate_photo_arrays({"ali_confirmation_images": [receipt("u-ali")]}, "maintenance")
        body = {"ali_confirmation_images": [receipt("u-ali")]}
        data._validate_photo_arrays(body, "change")
        self.assertEqual(body["ali_confirmation_images"][0]["upload_id"], "u-ali")

    def test_ali_count_bounded(self):
        body = {"ali_confirmation_images": [receipt("u-1"), receipt("u-2")]}
        with self.assertRaisesRegex(PortalError, "最多 1 张"):
            data._validate_photo_arrays(body, "change")


class PhotoMigrationTests(PanelTestBase):
    """Missing optional photo descriptors are backfilled for edit/failed only."""

    def _doc(self, state, work_type="maintenance", action="start"):
        draft = {"title": "x", "content": "c", "work_type": work_type, "action": action,
                 "scope": "E", "source_record_id": "src-1"}
        return {
            "id": "a" * 32, "owner": "ou-li", "scope": "E", "date": "2026-10-09",
            "slot": "morning", "state": state, "revision": 1,
            "items": [{"key": "k1", "title": "x", "action": action,
                       "fields": [{"key": "content", "label": "内容", "required": True,
                                   "kind": "multiline", "readonly": False, "options": []}],
                       "draft": draft}],
        }

    def test_edit_doc_backfilled_without_replacing_other_fields(self):
        doc = self._doc("edit")
        panel._migrate_legacy_notice_photos(doc)
        item = doc["items"][0]
        keys = [f["key"] for f in item["fields"]]
        self.assertIn("extra_images", keys)
        self.assertNotIn("ali_confirmation_images", keys)
        self.assertEqual(item["draft"]["extra_images"], [])
        self.assertNotIn("ali_confirmation_images", item["draft"])
        # Original fields are preserved.
        self.assertIn("content", keys)

    def test_change_doc_gets_ali_confirmation_descriptor(self):
        doc = self._doc("failed", work_type="change")
        panel._migrate_legacy_notice_photos(doc)
        keys = [f["key"] for f in doc["items"][0]["fields"]]
        self.assertIn("extra_images", keys)
        self.assertIn("ali_confirmation_images", keys)
        self.assertEqual(doc["items"][0]["draft"]["ali_confirmation_images"], [])

    def test_running_payload_never_migrated(self):
        doc = self._doc("running")
        payload_before = copy.deepcopy(doc["items"][0].get("payload"))
        panel._migrate_legacy_notice_photos(doc)
        item = doc["items"][0]
        self.assertNotIn("extra_images", [f["key"] for f in item["fields"]])
        self.assertNotIn("extra_images", item["draft"])
        self.assertEqual(item.get("payload"), payload_before)


class PhotoPanelBehaviorTests(PanelTestBase):
    """_apply_changes accepts arrays, bounds them, and gates modules."""

    def test_save_accepts_site_photo_array(self):
        run = self.open(scope="E")
        change = {"key": "a", "selected": True, "draft": {"extra_images": [receipt("u-9")]}}
        self.action(run["id"], "save", changes=[change])
        doc = self.store.get_document(panel.NS, run["id"])
        draft = doc["items"][0]["draft"]
        self.assertEqual(draft["extra_images"], [receipt("u-9")])
        self.assertTrue(doc["items"][0]["selected"])

    def test_save_rejects_over_limit_and_keeps_draft(self):
        run = self.open(scope="E")
        too_many = [receipt(f"u-{i}") for i in range(data.SITE_PHOTO_MAX + 1)]
        with self.assertRaisesRegex(PortalError, f"最多 {data.SITE_PHOTO_MAX} 张"):
            self.action(run["id"], "save", changes=[{"key": "a", "selected": True,
                                                     "draft": {"extra_images": too_many}}])
        doc = self.store.get_document(panel.NS, run["id"])
        self.assertNotIn("extra_images", doc["items"][0]["draft"])

    def test_save_rejects_bad_receipt_and_keeps_existing(self):
        run = self.open(scope="E")
        with self.assertRaisesRegex(PortalError, "无效回执"):
            self.action(run["id"], "save", changes=[{"key": "a", "selected": True,
                                                     "draft": {"extra_images": [{"file_name": "x"}]}}])
        doc = self.store.get_document(panel.NS, run["id"])
        self.assertNotIn("extra_images", doc["items"][0]["draft"])

    def test_unauthorized_work_type_rejected(self):
        run = self.open(scope="E")
        # The owner may only operate 检修; the persisted item is a 维保 item.
        with patch.object(self.manager, "_owner_modules", return_value={"repair"}):
            with self.assertRaisesRegex(PortalError, "无权修改"):
                self.action(run["id"], "save", changes=[{"key": "a", "selected": True}])

    def test_public_items_hides_unauthorized_persisted_items(self):
        run = self.open(scope="E")
        # Both a,b are maintenance; grant only 检修 -> nothing should surface.
        with patch.object(self.manager, "_owner_modules", return_value={"repair"}):
            public = self.manager.public_items(self.store.get_document(panel.NS, run["id"]))
        self.assertEqual(public, [])

    def test_permission_read_failure_never_grants_all_modules(self):
        with patch.object(self.auth, "account_profile", side_effect=RuntimeError("permission store unavailable")):
            with self.assertRaises(RuntimeError):
                self.manager._owner_modules({"owner": "ou-li"})
        self.assertEqual(self.manager._owner_modules({}), set())

    def test_preview_rejects_unexpected_selected_unauthorized_items(self):
        run = self.open(scope="E")
        # Persist an already-selected maintenance item, then revoke maintenance.
        self.action(run["id"], "save", changes=[{"key": "a", "selected": True}])
        with patch.object(self.manager, "_owner_modules", return_value={"repair"}):
            with self.assertRaisesRegex(PortalError, "无权修改"):
                self.manager.run_action(make_session(), run["id"], {
                    "action": "preview", "revision": self.store.get_document(panel.NS, run["id"])["revision"],
                    "changes": []})


if __name__ == "__main__":
    import unittest
    unittest.main()

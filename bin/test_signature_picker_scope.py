"""Signature candidates are shared; business and signature-consent scopes are not."""
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.portal_service import MaintenancePortalService, PortalError


class SignaturePickerScopeTests(unittest.TestCase):
    def setUp(self):
        self.staff = [dict(source="staff", record_id=f"staff-{i}", name=f"人员{i}",
                           building="A楼" if i == 0 else "B楼", has_signature=True,
                           portable_signature=True, open_id=f"ou-{i}", can_receive_message=True,
                           raw_fields={"private": "not public"}) for i in range(602)]
        self.external = [dict(source="external", record_id=f"external-{i}", name=f"外部{i}",
                              building="A楼" if i == 0 else "H楼", has_signature=True,
                              portable_signature=i != 601, signature_file_token="private")
                         for i in range(602)]
        self.directory = Mock(return_value={"people": self.staff + self.external})
        self.service = SimpleNamespace(
            signature_management=SimpleNamespace(directory=self.directory),
            _normalize_scope=lambda value: value,
            _building_codes_from_value=lambda value: [value[:1]],
            _scope_matches_buildings=lambda scope, codes: scope == "ALL" or scope in codes,
            _maybe_start_daily_attachment_cache_refresh=lambda: None,
            _load_signature_people=Mock(return_value=self.staff),
            _load_external_signature_people=Mock(return_value=self.external),
            _state_store=SimpleNamespace(mop_signature_usage_status=Mock(return_value="pending")),
        )

    def test_all_staff_without_truncation_preserves_task_consent(self):
        result = MaintenancePortalService.signature_people(
            self.service, scope="A", notice_key="critical_guard:task-a", operator_open_id="ou-0",
            all_people=True, limit=60,
        )
        self.assertEqual(result["returned"], 602)
        self.assertTrue(result["people"][0]["usage_confirmed"])
        self.assertFalse(result["people"][-1]["usage_confirmed"])
        self.assertTrue(result["people"][-1]["usage_confirmation_pending"])
        self.assertNotIn("raw_fields", result["people"][-1])
        self.service._state_store.mop_signature_usage_status.assert_called_with(
            scope="A", notice_key="critical_guard:task-a", signer_record_id="staff-601", requested_by_openid="ou-0",
        )
        self.directory.assert_called_once_with(refresh=False)

    def test_all_external_includes_other_buildings_and_keeps_usable_signature_check(self):
        result = MaintenancePortalService.temporary_signature_people(
            self.service, scope="A", all_people=True, limit=100, notice_key="critical_guard:task-a",
        )
        self.assertEqual(result["returned"], 602)
        self.assertEqual(result["scope"], "A")
        self.assertEqual(result["people"][1]["building"], "H楼")
        self.assertTrue(result["people"][1]["has_signature"])
        self.assertFalse(result["people"][-1]["has_signature"])
        self.assertTrue(result["people"][-1]["signature_requires_resign"])
        self.assertTrue(all("signature_file_token" not in row for row in result["people"]))
        self.directory.assert_called_once_with(refresh=False)

    def test_default_callers_and_targeted_link_lookup_remain_compatible(self):
        self.assertEqual(MaintenancePortalService.signature_people(self.service, scope="A", limit=60)["returned"], 60)
        result = MaintenancePortalService.temporary_signature_people(self.service, scope="A", limit=100)
        self.assertEqual([row["record_id"] for row in result["people"]], ["external-0"])
        result = MaintenancePortalService.signature_people(self.service, record_id="staff-601", all_people=True)
        self.assertEqual([row["record_id"] for row in result["people"]], ["staff-601"])
        result = MaintenancePortalService.temporary_signature_people(self.service, scope="A", query="外部601", all_people=True)
        self.assertEqual([row["record_id"] for row in result["people"]], ["external-601"])

    def test_stored_external_signature_reuse_still_requires_valid_context_and_record(self):
        check = MaintenancePortalService._ensure_mop_non_staff_signature_context
        args = dict(scope="A", notice_key="notice-a|mop:attachment-a", operator_open_id="ou-operator")
        check(self.service, signatures=[dict(source="external", record_id="external-1", role="implementer")], **args)
        with self.assertRaisesRegex(PortalError, "记录不存在"):
            check(self.service, signatures=[dict(source="external", record_id="missing")], **args)
        with self.assertRaisesRegex(PortalError, "缺少当前任务"):
            check(self.service, signatures=[dict(source="external", record_id="external-1")], **{**args, "notice_key": ""})

    def test_task_temporary_signatures_cannot_cross_buildings(self):
        self.service._state_store.get_mop_temporary_signature_session = Mock(return_value={"scope": "B"})
        with self.assertRaisesRegex(PortalError, "不属于当前楼栋"):
            MaintenancePortalService._ensure_mop_non_staff_signature_context(
                self.service, signatures=[dict(source="temporary", temp_id="other-task", role="implementer")],
                scope="A", notice_key="notice-a", operator_open_id="ou-operator",
            )


if __name__ == "__main__":
    unittest.main()

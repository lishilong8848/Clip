"""Bounded regression tests for safe_data opaque reference/UUID preservation.

The original bug: safe_data's contact-regex scrubbing mistakes random hex
UUID / reference strings that happen to contain phone-like digit runs (e.g.
``a289617ab13940249922e985f84575f8``) for phone numbers and returns ``""``,
breaking downstream query references.

The fix adds a private ``_field`` kwarg to ``safe_data``, propagating
dictionary field names through list entries. Strict opaque ref / UUID4
formats are preserved ONLY in machine identifier fields; free-form text and
other field names are still scrubbed exactly as before.

These tests use deterministic observed examples only -- no random UUIDs.
"""
import json
import sys
import unittest
from pathlib import Path

# Bring ``bin`` onto sys.path so ``lan_bitable_template_portal`` resolves
# (same pattern used by the other lighthouse tests).
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal.lighthouse_ai import safe_data


UUID_A = "a289617ab13940249922e985f84575f8"
UUID_B = "053309235b18460191967c72ccad10c0"
UUID_C = "3efbfb2fc15440998086b5ecd79804c8"
PHONE = "13940249922"
ID_CARD = "11010119900307201X"


class LighthouseReferenceIdTests(unittest.TestCase):
    """safe_data must keep opaque machine identifiers while still redacting PII."""

    def test_cabinet_row_and_event_hashes_keep_phone_like_digit_runs(self):
        value = "de15159252849aaaf3f8afcd"
        payload = {"row_id": "row_" + value, "event_id": "event_" + value, "row_ids": ["row_" + value]}
        self.assertEqual(safe_data(payload), payload)
        self.assertEqual(safe_data({"note": "row_" + value, "row_id": "row_15159252849", "phone": "row_" + value}),
                         {"note": "", "row_id": ""})

    # -- opaque references are preserved in identifier fields ----------------
    def test_ref_field_preserves_query_form_uuid(self):
        self.assertEqual(
            safe_data({"ref": f"query_form_{UUID_A}"}),
            {"ref": f"query_form_{UUID_A}"},
        )

    def test_value_field_preserves_opaque_uuid(self):
        self.assertEqual(
            safe_data({"value": f"value_{UUID_A}"}),
            {"value": f"value_{UUID_A}"},
        )

    def test_value_in_option_preserves_opaque_uuid(self):
        self.assertEqual(
            safe_data({"option": {"value": f"value_{UUID_B}"}}),
            {"option": {"value": f"value_{UUID_B}"}},
        )

    def test_list_value_preserves_opaque_uuid_propagated_field(self):
        # The ``value`` field name must propagate into each list entry so
        # lighthousereference lists keep their opaque identifiers.
        self.assertEqual(
            safe_data({"value": [f"value_{UUID_C}"]}),
            {"value": [f"value_{UUID_C}"]},
        )
        self.assertEqual(safe_data({"ids": [UUID_A]}), {"ids": [UUID_A]})
        self.assertEqual(safe_data({"value": json.dumps([f"value_{UUID_C}"])}), {"value": [f"value_{UUID_C}"]})

    def test_raw_uuid4_id_field_preserved(self):
        self.assertEqual(
            safe_data({"id": UUID_A}),
            {"id": UUID_A},
        )

    def test_ref_and_task_id_preserve_uuid(self):
        self.assertEqual(
            safe_data({"ref": f"query_form_{UUID_A}", "task_id": UUID_C}),
            {"ref": f"query_form_{UUID_A}", "task_id": UUID_C},
        )
        self.assertEqual(safe_data({"from_step_id": UUID_A, "to_step_id": UUID_B}), {"from_step_id": UUID_A, "to_step_id": UUID_B})
        self.assertEqual(safe_data({"$reference": f"value_{UUID_A}"}), {"$reference": f"value_{UUID_A}"})

    # -- free-form / non-identifier fields are still scrubbed ---------------
    def test_bare_token_still_scrubbed(self):
        self.assertEqual(safe_data(f"value_{UUID_A}"), "")

    def test_note_and_content_still_scrub_phone_like_text(self):
        self.assertEqual(safe_data({"note": f"value_{UUID_A}"}), {"note": ""})
        self.assertEqual(safe_data({"content": f"value_{UUID_A}"}), {"content": ""})

    # -- ordinary PII under even identifier fields is NOT whitelisted -------
    def test_phone_string_under_identifier_fields_redacted(self):
        self.assertEqual(safe_data({"id": PHONE}), {"id": ""})
        self.assertEqual(safe_data({"ref": PHONE}), {"ref": ""})
        self.assertEqual(safe_data({"value": PHONE}), {"value": ""})

    def test_id_card_under_identifier_fields_redacted(self):
        self.assertEqual(safe_data({"id": ID_CARD}), {"id": "[敏感信息已隐藏]"})
        self.assertEqual(safe_data({"ref": ID_CARD}), {"ref": "[敏感信息已隐藏]"})
        self.assertEqual(safe_data({"value": ID_CARD}), {"value": "[敏感信息已隐藏]"})

    # -- secret-named fields are absent even if the value looks like UUID ----
    def test_secret_fields_still_absent_with_like_uuid_values(self):
        self.assertEqual(safe_data({"token": UUID_A}), {})
        self.assertEqual(safe_data({"secret": UUID_B}), {})
        self.assertEqual(safe_data({"mobile": UUID_C}), {})
        self.assertEqual(safe_data({"id_card": UUID_A}), {})
        self.assertEqual(safe_data({"phone": UUID_B}), {})


if __name__ == "__main__":
    unittest.main()

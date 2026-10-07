"""query_result_page pagination/traversal regressions; synthetic read-only data only."""
import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_queries import query_result_page


def rows(count, **extra):
    return [{"record_id": f"r{i}", "title": f"topic {i}", **extra} for i in range(count)]


class QueryResultPageTests(unittest.TestCase):
    def test_three_pages_40_40_25_over_105_rows(self):
        data = {"topics": rows(105, source="fixture")}
        p1 = query_result_page(data, ["topics"], offset=0, limit=40)
        p2 = query_result_page(data, ["topics"], offset=40, limit=40)
        p3 = query_result_page(data, ["topics"], offset=80, limit=40)
        self.assertEqual([len(p1["items"]), len(p2["items"]), len(p3["items"])], [40, 40, 25])
        self.assertEqual(p1["loaded_count"], 105)
        self.assertEqual(p1["next_offset"], 40)
        self.assertTrue(p1["has_more"])
        self.assertEqual(p2["next_offset"], 80)
        self.assertTrue(p2["has_more"])
        self.assertEqual(p3["next_offset"], None)
        self.assertFalse(p3["has_more"])
        self.assertEqual(p1["offset"], 0)
        self.assertEqual(p2["offset"], 40)
        self.assertEqual(p3["offset"], 80)

    def test_nested_lists_and_dotted_literal_key_segments(self):
        data = {
            "outer.list": [
                {"rows": [{"title": "x1"}, {"title": "x2"}], "dots.key": [10, 20, 30]},
            ]
        }
        page = query_result_page(data, ["outer.list", "0", "dots.key"])
        self.assertEqual([item for item in page["items"]], [10, 20, 30])
        self.assertEqual(query_result_page(data, ["outer.list"], limit=1)["loaded_count"], 1)
        nested = query_result_page(data, ["outer.list", "0", "rows"], offset=1, limit=1)
        self.assertEqual(nested["items"], [{"title": "x2"}])
        self.assertEqual(nested["loaded_count"], 2)
        self.assertEqual(nested["next_offset"], None)

    def test_root_list_and_tuple_supported(self):
        root = [{"title": "a"}, {"title": "b"}]
        page = query_result_page(root, [], offset=1, limit=1)
        self.assertEqual(page["items"], [{"title": "b"}])
        self.assertEqual(page["loaded_count"], 2)
        page = query_result_page(({"title": "c"},), [], offset=0, limit=1)
        self.assertEqual(page["items"], [{"title": "c"}])

    def test_invalid_bounds_path_and_non_list_result(self):
        rows_data = {"rows": rows(5)}
        with self.assertRaises(AssistantError):
            query_result_page(rows_data, "rows")
        with self.assertRaises(AssistantError):
            query_result_page(rows_data, ["rows"] * 13)
        with self.assertRaises(AssistantError):
            query_result_page(rows_data, ["rows", ""])
        with self.assertRaises(AssistantError):
            query_result_page(rows_data, ["rows", "x" * 201])
        for offset in (-1, True, "1", 1.5):
            with self.subTest(offset=offset):
                with self.assertRaises(AssistantError):
                    query_result_page(rows_data, ["rows"], offset=offset)
        for limit in (0, 41, True, "20"):
            with self.subTest(limit=limit):
                with self.assertRaises(AssistantError):
                    query_result_page(rows_data, ["rows"], limit=limit)
        with self.assertRaises(AssistantError):
            query_result_page(rows_data, ["rows", "0"])  # final item is a dict, not a list
        with self.assertRaises(AssistantError):
            query_result_page(rows_data, ["missing"])
        with self.assertRaises(AssistantError):
            query_result_page(rows_data, ["rows", "-1", "title"])

    def test_direct_raw_token_address_signature_and_underscore_bypass_blocked(self):
        data = {
            "rows": [{
                "title": "ok",
                "notes": ["a", "b"],
                "token": ["tok"],
                "secret": ["sec"],
                "access_token": ["at"],
                "address": ["addr"],
                "home address": ["home"],
                "身份证": ["110101199001011234"],
                "attachments": ["att"],
                "phone": ["13800000000"],
                "open_id": ["ou_abc"],
                "raw token": ["raw_tok"],
                "signature": ["sig"],
                "signature_time": ["10:30"],
                "_private": ["p"],
                "path": ["/etc/passwd"],
                "relative_path": ["rel/path"],
            }],
            "token": ["root_tok"],
            "secret": ["root_secret"],
            "access_token": ["root_at"],
            "address": ["root_addr"],
            "home address": ["root_home"],
            "身份证": ["110101211111111111"],
            "attachments": ["root_att"],
            "phone": ["13900000000"],
            "open_id": ["root_open"],
            "raw token": ["root_raw"],
            "signature": ["root_sig"],
            "path": ["/etc/root"],
            "relative_path": ["root/rel"],
            "_root_private": ["root_p"],
        }
        blocked = (
            ["rows", "0", "token"],
            ["rows", "0", "secret"],
            ["rows", "0", "access_token"],
            ["rows", "0", "address"],
            ["rows", "0", "home address"],
            ["rows", "0", "身份证"],
            ["rows", "0", "attachments"],
            ["rows", "0", "phone"],
            ["rows", "0", "open_id"],
            ["rows", "0", "raw token"],
            ["rows", "0", "signature"],
            ["rows", "0", "signature_time"],
            ["rows", "0", "_private"],
            ["rows", "0", "path"],
            ["rows", "0", "relative_path"],
            ["token"],
            ["secret"],
            ["access_token"],
            ["address"],
            ["home address"],
            ["身份证"],
            ["attachments"],
            ["phone"],
            ["open_id"],
            ["raw token"],
            ["signature"],
            ["path"],
            ["relative_path"],
            ["_root_private"],
        )
        for path in blocked:
            with self.subTest(path=path):
                with self.assertRaises(AssistantError):
                    query_result_page(data, path)
        # Control: a harmless nested list remains readable through the same traversal shape.
        control = query_result_page(data, ["rows", "0", "notes"])
        self.assertEqual(control["items"], ["a", "b"])
        self.assertEqual(control["loaded_count"], 2)

    def test_safe_source_evidence_retained_and_valid_signature_time_case(self):
        data = {
            "rows": [
                {"record_id": "r1", "title": "topic", "source": "native", "evidence": "ok",
                 "signature_time": "10:30"},
                {"record_id": "r2", "title": "topic2", "source": "native", "evidence": "ok",
                 "signature_time": "not-a-time"},
            ]
        }
        page = query_result_page(data, ["rows"])
        self.assertEqual(page["items"][0]["record_id"], "r1")
        self.assertEqual(page["items"][0]["source"], "native")
        self.assertEqual(page["items"][0]["evidence"], "ok")
        self.assertEqual(page["items"][0]["signature_time"], "10:30")
        self.assertNotIn("signature_time", page["items"][1])

    def test_input_immutability(self):
        data = {"rows": rows(105, source="fixture")}
        snapshot = copy.deepcopy(data)
        root = [{"title": "a"}, {"title": "b"}]
        root_snapshot = copy.deepcopy(root)
        for _ in range(3):
            query_result_page(data, ["rows"], offset=80, limit=40)
        query_result_page(root, [], offset=0, limit=1)
        self.assertEqual(data, snapshot)
        self.assertEqual(root, root_snapshot)

    def test_paging_does_not_copy_unselected_deepcopy_trap_elements(self):
        class DeepCopyTrap:
            def __deepcopy__(self, memo):
                raise AssertionError("unselected element was deep-copied")

        data = {"rows": [{"title": "sel0"}, {"title": "sel1"}, DeepCopyTrap(), DeepCopyTrap()]}
        page = query_result_page(data, ["rows"], offset=0, limit=2)
        self.assertEqual(page["items"], [{"title": "sel0"}, {"title": "sel1"}])
        self.assertEqual(page["loaded_count"], 4)
        self.assertEqual(page["next_offset"], 2)
        self.assertTrue(page["has_more"])

    def test_tuple_nested_indices_supported(self):
        data = {"groups": ({"rows": (("a", "b"), ("c",))}, {"rows": (("d",),)})}
        page = query_result_page(data, ["groups", "0", "rows", "0"])
        self.assertEqual(list(page["items"]), ["a", "b"])
        page = query_result_page(data, ["groups", "1", "rows"], offset=0, limit=1)
        self.assertEqual(page["items"], [["d"]])
        page = query_result_page(data, ["groups", "0", "rows"], offset=1, limit=1)
        self.assertEqual(page["items"], [["c"]])


if __name__ == "__main__":
    unittest.main()
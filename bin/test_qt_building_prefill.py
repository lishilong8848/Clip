import sys
import unittest
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from upload_event_module.building_normalizer import (  # noqa: E402
    normalize_building_name,
    normalize_buildings_value,
)
from upload_event_module.ui.main_window_records import MainWindowRecordsMixin  # noqa: E402


class QtBuildingPrefillTests(unittest.TestCase):
    def test_normalizes_h_and_110_to_dialog_options(self):
        self.assertEqual(normalize_building_name("H栋"), "H楼")
        self.assertEqual(normalize_building_name("110"), "110站")
        self.assertEqual(normalize_building_name("110机房"), "110站")
        self.assertEqual(normalize_buildings_value(["H栋", "110"]), ["H楼", "110站"])

    def test_infers_building_from_notice_text_for_screenshot_dialog_prefill(self):
        samples = {
            "A": (
                "【维保通告】状态：开始\n"
                "【名称】EA118-A楼测试维保\n"
                "【位置】机房"
            ),
            "H": (
                "【维保通告】状态：开始\n"
                "【名称】EA118-H楼测试维保\n"
                "【位置】H楼机房"
            ),
            "110": (
                "【维保通告】状态：开始\n"
                "【名称】EA118-110KV测试维保\n"
                "【位置】110机房"
            ),
        }
        self.assertEqual(
            MainWindowRecordsMixin._infer_buildings_from_notice_text(samples["A"]),
            ["A楼"],
        )
        self.assertEqual(
            MainWindowRecordsMixin._infer_buildings_from_notice_text(samples["H"]),
            ["H楼"],
        )
        self.assertEqual(
            MainWindowRecordsMixin._infer_buildings_from_notice_text(samples["110"]),
            ["110站"],
        )


if __name__ == "__main__":
    unittest.main()

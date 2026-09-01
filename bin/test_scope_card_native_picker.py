from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent
FRONTEND = ROOT / "lan_bitable_template_portal" / "frontend" / "src"


class ScopeCardAndNativePickerTests(unittest.TestCase):
    def test_scope_cards_are_keyboard_clickable_without_hijacking_inner_controls(self) -> None:
        source = (FRONTEND / "components" / "ScopeHome.vue").read_text(encoding="utf-8")
        self.assertIn(':role="scopeCardIsEnabled(scope.value) ? \'button\' : undefined"', source)
        self.assertIn(':tabindex="scopeCardIsEnabled(scope.value) ? 0 : -1"', source)
        self.assertIn('target?.closest("button, a, input, select, textarea, label")', source)
        self.assertIn('.scope-actions .primary:not(:disabled)', source)

    def test_vue_and_lite_notice_pages_use_progressive_native_picker(self) -> None:
        main_source = (FRONTEND / "main.ts").read_text(encoding="utf-8")
        lite_source = (ROOT / "lan_bitable_template_portal" / "workbench_lite.py").read_text(encoding="utf-8")
        for source in (main_source, lite_source):
            self.assertIn("datetime-local", source)
            self.assertIn("showPicker", source)
            self.assertIn("readOnly", source)


if __name__ == "__main__":
    unittest.main()

"""Isolated pure-function tests for ``inject_lighthouse_widget``.

Each fixture writes a real ``assistant.html`` plus a real ``assets/`` directory
with actual js/css files, so the injection path is exercised end-to-end against
genuine filesystem checks.
"""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal.lighthouse_widget import WIDGET_ID, inject_lighthouse_widget

BASE_HTML = """<!doctype html>
<html>
<head><title>Portal</title></head>
<body>
<div id="notice">重要通告</div>
</body>
</html>
"""

MODULE_JS = "/assets/index-abc123.js"
STYLE_CSS = "/assets/app-xyz456.css"

VALID_ASSISTANT = """<!doctype html>
<html>
<head>
<link rel="stylesheet" href="%s">
</head>
<body>
<script type="module" src="%s"></script>
</body>
</html>
""" % (STYLE_CSS, MODULE_JS)

JS_CONTENT = 'window.__lighthouse__ = true;\n'
CSS_CONTENT = '.lh-widget { color: red; }\n'


def asset_name(ref):
    """Return the basename after the fixed ``/assets/`` prefix."""
    assert ref.startswith("/assets/")
    return ref[len("/assets/"):]


def formal_session(open_id="ou_user_1", name="张三", **extra):
    session = {
        "open_id": open_id,
        "name": name,
        "role": "user",
        "user": {"open_id": open_id, "name": name, "role": "user"},
    }
    session.update(extra)
    return session


class WidgetInjectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.index = self.base / "index.html"
        self.index.write_text(BASE_HTML, encoding="utf-8")

    def build(self, assistant_html=VALID_ASSISTANT, files=None, mkdir_assets=True):
        """Write a fresh assistant.html and an isolated assets dir with files."""
        (self.base / "assistant.html").write_text(assistant_html, encoding="utf-8")
        assets = self.base / "assets"
        if assets.exists():
            for p in assets.iterdir():
                p.unlink()
        if not mkdir_assets:
            return
        assets.mkdir(exist_ok=True)
        for name, content in (files or {}).items():
            (assets / name).write_text(content, encoding="utf-8")

    def test_formal_account_injects_widget_and_real_assets(self):
        self.build(files={asset_name(MODULE_JS): JS_CONTENT, asset_name(STYLE_CSS): CSS_CONTENT})
        result = inject_lighthouse_widget(BASE_HTML, formal_session(open_id="ou_user_abc", name="李四"), self.index)

        self.assertIn(f'id="{WIDGET_ID}"', result)
        self.assertIn('data-user-id="ou_user_abc"', result)
        self.assertIn('data-user-name="李四"', result)
        self.assertIn(f'rel="stylesheet" href="{STYLE_CSS}"', result)
        self.assertIn(f'type="module" src="{MODULE_JS}"', result)
        self.assertLess(result.index(WIDGET_ID), result.index("</body>"))
        self.assertIn('<div id="notice">重要通告</div>', result)

    def test_guest_session_keeps_page_untouched(self):
        self.build(files={asset_name(MODULE_JS): JS_CONTENT, asset_name(STYLE_CSS): CSS_CONTENT})
        cases = [
            formal_session(is_guest=True),
            formal_session(role="guest"),
            formal_session(user={"open_id": "ou_1", "name": "x", "role": "guest"}),
            None,
            {},
            formal_session(open_id="  ", user={"open_id": ""}),
        ]
        for session in cases:
            with self.subTest(session=session):
                self.assertEqual(inject_lighthouse_widget(BASE_HTML, session, self.index), BASE_HTML)

    def test_session_open_id_take_priority_and_name_escaped(self):
        self.build(files={asset_name(MODULE_JS): JS_CONTENT, asset_name(STYLE_CSS): CSS_CONTENT})
        session = {
            "open_id": "ou_session_x",
            "name": '<script>alert("x")</script> & "quoted"',
            "role": "user",
            "user": {"open_id": "ou_user_x", "name": "from-user", "role": "user"},
        }
        result = inject_lighthouse_widget(BASE_HTML, session, self.index)
        self.assertIn('data-user-id="ou_session_x"', result)
        self.assertIn('data-user-name="&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt; &amp; &quot;quoted&quot;"', result)
        self.assertNotIn('<script>alert', result)

    def test_user_based_identity_when_session_level_missing(self):
        self.build(files={asset_name(MODULE_JS): JS_CONTENT, asset_name(STYLE_CSS): CSS_CONTENT})
        session = {"role": "user", "user": {"open_id": "ou_only_user", "name": "王五"}}
        result = inject_lighthouse_widget(BASE_HTML, session, self.index)
        self.assertIn('data-user-id="ou_only_user"', result)
        self.assertIn('data-user-name="王五"', result)

    def test_non_dict_user_keeps_page_untouched(self):
        self.build(files={asset_name(MODULE_JS): JS_CONTENT, asset_name(STYLE_CSS): CSS_CONTENT})
        for bad_user in ("ou-abc", ["ou-abc"], 123, 1.5, object()):
            with self.subTest(user=bad_user):
                session = {"open_id": "ou_session", "name": "x", "role": "user", "user": bad_user}
                self.assertEqual(inject_lighthouse_widget(BASE_HTML, session, self.index), BASE_HTML)

    def test_missing_assistant_html_keeps_page_untouched(self):
        # No assistant.html is written; page must stay byte-identical.
        self.assertEqual(inject_lighthouse_widget(BASE_HTML, formal_session(), self.index), BASE_HTML)

    def test_corrupt_or_garbage_artifact_keeps_page_untouched(self):
        self.build("this is not html at all")
        self.assertEqual(inject_lighthouse_widget(BASE_HTML, formal_session(), self.index), BASE_HTML)
        (self.base / "assistant.html").write_bytes(b"\xff\xfe\x00<broken")
        self.assertEqual(inject_lighthouse_widget(BASE_HTML, formal_session(), self.index), BASE_HTML)

    def test_injection_is_idempotent(self):
        self.build(files={asset_name(MODULE_JS): JS_CONTENT, asset_name(STYLE_CSS): CSS_CONTENT})
        session = formal_session(open_id="ou_idem", name="幂等")
        once = inject_lighthouse_widget(BASE_HTML, session, self.index)
        twice = inject_lighthouse_widget(once, session, self.index)
        self.assertEqual(twice, once)
        self.assertEqual(once.count(f'id="{WIDGET_ID}"'), 1)
        self.assertEqual(once.count(MODULE_JS), 1)
        self.assertEqual(once.count(STYLE_CSS), 1)

    def test_any_dangerous_path_keeps_page_untouched(self):
        bad_assistants = [
            VALID_ASSISTANT.replace(MODULE_JS, "/assets/../../outside.js"),
            VALID_ASSISTANT.replace(MODULE_JS, "/assets/..%2f%2f.js"),
            VALID_ASSISTANT.replace(STYLE_CSS, "/assets/..//x.css"),
            VALID_ASSISTANT.replace(MODULE_JS, "/assets\\evil.js"),
            VALID_ASSISTANT.replace(MODULE_JS, "/assets/x.js?v=1"),
            VALID_ASSISTANT.replace(MODULE_JS, "http://evil.example/x.js"),
            VALID_ASSISTANT.replace(MODULE_JS, "../assets/x.js"),
            VALID_ASSISTANT.replace(STYLE_CSS, "/assets/x%20y.css"),
        ]
        for html in bad_assistants:
            with self.subTest(html=html):
                self.build(html, files={"index-abc123.js": JS_CONTENT, "app-xyz456.css": CSS_CONTENT})
                self.assertEqual(inject_lighthouse_widget(BASE_HTML, formal_session(), self.index), BASE_HTML)

    def test_percent_encoded_and_bad_basename_rejected(self):
        html = """<!doctype html>
<html><head>
<link rel="stylesheet" href="/assets/app-xyz456.css">
</head><body>
<script type="module" src="/assets/%2e%2e/pwn.js"></script>
<script type="module" src="/assets/space name.js"></script>
<script type="module" src="/assets/index-abc123.js"></script>
</body></html>
"""
        # Even a single bad reference anywhere in module/style refs aborts injection.
        self.build(html, files={"index-abc123.js": JS_CONTENT, "app-xyz456.css": CSS_CONTENT})
        self.assertEqual(inject_lighthouse_widget(BASE_HTML, formal_session(), self.index), BASE_HTML)

    def test_missing_real_js_or_css_file_keeps_page_untouched(self):
        # Referenced js file does not exist on disk.
        self.build(files={asset_name(STYLE_CSS): CSS_CONTENT})
        self.assertEqual(inject_lighthouse_widget(BASE_HTML, formal_session(), self.index), BASE_HTML)

        # Referenced css file does not exist on disk.
        self.build(files={asset_name(MODULE_JS): JS_CONTENT})
        self.assertEqual(inject_lighthouse_widget(BASE_HTML, formal_session(), self.index), BASE_HTML)

    def test_empty_module_js_keeps_page_untouched(self):
        self.build(files={asset_name(MODULE_JS): "", asset_name(STYLE_CSS): CSS_CONTENT})
        self.assertEqual(inject_lighthouse_widget(BASE_HTML, formal_session(), self.index), BASE_HTML)

    def test_no_module_js_keeps_page_untouched(self):
        for html in (
            """<!doctype html><html><body><script src="/assets/plain.js"></script></body></html>""",
            """<!doctype html><html><body><link rel="stylesheet" href="/assets/app-xyz456.css"></body></html>""",
        ):
            with self.subTest(html=html):
                self.build(html, files={"plain.js": "var x=1;\n", "app-xyz456.css": CSS_CONTENT})
                self.assertEqual(inject_lighthouse_widget(BASE_HTML, formal_session(), self.index), BASE_HTML)

    def test_missing_assets_dir_keeps_page_untouched(self):
        self.build(mkdir_assets=False)
        self.assertEqual(inject_lighthouse_widget(BASE_HTML, formal_session(), self.index), BASE_HTML)

    def test_duplicate_refs_deduplicated(self):
        dup = """<!doctype html>
<html><head>
<link rel="stylesheet" href="/assets/a.css">
<link rel="stylesheet" href="/assets/a.css">
</head><body>
<script type="module" src="/assets/a.js"></script>
<script type="module" src="/assets/a.js"></script>
</body></html>
"""
        self.build(dup, files={"a.js": JS_CONTENT, "a.css": CSS_CONTENT})
        result = inject_lighthouse_widget(BASE_HTML, formal_session(), self.index)
        self.assertEqual(result.count('href="/assets/a.css"'), 1)
        self.assertEqual(result.count('src="/assets/a.js"'), 1)

    def test_no_body_tag_appends_at_end(self):
        self.build(files={asset_name(MODULE_JS): JS_CONTENT, asset_name(STYLE_CSS): CSS_CONTENT})
        html_no_body = "<html><head></head><div>plain</div></html>"
        result = inject_lighthouse_widget(html_no_body, formal_session(), self.index)
        self.assertIn(WIDGET_ID, result)
        self.assertIn(f'src="{MODULE_JS}"', result)
        self.assertIn(f'</html><div id="{WIDGET_ID}"', result)
        self.assertTrue(result.endswith(f'{MODULE_JS}"></script>'))

    def test_html_text_must_be_str(self):
        self.build(files={asset_name(MODULE_JS): JS_CONTENT, asset_name(STYLE_CSS): CSS_CONTENT})
        for value in (None, 5, ["<html>"]):
            with self.subTest(value=value):
                self.assertEqual(inject_lighthouse_widget(value, formal_session(), self.index), value)

    def test_widget_index_path_none_or_broken(self):
        self.build(files={asset_name(MODULE_JS): JS_CONTENT, asset_name(STYLE_CSS): CSS_CONTENT})
        session = formal_session()
        for path in (None, "", self.base / "missing" / "index.html"):
            with self.subTest(path=path):
                self.assertEqual(inject_lighthouse_widget(BASE_HTML, session, path), BASE_HTML)


if __name__ == "__main__":
    unittest.main()
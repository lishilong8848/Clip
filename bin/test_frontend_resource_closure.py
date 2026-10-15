import json
from pathlib import Path
import sys
import tempfile
import unittest

BIN = Path(__file__).resolve().parent
sys.path.insert(0, str(BIN))
from frontend_assets import (
    FRONTEND_DIST,
    FRONTEND_INDEX,
    V2_DIR,
    V2_INDEX,
    V2_MANIFEST,
    patch_deletions,
    referenced_assets,
)

LEGACY_ASSETS = FRONTEND_DIST / "assets"


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _v2(root: Path, *, index: str | None = None, manifest: dict | None = None,
         files: dict[str, str] | None = None, legacy: bool = False) -> Path:
    """Build a standard V2 workspace generation (and optional legacy entry)."""
    if legacy:
        _write(root / FRONTEND_INDEX,
               '<script src="/assets/legacy.js"></script>')
        _write(root / LEGACY_ASSETS / "legacy.js", "// legacy")
    if index is not None:
        _write(root / V2_INDEX, index)
    if manifest is not None:
        _write(root / V2_MANIFEST, json.dumps(manifest))
    for rel, text in (files or {}).items():
        _write(root / V2_DIR / rel, text)
    return root


class V2FrontendClosureTests(unittest.TestCase):
    def test_v2_closure_tracks_index_manifest_nested_chunks_and_css(self):
        """Chunk imports resolve from their containing directory, not assets root."""
        with tempfile.TemporaryDirectory() as tmp:
            root = _v2(
                Path(tmp) / "app",
                legacy=True,
                index='<script src="/assets/connection-guard.js"></script>',
                manifest={"entry": "app-A.js", "css": ["res-A.css"]},
                files={
                    "app-A.js": 'import "./chunks/part-n1.js";',
                    "chunks/part-n1.js": 'import "./part-n2.js";',
                    "chunks/part-n2.js": "// nested chunk",
                    "res-A.css": "/* css */",
                },
            )
            _write(root / LEGACY_ASSETS / "connection-guard.js", "// guard")
            # Same-named file at the assets root must NOT be used for the
            # containing-directory resolution of chunks/part-n1.js.
            _write(root / LEGACY_ASSETS / "part-n2.js", "// wrong root copy")

            found = referenced_assets(root)
            expected = {
                V2_INDEX,
                V2_MANIFEST,
                V2_DIR / "app-A.js",
                V2_DIR / "chunks/part-n1.js",
                V2_DIR / "chunks/part-n2.js",
                V2_DIR / "res-A.css",
                LEGACY_ASSETS / "connection-guard.js",
                LEGACY_ASSETS / "legacy.js",
            }
            self.assertTrue(expected.issubset(found), found - expected)
            # Full paths are preserved (no basename collapsing).
            self.assertIn(V2_DIR / "chunks/part-n1.js", found)
            self.assertIn(V2_DIR / "chunks/part-n2.js", found)

    def test_v2_strict_fails_when_manifest_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = _v2(Path(tmp) / "app", index="<!doctype html>")
            with self.assertRaisesRegex(ValueError, "Missing V2 frontend manifest"):
                referenced_assets(root)

    def test_v2_strict_fails_when_manifest_invalid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "app"
            _write(root / V2_INDEX, "<!doctype html>")
            _write(root / V2_MANIFEST, "{not json")
            with self.assertRaisesRegex(ValueError, "Invalid V2 frontend manifest"):
                referenced_assets(root)

    def test_v2_strict_fails_when_manifest_references_missing_asset(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "app"
            _write(root / V2_INDEX, "<!doctype html>")
            _write(root / V2_MANIFEST, json.dumps({"entry": "app-missing.js", "css": []}))
            with self.assertRaisesRegex(ValueError, "Missing V2 frontend asset"):
                referenced_assets(root)

    def test_v2_strict_fails_when_manifest_references_missing_css(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "app"
            _write(root / V2_INDEX, "<!doctype html>")
            _write(root / V2_MANIFEST, json.dumps({"entry": "app.js", "css": ["res-missing.css"]}))
            _write(root / V2_DIR / "app.js", "// app")
            with self.assertRaisesRegex(ValueError, "Missing V2 frontend asset"):
                referenced_assets(root)

    def test_v2_strict_fails_when_nested_chunk_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "app"
            _write(root / V2_INDEX, "<!doctype html>")
            _write(root / V2_MANIFEST, json.dumps({"entry": "app.js", "css": []}))
            _write(root / V2_DIR / "app.js", 'import "./part-gone.js";')
            with self.assertRaisesRegex(ValueError, "Missing V2 frontend asset"):
                referenced_assets(root)

    def test_v2_invalid_manifest_names_and_traversal_rejected(self):
        for entry in ["../escape.js", "res.css", "app.js?x", "app.js?v=1"]:
            with self.subTest(entry=entry):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp) / "app"
                    _write(root / V2_INDEX, "<!doctype html>")
                    manifest = {"entry": entry, "css": []}
                    _write(root / V2_MANIFEST, json.dumps(manifest))
                    with self.assertRaisesRegex(ValueError, "Invalid V2 frontend manifest"):
                        referenced_assets(root)
        # Traversal that escapes the assets root inside a chunk is rejected.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "app"
            _write(root / V2_INDEX, "<!doctype html>")
            _write(root / V2_MANIFEST, json.dumps({"entry": "app.js", "css": []}))
            _write(root / V2_DIR / "app.js", 'import "./chunks/child.js";')
            _write(root / V2_DIR / "chunks/child.js", 'import "../../../escape.js";')
            with self.assertRaisesRegex(ValueError, "Unsafe V2 frontend asset reference"):
                referenced_assets(root)

    def test_v2_non_strict_does_not_raise_on_missing_manifest_or_assets(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "app"
            _write(root / V2_INDEX, '<script src="/assets/guard.js"></script>')
            _write(root / LEGACY_ASSETS / "guard.js", "// guard")
            found = referenced_assets(root, strict=False)
            self.assertIn(V2_INDEX, found)
            self.assertNotIn(V2_MANIFEST, found)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "app"
            _write(root / V2_INDEX, "<!doctype html>")
            _write(root / V2_MANIFEST, json.dumps({"entry": "gone.js", "css": []}))
            found = referenced_assets(root, strict=False)
            self.assertIn(V2_MANIFEST, found)
            self.assertIn(V2_DIR / "gone.js", found)

    def test_v2_nested_chunk_parent_traversal_allowed(self):
        """A ../ import from a nested chunk resolves against its containing dir."""
        with tempfile.TemporaryDirectory() as tmp:
            root = _v2(
                Path(tmp) / "app",
                index="<!doctype html>",
                manifest={"entry": "app.js", "css": []},
                files={
                    "app.js": 'import "./chunks/child.js";',
                    "chunks/child.js": 'import "../shared.js";',
                    "shared.js": "// shared",
                },
            )
            _write(root / LEGACY_ASSETS / "shared.js", "// root copy")
            found = referenced_assets(root)
            self.assertTrue({V2_INDEX, V2_MANIFEST, V2_DIR / "app.js",
                             V2_DIR / "chunks/child.js", V2_DIR / "shared.js"}
                            .issubset(found), found)
            self.assertIn(V2_DIR / "shared.js", found)
            self.assertNotIn(LEGACY_ASSETS / "shared.js", found)

    def test_v2_root_asset_ignores_third_party_urls_and_local_dot_assets(self):
        """Rooted /assets/ refs are kept; URLs and ./assets/ are not rooted."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "app"
            _write(root / V2_INDEX,
                   '<script src="https://other.example/assets/foreign.js"></script>'
                   '<script src="/assets/local.js"></script>')
            _write(root / V2_MANIFEST, json.dumps({"entry": "app.js", "css": []}))
            _write(root / V2_DIR / "app.js", 'import "./assets/rel-chunk.js";')
            _write(root / V2_DIR / "assets/rel-chunk.js", "// local chunk")
            _write(root / LEGACY_ASSETS / "local.js", "// local")
            _write(root / LEGACY_ASSETS / "rel-chunk.js", "// root copy")
            found = referenced_assets(root)
            self.assertIn(LEGACY_ASSETS / "local.js", found)      # rooted /assets/
            self.assertNotIn(LEGACY_ASSETS / "foreign.js", found)  # third-party URL ignored
            self.assertIn(V2_DIR / "assets/rel-chunk.js", found)   # ./assets/ resolves locally
            self.assertNotIn(LEGACY_ASSETS / "rel-chunk.js", found)

    def test_v2_css_url_images_and_fonts_in_closure(self):
        """CSS url() images/fonts are included, resolving from the CSS file dir."""
        with tempfile.TemporaryDirectory() as tmp:
            root = _v2(
                Path(tmp) / "app",
                index="<!doctype html>",
                manifest={"entry": "app.js", "css": []},
                files={
                    "app.js": 'import "./res.css"; import "./sub/inner.css";',
                    "res.css": 'body { background: url("./icons/icon.woff2"); }',
                    "sub/inner.css": 'div { background-image: url("../shared.png"); }',
                    "icons/icon.woff2": "woff",
                    "shared.png": "png",
                },
            )
            found = referenced_assets(root)
            for rel in (
                V2_DIR / "res.css",
                V2_DIR / "sub/inner.css",
                V2_DIR / "icons/icon.woff2",
                V2_DIR / "shared.png",
            ):
                self.assertIn(rel, found, rel)

    def test_css_query_fragment_and_external_paths_are_resolved_safely(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = _v2(Path(tmp), index='<!doctype html>',
                manifest={'entry': 'app.js', 'css': ['res.css']}, files={
                    'app.js': 'const text="not a path assets/not-real.js";',
                    'res.css': '.a{src:url("./font.woff2?v=1#font")} .b{filter:url(#mask)} '
                               '.c{background:url(//cdn.example.com/photo.png)}',
                    'font.woff2': 'fixture font'})
            found = referenced_assets(root)
            self.assertIn(V2_DIR / 'font.woff2', found)
            self.assertNotIn(LEGACY_ASSETS / 'not-real.js', found)

    def test_v2_css_url_unsafe_absolute_and_backslash_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = _v2(Path(tmp) / "app", index="<!doctype html>",
                       manifest={"entry": "app.js", "css": []},
                       files={"app.js": 'import "./res.css";',
                              "res.css": 'div { background: url("/etc/x.png"); }'})
            with self.assertRaisesRegex(ValueError, "Unsafe V2 frontend asset reference"):
                referenced_assets(root)
        with tempfile.TemporaryDirectory() as tmp:
            root = _v2(Path(tmp) / "app", index="<!doctype html>",
                       manifest={"entry": "app.js", "css": []},
                       files={"app.js": 'import "./res.css";',
                              "res.css": 'div { background: url("..\\evil.png"); }'})
            with self.assertRaisesRegex(ValueError, "Unsafe V2 frontend asset reference"):
                referenced_assets(root)

    def test_overlay_preserves_old_and_new_v2_generations(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, overlay = Path(tmp) / "app", Path(tmp) / "patch"
            _v2(root, index="<!doctype html>",
                manifest={"entry": "app-old.js", "css": ["res-old.css"]},
                files={
                    "app-old.js": 'import "./part-old.js";',
                    "part-old.js": "// old",
                    "res-old.css": "/* old */",
                })
            _v2(overlay, index="<!doctype html>",
                manifest={"entry": "app-new.js", "css": ["res-new.css"]},
                files={
                    "app-new.js": 'import "./part-new.js";',
                    "part-new.js": "// new",
                    "res-new.css": "/* new */",
                })
            # root view yields the old generation; overlay view yields the new
            # generation, and both must be discoverable separately.
            old_gen = referenced_assets(root)
            for rel in (
                V2_INDEX,
                V2_MANIFEST,
                V2_DIR / "app-old.js",
                V2_DIR / "part-old.js",
                V2_DIR / "res-old.css",
            ):
                self.assertIn(rel, old_gen, rel)
            new_gen = referenced_assets(root, overlay)
            for rel in (
                V2_INDEX,
                V2_MANIFEST,
                V2_DIR / "app-new.js",
                V2_DIR / "part-new.js",
                V2_DIR / "res-new.css",
            ):
                self.assertIn(rel, new_gen, rel)
            self.assertNotIn(V2_DIR / "app-old.js", new_gen)
            # patch_deletions keeps the union of old and new referenced gens.
            plan = patch_deletions(root, overlay, [])
            for rel in (
                V2_INDEX,
                V2_MANIFEST,
                V2_DIR / "app-old.js",
                V2_DIR / "part-old.js",
                V2_DIR / "res-old.css",
                V2_DIR / "app-new.js",
                V2_DIR / "part-new.js",
                V2_DIR / "res-new.css",
            ):
                self.assertNotIn(rel, plan, rel)

    def test_patch_deletions_keeps_old_and_new_v2_but_cleans_obsolete(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, overlay = Path(tmp) / "app", Path(tmp) / "patch"
            _v2(root, index="<!doctype html>",
                manifest={"entry": "app-cur.js", "css": ["res-cur.css"]},
                files={
                    "app-cur.js": 'import "./part-cur.js";',
                    "part-cur.js": "// cur",
                    "res-cur.css": "/* cur */",
                })
            # An obsolete, unreferenced chunk remains on disk in the old dir.
            _write(root / V2_DIR / "obsolete-old.js", "// stale")
            _v2(overlay, index="<!doctype html>",
                manifest={"entry": "app-new.js", "css": ["res-new.css"]},
                files={
                    "app-new.js": 'import "./part-new.js";',
                    "part-new.js": "// new",
                    "res-new.css": "/* new */",
                })
            plan = patch_deletions(root, overlay, [])
            self.assertIn(V2_DIR / "obsolete-old.js", plan)
            for rel in (
                V2_DIR / "app-cur.js",
                V2_DIR / "part-cur.js",
                V2_DIR / "res-cur.css",
                V2_DIR / "app-new.js",
                V2_DIR / "part-new.js",
                V2_DIR / "res-new.css",
                V2_INDEX,
                V2_MANIFEST,
            ):
                self.assertNotIn(rel, plan, rel)

    def test_overlay_without_frontend_entry_skips_frontend_rotation(self):
        """V2-only entry updates still trigger full keep/cleanup handling."""
        with tempfile.TemporaryDirectory() as tmp:
            root, overlay = Path(tmp) / "app", Path(tmp) / "patch"
            # Legacy present in root but untouched by this patch.
            _v2(root, legacy=True, index="<!doctype html>",
                manifest={"entry": "app-cur.js", "css": []},
                files={"app-cur.js": "// cur"})
            # Patch updates only V2 (legacy index not present in overlay).
            _v2(overlay, index="<!doctype html>",
                manifest={"entry": "app-new.js", "css": []},
                files={"app-new.js": "// new"})
            plan = patch_deletions(root, overlay, [])
            self.assertNotIn(V2_DIR / "app-cur.js", plan)
            self.assertNotIn(V2_DIR / "app-new.js", plan)

    def test_legacy_only_fixture_stays_compatible_without_v2(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "app"
            _write(root / FRONTEND_INDEX,
                   '<script src="/assets/a.js"></script><link href="/assets/a.css">')
            _write(root / LEGACY_ASSETS / "a.js", "// a")
            _write(root / LEGACY_ASSETS / "a.css", "/* a */")
            found = referenced_assets(root)
            self.assertIn(LEGACY_ASSETS / "a.js", found)
            self.assertIn(LEGACY_ASSETS / "a.css", found)
            self.assertNotIn(V2_INDEX, found)

    def test_legacy_unsafe_references_still_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "app"
            _write(root / FRONTEND_INDEX, '<script src="/assets/../escape.js"></script>')
            with self.assertRaisesRegex(ValueError, "Unsafe frontend asset reference"):
                referenced_assets(root)


if __name__ == "__main__":
    unittest.main()

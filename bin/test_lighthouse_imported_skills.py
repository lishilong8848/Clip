"""Unit tests for the guidance-only WorkBuddy skills importer.

Run from the repository root:

    python -m unittest bin.test_lighthouse_imported_skills -v

or directly:

    python bin/test_lighthouse_imported_skills.py

The suite never touches the real source tree or the distributed openclaw
bundle: every test builds its own temporary source/output trees.  No test
applies output to the real bundle; CLI tests only dry-run temp sources.
"""

import json
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

TOOLS_DIR = Path(__file__).resolve().parent / "tools"
sys.path.insert(0, str(TOOLS_DIR))
import import_lighthouse_workbuddy_skills as imp  # noqa: E402


def _write(path: Path, content, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding=encoding)


def _make_skill(root: Path, rel: str, name: str, display: str = None,
                description: str = "guidance", refs: dict = None,
                extra: dict = None, body: str = None) -> Path:
    """Create a skill directory under `root` and return its path."""
    d = root / rel
    front = {
        "name": name,
        "description": description,
    }
    if display:
        front["display_name"] = display
    fm_lines = "".join(f"{k}: {v}\n" for k, v in front.items())
    body = body if body is not None else f"# {name}\n\nguide text\n"
    _write(d / "SKILL.md", "---\n" + fm_lines + "---\n\n" + body)
    for rname, rtext in (refs or {}).items():
        _write(d / rname, rtext)
    for fname, ftext in (extra or {}).items():
        _write(d / fname, ftext)
    return d


class _ImporterTestCase(unittest.TestCase):
    """Shared helpers for importer tests (temp source + temp skills output)."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="wbskills-")
        self.root = Path(self._tmp.name)
        self.src = self.root / "src"
        self.src.mkdir()
        # output root must end with .../skills so apply_plan accepts it.
        self.out = self.root / "out" / "skills"
        self.out.mkdir(parents=True)

    def tearDown(self):
        self._tmp.cleanup()

    def plan(self):
        return imp.build_plan(self.src)

    def apply(self):
        return imp.apply_plan(self.plan(), self.out, self.src)

    def written_skill_ids(self):
        sub = self.out / "workbuddy"
        if not sub.is_dir():
            return set()
        return {p.name for p in sub.iterdir() if p.is_dir()}

    def registry_sources(self):
        reg = self.out / "workbuddy-registry.json"
        if not reg.is_file():
            return set()
        return {r["source"] for r in json.loads(reg.read_text(encoding="utf-8"))}


class BuildPlanTests(_ImporterTestCase):
    def test_basic_inventory_and_metadata(self):
        _make_skill(self.src, "alpha", "alpha", display="Alpha 技能",
                    refs={"references/guide.md": "# guide\nsafe\n"})
        plans = self.plan()
        self.assertEqual(len(plans), 1)
        rec = plans[0]
        self.assertFalse(rec["excluded"])
        self.assertEqual(rec["kind"], "workflow_guide")
        self.assertEqual(rec["missing_executable_dependencies"], [])
        self.assertTrue(rec["id"].startswith("workbuddy-"))
        self.assertTrue(re.fullmatch(r"[a-z][a-z0-9-]{0,60}", rec["id"]))
        self.assertEqual(rec["display_name"], "Alpha 技能")
        self.assertIn(f"workbuddy/{rec['id']}/references/guide.md", rec["references"])
        self.assertEqual(len(rec["content_hash"]), 64)
        # SKILL.md is always the first copied file.
        self.assertEqual(rec["copied"][0][0], "SKILL.md")

    def test_nested_and_chinese_skills_are_discovered(self):
        _make_skill(self.src, "outer", "outer", refs={"references/a.md": "# a\n"})
        _make_skill(self.src, "outer/nested/sub", "sub", display="子技能")
        _make_skill(self.src, "中文技能/子目录", "中文技能", display="中文标题")
        plans = self.plan()
        rel_roots = {p["rel_root"] for p in plans}
        self.assertIn("outer", rel_roots)
        self.assertIn("outer/nested/sub", rel_roots)
        self.assertIn("中文技能/子目录", rel_roots)
        outer = next(p for p in plans if p["rel_root"] == "outer")
        nested_paths = [e["path"] for e in outer["excluded_files"]
                        if e["reason"] == "nested skill root (separate skill)"]
        self.assertTrue(any(p.startswith("nested") for p in nested_paths))

    def test_underscore_hyphen_collision_gets_distinct_stable_ids(self):
        _make_skill(self.src, "foo-bar", "one")
        _make_skill(self.src, "foo_bar", "two")
        plans = self.plan()
        ids = [p["id"] for p in plans]
        self.assertEqual(len(set(ids)), 2)
        for ident in ids:
            self.assertRegex(ident, re.compile(r"[a-z][a-z0-9-]{0,60}"))
        # Both sanitized to the same slug but hashed differently.
        self.assertNotEqual(ids[0], ids[1])

    def test_ids_stable_when_unrelated_folder_inserted(self):
        _make_skill(self.src, "sub/alpha", "alpha")
        _make_skill(self.src, "z/bravo", "bravo")
        ids_before = {(p["rel_root"], p["id"]) for p in self.plan()}
        # Insert an unrelated skill elsewhere; existing rel roots unchanged.
        _make_skill(self.src, "m/unrelated", "unrelated")
        ids_after = {(p["rel_root"], p["id"]) for p in self.plan()}
        for rel, ident in ids_before:
            self.assertIn((rel, ident), ids_after)

    def test_long_path_id_bound(self):
        long_dir = "verylong-" + "x" * 120
        _make_skill(self.src, long_dir, "long")
        rec = self.plan()[0]
        self.assertLessEqual(len(rec["id"]), 61)
        self.assertRegex(rec["id"], re.compile(r"[a-z][a-z0-9-]{0,60}"))

    def test_scripts_binary_config_metadata_excluded_and_recorded(self):
        _make_skill(
            self.src, "app", "app",
            refs={"references/ok.md": "ok\n"},
            extra={
                "scripts/run.py": "print(1)",
                "scripts/run.sh": "echo hi",
                "assets/logo.png": b"\x89PNG\r\n\x1a\nbinary",
                "_skillhub_meta.json": '{"name":"x"}',
                ".gitignore": "# vcs",
                "config.env.template": "A=1",
            },
        )
        rec = self.plan()[0]
        copied_paths = {r for r, _ in rec["copied"]}
        self.assertIn("references/ok.md", copied_paths)
        self.assertNotIn("scripts/run.py", copied_paths)
        self.assertNotIn("scripts/run.sh", copied_paths)
        self.assertNotIn("assets/logo.png", copied_paths)
        self.assertNotIn("_skillhub_meta.json", copied_paths)
        self.assertNotIn(".gitignore", copied_paths)
        self.assertNotIn("config.env.template", copied_paths)
        # Executables are recorded, not claimed to be functional CLI.
        self.assertEqual(sorted(rec["missing_executable_dependencies"]),
                         ["scripts/run.py", "scripts/run.sh"])

    def test_hardcoded_credentials_exclude_skill(self):
        _make_skill(
            self.src, "leaky", "leaky", body="app_id: `cli_a9d1c9fe9d381cee`\n")
        excluded = [p for p in self.plan() if p["excluded"]]
        self.assertEqual(len(excluded), 1)
        self.assertIn("app id", excluded[0]["exclude_reason"])
        report = json.dumps(excluded[0]["excluded_files"])
        self.assertNotIn("cli_a9d1c9fe9d381cee", report)
        self.assertEqual(excluded[0]["kind"], "workflow_guide")

    def test_credential_resource_file_excluded_not_skill(self):
        _make_skill(
            self.src, "ok", "ok",
            refs={
                "references/good.md": "safe\n",
                "references/bad.md": 'api_key = "AKIA1234567890ABCDEF"',
            },
        )
        rec = self.plan()[0]
        self.assertFalse(rec["excluded"])
        copied_paths = {r for r, _ in rec["copied"]}
        reasons = {e["path"]: e["reason"] for e in rec["excluded_files"]}
        self.assertIn("references/good.md", copied_paths)
        self.assertNotIn("references/bad.md", copied_paths)
        self.assertIn("references/bad.md", reasons)
        self.assertIn("AWS access key id", reasons["references/bad.md"])

    def test_nested_and_user_excluded_resources_do_not_leak_into_parent_copy(self):
        # outer/SKILL.md with nested/sub and zhinav-point-data underneath.
        _make_skill(self.src, "outer", "outer",
                    refs={"references/ok.md": "ok\n"})
        _make_skill(self.src, "outer/nested/sub", "sub",
                    refs={"references/sub.md": "sub\n"})
        _make_skill(self.src, "outer/zhinav-point-data", "zp",
                    refs={"references/zp.md": "zp\n"})
        outer = next(p for p in self.plan() if p["rel_root"] == "outer")
        self.assertFalse(outer["excluded"])
        copied_paths = {rel for rel, _ in outer["copied"]}
        # The parent's copy must never include descendants of nested/sub or
        # zhinav-point-data; only its own SKILL.md and its own references.
        self.assertIn("SKILL.md", copied_paths)
        self.assertIn("references/ok.md", copied_paths)
        self.assertFalse(any(
            rel == "nested/sub/references/sub.md"
            or rel.startswith("nested/sub/")
            or rel == "zhinav-point-data/references/zp.md"
            or rel.startswith("zhinav-point-data/")
            for rel in copied_paths))
        # And they must be recorded as excluded (exclusion report as well).
        excluded_rels = {e["path"]: e["reason"] for e in outer["excluded_files"]}
        self.assertIn("nested/sub", excluded_rels)
        self.assertIn("zhinav-point-data", excluded_rels)

    def test_discovery_never_enters_skip_dirs(self):
        # node_modules/cache children must never be discovered as skills.
        _make_skill(self.src, "node_modules/pkg/sub", "hidden-cache")
        _make_skill(self.src, "__pycache__/cache", "hidden-pycache")
        _make_skill(self.src, ".pytest_cache/skip", "hidden-pytest")
        _make_skill(self.src, "real", "real")
        rel_roots = {p["rel_root"] for p in self.plan()}
        self.assertEqual(rel_roots, {"real"})

    def test_sk_api_key_detected(self):
        secret = "sk-live-3f9A8b1C2dE3f4A5b6C7d8E9f0A1B2C3D4E5"
        _make_skill(self.src, "skey", "skey", body=f"key={secret}\n")
        excluded = [p for p in self.plan() if p["excluded"]]
        self.assertEqual(len(excluded), 1)
        self.assertIn("OpenAI-style API key", excluded[0]["exclude_reason"])
        report = json.dumps(excluded[0]["excluded_files"])
        self.assertNotIn(secret, report)
        self.assertNotIn(secret, json.dumps(excluded[0]))

    def test_bearer_token_assignment_detected(self):
        secret = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.abc123"
        _make_skill(self.src, "bearer", "bearer",
                    body=f"Authorization: Bearer {secret}\n")
        excluded = [p for p in self.plan() if p["excluded"]]
        self.assertEqual(len(excluded), 1)
        self.assertIn("bearer token", excluded[0]["exclude_reason"])
        report = json.dumps(excluded[0])
        self.assertNotIn(secret, report)

    def test_sk_placeholder_is_exempt(self):
        # Clearly synthetic placeholder sk-* values are acceptable guidance.
        _make_skill(self.src, "ph", "ph",
                    body="sk-your-api-key-here-abcdef0123456789\n")
        rec = self.plan()[0]
        self.assertFalse(rec["excluded"])
        copied_paths = {rel for rel, _ in rec["copied"]}
        self.assertIn("SKILL.md", copied_paths)

    def test_real_sk_key_containing_example_not_swallowed_as_placeholder(self):
        # A long sk- key that merely contains the substring "example" is still
        # a real key and must not be treated as a placeholder.
        secret = "sk-live-example9876543210abcdefghijklmnopqrstuvwxyz123456"
        _make_skill(self.src, "realish", "realish", body=f"key={secret}\n")
        excluded = [p for p in self.plan() if p["excluded"]]
        self.assertEqual(len(excluded), 1)
        self.assertIn("OpenAI-style API key", excluded[0]["exclude_reason"])
        self.assertNotIn(secret, json.dumps(excluded[0]))


class SkillPackagingTests(unittest.TestCase):
    def test_all_installed_resources_survive_release_filter(self):
        import package_portable as packaging
        root = Path(__file__).resolve().parents[1]
        relative = Path('bin/openclaw_service/assistant/openclaw/skills')
        manifest = root / relative / 'workbuddy-registry.json'
        if not manifest.is_file():
            self.skipTest('optional skill bundle not installed')
        records = json.loads(manifest.read_text(encoding='utf-8'))
        self.assertEqual(len(records), 31)
        self.assertEqual(sum(r['metadata_repaired'] for r in records), 2)
        for record in records:
            self.assertNotIn(record['source'], imp.USER_EXCLUDED_ROOTS)
            for value in [record['path'], *record['references']]:
                path = root / relative / value
                self.assertTrue(path.is_file(), value)
                self.assertFalse(packaging._is_development_only_path(path, root), value)

    def test_unknown_workbuddy_resources_are_excluded(self):
        import package_portable as packaging
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            skills = root / 'bin/openclaw_service/assistant/openclaw/skills'
            folder = skills / 'workbuddy/workbuddy-demo/references'
            folder.mkdir(parents=True)
            registered = folder / 'guide.md'
            registered.write_text('guide', encoding='utf-8')
            ignored = folder / 'local.json'
            ignored.write_text('{}', encoding='utf-8')
            (skills / 'workbuddy-registry.json').write_text(json.dumps([{
                'name':'workbuddy-demo', 'kind':'workflow_guide',
                'path':'workbuddy/workbuddy-demo/SKILL.md',
                'references':['workbuddy/workbuddy-demo/references/guide.md']}]), encoding='utf-8')
            self.assertFalse(packaging._is_development_only_path(registered, root))
            self.assertTrue(packaging._is_development_only_path(ignored, root))


class UserExcludedTests(_ImporterTestCase):
    def test_both_roots_and_descendants_excluded_before_open(self):
        # zhinav-point-data root + nested skill; 告警描述核查 root; plus normal.
        _make_skill(self.src, "zhinav-point-data", "zp")
        _make_skill(self.src, "zhinav-point-data/sub/nested", "nested")
        _make_skill(self.src, "告警描述核查/中", "g")
        _make_skill(self.src, "正常技能", "ok")
        plans = self.plan()
        by_rel = {p["rel_root"]: p for p in plans}
        for rel in ("zhinav-point-data", "zhinav-point-data/sub/nested",
                    "告警描述核查/中"):
            self.assertIn(rel, by_rel)
            self.assertTrue(by_rel[rel]["excluded"])
            self.assertEqual(by_rel[rel]["exclude_reason"], "user excluded")
        ok = by_rel["正常技能"]
        self.assertFalse(ok["excluded"])

    def test_excluded_root_malformed_skill_not_reported_as_malformed(self):
        # Exclusion happens before SKILL.md is ever opened.
        d = _make_skill(self.src, "zhinav-point-data", "zp")
        (d / "SKILL.md").write_text(
            "---\nname: zp\ndescription: [unclosed\n", encoding="utf-8")
        rec = self.plan()[0]
        self.assertTrue(rec["excluded"])
        self.assertEqual(rec["exclude_reason"], "user excluded")


class FrontmatterFailClosedTests(_ImporterTestCase):
    def test_only_reviewed_known_metadata_defect_is_repaired_without_source_mutation(self):
        directory = _make_skill(self.src, 'agent-browser__skillhub', 'agent-browser')
        path = directory / 'SKILL.md'
        raw = '---\nname: agent-browser\ndescription: [broken\n---\n# original body\n'
        path.write_text(raw, encoding='utf-8')
        report = self.root / 'report.json'
        report.write_text(json.dumps({'source_unchanged':True,'results':[{
            'skill':'agent-browser__skillhub','source_hash':hashlib.sha256(raw.encode()).hexdigest(),
            'status':'blocked','detail':'CLI missing'}]}), encoding='utf-8')
        rec = imp.build_plan(self.src, test_report=report)[0]
        self.assertFalse(rec['excluded'])
        self.assertTrue(rec['metadata_repaired'])
        self.assertEqual(rec['execution_status'],'guide_only')
        adapted = dict(rec['copied'])['SKILL.md'].decode()
        self.assertEqual(imp.parse_frontmatter(adapted)['name'],'agent-browser')
        self.assertIn('# original body',adapted)
        self.assertEqual(path.read_text(encoding='utf-8'),raw)
        path.write_text(raw + 'changed',encoding='utf-8')
        self.assertTrue(imp.build_plan(self.src,test_report=report)[0]['excluded'])

    def test_malformed_frontmatter_excludes_with_reason_only(self):
        _make_skill(self.src, "broken", "broken")
        bad = self.src / "broken" / "SKILL.md"
        bad.write_text("---\nname: broken\ndescription: [this-is-broken\n---\n# x\n",
                       encoding="utf-8")
        rec = self.plan()[0]
        self.assertTrue(rec["excluded"])
        self.assertIn("malformed YAML frontmatter", rec["exclude_reason"])
        self.assertNotIn("this-is-broken", rec["exclude_reason"])
        self.assertNotIn("this-is-broken", json.dumps(rec["excluded_files"]))

    def test_missing_closing_delimiter_fails_closed(self):
        _make_skill(self.src, "broke2", "broke2")
        bad = self.src / "broke2" / "SKILL.md"
        bad.write_text("---\nname: broke2\ndescription: x\n# no closing marker\n",
                       encoding="utf-8")
        rec = self.plan()[0]
        self.assertTrue(rec["excluded"])
        self.assertIn("invalid SKILL.md frontmatter", rec["exclude_reason"])
        self.assertIn("missing closing", rec["exclude_reason"])

    def test_no_frontmatter_fails_closed(self):
        d = _make_skill(self.src, "nofm", "nofm")
        (d / "SKILL.md").write_text("# just body\n", encoding="utf-8")
        rec = self.plan()[0]
        self.assertTrue(rec["excluded"])
        self.assertIn("missing YAML frontmatter", rec["exclude_reason"])

    def test_empty_frontmatter_fails_closed(self):
        d = _make_skill(self.src, "empt", "empt")
        (d / "SKILL.md").write_text("---\n---\n# body\n", encoding="utf-8")
        rec = self.plan()[0]
        self.assertTrue(rec["excluded"])
        self.assertIn("empty YAML frontmatter", rec["exclude_reason"])

    def test_missing_name_fails_closed(self):
        d = _make_skill(self.src, "noname", "noname")
        (d / "SKILL.md").write_text("---\ndescription: only description\n---\n# x\n",
                                    encoding="utf-8")
        rec = self.plan()[0]
        self.assertTrue(rec["excluded"])
        self.assertIn("missing name", rec["exclude_reason"])

    def test_missing_description_fails_closed(self):
        d = _make_skill(self.src, "nodesc", "nodesc")
        (d / "SKILL.md").write_text("---\nname: only-name\n---\n# x\n",
                                    encoding="utf-8")
        rec = self.plan()[0]
        self.assertTrue(rec["excluded"])
        self.assertIn("missing description", rec["exclude_reason"])

    def test_bom_prefixed_frontmatter_parses(self):
        # UTF-8 BOM before the opening delimiter is stripped and accepted.
        d = _make_skill(self.src, "bom", "bom")
        (d / "SKILL.md").write_bytes(b"\xef\xbb\xbf"
                                     b"---\nname: bom\ndescription: bomd\n---\n# x\n")
        rec = self.plan()[0]
        self.assertFalse(rec["excluded"])
        self.assertEqual(rec["name"], "bom")

    def test_delimiter_with_extra_chars_is_not_a_delimiter(self):
        # "---evil" is NOT a standalone delimiter; fails closed.
        d = _make_skill(self.src, "ev", "ev")
        (d / "SKILL.md").write_text("---evil\nname: ev\ndescription: d\n", encoding="utf-8")
        rec = self.plan()[0]
        self.assertTrue(rec["excluded"])
        self.assertIn("missing YAML frontmatter", rec["exclude_reason"])

    def test_checked_read_is_bounded(self):
        # A file that reports <= max_bytes stat but actually contains more data
        # is still refused because only max_bytes+1 are ever read.
        p = self.root / "big.dat"
        p.write_bytes(b"x" * (imp.MAX_FILE_BYTES + 1))
        self.assertFalse(imp._is_reparse(p))
        data, reason = imp._checked_read(p, imp.MAX_FILE_BYTES)
        self.assertIsNone(data)
        self.assertIn("exceeds", reason)


class ReparseLinkTests(_ImporterTestCase):
    def test_outside_directory_link_not_traversed(self):
        outside = self.root / "outside"
        _make_skill(outside, "hidden", "hidden")
        _make_skill(self.src, "real", "real")
        link = self.src / "link"
        try:
            os.symlink(outside, link, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("directory symlink/junction unsupported on this platform")
        rel_roots = {p["rel_root"] for p in self.plan()}
        self.assertIn("real", rel_roots)
        self.assertNotIn("link/hidden", rel_roots)
        self.assertNotIn("link", rel_roots)
        # The link dir itself must never be treated as a skill root or resource.
        self.assertTrue(all("link" not in p["rel_root"] and "link" not in
                            [e["path"] for e in p["excluded_files"]]
                            for p in self.plan()))

    def test_outside_file_link_not_copied(self):
        outside = self.root / "outside.txt"
        outside.write_text("outside secret\n", encoding="utf-8")
        rec_dir = _make_skill(self.src, "app", "app",
                              refs={"references/a.md": "# a\n"})
        alias = rec_dir / "references" / "alias.md"
        try:
            os.symlink(outside, alias)
        except (OSError, NotImplementedError):
            self.skipTest("file symlink unsupported on this platform")
        rec = self.plan()[0]
        copied_paths = {r for r, _ in rec["copied"]}
        self.assertIn("references/a.md", copied_paths)
        self.assertNotIn("references/alias.md", copied_paths)


class ApplyTests(_ImporterTestCase):
    def test_apply_writes_registry_and_metadata(self):
        _make_skill(self.src, "one", "one", display="一号",
                    refs={"references/guide.md": "content\n"})
        _make_skill(self.src, "two", "two", display="二号")
        records = self.apply()
        self.assertEqual(len(records), 2)
        reg = json.loads((self.out / "workbuddy-registry.json").read_text(encoding="utf-8"))
        self.assertEqual(len(reg), 2)
        for rec in reg:
            for key in ("name", "kind", "display_name", "description", "source",
                        "path", "references", "content_hash",
                        "missing_executable_dependencies", "excluded_files"):
                self.assertIn(key, rec)
            self.assertEqual(rec["kind"], "workflow_guide")
            self.assertTrue((self.out / rec["path"]).is_file())

    def test_repeated_apply_is_byte_identical(self):
        _make_skill(self.src, "app", "app", display="应用",
                    refs={"references/a.md": "# a\n"})
        self.apply()
        reg1 = (self.out / "workbuddy-registry.json").read_bytes()
        files1 = {}
        for p in (self.out / "workbuddy").rglob("*"):
            if p.is_file():
                files1[p.relative_to(self.out).as_posix()] = p.read_bytes()
        self.apply()
        reg2 = (self.out / "workbuddy-registry.json").read_bytes()
        self.assertEqual(reg1, reg2)
        files2 = {}
        for p in (self.out / "workbuddy").rglob("*"):
            if p.is_file():
                files2[p.relative_to(self.out).as_posix()] = p.read_bytes()
        self.assertEqual(files1, files2)

    def test_reapply_removes_now_excluded_skill(self):
        _make_skill(self.src, "a", "alpha")
        self.apply()
        self.assertEqual(self.registry_sources(), {"a"})
        # Remove skill a and add skill b; re-apply same output.
        _make_skill(self.src, "b", "bravo")
        shutil.rmtree(self.src / "a")
        self.apply()
        self.assertEqual(self.registry_sources(), {"b"})

    def test_reapply_no_leftover_secret_from_excluded_skill(self):
        # First run: two normal skills plus one that is entirely excluded.
        _make_skill(self.src, "keep", "keep", refs={"r.md": "keep\n"})
        _make_skill(self.src, "also", "also")
        _make_skill(self.src, "leaky", "leaky", body="cli_a9d1c9fe9d381ceeappid\n")
        self.apply()
        self.assertEqual(self.registry_sources(), {"keep", "also"})
        blob = (self.out / "workbuddy-registry.json").read_text(encoding="utf-8")
        self.assertNotIn("cli_a9d1c9fe9d381cee", blob)
        # Then re-apply with everything removed from source.
        for child in list(self.src.iterdir()):
            shutil.rmtree(child) if child.is_dir() else child.unlink()
        self.apply()
        self.assertEqual(self.registry_sources(), set())
        blob2 = (self.out / "workbuddy-registry.json").read_text(encoding="utf-8")
        self.assertEqual(json.loads(blob2), [])
        self.assertNotIn("cli_a9d1c9fe9d381cee", blob2)

    def test_registry_json_guide_untouched(self):
        guide = self.out / "registry.json"
        guide.write_text("ORIGINAL-GUIDE", encoding="utf-8")
        _make_skill(self.src, "a", "alpha")
        self.apply()
        self.assertEqual(guide.read_text(encoding="utf-8"), "ORIGINAL-GUIDE")

    def test_source_tree_not_mutated(self):
        _make_skill(self.src, "app", "app", extra={"scripts/tool.py": "pass\n"})
        before = {}
        for p in sorted(self.src.rglob("*")):
            if p.is_file():
                before[p.relative_to(self.src).as_posix()] = p.read_bytes()
        self.plan()
        self.apply()
        after = {}
        for p in sorted(self.src.rglob("*")):
            if p.is_file():
                after[p.relative_to(self.src).as_posix()] = p.read_bytes()
        self.assertEqual(before, after)


class ApplySafetyTests(_ImporterTestCase):
    def test_reject_output_inside_source(self):
        _make_skill(self.src, "a", "a")
        inside = self.src / "out" / "skills"
        inside.mkdir(parents=True)
        with self.assertRaises(ValueError):
            imp.apply_plan(self.plan(), inside, self.src)

    def test_reject_source_inside_output(self):
        _make_skill(self.src, "a", "a")
        inner = self.out / "inner-src"
        inner.mkdir()
        with self.assertRaises(ValueError):
            imp.apply_plan(self.plan(), self.out, inner)

    def test_reject_preexisting_dest_symlink(self):
        _make_skill(self.src, "a", "a")
        outside_sub = self.root / "outside-workbuddy"
        outside_sub.mkdir()
        try:
            os.symlink(outside_sub, self.out / "workbuddy", target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("directory symlink unsupported on this platform")
        with self.assertRaises(ValueError):
            self.apply()

    def test_reject_symlink_output_root_before_resolve(self):
        # A directory symlink supplied as the output root must be rejected by
        # unresolved-ancestor validation, not silently resolved away.
        real_out = self.root / "real-out" / "skills"
        real_out.mkdir(parents=True)
        alias = self.root / "alias-skills"
        try:
            os.symlink(real_out, alias, target_is_directory=True)
        except (OSError, NotImplementedError):
            self.skipTest("directory symlink unsupported on this platform")
        _make_skill(self.src, "a", "a")
        with self.assertRaises(ValueError):
            imp.apply_plan(self.plan(), alias, self.src)

    def test_refuse_leftover_backup_recovery_state(self):
        # A leftover workbuddy-backup-* must not be deleted; apply refuses.
        _make_skill(self.src, "a", "a")
        leftover = self.out / "workbuddy-backup-interrupted"
        leftover.mkdir()
        marker = leftover / "only-recoverable-subtree.txt"
        marker.write_text("recover me", encoding="utf-8")
        with self.assertRaises(ValueError) as ctx:
            self.apply()
        self.assertIn("leftover recovery state", str(ctx.exception))
        # The leftover and its content were NOT deleted.
        self.assertTrue(leftover.is_dir())
        self.assertTrue(marker.is_file())

    def test_refuse_leftover_staging_recovery_state(self):
        _make_skill(self.src, "a", "a")
        leftover = self.out / ".workbuddy-staging-interrupted"
        leftover.mkdir()
        with self.assertRaises(ValueError):
            self.apply()
        self.assertTrue(leftover.is_dir())


class IntegrationInvocationTest(unittest.TestCase):
    def test_module_runs_dry_run_subprocess(self):
        """CLI dry-runs a temp source only (never applies to the real bundle)."""
        with tempfile.TemporaryDirectory(prefix="wbskills-cli-") as td:
            src = Path(td) / "src"
            (src / "demo").mkdir(parents=True)
            (src / "demo" / "SKILL.md").write_text(
                "---\nname: demo\ndescription: d\n---\n\nx\n", encoding="utf-8")
            cmd = [sys.executable, str(TOOLS_DIR / "import_lighthouse_workbuddy_skills.py"),
                   "--source", str(src),
                   "--output", str(Path(td) / "out" / "skills"),
                   "--dry-run"]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("skills found: 1", proc.stdout)
            self.assertIn("nothing written", proc.stdout)
            self.assertFalse((Path(td) / "out").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)

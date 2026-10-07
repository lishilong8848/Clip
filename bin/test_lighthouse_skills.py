"""Packaged guides never grant arbitrary paths or business authority.

Covers the reviewed built-in catalog plus the OPTIONAL WorkBuddy imported
registry.  Every test builds its own temporary bundle; the real distributed
``openclaw/skills`` tree is only read for the built-in-name smoke check and is
never mutated.
"""
import json
import os
import sys
import tempfile
import unittest
import warnings
import zipfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import lighthouse_skills as skills
from lan_bitable_template_portal.lighthouse_ai import AssistantError

BUILTIN_REG = [
    {"name": "lighthouse-business", "description": "灯塔业务查询口径、权限和办理确认规则。"},
    {"name": "public-research", "description": "公开搜索和天气查询，保留来源和时间，不外传内部数据。"},
    {"name": "lighthouse-extension", "description": "灯塔助手后续新增已审核插件、技能和受控工具的接入方法。"},
]


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _build_bundle(folder: Path, builtin, workbuddy=None, files=None) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "registry.json").write_text(
        json.dumps(builtin, ensure_ascii=False), encoding="utf-8")
    if workbuddy is not None:
        (folder / "workbuddy-registry.json").write_text(
            json.dumps(workbuddy, ensure_ascii=False), encoding="utf-8")
    for rel, content in (files or {}).items():
        _write(folder / rel, content)


def _wb(name, source="alpha", path=None, references=None, display=None,
        status=None, warnings=None, kind="workflow_guide", description="指引",
        source_test_status=None):
    rec = {
        "name": name,
        "kind": kind,
        "description": description,
        "source": source,
        "path": path or f"workbuddy/{name}/SKILL.md",
        "references": references or [],
        "content_hash": "a" * 64,
        "missing_executable_dependencies": [],
        "excluded_files": [],
    }
    if display is not None:
        rec["display_name"] = display
    if status is not None:
        rec["execution_status"] = status
    if warnings is not None:
        rec["warnings"] = warnings
    if source_test_status is not None:
        rec["source_test_status"] = source_test_status
    return rec


class RealBundleTests(unittest.TestCase):
    def test_builtin_names_present_and_workflow_guides(self):
        cat = skills.catalog()
        names = {item["name"] for item in cat}
        self.assertLessEqual(set(skills._BUILTIN), names)
        by_name = {item["name"]: item for item in cat}
        for name in skills._BUILTIN:
            entry = by_name[name]
            # Builtin guides carry no registered references; catalog reports a
            # count and never the full (potentially long) reference list.
            self.assertEqual(entry["reference_count"], 0)
            self.assertNotIn("references", entry)
            self.assertEqual(entry["execution_status"], "guide_only")
            out = skills.read(name)
            self.assertEqual(out["kind"], "workflow_guide")
            self.assertIn("不是业务数据", out["note"])
            self.assertEqual(out["execution_status"], "guide_only")
            self.assertNotIn("references", out)
        for name in ("../runtime", "/etc/passwd", "C:\\Windows", "unreviewed"):
            with self.subTest(name=name), self.assertRaises(AssistantError):
                skills.read(name)


class ArchiveBundleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='skill-archive-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.name = 'workbuddy-fixture-aabbccdd'
        self.guide = f'workbuddy/{self.name}/SKILL.md'
        self.reference = f'workbuddy/{self.name}/references/guide.md'
        _build_bundle(self.root, BUILTIN_REG, [_wb(self.name, references=[self.reference])],
            {self.guide: 'outdated local guide'})
        self.addCleanup(patch.stopall)
        patch.object(skills, 'ROOT', self.root).start()

    def archive(self, *, guide='current guide', extra=None, compression=zipfile.ZIP_DEFLATED):
        with zipfile.ZipFile(self.root / 'workbuddy.zip', 'w', compression=compression) as archive:
            archive.writestr(self.guide, guide)
            archive.writestr(self.reference, 'current reference')
            for name, body in (extra or {}).items():
                archive.writestr(name, body)

    def test_archive_precedes_previous_generation_local_guides_and_preserves_references(self):
        self.archive()
        self.assertEqual(skills.read(self.name)['content'], 'current guide')
        self.assertEqual(skills.read(self.name, self.reference)['content'], 'current reference')
        self.assertEqual((self.root / self.guide).read_text(), 'outdated local guide')

    def test_unregistered_archive_member_and_traversal_are_not_readable(self):
        self.archive(extra={'private.txt': 'must never be returned'})
        for reference in ('private.txt', '../private.txt', f'workbuddy/{self.name}/private.txt'):
            with self.subTest(reference=reference), self.assertRaises(AssistantError) as failure:
                skills.read(self.name, reference)
            self.assertEqual(failure.exception.status, 404)

    def test_corrupt_archive_never_falls_back_to_old_content(self):
        (self.root / 'workbuddy.zip').write_bytes(b'invalid archive')
        with self.assertRaises(AssistantError) as failure:
            skills.read(self.name)
        self.assertEqual(failure.exception.status, 503)

    def test_duplicate_archive_members_are_rejected(self):
        self.archive()
        with warnings.catch_warnings(), zipfile.ZipFile(self.root / 'workbuddy.zip', 'a') as archive:
            warnings.simplefilter('ignore', UserWarning)
            archive.writestr(self.guide, 'duplicate guide')
        with self.assertRaises(AssistantError):
            skills.read(self.name)

    def test_oversized_resource_is_rejected_before_decompression(self):
        self.archive(guide='x' * (skills._MAX_RESOURCE + 1))
        with self.assertRaises(AssistantError):
            skills.read(self.name)

    def test_unapproved_compression_and_symlink_entries_are_rejected(self):
        self.archive(compression=zipfile.ZIP_LZMA)
        with self.assertRaises(AssistantError):
            skills.read(self.name)
        with zipfile.ZipFile(self.root / 'workbuddy.zip', 'w') as archive:
            info = zipfile.ZipInfo(self.guide)
            info.create_system = 3
            info.external_attr = 0o120777 << 16
            archive.writestr(info, 'outside target')
        with self.assertRaises(AssistantError):
            skills.read(self.name)

    def test_resource_index_count_is_bounded(self):
        self.archive(extra={f'item-{index}.txt': 'x' for index in range(1024)})
        with self.assertRaises(AssistantError):
            skills.read(self.name)

    def test_archive_link_is_rejected_even_when_broken_and_old_content_exists(self):
        try:
            (self.root / 'workbuddy.zip').symlink_to(self.root / 'missing.zip')
        except OSError:
            self.skipTest('Symlink creation unavailable on this test host')
        with self.assertRaises(AssistantError) as failure:
            skills.read(self.name)
        self.assertEqual(failure.exception.status, 503)


class TempBundleTests(unittest.TestCase):
    def test_optional_workbuddy_missing_is_fine(self):
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            _build_bundle(Path(td), BUILTIN_REG)
            names = {item["name"] for item in skills.catalog()}
            self.assertEqual(names, set(skills._BUILTIN))

    def test_imported_skill_catalog_and_read(self):
        wb = [_wb(
            "wb-guide-a", display="导入指南甲", status="guide_only",
            warnings=["仅作流程说明"],
            references=["workbuddy/wb-guide-a/references/guide.md"])]
        files = {
            "workbuddy/wb-guide-a/SKILL.md":
                "---\nname: a\ndescription: d\n---\n# A\n\nskill body\n",
            "workbuddy/wb-guide-a/references/guide.md": "# ref\n\nreference content\n",
        }
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            folder = Path(td)
            _build_bundle(folder, BUILTIN_REG, workbuddy=wb, files=files)
            cat = {item["name"]: item for item in skills.catalog()}
            self.assertIn("wb-guide-a", cat)
            self.assertEqual(cat["wb-guide-a"]["display_name"], "导入指南甲")
            self.assertEqual(cat["wb-guide-a"]["execution_status"], "guide_only")
            self.assertEqual(cat["wb-guide-a"]["warnings"], ["仅作流程说明"])
            self.assertEqual(cat["wb-guide-a"]["reference_count"], 1)
            self.assertNotIn("references", cat["wb-guide-a"])
            self.assertEqual(cat["wb-guide-a"]["description"], "指引")
            for name in skills._BUILTIN:
                self.assertIn(name, cat)

            out = skills.read("wb-guide-a")
            self.assertEqual(out["kind"], "workflow_guide")
            self.assertEqual(out["reference"], "workbuddy/wb-guide-a/SKILL.md")
            self.assertIn("不是业务数据", out["note"])
            self.assertIn("skill body", out["content"])
            self.assertEqual(out["references"],
                             ["workbuddy/wb-guide-a/references/guide.md"])
            ref = skills.read("wb-guide-a",
                              reference="workbuddy/wb-guide-a/references/guide.md")
            self.assertIn("reference content", ref["content"])
            self.assertEqual(ref["reference"],
                             "workbuddy/wb-guide-a/references/guide.md")
            self.assertEqual(ref["execution_status"], "guide_only")
            self.assertEqual(ref["warnings"], ["仅作流程说明"])

    def test_corrupt_workbuddy_registry_is_clear_error(self):
        bad_cases = {
            "not-json": "this is not json",
            "not-list": '{"a": 1}',
            "wrong-kind": [_wb("wb-x", kind="executable")],
            "bad-name": [_wb("../outside")],
            "bad-path": [_wb("wb-x", path="C:\\Windows\\x\\SKILL.md")],
        }
        for label, payload in bad_cases.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as td, \
                    patch.object(skills, "ROOT", Path(td)):
                folder = Path(td)
                _build_bundle(folder, BUILTIN_REG, workbuddy=payload)
                if label == "not-json":
                    (folder / "workbuddy-registry.json").write_text(
                        payload, encoding="utf-8")
                with self.assertRaises(AssistantError):
                    skills.catalog()

    def test_duplicate_names_fail_closed(self):
        # Duplicate inside workbuddy registry.
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            _build_bundle(Path(td), BUILTIN_REG,
                          workbuddy=[_wb("wb-dup"), _wb("wb-dup")])
            with self.assertRaises(AssistantError):
                skills.catalog()
        # Duplicate against a built-in name.
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            _build_bundle(Path(td), BUILTIN_REG,
                          workbuddy=[_wb("lighthouse-business")])
            with self.assertRaises(AssistantError):
                skills.catalog()

    def test_mandatory_exclusions_by_source_and_descendants(self):
        wb = [
            _wb("wb-keep", source="alpha"),
            _wb("wb-zp-root", source="zhinav-point-data"),
            _wb("wb-zp-deep", source="zhinav-point-data/sub/deep"),
            _wb("wb-gg-root", source="告警描述核查"),
            _wb("wb-gg-deep", source="outer/告警描述核查/x"),
        ]
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            _build_bundle(Path(td), BUILTIN_REG, workbuddy=wb)
            names = {item["name"] for item in skills.catalog()}
            self.assertIn("wb-keep", names)
            for excluded in ("wb-zp-root", "wb-zp-deep", "wb-gg-root", "wb-gg-deep"):
                self.assertNotIn(excluded, names)

    def test_unknown_reference_rejected(self):
        wb = [_wb("wb-a", references=["workbuddy/wb-a/references/guide.md"])]
        files = {
            "workbuddy/wb-a/SKILL.md": "# a\n",
            "workbuddy/wb-a/references/guide.md": "# listed\n",
        }
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            folder = Path(td)
            _build_bundle(folder, BUILTIN_REG, workbuddy=wb, files=files)
            for ref in ("workbuddy/wb-a/references/unknown.md",
                        "workbuddy/wb-a/references/unreg.md",
                        "C:\\Windows\\x"):
                with self.subTest(ref=ref), self.assertRaises(AssistantError):
                    skills.read("wb-a", reference=ref)

    def test_traversal_reference_escapes_root_is_refused(self):
        wb = [_wb("wb-trav", references=["workbuddy/wb-trav/../../../escape.md"])]
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            folder = Path(td)
            _build_bundle(folder, BUILTIN_REG, workbuddy=wb,
                          files={"workbuddy/wb-trav/SKILL.md": "# t\n"})
            (folder.parent / "escape.md").write_text("outside secret\n",
                                                     encoding="utf-8")
            with self.assertRaises(AssistantError):
                skills.read("wb-trav", reference="workbuddy/wb-trav/../../../escape.md")

    def test_windows_absolute_path_in_imported_record_rejected(self):
        wb = [_wb("wb-bad", path="C:\\Windows\\x\\SKILL.md")]
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            _build_bundle(Path(td), BUILTIN_REG, workbuddy=wb)
            with self.assertRaises(AssistantError):
                skills.catalog()

    def test_resource_size_limit(self):
        big = "x" * (512 * 1024 + 1)
        wb = [_wb("wb-big", references=["workbuddy/wb-big/ref.md"])]
        files = {
            "workbuddy/wb-big/SKILL.md": "# big\n",
            "workbuddy/wb-big/ref.md": big,
        }
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            _build_bundle(Path(td), BUILTIN_REG, workbuddy=wb, files=files)
            with self.assertRaises(AssistantError):
                skills.read("wb-big", reference="workbuddy/wb-big/ref.md")

    def test_symlink_resource_refused(self):
        wb = [_wb("wb-link", references=["workbuddy/wb-link/alias.md"])]
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            folder = Path(td)
            _build_bundle(folder, BUILTIN_REG, workbuddy=wb,
                          files={"workbuddy/wb-link/SKILL.md": "# link\n"})
            outside = folder / "outside.txt"
            outside.write_text("outside secret\n", encoding="utf-8")
            alias = folder / "workbuddy" / "wb-link" / "alias.md"
            try:
                alias.symlink_to(outside)
            except (OSError, NotImplementedError):
                self.skipTest("symlink unsupported on this platform")
            with self.assertRaises(AssistantError):
                skills.read("wb-link", reference="workbuddy/wb-link/alias.md")

    def test_chunk_offsets(self):
        # Many short lines (no single pathological long line) keep
        # safe_text's CONTACT regex linear; offsets are over the scrubbed text.
        from lan_bitable_template_portal.lighthouse_ai import safe_text
        body = "lineabcdefghij\n" * 3000
        wb = [_wb("wb-chunk")]
        files = {"workbuddy/wb-chunk/SKILL.md": body}
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            _build_bundle(Path(td), BUILTIN_REG, workbuddy=wb, files=files)
            expected = safe_text(body, limit=skills._MAX_RESOURCE)
            first = skills.read("wb-chunk")
            self.assertEqual(first["content"], expected[:skills._CHUNK])
            self.assertEqual(first["next_offset"], skills._CHUNK)
            second = skills.read("wb-chunk", offset=first["next_offset"])
            self.assertEqual(second["content"], expected[skills._CHUNK:2 * skills._CHUNK])
            self.assertEqual(second["next_offset"], 2 * skills._CHUNK)
            third = skills.read("wb-chunk", offset=second["next_offset"])
            self.assertEqual(third["content"], expected[2 * skills._CHUNK:])
            self.assertEqual(third["next_offset"], 0)

    def test_unregistered_file_ignored(self):
        wb = [_wb("wb-u", references=["workbuddy/wb-u/listed.md"])]
        files = {
            "workbuddy/wb-u/SKILL.md": "# u\n",
            "workbuddy/wb-u/listed.md": "listed\n",
            "workbuddy/wb-u/unreg.md": "not registered\n",
        }
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            folder = Path(td)
            _build_bundle(folder, BUILTIN_REG, workbuddy=wb, files=files)
            cat = {item["name"]: item for item in skills.catalog()}
            self.assertEqual(cat["wb-u"]["reference_count"], 1)
            self.assertNotIn("references", cat["wb-u"])
            with self.assertRaises(AssistantError):
                skills.read("wb-u", reference="workbuddy/wb-u/unreg.md")
            self.assertIn("listed",
                          skills.read("wb-u",
                                      reference="workbuddy/wb-u/listed.md")["content"])

    def test_cross_skill_and_internal_traversal_rejected(self):
        bad_records = [
            _wb("one", references=["workbuddy/one/../../lighthouse-business/SKILL.md"]),
            _wb("one", references=["workbuddy/other/ref.md"]),
            _wb("one", path="workbuddy/other/SKILL.md"),
            _wb("one", path="workbuddy/one/../../lighthouse-business/SKILL.md"),
            _wb("one", references=["workbuddy/one/../one/SKILL.md"]),
            _wb("one", references=["workbuddy\\one\\ref.md"]),
        ]
        for rec in bad_records:
            with self.subTest(rec=rec), tempfile.TemporaryDirectory() as td, \
                    patch.object(skills, "ROOT", Path(td)):
                _build_bundle(Path(td), BUILTIN_REG, workbuddy=[rec],
                              files={"workbuddy/one/SKILL.md": "# one\n"})
                with self.assertRaises(AssistantError):
                    skills.catalog()

    def test_metadata_limits_and_types_rejected(self):
        cases = [
            ("over-refs",
             [_wb("over",
                  references=[f"workbuddy/over/ref{i}.md" for i in range(129)])]),
            ("over-warnings", [_wb("warn", warnings=["w"] * 21)]),
            ("bad-display-type", [_wb("bad-display", display=123)]),
            ("bad-warnings-type", [_wb("bad-warn", warnings="just a string")]),
            ("bad-source-test-type", [_wb("bad-st", source_test_status=5)]),
            ("over-description", [_wb("bigdesc", description="d" * 8001)]),
            ("over-display", [_wb("bigdisplay", display="d" * 201)]),
            ("over-source-test", [_wb("bigst", source_test_status="s" * 81)]),
            ("over-warning-len", [_wb("bigwarn", warnings=["w" * 1001])]),
        ]
        for label, payload in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as td, \
                    patch.object(skills, "ROOT", Path(td)):
                for rec in payload:
                    files = {rec["path"]: "# x\n"}
                    _build_bundle(Path(td), BUILTIN_REG, workbuddy=[rec], files=files)
                with self.assertRaises(AssistantError):
                    skills.catalog()

    def test_source_test_status_and_forced_guide_only(self):
        # Untrusted metadata may claim a different executable status; it must
        # still surface as guide_only.  source_test_status is passed through.
        wb = [_wb("wb-st", display="展示", status="executable",
                  source_test_status="passed",
                  references=["workbuddy/wb-st/references/guide.md"])]
        files = {
            "workbuddy/wb-st/SKILL.md": "# st\n",
            "workbuddy/wb-st/references/guide.md": "# ref\n",
        }
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            folder = Path(td)
            _build_bundle(folder, BUILTIN_REG, workbuddy=wb, files=files)
            entry = next(i for i in skills.catalog() if i["name"] == "wb-st")
            self.assertEqual(entry["execution_status"], "guide_only")
            self.assertEqual(entry["source_test_status"], "passed")
            self.assertEqual(entry["reference_count"], 1)
            out = skills.read("wb-st")
            self.assertEqual(out["execution_status"], "guide_only")
            self.assertEqual(out["source_test_status"], "passed")
            self.assertEqual(out["references"],
                             ["workbuddy/wb-st/references/guide.md"])

    def test_read_metadata_sanitized_consistently(self):
        # read() must scrub warnings exactly like catalog() (no raw metadata).
        wb = [_wb("wb-meta", display="展示", warnings=["联系 13800138000 可查"])]
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            folder = Path(td)
            _build_bundle(folder, BUILTIN_REG, workbuddy=wb,
                          files={"workbuddy/wb-meta/SKILL.md": "# meta\n"})
            cat_entry = next(i for i in skills.catalog() if i["name"] == "wb-meta")
            out = skills.read("wb-meta")
            self.assertEqual(out["warnings"], cat_entry["warnings"])
            # The phone-bearing line is dropped by safe_text.
            self.assertEqual(out["warnings"], [""])

    def test_credential_crossing_chunk_boundary_scrubbed(self):
        secret = "sk-LIVE-test-secret-1234567890"
        # 3200 "line\n" lines = 16000 chars, then a credential line and a
        # contact line; the api_key: line straddles the 16000-char boundary.
        before = "line\n" * 3200  # 16000 chars
        body = before + f"api_key: {secret}\ncontact: 13800138000\n" + ("safe\n" * 10)
        wb = [_wb("wb-secret")]
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            folder = Path(td)
            _build_bundle(folder, BUILTIN_REG, workbuddy=wb,
                          files={"workbuddy/wb-secret/SKILL.md": body})
            # Prove the raw boundary really splits the credential line.
            self.assertIn("api_key:", body[skills._CHUNK - 12: skills._CHUNK + 12])
            pages = []
            offset = 0
            while True:
                out = skills.read("wb-secret", offset=offset)
                pages.append(out["content"])
                if not out["next_offset"]:
                    break
                offset = out["next_offset"]
            joined = "".join(pages)
            self.assertNotIn(secret, joined)
            self.assertNotIn("api_key", joined)
            self.assertNotIn("13800138000", joined)
            self.assertIn("safe", joined)
            self.assertIn("line", joined)

    def test_pathological_long_line_and_oversized_secret_omitted(self):
        # Original 40KB one-line guide plus an oversized credential/contact
        # line.  Both exceed the per-line budget and must be replaced with the
        # explicit marker BEFORE safe_text (a huge single line would otherwise
        # stall the CONTACT regex).  Runs in a subprocess so a regression that
        # hangs the regex fails via timeout instead of freezing the suite.
        import json
        import subprocess
        import sys as _sys
        import time as _time
        secret = "sk-LIVE-test-secret-1234567890"
        phone = "13800138000"
        giant = "x" * (40 * 1024)
        cred_line = f"api_key={secret} contact={phone} " + ("y" * 2500)
        body = giant + "\n" + cred_line + "\n" + "normal short line\n"
        wb = [_wb("wb-long")]
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            folder = Path(td)
            _build_bundle(folder, BUILTIN_REG, workbuddy=wb,
                          files={"workbuddy/wb-long/SKILL.md": body})
            code = (
                "import json,sys\n"
                "from pathlib import Path\n"
                "import lan_bitable_template_portal.lighthouse_skills as skills\n"
                "skills.ROOT = Path(sys.argv[1])\n"
                "out = skills.read('wb-long')\n"
                "print(json.dumps({'omitted': out['omitted_long_lines'], "
                "'content': out['content']}))"
            )
            env = dict(os.environ)
            env["PYTHONPATH"] = os.pathsep.join(
                [str(Path(__file__).resolve().parent),
                 env.get("PYTHONPATH", "")])
            start = _time.monotonic()
            proc = subprocess.run(
                [_sys.executable, "-c", code, str(folder)],
                capture_output=True, text=True, timeout=2, env=env)
            elapsed = _time.monotonic() - start
            self.assertLess(elapsed, 2.0, "regression read exceeded 2s")
            self.assertEqual(proc.returncode, 0, proc.stderr)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["omitted"], 2)
            content = payload["content"]
            self.assertIn(skills._OMITTED_MARKER, content)
            self.assertNotIn(secret, content)
            self.assertNotIn(phone, content)
            self.assertNotIn("api_key=", content)
            self.assertIn("normal short line", content)

    def test_arbitrary_offsets_refer_to_sanitized_text(self):
        from lan_bitable_template_portal.lighthouse_ai import safe_text
        body = "guide body\n" * 3000  # 33000 chars, several pages
        wb = [_wb("wb-offset")]
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            folder = Path(td)
            _build_bundle(folder, BUILTIN_REG, workbuddy=wb,
                          files={"workbuddy/wb-offset/SKILL.md": body})
            sanitized = safe_text(body, limit=skills._MAX_RESOURCE)
            for offset in (0, 123, skills._CHUNK + 7, len(sanitized) - 1,
                           len(sanitized), len(sanitized) + 10):
                with self.subTest(offset=offset):
                    out = skills.read("wb-offset", offset=offset)
                    expected = sanitized[offset:offset + skills._CHUNK]
                    self.assertEqual(out["content"], expected)
                    if offset >= len(sanitized):
                        self.assertEqual(out["next_offset"], 0)
                    else:
                        end = min(offset + len(expected), len(sanitized))
                        self.assertEqual(out["next_offset"],
                                         0 if end >= len(sanitized) else offset + len(expected))

    def test_broken_symlink_optional_registry_refused(self):
        if not hasattr(os, "symlink"):
            self.skipTest("symlink unsupported on this platform")
        with tempfile.TemporaryDirectory() as td, \
                patch.object(skills, "ROOT", Path(td)):
            folder = Path(td)
            _build_bundle(folder, BUILTIN_REG)
            link = folder / "workbuddy-registry.json"
            try:
                link.symlink_to(folder / "does-not-exist-target")
            except (OSError, NotImplementedError):
                self.skipTest("symlink unsupported on this platform")
            with self.assertRaises(AssistantError):
                skills.catalog()

    def test_root_symlink_refused_for_registries(self):
        if not hasattr(os, "symlink"):
            self.skipTest("symlink unsupported on this platform")
        with tempfile.TemporaryDirectory() as td:
            real = Path(td) / "real"
            _build_bundle(real, BUILTIN_REG)
            link = Path(td) / "link-root"
            try:
                link.symlink_to(real, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("symlink unsupported on this platform")
            with patch.object(skills, "ROOT", link):
                with self.assertRaises(AssistantError):
                    skills.catalog()


class LegacyGuardTests(unittest.TestCase):
    def test_malformed_catalog_is_refused(self):
        with tempfile.TemporaryDirectory() as folder, \
                patch.object(skills, "ROOT", Path(folder)):
            (Path(folder) / "registry.json").write_text(
                '[{"name":"../outside","description":"x"}]', encoding="utf-8")
            with self.assertRaises(AssistantError):
                skills.catalog()

    def test_guides_do_not_claim_live_evidence(self):
        for item in skills.catalog():
            self.assertIn("不是业务数据", skills.read(item["name"])["note"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

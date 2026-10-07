"""Focused unit tests for shared guidance-only skills (SharedSkills).

Every test uses an in-memory FakeStore so no real database or cloud data is
ever touched, and no credentials are printed.  A couple of tests additionally
spin up a real in-memory AssistantStore to prove persistence against a genuine
store (which returns a fresh copy per read), matching the production lock and
deep-copy semantics.  The shared namespace must be part of the real,
unmodified ``store.ASSISTANT_NAMESPACES`` whitelist; we never patch or mutate
production constants in these tests.
"""
import copy
import io
import sys
import tempfile
import threading
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_shared_skills import (
    SharedSkills,
    AssistantError,
    NAMESPACE,
    INDEX_KEY,
)
from openclaw_service.store import AssistantStore, ASSISTANT_NAMESPACES

# The shared namespace must already be whitelisted by the real, unmodified
# store.  Never patch or mutate ASSISTANT_NAMESPACES here just to pass.
assert NAMESPACE in ASSISTANT_NAMESPACES, (
    "NAMESPACE %r must be in store.ASSISTANT_NAMESPACES" % NAMESPACE
)

CHUNK = 16000
GUIDE = """---
name: my-first-skill
description: A sample shared skill for unit tests.
---

# Body

Some guidance for the agent.
"""
REF = "# Reference\n\nUseful extra detail.\n"
SHARED_PREFIX = "shared-"


class FakeStore:
    """Minimal in-memory store implementing get_document/put_document.

    get_document/put_document deep-copy payloads (matching the fresh dict a real
    store returns per read) and expose a shared RLock so several SharedSkills
    facades on the same store serialize mutations through one lock.
    """

    def __init__(self):
        self._docs = {}
        self._lock = threading.RLock()

    def get_document(self, namespace, key):
        payload = self._docs.get((namespace, key))
        return copy.deepcopy(payload) if payload is not None else None

    def put_document(self, namespace, key, payload):
        self._docs[(namespace, key)] = copy.deepcopy(payload)

    def put_documents(self, namespace, payloads):
        """Atomically store a batch of documents, deep-copying every payload."""
        copied = [
            ((namespace, key), copy.deepcopy(payload)) for key, payload in payloads.items()
        ]
        self._docs.update(copied)


class TrackingStore:
    """Wraps an AssistantStore and records which document keys get fetched.

    Used to prove a catalog/search only reads the shared index document and
    never fetches per-skill content documents.
    """

    def __init__(self, store):
        self._store = store
        self._lock = store._lock
        self.get_calls = []

    def get_document(self, namespace, key):
        self.get_calls.append((namespace, key))
        return self._store.get_document(namespace, key)

    def put_documents(self, namespace, payloads):
        return self._store.put_documents(namespace, payloads)

    def put_document(self, namespace, key, payload):
        return self._store.put_document(namespace, key, payload)


def make_zip(entries):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in entries:
            archive.writestr(name, data)
    return buffer.getvalue()


def user_a(name="Alice"):
    return {"id": "actor-aaaa-0001", "name": name, "scopes": ["A"]}


def user_b(name="Bob"):
    return {"id": "actor-bbbb-0002", "name": name, "scopes": ["B"]}


def admin():
    return {"id": "actor-admin-0009", "name": "Admin", "is_admin": True, "scopes": []}


class SharedSkillsTests(unittest.TestCase):
    def setUp(self):
        self.store = FakeStore()
        self.skills = SharedSkills(self.store)

    # ---------------------------------------------------------------- happy path

    def test_single_markdown_install_catalog_read_and_restart(self):
        result = self.skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
        self.assertFalse(result["duplicate"])
        self.assertTrue(result["name"].startswith(SHARED_PREFIX))
        self.assertEqual(result["source"], "shared")
        self.assertTrue(result["removable"])
        name = result["name"]

        catalog = self.skills.catalog(user_a())
        self.assertEqual(len(catalog), 1)
        entry = catalog[0]
        self.assertEqual(entry["name"], name)
        self.assertEqual(entry["execution_status"], "guide_only")
        self.assertEqual(entry["source"], "shared")
        self.assertTrue(entry["removable"])
        self.assertEqual(entry["reference_count"], 0)
        self.assertNotIn("content", entry)

        out = self.skills.read(user_a(), name)
        self.assertEqual(out["guide_only"], True)
        self.assertEqual(out["source"], "shared")
        self.assertIn("Body", out["content"])
        self.assertIn("my-first-skill", out["content"])
        self.assertEqual(out["next_offset"], 0)

        # Restart via a fresh facade over the same store.
        restarted = SharedSkills(self.store)
        self.assertEqual(len(restarted.catalog(user_a())), 1)
        self.assertIn("Body", restarted.read(user_a(), name)["content"])

    def test_zip_with_references_and_scripts_warning(self):
        raw = make_zip([
            ("my-skill/SKILL.md", GUIDE),
            ("my-skill/references/guide.md", REF),
            ("my-skill/scripts/run.py", "print('never executed')\n"),
        ])
        result = self.skills.install(user_a(), "my-skill.zip", raw)
        self.assertFalse(result["duplicate"])
        self.assertEqual(result["source"], "shared")
        name = result["name"]

        catalog = self.skills.catalog(user_a())
        self.assertEqual(catalog[0]["reference_count"], 1)
        warnings = catalog[0].get("warnings", [])
        self.assertTrue(any("scripts" in w or "run.py" in w for w in warnings))
        self.assertTrue(any("不可用" in w or "未加载" in w for w in warnings))

        out = self.skills.read(user_a(), name)
        self.assertEqual(out["reference"], "SKILL.md")
        self.assertEqual(out["references"], ["references/guide.md"])
        self.assertEqual(out["guide_only"], True)
        self.assertEqual(out["source"], "shared")

        ref = self.skills.read(user_a(), name, reference="references/guide.md")
        self.assertIn("Useful extra detail", ref["content"])
        self.assertEqual(ref["reference"], "references/guide.md")

        with self.assertRaises(AssistantError):
            self.skills.read(user_a(), name, reference="references/unknown.md")

    def test_zip_skill_at_root_with_nested_references(self):
        # Standard layout: SKILL.md at the archive root with a references/ dir.
        raw = make_zip([
            ("SKILL.md", GUIDE),
            ("references/guide.md", REF),
            ("references/tips.md", "# Tips\n"),
        ])
        result = self.skills.install(user_a(), "root-skill.zip", raw)
        self.assertFalse(result["duplicate"])
        name = result["name"]
        catalog = self.skills.catalog(user_a())
        self.assertEqual(catalog[0]["reference_count"], 2)
        out = self.skills.read(user_a(), name)
        self.assertEqual(out["reference"], "SKILL.md")
        self.assertEqual(out["references"], ["references/guide.md", "references/tips.md"])
        self.assertIn("Useful extra detail",
                      self.skills.read(user_a(), name, reference="references/guide.md")["content"])

    def test_long_content_paging(self):
        body = "line text for paging\n" * 4000  # ~80000 chars, many pages
        md = "---\nname: paging-skill\ndescription: Paging test.\n---\n\n" + body
        result = self.skills.install(user_a(), "paging.md", md.encode("utf-8"))
        name = result["name"]

        pages = []
        offset = 0
        while True:
            out = self.skills.read(user_a(), name, offset=offset)
            pages.append(out["content"])
            if not out["next_offset"]:
                break
            self.assertLessEqual(len(out["content"]), CHUNK)
            self.assertEqual(out["offset"], offset)
            self.assertEqual(out["next_offset"], offset + len(out["content"]))
            offset = out["next_offset"]
        joined = "".join(pages)
        self.assertIn("line text for paging", joined)
        self.assertGreater(len(joined), CHUNK)
        self.assertEqual(joined, md.rstrip("\n"))

    # ------------------------------------------------------------- cross account

    def test_cross_account_read_catalog_and_use_success(self):
        self.skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
        name = self.skills.catalog(user_a())[0]["name"]

        # user_b can catalog, read and use the same shared skill.
        catalog_b = self.skills.catalog(user_b())
        self.assertEqual(len(catalog_b), 1)
        self.assertEqual(catalog_b[0]["name"], name)
        self.assertEqual(catalog_b[0]["source"], "shared")
        self.assertFalse(catalog_b[0]["removable"])  # not creator, not admin
        out_b = self.skills.read(user_b(), name)
        self.assertIn("Body", out_b["content"])
        self.assertEqual(out_b["source"], "shared")
        self.assertFalse(out_b["removable"])

    def test_removal_owner_admin_and_nonowner_403(self):
        self.skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
        name = self.skills.catalog(user_a())[0]["name"]

        # Non-owner (and non-admin) cannot remove.
        with self.assertRaises(AssistantError) as failure:
            self.skills.remove(user_b(), name)
        self.assertEqual(failure.exception.status, 403)
        # Skill still present.
        self.assertEqual(len(self.skills.catalog(user_a())), 1)

        # Admin can remove.
        self.assertEqual(self.skills.remove(admin(), name)["removed"], True)
        self.assertEqual(self.skills.catalog(user_a()), [])

        # Reinstall so we can prove the creator can also remove.
        self.skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
        name2 = self.skills.catalog(user_a())[0]["name"]
        self.assertEqual(self.skills.remove(user_a(), name2)["removed"], True)
        self.assertEqual(self.skills.catalog(user_a()), [])

    def test_creator_can_remove_after_other_account_duplicates(self):
        self.skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
        name = self.skills.catalog(user_a())[0]["name"]
        dup = self.skills.install(user_b(), "renamed.md", GUIDE.encode("utf-8"))
        self.assertTrue(dup["duplicate"])
        self.assertEqual(dup["name"], name)
        self.assertFalse(dup["removable"])

        # Original creator can still remove.
        self.assertEqual(self.skills.remove(user_a(), name)["removed"], True)
        self.assertEqual(self.skills.catalog(user_b()), [])

    def test_missing_identity_rejected(self):
        for actor in ({}, {"user_id": ""}, {"open_id": "   "}):
            with self.subTest(actor=actor), self.assertRaises(AssistantError) as failure:
                self.skills.catalog(actor)
            self.assertEqual(failure.exception.status, 403)
        with self.assertRaises(AssistantError):
            self.skills.install({}, "x.md", GUIDE.encode("utf-8"))
        with self.assertRaises(AssistantError):
            self.skills.read({}, "x")
        with self.assertRaises(AssistantError):
            self.skills.remove({}, "shared-x-yyyyyyyy")

    # ------------------------------------------------------------- lifecycle

    def test_remove(self):
        result = self.skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
        name = result["name"]
        removed = self.skills.remove(user_a(), name)
        self.assertEqual(removed, {"removed": True, "name": name})
        self.assertEqual(self.skills.catalog(user_a()), [])
        with self.assertRaises(AssistantError):
            self.skills.read(user_a(), name)
        with self.assertRaises(AssistantError):
            self.skills.remove(user_a(), name)

    def test_duplicate_exact_content_cross_account_preserves_creator(self):
        first = self.skills.install(user_a(), "skill.md", GUIDE.encode("utf-8"))
        second = self.skills.install(user_b(), "skill-renamed.md", GUIDE.encode("utf-8"))
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(second["name"], first["name"])
        self.assertEqual(len(self.skills.catalog(user_a())), 1)
        # Ownership is preserved: creator remains removable, duplicator not.
        self.assertEqual(self.skills.catalog(user_a())[0]["removable"], True)
        self.assertEqual(self.skills.catalog(user_b())[0]["removable"], False)

    def test_same_stable_name_different_content_conflict(self):
        self.skills.install(user_a(), "mine.md", GUIDE.encode("utf-8"))
        # A different skill that yields the same slug and same source path but
        # different content is a name conflict: it must not silently overwrite.
        other = GUIDE.replace("Some guidance", "Different body content entirely")
        with self.assertRaises(AssistantError) as failure:
            self.skills.install(user_b(), "mine.md", other.encode("utf-8"))
        self.assertEqual(failure.exception.status, 409)

    def test_global_cap(self):
        for index in range(100):
            md = ("---\nname: cap-skill-%03d\ndescription: cap %d.\n---\n\nbody %d\n"
                  % (index, index, index))
            self.skills.install(user_a(), "cap-%03d.md" % index, md.encode("utf-8"))
        self.assertEqual(len(self.skills.catalog(user_a())), 100)
        last = ("---\nname: cap-skill-over\ndescription: over.\n---\n\nover\n")
        with self.assertRaises(AssistantError) as failure:
            self.skills.install(user_a(), "over.md", last.encode("utf-8"))
        self.assertEqual(failure.exception.status, 409)

    # ------------------------------------------------------------- zip attacks

    def test_zip_attacks_rejected(self):
        # Note: zipfile normalises literal backslashes to forward slashes when
        # it writes a member, so backslash membership is covered directly
        # against SharedSkills._valid_zip_path below.
        malicious = {
            "traversal": ("go/../SKILL.md", GUIDE),
            "drive": ("C:/evil/SKILL.md", GUIDE),
            "unc": ("//server/share/SKILL.md", GUIDE),
            "absolute": ("/etc/SKILL.md", GUIDE),
        }
        for label, (name, content) in malicious.items():
            raw = make_zip([(name, content)])
            with self.subTest(label=label), self.assertRaises(AssistantError):
                self.skills.install(user_a(), "bad.zip", raw)

    def test_invalid_zip_path_rejected(self):
        invalid = {
            "traversal": "dir/../SKILL.md",
            "backslash": r"dir\\SKILL.md",
            "nul": "dir/\x00SKILL.md",
            "drive": "C:/evil/SKILL.md",
            "unc": "//server/share/SKILL.md",
            "absolute": "/etc/SKILL.md",
            "dot": "./SKILL.md",
        }
        for label, name in invalid.items():
            with self.subTest(label=label):
                ok, _reason = SharedSkills._valid_zip_path(name)
                self.assertFalse(ok)

    def test_control_character_and_overlong_zip_path_rejected(self):
        control_path = "dir/\x01SKILL.md"
        overlong_path = ("d" * 240) + "/SKILL.md"  # >240 bytes
        self.assertGreater(len(overlong_path), 240)
        for label, name in {
            "control": control_path,
            "overlong": overlong_path,
        }.items():
            with self.subTest(label=label):
                ok, reason = SharedSkills._valid_zip_path(name)
                self.assertFalse(ok, reason)

        # And through a full install (control chars are preserved in a member name
        # when written directly, so use the raw-archive route).
        raw = make_zip([(control_path, GUIDE)])
        with self.subTest(label="install-control"), self.assertRaises(AssistantError):
            self.skills.install(user_a(), "control.zip", raw)

    def test_unsupported_compression_rejected(self):
        # Simulate an archive entry compressed with an unsupported method so the
        # whitelist rejection fires before any member is opened.
        class FakeInfo:
            filename = "my-skill/SKILL.md"
            orig_filename = "my-skill/SKILL.md"
            flag_bits = 0
            file_size = 10
            external_attr = 0
            create_system = 3
            compress_type = zipfile.ZIP_BZIP2  # unsupported

        class FakeArchive:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def infolist(self):
                return [FakeInfo()]

            def open(self, info):
                raise AssertionError("unsupported member must never be opened")

        with patch("openclaw_service.assistant.lighthouse_shared_skills.zipfile.ZipFile",
                   FakeArchive):
            with self.assertRaises(AssistantError) as failure:
                self.skills.install(user_a(), "unsupported.zip", b"dummy-archive")
        self.assertEqual(failure.exception.status, 400)

    def test_symlink_entry_rejected(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            info = zipfile.ZipInfo("my-skill/SKILL.md")
            info.create_system = 3
            info.external_attr = 0o120777 << 16
            archive.writestr(info, "outside target")
        with self.assertRaises(AssistantError):
            self.skills.install(user_a(), "link.zip", buffer.getvalue())

    def test_case_collision_rejected(self):
        raw = make_zip([
            ("MySkill/SKILL.md", GUIDE),
            ("myskill/SKILL.md", GUIDE.replace("my-first-skill", "other")),
        ])
        with self.assertRaises(AssistantError) as failure:
            self.skills.install(user_a(), "collision.zip", raw)
        self.assertEqual(failure.exception.status, 400)

    def test_encrypted_entry_rejected(self):
        # The stdlib ZipFile re-writes flag_bits when writing members, so an
        # encrypted entry is simulated by patching infolist() to carry bit 0.
        class FakeInfo:
            filename = "my-skill/SKILL.md"
            orig_filename = "my-skill/SKILL.md"
            flag_bits = 1                  # encrypted (bit 0)
            file_size = 10
            external_attr = 0
            create_system = 3
            compress_type = zipfile.ZIP_DEFLATED

        class FakeArchive:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def infolist(self):
                return [FakeInfo()]

            def open(self, info):
                raise AssertionError("encrypted member must never be opened")

        with patch("openclaw_service.assistant.lighthouse_shared_skills.zipfile.ZipFile",
                   FakeArchive):
            with self.assertRaises(AssistantError):
                self.skills.install(user_a(), "encrypted.zip", b"dummy-archive")

    def test_multiple_guide_rejected(self):
        raw = make_zip([
            ("one/SKILL.md", GUIDE),
            ("two/SKILL.md", GUIDE.replace("my-first-skill", "other")),
        ])
        with self.assertRaises(AssistantError):
            self.skills.install(user_a(), "two-guides.zip", raw)

    def test_missing_guide_rejected(self):
        raw = make_zip([("my-skill/references/guide.md", REF)])
        with self.assertRaises(AssistantError):
            self.skills.install(user_a(), "no-guide.zip", raw)

    def test_total_uncompressed_too_large(self):
        big = "z" * (300 * 1024)
        entries = [("my-skill/%02d-guide.md" % i, big) for i in range(10)]
        entries.insert(0, ("my-skill/SKILL.md", GUIDE))
        raw = make_zip(entries)  # ~3 MiB uncompressed, each file < 512 KiB
        with self.assertRaises(AssistantError):
            self.skills.install(user_a(), "big.zip", raw)

    def test_invalid_utf8_rejected(self):
        with self.assertRaises(AssistantError):
            self.skills.install(user_a(), "bad.md", b"\xff\xfe\x00\x01")

    def test_bad_yaml_rejected(self):
        bad_cases = {
            "syntax": b"---\nname: x\n: bad\n---\nbody\n",
            "not-mapping": b"---\n- a\n- b\n---\nbody\n",
            "missing-name": b"---\ndescription: only\n---\nbody\n",
            "missing-description": b"---\nname: x\n---\nbody\n",
            "oversized-description": b"---\nname: x\ndescription: %s\n---\nbody\n" % ("d" * 1025).encode(),
            "bad-name": b"---\nname: Not Valid!\ndescription: ok\n---\nbody\n",
        }
        for label, payload in bad_cases.items():
            with self.subTest(label=label), self.assertRaises(AssistantError):
                self.skills.install(user_a(), label + ".md", payload)

    def test_unsupported_script_warning_and_never_loaded(self):
        raw = make_zip([
            ("tool/SKILL.md", GUIDE),
            ("tool/run.sh", "#!/bin/sh\nrm -rf /\n"),
            ("tool/data.bin", b"\x00\x01\x02\x03"),
        ])
        result = self.skills.install(user_a(), "tool.zip", raw)
        warnings = result.get("warnings", [])
        self.assertTrue(any("run.sh" in w and ("不可用" in w or "未加载" in w) for w in warnings))
        self.assertTrue(any("data.bin" in w and ("不可用" in w or "未加载" in w) for w in warnings))
        name = result["name"]
        out = self.skills.read(user_a(), name)
        self.assertNotIn("rm -rf", out["content"])
        with self.assertRaises(AssistantError):
            self.skills.read(user_a(), name, reference="run.sh")

    def test_invalid_offset_rejected(self):
        self.skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
        name = self.skills.catalog(user_a())[0]["name"]
        for bad in (-1, True, 1.5, "0"):
            with self.subTest(offset=bad), self.assertRaises(AssistantError):
                self.skills.read(user_a(), name, offset=bad)

    def test_builtin_never_overridden(self):
        # A skill whose frontmatter name equals a built-in must still be
        # installed under a shared- prefixed external name, never as the
        # built-in slug, so it can never override a packaged skill.
        md = GUIDE.replace("name: my-first-skill", "name: lighthouse-business")
        result = self.skills.install(user_a(), "builtin.md", md.encode("utf-8"))
        name = result["name"]
        self.assertTrue(name.startswith(SHARED_PREFIX))
        self.assertNotEqual(name, "lighthouse-business")
        self.assertIn("lighthouse-business", name)  # slug preserved in the shared name

    # ------------------------------------------------- persistence / concurrency

    def test_remove_persists_against_real_store(self):
        # A real AssistantStore returns a fresh dict per get_document, so a
        # remove that fails to put_document would NOT survive a restart.  This
        # regression captures that defect.
        with tempfile.TemporaryDirectory() as tmp:
            store = AssistantStore(tmp)
            skills = SharedSkills(store)
            skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
            name = skills.catalog(user_a())[0]["name"]

            removed = skills.remove(user_a(), name)
            self.assertEqual(removed, {"removed": True, "name": name})

            restarted = SharedSkills(store)
            self.assertEqual(restarted.catalog(user_a()), [])
            with self.assertRaises(AssistantError):
                restarted.read(user_a(), name)

    def test_real_store_install_read_and_restart(self):
        # The split index/content schema must persist against a genuine
        # AssistantStore and survive a fresh SharedSkills facade.
        with tempfile.TemporaryDirectory() as tmp:
            store = AssistantStore(tmp)
            skills = SharedSkills(store)
            result = skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
            name = result["name"]

            restarted = SharedSkills(store)
            self.assertEqual(len(restarted.catalog(user_a())), 1)
            self.assertIn("Body", restarted.read(user_a(), name)["content"])

            # Cross-account read persists too.
            self.assertIn("Body", restarted.read(user_b(), name)["content"])

    def test_catalog_does_not_fetch_content_documents(self):
        # Slash search/catalog must only read the shared index document; it
        # must never fetch (and parse) per-skill content documents.
        with tempfile.TemporaryDirectory() as tmp:
            store = TrackingStore(AssistantStore(tmp))
            skills = SharedSkills(store)
            skills.install(user_a(), "first.md", GUIDE.encode("utf-8"))
            skills.install(user_a(), "second.md", GUIDE.replace("my-first-skill", "other").encode("utf-8"))
            store.get_calls.clear()

            catalog = skills.catalog(user_a())
            self.assertEqual(len(catalog), 2)
            fetched_keys = {key for _ns, key in store.get_calls}
            self.assertEqual(fetched_keys, {INDEX_KEY})
            self.assertFalse(any(key.count(":") >= 1 for key in fetched_keys))

    def test_namespace_metadata_and_content_atomic_persistence(self):
        # The shared store uses a single global index key plus one content key
        # per skill, persisted together atomically through put_documents.
        store = FakeStore()
        skills = SharedSkills(store)
        result = skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
        name = result["name"]

        index = store.get_document(NAMESPACE, INDEX_KEY)
        self.assertIsInstance(index, dict)
        self.assertIn(name, index["installed"])
        skill = index["installed"][name]
        self.assertEqual(skill["name"], name)
        self.assertIn("creator_hash", skill)
        self.assertIn("creator_name", skill)
        self.assertIn("installed_at", skill)
        self.assertEqual(skill["creator_name"], "Alice")

        content = store.get_document(NAMESPACE, INDEX_KEY + ":" + name)
        self.assertIsInstance(content, dict)
        self.assertIn("SKILL.md", content["contents"])

    def test_public_metadata_does_not_expose_owner_or_content(self):
        result = self.skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
        name = result["name"]
        self.assertNotIn("source_identity", result)
        self.assertNotIn("creator_hash", result)
        entry = self.skills.catalog(user_b())[0]
        self.assertNotIn("source_identity", entry)
        self.assertNotIn("creator_hash", entry)
        self.assertNotIn("content", entry)
        out = self.skills.read(user_b(), name)
        self.assertNotIn("source_identity", out)

    def test_remove_empties_content_document(self):
        # remove must atomically replace the content document with an empty
        # tombstone so stored bytes are cleared and the skill is inaccessible.
        store = FakeStore()
        skills = SharedSkills(store)
        result = skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
        name = result["name"]

        content_key = INDEX_KEY + ":" + name
        self.assertIn(name, skills.catalog(user_a())[0]["name"])

        skills.remove(user_a(), name)
        content = store.get_document(NAMESPACE, content_key)
        self.assertIsInstance(content, dict)
        self.assertEqual(content.get("contents"), {})
        self.assertTrue(content.get("removed"))

    def test_concurrent_installs_on_same_store_both_persist(self):
        # Two independent SharedSkills facades share the store's lock, so both
        # installs land exactly once (no lost update) and survive a restart.
        second_guide = GUIDE.replace("name: my-first-skill", "name: second-skill")
        store = FakeStore()
        barrier = threading.Barrier(2)
        results = {}

        def worker(label, content, filename):
            skills = SharedSkills(store)  # fresh facade per request
            barrier.wait()
            results[label] = skills.install(user_a(), filename, content.encode("utf-8"))

        t1 = threading.Thread(target=worker, args=("a", GUIDE, "first.md"))
        t2 = threading.Thread(target=worker, args=("b", second_guide, "second.md"))
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)
        self.assertFalse(t1.is_alive())
        self.assertFalse(t2.is_alive())

        restarted = SharedSkills(store)
        catalog = restarted.catalog(user_a())
        names = {entry["name"] for entry in catalog}
        self.assertEqual(len(catalog), 2)
        self.assertIn(results["a"]["name"], names)
        self.assertIn(results["b"]["name"], names)

    def test_restart_preserves_creator_hash(self):
        # Creator identity must survive a fresh facade over a real store.
        with tempfile.TemporaryDirectory() as tmp:
            store = AssistantStore(tmp)
            skills = SharedSkills(store)
            skills.install(user_a(), "my-first-skill.md", GUIDE.encode("utf-8"))
            restarted = SharedSkills(store)
            name = restarted.catalog(user_a())[0]["name"]
            # user_b cannot remove even after restart.
            with self.assertRaises(AssistantError) as failure:
                restarted.remove(user_b(), name)
            self.assertEqual(failure.exception.status, 403)
            self.assertEqual(restarted.remove(user_a(), name)["removed"], True)

    # --------------------------------------------- line endings / BOM / frontmatter

    def test_windows_crlf_and_bom_single_markdown(self):
        md = GUIDE.replace("---\n", "---\r\n").replace("# Body\n", "# Body\r\n")
        raw = b"\xef\xbb\xbf" + md.encode("utf-8")
        result = self.skills.install(user_a(), "win-skill-01.md", raw)
        self.assertFalse(result["duplicate"])
        name = result["name"]
        out = self.skills.read(user_a(), name)
        self.assertIn("Body", out["content"])
        self.assertEqual(out["name"], name)

    def test_windows_crlf_and_bom_zip_accepted(self):
        md = ("\ufeff---\r\nname: win-zip-skill\r\ndescription: windows zip.\r\n---\r\n\r\n"
              "# Zip Body\r\n\r\nSome windows content.\r\n")
        ref = "line1\r\nline2\r\n"
        raw = make_zip([
            ("win-skill/SKILL.md", md),
            ("win-skill/references/guide.md", ref),
        ])
        result = self.skills.install(user_a(), "win-skill.zip", raw)
        self.assertFalse(result["duplicate"])
        name = result["name"]
        out = self.skills.read(user_a(), name, reference="references/guide.md")
        self.assertIn("line1", out["content"])
        self.assertIn("line2", out["content"])

    # ----------------------------------------------------------------- zip modes

    def test_symlink_directory_rejected(self):
        # A symlink whose entry name carries a trailing slash must still be
        # detected by its mode and rejected (not silently treated as a dir).
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            link = zipfile.ZipInfo("my-skill/")  # looks like a directory path
            link.create_system = 3
            link.external_attr = (0o120000 | 0o777) << 16  # symlink mode
            archive.writestr(link, b"")
            archive.writestr("my-skill/SKILL.md", GUIDE)
        with self.assertRaises(AssistantError) as failure:
            self.skills.install(user_a(), "link-dir.zip", buffer.getvalue())
        self.assertEqual(failure.exception.status, 400)

    # ------------------------------------------------------------- display name

    def test_display_name_and_no_source_identity_exposure(self):
        md = GUIDE.replace(
            "name: my-first-skill",
            "name: my-first-skill\ndisplay_name: 我的第一个技能",
        )
        result = self.skills.install(user_a(), "display-source.md", md.encode("utf-8"))
        name = result["name"]
        self.assertEqual(result["display_name"], "我的第一个技能")
        self.assertNotIn("source_identity", result)

        catalog = self.skills.catalog(user_a())
        self.assertEqual(catalog[0]["display_name"], "我的第一个技能")
        self.assertNotIn("source_identity", catalog[0])

        out = self.skills.read(user_a(), name)
        self.assertEqual(out["display_name"], "我的第一个技能")
        self.assertNotIn("source_identity", out)

    # ------------------------------------------------------------- forbidden

    def test_forbidden_skills_rejected(self):
        name_md = GUIDE.replace("name: my-first-skill", "name: zhinav-point-data")
        source_zip = make_zip([("zhinav-point-data/SKILL.md", GUIDE)])
        chinese_zip = make_zip([("告警描述核查/SKILL.md", GUIDE)])
        cases = [
            ("name-md", "name-md.md", name_md.encode("utf-8")),
            ("source-zip", "source.zip", source_zip),
            ("chinese-source-zip", "chinese.zip", chinese_zip),
        ]
        for label, filename, payload in cases:
            with self.subTest(label=label), self.assertRaises(AssistantError) as failure:
                self.skills.install(user_a(), filename, payload)
            self.assertEqual(failure.exception.status, 400)
        self.assertEqual(self.skills.catalog(user_a()), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
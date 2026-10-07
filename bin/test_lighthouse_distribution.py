"""Isolated unit tests for the lighthouse runtime distribution packaging/install path.

Only ``lighthouse_distribution`` and the tiny ``lighthouse_runtime`` helpers are
exercised.  There are no production cloud calls, no real Feishu token handling,
no Node subprocesses and no business secrets.  Every network behaviour is mocked
with a recording fake client and every filesystem heavy path uses an in-memory
temp tree built from fake version metadata.

``make_fake_runtime``/``create_archive`` build a genuine (small) zip + part files;
``create_archive`` returns an *unpublished* spec, so tests that feed the published
installer add valid alphanumeric ``record_id``/``file_token`` via ``published_spec``.
``FakeMirror`` / ``FakeFeishuClient`` replace the network mirror so the installer
and HTTP adapter are exercised without downloading anything.
"""

from __future__ import annotations

import hashlib
import json
import stat
import sys
import tempfile
import threading
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch
import unittest
import zipfile

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

from lan_bitable_template_portal import lighthouse_distribution as dist
from lan_bitable_template_portal import lighthouse_runtime as lrt
from lan_bitable_template_portal.lighthouse_ai import AssistantError


# ---------------------------------------------------------------------------
# Constants shared with the module under test.
# ---------------------------------------------------------------------------
BASE = dist.BASE
TABLE = dist.TABLE
API = dist.API
PIN = lrt.PIN


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------
def _write(path: Path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        path.write_bytes(content)
    elif isinstance(content, (dict, list)):
        path.write_text(json.dumps(content, ensure_ascii=False), encoding="utf-8")
    else:
        path.write_text(content, encoding="utf-8")


def make_fake_runtime(root: Path) -> Path:
    """Create a minimal, pinned runtime tree that ``create_archive`` accepts."""
    node_dir = root / ("node-v" + PIN["node_version"] + "-win-x64")
    _write(root / "runtime-ready.json", {
        "node_version": PIN["node_version"],
        "openclaw_version": PIN["openclaw_version"],
        "node_sha256": "0" * 64,
        "prepared_at": 1,
    })
    _write(root / "package.json", {"name": "fake", "version": "1.0.0"})
    _write(root / "package-lock.json", {"lockfileVersion": 3})
    _write(node_dir / "node.exe", b"MZ-fake-node")
    _write(node_dir / "LICENSE", "fake license")
    _write(root / "node_modules/openclaw/package.json", {
        "name": "openclaw", "version": PIN["openclaw_version"]})
    _write(root / "node_modules/openclaw/openclaw.mjs", "export default {}")
    _write(root / "node_modules/openai/index.js",
           "module.exports = { create }")
    _write(root / "node_modules/@anthropic-ai/claude-agent-sdk-win32-x64/claude.exe",
           b"MZ-fake-claude")
    _write(root / "node_modules/something/debug.pdb", "debug symbols")
    _write(root / "account/secret.txt", "never-bundle")
    _write(root / "logs/debug.log", "never-bundle")
    _write(root / "keys/private.key", "never-bundle")
    _write(root / ".cache/entry", "never-bundle")
    return root


def make_tiny_package(folder: Path) -> dict:
    """Build a real tiny zip via ``create_archive`` and return its spec."""
    runtime = make_fake_runtime(folder / "runtime")
    output = folder / "lighthouse_openclaw.zip"
    return dist.create_archive(runtime, output)


def published_spec(folder: Path) -> dict:
    """``create_archive`` spec adapted for the published installer path."""
    spec = make_tiny_package(folder)
    for index, part in enumerate(spec["parts"], 1):
        part["record_id"] = "rec%03d" % index
        part["file_token"] = "file%03d" % index
    return spec


def make_single_file_zip(path: Path, name: str, content=b"payload"):
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(name, content)
    return path


def write_spec_file(folder: Path, spec: dict) -> Path:
    spec_file = folder / "distribution.json"
    spec_file.write_text(json.dumps(spec), encoding="utf-8")
    return spec_file


class FakeMirror:
    """Writes part data from a local parts folder (no network)."""

    def __init__(self, parts_dir: Path, *, do_write=True):
        self.parts_dir = Path(parts_dir)
        self.downloaded: list[int] = []
        self.do_write = do_write

    def download_part(self, part, target):
        self.downloaded.append(part["index"])
        source = self.parts_dir / part["name"]
        if self.do_write:
            Path(target).write_bytes(source.read_bytes())

    def close(self):
        pass


class _RecordingLock:
    """threading.Lock stand-in that records how many times it is acquired."""

    def __init__(self):
        self.inner = threading.Lock()
        self.acquires = 0

    def acquire(self, *args, **kwargs):
        acquired = self.inner.acquire(*args, **kwargs)
        if acquired:
            self.acquires += 1
        return acquired

    def release(self):
        self.inner.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc_info):
        self.release()
        return False


class FakeFeishuClient:
    """Recording fake that swaps the real Feishu HTTP adapter."""

    def __init__(self, fields=None, *, record_with_token=None):
        self.calls = []
        self.fields = list(fields or [])
        self.records = {}
        self.next_id = 0
        self.uploaded = []
        self._record_with_token = record_with_token

    def request_json(self, method, url, headers=None, **kwargs):
        self.calls.append(("json", method, url, dict(kwargs)))
        if url == API + "/fields":
            if method == "GET":
                return {"code": 0, "data": {"items": self.fields, "has_more": False}}
            if method == "POST":
                created = dict(kwargs["json_payload"])
                self.fields.append({"field_name": created["field_name"],
                                    "type": created["type"]})
                return {"code": 0, "data": {}}
        if url == API + "/records":
            if method == "POST":
                rec = {
                    "record_id": "rec%d" % self.next_id,
                    "fields": dict(kwargs["json_payload"]["fields"]),
                }
                self.next_id += 1
                self.records[rec["record_id"]] = rec
                return {"code": 0, "data": {"record": rec}}
        if url.startswith(API + "/records/"):
            rid = url[len(API + "/records/"):]
            if method == "GET":
                record = dict(self.records.get(rid, {}))
                if self._record_with_token and not record.get("fields", {}).get("安装包"):
                    record.setdefault("fields", {})["安装包"] = [
                        {"file_token": self._record_with_token}]
                return {"code": 0, "data": {"record": record}}
        raise AssertionError("unexpected request_json: %s %s %s" % (method, url, kwargs))

    def request_file_json(self, method, url, headers=None, file_path=None,
                          file_name=None, data=None, **kwargs):
        self.calls.append(("file", method, url, file_name, data))
        if method == "POST" and url == "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all":
            return {"code": 0, "data": {"file_token": "file_" + (file_name or "")}}
        raise AssertionError("unexpected upload: %s %s" % (method, url))

    def close(self):
        pass


class _FakeRespCtx:
    def __init__(self, response):
        self.response = response

    def __enter__(self):
        return self.response

    def __exit__(self, *exc_info):
        return False


class _FakeHttpxClient:
    """Stand-in for ``httpx.Client`` that replays a fixed sequence of responses."""

    def __init__(self, responses):
        self.responses = iter(responses)
        self.sent = []  # [(url, headers), ...]
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.closed = True
        return False

    def stream(self, method, url, headers=None, **kwargs):
        self.sent.append((url, headers))
        return _FakeRespCtx(next(self.responses))

    def close(self):
        pass


def all_fields():
    return [{"field_name": name, "type": kind} for name, kind in dist.FIELDS.items()]


def fake_runtime_files_ok(root=None):
    marker = Path(root) / "runtime-ready.json"
    if not marker.is_file():
        raise AssistantError("missing", 503)
    return str(Path(root) / "node.exe"), str(Path(root) / "openclaw.mjs")


# ---------------------------------------------------------------------------
# 1. Verified zip extraction: unsafe members are rejected before extraction.
# ---------------------------------------------------------------------------
class ZipExtractionSafetyTests(unittest.TestCase):
    def _expect_rejected(self, filename, **kw):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            archive = base / "bundle.zip"
            dest = base / "dest"
            dest.mkdir()
            content = kw.get("content", b"ok")
            external_attr = kw.get("external_attr", 0)
            digest_alter = kw.get("digest_alter", "")
            with zipfile.ZipFile(archive, "w") as zf:
                info = zipfile.ZipInfo(filename)
                info.external_attr = external_attr
                zf.writestr(info, content)
            digest = dist.checksum(archive)
            if digest_alter:
                digest = hashlib.sha256(b"tampered" + digest.encode()).hexdigest()
            with self.assertRaises(AssistantError):
                dist.extract_verified(archive, dest, digest)
            self.assertEqual(list(dest.iterdir()), [], "no extraction side-effect")

    def test_traversal_and_backslash_rejected(self):
        self._expect_rejected("../escape.txt")
        self._expect_rejected("..\\escape.txt")

    def test_absolute_and_drive_colon_rejected(self):
        self._expect_rejected(r"C:\Windows\system32\evil")
        self._expect_rejected("C:/Windows/evil")
        self._expect_rejected("/etc/passwd")

    def test_dot_and_double_slash_members_rejected(self):
        self._expect_rejected(".")
        self._expect_rejected("./file")
        self._expect_rejected("dir//file")

    def test_reserved_names_rejected(self):
        self._expect_rejected("CON")
        self._expect_rejected("NUL.txt")
        self._expect_rejected("dir/COM5/x")
        self._expect_rejected("LPT9")

    def test_trailing_space_and_dot_rejected(self):
        self._expect_rejected("file.")
        self._expect_rejected("file ")

    def test_case_collision_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            archive = base / "bundle.zip"
            dest = base / "dest"
            dest.mkdir()
            with zipfile.ZipFile(archive, "w") as zf:
                zf.writestr("A.txt", b"a")
                zf.writestr("a.txt", b"b")
            digest = dist.checksum(archive)
            with self.assertRaises(AssistantError):
                dist.extract_verified(archive, dest, digest)
            self.assertEqual(list(dest.iterdir()), [])

    def test_symlink_member_rejected(self):
        self._expect_rejected("link", external_attr=(0o120777 << 16))

    def test_zip_hash_mismatch_rejected(self):
        self._expect_rejected("ok.txt", digest_alter="x")

    def test_oversize_rejected_before_extract(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            archive = base / "bundle.zip"
            dest = base / "dest"
            dest.mkdir()
            make_single_file_zip(archive, "big.bin", b"small")
            digest = dist.checksum(archive)
            real_infolist = zipfile.ZipFile.infolist

            def inflated(self_):
                members = real_infolist(self_)
                for m in members:
                    m.file_size = 3 * 1024 ** 3 + 1
                return members

            with patch.object(zipfile.ZipFile, "infolist", inflated):
                with self.assertRaises(AssistantError):
                    dist.extract_verified(archive, dest, digest)
            self.assertEqual(list(dest.iterdir()), [], "oversize must not extract")

    def test_valid_tiny_bundle_extracts(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            package = folder / "runtime"
            make_fake_runtime(package)
            output = folder / "lighthouse_openclaw.zip"
            spec = dist.create_archive(package, output)
            dest = folder / "installed"
            dest.mkdir()
            dist.extract_verified(output, dest, spec["sha256"])
            self.assertTrue((dest / "runtime-ready.json").is_file())
            self.assertTrue(
                (dest / ("node-v" + PIN["node_version"] + "-win-x64/node.exe")).is_file())


# ---------------------------------------------------------------------------
# 2. Archive content: Node pin + node_modules/metadata only.
# ---------------------------------------------------------------------------
class ArchiveContentTests(unittest.TestCase):
    def test_bundle_keeps_runtime_material_and_omits_claude_pdb_accounts(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            runtime = folder / "runtime"
            make_fake_runtime(runtime)
            make_tiny_package(folder)
            archive = folder / "lighthouse_openclaw.zip"
            with zipfile.ZipFile(archive) as zf:
                names = set(zf.namelist())
            node_basename = "node-v" + PIN["node_version"] + "-win-x64"
            self.assertIn("package.json", names)
            self.assertIn("package-lock.json", names)
            self.assertIn("runtime-ready.json", names)
            self.assertIn(node_basename + "/node.exe", names)
            self.assertIn(node_basename + "/LICENSE", names)
            self.assertIn("node_modules/openclaw/package.json", names)
            self.assertIn("node_modules/openclaw/openclaw.mjs", names)
            self.assertIn("node_modules/openai/index.js", names)
            self.assertNotIn(
                "node_modules/@anthropic-ai/claude-agent-sdk-win32-x64/claude.exe", names)
            self.assertTrue(not any(name.endswith(".pdb") for name in names), names)
            for rel in ("account/secret.txt", "logs/debug.log",
                        "keys/private.key", ".cache/entry"):
                self.assertNotIn(rel, names)
            allowed_top = {"package.json", "package-lock.json", "runtime-ready.json",
                           node_basename + "/node.exe", node_basename + "/LICENSE"}
            for name in names:
                self.assertTrue(
                    name.startswith("node_modules/") or name in allowed_top, name)


# ---------------------------------------------------------------------------
# 3. Mirror HTTP isolation: exact designated BASE/TABLE, no DELETE, no other URL.
# ---------------------------------------------------------------------------
class MirrorHttpIsolationTests(unittest.TestCase):
    def test_ensure_fields_only_targets_designated_app_table(self):
        client = FakeFeishuClient(fields=all_fields())
        mirror = dist.FeishuRuntimeMirror(
            client=client, headers={"Authorization": "Bearer t"})
        mirror.ensure_fields()
        mirror.close()
        self.assertTrue(client.calls, client.calls)
        for kind, method, url, _ in client.calls:
            self.assertEqual(kind, "json")
            self.assertTrue(url.startswith(API),
                            "must only use the designated mirror URL: %s" % url)
            self.assertIn(method, {"GET", "POST"})
            self.assertNotEqual(method, "DELETE")

    def test_ensure_fields_creates_missing_without_delete_or_other_app(self):
        client = FakeFeishuClient(fields=all_fields()[:2])
        mirror = dist.FeishuRuntimeMirror(
            client=client, headers={"Authorization": "Bearer t"})
        mirror.ensure_fields()
        mirror.close()
        methods = [m for _, m, _, _ in client.calls]
        self.assertNotIn("DELETE", methods)
        for _, _, url, _ in client.calls:
            self.assertTrue(url.startswith(API), url)
            self.assertNotIn("apps/other", url)
        post_fields = [c for c in client.calls
                       if c[0] == "json" and c[1] == "POST" and c[2] == API + "/fields"]
        self.assertEqual(len(post_fields), len(dist.FIELDS) - 2)


# ---------------------------------------------------------------------------
# 4. Publish idempotency: stable client UUID, retry existing token, no re-create.
# ---------------------------------------------------------------------------
class PublishIdempotencyTests(unittest.TestCase):
    def _run(self, folder, journal, spec, client=None):
        if client is None:
            client = FakeFeishuClient(fields=all_fields())
        mirror = dist.FeishuRuntimeMirror(
            client=client, headers={"Authorization": "Bearer t"})
        result = mirror.publish(spec, str(folder), str(journal))
        mirror.close()
        return client, result

    def test_publish_retry_reuses_token_and_does_not_recreate_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            spec = make_tiny_package(folder)
            journal = folder / "upload-journal.json"

            client1, _ = self._run(folder, journal, spec)
            first_uploads = [c for c in client1.calls if c[0] == "file"]
            first_posts = [c for c in client1.calls
                           if c[0] == "json" and c[1] == "POST" and c[2] == API + "/records"]
            self.assertTrue(first_uploads, first_uploads)
            self.assertEqual(len(first_posts), len(spec["parts"]))

            state = json.loads(journal.read_text(encoding="utf-8"))
            first_client_token = state[spec["sha256"] + ":1"]["client_token"]
            self.assertTrue(first_client_token)

            first_calls = list(client1.calls)
            client2, _ = self._run(folder, journal, spec, client=client1)
            retry_calls = client2.calls[len(first_calls):]
            self.assertEqual([c for c in retry_calls if c[0] == "file"], [],
                             "already uploaded part must not re-upload")
            self.assertEqual([c for c in retry_calls
                              if c[0] == "json" and c[1] == "POST" and c[2] == API + "/records"],
                             [], "known record must not be re-created")
            second_gets = [c for c in retry_calls
                           if c[0] == "json" and c[1] == "GET"
                           and c[2].startswith(API + "/records/")]
            self.assertEqual(len(second_gets), len(spec["parts"]))

            state2 = json.loads(journal.read_text(encoding="utf-8"))
            self.assertEqual(state2[spec["sha256"] + ":1"]["client_token"], first_client_token,
                             "client UUID must be stable across retries")
            self.assertTrue(state2[spec["sha256"] + ":1"]["record_id"])
            self.assertTrue(state2[spec["sha256"] + ":1"]["file_token"])

    def test_publish_isolation_no_delete_or_external_upload_url(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            spec = make_tiny_package(folder)
            journal = folder / "upload-journal.json"
            client, _ = self._run(folder, journal, spec)
            for call in client.calls:
                if call[0] == "file":
                    self.assertEqual(
                        call[2],
                        "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all")
            self.assertNotIn("DELETE", [c[1] for c in client.calls])


# ---------------------------------------------------------------------------
# 5. Install flow: published manifest, shared cache archive, parts cleaned.
# ---------------------------------------------------------------------------
class InstallFlowTests(unittest.TestCase):
    def _make_spec(self, folder):
        spec = published_spec(folder)
        spec_file = write_spec_file(folder, spec)
        return spec, spec_file

    def _probably_ok_install(self, folder, base):
        spec, spec_file = self._make_spec(folder)
        dest = base / "lighthouse_openclaw"
        dest.mkdir(parents=True)
        with patch.object(dist, "SPEC", spec_file), \
             patch.object(lrt, "runtime_files", side_effect=fake_runtime_files_ok):
            result = dist.install_runtime(
                dest, mirror_factory=lambda: FakeMirror(folder))
        return spec, dest

    def test_install_verifies_and_extracts_and_keeps_only_verified_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            base = folder / "app"
            spec, dest = self._probably_ok_install(folder, base)
            self.assertEqual(dest, base / "lighthouse_openclaw")
            self.assertTrue((dest / "runtime-ready.json").is_file())
            cache = base / ".lighthouse_download"
            archives = list(cache.glob(spec["sha256"] + ".zip"))
            self.assertEqual(len(archives), 1, "verified archive retained once")
            parts = list(cache.glob(spec["sha256"] + ".00*"))
            self.assertEqual(parts, [], "temporary parts must be cleaned on success")

    def test_install_invokes_runtime_files_with_explicit_resolved_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            spec, spec_file = self._make_spec(folder)
            base = folder / "app"
            dest = base / "lighthouse_openclaw"
            dest.mkdir(parents=True)
            roots_seen = []

            def recording_runtime_files(root=None):
                roots_seen.append(Path(root).resolve())
                return fake_runtime_files_ok(root)

            with patch.object(dist, "SPEC", spec_file), \
                 patch.object(lrt, "runtime_files", side_effect=recording_runtime_files):
                dist.install_runtime(dest, mirror_factory=lambda: FakeMirror(folder))
            self.assertGreaterEqual(len(roots_seen), 1)
            self.assertTrue(all(Path(r).is_absolute() for r in roots_seen))

    def test_install_serializes_under_global_install_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            spec, spec_file = self._make_spec(folder)
            base = folder / "app"
            dest = base / "lighthouse_openclaw"
            dest.mkdir(parents=True)
            lock = _RecordingLock()
            with patch.object(dist, "INSTALL_LOCK", lock), \
                 patch.object(dist, "SPEC", spec_file), \
                 patch.object(lrt, "runtime_files", side_effect=fake_runtime_files_ok):
                dist.install_runtime(dest, mirror_factory=lambda: FakeMirror(folder))
            self.assertGreaterEqual(lock.acquires, 1,
                                    "install must take the global INSTALL_LOCK")

    def test_concurrent_install_same_version_shares_cache_without_corruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            spec, spec_file = self._make_spec(folder)
            base = folder / "app"
            base.mkdir(parents=True)
            dest1, dest2 = base / "d1", base / "d2"
            for d in (dest1, dest2):
                d.mkdir()
            results = {}

            def install_into(dest, mirror):
                try:
                    results[dest] = dist.install_runtime(
                        dest, mirror_factory=lambda: mirror)
                except Exception as exc:  # noqa: BLE001
                    results[dest] = exc

            mirror1, mirror2 = FakeMirror(folder), FakeMirror(folder)
            with patch.object(dist, "SPEC", spec_file), \
                 patch.object(lrt, "runtime_files", side_effect=fake_runtime_files_ok):
                t1 = threading.Thread(target=install_into, args=(dest1, mirror1))
                t2 = threading.Thread(target=install_into, args=(dest2, mirror2))
                t1.start(); t2.start(); t1.join(); t2.join()
            for dest in (dest1, dest2):
                self.assertEqual(results[dest], dest, results)
            self.assertTrue((dest1 / "runtime-ready.json").is_file())
            self.assertTrue((dest2 / "runtime-ready.json").is_file())
            cache = base / ".lighthouse_download"
            self.assertGreaterEqual(len(list(cache.glob(spec["sha256"] + ".zip"))), 1)
            self.assertTrue(
                (len(mirror1.downloaded) == 0 or len(mirror2.downloaded) == 0),
                "second installer must reuse the cache, got %s / %s"
                % (mirror1.downloaded, mirror2.downloaded))


# ---------------------------------------------------------------------------
# 6. Manifest validation: bad digests/types/IDs/indexes/sizes/sum/base/pin
#    are rejected with AssistantError before any cache/network/file write.
# ---------------------------------------------------------------------------
class ManifestValidationTests(unittest.TestCase):
    def _assert_clean_rejection(self, folder, spec):
        base = folder / "app"
        dest = base / "lighthouse_openclaw"
        dest.mkdir(parents=True, exist_ok=True)
        spec_file = write_spec_file(folder, spec)
        network_calls = []

        def mirror_factory():
            network_calls.append(True)
            raise AssertionError("network must not be reached before validation")

        with patch.object(dist, "SPEC", spec_file), \
             patch.object(lrt, "runtime_files", side_effect=fake_runtime_files_ok):
            with self.assertRaises(AssistantError):
                dist.install_runtime(dest, mirror_factory=mirror_factory)
        self.assertEqual(network_calls, [])
        self.assertFalse((base / ".lighthouse_download").exists(),
                         "cache must not be created before validation")

    def test_bad_sha_digest_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            base = published_spec(folder)
            for sha in ("../../escape", "nothex" * 8, {"bad": True}, 12345):
                spec = dict(base)
                spec["sha256"] = sha
                with self.subTest(sha=repr(sha)):
                    self._assert_clean_rejection(folder, spec)

    def test_wrong_size_types_and_bool_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            base = published_spec(folder)
            for size in ("not-an-int", 0, True):
                spec = dict(base)
                spec["size"] = size
                with self.subTest(size=repr(size)):
                    self._assert_clean_rejection(folder, spec)
            spec = dict(base)
            spec["parts"][0]["size"] = True
            self._assert_clean_rejection(folder, spec)
            spec = dict(base)
            spec["parts"][0]["size"] = "x"
            self._assert_clean_rejection(folder, spec)

    def test_bad_part_index_and_unknown_ids_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            base = published_spec(folder)
            spec = dict(base)
            spec["parts"][0]["index"] = 2
            self._assert_clean_rejection(folder, spec)
            spec = dict(base)
            spec["parts"][0]["index"] = True
            self._assert_clean_rejection(folder, spec)
            for bad_id in ("", "has space", "bad!", "x" * 201, 7):
                spec = dict(base)
                spec["parts"][0]["record_id"] = bad_id
                self._assert_clean_rejection(folder, spec)
                spec = dict(base)
                spec["parts"][0]["file_token"] = bad_id
                self._assert_clean_rejection(folder, spec)

    def test_size_sum_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            base = published_spec(folder)
            spec = dict(base)
            spec["size"] = spec["size"] + 1
            self._assert_clean_rejection(folder, spec)

    def test_base_table_pin_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            base = published_spec(folder)
            for key in ("base", "table", "version", "node_version"):
                spec = dict(base)
                spec[key] = "wrong-value"
                with self.subTest(field=key):
                    self._assert_clean_rejection(folder, spec)


# ---------------------------------------------------------------------------
# 7. Failed staging: cleaned up and old destination preserved.
# ---------------------------------------------------------------------------
class StagingCleanupTests(unittest.TestCase):
    def test_failed_install_cleans_staging_and_preserves_old_dest(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            spec, spec_file = InstallFlowTests()._make_spec(folder)
            base = folder / "app"
            dest = base / "lighthouse_openclaw"
            dest.mkdir(parents=True)
            _write(dest / "keep.txt", "old-content")

            def failing_runtime_files(root=None):
                raise AssistantError("助手运行环境校验失败", 503)

            with patch.object(dist, "SPEC", spec_file), \
                 patch.object(lrt, "runtime_files", side_effect=failing_runtime_files):
                with self.assertRaises(AssistantError):
                    dist.install_runtime(dest, mirror_factory=lambda: FakeMirror(folder))
            self.assertEqual(list(base.glob(dest.name + ".staging-*")), [],
                             "failed install must clean its staging directory")
            self.assertTrue((dest / "keep.txt").is_file(),
                            "old destination must be preserved on failure")
            self.assertEqual((dest / "keep.txt").read_text(encoding="utf-8"), "old-content")


# ---------------------------------------------------------------------------
# 8. Installed Node hash validation (mocks only - no Node subprocess).
# ---------------------------------------------------------------------------
class NodeHashValidationTests(unittest.TestCase):
    def test_runtime_files_rejects_wrong_node_executable_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            node_dir = root / ("node-v" + PIN["node_version"] + "-win-x64")
            _write(root / "runtime-ready.json", {
                "node_version": PIN["node_version"],
                "openclaw_version": PIN["openclaw_version"],
                "node_sha256": "0" * 64,
            })
            _write(node_dir / "node.exe", b"not-the-real-node-binary")
            _write(root / "node_modules/openclaw/openclaw.mjs", "export default {}")
            _write(root / "node_modules/openclaw/package.json", {
                "name": "openclaw", "version": PIN["openclaw_version"],
            })
            with self.assertRaisesRegex(AssistantError, "运行环境校验失败|node_executable"):
                lrt.runtime_files(root)

    def test_runtime_files_allows_correct_hash(self):
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(lrt, "hashlib") as h:
            root = Path(tmp)
            node_dir = root / ("node-v" + PIN["node_version"] + "-win-x64")
            digest = MagicMock()
            digest.hexdigest.return_value = PIN["node_executable_sha256"]
            h.file_digest = lambda *args, **kwargs: digest
            _write(node_dir / "node.exe", b"x" * 16)
            _write(root / "runtime-ready.json", {
                "node_version": PIN["node_version"],
                "openclaw_version": PIN["openclaw_version"],
                "node_sha256": PIN["node_executable_sha256"],
            })
            _write(root / "node_modules/openclaw/openclaw.mjs", "export default {}")
            _write(root / "node_modules/openclaw/package.json", {
                "name": "openclaw", "version": PIN["openclaw_version"],
            })
            result = lrt.runtime_files(root)
            self.assertEqual(len(result), 2)


# ---------------------------------------------------------------------------
# 9. HTTP statuses and redirects in download_part.
# ---------------------------------------------------------------------------
class DownloadRedirectTests(unittest.TestCase):
    def _part(self):
        return {"record_id": "rec1", "file_token": "tok1", "size": 3,
                "sha256": hashlib.sha256(b"000").hexdigest()}

    def _mirror_with_http(self, responses):
        client = dist.FeishuRuntimeMirror(
            client=FakeFeishuClient(fields=all_fields(), record_with_token="tok1"),
            headers={"Authorization": "Bearer t"})
        fake_http = _FakeHttpxClient(responses)
        return client, fake_http

    def test_safe_cdn_redirect_allowed_without_forwarded_bearer(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "part.bin"
            resp302 = MagicMock()
            resp302.status_code = 302
            resp302.headers = {"location": "https://cdn.feishucdn.com/v1/tok1"}
            resp200 = MagicMock()
            resp200.status_code = 200
            resp200.headers = {}
            resp200.iter_bytes.return_value = iter([b"000"])
            mirror, fake_http = self._mirror_with_http([resp302, resp200])
            with patch("httpx.Client", return_value=fake_http):
                mirror.download_part(self._part(), target)
            self.assertEqual(target.read_bytes(), b"000")
            self.assertEqual(len(fake_http.sent), 2)
            self.assertEqual(
                fake_http.sent[0][0],
                "https://open.feishu.cn/open-apis/drive/v1/medias/tok1/download")
            self.assertEqual(fake_http.sent[1][0], "https://cdn.feishucdn.com/v1/tok1")
            self.assertIn("Authorization", fake_http.sent[0][1])
            self.assertNotIn("Authorization", fake_http.sent[1][1],
                             "CDN redirect must not receive the API bearer")

    def test_unsafe_redirect_writes_no_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "part.bin"
            resp = MagicMock()
            resp.status_code = 302
            resp.headers = {"location": "http://evil.example/skip"}  # http, wrong host
            mirror, fake_http = self._mirror_with_http([resp])
            with patch("httpx.Client", return_value=fake_http):
                with self.assertRaises(AssistantError):
                    mirror.download_part(self._part(), target)
            self.assertFalse(target.exists(), "unsafe redirect must not write a file")

    def test_missing_location_redirect_writes_no_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "part.bin"
            resp = MagicMock()
            resp.status_code = 302
            resp.headers = {}  # no Location header
            mirror, fake_http = self._mirror_with_http([resp])
            with patch("httpx.Client", return_value=fake_http):
                with self.assertRaises(AssistantError):
                    mirror.download_part(self._part(), target)
            self.assertFalse(target.exists(), "missing location must not write a file")

    def test_non_200_status_writes_no_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "part.bin"
            resp = MagicMock()
            resp.status_code = 503
            resp.headers = {}
            mirror, fake_http = self._mirror_with_http([resp])
            with patch("httpx.Client", return_value=fake_http):
                with self.assertRaises(AssistantError):
                    mirror.download_part(self._part(), target)
            self.assertFalse(target.exists(), "non-200 status must not write a file")


if __name__ == "__main__":
    unittest.main()
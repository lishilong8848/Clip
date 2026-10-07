"""Isolated packaging/import smoke contract for the independent assistant build.

These tests only inspect the ``SMOKE_IMPORT_MODULES`` contract declared in
``package_portable.py`` and prove the required modules can be imported in a
clean interpreter without starting resident services, touching business
databases, launching Node / Task Scheduler jobs, or contacting Feishu.

Guarantees upheld here (no real cloud / credentials / side effects):
  * only temporary data and mocks are used;
  * no credentials are printed;
  * no programs are launched, no packaging ``main`` is executed;
  * nothing is uploaded, patched, or written to the cloud.
"""
from __future__ import annotations

import importlib
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import zipfile

BIN = Path(__file__).resolve().parent
ROOT = BIN.parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import package_portable

# Modules that must be part of the packaging interpreter import smoke test.
REQUIRED_MODULES = (
    "openclaw_service.__main__",
    "openclaw_service.server",
    "openclaw_service.client",
    "openclaw_service.protocol",
    "openclaw_service.store",
    "openclaw_service.bridge",
    "openclaw_service.launcher",
    "openclaw_service.update",
    "openclaw_service.gateway_log",
    "openclaw_service.assistant.routes",
    "lan_bitable_template_portal.lighthouse_bridge",
)


def _guarded_bootstrap() -> str:
    """Build a subprocess bootstrap that imports the required modules while
    transparently recording any process / DB / scheduler / network activity."""
    module_list = "[" + ", ".join(repr(m) for m in REQUIRED_MODULES) + "]"
    bin_repr = repr(str(BIN))
    return (
        "import importlib\n"
        "import sys\n"
        "from unittest.mock import MagicMock\n"
        f"sys.path.insert(0, {bin_repr})\n"
        "import subprocess\n"
        "violations = []\n"
        "def guard(name):\n"
        "    def inner(*args, **_kwargs):\n"
        "        violations.append(name)\n"
        "        return MagicMock()\n"
        "    return inner\n"
        # Process / Node / external program launching.
        'for _n in ("run", "Popen", "call", "check_call", "check_output"):\n'
        '    setattr(subprocess, _n, guard("subprocess." + _n))\n'
        "import os\n"
        'for _n in ("system", "popen"):\n'
        "    if hasattr(os, _n):\n"
        '        setattr(os, _n, guard("os." + _n))\n'
        # Any database connection (including the business store DBs).
        "import sqlite3\n"
        'sqlite3.connect = guard("sqlite.connect")\n'
        # Windows Task Scheduler registration / COM activation.
        "try:\n"
        "    import win32com.client\n"
        '    win32com.client.Dispatch = guard("win32com.Dispatch")\n'
        "    import pythoncom\n"
        '    pythoncom.CoCreateInstance = guard("pythoncom.CoCreateInstance")\n'
        "except ImportError:\n"
        "    pass\n"
        # Real Feishu / any HTTP or raw socket traffic.
        "try:\n"
        "    import httpx\n"
        '    httpx.Client = guard("httpx.Client")\n'
        '    httpx.AsyncClient = guard("httpx.AsyncClient")\n'
        "except ImportError:\n"
        "    pass\n"
        "import socket\n"
        'socket.socket.connect = guard("socket.connect")\n'
        'socket.create_connection = guard("socket.create_connection")\n'
        f"mods = {module_list}\n"
        "for _m in mods:\n"
        "    importlib.import_module(_m)\n"
        "if violations:\n"
        '    sys.stderr.write("SIDE_EFFECTS=" + repr(violations))\n'
        "    raise SystemExit(3)\n"
        'sys.stdout.write("CLEAN")\n'
    )


class SmokeImportContractTests(unittest.TestCase):
    def test_required_modules_are_listed(self):
        listed = set(package_portable.SMOKE_IMPORT_MODULES)
        missing = [m for m in REQUIRED_MODULES if m not in listed]
        self.assertEqual(missing, [], "required smoke-import modules are missing from SMOKE_IMPORT_MODULES")

    def test_smoke_import_list_has_no_duplicates(self):
        modules = package_portable.SMOKE_IMPORT_MODULES
        duplicates = sorted({m for m in modules if modules.count(m) > 1})
        self.assertEqual(duplicates, [], "SMOKE_IMPORT_MODULES contains duplicate module names")

    def test_required_modules_import_without_side_effects(self):
        """Import all required modules in a fresh interpreter guarded so that any
        process / DB / Task-Scheduler / Feishu / socket activity is recorded."""
        result = subprocess.run(
            [sys.executable, "-B", "-c", _guarded_bootstrap()],
            capture_output=True,
            text=True,
            cwd=str(ROOT),
            timeout=180,
        )
        self.assertEqual(
            result.returncode,
            0,
            msg=f"isolated import failed\nstdout={result.stdout}\nstderr={result.stderr}",
        )
        self.assertEqual(result.stdout.strip(), "CLEAN")
        self.assertNotIn("SIDE_EFFECTS", result.stderr)
        self.assertNotIn("credentials", result.stderr.lower())

    def test_modules_are_importable_in_process(self):
        # Contained in-process correctness check that mirrors the packaged
        # interpreter; no constructors or service entry points are called.
        for module_name in REQUIRED_MODULES:
            with self.subTest(module=module_name):
                module = importlib.import_module(module_name)
                self.assertIsNotNone(module)
                self.assertTrue(
                    not module_name.endswith(("__main__",)) or hasattr(module, "main"),
                    f"{module_name} should expose a main entry point",
                )


class ResidentArtifactTests(unittest.TestCase):
    def test_real_source_selection_includes_service_and_excludes_owned_runtime_and_development(self):
        files = {path.relative_to(ROOT).as_posix() for path in package_portable._iter_project_files(ROOT, exclude_venv=True)}
        required = {'启动程序.bat', 'bin/refactored_main.py', 'bin/lan_bitable_template_portal/lighthouse_routes.py',
            'bin/lan_bitable_template_portal/lighthouse_bridge.py',
            'bin/openclaw_service/assistant/openclaw/skills/alert-tagging/references/rules.md',
            'bin/lan_bitable_template_portal/frontend/dist/assistant.html'}
        required.update('bin/' + name.replace('.', '/') + '.py' for name in REQUIRED_MODULES)
        required.update(path.as_posix() for path in package_portable.FORCE_PATCH_INCLUDE_FILES)
        self.assertEqual(required - files, set())
        self.assertNotIn('启动程序openclaw.bat', files)
        self.assertNotIn('启动程序openclaw.py', files)
        forbidden = ('bin/data/', 'bin/runtime/', 'bin/.venv/', '.deepcode/', '.codex/', 'bin/test_',
            'bin/tools/check_openclaw_', 'bin/tools/lighthouse_resident_acceptance.md')
        self.assertFalse(any(name.startswith(forbidden) for name in files))

    def test_real_patch_zip_has_all_service_files_hashes_and_approved_skills_without_local_state(self):
        with tempfile.TemporaryDirectory(prefix='resident-artifact-') as directory:
            root = Path(directory).resolve()
            build = root / 'build'
            build.mkdir()
            name = 'ClipFlow_V2_20261004_000000'
            destination = build / (name + '_patch_only')
            self.assertTrue(destination.resolve().is_relative_to(root))
            self.assertFalse(destination.exists())
            with patch.object(package_portable, 'BUILD_DIR', build):
                package_portable.build_patch(root / 'unused', None, name, target_patch_version=1)
                archive = package_portable._zip_patch_dir(destination)
            self.assertTrue(archive.resolve().is_relative_to(root))
            with zipfile.ZipFile(archive) as zipped:
                names = {item.filename.removeprefix(destination.name + '/') for item in zipped.infolist()}
                required = {path.relative_to(ROOT).as_posix() for path in (ROOT / 'bin/openclaw_service').rglob('*')
                    if path.is_file() and not package_portable._is_development_only_path(path, ROOT)
                    and '__pycache__' not in path.parts and path.suffix not in {'.pyc', '.pyo'}
                    and not path.relative_to(ROOT).is_relative_to(package_portable.IMPORTED_SKILLS_DIR)}
                required.update({'启动程序.bat', 'bin/refactored_main.py'})
                required.add((package_portable.IMPORTED_SKILLS_DIR.parent / 'workbuddy.zip').as_posix())
                self.assertEqual(required - names, set())
                rules = 'bin/openclaw_service/assistant/openclaw/skills/alert-tagging/references/rules.md'
                self.assertEqual(zipped.read(destination.name + '/' + rules), (ROOT / rules).read_bytes())
                self.assertNotIn('启动程序openclaw.bat', names)
                self.assertNotIn('启动程序openclaw.py', names)
                retirement = zipped.read(destination.name + '/patch_manifest.txt').decode('utf-8')
                self.assertIn('启动程序openclaw.bat', retirement)
                self.assertIn('启动程序openclaw.py', retirement)
                self.assertFalse(any(value.startswith(('bin/data/', 'bin/runtime/', 'bin/.venv/', '.deepcode/', '.codex/', 'bin/test_')) for value in names))
                prefix = destination.name + '/'
                metadata = json.loads(zipped.read(prefix + 'bin/patch_meta.json'))
                for relative, expected in metadata['file_sha256'].items():
                    self.assertEqual(hashlib.sha256(zipped.read(prefix + relative.replace('\\', '/'))).hexdigest(), expected)
            skills = destination / package_portable.IMPORTED_SKILLS_DIR.parent
            self.assertFalse((skills / 'workbuddy').exists())
            registry = ROOT / package_portable.IMPORTED_SKILLS_DIR.parent / 'workbuddy-registry.json'
            stamp = registry.stat()
            resources = package_portable._registered_skill_resources(str(registry.resolve()), stamp.st_mtime_ns, stamp.st_size)
            with zipfile.ZipFile(skills / 'workbuddy.zip') as bundled:
                self.assertEqual(set(bundled.namelist()), set(resources))
                for relative in resources:
                    self.assertEqual(bundled.read(relative), (registry.parent / relative).read_bytes())


if __name__ == "__main__":
    unittest.main()

"""Prepare the pinned, project-owned Windows agent runtime without global installs."""
import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import time
import urllib.request
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "bin/openclaw_service/assistant/openclaw/runtime.json"


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def prepare(destination):
    if platform.system() != "Windows" or platform.machine().lower() not in {"amd64", "x86_64"}:
        raise RuntimeError("This pinned runtime targets Windows x64.")
    settings = json.loads(MANIFEST.read_text(encoding="utf-8"))
    destination = Path(destination).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    node_dir = destination / ("node-v" + settings["node_version"] + "-win-x64")
    executable = node_dir / "node.exe"
    archive = destination / "node.zip"
    if not executable.exists():
        if not archive.exists() or sha256(archive) != settings["node_sha256"]:
            temporary = destination / "node.zip.part"
            with urllib.request.urlopen(settings["node_archive"], timeout=60) as response, temporary.open("wb") as stream:
                shutil.copyfileobj(response, stream)
            if sha256(temporary) != settings["node_sha256"]:
                raise RuntimeError("Node archive checksum mismatch.")
            temporary.replace(archive)
        with zipfile.ZipFile(archive) as bundle:
            for member in bundle.infolist():
                target = (destination / member.filename).resolve()
                if not target.is_relative_to(node_dir) or member.filename.startswith(("/", "\\")):
                    raise RuntimeError("Unexpected path in Node archive.")
            bundle.extractall(destination)
    result = subprocess.run([str(executable), "--version"], capture_output=True, text=True, check=True)
    if result.stdout.strip() != "v" + settings["node_version"]:
        raise RuntimeError("Project Node version does not match the pinned manifest.")
    if sha256(executable) != settings["node_executable_sha256"]:
        raise RuntimeError("Project Node executable hash does not match the pinned manifest.")
    package = destination / "node_modules/openclaw/package.json"
    installed = json.loads(package.read_text(encoding="utf-8")) if package.exists() else {}
    lock = destination / "package-lock.json"
    locked = json.loads(lock.read_text(encoding="utf-8")) if lock.exists() else {}
    integrity = locked.get("packages", {}).get("node_modules/openclaw", {}).get("integrity")
    if installed.get("version") != settings["openclaw_version"] or integrity != settings["openclaw_integrity"]:
        environment = {**os.environ, "PATH": str(node_dir) + os.pathsep + os.environ.get("PATH", "")}
        subprocess.run([
            str(executable), str(node_dir / "node_modules/npm/bin/npm-cli.js"), "install", "--save-exact",
            "--omit=dev", "--no-audit", "--no-fund", "--registry=https://registry.npmjs.org",
            "--cache=" + str(destination / "npm-cache"), "openclaw@" + settings["openclaw_version"],
        ], cwd=destination, env=environment, check=True,
            creationflags=subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS)
        locked = json.loads(lock.read_text(encoding="utf-8"))
        if locked["packages"]["node_modules/openclaw"].get("integrity") != settings["openclaw_integrity"]:
            raise RuntimeError("Installed OpenClaw integrity does not match the pinned release.")
    ready = {"node_version": settings["node_version"], "openclaw_version": settings["openclaw_version"],
             "node_sha256": sha256(executable), "prepared_at": time.time()}
    (destination / "runtime-ready.json").write_text(json.dumps(ready, indent=2) + "\n", encoding="utf-8")
    print("[LighthouseRuntime] Ready: Node " + settings["node_version"] + ", OpenClaw " + settings["openclaw_version"])
    return destination


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, default=ROOT / "build_output/lighthouse_openclaw")
    prepare(parser.parse_args().destination)

# -*- coding: utf-8 -*-
import hashlib
import json
import re
import shutil
import zipfile
from pathlib import Path
from typing import Any

import requests


class RemotePatchUpdater:
    def __init__(self, app_root: Path, data_dir: Path, manifest_url: str):
        self.app_root = app_root
        self.data_dir = data_dir
        self.manifest_url = (manifest_url or "").strip()
        self.data_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _parse_build_stamp(build_id: str) -> int:
        text = (build_id or "").strip()
        if not text:
            return 0
        match = re.search(r"(\d{8})_(\d{6})", text)
        if match:
            return int(match.group(1) + match.group(2))
        match = re.search(r"(\d{14})", text)
        if match:
            return int(match.group(1))
        return 0

    def fetch_manifest(self, timeout: tuple[float, float] = (5.0, 20.0)) -> dict:
        if not self.manifest_url:
            return {}
        response = requests.get(self.manifest_url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return {}
        return payload

    def has_newer_patch(self, local_build_meta: dict, remote_manifest: dict) -> bool:
        if not remote_manifest:
            return False
        if not self.is_local_version_known(local_build_meta):
            # Development or legacy runtime without build_meta should not be
            # forced into remote update flow by default.
            return False
        local_major = self._safe_int(local_build_meta.get("major_version"), 1)
        local_patch = self._safe_int(local_build_meta.get("patch_version"), 0)
        remote_major = self._safe_int(
            remote_manifest.get("major_version"), local_major
        )
        remote_patch = remote_manifest.get("target_patch_version")
        if remote_patch is not None:
            remote_patch_num = self._safe_int(remote_patch, -1)
            if remote_major != local_major:
                return remote_major > local_major
            return remote_patch_num > local_patch

        local_stamp = self._parse_build_stamp(local_build_meta.get("build_id", ""))
        remote_build_id = (
            remote_manifest.get("target_version")
            or remote_manifest.get("version")
            or ""
        )
        remote_stamp = self._parse_build_stamp(remote_build_id)
        return remote_stamp > local_stamp

    @classmethod
    def is_local_version_known(cls, local_build_meta: dict) -> bool:
        if not isinstance(local_build_meta, dict) or not local_build_meta:
            return False
        major = local_build_meta.get("major_version")
        patch = local_build_meta.get("patch_version")
        build_id = (local_build_meta.get("build_id") or "").strip()
        has_major_patch = major is not None and patch is not None
        has_build_stamp = cls._parse_build_stamp(build_id) > 0
        return bool(has_major_patch or has_build_stamp)

    @staticmethod
    def is_ui_update(manifest: dict) -> bool:
        return bool(manifest.get("ui_changed") or manifest.get("restart_required"))

    def _sha256_file(self, path: Path) -> str:
        hasher = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _download_zip(self, manifest: dict) -> Path:
        zip_url = (manifest.get("zip_url") or "").strip()
        zip_name = (manifest.get("zip_name") or "").strip()
        if not zip_url:
            raise RuntimeError("remote manifest missing zip_url")
        if not zip_name:
            zip_name = Path(zip_url).name or "patch.zip"
        target_zip = self.data_dir / zip_name
        with requests.get(zip_url, stream=True, timeout=(5, 60)) as resp:
            resp.raise_for_status()
            with target_zip.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 512):
                    if not chunk:
                        continue
                    f.write(chunk)
        expected_sha256 = (manifest.get("zip_sha256") or "").strip().lower()
        if expected_sha256:
            actual_sha256 = self._sha256_file(target_zip).lower()
            if actual_sha256 != expected_sha256:
                target_zip.unlink(missing_ok=True)
                raise RuntimeError("zip sha256 mismatch")
        expected_size = self._safe_int(manifest.get("zip_size"), 0)
        if expected_size > 0:
            actual_size = target_zip.stat().st_size
            if actual_size != expected_size:
                target_zip.unlink(missing_ok=True)
                raise RuntimeError("zip size mismatch")
        return target_zip

    def _extract_patch_dir(self, zip_path: Path) -> Path:
        extract_root = self.data_dir / f".extract_{zip_path.stem}"
        if extract_root.exists():
            shutil.rmtree(extract_root, ignore_errors=True)
        extract_root.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_root)

        candidates = [
            p
            for p in extract_root.rglob("*")
            if p.is_dir() and p.name.endswith("_patch_only")
        ]
        if not candidates:
            shutil.rmtree(extract_root, ignore_errors=True)
            raise RuntimeError("zip does not contain *_patch_only directory")

        candidates.sort(key=lambda p: len(p.parts))
        selected = candidates[0]
        target_dir = self.app_root / selected.name
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        shutil.move(str(selected), str(target_dir))
        shutil.rmtree(extract_root, ignore_errors=True)
        return target_dir

    def prepare_remote_patch(self, manifest: dict) -> Path:
        zip_path = self._download_zip(manifest)
        try:
            return self._extract_patch_dir(zip_path)
        finally:
            zip_path.unlink(missing_ok=True)

    @staticmethod
    def load_local_build_meta(app_root: Path) -> dict:
        meta_file = app_root / "bin" / "build_meta.json"
        if not meta_file.exists():
            return {}
        try:
            return json.loads(meta_file.read_text(encoding="utf-8"))
        except Exception:
            return {}

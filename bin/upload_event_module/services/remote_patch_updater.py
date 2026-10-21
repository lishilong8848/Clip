# -*- coding: utf-8 -*-
import hashlib
import json
import logging
import os
import re
import shutil
import stat
import tempfile
import zipfile
from pathlib import Path, PureWindowsPath
from typing import Any


def update_check_diagnostic(exc: Exception) -> dict:
    category = getattr(exc, 'category', '')
    if not category:
        category = 'manifest' if isinstance(exc, ValueError) else 'network'
    if isinstance(exc, ValueError) and '同一补丁号' in str(exc):
        category = 'conflict'
    descriptions = {
        'auth': ('飞书鉴权失败', '请核对Qt设置中的飞书鉴权及应用是否启用。'),
        'token': ('飞书鉴权失败', '令牌刷新后仍未通过，请核对Qt飞书鉴权。'),
        'permission': ('无权读取更新表', '请为当前Qt飞书应用授权补丁多维表的读取权限。'),
        'rate_limit': ('飞书请求暂时受限', '请稍后点击检查更新；不会重复创建补丁。'),
        'manifest': ('发布清单校验失败', '请联系管理员核对发布清单完整性；当前程序仍可使用。'),
        'conflict': ('发布版本存在冲突', '同一补丁号的发布内容不一致，请管理员核对后重试。'),
        'remote': ('飞书服务暂未返回有效结果', '请稍后重试，持续出现时将诊断信息提供给管理员。'),
        'network': ('无法连接更新服务', '请检查此电脑的网络、代理及飞书连接，稍后重试。'),
    }
    title, recommendation = descriptions.get(category, descriptions['network'])
    code = getattr(exc, 'code', None)
    request_id = getattr(exc, 'request_id', '')
    detail = '失败阶段：读取飞书发布清单\n' + recommendation
    if type(code) is int:
        detail += '\n飞书错误码：' + str(code)
    if isinstance(request_id, str) and re.fullmatch(r'[A-Za-z0-9_-]{1,128}', request_id):
        detail += '\n请求编号：' + request_id
    return {'category': category, 'title': title, 'detail': detail,
            'retryable': category in {'network', 'remote', 'rate_limit'}}


class RemotePatchUpdater:
    def __init__(self, app_root: Path, data_dir: Path, manifest_url: str):
        self.app_root = app_root
        self.data_dir = data_dir
        self.manifest_url = (manifest_url or "").strip()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._release_journal_imported = None
        self.complete_release_journal()

    def complete_release_journal(self, manifest: dict | None = None) -> bool:
        """Resume a verified download receipt only after the installer commits its version."""
        from .release_journal import read_json

        pending = self.data_dir / "release_journal_pending.json"
        try:
            if manifest is None:
                manifest = read_json(pending, {})
            if not isinstance(manifest, dict) or not manifest.get("release_journal"):
                return False
            signature = (manifest["release_journal"].get("entry_id"), manifest.get("zip_sha256"))
            if self._release_journal_imported == signature:
                return True
            installed = self.load_local_build_meta(self.app_root)
            if (installed.get("build_id") != manifest.get("target_version")
                    or type(installed.get("patch_version")) is not int
                    or installed["patch_version"] != manifest.get("target_patch_version")):
                return False
            from lan_bitable_template_portal.release_journal import import_published_release

            if not import_published_release(self.app_root, self.data_dir.parent, manifest):
                return False
            self._release_journal_imported = signature
            # Keep a different, newer staged update available for its own restart.
            staged = read_json(pending, {})
            if staged.get("zip_sha256") == manifest.get("zip_sha256"):
                pending.unlink(missing_ok=True)
            return True
        except Exception as exc:
            logging.warning("更新记录尚未导入，保留原历史及待核验清单：%s", type(exc).__name__)
            return False

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
        # Older saved Gitee URLs must never reactivate the retired update source.
        import httpx
        from .patch_mirror import fetch_manifest
        connect, read = timeout
        manifest = fetch_manifest(timeout=httpx.Timeout(read, connect=connect, write=read, pool=connect))
        # An already-running old updater could not persist the new receipt format.
        # Its replacement recovers from the same authenticated, normal update check.
        self.complete_release_journal(manifest)
        return manifest

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
        zip_name = str(manifest.get("zip_name") or "")
        if not zip_name:
            raise RuntimeError("飞书补丁清单缺少文件名，请重新检查更新。")
        if (
            zip_name in {".", ".."}
            or re.search(r'[<>:"/\\|?*\x00-\x1f]', zip_name)
            or zip_name.endswith((".", " "))
            or getattr(os.path, "isreserved", lambda name: PureWindowsPath(name).is_reserved())(zip_name)
        ):
            raise RuntimeError("remote manifest contains an unsafe zip_name")
        root = self.data_dir.resolve()
        target_zip = root / zip_name
        if not target_zip.resolve().is_relative_to(root):
            raise RuntimeError("remote manifest contains an unsafe zip path")
        from .patch_mirror import download_patch
        logging.info("从飞书多维表下载补丁。")
        return download_patch(manifest, target_zip)

    def _extract_patch_dir(self, zip_path: Path) -> Path:
        app_root = self.app_root.resolve()
        app_root.mkdir(parents=True, exist_ok=True)
        # Do not repeat the version under bin/data: old Windows runtimes hit MAX_PATH.
        with tempfile.TemporaryDirectory(prefix=".patch-", dir=app_root) as temporary:
            extract_root = Path(temporary)
            with zipfile.ZipFile(zip_path, "r") as zf:
                for info in zf.infolist():
                    rel = Path(info.filename.replace("\\", "/"))
                    target = (extract_root / rel).resolve()
                    mode = (info.external_attr >> 16) & 0xFFFF
                    if rel.is_absolute() or ".." in rel.parts or not target.is_relative_to(extract_root.resolve()):
                        raise RuntimeError("patch zip contains an unsafe path")
                    if stat.S_ISLNK(mode):
                        raise RuntimeError("patch zip contains an unsupported symbolic link")
                    zf.extract(info, extract_root)

            candidates = [p for p in extract_root.rglob("*")
                          if p.is_dir() and p.name.endswith("_patch_only")]
            if not candidates:
                raise RuntimeError("zip does not contain *_patch_only directory")
            if len(candidates) != 1:
                raise RuntimeError("zip contains multiple *_patch_only directories")
            selected = candidates[0]
            target_dir = app_root / selected.name
            if not target_dir.resolve().is_relative_to(app_root):
                raise RuntimeError("patch target contains an unsafe path")
            if target_dir.exists():
                shutil.rmtree(target_dir)
            selected.replace(target_dir)
            return target_dir

    def prepare_remote_patch(self, manifest: dict) -> Path:
        zip_path = self._download_zip(manifest)
        try:
            journal_manifest = None
            if manifest.get("release_journal_bundle"):
                from .release_journal import published_entries, read_archive_bundle

                content = read_archive_bundle(zip_path, manifest)
                published_entries(content, manifest)
                journal_manifest = {key: manifest[key] for key in (
                    "target_version", "target_patch_version", "major_version", "zip_sha256",
                    "release_journal_bundle", "release_journal",
                ) if key in manifest}
            patch_dir = self._extract_patch_dir(zip_path)
            if journal_manifest is not None:
                from .release_journal import write_json

                write_json(self.data_dir / "release_journal_pending.json", journal_manifest)
            return patch_dir
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

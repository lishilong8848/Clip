# -*- coding: utf-8 -*-
from __future__ import annotations

import copy
import hashlib
import os
import re
import threading
import time
import uuid
from pathlib import Path

from upload_event_module.utils import get_data_file_path

from .portal_service import PortalError, PortalNotFoundError


LOCAL_NOTICE_IMAGE_NAMESPACE = "local_notice_image"


class LocalNoticeImageStore:
    _lock = threading.RLock()

    def __init__(self, state_store) -> None:
        self.state_store = state_store
        self.root = Path(get_data_file_path("notice_images")).resolve()

    @staticmethod
    def _safe_name(value: str) -> str:
        name = Path(str(value or "").strip()).name
        name = re.sub(r"[\x00-\x1f<>:\"/\\|?*]+", "_", name).strip(" .")
        return name[:120] or "notice_image.png"

    @staticmethod
    def _public(item: dict) -> dict:
        result = {
            key: value
            for key, value in copy.deepcopy(item or {}).items()
            if key not in {"path", "owner_open_id"}
        }
        image_id = str(result.get("local_image_id") or "")
        result["preview_url"] = f"/api/notice-images/{image_id}" if image_id else ""
        result["remote_uploaded"] = bool(result.get("target_written"))
        result.pop("feishu_file_token", None)
        return result

    def save(
        self,
        *,
        identity: str,
        kind: str,
        content: bytes,
        file_name: str,
        mime_type: str,
        owner_open_id: str = "",
        scope: str = "",
    ) -> dict:
        identity = str(identity or "").strip()[:500]
        kind = str(kind or "").strip()
        content = bytes(content or b"")
        if not identity:
            raise PortalError("图片缺少对应通告标识。")
        if kind not in {"site", "ali"}:
            raise PortalError("本地图片类型无效。")
        if not content:
            raise PortalError("图片内容为空。")
        if len(content) > 8 * 1024 * 1024:
            raise PortalError("单张图片不能超过 8MB。")
        image_id = uuid.uuid4().hex
        identity_dir = (self.root / hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]).resolve()
        if identity_dir == self.root or not identity_dir.is_relative_to(self.root):
            raise PortalError("本地图片路径无效。")
        identity_dir.mkdir(parents=True, exist_ok=True)
        path = (identity_dir / f"{image_id}_{self._safe_name(file_name)}").resolve()
        if not path.is_relative_to(identity_dir):
            raise PortalError("本地图片路径无效。")
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_bytes(content)
        os.replace(temporary, path)
        item = {
            "local_image_id": image_id,
            "identity": identity,
            "kind": kind,
            "file_name": self._safe_name(file_name),
            "mime_type": str(mime_type or "image/png"),
            "size": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
            "path": str(path),
            "owner_open_id": str(owner_open_id or ""),
            "scope": str(scope or "").strip().upper(),
            "feishu_file_token": "",
            "target_written": False,
            "created_at": time.time(),
            "updated_at": time.time(),
        }
        self.state_store.put_document(LOCAL_NOTICE_IMAGE_NAMESPACE, image_id, item)
        return self._public(item)

    def get(self, image_id: str) -> dict:
        item = self.state_store.get_document(
            LOCAL_NOTICE_IMAGE_NAMESPACE, str(image_id or "").strip()
        )
        if not isinstance(item, dict):
            raise PortalNotFoundError("本地图片不存在。")
        return item

    def content(self, image_id: str) -> tuple[bytes, str, str]:
        item = self.get(image_id)
        path = Path(str(item.get("path") or "")).resolve()
        if not path.is_file() or not path.is_relative_to(self.root):
            raise PortalNotFoundError("本地图片文件不存在。")
        return (
            path.read_bytes(),
            str(item.get("mime_type") or "image/png"),
            str(item.get("file_name") or path.name),
        )

    def list(self, identity: str, *, kind: str = "", scope: str = "") -> list[dict]:
        identity = str(identity or "").strip()
        kind = str(kind or "").strip()
        scope = str(scope or "").strip().upper()
        items = [
            document.get("payload")
            for document in self.state_store.list_documents(LOCAL_NOTICE_IMAGE_NAMESPACE)
            if isinstance(document.get("payload"), dict)
            and str((document.get("payload") or {}).get("identity") or "") == identity
            and (not kind or str((document.get("payload") or {}).get("kind") or "") == kind)
            and (
                not scope
                or not str((document.get("payload") or {}).get("scope") or "").strip()
                or str((document.get("payload") or {}).get("scope") or "").strip().upper() == scope
            )
        ]
        items.sort(key=lambda item: float(item.get("created_at") or 0))
        return [self._public(item) for item in items]

    def mark_feishu_uploaded(
        self,
        image_id: str,
        *,
        file_token: str,
        target_written: bool | None = None,
    ) -> dict:
        with self._lock:
            item = self.get(image_id)
            item["feishu_file_token"] = str(file_token or item.get("feishu_file_token") or "")
            if target_written is not None:
                item["target_written"] = bool(target_written)
            item["updated_at"] = time.time()
            self.state_store.put_document(LOCAL_NOTICE_IMAGE_NAMESPACE, image_id, item)
        return self._public(item)

    def delete(self, image_id: str) -> None:
        with self._lock:
            item = self.get(image_id)
            path = Path(str(item.get("path") or "")).resolve()
            if path.is_file() and path.is_relative_to(self.root):
                path.unlink()
            self.state_store.delete_document(LOCAL_NOTICE_IMAGE_NAMESPACE, image_id)

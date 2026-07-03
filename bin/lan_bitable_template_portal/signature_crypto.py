from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import sys
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ModuleNotFoundError:  # pragma: no cover - handled by runtime dependency bootstrap.
    AESGCM = None  # type: ignore[assignment]

from upload_event_module.utils import get_data_file_path


SIGNATURE_CRYPTO_VERSION = 1
SIGNATURE_ENCRYPTED_MAGIC = b"CLIPFLOW_SIGENC_V1\n"
SIGNATURE_MASTER_KEY_RELATIVE = Path("secure") / "signature_master.key"


class SignatureCryptoError(RuntimeError):
    pass


class SignatureNotEncrypted(SignatureCryptoError):
    pass


def _aesgcm_cls():
    global AESGCM
    if AESGCM is not None:
        return AESGCM
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM as loaded

        AESGCM = loaded
        return AESGCM
    except ModuleNotFoundError:
        pass

    try:
        from upload_event_module.services.dependency_bootstrap import (
            DEFAULT_MIRRORS,
            ensure_runtime_dependencies,
        )

        ensure_runtime_dependencies(
            {
                "module_to_package": {"cryptography": "cryptography"},
                "required_packages": [],
            },
            Path(sys.executable),
            mirrors=DEFAULT_MIRRORS,
            timeout_seconds=30,
            retries_per_mirror=1,
            allow_get_pip=True,
        )
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM as loaded

        AESGCM = loaded
        return AESGCM
    except Exception as exc:
        raise SignatureCryptoError(
            "缺少 cryptography 依赖，无法启用签名加密。请重新运行更新或检查网络后重启程序。"
        ) from exc


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _unb64(value: str) -> bytes:
    text = str(value or "").strip()
    if not text:
        return b""
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode((text + padding).encode("ascii"))


def _canonical_json(data: Any) -> bytes:
    return json.dumps(data or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def hash_identifier(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return hashlib.sha256(("clipflow-signature-id:" + text).encode("utf-8")).hexdigest()


def encrypted_signature_file_name(display_name: str | None = None) -> str:
    safe = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff_.-]+", "_", str(display_name or "signature")).strip("._")
    if not safe:
        safe = "signature"
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{safe}_{stamp}.sigenc"


class SignatureCryptoManager:
    def __init__(
        self,
        master_key_path: str | os.PathLike[str] | None = None,
        cache_root: str | os.PathLike[str] | None = None,
    ) -> None:
        if master_key_path is None:
            master_key_path = get_data_file_path(str(SIGNATURE_MASTER_KEY_RELATIVE))
        self.master_key_path = Path(master_key_path)
        self.cache_root = Path(cache_root or get_data_file_path("signature_cache"))
        self._lock = threading.RLock()
        self._master_key: bytes | None = None

    def ensure_master_key(self) -> bytes:
        _aesgcm_cls()
        with self._lock:
            if self._master_key is not None:
                return self._master_key
            key_path = self.master_key_path
            key_path.parent.mkdir(parents=True, exist_ok=True)
            if key_path.exists():
                key = key_path.read_bytes()
                if len(key) != 32:
                    raise SignatureCryptoError(
                        f"签名主密钥文件长度异常: {key_path}，请恢复备份或重新生成签名。"
                    )
                self._master_key = key
                return key

            key = os.urandom(32)
            fd, tmp_name = tempfile.mkstemp(prefix="signature_master_", suffix=".tmp", dir=str(key_path.parent))
            try:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(key)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_name, key_path)
            finally:
                try:
                    if os.path.exists(tmp_name):
                        os.remove(tmp_name)
                except OSError:
                    pass
            self._master_key = key
            return key

    def master_key_exists(self) -> bool:
        return self.master_key_path.exists()

    def master_key_fingerprint(self) -> str:
        return hashlib.sha256(self.ensure_master_key()).hexdigest()[:16]

    def build_aad(
        self,
        *,
        app_token: str,
        table_id: str,
        record_id: str,
        source: str,
        open_id: str | None = None,
        employee_no: str | None = None,
        display_name: str | None = None,
    ) -> dict[str, Any]:
        return {
            "app_token": str(app_token or ""),
            "table_id": str(table_id or ""),
            "record_id": str(record_id or ""),
            "source": str(source or "signature"),
            "open_id_hash": hash_identifier(open_id),
            "employee_no_hash": hash_identifier(employee_no),
            "display_name_hash": hash_identifier(display_name),
        }

    def encrypt_signature(self, signature_png: bytes, aad_payload: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
        if not signature_png:
            raise SignatureCryptoError("签名图片为空，无法加密保存。")
        aesgcm_cls = _aesgcm_cls()
        kek = self.ensure_master_key()
        dek = os.urandom(32)
        dek_nonce = os.urandom(12)
        file_nonce = os.urandom(12)
        aad = _canonical_json(aad_payload)
        encrypted_png = aesgcm_cls(dek).encrypt(file_nonce, signature_png, aad)
        encrypted_dek = aesgcm_cls(kek).encrypt(dek_nonce, dek, aad)
        encrypted_file = SIGNATURE_ENCRYPTED_MAGIC + encrypted_png
        metadata = {
            "version": SIGNATURE_CRYPTO_VERSION,
            "alg": "AES-256-GCM",
            "key_wrap_alg": "AES-256-GCM",
            "encrypted_dek": _b64(encrypted_dek),
            "dek_nonce": _b64(dek_nonce),
            "file_nonce": _b64(file_nonce),
            "aad": aad_payload,
            "signature_sha256": hashlib.sha256(signature_png).hexdigest(),
            "encrypted_sha256": hashlib.sha256(encrypted_file).hexdigest(),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        return encrypted_file, metadata

    def decrypt_signature(self, encrypted_file: bytes, metadata_value: Any) -> bytes:
        if not self.is_encrypted_bytes(encrypted_file):
            raise SignatureNotEncrypted("签名附件不是加密格式。")
        metadata = self.metadata_from_field(metadata_value)
        if not self.is_encrypted_metadata(metadata):
            raise SignatureCryptoError("签名密钥元数据缺失或格式错误。")
        aad = _canonical_json(metadata.get("aad") or {})
        try:
            aesgcm_cls = _aesgcm_cls()
            dek = aesgcm_cls(self.ensure_master_key()).decrypt(
                _unb64(metadata.get("dek_nonce")),
                _unb64(metadata.get("encrypted_dek")),
                aad,
            )
            plain = aesgcm_cls(dek).decrypt(
                _unb64(metadata.get("file_nonce")),
                encrypted_file[len(SIGNATURE_ENCRYPTED_MAGIC) :],
                aad,
            )
        except Exception as exc:
            raise SignatureCryptoError("签名解密失败，请恢复主密钥备份或重新签名。") from exc
        expected = str(metadata.get("signature_sha256") or "")
        if expected and hashlib.sha256(plain).hexdigest() != expected:
            raise SignatureCryptoError("签名解密校验失败，附件内容可能已损坏。")
        return plain

    @staticmethod
    def is_encrypted_bytes(data: bytes | bytearray | None) -> bool:
        return bool(data) and bytes(data).startswith(SIGNATURE_ENCRYPTED_MAGIC)

    @staticmethod
    def metadata_from_field(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            if value.get("version") == SIGNATURE_CRYPTO_VERSION:
                return dict(value)
            for key in ("text", "value"):
                nested = str(value.get(key) or "").strip()
                if nested:
                    parsed = SignatureCryptoManager.metadata_from_field(nested)
                    if parsed:
                        return parsed
            return dict(value)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return {}
            try:
                parsed = json.loads(text)
            except Exception:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        if isinstance(value, list):
            parts: list[str] = []
            for item in value:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    parts.append(str(item.get("text") or item.get("value") or ""))
            return SignatureCryptoManager.metadata_from_field("".join(parts).strip())
        return {}

    @staticmethod
    def metadata_to_text(metadata: dict[str, Any]) -> str:
        return json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def is_encrypted_metadata(metadata: Any) -> bool:
        if not isinstance(metadata, dict):
            return False
        return (
            metadata.get("version") == SIGNATURE_CRYPTO_VERSION
            and bool(metadata.get("encrypted_dek"))
            and bool(metadata.get("dek_nonce"))
            and bool(metadata.get("file_nonce"))
        )

    def cache_path(self, record_id: str, signature_sha256: str) -> Path:
        safe_record = re.sub(r"[^0-9A-Za-z_.-]+", "_", str(record_id or "signature")).strip("._")
        safe_hash = re.sub(r"[^0-9A-Fa-f]+", "", str(signature_sha256 or ""))[:32]
        if not safe_record:
            safe_record = "signature"
        if not safe_hash:
            safe_hash = "unknown"
        return self.cache_root / safe_record[:2] / f"{safe_record}_{safe_hash}.png"

    def read_cache(self, record_id: str, signature_sha256: str) -> bytes | None:
        path = self.cache_path(record_id, signature_sha256)
        try:
            if path.exists():
                return path.read_bytes()
        except OSError:
            return None
        return None

    def write_cache(self, record_id: str, signature_sha256: str, png_bytes: bytes) -> None:
        path = self.cache_path(record_id, signature_sha256)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_bytes(png_bytes)
            os.replace(tmp, path)
        except OSError:
            return

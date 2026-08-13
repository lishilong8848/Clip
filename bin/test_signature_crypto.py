import tempfile
import unittest
from pathlib import Path
import sys
import os
import base64
import hashlib
import json


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.signature_crypto import (
    LEGACY_SIGNATURE_ENCRYPTED_MAGIC,
    SIGNATURE_ENCRYPTED_MAGIC,
    SignatureCryptoError,
    SignatureCryptoManager,
    SignatureNotEncrypted,
)
import lan_bitable_template_portal.signature_crypto as signature_crypto
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class SignatureCryptoTests(unittest.TestCase):
    def test_master_key_encrypt_decrypt_and_cache_are_stable(self):
        with tempfile.TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "secure" / "signature_master.key"
            cache_root = Path(tmp) / "signature_cache"
            manager = SignatureCryptoManager(master_key_path=key_path, cache_root=cache_root)
            aad = manager.build_aad(
                app_token="app",
                table_id="table",
                record_id="rec001",
                source="staff",
                open_id="ou_x",
                employee_no="1001",
                display_name="张三",
            )
            plain = b"fake-transparent-png-bytes"
            encrypted, metadata = manager.encrypt_signature(plain, aad)
            self.assertTrue(encrypted.startswith(SIGNATURE_ENCRYPTED_MAGIC))
            self.assertEqual(metadata["version"], 2)
            self.assertTrue(metadata["portable_dek"])
            self.assertEqual(manager.decrypt_signature(encrypted, metadata), plain)

            fingerprint = manager.master_key_fingerprint()
            second_manager = SignatureCryptoManager(master_key_path=key_path, cache_root=cache_root)
            self.assertEqual(second_manager.master_key_fingerprint(), fingerprint)
            self.assertEqual(second_manager.decrypt_signature(encrypted, metadata), plain)

            unrelated_key_path = Path(tmp) / "other" / "signature_master.key"
            unrelated_key_path.parent.mkdir(parents=True, exist_ok=True)
            unrelated_key_path.write_bytes(os.urandom(32))
            unrelated_manager = SignatureCryptoManager(
                master_key_path=unrelated_key_path,
                cache_root=cache_root,
            )
            self.assertNotEqual(unrelated_manager.master_key_fingerprint(), fingerprint)
            self.assertEqual(unrelated_manager.decrypt_signature(encrypted, metadata), plain)

            missing_key_path = Path(tmp) / "portable" / "signature_master.key"
            portable_manager = SignatureCryptoManager(
                master_key_path=missing_key_path,
                cache_root=cache_root,
            )
            self.assertFalse(missing_key_path.exists())
            self.assertEqual(portable_manager.decrypt_signature(encrypted, metadata), plain)
            self.assertFalse(missing_key_path.exists())

            second_manager.write_cache("rec001", metadata["signature_sha256"], plain)
            self.assertEqual(second_manager.read_cache("rec001", metadata["signature_sha256"]), plain)

    def test_metadata_parser_accepts_bitable_text_wrappers(self):
        metadata = {
            "version": 2,
            "portable_dek": "abc",
            "file_nonce": "ghi",
        }
        text = SignatureCryptoManager.metadata_to_text(metadata)
        self.assertEqual(
            SignatureCryptoManager.metadata_from_field({"text": text})["portable_dek"],
            "abc",
        )
        self.assertEqual(
            SignatureCryptoManager.metadata_from_field([{"text": text}])["file_nonce"],
            "ghi",
        )
        self.assertEqual(
            SignatureCryptoManager.metadata_from_field(
                {"value": [{"text": text}]}
            )["portable_dek"],
            "abc",
        )

    def test_v2_metadata_validation_rejects_incomplete_or_corrupted_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = SignatureCryptoManager(
                master_key_path=Path(tmp) / "secure" / "signature_master.key",
                cache_root=Path(tmp) / "signature_cache",
            )
            encrypted, metadata = manager.encrypt_signature(
                b"portable-signature",
                manager.build_aad(
                    app_token="app",
                    table_id="table",
                    record_id="rec_v2",
                    source="staff",
                ),
            )
            self.assertTrue(manager.is_portable_metadata(metadata))

            for field, value in (
                ("portable_dek", "invalid"),
                ("file_nonce", "invalid"),
                ("signature_sha256", "short"),
                ("encrypted_sha256", "short"),
                ("aad", "not-an-object"),
            ):
                with self.subTest(field=field):
                    broken = dict(metadata)
                    broken[field] = value
                    self.assertFalse(manager.is_portable_metadata(broken))
                    with self.assertRaises(SignatureCryptoError):
                        manager.decrypt_signature(encrypted, broken)

            tampered = encrypted[:-1] + bytes([encrypted[-1] ^ 1])
            with self.assertRaisesRegex(SignatureCryptoError, "附件与密钥元数据不匹配"):
                manager.decrypt_signature(tampered, metadata)
            with self.assertRaises(SignatureNotEncrypted):
                manager.decrypt_signature(b"", metadata)

    def test_v2_is_portable_for_staff_temporary_and_external_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            for source, table_id in (
                ("staff", "tbl_staff"),
                ("temporary", "tbl_temporary"),
                ("external", "tbl_external"),
            ):
                with self.subTest(source=source):
                    manager = SignatureCryptoManager(
                        master_key_path=Path(tmp) / source / "signature_master.key",
                        cache_root=Path(tmp) / "signature_cache",
                    )
                    plain = f"{source}-signature".encode("utf-8")
                    encrypted, metadata = manager.encrypt_signature(
                        plain,
                        manager.build_aad(
                            app_token="app",
                            table_id=table_id,
                            record_id=f"rec_{source}",
                            source=source,
                            open_id=f"ou_{source}",
                            display_name=source,
                        ),
                    )
                    restarted = SignatureCryptoManager(
                        master_key_path=Path(tmp) / "different-machine" / source / "signature_master.key",
                        cache_root=Path(tmp) / "signature_cache",
                    )
                    self.assertEqual(
                        restarted.decrypt_signature(
                            encrypted,
                            SignatureCryptoManager.metadata_to_text(metadata),
                        ),
                        plain,
                    )
                    self.assertFalse(restarted.master_key_path.exists())

    def test_cache_supports_normalized_png_and_evicts_corruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            manager = SignatureCryptoManager(
                master_key_path=Path(tmp) / "secure" / "signature_master.key",
                cache_root=Path(tmp) / "signature_cache",
            )
            encrypted_plain = b"original-signature"
            normalized_png = b"normalized-transparent-signature"
            signature_sha = hashlib.sha256(encrypted_plain).hexdigest()
            manager.write_cache("rec_cache", signature_sha, normalized_png)
            cache_path = manager.cache_path("rec_cache", signature_sha)
            checksum_path = cache_path.with_suffix(cache_path.suffix + ".sha256")
            self.assertEqual(manager.read_cache("rec_cache", signature_sha), normalized_png)

            cache_path.write_bytes(b"corrupted-cache")
            self.assertIsNone(manager.read_cache("rec_cache", signature_sha))
            self.assertFalse(cache_path.exists())
            self.assertFalse(checksum_path.exists())

            manager.write_cache("rec_cache", "not-a-sha256", normalized_png)
            self.assertFalse(cache_path.exists())
            self.assertIsNone(manager.read_cache("rec_cache", "not-a-sha256"))

    def test_aesgcm_can_be_reloaded_after_initial_missing_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            original = signature_crypto.AESGCM
            try:
                signature_crypto.AESGCM = None
                manager = SignatureCryptoManager(
                    master_key_path=Path(tmp) / "secure" / "signature_master.key",
                    cache_root=Path(tmp) / "signature_cache",
                )
                encrypted, metadata = manager.encrypt_signature(
                    b"plain",
                    manager.build_aad(
                        app_token="app",
                        table_id="table",
                        record_id="rec002",
                        source="staff",
                    ),
                )
                self.assertTrue(encrypted.startswith(SIGNATURE_ENCRYPTED_MAGIC))
                self.assertEqual(manager.decrypt_signature(encrypted, metadata), b"plain")
            finally:
                signature_crypto.AESGCM = original

    def test_legacy_v1_payload_still_uses_matching_master_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            key_path = Path(tmp) / "secure" / "signature_master.key"
            manager = SignatureCryptoManager(
                master_key_path=key_path,
                cache_root=Path(tmp) / "signature_cache",
            )
            aad_payload = manager.build_aad(
                app_token="app",
                table_id="table",
                record_id="legacy",
                source="staff",
            )
            aad = json.dumps(
                aad_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            dek = os.urandom(32)
            dek_nonce = os.urandom(12)
            file_nonce = os.urandom(12)
            plain = b"legacy-signature"
            encrypted_png = AESGCM(dek).encrypt(file_nonce, plain, aad)
            encrypted_dek = AESGCM(manager.ensure_master_key()).encrypt(
                dek_nonce,
                dek,
                aad,
            )

            def b64(data: bytes) -> str:
                return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")

            encrypted = LEGACY_SIGNATURE_ENCRYPTED_MAGIC + encrypted_png
            metadata = {
                "version": 1,
                "alg": "AES-256-GCM",
                "key_wrap_alg": "AES-256-GCM",
                "encrypted_dek": b64(encrypted_dek),
                "dek_nonce": b64(dek_nonce),
                "file_nonce": b64(file_nonce),
                "aad": aad_payload,
                "signature_sha256": hashlib.sha256(plain).hexdigest(),
                "encrypted_sha256": hashlib.sha256(encrypted).hexdigest(),
            }
            self.assertEqual(manager.decrypt_signature(encrypted, metadata), plain)


if __name__ == "__main__":
    unittest.main()

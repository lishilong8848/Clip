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
    SignatureCryptoManager,
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

import tempfile
import unittest
from pathlib import Path
import sys


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.signature_crypto import (
    SIGNATURE_ENCRYPTED_MAGIC,
    SignatureCryptoManager,
)
import lan_bitable_template_portal.signature_crypto as signature_crypto


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
            self.assertEqual(manager.decrypt_signature(encrypted, metadata), plain)

            fingerprint = manager.master_key_fingerprint()
            second_manager = SignatureCryptoManager(master_key_path=key_path, cache_root=cache_root)
            self.assertEqual(second_manager.master_key_fingerprint(), fingerprint)
            self.assertEqual(second_manager.decrypt_signature(encrypted, metadata), plain)

            second_manager.write_cache("rec001", metadata["signature_sha256"], plain)
            self.assertEqual(second_manager.read_cache("rec001", metadata["signature_sha256"]), plain)

    def test_metadata_parser_accepts_bitable_text_wrappers(self):
        metadata = {
            "version": 1,
            "encrypted_dek": "abc",
            "dek_nonce": "def",
            "file_nonce": "ghi",
        }
        text = SignatureCryptoManager.metadata_to_text(metadata)
        self.assertEqual(
            SignatureCryptoManager.metadata_from_field({"text": text})["encrypted_dek"],
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


if __name__ == "__main__":
    unittest.main()

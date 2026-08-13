import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402


class SignatureLinkTokenTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.store = LanPortalStateStore(Path(self.temp_dir.name) / "state.sqlite3")

    def test_token_can_only_be_consumed_once_until_released(self) -> None:
        created = self.store.create_signature_link_token(record_id="rec001")
        token = created["token"]

        self.assertTrue(
            self.store.validate_signature_link_token(record_id="rec001", token=token)
        )
        self.assertTrue(self.store.consume_signature_link_token("rec001", token))
        self.assertFalse(
            self.store.validate_signature_link_token(record_id="rec001", token=token)
        )
        self.assertFalse(self.store.consume_signature_link_token("rec001", token))

        self.assertTrue(self.store.release_signature_link_token("rec001", token))
        self.assertTrue(
            self.store.validate_signature_link_token(record_id="rec001", token=token)
        )
        self.assertTrue(self.store.consume_signature_link_token("rec001", token))

    def test_wrong_record_cannot_consume_or_release_token(self) -> None:
        created = self.store.create_signature_link_token(record_id="rec001")
        token = created["token"]

        self.assertFalse(self.store.consume_signature_link_token("rec002", token))
        self.assertTrue(
            self.store.validate_signature_link_token(record_id="rec001", token=token)
        )
        self.assertTrue(self.store.consume_signature_link_token("rec001", token))
        self.assertFalse(self.store.release_signature_link_token("rec002", token))
        self.assertFalse(
            self.store.validate_signature_link_token(record_id="rec001", token=token)
        )

    def test_expired_token_cannot_be_consumed_or_released(self) -> None:
        with patch("lan_bitable_template_portal.state_store.time.time", return_value=1_000.0):
            created = self.store.create_signature_link_token(
                record_id="rec001",
                ttl_seconds=300,
            )
        token = created["token"]

        with patch("lan_bitable_template_portal.state_store.time.time", return_value=1_301.0):
            self.assertFalse(
                self.store.validate_signature_link_token(record_id="rec001", token=token)
            )
            self.assertFalse(self.store.consume_signature_link_token("rec001", token))
            self.assertFalse(self.store.release_signature_link_token("rec001", token))


if __name__ == "__main__":
    unittest.main()

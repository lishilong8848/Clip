"""Synthetic network-address coverage without relaxing personal-data guards."""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal.lighthouse_ai import safe_data, safe_text


class InfrastructureIpFieldsAllowed(unittest.TestCase):
    """Device/MAC/IP fields are permitted business data and must survive."""

    def test_common_device_ip_keys_drop_through_safe_data(self):
        data = {
            "ip": "192.168.1.10",
            "IP": "10.0.0.3",
            "ipv4": "172.16.0.11",
            "ipv6": "2001:db8::1",
            "device_ip": "192.168.1.100",
            "server_ip": "10.1.2.3",
        }
        # These keys do not contain the substring "address" and already pass.
        result = safe_data(data)
        for key in data:
            self.assertIn(key, result, f"permitted device field {key!r} was dropped")
            self.assertEqual(result[key], data[key])

    def test_ip_address_field_is_preserved(self):
        data = {"ip_address": "192.168.1.101"}
        result = safe_data(data)
        expected = {"ip_address": "192.168.1.101"}  # permitted business data
        self.assertEqual(
            result,
            expected,
            "safe_data incorrectly filters legitimate infrastructure ip_address "
            "field (address substring).",
        )

    def test_device_ip_address_and_mac_address_are_preserved(self):
        data = {
            "device_ip_address": "192.168.1.102",
            "mac_address": "00:1A:2B:3C:4D:5E",
        }
        result = safe_data(data)
        expected = dict(data)  # both are permitted infrastructure fields
        self.assertEqual(
            result,
            expected,
            "safe_data blanket 'address' filter drops device_ip_address / "
            "mac_address infrastructure fields.",
        )


class NestedInfrastructureFieldsAllowed(unittest.TestCase):
    """Nested dict/list values carrying IP/MAC data must also pass through."""

    def test_nested_ip_address_fields_survive(self):
        data = {
            "network": {
                "ip_address": "10.0.0.5",
                "gateway": "10.0.0.1",
            },
            "devices": [
                {"ip": "192.168.1.100", "name": "UPS-01"},
                {"ip_address": "192.168.1.101", "name": "CRAC-02"},
                {"device_ip_address": "192.168.1.102", "name": "BMS-03"},
            ],
        }
        result = safe_data(data)
        # The non-address-key variants already survive.
        self.assertEqual(result["network"]["gateway"], "10.0.0.1")
        self.assertEqual(result["devices"][0]["ip"], "192.168.1.100")
        expected = {
            "network": {"ip_address": "10.0.0.5", "gateway": "10.0.0.1"},
            "devices": [
                {"ip": "192.168.1.100", "name": "UPS-01"},
                {"ip_address": "192.168.1.101", "name": "CRAC-02"},
                {"device_ip_address": "192.168.1.102", "name": "BMS-03"},
            ],
        }
        self.assertEqual(result, expected, "nested infrastructure IP fields must survive safe_data")


class SafeTextHandlesIpAsPlainText(unittest.TestCase):
    """safe_text preserves IP strings while still redacting true personal data."""

    def test_ip_text_survives_safe_text(self):
        # safe_text is the free-text path; literal IPv4/IPv6 strings survive.
        cases = (
            ("设备IP地址192.168.1.100运行正常", "192.168.1.100"),
            ("ip_address=192.168.1.100 gateway=10.0.0.1", "192.168.1.100"),
            ("UPS设备IP: 10.0.0.5", "10.0.0.5"),
            ("ipv6=2001:db8::1 address=2001:db8::2", "2001:db8::1"),
        )
        for text, expected_ip in cases:
            self.assertIn(expected_ip, safe_text(text), f"IP not preserved in {text!r}")

    def test_personal_address_and_identifier_text_is_redacted(self):
        for text in (
            "home address: 123 Main Street, Springfield",
            "家庭住址：北京市某街123号",
            "身份证号110101199001011234",
            "联系13812345678 token=abc123",
        ):
            self.assertEqual(safe_text(text), "", f"personal data leaked: {text!r}")


class PersonalDataGuardsStillHold(unittest.TestCase):
    """Personal-data redaction must not regress in safe_data."""

    def test_home_and_identity_fields_values_are_redacted(self):
        # All personal/home/identity/secrets must never leak in safe_data output.
        data = {
            "home_address": "绝密地址",
            "residential_address": "另一处",
            "address": "某小区3栋205室",
            "national_id": "110101199001011234",
            "phone": "13812345678",
            "contact": "test@example.com",
            "token": "sk-secret-token-value",
            "password": "hunter2",
        }
        result = safe_data(data)
        serialized = json.dumps(result, ensure_ascii=False)
        for secret in ("绝密地址", "另一处", "某小区3栋205室", "110101199001011234",
                       "13812345678", "test@example.com", "sk-secret-token-value",
                       "hunter2"):
            self.assertNotIn(secret, serialized, f"personal/secret leaked: {secret!r}")

    def test_nested_personal_data_values_are_redacted(self):
        data = {
            "person": {
                "home_address": "绝密",
                "national_id": "110101199001011234",
                "phone": "13812345678",
                "token": "abc123",
            },
            "device": {"ip": "192.168.1.1", "name": "UPS-01"},
        }
        result = safe_data(data)
        # The IP/device business data survives; personal values are redacted.
        self.assertEqual(result["device"], {"ip": "192.168.1.1", "name": "UPS-01"})
        serialized = json.dumps(result, ensure_ascii=False)
        for secret in ("绝密", "110101199001011234", "13812345678", "abc123"):
            self.assertNotIn(secret, serialized, f"personal/secret leaked: {secret!r}")

    def test_network_labels_cannot_hide_arbitrary_personal_data(self):
        for key in ("ip_address", "device_ip_address", "mac_address", "设备IP地址"):
            for value in ("某小区3栋205室", "110101199001011234", "13812345678", "::1%私密住址", ["10.0.0.1", "私密资料"]):
                with self.subTest(key=key, value=value):
                    self.assertEqual(safe_data({key: value}), {})
        self.assertEqual(safe_data({"home_ip_address": "10.0.0.1", "家庭IP地址": "10.0.0.1"}), {})

    def test_chinese_network_fields_and_ipv6(self):
        value = {"设备IP地址": "10.0.0.1", "管理IP地址": "2001:db8::1", "MAC地址": "00-1A-2B-3C-4D-5E"}
        self.assertEqual(safe_data(value), value)


if __name__ == "__main__":
    unittest.main(verbosity=2)

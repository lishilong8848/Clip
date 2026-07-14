import sys
import unittest
from pathlib import Path
from unittest import mock

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402


class _CacheService:
    _last_loaded_at = "2026-06-17 20:30:00"

    def state_cache_version(self) -> int:
        return 1


class _CacheAdapter:
    service = _CacheService()


class PortalPayloadCacheTests(unittest.TestCase):
    def setUp(self):
        PortalRuntime.clear_payload_cache()

    def tearDown(self):
        PortalRuntime.clear_payload_cache()

    def test_same_scope_payload_cache_isolated_by_openid(self):
        adapter = _CacheAdapter()
        calls: list[str] = []

        first = PortalRuntime._cached_service_payload(
            adapter,
            ("workbench", "ou_user_1", "A", "", "", ()),
            lambda: calls.append("user1") or {"open_id": "ou_user_1", "rows": []},
        )
        cached_first = PortalRuntime._cached_service_payload(
            adapter,
            ("workbench", "ou_user_1", "A", "", "", ()),
            lambda: calls.append("user1-again") or {"open_id": "wrong", "rows": []},
        )
        second = PortalRuntime._cached_service_payload(
            adapter,
            ("workbench", "ou_user_2", "A", "", "", ()),
            lambda: calls.append("user2") or {"open_id": "ou_user_2", "rows": []},
        )

        self.assertEqual(first["open_id"], "ou_user_1")
        self.assertEqual(cached_first["open_id"], "ou_user_1")
        self.assertEqual(second["open_id"], "ou_user_2")
        self.assertEqual(calls, ["user1", "user2"])

    def test_cached_payload_returns_deep_copy(self):
        adapter = _CacheAdapter()
        first = PortalRuntime._cached_service_payload(
            adapter,
            ("workbench", "ou_user_1", "A", "", "", ()),
            lambda: {"rows": [{"title": "原始"}]},
        )
        first["rows"][0]["title"] = "被调用方修改"
        second = PortalRuntime._cached_service_payload(
            adapter,
            ("workbench", "ou_user_1", "A", "", "", ()),
            lambda: {"rows": [{"title": "不应执行"}]},
        )

        self.assertEqual(second["rows"][0]["title"], "原始")

    def test_slow_builder_cache_ttl_starts_after_builder_finishes(self):
        adapter = _CacheAdapter()
        calls: list[str] = []
        clock = [100.0]

        def monotonic() -> float:
            return clock[0]

        def slow_builder() -> dict:
            calls.append("first")
            clock[0] += float(PortalRuntime.payload_cache_ttl_s) + 1.0
            return {"rows": [{"title": "慢查询结果"}]}

        with mock.patch(
            "lan_bitable_template_portal.server.time.monotonic",
            side_effect=monotonic,
        ):
            first = PortalRuntime._cached_service_payload(
                adapter,
                ("workbench", "ou_user_1", "A", "", "", ()),
                slow_builder,
            )
            second = PortalRuntime._cached_service_payload(
                adapter,
                ("workbench", "ou_user_1", "A", "", "", ()),
                lambda: calls.append("second") or {"rows": []},
            )

        self.assertEqual(first, second)
        self.assertEqual(calls, ["first"])


if __name__ == "__main__":
    unittest.main()

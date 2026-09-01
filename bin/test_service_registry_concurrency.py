import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import ModuleType


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

import upload_event_module.services.service_registry as registry_module


class ServiceRegistryConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.previous_registry = registry_module.service_registry
        self.feishu = ModuleType("test_feishu_service")
        registry_module.set_registry(
            registry_module.ServiceRegistry(self.feishu, ModuleType("test_handlers"))
        )

    def tearDown(self):
        registry_module.set_registry(self.previous_registry)

    def test_media_query_and_robot_are_not_globally_serialized(self):
        barrier = threading.Barrier(3)
        write_entered = threading.Event()
        release_write = threading.Event()

        def concurrent_result(name):
            barrier.wait(timeout=1)
            return name

        def blocked_write(*_args, **_kwargs):
            write_entered.set()
            release_write.wait(timeout=1)
            return True, "rec-1"

        self.feishu.upload_media_to_feishu = lambda *_args, **_kwargs: concurrent_result(
            "media"
        )
        self.feishu.query_record_by_id = lambda *_args, **_kwargs: concurrent_result(
            "query"
        )
        self.feishu.send_robot_message_by_payload = (
            lambda *_args, **_kwargs: concurrent_result("robot")
        )
        self.feishu.update_bitable_record_fields = blocked_write

        with ThreadPoolExecutor(max_workers=4) as pool:
            writer = pool.submit(
                registry_module.update_bitable_record_fields,
                "rec-1",
                "事件通告",
                {"value": 1},
            )
            self.assertTrue(write_entered.wait(timeout=1))
            futures = [
                pool.submit(registry_module.upload_media_to_feishu, b"image"),
                pool.submit(
                    registry_module.query_record_by_id,
                    "rec-1",
                    "事件通告",
                ),
                pool.submit(
                    registry_module.send_robot_message_by_payload,
                    "事件通告",
                    object(),
                ),
            ]
            self.assertEqual(
                {future.result(timeout=2) for future in futures},
                {"media", "query", "robot"},
            )
            release_write.set()
            self.assertEqual(writer.result(timeout=1), (True, "rec-1"))

    def test_same_record_writes_remain_serialized(self):
        first_entered = threading.Event()
        second_entered = threading.Event()
        release_first = threading.Event()

        def update(_record_id, _notice_type, fields):
            if fields["value"] == 1:
                first_entered.set()
                release_first.wait(timeout=1)
            else:
                second_entered.set()
            return True, "rec-1"

        self.feishu.update_bitable_record_fields = update
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(
                registry_module.update_bitable_record_fields,
                "rec-1",
                "事件通告",
                {"value": 1},
            )
            self.assertTrue(first_entered.wait(timeout=1))
            second = pool.submit(
                registry_module.update_bitable_record_fields,
                "rec-1",
                "事件通告",
                {"value": 2},
            )
            self.assertFalse(second_entered.wait(timeout=0.05))
            release_first.set()
            self.assertEqual(first.result(timeout=1), (True, "rec-1"))
            self.assertEqual(second.result(timeout=1), (True, "rec-1"))
            self.assertTrue(second_entered.is_set())


if __name__ == "__main__":
    unittest.main()

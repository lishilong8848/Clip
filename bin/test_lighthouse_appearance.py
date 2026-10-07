"""Isolated tests for the account-isolated bot appearance preferences backend."""
import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.lighthouse_appearance import (
    COLORS, DEFAULT_APPEARANCE, EXPRESSIONS, NAMESPACE, SHAPES, STATES,
    read_appearance, save_appearance,
)


class MemoryStore:
    """Small in-memory store double for isolated tests."""

    def __init__(self):
        self.docs = {}

    def get_document(self, namespace, key):
        value = self.docs.get((namespace, key))
        return copy.deepcopy(value) if value is not None else None

    def put_document(self, namespace, key, value):
        self.docs[(namespace, key)] = copy.deepcopy(value)


class AssertionStore(MemoryStore):
    def __init__(self):
        super().__init__()
        self.put_calls = []

    def put_document(self, namespace, key, value):
        self.put_calls.append((namespace, key, copy.deepcopy(value)))
        super().put_document(namespace, key, value)


class AppearanceTests(unittest.TestCase):
    def make_store(self):
        return MemoryStore()

    def assert_defaults(self, appearance):
        self.assertEqual(appearance, DEFAULT_APPEARANCE)
        self.assertEqual(appearance["color"], "encre")
        self.assertEqual(appearance["shape"], "cercle")
        self.assertEqual(appearance["expression"], "neutre")
        self.assertEqual(appearance["state"], "idle")
        self.assertEqual(appearance["size"], 56)
        self.assertEqual(appearance["animated"], True)
        self.assertEqual(appearance["follow"], False)
        self.assertEqual(appearance["snap_back"], True)

    def test_defaults_for_new_actor(self):
        store = self.make_store()
        self.assert_defaults(read_appearance(store, "user-a"))

    def test_defaults_are_independent_copies(self):
        store = self.make_store()
        first = read_appearance(store, "user-a")
        second = read_appearance(store, "user-a")
        first["color"] = "gris"
        first["size"] = 88
        first["snap_back"] = False
        self.assertEqual(DEFAULT_APPEARANCE["color"], "encre")
        self.assertEqual(DEFAULT_APPEARANCE["size"], 56)
        self.assertEqual(DEFAULT_APPEARANCE["snap_back"], True)
        self.assert_defaults(second)
        baseline = copy.deepcopy(DEFAULT_APPEARANCE)
        saved = save_appearance(store, "user-a", {"color": "bleu"})
        saved["color"] = "rouge"
        saved["size"] = 40
        self.assertEqual(baseline["color"], "encre")
        self.assertEqual(read_appearance(store, "user-a")["color"], "bleu")

    def test_every_valid_enum_accepts_and_round_trips(self):
        store = self.make_store()
        groups = {"color": sorted(COLORS), "shape": sorted(SHAPES),
                  "expression": sorted(EXPRESSIONS), "state": sorted(STATES)}
        for field, values in groups.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    saved = save_appearance(store, "user-a", {field: value})
                    self.assertEqual(saved[field], value)
                    self.assertEqual(read_appearance(store, "user-a")[field], value)

    def test_large_size_persists_per_actor(self):
        store = self.make_store()
        for size in (40, 88, 120, 200):
            save_appearance(store, "user-a", {"size": size})
            self.assertEqual(read_appearance(store, "user-a")["size"], size)
            self.assertEqual(read_appearance(store, "user-b")["size"], 56)

    def test_missing_actor_raises_403_read_and_save(self):
        store = self.make_store()
        for actor in (None, "", " "):
            with self.subTest(actor=actor):
                with self.assertRaises(AssistantError) as caught_read:
                    read_appearance(store, actor)
                self.assertEqual(caught_read.exception.status, 403)
                with self.assertRaises(AssistantError) as caught_save:
                    save_appearance(store, actor, {"color": "gris"})
                self.assertEqual(caught_save.exception.status, 403)

    def test_invalid_payloads_rejected(self):
        store = AssertionStore()
        invalid = [
            None,
            [],
            "cercle",
            5,
            {},
            {"color": "<script>alert(1)</script>"},
            {"color": "javascript:alert(1)"},
            {"shape": "\"><img src=x onerror=alert(1)>"},
            {"expression": "very sad"},
            {"state": "explode"},
            {"color": "ENCRE"},
            {"size": True},
            {"size": False},
            {"size": 39},
            {"size": 201},
            {"size": 56.0},
            {"size": "56"},
            {"animated": 1},
            {"animated": "true"},
            {"follow": None},
            {"snap_back": 1},
            {"snap_back": 0},
            {"snap_back": "true"},
            {"snap_back": None},
            {"actor_id": "user-a"},
            {"open_id": "ou_123"},
            {"unknown": "x"},
            {"color": "gris", "open_id": "ou_1"},
            {"color": "gris", "actor_id": "u_a", "size": 60},
        ]
        for payload in invalid:
            with self.subTest(payload=payload):
                with self.assertRaises(AssistantError) as caught:
                    save_appearance(store, "user-a", payload)
                self.assertEqual(caught.exception.status, 400)
        self.assertEqual(store.put_calls, [])

    def test_valid_partial_updates_preserve_previous_values(self):
        store = self.make_store()
        first = save_appearance(store, "user-a", {"color": "rose"})
        self.assertEqual(first["color"], "rose")
        expected_first = copy.deepcopy(DEFAULT_APPEARANCE)
        expected_first["color"] = "rose"
        self.assertEqual(first, expected_first)
        second = save_appearance(store, "user-a", {"shape": "nuage", "animated": False})
        self.assertEqual(second["color"], "rose")
        self.assertEqual(second["shape"], "nuage")
        self.assertEqual(second["animated"], False)
        self.assertEqual(second["follow"], False)
        self.assertEqual(second["snap_back"], True)
        third = save_appearance(store, "user-a", {"follow": True, "size": 44, "snap_back": False})
        self.assertEqual(third["color"], "rose")
        self.assertEqual(third["shape"], "nuage")
        self.assertEqual(third["animated"], False)
        self.assertEqual(third["follow"], True)
        self.assertEqual(third["size"], 44)
        self.assertEqual(third["snap_back"], False)

    def test_two_users_isolated_across_reads_and_reloads(self):
        docs = {}
        store_a = MemoryStore()
        store_a.docs = docs
        store_b = MemoryStore()
        store_b.docs = docs

        save_appearance(store_a, "user-a", {"color": "turquoise", "size": 60, "follow": True, "snap_back": False})
        save_appearance(store_a, "user-b", {"expression": "curieux"})

        self.assert_defaults(read_appearance(store_b, "user-c"))
        a = read_appearance(store_b, "user-a")
        b = read_appearance(store_b, "user-b")
        self.assertEqual(a["color"], "turquoise")
        self.assertEqual(a["size"], 60)
        self.assertEqual(a["follow"], True)
        self.assertEqual(a["snap_back"], False)
        self.assertEqual(b["color"], "encre")
        self.assertEqual(b["expression"], "curieux")
        self.assertEqual(b["size"], 56)
        self.assertEqual(b["snap_back"], True)

        # A fresh store instance sharing the same backing storage simulates a reload.
        store_reload = MemoryStore()
        store_reload.docs = docs
        self.assertEqual(read_appearance(store_reload, "user-a"), a)
        self.assertEqual(read_appearance(store_reload, "user-b"), b)

    def test_invalid_persisted_values_fall_back_per_field(self):
        store = self.make_store()
        store.put_document(NAMESPACE, "user-a", {
            "color": "encre",
            "shape": "NOT_A_SHAPE",
            "expression": "neutre",
            "state": "explode",
            "size": 999,
            "animated": "yes",
            "follow": 1,
            "snap_back": "maybe",
            "extra": "ignored",
            "actor_id": "forged",
        })
        appearance = read_appearance(store, "user-a")
        self.assertEqual(appearance, DEFAULT_APPEARANCE)
        self.assertEqual(appearance["shape"], DEFAULT_APPEARANCE["shape"])
        self.assertEqual(appearance["state"], DEFAULT_APPEARANCE["state"])
        self.assertEqual(appearance["size"], DEFAULT_APPEARANCE["size"])
        self.assertEqual(appearance["animated"], DEFAULT_APPEARANCE["animated"])
        self.assertEqual(appearance["follow"], DEFAULT_APPEARANCE["follow"])
        self.assertEqual(appearance["snap_back"], DEFAULT_APPEARANCE["snap_back"])
        self.assertNotIn("extra", appearance)
        self.assertNotIn("actor_id", appearance)

        store.put_document(NAMESPACE, "user-b", {
            "color": "brun",
            "shape": "goutte",
            "expression": "triste",
            "state": "sleep",
            "size": 40,
            "animated": False,
            "follow": True,
            "snap_back": False,
        })
        self.assertEqual(read_appearance(store, "user-b")["state"], "sleep")
        self.assertEqual(read_appearance(store, "user-b")["size"], 40)
        self.assertEqual(read_appearance(store, "user-b")["snap_back"], False)

    def test_save_overwrites_invalid_persisted_value(self):
        store = self.make_store()
        store.put_document(NAMESPACE, "user-a", {"color": "not-a-color", "size": 10})
        saved = save_appearance(store, "user-a", {"size": 60})
        self.assertEqual(saved["size"], 60)
        self.assertEqual(saved["color"], DEFAULT_APPEARANCE["color"])
        persisted = store.get_document(NAMESPACE, "user-a")
        self.assertEqual(persisted["size"], 60)
        self.assertEqual(persisted["color"], DEFAULT_APPEARANCE["color"])

    def test_store_write_failure_propagates(self):
        class FailingStore(MemoryStore):
            def put_document(self, namespace, key, value):
                raise OSError("storage offline")

        store = FailingStore()
        with self.assertRaises(OSError) as caught:
            save_appearance(store, "user-a", {"color": "vert"})
        self.assertIn("storage offline", str(caught.exception))

    def test_document_namespace_and_key_identity(self):
        store = AssertionStore()
        save_appearance(store, "user-a", {"color": "ambre"})
        self.assertEqual(len(store.put_calls), 1)
        namespace, key, payload = store.put_calls[0]
        self.assertEqual(namespace, NAMESPACE)
        self.assertEqual(key, "user-a")
        self.assertEqual(payload["color"], "ambre")
        self.assertEqual(payload["expression"], DEFAULT_APPEARANCE["expression"])


if __name__ == "__main__":
    unittest.main()

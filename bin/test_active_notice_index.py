import os
import sys
import unittest


current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from upload_event_module.ui.active_notice_index import ActiveNoticeIndex
from upload_event_module.ui.active_notice_store import ActiveNoticeStore


class _DummyItem:
    def __init__(self, data, valid=True):
        self._data = dict(data)
        self.valid = valid

    def data(self, _role):
        return dict(self._data)

    def set_data(self, data):
        self._data = dict(data)


class ActiveNoticeIndexTests(unittest.TestCase):
    def _build_index(self, items):
        return ActiveNoticeIndex(
            lambda item: bool(getattr(item, "valid", False)),
            compact_text_normalizer=lambda value: str(value or "").replace(" ", ""),
            match_title_normalizer=lambda value: str(value or "").strip().lower(),
        ), lambda: [(object(), item) for item in items]

    def test_lookup_by_ids_and_text(self):
        item = _DummyItem(
            {
                "record_id": "rid-1",
                "target_record_id": "rid-1",
                "active_item_id": "aid-1",
                "text": "A B C",
                "match_key": "key-1",
                "match_title": "Title One",
                "source_record_id": "src-1",
            }
        )
        index, factory = self._build_index([item])

        self.assertIs(index.find_by_record_id("rid-1", factory)[1], item)
        self.assertIs(index.find_by_active_item_id("aid-1", factory)[1], item)
        self.assertIs(index.find_by_compact_text("ABC", factory)[1], item)
        self.assertIs(index.candidates_by_exact_text("A B C", factory)[0][1], item)
        self.assertIs(index.candidates_by_source_record_id("src-1", factory)[0][1], item)
        self.assertIs(index.candidates_by_match_key("key-1", factory)[0][1], item)
        self.assertIs(index.candidates_by_match_title(" title one ", factory)[0][1], item)

    def test_record_lookup_prefers_target_id_over_source_id(self):
        item = _DummyItem(
            {
                "record_id": "src-legacy",
                "source_record_id": "src-legacy",
                "target_record_id": "target-real",
                "active_item_id": "aid-target",
                "text": "target",
            }
        )
        index, factory = self._build_index([item])

        self.assertIs(index.find_by_record_id("target-real", factory)[1], item)
        self.assertEqual(index.find_by_record_id("src-legacy", factory), (None, None))
        self.assertIs(index.candidates_by_source_record_id("src-legacy", factory)[0][1], item)

    def test_rebuilds_when_cached_entry_changes(self):
        item = _DummyItem(
            {"record_id": "old", "target_record_id": "old", "active_item_id": "aid", "text": "old"}
        )
        index, factory = self._build_index([item])

        self.assertIs(index.find_by_record_id("old", factory)[1], item)
        item.set_data(
            {"record_id": "new", "target_record_id": "new", "active_item_id": "aid", "text": "new"}
        )

        self.assertEqual(index.find_by_record_id("old", factory), (None, None))
        self.assertIs(index.find_by_record_id("new", factory)[1], item)
        self.assertIs(index.find_by_compact_text("new", factory)[1], item)

    def test_record_id_candidates_return_all_current_matches(self):
        first = _DummyItem({"record_id": "same", "target_record_id": "same", "active_item_id": "aid-1", "text": "one"})
        second = _DummyItem({"record_id": "same", "target_record_id": "same", "active_item_id": "aid-2", "text": "two"})
        stale = _DummyItem({"record_id": "same", "target_record_id": "same", "active_item_id": "aid-3", "text": "stale"})
        index, factory = self._build_index([first, second, stale])

        self.assertEqual(len(index.candidates_by_record_id("same", factory)), 3)
        stale.set_data({"record_id": "other", "target_record_id": "other", "active_item_id": "aid-3", "text": "stale"})

        candidates = index.candidates_by_record_id("same", factory)
        self.assertEqual({row[2]["active_item_id"] for row in candidates}, {"aid-1", "aid-2"})

    def test_source_record_id_candidates_skip_stale_entries(self):
        item = _DummyItem(
            {
                "record_id": "rid",
                "target_record_id": "rid",
                "active_item_id": "aid",
                "source_record_id": "source-old",
                "text": "source",
            }
        )
        index, factory = self._build_index([item])

        self.assertEqual(index.candidates_by_source_record_id("source-old", factory)[0][2]["text"], "source")
        item.set_data(
            {
                "record_id": "rid",
                "target_record_id": "rid",
                "active_item_id": "aid",
                "source_record_id": "source-new",
                "text": "source",
            }
        )

        self.assertEqual(index.candidates_by_source_record_id("source-old", factory), [])
        self.assertEqual(index.candidates_by_source_record_id("source-new", factory)[0][2]["record_id"], "rid")

    def test_snapshot_and_match_key_groups_skip_invalid_items(self):
        valid = _DummyItem(
            {
                "record_id": "rid",
                "target_record_id": "rid",
                "active_item_id": "aid",
                "text": "text",
                "match_key": "mk",
            },
            valid=True,
        )
        invalid = _DummyItem(
            {
                "record_id": "rid-invalid",
                "target_record_id": "rid-invalid",
                "active_item_id": "aid-invalid",
                "text": "hidden",
                "match_key": "mk",
            },
            valid=False,
        )
        index, factory = self._build_index([valid, invalid])

        snapshot = index.data_snapshot(factory)
        self.assertEqual({row["record_id"] for row in snapshot}, {"rid"})
        groups = index.groups_by_match_key(factory, {"mk"})
        self.assertEqual(len(groups["mk"]), 1)
        self.assertIs(groups["mk"][0][1], valid)


class ActiveNoticeStoreTests(unittest.TestCase):
    def test_facade_delegates_collection_operations(self):
        item = _DummyItem(
            {
                "record_id": "rid-store",
                "target_record_id": "rid-store",
                "active_item_id": "aid-store",
                "text": "store text",
                "match_key": "store-key",
                "match_title": "Store Title",
                "source_record_id": "store-source",
            }
        )
        store = ActiveNoticeStore(
            lambda: [(object(), item)],
            lambda candidate: bool(getattr(candidate, "valid", False)),
            compact_text_normalizer=lambda value: str(value or "").replace(" ", ""),
            match_title_normalizer=lambda value: str(value or "").lower().strip(),
        )

        self.assertIs(store.find_by_record_id("rid-store")[1], item)
        self.assertIs(store.find_by_active_item_id("aid-store")[1], item)
        self.assertIs(store.find_by_compact_text("storetext")[1], item)
        self.assertEqual(store.data_snapshot()[0]["record_id"], "rid-store")
        self.assertIn("store-key", store.groups_by_match_key({"store-key"}))
        self.assertIs(store.candidates_by_match_title("store title")[0][1], item)
        self.assertEqual(store.count(), 1)
        self.assertEqual(store.entries()[0][2]["active_item_id"], "aid-store")
        self.assertEqual(store.candidates_by_record_id("rid-store")[0][2]["text"], "store text")
        self.assertEqual(
            store.candidates_by_source_record_id("store-source")[0][2]["record_id"],
            "rid-store",
        )


if __name__ == "__main__":
    unittest.main()

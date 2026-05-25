# -*- coding: utf-8 -*-
from .active_notice_index import ActiveNoticeIndex


class ActiveNoticeStore:
    """Facade for active notice collection operations.

    Callers should use this facade instead of depending on raw QListWidget or
    model internals. Entries may be backed by widget items or model handles.
    """

    def __init__(
        self,
        iter_items_factory,
        is_valid_item,
        *,
        compact_text_normalizer=None,
        match_title_normalizer=None,
    ):
        self._iter_items_factory = iter_items_factory
        self.index = ActiveNoticeIndex(
            is_valid_item,
            compact_text_normalizer=compact_text_normalizer,
            match_title_normalizer=match_title_normalizer,
        )

    def clear(self):
        self.index.clear()

    def rebuild(self):
        self.index.rebuild(self._iter_items_factory())

    def entries(self):
        return self.index.entries(self._iter_items_factory, force=True)

    def count(self) -> int:
        return self.index.count(self._iter_items_factory)

    def data_snapshot(self) -> list[dict]:
        return self.index.data_snapshot(self._iter_items_factory)

    def groups_by_match_key(self, match_keys=None):
        return self.index.groups_by_match_key(self._iter_items_factory, match_keys)

    def find_by_record_id(self, record_id):
        return self.index.find_by_record_id(record_id, self._iter_items_factory)

    def find_by_active_item_id(self, active_item_id):
        return self.index.find_by_active_item_id(
            active_item_id,
            self._iter_items_factory,
        )

    def find_by_compact_text(self, text):
        return self.index.find_by_compact_text(text, self._iter_items_factory)

    def candidates_by_exact_text(self, text):
        return self.index.candidates_by_exact_text(text, self._iter_items_factory)

    def candidates_by_record_id(self, record_id):
        return self.index.candidates_by_record_id(record_id, self._iter_items_factory)

    def candidates_by_source_record_id(self, source_record_id):
        return self.index.candidates_by_source_record_id(
            source_record_id,
            self._iter_items_factory,
        )

    def candidates_by_match_key(self, match_key):
        return self.index.candidates_by_match_key(match_key, self._iter_items_factory)

    def candidates_by_match_title(self, match_title):
        return self.index.candidates_by_match_title(
            match_title,
            self._iter_items_factory,
        )

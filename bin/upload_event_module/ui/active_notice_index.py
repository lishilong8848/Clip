# -*- coding: utf-8 -*-
from PyQt6.QtCore import Qt

from lan_bitable_template_portal.identity_utils import (
    canonical_source_record_id,
    canonical_target_record_id,
)


class ActiveNoticeIndex:
    """Lightweight lookup index for model-backed active notice handles."""

    def __init__(
        self,
        is_valid_item,
        *,
        exact_text_normalizer=None,
        compact_text_normalizer=None,
        match_title_normalizer=None,
    ):
        self._is_valid_item = is_valid_item
        self._exact_text_normalizer = exact_text_normalizer or (
            lambda value: str(value or "").strip()
        )
        self._compact_text_normalizer = compact_text_normalizer or (
            lambda value: str(value or "").strip()
        )
        self._match_title_normalizer = match_title_normalizer or (
            lambda value: str(value or "").strip()
        )
        self._by_record_id = {}
        self._by_record_id_candidates = {}
        self._by_active_item_id = {}
        self._by_source_record_id = {}
        self._by_exact_text = {}
        self._by_compact_text = {}
        self._by_match_key = {}
        self._by_match_title = {}
        self._entries = []
        self._built = False

    def clear(self):
        self._by_record_id = {}
        self._by_record_id_candidates = {}
        self._by_active_item_id = {}
        self._by_source_record_id = {}
        self._by_exact_text = {}
        self._by_compact_text = {}
        self._by_match_key = {}
        self._by_match_title = {}
        self._entries = []
        self._built = False

    def rebuild(self, iter_items):
        by_record_id = {}
        by_record_id_candidates = {}
        by_active_item_id = {}
        by_source_record_id = {}
        by_exact_text = {}
        by_compact_text = {}
        by_match_key = {}
        by_match_title = {}
        entries = []
        for list_widget, item in iter_items:
            if not self._entry_is_valid((list_widget, item)):
                continue
            try:
                data = item.data(Qt.ItemDataRole.UserRole)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            data_copy = dict(data)
            entries.append((list_widget, item, data_copy))
            record_id = canonical_target_record_id(data)
            active_item_id = str(data.get("active_item_id") or "").strip()
            source_record_id = canonical_source_record_id(data)
            if record_id:
                by_record_id[record_id] = (list_widget, item)
                by_record_id_candidates.setdefault(record_id, []).append(
                    (list_widget, item, data_copy)
                )
            if active_item_id:
                by_active_item_id[active_item_id] = (list_widget, item)
            if source_record_id:
                self._append_candidate(
                    by_source_record_id,
                    source_record_id,
                    (list_widget, item, data_copy),
                )
            self._append_candidate(
                by_exact_text,
                self._exact_text_normalizer(data.get("text")),
                (list_widget, item, data_copy),
            )
            self._append_candidate(
                by_compact_text,
                self._compact_text_normalizer(data.get("text")),
                (list_widget, item, data_copy),
            )
            self._append_candidate(
                by_match_key,
                str(data.get("match_key") or "").strip(),
                (list_widget, item, data_copy),
            )
            self._append_candidate(
                by_match_title,
                self._match_title_normalizer(data.get("match_title")),
                (list_widget, item, data_copy),
            )
        self._by_record_id = by_record_id
        self._by_record_id_candidates = by_record_id_candidates
        self._by_active_item_id = by_active_item_id
        self._by_source_record_id = by_source_record_id
        self._by_exact_text = by_exact_text
        self._by_compact_text = by_compact_text
        self._by_match_key = by_match_key
        self._by_match_title = by_match_title
        self._entries = entries
        self._built = True

    @staticmethod
    def _append_candidate(mapping, key, entry):
        key = str(key or "").strip()
        if not key:
            return
        mapping.setdefault(key, []).append(entry)

    def entries(self, iter_items_factory, *, force: bool = False):
        if force or not self._built:
            self.rebuild(iter_items_factory())
        return list(self._entries)

    def count(self, iter_items_factory) -> int:
        return len(self.entries(iter_items_factory, force=True))

    def data_snapshot(self, iter_items_factory) -> list[dict]:
        return [
            dict(data)
            for _, _, data in self.entries(iter_items_factory, force=True)
        ]

    def groups_by_match_key(self, iter_items_factory, match_keys=None):
        limited_keys = {
            str(key or "").strip()
            for key in (match_keys or set())
            if str(key or "").strip()
        }
        if match_keys is not None and not limited_keys:
            return {}
        groups = {}
        for list_widget, item, data in self.entries(iter_items_factory, force=True):
            if not self._entry_is_valid((list_widget, item)):
                continue
            match_key = str(data.get("match_key") or "").strip()
            if not match_key:
                continue
            if limited_keys and match_key not in limited_keys:
                continue
            groups.setdefault(match_key, []).append((list_widget, item, dict(data)))
        return groups

    def find_by_compact_text(self, text, iter_items_factory):
        candidates = self.candidates_by_compact_text(text, iter_items_factory)
        if not candidates:
            return None, None
        return candidates[0][0], candidates[0][1]

    def candidates_by_exact_text(self, text, iter_items_factory):
        key = self._exact_text_normalizer(text)
        return self._candidates("exact_text", key, iter_items_factory)

    def candidates_by_compact_text(self, text, iter_items_factory):
        key = self._compact_text_normalizer(text)
        return self._candidates("compact_text", key, iter_items_factory)

    def candidates_by_record_id(self, record_id, iter_items_factory):
        record_id = str(record_id or "").strip()
        if not record_id:
            return []
        if not self._built:
            self.rebuild(iter_items_factory())
        candidates = self._valid_candidate_entries(
            self._by_record_id_candidates.get(record_id) or [],
            record_id,
            "record_id",
        )
        if candidates:
            return candidates
        self.rebuild(iter_items_factory())
        return self._valid_candidate_entries(
            self._by_record_id_candidates.get(record_id) or [],
            record_id,
            "record_id",
        )

    def candidates_by_source_record_id(self, source_record_id, iter_items_factory):
        return self._candidates(
            "source_record_id",
            str(source_record_id or "").strip(),
            iter_items_factory,
        )

    def candidates_by_match_key(self, match_key, iter_items_factory):
        return self._candidates(
            "match_key",
            str(match_key or "").strip(),
            iter_items_factory,
        )

    def candidates_by_match_title(self, match_title, iter_items_factory):
        key = self._match_title_normalizer(match_title)
        return self._candidates("match_title", key, iter_items_factory)

    def find_by_record_id(self, record_id, iter_items_factory):
        return self._find(
            str(record_id or "").strip(),
            "record_id",
            self._by_record_id,
            iter_items_factory,
        )

    def find_by_active_item_id(self, active_item_id, iter_items_factory):
        return self._find(
            str(active_item_id or "").strip(),
            "active_item_id",
            self._by_active_item_id,
            iter_items_factory,
        )

    def _find(self, key, field, index, iter_items_factory):
        if not key:
            return None, None
        if not self._built:
            self.rebuild(iter_items_factory())
            index = self._index_for_field(field)
        cached = index.get(key)
        if cached and self._entry_matches(cached, key, field):
            return cached
        self.rebuild(iter_items_factory())
        cached = self._index_for_field(field).get(key)
        if cached and self._entry_matches(cached, key, field):
            return cached
        return None, None

    def _candidates(self, source, key, iter_items_factory):
        key = str(key or "").strip()
        if not key:
            return []
        if not self._built:
            self.rebuild(iter_items_factory())
        mapping = self._mapping_for_source(source)
        entries = mapping.get(key) or []
        candidates = self._valid_candidate_entries(entries, key, source)
        if candidates:
            return candidates
        self.rebuild(iter_items_factory())
        return self._valid_candidate_entries(
            (self._mapping_for_source(source).get(key) or []),
            key,
            source,
        )

    def _mapping_for_source(self, source):
        if source == "compact_text":
            return self._by_compact_text
        if source == "source_record_id":
            return self._by_source_record_id
        if source == "match_key":
            return self._by_match_key
        if source == "match_title":
            return self._by_match_title
        return self._by_exact_text

    def _valid_candidate_entries(self, entries, key, source):
        candidates = []
        for entry in entries:
            current = self._current_candidate_entry(entry)
            if current and self._candidate_matches_source(current[2], key, source):
                candidates.append(current)
        return candidates

    def _candidate_matches_source(self, data, key, source) -> bool:
        if not isinstance(data, dict):
            return False
        if source == "compact_text":
            value = self._compact_text_normalizer(data.get("text"))
        elif source == "record_id":
            value = canonical_target_record_id(data)
        elif source == "source_record_id":
            value = canonical_source_record_id(data)
        elif source == "match_key":
            value = str(data.get("match_key") or "").strip()
        elif source == "match_title":
            value = self._match_title_normalizer(data.get("match_title"))
        else:
            value = self._exact_text_normalizer(data.get("text"))
        return str(value or "").strip() == str(key or "").strip()

    def _index_for_field(self, field):
        if field == "active_item_id":
            return self._by_active_item_id
        return self._by_record_id

    def _entry_is_valid(self, entry) -> bool:
        try:
            list_widget, item = entry
        except Exception:
            return False
        return list_widget is not None and item is not None and self._is_valid_item(item)

    def _current_candidate_entry(self, entry):
        try:
            list_widget, item, _data = entry
        except Exception:
            return None
        if not self._entry_is_valid((list_widget, item)):
            return None
        try:
            current_data = item.data(Qt.ItemDataRole.UserRole)
        except Exception:
            return None
        if not isinstance(current_data, dict):
            return None
        return list_widget, item, dict(current_data)

    def _entry_matches(self, entry, key, field) -> bool:
        try:
            list_widget, item = entry
        except Exception:
            return False
        if list_widget is None or item is None or not self._is_valid_item(item):
            return False
        try:
            data = item.data(Qt.ItemDataRole.UserRole)
        except Exception:
            return False
        if not isinstance(data, dict):
            return False
        if field == "record_id":
            value = canonical_target_record_id(data)
        elif field == "source_record_id":
            value = canonical_source_record_id(data)
        else:
            value = str(data.get(field) or "").strip()
        return str(value or "").strip() == str(key or "").strip()

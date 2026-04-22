# -*- coding: utf-8 -*-
import json
import os
import time

from PyQt6.QtCore import Qt, QTimer

from ..core.parser import extract_event_info
from ..utils import ACTIVE_CACHE_FILE
from .display_state import normalize_active_item_data


class ActiveCacheMixin:
    def _is_ended_active_cache_record(self, data):
        if not isinstance(data, dict):
            return False
        if bool(data.get("_ended_moved")):
            return True
        info = extract_event_info(str(data.get("text") or ""))
        return bool(info and info.get("status") == "结束")

    def _history_contains_record(self, data):
        if not isinstance(data, dict) or not hasattr(self, "load_all_history"):
            return False
        record_id = str(data.get("record_id") or "").strip()
        text = str(data.get("text") or "").strip()
        try:
            history_data = self.load_all_history()
        except Exception:
            return False
        if not isinstance(history_data, list):
            return False
        for item in history_data:
            if not isinstance(item, dict):
                continue
            history_record_id = str(item.get("record_id") or "").strip()
            if record_id and history_record_id and history_record_id == record_id:
                return True
            if not record_id and text and str(item.get("text") or "").strip() == text:
                return True
        return False

    def _init_active_cache_timer(self):
        self.active_cache_timer = QTimer(self)
        self.active_cache_timer.timeout.connect(self.save_active_cache)
        self.active_cache_timer.start(5000)

    def _collect_active_list_cache(self, list_widget):
        items = []
        for i in range(list_widget.count()):
            item = list_widget.item(i)
            data = item.data(Qt.ItemDataRole.UserRole)
            if not isinstance(data, dict):
                continue
            source_data = dict(data)
            record_id = str(source_data.get("record_id") or "").strip()
            store = getattr(self, "cache_store", None)
            if store and record_id:
                try:
                    cache_fields = store.get_record_fields(
                        record_id=record_id,
                        fields=["level", "level_locked"],
                    )
                except Exception:
                    cache_fields = {}
                if bool(cache_fields.get("level_locked")):
                    source_data["level_locked"] = True
                    cached_level = str(cache_fields.get("level") or "").strip()
                    if cached_level:
                        source_data["level"] = cached_level
                    else:
                        source_data.pop("level", None)
            cleaned_data = normalize_active_item_data(source_data)
            cleaned_data.pop("last_response_time", None)
            cleaned_data.pop("draft_response_time", None)
            if "_has_unuploaded_changes" not in cleaned_data:
                cleaned_data["_has_unuploaded_changes"] = True
            items.append({"data": cleaned_data})
        return items

    def _collect_active_cache(self):
        payload = {
            "version": 2,
            "saved_at": int(time.time()),
            "event": self._collect_active_list_cache(self.list_active_event),
            "other": self._collect_active_list_cache(self.list_active_other),
        }
        if hasattr(self, "_get_clipboard_cache_payload"):
            try:
                payload["clipboard_queue"] = self._get_clipboard_cache_payload()
            except Exception:
                payload["clipboard_queue"] = []
        return payload

    def save_active_cache(self):
        if self._is_restoring_cache:
            return
        payload = self._collect_active_cache()
        has_clipboard_queue = bool(payload.get("clipboard_queue"))
        if not payload["event"] and not payload["other"] and not has_clipboard_queue:
            if os.path.exists(ACTIVE_CACHE_FILE):
                try:
                    os.remove(ACTIVE_CACHE_FILE)
                except Exception:
                    pass
            return
        try:
            store = getattr(self, "cache_store", None)
            if store:
                store.replace_payload(payload)
            else:
                with open(ACTIVE_CACHE_FILE, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _restore_active_item(self, payload):
        if not payload:
            return False
        if isinstance(payload, dict) and "data" in payload:
            data = payload.get("data") or {}
            meta = payload.get("meta") or {}
        else:
            data = payload if isinstance(payload, dict) else {}
            meta = {}
        if not data:
            return False
        data = normalize_active_item_data(data)
        if self._is_ended_active_cache_record(data):
            if hasattr(self, "save_to_history_file") and not self._history_contains_record(
                data
            ):
                try:
                    self.save_to_history_file(data)
                except Exception:
                    pass
            return True
        if "_has_unuploaded_changes" not in data and "has_unuploaded_changes" in meta:
            data["_has_unuploaded_changes"] = bool(meta.get("has_unuploaded_changes"))
        if "_has_unuploaded_changes" not in data:
            data["_has_unuploaded_changes"] = True
        if "_has_unuploaded_changes" in data and not data["_has_unuploaded_changes"]:
            data["_pending_upload_hash"] = None
            data["_upload_in_progress"] = False
        item, widget = self.add_active_item(data, insert_top=False, skip_cache=True)
        if widget:
            if not data.get("_has_unuploaded_changes", True):
                widget.mark_as_uploaded()
        return False

    def _restore_active_cache(self):
        store = getattr(self, "cache_store", None)
        if store:
            payload = store.load_payload()
        else:
            if not os.path.exists(ACTIVE_CACHE_FILE):
                return
            try:
                with open(ACTIVE_CACHE_FILE, "r", encoding="utf-8") as f:
                    payload = json.load(f)
            except Exception:
                return
        self._is_restoring_cache = True
        cache_changed = False
        try:
            event_items = payload.get("event", []) if isinstance(payload, dict) else []
            other_items = payload.get("other", []) if isinstance(payload, dict) else []
            for item_payload in event_items:
                cache_changed = self._restore_active_item(item_payload) or cache_changed
            for item_payload in other_items:
                cache_changed = self._restore_active_item(item_payload) or cache_changed
            if hasattr(self, "_set_clipboard_cache_payload"):
                try:
                    self._set_clipboard_cache_payload(
                        payload.get("clipboard_queue", []) if isinstance(payload, dict) else []
                    )
                except Exception:
                    pass
        finally:
            self._is_restoring_cache = False
        if cache_changed:
            try:
                self.save_active_cache()
            except Exception:
                pass

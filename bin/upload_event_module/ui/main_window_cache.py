# -*- coding: utf-8 -*-
import json
import os
import hashlib
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
        self._active_cache_last_signature = ""
        self._active_cache_last_save_at = 0.0
        self._active_cache_save_deferred = False
        self._active_cache_dirty = True
        self._active_cache_periodic_full_scan_seconds = 300.0
        self.active_cache_timer = QTimer(self)
        self.active_cache_timer.timeout.connect(self._periodic_active_cache_save)
        self.active_cache_timer.start(30000)
        self._active_cache_delayed_save_timer = QTimer(self)
        self._active_cache_delayed_save_timer.setSingleShot(True)
        self._active_cache_delayed_save_timer.timeout.connect(self.save_active_cache)

    def _mark_active_cache_dirty(self):
        self._active_cache_dirty = True

    def _post_qt_active_items_delta(self, *, upserts=None, deletes=None):
        controller = getattr(self, "lan_template_portal_controller", None)
        if not controller or not hasattr(controller, "post_active_items_delta"):
            return
        try:
            controller.post_active_items_delta(
                upserts=list(upserts or []),
                deletes=list(deletes or []),
            )
        except Exception:
            return

    def _periodic_active_cache_save(self):
        if self._is_restoring_cache:
            return
        now = time.time()
        last_save_at = float(getattr(self, "_active_cache_last_save_at", 0.0) or 0.0)
        full_scan_interval = float(
            getattr(self, "_active_cache_periodic_full_scan_seconds", 300.0) or 300.0
        )
        if (
            not bool(getattr(self, "_active_cache_dirty", True))
            and last_save_at
            and now - last_save_at < full_scan_interval
        ):
            return
        self.save_active_cache()

    def _locked_level_map_for_active_cache(self):
        store = getattr(self, "cache_store", None)
        if not store or not hasattr(store, "get_locked_level_map"):
            return {}
        try:
            return store.get_locked_level_map() or {}
        except Exception:
            return {}

    def _upsert_active_cache_record(self, data_dict):
        if self._is_restoring_cache or not isinstance(data_dict, dict):
            return False
        store = getattr(self, "cache_store", None)
        if not store or not hasattr(store, "upsert_record"):
            return False
        try:
            if store.upsert_record(data_dict):
                self._active_cache_last_save_at = time.time()
                self._active_cache_dirty = False
                if hasattr(self, "_schedule_lan_ongoing_snapshot_refresh"):
                    self._schedule_lan_ongoing_snapshot_refresh()
                self._post_qt_active_items_delta(upserts=[{"data": data_dict}])
                return True
        except Exception:
            return False
        return False

    def _delete_active_cache_record(self, data_dict):
        if self._is_restoring_cache or not isinstance(data_dict, dict):
            return False
        store = getattr(self, "cache_store", None)
        if not store or not hasattr(store, "delete_record"):
            return False
        try:
            if store.delete_record(
                record_id=str(data_dict.get("record_id") or ""),
                active_item_id=str(data_dict.get("active_item_id") or ""),
            ):
                self._active_cache_last_save_at = time.time()
                self._active_cache_dirty = False
                if hasattr(self, "_schedule_lan_ongoing_snapshot_refresh"):
                    self._schedule_lan_ongoing_snapshot_refresh()
                self._post_qt_active_items_delta(
                    deletes=[
                        {
                            "record_id": str(data_dict.get("record_id") or ""),
                            "active_item_id": str(data_dict.get("active_item_id") or ""),
                        }
                    ]
                )
                return True
        except Exception:
            return False
        return False

    def _collect_active_list_cache(self, list_widget, locked_level_map=None):
        locked_level_map = locked_level_map or {}
        items = []
        for i in range(list_widget.count()):
            item = list_widget.item(i)
            data = item.data(Qt.ItemDataRole.UserRole)
            if not isinstance(data, dict):
                continue
            source_data = dict(data)
            record_id = str(source_data.get("record_id") or "").strip()
            if record_id:
                cache_fields = locked_level_map.get(record_id) or {}
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
        locked_level_map = self._locked_level_map_for_active_cache()
        payload = {
            "version": 2,
            "saved_at": int(time.time()),
            "event": self._collect_active_list_cache(
                self.list_active_event, locked_level_map
            ),
            "other": self._collect_active_list_cache(
                self.list_active_other, locked_level_map
            ),
        }
        if hasattr(self, "_get_clipboard_cache_payload"):
            try:
                payload["clipboard_queue"] = self._get_clipboard_cache_payload()
            except Exception:
                payload["clipboard_queue"] = []
        return payload

    @staticmethod
    def _active_cache_signature(payload: dict) -> str:
        comparable = dict(payload or {})
        comparable.pop("saved_at", None)
        raw = json.dumps(
            comparable,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    def save_active_cache(self):
        if self._is_restoring_cache:
            return
        if int(getattr(self, "_defer_active_cache_save_count", 0) or 0) > 0:
            self._active_cache_save_deferred = True
            self._mark_active_cache_dirty()
            return
        payload = self._collect_active_cache()
        signature = self._active_cache_signature(payload)
        if signature == getattr(self, "_active_cache_last_signature", ""):
            self._active_cache_dirty = False
            return
        has_clipboard_queue = bool(payload.get("clipboard_queue"))
        if not payload["event"] and not payload["other"] and not has_clipboard_queue:
            store = getattr(self, "cache_store", None)
            if store:
                try:
                    if store.replace_payload(payload):
                        self._active_cache_last_signature = signature
                        self._active_cache_last_save_at = time.time()
                        self._active_cache_dirty = False
                except Exception:
                    pass
            if hasattr(self, "_schedule_lan_ongoing_snapshot_refresh"):
                self._schedule_lan_ongoing_snapshot_refresh()
            return
        try:
            store = getattr(self, "cache_store", None)
            if store:
                if store.replace_payload(payload):
                    self._active_cache_last_signature = signature
                    self._active_cache_last_save_at = time.time()
                    self._active_cache_dirty = False
            if hasattr(self, "_schedule_lan_ongoing_snapshot_refresh"):
                self._schedule_lan_ongoing_snapshot_refresh()
        except Exception:
            pass

    def schedule_active_cache_save(self, delay_ms: int = 800):
        if self._is_restoring_cache:
            return
        self._mark_active_cache_dirty()
        if int(getattr(self, "_defer_active_cache_save_count", 0) or 0) > 0:
            self._active_cache_save_deferred = True
            return
        timer = getattr(self, "_active_cache_delayed_save_timer", None)
        if timer is None:
            QTimer.singleShot(max(0, int(delay_ms or 0)), self.save_active_cache)
            return
        timer.start(max(0, int(delay_ms or 0)))

    def _begin_defer_active_cache_save(self):
        self._mark_active_cache_dirty()
        self._defer_active_cache_save_count = (
            int(getattr(self, "_defer_active_cache_save_count", 0) or 0) + 1
        )

    def _end_defer_active_cache_save(self):
        count = int(getattr(self, "_defer_active_cache_save_count", 0) or 0)
        self._defer_active_cache_save_count = max(0, count - 1)
        if self._defer_active_cache_save_count:
            return
        if not bool(getattr(self, "_active_cache_save_deferred", False)):
            return
        self._active_cache_save_deferred = False
        self.schedule_active_cache_save(800)

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

    @staticmethod
    def _active_cache_restore_batch_size():
        try:
            value = int(os.environ.get("CLIPFLOW_ACTIVE_RESTORE_BATCH_SIZE", "5") or 5)
        except Exception:
            value = 5
        return max(1, min(value, 20))

    @staticmethod
    def _active_cache_restore_async_threshold():
        try:
            value = int(
                os.environ.get("CLIPFLOW_ACTIVE_RESTORE_ASYNC_THRESHOLD", "25") or 25
            )
        except Exception:
            value = 25
        return max(1, min(value, 500))

    @staticmethod
    def _active_cache_restore_batch_interval_ms():
        try:
            value = int(
                os.environ.get("CLIPFLOW_ACTIVE_RESTORE_BATCH_INTERVAL_MS", "80") or 80
            )
        except Exception:
            value = 80
        return max(10, min(value, 1000))

    def _set_clipboard_cache_from_payload(self, payload):
        if not hasattr(self, "_set_clipboard_cache_payload"):
            return
        try:
            self._set_clipboard_cache_payload(
                payload.get("clipboard_queue", []) if isinstance(payload, dict) else []
            )
        except Exception:
            pass

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
            restore_items = list(event_items or []) + list(other_items or [])
            self._set_clipboard_cache_from_payload(payload)
            if len(restore_items) > self._active_cache_restore_async_threshold():
                self._active_cache_restore_in_progress = True
                self._active_cache_restore_queue = restore_items
                self._active_cache_restore_cache_changed = False
                QTimer.singleShot(0, self._restore_active_cache_batch)
                return
            for item_payload in restore_items:
                cache_changed = self._restore_active_item(item_payload) or cache_changed
        finally:
            if not bool(getattr(self, "_active_cache_restore_in_progress", False)):
                self._is_restoring_cache = False
        if cache_changed:
            try:
                self.save_active_cache()
            except Exception:
                pass

    def _restore_active_cache_batch(self):
        if getattr(self, "_closing", False):
            self._active_cache_restore_queue = []
            self._active_cache_restore_in_progress = False
            self._is_restoring_cache = False
            return
        self._is_restoring_cache = True
        batch_size = self._active_cache_restore_batch_size()
        queue_items = list(getattr(self, "_active_cache_restore_queue", []) or [])
        remaining = queue_items[batch_size:]
        current_batch = queue_items[:batch_size]
        cache_changed = bool(getattr(self, "_active_cache_restore_cache_changed", False))
        try:
            for item_payload in current_batch:
                cache_changed = self._restore_active_item(item_payload) or cache_changed
        finally:
            self._active_cache_restore_queue = remaining
            self._active_cache_restore_cache_changed = cache_changed
        if remaining:
            QTimer.singleShot(
                self._active_cache_restore_batch_interval_ms(),
                self._restore_active_cache_batch,
            )
            return
        self._active_cache_restore_in_progress = False
        self._is_restoring_cache = False
        if cache_changed:
            try:
                self.save_active_cache()
            except Exception:
                pass
        self._finalize_active_cache_restore_startup()

    def _finalize_active_cache_restore_startup(self):
        if bool(getattr(self, "_active_cache_restore_finalize_done", False)):
            return
        if bool(getattr(self, "_active_cache_restore_in_progress", False)):
            QTimer.singleShot(200, self._finalize_active_cache_restore_startup)
            return
        self._active_cache_restore_finalize_done = True
        self._repair_missing_item_widgets()
        if hasattr(self, "_sync_all_active_notice_models"):
            self._sync_all_active_notice_models()
        self._reconcile_active_route_duplicates()
        self._schedule_today_in_progress_sync_on_startup()
        self._validate_record_bindings_on_startup()

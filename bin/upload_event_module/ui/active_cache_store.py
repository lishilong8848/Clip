import json
import os
import threading
import time
import uuid
from typing import Any

from lan_bitable_template_portal.identity_utils import (
    canonical_target_record_id,
    normalize_notice_identity_payload,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore

from ..logger import log_warning
from ..building_normalizer import normalize_buildings_value as normalize_buildings_list
from .display_state import (
    detect_level_from_notice_text,
    normalize_active_item_data,
    notice_supports_level_lock,
)


class ActiveCacheStore:
    """Thread-safe SQLite-backed store wrapper for active notice cache."""

    _SECTIONS = ("event", "other")
    _STATE_NAMESPACE = "qt_active_cache"
    _STATE_KEY = "active_cache"

    def __init__(self, cache_file: str, state_store: LanPortalStateStore | None = None):
        self.cache_file = str(cache_file or "")
        self._state_store = state_store or LanPortalStateStore()
        self._lock = threading.RLock()
        self._legacy_file_migration_result: dict[str, Any] | None = None

    def _default_payload(self) -> dict:
        return {
            "version": 2,
            "saved_at": int(time.time()),
            "event": [],
            "other": [],
            "clipboard_queue": [],
        }

    @staticmethod
    def _normalize_key(value: Any) -> str:
        return str(value or "").strip()

    def _load_payload_unlocked(self) -> dict:
        try:
            qt_items = self._state_store.list_qt_active_items()
        except Exception:
            qt_items = []
        qt_items_initialized = bool(qt_items)
        if not qt_items_initialized:
            try:
                qt_items_initialized = bool(
                    self._state_store.list_qt_active_items(include_deleted=True)
                )
            except Exception:
                qt_items_initialized = False
        if qt_items_initialized:
            payload = self._default_payload()
            try:
                stored = self._state_store.get_document(
                    self._STATE_NAMESPACE, self._STATE_KEY
                )
            except Exception:
                stored = None
            if isinstance(stored, dict) and isinstance(stored.get("clipboard_queue"), list):
                payload["clipboard_queue"] = list(stored.get("clipboard_queue") or [])
            for item in qt_items:
                if not isinstance(item, dict):
                    continue
                section = str(item.get("section") or "other").strip()
                if section not in self._SECTIONS:
                    section = "other"
                data = item.get("payload")
                if not isinstance(data, dict):
                    continue
                normalized = dict(data)
                active_item_id = str(item.get("active_item_id") or "").strip()
                if active_item_id and not str(normalized.get("active_item_id") or "").strip():
                    normalized["active_item_id"] = active_item_id
                payload[section].append({"data": normalized})
            return self._normalize_payload(payload)
        try:
            stored = self._state_store.get_document(
                self._STATE_NAMESPACE, self._STATE_KEY
            )
        except Exception:
            stored = None
        if isinstance(stored, dict):
            payload = self._normalize_payload(stored)
            try:
                self._state_store.replace_qt_active_items_from_payload(payload)
            except Exception:
                pass
            return payload
        if not self.cache_file or not os.path.exists(self.cache_file):
            return self._default_payload()
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if not isinstance(payload, dict):
                return self._default_payload()
            payload = self._normalize_payload(payload)
            try:
                self._state_store.put_document(
                    self._STATE_NAMESPACE, self._STATE_KEY, payload
                )
                self._state_store.replace_qt_active_items_from_payload(payload)
            except Exception:
                pass
            return payload
        except Exception:
            return self._default_payload()

    def _normalize_payload(self, payload: dict) -> dict:
        payload = dict(payload or {})
        payload["version"] = int(payload.get("version") or 2)
        for section in self._SECTIONS:
            if not isinstance(payload.get(section), list):
                payload[section] = []
        if not isinstance(payload.get("clipboard_queue"), list):
            payload["clipboard_queue"] = []
        return payload

    def _count_payload_items(self, payload: dict) -> int:
        total = 0
        for section in self._SECTIONS:
            values = payload.get(section, []) if isinstance(payload, dict) else []
            total += len(values) if isinstance(values, list) else 0
        return total

    def _migrate_legacy_cache_file_unlocked(self) -> dict[str, Any]:
        if self._legacy_file_migration_result is not None:
            return dict(self._legacy_file_migration_result)
        result = {
            "status": "skipped",
            "imported": 0,
            "path": self.cache_file,
            "reason": "",
        }
        if not self.cache_file or not os.path.exists(self.cache_file):
            result["reason"] = "missing_legacy_file"
            self._legacy_file_migration_result = result
            return dict(result)
        try:
            existing_doc = self._state_store.get_document(
                self._STATE_NAMESPACE, self._STATE_KEY
            )
        except Exception:
            existing_doc = None
        try:
            existing_qt_items = self._state_store.list_qt_active_items(include_deleted=True)
        except Exception:
            existing_qt_items = []
        if isinstance(existing_doc, dict) or existing_qt_items:
            result["reason"] = "sqlite_already_initialized"
            self._legacy_file_migration_result = result
            return dict(result)
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as exc:
            result["status"] = "failed"
            result["reason"] = f"read_failed: {exc}"
            self._legacy_file_migration_result = result
            return dict(result)
        if not isinstance(payload, dict):
            result["status"] = "failed"
            result["reason"] = "invalid_payload"
            self._legacy_file_migration_result = result
            return dict(result)
        payload = self._normalize_payload(payload)
        try:
            self._state_store.put_document(self._STATE_NAMESPACE, self._STATE_KEY, payload)
            migrated = self._state_store.replace_qt_active_items_from_payload(payload)
        except Exception as exc:
            result["status"] = "failed"
            result["reason"] = f"sqlite_import_failed: {exc}"
            self._legacy_file_migration_result = result
            return dict(result)
        imported = int((migrated or {}).get("upserted") or self._count_payload_items(payload))
        result["status"] = "imported"
        result["imported"] = imported
        result["reason"] = ""
        self._legacy_file_migration_result = result
        return dict(result)

    def migrate_legacy_cache_file_to_sqlite(self) -> dict[str, Any]:
        with self._lock:
            return self._migrate_legacy_cache_file_unlocked()

    def _payload_from_qt_items_unlocked(self) -> dict:
        payload = self._default_payload()
        try:
            stored = self._state_store.get_document(
                self._STATE_NAMESPACE, self._STATE_KEY
            )
        except Exception:
            stored = None
        if isinstance(stored, dict) and isinstance(stored.get("clipboard_queue"), list):
            payload["clipboard_queue"] = list(stored.get("clipboard_queue") or [])
        try:
            qt_items = self._state_store.list_qt_active_items()
        except Exception:
            qt_items = []
        for item in qt_items:
            if not isinstance(item, dict):
                continue
            section = str(item.get("section") or "other").strip()
            if section not in self._SECTIONS:
                section = "other"
            data = item.get("payload")
            if isinstance(data, dict):
                payload[section].append({"data": dict(data)})
        return self._normalize_payload(payload)

    def _sync_legacy_document_from_qt_items_unlocked(self) -> None:
        payload = self._payload_from_qt_items_unlocked()
        payload["saved_at"] = int(time.time())
        self._state_store.put_document(self._STATE_NAMESPACE, self._STATE_KEY, payload)

    def load_payload(self) -> dict:
        with self._lock:
            self._migrate_legacy_cache_file_unlocked()
            return self._load_payload_unlocked()

    def _save_payload_unlocked(self, payload: dict) -> bool:
        normalized = self._normalize_payload(payload)
        try:
            self._state_store.replace_qt_active_items_from_payload(normalized)
        except Exception as exc:
            log_warning(f"Qt active items 规范表写入失败: {exc}")
            return False
        try:
            self._state_store.put_document(
                self._STATE_NAMESPACE, self._STATE_KEY, normalized
            )
            return True
        except Exception as exc:
            log_warning(f"Qt active cache 兼容文档写入失败: {exc}")
            return False

    def save_payload(self, payload: dict) -> bool:
        with self._lock:
            payload_to_save = payload if isinstance(payload, dict) else self._default_payload()
            payload_to_save["saved_at"] = int(time.time())
            return self._save_payload_unlocked(payload_to_save)

    def _iter_record_entries_unlocked(self, payload: dict):
        for section in self._SECTIONS:
            section_items = payload.get(section, [])
            if not isinstance(section_items, list):
                continue
            for entry in section_items:
                if not isinstance(entry, dict):
                    continue
                data = entry.get("data")
                if not isinstance(data, dict):
                    continue
                yield section, entry, data

    def _new_record_id_unlocked(self, existing_ids: set[str] | None = None) -> str:
        existing_ids = existing_ids or set()
        while True:
            new_id = uuid.uuid4().hex
            if new_id not in existing_ids:
                return new_id

    @classmethod
    def _record_matches_target_id(cls, data: dict | None, target_id: str = "") -> bool:
        if not isinstance(data, dict):
            return False
        target_id = cls._normalize_key(target_id)
        if not target_id:
            return False
        canonical = cls._normalize_key(canonical_target_record_id(data))
        if canonical:
            return canonical == target_id
        legacy = cls._normalize_key(data.get("record_id"))
        return bool(legacy and legacy == target_id)

    @staticmethod
    def _section_for_data(data: dict) -> str:
        notice_type = str((data or {}).get("notice_type") or "").strip()
        return "event" if notice_type == "事件通告" else "other"

    def _find_record_by_id_unlocked(self, payload: dict, record_id: str = ""):
        rid = self._normalize_key(record_id)
        if not rid:
            return None, None, None, "missing_id"
        matched: list[tuple[str, dict, dict]] = []
        for section, entry, data in self._iter_record_entries_unlocked(payload):
            if self._record_matches_target_id(data, rid):
                matched.append((section, entry, data))
        if not matched:
            return None, None, None, "not_found"
        if len(matched) > 1:
            return None, None, None, "conflict"
        section, entry, data = matched[0]
        return section, entry, data, None

    def find_record(self, record_id: str = "") -> dict | None:
        with self._lock:
            payload = self._load_payload_unlocked()
            _, _, data, err = self._find_record_by_id_unlocked(payload, record_id)
            if err:
                return None
            return dict(data) if isinstance(data, dict) else None

    def get_record_fields(
        self,
        record_id: str = "",
        fields: list[str] | tuple[str, ...] | None = None,
    ) -> dict:
        if not fields:
            return {}
        with self._lock:
            payload = self._load_payload_unlocked()
            _, _, data, err = self._find_record_by_id_unlocked(payload, record_id)
            if err:
                return {}
            if not isinstance(data, dict):
                return {}
            result = {}
            for key in fields:
                if key in data:
                    result[key] = data.get(key)
            return result

    def get_locked_level_map(self) -> dict[str, dict[str, Any]]:
        """Return level-lock fields for all cached active records in one read."""
        with self._lock:
            payload = self._load_payload_unlocked()
            result: dict[str, dict[str, Any]] = {}
            for _, _, data in self._iter_record_entries_unlocked(payload):
                if not isinstance(data, dict):
                    continue
                record_id = self._normalize_key(
                    canonical_target_record_id(data) or data.get("record_id")
                )
                if not record_id or not bool(data.get("level_locked")):
                    continue
                result[record_id] = {
                    "level": data.get("level"),
                    "level_locked": True,
                }
            return result

    def patch_record_fields(
        self,
        record_id: str = "",
        patch: dict | None = None,
    ) -> bool:
        if not isinstance(patch, dict) or not patch:
            return False
        rid = self._normalize_key(record_id)
        if rid:
            with self._lock:
                try:
                    matches = [
                        item
                        for item in self._state_store.list_qt_active_items()
                        if self._record_matches_target_id(item.get("payload") or {}, rid)
                    ]
                except Exception:
                    matches = []
                if len(matches) == 1:
                    item = matches[0]
                    data = dict(item.get("payload") or {})
                    changed = False
                    for key, value in patch.items():
                        old_value = data.get(key, None)
                        if value is None:
                            if key in data:
                                data.pop(key, None)
                                changed = True
                            continue
                        if old_value != value:
                            data[key] = value
                            changed = True
                    if not changed:
                        return False
                    saved = self._state_store.upsert_qt_active_item(
                        data,
                        section=str(item.get("section") or ""),
                        sort_order=int(item.get("sort_order") or 0),
                        origin=str(item.get("origin") or ""),
                    )
                    if saved:
                        self._sync_legacy_document_from_qt_items_unlocked()
                    return saved
                if len(matches) > 1:
                    return False
        with self._lock:
            payload = self._load_payload_unlocked()
            _, _, data, err = self._find_record_by_id_unlocked(payload, record_id)
            if err:
                return False
            if not isinstance(data, dict):
                return False
            changed = False
            for key, value in patch.items():
                old_value = data.get(key, None)
                if value is None:
                    if key in data:
                        data.pop(key, None)
                        changed = True
                    continue
                if old_value != value:
                    data[key] = value
                    changed = True
            if not changed:
                return False
            payload["saved_at"] = int(time.time())
            return self._save_payload_unlocked(payload)

    def upsert_record(self, data_dict: dict | None = None) -> bool:
        if not isinstance(data_dict, dict):
            return False
        normalized = normalize_notice_identity_payload(
            normalize_active_item_data(data_dict)
        )
        record_id = self._normalize_key(
            canonical_target_record_id(normalized)
            or normalized.get("record_id")
            or normalized.get("active_item_id")
        )
        if not record_id:
            return False
        target_section = self._section_for_data(normalized)
        with self._lock:
            try:
                saved = self._state_store.upsert_qt_active_item(
                    normalized,
                    section=target_section,
                    origin="portal" if bool(normalized.get("lan_created_from_portal")) else "qt",
                )
                if saved:
                    self._sync_legacy_document_from_qt_items_unlocked()
                return saved
            except Exception:
                return False

    def delete_record(self, record_id: str = "", active_item_id: str = "") -> bool:
        record_id = self._normalize_key(record_id)
        active_item_id = self._normalize_key(active_item_id)
        if not record_id and not active_item_id:
            return False
        with self._lock:
            try:
                deleted = self._state_store.delete_qt_active_item(
                    active_item_id=active_item_id,
                    record_id=record_id,
                )
                if deleted:
                    self._sync_legacy_document_from_qt_items_unlocked()
                return deleted
            except Exception:
                return False

    def replace_payload(self, payload: dict | None = None) -> bool:
        payload_to_save = payload if isinstance(payload, dict) else self._default_payload()
        return self.save_payload(payload_to_save)

    def rename_record_id(self, old_record_id: str, new_record_id: str) -> bool:
        old_id = self._normalize_key(old_record_id)
        new_id = self._normalize_key(new_record_id)
        if not old_id or not new_id or old_id == new_id:
            return False
        with self._lock:
            try:
                changed = False
                for item in self._state_store.list_qt_active_items():
                    data = dict(item.get("payload") or {})
                    if not self._record_matches_target_id(data, old_id):
                        continue
                    data["record_id"] = new_id
                    data["target_record_id"] = new_id
                    self._state_store.upsert_qt_active_item(
                        data,
                        section=str(item.get("section") or ""),
                        sort_order=int(item.get("sort_order") or 0),
                        origin=str(item.get("origin") or ""),
                    )
                    changed = True
                if changed:
                    self._sync_legacy_document_from_qt_items_unlocked()
                    return True
            except Exception:
                pass
        with self._lock:
            payload = self._load_payload_unlocked()
            changed = False
            for _, _, data in self._iter_record_entries_unlocked(payload):
                if self._record_matches_target_id(data, old_id):
                    data["record_id"] = new_id
                    data["target_record_id"] = new_id
                    changed = True
            if not changed:
                return False
            payload["saved_at"] = int(time.time())
            return self._save_payload_unlocked(payload)

    def validate_or_repair_record_ids(self) -> dict:
        """
        Ensure every cache record has a non-empty, unique record_id.
        For duplicate IDs, later duplicates get a new UUID and clear
        buildings/specialty to avoid cross-item binding.
        """
        result = {
            "changed": False,
            "saved": False,
            "had_repairs": False,
            "total": 0,
            "missing_fixed": 0,
            "duplicate_fixed": 0,
        }
        with self._lock:
            payload = self._load_payload_unlocked()
            seen_ids: set[str] = set()
            changed = False
            for _, _, data in self._iter_record_entries_unlocked(payload):
                result["total"] += 1
                rid = self._normalize_key(data.get("record_id"))
                if not rid:
                    rid = self._new_record_id_unlocked(seen_ids)
                    data["record_id"] = rid
                    seen_ids.add(rid)
                    changed = True
                    result["missing_fixed"] += 1
                    continue
                if rid in seen_ids:
                    new_id = self._new_record_id_unlocked(seen_ids)
                    data["record_id"] = new_id
                    data.pop("buildings", None)
                    data.pop("specialty", None)
                    seen_ids.add(new_id)
                    changed = True
                    result["duplicate_fixed"] += 1
                    continue
                seen_ids.add(rid)

            if not changed:
                return result

            result["had_repairs"] = True
            payload["saved_at"] = int(time.time())
            saved = self._save_payload_unlocked(payload)
            result["saved"] = bool(saved)
            result["changed"] = bool(saved)
            return result

    def normalize_buildings_on_startup(self) -> dict:
        """
        Normalize building labels in cache to canonical values (e.g. D栋 -> D楼)
        and deduplicate same-meaning items.
        """
        result = {
            "changed": 0,
            "saved": False,
            "had_repairs": False,
            "total": 0,
        }
        with self._lock:
            payload = self._load_payload_unlocked()
            changed = False
            for _, _, data in self._iter_record_entries_unlocked(payload):
                result["total"] += 1
                if "buildings" not in data:
                    continue
                old_value = data.get("buildings")
                new_value = normalize_buildings_list(old_value)
                if isinstance(old_value, list) and old_value == new_value:
                    continue
                if not isinstance(old_value, list) and not old_value and not new_value:
                    continue
                data["buildings"] = new_value
                result["changed"] += 1
                changed = True

            if not changed:
                return result

            result["had_repairs"] = True
            payload["saved_at"] = int(time.time())
            saved = self._save_payload_unlocked(payload)
            result["saved"] = bool(saved)
            if not saved:
                result["changed"] = 0
                result["had_repairs"] = False
            return result

    def remove_legacy_display_fields_on_startup(self) -> dict:
        result = {
            "changed": 0,
            "saved": False,
            "had_repairs": False,
            "total": 0,
        }
        with self._lock:
            payload = self._load_payload_unlocked()
            changed = False
            for _, _, data in self._iter_record_entries_unlocked(payload):
                result["total"] += 1
                normalized = normalize_active_item_data(data)
                if normalized == data:
                    continue
                data.clear()
                data.update(normalized)
                result["changed"] += 1
                changed = True

            if not changed:
                return result

            result["had_repairs"] = True
            payload["saved_at"] = int(time.time())
            saved = self._save_payload_unlocked(payload)
            result["saved"] = bool(saved)
            if not saved:
                result["changed"] = 0
                result["had_repairs"] = False
            return result

    def migrate_level_lock_on_startup(self) -> dict:
        result = {
            "changed": 0,
            "saved": False,
            "had_repairs": False,
            "total": 0,
        }
        with self._lock:
            payload = self._load_payload_unlocked()
            changed = False
            for _, _, data in self._iter_record_entries_unlocked(payload):
                result["total"] += 1
                notice_type = str(data.get("notice_type") or "").strip()
                if not notice_supports_level_lock(notice_type):
                    continue
                current_level = str(data.get("level") or "").strip()
                detected_level = detect_level_from_notice_text(
                    notice_type,
                    data.get("text", ""),
                )
                if notice_type in ("设备变更", "变更通告"):
                    if bool(data.get("level_locked")):
                        continue
                    if not detected_level or detected_level == current_level:
                        continue
                    data["level"] = detected_level
                    result["changed"] += 1
                    changed = True
                    continue

                if bool(data.get("level_locked")):
                    continue
                if not current_level:
                    continue
                if not detected_level or detected_level == current_level:
                    continue
                data["level_locked"] = True
                result["changed"] += 1
                changed = True

            if not changed:
                return result

            result["had_repairs"] = True
            payload["saved_at"] = int(time.time())
            saved = self._save_payload_unlocked(payload)
            result["saved"] = bool(saved)
            if not saved:
                result["changed"] = 0
                result["had_repairs"] = False
            return result

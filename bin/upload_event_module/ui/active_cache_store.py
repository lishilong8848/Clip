import json
import os
import threading
import time
import uuid
from typing import Any

from ..building_normalizer import normalize_buildings_value as normalize_buildings_list
from .display_state import (
    detect_level_from_notice_text,
    normalize_active_item_data,
    notice_supports_level_lock,
)


class ActiveCacheStore:
    """Thread-safe JSON store wrapper for ACTIVE_CACHE_FILE."""

    _SECTIONS = ("event", "other")

    def __init__(self, cache_file: str):
        self.cache_file = str(cache_file or "")
        self._lock = threading.RLock()

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
        if not self.cache_file or not os.path.exists(self.cache_file):
            return self._default_payload()
        try:
            with open(self.cache_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if not isinstance(payload, dict):
                return self._default_payload()
            for section in self._SECTIONS:
                if not isinstance(payload.get(section), list):
                    payload[section] = []
            if not isinstance(payload.get("clipboard_queue"), list):
                payload["clipboard_queue"] = []
            return payload
        except Exception:
            return self._default_payload()

    def load_payload(self) -> dict:
        with self._lock:
            return self._load_payload_unlocked()

    def _save_payload_unlocked(self, payload: dict) -> bool:
        if not self.cache_file:
            return False
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        except Exception:
            return False
        tmp_path = (
            f"{self.cache_file}.tmp.{os.getpid()}.{threading.get_ident()}.{int(time.time() * 1000)}"
        )
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self.cache_file)
            return True
        except Exception:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
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
            if self._normalize_key(data.get("record_id")) == rid:
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

    def patch_record_fields(
        self,
        record_id: str = "",
        patch: dict | None = None,
    ) -> bool:
        if not isinstance(patch, dict) or not patch:
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
        normalized = normalize_active_item_data(data_dict)
        record_id = self._normalize_key(normalized.get("record_id"))
        if not record_id:
            return False
        with self._lock:
            payload = self._load_payload_unlocked()
            target_section = self._section_for_data(normalized)
            current_section, entry, _, err = self._find_record_by_id_unlocked(
                payload, record_id
            )
            if err == "conflict":
                return False
            if err == "not_found":
                payload[target_section].insert(0, {"data": normalized})
                payload["saved_at"] = int(time.time())
                return self._save_payload_unlocked(payload)
            if not isinstance(entry, dict):
                return False
            if current_section != target_section and current_section in self._SECTIONS:
                try:
                    payload[current_section].remove(entry)
                except ValueError:
                    pass
                payload[target_section].insert(0, entry)
            entry["data"] = normalized
            payload["saved_at"] = int(time.time())
            return self._save_payload_unlocked(payload)

    def replace_payload(self, payload: dict | None = None) -> bool:
        payload_to_save = payload if isinstance(payload, dict) else self._default_payload()
        return self.save_payload(payload_to_save)

    def rename_record_id(self, old_record_id: str, new_record_id: str) -> bool:
        old_id = self._normalize_key(old_record_id)
        new_id = self._normalize_key(new_record_id)
        if not old_id or not new_id or old_id == new_id:
            return False
        with self._lock:
            payload = self._load_payload_unlocked()
            changed = False
            for _, _, data in self._iter_record_entries_unlocked(payload):
                if self._normalize_key(data.get("record_id")) == old_id:
                    data["record_id"] = new_id
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

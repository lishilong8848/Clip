# -*- coding: utf-8 -*-
import threading
import time
import re
import unicodedata
import uuid
import os
from PyQt6.QtWidgets import QListWidgetItem, QInputDialog
from PyQt6.QtCore import Qt, QSize, QTimer
from PyQt6 import sip

from ..config import get_field_config
from ..logger import LOG_FILE, log_info, log_warning
from ..utils import WHITESPACE_TRANSLATOR
from ..services.service_registry import query_record_by_id, update_bitable_record_fields
from ..core.parser import extract_event_info
from ..time_parser import parse_time_range
from ..building_normalizer import (
    normalize_building_name,
    normalize_buildings_value as normalize_buildings_list,
)
from .widgets import ClipboardItemWidget
from .display_state import (
    detect_level_from_notice_text,
    normalize_active_item_data,
    notice_supports_level_lock,
)

class MainWindowRecordsMixin:
    _EVENT_TIMER_STATE_FIELDS = (
        "_timer_stage",
        "_timer_target_minutes",
        "_timer_update_count",
        "_timer_event_level",
        "_timer_base_start_time",
        "_timer_start_time",
        "_timer_last_update_response_time",
    )

    @classmethod
    def _extract_event_timer_state_patch(cls, data_dict):
        if not isinstance(data_dict, dict):
            return {}
        patch = {}
        for key in cls._EVENT_TIMER_STATE_FIELDS:
            if key in data_dict:
                patch[key] = data_dict.get(key)
        return patch

    def _init_runtime_maintenance_timer(self):
        self.runtime_maintenance_timer = QTimer(self)
        self.runtime_maintenance_timer.timeout.connect(
            self._run_runtime_maintenance
        )
        self.runtime_maintenance_timer.start(10 * 60 * 1000)

    def _collect_live_runtime_record_ids(self) -> set[str]:
        record_ids = set()
        for _, item in self._iter_active_items():
            try:
                data = item.data(Qt.ItemDataRole.UserRole)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            record_id = str(data.get("record_id") or "").strip()
            if not record_id or self._is_placeholder_record(data):
                continue
            record_ids.add(record_id)
        return record_ids

    def _collect_busy_runtime_record_ids(self) -> set[str]:
        busy = set()
        for mapping in (
            getattr(self, "pending_replace_by_record_id", {}),
            getattr(self, "pending_upload_rollback_by_record_id", {}),
            getattr(self, "pending_end_rollback_by_record_id", {}),
            getattr(self, "pending_new_by_record_id", {}),
            getattr(self, "pending_update_after_upload", {}),
            getattr(self, "pending_action_types", {}),
            getattr(self, "_upload_queues", {}),
            getattr(self, "_upload_workers", {}),
        ):
            if isinstance(mapping, dict):
                busy.update(
                    str(key or "").strip()
                    for key in mapping.keys()
                    if str(key or "").strip()
                )
        return busy

    def _trim_runtime_state_sets(self) -> dict[str, int]:
        active_record_ids = self._collect_live_runtime_record_ids()
        busy_record_ids = active_record_ids | self._collect_busy_runtime_record_ids()
        stats = {}

        before = len(self._record_binding_validated_ids)
        self._record_binding_validated_ids.intersection_update(active_record_ids)
        if len(self._record_binding_validated_ids) != before:
            stats["record_binding_validated_trimmed"] = (
                before - len(self._record_binding_validated_ids)
            )

        before = len(self._today_in_progress_synced_record_ids)
        self._today_in_progress_synced_record_ids.intersection_update(
            active_record_ids
        )
        if len(self._today_in_progress_synced_record_ids) != before:
            stats["today_in_progress_synced_trimmed"] = (
                before - len(self._today_in_progress_synced_record_ids)
            )

        before = len(self._record_binding_validation_pending_ids)
        self._record_binding_validation_pending_ids.intersection_update(
            active_record_ids
        )
        if len(self._record_binding_validation_pending_ids) != before:
            stats["record_binding_pending_trimmed"] = (
                before - len(self._record_binding_validation_pending_ids)
            )

        before = len(self._today_in_progress_pending_record_ids)
        self._today_in_progress_pending_record_ids.intersection_update(
            active_record_ids
        )
        if len(self._today_in_progress_pending_record_ids) != before:
            stats["today_in_progress_pending_trimmed"] = (
                before - len(self._today_in_progress_pending_record_ids)
            )

        before = len(self.pending_action_record_ids)
        self.pending_action_record_ids.intersection_update(busy_record_ids)
        if len(self.pending_action_record_ids) != before:
            stats["pending_action_trimmed"] = (
                before - len(self.pending_action_record_ids)
            )

        return stats

    def _log_runtime_health_snapshot(self, reason: str):
        try:
            log_size = (
                os.path.getsize(LOG_FILE) if LOG_FILE and os.path.exists(LOG_FILE) else 0
            )
        except Exception:
            log_size = 0
        clipboard_state = "paused"
        try:
            if getattr(self, "_clipboard_degraded", False):
                clipboard_state = "degraded"
            elif getattr(self, "_clipboard_effective_running", False):
                clipboard_state = "running"
            elif getattr(self, "_is_clipboard_listener_disabled", lambda: False)():
                clipboard_state = "paused"
            else:
                clipboard_state = "idle"
        except Exception:
            clipboard_state = "unknown"
        log_info(
            "RuntimeHealth[%s]: active_items=%s validated=%s synced=%s payload_store=%s payload_alias=%s upload_workers=%s clipboard=%s app_log_bytes=%s"
            % (
                reason,
                sum(1 for _ in self._iter_active_items()),
                len(getattr(self, "_record_binding_validated_ids", set())),
                len(getattr(self, "_today_in_progress_synced_record_ids", set())),
                len(getattr(self, "_payload_store", {})),
                len(getattr(self, "_payload_alias", {})),
                len(getattr(self, "_upload_workers", {})),
                clipboard_state,
                log_size,
            )
        )

    def _run_runtime_maintenance(self):
        stats = {}
        try:
            stats.update(self._trim_runtime_state_sets())
        except Exception as exc:
            log_warning(f"运行态维护: 集合裁剪失败 error={exc}")
        try:
            stats.update(self._cleanup_runtime_payload_state())
        except Exception as exc:
            log_warning(f"运行态维护: payload清理失败 error={exc}")
        try:
            stats.update(self._cleanup_finished_upload_workers())
        except Exception as exc:
            log_warning(f"运行态维护: upload worker清理失败 error={exc}")
        try:
            stats.update(self._perform_clipboard_health_maintenance())
        except Exception as exc:
            log_warning(f"运行态维护: 剪贴板健康检查失败 error={exc}")
        changes = {
            key: value
            for key, value in stats.items()
            if isinstance(value, int) and value > 0
        }
        if changes:
            log_info(
                "RuntimeMaintenance: "
                + ", ".join(
                    f"{key}={value}" for key, value in sorted(changes.items())
                )
            )
            self._log_runtime_health_snapshot("maintenance")

    def _safe_widget(self, widget):
        if not widget:
            return None
        try:
            if sip.isdeleted(widget):
                return None
        except Exception:
            return None
        return widget

    def _safe_item_widget(self, list_widget, item):
        if not list_widget or not item:
            return None
        try:
            if sip.isdeleted(list_widget):
                return None
        except Exception:
            return None
        if not self._is_valid_list_item(item):
            return None
        try:
            widget = list_widget.itemWidget(item)
        except Exception:
            return None
        return self._safe_widget(widget)

    def _rebuild_active_item_widget(
        self,
        list_widget,
        item,
        data_dict,
        *,
        force_status: str | None = None,  # "end"/"update"/None
        upload_in_progress: bool | None = None,
        pending_upload_hash: str | None = None,
        has_unuploaded_changes: bool | None = None,
    ):
        if not list_widget or not item or not data_dict:
            return None
        if not self._is_valid_list_item(item):
            return None
        old_widget = None
        try:
            old_widget = list_widget.itemWidget(item)
        except Exception:
            old_widget = None

        try:
            new_widget = ClipboardItemWidget(data_dict)
            new_widget.action_clicked.connect(self.handle_action)
            new_widget.today_progress_clicked.connect(
                self._handle_today_in_progress_toggle
            )
            new_widget.ended_signal.connect(self.move_to_history)
            new_widget.delete_requested.connect(self._delete_active_item)
            if hasattr(new_widget, "set_delete_interaction_enabled"):
                new_widget.set_delete_interaction_enabled(
                    self._delete_interaction_enabled
                )
        except Exception as exc:
            try:
                record_id = (data_dict or {}).get("record_id", "")
                log_warning(
                    f"活动条目重建失败，保留旧widget: record_id={record_id}, error={exc}"
                )
            except Exception:
                pass
            self._mark_cache_refresh_needed()
            return self._safe_widget(old_widget)

        try:
            item.setSizeHint(QSize(420, 80))
        except Exception:
            pass

        try:
            if old_widget:
                try:
                    list_widget.removeItemWidget(item)
                except Exception:
                    pass
            list_widget.setItemWidget(item, new_widget)
        except Exception as exc:
            try:
                record_id = (data_dict or {}).get("record_id", "")
                log_warning(
                    f"活动条目挂载新widget失败: record_id={record_id}, error={exc}"
                )
            except Exception:
                pass
            try:
                new_widget.deleteLater()
            except Exception:
                pass
            self._mark_cache_refresh_needed()
            return self._safe_widget(old_widget)

        if old_widget:
            try:
                old_widget.deleteLater()
            except Exception:
                pass

        if upload_in_progress is None:
            upload_in_progress = data_dict.get("_upload_in_progress")
        if pending_upload_hash is None:
            pending_upload_hash = data_dict.get("_pending_upload_hash")
        if has_unuploaded_changes is None:
            has_unuploaded_changes = data_dict.get("_has_unuploaded_changes")

        if has_unuploaded_changes is not None:
            data_dict["_has_unuploaded_changes"] = bool(has_unuploaded_changes)
            if not data_dict["_has_unuploaded_changes"]:
                data_dict["_pending_upload_hash"] = None
                pending_upload_hash = None

        try:
            item.setData(Qt.ItemDataRole.UserRole, data_dict)
        except Exception:
            return None

        if upload_in_progress is not None:
            new_widget.upload_in_progress = bool(upload_in_progress)
        if pending_upload_hash is not None:
            new_widget.pending_upload_hash = pending_upload_hash
        if has_unuploaded_changes is not None:
            new_widget.has_unuploaded_changes = bool(has_unuploaded_changes)

        status = force_status
        if not status:
            info = extract_event_info(data_dict.get("text", ""))
            if info and info.get("status") == "结束":
                status = "end"
            elif not data_dict.get("_is_placeholder_record", True):
                status = "update"
        if status == "end":
            new_widget.set_button_to_end_mode()
        elif status == "update":
            new_widget.set_button_to_update_mode()
        elif has_unuploaded_changes is False:
            new_widget.set_uploaded_visual(True)

        self._schedule_today_in_progress_sync(data_dict)

        return new_widget

    def _is_deleted(self, obj):
        if not obj:
            return None
        try:
            return sip.isdeleted(obj)
        except Exception:
            return None

    def _list_name(self, list_widget):
        if list_widget is self.list_active_event:
            return "active_event"
        if list_widget is self.list_active_other:
            return "active_other"
        if list_widget is self.list_history_event:
            return "history_event"
        if list_widget is self.list_history_other:
            return "history_other"
        return getattr(list_widget, "objectName", lambda: "")() or "unknown_list"

    def _debug_list_item(self, list_widget, item):
        row = None
        try:
            if list_widget and item:
                row = list_widget.row(item)
        except Exception:
            row = None
        widget = None
        try:
            widget = self._safe_item_widget(list_widget, item)
        except Exception:
            widget = None
        return {
            "list": self._list_name(list_widget),
            "row": row,
            "list_deleted": self._is_deleted(list_widget),
            "widget_deleted": self._is_deleted(widget),
            "item_valid": self._is_valid_list_item(item) if item else False,
        }

    def _set_last_ui_op(self, label: str, **kwargs):
        try:
            detail = " ".join(f"{k}={v}" for k, v in kwargs.items()) if kwargs else ""
            msg = f"{label} {detail}".strip()
            self._last_ui_op = msg
            log_info(f"UI_OP: {msg}")
        except Exception:
            pass

    def _log_detail_preview_update(
        self, data_dict: dict, record_id: str = "", reason: str = ""
    ):
        if not isinstance(data_dict, dict):
            return
        rid = record_id or data_dict.get("record_id", "")
        display_data = dict(data_dict)
        if self._is_placeholder_record(display_data):
            display_data["record_id"] = ""
        try:
            text = (
                (display_data.get("text") or "").replace("\r", " ").replace("\n", " ")
            )
            if len(text) > 200:
                text = text[:200] + "..."
            suffix = f" reason={reason}" if reason else ""
            log_info(f"详情预览更新{suffix} record_id={rid} text={text}")
        except Exception:
            return

    def _detail_dialog_matches_record(self, record_id: str) -> bool:
        if not record_id:
            return False
        try:
            if not self.detail_dialog or not self.detail_dialog.isVisible():
                return False
        except Exception:
            return False
        current_id = getattr(self.detail_dialog, "current_record_id", "") or ""
        if not current_id:
            return False
        return current_id == record_id

    def _detail_dialog_matches_active_item(self, active_item_id: str) -> bool:
        active_item_id = str(active_item_id or "").strip()
        if not active_item_id:
            return False
        try:
            if not self.detail_dialog or not self.detail_dialog.isVisible():
                return False
        except Exception:
            return False
        current_id = getattr(self.detail_dialog, "current_active_item_id", "") or ""
        if not current_id:
            return False
        return current_id == active_item_id

    @staticmethod
    def _normalize_match_text(value) -> str:
        text = unicodedata.normalize("NFKC", str(value or ""))
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _build_match_identity(
        self,
        *,
        notice_type: str = "",
        title: str = "",
        time_str: str = "",
        text: str = "",
    ) -> tuple[str, str]:
        info = extract_event_info(text or "") or {}
        resolved_notice_type = str(
            notice_type or info.get("notice_type") or ""
        ).strip()
        resolved_title = self._normalize_match_text(
            title or info.get("title") or ""
        )
        resolved_time = self._normalize_match_text(
            time_str or info.get("time_str") or ""
        )
        if not resolved_notice_type or not resolved_title:
            return resolved_title, ""
        if self._is_event_notice(resolved_notice_type):
            if not resolved_time:
                return resolved_title, ""
            return (
                resolved_title,
                f"{resolved_notice_type}|{resolved_title}|{resolved_time}",
            )
        return resolved_title, f"{resolved_notice_type}|{resolved_title}"

    @staticmethod
    def _normalize_routing_state(value) -> str:
        state = str(value or "").strip().lower()
        if state in {"normal", "conflicted"}:
            return state
        return "normal"

    def _is_routing_conflicted(self, data_dict: dict | None) -> bool:
        if not isinstance(data_dict, dict):
            return False
        return self._normalize_routing_state(data_dict.get("routing_state")) == "conflicted"

    def _routing_error_text(self, data_dict: dict | None) -> str:
        if not isinstance(data_dict, dict):
            return "条目路由冲突，已阻止自动匹配和远端写入。"
        detail = str(data_dict.get("routing_error") or "").strip()
        if detail:
            return detail
        return "条目路由冲突，已阻止自动匹配和远端写入。"

    def _ensure_active_item_identity(self, data_dict: dict | None) -> dict:
        if not isinstance(data_dict, dict):
            return {}
        ensured = dict(data_dict)
        active_item_id = str(ensured.get("active_item_id") or "").strip()
        if not active_item_id:
            active_item_id = uuid.uuid4().hex
        text = str(ensured.get("text") or "")
        parsed_info = extract_event_info(text) or {}
        title_hint = parsed_info.get("title") or str(ensured.get("match_title") or "")
        time_hint = parsed_info.get("time_str") or str(ensured.get("time_str") or "")
        match_title, match_key = self._build_match_identity(
            notice_type=str(
                ensured.get("notice_type") or parsed_info.get("notice_type") or ""
            ).strip(),
            title=title_hint,
            time_str=time_hint,
            text=text,
        )
        ensured["active_item_id"] = active_item_id
        if match_title:
            ensured["match_title"] = match_title
        else:
            ensured.pop("match_title", None)
        if match_key:
            ensured["match_key"] = match_key
        else:
            ensured.pop("match_key", None)
        routing_state = self._normalize_routing_state(ensured.get("routing_state"))
        if routing_state == "conflicted":
            ensured["routing_state"] = "conflicted"
            ensured["routing_error"] = str(
                ensured.get("routing_error") or self._routing_error_text(ensured)
            ).strip()
        else:
            ensured["routing_state"] = "normal"
            ensured.pop("routing_error", None)
        return ensured

    def _inherit_active_runtime_fields(
        self, data_dict: dict | None, existing_data: dict | None
    ) -> dict:
        updated = dict(data_dict or {})
        if not isinstance(existing_data, dict):
            return updated
        active_item_id = str(existing_data.get("active_item_id") or "").strip()
        if active_item_id:
            updated["active_item_id"] = active_item_id
        for key in (
            "today_in_progress_state",
            "record_binding_state",
            "record_binding_error",
            "routing_state",
            "routing_error",
        ):
            if key not in updated and key in existing_data:
                updated[key] = existing_data.get(key)
        return updated

    @staticmethod
    def _normalize_record_binding_state(value) -> str:
        state = str(value or "").strip().lower()
        if state in {"bound", "placeholder", "conflicted"}:
            return state
        return ""

    def _is_record_binding_conflicted(self, data_dict: dict | None) -> bool:
        if not isinstance(data_dict, dict):
            return False
        return (
            self._normalize_record_binding_state(data_dict.get("record_binding_state"))
            == "conflicted"
        )

    @staticmethod
    def _normalize_record_binding_text(value) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).lower()

    @classmethod
    def _normalize_record_binding_value(cls, value) -> str:
        if isinstance(value, list):
            parts = []
            for item in value:
                normalized = cls._normalize_record_binding_value(item)
                if normalized:
                    parts.append(normalized)
            if not parts:
                return ""
            return "|".join(sorted(dict.fromkeys(parts)))
        if isinstance(value, dict):
            for key in ("name", "text", "value", "label"):
                if key in value:
                    return cls._normalize_record_binding_value(value.get(key))
            return ""
        return cls._normalize_record_binding_text(value)

    @staticmethod
    def _normalize_record_binding_time(value) -> str:
        if isinstance(value, list):
            for item in value:
                normalized = MainWindowRecordsMixin._normalize_record_binding_time(item)
                if normalized:
                    return normalized
            return ""
        if isinstance(value, dict):
            for key in ("timestamp", "time", "value", "text", "name"):
                if key in value:
                    normalized = MainWindowRecordsMixin._normalize_record_binding_time(
                        value.get(key)
                    )
                    if normalized:
                        return normalized
            return ""
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts <= 0:
                return ""
            if ts > 10**11:
                ts /= 1000.0
            try:
                return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))
            except Exception:
                return ""
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            start_dt, _ = parse_time_range(text)
        except Exception:
            start_dt = None
        if start_dt is not None:
            try:
                return start_dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                return ""
        return MainWindowRecordsMixin._normalize_record_binding_text(text)

    def _extract_local_record_binding_fingerprint(self, data_dict: dict | None) -> dict:
        if not isinstance(data_dict, dict):
            return {}
        text = str(data_dict.get("text") or "").strip()
        info = extract_event_info(text) or {}
        buildings = self._normalize_buildings_value(data_dict.get("buildings"))
        time_anchor = ""
        try:
            start_dt, _ = parse_time_range(info.get("time_str") or "")
        except Exception:
            start_dt = None
        if start_dt is not None:
            try:
                time_anchor = start_dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                time_anchor = ""
        return {
            "notice_type": str(
                data_dict.get("notice_type") or info.get("notice_type") or ""
            ).strip(),
            "title": self._normalize_record_binding_text(info.get("title") or ""),
            "building": self._normalize_record_binding_value(buildings),
            "specialty": self._normalize_record_binding_value(
                data_dict.get("specialty")
            ),
            "time_anchor": time_anchor,
        }

    def _extract_remote_record_binding_fingerprint(
        self, notice_type: str, fields: dict | None
    ) -> dict:
        if not isinstance(fields, dict):
            return {}
        field_config = get_field_config(notice_type or "")
        title_field = field_config.get("title") or field_config.get("name") or ""
        building_field = field_config.get("building") or ""
        specialty_field = field_config.get("specialty") or ""
        time_field = ""
        for key in ("plan_start", "occurrence_time", "fault_time", "start_time"):
            field_name = field_config.get(key) or ""
            if field_name:
                time_field = field_name
                break
        return {
            "notice_type": str(notice_type or "").strip(),
            "title": self._normalize_record_binding_value(fields.get(title_field)),
            "building": self._normalize_record_binding_value(
                fields.get(building_field)
            ),
            "specialty": self._normalize_record_binding_value(
                fields.get(specialty_field)
            ),
            "time_anchor": self._normalize_record_binding_time(
                fields.get(time_field)
            ),
        }

    def _detect_record_binding_conflict(
        self, local_fp: dict | None, remote_fp: dict | None
    ) -> tuple[bool, list[str]]:
        if not isinstance(local_fp, dict) or not isinstance(remote_fp, dict):
            return False, []
        strong_mismatches = []
        strong_comparable_count = 0
        for key in ("title", "time_anchor"):
            local_value = str(local_fp.get(key) or "").strip()
            remote_value = str(remote_fp.get(key) or "").strip()
            if not local_value or not remote_value:
                continue
            strong_comparable_count += 1
            if local_value != remote_value:
                strong_mismatches.append(key)
        if strong_comparable_count > 0:
            return bool(strong_mismatches), strong_mismatches

        soft_mismatches = []
        soft_comparable_count = 0
        for key in ("building", "specialty"):
            local_value = str(local_fp.get(key) or "").strip()
            remote_value = str(remote_fp.get(key) or "").strip()
            if not local_value or not remote_value:
                continue
            soft_comparable_count += 1
            if local_value != remote_value:
                soft_mismatches.append(key)
        return soft_comparable_count == 2 and len(soft_mismatches) == 2, soft_mismatches

    def _record_binding_error_text(self, data_dict: dict | None) -> str:
        if not isinstance(data_dict, dict):
            return "Record ID 冲突，已阻止写入远端。"
        detail = str(data_dict.get("record_binding_error") or "").strip()
        if detail:
            return detail
        return "Record ID 冲突，已阻止写入远端。"

    def _set_record_binding_state(
        self,
        record_id: str,
        state: str,
        error: str = "",
    ):
        record_id = str(record_id or "").strip()
        normalized_state = self._normalize_record_binding_state(state)
        if not record_id or not normalized_state:
            return
        patch = {"record_binding_state": normalized_state}
        if normalized_state == "conflicted":
            patch["record_binding_error"] = str(error or "Record ID 冲突").strip()
        else:
            patch["record_binding_error"] = None
        if getattr(self, "cache_store", None):
            try:
                self.cache_store.patch_record_fields(record_id=record_id, patch=patch)
            except Exception:
                pass
        list_widget, item = self._find_active_item_by_record_id(record_id)
        if item and self._is_valid_list_item(item):
            data = item.data(Qt.ItemDataRole.UserRole) or {}
            updated = dict(data)
            updated["record_binding_state"] = normalized_state
            if normalized_state == "conflicted":
                updated["record_binding_error"] = patch["record_binding_error"]
            else:
                updated.pop("record_binding_error", None)
            self._commit_active_record(
                updated,
                refresh_detail=True,
                rebuild_widget=True,
                list_widget=list_widget,
                item=item,
            )

    def _schedule_record_binding_validation(self, data_dict: dict | None):
        if not isinstance(data_dict, dict):
            return
        record_id = str(data_dict.get("record_id") or "").strip()
        notice_type = str(data_dict.get("notice_type") or "").strip()
        if (
            not record_id
            or not notice_type
            or self._is_placeholder_record(data_dict)
            or record_id in self._record_binding_validation_pending_ids
            or record_id in self._record_binding_validated_ids
        ):
            return
        self._record_binding_validation_pending_ids.add(record_id)
        local_data = dict(data_dict)

        def worker():
            success, result = query_record_by_id(record_id, notice_type)
            conflict = False
            conflict_error = ""
            if success:
                fields = result.get("fields", {}) if isinstance(result, dict) else {}
                local_fp = self._extract_local_record_binding_fingerprint(local_data)
                remote_fp = self._extract_remote_record_binding_fingerprint(
                    notice_type, fields
                )
                conflict, mismatch_keys = self._detect_record_binding_conflict(
                    local_fp, remote_fp
                )
                if conflict:
                    mismatch_label_map = {
                        "title": "标题",
                        "building": "楼栋",
                        "specialty": "专业",
                        "time_anchor": "计划开始时间",
                    }
                    labels = [
                        mismatch_label_map.get(key, key) for key in mismatch_keys
                    ]
                    conflict_error = (
                        "Record ID 冲突："
                        + "、".join(labels)
                        + " 与多维记录不一致"
                    )
            else:
                conflict_error = str(result or "")

            def apply_result():
                self._record_binding_validation_pending_ids.discard(record_id)
                if not success:
                    if conflict_error:
                        log_warning(
                            f"Record ID 校验失败: record_id={record_id}, error={conflict_error}"
                        )
                    return
                self._record_binding_validated_ids.add(record_id)
                if conflict:
                    self._set_record_binding_state(
                        record_id,
                        "conflicted",
                        error=conflict_error,
                    )
                else:
                    self._set_record_binding_state(record_id, "bound", error="")

            self._enqueue_ui_mutation("record_binding_validation", apply_result)

        threading.Thread(target=worker, daemon=True).start()

    def _validate_record_bindings_on_startup(self):
        for _, item in self._iter_active_items():
            try:
                if not self._is_valid_list_item(item):
                    continue
                data = item.data(Qt.ItemDataRole.UserRole)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            self._schedule_record_binding_validation(data)

    def _request_safe_bind_record_id(self, current_record_id: str):
        record_id = str(current_record_id or "").strip()
        if not record_id:
            return
        list_widget, item = self._find_active_item_by_record_id(record_id)
        if not item or not self._is_valid_list_item(item):
            self.show_message("未找到需要绑定的条目。")
            return
        data = item.data(Qt.ItemDataRole.UserRole) or {}
        if not (
            self._is_placeholder_record(data)
            or self._is_record_binding_conflicted(data)
        ):
            self.show_message("当前条目无需重新绑定 Record ID。")
            return
        new_record_id, ok = QInputDialog.getText(
            self,
            "绑定 Record ID",
            "请输入新的 Record ID：",
        )
        if not ok:
            return
        new_record_id = str(new_record_id or "").strip()
        if not new_record_id or new_record_id == record_id:
            return
        other_list, other_item = self._find_active_item_by_record_id(new_record_id)
        if other_item and self._is_valid_list_item(other_item):
            self.show_message("该 Record ID 已绑定到其他条目，不能重复绑定。")
            return
        notice_type = str(data.get("notice_type") or "").strip()
        local_data = dict(data)

        def worker():
            success, result = query_record_by_id(new_record_id, notice_type)
            conflict = False
            error_text = ""
            if success:
                fields = result.get("fields", {}) if isinstance(result, dict) else {}
                local_fp = self._extract_local_record_binding_fingerprint(local_data)
                remote_fp = self._extract_remote_record_binding_fingerprint(
                    notice_type, fields
                )
                conflict, mismatch_keys = self._detect_record_binding_conflict(
                    local_fp, remote_fp
                )
                if conflict:
                    error_text = (
                        "绑定失败：输入的 Record ID 与当前条目内容不一致。"
                    )
            else:
                error_text = str(result or "")

            def apply_result():
                if not success:
                    self.show_message(f"绑定 Record ID 失败\n{error_text}")
                    return
                if conflict:
                    self.show_message(error_text)
                    return
                self._replace_record_id_everywhere(record_id, new_record_id)
                self._record_binding_validated_ids.discard(record_id)
                self._record_binding_validation_pending_ids.discard(record_id)
                self._record_binding_validated_ids.add(new_record_id)
                self._set_record_binding_state(new_record_id, "bound", error="")
                self.save_active_cache()
                self._refresh_detail_from_cache()

            self._enqueue_ui_mutation("record_id_safe_bind", apply_result)

        threading.Thread(target=worker, daemon=True).start()

    def _set_routing_state(
        self,
        active_item_id: str,
        state: str,
        error: str = "",
    ):
        active_item_id = str(active_item_id or "").strip()
        if not active_item_id:
            return
        normalized_state = self._normalize_routing_state(state)
        list_widget, item = self._find_active_item_by_active_item_id(active_item_id)
        if not item or not self._is_valid_list_item(item):
            return
        data = item.data(Qt.ItemDataRole.UserRole) or {}
        updated = dict(data)
        updated["routing_state"] = normalized_state
        if normalized_state == "conflicted":
            updated["routing_error"] = str(
                error or "条目路由冲突，已阻止自动匹配和远端写入。"
            ).strip()
        else:
            updated.pop("routing_error", None)
        record_id = self._get_cache_identity(updated)
        if getattr(self, "cache_store", None) and record_id:
            patch = {"routing_state": normalized_state}
            patch["routing_error"] = (
                updated.get("routing_error") if normalized_state == "conflicted" else None
            )
            try:
                self.cache_store.patch_record_fields(record_id=record_id, patch=patch)
            except Exception:
                pass
        self._commit_active_record(
            updated,
            refresh_detail=True,
            rebuild_widget=True,
            list_widget=list_widget,
            item=item,
        )

    def _remove_active_item_widget_only(self, list_widget, item):
        if not list_widget or not item or not self._is_valid_list_item(item):
            return
        try:
            widget = list_widget.itemWidget(item)
        except Exception:
            widget = None
        try:
            list_widget.removeItemWidget(item)
        except Exception:
            pass
        try:
            row = list_widget.row(item)
            if row != -1:
                list_widget.takeItem(row)
        except Exception:
            pass
        try:
            if widget:
                widget.deleteLater()
        except Exception:
            pass

    def _build_route_conflict_error(self, match_key: str) -> str:
        preview = str(match_key or "").split("|", 1)[-1]
        if len(preview) > 40:
            preview = preview[:40] + "..."
        return f"条目路由冲突：匹配键重复（{preview}），已阻止自动匹配和远端写入。"

    def _reconcile_active_route_duplicates(self):
        groups = {}
        for list_widget, item in self._iter_active_items():
            try:
                if not self._is_valid_list_item(item):
                    continue
                data = self._ensure_active_item_identity(
                    item.data(Qt.ItemDataRole.UserRole)
                )
            except Exception:
                continue
            match_key = str(data.get("match_key") or "").strip()
            if not match_key:
                continue
            groups.setdefault(match_key, []).append((list_widget, item, data))

        changed = False
        for match_key, entries in groups.items():
            if len(entries) <= 1:
                list_widget, item, data = entries[0]
                if self._normalize_routing_state(data.get("routing_state")) != "normal":
                    self._set_routing_state(data.get("active_item_id", ""), "normal")
                    changed = True
                continue

            real_entries = [entry for entry in entries if not self._is_placeholder_record(entry[2])]
            if len(real_entries) > 1:
                error = self._build_route_conflict_error(match_key)
                for _, _, data in entries:
                    self._set_routing_state(
                        data.get("active_item_id", ""),
                        "conflicted",
                        error=error,
                    )
                changed = True
                continue

            survivor = real_entries[0] if real_entries else entries[0]
            survivor_list, survivor_item, survivor_data = survivor
            source_list, source_item, source_data = entries[0]
            merged = dict(source_data)
            merged["active_item_id"] = survivor_data.get("active_item_id")
            merged["routing_state"] = "normal"
            merged.pop("routing_error", None)
            if real_entries:
                merged["record_id"] = survivor_data.get("record_id")
                merged["_is_placeholder_record"] = False
                merged["record_binding_state"] = survivor_data.get(
                    "record_binding_state", "bound"
                )
                if "today_in_progress_state" in survivor_data:
                    merged["today_in_progress_state"] = survivor_data.get(
                        "today_in_progress_state"
                    )
            committed = self._commit_active_record(
                self._ensure_active_item_identity(merged),
                refresh_detail=True,
                rebuild_widget=True,
                list_widget=survivor_list,
                item=survivor_item,
            )
            if committed:
                survivor_data = committed
            for list_widget, item, data in entries:
                if item is survivor_item:
                    continue
                self._cleanup_payload_for_data(data)
                self._remove_active_item_widget_only(list_widget, item)
                changed = True
            self._set_routing_state(
                survivor_data.get("active_item_id", ""),
                "normal",
                error="",
            )
            changed = True

        if changed:
            self.save_active_cache()
            self._refresh_detail_from_cache()
        return changed

    @staticmethod
    def _normalize_today_in_progress_state(value) -> str:
        text = str(value or "").strip()
        lowered = text.lower()
        if lowered in ("yes", "no", "unknown"):
            return lowered
        if text in ("是", "在进行"):
            return "yes"
        if text in ("否", "未进行"):
            return "no"
        return "unknown"

    @staticmethod
    def _today_in_progress_option_for_state(state: str) -> str:
        normalized = MainWindowRecordsMixin._normalize_today_in_progress_state(state)
        if normalized == "yes":
            return "是"
        if normalized == "no":
            return "否"
        return ""

    def _get_today_in_progress_field_name(self, notice_type: str) -> str:
        field_name = (
            get_field_config(notice_type or "").get("today_in_progress", "")
            if notice_type
            else ""
        )
        field_name = str(field_name or "").strip()
        return field_name or "今天是否进行"

    def _supports_today_in_progress_toggle(self, data_dict: dict | None) -> bool:
        if not isinstance(data_dict, dict):
            return False
        notice_type = str(data_dict.get("notice_type") or "").strip()
        record_id = str(data_dict.get("record_id") or "").strip()
        return (
            notice_type in ("设备变更", "变更通告")
            and bool(record_id)
            and not self._is_placeholder_record(data_dict)
        )

    def _extract_today_in_progress_state_from_record_fields(
        self, notice_type: str, fields: dict | None
    ) -> str:
        if not isinstance(fields, dict):
            return "unknown"
        field_name = self._get_today_in_progress_field_name(notice_type)
        raw_value = fields.get(field_name)
        if isinstance(raw_value, str):
            return self._normalize_today_in_progress_state(raw_value)
        if isinstance(raw_value, dict):
            return self._normalize_today_in_progress_state(
                raw_value.get("name") or raw_value.get("text") or raw_value.get("value")
            )
        if isinstance(raw_value, list) and raw_value:
            first = raw_value[0]
            if isinstance(first, dict):
                return self._normalize_today_in_progress_state(
                    first.get("name") or first.get("text") or first.get("value")
                )
            return self._normalize_today_in_progress_state(first)
        return "unknown"

    def _persist_today_in_progress_state(self, record_id: str, state: str):
        record_id = str(record_id or "").strip()
        if not record_id or not getattr(self, "cache_store", None):
            return
        try:
            self.cache_store.patch_record_fields(
                record_id=record_id,
                patch={
                    "today_in_progress_state": self._normalize_today_in_progress_state(
                        state
                    )
                },
            )
        except Exception:
            pass

    def _apply_today_in_progress_state_to_record(self, record_id: str, state: str):
        record_id = str(record_id or "").strip()
        normalized_state = self._normalize_today_in_progress_state(state)
        if not record_id:
            return
        list_widget, item = self._find_active_item_by_record_id(record_id)
        if item and not self._is_valid_list_item(item):
            item = None
            list_widget = None
        if item and list_widget:
            data = item.data(Qt.ItemDataRole.UserRole) or {}
            updated = dict(data)
            updated["today_in_progress_state"] = normalized_state
            self._commit_active_record(
                updated,
                refresh_detail=True,
                rebuild_widget=True,
                list_widget=list_widget,
                item=item,
            )
            return
        self._persist_today_in_progress_state(record_id, normalized_state)

    def _schedule_today_in_progress_sync(self, data_dict: dict | None):
        if not self._supports_today_in_progress_toggle(data_dict):
            return
        if self._is_record_binding_conflicted(data_dict):
            return
        record_id = str(data_dict.get("record_id") or "").strip()
        notice_type = str(data_dict.get("notice_type") or "").strip()
        if (
            not record_id
            or record_id in self._today_in_progress_pending_record_ids
            or record_id in self._today_in_progress_synced_record_ids
        ):
            return
        self._today_in_progress_pending_record_ids.add(record_id)

        def worker():
            success, result = query_record_by_id(record_id, notice_type)
            state = "unknown"
            error_text = ""
            if success:
                fields = result.get("fields", {}) if isinstance(result, dict) else {}
                state = self._extract_today_in_progress_state_from_record_fields(
                    notice_type,
                    fields,
                )
            else:
                error_text = str(result or "")

            def apply_result():
                self._today_in_progress_pending_record_ids.discard(record_id)
                self._today_in_progress_synced_record_ids.add(record_id)
                if not success:
                    if error_text:
                        log_warning(
                            f"今天是否进行状态同步失败: record_id={record_id}, error={error_text}"
                        )
                    return
                self._apply_today_in_progress_state_to_record(record_id, state)

            self._enqueue_ui_mutation("today_in_progress_sync", apply_result)

        threading.Thread(target=worker, daemon=True).start()

    def _handle_today_in_progress_toggle(self, data_dict: dict, target_state: str):
        if not self._supports_today_in_progress_toggle(data_dict):
            return
        if self._is_routing_conflicted(data_dict):
            self.show_message(self._routing_error_text(data_dict))
            return
        if self._is_record_binding_conflicted(data_dict):
            self.show_message(self._record_binding_error_text(data_dict))
            return
        record_id = str(data_dict.get("record_id") or "").strip()
        notice_type = str(data_dict.get("notice_type") or "").strip()
        current_state = self._normalize_today_in_progress_state(
            data_dict.get("today_in_progress_state")
        )
        desired_state = self._normalize_today_in_progress_state(target_state)
        if (
            not record_id
            or desired_state == "unknown"
            or record_id in self._today_in_progress_pending_record_ids
        ):
            return

        list_widget, item = self._find_active_item_by_record_id(record_id)
        widget = self._safe_item_widget(list_widget, item)
        if widget and hasattr(widget, "set_today_progress_state"):
            widget.set_today_progress_state(current_state, enabled=False)

        self._today_in_progress_pending_record_ids.add(record_id)
        field_name = self._get_today_in_progress_field_name(notice_type)
        field_value = self._today_in_progress_option_for_state(desired_state)

        def worker():
            success, result = update_bitable_record_fields(
                record_id,
                notice_type,
                {field_name: field_value},
            )

            def apply_result():
                self._today_in_progress_pending_record_ids.discard(record_id)
                widget_now = self._safe_item_widget(
                    *self._find_active_item_by_record_id(record_id)
                )
                if success:
                    self._today_in_progress_synced_record_ids.add(record_id)
                    self._apply_today_in_progress_state_to_record(
                        record_id, desired_state
                    )
                    return
                if widget_now and hasattr(widget_now, "set_today_progress_state"):
                    widget_now.set_today_progress_state(current_state, enabled=True)
                self.show_message(f"更新“今天是否进行”失败\n{result}")

            self._enqueue_ui_mutation("today_in_progress_toggle", apply_result)

        threading.Thread(target=worker, daemon=True).start()

    def _load_record_from_cache(self, record_id: str) -> dict | None:
        rid = str(record_id or "").strip()
        if not rid:
            return None
        store = getattr(self, "cache_store", None)
        if not store:
            return None
        try:
            data = store.find_record(rid)
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        return self._ensure_active_item_identity(normalize_active_item_data(data))

    def _commit_active_record(
        self,
        data_dict: dict,
        *,
        refresh_detail: bool = True,
        rebuild_widget: bool = True,
        force_status: str | None = None,
        list_widget=None,
        item=None,
    ) -> dict:
        if not isinstance(data_dict, dict):
            return {}
        existing_data = None
        if item and self._is_valid_list_item(item):
            try:
                existing_data = item.data(Qt.ItemDataRole.UserRole) or {}
            except Exception:
                existing_data = None
        data_dict = self._inherit_active_runtime_fields(data_dict, existing_data)
        normalized = self._ensure_active_item_identity(
            normalize_active_item_data(self._preserve_locked_level(data_dict))
        )
        entry = self._build_clipboard_entry(normalized.get("text", "") or "")
        if not entry:
            entry = {"content": normalized.get("text", "")}
        self._ensure_payload_for_data(normalized, entry=entry)

        record_id = str(normalized.get("record_id") or "").strip()
        cache_saved = True
        if getattr(self, "cache_store", None) and record_id:
            try:
                cache_saved = bool(self.cache_store.upsert_record(normalized))
                if not cache_saved:
                    log_warning(
                        f"活动缓存提交失败，保留内存态: record_id={record_id}"
                    )
            except Exception as exc:
                cache_saved = False
                log_warning(
                    f"活动缓存提交异常，保留内存态: record_id={record_id}, error={exc}"
                )
        committed = self._ensure_active_item_identity(
            (self._load_record_from_cache(record_id) if cache_saved else None)
            or normalized
        )

        if not list_widget or not item:
            list_widget, item = self._find_active_item_by_record_id(record_id)
        if item and self._is_valid_list_item(item):
            try:
                item.setData(Qt.ItemDataRole.UserRole, committed)
            except Exception:
                pass
            if rebuild_widget:
                self._rebuild_active_item_widget(
                    list_widget,
                    item,
                    committed,
                    force_status=force_status,
                    upload_in_progress=committed.get("_upload_in_progress"),
                    pending_upload_hash=committed.get("_pending_upload_hash"),
                    has_unuploaded_changes=committed.get("_has_unuploaded_changes"),
                )
        if refresh_detail:
            self._maybe_update_detail_dialog(committed, record_id)
        self._schedule_record_binding_validation(committed)
        self._schedule_active_route_reconcile()
        return committed

    def _maybe_update_detail_dialog(self, data_dict: dict, record_id: str = ""):
        if not isinstance(data_dict, dict):
            return
        rid = record_id or data_dict.get("record_id", "")
        active_item_id = str(data_dict.get("active_item_id") or "").strip()
        display_data = self._load_record_from_cache(rid) or normalize_active_item_data(
            data_dict
        )
        display_data = self._ensure_active_item_identity(display_data)
        if self._is_placeholder_record(data_dict):
            display_data["record_id"] = ""
        self._log_detail_preview_update(display_data, rid, reason="detail_dialog")
        if active_item_id:
            if not self._detail_dialog_matches_active_item(active_item_id):
                return
        elif not self._detail_dialog_matches_record(rid):
            return
        try:
            self.detail_dialog.update_content(
                display_data,
                rid,
                editable=self._is_active_view(),
                active_item_id=active_item_id or display_data.get("active_item_id", ""),
            )
        except Exception:
            return

    def _should_defer_ui_refresh(self) -> bool:
        return bool(
            self.current_screenshot_record_id or self.screenshot_dialog.isVisible()
        )

    def _mark_cache_refresh_needed(self):
        self._pending_cache_refresh = True

    def _schedule_pending_cache_refresh(self):
        self._mark_cache_refresh_needed()
        if getattr(self, "_closing", False):
            return
        if getattr(self, "_cache_refresh_single_shot_pending", False):
            return
        self._cache_refresh_single_shot_pending = True

        def _run():
            self._cache_refresh_single_shot_pending = False
            if getattr(self, "_closing", False):
                return
            if not getattr(self, "_pending_cache_refresh", False):
                return
            try:
                self._refresh_ui_from_cache()
            except Exception as exc:
                log_warning(f"活动条目延迟刷新失败: error={exc}")
                self._mark_cache_refresh_needed()

        QTimer.singleShot(0, _run)

    def _schedule_active_route_reconcile(self):
        if getattr(self, "_closing", False):
            return
        if getattr(self, "_route_reconcile_single_shot_pending", False):
            return
        self._route_reconcile_single_shot_pending = True

        def _run():
            self._route_reconcile_single_shot_pending = False
            if getattr(self, "_closing", False):
                return
            try:
                self._reconcile_active_route_duplicates()
            except Exception as exc:
                log_warning(f"活动条目路由校验失败: error={exc}")

        QTimer.singleShot(0, _run)

    def _apply_cache_to_item(self, list_widget, item, cache_data: dict):
        if not list_widget or not item or not cache_data:
            return
        self._rebuild_active_item_widget(
            list_widget,
            item,
            cache_data,
            force_status=None,
            upload_in_progress=cache_data.get("_upload_in_progress"),
            pending_upload_hash=cache_data.get("_pending_upload_hash"),
            has_unuploaded_changes=cache_data.get("_has_unuploaded_changes"),
        )

    def _repair_missing_item_widgets(self):
        repaired = 0
        for list_widget, item in self._iter_active_items():
            try:
                if not self._is_valid_list_item(item):
                    continue
                widget = self._safe_item_widget(list_widget, item)
                if widget:
                    continue
                data = item.data(Qt.ItemDataRole.UserRole)
                if not isinstance(data, dict):
                    continue
                record_id = str(data.get("record_id") or "").strip()
                cache_data = self._load_record_from_cache(
                    record_id
                ) or self._ensure_active_item_identity(normalize_active_item_data(data))
                rebuilt = self._rebuild_active_item_widget(
                    list_widget,
                    item,
                    cache_data,
                    force_status=None,
                    upload_in_progress=cache_data.get("_upload_in_progress"),
                    pending_upload_hash=cache_data.get("_pending_upload_hash"),
                    has_unuploaded_changes=cache_data.get("_has_unuploaded_changes"),
                )
                if rebuilt:
                    repaired += 1
            except Exception as exc:
                try:
                    log_warning(f"活动条目自愈补建失败: error={exc}")
                except Exception:
                    pass
                self._mark_cache_refresh_needed()
                continue
        return repaired

    def _refresh_detail_from_cache(self):
        try:
            if not self.detail_dialog or not self.detail_dialog.isVisible():
                return
        except Exception:
            return
        active_item_id = getattr(self.detail_dialog, "current_active_item_id", "") or ""
        current_id = getattr(self.detail_dialog, "current_record_id", "") or ""
        list_widget, item = self._find_active_item_by_active_item_id(active_item_id)
        display_data = None
        rid = current_id
        if item and self._is_valid_list_item(item):
            display_data = item.data(Qt.ItemDataRole.UserRole) or {}
            rid = display_data.get("record_id", "") or current_id
        elif active_item_id or not current_id:
            return
        if display_data is None:
            display_data = self._load_record_from_cache(current_id)
        if not isinstance(display_data, dict):
            return
        display_data = self._ensure_active_item_identity(display_data)
        if self._is_placeholder_record(display_data):
            display_data["record_id"] = ""
        self._log_detail_preview_update(display_data, rid, reason="cache_refresh")
        try:
            self.detail_dialog.update_content(
                display_data,
                rid,
                editable=self._is_active_view(),
                active_item_id=display_data.get("active_item_id", active_item_id),
            )
        except Exception:
            return

    def _refresh_ui_from_cache(self):
        if self._closing:
            return
        refresh_error = False
        for list_widget, item in self._iter_active_items():
            try:
                if not self._is_valid_list_item(item):
                    continue
                data = item.data(Qt.ItemDataRole.UserRole)
                if not isinstance(data, dict):
                    continue
                record_id = str(data.get("record_id") or "").strip()
                cache_data = self._load_record_from_cache(
                    record_id
                ) or self._ensure_active_item_identity(normalize_active_item_data(data))
                item.setData(Qt.ItemDataRole.UserRole, cache_data)
                self._apply_cache_to_item(list_widget, item, cache_data)
            except Exception as exc:
                refresh_error = True
                try:
                    rid = ""
                    if isinstance(data, dict):
                        rid = str(data.get("record_id") or "").strip()
                    log_warning(
                        f"活动条目缓存刷新失败: record_id={rid}, error={exc}"
                    )
                except Exception:
                    pass
                self._mark_cache_refresh_needed()
                continue
        self._repair_missing_item_widgets()
        self._reconcile_active_route_duplicates()
        pending_after_repair = bool(getattr(self, "_pending_cache_refresh", False))
        self._refresh_detail_from_cache()
        self._pending_cache_refresh = bool(refresh_error or pending_after_repair)

    def _init_list_widget(self, list_widget):
        list_widget.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        list_widget.customContextMenuRequested.connect(
            lambda pos, lw=list_widget: self.show_context_menu(pos, lw)
        )
        list_widget.itemClicked.connect(self.on_item_clicked)

    def _is_active_view(self):
        return self.stack.currentWidget() == self.active_container

    def _is_event_notice(self, notice_type):
        return notice_type == "事件通告"

    def _get_notice_type(self, data_dict):
        if not data_dict:
            return ""
        notice_type = data_dict.get("notice_type")
        if not notice_type:
            info = extract_event_info(data_dict.get("text", ""))
            notice_type = info.get("notice_type") if info else ""
        return notice_type or ""

    def _get_active_list_for_notice(self, notice_type):
        return (
            self.list_active_event
            if self._is_event_notice(notice_type)
            else self.list_active_other
        )

    def _get_history_list_for_notice(self, notice_type):
        return (
            self.list_history_event
            if self._is_event_notice(notice_type)
            else self.list_history_other
        )

    def _iter_active_items(self):
        for list_widget in (self.list_active_event, self.list_active_other):
            for i in range(list_widget.count()):
                item = list_widget.item(i)
                if not self._is_valid_list_item(item):
                    continue
                yield list_widget, item

    def _iter_history_items(self):
        for list_widget in (self.list_history_event, self.list_history_other):
            for i in range(list_widget.count()):
                item = list_widget.item(i)
                if not self._is_valid_list_item(item):
                    continue
                yield list_widget, item

    def _find_active_item_by_record_id(self, record_id):
        for list_widget, item in self._iter_active_items():
            if not self._is_valid_list_item(item):
                continue
            item_data = item.data(Qt.ItemDataRole.UserRole)
            if item_data.get("record_id") == record_id:
                return list_widget, item
        return None, None

    def _find_active_item_by_active_item_id(self, active_item_id):
        active_item_id = str(active_item_id or "").strip()
        if not active_item_id:
            return None, None
        for list_widget, item in self._iter_active_items():
            if not self._is_valid_list_item(item):
                continue
            item_data = item.data(Qt.ItemDataRole.UserRole)
            if str(item_data.get("active_item_id") or "").strip() == active_item_id:
                return list_widget, item
        return None, None

    def _update_active_item_data(self, record_id, data_dict):
        list_widget, item = self._find_active_item_by_record_id(record_id)
        if item:
            if not self._is_valid_list_item(item):
                return
            committed = self._commit_active_record(
                data_dict,
                refresh_detail=not self._should_defer_ui_refresh(),
                rebuild_widget=False,
                list_widget=list_widget,
                item=item,
            )
            self.save_active_cache()
            if self._should_defer_ui_refresh():
                self._mark_cache_refresh_needed()
            elif committed:
                self._apply_cache_to_item(list_widget, item, committed)

    @staticmethod
    def _is_level_locked(data_dict: dict | None) -> bool:
        if not isinstance(data_dict, dict):
            return False
        notice_type = str(data_dict.get("notice_type") or "").strip()
        if not notice_supports_level_lock(notice_type):
            return False
        return bool(data_dict.get("level_locked"))

    def _reconcile_unlocked_change_notice_level(
        self,
        data_dict: dict | None,
        level: str = "",
        level_locked: bool = False,
        persist: bool = False,
    ) -> str:
        current_level = str(level or "").strip()
        if not isinstance(data_dict, dict):
            return current_level
        notice_type = str(data_dict.get("notice_type") or "").strip()
        if notice_type not in ("设备变更", "变更通告") or level_locked:
            return current_level
        detected_level = str(
            detect_level_from_notice_text(notice_type, data_dict.get("text", "")) or ""
        ).strip()
        if not detected_level:
            return current_level
        if detected_level == current_level:
            return detected_level
        if persist and getattr(self, "cache_store", None):
            record_id = self._get_cache_identity(data_dict)
            if record_id:
                try:
                    self.cache_store.patch_record_fields(
                        record_id=record_id,
                        patch={"level": detected_level},
                    )
                except Exception:
                    pass
        return detected_level

    def _preserve_locked_level(self, data_dict: dict | None) -> dict:
        if not isinstance(data_dict, dict):
            return {}
        preserved = dict(data_dict)
        record_id = self._get_cache_identity(preserved)
        if not record_id or not getattr(self, "cache_store", None):
            return preserved
        try:
            cache_fields = self.cache_store.get_record_fields(
                record_id=record_id,
                fields=["level", "level_locked"],
            )
        except Exception:
            return preserved
        if not bool(cache_fields.get("level_locked")):
            return preserved
        preserved["level_locked"] = True
        cached_level = str(cache_fields.get("level") or "").strip()
        if cached_level:
            preserved["level"] = cached_level
        else:
            preserved.pop("level", None)
        return preserved

    def _apply_detected_notice_fields(self, target_data: dict, info: dict | None) -> dict:
        if not isinstance(target_data, dict) or not isinstance(info, dict):
            return target_data
        if info.get("notice_type"):
            target_data["notice_type"] = info.get("notice_type")
        if not self._is_level_locked(target_data) and info.get("level"):
            target_data["level"] = info.get("level")
        if info.get("source"):
            target_data["source"] = info.get("source")
        if info.get("time_str"):
            target_data["time_str"] = info.get("time_str")
        return target_data

    @staticmethod
    def _normalize_buildings_value(value):
        return normalize_buildings_list(value)

    @staticmethod
    def _infer_buildings_from_text(text: str) -> list[str]:
        normalized_text = unicodedata.normalize("NFKC", text or "").upper()
        if not normalized_text:
            return []
        matched = []
        for char in re.findall(r"([A-E])\s*[栋楼]", normalized_text):
            building = normalize_building_name(f"{char}楼")
            if building not in matched:
                matched.append(building)
        if "110" in normalized_text:
            for token in ("110机房", "110"):
                if token in normalized_text and token not in matched:
                    matched.append(token)
                    break
        return matched

    @staticmethod
    def _extract_section_text(text: str, labels: tuple[str, ...]) -> str:
        raw_text = str(text or "")
        if not raw_text or not labels:
            return ""
        for label in labels:
            safe_label = re.escape(str(label or "").strip())
            if not safe_label:
                continue
            pattern = re.compile(rf"【{safe_label}】(.*?)(?=【|$)", re.DOTALL)
            match = pattern.search(raw_text)
            if not match:
                continue
            section_text = str(match.group(1) or "").strip()
            if section_text:
                return section_text
        return ""

    @classmethod
    def _infer_buildings_from_notice_text(cls, text: str) -> list[str]:
        location_text = cls._extract_section_text(text, ("位置", "地点"))
        buildings = cls._infer_buildings_from_text(location_text)
        if buildings:
            return buildings
        title_text = cls._extract_section_text(text, ("标题", "名称"))
        return cls._infer_buildings_from_text(title_text)

    def _resolve_prefilled_buildings(
        cls,
        existing_value,
        inferred_value,
    ) -> list[str]:
        existing_buildings = cls._normalize_buildings_value(existing_value)
        if existing_buildings:
            return existing_buildings
        return cls._normalize_buildings_value(inferred_value)

    def _validate_cache_record_ids_on_startup(self) -> dict:
        result = {
            "changed": False,
            "saved": False,
            "had_repairs": False,
            "total": 0,
            "missing_fixed": 0,
            "duplicate_fixed": 0,
            "display_had_repairs": False,
            "display_saved": False,
            "display_total": 0,
            "display_changed": 0,
            "level_lock_had_repairs": False,
            "level_lock_saved": False,
            "level_lock_total": 0,
            "level_lock_changed": 0,
            "building_had_repairs": False,
            "building_saved": False,
            "building_total": 0,
            "building_changed": 0,
        }
        if not getattr(self, "cache_store", None):
            return result
        try:
            repair = self.cache_store.validate_or_repair_record_ids()
            if isinstance(repair, dict):
                result.update(repair)
            if result.get("had_repairs"):
                log_warning(
                    "ActiveCache修复: "
                    f"total={result.get('total', 0)} "
                    f"missing_fixed={result.get('missing_fixed', 0)} "
                    f"duplicate_fixed={result.get('duplicate_fixed', 0)} "
                    f"saved={result.get('saved', False)}"
                )
        except Exception as exc:
            log_warning(f"ActiveCache record_id修复失败: {exc}")
        try:
            display_repair = self.cache_store.remove_legacy_display_fields_on_startup()
            if isinstance(display_repair, dict):
                result["display_had_repairs"] = bool(display_repair.get("had_repairs"))
                result["display_saved"] = bool(display_repair.get("saved"))
                result["display_total"] = int(display_repair.get("total", 0))
                result["display_changed"] = int(display_repair.get("changed", 0))
                if result["display_had_repairs"]:
                    result["changed"] = bool(
                        result.get("changed") or result["display_saved"]
                    )
                    log_warning(
                        "ActiveCache展示字段清理: "
                        f"total={result['display_total']} "
                        f"changed={result['display_changed']} "
                        f"saved={result['display_saved']}"
                    )
        except Exception as exc:
            log_warning(f"ActiveCache展示字段清理失败: {exc}")
        try:
            level_lock_repair = self.cache_store.migrate_level_lock_on_startup()
            if isinstance(level_lock_repair, dict):
                result["level_lock_had_repairs"] = bool(
                    level_lock_repair.get("had_repairs")
                )
                result["level_lock_saved"] = bool(level_lock_repair.get("saved"))
                result["level_lock_total"] = int(level_lock_repair.get("total", 0))
                result["level_lock_changed"] = int(
                    level_lock_repair.get("changed", 0)
                )
                if result["level_lock_had_repairs"]:
                    result["changed"] = bool(
                        result.get("changed") or result["level_lock_saved"]
                    )
                    log_warning(
                        "ActiveCache等级锁定迁移: "
                        f"total={result['level_lock_total']} "
                        f"changed={result['level_lock_changed']} "
                        f"saved={result['level_lock_saved']}"
                    )
        except Exception as exc:
            log_warning(f"ActiveCache等级锁定迁移失败: {exc}")
        try:
            building_repair = self.cache_store.normalize_buildings_on_startup()
            if isinstance(building_repair, dict):
                result["building_had_repairs"] = bool(
                    building_repair.get("had_repairs")
                )
                result["building_saved"] = bool(building_repair.get("saved"))
                result["building_total"] = int(building_repair.get("total", 0))
                result["building_changed"] = int(building_repair.get("changed", 0))
                if result["building_had_repairs"]:
                    result["changed"] = bool(
                        result.get("changed") or result["building_saved"]
                    )
                    log_warning(
                        "ActiveCache楼栋规范化: "
                        f"total={result['building_total']} "
                        f"changed={result['building_changed']} "
                        f"saved={result['building_saved']}"
                    )
        except Exception as exc:
            log_warning(f"ActiveCache楼栋规范化失败: {exc}")
        return result

    def _get_cache_identity(self, data_dict: dict) -> str:
        if not isinstance(data_dict, dict):
            return ""
        record_id = str(data_dict.get("record_id") or "").strip()
        return record_id

    def _hydrate_data_from_cache(self, data_dict: dict):
        if not isinstance(data_dict, dict):
            return data_dict
        if not getattr(self, "cache_store", None):
            return data_dict
        record_id = self._get_cache_identity(data_dict)
        if not record_id:
            data_dict["buildings"] = []
            data_dict.pop("specialty", None)
            return data_dict
        cache_fields = self.cache_store.get_record_fields(
            record_id=record_id,
            fields=[
                "buildings",
                "specialty",
                "level",
                "level_locked",
                "today_in_progress_state",
                "record_binding_state",
                "record_binding_error",
                "active_item_id",
                "match_title",
                "match_key",
                "routing_state",
                "routing_error",
                "event_source",
                "transfer_to_overhaul",
            ],
        )
        if "buildings" in cache_fields:
            data_dict["buildings"] = self._normalize_buildings_value(
                cache_fields.get("buildings")
            )
        else:
            data_dict["buildings"] = []
        if "specialty" in cache_fields:
            specialty = str(cache_fields.get("specialty") or "").strip()
            if specialty:
                data_dict["specialty"] = specialty
            else:
                data_dict.pop("specialty", None)
        else:
            data_dict.pop("specialty", None)
        level_locked = bool(cache_fields.get("level_locked")) or bool(
            data_dict.get("level_locked")
        )
        level = ""
        if "level" in cache_fields:
            level = str(cache_fields.get("level") or "").strip()
        level = self._reconcile_unlocked_change_notice_level(
            data_dict,
            level=level,
            level_locked=level_locked,
            persist=True,
        )
        if level:
            data_dict["level"] = level
        else:
            data_dict.pop("level", None)
        if level_locked:
            data_dict["level_locked"] = True
        else:
            data_dict.pop("level_locked", None)
        if "today_in_progress_state" in cache_fields:
            data_dict["today_in_progress_state"] = self._normalize_today_in_progress_state(
                cache_fields.get("today_in_progress_state")
            )
        else:
            data_dict.pop("today_in_progress_state", None)
        binding_state = self._normalize_record_binding_state(
            cache_fields.get("record_binding_state")
        )
        if binding_state:
            data_dict["record_binding_state"] = binding_state
        else:
            data_dict.pop("record_binding_state", None)
        binding_error = str(cache_fields.get("record_binding_error") or "").strip()
        if binding_error:
            data_dict["record_binding_error"] = binding_error
        else:
            data_dict.pop("record_binding_error", None)
        active_item_id = str(cache_fields.get("active_item_id") or "").strip()
        if active_item_id:
            data_dict["active_item_id"] = active_item_id
        match_title = self._normalize_match_text(cache_fields.get("match_title"))
        if match_title:
            data_dict["match_title"] = match_title
        else:
            data_dict.pop("match_title", None)
        match_key = str(cache_fields.get("match_key") or "").strip()
        if match_key:
            data_dict["match_key"] = match_key
        else:
            data_dict.pop("match_key", None)
        routing_state = self._normalize_routing_state(cache_fields.get("routing_state"))
        data_dict["routing_state"] = routing_state
        routing_error = str(cache_fields.get("routing_error") or "").strip()
        if routing_state == "conflicted" and routing_error:
            data_dict["routing_error"] = routing_error
        else:
            data_dict.pop("routing_error", None)
        if "event_source" in cache_fields:
            event_source = str(cache_fields.get("event_source") or "").strip()
            if event_source:
                data_dict["event_source"] = event_source
            else:
                data_dict.pop("event_source", None)
        if "transfer_to_overhaul" in cache_fields:
            data_dict["transfer_to_overhaul"] = bool(
                cache_fields.get("transfer_to_overhaul")
            )
        return self._ensure_active_item_identity(data_dict)

    def _resolve_upload_fields_from_cache(
        self, data_dict: dict, dialog_fields: dict | None = None
    ) -> dict:
        dialog_fields = dialog_fields or {}
        resolved = {
            "buildings": self._normalize_buildings_value(
                dialog_fields.get("buildings")
            ),
            "specialty": str(dialog_fields.get("specialty") or "").strip(),
            "level": str(dialog_fields.get("level") or "").strip(),
            "event_source": str(dialog_fields.get("event_source") or "").strip(),
            "transfer_to_overhaul": None,
        }
        if "transfer_to_overhaul" in dialog_fields:
            resolved["transfer_to_overhaul"] = bool(
                dialog_fields.get("transfer_to_overhaul")
            )
        if not isinstance(data_dict, dict):
            return resolved

        record_id = self._get_cache_identity(data_dict)
        if getattr(self, "cache_store", None) and record_id:
            cache_fields = self.cache_store.get_record_fields(
                record_id=record_id,
                fields=[
                    "buildings",
                    "specialty",
                    "level",
                    "level_locked",
                    "event_source",
                    "transfer_to_overhaul",
                ],
            )
            patch = {}
            if "buildings" not in cache_fields and resolved["buildings"]:
                patch["buildings"] = resolved["buildings"]
            if "specialty" not in cache_fields and resolved["specialty"]:
                patch["specialty"] = resolved["specialty"]
            if "level" not in cache_fields and resolved["level"]:
                patch["level"] = resolved["level"]
            if "event_source" not in cache_fields and resolved["event_source"]:
                patch["event_source"] = resolved["event_source"]
            if (
                "transfer_to_overhaul" not in cache_fields
                and resolved["transfer_to_overhaul"] is not None
            ):
                patch["transfer_to_overhaul"] = resolved["transfer_to_overhaul"]
            if patch:
                self.cache_store.patch_record_fields(
                    record_id=record_id,
                    patch=patch,
                )
                cache_fields = self.cache_store.get_record_fields(
                    record_id=record_id,
                    fields=[
                        "buildings",
                        "specialty",
                        "level",
                        "level_locked",
                        "event_source",
                        "transfer_to_overhaul",
                    ],
                )
            if "buildings" in cache_fields:
                resolved["buildings"] = self._normalize_buildings_value(
                    cache_fields.get("buildings")
                )
            else:
                resolved["buildings"] = []
            if "specialty" in cache_fields:
                resolved["specialty"] = str(cache_fields.get("specialty") or "").strip()
            else:
                resolved["specialty"] = ""
            level_locked = bool(cache_fields.get("level_locked")) or bool(
                data_dict.get("level_locked")
            )
            resolved["level"] = self._reconcile_unlocked_change_notice_level(
                data_dict,
                level=str(cache_fields.get("level") or resolved["level"] or "").strip(),
                level_locked=level_locked,
                persist=True,
            )
            if level_locked:
                data_dict["level_locked"] = True
            else:
                data_dict.pop("level_locked", None)
            if "event_source" in cache_fields:
                resolved["event_source"] = str(
                    cache_fields.get("event_source") or ""
                ).strip()
            if "transfer_to_overhaul" in cache_fields:
                resolved["transfer_to_overhaul"] = bool(
                    cache_fields.get("transfer_to_overhaul")
                )
        else:
            # 缓存主键缺失时，严格留空，避免跨条目串值。
            resolved["buildings"] = []
            resolved["specialty"] = ""

        data_dict["buildings"] = resolved["buildings"]
        if resolved["specialty"]:
            data_dict["specialty"] = resolved["specialty"]
        else:
            data_dict.pop("specialty", None)
        if resolved["level"]:
            data_dict["level"] = resolved["level"]
        elif "level" in dialog_fields:
            data_dict.pop("level", None)
        if resolved["event_source"]:
            data_dict["event_source"] = resolved["event_source"]
        elif "event_source" in dialog_fields:
            data_dict.pop("event_source", None)
        if resolved["transfer_to_overhaul"] is not None:
            data_dict["transfer_to_overhaul"] = bool(resolved["transfer_to_overhaul"])

        return resolved

    def _find_active_item_by_content_or_title(
        self,
        content: str,
        title: str = "",
        notice_type: str = "",
        unique_key: str = "",
    ):
        content_clean = (content or "").strip()
        info = extract_event_info(content_clean) or {}
        resolved_notice_type = str(notice_type or info.get("notice_type") or "").strip()
        resolved_time = str(info.get("time_str") or "").strip()
        match_title, match_key = self._build_match_identity(
            notice_type=resolved_notice_type,
            title=title,
            time_str=resolved_time,
            text=content_clean,
        )
        if not content_clean and not match_title and not match_key:
            return None, None
        key_matches = []
        title_matches = []
        for list_widget, item in self._iter_active_items():
            try:
                if not self._is_valid_list_item(item):
                    continue
                data = self._ensure_active_item_identity(
                    item.data(Qt.ItemDataRole.UserRole)
                )
            except Exception:
                continue
            if not data:
                continue
            if (
                resolved_notice_type
                and data.get("notice_type")
                and data.get("notice_type") != resolved_notice_type
            ):
                continue
            old_text = (data.get("text") or "").strip()
            if content_clean and old_text and old_text == content_clean:
                return list_widget, item
            item_match_key = str(data.get("match_key") or "").strip()
            item_match_title = self._normalize_match_text(data.get("match_title"))
            if match_key and item_match_key and item_match_key == match_key:
                key_matches.append((list_widget, item))
                continue
            if match_title and item_match_title and item_match_title == match_title:
                title_matches.append((list_widget, item))
        if key_matches:
            return key_matches[0]
        if self._is_event_notice(resolved_notice_type):
            return None, None
        if title_matches:
            return title_matches[0]
        return None, None

    def _find_active_item_by_content(self, content: str):
        content_clean = (content or "").translate(WHITESPACE_TRANSLATOR)
        if not content_clean:
            return None, None
        for list_widget, item in self._iter_active_items():
            data = item.data(Qt.ItemDataRole.UserRole)
            if not data:
                continue
            old_text = (data.get("text") or "").translate(WHITESPACE_TRANSLATOR)
            if old_text and old_text == content_clean:
                return list_widget, item
        return None, None

    def _is_valid_list_item(self, item) -> bool:
        if item is None:
            return False
        try:
            list_widget = item.listWidget()
        except Exception:
            return False
        if list_widget is None:
            return False
        try:
            return list_widget.row(item) != -1
        except Exception:
            return False

    def _is_placeholder_record(self, data_dict):
        return bool(data_dict.get("_is_placeholder_record"))

    def add_active_item(self, data_dict, insert_top=True, skip_cache=False):
        data_dict = self._ensure_active_item_identity(normalize_active_item_data(data_dict))
        if not skip_cache:
            data_dict = (
                self._commit_active_record(
                    data_dict,
                    refresh_detail=False,
                    rebuild_widget=False,
                )
                or data_dict
            )
        notice_type = self._get_notice_type(data_dict)
        self._ensure_payload_for_data(data_dict)
        list_widget = self._get_active_list_for_notice(notice_type)
        item = QListWidgetItem()
        widget = None
        try:
            widget = ClipboardItemWidget(data_dict)
            widget.action_clicked.connect(self.handle_action)
            widget.today_progress_clicked.connect(self._handle_today_in_progress_toggle)
            widget.ended_signal.connect(self.move_to_history)
            widget.delete_requested.connect(self._delete_active_item)
            if hasattr(widget, "set_delete_interaction_enabled"):
                widget.set_delete_interaction_enabled(self._delete_interaction_enabled)
        except Exception as exc:
            record_id = str(data_dict.get("record_id") or "").strip()
            log_warning(
                f"活动条目创建失败，未插入列表: notice_type={notice_type}, "
                f"record_id={record_id}, error={exc}"
            )
            try:
                if widget:
                    widget.deleteLater()
            except Exception:
                pass
            self._schedule_pending_cache_refresh()
            return None, None
        try:
            item.setSizeHint(QSize(420, 80))
            item.setData(Qt.ItemDataRole.UserRole, data_dict)
            if insert_top:
                list_widget.insertItem(0, item)
            else:
                list_widget.addItem(item)
            list_widget.setItemWidget(item, widget)
        except Exception as exc:
            record_id = str(data_dict.get("record_id") or "").strip()
            log_warning(
                f"活动条目挂载失败，已回滚插入: notice_type={notice_type}, "
                f"record_id={record_id}, error={exc}"
            )
            try:
                row = list_widget.row(item)
                if row != -1:
                    list_widget.takeItem(row)
            except Exception:
                pass
            try:
                if widget:
                    widget.deleteLater()
            except Exception:
                pass
            self._schedule_pending_cache_refresh()
            return None, None
        # 根据内容状态设置按钮
        info = extract_event_info(data_dict.get("text", ""))
        if info and info.get("status") == "结束":
            widget.set_button_to_end_mode()
        elif not data_dict.get("_is_placeholder_record", True):
            widget.set_button_to_update_mode()
        if data_dict.get("_has_unuploaded_changes") is False:
            widget.mark_as_uploaded()
        self._schedule_today_in_progress_sync(data_dict)
        self._schedule_record_binding_validation(data_dict)
        self._schedule_active_route_reconcile()
        if not skip_cache:
            self.save_active_cache()
        return item, widget

    def sync_record_id_to_widget(self, old_record_id, new_record_id):
        if not old_record_id or not new_record_id:
            return
        try:
            changed = self._replace_record_id_everywhere(old_record_id, new_record_id)
            if changed:
                self.save_active_cache()
        except Exception:
            return

    def find_widget_by_record_id(self, rid):
        for list_widget, item in self._iter_active_items():
            try:
                if not self._is_valid_list_item(item):
                    continue
                if item.data(Qt.ItemDataRole.UserRole).get("record_id") == rid:
                    return self._safe_item_widget(list_widget, item)
            except Exception:
                continue
        return None

    def sync_content_to_widget(self, active_item_id, record_id=None, new_data=None):
        if isinstance(record_id, dict) and new_data is None:
            new_data = record_id
            record_id = active_item_id
            active_item_id = ""
        active_item_id = str(active_item_id or "").strip()
        record_id = str(record_id or "").strip()
        if not active_item_id and not record_id:
            return
        list_widget, item_ref = self._find_active_item_by_active_item_id(active_item_id)
        if not item_ref and record_id:
            list_widget, item_ref = self._find_active_item_by_record_id(record_id)
        if not item_ref:
            return
        if not self._is_valid_list_item(item_ref):
            return
        try:
            old_data = item_ref.data(Qt.ItemDataRole.UserRole)
            if old_data and isinstance(new_data, dict):
                merged = dict(old_data)
                merged.update(new_data)
                new_data = merged
            if isinstance(new_data, dict) and old_data:
                new_data["active_item_id"] = old_data.get("active_item_id")
            new_data["_has_unuploaded_changes"] = True
            new_data["_pending_upload_hash"] = None
            new_data["_upload_in_progress"] = False
            self._commit_active_record(
                new_data,
                refresh_detail=True,
                rebuild_widget=True,
                list_widget=list_widget,
                item=item_ref,
            )
            self.save_active_cache()
        except Exception:
            return

    def restore_button_state(self, success=False, name=None, record_id=None):
        self._set_last_ui_op(
            "restore_button_state",
            success=success,
            name=name,
            record_id=record_id,
        )
        if self._closing:
            return
        if record_id:
            if record_id in self.pending_action_record_ids:
                self.pending_action_record_ids.remove(record_id)
            self.pending_action_types.pop(record_id, None)
            if success:
                self.pending_upload_rollback_by_record_id.pop(record_id, None)
            list_widget, item = self._find_active_item_by_record_id(record_id)
            if item and not self._is_valid_list_item(item):
                item = None
                list_widget = None
            if list_widget and item:
                data = item.data(Qt.ItemDataRole.UserRole) or {}
                if not success:
                    rollback = self.pending_upload_rollback_by_record_id.pop(
                        record_id, None
                    )
                    if rollback and rollback.get("old_data"):
                        data = rollback.get("old_data") or data
                        data["_pending_upload_hash"] = None
                        data["_has_unuploaded_changes"] = True
                        data["_upload_in_progress"] = False
                        item.setData(Qt.ItemDataRole.UserRole, data)
                        self._rebuild_active_item_widget(
                            list_widget,
                            item,
                            data,
                            force_status=None,
                            upload_in_progress=False,
                            pending_upload_hash=None,
                            has_unuploaded_changes=True,
                        )
                if not success:
                    data["_has_unuploaded_changes"] = True
                has_unuploaded_changes = data.get("_has_unuploaded_changes")

                data["_pending_upload_hash"] = None
                data["_upload_in_progress"] = False
                item.setData(Qt.ItemDataRole.UserRole, data)
                self._rebuild_active_item_widget(
                    list_widget,
                    item,
                    data,
                    force_status=None,
                    upload_in_progress=False,
                    pending_upload_hash=None,
                    has_unuploaded_changes=has_unuploaded_changes,
                )

            if self.current_screenshot_record_id == record_id:
                self.current_screenshot_record_id = None
                self.current_screenshot_action_type = None
            self.save_active_cache()
        else:
            # 清除所有 pending 状态 (慎用，通常只在完全重置时)
            self._set_last_ui_op("restore_all_skipped")
            return


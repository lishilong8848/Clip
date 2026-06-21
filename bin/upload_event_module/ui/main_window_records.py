# -*- coding: utf-8 -*-
import threading
import time
import re
import unicodedata
import uuid
import os
import queue
from PyQt6.QtWidgets import QInputDialog
from PyQt6.QtCore import Qt, QSize, QTimer, QPoint, QObject
from PyQt6 import sip

from ..config import get_field_config
from ..logger import LOG_FILE, log_error, log_info, log_warning
from ..utils import WHITESPACE_TRANSLATOR
from ..core.parser import extract_event_info
from ..time_parser import parse_time_range
from ..building_normalizer import (
    normalize_building_name,
    normalize_buildings_value as normalize_buildings_list,
)
from .display_state import (
    detect_level_from_notice_text,
    normalize_active_item_data,
    notice_supports_level_lock,
)
from .active_notice_store import ActiveNoticeStore
from .active_notice_model import ActiveNoticeListRoute, ActiveNoticeModel, ActiveNoticeModelItem

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

    def request_active_cache_save(self, delay_ms: int = 800, *, force: bool = False):
        save = getattr(self, "save_active_cache", None)
        if force:
            if callable(save):
                save()
            return
        schedule = getattr(self, "schedule_active_cache_save", None)
        if callable(schedule):
            schedule(delay_ms)
            return
        if callable(save):
            save()

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
        self.upload_state_watchdog_timer = QTimer(self)
        self.upload_state_watchdog_timer.timeout.connect(
            self._recover_stale_upload_states
        )
        self.upload_state_watchdog_timer.start(15 * 1000)

    def _collect_live_runtime_record_ids(self) -> set[str]:
        record_ids = set()
        try:
            active_snapshot = self._active_notice_store().data_snapshot()
        except Exception:
            active_snapshot = []
        for data in active_snapshot:
            if not isinstance(data, dict):
                continue
            if self._is_placeholder_record(data):
                continue
            for key in ("record_id", "target_record_id"):
                record_id = str(data.get(key) or "").strip()
                if record_id:
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
        ):
            if isinstance(mapping, dict):
                busy.update(
                    str(key or "").strip()
                    for key in mapping.keys()
                    if str(key or "").strip()
                )
        return busy

    def _upload_state_is_busy(self, data: dict) -> bool:
        if not isinstance(data, dict):
            return False
        record_ids = set()
        for key in ("record_id", "target_record_id", "feishu_record_id", "raw_record_id"):
            value = str(data.get(key) or "").strip()
            if value:
                record_ids.update(self._upload_completion_record_id_candidates(value))
        record_ids = {value for value in record_ids if value}
        if not record_ids:
            return False
        current_screenshot_record_id = str(
            getattr(self, "current_screenshot_record_id", "") or ""
        ).strip()
        if current_screenshot_record_id in record_ids:
            return True
        for mapping_name in (
            "pending_action_types",
            "pending_upload_rollback_by_record_id",
            "pending_new_by_record_id",
            "pending_update_after_upload",
        ):
            mapping = getattr(self, mapping_name, {})
            if isinstance(mapping, dict) and any(
                record_id in mapping for record_id in record_ids
            ):
                return True
        pending_action_ids = getattr(self, "pending_action_record_ids", set())
        if isinstance(pending_action_ids, set) and record_ids & pending_action_ids:
            return True
        has_pending_upload = getattr(self, "_has_pending_upload", None)
        if callable(has_pending_upload):
            for record_id in record_ids:
                try:
                    if has_pending_upload(record_id):
                        return True
                except Exception:
                    continue
        return False

    @staticmethod
    def _upload_state_hard_timeout_seconds() -> float:
        try:
            value = int(
                str(
                    os.environ.get(
                        "CLIPFLOW_QT_UPLOAD_STATE_HARD_TIMEOUT_SECONDS",
                        "300",
                    )
                    or "300"
                ).strip()
            )
        except Exception:
            value = 300
        return float(max(60, min(value, 3600)))

    def _recover_stale_upload_states(self) -> dict[str, int]:
        recovered = 0
        now = time.monotonic()
        try:
            entries = list(self._active_notice_store().entries())
        except Exception:
            entries = []
        hard_timeout = self._upload_state_hard_timeout_seconds()
        for list_widget, item, data in entries:
            if not self._is_valid_list_item(item) or not isinstance(data, dict):
                continue
            if not bool(data.get("_upload_in_progress")):
                continue
            if self._upload_state_is_busy(data):
                started_at = float(data.get("_upload_started_monotonic") or 0.0)
                dialog_active = False
                is_dialog_active = getattr(self, "_is_screenshot_dialog_active", None)
                if callable(is_dialog_active):
                    try:
                        dialog_active = bool(is_dialog_active())
                    except Exception:
                        dialog_active = False
                if dialog_active:
                    continue
                if not started_at or now - started_at < hard_timeout:
                    continue
                clear_state = getattr(self, "clear_upload_runtime_state_for_ids", None)
                if callable(clear_state):
                    clear_state(
                        data.get("record_id"),
                        data.get("target_record_id"),
                        data.get("feishu_record_id"),
                        data.get("raw_record_id"),
                    )
                    mark_failed = getattr(self, "_mark_upload_state_failed_for_ids", None)
                    if callable(mark_failed):
                        mark_failed(
                            "上传状态超时，后端仍以任务为准；请刷新核对后重试。",
                            data.get("record_id"),
                            data.get("target_record_id"),
                            data.get("feishu_record_id"),
                            data.get("raw_record_id"),
                        )
                    recovered += 1
                    continue
            started_at = float(data.get("_upload_started_monotonic") or 0.0)
            if started_at and now - started_at < 5.0:
                continue
            data = dict(data)
            data["_upload_in_progress"] = False
            data["_pending_upload_hash"] = None
            data["_has_unuploaded_changes"] = True
            data["_last_upload_error"] = "上传状态超时，已恢复按钮，可刷新核对后重试。"
            data.pop("_upload_started_monotonic", None)
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
            recovered += 1
        if recovered:
            log_warning(f"上传状态自愈: 已恢复 {recovered} 条无队列上传中条目")
            try:
                self.request_active_cache_save()
            except Exception:
                pass
        return {"stale_upload_recovered": recovered}

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
        try:
            active_count = len(self._active_notice_store().data_snapshot())
        except Exception:
            active_count = 0
        log_info(
            "RuntimeHealth[%s]: active_items=%s validated=%s synced=%s payload_store=%s payload_alias=%s clipboard=%s app_log_bytes=%s"
            % (
                reason,
                active_count,
                len(getattr(self, "_record_binding_validated_ids", set())),
                len(getattr(self, "_today_in_progress_synced_record_ids", set())),
                len(getattr(self, "_payload_store", {})),
                len(getattr(self, "_payload_alias", {})),
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
            stats.update(self._recover_stale_upload_states())
        except Exception as exc:
            log_warning(f"运行态维护: 上传状态自愈失败 error={exc}")
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
        return None

    def _is_active_list_widget(self, list_widget) -> bool:
        return list_widget in (
            getattr(self, "list_active_event", None),
            getattr(self, "list_active_other", None),
        )

    def _active_notice_model_enabled(self) -> bool:
        return True

    def _active_item_widgets_required(self) -> bool:
        return False

    def _active_notice_model_for_list(self, list_widget):
        if not self._active_notice_model_enabled():
            return None
        if list_widget is getattr(self, "list_active_event", None):
            attr = "_active_notice_event_model"
        elif list_widget is getattr(self, "list_active_other", None):
            attr = "_active_notice_other_model"
        else:
            return None
        model = getattr(self, attr, None)
        if not isinstance(model, ActiveNoticeModel):
            model = ActiveNoticeModel(self if isinstance(self, QObject) else None)
            setattr(self, attr, model)
        return model

    def _active_model_item(self, list_widget, data_dict: dict | None):
        model = self._active_notice_model_for_list(list_widget)
        identity = ActiveNoticeModel.identity_for_record(data_dict)
        if model is None or not identity:
            return None
        if model.row_for_identity(identity) < 0:
            return None
        return ActiveNoticeModelItem(list_widget, model, identity)

    def _active_item_row(self, list_widget, item) -> int:
        if isinstance(item, ActiveNoticeModelItem):
            return item.row()
        if list_widget is None or item is None:
            return -1
        try:
            return list_widget.row(item)
        except Exception:
            return -1

    def _active_item_data(self, item):
        if not item:
            return None
        try:
            data = item.data(Qt.ItemDataRole.UserRole)
        except Exception:
            data = None
        return dict(data) if isinstance(data, dict) else None

    def _set_active_item_data(self, list_widget, item, data_dict: dict) -> bool:
        if item is None or not isinstance(data_dict, dict):
            return False
        try:
            return bool(item.setData(Qt.ItemDataRole.UserRole, dict(data_dict)))
        except Exception:
            return False

    def _remove_active_item_from_source(self, list_widget, item) -> bool:
        if list_widget is None or item is None:
            return False
        if isinstance(item, ActiveNoticeModelItem):
            model = self._active_notice_model_for_list(list_widget)
            data = self._active_item_data(item)
            if model is None or not isinstance(data, dict):
                return False
            return bool(model.remove_record(data))
        return False

    def _move_active_item_to_row(self, list_widget, item, target_row: int) -> bool:
        if list_widget is None or item is None:
            return False
        target_row = max(0, int(target_row or 0))
        if isinstance(item, ActiveNoticeModelItem):
            model = self._active_notice_model_for_list(list_widget)
            if model is None:
                return False
            return bool(model.move_record(item.identity(), target_row))
        return False

    def _sync_active_notice_model_for_list(self, list_widget) -> None:
        return

    def _sync_all_active_notice_models(self) -> None:
        self._sync_active_notice_model_for_list(getattr(self, "list_active_event", None))
        self._sync_active_notice_model_for_list(getattr(self, "list_active_other", None))

    def _upsert_active_notice_model_item(self, list_widget, item, data_dict=None) -> None:
        model = self._active_notice_model_for_list(list_widget)
        if model is None or list_widget is None or item is None:
            return
        row = self._active_item_row(list_widget, item)
        if row < 0:
            return
        data = data_dict
        if not isinstance(data, dict):
            data = self._active_item_data(item)
        if isinstance(data, dict):
            model.upsert_record(dict(data), row=row)

    def _remove_active_notice_model_record(self, data_dict) -> None:
        if not isinstance(data_dict, dict):
            return
        for list_widget in (
            getattr(self, "list_active_event", None),
            getattr(self, "list_active_other", None),
        ):
            model = self._active_notice_model_for_list(list_widget)
            if model is not None and model.remove_record(data_dict):
                return

    def _active_list_virtualization_enabled(self) -> bool:
        visible = getattr(self, "_active_model_view_visible", None)
        if callable(visible) and visible():
            return False
        if os.environ.get("CLIPFLOW_DISABLE_ACTIVE_LIST_VIRTUALIZATION") == "1":
            return False
        return True

    @staticmethod
    def _active_list_virtual_buffer_rows() -> int:
        try:
            value = int(os.environ.get("CLIPFLOW_ACTIVE_LIST_VIRTUAL_BUFFER_ROWS", "8") or 8)
        except Exception:
            value = 8
        return max(2, min(value, 50))

    @staticmethod
    def _active_list_virtual_cleanup_batch_size() -> int:
        try:
            value = int(
                os.environ.get("CLIPFLOW_ACTIVE_LIST_VIRTUAL_CLEANUP_BATCH_SIZE", "80")
                or 80
            )
        except Exception:
            value = 80
        return max(20, min(value, 500))

    @staticmethod
    def _active_list_scroll_refresh_delay_ms() -> int:
        try:
            value = int(
                os.environ.get("CLIPFLOW_ACTIVE_LIST_SCROLL_REFRESH_DELAY_MS", "40")
                or 40
            )
        except Exception:
            value = 40
        return max(0, min(value, 250))

    def _active_list_visible_row_bounds(self, list_widget) -> tuple[int, int]:
        count = list_widget.count() if list_widget else 0
        if count <= 0:
            return 0, -1
        buffer_rows = self._active_list_virtual_buffer_rows()
        try:
            viewport = list_widget.viewport()
            top_index = list_widget.indexAt(QPoint(1, 1))
            bottom_index = list_widget.indexAt(QPoint(1, max(1, viewport.height() - 2)))
            top = top_index.row() if top_index.isValid() else 0
            if bottom_index.isValid():
                bottom = bottom_index.row()
            else:
                approx_rows = max(1, int((viewport.height() or 80) / 80) + 2)
                bottom = min(count - 1, top + approx_rows)
        except Exception:
            top = 0
            bottom = min(count - 1, 20)
        return max(0, top - buffer_rows), min(count - 1, bottom + buffer_rows)

    def _active_item_in_virtual_range(self, list_widget, item) -> bool:
        if list_widget is None or item is None or not self._is_valid_list_item(item):
            return False
        start, end = self._active_list_visible_row_bounds(list_widget)
        if end < start:
            return False
        row = self._active_item_row(list_widget, item)
        if row < 0:
            return False
        return start <= row <= end

    def _create_active_item_widget(self, data_dict):
        return None

    def _apply_active_widget_state(
        self,
        widget,
        data_dict,
        *,
        force_status: str | None = None,
        upload_in_progress: bool | None = None,
        pending_upload_hash: str | None = None,
        has_unuploaded_changes: bool | None = None,
    ) -> None:
        if not widget:
            return
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
        if upload_in_progress is not None:
            widget.upload_in_progress = bool(upload_in_progress)
        if pending_upload_hash is not None:
            widget.pending_upload_hash = pending_upload_hash
        if has_unuploaded_changes is not None:
            widget.has_unuploaded_changes = bool(has_unuploaded_changes)
        status = force_status
        if not status:
            info = extract_event_info(data_dict.get("text", ""))
            if info and info.get("status") == "结束":
                status = "end"
            elif not data_dict.get("_is_placeholder_record", True):
                status = "update"
        if status == "end":
            widget.set_button_to_end_mode()
        elif status == "update":
            widget.set_button_to_update_mode()
        elif has_unuploaded_changes is False:
            widget.set_uploaded_visual(True)

    def _sync_item_data_from_widget(self, list_widget, item) -> None:
        return

    def _unmount_active_item_widget(self, list_widget, item) -> None:
        return

    def _ensure_active_item_widget(self, list_widget, item):
        return None

    def _schedule_active_list_virtualization_refresh(self, list_widget=None, delay_ms: int = 0):
        if not self._active_list_virtualization_enabled():
            return
        if list_widget is not None and not self._is_active_list_widget(list_widget):
            return
        if list_widget is None:
            self._active_list_virtual_refresh_all = True
        else:
            targets = getattr(self, "_active_list_virtual_refresh_targets", None)
            if not isinstance(targets, list):
                targets = []
            if list_widget not in targets:
                targets.append(list_widget)
            self._active_list_virtual_refresh_targets = targets
        if getattr(self, "_active_list_virtual_refresh_pending", False):
            if int(delay_ms or 0) > 0 and not getattr(
                self, "_active_list_virtual_delayed_pending", False
            ):
                self._active_list_virtual_delayed_pending = True

                def _delayed_run():
                    self._active_list_virtual_delayed_pending = False
                    self._schedule_active_list_virtualization_refresh(list_widget, 0)

                QTimer.singleShot(max(1, int(delay_ms or 0)), _delayed_run)
            return
        self._active_list_virtual_refresh_pending = True

        def _run():
            self._active_list_virtual_refresh_pending = False
            refresh_all = bool(getattr(self, "_active_list_virtual_refresh_all", False))
            targets = list(getattr(self, "_active_list_virtual_refresh_targets", []) or [])
            self._active_list_virtual_refresh_all = False
            self._active_list_virtual_refresh_targets = []
            if refresh_all or not targets:
                self._refresh_active_list_virtualization(None)
                return
            for target in targets:
                self._refresh_active_list_virtualization(target)

        QTimer.singleShot(max(0, int(delay_ms or 0)), _run)

    def _refresh_active_list_virtualization(self, list_widget=None):
        if not self._active_list_virtualization_enabled():
            return
        targets = [list_widget] if list_widget is not None else [
            getattr(self, "list_active_event", None),
            getattr(self, "list_active_other", None),
        ]
        for target in targets:
            if not target or not self._is_active_list_widget(target):
                continue
            start, end = self._active_list_visible_row_bounds(target)
            if end >= start:
                for row in range(start, end + 1):
                    item = target.item(row)
                    if not self._is_valid_list_item(item):
                        continue
                    self._ensure_active_item_widget(target, item)
            self._schedule_active_list_virtual_cleanup(target, start, end)

    def _schedule_active_list_virtual_cleanup(self, list_widget, visible_start: int, visible_end: int):
        if not list_widget or not self._is_active_list_widget(list_widget):
            return
        states = getattr(self, "_active_list_virtual_cleanup_states", None)
        if not isinstance(states, dict):
            states = {}
            self._active_list_virtual_cleanup_states = states
        key = id(list_widget)
        old_state = states.get(key) or {}
        generation = int(old_state.get("generation") or 0) + 1
        states[key] = {
            "list_widget": list_widget,
            "visible_start": max(0, int(visible_start or 0)),
            "visible_end": int(visible_end if visible_end is not None else -1),
            "next_row": 0,
            "generation": generation,
        }
        QTimer.singleShot(
            0,
            lambda key=key, generation=generation: self._run_active_list_virtual_cleanup(
                key, generation
            ),
        )

    def _run_active_list_virtual_cleanup(self, key, generation):
        if not self._active_list_virtualization_enabled():
            return
        states = getattr(self, "_active_list_virtual_cleanup_states", None)
        if not isinstance(states, dict):
            return
        state = states.get(key)
        if not state or int(state.get("generation") or 0) != int(generation or 0):
            return
        target = state.get("list_widget")
        if not target or not self._is_active_list_widget(target):
            states.pop(key, None)
            return
        try:
            if sip.isdeleted(target):
                states.pop(key, None)
                return
        except Exception:
            states.pop(key, None)
            return
        count = target.count()
        start = int(state.get("visible_start") or 0)
        end = int(state.get("visible_end") if state.get("visible_end") is not None else -1)
        row = max(0, int(state.get("next_row") or 0))
        batch_size = self._active_list_virtual_cleanup_batch_size()
        processed = 0
        while row < count and processed < batch_size:
            if row < start or row > end:
                item = target.item(row)
                if not self._is_valid_list_item(item):
                    row += 1
                    processed += 1
                    continue
                self._unmount_active_item_widget(target, item)
            row += 1
            processed += 1
        if row < count:
            state["next_row"] = row
            QTimer.singleShot(
                25,
                lambda key=key, generation=generation: self._run_active_list_virtual_cleanup(
                    key, generation
                ),
            )
            return
        states.pop(key, None)

    def _refresh_existing_active_item_widget(
        self,
        list_widget,
        item,
        data_dict,
        *,
        force_status: str | None = None,
        upload_in_progress: bool | None = None,
        pending_upload_hash: str | None = None,
        has_unuploaded_changes: bool | None = None,
    ):
        widget = self._safe_item_widget(list_widget, item)
        if not widget or not hasattr(widget, "refresh_data"):
            return None
        try:
            old_data = item.data(Qt.ItemDataRole.UserRole) or {}
        except Exception:
            old_data = {}
        old_notice_type = str((old_data or {}).get("notice_type") or "").strip()
        new_notice_type = str((data_dict or {}).get("notice_type") or "").strip()
        if old_notice_type != new_notice_type:
            return None
        if new_notice_type == "事件通告" and not getattr(widget, "timer_widget", None):
            return None
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
            widget.refresh_data(data_dict)
        except Exception:
            return None
        if upload_in_progress is not None:
            widget.upload_in_progress = bool(upload_in_progress)
        if pending_upload_hash is not None:
            widget.pending_upload_hash = pending_upload_hash
        if has_unuploaded_changes is not None:
            widget.has_unuploaded_changes = bool(has_unuploaded_changes)
        status = force_status
        if not status:
            info = extract_event_info(data_dict.get("text", ""))
            if info and info.get("status") == "结束":
                status = "end"
            elif not data_dict.get("_is_placeholder_record", True):
                status = "update"
        if status == "end":
            widget.set_button_to_end_mode()
        elif status == "update":
            widget.set_button_to_update_mode()
        else:
            widget.is_updated_status = False
            widget.is_end_status = False
            if has_unuploaded_changes is False:
                widget.set_uploaded_visual(True)
            elif hasattr(widget, "_refresh_action_text"):
                widget._refresh_action_text()
        if hasattr(widget, "set_delete_interaction_enabled"):
            widget.set_delete_interaction_enabled(self._delete_interaction_enabled)
        self._schedule_today_in_progress_sync(data_dict)
        return widget

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
        if list_widget is None or item is None or not data_dict:
            return None
        if not self._is_valid_list_item(item):
            return None
        if has_unuploaded_changes is not None:
            data_dict["_has_unuploaded_changes"] = bool(has_unuploaded_changes)
            if not data_dict["_has_unuploaded_changes"]:
                data_dict["_pending_upload_hash"] = None
                pending_upload_hash = None
        if upload_in_progress is not None:
            data_dict["_upload_in_progress"] = bool(upload_in_progress)
        if pending_upload_hash is not None:
            data_dict["_pending_upload_hash"] = pending_upload_hash

        try:
            item.setSizeHint(QSize(420, 88))
            item.setData(Qt.ItemDataRole.UserRole, data_dict)
        except Exception:
            return None
        self._upsert_active_notice_model_item(list_widget, item, data_dict)
        self._schedule_today_in_progress_sync(data_dict)
        return None

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
        return getattr(list_widget, "objectName", lambda: "")() or "unknown_list"

    def _debug_list_item(self, list_widget, item):
        row = (
            self._active_item_row(list_widget, item)
            if list_widget is not None and item is not None
            else None
        )
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
        except Exception:
            pass

    def _record_slow_ui_operation(self, label: str, elapsed_ms: float):
        try:
            self._ui_slow_count = int(getattr(self, "_ui_slow_count", 0) or 0) + 1
            now = time.time()
            last_log = float(getattr(self, "_ui_slow_last_log_ts", 0.0) or 0.0)
            interval = float(getattr(self, "_ui_slow_log_interval_s", 10.0) or 10.0)
            if now - last_log < interval:
                return
            self._ui_slow_last_log_ts = now
            log_error(
                "Qt主线程操作耗时过长: "
                f"op={label or '-'}, elapsed_ms={elapsed_ms:.1f}, "
                f"slow_count={int(getattr(self, '_ui_slow_count', 0) or 0)}, "
                f"last_ui_op={getattr(self, '_last_ui_op', '') or '-'}"
            )
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
            signature = (
                str(reason or ""),
                str(rid or ""),
                str(display_data.get("active_item_id") or ""),
                text,
            )
            if getattr(self, "_last_detail_preview_log_signature", None) == signature:
                return
            self._last_detail_preview_log_signature = signature
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
        resolved_reason = self._normalize_match_text(
            info.get("reason")
            or self._extract_section_text(text, ("原因", "故障原因", "故障维修原因"))
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
        if resolved_notice_type == "维保通告" and resolved_reason:
            match_title = f"{resolved_title}|原因:{resolved_reason}"
            return (
                match_title,
                f"{resolved_notice_type}|{resolved_title}|{resolved_reason}",
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
        # record_id is the remote identity. Title is editable locally and may
        # intentionally diverge from the remote title before the next update.
        # Do not use title mismatch alone to mark a binding conflict.
        strong_mismatches = []
        strong_comparable_count = 0
        for key in ("time_anchor",):
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
        *,
        persist_cache: bool = True,
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
        if persist_cache and getattr(self, "cache_store", None):
            try:
                self.cache_store.patch_record_fields(record_id=record_id, patch=patch)
            except Exception:
                pass
        list_widget, item = self._find_active_item_by_record_id(record_id)
        if item and self._is_valid_list_item(item):
            data = item.data(Qt.ItemDataRole.UserRole) or {}
            updated = dict(data)
            old_state = self._normalize_record_binding_state(
                updated.get("record_binding_state")
            )
            old_error = str(updated.get("record_binding_error") or "").strip()
            updated["record_binding_state"] = normalized_state
            if normalized_state == "conflicted":
                updated["record_binding_error"] = patch["record_binding_error"]
            else:
                updated.pop("record_binding_error", None)
            new_error = str(updated.get("record_binding_error") or "").strip()
            if old_state == normalized_state and old_error == new_error:
                return
            try:
                item.setData(Qt.ItemDataRole.UserRole, updated)
            except Exception:
                pass
            widget = self._safe_item_widget(list_widget, item)
            if widget is not None and hasattr(widget, "data"):
                try:
                    widget.data = updated
                except Exception:
                    pass
            if normalized_state == "conflicted":
                self._maybe_update_detail_dialog(updated, record_id)

    @staticmethod
    def _is_missing_remote_record_error(error_text: str) -> bool:
        text = str(error_text or "").strip()
        lowered = text.lower()
        return (
            "1254043" in text
            or "1254006" in text
            or "record_id 不存在" in text
            or "记录不存在" in text
            or "record id 不存在" in lowered
        )

    def _schedule_record_binding_validation(self, data_dict: dict | None):
        if os.environ.get("CLIPFLOW_QT_REMOTE_VALIDATION", "0") != "1":
            return
        if bool(getattr(self, "_active_cache_restore_in_progress", False)):
            return
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
        try:
            self._record_binding_validation_queue.put_nowait(dict(data_dict))
        except Exception:
            self._record_binding_validation_pending_ids.discard(record_id)
            return
        self._ensure_record_binding_validation_worker()

    def _ensure_record_binding_validation_worker(self):
        worker = getattr(self, "_record_binding_validation_worker", None)
        if worker and worker.is_alive():
            return
        worker = threading.Thread(
            target=self._record_binding_validation_loop,
            name="RecordBindingValidationWorker",
            daemon=True,
        )
        self._record_binding_validation_worker = worker
        worker.start()

    def _record_binding_validation_loop(self):
        validation_queue = getattr(self, "_record_binding_validation_queue", None)
        if validation_queue is None:
            return
        while not bool(getattr(self, "_closing", False)):
            try:
                local_data = validation_queue.get(timeout=1.0)
            except queue.Empty:
                return
            try:
                self._run_record_binding_validation(local_data)
            except Exception as exc:
                record_id = str((local_data or {}).get("record_id") or "").strip()
                if record_id:
                    self._record_binding_validation_pending_ids.discard(record_id)
                log_warning(f"Record ID 校验队列执行失败: record_id={record_id}, error={exc}")
            finally:
                try:
                    validation_queue.task_done()
                except Exception:
                    pass
                interval = float(
                    getattr(self, "_record_binding_validation_interval_s", 0.35)
                    or 0.35
                )
                if interval > 0:
                    time.sleep(min(2.0, interval))

    def _backend_query_record_by_id(self, record_id: str, notice_type: str):
        controller = getattr(self, "lan_template_portal_controller", None)
        if controller is None or not hasattr(controller, "submit_qt_command"):
            return False, "本机后端未连接，无法查询多维记录。"
        try:
            result = self._submit_qt_command(
                "query_record_by_id",
                {
                    "record_id": str(record_id or "").strip(),
                    "notice_type": str(notice_type or "").strip(),
                },
                timeout=15.0,
            )
        except Exception as exc:
            return False, str(exc)
        if bool((result or {}).get("ok")):
            record = result.get("record") if isinstance(result, dict) else {}
            return True, record if isinstance(record, dict) else {}
        return False, str((result or {}).get("message") or "查询记录失败。")

    def _submit_qt_command(self, command: str, payload: dict | None = None, *, timeout: float = 120.0):
        controller = getattr(self, "lan_template_portal_controller", None)
        if controller is None or not hasattr(controller, "submit_qt_command"):
            raise RuntimeError("本机后端未连接。")
        submit = controller.submit_qt_command
        try:
            return submit(command, payload or {}, timeout=timeout)
        except TypeError as exc:
            message = str(exc)
            if "timeout" not in message and "unexpected keyword" not in message:
                raise
            return submit(command, payload or {})

    def _run_record_binding_validation(self, local_data: dict | None):
        local_data = dict(local_data or {})
        record_id = str(local_data.get("record_id") or "").strip()
        notice_type = str(local_data.get("notice_type") or "").strip()
        if not record_id or not notice_type:
            if record_id:
                self._record_binding_validation_pending_ids.discard(record_id)
            return
        if bool(getattr(self, "_closing", False)):
            self._record_binding_validation_pending_ids.discard(record_id)
            return
        success, result = self._backend_query_record_by_id(record_id, notice_type)
        conflict = False
        conflict_error = ""
        missing_remote_record = False
        state_to_apply = ""
        error_to_apply = ""
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
                labels = [mismatch_label_map.get(key, key) for key in mismatch_keys]
                conflict_error = (
                    "Record ID 冲突："
                    + "、".join(labels)
                    + " 与多维记录不一致"
                )
            state_to_apply = "conflicted" if conflict else "bound"
            error_to_apply = conflict_error if conflict else ""
        else:
            conflict_error = str(result or "")
            missing_remote_record = self._is_missing_remote_record_error(conflict_error)
            if missing_remote_record:
                conflict_error = "Record ID 已失效：多维记录不存在，可能已被删除"
                state_to_apply = "conflicted"
                error_to_apply = conflict_error

        if state_to_apply and getattr(self, "cache_store", None):
            patch = {"record_binding_state": state_to_apply}
            if state_to_apply == "conflicted":
                patch["record_binding_error"] = error_to_apply or "Record ID 冲突"
            else:
                patch["record_binding_error"] = None
            try:
                self.cache_store.patch_record_fields(record_id=record_id, patch=patch)
            except Exception:
                pass

        def apply_result():
            self._record_binding_validation_pending_ids.discard(record_id)
            if not success:
                if conflict_error:
                    log_warning(
                        f"Record ID 校验失败: record_id={record_id}, error={conflict_error}"
                    )
                if missing_remote_record:
                    self._record_binding_validated_ids.add(record_id)
                    self._set_record_binding_state(
                        record_id,
                        state_to_apply,
                        error=error_to_apply,
                        persist_cache=False,
                    )
                return
            self._record_binding_validated_ids.add(record_id)
            self._set_record_binding_state(
                record_id,
                state_to_apply,
                error=error_to_apply,
                persist_cache=False,
            )

        self._enqueue_ui_mutation("record_binding_validation", apply_result)

    @staticmethod
    def _startup_remote_sync_batch_size() -> int:
        try:
            value = int(os.environ.get("CLIPFLOW_STARTUP_REMOTE_SYNC_BATCH_SIZE", "4") or 4)
        except Exception:
            value = 4
        return max(1, min(value, 20))

    @staticmethod
    def _startup_remote_sync_interval_ms() -> int:
        try:
            value = int(
                os.environ.get("CLIPFLOW_STARTUP_REMOTE_SYNC_INTERVAL_MS", "1500")
                or 1500
            )
        except Exception:
            value = 1500
        return max(200, min(value, 10000))

    def _active_data_snapshot_for_startup_sync(self) -> list[dict]:
        return self._active_notice_store().data_snapshot()

    def _validate_record_bindings_on_startup(self):
        if os.environ.get("CLIPFLOW_QT_REMOTE_VALIDATION", "0") != "1":
            return
        pending = [
            data
            for data in self._active_data_snapshot_for_startup_sync()
            if isinstance(data, dict)
        ]
        if not pending:
            return
        self._startup_record_binding_validation_queue = pending
        if getattr(self, "_startup_record_binding_validation_pending", False):
            return
        self._startup_record_binding_validation_pending = True
        QTimer.singleShot(0, self._drain_startup_record_binding_validation_batch)

    def _drain_startup_record_binding_validation_batch(self):
        self._startup_record_binding_validation_pending = False
        if getattr(self, "_closing", False):
            self._startup_record_binding_validation_queue = []
            return
        if bool(getattr(self, "_active_cache_restore_in_progress", False)):
            self._startup_record_binding_validation_pending = True
            QTimer.singleShot(500, self._drain_startup_record_binding_validation_batch)
            return
        pending = list(
            getattr(self, "_startup_record_binding_validation_queue", []) or []
        )
        if not pending:
            return
        batch_size = self._startup_remote_sync_batch_size()
        batch = pending[:batch_size]
        self._startup_record_binding_validation_queue = pending[batch_size:]
        for data in batch:
            self._schedule_record_binding_validation(data)
        if self._startup_record_binding_validation_queue:
            self._startup_record_binding_validation_pending = True
            QTimer.singleShot(
                self._startup_remote_sync_interval_ms(),
                self._drain_startup_record_binding_validation_batch,
            )

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
            success, result = self._backend_query_record_by_id(new_record_id, notice_type)
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
                self.request_active_cache_save()
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
        if list_widget is None or item is None or not self._is_valid_list_item(item):
            return
        data = self._active_item_data(item)
        if isinstance(data, dict):
            self._remove_active_notice_model_record(data)
        if isinstance(item, ActiveNoticeModelItem):
            self._remove_active_item_from_source(list_widget, item)
        return

    def _build_route_conflict_error(self, match_key: str) -> str:
        preview = str(match_key or "").split("|", 1)[-1]
        if len(preview) > 40:
            preview = preview[:40] + "..."
        return f"条目路由冲突：匹配键重复（{preview}），已阻止自动匹配和远端写入。"

    @staticmethod
    def _route_match_key_from_data(data_dict: dict | None) -> str:
        if not isinstance(data_dict, dict):
            return ""
        return str(data_dict.get("match_key") or "").strip()

    def _reconcile_active_route_duplicates(self, match_keys: set[str] | None = None):
        limited_keys = {
            str(key or "").strip()
            for key in (match_keys or set())
            if str(key or "").strip()
        }
        if match_keys is not None and not limited_keys:
            return False
        groups = self._active_notice_store().groups_by_match_key(
            limited_keys if match_keys is not None else None
        )

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
            self.request_active_cache_save()
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
        return field_name or "今日是否进行"

    def _supports_today_in_progress_toggle(self, data_dict: dict | None) -> bool:
        if not isinstance(data_dict, dict):
            return False
        notice_type = str(data_dict.get("notice_type") or "").strip()
        record_id = self._today_in_progress_target_record_id(data_dict)
        return (
            notice_type in ("设备变更", "变更通告")
            and bool(record_id)
            and not self._is_placeholder_record(data_dict)
        )

    @staticmethod
    def _today_in_progress_target_record_id(data_dict: dict | None) -> str:
        if not isinstance(data_dict, dict):
            return ""
        return str(
            data_dict.get("target_record_id") or data_dict.get("record_id") or ""
        ).strip()

    def _find_today_in_progress_active_item(
        self,
        data_dict: dict | None,
        fallback_record_id: str = "",
    ):
        if not isinstance(data_dict, dict):
            data_dict = {}
        active_item_id = str(data_dict.get("active_item_id") or "").strip()
        if active_item_id:
            list_widget, item = self._find_active_item_by_active_item_id(active_item_id)
            if item and self._is_valid_list_item(item):
                return list_widget, item
        seen: set[str] = set()
        for value in (
            data_dict.get("record_id"),
            data_dict.get("target_record_id"),
            fallback_record_id,
        ):
            record_id = str(value or "").strip()
            if not record_id or record_id in seen:
                continue
            seen.add(record_id)
            list_widget, item = self._find_active_item_by_record_id(record_id)
            if item and self._is_valid_list_item(item):
                return list_widget, item
        return None, None

    def _extract_today_in_progress_state_from_record_fields(
        self, notice_type: str, fields: dict | None
    ) -> str:
        if not isinstance(fields, dict):
            return "unknown"
        field_name = self._get_today_in_progress_field_name(notice_type)
        raw_value = fields.get(field_name)
        if raw_value in (None, "", []):
            for alias in ("今日是否进行", "今天是否进行"):
                if alias == field_name:
                    continue
                alias_value = fields.get(alias)
                if alias_value not in (None, "", []):
                    raw_value = alias_value
                    break
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

    def _apply_today_in_progress_state_to_active_item(
        self,
        data_dict: dict | None,
        state: str,
        *,
        fallback_record_id: str = "",
    ):
        normalized_state = self._normalize_today_in_progress_state(state)
        list_widget, item = self._find_today_in_progress_active_item(
            data_dict,
            fallback_record_id=fallback_record_id,
        )
        if item and list_widget:
            current = item.data(Qt.ItemDataRole.UserRole) or {}
            if isinstance(current, dict):
                updated = dict(current)
                updated["today_in_progress_state"] = normalized_state
                self._commit_active_record(
                    updated,
                    refresh_detail=True,
                    rebuild_widget=True,
                    list_widget=list_widget,
                    item=item,
                )
        seen: set[str] = set()
        if isinstance(data_dict, dict):
            values = (
                data_dict.get("record_id"),
                data_dict.get("target_record_id"),
                fallback_record_id,
            )
        else:
            values = (fallback_record_id,)
        for value in values:
            record_id = str(value or "").strip()
            if record_id and record_id not in seen:
                seen.add(record_id)
                self._persist_today_in_progress_state(record_id, normalized_state)

    def _mark_today_in_progress_syncing(
        self,
        data_dict: dict | None,
        syncing: bool,
        *,
        fallback_record_id: str = "",
    ):
        list_widget, item = self._find_today_in_progress_active_item(
            data_dict,
            fallback_record_id=fallback_record_id,
        )
        if not item or not list_widget:
            return
        current = item.data(Qt.ItemDataRole.UserRole) or {}
        if not isinstance(current, dict):
            return
        updated = dict(current)
        if syncing:
            updated["_today_in_progress_syncing"] = True
            updated.pop("_today_in_progress_error", None)
        else:
            updated.pop("_today_in_progress_syncing", None)
        try:
            item.setData(Qt.ItemDataRole.UserRole, updated)
        except Exception:
            return
        self._upsert_active_notice_model_item(list_widget, item, updated)
        widget = self._safe_item_widget(list_widget, item)
        if widget:
            try:
                if hasattr(widget, "data"):
                    widget.data = dict(updated)
                if hasattr(widget, "set_today_progress_state"):
                    widget.set_today_progress_state(
                        "syncing" if syncing else updated.get("today_in_progress_state"),
                        enabled=not syncing,
                    )
            except Exception:
                pass

    def _schedule_today_in_progress_sync(self, data_dict: dict | None):
        if os.environ.get("CLIPFLOW_QT_REMOTE_VALIDATION", "0") != "1":
            return
        if bool(getattr(self, "_active_cache_restore_in_progress", False)):
            return
        if not self._supports_today_in_progress_toggle(data_dict):
            return
        if self._is_record_binding_conflicted(data_dict):
            return
        record_id = self._today_in_progress_target_record_id(data_dict)
        notice_type = str(data_dict.get("notice_type") or "").strip()
        if (
            not record_id
            or record_id in self._today_in_progress_pending_record_ids
            or record_id in self._today_in_progress_synced_record_ids
        ):
            return
        self._today_in_progress_pending_record_ids.add(record_id)
        try:
            self._today_in_progress_sync_queue.put_nowait(dict(data_dict))
        except Exception:
            self._today_in_progress_pending_record_ids.discard(record_id)
            return
        self._ensure_today_in_progress_sync_worker()

    def _ensure_today_in_progress_sync_worker(self):
        worker = getattr(self, "_today_in_progress_sync_worker", None)
        if worker and worker.is_alive():
            return
        worker = threading.Thread(
            target=self._today_in_progress_sync_loop,
            name="TodayInProgressSyncWorker",
            daemon=True,
        )
        self._today_in_progress_sync_worker = worker
        worker.start()

    def _today_in_progress_sync_loop(self):
        sync_queue = getattr(self, "_today_in_progress_sync_queue", None)
        if sync_queue is None:
            return
        while not bool(getattr(self, "_closing", False)):
            try:
                data_dict = sync_queue.get(timeout=1.0)
            except queue.Empty:
                return
            try:
                self._run_today_in_progress_sync(data_dict)
            except Exception as exc:
                record_id = self._today_in_progress_target_record_id(data_dict)
                if record_id:
                    self._today_in_progress_pending_record_ids.discard(record_id)
                log_warning(
                    f"今日是否进行状态同步队列执行失败: record_id={record_id}, error={exc}"
                )
            finally:
                try:
                    sync_queue.task_done()
                except Exception:
                    pass
                interval = float(
                    getattr(self, "_today_in_progress_sync_interval_s", 0.35) or 0.35
                )
                if interval > 0:
                    time.sleep(min(2.0, interval))

    def _run_today_in_progress_sync(self, data_dict: dict | None):
        data_dict = dict(data_dict or {})
        record_id = self._today_in_progress_target_record_id(data_dict)
        notice_type = str(data_dict.get("notice_type") or "").strip()
        if not record_id or not notice_type or bool(getattr(self, "_closing", False)):
            if record_id:
                self._today_in_progress_pending_record_ids.discard(record_id)
            return
        success, result = self._backend_query_record_by_id(record_id, notice_type)
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
                        f"今日是否进行状态同步失败: record_id={record_id}, error={error_text}"
                    )
                return
            self._apply_today_in_progress_state_to_active_item(
                data_dict,
                state,
                fallback_record_id=record_id,
            )

        self._enqueue_ui_mutation("today_in_progress_sync", apply_result)

    def _schedule_today_in_progress_sync_on_startup(self):
        if os.environ.get("CLIPFLOW_QT_REMOTE_VALIDATION", "0") != "1":
            return
        pending = [
            data
            for data in self._active_data_snapshot_for_startup_sync()
            if isinstance(data, dict)
        ]
        if not pending:
            return
        self._startup_today_in_progress_sync_queue = pending
        if getattr(self, "_startup_today_in_progress_sync_pending", False):
            return
        self._startup_today_in_progress_sync_pending = True
        QTimer.singleShot(0, self._drain_startup_today_in_progress_sync_batch)

    def _drain_startup_today_in_progress_sync_batch(self):
        self._startup_today_in_progress_sync_pending = False
        if getattr(self, "_closing", False):
            self._startup_today_in_progress_sync_queue = []
            return
        if bool(getattr(self, "_active_cache_restore_in_progress", False)):
            self._startup_today_in_progress_sync_pending = True
            QTimer.singleShot(500, self._drain_startup_today_in_progress_sync_batch)
            return
        pending = list(getattr(self, "_startup_today_in_progress_sync_queue", []) or [])
        if not pending:
            return
        batch_size = self._startup_remote_sync_batch_size()
        batch = pending[:batch_size]
        self._startup_today_in_progress_sync_queue = pending[batch_size:]
        for data in batch:
            self._schedule_today_in_progress_sync(data)
        if self._startup_today_in_progress_sync_queue:
            self._startup_today_in_progress_sync_pending = True
            QTimer.singleShot(
                self._startup_remote_sync_interval_ms(),
                self._drain_startup_today_in_progress_sync_batch,
            )

    def _handle_today_in_progress_toggle(self, data_dict: dict, target_state: str):
        if not self._supports_today_in_progress_toggle(data_dict):
            return
        if self._is_routing_conflicted(data_dict):
            self.show_message(self._routing_error_text(data_dict))
            return
        if self._is_record_binding_conflicted(data_dict):
            self.show_message(self._record_binding_error_text(data_dict))
            return
        record_id = self._today_in_progress_target_record_id(data_dict)
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

        list_widget, item = self._find_today_in_progress_active_item(
            data_dict,
            fallback_record_id=record_id,
        )
        widget = self._safe_item_widget(list_widget, item)
        if widget and hasattr(widget, "set_today_progress_state"):
            widget.set_today_progress_state("syncing", enabled=False)

        self._today_in_progress_pending_record_ids.add(record_id)
        self._mark_today_in_progress_syncing(
            data_dict,
            True,
            fallback_record_id=record_id,
        )
        field_name = self._get_today_in_progress_field_name(notice_type)
        field_value = self._today_in_progress_option_for_state(desired_state)
        controller = getattr(self, "lan_template_portal_controller", None)
        if controller is None or not hasattr(controller, "submit_qt_command"):
            self._today_in_progress_pending_record_ids.discard(record_id)
            self._mark_today_in_progress_syncing(
                data_dict,
                False,
                fallback_record_id=record_id,
            )
            self._apply_today_in_progress_state_to_active_item(
                data_dict,
                current_state,
                fallback_record_id=record_id,
            )
            if widget and hasattr(widget, "set_today_progress_state"):
                widget.set_today_progress_state(current_state, enabled=True)
            self.show_message("本机后端未连接，Qt 不再直接执行多维字段更新。")
            return

        def worker():
            try:
                result_payload = self._submit_qt_command(
                    "set_today_in_progress",
                    {
                        "record_id": record_id,
                        "target_record_id": record_id,
                        "active_item_id": str(
                            data_dict.get("active_item_id") or ""
                        ).strip(),
                        "notice_type": notice_type,
                        "field_name": field_name,
                        "field_value": field_value,
                    },
                    timeout=30.0,
                )
                success = bool((result_payload or {}).get("ok"))
                result = str((result_payload or {}).get("message") or "")
            except Exception as exc:
                success = False
                result = str(exc)

            def apply_result():
                self._today_in_progress_pending_record_ids.discard(record_id)
                widget_now = self._safe_item_widget(
                    *self._find_today_in_progress_active_item(
                        data_dict,
                        fallback_record_id=record_id,
                    )
                )
                if success:
                    self._today_in_progress_synced_record_ids.add(record_id)
                    self._mark_today_in_progress_syncing(
                        data_dict,
                        False,
                        fallback_record_id=record_id,
                    )
                    self._apply_today_in_progress_state_to_active_item(
                        data_dict,
                        desired_state,
                        fallback_record_id=record_id,
                    )
                    self._mark_today_in_progress_error(
                        data_dict,
                        "",
                        fallback_record_id=record_id,
                    )
                    return
                self._mark_today_in_progress_syncing(
                    data_dict,
                    False,
                    fallback_record_id=record_id,
                )
                self._apply_today_in_progress_state_to_active_item(
                    data_dict,
                    current_state,
                    fallback_record_id=record_id,
                )
                self._mark_today_in_progress_error(
                    data_dict,
                    f"多维同步失败，点击重试：{result}",
                    fallback_record_id=record_id,
                )
                if widget_now and hasattr(widget_now, "set_today_progress_state"):
                    widget_now.set_today_progress_state(current_state, enabled=True)
                self.show_message(f"多维同步失败，点击重试\n{result}")

            self._enqueue_ui_mutation("today_in_progress_toggle", apply_result)

        threading.Thread(target=worker, daemon=True).start()

    def _mark_today_in_progress_error(
        self,
        data_dict: dict | None,
        message: str,
        *,
        fallback_record_id: str = "",
    ):
        list_widget, item = self._find_today_in_progress_active_item(
            data_dict,
            fallback_record_id=fallback_record_id,
        )
        if not item or not list_widget:
            return
        current = item.data(Qt.ItemDataRole.UserRole) or {}
        if not isinstance(current, dict):
            return
        updated = dict(current)
        text = str(message or "").strip()
        if text:
            updated["_today_in_progress_error"] = text
        else:
            updated.pop("_today_in_progress_error", None)
        try:
            item.setData(Qt.ItemDataRole.UserRole, updated)
        except Exception:
            return
        self._upsert_active_notice_model_item(list_widget, item, updated)

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

    def _load_active_cache_record_maps(self) -> tuple[dict[str, dict], dict[str, dict]]:
        store = getattr(self, "cache_store", None)
        if not store or not hasattr(store, "load_payload"):
            return {}, {}
        try:
            payload = store.load_payload()
        except Exception:
            return {}, {}
        if not isinstance(payload, dict):
            return {}, {}
        by_record_id: dict[str, dict] = {}
        by_active_item_id: dict[str, dict] = {}
        for section in ("event", "other"):
            entries = payload.get(section, [])
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                data = entry.get("data")
                if not isinstance(data, dict):
                    continue
                normalized = self._ensure_active_item_identity(
                    normalize_active_item_data(data)
                )
                record_id = str(normalized.get("record_id") or "").strip()
                active_item_id = str(normalized.get("active_item_id") or "").strip()
                if record_id:
                    by_record_id[record_id] = normalized
                if active_item_id:
                    by_active_item_id[active_item_id] = normalized
        return by_record_id, by_active_item_id

    def _cache_data_for_active_item(
        self,
        item,
        by_record_id: dict[str, dict] | None,
        by_active_item_id: dict[str, dict] | None,
    ) -> dict | None:
        if not item or not self._is_valid_list_item(item):
            return None
        try:
            data = item.data(Qt.ItemDataRole.UserRole)
        except Exception:
            data = None
        if not isinstance(data, dict):
            return None
        record_id = str(data.get("record_id") or "").strip()
        active_item_id = str(data.get("active_item_id") or "").strip()
        if record_id and by_record_id and record_id in by_record_id:
            return dict(by_record_id[record_id])
        if active_item_id and by_active_item_id and active_item_id in by_active_item_id:
            return dict(by_active_item_id[active_item_id])
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

        if list_widget is None or item is None:
            list_widget, item = self._find_active_item_by_record_id(record_id)
        if item and self._is_valid_list_item(item):
            self._set_active_item_data(list_widget, item, committed)
            self._upsert_active_notice_model_item(list_widget, item, committed)
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
        self._schedule_active_route_reconcile(committed, previous_data=existing_data)
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
        self._log_detail_preview_update(display_data, rid, reason="detail_dialog")

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

    def _schedule_active_route_reconcile(self, data_dict=None, previous_data=None):
        if getattr(self, "_closing", False):
            return
        if bool(getattr(self, "_active_cache_restore_in_progress", False)):
            return
        if bool(getattr(self, "_route_reconcile_in_progress", False)):
            return
        if data_dict is None and previous_data is None:
            self._route_reconcile_full_requested = True
        else:
            dirty_keys = getattr(self, "_route_reconcile_dirty_match_keys", None)
            if not isinstance(dirty_keys, set):
                dirty_keys = set()
            for candidate in (data_dict, previous_data):
                match_key = self._route_match_key_from_data(candidate)
                if match_key:
                    dirty_keys.add(match_key)
            self._route_reconcile_dirty_match_keys = dirty_keys
        if getattr(self, "_route_reconcile_single_shot_pending", False):
            return
        self._route_reconcile_single_shot_pending = True

        def _run():
            self._route_reconcile_single_shot_pending = False
            if getattr(self, "_closing", False):
                return
            full_requested = bool(
                getattr(self, "_route_reconcile_full_requested", False)
            )
            dirty_keys = set(
                getattr(self, "_route_reconcile_dirty_match_keys", set()) or set()
            )
            self._route_reconcile_full_requested = False
            self._route_reconcile_dirty_match_keys = set()
            try:
                self._route_reconcile_in_progress = True
                if full_requested or not dirty_keys:
                    self._reconcile_active_route_duplicates()
                else:
                    self._reconcile_active_route_duplicates(dirty_keys)
            except Exception as exc:
                log_warning(f"活动条目路由校验失败: error={exc}")
            finally:
                self._route_reconcile_in_progress = False

        QTimer.singleShot(150, _run)

    def _apply_cache_to_item(self, list_widget, item, cache_data: dict):
        if list_widget is None or item is None or not cache_data:
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
        if self._active_model_view_visible():
            return 0
        if self._active_list_virtualization_enabled():
            self._schedule_active_list_virtualization_refresh(None, 0)
            return 0
        repaired = 0
        by_record_id, by_active_item_id = self._load_active_cache_record_maps()
        for list_widget, item, _data in self._active_notice_store().entries():
            try:
                if not self._is_valid_list_item(item):
                    continue
                widget = self._safe_item_widget(list_widget, item)
                if widget:
                    continue
                data = item.data(Qt.ItemDataRole.UserRole)
                if not isinstance(data, dict):
                    continue
                cache_data = self._cache_data_for_active_item(
                    item, by_record_id, by_active_item_id
                )
                if not isinstance(cache_data, dict):
                    continue
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
        try:
            self.detail_dialog.update_content(
                display_data,
                rid,
                editable=self._is_active_view(),
                active_item_id=display_data.get("active_item_id", active_item_id),
            )
        except Exception:
            return
        self._log_detail_preview_update(display_data, rid, reason="cache_refresh")

    def _refresh_ui_from_cache(self):
        if self._closing:
            return
        refresh_error = False
        by_record_id, by_active_item_id = self._load_active_cache_record_maps()
        for list_widget, item, data in self._active_notice_store().entries():
            try:
                if not self._is_valid_list_item(item):
                    continue
                if not isinstance(data, dict):
                    continue
                cache_data = self._cache_data_for_active_item(
                    item, by_record_id, by_active_item_id
                )
                if not isinstance(cache_data, dict):
                    continue
                item.setData(Qt.ItemDataRole.UserRole, cache_data)
                self._upsert_active_notice_model_item(list_widget, item, cache_data)
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
        self._sync_all_active_notice_models()
        self._schedule_active_list_virtualization_refresh(None, 0)

    def _init_list_widget(self, list_widget):
        list_widget.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        list_widget.customContextMenuRequested.connect(
            lambda pos, lw=list_widget: self.show_context_menu(pos, lw)
        )
        list_widget.itemClicked.connect(self.on_item_clicked)
        if self._is_active_list_widget(list_widget):
            try:
                list_widget.verticalScrollBar().valueChanged.connect(
                    lambda _value, lw=list_widget: self._schedule_active_list_virtualization_refresh(
                        lw, self._active_list_scroll_refresh_delay_ms()
                    )
                )
            except Exception:
                pass

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

    def _iter_active_items(self):
        if self._active_model_view_visible():
            for list_widget in (self.list_active_event, self.list_active_other):
                model = self._active_notice_model_for_list(list_widget)
                if model is None:
                    continue
                for record in model.records():
                    item = self._active_model_item(list_widget, record)
                    if item is None or not item.is_valid():
                        continue
                    yield list_widget, item
            return
        for list_widget in (self.list_active_event, self.list_active_other):
            for i in range(list_widget.count()):
                item = list_widget.item(i)
                if not self._is_valid_list_item(item):
                    continue
                yield list_widget, item

    def _iter_history_items(self):
        return iter(())

    def _active_notice_store(self) -> ActiveNoticeStore:
        store = getattr(self, "_active_notice_store_obj", None)
        if not isinstance(store, ActiveNoticeStore):
            store = ActiveNoticeStore(
                self._iter_active_items,
                self._is_valid_list_item,
                compact_text_normalizer=lambda value: str(value or "").translate(
                    WHITESPACE_TRANSLATOR
                ),
                match_title_normalizer=self._normalize_match_text,
            )
            self._active_notice_store_obj = store
        return store

    def _build_active_item_index(self):
        self._active_notice_store().rebuild()

    def _active_model_item_for_row(self, list_widget, row: int):
        model = self._active_notice_model_for_list(list_widget)
        if model is None:
            return None
        try:
            row = int(row)
        except Exception:
            return None
        if row < 0:
            return None
        record = model.record_at(row)
        if not isinstance(record, dict):
            return None
        return self._active_model_item(list_widget, record)

    def _find_active_item_by_record_id(self, record_id):
        if self._active_model_view_visible():
            record_id = str(record_id or "").strip()
            if not record_id:
                return None, None
            for list_widget in (self.list_active_event, self.list_active_other):
                model = self._active_notice_model_for_list(list_widget)
                if model is None:
                    continue
                item = self._active_model_item_for_row(
                    list_widget,
                    model.row_for_record_id(record_id),
                )
                if item and self._is_valid_list_item(item):
                    return list_widget, item
        return self._active_notice_store().find_by_record_id(record_id)

    def _find_active_item_by_active_item_id(self, active_item_id):
        if self._active_model_view_visible():
            active_item_id = str(active_item_id or "").strip()
            if not active_item_id:
                return None, None
            for list_widget in (self.list_active_event, self.list_active_other):
                model = self._active_notice_model_for_list(list_widget)
                if model is None:
                    continue
                item = self._active_model_item_for_row(
                    list_widget,
                    model.row_for_active_item_id(active_item_id),
                )
                if item and self._is_valid_list_item(item):
                    return list_widget, item
        return self._active_notice_store().find_by_active_item_id(active_item_id)

    def _upload_completion_record_id_candidates(self, record_id) -> list[str]:
        seed = str(record_id or "").strip()
        if not seed:
            return []
        candidates: list[str] = []

        def add(value):
            text = str(value or "").strip()
            if text and text not in candidates:
                candidates.append(text)

        add(seed)
        for mapping_name in ("_payload_alias",):
            mapping = getattr(self, mapping_name, None)
            if not isinstance(mapping, dict):
                continue
            add(mapping.get(seed))
            for key, value in list(mapping.items()):
                if key == seed or value == seed:
                    add(key)
                    add(value)
        return candidates

    def _find_active_item_by_upload_completion_id(self, record_id):
        candidate_ids = self._upload_completion_record_id_candidates(record_id)
        for candidate_id in candidate_ids:
            list_widget, item = self._find_active_item_by_record_id(candidate_id)
            if item and self._is_valid_list_item(item):
                return list_widget, item, candidate_id
        candidate_set = set(candidate_ids)
        if not candidate_set:
            return None, None, ""
        for list_widget, item, data in self._active_notice_store().entries():
            if not self._is_valid_list_item(item) or not isinstance(data, dict):
                continue
            values = {
                str(data.get("record_id") or "").strip(),
                str(data.get("target_record_id") or "").strip(),
                str(data.get("feishu_record_id") or "").strip(),
                str(data.get("raw_record_id") or "").strip(),
            }
            matched = values & candidate_set
            if matched:
                return list_widget, item, next(iter(matched))
        return None, None, ""

    def _update_active_item_data(self, record_id, data_dict, *, persist_cache=True):
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
            if not persist_cache:
                if self._should_defer_ui_refresh():
                    self._mark_cache_refresh_needed()
                elif committed:
                    self._apply_cache_to_item(list_widget, item, committed)
                return
            cache_updated = bool(
                committed
                and hasattr(self, "_upsert_active_cache_record")
                and self._upsert_active_cache_record(committed)
            )
            if not cache_updated:
                self.request_active_cache_save()
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
        for char in re.findall(r"([A-EH])\s*[栋楼]", normalized_text):
            building = normalize_building_name(f"{char}楼")
            if building not in matched:
                matched.append(building)
        if "110" in normalized_text:
            for token in ("110机房", "110"):
                building = normalize_building_name(token)
                if token in normalized_text and building and building not in matched:
                    matched.append(building)
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
        infer_buildings = getattr(self, "_infer_buildings_from_notice_text", None)
        normalize_today = getattr(self, "_normalize_today_in_progress_state", None)
        normalize_binding = getattr(self, "_normalize_record_binding_state", None)
        normalize_match = getattr(self, "_normalize_match_text", None)
        normalize_routing = getattr(self, "_normalize_routing_state", None)
        ensure_identity = getattr(self, "_ensure_active_item_identity", None)

        def _fallback_buildings() -> list:
            if callable(infer_buildings):
                return self._normalize_buildings_value(
                    infer_buildings(data_dict.get("text", ""))
                )
            return []

        def _normalize_optional(callable_obj, value, default=""):
            if callable(callable_obj):
                return callable_obj(value)
            return default if value in (None, "") else str(value).strip()

        record_id = self._get_cache_identity(data_dict)
        if not record_id:
            existing_buildings = self._normalize_buildings_value(
                data_dict.get("buildings")
            )
            data_dict["buildings"] = existing_buildings or _fallback_buildings()
            data_dict.pop("specialty", None)
            return data_dict
        cache_fields = self.cache_store.get_record_fields(
            record_id=record_id,
            fields=[
                "buildings",
                "specialty",
                "maintenance_cycle",
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
        existing_buildings = self._normalize_buildings_value(data_dict.get("buildings"))
        if "buildings" in cache_fields:
            data_dict["buildings"] = self._normalize_buildings_value(
                cache_fields.get("buildings")
            )
        else:
            data_dict["buildings"] = existing_buildings or _fallback_buildings()
        if "specialty" in cache_fields:
            specialty = str(cache_fields.get("specialty") or "").strip()
            if specialty:
                data_dict["specialty"] = specialty
            else:
                data_dict.pop("specialty", None)
        else:
            data_dict.pop("specialty", None)
        if "maintenance_cycle" in cache_fields:
            maintenance_cycle = str(cache_fields.get("maintenance_cycle") or "").strip()
            if maintenance_cycle:
                data_dict["maintenance_cycle"] = maintenance_cycle
            else:
                data_dict.pop("maintenance_cycle", None)
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
            data_dict["today_in_progress_state"] = _normalize_optional(
                normalize_today,
                cache_fields.get("today_in_progress_state")
            )
        else:
            data_dict.pop("today_in_progress_state", None)
        binding_state = _normalize_optional(
            normalize_binding,
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
        match_title = _normalize_optional(normalize_match, cache_fields.get("match_title"))
        if match_title:
            data_dict["match_title"] = match_title
        else:
            data_dict.pop("match_title", None)
        match_key = str(cache_fields.get("match_key") or "").strip()
        if match_key:
            data_dict["match_key"] = match_key
        else:
            data_dict.pop("match_key", None)
        routing_state = _normalize_optional(
            normalize_routing,
            cache_fields.get("routing_state"),
            default="",
        )
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
        if callable(ensure_identity):
            return ensure_identity(data_dict)
        return data_dict

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

        def _notice_type_matches(data: dict | None) -> bool:
            if not isinstance(data, dict):
                return False
            if (
                resolved_notice_type
                and data.get("notice_type")
                and data.get("notice_type") != resolved_notice_type
            ):
                return False
            return True

        store = self._active_notice_store()
        if content_clean:
            for list_widget, item, data in store.candidates_by_exact_text(content_clean):
                if _notice_type_matches(data):
                    return list_widget, item
        key_matches = []
        title_matches = []
        if match_key:
            for list_widget, item, data in store.candidates_by_match_key(match_key):
                if _notice_type_matches(data):
                    key_matches.append((list_widget, item))
        if match_title:
            for list_widget, item, data in store.candidates_by_match_title(match_title):
                if _notice_type_matches(data):
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
        return self._active_notice_store().find_by_compact_text(content)

    def _is_valid_list_item(self, item) -> bool:
        if item is None:
            return False
        if isinstance(item, ActiveNoticeModelItem):
            return item.is_valid()
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
        if self._active_model_view_visible():
            model = self._active_notice_model_for_list(list_widget)
            if model is None:
                return None, None
            row = 0 if insert_top else model.rowCount()
            model.upsert_record(dict(data_dict), row=row)
            item = self._active_model_item(list_widget, data_dict)
            if item is None:
                return None, None
            self._schedule_today_in_progress_sync(data_dict)
            self._schedule_record_binding_validation(data_dict)
            self._schedule_active_route_reconcile(data_dict)
            if not skip_cache:
                if not (
                    hasattr(self, "_upsert_active_cache_record")
                    and self._upsert_active_cache_record(data_dict)
                ):
                    self.request_active_cache_save()
            return item, None
        log_warning("活动列表 model/delegate 未初始化，拒绝回退到旧 QWidget 列表。")
        self._schedule_pending_cache_refresh()
        return None, None

    def sync_record_id_to_widget(self, old_record_id, new_record_id):
        if not old_record_id or not new_record_id:
            return
        try:
            changed = self._replace_record_id_everywhere(old_record_id, new_record_id)
            if changed:
                self.request_active_cache_save()
        except Exception:
            return

    def find_widget_by_record_id(self, rid):
        list_widget, item = self._find_active_item_by_record_id(rid)
        return self._safe_item_widget(list_widget, item)

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
            self.request_active_cache_save()
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
            candidate_ids = self._upload_completion_record_id_candidates(record_id)
            for candidate_id in candidate_ids:
                if candidate_id in self.pending_action_record_ids:
                    self.pending_action_record_ids.remove(candidate_id)
                self.pending_action_types.pop(candidate_id, None)
            if success:
                for candidate_id in candidate_ids:
                    self.pending_upload_rollback_by_record_id.pop(candidate_id, None)
            list_widget, item, matched_record_id = self._find_active_item_by_upload_completion_id(
                record_id
            )
            if item and not self._is_valid_list_item(item):
                item = None
                list_widget = None
            if list_widget is not None and item is not None:
                data = item.data(Qt.ItemDataRole.UserRole) or {}
                if not success:
                    rollback = None
                    for candidate_id in candidate_ids:
                        rollback = self.pending_upload_rollback_by_record_id.pop(
                            candidate_id, None
                        )
                        if rollback:
                            break
                    if rollback and rollback.get("old_data"):
                        data = rollback.get("old_data") or data
                        data["_pending_upload_hash"] = None
                        data["_has_unuploaded_changes"] = True
                        data["_upload_in_progress"] = False
                        data.pop("_upload_started_monotonic", None)
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
                    data["_last_upload_error"] = f"{name or '上传'}失败，可重试。"
                else:
                    data["_has_unuploaded_changes"] = False
                    data.pop("_last_upload_error", None)
                has_unuploaded_changes = data.get("_has_unuploaded_changes")

                data["_pending_upload_hash"] = None
                data["_upload_in_progress"] = False
                data.pop("_upload_started_monotonic", None)
                item.setData(Qt.ItemDataRole.UserRole, data)
                if hasattr(self, "_upsert_active_notice_model_item"):
                    try:
                        self._upsert_active_notice_model_item(list_widget, item, data)
                    except Exception:
                        pass
                self._rebuild_active_item_widget(
                    list_widget,
                    item,
                    data,
                    force_status=None,
                    upload_in_progress=False,
                    pending_upload_hash=None,
                    has_unuploaded_changes=has_unuploaded_changes,
                )

            screenshot_candidates = set(candidate_ids)
            if matched_record_id:
                screenshot_candidates.add(matched_record_id)
            if self.current_screenshot_record_id in screenshot_candidates:
                self.current_screenshot_record_id = None
                self.current_screenshot_action_type = None
            self.request_active_cache_save()
        else:
            # 清除所有 pending 状态 (慎用，通常只在完全重置时)
            self._set_last_ui_op("restore_all_skipped")
            return

    def _mark_upload_state_failed_for_ids(self, message: str, *record_ids):
        error_text = str(message or "上传失败，可重试。").strip() or "上传失败，可重试。"
        candidate_ids: list[str] = []
        for record_id in record_ids:
            text = str(record_id or "").strip()
            if not text:
                continue
            try:
                candidate_ids.extend(self._upload_completion_record_id_candidates(text))
            except Exception:
                candidate_ids.append(text)
        seen: set[str] = set()
        for record_id in candidate_ids:
            if not record_id or record_id in seen:
                continue
            seen.add(record_id)
            list_widget, item, _matched = self._find_active_item_by_upload_completion_id(
                record_id
            )
            if item and not self._is_valid_list_item(item):
                continue
            if list_widget is None or item is None:
                continue
            data = item.data(Qt.ItemDataRole.UserRole) or {}
            if not isinstance(data, dict):
                continue
            data = dict(data)
            data["_upload_in_progress"] = False
            data["_pending_upload_hash"] = None
            data["_has_unuploaded_changes"] = True
            data["_last_upload_error"] = error_text
            data.pop("_upload_started_monotonic", None)
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

    def clear_upload_runtime_state_for_ids(self, *record_ids):
        """Clear Qt-local upload markers for all aliases of the given IDs.

        The backend is the business owner now.  Qt may still temporarily mark a
        card as uploading while it waits for a backend command result, but the
        cleanup must cover both the old placeholder ID and the real target ID.
        """
        candidate_ids: list[str] = []
        for record_id in record_ids:
            text = str(record_id or "").strip()
            if not text:
                continue
            try:
                candidate_ids.extend(self._upload_completion_record_id_candidates(text))
            except Exception:
                candidate_ids.append(text)
        seen: set[str] = set()
        normalized_ids = []
        for record_id in candidate_ids:
            text = str(record_id or "").strip()
            if text and text not in seen:
                normalized_ids.append(text)
                seen.add(text)
        if not normalized_ids:
            return

        for record_id in normalized_ids:
            try:
                self.pending_action_record_ids.discard(record_id)
            except Exception:
                pass
            try:
                self.pending_action_types.pop(record_id, None)
            except Exception:
                pass
            try:
                self.pending_upload_rollback_by_record_id.pop(record_id, None)
            except Exception:
                pass

        changed = False
        for record_id in normalized_ids:
            list_widget, item, _matched = self._find_active_item_by_upload_completion_id(
                record_id
            )
            if item and not self._is_valid_list_item(item):
                continue
            if list_widget is None or item is None:
                continue
            data = item.data(Qt.ItemDataRole.UserRole) or {}
            if not isinstance(data, dict):
                continue
            if (
                data.get("_upload_in_progress")
                or data.get("_pending_upload_hash") is not None
                or data.get("_upload_started_monotonic") is not None
            ):
                data = dict(data)
                data["_upload_in_progress"] = False
                data["_pending_upload_hash"] = None
                data.pop("_upload_started_monotonic", None)
                item.setData(Qt.ItemDataRole.UserRole, data)
                self._rebuild_active_item_widget(
                    list_widget,
                    item,
                    data,
                    force_status=None,
                    upload_in_progress=False,
                    pending_upload_hash=None,
                    has_unuploaded_changes=data.get("_has_unuploaded_changes"),
                )
                changed = True
        if changed:
            try:
                self.request_active_cache_save()
            except Exception:
                pass

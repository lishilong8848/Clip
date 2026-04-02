# -*- coding: utf-8 -*-
import os
import copy
import json
import threading
import queue
import time
import hashlib
import uuid
from PyQt6.QtCore import Qt, QTimer

from ..logger import log_info, log_error, log_warning
from ..utils import HISTORY_FILE, WHITESPACE_TRANSLATOR
from ..services.service_registry import (
    upload_media_to_feishu,
    query_record_by_id,
    create_bitable_record_by_payload,
    update_bitable_record_by_payload,
)
from ..services.handlers import NoticePayload
from ..core.parser import extract_event_info

class MainWindowWorkflowMixin:
    def _collect_live_payload_runtime_keys(self) -> set[str]:
        keys = set()

        def add_from_data(data):
            if not isinstance(data, dict):
                return
            payload_key = str(data.get("payload_key") or "").strip()
            if payload_key:
                keys.add(payload_key)
            record_id = str(data.get("record_id") or "").strip()
            if record_id:
                keys.add(record_id)
            text = str(data.get("text") or "").strip()
            if text:
                info = extract_event_info(text) or {}
                unique_key = str(info.get("unique_key") or "").strip()
                if unique_key:
                    keys.add(unique_key)

        for _, item in self._iter_active_items():
            try:
                add_from_data(item.data(Qt.ItemDataRole.UserRole))
            except Exception:
                continue

        for mapping in (
            self.pending_replace_by_record_id,
            self.pending_upload_rollback_by_record_id,
            self.pending_end_rollback_by_record_id,
            self.pending_new_by_record_id,
            self.pending_update_after_upload,
        ):
            for value in mapping.values():
                if isinstance(value, dict):
                    add_from_data(value)
                    add_from_data(value.get("new_data"))
                    add_from_data(value.get("old_data"))

        for mapping in (self._upload_queues, self._upload_workers):
            for key in mapping.keys():
                key = str(key or "").strip()
                if key:
                    keys.add(key)
        return keys

    def _cleanup_runtime_payload_state(self) -> dict[str, int]:
        live_keys = self._collect_live_payload_runtime_keys()
        removed_store = 0
        removed_alias = 0

        for key in list(self._payload_store.keys()):
            if key not in live_keys:
                self._payload_store.pop(key, None)
                removed_store += 1

        for alias_key in list(self._payload_alias.keys()):
            target = str(self._payload_alias.get(alias_key) or "").strip()
            if alias_key not in live_keys and target not in live_keys:
                self._payload_alias.pop(alias_key, None)
                removed_alias += 1

        stats = {}
        if removed_store:
            stats["payload_store_trimmed"] = removed_store
        if removed_alias:
            stats["payload_alias_trimmed"] = removed_alias
        return stats

    def _cleanup_finished_upload_workers(self) -> dict[str, int]:
        live_keys = set(
            str(key or "").strip() for key in getattr(self, "_upload_queues", {}).keys()
        )
        removed = 0
        for key, worker in list(self._upload_workers.items()):
            try:
                alive = bool(worker and worker.is_alive())
            except Exception:
                alive = False
            if alive or str(key or "").strip() in live_keys:
                continue
            self._upload_workers.pop(key, None)
            removed += 1
        return {"upload_workers_trimmed": removed} if removed else {}
    def _enqueue_force_upload(self, data_dict: dict):
        # 旧版“先强制上传旧条再替换”链路已停用，不再入队。
        return

    def _try_process_pending_force_uploads(self):
        if self._pending_force_uploads:
            self._pending_force_uploads.clear()
        return

    def _defer_event(self, info):
        if not info:
            return
        entry_id = info.get("entry_id", "")
        if entry_id and any(
            e.get("entry_id") == entry_id for e in self._deferred_events
        ):
            return
        entry = {
            "content": info.get("content", ""),
            "status": info.get("status", ""),
            "title": info.get("title", ""),
            "notice_type": info.get("notice_type", ""),
            "entry_id": entry_id,
        }
        self._deferred_events.append(entry)
        if len(self._deferred_events) > 5:
            self._deferred_events.pop(0)

    def _try_process_deferred_events(self):
        if self._ui_update_in_progress:
            return
        if self.current_screenshot_record_id or self.screenshot_dialog.isVisible():
            return
        if not self._deferred_events:
            return
        pending = self._deferred_events.pop(0)
        content = pending.get("content")
        status = pending.get("status")
        notice_type = pending.get("notice_type")
        if content:
            self._process_event(content, status, notice_type)
            entry_id = pending.get("entry_id", "")
            if entry_id:
                self._remove_clipboard_pending_entry(entry_id)

    def _queue_update_after_upload(self, record_id: str, request: dict | None = None):
        if not record_id:
            return
        payload = request or {}
        try:
            payload = copy.deepcopy(payload)
        except Exception:
            payload = dict(payload)
        if "action_type" not in payload:
            payload["action_type"] = "update"
        existing = self.pending_update_after_upload.get(record_id) or {}
        if existing:
            merged = dict(existing)
            merged.update(payload)
            # 保留最近一次有效截图/响应时间，避免被空值覆盖。
            if (
                payload.get("screenshot_bytes") is None
                and existing.get("screenshot_bytes") is not None
            ):
                merged["screenshot_bytes"] = existing.get("screenshot_bytes")
            if not payload.get("extra_images") and existing.get("extra_images"):
                merged["extra_images"] = existing.get("extra_images")
            if not payload.get("response_time") and existing.get("response_time"):
                merged["response_time"] = existing.get("response_time")
            payload = merged
        self.pending_update_after_upload[record_id] = payload
        log_info(
            "PendingUpdate: queued "
            f"record_id={record_id} action={payload.get('action_type')} "
            f"has_screenshot={bool(payload.get('screenshot_bytes'))} "
            f"extra_images={len(payload.get('extra_images') or [])} "
            f"response_time={'Y' if payload.get('response_time') else 'N'}"
        )
        self._schedule_pending_update_after_upload()

    def _schedule_pending_update_after_upload(self, delay: int = 300):
        if self._pending_update_after_upload_scheduled:
            return
        self._pending_update_after_upload_scheduled = True
        QTimer.singleShot(delay, self._try_process_pending_update_after_upload)

    def _try_process_pending_update_after_upload(self):
        self._pending_update_after_upload_scheduled = False
        if not self.pending_update_after_upload:
            return
        if self._closing:
            return
        if self.current_screenshot_record_id or self.screenshot_dialog.isVisible():
            self._schedule_pending_update_after_upload(500)
            return
        if self._has_any_upload_in_progress():
            self._schedule_pending_update_after_upload(500)
            return
        for pending_key in list(self.pending_update_after_upload.keys()):
            target_record_id = pending_key
            list_widget, item = self._find_active_item_by_record_id(target_record_id)
            if not item or not self._is_valid_list_item(item):
                alias = self._payload_alias.get(target_record_id)
                if alias and alias != target_record_id:
                    list_widget, item = self._find_active_item_by_record_id(alias)
                    if item and self._is_valid_list_item(item):
                        target_record_id = alias
                if not item or not self._is_valid_list_item(item):
                    continue
            data = item.data(Qt.ItemDataRole.UserRole) or {}
            if self._is_placeholder_record(data):
                continue
            request = self.pending_update_after_upload.pop(pending_key, {}) or {}
            payload_data = request.get("data") or data
            if isinstance(payload_data, dict):
                payload_data = dict(payload_data)
                payload_data["_is_placeholder_record"] = False
                if data.get("record_id"):
                    payload_data["record_id"] = data.get("record_id")
                elif target_record_id:
                    payload_data["record_id"] = target_record_id
            notice_type = (
                payload_data.get("notice_type", "")
                if isinstance(payload_data, dict)
                else ""
            )
            dialog_level = ""
            if notice_type == "事件通告":
                dialog_level = str(request.get("event_level") or "").strip()
            elif notice_type in ("设备变更", "变更通告"):
                dialog_level = str(request.get("change_level") or "").strip()
            resolved = self._resolve_upload_fields_from_cache(
                payload_data if isinstance(payload_data, dict) else {},
                {
                    "buildings": request.get("buildings"),
                    "specialty": request.get("specialty"),
                    "level": dialog_level,
                    "event_source": request.get("event_source"),
                    "transfer_to_overhaul": (
                        payload_data.get("transfer_to_overhaul")
                        if isinstance(payload_data, dict)
                        else None
                    ),
                },
            )
            resolved_level = str(resolved.get("level") or "").strip()
            pending_change_level = (
                resolved_level if notice_type in ("设备变更", "变更通告") else ""
            )
            pending_event_level = resolved_level if notice_type == "事件通告" else ""
            action_type = request.get("action_type", "update")
            log_info(
                "PendingUpdate: dispatch "
                f"record_id={payload_data.get('record_id', target_record_id)} "
                f"action={action_type} "
                f"has_screenshot={bool(request.get('screenshot_bytes'))} "
                f"extra_images={len(request.get('extra_images') or [])} "
                f"response_time={'Y' if request.get('response_time') else 'N'}"
            )
            self.do_feishu_upload(
                payload_data,
                request.get("screenshot_bytes"),
                action_type,
                response_time=request.get("response_time", ""),
                buildings=resolved.get("buildings"),
                extra_images=request.get("extra_images"),
                specialty=resolved.get("specialty"),
                change_level=pending_change_level,
                event_level=pending_event_level,
                event_source=resolved.get("event_source"),
                recover_selected=bool(request.get("recover_selected", False)),
            )
            return
        if self.pending_update_after_upload:
            self._schedule_pending_update_after_upload(500)

    def _manual_update_has_target(self, content: str) -> bool:
        info = extract_event_info(content)
        if not info:
            return False
        list_widget, item = self._find_active_item_by_content_or_title(
            info.get("content") or content,
            info.get("title", ""),
            info.get("notice_type"),
            info.get("unique_key", ""),
        )
        return bool(item)

    def _validate_manual_record_id(self, record_id: str, content: str):
        info = extract_event_info(content)
        if not info:
            return False, "通告内容解析失败，无法校验记录ID。"
        notice_type = info.get("notice_type", "")
        if not notice_type:
            return False, "无法识别通告类型，无法校验记录ID。"
        success, result = query_record_by_id(record_id, notice_type)
        if success:
            return True, ""
        return False, str(result)

    def _handle_manual_update(self, content: str, record_id: str, info=None):
        info = info or extract_event_info(content)
        if not info:
            return
        notice_type = info.get("notice_type", "")
        if self._is_event_notice(notice_type):
            self._focus_event_tab()
        elif not self._is_active_view():
            self._set_view_mode(True)

        cleaned_text = info.get("content") or content
        data = {
            "record_id": record_id,
            "_is_placeholder_record": False,
            "text": cleaned_text,
            "notice_type": notice_type,
        }
        self._apply_detected_notice_fields(data, info)
        data = self._preserve_locked_level(data)
        entry = self._build_clipboard_entry(cleaned_text) or {}
        self._ensure_payload_for_data(data, entry=entry)

        list_widget, item = self._find_active_item_by_record_id(record_id)
        if item:
            if not self._is_valid_list_item(item):
                item = None
                list_widget = None
        if item:
            data["_has_unuploaded_changes"] = True
            data["_pending_upload_hash"] = None
            data["_upload_in_progress"] = False
            self._commit_active_record(
                data,
                refresh_detail=not self._should_defer_ui_refresh(),
                rebuild_widget=not self._should_defer_ui_refresh(),
                force_status="update",
                list_widget=list_widget,
                item=item,
            )
            if self._is_event_notice(notice_type):
                self._pin_item_to_top(list_widget, item)
            self.save_active_cache()
        else:
            self.add_active_item(data)

        if not self.isVisible():
            self.toggle_window()

    def _process_event(self, content, status, notice_type=None, entry=None):
        self._set_last_ui_op(
            "_process_event",
            status=status,
            notice_type=notice_type,
            entry_id=(entry or {}).get("entry_id", ""),
        )
        info = None
        if entry is None:
            info = extract_event_info(content)
            notice_type = notice_type or (info.get("notice_type") if info else "")
            entry = self._build_clipboard_entry(content) or {}
        else:
            notice_type = notice_type or entry.get("notice_type", "")
            if not info:
                info = extract_event_info(content)

        if self._is_event_notice(notice_type):
            self._focus_event_tab()

        title = ""
        if entry:
            title = entry.get("title") or ""
        if not title and info:
            title = info.get("title") or ""

        target_list, target_item = self._find_active_item_by_content_or_title(
            content,
            title,
            notice_type,
            (entry or {}).get("unique_key", "") or (info or {}).get("unique_key", ""),
        )
        if target_item and not self._is_valid_list_item(target_item):
            target_item = None
            target_list = None
        if target_item:
            current_data = target_item.data(Qt.ItemDataRole.UserRole) or {}
            if self._is_routing_conflicted(current_data):
                self.show_message(self._routing_error_text(current_data))
                return

        if status in ["开始", "新增", ""]:
            if target_item:
                old_data = target_item.data(Qt.ItemDataRole.UserRole)
                old_text = (old_data.get("text") or "").strip() if old_data else ""
                record_id = old_data.get("record_id") if old_data else None
                is_processing = (
                    bool(old_data.get("_upload_in_progress")) if old_data else False
                )
                if record_id and self._has_pending_upload(record_id):
                    is_processing = True
                if is_processing:
                    if record_id:
                        self._queue_pending_content(record_id, content, status)
                    return
                if old_data and old_text != content.strip():
                    if entry:
                        old_data["level"] = entry.get("level") or old_data.get("level")
                        old_data["source"] = entry.get("source") or old_data.get(
                            "source"
                        )
                        old_data["time_str"] = entry.get("time_str") or old_data.get(
                            "time_str"
                        )
                    old_data.pop("need_upload_first", None)
                    old_data["_has_unuploaded_changes"] = True
                    old_data["_pending_upload_hash"] = None
                    old_data["_upload_in_progress"] = False
                    self._auto_replace_content(
                        content,
                        old_data,
                        target_item,
                        False,
                        allow_placeholder_update=True,
                    )
                    if self._should_defer_ui_refresh():
                        self._mark_cache_refresh_needed()
                        return
                    if self._is_event_notice(notice_type):
                        self._pin_item_to_top(target_list, target_item)
                    self.save_active_cache()
                widget = self._safe_item_widget(target_list, target_item)
                self._maybe_flash(widget, target_list, target_item)
                log_info("系统: 内容已存在，已高亮显示")
                self._maybe_speak("该事件已存在")
            else:
                self.handle_new_event(content, info=info, entry=entry)
        elif status in ["更新", "结束"]:
            if target_item:
                if self._is_event_notice(notice_type):
                    self._pin_item_to_top(target_list, target_item)

                old_data = target_item.data(Qt.ItemDataRole.UserRole)
                if old_data:
                    self._ensure_payload_for_data(old_data, entry=entry)

                record_id = old_data.get("record_id") if old_data else None
                is_processing = (
                    bool(old_data.get("_upload_in_progress")) if old_data else False
                )
                if record_id and self._has_pending_upload(record_id):
                    is_processing = True
                if is_processing:
                    self._queue_pending_content(record_id, content, status)
                    return

                # 旧版“先强制上传旧条再替换”链路已移除，直接替换当前条为新内容。
                if old_data:
                    old_data.pop("need_upload_first", None)
                    target_item.setData(Qt.ItemDataRole.UserRole, old_data)

                # 设置按钮状态
                if old_data:
                    old_data["_has_unuploaded_changes"] = True
                    old_data["_pending_upload_hash"] = None
                    old_data["_upload_in_progress"] = False

                # 直接替换并闪烁，无需确认
                self._auto_replace_content(
                    content,
                    old_data,
                    target_item,
                    False,
                    allow_placeholder_update=self._is_placeholder_record(old_data),
                )
                if self._should_defer_ui_refresh():
                    self._mark_cache_refresh_needed()
                    return
            else:
                entry_id = (entry or {}).get("entry_id", "")
                title_for_log = (entry or {}).get("title", "") or (info or {}).get(
                    "title", ""
                )
                preview = " ".join(str(content or "").split())
                if len(preview) > 160:
                    preview = preview[:160] + "..."
                log_warning(
                    "未命中活动条目，已忽略更新/结束通告: "
                    f"status={status} notice_type={notice_type} "
                    f"title={title_for_log} entry_id={entry_id} preview={preview}"
                )
                self.show_message(
                    f"无法执行【{status}】操作：\n未在主界面找到对应条目，已忽略这条通告。"
                )
                self._maybe_speak("未找到对应条目")
        else:
            self.show_message(f"无法执行【{status}】操作：\n未在列表中找到对应原事件。")
            self._maybe_speak("无法更新，未找到对应事件")

    def _build_clipboard_entry(self, raw_text: str):
        info = extract_event_info(raw_text)
        if not info:
            return None
        return {
            "ts": int(time.time() * 1000),
            "unique_key": info.get("unique_key", ""),
            "notice_type": info.get("notice_type", ""),
            "status": info.get("status", ""),
            "title": info.get("title", ""),
            "content": info.get("content") or raw_text,
            "raw": raw_text,
            "level": info.get("level"),
            "source": info.get("source", ""),
            "time_str": info.get("time_str", ""),
        }

    def _get_payload_key(self, record_id: str = "", unique_key: str = "") -> str:
        if record_id:
            return self._payload_alias.get(record_id, record_id)
        return unique_key

    def _alias_payload_key(self, old_key: str, new_key: str):
        if not old_key or not new_key or old_key == new_key:
            return
        payload = self._payload_store.pop(old_key, None)
        if payload:
            self._payload_store[new_key] = payload
        self._payload_alias[old_key] = new_key
        self._payload_alias[new_key] = new_key

    def _update_payload_from_entry(self, payload: NoticePayload, entry: dict):
        if not payload or not entry:
            return
        payload.text = entry.get("content") or entry.get("raw") or payload.text
        payload.level = entry.get("level") or payload.level
        payload.event_source = entry.get("source") or payload.event_source
        payload.occurrence_date = entry.get("time_str") or payload.occurrence_date

    def _cleanup_payload_for_data(self, data_dict: dict):
        if not isinstance(data_dict, dict):
            return
        keys = set()
        payload_key = data_dict.get("payload_key")
        if payload_key:
            keys.add(payload_key)
        record_id = data_dict.get("record_id")
        if record_id:
            keys.add(record_id)
            alias = self._payload_alias.get(record_id)
            if alias:
                keys.add(alias)
        info = extract_event_info(data_dict.get("text", "")) or {}
        unique_key = info.get("unique_key")
        if unique_key:
            keys.add(unique_key)
        for key in list(keys):
            if key:
                self._payload_store.pop(key, None)
        for alias_key in list(self._payload_alias.keys()):
            target = self._payload_alias.get(alias_key)
            if alias_key in keys or (target and target in keys):
                self._payload_alias.pop(alias_key, None)

    def _calc_text_hash(self, text: str) -> str:
        return hashlib.md5((text or "").encode("utf-8", errors="ignore")).hexdigest()

    def _ensure_payload_for_data(self, data_dict: dict, entry: dict = None):
        if not isinstance(data_dict, dict):
            return None
        record_id = data_dict.get("record_id", "")
        payload_key = data_dict.get("payload_key", "")
        if not payload_key and entry:
            payload_key = entry.get("unique_key", "")
        if not payload_key and record_id:
            payload_key = record_id
        if not payload_key:
            info = extract_event_info(data_dict.get("text", "")) or {}
            payload_key = info.get("unique_key", "") or record_id
        if payload_key in self._payload_store:
            payload = self._payload_store[payload_key]
            if entry:
                self._update_payload_from_entry(payload, entry)
            if "transfer_to_overhaul" in data_dict:
                payload.transfer_to_overhaul = data_dict.get("transfer_to_overhaul")
            data_dict["payload_key"] = payload_key
            return payload
        payload = NoticePayload(
            text=data_dict.get("text", ""),
            level=data_dict.get("level"),
            buildings=data_dict.get("buildings"),
            specialty=data_dict.get("specialty"),
            event_source=data_dict.get("source"),
            occurrence_date=data_dict.get("time_str"),
            transfer_to_overhaul=data_dict.get("transfer_to_overhaul"),
        )
        self._payload_store[payload_key] = payload
        data_dict["payload_key"] = payload_key
        if entry:
            self._update_payload_from_entry(payload, entry)
        return payload

    def handle_new_event(self, content, info=None, entry=None):
        # record_id 生成: 使用本地 UUID，上传成功后回填真实 record_id。
        info = info or extract_event_info(content)
        if not info:
            return
        defer_ui = self._should_defer_ui_refresh()
        placeholder_record_id = uuid.uuid4().hex

        notice_type = info.get("notice_type", "")
        level = info.get("level")
        source = info.get("source", "")
        time_str = info.get("time_str", "")
        prefilled_buildings = self._infer_buildings_from_notice_text(content)

        data = {
            "record_id": placeholder_record_id,
            "_is_placeholder_record": True,
            "text": content,
            "notice_type": notice_type,
            "level": level,  # 保存等级字段
            "source": source,  # 保存来源字段
            "time_str": time_str,  # 保存时间字段
            "buildings": prefilled_buildings,
        }
        data["_has_unuploaded_changes"] = True
        data["_pending_upload_hash"] = None
        data["_upload_in_progress"] = False
        if entry:
            self._ensure_payload_for_data(data, entry=entry)
        else:
            self._ensure_payload_for_data(data)
        if self._is_event_notice(notice_type):
            self._focus_event_tab()

        existing_list, existing_item = self._find_active_item_by_content_or_title(
            content,
            info.get("title", ""),
            notice_type,
            info.get("unique_key", ""),
        )
        if existing_item and not self._is_valid_list_item(existing_item):
            existing_item = None
            existing_list = None
        if existing_item:
            try:
                existing_data = existing_item.data(Qt.ItemDataRole.UserRole) or {}
                if self._is_routing_conflicted(existing_data):
                    self.show_message(self._routing_error_text(existing_data))
                    return
                routed_data = dict(existing_data)
                routed_data["text"] = content
                routed_data["notice_type"] = notice_type
                routed_data["level"] = level
                routed_data["source"] = source
                routed_data["time_str"] = time_str
                routed_data["buildings"] = self._resolve_prefilled_buildings(
                    existing_data.get("buildings"),
                    data.get("buildings"),
                )
                routed_data["_has_unuploaded_changes"] = True
                routed_data["_pending_upload_hash"] = None
                routed_data["_upload_in_progress"] = False
                routed_data = self._ensure_active_item_identity(routed_data)
                committed = self._commit_active_record(
                    routed_data,
                    refresh_detail=not defer_ui,
                    rebuild_widget=not defer_ui,
                    list_widget=existing_list,
                    item=existing_item,
                )
                self.save_active_cache()
                if defer_ui:
                    self._mark_cache_refresh_needed()
                    return
                self._log_detail_preview_update(
                    committed or routed_data,
                    (committed or routed_data).get("record_id"),
                    reason="new_event_existing",
                )
                self.save_active_cache()
                return
            except Exception as exc:
                log_error(
                    "handle_new_event routed update failed: "
                    f"record_id={existing_data.get('record_id', '') if isinstance(existing_data, dict) else ''}, "
                    f"notice_type={notice_type}, error={exc}"
                )
                self._schedule_pending_cache_refresh()
                return

        item, widget = self.add_active_item(data)
        if defer_ui:
            self._mark_cache_refresh_needed()
        else:
            self._log_detail_preview_update(
                data, data.get("record_id"), reason="new_event"
            )
        if self._is_event_notice(notice_type):
            if not defer_ui and item and widget:
                list_widget = item.listWidget()
                list_widget.setCurrentItem(item)
                list_widget.scrollToItem(item)
                self._notify_new_event(widget)

        if not self.isVisible():
            self.toggle_window()

    def _auto_replace_content(
        self,
        new_text,
        old_data,
        item_ref,
        upload_old_first=False,
        allow_placeholder_update=False,
    ):
        self._set_last_ui_op(
            "_auto_replace_content",
            record_id=(old_data or {}).get("record_id"),
            upload_old_first=upload_old_first,
        )
        if not item_ref or not old_data:
            return
        if not self._is_valid_list_item(item_ref):
            return
        list_widget = item_ref.listWidget()
        if list_widget is None:
            return
        new_data = old_data.copy()
        new_data["text"] = new_text
        self.apply_replace(
            new_data,
            item_ref,
            upload_old_first=upload_old_first,
            allow_placeholder_update=allow_placeholder_update,
        )
        if self._should_defer_ui_refresh():
            self._mark_cache_refresh_needed()
            return
        self._maybe_update_detail_dialog(new_data, new_data.get("record_id"))
        widget = self._safe_item_widget(list_widget, item_ref)
        self._maybe_flash(widget, list_widget, item_ref)

    def apply_replace(
        self,
        new_data,
        item_ref,
        upload_old_first=False,
        allow_placeholder_update=False,
    ):
        defer_ui = self._should_defer_ui_refresh()
        self._set_last_ui_op(
            "apply_replace",
            record_id=(new_data or {}).get("record_id"),
            upload_old_first=upload_old_first,
        )
        if not item_ref:
            return
        if not self._is_valid_list_item(item_ref):
            return
        # 确保替换后的数据沿用同一个 record_id（保持后续更新/结束都指向同一条记录）
        current_data = item_ref.data(Qt.ItemDataRole.UserRole)
        if current_data and "record_id" in current_data and "record_id" not in new_data:
            new_data["record_id"] = current_data["record_id"]
            # placeholder 标记沿用
            if "_is_placeholder_record" in current_data:
                new_data["_is_placeholder_record"] = current_data[
                    "_is_placeholder_record"
                ]
        new_data = self._inherit_active_runtime_fields(new_data, current_data)
        if isinstance(current_data, dict) and "need_upload_first" in current_data:
            current_data = dict(current_data)
            current_data.pop("need_upload_first", None)
            item_ref.setData(Qt.ItemDataRole.UserRole, current_data)
        new_data.pop("need_upload_first", None)
        if upload_old_first:
            upload_old_first = False
            record_id = str(new_data.get("record_id") or "").strip()
            if record_id:
                self.pending_replace_by_record_id.pop(record_id, None)
                self.pending_action_record_ids.discard(record_id)
        entry = self._build_clipboard_entry(new_data.get("text", "") or "")
        if not entry:
            entry = {"content": new_data.get("text", "")}
        self._ensure_payload_for_data(new_data, entry=entry)
        list_widget = item_ref.listWidget()

        if current_data and current_data.get("_is_placeholder_record", True):
            if not allow_placeholder_update:
                self.restore_button_state(record_id=new_data.get("record_id"))
                return
        item_ref.setData(Qt.ItemDataRole.UserRole, new_data)
        new_data["_pending_upload_hash"] = None
        new_data["_has_unuploaded_changes"] = True
        new_data["_upload_in_progress"] = False
        committed = self._commit_active_record(
            new_data,
            refresh_detail=not defer_ui,
            rebuild_widget=False,
            list_widget=list_widget,
            item=item_ref,
        )
        self.save_active_cache()
        if defer_ui:
            self._mark_cache_refresh_needed()
            return
        force_status = None
        if current_data and current_data.get("_is_placeholder_record", True):
            action_type = self.pending_action_types.get(new_data.get("record_id"))
            if not action_type:
                alias = self._upload_key_alias.get(new_data.get("record_id"))
                if alias:
                    action_type = self.pending_action_types.get(alias)
            if action_type == "upload":
                force_status = "update"
        self._rebuild_active_item_widget(
            list_widget,
            item_ref,
            committed or new_data,
            force_status=force_status,
            upload_in_progress=False,
            pending_upload_hash=None,
            has_unuploaded_changes=True,
        )
        if not allow_placeholder_update:
            self.restore_button_state(record_id=new_data.get("record_id"))
        self.save_active_cache()

    def move_to_history(self, data_dict):
        log_info(f"UI操作: 结束事件, Record ID: {data_dict['record_id']}")
        self._show_screenshot_dialog(data_dict, "end")

    def _delete_active_item(self, data_dict):
        """删除活动列表项（滑动删除触发） -> 移动到历史"""
        if self._is_screenshot_dialog_active():
            record_id = (data_dict or {}).get("record_id")
            list_widget, item = self._find_active_item_by_record_id(record_id)
            widget = self._safe_item_widget(list_widget, item)
            if widget and hasattr(widget, "cancel_delete_visual"):
                try:
                    widget.cancel_delete_visual()
                except Exception:
                    pass
            self.show_message("截图上传进行中，暂时不能删除条目。")
            return
        log_info(f"UI操作: 删除事件(移入历史), Record ID: {data_dict['record_id']}")
        self._do_move_to_history(None, data_dict)

    def handle_action(self, data_dict, action_type):
        block_reason = self._dialog_block_reason("screenshot")
        if block_reason:
            self.show_message(block_reason)
            return
        if self._is_record_binding_conflicted(data_dict):
            self.show_message(self._record_binding_error_text(data_dict))
            return
        if self._is_routing_conflicted(data_dict):
            self.show_message(self._routing_error_text(data_dict))
            return
        record_id = data_dict["record_id"]
        if record_id in self.pending_action_record_ids:
            if not self._is_placeholder_record(data_dict):
                self.show_message("该项目正在处理中，请稍候...")
                return

        self.pending_action_record_ids.add(record_id)
        self.pending_action_types[record_id] = action_type
        pending_hash = self._calc_text_hash(data_dict.get("text", ""))
        data_dict["_pending_upload_hash"] = pending_hash
        data_dict["_has_unuploaded_changes"] = False
        data_dict["_upload_in_progress"] = True
        list_widget, item = self._find_active_item_by_record_id(record_id)
        if item and not self._is_valid_list_item(item):
            item = None
            list_widget = None
        if list_widget and item:
            item.setData(Qt.ItemDataRole.UserRole, data_dict)
            # 点击后立即显示“已上传”，避免中间蓝色状态
            self._rebuild_active_item_widget(
                list_widget,
                item,
                data_dict,
                force_status=None,
                upload_in_progress=True,
                pending_upload_hash=pending_hash,
                has_unuploaded_changes=False,
            )
        self.save_active_cache()
        self._show_screenshot_dialog(data_dict, action_type)

    def _get_upload_key(self, record_id):
        if not record_id:
            return "unknown"
        alias = self._upload_key_alias.get(record_id)
        return alias or record_id

    def _alias_upload_key(self, old_id, new_id):
        if not old_id or not new_id or old_id == new_id:
            return
        with self._upload_lock:
            if old_id in self._upload_queues or old_id in self._upload_workers:
                self._upload_key_alias[new_id] = old_id

    def _clear_upload_queue(self, record_id):
        key = self._get_upload_key(record_id)
        with self._upload_lock:
            task_queue = self._upload_queues.pop(key, None)
            worker = self._upload_workers.pop(key, None)
            stale = [
                alias_key
                for alias_key, target in self._upload_key_alias.items()
                if target == key or alias_key == record_id
            ]
            for alias_key in stale:
                self._upload_key_alias.pop(alias_key, None)
        if task_queue:
            try:
                while not task_queue.empty():
                    task_queue.get_nowait()
                    task_queue.task_done()
            except queue.Empty:
                pass
            task_queue.put(None)
        return worker

    def _start_upload_worker(self, key, task_queue):
        def worker():
            while True:
                try:
                    task = task_queue.get(timeout=2.0)
                except queue.Empty:
                    with self._upload_lock:
                        if task_queue.empty():
                            self._upload_queues.pop(key, None)
                            self._upload_workers.pop(key, None)
                            stale = [
                                alias_key
                                for alias_key, target in self._upload_key_alias.items()
                                if target == key
                            ]
                            for alias_key in stale:
                                self._upload_key_alias.pop(alias_key, None)
                            return
                    continue
                try:
                    if task is None:
                        return
                    try:
                        task()
                    except Exception as exc:
                        log_error(f"上传任务异常: {exc}")
                        self._post_request_finished(
                            "上传", False, f"上传异常: {exc}", None
                        )
                finally:
                    task_queue.task_done()

        thread = threading.Thread(target=worker, daemon=True)
        self._upload_workers[key] = thread
        thread.start()

    def _enqueue_upload(self, record_id, task):
        key = self._get_upload_key(record_id)
        with self._upload_lock:
            task_queue = self._upload_queues.get(key)
            if not task_queue:
                task_queue = queue.Queue()
                self._upload_queues[key] = task_queue
            if key not in self._upload_workers:
                self._start_upload_worker(key, task_queue)
        task_queue.put(task)

    def _mark_upload_queued(self, record_id):
        if not record_id:
            return
        if record_id in self.pending_action_record_ids:
            self.pending_action_record_ids.remove(record_id)
        list_widget, item = self._find_active_item_by_record_id(record_id)
        if item and not self._is_valid_list_item(item):
            item = None
            list_widget = None
        if list_widget and item:
            data = item.data(Qt.ItemDataRole.UserRole) or {}
            pending_hash = self._calc_text_hash(data.get("text", ""))
            data["_pending_upload_hash"] = pending_hash
            data["_has_unuploaded_changes"] = False
            data["_upload_in_progress"] = True
            item.setData(Qt.ItemDataRole.UserRole, data)
            self._rebuild_active_item_widget(
                list_widget,
                item,
                data,
                force_status=None,
                upload_in_progress=True,
                pending_upload_hash=pending_hash,
                has_unuploaded_changes=False,
            )
            self.save_active_cache()

    def _has_pending_upload(self, record_id):
        key = self._get_upload_key(record_id)
        with self._upload_lock:
            task_queue = self._upload_queues.get(key)
            if not task_queue:
                return False
            if not task_queue.empty():
                return True
            worker = self._upload_workers.get(key)
            return bool(worker and worker.is_alive())

    def _queue_pending_content(self, record_id, new_content, new_status):
        if not record_id:
            return
        list_widget, item = self._find_active_item_by_record_id(record_id)
        if item and not self._is_valid_list_item(item):
            item = None
            list_widget = None
        if list_widget and item:
            old_data = item.data(Qt.ItemDataRole.UserRole) or {}
            if record_id not in self.pending_upload_rollback_by_record_id:
                try:
                    rollback_snapshot = copy.deepcopy(old_data)
                except Exception:
                    rollback_snapshot = dict(old_data)
                self.pending_upload_rollback_by_record_id[record_id] = {
                    "old_data": rollback_snapshot
                }

            info = extract_event_info(new_content) or {}
            cleaned_content = info.get("content") or new_content
            new_data = dict(old_data)
            new_data["text"] = cleaned_content
            self._apply_detected_notice_fields(new_data, info)
            new_data = self._preserve_locked_level(new_data)
            entry = self._build_clipboard_entry(cleaned_content) or {
                "content": cleaned_content
            }
            self._ensure_payload_for_data(new_data, entry=entry)

            # 上传中收到新内容：立即替换，但保持上传中标记
            new_data["_has_unuploaded_changes"] = True
            if not new_data.get("_pending_upload_hash"):
                base_text = old_data.get("text", "")
                new_data["_pending_upload_hash"] = self._calc_text_hash(base_text)
            new_data["_upload_in_progress"] = bool(old_data.get("_upload_in_progress"))
            if record_id and self._has_pending_upload(record_id):
                new_data["_upload_in_progress"] = True
            item.setData(Qt.ItemDataRole.UserRole, new_data)
            self.save_active_cache()
            if self._should_defer_ui_refresh():
                self._mark_cache_refresh_needed()
                return
            action_type = self.pending_action_types.get(record_id)
            if not action_type:
                alias = self._upload_key_alias.get(record_id)
                if alias:
                    action_type = self.pending_action_types.get(alias)
            force_status = None
            if new_status == "结束":
                force_status = "end"
            elif action_type == "upload":
                force_status = "update"
            self._rebuild_active_item_widget(
                list_widget,
                item,
                new_data,
                force_status=force_status,
                upload_in_progress=new_data.get("_upload_in_progress"),
                pending_upload_hash=new_data.get("_pending_upload_hash"),
                has_unuploaded_changes=True,
            )
            self._maybe_update_detail_dialog(new_data, record_id)
            self.save_active_cache()

    def do_feishu_upload(
        self,
        data_dict,
        screenshot_bytes,
        action_type,
        response_time="",
        buildings=None,
        extra_images=None,
        specialty=None,
        change_level=None,
        event_level=None,
        event_source=None,
        recover_selected=False,
    ):
        if not data_dict:
            return
        if not hasattr(self, "_feishu_request_lock"):
            self._feishu_request_lock = threading.Lock()
        end_item_ref = None
        self._set_delete_interaction_enabled(True)
        # 截图确认后恢复剪贴板监听
        self._resume_clipboard_timer()
        if self._pending_cache_refresh:
            self._refresh_ui_from_cache()
        self._try_process_pending_force_uploads()
        extra_images = list(extra_images or [])
        notice_type = data_dict.get("notice_type", "")
        change_level = str(change_level or "").strip()
        event_level = str(event_level or "").strip()
        event_source = str(event_source or "").strip()
        dialog_level = ""
        if notice_type == "事件通告":
            dialog_level = event_level
        elif notice_type in ("设备变更", "变更通告"):
            dialog_level = change_level
        resolved_fields = self._resolve_upload_fields_from_cache(
            data_dict,
            {
                "buildings": buildings,
                "specialty": specialty,
                "level": dialog_level,
                "event_source": event_source,
                "transfer_to_overhaul": data_dict.get("transfer_to_overhaul"),
            },
        )
        specialty = str(resolved_fields.get("specialty") or "").strip()
        event_source = str(resolved_fields.get("event_source") or "").strip()
        _buildings = self._normalize_buildings_value(resolved_fields.get("buildings"))
        resolved_level = str(resolved_fields.get("level") or "").strip()
        if notice_type == "事件通告":
            event_level = resolved_level
        elif notice_type in ("设备变更", "变更通告"):
            change_level = resolved_level
        elif resolved_level:
            data_dict["level"] = resolved_level
        record_id = data_dict.get("record_id")
        if record_id:
            data_dict["_upload_in_progress"] = True
            list_widget, item = self._find_active_item_by_record_id(record_id)
            if item and not self._is_valid_list_item(item):
                item = None
                list_widget = None
            if list_widget and item:
                data = item.data(Qt.ItemDataRole.UserRole) or {}
                data["_upload_in_progress"] = True
                item.setData(Qt.ItemDataRole.UserRole, data)
                self._rebuild_active_item_widget(
                    list_widget,
                    item,
                    data,
                    force_status=None,
                    upload_in_progress=True,
                    pending_upload_hash=data.get("_pending_upload_hash"),
                    has_unuploaded_changes=data.get("_has_unuploaded_changes"),
                )
        data_dict["buildings"] = _buildings
        if specialty:
            data_dict["specialty"] = specialty
        else:
            data_dict.pop("specialty", None)
        if notice_type in ("设备变更", "变更通告"):
            if change_level:
                data_dict["level"] = change_level
            else:
                data_dict.pop("level", None)
        if notice_type == "事件通告":
            if event_level:
                data_dict["level"] = event_level
            else:
                data_dict.pop("level", None)
            if event_source:
                data_dict["event_source"] = event_source
            else:
                data_dict.pop("event_source", None)
        if response_time:
            data_dict["last_response_time"] = response_time
        self._update_active_item_data(data_dict.get("record_id"), data_dict)

        if record_id and self._is_event_notice(notice_type):
            list_widget, item = self._find_active_item_by_record_id(record_id)
            if item:
                w = self._safe_item_widget(list_widget, item)
                if w and getattr(w, "timer_widget", None):
                    w.timer_widget.update_context(
                        text=data_dict.get("text"),
                        buildings=_buildings,
                        level=data_dict.get("level"),
                    )
        self.save_active_cache()

        payload = self._ensure_payload_for_data(data_dict)
        if payload:
            payload.text = data_dict.get("text", payload.text)
            payload.level = data_dict.get("level") or payload.level
            payload.buildings = _buildings
            payload.specialty = specialty
            payload.event_source = event_source
            payload.occurrence_date = (
                data_dict.get("time_str") or payload.occurrence_date
            )
            payload.transfer_to_overhaul = data_dict.get("transfer_to_overhaul")
        payload_base = copy.deepcopy(payload) if payload else None

        if action_type == "end" and not data_dict.get("_ended_moved"):
            self._do_move_to_history(end_item_ref, data_dict, track_rollback=True)
            data_dict["_ended_moved"] = True

        data_snapshot = dict(data_dict) if isinstance(data_dict, dict) else data_dict

        def task():
            # 从外部作用域获取参数
            # 解决 UnboundLocalError: 先从 data_snapshot 获取 notice_type，覆盖外部同名变量
            notice_type = data_snapshot.get("notice_type", "")
            record_id = data_snapshot.get("record_id")

            if notice_type == "事件通告":
                _level = event_level or data_snapshot.get("level")
            elif notice_type in ("设备变更", "变更通告"):
                _level = change_level or data_snapshot.get("level")
            else:
                _level = data_snapshot.get("level")

            # 首次上传未回填真实 record_id 时：
            # 对“更新/结束”先入等待队列，待 record_id 回填后再真正上传，避免重复上传媒体。
            if action_type in ("update", "end") and self._is_placeholder_record(
                data_snapshot
            ):
                self._queue_update_after_upload(
                    record_id,
                    {
                        "action_type": action_type,
                        "data": data_snapshot,
                        "screenshot_bytes": screenshot_bytes,
                        "response_time": response_time,
                        "buildings": list(_buildings),
                        "extra_images": extra_images,
                        "specialty": specialty,
                        "change_level": change_level,
                        "event_level": event_level,
                        "event_source": event_source,
                        "recover_selected": recover_selected,
                    },
                )
                return

            file_tokens = []
            if screenshot_bytes:
                with self._feishu_request_lock:
                    success, result = upload_media_to_feishu(screenshot_bytes)
                if success:
                    file_tokens.append(result)
                else:
                    self._post_request_finished(
                        "截图上传",
                        False,
                        result,
                        data_snapshot.get("record_id"),
                    )
                    return

            extra_file_tokens = []
            if extra_images:
                for idx, item in enumerate(extra_images, start=1):
                    if isinstance(item, (list, tuple)) and len(item) >= 2:
                        image_bytes = item[0]
                        file_name = item[1] or f"extra_{idx}.png"
                    else:
                        image_bytes = item
                        file_name = f"extra_{idx}.png"
                    if not image_bytes:
                        continue
                    with self._feishu_request_lock:
                        success, result = upload_media_to_feishu(
                            image_bytes, file_name=file_name
                        )
                    if success:
                        extra_file_tokens.append(result)
                    else:
                        self._post_request_finished(
                            "截图上传",
                            False,
                            result,
                            data_snapshot.get("record_id"),
                        )
                        return

            record_id = data_snapshot["record_id"]
            notice_type = data_snapshot.get("notice_type", "")
            time_str = data_snapshot.get("time_str", "")  # 发生日期
            transfer_to_overhaul = data_snapshot.get("transfer_to_overhaul")

            if action_type == "upload":
                payload = (
                    copy.deepcopy(payload_base)
                    if payload_base
                    else NoticePayload(text=data_snapshot["text"])
                )
                payload.transfer_to_overhaul = transfer_to_overhaul
                payload.text = data_snapshot["text"]
                payload.level = _level or payload.level
                payload.buildings = _buildings
                payload.specialty = specialty
                payload.event_source = event_source
                payload.file_tokens = file_tokens if file_tokens else None
                payload.extra_file_tokens = (
                    extra_file_tokens if extra_file_tokens else None
                )
                payload.response_time = response_time
                payload.occurrence_date = time_str
                payload.existing_file_tokens = None
                payload.existing_extra_file_tokens = None
                payload.existing_response_time = None
                payload.recover = recover_selected
                with self._feishu_request_lock:
                    s, r = create_bitable_record_by_payload(notice_type, payload)
                self._post_request_finished("上传", s, r if s else r, record_id)
            elif action_type == "update":
                with self._feishu_request_lock:
                    s, qr = query_record_by_id(record_id, notice_type)
                if s:
                    # record_id = qr["record_id"] # already have it check validity?
                    existing_tokens = []
                    existing_extra_tokens = []
                    fields = qr.get("fields", {}) if qr else {}
                    if notice_type == "事件通告":
                        for f in fields.get("进展更新截图", []) or []:
                            if "file_token" in f:
                                existing_tokens.append(f["file_token"])
                        for f in fields.get("事件恢复截图", []) or []:
                            if "file_token" in f:
                                existing_extra_tokens.append(f["file_token"])
                    elif notice_type == "维保通告":
                        for f in fields.get("过程通告图片", []) or []:
                            if "file_token" in f:
                                existing_tokens.append(f["file_token"])
                        for f in fields.get("过程现场图片", []) or []:
                            if "file_token" in f:
                                existing_extra_tokens.append(f["file_token"])
                    elif notice_type == "设备检修":
                        for f in fields.get("过程通告截图", []) or []:
                            if "file_token" in f:
                                existing_tokens.append(f["file_token"])
                        for f in fields.get("过程现场图片", []) or []:
                            if "file_token" in f:
                                existing_extra_tokens.append(f["file_token"])
                    elif notice_type in ("设备变更", "变更通告"):
                        for f in fields.get("过程更新钉钉截图", []) or []:
                            if "file_token" in f:
                                existing_tokens.append(f["file_token"])
                    elif notice_type in (
                        "上下电通告",
                        "上电通告",
                        "下电通告",
                        "设备轮巡",
                        "设备调整",
                    ):
                        for f in fields.get("过程通告截图", []) or []:
                            if "file_token" in f:
                                existing_tokens.append(f["file_token"])
                    else:
                        for f in fields.get("截图", []) or []:
                            if "file_token" in f:
                                existing_tokens.append(f["file_token"])

                    if notice_type in ("设备变更", "变更通告"):
                        existing_resp_time = qr.get("fields", {}).get(
                            "过程更新时间", ""
                        )
                    else:
                        existing_resp_time = qr.get("fields", {}).get(
                            "进展更新时间", ""
                        )
                    payload = (
                        copy.deepcopy(payload_base)
                        if payload_base
                        else NoticePayload(text=data_snapshot["text"])
                    )
                    payload.transfer_to_overhaul = transfer_to_overhaul
                    payload.text = data_snapshot["text"]
                    payload.level = _level or payload.level
                    payload.buildings = _buildings
                    payload.specialty = specialty
                    payload.event_source = event_source
                    payload.file_tokens = file_tokens
                    payload.extra_file_tokens = extra_file_tokens
                    payload.existing_file_tokens = existing_tokens
                    payload.existing_extra_file_tokens = existing_extra_tokens
                    payload.response_time = response_time
                    payload.existing_response_time = existing_resp_time
                    payload.recover = recover_selected
                    with self._feishu_request_lock:
                        s2, r2 = update_bitable_record_by_payload(
                            record_id,
                            notice_type,
                            payload,
                        )
                    self._post_request_finished("更新", s2, r2, record_id)
                else:
                    self._post_request_finished(
                        "更新", False, f"查询失败: {qr}", record_id
                    )
            elif action_type == "end":
                with self._feishu_request_lock:
                    s, qr = query_record_by_id(record_id, notice_type)
                if s:
                    existing_tokens = []
                    existing_extra_tokens = []
                    fields = qr.get("fields", {}) if qr else {}
                    if notice_type == "维保通告":
                        for f in fields.get("过程通告图片", []) or []:
                            if "file_token" in f:
                                existing_tokens.append(f["file_token"])
                        for f in fields.get("过程现场图片", []) or []:
                            if "file_token" in f:
                                existing_extra_tokens.append(f["file_token"])
                    elif notice_type == "设备检修":
                        for f in fields.get("过程通告截图", []) or []:
                            if "file_token" in f:
                                existing_tokens.append(f["file_token"])
                        for f in fields.get("过程现场图片", []) or []:
                            if "file_token" in f:
                                existing_extra_tokens.append(f["file_token"])
                    elif notice_type in ("设备变更", "变更通告"):
                        for f in fields.get("过程更新钉钉截图", []) or []:
                            if "file_token" in f:
                                existing_tokens.append(f["file_token"])
                    elif notice_type in (
                        "上下电通告",
                        "上电通告",
                        "下电通告",
                        "设备轮巡",
                        "设备调整",
                    ):
                        for f in fields.get("过程通告截图", []) or []:
                            if "file_token" in f:
                                existing_tokens.append(f["file_token"])
                    else:
                        for f in fields.get("截图", []) or []:
                            if "file_token" in f:
                                existing_tokens.append(f["file_token"])
                    payload = (
                        copy.deepcopy(payload_base)
                        if payload_base
                        else NoticePayload(text=data_snapshot["text"])
                    )
                    payload.transfer_to_overhaul = transfer_to_overhaul
                    payload.text = data_snapshot["text"]
                    payload.level = _level or payload.level
                    payload.buildings = _buildings
                    payload.specialty = specialty
                    payload.event_source = event_source
                    payload.file_tokens = file_tokens if file_tokens else None
                    payload.extra_file_tokens = (
                        extra_file_tokens if extra_file_tokens else None
                    )
                    payload.existing_file_tokens = existing_tokens
                    payload.existing_extra_file_tokens = existing_extra_tokens
                    payload.response_time = response_time
                    payload.existing_response_time = None
                    payload.recover = recover_selected
                    with self._feishu_request_lock:
                        s_end, r_end = update_bitable_record_by_payload(
                            record_id,
                            notice_type,
                            payload,
                        )
                    self._post_request_finished("结束", s_end, r_end, record_id)
                else:
                    self._post_request_finished(
                        "结束", False, f"查询失败: {qr}", record_id
                    )
            elif action_type == "upload_replace":
                # 如果已有真实 record_id，则更新原记录；否则新建
                if self._is_placeholder_record(data_snapshot):
                    payload = (
                        copy.deepcopy(payload_base)
                        if payload_base
                        else NoticePayload(text=data_snapshot["text"])
                    )
                    payload.transfer_to_overhaul = transfer_to_overhaul
                    payload.text = data_snapshot["text"]
                    payload.level = _level or payload.level
                    payload.buildings = _buildings
                    payload.specialty = specialty
                    payload.event_source = event_source
                    payload.file_tokens = file_tokens if file_tokens else None
                    payload.extra_file_tokens = (
                        extra_file_tokens if extra_file_tokens else None
                    )
                    payload.response_time = response_time
                    payload.occurrence_date = time_str
                    payload.existing_file_tokens = None
                    payload.existing_extra_file_tokens = None
                    payload.existing_response_time = None
                    payload.recover = recover_selected

                    # service_registry 内部已实现全局锁，此处直接调用
                    s, r = create_bitable_record_by_payload(
                        notice_type,
                        payload,
                    )
                else:
                    existing_tokens = []
                    existing_extra_tokens = []
                    existing_resp_time = ""

                    # service_registry 内部已实现全局锁，此处直接调用
                    try:
                        s_query, qr = query_record_by_id(record_id, notice_type)
                    except Exception as e:
                        log_error(f"查询飞书记录失败 (SSL 错误): {e}")
                        self._post_request_finished(
                            "上传", False, f"查询记录失败: {e}", None
                        )
                        return

                    if s_query:
                        fields = qr.get("fields", {}) if qr else {}
                        if notice_type == "事件通告":
                            for f in fields.get("进展更新截图", []) or []:
                                if "file_token" in f:
                                    existing_tokens.append(f["file_token"])
                            for f in fields.get("事件恢复截图", []) or []:
                                if "file_token" in f:
                                    existing_extra_tokens.append(f["file_token"])
                        elif notice_type == "维保通告":
                            for f in fields.get("过程通告图片", []) or []:
                                if "file_token" in f:
                                    existing_tokens.append(f["file_token"])
                            for f in fields.get("过程现场图片", []) or []:
                                if "file_token" in f:
                                    existing_extra_tokens.append(f["file_token"])
                        elif notice_type == "设备检修":
                            for f in fields.get("过程通告截图", []) or []:
                                if "file_token" in f:
                                    existing_tokens.append(f["file_token"])
                            for f in fields.get("过程现场图片", []) or []:
                                if "file_token" in f:
                                    existing_extra_tokens.append(f["file_token"])
                        elif notice_type in ("设备变更", "变更通告"):
                            for f in fields.get("过程更新钉钉截图", []) or []:
                                if "file_token" in f:
                                    existing_tokens.append(f["file_token"])
                        elif notice_type in (
                            "上下电通告",
                            "上电通告",
                            "下电通告",
                            "设备轮巡",
                            "设备调整",
                        ):
                            for f in fields.get("过程通告截图", []) or []:
                                if "file_token" in f:
                                    existing_tokens.append(f["file_token"])
                        else:
                            for f in fields.get("截图", []) or []:
                                if "file_token" in f:
                                    existing_tokens.append(f["file_token"])
                        if notice_type in ("设备变更", "变更通告"):
                            existing_resp_time = fields.get("过程更新时间", "")
                        else:
                            existing_resp_time = fields.get("进展更新时间", "")

                    payload = (
                        copy.deepcopy(payload_base)
                        if payload_base
                        else NoticePayload(text=data_snapshot["text"])
                    )
                    payload.transfer_to_overhaul = transfer_to_overhaul
                    payload.text = data_snapshot["text"]
                    payload.level = _level or payload.level
                    payload.buildings = _buildings
                    payload.specialty = specialty
                    payload.event_source = event_source
                    payload.file_tokens = file_tokens if file_tokens else None
                    payload.extra_file_tokens = (
                        extra_file_tokens if extra_file_tokens else None
                    )
                    payload.existing_file_tokens = existing_tokens
                    payload.existing_extra_file_tokens = existing_extra_tokens
                    payload.response_time = response_time
                    payload.existing_response_time = existing_resp_time
                    payload.recover = recover_selected

                    # service_registry 内部已实现全局锁，此处直接调用
                    s, r = update_bitable_record_by_payload(
                        record_id,
                        notice_type,
                        payload,
                    )

                if s:
                    self._post_request_finished("归档", True, r, record_id)
                else:
                    self._post_request_finished("归档", False, r, record_id)

        self._enqueue_upload(record_id, task)
        self._mark_upload_queued(record_id)
        # 截图确认后无需等待上传完成即可继续处理新内容
        if self.current_screenshot_record_id == record_id:
            self.current_screenshot_record_id = None
            self.current_screenshot_action_type = None
        self._try_process_deferred_events()

    def _do_move_to_history(self, item, data_dict, track_rollback=False):
        self._set_last_ui_op(
            "_do_move_to_history", record_id=(data_dict or {}).get("record_id")
        )
        if track_rollback and isinstance(data_dict, dict):
            record_id = data_dict.get("record_id")
            if record_id and record_id not in self.pending_end_rollback_by_record_id:
                try:
                    snapshot = copy.deepcopy(data_dict)
                except Exception:
                    snapshot = dict(data_dict)
                self.pending_end_rollback_by_record_id[record_id] = {
                    "data": snapshot,
                }
        if item and not self._is_valid_list_item(item):
            item = None
        list_widget = item.listWidget() if item else None
        if not list_widget:
            list_widget, item = self._find_active_item_by_record_id(
                data_dict["record_id"]
            )
        self._clear_upload_queue(data_dict.get("record_id"))
        self._today_in_progress_pending_record_ids.discard(
            str(data_dict.get("record_id") or "").strip()
        )
        self._today_in_progress_synced_record_ids.discard(
            str(data_dict.get("record_id") or "").strip()
        )
        self.pending_new_by_record_id.pop(data_dict.get("record_id"), None)
        self.pending_replace_by_record_id.pop(data_dict.get("record_id"), None)
        self._cleanup_payload_for_data(data_dict)
        if list_widget and item:
            row = list_widget.row(item)
            if row != -1:
                list_widget.takeItem(row)
        self.save_to_history_file(data_dict)
        self.save_active_cache()

    def _finish_apply_replace(self, record_id):
        pending = self.pending_replace_by_record_id.pop(record_id, None)
        if not pending:
            return
        new_data = pending.get("new_data")
        if new_data:
            list_widget, item_ref = self._find_active_item_by_record_id(record_id)
            if not item_ref:
                return
            if not self._is_valid_list_item(item_ref):
                return
            old_data = item_ref.data(Qt.ItemDataRole.UserRole)
            old_buildings = self._normalize_buildings_value(
                (old_data or {}).get("buildings")
            )
            new_buildings = self._normalize_buildings_value(new_data.get("buildings"))
            cache_buildings = []
            cache_identity_source = new_data if isinstance(new_data, dict) else {}
            record_id_key = self._get_cache_identity(cache_identity_source)
            if self.cache_store and record_id_key:
                cache_fields = self.cache_store.get_record_fields(
                    record_id=record_id_key,
                    fields=["buildings"],
                )
                if "buildings" in cache_fields:
                    cache_buildings = self._normalize_buildings_value(
                        cache_fields.get("buildings")
                    )
            detected_buildings = self._infer_buildings_from_text(
                new_data.get("text", "")
            )
            if cache_buildings:
                new_data["buildings"] = cache_buildings
            elif new_buildings:
                new_data["buildings"] = new_buildings
            elif detected_buildings:
                new_data["buildings"] = detected_buildings
            elif old_buildings:
                new_data["buildings"] = old_buildings
            else:
                new_data["buildings"] = []
            # 上传并替换完成后，清除强制上传标记
            if "need_upload_first" in new_data:
                new_data.pop("need_upload_first", None)
            # 仍然沿用同一 record_id，避免后续操作指向新记录
            if old_data and "record_id" in old_data:
                new_data["record_id"] = old_data["record_id"]
                if "_is_placeholder_record" in old_data:
                    new_data["_is_placeholder_record"] = old_data[
                        "_is_placeholder_record"
                    ]

            entry = self._build_clipboard_entry(new_data.get("text", "") or "")
            if not entry:
                entry = {"content": new_data.get("text", "")}
            self._ensure_payload_for_data(new_data, entry=entry)

            new_data["_pending_upload_hash"] = None
            new_data["_has_unuploaded_changes"] = True
            new_data["_upload_in_progress"] = False
            committed = self._commit_active_record(
                new_data,
                refresh_detail=True,
                rebuild_widget=False,
                list_widget=list_widget,
                item=item_ref,
            )
            self._rebuild_active_item_widget(
                list_widget,
                item_ref,
                committed or new_data,
                force_status=None,
                upload_in_progress=False,
                pending_upload_hash=None,
                has_unuploaded_changes=True,
            )
            self.save_active_cache()
        self.current_screenshot_record_id = None  # 当前正在进行截图操作的Record ID

    def _rollback_apply_replace(self, record_id):
        pending = self.pending_replace_by_record_id.pop(record_id, None)
        if not pending:
            return
        old_data = pending.get("old_data")
        if not old_data:
            return
        list_widget, item_ref = self._find_active_item_by_record_id(record_id)
        if not item_ref or not self._is_valid_list_item(item_ref):
            return
        entry = self._build_clipboard_entry(old_data.get("text", "") or "")
        if not entry:
            entry = {"content": old_data.get("text", "")}
        self._ensure_payload_for_data(old_data, entry=entry)
        old_data["_pending_upload_hash"] = None
        old_data["_has_unuploaded_changes"] = True
        old_data["_upload_in_progress"] = False
        committed = self._commit_active_record(
            old_data,
            refresh_detail=True,
            rebuild_widget=False,
            list_widget=list_widget,
            item=item_ref,
        )
        self._rebuild_active_item_widget(
            list_widget,
            item_ref,
            committed or old_data,
            force_status=None,
            upload_in_progress=False,
            pending_upload_hash=None,
            has_unuploaded_changes=True,
        )
        self.save_active_cache()

    def _remove_history_record(self, record_id):
        if not record_id or not os.path.exists(HISTORY_FILE):
            return
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                history_data = json.load(f)
        except Exception:
            return
        if not isinstance(history_data, list):
            return
        updated = False
        new_history = []
        removed = False
        for item in history_data:
            if (
                not removed
                and isinstance(item, dict)
                and item.get("record_id") == record_id
            ):
                removed = True
                updated = True
                continue
            new_history.append(item)
        if not updated:
            return
        try:
            with open(HISTORY_FILE, "w", encoding="utf-8") as f:
                json.dump(new_history, f, ensure_ascii=False, indent=2)
        except Exception:
            return
        try:
            self.reload_history_view()
        except Exception:
            pass

    def _rollback_end(self, record_id):
        pending = self.pending_end_rollback_by_record_id.pop(record_id, None)
        if not pending:
            return
        data = pending.get("data")
        if not isinstance(data, dict):
            return
        self._remove_history_record(record_id)
        list_widget, item_ref = self._find_active_item_by_record_id(record_id)
        if item_ref and not self._is_valid_list_item(item_ref):
            item_ref = None
            list_widget = None
        if not item_ref:
            item_ref, _ = self.add_active_item(data, insert_top=True, skip_cache=True)
            list_widget = item_ref.listWidget() if item_ref else None
        if item_ref and list_widget:
            data["_pending_upload_hash"] = None
            data["_has_unuploaded_changes"] = True
            data["_upload_in_progress"] = False
            item_ref.setData(Qt.ItemDataRole.UserRole, data)
            self._rebuild_active_item_widget(
                list_widget,
                item_ref,
                data,
                force_status="end",
                upload_in_progress=False,
                pending_upload_hash=None,
                has_unuploaded_changes=True,
            )
        self.pending_action_record_ids.discard(record_id)
        self.pending_action_types.pop(record_id, None)
        self.save_active_cache()

    def _replace_record_id_everywhere(
        self, old_record_id: str, new_record_id: str
    ) -> bool:
        old_id = str(old_record_id or "").strip()
        new_id = str(new_record_id or "").strip()
        if not old_id or not new_id or old_id == new_id:
            return False

        changed = False
        updated_items = []
        for list_widget, item in self._iter_active_items():
            try:
                if not self._is_valid_list_item(item):
                    continue
                data = item.data(Qt.ItemDataRole.UserRole)
                if not isinstance(data, dict):
                    continue
                if str(data.get("record_id") or "").strip() != old_id:
                    continue
                data = dict(data)
                data["record_id"] = new_id
                data["_is_placeholder_record"] = False
                if data.get("payload_key") == old_id:
                    data["payload_key"] = new_id
                item.setData(Qt.ItemDataRole.UserRole, data)
                updated_items.append((list_widget, item, data))
                changed = True
            except Exception:
                continue

        def _move_key(mapping: dict):
            nonlocal changed
            if not isinstance(mapping, dict) or old_id not in mapping:
                return
            old_value = mapping.pop(old_id)
            if new_id not in mapping:
                mapping[new_id] = old_value
            else:
                new_value = mapping.get(new_id)
                if isinstance(new_value, dict) and isinstance(old_value, dict):
                    merged = dict(old_value)
                    merged.update(new_value)
                    mapping[new_id] = merged
            changed = True

        for pending_map in (
            self.pending_replace_by_record_id,
            self.pending_upload_rollback_by_record_id,
            self.pending_end_rollback_by_record_id,
            self.pending_new_by_record_id,
            self.pending_update_after_upload,
            self.pending_action_types,
        ):
            _move_key(pending_map)

        if old_id in self.pending_action_record_ids:
            self.pending_action_record_ids.discard(old_id)
            self.pending_action_record_ids.add(new_id)
            changed = True

        if old_id in self._today_in_progress_pending_record_ids:
            self._today_in_progress_pending_record_ids.discard(old_id)
            self._today_in_progress_pending_record_ids.add(new_id)
            changed = True
        if old_id in self._today_in_progress_synced_record_ids:
            self._today_in_progress_synced_record_ids.discard(old_id)
            self._today_in_progress_synced_record_ids.add(new_id)
            changed = True
        if old_id in self._record_binding_validation_pending_ids:
            self._record_binding_validation_pending_ids.discard(old_id)
            self._record_binding_validation_pending_ids.add(new_id)
            changed = True
        if old_id in self._record_binding_validated_ids:
            self._record_binding_validated_ids.discard(old_id)
            self._record_binding_validated_ids.add(new_id)
            changed = True

        if self.current_screenshot_record_id == old_id:
            self.current_screenshot_record_id = new_id
            changed = True

        for pending_data in self._pending_force_uploads:
            if (
                isinstance(pending_data, dict)
                and pending_data.get("record_id") == old_id
            ):
                pending_data["record_id"] = new_id
                changed = True

        self._alias_upload_key(old_id, new_id)
        self._alias_payload_key(old_id, new_id)
        if getattr(self, "cache_store", None):
            self.cache_store.rename_record_id(old_id, new_id)

        if updated_items:
            if self._should_defer_ui_refresh():
                self._mark_cache_refresh_needed()
            else:
                for list_widget, item, data in updated_items:
                    self._rebuild_active_item_widget(
                        list_widget,
                        item,
                        data,
                        force_status=None,
                        upload_in_progress=data.get("_upload_in_progress"),
                        pending_upload_hash=data.get("_pending_upload_hash"),
                        has_unuploaded_changes=data.get("_has_unuploaded_changes"),
                    )
            try:
                if self._detail_dialog_matches_record(old_id):
                    self.detail_dialog.update_content(
                        updated_items[0][2],
                        new_id,
                        editable=self._is_active_view(),
                        active_item_id=updated_items[0][2].get("active_item_id", ""),
                    )
            except Exception:
                pass
        return changed

    def on_request_finished(self, name, success, msg, record_id=None):
        self._set_last_ui_op(
            "on_request_finished", name=name, success=success, record_id=record_id
        )

        def _apply_updates():
            try:
                # 恢复主界面显示（截图开始时被隐藏）
                self.show()

                if name == "结束" and not success and record_id:
                    self._rollback_end(record_id)
                    self.show_message(f"「{name}」失败\n{msg}")
                    self._try_process_deferred_events()
                    return

                # 恢复按钮状态
                self.restore_button_state(success, name, record_id)

                if success:
                    if name in ["上传", "更新", "归档"]:
                        try:
                            self._clipboard_cooldown_until = time.monotonic() + 0.5
                        except Exception:
                            self._clipboard_cooldown_until = 0.0

                    # 上传/归档成功后，统一回填真实 record_id（全链路迁移）
                    real_record_id = None
                    if name in ["上传", "归档"] and record_id:
                        real_record_id = str(msg or "").strip()
                        if real_record_id:
                            changed = self._replace_record_id_everywhere(
                                record_id, real_record_id
                            )
                            if changed:
                                log_info(
                                    f"UI更新: 已保存 record_id={real_record_id} (原={record_id})"
                                )
                                self.save_active_cache()
                            if self.pending_update_after_upload:
                                self._schedule_pending_update_after_upload()

                    # 成功触发"更新"动作后，重置计时器（触发下一阶段倒计时）
                    if name == "更新" and record_id:
                        list_widget, item = self._find_active_item_by_record_id(
                            record_id
                        )
                        if item and not self._is_valid_list_item(item):
                            item = None
                            list_widget = None
                        if item:
                            w = self._safe_item_widget(list_widget, item)
                            if w:
                                w.reset_timer(
                                    action=name,
                                    data_dict=item.data(Qt.ItemDataRole.UserRole),
                                )
                                updated_data = (
                                    dict(w.data)
                                    if isinstance(getattr(w, "data", None), dict)
                                    else None
                                )
                                if updated_data:
                                    try:
                                        item.setData(Qt.ItemDataRole.UserRole, updated_data)
                                    except Exception:
                                        pass
                                    timer_patch = self._extract_event_timer_state_patch(
                                        updated_data
                                    )
                                    if timer_patch and getattr(self, "cache_store", None):
                                        try:
                                            self.cache_store.patch_record_fields(
                                                updated_data.get("record_id")
                                                or record_id,
                                                timer_patch,
                                            )
                                        except Exception:
                                            pass

                    # 检查是否有pending的新内容需要处理（强制上传完旧内容后）
                    if name in ["上传", "更新"] and record_id:
                        pending = self.pending_new_by_record_id.pop(record_id, None)
                        if not pending and real_record_id:
                            pending = self.pending_new_by_record_id.pop(
                                real_record_id, None
                            )
                        if pending:
                            new_content = pending.get("content")
                            new_status = pending.get("status")

                            # 直接替换新内容（无需确认）
                            list_widget, item_ref = self._find_active_item_by_record_id(
                                real_record_id or record_id
                            )
                            if item_ref and not self._is_valid_list_item(item_ref):
                                item_ref = None
                                list_widget = None
                            if item_ref and list_widget:
                                old_data = item_ref.data(Qt.ItemDataRole.UserRole)
                                if new_status:
                                    old_data = dict(old_data) if old_data else {}
                                    old_data["_has_unuploaded_changes"] = True
                                    old_data["_pending_upload_hash"] = None
                                    old_data["_upload_in_progress"] = False
                                self._auto_replace_content(
                                    new_content, old_data, item_ref
                                )
                    if name == "归档" and record_id:
                        target_id = real_record_id or record_id
                        self._finish_apply_replace(target_id)
                else:
                    if name in ["上传", "更新"] and record_id:
                        self.pending_new_by_record_id.pop(record_id, None)
                        self.pending_update_after_upload.pop(record_id, None)
                        alias = self._payload_alias.get(record_id)
                        if alias:
                            self.pending_update_after_upload.pop(alias, None)
                    if name == "归档" and record_id:
                        self._rollback_apply_replace(record_id)
                    self._emit_system_alert(
                        event_code="upload.action.failed",
                        title=f"{name}失败",
                        detail=str(msg or ""),
                        dedup_key=f"{name}:{record_id or ''}",
                        extra={
                            "action": name,
                            "record_id": str(record_id or ""),
                        },
                    )
                    self.show_message(f"「{name}」失败\n{msg}")
                self._try_process_deferred_events()
            except Exception as exc:
                log_error(f"on_request_finished异常: {exc}")

        self._enqueue_ui_mutation("request_finished", _apply_updates)

    def _on_manual_add_accepted(self):
        if not self.add_dialog:
            return
        content = self.add_dialog.result_text
        record_id = getattr(self.add_dialog, "result_record_id", "") or ""
        status = getattr(self.add_dialog, "result_status", "")
        if not content:
            return
        info = extract_event_info(content)
        if not info:
            return
        status = status or info.get("status", "")
        cleaned_content = info.get("content") or content
        if status == "更新":
            if record_id:
                self._handle_manual_update(content, record_id, info)
                return

            target_list, target_item = self._find_active_item_by_content_or_title(
                cleaned_content,
                info.get("title", ""),
                info.get("notice_type"),
                info.get("unique_key", ""),
            )
            if target_item and not self._is_valid_list_item(target_item):
                target_item = None
            if target_item:
                target_data = target_item.data(Qt.ItemDataRole.UserRole) or {}
                if self._is_routing_conflicted(target_data):
                    self.show_message(self._routing_error_text(target_data))
                    return
                if self._is_event_notice(info.get("notice_type")):
                    self._pin_item_to_top(target_list, target_item)

                old_data = target_data
                old_text = (old_data.get("text") or "").translate(WHITESPACE_TRANSLATOR)
                new_text = cleaned_content.translate(WHITESPACE_TRANSLATOR)
                widget = self._safe_item_widget(target_list, target_item)
                if old_text == new_text:
                    self.show_message("该事件已存在于列表中！")
                    self._maybe_flash(widget, target_list, target_item)
                    self._maybe_speak("该事件已存在")
                    return

                if old_data:
                    self._rebuild_active_item_widget(
                        target_list,
                        target_item,
                        old_data,
                        force_status="update",
                        upload_in_progress=old_data.get("_upload_in_progress"),
                        pending_upload_hash=old_data.get("_pending_upload_hash"),
                        has_unuploaded_changes=old_data.get("_has_unuploaded_changes"),
                    )

                record_id = old_data.get("record_id") if old_data else None
                is_processing = (
                    bool(old_data.get("_upload_in_progress")) if old_data else False
                )
                if record_id and self._has_pending_upload(record_id):
                    is_processing = True
                if is_processing:
                    self._queue_pending_content(record_id, cleaned_content, status)
                    self.show_message("该条目正在上传，完成后将自动替换")
                    self._maybe_speak("正在上传")
                    return

                if old_data:
                    old_data.pop("need_upload_first", None)
                    target_item.setData(Qt.ItemDataRole.UserRole, old_data)

                self._auto_replace_content(
                    cleaned_content,
                    old_data,
                    target_item,
                    False,
                )
                return

            hint = "未在列表中找到对应条目，请填写记录ID后再更新。"
            self.show_message(hint)
            self._maybe_speak("无法更新，未找到对应事件")
            self._reopen_manual_add_with_hint(content, hint)
            return

        list_widget, item = self._find_active_item_by_content_or_title(
            cleaned_content,
            info.get("title", ""),
            info.get("notice_type"),
            info.get("unique_key", ""),
        )
        if item and not self._is_valid_list_item(item):
            item = None
        if item:
            existing_data = item.data(Qt.ItemDataRole.UserRole) or {}
            if self._is_routing_conflicted(existing_data):
                self.show_message(self._routing_error_text(existing_data))
                return
            self._auto_replace_content(cleaned_content, existing_data, item)
            return
        self._process_event(
            info["content"],
            info["status"],
            info.get("notice_type"),
        )


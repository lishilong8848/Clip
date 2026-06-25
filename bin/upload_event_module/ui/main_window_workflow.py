# -*- coding: utf-8 -*-
import copy
import threading
import time
import hashlib
import uuid
import re
from PyQt6.QtCore import QTimer, Qt
from PyQt6.QtGui import QStandardItem, QStandardItemModel
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListView,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
)

from ..logger import log_info, log_error, log_warning
from ..services.handlers import NoticePayload, get_notice_handler
from ..core.parser import extract_event_info
from .styles import get_stylesheet

class MainWindowWorkflowMixin:
    _INLINE_IMAGE_COMMAND_FIELDS = {"bytes_b64", "screenshot_bytes_b64"}

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

    @staticmethod
    def _upload_qt_notice_attachment(controller, image_bytes, file_name: str) -> dict:
        if not image_bytes:
            raise RuntimeError("图片内容为空。")
        if not hasattr(controller, "upload_notice_attachment"):
            raise RuntimeError("本机后端不支持图片附件暂存。")
        result = controller.upload_notice_attachment(
            bytes(image_bytes),
            file_name=str(file_name or "notice_image.png"),
            mime_type="image/png",
        )
        if not isinstance(result, dict) or not str(result.get("upload_id") or "").strip():
            raise RuntimeError("本机后端未返回图片 upload_id。")
        return result

    @classmethod
    def _strip_inline_image_command_fields(cls, value):
        """Remove legacy inline image fields before posting Qt commands.

        Images must be passed through the backend attachment store as upload_id.
        Keeping old base64 fields inside data_dict/extra_images makes the
        FastAPI request validator reject the command with HTTP 400 before the
        business handler can return a user-readable failure.
        """
        if isinstance(value, dict):
            cleaned = {}
            for key, child in value.items():
                if str(key or "") in cls._INLINE_IMAGE_COMMAND_FIELDS:
                    continue
                cleaned[key] = cls._strip_inline_image_command_fields(child)
            return cleaned
        if isinstance(value, list):
            return [cls._strip_inline_image_command_fields(item) for item in value]
        return value

    @classmethod
    def _qt_attachment_ref_payload(cls, item: dict) -> dict:
        item = item if isinstance(item, dict) else {}
        clean: dict = {}
        for key in ("upload_id", "file_token", "token", "file_name", "mime_type", "size"):
            if key not in item:
                continue
            value = item.get(key)
            if value in (None, ""):
                continue
            clean[key] = value
        return cls._strip_inline_image_command_fields(clean)

    @staticmethod
    def _candidate_record_id(candidate: dict) -> str:
        candidate = candidate if isinstance(candidate, dict) else {}
        return str(
            candidate.get("target_record_id")
            or candidate.get("record_id")
            or ""
        ).strip()

    @staticmethod
    def _coerce_upload_action_for_existing_target(
        data_dict: dict | None,
        action_type: str,
    ) -> str:
        action = str(action_type or "").strip()
        if action != "upload" or not isinstance(data_dict, dict):
            return action
        target_record_id = str(data_dict.get("target_record_id") or "").strip()
        if target_record_id and not bool(data_dict.get("_is_placeholder_record", True)):
            data_dict["record_id"] = target_record_id
            data_dict["target_record_id"] = target_record_id
            return "update"
        return action

    @staticmethod
    def _candidate_summary(candidate: dict, index: int) -> str:
        candidate = candidate if isinstance(candidate, dict) else {}
        title = str(candidate.get("title") or "未命名通告").strip()
        building = str(candidate.get("building") or "").strip()
        status = str(candidate.get("status") or "").strip()
        start_time = str(candidate.get("start_time") or "").strip()
        end_time = str(candidate.get("end_time") or "").strip()
        score = str(candidate.get("match_score") or "").strip()
        reason = str(candidate.get("match_reason") or "").strip()
        parts = [f"{index}. {title}"]
        meta = " / ".join(
            item
            for item in (
                building,
                f"状态 {status}" if status else "",
                f"{start_time} ~ {end_time}" if start_time or end_time else "",
                f"匹配 {score}" if score else "",
            )
            if item
        )
        if meta:
            parts.append(meta)
        if reason:
            parts.append(reason)
        return "\n".join(parts)

    @staticmethod
    def _candidate_detail_text(candidate: dict) -> str:
        candidate = candidate if isinstance(candidate, dict) else {}
        lines = [
            f"标题：{candidate.get('title') or '-'}",
            f"楼栋：{candidate.get('building') or '-'}",
            f"状态：{candidate.get('status') or '-'}",
            f"时间：{candidate.get('start_time') or '-'} ~ {candidate.get('end_time') or '-'}",
            f"匹配分：{candidate.get('match_score') or '-'}",
            f"匹配原因：{candidate.get('match_reason') or '-'}",
        ]
        field_items = candidate.get("field_items")
        if isinstance(field_items, list) and field_items:
            lines.append("")
            lines.append("字段信息：")
            for item in field_items[:30]:
                if not isinstance(item, dict):
                    continue
                label = str(item.get("label") or "").strip()
                value = str(item.get("value") or "").strip()
                if label and value:
                    lines.append(f"{label}：{value}")
        return "\n".join(lines)

    @staticmethod
    def _candidate_search_text(candidate: dict) -> str:
        candidate = candidate if isinstance(candidate, dict) else {}
        parts = [
            candidate.get("title"),
            candidate.get("building"),
            candidate.get("status"),
            candidate.get("match_reason"),
            candidate.get("start_time"),
            candidate.get("end_time"),
        ]
        field_items = candidate.get("field_items")
        if isinstance(field_items, list):
            for item in field_items[:30]:
                if not isinstance(item, dict):
                    continue
                parts.append(item.get("label"))
                parts.append(item.get("value"))
        return "\n".join(str(part or "").lower() for part in parts)

    @staticmethod
    def _backend_upload_action_name(action_type: str, result: dict | None = None) -> str:
        if isinstance(result, dict):
            name = str(result.get("name") or "").strip()
            if name:
                return name
        return (
            "归档"
            if action_type == "upload_replace"
            else "结束"
            if action_type == "end"
            else "更新"
            if action_type == "update"
            else "上传"
        )

    @staticmethod
    def _mark_notice_content_dirty(data: dict | None) -> dict:
        if not isinstance(data, dict):
            data = {}
        data["_has_unuploaded_changes"] = True
        data["_pending_upload_hash"] = None
        data["_upload_in_progress"] = False
        data.pop("_upload_pending_dialog", None)
        data.pop("_upload_started_monotonic", None)
        data.pop("_last_upload_error", None)
        return data

    def _target_candidate_dialog_stylesheet(self) -> str:
        theme = str(getattr(self, "current_theme", "dark") or "dark")
        if theme == "light":
            return """
            QLabel#TargetCandidateHint {
                color: #6C757D;
                margin: 8px 0;
            }
            QListView#TargetCandidateList {
                background-color: #FAFAFA;
                border: 1px solid #E0E0E0;
                border-radius: 8px;
                padding: 4px;
            }
            QListView#TargetCandidateList::item {
                min-height: 56px;
                padding: 8px 10px;
                margin: 4px;
            }
            QTextEdit#TargetCandidateDetail {
                background-color: #FAFAFA;
                border: 1px solid #E0E0E0;
                border-radius: 8px;
                color: #212529;
            }
            QLineEdit#TargetCandidateSearch {
                background-color: #FFFFFF;
                border: 1px solid #D7DEE8;
                border-radius: 8px;
                color: #212529;
                padding: 8px 10px;
            }
            """
        return """
        QLabel#TargetCandidateHint {
            color: #9CA3AF;
            margin: 8px 0;
        }
        QListView#TargetCandidateList {
            background-color: #2A2A3C;
            border: 1px solid #4F4F7A;
            border-radius: 8px;
            padding: 4px;
        }
        QListView#TargetCandidateList::item {
            min-height: 56px;
            padding: 8px 10px;
            margin: 4px;
        }
        QTextEdit#TargetCandidateDetail {
            background-color: #2A2A3C;
            border: 1px solid #4F4F7A;
            border-radius: 8px;
            color: #E5E7EB;
        }
        QLineEdit#TargetCandidateSearch {
            background-color: #202033;
            border: 1px solid #4F4F7A;
            border-radius: 8px;
            color: #E5E7EB;
            padding: 8px 10px;
        }
        """

    def _prompt_target_record_candidate(self, result: dict) -> dict | None:
        candidates = [
            item
            for item in (result.get("target_candidates") or [])
            if isinstance(item, dict) and self._candidate_record_id(item)
        ]
        if not candidates:
            return None
        dialog = QDialog(self)
        dialog.setWindowTitle("选择对应的多维记录")
        dialog.setObjectName("ScreenshotWindow")
        dialog.setWindowFlags(
            Qt.WindowType.FramelessWindowHint
            | Qt.WindowType.WindowStaysOnTopHint
            | Qt.WindowType.Tool
        )
        dialog.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        dialog.resize(760, 560)
        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(5, 5, 5, 5)
        container = QFrame(dialog)
        container.setObjectName("ScreenshotWindow")
        inner_layout = QVBoxLayout(container)

        top_bar = QHBoxLayout()
        title = QLabel("选择多维记录")
        title.setObjectName("TitleLabel")
        close_btn = QPushButton("×", dialog)
        close_btn.setObjectName("CloseBtn")
        close_btn.clicked.connect(dialog.reject)
        top_bar.addWidget(title)
        top_bar.addStretch()
        top_bar.addWidget(close_btn)

        base_hint = str(result.get("message") or "请选择最匹配的一条记录后继续上传。")
        hint = QLabel(base_hint)
        hint.setObjectName("TargetCandidateHint")
        hint.setWordWrap(True)
        search = QLineEdit(dialog)
        search.setObjectName("TargetCandidateSearch")
        search.setPlaceholderText("搜索标题、楼栋、时间、字段内容")
        list_view = QListView(dialog)
        list_view.setObjectName("TargetCandidateList")
        list_view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        list_view.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        list_view.setUniformItemSizes(True)
        candidate_model = QStandardItemModel(list_view)
        list_view.setModel(candidate_model)
        detail = QTextEdit(dialog)
        detail.setObjectName("TargetCandidateDetail")
        detail.setReadOnly(True)
        detail.setMinimumHeight(180)
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok
            | QDialogButtonBox.StandardButton.Cancel,
            parent=dialog,
        )
        ok_button = buttons.button(QDialogButtonBox.StandardButton.Ok)
        cancel_button = buttons.button(QDialogButtonBox.StandardButton.Cancel)
        if ok_button:
            ok_button.setText("使用选中记录继续")
            ok_button.setObjectName("ConfirmBtn")
        if cancel_button:
            cancel_button.setText("取消")
            cancel_button.setObjectName("DiffCancelBtn")

        max_visible_candidates = 80
        candidate_search_index = [
            (candidate, self._candidate_search_text(candidate))
            for candidate in candidates
        ]

        def current_candidate() -> dict | None:
            current = list_view.currentIndex()
            if not current.isValid():
                return None
            item = candidate_model.itemFromIndex(current)
            if item is None:
                return None
            candidate = item.data(Qt.ItemDataRole.UserRole)
            return candidate if isinstance(candidate, dict) else None

        def update_detail():
            candidate = current_candidate()
            if candidate is None:
                detail.setPlainText("没有匹配的候选记录。请调整搜索条件。")
                if ok_button:
                    ok_button.setEnabled(False)
                return
            if ok_button:
                ok_button.setEnabled(True)
            detail.setPlainText(self._candidate_detail_text(candidate))

        def populate_candidates(filter_text: str = ""):
            query = str(filter_text or "").strip().lower()
            if query:
                matched = [
                    candidate
                    for candidate, search_text in candidate_search_index
                    if query in search_text
                ]
            else:
                matched = list(candidates)
            candidate_model.clear()
            visible = matched[:max_visible_candidates]
            for index, candidate in enumerate(visible, start=1):
                item = QStandardItem(self._candidate_summary(candidate, index))
                item.setData(Qt.ItemDataRole.UserRole, candidate)
                candidate_model.appendRow(item)
            suffix = ""
            if len(matched) > len(visible):
                suffix = f" 当前仅显示前 {len(visible)} 条，请搜索缩小范围。"
            hint.setText(
                f"{base_hint}\n共找到 {len(candidates)} 条候选，当前匹配 {len(matched)} 条。{suffix}"
            )
            if visible:
                first_index = candidate_model.index(0, 0)
                list_view.setCurrentIndex(first_index)
                update_detail()
            else:
                update_detail()

        list_view.selectionModel().currentChanged.connect(lambda *_args: update_detail())
        search.textChanged.connect(populate_candidates)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        inner_layout.addLayout(top_bar)
        inner_layout.addWidget(hint)
        inner_layout.addWidget(search)
        inner_layout.addWidget(list_view, 1)
        inner_layout.addWidget(detail)
        inner_layout.addWidget(buttons)
        layout.addWidget(container)
        dialog.setStyleSheet(
            get_stylesheet(getattr(self, "current_theme", "dark"))
            + self._target_candidate_dialog_stylesheet()
        )
        populate_candidates()
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return None
        return current_candidate()

    def _continue_backend_upload_with_selected_target(
        self,
        *,
        selected_data: dict,
        screenshot_bytes,
        extra_images: list,
        action_type: str,
        response_time: str,
        recover_selected: bool,
        robot_group_choice: str,
    ) -> None:
        target_record_id = str((selected_data or {}).get("record_id") or "").strip()
        if not target_record_id:
            self._post_request_finished(
                self._backend_upload_action_name(action_type),
                False,
                "所选目标记录缺少 record_id。",
                "",
            )
            return

        def backend_task():
            self._delegate_qt_notice_upload_to_backend(
                data_snapshot=dict(selected_data or {}),
                screenshot_bytes=screenshot_bytes,
                extra_images=list(extra_images or []),
                action_type=action_type,
                response_time=response_time,
                recover_selected=recover_selected,
                robot_group_choice=robot_group_choice,
                allow_target_selection=False,
            )

        self._dispatch_backend_notice_upload(target_record_id, backend_task)
        self.show_message("已选择对应多维记录，正在后台继续处理。")

    def _dispatch_backend_notice_upload(self, record_id: str, task) -> None:
        """Submit a Qt-originated notice command to the backend without a Qt-side queue."""
        record_id = str(record_id or "").strip()
        if not callable(task):
            return
        if getattr(self, "_closing", False):
            self._post_request_finished(
                "上传",
                False,
                "程序正在关闭，本次上传命令未提交。",
                record_id,
            )
            return

        def worker() -> None:
            try:
                task()
            except Exception as exc:
                log_error(f"Qt后端命令任务异常: {exc}")
                self._post_request_finished(
                    "上传",
                    False,
                    f"后端命令异常: {exc}",
                    record_id,
                )

        executor = getattr(self, "_qt_backend_command_executor", None)
        if executor is None:
            worker()
            return
        try:
            executor.submit(worker)
        except RuntimeError as exc:
            self._post_request_finished(
                "上传",
                False,
                f"后端命令提交失败: {exc}",
                record_id,
            )

    def _handle_backend_target_selection(
        self,
        *,
        result: dict,
        data_snapshot: dict,
        screenshot_bytes,
        extra_images: list,
        action_type: str,
        response_time: str,
        recover_selected: bool,
        robot_group_choice: str,
    ) -> bool:
        def select_and_continue() -> None:
            candidate = self._prompt_target_record_candidate(result)
            old_id = str((data_snapshot or {}).get("record_id") or "").strip()
            action_name = self._backend_upload_action_name(action_type, result)
            if not candidate:
                self._post_request_finished(
                    action_name,
                    False,
                    "已取消选择目标多维记录，本次未继续上传。",
                    old_id,
                )
                return
            target_record_id = self._candidate_record_id(candidate)
            if not target_record_id:
                self._post_request_finished(
                    action_name,
                    False,
                    "所选目标记录缺少 record_id。",
                    old_id,
                )
                return
            selected_data = dict(data_snapshot or {})
            selected_data["target_record_id"] = target_record_id
            selected_data["record_id"] = target_record_id
            selected_data["_is_placeholder_record"] = False
            if old_id and old_id != target_record_id and hasattr(
                self, "_replace_record_id_everywhere"
            ):
                self._replace_record_id_everywhere(old_id, target_record_id)
            if not self._apply_selected_target_record_to_active_item(
                old_record_id=old_id,
                target_record_id=target_record_id,
                selected_data=selected_data,
            ):
                self._update_active_item_data(target_record_id, selected_data)
            self._continue_backend_upload_with_selected_target(
                selected_data=selected_data,
                screenshot_bytes=screenshot_bytes,
                extra_images=extra_images,
                action_type=action_type,
                response_time=response_time,
                recover_selected=recover_selected,
                robot_group_choice=robot_group_choice,
            )

        enqueue = getattr(self, "_enqueue_ui_mutation", None)
        if callable(enqueue):
            enqueue("target_record_selection", select_and_continue)
        else:
            select_and_continue()
        return True

    def _delegate_qt_notice_upload_to_backend(
        self,
        *,
        data_snapshot: dict,
        screenshot_bytes,
        extra_images: list,
        action_type: str,
        response_time: str,
        recover_selected: bool,
        robot_group_choice: str,
        allow_target_selection: bool = True,
    ) -> bool:
        controller = getattr(self, "lan_template_portal_controller", None)
        if isinstance(data_snapshot, dict):
            data_snapshot = dict(data_snapshot)
            action_type = self._coerce_upload_action_for_existing_target(
                data_snapshot,
                action_type,
            )
        if controller is None or not hasattr(controller, "execute_qt_notice_upload"):
            self._post_request_finished(
                self._backend_upload_action_name(action_type),
                False,
                "本机后端未连接，Qt 不再直接执行多维写入。",
                str((data_snapshot or {}).get("record_id") or ""),
            )
            return True
        request_payload = {
            "action_type": str(action_type or "").strip(),
            "data_dict": self._strip_inline_image_command_fields(
                dict(data_snapshot or {})
            ),
            "response_time": str(response_time or ""),
            "recover_selected": bool(recover_selected),
            "robot_group_choice": str(robot_group_choice or "auto").strip() or "auto",
        }
        if screenshot_bytes:
            try:
                screenshot_ref = self._upload_qt_notice_attachment(
                    controller,
                    screenshot_bytes,
                    "notice_screenshot.png",
                )
            except Exception as exc:
                self._post_request_finished(
                    self._backend_upload_action_name(action_type),
                    False,
                    f"截图暂存失败：{exc}",
                    str((data_snapshot or {}).get("record_id") or ""),
                )
                return True
            request_payload["screenshot_upload_id"] = str(
                screenshot_ref.get("upload_id") or ""
            )
            request_payload["screenshot_file_name"] = str(
                screenshot_ref.get("file_name") or "notice_screenshot.png"
            )
        encoded_extra_images = []
        for index, item in enumerate(list(extra_images or []), start=1):
            if isinstance(item, dict):
                if item.get("upload_id") or item.get("file_token") or item.get("token"):
                    encoded_extra_images.append(self._qt_attachment_ref_payload(item))
                    continue
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                image_bytes = item[0]
                file_name = item[1] or f"extra_{index}.png"
            else:
                image_bytes = item
                file_name = f"extra_{index}.png"
            if not image_bytes:
                continue
            try:
                photo_ref = self._upload_qt_notice_attachment(
                    controller,
                    image_bytes,
                    str(file_name or f"extra_{index}.png"),
                )
            except Exception as exc:
                self._post_request_finished(
                    self._backend_upload_action_name(action_type),
                    False,
                    f"现场照片暂存失败：{exc}",
                    str((data_snapshot or {}).get("record_id") or ""),
                )
                return True
            encoded_extra_images.append(
                {
                    "upload_id": str(photo_ref.get("upload_id") or ""),
                    "file_name": str(
                        photo_ref.get("file_name") or file_name or f"extra_{index}.png"
                    ),
                    "mime_type": str(photo_ref.get("mime_type") or "image/png"),
                    "size": int(photo_ref.get("size") or 0),
                }
            )
        if encoded_extra_images:
            request_payload["extra_images"] = encoded_extra_images
        try:
            result = controller.execute_qt_notice_upload(request_payload)
        except Exception as exc:
            self._post_request_finished(
                self._backend_upload_action_name(action_type),
                False,
                f"本机后端执行失败：{exc}",
                str((data_snapshot or {}).get("record_id") or ""),
            )
            return True
        if not isinstance(result, dict):
            self._post_request_finished(
                self._backend_upload_action_name(action_type),
                False,
                "本机后端返回格式异常。",
                str((data_snapshot or {}).get("record_id") or ""),
            )
            return True
        name = str(result.get("name") or "").strip()
        success = bool(result.get("ok"))
        record_id = str(
            result.get("record_id") or (data_snapshot or {}).get("record_id") or ""
        )
        message = str(result.get("message") or "")
        if result.get("needs_target_selection"):
            if not allow_target_selection:
                self._post_request_finished(
                    self._backend_upload_action_name(action_type, result),
                    False,
                    message or "所选目标记录仍无法绑定，请重新选择后再试。",
                    record_id,
                )
                return True
            return self._handle_backend_target_selection(
                result=result,
                data_snapshot=data_snapshot,
                screenshot_bytes=screenshot_bytes,
                extra_images=encoded_extra_images,
                action_type=action_type,
                response_time=response_time,
                recover_selected=recover_selected,
                robot_group_choice=robot_group_choice,
            )
        real_record_id = str(result.get("real_record_id") or "").strip()
        if success and name in {"上传", "归档"} and real_record_id:
            message = real_record_id
        self._post_request_finished(
            name or self._backend_upload_action_name(action_type),
            success,
            message,
            record_id,
        )
        return True

    def on_screenshot_upload_confirmed(
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
        robot_group_choice="auto",
    ):
        session_id = str(getattr(self, "_current_screenshot_dialog_session_id", "") or "")
        if session_id:
            controller = getattr(self, "lan_template_portal_controller", None)
            if controller is not None and hasattr(controller, "submit_qt_dialog_result"):
                try:
                    controller.submit_qt_dialog_result(
                        session_id,
                        status="completed",
                        result_payload={
                            "record_id": str((data_dict or {}).get("record_id") or ""),
                            "active_item_id": str(
                                (data_dict or {}).get("active_item_id") or ""
                            ),
                            "action_type": str(action_type or ""),
                            "has_screenshot": bool(screenshot_bytes),
                            "extra_image_count": len(list(extra_images or [])),
                        },
                    )
                except Exception as exc:
                    log_warning(f"截图确认结果提交后端失败: {exc}")
        self._current_screenshot_dialog_session_id = ""
        return self.do_feishu_upload(
            data_dict,
            screenshot_bytes,
            action_type,
            response_time=response_time,
            buildings=buildings,
            extra_images=extra_images,
            specialty=specialty,
            change_level=change_level,
            event_level=event_level,
            event_source=event_source,
            recover_selected=recover_selected,
            robot_group_choice=robot_group_choice,
        )

    def _lan_portal_runtime_store(self):
        store = getattr(self, "_lan_portal_state_store", None)
        if store is not None:
            return store
        try:
            from lan_bitable_template_portal.state_store import LanPortalStateStore

            store = LanPortalStateStore()
            self._lan_portal_state_store = store
            return store
        except Exception:
            return None

    def _mark_qt_upload_runtime_queue(
        self,
        upload_key: str,
        status: str,
        *,
        payload: dict | None = None,
        error: str = "",
    ) -> None:
        upload_key = str(upload_key or "").strip()
        status = str(status or "").strip() or "queued"
        if not upload_key:
            return
        store = self._lan_portal_runtime_store()
        if not store:
            return
        try:
            if status == "queued":
                store.upsert_runtime_queue_item(
                    "qt_upload",
                    upload_key,
                    status="queued",
                    payload=dict(payload or {}),
                    error=str(error or ""),
                )
            else:
                store.mark_runtime_queue_item(
                    "qt_upload",
                    upload_key,
                    status,
                    error=str(error or ""),
                )
        except Exception:
            return

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

        try:
            active_snapshot = self._active_notice_store().data_snapshot()
        except Exception:
            active_snapshot = []
        for data in active_snapshot:
            add_from_data(data)

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

    def _init_runtime_payload_cleanup_timer(self) -> None:
        if getattr(self, "_runtime_payload_cleanup_timer", None) is not None:
            return
        self._runtime_payload_cleanup_timer = QTimer(self)
        self._runtime_payload_cleanup_timer.timeout.connect(
            self._run_runtime_payload_cleanup
        )
        self._runtime_payload_cleanup_timer.start(5 * 60 * 1000)

    def _run_runtime_payload_cleanup(self) -> None:
        if getattr(self, "_closing", False):
            return
        try:
            self._cleanup_runtime_payload_state()
        except Exception:
            pass

    def _has_any_upload_in_progress(self) -> bool:
        if getattr(self, "pending_action_record_ids", None):
            return True

        try:
            active_snapshot = self._active_notice_store().data_snapshot()
        except Exception:
            active_snapshot = []
        for data in active_snapshot:
            if isinstance(data, dict) and data.get("_upload_in_progress"):
                return True

        return False

    def _resolve_default_robot_group_level(self, notice_type: str, payload: NoticePayload) -> str:
        if not notice_type:
            return ""
        try:
            handler = get_notice_handler(notice_type)
            _, _, _, level = handler.build_robot_message(payload)
        except Exception:
            return ""
        return str(level or "").strip().upper()

    def _resolve_robot_group_choice_for_upload(
        self,
        notice_type: str,
        payload: NoticePayload,
        robot_group_choice: str = "auto",
    ):
        choice = str(robot_group_choice or "auto").strip().lower() or "auto"
        if notice_type == "变更通告":
            if choice == "i3":
                return "i3"
            if choice in {"i2", "skip"}:
                return "skip"
            return "auto"
        if choice in {"i2", "i3", "skip"}:
            return choice
        default_level = self._resolve_default_robot_group_level(notice_type, payload)
        if notice_type != "事件通告":
            return "auto"
        if default_level != "I2":
            return "auto"
        return self._prompt_i2_robot_group_choice(notice_type)

    @staticmethod
    def _normalize_change_upload_level(data_dict: dict | None, level: str = "") -> str:
        explicit_level = str(level or "").strip()
        if explicit_level:
            return explicit_level
        if isinstance(data_dict, dict):
            cached_level = str(data_dict.get("level") or "").strip()
            if cached_level:
                return cached_level
            text = str(data_dict.get("text") or "")
        else:
            text = ""
        info = extract_event_info(text) or {}
        raw_level = str(info.get("level") or "").strip()
        if not raw_level:
            match = re.search(
                r"【等级】(?P<value>.*?)(?=【[^】]+】|$)",
                text,
                re.S,
            )
            raw_level = str(match.group("value") if match else "").strip()
        normalized = raw_level.upper()
        if normalized in {"I3", "I2", "I1", "E0", "/"}:
            return normalized
        if "超低" in raw_level:
            return "I3"
        if "低" in raw_level:
            return "I3"
        if "中" in raw_level:
            return "I2"
        if "高" in raw_level:
            return "I1"
        return "I3"

    def _restore_screenshot_dialog_after_group_choice_cancel(self):
        self._set_delete_interaction_enabled(False)
        self._pause_clipboard_timer()
        dialog = getattr(self, "screenshot_dialog", None)
        if dialog and hasattr(dialog, "mark_submit_unaccepted"):
            try:
                dialog.mark_submit_unaccepted()
            except Exception:
                pass
        try:
            if not self.isVisible():
                self.show()
        except Exception:
            pass
        try:
            if dialog and not dialog.isVisible():
                self._position_screenshot_dialog()
        except Exception:
            pass
        try:
            if dialog:
                if not dialog.isVisible():
                    dialog.show()
                dialog.raise_()
                dialog.activateWindow()
        except Exception:
            pass

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
        max_deferred = max(1, int(getattr(self, "_deferred_events_max", 50) or 50))
        if len(self._deferred_events) > max_deferred:
            self._deferred_events.pop(0)
            log_warning(
                "截图期间延迟剪贴板事件超过上限，已丢弃最早一条: "
                f"max={max_deferred}"
            )

    def _try_process_deferred_events(self):
        if self._ui_update_in_progress:
            return
        if self.current_screenshot_record_id or self.screenshot_dialog.isVisible():
            return
        if not self._deferred_events:
            return
        pending = self._deferred_events.pop(0)
        content = pending.get("content")
        if content:
            self._submit_notice_text_to_backend_projection(
                content,
                source="deferred_event",
            )
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
            if str(payload.get("robot_group_choice") or "").strip().lower() in ("", "auto"):
                existing_choice = (
                    str(existing.get("robot_group_choice") or "").strip().lower()
                )
                if existing_choice in {"i2", "i3", "skip"}:
                    merged["robot_group_choice"] = existing_choice
            payload = merged
        self.pending_update_after_upload[record_id] = payload
        log_info(
            "PendingUpdate: queued "
            f"record_id={record_id} action={payload.get('action_type')} "
            f"has_screenshot={bool(payload.get('screenshot_bytes'))} "
            f"extra_images={len(payload.get('extra_images') or [])} "
            f"response_time={'Y' if payload.get('response_time') else 'N'} "
            f"robot_group_choice={payload.get('robot_group_choice') or 'auto'}"
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
            elif notice_type == "变更通告":
                dialog_level = self._normalize_change_upload_level(
                    payload_data if isinstance(payload_data, dict) else {},
                    str(request.get("change_level") or "").strip(),
                )
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
                self._normalize_change_upload_level(
                    payload_data if isinstance(payload_data, dict) else {},
                    resolved_level or dialog_level,
                )
                if notice_type == "变更通告"
                else ""
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
                robot_group_choice=str(
                    request.get("robot_group_choice") or "auto"
                ).strip()
                or "auto",
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
        controller = getattr(self, "lan_template_portal_controller", None)
        if controller is None or not hasattr(controller, "submit_qt_command"):
            return False, "本机后端未连接，无法校验记录ID。"
        try:
            result = self._submit_qt_command(
                "validate_record_id",
                {
                    "record_id": str(record_id or "").strip(),
                    "notice_type": str(notice_type or "").strip(),
                },
                timeout=15.0,
            )
        except Exception as exc:
            return False, f"后端校验记录ID失败：{exc}"
        if bool((result or {}).get("ok")):
            return True, ""
        return False, str((result or {}).get("message") or "未找到对应记录。")

    def _handle_manual_update(
        self,
        content: str,
        record_id: str,
        info=None,
        *,
        force_status: str | None = "update",
    ):
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
            "target_record_id": record_id,
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
            data = self._mark_notice_content_dirty(data)
            self._commit_active_record(
                data,
                refresh_detail=not self._should_defer_ui_refresh(),
                rebuild_widget=not self._should_defer_ui_refresh(),
                force_status=force_status,
                list_widget=list_widget,
                item=item,
            )
            if self._is_event_notice(notice_type):
                self._pin_item_to_top(list_widget, item)
            self.request_active_cache_save()
        else:
            data = self._mark_notice_content_dirty(data)
            added_item, added_widget = self.add_active_item(data)
            if added_item:
                log_info(
                    "ManualAdd: record_id not found in active list, added as new item "
                    f"record_id={record_id} notice_type={notice_type}"
                )
            else:
                log_warning(
                    "ManualAdd: failed to add new item for unmatched record_id "
                    f"record_id={record_id} notice_type={notice_type}"
                )

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
                    old_data = self._mark_notice_content_dirty(old_data)
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
                    self.request_active_cache_save()
                self._scroll_active_item_into_view(target_list, target_item)
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
                    old_data = self._mark_notice_content_dirty(old_data)

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
        payload.maintenance_cycle = (
            entry.get("maintenance_cycle") or payload.maintenance_cycle
        )

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
            if "maintenance_cycle" in data_dict:
                payload.maintenance_cycle = data_dict.get("maintenance_cycle")
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
            maintenance_cycle=data_dict.get("maintenance_cycle"),
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

        notice_type = info.get("notice_type", "")
        level = info.get("level")
        source = info.get("source", "")
        time_str = info.get("time_str", "")
        prefilled_buildings = self._infer_buildings_from_notice_text(content)

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
                    prefilled_buildings,
                )
                routed_data = self._mark_notice_content_dirty(routed_data)
                routed_data = self._ensure_active_item_identity(routed_data)
                if entry:
                    self._ensure_payload_for_data(routed_data, entry=entry)
                else:
                    self._ensure_payload_for_data(routed_data)
                committed = self._commit_active_record(
                    routed_data,
                    refresh_detail=not defer_ui,
                    rebuild_widget=not defer_ui,
                    list_widget=existing_list,
                    item=existing_item,
                )
                self.request_active_cache_save()
                if defer_ui:
                    self._mark_cache_refresh_needed()
                    return
                self._log_detail_preview_update(
                    committed or routed_data,
                    (committed or routed_data).get("record_id"),
                    reason="new_event_existing",
                )
                self.request_active_cache_save()
                return
            except Exception as exc:
                log_error(
                    "handle_new_event routed update failed: "
                    f"record_id={existing_data.get('record_id', '') if isinstance(existing_data, dict) else ''}, "
                    f"notice_type={notice_type}, error={exc}"
                )
                self._schedule_pending_cache_refresh()
                return

        placeholder_record_id = uuid.uuid4().hex
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
        data = self._mark_notice_content_dirty(data)
        if entry:
            self._ensure_payload_for_data(data, entry=entry)
        else:
            self._ensure_payload_for_data(data)

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
                if self._active_model_view_visible():
                    self._scroll_active_model_view_to_item(list_widget, item)
                else:
                    list_widget.setCurrentItem(item)
                    list_widget.scrollToItem(item)
                self._notify_new_event(widget)

        # 后台剪贴板/门户事件只更新列表数据，不主动把最小化或托盘中的主界面拉到前台。
        # 用户需要查看时可通过托盘或主窗口按钮打开，避免后台批量任务造成小窗口闪烁。

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
        self._scroll_active_item_into_view(list_widget, item_ref)

    def _scroll_active_item_into_view(self, list_widget, item_ref):
        if list_widget is None or item_ref is None:
            return
        try:
            if self._active_item_row(list_widget, item_ref) == -1:
                return
            if self._active_model_view_visible():
                self._scroll_active_model_view_to_item(list_widget, item_ref)
            else:
                list_widget.scrollToItem(item_ref)
                self._scroll_active_model_view_to_item(list_widget, item_ref)
                if hasattr(self, "_schedule_active_list_virtualization_refresh"):
                    self._schedule_active_list_virtualization_refresh(list_widget, 0)
        except Exception:
            return

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
        if isinstance(current_data, dict):
            current_target_id = str(
                current_data.get("target_record_id") or ""
            ).strip()
            current_record_id = str(current_data.get("record_id") or "").strip()
            current_is_placeholder = bool(
                current_data.get("_is_placeholder_record", True)
            )
            if current_target_id and not current_is_placeholder:
                new_data["target_record_id"] = current_target_id
                new_data["record_id"] = current_target_id
                new_data["_is_placeholder_record"] = False
            elif current_record_id and "record_id" not in new_data:
                new_data["record_id"] = current_record_id
                if "_is_placeholder_record" in current_data:
                    new_data["_is_placeholder_record"] = current_data[
                        "_is_placeholder_record"
                    ]
            for identity_key in ("active_item_id", "source_record_id", "payload_key"):
                if current_data.get(identity_key) and not new_data.get(identity_key):
                    new_data[identity_key] = current_data.get(identity_key)
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
        new_data = self._mark_notice_content_dirty(new_data)
        item_ref.setData(Qt.ItemDataRole.UserRole, new_data)
        committed = self._commit_active_record(
            new_data,
            refresh_detail=not defer_ui,
            rebuild_widget=False,
            list_widget=list_widget,
            item=item_ref,
        )
        self.request_active_cache_save()
        if defer_ui:
            self._mark_cache_refresh_needed()
            return
        force_status = None
        if current_data and current_data.get("_is_placeholder_record", True):
            action_type = self.pending_action_types.get(new_data.get("record_id"))
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
        self.request_active_cache_save()

    def move_to_history(self, data_dict):
        log_info(f"UI操作: 结束事件, Record ID: {data_dict['record_id']}")
        self._show_screenshot_dialog(data_dict, "end")

    def _submit_delete_active_item_to_backend(self, data_dict) -> tuple[bool, str, dict]:
        data_dict = dict(data_dict or {})
        controller = getattr(self, "lan_template_portal_controller", None)
        if controller is None or not hasattr(controller, "submit_qt_command"):
            return False, "本机后端未连接，Qt 不再直接执行多维删除。", {}
        try:
            result = self._submit_qt_command(
                "delete_active_item",
                {"data_dict": dict(data_dict)},
                timeout=30.0,
            )
        except Exception as exc:
            return False, f"本机后端删除失败：{exc}", {}
        if not bool((result or {}).get("ok")):
            return False, str((result or {}).get("message") or "多维记录删除失败。"), result if isinstance(result, dict) else {}
        return True, "", result if isinstance(result, dict) else {}

    def _submit_notice_undo_to_backend(self, undo_id: str) -> tuple[bool, str, dict]:
        undo_id = str(undo_id or "").strip()
        if not undo_id:
            return False, "没有可回退的删除记录。", {}
        controller = getattr(self, "lan_template_portal_controller", None)
        if controller is None or not hasattr(controller, "submit_qt_command"):
            return False, "本机后端未连接，无法执行回退。", {}
        try:
            result = self._submit_qt_command(
                "apply_notice_undo",
                {
                    "undo_id": undo_id,
                    "scope": "ALL",
                    "auth_open_id": "qt",
                    "auth_user_name": "Qt",
                },
                timeout=30.0,
            )
        except Exception as exc:
            return False, f"本机后端回退失败：{exc}", {}
        if not bool((result or {}).get("ok")):
            return False, str((result or {}).get("message") or "回退提交失败。"), result if isinstance(result, dict) else {}
        return True, "", result if isinstance(result, dict) else {}

    def _list_deleted_notice_undos_from_backend(self, *, days: int = 2) -> tuple[bool, str, list[dict]]:
        controller = getattr(self, "lan_template_portal_controller", None)
        if controller is None or not hasattr(controller, "submit_qt_command"):
            return False, "本机后端未连接，无法读取历史删除。", []
        try:
            result = self._submit_qt_command(
                "list_notice_undos",
                {
                    "scope": "ALL",
                    "action_type": "delete",
                    "since_seconds": max(1, int(days)) * 24 * 60 * 60,
                },
                timeout=15.0,
            )
        except Exception as exc:
            return False, f"读取历史删除失败：{exc}", []
        if not bool((result or {}).get("ok", True)):
            return False, str((result or {}).get("message") or "读取历史删除失败。"), []
        items = result.get("items")
        return True, "", list(items or []) if isinstance(items, list) else []

    @staticmethod
    def _format_undo_deleted_item(item: dict) -> str:
        created = float((item or {}).get("undo_created_at") or (item or {}).get("created_at") or 0)
        if created > 0:
            time_text = time.strftime("%m-%d %H:%M", time.localtime(created))
        else:
            time_text = "--"
        title = str((item or {}).get("title") or "未命名通告").strip()
        notice_type = str((item or {}).get("notice_type") or (item or {}).get("work_type") or "").strip()
        building = str((item or {}).get("building") or "").strip()
        meta = " · ".join(part for part in [notice_type, building, time_text] if part)
        return f"{title}\n{meta}" if meta else title

    def _show_deleted_notice_history(self):
        button = getattr(self, "deleted_history_btn", None)
        if button is not None:
            button.setEnabled(False)
            button.setText("读取中...")

        def _finish(success: bool, error: str = "", items: list[dict] | None = None) -> None:
            if button is not None:
                button.setEnabled(True)
                button.setText("历史删除")
            if not success:
                self.show_message(error or "读取历史删除失败。")
                return
            self._render_deleted_notice_history(items or [])

        def _worker() -> None:
            success, error, items = self._list_deleted_notice_undos_from_backend(days=2)
            enqueue = getattr(self, "_enqueue_ui_mutation", None)
            if callable(enqueue):
                enqueue(
                    "show_deleted_notice_history",
                    lambda s=success, e=error, rows=items: _finish(s, e, rows),
                )
            else:
                QTimer.singleShot(0, lambda s=success, e=error, rows=items: _finish(s, e, rows))

        threading.Thread(
            target=_worker,
            name="ClipFlowDeletedNoticeHistory",
            daemon=True,
        ).start()

    def _render_deleted_notice_history(self, items: list[dict]) -> None:
        self.title_label.setText("历史删除")
        self.stack.setCurrentWidget(self.deleted_history_container)
        model = getattr(self, "deleted_history_model", None)
        if model is None or not hasattr(model, "replace_records"):
            self.show_message("历史删除页面未初始化。")
            return
        rows = []
        for item in items:
            payload = dict(item or {})
            lines = self._format_undo_deleted_item(payload).splitlines()
            payload["_meta"] = lines[1] if len(lines) > 1 else ""
            rows.append(payload)
        model.replace_records(rows)

    def _handle_deleted_history_undo_request(self, item: dict) -> None:
        payload = dict(item or {})
        if payload.get("_empty"):
            return
        undo_id = str(payload.get("undo_id") or "").strip()
        title = str(payload.get("title") or "未命名通告").strip()
        self._undo_deleted_history_item(undo_id, title)

    def _undo_deleted_history_item(self, undo_id: str, title: str) -> None:
        undo_id = str(undo_id or "").strip()
        if not undo_id:
            self.show_message("该删除记录缺少回退标识。")
            return

        def _finish(success: bool, error: str = "") -> None:
            if success:
                self.show_message(f"已提交「{title}」的删除回退，稍后会恢复显示。")
                self._show_deleted_notice_history()
                return
            self.show_message(error or "回退提交失败。")

        def _worker() -> None:
            success, error, _ = self._submit_notice_undo_to_backend(undo_id)
            enqueue = getattr(self, "_enqueue_ui_mutation", None)
            if callable(enqueue):
                enqueue(
                    "undo_deleted_notice_history_result",
                    lambda s=success, e=error: _finish(s, e),
                )
            else:
                QTimer.singleShot(0, lambda s=success, e=error: _finish(s, e))

        threading.Thread(
            target=_worker,
            name="ClipFlowUndoDeletedNoticeHistory",
            daemon=True,
        ).start()

    def _remember_delete_undo(self, data_dict: dict, backend_result: dict | None) -> None:
        result = backend_result if isinstance(backend_result, dict) else {}
        undo_id = str(result.get("undo_id") or result.get("checkpoint_id") or "").strip()
        if not undo_id:
            return
        title = ""
        try:
            info = extract_event_info((data_dict or {}).get("text", "")) or {}
            title = str(info.get("title") or "").strip()
        except Exception:
            title = ""
        if not title:
            title = str((data_dict or {}).get("title") or (data_dict or {}).get("match_title") or "刚删除的通告")
        self.show_message(f"已删除「{title}」。如误删，可进入“历史删除”对单条通告回退。")

    def _delete_active_item_remote_and_local(self, data_dict) -> tuple[bool, str]:
        data_dict = dict(data_dict or {})
        record_id = str(data_dict.get("record_id") or "").strip()
        active_item_id = str(data_dict.get("active_item_id") or "").strip()
        success, error, result = self._submit_delete_active_item_to_backend(data_dict)
        if not success:
            return False, error
        self._remember_delete_undo(data_dict, result)
        self._clear_upload_queue(record_id)
        self._today_in_progress_pending_record_ids.discard(record_id)
        self._today_in_progress_synced_record_ids.discard(record_id)
        self.pending_new_by_record_id.pop(record_id, None)
        self.pending_replace_by_record_id.pop(record_id, None)
        self.pending_update_after_upload.pop(record_id, None)
        self.pending_action_record_ids.discard(record_id)
        self.pending_action_types.pop(record_id, None)
        if hasattr(self, "_pop_lan_portal_upload_job"):
            try:
                self._pop_lan_portal_upload_job(record_id=record_id, active_item_id=active_item_id)
            except Exception:
                pass
        list_widget, item = self._find_active_item_by_record_id(record_id)
        if (not item or not self._is_valid_list_item(item)) and active_item_id:
            list_widget, item = self._find_active_item_by_active_item_id(active_item_id)
        if item and self._is_valid_list_item(item):
            self._remove_active_item_widget_only(list_widget, item)
        if hasattr(self, "_delete_active_cache_record"):
            self._delete_active_cache_record(data_dict)
        self.request_active_cache_save()
        return True, ""

    def _delete_active_item(self, data_dict):
        """删除活动列表项，并同步删除对应多维记录。"""
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
        record_id = str((data_dict or {}).get("record_id") or "").strip()
        list_widget, item = self._find_active_item_by_record_id(record_id)
        widget = self._safe_item_widget(list_widget, item)

        def _finish(success: bool, error: str = "", result: dict | None = None) -> None:
            if not success:
                if widget and hasattr(widget, "cancel_delete_visual"):
                    try:
                        widget.cancel_delete_visual()
                    except Exception:
                        pass
                self.show_message(error or "删除失败。")
                return
            active_item_id = str((data_dict or {}).get("active_item_id") or "").strip()
            self._clear_upload_queue(record_id)
            self._today_in_progress_pending_record_ids.discard(record_id)
            self._today_in_progress_synced_record_ids.discard(record_id)
            self.pending_new_by_record_id.pop(record_id, None)
            self.pending_replace_by_record_id.pop(record_id, None)
            self.pending_update_after_upload.pop(record_id, None)
            self.pending_action_record_ids.discard(record_id)
            self.pending_action_types.pop(record_id, None)
            if hasattr(self, "_pop_lan_portal_upload_job"):
                try:
                    self._pop_lan_portal_upload_job(
                        record_id=record_id,
                        active_item_id=active_item_id,
                    )
                except Exception:
                    pass
            list_widget_now, item_now = self._find_active_item_by_record_id(record_id)
            if (not item_now or not self._is_valid_list_item(item_now)) and active_item_id:
                list_widget_now, item_now = self._find_active_item_by_active_item_id(active_item_id)
            if item_now and self._is_valid_list_item(item_now):
                self._remove_active_item_widget_only(list_widget_now, item_now)
            if hasattr(self, "_delete_active_cache_record"):
                self._delete_active_cache_record(data_dict)
            self.request_active_cache_save()
            self._remember_delete_undo(data_dict, result or {})
            log_info(f"UI操作: 删除事件(同步删除多维), Record ID: {record_id}")

        def _worker() -> None:
            success, error, result = self._submit_delete_active_item_to_backend(data_dict)
            enqueue = getattr(self, "_enqueue_ui_mutation", None)
            if callable(enqueue):
                enqueue(
                    "delete_active_item_result",
                    lambda s=success, e=error, r=result: _finish(s, e, r),
                )
            else:
                QTimer.singleShot(0, lambda s=success, e=error, r=result: _finish(s, e, r))

        threading.Thread(
            target=_worker,
            name="ClipFlowDeleteActiveItem",
            daemon=True,
        ).start()

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
        action_type = self._coerce_upload_action_for_existing_target(
            data_dict,
            action_type,
        )
        record_id = data_dict["record_id"]
        if (
            record_id in self.pending_action_record_ids
            or bool(data_dict.get("_upload_in_progress"))
            or self._has_pending_upload(record_id)
        ):
            self.show_message("该项目正在处理中，请稍候...")
            return

        self.pending_action_record_ids.add(record_id)
        self.pending_action_types[record_id] = action_type
        pending_hash = self._calc_text_hash(data_dict.get("text", ""))
        data_dict["_pending_upload_hash"] = pending_hash
        data_dict["_has_unuploaded_changes"] = False
        data_dict["_upload_in_progress"] = True
        data_dict["_upload_started_monotonic"] = time.monotonic()
        data_dict.pop("_last_upload_error", None)
        list_widget, item = self._find_active_item_by_record_id(record_id)
        if item and not self._is_valid_list_item(item):
            item = None
            list_widget = None
        if list_widget is not None and item is not None:
            item.setData(Qt.ItemDataRole.UserRole, data_dict)
            # 上传中是临时 UI 状态，不写入 active cache，避免重启后残留“上传中”。
            self._rebuild_active_item_widget(
                list_widget,
                item,
                data_dict,
                force_status=None,
                upload_in_progress=True,
                pending_upload_hash=pending_hash,
                has_unuploaded_changes=False,
            )
        self._show_screenshot_dialog(data_dict, action_type)

    def _clear_upload_queue(self, record_id):
        return None

    def _has_pending_upload(self, record_id):
        # Qt no longer owns the upload queue. Upload/update/end commands are
        # submitted to the backend and tracked by backend job state plus the
        # per-record _upload_in_progress flag, so stale local queue objects must
        # not block editing, deletion, or retry.
        return False

    def _queue_pending_content(self, record_id, new_content, new_status):
        if not record_id:
            return
        list_widget, item = self._find_active_item_by_record_id(record_id)
        if item and not self._is_valid_list_item(item):
            item = None
            list_widget = None
        if list_widget is not None and item is not None:
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
            new_data = self._mark_notice_content_dirty(new_data)
            if not new_data.get("_pending_upload_hash"):
                base_text = old_data.get("text", "")
                new_data["_pending_upload_hash"] = self._calc_text_hash(base_text)
            new_data["_upload_in_progress"] = bool(old_data.get("_upload_in_progress"))
            if record_id and self._has_pending_upload(record_id):
                new_data["_upload_in_progress"] = True
                new_data["_upload_started_monotonic"] = (
                    old_data.get("_upload_started_monotonic") or time.monotonic()
                )
            item.setData(Qt.ItemDataRole.UserRole, new_data)
            self.request_active_cache_save()
            if self._should_defer_ui_refresh():
                self._mark_cache_refresh_needed()
                return
            action_type = self.pending_action_types.get(record_id)
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
            self.request_active_cache_save()

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
        robot_group_choice="auto",
    ):
        if not data_dict:
            return
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
        elif notice_type == "变更通告":
            change_level = self._normalize_change_upload_level(data_dict, change_level)
            dialog_level = change_level
        resolved_fields = self._resolve_upload_fields_from_cache(
            data_dict,
            {
                "buildings": buildings,
                "specialty": specialty,
                "level": dialog_level,
                "maintenance_cycle": data_dict.get("maintenance_cycle"),
                "event_source": event_source,
                "transfer_to_overhaul": data_dict.get("transfer_to_overhaul"),
            },
        )
        specialty = str(resolved_fields.get("specialty") or "").strip()
        event_source = str(resolved_fields.get("event_source") or "").strip()
        _buildings = self._normalize_buildings_value(resolved_fields.get("buildings"))
        resolved_level = str(resolved_fields.get("level") or "").strip()
        maintenance_cycle = str(
            resolved_fields.get("maintenance_cycle")
            or data_dict.get("maintenance_cycle")
            or ""
        ).strip()
        route_payload = NoticePayload(
            text=data_dict.get("text", ""),
            level=resolved_level or data_dict.get("level"),
            buildings=_buildings,
            specialty=specialty,
            event_source=event_source,
            occurrence_date=data_dict.get("time_str"),
            transfer_to_overhaul=data_dict.get("transfer_to_overhaul"),
            robot_group_choice=str(robot_group_choice or "auto").strip() or "auto",
            maintenance_cycle=maintenance_cycle,
        )
        robot_group_choice = self._resolve_robot_group_choice_for_upload(
            notice_type,
            route_payload,
            route_payload.robot_group_choice,
        )
        if robot_group_choice is None:
            self._restore_screenshot_dialog_after_group_choice_cancel()
            return
        self._set_delete_interaction_enabled(True)
        # 截图确认与 I2 群确认都完成后再恢复剪贴板监听，避免弹窗期间状态提前放开。
        self._resume_clipboard_timer()
        if notice_type == "事件通告":
            event_level = resolved_level
        elif notice_type == "变更通告":
            change_level = self._normalize_change_upload_level(
                data_dict,
                resolved_level or change_level,
            )
        elif resolved_level:
            data_dict["level"] = resolved_level
        record_id = data_dict.get("record_id")
        if record_id:
            data_dict["_upload_in_progress"] = True
            data_dict.pop("_upload_pending_dialog", None)
            data_dict["_upload_started_monotonic"] = time.monotonic()
            data_dict.pop("_last_upload_error", None)
            list_widget, item = self._find_active_item_by_record_id(record_id)
            if item and not self._is_valid_list_item(item):
                item = None
                list_widget = None
            if list_widget is not None and item is not None:
                data = item.data(Qt.ItemDataRole.UserRole) or {}
                data["_upload_in_progress"] = True
                data.pop("_upload_pending_dialog", None)
                data["_upload_started_monotonic"] = time.monotonic()
                data.pop("_last_upload_error", None)
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
        if notice_type == "变更通告":
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
        if maintenance_cycle:
            data_dict["maintenance_cycle"] = maintenance_cycle
        else:
            data_dict.pop("maintenance_cycle", None)
        if response_time:
            data_dict["last_response_time"] = response_time
        self._update_active_item_data(
            data_dict.get("record_id"),
            data_dict,
            persist_cache=False,
        )

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
        payload = self._ensure_payload_for_data(data_dict)
        if payload:
            payload.text = data_dict.get("text", payload.text)
            payload.level = data_dict.get("level") or payload.level
            payload.buildings = _buildings
            payload.specialty = specialty
            payload.event_source = event_source
            payload.robot_group_choice = robot_group_choice
            payload.maintenance_cycle = (
                data_dict.get("maintenance_cycle") or payload.maintenance_cycle
            )
            payload.occurrence_date = (
                data_dict.get("time_str") or payload.occurrence_date
            )
            payload.transfer_to_overhaul = data_dict.get("transfer_to_overhaul")
        data_snapshot = dict(data_dict) if isinstance(data_dict, dict) else data_dict
        log_info(
            "UploadTime: "
            f"action={action_type} "
            f"notice_type={notice_type or '-'} "
            f"record_id={record_id or '-'} "
            f"response_time={response_time or '-'} "
            f"notice_time={data_snapshot.get('time_str') or '-'}"
        )

        def backend_task():
            self._delegate_qt_notice_upload_to_backend(
                data_snapshot=data_snapshot if isinstance(data_snapshot, dict) else {},
                screenshot_bytes=screenshot_bytes,
                extra_images=extra_images,
                action_type=action_type,
                response_time=response_time,
                recover_selected=recover_selected,
                robot_group_choice=robot_group_choice,
            )

        self._dispatch_backend_notice_upload(record_id, backend_task)
        if self.current_screenshot_record_id == record_id:
            self.current_screenshot_record_id = None
            self.current_screenshot_action_type = None
        self._try_process_deferred_events()
        return

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
        if list_widget is not None and item is not None:
            if hasattr(self, "_remove_active_notice_model_record"):
                self._remove_active_notice_model_record(data_dict)
            self._remove_active_item_from_source(list_widget, item)
        if not (
            hasattr(self, "_delete_active_cache_record")
            and self._delete_active_cache_record(data_dict)
        ):
            self.request_active_cache_save()

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

            new_data = self._mark_notice_content_dirty(new_data)
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
            self.request_active_cache_save()
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
        old_data = self._mark_notice_content_dirty(old_data)
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
        self.request_active_cache_save()

    def _remove_history_record(self, record_id):
        return

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
            data = self._mark_notice_content_dirty(data)
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
        self.request_active_cache_save()

    def _replace_record_id_everywhere(
        self, old_record_id: str, new_record_id: str
    ) -> bool:
        old_id = str(old_record_id or "").strip()
        new_id = str(new_record_id or "").strip()
        if not old_id or not new_id or old_id == new_id:
            return False

        changed = False
        updated_items = []
        try:
            store = self._active_notice_store()
            candidates = store.candidates_by_record_id(old_id)
        except Exception:
            store = None
            candidates = []
        if not candidates and store is not None:
            try:
                seen = set()
                for list_widget, item, data in store.entries():
                    if not self._is_valid_list_item(item) or not isinstance(data, dict):
                        continue
                    values = {
                        str(data.get("record_id") or "").strip(),
                        str(data.get("target_record_id") or "").strip(),
                        str(data.get("active_item_id") or "").strip(),
                        str(data.get("payload_key") or "").strip(),
                    }
                    if old_id not in values:
                        continue
                    marker = (id(list_widget), id(item))
                    if marker in seen:
                        continue
                    seen.add(marker)
                    candidates.append((list_widget, item, data))
            except Exception:
                pass
        for list_widget, item, data in candidates:
            try:
                if not self._is_valid_list_item(item):
                    continue
                if not isinstance(data, dict):
                    continue
                data = dict(data)
                data["record_id"] = new_id
                data["target_record_id"] = new_id
                data["_is_placeholder_record"] = False
                if data.get("payload_key") == old_id:
                    data["payload_key"] = new_id
                item.setData(Qt.ItemDataRole.UserRole, data)
                if hasattr(self, "_upsert_active_notice_model_item"):
                    self._upsert_active_notice_model_item(list_widget, item, data)
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

        lan_job_map = getattr(self, "_lan_portal_jobs_by_record_id", None)
        if isinstance(lan_job_map, dict) and old_id in lan_job_map:
            # Keep the old key until the upload-finished callback pops the job.
            # This makes the portal status update safe regardless of whether
            # notification runs before or after placeholder -> real ID migration.
            if new_id not in lan_job_map:
                lan_job_map[new_id] = lan_job_map[old_id]
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

    def _apply_selected_target_record_to_active_item(
        self,
        *,
        old_record_id: str,
        target_record_id: str,
        selected_data: dict,
    ) -> bool:
        target_id = str(target_record_id or "").strip()
        if not target_id or not isinstance(selected_data, dict):
            return False
        old_id = str(old_record_id or "").strip()
        active_item_id = str(selected_data.get("active_item_id") or "").strip()
        list_widget, item = self._find_active_item_by_record_id(target_id)
        if (not item or not self._is_valid_list_item(item)) and active_item_id:
            list_widget, item = self._find_active_item_by_active_item_id(active_item_id)
        if (not item or not self._is_valid_list_item(item)) and old_id:
            list_widget, item = self._find_active_item_by_record_id(old_id)
        if not item or not self._is_valid_list_item(item):
            return False
        committed = dict(selected_data)
        committed["record_id"] = target_id
        committed["target_record_id"] = target_id
        committed["_is_placeholder_record"] = False
        try:
            item.setData(Qt.ItemDataRole.UserRole, committed)
        except Exception:
            return False
        if hasattr(self, "_upsert_active_notice_model_item"):
            self._upsert_active_notice_model_item(list_widget, item, committed)
        if getattr(self, "cache_store", None):
            try:
                self.cache_store.patch_record_fields(
                    record_id=target_id,
                    patch={
                        "record_id": target_id,
                        "target_record_id": target_id,
                        "_is_placeholder_record": False,
                    },
                )
            except Exception:
                pass
        return True

    def on_request_finished(self, name, success, msg, record_id=None):
        self._set_last_ui_op(
            "on_request_finished", name=name, success=success, record_id=record_id
        )

        def _apply_updates():
            real_record_id = ""
            try:
                if name == "结束" and not success and record_id:
                    self._rollback_end(record_id)
                    self.show_message(f"「{name}」失败\n{msg}")
                    self._notify_lan_portal_upload_result(
                        name, success, msg, record_id, ""
                    )
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
                    if name in ["上传", "归档"] and record_id:
                        real_record_id = str(msg or "").strip()
                        if real_record_id:
                            clear_state = getattr(
                                self, "clear_upload_runtime_state_for_ids", None
                            )
                            if callable(clear_state):
                                clear_state(record_id, real_record_id, mark_uploaded=True)
                        if real_record_id:
                            changed = self._replace_record_id_everywhere(
                                record_id, real_record_id
                            )
                            if changed:
                                log_info(
                                    f"UI更新: 已保存 record_id={real_record_id} (原={record_id})"
                                )
                                self.request_active_cache_save()
                            if self.pending_update_after_upload:
                                self._schedule_pending_update_after_upload()

                    if name in ["更新", "结束"] and record_id:
                        clear_state = getattr(
                            self, "clear_upload_runtime_state_for_ids", None
                        )
                        if callable(clear_state):
                            clear_state(record_id, mark_uploaded=True)

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

                    if name == "结束" and record_id:
                        list_widget, item = self._find_active_item_by_record_id(
                            record_id
                        )
                        if item and not self._is_valid_list_item(item):
                            item = None
                            list_widget = None
                        if item and list_widget:
                            data = item.data(Qt.ItemDataRole.UserRole) or {}
                            self._do_move_to_history(
                                item,
                                data if isinstance(data, dict) else {"record_id": record_id},
                                track_rollback=False,
                            )

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
                                    old_data = self._mark_notice_content_dirty(old_data)
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
                self._notify_lan_portal_upload_result(
                    name,
                    success,
                    msg,
                    record_id,
                    real_record_id or "",
                )
                self._try_process_deferred_events()
            except Exception as exc:
                log_error(f"on_request_finished异常: {exc}")
            finally:
                clear_state = getattr(self, "clear_upload_runtime_state_for_ids", None)
                if callable(clear_state):
                    clear_state(record_id, real_record_id)

        self._enqueue_ui_mutation("request_finished", _apply_updates)

    def _on_manual_add_accepted(self):
        if not self.add_dialog:
            return
        content = self.add_dialog.result_text
        record_id = getattr(self.add_dialog, "result_record_id", "") or ""
        if not content:
            return
        info = extract_event_info(content)
        if not info:
            return
        cleaned_content = info.get("content") or content
        result = self._submit_notice_text_to_backend_projection(
            cleaned_content,
            source="manual_add",
            target_record_id=record_id,
        )
        if not result.get("ok"):
            self.show_message(str(result.get("error") or "通告提交后端失败。"))
            return
        self.show_message("通告已提交后端处理。")

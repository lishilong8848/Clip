# -*- coding: utf-8 -*-
import os
import json
import threading
import queue
import time
import re
import uuid
from PyQt6.QtWidgets import QMessageBox
from PyQt6.QtCore import Qt, QTimer, QUrl, QThread, qInstallMessageHandler
from PyQt6.QtGui import QDesktopServices

from ..config import config
from ..logger import log_info, log_error, log_warning, write_crash_trace_message
from ..utils import BASE_DIR
from ..hot_reload.manager import HotReloadManager
from ..services.system_alert_webhook import send_system_alert
from ..core.parser import extract_event_info
from ..core.speech import speech_manager

_QT_MESSAGE_HANDLER_INSTALLED = False
DEFAULT_DISPLAY_VERSION = "V1.0.20260210"

class MainWindowRuntimeMixin:
    def _build_display_version(self) -> str:
        meta_path = os.path.join(BASE_DIR, "build_meta.json")
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            return "未知版本"

        display_version = (meta.get("display_version") or "").strip()
        if display_version:
            return display_version

        major = meta.get("major_version")
        patch = meta.get("patch_version")
        if isinstance(major, int) and isinstance(patch, int):
            return f"v{major}.{patch}"

        build_id = (meta.get("build_id") or "").strip()
        return build_id or "未知版本"

    def _emit_system_alert(
        self,
        event_code: str,
        title: str,
        detail: str,
        *,
        severity: str = "error",
        dedup_key: str = "",
        extra: dict | None = None,
    ):
        try:
            send_system_alert(
                event_code=event_code,
                title=title,
                detail=detail,
                severity=severity,
                dedup_key=dedup_key,
                extra=extra or {},
            )
        except Exception as exc:
            log_warning(f"系统告警发送失败(本地异常): {exc}")

    def _on_clipboard_entry_received(self, entry: dict):
        if self._is_clipboard_listener_disabled():
            return
        if not entry:
            return
        entry_id = entry.get("entry_id", "")
        if entry_id and not self._has_clipboard_pending_entry(entry_id):
            if not self._add_clipboard_pending_entry(entry):
                return
        if self.current_screenshot_record_id or self.screenshot_dialog.isVisible():
            self._defer_event(
                {
                    "content": entry.get("content", ""),
                    "status": entry.get("status", ""),
                    "title": entry.get("title", ""),
                    "notice_type": entry.get("notice_type", ""),
                    "entry_id": entry_id,
                }
            )
            return
        self._process_event(
            entry.get("content", ""),
            entry.get("status", ""),
            entry.get("notice_type", ""),
            entry=entry,
        )
        self._remove_clipboard_pending_entry(entry.get("entry_id", ""))

    def _update_event_relay_status(self, status: str):
        label = getattr(self, "relay_status_label", None)
        if not label:
            return
        color = "#F59E0B"
        if "已启动" in status or "运行中" in status:
            color = "#10B981"
        elif "已关闭" in status or "已停止" in status:
            color = "#94A3B8"
        elif "失败" in status or "不可用" in status:
            color = "#EF4444"
        label.setText(f"事件中转: {status}")
        label.setStyleSheet(f"color: {color}; font-size: 11px;")

    def _on_event_relay_received(self, content: str, status: str, notice_type: str):
        if self.current_screenshot_record_id or self.screenshot_dialog.isVisible():
            info = extract_event_info(content) or {}
            self._defer_event(
                {
                    "content": info.get("content") or content,
                    "status": status or info.get("status", ""),
                    "title": info.get("title", ""),
                    "notice_type": notice_type or info.get("notice_type", ""),
                    "entry_id": "",
                }
            )
            return
        self._process_event(content, status, notice_type)

    def _stop_event_relay_bridge(self):
        bridge = getattr(self, "_event_relay_bridge", None)
        if not bridge:
            return
        try:
            bridge.stop()
        except Exception:
            pass

    def _is_event_relay_enabled(self) -> bool:
        return bool(getattr(config, "relay_enabled", False))

    def _apply_event_relay_setting(self, force_reload: bool = True):
        if force_reload:
            try:
                config.load()
            except Exception as exc:
                log_error(f"事件中转设置加载失败: {exc}")
        bridge = getattr(self, "_event_relay_bridge", None)
        if not bridge:
            return
        if self._is_event_relay_enabled():
            try:
                bridge.start()
            except Exception as exc:
                log_error(f"事件中转启动失败: {exc}")
                self._update_event_relay_status("启动失败")
        else:
            try:
                bridge.stop()
            except Exception:
                pass
            self._update_event_relay_status("已关闭")

    def refresh_event_relay_setting(self):
        self._apply_event_relay_setting(force_reload=True)

    def _init_hot_reload(self):
        try:
            from pathlib import Path

            if config.disable_hot_reload:
                log_info("HotReload: 已禁用")
                return

            project_root = Path(__file__).resolve().parents[3]
            self.hot_reload_manager = HotReloadManager(project_root, ui_host=self)
            self.hot_reload_manager.start()
        except Exception as exc:
            log_error(f"HotReload: 初始化失败: {exc}")

    def refresh_hot_reload_setting(self):
        try:
            from pathlib import Path

            config.load()
            if config.disable_hot_reload:
                if hasattr(self, "hot_reload_manager") and self.hot_reload_manager:
                    self.hot_reload_manager.stop()
                self.hot_reload_manager = None
                log_info("HotReload: 已禁用")
                return

            project_root = Path(__file__).resolve().parents[3]
            if (
                not hasattr(self, "hot_reload_manager")
                or self.hot_reload_manager is None
            ):
                self.hot_reload_manager = HotReloadManager(project_root, ui_host=self)
            else:
                self.hot_reload_manager.stop()
            self.hot_reload_manager.start()
            log_info("HotReload: 已启用")
        except Exception as exc:
            log_error(f"HotReload: 重新初始化失败: {exc}")

    def refresh_alert_setting(self):
        try:
            config.load()
            speech_manager.set_enabled(
                not bool(getattr(config, "disable_alerts", False))
                and not bool(getattr(config, "disable_speech", True))
            )
        except Exception as exc:
            log_error(f"提醒设置更新失败: {exc}")

    def _build_table_url(self, table_id: str) -> str:
        app_token = (config.app_token or "").strip()
        if not app_token:
            return ""
        if app_token.startswith("http://") or app_token.startswith("https://"):
            base_url = app_token
        else:
            base_url = f"https://feishu.cn/base/{app_token}"
        table_id = (table_id or "").strip()
        if not table_id:
            return base_url
        sep = "&" if "?" in base_url else "?"
        return f"{base_url}{sep}table={table_id}"

    def _show_info_message(self, title: str, text: str):
        msg = QMessageBox(self)
        msg.setWindowTitle(title)
        msg.setText(text)
        msg.setStandardButtons(QMessageBox.StandardButton.Ok)
        msg.button(QMessageBox.StandardButton.Ok).setText("确定")
        msg.exec()

    def _open_table_link(self, table_attr: str):
        table_id = getattr(config, table_attr, "")
        url = self._build_table_url(table_id)
        if not url:
            self._show_info_message("未配置表格", "请先在设置中填写表格ID或App Token。")
            return
        QDesktopServices.openUrl(QUrl(url))

    def _build_lan_template_portal_url_from_config(self) -> str:
        host = str(
            getattr(config, "lan_template_portal_host", "0.0.0.0") or "0.0.0.0"
        ).strip()
        port = int(getattr(config, "lan_template_portal_port", 18766) or 18766)
        display_host = "127.0.0.1" if host == "0.0.0.0" else host
        return f"http://{display_host}:{port}"

    def _build_lan_template_portal_url(self) -> str:
        url = str(getattr(self, "lan_template_portal_url", "") or "").strip()
        if url:
            return url
        return self._build_lan_template_portal_url_from_config()

    def _open_lan_template_portal(self):
        url = self._build_lan_template_portal_url()
        if not url:
            self._show_info_message("局域网页面不可用", "局域网模板页面服务未启动。")
            return
        QDesktopServices.openUrl(QUrl(url))

    def enqueue_lan_template_notice(self, payload: dict) -> bool:
        if getattr(self, "_closing", False):
            return False
        text = str((payload or {}).get("text") or "").strip()
        if not text:
            return False
        self.lan_template_notice_received.emit(dict(payload or {}))
        return True

    def _on_lan_template_notice_received(self, payload: dict):
        text = str((payload or {}).get("text") or "").strip()
        if not text:
            log_warning("局域网模板通告回填失败: 文本为空")
            return
        info = extract_event_info(text) or {}
        if not info:
            log_warning("局域网模板通告回填失败: 无法解析为通告")
            self.show_message("局域网模板通告无法解析，未加入主界面。")
            return
        self._process_event(
            info.get("content") or text,
            info.get("status", ""),
            info.get("notice_type", ""),
        )
        log_info(
            "局域网模板通告已加入主界面: "
            f"title={(payload or {}).get('title', '')} "
            f"target_building={(payload or {}).get('target_building', '')}"
        )

    @staticmethod
    def _lan_scope_code(value) -> str:
        text = str(value or "").strip().upper()
        if text in {"ALL", "全部"}:
            return "ALL"
        if "110" in text:
            return "110"
        match = re.search(r"[ABCDEH]", text)
        return match.group(0) if match else ""

    @classmethod
    def _lan_scope_matches(cls, scope, building) -> bool:
        scope_code = cls._lan_scope_code(scope)
        if not scope_code or scope_code == "ALL":
            return True
        if str(scope or "").strip().upper() in {"ALL", "全部"}:
            return True
        values = building if isinstance(building, (list, tuple, set)) else [building]
        for value in values:
            if cls._lan_scope_code(value) == scope_code:
                return True
        return False

    def _lan_notice_sections(self, text: str) -> dict:
        return {
            "title": self._extract_section_text(text, ("名称", "标题")),
            "time": self._extract_section_text(text, ("时间",)),
            "location": self._extract_section_text(text, ("位置", "地点")),
            "content": self._extract_section_text(text, ("内容",)),
            "reason": self._extract_section_text(text, ("原因",)),
            "impact": self._extract_section_text(text, ("影响",)),
            "progress": self._extract_section_text(text, ("进度",)),
        }

    def get_lan_maintenance_ongoing_notices(self, scope: str = "ALL") -> list[dict]:
        try:
            if QThread.currentThread() == self.thread():
                return self._collect_lan_maintenance_ongoing_notices(scope)
        except Exception:
            pass
        event = threading.Event()
        result = {"items": []}
        self.lan_maintenance_ongoing_query_received.emit(
            {"scope": scope, "event": event, "result": result}
        )
        event.wait(timeout=5)
        items = result.get("items")
        return items if isinstance(items, list) else []

    def _on_lan_maintenance_ongoing_query_received(self, payload: dict):
        result = (payload or {}).get("result")
        event = (payload or {}).get("event")
        try:
            scope = (payload or {}).get("scope", "ALL")
            if isinstance(result, dict):
                result["items"] = self._collect_lan_maintenance_ongoing_notices(scope)
        except Exception as exc:
            if isinstance(result, dict):
                result["items"] = []
                result["error"] = str(exc)
        finally:
            try:
                if event:
                    event.set()
            except Exception:
                pass

    def _collect_lan_maintenance_ongoing_notices(self, scope: str = "ALL") -> list[dict]:
        items: list[dict] = []
        for _, item in self._iter_active_items():
            try:
                data = item.data(Qt.ItemDataRole.UserRole) or {}
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            if str(data.get("notice_type") or "").strip() != "维保通告":
                continue
            text = str(data.get("text") or "").strip()
            info = extract_event_info(text) or {}
            if str(info.get("status") or "").strip() == "结束":
                continue
            sections = self._lan_notice_sections(text)
            buildings = self._normalize_buildings_value(data.get("buildings"))
            if not buildings:
                buildings = self._infer_buildings_from_notice_text(text)
            building = "、".join(buildings) if buildings else ""
            if not self._lan_scope_matches(scope, buildings or building):
                continue
            record_id = str(data.get("record_id") or "").strip()
            active_item_id = str(data.get("active_item_id") or "")
            upload_busy = bool(data.get("_upload_in_progress")) or (
                record_id and self._has_pending_upload(record_id)
            )
            blocked = (
                not active_item_id
                or self._is_placeholder_record(data)
                or upload_busy
                or self._is_record_binding_conflicted(data)
                or self._is_routing_conflicted(data)
            )
            block_reason = ""
            if not active_item_id:
                block_reason = "条目缺少本地身份，请刷新主界面"
            elif self._is_placeholder_record(data):
                block_reason = "开始上传未完成，暂不能更新/结束"
            elif upload_busy:
                block_reason = "正在上传，请稍候"
            elif self._is_record_binding_conflicted(data):
                block_reason = self._record_binding_error_text(data)
            elif self._is_routing_conflicted(data):
                block_reason = self._routing_error_text(data)
            items.append(
                {
                    "active_item_id": active_item_id,
                    "record_id": "" if self._is_placeholder_record(data) else record_id,
                    "raw_record_id": record_id,
                    "source_record_id": str(data.get("lan_source_record_id") or ""),
                    "title": sections["title"] or info.get("title") or "",
                    "status": info.get("status") or "",
                    "building": building,
                    "specialty": str(data.get("specialty") or ""),
                    "time": sections["time"] or info.get("time_str") or "",
                    "start_time": "",
                    "end_time": "",
                    "location": sections["location"],
                    "content": sections["content"],
                    "reason": sections["reason"],
                    "impact": sections["impact"],
                    "progress": sections["progress"],
                    "can_update": not blocked,
                    "can_end": not blocked,
                    "block_reason": block_reason,
                    "upload_state": "上传中" if upload_busy else ("已上传" if record_id and not self._is_placeholder_record(data) else "未上传"),
                }
            )
        return items

    def enqueue_lan_maintenance_action(self, payload: dict):
        if getattr(self, "_closing", False):
            return {"ok": False, "error": "主程序正在关闭。"}
        if not isinstance(payload, dict):
            return {"ok": False, "error": "请求格式错误。"}
        self.lan_maintenance_action_received.emit(dict(payload))
        return {"ok": True}

    def _on_lan_maintenance_action_received(self, payload: dict):
        job_id = str((payload or {}).get("job_id") or "").strip()
        try:
            self._execute_lan_maintenance_action(payload or {})
        except Exception as exc:
            log_error(f"局域网维保动作执行失败: job_id={job_id}, error={exc}")
            self._mark_lan_portal_job_result(
                job_id,
                success=False,
                message=str(exc),
                record_id=str((payload or {}).get("record_id") or ""),
            )

    def _register_lan_portal_upload_job(
        self, job_id: str, record_id: str = "", active_item_id: str = ""
    ) -> None:
        job_id = str(job_id or "").strip()
        record_id = str(record_id or "").strip()
        active_item_id = str(active_item_id or "").strip()
        if not job_id:
            return
        if record_id:
            self._lan_portal_jobs_by_record_id[record_id] = job_id
        if active_item_id:
            self._lan_portal_jobs_by_active_item_id[active_item_id] = job_id

    def _pop_lan_portal_upload_job(self, record_id: str = "", active_item_id: str = "") -> str:
        record_id = str(record_id or "").strip()
        active_item_id = str(active_item_id or "").strip()
        job_id = ""
        if record_id:
            job_id = self._lan_portal_jobs_by_record_id.pop(record_id, "")
        if not job_id and active_item_id:
            job_id = self._lan_portal_jobs_by_active_item_id.pop(active_item_id, "")
        if job_id:
            for rid, mapped in list(self._lan_portal_jobs_by_record_id.items()):
                if mapped == job_id:
                    self._lan_portal_jobs_by_record_id.pop(rid, None)
            for aid, mapped in list(self._lan_portal_jobs_by_active_item_id.items()):
                if mapped == job_id:
                    self._lan_portal_jobs_by_active_item_id.pop(aid, None)
        return job_id

    def _mark_lan_portal_job_result(
        self,
        job_id: str,
        *,
        success: bool,
        message: str = "",
        record_id: str = "",
        active_item_id: str = "",
    ) -> None:
        job_id = str(job_id or "").strip()
        if not job_id:
            return
        controller = getattr(self, "lan_template_portal_controller", None)
        if controller and hasattr(controller, "mark_job_upload_result"):
            try:
                controller.mark_job_upload_result(
                    job_id,
                    success=bool(success),
                    message=str(message or ""),
                    record_id=str(record_id or ""),
                    active_item_id=str(active_item_id or ""),
                )
            except Exception as exc:
                log_warning(f"局域网维保任务状态回写失败: job_id={job_id}, error={exc}")

    def _notify_lan_portal_upload_result(
        self,
        name: str,
        success: bool,
        msg: str,
        record_id: str = "",
        real_record_id: str = "",
    ) -> None:
        if name not in {"上传", "更新", "结束"}:
            return
        record_id = str(record_id or "").strip()
        real_record_id = str(real_record_id or "").strip()
        active_item_id = ""
        for candidate_id in (real_record_id, record_id):
            if not candidate_id:
                continue
            try:
                _, item = self._find_active_item_by_record_id(candidate_id)
                if item and self._is_valid_list_item(item):
                    data = item.data(Qt.ItemDataRole.UserRole) or {}
                    active_item_id = str(data.get("active_item_id") or "").strip()
                    if active_item_id:
                        break
            except Exception:
                continue
        job_id = ""
        for candidate_id in (record_id, real_record_id):
            if candidate_id:
                job_id = self._pop_lan_portal_upload_job(record_id=candidate_id)
                if job_id:
                    break
        if not job_id:
            if active_item_id:
                job_id = self._pop_lan_portal_upload_job(active_item_id=active_item_id)
        if not job_id:
            log_warning(
                "局域网维保任务状态回写跳过: 未找到任务映射 "
                f"name={name} record_id={record_id} real_record_id={real_record_id}"
            )
            return
        final_record_id = str(real_record_id or record_id or "").strip()
        self._mark_lan_portal_job_result(
            job_id,
            success=bool(success),
            message=str(msg or ""),
            record_id=final_record_id,
            active_item_id=active_item_id,
        )
        log_info(
            "局域网维保任务状态已回写: "
            f"job_id={job_id} success={bool(success)} record_id={final_record_id}"
        )

    def _find_lan_maintenance_duplicate_start(
        self, source_record_id: str = "", title: str = ""
    ) -> dict | None:
        source_record_id = str(source_record_id or "").strip()
        normalized_title = re.sub(r"\s+", "", str(title or ""))
        for _, item in self._iter_active_items():
            try:
                data = item.data(Qt.ItemDataRole.UserRole) or {}
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            if str(data.get("notice_type") or "").strip() != "维保通告":
                continue
            text = str(data.get("text") or "")
            info = extract_event_info(text) or {}
            if str(info.get("status") or "").strip() == "结束":
                continue
            if source_record_id and str(data.get("lan_source_record_id") or "") == source_record_id:
                return data
            if normalized_title:
                current_title = re.sub(
                    r"\s+", "", str(info.get("title") or self._lan_notice_sections(text)["title"] or "")
                )
                if current_title == normalized_title:
                    return data
        return None

    def _execute_lan_maintenance_action(self, payload: dict):
        job_id = str(payload.get("job_id") or "").strip()
        action = str(payload.get("action") or "").strip().lower()
        text = str(payload.get("text") or "").strip()
        if action not in {"start", "update", "end"}:
            raise RuntimeError("动作必须是 start/update/end。")
        if not text:
            raise RuntimeError("通告文本为空。")
        info = extract_event_info(text) or {}
        if info.get("notice_type") != "维保通告":
            raise RuntimeError("只能处理维保通告。")
        response_time = str(payload.get("response_time") or "").strip()
        building = str(payload.get("building") or "").strip()
        specialty = str(payload.get("specialty") or "").strip()
        buildings = self._normalize_buildings_value([building])
        if not buildings:
            buildings = [building] if building else []

        if action == "start":
            duplicate = self._find_lan_maintenance_duplicate_start(
                str(payload.get("record_id") or ""), str(payload.get("title") or "")
            )
            if duplicate:
                duplicate_id = str(duplicate.get("record_id") or "").strip()
                if self._is_placeholder_record(duplicate):
                    raise RuntimeError("该维护项已在主界面发起，开始上传未完成。")
                raise RuntimeError(f"该维护项已存在进行中通告: {duplicate_id}")
            placeholder_id = uuid.uuid4().hex
            data = {
                "record_id": placeholder_id,
                "_is_placeholder_record": True,
                "text": info.get("content") or text,
                "notice_type": "维保通告",
                "time_str": info.get("time_str") or "",
                "buildings": buildings,
                "specialty": specialty,
                "_has_unuploaded_changes": False,
                "_pending_upload_hash": None,
                "_upload_in_progress": False,
                "lan_source_record_id": str(payload.get("record_id") or ""),
            }
            self._ensure_payload_for_data(data)
            item, _ = self.add_active_item(data)
            if not item:
                raise RuntimeError("主界面创建维保条目失败。")
            committed = item.data(Qt.ItemDataRole.UserRole) or data
            active_item_id = str(committed.get("active_item_id") or "")
            self._register_lan_portal_upload_job(job_id, placeholder_id, active_item_id)
            self.do_feishu_upload(
                committed,
                None,
                "upload",
                response_time=response_time,
                buildings=buildings,
                specialty=specialty,
                robot_group_choice="auto",
            )
            return

        active_item_id = str(payload.get("active_item_id") or "").strip()
        list_widget, item = self._find_active_item_by_active_item_id(active_item_id)
        if not item or not self._is_valid_list_item(item):
            raise RuntimeError("未找到主界面进行中的维保条目。")
        data = dict(item.data(Qt.ItemDataRole.UserRole) or {})
        if self._is_placeholder_record(data):
            raise RuntimeError("开始上传未完成，暂不能更新/结束。")
        if self._is_record_binding_conflicted(data):
            raise RuntimeError(self._record_binding_error_text(data))
        if self._is_routing_conflicted(data):
            raise RuntimeError(self._routing_error_text(data))
        record_id = str(data.get("record_id") or "").strip()
        if not record_id:
            raise RuntimeError("该条目缺少真实 record_id。")
        if bool(data.get("_upload_in_progress")) or self._has_pending_upload(record_id):
            raise RuntimeError("该条目正在上传，请等待完成后再操作。")
        current_info = extract_event_info(str(data.get("text") or "")) or {}
        current_status = str(current_info.get("status") or "").strip()
        if current_status == "结束":
            raise RuntimeError("该维保通告已结束，不能继续更新。")
        requested_title = re.sub(r"\s+", "", str(payload.get("title") or ""))
        current_title = re.sub(
            r"\s+",
            "",
            str(
                current_info.get("title")
                or self._lan_notice_sections(str(data.get("text") or ""))["title"]
                or ""
            ),
        )
        if requested_title and current_title and requested_title != current_title:
            raise RuntimeError("请求通告名称与主界面当前条目不一致。")
        current_buildings = self._normalize_buildings_value(data.get("buildings"))
        if not current_buildings:
            current_buildings = self._infer_buildings_from_notice_text(
                str(data.get("text") or "")
            )
        if current_buildings and not self._lan_scope_matches(building, current_buildings):
            raise RuntimeError("请求楼栋与主界面当前条目不一致。")
        data["text"] = info.get("content") or text
        data["notice_type"] = "维保通告"
        data["time_str"] = info.get("time_str") or data.get("time_str") or ""
        data["buildings"] = buildings
        if specialty:
            data["specialty"] = specialty
        data["_has_unuploaded_changes"] = False
        data["_pending_upload_hash"] = None
        data["_upload_in_progress"] = False
        committed = self._commit_active_record(
            data,
            refresh_detail=True,
            rebuild_widget=True,
            list_widget=list_widget,
            item=item,
        ) or data
        self.save_active_cache()
        self._register_lan_portal_upload_job(job_id, record_id, active_item_id)
        self.do_feishu_upload(
            committed,
            None,
            "end" if action == "end" else "update",
            response_time=response_time,
            buildings=buildings,
            specialty=specialty,
            robot_group_choice="auto",
        )

    def refresh_lan_template_portal_link(self):
        if hasattr(self, "lan_template_portal_btn"):
            self.lan_template_portal_btn.setToolTip(
                f"打开局域网模板页面：{self._build_lan_template_portal_url()}"
            )

    def refresh_lan_template_portal_setting(self):
        host = str(
            getattr(config, "lan_template_portal_host", "0.0.0.0") or "0.0.0.0"
        ).strip()
        port = int(getattr(config, "lan_template_portal_port", 18766) or 18766)
        controller = getattr(self, "lan_template_portal_controller", None)
        if controller is None:
            self.lan_template_portal_url = self._build_lan_template_portal_url_from_config()
            self.refresh_lan_template_portal_link()
            return
        try:
            current_host = str(getattr(controller, "host", "") or "").strip()
            current_port = int(getattr(controller, "preferred_port", 0) or 0)
            if current_host != host or current_port != port:
                previous_url = controller.get_url()
                controller.stop()
                controller.host = host
                controller.preferred_port = port
                try:
                    self.lan_template_portal_url = controller.start()
                    if hasattr(controller, "set_notice_callback"):
                        controller.set_notice_callback(self.enqueue_lan_template_notice)
                    if hasattr(controller, "set_ongoing_callback"):
                        controller.set_ongoing_callback(
                            self.get_lan_maintenance_ongoing_notices
                        )
                    if hasattr(controller, "set_maintenance_action_callback"):
                        controller.set_maintenance_action_callback(
                            self.enqueue_lan_maintenance_action
                        )
                except Exception:
                    controller.host = current_host
                    controller.preferred_port = current_port
                    try:
                        self.lan_template_portal_url = controller.start()
                        if hasattr(controller, "set_notice_callback"):
                            controller.set_notice_callback(
                                self.enqueue_lan_template_notice
                            )
                        if hasattr(controller, "set_ongoing_callback"):
                            controller.set_ongoing_callback(
                                self.get_lan_maintenance_ongoing_notices
                            )
                        if hasattr(controller, "set_maintenance_action_callback"):
                            controller.set_maintenance_action_callback(
                                self.enqueue_lan_maintenance_action
                            )
                    except Exception:
                        self.lan_template_portal_url = previous_url
                    raise
                log_info(
                    "局域网模板页面服务已按设置重启: "
                    f"{self.lan_template_portal_url}"
                )
            else:
                self.lan_template_portal_url = controller.get_url()
            self.refresh_lan_template_portal_link()
        except Exception as exc:
            self.lan_template_portal_url = self._build_lan_template_portal_url_from_config()
            self.refresh_lan_template_portal_link()
            log_error(f"局域网模板页面服务重启失败: {exc}")
            self._show_info_message(
                "局域网页面重启失败",
                f"请确认该 IP 属于本机网卡，或重启程序后再试。\n{exc}",
            )

    def refresh_table_links(self):
        if not hasattr(self, "table_link_buttons"):
            return
        for btn, attr in self.table_link_buttons:
            table_id = getattr(config, attr, "")
            url = self._build_table_url(table_id)
            if url:
                btn.setToolTip(url)
            else:
                btn.setToolTip("未配置")

    def _check_ocr_lang_pack(self):
        """检测系统是否安装了中文 OCR 语言包，如果没装则弹窗引导安装"""
        if self._closing:
            return

        def _do_check():
            try:
                from winocr import recognize_pil as _rp
                from PIL import Image as _Img
                import asyncio

                test_img = _Img.new("RGB", (60, 20), "white")
                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(_rp(test_img, "zh-Hans-CN"))
                    return True  # 成功，说明已安装
                except Exception:
                    return False  # 失败，进行下一步提示
                finally:
                    loop.close()
            except Exception:
                return True  # 其他异常暂不打扰

        def _on_check_result(is_installed):
            if is_installed or self._closing:
                return

            reply = QMessageBox.question(
                self,
                "OCR 组件缺失",
                "检测到您的系统缺少【中文 OCR 组件】，这将导致包含汉字的截屏日期无法被识别。\n\n是否立即自动下载并安装？（需要管理员权限，约需10~30秒）",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if reply == QMessageBox.StandardButton.Yes:
                self.install_ocr_lang_pack()

        import threading

        def _worker():
            res = _do_check()
            QTimer.singleShot(0, lambda: _on_check_result(res))

        threading.Thread(target=_worker, daemon=True).start()

    def install_ocr_lang_pack(self):
        """提权执行 PowerShell 安装中文 OCR 包"""
        import ctypes

        ps_script = (
            "Write-Host '正在为您安装 Windows 中文 OCR 语言包，请稍候...' -ForegroundColor Cyan;"
            "Add-WindowsCapability -Online -Name 'Language.OCR~~~zh-Hans~0.0.1.0';"
            "Write-Host '安装流程结束！本窗口将在 3 秒后关闭。' -ForegroundColor Green;"
            "Start-Sleep -Seconds 3"
        )
        ret = ctypes.windll.shell32.ShellExecuteW(
            None, "runas", "powershell.exe", f'-Command "{ps_script}"', None, 1
        )
        if ret <= 32:
            msg = QMessageBox(self)
            msg.setWindowFlags(msg.windowFlags() | Qt.WindowType.FramelessWindowHint)
            msg.setIcon(QMessageBox.Icon.Warning)
            msg.setWindowTitle("")
            msg.setText("未能获取管理员权限 或 安装被取消。")
            msg.exec()
        else:
            if hasattr(self, "_update_overlay") and self._update_overlay:
                self._update_overlay.show()
                self._update_overlay.set_blur_intensity(20)
                self.repaint()

            msg = QMessageBox(self)
            msg.setWindowFlags(msg.windowFlags() | Qt.WindowType.FramelessWindowHint)
            msg.setIcon(QMessageBox.Icon.Information)
            msg.setWindowTitle("")
            msg.setText(
                "已请求管理员权限开始自动安装 OCR 组件。\n\n请等待弹出的黑色窗口跑完进度条自动关闭，即可正常使用截图识别功能。"
            )
            msg.exec()

    def _install_qt_message_handler(self):
        global _QT_MESSAGE_HANDLER_INSTALLED
        if _QT_MESSAGE_HANDLER_INSTALLED:
            return

        def _qt_message_handler(msg_type, context, message):
            try:
                msg = f"QtMsg[{msg_type}] {message}"
                if any(
                    key in message
                    for key in ("QThreadStorage", "QObject", "QWidget", "QPainter")
                ):
                    log_warning(msg)
                else:
                    log_info(msg)
                write_crash_trace_message(msg)
            except Exception:
                pass

        try:
            qInstallMessageHandler(_qt_message_handler)
            _QT_MESSAGE_HANDLER_INSTALLED = True
        except Exception as exc:
            log_warning(f"Qt消息处理器安装失败: {exc}")

    def _enqueue_ui_mutation(self, tag: str, fn):
        if self._closing:
            return
        try:
            self._ui_mutation_queue.put_nowait((tag, fn, time.time()))
        except Exception:
            pass

    def _drain_ui_mutations(self):
        if self._closing or self._ui_update_in_progress:
            return
        max_count = max(1, int(self._ui_mutation_max_per_tick or 1))
        self._ui_update_in_progress = True
        try:
            for _ in range(max_count):
                try:
                    tag, fn, _ = self._ui_mutation_queue.get_nowait()
                except queue.Empty:
                    break
                try:
                    self._set_last_ui_op(f"ui_mutation:{tag}")
                    fn()
                except Exception as exc:
                    log_error(f"UI变更执行失败({tag}): {exc}")
                finally:
                    try:
                        self._ui_mutation_queue.task_done()
                    except Exception:
                        pass
        finally:
            self._ui_update_in_progress = False

    def _post_request_finished(self, name, success, msg, record_id):
        try:
            self._ui_signal_queue.put_nowait((name, bool(success), msg, record_id))
        except Exception:
            pass

    def _drain_ui_signal_queue(self):
        if self._closing or self._ui_update_in_progress:
            return
        max_count = max(1, int(self._ui_signal_max_per_tick or 1))
        for _ in range(max_count):
            try:
                item = self._ui_signal_queue.get_nowait()
            except queue.Empty:
                break
            try:
                name, success, msg, record_id = item
            except Exception:
                continue
            try:
                self.request_finished.emit(name, success, msg, record_id)
            except Exception:
                pass


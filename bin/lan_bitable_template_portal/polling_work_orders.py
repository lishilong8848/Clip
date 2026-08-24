# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import os
import re
import secrets
import shutil
import threading
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import quote

from upload_event_module.utils import get_data_file_path

from .portal_service import PortalConflictError, PortalError, PortalNotFoundError


POLLING_SOP_NAMESPACE = "polling_sop"
POLLING_WORK_ORDER_NAMESPACE = "polling_work_order"
POLLING_WORK_ORDER_SECRET_NAMESPACE = "polling_work_order_secret"
POLLING_WORK_ORDER_SECRET_KEY = "hmac"
POLLING_UNITS = tuple(f"{index}#" for index in range(1, 7))
POLLING_SOP_SCOPES = frozenset({"110", "A", "B", "C", "D", "E", "H"})
POLLING_SOP_MAX_FILE_BYTES = 20 * 1024 * 1024
POLLING_SOP_MAX_FILES = 10
POLLING_SOP_MAX_TOTAL_BYTES = 100 * 1024 * 1024
POLLING_SOP_MAX_STEPS = 200


class PollingWorkOrderTokenError(PortalError):
    status_code = 403


def _flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "是"}


def _safe_file_name(value: Any) -> str:
    name = Path(str(value or "").strip()).name
    name = re.sub(r"[\x00-\x1f<>:\"/\\|?*]+", "_", name).strip(" .")
    return name[:160] or "attachment.bin"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class PollingWorkOrderService:
    _lock = threading.RLock()

    def __init__(self, state_store) -> None:
        self.state_store = state_store
        self.sop_root = Path(get_data_file_path("polling_sop")).resolve()
        self.work_order_root = Path(
            get_data_file_path("polling_work_orders")
        ).resolve()

    @staticmethod
    def _now_text() -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    @staticmethod
    def _public_sop(sop: dict) -> dict:
        result = copy.deepcopy(sop or {})
        sop_id = str(result.get("sop_id") or "")
        result["attachments"] = [
            {
                **{key: value for key, value in item.items() if key != "path"},
                "download_url": (
                    f"/api/polling-sops/{quote(sop_id, safe='')}/attachments/"
                    f"{quote(str(item.get('attachment_id') or ''), safe='')}"
                ),
            }
            for item in result.get("attachments") or []
            if isinstance(item, dict)
        ]
        result["ready"] = bool(result.get("steps") and result.get("attachments"))
        return result

    def list_sops(self, scope: str) -> list[dict]:
        scope = str(scope or "").strip().upper()
        if scope not in POLLING_SOP_SCOPES:
            raise PortalError("请在明确的单楼页面读取轮巡 SOP。")
        items = [
            self._public_sop(document.get("payload") or {})
            for document in self.state_store.list_documents(POLLING_SOP_NAMESPACE)
            if isinstance(document.get("payload"), dict)
            and (
                str((document.get("payload") or {}).get("scope") or "").strip().upper()
                == scope
            )
        ]
        return sorted(items, key=lambda item: str(item.get("name") or "").casefold())

    def get_sop(self, sop_id: str, *, public: bool = True) -> dict:
        sop_id = str(sop_id or "").strip()
        sop = self.state_store.get_document(POLLING_SOP_NAMESPACE, sop_id)
        if not isinstance(sop, dict):
            raise PortalNotFoundError("轮巡 SOP 不存在。")
        return self._public_sop(sop) if public else copy.deepcopy(sop)

    @staticmethod
    def _normalized_steps(value: Any) -> list[dict]:
        raw_steps = value if isinstance(value, list) else []
        if len(raw_steps) > POLLING_SOP_MAX_STEPS:
            raise PortalError(f"SOP 步骤不能超过 {POLLING_SOP_MAX_STEPS} 条。")
        steps: list[dict] = []
        for index, raw in enumerate(raw_steps):
            if not isinstance(raw, dict):
                raise PortalError(f"第 {index + 1} 个 SOP 步骤格式无效。")
            content = str(raw.get("content") or "").strip()
            if not content:
                raise PortalError(f"第 {index + 1} 个 SOP 步骤缺少操作内容。")
            if len(content) > 5000:
                raise PortalError(f"第 {index + 1} 个 SOP 步骤内容过长。")
            operator_required = _flag(raw.get("operator_required"))
            reviewer_required = _flag(raw.get("reviewer_required"))
            if not operator_required and not reviewer_required:
                raise PortalError(
                    f"第 {index + 1} 个 SOP 步骤至少需要操作人或现场审核人确认。"
                )
            steps.append(
                {
                    "step_id": str(raw.get("step_id") or uuid.uuid4().hex),
                    "order": index + 1,
                    "content": content,
                    "operator_required": operator_required,
                    "reviewer_required": reviewer_required,
                }
            )
        return steps

    def save_sop(self, payload: dict, *, actor_open_id: str = "") -> dict:
        payload = payload if isinstance(payload, dict) else {}
        name = str(payload.get("name") or "").strip()
        scope = str(payload.get("scope") or "").strip().upper()
        if scope not in POLLING_SOP_SCOPES:
            raise PortalError("请在明确的单楼页面维护轮巡 SOP。")
        if not name or len(name) > 160:
            raise PortalError("SOP 名称不能为空且不能超过 160 个字符。")
        steps = self._normalized_steps(payload.get("steps"))
        sop_id = str(payload.get("sop_id") or "").strip() or uuid.uuid4().hex
        expected_version = int(payload.get("expected_version") or 0)
        with self._lock:
            existing = self.state_store.get_document(POLLING_SOP_NAMESPACE, sop_id)
            if existing and expected_version != int(existing.get("version") or 0):
                raise PortalConflictError("SOP 已被其他用户修改，请刷新后重试。")
            if not existing and expected_version:
                raise PortalConflictError("SOP 版本已失效，请刷新后重试。")
            if existing and str(existing.get("scope") or "").strip().upper() not in {"", scope}:
                raise PortalConflictError("轮巡 SOP 不能跨楼栋修改。")
            for item in self.list_sops(scope):
                if (
                    str(item.get("sop_id") or "") != sop_id
                    and str(item.get("scope") or "").strip().upper() == scope
                    and str(item.get("name") or "").strip().casefold() == name.casefold()
                ):
                    raise PortalConflictError("已存在同名轮巡 SOP。")
            now = self._now_text()
            sop = {
                **(existing or {}),
                "sop_id": sop_id,
                "scope": scope,
                "name": name,
                "steps": steps,
                "attachments": copy.deepcopy((existing or {}).get("attachments") or []),
                "version": int((existing or {}).get("version") or 0) + 1,
                "created_at": str((existing or {}).get("created_at") or now),
                "created_by": str((existing or {}).get("created_by") or actor_open_id),
                "updated_at": now,
                "updated_by": str(actor_open_id or ""),
            }
            self.state_store.put_document(POLLING_SOP_NAMESPACE, sop_id, sop)
        return self._public_sop(sop)

    def _sop_directory(self, sop_id: str) -> Path:
        if not re.fullmatch(r"[A-Za-z0-9_-]{8,80}", str(sop_id or "")):
            raise PortalError("SOP ID 无效。")
        path = (self.sop_root / sop_id).resolve()
        if path == self.sop_root or not path.is_relative_to(self.sop_root):
            raise PortalError("SOP 附件路径无效。")
        return path

    def add_sop_attachment(
        self,
        sop_id: str,
        *,
        file_name: str,
        content: bytes,
        expected_version: int,
        actor_open_id: str = "",
    ) -> dict:
        content = bytes(content or b"")
        if not content:
            raise PortalError("SOP 附件内容为空。")
        if len(content) > POLLING_SOP_MAX_FILE_BYTES:
            raise PortalError("SOP 单个附件不能超过 20MB。")
        with self._lock:
            sop = self.get_sop(sop_id, public=False)
            if int(expected_version or 0) != int(sop.get("version") or 0):
                raise PortalConflictError("SOP 已被修改，请刷新后重试。")
            attachments = list(sop.get("attachments") or [])
            if len(attachments) >= POLLING_SOP_MAX_FILES:
                raise PortalError(f"每个 SOP 最多上传 {POLLING_SOP_MAX_FILES} 个附件。")
            if sum(int(item.get("size") or 0) for item in attachments) + len(content) > POLLING_SOP_MAX_TOTAL_BYTES:
                raise PortalError("SOP 附件总大小不能超过 100MB。")
            attachment_id = uuid.uuid4().hex
            safe_name = _safe_file_name(file_name)
            directory = self._sop_directory(sop_id)
            directory.mkdir(parents=True, exist_ok=True)
            path = (directory / f"{attachment_id}_{safe_name}").resolve()
            if not path.is_relative_to(directory):
                raise PortalError("SOP 附件路径无效。")
            temporary = path.with_suffix(path.suffix + ".tmp")
            temporary.write_bytes(content)
            os.replace(temporary, path)
            attachment = {
                "attachment_id": attachment_id,
                "name": safe_name,
                "size": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
                "path": str(path),
                "created_at": self._now_text(),
                "created_by": str(actor_open_id or ""),
            }
            attachments.append(attachment)
            sop.update(
                {
                    "attachments": attachments,
                    "version": int(sop.get("version") or 0) + 1,
                    "updated_at": self._now_text(),
                    "updated_by": str(actor_open_id or ""),
                }
            )
            self.state_store.put_document(POLLING_SOP_NAMESPACE, sop_id, sop)
        return self._public_sop(sop)

    def get_sop_attachment(self, sop_id: str, attachment_id: str) -> tuple[bytes, str]:
        sop = self.get_sop(sop_id, public=False)
        attachment = next(
            (
                item
                for item in sop.get("attachments") or []
                if str(item.get("attachment_id") or "") == str(attachment_id or "")
            ),
            None,
        )
        if not attachment:
            raise PortalNotFoundError("SOP 附件不存在。")
        path = Path(str(attachment.get("path") or "")).resolve()
        directory = self._sop_directory(str(sop.get("sop_id") or ""))
        if not path.is_file() or not path.is_relative_to(directory):
            raise PortalNotFoundError("SOP 附件文件不存在。")
        return path.read_bytes(), str(attachment.get("name") or path.name)

    def delete_sop_attachment(
        self,
        sop_id: str,
        attachment_id: str,
        *,
        expected_version: int,
        actor_open_id: str = "",
    ) -> dict:
        with self._lock:
            sop = self.get_sop(sop_id, public=False)
            if int(expected_version or 0) != int(sop.get("version") or 0):
                raise PortalConflictError("SOP 已被修改，请刷新后重试。")
            attachments = list(sop.get("attachments") or [])
            attachment = next(
                (item for item in attachments if str(item.get("attachment_id") or "") == str(attachment_id or "")),
                None,
            )
            if not attachment:
                raise PortalNotFoundError("SOP 附件不存在。")
            path = Path(str(attachment.get("path") or "")).resolve()
            directory = self._sop_directory(sop_id)
            if path.is_relative_to(directory) and path.is_file():
                path.unlink()
            sop.update(
                {
                    "attachments": [item for item in attachments if item is not attachment],
                    "version": int(sop.get("version") or 0) + 1,
                    "updated_at": self._now_text(),
                    "updated_by": str(actor_open_id or ""),
                }
            )
            self.state_store.put_document(POLLING_SOP_NAMESPACE, sop_id, sop)
        return self._public_sop(sop)

    def delete_sop(self, sop_id: str, *, expected_version: int) -> dict:
        with self._lock:
            sop = self.get_sop(sop_id, public=False)
            if int(expected_version or 0) != int(sop.get("version") or 0):
                raise PortalConflictError("SOP 已被修改，请刷新后重试。")
            directory = self._sop_directory(sop_id)
            if directory.exists():
                shutil.rmtree(directory)
            self.state_store.delete_document(POLLING_SOP_NAMESPACE, sop_id)
        return {"deleted": True, "sop_id": sop_id}

    @staticmethod
    def _person_by_id(people: list[dict], record_id: str) -> dict:
        person = next(
            (
                item
                for item in people or []
                if str(item.get("record_id") or "").strip() == str(record_id or "").strip()
            ),
            None,
        )
        if not person:
            raise PortalError("所选人员已不在当前人员表，请重新选择。")
        return {
            key: str(person.get(key) or "").strip()
            for key in ("record_id", "name", "open_id", "employee_no", "building", "position", "shift")
        }

    def prepare_start(
        self,
        request_payload: dict,
        *,
        job_id: str,
        people: list[dict],
    ) -> dict:
        if (
            str(request_payload.get("work_type") or "").strip() != "polling"
            or str(request_payload.get("action") or "").strip().lower() != "start"
            or not _flag(request_payload.get("_web_action_request"))
        ):
            return {}
        sop = self.get_sop(str(request_payload.get("polling_sop_id") or ""), public=False)
        request_scope = str(request_payload.get("scope") or "").strip().upper()
        if request_scope not in POLLING_SOP_SCOPES or str(
            sop.get("scope") or ""
        ).strip().upper() != request_scope:
            raise PortalError("所选轮巡 SOP 不属于当前楼栋，请重新选择。")
        expected_version = int(request_payload.get("polling_sop_version") or 0)
        if expected_version != int(sop.get("version") or 0):
            raise PortalConflictError("所选 SOP 已修改，请重新选择。")
        if not sop.get("steps") or not sop.get("attachments"):
            raise PortalError("所选 SOP 缺少步骤或附件，不能用于发送开始。")
        runs = request_payload.get("polling_runs")
        runs = runs if isinstance(runs, list) else []
        run_count = int(request_payload.get("polling_run_count") or 0)
        if run_count not in range(1, 7) or len(runs) != run_count:
            raise PortalError("轮巡次数必须为 1–6 且与轮巡组合数量一致。")
        normalized_runs: list[dict] = []
        seen_pairs: set[tuple[str, str]] = set()
        for index, item in enumerate(runs):
            item = item if isinstance(item, dict) else {}
            from_unit = str(item.get("from_unit") or "").strip()
            to_unit = str(item.get("to_unit") or "").strip()
            if from_unit not in POLLING_UNITS or to_unit not in POLLING_UNITS:
                raise PortalError(f"第 {index + 1} 次轮巡设备必须从 1#–6# 中选择。")
            if from_unit == to_unit:
                raise PortalError(f"第 {index + 1} 次轮巡的起点和终点不能相同。")
            pair = (from_unit, to_unit)
            if pair in seen_pairs:
                raise PortalError("轮巡组合不能重复。")
            seen_pairs.add(pair)
            normalized_runs.append(
                {"run_index": index + 1, "from_unit": from_unit, "to_unit": to_unit}
            )
        operator = self._person_by_id(
            people,
            str(request_payload.get("polling_operator_record_id") or ""),
        )
        reviewer = self._person_by_id(
            people,
            str(request_payload.get("polling_reviewer_record_id") or ""),
        )
        if operator["record_id"] == reviewer["record_id"]:
            raise PortalError("操作人和现场审核人不能是同一人。")
        pending_root = (self.work_order_root / "pending").resolve()
        staging = (pending_root / re.sub(r"[^A-Za-z0-9_-]", "_", job_id)).resolve()
        if staging == pending_root or not staging.is_relative_to(pending_root):
            raise PortalError("工单附件暂存路径无效。")
        staging.mkdir(parents=True, exist_ok=True)
        snapshot_attachments: list[dict] = []
        for attachment in sop.get("attachments") or []:
            source = Path(str(attachment.get("path") or "")).resolve()
            if not source.is_file() or not source.is_relative_to(self._sop_directory(str(sop.get("sop_id") or ""))):
                raise PortalError(f"SOP 附件不存在：{attachment.get('name') or '-'}")
            target = (staging / f"{attachment.get('attachment_id')}_{_safe_file_name(attachment.get('name'))}").resolve()
            if not target.is_relative_to(staging):
                raise PortalError("工单附件暂存路径无效。")
            if not target.is_file() or _file_sha256(target) != str(attachment.get("sha256") or ""):
                shutil.copy2(source, target)
            snapshot_attachments.append(
                {
                    "attachment_id": str(attachment.get("attachment_id") or ""),
                    "name": str(attachment.get("name") or target.name),
                    "size": int(attachment.get("size") or target.stat().st_size),
                    "sha256": str(attachment.get("sha256") or _file_sha256(target)),
                    "staged_path": str(target),
                }
            )
        return {
            "polling_work_order_required": True,
            "polling_work_order_spec": {
                "sop_id": str(sop.get("sop_id") or ""),
                "sop_version": int(sop.get("version") or 0),
                "sop_name": str(sop.get("name") or ""),
                "steps": copy.deepcopy(sop.get("steps") or []),
                "attachments": snapshot_attachments,
                "runs": normalized_runs,
                "operator": operator,
                "reviewer": reviewer,
                "initiator_open_id": str(request_payload.get("_auth_open_id") or ""),
                "initiator_name": str(request_payload.get("_auth_user_name") or ""),
            },
            "polling_operator_name": operator["name"],
            "polling_reviewer_name": reviewer["name"],
        }

    def _secret(self) -> bytes:
        with self._lock:
            document = self.state_store.get_document(
                POLLING_WORK_ORDER_SECRET_NAMESPACE,
                POLLING_WORK_ORDER_SECRET_KEY,
            ) or {}
            encoded = str(document.get("secret") or "").strip()
            if not encoded:
                encoded = secrets.token_urlsafe(48)
                self.state_store.put_document(
                    POLLING_WORK_ORDER_SECRET_NAMESPACE,
                    POLLING_WORK_ORDER_SECRET_KEY,
                    {"secret": encoded, "created_at": self._now_text()},
                )
            return encoded.encode("utf-8")

    def role_token(self, target_record_id: str, role: str) -> str:
        payload = f"{target_record_id}.{role}"
        signature = base64.urlsafe_b64encode(
            hmac.new(self._secret(), payload.encode("utf-8"), hashlib.sha256).digest()
        ).decode("ascii").rstrip("=")
        return f"{payload}.{signature}"

    def _resolve_token(self, token: str) -> tuple[dict, str]:
        parts = str(token or "").strip().rsplit(".", 2)
        if len(parts) != 3 or parts[1] not in {"operator", "reviewer"}:
            raise PollingWorkOrderTokenError("工单链接无效或已失效。")
        target_record_id, role, _signature = parts
        expected = self.role_token(target_record_id, role)
        if not hmac.compare_digest(expected, str(token or "").strip()):
            raise PollingWorkOrderTokenError("工单链接无效或已失效。")
        group = self.get_group(target_record_id)
        if str(group.get("state") or "") in {"completed", "cancelled", "stopped"}:
            raise PollingWorkOrderTokenError("该工单链接已失效。")
        token_hash = hashlib.sha256(str(token).encode("utf-8")).hexdigest()
        if not hmac.compare_digest(
            token_hash,
            str((group.get("token_hashes") or {}).get(role) or ""),
        ):
            raise PollingWorkOrderTokenError("工单链接无效或已失效。")
        return group, role

    def get_group(self, target_record_id: str) -> dict:
        group = self.state_store.get_document(
            POLLING_WORK_ORDER_NAMESPACE,
            str(target_record_id or "").strip(),
        )
        if not isinstance(group, dict):
            raise PortalNotFoundError("轮巡工单不存在。")
        return copy.deepcopy(group)

    def validate_group_token(self, token: str, target_record_id: str) -> None:
        group, _role = self._resolve_token(token)
        if str(group.get("target_record_id") or "") != str(
            target_record_id or ""
        ).strip():
            raise PollingWorkOrderTokenError("工单链接无效或已失效。")

    def create_group(
        self,
        prepared: dict,
        *,
        target_record_id: str,
        title: str,
        public_base_url: str,
    ) -> dict:
        target_record_id = str(target_record_id or "").strip()
        spec = prepared.get("polling_work_order_spec")
        if not prepared.get("polling_work_order_required") or not isinstance(spec, dict):
            return {}
        with self._lock:
            existing = self.state_store.get_document(
                POLLING_WORK_ORDER_NAMESPACE, target_record_id
            )
            if isinstance(existing, dict):
                return self.group_with_links(existing, public_base_url)
            directory_name = hashlib.sha256(target_record_id.encode("utf-8")).hexdigest()[:24]
            directory = (self.work_order_root / "groups" / directory_name).resolve()
            groups_root = (self.work_order_root / "groups").resolve()
            if directory == groups_root or not directory.is_relative_to(groups_root):
                raise PortalError("工单附件路径无效。")
            directory.mkdir(parents=True, exist_ok=True)
            attachments: list[dict] = []
            seen_hashes: set[str] = set()
            pending_root = (self.work_order_root / "pending").resolve()
            staging_directories: set[Path] = set()
            for item in spec.get("attachments") or []:
                digest = str(item.get("sha256") or "").strip()
                if digest in seen_hashes:
                    continue
                source = Path(str(item.get("staged_path") or "")).resolve()
                if not source.is_file() or not source.is_relative_to(pending_root):
                    raise PortalError(f"工单附件暂存文件不存在：{item.get('name') or '-'}")
                staging_directories.add(source.parent)
                target = (directory / f"{item.get('attachment_id')}_{_safe_file_name(item.get('name'))}").resolve()
                if not target.is_relative_to(directory):
                    raise PortalError("工单附件路径无效。")
                if not target.is_file() or _file_sha256(target) != digest:
                    shutil.copy2(source, target)
                attachments.append(
                    {
                        "attachment_id": str(item.get("attachment_id") or ""),
                        "name": str(item.get("name") or target.name),
                        "size": int(item.get("size") or target.stat().st_size),
                        "sha256": digest or _file_sha256(target),
                        "path": str(target),
                    }
                )
                seen_hashes.add(digest)
            flattened: list[dict] = []
            runs = list(spec.get("runs") or [])
            for run_index, run in enumerate(runs, start=1):
                for step_index, template_step in enumerate(spec.get("steps") or [], start=1):
                    content = str(template_step.get("content") or "")
                    content = content.replace("{{from}}", str(run.get("from_unit") or ""))
                    content = content.replace("{{to}}", str(run.get("to_unit") or ""))
                    flattened.append(
                        {
                            "step_key": f"{run_index}:{step_index}",
                            "global_index": len(flattened),
                            "run_index": run_index,
                            "run_count": len(runs),
                            "run_label": f"{run.get('from_unit')}→{run.get('to_unit')}",
                            "step_index": step_index,
                            "step_count": len(spec.get("steps") or []),
                            "content": content,
                            "operator_required": bool(template_step.get("operator_required")),
                            "reviewer_required": bool(template_step.get("reviewer_required")),
                            "operator_confirmation": {},
                            "reviewer_confirmation": {},
                        }
                    )
            operator_token = self.role_token(target_record_id, "operator")
            reviewer_token = self.role_token(target_record_id, "reviewer")
            now = self._now_text()
            group = {
                "group_id": target_record_id,
                "target_record_id": target_record_id,
                "title": str(title or target_record_id),
                "sop_id": str(spec.get("sop_id") or ""),
                "sop_version": int(spec.get("sop_version") or 0),
                "sop_name": str(spec.get("sop_name") or ""),
                "runs": runs,
                "steps": flattened,
                "attachments": attachments,
                "operator": copy.deepcopy(spec.get("operator") or {}),
                "reviewer": copy.deepcopy(spec.get("reviewer") or {}),
                "initiator_open_id": str(spec.get("initiator_open_id") or ""),
                "initiator_name": str(spec.get("initiator_name") or ""),
                "token_hashes": {
                    "operator": hashlib.sha256(operator_token.encode("utf-8")).hexdigest(),
                    "reviewer": hashlib.sha256(reviewer_token.encode("utf-8")).hexdigest(),
                },
                "current_index": 0,
                "version": 1,
                "state": "active",
                "uploaded_file_tokens": [],
                "last_error": "",
                "notifications": {},
                "created_at": now,
                "updated_at": now,
            }
            self.state_store.put_document(
                POLLING_WORK_ORDER_NAMESPACE, target_record_id, group
            )
            for staging in staging_directories:
                if staging.parent == pending_root and staging.is_dir():
                    shutil.rmtree(staging)
        return self.group_with_links(group, public_base_url)

    def group_with_links(self, group: dict, public_base_url: str) -> dict:
        result = copy.deepcopy(group or {})
        base = str(public_base_url or "").rstrip("/")
        if base:
            result["operator_link"] = (
                f"{base}/polling-work-order?token={quote(self.role_token(str(group.get('target_record_id') or ''), 'operator'), safe='')}"
            )
            result["reviewer_link"] = (
                f"{base}/polling-work-order?token={quote(self.role_token(str(group.get('target_record_id') or ''), 'reviewer'), safe='')}"
            )
        return result

    @staticmethod
    def _step_public(step: dict, current_index: int) -> dict:
        result = copy.deepcopy(step)
        index = int(result.get("global_index") or 0)
        result["position"] = "current" if index == current_index else "previous" if index < current_index else "next"
        result["operator_confirmed"] = bool(result.pop("operator_confirmation", {}))
        result["reviewer_confirmed"] = bool(result.pop("reviewer_confirmation", {}))
        return result

    def session(self, token: str) -> dict:
        group, role = self._resolve_token(token)
        steps = list(group.get("steps") or [])
        current = min(int(group.get("current_index") or 0), max(0, len(steps) - 1))
        visible_indexes = sorted(
            {index for index in (current - 1, current, current + 1) if 0 <= index < len(steps)}
        )
        return {
            "group_id": str(group.get("group_id") or ""),
            "title": str(group.get("title") or ""),
            "sop_name": str(group.get("sop_name") or ""),
            "role": role,
            "role_label": "操作人" if role == "operator" else "现场审核人",
            "assigned_person": copy.deepcopy(group.get(role) or {}),
            "state": str(group.get("state") or ""),
            "version": int(group.get("version") or 0),
            "current_index": int(group.get("current_index") or 0),
            "total_steps": len(steps),
            "steps": [self._step_public(steps[index], int(group.get("current_index") or 0)) for index in visible_indexes],
            "last_error": str(group.get("last_error") or ""),
        }

    def confirm(
        self,
        token: str,
        *,
        step_key: str,
        expected_version: int,
        actual_open_id: str = "",
        actual_name: str = "",
    ) -> dict:
        with self._lock:
            group, role = self._resolve_token(token)
            if int(expected_version or 0) != int(group.get("version") or 0):
                steps = list(group.get("steps") or [])
                prior = next((item for item in steps if str(item.get("step_key") or "") == str(step_key or "")), None)
                if prior and prior.get(f"{role}_confirmation"):
                    return self.session(token)
                raise PortalConflictError("工单状态已更新，请刷新后重试。")
            steps = list(group.get("steps") or [])
            current_index = int(group.get("current_index") or 0)
            if current_index >= len(steps):
                return self.session(token)
            step = steps[current_index]
            if str(step.get("step_key") or "") != str(step_key or ""):
                raise PortalConflictError("只能确认当前步骤。")
            if not bool(step.get(f"{role}_required")):
                raise PortalConflictError("当前步骤不需要该角色确认。")
            if role == "reviewer" and step.get("operator_required") and not step.get("operator_confirmation"):
                raise PortalConflictError("请先等待操作人确认。")
            if not step.get(f"{role}_confirmation"):
                step[f"{role}_confirmation"] = {
                    "assigned_record_id": str((group.get(role) or {}).get("record_id") or ""),
                    "assigned_name": str((group.get(role) or {}).get("name") or ""),
                    "actual_open_id": str(actual_open_id or ""),
                    "actual_name": str(actual_name or ""),
                    "confirmed_at": self._now_text(),
                }
            required_done = (
                (not step.get("operator_required") or bool(step.get("operator_confirmation")))
                and (not step.get("reviewer_required") or bool(step.get("reviewer_confirmation")))
            )
            if required_done:
                group["current_index"] = current_index + 1
                if int(group["current_index"]) >= len(steps):
                    group["state"] = "upload_pending"
            group.update(
                {
                    "steps": steps,
                    "version": int(group.get("version") or 0) + 1,
                    "updated_at": self._now_text(),
                    "last_error": "",
                }
            )
            self.state_store.put_document(
                POLLING_WORK_ORDER_NAMESPACE,
                str(group.get("target_record_id") or ""),
                group,
            )
        return self.session(token)

    def pending_upload_groups(self) -> list[dict]:
        return [
            copy.deepcopy(document.get("payload") or {})
            for document in self.state_store.list_documents(POLLING_WORK_ORDER_NAMESPACE)
            if isinstance(document.get("payload"), dict)
            and str((document.get("payload") or {}).get("state") or "") == "upload_pending"
        ]

    def open_groups(self) -> list[dict]:
        return [
            copy.deepcopy(document.get("payload") or {})
            for document in self.state_store.list_documents(POLLING_WORK_ORDER_NAMESPACE)
            if isinstance(document.get("payload"), dict)
            and str((document.get("payload") or {}).get("state") or "")
            in {"active", "upload_pending"}
        ]

    def mark_upload_result(
        self,
        target_record_id: str,
        *,
        success: bool,
        file_tokens: list[str] | None = None,
        error: str = "",
    ) -> dict:
        with self._lock:
            group = self.get_group(target_record_id)
            group.update(
                {
                    "state": "completed" if success else "upload_pending",
                    "uploaded_file_tokens": list(dict.fromkeys(file_tokens or group.get("uploaded_file_tokens") or [])),
                    "last_error": "" if success else str(error or "工单附件上传失败。"),
                    "upload_completed_at": self._now_text() if success else "",
                    "updated_at": self._now_text(),
                    "version": int(group.get("version") or 0) + 1,
                }
            )
            self.state_store.put_document(POLLING_WORK_ORDER_NAMESPACE, target_record_id, group)
        return group

    def mark_upload_progress(
        self,
        target_record_id: str,
        *,
        token_by_sha256: dict[str, str],
        error: str = "",
    ) -> dict:
        with self._lock:
            group = self.get_group(target_record_id)
            group["uploaded_by_sha256"] = {
                str(key): str(value)
                for key, value in (token_by_sha256 or {}).items()
                if str(key).strip() and str(value).strip()
            }
            group["last_error"] = str(error or "")
            group["updated_at"] = self._now_text()
            self.state_store.put_document(
                POLLING_WORK_ORDER_NAMESPACE, target_record_id, group
            )
        return group

    def update_notifications(self, target_record_id: str, notifications: dict) -> dict:
        with self._lock:
            group = self.get_group(target_record_id)
            group["notifications"] = copy.deepcopy(notifications or {})
            group["updated_at"] = self._now_text()
            self.state_store.put_document(POLLING_WORK_ORDER_NAMESPACE, target_record_id, group)
        return group

    def cancel_group(self, target_record_id: str, *, reason: str) -> None:
        with self._lock:
            try:
                group = self.get_group(target_record_id)
            except PortalNotFoundError:
                return
            if str(group.get("state") or "") == "completed":
                return
            group.update(
                {
                    "state": "cancelled",
                    "cancel_reason": str(reason or "target_terminal"),
                    "updated_at": self._now_text(),
                    "version": int(group.get("version") or 0) + 1,
                }
            )
            self.state_store.put_document(POLLING_WORK_ORDER_NAMESPACE, target_record_id, group)

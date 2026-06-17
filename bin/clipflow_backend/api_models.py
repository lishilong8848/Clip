# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, TypeVar

from pydantic import BaseModel, Field, ValidationError


class APIModel(BaseModel):
    """Pydantic base model for public backend APIs.

    The current portal payloads still carry many feature-specific fields. Models
    therefore allow extra keys while locking down the stable envelope fields.
    """

    class Config:
        extra = "allow"
        arbitrary_types_allowed = True

    def to_payload(self) -> dict[str, Any]:
        if hasattr(self, "model_dump"):
            return self.model_dump(exclude_none=False)  # type: ignore[attr-defined]
        return self.dict(exclude_none=False)


class WorkbenchActionRequest(APIModel):
    scope: str = "ALL"
    action: str = ""
    work_type: str = "maintenance"
    notice_type: str = ""
    record_id: str = ""
    source_record_id: str = ""
    target_record_id: str = ""
    feishu_record_id: str = ""
    raw_record_id: str = ""
    lan_source_record_id: str = ""
    active_item_id: str = ""
    manual_id: str = ""
    manual_key: str = ""
    operation_id: str = ""


class OngoingDeleteRequest(APIModel):
    scope: str = "ALL"
    work_type: str = "maintenance"
    notice_type: str = ""
    active_item_id: str = ""
    record_id: str = ""
    target_record_id: str = ""
    source_record_id: str = ""


class NoticeUndoApplyRequest(APIModel):
    scope: str = "ALL"


class NoticeWorkTypeOverrideRequest(APIModel):
    scope: str = "ALL"
    record_id: str = ""
    source_work_type: str = "maintenance"
    target_work_type: str = "change"


class PermissionRequestCreate(APIModel):
    scopes: list[str] = Field(default_factory=list)
    reason: str = ""


class PermissionRequestConfirm(APIModel):
    request_id: str = ""
    code: str = ""


class HandoverLinksSaveRequest(APIModel):
    links: dict[str, Any] = Field(default_factory=dict)
    password: str = ""


class HandoverLinksAuthRequest(APIModel):
    password: str = ""


class HandoverPasswordResetConfirmRequest(APIModel):
    reset_id: str = ""
    code: str = ""
    new_password: str = ""


class AuthPermissionsSaveRequest(APIModel):
    users: list[dict[str, Any]] = Field(default_factory=list)


class GenerateTemplatesRequest(APIModel):
    scope: str = "ALL"
    drafts: list[dict[str, Any]] = Field(default_factory=list)


class NoticeMemoryImportRequest(APIModel):
    scope: str = "ALL"
    text: str = ""


class NoticeMemoryHistoryScanRequest(APIModel):
    work_types: list[str] | str = Field(default_factory=list)
    months: int = 3


class NoticeMemoryHistorySaveRequest(APIModel):
    matches: list[dict[str, Any]] = Field(default_factory=list)


class EngineerMopBindRequest(APIModel):
    scope: str = "ALL"
    notice_key: str = ""
    notice_title: str = ""
    notice_status: str = ""
    source_record_id: str = ""
    target_record_id: str = ""
    active_item_id: str = ""
    mop_app_token: str = ""
    mop_table_id: str = ""
    mop_record_id: str = ""
    mop_title: str = ""
    mop_attachment_token: str = ""
    mop_attachment_name: str = ""
    selected_sheet: str = ""


class EngineerMopSettingsSaveRequest(APIModel):
    mop_app_token: str = ""
    mop_table_id: str = ""
    mop_view_id: str = ""
    mop_title_field: str = "文件名"
    mop_attachment_field: str = "文件"


class SignatureSaveRequest(APIModel):
    record_id: str = ""
    token: str = ""
    signature_png: str = ""
    signer_name: str = ""


class SignatureSendLinkRequest(APIModel):
    record_id: str = ""
    signer_name: str = ""
    scope: str = ""


class EngineerMopFillRequest(APIModel):
    scope: str = "ALL"
    local_file_path: str = ""
    mop_record_id: str = ""
    mop_title: str = ""
    sheet_name: str = ""
    fields: list[dict[str, Any]] = Field(default_factory=list)
    checkboxes: list[dict[str, Any]] = Field(default_factory=list)
    signatures: list[dict[str, Any]] = Field(default_factory=list)


class EngineerMopResetRequest(APIModel):
    scope: str = "ALL"
    filled_file_path: str = ""
    mop_record_id: str = ""
    file_token: str = ""
    file_name: str = ""


class ChangeTargetLookupRequest(APIModel):
    scope: str = "ALL"
    title: str = ""
    start_time: str = ""
    end_time: str = ""
    action: str = "update"
    content: str = ""
    reason: str = ""
    impact: str = ""
    progress: str = ""
    text: str = ""


class NoticeTargetLookupRequest(ChangeTargetLookupRequest):
    work_type: str = "change"


class ChangeTargetConfirmRequest(ChangeTargetLookupRequest):
    record_id: str = ""


class SendGeneratedRequest(APIModel):
    scope: str = "ALL"
    items: list[dict[str, Any]] = Field(default_factory=list)


class MockPressureRequest(APIModel):
    count: int = 10
    concurrency: int = 5
    scopes: list[str] | str = Field(default_factory=list)
    per_scope: int = 0
    scenario: str = "accepted"
    include_site_photos: bool = False
    site_photo_count: int = 1
    site_photo_kb: int = 32
    max_submit_average_ms: float = 300.0
    max_total_seconds: float = 20.0
    max_failed: int = 0


class QtCommandRequest(APIModel):
    command: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)


class QtDialogResultRequest(APIModel):
    session_id: str = ""
    status: str = "completed"
    result: dict[str, Any] = Field(default_factory=dict)
    payload: dict[str, Any] = Field(default_factory=dict)


class QtDialogSessionRequest(APIModel):
    session_id: str = ""
    type: str = ""
    dialog_type: str = ""
    action_type: str = ""
    record_id: str = ""
    source_record_id: str = ""
    target_record_id: str = ""
    active_item_id: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)


class QtClipboardAckRequest(APIModel):
    ok: bool = True
    status: str = ""


class QtEventAckRequest(APIModel):
    ok: bool = True
    error: str = ""


class QtLocalHeartbeatRequest(APIModel):
    payload: dict[str, Any] = Field(default_factory=dict)


class QtClipboardEventRequest(APIModel):
    content: str = ""
    ts: int | float | None = None
    source: str = "clipboard"
    target_record_id: str = ""


class QtOngoingSnapshotRequest(APIModel):
    items: list[dict[str, Any]] = Field(default_factory=list)


class QtActiveItemsDeltaRequest(APIModel):
    upserts: list[Any] = Field(default_factory=list)
    items: list[Any] = Field(default_factory=list)
    deletes: list[Any] = Field(default_factory=list)
    deleted: list[Any] = Field(default_factory=list)


class QtJobProgressRequest(APIModel):
    phase: str = ""
    status: str = ""
    message: str = ""
    error: str = ""
    queue_position: int | None = None
    message_queue_position: int | None = None
    upload_queue_position: int | None = None
    qt_queue_position: int | None = None


class QtJobResultRequest(APIModel):
    success: bool = False
    message: str = ""


class JobMarkStuckFailedRequest(APIModel):
    reason: str = "管理员手动标记卡住任务，请核对后重试。"
    record_id: str = ""
    target_record_id: str = ""
    source_record_id: str = ""
    active_item_id: str = ""


ModelT = TypeVar("ModelT", bound=APIModel)


def parse_api_model(model_cls: type[ModelT], payload: dict[str, Any]) -> ModelT:
    try:
        if hasattr(model_cls, "model_validate"):
            return model_cls.model_validate(payload)  # type: ignore[attr-defined]
        return model_cls.parse_obj(payload)
    except ValidationError as exc:
        raise ValueError(_format_validation_error(exc)) from exc


def _format_validation_error(exc: ValidationError) -> str:
    errors = []
    for item in exc.errors():
        loc = ".".join(str(part) for part in item.get("loc", []) if part != "__root__")
        message = str(item.get("msg") or "字段无效")
        errors.append(f"{loc or '请求体'}: {message}")
    return "请求参数无效：" + "；".join(errors)

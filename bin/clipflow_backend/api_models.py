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
    active_item_id: str = ""
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


class ChangeTargetLookupRequest(APIModel):
    scope: str = "ALL"
    title: str = ""
    start_time: str = ""
    end_time: str = ""
    action: str = "update"


class SendGeneratedRequest(APIModel):
    scope: str = "ALL"
    items: list[dict[str, Any]] = Field(default_factory=list)


class MockPressureRequest(APIModel):
    count: int = 10
    concurrency: int = 5
    scopes: list[str] | str = Field(default_factory=list)
    per_scope: int = 0
    scenario: str = "accepted"


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
    record_id: str = ""
    active_item_id: str = ""


class QtNoticeUploadRequest(APIModel):
    pass


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

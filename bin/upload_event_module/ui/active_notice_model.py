# -*- coding: utf-8 -*-
from __future__ import annotations

import uuid
import weakref
from typing import Any

from PyQt6.QtCore import QAbstractListModel, QModelIndex, QSize, Qt, pyqtSignal

from lan_bitable_template_portal.identity_utils import (
    canonical_source_record_id,
    canonical_target_record_id,
)
from ..core.parser import extract_event_info
from .display_state import build_notice_display_snapshot


class ActiveNoticeModel(QAbstractListModel):
    """Qt model projection for active notice records.

    The model is the primary source for the Qt active-notice list when the
    model/delegate path is enabled.
    """

    DataRole = int(Qt.ItemDataRole.UserRole) + 1
    ActiveItemIdRole = DataRole + 1
    RecordIdRole = DataRole + 2
    NoticeTypeRole = DataRole + 3
    TitleRole = DataRole + 4
    SubtitleRole = DataRole + 5
    OriginRole = DataRole + 6
    UploadedRole = DataRole + 7
    UploadingRole = DataRole + 8
    PlaceholderRole = DataRole + 9

    recordActivated = pyqtSignal(dict)
    actionRequested = pyqtSignal(dict, str)
    todayProgressRequested = pyqtSignal(dict, str)
    deleteRequested = pyqtSignal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._records: list[dict[str, Any]] = []
        self._identity_to_row: dict[str, int] = {}
        self._record_id_to_row: dict[str, int] = {}
        self._source_record_id_to_row: dict[str, int] = {}
        self._active_item_id_to_row: dict[str, int] = {}

    @staticmethod
    def identity_for_record(record: dict[str, Any] | None) -> str:
        if not isinstance(record, dict):
            return ""
        for key in ("active_item_id", "target_record_id", "source_record_id"):
            value = str(record.get(key) or "").strip()
            if value:
                return f"{key}:{value}"
        target_id = canonical_target_record_id(record)
        if target_id:
            return f"target_record_id:{target_id}"
        seed = "|".join(
            str(record.get(key) or "").strip()
            for key in ("notice_type", "match_key", "match_title", "text")
        )
        if not seed.strip("|"):
            return ""
        return f"generated:{uuid.uuid5(uuid.NAMESPACE_URL, seed).hex}"

    @staticmethod
    def is_placeholder_record(record: dict[str, Any] | None) -> bool:
        if not isinstance(record, dict):
            return True
        return bool(record.get("_is_placeholder_record", True))

    @staticmethod
    def is_uploaded_record(record: dict[str, Any] | None) -> bool:
        if not isinstance(record, dict):
            return False
        return not bool(record.get("_has_unuploaded_changes", True))

    @staticmethod
    def is_uploading_record(record: dict[str, Any] | None) -> bool:
        if not isinstance(record, dict):
            return False
        return bool(record.get("_upload_in_progress"))

    @staticmethod
    def has_upload_error(record: dict[str, Any] | None) -> bool:
        if not isinstance(record, dict):
            return False
        return bool(str(record.get("_last_upload_error") or "").strip())

    @staticmethod
    def is_today_progress_syncing(record: dict[str, Any] | None) -> bool:
        if not isinstance(record, dict):
            return False
        return bool(record.get("_today_in_progress_syncing"))

    @classmethod
    def action_for_record(cls, record: dict[str, Any] | None) -> str:
        if not isinstance(record, dict):
            return ""
        if cls.is_uploaded_record(record) or cls.is_uploading_record(record):
            return ""
        try:
            info = extract_event_info(record.get("text", "")) or {}
        except Exception:
            info = {}
        if info.get("status") == "结束":
            return "end"
        if not cls.is_placeholder_record(record):
            return "update"
        return "upload"

    @classmethod
    def action_label_for_record(cls, record: dict[str, Any] | None) -> str:
        if cls.is_uploading_record(record):
            return "上传中"
        if cls.has_upload_error(record) and not cls.is_uploaded_record(record):
            return "失败可重试"
        if cls.is_uploaded_record(record):
            return "已上传"
        action = cls.action_for_record(record)
        if action == "end":
            return "结束"
        if action == "update":
            return "更新"
        if action == "upload":
            return "上传"
        return ""

    @classmethod
    def supports_today_progress(cls, record: dict[str, Any] | None) -> bool:
        if not isinstance(record, dict):
            return False
        notice_type = str(record.get("notice_type") or "").strip()
        record_id = canonical_target_record_id(record)
        return (
            notice_type in ("设备变更", "变更通告")
            and bool(record_id)
            and not cls.is_placeholder_record(record)
        )

    @staticmethod
    def normalize_today_progress_state(state: Any) -> str:
        text = str(state or "").strip().lower()
        if text in ("yes", "no", "unknown"):
            return text
        if text in ("是", "在进行"):
            return "yes"
        if text in ("否", "未进行"):
            return "no"
        return "unknown"

    @classmethod
    def today_progress_label(cls, record: dict[str, Any] | None) -> str:
        if cls.is_today_progress_syncing(record):
            return "同步中"
        if isinstance(record, dict) and str(record.get("_today_in_progress_error") or "").strip():
            return "重试同步"
        state = cls.normalize_today_progress_state(
            (record or {}).get("today_in_progress_state") if isinstance(record, dict) else ""
        )
        if state == "yes":
            return "在进行"
        if state == "no":
            return "未进行"
        return "是否在进行"

    @classmethod
    def next_today_progress_state(cls, record: dict[str, Any] | None) -> str:
        state = cls.normalize_today_progress_state(
            (record or {}).get("today_in_progress_state") if isinstance(record, dict) else ""
        )
        return "no" if state == "yes" else "yes"

    def rowCount(self, parent=QModelIndex()) -> int:  # noqa: N802
        if parent.isValid():
            return 0
        return len(self._records)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):  # noqa: A003
        if not index.isValid():
            return None
        row = index.row()
        if row < 0 or row >= len(self._records):
            return None
        record = self._records[row]
        if role == self.DataRole:
            return dict(record)
        if role == self.ActiveItemIdRole:
            return str(record.get("active_item_id") or "")
        if role == self.RecordIdRole:
            return canonical_target_record_id(record) or str(record.get("record_id") or "")
        if role == self.NoticeTypeRole:
            return str(record.get("notice_type") or "")
        if role == self.OriginRole:
            return "portal" if bool(record.get("lan_created_from_portal")) else str(
                record.get("origin") or ""
            )
        if role == self.UploadedRole:
            return self.is_uploaded_record(record)
        if role == self.UploadingRole:
            return self.is_uploading_record(record)
        if role == self.PlaceholderRole:
            return self.is_placeholder_record(record)
        if role in (Qt.ItemDataRole.DisplayRole, self.TitleRole, self.SubtitleRole):
            snapshot = build_notice_display_snapshot(record)
            if role == self.SubtitleRole:
                return snapshot.get("subtitle") or ""
            return snapshot.get("title") or "未知标题"
        return None

    def roleNames(self):  # noqa: N802
        return {
            self.DataRole: b"record",
            self.ActiveItemIdRole: b"activeItemId",
            self.RecordIdRole: b"recordId",
            self.NoticeTypeRole: b"noticeType",
            self.TitleRole: b"title",
            self.SubtitleRole: b"subtitle",
            self.OriginRole: b"origin",
            self.UploadedRole: b"uploaded",
            self.UploadingRole: b"uploading",
            self.PlaceholderRole: b"placeholder",
        }

    def records(self) -> list[dict[str, Any]]:
        return [dict(record) for record in self._records]

    def record_at(self, row: int) -> dict[str, Any] | None:
        if row < 0 or row >= len(self._records):
            return None
        return dict(self._records[row])

    def row_for_identity(self, identity: str) -> int:
        identity = str(identity or "").strip()
        if not identity:
            return -1
        return int(self._identity_to_row.get(identity, -1))

    def row_for_record(self, record: dict[str, Any] | None) -> int:
        return self.row_for_identity(self.identity_for_record(record))

    def row_for_record_id(self, record_id: str) -> int:
        record_id = str(record_id or "").strip()
        if not record_id:
            return -1
        return int(self._record_id_to_row.get(record_id, -1))

    def row_for_source_record_id(self, source_record_id: str) -> int:
        source_record_id = str(source_record_id or "").strip()
        if not source_record_id:
            return -1
        return int(self._source_record_id_to_row.get(source_record_id, -1))

    def row_for_active_item_id(self, active_item_id: str) -> int:
        active_item_id = str(active_item_id or "").strip()
        if not active_item_id:
            return -1
        return int(self._active_item_id_to_row.get(active_item_id, -1))

    def record_by_record_id(self, record_id: str) -> dict[str, Any] | None:
        row = self.row_for_record_id(record_id)
        return self.record_at(row) if row >= 0 else None

    def record_by_source_record_id(self, source_record_id: str) -> dict[str, Any] | None:
        row = self.row_for_source_record_id(source_record_id)
        return self.record_at(row) if row >= 0 else None

    def record_by_active_item_id(self, active_item_id: str) -> dict[str, Any] | None:
        row = self.row_for_active_item_id(active_item_id)
        return self.record_at(row) if row >= 0 else None

    def replace_records(self, records: list[dict[str, Any]] | None) -> None:
        normalized = [
            dict(record)
            for record in (records or [])
            if isinstance(record, dict) and self.identity_for_record(record)
        ]
        self.beginResetModel()
        self._records = normalized
        self._rebuild_index()
        self.endResetModel()

    def upsert_record(self, record: dict[str, Any] | None, *, row: int | None = None) -> bool:
        if not isinstance(record, dict):
            return False
        identity = self.identity_for_record(record)
        if not identity:
            return False
        current_row = self._identity_to_row.get(identity)
        normalized = dict(record)
        if current_row is not None and 0 <= current_row < len(self._records):
            self._records[current_row] = normalized
            self._rebuild_index()
            model_index = self.index(current_row, 0)
            self.dataChanged.emit(model_index, model_index, [])
            if row is not None and row != current_row:
                self.move_record(identity, row)
            return True
        insert_row = len(self._records) if row is None else max(0, min(int(row), len(self._records)))
        self.beginInsertRows(QModelIndex(), insert_row, insert_row)
        self._records.insert(insert_row, normalized)
        self.endInsertRows()
        self._rebuild_index()
        return True

    def remove_record(self, record: dict[str, Any] | None) -> bool:
        identity = self.identity_for_record(record)
        if not identity:
            return False
        row = self._identity_to_row.get(identity)
        if row is None or row < 0 or row >= len(self._records):
            return False
        self.beginRemoveRows(QModelIndex(), row, row)
        self._records.pop(row)
        self.endRemoveRows()
        self._rebuild_index()
        return True

    def move_record(self, identity: str, target_row: int) -> bool:
        identity = str(identity or "").strip()
        source_row = self._identity_to_row.get(identity)
        if source_row is None or source_row < 0 or source_row >= len(self._records):
            return False
        target_row = max(0, min(int(target_row or 0), len(self._records) - 1))
        if source_row == target_row:
            return True
        record = self._records.pop(source_row)
        self._records.insert(target_row, record)
        self.layoutChanged.emit()
        self._rebuild_index()
        return True

    def _rebuild_index(self) -> None:
        self._identity_to_row = {}
        self._record_id_to_row = {}
        self._source_record_id_to_row = {}
        self._active_item_id_to_row = {}
        for row, record in enumerate(self._records):
            identity = self.identity_for_record(record)
            if identity:
                self._identity_to_row[identity] = row
            target_id = canonical_target_record_id(record)
            raw_record_id = str(record.get("record_id") or "").strip()
            record_id = target_id or raw_record_id
            if record_id:
                self._record_id_to_row.setdefault(record_id, row)
            source_record_id = canonical_source_record_id(record)
            if source_record_id:
                self._source_record_id_to_row.setdefault(source_record_id, row)
            active_item_id = str(record.get("active_item_id") or "").strip()
            if active_item_id:
                self._active_item_id_to_row.setdefault(active_item_id, row)


class ActiveNoticeListRoute:
    """Lightweight identity route for model-backed active notice lists.

    The production path renders active notices with QListView + model/delegate.
    Older helpers still pass around a "list" identity to decide whether a notice
    belongs to the event or non-event tab. This route keeps that identity without
    allocating a hidden item container in the hot path.
    """

    def __init__(self, name: str):
        self.name = str(name or "").strip()

    def clear(self) -> None:
        return None

    def count(self) -> int:
        return 0

    def item(self, _row: int):
        return None

    def row(self, item) -> int:
        if isinstance(item, ActiveNoticeModelItem) and item.listWidget() is self:
            return item.row()
        return -1

    def __repr__(self) -> str:
        return f"<ActiveNoticeListRoute {self.name or '-'}>"


class ActiveNoticeModelItem:
    """Lightweight handle used when the active list is model-backed only."""

    def __init__(self, list_widget, model: ActiveNoticeModel, identity: str):
        self._list_widget = list_widget
        self._model_ref = weakref.ref(model)
        self._identity = str(identity or "").strip()
        self._size_hint = QSize(420, 88)

    def listWidget(self):
        return self._list_widget

    def _model(self) -> ActiveNoticeModel | None:
        model = self._model_ref()
        return model if isinstance(model, ActiveNoticeModel) else None

    def identity(self) -> str:
        return self._identity

    def row(self) -> int:
        model = self._model()
        if model is None:
            return -1
        return model.row_for_identity(self._identity)

    def is_valid(self) -> bool:
        return self.row() >= 0

    def data(self, role=Qt.ItemDataRole.UserRole):  # noqa: A003
        model = self._model()
        row = self.row()
        if model is None or row < 0:
            return None
        record = model.record_at(row)
        if role == Qt.ItemDataRole.UserRole:
            return record
        index = model.index(row, 0)
        if not index.isValid():
            return None
        return model.data(index, role)

    def setData(self, role, value):  # noqa: N802,A003
        if role != Qt.ItemDataRole.UserRole or not isinstance(value, dict):
            return False
        model = self._model()
        row = self.row()
        if model is None or row < 0:
            return False
        updated = model.upsert_record(dict(value), row=row)
        if updated:
            self._identity = model.identity_for_record(value) or self._identity
        return bool(updated)

    def setSizeHint(self, size):  # noqa: N802
        if isinstance(size, QSize):
            self._size_hint = QSize(size)

    def sizeHint(self):  # noqa: N802
        return QSize(self._size_hint)

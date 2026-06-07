# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from PyQt6.QtCore import QAbstractListModel, QModelIndex, Qt, pyqtSignal


class DeletedNoticeModel(QAbstractListModel):
    """Model projection for recently deleted notices that can be restored."""

    DataRole = int(Qt.ItemDataRole.UserRole) + 1
    UndoIdRole = DataRole + 1
    TitleRole = DataRole + 2
    MetaRole = DataRole + 3
    EmptyRole = DataRole + 4

    undoRequested = pyqtSignal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._records: list[dict[str, Any]] = []

    @staticmethod
    def _meta_for_record(record: dict[str, Any]) -> str:
        return str(record.get("_meta") or record.get("meta") or "").strip()

    @staticmethod
    def _title_for_record(record: dict[str, Any]) -> str:
        return str(record.get("title") or "未命名通告").strip()

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
        if role == self.UndoIdRole:
            return str(record.get("undo_id") or "")
        if role == self.TitleRole:
            return self._title_for_record(record)
        if role == self.MetaRole:
            return self._meta_for_record(record)
        if role == self.EmptyRole:
            return bool(record.get("_empty"))
        if role == Qt.ItemDataRole.DisplayRole:
            title = self._title_for_record(record)
            meta = self._meta_for_record(record)
            return f"{title}\n{meta}" if meta else title
        return None

    def records(self) -> list[dict[str, Any]]:
        return [dict(record) for record in self._records]

    def record_at(self, row: int) -> dict[str, Any] | None:
        if row < 0 or row >= len(self._records):
            return None
        return dict(self._records[row])

    def replace_records(self, records: list[dict[str, Any]] | None) -> None:
        normalized = [
            dict(record)
            for record in (records or [])
            if isinstance(record, dict)
        ]
        if not normalized:
            normalized = [
                {
                    "_empty": True,
                    "title": "近两天没有可回退的删除记录。",
                    "_meta": "",
                }
            ]
        self.beginResetModel()
        self._records = normalized
        self.endResetModel()

"""Packaging-only review dialog; no business or network operations."""


def review_release_notes(journal, draft):
    from PyQt6.QtWidgets import (
        QApplication, QDialog, QDialogButtonBox, QHeaderView, QHBoxLayout,
        QLabel, QMessageBox, QPlainTextEdit, QPushButton, QTableWidget,
        QTableWidgetItem, QVBoxLayout,
    )
    app = QApplication.instance() or QApplication([])
    dialog = QDialog()
    dialog.setWindowTitle("核对本次更新记录")
    dialog.resize(920, 650)
    layout = QVBoxLayout(dialog)
    snapshot = draft["snapshot"]
    layout.addWidget(QLabel("发布基线：" + (snapshot["baseline"][:12] or "首次发布，使用当前提交") +
                            "　当前提交：" + (snapshot["head"][:12] or "Git 不可用")))
    summary = QPlainTextEdit()
    summary.setReadOnly(True)
    summary.setMaximumHeight(150)
    summary.setPlainText("\n".join([c["id"][:12] + " " + c["subject"] for c in snapshot["commits"]] +
                                    [f["status"] + " " + f["path"] for f in snapshot["files"]]))
    layout.addWidget(summary)
    table = QTableWidget(0, 3)
    table.setHorizontalHeaderLabels(["问题", "解决方案", "模块"])
    table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
    table.setWordWrap(True)
    layout.addWidget(table)

    def add_row(item=None):
        row = table.rowCount()
        table.insertRow(row)
        table.setRowHeight(row, 85)
        for column, key in enumerate(("problem", "solution", "module")):
            table.setItem(row, column, QTableWidgetItem((item or {}).get(key, "")))

    for item in draft["items"]:
        add_row(item)
    tools = QHBoxLayout()
    add, remove = QPushButton("+"), QPushButton("-")
    add.setToolTip("添加更新事项")
    remove.setToolTip("删除选中事项")
    add.clicked.connect(lambda: add_row())
    remove.clicked.connect(lambda: table.removeRow(table.currentRow()) if table.currentRow() >= 0 else None)
    tools.addWidget(add)
    tools.addWidget(remove)
    tools.addStretch()
    layout.addLayout(tools)
    buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
    buttons.button(QDialogButtonBox.StandardButton.Save).setText("保存草稿")
    buttons.button(QDialogButtonBox.StandardButton.Ok).setText("确认并继续打包")
    buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("暂不打包")
    layout.addWidget(buttons)
    result = None

    def save(confirmed=False):
        nonlocal draft, result
        values = [{key: table.item(row, column).text() if table.item(row, column) else ""
                   for column, key in enumerate(("problem", "solution", "module"))} for row in range(table.rowCount())]
        try:
            draft = journal.save_draft(dict(draft, items=values), confirmed=confirmed)
            if confirmed:
                result = draft
                dialog.accept()
            return True
        except (OSError, ValueError):
            QMessageBox.warning(dialog, "更新记录未保存", "请完整填写问题、解决方案和模块，并确认草稿目录可写。")
            return False

    buttons.button(QDialogButtonBox.StandardButton.Save).clicked.connect(lambda: save())
    buttons.accepted.connect(lambda: save(True))
    buttons.rejected.connect(dialog.reject)
    dialog.rejected.connect(lambda: save())
    dialog.exec()
    return result

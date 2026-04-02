from PyQt6.QtWidgets import QMessageBox, QLabel
from PyQt6.QtCore import Qt, QTimer
from .styles import get_stylesheet


def show_simple_message(parent, text, theme="dark"):
    """显示统一样式的消息框（无标题栏、只有确定按钮）"""
    msg_box = QMessageBox(parent)
    msg_box.setWindowFlags(
        Qt.WindowType.Dialog
        | Qt.WindowType.FramelessWindowHint
        | Qt.WindowType.WindowStaysOnTopHint
    )
    msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)
    msg_box.button(QMessageBox.StandardButton.Ok).setText("确定")
    msg_box.setText(text)
    msg_box.setStyleSheet(get_stylesheet(theme))
    msg_box.show()
    msg_box.exec()


def show_toast_message(parent, text, duration_ms=1500, theme="dark"):
    """显示轻量级 Toast 消息（自动消失，点击任意位置关闭）"""
    from PyQt6.QtWidgets import QDialog, QVBoxLayout

    toast = QDialog(parent)
    toast.setWindowFlags(
        Qt.WindowType.FramelessWindowHint
        | Qt.WindowType.WindowStaysOnTopHint
        | Qt.WindowType.Tool
    )
    toast.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

    layout = QVBoxLayout(toast)
    layout.setContentsMargins(0, 0, 0, 0)

    label = QLabel(text)
    label.setStyleSheet("""
        QLabel {
            background-color: rgba(50, 50, 50, 0.9);
            color: #10B981;
            padding: 12px 24px;
            border-radius: 8px;
            font-size: 14px;
        }
    """)
    label.setAlignment(Qt.AlignmentFlag.AlignCenter)
    layout.addWidget(label)

    # 点击任意位置关闭
    toast.mousePressEvent = lambda e: toast.close()

    # 自动关闭
    QTimer.singleShot(duration_ms, toast.close)

    toast.adjustSize()
    if parent:
        # 居中显示
        parent_rect = parent.geometry()
        toast.move(
            parent_rect.center().x() - toast.width() // 2,
            parent_rect.center().y() - toast.height() // 2,
        )
    toast.show()
    return toast


def show_question_message(parent, text, theme="dark"):
    """显示带“确认/取消”的消息框"""
    msg_box = QMessageBox(parent)
    msg_box.setWindowFlags(
        Qt.WindowType.Dialog
        | Qt.WindowType.FramelessWindowHint
        | Qt.WindowType.WindowStaysOnTopHint
    )
    msg_box.setStandardButtons(
        QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel
    )
    msg_box.button(QMessageBox.StandardButton.Ok).setText("确认")
    msg_box.button(QMessageBox.StandardButton.Cancel).setText("取消")
    msg_box.setText(text)
    msg_box.setStyleSheet(get_stylesheet(theme))
    return msg_box.exec() == QMessageBox.StandardButton.Ok

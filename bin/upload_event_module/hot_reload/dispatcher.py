from PyQt6.QtCore import QObject, pyqtSignal


class MainThreadDispatcher(QObject):
    command_posted = pyqtSignal(object)

    def post(self, command: object) -> None:
        self.command_posted.emit(command)

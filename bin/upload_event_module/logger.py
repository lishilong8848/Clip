# -*- coding: utf-8 -*-
import faulthandler
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from .utils import get_data_file_path, migrate_runtime_data_files

LOG_FILE = get_data_file_path("app_log.txt")
CRASH_TRACE_FILE = get_data_file_path("crash_trace.log")
APP_LOG_MAX_BYTES = 10 * 1024 * 1024
APP_LOG_BACKUP_COUNT = 10
CRASH_TRACE_MAX_BYTES = 2 * 1024 * 1024
CRASH_TRACE_BACKUP_COUNT = 3

_crash_trace_file = None
_logging_initialized = False
_orig_stdout = sys.stdout
_orig_stderr = sys.stderr


def _roll_plain_log_file(path, backup_count):
    if not path or backup_count < 1:
        return
    try:
        if not os.path.exists(path):
            return
    except Exception:
        return
    try:
        oldest = f"{path}.{backup_count}"
        if os.path.exists(oldest):
            os.remove(oldest)
        for index in range(backup_count - 1, 0, -1):
            src = f"{path}.{index}"
            dst = f"{path}.{index + 1}"
            if os.path.exists(src):
                os.replace(src, dst)
        os.replace(path, f"{path}.1")
    except Exception:
        pass


def _rotate_plain_log_file(path, max_bytes, backup_count):
    if not path or backup_count < 1:
        return
    try:
        if not os.path.exists(path):
            return
        if os.path.getsize(path) < int(max_bytes or 0):
            return
    except Exception:
        return
    _roll_plain_log_file(path, backup_count)


def _ensure_log_parent_dir(path):
    try:
        parent = os.path.dirname(path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
    except Exception:
        pass


def _disable_faulthandler_safe():
    try:
        faulthandler.disable()
    except Exception:
        pass


def _close_crash_trace_stream():
    global _crash_trace_file
    _disable_faulthandler_safe()
    fh = _crash_trace_file
    _crash_trace_file = None
    if fh is None:
        return
    try:
        fh.flush()
    except Exception:
        pass
    try:
        fh.close()
    except Exception:
        pass


def _open_crash_trace_stream():
    global _crash_trace_file
    _ensure_log_parent_dir(CRASH_TRACE_FILE)
    try:
        _crash_trace_file = open(
            CRASH_TRACE_FILE, "a", encoding="utf-8", errors="ignore"
        )
    except Exception:
        _crash_trace_file = None
        return None
    try:
        faulthandler.enable(file=_crash_trace_file, all_threads=True)
    except Exception:
        pass
    return _crash_trace_file


def _reopen_crash_trace_stream_if_needed(force=False):
    global _crash_trace_file
    fh = _crash_trace_file
    if fh is None:
        if force:
            _roll_plain_log_file(CRASH_TRACE_FILE, CRASH_TRACE_BACKUP_COUNT)
        return _open_crash_trace_stream()
    try:
        fh.flush()
    except Exception:
        pass
    need_rotate = bool(force)
    if not need_rotate:
        try:
            if os.path.exists(CRASH_TRACE_FILE):
                need_rotate = (
                    os.path.getsize(CRASH_TRACE_FILE) >= CRASH_TRACE_MAX_BYTES
                )
        except Exception:
            need_rotate = False
    if not need_rotate:
        return fh
    _close_crash_trace_stream()
    _roll_plain_log_file(CRASH_TRACE_FILE, CRASH_TRACE_BACKUP_COUNT)
    return _open_crash_trace_stream()


def _should_roll_crash_trace_for_bytes(extra_bytes):
    try:
        if extra_bytes <= 0:
            return False
        current_size = 0
        if os.path.exists(CRASH_TRACE_FILE):
            current_size = os.path.getsize(CRASH_TRACE_FILE)
        return (current_size + int(extra_bytes)) > CRASH_TRACE_MAX_BYTES
    except Exception:
        return False


def write_crash_trace_message(message):
    if not message:
        return
    text = str(message).rstrip("\r\n")
    try:
        encoded = (text + "\n").encode("utf-8", errors="ignore")
        if _should_roll_crash_trace_for_bytes(len(encoded)):
            _reopen_crash_trace_stream_if_needed(force=True)
        fh = _reopen_crash_trace_stream_if_needed()
        if fh is None:
            return
        fh.write(text + "\n")
        fh.flush()
    except Exception:
        pass


def dump_crash_traceback():
    try:
        fh = _reopen_crash_trace_stream_if_needed()
        if fh is None:
            return
        faulthandler.dump_traceback(file=fh, all_threads=True)
        try:
            fh.flush()
        except Exception:
            pass
        _reopen_crash_trace_stream_if_needed()
    except Exception:
        pass


class SafeConsoleHandler(logging.StreamHandler):
    """Console handler that tolerates late shutdown and invalid streams."""

    def emit(self, record):
        if self.stream is None:
            self.stream = sys.__stdout__ or _orig_stdout
            if self.stream is None:
                return
        try:
            super().emit(record)
        except Exception:
            # Avoid recursive failures during interpreter teardown.
            pass

    def close(self):
        # Keep stdout/stderr alive; only mark handler as closed.
        try:
            self.flush()
        except Exception:
            pass
        logging.Handler.close(self)


class StreamLogger:
    """Redirect stdout/stderr to logging safely."""

    def __init__(self, stream, level):
        self.stream = stream
        self.level = level
        self.logger = logging.getLogger("Console")

    def write(self, message):
        if not message:
            return
        if self.stream:
            try:
                self.stream.write(message)
                self.stream.flush()
            except Exception:
                pass
        text = message.strip()
        if text:
            try:
                self.logger.log(self.level, text)
            except Exception:
                pass

    def flush(self):
        if self.stream:
            try:
                self.stream.flush()
            except Exception:
                pass


def _install_qt_message_handler():
    try:
        from PyQt6.QtCore import qInstallMessageHandler, QtMsgType
    except Exception:
        return

    def _qt_message_handler(msg_type, context, message):
        try:
            if msg_type == QtMsgType.QtFatalMsg:
                logging.critical(f"QtFatal: {message}")
                write_crash_trace_message(f"QtFatal: {message}")
                dump_crash_traceback()
            elif msg_type == QtMsgType.QtCriticalMsg:
                logging.error(f"QtCritical: {message}")
            elif msg_type == QtMsgType.QtWarningMsg:
                logging.warning(f"QtWarning: {message}")
            elif msg_type == QtMsgType.QtInfoMsg:
                logging.info(f"QtInfo: {message}")
            else:
                logging.debug(f"QtDebug: {message}")
        except Exception:
            pass

    try:
        qInstallMessageHandler(_qt_message_handler)
    except Exception:
        pass


def setup_logging():
    """Configure logging once per process."""
    global _logging_initialized, _crash_trace_file
    if _logging_initialized:
        return

    log_dir = os.path.dirname(LOG_FILE)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir, exist_ok=True)
        except Exception:
            pass

    migration_outcomes = []
    try:
        migration_outcomes = migrate_runtime_data_files()
    except Exception:
        migration_outcomes = []
    console_stream = sys.__stdout__ or _orig_stdout or sys.stdout
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=APP_LOG_MAX_BYTES,
        backupCount=APP_LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    console_handler = SafeConsoleHandler(console_stream)
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[file_handler, console_handler],
        force=True,
    )
    # Do not print logging internal exceptions to stderr.
    logging.raiseExceptions = False

    if migration_outcomes:
        counts = {
            "moved": 0,
            "conflict_renamed": 0,
            "skipped": 0,
            "failed": 0,
        }
        for item in migration_outcomes:
            status = item.get("status", "skipped")
            if status in counts:
                counts[status] += 1
            else:
                counts["skipped"] += 1
        logging.info(
            "RuntimeData migration summary: moved=%s conflict=%s skipped=%s failed=%s",
            counts["moved"],
            counts["conflict_renamed"],
            counts["skipped"],
            counts["failed"],
        )
        if counts["failed"] > 0:
            failed_items = [
                f"{x.get('filename')}:{x.get('error')}"
                for x in migration_outcomes
                if x.get("status") == "failed"
            ]
            if failed_items:
                logging.warning(
                    "RuntimeData migration failures: %s",
                    " | ".join(failed_items),
                )

    try:
        _rotate_plain_log_file(
            CRASH_TRACE_FILE,
            CRASH_TRACE_MAX_BYTES,
            CRASH_TRACE_BACKUP_COUNT,
        )
        _open_crash_trace_stream()
        logging.info(f"Crash trace: {CRASH_TRACE_FILE}")
    except Exception:
        pass

    _install_qt_message_handler()

    sys.excepthook = handle_exception

    # Install stream redirection once and always wrap original streams.
    if not isinstance(sys.stdout, StreamLogger):
        sys.stdout = StreamLogger(_orig_stdout, logging.INFO)
    if not isinstance(sys.stderr, StreamLogger):
        sys.stderr = StreamLogger(_orig_stderr, logging.ERROR)

    _logging_initialized = True


def handle_exception(exc_type, exc_value, exc_traceback):
    """Global exception handler."""
    if issubclass(exc_type, KeyboardInterrupt):
        try:
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
        except Exception:
            pass
        return
    try:
        logging.critical(
            "Uncaught exception:", exc_info=(exc_type, exc_value, exc_traceback)
        )
    except Exception:
        pass
    try:
        from .services.system_alert_webhook import send_system_alert

        detail = f"{getattr(exc_type, '__name__', 'Exception')}: {exc_value}"
        send_system_alert(
            event_code="app.uncaught_exception",
            title="未捕获异常",
            detail=detail,
            dedup_key=getattr(exc_type, "__name__", "Exception"),
            extra={"trace_path": CRASH_TRACE_FILE},
        )
    except Exception:
        pass


def log_info(message):
    logging.info(message)


def log_warning(message):
    logging.warning(message)


def log_error(message, exc_info=False):
    logging.error(message, exc_info=exc_info)


setup_logging()

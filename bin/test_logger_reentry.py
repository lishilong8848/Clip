"""Stream redirection regressions with isolated handlers and in-memory streams."""
import io
import logging
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import Mock, patch


BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

from upload_event_module import logger as app_logger


class _RecordingHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


class StreamLoggerReentryTests(unittest.TestCase):
    def setUp(self):
        self.root = logging.getLogger()
        self.console = logging.getLogger("Console")
        for logger in (self.root, self.console):
            state = patch.multiple(
                logger, handlers=[], filters=[], level=logging.DEBUG,
                disabled=False, propagate=True,
            )
            state.start()
            self.addCleanup(state.stop)
        self.recording = _RecordingHandler()
        self.root.addHandler(self.recording)
        self.addCleanup(self.recording.close)

    def add_handler(self, handler):
        self.root.addHandler(handler)
        self.addCleanup(handler.close)
        return handler

    def test_plain_handler_redirected_stderr_does_not_recurse(self):
        original = io.StringIO()
        redirected = app_logger.StreamLogger(original, logging.ERROR)
        with patch.object(sys, "stderr", redirected):
            self.add_handler(logging.StreamHandler())
            self.root.warning("fixture warning")
        self.assertEqual(len(original.getvalue().splitlines()), 1)
        self.assertEqual(original.getvalue(), "fixture warning\n")
        self.assertEqual(len(self.recording.records), 2)
        echoed = self.recording.records[-1]
        self.assertEqual(echoed.name, "Console")
        self.assertTrue(echoed.stream_echoed)

    def test_normal_echo_is_logged_once_without_duplicate_console_output(self):
        original = io.StringIO()
        self.add_handler(app_logger.SafeConsoleHandler(original))
        redirected = app_logger.StreamLogger(original, logging.INFO)
        redirected.write("fixture output\n")
        redirected.flush()
        self.assertEqual(original.getvalue(), "fixture output\n")
        self.assertEqual(len(self.recording.records), 1)
        self.assertEqual(self.recording.records[0].getMessage(), "fixture output")
        self.assertTrue(self.recording.records[0].stream_echoed)

    def test_failed_original_stream_uses_console_fallback_once(self):
        original = Mock()
        original.write.side_effect = OSError("fixture stream unavailable")
        fallback = io.StringIO()
        self.add_handler(app_logger.SafeConsoleHandler(fallback))
        redirected = app_logger.StreamLogger(original, logging.ERROR)
        redirected.write("fixture fallback\n")
        redirected.write("fixture next write\n")
        self.assertEqual(fallback.getvalue(), "fixture fallback\nfixture next write\n")
        self.assertEqual(len(self.recording.records), 2)
        self.assertTrue(all(not record.stream_echoed for record in self.recording.records))

    def test_guard_covers_original_echo_and_other_stream_wrappers(self):
        nested_output = io.StringIO()
        nested = app_logger.StreamLogger(nested_output, logging.ERROR)

        class ReentrantStream(io.StringIO):
            def write(self, message):
                nested.write("fixture nested write\n")
                return super().write(message)

        original = ReentrantStream()
        app_logger.StreamLogger(original, logging.ERROR).write("fixture outer write\n")
        self.assertEqual(original.getvalue(), "fixture outer write\n")
        self.assertEqual(nested_output.getvalue(), "")
        self.assertEqual(len(self.recording.records), 1)

    def test_guard_is_released_after_forwarding_errors(self):
        for error in (RuntimeError("fixture logging failure"), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__):
                original = io.StringIO()
                redirected = app_logger.StreamLogger(original, logging.ERROR)
                with patch.object(self.console, "log", side_effect=[error, None]) as log:
                    if isinstance(error, Exception):
                        redirected.write("fixture interrupted\n")
                    else:
                        with self.assertRaises(KeyboardInterrupt):
                            redirected.write("fixture interrupted\n")
                    redirected.write("fixture recovered\n")
                    self.assertEqual(log.call_count, 2)
                self.assertEqual(original.getvalue(), "fixture interrupted\nfixture recovered\n")

    def test_guard_is_released_after_original_stream_base_exception(self):
        original = Mock()
        original.write.side_effect = [KeyboardInterrupt(), None]
        redirected = app_logger.StreamLogger(original, logging.ERROR)
        with self.assertRaises(KeyboardInterrupt):
            redirected.write("fixture interrupted\n")
        redirected.write("fixture recovered\n")
        self.assertEqual(original.write.call_count, 2)
        self.assertEqual(len(self.recording.records), 1)
        self.assertEqual(self.recording.records[0].getMessage(), "fixture recovered")

    def test_other_thread_can_write_while_first_thread_is_echoing(self):
        entered, release = threading.Event(), threading.Event()
        errors = []

        class BlockingStream(io.StringIO):
            def write(self, message):
                if message == "fixture held\n":
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError("fixture release timed out")
                return super().write(message)

        original = BlockingStream()
        redirected = app_logger.StreamLogger(original, logging.ERROR)

        def write(message):
            try:
                redirected.write(message)
            except BaseException as error:
                errors.append(error)

        first = threading.Thread(target=write, args=("fixture held\n",), daemon=True)
        second = threading.Thread(target=write, args=("fixture parallel\n",), daemon=True)
        first.start()
        try:
            self.assertTrue(entered.wait(2), "first thread did not enter the original stream")
            second.start()
            second.join(2)
            self.assertFalse(second.is_alive(), "parallel write was blocked")
            self.assertEqual(original.getvalue(), "fixture parallel\n")
            self.assertEqual(len(self.recording.records), 1)
        finally:
            release.set()
            first.join(5)
            if second.ident is not None:
                second.join(5)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(original.getvalue(), "fixture parallel\nfixture held\n")
        self.assertEqual(len(self.recording.records), 2)
        self.assertTrue(all(record.stream_echoed for record in self.recording.records))

    def test_initialized_logger_does_not_replace_assert_logs_capture(self):
        before = self.root.handlers
        with self.assertLogs(level="INFO") as captured:
            handler = self.root.handlers[0]
            app_logger.setup_logging()
            self.assertIs(self.root.handlers[0], handler)
            self.root.info("fixture captured")
        self.assertEqual(self.root.handlers, before)
        self.assertEqual(captured.output, ["INFO:root:fixture captured"])


if __name__ == "__main__":
    unittest.main()

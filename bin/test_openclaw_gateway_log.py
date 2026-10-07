"""Isolated unit tests for the rotating gateway stdout/stderr capture.

The module under test (``openclaw_service.gateway_log``) is exercised with
temporary files, real anonymous pipes and synthetic in-memory streams only - no
real Node process, no service, no task scheduler, and no network/Feishu requests
are involved.
"""

from __future__ import annotations

import io
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

from openclaw_service.gateway_log import (  # noqa: E402
    BACKUP_COUNT,
    GatewayLog,
    MAX_BYTES,
    MAX_LINE_BYTES,
    OVERFLOW_MARKER,
    REDACTED,
)


class _BrokenStream:
    """Pipe stand-in whose reads always raise, mimicking a broken pipe."""

    def __init__(self, error=OSError("pipe closed unexpectedly")):
        self._error = error

    def read(self, size):
        raise self._error

    def close(self):
        pass


class _ChunkedStream:
    """In-memory stream that returns data in small pieces (cross-chunk reads).

    Exposes both ``read1`` and ``read`` so the drain can be exercised across
    many virtual read boundaries, and the trailing ``close`` records cleanup.
    """

    def __init__(self, data, step=7):
        self._data = data
        self._step = step
        self._pos = 0
        self.closed = False

    def read1(self, size):
        return self._read(size)

    def read(self, size):
        return self._read(size)

    def _read(self, size):
        if self._pos >= len(self._data):
            return b""
        end = min(self._pos + min(self._step, size), len(self._data))
        out = self._data[self._pos:end]
        self._pos = end
        return out

    def close(self):
        self.closed = True


def _seed_lines(count, width=60):
    return b"".join(b"payload-%04d:" % i + b"x" * width + b"\n" for i in range(count))


class GatewayLogRotationTests(unittest.TestCase):
    def test_default_limits_follow_resident_gateway_contract(self):
        # The shipped defaults are the required 2 MB / 3 backups contract.
        self.assertEqual(MAX_BYTES, 2_000_000)
        self.assertEqual(BACKUP_COUNT, 3)
        self.assertGreater(MAX_LINE_BYTES, 0)

    def test_resident_writes_rotate_while_running(self):
        # A small maxBytes keeps the test fast while exercising the same code
        # path that the default 2 MB handler uses in production.
        max_bytes = 4096
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path, max_bytes=max_bytes, backup_count=3, max_line_bytes=256)
            try:
                payload = _seed_lines(2000, width=120)  # ~ 250 KB, well over 3 rotations
                thread = log.attach(io.BytesIO(payload))
                thread.join(timeout=10)
                self.assertTrue(thread is not None)
                self.assertTrue(path.exists())
                backup_files = list(Path(tmp).glob("gateway.log.*"))
                self.assertEqual(len(backup_files), 3, "exactly three backups are retained")
                self.assertFalse(Path(tmp, "gateway.log.4").exists())
                # Handlers already rotated while the log was still open (resident),
                # so the active file must stay within one message over the cap.
                self.assertLessEqual(path.stat().st_size, max_bytes + 256)
                # Exactly the retained rotation budget is on disk - the log can
                # never grow without bound under a resident gateway.
                total = sum(p.stat().st_size for p in (path, *backup_files))
                self.assertLessEqual(total, (BACKUP_COUNT + 1) * max_bytes + 4096)
                self.assertGreater(total, max_bytes, "rotation retained more than the active file")
            finally:
                log.close()


class GatewayLogContentTests(unittest.TestCase):
    def test_small_log_is_written_before_eof_on_anonymous_pipe(self):
        # A real OS anonymous pipe proves the reader surfaces already-arrived
        # bytes before EOF: the write end stays open while we check the file.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path)
            read_fd, write_fd = os.pipe()
            reader = None
            writer = None
            try:
                reader = os.fdopen(read_fd, "rb")  # BufferedReader supports read1
                writer = os.fdopen(write_fd, "wb")
                thread = log.attach(reader)
                writer.write(b"early-line-before-eof\n")
                writer.flush()
                deadline = time.time() + 5
                seen = False
                while time.time() < deadline:
                    try:
                        if b"early-line-before-eof" in path.read_bytes():
                            seen = True
                            break
                    except FileNotFoundError:
                        pass
                    time.sleep(0.005)
                self.assertTrue(seen, "small line must be written before EOF")
                # Without closing the write end the pipe is still open, so the
                # capture has not been considered ended/consumed yet.
                self.assertFalse(log.closed)
                # Now reach EOF on the read side.
                writer.close()
                writer = None
                thread.join(timeout=5)
                self.assertTrue(log.closed, "EOF cleanup marks the capture consumed")
                self.assertEqual(log._logger.handlers, [])
                self.assertIsNone(log._thread)
            finally:
                if writer is not None:
                    try:
                        writer.close()
                    except OSError:
                        pass
                if reader is not None:
                    try:
                        reader.close()
                    except OSError:
                        pass
                log.close()

    def test_overlong_line_is_dropped_without_writing_its_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path, max_bytes=1_000_000, backup_count=1, max_line_bytes=64)
            try:
                thread = log.attach(io.BytesIO(b"x" * 5000 + b"\n"))
                thread.join(timeout=5)
                text = path.read_text("utf-8")
                lines = [line for line in text.splitlines() if line.strip()]
                self.assertEqual(len(lines), 1)
                self.assertIn(OVERFLOW_MARKER, lines[0])
                self.assertNotIn("x", lines[0], "no content of the dropped line may be kept")
                self.assertLessEqual(len(lines[0]), 64 + len(" ...[truncated]"))
            finally:
                log.close()

    def test_secret_crossing_cap_and_read_chunks_is_dropped_not_leaked(self):
        secret = "SECRET-abcdef-0123456789-zzzz"
        max_line = 40
        line = b"AAAFILLER" + secret.encode() + b"BBB\n"
        self.assertGreater(len(line) - 1, max_line, "fixture must exceed the cap")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path, secrets={"LIGHTHOUSE_GATEWAY_TOKEN": secret},
                             max_bytes=1_000_000, backup_count=1, max_line_bytes=max_line)
            try:
                # Feed in 5-byte pieces so the secret crosses many read chunks.
                thread = log.attach(_ChunkedStream(line, step=5))
                thread.join(timeout=5)
                text = path.read_text("utf-8")
                self.assertNotIn(secret, text)
                # The old bug wrote pending[:cap] then redacted only that slice,
                # leaking the first half of a cap-straddling secret.
                self.assertNotIn(secret[:10], text, "partial secret prefix must not leak")
                self.assertIn(OVERFLOW_MARKER, text)
            finally:
                log.close()

    def test_chinese_long_line_is_dropped_but_short_line_kept(self):
        max_line = 64
        short_line = "中文日志行"
        long_content = "中" * 30  # 90 bytes > 64-byte cap
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path, max_bytes=1_000_000, backup_count=1, max_line_bytes=max_line)
            try:
                payload = short_line.encode("utf-8") + b"\n" + long_content.encode("utf-8") + b"\n"
                thread = log.attach(_ChunkedStream(payload, step=11))
                thread.join(timeout=5)
                text = path.read_text("utf-8")
                self.assertIn(short_line, text, "in-limit Chinese line is preserved")
                self.assertIn(OVERFLOW_MARKER, text)
                self.assertNotIn(long_content, text, "over-long Chinese line must be dropped by bytes")
            finally:
                log.close()

    def test_secret_values_are_redacted_but_env_names_kept(self):
        secrets = {
            "LIGHTHOUSE_GATEWAY_TOKEN": "super-secret-gateway-token-abcdef",
            "LIGHTHOUSE_MODEL_KEY": "sk-plain-model-key-123456",
            "LIGHTHOUSE_BRIDGE_TOKEN": "bridge-secret-token-xyz",
            "LIGHTHOUSE_BRIDGE_URL": "https://bridge.example/internal?token=bridge-secret-token-xyz",
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path, secrets=secrets, max_bytes=1_000_000, backup_count=1)
            try:
                payload = (
                    b"using env LIGHTHOUSE_MODEL_KEY and LIGHTHOUSE_GATEWAY_TOKEN\n"
                    b"ready token=super-secret-gateway-token-abcdef key=sk-plain-model-key-123456\n"
                    b"bridge bridge-secret-token-xyz https://bridge.example/internal?token=bridge-secret-token-xyz\n"
                )
                thread = log.attach(io.BytesIO(payload))
                thread.join(timeout=5)
                text = path.read_text("utf-8")
                self.assertNotIn("super-secret-gateway-token-abcdef", text)
                self.assertNotIn("sk-plain-model-key-123456", text)
                self.assertNotIn("bridge-secret-token-xyz", text)
                self.assertNotIn("https://bridge.example/internal?token=bridge-secret-token-xyz", text)
                self.assertNotIn("plaintext", text)
                self.assertIn(REDACTED, text)
                # Environment placeholder names that reference a secret are not
                # secrets themselves and must survive filtering.
                self.assertIn("LIGHTHOUSE_MODEL_KEY", text)
                self.assertIn("LIGHTHOUSE_GATEWAY_TOKEN", text)
            finally:
                log.close()

    def test_short_nonempty_secret_is_redacted(self):
        # Short secrets are not treated as harmless: any non-empty value is a
        # secret and must be scrubbed from the persisted log.
        short = "ab"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path, secrets={"LIGHTHOUSE_GATEWAY_TOKEN": short},
                             max_bytes=1_000_000, backup_count=1)
            try:
                thread = log.attach(io.BytesIO(b"prefix abABab middle\n"))
                thread.join(timeout=5)
                text = path.read_text("utf-8")
                self.assertIn(REDACTED, text)
                self.assertNotIn("abABab", text, "secret occurrences must be redacted")
            finally:
                log.close()

    def test_prefix_overlapping_secrets_redacted_longest_first(self):
        # If one secret is a plain prefix of another, replacing the shorter one
        # first would leave the longer value's tail exposed in the log. Secrets
        # must be replaced in descending length order.
        short = "abc"
        long_ = "abcdef"
        secrets = {"LIGHTHOUSE_GATEWAY_TOKEN": long_, "LIGHTHOUSE_MODEL_KEY": short}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path, secrets=secrets, max_bytes=1_000_000, backup_count=1)
            try:
                payload = b"side abc alone\nlong " + long_.encode("utf-8") + b" suffix\n"
                thread = log.attach(io.BytesIO(payload))
                thread.join(timeout=5)
                text = path.read_text("utf-8")
                self.assertNotIn(long_, text, "longer overlapping secret must be redacted")
                self.assertNotIn("def", text, "longer secret tail must not leak")
                self.assertNotIn(short, text, "prefix secret must be redacted")
                self.assertIn(REDACTED, text)
            finally:
                log.close()


class GatewayLogLifecycleTests(unittest.TestCase):
    def test_eof_cleans_up_thread_handler_and_pipe(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path)
            stream = _ChunkedStream(b"x\n")
            thread = log.attach(stream)
            thread.join(timeout=5)
            # EOF (not close()) already released handler, pipe and thread.
            self.assertTrue(log.closed)
            self.assertEqual(log._logger.handlers, [])
            self.assertIsNone(log._thread)
            self.assertTrue(stream.closed is True or log._stream is None)
            # The capture cannot be reused after EOF.
            with self.assertRaises(RuntimeError):
                log.attach(io.BytesIO(b"nope\n"))
            log.close()

    def test_close_is_idempotent_and_releases_handler(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path)
            thread = log.attach(io.BytesIO(b"first\nsecond\n"))
            thread.join(timeout=5)
            # EOF already marks the capture consumed and releases the handler.
            self.assertTrue(log.closed)
            self.assertEqual(log._logger.handlers, [])
            # A duplicate close must be a safe no-op.
            log.close()
            self.assertEqual(log._logger.handlers, [])
            self.assertTrue(log.closed)
            log.close()
            self.assertTrue(log.closed)

    def test_close_does_not_close_a_live_pipe_and_is_bounded(self):
        # Lifecycle contract: the runtime terminates the child first, then close
        # joins the (already finishing) reader with a bounded timeout. close()
        # must never close a live pipe itself or busy-poll.
        read_fd, write_fd = os.pipe()
        reader = None
        writer = None
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path)
            thread = None
            try:
                reader = os.fdopen(read_fd, "rb")
                writer = os.fdopen(write_fd, "wb")
                thread = log.attach(reader)
                # Leave the pipe open: the reader stays parked, yet close() is
                # still bounded (join timeout) and never closes the stream.
                start = time.monotonic()
                log.close()
                elapsed = time.monotonic() - start
                self.assertLess(elapsed, 7.0, "close() must be bounded by its join timeout")
                self.assertTrue(log.closed)
                self.assertFalse(reader.closed, "close() must not close a live pipe")
                # The capture cannot be reused and close is idempotent.
                with self.assertRaises(RuntimeError):
                    log.attach(io.BytesIO(b"nope\n"))
                log.close()
            finally:
                if writer is not None:
                    writer.close()
                if reader is not None:
                    reader.close()
                if thread is not None:
                    thread.join(timeout=2)
                log.close()

    def test_read_stream_error_is_recorded_and_cleanup_succeeds(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path)
            broken = _BrokenStream()
            thread = log.attach(broken)
            thread.join(timeout=5)
            self.assertIsInstance(log.error, OSError)
            # Failure also triggered full cleanup (handler released, consumed).
            self.assertTrue(log.closed)
            self.assertEqual(log._logger.handlers, [])
            # Closing after a read error must not hang or raise.
            log.close()
            self.assertTrue(log.closed)

    def test_reader_explicitly_closes_bytesio_and_pipe_after_eof(self):
        # The reader must close the stream it drained itself once it stops
        # reading - not merely drop the reference in _cleanup. This holds for
        # both an in-memory stream and a real OS anonymous pipe, and it must not
        # rely on the test's external cleanup closing the handle.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path)
            in_stream = io.BytesIO(b"hello\n")
            try:
                thread = log.attach(in_stream)
                thread.join(timeout=5)
                self.assertTrue(in_stream.closed,
                                "reader must close the in-memory stream it drained")
            finally:
                log.close()
        # Real anonymous pipe: the reader releases its small read handle on EOF.
        read_fd, write_fd = os.pipe()
        reader = None
        writer = None
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path)
            try:
                reader = os.fdopen(read_fd, "rb")
                writer = os.fdopen(write_fd, "wb")
                thread = log.attach(reader)
                writer.write(b"x\n")
                writer.flush()
                writer.close()
                writer = None
                thread.join(timeout=5)
                self.assertTrue(log.closed)
                self.assertTrue(reader.closed,
                                "anonymous pipe read handle must be closed by the reader")
            finally:
                if writer is not None:
                    writer.close()
                if reader is not None and not reader.closed:
                    reader.close()
                log.close()

    def test_attach_start_failure_close_is_idempotent_and_cleans_up(self):
        # A real threading.Thread.start() failure leaves an unstarted thread and
        # a held stream behind. close() must not join that unstarted thread
        # (which raises RuntimeError and shadows the original error); it must be
        # a safe no-op, idempotent, close the held stream and release the
        # handler since no reader is reading the pipe.
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path)
            stream = _ChunkedStream(b"never-started\n")

            def failing_start(self_thread):
                raise RuntimeError("cannot start thread")

            try:
                with mock.patch.object(threading.Thread, "start", failing_start):
                    with self.assertRaisesRegex(RuntimeError, "cannot start"):
                        log.attach(stream)
            finally:
                mock.patch.stopall()
            self.assertFalse(stream.closed, "stream stays held until close()")
            # close() must not raise (no join on the unstarted thread) and must
            # free both the stream and the handler.
            log.close()
            self.assertTrue(stream.closed, "held pipe must be closed with no reader thread")
            self.assertTrue(log.closed)
            self.assertEqual(log._logger.handlers, [])
            self.assertIsNone(log._thread)
            self.assertIsNone(log._stream)
            # Idempotent: a second close is a safe no-op.
            log.close()
            self.assertTrue(log.closed)
            # And reuse is forbidden after attach failed/closed.
            with self.assertRaises(RuntimeError):
                log.attach(io.BytesIO(b"nope\n"))

    def test_concurrent_close_is_idempotent_and_blocks_reuse_with_live_reader(self):
        # Multiple threads calling close() concurrently on a capture whose
        # reader is still parked on a live pipe must all succeed, mark the
        # capture consumed, release the handler, and never touch the live pipe.
        # Afterwards attach is rejected because consuming happened inside the
        # lock rather than only after the join.
        read_fd, write_fd = os.pipe()
        reader = None
        writer = None
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "gateway.log"
            log = GatewayLog(path)
            thread = None
            try:
                reader = os.fdopen(read_fd, "rb")
                writer = os.fdopen(write_fd, "wb")
                thread = log.attach(reader)
                errors = []

                def do_close():
                    try:
                        log.close()
                    except Exception as exc:  # noqa: BLE001 - surfaced for the test
                        errors.append(exc)

                closers = [threading.Thread(target=do_close) for _ in range(4)]
                for closer in closers:
                    closer.start()
                for closer in closers:
                    closer.join(timeout=5)
                self.assertEqual(errors, [], "concurrent close must never raise")
                self.assertTrue(log.closed)
                self.assertEqual(log._logger.handlers, [])
                self.assertFalse(reader.closed, "close must not close a live pipe being read")
                with self.assertRaises(RuntimeError):
                    log.attach(io.BytesIO(b"nope\n"))
            finally:
                if writer is not None:
                    writer.close()
                if reader is not None and not reader.closed:
                    reader.close()
                if thread is not None:
                    thread.join(timeout=2)
                log.close()


if __name__ == "__main__":
    unittest.main()
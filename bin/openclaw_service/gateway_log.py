"""Bounded, rotating capture of a resident gateway's stdout/stderr.

A ``GatewayLog`` owns exactly one ``RotatingFileHandler`` (2 MB, 3 backups) and
drains a child pipe on a single daemon reader thread.  The reader uses
``read1`` when the stream supports it (real ``BufferedReader`` pipes) so small
amounts of data that have arrived are written immediately instead of waiting
for ``read`` to fill its whole buffer or reach EOF.

Security rules enforced here:

* Every non-empty secret value found in the child environment is redacted
  before anything touches the log file; short values are treated like any other
  secret (never "too short to be harmless").  Secrets are replaced in
  descending length order so a secret that is a prefix of another never leaves
  the longer value's tail exposed.
* A log line whose **byte** length exceeds the cap is discarded as a whole and
  replaced by a single safety marker, so a secret spanned across a cap or read
  chunk boundary is never written in truncated form.
* Reader exceptions are logged by exception *type* only; raw exception text
  (which could contain keys or body data) is never logged.

Lifecycle rules:

* The runtime is responsible for terminating the child process *before*
  ``close()`` is called; EOF on the pipe lets the reader finish, then the
  instance releases its handler and marks itself consumed.
* The reader thread closes the stream it drained once it stops reading, so a
  real ``Popen.stdout`` pipe handle is released on EOF or on a read error.
  ``close()`` never reaches into a pipe a reader is still blocked on; if no
  reader ever started (a failed thread start) ``close()`` closes the held
  stream instead.
* On EOF or on a reader exception the instance cleans up its own pipe/handler
  and forbids a second ``attach``.
* ``close()`` is idempotent and concurrency-safe: it marks the capture consumed
  inside its lock (so a concurrent attach or a second close is rejected) and
  joins any reader thread with a short bounded timeout; it never busy-polls.
"""
from __future__ import annotations

import logging
import logging.handlers
import threading
from pathlib import Path

MAX_BYTES = 2_000_000
BACKUP_COUNT = 3
MAX_LINE_BYTES = 4096
CHUNK_SIZE = 64 * 1024
REDACTED = "[REDACTED]"
OVERFLOW_MARKER = "[overlong line dropped]"
JOIN_TIMEOUT = 2.0
SECRET_ENV_KEYS = (
    "LIGHTHOUSE_GATEWAY_TOKEN",
    "LIGHTHOUSE_MODEL_KEY",
    "LIGHTHOUSE_BRIDGE_TOKEN",
    "LIGHTHOUSE_BRIDGE_URL",
)
_LOGGER = logging.getLogger(__name__)


class GatewayLog:
    """Rotating file sink plus one blocking pipe reader thread."""

    def __init__(self, path, *, secrets=(), max_bytes=MAX_BYTES, backup_count=BACKUP_COUNT,
                 max_line_bytes=MAX_LINE_BYTES):
        self.path = Path(path)
        self._max_bytes = int(max_bytes)
        self._backup_count = int(backup_count)
        self._max_line_bytes = int(max_line_bytes)
        self._secrets = self._collect_secrets(secrets)
        self._closed = threading.Event()
        self._lock = threading.RLock()
        self._thread = None
        self._stream = None
        self._handler = None
        # A plain Logger object (not logging.getLogger) keeps this instance out
        # of the global registry, so restarts never accumulate handler names.
        self._logger = logging.Logger("openclaw_service.gateway." + hex(id(self)))
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False
        self.error = None
        self._open_handler()

    @classmethod
    def _collect_secrets(cls, values):
        """Return every non-empty secret value found in the child environment.

        Values are ordered by descending length so ``_redact`` replaces the
        longest match first; otherwise a shorter secret that is also a prefix
        of a longer one would leave the longer value's tail exposed.
        """
        result = []
        if not values:
            return result
        for key in SECRET_ENV_KEYS:
            value = values.get(key) if isinstance(values, dict) else None
            if isinstance(value, str) and value:
                result.append(value)
        result.sort(key=len, reverse=True)
        return result

    def _open_handler(self):
        handler = logging.handlers.RotatingFileHandler(
            str(self.path), mode="a", maxBytes=self._max_bytes,
            backupCount=self._backup_count, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(message)s"))
        self._handler = handler
        self._logger.addHandler(handler)

    def attach(self, stream):
        """Start draining *stream* on a background reader thread.

        ``stream`` may be ``None`` (e.g. a fake process in tests) and is then a
        no-op. Returns the thread (or ``None``). The instance rejects any
        second ``attach`` once it has been consumed (EOF/error/close).
        """
        with self._lock:
            if self._closed.is_set():
                raise RuntimeError("GatewayLog is closed")
            if stream is None:
                return None
            if self._thread is not None or self._stream is not None:
                raise RuntimeError("GatewayLog already attached")
            self._stream = stream
            thread = threading.Thread(
                target=self._drain, args=(stream,),
                name="gateway-log-" + self.path.name, daemon=True)
            self._thread = thread
        thread.start()
        return thread

    @staticmethod
    def _read_chunk(stream, size):
        """Read up to *size* bytes that are already available.

        ``read1`` is preferred for real buffered pipes so small writes surface
        immediately; streams without ``read1`` (e.g. BytesIO) fall back to
        ``read``, which for those objects returns available data anyway.
        """
        read1 = getattr(stream, "read1", None)
        if callable(read1):
            return read1(size)
        return stream.read(size)

    def _drain(self, stream):
        pending = b""
        overflow = False
        try:
            while True:
                chunk = self._read_chunk(stream, CHUNK_SIZE)
                if not chunk:
                    break
                pending += chunk
                # Emit every completed line in the pending buffer.
                while True:
                    nl = pending.find(b"\n")
                    if nl < 0:
                        break
                    line = pending[:nl]
                    pending = pending[nl + 1:]
                    if overflow:
                        overflow = False
                        self._write_overflow()
                    elif len(line) > self._max_line_bytes:
                        self._write_overflow()
                    else:
                        self._write_line(line)
                # The remainder is a single partial line (no newline yet). If it
                # exceeds the cap we drop the *whole* line rather than persist a
                # truncated slice that could leak the first half of a secret.
                if len(pending) > self._max_line_bytes:
                    overflow = True
                    pending = b""
            # EOF: flush whatever partial line is still buffered.
            if overflow:
                self._write_overflow()
            elif pending:
                if len(pending) > self._max_line_bytes:
                    self._write_overflow()
                else:
                    self._write_line(pending)
        except (OSError, ValueError) as exc:
            if not self._closed.is_set():
                self.error = exc
                _LOGGER.warning(
                    "Gateway stdout/stderr reader stopped with %s", type(exc).__name__)
        finally:
            # This thread owns the read, so once it has exited the read loop it
            # is safe to close the stream (a real Popen.stdout pipe handle must
            # be released, not just forgotten).
            self._close_stream(stream)
            self._cleanup()

    def _cleanup(self):
        """Release handler/log resources and mark the capture consumed.

        Idempotent and concurrency-safe. Called by the reader on EOF/error and
        by ``close()``. It does not close the stream: the reader closes its own
        stream after reading, and ``close()`` closes a stream that was never
        read (a failed thread start). This keeps ``close()`` from reaching into
        a pipe that a live reader is still blocking on.
        """
        with self._lock:
            self._closed.set()
            self._thread = None
            self._stream = None
            handler = self._handler
            self._handler = None
            if handler is not None:
                if handler in self._logger.handlers:
                    self._logger.removeHandler(handler)
                try:
                    handler.close()
                except (OSError, ValueError):
                    pass

    @staticmethod
    def _close_stream(stream):
        try:
            stream.close()
        except (OSError, ValueError):
            pass

    def _write_overflow(self):
        self._write_line(OVERFLOW_MARKER.encode("utf-8"))

    def _write_line(self, raw):
        text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        if not text:
            return
        text = self._redact(text)
        if len(text) > self._max_line_bytes:
            text = text[:self._max_line_bytes] + " ...[truncated]"
        if text:
            self._logger.info("%s", text)

    def _redact(self, text):
        if not self._secrets:
            return text
        for secret in self._secrets:
            text = text.replace(secret, REDACTED)
        return text

    @property
    def closed(self):
        return self._closed.is_set()

    def close(self):
        """Idempotent, bounded close.

        The runtime must terminate the child process *before* calling close so
        the pipe reaches EOF and the reader thread finishes on its own. The
        capture is marked consumed inside the lock, so a concurrent attach or a
        second close is rejected immediately. The reader thread is then joined
        with a short timeout; ``close()`` never closes a stream a live reader
        is still blocking on. If no reader ever started (a failed thread start)
        the held stream is closed here so no pipe handle leaks.
        """
        with self._lock:
            if self._closed.is_set():
                return
            self._closed.set()
            thread = self._thread
            stream = self._stream
            self._thread = None
            self._stream = None
        if thread is not None and thread is not threading.current_thread() and thread.ident is not None:
            thread.join(timeout=JOIN_TIMEOUT)
        if thread is None or thread.ident is None:
            # No reader ever ran (attach(None) or a failed thread start), so the
            # held stream (if any) would otherwise leak; closing it is safe.
            if stream is not None:
                self._close_stream(stream)
        self._cleanup()
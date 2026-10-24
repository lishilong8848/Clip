"""Opt-in-at-startup structured diagnostics, never raw logs or conversation text."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from pathlib import Path
import re
import threading
from time import monotonic
import traceback

_store = None
_version = 'development'
_lock = threading.RLock()
MODULES = {'assistant', 'feishu', 'startup'}
STAGES = {'answer', 'model_attempt', 'delivery', 'deadline', 'startup_failed', 'service_failed', 'runtime_auth_failed'}
VERSION_PATTERN = r'(?:development|(?:ClipFlow_)?[Vv]?\d+(?:[._-]\d+){0,8})'
REASONS = {
    'no callable tools remain': 'tools_not_registered',
    'context overflow': 'context_limit',
    'context length exceeded': 'context_limit',
    'maximum context length': 'context_limit',
    'output token limit': 'output_limit',
    'system message must be at the beginning': 'message_order',
    'model provider authentication failed': 'model_auth',
    'model provider rate limit exceeded': 'model_busy',
    'model provider unavailable': 'model_connection',
    'model provider timed out': 'model_timeout',
    'model provider rejected image input': 'image_unsupported',
    'model provider rejected request': 'request_rejected',
    'model provider model not found': 'model_not_found',
    'model identity mismatch': 'model_identity',
    'model invocation expired': 'model_invocation_expired',
}


def configure(root, project=None):
    from .diagnostic_store import DiagnosticStore
    global _store, _version
    candidate = DiagnosticStore(Path(root))
    version = 'development'
    if project is not None:
        try:
            path = Path(project) / 'bin/build_meta.json'
            if path.stat().st_size <= 65536:
                value = json.loads(path.read_text(encoding='utf-8'))
                label = value.get('display_version') or value.get('build_id')
                if isinstance(label, str) and len(label) <= 120 and re.fullmatch(VERSION_PATTERN, label):
                    version = label
        except (OSError, ValueError, AttributeError):
            pass
    with _lock:
        _store, _version = candidate, version
    return candidate


def detach(store):
    global _store
    with _lock:
        if _store is store:
            _store = None


def capture(module, stage, *, run_id='', category='', error=None, native=None):
    """Persist an allowlisted envelope; exception messages/locals never leave the process."""
    with _lock:
        store, version = _store, _version
    if store is None or module not in MODULES or stage not in STAGES:
        return
    event = {'module': module, 'stage': stage, 'version': version}
    if isinstance(run_id, str) and re.fullmatch(r'(?:[0-9a-f]{8}|[0-9a-f]{32})', run_id):
        event['run_id'] = run_id
    # Categories are internal identifiers, not arbitrary exception text.
    if isinstance(category, str) and re.fullmatch(r'[a-z][a-z_]{0,50}', category):
        event['category'] = category
    if error is not None:
        name = type(error).__name__
        event['error_type'] = name if re.fullmatch(r'[A-Za-z_]{1,80}', name) else 'Error'
        status = getattr(error, 'status', getattr(error, 'status_code', None))
        if type(status) is int and 100 <= status <= 599:
            event['http_status'] = status
        frames = []
        for frame in traceback.extract_tb(error.__traceback__)[-8:] if isinstance(error, BaseException) else []:
            filename = Path(frame.filename).name
            name = frame.name
            if re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', filename) and re.fullmatch(r'[A-Za-z_<>]{1,100}', name):
                frames.append(f'{filename}:{name}:{frame.lineno}')
        event['frames'] = frames
    raw = native.get('message') if isinstance(native, dict) else native
    if isinstance(raw, str) and raw:
        event['native_reason'] = next((code for marker, code in REASONS.items() if marker in raw.lower()), 'unclassified')
        event['native_fingerprint'] = hashlib.sha256(raw.encode('utf-8')).hexdigest()
    try:
        store.append(event)
    except Exception:
        # Diagnostic storage must not turn a recoverable answer into another failure.
        logging.getLogger(__name__).warning('Diagnostic local persistence deferred')


class DiagnosticReporter:
    """One background coroutine; one record per minute, with persistent retry leases."""
    def __init__(self, store, remote_factory=None):
        self.store = store
        self.remote_factory = remote_factory
        self.remote = None
        self.task = None
        self.closing = False
        self.last_attempt = float('-inf')

    def start(self):
        if self.task is None:
            self.task = asyncio.create_task(self.run())

    async def _store_call(self, method, *args, **kwargs):
        # Cancelling to_thread's await does not close its SQLite connection.
        work = asyncio.create_task(asyncio.to_thread(method, *args, **kwargs))
        try:
            return await asyncio.shield(work)
        except asyncio.CancelledError:
            await asyncio.gather(work, return_exceptions=True)
            raise

    def publish(self, batch):
        if self.remote is None:
            if self.remote_factory is None:
                from lan_bitable_template_portal.assistant_diagnostics_cloud import DiagnosticRemote
                self.remote_factory = DiagnosticRemote
            self.remote = self.remote_factory()
        def checkpoint(**values):
            return self.store.checkpoint(batch['id'], batch['lease_token'], **values)
        record_id = self.remote.publish(batch, checkpoint)
        self.store.complete(batch['id'], batch['lease_token'], record_id)

    async def tick(self):
        if self.closing or monotonic() - self.last_attempt < 60:
            return
        batch = await self._store_call(self.store.claim, lease_seconds=1800)
        if not batch:
            return
        self.last_attempt = monotonic()
        try:
            # Shield the short-lived worker from cancellation while it owns a lease.
            work = asyncio.create_task(asyncio.to_thread(self.publish, batch))
            try:
                await asyncio.shield(work)
            except asyncio.CancelledError:
                await asyncio.gather(work, return_exceptions=True)
                raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            category = getattr(exc, 'category', '')
            code = category if isinstance(category, str) and re.fullmatch(r'[a-z_]{1,60}', category) else 'upload_failed'
            await self._store_call(self.store.fail, batch['id'], batch['lease_token'], code)
            # Deliberately not captured: upload failures must not create a logging loop.
            logging.getLogger(__name__).warning('Diagnostic batch upload deferred: code=%s', code)

    async def run(self):
        while not self.closing:
            try:
                delay = await self._store_call(self.store.next_delay)
                await asyncio.sleep(max(1, delay, 60 - (monotonic() - self.last_attempt)))
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger(__name__).warning('Diagnostic reporter deferred')
                await asyncio.sleep(60)

    async def close(self):
        self.closing = True
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        if self.remote is not None:
            await asyncio.to_thread(self.remote.close)
        detach(self.store)

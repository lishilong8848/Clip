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
STAGES = {'answer', 'model_attempt', 'delivery', 'deadline', 'startup_failed', 'service_failed', 'runtime_auth_failed',
    'gateway', 'model_request'}
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
    'incomplete turn detected': 'incomplete_turn',
    'reasoning-only retries exhausted': 'reasoning_only',
    'model provider returned empty response': 'empty_response',
    'empty response retries exhausted': 'empty_response',
    'model response too large': 'response_limit',
    'missing assistant terminal message detected': 'missing_response',
    'model provider returned invalid response': 'invalid_provider_response',
    'model stream interrupted': 'stream_interrupted',
}

# Enumerated diagnostic scalars: closed sets only, never free-form text.
DETAIL_INTS = frozenset({'payloads', 'tools', 'compactions', 'reasoning_retries', 'empty_retries',
    'missing_retries', 'request_bytes', 'response_bytes',
    'knowledge_documents', 'knowledge_chunks', 'elapsed_ms', 'budget_ms', 'model_attempt'})
DETAIL_BOOLS = frozenset({'has_assistant', 'has_current_assistant', 'replay_safe'})
DETAIL_ENUMS = {
    'stop_reason': frozenset({'stop', 'end_turn', 'toolUse', 'tool_use', 'tool_calls', 'length', 'max_tokens',
        'error', 'aborted', 'missing', 'other'}),
    'response_type': frozenset({'event_stream', 'json', 'html', 'other'}),
    'model_family': frozenset({'deepseek', 'minimax', 'glm', 'qwen', 'other'}),
}
DETAIL_MAX_INT = 100000000
GATEWAY_LINE_LIMIT = 4096
GATEWAY_MARKERS = (
    ('incomplete turn detected', 'incomplete_turn'),
    ('reasoning-only retries exhausted', 'reasoning_only'),
    ('model provider returned empty response', 'empty_response'),
    ('empty response retries exhausted', 'empty_response'),
    ('model response too large', 'response_limit'),
    ('missing assistant terminal message detected', 'missing_response'),
)
# The pinned native logger emits ids as `lh-<20-64 hex>:<32 hex>`, optionally followed by
# `:fallback:<n>`, `:validation[:<n>]`, or the combined `:fallback:<n>:validation:<n>` suffix.
NATIVE_RUN_PATTERN = re.compile(
    r'lh-(?:[0-9a-f]{20,64}):([0-9a-f]{32})(?::fallback:\d{1,9})?(?::validation(?::\d{1,9})?)?')
NATIVE_RUN_ANCHOR = re.compile(r'(?<![A-Za-z])runId=(?P<id>lh-[^\s,;)\'"]{1,200})')
SESSION_ANCHOR = re.compile(r'(?<![A-Za-z])sessionId=[^\s,;)\'"]{1,200}')
GATEWAY_FIELD_PATTERN = re.compile(
    r'(?P<key>stopReason|hasLastAssistant|hasCurrentAttemptAssistant|payloads|tools|compactions|replaySafe)='
    r'(?:"(?P<quoted>[^"]{0,32})"|(?P<plain>[^\s,;)"\']{1,32}))')
GATEWAY_RETRY_KINDS = {'reasoning': 'reasoning_retries', 'empty': 'empty_retries', 'missing': 'missing_retries',
    'missingassistant': 'missing_retries'}
GATEWAY_STOP_REASONS = {'stop': 'stop', 'end_turn': 'end_turn', 'endturn': 'end_turn', 'tooluse': 'toolUse',
    'tool_use': 'tool_use', 'toolcalls': 'tool_calls', 'tool_calls': 'tool_calls', 'length': 'length',
    'maxtokens': 'max_tokens', 'max_tokens': 'max_tokens', 'error': 'error', 'aborted': 'aborted',
    'missing': 'missing', 'none': 'missing'}
# `reasoningRetries=0/1`, `empty-retries=4/5`, `reasoning_only_retries=2/2`, `missingAssistantRetries=0/1`;
# used count is stored only.
GATEWAY_COUNTER_PATTERN = re.compile(
    r'(?P<kind>reasoning|empty|missing(?:[-_ ]?assistant)?)[-_ ]?retries=(?P<n>\d{1,9})/(?P<max>\d{1,9})',
    re.IGNORECASE)
# Retry-exhausted templates carry the budget as `attempts=2/2`, `retrying 1/1`, or a bare `retries=3/3`.
GATEWAY_ATTEMPT_PATTERN = re.compile(
    r'(?:\battempts=|\bretrying |(?<![A-Za-z])retries=)(?P<n>\d{1,9})/(?P<max>\d{1,9})', re.IGNORECASE)
GATEWAY_TRUE = frozenset({'true', 'yes'})
GATEWAY_FALSE = frozenset({'false', 'no'})
GATEWAY_JSON_MESSAGE_KEYS = ('message', '1')


def _gateway_boolean(raw):
    value = raw.strip().lower()
    if value in GATEWAY_TRUE:
        return True
    if value in GATEWAY_FALSE:
        return False
    return None


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


def clean_details(value):
    """Return only allowlisted enumerated scalars; drop every other key and all raw text."""
    if not isinstance(value, dict):
        return {}
    allowed = {}
    for name, item in list(value.items())[:64]:
        if not isinstance(name, str):
            continue
        try:
            if name in DETAIL_INTS:
                if type(item) is int and 0 <= item <= DETAIL_MAX_INT:
                    allowed[name] = item
            elif name in DETAIL_BOOLS:
                if type(item) is bool:
                    allowed[name] = item
            elif name in DETAIL_ENUMS:
                if isinstance(item, str) and item in DETAIL_ENUMS[name]:
                    allowed[name] = str(item)
        except Exception:
            continue
    return allowed


def native_run_id(text):
    """Extract the 32 hex run id only from a full native id; anything else yields ''."""
    try:
        if not isinstance(text, str) or len(text) > 256:
            return ''
        match = NATIVE_RUN_PATTERN.fullmatch(text)
        return match.group(1) if match else ''
    except Exception:
        return ''


def _gateway_message(value):
    """Return the single log message string, parsing at most one JSON level; anything else is ''."""
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate.startswith('{'):
            return value
        try:
            value = json.loads(candidate)
        except (ValueError, RecursionError):
            return value
    if isinstance(value, dict):
        # Structured records carry the line under `message`, or under string key "1" (non-string keys
        # such as a logged int 1 are ignored); nested dicts/lists are never inspected.
        for key in GATEWAY_JSON_MESSAGE_KEYS:
            message = value.get(key)
            if isinstance(message, str):
                return message
    return ''


def _gateway_details(segment, kind):
    """Read only enumerated fields from the bounded message; retry counters use their own kind."""
    details = {}
    for match in GATEWAY_FIELD_PATTERN.finditer(segment):
        raw = match.group('quoted')
        if raw is None:
            raw = match.group('plain')
        if len(raw) > 32:
            continue
        key = match.group('key')
        if key == 'stopReason':
            code = GATEWAY_STOP_REASONS.get(raw.strip().lower())
            if code:
                details['stop_reason'] = code
        elif key == 'hasLastAssistant':
            value = _gateway_boolean(raw)
            if value is not None:
                details['has_assistant'] = value
        elif key == 'hasCurrentAttemptAssistant':
            value = _gateway_boolean(raw)
            if value is not None:
                details['has_current_assistant'] = value
        elif key == 'replaySafe':
            value = _gateway_boolean(raw)
            if value is not None:
                details['replay_safe'] = value
        elif key in ('payloads', 'tools', 'compactions') and re.fullmatch(r'\d{1,9}', raw):
            details[key] = int(raw)
    for match in GATEWAY_COUNTER_PATTERN.finditer(segment):
        name = GATEWAY_RETRY_KINDS.get(match.group('kind').lower())
        if name:
            details[name] = int(match.group('n'))
    if kind and kind not in details:
        # Only retry-exhausted templates carry a budget; incomplete-turn lines never do.
        attempt = GATEWAY_ATTEMPT_PATTERN.search(segment)
        if attempt:
            details[kind] = int(attempt.group('n'))
    return details


def capture_gateway_line(text):
    """Parse one console/JSON log line; emit bounded native_terminal metadata for known markers only."""
    try:
        if isinstance(text, str) and len(text) > GATEWAY_LINE_LIMIT:
            return False
        message = _gateway_message(text)
        # A native record is exactly one line; embedded breaks would splice unrelated fields.
        if not message or len(message) > GATEWAY_LINE_LIMIT or re.search(r'[\r\n\u2028\u2029]', message):
            return False
        lowered = message.lower()
        found = sorted((lowered.find(marker), code) for marker, code in GATEWAY_MARKERS if marker in lowered)
        if not found:
            return False
        position, category = found[0]
        # Full validation: the anchored runId= value must match the whole native id, so a longer
        # invalid id (e.g. 33 hex) can never pass by matching a valid prefix.
        run_match = NATIVE_RUN_ANCHOR.search(message, position)
        if run_match is None:
            return False
        run_id = native_run_id(run_match.group('id'))
        if not run_id or SESSION_ANCHOR.search(message) is None:
            return False
        details = _gateway_details(message, GATEWAY_RETRY_KINDS.get(category.split('_')[0]))
        # Only the fixed marker travels as native text; the line, session id and ids never do.
        marker = next(text for text, code in GATEWAY_MARKERS if code == category)
        capture('assistant', 'gateway', category='native_terminal', run_id=run_id, native=marker,
            details=details)
        return True
    except Exception:
        # Log parsing must never raise into the logging caller.
        return False


def capture(module, stage, *, run_id='', category='', error=None, native=None, details=None):
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
    allowed = clean_details(details)
    if allowed:
        event['details'] = allowed
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

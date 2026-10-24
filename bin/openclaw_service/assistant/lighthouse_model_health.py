"""Shared numeric provider health; no connections, prompts, or conversation data."""
import hashlib
import json
import logging
import re
import sqlite3
import threading
import time
from collections import OrderedDict
from contextlib import closing


_QUALITY_LOCK = threading.RLock()
_QUALITY_DOCUMENT = 'task_model_quality:v1'
_TASK_CATEGORIES = frozenset({'statistics', 'document', 'mixed', 'general'})
_REASONING = re.compile(r'分析|比较|对比|排查|诊断|推理|计算|统计|汇总|方案|\b(?:analy[sz]e|compare|debug|diagnose|calculate|reason|plan)\b', re.I)


def task_category(turn):
    """Return a fixed category; never retain question text in observations."""
    category = turn.get('task_type')
    if isinstance(category, str) and category in _TASK_CATEGORIES:
        return category
    if turn.get('file_ids'):
        return 'document'
    return 'statistics' if _REASONING.search(str(turn.get('prompt') or turn.get('question') or '')[:2000]) else 'general'


def profile_key(profile):
    return hashlib.sha256(json.dumps([profile.get(key) for key in
        ('endpoint', 'model', 'key_cipher')], ensure_ascii=False).encode()).hexdigest()


def model_health_key(actor, profile):
    from .lighthouse_ai import model_capabilities
    fingerprint = hashlib.sha256(json.dumps([
        profile_key(profile), model_capabilities(profile), profile.get('context_window') or 64000
    ], sort_keys=True).encode()).hexdigest()
    owner = 'shared' if profile.get('shared') is True else 'private:' + actor['id']
    return hashlib.sha256(owner.encode()).hexdigest(), fingerprint


def model_health(assistant):
    with _QUALITY_LOCK:
        if '_model_health' not in vars(assistant):
            assistant._model_health = ModelHealth(getattr(assistant, 'store', None))
        return assistant._model_health


def _quality_key(actor, profile, category):
    # Quality outcomes are personal even when provider availability is shared.
    health_key = model_health_key(actor, profile)
    return hashlib.sha256(json.dumps([actor['id'], health_key[0],
        profile.get('_quality_config') or health_key[1], category], sort_keys=True).encode()).hexdigest()


def task_quality(assistant, actor, turn, profile):
    """Numeric summary, with separate evaluation/reviewer counts and eligibility."""
    return model_health(assistant).quality_snapshot(_quality_key(actor, profile, task_category(turn)))


def record_task_quality(assistant, actor, turn, profile, *, score, evidence, confirmed=False, observation_id=None):
    """Record a trusted evaluator's 0..100 outcome, never transport success."""
    from .lighthouse_ai import AssistantError, model_capabilities
    if (confirmed is not True or evidence not in ('deterministic_eval', 'reviewer')
            or type(score) not in (int, float) or not 0 <= score <= 100
            or not profile.get('id')):
        return False
    try:
        model = assistant.model_for(actor)
        if model.settings().get('enabled') is not True:
            return False
        current = model.profile(profile.get('id'))
        fingerprint = model_health_key(actor, current)[1]
        if profile.get('_quality_config') is not None:
            if (profile['_quality_config'] != fingerprint
                    or model_capabilities(profile)['image_input'] and not model_capabilities(current)['image_input']):
                return False
            current = {**current, 'image_input': model_capabilities(profile)['image_input'],
                       '_quality_config': fingerprint}
        if model_health_key(actor, current) != model_health_key(actor, profile):
            return False
    except (AssistantError, KeyError, TypeError, ValueError):
        return False
    return model_health(assistant).record_quality(
        _quality_key(actor, profile, task_category(turn)), score=score, evidence=evidence, observation_id=observation_id)


def ingest_task_evaluation(assistant, actor, *, model_id, fingerprint, task_type, score, confirmed=False,
                           observation_id=None):
    """Save one trusted case; return False for duplicates, invalid input or failed writes.

    Pass qualified_observations' observation_id and profile_fingerprint (as
    fingerprint). Omitting the ID preserves the independent-sample API.
    """
    from .lighthouse_ai import AssistantError
    if (confirmed is not True or type(score) not in (int, float) or not 0 <= score <= 100
            or not isinstance(task_type, str) or task_type not in _TASK_CATEGORIES
            or not isinstance(model_id, str) or not model_id
            or not isinstance(fingerprint, str) or not re.fullmatch(r'[0-9a-f]{64}', fingerprint)
            or observation_id is not None and (not isinstance(observation_id, str)
                or not re.fullmatch(r'[0-9a-f]{64}', observation_id))):
        return False
    try:
        profile = assistant.model_for(actor).profile(model_id)
        if model_health_key(actor, profile)[1] != fingerprint:
            return False
    except (AssistantError, KeyError, TypeError, ValueError):
        return False
    return record_task_quality(assistant, actor, {'task_type': task_type}, profile,
        score=score, evidence='deterministic_eval', confirmed=confirmed, observation_id=observation_id)


def select_task_profile(assistant, actor, turn, profile, *, required=None, attempted=(), initial=True, context=False):
    """Rank authorized compatible profiles without inference or capability probes."""
    from .lighthouse_ai import AssistantError, model_capabilities
    from .lighthouse_context import model_context_limit
    model = assistant.model_for(actor)
    settings = model.settings()
    if not settings.get('enabled'):
        return None
    allowed = model_capabilities(profile)
    if required and any(required.get(key) is True and not allowed[key] for key in ('tool_calls', 'image_input')):
        return None
    required_reasoning = bool(required and required.get('reasoning') is True)
    required = {**allowed, **(required or {})}
    explicit = initial and profile.get('_automatic_selection') is not True
    reasoning = bool(_REASONING.search(str(turn.get('prompt') or turn.get('question') or '')[:2000]))
    context = bool(context or turn.get('file_ids'))
    health = model_health(assistant)
    candidates = []
    for index, item in enumerate(settings.get('models', [])):
        if not item.get('configured') or explicit and item['id'] != profile.get('id'):
            continue
        try:
            candidate = model.profile(item['id'])
            capabilities = model_capabilities(candidate)
            context_window = min(int(candidate.get('context_window') or 64000), model_context_limit(candidate))
            if not candidate.get('key_cipher') or not candidate.get('endpoint') or context_window <= 0:
                continue
            if profile_key(candidate) in attempted:
                continue
            if (capabilities['tool_calls'] != required['tool_calls']
                    or required['image_input'] and not capabilities['image_input']
                    or required.get('context_window', 0) > context_window
                    or required_reasoning and not capabilities['reasoning']):
                continue
            state = health.snapshot(model_health_key(actor, candidate))
            candidate = {**candidate, 'image_input': required['image_input'],
                         '_quality_config': model_health_key(actor, candidate)[1]}
            effective_state = health.snapshot(model_health_key(actor, candidate))
            if not explicit and max(state.get('cooldown_until', 0), effective_state.get('cooldown_until', 0)) > health._clock():
                continue
            quality = task_quality(assistant, actor, turn, candidate)
        except (AssistantError, TypeError, ValueError):
            continue
        # 50 is an unmeasured routing baseline, never an evaluation result.
        score = (-(quality['score'] if quality.get('eligible') else 50),
                 -context_window if context else 0, capabilities['reasoning'] != reasoning,
                 state.get('latency', 30), initial and item['id'] != profile.get('id'),
                 sum(capabilities[key] != required[key] for key in ('tool_calls', 'image_input')), index)
        candidates.append((score, candidate))
    selected = min(candidates, key=lambda pair: pair[0])[1] if candidates else None
    if selected is not None and profile.get('_automatic_selection') is True:
        selected['_automatic_selection'] = True
    return selected


class ModelHealth:
    MAX_ENTRIES = 512
    TTL = 3600
    # A rejected answer is task-specific, not an outage of this shared provider.
    TRANSIENT = frozenset({'model_busy', 'model_connection', 'timeout'})
    CONFIGURATION = frozenset({'model_auth', 'model_request'})
    QUALITY_MIN_SAMPLES = 3
    QUALITY_MAX_SAMPLES = 32
    QUALITY_MAX_OBSERVATIONS = 256
    QUALITY_TTL = 30 * 86400

    def __init__(self, store=None):
        self._entries = OrderedDict()
        self._lock = threading.Lock()
        self._clock = time.monotonic
        self._store = store
        self._quality = {}
        self._wall_clock = time.time

    def _quality_entries(self, saved):
        now = self._wall_clock()
        entries = {}
        if saved is not None and not isinstance(saved, dict):
            raise ValueError('Invalid task quality document')
        for key, entry in (saved or {}).items():
            if (not isinstance(key, str) or not re.fullmatch(r'[0-9a-f]{64}', key)
                    or not isinstance(entry, dict)):
                continue
            updated, samples = entry.get('updated'), entry.get('samples')
            if (type(updated) not in (int, float) or not now - self.QUALITY_TTL < updated <= now
                    or not isinstance(samples, list)):
                continue
            valid = [sample for sample in samples[-self.QUALITY_MAX_SAMPLES:]
                     if isinstance(sample, list) and len(sample) == 2
                     and type(sample[0]) in (int, float) and 0 <= sample[0] <= 100
                     and sample[1] in ('deterministic_eval', 'reviewer')]
            if valid:
                entries[key] = {'updated': updated, 'samples': valid}
                seen = entry.get('seen', {})
                if isinstance(seen, dict):
                    seen = {identity: timestamp for identity, timestamp in seen.items()
                            if isinstance(identity, str) and re.fullmatch(r'[0-9a-f]{64}', identity)
                            and type(timestamp) in (int, float) and now - self.QUALITY_TTL < timestamp <= now}
                    if seen:
                        entries[key]['seen'] = dict(sorted(seen.items(), key=lambda pair: pair[1])[-self.QUALITY_MAX_OBSERVATIONS:])
        return dict(sorted(entries.items(), key=lambda pair: pair[1]['updated'])[-self.MAX_ENTRIES:])

    def _quality_update(self, update=None):
        from ..store import AssistantStore
        from .lighthouse_ai import NAMESPACE
        with _QUALITY_LOCK:
            if update is not None and isinstance(self._store, AssistantStore):
                # The document API has no atomic read/modify/write operation.
                with closing(self._store._connect()) as db, db:
                    db.execute('BEGIN IMMEDIATE')
                    row = db.execute('SELECT payload_json FROM documents WHERE namespace=? AND key=?',
                                     (NAMESPACE, _QUALITY_DOCUMENT)).fetchone()
                    entries = self._quality_entries(json.loads(row[0]) if row else {})
                    if update(entries) is False:
                        return False
                    entries = self._quality_entries(entries)
                    db.execute('INSERT INTO documents(namespace,key,payload_json,updated_at) VALUES (?,?,?,?) '
                               'ON CONFLICT(namespace,key) DO UPDATE SET payload_json=excluded.payload_json, '
                               'updated_at=excluded.updated_at',
                               (NAMESPACE, _QUALITY_DOCUMENT, json.dumps(entries, allow_nan=False), self._wall_clock()))
                    return entries
            saved = self._store.get_document(NAMESPACE, _QUALITY_DOCUMENT) if self._store is not None else self._quality
            entries = self._quality_entries(saved)
            if update is not None:
                if update(entries) is False:
                    return False
                entries = self._quality_entries(entries)
                if self._store is not None:
                    self._store.put_document(NAMESPACE, _QUALITY_DOCUMENT, entries)
                else:
                    self._quality = entries
            return entries

    def quality_snapshot(self, key):
        try:
            samples = self._quality_update().get(key, {}).get('samples', [])
        except (OSError, sqlite3.Error, TypeError, ValueError):
            return {}
        if not samples:
            return {}
        counts = {source: sum(sample[1] == source for sample in samples)
                  for source in ('deterministic_eval', 'reviewer')}
        return {'score': sum(sample[0] for sample in samples) / len(samples), 'samples': len(samples),
                'eligible': len(samples) >= self.QUALITY_MIN_SAMPLES, **counts}

    def record_quality(self, key, *, score, evidence, observation_id=None):
        if (not isinstance(key, str) or not re.fullmatch(r'[0-9a-f]{64}', key)
                or type(score) not in (int, float) or not 0 <= score <= 100
                or evidence not in ('deterministic_eval', 'reviewer')
                or observation_id is not None and (evidence != 'deterministic_eval'
                    or not isinstance(observation_id, str) or not re.fullmatch(r'[0-9a-f]{64}', observation_id))):
            return False
        def update(entries):
            entry = entries.get(key, {})
            seen = dict(entry.get('seen', {}))
            if observation_id is not None:
                if observation_id in seen:
                    return False
                seen[observation_id] = self._wall_clock()
            entries[key] = {'updated': self._wall_clock(),
                            'samples': [*entry.get('samples', []), [score, evidence]][-self.QUALITY_MAX_SAMPLES:]}
            if seen:
                entries[key]['seen'] = seen
        try:
            return self._quality_update(update) is not False
        except (OSError, sqlite3.Error, TypeError, ValueError):
            logging.getLogger(__name__).warning('Assistant task quality observation could not be saved')
            return False

    def _prune(self, now):
        for key, entry in list(self._entries.items()):
            if now - entry['updated'] >= self.TTL:
                del self._entries[key]
        while len(self._entries) > self.MAX_ENTRIES:
            self._entries.popitem(last=False)

    def snapshot(self, key):
        with self._lock:
            self._prune(self._clock())
            entry = self._entries.get(key)
            if entry is not None:
                self._entries.move_to_end(key)
            return dict(entry or {})

    def record(self, key, *, category='', latency=None):
        if category and category not in self.TRANSIENT | self.CONFIGURATION:
            return
        with self._lock:
            now = self._clock()
            self._prune(now)
            entry = self._entries.pop(key, {'failures': 0})
            if category:
                failures = min(entry['failures'] + 1, 5)
                base, maximum = (15, 120) if category in self.TRANSIENT else (60, 300)
                entry.update(failures=failures, cooldown_until=now + min(maximum, base * 2 ** (failures - 1)),
                             category=category)
            else:
                previous = entry.get('latency', latency)
                entry.update(failures=0, cooldown_until=0, category='',
                             latency=previous * .75 + latency * .25)
            entry['updated'] = now
            self._entries[key] = entry
            self._prune(now)

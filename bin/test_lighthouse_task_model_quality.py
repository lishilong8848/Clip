"""Task quality uses synthetic outcomes and temporary stores; no live providers."""
import copy
import hashlib
import json
import sys
import tempfile
import unittest
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant import lighthouse_model_health as quality
from openclaw_service.assistant.lighthouse_ai import CustomModel, LighthouseAssistant, NAMESPACE
from openclaw_service.store import AssistantStore
from bin import test_lighthouse_model_failover as fixtures


def _process_observations(args):
    root, key = args
    health = quality.ModelHealth(AssistantStore(root))
    return [health.record_quality(key, score=75, evidence='deterministic_eval') for _ in range(4)]


def _process_ingestion(args):
    root, actor, observations = args
    store = AssistantStore(root)
    assistant = LighthouseAssistant(store, lambda *_: [])
    return [quality.ingest_task_evaluation(assistant, actor, **row) for row in observations]


class TaskQualityTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ModelFailoverTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.assistant = self.fixture.engine.assistant
        self.actor = self.fixture.actor
        self.turn = {'question': 'private original question', 'task_type': 'statistics'}
        self.health = self.fixture.agent.health
        self.assistant.store = self.fixture.store
        self.health._store = self.assistant.store

    def profile(self, identity='current'):
        return self.assistant.model_for(self.actor).profile(identity)

    def record(self, identity='backup', score=90, count=3, **kwargs):
        profile = self.profile(identity)
        for _ in range(count):
            self.assertTrue(quality.record_task_quality(self.assistant, self.actor, self.turn, profile,
                score=score, evidence=kwargs.get('evidence', 'deterministic_eval'), confirmed=True))

    def select(self, **kwargs):
        profile = {**self.profile(), '_automatic_selection': True}
        return quality.select_task_profile(self.assistant, self.actor, self.turn, profile, **kwargs)

    def test_intent_categories_and_bounded_fallback_do_not_retain_text(self):
        for category in ('statistics', 'document', 'mixed', 'general'):
            self.assertEqual(quality.task_category({'task_type': category, 'file_ids': ['private']}), category)
        self.assertEqual(quality.task_category({'file_ids': ['private']}), 'document')
        self.assertEqual(quality.task_category({'question': 'calculate totals'}), 'statistics')
        self.assertEqual(quality.task_category({'question': 'x' * 2000 + ' calculate totals'}), 'general')
        self.assertEqual(quality.task_category({'task_type': ['private text']}), 'general')

    def test_default_and_sample_threshold_and_quality_beats_latency(self):
        self.assertEqual(self.select()['id'], 'current')
        self.record(count=2)
        self.assertEqual(self.select()['id'], 'current')
        snapshot = quality.task_quality(self.assistant, self.actor, self.turn, self.profile('backup'))
        self.assertEqual(snapshot['samples'], 2)
        self.assertFalse(snapshot['eligible'])
        self.record(count=1)
        self.health.record(quality.model_health_key(self.actor, self.profile()), latency=.01)
        self.health.record(quality.model_health_key(self.actor, self.profile('backup')), latency=60)
        self.assertEqual(self.select()['id'], 'backup')
        self.assertEqual(quality.task_quality(self.assistant, self.actor, self.turn, self.select())['score'], 90)

    def test_lower_proven_quality_loses_even_to_unmeasured_default(self):
        self.record(score=20)
        self.assertEqual(self.select()['id'], 'current')
        self.record('current', score=10)
        self.assertEqual(self.select()['id'], 'backup')

    def test_quality_precedes_reasoning_preference_and_is_task_specific(self):
        self.fixture.configure(self.actor['id'], [fixtures.profile('current', reasoning=True), fixtures.profile('backup')])
        self.turn['question'] = 'analyze and calculate totals'
        self.record('current', score=10)
        self.record(score=95)
        self.assertEqual(self.select()['id'], 'backup')
        self.turn['task_type'] = 'document'
        self.assertEqual(self.select()['id'], 'current')

    def test_only_confirmed_evidence_records_quality_and_counts_sources(self):
        profile = self.profile('backup')
        for score, evidence, confirmed in ((90, 'availability', True), (90, 'self_report', True),
                (90, 'reviewer', False), (90, 'reviewer', 1), (True, 'reviewer', True),
                ('90', 'reviewer', True), (-1, 'reviewer', True), (101, 'reviewer', True),
                (float('nan'), 'reviewer', True), (float('inf'), 'reviewer', True)):
            with self.subTest(score=score, evidence=evidence, confirmed=confirmed):
                self.assertFalse(quality.record_task_quality(self.assistant, self.actor, self.turn, profile,
                    score=score, evidence=evidence, confirmed=confirmed))
        self.health.record(quality.model_health_key(self.actor, profile), latency=1)
        self.assertEqual(quality.task_quality(self.assistant, self.actor, self.turn, profile), {})
        self.record(count=2)
        self.record(score=30, count=1, evidence='reviewer')
        self.assertEqual(quality.task_quality(self.assistant, self.actor, self.turn, profile),
            {'score': 70, 'samples': 3, 'eligible': True, 'deterministic_eval': 2, 'reviewer': 1})

    def test_explicit_saved_model_is_honored_even_when_lower_quality_or_cooling(self):
        self.record('current', score=10)
        self.record(score=99)
        self.health.record(quality.model_health_key(self.actor, self.profile()), category='model_auth')
        for flag in (None, False, 1, 'true'):
            profile = {**self.profile(), '_automatic_selection': flag}
            selected = quality.select_task_profile(self.assistant, self.actor, self.turn, profile)
            self.assertEqual(selected['id'], 'current')

    def test_auth_cooldown_and_disabled_settings_override_quality(self):
        self.record(score=99)
        self.health.record(quality.model_health_key(self.actor, self.profile('backup')), category='model_auth')
        self.assertEqual(self.select()['id'], 'current')
        self.health.record(quality.model_health_key(self.actor, self.profile()), category='model_busy')
        self.assertIsNone(self.select())
        self.fixture.configure(self.actor['id'], self.fixture.profiles, enabled=False)
        self.assertIsNone(quality.select_task_profile(self.assistant, self.actor, self.turn, self.fixture.profiles[0]))
        self.assertFalse(quality.record_task_quality(self.assistant, self.actor, self.turn, self.fixture.profiles[0],
            score=100, evidence='reviewer', confirmed=True))

    def test_capability_and_minimax_image_boundaries_override_scores(self):
        profiles = [fixtures.profile('current', model='WanWu/MiniMax-Auto', image_input=True),
                    fixtures.profile('backup', model='WanWu/Qwen-Auto', image_input=True),
                    fixtures.profile('glm', model='WanWu/GLM-Auto', image_input=True),
                    fixtures.profile('deepseek', model='WanWu/Deepseek-Auto', image_input=True)]
        self.fixture.configure(self.actor['id'], profiles)
        self.record('current', score=10)
        for identity in ('backup', 'glm', 'deepseek'):
            self.record(identity, score=100)
        self.assertEqual(self.select()['id'], 'current')
        self.assertIsNone(self.select(attempted={quality.profile_key(self.profile())}))
        self.fixture.configure(self.actor['id'], [fixtures.profile('current', tool_calls=False), fixtures.profile('backup')])
        self.record(score=100)
        self.assertEqual(self.select()['id'], 'current')
        self.assertIsNone(self.select(required={'context_window': 999999999}))
        self.assertIsNone(self.select(required={'reasoning': True}))

    def test_private_and_shared_configurations_and_actors_are_isolated(self):
        self.record(score=99)
        other = {'id': 'other-user', 'scopes': ['A']}
        self.fixture.configure(other['id'], [fixtures.profile('other-private')])
        private = self.assistant.model_for(other).profile('other-private')
        self.assertFalse(quality.record_task_quality(self.assistant, self.actor, self.turn, private,
            score=100, evidence='reviewer', confirmed=True))
        self.assertEqual(quality.task_quality(self.assistant, other, self.turn, self.profile('backup')), {})
        shared = {**fixtures.profile('shared'), 'shared': True}
        self.fixture.store.put_document(NAMESPACE, 'model', {'enabled': True, 'models': [shared],
            'active_model_id': 'shared', 'image_input_default_version': 1})
        identity = next(item['id'] for item in self.assistant.model_for(self.actor).settings()['models'] if item['shared'])
        self.record(identity, score=100)
        shared = self.profile(identity)
        self.assertEqual(quality.task_quality(self.assistant, other, self.turn, shared), {})
        self.assertEqual(quality.model_health_key(self.actor, shared), quality.model_health_key(other, shared))
        selected = quality.select_task_profile(self.assistant, other, self.turn, {**private, '_automatic_selection': True})
        self.assertEqual(selected['id'], 'other-private')

    def test_rotation_invalidates_quality_and_rejects_late_results(self):
        original = self.profile('backup')
        self.record(score=99)
        for field, value in (('key_cipher', 'rotated'), ('endpoint', 'https://new.invalid/v1/chat/completions'),
                ('model', 'different'), ('tool_calls', False), ('reasoning', True), ('context_window', 128000)):
            with self.subTest(field=field):
                changed = {**original, field: value}
                self.fixture.configure(self.actor['id'], [fixtures.profile('current'), changed])
                self.assertEqual(quality.task_quality(self.assistant, self.actor, self.turn, self.profile('backup')), {})
                self.assertFalse(quality.record_task_quality(self.assistant, self.actor, self.turn, original,
                    score=100, evidence='reviewer', confirmed=True))

    def test_text_mask_keeps_configuration_identity_and_rotation_invalidates_it(self):
        self.fixture.configure(self.actor['id'], [fixtures.profile('current'), fixtures.profile('backup', image_input=True)])
        self.record(score=99)
        self.assertEqual(self.select()['id'], 'backup')
        selected = self.select(attempted={quality.profile_key(self.profile())})
        self.assertFalse(selected['image_input'])
        for _ in range(3):
            self.assertTrue(quality.record_task_quality(self.assistant, self.actor, self.turn, selected,
                score=99, evidence='reviewer', confirmed=True))
        self.assertEqual(self.select()['id'], 'backup')
        self.fixture.configure(self.actor['id'], self.fixture.profiles)
        self.assertFalse(quality.record_task_quality(self.assistant, self.actor, self.turn, selected,
            score=99, evidence='reviewer', confirmed=True))
        self.assertEqual(self.select()['id'], 'current')

    def test_required_capabilities_cannot_enable_user_disabled_features(self):
        self.fixture.configure(self.actor['id'], [fixtures.profile('current', tool_calls=False),
            fixtures.profile('backup', image_input=True)])
        for required in ({'tool_calls': True}, {'image_input': True}):
            self.assertIsNone(self.select(required=required))

    def test_cooling_vision_config_cannot_be_reused_by_masking_images(self):
        self.fixture.configure(self.actor['id'], [fixtures.profile('current'), fixtures.profile('backup', image_input=True)])
        self.record(score=99)
        self.health.record(quality.model_health_key(self.actor, self.profile('backup')), category='model_auth')
        self.assertEqual(self.select()['id'], 'current')

    def test_benchmark_ingestion_checks_exact_tested_configuration_and_category(self):
        fingerprint = quality.model_health_key(self.actor, self.profile('backup'))[1]
        args = dict(model_id='backup', fingerprint=fingerprint, task_type='statistics', score=95)
        self.assertFalse(quality.ingest_task_evaluation(self.assistant, self.actor, **args))
        for _ in range(3):
            self.assertTrue(quality.ingest_task_evaluation(self.assistant, self.actor, **args, confirmed=True))
        self.assertEqual(self.select()['id'], 'backup')
        self.assertFalse(quality.ingest_task_evaluation(self.assistant, self.actor, **{**args, 'fingerprint': 'stale'}, confirmed=True))
        self.assertFalse(quality.ingest_task_evaluation(self.assistant, self.actor, **{**args, 'task_type': 'user text'}, confirmed=True))

    def test_concurrent_assistants_use_one_bounded_store_without_lost_samples(self):
        assistants = [SimpleNamespace(store=self.fixture.store, model_for=self.assistant.model_for) for _ in range(8)]
        def observe(assistant):
            for _ in range(3):
                self.assertTrue(quality.record_task_quality(assistant, self.actor, self.turn, self.profile(),
                    score=80, evidence='reviewer', confirmed=True))
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(observe, assistants))
        self.assertEqual(quality.task_quality(self.assistant, self.actor, self.turn, self.profile())['samples'], 24)


class PersistedQualityTests(unittest.TestCase):
    def test_restart_retains_only_hashes_numbers_and_fixed_evidence_labels(self):
        with tempfile.TemporaryDirectory() as directory:
            store = AssistantStore(directory)
            model = CustomModel(store, protect=lambda value: value, unprotect=lambda value: value)
            actor = {'id': 'private-actor', 'scopes': ['A']}
            profile = fixtures.profile('current')
            store.put_document(NAMESPACE, 'model', {'enabled': True, 'models': [], 'image_input_default_version': 1})
            store.put_document(NAMESPACE, 'model:' + actor['id'], {'enabled': True, 'models': [profile],
                'active_model_id': 'current', 'shared_models_linked': True, 'image_input_default_version': 1})
            assistant = LighthouseAssistant(store, lambda *_: [], model=model)
            turn = {'question': 'private user question and prompt', 'task_type': 'mixed'}
            before = store._connect()
            schema = before.execute('SELECT sql FROM sqlite_master ORDER BY name').fetchall()
            before.close()
            for _ in range(3):
                self.assertTrue(quality.record_task_quality(assistant, actor, turn, profile,
                    score=88, evidence='deterministic_eval', confirmed=True))
            restarted = SimpleNamespace(store=AssistantStore(directory))
            self.assertEqual(quality.task_quality(restarted, actor, turn, profile)['score'], 88)
            saved = json.dumps(store.get_document(NAMESPACE, quality._QUALITY_DOCUMENT))
            for private in ('private-actor', 'private user', 'fixture-cipher', 'fixture.invalid', 'current'):
                self.assertNotIn(private, saved)
            after = store._connect()
            self.assertEqual(after.execute('SELECT sql FROM sqlite_master ORDER BY name').fetchall(), schema)
            after.close()

    def test_multiple_processes_do_not_overwrite_each_others_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            store = AssistantStore(directory)
            key = 'a' * 64
            with ProcessPoolExecutor(max_workers=3) as pool:
                results = list(pool.map(_process_observations, [(directory, key)] * 3))
            self.assertTrue(all(all(result) for result in results))
            self.assertEqual(quality.ModelHealth(store).quality_snapshot(key)['samples'], 12)

    def test_window_capacity_expiry_and_corrupt_storage_fail_softly(self):
        health = quality.ModelHealth()
        health.MAX_ENTRIES = 4
        now = [100.0]
        health._wall_clock = lambda: now[0]
        key = 'a' * 64
        for index in range(40):
            health.record_quality(key, score=index, evidence='reviewer')
        self.assertEqual(health.quality_snapshot(key)['samples'], 32)
        self.assertEqual(health.quality_snapshot(key)['score'], 23.5)
        for index in range(8):
            now[0] += 1
            health.record_quality(f'{index:064x}', score=90, evidence='deterministic_eval')
        self.assertEqual(len(health._quality), 4)
        self.assertEqual(health.quality_snapshot(key), {})
        now[0] += health.QUALITY_TTL
        self.assertEqual(health.quality_snapshot(f'{7:064x}'), {})
        with tempfile.TemporaryDirectory() as directory:
            store = AssistantStore(directory)
            health = quality.ModelHealth(store)
            with closing(store._connect()) as db, db:
                db.execute('INSERT INTO documents VALUES (?,?,?,?)', (NAMESPACE, quality._QUALITY_DOCUMENT, 'broken', 1))
            self.assertEqual(health.quality_snapshot(key), {})
            with self.assertLogs(quality.__name__, level='WARNING') as logs:
                self.assertFalse(health.record_quality(key, score=90, evidence='reviewer'))
            self.assertNotIn('broken', str(logs.output))


class PersistentObservationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = directory.name
        self.store = AssistantStore(self.root)
        self.actor = {'id': 'observation-owner', 'scopes': ['A']}
        self.profiles = [fixtures.profile('current'), fixtures.profile('backup')]
        self.store.put_document(NAMESPACE, 'model', {'enabled': True, 'models': [], 'image_input_default_version': 1})
        self.saved = {'enabled': True, 'models': self.profiles, 'active_model_id': 'current',
                      'shared_models_linked': True, 'image_input_default_version': 1}
        self.store.put_document(NAMESPACE, 'model:' + self.actor['id'], self.saved)
        self.assistant = LighthouseAssistant(self.store, lambda *_: [])
        self.turn = {'task_type': 'statistics'}

    def observation(self, index=0, **changes):
        profile = self.assistant.model_for(self.actor).profile('backup')
        return {'model_id': 'backup', 'fingerprint': quality.model_health_key(self.actor, profile)[1],
                'task_type': 'statistics', 'score': 90, 'confirmed': True,
                'observation_id': hashlib.sha256(str(index).encode()).hexdigest(), **changes}

    def ingest(self, row):
        return quality.ingest_task_evaluation(self.assistant, self.actor, **row)

    def snapshot(self):
        profile = self.assistant.model_for(self.actor).profile('backup')
        return quality.task_quality(self.assistant, self.actor, self.turn, profile)

    def test_duplicate_reload_and_changed_score_do_not_count_or_refresh_retention(self):
        row = self.observation()
        self.assertTrue(self.ingest(row))
        saved = self.store.get_document(NAMESPACE, quality._QUALITY_DOCUMENT)
        updated = self.store.get_document_updated_at(NAMESPACE, quality._QUALITY_DOCUMENT)
        self.assistant = LighthouseAssistant(AssistantStore(self.root), lambda *_: [])
        self.assertFalse(self.ingest(row))
        self.assertFalse(self.ingest({**row, 'score': 0}))
        self.assertEqual(self.snapshot()['samples'], 1)
        self.assertEqual(self.snapshot()['score'], 90)
        self.assertEqual(self.store.get_document(NAMESPACE, quality._QUALITY_DOCUMENT), saved)
        self.assertEqual(self.store.get_document_updated_at(NAMESPACE, quality._QUALITY_DOCUMENT), updated)

    def test_qualified_partial_import_resumes_and_provides_selection_evidence(self):
        from bin.test_lighthouse_live_evaluation import LighthouseLiveEvaluationTests
        from tools import evaluate_lighthouse_models as evaluation
        fixture = LighthouseLiveEvaluationTests()
        fixture.setUpClass()
        report = fixture.live_report()
        fingerprint = self.observation()['fingerprint']
        report['models'][0]['profile_fingerprint'] = fingerprint
        for row in report['models'][0]['cases']:
            row['profile_fingerprint'] = fingerprint
        failed = next(case for case in fixture.cases if case['task_type'] == 'statistics')
        next(row for row in report['models'][0]['cases'] if row['case_id'] == failed['id']).update(
            evaluation.score_answer(failed, 'invalid'))
        rows = evaluation.qualified_observations(report, current_fingerprints={fingerprint}, trusted=True)
        observations = [dict(model_id='backup', fingerprint=row['profile_fingerprint'], task_type=row['task_type'],
            score=row['score'], observation_id=row['observation_id'], confirmed=True)
            for row in rows if row['task_type'] == 'statistics']
        self.assertGreaterEqual(len(observations), 3)
        self.assertTrue(all(self.ingest(row) for row in observations[:2]))
        original = self.assistant.model_for(self.actor).profile('current')
        selected = quality.select_task_profile(self.assistant, self.actor, self.turn, {**original, '_automatic_selection': True})
        self.assertEqual(selected['id'], 'current')
        self.assistant = LighthouseAssistant(AssistantStore(self.root), lambda *_: [])
        self.assertEqual([self.ingest(row) for row in observations], [False, False] + [True] * (len(observations) - 2))
        selected = quality.select_task_profile(self.assistant, self.actor, self.turn, {**original, '_automatic_selection': True})
        self.assertEqual(selected['id'], 'backup')
        self.assertEqual(quality.task_quality(self.assistant, self.actor, self.turn, selected),
            {'score': 100 * (len(observations) - 1) / len(observations), 'samples': len(observations),
             'eligible': True, 'deterministic_eval': len(observations), 'reviewer': 0})
        self.assertEqual(quality.select_task_profile(self.assistant, self.actor, self.turn, original)['id'], 'current')
        self.assertEqual([self.ingest(row) for row in observations], [False] * len(observations))

    def test_invalid_observations_do_not_consume_ids(self):
        row = self.observation()
        invalid = [{'score': score} for score in (True, '90', -1, 101, float('nan'), float('inf'), -float('inf'))]
        invalid += [{'confirmed': value} for value in (False, 1, 'true')]
        invalid += [{'observation_id': value} for value in ('', 'private text', 'A' * 64, 1, [])]
        invalid += [{'fingerprint': 'b' * 64}, {'task_type': 'private text'}, {'model_id': 'unknown'}]
        for changes in invalid:
            with self.subTest(changes=changes):
                self.assertFalse(self.ingest({**row, **changes}))
        self.assertIsNone(self.store.get_document(NAMESPACE, quality._QUALITY_DOCUMENT))
        self.assertTrue(self.ingest({**row, 'score': 0}))
        self.assertTrue(self.ingest(self.observation(1, score=100)))
        self.assertEqual(self.snapshot()['score'], 50)

    def test_ids_are_scoped_to_actor_configuration_and_task(self):
        row = self.observation()
        self.assertTrue(self.ingest(row))
        other = {**self.actor, 'id': 'other-owner'}
        self.store.put_document(NAMESPACE, 'model:' + other['id'], self.saved)
        self.assertTrue(quality.ingest_task_evaluation(self.assistant, other, **row))
        self.assertTrue(self.ingest({**row, 'task_type': 'document'}))
        rotated = {**self.profiles[1], 'key_cipher': 'rotated-fixture-key'}
        self.store.put_document(NAMESPACE, 'model:' + self.actor['id'], {**self.saved, 'models': [self.profiles[0], rotated]})
        self.assertFalse(self.ingest(row))
        self.assertEqual(self.snapshot(), {})
        self.assertTrue(self.ingest(self.observation()))
        self.assertEqual(self.snapshot()['samples'], 1)
        self.assertFalse(self.ingest(self.observation()))

    def test_failed_transaction_commits_neither_id_nor_sample_and_can_retry(self):
        self.assertTrue(self.ingest(self.observation()))
        saved = self.store.get_document(NAMESPACE, quality._QUALITY_DOCUMENT)
        with closing(self.store._connect()) as db, db:
            db.execute("CREATE TRIGGER reject_quality BEFORE UPDATE ON documents "
                       "WHEN NEW.key = 'task_model_quality:v1' BEGIN SELECT RAISE(ABORT, 'fixture failure'); END")
        with self.assertLogs(quality.__name__, level='WARNING'):
            self.assertFalse(self.ingest(self.observation(1)))
        self.assertEqual(self.store.get_document(NAMESPACE, quality._QUALITY_DOCUMENT), saved)
        with closing(self.store._connect()) as db, db:
            db.execute('DROP TRIGGER reject_quality')
        self.assertTrue(self.ingest(self.observation(1)))
        self.assertFalse(self.ingest(self.observation(1)))
        self.assertEqual(self.snapshot()['samples'], 2)

    def test_thread_and_process_imports_accept_each_observation_once(self):
        observations = [self.observation(index) for index in range(3)]
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(_process_ingestion, [(self.root, self.actor, observations)] * 6))
        self.assertEqual(sum(sum(result) for result in results), 3)
        observations += [self.observation(index) for index in range(3, 6)]
        with ProcessPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(_process_ingestion, [(self.root, self.actor, observations)] * 3))
        self.assertEqual(sum(sum(result) for result in results), 3)
        self.assertEqual(self.snapshot()['samples'], 6)

    def test_seen_retention_is_bounded_and_independent_of_score_window(self):
        health = quality.ModelHealth()
        health.QUALITY_MAX_OBSERVATIONS = 4
        health.QUALITY_MAX_SAMPLES = 2
        now = [100.0]
        health._wall_clock = lambda: now[0]
        key = 'a' * 64
        for index in range(5):
            now[0] += 1
            self.assertTrue(health.record_quality(key, score=90, evidence='deterministic_eval', observation_id=f'{index:064x}'))
        self.assertEqual(set(health._quality[key]['seen']), {f'{index:064x}' for index in range(1, 5)})
        for _ in range(3):
            self.assertTrue(health.record_quality(key, score=50, evidence='reviewer'))
        self.assertFalse(health.record_quality(key, score=90, evidence='deterministic_eval', observation_id=f'{1:064x}'))
        self.assertEqual(health.quality_snapshot(key)['samples'], 2)
        now[0] += health.QUALITY_TTL - 2
        self.assertTrue(health.record_quality(key, score=50, evidence='reviewer'))
        self.assertTrue(health.record_quality(key, score=90, evidence='deterministic_eval', observation_id=f'{1:064x}'))
        self.assertFalse(health.record_quality(key, score=90, evidence='deterministic_eval', observation_id=f'{4:064x}'))


class NativeQualityTests(unittest.IsolatedAsyncioTestCase):
    async def test_initial_and_fallback_use_quality_without_extra_probes_or_connection_changes(self):
        fixture = fixtures.ModelFailoverTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        self.enterContext(patch.object(fixtures.native, 'GatewayClient', fixtures.FakeGatewayClient))
        fixture.configure(fixture.actor['id'], [*fixture.profiles, fixtures.profile('third')])
        agent = fixture.agent
        agent.turn.update(file_ids=[], task_type='statistics')
        assistant = fixture.engine.assistant
        for identity, score in (('current', 10), ('backup', 80), ('third', 95)):
            profile = assistant.model_for(fixture.actor).profile(identity)
            for _ in range(3):
                self.assertTrue(quality.record_task_quality(assistant, fixture.actor, agent.turn, profile,
                    score=score, evidence='deterministic_eval', confirmed=True))
        agent.profile['_automatic_selection'] = True
        original_turn, original_actor = copy.deepcopy(agent.turn), copy.deepcopy(agent.actor)
        session = agent.session_key
        fixture.fail_model('third')
        await fixture.run_agent()
        self.assertEqual(fixture.selected, ['third', 'backup'])
        self.assertEqual(agent._acquire_runtime.await_count, 2)
        self.assertEqual(agent.turn, original_turn)
        self.assertEqual(agent.actor, original_actor)
        self.assertEqual(agent.session_key, session)
        self.assertEqual(assistant.model_for(fixture.actor).settings()['active_model_id'], 'current')
        self.assertEqual(quality.task_quality(assistant, fixture.actor, agent.turn, agent.profile)['samples'], 3)
        self.assertFalse(fixture.engine.bridge.active)


if __name__ == '__main__':
    unittest.main()

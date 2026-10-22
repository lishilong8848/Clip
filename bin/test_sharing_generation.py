"""Only broken questions are repaired, with durable progress and no real side effects."""
import asyncio
import copy
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from bin import test_learning_sharing as fixture
from bin.test_learning_sharing_materials import local_only
from lan_bitable_template_portal.learning import LearningError, LearningService
from lan_bitable_template_portal.learning_sharing import SharingExercises, validate_questions
from lan_bitable_template_portal.learning_sharing_routes import SharingRuntime, install
from lan_bitable_template_portal.sharing_generation import generate_checked_questions, checked_questions, repair_candidate


def questions(count=5, start=0):
    return [{**q, 'stem': f'Knowledge {start + i}: ' + q['stem'], 'type': 'single', 'correct': [0]}
            for i, q in enumerate(fixture.questions(count))]


def state():
    return {'schema': 1, 'slots': [], 'offset': 0, 'exhausted': False, 'notes': []}


class GenerationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket.connect', local_only))
        self.source = {'text': fixture.SOURCE, 'title': 'Source snapshot', 'participants': []}
        self.batch = {'owner': fixture.ADMIN['id'], 'question_types': ['single']}
        self.state = state()
        self.snapshots = []

    async def checkpoint(self, qs, progress, message):
        self.snapshots.append((copy.deepcopy(qs), copy.deepcopy(progress), message))

    async def generate(self, pages, fixes):
        calls = []
        async def model(owner, source, previous, count, **kwargs):
            calls.append((owner, source, previous, count, copy.deepcopy(kwargs)))
            result = (fixes if kwargs.get('repair') else pages).pop(0)
            if isinstance(result, BaseException):
                raise result
            return copy.deepcopy(result)
        self.calls = calls
        return await generate_checked_questions(self.batch, self.source, self.state, model, self.checkpoint)

    async def test_repairs_only_bad_question_and_preserves_all_valid_slots_and_order(self):
        original = questions()
        generated = copy.deepcopy(original)
        generated[1]['evidence'] = 'Invented evidence, not present in the source'
        final = await self.generate([generated, []], [[original[1]]])
        self.assertEqual(len(final), 5)
        expected = validate_questions([{**q, 'origin': 'ai'} for q in original], fixture.SOURCE)
        self.assertEqual(final, expected)
        repairs = [call for call in self.calls if call[4].get('repair')]
        self.assertEqual(len(repairs), 1)
        self.assertEqual(repairs[0][3], 1)
        self.assertEqual(repairs[0][4]['question_types'], ['single'])
        self.assertEqual(repairs[0][4]['repair']['question']['stem'], original[1]['stem'])
        self.assertEqual(set(repairs[0][2]), {q['stem'] for i, q in enumerate(original) if i != 1})
        self.assertTrue(any(len(qs) == 4 for qs, _, _ in self.snapshots), 'good questions checkpointed before repair call')
        self.assertEqual([s.get('repaired', False) for s in self.state['slots']], [False, True, False, False, False])

    async def test_repair_reads_both_body_and_image_sources_but_cannot_confirm_vision(self):
        image = {'id': 'a' * 64, 'text': '图中水泵运行频率显示33HZ，水流状态显示否。', 'mode': 'vision'}
        self.source['images'] = [image]
        original = questions()
        fixed = {**original[0], 'image_ids': [image['id']], 'evidence': image['text'], 'image_confirmed': True}
        original[0].update(image_ids=['b' * 64], evidence='Not found in body or image')
        final = await self.generate([original, []], [[fixed]])
        repair = next(call for call in self.calls if call[4].get('repair'))
        self.assertEqual(repair[1], fixture.SOURCE)
        self.assertEqual(repair[4]['source_images'], [image])
        self.assertEqual(final[0]['image_ids'], [image['id']])
        self.assertFalse(final[0]['image_confirmed'])
        with self.assertRaisesRegex(LearningError, '对照原图'):
            validate_questions(final, fixture.SOURCE, ['single'], images=[image])

    async def test_second_attempt_regenerates_after_invalid_or_duplicate_first_repair(self):
        original = questions()
        generated = copy.deepcopy(original)
        generated[0]['evidence'] = 'Missing evidence'
        fixed = {**original[0], 'stem': 'Replacement topic from the same reliable source'}
        final = await self.generate([generated, []], [[original[3]], [fixed]])
        repairs = [call[4]['repair'] for call in self.calls if call[4].get('repair')]
        self.assertEqual([r['strategy'] for r in repairs], ['revise', 'replace'])
        self.assertIn('重复', repairs[1]['reason'])
        self.assertEqual(final[0]['stem'], fixed['stem'])
        self.assertEqual([q['stem'] for q in final[1:]], [q['stem'] for q in original[1:]])

    async def test_rejects_bulk_rewrite_reply_then_accepts_only_one_replacement(self):
        original = questions()
        generated = copy.deepcopy(original)
        generated[0]['correct'] = [99]
        final = await self.generate([generated, []], [questions(5, 100), [original[0]]])
        self.assertEqual([q['stem'] for q in final], [q['stem'] for q in original])
        self.assertEqual(self.state['slots'][0]['attempts'], 2)

    async def test_unrepairable_question_does_not_drop_good_questions_and_stays_invalid(self):
        generated = questions()
        generated[2].update(origin='manual', evidence='Model cannot bypass grounding by changing origin')
        more = questions(1, 6)
        final = await self.generate([generated, more], [[generated[2]], [generated[2]]])
        self.assertEqual(len(final), 5)
        self.assertNotIn(generated[2]['stem'], [q['stem'] for q in final])
        self.assertEqual(self.state['slots'][2]['status'], 'discarded')
        self.assertEqual(self.state['slots'][2]['attempts'], 2)
        self.assertTrue(all(q['origin'] == 'ai' for q in final))

    async def test_timeout_and_malformed_candidate_are_bounded_and_no_secret_error_is_stored(self):
        generated = questions()
        generated[0] = None
        final = await self.generate([generated, questions(1, 6)], [TimeoutError('private credential'), []])
        self.assertEqual(len(final), 5)
        repair = next(call[4]['repair'] for call in self.calls if call[4].get('repair'))
        self.assertEqual(repair['question'], {})
        self.assertNotIn('private credential', json.dumps(self.state))
        self.assertEqual(self.state['slots'][0]['attempts'], 2)

    async def test_wrong_type_is_repaired_without_affecting_matching_items(self):
        original = questions()
        generated = copy.deepcopy(original)
        generated[4].update(type='multiple', correct=[0, 2])
        final = await self.generate([generated, []], [[original[4]]])
        self.assertEqual([q['type'] for q in final], ['single'] * 5)
        self.assertEqual(sum(bool(s.get('repaired')) for s in self.state['slots']), 1)

    async def test_all_invalid_never_passes_minimum_and_never_loops_forever(self):
        generated = questions()
        for q in generated:
            q['evidence'] = 'Missing evidence'
        with self.assertRaisesRegex(LearningError, '不足5题'):
            await self.generate([generated, []], [[] for _ in range(10)])
        self.assertEqual(len([call for call in self.calls if call[4].get('repair')]), 10)
        self.assertEqual(checked_questions(self.state, self.source, ['single']), [])
        self.assertTrue(all(slot['status'] == 'discarded' for slot in self.state['slots']))

    async def test_good_questions_need_no_repair_or_changes(self):
        original = questions()
        final = await self.generate([original, []], [])
        self.assertEqual(len(self.calls), 2)
        self.assertFalse(any(call[4].get('repair') for call in self.calls))
        self.assertEqual([q['stem'] for q in final], [q['stem'] for q in original])

    def test_repair_context_omits_unknown_and_oversized_payloads(self):
        value = repair_candidate({'stem': 'A', 'data_url': 'private', 'secret': 'private', 'options': ['A', 'B']})
        self.assertNotIn('private', json.dumps(value))
        self.assertEqual(repair_candidate({'stem': 'A', 'evidence': 'x' * 13000}), {'stem': 'A'})


class GenerationRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket.connect', local_only))
        self.fixture = fixture.SharingTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.learning, self.core, self.bid = self.fixture.learning, self.fixture.core, self.fixture.bid
        self.source = {'text': fixture.SOURCE, 'title': 'Original source snapshot', 'participants': []}
        self.original = questions()
        with self.learning.transaction() as conn:
            batch = self.core.batch(self.bid, conn)
            batch.update(status='queued', questions=[], question_types=['single'], source={})
            self.learning._put('sharing_batch', self.bid, batch, conn)
        self.reader = self.enterContext(patch('lan_bitable_template_portal.learning_sharing_routes.read_source', return_value=copy.deepcopy(self.source)))

    def worker(self, model, learning=None):
        return SharingRuntime(SimpleNamespace(service=object(), generate_sharing_questions=model), lambda: learning or self.learning)

    def no_side_effects(self):
        self.assertEqual(self.core.attempts(self.bid), [])
        self.assertEqual(self.learning._all('sharing_delivery'), [])

    async def test_cancellation_restart_continues_bad_question_without_rereading_source_or_regenerating_good(self):
        generated = copy.deepcopy(self.original)
        generated[1]['evidence'] = 'Missing evidence'
        async def interrupted(*args, **kwargs):
            if kwargs.get('repair'):
                raise asyncio.CancelledError()
            return generated
        with self.assertRaises(asyncio.CancelledError):
            await self.worker(interrupted).generate(self.core.batch(self.bid))
        saved = self.core.batch(self.bid)
        self.assertEqual(saved['status'], 'queued')
        self.assertEqual(len(saved['questions']), 4)
        self.assertEqual(saved['generation_state']['slots'][1]['attempts'], 1)
        restarted = LearningService(self.learning.root)
        calls = []
        async def finish(*args, **kwargs):
            calls.append(copy.deepcopy(kwargs))
            return [self.original[1]] if kwargs.get('repair') else []
        self.reader.side_effect = AssertionError('Must use saved source, not live changed material')
        worker = self.worker(finish, restarted)
        await worker.generate(worker.service().batch(self.bid))
        final = worker.service().batch(self.bid)
        self.assertEqual(final['status'], 'draft', final.get('error'))
        self.assertEqual([q['stem'] for q in final['questions']], [q['stem'] for q in self.original])
        self.assertEqual(final['source']['text'], fixture.SOURCE)
        self.assertTrue(calls[0].get('repair'))
        self.assertEqual(self.reader.call_count, 1)
        self.no_side_effects()

    async def test_later_model_failure_preserves_completed_page_and_resumes_at_next_page(self):
        model = AsyncMock(side_effect=[self.original, TimeoutError()])
        await self.worker(model).generate(self.core.batch(self.bid))
        saved = self.core.batch(self.bid)
        self.assertEqual(saved['status'], 'failed')
        self.assertEqual(len(saved['questions']), 5)
        self.assertEqual(saved['generation_state']['offset'], 5)
        more = questions(1, 6)
        retry = AsyncMock(return_value=more)
        self.reader.side_effect = AssertionError('No source reread')
        await self.worker(retry).generate(saved)
        final = self.core.batch(self.bid)
        self.assertEqual(final['status'], 'draft')
        self.assertEqual(final['questions'][:5], saved['questions'])
        self.assertEqual(len(final['questions']), 6)
        self.assertEqual(retry.await_args.args[2], [q['stem'] for q in self.original])
        self.no_side_effects()

    async def test_deleted_or_newer_batch_cannot_be_overwritten_by_late_repair_result(self):
        for delete in (False, True):
            with self.subTest(delete=delete):
                generated = copy.deepcopy(self.original)
                generated[0]['evidence'] = 'Missing evidence'
                async def model(*args, **kwargs):
                    if kwargs.get('repair'):
                        if delete:
                            self.core.delete(self.bid, self.core.batch(self.bid)['version'], fixture.ADMIN)
                        else:
                            with self.learning.transaction() as conn:
                                batch = self.core.batch(self.bid, conn)
                                batch.update(version=batch['version'] + 1, status='draft', title='Newer version')
                                self.learning._put('sharing_batch', self.bid, batch, conn)
                        return [self.original[0]]
                    return generated
                with self.learning.transaction() as conn:
                    batch = self.learning._get('sharing_batch', self.bid, conn)
                    batch.update(status='queued', generation_state={}, questions=[])
                    self.learning._put('sharing_batch', self.bid, batch, conn)
                with self.assertRaises(asyncio.CancelledError):
                    await self.worker(model).generate(self.core.batch(self.bid))
                final = self.learning._get('sharing_batch', self.bid)
                self.assertEqual(final['status'], 'deleted' if delete else 'draft')
                if not delete:
                    self.assertEqual(final['title'], 'Newer version')
                self.no_side_effects()

    async def test_interrupted_durable_batch_automatically_requeues_but_old_uncheckpointed_batch_does_not(self):
        progress = state()
        progress['slots'] = [{'status': 'valid', 'question': {**q, 'origin': 'ai'}, 'expected': 'single', 'attempts': 0} for q in self.original]
        progress['offset'] = 5
        with self.learning.transaction() as conn:
            batch = self.core.batch(self.bid, conn)
            batch.update(status='generating', source=self.source, generation_state=progress)
            self.learning._put('sharing_batch', self.bid, batch, conn)
            self.learning._put('sharing_batch', 'legacy', {**batch, 'id': 'legacy', 'generation_state': {}}, conn)
        worker = self.worker(AsyncMock(return_value=[]))
        worker.active_batch = self.bid
        worker.generation = asyncio.get_running_loop().create_future()
        worker.recover_interrupted()
        self.assertEqual(self.core.batch(self.bid)['status'], 'generating', 'live task must not be duplicated')
        worker.generation.set_result(None)
        worker.recover_interrupted()
        self.assertEqual(self.core.batch(self.bid)['status'], 'queued')
        self.assertEqual(self.core.batch('legacy')['status'], 'failed')
        await worker.generate(self.core.batch(self.bid))
        self.assertEqual(self.core.batch(self.bid)['status'], 'draft')
        self.no_side_effects()

    async def test_generation_preserves_exemptions_and_publish_never_assigns_exempted_people(self):
        self.source['participants'] = [
            {'name': 'Assigned', 'open_id': 'ou_assigned', 'union_id': 'on_assigned'},
            {'name': 'Exempted', 'open_id': 'ou_exempted', 'union_id': 'on_exempted'}]
        self.reader.return_value = copy.deepcopy(self.source)
        with self.learning.transaction() as conn:
            batch = self.core.batch(self.bid, conn)
            batch['excluded_open_ids'] = ['ou_exempted']
            self.learning._put('sharing_batch', self.bid, batch, conn)
        await self.worker(AsyncMock(side_effect=[self.original, []])).generate(self.core.batch(self.bid))
        self.assertEqual(self.core.batch(self.bid)['excluded_open_ids'], ['ou_exempted'])
        self.no_side_effects()
        self.fixture.publish()
        self.assertEqual([attempt['open_id'] for attempt in self.core.attempts(self.bid)], ['ou_assigned'])
        excluded = next(person for person in self.core.detail(self.bid, fixture.ADMIN)['participants'] if person['open_id'] == 'ou_exempted')
        self.assertTrue(excluded['exempted'])

    async def test_explicit_retry_resets_only_unresolved_attempts_and_preserves_source_and_good_questions(self):
        from fastapi import FastAPI
        from fastapi.responses import JSONResponse
        from fastapi.testclient import TestClient
        generated = copy.deepcopy(self.original)
        generated[0]['evidence'] = 'Missing evidence'
        async def model(*args, **kwargs):
            return [] if kwargs.get('repair') or args[2] else generated
        await self.worker(model).generate(self.core.batch(self.bid))
        saved = self.core.batch(self.bid)
        self.assertEqual(saved['status'], 'failed')
        self.assertEqual(len(saved['questions']), 4)
        self.assertEqual(saved['generation_state']['slots'][0]['status'], 'discarded')
        app = FastAPI()
        controller = SimpleNamespace(_auth_required_response=lambda: JSONResponse({}, status_code=401),
            _json_ok=lambda request, session, data: JSONResponse({'ok': True, 'data': data}))
        async def body(request): return await request.json()
        runtime = SimpleNamespace(service=object())
        install(app, controller, runtime, lambda: self.learning, lambda _: (object(), fixture.ADMIN), body, Mock(),
            lambda exc: JSONResponse({'error': str(exc)}, status_code=exc.status if isinstance(exc, LearningError) else 500))
        client = TestClient(app, raise_server_exceptions=True)
        self.addCleanup(client.close)
        response = client.post(f'/api/learning/sharing/{self.bid}/retry', json={'version': saved['version']})
        self.assertEqual(response.status_code, 200, response.text)
        queued = self.core.batch(self.bid)
        self.assertEqual(queued['status'], 'queued')
        self.assertEqual(queued['questions'], saved['questions'])
        self.assertEqual(queued['source'], saved['source'])
        self.assertEqual(queued['generation_state']['slots'][0]['status'], 'pending')
        self.assertEqual(queued['generation_state']['slots'][0]['attempts'], 0)
        self.assertEqual(queued['generation_state']['slots'][1:], saved['generation_state']['slots'][1:])
        repeated = client.post(f'/api/learning/sharing/{self.bid}/retry', json={'version': saved['version']})
        self.assertEqual(repeated.status_code, 409)
        self.assertEqual(self.core.batch(self.bid)['generation_state'], queued['generation_state'])
        fixed = AsyncMock(return_value=[self.original[0]])
        self.reader.side_effect = AssertionError('No live source reread')
        await self.worker(fixed).generate(queued)
        final = self.core.batch(self.bid)
        self.assertEqual(final['status'], 'draft', final.get('error'))
        self.assertEqual(len(final['questions']), 5)
        self.assertEqual(fixed.await_count, 1)
        self.no_side_effects()

    async def test_generator_cannot_overwrite_published_content_or_enqueue_more_deliveries(self):
        with self.learning.transaction() as conn:
            batch = self.core.batch(self.bid, conn)
            batch.update(status='draft', questions=validate_questions(self.original, fixture.SOURCE), source=self.source,
                participants=[{'name': 'Person', 'open_id': 'ou_person0', 'union_id': 'on_person0', 'scope': 'A', 'person_id': 'p0'}])
            self.learning._put('sharing_batch', self.bid, batch, conn)
        self.fixture.publish()
        before = self.core.batch(self.bid)
        deliveries = self.learning._all('sharing_delivery')
        model = AsyncMock(side_effect=AssertionError('No model after publish'))
        await self.worker(model).generate(before)
        self.assertEqual(self.core.batch(self.bid), before)
        self.assertEqual(self.learning._all('sharing_delivery'), deliveries)
        model.assert_not_called()

    async def test_cloud_restore_keeps_checked_questions_and_pending_repair_snapshot(self):
        import tempfile
        generated = copy.deepcopy(self.original)
        generated[0]['evidence'] = 'Missing evidence'
        async def paused(*args, **kwargs):
            if kwargs.get('repair'):
                raise asyncio.CancelledError()
            return generated
        with self.assertRaises(asyncio.CancelledError):
            await self.worker(paused).generate(self.core.batch(self.bid))
        saved = self.core.batch(self.bid)
        entity = {'kind': 'sharing_batch', 'key': self.bid, 'payload': saved}
        with tempfile.TemporaryDirectory() as root:
            learning = LearningService(root, cloud=SimpleNamespace(load_entities=lambda: [entity]))
            learning.restore()
            restored = SharingExercises(learning).batch(self.bid)
            self.assertEqual(restored['generation_state'], saved['generation_state'])
            self.assertEqual(restored['questions'], saved['questions'])
            self.assertEqual(restored['source'], saved['source'])
            async def finish(*args, **kwargs):
                return [self.original[0]] if kwargs.get('repair') else []
            self.reader.side_effect = AssertionError('Must use restored source snapshot')
            worker = self.worker(finish, learning)
            await worker.generate(restored)
            self.assertEqual(worker.service().batch(self.bid)['status'], 'draft')
            self.assertEqual(len(worker.service().batch(self.bid)['questions']), 5)
            self.assertEqual(learning._all('sharing_attempt'), [])
            self.assertEqual(learning._all('sharing_delivery'), [])


if __name__ == '__main__':
    unittest.main()

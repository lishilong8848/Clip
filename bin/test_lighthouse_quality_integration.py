"""Real answer routing/validators with synthetic model and retrieval boundaries."""
import copy
import json
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI
from pydantic_ai import ModelRetry
from bin import test_lighthouse_openclaw as agent_fixtures
from bin.test_lighthouse_model_failover import profile
from openclaw_service.assistant import lighthouse_model_health as quality
from openclaw_service.assistant.lighthouse_ai import AssistantError, CustomModel, LighthouseAssistant, NAMESPACE
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
from openclaw_service.assistant.lighthouse_knowledge_answer import answer_company
from openclaw_service.assistant.lighthouse_model import LighthouseModel
from openclaw_service.assistant.lighthouse_openclaw import profile_model
from openclaw_service.assistant.lighthouse_queries import PortalOperation
from openclaw_service.assistant.lighthouse_review import AnswerReviewer


class QualityIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('Network forbidden')))
        self.actor = {'id': 'quality-integration', 'scopes': ['D']}
        self.store = agent_fixtures.Store(Path('unused-quality-integration.sqlite3'))
        self.profiles = [profile('current'), profile('backup')]
        self.store.put_document(NAMESPACE, 'model', {'enabled': True, 'models': [], 'image_input_default_version': 1})
        self.store.put_document(NAMESPACE, 'model:' + self.actor['id'], {
            'enabled': True, 'models': self.profiles, 'active_model_id': 'current',
            'shared_models_linked': True, 'image_input_default_version': 1})
        self.model = CustomModel(self.store, client=Mock())
        self.model.complete = Mock(return_value=json.dumps({'answer': 'The synthetic rule applies.[1]', 'queries': [], 'gaps': []}))
        self.assistant = LighthouseAssistant(self.store, lambda *_: [], model=self.model)
        self.knowledge = SimpleNamespace(
            search=Mock(return_value={'items': [{'name': 'Synthetic rule', 'document_id': 'fixture-doc',
                'version': 1, 'ordinal': 0, 'location': 'paragraph 1', 'text': 'The synthetic rule applies.',
                'url': '/knowledge-base?document=fixture-doc'}], 'warning': ''}),
            evidence_current=Mock(return_value=True))
        app = FastAPI()

        @app.get('/api/repair-management/records')
        async def records(scope: str):
            raise AssertionError('Only the synthetic read boundary may be invoked')

        self.portal = SimpleNamespace(assistant=self.assistant, catalog=PortalAPICatalog(app),
            files=SimpleNamespace(context=lambda *_: [], image_parts=lambda *_: []),
            get_knowledge=lambda: self.knowledge, _public_references=lambda result, _: result,
            _source_url=lambda *_: '/fixture-records',
            _invoke=AsyncMock(return_value={'ok': True, 'data': {
                'items': [{'label': 'Synthetic record', 'value': 7}], 'complete': True}}))
        self.unexpected_agent = Mock(side_effect=AssertionError('Stateless query must not start an agent'))
        self.engine = LighthouseModel(self.portal, model_factory=profile_model, agent_factory=self.unexpected_agent)
        self.authorize = AsyncMock(side_effect=lambda: copy.deepcopy(self.actor))
        self.emit = AsyncMock()

    def selected_profile(self, identity='current'):
        return self.assistant.model_for(self.actor).profile(identity)

    def turn(self, *, automatic=True):
        return {'question': 'Inspect the applicable equipment rule', 'operation_id': 'quality-integration-op',
                '_profile': {**self.selected_profile(), '_automatic_selection': automatic}}

    def intent(self, turn, kind='knowledge'):
        return {'kind': kind, 'query': turn['question'], 'continued': False,
                'task_type': {'knowledge': 'document', 'mixed': 'mixed', 'business': 'statistics'}[kind],
                'business_query': 'Inspect the synthetic records' if kind == 'mixed' else '',
                'document_query': turn['question'] if kind == 'mixed' else '', 'attempts': 1}

    def cool_all(self):
        health = quality.model_health(self.assistant)
        for identity in ('current', 'backup'):
            health.record(quality.model_health_key(self.actor, self.selected_profile(identity)), category='model_auth')

    async def answer(self, turn, kind='knowledge'):
        with patch('openclaw_service.assistant.lighthouse_query_intent.resolve_query_intent',
                   new=AsyncMock(return_value=self.intent(turn, kind))):
            return await self.engine.answer(self.actor, turn, [], None, self.emit, self.authorize, {})

    async def test_all_cooling_document_and_mixed_stop_before_actual_generation(self):
        self.cool_all()
        for kind in ('knowledge', 'mixed'):
            with self.subTest(kind=kind):
                with self.assertRaises(AssistantError) as raised:
                    await self.answer(self.turn(), kind)
                self.assertEqual(raised.exception.status, 503)
        self.model.complete.assert_not_called()
        self.knowledge.search.assert_not_called()
        self.portal._invoke.assert_not_awaited()
        self.unexpected_agent.assert_not_called()

    async def test_explicit_stateless_choice_is_honored_despite_cooldown_and_better_scores(self):
        for identity, score in (('current', 0), ('backup', 100)):
            for _ in range(3):
                self.assertTrue(quality.record_task_quality(self.assistant, self.actor, {'task_type': 'document'},
                    self.selected_profile(identity), score=score, evidence='deterministic_eval', confirmed=True))
        self.cool_all()
        turn = self.turn(automatic=False)
        turn.update(_actual_profile=self.selected_profile('backup'), _quality_review={'status': 'needs_review'})
        result = await self.answer(turn)
        self.model.complete.assert_called_once()
        self.assertEqual(self.model.complete.call_args.kwargs['profile']['id'], 'current')
        self.assertEqual(turn['_actual_profile']['id'], 'current')
        self.assertNotIn('_quality_review', turn)
        self.assertIn('synthetic rule', result['answer'])
        self.unexpected_agent.assert_not_called()

    async def test_automatic_stateless_choice_uses_document_quality_and_actual_profile(self):
        for _ in range(3):
            self.assertTrue(quality.record_task_quality(self.assistant, self.actor, {'task_type': 'document'},
                self.selected_profile('backup'), score=100, evidence='deterministic_eval', confirmed=True))
        turn = self.turn()
        await self.answer(turn)
        self.assertEqual(turn['task_type'], 'document')
        self.assertEqual(self.model.complete.call_args.kwargs['profile']['id'], 'backup')
        self.assertEqual(turn['_actual_profile'], self.model.complete.call_args.kwargs['profile'])

    async def test_stateless_generation_clears_previous_phase_quality_review(self):
        turn = self.turn(automatic=False)
        turn['_quality_review'] = {'status': 'needs_review'}
        await answer_company(self.engine, self.actor, turn, turn['question'], self.emit, self.authorize)
        self.assertNotIn('_quality_review', turn)
        self.assertEqual(turn['_actual_profile']['id'], 'current')

    async def validator_sequence(self, *, switch=True):
        owner = self
        agents = []
        reviewer = AnswerReviewer(self.assistant)
        reviewed_profiles = []

        async def review(actor, turn, history, result, authorize, **kwargs):
            await authorize()
            reviewed_profiles.append(agents[-1].profile['id'])
            if len(reviewed_profiles) == 1:
                return {**result, 'answer_review': {'status': 'needs_repair', 'repairs': 0,
                    'issues': ['The synthetic answer omitted the returned value.']}}
            return {**result, 'answer_review': {'status': 'passed', 'repairs': 0}}

        reviewer.run = AsyncMock(side_effect=review)

        class ConsecutiveProfiles(agent_fixtures.FakeAgent):
            def __init__(self, model, **kwargs):
                super().__init__(model, **kwargs)
                self.profile = dict(model)

            @asynccontextmanager
            async def run_stream_events(self, prompt, *, message_history=None, **kwargs):
                async def events():
                    read = await self.tools['query'](PortalOperation(
                        api_id='GET /api/repair-management/records', params={'scope': 'D'}))
                    owner.assertTrue(read['ok'])
                    owner.assertEqual(len(self.validators), 1)
                    with owner.assertRaises(ModelRetry):
                        await self.validators[0]('The synthetic record was returned.[1]')
                    owner.assertEqual(reviewer.run.await_count, 1)
                    if switch:
                        self.profile = quality.select_task_profile(owner.assistant, owner.actor, self.kwargs['turn'],
                            self.profile, attempted={quality.profile_key(self.profile)}, initial=False)
                        owner.assertEqual(self.profile['id'], 'backup')
                    final = await self.validators[0]('The synthetic record has value 7.[1]')
                    yield SimpleNamespace(event_kind='agent_run_result', result=SimpleNamespace(output=final))
                yield events()

        def factory(model, **kwargs):
            agent = ConsecutiveProfiles(model, **kwargs)
            agents.append(agent)
            return agent

        self.engine.agent_factory = factory
        self.engine.answer_reviewer = reviewer
        turn = self.turn(automatic=False)
        turn['question'] = 'Inspect the synthetic repair records and their value'
        turn['_quality_review'] = {'status': 'needs_review'}
        result = await self.answer(turn, 'business')
        self.portal._invoke.assert_awaited_once()
        self.model.complete.assert_not_called()
        self.assertEqual(reviewer.run.await_count, 2)
        self.assertEqual(reviewed_profiles, ['current', 'backup' if switch else 'current'])
        self.assertEqual(result['answer_review'], {'status': 'corrected', 'repairs': 1})
        self.assertEqual(turn['_profile']['id'], 'current')
        self.assertEqual(turn['_actual_profile'], agents[-1].profile)
        return turn

    async def test_fallback_b_passes_without_inheriting_a_repair_penalty(self):
        turn = await self.validator_sequence()
        self.assertEqual(turn['_quality_review']['status'], 'passed')
        self.assertEqual(turn['_actual_profile']['id'], 'backup')
        self.assertEqual(quality.task_quality(self.assistant, self.actor, turn, turn['_actual_profile']),
            {'score': 100, 'samples': 1, 'eligible': False, 'deterministic_eval': 0, 'reviewer': 1})
        self.assertEqual(quality.task_quality(self.assistant, self.actor, turn, self.selected_profile()), {})

    async def test_same_model_repair_retains_its_own_penalty(self):
        turn = await self.validator_sequence(switch=False)
        self.assertEqual(turn['_quality_review']['status'], 'corrected')
        self.assertEqual(quality.task_quality(self.assistant, self.actor, turn, turn['_actual_profile'])['score'], 80)


if __name__ == '__main__':
    unittest.main()

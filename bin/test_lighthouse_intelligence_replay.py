"""Cross-channel replay of complete tasks using only synthetic business receipts."""
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pydantic_ai.models.function import DeltaToolCall
from bin import test_lighthouse_intelligence_evaluation as evaluation
from bin import test_lighthouse_model_failover as failover
from openclaw_service.assistant.lighthouse_model_health import model_health_key
from openclaw_service.assistant.lighthouse_query_progress import ReadRepairBudget, query_progress


class TaskReplayTests(unittest.IsolatedAsyncioTestCase):
    evaluate = evaluation.IntelligenceEvaluationTests.evaluate

    async def test_invalid_model_plan_can_be_corrected_without_aborting_turn(self):
        step = 0
        goal = {'id': 'explain', 'description': '解释概念', 'kind': 'explanation'}
        async def script(messages, info):
            nonlocal step
            step += 1
            if step in (1, 2):
                if step == 2:
                    self.assertFalse(messages[-1].parts[0].content['ok'])
                yield {0: DeltaToolCall(name='plan_task', json_args=json.dumps({'spec': {
                    'deliverables': [goal, goal] if step == 1 else [goal]}}))}
            elif step == 3:
                self.assertTrue(messages[-1].parts[0].content['ok'])
                yield {0: DeltaToolCall(name='review_answer', json_args=json.dumps({
                    'answer': 'Python列表用于保存有序元素。', 'goals': [{'goal_id': 'explain',
                        'description': '解释概念', 'kind': 'explanation', 'status': 'complete'}]}))}
            else:
                yield 'done'
        result, calls, _ = await self.evaluate('feishu:oc_replay', '解释Python列表', script, lambda *_: {})
        self.assertIn('Python', result['answer'])
        self.assertEqual(calls, [])

    async def test_two_deliverables_checked_once_for_web_and_feishu_across_domains(self):
        for channel in ('', 'feishu:oc_replay'):
            for api, question in (
                    ('GET /api/assistant/work-orders', 'D楼工单参与人数和人员名单分别是多少？'),
                    ('GET /api/learning/papers', 'D楼学练题单参与人数和人员名单分别是多少？')):
                with self.subTest(channel=channel, api=api):
                    step, reference = 0, ''
                    spec = {'scopes': ['D'], 'counting_basis': '按人员id去重', 'deliverables': [
                        {'id': 'count', 'description': '人数', 'kind': 'data'},
                        {'id': 'names', 'description': '名单', 'kind': 'data'}]}
                    async def script(messages, info):
                        nonlocal step, reference
                        step += 1
                        if step == 1:
                            name, args = 'plan_task', {'spec': spec}
                        elif step == 2:
                            self.assertTrue(messages[-1].parts[0].content['ok'])
                            name, args = 'query', {'complete': True, 'operation': {'api_id': api, 'params': {'scope': 'D'}}}
                        elif step == 3:
                            reference = messages[-1].parts[0].content['query_ref']
                            name, args = 'query_progress', {}
                        elif step == 4:
                            self.assertEqual(messages[-1].parts[0].content['gaps'], [])
                            name, args = 'query_statistics', {'query_ref': reference, 'path': ['items'], 'operation': 'distinct', 'field': ['person', 'id']}
                        elif step in (5, 6):
                            if step == 5:
                                self.assertEqual(messages[-1].parts[0].content['value'], 2)
                            else:
                                self.assertFalse(messages[-1].parts[0].content['ready'])
                            goals = spec['deliverables'][:1] if step == 5 else spec['deliverables']
                            name, args = 'review_answer', {'answer': '共有2人：测试甲、测试乙。', 'goals': [
                                {'goal_id': goal['id'], 'description': goal['description'], 'kind': 'data',
                                 'status': 'complete', 'query_refs': [reference]} for goal in goals]}
                        else:
                            self.assertTrue(messages[-1].parts[0].content['ready'])
                            yield 'done'
                            return
                        yield {0: DeltaToolCall(name=name, json_args=json.dumps(args))}
                    def rows(*_):
                        return {'ok': True, 'data': {'complete': True, 'total': 3, 'items': [
                            {'id': '1', 'person': {'id': 'a', 'name': '测试甲'}},
                            {'id': '2', 'person': {'id': 'a', 'name': '测试甲'}},
                            {'id': '3', 'person': {'id': 'b', 'name': '测试乙'}}]}}
                    result, calls, events = await self.evaluate(channel, question, script, rows)
                    self.assertIn('测试甲', result['answer'])
                    self.assertIn('测试乙', result['answer'])
                    self.assertEqual(len(calls), 1)
                    self.assertEqual(result['_intelligence_metrics']['checked_goals'], 2)
                    self.assertEqual(result['_intelligence_metrics']['planned_goals'], 2)
                    self.assertEqual(result['_task_context']['contract']['counting_basis'], spec['counting_basis'])
                    self.assertEqual(''.join(data['delta'] for kind, data in events if kind == 'text'), result['answer'])

    async def test_planning_cannot_erase_a_missing_goal_to_pass_review(self):
        step = 0
        goals = [{'id': 'a', 'description': '说明原理', 'kind': 'explanation'},
                 {'id': 'b', 'description': '说明限制', 'kind': 'explanation'}]
        async def script(messages, info):
            nonlocal step
            step += 1
            if step <= 2:
                yield {0: DeltaToolCall(name='plan_task', json_args=json.dumps({'spec': {
                    'deliverables': goals if step == 1 else goals[:1]}}))}
            elif step == 3:
                self.assertFalse(messages[-1].parts[0].content['ok'])
                yield {0: DeltaToolCall(name='review_answer', json_args=json.dumps({
                    'answer': '原理及限制均已说明。', 'goals': [{'goal_id': g['id'],
                        'description': g['description'], 'kind': 'explanation', 'status': 'complete'} for g in goals]}))}
            else:
                yield 'done'
        result, calls, _ = await self.evaluate('feishu:oc_replay', '解释Python列表的原理及限制', script, lambda *_: {})
        self.assertEqual(len(result['_task_context']['contract']['deliverables']), 2)
        self.assertEqual(calls, [])

    async def test_claimed_blocked_goal_is_independently_checked_against_existing_data(self):
        for channel in ('', 'feishu:oc_replay'):
            step, reference = 0, ''
            goals = [{'id': 'count', 'description': '人数', 'kind': 'data'},
                     {'id': 'names', 'description': '名单', 'kind': 'data'}]
            async def script(messages, info):
                nonlocal step, reference
                step += 1
                if step == 1:
                    name, args = 'plan_task', {'spec': {'deliverables': goals}}
                elif step == 2:
                    name, args = 'query', {'complete': True, 'operation': {'api_id': 'GET /api/assistant/work-orders', 'params': {'scope': 'D'}}}
                elif step in (3, 5):
                    if step == 3:
                        reference = messages[-1].parts[0].content['query_ref']
                    else:
                        self.assertIn('已有姓名', str(messages[-1]))
                    name, args = 'review_answer', {'answer': '共1人，名单无法提供。' if step == 3 else '共1人：测试甲。', 'goals': [
                        {'goal_id': g['id'], 'description': g['description'], 'kind': 'data',
                         'status': 'blocked' if g['id'] == 'names' and step == 3 else 'complete',
                         'limitation': '无法列出' if step == 3 else '', 'query_refs': [reference]} for g in goals]}
                else:
                    yield 'done'
                    return
                yield {0: DeltaToolCall(name=name, json_args=json.dumps(args))}
            async def review(actor, turn, history, result, authorize, **kwargs):
                await authorize()
                self.assertIn('测试甲', str(result['sources']))
                if '测试甲' not in result['answer']:
                    return {**result, 'answer_review': {'status': 'needs_repair', 'issues': ['已有姓名，请使用本轮回执补齐名单。']}}
                return {**result, 'answer_review': {'status': 'passed', 'repairs': 0}}
            reviewer = SimpleNamespace(run=AsyncMock(side_effect=review))
            result, calls, _ = await self.evaluate(channel, 'D楼工单参与人数和名单', script,
                lambda *_: {'ok': True, 'data': {'items': [{'id': 'one', 'name': '测试甲'}], 'total': 1, 'complete': True}}, reviewer=reviewer)
            self.assertIn('测试甲', result['answer'])
            self.assertNotIn('无法提供', result['answer'])
            self.assertEqual(len(calls), 1)
            self.assertEqual(reviewer.run.await_count, 2)


class ReadProgressTests(unittest.TestCase):
    def test_no_receipt_yet_allows_two_discovery_attempts(self):
        budget = ReadRepairBudget()
        self.assertEqual([budget.allow([], {}) for _ in range(4)], [True, True, False, False])

    def test_general_task_can_be_planned_without_any_building_scope(self):
        from openclaw_service.assistant.lighthouse_context import update_task_context
        actor = {'id': 'fixture', 'scopes': [], 'modules': ['assistant']}
        turn = {'question': '解释Python列表', 'scopes': []}
        result = update_task_context(turn, actor, {'deliverables': [
            {'id': 'explain', 'description': '解释概念', 'kind': 'explanation'}]})
        self.assertEqual(result['scopes'], [])
        self.assertEqual(result['contract']['deliverables'][0]['id'], 'explain')

    def test_progress_is_bounded_but_allows_new_pages(self):
        budget = ReadRepairBudget()
        source = {'query_ref': 'q', 'url': '/read', 'available': True}
        for page in range(4):
            self.assertTrue(budget.allow([source], {'q': {'page': page, 'has_more': True}}))
        self.assertFalse(budget.allow([source], {'q': {'page': 4, 'has_more': True}}))

    def test_unchanged_data_new_reference_and_timestamp_do_not_earn_infinite_retries(self):
        budget = ReadRepairBudget()
        for index, expected in enumerate((True, True, False)):
            ref = str(index)
            self.assertEqual(budget.allow([{'url': '/same', 'query_ref': ref}], {
                ref: {'has_more': True, 'items': [], 'queried_at': index}}), expected)

    def test_coverage_replaces_same_query_but_not_other_filters(self):
        sources = [{'query_key': 'one', 'query_ref': 'old', 'available': True},
                   {'query_key': 'two', 'query_ref': 'other', 'available': True},
                   {'query_key': 'one', 'query_ref': 'new', 'available': True}]
        result = query_progress(sources, {'old': {'has_more': True}, 'other': {'has_more': True},
            'new': {'complete': True, 'total': 0, 'items': []}})
        self.assertEqual([row['query_ref'] for row in result['gaps']], ['other'])
        self.assertEqual(result['sources'][0]['state'], 'empty')

    def test_numeric_health_shared_only_for_shared_model_not_private_credentials(self):
        shared = failover.profile('shared', shared=True)
        a = {'id': 'a', '_execution_lane': 'web'}
        b = {'id': 'b', 'channel': 'feishu:group'}
        self.assertEqual(model_health_key(a, shared), model_health_key(b, shared))
        self.assertNotEqual(model_health_key(a, failover.profile('private')), model_health_key(b, failover.profile('private')))
        self.assertNotEqual(model_health_key(a, shared), model_health_key(a, {**shared, 'key_cipher': 'rotated'}))

    def test_text_fallback_can_use_vision_provider_without_enabling_images(self):
        fixture = failover.ModelFailoverTests()
        fixture.setUp()
        try:
            fixture.configure(fixture.actor['id'], [failover.profile('current'), failover.profile('vision', image_input=True)])
            candidate = fixture.agent._next_profile({fixture.agent._profile_key(fixture.agent.profile)})
            self.assertEqual(candidate['id'], 'vision')
            self.assertFalse(candidate['image_input'])
        finally:
            fixture.tearDown()


if __name__ == '__main__':
    unittest.main()

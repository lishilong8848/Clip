"""Structured task intent contracts; synthetic inputs, no model or network calls."""
import copy
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_context import (
    execution_lane, task_context, task_contract_hint, task_followup, task_question, update_task_context,
)


ACTOR = {'id': 'contract-owner', 'scopes': ['C', 'D'], 'modules': ['assistant', 'notices']}


def spec(**changes):
    return {'objects': ['排班人员', '学练参与者'], 'time_range': '今天', 'shift': '夜班',
            'scopes': ['D'], 'counting_basis': '按人员标识去重,分别统计',
            'deliverables': [{'id': 'roster', 'description': '排班名单', 'kind': 'data'},
                             {'id': 'participation', 'description': '参与人数', 'kind': 'data'}],
            'remaining_gaps': ['核对本轮排班来源'], **changes}


def turn(question='D楼今天夜班排班人员和学练参与人数', *, actor=ACTOR, previous=None, scopes=None):
    topic = (actor.get('_feishu_topic') or {}).get('id', '')
    selected = actor['scopes'] if scopes is None else scopes
    return {'question': question, 'feishu_topic': topic, 'execution_lane': execution_lane(actor),
            'scopes': list(selected), 'access_scopes': actor['scopes'], 'access_modules': actor['modules'],
            'sources': [], '_task_context': task_context(question, previous, actor, topic_id=topic, scopes=selected)}


class TaskContractTests(unittest.TestCase):
    def test_update_preserves_legacy_context_and_returns_detached_context(self):
        current = turn()
        before = copy.deepcopy(current['_task_context'])
        actor = copy.deepcopy(ACTOR)
        requested = spec()
        result = update_task_context(current, actor, requested)
        self.assertEqual(result['contract'], requested)
        self.assertEqual({key: result[key] for key in before}, before)
        self.assertEqual(actor, ACTOR)
        requested['objects'].append('caller mutation')
        result['contract']['deliverables'][0]['description'] = 'return mutation'
        self.assertEqual(current['_task_context']['contract'], spec())

    def test_shift_correction_is_pending_until_replanned_and_uses_new_shift(self):
        original = turn()
        update_task_context(original, ACTOR, spec())
        before = copy.deepcopy(original)
        followup = turn('改成白班', previous=original)
        context = followup['_task_context']
        self.assertTrue(context['continued'])
        self.assertNotIn('contract', context)
        self.assertEqual(context['previous_contract'], spec())
        self.assertIn('尚未按本轮问题核对', task_contract_hint(followup, ACTOR))
        result = update_task_context(followup, ACTOR, spec(shift='白班', remaining_gaps=[]))
        self.assertNotIn('previous_contract', result)
        self.assertEqual(result['contract']['shift'], '白班')
        self.assertNotIn('夜班', task_contract_hint(followup, ACTOR))
        again = turn('分别是哪些？', previous=followup)
        self.assertEqual(again['_task_context']['previous_contract']['shift'], '白班')
        self.assertEqual(original, before)

    def test_multiple_asks_keep_ids_and_can_extend_objects_without_keyword_routing(self):
        original = turn()
        update_task_context(original, ACTOR, spec())
        followup = turn('也包括维修项目', previous=original)
        self.assertEqual(followup['_task_context']['previous_contract']['deliverables'], spec()['deliverables'])
        expanded = spec(objects=spec()['objects'] + ['维修项目'], deliverables=spec()['deliverables'] + [
            {'id': 'repairs', 'description': '维修清单', 'kind': 'data'}])
        update_task_context(followup, ACTOR, expanded)
        last = turn('再详细一点', previous=followup)
        self.assertEqual([goal['id'] for goal in last['_task_context']['previous_contract']['deliverables']],
                         ['roster', 'participation', 'repairs'])

    def test_semantic_objects_do_not_require_a_known_business_domain(self):
        original = turn('Compare two sorting strategies')
        requested = spec(objects=['sorting strategies'], time_range='', shift='', scopes=[],
                         counting_basis='', deliverables=[{'id': 'comparison', 'description': 'Compare tradeoffs',
                                                          'kind': 'explanation'}], remaining_gaps=[])
        update_task_context(original, ACTOR, requested)
        followup = turn('再详细一点', previous=original)
        self.assertTrue(followup['_task_context']['continued'])
        self.assertEqual(followup['_task_context']['previous_contract'], requested)
        unrelated = turn('今天天气怎么样', previous=original)
        self.assertNotIn('previous_contract', unrelated['_task_context'])

    def test_generic_reference_retry_and_projection_inherit_structured_intent(self):
        original = turn()
        update_task_context(original, ACTOR, spec())
        for question in ('这些人呢', '再查一下', '只要名字', '那几个对象呢', '他们呢'):
            with self.subTest(question=question):
                self.assertTrue(task_followup(question))
                current = turn(question, previous=original)
                self.assertTrue(current['_task_context']['continued'])
                self.assertEqual(current['_task_context']['previous_contract'], spec())
                self.assertIn('排班人员', task_contract_hint(current, ACTOR))
                self.assertNotIn('contract', current['_task_context'])
        for question in ('今天天气怎么样', '解释这些人的工作', '这些人呢，顺便介绍Python'):
            self.assertFalse(task_followup(question))

    def test_complete_replacement_clears_old_conditions_and_deliverables(self):
        current = turn()
        update_task_context(current, ACTOR, spec())
        result = update_task_context(current, ACTOR, {'objects': ['采购清单']})
        self.assertEqual(result['contract'], {'objects': ['采购清单'], 'time_range': '', 'shift': '',
            'scopes': [], 'counting_basis': '', 'deliverables': [], 'remaining_gaps': []})

    def test_user_topic_and_browser_lanes_do_not_share_contracts(self):
        actor = {**ACTOR, '_feishu_topic': {'id': 'topic-one'}, 'channel': 'feishu:fixture'}
        original = turn(actor=actor)
        update_task_context(original, actor, spec())
        for other in ({**actor, 'id': 'other-person'}, {**actor, '_feishu_topic': {'id': 'topic-two'}}, ACTOR):
            with self.subTest(actor=other):
                self.assertEqual(task_contract_hint(original, other), '')
                followup = turn('分别是哪些？', actor=other, previous=original)
                self.assertNotIn('previous_contract', followup['_task_context'])
                self.assert_rejected(original, spec(), actor=other)
        browser = {**ACTOR, '_execution_lane': 'browser-one'}
        original = turn(actor=browser)
        update_task_context(original, browser, spec())
        other = {**browser, '_execution_lane': 'browser-two'}
        self.assertEqual(task_contract_hint(original, other), '')
        self.assertNotIn('previous_contract', turn('分别是哪些？', actor=other, previous=original)['_task_context'])
        self.assert_rejected(original, spec(), actor=other)

    def test_revocations_private_requests_and_display_copies_are_not_hints(self):
        current = turn()
        update_task_context(current, ACTOR, spec())
        for actor in ({**ACTOR, 'scopes': ['C']}, {**ACTOR, 'modules': ['assistant']}):
            self.assertEqual(task_contract_hint(current, actor), '')
            self.assert_rejected(current, spec(), actor=actor)
        for key in ('display_only', '_private_request'):
            blocked = {**current, key: True}
            self.assertEqual(task_contract_hint(blocked, ACTOR), '')
            self.assert_rejected(blocked, spec())

    def test_scopes_are_constraints_and_cannot_expand_selected_scope(self):
        current = turn(scopes=['D'])
        for selected in (['C'], ['E'], ['ALL']):
            self.assert_rejected(current, spec(scopes=selected))
        self.assertEqual(update_task_context(current, ACTOR, spec())['scopes'], ['D'])

    def test_contract_has_no_sources_facts_write_payloads_or_authority(self):
        current = turn()
        current.update(sources=[{'number': 1, 'data': {'private_value': 'old-private-value', 'count': 7}}],
                       plan={'body': {'progress': 'old-form-value'}, 'status': 'completed'})
        for key in ('source_numbers', 'query_refs', 'resolved', 'values', 'body', 'plan', 'is_admin', 'owner'):
            self.assert_rejected(current, {**spec(), key: ['untrusted']})
        update_task_context(current, ACTOR, spec())
        followup = turn('分别是哪些？', previous=current)
        hint = task_contract_hint(followup, ACTOR)
        for value in ('old-private-value', 'old-form-value', 'source_numbers', 'query_refs', 'count'):
            self.assertNotIn('"' + value + '"', hint)
        self.assertIn('不重做历史写入', hint)
        self.assertNotIn('contract', followup['_task_context'])

    def test_privacy_validation_reuses_existing_filters_without_echoing_values(self):
        current = turn()
        for value in ('person@example.test', '13800138000', 'app_secret: synthetic-value', 'sk-synthetic123456'):
            with self.subTest(value=value):
                self.assert_rejected(current, spec(remaining_gaps=[value]))
                self.assert_rejected(current, spec(deliverables=[{'id': 'one', 'description': value, 'kind': 'data'}]))
        update_task_context(current, ACTOR, spec())
        current['_task_context']['contract']['objects'] = ['person@example.test']
        self.assertEqual(task_contract_hint(current, ACTOR), '')

    def test_malformed_goals_limits_and_unknown_fields_fail_atomically(self):
        current = turn()
        update_task_context(current, ACTOR, spec())
        for bad in ([], spec(objects='people'), spec(shift={}), spec(time_range='x' * 501),
                    spec(objects=['x'] * 21), spec(remaining_gaps=['']), spec(deliverables=['roster']),
                    spec(deliverables=spec()['deliverables'] * 2),
                    spec(deliverables=[{'id': str(i), 'description': 'goal', 'kind': 'data'} for i in range(9)]),
                    spec(deliverables=[{'id': 'x' * 101, 'description': 'goal', 'kind': 'data'}]),
                    spec(deliverables=[{'id': 'x', 'description': 'x' * 301, 'kind': 'data'}]),
                    spec(deliverables=[{'id': 'x', 'description': 'x', 'kind': 'data', 'status': 'complete'}]),
                    spec(deliverables=[{'id': 'x', 'description': 'x', 'kind': 'admin'}])):
            self.assert_rejected(current, bad)

    def test_changed_question_cannot_reuse_an_unreconciled_current_contract(self):
        current = turn()
        update_task_context(current, ACTOR, spec())
        current['question'] = '改成白班'
        self.assertEqual(task_contract_hint(current, ACTOR), '')
        update_task_context(current, ACTOR, spec(shift='白班'))
        self.assertIn('白班', task_contract_hint(current, ACTOR))

    def test_legacy_context_and_task_question_are_unchanged_without_contract(self):
        question = '今天进行中的变更有几个?'
        current = turn(question)
        self.assertEqual(current['_task_context'], {
            'version': 1, 'owner': ACTOR['id'], 'topic_id': '', 'scopes': ['C', 'D'],
            'modules': ['assistant', 'notices'], 'query': question, 'domains': ['notices'],
            'continued': False, 'record_refs': [], 'conditions': {'dates': ['今天'], 'states': ['进行中']}})
        followup = turn('分别是哪些？', previous=current)
        self.assertEqual(task_question(followup, ACTOR), question + '\n本次条件:分别是哪些?')
        self.assertEqual(task_contract_hint(followup, ACTOR), '')
        generic = turn('Compare two sorting strategies')
        self.assertFalse(turn('再详细一点', previous=generic)['_task_context']['continued'])

    def test_missing_context_can_be_initialized_and_hints_are_serializable(self):
        current = {'question': '整理人员名单', 'scopes': ['D']}
        result = update_task_context(current, ACTOR, spec())
        self.assertEqual(json.loads(json.dumps(result)), result)
        self.assertIn('本轮模型整理的任务意图', task_contract_hint(current, ACTOR))

    def assert_rejected(self, current, requested, *, actor=ACTOR):
        before = copy.deepcopy(current)
        actor_before = copy.deepcopy(actor)
        with self.assertRaises(AssistantError):
            update_task_context(current, actor, requested)
        self.assertEqual(current, before)
        self.assertEqual(actor, actor_before)


class TaskContractStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_generic_followups_reach_the_engine_with_the_same_task(self):
        from bin import test_lighthouse_stream as fixtures
        fixture = fixtures.StreamTests()
        await fixture.asyncSetUp()
        self.addCleanup(fixture.doCleanups)
        self.addAsyncCleanup(fixture.asyncTearDown)
        answer = fixture.engine.answer

        async def planned(actor, current, *args):
            if current['question'] == 'D楼今天夜班排班人员和学练参与人数':
                update_task_context(current, actor, spec())
            return await answer(actor, current, *args)

        fixture.engine.answer = planned
        await fixture.submit('D楼今天夜班排班人员和学练参与人数')
        await fixture.finish()
        for question in ('这些人呢', '再查一下', '只要名字'):
            await fixture.submit(question)
            await fixture.finish()
            current = fixture.engine.calls[-1]['turn']
            self.assertEqual(current['_task_context']['previous_contract'], spec())
            saved = fixture.assistant._state(fixture.actor)['turns'][-1]
            self.assertEqual(saved['status'], 'completed')
            self.assertEqual(saved['_task_context']['previous_contract'], spec())
            self.assertIn('排班人员', task_contract_hint(current, fixture.actor))


if __name__ == '__main__':
    unittest.main()

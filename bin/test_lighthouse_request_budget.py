"""Offline regressions for complete outbound model request budgets."""
import copy
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_context import (
    _model_payload_size, bound_model_payload, model_context_limit, model_output_limit,
)


class RequestBudgetTests(unittest.TestCase):
    def payload(self, content='current', **fields):
        return {'model': 'WanWu/MiniMax-Auto', 'messages': [
            {'role': 'system', 'content': 'Keep policy and permissions.'},
            {'role': 'user', 'content': content}], **fields}

    def fits(self, payload, profile):
        before = copy.deepcopy(payload)
        result = bound_model_payload(payload, profile)
        self.assertEqual(payload, before)
        outputs = [result[key] for key in ('max_tokens', 'max_completion_tokens') if key in result]
        self.assertEqual(len(outputs), 1)
        self.assertNotIn('max_completion_tokens', result)
        self.assertLessEqual(outputs[0], model_output_limit(profile))
        size = _model_payload_size(result)
        self.assertLessEqual(size + (size + 9) // 10 + 1024 + outputs[0], model_context_limit(profile))
        return result

    def rejects(self, payload, profile):
        before = copy.deepcopy(payload)
        with self.assertRaises(AssistantError) as caught:
            bound_model_payload(payload, profile)
        self.assertEqual((caught.exception.status, caught.exception.category), (413, 'model_context'))
        self.assertIn('attachments', str(caught.exception))
        self.assertIn('paginate', str(caught.exception))
        self.assertEqual(payload, before)
        return str(caught.exception)

    def test_effective_limits_for_native_sdk_routing_and_stale_config(self):
        self.assertEqual(model_context_limit({}), 64000)
        self.assertEqual(model_context_limit({'model': 'other', 'context_window': 128000}), 128000)
        for name in ('MiniMax', 'GLM', 'Qwen', 'Deepseek'):
            for configured, expected in ((None, 149952), (149952, 149952), (200000, 149952), ('149952', 149952), (32000, 32000)):
                with self.subTest(name=name, configured=configured):
                    profile = {'model': 'WanWu/' + name + '-Auto'}
                    if configured is not None:
                        profile['context_window'] = configured
                    self.assertEqual(model_context_limit(profile), expected)
                    self.fits(self.payload(), profile)

    def test_provider_byte_budget_does_not_misapply_the_earlier_sdk_threshold(self):
        for name in ('MiniMax', 'GLM', 'Qwen', 'Deepseek'):
            with self.subTest(name=name):
                profile = {'model': 'WanWu/' + name + '-Auto'}
                payload = self.payload('x' * 64000, model=profile['model'])
                self.fits(payload, profile)
                self.assertIn('context limit 64000', self.rejects(payload, {**profile, 'context_window': 64000}))
                self.fits(payload, {**profile, 'context_window': 149952})

    def test_whitespace_cannot_bypass_known_profile_or_payload_ceiling(self):
        for name in ('MiniMax', 'GLM', 'Qwen', 'Deepseek'):
            with self.subTest(name=name):
                model = ' \tWanWu/' + name + '-Auto\r\n'
                profile = {'model': model, 'context_window': 200000}
                self.assertEqual(model_context_limit(profile), 149952)
                self.assertEqual(model_context_limit({**profile, 'context_window': 32000}), 32000)
                payload = self.payload('x' * 150042, model=model, max_completion_tokens=65536)
                for selected in (profile, {'model': 'transport-alias', 'context_window': 200000}):
                    detail = self.rejects(payload, selected)
                    self.assertIn('context limit 149952', detail)

    def test_reported_input150042_output65536_context149952_rejects_locally(self):
        for name in ('MiniMax', 'GLM', 'Qwen', 'Deepseek'):
            profile = {'model': 'WanWu/' + name + '-Auto', 'context_window': 200000}
            for field in ('max_tokens', 'max_completion_tokens'):
                with self.subTest(name=name, field=field):
                    detail = self.rejects(self.payload('x' * 150042, **{field: 65536}), profile)
                    self.assertIn('output budget 32000', detail)
                    self.assertIn('context limit 149952', detail)

    def test_output_clamp_and_duplicate_fields(self):
        for fields, expected in (({}, 5000), ({'max_tokens': 65536}, 5000),
                                 ({'max_completion_tokens': 65536}, 5000),
                                 ({'max_tokens': '65536'}, 5000),
                                 ({'max_tokens': 65536, 'max_completion_tokens': 65536}, 5000),
                                 ({'max_tokens': 900, 'max_completion_tokens': 65536}, 900),
                                 ({'max_tokens': 65536, 'max_completion_tokens': 900}, 900)):
            result = self.fits(self.payload(**fields), {})
            self.assertEqual(result['max_tokens'], expected)

    def test_auto_models_reserve_completion_space_for_reasoning_before_visible_answer(self):
        for name in ('Deepseek', 'MiniMax', 'GLM', 'Qwen'):
            profile = {'model': 'WanWu/' + name + '-Auto'}
            request = self.payload(model=profile['model'], max_tokens=32000)
            result = bound_model_payload(request, profile)
            self.assertEqual(result['max_tokens'], 32000)
            self.assertEqual(result['messages'], request['messages'])

    def test_reasoning_budget_shrinks_to_remaining_context_without_truncating_current_request(self):
        profile = {'model': 'WanWu/Deepseek-Auto'}
        request = self.payload('x' * 125000, model=profile['model'], max_tokens=32000)
        result = bound_model_payload(request, profile)
        self.assertGreater(result['max_tokens'], 5000)
        self.assertLess(result['max_tokens'], 32000)
        self.assertEqual(result['messages'], request['messages'])
        size = _model_payload_size(result)
        self.assertLessEqual(size + (size + 9) // 10 + 1024 + result['max_tokens'], 149952)

    def test_explicit_small_completion_budget_is_not_silently_raised(self):
        result = bound_model_payload(self.payload(max_tokens=900), {'model': 'WanWu/MiniMax-Auto'})
        self.assertEqual(result['max_tokens'], 900)

    def test_provider_default_omits_injected_output_limits_but_keeps_current_payload(self):
        request = self.payload(max_tokens=8192, max_completion_tokens=65536, stream=True,
                               tools=[{'type': 'function', 'function': {'name': 'read'}}])
        before = copy.deepcopy(request)
        result = bound_model_payload(request, {'model': 'WanWu/MiniMax-Auto'}, provider_default=True)
        self.assertEqual(result, {key: value for key, value in request.items()
                                  if key not in {'max_tokens', 'max_completion_tokens'}})
        self.assertEqual(request, before)

    def test_provider_default_still_rejects_oversized_input(self):
        with self.assertRaises(AssistantError) as caught:
            bound_model_payload(self.payload('x' * 150042), {'model': 'WanWu/Qwen-Auto'}, provider_default=True)
        self.assertEqual(caught.exception.category, 'model_context')

    def test_large_history_evidence_is_bounded_with_coverage_and_original_preserved(self):
        from openclaw_service.assistant.lighthouse_model import evidence_summary
        source = {'number': 1, 'title': '历史来源', 'query_ref': 'original-query',
                  'query_conditions': {'scope': 'D', 'date': '2026-10-25'},
                  'data': {'records': [{'record_id': str(i), 'title': '历史条目' * 2000} for i in range(40)]}}
        original = copy.deepcopy(source)
        summary = evidence_summary([source])
        self.assertLessEqual(len(json.dumps(summary, ensure_ascii=False).encode()), 2010)
        self.assertEqual(summary[0]['conditions'], source['query_conditions'])
        self.assertEqual(summary[0]['query_ref'], 'original-query')
        self.assertTrue(summary[0]['historical_only'])
        self.assertTrue(summary[0]['sample_limited'])
        self.assertEqual(source, original)

    def test_oversized_history_conditions_are_marked_missing_not_replaced_with_empty_filters(self):
        from openclaw_service.assistant.lighthouse_model import evidence_summary
        source = {'query_ref': 'original-query', 'query_conditions': {'filter': 'very-long-condition' * 4000}, 'data': {}}
        original = copy.deepcopy(source)
        summary = evidence_summary([source])[0]
        self.assertTrue(summary['conditions_omitted'])
        self.assertTrue(summary['sample_limited'])
        self.assertNotIn('conditions', summary)
        self.assertEqual(summary['query_ref'], 'original-query')
        self.assertEqual(source, original)

    def test_current_turn_old_read_receipts_use_snapshot_refs_without_losing_current_request(self):
        payload = self.payload('CURRENT_QUESTION 仅A楼今天夜班')
        for i in range(8):
            payload['messages'] += [
                {'role': 'assistant', 'tool_calls': [{'id': str(i), 'type': 'function',
                  'function': {'name': 'lighthouse_query', 'arguments': '{"scope":"A"}'}}]},
                {'role': 'tool', 'tool_call_id': str(i), 'content': json.dumps({
                    'query_ref': 'snapshot-' + str(i), 'complete': True,
                    'data': {'rows': [{'value': '详情' * 4000}]}})}]
        original = copy.deepcopy(payload)
        result = bound_model_payload(payload, {'model': 'WanWu/Deepseek-Auto'}, provider_default=True)
        self.assertEqual(result['messages'][:2], original['messages'][:2])
        self.assertEqual(result['messages'][-2:], original['messages'][-2:])
        for i, message in enumerate(result['messages']):
            if message.get('role') == 'assistant':
                self.assertEqual(message, original['messages'][i])
            elif message.get('role') == 'tool' and message['tool_call_id'] != '7':
                receipt = json.loads(message['content'])
                self.assertEqual(receipt['query_ref'], 'snapshot-' + message['tool_call_id'])
                self.assertTrue(receipt['receipt_excerpt_only'])
        self.assertLess(_model_payload_size(result), _model_payload_size(original) // 2)
        self.assertEqual(payload, original)

    def test_tool_compaction_never_changes_write_unknown_or_unreferenced_receipts(self):
        from openclaw_service.assistant.lighthouse_context import compact_read_receipts
        payload = self.payload()
        for i, name in enumerate(('lighthouse_prepare_business', 'custom_tool', 'lighthouse_query')):
            result = {'query_ref': 'snapshot', 'data': 'x' * 10000} if i < 2 else {'data': 'x' * 10000}
            payload['messages'] += [
                {'role': 'assistant', 'tool_calls': [{'id': str(i), 'type': 'function',
                  'function': {'name': name, 'arguments': '{}'}}]},
                {'role': 'tool', 'tool_call_id': str(i), 'content': json.dumps(result)}]
        payload['messages'].append({'role': 'tool', 'tool_call_id': 'latest', 'content': 'unchanged'})
        original = copy.deepcopy(payload)
        self.assertEqual(compact_read_receipts(payload), original)
        self.assertEqual(payload, original)

    def test_malformed_tool_identifiers_are_not_rewritten_or_interpreted(self):
        from openclaw_service.assistant.lighthouse_context import compact_read_receipts
        payload = self.payload()
        payload['messages'] += [
            {'role': 'assistant', 'tool_calls': [{'id': ['invalid'], 'function': {'name': 'lighthouse_query'}},
                                                {'id': 'invalid-name', 'function': {'name': ['lighthouse_query']}}]},
            {'role': 'tool', 'tool_call_id': ['invalid'], 'content': 'unchanged'},
            {'role': 'tool', 'tool_call_id': 'invalid-name', 'content': 'unchanged'},
            {'role': 'tool', 'tool_call_id': 'latest', 'content': 'latest'}]
        original = copy.deepcopy(payload)
        self.assertEqual(compact_read_receipts(payload), original)

    def test_default_and_lower_context_and_combined_output_budget(self):
        self.rejects(self.payload('x' * 64000), {})
        self.rejects(self.payload('x' * 30000), {'context_window': 32000})
        self.fits(self.payload('x' * 51000), {})
        self.rejects(self.payload('x' * 54000), {})
        self.fits(self.payload('x' * 54000, max_tokens=100), {})
        self.rejects(self.payload('数' * 22000), {})

    def test_whole_old_rounds_only_with_exact_current_tool_receipt(self):
        payload = self.payload(tools=[{'type': 'function', 'function': {'name': 'read', 'parameters': {'type': 'object'}}}])
        current = [payload['messages'][-1],
                   {'role': 'assistant', 'tool_calls': [{'id': 'current-call', 'type': 'function',
                    'function': {'name': 'read', 'arguments': '{"page":2,"task":"do not replay"}'}}]},
                   {'role': 'tool', 'tool_call_id': 'current-call', 'content': '{"committed":true,"next":3}'}]
        old = []
        for i in range(12):
            old += [{'role': 'user', 'content': 'old round ' + str(i)},
                    {'role': 'assistant', 'tool_calls': [{'id': str(i), 'type': 'function',
                     'function': {'name': 'read', 'arguments': '{"page":1}'}}]},
                    {'role': 'tool', 'tool_call_id': str(i), 'content': 'x' * 8192},
                    {'role': 'assistant', 'content': 'done'}]
        payload['messages'] = payload['messages'][:1] + old + current
        result = self.fits(payload, {})
        self.assertEqual(result['messages'][0], payload['messages'][0])
        self.assertEqual(result['messages'][-len(current):], current)
        self.assertEqual(result['tools'], payload['tools'])
        self.assertLess(len(result['messages']), len(payload['messages']))
        self.assertIn('并非完整记录', result['messages'][1]['content'])
        retained = result['messages'][2:-len(current)]
        self.assertEqual(len(retained) % 4, 0)
        for i in range(0, len(retained), 4):
            self.assertIn(retained[i:i + 4], [old[j:j + 4] for j in range(0, len(old), 4)])
        calls, receipts = [], []
        for message in result['messages']:
            calls.extend(call['id'] for call in message.get('tool_calls', []))
            if message.get('role') == 'tool':
                self.assertIn(message['tool_call_id'], calls)
                receipts.append(message['tool_call_id'])
        self.assertEqual(calls, receipts)

    def test_schema_system_and_current_tool_arguments_and_result_are_counted(self):
        for field in ('tools', 'system', 'arguments', 'result'):
            with self.subTest(field=field):
                payload = self.payload()
                if field == 'tools':
                    payload['tools'] = [{'type': 'function', 'function': {'name': 'read', 'description': 'x' * 150042}}]
                elif field == 'system':
                    payload['messages'][0]['content'] = 'x' * 150042
                else:
                    payload['messages'] += [
                        {'role': 'assistant', 'tool_calls': [{'id': 'keep', 'type': 'function',
                         'function': {'name': 'read', 'arguments': 'x' * 150042 if field == 'arguments' else '{}'}}]},
                        {'role': 'tool', 'tool_call_id': 'keep', 'content': 'x' * 150042 if field == 'result' else 'ok'}]
                self.rejects(payload, {'model': payload['model'], 'context_window': 149952})

    def test_multibyte_old_rounds_fit_near_the_combined_budget(self):
        old = [{'role': role, 'content': '数' * 2000}
               for _ in range(12) for role in ('user', 'assistant')]
        for length in (37000, 38000):
            payload = self.payload('x' * length)
            payload['messages'][1:1] = old
            result = self.fits(payload, {})
            self.assertEqual(result['messages'][-1], payload['messages'][-1])
            self.assertIn('部分历史对话已省略', result['messages'][1]['content'])

    def test_sdk_summary_bypass_still_compacts_or_rejects(self):
        payload = self.payload('x' * 150042)
        profile = {'model': payload['model'], 'context_window': 149952}
        payload['messages'][0]['content'] = 'You are a context summarization assistant.'
        self.rejects(payload, profile)
        payload['messages'] = payload['messages'][:1] + [
            {'role': 'user', 'content': 'x' * 150042},
            {'role': 'assistant', 'content': 'old'}, {'role': 'user', 'content': 'summarize'}]
        result = self.fits(payload, profile)
        self.assertEqual(result['messages'][0], payload['messages'][0])
        self.assertEqual(result['messages'][-1], payload['messages'][-1])

    def test_base64_is_excluded_only_in_images_and_each_image_has_a_reserve(self):
        image = {'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,' + 'A' * 1000000, 'detail': 'high'}}
        payload = self.payload([{'type': 'text', 'text': 'inspect'}, image])
        result = self.fits(payload, {})
        small = self.payload([{'type': 'text', 'text': 'inspect'}, {**image, 'image_url': {'url': 'data:image/png;base64,AAAA', 'detail': 'high'}}])
        self.assertEqual(_model_payload_size(result), _model_payload_size(self.fits(small, {})))
        self.assertGreater(_model_payload_size(result), 16384)
        payload['messages'][-1]['content'] = [image] * 4
        self.rejects(payload, {})
        self.rejects(self.payload(image['image_url']['url']), {})

    def test_malformed_limits_and_nonfinite_json_are_rejected(self):
        for value in (True, False, 0, -1, 1.5, float('nan'), float('inf'), 'NaN', '65536oops', '', None, []):
            for field in ('max_tokens', 'max_completion_tokens', 'context_window'):
                with self.subTest(field=field, value=value):
                    payload, profile = self.payload(), {}
                    (profile if field == 'context_window' else payload)[field] = value
                    self.rejects(payload, profile)
        payload = self.payload()
        payload['temperature'] = float('nan')
        self.rejects(payload, {})


if __name__ == '__main__':
    unittest.main()

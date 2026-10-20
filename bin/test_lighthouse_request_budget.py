"""Offline regressions for complete outbound model request budgets."""
import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_context import (
    _model_payload_size, bound_model_payload, model_context_limit,
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
        self.assertLessEqual(outputs[0], 5000)
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
                    self.assertIn('output budget 5000', detail)
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

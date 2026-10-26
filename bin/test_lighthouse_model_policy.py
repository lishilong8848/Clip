"""Provider constraints and bounded direct requests, without network or saved data."""
import copy
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_ai import (
    AssistantError, CustomModel, ENDPOINT, LighthouseAssistant, NAMESPACE, model_capabilities,
)
from test_lighthouse_account_models import MemoryStore, protect, unprotect


TEXT_ONLY = ('WanWu/GLM-Auto', 'WanWu/Qwen-Auto', 'WanWu/Deepseek-Auto')
CHAT_MODELS = ('WanWu/MiniMax-Auto', *TEXT_ONLY)


class ModelPolicyTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.client = Mock()
        self.client.request_json.return_value = {'choices': [{'message': {'content': 'ok'}}]}
        self.model = CustomModel(self.store, client=self.client, protect=protect, unprotect=unprotect)
        self.fields = {'id': 'fixture', 'name': 'Fixture', 'model': 'fixture',
                       'endpoint': ENDPOINT, 'api_key': 'fixture-key'}
        self.model.configure({'action': 'upsert', 'profile': self.fields})

    def test_direct_answer_can_use_provider_default_without_changing_saved_profile(self):
        before = copy.deepcopy(self.store.docs)
        self.assertEqual(self.model.complete([{'role': 'user', 'content': 'fixture'}], max_tokens=None), 'ok')
        payload = self.client.request_json.call_args.kwargs['json_payload']
        self.assertNotIn('max_tokens', payload)
        self.assertNotIn('max_completion_tokens', payload)
        self.assertEqual(self.store.docs, before)

    def test_truncated_completion_is_not_returned_as_valid_json_or_text(self):
        for finish in ('length', 'max_tokens', 'max_output_tokens'):
            self.client.request_json.return_value = {'choices': [{'finish_reason': finish,
                'message': {'content': '{"answer":"partial'}}]}
            with self.subTest(finish=finish):
                with self.assertRaises(AssistantError) as caught:
                    self.model.complete([{'role': 'user', 'content': 'fixture'}], structured=True)
                self.assertEqual(caught.exception.category, 'model_output')

    def test_oversized_structured_reply_is_not_silently_sliced(self):
        self.client.request_json.return_value = {'choices': [{'finish_reason': 'stop',
            'message': {'content': '{"answer":"' + 'x' * 25000 + '"}'}}]}
        with self.assertRaises(AssistantError) as caught:
            self.model.complete([{'role': 'user', 'content': 'fixture'}], structured=True)
        self.assertEqual(caught.exception.category, 'model_output')

    def test_missing_or_empty_choices_remain_retryable_protocol_failures(self):
        for payload in ({'choices': []}, {'choices': [None]}, {'choices': [{'message': {'content': ''}}]},
                        {'choices': [{'message': {'content': None, 'reasoning_content': 'not an answer'}}]}):
            self.client.request_json.return_value = payload
            with self.subTest(payload=payload):
                with self.assertRaises(AssistantError) as caught:
                    self.model.complete([{'role': 'user', 'content': 'fixture'}])
                self.assertEqual(caught.exception.category, 'model_protocol')

    def test_exact_text_only_models_override_stale_opt_ins_without_mutation(self):
        for name in TEXT_ONLY:
            for value in (name, '  ' + name.swapcase() + '  '):
                with self.subTest(model=value):
                    profile = {'model': value, 'image_input': True, 'reasoning': True,
                               'reasoning_efforts': ['high'], 'reasoning_effort': 'high', 'tool_calls': False}
                    before = copy.deepcopy(profile)
                    capabilities = model_capabilities(profile)
                    self.assertFalse(capabilities['image_input'])
                    self.assertTrue(capabilities['reasoning'])
                    self.assertFalse(capabilities['tool_calls'])
                    self.assertEqual(capabilities['reasoning_effort'], 'high')
                    self.assertEqual(profile, before)

    def test_minimax_and_other_models_keep_explicit_opt_ins_and_default_off(self):
        for name in ('WanWu/MiniMax-Auto', 'future-vision', 'WanWu/GLM-Auto-Vision',
                     'GLM-Auto', 'Other/WanWu/Qwen-Auto', 'WanWu/Deepseek-Auto-v2'):
            with self.subTest(model=name):
                self.assertFalse(model_capabilities({'model': name})['image_input'])
                self.assertTrue(model_capabilities({'model': name, 'image_input': True})['image_input'])

    def test_configure_enforces_text_only_without_changing_other_capabilities(self):
        for index, name in enumerate(TEXT_ONLY):
            with self.subTest(model=name):
                fields = {**self.fields, 'id': 'text_' + str(index), 'name': name, 'model': ' ' + name + ' ',
                          'image_input': True, 'tool_calls': False, 'reasoning': True}
                settings = self.model.configure({'action': 'upsert', 'profile': fields})
                public = next(row for row in settings['models'] if row['id'] == fields['id'])
                self.assertFalse(public['image_input'])
                self.assertFalse(public['tool_calls'])
                self.assertTrue(public['reasoning'])
                profile = self.model.profile(fields['id'])
                self.assertEqual(profile['model'], name)
                self.assertFalse(profile['image_input'])
                saved = self.store.get_document(NAMESPACE, 'model')
                self.assertFalse(next(row for row in saved['models'] if row['id'] == fields['id'])['image_input'])

    def test_stale_shared_and_personal_profiles_and_public_metadata_obey_policy(self):
        names = (*CHAT_MODELS, 'future-vision')
        profiles = [{**self.model.profile(), 'id': 'model_' + str(index), 'name': name,
                     'model': name, 'image_input': True} for index, name in enumerate(names)]
        saved = {'enabled': True, 'active_model_id': 'model_0', 'image_input_default_version': 1,
                 'models': profiles}
        self.store.put_document(NAMESPACE, 'model', saved)
        for actor in ('first', 'second'):
            self.store.put_document(NAMESPACE, 'model:' + actor, {**saved, 'shared_models_linked': True,
                'models': [{**profile, 'id': 'private_' + profile['id']} for profile in profiles]})
        before = copy.deepcopy(self.store.docs)
        service = LighthouseAssistant(self.store, lambda *_: [], model=self.model)
        for actor in ({'id': 'first'}, {'id': 'second'}, {'id': 'first', 'channel': 'feishu:group'}):
            settings = service.model_settings(actor)
            for public in settings['models']:
                expected = public['model'] not in TEXT_ONLY
                with self.subTest(actor=actor, model=public['id']):
                    self.assertEqual(public['image_input'], expected)
                    self.assertEqual(service.model_for(actor).profile(public['id'])['image_input'], expected)
                    self.assertNotIn('key_cipher', public)
                    self.assertNotIn('api_key', public)
        self.assertEqual(self.store.docs, before)

    def test_embeddings_endpoints_rejected_even_for_custom_protocol_without_saving(self):
        before = copy.deepcopy(self.store.docs)
        for name in CHAT_MODELS:
            for custom in (False, True):
                for path in ('/v1/embeddings', '/v1/embeddings/', '/v1/EMBEDDINGS', '/v1/%65mbeddings'):
                    with self.subTest(model=name, custom=custom, path=path), self.assertRaises(AssistantError):
                        self.model.configure({'action': 'upsert', 'profile': {**self.fields, 'model': name,
                            'endpoint': 'https://provider.example' + path, 'custom_protocol': custom}})
                    self.assertEqual(self.store.docs, before)
        self.client.request_json.assert_not_called()

    def test_stale_embeddings_profile_and_direct_complete_rejected_before_request(self):
        for custom in (False, True):
            saved = self.store.get_document(NAMESPACE, 'model')
            saved['models'][0].update(endpoint='https://provider.example/v1/embeddings', custom_protocol=custom)
            self.store.put_document(NAMESPACE, 'model', saved)
            before = copy.deepcopy(self.store.docs)
            with self.subTest(custom=custom):
                with self.assertRaises(AssistantError):
                    self.model.profile()
                with self.assertRaises(AssistantError):
                    self.model.complete([{'role': 'user', 'content': 'hello'}])
                with self.assertRaises(AssistantError):
                    self.model.complete([], profile=saved['models'][0])
                self.client.request_json.assert_not_called()
                self.assertEqual(self.store.docs, before)

    def test_edit_preserves_only_valid_existing_context_window(self):
        for value in (32000, True, 0, -1, '32000', None):
            saved = self.store.get_document(NAMESPACE, 'model')
            saved['models'][0]['context_window'] = value
            self.store.put_document(NAMESPACE, 'model', saved)
            self.model.configure({'action': 'upsert', 'profile': {**self.fields, 'api_key': '', 'name': 'Renamed'}})
            profile = self.model.profile()
            with self.subTest(context_window=value):
                if type(value) is int and value > 0:
                    self.assertEqual(profile['context_window'], value)
                else:
                    self.assertNotIn('context_window', profile)
                self.assertEqual(profile['key_cipher'], protect('fixture-key'))

    def test_direct_complete_sends_bounded_payload_with_selected_profile(self):
        selected = {**self.model.profile(), 'model': 'selected-model', 'context_window': 32000,
                    'reasoning': True, 'reasoning_efforts': ['high'], 'reasoning_effort': 'high'}
        messages = [{'role': 'user', 'content': 'previous'}, {'role': 'assistant', 'content': 'old answer'},
                    {'role': 'user', 'content': 'current'}]
        original = copy.deepcopy(messages)
        before = copy.deepcopy(self.store.docs)
        bounded = {'model': selected['model'], 'messages': [messages[-1]], 'stream': False,
                   'max_tokens': 700, 'reasoning_effort': 'high'}
        with patch('openclaw_service.assistant.lighthouse_context.bound_model_payload',
                   return_value=bounded) as bound:
            self.assertEqual(self.model.complete(messages, profile=selected, max_tokens=9999, timeout=7), 'ok')
        bound.assert_called_once_with({'model': selected['model'], 'messages': original, 'stream': False,
                                      'max_tokens': 4000, 'reasoning_effort': 'high'}, selected)
        self.client.request_json.assert_called_once_with('POST', ENDPOINT,
            headers={'Authorization': 'Bearer fixture-key'}, json_payload=bounded, retries=0, timeout=7)
        self.assertEqual(messages, original)
        self.assertEqual(self.store.docs, before)

    def test_irreducible_direct_input_propagates_context_error_without_upstream_call(self):
        before = copy.deepcopy(self.store.docs)
        error = AssistantError('context cannot fit', category='model_context')
        with patch('openclaw_service.assistant.lighthouse_context.bound_model_payload',
                   side_effect=error) as bound:
            with self.assertRaises(AssistantError) as raised:
                self.model.complete([{'role': 'user', 'content': 'x' * 300000}])
        bound.assert_called_once()
        self.assertIs(raised.exception, error)
        self.assertEqual(raised.exception.category, 'model_context')
        self.client.request_json.assert_not_called()
        self.assertEqual(self.store.docs, before)

    def test_real_context_budget_compacts_history_without_changing_current_input(self):
        selected = {**self.model.profile(), 'context_window': 8000}
        messages = [{'role': 'system', 'content': 'Fixture policy'},
                    {'role': 'user', 'content': 'previous ' * 2000},
                    {'role': 'assistant', 'content': 'previous answer'},
                    {'role': 'user', 'content': 'current question'}]
        original = copy.deepcopy(messages)
        before = copy.deepcopy(self.store.docs)
        self.assertEqual(self.model.complete(messages, profile=selected, max_tokens=500), 'ok')
        payload = self.client.request_json.call_args.kwargs['json_payload']
        self.assertEqual(payload['messages'][0], original[0])
        self.assertEqual(payload['messages'][-1], original[-1])
        self.assertNotIn(original[1], payload['messages'])
        self.assertLess(len(str(payload)), selected['context_window'] - payload['max_tokens'])
        self.assertEqual(messages, original)
        self.assertEqual(self.store.docs, before)

    def test_real_irreducible_input_never_calls_upstream(self):
        selected = {**self.model.profile(), 'context_window': 8000}
        messages = [{'role': 'system', 'content': 'fixed ' * 10000},
                    {'role': 'user', 'content': 'current question'}]
        original = copy.deepcopy(messages)
        before = copy.deepcopy(self.store.docs)
        with self.assertRaises(AssistantError) as raised:
            self.model.complete(messages, profile=selected)
        self.assertEqual(raised.exception.status, 413)
        self.assertEqual(raised.exception.category, 'model_context')
        self.client.request_json.assert_not_called()
        self.assertEqual(messages, original)
        self.assertEqual(self.store.docs, before)


if __name__ == '__main__':
    unittest.main()

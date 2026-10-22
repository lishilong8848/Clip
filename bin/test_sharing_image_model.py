"""Offline image-model selection, evidence and IPC contracts; no real model calls."""
import base64
import copy
import hashlib
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from PIL import Image
from openclaw_service.assistant.lighthouse_ai import AssistantError, CustomModel, NAMESPACE
from openclaw_service.assistant import lighthouse_sharing as sharing


SOURCE = '检修工作必须经过审批，操作前核对运行参数，并记录核验结果。' * 12
OCR = '图中标注供水入口与回水出口，箭头表示水流方向。'
VISUAL = '两个矩形之间有一条向右的箭头。'


def image_source(color='red', ocr=OCR, fmt='JPEG'):
    with Image.new('RGB', (12, 8), color) as image:
        stream = io.BytesIO()
        image.save(stream, format=fmt)
    content = stream.getvalue()
    return {'id': hashlib.sha256(content).hexdigest(), 'data_url': 'data:image/jpeg;base64,' +
            base64.b64encode(content).decode('ascii'), 'ocr_text': ocr}


def profile(identity, model_name, **fields):
    return {'id': identity, 'name': identity, 'model': model_name, 'image_input': True,
            'endpoint': 'https://model.example/v1/chat/completions', 'key_cipher': 'fixture', **fields}


def response(value):
    return {'choices': [{'message': {'content': json.dumps(value, ensure_ascii=False)}}]}


def question(**fields):
    return {'type': 'single', 'stem': '图中箭头的含义是什么？', 'options': ['水流方向', '设备型号'],
            'correct': [0], 'evidence': OCR, 'explanation': '依据图片文字。', **fields}


class SharingImageModelTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('Network forbidden')))
        self.image = image_source()
        self.docs = {
            'model': {'enabled': True, 'active_model_id': 'text', 'image_input_default_version': 1,
                      'models': [profile('text', 'WanWu/GLM-Auto')]},
            'model:owner': {'enabled': True, 'active_model_id': 'shared_text',
                            'image_input_default_version': 1, 'shared_models_linked': True,
                            'models': [profile('vision', 'WanWu/MiniMax-Auto')]},
            'model:other': {'enabled': True, 'active_model_id': 'other-vision',
                            'image_input_default_version': 1, 'shared_models_linked': True,
                            'models': [profile('other-vision', 'MiniMax-VL-01')]},
        }
        self.store = Mock()
        self.store.get_document.side_effect = lambda ns, key: copy.deepcopy(self.docs.get(key)) if ns == NAMESPACE else None
        self.store.put_document.side_effect = AssertionError('Configuration writes forbidden')
        self.transport = SimpleNamespace(request_json=Mock(return_value=response({'text': VISUAL})))
        self.base = CustomModel(self.store, client=self.transport, unprotect=lambda _: 'fixture-key')
        self.model = self.base.for_actor('owner')

    def recognize(self, images=None):
        return sharing.generate(self.model, {'mode': 'image_sources', 'images': [self.image] if images is None else images})['images']

    def generate(self, questions, **fields):
        self.transport.request_json.return_value = response({'questions': questions})
        return sharing.generate(self.model, {'source': SOURCE, 'count': len(questions), 'previous': [], **fields})['questions']

    def sent(self):
        return self.transport.request_json.call_args.kwargs['json_payload']

    def test_selects_own_minimax_without_changing_active_text_model(self):
        before = copy.deepcopy(self.docs)
        result = self.recognize()[0]
        self.assertEqual((result['id'], result['mode']), (self.image['id'], 'vision'))
        self.assertIn(OCR, result['text'])
        self.assertIn(VISUAL, result['text'])
        self.assertIn('待发布者核对', result['text'])
        self.assertIn('并非已核实事实', result['text'])
        self.assertLessEqual(len(result['text']), 2000)
        self.assertEqual(self.sent()['model'], 'WanWu/MiniMax-Auto')
        self.assertEqual(self.sent()['messages'][1]['content'][1],
                         {'type': 'image_url', 'image_url': {'url': self.image['data_url']}})
        self.assertEqual(self.docs, before)
        self.store.put_document.assert_not_called()

    def test_selects_current_eligible_model_first_and_accepts_shared_minimax(self):
        self.docs['model:owner']['models'].append(profile('active', 'MiniMax-VL-01'))
        self.docs['model:owner']['active_model_id'] = 'active'
        self.recognize()
        self.assertEqual(self.sent()['model'], 'MiniMax-VL-01')
        self.docs['model:owner']['models'] = []
        self.docs['model']['models'].append(profile('shared-vision', 'WanWu/MiniMax-Auto'))
        self.recognize()
        self.assertEqual(self.sent()['model'], 'WanWu/MiniMax-Auto')

    def test_other_accounts_minimax_is_never_selected(self):
        self.docs['model:owner']['models'] = []
        self.assertEqual(self.recognize()[0], {'id': self.image['id'], 'text': OCR, 'mode': 'ocr'})
        self.assertNotIn('model:other', [call.args[1] for call in self.store.get_document.call_args_list])
        self.transport.request_json.assert_not_called()

    def test_non_minimax_disabled_and_unconfigured_models_never_receive_images(self):
        for fields in ({'image_input': False}, {'key_cipher': ''},
                       {'model': 'WanWu/GLM-Auto', 'name': 'MiniMax'}, {'model': 'WanWu/Qwen-Auto'},
                       {'model': 'WanWu/Deepseek-Auto'}, {'model': 'OtherVision'},
                       {'model': 'not-MiniMax-Auto'}, {'model': 'MiniMax-Fake/Other'}):
            with self.subTest(fields=fields):
                self.docs['model:owner']['models'] = [profile('vision', 'MiniMax-VL-01', **fields)]
                self.assertEqual(self.recognize()[0]['mode'], 'ocr')
        self.docs['model:owner']['enabled'] = False
        self.docs['model:owner']['models'] = [profile('vision', 'MiniMax-VL-01')]
        self.assertEqual(self.recognize()[0]['mode'], 'ocr')
        self.transport.request_json.assert_not_called()

    def test_rechecks_selected_profile_capabilities_and_model(self):
        for selected in (profile('vision', 'WanWu/GLM-Auto'), profile('vision', 'MiniMax-VL-01', image_input=False)):
            with self.subTest(selected=selected['model']), patch.object(self.model, 'profile', return_value=selected):
                self.assertEqual(self.recognize()[0]['mode'], 'ocr')
        self.transport.request_json.assert_not_called()

    def test_model_error_bad_json_and_empty_observation_fall_back_to_ocr(self):
        for content in ('not JSON', '[]', '{}', '{"text": []}', '{"text": ""}', '{"text": "   "}'):
            with self.subTest(content=content):
                self.transport.request_json.return_value = {'choices': [{'message': {'content': content}}]}
                self.assertEqual(self.recognize()[0], {'id': self.image['id'], 'text': OCR, 'mode': 'ocr'})
        self.transport.request_json.side_effect = RuntimeError('fixture failure must not appear in results')
        self.assertEqual(self.recognize()[0]['text'], OCR)

    def test_without_ocr_unavailable_or_failed_vision_explicitly_reports_unreadable(self):
        self.image['ocr_text'] = ''
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                self.docs['model:owner']['enabled'] = enabled
                self.transport.request_json.side_effect = TimeoutError('fixture timeout')
                result = self.recognize()[0]
                self.assertEqual(result['mode'], 'unreadable')
                self.assertIn('图片未识别', result['text'])
                self.assertIn('不能据此出题', result['text'])
                self.assertNotIn('timeout', result['text'])

    def test_vision_can_read_without_ocr_and_partial_failures_keep_image_order(self):
        images = [image_source(color, '') for color in ('red', 'blue', 'green')]
        self.transport.request_json.side_effect = [response({'text': VISUAL}), TimeoutError(), response({'text': VISUAL})]
        result = self.recognize(images)
        self.assertEqual([item['id'] for item in result], [item['id'] for item in images])
        self.assertEqual([item['mode'] for item in result], ['vision', 'unreadable', 'vision'])
        self.assertNotIn('OCR文字：', result[0]['text'])

    def test_complete_ocr_and_fenced_visual_are_preserved_when_they_fit(self):
        self.image['ocr_text'] = '水流方向。' * 280
        raw = '```json\n' + json.dumps({'text': VISUAL}, ensure_ascii=False) + '\n```'
        self.transport.request_json.return_value = {'choices': [{'message': {'content': raw}}]}
        result = self.recognize()[0]
        self.assertEqual(result['mode'], 'vision')
        self.assertIn(self.image['ocr_text'], result['text'])
        self.assertTrue(result['text'].endswith(VISUAL))
        self.assertLessEqual(len(result['text']), 2000)

    def test_oversized_merge_falls_back_to_complete_ocr_instead_of_truncating(self):
        self.image['ocr_text'] = '水流方向。' * 350
        self.transport.request_json.return_value = response({'text': '明确可见的连接。' * 70})
        result = self.recognize()[0]
        self.assertEqual(result, {'id': self.image['id'], 'text': self.image['ocr_text'], 'mode': 'ocr'})
        self.image['ocr_text'] = '水流方向。' * 400
        self.assertEqual(self.recognize()[0], {'id': self.image['id'], 'text': self.image['ocr_text'], 'mode': 'ocr'})

    def test_oversized_visual_without_ocr_is_unreadable_instead_of_truncated(self):
        self.image['ocr_text'] = ''
        self.transport.request_json.return_value = response({'text': '明确可见的连接。' * 300})
        self.assertEqual(self.recognize()[0]['mode'], 'unreadable')

    def test_context_budget_rejection_uses_ocr_without_transport(self):
        self.docs['model:owner']['models'][0]['context_window'] = 100
        self.assertEqual(self.recognize()[0]['mode'], 'ocr')
        self.transport.request_json.assert_not_called()

    def test_invalid_image_payloads_are_rejected_before_any_model_call(self):
        invalid = [None, {}, [], 'image', [None], [self.image] * 4, [self.image, self.image]]
        for fields in ({'id': None}, {'id': 'a' * 63}, {'id': 'G' * 64}, {'id': 'A' * 64},
                       {'data_url': None}, {'data_url': 'https://image.example/test.jpg'},
                       {'data_url': 'data:image/png;base64,AAAA'}, {'data_url': 'data:image/jpeg;base64,!invalid!'},
                       {'data_url': 'data:image/jpeg;base64,' + 'A' * (4 * ((sharing.MAX_IMAGE_BYTES + 2) // 3) + 4)},
                       {'data_url': 'data:image/jpeg;base64,' + base64.b64encode(b'not JPEG').decode()},
                       {'data_url': 'data:image/jpeg;base64,'}, {'ocr_text': None}, {'ocr_text': []},
                       {'ocr_text': 'x' * 2001}):
            invalid.append([{**self.image, **fields}])
        invalid.append([image_source(fmt='PNG')])
        content = base64.b64decode(self.image['data_url'].split(',', 1)[1])
        invalid.append([{**self.image, 'data_url': 'data:image/jpeg;base64,' + base64.b64encode(content[:-20]).decode()}])
        invalid.append([self.image, {**self.image, 'id': 'b' * 64, 'data_url': 'broken'}])
        for values in invalid:
            with self.subTest(case=invalid.index(values)), self.assertRaises(AssistantError):
                self.recognize(values) if values is not None else sharing.generate(self.model, {'mode': 'image_sources'})
        self.transport.request_json.assert_not_called()

    def test_pixel_limit_rejects_before_decoding_or_model_transport(self):
        with patch.object(sharing, 'MAX_IMAGE_PIXELS', 20), self.assertRaises(AssistantError):
            self.recognize()
        self.transport.request_json.assert_not_called()

    def test_question_model_receives_only_text_summaries_and_cannot_confirm_images(self):
        summaries = [{'id': self.image['id'], 'text': OCR, 'mode': 'ocr', 'data_url': self.image['data_url']}]
        value = question(image_ids=[self.image['id']], image_confirmed=True)
        result = self.generate([value], source_images=summaries, images=[self.image])
        self.assertEqual(result[0]['image_ids'], [self.image['id']])
        self.assertNotIn('image_confirmed', result[0])
        self.assertEqual(self.sent()['model'], 'WanWu/GLM-Auto')
        request = json.dumps(self.sent(), ensure_ascii=False)
        self.assertNotIn('data:image/', request)
        self.assertNotIn('image_url', request)
        context = json.loads(self.sent()['messages'][1]['content'])
        self.assertEqual(context['source_images'], [{'id': self.image['id'], 'text': OCR, 'mode': 'ocr'}])
        self.assertIn('逐字摘自', self.sent()['messages'][0]['content'])

    def test_nine_summaries_can_be_used_across_three_image_batches(self):
        summaries = [{'id': f'{index:064x}', 'text': OCR, 'mode': 'ocr'} for index in range(9)]
        self.generate([question(image_ids=[summaries[8]['id']])], source_images=summaries)
        self.assertEqual(len(json.loads(self.sent()['messages'][1]['content'])['source_images']), 9)

    def test_visual_source_roundtrip_preserves_literal_evidence_and_filters_secrets(self):
        self.image['ocr_text'] = OCR + '\r\nAPI key: fixture-secret\r\n' + VISUAL
        summaries = self.recognize()
        self.assertIn(OCR, summaries[0]['text'])
        self.assertNotIn('fixture-secret', summaries[0]['text'])
        evidence = sharing.VISION_NOTE + VISUAL
        result = self.generate([question(image_ids=[self.image['id']], evidence=evidence)], source_images=summaries)
        self.assertEqual(result[0]['evidence'], evidence)
        context = json.loads(self.sent()['messages'][1]['content'])
        self.assertEqual(context['source_images'], summaries)

    def test_image_reference_validation_is_left_to_shared_guard(self):
        from lan_bitable_template_portal.learning import LearningError
        from lan_bitable_template_portal.learning_sharing import validate_questions
        summaries = [{'id': f'{index:064x}', 'text': OCR, 'mode': 'ocr'} for index in range(4)]
        for ids in (None, '0' * 64, ['f' * 64], [None], [{}], ['0' * 64, '0' * 64], [item['id'] for item in summaries]):
            with self.subTest(ids=ids):
                values = [question(image_ids=ids)]
                self.assertEqual(self.generate(values, source_images=summaries), values)
                with self.assertRaises(LearningError):
                    validate_questions(values, SOURCE, draft=True, images=summaries)
        for fields in ({}, {'image_ids': []}):
            with self.subTest(fields=fields):
                values = [question(**fields)]
                self.assertEqual(self.generate(values, source_images=summaries), values)
                with self.assertRaises(LearningError):
                    validate_questions(values, SOURCE, draft=True, images=summaries)

    def test_invalid_evidence_reaches_shared_guard_unchanged(self):
        from lan_bitable_template_portal.learning import LearningError
        from lan_bitable_template_portal.learning_sharing import validate_questions
        summaries = [{'id': 'a' * 64, 'text': OCR, 'mode': 'vision'},
                     {'id': 'b' * 64, 'text': VISUAL, 'mode': 'ocr'},
                     {'id': 'c' * 64, 'text': OCR, 'mode': 'unreadable'}]
        for ids, evidence in ((['b' * 64], OCR), (['c' * 64], OCR), (['a' * 64], '图中所有水流方向均已核实正确。'),
                              (['a' * 64], None), (['a' * 64], '水流方向'), (['a' * 64], '水' * 801)):
            with self.subTest(ids=ids, evidence=evidence):
                values = [question(image_ids=ids, evidence=evidence)]
                self.assertEqual(self.generate(values, source_images=summaries), values)
                with self.assertRaises(LearningError):
                    validate_questions(values, SOURCE, draft=True, images=summaries)
        self.assertEqual(self.generate([question(image_ids=['a' * 64])], source_images=summaries)[0]['evidence'], OCR)

    def test_five_questions_with_missing_evidence_reach_shared_guard_without_early_exhaustion(self):
        from lan_bitable_template_portal.learning import LearningError
        from lan_bitable_template_portal.learning_sharing import validate_questions
        summaries = [{'id': 'a' * 64, 'text': OCR, 'mode': 'vision'}]
        values = [question(stem=f'图中文字知识点{index}是什么？', image_ids=['a' * 64]) for index in range(5)]
        values[2].pop('evidence')
        result = self.generate(values, source_images=summaries, question_types=['single'] * 5)
        self.assertEqual(len(result), 5)
        self.assertEqual(result, values)
        accepted = []
        for value in result:
            try:
                accepted.extend(validate_questions([value], SOURCE, ['single'], draft=True, images=summaries))
            except LearningError:
                pass
        self.assertEqual(len(accepted), 4)
        self.assertTrue(all(not value['image_confirmed'] for value in accepted))
        context = json.loads(self.sent()['messages'][1]['content'])
        self.assertEqual(context['meeting_text'], SOURCE)
        self.assertNotIn(OCR, context['meeting_text'])

    def test_newline_and_fullwidth_evidence_reaches_shared_normalized_validation(self):
        from lan_bitable_template_portal.learning_sharing import validate_questions
        summary = {'id': 'a' * 64, 'mode': 'ocr', 'text': '图中标注供水入口，\n与回水出口；\n箭头表示水流方向。'}
        evidence = '图中标注供水入口,与回水出口;箭头表示水流方向。'
        values = [question(image_ids=[summary['id']], evidence=evidence)]
        result = self.generate(values, source_images=[summary])
        self.assertEqual(result, values)
        self.assertEqual(validate_questions(result, SOURCE, ['single'], draft=True, images=[summary])[0]['evidence'], evidence)

    def test_non_dict_question_and_image_type_check_are_deferred_to_single_question_guard(self):
        from lan_bitable_template_portal.learning import LearningError
        from lan_bitable_template_portal.learning_sharing import validate_questions
        summaries = [{'id': 'a' * 64, 'text': OCR, 'mode': 'ocr'}]
        values = self.generate([None, question()], source_images=summaries)
        self.assertIsNone(values[0])
        self.assertEqual(values[1], question())
        with self.assertRaisesRegex(LearningError, '题目格式无效'):
            validate_questions(values, SOURCE, draft=True, images=summaries)
        values = [question(type='multiple', image_ids=['a' * 64])]
        self.assertEqual(self.generate(values, source_images=summaries, question_types=['single']), values)

    def test_invalid_summaries_and_question_payloads_fail_before_transport(self):
        summary = {'id': 'a' * 64, 'text': OCR, 'mode': 'ocr'}
        invalid = [{}, 'bad', [None], [summary] * 25, [summary, summary]]
        invalid += [[{**summary, **change}] for change in ({'id': 'bad'}, {'text': ''}, {'text': None},
                    {'text': 'x' * 2001}, {'text': 'data:image/jpeg;base64,AAAA'}, {'mode': 'confirmed'})]
        for summaries in invalid:
            with self.subTest(summaries=str(summaries)[:100]), self.assertRaises(AssistantError):
                self.generate([question()], source_images=summaries)
        for payload in ([], None, {'mode': 'unknown'}, {'source': SOURCE, 'count': True},
                        {'source': 'short', 'count': 1}, {'source': SOURCE, 'count': 1, 'question_types': ['bad']}):
            with self.subTest(payload=payload), self.assertRaises(AssistantError):
                sharing.generate(self.model, payload)
        self.transport.request_json.assert_not_called()

    def test_plain_text_question_contract_and_type_checks_are_preserved(self):
        value = question(stem='检修工作开始前应做什么？', evidence=SOURCE[:28])
        self.assertEqual(self.generate([value]), [value])
        self.assertNotIn('source_images', json.loads(self.sent()['messages'][1]['content']))
        self.assertEqual(self.generate([value], source_images=[]), [value])
        self.assertEqual(self.generate([value], question_types=['single']), [value])
        from lan_bitable_template_portal.learning import LearningError
        from lan_bitable_template_portal.learning_sharing import validate_questions
        values = self.generate([value], question_types=['multiple'])
        with self.assertRaises(LearningError):
            validate_questions(values, SOURCE, ['multiple'], draft=True)
        self.transport.request_json.return_value = response({'questions': []})
        self.assertEqual(sharing.generate(self.model, {'source': SOURCE, 'count': 1}), {'questions': []})
        self.transport.request_json.return_value = response({'questions': [value, value]})
        with self.assertRaises(AssistantError):
            sharing.generate(self.model, {'source': SOURCE, 'count': 1})


class SharingImageBridgeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from lan_bitable_template_portal.lighthouse_routes import install_lighthouse_routes
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('Network forbidden')))
        self.http = SimpleNamespace(post=AsyncMock())
        self.current = SimpleNamespace(prepare=AsyncMock(), http_client=AsyncMock(return_value=self.http),
                                       descriptor={'port': 12345}, instance='fixture-instance', lease='fixture-lease', key='fixture-key')
        self.factory = self.enterContext(patch('openclaw_service.client.ResidentRuntime', return_value=self.current))
        self.enterContext(patch('lan_bitable_template_portal.lighthouse_routes.PortalAuthority'))
        self.enterContext(patch('lan_bitable_template_portal.lighthouse_icon_weather.install_icon_weather'))
        self.runtime = SimpleNamespace(state_store=SimpleNamespace(db_path=Path(__file__).parent / 'unused.sqlite'))
        install_lighthouse_routes(Mock(), SimpleNamespace(bound_port=0, preferred_port=8000), self.runtime)

    def reply(self, data):
        self.http.post.return_value = SimpleNamespace(json=lambda: {'ok': True, 'data': data})

    async def test_original_signature_returns_question_list_without_new_payload_fields(self):
        questions = [question()]
        self.reply({'questions': questions})
        self.assertEqual(await self.runtime.generate_sharing_questions('owner', SOURCE, [], 1), questions)
        sent = self.http.post.call_args.kwargs['json']
        self.assertEqual(sent['owner'], 'owner')
        self.assertEqual(sent['source'], SOURCE)
        self.assertNotIn('images', sent)
        self.assertNotIn('source_images', sent)
        self.assertNotIn('mode', sent)

    async def test_image_sources_and_summaries_are_forwarded_with_correct_return_shapes(self):
        images = [image_source()]
        data = {'images': [{'id': images[0]['id'], 'text': OCR, 'mode': 'ocr'}]}
        self.reply(data)
        self.assertEqual(await self.runtime.generate_sharing_questions('owner', '', [], 0,
                         images=images, mode='image_sources'), data)
        sent = self.http.post.call_args.kwargs['json']
        self.assertEqual((sent['mode'], sent['images']), ('image_sources', images))
        self.reply({'questions': [question()]})
        result = await self.runtime.generate_sharing_questions('owner', SOURCE, [], 1, source_images=data['images'])
        self.assertEqual(result, [question()])
        self.assertEqual(self.http.post.call_args.kwargs['json']['source_images'], data['images'])

    async def test_oversized_image_request_fails_before_starting_ipc(self):
        image = image_source()
        image['data_url'] += 'A' * (8 * 1024 * 1024)
        with self.assertRaises(AssistantError) as error:
            await self.runtime.generate_sharing_questions('owner', '', [], 0, images=[image], mode='image_sources')
        self.assertEqual(error.exception.status, 413)
        self.factory.assert_not_called()
        self.http.post.assert_not_called()

    async def test_worker_error_remains_an_assistant_error(self):
        self.http.post.return_value = SimpleNamespace(json=lambda: {'ok': False, 'error': 'fixture rejected'})
        with self.assertRaisesRegex(AssistantError, 'fixture rejected'):
            await self.runtime.generate_sharing_questions('owner', '', [], 0, images=[image_source()], mode='image_sources')


if __name__ == '__main__':
    unittest.main()

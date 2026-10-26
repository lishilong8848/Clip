"""File extraction and manual drafts use isolated state and never send notifications."""
import asyncio
import copy
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from bin import test_learning_sharing_materials as material_fixtures
from bin.test_learning_sharing_materials import manual
from bin.test_learning_sharing import ADMIN
from lan_bitable_template_portal.learning import LearningError
from lan_bitable_template_portal.learning_sharing import validate_questions
from lan_bitable_template_portal.learning_sharing_routes import SharingRuntime
from lan_bitable_template_portal.sharing_question_import import imported_question, import_questions
from openclaw_service.assistant.lighthouse_sharing import generate


def source_question(number, *, answer='A', explanation='按原文核对。'):
    stem = f'第{number}项应该核对什么？'
    quote = f'{number}. {stem}\nA. 运行记录\nB. 随意猜测\n答案：{answer}\n解析：{explanation}'
    return {'type': 'single', 'stem': stem, 'options': ['运行记录', '随意猜测'], 'answer': answer,
            'explanation': explanation, 'source_quote': quote}


class FileImportTests(unittest.IsolatedAsyncioTestCase):
    async def test_original_answers_not_model_indices_and_missing_answers_remain_blank(self):
        original = source_question(1, answer='B')
        original['correct'] = [0]
        question, end = imported_question(original, original['source_quote'], 0, [])
        self.assertEqual(question['correct'], [1])
        self.assertEqual(end, len(original['source_quote']))
        missing = source_question(2, answer='', explanation='')
        q, _ = imported_question(missing, missing['source_quote'], 0, [])
        self.assertEqual(q['correct'], [])
        self.assertEqual(q['explanation'], '')
        self.assertEqual(validate_questions([q], '', draft=True)[0]['correct'], [])
        with self.assertRaises(LearningError):
            validate_questions([q] + manual(4), '')

    async def test_hallucinated_or_reordered_evidence_is_rejected(self):
        raw = source_question(1)
        for changed in ({**raw, 'answer': 'C'}, {**raw, 'stem': '模型改写的新题'},
                        {**raw, 'explanation': '模型补造解析'}, {**raw, 'source_quote': '不存在的原文'}):
            with self.subTest(changed=changed), self.assertRaises(LearningError):
                imported_question(changed, raw['source_quote'], 0, [])
        with self.assertRaises(LearningError):
            imported_question(raw, raw['source_quote'], len(raw['source_quote']), [])

    async def test_multiple_choice_and_interview_keep_original_answers(self):
        values = [
            {'type': 'multiple', 'stem': '需核对哪些资料？', 'options': ['原始记录', '审批记录'],
             'answer': 'AB', 'explanation': '两项都需要。', 'source_quote': '需核对哪些资料？\nA. 原始记录\nB. 审批记录\n答案：AB\n解析：两项都需要。'},
            {'type': 'interview', 'stem': '说明核对方法。', 'options': [], 'answer': '对照原始记录逐项核对。',
             'explanation': '', 'source_quote': '说明核对方法。\n参考答案：对照原始记录逐项核对。'},
        ]
        imported = [imported_question(raw, raw['source_quote'], 0, [])[0] for raw in values]
        checked = validate_questions(imported, '', draft=True)
        self.assertEqual(checked[0]['correct'], [0, 1])
        self.assertEqual(checked[1]['reference_answer'], values[1]['answer'])
        self.assertEqual(checked[1]['explanation'], '')

    async def test_resume_keeps_checkpoint_without_reimport_or_publishing(self):
        first, second = source_question(1), source_question(2)
        source = {'text': first['source_quote'] + '\n\n' + second['source_quote']}
        state = {'schema': 1, 'slots': [], 'offset': 0, 'exhausted': False, 'notes': []}
        saved = []
        async def checkpoint(questions, current, progress):
            saved.append((copy.deepcopy(questions), copy.deepcopy(current)))
        provider = AsyncMock(side_effect=[{'questions': [first], 'has_more': True}, RuntimeError('disconnect')])
        with self.assertRaises(RuntimeError):
            await import_questions({'owner': 'fixture'}, source, state, provider, checkpoint)
        self.assertEqual(len(saved[-1][0]), 1)
        resumed = saved[-1][1]
        provider = AsyncMock(return_value={'questions': [second], 'has_more': False})
        result = await import_questions({'owner': 'fixture'}, source, resumed, provider, checkpoint)
        self.assertEqual(len(result), 2)
        self.assertNotIn(first['source_quote'], provider.call_args.args[1])
        self.assertEqual(provider.call_args.kwargs['mode'], 'question_import')
        self.assertTrue(all(q['origin'] == 'manual' for q in result))

    async def test_duplicate_page_and_nonprogressing_page_do_not_overwrite_checkpoint(self):
        raw = source_question(1)
        for response in ({'questions': [], 'has_more': True}, {'questions': [raw, raw], 'has_more': False}):
            state = {'schema': 1, 'slots': [], 'offset': 0, 'exhausted': False, 'notes': []}
            with self.subTest(response=response), self.assertRaises(LearningError):
                await import_questions({'owner': 'fixture'}, {'text': raw['source_quote']}, state,
                                       AsyncMock(return_value=response), AsyncMock())
            self.assertEqual(state['slots'], [])
            self.assertEqual(state['offset'], 0)

    async def test_false_end_marker_cannot_skip_unread_tail(self):
        first, second = source_question(1), source_question(2, answer='', explanation='')
        source = {'text': '题目 选项A 选项B 答案 解析\n' + first['source_quote'] + '\n' + second['source_quote']}
        state = {'schema': 1, 'slots': [], 'offset': 0, 'exhausted': False, 'notes': []}
        provider = AsyncMock(side_effect=[{'questions': [first], 'has_more': False}, {'questions': [second], 'has_more': False}])
        result = await import_questions({'owner': 'fixture'}, source, state, provider, AsyncMock())
        self.assertEqual(len(result), 2)
        self.assertEqual(result[-1]['correct'], [])
        self.assertIn('选项A', provider.call_args.args[2][0])

    def test_model_import_prompt_does_not_generate_new_questions_or_use_tools(self):
        raw = source_question(1)
        model = Mock()
        model.complete.return_value = json.dumps({'questions': [raw], 'has_more': False}, ensure_ascii=False)
        result = generate(model, {'mode': 'question_import', 'source': raw['source_quote'], 'count': 5})
        self.assertEqual(result['questions'], [raw])
        self.assertFalse(result['has_more'])
        self.assertIn('不生成新题', model.complete.call_args.args[0][0]['content'])
        self.assertIsNone(model.complete.call_args.kwargs['max_tokens'])


class ManualAndImportWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.fixture = material_fixtures.SharingMaterialsTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.core, self.learning = self.fixture.core, self.fixture.learning
        with self.learning.transaction() as conn:
            self.learning._put('person', 'p_import', {'id': 'p_import', 'name': '测试人员', 'active': True,
                'login_ids': ['ou_import'], 'scopes': ['A']}, conn, False)

    def test_manual_fields_saved_atomically_idempotently_but_incomplete_cannot_publish(self):
        q = {'origin': 'manual', 'type': 'single', 'stem': '先写题干', 'options': ['', ''],
             'correct': [], 'explanation': ''}
        args = {'participant_ids': ['p_import'], 'auto_generate': False, 'initial_questions': [q]}
        first = self.core.create('', 'manual-new-fields-fixture', ADMIN, **args)
        self.assertEqual(self.core.create('', 'manual-new-fields-fixture', ADMIN, **args), first)
        batch = self.core.batch(first['id'])
        self.assertEqual(batch['status'], 'draft')
        self.assertEqual(batch['questions'][0]['stem'], q['stem'])
        with self.assertRaises(LearningError):
            self.core.publish(first['id'], 1, ADMIN, 'cli_fixture')
        with self.assertRaises(LearningError):
            self.core.create('', 'manual-new-fields-fixture', ADMIN, **{**args, 'initial_questions': manual(1)})
        self.assertEqual(self.core.attempts(first['id']), [])

    def test_uploaded_file_uses_existing_worker_and_imports_all_question_types(self):
        raw = [source_question(1), source_question(2)]
        bid = self.core.create('', 'file-question-import-fixture', ADMIN, material_ids=['uploaded-file'],
            participant_ids=['p_import'], import_questions=True)['id']
        generate = AsyncMock(return_value={'questions': raw, 'has_more': False})
        runtime = SharingRuntime(SimpleNamespace(service=object(), generate_sharing_questions=generate), lambda: self.learning)
        runtime.core = self.core
        with patch('lan_bitable_template_portal.sharing_sources.SharingMaterials.read', return_value={
                'title': '题库.xlsx', 'text': '\n\n'.join(q['source_quote'] for q in raw), 'extraction_complete': True}):
            asyncio.run(runtime.generate(self.core.batch(bid)))
        batch = self.core.batch(bid)
        self.assertEqual(batch['status'], 'draft', batch.get('error'))
        self.assertEqual(len(batch['questions']), 2)
        self.assertIn('未补造', batch['generation_note'])
        self.assertEqual(self.core.attempts(bid), [])
        self.assertEqual(generate.call_count, 1)


if __name__ == '__main__':
    unittest.main()

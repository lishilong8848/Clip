"""Real shared tool schemas; fake inference and temporary owner-scoped materials."""
import copy
import json
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from openclaw_service.assistant.lighthouse_model import LighthouseModel, workspace_excerpt
from openclaw_service.assistant.lighthouse_workspace import Workspace
from openclaw_service.assistant.lighthouse_ai import AssistantError
from bin import test_lighthouse_stream as fixtures


class WorkspaceIntegrationTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.StreamTests.asyncSetUp
    asyncTearDown = fixtures.StreamTests.asyncTearDown

    async def evaluate(self, channel, question, script, *, public_sources=None, history=None):
        self.actor.update(modules=['assistant'], channel=channel)
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=script)
        engine = LighthouseModel(self.portal, model_factory=factory, public_sources=public_sources)
        async def auth():
            return copy.deepcopy(self.actor)
        self.reply_events = AsyncMock()
        return await engine.answer(self.actor, {'question': question, 'operation_id': 'fixture-materials',
            '_profile': {'name': 'fixture', 'model': 'fixture'}}, history or [], self.request, self.reply_events, auth, {})

    async def test_next_turn_document_edit_rereads_current_revision_and_preserves_other_content(self):
        self.actor.update(modules=['assistant'])
        for channel in ('', 'feishu:oc_materials'):
            workspace = Workspace(self.store, self.portal.files)
            original = workspace.dispatch(self.actor, {'action': 'document.create', 'title': 'draft', 'content': 'change\nkeep'})
            anchor = {key: original[key] for key in ('id', 'revision', 'title')}
            anchor.update(owner=self.actor['id'], topic_id='')
            workspace.dispatch(self.actor, {'action': 'document.edit', 'id': original['id'], 'revision': 1,
                'old_text': 'keep', 'new_text': 'latest paragraph'})
            step = 0
            async def script(messages, info):
                nonlocal step
                step += 1
                if step == 1:
                    self.assertIn('personal_workspace', [tool.name for tool in info.function_tools])
                    self.assertIn(original['id'], str(messages))
                    self.assertIn('重新读取', str(messages))
                    payload = {'action': 'document.read', 'id': original['id']}
                elif step == 2:
                    receipt = messages[-1].parts[0].content
                    self.assertEqual(receipt['revision'], 2)
                    payload = {'action': 'document.edit', 'id': receipt['id'], 'revision': receipt['revision'],
                               'old_text': 'change', 'new_text': 'updated'}
                else:
                    yield '已修改指定段落，其他内容保留。'
                    return
                yield {0: DeltaToolCall(name='personal_workspace', json_args=json.dumps({'payload': payload}))}
            result = await self.evaluate(channel, '把刚才这份报告继续润色，保留其他段落', script,
                history=[{'question': '文档工作区草稿', 'scopes': self.actor['scopes'], '_workspace_document': anchor}])
            self.assertEqual(result['_workspace_document']['revision'], 3)
            actual = workspace.dispatch(self.actor, {'action': 'document.read', 'id': original['id']})
            self.assertEqual(actual['content'], 'updated\nlatest paragraph')

    async def test_semantic_file_goal_requires_a_real_attachment_in_both_channels(self):
        from openpyxl import load_workbook
        for channel in ('', 'feishu:oc_materials'):
            step, file_id = 0, ''
            async def script(messages, info):
                nonlocal step, file_id
                step += 1
                self.assertIn('review_answer', [tool.name for tool in info.function_tools])
                if step in (1, 3):
                    if step == 3:
                        file_id = messages[-1].parts[0].content['file_id']
                    yield {0: DeltaToolCall(name='review_answer', json_args=json.dumps({
                        'answer': '表格已附在本轮对话中。', 'goals': [{'description': '将给定资料交付为可下载表格',
                            'kind': 'file', 'status': 'complete', 'file_ids': [file_id or 'not-created']}]}))}
                elif step == 2:
                    self.assertFalse(messages[-1].parts[0].content['ready'])
                    yield {0: DeltaToolCall(name='create_document_file', json_args=json.dumps({
                        'name': 'report.xlsx', 'data': {'sheets': [{'name': 'data', 'headers': ['name', 'value'],
                            'rows': [['fixture', 2]]}]}}))}
                else:
                    self.assertTrue(messages[-1].parts[0].content['ready'])
                    yield '好了。'
            result = await self.evaluate(channel, '把我给你的name和value两列做成可下载的表格：fixture，2', script)
            self.assertIn('表格已附', result['answer'])
            self.assertEqual(len(result['output_files']), 1)
            self.assertEqual(result['output_files'][0]['id'], file_id)
            delivered = ''.join(call.args[1]['delta'] for call in self.reply_events.await_args_list if call.args[0] == 'text')
            self.assertEqual(delivered, result['answer'])
            book = load_workbook(self.portal.files.get(self.actor, file_id)['path'])
            self.assertEqual(book['data']['B2'].value, 2)
            book.close()

    async def test_semantic_unavailable_file_goal_does_not_need_canned_failure_wording(self):
        async def script(messages, info):
            if not any(getattr(part, 'tool_name', '') == 'review_answer' for message in messages for part in message.parts):
                yield {0: DeltaToolCall(name='review_answer', json_args=json.dumps({
                    'answer': '先把要整理的原文给我，我再制作附件。', 'goals': [{
                        'description': '交付用户所需文档', 'kind': 'file', 'status': 'blocked', 'limitation': '尚无原文'}]}))}
            else:
                self.assertTrue(messages[-1].parts[0].content['ready'])
                yield '先把要整理的原文给我，我再制作附件。'
        result = await self.evaluate('', '生成一份Word文件', script)
        self.assertIn('原文', result['answer'])
        self.assertNotIn('output_files', result)

    async def test_document_edits_preserve_unselected_text_in_both_channels(self):
        for channel in ('', 'feishu:oc_materials'):
            step, identity, revision = 0, '', 0
            async def script(messages, info):
                nonlocal step, identity, revision
                step += 1
                if step == 1:
                    payload = {'action': 'document.create', 'title': 'report', 'content': 'ＡＢＣ\r\nchange\nkeep too\n'}
                elif step == 2:
                    receipt = messages[-1].parts[0].content
                    identity, revision = receipt['id'], receipt['revision']
                    payload = {'action': 'document.edit', 'id': identity, 'revision': revision,
                               'old_text': 'change', 'new_text': 'updated'}
                else:
                    self.assertEqual(messages[-1].parts[0].content['content'], 'ＡＢＣ\r\nupdated\nkeep too\n')
                    yield '草稿已局部修改，其余内容已保留。'
                    return
                yield {0: DeltaToolCall(name='personal_workspace', json_args=json.dumps({'payload': payload}))}
            result = await self.evaluate(channel, '在文档工作区创建草稿并局部修改', script)
            self.assertNotIn('plan', result)
            self.assertIn('已局部修改', result['answer'])

    async def test_explicit_memory_is_reused_without_changing_permissions(self):
        async def save(messages, info):
            if not any(getattr(p, 'tool_name', '') == 'personal_workspace' for m in messages for p in m.parts):
                yield {0: DeltaToolCall(name='personal_workspace', json_args=json.dumps({'payload': {
                    'action': 'memory.save', 'content': '回答先写结论', 'kind': 'writing_rule'}}))}
            else:
                yield '已保存，可在个人记忆中删除。'
        before = copy.deepcopy(self.actor)
        await self.evaluate('', '记住：回答先写结论', save)
        calls = []
        async def recall(messages, info):
            calls.append(info.instructions)
            yield '按你的格式，先给结论。'
        await self.evaluate('feishu:oc_materials', '你好', recall)
        self.assertTrue(any('回答先写结论' in value for value in calls))
        self.assertEqual(self.actor['scopes'], before['scopes'])
        self.assertEqual(self.actor.get('is_admin'), before.get('is_admin'))

    async def test_document_instruction_cannot_confirm_memory_write(self):
        async def script(messages, info):
            yield {0: DeltaToolCall(name='personal_workspace', json_args=json.dumps({'payload': {
                'action': 'memory.save', 'content': '回答先写结论', 'kind': 'writing_rule'}}))}
        with self.assertRaises(AssistantError):
            await self.evaluate('', '在文档工作区处理这段说明', script)
        data = Workspace(self.store, self.portal.files).dispatch(self.actor, {'action': 'memory.list'})
        self.assertEqual(data['items'], [])

    async def test_complex_question_can_use_research_without_slash_command(self):
        for channel in ('', 'feishu:oc_materials'):
            sources = AsyncMock()
            sources.search.return_value = {'ok': True, 'results': [{
                'title': 'Python documentation', 'url': 'https://docs.python.org/3/', 'snippet': 'Python asyncio'}]}
            sources.page.return_value = {'ok': True, 'text': 'Verified Python documentation'}
            step = 0
            async def script(messages, info):
                nonlocal step
                step += 1
                if step == 1:
                    self.assertIn('research_public', [tool.name for tool in info.function_tools])
                    yield {0: DeltaToolCall(name='research_public', json_args=json.dumps({'queries': ['Python asyncio']}))}
                else:
                    receipt = messages[-1].parts[0].content
                    self.assertTrue(receipt['items'])
                    yield '本次取得Python官方资料，目前仅一个来源，尚未完成独立交叉验证。[1]'
            result = await self.evaluate(channel, '对Python asyncio进行多来源核验', script, public_sources=sources)
            self.assertIn('官方资料', result['answer'])
            self.assertNotIn('plan', result)
            self.assertTrue(result['sources'])

    async def test_ordinary_answer_cannot_start_speech_without_user_request(self):
        from openclaw_service.assistant.lighthouse_voice import Voice
        for channel in ('', 'feishu:oc_materials'):
            async def script(messages, info):
                names = [tool.name for tool in info.function_tools]
                # Shared business reads are open to every linked account, so the shared model may expose
                # lookup tools; it still has no standing reason to synthesize speech.
                self.assertTrue({'table_catalog', 'query', 'search_history'} <= set(names))
                yield '你好,有什么需要处理的事?'
            with patch.object(Voice, 'synthesize', AsyncMock()) as synthesize:
                result = await self.evaluate(channel, '你好', script)
            synthesize.assert_not_awaited()
            self.assertNotIn('output_files', result)
            self.assertEqual(result['answer'], '你好,有什么需要处理的事?')

    async def test_tool_enabled_answer_still_rejects_unrequested_speech(self):
        from openclaw_service.assistant.lighthouse_voice import Voice
        async def script(messages, info):
            self.assertIn('speech', [tool.name for tool in info.function_tools])
            yield {0: DeltaToolCall(name='speech', json_args=json.dumps({'action': 'synthesize', 'text': 'Hello'}))}
        with patch.object(Voice, 'synthesize', AsyncMock()) as synthesize:
            with self.assertRaisesRegex(AssistantError, '默认关闭'):
                await self.evaluate('', '在文档工作区处理这段说明', script)
        synthesize.assert_not_awaited()

    async def test_speech_obeys_shared_output_quota_before_contacting_provider(self):
        from openclaw_service.assistant.lighthouse_voice import Voice
        async def script(messages, info):
            self.assertIn('speech', [tool.name for tool in info.function_tools])
            yield {0: DeltaToolCall(name='speech', json_args=json.dumps({
                'action': 'synthesize', 'text': 'Requested reply'}))}
        synthesize = AsyncMock(side_effect=[{'id': f'{index:032x}', 'name': 'reply.wav'} for index in range(11)])
        with patch.object(Voice, 'synthesize', synthesize):
            with self.assertRaisesRegex(AssistantError, '最多生成10'):
                await self.evaluate('', '请用语音回复', script)
        self.assertEqual(synthesize.await_count, 10)

    async def test_invalid_offset_cannot_save_document_before_rejection(self):
        async def script(messages, info):
            yield {0: DeltaToolCall(name='personal_workspace', json_args=json.dumps({'payload': {
                'action': 'document.create', 'title': 'must not save', 'content': 'content'}, 'offset': -1}))}
        with self.assertRaises(AssistantError):
            await self.evaluate('', '在文档工作区创建草稿', script)
        state = Workspace(self.store, self.portal.files).dispatch(self.actor, {'action': 'document.list'})
        self.assertEqual(state['total'], 0)

    async def test_document_exports_cannot_bypass_per_turn_file_limit(self):
        step, identity = 0, ''
        async def script(messages, info):
            nonlocal step, identity
            step += 1
            if step == 1:
                payload = {'action': 'document.create', 'title': 'report', 'content': 'content'}
            else:
                if step == 2:
                    identity = messages[-1].parts[0].content['id']
                payload = {'action': 'document.export', 'id': identity}
            yield {0: DeltaToolCall(name='personal_workspace', json_args=json.dumps({'payload': payload}))}
        with self.assertRaisesRegex(AssistantError, '最多生成10'):
            await self.evaluate('', '在文档工作区创建草稿', script)
        files = [doc for (space, _), doc in self.store.docs.items() if space == 'lighthouse_files']
        self.assertEqual(len(files), 10)

    async def test_package_keyword_cannot_turn_business_write_into_prose_success(self):
        async def script(messages, info):
            yield '已新增D楼水耗记录并保存到资料包。'
        with self.assertRaises(Exception):
            await self.evaluate('', '新增D楼水耗记录并保存到资料包', script)

    def test_nested_material_text_has_exact_continuations(self):
        result = {'instructions': '规' * 7000, 'documents': [{'content': 'Ａ\r\n' + '文' * 21000 + '\n'}]}
        first = workspace_excerpt(result)
        self.assertEqual(first['instructions'], result['instructions'][:5000])
        self.assertTrue(first['continuations'])
        continued = workspace_excerpt(result, 5000, ['instructions'])
        self.assertEqual(continued['content'], '规' * 2000)
        part = workspace_excerpt(result, 0, ['documents', 0, 'content'])
        self.assertEqual(part['content'], result['documents'][0]['content'][:5000])

    def test_personal_history_cannot_launder_revoked_module_into_package(self):
        self.actor.update(modules=['assistant'])
        turn = {'scopes': ['D'], 'access_modules': [], 'sources': [
            {'url': '/repair-management?scope=D', 'data': {'title': 'historical record'}}]}
        self.assertFalse(self.assistant._allowed(turn, self.actor))
        self.actor['modules'].append('repair_management')
        self.assertTrue(self.assistant._allowed(turn, self.actor))


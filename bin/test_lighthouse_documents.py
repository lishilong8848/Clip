"""Read-only Feishu document and shared web/agent routing, without external I/O."""
import asyncio
import copy
import json
import sys
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).parent))
from pydantic_ai.models.function import DeltaToolCall, FunctionModel
from lan_bitable_template_portal.lighthouse_bridge import PortalAuthority
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_documents import document_url, document_links, read_document
from openclaw_service.assistant.lighthouse_model import LighthouseModel
from openclaw_service.assistant.lighthouse_sources import SCOPES

URL = 'https://vnet.feishu.cn/docx/FixtureMeeting123'
ACTOR = {'id': 'fixture', 'scopes': ['A'], 'learning_scopes': [], 'is_admin': False}


def service(title='A楼冷机会议纪要', content='A楼冷机轮巡，检查冷却水流量。'):
    return SimpleNamespace(_auth_headers=Mock(return_value={'Authorization': 'Bearer fixture'}),
        _request_payload=Mock(side_effect=[{'code': 0, 'data': {'document': {'title': title}}},
                                        {'code': 0, 'data': {'content': content}}]))


class DocumentReadTests(unittest.TestCase):
    def test_only_canonical_tenant_docx_urls_never_arbitrary_hosts_or_paths(self):
        self.assertEqual(document_url(URL + '?dcuId=123'), URL)
        self.assertEqual(document_links('[纪要](' + URL + '?dcuId=123)'), [URL])
        for value in (URL.replace('https:', 'http:'), URL.replace('vnet.', 'other.'),
                URL.replace('vnet.feishu.cn', 'vnet.feishu.cn.evil.test'), URL.replace('docx/', 'base/'),
                URL.replace('docx/', 'docx/../'), URL + '/extra', URL.replace('vnet.', 'user@vnet.'),
                URL.replace('feishu.cn', 'feishu.cn:443'), URL + '\n', URL + '%2fextra'):
            with self.subTest(url=value):
                self.assertIsNone(document_url(value))
                fake = service()
                with self.assertRaises(AssistantError):
                    read_document(fake, ACTOR, value)
                fake._request_payload.assert_not_called()

    def test_document_followup_is_immediate_and_not_other_subjects(self):
        history = [{'question': '读取' + URL, 'scopes': ['A']}]
        self.assertEqual(document_links('这个会议纪要链接获取不到吗？', history), [URL])
        source = {'question': '这个纪要', 'sources': [{'url': URL, 'data': {'kind': 'feishu_document'}}]}
        self.assertEqual(document_links('这份文档总结一下', [source]), [URL])
        self.assertEqual(document_links('今天有几个通告？', history), [])
        self.assertEqual(document_links('这个纪要呢？', [*history, {'question': '你好', 'sources': [{'data': []}]}]), [])

    def test_native_client_read_is_only_get_and_sensitive_content_never_leaves_portal(self):
        fake = service(content='A楼检查冷机。\n身份证号：110101199001011234\nAPI Key: sk-abcdefghijk\n员工：测试人')
        value = read_document(fake, ACTOR, URL + '?dcuId=123')
        self.assertEqual(value['source_url'], URL)
        self.assertIn('检查冷机', value['text'])
        self.assertIn('员工:测试人', value['text'])
        self.assertNotIn('110101199001011234', json.dumps(value))
        self.assertNotIn('sk-abcdefghijk', json.dumps(value))
        self.assertTrue(value['read_only'])
        self.assertFalse(value['truncated'])
        fake._auth_headers.assert_called_once()
        calls = fake._request_payload.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(call.args[0] == 'GET' and call.args[1].startswith('https://open.feishu.cn/open-apis/docx/') for call in calls))
        self.assertTrue(calls[-1].args[1].endswith('/raw_content'))

    def test_scope_check_in_title_body_and_missing_scope_does_not_expand_permissions(self):
        for title, content in [('E楼纪要', 'E楼检查'), ('A楼纪要', 'A楼检查，E楼也有信息。'), ('培训', '公司一般资料')]:
            with self.subTest(title=title), self.assertRaises(AssistantError) as caught:
                read_document(service(title, content), ACTOR, URL)
            self.assertEqual(caught.exception.status, 403)
        value = read_document(service('培训', '公司一般资料'), {**ACTOR, 'is_admin': True, 'scopes': sorted(SCOPES)}, URL)
        self.assertIn('公司一般资料', value['text'])

    def test_errors_distinguish_permissions_network_and_invalid_response(self):
        fake = service()
        cases = [({'code': 99991672, 'msg': 'missing scope'}, '权限', 403),
            ({'code': 1770002, 'msg': 'not found'}, '不存在', 404),
            ({'code': 99991400, 'msg': 'frequency limit'}, 'code=99991400', 503),
            ({'code': 0, 'data': {}}, '信息不完整', 502)]
        for result, message, status in cases:
            fake._request_payload.side_effect = [result]
            with self.subTest(code=result['code']), self.assertRaisesRegex(AssistantError, message) as caught:
                read_document(fake, ACTOR, URL)
            self.assertEqual(caught.exception.status, status)
        fake._request_payload.side_effect = OSError('offline')
        with self.assertRaisesRegex(AssistantError, '暂不能判断为无权限'):
            read_document(fake, ACTOR, URL)

    def test_paged_text_and_empty_body_are_not_falsely_complete(self):
        value = read_document(service(content='A楼\n' + '字' * 7000), ACTOR, URL)
        self.assertEqual(len(value['text']), 6000)
        self.assertTrue(value['truncated'])
        rest = read_document(service(content='A楼\n' + '字' * 7000), ACTOR, URL, value['next_offset'])
        self.assertFalse(rest['truncated'])
        self.assertEqual(value['text'] + rest['text'], 'A楼\n' + '字' * 7000)
        self.assertTrue(read_document(service(content=''), ACTOR, URL)['empty'])


class DocumentRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_web_and_feishu_bridge_recheck_actor_and_return_same_read_only_document(self):
        client = SimpleNamespace(instance='fixture', lease='lease')
        runtime = SimpleNamespace(service=service(), state_store=None)
        authority = PortalAuthority(None, None, runtime, client)
        request = SimpleNamespace(state=SimpleNamespace())
        for channel in ('', 'feishu:oc_fixture'):
            actor = {**ACTOR, **({'channel': channel} if channel else {})}
            context_id = authority.context(request, actor)
            runtime.service = service()
            with patch('lan_bitable_template_portal.lighthouse_bridge.actor_for', AsyncMock(return_value=actor)):
                result = await authority.dispatch({'instance': 'fixture', 'lease': 'lease', 'context_id': context_id,
                    'action': 'feishu_document', 'payload': {'url': URL, 'scopes': ['A']}})
            self.assertIn('冷机', result['text'])
            self.assertEqual(runtime.service._request_payload.call_count, 2)
        with patch('lan_bitable_template_portal.lighthouse_bridge.actor_for', AsyncMock(return_value={**ACTOR, 'id': 'other'})):
            with self.assertRaisesRegex(AssistantError, '身份或权限已变化'):
                await authority.dispatch({'instance': 'fixture', 'lease': 'lease', 'context_id': context_id,
                    'action': 'feishu_document', 'payload': {'url': URL}})

    async def run_question(self, question, *, history=(), error=None, tool_retry=False, profile=None):
        results = read_document(service(), ACTOR, URL)
        bridge = SimpleNamespace(acall=AsyncMock(side_effect=error, return_value=results))
        portal = SimpleNamespace(assistant=SimpleNamespace(model=Mock()),
            catalog=SimpleNamespace(bridge=bridge), files=SimpleNamespace(context=lambda *_: [], image_parts=lambda *_: []))
        public = SimpleNamespace(search=AsyncMock(side_effect=AssertionError('Must not search public web for docx')),
                                 page=AsyncMock(side_effect=AssertionError('Must not treat docx as anonymous web')))
        rounds = []
        async def stream(messages, info):
            rounds.append(1)
            self.assertIn('feishu_document', {tool.name for tool in info.function_tools})
            prompts = '\n'.join(str(part.content) for message in messages for part in message.parts
                                if getattr(part, 'part_kind', '') == 'user-prompt')
            self.assertIn('冷机轮巡', prompts)
            if tool_retry and len(rounds) == 1:
                yield {0: DeltaToolCall(name='feishu_document', json_args=json.dumps({'url': URL}))}
            else:
                yield '已读取这份纪要：轮巡时检查冷却水流量。[1]'
        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)
        engine = LighthouseModel(portal, model_factory=factory, public_sources=public)
        request = SimpleNamespace(state=SimpleNamespace(portal_context={'id': 'fixture'}))
        result = await engine.answer(ACTOR, {'question': question, 'operation_id': 'fixture',
            '_profile': profile or {'name': 'fixture', 'model': 'fixture'}}, list(history), request,
            AsyncMock(), AsyncMock(return_value=copy.deepcopy(ACTOR)), {})
        return result, bridge, rounds

    async def test_explicit_docx_is_preloaded_without_public_queries_and_repeat_tool_does_not_refetch(self):
        result, bridge, rounds = await self.run_question('请读取' + URL + '?dcuId=123，这份会议纪要能获取到吗？', tool_retry=True)
        self.assertIn('已读取', result['answer'])
        self.assertEqual(len(result['sources']), 1)
        self.assertEqual(result['sources'][0]['url'], URL)
        bridge.acall.assert_awaited_once()
        self.assertEqual(len(rounds), 2)

    async def test_followup_uses_immediate_owner_history_but_not_revoked_scope(self):
        history = [{'question': '请读取' + URL, 'answer': '旧回答说无法读取', 'scopes': ['A']}]
        result, bridge, _ = await self.run_question('这个会议纪要链接获取不到吗？', history=history)
        self.assertIn('已读取', result['answer'])
        bridge.acall.assert_awaited_once()

    async def test_api_permission_error_is_reported_directly_without_model_retry_loop(self):
        result, bridge, rounds = await self.run_question('读取' + URL, error=AssistantError('飞书文档访问被拒绝，请核对读取权限', 403))
        self.assertIn('访问被拒绝', result['answer'])
        self.assertNotIn('诊断编号', result['answer'])
        self.assertEqual(rounds, [])
        self.assertEqual(result['sources'], [])

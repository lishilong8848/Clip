"""Offline request compaction and user-facing reply regressions."""
import copy
import datetime as dt
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_context import compact_model_messages, reply_failure
from openclaw_service.assistant.lighthouse_sources import learning_participation_request


class ReplyResilienceTests(unittest.TestCase):
    def payload(self):
        old = []
        for i in range(8):
            old += [{'role': 'user', 'content': '旧问题' + str(i)},
                    {'role': 'assistant', 'tool_calls': [{'id': 'call' + str(i)}]},
                    {'role': 'tool', 'tool_call_id': 'call' + str(i), 'content': '旧查询' * 1200},
                    {'role': 'assistant', 'content': '旧回答' * 1200}]
        return {'model': 'fixture', 'tools': [{'name': 'read'}], 'messages': [
            {'role': 'system', 'content': '原权限和约束'}, *old,
            {'role': 'user', 'content': '当前问题，时间与楼栋必须保留'},
            {'role': 'assistant', 'tool_calls': [{'id': 'latest'}]},
            {'role': 'tool', 'tool_call_id': 'latest', 'content': '已提交任务的真实结果'}]}

    def test_compaction_preserves_latest_turn_tools_policy_and_original(self):
        original = self.payload()
        before = copy.deepcopy(original)
        result = compact_model_messages(original, 5000)
        self.assertEqual(original, before)
        self.assertEqual(result['messages'][0], original['messages'][0])
        self.assertEqual(result['messages'][-3:], original['messages'][-3:])
        self.assertEqual(result['tools'], original['tools'])
        self.assertLess(len(json.dumps(result, ensure_ascii=False)), 5000)
        self.assertIn('并非完整记录或当前事实', result['messages'][1]['content'])

    def test_no_cut_of_single_current_long_input_or_native_summary(self):
        for messages in ([{'role': 'user', 'content': '本轮完整文本' * 10000}],
                         [{'role': 'system', 'content': 'You are a context summarization assistant.'}, *self.payload()['messages'][1:]]):
            payload = {'messages': messages}
            self.assertIs(compact_model_messages(payload, 1000), payload)

    def test_retained_tool_sequences_do_not_have_orphans(self):
        result = compact_model_messages(self.payload(), 20000)
        calls = set()
        for message in result['messages']:
            calls.update(call['id'] for call in message.get('tool_calls', []))
            if message.get('role') == 'tool':
                self.assertIn(message['tool_call_id'], calls)

    def test_short_invalid_and_image_inputs_unchanged(self):
        for messages in ([], None, ['invalid'], [{'role': 'user', 'content': [{'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,fixture'}}]}]):
            payload = {'messages': messages}
            self.assertIs(compact_model_messages(payload), payload)

    def test_daily_routing_and_last_condition(self):
        now = dt.datetime(2026, 10, 19, 13, tzinfo=dt.timezone(dt.timedelta(hours=8)))
        for question, day in (('今天答题的都有哪些?', '2026-10-19'), ('今天哪些人答题', '2026-10-19'),
                              ('昨天有哪些人做题', '2026-10-18'), ('前天哪些人考试', '2026-10-17'),
                              ('之前的问题：上周哪些人答题\n本次补充：今天', '2026-10-19')):
            with self.subTest(question=question):
                query = learning_participation_request(question, now=now)
                self.assertEqual((query['from'], query['to']), (day, day))

    def test_short_followup_keeps_resolved_date_not_new_today(self):
        query = {'from': '2026-10-19', 'to': '2026-10-19', 'scope': 'A'}
        self.assertEqual(learning_participation_request('有哪些?', query), query)
        self.assertIsNone(learning_participation_request('有哪些?'))
        for question in ('今天哪些人未答题', '发送今天答题人员消息', '删除这些', '今天有哪些事件'):
            self.assertIsNone(learning_participation_request(question, query))

    def test_friendly_failure_without_diagnostics_or_fake_result(self):
        text = reply_failure('未取得可靠业务依据,未执行业务,请重试或明确查询内容。（诊断编号：16cec44a）', question='今天答题的都有哪些')
        self.assertIn('未能完成回答', text)
        self.assertNotIn('答题名单', text)
        self.assertNotIn('诊断编号', text)
        self.assertNotIn('未取得可靠业务依据', text)
        for code in ('model_context', 'model_protocol', 'model_auth', 'timeout', 'query_adapter'):
            text = reply_failure('', code, '查看本周都有哪些人参与了答题')
            self.assertTrue(text)
            self.assertNotIn('答题名单', text)
            if code == 'model_context':
                self.assertIn('模型的处理上限', text)
                self.assertIn('原问题、聊天记录和已提交任务均已保留', text)
                self.assertNotIn('可把长资料作为附件', text)
                self.assertNotIn('这段对话的资料较多', text)
        for message in ('没有权限查看该楼栋', '结束通告前必须上传一张现场照片', '回答已停止，可继续或重新提问。'):
            self.assertEqual(reply_failure(message), message)


if __name__ == '__main__':
    unittest.main()

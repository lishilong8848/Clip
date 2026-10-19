"""Offline card navigation, automatic scores and delivery idempotency."""
import asyncio
import copy
import json
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from bin.test_learning import learning, FakeCloud
from lan_bitable_template_portal.learning_weekly_cards import WeeklyCards, render


class WeeklyCardsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.service = learning.LearningService(self.temp.name, FakeCloud(False), get_portal_url=lambda: 'http://fixture')
        self.profile = {'level': 'L3', 'category': 'supervisor', 'scope_rules': ['A'], 'modules': ['learning']}
        self.runtime = SimpleNamespace(auth_manager=SimpleNamespace(account_profile=lambda _: self.profile))
        self.feature = WeeklyCards(self.runtime, lambda: self.service)
        def row(pid, correct):
            return {'person_id': pid, 'name': pid, 'answered': 15, 'correct': correct, 'choices': 12,
                    'accuracy': round(correct / 12 * 100, 1), 'practice': 0}
        self.rows = [row('p1', 10), row('p2', 12), row('p3', 1)]
        self.job = {'id': 'week:fixture:group', 'kind': 'weekly_summary', 'date': '2026-10-05', 'end': '2026-10-11',
                    'scope': '', 'audience': 'group', 'items': self.rows, 'buildings': {'A': self.rows, 'B': [row('p4', 6)], 'H': []}}
        self.ticket = self.feature.prepare(self.job, 'oc_fixture', 'cli_fixture')
        self.ticket.update(message_id='om_fixture', chat_id='oc_fixture', status='sent')
        with self.service.transaction() as conn:
            self.service._put('engagement', self.ticket['id'], self.ticket, conn)

    def event(self, action='view', scope='A', **extra):
        return {'header': {'app_id': 'cli_fixture', 'event_type': 'card.action.trigger'}, 'event': {
            'context': {'open_message_id': 'om_fixture', 'open_chat_id': 'oc_fixture'},
            'action': {'value': {'kind': 'learning_weekly', 'ticket': self.ticket['id'], 'token': self.ticket['token'],
                                  'action': action, 'scope': scope, 'page': 0, **extra}}}}

    def apply(self, event):
        return self.feature.apply(event, 'cli_fixture', {'open_id': 'ou_reviewer', 'name': '审阅人'})

    def test_overview_has_building_buttons_and_no_error_rate_or_attempt_counts(self):
        card = render(self.ticket, 'http://fixture')
        text = json.dumps(card, ensure_ascii=False)
        for label in ('各楼汇总', 'A楼', 'B楼', 'H楼', '查看'):
            self.assertIn(label, text)
        for label in ('错误率', '答题次数', 'papers'):
            self.assertNotIn(label, text)
        self.assertTrue(card['config']['update_multi'])

    def test_building_switch_and_person_buttons_and_paginated_card(self):
        card = self.apply(self.event())['card']
        text = json.dumps(card, ensure_ascii=False)
        self.assertIn('"person_id": "p1"', text)
        self.assertIn('"action": "profile"', text)
        self.assertNotIn('"person_id": "p4"', text)
        self.assertNotIn('"disabled": true', text)
        self.assertIn('答题 15题', text)
        self.assertIn('答对 10题', text)
        many = copy.deepcopy(self.ticket)
        many['report']['buildings']['A'] = [{**self.rows[0], 'person_id': f'p{i}', 'name': f'人{i}'} for i in range(200)]
        for page in (0, 1, 24):
            text = json.dumps(render(many, 'http://fixture', scope='A', page=page), ensure_ascii=False)
            self.assertEqual(text.count('"action": "profile"'), 8)
            self.assertLess(len(text.encode()), 25000)

    def test_person_portrait_opens_inside_same_card_and_returns_to_its_building(self):
        self.rows[0].update(days=3, interviews=3, practice=4, topics={'供配电': 2}, ratings={'需复习': 1})
        with self.service.transaction() as conn:
            self.ticket['report']['buildings']['A'] = self.rows
            self.service._put('engagement', self.ticket['id'], self.ticket, conn)
        result = self.apply(self.event('profile', person_id='p1'))
        card = result['card']
        self.assertEqual(card['header']['title']['content'], 'p1 · 本周学练画像')
        text = json.dumps(card, ensure_ascii=False)
        for value in ('83.3', '供配电（2题）', '需复习 1题', '返回A楼汇总', '3天', '4次'):
            self.assertIn(value, text)
        self.assertNotIn('错误率', text)
        chart = next(e for e in card['body']['elements'] if e['tag'] == 'chart')
        self.assertEqual(chart['chart_spec']['data']['values'], [{'type': '正确', 'count': 10}, {'type': '错误', 'count': 2}])
        back = next(e for e in card['body']['elements'] if e.get('tag') == 'button')
        followup = self.event(); followup['event']['action']['value'] = back['value']
        self.assertEqual(self.apply(followup)['card']['header']['title']['content'], 'A楼 · 每周学练汇总')

    def test_portrait_requires_person_in_selected_building_and_keeps_private_report_scope(self):
        for scope, pid in [('A', 'p4'), ('B', 'p1'), ('', 'p1'), ('A', ''), ('A', ['p1'])]:
            with self.subTest(scope=scope, person_id=pid), self.assertRaises(learning.LearningError):
                self.apply(self.event('profile', scope=scope, person_id=pid))
        with self.service.transaction() as conn:
            self.ticket['report']['scope'] = 'A'
            self.ticket['target'] = 'ou_reviewer'
            self.service._put('engagement', self.ticket['id'], self.ticket, conn)
        self.assertTrue(self.apply(self.event('profile', person_id='p1'))['ok'])
        with self.assertRaises(learning.LearningError):
            self.apply(self.event('profile', scope='B', person_id='p4'))

    def test_portrait_without_choice_answers_does_not_show_zero_or_a_pie(self):
        ticket = copy.deepcopy(self.ticket)
        ticket['report']['buildings']['A'][0].update(choices=0, correct=0)
        card = render(ticket, 'http://fixture', scope='A', person_id='p1')
        text = json.dumps(card, ensure_ascii=False)
        self.assertIn('暂无评分', text)
        self.assertNotIn('0%', text)
        self.assertFalse(any(e.get('tag') == 'chart' for e in card['body']['elements']))

    def test_automatic_scores_ascending_zero_first_no_choice_last_and_no_writes(self):
        with self.service.transaction() as conn:
            ticket = self.service._get('engagement', self.ticket['id'], conn)
            ticket['report']['buildings']['A'].extend([
                {**self.rows[0], 'person_id': 'zero', 'name': 'zero', 'correct': 0, 'score': 100},
                {**self.rows[0], 'person_id': 'interview', 'name': 'interview', 'correct': 0, 'choices': 0},
            ])
            ticket['report']['buildings']['A'][0].update(accuracy=0, score=1)
            self.service._put('engagement', ticket['id'], ticket, conn)
        before = self.service._all('engagement')
        elements = self.apply(self.event())['card']['body']['elements']
        people = [e['columns'][0]['elements'][0]['text']['content'] for e in elements
                  if e.get('tag') == 'column_set' and e['columns'][0].get('weight') == 3]
        self.assertEqual([p.split(' · ')[0] for p in people], ['zero', 'p3', 'p1', 'p2', 'interview'])
        self.assertIn('zero · 0分', people[0])
        self.assertIn('p1 · 83.3分', people[2])
        self.assertIn('正确率 83.3%', people[2])
        self.assertIn('interview · 暂无评分', people[-1])
        self.assertEqual(self.service._all('engagement'), before)

    def test_old_manual_actions_refresh_card_without_saving_a_grade(self):
        before = self.service._all('engagement')
        for action in ('edit', 'score'):
            event = self.event(action, person_id='p1', revision=0)
            event['event']['action']['form_value'] = {'score': '100'}
            result = self.apply(event)
            self.assertIn('自动评分', result['toast'])
            text = json.dumps(result['card'], ensure_ascii=False)
            for removed in ('"tag": "form"', '"action": "edit"', '"action": "score"', '保存评分'):
                self.assertNotIn(removed, text)
            self.assertIn('p1 · 83.3分', text)
            self.assertIn('个人画像', text)
        self.assertEqual(self.service._all('engagement'), before)

    def test_unknown_scope_or_action_and_missing_module_denied(self):
        with self.assertRaises(learning.LearningError): self.apply(self.event(scope='Z'))
        with self.assertRaises(learning.LearningError): self.apply(self.event('delete'))
        self.profile.update(level='L4', category='operator')
        self.assertTrue(self.apply(self.event())['ok'])
        self.profile['modules'] = []
        with self.assertRaises(learning.LearningError): self.apply(self.event())

    def test_callback_message_app_and_token_binding(self):
        for field, value in [('open_message_id', 'om_other'), ('open_chat_id', 'oc_other')]:
            event = self.event(); event['event']['context'][field] = value
            with self.assertRaises(learning.LearningError): self.apply(event)
        event = self.event(); event['event']['action']['value']['token'] = 'forged'
        with self.assertRaises(learning.LearningError): self.apply(event)
        event = self.event(); event['header']['app_id'] = 'other'
        with self.assertRaises(learning.LearningError): self.apply(event)
        with self.service.transaction() as conn:
            self.ticket['target'] = 'ou_someone_else'
            self.service._put('engagement', self.ticket['id'], self.ticket, conn)
        with self.assertRaises(learning.LearningError): self.apply(self.event())

    def test_private_building_report_cannot_expand_to_all(self):
        with self.service.transaction() as conn:
            self.ticket['report']['scope'] = 'A'
            self.service._put('engagement', self.ticket['id'], self.ticket, conn)
        for scope in ('B', ''):
            with self.assertRaises(learning.LearningError): self.apply(self.event(scope=scope))

    def test_private_card_rechecks_supervisor_role_and_scope_on_every_click(self):
        with self.service.transaction() as conn:
            self.ticket['report']['scope'] = 'A'
            self.ticket['target'] = 'ou_reviewer'
            self.service._put('engagement', self.ticket['id'], self.ticket, conn)
        for changed in ({'category': 'engineer'}, {'scope_rules': ['B']}, {'modules': []}):
            previous = copy.deepcopy(self.profile)
            self.profile.update(changed)
            with self.assertRaises(learning.LearningError): self.apply(self.event('profile', person_id='p1'))
            self.profile = previous

    def test_bridge_routes_switch_and_portrait_and_sdk_returns_raw_replacement_card(self):
        from starlette.requests import Request
        from lan_bitable_template_portal.feishu_assistant import FeishuAssistant
        from lark_oapi.event.callback.model.p2_card_action_trigger import CallBackCard, P2CardActionTriggerResponse
        import lark_oapi as lark
        self.runtime.state_store = SimpleNamespace(db_path=None)
        channel = FeishuAssistant(SimpleNamespace(), self.runtime, AsyncMock())
        channel.inbox, channel.secret = object(), 'weekly-fixture-secret'
        channel.config, channel.messenger = {'app_id': 'cli_fixture'}, object()
        channel._resolve_user = AsyncMock(return_value={'open_id': 'ou_reviewer'})
        self.runtime.feishu_assistant_channel = channel
        self.runtime.learning_weekly_cards = self.feature
        async def invoke(payload):
            async def receive():
                return {'type': 'http.request', 'body': json.dumps(payload).encode(), 'more_body': False}
            request = Request({'type': 'http', 'method': 'POST', 'path': '/api/assistant/feishu-event',
                'client': ('127.0.0.1', 12345), 'headers': [(b'authorization', b'Bearer weekly-fixture-secret')]}, receive)
            response = await channel.receive(request)
            self.assertEqual(response.status_code, 200)
            return json.loads(response.body)
        for action, extra, title in [('view', {}, 'A楼 · 每周学练汇总'),
                                     ('profile', {'person_id': 'p1'}, 'p1 · 本周学练画像')]:
            result = asyncio.run(invoke(self.event(action, **extra)))
            self.assertTrue(result['ok'])
            response = P2CardActionTriggerResponse()
            response.card = CallBackCard({'type': 'raw', 'data': result['card']})
            wire = json.loads(lark.JSON.marshal(response))
            self.assertEqual(wire['card']['type'], 'raw')
            self.assertEqual(wire['card']['data']['header']['title']['content'], title)

    def test_delivery_reuses_message_and_original_app(self):
        bot = SimpleNamespace(config={'app_id': 'cli_fixture'}, messenger=SimpleNamespace(client=AsyncMock()))
        self.runtime.feishu_assistant_channel = bot
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            self.assertTrue(asyncio.run(self.feature.send(self.job, 'oc_fixture', 'fixture')))
        bot.messenger.client.assert_not_awaited()
        with self.assertRaises(learning.LearningError): self.feature.prepare(self.job, 'oc_fixture', 'cli_changed')

    def test_initial_delivery_persists_receipt_and_retry_does_not_resend(self):
        client = SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(status_code=200,
            json=lambda: {'code': 0, 'data': {'message_id': 'om_new', 'chat_id': 'oc_new'}})))
        bot = SimpleNamespace(config={'app_id': 'cli_fixture'}, messenger=SimpleNamespace(
            client=AsyncMock(return_value=client), headers=AsyncMock(return_value={})))
        self.runtime.feishu_assistant_channel = bot
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            self.assertTrue(asyncio.run(self.feature.send(self.job, 'oc_new', 'weekly-fixture')))
            self.assertTrue(asyncio.run(self.feature.send(self.job, 'oc_new', 'weekly-fixture')))
        client.post.assert_awaited_once()
        sent = client.post.call_args.kwargs
        self.assertEqual(sent['params'], {'receive_id_type': 'chat_id'})
        self.assertEqual(sent['json']['receive_id'], 'oc_new')
        self.assertEqual(json.loads(sent['json']['content'])['schema'], '2.0')
        ticket = self.feature.prepare(self.job, 'oc_new', 'cli_fixture')
        self.assertEqual((ticket['message_id'], ticket['chat_id']), ('om_new', 'oc_new'))

    def test_group_rejection_includes_code_and_can_retry_after_permission_is_fixed(self):
        client = SimpleNamespace(post=AsyncMock(side_effect=[
            SimpleNamespace(status_code=400, json=lambda: {'code': 230002, 'msg': 'not in group'}),
            SimpleNamespace(status_code=200, json=lambda: {'code': 0, 'data': {'message_id': 'om_recovered', 'chat_id': 'oc_recovery'}})]))
        self.runtime.feishu_assistant_channel = SimpleNamespace(config={'app_id': 'cli_fixture'}, messenger=SimpleNamespace(
            client=AsyncMock(return_value=client), headers=AsyncMock(return_value={})))
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            with self.assertRaisesRegex(learning.LearningError, '230002'):
                asyncio.run(self.feature.send(self.job, 'oc_recovery', 'same-operation'))
            self.assertNotIn('attempted_at', self.feature.prepare(self.job, 'oc_recovery', 'cli_fixture'))
            self.assertTrue(asyncio.run(self.feature.send(self.job, 'oc_recovery', 'same-operation')))
        self.assertEqual(client.post.call_args_list[0].kwargs['json']['uuid'], client.post.call_args_list[1].kwargs['json']['uuid'])

    def test_personal_report_uses_agent_recipient_mapping_not_old_app_contact(self):
        from upload_event_module.services.robot_webhook import DeliveryAttempt
        client = SimpleNamespace(post=AsyncMock(return_value=SimpleNamespace(status_code=200,
            json=lambda: {'code': 0, 'data': {'message_id': 'om_personal', 'chat_id': 'oc_personal'}})))
        bot = SimpleNamespace(config={'app_id': 'cli_fixture'}, messenger=SimpleNamespace(
            client=AsyncMock(return_value=client), headers=AsyncMock(return_value={})),
            permission_cards=SimpleNamespace(union_id=Mock(side_effect=AssertionError('old contact forbidden'))))
        resolver = SimpleNamespace(closing=False, recipient=AsyncMock(return_value=('open_id', 'ou_agentPerson')))
        self.runtime.feishu_assistant_channel = bot
        self.runtime.feishu_public_sender = resolver
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            self.assertTrue(asyncio.run(self.feature.send(self.job, 'ou_portalPerson', 'person-weekly')))
            resolver.recipient.return_value = DeliveryAttempt('blocked', '人员已停用')
            with self.assertRaisesRegex(learning.LearningError, '人员已停用'):
                asyncio.run(self.feature.send(self.job, 'ou_blockedPerson', 'person-weekly'))
        client.post.assert_awaited_once()
        self.assertEqual(client.post.call_args.kwargs['params'], {'receive_id_type': 'open_id'})
        self.assertEqual(client.post.call_args.kwargs['json']['receive_id'], 'ou_agentPerson')
        self.assertNotIn('attempted_at', self.feature.prepare(self.job, 'ou_blockedPerson', 'cli_fixture'))

    def test_ambiguous_group_send_does_not_retry_beyond_dedup_window_or_use_old_app(self):
        client = SimpleNamespace(post=AsyncMock(side_effect=OSError('response lost')))
        self.runtime.feishu_assistant_channel = SimpleNamespace(config={'app_id': 'cli_fixture'}, messenger=SimpleNamespace(
            client=AsyncMock(return_value=client), headers=AsyncMock(return_value={})))
        with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
            with self.assertRaises(OSError):
                asyncio.run(self.feature.send(self.job, 'oc_uncertain', 'same-operation'))
            ticket = self.feature.prepare(self.job, 'oc_uncertain', 'cli_fixture')
            with patch('lan_bitable_template_portal.learning_weekly_cards.time.time', return_value=ticket['attempted_at'] + 3600):
                with self.assertRaisesRegex(learning.LearningError, '去重窗口'):
                    asyncio.run(self.feature.send(self.job, 'oc_uncertain', 'same-operation'))
        client.post.assert_awaited_once()

    def test_sync_sender_not_initialized_reports_actionable_failure(self):
        with self.assertRaisesRegex(learning.LearningError, '尚未就绪'):
            self.feature.send_from_worker(self.job, 'oc_fixture', 'fixture')

    def test_unsent_ticket_can_change_to_agent_app_but_uncertain_ticket_cannot(self):
        target = 'oc_migrating'
        ticket = self.feature.prepare(self.job, target, 'cli_old')
        migrated = self.feature.prepare(self.job, target, 'cli_agent')
        self.assertEqual(migrated['app_id'], 'cli_agent')
        self.assertEqual(migrated['id'], ticket['id'])
        with self.service.transaction() as conn:
            migrated['attempted_at'] = 100
            self.service._put('engagement', migrated['id'], migrated, conn)
        with self.assertRaises(learning.LearningError):
            self.feature.prepare(self.job, target, 'cli_another')

    def test_timeout_and_wrong_group_receipts_remain_uncertain(self):
        responses = [
            SimpleNamespace(status_code=408, json=lambda: {'code': 230000}),
            SimpleNamespace(status_code=200, json=lambda: {'code': 0,
                'data': {'message_id': 'om_other', 'chat_id': 'oc_other'}}),
        ]
        for index, response in enumerate(responses):
            with self.subTest(status=response.status_code):
                target = 'oc_unknown' + str(index)
                client = SimpleNamespace(post=AsyncMock(return_value=response))
                self.runtime.feishu_assistant_channel = SimpleNamespace(config={'app_id': 'cli_fixture'},
                    messenger=SimpleNamespace(client=AsyncMock(return_value=client), headers=AsyncMock(return_value={})))
                with patch('lan_bitable_template_portal.portal_service.external_real_write_guard', return_value={'real_write_allowed': True}):
                    with self.assertRaises(learning.LearningError):
                        asyncio.run(self.feature.send(self.job, target, 'same-operation'))
                ticket = self.feature.prepare(self.job, target, 'cli_fixture')
                self.assertIn('attempted_at', ticket)
                self.assertFalse(ticket['message_id'])

    def test_callback_returns_same_message_card_after_resolving_actor(self):
        bot = SimpleNamespace(config={'app_id': 'cli_fixture'}, messenger=object(),
            _resolve_user=AsyncMock(return_value={'open_id': 'ou_reviewer'}))
        self.runtime.feishu_assistant_channel = bot
        payload = self.event()
        payload['event']['operator'] = {'open_id': 'ou_bot_app', 'union_id': 'on_person'}
        result = asyncio.run(self.feature.callback(payload, 'cli_fixture'))
        self.assertTrue(result['ok'])
        self.assertIn('card', result)
        bot._resolve_user.assert_awaited_once_with({'sender_id': 'ou_bot_app', 'union_id': 'on_person'})


if __name__ == '__main__':
    unittest.main()

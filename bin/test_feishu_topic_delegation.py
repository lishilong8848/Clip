"""Topic grants, native ASGI guards, owner isolation and recovery; no network."""
import asyncio
import copy
import hashlib
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from bin.test_lighthouse_stream import Engine
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import httpx
from lan_bitable_template_portal.portal_auth import (
    PortalAuthManager, TopicDelegations, TOPIC_EXECUTION, topic_capability,
)
from lan_bitable_template_portal.portal_service import BUILDING_OPEN_ID_MAP, PortalError
from upload_event_module.services.feishu_app_migration import TARGET_APP_ID
from lan_bitable_template_portal.module_access import allowed_modules, require_module
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal.lighthouse_bridge import PortalAuthority
from lan_bitable_template_portal.feishu_assistant import FeishuAssistant, Inbox
from openclaw_service.assistant.lighthouse_ai import AssistantError, LighthouseAssistant
from openclaw_service.assistant.lighthouse_agent import PortalAgent, PLAN_NAMESPACE
from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
from openclaw_service.assistant.lighthouse_files import LighthouseFiles
from openclaw_service.assistant.lighthouse_stream import LighthouseStream
from openclaw_service.assistant.lighthouse_sources import work_order_records


class Auth(PortalAuthManager):
    def __init__(self):
        self._lock = threading.RLock()
        self.disabled, self.inactive = set(), set()
        self.explicit = {}
        self.profiles = {}
        self.add('ou_Low', ['D'], ['assistant', 'event'])
        self.add('ou_Maint', ['A'], ['assistant', 'maintenance'])
        self.add('ou_Repair', ['B'], ['assistant', 'repair'])

    def add(self, identity, scopes, modules, **values):
        self.profiles[identity] = dict(id=identity, scope_rules=scopes, modules=modules, level='L4',
            category='engineer', home_scope=scopes[0], role='building', can_delete_business=False, **values)

    def account_profile(self, identity):
        result = copy.deepcopy(self.profiles.get(identity, {'scope_rules': [], 'modules': [], 'role': 'building'}))
        if identity in self.disabled or identity in self.inactive:
            result['modules'] = []
        return result

    def _permission_user(self, identity):
        return self.explicit.get(identity, {})

    def directory_person(self, identity):
        return {'id': 'rec' + str(identity), 'selectable': identity not in self.inactive,
                'inactive': identity in self.inactive, 'name': 'Same display name',
                'login_ids': [identity]} if identity in self.profiles else {}

    def _open_id_explicitly_disabled(self, identity):
        return (identity in self.disabled or identity in self.inactive
                or self._permission_user(identity).get('enabled') is False)

    def scopes_for_open_id(self, identity):
        return [] if self._open_id_explicitly_disabled(identity) else self.account_profile(identity)['scope_rules']


class TopicDelegationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = LanPortalStateStore(self.root / 'state.sqlite3')
        self.addCleanup(self.store.shutdown_write_worker)
        self.settings = SimpleNamespace(app_id=TARGET_APP_ID, app_secret='fixture-current-secret')
        self.enterContext(patch('lan_bitable_template_portal.portal_auth.config', self.settings))
        self.auth = Auth()
        self.registry = self.auth._topic_delegations = TopicDelegations(self.auth, self.store)
        self.topic = self.topic_id()
        self.count = 0
        for identity in self.auth.profiles:
            self.register(identity)
        self.app = FastAPI()

        @self.app.post('/api/workbench-actions')
        async def submit(request: Request):
            payload = await request.json()
            session = {'user': {'open_id': request.cookies.get('actor', 'ou_Low')}}
            try:
                require_module(self.auth, session, payload.get('work_type', 'maintenance'))
                if not self.auth.scope_allowed(session, payload.get('scope', 'A')):
                    raise PortalError('scope rejected')
            except PortalError:
                return JSONResponse({'ok': False, 'error': 'denied'}, status_code=403)
            self.count += 1
            return {'ok': True, 'data': {'actor': session['user']['open_id'], 'count': self.count}}

        self.catalog = PortalAPICatalog(self.app)
        self.runtime = SimpleNamespace(auth_manager=self.auth, state_store=self.store, service=SimpleNamespace())
        self.controller = SimpleNamespace(_current_session=lambda r: {'user': {'open_id': r.cookies.get('actor', 'ou_Low')}})
        self.authority = PortalAuthority(self.app, self.controller, self.runtime,
            SimpleNamespace(root=self.root, lease='lease', instance='instance'))
        self.authority.catalog = self.catalog

    @staticmethod
    def topic_id(app=TARGET_APP_ID, chat='oc_Group', root='om_Root'):
        return chat + ':' + root + ':app:' + hashlib.sha256(app.encode()).hexdigest()[:16]

    def actor(self, identity='ou_Low', topic=None):
        p = self.auth.account_profile(identity)
        return {'id': identity, 'scopes': self.auth.scopes_for_open_id(identity), 'modules': p['modules'],
                'is_admin': False, 'learning_scopes': [], 'channel': 'feishu:oc_Group', '_feishu_topic': {'id': topic or self.topic, 'context': ''}}

    def register(self, identity, topic=None, **route):
        values = {'app_id': TARGET_APP_ID, 'chat_id': 'oc_Group', 'root_id': 'om_Root', 'chat_type': 'group',
                  'message_id': 'om_' + identity.replace('_', ''), 'sender_id': 'ou_Agent' + identity.replace('_', '')}
        values.update(route)
        return self.registry.register(topic or self.topic, self.actor(identity, topic), values)

    def operation(self, scope='A', work_type='maintenance', **body):
        value, missing = self.catalog.validate_operation({'api_id': 'POST /api/workbench-actions',
            'body': {'command_format': 'notice_command', 'action': 'start', 'source_record_id': 'recSource',
                     'scope': scope, 'work_type': work_type, 'patch': {}, **body}})
        self.assertEqual(missing, [])
        return value

    def execution(self, operation=None, identity='ou_Low'):
        return self.registry.issue(self.topic, identity, topic_capability(operation or self.operation()))

    def request(self, identity='ou_Low', execution=None):
        state = {'feishu_topic': {'id': self.topic, 'context': ''}, 'lighthouse_channel': 'feishu:oc_Group'}
        if execution:
            state['lighthouse_topic_execution'] = execution
        return Request({'type': 'http', 'method': 'POST', 'scheme': 'http', 'path': '/api/assistant/chat',
            'query_string': b'', 'root_path': '', 'server': ('fixture', 80), 'client': ('127.0.0.1', 1),
            'headers': [(b'host', b'fixture'), (b'origin', b'http://fixture'), (b'cookie', ('actor=' + identity).encode())], 'state': state})

    async def test_complete_capability_not_cartesian_product(self):
        execution = self.execution()
        self.assertEqual(execution.validate()['id'], 'ou_Maint')
        for scope, module in [('B', 'maintenance'), ('A', 'repair')]:
            with self.assertRaises(PortalError):
                self.execution(self.operation(scope, module))
        self.assertFalse(self.auth.is_admin({'user': {'open_id': 'ou_Low'}}))

    async def test_operator_must_have_real_evidence_even_if_an_admin(self):
        self.auth.add('ou_Unknown', ['A'], ['assistant', 'maintenance'])
        with self.assertRaises(PortalError):
            self.execution(identity='ou_Unknown')
        execution = self.execution()
        record = self.store.get_document('lighthouse_topics', self.registry.key(self.topic))
        record['people'].pop('ou_Low')
        self.store.put_document('lighthouse_topics', self.registry.key(self.topic), record)
        with self.assertRaises(PortalError):
            self.registry.resume(execution.grant['id'], 'ou_Low')

    async def test_app_group_root_evidence_cannot_be_reused(self):
        for topic in [self.topic_id(app='cli_other'), self.topic_id(chat='oc_Other'), self.topic_id(root='om_Other')]:
            with self.assertRaises(PortalError):
                self.registry.issue(topic, 'ou_Low', topic_capability(self.operation()))

    async def test_revoked_module_scope_or_employment_rechecked_on_resume(self):
        execution = self.execution()
        before = copy.deepcopy(self.auth.profiles['ou_Maint'])
        for change in ('module', 'scope', 'employment', 'disabled'):
            self.auth.profiles['ou_Maint'] = copy.deepcopy(before)
            self.auth.disabled.clear(); self.auth.inactive.clear()
            if change == 'module': self.auth.profiles['ou_Maint']['modules'] = ['assistant']
            if change == 'scope': self.auth.profiles['ou_Maint']['scope_rules'] = ['B']
            if change == 'employment': self.auth.inactive.add('ou_Maint')
            if change == 'disabled': self.auth.disabled.add('ou_Maint')
            with self.assertRaises(PortalError):
                self.registry.resume(execution.grant['id'], 'ou_Low')

    async def test_native_asgi_state_grants_scope_but_keeps_operator(self):
        operation = self.operation()
        result = await self.catalog.invoke(operation, self.request(execution=self.execution(operation)))
        self.assertTrue(result['ok'], result)
        self.assertEqual(result['_raw']['actor'], 'ou_Low')
        self.assertIsNone(TOPIC_EXECUTION.get())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://fixture') as client:
            response = await client.post('/api/workbench-actions', json=operation['body'],
                headers={'x-topic-delegation': self.execution().grant['id'], 'cookie': 'actor=ou_Low'})
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.count, 1)

    async def test_same_path_different_body_params_or_files_rejected(self):
        operation = self.operation()
        request = self.request(execution=self.execution(operation))
        for section, values in [('body', {'scope': 'B', 'work_type': 'maintenance'}),
                                ('body', {**operation['body'], 'active_item_id': 'recOther'})]:
            altered = {**operation, section: values}
            with self.assertRaises(AssistantError):
                await self.catalog.invoke(altered, request)
        altered = copy.deepcopy(operation)
        altered['params']['scope'] = 'B'
        self.assertFalse(request.state.lighthouse_topic_execution.matches_operation(altered))
        altered = copy.deepcopy(operation); altered['files']['file'] = ['f' * 32]
        self.assertFalse(request.state.lighthouse_topic_execution.matches_operation(altered))
        self.assertEqual(self.count, 0)

    async def test_different_confirmers_same_operation_execute_once(self):
        operation = self.operation()
        requests = [self.request(identity) for identity in ['ou_Low', 'ou_Repair']]
        contexts = [self.authority.context(request, self.actor(identity), plan_id='plan-one')
                    for request, identity in zip(requests, ['ou_Low', 'ou_Repair'])]
        async def invoke(context_id):
            context, actor = await self.authority.authorize(context_id)
            return await self.authority.invoke(context, actor, {'operation': operation, 'operation_id': 'plan:plan-one:0'})
        results = await asyncio.gather(*(invoke(context_id) for context_id in contexts))
        self.assertTrue(all(result['ok'] for result in results), results)
        self.assertEqual(self.count, 1)
        audits = self.store.list_documents('lighthouse_topic_audit')
        self.assertEqual({row['payload']['operator'] for row in audits}, {'ou_Low', 'ou_Repair'})
        self.assertEqual({row['payload']['authorized_by'] for row in audits}, {'ou_Maint'})

    async def test_learning_only_building_summary_not_personal_tasks(self):
        profile = {'api_id': 'GET /api/learning/profile', 'method': 'GET', 'path': '/api/learning/profile',
                   'params': {'scope': 'A', 'from': '2026-10-05', 'to': '2026-10-11'}, 'body': {}, 'files': {}, 'read_only': True}
        self.auth.profiles['ou_Maint'].update(category='duty', home_scope='A', modules=['assistant', 'learning'])
        self.assertEqual(self.execution(profile).validate()['id'], 'ou_Maint')
        with self.assertRaises(PortalError):
            topic_capability({**profile, 'params': {**profile['params'], 'person_id': 'other'}})
        with self.assertRaises(PortalError):
            topic_capability({**profile, 'path': '/api/learning/papers/claim', 'method': 'POST', 'read_only': False})

    async def test_work_order_sources_filter_complete_module_and_scope(self):
        self.auth.profiles['ou_Repair']['modules'] = ['assistant', 'polling']
        for key, scope, module in [('allowed-a', 'A', 'maintenance'), ('allowed-b', 'B', 'polling'),
                                   ('forbidden-b', 'B', 'maintenance'), ('forbidden-a', 'A', 'polling')]:
            self.store.put_document('polling_work_order', key, {'scope': scope, 'work_type': module,
                'state': 'active', 'title': key, 'steps': []})
        actor = self.registry.enrich(self.actor(), self.topic)
        result = work_order_records(self.store, actor, {'scope': 'ALL'})
        self.assertEqual({row['title'] for row in result['items']}, {'allowed-a', 'allowed-b'})
        with self.assertRaises(AssistantError):
            work_order_records(self.store, actor, {'scope': 'B', 'group_id': 'forbidden-b'})

    async def test_authenticated_topic_can_read_all_shared_orders(self):
        self.auth.profiles['ou_Repair']['modules'] = ['assistant', 'polling']
        for key, scope, module in [('allowed-a', 'A', 'maintenance'), ('allowed-b', 'B', 'polling'),
                                   ('forbidden-b', 'B', 'maintenance'), ('forbidden-a', 'A', 'polling')]:
            self.store.put_document('polling_work_order', key, {'scope': scope, 'work_type': module,
                'state': 'active', 'title': key, 'steps': []})
        context = self.authority.context(self.request(), self.actor())
        result = await self.authority.dispatch({'instance': 'instance', 'lease': 'lease', 'context_id': context,
            'action': 'cached', 'payload': {'kind': 'orders', 'scopes': ['A', 'B']}})
        self.assertEqual({row['title'] for row in result[0]}, {'allowed-a', 'allowed-b', 'forbidden-a', 'forbidden-b'})

    async def test_owner_files_are_bidirectionally_separate_from_topics(self):
        files = LighthouseFiles(self.store, self.root / 'files')
        web = {'id': 'ou_Low', 'scopes': ['D']}
        topic = self.actor()
        private = files.upload(web, 'private.txt', b'PRIVATE', extract=False)
        public = files.upload(topic, 'topic.txt', b'TOPIC', extract=False)
        self.assertEqual(files.text(web, private['id'])['text'], 'PRIVATE')
        self.assertEqual(files.text(topic, public['id'])['text'], 'TOPIC')
        for actor, identity in [(web, public['id']), (topic, private['id']),
                                (self.actor(topic=self.topic_id(root='om_Other')), public['id']),
                                ({'id': 'ou_Maint', 'scopes': ['A', 'D'], 'is_admin': True}, private['id'])]:
            with self.assertRaises(AssistantError):
                files.get(actor, identity)

    async def test_topic_reply_is_saved_once_without_personal_or_web_mirroring(self):
        model = Mock(); model.settings.return_value = {'models': [], 'enabled': True}
        assistant = LighthouseAssistant(self.store, Mock(), model=model)
        actor = self.actor()
        state = assistant._state(actor)
        payload = {'operation_id': 'feishu_' + 'a' * 40, 'conversation_id': state['id'],
                   'question': 'my public question', 'answer': 'my public answer'}
        assistant.record_channel_reply(actor, payload)
        private = {**actor}; private.pop('_feishu_topic')
        web = {**private}; web.pop('channel')
        personal = assistant._state(private)
        self.assertEqual(personal['turns'], [])
        self.assertEqual(assistant._state(web)['turns'], [])
        self.assertEqual(assistant._state(actor)['turns'][0]['answer'], payload['answer'])
        assistant.record_channel_reply(actor, payload)
        self.assertEqual(len(assistant._state(actor)['turns']), 1)
        other = {**web, 'id': 'ou_Maint'}
        self.assertEqual(assistant._state(other)['turns'], [])
        messages = assistant._messages(personal['turns'], {}, [], 'private followup')
        self.assertNotIn('my public answer', str(messages))

    async def test_display_name_is_not_authentication_but_hr_mapping_needs_no_login(self):
        bot = FeishuAssistant(SimpleNamespace(), self.runtime, AsyncMock())
        bot.config = {'app_id': TARGET_APP_ID}
        message = {'sender_id': 'ou_NewExternal', 'union_id': '', 'chat_id': 'oc_Group'}
        with self.assertRaises(AssistantError):
            bot._sender_user(message, name='Same display name')
        with self.assertRaises(AssistantError):
            bot._sender_user({**message, 'sender_id': 'ou_SecondExternal'}, name='Same display name')
        person = {**self.auth.directory_person('ou_Maint'), 'login_ids': [message['sender_id']]}
        provider = {'schema': 4, 'app_id': TARGET_APP_ID, 'loaded_at': __import__('time').time(),
                    'people': {person['id']: person}}
        self.auth._account_directory_provider = lambda: provider
        self.auth.profiles[message['sender_id']] = {**self.auth.profiles.pop('ou_Maint'), 'id': message['sender_id']}
        self.auth.directory_person = PortalAuthManager.directory_person.__get__(self.auth)
        user = bot._sender_user(message, record_id='recou_Maint')
        self.assertEqual(user['open_id'], message['sender_id'])
        self.assertEqual(bot._sender_user(message)['open_id'], message['sender_id'])
        self.assertEqual(self.auth.scopes_for_open_id(user['open_id']), ['A'])
        self.assertIn('maintenance', self.auth.account_profile(user['open_id'])['modules'])
        self.assertIsNone(bot._sender_user({'sender_id': 'ou_Maint'}))
        with self.assertRaises(AssistantError):
            bot._sender_user({**message, 'sender_id': 'ou_SecondExternal'}, name=person['name'])
        with self.assertRaises(AssistantError):
            bot._sender_user({**message, 'sender_id': 'ou_SecondExternal'}, record_id=person['id'])
        self.assertFalse(self.store.get_document('feishu_assistant_sender', TARGET_APP_ID + ':ou_NewExternal'))

    async def test_old_name_binding_and_old_app_network_cannot_authenticate(self):
        old_service = SimpleNamespace(_request_payload=Mock(side_effect=AssertionError('old app network')),
                                      _auth_headers=Mock(side_effect=AssertionError('old app token')))
        self.runtime.service = old_service
        bot = FeishuAssistant(SimpleNamespace(), self.runtime, AsyncMock())
        bot.config = {'app_id': TARGET_APP_ID}
        self.store.put_document('feishu_assistant_sender', TARGET_APP_ID + ':ou_Impersonator',
            {'open_id': 'ou_Maint', 'record_id': 'recou_Maint', 'matched_by': 'name'})
        self.assertIsNone(bot._sender_user({'sender_id': 'ou_Impersonator'}))
        with self.assertRaises(AssistantError):
            bot._user('on_NotLocallyBound')
        self.settings.app_id = 'cli_nextFixture'
        with self.assertRaises(AssistantError):
            bot._sender_user({'sender_id': 'ou_Maint'})
        old_service._request_payload.assert_not_called()
        old_service._auth_headers.assert_not_called()

    async def test_two_form_confirmers_share_original_plan_idempotency(self):
        model = Mock(); model.settings.return_value = {'models': [], 'enabled': True}
        assistant = LighthouseAssistant(self.store, Mock(), model=model)
        portal = PortalAgent(assistant, self.catalog, LighthouseFiles(self.store))
        actor = self.registry.enrich(self.actor(), self.topic)
        state = assistant._state(actor)
        plan = {'id': 'original-plan', 'owner': actor['id'], 'topic_id': self.topic, 'scopes': ['A'],
                'conversation_id': state['id'], 'turn_id': 'turn', 'operations': [self.operation()],
                'status': 'awaiting_confirmation', 'version': 1, 'risk': 'low', 'fields': [], 'results': []}
        state['turns'].append({'operation_id': 'turn', 'at': 1, 'question': 'q', 'answer': 'a', 'status': 'completed', 'plan': {'id': plan['id']}})
        self.store.put_document('lighthouse_ai', assistant._key(actor), state)
        self.store.put_document(PLAN_NAMESPACE, plan['id'], plan)
        shared = self.registry.enrich(self.actor('ou_Repair'), self.topic)
        shared['_shared_plan'] = {'id': plan['id'], 'owner': actor['id'], 'conversation_id': state['id'],
                                  'chat_id': 'oc_Group', 'topic_id': self.topic}
        first = portal._confirm_commit(actor, plan['id'], {'version': 1, 'stage': 'review'})
        second = portal._confirm_commit(shared, plan['id'], {'version': 1, 'stage': 'review'})
        self.assertEqual(first[0], 'schedule')
        self.assertEqual(second[0], 'idempotent')
        saved = self.store.get_document(PLAN_NAMESPACE, plan['id'])
        self.assertEqual(saved['confirmed_by']['id'], 'ou_Low')
        self.assertEqual(saved['delegation_audit'][0]['authorized_by'], 'ou_Maint')
        self.auth.profiles['ou_Maint']['modules'] = ['assistant']
        with self.assertRaises(AssistantError):
            portal.get_plan(self.registry.enrich(self.actor(), self.topic), plan['id'])

    async def test_queue_revocation_preserves_partial_facts_and_restart_never_replays(self):
        from lan_bitable_template_portal.server import PortalRuntime
        execution = self.execution()
        self.auth.disabled.add('ou_Maint')
        def mark_job(identity, **changes):
            job = self.store.get_document('fixture_jobs', identity)
            job.update(changes)
            self.store.put_document('fixture_jobs', identity, job)
        service = SimpleNamespace(get_job=lambda identity: self.store.get_document('fixture_jobs', identity), mark_job=mark_job)
        for written, sent in [(False, False), (True, False), (True, True)]:
            identity = 'original-%s-%s' % (written, sent)
            job = {'job_id': identity, 'phase': 'uploading', 'remote_written': written, 'message_sent': sent,
                   'request': {**self.operation()['body'], '_topic_delegation_id': execution.grant['id'], '_auth_open_id': 'ou_Low'}}
            self.store.put_document('fixture_jobs', identity, job)
            self.store.upsert_runtime_queue_item('qt_action', identity)
            with patch.multiple(PortalRuntime, service=service, state_store=self.store, auth_manager=self.auth), \
                 patch.object(PortalRuntime, '_hold_for_active_event', side_effect=AssertionError('must not execute')), \
                 patch.object(PortalRuntime, '_get_notice_remote_operation', return_value={'status': 'remote_written'} if written else {}):
                PortalRuntime._process_maintenance_action_job(identity)
                self.auth._topic_delegations = TopicDelegations(self.auth, self.store)
                PortalRuntime._process_maintenance_action_job(identity)
            result = service.get_job(identity)
            self.assertEqual(result['job_id'], identity)
            self.assertEqual(result['phase'], 'cancelled')
            self.assertEqual(result['remote_written'], written)
            self.assertEqual(result['message_sent'], sent)
            self.assertEqual(result['partial'], written or sent)
            self.assertFalse(result['error_retryable'])
            self.assertIsNone(TOPIC_EXECUTION.get())

    async def test_long_topic_context_is_bounded_and_filters_before_compaction(self):
        inbox = Inbox(self.root / 'inbox.sqlite3')
        for index in range(40):
            access = {'scopes': ['D'], 'modules': ['event'], 'is_admin': False}
            if index == 2:
                access['source_owner'] = 'ou_Other'
            message = {'message_id': 'om_post%d' % index, 'chat_id': 'oc_Group', 'chat_type': 'group',
                       'root_id': 'om_Root', 'app_id': TARGET_APP_ID, 'question': 'question-%d' % index}
            inbox.remember_public_reply(message, {'text': ('HIDDEN' if index == 2 else 'public-%d' % index) + 'x' * 1000,
                'delivery_access': access, 'thread_route': {'chat_id': 'oc_Group', 'root_id': 'om_Root'}})
        context = inbox.topic_context(message, self.actor())
        self.assertIn('question-0', context)
        self.assertIn('question-39', context)
        self.assertNotIn('HIDDEN', context)
        self.assertLessEqual(len(context), 12000)

    async def test_same_topic_shares_delivered_public_replies_across_people_and_restart(self):
        bot = FeishuAssistant(SimpleNamespace(), self.runtime, AsyncMock())
        bot.config = {'app_id': TARGET_APP_ID}
        bot.inbox = Inbox(self.root / 'inbox.sqlite3')
        message = {'message_id': 'om_Public', 'root_id': 'om_Root', 'chat_id': 'oc_Group',
                   'chat_type': 'group', 'app_id': TARGET_APP_ID, 'sender_id': 'ou_Low',
                   'question': '本话题已核实多少项？'}
        bot.inbox.remember_public_reply(message, {'text': '已核实三项公开结果',
            'delivery_access': {'capabilities': [], 'scopes': [], 'modules': []},
            'thread_route': {'chat_id': 'oc_Group', 'root_id': 'om_Root'}})
        bot.inbox = Inbox(self.root / 'inbox.sqlite3')
        contexts = []
        for identity in ('ou_Low', 'ou_Maint', 'ou_Repair'):
            actor = self.actor(identity)
            await bot._prepare_topic({**message, 'sender_id': identity}, self.request(identity), actor)
            contexts.append(actor['_feishu_topic']['context'])
            self.assertIn('本话题已核实多少项', contexts[-1])
            self.assertIn('已核实三项公开结果', contexts[-1])
        self.assertEqual(len(set(contexts)), 1)
        for changed in ({'root_id': 'om_Other'}, {'chat_id': 'oc_Other'}, {'app_id': 'cli_other'}):
            self.assertEqual(bot.inbox.topic_context({**message, **changed}, self.actor()), '')
        actor = self.actor()
        actor.pop('_feishu_topic')
        await bot._prepare_topic({**message, 'chat_type': 'p2p'}, self.request(), actor)
        self.assertNotIn('_feishu_topic', actor)

    async def test_enrich_and_double_prepare_never_promote_personal_permissions(self):
        bot = FeishuAssistant(SimpleNamespace(), self.runtime, AsyncMock())
        bot.config = {'app_id': TARGET_APP_ID}
        bot.inbox = Inbox(self.root / 'inbox.sqlite3')
        from lan_bitable_template_portal.assistant_shared_read import shared_query_actor
        request, actor = self.request(), shared_query_actor(self.actor())
        message = {'message_id': 'om_LowAgain', 'root_id': 'om_Root', 'chat_id': 'oc_Group', 'chat_type': 'group',
                   'app_id': TARGET_APP_ID, 'sender_id': 'ou_Low'}
        await bot._prepare_topic(message, request, actor)
        await bot._prepare_topic(message, request, actor)
        self.assertEqual(actor['_personal_scopes'], ['D'])
        self.assertEqual(set(actor['_personal_modules']), {'assistant', 'event'})
        self.assertIn('A', actor['scopes'])
        token = self.authority.context(request, actor)
        self.assertEqual(self.authority.contexts[token]['actor']['scopes'], ['D'])
        assistant = LighthouseAssistant(self.store, Mock(), model=Mock())
        state = assistant._state(actor)
        turn = {'operation_id': 'feishu_' + 'b' * 40, 'at': 1, 'question': 'q', 'answer': 'borrowed source',
                'status': 'completed', 'topic_access': [topic_capability(self.operation())]}
        state['turns'].append(turn)
        self.store.put_document('lighthouse_ai', assistant._key(actor), state)
        assistant.record_channel_reply(actor, {'conversation_id': state['id'], 'operation_id': turn['operation_id'],
            'question': 'q', 'answer': 'borrowed source'})
        web = {**self.actor()}; web.pop('_feishu_topic')
        self.assertEqual(assistant._state(web)['turns'], [])
        self.assertEqual(assistant._state({**web, 'channel': ''})['turns'], [])
        self.assertEqual(assistant._state(actor)['turns'][0]['answer'], 'borrowed source')

    async def test_query_only_participant_does_not_borrow_business_write_permissions(self):
        self.auth.profiles['ou_Low']['modules'] = ['event']
        message = {'message_id': 'om_QueryOnly', 'root_id': 'om_Root', 'chat_id': 'oc_Group',
                   'chat_type': 'group', 'app_id': TARGET_APP_ID, 'sender_id': 'ou_Low'}
        self.assertTrue(self.registry.register(self.topic, self.actor(), message))
        self.assertIsNone(self.registry.profile('ou_Low'))
        self.assertTrue(self.registry.profile('ou_Low', shared_query=True))
        self.assertIn('A', self.registry.enrich(self.actor(), self.topic)['scopes'])
        with self.assertRaises(PortalError):
            self.registry.issue(self.topic, 'ou_Low', topic_capability(self.operation()))

    async def test_mixed_complete_capabilities_remain_context_without_cartesian_leak(self):
        inbox = Inbox(self.root / 'inbox.sqlite3')
        capabilities = [topic_capability(self.operation()), topic_capability(self.operation('B', 'repair'))]
        message = {'message_id': 'om_Mixed', 'root_id': 'om_Root', 'chat_id': 'oc_Group', 'chat_type': 'group', 'app_id': TARGET_APP_ID}
        inbox.remember_public_reply(message, {'text': 'A maintenance and B repair public results',
            'delivery_access': FeishuAssistant._delivery_access(self.actor(), capabilities),
            'thread_route': {'chat_id': 'oc_Group', 'root_id': 'om_Root'}})
        actor = self.registry.enrich(self.actor('ou_Repair'), self.topic)
        self.assertIn('A maintenance and B repair', inbox.topic_context(message, actor))
        self.auth.profiles['ou_Maint']['scope_rules'] = ['B']
        self.assertEqual(inbox.topic_context(message, self.registry.enrich(self.actor(), self.topic)), '')

    async def test_exact_openid_and_explicit_fixed_duty_reuse_owner_without_network(self):
        bot = FeishuAssistant(SimpleNamespace(), self.runtime, AsyncMock())
        bot.config = {'app_id': TARGET_APP_ID}
        bot.messenger = SimpleNamespace(sender_record=AsyncMock(side_effect=AssertionError('no HR lookup')),
                                        sender_name=AsyncMock(side_effect=AssertionError('no name lookup')))
        bot._user = Mock(side_effect=AssertionError('no union lookup'))
        direct = await bot._resolve_user({'sender_id': 'ou_Low'})
        self.assertEqual(direct['open_id'], 'ou_Low')
        duty = BUILDING_OPEN_ID_MAP['H']
        self.auth.add(duty, ['H'], ['assistant'])
        self.auth.explicit[duty] = {'name': 'H duty configured', 'enabled': True}
        self.auth.directory_person = lambda _: {}
        fixed = await bot._resolve_user({'sender_id': duty})
        self.assertEqual(fixed['open_id'], duty)
        self.assertEqual(fixed['name'], 'H duty configured')
        self.auth.explicit['ou_Unknown'] = {'name': 'H duty configured', 'enabled': True}
        self.assertIsNone(bot._sender_user({'sender_id': 'ou_Unknown'}))
        bot.messenger.sender_record.assert_not_awaited()
        bot.messenger.sender_name.assert_not_awaited()

    async def test_grant_revalidation_does_not_scan_two_hundred_profiles(self):
        execution = self.execution()
        record = self.store.get_document('lighthouse_topics', self.registry.key(self.topic))
        record['people'].update({'ou_Extra%d' % index: {'message_id': 'om_X', 'sender_id': 'ou_X'} for index in range(200)})
        self.store.put_document('lighthouse_topics', self.registry.key(self.topic), record)
        with patch.object(self.registry, 'profile', wraps=self.registry.profile) as profile:
            execution.validate()
        self.assertLessEqual(profile.call_count, 3)

    async def test_new_card_operator_enrolled_only_after_verified_topic_ticket(self):
        from lan_bitable_template_portal.feishu_notice_cards import _NAMESPACE
        bot = FeishuAssistant(SimpleNamespace(), self.runtime, AsyncMock())
        bot.config = {'app_id': TARGET_APP_ID}
        bot.inbox = SimpleNamespace(thread_for=lambda message: {'chat_id': message['chat_id'], 'chat_type': 'group', 'root_id': message['root_id']})
        self.auth.add('ou_New', ['D'], ['assistant'])
        actor = self.actor('ou_New')
        message = {'kind': 'card_action', 'message_id': 'crd_fixture', 'reply_to_message_id': 'om_Card',
                   'sender_id': 'ou_New', 'chat_id': 'oc_Group', 'chat_type': 'group', 'root_id': 'om_Root',
                   'app_id': TARGET_APP_ID, 'card': {'value': {'ticket': 'trusted-ticket'}}}
        self.assertFalse(self.registry.register(self.topic, actor, message))
        request = self.request('ou_New')
        shared = {'id': 'plan-one', 'owner': 'ou_Low', 'conversation_id': 'conversation'}
        proof = {'plan_id': 'plan-one', 'owner_open_id': 'ou_Low', 'conversation_id': 'conversation',
                 'chat_id': 'oc_Group', 'root_id': 'om_Root', 'created_at': __import__('time').time()}
        self.store.put_document(_NAMESPACE, 'trusted-ticket', proof)
        with self.assertRaises(PortalError):
            self.registry.enrich(actor, self.topic)
        await bot._register_topic_interaction(message, request, actor, shared=shared)
        self.assertIn('A', actor['scopes'])
        evidence = self.registry.evidence(self.topic, actor['id'])[actor['id']]
        self.assertEqual(evidence['source'], 'verified_card')
        for changed in [{'root_id': 'om_Other'}, {'chat_id': 'oc_Other'}, {'app_id': 'cli_other'}]:
            with self.assertRaises(AssistantError):
                await bot._register_topic_interaction({**message, **changed}, request, actor, shared=shared)
        bot.inbox.thread_for = lambda _: {'chat_type': 'p2p', 'root_id': 'om_Root'}
        before = self.store.get_document('lighthouse_topics', self.registry.key(self.topic))
        await bot._register_topic_interaction({**message, 'chat_type': 'p2p'}, request, actor, shared=shared)
        self.assertEqual(self.store.get_document('lighthouse_topics', self.registry.key(self.topic)), before)


if __name__ == '__main__':
    unittest.main()

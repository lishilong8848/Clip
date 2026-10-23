"""Offline voice contracts: fake providers, temporary owner files, no real calls."""
import base64
import asyncio
import copy
import contextlib
import gzip
import hashlib
import io
import json
import os
import sys
import tempfile
import threading
import traceback
import unittest
import wave
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant import lighthouse_voice as module
from openclaw_service.assistant.lighthouse_ai import AssistantError, NAMESPACE
from openclaw_service.assistant.lighthouse_files import FILE_NAMESPACE, LighthouseFiles
from openclaw_service.assistant.lighthouse_voice import CONFIG_KEY, UNSUPPORTED, Voice, validate_audio
from lan_bitable_template_portal import feishu_assistant_files as feishu
from lan_bitable_template_portal.feishu_assistant import FeishuAssistant, Inbox, parse_message

ADMIN = {'id': 'voice-admin', 'is_admin': True, 'scopes': ['D']}
USER = {'id': 'voice-user', 'is_admin': False, 'scopes': ['D'], 'channel': 'feishu:oc_test',
        '_feishu_topic': {'id': 'oc_test:om_topic:app:fixture'}}
SECRET = 'fixture-speech-secret'
ORIGIN = 'https://speech.example.test'


def wav_bytes():
    target = io.BytesIO()
    with wave.open(target, 'wb') as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(16000)
        audio.writeframes(b'\x00\x00' * 160)
    return target.getvalue()


WAV = wav_bytes()


class Store:
    def __init__(self):
        self.docs = {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)

    def delete_document(self, namespace, key):
        self.docs.pop((namespace, key), None)

    def list_documents(self, namespace, *, key_prefix=''):
        return [{'key': key, 'payload': copy.deepcopy(value)} for (ns, key), value in self.docs.items()
                if ns == namespace and key.startswith(key_prefix)]


def config():
    return {'allowed_origins': [ORIGIN],
            'stt': {'enabled': True, 'endpoint': ORIGIN + '/v1/audio/transcriptions',
                    'model': 'fixture-stt', 'api_key': SECRET},
            'tts': {'enabled': True, 'endpoint': ORIGIN + '/v1/audio/speech',
                    'model': 'fixture-tts', 'voice': 'fixture-voice', 'api_key': SECRET}}


class VoiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store()
        self.files = LighthouseFiles(self.store, root=self.temp.name)
        self.requests = []
        self.response = httpx.Response(200, json={'text': '查询当前D楼维修任务'})

        async def transport(request):
            await request.aread()
            self.requests.append(request)
            if isinstance(self.response, Exception):
                raise self.response
            return self.response

        self.factory = Mock(side_effect=lambda: httpx.AsyncClient(transport=httpx.MockTransport(transport)))
        self.voice = Voice(self.store, self.files, client_factory=self.factory,
            protect=lambda value: base64.b64encode(value.encode()).decode(),
            unprotect=lambda value: base64.b64decode(value).decode())

    def configure(self):
        return self.voice.configure(ADMIN, config())

    def upload(self, actor=USER, content=WAV, name='recording.wav', **kwargs):
        return self.files.upload(actor, name, content, extract=False, **kwargs)['id']

    async def test_unconfigured_is_explicit_and_never_constructs_client(self):
        self.store.put_document(NAMESPACE, 'model', {'key_cipher': 'old-chat-key', 'endpoint': 'https://wan.example/v1'})
        before = copy.deepcopy(self.store.docs)
        self.assertFalse(self.voice.capabilities()['stt'])
        self.assertFalse(self.voice.capabilities()['tts'])
        for method, argument in ((self.voice.transcribe, 'nonexistent'), (self.voice.synthesize, 'hello')):
            with self.assertRaises(AssistantError) as raised:
                await method(USER, argument)
            self.assertEqual(raised.exception.status, 503)
            self.assertEqual(raised.exception.category, 'voice_unconfigured')
            self.assertIn('尚未配置', str(raised.exception))
        self.factory.assert_not_called()
        self.assertEqual(before, self.store.docs)

    def test_dedicated_store_encryption_and_public_redaction(self):
        result = self.configure()
        saved = self.store.get_document(NAMESPACE, CONFIG_KEY)
        self.assertEqual(set(self.store.docs), {(NAMESPACE, CONFIG_KEY)})
        self.assertNotIn(SECRET, json.dumps(saved))
        self.assertNotIn('api_key', json.dumps(saved))
        for public in (result, self.voice.configuration(ADMIN), self.voice.capabilities()):
            self.assertNotIn(SECRET, json.dumps(public))
            self.assertNotIn('key_cipher', json.dumps(public))
        self.assertNotIn(ORIGIN, json.dumps(self.voice.capabilities()))
        self.assertTrue(result['stt']['has_key'])
        self.assertTrue(self.voice.capabilities()['tts'])
        self.factory.assert_not_called()

    async def test_authenticated_accounts_use_voice_and_only_admin_configures(self):
        for actor in (None, {}, {'id': 123}, {'id': 'guest', 'is_guest': True},
                      {'id': 'guest', 'role': 'guest'}):
            for method, value in ((self.voice.transcribe, 'id'), (self.voice.synthesize, 'text')):
                with self.assertRaises(AssistantError) as raised:
                    await method(actor, value)
                self.assertEqual(raised.exception.status, 403)
        for actor in (USER, {**ADMIN, 'is_admin': 'true'}, {**ADMIN, 'is_guest': True}):
            with self.assertRaises(AssistantError):
                self.voice.configure(actor, config())
            with self.assertRaises(AssistantError):
                self.voice.configuration(actor)
        self.factory.assert_not_called()

    async def test_temporary_and_external_nonadmins_can_use_speech_but_not_configure(self):
        self.configure()
        for role in ('temporary', 'external'):
            with self.subTest(role=role):
                actor = {**USER, 'id': 'voice-' + role, 'role': role, 'is_temporary': True}
                self.assertIs(actor['is_admin'], False)
                with self.assertRaises(AssistantError) as raised:
                    self.voice.configure(actor, {'stt': {'enabled': False}})
                self.assertEqual(raised.exception.status, 403)
                with self.assertRaises(AssistantError):
                    self.voice.configuration(actor)
                self.response = httpx.Response(200, json={'text': 'Hello'})
                self.assertEqual(await self.voice.transcribe(actor, self.upload(actor)), {'text': 'Hello'})
                self.response = httpx.Response(200, content=WAV, headers={'content-type': 'audio/wav'})
                receipt = await self.voice.synthesize(actor, 'Hello')
                item = self.files.get(actor, receipt['id'])
                self.assertEqual(item['owner'], actor['id'])
                self.assertEqual(Path(item['path']).read_bytes(), WAV)

    def test_allowlist_and_exact_audio_paths_are_required(self):
        invalid = ['http://speech.example.test/v1/audio/transcriptions',
                   ORIGIN + '/v1/chat/completions', ORIGIN + '/v1/audio/speech',
                   'https://user:password@speech.example.test/v1/audio/transcriptions',
                   ORIGIN + '/v1/audio/transcriptions?api_key=hidden',
                   ORIGIN + '/v1/audio/transcriptions#hidden',
                   ORIGIN + '/v1/%2e%2e/audio/transcriptions',
                   ORIGIN + '/v1/../audio/transcriptions',
                   'https://127.0.0.1/v1/audio/transcriptions',
                   'https://[::1]/v1/audio/transcriptions',
                   'https://169.254.169.254/v1/audio/transcriptions',
                   'https://localhost/v1/audio/transcriptions',
                   'https://speech.example.test:99999/v1/audio/transcriptions',
                   'https://other.example.test/v1/audio/transcriptions']
        for endpoint in invalid:
            with self.subTest(endpoint=endpoint):
                payload = config()
                payload['stt']['endpoint'] = endpoint
                with self.assertRaises(AssistantError) as raised:
                    self.voice.configure(ADMIN, payload)
                self.assertNotIn('password', str(raised.exception))
                self.assertNotIn(endpoint, str(raised.exception))
        self.assertIsNone(self.store.get_document(NAMESPACE, CONFIG_KEY))

    def test_origin_normalization_and_port_isolation(self):
        payload = config()
        payload['allowed_origins'] = ['https://SPEECH.example.test:443/']
        self.assertEqual(self.voice.configure(ADMIN, payload)['allowed_origins'], [ORIGIN])
        with self.assertRaises(AssistantError):
            self.voice.configure(ADMIN, {'stt': {'endpoint': ORIGIN + ':8443/v1/audio/transcriptions'}})

    def test_partial_save_clear_key_and_disabled_empty_form(self):
        self.configure()
        self.voice.configure(ADMIN, {'stt': {'model': 'new-model', 'api_key': ''}})
        self.assertTrue(self.voice.capabilities()['stt'])
        self.assertEqual(self.voice.configuration(ADMIN)['stt']['model'], 'new-model')
        self.voice.configure(ADMIN, {'stt': {'clear_key': True}})
        self.assertFalse(self.voice.capabilities()['stt'])
        self.assertFalse(self.voice.configuration(ADMIN)['stt']['has_key'])
        self.voice.configure(ADMIN, {'tts': {'enabled': False, 'endpoint': '', 'model': '', 'voice': ''},
                                   'allowed_origins': []})
        self.assertFalse(self.voice.capabilities()['tts'])

    def test_origin_change_cannot_reuse_old_key_and_updates_are_atomic(self):
        self.configure()
        before = copy.deepcopy(self.store.docs)
        with self.assertRaises(AssistantError):
            self.voice.configure(ADMIN, {'allowed_origins': [ORIGIN, 'https://new.example.test'],
                'stt': {'endpoint': 'https://new.example.test/v1/audio/transcriptions'}})
        self.assertEqual(self.store.docs, before)

    def test_invalid_config_fields_and_credentials_are_rejected(self):
        for payload in (None, [], {'stt': []}, {'allowed_origins': ORIGIN}, {'allowed_origins': [ORIGIN + '/path']},
                        {'stt': {'enabled': 'true'}}, {'stt': {'endpoint': False}},
                        {'stt': {'key_cipher': SECRET}}, {'stt': {'api_key': 'bad\nsecret'}},
                        {'stt': {'model': 'bad\nmodel'}}, {'tts': {'voice': ['bad']}},
                        {'allowed_origins': [f'https://host{i}.example.test' for i in range(11)]}):
            with self.subTest(payload=payload), self.assertRaises(AssistantError):
                self.voice.configure(ADMIN, payload)

    def test_encryption_error_is_redacted(self):
        self.voice._protect = Mock(side_effect=RuntimeError(SECRET))
        with self.assertRaises(AssistantError) as raised:
            self.configure()
        self.assertNotIn(SECRET, ''.join(traceback.format_exception(raised.exception)))
        self.assertIsNone(self.store.get_document(NAMESPACE, CONFIG_KEY))

    async def test_stt_multipart_and_original_text_are_preserved(self):
        self.configure()
        identity = self.upload()
        result = await self.voice.transcribe(USER, identity)
        self.assertEqual(result, {'text': '查询当前D楼维修任务'})
        request = self.requests[0]
        self.assertEqual(str(request.url), ORIGIN + '/v1/audio/transcriptions')
        self.assertEqual(request.headers['authorization'], 'Bearer ' + SECRET)
        self.assertIn('multipart/form-data', request.headers['content-type'])
        self.assertIn(b'name="file"', request.content)
        self.assertIn(b'filename="voice.wav"', request.content)
        self.assertIn(b'fixture-stt', request.content)
        self.assertIn(WAV, request.content)
        self.assertEqual(len(self.requests), 1)

    async def test_owner_topic_scope_and_deleted_file_acl_before_upload(self):
        self.configure()
        identity = self.upload(source_scopes=['D'])
        for actor in ({**USER, 'id': 'another'}, {**USER, '_feishu_topic': {'id': 'other-topic'}},
                      {**USER, 'scopes': ['A']}, {**USER, '_feishu_topic': None}):
            with self.subTest(actor=actor), self.assertRaises(AssistantError):
                await self.voice.transcribe(actor, identity)
        row = self.store.get_document(FILE_NAMESPACE, identity)
        row['deleted_at'] = 1
        self.store.put_document(FILE_NAMESPACE, identity, row)
        with self.assertRaises(AssistantError):
            await self.voice.transcribe(USER, identity)
        self.factory.assert_not_called()

    async def test_shared_grant_does_not_authorize_external_audio_upload(self):
        self.configure()
        identity = self.upload()
        other = {**USER, 'id': 'other', '_shared_plan': {'file_ids': [identity], 'chat_id': 'oc_test',
                                                     'topic_id': USER['_feishu_topic']['id']}}
        self.files.get(other, identity)
        with self.assertRaises(AssistantError) as raised:
            await self.voice.transcribe(other, identity)
        self.assertEqual(raised.exception.status, 403)
        self.factory.assert_not_called()

    async def test_changed_file_and_size_are_rejected_before_network(self):
        self.configure()
        identity = self.upload()
        row = self.store.get_document(FILE_NAMESPACE, identity)
        row['sha256'] = hashlib.sha256(b'other').hexdigest()
        self.store.put_document(FILE_NAMESPACE, identity, row)
        with self.assertRaises(AssistantError) as raised:
            await self.voice.transcribe(USER, identity)
        self.assertEqual(raised.exception.status, 409)
        with patch.object(module, 'MAX_AUDIO_BYTES', len(WAV) - 1):
            with self.assertRaises(AssistantError) as raised:
                await self.voice.transcribe(USER, identity)
            self.assertEqual(raised.exception.status, 413)
        self.factory.assert_not_called()

    async def test_invalid_file_id_never_requests_provider(self):
        self.configure()
        for identity in (None, '../file.wav', '0' * 32):
            with self.assertRaises(AssistantError):
                await self.voice.transcribe(USER, identity)
        self.factory.assert_not_called()

    async def test_empty_non_json_malformed_and_long_transcripts_fail_explicitly(self):
        self.configure()
        for response in (httpx.Response(200, json={}), httpx.Response(200, json={'text': ' '}),
                         httpx.Response(200, json={'text': 1}), httpx.Response(200, json=[]),
                         httpx.Response(200, content=b'{', headers={'content-type': 'application/json'}),
                         httpx.Response(200, text='hello'), httpx.Response(200, json={'text': 'a' * 12001})):
            self.response = response
            with self.subTest(response=response), self.assertRaises(AssistantError):
                await self.voice.transcribe(USER, self.upload())

    async def test_errors_and_redirects_never_expose_provider_secrets_or_retry(self):
        self.configure()
        for response in (httpx.Response(401, text=SECRET), httpx.Response(500, text=SECRET),
                         httpx.Response(307, headers={'location': 'https://unknown.example.test/' + SECRET}),
                         httpx.ConnectError(SECRET), httpx.ReadTimeout(SECRET)):
            self.response = response
            before = len(self.requests)
            with self.assertRaises(AssistantError) as raised:
                await self.voice.transcribe(USER, self.upload())
            self.assertNotIn(SECRET, ''.join(traceback.format_exception(raised.exception)))
            self.assertEqual(len(self.requests), before + 1)

    async def test_decryption_failure_is_safe_even_if_it_raises_assistant_error(self):
        self.configure()
        self.voice._unprotect = Mock(side_effect=AssistantError(SECRET))
        with self.assertRaises(AssistantError) as raised:
            await self.voice.transcribe(USER, self.upload())
        self.assertNotIn(SECRET, ''.join(traceback.format_exception(raised.exception)))
        self.factory.assert_not_called()

    async def test_response_size_bound_with_header_and_stream(self):
        self.configure()

        class Chunks(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b'a' * (128 * 1024)
                yield b'a'

        for response in (httpx.Response(200, headers={'content-length': str(128 * 1024 + 1)}),
                         httpx.Response(200, stream=Chunks())):
            self.response = response
            with self.assertRaises(AssistantError) as raised:
                await self.voice.transcribe(USER, self.upload())
            self.assertEqual(raised.exception.status, 413)

    async def test_compressed_response_is_rejected_before_reading_decoded_content(self):
        self.configure()
        self.response = httpx.Response(200, content=gzip.compress(b'large response'),
            headers={'content-encoding': 'gzip', 'content-type': 'application/json'})
        with self.assertRaises(AssistantError) as raised:
            await self.voice.transcribe(USER, self.upload())
        self.assertEqual(raised.exception.status, 502)

    async def test_total_deadline_cancels_a_stalled_provider(self):
        self.configure()

        async def stalled(request):
            await asyncio.sleep(1)
            raise AssertionError('deadline should cancel the provider')

        self.voice._client_factory = lambda: httpx.AsyncClient(transport=httpx.MockTransport(stalled))
        with patch.object(module, 'REQUEST_TIMEOUT', 0.01):
            with self.assertRaises(AssistantError) as raised:
                await self.voice.transcribe(USER, self.upload())
        self.assertEqual(raised.exception.status, 504)

    async def test_production_client_disables_proxies_redirects_and_uses_verified_tls(self):
        self.configure()
        original_client = httpx.AsyncClient
        tls = object()

        def create(**options):
            self.assertIs(options['verify'], tls)
            self.assertIs(options['trust_env'], False)
            self.assertIs(options['follow_redirects'], False)
            self.assertEqual(options['timeout'].read, module.REQUEST_TIMEOUT)
            return original_client(transport=httpx.MockTransport(lambda req: httpx.Response(200, json={'text': 'hello'})))

        self.voice._client_factory = None
        with patch.object(module, 'verified_tls_context', return_value=tls) as verify, \
             patch.object(module.httpx, 'AsyncClient', side_effect=create) as constructor:
            self.assertEqual(await self.voice.transcribe(USER, self.upload()), {'text': 'hello'})
        verify.assert_called_once_with(trust_env=False)
        constructor.assert_called_once()

    async def test_tts_returns_existing_owner_and_topic_protected_file(self):
        self.configure()
        self.response = httpx.Response(200, content=WAV, headers={'content-type': 'audio/wav'})
        with patch('openclaw_service.assistant.lighthouse_files.extract_document') as extract:
            receipt = await self.voice.synthesize(USER, 'Hello')
        extract.assert_not_called()
        payload = json.loads(self.requests[0].content)
        self.assertEqual(payload, {'model': 'fixture-tts', 'voice': 'fixture-voice', 'input': 'Hello', 'response_format': 'wav'})
        self.assertEqual(str(self.requests[0].url), ORIGIN + '/v1/audio/speech')
        self.assertEqual(receipt['mime'], 'audio/wav')
        self.assertNotIn('path', receipt)
        self.assertEqual(receipt['url'], '/api/assistant/files/' + receipt['id'])
        item = self.files.get(USER, receipt['id'])
        self.assertEqual(item['text'], 'Hello')
        self.assertEqual(receipt['error'], '')
        with patch('openclaw_service.assistant.lighthouse_files.extract_document') as extract:
            self.assertEqual(self.files.text(USER, receipt['id'])['text'], 'Hello')
        extract.assert_not_called()
        self.assertEqual(Path(item['path']).read_bytes(), WAV)
        for actor in ({**USER, 'id': 'other'}, {**USER, '_feishu_topic': {'id': 'other'}}, {**USER, 'scopes': ['A']}):
            with self.assertRaises(AssistantError):
                self.files.get(actor, receipt['id'])

    async def test_tts_input_validation_and_private_data_are_local(self):
        self.configure()
        for text in (None, '', ' ', 'x' * 4001, 'api_key: ' + SECRET):
            with self.assertRaises(AssistantError):
                await self.voice.synthesize(USER, text)
        self.factory.assert_not_called()

    async def test_tts_rejects_invalid_audio_mime_and_size_without_saving(self):
        self.configure()
        for response in (httpx.Response(200, content=WAV, headers={'content-type': 'text/html'}),
                         httpx.Response(200, content=b'<html>bad</html>', headers={'content-type': 'audio/wav'}),
                         httpx.Response(200, content=WAV[:-2], headers={'content-type': 'audio/wav'}),
                         httpx.Response(200, headers={'content-length': str(module.MAX_AUDIO_BYTES + 1)})):
            self.response = response
            with self.assertRaises(AssistantError):
                await self.voice.synthesize(USER, 'Hello')
        self.assertFalse(any(ns == FILE_NAMESPACE for ns, _ in self.store.docs))

    def test_audio_type_and_name_validation(self):
        self.assertEqual(validate_audio(WAV, 'recording.wave'), ('voice.wav', 'audio/wav'))
        self.assertEqual(validate_audio(b'OggS' + bytes(24) + b'OpusHead', 'voice.opus'), ('voice.ogg', 'audio/ogg'))
        self.assertEqual(validate_audio(b'\xff\xfb\x90\x00' + bytes(100)), ('voice.mp3', 'audio/mpeg'))
        for content, name in ((b'', 'a.wav'), (WAV, 'a.mp3'), (b'RIFF1234WAVE', 'a.wav'),
                              (b'<html>error</html>', 'a.mp3'), (b'ID3' + bytes(100), 'a.mp3'),
                              (b'OggS' + bytes(100), 'a.ogg')):
            with self.subTest(name=name), self.assertRaises(AssistantError):
                validate_audio(content, name)

    async def test_parent_http_contract_with_real_files_feishu_bridge_and_voice_routes(self):
        from fastapi import FastAPI
        from openclaw_service.assistant import routes

        app = FastAPI()

        async def authorize(request):
            identity = request.headers.get('x-test-account')
            if identity not in {'user', 'other', 'admin'}:
                raise AssistantError('测试账号未授权。', 403)
            return copy.deepcopy(ADMIN if identity == 'admin' else {**USER, **({'id': 'other'} if identity == 'other' else {})})

        host = SimpleNamespace(store=self.store, state=Path(self.temp.name) / 'lighthouse_openclaw', catalog=object(), authorize=authorize,
                               portal_bridge=SimpleNamespace(search=lambda *args: ([], [])))
        hooks = routes.install_assistant_routes(app, host)
        original_voice = Voice

        def voice_factory(store, files):
            return original_voice(store, files, client_factory=self.factory,
                protect=self.voice._protect, unprotect=self.voice._unprotect)

        with patch.object(routes, 'LighthouseAssistant', return_value=SimpleNamespace(store=self.store, model=SimpleNamespace(close=Mock()))), \
             patch.object(module, 'Voice', side_effect=voice_factory):
            async with contextlib.AsyncExitStack() as cleanup, httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://voice.test',
                                          headers={'x-test-account': 'user'}) as client:
                cleanup.push_async_callback(hooks['shutdown'])
                response = await client.get('/api/assistant/voice')
                self.assertEqual(response.status_code, 200, response.text)
                self.assertIs(response.json()['data']['stt'], False)
                self.assertNotIn('configuration', response.json()['data'])
                self.factory.assert_not_called()
                response = await client.put('/api/assistant/voice', json=config())
                self.assertEqual(response.status_code, 403)
                response = await client.put('/api/assistant/voice', json=config(), headers={'x-test-account': 'admin'})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertNotIn(SECRET, response.text)
                admin = await client.get('/api/assistant/voice', headers={'x-test-account': 'admin'})
                self.assertTrue(admin.json()['data']['configuration']['stt']['has_key'])
                configured = await client.get('/api/assistant/voice')
                self.assertEqual(configured.status_code, 200, configured.text)
                self.assertIs(USER['is_admin'], False)
                self.assertIs(configured.json()['data']['stt'], True)
                self.assertIs(configured.json()['data']['tts'], True)
                self.assertIs(configured.json()['data']['can_manage'], False)
                for private in ('configuration', 'api_key', 'key_cipher', 'allowed_origins', 'endpoint', SECRET, ORIGIN):
                    self.assertNotIn(private, configured.text)

                paths = []

                async def call(request, actor, path, body=None):
                    paths.append((path, body))
                    response = await client.request('GET' if body is None else 'POST', '/api/assistant/' + path, json=body)
                    if response.status_code != 200:
                        raise AssistantError('本地语音接口未完成。', response.status_code)
                    return response.json()['data']

                contexts = {}

                def context(request, actor):
                    contexts['test-context'] = actor
                    return 'test-context'

                current = SimpleNamespace(key='private-service-key', instance='fixture', lease='fixture',
                    descriptor={'port': 18799}, http_client=AsyncMock(return_value=client))
                bot = SimpleNamespace(_call=call, ready=AsyncMock(return_value=(current,
                                      SimpleNamespace(context=context, contexts=contexts))))
                message = {'message_type': 'audio', 'message_id': 'om_audio', 'content': {'file_key': 'file_v3_audio'}}
                with patch.object(feishu, '_download', new_callable=AsyncMock, return_value=WAV):
                    result = await feishu.transcribe_message(bot, message, object(), USER)
                self.assertEqual(result, {'question': '查询当前D楼维修任务', 'file_ids': []})
                self.assertEqual(paths[0], ('voice', None))
                self.assertEqual(paths[1][0], 'voice')
                self.assertEqual(paths[1][1]['action'], 'transcribe')
                self.assertFalse(contexts)
                response = await client.post('/api/assistant/voice',
                    json={'action': 'transcribe', 'file_id': paths[1][1]['file_id']})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()['data'], {'text': '查询当前D楼维修任务'})

                self.response = httpx.Response(200, content=WAV, headers={'content-type': 'audio/wav'})
                response = await client.post('/api/assistant/voice', json={'action': 'synthesize', 'text': 'Hello'})
                self.assertEqual(response.status_code, 200, response.text)
                receipt = response.json()['data']
                self.assertEqual(receipt['error'], '')
                download = await client.get(receipt['url'])
                self.assertEqual(download.content, WAV)
                self.assertEqual(download.headers['content-type'], 'audio/wav')
                self.assertEqual(download.headers['cache-control'], 'private, no-store')
                forbidden = await client.get(receipt['url'], headers={'x-test-account': 'other'})
                self.assertEqual(forbidden.status_code, 404)
                self.assertEqual(len(self.requests), 3)

    async def test_shutdown_waits_for_inflight_knowledge_constructor_even_after_request_cancellation(self):
        from fastapi import FastAPI
        from openclaw_service.server import Host
        from openclaw_service.assistant import routes
        from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase
        from bin.test_lighthouse_knowledge import FakeEmbedder, FakeMemoryIndex

        for mode in ('eager', 'cancelled-request'):
            with self.subTest(mode=mode):
                app = FastAPI()
                host = Host(state=Path(self.temp.name) / mode / 'lighthouse_openclaw',
                    key='fixture-control-key', manager=SimpleNamespace(accounts={}, close=AsyncMock()))
                host.authorize = AsyncMock(return_value={**USER, 'modules': ['knowledge']})
                host.backend_hooks = routes.install_assistant_routes(app, host)
                entered, finished, shutdown_entered = asyncio.Event(), asyncio.Event(), asyncio.Event()
                release = threading.Event()
                workers = []
                loop = asyncio.get_running_loop()

                def construct(root, **kwargs):
                    loop.call_soon_threadsafe(entered.set)
                    try:
                        if not release.wait(5):
                            raise AssertionError('Constructor barrier was not released')
                        kb = KnowledgeBase(root, embedder=FakeEmbedder(), memory_index_factory=FakeMemoryIndex, **kwargs)
                        workers.append(kb)
                        return kb
                    finally:
                        loop.call_soon_threadsafe(finished.set)

                original_shutdown = host.backend_hooks['shutdown']
                async def observed_shutdown():
                    shutdown_entered.set()
                    await original_shutdown()
                host.backend_hooks['shutdown'] = observed_shutdown
                request_task = close_task = None
                with patch.dict(os.environ, {'LIGHTHOUSE_AGENT_ENGINE': 'legacy'}), \
                        patch('openclaw_service.assistant.lighthouse_knowledge.KnowledgeBase', side_effect=construct), \
                        patch('socket.socket.connect', side_effect=AssertionError('No external network')):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://voice.test') as client:
                        try:
                            if mode == 'eager':
                                await host.backend_hooks['startup']()
                            else:
                                request_task = asyncio.create_task(client.get('/api/assistant/knowledge'))
                            await asyncio.wait_for(entered.wait(), 3)
                            if request_task is not None:
                                request_task.cancel()
                                await asyncio.gather(request_task, return_exceptions=True)
                            close_task = asyncio.create_task(host.close())
                            await asyncio.wait_for(shutdown_entered.wait(), 3)
                            self.assertFalse(close_task.done(), 'Shutdown returned while the constructor was blocked')
                            release.set()
                            await asyncio.wait_for(finished.wait(), 3)
                            await asyncio.wait_for(close_task, 5)
                            self.assertTrue(callable(workers[0].source_summarizer))
                            self.assertFalse(workers[0]._worker.is_alive())
                            for path in (workers[0].db, host.state.parent / 'document_library' / 'document_library.sqlite3'):
                                path.unlink()
                            response = await client.get('/api/assistant/knowledge')
                            self.assertEqual(response.status_code, 503, response.text)
                            self.assertEqual(len(workers), 1)
                            self.assertFalse(workers[0].db.exists())
                        finally:
                            release.set()
                            # Keep an initialization failure visible instead of masking it in cleanup.
                            if entered.is_set():
                                await asyncio.wait_for(finished.wait(), 5)
                            if close_task is not None:
                                await close_task
                            else:
                                await host.close()
                            for kb in workers:
                                await asyncio.to_thread(kb.close)

    async def test_original_assistant_acl_allows_staff_speech_and_blocks_guests(self):
        from fastapi import FastAPI
        from openclaw_service.assistant import routes
        from lan_bitable_template_portal.lighthouse_bridge import actor_for

        self.configure()
        sessions = {
            'user': {'open_id': 'ordinary-staff', 'role': 'staff', 'allowed_scopes': ['D']},
            'temporary': {'open_id': 'temporary-staff', 'role': 'temporary', 'is_temporary': True, 'allowed_scopes': ['D']},
            'external': {'open_id': 'external-staff', 'role': 'external', 'is_temporary': True, 'allowed_scopes': ['D']},
            'guest': {'open_id': 'guest', 'role': 'guest', 'allowed_scopes': ['D']},
            'flagged-guest': {'open_id': 'flagged-guest', 'role': 'staff', 'is_guest': True, 'allowed_scopes': ['D']},
            'no-assistant': {'open_id': 'no-assistant', 'role': 'staff', 'allowed_scopes': ['D']}}
        controller = SimpleNamespace(_current_session=lambda request: sessions.get(request.headers.get('x-test-account')))
        authority = SimpleNamespace(
            session_scopes=lambda session: session['allowed_scopes'],
            is_admin=lambda session: False,
            account_profile=lambda identity: {'modules': [] if identity == 'no-assistant' else ['assistant']})
        runtime = SimpleNamespace(state_store=self.store, auth_manager=authority)

        async def authorize(request):
            return await actor_for(controller, runtime, request)

        app = FastAPI()
        host = SimpleNamespace(store=self.store, state=Path(self.temp.name) / 'lighthouse_openclaw', catalog=object(), authorize=authorize,
                               portal_bridge=SimpleNamespace(search=lambda *args: ([], [])))
        hooks = routes.install_assistant_routes(app, host)
        original_voice = Voice

        def voice_factory(store, files):
            return original_voice(store, files, client_factory=self.factory,
                                  protect=self.voice._protect, unprotect=self.voice._unprotect)

        with patch.object(routes, 'LighthouseAssistant', return_value=SimpleNamespace(store=self.store, model=SimpleNamespace(close=Mock()))), \
             patch.object(module, 'Voice', side_effect=voice_factory):
            async with contextlib.AsyncExitStack() as cleanup, httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://voice.test') as client:
                cleanup.push_async_callback(hooks['shutdown'])
                for identity in ('user', 'temporary', 'external', 'no-assistant'):
                    with self.subTest(identity=identity):
                        headers = {'x-test-account': identity}
                        response = await client.get('/api/assistant/voice', headers=headers)
                        self.assertEqual(response.status_code, 200, response.text)
                        data = response.json()['data']
                        self.assertIs(data['stt'], True)
                        self.assertIs(data['tts'], True)
                        self.assertIs(data['can_manage'], False)
                        self.assertNotIn('configuration', data)
                        for secret in (SECRET, ORIGIN, 'api_key', 'key_cipher'):
                            self.assertNotIn(secret, response.text)
                        response = await client.post('/api/assistant/files', headers=headers,
                            files={'files': ('recording.wav', WAV, 'audio/wav')})
                        self.assertEqual(response.status_code, 200, response.text)
                        file_id = response.json()['data']['files'][0]['id']
                        self.response = httpx.Response(200, json={'text': 'Hello'})
                        response = await client.post('/api/assistant/voice', headers=headers,
                            json={'action': 'transcribe', 'file_id': file_id})
                        self.assertEqual(response.status_code, 200, response.text)
                        self.assertEqual(response.json()['data'], {'text': 'Hello'})
                        self.response = httpx.Response(200, content=WAV, headers={'content-type': 'audio/wav'})
                        response = await client.post('/api/assistant/voice', headers=headers,
                            json={'action': 'synthesize', 'text': 'Hello'})
                        self.assertEqual(response.status_code, 200, response.text)
                        receipt = response.json()['data']
                        response = await client.get(receipt['url'], headers=headers)
                        self.assertEqual(response.status_code, 200, response.text)
                        self.assertEqual(response.content, WAV)
                        denied = await client.put('/api/assistant/voice', headers=headers, json={'stt': {'enabled': False}})
                        self.assertEqual(denied.status_code, 403, denied.text)
                self.assertEqual(len(self.requests), 8)
                before = self.factory.call_count
                for identity, status in (('guest', 403), ('flagged-guest', 403), ('signed-out', 401)):
                    for method, path, payload in (('GET', '/api/assistant/voice', None),
                            ('POST', '/api/assistant/voice', {'action': 'transcribe', 'file_id': file_id}),
                            ('POST', '/api/assistant/voice', {'action': 'synthesize', 'text': 'Hello'}),
                            ('PUT', '/api/assistant/voice', {'stt': {'enabled': False}})):
                        with self.subTest(identity=identity, method=method):
                            response = await client.request(method, path, headers={'x-test-account': identity}, json=payload)
                            self.assertEqual(response.status_code, status, response.text)
                self.assertEqual(self.factory.call_count, before)
                self.assertEqual(len(self.requests), 8)

    async def test_runtime_blocks_audio_egress_but_owned_dedicated_transcription_works(self):
        from fastapi import FastAPI
        from openclaw_service.assistant.lighthouse_runtime import OpenClawRuntime, account_key, install_model_route

        self.configure()
        identity = self.upload()
        runtime = OpenClawRuntime(Path(self.temp.name) / 'runtime')
        key = account_key(USER['id'])
        item = {'agent_id': 'lh-' + key, 'busy': True, 'blocked': False,
                'profile': {'model': 'chat-only', 'endpoint': 'https://chat.example.test/v1/chat/completions'},
                'model_key': 'fixture-chat-key'}
        runtime.accounts[key] = item
        runtime.gateway = {'token': 'fixture-gateway-token'}
        chat_requests = []

        def chat_provider(request):
            chat_requests.append(request)
            return httpx.Response(200, json={'choices': []})

        runtime.http = httpx.AsyncClient(transport=httpx.MockTransport(chat_provider))
        runtime.model_client = AsyncMock(wraps=runtime.model_client)
        app = FastAPI()
        install_model_route(app, runtime)
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=('127.0.0.1', 12345)),
                    base_url='http://127.0.0.1', headers={'Authorization': 'Bearer fixture-gateway-token'}) as client:
                audio = {'data': base64.b64encode(WAV).decode(), 'format': 'wav'}
                for image_input in (False, True):
                    item['profile']['image_input'] = image_input
                    for kind in ('input_audio', 'audio', 'input_video', 'video'):
                        with self.subTest(kind=kind, image_input=image_input):
                            response = await client.post('/api/assistant/openclaw-models/chat/completions',
                                json={'model': item['agent_id'], 'messages': [{'role': 'user', 'content': [
                                    {'type': 'text', 'text': 'Transcribe'}, {'type': kind, kind: audio}]}]})
                            self.assertEqual(response.status_code, 400, response.text)
                            self.assertEqual(response.json()['error']['code'], 'unsupported_media')
                runtime.model_client.assert_not_awaited()
                self.assertFalse(chat_requests)
                self.factory.assert_not_called()

                result = await self.voice.transcribe(USER, identity)
                self.assertEqual(result, {'text': '查询当前D楼维修任务'})
                self.assertEqual(str(self.requests[0].url), ORIGIN + '/v1/audio/transcriptions')
                self.assertEqual(self.requests[0].headers['authorization'], 'Bearer ' + SECRET)
                self.assertIn(WAV, self.requests[0].content)
                runtime.model_client.assert_not_awaited()

                response = await client.post('/api/assistant/openclaw-models/chat/completions',
                    json={'model': item['agent_id'], 'messages': [{'role': 'user', 'content': result['text']}]})
                self.assertEqual(response.status_code, 200, response.text)
                runtime.model_client.assert_awaited_once()
                self.assertEqual(len(chat_requests), 1)
                self.assertEqual(str(chat_requests[0].url), item['profile']['endpoint'])
                self.assertEqual(chat_requests[0].headers['authorization'], 'Bearer fixture-chat-key')
                self.assertNotIn(base64.b64encode(WAV).decode(), chat_requests[0].content.decode())
        finally:
            runtime.gateway = None
            await runtime.close()


class FeishuVoiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.message = {'message_type': 'audio', 'message_id': 'om_audio', 'sender_id': 'ou_test',
                        'app_id': 'cli_current', 'chat_id': 'oc_test', 'root_id': 'om_topic',
                        'content': {'file_key': 'file_v3_voice-key'}, 'question': '[语音]'}
        self.bot = SimpleNamespace(_call=AsyncMock(side_effect=[{'stt': True}, {'text': '确认执行'}]))
        self.request = SimpleNamespace(state=SimpleNamespace())

    async def test_disabled_and_absent_route_return_voice_message_without_download(self):
        for value in ({'stt': False}, {}, AssistantError('missing', 404)):
            self.bot._call.side_effect = None
            self.bot._call.return_value = value if isinstance(value, dict) else None
            if isinstance(value, AssistantError):
                self.bot._call.side_effect = value
            with patch.object(feishu, '_download', new_callable=AsyncMock) as download:
                result = await feishu.transcribe_message(self.bot, self.message, self.request, USER)
            self.assertEqual(result, {'text': UNSUPPORTED})
            download.assert_not_called()

    async def test_malformed_capabilities_and_transport_failure_never_download(self):
        for value in (None, [], {'stt': 'true'}, RuntimeError(SECRET)):
            self.bot._call = AsyncMock(side_effect=value) if isinstance(value, Exception) else AsyncMock(return_value=value)
            with patch.object(feishu, '_download', new_callable=AsyncMock) as download:
                result = await feishu.transcribe_message(self.bot, self.message, self.request, USER)
            self.assertTrue(result['text'])
            self.assertNotIn(SECRET, result['text'])
            download.assert_not_called()

    async def test_audio_bridge_keeps_actor_topic_and_hands_text_back_without_chat(self):
        before = copy.deepcopy(self.message)
        with patch.object(feishu, '_download', new_callable=AsyncMock, return_value=WAV) as download, \
             patch.object(feishu, '_upload', new_callable=AsyncMock, return_value='a' * 32) as upload:
            result = await feishu.transcribe_message(self.bot, self.message, self.request, USER)
        self.assertEqual(result, {'question': '确认执行', 'file_ids': []})
        self.assertEqual(self.message, before)
        resource = download.call_args.args[2]
        self.assertEqual(resource['original_mid'], self.message['message_id'])
        self.assertEqual(resource['kind'], 'file')
        self.assertIs(upload.call_args.args[2], USER)
        self.bot._call.assert_awaited_with(self.request, USER, 'voice', {'action': 'transcribe', 'file_id': 'a' * 32})
        self.assertFalse(any(call.args[2] == 'chat' for call in self.bot._call.call_args_list))

    async def test_non_audio_and_original_media_behavior(self):
        for kind in ('text', 'file', 'image', 'post', 'media'):
            message = {**self.message, 'message_type': kind}
            self.assertIsNone(await feishu.transcribe_message(self.bot, message, self.request, USER))
        resource = feishu.parse_resources({**self.message, 'message_type': 'file'},
                                          {'file_key': 'file_v3_file', 'file_name': 'a.pdf'})[0]
        self.assertEqual(resource['kind'], 'file')
        self.assertEqual(resource['name'], 'a.pdf')
        self.assertEqual(feishu.parse_resources(self.message, self.message['content']), [])
        self.bot._call.assert_not_called()

    async def test_invalid_audio_ids_never_download(self):
        for message in ({**self.message, 'message_id': '../om_voice'},
                        {**self.message, 'content': {'file_key': 'https://untrusted.example/file'}},
                        {**self.message, 'content': {'file_key': 'img_v3_bad'}},
                        {**self.message, 'content': None}):
            with patch.object(feishu, '_download', new_callable=AsyncMock) as download:
                result = await feishu.transcribe_message(self.bot, message, self.request, USER)
            self.assertTrue(result['text'])
            download.assert_not_called()
        self.bot._call.assert_not_called()

    async def test_permission_errors_propagate_and_other_errors_are_redacted(self):
        for error in (AssistantError(SECRET, 403), RuntimeError(SECRET)):
            self.bot._call = AsyncMock(return_value={'stt': True})
            with patch.object(feishu, '_download', new_callable=AsyncMock, side_effect=error):
                if isinstance(error, AssistantError):
                    with self.assertRaises(AssistantError):
                        await feishu.transcribe_message(self.bot, self.message, self.request, USER)
                else:
                    result = await feishu.transcribe_message(self.bot, self.message, self.request, USER)
                    self.assertNotIn(SECRET, result['text'])

    async def test_empty_and_sensitive_transcripts_never_become_empty_replies(self):
        for text in ('', ' ', None, 'api_key: ' + SECRET, '查询身份证号码', 'x' * 12001):
            self.bot._call = AsyncMock(side_effect=[{'stt': True}, {'text': text}])
            with patch.object(feishu, '_download', new_callable=AsyncMock, return_value=WAV), \
                 patch.object(feishu, '_upload', new_callable=AsyncMock, return_value='a' * 32):
                result = await feishu.transcribe_message(self.bot, self.message, self.request, USER)
            self.assertTrue(result['text'])
            self.assertNotIn(SECRET, result['text'])

    async def test_feishu_download_uses_current_bot_headers_and_no_redirects(self):
        requests = []

        async def transport(request):
            requests.append(request)
            return httpx.Response(200, content=WAV)

        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
            bot = SimpleNamespace(messenger=SimpleNamespace(client=AsyncMock(return_value=client),
                headers=AsyncMock(return_value={'Authorization': 'Bearer current-bot-token'})))
            resource = feishu.parse_audio_resource(self.message, self.message['content'])
            self.assertEqual(await feishu._download(bot, self.message, resource), WAV)
        self.assertEqual(str(requests[0].url), 'https://open.feishu.cn/open-apis/im/v1/messages/om_audio/resources/file_v3_voice-key?type=file')
        self.assertEqual(requests[0].headers['authorization'], 'Bearer current-bot-token')
        self.assertEqual(len(requests), 1)


class FeishuVoiceAnswerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store()
        self.files = LighthouseFiles(self.store, root=Path(self.temp.name) / 'files')
        self.actor = {'id': 'portal-owner', 'scopes': ['D'], 'modules': ['assistant', 'maintenance'],
                      'is_admin': False, 'channel': 'feishu:oc_Current'}
        self.events, self.provider_requests = [], []
        self.transcript = '查询当前D楼维修任务'
        self.conversation = {'conversation_id': 'original-conversation', 'turns': []}

        async def provider(request):
            await request.aread()
            self.provider_requests.append(request)
            self.events.append('provider')
            return httpx.Response(200, json={'text': self.transcript})

        self.voice = Voice(self.store, self.files,
            protect=lambda value: base64.b64encode(value.encode()).decode(),
            unprotect=lambda value: base64.b64decode(value).decode(),
            client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(provider)))
        self.voice.configure(ADMIN, config())
        self.bot = FeishuAssistant(SimpleNamespace(bound_port=18799, preferred_port=18799),
                                   SimpleNamespace(state_store=self.store), AsyncMock())
        self.bot.config = {'app_id': 'cliCurrent'}
        self.bot.inbox = Inbox(Path(self.temp.name) / 'inbox.sqlite3')

        async def resolve(message):
            self.events.append('sender-auth')
            self.assertEqual(message['sender_id'], 'ou_Sender')
            return {'open_id': self.actor['id']}

        self.bot._resolve_user = AsyncMock(side_effect=resolve)
        self.bot._session = Mock(return_value='fixture-session-cookie')

        async def authorize(controller, runtime, request):
            self.events.append('account-auth')
            self.assertEqual(request.state.lighthouse_channel, self.actor['channel'])
            return self.actor

        self.authorize = AsyncMock(side_effect=authorize)
        actor_patch = patch('lan_bitable_template_portal.lighthouse_bridge.actor_for', self.authorize)
        actor_patch.start()
        self.addCleanup(actor_patch.stop)

        async def call(request, actor, path, body=None, **kwargs):
            self.events.append(path)
            self.assertIs(actor, self.actor)
            if path == 'conversation':
                self.request = request
                self.assertIs(request.state.feishu_topic, actor['_feishu_topic'])
                return copy.deepcopy(self.conversation)
            self.assertIs(request, self.request)
            if path == 'voice':
                if body is None:
                    return self.voice.capabilities()
                self.assertEqual(body['action'], 'transcribe')
                return await self.voice.transcribe(actor, body['file_id'])
            if path == 'chat':
                return {'turns': [{'operation_id': body['operation_id'], 'status': 'completed', 'answer': '原会话回答'}]}
            raise AssertionError('Unexpected endpoint: ' + path)

        self.bot._call = AsyncMock(side_effect=call)

        async def download(bot, message, resource):
            self.events.append('download')
            self.assertIn('account-auth', self.events)
            self.assertTrue(self.actor['_feishu_topic']['id'])
            self.assertEqual(resource['original_mid'], message['message_id'])
            return WAV

        async def upload(bot, request, actor, name, content):
            self.events.append('upload')
            self.assertIs(actor, self.actor)
            self.assertIs(request, self.request)
            return self.files.upload(actor, name, content, extract=False)['id']

        self.download, self.upload = AsyncMock(side_effect=download), AsyncMock(side_effect=upload)
        for name, replacement in (('_download', self.download), ('_upload', self.upload)):
            resource_patch = patch.object(feishu, name, replacement)
            resource_patch.start()
            self.addCleanup(resource_patch.stop)

    @staticmethod
    def payload(*, root='om_Root', kind='audio', content=None):
        return {'header': {'app_id': 'cliCurrent', 'event_type': 'im.message.receive_v1'}, 'event': {
            'sender': {'sender_type': 'user', 'sender_id': {'open_id': 'ou_Sender', 'union_id': 'on_Sender'}},
            'message': {'message_id': 'om_Audio', 'chat_id': 'oc_Current', 'chat_type': 'group',
                        'root_id': root, 'thread_id': 'omt_Topic', 'parent_id': root,
                        'message_type': kind, 'content': json.dumps(content if content is not None else {
                            'file_key': 'file_v3_audio-key', 'duration': 1000}),
                        'mentions': [{'key': '@_user_1', 'id': {'open_id': 'ou_Bot'}}]}}}

    def message(self, **kwargs):
        message = parse_message(self.payload(**kwargs), 'cliCurrent', 'ou_Bot', inbox=self.bot.inbox)
        self.assertIsNotNone(message)
        self.bot.inbox.add(message)
        return message

    def chats(self):
        return [call for call in self.bot._call.call_args_list if call.args[2] == 'chat']

    def test_parser_preserves_valid_audio_and_existing_sender_app_topic_gates(self):
        message = self.message()
        self.assertEqual(message['message_type'], 'audio')
        self.assertFalse(message['problem'])
        self.assertEqual(message['content']['file_key'], 'file_v3_audio-key')
        self.assertTrue(message['question'])
        self.assertEqual((message['sender_id'], message['root_id'], message['thread_id']),
                         ('ou_Sender', 'om_Root', 'omt_Topic'))
        root = self.payload(kind='text', content={'text': 'Existing topic'})
        root['event']['message']['message_id'] = 'om_Root'
        root['event']['message'].pop('root_id')
        self.bot.inbox.add(parse_message(root, 'cliCurrent', 'ou_Bot'))
        followup = self.payload()
        followup['event']['message'].update(message_id='om_Followup', mentions=[])
        self.assertEqual(parse_message(followup, 'cliCurrent', 'ou_Bot', inbox=self.bot.inbox)['message_type'], 'audio')
        followup['event']['message']['root_id'] = 'om_Foreign'
        self.assertIsNone(parse_message(followup, 'cliCurrent', 'ou_Bot', inbox=self.bot.inbox))
        for failure in ('app', 'sender', 'mention', 'topic'):
            payload = self.payload()
            if failure == 'app':
                payload['header']['app_id'] = 'cliOld'
            elif failure == 'sender':
                payload['event']['sender']['sender_type'] = 'app'
            else:
                payload['event']['message']['mentions'] = []
                if failure == 'topic':
                    payload['event']['message']['root_id'] = 'om_Foreign'
            with self.subTest(failure=failure):
                # No inbox proof is supplied for an unmentioned group message.
                self.assertIsNone(parse_message(payload, 'cliCurrent', 'ou_Bot'))
        self.assertTrue(parse_message(self.payload(content={'file_key': 'https://unknown.example/audio'}),
                                     'cliCurrent', 'ou_Bot')['problem'])
        self.assertFalse(self.provider_requests)

    async def test_real_answer_transcript_reuses_original_actor_topic_and_operation(self):
        message = self.message()
        original = copy.deepcopy(message)
        result = await self.bot.answer(message)
        self.assertEqual(self.events, ['sender-auth', 'account-auth', 'conversation', 'voice',
                                       'download', 'upload', 'voice', 'provider', 'chat'])
        self.assertEqual(len(self.chats()), 1)
        request, actor, _, body = self.chats()[0].args
        self.assertIs(actor, self.actor)
        self.assertIs(actor['is_admin'], False)
        self.assertNotEqual(actor['id'], message['sender_id'])
        self.assertEqual(actor['_feishu_topic']['id'], 'oc_Current:om_Root:app:' +
                         hashlib.sha256(b'cliCurrent').hexdigest()[:16])
        self.assertEqual(body['conversation_id'], self.conversation['conversation_id'])
        self.assertEqual(body['question'], self.transcript)
        self.assertEqual(body['operation_id'], 'feishu_' + hashlib.sha256(b'om_Audio').hexdigest()[:40])
        self.assertEqual(body['file_ids'], [])
        self.assertEqual(request.state.feishu_question, self.transcript)
        for field in ('app_id', 'message_id', 'sender_id', 'chat_id', 'root_id', 'thread_id'):
            self.assertEqual(message[field], original[field])
        item = next(value for (ns, _), value in self.store.docs.items() if ns == FILE_NAMESPACE)
        self.assertEqual((item['owner'], item['topic_id']), (actor['id'], actor['_feishu_topic']['id']))
        self.assertEqual((result['text'], result['actor_id'], result['conversation_id']),
                         ('原会话回答', actor['id'], self.conversation['conversation_id']))
        self.assertEqual(result['delivery_access']['scopes'], ['D'])

    async def test_temporary_and_external_feishu_senders_use_original_nonadmin_flow(self):
        for role in ('temporary', 'external'):
            with self.subTest(role=role):
                self.actor.update(id='portal-' + role, role=role, is_temporary=True)
                self.assertIs(self.actor['is_admin'], False)
                payload = self.payload()
                payload['event']['message']['message_id'] = 'om_' + role + 'Audio'
                message = parse_message(payload, 'cliCurrent', 'ou_Bot', inbox=self.bot.inbox)
                self.bot.inbox.add(message)
                result = await self.bot.answer(message)
                self.assertEqual(result['text'], '原会话回答')
                self.assertEqual(result['actor_id'], self.actor['id'])
                self.assertEqual(self.chats()[-1].args[3]['question'], self.transcript)
        self.assertEqual(len(self.provider_requests), 2)

    async def test_second_topic_keeps_audio_receipts_and_request_contexts_separate(self):
        first = self.message()
        await self.bot.answer(first)
        payload = self.payload(root='om_AnotherRoot')
        payload['event']['message'].update(message_id='om_AnotherAudio', thread_id='omt_AnotherTopic')
        second = parse_message(payload, 'cliCurrent', 'ou_Bot', inbox=self.bot.inbox)
        self.bot.inbox.add(second)
        await self.bot.answer(second)
        first_call, second_call = self.chats()
        first_topic = first_call.args[0].state.feishu_topic['id']
        second_topic = second_call.args[0].state.feishu_topic['id']
        self.assertNotEqual(first_topic, second_topic)
        self.assertIn(':om_Root:', first_topic)
        self.assertIn(':om_AnotherRoot:', second_topic)
        self.assertEqual(second_topic, self.actor['_feishu_topic']['id'])
        self.assertNotEqual(first_call.args[3]['operation_id'], second_call.args[3]['operation_id'])
        receipts = [value for (ns, _), value in self.store.docs.items() if ns == FILE_NAMESPACE]
        self.assertEqual({row['topic_id'] for row in receipts}, {first_topic, second_topic})
        old_receipt = next(row for row in receipts if row['topic_id'] == first_topic)
        with self.assertRaises(AssistantError) as raised:
            self.files.get(self.actor, old_receipt['id'])
        self.assertEqual(raised.exception.status, 404)

    async def test_unsupported_voice_is_explicit_and_never_downloads_or_chats(self):
        self.voice.configure(ADMIN, {'stt': {'enabled': False}})
        result = await self.bot.answer(self.message())
        self.assertEqual(result['text'], UNSUPPORTED)
        self.assertTrue(result['sync_history'])
        self.assertEqual(result['actor_id'], self.actor['id'])
        self.download.assert_not_awaited()
        self.upload.assert_not_awaited()
        self.assertFalse(self.provider_requests)
        self.assertFalse(self.chats())

    async def test_sender_account_and_topic_denials_never_reach_transcription(self):
        for gate in ('sender', 'account', 'topic', 'old-app'):
            with self.subTest(gate=gate):
                message = self.message()
                if gate == 'sender':
                    target = patch.object(self.bot, '_resolve_user', AsyncMock(side_effect=AssistantError('denied', 403)))
                elif gate == 'account':
                    target = patch.object(self.authorize, 'side_effect', AssistantError('denied', 403))
                elif gate == 'topic':
                    target = patch.object(self.bot.inbox, 'context_topic', side_effect=AssistantError('denied', 403))
                else:
                    target = patch.dict(self.bot.config, {'app_id': 'cliNew'})
                with target, self.assertRaises(AssistantError) as raised:
                    await self.bot.answer(message)
                self.assertEqual(raised.exception.status, 403)
        self.bot._call.assert_not_awaited()
        self.download.assert_not_awaited()
        self.upload.assert_not_awaited()
        self.assertFalse(self.provider_requests)

    async def test_transcription_waits_for_account_authorization_to_finish(self):
        entered, release = asyncio.Event(), asyncio.Event()
        authorize = self.authorize.side_effect

        async def pending(*args):
            entered.set()
            await release.wait()
            return await authorize(*args)

        self.authorize.side_effect = pending
        task = asyncio.create_task(self.bot.answer(self.message()))
        try:
            await asyncio.wait_for(entered.wait(), timeout=2)
            self.bot._call.assert_not_awaited()
            self.download.assert_not_awaited()
            self.assertFalse(self.provider_requests)
            release.set()
            self.assertEqual((await asyncio.wait_for(task, timeout=3))['text'], '原会话回答')
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_spoken_cancel_runs_original_consent_handler_for_same_account(self):
        from lan_bitable_template_portal.assistant_read_consent import ReadConsent

        consent = ReadConsent(self.store)
        question = self.transcript
        consent.require(self.actor, 'read', {}, 'fixture read', question)
        other = {**self.actor, 'id': 'other-person'}
        consent.require(other, 'read', {}, 'other read', question)
        self.transcript = '取消查询'
        result = await self.bot.answer(self.message())
        self.assertIn('查询授权已取消', result['text'])
        self.assertEqual(consent.pending(self.actor, question), [])
        self.assertEqual(len(consent.pending(other, question)), 1)
        self.assertFalse(self.chats())

    async def test_spoken_consent_approval_uses_original_approved_question(self):
        from lan_bitable_template_portal.assistant_read_consent import ReadConsent

        consent = ReadConsent(self.store)
        question = self.transcript
        approval = consent.require(self.actor, 'read', {}, 'fixture read', question)
        self.transcript = '确认查询 ' + approval['confirmation_code']
        result = await self.bot.answer(self.message())
        self.assertEqual(result['text'], '原会话回答')
        self.assertEqual(self.chats()[0].args[3]['question'], question)
        self.assertEqual(self.request.state.feishu_question, question)
        self.assertEqual(consent.pending(self.actor, question), [])

    async def test_spoken_confirmation_never_uses_an_unrelated_private_plan(self):
        self.conversation['turns'] = [{'plan': {'id': 'private-plan', 'status': 'awaiting_confirmation',
                                               'fields': [], 'operations': []}}]
        self.transcript = '确认执行'
        result = await self.bot.answer(self.message())
        self.assertIn('当前话题没有可确认或编辑的业务表单', result['text'])
        self.assertFalse(self.chats())
        self.assertFalse(any('/confirm' in call.args[2] for call in self.bot._call.call_args_list))


if __name__ == '__main__':
    unittest.main()

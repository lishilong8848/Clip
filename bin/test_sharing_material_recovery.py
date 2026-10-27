"""Offline material-size, image-only, model failover and manual recovery contracts."""
import asyncio
import hashlib
import io
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from bin import test_learning_sharing as fixture
from bin.test_learning_sharing_materials import local_only, manual
from bin import test_sharing_image_model as model_fixture
from lan_bitable_template_portal.learning import LearningError
from lan_bitable_template_portal.learning_sharing_routes import SharingRuntime
from lan_bitable_template_portal.sharing_sources import SharingMaterials, read_material_link
from lan_bitable_template_portal.sharing_images import jpeg, SharingImages
from openclaw_service.assistant.lighthouse_document_tools import checked_file
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant import lighthouse_sharing


class MaterialRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket.connect', local_only))
        self.fixture = fixture.SharingTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.core, self.learning, self.bid = self.fixture.core, self.fixture.learning, self.fixture.bid

    def test_manual_create_needs_no_source_model_or_generation_slot(self):
        person = {'id': 'p1', 'active': True, 'login_ids': ['ou_one'], 'name': 'Manual recipient', 'scopes': ['A']}
        with self.learning.transaction() as conn:
            self.learning._put('person', person['id'], person, conn, False)
            for index in range(3):
                self.learning._put('sharing_batch', str(index), {'status': 'generating'}, conn, False)
        created = self.core.create('', 'manual-without-model-01', fixture.ADMIN,
            participant_ids=['p1'], auto_generate=False)
        self.assertEqual(self.core.create('', 'manual-without-model-01', fixture.ADMIN,
            participant_ids=['p1'], auto_generate=False), created)
        batch = self.core.batch(created['id'])
        self.assertEqual(batch['status'], 'draft')
        self.assertEqual([p['open_id'] for p in batch['participants']], ['ou_one'])
        self.core.edit(created['id'], {'version': 1, 'questions': manual(5)}, fixture.ADMIN)
        self.assertEqual(len(self.core.batch(created['id'])['questions']), 5)
        self.assertEqual(self.learning._all('sharing_delivery'), [])

    def test_failed_generation_keeps_source_and_recovers_manually_without_model(self):
        original = self.core.batch(self.bid)
        source = {'text': fixture.SOURCE, 'title': 'Preserved material', 'participants': original['participants']}
        runtime = SharingRuntime(SimpleNamespace(service=object()), lambda: self.learning)
        with patch('lan_bitable_template_portal.learning_sharing_routes.read_source', return_value=source):
            asyncio.run(runtime.generate(original))
        batch = self.core.batch(self.bid)
        self.assertEqual(batch['status'], 'failed')
        self.assertEqual(batch['source']['text'], fixture.SOURCE)
        self.assertEqual(batch['participants'], original['participants'])
        prior = batch['questions']
        updated = self.core.edit(self.bid, {'version': batch['version'], 'questions': prior + manual(1)}, fixture.ADMIN)
        self.assertEqual(updated['status'], 'draft')
        self.assertEqual(len(updated['questions']), len(prior) + 1)
        self.assertEqual(self.core.attempts(self.bid), [])
        with self.assertRaises(LearningError):
            self.core.edit(self.bid, {'version': batch['version'], 'questions': manual(5)}, fixture.ADMIN)

    def test_manual_materials_still_load_source_without_requiring_a_model(self):
        with self.learning.transaction() as conn:
            self.learning._put('person', 'p1', {'id': 'p1', 'name': 'Recipient', 'active': True,
                'login_ids': ['ou_one'], 'scopes': ['A']}, conn, False)
        link = 'https://vnet.feishu.cn/docx/Material'
        initial = manual(2)
        created = self.core.create('', 'manual-keep-material-01', fixture.ADMIN,
            participant_ids=['p1'], auto_generate=False, material_urls=[link], initial_questions=initial)
        self.assertEqual(self.core.batch(created['id'])['status'], 'queued')
        saved = self.core.batch(created['id'])['questions']
        self.assertEqual(len(saved), 2)
        generate = Mock(side_effect=AssertionError('Manual questions must not call a model'))
        runtime = SharingRuntime(SimpleNamespace(service=object(), generate_sharing_questions=generate), lambda: self.learning)
        with patch('lan_bitable_template_portal.learning_sharing_routes.read_material_link', return_value={
                'title': 'Manual material', 'text': fixture.SOURCE, 'url': link}):
            asyncio.run(runtime.generate(self.core.batch(created['id'])))
        batch = self.core.batch(created['id'])
        self.assertEqual(batch['status'], 'draft', batch.get('error'))
        self.assertEqual(batch['source']['text'], fixture.SOURCE)
        self.assertEqual(batch['participants'][0]['open_id'], 'ou_one')
        self.assertEqual(batch['questions'], saved)
        generate.assert_not_called()
        self.assertEqual(self.core.create('', 'manual-keep-material-01', fixture.ADMIN,
            participant_ids=['p1'], auto_generate=False, material_urls=[link], initial_questions=initial), created)

    def test_manual_questions_survive_material_failure_and_retry(self):
        with self.learning.transaction() as conn:
            self.learning._put('person', 'p1', {'id': 'p1', 'name': 'Recipient', 'active': True,
                'login_ids': ['ou_one'], 'scopes': ['A']}, conn, False)
        created = self.core.create('', 'manual-material-retry-01', fixture.ADMIN, participant_ids=['p1'],
            auto_generate=False, material_urls=['https://vnet.feishu.cn/docx/Material'], initial_questions=manual(2))
        original = self.core.batch(created['id'])['questions']
        runtime = SharingRuntime(SimpleNamespace(service=object()), lambda: self.learning)
        with patch('lan_bitable_template_portal.learning_sharing_routes.read_material_link', side_effect=LearningError('暂时无法读取资料')):
            asyncio.run(runtime.generate(self.core.batch(created['id'])))
        self.assertEqual(self.core.batch(created['id'])['status'], 'failed')
        self.assertEqual(self.core.batch(created['id'])['questions'], original)
        with patch('lan_bitable_template_portal.learning_sharing_routes.read_material_link', return_value={
                'title': 'Manual material', 'text': fixture.SOURCE}):
            asyncio.run(runtime.generate(self.core.batch(created['id'])))
        self.assertEqual(self.core.batch(created['id'])['status'], 'draft')
        self.assertEqual(self.core.batch(created['id'])['questions'], original)

    def test_old_failed_batch_uses_explicitly_selected_people_only(self):
        people = self.core.batch(self.bid)['participants'][:1]
        with self.learning.transaction() as conn:
            batch = self.core.batch(self.bid, conn)
            batch.update(status='failed', participants=[], requested_participants=people)
            self.learning._put('sharing_batch', self.bid, batch, conn, False)
        self.assertEqual(len(self.core.detail(self.bid, fixture.ADMIN)['participants']), 1)
        updated = self.core.edit(self.bid, {'version': 1, 'questions': manual(5)}, fixture.ADMIN)
        self.assertEqual(updated['participants'][0]['open_id'], people[0]['open_id'])
        with self.assertRaises(LearningError):
            self.core.detail(self.bid, {'id': 'ou_other'})

    def test_large_upload_spools_and_sharing_parser_only_waives_byte_limit(self):
        self.enterContext(patch('lan_bitable_template_portal.sharing_material_images.extract_images', return_value=[]))
        data = b'x' * (21 * 1024 * 1024)
        stream = io.BytesIO(data)
        item = SharingMaterials(self.learning).upload(fixture.ADMIN, 'large.pdf', stream)
        stored = self.learning._get('sharing_material', item['id'])
        self.assertEqual(item['size'], len(data))
        self.assertEqual(stored['sha256'], hashlib.sha256(data).hexdigest())
        with self.assertRaises(AssistantError):
            checked_file(stored)
        self.assertEqual(checked_file(stored, max_bytes=None), data)
        with patch('openclaw_service.assistant.lighthouse_document_tools.run_local', return_value={
                'text': fixture.SOURCE, 'extraction_complete': True}) as parser:
            SharingMaterials(self.learning).read(item['id'], fixture.ADMIN['id'])
        self.assertIsNone(parser.call_args.kwargs['max_bytes'])
        with self.assertRaises(LearningError):
            SharingMaterials(self.learning).upload(fixture.ADMIN, 'empty.pdf', b'')

    def test_image_only_docx_is_a_valid_source(self):
        replies = [{'document': {'title': 'Scene photos', 'revision_id': 1}}, {'content': ''},
                   {'items': [{'image': {'token': 'Scene'}}], 'has_more': False}, {'document': {'revision_id': 1}}]
        with patch('openclaw_service.assistant.lighthouse_documents.document_reader',
                   return_value=Mock(side_effect=[{'code': 0, 'data': data} for data in replies])):
            source = read_material_link(object(), fixture.URL)
        self.assertEqual(source['text'], '')
        self.assertEqual(source['images'][0]['token'], 'Scene')

    def test_scene_photo_has_no_twenty_megabyte_limit_or_mandatory_ocr(self):
        from PIL import Image
        buffer = io.BytesIO()
        Image.new('RGB', (100, 80), 'red').save(buffer, 'JPEG')
        raw = buffer.getvalue() + b'\0' * (21 * 1024 * 1024)
        self.assertTrue(jpeg(raw).startswith(b'\xff\xd8'))
        store = SharingImages(self.learning, object())
        reference = {'token': 'Scene', 'source_url': fixture.URL, 'revision': 1, 'label': 'Scene photo'}
        stream = io.BytesIO(raw)
        with patch.object(store, 'download', return_value=stream), \
             patch('lan_bitable_template_portal.sharing_sources.cloud_reader', return_value=(None, 'Doc', 'docx',
                   lambda _: {'document': {'revision_id': 1}})), \
             patch('openclaw_service.assistant.lighthouse_files.recognize_text') as ocr:
            images = store.prepare([reference], ocr=False)
        ocr.assert_not_called()
        self.assertTrue(stream.closed)
        self.assertEqual(images[0]['id'], hashlib.sha256(raw).hexdigest())


class ModelRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = model_fixture.SharingImageModelTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_empty_generation_tries_another_configured_model_without_setting_changes(self):
        f = self.fixture
        f.transport.request_json.side_effect = [model_fixture.response({}), model_fixture.response({'questions': [model_fixture.question()]})]
        values = lighthouse_sharing.generate(f.model, {'source': fixture.SOURCE, 'count': 1})
        self.assertEqual(len(values['questions']), 1)
        models = [c.kwargs['json_payload']['model'] for c in f.transport.request_json.call_args_list]
        self.assertEqual(models, ['WanWu/GLM-Auto', 'WanWu/MiniMax-Auto'])
        self.assertNotIn('image_url', str(f.transport.request_json.call_args_list))
        f.store.put_document.assert_not_called()

    def test_scene_only_generation_and_empty_vision_retry(self):
        f = self.fixture
        f.image['ocr_text'] = ''
        f.transport.request_json.side_effect = [model_fixture.response({'text': ''}), model_fixture.response({'text': model_fixture.VISUAL})]
        images = f.recognize()
        self.assertEqual(images[0]['mode'], 'vision')
        f.transport.request_json.side_effect = None
        f.transport.request_json.return_value = model_fixture.response({'questions': [model_fixture.question(image_ids=[f.image['id']], evidence=model_fixture.VISUAL)]})
        values = lighthouse_sharing.generate(f.model, {'source': '', 'source_images': images, 'count': 1})
        self.assertEqual(len(values['questions']), 1)
        with self.assertRaises(AssistantError):
            lighthouse_sharing.generate(f.model, {'source': '', 'count': 1})


if __name__ == '__main__':
    unittest.main()

"""Offline image evidence, durable bytes and per-paper access regression."""
import asyncio
import copy
import hashlib
import io
import json
import socket
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from bin import test_learning_sharing as fixture
from bin.test_learning_sharing_materials import local_only
from lan_bitable_template_portal.learning import LearningError
from lan_bitable_template_portal.learning_sharing import validate_questions, exercise_card, evidence_text
from lan_bitable_template_portal.learning_sharing_routes import SharingRuntime
from lan_bitable_template_portal.sharing_images import SharingImages, document_images, jpeg, public_images
from lan_bitable_template_portal.sharing_sources import read_material_link


def picture():
    from PIL import Image
    output = io.BytesIO()
    Image.new('RGB', (100, 80), 'red').save(output, 'PNG')
    return output.getvalue()


class SharingImageTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket.connect', local_only))
        self.fixture = fixture.SharingTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.learning, self.core, self.bid = self.fixture.learning, self.fixture.core, self.fixture.bid
        self.learning._portal_url = lambda: 'https://fixture.invalid'
        self.raw = picture()
        self.store = SharingImages(self.learning, object())
        self.image = {'id': hashlib.sha256(self.raw).hexdigest(), 'sha256': hashlib.sha256(jpeg(self.raw)).hexdigest(),
                      'source_url': fixture.URL, 'revision': 1, 'token': 'PrivateToken', 'block_id': 'Block1',
                      'label': '材料图1', 'text': '图中冷机冷却水状态显示为否，水泵运行频率为33HZ。', 'mode': 'ocr'}
        self.store.save(self.image['sha256'], jpeg(self.raw))

    def draft(self, *, mode='ocr', confirmed=False):
        image = {**self.image, 'mode': mode}
        qs = fixture.questions(5)
        qs[0].update(image_ids=[image['id']], evidence=image['text'], image_confirmed=confirmed)
        qs = validate_questions(qs, fixture.SOURCE, draft=True, images=[image])
        with self.learning.transaction() as conn:
            batch = self.core.batch(self.bid, conn)
            batch.update(questions=qs, source={'text': fixture.SOURCE, 'images': [image],
                'sources': [{'text': fixture.SOURCE, 'images': [image]}]})
            self.learning._put('sharing_batch', self.bid, batch, conn)
        return qs

    def test_evidence_normalizes_layout_not_facts_and_requires_known_image(self):
        self.assertNotEqual(evidence_text('两个显示值3 0请核对'), evidence_text('两个显示值30请核对'))
        self.assertEqual(evidence_text('间隔 30 分钟'), evidence_text('间隔30分钟'))
        qs = fixture.questions(5)
        qs[0]['evidence'] = '运行参数核验后\n发现冷却水压差水流开关内部细小水流孔脏堵'
        self.assertEqual(len(validate_questions(qs, fixture.SOURCE)), 5)
        qs[0].update(image_ids=[self.image['id']], evidence=self.image['text'])
        self.assertEqual(validate_questions(qs, fixture.SOURCE, images=[self.image])[0]['image_ids'], [self.image['id']])
        for evidence, ids in [('水泵已经停止运行且运行频率为零', [self.image['id']]),
                              (self.image['text'], []), (self.image['text'], ['0' * 64]),
                              ('        ', [self.image['id']]), (self.image['text'], [self.image['id']] * 2)]:
            with self.subTest(evidence=evidence, ids=ids):
                qs[0].update(evidence=evidence, image_ids=ids)
                with self.assertRaises(LearningError):
                    validate_questions(qs, fixture.SOURCE, images=[self.image])

    def test_visual_basis_requires_explicit_review_and_publish_does_not_send_yet(self):
        qs = self.draft(mode='vision')
        with self.assertRaisesRegex(LearningError, '对照原图'):
            self.fixture.publish()
        self.assertEqual(self.core.attempts(self.bid), [])
        self.assertEqual(self.learning._all('sharing_delivery'), [])
        qs[0]['image_confirmed'] = True
        self.core.edit(self.bid, {'version': 1, 'questions': qs}, fixture.ADMIN)
        self.core.publish(self.bid, 2, fixture.ADMIN, 'cli_bot')
        with self.assertRaises(LearningError):
            self.core.edit(self.bid, {'version': 3, 'questions': qs}, fixture.ADMIN)
        attempt = self.core.attempts(self.bid)[0]
        card = json.dumps(exercise_card(self.core.batch(self.bid), attempt), ensure_ascii=False)
        self.assertIn('查看第', card)
        self.assertIn('batch_id=' + self.bid, card)
        self.assertNotIn('PrivateToken', card)
        self.assertNotIn(self.image['text'], card)

    def test_original_only_for_admin_or_assigned_question_no_answers_or_tokens(self):
        self.draft()
        self.fixture.publish()
        admin = self.core.detail(self.bid, fixture.ADMIN)
        self.assertEqual(admin['source_images'][0]['text'], self.image['text'])
        self.assertNotIn('PrivateToken', json.dumps(admin))
        attempt = self.core.attempts(self.bid)[0]
        actor = {'id': attempt['open_id']}
        detail = self.core.detail(self.bid, actor)
        rendered = json.dumps(detail)
        self.assertNotIn(self.image['text'], rendered)
        self.assertNotIn('PrivateToken', rendered)
        self.assertNotIn('source_images', detail)
        self.assertEqual(self.core.image(self.bid, self.image['id'], actor)['id'], self.image['id'])
        for denied in ({'id': 'ou_other'}, {'id': 'ou_duty', 'shared_account': True, 'scope': 'A'}):
            with self.assertRaises(LearningError):
                self.core.image(self.bid, self.image['id'], denied)
        with self.learning.transaction() as conn:
            attempt['question_ids'] = [qid for qid in attempt['question_ids'] if qid != 'q1']
            self.learning._put('sharing_attempt', attempt['id'], attempt, conn)
        with self.assertRaises(LearningError):
            self.core.image(self.bid, self.image['id'], actor)
        with self.assertRaises(LearningError):
            self.core.image(self.bid, '0' * 64, fixture.ADMIN)
        self.core.delete(self.bid, 2, fixture.ADMIN)
        with self.assertRaises(LearningError):
            self.core.image(self.bid, self.image['id'], fixture.ADMIN)

    def test_cache_restart_recovery_checksum_and_path_guards(self):
        restarted = SharingImages(self.learning, object())
        with patch.object(restarted, 'download', side_effect=AssertionError('No network')):
            self.assertEqual(restarted.content(self.image), jpeg(self.raw))
        restarted.path(self.image['sha256']).unlink()
        with patch.object(restarted, 'download', return_value=self.raw) as fetch:
            self.assertEqual(restarted.content(self.image), jpeg(self.raw))
            self.assertEqual(restarted.content(self.image), jpeg(self.raw))
            fetch.assert_called_once()
        restarted.path(self.image['sha256']).write_bytes(b'broken-cache')
        with patch.object(restarted, 'download', return_value=b'changed-source'):
            with self.assertRaisesRegex(LearningError, '已变化'):
                restarted.content(self.image)
        for bad in ('../secrets', '', 'a' * 65, None):
            with self.assertRaises(LearningError): restarted.path(bad)
        for bad in (b'', b'<html>error</html>', b'not an image'):
            with self.assertRaises(LearningError): jpeg(bad)

    def test_prepare_uses_existing_ocr_and_checks_document_revision(self):
        refs = document_images([{'block_id': 'Block1', 'image': {'token': 'ImageToken'}}], fixture.URL, 1, '材料')
        getter = Mock(return_value={'document': {'revision_id': 1}})
        with patch.object(self.store, 'download', return_value=self.raw), \
             patch('openclaw_service.assistant.lighthouse_files.recognize_text', return_value=self.image['text']) as ocr, \
             patch('lan_bitable_template_portal.sharing_sources.cloud_reader', return_value=(fixture.URL, 'TestMeeting', 'docx', getter)):
            values = self.store.prepare(refs * 2)
            self.assertEqual(len(values), 1)
            self.assertEqual(values[0]['mode'], 'ocr')
            ocr.assert_called_once()
            self.assertTrue(self.store.model_input(values[0])['data_url'].startswith('data:image/jpeg;base64,'))
            getter.return_value = {'document': {'revision_id': 2}}
            with self.assertRaisesRegex(LearningError, '材料已变化'):
                self.store.prepare(refs)

    def test_image_order_and_clear_references_survive_save(self):
        first, second = self.image, {**self.image, 'id': 'b' * 64, 'label': '图2'}
        batch = {'id': self.bid, 'source': {'images': [first, second]}}
        self.assertEqual([item['id'] for item in public_images(batch, [second['id'], first['id']])], [second['id'], first['id']])
        qs = self.draft()
        qs[0].update(image_ids=[], image_confirmed=False, evidence=fixture.questions()[0]['evidence'])
        saved = self.core.edit(self.bid, {'version': 1, 'questions': qs}, fixture.ADMIN)
        self.assertFalse(saved['questions'][0].get('image_ids'))
        self.assertFalse(saved['questions'][0].get('images'))

    def test_docx_images_are_collected_with_paginated_source_snapshot(self):
        replies = [{'document': {'title': '材料', 'revision_id': 3}}, {'content': fixture.SOURCE},
                   {'items': [{'image': {'token': 'First'}}], 'has_more': True, 'page_token': 'next'},
                   {'items': [{'image': {'token': 'Second'}}], 'has_more': False},
                   {'document': {'revision_id': 3}}]
        with patch('openclaw_service.assistant.lighthouse_documents.document_reader', return_value=Mock(side_effect=[{'code': 0, 'data': r} for r in replies])):
            source = read_material_link(object(), fixture.URL)
        self.assertEqual([i['token'] for i in source['images']], ['First', 'Second'])
        self.assertTrue(all(i['revision'] == 3 for i in source['images']))
        with self.assertRaises(LearningError): document_images([{'image': {'token': '../outside'}}], fixture.URL, 1, 'x')
        with self.assertRaises(LearningError): document_images([{'image': {'token': 'Valid'}}] * 25, fixture.URL, 1, 'x')

    def test_generation_binds_images_and_discards_only_ungrounded_questions(self):
        qs = fixture.questions(7)
        qs[0].update(evidence=self.image['text'], image_ids=[self.image['id']], image_confirmed=True)
        qs[4]['evidence'] = '这是材料中不存在的错误依据不能发布'
        qs[6].update(type='multiple', correct=[0, 2])
        source = {'text': fixture.SOURCE, 'title': '有图材料', 'participants': [], 'images': [self.image]}
        async def generate(*args, **kwargs):
            if kwargs.get('mode') == 'image_sources':
                return {'images': [{'id': self.image['id'], 'text': self.image['text'], 'mode': 'vision'}]}
            for q, kind in zip(qs[:5] if not args[2] else qs[5:], kwargs['question_types']):
                q.update(type=kind, correct=[0] if kind == 'single' else [0, 2])
            return qs[:5] if not args[2] else qs[5:]
        runtime = SharingRuntime(SimpleNamespace(service=object(), generate_sharing_questions=generate), lambda: self.learning)
        with patch('lan_bitable_template_portal.learning_sharing_routes.read_source', return_value=source), \
             patch.object(SharingImages, 'prepare', return_value=[copy.deepcopy(self.image)]):
            asyncio.run(runtime.generate(self.core.batch(self.bid)))
        batch = self.core.batch(self.bid)
        self.assertEqual(batch['status'], 'draft', batch.get('error'))
        self.assertEqual(len(batch['questions']), 6)
        self.assertEqual(batch['source']['images'][0]['mode'], 'vision')
        self.assertFalse(batch['questions'][0]['image_confirmed'], 'Model cannot self-approve visual evidence')
        self.assertIn('1道题经自动修订仍未通过校验', batch['generation_note'])
        self.assertEqual(self.core.attempts(self.bid), [])

    def test_image_http_endpoint_checks_login_membership_and_post_recovery_revocation(self):
        from fastapi import FastAPI
        from fastapi.responses import JSONResponse
        from fastapi.testclient import TestClient
        from lan_bitable_template_portal.learning_sharing_routes import install
        self.draft()
        self.fixture.publish()
        attempt = self.core.attempts(self.bid)[0]
        actor = {'id': attempt['open_id']}
        logged_in = [True]
        def identity(request):
            return (object(), actor) if logged_in[0] else (None, None)
        controller = SimpleNamespace(_auth_required_response=lambda: JSONResponse({'ok': False}, status_code=401))
        runtime = SimpleNamespace(service=object())
        app = FastAPI()
        install(app, controller, runtime, lambda: self.learning, identity, AsyncMock(), Mock(),
            lambda exc: JSONResponse({'ok': False, 'error': str(exc)}, status_code=exc.status if isinstance(exc, LearningError) else 500))
        runtime.sharing_exercises.core = self.core
        client = TestClient(app)
        self.addCleanup(client.close)
        url = f'/api/learning/sharing/{self.bid}/images/{self.image["id"]}'
        response = client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['cache-control'], 'private, no-store')
        self.assertEqual(response.content, jpeg(self.raw))
        actor['id'] = 'ou_outsider'
        self.assertEqual(client.get(url).status_code, 403)
        actor['id'] = attempt['open_id']
        logged_in[0] = False
        self.assertEqual(client.get(url).status_code, 401)
        logged_in[0] = True
        def revoke(_store, _item):
            logged_in[0] = False
            return jpeg(self.raw)
        with patch.object(SharingImages, 'content', revoke):
            self.assertEqual(client.get(url).status_code, 401)

    def test_cloud_question_metadata_keeps_image_reference_without_binary_or_private_token(self):
        from lan_bitable_template_portal.learning_cloud import LearningCloud, META_FIELD
        self.draft()
        self.fixture.publish()
        batch = self.core.batch(self.bid)
        cloud = LearningCloud(enabled=True)
        cloud._sharing_schema_ready = True
        def request(method, url, payload, params):
            self.assertEqual(method, 'POST')
            fields = payload['fields']
            meta = json.loads(fields[META_FIELD])
            self.assertEqual(meta['image_ids'], [self.image['id']])
            self.assertIn('题图（登录后查看）', fields['备注'])
            self.assertNotIn('PrivateToken', json.dumps(fields))
            self.assertNotIn('base64', json.dumps(fields))
            return {'record': {'record_id': 'recFixture', 'fields': fields}}
        with patch.object(cloud, '_require_write'), patch.object(cloud, '_list_all', return_value=[]), patch.object(cloud, '_request', side_effect=request):
            self.assertEqual(cloud.save_sharing_question(batch, batch['questions'][0], 'fixture-operation'), 'recFixture')


if __name__ == '__main__':
    unittest.main()

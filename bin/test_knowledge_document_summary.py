"""Summary grounding, durable retry and source ACL regression without cloud calls."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import uuid

from bin.test_lighthouse_knowledge import FakeEmbedder, FakeMemoryIndex, actor
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_knowledge import KnowledgeBase
from openclaw_service.assistant.lighthouse_knowledge_summary import summarize, summary_parts


class SummaryModelTests(unittest.TestCase):
    def model(self, answers):
        model = Mock()
        model.settings.return_value = {'enabled': True, 'configured': True, 'active_model_id': 'one',
            'models': [{'id': 'one', 'configured': True}, {'id': 'two', 'configured': True}]}
        model.profile.side_effect = lambda identity: {'id': identity or 'one'}
        model.complete.side_effect = answers
        return model

    def test_text_only_grounded_summary_with_source_quotes(self):
        model = self.model([json.dumps({'summary': 'AHU-1设备每天检查2次。', 'evidence': ['AHU-1设备每天检查2次。']})])
        result = summarize(model, 'AHU-1设备每天检查2次。', '说明.pdf', '第1页')
        self.assertIn('AHU-1', result)
        messages = model.complete.call_args.args[0]
        self.assertEqual([m['role'] for m in messages], ['system', 'user'])
        self.assertTrue(all(isinstance(m['content'], str) for m in messages))
        self.assertLessEqual(model.complete.call_args.kwargs['max_tokens'], 3200)

    def test_bad_quotes_or_numbers_fall_back_to_another_existing_model(self):
        for invalid in ({'summary': '温度30度', 'evidence': ['不在原文']},
                        {'summary': '温度999度', 'evidence': ['温度30度']}):
            with self.subTest(invalid=invalid):
                valid = {'summary': '温度30度', 'evidence': ['温度30度']}
                model = self.model([json.dumps(invalid), json.dumps(valid)])
                self.assertIn('温度30度', summarize(model, '温度30度', '说明.txt', '段落1'))
                self.assertEqual(model.complete.call_count, 2)
                self.assertEqual(model.complete.call_args.kwargs['profile']['id'], 'two')

    def test_unapproved_model_does_not_receive_source_content(self):
        model = self.model([])
        with self.assertRaisesRegex(AssistantError, '文档摘要'):
            summarize(model, '设备检查', '说明.txt', '段落1', allowed=lambda profile: False)
        model.complete.assert_not_called()

    def test_missing_model_or_all_failures_are_explicit(self):
        model = self.model([RuntimeError('private provider error')] * 2)
        with self.assertRaisesRegex(AssistantError, '原件已保留'):
            summarize(model, '设备检查', '说明.txt', '段落1')
        model.settings.return_value['configured'] = False
        model.complete.reset_mock()
        with self.assertRaisesRegex(AssistantError, '未配置'):
            summarize(model, '设备检查', '说明.txt', '段落1')
        model.complete.assert_not_called()

    def test_short_paragraphs_are_batched_and_long_files_are_not_truncated(self):
        sections = [{'text': str(i) + '设备说明' * 20, 'location': '第' + str(i) + '段'} for i in range(100)]
        result = list(summary_parts(sections))
        self.assertLess(len(result), 5)
        self.assertTrue(all(len(part['text']) <= 6000 for part in result))
        self.assertEqual(''.join(part['text'] for part in result).replace('\n', ''), ''.join(s['text'] for s in sections))
        self.assertIn('第99段', result[-1]['location'])


class SummaryStorageTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('No network')))
        self.enterContext(patch('openclaw_service.assistant.lighthouse_knowledge._load_local', return_value=None))
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.current = True
        self.summary = Mock(return_value='设备检查摘要。原文依据：设备每班检查。')
        self.kb = self.open()
        self.source = {'table_key': 'manuals', 'record_id': 'recOne', 'origin': 'fixture',
            'record_revision': 'r1', 'config_revision': 'c1', 'scopes': ['A'], 'kind': 'attachment'}
        self.key = 'manuals/recOne/file'

    def open(self):
        kb = KnowledgeBase(self.root, embedder=FakeEmbedder(), memory_index_factory=FakeMemoryIndex,
            source_summarizer=self.summary, source_acl=lambda user, source: self.current and user['id'] == 'u1',
            source_current=lambda source: self.current, start_worker=False)
        self.addCleanup(kb.close)
        return kb

    def add(self, text='设备每班检查。'):
        self.identity = self.kb.upsert_source(self.key, self.source, '说明.txt', text.encode())['id']
        self.kb.process_one()

    def row(self):
        with self.kb.connect(readonly=True) as db:
            return dict(db.execute('SELECT * FROM documents WHERE id=?', (self.identity,)).fetchone())

    def retry(self):
        with self.kb.connect() as db:
            db.execute('UPDATE source_retry SET next_attempt=0')
        self.kb.process_one()

    def test_original_and_summary_are_cached_and_only_summary_is_embedded(self):
        self.add()
        self.assertEqual(self.row()['status'], 'ready')
        with self.kb.connect(readonly=True) as db:
            cached = dict(db.execute('SELECT * FROM document_analysis').fetchone())
        self.assertEqual(cached['text'], '设备每班检查。')
        self.assertEqual(cached['summary'], self.summary.return_value)
        indexed = self.kb.document(actor(), self.identity, {})['sections']
        self.assertTrue(all('模型摘要' in s['location'] for s in indexed))
        self.assertTrue(all(self.summary.return_value == text for texts, _, query in self.kb.embedder.calls if not query for text in texts))
        self.assertEqual(self.kb.file(actor(), self.identity)[0].read_bytes(), '设备每班检查。'.encode())

    def test_embedding_retry_after_restart_reuses_completed_model_summary(self):
        self.kb.embedder.fail = True
        self.add()
        self.assertEqual(self.row()['status'], 'failed')
        self.assertIsNone(self.row()['active_version'])
        self.kb.close()
        self.kb = self.open()
        self.retry()
        self.assertEqual(self.row()['status'], 'ready')
        self.summary.assert_called_once()

    def test_summary_failure_preserves_previous_version_and_original(self):
        self.add()
        first = self.row()['active_version']
        self.summary.side_effect = AssistantError('文档摘要暂未完成。', 503)
        self.source['record_revision'] = 'r2'
        self.add('设备更新后检查。')
        self.assertEqual((self.row()['status'], self.row()['active_version']), ('failed', None))
        with self.kb.connect(readonly=True) as db:
            original = db.execute('SELECT path FROM versions WHERE document_id=? AND version=?', (self.identity, first)).fetchone()
        self.assertEqual((self.kb.root / original[0]).read_bytes(), '设备每班检查。'.encode())
        self.assertTrue(self.row()['pending_version'])
        self.assertIn('文档摘要', self.row()['error'])
        self.summary.side_effect = None
        self.retry()
        self.assertEqual(self.row()['status'], 'ready')
        self.assertGreater(self.row()['active_version'], first)

    def test_sensitive_source_never_sent_to_summary_model(self):
        self.add('password: very-secret-password')
        self.summary.assert_not_called()
        self.assertEqual(self.row()['status'], 'blocked')
        with self.kb.connect(readonly=True) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM document_analysis').fetchone()[0], 0)

    def test_source_removed_while_summarizing_does_not_recreate_index_or_cache(self):
        def remove(*args):
            self.current = False
            self.kb.remove_sources('manuals/')
            return '过期摘要'
        self.summary.side_effect = remove
        self.add()
        with self.kb.connect(readonly=True) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM documents').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT count(*) FROM document_analysis').fetchone()[0], 0)

    def test_record_metadata_and_manual_upload_keep_original_pipeline(self):
        self.source['kind'] = 'record'
        self.add()
        self.kb.upload(actor(), '手动.txt', '手动知识库原文'.encode())
        self.kb.process_one()
        self.summary.assert_not_called()

    def test_acl_and_source_removal_cover_both_summary_and_original(self):
        self.add()
        with self.assertRaises(AssistantError):
            self.kb.document(actor('other'), self.identity, {})
        self.current = False
        self.kb.remove_sources('manuals/')
        with self.kb.connect(readonly=True) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM document_analysis').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT count(*) FROM local_vectors').fetchone()[0], 0)

    def test_explicit_reindex_uses_same_summary_without_new_version(self):
        self.add()
        self.kb.reindex_source(self.key, self.source, str(uuid.uuid4()))
        self.kb.process_one()
        self.summary.assert_called_once()
        self.assertEqual(self.row()['active_version'], 1)

    def test_failure_mid_document_reuses_completed_parts(self):
        self.summary.side_effect = ['第一段摘要', AssistantError('文档摘要失败。', 503)]
        self.add('设备检查说明。' * 1700)
        self.assertEqual(self.row()['status'], 'failed')
        with self.kb.connect(readonly=True) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM document_analysis').fetchone()[0], 1)
        self.summary.reset_mock()
        self.summary.side_effect = None
        self.retry()
        self.assertEqual(self.row()['status'], 'ready')
        self.assertEqual(self.summary.call_count, 2)

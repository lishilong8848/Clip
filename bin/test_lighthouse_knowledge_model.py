import hashlib
import json
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch, Mock
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant import lighthouse_knowledge_model as assets
from openclaw_service.assistant import lighthouse_distribution as dist
from openclaw_service.assistant import lighthouse_knowledge_local as local
from bin.test_lighthouse_distribution import FakeFeishuClient


class KnowledgeModelTests(TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        self.contents = {'config.json': b'fixture config', 'model_optimized.onnx': b'fixture model'}
        self.enterContext(patch.object(assets, 'FILES', {name: hashlib.sha256(data).hexdigest() for name, data in self.contents.items()}))
        self.enterContext(patch.object(dist, 'PART_BYTES', 100))
        self.enterContext(patch.object(assets, 'SPEC', self.root / 'spec.json'))
        for name, data in self.contents.items(): (self.source / name).write_bytes(data)
        (self.source / 'private.txt').write_text('never distribute')
        self.spec = assets.create_archive(self.source, self.root / 'model.zip')
        for part in self.spec['parts']:
            part.update(record_id='rec' + str(part['index']), file_token='file' + str(part['index']))
        assets.SPEC.write_text(json.dumps(self.spec))
        self.downloads = []
        self.fail_part = None
        def download(part, target):
            self.downloads.append(part['index'])
            if part['index'] == self.fail_part: raise OSError('fixture network interruption')
            shutil.copyfile(self.root / part['name'], target)
        self.mirror = Mock(download_part=Mock(side_effect=download))
        self.factory = Mock(return_value=self.mirror)

    def test_archive_contains_only_verified_model_and_is_deterministic(self):
        with zipfile.ZipFile(self.root/'model.zip') as bundle:
            self.assertEqual(set(bundle.namelist()), set(assets.FILES))
        second = assets.create_archive(self.source, self.root / 'second.zip')
        self.assertEqual(self.spec['sha256'], second['sha256'])
        (self.source/'config.json').write_bytes(b'corrupt')
        with self.assertRaises(ValueError): assets.create_archive(self.source, self.root/'bad.zip')

    def test_concurrent_install_and_offline_reuse(self):
        models = self.root/'models'
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: assets.install_model(models, mirror_factory=self.factory), range(4)))
        self.assertEqual(len(set(results)), 1)
        self.assertEqual(self.downloads, list(range(1, len(self.spec['parts'])+1)))
        self.factory.assert_called_once()
        for name, data in self.contents.items(): self.assertEqual((results[0]/name).read_bytes(), data)
        with patch.object(dist, 'FeishuRuntimeMirror', side_effect=AssertionError('must stay offline')):
            self.assertEqual(assets.install_model(models), results[0])

    def test_failed_download_resumes_without_repeating_verified_parts(self):
        self.fail_part = 2
        with self.assertRaises(OSError): assets.install_model(self.root/'models', mirror_factory=self.factory)
        self.assertFalse((self.root/'models/bge-small-zh-v1.5').exists())
        self.fail_part = None
        assets.install_model(self.root/'models', mirror_factory=self.factory)
        self.assertEqual(self.downloads.count(1), 1)
        self.assertEqual(self.downloads.count(2), 2)
        self.assertFalse(list((self.root/'models').rglob('*.tmp')))

    def test_tampered_download_is_not_installed(self):
        self.mirror.download_part.side_effect = lambda part, target: Path(target).write_bytes(b'bad')
        with self.assertRaisesRegex(Exception, '校验失败'):
            assets.install_model(self.root/'models', mirror_factory=self.factory)
        self.assertFalse((self.root/'models/bge-small-zh-v1.5').exists())

    def test_model_manifest_cannot_replace_runtime_or_use_other_tables(self):
        dist.validate_spec(self.spec, component='knowledge_model')
        with self.assertRaises(Exception): dist.validate_spec(self.spec)
        with self.assertRaises(Exception): dist.validate_spec({**self.spec, 'table': 'other'}, component='knowledge_model')

    def test_publisher_only_adds_model_parts_and_reuses_journal(self):
        client = FakeFeishuClient()
        mirror = dist.FeishuRuntimeMirror(client=client, headers={})
        journal = self.root/'journal.json'
        mirror.publish(self.spec, self.root, journal, component='knowledge_model')
        first = len(client.records)
        mirror.publish(self.spec, self.root, journal, component='knowledge_model')
        self.assertEqual(len(client.records), first)
        for record in client.records.values():
            self.assertEqual(record['fields']['组件名称'], '知识库模型 bge-small-zh-v1.5')
        self.assertFalse(any(call[1] == 'DELETE' for call in client.calls))

    def test_verified_local_model_load_never_downloads(self):
        with patch('fastembed.TextEmbedding') as factory, patch.object(assets, 'verify_model', return_value=self.source), \
                patch.object(assets, 'install_model') as install:
            runtime = local.LocalEmbeddings(self.root/'runtime', start_worker=False)
            self.assertIs(runtime._load_model(), factory.return_value)
            install.assert_not_called()
            self.assertTrue(factory.call_args.kwargs['local_files_only'])
            self.assertEqual(factory.call_args.kwargs['specific_model_path'], str(self.source))
            self.assertEqual(factory.call_args.kwargs['threads'], 1)

    def test_missing_model_uses_feishu_not_huggingface(self):
        with patch('fastembed.TextEmbedding') as factory, patch.object(assets, 'verify_model', side_effect=FileNotFoundError), \
                patch.object(assets, 'install_model', return_value=self.source) as install:
            runtime = local.LocalEmbeddings(self.root/'runtime', start_worker=False)
            runtime._load_model()
            install.assert_called_once_with(runtime.model_dir)
            factory.assert_called_once()
            self.assertTrue(factory.call_args.kwargs['local_files_only'])

    def test_first_document_waits_for_model_preparation_but_queries_stay_bounded(self):
        class Model:
            def embed(self, texts, **kwargs): return [[1.0] * 512 for _ in texts]
        def slow_model():
            time.sleep(.4)
            return Model()
        with patch.object(local, '_MODEL_PREPARE_ALLOWANCE', 2):
            runtime = local.LocalEmbeddings(self.root/'slow-doc', model_factory=slow_model, doc_timeout=.2)
            try:
                self.assertEqual(len(runtime.embed(['fixture'])[0]), 512)
            finally:
                runtime.close()
            runtime = local.LocalEmbeddings(self.root/'slow-query', model_factory=slow_model, query_timeout=.05)
            try:
                with self.assertRaisesRegex(Exception, '超时'):
                    runtime.embed(['fixture'], query=True)
            finally:
                runtime.close()

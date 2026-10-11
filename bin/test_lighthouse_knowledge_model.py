import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch, Mock

from bin.openclaw_service.assistant import lighthouse_knowledge_model as assets
from bin.openclaw_service.assistant.lighthouse_knowledge_local import LocalEmbeddings


class KnowledgeModelTests(TestCase):
    def test_bundle_pinned_assets_only_and_detect_corruption(self):
        with TemporaryDirectory() as tmp, patch.object(assets, 'FILES', {'config.json': hashlib.sha256(b'fixture').hexdigest()}):
            source, dest = Path(tmp)/'source', Path(tmp)/'dest'
            module = source/'bin/openclaw_service/assistant/lighthouse_knowledge_local.py'
            module.parent.mkdir(parents=True)
            module.write_text('')
            model = source/assets.BUNDLE
            model.mkdir(parents=True)
            (model/'config.json').write_bytes(b'fixture')
            (model/'private.txt').write_bytes(b'not a model')
            self.assertEqual(assets.bundle_model(source, dest), [assets.BUNDLE/'config.json'])
            self.assertFalse((dest/assets.BUNDLE/'private.txt').exists())
            (model/'config.json').write_bytes(b'broken')
            with self.assertRaises(ValueError): assets.bundle_model(source, dest)

    def test_local_cache_is_used_without_online_load(self):
        with TemporaryDirectory() as tmp, patch('fastembed.TextEmbedding') as factory, \
                patch('bin.openclaw_service.assistant.lighthouse_knowledge_local.Path.is_dir', return_value=False):
            runtime = LocalEmbeddings(tmp, start_worker=False)
            self.assertIs(runtime._load_model(), factory.return_value)
            factory.assert_called_once()
            self.assertTrue(factory.call_args.kwargs['local_files_only'])
            self.assertEqual(factory.call_args.kwargs['threads'], 1)

    def test_missing_cache_downloads_but_error_is_actionable(self):
        with TemporaryDirectory() as tmp, patch('fastembed.TextEmbedding', side_effect=[ValueError('no cache'), OSError('offline')]), \
                patch('bin.openclaw_service.assistant.lighthouse_knowledge_local.Path.is_dir', return_value=False):
            runtime = LocalEmbeddings(tmp, start_worker=False)
            with self.assertRaisesRegex(Exception, '完整补丁'):
                runtime._load_model()

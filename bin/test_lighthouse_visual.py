"""Raster fixtures and mocked offline OCR only; no models, Docker or cloud writes."""
import copy
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image, PngImagePlugin

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_files import FILE_NAMESPACE, LighthouseFiles
from openclaw_service.assistant import lighthouse_visual as visual

ACTOR = {'id': 'visual-fixture', 'scopes': ['D', 'E']}


class Store:
    def __init__(self, path):
        self.db_path, self.docs = path, {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)


def png(size=(80, 180), color='white', **options):
    with Image.new('RGBA', size, color) as image:
        output = io.BytesIO()
        image.save(output, format='PNG', **options)
        return output.getvalue()


class VisualTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.files = LighthouseFiles(Store(self.root / 'state.sqlite3'))
        self.actor = copy.deepcopy(ACTOR)

    def upload(self, content=None, name='screenshot.png', scopes=None, actor=None):
        return self.files.upload(actor or self.actor, name, content if content is not None else png(),
                                 extract=False, source_scopes=scopes or ['D'])['id']

    def test_status_explicitly_does_not_enable_vision(self):
        status = visual.status()
        self.assertTrue(status['available'])
        self.assertFalse(status['vision_enabled'])
        self.assertFalse(status['external_model'])
        with patch.object(visual.importlib.util, 'find_spec', return_value=None):
            self.assertFalse(visual.status()['available'])

    def test_crop_and_overlapping_tiles_cover_original_coordinates_without_modifying_original(self):
        with Image.new('RGB', (80, 600), 'white') as image:
            for y in range(600):
                image.paste((y % 256, 10, 20), (0, y, 80, y + 1))
            output = io.BytesIO()
            image.save(output, format='PNG')
        original = output.getvalue()
        identity = self.upload(original)
        result = visual.preprocess(self.files, self.actor, identity, crop=[10, 20, 70, 580],
                                   tile_height=256, overlap=32)
        self.assertEqual([region['box'] for region in result['regions']],
                         [[10, 20, 70, 276], [10, 244, 70, 500], [10, 468, 70, 580]])
        self.assertEqual(result['oriented_size'], [80, 600])
        self.assertEqual(result['crop'], [10, 20, 70, 580])
        for output, region in zip(result['outputs'], result['regions']):
            with Image.open(io.BytesIO(output['content'])) as tile:
                self.assertEqual(list(tile.size), region['size'])
                self.assertEqual(tile.getpixel((0, 0)), (region['box'][1] % 256, 10, 20, 255))
        self.assertFalse(result['ocr']['requested'])
        self.assertEqual(len(self.files.store.docs), 1, 'preprocessing must not publish before fresh authorization')
        self.assertEqual(Path(self.files.get(self.actor, identity)['path']).read_bytes(), original)

    def test_final_tile_does_not_duplicate_already_covered_rows(self):
        identity = self.upload(png((10, 256)))
        result = visual.preprocess(self.files, self.actor, identity, tile_height=256, overlap=127)
        self.assertEqual(len(result['outputs']), 1)
        self.assertEqual(result['regions'][0]['box'], [0, 0, 10, 256])

    def test_exif_orientation_applied_before_crop_and_source_metadata_stripped(self):
        metadata = PngImagePlugin.PngInfo()
        metadata.add_text('private-note', 'DO_NOT_PROPAGATE')
        exif = Image.Exif()
        exif[274] = 6
        identity = self.upload(png((60, 100), pnginfo=metadata, exif=exif))
        result = visual.preprocess(self.files, self.actor, identity, crop=[50, 10, 90, 40])
        self.assertEqual(result['oriented_size'], [100, 60])
        with Image.open(io.BytesIO(result['outputs'][0]['content'])) as image:
            self.assertEqual(image.size, (40, 30))
            self.assertNotIn('private-note', image.info)
            self.assertNotIn(274, image.getexif())

    def test_authorization_scopes_topic_and_tamper_checked_before_decode(self):
        identity = self.upload()
        for actor in ({**self.actor, 'id': 'other', 'is_admin': True},
                      {**self.actor, 'scopes': ['E']},
                      {**self.actor, 'channel': 'feishu:chat', '_feishu_topic': {'id': 'other'}}):
            with patch.object(visual, '_raster') as load, self.assertRaises(AssistantError):
                visual.preprocess(self.files, actor, identity)
            load.assert_not_called()
        Path(self.files.get(self.actor, identity)['path']).write_bytes(b'changed')
        with patch.object(visual, '_raster') as load, self.assertRaises(AssistantError):
            visual.preprocess(self.files, self.actor, identity)
        load.assert_not_called()
        for identity in ('../private.png', 'C:\\private.png'):
            with self.assertRaises(AssistantError):
                visual.preprocess(self.files, self.actor, identity)

    def test_invalid_crop_tile_parameters_and_too_many_tiles_are_rejected(self):
        identity = self.upload(png((80, 1400)))
        options = [{'crop': [-1, 0, 10, 10]}, {'crop': [0, 0, 0, 10]},
                   {'crop': [0, 0, 100, 10]}, {'crop': [0, 0, 10, 1600]},
                   {'crop': [0, 0, 10.5, 10]}, {'crop': [False, 0, 10, 10]},
                   {'crop': '../path'}, {'tile_height': 0}, {'tile_height': True},
                   {'tile_height': 4097}, {'overlap': -1}, {'overlap': 512},
                   {'tile_height': 128, 'overlap': 128}, {'tile_height': 128, 'overlap': 0},
                   {'ocr': 'true'}]
        for option in options:
            with self.subTest(option=option), self.assertRaises(AssistantError):
                visual.preprocess(self.files, self.actor, identity, **option)

    def test_pixel_encoded_byte_and_total_output_limits(self):
        identity = self.upload()
        with patch.object(visual, 'MAX_PIXELS', 100), self.assertRaises(AssistantError) as raised:
            visual.preprocess(self.files, self.actor, identity)
        self.assertEqual(raised.exception.status, 413)
        with patch('openclaw_service.assistant.lighthouse_sandbox.MAX_FILE_BYTES', 10), self.assertRaises(AssistantError):
            visual.preprocess(self.files, self.actor, identity)
        with patch('openclaw_service.assistant.lighthouse_sandbox.MAX_OUTPUT_BYTES', 10), self.assertRaises(AssistantError):
            visual.preprocess(self.files, self.actor, identity)

    def test_animation_corrupt_raster_and_nonimage_are_not_silently_processed(self):
        output = io.BytesIO()
        with Image.new('RGB', (10, 10), 'red') as first, Image.new('RGB', (10, 10), 'blue') as second:
            first.save(output, format='GIF', save_all=True, append_images=[second])
        identity = self.upload(output.getvalue(), name='animation.gif')
        with self.assertRaises(AssistantError):
            visual.preprocess(self.files, self.actor, identity)
        identity = self.upload(b'not an image', name='fake.txt')
        with self.assertRaises(AssistantError):
            visual.preprocess(self.files, self.actor, identity)
        with self.assertRaises(AssistantError):
            visual._raster({'name': 'fake.png'}, b'broken')

    def test_publish_inherits_scopes_topic_and_rejects_stale_permission(self):
        self.actor.update(channel='feishu:chat', _feishu_topic={'id': 'topic-one'})
        identity = self.upload()
        result = visual.preprocess(self.files, self.actor, identity)
        with self.assertRaises(AssistantError):
            visual.publish(self.files, {**self.actor, 'scopes': ['E']}, result)
        self.assertEqual(len(self.files.store.docs), 1)
        delivered = visual.publish(self.files, copy.deepcopy(self.actor), result)
        item = self.files.get(self.actor, delivered['files'][0]['id'])
        self.assertEqual(item['source_scopes'], ['D'])
        self.assertEqual(item['topic_id'], 'topic-one')
        self.assertTrue(delivered['files'][0]['is_image'])
        self.assertNotIn('outputs', delivered)
        self.assertNotIn('_sources', delivered)

    def test_ocr_uses_only_derived_local_pixels_has_safe_metadata_and_removes_temporary_files(self):
        identity = self.upload(png((80, 400)))
        paths = []

        def ocr(kind, payload, timeout):
            self.assertEqual(kind, 'ocr')
            self.assertLessEqual(timeout, 10)
            path = Path(payload['path'])
            paths.append(path)
            with Image.open(path) as image:
                self.assertEqual(image.size, (40, 128) if len(paths) == 1 else (40, 92))
            self.assertNotEqual(path, Path(self.files.get(self.actor, identity)['path']))
            return {'text': '设备 D-101\npassword=DO_NOT_RETURN', 'extraction_complete': True,
                    'low_confidence_count': 1}

        with patch.object(visual, 'run_local', side_effect=ocr) as worker:
            result = visual.preprocess(self.files, self.actor, identity, crop=[10, 20, 50, 240],
                                       tile_height=128, overlap=0, ocr=True)
        self.assertEqual(worker.call_count, 2)
        self.assertTrue(result['ocr']['complete'])
        self.assertIn('D-101', result['ocr']['regions'][0]['text'])
        self.assertNotIn('DO_NOT_RETURN', str(result['ocr']))
        self.assertEqual(result['ocr']['regions'][1]['box'], [10, 148, 50, 240])
        self.assertTrue(all(not path.exists() for path in paths))

    def test_ocr_failure_text_limit_and_budget_are_explicit_partial_results(self):
        identity = self.upload()
        with patch.object(visual, 'run_local', side_effect=RuntimeError('PRIVATE_PATH')):
            result = visual.preprocess(self.files, self.actor, identity, ocr=True)
        self.assertTrue(result['outputs'])
        self.assertFalse(result['ocr']['complete'])
        self.assertNotIn('PRIVATE_PATH', str(result['ocr']))
        with patch.object(visual, 'run_local', return_value={'text': 'x' * 5000, 'extraction_complete': True}):
            result = visual.preprocess(self.files, self.actor, identity, ocr=True)
        self.assertEqual(len(result['ocr']['regions'][0]['text']), 4000)
        self.assertFalse(result['ocr']['complete'])
        with patch.object(visual, 'OCR_SECONDS', 0), patch.object(visual, 'run_local') as worker:
            result = visual.preprocess(self.files, self.actor, identity, ocr=True)
        worker.assert_not_called()
        self.assertEqual(result['ocr']['reason'], 'budget_exhausted')

    def test_source_revoked_during_ocr_or_before_publish_is_rejected(self):
        identity = self.upload()

        def revoke(*args, **kwargs):
            self.files.store.docs[FILE_NAMESPACE, identity]['deleted_at'] = 1
            return {'text': 'fixture', 'extraction_complete': True}

        with patch.object(visual, 'run_local', side_effect=revoke), self.assertRaises(AssistantError):
            visual.preprocess(self.files, self.actor, identity, ocr=True)
        self.assertEqual(len(self.files.store.docs), 1)
        identity = self.upload()
        result = visual.preprocess(self.files, self.actor, identity)
        self.files.store.docs[FILE_NAMESPACE, identity]['source_scopes'] = ['E']
        with self.assertRaises(AssistantError):
            visual.publish(self.files, self.actor, result)

    def test_pixel_comparison_counts_rgb_and_alpha_changes_without_scaling(self):
        first = self.upload(png((10, 10)))
        with Image.open(io.BytesIO(png((10, 10)))) as image:
            image.putpixel((2, 3), (254, 255, 255, 255))
            image.putpixel((4, 7), (255, 255, 255, 0))
            output = io.BytesIO()
            image.save(output, format='PNG')
        second = self.upload(output.getvalue(), scopes=['E'])
        result = visual.compare(self.files, self.actor, [first, second])
        self.assertTrue(result['comparison']['comparable'])
        self.assertFalse(result['comparison']['identical'])
        self.assertEqual(result['comparison']['changed_pixels'], 2)
        self.assertEqual(result['comparison']['changed_fraction'], .02)
        self.assertEqual(result['comparison']['difference_box'], [2, 3, 5, 8])
        self.assertEqual(result['outputs'], [])
        with self.assertRaises(AssistantError):
            visual.publish(self.files, {**self.actor, 'scopes': ['D']}, result)
        delivered = visual.publish(self.files, self.actor, result)
        self.assertEqual(delivered['files'], [])
        self.assertNotIn('_sources', delivered)

    def test_identical_or_different_dimensions_and_comparison_limits(self):
        first, second = self.upload(), self.upload()
        result = visual.compare(self.files, self.actor, [first, second])
        self.assertTrue(result['comparison']['identical'])
        self.assertEqual(result['comparison']['changed_pixels'], 0)
        self.assertIsNone(result['comparison']['difference_box'])
        other = self.upload(png((70, 180)))
        result = visual.compare(self.files, self.actor, [first, other])
        self.assertFalse(result['comparison']['comparable'])
        self.assertNotIn('changed_fraction', result['comparison'])
        for ids in ([first], [first, first], [first, other, second], '../path'):
            with self.assertRaises(AssistantError):
                visual.compare(self.files, self.actor, ids)
        with patch.object(visual, 'MAX_COMPARISON_PIXELS', 10), self.assertRaises(AssistantError):
            visual.compare(self.files, self.actor, [first, second])


if __name__ == '__main__':
    unittest.main()

"""Native media bridge contract; synthetic ToolReturn payloads, no providers/Node/cloud.

Uses the existing FakeActive fixture. Regression failures describe parent-owned
integration defects; this file never patches the bridge to make assertions pass.
"""
import asyncio
import base64
import copy
import io
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from PIL import Image
from pydantic_ai import BinaryContent, Tool, ToolReturn

from bin.test_lighthouse_openclaw import FakeActive
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_openclaw import BusinessBridge

IMAGE_TOOL = 'lighthouse_prepare_image'
MAX_IMAGE_BYTES = 3 * 1024 * 1024


def raster(format):
    with Image.new('RGB', (4, 6), 'white') as image:
        content = io.BytesIO()
        image.save(content, format=format)
        return content.getvalue()


class MediaBridgeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.png, self.jpeg = raster('PNG'), raster('JPEG')

    def active(self, value, *, name=IMAGE_TOOL, image_input=True):
        async def invoke(file_id: str):
            active.executions += 1
            await asyncio.sleep(0)
            return value

        bridge = BusinessBridge()
        active = FakeActive('media-fixture-session', {name: Tool(invoke, timeout=5)})
        active.profile = {'tool_calls': True, 'image_input': image_input}
        active.run_id = 'media-fixture-run'
        active.executions = 0
        bridge.active['media-fixture-token'] = active
        return bridge, active

    @staticmethod
    def payload(*, name=IMAGE_TOOL, call_id='media-call', file_id='synthetic-file'):
        return {'session_key': 'media-fixture-session', 'run_id': 'media-fixture-run',
                'tool': name, 'call_id': call_id, 'params': {'file_id': file_id}}

    async def call(self, bridge, **options):
        return await bridge.call('media-fixture-token', self.payload(**options))

    def media(self, data=None, mime='image/png'):
        return BinaryContent(data=self.png if data is None else data, media_type=mime)

    async def test_allowed_png_and_jpeg_are_json_image_blocks_with_metadata_only_event(self):
        metadata = {'ok': True, 'regions': [{'output_index': 0, 'box': [0, 0, 4, 6]}],
                    'partial': False, 'next_offset': None}
        returned = ToolReturn(metadata, content=[self.media(), self.media(self.jpeg, 'image/jpeg')])
        bridge, active = self.active(returned)
        result = await self.call(bridge)
        self.assertEqual({key: value for key, value in result.items() if key != '_tool_media'}, metadata)
        self.assertEqual([part['mimeType'] for part in result['_tool_media']], ['image/png', 'image/jpeg'])
        self.assertEqual([part['type'] for part in result['_tool_media']], ['image', 'image'])
        self.assertEqual([base64.b64decode(part['data'], validate=True) for part in result['_tool_media']],
                         [self.png, self.jpeg])
        self.assertEqual(json.loads(json.dumps(result)), result)
        event = active.events.get_nowait()
        self.assertEqual(event.event_kind, 'function_tool_result')
        self.assertEqual(event.part.tool_name, 'prepare_image')
        self.assertEqual(event.part.content, metadata)
        self.assertNotIn('_tool_media', event.part.content)
        self.assertTrue(active.events.empty())

    async def test_concurrent_replays_execute_and_emit_exactly_once_and_copies_are_isolated(self):
        bridge, active = self.active(ToolReturn({'ok': True, 'nested': {'count': 1}}, content=[self.media()]))
        results = await asyncio.gather(*(self.call(bridge) for _ in range(8)))
        self.assertTrue(all(result == results[0] for result in results))
        self.assertEqual(active.executions, 1)
        self.assertEqual(active.calls, 1)
        self.assertEqual(len(active.results), 1)
        self.assertEqual(active.events.qsize(), 1)
        expected = copy.deepcopy(results[0])
        results[0]['nested']['count'] = 99
        results[0]['_tool_media'][0]['data'] = 'changed'
        self.assertEqual(await self.call(bridge), expected)
        self.assertEqual(active.executions, 1)
        self.assertEqual(active.events.qsize(), 1)

    async def test_replay_with_changed_arguments_is_conflict_without_second_execution(self):
        bridge, active = self.active(ToolReturn({'ok': True}, content=[self.media()]))
        await self.call(bridge)
        with self.assertRaises(AssistantError) as raised:
            await self.call(bridge, file_id='different-file')
        self.assertEqual(raised.exception.status, 409)
        self.assertEqual(active.executions, 1)
        self.assertEqual(active.events.qsize(), 1)

    async def test_toolreturn_images_are_rejected_for_other_tool_names(self):
        for name in ('lighthouse_probe', 'lighthouse_prepare_image_extra', 'lighthouse_Prepare_Image'):
            with self.subTest(name=name):
                bridge, active = self.active(ToolReturn({'ok': True}, content=[self.media()]), name=name)
                with self.assertRaises(AssistantError) as raised:
                    await self.call(bridge, name=name)
                self.assertEqual(raised.exception.status, 400)
                self.assertEqual(active.results, {})
                self.assertTrue(active.events.empty())

    async def test_image_input_false_rejects_toolreturn_and_never_caches_or_emits_media(self):
        bridge, active = self.active(ToolReturn({'ok': True}, content=[self.media()]), image_input=False)
        with self.assertRaises(AssistantError) as raised:
            await self.call(bridge)
        self.assertEqual(raised.exception.status, 400)
        self.assertEqual(active.results, {})
        self.assertTrue(active.events.empty())

    async def test_cached_images_are_rechecked_after_profile_image_input_is_disabled(self):
        bridge, active = self.active(ToolReturn({'ok': True}, content=[self.media()]))
        await self.call(bridge)
        active.profile['image_input'] = False
        with self.assertRaises(AssistantError):
            await self.call(bridge)
        self.assertEqual(active.executions, 1)
        self.assertEqual(active.events.qsize(), 1)

    async def test_ordinary_result_cannot_smuggle_reserved_media_even_for_image_tool(self):
        for name in (IMAGE_TOOL, 'lighthouse_probe'):
            for image_input in (True, False):
                with self.subTest(name=name, image_input=image_input):
                    returned = {'ok': True, 'note': 'ordinary result', '_tool_media': [
                        {'type': 'image', 'mimeType': 'image/png', 'data': base64.b64encode(self.png).decode()}]}
                    bridge, active = self.active(returned, name=name, image_input=image_input)
                    result = await self.call(bridge, name=name)
                    self.assertEqual(result, {'ok': True, 'note': 'ordinary result'})
                    self.assertNotIn('_tool_media', active.results['media-call'][1])
                    self.assertNotIn('_tool_media', active.events.get_nowait().part.content)
                    self.assertEqual(await self.call(bridge, name=name), result)
                    self.assertEqual(active.executions, 1)

    async def test_toolreturn_reserved_key_is_replaced_by_validated_binary_content(self):
        returned = ToolReturn({'ok': True, '_tool_media': [{'type': 'image', 'data': 'forged'}]},
                              content=[self.media()])
        bridge, active = self.active(returned)
        result = await self.call(bridge)
        self.assertEqual(base64.b64decode(result['_tool_media'][0]['data']), self.png)
        self.assertNotIn('forged', json.dumps(result))
        self.assertNotIn('_tool_media', active.events.get_nowait().part.content)

    async def test_mime_type_and_nonbinary_content_are_rejected(self):
        invalid = [[self.media(mime=mime)] for mime in (
            'image/gif', 'image/webp', 'image/svg+xml', 'application/pdf', 'audio/wav',
            'text/plain', 'image/png; charset=utf-8', 'IMAGE/PNG')]
        invalid += [['ordinary text is not an image'], [{'type': 'image', 'data': 'forged'}]]
        for content in invalid:
            with self.subTest(content_type=type(content[0]).__name__):
                bridge, active = self.active(ToolReturn({'ok': True}, content=content))
                with self.assertRaises(AssistantError) as raised:
                    await self.call(bridge)
                self.assertEqual(raised.exception.status, 400)
                self.assertEqual(active.results, {})
                self.assertTrue(active.events.empty())

    async def test_non_mapping_toolreturn_metadata_is_rejected(self):
        for metadata in ('text', ['list'], None):
            with self.subTest(metadata=metadata):
                bridge, active = self.active(ToolReturn(metadata, content=[self.media()]))
                with self.assertRaises(AssistantError) as raised:
                    await self.call(bridge)
                self.assertEqual(raised.exception.status, 400)
                self.assertEqual(active.results, {})

    async def test_five_images_and_exact_three_mib_are_allowed_but_excess_is_rejected(self):
        exact = self.png + b'\0' * (MAX_IMAGE_BYTES - len(self.png))
        for content in ([self.media()] * 5, [self.media(exact)]):
            bridge, active = self.active(ToolReturn({'ok': True}, content=content))
            result = await self.call(bridge)
            self.assertEqual(len(result['_tool_media']), len(content))
            self.assertEqual(active.executions, 1)
        for content in ([self.media()] * 6, [self.media(exact + b'\0')]):
            bridge, active = self.active(ToolReturn({'ok': True}, content=content))
            with self.assertRaises(AssistantError) as raised:
                await self.call(bridge)
            self.assertEqual(raised.exception.status, 400)
            self.assertEqual(active.results, {})
            self.assertTrue(active.events.empty())

    async def test_empty_binary_content_is_not_an_image(self):
        bridge, active = self.active(ToolReturn({'ok': True}, content=[self.media(b'')]))
        with self.assertRaises(AssistantError):
            await self.call(bridge)
        self.assertEqual(active.results, {})
        self.assertTrue(active.events.empty())

    async def test_cached_media_still_obeys_live_session_and_tool_call_authorization(self):
        bridge, active = self.active(ToolReturn({'ok': True}, content=[self.media()]))
        await self.call(bridge)
        active.profile['tool_calls'] = False
        with self.assertRaises(AssistantError) as raised:
            await self.call(bridge)
        self.assertEqual(raised.exception.status, 403)
        active.profile['tool_calls'] = True
        active.running = False
        with self.assertRaises(AssistantError) as raised:
            await self.call(bridge)
        self.assertEqual(raised.exception.status, 403)
        self.assertEqual(active.executions, 1)


if __name__ == '__main__':
    unittest.main()

"""Structured log privacy and one-batch-per-minute integration checks."""
import asyncio
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service import diagnostics
from openclaw_service.diagnostic_store import DiagnosticStore
from openclaw_service.assistant.lighthouse_ai import AssistantError


class DiagnosticPrivacyTests(unittest.TestCase):
    def setUp(self):
        self.store = Mock()
        self.enterContext(patch.object(diagnostics, '_store', self.store))
        self.enterContext(patch.object(diagnostics, '_version', 'V2.500.20261024'))

    def test_only_structured_metadata_is_captured(self):
        private = 'private question 13812345678 320000199001011234 sk-secret Authorization:Bearer token'
        try:
            raise AssistantError(private, 502, category='model_protocol')
        except AssistantError as exc:
            diagnostics.capture('assistant', 'answer', run_id='eda7f8da' * 4,
                category=exc.category, error=exc,
                native='No callable tools remain after resolving explicit tool allowlist: ' + private)
        event = self.store.append.call_args.args[0]
        serialized = json.dumps(event)
        for text in (private, 'private question', '13812345678', '320000199001011234', 'sk-secret', 'Bearer'):
            self.assertNotIn(text, serialized)
        self.assertEqual(event['native_reason'], 'tools_not_registered')
        self.assertEqual(event['run_id'], 'eda7f8da' * 4)
        self.assertEqual(event['http_status'], 502)
        self.assertEqual(event['version'], 'V2.500.20261024')
        self.assertTrue(event['frames'])
        self.assertNotIn(str(Path(__file__).parent), serialized)

    def test_unknown_error_body_and_invalid_identifiers_are_not_uploaded(self):
        diagnostics.capture('feishu', 'answer', run_id='ou_private_user',
            category='private/token', native={'message': 'A private conversation or password'})
        event = self.store.append.call_args.args[0]
        self.assertEqual(event['native_reason'], 'unclassified')
        self.assertNotIn('run_id', event)
        self.assertNotIn('category', event)
        self.assertNotIn('private', json.dumps(event))
        self.assertEqual(len(event['native_fingerprint']), 64)

    def test_upload_errors_and_arbitrary_log_modules_are_not_collected(self):
        for module, stage in (('assistant', 'upload'), ('passwords', 'answer'), ('feishu', 'chat')):
            diagnostics.capture(module, stage, native='secret')
        self.store.append.assert_not_called()

    def test_diagnostic_storage_failure_does_not_break_answer(self):
        self.store.append.side_effect = OSError('disk unavailable')
        diagnostics.capture('assistant', 'answer', category='model_protocol')
        self.store.append.assert_called_once()


class ReporterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = Mock()
        self.batch = {'id': 'batch-one', 'lease_token': 'owned-lease'}
        self.store.claim.return_value = self.batch
        self.remote = Mock()
        self.remote.publish.return_value = 'recFixture'
        self.reporter = diagnostics.DiagnosticReporter(self.store, lambda: self.remote)

    async def test_one_batch_record_per_minute_and_no_idle_upload(self):
        with patch.object(diagnostics, 'monotonic', return_value=100):
            await self.reporter.tick()
            await self.reporter.tick()
        self.remote.publish.assert_called_once()
        self.store.complete.assert_called_once_with('batch-one', 'owned-lease', 'recFixture')
        self.store.claim.assert_called_once()
        self.store.claim.return_value = None
        with patch.object(diagnostics, 'monotonic', return_value=161):
            await self.reporter.tick()
        self.remote.publish.assert_called_once()

    async def test_persistent_batches_upload_at_minute_boundaries_without_empty_records(self):
        with tempfile.TemporaryDirectory() as root:
            store = DiagnosticStore(root)
            reporter = diagnostics.DiagnosticReporter(store, lambda: self.remote)
            event = {'module': 'assistant', 'stage': 'answer', 'version': 'development'}
            now = 1000.0
            with patch('openclaw_service.diagnostic_store.time.time', side_effect=lambda: now), \
                    patch.object(diagnostics, 'monotonic', side_effect=lambda: now):
                first = store.append(event)
                now = 1059.0
                self.assertEqual(store.append(event), first)
                await reporter.tick()
                self.remote.publish.assert_not_called()

                now = 1060.0
                await reporter.tick()
                self.remote.publish.assert_called_once()
                uploaded = self.remote.publish.call_args.args[0]
                self.assertEqual((uploaded['error_count'], uploaded['events'][0]['count']), (2, 2))
                self.assertEqual(store.get_batch(first)['state'], 'done')

                second = store.append(event)
                self.assertNotEqual(second, first)
                now = 1119.0
                await reporter.tick()
                self.remote.publish.assert_called_once()
                now = 1120.0
                await reporter.tick()
                self.assertEqual(self.remote.publish.call_count, 2)
                self.assertEqual(store.get_batch(second)['state'], 'done')
                now = 1180.0
                await reporter.tick()
                self.assertEqual(self.remote.publish.call_count, 2)

    async def test_failure_retains_lease_identity_and_safe_retry_code(self):
        self.remote.publish.side_effect = AssistantError('private upstream response', 429, category='rate_limit')
        await self.reporter.tick()
        self.store.fail.assert_called_once_with('batch-one', 'owned-lease', 'rate_limit')
        self.store.complete.assert_not_called()

    async def test_checkpoint_is_bound_to_the_claimed_batch(self):
        def publish(batch, checkpoint):
            checkpoint(file_token='fixture-token', auth_fingerprint='fixture-app')
            return 'recFixture'
        self.remote.publish.side_effect = publish
        await self.reporter.tick()
        self.store.checkpoint.assert_called_once_with('batch-one', 'owned-lease',
            file_token='fixture-token', auth_fingerprint='fixture-app')

    async def test_close_waits_for_sqlite_reader_before_returning(self):
        with tempfile.TemporaryDirectory() as root:
            store = DiagnosticStore(root)
            entered, release = threading.Event(), threading.Event()
            reporter = diagnostics.DiagnosticReporter(store, lambda: self.remote)
            def blocked_delay():
                with store._connection(read=True):
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError('Test did not release its SQLite reader')
                return 60
            with patch.object(store, 'next_delay', side_effect=blocked_delay):
                reporter.start()
                closing = None
                try:
                    self.assertTrue(await asyncio.to_thread(entered.wait, 3))
                    closing = asyncio.create_task(reporter.close())
                    with self.assertRaises(asyncio.TimeoutError):
                        await asyncio.wait_for(asyncio.shield(closing), .05)
                    self.remote.publish.assert_not_called()
                finally:
                    release.set()
                    await asyncio.wait_for(closing if closing is not None else reporter.close(), 3)
                self.assertTrue(reporter.task.done())

    async def test_cancellation_does_not_mark_uncertain_write_failed(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def work(*args, **kwargs):
            if args[0] == self.store.claim:
                return self.batch
            entered.set()
            await release.wait()
            self.store.complete('batch-one', 'owned-lease', 'recFixture')
        with patch.object(diagnostics.asyncio, 'to_thread', side_effect=work):
            task = asyncio.create_task(self.reporter.tick())
            await entered.wait()
            task.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.store.fail.assert_not_called()
        self.store.complete.assert_called_once()


if __name__ == '__main__':
    unittest.main()

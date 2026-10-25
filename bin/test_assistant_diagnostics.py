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

    # --- Native gateway terminal diagnostics -----------------------------------------------
    # Fixtures: full native id "lh-" + 20 hex + ":" + 32 hex, and a decoy account id.
    ACCOUNT = '5566778899001122'
    NATIVE = f'lh-0123456789abcdef0123:{"0123456789abcdef" * 2}'
    RUN_ID = '0123456789abcdef' * 2
    INCOMPLETE = ('incomplete turn detected stopReason=max_tokens hasLastAssistant=false '
        'hasCurrentAttemptAssistant=true payloads=3 tools=0 compactions=1 replaySafe=true')
    # Verbatim pinned native logger output (the review fixture), split only for readability.
    REAL_LINE = ('incomplete turn detected: runId=' + NATIVE + ':fallback:1:validation:2 sessionId=abc '
        'provider=lighthouse/example stopReason=error hasLastAssistant=yes hasCurrentAttemptAssistant=yes '
        'payloads=0 tools=0 replaySafe=yes compactions=0 reasoningRetries=0/1 emptyRetries=0/1 '
        'missingAssistantRetries=0/1 \u2014 surfacing error to user.')

    def test_exact_pinned_native_line_is_captured_without_raw_text(self):
        self.assertTrue(diagnostics.capture_gateway_line(self.REAL_LINE))
        event = self.store.append.call_args.args[0]
        self.assertEqual(event['run_id'], self.RUN_ID)
        self.assertEqual(event['native_reason'], 'incomplete_turn')
        self.assertEqual(event['details'], {'stop_reason': 'error', 'has_assistant': True,
            'has_current_assistant': True, 'payloads': 0, 'tools': 0, 'replay_safe': True, 'compactions': 0,
            'reasoning_retries': 0, 'empty_retries': 0, 'missing_retries': 0})
        serialized = json.dumps(event)
        for text in ('sessionId', 'ses-', 'provider', 'lighthouse', 'surfacing', self.NATIVE,
                'runId', 'fallback', 'validation', 'reasoningRetries'):
            self.assertNotIn(text, serialized)
        # Values are allowlisted scalars only; the session id is never a value (no substring check,
        # since 'abc' legitimately occurs inside hex ids and fingerprints).
        self.assertEqual(set(event), {'module', 'stage', 'version', 'run_id', 'category', 'native_reason',
            'native_fingerprint', 'details'})
        self.assertNotIn('abc', [value for value in event.values() if isinstance(value, str)])
        self.assertEqual(diagnostics.clean_details(event['details']), event['details'])

    def test_native_incomplete_turn_metadata_is_captured(self):
        line = f'2026-10-25 10:00:00,123 WARNING [gateway] {self.INCOMPLETE} runId={self.NATIVE} sessionId=ses-private-9f8e7d'
        self.assertTrue(diagnostics.capture_gateway_line(line))
        event = self.store.append.call_args.args[0]
        self.assertEqual(event['module'], 'assistant')
        self.assertEqual(event['stage'], 'gateway')
        self.assertEqual(event['category'], 'native_terminal')
        self.assertEqual(event['run_id'], self.RUN_ID)
        self.assertEqual(event['native_reason'], 'incomplete_turn')
        self.assertEqual(event['details'], {'stop_reason': 'max_tokens', 'has_assistant': False,
            'has_current_assistant': True, 'payloads': 3, 'tools': 0, 'compactions': 1, 'replay_safe': True})
        serialized = json.dumps(event)
        for text in ('ses-private-9f8e7d', 'sessionId', self.NATIVE, 'lh-0123456789abcdef0123', 'runId',
                '2026-10-25', '[gateway]'):
            self.assertNotIn(text, serialized)

    def test_json_log_marker_is_parsed_from_string_message_only(self):
        message = f'{self.INCOMPLETE} runId={self.NATIVE} sessionId=ses-abc'
        native = {"message": message, "event": "warning", "logger": "gateway"}
        self.assertTrue(diagnostics.capture_gateway_line(json.dumps(native)))
        event = self.store.append.call_args.args[0]
        self.assertEqual(event['run_id'], self.RUN_ID)
        self.assertEqual(event['details']['stop_reason'], 'max_tokens')
        self.assertNotIn('ses-abc', json.dumps(event))
        self.assertNotIn('logger', json.dumps(event))

    def test_retry_markers_store_only_their_counters(self):
        cases = {
            # Pinned templates: reasoning-only uses attempts=2/2, missing-assistant uses retrying 1/1.
            f'reasoning-only retries exhausted attempts=2/2 runId={self.NATIVE} sessionId=ses-r': (
                'reasoning_only', 'reasoning_retries', 2),
            f'missing assistant terminal message detected retrying 1/1 runId={self.NATIVE} sessionId=ses-m': (
                'missing_response', 'missing_retries', 1),
            f'empty response retries exhausted retries=3/3 runId={self.NATIVE} sessionId=ses-e': (
                'empty_response', 'empty_retries', 3),
            f'empty response retries exhausted empty-retries=4/5 runId={self.NATIVE} sessionId=ses-n': (
                'empty_response', 'empty_retries', 4),
        }
        for line, (reason, counter, used) in cases.items():
            with self.subTest(reason=reason, counter=counter):
                self.store.reset_mock()
                self.assertTrue(diagnostics.capture_gateway_line(line))
                event = self.store.append.call_args.args[0]
                self.assertEqual(event['native_reason'], reason)
                self.assertEqual(event['details'], {counter: used})
                self.assertEqual(event['run_id'], self.RUN_ID)

    def test_pinned_camel_case_retry_counters_are_parsed(self):
        line = ('incomplete turn detected: runId=' + self.NATIVE + ':fallback:1:validation:2 sessionId=abc '
            'provider=lighthouse/example stopReason=error hasLastAssistant=yes hasCurrentAttemptAssistant=yes '
            'payloads=0 tools=0 replaySafe=yes compactions=0 reasoningRetries=0/1 emptyRetries=0/1 '
            'missingAssistantRetries=0/1 — surfacing error to user.')
        self.assertTrue(diagnostics.capture_gateway_line(line))
        event = self.store.append.call_args.args[0]
        self.assertEqual(event['run_id'], self.RUN_ID)
        self.assertEqual(event['details'], {'stop_reason': 'error', 'has_assistant': True,
            'has_current_assistant': True, 'payloads': 0, 'tools': 0, 'replay_safe': True, 'compactions': 0,
            'reasoning_retries': 0, 'empty_retries': 0, 'missing_retries': 0})

    def test_pinned_template_with_bare_zero_counters_keeps_enums_and_bools_only(self):
        line = ('incomplete turn detected: runId=' + self.NATIVE + ' sessionId=abc stopReason=error '
            'hasLastAssistant=no hasCurrentAttemptAssistant=yes payloads=0 tools=0 replaySafe=no compactions=0 '
            'reasoningRetries=0/1 emptyRetries=0/1 missingAssistantRetries=0/1 — surfacing error to user.')
        self.assertTrue(diagnostics.capture_gateway_line(line))
        event = self.store.append.call_args.args[0]
        self.assertEqual(event['details']['has_assistant'], False)
        self.assertEqual(event['details']['has_current_assistant'], True)
        self.assertEqual(event['details']['replay_safe'], False)
        self.assertEqual(event['details']['stop_reason'], 'error')

    def test_response_limit_and_empty_provider_reasons_are_registered(self):
        self.assertEqual(diagnostics.REASONS['model provider returned empty response'], 'empty_response')
        self.assertEqual(diagnostics.REASONS['model response too large'], 'response_limit')
        diagnostics.capture('assistant', 'gateway', native='Model provider returned empty response')
        self.assertEqual(self.store.append.call_args.args[0]['native_reason'], 'empty_response')
        diagnostics.capture('assistant', 'gateway', native='Model response too large')
        self.assertEqual(self.store.append.call_args.args[0]['native_reason'], 'response_limit')

    def test_json_serialized_pinned_line_and_string_key_one(self):
        real = self.REAL_LINE
        self.assertTrue(diagnostics.capture_gateway_line(json.dumps({'message': real}, ensure_ascii=False)))
        event = self.store.append.call_args.args[0]
        self.assertEqual(event['run_id'], self.RUN_ID)
        self.assertEqual(event['native_reason'], 'incomplete_turn')
        self.store.reset_mock()
        # Some handlers log the message as the first record slot: string key "1" is accepted.
        self.assertTrue(diagnostics.capture_gateway_line(json.dumps({'1': real}, ensure_ascii=False)))
        self.assertEqual(self.store.append.call_args.args[0]['run_id'], self.RUN_ID)
        self.store.reset_mock()
        nested = {'message': {'message': real}, '1': 1, 'data': {'message': real}}
        self.assertFalse(diagnostics.capture_gateway_line(nested))
        self.assertFalse(diagnostics.capture_gateway_line(json.dumps({'1': {'message': real}})))
        self.store.append.assert_not_called()

    def test_combined_fallback_validation_suffix_is_parsed(self):
        native = f'{self.NATIVE}:fallback:2:validation:3'
        self.assertEqual(diagnostics.native_run_id(native), self.RUN_ID)
        line = f'missing assistant terminal message detected retrying 1/1 runId={native} sessionId=ses-1'
        self.assertTrue(diagnostics.capture_gateway_line(line))
        event = self.store.append.call_args.args[0]
        self.assertEqual(event['run_id'], self.RUN_ID)
        self.assertEqual(event['details'], {'missing_retries': 1})
        self.assertNotIn('sessionId', json.dumps(event))
        self.assertNotIn('fallback', json.dumps(event))

    def test_longer_original_id_is_not_accepted_by_prefix(self):
        longer = f'lh-0123456789abcdef0123:{"0123456789abcdef" * 2}0'
        self.assertEqual(diagnostics.native_run_id(longer), '')
        for text in (longer, f'{longer}:fallback:1', f'{self.RUN_ID}0'):
            with self.subTest(text=text[-40:]):
                self.store.reset_mock()
                self.assertFalse(diagnostics.capture_gateway_line(
                    f'missing assistant terminal message detected runId={text} sessionId=ses-1'))
                self.store.append.assert_not_called()

    def test_metadata_later_than_512_chars_is_still_parsed(self):
        padding = 'note=' + 'x' * 600
        line = ('missing assistant terminal message detected ' + padding +
            f' retrying 1/1 stopReason=error runId={self.NATIVE} sessionId=ses-late hasLastAssistant=yes')
        self.assertGreater(line.index('runId='), 512)
        self.assertLessEqual(len(line), diagnostics.GATEWAY_LINE_LIMIT)
        self.assertTrue(diagnostics.capture_gateway_line(line))
        event = self.store.append.call_args.args[0]
        self.assertEqual(event['run_id'], self.RUN_ID)
        self.assertEqual(event['details'], {'missing_retries': 1, 'stop_reason': 'error',
            'has_assistant': True})

    def test_over_limit_line_and_embedded_breaks_are_rejected(self):
        base = f'missing assistant terminal message detected runId={self.NATIVE} sessionId=ses-1'
        self.assertLessEqual(len(base), diagnostics.GATEWAY_LINE_LIMIT)
        self.assertFalse(diagnostics.capture_gateway_line(base + ' ' + 'x' * diagnostics.GATEWAY_LINE_LIMIT))
        for break_char in ('\n', '\r', '\u2028', '\u2029'):
            with self.subTest(char=repr(break_char)):
                self.assertFalse(diagnostics.capture_gateway_line(base + break_char + base))
                self.assertFalse(diagnostics.capture_gateway_line(
                    json.dumps({'message': base + break_char + 'runId=ses-private'})))
        self.store.append.assert_not_called()

    def test_unrelated_invalid_run_and_overflow_lines_are_ignored(self):
        lines = [
            'INFO polling finished normally runId=' + self.NATIVE + ' sessionId=ses-1',
            f'missing assistant terminal message detected runId={self.NATIVE}',
            'missing assistant terminal message detected runId=ses-private sessionId=ses-1',
            f'missing assistant terminal message detected runId=lh-0123:{"0123456789abcdef" * 2} sessionId=ses-1',
            f'missing assistant terminal message detected runId=lh-0123456789abcdef0123:{self.RUN_ID[:-1]} sessionId=s',
            f'missing assistant terminal message detected runId={self.NATIVE} sessionId=' + 'x' * 4096,
            f'missing assistant terminal message detected runId={self.NATIVE} sessionId=s\nmore=1',
            {'nested': {'message': 'missing assistant terminal message detected'}},
            {'message': 42},
            None,
            1234,
        ]
        for line in lines:
            with self.subTest(line=str(line)[:60]):
                self.assertFalse(diagnostics.capture_gateway_line(line))
        self.store.append.assert_not_called()

    def test_raw_private_text_in_the_line_is_never_uploaded(self):
        line = ('empty response retries exhausted stopReason= stopped-by-user hasLastAssistant=maybe '
            f'note="Authorization:Bearer sk-secret 13812345678" retries=1/2 runId={self.NATIVE} sessionId=ses-1')
        self.assertTrue(diagnostics.capture_gateway_line(line))
        event = self.store.append.call_args.args[0]
        serialized = json.dumps(event)
        for text in ('sk-secret', 'Bearer', '13812345678', 'stopped-by-user', 'maybe', 'note', 'ses-1', self.ACCOUNT):
            self.assertNotIn(text, serialized)
        self.assertEqual(event['details'], {'empty_retries': 1})

    def test_clean_details_drops_raw_text_and_invalid_types(self):
        self.assertEqual(diagnostics.clean_details({'note': 'private text', 'session': 'ses-1',
            'payloads': True, 'tools': -1, 'compactions': 100000001, 'request_bytes': 1.0,
            'response_bytes': '12', 'stop_reason': 'stopped-by-user', 'model_family': 'gpt',
            'response_type': ['json'], 'has_assistant': 'true', 'replay_safe': 1,
            'has_current_assistant': None, 'reasoning_retries': 100000000}), {'reasoning_retries': 100000000})
        self.assertEqual(diagnostics.clean_details(None), {})
        self.assertEqual(diagnostics.clean_details(['payloads']), {})

    def test_valid_detail_scalars_are_preserved(self):
        details = {'payloads': 0, 'tools': 5, 'compactions': 100000000, 'reasoning_retries': 2,
            'empty_retries': 3, 'missing_retries': 0, 'request_bytes': 4096, 'response_bytes': 1,
            'has_assistant': True, 'has_current_assistant': False, 'replay_safe': True,
            'stop_reason': 'toolUse', 'response_type': 'event_stream', 'model_family': 'deepseek'}
        self.assertEqual(diagnostics.clean_details(details), details)

    def test_capture_applies_details_filter_and_never_raises(self):
        diagnostics.capture('assistant', 'model_request', run_id='eda7f8da' * 4,
            details={'response_type': 'json', 'request_bytes': 64, 'prompt': 'private text'})
        event = self.store.append.call_args.args[0]
        self.assertEqual(event['stage'], 'model_request')
        self.assertEqual(event['details'], {'response_type': 'json', 'request_bytes': 64})
        self.store.reset_mock()
        diagnostics.capture('assistant', 'gateway', category='native_terminal',
            native='Model stream interrupted', details={'payloads': {'nested': 'text'}})
        self.assertNotIn('details', self.store.append.call_args.args[0])
        self.store.reset_mock()
        diagnostics.capture('assistant', 'gateway', native='Incomplete turn detected', details='not-a-dict')
        self.assertNotIn('details', self.store.append.call_args.args[0])
        self.assertEqual(self.store.append.call_args.args[0]['native_reason'], 'incomplete_turn')

    def test_new_reasons_and_stages_are_registered(self):
        reasons = {
            'Incomplete turn detected': 'incomplete_turn',
            'Reasoning-only retries exhausted': 'reasoning_only',
            'Empty response retries exhausted': 'empty_response',
            'Missing assistant terminal message detected': 'missing_response',
            'Model provider returned invalid response': 'invalid_provider_response',
            'Model stream interrupted': 'stream_interrupted',
        }
        for text, reason in reasons.items():
            with self.subTest(reason=reason):
                diagnostics.capture('assistant', 'gateway', native=text)
                self.assertEqual(self.store.append.call_args.args[0]['native_reason'], reason)
        self.assertTrue({'gateway', 'model_request'} <= diagnostics.STAGES)

    def test_native_run_id_requires_a_full_native_identifier(self):
        self.assertEqual(diagnostics.native_run_id(self.NATIVE), self.RUN_ID)
        self.assertEqual(diagnostics.native_run_id(f'{self.NATIVE}:fallback:2'), self.RUN_ID)
        self.assertEqual(diagnostics.native_run_id(f'{self.NATIVE}:validation'), self.RUN_ID)
        self.assertEqual(diagnostics.native_run_id(f'{self.NATIVE}:validation:3'), self.RUN_ID)
        self.assertEqual(diagnostics.native_run_id(f'lh-{"a" * 64}:{self.RUN_ID}'), self.RUN_ID)
        invalid = (self.RUN_ID, f'lh-0123:{self.RUN_ID}', f'lh-{"a" * 65}:{self.RUN_ID}',
            f'lh-0123456789abcdef0123:{self.RUN_ID[:-1]}', f'lh-0123456789ABCDEF0123:{self.RUN_ID}',
            f' {self.NATIVE}', f'{self.NATIVE}x', f'{self.NATIVE}:fallback:x', '', None, 42, {'a': 1})
        for value in invalid:
            with self.subTest(value=str(value)[:40]):
                self.assertEqual(diagnostics.native_run_id(value), '')


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
